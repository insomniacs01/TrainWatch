import asyncio
import base64
import json
import re
import shlex
import shutil
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import paramiko

from .config import AppConfig, NodeConfig, RunConfig
from .mock_data import build_mock_raw
from .models import AlertEvent, AppSnapshot, GPUInfo, GPUProcess, NodeSnapshot, RunSnapshot
from .parsers import parse_training_output
from .ssh_support import build_system_ssh_command


def _parse_iso8601(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _command_priority(command: str) -> int:
    if re.search(r"\b(torchrun|deepspeed)\b", command):
        return 0
    if re.search(r"\baccelerate\s+launch\b", command):
        return 1
    if re.search(r"\bpython(?:\d+(?:\.\d+)*)?\b", command):
        return 2
    return 3


def _basename(token: str) -> str:
    return token.rstrip("/").rsplit("/", 1)[-1]


def _summarize_command(command: str) -> str:
    if not command.strip():
        return ""
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()
    if not parts:
        return command.strip()

    launcher = _basename(parts[0])
    search_parts = parts[1:]
    if launcher == "accelerate" and len(parts) > 1 and parts[1] == "launch":
        launcher = "accelerate launch"
        search_parts = parts[2:]

    if launcher.startswith("python") or launcher in {"torchrun", "deepspeed", "accelerate launch", "bash", "sh"}:
        script = next((item for item in search_parts if item.endswith((".py", ".sh"))), "")
        if not script:
            script = next((item for item in search_parts if not item.startswith("-") and "=" not in item), "")
        if script:
            return f"{launcher} {_basename(script)}"
    return _basename(parts[0])


def _select_primary_process(processes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not processes:
        return None
    normalized = [item for item in processes if isinstance(item, dict)]
    if not normalized:
        return None
    return sorted(
        normalized,
        key=lambda item: (
            _command_priority(str(item.get("command", ""))),
            -(int(item.get("elapsed_seconds") or 0)),
            int(item.get("pid") or 0),
        ),
    )[0]


def _derive_remaining_seconds(parsed: Any, elapsed_seconds: Optional[int], status: str) -> Optional[int]:
    if status == "completed":
        return 0
    if parsed.eta_seconds is not None:
        return max(0, int(parsed.eta_seconds))
    if elapsed_seconds is None or parsed.step is None or parsed.step_total is None:
        return None
    if parsed.step_total <= 0 or parsed.step <= 0:
        return None
    if parsed.step >= parsed.step_total:
        return 0
    progress = min(max(parsed.step / parsed.step_total, 0.0), 0.999999)
    estimated_total = int(round(elapsed_seconds / progress))
    return max(0, estimated_total - int(elapsed_seconds))


def _derive_progress_percent(parsed: Any) -> Optional[float]:
    if parsed.step is None or parsed.step_total is None or parsed.step_total <= 0:
        return None
    return round(max(0.0, min(100.0, (parsed.step / parsed.step_total) * 100.0)), 1)


def _derive_run_activity(parsed: Any, matched_processes: List[Dict[str, Any]], collected_at: str, status: str) -> Dict[str, Any]:
    primary = _select_primary_process(matched_processes)
    command = str(primary.get("command", "")).strip() if primary else ""
    elapsed_seconds = int(primary.get("elapsed_seconds")) if primary and primary.get("elapsed_seconds") is not None else None
    collected_at_dt = _parse_iso8601(collected_at)
    started_at = ""
    if collected_at_dt and elapsed_seconds is not None:
        started_at = _isoformat_utc(collected_at_dt - timedelta(seconds=elapsed_seconds))
    remaining_seconds = _derive_remaining_seconds(parsed, elapsed_seconds, status)
    estimated_end_at = ""
    if collected_at_dt and remaining_seconds is not None:
        estimated_end_at = _isoformat_utc(collected_at_dt + timedelta(seconds=remaining_seconds))
    return {
        "task_name": _summarize_command(command),
        "task_command": command,
        "task_pid": primary.get("pid") if primary else None,
        "started_at": started_at,
        "elapsed_seconds": elapsed_seconds,
        "remaining_seconds": remaining_seconds,
        "estimated_end_at": estimated_end_at,
        "progress_percent": _derive_progress_percent(parsed),
    }


REMOTE_SCRIPT = r'''
import base64
import glob
import json
import os
import re
import shlex
import socket
import subprocess
import time

cfg = json.loads(base64.b64decode("__PAYLOAD__").decode("utf-8"))

TRAIN_LAUNCH_RE = re.compile(r"\b(torchrun|deepspeed)\b|\baccelerate\s+launch\b", flags=re.IGNORECASE)
NON_TRAIN_HINT_RE = re.compile(r"\b(jupyter|tensorboard|gradio|streamlit|ray|serve|server|inference|infer|vllm)\b", flags=re.IGNORECASE)


def run_command(command):
    proc = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.stdout, proc.stderr, proc.returncode


def tail_file(path, line_limit=1200, byte_limit=1048576):
    with open(path, "rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(0, size - byte_limit))
        data = handle.read().decode("utf-8", "replace")
    return "\n".join(data.splitlines()[-line_limit:])


def resolve_path(run_cfg):
    path = run_cfg.get("log_path")
    if path:
        return os.path.expanduser(path)
    candidates = glob.glob(os.path.expanduser(run_cfg.get("log_glob", "")))
    if not candidates:
        return ""
    candidates = [candidate for candidate in candidates if os.path.isfile(candidate)]
    if not candidates:
        return ""
    return max(candidates, key=lambda item: os.path.getmtime(item))


def safe_split(command):
    try:
        return shlex.split(command)
    except Exception:
        return command.split()


def command_priority(command):
    if re.search(r"\b(torchrun|deepspeed)\b", command):
        return 0
    if re.search(r"\baccelerate\s+launch\b", command):
        return 1
    if re.search(r"\bpython(?:\d+(?:\.\d+)*)?\b", command):
        return 2
    return 3


def basename(token):
    return token.rstrip("/").rsplit("/", 1)[-1]


def summarize_command(command):
    if not command.strip():
        return ""
    parts = safe_split(command)
    if not parts:
        return command.strip()
    launcher = basename(parts[0])
    search_parts = parts[1:]
    if launcher == "accelerate" and len(parts) > 1 and parts[1] == "launch":
        launcher = "accelerate launch"
        search_parts = parts[2:]
    if launcher.startswith("python") or launcher in {"torchrun", "deepspeed", "accelerate launch", "bash", "sh"}:
        script = next((item for item in search_parts if item.endswith((".py", ".sh"))), "")
        if not script:
            script = next((item for item in search_parts if not item.startswith("-") and "=" not in item), "")
        if script:
            return f"{launcher} {basename(script)}"
    return basename(parts[0])


def command_signature(command):
    parts = safe_split(command)
    if not parts:
        return ""
    launcher = basename(parts[0])
    search_parts = parts[1:]
    if launcher == "accelerate" and len(parts) > 1 and parts[1] == "launch":
        search_parts = parts[2:]
    script = next((item for item in search_parts if item.endswith((".py", ".sh"))), "")
    if script:
        return basename(script)
    token = next((item for item in search_parts if not item.startswith("-") and "=" not in item), "")
    if token:
        return basename(token)
    return launcher


def guess_parser(command):
    lowered = command.lower()
    if "deepspeed" in lowered:
        return "deepspeed"
    if "mapanything" in lowered:
        return "mapanything"
    return "auto"


def is_regular_file_target(path):
    if not path:
        return ""
    cleaned = path.replace(" (deleted)", "").strip()
    if not cleaned or cleaned == "/dev/null":
        return ""
    if cleaned.startswith(("pipe:[", "socket:[", "anon_inode:")):
        return ""
    if cleaned.startswith(("/dev/pts/", "/dev/tty")):
        return ""
    return cleaned if os.path.isfile(cleaned) else ""


def read_proc_link(pid, suffix):
    try:
        return os.readlink(f"/proc/{pid}/{suffix}")
    except Exception:
        return ""


def read_proc_fd(pid, fd):
    return is_regular_file_target(read_proc_link(pid, f"fd/{fd}"))


def read_proc_cwd(pid):
    try:
        return os.path.realpath(f"/proc/{pid}/cwd")
    except Exception:
        return ""


def normalize_path(value, cwd=""):
    if not value:
        return ""
    cleaned = os.path.expanduser(str(value).strip().strip('"').strip("'"))
    if not cleaned:
        return ""
    if not os.path.isabs(cleaned):
        base = cwd or os.getcwd()
        cleaned = os.path.abspath(os.path.join(base, cleaned))
    return cleaned


def extract_path_hints(command, cwd):
    file_candidates = []
    dir_candidates = []
    parts = safe_split(command)
    keys = {
        "--log-file",
        "--log_file",
        "--log-path",
        "--log_path",
        "--output-dir",
        "--output_dir",
        "--logging-dir",
        "--logging_dir",
        "--log-dir",
        "--log_dir",
        "--save-dir",
        "--save_dir",
        "--run-dir",
        "--run_dir",
        "--workdir",
    }
    for index, item in enumerate(parts):
        value = ""
        if item.startswith("--"):
            if "=" in item:
                key, value = item.split("=", 1)
            else:
                key = item
                if index + 1 < len(parts):
                    value = parts[index + 1]
            if key not in keys:
                continue
            normalized = normalize_path(value, cwd)
            if not normalized:
                continue
            if os.path.isfile(normalized):
                file_candidates.append(normalized)
            else:
                dir_candidates.append(normalized)
            continue
        if "=" in item:
            key, value = item.split("=", 1)
            if key not in {"output_dir", "logging_dir", "log_dir", "save_dir", "run_dir", "workdir", "log_path"}:
                continue
            normalized = normalize_path(value, cwd)
            if not normalized:
                continue
            if os.path.isfile(normalized):
                file_candidates.append(normalized)
            else:
                dir_candidates.append(normalized)

    for token in parts:
        if token.endswith((".py", ".sh")):
            candidate = normalize_path(os.path.dirname(token), cwd)
            if candidate:
                dir_candidates.append(candidate)
    if cwd:
        dir_candidates.append(cwd)
    return file_candidates, dir_candidates


def candidate_log_files(base_dir):
    if not base_dir or not os.path.isdir(base_dir):
        return []
    patterns = [
        os.path.join(base_dir, "*.log"),
        os.path.join(base_dir, "*.out"),
        os.path.join(base_dir, "*.txt"),
        os.path.join(base_dir, "logs", "*.log"),
        os.path.join(base_dir, "logs", "*.out"),
        os.path.join(base_dir, "logs", "*.txt"),
        os.path.join(base_dir, "output", "*.log"),
        os.path.join(base_dir, "output", "*.out"),
        os.path.join(base_dir, "outputs", "*.log"),
        os.path.join(base_dir, "outputs", "*.out"),
        os.path.join(base_dir, "*", "*.log"),
        os.path.join(base_dir, "*", "*.out"),
    ]
    items = []
    for pattern in patterns:
        items.extend(glob.glob(pattern))
    return [item for item in items if os.path.isfile(item)]


def pick_best_log_path(proc_items):
    ranked = []
    seen = set()
    for proc in proc_items:
        for priority, path in ((0, proc.get("stdout_path", "")), (0, proc.get("stderr_path", ""))):
            target = is_regular_file_target(path)
            if not target or target in seen:
                continue
            seen.add(target)
            try:
                ranked.append((priority, -os.path.getmtime(target), target))
            except Exception:
                continue

        files, dirs = extract_path_hints(proc.get("command", ""), proc.get("cwd", ""))
        for path in files:
            target = is_regular_file_target(path)
            if not target or target in seen:
                continue
            seen.add(target)
            try:
                ranked.append((1, -os.path.getmtime(target), target))
            except Exception:
                continue
        for directory in dirs:
            for target in candidate_log_files(directory):
                normalized = is_regular_file_target(target)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                try:
                    ranked.append((2, -os.path.getmtime(normalized), normalized))
                except Exception:
                    continue
    if not ranked:
        return ""
    ranked.sort()
    return ranked[0][2]


def build_pid_lookup():
    stdout, _stderr, _rc = run_command("ps -eo pid=,etimes=,args=")
    items = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        pid, elapsed, command = parts
        pid_int = int(pid)
        items[pid_int] = {
            "elapsed_seconds": int(elapsed),
            "command": command,
            "cwd": read_proc_cwd(pid_int),
            "stdout_path": read_proc_fd(pid_int, 1),
            "stderr_path": read_proc_fd(pid_int, 2),
        }
    return items


def collect_processes(pattern, pid_lookup=None):
    if not pattern:
        return []
    pid_lookup = pid_lookup or build_pid_lookup()
    items = []
    for pid, proc in pid_lookup.items():
        command = proc["command"]
        if re.search(pattern, command):
            items.append({
                "pid": int(pid),
                "elapsed_seconds": int(proc["elapsed_seconds"]),
                "command": command,
            })
    return items


def is_training_candidate(command, has_gpu=False):
    lowered = command.lower().strip()
    if not lowered:
        return False
    if NON_TRAIN_HINT_RE.search(lowered):
        return False
    if TRAIN_LAUNCH_RE.search(lowered):
        return True
    if has_gpu and re.search(r"\bpython(?:\d+(?:\.\d+)*)?\b", lowered) and ".py" in lowered:
        return True
    if re.search(r"\bpython(?:\d+(?:\.\d+)*)?\b", lowered) and re.search(r"(train|trainer|finetune|fine-tune|pretrain|pre-training|sft|rlhf|dpo|ppo|main\.py)", lowered):
        return True
    return False


def parse_nvidia_smi(pid_lookup=None):
    result = {"gpus": [], "gpu_processes": [], "nvidia_smi": False, "gpu_error": ""}
    pid_lookup = pid_lookup or build_pid_lookup()
    stdout, stderr, rc = run_command(
        "nvidia-smi --query-gpu=index,uuid,name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,power.limit --format=csv,noheader,nounits"
    )
    if rc != 0:
        result["gpu_error"] = stderr.strip() or "nvidia-smi unavailable"
        return result
    result["nvidia_smi"] = True
    for line in stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 8:
            continue
        result["gpus"].append({
            "index": int(parts[0]),
            "uuid": parts[1],
            "name": parts[2],
            "utilization_gpu": float(parts[3]) if parts[3] else None,
            "memory_used_mb": float(parts[4]) if parts[4] else None,
            "memory_total_mb": float(parts[5]) if parts[5] else None,
            "temperature_c": float(parts[6]) if parts[6] else None,
            "power_draw_w": float(parts[7]) if parts[7] else None,
            "power_limit_w": float(parts[8]) if len(parts) > 8 and parts[8] else None,
        })
    proc_stdout, _proc_stderr, _proc_rc = run_command(
        "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits"
    )
    for line in proc_stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 4:
            continue
        pid = int(parts[1]) if parts[1] else None
        proc = pid_lookup.get(pid or -1, {})
        result["gpu_processes"].append({
            "gpu_uuid": parts[0],
            "pid": pid,
            "process_name": parts[2],
            "used_gpu_memory_mb": float(parts[3]) if parts[3] else None,
            "command": proc.get("command", ""),
            "elapsed_seconds": proc.get("elapsed_seconds"),
            "cwd": proc.get("cwd", ""),
            "stdout_path": proc.get("stdout_path", ""),
            "stderr_path": proc.get("stderr_path", ""),
        })
    return result


def discover_runs(pid_lookup, gpu_processes):
    gpu_pid_set = {int(item.get("pid")) for item in gpu_processes if item.get("pid") is not None}
    grouped = {}
    for pid, proc in pid_lookup.items():
        command = proc.get("command", "")
        has_gpu = pid in gpu_pid_set
        if not is_training_candidate(command, has_gpu=has_gpu):
            continue
        signature = command_signature(command) or f"pid-{pid}"
        group_key = f"{proc.get('cwd', '')}::{signature}"
        group = grouped.setdefault(group_key, {"proc_items": []})
        group["proc_items"].append({
            "pid": pid,
            "elapsed_seconds": int(proc.get("elapsed_seconds") or 0),
            "command": command,
            "cwd": proc.get("cwd", ""),
            "stdout_path": proc.get("stdout_path", ""),
            "stderr_path": proc.get("stderr_path", ""),
        })

    results = []
    for group in grouped.values():
        proc_items = sorted(
            group["proc_items"],
            key=lambda item: (command_priority(item.get("command", "")), -int(item.get("elapsed_seconds") or 0), int(item.get("pid") or 0)),
        )
        primary = proc_items[0]
        log_path = pick_best_log_path(proc_items)
        log_exists = bool(log_path and os.path.exists(log_path))
        last_update_at = ""
        log_age_seconds = None
        log_text = ""
        log_error = ""
        if log_exists:
            try:
                mtime = os.path.getmtime(log_path)
                last_update_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime))
                log_age_seconds = int(time.time() - mtime)
                log_text = tail_file(log_path)
            except Exception as exc:
                log_error = str(exc)
        results.append({
            "id": f"auto-{int(primary.get('pid') or 0)}",
            "label": summarize_command(primary.get("command", "")) or f"Task {primary.get('pid')}",
            "parser": guess_parser(primary.get("command", "")),
            "log_path": log_path,
            "log_exists": log_exists,
            "last_update_at": last_update_at,
            "log_age_seconds": log_age_seconds,
            "tail": log_text,
            "log_error": log_error,
            "matched_processes": [
                {"pid": item.get("pid"), "elapsed_seconds": item.get("elapsed_seconds"), "command": item.get("command", "")}
                for item in proc_items
            ],
        })
    return results


def read_cpu_sample():
    with open("/proc/stat", "r", encoding="utf-8") as handle:
        parts = handle.readline().split()
    values = [int(item) for item in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return total, idle


def collect_cpu():
    try:
        total_a, idle_a = read_cpu_sample()
        time.sleep(0.2)
        total_b, idle_b = read_cpu_sample()
        total_delta = max(total_b - total_a, 1)
        idle_delta = max(idle_b - idle_a, 0)
        usage_percent = max(0.0, min(100.0, 100.0 * (total_delta - idle_delta) / total_delta))
    except Exception:
        usage_percent = None
    return {
        "usage_percent": round(usage_percent, 2) if usage_percent is not None else None,
        "cores_logical": int(os.cpu_count() or 0),
    }


def collect_memory():
    info = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, raw_value = line.split(":", 1)
                value = raw_value.strip().split()[0]
                info[key] = int(value)
    except Exception as exc:
        return {"error": str(exc)}

    total_kb = int(info.get("MemTotal", 0))
    available_kb = int(info.get("MemAvailable", info.get("MemFree", 0)))
    used_kb = max(total_kb - available_kb, 0)
    swap_total_kb = int(info.get("SwapTotal", 0))
    swap_free_kb = int(info.get("SwapFree", 0))
    swap_used_kb = max(swap_total_kb - swap_free_kb, 0)
    return {
        "total_mb": round(total_kb / 1024.0, 2),
        "used_mb": round(used_kb / 1024.0, 2),
        "available_mb": round(available_kb / 1024.0, 2),
        "used_percent": round(100.0 * used_kb / total_kb, 2) if total_kb else None,
        "swap_total_mb": round(swap_total_kb / 1024.0, 2),
        "swap_used_mb": round(swap_used_kb / 1024.0, 2),
        "swap_used_percent": round(100.0 * swap_used_kb / swap_total_kb, 2) if swap_total_kb else 0.0,
    }


def collect_disk(path="/"):
    try:
        stat = os.statvfs(path)
        total_bytes = stat.f_frsize * stat.f_blocks
        free_bytes = stat.f_frsize * stat.f_bavail
        used_bytes = max(total_bytes - free_bytes, 0)
        return {
            "path": path,
            "total_gb": round(total_bytes / (1024.0 ** 3), 2),
            "used_gb": round(used_bytes / (1024.0 ** 3), 2),
            "free_gb": round(free_bytes / (1024.0 ** 3), 2),
            "used_percent": round(100.0 * used_bytes / total_bytes, 2) if total_bytes else None,
        }
    except Exception as exc:
        return {"path": path, "error": str(exc)}


pid_lookup = build_pid_lookup()
payload = {
    "hostname": socket.gethostname(),
    "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "loadavg": list(os.getloadavg()) if hasattr(os, "getloadavg") else [],
    "cpu": collect_cpu(),
    "memory": collect_memory(),
    "disk": collect_disk("/"),
    "runs": [],
    "discovered_runs": [],
}

payload.update(parse_nvidia_smi(pid_lookup))

for run_cfg in cfg.get("runs", []):
    path = resolve_path(run_cfg)
    exists = bool(path and os.path.exists(path))
    last_update_at = ""
    log_age_seconds = None
    log_text = ""
    log_error = ""
    if exists:
        try:
            mtime = os.path.getmtime(path)
            last_update_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime))
            log_age_seconds = int(time.time() - mtime)
            log_text = tail_file(path)
        except Exception as exc:
            log_error = str(exc)
    processes = collect_processes(run_cfg.get("process_match", ""), pid_lookup)
    payload["runs"].append({
        "id": run_cfg["id"],
        "label": run_cfg["label"],
        "log_path": path,
        "log_exists": exists,
        "last_update_at": last_update_at,
        "log_age_seconds": log_age_seconds,
        "tail": log_text,
        "log_error": log_error,
        "matched_processes": processes,
    })

payload["discovered_runs"] = discover_runs(pid_lookup, payload.get("gpu_processes", []))
print(json.dumps(payload))
'''


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ParamikoConnectionPool:
    def __init__(self) -> None:
        self._clients: Dict[str, paramiko.SSHClient] = {}
        self._lock = threading.Lock()
        self._ssh_binary = shutil.which("ssh") or ""

    def _cache_key(self, node: NodeConfig) -> str:
        return node.id

    def _connect(self, node: NodeConfig) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": node.host,
            "port": node.port,
            "username": node.user,
            "timeout": 15,
            "banner_timeout": 15,
            "auth_timeout": 15,
            "allow_agent": False,
            "look_for_keys": False,
        }
        if node.key_path:
            connect_kwargs["key_filename"] = node.key_path
        if node.password:
            connect_kwargs["password"] = node.password
            if node.key_path:
                connect_kwargs["passphrase"] = node.password

        client.connect(**connect_kwargs)
        return client

    def _get_client(self, node: NodeConfig) -> paramiko.SSHClient:
        key = self._cache_key(node)
        with self._lock:
            client = self._clients.get(key)
            if client and client.get_transport() and client.get_transport().is_active():
                return client
            client = self._connect(node)
            self._clients[key] = client
            return client

    def _execute_system_ssh(self, node: NodeConfig, command: str, timeout: int) -> Tuple[str, str, int]:
        if not self._ssh_binary:
            raise RuntimeError("ssh binary not found for key/config-based connection")
        ssh_command = build_system_ssh_command(node, command, ssh_binary=self._ssh_binary)
        try:
            proc = subprocess.run(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("SSH command timed out") from exc
        return proc.stdout, proc.stderr, proc.returncode

    def execute(self, node: NodeConfig, command: str, timeout: int) -> Tuple[str, str, int]:
        if not node.password:
            return self._execute_system_ssh(node, command, timeout)

        attempts = 0
        last_error = None
        while attempts < 2:
            attempts += 1
            try:
                client = self._get_client(node)
                _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
                output = stdout.read().decode("utf-8", "replace")
                error = stderr.read().decode("utf-8", "replace")
                code = stdout.channel.recv_exit_status()
                return output, error, code
            except Exception as exc:
                last_error = exc
                self.close_node(node)
        raise RuntimeError(str(last_error) if last_error else "SSH execution failed")

    def close_node(self, node: NodeConfig) -> None:
        key = self._cache_key(node)
        with self._lock:
            client = self._clients.pop(key, None)
        if client:
            client.close()

    def close_all(self) -> None:
        with self._lock:
            keys = list(self._clients.keys())
        for key in keys:
            client = self._clients.pop(key, None)
            if client:
                client.close()


class Collector:
    def __init__(self, config: AppConfig, pool: Optional[ParamikoConnectionPool] = None) -> None:
        self.config = config
        self.pool = pool or ParamikoConnectionPool()

    def close(self) -> None:
        self.pool.close_all()

    def _build_command(self, node: NodeConfig) -> str:
        payload = {
            "runs": [
                {
                    "id": run.id,
                    "label": run.label,
                    "log_path": run.log_path,
                    "log_glob": run.log_glob,
                    "process_match": run.process_match,
                }
                for run in node.runs
            ]
        }
        encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
        script = REMOTE_SCRIPT.replace("__PAYLOAD__", encoded)
        return """PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "python not found" >&2
  exit 127
fi
"$PYTHON_BIN" - <<'PY'
%s
PY""" % script

    def collect_node(self, node: NodeConfig) -> NodeSnapshot:
        try:
            if node.transport == "mock":
                return self._build_node_snapshot(node, build_mock_raw(node))

            output, error, code = self.pool.execute(node, self._build_command(node), timeout=45)
            if code != 0:
                raise RuntimeError(error.strip() or "Remote command failed")
            raw = json.loads(output)
            return self._build_node_snapshot(node, raw)
        except Exception as exc:
            fallback_status = "offline" if node.transport == "ssh" else "degraded"
            return NodeSnapshot(
                id=node.id,
                label=node.label,
                host=node.host,
                hostname=node.host,
                status=fallback_status,
                error=str(exc),
                collected_at=utc_now_iso(),
                loadavg=[],
                metrics={},
                gpus=[],
                gpu_processes=[],
                runs=[
                    RunSnapshot(
                        id=run.id,
                        label=run.label,
                        parser=run.parser,
                        status="unknown",
                        error=str(exc),
                        log_path=run.log_path or run.log_glob or "",
                        log_exists=False,
                        log_age_seconds=None,
                        last_update_at="",
                        last_log_line="",
                    )
                    for run in node.runs
                ],
            )

    def _build_node_snapshot(self, node: NodeConfig, raw: Dict[str, Any]) -> NodeSnapshot:
        process_by_uuid: Dict[str, List[GPUProcess]] = {}
        gpu_processes: List[GPUProcess] = []
        gpu_uuid_to_index: Dict[str, int] = {}

        for raw_gpu in raw.get("gpus", []):
            gpu_uuid_to_index[str(raw_gpu.get("uuid", ""))] = int(raw_gpu.get("index", 0))

        for raw_proc in raw.get("gpu_processes", []):
            process = GPUProcess(
                pid=raw_proc.get("pid"),
                process_name=str(raw_proc.get("process_name", "")),
                gpu_uuid=str(raw_proc.get("gpu_uuid", "")),
                gpu_index=gpu_uuid_to_index.get(str(raw_proc.get("gpu_uuid", ""))),
                used_gpu_memory_mb=raw_proc.get("used_gpu_memory_mb"),
                command=str(raw_proc.get("command", "")),
                elapsed_seconds=raw_proc.get("elapsed_seconds"),
            )
            process_by_uuid.setdefault(process.gpu_uuid, []).append(process)
            gpu_processes.append(process)

        gpus: List[GPUInfo] = []
        for raw_gpu in raw.get("gpus", []):
            gpu = GPUInfo(
                index=int(raw_gpu.get("index", 0)),
                uuid=str(raw_gpu.get("uuid", "")),
                name=str(raw_gpu.get("name", "GPU")),
                utilization_gpu=raw_gpu.get("utilization_gpu"),
                memory_used_mb=raw_gpu.get("memory_used_mb"),
                memory_total_mb=raw_gpu.get("memory_total_mb"),
                temperature_c=raw_gpu.get("temperature_c"),
                power_draw_w=raw_gpu.get("power_draw_w"),
                power_limit_w=raw_gpu.get("power_limit_w"),
                processes=process_by_uuid.get(str(raw_gpu.get("uuid", "")), []),
            )
            gpus.append(gpu)

        runs = self._build_runs(node, raw)
        metrics = self._build_node_metrics(raw, gpus)
        node_status = "online"
        node_error = ""
        if not raw.get("nvidia_smi") and raw.get("gpu_error") and raw.get("gpus"):
            node_status = "degraded"
            node_error = str(raw.get("gpu_error", ""))

        if any(run.status in {"failed", "stalled"} for run in runs):
            node_status = "degraded"

        return NodeSnapshot(
            id=node.id,
            label=node.label,
            host=node.host,
            hostname=str(raw.get("hostname", node.host)),
            status=node_status,
            error=node_error,
            collected_at=str(raw.get("collected_at", utc_now_iso())),
            loadavg=list(raw.get("loadavg", []) or []),
            metrics=metrics,
            gpus=gpus,
            gpu_processes=gpu_processes,
            runs=runs,
        )

    def _effective_run_configs(self, node: NodeConfig, raw: Dict[str, Any]) -> List[RunConfig]:
        if node.runs:
            return node.runs
        discovered: List[RunConfig] = []
        for item in raw.get("discovered_runs", []) or []:
            run_id = str(item.get("id", "")).strip()
            if not run_id:
                continue
            discovered.append(
                RunConfig(
                    id=run_id,
                    label=str(item.get("label") or run_id).strip() or run_id,
                    log_path=str(item.get("log_path") or "") or None,
                    process_match="",
                    parser=str(item.get("parser") or "auto"),
                    stall_after_seconds=900,
                )
            )
        return discovered

    def _build_node_metrics(self, raw: Dict[str, Any], gpus: List[GPUInfo]) -> Dict[str, float]:
        loadavg = list(raw.get("loadavg", []) or [])
        cpu = dict(raw.get("cpu") or {})
        memory = dict(raw.get("memory") or {})
        disk = dict(raw.get("disk") or {})

        metrics: Dict[str, float] = {
            "loadavg_1m": float(loadavg[0]) if len(loadavg) > 0 else 0.0,
            "loadavg_5m": float(loadavg[1]) if len(loadavg) > 1 else 0.0,
            "loadavg_15m": float(loadavg[2]) if len(loadavg) > 2 else 0.0,
            "cpu_usage_percent": float(cpu.get("usage_percent") or 0.0),
            "cpu_cores_logical": float(cpu.get("cores_logical") or 0.0),
            "memory_total_mb": float(memory.get("total_mb") or 0.0),
            "memory_used_mb": float(memory.get("used_mb") or 0.0),
            "memory_available_mb": float(memory.get("available_mb") or 0.0),
            "memory_used_percent": float(memory.get("used_percent") or 0.0),
            "swap_total_mb": float(memory.get("swap_total_mb") or 0.0),
            "swap_used_mb": float(memory.get("swap_used_mb") or 0.0),
            "swap_used_percent": float(memory.get("swap_used_percent") or 0.0),
            "disk_total_gb": float(disk.get("total_gb") or 0.0),
            "disk_used_gb": float(disk.get("used_gb") or 0.0),
            "disk_free_gb": float(disk.get("free_gb") or 0.0),
            "disk_used_percent": float(disk.get("used_percent") or 0.0),
            "gpu_count": float(len(gpus)),
            "gpu_utilization_avg": 0.0,
            "gpu_temperature_avg": 0.0,
            "gpu_memory_used_mb_total": 0.0,
            "gpu_power_draw_w_total": 0.0,
            "gpu_process_count": 0.0,
        }

        if not gpus:
            return metrics

        util_values = [gpu.utilization_gpu for gpu in gpus if gpu.utilization_gpu is not None]
        temp_values = [gpu.temperature_c for gpu in gpus if gpu.temperature_c is not None]
        memory_values = [gpu.memory_used_mb for gpu in gpus if gpu.memory_used_mb is not None]
        power_values = [gpu.power_draw_w for gpu in gpus if gpu.power_draw_w is not None]
        metrics.update(
            {
                "gpu_utilization_avg": float(sum(util_values) / len(util_values)) if util_values else 0.0,
                "gpu_temperature_avg": float(sum(temp_values) / len(temp_values)) if temp_values else 0.0,
                "gpu_memory_used_mb_total": float(sum(memory_values)) if memory_values else 0.0,
                "gpu_power_draw_w_total": float(sum(power_values)) if power_values else 0.0,
                "gpu_process_count": float(sum(len(gpu.processes) for gpu in gpus)),
            }
        )
        return metrics

    def _build_runs(self, node: NodeConfig, raw: Dict[str, Any]) -> List[RunSnapshot]:
        raw_items = list(raw.get("runs", []) or []) + list(raw.get("discovered_runs", []) or [])
        raw_runs = {str(item.get("id")): item for item in raw_items}
        collected_at = str(raw.get("collected_at", utc_now_iso()))
        results: List[RunSnapshot] = []
        for run_cfg in self._effective_run_configs(node, raw):
            raw_run = raw_runs.get(run_cfg.id, {})
            tail_text = str(raw_run.get("tail", ""))
            parsed = parse_training_output(
                run_cfg.parser,
                tail_text,
                run_cfg.completion_regex,
                run_cfg.error_regex,
            )
            matched_processes = list(raw_run.get("matched_processes", []) or [])
            status, error_message = self._determine_status(run_cfg, raw_run, parsed, matched_processes)
            activity = _derive_run_activity(parsed, matched_processes, collected_at, status)
            results.append(
                RunSnapshot(
                    id=run_cfg.id,
                    label=run_cfg.label,
                    parser=parsed.parser,
                    status=status,
                    error=error_message,
                    log_path=str(raw_run.get("log_path", run_cfg.log_path or run_cfg.log_glob or "")),
                    log_exists=bool(raw_run.get("log_exists")),
                    log_age_seconds=raw_run.get("log_age_seconds"),
                    last_update_at=str(raw_run.get("last_update_at", "")),
                    last_log_line=parsed.last_log_line,
                    epoch=parsed.epoch,
                    step=parsed.step,
                    step_total=parsed.step_total,
                    loss=parsed.loss,
                    eval_loss=parsed.eval_loss,
                    lr=parsed.lr,
                    grad_norm=parsed.grad_norm,
                    tokens_per_sec=parsed.tokens_per_sec,
                    samples_per_sec=parsed.samples_per_sec,
                    eta=parsed.eta,
                    eta_seconds=parsed.eta_seconds,
                    task_name=str(activity.get("task_name", "")),
                    task_command=str(activity.get("task_command", "")),
                    task_pid=activity.get("task_pid"),
                    started_at=str(activity.get("started_at", "")),
                    elapsed_seconds=activity.get("elapsed_seconds"),
                    remaining_seconds=activity.get("remaining_seconds"),
                    estimated_end_at=str(activity.get("estimated_end_at", "")),
                    progress_percent=activity.get("progress_percent"),
                    completion_matched=parsed.completion_matched,
                    error_matched=parsed.error_matched,
                    matched_processes=matched_processes,
                )
            )
        return results

    def _determine_status(
        self,
        run_cfg: RunConfig,
        raw_run: Dict[str, Any],
        parsed: Any,
        matched_processes: List[Dict[str, Any]],
    ) -> Tuple[str, str]:
        log_exists = bool(raw_run.get("log_exists"))
        log_error = str(raw_run.get("log_error", "")).strip()
        log_age_seconds = raw_run.get("log_age_seconds")

        if log_error:
            return "unknown", log_error
        if parsed.error_matched:
            return "failed", "Error pattern matched in training log"
        if parsed.completion_matched and not matched_processes:
            return "completed", "Completion pattern matched and process exited"
        if matched_processes:
            if isinstance(log_age_seconds, int) and log_age_seconds > run_cfg.stall_after_seconds:
                return "stalled", "Log is stale while matching process is still alive"
            return "running", ""
        if log_exists and parsed.loss is not None:
            return "idle", "No matching training process found"
        if log_exists:
            return "idle", "Log exists but no active process matched"
        return "unknown", "Log file not found"

    async def poll_once(
        self,
        previous_snapshot: Optional[AppSnapshot],
        nodes: Optional[List[NodeConfig]] = None,
    ) -> Tuple[AppSnapshot, List[AlertEvent]]:
        active_nodes = nodes if nodes is not None else self.config.nodes
        snapshots = await asyncio.gather(*[asyncio.to_thread(self.collect_node, node) for node in active_nodes])
        snapshot = AppSnapshot(generated_at=utc_now_iso(), summary=self._build_summary(snapshots), nodes=snapshots)
        return snapshot, self._diff_events(previous_snapshot, snapshot)

    def _build_summary(self, nodes: List[NodeSnapshot]) -> Dict[str, Any]:
        runs = [run for node in nodes for run in node.runs]
        gpus = [gpu for node in nodes for gpu in node.gpus]
        cpu_values = [float(node.metrics.get("cpu_usage_percent", 0.0)) for node in nodes]
        memory_percent_values = [float(node.metrics.get("memory_used_percent", 0.0)) for node in nodes]
        disk_percent_values = [float(node.metrics.get("disk_used_percent", 0.0)) for node in nodes]
        memory_used_values = [float(node.metrics.get("memory_used_mb", 0.0)) for node in nodes]
        return {
            "nodes_total": len(nodes),
            "nodes_online": sum(1 for node in nodes if node.status == "online"),
            "nodes_degraded": sum(1 for node in nodes if node.status == "degraded"),
            "nodes_offline": sum(1 for node in nodes if node.status == "offline"),
            "runs_total": len(runs),
            "runs_running": sum(1 for run in runs if run.status == "running"),
            "runs_alerting": sum(1 for run in runs if run.status in ("failed", "stalled")),
            "gpus_total": len(gpus),
            "gpus_busy": sum(1 for gpu in gpus if (gpu.utilization_gpu or 0) >= 10),
            "cpu_usage_avg": float(sum(cpu_values) / len(cpu_values)) if cpu_values else 0.0,
            "memory_used_percent_avg": float(sum(memory_percent_values) / len(memory_percent_values)) if memory_percent_values else 0.0,
            "disk_used_percent_avg": float(sum(disk_percent_values) / len(disk_percent_values)) if disk_percent_values else 0.0,
            "memory_used_mb_total": float(sum(memory_used_values)) if memory_used_values else 0.0,
        }

    def _diff_events(
        self,
        previous_snapshot: Optional[AppSnapshot],
        current_snapshot: AppSnapshot,
    ) -> List[AlertEvent]:
        if previous_snapshot is None:
            return []
        previous_map: Dict[Tuple[str, str], str] = {}
        for node in previous_snapshot.nodes:
            for run in node.runs:
                previous_map[(node.id, run.id)] = run.status

        events: List[AlertEvent] = []
        for node in current_snapshot.nodes:
            for run in node.runs:
                key = (node.id, run.id)
                previous_status = previous_map.get(key, "")
                if previous_status in {"", "connecting"}:
                    continue
                if previous_status != run.status:
                    events.append(
                        AlertEvent(
                            kind="run_status_changed",
                            node_id=node.id,
                            node_label=node.label,
                            run_id=run.id,
                            run_label=run.label,
                            status=run.status,
                            previous_status=previous_status,
                            at=current_snapshot.generated_at,
                            message="%s / %s: %s → %s"
                            % (node.label, run.label, previous_status, run.status),
                        )
                    )
        return events
