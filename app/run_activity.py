import re
import shlex
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def parse_iso8601(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def command_priority(command: str) -> int:
    if re.search(r"\b(torchrun|deepspeed)\b", command):
        return 0
    if re.search(r"\baccelerate\s+launch\b", command):
        return 1
    if re.search(r"\bpython(?:\d+(?:\.\d+)*)?\b", command):
        return 2
    return 3


def safe_split(command: str) -> List[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def basename(token: str) -> str:
    return token.rstrip("/").rsplit("/", 1)[-1]


def summarize_command(command: str) -> str:
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


def command_signature(command: str) -> str:
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


def select_primary_process(processes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not processes:
        return None
    normalized = [item for item in processes if isinstance(item, dict)]
    if not normalized:
        return None
    return sorted(
        normalized,
        key=lambda item: (
            command_priority(str(item.get("command", ""))),
            -(int(item.get("elapsed_seconds") or 0)),
            int(item.get("pid") or 0),
        ),
    )[0]


def derive_remaining_seconds(parsed: Any, elapsed_seconds: Optional[int], status: str) -> Optional[int]:
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


def derive_progress_percent(parsed: Any) -> Optional[float]:
    if parsed.step is None or parsed.step_total is None or parsed.step_total <= 0:
        return None
    return round(max(0.0, min(100.0, (parsed.step / parsed.step_total) * 100.0)), 1)


def derive_run_activity(parsed: Any, matched_processes: List[Dict[str, Any]], collected_at: str, status: str) -> Dict[str, Any]:
    primary = select_primary_process(matched_processes)
    command = str(primary.get("command", "")).strip() if primary else ""
    elapsed_seconds = int(primary.get("elapsed_seconds")) if primary and primary.get("elapsed_seconds") is not None else None
    collected_at_dt = parse_iso8601(collected_at)
    started_at = ""
    if collected_at_dt and elapsed_seconds is not None:
        started_at = isoformat_utc(collected_at_dt - timedelta(seconds=elapsed_seconds))
    remaining_seconds = derive_remaining_seconds(parsed, elapsed_seconds, status)
    estimated_end_at = ""
    if collected_at_dt and remaining_seconds is not None:
        estimated_end_at = isoformat_utc(collected_at_dt + timedelta(seconds=remaining_seconds))
    return {
        "task_name": summarize_command(command),
        "task_command": command,
        "task_pid": primary.get("pid") if primary else None,
        "started_at": started_at,
        "elapsed_seconds": elapsed_seconds,
        "remaining_seconds": remaining_seconds,
        "estimated_end_at": estimated_end_at,
        "progress_percent": derive_progress_percent(parsed),
    }
