import base64
import json
import re
from typing import Any, Dict, List, Optional, Sequence

from .config import RunConfig
from .models import NodeSnapshot, QueueJob
from .time_utils import utc_now_iso


ACTIVE_QUEUE_STATUSES = {"queued", "starting", "running"}
LAUNCHED_QUEUE_STATUSES = {"starting", "running"}
TERMINAL_QUEUE_STATUSES = {"completed", "failed", "canceled"}
def queue_job_from_dict(payload: Dict[str, Any]) -> QueueJob:
    return QueueJob(
        id=str(payload.get("id", "")).strip(),
        node_id=str(payload.get("node_id", "")).strip(),
        node_label=str(payload.get("node_label", "")).strip(),
        owner=str(payload.get("owner", "anonymous")).strip() or "anonymous",
        label=str(payload.get("label", "Queued Job")).strip() or "Queued Job",
        command=str(payload.get("command", "")).strip(),
        gpu_count=max(1, int(payload.get("gpu_count", 1))),
        created_at=str(payload.get("created_at", "")).strip() or utc_now_iso(),
        updated_at=str(payload.get("updated_at", "")).strip() or utc_now_iso(),
        workdir=str(payload.get("workdir", "")).strip(),
        parser=str(payload.get("parser", "auto") or "auto").strip() or "auto",
        status=str(payload.get("status", "queued") or "queued").strip() or "queued",
        run_status=str(payload.get("run_status", "") or "").strip(),
        started_at=str(payload.get("started_at", "") or "").strip(),
        finished_at=str(payload.get("finished_at", "") or "").strip(),
        allocated_gpu_indices=[int(item) for item in list(payload.get("allocated_gpu_indices", []) or [])],
        log_path=str(payload.get("log_path", "") or "").strip(),
        script_path=str(payload.get("script_path", "") or "").strip(),
        process_match=str(payload.get("process_match", "") or "").strip(),
        run_id=str(payload.get("run_id", "") or "").strip(),
        remote_pid=int(payload["remote_pid"]) if payload.get("remote_pid") is not None else None,
        error=str(payload.get("error", "") or "").strip(),
    )


def summarize_command(command: str) -> str:
    cleaned = " ".join((command or "").strip().split())
    if not cleaned:
        return "Queued Job"
    if len(cleaned) <= 72:
        return cleaned
    return f"{cleaned[:69]}..."


def build_run_config(job: QueueJob) -> RunConfig:
    run_id = job.run_id or f"queue-run-{job.id}"
    process_match = job.process_match or re.escape(job.script_path or f"train-watch/jobs/{job.id}/run.sh")
    return RunConfig(
        id=run_id,
        label=f"[Queue] {job.owner} - {job.label}",
        log_path=job.log_path or None,
        process_match=process_match,
        parser=job.parser or "auto",
        stall_after_seconds=900,
        completion_regex=r"(TRAIN_WATCH_QUEUE_COMPLETED|Training complete|Finished training|saving final checkpoint)",
        error_regex=r"(TRAIN_WATCH_QUEUE_FAILED|TRAIN_WATCH_QUEUE_EXIT_CODE=[1-9][0-9]*|Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)",
    )


def _has_blocking_gpu_processes(processes: Sequence[Any]) -> bool:
    if not processes:
        return False
    memory_values: List[float] = []
    for process in processes:
        used_memory = getattr(process, "used_gpu_memory_mb", None)
        if used_memory is None:
            return True
        memory_values.append(float(used_memory or 0.0))
    return sum(memory_values) >= 1024.0 or max(memory_values, default=0.0) >= 768.0


def select_free_gpu_indices(node: NodeSnapshot, reserved_gpu_indices: Optional[Sequence[int]] = None) -> List[int]:
    reserved = {int(item) for item in list(reserved_gpu_indices or [])}
    available: List[int] = []
    for gpu in sorted(node.gpus, key=lambda item: item.index):
        if gpu.index in reserved:
            continue
        if _has_blocking_gpu_processes(gpu.processes):
            continue
        utilization = float(gpu.utilization_gpu or 0.0)
        memory_used = float(gpu.memory_used_mb or 0.0)
        if utilization >= 10.0:
            continue
        if memory_used >= 1024.0:
            continue
        available.append(gpu.index)
    return available


def queue_summary(jobs: Sequence[QueueJob]) -> Dict[str, Any]:
    items = list(jobs)
    return {
        "jobs_total": len(items),
        "jobs_queued": sum(1 for job in items if job.status == "queued"),
        "jobs_starting": sum(1 for job in items if job.status == "starting"),
        "jobs_running": sum(1 for job in items if job.status == "running"),
        "jobs_completed": sum(1 for job in items if job.status == "completed"),
        "jobs_failed": sum(1 for job in items if job.status == "failed"),
        "jobs_canceled": sum(1 for job in items if job.status == "canceled"),
        "gpu_requested_active": sum(job.gpu_count for job in items if job.status in {"queued", "starting", "running"}),
    }


def build_remote_launch_command(job: QueueJob, gpu_indices: Sequence[int]) -> str:
    job_dir = f"~/.train-watch/jobs/{job.id}"
    payload = {
        "job_id": job.id,
        "label": job.label,
        "owner": job.owner,
        "command": job.command,
        "workdir": job.workdir,
        "job_dir": job_dir,
        "gpu_indices": [int(item) for item in gpu_indices],
    }
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    script = r'''
import base64
import json
import os
import shlex
import stat
import subprocess
import sys

payload = json.loads(base64.b64decode("__PAYLOAD__").decode("utf-8"))
job_id = str(payload["job_id"])
job_dir = os.path.expanduser(str(payload["job_dir"]))
log_path = os.path.join(job_dir, "train-watch.log")
script_path = os.path.join(job_dir, "run.sh")
workdir = os.path.expanduser(str(payload.get("workdir") or "").strip())
command = str(payload.get("command") or "").strip()
if not command:
    raise SystemExit("command is required")
gpu_indices = [str(int(item)) for item in list(payload.get("gpu_indices") or [])]
gpu_csv = ",".join(gpu_indices)
os.makedirs(job_dir, exist_ok=True)
with open(os.path.join(job_dir, "metadata.json"), "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=False, indent=2)
lines = [
    "#!/usr/bin/env bash",
    "set -u",
    f"export TRAIN_WATCH_JOB_ID={shlex.quote(job_id)}",
    f"export TRAIN_WATCH_JOB_LABEL={shlex.quote(str(payload.get('label') or ''))}",
    f"export TRAIN_WATCH_JOB_OWNER={shlex.quote(str(payload.get('owner') or ''))}",
    f"export CUDA_VISIBLE_DEVICES={shlex.quote(gpu_csv)}",
    f"echo {shlex.quote('[train-watch] job_id=' + job_id)}",
    f"echo {shlex.quote('[train-watch] owner=' + str(payload.get('owner') or ''))}",
    f"echo {shlex.quote('[train-watch] label=' + str(payload.get('label') or ''))}",
    f"echo {shlex.quote('[train-watch] gpus=' + gpu_csv)}",
    f"echo {shlex.quote('[train-watch] command=' + command)}",
]
if workdir:
    lines.append(f"cd {shlex.quote(workdir)} || exit 98")
lines.extend([
    "bash -lc %s" % shlex.quote(command),
    'status=$?',
    'echo "[train-watch] exit_code=$status"',
    'if [ "$status" -eq 0 ]; then',
    '  echo "TRAIN_WATCH_QUEUE_COMPLETED"',
    'else',
    '  echo "TRAIN_WATCH_QUEUE_EXIT_CODE=$status"',
    '  echo "TRAIN_WATCH_QUEUE_FAILED"',
    'fi',
    'exit "$status"',
])
with open(script_path, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines) + "\n")
os.chmod(script_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
launch = "nohup bash %s >> %s 2>&1 < /dev/null & echo $!" % (shlex.quote(script_path), shlex.quote(log_path))
proc = subprocess.run(launch, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
if proc.returncode != 0:
    raise SystemExit(proc.stderr.strip() or "launch failed")
pid_text = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
pid_value = int(pid_text) if pid_text.isdigit() else None
print(json.dumps({
    "remote_pid": pid_value,
    "script_path": script_path,
    "log_path": log_path,
}))
'''.replace("__PAYLOAD__", encoded)
    return """PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z \"$PYTHON_BIN\" ]; then
  echo \"python not found\" >&2
  exit 1
fi
\"$PYTHON_BIN\" - <<'PY'
%s
PY""" % script
