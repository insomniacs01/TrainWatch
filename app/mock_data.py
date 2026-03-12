import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from .config import NodeConfig, RunConfig


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stable_int(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16)


def _status_for_run(run: RunConfig, index: int) -> str:
    if run.mock_state and run.mock_state != "auto":
        return run.mock_state
    cycle = ["running", "running", "stalled", "completed", "failed", "idle"]
    return cycle[index % len(cycle)]


def _gpu_uuid(node: NodeConfig, index: int) -> str:
    return f"MOCK-{node.id}-{index:02d}"


def _running_line(now: datetime, run: RunConfig, step: int, step_total: int, loss: float, eta_seconds: int) -> str:
    epoch = 4 + step // step_total
    inner_step = step % step_total
    eta = str(timedelta(seconds=max(0, eta_seconds)))
    grad_norm = 0.6 + (step % 17) * 0.03
    lr = 5e-6
    timestamp = now.strftime("[%Y-%m-%d %H:%M:%S,%f]")[:-3] + "][__main__][INFO] -"
    if run.parser == "mapanything":
        return (
            f"{timestamp} Epoch: [{epoch}]  [{inner_step}/{step_total}]  eta: {eta}  lr: {lr:.6f}  "
            f"loss: {loss:.4f}  grad_norm: {grad_norm:.4f}  time: 2.9132  data: 0.0002  max mem: 19498"
        )
    if run.parser == "deepspeed":
        return (
            f"[{now.strftime('%Y-%m-%d %H:%M:%S,000')}] [INFO] step={inner_step} loss={loss:.4f} "
            f"lr={lr:.2e} grad_norm={grad_norm:.3f} tokens/s={4200 + (step % 100) * 8:.1f} "
            f"samples/s={12.5 + (step % 10) * 0.1:.1f} eta={eta}"
        )
    return (
        f"epoch={epoch} step={inner_step} total_steps={step_total} loss={loss:.4f} lr={lr:.2e} "
        f"grad_norm={grad_norm:.3f} eta={eta} samples/s={11.5 + (step % 10) * 0.2:.1f}"
    )


def _completed_line(now: datetime) -> str:
    timestamp = now.strftime("[%Y-%m-%d %H:%M:%S,%f]")[:-3] + "][__main__][INFO] -"
    return f"{timestamp} Training complete, saving final checkpoint"


def _failed_line(now: datetime) -> str:
    timestamp = now.strftime("[%Y-%m-%d %H:%M:%S,%f]")[:-3] + "][__main__][INFO] -"
    return f"{timestamp} RuntimeError: CUDA out of memory while allocating tensor"


def build_mock_raw(node: NodeConfig) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    gpus: List[Dict[str, Any]] = []
    gpu_processes: List[Dict[str, Any]] = []
    gpu_loads = {index: [] for index in range(node.mock_gpu_count)}
    runs: List[Dict[str, Any]] = []

    for run_index, run in enumerate(node.runs):
        status = _status_for_run(run, run_index)
        offset = _stable_int(f"{node.id}:{run.id}") % 180
        gpu_index = run.mock_gpu_index if run.mock_gpu_index is not None else run_index % node.mock_gpu_count
        step_total = 409 if run.parser == "mapanything" else 12000
        progress = ((int(now.timestamp()) // 5) + offset) % step_total
        loss_floor = 0.35 + (run_index * 0.15)
        loss = max(loss_floor, 22.0 - progress * 0.035)
        eta_seconds = max(0, (step_total - progress) * 3)
        matched_processes: List[Dict[str, Any]] = []
        log_age_seconds = 6
        last_update_at = now - timedelta(seconds=log_age_seconds)
        tail = _running_line(last_update_at, run, progress, step_total, loss, eta_seconds)
        log_exists = True
        log_error = ""

        if status in {"running", "stalled"}:
            command = (
                f"python scripts/train.py output_dir=/mock/{run.id} parser={run.parser} "
                f"experiment={run.id} --demo-mode"
            )
            matched_processes.append(
                {
                    "pid": 21000 + run_index,
                    "elapsed_seconds": 2400 + offset,
                    "command": command,
                }
            )
            gpu_loads[gpu_index].append({
                "pid": 21000 + run_index,
                "command": command,
                "process_name": "python",
                "used_gpu_memory_mb": 8000 + run_index * 1200,
                "status": status,
            })
        if status == "stalled":
            log_age_seconds = max(run.stall_after_seconds + 60, 1200)
            last_update_at = now - timedelta(seconds=log_age_seconds)
            tail = _running_line(last_update_at, run, progress, step_total, loss, eta_seconds)
        elif status == "completed":
            log_age_seconds = 24
            last_update_at = now - timedelta(seconds=log_age_seconds)
            tail = _completed_line(last_update_at)
        elif status == "failed":
            log_age_seconds = 10
            last_update_at = now - timedelta(seconds=log_age_seconds)
            tail = _failed_line(last_update_at)
        elif status == "idle":
            log_age_seconds = 180
            last_update_at = now - timedelta(seconds=log_age_seconds)
            tail = _running_line(last_update_at, run, max(progress - 40, 0), step_total, loss + 1.0, eta_seconds + 120)
        elif status == "unknown":
            log_exists = False
            log_age_seconds = None
            last_update_at = now
            tail = ""

        runs.append(
            {
                "id": run.id,
                "label": run.label,
                "log_path": run.log_path or f"/mock/{run.id}/train.log",
                "log_exists": log_exists,
                "last_update_at": _iso(last_update_at) if log_exists else "",
                "log_age_seconds": log_age_seconds,
                "tail": tail,
                "log_error": log_error,
                "matched_processes": matched_processes,
            }
        )

    for gpu_index in range(node.mock_gpu_count):
        uuid = _gpu_uuid(node, gpu_index)
        loads = gpu_loads[gpu_index]
        base_util = 4 + int(8 * abs(math.sin((int(now.timestamp()) + gpu_index) / 15)))
        util = min(100, base_util + sum(42 if proc["status"] == "running" else 18 for proc in loads))
        memory_used = min(24576, 400 + sum(proc["used_gpu_memory_mb"] for proc in loads))
        temperature = min(84, 40 + util * 0.35)
        power_draw = min(350, 35 + util * 2.3)
        gpus.append(
            {
                "index": gpu_index,
                "uuid": uuid,
                "name": "NVIDIA A100-SIM" if gpu_index % 2 == 0 else "NVIDIA RTX 4090-SIM",
                "utilization_gpu": round(util, 1),
                "memory_used_mb": float(memory_used),
                "memory_total_mb": 24576.0,
                "temperature_c": round(temperature, 1),
                "power_draw_w": round(power_draw, 1),
                "power_limit_w": 350.0,
            }
        )
        for proc in loads:
            gpu_processes.append(
                {
                    "gpu_uuid": uuid,
                    "pid": proc["pid"],
                    "process_name": proc["process_name"],
                    "used_gpu_memory_mb": float(proc["used_gpu_memory_mb"]),
                    "command": proc["command"],
                    "elapsed_seconds": 2400 + gpu_index * 90,
                }
            )

    busy_count = sum(1 for gpu in gpus if gpu["utilization_gpu"] >= 10)
    return {
        "hostname": f"{node.id}.mock.local",
        "collected_at": _iso(now),
        "loadavg": [round(0.8 + busy_count * 0.9, 2), round(1.2 + busy_count * 0.7, 2), round(1.6 + busy_count * 0.5, 2)],
        "gpus": gpus,
        "gpu_processes": gpu_processes,
        "nvidia_smi": True,
        "gpu_error": "",
        "runs": runs,
    }
