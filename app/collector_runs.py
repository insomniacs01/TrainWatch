from typing import Any, Dict, List, Optional, Tuple

from .config import NodeConfig, RunConfig
from .gpu_utils import count_busy_gpus
from .models import ExternalQueueItem, GPUInfo, GPUProcess, RunSnapshot
from .parsers import parse_training_output
from .run_activity import command_signature, derive_run_activity
from .time_utils import utc_now_iso

_EXTERNAL_QUEUE_SORT_ORDER = {"queued": 0, "starting": 1, "running": 2, "unknown": 3}


def effective_run_configs(node: NodeConfig, raw: Dict[str, Any]) -> List[RunConfig]:
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
    if not node.runs:
        return discovered
    configured_ids = {run.id for run in node.runs}
    return list(node.runs) + [run for run in discovered if run.id not in configured_ids]


def build_external_queue_items(payload: Dict[str, Any]) -> List[ExternalQueueItem]:
    items: List[ExternalQueueItem] = []
    for raw_item in payload.get("items", []) or []:
        if not isinstance(raw_item, dict):
            continue
        gpu_count = raw_item.get("gpu_count")
        try:
            parsed_gpu_count = int(gpu_count) if gpu_count not in (None, "") else None
        except (TypeError, ValueError):
            parsed_gpu_count = None
        item_id = str(raw_item.get("id") or raw_item.get("label") or "").strip()
        label = str(raw_item.get("label") or item_id or "外部任务").strip() or "外部任务"
        if not item_id and not label:
            continue
        items.append(
            ExternalQueueItem(
                id=item_id or label,
                owner=str(raw_item.get("owner", "") or "").strip(),
                label=label,
                status=str(raw_item.get("status", "unknown") or "unknown").strip() or "unknown",
                source=str(raw_item.get("source") or payload.get("source") or "external").strip() or "external",
                raw_status=str(raw_item.get("raw_status", "") or "").strip(),
                submitted_at=str(raw_item.get("submitted_at", "") or "").strip(),
                gpu_count=parsed_gpu_count,
                command=str(raw_item.get("command", "") or "").strip(),
                workdir=str(raw_item.get("workdir", "") or "").strip(),
                reason=str(raw_item.get("reason", "") or "").strip(),
            )
        )
    return sorted(
        items,
        key=lambda item: (_EXTERNAL_QUEUE_SORT_ORDER.get(item.status, 9), item.submitted_at or "", item.id),
    )


def build_node_metrics(raw: Dict[str, Any], gpus: List[GPUInfo]) -> Dict[str, float]:
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
        "gpus_busy": float(count_busy_gpus(gpus)),
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


def as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def select_gpu_processes_for_run(
    matched_processes: List[Dict[str, Any]],
    gpu_processes: List[GPUProcess],
) -> List[GPUProcess]:
    selected: Dict[Tuple[str, Optional[int], Optional[int]], GPUProcess] = {}
    gpu_processes_by_pid: Dict[int, List[GPUProcess]] = {}
    for process in gpu_processes:
        pid = as_int(process.pid)
        if pid is None:
            continue
        gpu_processes_by_pid.setdefault(pid, []).append(process)

    for matched in matched_processes:
        pid = as_int(matched.get("pid"))
        if pid is None:
            continue
        for process in gpu_processes_by_pid.get(pid, []):
            selected[(process.gpu_uuid, process.pid, process.gpu_index)] = process

    for matched in matched_processes:
        pid = as_int(matched.get("pid"))
        if pid is not None and gpu_processes_by_pid.get(pid):
            continue
        cwd = str(matched.get("cwd", "") or "").strip()
        signature = command_signature(str(matched.get("command", "")))
        elapsed_seconds = as_int(matched.get("elapsed_seconds"))
        if not cwd or not signature:
            continue
        for process in gpu_processes:
            process_cwd = str(process.cwd or "").strip()
            if process_cwd != cwd:
                continue
            if command_signature(process.command) != signature:
                continue
            process_elapsed = as_int(process.elapsed_seconds)
            if (
                elapsed_seconds is not None
                and process_elapsed is not None
                and abs(process_elapsed - elapsed_seconds) > 600
            ):
                continue
            selected[(process.gpu_uuid, process.pid, process.gpu_index)] = process

    return sorted(
        selected.values(),
        key=lambda item: (
            item.gpu_index is None,
            item.gpu_index if item.gpu_index is not None else 9999,
            item.pid if item.pid is not None else 0,
        ),
    )


def build_run_gpu_usage(
    matched_processes: List[Dict[str, Any]],
    gpu_processes: List[GPUProcess],
) -> Dict[str, Any]:
    selected_processes = select_gpu_processes_for_run(matched_processes, gpu_processes)
    gpu_indices = sorted({int(item.gpu_index) for item in selected_processes if item.gpu_index is not None})
    memory_values = [
        float(item.used_gpu_memory_mb) for item in selected_processes if item.used_gpu_memory_mb is not None
    ]
    return {
        "gpu_indices": gpu_indices,
        "gpu_memory_used_mb": float(sum(memory_values)) if memory_values else None,
    }


def determine_run_status(
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


def build_runs(node: NodeConfig, raw: Dict[str, Any], gpu_processes: List[GPUProcess]) -> List[RunSnapshot]:
    raw_items = list(raw.get("runs", []) or []) + list(raw.get("discovered_runs", []) or [])
    raw_runs = {str(item.get("id")): item for item in raw_items}
    collected_at = str(raw.get("collected_at", utc_now_iso()))
    results: List[RunSnapshot] = []
    for run_cfg in effective_run_configs(node, raw):
        raw_run = raw_runs.get(run_cfg.id, {})
        tail_text = str(raw_run.get("tail", ""))
        parsed = parse_training_output(
            run_cfg.parser,
            tail_text,
            run_cfg.completion_regex,
            run_cfg.error_regex,
        )
        matched_processes = list(raw_run.get("matched_processes", []) or [])
        status, error_message = determine_run_status(run_cfg, raw_run, parsed, matched_processes)
        activity = derive_run_activity(parsed, matched_processes, collected_at, status)
        gpu_usage = build_run_gpu_usage(matched_processes, gpu_processes)
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
                gpu_indices=list(gpu_usage.get("gpu_indices", []) or []),
                gpu_memory_used_mb=gpu_usage.get("gpu_memory_used_mb"),
                progress_percent=activity.get("progress_percent"),
                completion_matched=parsed.completion_matched,
                error_matched=parsed.error_matched,
                matched_processes=matched_processes,
            )
        )
    return results
