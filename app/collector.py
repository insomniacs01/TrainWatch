import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import AppConfig, NodeConfig, RunConfig
from .mock_data import build_mock_raw
from .models import AlertEvent, AppSnapshot, ExternalQueueItem, GPUInfo, GPUProcess, NodeSnapshot, RunSnapshot
from .parsers import parse_training_output
from .remote_probe import build_remote_probe_command
from .run_activity import command_signature, derive_run_activity
from .ssh_pool import ParamikoConnectionPool


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


logger = logging.getLogger(__name__)


def is_gpu_busy(gpu: GPUInfo) -> bool:
    utilization = float(gpu.utilization_gpu or 0.0)
    memory_used = float(gpu.memory_used_mb or 0.0)
    return utilization >= 10.0 or memory_used >= 1024.0


def count_busy_gpus(gpus: List[GPUInfo]) -> int:
    return sum(1 for gpu in gpus if gpu.is_busy)


class Collector:
    def __init__(self, config: AppConfig, pool: Optional[ParamikoConnectionPool] = None) -> None:
        self.config = config
        self.pool = pool or ParamikoConnectionPool(config.server)

    def close(self) -> None:
        self.pool.close_all()

    def _build_command(self, node: NodeConfig) -> str:
        return build_remote_probe_command(node)

    def _build_error_snapshot(self, node: NodeConfig, error: str, status: Optional[str] = None) -> NodeSnapshot:
        snapshot_status = status or ("offline" if node.transport == "ssh" else "degraded")
        return NodeSnapshot(
            id=node.id,
            label=node.label,
            host=node.host,
            hostname=node.host,
            status=snapshot_status,
            error=error,
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
                    error=error,
                    log_path=run.log_path or run.log_glob or "",
                    log_exists=False,
                    log_age_seconds=None,
                    last_update_at="",
                    last_log_line="",
                )
                for run in node.runs
            ],
        )

    def collect_node(self, node: NodeConfig) -> NodeSnapshot:
        try:
            if node.transport == "mock":
                return self._build_node_snapshot(node, build_mock_raw(node))
            if node.needs_password and not node.password and not node.key_path:
                return self._build_error_snapshot(
                    node,
                    "SSH password was not persisted. Re-enter the password or set server.persist_passwords=true.",
                    status="offline",
                )

            output, error, code = self.pool.execute(node, self._build_command(node), timeout=45)
            if code != 0:
                raise RuntimeError(error.strip() or "Remote command failed")
            raw = json.loads(output)
            return self._build_node_snapshot(node, raw)
        except Exception as exc:
            logger.warning("Node collection failed for %s (%s): %s", node.label, node.host, exc)
            return self._build_error_snapshot(node, str(exc))

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
                cwd=str(raw_proc.get("cwd", "")),
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
            gpu.is_busy = is_gpu_busy(gpu)
            gpus.append(gpu)

        runs = self._build_runs(node, raw, gpu_processes)
        external_queue_raw = dict(raw.get("external_queue") or {})
        external_queue = self._build_external_queue_items(external_queue_raw)
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
            external_queue=external_queue,
            external_queue_source=str(external_queue_raw.get("source", "") or ""),
            external_queue_error=str(external_queue_raw.get("error", "") or ""),
        )

    def _effective_run_configs(self, node: NodeConfig, raw: Dict[str, Any]) -> List[RunConfig]:
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

    def _build_external_queue_items(self, payload: Dict[str, Any]) -> List[ExternalQueueItem]:
        items: List[ExternalQueueItem] = []
        sort_order = {"queued": 0, "starting": 1, "running": 2, "unknown": 3}
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
        return sorted(items, key=lambda item: (sort_order.get(item.status, 9), item.submitted_at or "", item.id))

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

    def _as_int(self, value: Any) -> Optional[int]:
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _select_gpu_processes_for_run(
        self,
        matched_processes: List[Dict[str, Any]],
        gpu_processes: List[GPUProcess],
    ) -> List[GPUProcess]:
        selected: Dict[Tuple[str, Optional[int], Optional[int]], GPUProcess] = {}
        gpu_processes_by_pid: Dict[int, List[GPUProcess]] = {}
        for process in gpu_processes:
            pid = self._as_int(process.pid)
            if pid is None:
                continue
            gpu_processes_by_pid.setdefault(pid, []).append(process)

        for matched in matched_processes:
            pid = self._as_int(matched.get("pid"))
            if pid is None:
                continue
            for process in gpu_processes_by_pid.get(pid, []):
                selected[(process.gpu_uuid, process.pid, process.gpu_index)] = process

        for matched in matched_processes:
            pid = self._as_int(matched.get("pid"))
            if pid is not None and gpu_processes_by_pid.get(pid):
                continue
            cwd = str(matched.get("cwd", "") or "").strip()
            signature = command_signature(str(matched.get("command", "")))
            elapsed_seconds = self._as_int(matched.get("elapsed_seconds"))
            if not cwd or not signature:
                continue
            for process in gpu_processes:
                process_cwd = str(process.cwd or "").strip()
                if process_cwd != cwd:
                    continue
                if command_signature(process.command) != signature:
                    continue
                process_elapsed = self._as_int(process.elapsed_seconds)
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

    def _build_run_gpu_usage(
        self,
        matched_processes: List[Dict[str, Any]],
        gpu_processes: List[GPUProcess],
    ) -> Dict[str, Any]:
        selected_processes = self._select_gpu_processes_for_run(matched_processes, gpu_processes)
        gpu_indices = sorted({int(item.gpu_index) for item in selected_processes if item.gpu_index is not None})
        memory_values = [float(item.used_gpu_memory_mb) for item in selected_processes if item.used_gpu_memory_mb is not None]
        return {
            "gpu_indices": gpu_indices,
            "gpu_memory_used_mb": float(sum(memory_values)) if memory_values else None,
        }

    def _build_runs(self, node: NodeConfig, raw: Dict[str, Any], gpu_processes: List[GPUProcess]) -> List[RunSnapshot]:
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
            activity = derive_run_activity(parsed, matched_processes, collected_at, status)
            gpu_usage = self._build_run_gpu_usage(matched_processes, gpu_processes)
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
        external_items = [item for node in nodes for item in node.external_queue]
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
            "gpus_busy": count_busy_gpus(gpus),
            "external_queue_total": len(external_items),
            "external_queue_queued": sum(1 for item in external_items if item.status == "queued"),
            "external_queue_starting": sum(1 for item in external_items if item.status == "starting"),
            "external_queue_running": sum(1 for item in external_items if item.status == "running"),
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
