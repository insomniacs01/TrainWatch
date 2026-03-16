import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .collector_runs import build_external_queue_items, build_node_metrics, build_runs
from .config import AppConfig, NodeConfig
from .gpu_utils import is_gpu_busy
from .mock_data import build_mock_raw
from .models import AlertEvent, AppSnapshot, GPUInfo, GPUProcess, NodeSnapshot, RunSnapshot
from .remote_probe import build_remote_probe_command
from .runtime_views import build_nodes_summary
from .ssh_pool import ParamikoConnectionPool
from .time_utils import utc_now_iso

logger = logging.getLogger(__name__)


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
        except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
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

        runs = build_runs(node, raw, gpu_processes)
        external_queue_raw = dict(raw.get("external_queue") or {})
        external_queue = build_external_queue_items(external_queue_raw)
        metrics = build_node_metrics(raw, gpus)
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

    async def poll_once(
        self,
        previous_snapshot: Optional[AppSnapshot],
        nodes: Optional[List[NodeConfig]] = None,
    ) -> Tuple[AppSnapshot, List[AlertEvent]]:
        del previous_snapshot
        active_nodes = nodes if nodes is not None else self.config.nodes
        snapshots = await asyncio.gather(*[asyncio.to_thread(self.collect_node, node) for node in active_nodes])
        snapshot = AppSnapshot(generated_at=utc_now_iso(), summary=build_nodes_summary(snapshots), nodes=snapshots)
        return snapshot, []
