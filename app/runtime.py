import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .collector import Collector
from .config import AppConfig, NodeConfig
from .models import AlertEvent, AppSnapshot, NodeSnapshot, RunSnapshot
from .storage import SQLiteStore


def empty_snapshot() -> AppSnapshot:
    return AppSnapshot(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        summary={
            "nodes_total": 0,
            "nodes_online": 0,
            "nodes_degraded": 0,
            "nodes_offline": 0,
            "runs_total": 0,
            "runs_running": 0,
            "runs_alerting": 0,
            "gpus_total": 0,
            "gpus_busy": 0,
        },
        nodes=[],
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WebSocketHub:
    def __init__(self) -> None:
        self.connections = set()

    async def connect(self, websocket: Any) -> None:
        await websocket.accept()
        self.connections.add(websocket)

    def disconnect(self, websocket: Any) -> None:
        self.connections.discard(websocket)

    async def broadcast(self, payload: Dict[str, Any]) -> None:
        stale = []
        for websocket in list(self.connections):
            try:
                await websocket.send_json(payload)
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.disconnect(websocket)


class TrainWatchRuntime:
    def __init__(self, config: AppConfig, collector: Optional[Collector] = None) -> None:
        self.config = config
        self.collector = collector or Collector(config)
        self.store = SQLiteStore(config.server.sqlite_path, config.server.retention_days)
        self.hub = WebSocketHub()
        self.snapshot = empty_snapshot()
        self.recent_events: List[AlertEvent] = []
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._lock: Optional[asyncio.Lock] = None

    def _ensure_async_state(self) -> Tuple[asyncio.Lock, asyncio.Event]:
        if self._lock is None:
            self._lock = asyncio.Lock()
        if self._stop_event is None or self._stop_event.is_set():
            self._stop_event = asyncio.Event()
        return self._lock, self._stop_event

    async def start(self) -> None:
        self._ensure_async_state()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None
        self.collector.close()

    async def _poll_loop(self) -> None:
        _lock, stop_event = self._ensure_async_state()
        while not stop_event.is_set():
            try:
                await self.refresh_once()
            except Exception as exc:
                await self.hub.broadcast(
                    {
                        "type": "error",
                        "error": str(exc),
                        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.server.poll_seconds)
            except asyncio.TimeoutError:
                continue

    def _matching_connection(self, node: NodeConfig) -> Optional[NodeConfig]:
        def normalized(value: str) -> str:
            return (value or "").strip().lower()

        for existing in self.config.nodes:
            if existing.transport != node.transport:
                continue
            if normalized(existing.host) != normalized(node.host):
                continue
            if int(existing.port) != int(node.port):
                continue
            if normalized(existing.user) != normalized(node.user):
                continue
            return existing
        return None

    def _connecting_run_snapshots(self, node: NodeConfig) -> List[RunSnapshot]:
        return [
            RunSnapshot(
                id=run.id,
                label=run.label,
                parser=run.parser,
                status="connecting",
                error="Waiting for first SSH poll",
                log_path=run.log_path or run.log_glob or "",
                log_exists=False,
                log_age_seconds=None,
                last_update_at="",
                last_log_line="",
            )
            for run in node.runs
        ]

    def _placeholder_snapshot_for_node(self, node: NodeConfig) -> NodeSnapshot:
        return NodeSnapshot(
            id=node.id,
            label=node.label,
            host=node.host,
            hostname=node.host,
            status="connecting",
            error="正在建立 SSH 连接并等待首轮采集...",
            collected_at=_now_iso(),
            loadavg=[],
            metrics={},
            gpus=[],
            gpu_processes=[],
            runs=self._connecting_run_snapshots(node),
        )

    def _summary_for_nodes(self, nodes: List[NodeSnapshot]) -> Dict[str, Any]:
        runs = [run for node in nodes for run in node.runs]
        gpus = [gpu for node in nodes for gpu in node.gpus]
        cpu_values = [float(node.metrics.get("cpu_usage_percent", 0.0)) for node in nodes if node.metrics]
        memory_percent_values = [float(node.metrics.get("memory_used_percent", 0.0)) for node in nodes if node.metrics]
        disk_percent_values = [float(node.metrics.get("disk_used_percent", 0.0)) for node in nodes if node.metrics]
        memory_used_values = [float(node.metrics.get("memory_used_mb", 0.0)) for node in nodes if node.metrics]
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

    def _set_placeholder_node(self, node: NodeConfig) -> None:
        placeholder = self._placeholder_snapshot_for_node(node)
        current_nodes = [item for item in self.snapshot.nodes if item.id != node.id]
        current_nodes.append(placeholder)
        self.snapshot = AppSnapshot(
            generated_at=_now_iso(),
            summary=self._summary_for_nodes(current_nodes),
            nodes=current_nodes,
        )

    async def _broadcast_snapshot(self) -> None:
        await self.hub.broadcast({
            "type": "snapshot",
            "snapshot": self.snapshot.to_dict(),
            "events": [],
        })

    async def refresh_once(self) -> Dict[str, Any]:
        lock, _stop_event = self._ensure_async_state()
        async with lock:
            if not self.config.nodes:
                self.snapshot = empty_snapshot()
                payload = {"type": "snapshot", "snapshot": self.snapshot.to_dict(), "events": []}
            else:
                snapshot, events = await self.collector.poll_once(
                    self.snapshot if self.snapshot.nodes else None,
                    self.config.nodes,
                )
                self.snapshot = snapshot
                self.recent_events = (events + self.recent_events)[:20]
                self.store.persist_snapshot(snapshot)
                payload = {
                    "type": "snapshot",
                    "snapshot": snapshot.to_dict(),
                    "events": [event.to_dict() for event in events],
                }
        await self.hub.broadcast(payload)
        return payload

    async def add_node(self, node: NodeConfig) -> Dict[str, Any]:
        lock, _stop_event = self._ensure_async_state()
        async with lock:
            if any(existing.id == node.id for existing in self.config.nodes):
                raise ValueError(f"Node id already exists: {node.id}")
            duplicate = self._matching_connection(node)
            if duplicate is not None:
                raise ValueError(f"Connection already exists: {duplicate.label} ({duplicate.host})")
            self.config.nodes.append(node)
            self._set_placeholder_node(node)
        await self._broadcast_snapshot()
        asyncio.create_task(self.refresh_once())
        return self.connection_summaries()[-1]

    async def remove_node(self, node_id: str) -> bool:
        lock, _stop_event = self._ensure_async_state()
        removed: Optional[NodeConfig] = None
        async with lock:
            remaining = []
            for node in self.config.nodes:
                if node.id == node_id:
                    removed = node
                else:
                    remaining.append(node)
            self.config.nodes = remaining
        if removed is None:
            return False
        if removed.transport == "ssh" and hasattr(self.collector, "pool"):
            try:
                self.collector.pool.close_node(removed)
            except Exception:
                pass
        await self.refresh_once()
        return True

    def connection_summaries(self) -> List[Dict[str, Any]]:
        status_by_id = {node.id: node.status for node in self.snapshot.nodes}
        runs_by_id = {node.id: len(node.runs) for node in self.snapshot.nodes}
        return [
            {
                "id": node.id,
                "label": node.label,
                "host": node.host,
                "port": node.port,
                "user": node.user,
                "transport": node.transport,
                "runs": runs_by_id.get(node.id, len(node.runs)),
                "status": status_by_id.get(node.id, "unknown"),
            }
            for node in self.config.nodes
        ]

    def snapshot_dict(self) -> Dict[str, Any]:
        if not self.snapshot.nodes and self.config.nodes:
            latest = self.store.latest_snapshot()
            if latest:
                return latest
        return self.snapshot.to_dict()

    def history_range_defaults(self) -> Dict[str, str]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=6)
        return {
            "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
