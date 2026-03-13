import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .collector import Collector
from .config import AppConfig, NodeConfig, node_from_persisted_dict, node_to_dict
from .job_queue import (
    ACTIVE_QUEUE_STATUSES,
    LAUNCHED_QUEUE_STATUSES,
    TERMINAL_QUEUE_STATUSES,
    build_remote_launch_command,
    build_run_config,
    queue_job_from_dict,
    queue_summary,
    select_free_gpu_indices,
    utc_now_iso,
)
from .models import AlertEvent, AppSnapshot, NodeSnapshot, QueueJob, RunSnapshot
from .storage import SQLiteStore


QUEUE_START_TIMEOUT_SECONDS = 180


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
            "external_queue_total": 0,
            "external_queue_queued": 0,
            "external_queue_starting": 0,
            "external_queue_running": 0,
        },
        nodes=[],
    )


class WebSocketHub:
    def __init__(self) -> None:
        self.connections = set()

    async def connect(self, websocket: Any, already_accepted: bool = False) -> None:
        if not already_accepted:
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
        self._persisted_node_ids: set = set()
        self._restore_persisted_nodes()
        self.queue_jobs: List[QueueJob] = [queue_job_from_dict(item) for item in self.store.list_queue_jobs()]
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._lock: Optional[asyncio.Lock] = None
        self._restore_launched_queue_runs()

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
            collected_at=utc_now_iso(),
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
            generated_at=utc_now_iso(),
            summary=self._summary_for_nodes(current_nodes),
            nodes=current_nodes,
        )

    def find_node(self, node_id: str) -> Optional[NodeConfig]:
        for node in self.config.nodes:
            if node.id == node_id:
                return node
        return None

    def _find_snapshot_node(self, node_id: str, snapshot: Optional[AppSnapshot] = None) -> Optional[NodeSnapshot]:
        target_snapshot = snapshot or self.snapshot
        for node in target_snapshot.nodes:
            if node.id == node_id:
                return node
        return None

    def _find_queue_job(self, job_id: str) -> Optional[QueueJob]:
        for job in self.queue_jobs:
            if job.id == job_id:
                return job
        return None

    def _persist_queue_job(self, job: QueueJob) -> None:
        self.store.upsert_queue_job(job.to_dict())

    def _persist_node(self, node: NodeConfig) -> None:
        self.store.upsert_persisted_node(
            node_to_dict(node, include_password=self.config.server.persist_passwords)
        )
        self._persisted_node_ids.add(node.id)

    def _restore_persisted_nodes(self) -> None:
        for payload in self.store.list_persisted_nodes():
            node_id = str(payload.get("id", "")).strip()
            try:
                node = node_from_persisted_dict(payload)
            except Exception:
                if node_id:
                    self.store.delete_persisted_node(node_id)
                continue
            if not self.config.server.persist_passwords and node.password:
                self.store.upsert_persisted_node(node_to_dict(node, include_password=False))
                node.password = ""
                node.needs_password = True
            if any(existing.id == node.id for existing in self.config.nodes):
                self.store.delete_persisted_node(node.id)
                continue
            duplicate = self._matching_connection(node)
            if duplicate is not None:
                self.store.delete_persisted_node(node.id)
                continue
            self.config.nodes.append(node)
            self._persisted_node_ids.add(node.id)

    def _restore_launched_queue_runs(self) -> None:
        for job in self.queue_jobs:
            if job.status in LAUNCHED_QUEUE_STATUSES:
                self._attach_job_run(job)

    def _attach_job_run(self, job: QueueJob) -> None:
        node = self.find_node(job.node_id)
        if node is None:
            return
        run_cfg = build_run_config(job)
        job.run_id = run_cfg.id
        job.process_match = run_cfg.process_match
        existing = [run for run in node.runs if run.id != run_cfg.id]
        existing.append(run_cfg)
        node.runs = existing

    def _detach_job_run(self, job: QueueJob) -> None:
        node = self.find_node(job.node_id)
        if node is None or not job.run_id:
            return
        node.runs = [run for run in node.runs if run.id != job.run_id]

    def _parse_iso(self, value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    def _seconds_since(self, value: str, now_value: str) -> Optional[int]:
        start = self._parse_iso(value)
        end = self._parse_iso(now_value)
        if start is None or end is None:
            return None
        return max(0, int((end - start).total_seconds()))

    def _run_snapshot_for_job(self, snapshot: AppSnapshot, job: QueueJob) -> Optional[RunSnapshot]:
        if not job.run_id:
            return None
        node_snapshot = self._find_snapshot_node(job.node_id, snapshot)
        if node_snapshot is None:
            return None
        for run in node_snapshot.runs:
            if run.id == job.run_id:
                return run
        return None

    def _queue_jobs_by_node(self, statuses: Optional[set] = None) -> Dict[str, List[QueueJob]]:
        grouped: Dict[str, List[QueueJob]] = {}
        for job in self.queue_jobs:
            if statuses is not None and job.status not in statuses:
                continue
            grouped.setdefault(job.node_id, []).append(job)
        for items in grouped.values():
            items.sort(key=lambda item: (item.created_at, item.id))
        return grouped

    async def _broadcast_snapshot(self) -> None:
        await self.hub.broadcast(
            {
                "type": "snapshot",
                "snapshot": self.snapshot.to_dict(),
                "events": [],
            }
        )

    async def _launch_queue_job(self, job: QueueJob, node: NodeConfig, gpu_indices: List[int], launched_at: str) -> None:
        if not hasattr(self.collector, "pool"):
            raise RuntimeError("Queue launching is unavailable in the current collector")
        command = build_remote_launch_command(job, gpu_indices)
        output, error, code = await asyncio.to_thread(self.collector.pool.execute, node, command, 45)
        if code != 0:
            raise RuntimeError(error.strip() or output.strip() or "Failed to launch queued job")
        payload = json.loads(output or "{}")
        job.status = "starting"
        job.run_status = ""
        job.started_at = launched_at
        job.updated_at = launched_at
        job.finished_at = ""
        job.allocated_gpu_indices = [int(item) for item in gpu_indices]
        job.remote_pid = int(payload["remote_pid"]) if payload.get("remote_pid") is not None else None
        job.script_path = str(payload.get("script_path", "") or "")
        job.log_path = str(payload.get("log_path", "") or "")
        job.process_match = build_run_config(job).process_match
        job.run_id = build_run_config(job).id
        job.error = "Waiting for first poll after launch"
        self._attach_job_run(job)
        self._persist_queue_job(job)

    def _reconcile_queue_job_states(self, snapshot: AppSnapshot) -> None:
        for job in self.queue_jobs:
            if job.status not in LAUNCHED_QUEUE_STATUSES:
                continue
            node_snapshot = self._find_snapshot_node(job.node_id, snapshot)
            run_snapshot = self._run_snapshot_for_job(snapshot, job)
            if node_snapshot is None:
                continue
            if run_snapshot is None:
                age_seconds = self._seconds_since(job.started_at or job.updated_at, snapshot.generated_at)
                if node_snapshot.status != "offline" and age_seconds is not None and age_seconds >= QUEUE_START_TIMEOUT_SECONDS:
                    job.status = "failed"
                    job.run_status = "unknown"
                    job.finished_at = snapshot.generated_at
                    job.updated_at = snapshot.generated_at
                    job.error = "Queued job did not appear in monitoring within the startup timeout"
                    self._detach_job_run(job)
                    self._persist_queue_job(job)
                continue

            job.run_status = run_snapshot.status
            job.updated_at = snapshot.generated_at
            if run_snapshot.status in {"running", "stalled"}:
                job.status = "running"
                job.error = run_snapshot.error or ("Job log looks stalled" if run_snapshot.status == "stalled" else "")
                self._persist_queue_job(job)
                continue
            if run_snapshot.status == "completed":
                job.status = "completed"
                job.finished_at = snapshot.generated_at
                job.error = ""
                self._detach_job_run(job)
                self._persist_queue_job(job)
                continue
            if run_snapshot.status == "failed":
                job.status = "failed"
                job.finished_at = snapshot.generated_at
                job.error = run_snapshot.error or "Queued job failed"
                self._detach_job_run(job)
                self._persist_queue_job(job)
                continue
            if run_snapshot.status == "idle":
                job.status = "failed"
                job.finished_at = snapshot.generated_at
                job.error = run_snapshot.error or "Queued job exited without a completion marker"
                self._detach_job_run(job)
                self._persist_queue_job(job)
                continue
            if run_snapshot.status == "unknown":
                age_seconds = self._seconds_since(job.started_at or job.updated_at, snapshot.generated_at)
                if node_snapshot.status != "offline" and age_seconds is not None and age_seconds >= QUEUE_START_TIMEOUT_SECONDS:
                    job.status = "failed"
                    job.finished_at = snapshot.generated_at
                    job.error = run_snapshot.error or "Queued job became unreachable during startup"
                    self._detach_job_run(job)
                    self._persist_queue_job(job)
                    continue
                job.error = run_snapshot.error or job.error
                self._persist_queue_job(job)

    async def _schedule_pending_queue_jobs(self, snapshot: AppSnapshot) -> None:
        if not hasattr(self.collector, "pool"):
            return
        queued_by_node = self._queue_jobs_by_node(statuses={"queued"})
        active_by_node = self._queue_jobs_by_node(statuses=LAUNCHED_QUEUE_STATUSES)
        for node in self.config.nodes:
            if node.transport != "ssh":
                continue
            node_snapshot = self._find_snapshot_node(node.id, snapshot)
            if node_snapshot is None or node_snapshot.status == "offline" or not node_snapshot.gpus:
                continue
            reserved = [gpu_index for job in active_by_node.get(node.id, []) for gpu_index in job.allocated_gpu_indices]
            free_gpu_indices = select_free_gpu_indices(node_snapshot, reserved)
            for job in queued_by_node.get(node.id, []):
                if len(free_gpu_indices) < job.gpu_count:
                    break
                allocated = free_gpu_indices[: job.gpu_count]
                try:
                    await self._launch_queue_job(job, node, allocated, snapshot.generated_at)
                except Exception as exc:
                    job.updated_at = snapshot.generated_at
                    job.error = str(exc)
                    self._persist_queue_job(job)
                    break
                free_gpu_indices = free_gpu_indices[job.gpu_count :]

    async def _sync_queue_jobs(self, snapshot: AppSnapshot) -> None:
        self._reconcile_queue_job_states(snapshot)
        await self._schedule_pending_queue_jobs(snapshot)

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
                await self._sync_queue_jobs(snapshot)
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
            self._persist_node(node)
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
            for job in self.queue_jobs:
                if job.node_id != node_id or job.status in TERMINAL_QUEUE_STATUSES:
                    continue
                job.status = "canceled"
                job.run_status = job.run_status or "canceled"
                job.finished_at = utc_now_iso()
                job.updated_at = job.finished_at
                job.error = "Connection removed before queued job could finish"
                self._detach_job_run(job)
                self._persist_queue_job(job)
        if removed is None:
            return False
        if removed.id in self._persisted_node_ids:
            self.store.delete_persisted_node(removed.id)
            self._persisted_node_ids.discard(removed.id)
        if removed.transport == "ssh" and hasattr(self.collector, "pool"):
            try:
                self.collector.pool.close_node(removed)
            except Exception:
                pass
        await self.refresh_once()
        return True

    async def enqueue_job(self, job: QueueJob) -> Dict[str, Any]:
        lock, _stop_event = self._ensure_async_state()
        async with lock:
            node = self.find_node(job.node_id)
            if node is None:
                raise ValueError("Target connection was not found")
            if node.transport != "ssh":
                raise ValueError("Queued jobs currently require an SSH connection")
            node_snapshot = self._find_snapshot_node(node.id)
            if node_snapshot is not None and node_snapshot.gpus and job.gpu_count > len(node_snapshot.gpus):
                raise ValueError("Requested GPU count exceeds the GPUs visible on this node")
            job.node_label = node.label
            self.queue_jobs.append(job)
            self.queue_jobs.sort(key=lambda item: (item.created_at, item.id))
            self._persist_queue_job(job)
            queue_positions = {}
            grouped = self._queue_jobs_by_node(statuses={"queued"})
            for group_items in grouped.values():
                for index, queued_job in enumerate(group_items, start=1):
                    queue_positions[queued_job.id] = index
            item = self._job_item(job, queue_positions)
        asyncio.create_task(self.refresh_once())
        return item

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        lock, _stop_event = self._ensure_async_state()
        async with lock:
            job = self._find_queue_job(job_id)
            if job is None:
                raise ValueError("Queued job was not found")
            if job.status != "queued":
                raise ValueError("Only queued jobs can be canceled right now")
            job.status = "canceled"
            job.run_status = "canceled"
            job.finished_at = utc_now_iso()
            job.updated_at = job.finished_at
            job.error = "Canceled before launch"
            self._persist_queue_job(job)
            item = self._job_item(job)
        return item

    def _job_item(self, job: QueueJob, queue_positions: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        item = job.to_dict()
        position = None
        if queue_positions is not None and job.status == "queued":
            position = queue_positions.get(job.id)
        item["queue_position"] = position
        item["can_cancel"] = job.status == "queued"
        return item

    def _external_job_items(self, node_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sort_order = {"queued": 0, "starting": 1, "running": 2, "unknown": 3, "failed": 4, "completed": 5, "canceled": 6}
        items: List[Dict[str, Any]] = []
        for node in self.snapshot.nodes:
            if node_id is not None and node.id != node_id:
                continue
            for external_job in node.external_queue:
                payload = external_job.to_dict()
                payload["node_id"] = node.id
                payload["node_label"] = node.label
                payload["can_cancel"] = False
                items.append(payload)
        return sorted(items, key=lambda item: (sort_order.get(str(item.get("status", "unknown")), 9), str(item.get("submitted_at", "")), str(item.get("id", ""))))

    def _external_queue_summary(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        gpu_requested_active = 0
        for item in items:
            if item.get("status") not in {"queued", "starting", "running"}:
                continue
            try:
                if item.get("gpu_count") is not None:
                    gpu_requested_active += int(item.get("gpu_count") or 0)
            except (TypeError, ValueError):
                continue
        return {
            "jobs_total": len(items),
            "jobs_queued": sum(1 for item in items if item.get("status") == "queued"),
            "jobs_starting": sum(1 for item in items if item.get("status") == "starting"),
            "jobs_running": sum(1 for item in items if item.get("status") == "running"),
            "jobs_failed": sum(1 for item in items if item.get("status") == "failed"),
            "jobs_completed": sum(1 for item in items if item.get("status") == "completed"),
            "jobs_canceled": sum(1 for item in items if item.get("status") == "canceled"),
            "gpu_requested_active": gpu_requested_active,
        }

    def job_summaries(self, node_id: Optional[str] = None) -> Dict[str, Any]:
        items = [job for job in self.queue_jobs if node_id is None or job.node_id == node_id]
        sort_order = {"running": 0, "starting": 1, "queued": 2, "failed": 3, "completed": 4, "canceled": 5}
        items = sorted(items, key=lambda job: (sort_order.get(job.status, 9), job.created_at, job.id))
        queue_positions: Dict[str, int] = {}
        grouped = self._queue_jobs_by_node(statuses={"queued"})
        for group_items in grouped.values():
            for index, job in enumerate(group_items, start=1):
                queue_positions[job.id] = index
        external_items = self._external_job_items(node_id=node_id)
        return {
            "summary": queue_summary(items),
            "items": [self._job_item(job, queue_positions) for job in items],
            "external_summary": self._external_queue_summary(external_items),
            "external_items": external_items,
        }

    def connection_summaries(self) -> List[Dict[str, Any]]:
        status_by_id = {node.id: node.status for node in self.snapshot.nodes}
        runs_by_id = {node.id: len(node.runs) for node in self.snapshot.nodes}
        jobs_by_id: Dict[str, int] = {}
        for job in self.queue_jobs:
            if job.status in ACTIVE_QUEUE_STATUSES:
                jobs_by_id[job.node_id] = jobs_by_id.get(job.node_id, 0) + 1
        return [
            {
                "id": node.id,
                "label": node.label,
                "host": node.host,
                "port": node.port,
                "user": node.user,
                "transport": node.transport,
                "runs": runs_by_id.get(node.id, len(node.runs)),
                "jobs": jobs_by_id.get(node.id, 0),
                "status": status_by_id.get(node.id, "unknown"),
                "has_key_path": bool(node.key_path),
                "has_password": bool(node.password),
                "needs_password": bool(node.needs_password),
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
