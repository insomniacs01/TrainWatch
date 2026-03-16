import asyncio
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

from .config import NodeConfig, node_from_persisted_dict, node_to_dict
from .job_queue import ACTIVE_QUEUE_STATUSES, TERMINAL_QUEUE_STATUSES, utc_now_iso
from .models import AppSnapshot, NodeSnapshot, RunSnapshot
from .runtime_views import build_nodes_summary
from .time_utils import coerce_utc_timestamp

logger = logging.getLogger(__name__)
SSH_OFFLINE_FAILURE_THRESHOLD = 2
IMMEDIATE_OFFLINE_ERROR_MARKERS = (
    "password was not persisted",
    "password auth is not supported",
)


class RuntimeConnectionsMixin:
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
            error="Establishing SSH connection and waiting for first poll...",
            collected_at=utc_now_iso(),
            loadavg=[],
            metrics={},
            gpus=[],
            gpu_processes=[],
            runs=self._connecting_run_snapshots(node),
        )

    def _summary_for_nodes(self, nodes: List[NodeSnapshot]) -> Dict[str, Any]:
        return build_nodes_summary(nodes)

    def _is_immediate_offline_error(self, error: str) -> bool:
        normalized = (error or "").strip().lower()
        return any(marker in normalized for marker in IMMEDIATE_OFFLINE_ERROR_MARKERS)

    def _stabilize_node_snapshot(
        self,
        current_node: NodeSnapshot,
        previous_node: Optional[NodeSnapshot],
    ) -> NodeSnapshot:
        if current_node.status != "offline":
            self._node_consecutive_ssh_failures.pop(current_node.id, None)
            return current_node

        if (
            previous_node is None
            or previous_node.status in {"offline", "connecting"}
            or self._is_immediate_offline_error(current_node.error)
        ):
            return current_node

        failures = self._node_consecutive_ssh_failures.get(current_node.id, 0) + 1
        self._node_consecutive_ssh_failures[current_node.id] = failures
        if failures >= SSH_OFFLINE_FAILURE_THRESHOLD:
            return current_node

        preserved = deepcopy(previous_node)
        preserved.status = "degraded"
        preserved.error = "SSH polling temporarily failed; 保留上次成功数据 (failure %s): %s" % (
            failures,
            current_node.error or "Remote connection interrupted briefly",
        )
        return preserved

    def _stabilize_snapshot(
        self,
        current_snapshot: AppSnapshot,
        previous_snapshot: Optional[AppSnapshot],
    ) -> AppSnapshot:
        previous_nodes = {node.id: node for node in previous_snapshot.nodes} if previous_snapshot else {}
        stabilized_nodes = [
            self._stabilize_node_snapshot(node, previous_nodes.get(node.id)) for node in current_snapshot.nodes
        ]
        active_node_ids = {node.id for node in current_snapshot.nodes}
        stale_node_ids = [node_id for node_id in self._node_consecutive_ssh_failures if node_id not in active_node_ids]
        for node_id in stale_node_ids:
            self._node_consecutive_ssh_failures.pop(node_id, None)
        return AppSnapshot(
            generated_at=current_snapshot.generated_at,
            summary=self._summary_for_nodes(stabilized_nodes),
            nodes=stabilized_nodes,
        )

    def _set_placeholder_node(self, node: NodeConfig) -> None:
        placeholder = self._placeholder_snapshot_for_node(node)
        current_nodes = [item for item in self.snapshot.nodes if item.id != node.id]
        current_nodes.append(placeholder)
        self.snapshot = AppSnapshot(
            generated_at=utc_now_iso(),
            summary=self._summary_for_nodes(current_nodes),
            nodes=current_nodes,
        )

    def _rebuild_snapshot_from_config(self) -> None:
        if not self.config.nodes:
            self.snapshot = AppSnapshot(generated_at=utc_now_iso(), summary=self._summary_for_nodes([]), nodes=[])
            self.current_alerts = []
            return

        current_nodes = {item.id: item for item in self.snapshot.nodes}
        next_nodes = [
            current_nodes.get(node.id) or self._placeholder_snapshot_for_node(node) for node in self.config.nodes
        ]
        self.snapshot = AppSnapshot(
            generated_at=utc_now_iso(),
            summary=self._summary_for_nodes(next_nodes),
            nodes=next_nodes,
        )
        self.current_alerts = self._build_current_alerts(self.snapshot)

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

    def _persist_node(self, node: NodeConfig) -> None:
        self.store.upsert_persisted_node(node_to_dict(node, include_password=self.config.server.persist_passwords))
        self._persisted_node_ids.add(node.id)

    def _restore_persisted_nodes(self) -> None:
        for payload in self.store.list_persisted_nodes():
            node_id = str(payload.get("id", "")).strip()
            try:
                node = node_from_persisted_dict(payload)
            except (TypeError, ValueError, KeyError):
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

    async def _broadcast_snapshot(self) -> None:
        await self.hub.broadcast(
            {
                "type": "snapshot",
                "snapshot": self.snapshot_dict(),
                "events": [],
            }
        )

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
            logger.info("Connection added: id=%s label=%s host=%s", node.id, node.label, node.host)
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
            if removed is not None:
                self._node_consecutive_ssh_failures.pop(node_id, None)
                self.recent_events = [event for event in self.recent_events if event.node_id != node_id][:20]
                self._rebuild_snapshot_from_config()
                self.store.persist_snapshot(self.snapshot)
        if removed is None:
            return False
        logger.info("Connection removed: id=%s label=%s host=%s", removed.id, removed.label, removed.host)
        if removed.id in self._persisted_node_ids:
            self.store.delete_persisted_node(removed.id)
            self._persisted_node_ids.discard(removed.id)
        if removed.transport == "ssh" and hasattr(self.collector, "pool"):
            asyncio.create_task(asyncio.to_thread(self.collector.pool.close_node, removed))
        await self._broadcast_snapshot()
        asyncio.create_task(self.refresh_once())
        return True

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
                payload = dict(latest)
                payload["recent_events"] = [event.to_dict() for event in self.recent_events]
                payload["current_alerts"] = list(self.current_alerts)
                return payload
        payload = self.snapshot.to_dict()
        payload["recent_events"] = [event.to_dict() for event in self.recent_events]
        payload["current_alerts"] = list(self.current_alerts)
        return payload

    def history_range_defaults(self) -> Dict[str, str]:
        return {
            "from": coerce_utc_timestamp(None, default_delta_hours=6),
            "to": coerce_utc_timestamp(None, default_delta_hours=None),
        }
