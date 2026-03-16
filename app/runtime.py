import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from starlette.websockets import WebSocketDisconnect

from .auth import AuthManager
from .collector import Collector
from .config import AppConfig, finalize_server_config
from .job_queue import queue_job_from_dict, utc_now_iso
from .models import AlertEvent, AppSnapshot
from .runtime_alerts import build_current_alerts, diff_runtime_events
from .runtime_connections import RuntimeConnectionsMixin
from .runtime_queue import RuntimeQueueMixin
from .runtime_views import build_nodes_summary
from .storage import SQLiteStore

logger = logging.getLogger(__name__)


def empty_snapshot() -> AppSnapshot:
    return AppSnapshot(
        generated_at=utc_now_iso(),
        summary=build_nodes_summary([]),
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
            except (RuntimeError, OSError, WebSocketDisconnect):
                stale.append(websocket)
        for websocket in stale:
            self.disconnect(websocket)


class TrainWatchRuntime(RuntimeConnectionsMixin, RuntimeQueueMixin):
    def __init__(self, config: AppConfig, collector: Optional[Collector] = None) -> None:
        self.config = config
        self.config.server = finalize_server_config(self.config.server)
        self.collector = collector or Collector(config)
        self.store = SQLiteStore(config.server.sqlite_path, config.server.retention_days)
        self.auth = AuthManager(self.store, self.config.server)
        self.hub = WebSocketHub()
        self.snapshot = empty_snapshot()
        self.recent_events: List[AlertEvent] = []
        self.current_alerts: List[Dict[str, Any]] = []
        self._node_consecutive_ssh_failures: Dict[str, int] = {}
        self._persisted_node_ids: set = set()
        self._restore_persisted_nodes()
        self.queue_jobs = [queue_job_from_dict(item) for item in self.store.list_queue_jobs()]
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
            except asyncio.CancelledError:
                raise
            except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
                logger.exception("Background refresh failed")
                await self.hub.broadcast(
                    {
                        "type": "error",
                        "error": str(exc),
                        "at": utc_now_iso(),
                    }
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.server.poll_seconds)
            except asyncio.TimeoutError:
                continue

    def _diff_events_v2(
        self,
        previous_snapshot: Optional[AppSnapshot],
        current_snapshot: AppSnapshot,
    ) -> List[AlertEvent]:
        return diff_runtime_events(previous_snapshot, current_snapshot)

    def _build_current_alerts(self, snapshot: AppSnapshot) -> List[Dict[str, Any]]:
        return build_current_alerts(snapshot, self.config.server)

    def _persist_events(self, events: List[AlertEvent]) -> None:
        for event in events:
            self.store.add_alert_event(event)

    def add_audit_log(
        self,
        username: str,
        action: str,
        target_type: str,
        target_id: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.store.add_audit_log(
            log_id=f"audit-{uuid4().hex}",
            at=utc_now_iso(),
            username=username,
            action=action,
            target_type=target_type,
            target_id=target_id,
            message=message,
            details=details,
        )

    def list_alert_events(self, limit: int = 100, acknowledged: Optional[bool] = None) -> List[Dict[str, Any]]:
        return self.store.list_alert_events(limit=limit, acknowledged=acknowledged)

    def acknowledge_alert_event(self, event_id: str, username: str) -> Optional[Dict[str, Any]]:
        return self.store.acknowledge_alert_event(event_id, username)

    def list_audit_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.store.list_audit_logs(limit=limit)

    async def refresh_once(self) -> Dict[str, Any]:
        lock, _stop_event = self._ensure_async_state()
        async with lock:
            if not self.config.nodes:
                self.snapshot = empty_snapshot()
                self.current_alerts = []
                payload = {"type": "snapshot", "snapshot": self.snapshot_dict(), "events": []}
            else:
                previous_snapshot = self.snapshot if self.snapshot.nodes else None
                snapshot, _events = await self.collector.poll_once(previous_snapshot, self.config.nodes)
                snapshot = self._stabilize_snapshot(snapshot, previous_snapshot)
                events = self._diff_events_v2(previous_snapshot, snapshot)
                self.snapshot = snapshot
                self.current_alerts = self._build_current_alerts(snapshot)
                self.recent_events = (events + self.recent_events)[:20]
                self.store.persist_snapshot(snapshot)
                self._persist_events(events)
                await self._sync_queue_jobs(snapshot)
                payload = {
                    "type": "snapshot",
                    "snapshot": self.snapshot_dict(),
                    "events": [event.to_dict() for event in events],
                }
        await self.hub.broadcast(payload)
        return payload
