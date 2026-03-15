import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from .config import NodeConfig
from .job_queue import (
    LAUNCHED_QUEUE_STATUSES,
    build_remote_launch_command,
    build_run_config,
    queue_summary,
    select_free_gpu_indices,
    utc_now_iso,
)
from .models import AppSnapshot, QueueJob, RunSnapshot
from .runtime_views import build_external_job_items, build_external_queue_summary
from .time_utils import parse_utc_timestamp


logger = logging.getLogger(__name__)
QUEUE_START_TIMEOUT_SECONDS = 180


class RuntimeQueueMixin:
    def _find_queue_job(self, job_id: str) -> Optional[QueueJob]:
        for job in self.queue_jobs:
            if job.id == job_id:
                return job
        return None

    def _persist_queue_job(self, job: QueueJob) -> None:
        self.store.upsert_queue_job(job.to_dict())

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
            return parse_utc_timestamp(value)
        except ValueError:
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
        run_cfg = build_run_config(job)
        job.process_match = run_cfg.process_match
        job.run_id = run_cfg.id
        job.error = "Waiting for first poll after launch"
        self._attach_job_run(job)
        self._persist_queue_job(job)
        logger.info(
            "Queued job launched: id=%s node=%s gpus=%s remote_pid=%s",
            job.id,
            node.id,
            ",".join(str(item) for item in gpu_indices),
            job.remote_pid,
        )

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
                    logger.warning("Queued job startup timed out: id=%s node=%s", job.id, job.node_id)
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
                logger.info("Queued job completed: id=%s node=%s", job.id, job.node_id)
                continue
            if run_snapshot.status == "failed":
                job.status = "failed"
                job.finished_at = snapshot.generated_at
                job.error = run_snapshot.error or "Queued job failed"
                self._detach_job_run(job)
                self._persist_queue_job(job)
                logger.warning("Queued job failed: id=%s node=%s error=%s", job.id, job.node_id, job.error)
                continue
            if run_snapshot.status == "idle":
                job.status = "failed"
                job.finished_at = snapshot.generated_at
                job.error = run_snapshot.error or "Queued job exited without a completion marker"
                self._detach_job_run(job)
                self._persist_queue_job(job)
                logger.warning("Queued job exited without completion marker: id=%s node=%s", job.id, job.node_id)
                continue
            if run_snapshot.status == "unknown":
                age_seconds = self._seconds_since(job.started_at or job.updated_at, snapshot.generated_at)
                if node_snapshot.status != "offline" and age_seconds is not None and age_seconds >= QUEUE_START_TIMEOUT_SECONDS:
                    job.status = "failed"
                    job.finished_at = snapshot.generated_at
                    job.error = run_snapshot.error or "Queued job became unreachable during startup"
                    self._detach_job_run(job)
                    self._persist_queue_job(job)
                    logger.warning("Queued job became unreachable during startup: id=%s node=%s", job.id, job.node_id)
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
            if node_snapshot is None or node_snapshot.status != "online" or not node_snapshot.gpus:
                continue
            reserved = [gpu_index for job in active_by_node.get(node.id, []) for gpu_index in job.allocated_gpu_indices]
            free_gpu_indices = select_free_gpu_indices(node_snapshot, reserved)
            for job in queued_by_node.get(node.id, []):
                if len(free_gpu_indices) < job.gpu_count:
                    break
                allocated = free_gpu_indices[: job.gpu_count]
                try:
                    await self._launch_queue_job(job, node, allocated, snapshot.generated_at)
                except (RuntimeError, ValueError, KeyError, OSError) as exc:
                    job.status = "failed"
                    job.run_status = "failed"
                    job.finished_at = snapshot.generated_at
                    job.updated_at = snapshot.generated_at
                    job.error = str(exc)
                    self._persist_queue_job(job)
                    logger.warning("Queued job launch failed: id=%s node=%s error=%s", job.id, node.id, job.error)
                    break
                free_gpu_indices = free_gpu_indices[job.gpu_count :]

    async def _sync_queue_jobs(self, snapshot: AppSnapshot) -> None:
        self._reconcile_queue_job_states(snapshot)
        await self._schedule_pending_queue_jobs(snapshot)

    async def enqueue_job(self, job: QueueJob) -> Dict[str, object]:
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
            logger.info(
                "Queued job enqueued: id=%s node=%s owner=%s gpus=%s",
                job.id,
                job.node_id,
                job.owner,
                job.gpu_count,
            )
            queue_positions = {}
            grouped = self._queue_jobs_by_node(statuses={"queued"})
            for group_items in grouped.values():
                for index, queued_job in enumerate(group_items, start=1):
                    queue_positions[queued_job.id] = index
            item = self._job_item(job, queue_positions)
        asyncio.create_task(self.refresh_once())
        return item

    async def cancel_job(self, job_id: str) -> Dict[str, object]:
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
            logger.info("Queued job canceled: id=%s node=%s", job.id, job.node_id)
            item = self._job_item(job)
        return item

    def _job_item(self, job: QueueJob, queue_positions: Optional[Dict[str, int]] = None) -> Dict[str, object]:
        item = job.to_dict()
        position = None
        if queue_positions is not None and job.status == "queued":
            position = queue_positions.get(job.id)
        item["queue_position"] = position
        item["can_cancel"] = job.status == "queued"
        return item

    def _external_job_items(self, node_id: Optional[str] = None) -> List[Dict[str, object]]:
        return build_external_job_items(self.snapshot, node_id=node_id)

    def _external_queue_summary(self, items: List[Dict[str, object]]) -> Dict[str, object]:
        return build_external_queue_summary(items)

    def job_summaries(self, node_id: Optional[str] = None) -> Dict[str, object]:
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
