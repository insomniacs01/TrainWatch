import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import AppConfig, NodeConfig, ServerConfig
from app.job_queue import select_free_gpu_indices
from app.models import AppSnapshot, GPUInfo, GPUProcess, NodeSnapshot, QueueJob, RunSnapshot
from app.runtime import TrainWatchRuntime


class FakePool:
    def __init__(self) -> None:
        self.commands = []

    def execute(self, node, command, timeout):
        self.commands.append(command)
        job_index = len(self.commands)
        payload = {
            "remote_pid": 4000 + job_index,
            "script_path": f"/home/ubuntu/.train-watch/jobs/job-{job_index}/run.sh",
            "log_path": f"/home/ubuntu/.train-watch/jobs/job-{job_index}/train-watch.log",
        }
        return json.dumps(payload), "", 0

    def close_all(self):
        return None

    def close_node(self, node):
        return None


class FailingThenSucceedingPool:
    def __init__(self) -> None:
        self.commands = []
        self.calls = 0

    def execute(self, node, command, timeout):
        self.calls += 1
        self.commands.append(command)
        if self.calls == 1:
            return "", "launch exploded", 1
        payload = {
            "remote_pid": 5000 + self.calls,
            "script_path": f"/home/ubuntu/.train-watch/jobs/job-{self.calls}/run.sh",
            "log_path": f"/home/ubuntu/.train-watch/jobs/job-{self.calls}/train-watch.log",
        }
        return json.dumps(payload), "", 0

    def close_all(self):
        return None

    def close_node(self, node):
        return None


class LaunchingCollector:
    def __init__(self) -> None:
        self.pool = FakePool()
        self.calls = 0

    async def poll_once(self, previous_snapshot, nodes):
        self.calls += 1
        phase = self.calls
        snapshot = AppSnapshot(
            generated_at=f"2026-03-12T00:00:0{phase}Z",
            summary={
                "nodes_total": 1,
                "nodes_online": 1,
                "nodes_degraded": 0,
                "nodes_offline": 0,
                "runs_total": len(nodes[0].runs),
                "runs_running": 1 if phase == 2 and nodes[0].runs else 0,
                "runs_alerting": 0,
                "gpus_total": 2,
                "gpus_busy": 2 if phase == 2 and nodes[0].runs else 0,
            },
            nodes=[self._build_node(nodes[0], phase)],
        )
        return snapshot, []

    def _build_node(self, node, phase):
        running = phase == 2 and bool(node.runs)
        completed = phase >= 3 and bool(node.runs)
        processes = [
            GPUProcess(
                pid=4321,
                process_name="bash",
                gpu_uuid="gpu-0",
                gpu_index=0,
                used_gpu_memory_mb=2048.0,
                command=node.runs[0].process_match if node.runs else "",
                elapsed_seconds=30,
            )
        ] if running else []
        gpus = [
            GPUInfo(
                index=0,
                uuid="gpu-0",
                name="RTX 3090",
                utilization_gpu=90.0 if running else 0.0,
                memory_used_mb=2048.0 if running else 0.0,
                memory_total_mb=24576.0,
                temperature_c=60.0 if running else 40.0,
                power_draw_w=200.0 if running else 25.0,
                power_limit_w=350.0,
                processes=processes,
            ),
            GPUInfo(
                index=1,
                uuid="gpu-1",
                name="RTX 3090",
                utilization_gpu=85.0 if running else 0.0,
                memory_used_mb=2048.0 if running else 0.0,
                memory_total_mb=24576.0,
                temperature_c=61.0 if running else 39.0,
                power_draw_w=205.0 if running else 24.0,
                power_limit_w=350.0,
                processes=processes if running else [],
            ),
        ]
        runs = []
        for run_cfg in node.runs:
            status = "running" if running else "completed" if completed else "unknown"
            runs.append(
                RunSnapshot(
                    id=run_cfg.id,
                    label=run_cfg.label,
                    parser=run_cfg.parser,
                    status=status,
                    error="",
                    log_path=run_cfg.log_path or "",
                    log_exists=True,
                    log_age_seconds=1,
                    last_update_at=f"2026-03-12T00:00:0{phase}Z",
                    last_log_line="TRAIN_WATCH_QUEUE_COMPLETED" if completed else "step 10/100",
                    matched_processes=[{"pid": 4321, "elapsed_seconds": 30, "command": run_cfg.process_match}] if running else [],
                    completion_matched=completed,
                    error_matched=False,
                )
            )
        return NodeSnapshot(
            id=node.id,
            label=node.label,
            host=node.host,
            hostname=node.host,
            status="online",
            error="",
            collected_at=f"2026-03-12T00:00:0{phase}Z",
            loadavg=[0.2, 0.2, 0.2],
            metrics={"gpu_process_count": 2.0 if running else 0.0},
            gpus=gpus,
            gpu_processes=processes,
            runs=runs,
        )

    def close(self):
        return None


class StaticCollector:
    def __init__(self) -> None:
        self.pool = FakePool()

    async def poll_once(self, previous_snapshot, nodes):
        node = nodes[0]
        snapshot = AppSnapshot(
            generated_at="2026-03-12T00:00:01Z",
            summary={
                "nodes_total": 1,
                "nodes_online": 1,
                "nodes_degraded": 0,
                "nodes_offline": 0,
                "runs_total": len(node.runs),
                "runs_running": 0,
                "runs_alerting": 0,
                "gpus_total": 2,
                "gpus_busy": 0,
            },
            nodes=[
                NodeSnapshot(
                    id=node.id,
                    label=node.label,
                    host=node.host,
                    hostname=node.host,
                    status="online",
                    error="",
                    collected_at="2026-03-12T00:00:01Z",
                    loadavg=[0.1, 0.1, 0.1],
                    metrics={"gpu_process_count": 0.0},
                    gpus=[
                        GPUInfo(0, "gpu-0", "RTX 3090", 0.0, 0.0, 24576.0, 40.0, 20.0, 350.0, []),
                        GPUInfo(1, "gpu-1", "RTX 3090", 0.0, 0.0, 24576.0, 41.0, 21.0, 350.0, []),
                    ],
                    gpu_processes=[],
                    runs=[],
                )
            ],
        )
        return snapshot, []

    def close(self):
        return None


class FailingLaunchCollector:
    def __init__(self) -> None:
        self.pool = FailingThenSucceedingPool()

    async def poll_once(self, previous_snapshot, nodes):
        node = nodes[0]
        snapshot = AppSnapshot(
            generated_at="2026-03-12T00:00:01Z",
            summary={
                "nodes_total": 1,
                "nodes_online": 1,
                "nodes_degraded": 0,
                "nodes_offline": 0,
                "runs_total": len(node.runs),
                "runs_running": 0,
                "runs_alerting": 0,
                "gpus_total": 2,
                "gpus_busy": 0,
            },
            nodes=[
                NodeSnapshot(
                    id=node.id,
                    label=node.label,
                    host=node.host,
                    hostname=node.host,
                    status="online",
                    error="",
                    collected_at="2026-03-12T00:00:01Z",
                    loadavg=[0.1, 0.1, 0.1],
                    metrics={"gpu_process_count": 0.0},
                    gpus=[
                        GPUInfo(0, "gpu-0", "RTX 3090", 0.0, 0.0, 24576.0, 40.0, 20.0, 350.0, []),
                        GPUInfo(1, "gpu-1", "RTX 3090", 0.0, 0.0, 24576.0, 41.0, 21.0, 350.0, []),
                    ],
                    gpu_processes=[],
                    runs=[],
                )
            ],
        )
        return snapshot, []

    def close(self):
        return None


class FlakySSHCollector:
    def __init__(self) -> None:
        self.pool = FakePool()
        self.calls = 0

    async def poll_once(self, previous_snapshot, nodes):
        self.calls += 1
        phase = self.calls
        node = nodes[0]
        if phase == 1:
            return self._online_snapshot(node), []
        return self._offline_snapshot(node, phase), []

    def _online_snapshot(self, node):
        return AppSnapshot(
            generated_at="2026-03-12T00:00:01Z",
            summary={
                "nodes_total": 1,
                "nodes_online": 1,
                "nodes_degraded": 0,
                "nodes_offline": 0,
                "runs_total": 1,
                "runs_running": 1,
                "runs_alerting": 0,
                "gpus_total": 2,
                "gpus_busy": 1,
            },
            nodes=[
                NodeSnapshot(
                    id=node.id,
                    label=node.label,
                    host=node.host,
                    hostname=node.host,
                    status="online",
                    error="",
                    collected_at="2026-03-12T00:00:01Z",
                    loadavg=[0.1, 0.1, 0.1],
                    metrics={"gpu_process_count": 1.0},
                    gpus=[
                        GPUInfo(0, "gpu-0", "RTX 3090", 92.0, 2048.0, 24576.0, 62.0, 200.0, 350.0, []),
                        GPUInfo(1, "gpu-1", "RTX 3090", 0.0, 0.0, 24576.0, 40.0, 20.0, 350.0, []),
                    ],
                    gpu_processes=[],
                    runs=[
                        RunSnapshot(
                            id="run-1",
                            label="Main Run",
                            parser="auto",
                            status="running",
                            error="",
                            log_path="/tmp/train.log",
                            log_exists=True,
                            log_age_seconds=3,
                            last_update_at="2026-03-12T00:00:01Z",
                            last_log_line="step 1/10",
                        )
                    ],
                )
            ],
        )

    def _offline_snapshot(self, node, phase):
        return AppSnapshot(
            generated_at="2026-03-12T00:00:0%sZ" % phase,
            summary={
                "nodes_total": 1,
                "nodes_online": 0,
                "nodes_degraded": 0,
                "nodes_offline": 1,
                "runs_total": 1,
                "runs_running": 0,
                "runs_alerting": 0,
                "gpus_total": 0,
                "gpus_busy": 0,
            },
            nodes=[
                NodeSnapshot(
                    id=node.id,
                    label=node.label,
                    host=node.host,
                    hostname=node.host,
                    status="offline",
                    error="kex_exchange_identification: read: Connection reset by peer",
                    collected_at="2026-03-12T00:00:0%sZ" % phase,
                    loadavg=[],
                    metrics={},
                    gpus=[],
                    gpu_processes=[],
                    runs=[
                        RunSnapshot(
                            id="run-1",
                            label="Main Run",
                            parser="auto",
                            status="unknown",
                            error="kex_exchange_identification: read: Connection reset by peer",
                            log_path="/tmp/train.log",
                            log_exists=False,
                            log_age_seconds=None,
                            last_update_at="",
                            last_log_line="",
                        )
                    ],
                )
            ],
        )

    def close(self):
        return None


class QueueRuntimeTests(unittest.TestCase):
    def _config(self, sqlite_path: str) -> AppConfig:
        return AppConfig(
            server=ServerConfig(sqlite_path=sqlite_path),
            nodes=[
                NodeConfig(
                    id="node-1",
                    label="GPU Box",
                    host="gpu.example.com",
                    port=22,
                    user="ubuntu",
                    key_path="",
                    password="secret",
                    runs=[],
                )
            ],
            config_path=Path(sqlite_path).with_suffix(".yaml"),
        )

    def _no_task(self, coro):
        coro.close()
        return None

    def test_queue_job_launches_then_completes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                config = self._config(str(Path(tmp_dir) / "queue.sqlite3"))
                runtime = TrainWatchRuntime(config, collector=LaunchingCollector())
                job = QueueJob(
                    id="job-1",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="alice",
                    label="SFT",
                    command="torchrun train.py --config conf.yaml",
                    gpu_count=2,
                    created_at="2026-03-12T00:00:00Z",
                    updated_at="2026-03-12T00:00:00Z",
                    workdir="/workspace/project",
                )
                with patch("app.runtime.asyncio.create_task", side_effect=self._no_task):
                    await runtime.enqueue_job(job)
                await runtime.refresh_once()
                self.assertEqual(runtime.job_summaries()["items"][0]["status"], "starting")
                self.assertEqual(len(runtime.config.nodes[0].runs), 1)
                await runtime.refresh_once()
                self.assertEqual(runtime.job_summaries()["items"][0]["status"], "running")
                await runtime.refresh_once()
                self.assertEqual(runtime.job_summaries()["items"][0]["status"], "completed")
                self.assertEqual(len(runtime.config.nodes[0].runs), 0)

        asyncio.run(scenario())

    def test_fifo_does_not_skip_head_job_when_gpus_are_insufficient(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                config = self._config(str(Path(tmp_dir) / "queue.sqlite3"))
                collector = StaticCollector()
                runtime = TrainWatchRuntime(config, collector=collector)
                first = QueueJob(
                    id="job-1",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="alice",
                    label="Need Three GPUs",
                    command="torchrun train_a.py",
                    gpu_count=3,
                    created_at="2026-03-12T00:00:00Z",
                    updated_at="2026-03-12T00:00:00Z",
                )
                second = QueueJob(
                    id="job-2",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="bob",
                    label="Need One GPU",
                    command="python train_b.py",
                    gpu_count=1,
                    created_at="2026-03-12T00:00:01Z",
                    updated_at="2026-03-12T00:00:01Z",
                )
                with patch("app.runtime.asyncio.create_task", side_effect=self._no_task):
                    await runtime.enqueue_job(first)
                    await runtime.enqueue_job(second)
                await runtime.refresh_once()
                statuses = [item["status"] for item in runtime.job_summaries()["items"]]
                self.assertEqual(statuses, ["queued", "queued"])
                self.assertEqual(len(collector.pool.commands), 0)

        asyncio.run(scenario())

    def test_launch_failure_marks_head_failed_and_unblocks_following_jobs(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                config = self._config(str(Path(tmp_dir) / "queue.sqlite3"))
                collector = FailingLaunchCollector()
                runtime = TrainWatchRuntime(config, collector=collector)
                first = QueueJob(
                    id="job-1",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="alice",
                    label="Broken Job",
                    command="python broken.py",
                    gpu_count=1,
                    created_at="2026-03-12T00:00:00Z",
                    updated_at="2026-03-12T00:00:00Z",
                )
                second = QueueJob(
                    id="job-2",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="bob",
                    label="Healthy Job",
                    command="python train_ok.py",
                    gpu_count=1,
                    created_at="2026-03-12T00:00:01Z",
                    updated_at="2026-03-12T00:00:01Z",
                )
                with patch("app.runtime.asyncio.create_task", side_effect=self._no_task):
                    await runtime.enqueue_job(first)
                    await runtime.enqueue_job(second)

                await runtime.refresh_once()
                items = {item["id"]: item for item in runtime.job_summaries()["items"]}
                self.assertEqual(items["job-1"]["status"], "failed")
                self.assertEqual(items["job-1"]["run_status"], "failed")
                self.assertEqual(items["job-1"]["finished_at"], "2026-03-12T00:00:01Z")
                self.assertIn("launch exploded", items["job-1"]["error"])
                self.assertEqual(items["job-2"]["status"], "queued")

                await runtime.refresh_once()
                items = {item["id"]: item for item in runtime.job_summaries()["items"]}
                self.assertEqual(items["job-2"]["status"], "starting")
                self.assertEqual(len(collector.pool.commands), 2)

        asyncio.run(scenario())

    def test_transient_ssh_failure_keeps_previous_snapshot_and_skips_launch(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                config = self._config(str(Path(tmp_dir) / "queue.sqlite3"))
                config.nodes[0].runs = []
                collector = FlakySSHCollector()
                runtime = TrainWatchRuntime(config, collector=collector)

                await runtime.refresh_once()
                self.assertEqual(runtime.snapshot.nodes[0].status, "online")

                job = QueueJob(
                    id="job-1",
                    node_id="node-1",
                    node_label="GPU Box",
                    owner="alice",
                    label="SFT",
                    command="torchrun train.py",
                    gpu_count=1,
                    created_at="2026-03-12T00:00:00Z",
                    updated_at="2026-03-12T00:00:00Z",
                )
                with patch("app.runtime.asyncio.create_task", side_effect=self._no_task):
                    await runtime.enqueue_job(job)

                await runtime.refresh_once()
                node = runtime.snapshot.nodes[0]
                self.assertEqual(node.status, "degraded")
                self.assertIn("保留上次成功数据", node.error)
                self.assertEqual(len(node.gpus), 2)
                self.assertEqual(runtime.snapshot.summary["nodes_offline"], 0)
                self.assertEqual(runtime.snapshot.summary["nodes_degraded"], 1)
                self.assertEqual(runtime.job_summaries()["items"][0]["status"], "queued")
                self.assertEqual(len(collector.pool.commands), 0)
                self.assertEqual(runtime.recent_events, [])

        asyncio.run(scenario())

    def test_consecutive_ssh_failures_eventually_mark_node_offline(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                config = self._config(str(Path(tmp_dir) / "queue.sqlite3"))
                collector = FlakySSHCollector()
                runtime = TrainWatchRuntime(config, collector=collector)

                await runtime.refresh_once()
                await runtime.refresh_once()
                self.assertEqual(runtime.snapshot.nodes[0].status, "degraded")

                await runtime.refresh_once()
                node = runtime.snapshot.nodes[0]
                self.assertEqual(node.status, "offline")
                self.assertIn("Connection reset by peer", node.error)
                self.assertEqual(runtime.snapshot.summary["nodes_offline"], 1)

        asyncio.run(scenario())


class QueueHeuristicTests(unittest.TestCase):
    def test_small_residual_gpu_contexts_are_treated_as_free(self) -> None:
        node = NodeSnapshot(
            id="node-1",
            label="GPU Box",
            host="gpu.example.com",
            hostname="gpu.example.com",
            status="online",
            error="",
            collected_at="2026-03-13T02:00:00Z",
            loadavg=[],
            metrics={},
            gpus=[
                GPUInfo(
                    index=3,
                    uuid="gpu-3",
                    name="RTX 3090",
                    utilization_gpu=0.0,
                    memory_used_mb=450.0,
                    memory_total_mb=24576.0,
                    temperature_c=42.0,
                    power_draw_w=30.0,
                    power_limit_w=350.0,
                    processes=[
                        GPUProcess(
                            pid=123,
                            process_name="python3",
                            gpu_uuid="gpu-3",
                            gpu_index=3,
                            used_gpu_memory_mb=442.0,
                            command="/usr/bin/python3 -",
                            elapsed_seconds=120,
                        )
                    ],
                ),
                GPUInfo(
                    index=4,
                    uuid="gpu-4",
                    name="RTX 3090",
                    utilization_gpu=0.0,
                    memory_used_mb=450.0,
                    memory_total_mb=24576.0,
                    temperature_c=43.0,
                    power_draw_w=31.0,
                    power_limit_w=350.0,
                    processes=[
                        GPUProcess(
                            pid=124,
                            process_name="python3",
                            gpu_uuid="gpu-4",
                            gpu_index=4,
                            used_gpu_memory_mb=442.0,
                            command="/usr/bin/python3 -",
                            elapsed_seconds=120,
                        )
                    ],
                ),
            ],
            gpu_processes=[],
            runs=[],
        )
        self.assertEqual(select_free_gpu_indices(node), [3, 4])


if __name__ == "__main__":
    unittest.main()
