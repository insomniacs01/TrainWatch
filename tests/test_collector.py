import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from app.collector import Collector, ParamikoConnectionPool
from app.config import AppConfig, NodeConfig, RunConfig, ServerConfig


class FakePool:
    def __init__(self, payload):
        self.payload = payload

    def execute(self, node, command, timeout):
        return json.dumps(self.payload), "", 0

    def close_all(self):
        return None


class RecordingSSHClient:
    def __init__(self):
        self.policy = None
        self.connected_kwargs = None

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, **kwargs):
        self.connected_kwargs = kwargs


class CollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = AppConfig(
            server=ServerConfig(sqlite_path="/tmp/train-watch-test.sqlite3"),
            nodes=[
                NodeConfig(
                    id="node-1",
                    label="Node 1",
                    host="example.com",
                    port=22,
                    user="ubuntu",
                    key_path=str(Path("~/.ssh/id_ed25519").expanduser()),
                    runs=[
                        RunConfig(
                            id="run-1",
                            label="Run 1",
                            log_path="/tmp/train.log",
                            process_match="torchrun .*train.py",
                        )
                    ],
                )
            ],
            config_path=Path("/tmp/config.yaml"),
        )

    def test_collect_node_builds_running_snapshot(self) -> None:
        payload = {
            "hostname": "trainer-a",
            "collected_at": "2026-03-11T10:00:00Z",
            "loadavg": [1.0, 1.2, 1.3],
            "cpu": {"usage_percent": 63.5, "cores_logical": 48},
            "memory": {
                "total_mb": 131072.0,
                "used_mb": 65536.0,
                "available_mb": 65536.0,
                "used_percent": 50.0,
                "swap_total_mb": 0.0,
                "swap_used_mb": 0.0,
                "swap_used_percent": 0.0,
            },
            "disk": {
                "path": "/",
                "total_gb": 2048.0,
                "used_gb": 1024.0,
                "free_gb": 1024.0,
                "used_percent": 50.0,
            },
            "nvidia_smi": True,
            "gpu_error": "",
            "gpus": [
                {
                    "index": 0,
                    "uuid": "GPU-0",
                    "name": "NVIDIA A100",
                    "utilization_gpu": 96.0,
                    "memory_used_mb": 70200.0,
                    "memory_total_mb": 81920.0,
                    "temperature_c": 73.0,
                    "power_draw_w": 264.0,
                    "power_limit_w": 300.0,
                }
            ],
            "gpu_processes": [
                {
                    "gpu_uuid": "GPU-0",
                    "pid": 1234,
                    "process_name": "python",
                    "used_gpu_memory_mb": 69000.0,
                }
            ],
            "runs": [
                {
                    "id": "run-1",
                    "label": "Run 1",
                    "log_path": "/tmp/train.log",
                    "log_exists": True,
                    "last_update_at": "2026-03-11T09:59:59Z",
                    "log_age_seconds": 8,
                    "log_error": "",
                    "tail": "Epoch: [3]  [44/100]  eta: 0:11:02  lr: 0.000020  loss: 1.2044  grad_norm: 0.77",
                    "matched_processes": [{"pid": 1234, "elapsed_seconds": 300, "command": "torchrun train.py"}],
                }
            ],
        }
        collector = Collector(self.config, pool=FakePool(payload))
        node = collector.collect_node(self.config.nodes[0])
        self.assertEqual(node.status, "online")
        self.assertEqual(len(node.gpus), 1)
        self.assertEqual(node.runs[0].status, "running")
        self.assertAlmostEqual(node.runs[0].loss, 1.2044, places=4)
        self.assertEqual(node.runs[0].task_name, "torchrun train.py")
        self.assertEqual(node.runs[0].task_pid, 1234)
        self.assertEqual(node.runs[0].elapsed_seconds, 300)
        self.assertEqual(node.runs[0].started_at, "2026-03-11T09:55:00Z")
        self.assertEqual(node.runs[0].remaining_seconds, 662)
        self.assertEqual(node.runs[0].estimated_end_at, "2026-03-11T10:11:02Z")
        self.assertAlmostEqual(node.runs[0].progress_percent, 44.0, places=1)
        self.assertAlmostEqual(node.metrics["cpu_usage_percent"], 63.5, places=1)
        self.assertAlmostEqual(node.metrics["memory_used_percent"], 50.0, places=1)
        self.assertAlmostEqual(node.metrics["disk_used_percent"], 50.0, places=1)

    def test_paramiko_pool_supports_password_auth(self) -> None:
        node = NodeConfig(
            id="node-password",
            label="Password Node",
            host="gpu.example.com",
            port=2222,
            user="ubuntu",
            key_path="",
            password="ssh-secret",
            runs=[],
        )
        fake_client = RecordingSSHClient()
        with patch("app.collector.paramiko.SSHClient", return_value=fake_client):
            client = ParamikoConnectionPool()._connect(node)

        self.assertIs(client, fake_client)
        self.assertEqual(fake_client.connected_kwargs["hostname"], "gpu.example.com")
        self.assertEqual(fake_client.connected_kwargs["port"], 2222)
        self.assertEqual(fake_client.connected_kwargs["username"], "ubuntu")
        self.assertEqual(fake_client.connected_kwargs["password"], "ssh-secret")
        self.assertFalse(fake_client.connected_kwargs["allow_agent"])
        self.assertFalse(fake_client.connected_kwargs["look_for_keys"])
        self.assertNotIn("key_filename", fake_client.connected_kwargs)

    def test_node_without_nvidia_smi_is_still_online_for_system_metrics(self) -> None:
        payload = {
            "hostname": "jump-box",
            "collected_at": "2026-03-11T10:00:00Z",
            "loadavg": [0.2, 0.3, 0.4],
            "cpu": {"usage_percent": 8.0, "cores_logical": 4},
            "memory": {"total_mb": 4096.0, "used_mb": 1024.0, "available_mb": 3072.0, "used_percent": 25.0},
            "disk": {"path": "/", "total_gb": 100.0, "used_gb": 20.0, "free_gb": 80.0, "used_percent": 20.0},
            "nvidia_smi": False,
            "gpu_error": "nvidia-smi unavailable",
            "gpus": [],
            "gpu_processes": [],
            "runs": [],
        }
        collector = Collector(self.config, pool=FakePool(payload))
        node = collector.collect_node(self.config.nodes[0])
        self.assertEqual(node.status, "online")
        self.assertEqual(node.error, "")
        self.assertAlmostEqual(node.metrics["cpu_usage_percent"], 8.0, places=1)

    def test_remaining_seconds_can_be_estimated_from_progress(self) -> None:
        payload = {
            "hostname": "trainer-a",
            "collected_at": "2026-03-11T10:00:00Z",
            "loadavg": [0.8, 1.0, 1.1],
            "cpu": {"usage_percent": 20.0, "cores_logical": 16},
            "memory": {"total_mb": 32768.0, "used_mb": 8192.0, "available_mb": 24576.0, "used_percent": 25.0},
            "disk": {"path": "/", "total_gb": 1000.0, "used_gb": 200.0, "free_gb": 800.0, "used_percent": 20.0},
            "nvidia_smi": True,
            "gpu_error": "",
            "gpus": [],
            "gpu_processes": [],
            "runs": [
                {
                    "id": "run-1",
                    "label": "Run 1",
                    "log_path": "/tmp/train.log",
                    "log_exists": True,
                    "last_update_at": "2026-03-11T09:59:58Z",
                    "log_age_seconds": 2,
                    "log_error": "",
                    "tail": "Epoch: [2]  [50/100]  lr: 0.000020  loss: 1.0000  grad_norm: 0.50",
                    "matched_processes": [{"pid": 9999, "elapsed_seconds": 600, "command": "python /workspace/train.py --config conf.yaml"}],
                }
            ],
        }
        collector = Collector(self.config, pool=FakePool(payload))
        node = collector.collect_node(self.config.nodes[0])
        run = node.runs[0]
        self.assertEqual(run.status, "running")
        self.assertEqual(run.task_name, "python train.py")
        self.assertEqual(run.elapsed_seconds, 600)
        self.assertEqual(run.remaining_seconds, 600)
        self.assertEqual(run.started_at, "2026-03-11T09:50:00Z")
        self.assertEqual(run.estimated_end_at, "2026-03-11T10:10:00Z")
        self.assertAlmostEqual(run.progress_percent, 50.0, places=1)

    def test_auto_discovered_runs_work_without_configured_logs(self) -> None:
        config = AppConfig(
            server=ServerConfig(sqlite_path="/tmp/train-watch-test-auto.sqlite3"),
            nodes=[
                NodeConfig(
                    id="node-auto",
                    label="Auto Node",
                    host="example.com",
                    port=22,
                    user="ubuntu",
                    key_path=str(Path("~/.ssh/id_ed25519").expanduser()),
                    runs=[],
                )
            ],
            config_path=Path("/tmp/config-auto.yaml"),
        )
        payload = {
            "hostname": "trainer-auto",
            "collected_at": "2026-03-11T10:00:00Z",
            "loadavg": [0.9, 1.0, 1.1],
            "cpu": {"usage_percent": 32.0, "cores_logical": 32},
            "memory": {"total_mb": 65536.0, "used_mb": 32768.0, "available_mb": 32768.0, "used_percent": 50.0},
            "disk": {"path": "/", "total_gb": 1000.0, "used_gb": 350.0, "free_gb": 650.0, "used_percent": 35.0},
            "nvidia_smi": True,
            "gpu_error": "",
            "gpus": [],
            "gpu_processes": [],
            "runs": [],
            "discovered_runs": [
                {
                    "id": "auto-4321",
                    "label": "torchrun train.py",
                    "parser": "auto",
                    "log_path": "/tmp/auto-train.log",
                    "log_exists": True,
                    "last_update_at": "2026-03-11T09:59:55Z",
                    "log_age_seconds": 5,
                    "log_error": "",
                    "tail": "Epoch: [1]  [25/100]  eta: 0:30:00  lr: 0.000100  loss: 2.5000  grad_norm: 1.00",
                    "matched_processes": [
                        {"pid": 4321, "elapsed_seconds": 900, "command": "torchrun train.py --config demo.yaml"},
                        {"pid": 4322, "elapsed_seconds": 890, "command": "python train.py --local_rank=1"},
                    ],
                }
            ],
        }
        collector = Collector(config, pool=FakePool(payload))
        node = collector.collect_node(config.nodes[0])
        self.assertEqual(len(node.runs), 1)
        run = node.runs[0]
        self.assertEqual(run.status, "running")
        self.assertEqual(run.label, "torchrun train.py")
        self.assertEqual(run.task_name, "torchrun train.py")
        self.assertEqual(run.task_pid, 4321)
        self.assertEqual(run.elapsed_seconds, 900)
        self.assertEqual(run.remaining_seconds, 1800)
        self.assertEqual(run.started_at, "2026-03-11T09:45:00Z")
        self.assertEqual(run.estimated_end_at, "2026-03-11T10:30:00Z")
        self.assertAlmostEqual(run.progress_percent, 25.0, places=1)

    def test_poll_once_accepts_explicit_nodes(self) -> None:
        payload = {
            "hostname": "trainer-a",
            "collected_at": "2026-03-11T10:00:00Z",
            "loadavg": [0.5, 0.7, 0.9],
            "cpu": {"usage_percent": 12.0, "cores_logical": 8},
            "memory": {"total_mb": 1024.0, "used_mb": 512.0, "available_mb": 512.0, "used_percent": 50.0},
            "disk": {"path": "/", "total_gb": 100.0, "used_gb": 25.0, "free_gb": 75.0, "used_percent": 25.0},
            "nvidia_smi": True,
            "gpu_error": "",
            "gpus": [],
            "gpu_processes": [],
            "runs": [],
        }
        collector = Collector(self.config, pool=FakePool(payload))
        snapshot, events = asyncio.run(collector.poll_once(None, self.config.nodes))
        self.assertEqual(snapshot.summary["nodes_total"], 1)
        self.assertEqual(len(snapshot.nodes), 1)
        self.assertAlmostEqual(snapshot.summary["cpu_usage_avg"], 12.0, places=1)
        self.assertAlmostEqual(snapshot.summary["memory_used_percent_avg"], 50.0, places=1)
        self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
