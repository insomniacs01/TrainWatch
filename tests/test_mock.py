import tempfile
import unittest
from pathlib import Path

from app.collector import Collector
from app.config import AppConfig, NodeConfig, RunConfig, ServerConfig, load_config


class MockTests(unittest.TestCase):
    def test_mock_config_loads_without_ssh_credentials(self) -> None:
        config = load_config(str(Path(__file__).resolve().parents[1] / "config.mock.yaml"))
        self.assertEqual(len(config.nodes), 2)
        self.assertEqual(config.nodes[0].transport, "mock")
        self.assertEqual(config.nodes[0].user, "mock")
        self.assertEqual(config.nodes[0].key_path, "")

    def test_empty_config_loads_with_zero_nodes(self) -> None:
        config = load_config(str(Path(__file__).resolve().parents[1] / "config.empty.yaml"))
        self.assertEqual(config.nodes, [])
        self.assertEqual(config.server.poll_seconds, 5)

    def test_mock_collector_returns_demo_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[
                    NodeConfig(
                        id="demo-node",
                        label="Demo Node",
                        host="mock.local",
                        port=22,
                        user="mock",
                        key_path="",
                        transport="mock",
                        mock_gpu_count=4,
                        runs=[
                            RunConfig(id="run-a", label="Run A", parser="mapanything", mock_state="running"),
                            RunConfig(id="run-b", label="Run B", parser="generic_torch", mock_state="stalled"),
                        ],
                    )
                ],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            collector = Collector(config)
            node = collector.collect_node(config.nodes[0])
            self.assertEqual(node.status, "degraded")
            self.assertEqual(len(node.gpus), 4)
            self.assertEqual(node.runs[0].status, "running")
            self.assertEqual(node.runs[1].status, "stalled")
            self.assertTrue(node.runs[0].matched_processes)


if __name__ == "__main__":
    unittest.main()
