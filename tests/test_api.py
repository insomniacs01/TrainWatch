import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from unittest.mock import patch

from app.config import AppConfig, ServerConfig
from app.main import create_app
from app.runtime import TrainWatchRuntime, empty_snapshot


class DummyCollector:
    async def poll_once(self, previous_snapshot, nodes):
        snapshot = empty_snapshot()
        snapshot.summary["nodes_total"] = len(nodes)
        snapshot.summary["runs_total"] = sum(len(node.runs) for node in nodes)
        return snapshot, []

    def close(self):
        return None


class ApiTests(unittest.TestCase):
    def test_health_requires_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="secret", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                unauthorized = client.get("/api/v1/health")
                authorized = client.get("/api/v1/health", headers={"x-train-watch-token": "secret"})
            self.assertEqual(unauthorized.status_code, 401)
            self.assertEqual(authorized.status_code, 200)

    def test_add_connection_accepts_password_only_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/connections",
                    json={
                        "label": "My Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["item"]["label"], "My Box")
            self.assertEqual(body["item"]["host"], "gpu.example.com")
            self.assertEqual(body["item"]["port"], 2222)
            self.assertEqual(body["item"]["user"], "ubuntu")
            self.assertEqual(body["item"]["transport"], "ssh")
            self.assertEqual(len(runtime.config.nodes), 1)
            self.assertEqual(runtime.config.nodes[0].password, "secret-password")
            self.assertEqual(runtime.config.nodes[0].key_path, "")

    def test_add_connection_rejects_duplicate_host_user_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                first = client.post(
                    "/api/v1/connections",
                    json={
                        "label": "My Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )
                second = client.post(
                    "/api/v1/connections",
                    json={
                        "label": "My Box Again",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 409)
            self.assertIn("Connection already exists", second.json()["detail"])

    def test_add_connection_accepts_local_ssh_alias_without_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with patch("app.main.ssh_config_alias_exists", return_value=True):
                with TestClient(app) as client:
                    response = client.post(
                        "/api/v1/connections",
                        json={
                            "label": "Alias Box",
                            "host": "gpu-lab-a",
                            "port": 22,
                            "user": "",
                            "runs": [],
                        },
                    )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["item"]["host"], "gpu-lab-a")
            self.assertEqual(body["item"]["user"], "")
            self.assertEqual(len(runtime.config.nodes), 1)

    def test_ssh_aliases_endpoint_returns_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with patch("app.main.ssh_config_alias_records", return_value=[{
                "alias": "gpu-lab-a",
                "hostname": "gpu.example.com",
                "user": "ubuntu",
                "port": 10800,
                "proxyjump": "",
                "identityfile": "~/.ssh/id_ed25519",
            }]):
                with TestClient(app) as client:
                    response = client.get("/api/v1/ssh-aliases")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(len(body["items"]), 1)
            self.assertEqual(body["items"][0]["alias"], "gpu-lab-a")
            self.assertEqual(body["items"][0]["port"], 10800)


if __name__ == "__main__":
    unittest.main()
