import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.config import AppConfig, NodeConfig, ServerConfig
from app.main import create_app
from app.models import AlertEvent, ExternalQueueItem, NodeSnapshot
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
    def _auth_headers(self, runtime: TrainWatchRuntime) -> dict:
        return {"x-train-watch-token": runtime.config.server.shared_token}

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
                authorized = client.get("/api/v1/health", headers=self._auth_headers(runtime))
            self.assertEqual(unauthorized.status_code, 401)
            self.assertEqual(authorized.status_code, 200)

    def test_health_allows_access_when_token_is_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.get("/api/v1/health")

            self.assertEqual(response.status_code, 200)

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
                    headers=self._auth_headers(runtime),
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

    def test_websocket_stream_requires_auth_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="secret", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                with self.assertRaises(WebSocketDisconnect) as context:
                    with client.websocket_connect("/api/v1/stream") as websocket:
                        websocket.send_json({"type": "auth", "token": "wrong"})
                        websocket.receive_json()

            self.assertEqual(context.exception.code, 4401)

    def test_websocket_stream_accepts_valid_auth_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="secret", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                with client.websocket_connect("/api/v1/stream") as websocket:
                    websocket.send_json({"type": "auth", "token": "secret"})
                    payload = websocket.receive_json()

            self.assertEqual(payload["type"], "snapshot")
            self.assertIn("snapshot", payload)

    def test_snapshot_endpoint_includes_recent_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="secret", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            runtime.recent_events = [
                AlertEvent(
                    kind="run_status_changed",
                    node_id="node-1",
                    node_label="GPU Box",
                    run_id="run-1",
                    run_label="Main Run",
                    status="failed",
                    previous_status="running",
                    at="2026-03-13T10:00:00Z",
                    message="GPU Box / Main Run: running -> failed",
                )
            ]
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.get("/api/v1/snapshot", headers=self._auth_headers(runtime))

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertIn("recent_events", body)
            self.assertEqual(body["recent_events"][0]["status"], "failed")

    def test_websocket_initial_snapshot_includes_recent_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="secret", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            runtime.recent_events = [
                AlertEvent(
                    kind="run_status_changed",
                    node_id="node-1",
                    node_label="GPU Box",
                    run_id="run-1",
                    run_label="Main Run",
                    status="stalled",
                    previous_status="running",
                    at="2026-03-13T10:05:00Z",
                    message="GPU Box / Main Run: running -> stalled",
                )
            ]
            app = create_app(runtime)
            with TestClient(app) as client:
                with client.websocket_connect("/api/v1/stream") as websocket:
                    websocket.send_json({"type": "auth", "token": "secret"})
                    payload = websocket.receive_json()

            self.assertEqual(payload["type"], "snapshot")
            self.assertIn("recent_events", payload["snapshot"])
            self.assertEqual(payload["snapshot"]["recent_events"][0]["status"], "stalled")

    def test_websocket_stream_allows_missing_auth_when_token_is_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(shared_token="", sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                with client.websocket_connect("/api/v1/stream") as websocket:
                    payload = websocket.receive_json()

            self.assertEqual(payload["type"], "snapshot")
            self.assertIn("snapshot", payload)

    def test_add_connection_persists_queue_probe_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = str(Path(tmp_dir) / "test.sqlite3")
            config_path = Path(tmp_dir) / "config.yaml"
            config = AppConfig(
                server=ServerConfig(sqlite_path=sqlite_path),
                nodes=[],
                config_path=config_path,
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/connections",
                    headers=self._auth_headers(runtime),
                    json={
                        "label": "Queue Aware Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "queue_probe_command": "python3 /opt/lab/queue_probe.py",
                        "runs": [],
                    },
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(runtime.config.nodes[0].queue_probe_command, "python3 /opt/lab/queue_probe.py")

            restored_runtime = TrainWatchRuntime(
                AppConfig(server=ServerConfig(sqlite_path=sqlite_path), nodes=[], config_path=config_path),
                collector=DummyCollector(),
            )
            self.assertEqual(len(restored_runtime.config.nodes), 1)
            self.assertEqual(restored_runtime.config.nodes[0].queue_probe_command, "python3 /opt/lab/queue_probe.py")

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
                    headers=self._auth_headers(runtime),
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
                    headers=self._auth_headers(runtime),
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
            with patch("app.api_inputs.ssh_config_alias_exists", return_value=True):
                with TestClient(app) as client:
                    response = client.post(
                        "/api/v1/connections",
                        headers=self._auth_headers(runtime),
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

    def test_add_connection_rejects_password_for_local_ssh_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with patch("app.api_inputs.ssh_config_alias_exists", return_value=True):
                with TestClient(app) as client:
                    response = client.post(
                        "/api/v1/connections",
                        headers=self._auth_headers(runtime),
                        json={
                            "label": "Alias Box",
                            "host": "gpu-lab-a",
                            "port": 22,
                            "user": "",
                            "password": "secret-password",
                            "runs": [],
                        },
                    )

            self.assertEqual(response.status_code, 400)
            self.assertIn("password auth is not supported", response.json()["detail"])

    def test_add_connection_rejects_unknown_parser(self) -> None:
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
                    headers=self._auth_headers(runtime),
                    json={
                        "label": "Parser Box",
                        "host": "gpu.example.com",
                        "port": 22,
                        "user": "ubuntu",
                        "password": "secret",
                        "runs": [
                            {
                                "label": "Main Run",
                                "log_path": "/tmp/train.log",
                                "parser": "unknown-parser",
                            }
                        ],
                    },
                )

            self.assertEqual(response.status_code, 400)
            self.assertIn("parser must be one of", response.json()["detail"])

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
                    response = client.get("/api/v1/ssh-aliases", headers=self._auth_headers(runtime))

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(len(body["items"]), 1)
            self.assertEqual(body["items"][0]["alias"], "gpu-lab-a")
            self.assertEqual(body["items"][0]["port"], 10800)

    def test_queue_job_endpoints_support_enqueue_list_and_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[
                    NodeConfig(
                        id="node-1",
                        label="GPU Box",
                        host="gpu.example.com",
                        port=22,
                        user="ubuntu",
                        key_path="",
                        password="secret-password",
                        runs=[],
                    )
                ],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                created = client.post(
                    "/api/v1/jobs",
                    headers=self._auth_headers(runtime),
                    json={
                        "node_id": "node-1",
                        "owner": "alice",
                        "label": "SFT",
                        "command": "torchrun train.py --config conf.yaml",
                        "gpu_count": 2,
                        "workdir": "/workspace/project",
                    },
                )
                listed = client.get("/api/v1/jobs", headers=self._auth_headers(runtime))
                job_id = created.json()["item"]["id"]
                canceled = client.delete(
                    f"/api/v1/jobs/{job_id}",
                    headers=self._auth_headers(runtime),
                )

            self.assertEqual(created.status_code, 200)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(canceled.status_code, 200)
            self.assertEqual(listed.json()["summary"]["jobs_queued"], 1)
            self.assertEqual(listed.json()["items"][0]["owner"], "alice")
            self.assertEqual(listed.json()["items"][0]["queue_position"], 1)
            self.assertEqual(canceled.json()["item"]["status"], "canceled")

    def test_jobs_endpoint_includes_external_queue_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = AppConfig(
                server=ServerConfig(sqlite_path=str(Path(tmp_dir) / "test.sqlite3")),
                nodes=[
                    NodeConfig(
                        id="node-1",
                        label="GPU Box",
                        host="gpu.example.com",
                        port=22,
                        user="ubuntu",
                        key_path="",
                        password="secret-password",
                        runs=[],
                    )
                ],
                config_path=Path(tmp_dir) / "config.yaml",
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                runtime.snapshot = empty_snapshot()
                runtime.snapshot.nodes = [
                    NodeSnapshot(
                        id="node-1",
                        label="GPU Box",
                        host="gpu.example.com",
                        hostname="gpu.example.com",
                        status="online",
                        error="",
                        collected_at="2026-03-13T10:00:00Z",
                        external_queue=[
                            ExternalQueueItem(
                                id="12345",
                                owner="alice",
                                label="slurm-train",
                                status="queued",
                                source="slurm",
                                raw_status="PENDING",
                                submitted_at="2026-03-13T09:59:00Z",
                                command="sbatch train.sh",
                                reason="Resources",
                            )
                        ],
                        external_queue_source="slurm",
                    )
                ]
                listed = client.get("/api/v1/jobs", headers=self._auth_headers(runtime))

            self.assertEqual(listed.status_code, 200)
            body = listed.json()
            self.assertEqual(body["external_summary"]["jobs_queued"], 1)
            self.assertEqual(len(body["external_items"]), 1)
            self.assertEqual(body["external_items"][0]["source"], "slurm")
            self.assertEqual(body["external_items"][0]["owner"], "alice")

    def test_dynamic_connection_is_restored_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = str(Path(tmp_dir) / "test.sqlite3")
            config_path = Path(tmp_dir) / "config.yaml"
            config = AppConfig(
                server=ServerConfig(sqlite_path=sqlite_path),
                nodes=[],
                config_path=config_path,
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/connections",
                    headers=self._auth_headers(runtime),
                    json={
                        "label": "Persistent Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )
            self.assertEqual(response.status_code, 200)

            restored_runtime = TrainWatchRuntime(
                AppConfig(
                    server=ServerConfig(sqlite_path=sqlite_path),
                    nodes=[],
                    config_path=config_path,
                ),
                collector=DummyCollector(),
            )
            self.assertEqual(len(restored_runtime.config.nodes), 1)
            restored = restored_runtime.config.nodes[0]
            self.assertEqual(restored.label, "Persistent Box")
            self.assertEqual(restored.host, "gpu.example.com")
            self.assertEqual(restored.port, 2222)
            self.assertEqual(restored.user, "ubuntu")
            self.assertEqual(restored.password, "")
            self.assertTrue(restored.needs_password)

    def test_dynamic_connection_can_opt_in_to_password_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = str(Path(tmp_dir) / "test.sqlite3")
            config_path = Path(tmp_dir) / "config.yaml"
            config = AppConfig(
                server=ServerConfig(sqlite_path=sqlite_path, persist_passwords=True),
                nodes=[],
                config_path=config_path,
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/connections",
                    headers=self._auth_headers(runtime),
                    json={
                        "label": "Persistent Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )
            self.assertEqual(response.status_code, 200)

            restored_runtime = TrainWatchRuntime(
                AppConfig(
                    server=ServerConfig(sqlite_path=sqlite_path, persist_passwords=True),
                    nodes=[],
                    config_path=config_path,
                ),
                collector=DummyCollector(),
            )
            self.assertEqual(len(restored_runtime.config.nodes), 1)
            restored = restored_runtime.config.nodes[0]
            self.assertEqual(restored.password, "secret-password")
            self.assertFalse(restored.needs_password)

    def test_persisted_alias_connection_restores_without_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = str(Path(tmp_dir) / "test.sqlite3")
            config_path = Path(tmp_dir) / "config.yaml"
            config = AppConfig(
                server=ServerConfig(sqlite_path=sqlite_path),
                nodes=[],
                config_path=config_path,
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with patch("app.api_inputs.ssh_config_alias_exists", return_value=True):
                with TestClient(app) as client:
                    response = client.post(
                        "/api/v1/connections",
                        headers=self._auth_headers(runtime),
                        json={
                            "label": "Alias Box",
                            "host": "gpu-lab-a",
                            "port": 22,
                            "user": "",
                            "runs": [],
                        },
                    )
            self.assertEqual(response.status_code, 200)

            restored_runtime = TrainWatchRuntime(
                AppConfig(
                    server=ServerConfig(sqlite_path=sqlite_path),
                    nodes=[],
                    config_path=config_path,
                ),
                collector=DummyCollector(),
            )
            self.assertEqual(len(restored_runtime.config.nodes), 1)
            restored = restored_runtime.config.nodes[0]
            self.assertEqual(restored.label, "Alias Box")
            self.assertEqual(restored.host, "gpu-lab-a")
            self.assertEqual(restored.user, "")
            self.assertEqual(restored.password, "")
            self.assertEqual(restored.key_path, "")

    def test_remove_connection_clears_persisted_dynamic_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = str(Path(tmp_dir) / "test.sqlite3")
            config_path = Path(tmp_dir) / "config.yaml"
            config = AppConfig(
                server=ServerConfig(sqlite_path=sqlite_path),
                nodes=[],
                config_path=config_path,
            )
            runtime = TrainWatchRuntime(config, collector=DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                created = client.post(
                    "/api/v1/connections",
                    headers=self._auth_headers(runtime),
                    json={
                        "label": "Disposable Box",
                        "host": "gpu.example.com",
                        "port": 2222,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )
                node_id = created.json()["item"]["id"]
                deleted = client.delete(
                    f"/api/v1/connections/{node_id}",
                    headers=self._auth_headers(runtime),
                )
            self.assertEqual(created.status_code, 200)
            self.assertEqual(deleted.status_code, 200)

            restored_runtime = TrainWatchRuntime(
                AppConfig(
                    server=ServerConfig(sqlite_path=sqlite_path),
                    nodes=[],
                    config_path=config_path,
                ),
                collector=DummyCollector(),
            )
            self.assertEqual(restored_runtime.config.nodes, [])


if __name__ == "__main__":
    unittest.main()
