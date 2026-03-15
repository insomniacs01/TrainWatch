import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.config import AppConfig, NodeConfig, ServerConfig
from app.main import create_app
from app.models import NodeSnapshot, RunSnapshot
from app.runtime import TrainWatchRuntime, empty_snapshot


class DummyCollector:
    async def poll_once(self, previous_snapshot, nodes):
        snapshot = empty_snapshot()
        snapshot.summary["nodes_total"] = len(nodes)
        snapshot.summary["runs_total"] = sum(len(node.runs) for node in nodes)
        return snapshot, []

    def close(self):
        return None


class SequencedCollector:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.index = 0

    async def poll_once(self, previous_snapshot, nodes):
        if self.index >= len(self.snapshots):
            payload = self.snapshots[-1]
        else:
            payload = self.snapshots[self.index]
            self.index += 1
        return payload, []

    def close(self):
        return None


def make_runtime(
    tmp_dir: str,
    collector,
    *,
    enable_user_auth: bool = True,
    bootstrap_admin: bool = True,
    shared_token: str = "",
) -> TrainWatchRuntime:
    config = AppConfig(
        server=ServerConfig(
            sqlite_path=str(Path(tmp_dir) / "test.sqlite3"),
            enable_user_auth=enable_user_auth,
            shared_token=shared_token,
            bootstrap_admin_username="admin" if bootstrap_admin else "",
            bootstrap_admin_password="secret-pass" if bootstrap_admin else "",
        ),
        nodes=[],
        config_path=Path(tmp_dir) / "config.yaml",
    )
    runtime = TrainWatchRuntime(config, collector=collector)
    runtime.start = AsyncMock(return_value=None)
    runtime.stop = AsyncMock(return_value=None)
    return runtime


class ProductizationTests(unittest.TestCase):
    def test_personal_mode_stays_public_even_if_user_rows_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector(), enable_user_auth=False, bootstrap_admin=False)
            runtime.auth.create_or_update_user("owner", "owner-pass", "admin", "Owner")
            app = create_app(runtime)
            with TestClient(app) as client:
                auth_config = client.get("/api/v1/auth/config")
                snapshot = client.get("/api/v1/snapshot")

            self.assertEqual(auth_config.status_code, 200)
            payload = auth_config.json()
            self.assertFalse(payload["auth_required"])
            self.assertFalse(payload["user_auth_enabled"])
            self.assertEqual(payload["mode"], "personal")
            self.assertEqual(snapshot.status_code, 200)

    def test_personal_mode_keeps_mutating_endpoints_available_without_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector(), enable_user_auth=False, bootstrap_admin=False)
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/connections",
                    json={
                        "label": "My Box",
                        "host": "gpu.example.com",
                        "port": 22,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["item"]["label"], "My Box")

    def test_auth_config_reports_team_bootstrap_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector(), enable_user_auth=True, bootstrap_admin=False)
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.get("/api/v1/auth/config")

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertTrue(payload["auth_required"])
            self.assertTrue(payload["user_auth_enabled"])
            self.assertTrue(payload["bootstrap_required"])
            self.assertEqual(payload["mode"], "team")
            self.assertEqual(payload["login_methods"], ["password"])

    def test_bootstrap_admin_creates_first_admin_and_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector(), enable_user_auth=True, bootstrap_admin=False)
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/session/bootstrap-admin",
                    json={"username": "owner", "password": "owner-pass", "display_name": "Owner"},
                )
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                token = payload["token"]
                me = client.get("/api/v1/session/me", headers={"x-train-watch-token": token})
                auth_config = client.get("/api/v1/auth/config")

            self.assertEqual(payload["user"]["username"], "owner")
            self.assertEqual(payload["user"]["role"], "admin")
            self.assertEqual(me.status_code, 200)
            self.assertEqual(me.json()["user"]["display_name"], "Owner")
            self.assertFalse(auth_config.json()["bootstrap_required"])

    def test_local_user_login_and_me(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                login = client.post("/api/v1/session/login", json={"username": "admin", "password": "secret-pass"})
                self.assertEqual(login.status_code, 200)
                token = login.json()["token"]
                me = client.get("/api/v1/session/me", headers={"x-train-watch-token": token})

            self.assertEqual(me.status_code, 200)
            self.assertEqual(me.json()["user"]["username"], "admin")
            self.assertEqual(me.json()["user"]["role"], "admin")

    def test_viewer_cannot_mutate_connections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector())
            runtime.auth.create_or_update_user("viewer", "viewer-pass", "viewer", "Viewer")
            app = create_app(runtime)
            with TestClient(app) as client:
                login = client.post("/api/v1/session/login", json={"username": "viewer", "password": "viewer-pass"})
                token = login.json()["token"]
                response = client.post(
                    "/api/v1/connections",
                    headers={"x-train-watch-token": token},
                    json={
                        "label": "My Box",
                        "host": "gpu.example.com",
                        "port": 22,
                        "user": "ubuntu",
                        "password": "secret-password",
                        "runs": [],
                    },
                )

            self.assertEqual(response.status_code, 403)

    def test_alerts_persist_and_can_be_acknowledged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            node = NodeConfig(
                id="node-1",
                label="GPU Box",
                host="gpu.example.com",
                port=22,
                user="ubuntu",
                key_path="~/.ssh/id_ed25519",
                runs=[],
            )
            first = empty_snapshot()
            first.nodes = [
                NodeSnapshot(
                    id="node-1",
                    label="GPU Box",
                    host="gpu.example.com",
                    hostname="gpu.example.com",
                    status="online",
                    error="",
                    collected_at="2026-03-14T10:00:00Z",
                    metrics={},
                    runs=[],
                )
            ]
            second = empty_snapshot()
            second.nodes = [
                NodeSnapshot(
                    id="node-1",
                    label="GPU Box",
                    host="gpu.example.com",
                    hostname="gpu.example.com",
                    status="offline",
                    error="SSH timeout",
                    collected_at="2026-03-14T10:01:00Z",
                    metrics={"cpu_usage_percent": 96.0},
                    runs=[
                        RunSnapshot(
                            id="run-1",
                            label="Main Run",
                            parser="auto",
                            status="stalled",
                            error="No new log lines",
                            log_path="/tmp/train.log",
                            log_exists=True,
                            log_age_seconds=1000,
                            last_update_at="2026-03-14T10:01:00Z",
                            last_log_line="waiting",
                        )
                    ],
                )
            ]
            collector = SequencedCollector([first, second, second])
            runtime = make_runtime(tmp_dir, collector)
            runtime.config.nodes = [node]
            app = create_app(runtime)
            with TestClient(app) as client:
                login = client.post("/api/v1/session/login", json={"username": "admin", "password": "secret-pass"})
                token = login.json()["token"]
                headers = {"x-train-watch-token": token}
                client.post("/api/v1/refresh", headers=headers)
                client.post("/api/v1/refresh", headers=headers)
                client.post("/api/v1/refresh", headers=headers)
                alerts = client.get("/api/v1/alerts", headers=headers)
                self.assertEqual(alerts.status_code, 200)
                items = alerts.json()["items"]
                self.assertTrue(items)
                first_alert = items[0]
                ack = client.post(f"/api/v1/alerts/{first_alert['id']}/ack", headers=headers)

            self.assertEqual(ack.status_code, 200)
            self.assertTrue(ack.json()["item"]["acknowledged"])

    def test_metrics_endpoint_exports_prometheus_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime = make_runtime(tmp_dir, DummyCollector())
            app = create_app(runtime)
            with TestClient(app) as client:
                login = client.post("/api/v1/session/login", json={"username": "admin", "password": "secret-pass"})
                token = login.json()["token"]
                response = client.get("/api/v1/metrics", headers={"x-train-watch-token": token})

            self.assertEqual(response.status_code, 200)
            self.assertIn("train_watch_nodes_total", response.text)
