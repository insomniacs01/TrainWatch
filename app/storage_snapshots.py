import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .models import AppSnapshot
from .time_utils import utc_now_iso


class SQLiteSnapshotStoreMixin:
    def persist_snapshot(self, snapshot: AppSnapshot) -> None:
        payload = json.dumps(snapshot.to_dict(), ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO snapshots(ts, payload) VALUES (?, ?)",
                (snapshot.generated_at, payload),
            )
            self._persist_timeseries(connection, snapshot)
            self._cleanup(connection)
            connection.commit()

    def _persist_timeseries(self, connection: sqlite3.Connection, snapshot: AppSnapshot) -> None:
        for node in snapshot.nodes:
            for metric, value in node.metrics.items():
                if isinstance(value, (int, float)):
                    connection.execute(
                        "INSERT INTO timeseries(ts, node_id, run_id, metric, value) VALUES (?, ?, ?, ?, ?)",
                        (snapshot.generated_at, node.id, None, metric, float(value)),
                    )
            for gpu in node.gpus:
                for metric, value in (
                    ("gpu.%s.utilization_gpu" % gpu.index, gpu.utilization_gpu),
                    ("gpu.%s.memory_used_mb" % gpu.index, gpu.memory_used_mb),
                    ("gpu.%s.temperature_c" % gpu.index, gpu.temperature_c),
                ):
                    if value is None:
                        continue
                    connection.execute(
                        "INSERT INTO timeseries(ts, node_id, run_id, metric, value) VALUES (?, ?, ?, ?, ?)",
                        (snapshot.generated_at, node.id, None, metric, float(value)),
                    )
            for run in node.runs:
                for metric, value in (
                    ("loss", run.loss),
                    ("eval_loss", run.eval_loss),
                    ("lr", run.lr),
                    ("grad_norm", run.grad_norm),
                    ("tokens_per_sec", run.tokens_per_sec),
                    ("samples_per_sec", run.samples_per_sec),
                    ("eta_seconds", run.eta_seconds),
                    ("elapsed_seconds", run.elapsed_seconds),
                    ("remaining_seconds", run.remaining_seconds),
                    ("progress_percent", run.progress_percent),
                    ("epoch", run.epoch),
                    ("step", run.step),
                    ("log_age_seconds", run.log_age_seconds),
                ):
                    if value is None:
                        continue
                    connection.execute(
                        "INSERT INTO timeseries(ts, node_id, run_id, metric, value) VALUES (?, ?, ?, ?, ?)",
                        (snapshot.generated_at, node.id, run.id, metric, float(value)),
                    )

    def _cleanup(self, connection: sqlite3.Connection) -> None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.retention_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        connection.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
        connection.execute("DELETE FROM timeseries WHERE ts < ?", (cutoff,))
        connection.execute("DELETE FROM alert_events WHERE at < ?", (cutoff,))
        connection.execute("DELETE FROM audit_logs WHERE at < ?", (cutoff,))
        connection.execute("DELETE FROM sessions WHERE expires_at < ?", (utc_now_iso(),))

    def latest_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT payload FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
            if not row:
                return None
            return json.loads(row["payload"])

    def query_history(
        self,
        metric: str,
        node_id: str,
        run_id: Optional[str],
        from_ts: str,
        to_ts: str,
    ) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            if run_id:
                rows = connection.execute(
                    """
                    SELECT ts, value FROM timeseries
                    WHERE metric = ? AND node_id = ? AND run_id = ? AND ts >= ? AND ts <= ?
                    ORDER BY ts ASC
                    """,
                    (metric, node_id, run_id, from_ts, to_ts),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT ts, value FROM timeseries
                    WHERE metric = ? AND node_id = ? AND run_id IS NULL AND ts >= ? AND ts <= ?
                    ORDER BY ts ASC
                    """,
                    (metric, node_id, from_ts, to_ts),
                ).fetchall()
        return [{"ts": row["ts"], "value": row["value"]} for row in rows]

    def list_queue_jobs(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT payload FROM queue_jobs ORDER BY created_at ASC, id ASC").fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def upsert_queue_job(self, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO queue_jobs(id, node_id, status, created_at, updated_at, payload) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(payload.get("id", "")),
                    str(payload.get("node_id", "")),
                    str(payload.get("status", "queued")),
                    str(payload.get("created_at", utc_now_iso())),
                    str(payload.get("updated_at", utc_now_iso())),
                    encoded,
                ),
            )
            connection.commit()

    def delete_queue_job(self, job_id: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM queue_jobs WHERE id = ?", (job_id,))
            connection.commit()

    def list_persisted_nodes(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT payload FROM persisted_nodes ORDER BY updated_at ASC, id ASC").fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def upsert_persisted_node(self, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO persisted_nodes(id, host, port, user, transport, updated_at, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(payload.get("id", "")),
                    str(payload.get("host", "")),
                    int(payload.get("port", 22)),
                    str(payload.get("user", "")),
                    str(payload.get("transport", "ssh")),
                    utc_now_iso(),
                    encoded,
                ),
            )
            connection.commit()

    def delete_persisted_node(self, node_id: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM persisted_nodes WHERE id = ?", (node_id,))
            connection.commit()
