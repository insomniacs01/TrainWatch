import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import AppSnapshot, AlertEvent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SQLiteStore:
    def __init__(self, path_value: str, retention_days: int) -> None:
        self.path = Path(path_value)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    ts TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS timeseries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    run_id TEXT,
                    metric TEXT NOT NULL,
                    value REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_timeseries_lookup
                ON timeseries(node_id, run_id, metric, ts);

                CREATE TABLE IF NOT EXISTS queue_jobs (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_queue_jobs_lookup
                ON queue_jobs(node_id, status, created_at);

                CREATE TABLE IF NOT EXISTS persisted_nodes (
                    id TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    user TEXT NOT NULL,
                    transport TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_persisted_nodes_lookup
                ON persisted_nodes(host, port, user, transport, updated_at);

                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    disabled INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    token TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    role TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sessions_lookup
                ON sessions(username, expires_at);

                CREATE TABLE IF NOT EXISTS alert_events (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_label TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    run_label TEXT NOT NULL,
                    status TEXT NOT NULL,
                    previous_status TEXT NOT NULL,
                    at TEXT NOT NULL,
                    message TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'info',
                    source TEXT NOT NULL DEFAULT 'runtime',
                    dedupe_key TEXT NOT NULL DEFAULT '',
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    acknowledged_at TEXT NOT NULL DEFAULT '',
                    acknowledged_by TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_alert_events_time
                ON alert_events(at DESC);

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id TEXT PRIMARY KEY,
                    at TEXT NOT NULL,
                    username TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_audit_logs_time
                ON audit_logs(at DESC);
                """
            )

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
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.retention_days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        connection.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
        connection.execute("DELETE FROM timeseries WHERE ts < ?", (cutoff,))
        connection.execute("DELETE FROM alert_events WHERE at < ?", (cutoff,))
        connection.execute("DELETE FROM audit_logs WHERE at < ?", (cutoff,))
        connection.execute("DELETE FROM sessions WHERE expires_at < ?", (utc_now_iso(),))

    def latest_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
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
            rows = connection.execute(
                "SELECT payload FROM queue_jobs ORDER BY created_at ASC, id ASC"
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def upsert_queue_job(self, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO queue_jobs(id, node_id, status, created_at, updated_at, payload) VALUES (?, ?, ?, ?, ?, ?)",
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
            rows = connection.execute(
                "SELECT payload FROM persisted_nodes ORDER BY updated_at ASC, id ASC"
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def upsert_persisted_node(self, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO persisted_nodes(id, host, port, user, transport, updated_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
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

    def user_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"] if row else 0)

    def get_user_record(self, username: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT username, password_hash, role, display_name, disabled, created_at, updated_at FROM users WHERE username = ?",
                (str(username or "").strip().lower(),),
            ).fetchone()
        return dict(row) if row else None

    def list_users(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT username, role, display_name, disabled, created_at, updated_at FROM users ORDER BY username ASC"
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_user(
        self,
        username: str,
        password_hash: str,
        role: str,
        display_name: str,
        disabled: bool,
        created_at: str,
        updated_at: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO users(username, password_hash, role, display_name, disabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(username or "").strip().lower(),
                    str(password_hash or ""),
                    str(role or "viewer"),
                    str(display_name or username),
                    1 if disabled else 0,
                    str(created_at or utc_now_iso()),
                    str(updated_at or utc_now_iso()),
                ),
            )
            connection.commit()

    def create_session(self, payload: Dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO sessions(token, username, role, display_name, created_at, last_seen_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("token", "")),
                    str(payload.get("username", "")),
                    str(payload.get("role", "viewer")),
                    str(payload.get("display_name", payload.get("username", ""))),
                    str(payload.get("created_at", utc_now_iso())),
                    str(payload.get("last_seen_at", utc_now_iso())),
                    str(payload.get("expires_at", utc_now_iso())),
                ),
            )
            connection.commit()

    def get_session(self, token: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT token, username, role, display_name, created_at, last_seen_at, expires_at FROM sessions WHERE token = ?",
                (str(token or "").strip(),),
            ).fetchone()
            if not row:
                return None
            expires_at = str(row["expires_at"])
            if expires_at <= utc_now_iso():
                connection.execute("DELETE FROM sessions WHERE token = ?", (str(token or "").strip(),))
                connection.commit()
                return None
            connection.execute(
                "UPDATE sessions SET last_seen_at = ? WHERE token = ?",
                (utc_now_iso(), str(token or "").strip()),
            )
            connection.commit()
        updated = dict(row)
        updated["last_seen_at"] = utc_now_iso()
        return updated

    def delete_session(self, token: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM sessions WHERE token = ?", (str(token or "").strip(),))
            connection.commit()

    def add_alert_event(self, event: AlertEvent) -> None:
        payload = event.to_dict()
        event_id = str(payload.get("id") or payload.get("dedupe_key") or f"event-{payload.get('at','')}-{payload.get('kind','')}")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO alert_events(
                    id, kind, node_id, node_label, run_id, run_label, status, previous_status, at,
                    message, severity, source, dedupe_key, acknowledged, acknowledged_at, acknowledged_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    str(payload.get("kind", "")),
                    str(payload.get("node_id", "")),
                    str(payload.get("node_label", "")),
                    str(payload.get("run_id", "")),
                    str(payload.get("run_label", "")),
                    str(payload.get("status", "")),
                    str(payload.get("previous_status", "")),
                    str(payload.get("at", utc_now_iso())),
                    str(payload.get("message", "")),
                    str(payload.get("severity", "info")),
                    str(payload.get("source", "runtime")),
                    str(payload.get("dedupe_key", "")),
                    1 if payload.get("acknowledged") else 0,
                    str(payload.get("acknowledged_at", "")),
                    str(payload.get("acknowledged_by", "")),
                ),
            )
            connection.commit()

    def list_alert_events(self, limit: int = 100, acknowledged: Optional[bool] = None) -> List[Dict[str, Any]]:
        limit_value = max(1, min(500, int(limit or 100)))
        query = "SELECT * FROM alert_events"
        args: List[Any] = []
        if acknowledged is not None:
            query += " WHERE acknowledged = ?"
            args.append(1 if acknowledged else 0)
        query += " ORDER BY at DESC LIMIT ?"
        args.append(limit_value)
        with self._connect() as connection:
            rows = connection.execute(query, tuple(args)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["acknowledged"] = bool(item.get("acknowledged"))
            items.append(item)
        return items

    def acknowledge_alert_event(self, event_id: str, username: str) -> Optional[Dict[str, Any]]:
        now_value = utc_now_iso()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM alert_events WHERE id = ?",
                (str(event_id or "").strip(),),
            ).fetchone()
            if not row:
                return None
            connection.execute(
                "UPDATE alert_events SET acknowledged = 1, acknowledged_at = ?, acknowledged_by = ? WHERE id = ?",
                (now_value, str(username or "system"), str(event_id or "").strip()),
            )
            connection.commit()
        item = dict(row)
        item["acknowledged"] = True
        item["acknowledged_at"] = now_value
        item["acknowledged_by"] = str(username or "system")
        return item

    def add_audit_log(
        self,
        log_id: str,
        at: str,
        username: str,
        action: str,
        target_type: str,
        target_id: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO audit_logs(id, at, username, action, target_type, target_id, message, details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(log_id or ""),
                    str(at or utc_now_iso()),
                    str(username or "system"),
                    str(action or "unknown"),
                    str(target_type or "system"),
                    str(target_id or ""),
                    str(message or ""),
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )
            connection.commit()

    def list_audit_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit_value = max(1, min(500, int(limit or 100)))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id, at, username, action, target_type, target_id, message, details FROM audit_logs ORDER BY at DESC LIMIT ?",
                (limit_value,),
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            try:
                item["details"] = json.loads(str(item.get("details", "{}")) or "{}")
            except json.JSONDecodeError:
                item["details"] = {}
            items.append(item)
        return items
