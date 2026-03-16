import json
from typing import Any, Dict, List, Optional

from .models import AlertEvent
from .time_utils import utc_now_iso


class SQLiteRecordStoreMixin:
    def user_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"] if row else 0)

    def get_user_record(self, username: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT username, password_hash, role, display_name, disabled, created_at, updated_at "
                "FROM users WHERE username = ?",
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
                INSERT OR REPLACE INTO users(
                    username, password_hash, role, display_name, disabled, created_at, updated_at
                )
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
                INSERT OR REPLACE INTO sessions(
                    token, username, role, display_name, created_at, last_seen_at, expires_at
                )
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
                "SELECT token, username, role, display_name, created_at, last_seen_at, expires_at "
                "FROM sessions WHERE token = ?",
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
        event_id = str(
            payload.get("id") or payload.get("dedupe_key") or f"event-{payload.get('at', '')}-{payload.get('kind', '')}"
        )
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
                "SELECT id, at, username, action, target_type, target_id, message, details "
                "FROM audit_logs ORDER BY at DESC LIMIT ?",
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
