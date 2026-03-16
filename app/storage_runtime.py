import json
from typing import Any, Dict, List

from .time_utils import utc_now_iso


class SQLiteRuntimeStoreMixin:
    def get_setting(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute("SELECT value FROM app_settings WHERE key = ?", (str(key or ""),)).fetchone()
        return str(row["value"]) if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO app_settings(key, value, updated_at) VALUES (?, ?, ?)",
                (str(key or ""), str(value or ""), utc_now_iso()),
            )
            connection.commit()

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
