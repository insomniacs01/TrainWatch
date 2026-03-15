import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .api_inputs import QueueJobInput, SSHConnectionInput, build_node_from_input, build_queue_job_from_input
from .auth import AuthPrincipal, ROLE_ADMIN, ROLE_OPERATOR, ROLE_VIEWER
from .config import load_config
from .errors import InputValidationError
from .runtime import TrainWatchRuntime
from .ssh_support import ssh_config_alias_records


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
WEBSOCKET_AUTH_TIMEOUT_SECONDS = 10
logger = logging.getLogger(__name__)


class LoginInput(BaseModel):
    username: str
    password: str


class BootstrapAdminInput(BaseModel):
    username: str
    password: str
    display_name: str = ""


class UserUpsertInput(BaseModel):
    username: str
    password: Optional[str] = None
    role: str = Field(default=ROLE_VIEWER)
    display_name: str = ""
    disabled: bool = False


def _parse_timestamp(value: Optional[str], default_delta_hours: Optional[int] = None) -> str:
    if not value:
        if default_delta_hours is None:
            return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        dt = datetime.now(timezone.utc) - timedelta(hours=default_delta_hours)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    raw = value.strip()
    if raw.isdigit():
        return datetime.fromtimestamp(float(raw), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _request_token(request: Request) -> str:
    header_token = request.headers.get("x-train-watch-token", "").strip()
    if header_token:
        return header_token
    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def _authorize_request(request: Request, required_role: str) -> AuthPrincipal:
    runtime: TrainWatchRuntime = request.app.state.runtime
    token = _request_token(request)
    try:
        principal = runtime.auth.require_token(token)
    except PermissionError as exc:
        client_host = request.client.host if request.client else "unknown"
        logger.warning("Rejected API request from %s: %s", client_host, exc)
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if not principal.has_role(required_role):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return principal


def require_viewer(request: Request) -> AuthPrincipal:
    return _authorize_request(request, ROLE_VIEWER)


def require_operator(request: Request) -> AuthPrincipal:
    return _authorize_request(request, ROLE_OPERATOR)


def require_admin(request: Request) -> AuthPrincipal:
    return _authorize_request(request, ROLE_ADMIN)


async def _authenticate_websocket(websocket: WebSocket, runtime: TrainWatchRuntime) -> Optional[AuthPrincipal]:
    if not runtime.auth.auth_required:
        return runtime.auth.public_principal()
    try:
        payload = await asyncio.wait_for(websocket.receive_json(), timeout=WEBSOCKET_AUTH_TIMEOUT_SECONDS)
    except Exception:
        await websocket.close(code=4401)
        logger.debug("Rejected websocket due to missing auth payload")
        return None

    if not isinstance(payload, dict) or payload.get("type") != "auth":
        await websocket.close(code=4401)
        logger.debug("Rejected websocket due to malformed auth payload")
        return None

    token = str(payload.get("token", "")).strip()
    try:
        return runtime.auth.require_token(token)
    except PermissionError:
        await websocket.close(code=4401)
        client_host = websocket.client.host if websocket.client else "unknown"
        logger.warning("Rejected websocket with invalid auth from %s", client_host)
        return None


def _input_error_to_http(exc: InputValidationError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


def _prometheus_metrics(snapshot: dict) -> str:
    summary = snapshot.get("summary", {}) or {}
    nodes = snapshot.get("nodes", []) or []
    lines = [
        "# HELP train_watch_nodes_total Total tracked nodes.",
        "# TYPE train_watch_nodes_total gauge",
        f"train_watch_nodes_total {int(summary.get('nodes_total', 0) or 0)}",
        "# HELP train_watch_runs_total Total tracked runs.",
        "# TYPE train_watch_runs_total gauge",
        f"train_watch_runs_total {int(summary.get('runs_total', 0) or 0)}",
        "# HELP train_watch_runs_alerting Total alerting runs.",
        "# TYPE train_watch_runs_alerting gauge",
        f"train_watch_runs_alerting {int(summary.get('runs_alerting', 0) or 0)}",
        "# HELP train_watch_gpus_busy Busy GPUs.",
        "# TYPE train_watch_gpus_busy gauge",
        f"train_watch_gpus_busy {int(summary.get('gpus_busy', 0) or 0)}",
    ]
    status_value = {"online": 1, "degraded": 0.5, "offline": 0, "connecting": 0.25}
    for node in nodes:
        node_id = str(node.get("id", "")).replace('"', "")
        label = str(node.get("label", node_id)).replace('"', "")
        lines.append(
            f'train_watch_node_status{{node_id="{node_id}",label="{label}"}} {status_value.get(str(node.get("status", "unknown")), -1)}'
        )
        metrics = node.get("metrics", {}) or {}
        for metric_name, value in (
            ("cpu_usage_percent", metrics.get("cpu_usage_percent")),
            ("memory_used_percent", metrics.get("memory_used_percent")),
            ("disk_used_percent", metrics.get("disk_used_percent")),
        ):
            if value is None:
                continue
            prom_name = metric_name.replace(".", "_")
            lines.append(f'train_watch_{prom_name}{{node_id="{node_id}",label="{label}"}} {float(value)}')
    return "\n".join(lines) + "\n"


def _payload_dict(model: BaseModel) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def create_app(runtime: TrainWatchRuntime) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="Train Watch", version="1.2.0", lifespan=lifespan)
    app.state.runtime = runtime
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/manifest.webmanifest")
    async def manifest() -> FileResponse:
        return FileResponse(STATIC_DIR / "manifest.webmanifest", media_type="application/manifest+json")

    @app.get("/service-worker.js")
    async def service_worker() -> FileResponse:
        return FileResponse(STATIC_DIR / "service-worker.js", media_type="application/javascript")

    @app.post("/api/v1/session/login")
    async def session_login(payload: LoginInput) -> dict:
        if not runtime.auth.user_auth_enabled:
            raise HTTPException(status_code=400, detail="Local user auth is not enabled")
        try:
            session = runtime.auth.login(payload.username, payload.password)
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        runtime.add_audit_log(payload.username, "session.login", "session", payload.username, "User logged in")
        return session

    @app.get("/api/v1/auth/config")
    async def auth_config() -> dict:
        return {
            "auth_required": runtime.auth.auth_required,
            "user_auth_enabled": runtime.auth.user_auth_enabled,
            "bootstrap_required": runtime.auth.bootstrap_required,
            "shared_token_enabled": bool(runtime.config.server.shared_token),
            "mode": runtime.auth.mode,
            "login_methods": [
                method
                for method, enabled in (
                    ("password", runtime.auth.user_auth_enabled),
                    ("token", bool(runtime.config.server.shared_token)),
                )
                if enabled
            ],
        }

    @app.post("/api/v1/session/bootstrap-admin")
    async def bootstrap_admin(payload: BootstrapAdminInput) -> dict:
        try:
            item = runtime.auth.bootstrap_admin(
                username=payload.username,
                password=payload.password,
                display_name=payload.display_name,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        runtime.add_audit_log(item["username"], "users.bootstrap_admin", "user", item["username"], "Created initial admin")
        session = runtime.auth.login(payload.username, payload.password)
        return {
            "bootstrap": item,
            "token": session["token"],
            "expires_at": session["expires_at"],
            "user": session["user"],
        }

    @app.post("/api/v1/session/logout")
    async def session_logout(request: Request, principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        token = _request_token(request)
        runtime.auth.logout(token)
        runtime.add_audit_log(principal.username, "session.logout", "session", principal.username, "User logged out")
        return {"ok": True}

    @app.get("/api/v1/session/me")
    async def session_me(request: Request, principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        token = _request_token(request)
        session = runtime.auth.session_summary(token)
        return {
            "auth_required": runtime.auth.auth_required,
            "user_auth_enabled": runtime.auth.user_auth_enabled,
            "session": session,
            "user": principal.to_dict(),
        }

    @app.get("/api/v1/health")
    async def health(principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        snapshot = runtime.snapshot_dict()
        return {
            "ok": True,
            "generated_at": snapshot.get("generated_at"),
            "nodes_total": snapshot.get("summary", {}).get("nodes_total", 0),
            "runs_total": snapshot.get("summary", {}).get("runs_total", 0),
            "user": principal.to_dict(),
        }

    @app.get("/api/v1/snapshot")
    async def snapshot(_principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        return runtime.snapshot_dict()

    @app.get("/api/v1/metrics")
    async def metrics(_principal: AuthPrincipal = Depends(require_viewer)) -> PlainTextResponse:
        return PlainTextResponse(_prometheus_metrics(runtime.snapshot_dict()))

    @app.get("/api/v1/ssh-aliases")
    async def ssh_aliases(_principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        return {"items": ssh_config_alias_records()}

    @app.post("/api/v1/refresh")
    async def refresh(_principal: AuthPrincipal = Depends(require_operator)) -> dict:
        payload = await runtime.refresh_once()
        return payload["snapshot"]

    @app.get("/api/v1/history")
    async def history(
        metric: str,
        node_id: str,
        run_id: Optional[str] = None,
        from_ts: Optional[str] = Query(default=None, alias="from"),
        to_ts: Optional[str] = Query(default=None, alias="to"),
        _principal: AuthPrincipal = Depends(require_viewer),
    ) -> dict:
        if not metric or not node_id:
            raise HTTPException(status_code=400, detail="metric and node_id are required")
        start = _parse_timestamp(from_ts, default_delta_hours=6)
        end = _parse_timestamp(to_ts, default_delta_hours=None)
        points = runtime.store.query_history(metric, node_id, run_id, start, end)
        return {
            "metric": metric,
            "node_id": node_id,
            "run_id": run_id,
            "from": start,
            "to": end,
            "points": points,
        }

    @app.get("/api/v1/connections")
    async def list_connections(_principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        return {"items": runtime.connection_summaries()}

    @app.get("/api/v1/jobs")
    async def list_jobs(node_id: Optional[str] = None, _principal: AuthPrincipal = Depends(require_viewer)) -> dict:
        return runtime.job_summaries(node_id=node_id)

    @app.get("/api/v1/alerts")
    async def list_alerts(
        limit: int = 100,
        acknowledged: Optional[bool] = None,
        _principal: AuthPrincipal = Depends(require_viewer),
    ) -> dict:
        return {"items": runtime.list_alert_events(limit=limit, acknowledged=acknowledged)}

    @app.post("/api/v1/alerts/{alert_id}/ack")
    async def acknowledge_alert(
        alert_id: str,
        principal: AuthPrincipal = Depends(require_operator),
    ) -> dict:
        item = runtime.acknowledge_alert_event(alert_id, principal.username)
        if item is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        runtime.add_audit_log(principal.username, "alerts.ack", "alert", alert_id, "Acknowledged alert")
        return {"item": item}

    @app.get("/api/v1/audit-logs")
    async def audit_logs(limit: int = 100, _principal: AuthPrincipal = Depends(require_admin)) -> dict:
        return {"items": runtime.list_audit_logs(limit=limit)}

    @app.get("/api/v1/users")
    async def users(_principal: AuthPrincipal = Depends(require_admin)) -> dict:
        return {"items": runtime.auth.list_users()}

    @app.post("/api/v1/users")
    async def create_user(payload: UserUpsertInput, principal: AuthPrincipal = Depends(require_admin)) -> dict:
        try:
            item = runtime.auth.create_or_update_user(
                username=payload.username,
                password=payload.password,
                role=payload.role,
                display_name=payload.display_name,
                disabled=payload.disabled,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        runtime.add_audit_log(principal.username, "users.upsert", "user", item["username"], "Created or updated user")
        return {"item": item}

    @app.patch("/api/v1/users/{username}")
    async def update_user(
        username: str,
        payload: UserUpsertInput,
        principal: AuthPrincipal = Depends(require_admin),
    ) -> dict:
        try:
            item = runtime.auth.create_or_update_user(
                username=username,
                password=payload.password,
                role=payload.role,
                display_name=payload.display_name or username,
                disabled=payload.disabled,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        runtime.add_audit_log(principal.username, "users.upsert", "user", username, "Created or updated user")
        return {"item": item}

    @app.post("/api/v1/jobs")
    async def add_job(payload: QueueJobInput, principal: AuthPrincipal = Depends(require_operator)) -> dict:
        try:
            payload_data = _payload_dict(payload)
            owner = str(payload_data.get("owner", "")).strip()
            if owner in {"", "anonymous", "匿名", "鍖垮悕"}:
                payload_data["owner"] = principal.display_name or principal.username
            job = build_queue_job_from_input(QueueJobInput(**payload_data), runtime.find_node)
            item = await runtime.enqueue_job(job)
        except InputValidationError as exc:
            raise _input_error_to_http(exc) from exc
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "not found" in detail.lower() else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        runtime.add_audit_log(principal.username, "jobs.enqueue", "job", item.get("id", ""), "Queued a training job")
        return {"item": item}

    @app.delete("/api/v1/jobs/{job_id}")
    async def cancel_job(job_id: str, principal: AuthPrincipal = Depends(require_operator)) -> dict:
        try:
            item = await runtime.cancel_job(job_id)
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "not found" in detail.lower() else 409
            raise HTTPException(status_code=status_code, detail=detail) from exc
        runtime.add_audit_log(principal.username, "jobs.cancel", "job", job_id, "Canceled queued job")
        return {"item": item}

    @app.post("/api/v1/connections")
    async def add_connection(payload: SSHConnectionInput, principal: AuthPrincipal = Depends(require_operator)) -> dict:
        try:
            node = build_node_from_input(payload)
            item = await runtime.add_node(node)
        except InputValidationError as exc:
            raise _input_error_to_http(exc) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        runtime.add_audit_log(principal.username, "connections.add", "node", item.get("id", ""), "Added SSH connection")
        return {"item": item}

    @app.delete("/api/v1/connections/{node_id}")
    async def delete_connection(node_id: str, principal: AuthPrincipal = Depends(require_operator)) -> dict:
        deleted = await runtime.remove_node(node_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Connection not found")
        runtime.add_audit_log(principal.username, "connections.delete", "node", node_id, "Removed SSH connection")
        return {"ok": True}

    @app.websocket("/api/v1/stream")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        principal = await _authenticate_websocket(websocket, runtime)
        if principal is None:
            return
        await runtime.hub.connect(websocket, already_accepted=True)
        await websocket.send_json({"type": "snapshot", "snapshot": runtime.snapshot_dict(), "events": [], "user": principal.to_dict()})
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            runtime.hub.disconnect(websocket)
        except Exception:
            runtime.hub.disconnect(websocket)

    return app


def build_app() -> FastAPI:
    config_path = os.environ.get("TRAIN_WATCH_CONFIG", str(BASE_DIR / "config.empty.yaml"))
    config = load_config(config_path)
    return create_app(TrainWatchRuntime(config))
