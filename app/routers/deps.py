import asyncio
import logging
from typing import Optional

from fastapi import HTTPException, Request, WebSocket
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

from ..auth import ROLE_ADMIN, ROLE_OPERATOR, ROLE_VIEWER, AuthPrincipal
from ..errors import InputValidationError
from ..runtime import TrainWatchRuntime

WEBSOCKET_AUTH_TIMEOUT_SECONDS = 10
logger = logging.getLogger(__name__)


def get_runtime(request: Request) -> TrainWatchRuntime:
    return request.app.state.runtime


def request_token(request: Request) -> str:
    header_token = request.headers.get("x-train-watch-token", "").strip()
    if header_token:
        return header_token
    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def _authorize_request(request: Request, required_role: str) -> AuthPrincipal:
    runtime = get_runtime(request)
    token = request_token(request)
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


async def authenticate_websocket(websocket: WebSocket, runtime: TrainWatchRuntime) -> Optional[AuthPrincipal]:
    if not runtime.auth.auth_required:
        return runtime.auth.public_principal()
    try:
        payload = await asyncio.wait_for(websocket.receive_json(), timeout=WEBSOCKET_AUTH_TIMEOUT_SECONDS)
    except (asyncio.TimeoutError, TypeError, ValueError, WebSocketDisconnect):
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


def input_error_to_http(exc: InputValidationError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


def prometheus_metrics(snapshot: dict) -> str:
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
        status = str(node.get("status", "unknown"))
        lines.append(
            f'train_watch_node_status{{node_id="{node_id}",label="{label}"}} '
            f"{status_value.get(status, -1)}"
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


def payload_dict(model: BaseModel) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()
