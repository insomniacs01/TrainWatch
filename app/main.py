import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import AppConfig, NodeConfig, RunConfig, load_config
from .runtime import TrainWatchRuntime
from .ssh_support import ssh_config_alias_exists, ssh_config_alias_records


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"


class RunConnectionInput(BaseModel):
    label: str = "Main Run"
    log_path: Optional[str] = None
    log_glob: Optional[str] = None
    process_match: str = ""
    parser: str = "auto"
    stall_after_seconds: int = 900
    completion_regex: str = r"(Training complete|Finished training|saving final checkpoint)"
    error_regex: str = r"(Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)"


class SSHConnectionInput(BaseModel):
    label: Optional[str] = None
    host: str
    port: int = 22
    user: Optional[str] = ""
    password: Optional[str] = None
    key_path: Optional[str] = None
    runs: List[RunConnectionInput] = Field(default_factory=list)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "node"


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


def _check_token(expected_token: str, actual_token: str) -> None:
    if expected_token and actual_token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_token(request: Request) -> None:
    runtime = request.app.state.runtime
    expected = runtime.config.server.shared_token
    actual = request.headers.get("x-train-watch-token", "")
    _check_token(expected, actual)


def _build_node_from_input(payload: SSHConnectionInput) -> NodeConfig:
    password = (payload.password or "").strip()
    key_path = str(Path(payload.key_path).expanduser()) if payload.key_path else ""
    host = payload.host.strip()
    user = (payload.user or "").strip()
    if not host:
        raise HTTPException(status_code=400, detail="host is required")
    if not user and not ssh_config_alias_exists(host):
        raise HTTPException(status_code=400, detail="user is required unless host is a local SSH config alias")

    label = (payload.label or host).strip() or host
    node_id = f"{_slugify(label)}-{uuid4().hex[:8]}"
    runs = []
    for index, run in enumerate(payload.runs):
        if not run.log_path and not run.log_glob and not run.process_match:
            continue
        if not run.log_path and not run.log_glob:
            raise HTTPException(status_code=400, detail="run.log_path or run.log_glob is required when adding a run")
        run_label = (run.label or f"Run {index + 1}").strip() or f"Run {index + 1}"
        runs.append(
            RunConfig(
                id=f"{_slugify(run_label)}-{uuid4().hex[:8]}",
                label=run_label,
                log_path=run.log_path,
                log_glob=run.log_glob,
                process_match=run.process_match,
                parser=run.parser,
                stall_after_seconds=max(30, run.stall_after_seconds),
                completion_regex=run.completion_regex,
                error_regex=run.error_regex,
            )
        )

    return NodeConfig(
        id=node_id,
        label=label,
        host=host,
        port=max(1, payload.port),
        user=user,
        key_path=key_path,
        password=password,
        runs=runs,
        transport="ssh",
    )


def create_app(runtime: TrainWatchRuntime) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="Train Watch", version="1.1.0", lifespan=lifespan)
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

    @app.get("/api/v1/health", dependencies=[Depends(require_token)])
    async def health() -> dict:
        snapshot = runtime.snapshot_dict()
        return {
            "ok": True,
            "generated_at": snapshot.get("generated_at"),
            "nodes_total": snapshot.get("summary", {}).get("nodes_total", 0),
            "runs_total": snapshot.get("summary", {}).get("runs_total", 0),
        }

    @app.get("/api/v1/snapshot", dependencies=[Depends(require_token)])
    async def snapshot() -> dict:
        return runtime.snapshot_dict()

    @app.get("/api/v1/ssh-aliases", dependencies=[Depends(require_token)])
    async def ssh_aliases() -> dict:
        return {"items": ssh_config_alias_records()}

    @app.post("/api/v1/refresh", dependencies=[Depends(require_token)])
    async def refresh() -> dict:
        payload = await runtime.refresh_once()
        return payload["snapshot"]

    @app.get("/api/v1/history", dependencies=[Depends(require_token)])
    async def history(
        metric: str,
        node_id: str,
        run_id: Optional[str] = None,
        from_ts: Optional[str] = Query(default=None, alias="from"),
        to_ts: Optional[str] = Query(default=None, alias="to"),
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

    @app.get("/api/v1/connections", dependencies=[Depends(require_token)])
    async def list_connections() -> dict:
        return {"items": runtime.connection_summaries()}

    @app.post("/api/v1/connections", dependencies=[Depends(require_token)])
    async def add_connection(payload: SSHConnectionInput) -> dict:
        node = _build_node_from_input(payload)
        try:
            item = await runtime.add_node(node)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"item": item}

    @app.delete("/api/v1/connections/{node_id}", dependencies=[Depends(require_token)])
    async def delete_connection(node_id: str) -> dict:
        deleted = await runtime.remove_node(node_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Connection not found")
        return {"ok": True}

    @app.websocket("/api/v1/stream")
    async def stream(websocket: WebSocket) -> None:
        expected = runtime.config.server.shared_token
        token = websocket.query_params.get("token", "")
        if expected and token != expected:
            await websocket.close(code=4401)
            return
        await runtime.hub.connect(websocket)
        await websocket.send_json({"type": "snapshot", "snapshot": runtime.snapshot_dict(), "events": []})
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            runtime.hub.disconnect(websocket)
        except Exception:
            runtime.hub.disconnect(websocket)

    return app


def build_app() -> FastAPI:
    config_path = os.environ.get("TRAIN_WATCH_CONFIG", str(BASE_DIR / "config.yaml"))
    config = load_config(config_path)
    return create_app(TrainWatchRuntime(config))
