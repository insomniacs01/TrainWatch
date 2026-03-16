import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import load_config
from .routers import register_routers
from .runtime import TrainWatchRuntime
from .ssh_support import ssh_config_alias_records as ssh_config_alias_records

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"


def _register_static_routes(app: FastAPI) -> None:
    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/manifest.webmanifest")
    async def manifest() -> FileResponse:
        return FileResponse(STATIC_DIR / "manifest.webmanifest", media_type="application/manifest+json")

    @app.get("/service-worker.js")
    async def service_worker() -> FileResponse:
        return FileResponse(STATIC_DIR / "service-worker.js", media_type="application/javascript")


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
    _register_static_routes(app)
    register_routers(app)
    return app


def build_app() -> FastAPI:
    config_path = os.environ.get("TRAIN_WATCH_CONFIG", str(BASE_DIR / "config.empty.yaml"))
    config = load_config(config_path)
    return create_app(TrainWatchRuntime(config))
