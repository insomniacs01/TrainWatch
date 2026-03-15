from fastapi import FastAPI

from .auth import router as auth_router
from .management import router as management_router
from .monitoring import router as monitoring_router
from .stream import router as stream_router


def register_routers(app: FastAPI) -> None:
    app.include_router(auth_router)
    app.include_router(monitoring_router)
    app.include_router(management_router)
    app.include_router(stream_router)
