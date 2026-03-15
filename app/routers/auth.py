from fastapi import APIRouter, Depends, HTTPException, Request

from ..auth import AuthPrincipal
from ..runtime import TrainWatchRuntime
from .deps import get_runtime, request_token, require_admin, require_viewer
from .schemas import BootstrapAdminInput, LoginInput, UserUpsertInput


router = APIRouter()


@router.post("/api/v1/session/login")
async def session_login(payload: LoginInput, runtime: TrainWatchRuntime = Depends(get_runtime)) -> dict:
    if not runtime.auth.user_auth_enabled:
        raise HTTPException(status_code=400, detail="Local user auth is not enabled")
    try:
        session = runtime.auth.login(payload.username, payload.password)
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    runtime.add_audit_log(payload.username, "session.login", "session", payload.username, "User logged in")
    return session


@router.get("/api/v1/auth/config")
async def auth_config(runtime: TrainWatchRuntime = Depends(get_runtime)) -> dict:
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


@router.post("/api/v1/session/bootstrap-admin")
async def bootstrap_admin(payload: BootstrapAdminInput, runtime: TrainWatchRuntime = Depends(get_runtime)) -> dict:
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


@router.post("/api/v1/session/logout")
async def session_logout(
    request: Request,
    principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    token = request_token(request)
    runtime.auth.logout(token)
    runtime.add_audit_log(principal.username, "session.logout", "session", principal.username, "User logged out")
    return {"ok": True}


@router.get("/api/v1/session/me")
async def session_me(
    request: Request,
    principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    token = request_token(request)
    session = runtime.auth.session_summary(token)
    return {
        "auth_required": runtime.auth.auth_required,
        "user_auth_enabled": runtime.auth.user_auth_enabled,
        "session": session,
        "user": principal.to_dict(),
    }


@router.get("/api/v1/audit-logs")
async def audit_logs(
    limit: int = 100,
    _principal: AuthPrincipal = Depends(require_admin),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return {"items": runtime.list_audit_logs(limit=limit)}


@router.get("/api/v1/users")
async def users(
    _principal: AuthPrincipal = Depends(require_admin),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return {"items": runtime.auth.list_users()}


@router.post("/api/v1/users")
async def create_user(
    payload: UserUpsertInput,
    principal: AuthPrincipal = Depends(require_admin),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
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


@router.patch("/api/v1/users/{username}")
async def update_user(
    username: str,
    payload: UserUpsertInput,
    principal: AuthPrincipal = Depends(require_admin),
    runtime: TrainWatchRuntime = Depends(get_runtime),
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
