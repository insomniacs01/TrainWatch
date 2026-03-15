from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from ..api_inputs import QueueJobInput, SSHConnectionInput, build_node_from_input, build_queue_job_from_input
from ..auth import AuthPrincipal
from ..errors import InputValidationError
from ..runtime import TrainWatchRuntime
from .deps import get_runtime, input_error_to_http, payload_dict, require_operator, require_viewer


router = APIRouter()


@router.get("/api/v1/connections")
async def list_connections(
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return {"items": runtime.connection_summaries()}


@router.get("/api/v1/jobs")
async def list_jobs(
    node_id: Optional[str] = None,
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return runtime.job_summaries(node_id=node_id)


@router.post("/api/v1/jobs")
async def add_job(
    payload: QueueJobInput,
    principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    try:
        payload_data = payload_dict(payload)
        owner = str(payload_data.get("owner", "")).strip()
        if owner in {"", "anonymous", "??"}:
            payload_data["owner"] = principal.display_name or principal.username
        job = build_queue_job_from_input(QueueJobInput(**payload_data), runtime.find_node)
        item = await runtime.enqueue_job(job)
    except InputValidationError as exc:
        raise input_error_to_http(exc) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    runtime.add_audit_log(principal.username, "jobs.enqueue", "job", item.get("id", ""), "Queued a training job")
    return {"item": item}


@router.delete("/api/v1/jobs/{job_id}")
async def cancel_job(
    job_id: str,
    principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    try:
        item = await runtime.cancel_job(job_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 409
        raise HTTPException(status_code=status_code, detail=detail) from exc
    runtime.add_audit_log(principal.username, "jobs.cancel", "job", job_id, "Canceled queued job")
    return {"item": item}


@router.post("/api/v1/connections")
async def add_connection(
    payload: SSHConnectionInput,
    principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    try:
        node = build_node_from_input(payload)
        item = await runtime.add_node(node)
    except InputValidationError as exc:
        raise input_error_to_http(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    runtime.add_audit_log(principal.username, "connections.add", "node", item.get("id", ""), "Added SSH connection")
    return {"item": item}


@router.delete("/api/v1/connections/{node_id}")
async def delete_connection(
    node_id: str,
    principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    deleted = await runtime.remove_node(node_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Connection not found")
    runtime.add_audit_log(principal.username, "connections.delete", "node", node_id, "Removed SSH connection")
    return {"ok": True}
