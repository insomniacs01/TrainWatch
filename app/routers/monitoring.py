import sys
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse

from ..auth import AuthPrincipal
from ..runtime import TrainWatchRuntime
from ..ssh_support import ssh_config_alias_records as load_ssh_alias_records
from ..time_utils import coerce_utc_timestamp
from .deps import get_runtime, prometheus_metrics, require_operator, require_viewer


router = APIRouter()


def _ssh_alias_records() -> list:
    main_module = sys.modules.get("app.main")
    if main_module is not None and hasattr(main_module, "ssh_config_alias_records"):
        return main_module.ssh_config_alias_records()
    return load_ssh_alias_records()


@router.get("/api/v1/health")
async def health(
    principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    snapshot = runtime.snapshot_dict()
    return {
        "ok": True,
        "generated_at": snapshot.get("generated_at"),
        "nodes_total": snapshot.get("summary", {}).get("nodes_total", 0),
        "runs_total": snapshot.get("summary", {}).get("runs_total", 0),
        "user": principal.to_dict(),
    }


@router.get("/api/v1/snapshot")
async def snapshot(
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return runtime.snapshot_dict()


@router.get("/api/v1/metrics")
async def metrics(
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> PlainTextResponse:
    return PlainTextResponse(prometheus_metrics(runtime.snapshot_dict()))


@router.get("/api/v1/ssh-aliases")
async def ssh_aliases(_principal: AuthPrincipal = Depends(require_viewer)) -> dict:
    return {"items": _ssh_alias_records()}


@router.post("/api/v1/refresh")
async def refresh(
    _principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    payload = await runtime.refresh_once()
    return payload["snapshot"]


@router.get("/api/v1/history")
async def history(
    metric: str,
    node_id: str,
    run_id: Optional[str] = None,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    if not metric or not node_id:
        raise HTTPException(status_code=400, detail="metric and node_id are required")
    start = coerce_utc_timestamp(from_ts, default_delta_hours=6)
    end = coerce_utc_timestamp(to_ts, default_delta_hours=None)
    points = runtime.store.query_history(metric, node_id, run_id, start, end)
    return {
        "metric": metric,
        "node_id": node_id,
        "run_id": run_id,
        "from": start,
        "to": end,
        "points": points,
    }


@router.get("/api/v1/alerts")
async def list_alerts(
    limit: int = 100,
    acknowledged: Optional[bool] = None,
    _principal: AuthPrincipal = Depends(require_viewer),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    return {"items": runtime.list_alert_events(limit=limit, acknowledged=acknowledged)}


@router.post("/api/v1/alerts/{alert_id}/ack")
async def acknowledge_alert(
    alert_id: str,
    principal: AuthPrincipal = Depends(require_operator),
    runtime: TrainWatchRuntime = Depends(get_runtime),
) -> dict:
    item = runtime.acknowledge_alert_event(alert_id, principal.username)
    if item is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    runtime.add_audit_log(principal.username, "alerts.ack", "alert", alert_id, "Acknowledged alert")
    return {"item": item}
