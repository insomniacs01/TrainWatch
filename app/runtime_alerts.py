from typing import Any, Dict, List, Optional, Tuple

from .config import ServerConfig
from .models import AlertEvent, AppSnapshot


def diff_run_status_events(
    previous_snapshot: Optional[AppSnapshot],
    current_snapshot: AppSnapshot,
) -> List[AlertEvent]:
    if previous_snapshot is None:
        return []

    previous_map: Dict[Tuple[str, str], str] = {}
    for node in previous_snapshot.nodes:
        for run in node.runs:
            previous_map[(node.id, run.id)] = run.status

    events: List[AlertEvent] = []
    for node in current_snapshot.nodes:
        for run in node.runs:
            previous_status = previous_map.get((node.id, run.id), "")
            if previous_status in {"", "connecting"}:
                continue
            if previous_status == run.status:
                continue
            events.append(
                AlertEvent(
                    kind="run_status_changed",
                    node_id=node.id,
                    node_label=node.label,
                    run_id=run.id,
                    run_label=run.label,
                    status=run.status,
                    previous_status=previous_status,
                    at=current_snapshot.generated_at,
                    message="%s / %s: %s -> %s" % (node.label, run.label, previous_status, run.status),
                )
            )
    return events


def diff_runtime_events(
    previous_snapshot: Optional[AppSnapshot],
    current_snapshot: AppSnapshot,
) -> List[AlertEvent]:
    events = list(diff_run_status_events(previous_snapshot, current_snapshot))
    if previous_snapshot is None:
        return events

    previous_node_status = {node.id: node.status for node in previous_snapshot.nodes}
    for node in current_snapshot.nodes:
        previous_status = previous_node_status.get(node.id, "")
        if (
            previous_status
            and previous_status not in {"connecting"}
            and previous_status != node.status
            and node.status in {"online", "offline"}
        ):
            events.append(
                AlertEvent(
                    id=f"node-status-{node.id}-{current_snapshot.generated_at}",
                    kind="node_status_changed",
                    node_id=node.id,
                    node_label=node.label,
                    run_id="",
                    run_label="",
                    status=node.status,
                    previous_status=previous_status,
                    at=current_snapshot.generated_at,
                    message="%s: %s -> %s" % (node.label, previous_status, node.status),
                    severity="critical" if node.status == "offline" else "warning",
                    source="runtime",
                    dedupe_key=f"node-status:{node.id}:{node.status}",
                )
            )
    return events


def build_current_alerts(snapshot: AppSnapshot, server: ServerConfig) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for node in snapshot.nodes:
        if node.status in {"offline", "degraded"}:
            items.append(
                {
                    "id": f"current-node:{node.id}:{node.status}",
                    "kind": "current_node_alert",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": "",
                    "run_label": "",
                    "status": node.status,
                    "at": node.collected_at or snapshot.generated_at,
                    "message": f"{node.label}: {node.status}{f' / {node.error}' if node.error else ''}",
                    "severity": "critical" if node.status == "offline" else "warning",
                }
            )

        cpu_value = float(node.metrics.get("cpu_usage_percent", 0.0) or 0.0)
        if cpu_value >= server.cpu_alert_percent:
            items.append(
                {
                    "id": f"metric-cpu:{node.id}",
                    "kind": "metric_threshold",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": "",
                    "run_label": "",
                    "status": "alert",
                    "at": node.collected_at or snapshot.generated_at,
                    "message": f"{node.label}: CPU {cpu_value:.1f}% >= {server.cpu_alert_percent:.1f}%",
                    "severity": "warning",
                }
            )

        memory_value = float(node.metrics.get("memory_used_percent", 0.0) or 0.0)
        if memory_value >= server.memory_alert_percent:
            items.append(
                {
                    "id": f"metric-memory:{node.id}",
                    "kind": "metric_threshold",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": "",
                    "run_label": "",
                    "status": "alert",
                    "at": node.collected_at or snapshot.generated_at,
                    "message": f"{node.label}: memory {memory_value:.1f}% >= {server.memory_alert_percent:.1f}%",
                    "severity": "warning",
                }
            )

        disk_value = float(node.metrics.get("disk_used_percent", 0.0) or 0.0)
        if disk_value >= server.disk_alert_percent:
            items.append(
                {
                    "id": f"metric-disk:{node.id}",
                    "kind": "metric_threshold",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": "",
                    "run_label": "",
                    "status": "alert",
                    "at": node.collected_at or snapshot.generated_at,
                    "message": f"{node.label}: disk {disk_value:.1f}% >= {server.disk_alert_percent:.1f}%",
                    "severity": "warning",
                }
            )

        for gpu in node.gpus:
            gpu_temp = float(gpu.temperature_c or 0.0)
            if gpu_temp < server.gpu_temp_alert_c:
                continue
            items.append(
                {
                    "id": f"metric-gpu-temp:{node.id}:{gpu.index}",
                    "kind": "metric_threshold",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": "",
                    "run_label": "",
                    "status": "alert",
                    "at": node.collected_at or snapshot.generated_at,
                    "message": f"{node.label}: GPU {gpu.index} temp {gpu_temp:.1f}C >= {server.gpu_temp_alert_c:.1f}C",
                    "severity": "warning",
                }
            )

        for run in node.runs:
            if run.status not in {"failed", "stalled"}:
                continue
            items.append(
                {
                    "id": f"current-run:{node.id}:{run.id}:{run.status}",
                    "kind": "current_run_alert",
                    "node_id": node.id,
                    "node_label": node.label,
                    "run_id": run.id,
                    "run_label": run.label,
                    "status": run.status,
                    "at": run.last_update_at or node.collected_at or snapshot.generated_at,
                    "message": f"{node.label} / {run.label}: {run.status}{f' / {run.error}' if run.error else ''}",
                    "severity": "critical",
                }
            )

    items.sort(
        key=lambda item: (0 if item.get("severity") == "critical" else 1, str(item.get("at", ""))),
        reverse=True,
    )
    return items[:30]
