from typing import Any, Dict, List, Optional

from .collector import count_busy_gpus
from .models import AppSnapshot, NodeSnapshot


_EXTERNAL_JOB_SORT_ORDER = {
    "queued": 0,
    "starting": 1,
    "running": 2,
    "unknown": 3,
    "failed": 4,
    "completed": 5,
    "canceled": 6,
}


def build_nodes_summary(nodes: List[NodeSnapshot]) -> Dict[str, Any]:
    runs = [run for node in nodes for run in node.runs]
    gpus = [gpu for node in nodes for gpu in node.gpus]
    external_items = [item for node in nodes for item in node.external_queue]
    cpu_values = [float(node.metrics.get("cpu_usage_percent", 0.0)) for node in nodes if node.metrics]
    memory_percent_values = [float(node.metrics.get("memory_used_percent", 0.0)) for node in nodes if node.metrics]
    disk_percent_values = [float(node.metrics.get("disk_used_percent", 0.0)) for node in nodes if node.metrics]
    memory_used_values = [float(node.metrics.get("memory_used_mb", 0.0)) for node in nodes if node.metrics]
    return {
        "nodes_total": len(nodes),
        "nodes_online": sum(1 for node in nodes if node.status == "online"),
        "nodes_degraded": sum(1 for node in nodes if node.status == "degraded"),
        "nodes_offline": sum(1 for node in nodes if node.status == "offline"),
        "runs_total": len(runs),
        "runs_running": sum(1 for run in runs if run.status == "running"),
        "runs_alerting": sum(1 for run in runs if run.status in {"failed", "stalled"}),
        "gpus_total": len(gpus),
        "gpus_busy": count_busy_gpus(gpus),
        "external_queue_total": len(external_items),
        "external_queue_queued": sum(1 for item in external_items if item.status == "queued"),
        "external_queue_starting": sum(1 for item in external_items if item.status == "starting"),
        "external_queue_running": sum(1 for item in external_items if item.status == "running"),
        "cpu_usage_avg": float(sum(cpu_values) / len(cpu_values)) if cpu_values else 0.0,
        "memory_used_percent_avg": float(sum(memory_percent_values) / len(memory_percent_values)) if memory_percent_values else 0.0,
        "disk_used_percent_avg": float(sum(disk_percent_values) / len(disk_percent_values)) if disk_percent_values else 0.0,
        "memory_used_mb_total": float(sum(memory_used_values)) if memory_used_values else 0.0,
    }


def build_external_job_items(snapshot: AppSnapshot, node_id: Optional[str] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for node in snapshot.nodes:
        if node_id is not None and node.id != node_id:
            continue
        for external_job in node.external_queue:
            payload = external_job.to_dict()
            payload["node_id"] = node.id
            payload["node_label"] = node.label
            payload["can_cancel"] = False
            items.append(payload)
    return sorted(
        items,
        key=lambda item: (
            _EXTERNAL_JOB_SORT_ORDER.get(str(item.get("status", "unknown")), 9),
            str(item.get("submitted_at", "")),
            str(item.get("id", "")),
        ),
    )


def build_external_queue_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    gpu_requested_active = 0
    for item in items:
        if item.get("status") not in {"queued", "starting", "running"}:
            continue
        try:
            if item.get("gpu_count") is not None:
                gpu_requested_active += int(item.get("gpu_count") or 0)
        except (TypeError, ValueError):
            continue

    return {
        "jobs_total": len(items),
        "jobs_queued": sum(1 for item in items if item.get("status") == "queued"),
        "jobs_starting": sum(1 for item in items if item.get("status") == "starting"),
        "jobs_running": sum(1 for item in items if item.get("status") == "running"),
        "jobs_failed": sum(1 for item in items if item.get("status") == "failed"),
        "jobs_completed": sum(1 for item in items if item.get("status") == "completed"),
        "jobs_canceled": sum(1 for item in items if item.get("status") == "canceled"),
        "gpu_requested_active": gpu_requested_active,
    }
