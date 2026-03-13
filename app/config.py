from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8420
    shared_token: str = ""
    poll_seconds: int = 10
    sqlite_path: str = "data/train-watch.sqlite3"
    retention_days: int = 7
    persist_passwords: bool = False


@dataclass
class RunConfig:
    id: str
    label: str
    log_path: Optional[str] = None
    log_glob: Optional[str] = None
    workdir: str = ""
    process_match: str = ""
    parser: str = "auto"
    stall_after_seconds: int = 900
    completion_regex: str = r"(Training complete|Finished training|saving final checkpoint)"
    error_regex: str = r"(Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)"
    mock_state: str = "auto"
    mock_gpu_index: Optional[int] = None


@dataclass
class NodeConfig:
    id: str
    label: str
    host: str
    port: int
    user: str
    key_path: str
    password: str = ""
    runs: List[RunConfig] = field(default_factory=list)
    queue_probe_command: str = ""
    transport: str = "ssh"
    mock_profile: str = "demo"
    mock_gpu_count: int = 4
    needs_password: bool = False


@dataclass
class AppConfig:
    server: ServerConfig
    nodes: List[NodeConfig]
    config_path: Path


def run_to_dict(run: RunConfig) -> Dict[str, Any]:
    return {
        "id": run.id,
        "label": run.label,
        "log_path": run.log_path,
        "log_glob": run.log_glob,
        "workdir": run.workdir,
        "process_match": run.process_match,
        "parser": run.parser,
        "stall_after_seconds": run.stall_after_seconds,
        "completion_regex": run.completion_regex,
        "error_regex": run.error_regex,
        "mock_state": run.mock_state,
        "mock_gpu_index": run.mock_gpu_index,
    }


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def node_to_dict(node: NodeConfig, include_password: bool = True) -> Dict[str, Any]:
    password = node.password if include_password else ""
    needs_password = bool(node.needs_password or (node.password and not include_password))
    return {
        "id": node.id,
        "label": node.label,
        "host": node.host,
        "port": node.port,
        "user": node.user,
        "key_path": node.key_path,
        "password": password,
        "queue_probe_command": node.queue_probe_command,
        "transport": node.transport,
        "mock_profile": node.mock_profile,
        "mock_gpu_count": node.mock_gpu_count,
        "needs_password": needs_password,
        "runs": [run_to_dict(run) for run in node.runs],
    }


def run_from_persisted_dict(item: Dict[str, Any]) -> RunConfig:
    run_id = str(item.get("id", "")).strip()
    if not run_id:
        raise ValueError("persisted run id is required")
    return RunConfig(
        id=run_id,
        label=str(item.get("label", run_id)).strip() or run_id,
        log_path=str(item.get("log_path")) if item.get("log_path") else None,
        log_glob=str(item.get("log_glob")) if item.get("log_glob") else None,
        workdir=str(item.get("workdir", "")),
        process_match=str(item.get("process_match", "")),
        parser=str(item.get("parser", "auto") or "auto"),
        stall_after_seconds=max(30, int(item.get("stall_after_seconds", 900))),
        completion_regex=str(item.get("completion_regex", r"(Training complete|Finished training|saving final checkpoint)")),
        error_regex=str(item.get("error_regex", r"(Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)")),
        mock_state=str(item.get("mock_state", "auto") or "auto"),
        mock_gpu_index=int(item["mock_gpu_index"]) if item.get("mock_gpu_index") is not None else None,
    )


def node_from_persisted_dict(item: Dict[str, Any]) -> NodeConfig:
    node_id = str(item.get("id", "")).strip()
    if not node_id:
        raise ValueError("persisted node id is required")
    transport = str(item.get("transport", "ssh") or "ssh").strip().lower()
    runs = [run_from_persisted_dict(run_item or {}) for run_item in item.get("runs", [])]
    return NodeConfig(
        id=node_id,
        label=str(item.get("label", node_id)).strip() or node_id,
        host=str(item.get("host", "")).strip(),
        port=int(item.get("port", 22)),
        user=str(item.get("user", "")).strip(),
        key_path=str(Path(str(item.get("key_path", "") or "")).expanduser()) if item.get("key_path") else "",
        password=str(item.get("password", "") or ""),
        runs=runs,
        queue_probe_command=str(item.get("queue_probe_command", "") or "").strip(),
        transport=transport or "ssh",
        mock_profile=str(item.get("mock_profile", "demo") or "demo"),
        mock_gpu_count=max(1, int(item.get("mock_gpu_count", 4))),
        needs_password=_parse_bool(item.get("needs_password", False)),
    )


def _resolve_path(config_dir: Path, raw_value: str) -> str:
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    return str((config_dir / candidate).resolve())


def _load_run(item: Dict[str, Any], allow_missing_log_source: bool = False) -> RunConfig:
    run_id = str(item.get("id", "")).strip()
    if not run_id:
        raise ValueError("runs[].id is required")
    label = str(item.get("label", run_id)).strip() or run_id
    log_path = item.get("log_path")
    log_glob = item.get("log_glob")
    if not allow_missing_log_source and not log_path and not log_glob:
        raise ValueError("runs[].log_path or runs[].log_glob is required")
    return RunConfig(
        id=run_id,
        label=label,
        log_path=str(log_path) if log_path else None,
        log_glob=str(log_glob) if log_glob else None,
        workdir=str(item.get("workdir", "")),
        process_match=str(item.get("process_match", "")),
        parser=str(item.get("parser", "auto") or "auto"),
        stall_after_seconds=int(item.get("stall_after_seconds", 900)),
        completion_regex=str(
            item.get(
                "completion_regex",
                r"(Training complete|Finished training|saving final checkpoint)",
            )
        ),
        error_regex=str(
            item.get(
                "error_regex",
                r"(Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)",
            )
        ),
        mock_state=str(item.get("mock_state", "auto") or "auto"),
        mock_gpu_index=int(item["mock_gpu_index"]) if item.get("mock_gpu_index") is not None else None,
    )


def _load_node(config_dir: Path, item: Dict[str, Any]) -> NodeConfig:
    node_id = str(item.get("id", "")).strip()
    if not node_id:
        raise ValueError("nodes[].id is required")

    transport = str(item.get("transport", "ssh") or "ssh").strip().lower()
    if transport not in {"ssh", "mock"}:
        raise ValueError("nodes[].transport must be 'ssh' or 'mock'")

    allow_missing_log_source = transport == "mock"
    runs = [_load_run(run_item or {}, allow_missing_log_source=allow_missing_log_source) for run_item in item.get("runs", [])]

    host = str(item.get("host", "")).strip() or ("mock.local" if transport == "mock" else "")
    if not host:
        raise ValueError("nodes[].host is required")

    user = str(item.get("user", "")).strip() or ("mock" if transport == "mock" else "")
    key_path = str(item.get("key_path", "")).strip()
    password = str(item.get("password", "") or "")
    resolved_key_path = _resolve_path(config_dir, key_path) if key_path else ""

    if transport == "ssh" and not user:
        raise ValueError("nodes[].user is required for ssh transport")
    if transport == "ssh" and not resolved_key_path and not password:
        raise ValueError("nodes[].key_path or nodes[].password is required for ssh transport")

    return NodeConfig(
        id=node_id,
        label=str(item.get("label", node_id)).strip() or node_id,
        host=host,
        port=int(item.get("port", 22)),
        user=user,
        key_path=resolved_key_path,
        password=password,
        runs=runs,
        queue_probe_command=str(item.get("queue_probe_command", "") or "").strip(),
        transport=transport,
        mock_profile=str(item.get("mock_profile", "demo") or "demo"),
        mock_gpu_count=max(1, int(item.get("mock_gpu_count", 4))),
        needs_password=False,
    )


def load_config(path_value: str) -> AppConfig:
    config_path = Path(path_value).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError("Config file not found: %s" % config_path)

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    server_raw = raw.get("server") or {}
    server = ServerConfig(
        host=str(server_raw.get("host", "127.0.0.1")),
        port=int(server_raw.get("port", 8420)),
        shared_token=str(server_raw.get("shared_token", "")),
        poll_seconds=max(3, int(server_raw.get("poll_seconds", 10))),
        sqlite_path=_resolve_path(
            config_path.parent,
            str(server_raw.get("sqlite_path", "data/train-watch.sqlite3")),
        ),
        retention_days=max(1, int(server_raw.get("retention_days", 7))),
        persist_passwords=_parse_bool(server_raw.get("persist_passwords", False)),
    )

    nodes_raw = raw.get("nodes") or []
    nodes = [_load_node(config_path.parent, item or {}) for item in nodes_raw]
    return AppConfig(server=server, nodes=nodes, config_path=config_path)
