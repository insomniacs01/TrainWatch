import re
from pathlib import Path
from typing import Callable, List, Optional
from uuid import uuid4

from fastapi import HTTPException
from pydantic import BaseModel, Field

from .config import NodeConfig, RunConfig
from .job_queue import summarize_command, utc_now_iso
from .models import QueueJob
from .ssh_support import ssh_config_alias_exists


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
    queue_probe_command: Optional[str] = ""
    runs: List[RunConnectionInput] = Field(default_factory=list)


class QueueJobInput(BaseModel):
    node_id: str
    owner: str = "Anonymous"
    label: Optional[str] = None
    command: str
    gpu_count: int = Field(default=1, ge=1)
    workdir: str = ""
    parser: str = "auto"


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "node"


def build_node_from_input(payload: SSHConnectionInput) -> NodeConfig:
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
        queue_probe_command=(payload.queue_probe_command or "").strip(),
        transport="ssh",
    )


def build_queue_job_from_input(
    payload: QueueJobInput,
    find_node: Callable[[str], Optional[NodeConfig]],
) -> QueueJob:
    node = find_node(payload.node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="Target connection not found")
    command = payload.command.strip()
    if not command:
        raise HTTPException(status_code=400, detail="command is required")
    now_value = utc_now_iso()
    label = (payload.label or summarize_command(command)).strip() or summarize_command(command)
    return QueueJob(
        id=f"job-{uuid4().hex[:10]}",
        node_id=node.id,
        node_label=node.label,
        owner=(payload.owner or "Anonymous").strip() or "Anonymous",
        label=label,
        command=command,
        gpu_count=max(1, int(payload.gpu_count)),
        created_at=now_value,
        updated_at=now_value,
        workdir=(payload.workdir or "").strip(),
        parser=(payload.parser or "auto").strip() or "auto",
    )
