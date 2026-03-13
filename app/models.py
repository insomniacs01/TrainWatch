from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GPUProcess:
    pid: Optional[int]
    process_name: str
    gpu_uuid: str
    gpu_index: Optional[int]
    used_gpu_memory_mb: Optional[float]
    command: str = ""
    elapsed_seconds: Optional[int] = None
    cwd: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GPUInfo:
    index: int
    uuid: str
    name: str
    utilization_gpu: Optional[float]
    memory_used_mb: Optional[float]
    memory_total_mb: Optional[float]
    temperature_c: Optional[float]
    power_draw_w: Optional[float]
    power_limit_w: Optional[float]
    processes: List[GPUProcess] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RunSnapshot:
    id: str
    label: str
    parser: str
    status: str
    error: str
    log_path: str
    log_exists: bool
    log_age_seconds: Optional[int]
    last_update_at: str
    last_log_line: str
    epoch: Optional[int] = None
    step: Optional[int] = None
    step_total: Optional[int] = None
    loss: Optional[float] = None
    eval_loss: Optional[float] = None
    lr: Optional[float] = None
    grad_norm: Optional[float] = None
    tokens_per_sec: Optional[float] = None
    samples_per_sec: Optional[float] = None
    eta: str = ""
    eta_seconds: Optional[int] = None
    task_name: str = ""
    task_command: str = ""
    task_pid: Optional[int] = None
    started_at: str = ""
    elapsed_seconds: Optional[int] = None
    remaining_seconds: Optional[int] = None
    estimated_end_at: str = ""
    gpu_indices: List[int] = field(default_factory=list)
    gpu_memory_used_mb: Optional[float] = None
    progress_percent: Optional[float] = None
    completion_matched: bool = False
    error_matched: bool = False
    matched_processes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExternalQueueItem:
    id: str
    owner: str
    label: str
    status: str
    source: str
    raw_status: str = ""
    submitted_at: str = ""
    gpu_count: Optional[int] = None
    command: str = ""
    workdir: str = ""
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NodeSnapshot:
    id: str
    label: str
    host: str
    hostname: str
    status: str
    error: str
    collected_at: str
    loadavg: List[float] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    gpus: List[GPUInfo] = field(default_factory=list)
    gpu_processes: List[GPUProcess] = field(default_factory=list)
    runs: List[RunSnapshot] = field(default_factory=list)
    external_queue: List[ExternalQueueItem] = field(default_factory=list)
    external_queue_source: str = ""
    external_queue_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AlertEvent:
    kind: str
    node_id: str
    node_label: str
    run_id: str
    run_label: str
    status: str
    previous_status: str
    at: str
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AppSnapshot:
    generated_at: str
    summary: Dict[str, Any]
    nodes: List[NodeSnapshot] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QueueJob:
    id: str
    node_id: str
    node_label: str
    owner: str
    label: str
    command: str
    gpu_count: int
    created_at: str
    updated_at: str
    workdir: str = ""
    parser: str = "auto"
    status: str = "queued"
    run_status: str = ""
    started_at: str = ""
    finished_at: str = ""
    allocated_gpu_indices: List[int] = field(default_factory=list)
    log_path: str = ""
    script_path: str = ""
    process_match: str = ""
    run_id: str = ""
    remote_pid: Optional[int] = None
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
