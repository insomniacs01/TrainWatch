from typing import List

from .models import GPUInfo


def is_gpu_busy(gpu: GPUInfo) -> bool:
    utilization = float(gpu.utilization_gpu or 0.0)
    memory_used = float(gpu.memory_used_mb or 0.0)
    return utilization >= 10.0 or memory_used >= 1024.0


def count_busy_gpus(gpus: List[GPUInfo]) -> int:
    return sum(1 for gpu in gpus if gpu.is_busy)
