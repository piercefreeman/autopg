from dataclasses import dataclass
from enum import StrEnum

import psutil
from rich.console import Console

console = Console()


@dataclass
class MemoryInfo:
    total: float | None
    available: float


@dataclass
class CpuInfo:
    count: int | None
    current_freq: float


class DiskType(StrEnum):
    SSD = "SSD"
    HDD = "HDD"


def get_memory_info() -> MemoryInfo:
    """
    Get the total and available memory in GB
    """
    vm = psutil.virtual_memory()
    total_gb = vm.total / (1024**3)
    available_gb = vm.available / (1024**3)
    return MemoryInfo(total=total_gb, available=available_gb)


def get_cpu_info() -> CpuInfo:
    """
    Get CPU count and current frequency
    """
    cpu_count = psutil.cpu_count(logical=True)
    # Get the average frequency across all CPUs
    freq = psutil.cpu_freq()
    current_freq = freq.current if freq else 0.0
    return CpuInfo(count=cpu_count, current_freq=current_freq)


def get_disk_type() -> DiskType | None:
    """
    Attempt to determine if the primary disk is SSD or HDD
    """
    try:
        # On Linux, we can check rotational flag
        import os

        # Check the first disk device
        for device in psutil.disk_partitions():
            if device.device.startswith("/dev/"):
                # Get the base device (strip partition number)
                base_device = "".join(filter(str.isalpha, device.device))
                rotational_path = f"/sys/block/{base_device}/queue/rotational"

                if os.path.exists(rotational_path):
                    with open(rotational_path, "r") as f:
                        rotational = int(f.read().strip())
                        return DiskType.HDD if rotational == 1 else DiskType.SSD
        return None
    except Exception:
        return None
