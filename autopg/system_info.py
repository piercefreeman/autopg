from typing import Optional, Tuple

import psutil
from rich.console import Console

console = Console()


def get_memory_info() -> Tuple[float, float]:
    """
    Get the total and available memory in GB
    """
    vm = psutil.virtual_memory()
    total_gb = vm.total / (1024**3)
    available_gb = vm.available / (1024**3)
    return total_gb, available_gb


def get_cpu_info() -> Tuple[int, float]:
    """
    Get CPU count and current frequency
    """
    cpu_count = psutil.cpu_count(logical=True)
    # Get the average frequency across all CPUs
    freq = psutil.cpu_freq()
    current_freq = freq.current if freq else 0.0
    return cpu_count, current_freq


def get_disk_type() -> Optional[str]:
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
                        return "HDD" if rotational == 1 else "SSD"
        return None
    except Exception:
        return None
