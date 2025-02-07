"""
The MIT License (MIT)

Copyright (c) 2014 Alexey Vasiliev
Copyright (c) 2025 Pierce Freeman

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

from math import ceil
from typing import Callable

from pydantic import BaseModel

from autopg.constants import (
    DB_TYPE_DESKTOP,
    DB_TYPE_DW,
    DB_TYPE_MIXED,
    DB_TYPE_OLTP,
    DB_TYPE_WEB,
    DEFAULT_DB_VERSION,
    HARD_DRIVE_HDD,
    HARD_DRIVE_SAN,
    HARD_DRIVE_SSD,
    OS_LINUX,
    OS_WINDOWS,
    SIZE_UNIT_GB,
    SIZE_UNIT_MAP,
)


class Configuration(BaseModel):
    db_version: float = DEFAULT_DB_VERSION
    os_type: str = OS_LINUX
    db_type: str = DB_TYPE_WEB
    total_memory: int | None = None
    total_memory_unit: str = SIZE_UNIT_GB
    cpu_num: int | None = None
    connection_num: int | None = None
    hd_type: str = HARD_DRIVE_SSD


class PostgresConfig:
    def __init__(self, config: Configuration):
        self.state = config

    def get_total_memory_in_bytes(self) -> int | None:
        if self.state.total_memory is None:
            return None
        return self.state.total_memory * SIZE_UNIT_MAP[self.state.total_memory_unit]

    def get_total_memory_in_kb(self) -> float | None:
        memory_bytes = self.get_total_memory_in_bytes()
        if memory_bytes is None:
            return None
        return memory_bytes / SIZE_UNIT_MAP["KB"]

    def get_max_connections(self) -> int:
        if self.state.connection_num:
            return self.state.connection_num

        connection_map = {
            DB_TYPE_WEB: 200,
            DB_TYPE_OLTP: 300,
            DB_TYPE_DW: 40,
            DB_TYPE_DESKTOP: 20,
            DB_TYPE_MIXED: 100,
        }
        return connection_map[self.state.db_type]

    def get_huge_pages(self) -> str:
        memory_kb = self.get_total_memory_in_kb()
        if memory_kb is None:
            return "off"
        return "try" if memory_kb >= 33554432 else "off"

    def get_shared_buffers(self) -> int | None:
        memory_kb = self.get_total_memory_in_kb()
        if memory_kb is None:
            return None

        shared_buffers_map: dict[str, Callable[[float], float]] = {
            DB_TYPE_WEB: lambda x: x / 4,
            DB_TYPE_OLTP: lambda x: x / 4,
            DB_TYPE_DW: lambda x: x / 4,
            DB_TYPE_DESKTOP: lambda x: x / 16,
            DB_TYPE_MIXED: lambda x: x / 4,
        }

        value = shared_buffers_map[self.state.db_type](memory_kb)

        if self.state.db_version < 10 and self.state.os_type == OS_WINDOWS:
            win_memory_limit = (512 * SIZE_UNIT_MAP["MB"]) / SIZE_UNIT_MAP["KB"]
            if value > win_memory_limit:
                value = win_memory_limit

        return int(value)

    def get_effective_cache_size(self) -> int | None:
        memory_kb = self.get_total_memory_in_kb()
        if memory_kb is None:
            return None

        cache_map: dict[str, Callable[[float], float]] = {
            DB_TYPE_WEB: lambda x: (x * 3) / 4,
            DB_TYPE_OLTP: lambda x: (x * 3) / 4,
            DB_TYPE_DW: lambda x: (x * 3) / 4,
            DB_TYPE_DESKTOP: lambda x: x / 4,
            DB_TYPE_MIXED: lambda x: (x * 3) / 4,
        }
        return int(cache_map[self.state.db_type](memory_kb))

    def get_maintenance_work_mem(self) -> int | None:
        memory_kb = self.get_total_memory_in_kb()
        if memory_kb is None:
            return None

        maintenance_map: dict[str, Callable[[float], float]] = {
            DB_TYPE_WEB: lambda x: x / 16,
            DB_TYPE_OLTP: lambda x: x / 16,
            DB_TYPE_DW: lambda x: x / 8,
            DB_TYPE_DESKTOP: lambda x: x / 16,
            DB_TYPE_MIXED: lambda x: x / 16,
        }

        value = maintenance_map[self.state.db_type](memory_kb)
        memory_limit = (2 * SIZE_UNIT_MAP["GB"]) / SIZE_UNIT_MAP["KB"]

        if value >= memory_limit:
            if self.state.os_type == OS_WINDOWS:
                # 2048MB (2 GB) will raise error at Windows, so we need remove 1 MB from it
                value = memory_limit - (1 * SIZE_UNIT_MAP["MB"]) / SIZE_UNIT_MAP["KB"]
            else:
                value = memory_limit

        return int(value)

    def get_checkpoint_segments(self) -> dict[str, str | float]:
        min_wal_size_map = {
            DB_TYPE_WEB: 1024 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_OLTP: 2048 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_DW: 4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_DESKTOP: 100 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_MIXED: 1024 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
        }

        max_wal_size_map = {
            DB_TYPE_WEB: 4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_OLTP: 8192 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_DW: 16384 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_DESKTOP: 2048 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
            DB_TYPE_MIXED: 4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"],
        }

        return {
            "min_wal_size": min_wal_size_map[self.state.db_type],
            "max_wal_size": max_wal_size_map[self.state.db_type],
        }

    def get_checkpoint_completion_target(self) -> float:
        return 0.9  # based on https://github.com/postgres/postgres/commit/bbcc4eb2

    def get_wal_buffers(self) -> int | None:
        shared_buffers = self.get_shared_buffers()
        if shared_buffers is None:
            return None

        # Follow auto-tuning guideline for wal_buffers added in 9.1, where it's
        # set to 3% of shared_buffers up to a maximum of 16MB.
        value = (3 * shared_buffers) // 100
        max_wal_buffer = int((16 * SIZE_UNIT_MAP["MB"]) / SIZE_UNIT_MAP["KB"])

        if value > max_wal_buffer:
            value = max_wal_buffer

        # It's nice if wal_buffers is an even 16MB if it's near that number
        wal_buffer_near_value = int((14 * SIZE_UNIT_MAP["MB"]) / SIZE_UNIT_MAP["KB"])
        if wal_buffer_near_value < value < max_wal_buffer:
            value = max_wal_buffer

        # if less than 32 kb, set it to minimum
        if value < 32:
            value = 32

        return int(value)

    def get_default_statistics_target(self) -> int:
        statistics_map = {
            DB_TYPE_WEB: 100,
            DB_TYPE_OLTP: 100,
            DB_TYPE_DW: 500,
            DB_TYPE_DESKTOP: 100,
            DB_TYPE_MIXED: 100,
        }
        return statistics_map[self.state.db_type]

    def get_random_page_cost(self) -> float:
        cost_map = {HARD_DRIVE_HDD: 4.0, HARD_DRIVE_SSD: 1.1, HARD_DRIVE_SAN: 1.1}
        return cost_map[self.state.hd_type]

    def get_effective_io_concurrency(self) -> int | None:
        if self.state.os_type != OS_LINUX:
            return None

        concurrency_map = {HARD_DRIVE_HDD: 2, HARD_DRIVE_SSD: 200, HARD_DRIVE_SAN: 300}
        return concurrency_map[self.state.hd_type]

    def get_parallel_settings(self) -> dict[str, str | int]:
        if not self.state.cpu_num or self.state.cpu_num < 4:
            return {}

        workers_per_gather = ceil(self.state.cpu_num / 2)

        if self.state.db_type != DB_TYPE_DW and workers_per_gather > 4:
            #  no clear evidence, that each new worker will provide big benefit for each new core
            workers_per_gather = 4

        config: dict[str, str | int] = {
            "max_worker_processes": self.state.cpu_num,
            "max_parallel_workers_per_gather": workers_per_gather,
        }

        if self.state.db_version >= 10:
            config["max_parallel_workers"] = self.state.cpu_num

        if self.state.db_version >= 11:
            parallel_maintenance_workers = ceil(self.state.cpu_num / 2)
            if parallel_maintenance_workers > 4:
                parallel_maintenance_workers = 4

            config["max_parallel_maintenance_workers"] = parallel_maintenance_workers

        return config

    def get_work_mem(self) -> int | None:
        memory_kb = self.get_total_memory_in_kb()
        shared_buffers = self.get_shared_buffers()
        if memory_kb is None or shared_buffers is None:
            return None

        max_connections = self.get_max_connections()
        parallel_settings = self.get_parallel_settings()

        # Determine parallel workers
        parallel_workers = 1
        for key, value in parallel_settings.items():
            if key == "max_parallel_workers_per_gather":
                if isinstance(value, int) and value > 0:
                    parallel_workers = value
                break

        # Calculate work_mem
        work_mem = float(memory_kb - shared_buffers) / (max_connections * 3) / parallel_workers

        work_mem_map: dict[str, Callable[[float], float]] = {
            DB_TYPE_WEB: lambda x: x,
            DB_TYPE_OLTP: lambda x: x,
            DB_TYPE_DW: lambda x: x / 2,
            DB_TYPE_DESKTOP: lambda x: x / 6,
            DB_TYPE_MIXED: lambda x: x / 2,
        }

        value = int(work_mem_map[self.state.db_type](work_mem))
        return max(64, value)  # Minimum 64kb

    def get_warning_info_messages(self) -> list[str]:
        memory_bytes = self.get_total_memory_in_bytes()
        if memory_bytes is None:
            return []

        if memory_bytes < 256 * SIZE_UNIT_MAP["MB"]:
            return ["WARNING", "this tool not being optimal", "for low memory systems"]
        if memory_bytes > 100 * SIZE_UNIT_MAP["GB"]:
            return ["WARNING", "this tool not being optimal", "for very high memory systems"]
        return []

    def get_wal_level(self) -> dict[str, str]:
        if self.state.db_type == DB_TYPE_DESKTOP:
            return {
                "wal_level": "minimal",
                "max_wal_senders": "0",
            }
        return {}
