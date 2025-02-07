"""
Tests for this core logic are almost fully written from the original Javascript:
https://github.com/le0pard/pgtune/blob/9ae57d0a97ba6c597390d43b15cd428311327939/src/features/configuration/__tests__/configurationSlice.test.js

"""

import pytest

from autopg.constants import (
    DB_TYPE_DESKTOP,
    DB_TYPE_DW,
    DB_TYPE_MIXED,
    DB_TYPE_OLTP,
    DB_TYPE_WEB,
    HARD_DRIVE_HDD,
    HARD_DRIVE_SAN,
    HARD_DRIVE_SSD,
    OS_LINUX,
    OS_WINDOWS,
)
from autopg.logic import Configuration, PostgresConfig

#
# New tests
#


@pytest.mark.parametrize(
    "kb_value,expected",
    [
        # GB cases
        (1048576, "1GB"),  # 1 GB
        (2097152, "2GB"),  # 2 GB
        (5242880, "5GB"),  # 5 GB
        # MB cases
        (1024, "1MB"),  # 1 MB
        (2048, "2MB"),  # 2 MB
        (51200, "50MB"),  # 50 MB
        # KB cases
        (1, "1kB"),  # 1 KB
        (512, "512kB"),  # 512 KB
        (1000, "1000kB"),  # Not quite 1 MB, should stay as KB
        # Edge cases
        (0, "0kB"),
    ],
)
def test_format_kb_value(kb_value: int, expected: str):
    """
    Test the format_kb_value function with various inputs to ensure it correctly
    formats values in kilobytes to the most appropriate unit (GB, MB, or kB).
    """
    from autopg.logic import format_kb_value

    assert format_kb_value(kb_value) == expected


#
# Original tests
#


def test_is_configured_nothing_set():
    config = PostgresConfig(Configuration())
    assert config.state.total_memory is None


def test_is_configured_with_memory():
    config = PostgresConfig(
        Configuration(
            total_memory=100,
            db_version="14",
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.state.total_memory == 100


@pytest.mark.parametrize(
    "db_type,expected",
    [
        (DB_TYPE_WEB, 200),
        (DB_TYPE_OLTP, 300),
        (DB_TYPE_DW, 40),
        (DB_TYPE_DESKTOP, 20),
        (DB_TYPE_MIXED, 100),
    ],
)
def test_max_connections(db_type, expected):
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version="14",
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_max_connections() == expected


@pytest.mark.parametrize(
    "db_type,expected",
    [
        (DB_TYPE_WEB, 100),
        (DB_TYPE_OLTP, 100),
        (DB_TYPE_DW, 500),
        (DB_TYPE_DESKTOP, 100),
        (DB_TYPE_MIXED, 100),
    ],
)
def test_default_statistics_target(db_type, expected):
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version="14",
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_default_statistics_target() == expected


@pytest.mark.parametrize(
    "hd_type,expected", [(HARD_DRIVE_HDD, 4.0), (HARD_DRIVE_SSD, 1.1), (HARD_DRIVE_SAN, 1.1)]
)
def test_random_page_cost(hd_type, expected):
    config = PostgresConfig(
        Configuration(
            hd_type=hd_type,
            db_version="14",
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_random_page_cost() == expected


@pytest.mark.parametrize(
    "os_type,hd_type,expected",
    [
        (OS_LINUX, HARD_DRIVE_HDD, 2),
        (OS_LINUX, HARD_DRIVE_SSD, 200),
        (OS_LINUX, HARD_DRIVE_SAN, 300),
        (OS_WINDOWS, HARD_DRIVE_SSD, None),
    ],
)
def test_effective_io_concurrency(os_type, hd_type, expected):
    config = PostgresConfig(
        Configuration(
            os_type=os_type,
            hd_type=hd_type,
            db_version="14",
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_effective_io_concurrency() == expected


def test_parallel_settings_less_than_2_cpu():
    config = PostgresConfig(
        Configuration(
            cpu_num=1,
            db_version="14",
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == []


def test_parallel_settings_postgresql_13():
    config = PostgresConfig(
        Configuration(
            db_version="13",
            cpu_num=12,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == [
        {"key": "max_worker_processes", "value": 12},
        {"key": "max_parallel_workers_per_gather", "value": 4},
        {"key": "max_parallel_workers", "value": 12},
        {"key": "max_parallel_maintenance_workers", "value": 4},
    ]


def test_parallel_settings_postgresql_10():
    config = PostgresConfig(
        Configuration(
            db_version="10",
            cpu_num=12,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == [
        {"key": "max_worker_processes", "value": 12},
        {"key": "max_parallel_workers_per_gather", "value": 4},
        {"key": "max_parallel_workers", "value": 12},
    ]


def test_parallel_settings_postgresql_10_with_31_cpu():
    config = PostgresConfig(
        Configuration(
            db_version="10",
            cpu_num=31,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == [
        {"key": "max_worker_processes", "value": 31},
        {"key": "max_parallel_workers_per_gather", "value": 4},
        {"key": "max_parallel_workers", "value": 31},
    ]


def test_parallel_settings_postgresql_12_with_31_cpu_and_dwh():
    config = PostgresConfig(
        Configuration(
            db_version="12",
            cpu_num=31,
            db_type=DB_TYPE_DW,
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_parallel_settings() == [
        {"key": "max_worker_processes", "value": 31},
        {"key": "max_parallel_workers_per_gather", "value": 16},
        {"key": "max_parallel_workers", "value": 31},
        {"key": "max_parallel_maintenance_workers", "value": 4},
    ]


@pytest.mark.parametrize(
    "db_type,expected",
    [
        (
            DB_TYPE_DESKTOP,
            [{"key": "wal_level", "value": "minimal"}, {"key": "max_wal_senders", "value": "0"}],
        ),
        (DB_TYPE_WEB, []),
    ],
)
def test_wal_level(db_type, expected):
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version="14",
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_wal_level() == expected
