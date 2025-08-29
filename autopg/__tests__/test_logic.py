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


def test_pg_stat_statements_enabled_by_default() -> None:
    config = PostgresConfig(Configuration())
    pg_stat_config = config.get_pg_stat_statements_config()

    assert pg_stat_config == {
        "shared_preload_libraries": "pg_stat_statements",
        "pg_stat_statements.track": "all",
        "pg_stat_statements.max": 10000,
    }


def test_pg_stat_statements_disabled() -> None:
    config = PostgresConfig(Configuration(enable_pg_stat_statements=False))
    pg_stat_config = config.get_pg_stat_statements_config()

    assert pg_stat_config == {}


def test_pg_stat_statements_enabled_explicitly() -> None:
    config = PostgresConfig(Configuration(enable_pg_stat_statements=True))
    pg_stat_config = config.get_pg_stat_statements_config()

    assert pg_stat_config == {
        "shared_preload_libraries": "pg_stat_statements",
        "pg_stat_statements.track": "all",
        "pg_stat_statements.max": 10000,
    }


def test_is_configured_nothing_set() -> None:
    config = PostgresConfig(Configuration())
    assert config.state.total_memory is None


def test_is_configured_with_memory() -> None:
    config = PostgresConfig(
        Configuration(
            total_memory=100,
            db_version=14.0,
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
def test_max_connections(db_type: str, expected: int) -> None:
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version=14.0,
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
def test_default_statistics_target(db_type: str, expected: int) -> None:
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version=14.0,
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_default_statistics_target() == expected


@pytest.mark.parametrize(
    "hd_type,expected",
    [(HARD_DRIVE_HDD, 4.0), (HARD_DRIVE_SSD, 1.1), (HARD_DRIVE_SAN, 1.1)],
)
def test_random_page_cost(hd_type: str, expected: float) -> None:
    config = PostgresConfig(
        Configuration(
            hd_type=hd_type,
            db_version=14.0,
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
def test_effective_io_concurrency(os_type: str, hd_type: str, expected: int | None) -> None:
    config = PostgresConfig(
        Configuration(
            os_type=os_type,
            hd_type=hd_type,
            db_version=14.0,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_effective_io_concurrency() == expected


def test_parallel_settings_less_than_2_cpu() -> None:
    config = PostgresConfig(
        Configuration(
            cpu_num=1,
            db_version=14.0,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == {}


def test_parallel_settings_postgresql_13() -> None:
    config = PostgresConfig(
        Configuration(
            db_version=13.0,
            cpu_num=12,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == {
        "max_worker_processes": 12,
        "max_parallel_workers_per_gather": 4,
        "max_parallel_workers": 12,
        "max_parallel_maintenance_workers": 4,
    }


def test_parallel_settings_postgresql_10() -> None:
    config = PostgresConfig(
        Configuration(
            db_version=10.0,
            cpu_num=12,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == {
        "max_worker_processes": 12,
        "max_parallel_workers_per_gather": 4,
        "max_parallel_workers": 12,
    }


def test_parallel_settings_postgresql_10_with_31_cpu() -> None:
    config = PostgresConfig(
        Configuration(
            db_version=10.0,
            cpu_num=31,
            os_type=OS_LINUX,
            db_type=DB_TYPE_WEB,
            total_memory_unit="GB",
        )
    )
    assert config.get_parallel_settings() == {
        "max_worker_processes": 31,
        "max_parallel_workers_per_gather": 4,
        "max_parallel_workers": 31,
    }


def test_parallel_settings_postgresql_12_with_31_cpu_and_dwh() -> None:
    config = PostgresConfig(
        Configuration(
            db_version=12.0,
            cpu_num=31,
            db_type=DB_TYPE_DW,
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_parallel_settings() == {
        "max_worker_processes": 31,
        "max_parallel_workers_per_gather": 16,
        "max_parallel_workers": 31,
        "max_parallel_maintenance_workers": 4,
    }


@pytest.mark.parametrize(
    "db_type,expected",
    [
        (
            DB_TYPE_DESKTOP,
            {"wal_level": "minimal", "max_wal_senders": "0"},
        ),
        (DB_TYPE_WEB, {}),
    ],
)
def test_wal_level(db_type: str, expected: list[dict[str, str]]) -> None:
    config = PostgresConfig(
        Configuration(
            db_type=db_type,
            db_version=14.0,
            os_type=OS_LINUX,
            total_memory_unit="GB",
            hd_type=HARD_DRIVE_SSD,
        )
    )
    assert config.get_wal_level() == expected
