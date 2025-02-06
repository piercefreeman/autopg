import platform
import sys
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Dict

import click
from pydantic import ConfigDict
from pydantic_settings import BaseSettings
from rich.console import Console
from rich.table import Table

from autopg.constants import (
    DB_TYPE_WEB,
    HARD_DRIVE_SSD,
    OS_LINUX,
    OS_MAC,
    OS_WINDOWS,
    SIZE_UNIT_GB,
)
from autopg.logic import PostgresConfig
from autopg.postgres import read_postgresql_conf, write_postgresql_conf
from autopg.system_info import get_cpu_info, get_disk_type, get_memory_info

console = Console()


class DBType(StrEnum):
    WEB = "web"
    OLTP = "oltp"
    DW = "dw"
    DESKTOP = "desktop"
    MIXED = "mixed"


@dataclass
class DBDefinition:
    name: str
    description: str


DB_TYPES = {
    DBType.WEB: DBDefinition(
        name="Web Application",
        description="Typically CPU-bound, DB much smaller than RAM, 90% or more simple queries",
    ),
    DBType.OLTP: DBDefinition(
        name="Online Transaction Processing",
        description="Typically CPU- or I/O-bound, DB slightly larger than RAM to 1TB, 20-40% small data write queries",
    ),
    DBType.DW: DBDefinition(
        name="Data Warehouse",
        description="Typically I/O- or RAM-bound, large bulk loads of data, large complex reporting queries",
    ),
    DBType.DESKTOP: DBDefinition(
        name="Desktop Application",
        description="Not a dedicated database, general workstation use",
    ),
    DBType.MIXED: DBDefinition(
        name="Mixed Type",
        description="Mixed DW and OLTP characteristics, wide mixture of queries",
    ),
}


class EnvOverrides(BaseSettings):
    """
    Users can optionally override our detected system information. These are reasonable
    defaults for most applications where we have no other context.
    """

    DB_TYPE: str = "web"
    TOTAL_MEMORY: float | None = None
    CPU_COUNT: int | None = None
    NUM_CONNECTIONS: int | None = 100
    PRIMARY_DISK_TYPE: str | None = None

    model_config = ConfigDict(env_file=".env", env_prefix="AUTOPG_")


def get_os_type() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return OS_MAC
    elif system == "windows":
        return OS_WINDOWS
    return OS_LINUX


def display_config_diff(old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
    """Display the configuration differences in a rich table"""
    table = Table(title="PostgreSQL Configuration Changes")
    table.add_column("Parameter")
    table.add_column("Old Value")
    table.add_column("New Value")
    table.add_column("Source")

    all_keys = sorted(set(old_config.keys()) | set(new_config.keys()))
    for key in all_keys:
        old_val = old_config.get(key, "")
        new_val = new_config.get(key, "")
        source = "Existing" if key in old_config else "AutoPG"

        if old_val != new_val:
            table.add_row(key, str(old_val), str(new_val), source)

    console.print(table)


@click.group()
def cli() -> None:
    """AutoPG CLI tool for PostgreSQL configuration and system analysis."""
    pass


@cli.command()
@click.option(
    "--pg-path", default="/etc/postgresql", help="Path to PostgreSQL configuration directory"
)
def build_config(pg_path: str) -> None:
    """Build a PostgreSQL configuration based on workload and system characteristics."""
    # Load environment overrides
    env = EnvOverrides()

    # Initialize PostgreSQL config calculator
    pg_config = PostgresConfig()

    # Get system information
    total_mem, available_mem = get_memory_info()
    cpu_count, cpu_freq = get_cpu_info()
    disk_type = get_disk_type()
    os_type = get_os_type()

    # Configure with detected values, allowing env overrides
    config_payload = {
        "db_version": pg_config.state["db_version"],  # Use default version
        "os_type": os_type,
        "db_type": env.DB_TYPE or DB_TYPE_WEB,
        "total_memory": env.TOTAL_MEMORY or total_mem,
        "total_memory_unit": SIZE_UNIT_GB,
        "cpu_num": env.CPU_COUNT or cpu_count,
        "connection_num": env.NUM_CONNECTIONS,
        "hd_type": env.PRIMARY_DISK_TYPE or disk_type or HARD_DRIVE_SSD,
    }

    pg_config.submit_configuration(config_payload)

    # Calculate recommended settings
    new_config = {
        "shared_buffers": pg_config.get_shared_buffers(),
        "effective_cache_size": pg_config.get_effective_cache_size(),
        "maintenance_work_mem": pg_config.get_maintenance_work_mem(),
        "work_mem": pg_config.get_work_mem(),
        "huge_pages": pg_config.get_huge_pages(),
        "default_statistics_target": pg_config.get_default_statistics_target(),
        "random_page_cost": pg_config.get_random_page_cost(),
        "checkpoint_completion_target": pg_config.get_checkpoint_completion_target(),
    }

    # Add WAL settings
    for setting in pg_config.get_checkpoint_segments():
        new_config[setting["key"]] = setting["value"]

    # Add WAL buffers if available
    wal_buffers = pg_config.get_wal_buffers()
    if wal_buffers is not None:
        new_config["wal_buffers"] = wal_buffers

    # Add parallel settings
    for setting in pg_config.get_parallel_settings():
        new_config[setting["key"]] = setting["value"]

    # Add WAL level settings
    for setting in pg_config.get_wal_level():
        new_config[setting["key"]] = setting["value"]

    # Add IO concurrency if available
    io_concurrency = pg_config.get_effective_io_concurrency()
    if io_concurrency is not None:
        new_config["effective_io_concurrency"] = io_concurrency

    # Read existing configuration
    existing_config = read_postgresql_conf(pg_path)

    # Merge configurations, preferring existing values
    final_config = {**new_config, **existing_config}

    # Display the differences
    display_config_diff(existing_config, final_config)

    # Check for any warnings
    warnings = pg_config.get_warning_info_messages()
    if warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for warning in warnings:
            console.print(f"[yellow]- {warning}[/yellow]")

    # Write the new configuration
    try:
        write_postgresql_conf(final_config, pg_path)
        console.print("\n[green]Successfully wrote new PostgreSQL configuration![/green]")
    except Exception as e:
        console.print(f"\n[red]Error writing configuration: {str(e)}[/red]")
        sys.exit(1)
