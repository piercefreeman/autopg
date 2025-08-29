import platform
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict

import click
from pydantic_settings import BaseSettings
from rich.console import Console
from rich.table import Table

from autopg.constants import (
    DB_TYPE_WEB,
    HARD_DRIVE_SSD,
    OS_LINUX,
    OS_MAC,
    OS_WINDOWS,
    SIZE_UNIT_MB,
)
from autopg.logic import Configuration, PostgresConfig
from autopg.postgres import (
    CONFIG_TYPES,
    format_postgres_values,
    get_postgres_version,
    read_postgresql_conf,
    write_postgresql_conf,
    write_sql_init_file,
)
from autopg.system_info import DiskType, get_cpu_info, get_disk_type, get_memory_info

console = Console()


class DBType(StrEnum):
    WEB = "web"
    """
    Web Application
    Typically CPU-bound, DB much smaller than RAM, 90% or more simple queries
    """

    OLTP = "oltp"
    """
    Online Transaction Processing
    Typically CPU- or I/O-bound, DB slightly larger than RAM to 1TB, 20-40% small data write queries
    """

    DW = "dw"
    """
    Data Warehouse
    Typically I/O- or RAM-bound, large bulk loads of data, large complex reporting queries
    """

    DESKTOP = "desktop"
    """
    Desktop Application
    Not a dedicated database, general workstation use
    """

    MIXED = "mixed"
    """
    Mixed Type
    Mixed DW and OLTP characteristics, wide mixture of queries
    """


@dataclass
class DBDefinition:
    name: str
    description: str


class EnvOverrides(BaseSettings):
    """
    Users can optionally override our detected system information. These are reasonable
    defaults for most applications where we have no other context.
    """

    DB_TYPE: DBType = DBType.WEB
    TOTAL_MEMORY_MB: int | None = None
    CPU_COUNT: int | None = None
    NUM_CONNECTIONS: int | None = 100
    PRIMARY_DISK_TYPE: DiskType | None = None
    ENABLE_PG_STAT_STATEMENTS: bool = True

    model_config = {"env_file": ".env", "env_prefix": "AUTOPG_"}


def get_os_type() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return OS_MAC
    elif system == "windows":
        return OS_WINDOWS
    return OS_LINUX


def display_config_diff(old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
    """Display the configuration differences in a rich table"""
    table = Table(title="Autopg Configuration")
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


def display_detected_params(config: Configuration) -> None:
    """Display the detected system parameters in a rich table"""
    table = Table(title="Detected System Parameters")
    table.add_column("Parameter")
    table.add_column("Value")

    # Add all configuration parameters
    table.add_row("Database Version", str(config.db_version))
    table.add_row("Operating System", config.os_type)
    table.add_row("Database Type", config.db_type)
    table.add_row("Total Memory (MB)", str(config.total_memory))
    table.add_row("Memory Unit", config.total_memory_unit)
    table.add_row("CPU Count", str(config.cpu_num))
    table.add_row("Connection Count", str(config.connection_num))
    table.add_row("Hard Drive Type", config.hd_type)
    table.add_row(
        "pg_stat_statements", "Enabled" if config.enable_pg_stat_statements else "Disabled"
    )

    console.print(table)
    console.print()


@click.group()
def cli() -> None:
    """AutoPG CLI tool for PostgreSQL configuration and system analysis."""
    pass


@cli.command()
def webapp() -> None:
    """Start the AutoPG diagnostics web application."""
    from autopg.webapp import start_webapp

    start_webapp()


@cli.command()
@click.option(
    "--pg-path", default="/etc/postgresql", help="Path to PostgreSQL configuration directory"
)
def build_config(pg_path: str) -> None:
    """Build a PostgreSQL configuration based on workload and system characteristics."""
    # Load environment overrides
    env = EnvOverrides()

    # Get system information
    memory_info = get_memory_info()
    cpu_info = get_cpu_info()
    disk_type = get_disk_type()
    os_type = get_os_type()
    postgres_version = get_postgres_version()

    # Configure with detected values, allowing env overrides
    config_payload = Configuration(
        db_version=postgres_version,
        os_type=os_type,
        db_type=env.DB_TYPE or DB_TYPE_WEB,
        total_memory=(
            (int(env.TOTAL_MEMORY_MB) if env.TOTAL_MEMORY_MB else None)
            or (int(memory_info.total * 1024) if memory_info.total else None)
        ),
        total_memory_unit=SIZE_UNIT_MB,
        cpu_num=env.CPU_COUNT or cpu_info.count,
        connection_num=env.NUM_CONNECTIONS,
        hd_type=env.PRIMARY_DISK_TYPE or disk_type or HARD_DRIVE_SSD,
        enable_pg_stat_statements=env.ENABLE_PG_STAT_STATEMENTS,
    )

    # Display detected parameters
    display_detected_params(config_payload)

    # Initialize PostgreSQL config calculator
    pg_config = PostgresConfig(config_payload)

    # Calculate recommended settings
    new_config: dict[str, CONFIG_TYPES | None] = {
        "shared_buffers": pg_config.get_shared_buffers(),
        "effective_cache_size": pg_config.get_effective_cache_size(),
        "maintenance_work_mem": pg_config.get_maintenance_work_mem(),
        "work_mem": pg_config.get_work_mem(),
        "huge_pages": pg_config.get_huge_pages(),
        "default_statistics_target": pg_config.get_default_statistics_target(),
        "random_page_cost": pg_config.get_random_page_cost(),
        "checkpoint_completion_target": pg_config.get_checkpoint_completion_target(),
        "max_connections": pg_config.get_max_connections(),
    }

    # Add WAL settings
    new_config = {**new_config, **pg_config.get_checkpoint_segments()}

    # Add parallel settings
    new_config = {**new_config, **pg_config.get_parallel_settings()}

    # Add WAL level settings
    new_config = {**new_config, **pg_config.get_wal_level()}

    # Add pg_stat_statements settings
    new_config = {**new_config, **pg_config.get_pg_stat_statements_config()}

    # Add WAL buffers if available
    wal_buffers = pg_config.get_wal_buffers()
    if wal_buffers is not None:
        new_config["wal_buffers"] = wal_buffers

    # Add IO concurrency if available
    io_concurrency = pg_config.get_effective_io_concurrency()
    if io_concurrency is not None:
        new_config["effective_io_concurrency"] = io_concurrency

    # Add in the docker specific settings
    new_config["listen_addresses"] = "*"
    new_config["dynamic_shared_memory_type"] = "posix"
    new_config["log_timezone"] = "Etc/UTC"
    new_config["datestyle"] = "iso, mdy"
    new_config["timezone"] = "Etc/UTC"

    # Merge configurations, preferring existing values
    existing_config = read_postgresql_conf(pg_path)
    final_config = format_postgres_values({**new_config, **existing_config})

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

        # Write SQL initialization file if pg_stat_statements is enabled
        init_sql = pg_config.get_pg_stat_statements_sql()
        if init_sql.strip():
            success, _ = write_sql_init_file(init_sql, "init_extensions.sql")
            if not success:
                console.print(
                    "\n[yellow]Failed to write SQL initialization file. Run this SQL manually:[/yellow]"
                )
                console.print(f"[yellow]{init_sql}[/yellow]")

    except Exception as e:
        console.print(f"\n[red]Error writing configuration: {str(e)}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    "--output-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=None,
    help="Output directory for CSS files (defaults to autopg/static/)",
)
@click.option(
    "--style",
    type=str,
    default="default",
    help="Pygments style to use (default, github, monokai, etc.)",
)
def generate_css(output_dir: Path | None, style: str) -> None:
    """Generate Pygments CSS for SQL syntax highlighting."""
    try:
        from pygments.formatters import HtmlFormatter
    except ImportError:
        console.print(
            "[red]Error: pygments is not installed. Install it with: pip install pygments[/red]"
        )
        sys.exit(1)

    if output_dir is None:
        # Default to the static directory relative to this file
        output_dir = Path(__file__).parent / "static"

    output_dir.mkdir(parents=True, exist_ok=True)
    css_file = output_dir / "pygments.css"

    console.print(f"Generating Pygments CSS with style '{style}'...")

    # Create HTML formatter with the specified style
    formatter = HtmlFormatter(  # type: ignore[no-untyped-call]
        style=style, cssclass="highlight", noclasses=False
    )

    # Generate CSS
    css_content = formatter.get_style_defs(".highlight")  # type: ignore[no-untyped-call]

    # Write CSS file
    with open(css_file, "w", encoding="utf-8") as f:
        f.write(css_content)

    console.print(f"[green]✓ Generated Pygments CSS: {css_file}[/green]")
    console.print(f"[blue]Style used: {style}[/blue]")
    console.print("[yellow]Don't forget to include this CSS file in your HTML![/yellow]")
