import json
import platform
import sys
from typing import Any, Dict

import click
import questionary
from rich.console import Console

from .system_info import get_cpu_info, get_disk_type, get_memory_info

console = Console()

DB_TYPES = {
    "web": {
        "name": "Web Application",
        "description": "Typically CPU-bound, DB much smaller than RAM, 90% or more simple queries",
    },
    "oltp": {
        "name": "Online Transaction Processing",
        "description": "Typically CPU- or I/O-bound, DB slightly larger than RAM to 1TB, 20-40% small data write queries",
    },
    "dw": {
        "name": "Data Warehouse",
        "description": "Typically I/O- or RAM-bound, large bulk loads of data, large complex reporting queries",
    },
    "desktop": {
        "name": "Desktop Application",
        "description": "Not a dedicated database, general workstation use",
    },
    "mixed": {
        "name": "Mixed Type",
        "description": "Mixed DW and OLTP characteristics, wide mixture of queries",
    },
}


@click.group()
def cli() -> None:
    """AutoPG CLI tool for PostgreSQL configuration and system analysis."""
    pass


@cli.command()
def build_config() -> None:
    """Build a PostgreSQL configuration based on workload and system characteristics."""
    config: Dict[str, Any] = {}

    # Version
    config["postgres_version"] = questionary.text("PostgreSQL version?", default="17").ask()

    # OS Type
    config["os_type"] = questionary.select(
        "Operating System Type:", choices=["Linux"], default="Linux"
    ).ask()

    # DB Type
    choices = [
        {"name": f"{v['name']}: {v['description']}", "value": k} for k, v in DB_TYPES.items()
    ]

    config["db_type"] = questionary.select("What type of database workload?", choices=choices).ask()

    # System Resources
    config["cpu_count"] = questionary.text(
        "Number of CPUs/cores available?",
        default=str(get_cpu_info()[0]),
        validate=lambda text: text.isdigit() and int(text) > 0,
    ).ask()

    config["max_connections"] = questionary.text(
        "Expected number of concurrent connections?",
        default="100",
        validate=lambda text: text.isdigit() and int(text) > 0,
    ).ask()

    config["storage_type"] = questionary.select(
        "Primary data storage type:", choices=["SSD", "Network SAN", "HDD"]
    ).ask()

    # Output the configuration
    console.print("\nGenerated Configuration Profile:")
    console.print(json.dumps(config, indent=2))


@cli.command()
def system_info() -> None:
    """Get system configuration information as JSON."""
    try:
        total_mem, available_mem = get_memory_info()
        cpu_count, cpu_freq = get_cpu_info()
        disk_type = get_disk_type()

        info = {
            "memory": {"total_gb": round(total_mem, 2), "available_gb": round(available_mem, 2)},
            "cpu": {"cores": cpu_count, "frequency_mhz": round(cpu_freq, 2)},
            "storage": {"primary_disk_type": disk_type if disk_type else "Unknown"},
            "os": platform.system(),
        }

        console.print(json.dumps(info, indent=2))

    except Exception as e:
        console.print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    cli()
