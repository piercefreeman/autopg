import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from autopg.constants import (
    KNOWN_STORAGE_VARS,
    PG_CONFIG_DIR,
    PG_CONFIG_FILE,
    PG_CONFIG_FILE_BASE,
    SIZE_UNIT_MAP,
)

#
# Config management
#


def read_postgresql_conf(base_path: str = PG_CONFIG_DIR) -> dict[str, Any]:
    """Read the postgresql.conf file, preferring .base if it exists"""
    conf_path = Path(base_path) / PG_CONFIG_FILE
    base_conf_path = Path(base_path) / PG_CONFIG_FILE_BASE

    target_path = base_conf_path if base_conf_path.exists() else conf_path
    if not target_path.exists():
        return {}

    config: dict[str, str] = {}
    with target_path.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                try:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'")

                    if key in KNOWN_STORAGE_VARS:
                        value = parse_storage_value(value)
                    else:
                        value = parse_value(value)
                    config[key] = value
                except ValueError:
                    continue
    return config


def format_postgres_values(config: dict[str, Any]) -> dict[str, str]:
    """
    Re-format based on known units. The pipeline is expected to be:

    - format_postgres_values()
    - write_postgresql_conf()

    """

    # These values are ready for direct insertion into the config file
    str_config: dict[str, str] = {}

    for key, value in config.items():
        if key in KNOWN_STORAGE_VARS:
            # Storage values are always strings
            config_value = f"'{format_kb_value(value)}'"
        else:
            # We should only wrap with single quotes if the original value is a string
            config_value = format_value(value)
            config_value = f"'{config_value}'" if isinstance(value, str) else config_value
        str_config[key] = config_value

    return str_config


def write_postgresql_conf(
    config: dict[str, str], base_path: str = PG_CONFIG_DIR, backup: bool = True
) -> None:
    """Write the postgresql.conf file and optionally backup the old one"""
    conf_path = Path(base_path) / PG_CONFIG_FILE
    base_conf_path = Path(base_path) / PG_CONFIG_FILE_BASE

    # Backup existing config if requested
    if backup and conf_path.exists():
        shutil.copy(conf_path, base_conf_path)

    # Write new config
    with open(conf_path, "w") as f:
        f.write("# Generated by AutoPG\n\n")
        for key, value in sorted(config.items()):
            f.write(f"{key} = {value}\n")


#
# Formatters
#


def format_value(value: int | float | str | bool) -> str:
    """Format configuration values appropriately"""
    if isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, (int, float)):
        return str(value)
    return value


def parse_value(value: str) -> int | float | str | bool:
    """Parse configuration values appropriately"""
    if value.lower() in ["true", "false"]:
        return value.lower() == "true"
    elif value.isdigit():
        return int(value)
    return value


def format_kb_value(value: int) -> str:
    """
    Format a value in kilobytes to a human readable string with appropriate unit.
    The function will use the largest unit (GB, MB, KB) that results in a whole number.

    Args:
        value: The value in kilobytes to format

    Returns:
        A formatted string with the value and unit (e.g. "1GB", "100MB", "64kB")
    """
    # 0 is a special case
    if value == 0:
        return "0kB"

    if value % (SIZE_UNIT_MAP["GB"] // SIZE_UNIT_MAP["KB"]) == 0:
        return f"{value // (SIZE_UNIT_MAP['GB'] // SIZE_UNIT_MAP['KB'])}GB"
    elif value % (SIZE_UNIT_MAP["MB"] // SIZE_UNIT_MAP["KB"]) == 0:
        return f"{value // (SIZE_UNIT_MAP['MB'] // SIZE_UNIT_MAP['KB'])}MB"
    return f"{value}kB"


def parse_storage_value(value: str) -> int:
    """Parse storage values into kb"""
    if value.endswith("GB"):
        return int(value.strip("GB")) * SIZE_UNIT_MAP["GB"] // SIZE_UNIT_MAP["KB"]
    elif value.endswith("MB"):
        return int(value.strip("MB")) * SIZE_UNIT_MAP["MB"] // SIZE_UNIT_MAP["KB"]
    return int(value.strip("kB"))


#
# Helpers
#


def get_postgres_version() -> int:
    """Get the version of PostgreSQL installed

    Returns:
        int: The major version number of PostgreSQL (e.g. 16 for PostgreSQL 16.3)

    Raises:
        subprocess.CalledProcessError: If postgres is not installed or command fails
        ValueError: If version string cannot be parsed
    """
    try:
        result = subprocess.run(
            ["postgres", "--version"], capture_output=True, text=True, check=True
        )
        version_str = result.stdout.strip()
        # Use regex to find version number pattern (e.g. "16.3" in "postgres (PostgreSQL) 16.3 (Homebrew)")
        version_match = re.search(r"(\d+)\.?\d*", version_str)
        if not version_match:
            raise ValueError("Could not find version number in postgres output")
        return int(version_match.group(1))
    except (subprocess.CalledProcessError, ValueError) as e:
        raise ValueError(f"Failed to get PostgreSQL version: {str(e)}") from e
