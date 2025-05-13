import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from autopgpool.config import MainConfig
from autopgpool.ini_writer import (
    UserWithGrants,
    write_hba_file,
    write_ini_file,
    write_userlist_file,
)

console = Console()

DEFAULT_CONFIG_PATH = "/etc/autopgpool/autopgpool.toml"
DEFAULT_OUTPUT_DIR = "/etc/pgbouncer"


def load_toml_config(config_path: str) -> dict[str, Any]:
    """
    Load a TOML configuration file.

    Args:
        config_path: Path to the TOML file

    Returns:
        Dictionary containing the parsed TOML data
    """
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        console.print(f"[red]Error: Config file not found at {config_path}[/red]")
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        console.print(f"[red]Error parsing TOML file: {str(e)}[/red]")
        sys.exit(1)


def generate_pgbouncer_config(config: MainConfig, output_dir: str) -> None:
    """
    Generate pgbouncer configuration files from the MainConfig.

    Args:
        config: The parsed configuration
        output_dir: Directory to write configuration files to
    """
    output_path = Path(output_dir)
    os.makedirs(output_path, exist_ok=True)

    # Create users with grants for HBA configuration
    users_with_grants = [
        UserWithGrants(username=user.username, password=user.password, grants=user.grants)
        for user in config.users
    ]

    # Write userlist.txt file
    write_userlist_file(
        [
            UserWithGrants(username=u.username, password=u.password, grants=u.grants)
            for u in config.users
        ],
        str(output_path / "userlist.txt"),
        encrypt=config.pgbouncer.auth_type,
    )

    # Even when the user hasn't requested hba auth, we want to write the HBA file
    # to provide our access grants
    write_hba_file(
        users_with_grants,
        str(output_path / "pgbouncer_hba.conf"),
    )
    # Create pgbouncer.ini
    pgbouncer_config = {
        "pgbouncer": {
            **config.pgbouncer.model_dump(exclude={"passthrough_kwargs"}),
            **config.pgbouncer.passthrough_kwargs,
            **{
                "auth_type": "hba",
                "auth_file": str(output_path / "pgbouncer_hba.conf"),
            },
        },
        "databases": {
            # Format: dbname = connection_string
            pool_name: (
                f"host={pool.remote.host} port={pool.remote.port} dbname={pool.remote.database} "
                f"user={pool.remote.username} password={pool.remote.password} pool_mode={pool.pool_mode}"
            )
            for pool_name, pool in config.pools.items()
        },
    }

    # Write the pgbouncer.ini file
    write_ini_file(
        pgbouncer_config,
        str(output_path / "pgbouncer.ini"),
    )

    console.print(f"[green]Successfully wrote configuration to {output_dir}[/green]")


@click.group()
def cli() -> None:
    """autopgpool CLI tool for pgbouncer configuration management."""
    pass


@cli.command()
@click.option(
    "--config-path",
    default=DEFAULT_CONFIG_PATH,
    help="Path to the autopgpool TOML configuration file",
)
@click.option(
    "--output-dir",
    default=DEFAULT_OUTPUT_DIR,
    help="Directory to write pgbouncer configuration files to",
)
def generate(config_path: str, output_dir: str) -> None:
    """Generate pgbouncer configuration files from TOML config."""
    # Load TOML configuration
    config_data = load_toml_config(config_path)

    try:
        # Parse into Pydantic model
        config = MainConfig.model_validate(config_data)

        # Generate configuration files
        generate_pgbouncer_config(config, output_dir)
    except Exception as e:
        console.print(f"[red]Error generating configuration: {str(e)}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    "--config-path",
    default=DEFAULT_CONFIG_PATH,
    help="Path to the autopgpool TOML configuration file",
)
def validate(config_path: str) -> None:
    """Validate the autopgpool TOML configuration file."""
    # Load TOML configuration
    config_data = load_toml_config(config_path)

    try:
        # Parse into Pydantic model
        MainConfig.model_validate(config_data)
        console.print(f"[green]Configuration file at {config_path} is valid.[/green]")
    except Exception as e:
        console.print(f"[red]Configuration validation error: {str(e)}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    cli()
