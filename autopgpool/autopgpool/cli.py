import os
import sys
from pathlib import Path

import click
from rich.markup import escape

from autopgpool.config import MainConfig, User
from autopgpool.env import load_toml_config
from autopgpool.ini_writer import (
    write_hba_file,
    write_ini_file,
    write_userlist_file,
)
from autopgpool.logging import CONSOLE

DEFAULT_CONFIG_PATH = "/etc/autopgpool/autopgpool.toml"
DEFAULT_OUTPUT_DIR = "/etc/pgbouncer"


def generate_pgbouncer_config(config: MainConfig, output_dir: str) -> None:
    """
    Generate pgbouncer configuration files from the MainConfig.

    Args:
        config: The parsed configuration
        output_dir: Directory to write configuration files to
    """
    output_path = Path(output_dir)
    os.makedirs(output_path, exist_ok=True)

    userlist_path = output_path / "userlist.txt"
    hba_path = output_path / "pgbouncer_hba.conf"
    ini_path = output_path / "pgbouncer.ini"

    # Create users with grants for HBA configuration
    users = [
        User(username=user.username, password=user.password, grants=user.grants)
        for user in config.users
    ]

    # Write userlist.txt file
    write_userlist_file(users, userlist_path, encrypt=config.pgbouncer.auth_type)
    CONSOLE.print(f"Wrote userlist file to [bold]{userlist_path}[/bold]")
    CONSOLE.print(f"Userlist file contents:\n###\n{escape(userlist_path.read_text())}\n###\n")

    # Even when the user hasn't requested hba auth, we want to write the HBA file
    # to provide our access grants
    write_hba_file(users, hba_path)
    CONSOLE.print(f"Wrote HBA file to [bold]{hba_path}[/bold]")
    CONSOLE.print(f"HBA file contents:\n###\n{escape(hba_path.read_text())}\n###\n")

    # Create pgbouncer.ini
    pgbouncer_config = {
        "pgbouncer": {
            **config.pgbouncer.model_dump(exclude={"passthrough_kwargs"}),
            **config.pgbouncer.passthrough_kwargs,
            **{
                "auth_type": "hba",
                "auth_file": userlist_path,
                "auth_hba_file": hba_path,
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
    write_ini_file(pgbouncer_config, ini_path)
    CONSOLE.print(f"Wrote pgbouncer.ini file to [bold]{ini_path}[/bold]")
    CONSOLE.print(f"PGBouncer.ini file contents:\n###\n{escape(ini_path.read_text())}\n###\n")

    CONSOLE.print(f"[green]Successfully wrote configuration to {output_dir}[/green]")


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
        CONSOLE.print(f"[red]Error generating configuration: {str(e)}[/red]")
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
        CONSOLE.print(f"[green]Configuration file at {config_path} is valid.[/green]")
    except Exception as e:
        CONSOLE.print(f"[red]Configuration validation error: {str(e)}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    cli()
