import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from autopgpool.config import MainConfig, User
from autopgpool.ini_writer import write_ini_file, write_userlist_file

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

    # Create pgbouncer.ini
    pgbouncer_config = {
        "pgbouncer": {
            **config.pgbouncer.model_dump(exclude={"passthrough_kwargs"}),
            **config.pgbouncer.passthrough_kwargs,
        },
        "databases": {
            # Format: dbname = connection_string
            db.database: (
                f"host={db.host} port={db.port} dbname={db.database} "
                f"user={db.username} password={db.password} pool_mode={db.pool_mode}"
            )
            for db in config.databases
        },
    }

    # Write the pgbouncer.ini file
    write_ini_file(
        pgbouncer_config,
        output_path / "pgbouncer.ini",
    )

    # Write userlist.txt file
    users = [User(username=user.username, password=user.password) for user in config.users]
    write_userlist_file(users, output_path / "userlist.txt", encrypt=config.pgbouncer.auth_type)

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
