import sys
import tomllib
from os import getenv
from typing import Any, TypeVar

from autopgpool.logging import CONSOLE

T = TypeVar("T")


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
            payload = tomllib.load(f)
            payload = swap_env(payload)
            return payload
    except FileNotFoundError:
        CONSOLE.print(f"[red]Error: Config file not found at {config_path}[/red]")
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        CONSOLE.print(f"[red]Error parsing TOML file: {str(e)}[/red]")
        sys.exit(1)


def swap_env(obj: T) -> T:
    """
    Recursively walk a structure (dict / list / scalar) and replace every string that
    starts with `$` by the matching OS environment variable.

    """
    if isinstance(obj, dict):
        return {k: swap_env(v) for k, v in obj.items()}  # type: ignore

    if isinstance(obj, list):
        return [swap_env(item) for item in obj]  # type: ignore

    if isinstance(obj, str) and obj.startswith("$"):
        env_name = obj[1:]
        env_val = getenv(env_name)
        if env_val is None:
            raise EnvironmentError(
                f"Environment variable '{env_name}' referenced in config but not set."
            )
        return env_val  # type: ignore

    return obj
