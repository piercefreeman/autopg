import tomllib
from pathlib import Path
from typing import Any

from autopgpool.config import MainConfig


def test_example_config_loads_correctly(project_root: Path) -> None:
    """
    Test that the example config file can be loaded correctly into the MainConfig model.
    """
    # Find the project root and the example config file
    example_config_path = project_root / "config.example.toml"

    assert example_config_path.exists(), f"Example config file not found at {example_config_path}"

    # Load the TOML file
    with open(example_config_path, "rb") as f:
        config_data: dict[str, Any] = tomllib.load(f)

    # Parse the config data into the MainConfig model
    MainConfig.model_validate(config_data)
