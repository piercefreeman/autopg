from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    """
    Find the project root by looking for the pyproject.toml file.

    Returns:
        pathlib.Path: Path to the project root
    """
    current_dir = Path(__file__).resolve().parent

    while current_dir != current_dir.parent:
        if (current_dir / "pyproject.toml").exists():
            return current_dir
        current_dir = current_dir.parent

    raise FileNotFoundError("Could not find project root (pyproject.toml)")
