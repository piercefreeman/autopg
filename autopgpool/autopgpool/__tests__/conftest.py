from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator

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


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Fixture that provides a temporary directory as a Path object."""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)
