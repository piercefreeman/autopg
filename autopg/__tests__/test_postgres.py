from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from autopg.postgres import (
    format_value,
    get_postgres_version,
    read_postgresql_conf,
    write_postgresql_conf,
)


@pytest.mark.parametrize(
    "version_string,expected_version",
    [
        ("postgres (PostgreSQL) 16.3 (Homebrew)", 16),
        ("postgres (PostgreSQL) 16.6 (Debian 16.6-1.pgdg120+1)", 16),
    ],
)
def test_get_postgres_version(version_string: str, expected_version: int) -> None:
    """Test that we can correctly parse different PostgreSQL version strings"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = version_string
        assert get_postgres_version() == expected_version


def test_read_postgresql_conf(tmp_path: Path) -> None:
    """Test reading PostgreSQL configuration from a file"""
    # Create a mock postgresql.conf file
    conf_dir = tmp_path / "postgresql"
    conf_dir.mkdir()
    conf_file = conf_dir / "postgresql.conf"

    test_config = """
    # This is a comment
    shared_buffers = 128MB
    work_mem = '4MB'
    max_connections = 100
    invalid_line_without_equals
    """
    conf_file.write_text(test_config)

    # Read the configuration
    config = read_postgresql_conf(str(conf_dir))

    # Verify the parsed configuration
    assert config == {
        "shared_buffers": "128MB",
        "work_mem": "4MB",
        "max_connections": "100",
    }

    # Test with non-existent file
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert read_postgresql_conf(str(empty_dir)) == {}

    # Test with .base file taking precedence
    base_file = conf_dir / "postgresql.conf.base"
    base_file.write_text("shared_buffers = 256MB")
    config = read_postgresql_conf(str(conf_dir))
    assert config == {"shared_buffers": "256MB"}


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        (100, "100"),
        (3.14, "3.14"),
        ("128MB", "'128MB'"),
        ("on", "'on'"),
        (0, "0"),
        (-1, "-1"),
        (True, "true"),
        (False, "false"),
    ],
)
def test_format_value(input_value: int | float | str | bool, expected_output: str) -> None:
    """Test formatting of different configuration value types"""
    assert format_value(input_value) == expected_output


def test_write_postgresql_conf(tmp_path: Path) -> None:
    """Test writing PostgreSQL configuration to a file"""
    conf_dir = tmp_path / "postgresql"
    conf_dir.mkdir()

    test_config: Dict[str, Any] = {
        "shared_buffers": "128MB",
        "work_mem": 4,
        "max_connections": 100,
        "ssl": "on",
    }

    # Write the configuration
    write_postgresql_conf(test_config, str(conf_dir))

    # Verify the written file
    conf_file = conf_dir / "postgresql.conf"
    assert conf_file.exists()

    # Read the written content
    content = conf_file.read_text()

    # Check header
    assert "# Generated by AutoPG" in content

    # Check values are properly formatted
    assert "shared_buffers = '128MB'" in content
    assert "work_mem = 4" in content
    assert "max_connections = 100" in content
    assert "ssl = 'on'" in content


def test_backup_postgresql_conf(tmp_path: Path) -> None:
    """Test backup functionality"""
    # Existing configuration file should be backed up
    conf_dir = tmp_path / "postgresql"
    conf_dir.mkdir()
    conf_file = conf_dir / "postgresql.conf"
    conf_file.write_text("existing_param = 'value'")

    write_postgresql_conf({"new_param": "value"}, str(conf_dir))

    # Test backup functionality
    base_conf = conf_dir / "postgresql.conf.base"
    assert base_conf.exists()  # Backup should be created
    assert base_conf.read_text() == "existing_param = 'value'"
