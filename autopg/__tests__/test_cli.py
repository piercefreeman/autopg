from pathlib import Path
from typing import Any, Generator
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from autopg.cli import cli
from autopg.constants import (
    OS_LINUX,
)
from autopg.system_info import CpuInfo, DiskType, MemoryInfo


@pytest.fixture
def cli_runner() -> Generator[CliRunner, None, None]:
    """Create a Click CLI runner for testing"""
    runner = CliRunner()
    with runner.isolated_filesystem():
        yield runner


@pytest.fixture
def mock_system_info():
    """Mock all system info calls to return consistent values"""
    with (
        patch("autopg.cli.get_memory_info") as mock_memory,
        patch("autopg.cli.get_cpu_info") as mock_cpu,
        patch("autopg.cli.get_disk_type") as mock_disk,
        patch("autopg.cli.get_os_type") as mock_os,
        patch("autopg.cli.get_postgres_version") as mock_postgres,
    ):
        # Set up mock returns
        mock_memory.return_value = MemoryInfo(total=16, available=8)  # 16GB total, 8GB available
        mock_cpu.return_value = CpuInfo(count=4, current_freq=2.5)  # 4 cores, 2.5GHz
        mock_disk.return_value = DiskType.SSD
        mock_os.return_value = OS_LINUX
        mock_postgres.return_value = "14.0"

        yield


def test_build_config(cli_runner: CliRunner, mock_system_info: Any, tmp_path: Path):
    """Test that build_config generates a valid configuration file"""
    # Create a mock postgresql.conf in the temporary directory
    pg_conf_dir = tmp_path / "postgresql"
    pg_conf_dir.mkdir()
    pg_conf_file = pg_conf_dir / "postgresql.conf"
    pg_conf_file.write_text("")

    # Run the CLI command
    result = cli_runner.invoke(cli, ["build-config", "--pg-path", str(pg_conf_dir)])

    # Check the command succeeded
    assert result.exit_code == 0
    assert "Successfully wrote new PostgreSQL configuration!" in result.output

    # Verify the configuration file was created and contains expected settings
    assert pg_conf_file.exists()
    config_content = pg_conf_file.read_text()

    # Check for some key configuration parameters
    assert "shared_buffers" in config_content
    assert "effective_cache_size" in config_content
    assert "work_mem" in config_content
    assert "max_connections" in config_content
