from collections import namedtuple
from unittest.mock import mock_open, patch

import pytest

from autopg.system_info import get_cpu_info, get_disk_type, get_memory_info


def test_get_memory_info() -> None:
    """Test memory info retrieval with mocked values"""
    VirtualMemory = namedtuple("VirtualMemory", ["total", "available"])
    mock_vm = VirtualMemory(
        total=32 * (1024**3),  # 32GB total
        available=16 * (1024**3),  # 16GB available
    )

    with patch("psutil.virtual_memory", return_value=mock_vm):
        total_gb, available_gb = get_memory_info()
        assert total_gb == 32.0
        assert available_gb == 16.0


def test_get_cpu_info() -> None:
    """Test CPU info retrieval with mocked values"""
    CpuFreq = namedtuple("CpuFreq", ["current"])
    mock_freq = CpuFreq(current=2.5)  # 2.5 GHz

    with patch("psutil.cpu_count", return_value=8) as mock_count:
        with patch("psutil.cpu_freq", return_value=mock_freq):
            cpu_count, current_freq = get_cpu_info()
            assert cpu_count == 8
            assert current_freq == 2.5
            mock_count.assert_called_once_with(logical=True)


@pytest.mark.parametrize(
    "rotational_value,expected_type",
    [
        ("0\n", "SSD"),
        ("1\n", "HDD"),
    ],
)
def test_get_disk_type(rotational_value: str, expected_type: str) -> None:
    """Test disk type detection for both SSD and HDD"""
    DiskPartition = namedtuple("DiskPartition", ["device"])
    mock_partition = DiskPartition(device="/dev/sda1")

    with (
        patch("psutil.disk_partitions", return_value=[mock_partition]),
        patch("os.path.exists", return_value=True),
        patch("builtins.open", mock_open(read_data=rotational_value)),
    ):
        disk_type = get_disk_type()
        assert disk_type == expected_type


@pytest.mark.parametrize(
    "error_source,expected_result",
    [
        ("disk_partitions", None),  # Test disk_partitions raising exception
        ("file_read", None),  # Test file read raising exception
    ],
)
def test_get_disk_type_errors(error_source: str, expected_result: None) -> None:
    """Test error handling in disk type detection"""
    DiskPartition = namedtuple("DiskPartition", ["device"])
    mock_partition = DiskPartition(device="/dev/sda1")

    if error_source == "disk_partitions":
        with patch("psutil.disk_partitions", side_effect=Exception()):
            assert get_disk_type() == expected_result
    else:
        with (
            patch("psutil.disk_partitions", return_value=[mock_partition]),
            patch("os.path.exists", return_value=True),
            patch("builtins.open", side_effect=Exception()),
        ):
            assert get_disk_type() == expected_result
