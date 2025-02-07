from unittest.mock import patch

import pytest

from autopg.postgres import get_postgres_version


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
