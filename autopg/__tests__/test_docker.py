import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Generator

import psycopg2
import pytest

from autopg.postgres import write_postgresql_conf


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace for Docker tests"""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir) / "workspace"
        # Copy current workspace to temp directory
        shutil.copytree(os.getcwd(), workspace, dirs_exist_ok=True)
        yield workspace


@pytest.mark.parametrize("postgres_version", ["16", "17"])
def test_docker_max_connections(temp_workspace: Path, postgres_version: str) -> None:
    """
    Test that Docker image correctly applies PostgreSQL configuration changes.
    Specifically tests max_connections parameter.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param postgres_version: Version of PostgreSQL to test with

    """
    # Write a custom PostgreSQL configuration
    postgres_dir = temp_workspace / "postgresql"
    postgres_dir.mkdir(exist_ok=True)
    write_postgresql_conf({"max_connections": "45"}, str(postgres_dir))

    # Build the Docker image
    test_tag = f"autopg:test-{postgres_version}"
    subprocess.run(
        [
            "docker",
            "build",
            "--build-arg",
            f"POSTGRES_VERSION={postgres_version}",
            "-t",
            test_tag,
            ".",
        ],
        cwd=temp_workspace,
        check=True,
    )

    # Start the container with credentials
    container_id = (
        subprocess.check_output(
            [
                "docker",
                "run",
                "-d",
                "-p",
                "5432:5432",
                "-e",
                "POSTGRES_USER=test_user",
                "-e",
                "POSTGRES_PASSWORD=test_password",
                test_tag,
            ],
            cwd=temp_workspace,
        )
        .decode()
        .strip()
    )

    try:
        # Wait for PostgreSQL to be ready
        subprocess.run(
            ["docker", "exec", container_id, "pg_isready", "-t", "30"],
            check=True,
        )

        # Connect and verify max_connections
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_user",  # PostgreSQL creates a database with the same name as the user by default
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW max_connections")
                result = cur.fetchone()
                if result is None:
                    raise AssertionError("No result returned from max_connections query")
                assert result[0] == "45"  # PostgreSQL returns this as a string
        finally:
            conn.close()

    finally:
        # Clean up the container
        subprocess.run(["docker", "stop", container_id], check=True)
        subprocess.run(["docker", "rm", container_id], check=True)
