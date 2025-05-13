import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from time import sleep, time
from typing import Generator

import psycopg
import pytest
from rich.console import Console

console = Console()


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace for Docker tests"""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir) / "workspace"
        # Copy current workspace to temp directory
        shutil.copytree(os.getcwd(), workspace, dirs_exist_ok=True)
        yield workspace


def build_autopgpool_docker_image(temp_workspace: Path) -> str:
    """
    Build the AutoPGPool Docker image for testing.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :return: Docker image tag
    """
    test_tag = "autopgpool:test"
    subprocess.run(
        [
            "docker",
            "build",
            "-t",
            test_tag,
            ".",
        ],
        cwd=temp_workspace,
        check=True,
    )
    return test_tag


def start_postgres_container(temp_workspace: Path) -> tuple[str, int]:
    """
    Start a PostgreSQL container for testing.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :return: Container ID and mapped port
    """
    # Use a random port on the host to avoid conflicts
    postgres_port = 5433

    container_id = (
        subprocess.check_output(
            [
                "docker",
                "run",
                "-d",
                "-p",
                f"{postgres_port}:5432",
                "-e",
                "POSTGRES_USER=test_user",
                "-e",
                "POSTGRES_PASSWORD=test_password",
                "-e",
                "POSTGRES_DB=test_db",
                "postgres:15",
            ],
            cwd=temp_workspace,
        )
        .decode()
        .strip()
    )

    return container_id, postgres_port


def wait_for_postgres(container_id: str, timeout_seconds: int = 30) -> None:
    """
    Wait for PostgreSQL to be ready with a timeout.

    :param container_id: Docker container ID
    :param timeout_seconds: Maximum time to wait in seconds
    """
    start_time = time()
    while True:
        if time() - start_time > timeout_seconds:
            raise TimeoutError(f"PostgreSQL not ready after {timeout_seconds} seconds")

        try:
            subprocess.run(
                ["docker", "exec", container_id, "pg_isready", "-t", "5"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            console.print("PostgreSQL is ready")
            break
        except subprocess.CalledProcessError:
            sleep(1)

    # Give time to fully boot and be reachable
    sleep(2)


def create_test_config(temp_workspace: Path, postgres_host: str, postgres_port: int) -> Path:
    """
    Create a test configuration file for autopgpool.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param postgres_host: Hostname of the PostgreSQL container
    :param postgres_port: Port of the PostgreSQL container
    :return: Path to the configuration file
    """
    config_content = f"""
# AutoPGPool Test Configuration

# User definitions
[[users]]
username = "test_user"
password = "test_password"
grants = ["test_db"]

# Database definitions
[pools.test_db]
pool_mode = "transaction"

[pools.test_db.remote]
host = "{postgres_host}"
port = {postgres_port}
database = "test_db"
username = "test_user"
password = "test_password"

# PGBouncer configuration
[pgbouncer]
listen_addr = "0.0.0.0"
listen_port = 6432
auth_type = "md5"
pool_mode = "transaction"
max_client_conn = 100
default_pool_size = 20
ignore_startup_parameters = ["extra_float_digits"]
"""

    config_path = temp_workspace / "test_config.toml"
    with open(config_path, "w") as f:
        f.write(config_content)

    return config_path


def start_autopgpool_container(
    temp_workspace: Path,
    image_tag: str,
    config_path: Path,
) -> tuple[str, int]:
    """
    Start an autopgpool container for testing.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param image_tag: Docker image tag to run
    :param config_path: Path to the configuration file
    :return: Container ID and mapped port
    """
    # Use a random port to avoid conflicts
    pgbouncer_port = 6432

    container_id = (
        subprocess.check_output(
            [
                "docker",
                "run",
                "-d",
                "-p",
                f"{pgbouncer_port}:6432",
                "-v",
                f"{config_path}:/etc/autopgpool/autopgpool.toml",
                image_tag,
            ],
            cwd=temp_workspace,
        )
        .decode()
        .strip()
    )

    return container_id, pgbouncer_port


def wait_for_pgbouncer(container_id: str, timeout_seconds: int = 30) -> None:
    """
    Wait for PgBouncer to be ready with a timeout.

    :param container_id: Docker container ID
    :param timeout_seconds: Maximum time to wait in seconds
    """
    start_time = time()

    # Wait for pgbouncer to start
    while True:
        if time() - start_time > timeout_seconds:
            raise TimeoutError(f"PgBouncer not ready after {timeout_seconds} seconds")

        try:
            # Check if pgbouncer process is running
            result = subprocess.run(
                ["docker", "exec", container_id, "ps", "aux"],
                check=True,
                capture_output=True,
                text=True,
            )
            if "pgbouncer" in result.stdout and "/usr/bin/pgbouncer" in result.stdout:
                console.print("PgBouncer is running")
                break
        except subprocess.CalledProcessError:
            pass

        sleep(1)

    # Give pgbouncer time to initialize
    sleep(5)


def cleanup_container(container_id: str) -> None:
    """
    Stop and remove a Docker container.

    :param container_id: Docker container ID
    """
    subprocess.run(["docker", "stop", container_id], check=True)
    subprocess.run(["docker", "rm", container_id], check=True)


@pytest.mark.integration
def test_autopgpool_connection(temp_workspace: Path) -> None:
    """
    Test that AutoPGPool correctly routes connections to PostgreSQL.

    This test:
    1. Starts a PostgreSQL container
    2. Builds and starts the AutoPGPool container
    3. Verifies that connections can be made through the pool

    :param temp_workspace: Temporary directory containing a copy of the workspace
    """
    postgres_container_id = None
    pgbouncer_container_id = None

    try:
        # Start PostgreSQL container
        postgres_container_id, postgres_port = start_postgres_container(temp_workspace)
        wait_for_postgres(postgres_container_id)

        # Create test configuration
        config_path = create_test_config(temp_workspace, "host.docker.internal", postgres_port)

        # Build and start AutoPGPool container
        autopgpool_tag = build_autopgpool_docker_image(temp_workspace)
        pgbouncer_container_id, pgbouncer_port = start_autopgpool_container(
            temp_workspace,
            autopgpool_tag,
            config_path,
        )
        wait_for_pgbouncer(pgbouncer_container_id)

        # Verify connection through pgbouncer
        conn = psycopg.connect(
            host="localhost",
            port=pgbouncer_port,
            user="test_user",
            password="test_password",
            dbname="test_db",
        )

        try:
            with conn.cursor() as cur:
                # Simple query to verify connection
                cur.execute("SELECT 1 AS test")
                result = cur.fetchone()
                assert result is not None
                assert result[0] == 1

                # Verify connection is through pgbouncer by checking application_name
                cur.execute("SHOW application_name")
                result = cur.fetchone()
                assert result is not None
                assert "pgbouncer" in result[0].lower()
        finally:
            conn.close()

    except Exception as e:
        console.print(f"Error: {e}")

        # Print logs from containers
        if pgbouncer_container_id:
            console.print("PgBouncer logs:")
            subprocess.run(["docker", "logs", pgbouncer_container_id], check=True)

        if postgres_container_id:
            console.print("PostgreSQL logs:")
            subprocess.run(["docker", "logs", postgres_container_id], check=True)

        raise e
    finally:
        # Clean up containers
        if pgbouncer_container_id:
            cleanup_container(pgbouncer_container_id)
        if postgres_container_id:
            cleanup_container(postgres_container_id)
