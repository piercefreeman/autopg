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


def build_docker_image(temp_workspace: Path, postgres_version: str) -> str:
    """
    Build the Docker image for testing.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param postgres_version: Version of PostgreSQL to test with

    """
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
    return test_tag


def start_postgres_container(
    temp_workspace: Path,
    test_tag: str,
    env_vars: dict[str, str] | None = None,
) -> str:
    """
    Start a PostgreSQL container for testing.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param test_tag: Docker image tag to run
    :param env_vars: Environment variables to set in the container

    """
    prefix_env_args = [
        "docker",
        "run",
        "-d",
        "-p",
        "5432:5432",
        "-e",
        "POSTGRES_USER=test_user",
        "-e",
        "POSTGRES_PASSWORD=test_password",
    ]

    for k, v in (env_vars or {}).items():
        prefix_env_args.extend(["-e", f"{k}={v}"])

    return (
        subprocess.check_output(
            [
                *prefix_env_args,
                test_tag,
            ],
            cwd=temp_workspace,
        )
        .decode()
        .strip()
    )


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
            )
            console.print("PostgreSQL is ready")
            break
        except subprocess.CalledProcessError:
            sleep(1)

    # Give time to fully boot and be reachable
    sleep(2)


def cleanup_container(container_id: str) -> None:
    """
    Stop and remove a Docker container.

    :param container_id: Docker container ID

    """
    subprocess.run(["docker", "stop", container_id], check=True)
    subprocess.run(["docker", "rm", container_id], check=True)


@pytest.mark.integration
@pytest.mark.parametrize("postgres_version", ["16", "17"])
def test_docker_max_connections(temp_workspace: Path, postgres_version: str) -> None:
    """
    Test that Docker image correctly applies PostgreSQL configuration changes.
    Specifically tests max_connections parameter.

    :param temp_workspace: Temporary directory containing a copy of the workspace
    :param postgres_version: Version of PostgreSQL to test with

    """
    # Build and start container
    test_tag = build_docker_image(temp_workspace, postgres_version)
    container_id = start_postgres_container(
        temp_workspace,
        test_tag,
        env_vars={
            "AUTOPG_NUM_CONNECTIONS": "45",
        },
    )

    try:
        # Wait for PostgreSQL to be ready
        wait_for_postgres(container_id)

        # Connect and verify max_connections
        conn = psycopg.connect(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            dbname="test_user",  # PostgreSQL creates a database with the same name as the user by default
        )

        try:
            with conn.cursor() as cur:
                cur.execute("SHOW max_connections")
                result = cur.fetchone()
                assert result is not None
                assert result[0] == "45"  # PostgreSQL returns this as a string (default is 100)
        finally:
            conn.close()

    except Exception as e:
        console.print(f"Error: {e}")

        # Return all of the docker errors
        subprocess.run(["docker", "logs", container_id], check=True)

        raise e
    finally:
        cleanup_container(container_id)
