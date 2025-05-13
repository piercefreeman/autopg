import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from autopgpool.cli import generate_pgbouncer_config
from autopgpool.config import MainConfig, PgbouncerConfig, Pool, User


def test_generate_pgbouncer_config() -> None:
    """Test that pgbouncer config files are generated correctly."""
    # Create a test config
    config = MainConfig(
        users=[
            User(username="testuser", password="testpass", grants=["testdb"]),
            User(username="admin", password="adminpass", grants=["testdb"]),
        ],
        pools={
            "testdb": Pool(
                remote=Pool.RemoteDatabase(
                    host="localhost",
                    port=5432,
                    database="testdb",
                    username="pguser",
                    password="pgpass",
                ),
                pool_mode="transaction",
            )
        },
        pgbouncer=PgbouncerConfig(
            listen_port=6432,
            auth_type="md5",
            admin_users=["admin"],
            passthrough_kwargs={"application_name": "pgbouncer"},
        ),
    )

    # Create a temporary directory for the output
    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate the config files
        generate_pgbouncer_config(config, temp_dir)

        # Check that the files were created
        assert os.path.exists(os.path.join(temp_dir, "pgbouncer.ini"))
        assert os.path.exists(os.path.join(temp_dir, "userlist.txt"))

        # Check the content of the pgbouncer.ini file
        with open(os.path.join(temp_dir, "pgbouncer.ini"), "r") as f:
            pgbouncer_ini = f.read()
            # Verify key configuration elements are present
            assert "[pgbouncer]" in pgbouncer_ini
            assert "listen_port = 6432" in pgbouncer_ini
            assert "auth_type = \"md5\"" in pgbouncer_ini
            assert "application_name = \"pgbouncer\"" in pgbouncer_ini
            assert "[databases]" in pgbouncer_ini
            assert "testdb = " in pgbouncer_ini
            assert "host=localhost" in pgbouncer_ini
            assert "port=5432" in pgbouncer_ini

        # Check the content of the userlist.txt file
        with open(os.path.join(temp_dir, "userlist.txt"), "r") as f:
            userlist = f.read()
            # MD5 passwords are hashed, so we can't check exact values
            assert "\"testuser\"" in userlist
            assert "\"admin\"" in userlist
