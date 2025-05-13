from pathlib import Path

from autopgpool.cli import generate_pgbouncer_config
from autopgpool.config import MainConfig, PgbouncerConfig, Pool, User


def test_generate_pgbouncer_config(temp_dir: Path) -> None:
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

    # Generate the config files
    generate_pgbouncer_config(config, str(temp_dir))

    # Check that the files were created
    assert (temp_dir / "pgbouncer.ini").exists()
    assert (temp_dir / "userlist.txt").exists()
    assert (temp_dir / "pgbouncer_hba.conf").exists()

    # Check the content of the pgbouncer.ini file
    pgbouncer_ini = (temp_dir / "pgbouncer.ini").read_text()
    # Verify key configuration elements are present
    assert "[pgbouncer]" in pgbouncer_ini
    assert "listen_port = 6432" in pgbouncer_ini
    assert 'auth_type = "hba"' in pgbouncer_ini  # overridden
    assert "auth_file = " in pgbouncer_ini
    assert 'application_name = "pgbouncer"' in pgbouncer_ini
    assert "[databases]" in pgbouncer_ini
    assert "testdb = " in pgbouncer_ini
    assert "host=localhost" in pgbouncer_ini
    assert "port=5432" in pgbouncer_ini

    # Check the content of the userlist.txt file
    userlist = (temp_dir / "userlist.txt").read_text()
    # MD5 passwords are hashed, so we can't check exact values
    assert '"testuser"' in userlist
    assert '"admin"' in userlist

    # Check the content of the HBA file
    hba_content = (temp_dir / "pgbouncer_hba.conf").read_text()
    assert "# TYPE\tDATABASE\tUSER\tADDRESS\tMETHOD" in hba_content

    # Check HBA entries for testuser
    assert "local\ttestdb\ttestuser\t\tmd5" in hba_content
    assert "host\ttestdb\ttestuser\t0.0.0.0/0\tmd5" in hba_content
    assert "host\ttestdb\ttestuser\t::0/0\tmd5" in hba_content
    assert "host\tall\ttestuser\t!0.0.0.0/0\t!md5" in hba_content
    assert "host\tall\ttestuser\t!::0/0\t!md5" in hba_content

    # Check HBA entries for admin
    assert "local\ttestdb\tadmin\t\tmd5" in hba_content
    assert "host\ttestdb\tadmin\t0.0.0.0/0\tmd5" in hba_content
    assert "host\ttestdb\tadmin\t::0/0\tmd5" in hba_content
    assert "host\tall\tadmin\t!0.0.0.0/0\t!md5" in hba_content
    assert "host\tall\tadmin\t!::0/0\t!md5" in hba_content
