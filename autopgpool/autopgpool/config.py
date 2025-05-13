from typing import Any, Literal

from pydantic import BaseModel, model_validator

POOL_MODES = Literal["session", "transaction", "statement"]
AUTH_TYPES = Literal["cert", "md5", "scram-sha-256", "plain", "trust", "any", "hba", "pam"]


class User(BaseModel):
    """
    A user that can connect to the database through pgbouncer. This user gets
    encoded and placed into the pgbouncer userlist.

    """

    username: str

    # Specified in raw text; we will encode this internally
    password: str


class Database(BaseModel):
    """
    A synthetically defined database that can be connected to through pgbouncer. These will
    establish an independent connection pool for each of these databases.

    """

    host: str
    port: int
    database: str
    username: str
    password: str
    pool_mode: POOL_MODES = "transaction"


class PgbouncerConfig(BaseModel):
    listen_addr: str = "*"
    listen_port: int = 6432
    listen_addr: str = "0.0.0.0"

    auth_type: AUTH_TYPES = "md5"
    pool_mode: POOL_MODES = "transaction"

    max_client_conn: int = 100
    default_pool_size: int = 10

    ignore_startup_parameters: list[str] = ["extra_float_digits"]

    admin_users: list[str] | None = None
    stats_users: list[str] | None = None

    # By default we stop stalled transactions from blocking the pool
    # fixes: common query_wait_timeout (age=120s) where queries can't
    # be handled in the pool because the connection stream is saturated
    # If users override this to None, no timeout will be applied.
    # https://dba.stackexchange.com/questions/261709/pgbouncer-logging-details-for-query-wait-timeout-error
    # https://stackoverflow.com/questions/23394272/how-does-pgbouncer-behave-when-transaction-pooling-is-enabled-and-a-single-state
    idle_transaction_timeout: int | None = 60

    # Support prepared statements, which are used by some default query constructors
    # in sqlalchemy and asyncpg
    # https://github.com/pgbouncer/pgbouncer/pull/845
    max_prepared_statements: int = 10

    passthrough_kwargs: dict[str, Any] = {}


class MainConfig(BaseModel):
    """
    The main configuration for pgbouncer.
    """

    users: list[User]
    databases: list[Database]
    pgbouncer: PgbouncerConfig

    @model_validator(mode="after")
    def validate_pgbouncer_users(self):
        # Ensure that any specified users have been added to the userlist
        valid_users = {user.username for user in self.users}
        for user in self.pgbouncer.admin_users or []:
            if user not in valid_users:
                raise ValueError(f"User {user} is not in the userlist")
        for user in self.pgbouncer.stats_users or []:
            if user not in valid_users:
                raise ValueError(f"User {user} is not in the userlist")

        return self
