# postgresql versions
DEFAULT_DB_VERSION = 18
DB_VERSIONS = [DEFAULT_DB_VERSION, 17, 16, 15, 14, 13, 12, 11, 10]

# os types
OS_LINUX = "linux"
OS_WINDOWS = "windows"
OS_MAC = "mac"

# db types
DB_TYPE_WEB = "web"
DB_TYPE_OLTP = "oltp"
DB_TYPE_DW = "dw"
DB_TYPE_DESKTOP = "desktop"
DB_TYPE_MIXED = "mixed"

# size units
SIZE_UNIT_MB = "MB"
SIZE_UNIT_GB = "GB"

# harddrive types
HARD_DRIVE_SSD = "SSD"
HARD_DRIVE_SAN = "SAN"
HARD_DRIVE_HDD = "HDD"

# maximum value for integer fields
MAX_NUMERIC_VALUE = 999999

SIZE_UNIT_MAP: dict[str, int] = {"KB": 1024, "MB": 1048576, "GB": 1073741824, "TB": 1099511627776}

KNOWN_STORAGE_VARS = [
    "shared_buffers",
    "effective_cache_size",
    "maintenance_work_mem",
    "wal_buffers",
    "work_mem",
    "min_wal_size",
    "max_wal_size",
]

PG_CONFIG_DIR = "/etc/postgresql"
PG_CONFIG_FILE = "postgresql.conf"
PG_CONFIG_FILE_BASE = "postgresql.conf.base"

PG_STAT_STATEMENTS_SQL = """-- AutoPG Extension Initialization
-- Enable pg_stat_statements extension for query statistics

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
"""
