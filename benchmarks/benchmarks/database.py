"""
Database connection and utilities for benchmarking.
"""

import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from rich.console import Console

console = Console()


class DatabaseConnection:
    """Database connection wrapper with benchmarking utilities."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection: Optional[Connection] = None
    
    def __enter__(self) -> 'DatabaseConnection':
        """Enter context manager and establish connection."""
        self.connection = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            row_factory=dict_row
        )
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query and return the cursor."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor
    
    def execute_many(self, query: str, params_list: list) -> None:
        """Execute a query multiple times with different parameters."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        self.connection.commit()
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()
    
    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Context manager for database transactions."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        try:
            yield
            self.commit()
        except Exception:
            self.rollback()
            raise
    
    def get_table_info(self, schema: str = 'benchmark') -> Dict[str, Dict[str, Any]]:
        """Get information about tables in the specified schema."""
        query = """
            SELECT 
                t.table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))) as size,
                pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) as size_bytes,
                obj_description(c.oid) as comment
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            WHERE t.table_schema = %s
            AND t.table_type = 'BASE TABLE'
            ORDER BY pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) DESC
        """
        
        result = self.execute(query, (schema,)).fetchall()
        
        tables = {}
        for row in result:
            table_name = row['table_name']
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
            count_result = self.execute(count_query).fetchone()
            
            # Get column info
            col_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            columns = self.execute(col_query, (schema, table_name)).fetchall()
            
            tables[table_name] = {
                'size': row['size'],
                'size_bytes': row['size_bytes'],
                'row_count': count_result['count'] if count_result else 0,
                'comment': row['comment'],
                'columns': columns
            }
        
        return tables
    
    def analyze_table(self, table_name: str, schema: str = 'benchmark') -> None:
        """Run ANALYZE on a table to update statistics."""
        query = f"ANALYZE {schema}.{table_name}"
        self.execute(query)
        self.commit()
    
    def get_query_stats(self) -> list:
        """Get query statistics from pg_stat_statements."""
        query = """
            SELECT 
                query,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time,
                min_exec_time,
                rows
            FROM pg_stat_statements
            WHERE query LIKE '%benchmark%'
            ORDER BY total_exec_time DESC
            LIMIT 20
        """
        
        try:
            return self.execute(query).fetchall()
        except Exception:
            # pg_stat_statements might not be available
            return []
    
    def reset_stats(self) -> None:
        """Reset PostgreSQL statistics."""
        try:
            self.execute("SELECT pg_stat_reset()")
            self.execute("SELECT pg_stat_statements_reset()")
            self.commit()
        except Exception as e:
            console.print(f"Warning: Could not reset stats: {e}", style="yellow")
    
    def vacuum_analyze_table(self, table_name: str, schema: str = 'benchmark') -> None:
        """Run VACUUM ANALYZE on a table."""
        # VACUUM cannot be run inside a transaction
        if self.connection:
            self.connection.autocommit = True
            try:
                query = f"VACUUM ANALYZE {schema}.{table_name}"
                self.execute(query)
            finally:
                self.connection.autocommit = False


@contextmanager
def timed_operation(description: str, verbose: bool = False) -> Generator[Dict[str, float], None, None]:
    """Context manager to time database operations."""
    if verbose:
        console.print(f"⏱️  Starting: {description}")
    
    start_time = time.time()
    timing_info = {}
    
    try:
        yield timing_info
    finally:
        end_time = time.time()
        duration = end_time - start_time
        timing_info['duration'] = duration
        timing_info['start_time'] = start_time
        timing_info['end_time'] = end_time
        
        if verbose:
            console.print(f"✅ Completed: {description} ({duration:.2f}s)")


def get_connection_pool(db_config: Dict[str, Any], pool_size: int = 5) -> 'psycopg.pool.ConnectionPool':
    """Create a connection pool for concurrent operations."""
    from psycopg.pool import ConnectionPool
    
    conninfo = (
        f"host={db_config['host']} "
        f"port={db_config['port']} "
        f"dbname={db_config['database']} "
        f"user={db_config['user']} "
        f"password={db_config['password']}"
    )
    
    return ConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=pool_size,
        kwargs={'row_factory': dict_row}
    )
