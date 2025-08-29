"""
Database connection and utilities for benchmarking using asyncpg.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Generator, List, Optional

import asyncpg
from rich.console import Console

console = Console()


class AsyncDatabaseConnection:
    """Async database connection wrapper with benchmarking utilities."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection: Optional[asyncpg.Connection] = None
    
    async def __aenter__(self) -> 'AsyncDatabaseConnection':
        """Enter async context manager and establish connection."""
        self.connection = await asyncpg.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        return self
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context manager and close connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None
    
    async def execute(self, query: str, *params: Any) -> List[asyncpg.Record]:
        """Execute a query and return all results."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        return await self.connection.fetch(query, *params)
    
    async def execute_one(self, query: str, *params: Any) -> Optional[asyncpg.Record]:
        """Execute a query and return one result."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        return await self.connection.fetchrow(query, *params)
    
    async def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query multiple times with different parameters."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        await self.connection.executemany(query, params_list)
    
    async def execute_batch(self, query: str, params_list: List[tuple]) -> None:
        """Execute a batch of queries efficiently."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        # Convert to format expected by asyncpg
        await self.connection.executemany(query, params_list)
    
    @asynccontextmanager
    async def transaction(self):
        """Async context manager for database transactions."""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        async with self.connection.transaction():
            yield
    
    async def get_table_info(self, schema: str = 'benchmark') -> Dict[str, Dict[str, Any]]:
        """Get information about tables in the specified schema."""
        query = """
            SELECT 
                t.table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))) as size,
                pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) as size_bytes,
                obj_description(c.oid) as comment
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            ORDER BY pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) DESC
        """
        
        result = await self.execute(query, schema)
        
        tables = {}
        for row in result:
            table_name = row['table_name']
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
            count_result = await self.execute_one(count_query)
            
            # Get column info
            col_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """
            columns = await self.execute(col_query, schema, table_name)
            
            tables[table_name] = {
                'size': row['size'],
                'size_bytes': row['size_bytes'],
                'row_count': count_result['count'] if count_result else 0,
                'comment': row['comment'],
                'columns': [dict(col) for col in columns]
            }
        
        return tables
    
    async def analyze_table(self, table_name: str, schema: str = 'benchmark') -> None:
        """Run ANALYZE on a table to update statistics."""
        query = f"ANALYZE {schema}.{table_name}"
        await self.connection.execute(query)
    
    async def get_query_stats(self) -> List[Dict[str, Any]]:
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
            result = await self.execute(query)
            return [dict(row) for row in result]
        except Exception:
            # pg_stat_statements might not be available
            return []
    
    async def reset_stats(self) -> None:
        """Reset PostgreSQL statistics."""
        try:
            await self.connection.execute("SELECT pg_stat_reset()")
            await self.connection.execute("SELECT pg_stat_statements_reset()")
        except Exception as e:
            console.print(f"Warning: Could not reset stats: {e}", style="yellow")
    
    async def vacuum_analyze_table(self, table_name: str, schema: str = 'benchmark') -> None:
        """Run VACUUM ANALYZE on a table."""
        # VACUUM cannot be run inside a transaction, so we need a separate connection
        vacuum_conn = await asyncpg.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        
        try:
            query = f"VACUUM ANALYZE {schema}.{table_name}"
            await vacuum_conn.execute(query)
        finally:
            await vacuum_conn.close()


@asynccontextmanager
async def timed_operation(description: str, verbose: bool = False) -> Generator[Dict[str, float], None, None]:
    """Async context manager to time database operations."""
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


class AsyncConnectionPool:
    """Async connection pool wrapper for concurrent operations."""
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 5):
        self.db_config = db_config
        self.pool_size = pool_size
        self.pool: Optional[asyncpg.Pool] = None
    
    async def __aenter__(self) -> 'AsyncConnectionPool':
        """Create and return the connection pool."""
        self.pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            min_size=1,
            max_size=self.pool_size
        )
        return self
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    @asynccontextmanager
    async def acquire(self) -> asyncpg.Connection:
        """Acquire a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        async with self.pool.acquire() as connection:
            yield connection


def get_connection_pool(db_config: Dict[str, Any], pool_size: int = 5) -> AsyncConnectionPool:
    """Create an async connection pool for concurrent operations."""
    return AsyncConnectionPool(db_config, pool_size)


# Legacy sync interface for compatibility (will be removed after refactoring)
class DatabaseConnection:
    """Synchronous wrapper around async database connection for backward compatibility."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.async_conn = AsyncDatabaseConnection(host, port, database, user, password)
        self._loop = None
    
    def __enter__(self) -> 'DatabaseConnection':
        """Enter context manager and establish connection."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.async_conn.__aenter__())
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close connection."""
        if self._loop:
            self._loop.run_until_complete(self.async_conn.__aexit__(exc_type, exc_val, exc_tb))
            self._loop.close()
            self._loop = None
    
    def execute(self, query: str, params: Optional[tuple] = None):
        """Execute a query and return a cursor-like object."""
        if not self._loop:
            raise RuntimeError("Connection not established")
        
        # Convert params to individual arguments for asyncpg
        args = params if params else ()
        result = self._loop.run_until_complete(self.async_conn.execute(query, *args))
        
        # Return a cursor-like object for compatibility
        return SyncCursor(result)
    
    def execute_many(self, query: str, params_list: list) -> None:
        """Execute a query multiple times with different parameters."""
        if not self._loop:
            raise RuntimeError("Connection not established")
        
        self._loop.run_until_complete(self.async_conn.execute_many(query, params_list))
    
    def commit(self) -> None:
        """Commit is handled automatically by asyncpg."""
        pass
    
    def rollback(self) -> None:
        """Rollback is handled by transaction context managers."""
        pass
    
    def transaction(self):
        """Return a transaction context manager."""
        return SyncTransaction(self.async_conn, self._loop)
    
    def get_table_info(self, schema: str = 'benchmark') -> Dict[str, Dict[str, Any]]:
        """Get information about tables in the specified schema."""
        if not self._loop:
            raise RuntimeError("Connection not established")
        
        return self._loop.run_until_complete(self.async_conn.get_table_info(schema))


class SyncCursor:
    """Cursor-like object for backward compatibility with psycopg."""
    
    def __init__(self, records: List[asyncpg.Record]):
        self.records = records
        self._index = 0
    
    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all records as dictionaries."""
        return [dict(record) for record in self.records]
    
    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch one record as a dictionary."""
        if self._index < len(self.records):
            record = dict(self.records[self._index])
            self._index += 1
            return record
        return None
    
    def __iter__(self):
        """Make cursor iterable."""
        return iter(dict(record) for record in self.records)


class SyncTransaction:
    """Transaction context manager for backward compatibility."""
    
    def __init__(self, async_conn: AsyncDatabaseConnection, loop: asyncio.AbstractEventLoop):
        self.async_conn = async_conn
        self.loop = loop
        self._transaction = None
    
    def __enter__(self):
        """Start a transaction."""
        if not self.async_conn.connection:
            raise RuntimeError("Connection not established")
        
        # Start transaction
        self._transaction = self.async_conn.connection.transaction()
        self.loop.run_until_complete(self._transaction.__aenter__())
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        """End transaction."""
        if self._transaction:
            self.loop.run_until_complete(self._transaction.__aexit__(exc_type, exc_val, exc_tb))
            self._transaction = None