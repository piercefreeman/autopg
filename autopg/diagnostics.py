"""PostgreSQL diagnostics models and controller for performance analysis."""

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class IndexUsageLevel(StrEnum):
    """Index usage severity levels."""

    CRITICAL = "critical"  # < 10% index usage
    WARNING = "warning"  # 10-50% index usage
    OK = "ok"  # > 50% index usage


class TableScanStats(BaseModel):
    """Statistics about sequential and index scans for a table."""

    model_config = {"populate_by_name": True}

    schema_name: str = Field(alias="schemaname")
    table_name: str = Field(alias="relname")
    seq_scan_count: int = Field(alias="seq_scan", description="Number of sequential scans")
    seq_rows_read: int = Field(alias="seq_tup_read", description="Total rows read via seq scans")
    idx_scan_count: int = Field(alias="idx_scan", description="Number of index scans")
    idx_rows_fetched: int = Field(alias="idx_tup_fetch", description="Rows fetched via index")
    index_usage_percentage: float = Field(description="Percentage of queries using indexes")
    table_size: str = Field(description="Human-readable table size")
    severity: IndexUsageLevel = Field(description="Severity level based on index usage")

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "TableScanStats":
        """Create from database row result."""
        total_scans = row["seq_scan"] + row["idx_scan"]
        if total_scans == 0:
            index_usage = 0.0
        else:
            index_usage = round(100.0 * row["idx_scan"] / total_scans, 2)

        # Determine severity
        if index_usage < 10:
            severity = IndexUsageLevel.CRITICAL
        elif index_usage < 50:
            severity = IndexUsageLevel.WARNING
        else:
            severity = IndexUsageLevel.OK

        return cls(
            schemaname=row.get("schemaname", "unknown"),
            relname=row.get("relname", "unknown"),
            seq_scan=row.get("seq_scan", 0),
            seq_tup_read=row.get("seq_tup_read", 0),
            idx_scan=row.get("idx_scan", 0),
            idx_tup_fetch=row.get("idx_tup_fetch", 0),
            index_usage_percentage=index_usage,
            table_size=row.get("table_size", "unknown"),
            severity=severity,
        )


class QueryStats(BaseModel):
    """Statistics for a specific query."""

    query_text: str = Field(description="Truncated query text")
    calls: int = Field(description="Number of times executed")
    total_time_ms: float = Field(description="Total execution time in milliseconds")
    mean_time_ms: float = Field(description="Average execution time in milliseconds")
    max_time_ms: float = Field(description="Maximum execution time in milliseconds")
    rows_returned: int = Field(description="Average rows returned", default=0)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "QueryStats":
        """Create from database row result."""
        return cls(
            query_text=row.get("query_text", ""),
            calls=row.get("calls", 0),
            total_time_ms=float(row.get("total_ms", 0)),
            mean_time_ms=float(row.get("avg_ms", 0)),
            max_time_ms=float(row.get("max_ms", 0)),
            rows_returned=row.get("rows", 0),
        )


class TableIndexInfo(BaseModel):
    """Information about indexes on a table."""

    table_name: str
    index_name: str
    index_def: str
    index_size: str
    is_primary: bool = False
    is_unique: bool = False

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "TableIndexInfo":
        """Create from database row result."""
        index_def = row.get("indexdef", "")
        return cls(
            table_name=row["tablename"],
            index_name=row["indexname"],
            index_def=index_def,
            index_size=row.get("index_size", "0 bytes"),
            is_primary="PRIMARY KEY" in index_def.upper(),
            is_unique="UNIQUE" in index_def.upper(),
        )


class TableDiagnostics(BaseModel):
    """Complete diagnostics for a table."""

    table_name: str
    scan_stats: TableScanStats
    indexes: List[TableIndexInfo]
    problem_queries: List[QueryStats]
    recommendations: List[str]


class ActiveQuery(BaseModel):
    """Currently running query information."""

    pid: int
    duration_seconds: float
    state: str
    wait_event: Optional[str]
    query: str
    application_name: str
    is_blocking: bool = False

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "ActiveQuery":
        """Create from database row result."""
        return cls(
            pid=row["pid"],
            duration_seconds=float(row.get("duration_seconds", 0)),
            state=row.get("state", "unknown"),
            wait_event=row.get("wait_event"),
            query=row.get("query", "")[:500],  # Truncate long queries
            application_name=row.get("application_name", ""),
            is_blocking=row.get("is_blocking", False),
        )


class IndexRecommendation(BaseModel):
    """Recommended index to create."""

    table_name: str
    columns: List[str]
    reason: str
    estimated_improvement: str
    create_statement: str
    priority: IndexUsageLevel  # Reusing for priority levels


class DiagnosticSummary(BaseModel):
    """Overall diagnostic summary."""

    timestamp: datetime
    critical_tables: List[TableScanStats]
    active_problems: List[ActiveQuery]
    recommendations: List[IndexRecommendation]
    total_seq_reads: int
    total_idx_reads: int
    overall_health_score: float  # 0-100


class DiagnosticController:
    """Controller for PostgreSQL diagnostics."""

    def __init__(self, connection_params: dict):
        """Initialize with database connection parameters."""
        self.connection_params = connection_params
        self._conn = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._conn is None:
            import psycopg

            self._conn = psycopg.connect(**self.connection_params)
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_heavy_seq_scan_tables(self, limit: int = 20) -> List[TableScanStats]:
        """Find tables with heavy sequential scans."""
        query = """
        SELECT
            schemaname,
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS table_size
        FROM pg_stat_user_tables
        WHERE seq_tup_read > 1000000  -- Only tables with significant seq reads
        ORDER BY seq_tup_read DESC
        LIMIT %s
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            columns = [desc[0] for desc in cur.description]
            results = []
            for row in cur.fetchall():
                row_dict = dict(zip(columns, row, strict=False))
                results.append(TableScanStats.from_db_row(row_dict))
        return results

    def get_problem_queries(
        self, table_name: Optional[str] = None, limit: int = 10
    ) -> List[QueryStats]:
        """Get problematic queries, optionally filtered by table."""
        # Check if pg_stat_statements extension is enabled
        check_query = (
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')"
        )
        conn = self._get_connection()

        with conn.cursor() as cur:
            cur.execute(check_query)
            if not cur.fetchone()[0]:
                return []  # Extension not available

        query = """
        SELECT
            substring(query, 1, 500) as query_text,
            calls,
            total_exec_time::bigint as total_ms,
            mean_exec_time::bigint as avg_ms,
            max_exec_time::bigint as max_ms,
            rows
        FROM pg_stat_statements
        """

        if table_name:
            query += " WHERE query ILIKE %s"
            query += " ORDER BY total_exec_time DESC LIMIT %s"
            params = (f"%{table_name}%", limit)
        else:
            query += " WHERE mean_exec_time > 100"  # Only slow queries
            query += " ORDER BY total_exec_time DESC LIMIT %s"
            params = (limit,)

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = []
                for row in cur.fetchall():
                    row_dict = dict(zip(columns, row, strict=False))
                    results.append(QueryStats.from_db_row(row_dict))
            return results
        except Exception:
            # pg_stat_statements might not be available
            return []

    def get_table_indexes(self, table_name: str) -> List[TableIndexInfo]:
        """Get indexes for a specific table."""
        # Handle schema-qualified table names
        if "." in table_name:
            schema_name, table_name_only = table_name.split(".", 1)
            where_clause = "WHERE schemaname = %s AND tablename = %s"
            params = (schema_name, table_name_only)
        else:
            where_clause = "WHERE tablename = %s"
            params = (table_name,)

        query = f"""
        SELECT
            tablename,
            indexname,
            indexdef,
            COALESCE(
                (SELECT pg_size_pretty(pg_relation_size(c.oid))
                 FROM pg_class c
                 WHERE c.relname = pg_indexes.indexname
                 AND c.relkind = 'i'),
                'N/A'
            ) as index_size
        FROM pg_indexes
        {where_clause}
        ORDER BY indexname
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            results = []
            for row in cur.fetchall():
                row_dict = dict(zip(columns, row, strict=False))
                results.append(TableIndexInfo.from_db_row(row_dict))
        return results

    def get_active_queries(self, min_duration_seconds: float = 5.0) -> List[ActiveQuery]:
        """Get currently active queries."""
        query = """
        SELECT
            main.pid,
            EXTRACT(EPOCH FROM (now() - main.query_start)) as duration_seconds,
            main.state,
            main.wait_event,
            main.query,
            main.application_name,
            false as is_blocking
        FROM pg_stat_activity main
        WHERE main.state != 'idle'
          AND main.query NOT ILIKE '%%pg_stat_activity%%'
          AND EXTRACT(EPOCH FROM (now() - main.query_start)) > %s
        ORDER BY duration_seconds DESC
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (min_duration_seconds,))
            columns = [desc[0] for desc in cur.description]
            results = []
            for row in cur.fetchall():
                row_dict = dict(zip(columns, row, strict=False))
                results.append(ActiveQuery.from_db_row(row_dict))
        return results

    def analyze_table(self, table_name: str) -> TableDiagnostics:
        """Complete analysis of a specific table."""
        # Handle schema-qualified table names
        if "." in table_name:
            schema_name, table_name_only = table_name.split(".", 1)
            where_clause = "WHERE schemaname = %s AND relname = %s"
            params = (schema_name, table_name_only)
        else:
            where_clause = "WHERE relname = %s"
            params = (table_name,)

        # Get scan stats for this table
        query = f"""
        SELECT
            schemaname,
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS table_size
        FROM pg_stat_user_tables
        {where_clause}
        LIMIT 1
        """

        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Table {table_name} not found")
            row_dict = dict(zip(columns, row, strict=False))
            scan_stats = TableScanStats.from_db_row(row_dict)

        # Get indexes
        indexes = self.get_table_indexes(table_name)

        # Get problem queries
        problem_queries = self.get_problem_queries(table_name, limit=5)

        # Generate recommendations
        recommendations = self._generate_recommendations(scan_stats, indexes, problem_queries)

        return TableDiagnostics(
            table_name=table_name,
            scan_stats=scan_stats,
            indexes=indexes,
            problem_queries=problem_queries,
            recommendations=recommendations,
        )

    def _generate_recommendations(
        self, scan_stats: TableScanStats, indexes: List[TableIndexInfo], queries: List[QueryStats]
    ) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []

        # Check for missing indexes
        if scan_stats.severity == IndexUsageLevel.CRITICAL:
            if not indexes or all(idx.is_primary for idx in indexes):
                recommendations.append(
                    f"Table {scan_stats.table_name} has {scan_stats.seq_rows_read:,} sequential reads "
                    f"but no secondary indexes. Analyze query patterns to determine needed indexes."
                )
            else:
                recommendations.append(
                    f"Table {scan_stats.table_name} has very low index usage ({scan_stats.index_usage_percentage}%). "
                    f"Review existing indexes for effectiveness."
                )

        # Check for slow queries
        if queries:
            slow_queries = [q for q in queries if q.mean_time_ms > 1000]
            if slow_queries:
                recommendations.append(
                    f"Found {len(slow_queries)} queries averaging over 1 second. "
                    f"Consider query optimization or additional indexes."
                )

        # Check table size vs scan frequency
        if scan_stats.seq_scan_count > 1000000 and "MB" in scan_stats.table_size:
            size_mb = float(scan_stats.table_size.split()[0])
            if size_mb < 100:
                recommendations.append(
                    f"Small table ({scan_stats.table_size}) with {scan_stats.seq_scan_count:,} scans. "
                    f"This table is a prime candidate for complete caching or index optimization."
                )

        return recommendations

    def get_diagnostic_summary(self) -> DiagnosticSummary:
        """Get overall diagnostic summary."""
        # Get critical tables
        heavy_tables = self.get_heavy_seq_scan_tables(limit=10)
        critical_tables = [t for t in heavy_tables if t.severity == IndexUsageLevel.CRITICAL]

        # Get active problems
        active_queries = self.get_active_queries(min_duration_seconds=10)

        # Calculate totals
        total_seq = sum(t.seq_rows_read for t in heavy_tables)
        total_idx = sum(t.idx_rows_fetched for t in heavy_tables)

        # Calculate health score (simple heuristic)
        if total_seq + total_idx > 0:
            idx_ratio = total_idx / (total_seq + total_idx)
            health_score = min(100, idx_ratio * 100)
        else:
            health_score = 100.0

        # Adjust for active problems
        if active_queries:
            health_score *= 0.8  # 20% penalty for long-running queries
        if critical_tables:
            health_score *= 1 - 0.1 * min(len(critical_tables), 5)  # Up to 50% penalty

        # Generate recommendations
        recommendations = []
        for table in critical_tables[:3]:  # Top 3 critical tables
            rec = IndexRecommendation(
                table_name=table.table_name,
                columns=["TODO: Analyze query patterns"],  # Would need query analysis
                reason=f"Table has {table.seq_rows_read:,} seq reads with {table.index_usage_percentage}% index usage",
                estimated_improvement=f"Could reduce seq scans by up to {100 - table.index_usage_percentage}%",
                create_statement=f"-- Analyze queries on {table.table_name} to determine optimal index columns",
                priority=IndexUsageLevel.CRITICAL,
            )
            recommendations.append(rec)

        return DiagnosticSummary(
            timestamp=datetime.now(),
            critical_tables=critical_tables,
            active_problems=active_queries,
            recommendations=recommendations,
            total_seq_reads=total_seq,
            total_idx_reads=total_idx,
            overall_health_score=health_score,
        )
