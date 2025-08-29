"""FastAPI web application for PostgreSQL diagnostics."""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import SqlLexer

from autopg.diagnostics import (
    ActiveQuery,
    DiagnosticController,
    DiagnosticSummary,
    TableIndexInfo,
    TableScanStats,
)


class DatabaseConfig(BaseModel):
    """Database connection configuration."""

    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str = "postgres"
    password: Optional[str] = None

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create config from environment variables."""
        return cls(
            host=os.getenv("AUTOPG_DB_HOST", "localhost"),
            port=int(os.getenv("AUTOPG_DB_PORT", "5432")),
            dbname=os.getenv("AUTOPG_DB_NAME", "postgres"),
            user=os.getenv("AUTOPG_DB_USER", "postgres"),
            password=os.getenv("AUTOPG_DB_PASSWORD"),
        )

    def to_connection_params(self) -> dict:
        """Convert to psycopg connection parameters."""
        params = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
        }
        if self.password:
            params["password"] = self.password
        return params


class CreateIndexRequest(BaseModel):
    """Request to create an index."""

    table_name: str
    columns: List[str]
    unique: bool = False
    concurrent: bool = True


class DiagnosticResponse(BaseModel):
    """Standard response wrapper."""

    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None


class DiagnosticError(Exception):
    """Custom exception for diagnostic operations."""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class TableNotFoundError(DiagnosticError):
    """Exception raised when a table is not found."""

    def __init__(self, table_name: str):
        super().__init__(f"Table '{table_name}' not found", status_code=404)


class HealthCheckResponse(BaseModel):
    """Health check response."""

    status: str
    service: str


class IndexRecommendationResponse(BaseModel):
    """Index recommendation response."""

    table_name: str
    severity: str
    current_index_usage: float
    seq_reads: Optional[int] = None
    existing_indexes: Optional[List[str]] = None
    recommendations: List[str]
    suggested_action: Optional[str] = None
    message: Optional[str] = None


class KillQueryResponse(BaseModel):
    """Kill query response."""

    success: bool
    message: str


class QueryPlanResponse(BaseModel):
    """Query execution plan response."""

    query: str
    plan: dict


class EnhancedTableIndexInfo(BaseModel):
    """Enhanced table index info with HTML formatting."""

    index_name: str
    index_size: str
    index_def: str
    index_def_html: str  # HTML-formatted definition


class EnhancedQueryStats(BaseModel):
    """Enhanced query stats with HTML formatting."""

    query_text: str
    query_text_html: str  # HTML-formatted query
    calls: int
    total_time_ms: float
    mean_time_ms: float
    max_time_ms: float


class EnhancedTableDiagnostics(BaseModel):
    """Enhanced table diagnostics with HTML formatting."""

    table_name: str
    scan_stats: TableScanStats
    indexes: List[EnhancedTableIndexInfo]
    recommendations: List[str]
    problem_queries: List[EnhancedQueryStats]


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global controller instance
controller: Optional[DiagnosticController] = None

# SQL syntax highlighting setup
sql_lexer = SqlLexer()
html_formatter = HtmlFormatter(style="default", cssclass="highlight", nowrap=True, noclasses=False)


def highlight_sql(sql_text: str) -> str:
    """Apply SQL syntax highlighting to a query string.

    Args:
        sql_text: Raw SQL query text

    Returns:
        HTML-formatted SQL with syntax highlighting
    """
    if not sql_text or not sql_text.strip():
        return ""

    # Clean up the SQL text
    cleaned_sql = sql_text.strip()

    # Apply syntax highlighting
    try:
        highlighted = highlight(cleaned_sql, sql_lexer, html_formatter)
        return highlighted
    except Exception as e:
        logger.warning(f"Failed to highlight SQL: {e}")
        # Fallback to escaped HTML
        import html

        return f"<code>{html.escape(cleaned_sql)}</code>"


def format_index_definition(index_def: str) -> str:
    """Format an index definition with syntax highlighting.

    Args:
        index_def: Raw index definition SQL

    Returns:
        HTML-formatted index definition
    """
    return highlight_sql(index_def)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global controller

    # Startup
    db_config = DatabaseConfig.from_env()
    controller = DiagnosticController(db_config.to_connection_params())
    yield

    # Shutdown
    if controller:
        controller.close()


# Create FastAPI app
app = FastAPI(
    title="AutoPG Diagnostics",
    description="PostgreSQL performance diagnostics and optimization",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


# Global exception handlers
@app.exception_handler(DiagnosticError)
async def diagnostic_error_handler(request: Request, exc: DiagnosticError) -> JSONResponse:
    """Handle custom diagnostic errors."""
    logger.error(f"Diagnostic error on {request.url}: {exc.message}", exc_info=True)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """Handle ValueError exceptions (typically invalid input)."""
    logger.warning(f"Value error on {request.url}: {exc}")
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle all other exceptions."""
    logger.error(f"Unhandled exception on {request.url}: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main diagnostics HTML page."""
    html_path = Path(__file__).parent / "static" / "diagnostics.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>AutoPG Diagnostics</h1><p>HTML interface not found.</p>")


@app.get("/api/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check endpoint."""
    return HealthCheckResponse(status="healthy", service="autopg-diagnostics")


@app.get("/api/diagnostics/summary", response_model=DiagnosticSummary)
async def get_diagnostic_summary():
    """Get overall diagnostic summary."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    return controller.get_diagnostic_summary()


@app.get("/api/diagnostics/heavy-scans", response_model=List[TableScanStats])
async def get_heavy_seq_scans(limit: int = Query(default=20, le=100)):
    """Get tables with heavy sequential scans."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    return controller.get_heavy_seq_scan_tables(limit=limit)


@app.get("/api/diagnostics/table/{table_name}", response_model=EnhancedTableDiagnostics)
async def analyze_table(table_name: str):
    """Analyze a specific table for performance issues."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    # Get the original diagnostics
    diagnostics = controller.analyze_table(table_name)

    # Enhance with HTML formatting
    enhanced_indexes = [
        EnhancedTableIndexInfo(
            index_name=idx.index_name,
            index_size=idx.index_size,
            index_def=idx.index_def,
            index_def_html=format_index_definition(idx.index_def),
        )
        for idx in diagnostics.indexes
    ]

    enhanced_queries = [
        EnhancedQueryStats(
            query_text=query.query_text,
            query_text_html=highlight_sql(query.query_text),
            calls=query.calls,
            total_time_ms=query.total_time_ms,
            mean_time_ms=query.mean_time_ms,
            max_time_ms=query.max_time_ms,
        )
        for query in diagnostics.problem_queries
    ]

    return EnhancedTableDiagnostics(
        table_name=diagnostics.table_name,
        scan_stats=diagnostics.scan_stats,
        indexes=enhanced_indexes,
        recommendations=diagnostics.recommendations,
        problem_queries=enhanced_queries,
    )


@app.get("/api/diagnostics/queries", response_model=List[EnhancedQueryStats])
async def get_problem_queries(
    table_name: Optional[str] = Query(default=None), limit: int = Query(default=10, le=100)
):
    """Get problematic queries, optionally filtered by table."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    queries = controller.get_problem_queries(table_name=table_name, limit=limit)

    # Enhance with HTML formatting
    return [
        EnhancedQueryStats(
            query_text=query.query_text,
            query_text_html=highlight_sql(query.query_text),
            calls=query.calls,
            total_time_ms=query.total_time_ms,
            mean_time_ms=query.mean_time_ms,
            max_time_ms=query.max_time_ms,
        )
        for query in queries
    ]


@app.get("/api/diagnostics/active-queries", response_model=List[ActiveQuery])
async def get_active_queries(min_duration: float = Query(default=5.0, ge=0)):
    """Get currently active queries."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    return controller.get_active_queries(min_duration_seconds=min_duration)


@app.get("/api/diagnostics/indexes/{table_name}", response_model=List[TableIndexInfo])
async def get_table_indexes(table_name: str):
    """Get indexes for a specific table."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    return controller.get_table_indexes(table_name)


@app.post(
    "/api/diagnostics/recommend-index/{table_name}", response_model=IndexRecommendationResponse
)
async def recommend_index(table_name: str):
    """Get index recommendation for a table based on query patterns."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    # Analyze the table
    diagnostics = controller.analyze_table(table_name)

    # Generate recommendation based on analysis
    if diagnostics.scan_stats.severity == "critical":
        return IndexRecommendationResponse(
            table_name=table_name,
            severity=diagnostics.scan_stats.severity,
            current_index_usage=diagnostics.scan_stats.index_usage_percentage,
            seq_reads=diagnostics.scan_stats.seq_rows_read,
            existing_indexes=[idx.index_name for idx in diagnostics.indexes],
            recommendations=diagnostics.recommendations,
            suggested_action=(
                "This table needs immediate index optimization. "
                "Analyze your most frequent queries to determine optimal index columns."
            ),
        )
    else:
        return IndexRecommendationResponse(
            table_name=table_name,
            severity=diagnostics.scan_stats.severity,
            current_index_usage=diagnostics.scan_stats.index_usage_percentage,
            message="Table performance is acceptable",
            recommendations=diagnostics.recommendations,
        )


@app.post("/api/diagnostics/kill-query/{pid}", response_model=KillQueryResponse)
async def kill_query(pid: int):
    """Terminate a running query by PID."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    # Execute kill command
    conn = controller._get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
        result = cur.fetchone()[0]

    if result:
        return KillQueryResponse(success=True, message=f"Query with PID {pid} terminated")
    else:
        raise DiagnosticError(f"Query with PID {pid} not found", status_code=404)


@app.get("/api/diagnostics/explain/{table_name}", response_model=QueryPlanResponse)
async def explain_query_plan(table_name: str, query: str = Query(...)):
    """Get query execution plan for analysis."""
    if not controller:
        raise DiagnosticError("Database controller not initialized")

    # Safety check - only allow EXPLAIN
    if not query.strip().upper().startswith("SELECT"):
        raise DiagnosticError("Only SELECT queries can be explained", status_code=400)

    explain_query = f"EXPLAIN (ANALYZE false, BUFFERS true, FORMAT JSON) {query}"

    conn = controller._get_connection()  # type: ignore[reportPrivateUsage]
    with conn.cursor() as cur:
        cur.execute(explain_query)  # type: ignore[reportArgumentType]
        plan = cur.fetchone()[0]

    return QueryPlanResponse(query=query, plan=plan)


def start_webapp():
    """Start the FastAPI webapp if enabled."""
    if os.getenv("AUTOPG_ENABLE_WEBAPP", "false").lower() != "true":
        print("AutoPG webapp is disabled. Set AUTOPG_ENABLE_WEBAPP=true to enable.")
        return

    host = os.getenv("AUTOPG_WEBAPP_HOST", "0.0.0.0")
    port = int(os.getenv("AUTOPG_WEBAPP_PORT", "8000"))

    print(f"Starting AutoPG Diagnostics webapp on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    start_webapp()
