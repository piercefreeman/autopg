"""FastAPI web application for PostgreSQL diagnostics."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from autopg.diagnostics import (
    ActiveQuery,
    DiagnosticController,
    DiagnosticSummary,
    IndexRecommendation,
    QueryStats,
    TableDiagnostics,
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


# Global controller instance
controller: Optional[DiagnosticController] = None


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
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main diagnostics HTML page."""
    html_path = Path(__file__).parent / "static" / "diagnostics.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>AutoPG Diagnostics</h1><p>HTML interface not found.</p>")


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "autopg-diagnostics"}


@app.get("/api/diagnostics/summary", response_model=DiagnosticSummary)
async def get_diagnostic_summary():
    """Get overall diagnostic summary."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        summary = controller.get_diagnostic_summary()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/heavy-scans", response_model=List[TableScanStats])
async def get_heavy_seq_scans(limit: int = Query(default=20, le=100)):
    """Get tables with heavy sequential scans."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        results = controller.get_heavy_seq_scan_tables(limit=limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/table/{table_name}", response_model=TableDiagnostics)
async def analyze_table(table_name: str):
    """Analyze a specific table for performance issues."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        diagnostics = controller.analyze_table(table_name)
        return diagnostics
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/queries", response_model=List[QueryStats])
async def get_problem_queries(
    table_name: Optional[str] = Query(default=None),
    limit: int = Query(default=10, le=100)
):
    """Get problematic queries, optionally filtered by table."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        queries = controller.get_problem_queries(table_name=table_name, limit=limit)
        return queries
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/active-queries", response_model=List[ActiveQuery])
async def get_active_queries(min_duration: float = Query(default=5.0, ge=0)):
    """Get currently active queries."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        queries = controller.get_active_queries(min_duration_seconds=min_duration)
        return queries
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/indexes/{table_name}", response_model=List[TableIndexInfo])
async def get_table_indexes(table_name: str):
    """Get indexes for a specific table."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        indexes = controller.get_table_indexes(table_name)
        return indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/diagnostics/recommend-index/{table_name}")
async def recommend_index(table_name: str):
    """Get index recommendation for a table based on query patterns."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        # Analyze the table
        diagnostics = controller.analyze_table(table_name)
        
        # Generate recommendation based on analysis
        if diagnostics.scan_stats.severity == "critical":
            recommendation = {
                "table_name": table_name,
                "severity": diagnostics.scan_stats.severity,
                "current_index_usage": diagnostics.scan_stats.index_usage_percentage,
                "seq_reads": diagnostics.scan_stats.seq_rows_read,
                "existing_indexes": [idx.index_name for idx in diagnostics.indexes],
                "recommendations": diagnostics.recommendations,
                "suggested_action": (
                    "This table needs immediate index optimization. "
                    "Analyze your most frequent queries to determine optimal index columns."
                )
            }
        else:
            recommendation = {
                "table_name": table_name,
                "severity": diagnostics.scan_stats.severity,
                "current_index_usage": diagnostics.scan_stats.index_usage_percentage,
                "message": "Table performance is acceptable",
                "recommendations": diagnostics.recommendations
            }
            
        return recommendation
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/diagnostics/kill-query/{pid}")
async def kill_query(pid: int):
    """Terminate a running query by PID."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        # Execute kill command
        conn = controller._get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            result = cur.fetchone()[0]
            
        if result:
            return {"success": True, "message": f"Query with PID {pid} terminated"}
        else:
            raise HTTPException(status_code=404, detail=f"Query with PID {pid} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/explain/{table_name}")
async def explain_query_plan(table_name: str, query: str = Query(...)):
    """Get query execution plan for analysis."""
    if not controller:
        raise HTTPException(status_code=500, detail="Database controller not initialized")
    
    try:
        # Safety check - only allow EXPLAIN
        if not query.strip().upper().startswith("SELECT"):
            raise HTTPException(status_code=400, detail="Only SELECT queries can be explained")
            
        explain_query = f"EXPLAIN (ANALYZE false, BUFFERS true, FORMAT JSON) {query}"
        
        conn = controller._get_connection()
        with conn.cursor() as cur:
            cur.execute(explain_query)
            plan = cur.fetchone()[0]
            
        return {"query": query, "plan": plan}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def start_webapp():
    """Start the FastAPI webapp if enabled."""
    if os.getenv("AUTOPG_ENABLE_WEBAPP", "false").lower() == "true":
        import uvicorn
        
        host = os.getenv("AUTOPG_WEBAPP_HOST", "0.0.0.0")
        port = int(os.getenv("AUTOPG_WEBAPP_PORT", "8000"))
        
        print(f"Starting AutoPG Diagnostics webapp on {host}:{port}")
        uvicorn.run(app, host=host, port=port)
    else:
        print("AutoPG webapp is disabled. Set AUTOPG_ENABLE_WEBAPP=true to enable.")


if __name__ == "__main__":
    start_webapp()