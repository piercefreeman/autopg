"""
Sequential scan benchmark for load testing database reads on unoptimized tables using asyncpg.
"""

import asyncio
import random
import time
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from .database import AsyncConnectionPool, AsyncDatabaseConnection, timed_operation
from .utils import calculate_statistics, format_duration, format_number

console = Console()


class AsyncSequentialScanBenchmark:
    """Async benchmark for testing sequential scan performance on unoptimized tables."""

    def __init__(self, db_config: Dict[str, Any], verbose: bool = False):
        self.db_config = db_config
        self.verbose = verbose

        # Table-specific scan queries designed to force sequential scans
        self.table_queries = {
            "users": [
                "SELECT * FROM benchmark.users WHERE profile_data::text LIKE '%theme%'",
                "SELECT * FROM benchmark.users WHERE created_at > NOW() - INTERVAL '30 days'",
                "SELECT username, email FROM benchmark.users WHERE status != 'active'",
                "SELECT COUNT(*) FROM benchmark.users WHERE last_login IS NULL",
                "SELECT * FROM benchmark.users WHERE email LIKE '%@example.com'",
            ],
            "posts": [
                "SELECT * FROM benchmark.posts WHERE content ILIKE '%lorem%'",
                "SELECT title, view_count FROM benchmark.posts WHERE view_count > 100",
                "SELECT * FROM benchmark.posts WHERE tags && ARRAY['tech', 'news']",
                "SELECT COUNT(*) FROM benchmark.posts WHERE created_at > NOW() - INTERVAL '7 days'",
                "SELECT * FROM benchmark.posts WHERE metadata::text LIKE '%featured%'",
            ],
            "comments": [
                "SELECT * FROM benchmark.comments WHERE content ILIKE '%dolor%'",
                "SELECT * FROM benchmark.comments WHERE likes > 10",
                "SELECT COUNT(*) FROM benchmark.comments WHERE parent_id IS NOT NULL",
                "SELECT * FROM benchmark.comments WHERE created_at > NOW() - INTERVAL '1 day'",
                "SELECT user_id, COUNT(*) FROM benchmark.comments GROUP BY user_id HAVING COUNT(*) > 5",
            ],
            "events": [
                "SELECT * FROM benchmark.events WHERE event_type = 'login'",
                "SELECT * FROM benchmark.events WHERE event_data::text LIKE '%Chrome%'",
                "SELECT COUNT(*) FROM benchmark.events WHERE created_at > NOW() - INTERVAL '1 hour'",
                "SELECT event_type, COUNT(*) FROM benchmark.events GROUP BY event_type",
                "SELECT * FROM benchmark.events WHERE ip_address::text LIKE '192.168.%'",
            ],
        }

    async def run(
        self,
        table_name: str,
        iterations: int = 10,
        limit: Optional[int] = None,
        num_workers: int = 1,
    ) -> Dict[str, Any]:
        """Run the async sequential scan benchmark."""
        if table_name not in self.table_queries:
            raise ValueError(f"Unsupported table: {table_name}")

        queries = self.table_queries[table_name]

        console.print(
            f"[cyan]Starting async sequential scan benchmark for table '{table_name}'[/cyan]"
        )
        console.print(f"Iterations: {iterations}, Workers: {num_workers}, Limit: {limit or 'None'}")

        # Modify queries with LIMIT if specified
        if limit:
            queries = [f"{query} LIMIT {limit}" for query in queries]

        # Get table info for context
        table_info = await self._get_table_info(table_name)
        console.print(
            f"Table size: {table_info['size']}, Rows: {format_number(table_info['row_count'])}"
        )

        # Run benchmark
        async with timed_operation(
            f"Async sequential scan benchmark ({num_workers} workers)", self.verbose
        ) as timing:
            if num_workers == 1:
                iteration_times, total_rows = await self._run_single_connection(queries, iterations)
            else:
                iteration_times, total_rows = await self._run_multi_connection(
                    queries, iterations, num_workers
                )

        # Calculate results
        total_duration = timing["duration"]
        total_iterations = len(iteration_times)
        avg_iteration_time = sum(iteration_times) / len(iteration_times) if iteration_times else 0
        records_per_second = total_rows / total_duration if total_duration > 0 else 0

        stats = calculate_statistics(iteration_times)

        results = {
            "table_name": table_name,
            "total_duration": total_duration,
            "iterations": total_iterations,
            "avg_iteration_time": avg_iteration_time,
            "records_processed": total_rows,
            "records_per_second": records_per_second,
            "num_workers": num_workers,
            "limit": limit,
            **{f"iteration_{k}": v for k, v in stats.items()},
        }

        # Add min/max/median for compatibility with CLI display
        if stats:
            results.update(
                {"min_time": stats["min"], "max_time": stats["max"], "median_time": stats["median"]}
            )

        return results

    async def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about the table being scanned."""
        async with AsyncDatabaseConnection(**self.db_config) as db:
            table_info = await db.get_table_info()
            return table_info.get(table_name, {"size": "Unknown", "row_count": 0})

    async def _run_single_connection(
        self, queries: List[str], iterations: int
    ) -> tuple[List[float], int]:
        """Run sequential scan benchmark with a single connection."""
        iteration_times = []
        total_rows = 0

        async with AsyncDatabaseConnection(**self.db_config) as db:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                transient=False,
            ) as progress:
                task = progress.add_task("Running sequential scans...", total=iterations)

                for i in range(iterations):
                    # Randomly select a query for this iteration
                    query = random.choice(queries)

                    start_time = time.time()
                    result = await db.execute(query)

                    # Count rows processed
                    rows = len(result)

                    iteration_time = time.time() - start_time
                    iteration_times.append(iteration_time)
                    total_rows += rows

                    if self.verbose:
                        console.print(
                            f"Iteration {i + 1}: {format_duration(iteration_time)}, {format_number(rows)} rows"
                        )

                    progress.update(task, completed=i + 1)

        return iteration_times, total_rows

    async def _run_multi_connection(
        self, queries: List[str], iterations: int, num_workers: int
    ) -> tuple[List[float], int]:
        """Run sequential scan benchmark with multiple connections."""
        iteration_times = []
        total_rows = 0
        completed_iterations = 0

        async with AsyncConnectionPool(self.db_config, num_workers) as pool:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                transient=False,
            ) as progress:
                task = progress.add_task("Running sequential scans...", total=iterations)

                # Create semaphore to limit concurrent operations
                semaphore = asyncio.Semaphore(num_workers)

                async def execute_scan_with_semaphore(
                    query: str, iteration_num: int
                ) -> tuple[float, int]:
                    async with semaphore:
                        return await self._execute_scan(pool, query, iteration_num)

                # Submit all iteration jobs
                tasks = [
                    execute_scan_with_semaphore(random.choice(queries), i)
                    for i in range(iterations)
                ]

                # Collect results as they complete
                for coro in asyncio.as_completed(tasks):
                    iteration_time, rows = await coro
                    iteration_times.append(iteration_time)
                    total_rows += rows
                    completed_iterations += 1

                    if self.verbose:
                        console.print(
                            f"Iteration {completed_iterations}: {format_duration(iteration_time)}, {format_number(rows)} rows"
                        )

                    progress.update(task, completed=completed_iterations)

        return iteration_times, total_rows

    async def _execute_scan(
        self, pool: AsyncConnectionPool, query: str, iteration_num: int
    ) -> tuple[float, int]:
        """Execute a single sequential scan."""
        start_time = time.time()

        async with pool.acquire() as conn:
            result = await conn.fetch(query)  # type: ignore[no-untyped-call]
            rows = len(result)

        iteration_time = time.time() - start_time
        return iteration_time, rows

    async def run_explain_analyze(
        self, table_name: str, sample_queries: int = 3
    ) -> List[Dict[str, Any]]:
        """Run EXPLAIN ANALYZE on sample queries to show execution plans."""
        if table_name not in self.table_queries:
            raise ValueError(f"Unsupported table: {table_name}")

        queries = self.table_queries[table_name]
        sample_queries = min(sample_queries, len(queries))
        selected_queries = random.sample(queries, sample_queries)

        results = []

        async with AsyncDatabaseConnection(**self.db_config) as db:
            for i, query in enumerate(selected_queries):
                console.print(f"\n[yellow]Query {i + 1}: {query}[/yellow]")

                explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"

                try:
                    result = await db.execute_one(explain_query)  # type: ignore[no-untyped-call]
                    explain_data = result[0][0] if result else {}  # type: ignore[index]

                    # Extract key metrics
                    plan = explain_data.get("Plan", {})
                    execution_time = explain_data.get("Execution Time", 0)
                    planning_time = explain_data.get("Planning Time", 0)

                    analysis = {
                        "query": query,
                        "execution_time_ms": execution_time,
                        "planning_time_ms": planning_time,
                        "node_type": plan.get("Node Type", "Unknown"),
                        "total_cost": plan.get("Total Cost", 0),
                        "rows": plan.get("Actual Rows", 0),
                        "shared_hit_blocks": plan.get("Shared Hit Blocks", 0),
                        "shared_read_blocks": plan.get("Shared Read Blocks", 0),
                        "full_explain": explain_data,
                    }

                    results.append(analysis)

                    # Display summary
                    console.print(f"  Execution Time: {execution_time:.2f}ms")
                    console.print(f"  Planning Time: {planning_time:.2f}ms")
                    console.print(f"  Node Type: {plan.get('Node Type', 'Unknown')}")
                    console.print(f"  Rows: {format_number(plan.get('Actual Rows', 0))}")

                except Exception as e:
                    console.print(f"  Error running EXPLAIN: {e}", style="red")
                    results.append({"query": query, "error": str(e)})

        return results


# Synchronous wrapper for backward compatibility
class SequentialScanBenchmark:
    """Synchronous wrapper around AsyncSequentialScanBenchmark."""

    def __init__(self, db_config: Dict[str, Any], verbose: bool = False):
        self.async_benchmark = AsyncSequentialScanBenchmark(db_config, verbose)

    def run(
        self,
        table_name: str,
        iterations: int = 10,
        limit: Optional[int] = None,
        num_workers: int = 1,
    ) -> Dict[str, Any]:
        """Run the sequential scan benchmark synchronously."""
        return asyncio.run(self.async_benchmark.run(table_name, iterations, limit, num_workers))

    def run_explain_analyze(self, table_name: str, sample_queries: int = 3) -> List[Dict[str, Any]]:
        """Run EXPLAIN ANALYZE synchronously."""
        return asyncio.run(self.async_benchmark.run_explain_analyze(table_name, sample_queries))
