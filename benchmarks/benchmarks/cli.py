#!/usr/bin/env python3
"""
Benchmarking CLI for AutoPG - Load testing PostgreSQL with unoptimized queries using asyncpg.
"""

import asyncio
import os
import sys
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .database import AsyncDatabaseConnection
from .insertion import InsertionBenchmark
from .seqscan import SequentialScanBenchmark
from .utils import format_duration, format_number

console = Console()


@click.group()
@click.option(
    "--host", default=lambda: os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host"
)
@click.option(
    "--port", default=lambda: int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port"
)
@click.option(
    "--database",
    default=lambda: os.getenv("POSTGRES_DB", "benchmark"),
    help="PostgreSQL database name",
)
@click.option(
    "--user", default=lambda: os.getenv("POSTGRES_USER", "postgres"), help="PostgreSQL username"
)
@click.option(
    "--password",
    default=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"),
    help="PostgreSQL password",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cli(
    ctx: click.Context, host: str, port: int, database: str, user: str, password: str, verbose: bool
) -> None:
    """AutoPG Database Benchmarking Tool - Load test your PostgreSQL instance."""
    ctx.ensure_object(dict)
    ctx.obj["db_config"] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }
    ctx.obj["verbose"] = verbose

    # Test database connection
    async def test_connection():
        try:
            async with AsyncDatabaseConnection(**ctx.obj["db_config"]) as db:
                await db.execute("SELECT 1")
            if verbose:
                console.print(
                    f"✅ Connected to PostgreSQL at {host}:{port}/{database}", style="green"
                )
            return True
        except Exception as e:
            console.print(f"❌ Failed to connect to PostgreSQL: {e}", style="red")
            return False

    # Run the async connection test
    if not asyncio.run(test_connection()):
        sys.exit(1)


@cli.command()
@click.option("--records", "-n", default=10000, help="Number of records to insert")
@click.option("--batch-size", "-b", default=1000, help="Batch size for insertions")
@click.option("--workers", "-w", default=1, help="Number of concurrent workers")
@click.option(
    "--table",
    default="users",
    type=click.Choice(["users", "posts", "comments", "events"]),
    help="Table to insert into",
)
@click.pass_context
def insert(ctx: click.Context, records: int, batch_size: int, workers: int, table: str) -> None:
    """Run insertion load test on unoptimized tables."""
    console.print(
        Panel.fit(
            f"[bold blue]Insertion Benchmark[/bold blue]\n"
            f"Table: {table}\n"
            f"Records: {format_number(records)}\n"
            f"Batch Size: {format_number(batch_size)}\n"
            f"Workers: {workers}",
            title="Configuration",
        )
    )

    benchmark = InsertionBenchmark(ctx.obj["db_config"], verbose=ctx.obj["verbose"])
    results = benchmark.run(
        table_name=table, num_records=records, batch_size=batch_size, num_workers=workers
    )

    _display_results("Insertion Benchmark Results", results)


@cli.command()
@click.option("--iterations", "-i", default=10, help="Number of scan iterations")
@click.option(
    "--table",
    default="posts",
    type=click.Choice(["users", "posts", "comments", "events"]),
    help="Table to scan",
)
@click.option("--limit", default=None, type=int, help="LIMIT clause for scans")
@click.option("--workers", "-w", default=1, help="Number of concurrent workers")
@click.pass_context
def seqscan(
    ctx: click.Context, iterations: int, table: str, limit: Optional[int], workers: int
) -> None:
    """Run sequential scan load test on unoptimized tables."""
    console.print(
        Panel.fit(
            f"[bold blue]Sequential Scan Benchmark[/bold blue]\n"
            f"Table: {table}\n"
            f"Iterations: {iterations}\n"
            f"Limit: {limit or 'None'}\n"
            f"Workers: {workers}",
            title="Configuration",
        )
    )

    benchmark = SequentialScanBenchmark(ctx.obj["db_config"], verbose=ctx.obj["verbose"])
    results = benchmark.run(
        table_name=table, iterations=iterations, limit=limit, num_workers=workers
    )

    _display_results("Sequential Scan Benchmark Results", results)


@cli.command()
@click.option("--insert-records", default=10000, help="Records to insert per table")
@click.option("--scan-iterations", default=5, help="Sequential scan iterations")
@click.option("--workers", "-w", default=2, help="Number of concurrent workers")
@click.pass_context
def full(ctx: click.Context, insert_records: int, scan_iterations: int, workers: int) -> None:
    """Run complete benchmark suite (insert + sequential scans)."""
    console.print(
        Panel.fit(
            f"[bold blue]Full Benchmark Suite[/bold blue]\n"
            f"Insert Records: {format_number(insert_records)}\n"
            f"Scan Iterations: {scan_iterations}\n"
            f"Workers: {workers}",
            title="Configuration",
        )
    )

    all_results = {}

    # Run insertion benchmarks
    console.print("\n[bold yellow]Phase 1: Insertion Benchmarks[/bold yellow]")
    insertion_benchmark = InsertionBenchmark(ctx.obj["db_config"], verbose=ctx.obj["verbose"])

    for table in ["users", "posts", "comments", "events"]:
        console.print(f"\n[cyan]Inserting into {table}...[/cyan]")
        results = insertion_benchmark.run(
            table_name=table, num_records=insert_records, batch_size=1000, num_workers=workers
        )
        all_results[f"insert_{table}"] = results

    # Run sequential scan benchmarks
    console.print("\n[bold yellow]Phase 2: Sequential Scan Benchmarks[/bold yellow]")
    seqscan_benchmark = SequentialScanBenchmark(ctx.obj["db_config"], verbose=ctx.obj["verbose"])

    for table in ["users", "posts", "comments", "events"]:
        console.print(f"\n[cyan]Sequential scanning {table}...[/cyan]")
        results = seqscan_benchmark.run(
            table_name=table, iterations=scan_iterations, limit=None, num_workers=workers
        )
        all_results[f"seqscan_{table}"] = results

    # Display summary
    _display_full_results(all_results)


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show database status and table statistics."""

    async def get_status():
        async with AsyncDatabaseConnection(**ctx.obj["db_config"]) as db:
            # Get table sizes
            table_stats = await db.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    seq_scan,
                    seq_tup_read,
                    idx_scan,
                    idx_tup_fetch
                FROM pg_stat_user_tables 
                WHERE schemaname = 'benchmark'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)

            # Create table
            table = Table(title="Database Table Statistics")
            table.add_column("Table", style="cyan")
            table.add_column("Size", style="magenta")
            table.add_column("Rows", style="green")
            table.add_column("Seq Scans", style="yellow")
            table.add_column("Seq Reads", style="yellow")
            table.add_column("Index Scans", style="blue")
            table.add_column("Index Fetches", style="blue")

            for row in table_stats:
                # Get row count
                count_result = await db.execute_one(
                    f"SELECT COUNT(*) FROM benchmark.{row['tablename']}"
                )
                row_count = format_number(count_result[0]) if count_result else "0"

                table.add_row(
                    row["tablename"],
                    row["size"],
                    row_count,
                    format_number(row["seq_scan"]),
                    format_number(row["seq_tup_read"]),
                    format_number(row["idx_scan"] or 0),
                    format_number(row["idx_tup_fetch"] or 0),
                )

            console.print(table)

    asyncio.run(get_status())


def _display_results(title: str, results: dict) -> None:
    """Display benchmark results in a formatted table."""
    table = Table(title=title)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    # Core metrics
    table.add_row("Total Duration", format_duration(results["total_duration"]))
    table.add_row("Records Processed", format_number(results["records_processed"]))
    table.add_row("Records/Second", format_number(results["records_per_second"]))

    if "batches_processed" in results:
        table.add_row("Batches Processed", format_number(results["batches_processed"]))
        table.add_row("Avg Batch Time", format_duration(results["avg_batch_time"]))

    if "iterations" in results:
        table.add_row("Iterations", format_number(results["iterations"]))
        table.add_row("Avg Iteration Time", format_duration(results["avg_iteration_time"]))

    # Performance metrics
    if "min_time" in results:
        table.add_row("Min Time", format_duration(results["min_time"]))
        table.add_row("Max Time", format_duration(results["max_time"]))
        table.add_row("Median Time", format_duration(results["median_time"]))

    console.print(table)


def _display_full_results(all_results: dict) -> None:
    """Display results from the full benchmark suite."""
    console.print("\n[bold green]📊 Full Benchmark Results Summary[/bold green]")

    # Insertion results
    insert_table = Table(title="Insertion Benchmark Summary")
    insert_table.add_column("Table", style="cyan")
    insert_table.add_column("Records", style="green")
    insert_table.add_column("Duration", style="yellow")
    insert_table.add_column("Records/sec", style="magenta")

    for key, results in all_results.items():
        if key.startswith("insert_"):
            table_name = key.replace("insert_", "")
            insert_table.add_row(
                table_name,
                format_number(results["records_processed"]),
                format_duration(results["total_duration"]),
                format_number(results["records_per_second"]),
            )

    console.print(insert_table)

    # Sequential scan results
    scan_table = Table(title="Sequential Scan Benchmark Summary")
    scan_table.add_column("Table", style="cyan")
    scan_table.add_column("Iterations", style="green")
    scan_table.add_column("Avg Duration", style="yellow")
    scan_table.add_column("Records/sec", style="magenta")

    for key, results in all_results.items():
        if key.startswith("seqscan_"):
            table_name = key.replace("seqscan_", "")
            scan_table.add_row(
                table_name,
                format_number(results["iterations"]),
                format_duration(results["avg_iteration_time"]),
                format_number(results["records_per_second"]),
            )

    console.print(scan_table)


if __name__ == "__main__":
    cli()
