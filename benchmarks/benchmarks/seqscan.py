"""
Sequential scan benchmark for load testing database reads on unoptimized tables.
"""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from .database import DatabaseConnection, get_connection_pool, timed_operation
from .utils import calculate_statistics, format_duration, format_number

console = Console()


class SequentialScanBenchmark:
    """Benchmark for testing sequential scan performance on unoptimized tables."""
    
    def __init__(self, db_config: Dict[str, Any], verbose: bool = False):
        self.db_config = db_config
        self.verbose = verbose
        self.lock = threading.Lock()
        
        # Table-specific scan queries designed to force sequential scans
        self.table_queries = {
            'users': [
                "SELECT * FROM benchmark.users WHERE profile_data::text LIKE '%theme%'",
                "SELECT * FROM benchmark.users WHERE created_at > NOW() - INTERVAL '30 days'",
                "SELECT username, email FROM benchmark.users WHERE status != 'active'",
                "SELECT COUNT(*) FROM benchmark.users WHERE last_login IS NULL",
                "SELECT * FROM benchmark.users WHERE email LIKE '%@example.com'",
            ],
            'posts': [
                "SELECT * FROM benchmark.posts WHERE content ILIKE '%lorem%'",
                "SELECT title, view_count FROM benchmark.posts WHERE view_count > 100",
                "SELECT * FROM benchmark.posts WHERE tags && ARRAY['tech', 'news']",
                "SELECT COUNT(*) FROM benchmark.posts WHERE created_at > NOW() - INTERVAL '7 days'",
                "SELECT * FROM benchmark.posts WHERE metadata::text LIKE '%featured%'",
            ],
            'comments': [
                "SELECT * FROM benchmark.comments WHERE content ILIKE '%dolor%'",
                "SELECT * FROM benchmark.comments WHERE likes > 10",
                "SELECT COUNT(*) FROM benchmark.comments WHERE parent_id IS NOT NULL",
                "SELECT * FROM benchmark.comments WHERE created_at > NOW() - INTERVAL '1 day'",
                "SELECT user_id, COUNT(*) FROM benchmark.comments GROUP BY user_id HAVING COUNT(*) > 5",
            ],
            'events': [
                "SELECT * FROM benchmark.events WHERE event_type = 'login'",
                "SELECT * FROM benchmark.events WHERE event_data::text LIKE '%Chrome%'",
                "SELECT COUNT(*) FROM benchmark.events WHERE created_at > NOW() - INTERVAL '1 hour'",
                "SELECT event_type, COUNT(*) FROM benchmark.events GROUP BY event_type",
                "SELECT * FROM benchmark.events WHERE ip_address::text LIKE '192.168.%'",
            ]
        }
    
    def run(self, table_name: str, iterations: int = 10, limit: Optional[int] = None,
            num_workers: int = 1) -> Dict[str, Any]:
        """Run the sequential scan benchmark."""
        if table_name not in self.table_queries:
            raise ValueError(f"Unsupported table: {table_name}")
        
        queries = self.table_queries[table_name]
        
        console.print(f"[cyan]Starting sequential scan benchmark for table '{table_name}'[/cyan]")
        console.print(f"Iterations: {iterations}, Workers: {num_workers}, Limit: {limit or 'None'}")
        
        # Modify queries with LIMIT if specified
        if limit:
            queries = [f"{query} LIMIT {limit}" for query in queries]
        
        # Get table info for context
        table_info = self._get_table_info(table_name)
        console.print(f"Table size: {table_info['size']}, Rows: {format_number(table_info['row_count'])}")
        
        # Run benchmark
        with timed_operation(f"Sequential scan benchmark ({num_workers} workers)", self.verbose) as timing:
            if num_workers == 1:
                iteration_times, total_rows = self._run_single_threaded(queries, iterations)
            else:
                iteration_times, total_rows = self._run_multi_threaded(queries, iterations, num_workers)
        
        # Calculate results
        total_duration = timing['duration']
        total_iterations = len(iteration_times)
        avg_iteration_time = sum(iteration_times) / len(iteration_times) if iteration_times else 0
        records_per_second = total_rows / total_duration if total_duration > 0 else 0
        
        stats = calculate_statistics(iteration_times)
        
        results = {
            'table_name': table_name,
            'total_duration': total_duration,
            'iterations': total_iterations,
            'avg_iteration_time': avg_iteration_time,
            'records_processed': total_rows,
            'records_per_second': records_per_second,
            'num_workers': num_workers,
            'limit': limit,
            **{f'iteration_{k}': v for k, v in stats.items()}
        }
        
        # Add min/max/median for compatibility with CLI display
        if stats:
            results.update({
                'min_time': stats['min'],
                'max_time': stats['max'],
                'median_time': stats['median']
            })
        
        return results
    
    def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about the table being scanned."""
        with DatabaseConnection(**self.db_config) as db:
            table_info = db.get_table_info()
            return table_info.get(table_name, {'size': 'Unknown', 'row_count': 0})
    
    def _run_single_threaded(self, queries: List[str], iterations: int) -> tuple[List[float], int]:
        """Run sequential scan benchmark in single-threaded mode."""
        iteration_times = []
        total_rows = 0
        
        with DatabaseConnection(**self.db_config) as db:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                transient=False
            ) as progress:
                
                task = progress.add_task("Running sequential scans...", total=iterations)
                
                for i in range(iterations):
                    # Randomly select a query for this iteration
                    query = random.choice(queries)
                    
                    start_time = time.time()
                    result = db.execute(query)
                    
                    # Count rows processed
                    rows = 0
                    for _ in result:
                        rows += 1
                    
                    iteration_time = time.time() - start_time
                    iteration_times.append(iteration_time)
                    total_rows += rows
                    
                    if self.verbose:
                        console.print(f"Iteration {i+1}: {format_duration(iteration_time)}, {format_number(rows)} rows")
                    
                    progress.update(task, completed=i + 1)
        
        return iteration_times, total_rows
    
    def _run_multi_threaded(self, queries: List[str], iterations: int, 
                          num_workers: int) -> tuple[List[float], int]:
        """Run sequential scan benchmark in multi-threaded mode."""
        iteration_times = []
        total_rows = 0
        completed_iterations = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            transient=False
        ) as progress:
            
            task = progress.add_task("Running sequential scans...", total=iterations)
            
            with get_connection_pool(self.db_config, num_workers) as pool:
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    # Submit all iteration jobs
                    future_to_iteration = {
                        executor.submit(self._execute_scan, pool, random.choice(queries), i): i
                        for i in range(iterations)
                    }
                    
                    # Collect results as they complete
                    for future in as_completed(future_to_iteration):
                        iteration_time, rows = future.result()
                        iteration_num = future_to_iteration[future]
                        
                        with self.lock:
                            iteration_times.append(iteration_time)
                            total_rows += rows
                            completed_iterations += 1
                            
                            if self.verbose:
                                console.print(f"Iteration {iteration_num+1}: {format_duration(iteration_time)}, {format_number(rows)} rows")
                            
                            progress.update(task, completed=completed_iterations)
        
        return iteration_times, total_rows
    
    def _execute_scan(self, pool, query: str, iteration_num: int) -> tuple[float, int]:
        """Execute a single sequential scan."""
        start_time = time.time()
        rows = 0
        
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                # Count rows processed
                for _ in cursor:
                    rows += 1
        
        iteration_time = time.time() - start_time
        return iteration_time, rows
    
    def run_explain_analyze(self, table_name: str, sample_queries: int = 3) -> List[Dict[str, Any]]:
        """Run EXPLAIN ANALYZE on sample queries to show execution plans."""
        if table_name not in self.table_queries:
            raise ValueError(f"Unsupported table: {table_name}")
        
        queries = self.table_queries[table_name]
        sample_queries = min(sample_queries, len(queries))
        selected_queries = random.sample(queries, sample_queries)
        
        results = []
        
        with DatabaseConnection(**self.db_config) as db:
            for i, query in enumerate(selected_queries):
                console.print(f"\n[yellow]Query {i+1}: {query}[/yellow]")
                
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                
                try:
                    result = db.execute(explain_query).fetchone()
                    explain_data = result[0][0] if result else {}
                    
                    # Extract key metrics
                    plan = explain_data.get('Plan', {})
                    execution_time = explain_data.get('Execution Time', 0)
                    planning_time = explain_data.get('Planning Time', 0)
                    
                    analysis = {
                        'query': query,
                        'execution_time_ms': execution_time,
                        'planning_time_ms': planning_time,
                        'node_type': plan.get('Node Type', 'Unknown'),
                        'total_cost': plan.get('Total Cost', 0),
                        'rows': plan.get('Actual Rows', 0),
                        'shared_hit_blocks': plan.get('Shared Hit Blocks', 0),
                        'shared_read_blocks': plan.get('Shared Read Blocks', 0),
                        'full_explain': explain_data
                    }
                    
                    results.append(analysis)
                    
                    # Display summary
                    console.print(f"  Execution Time: {execution_time:.2f}ms")
                    console.print(f"  Planning Time: {planning_time:.2f}ms")
                    console.print(f"  Node Type: {plan.get('Node Type', 'Unknown')}")
                    console.print(f"  Rows: {format_number(plan.get('Actual Rows', 0))}")
                    
                except Exception as e:
                    console.print(f"  Error running EXPLAIN: {e}", style="red")
                    results.append({
                        'query': query,
                        'error': str(e)
                    })
        
        return results
