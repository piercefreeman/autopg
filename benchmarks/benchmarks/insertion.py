"""
Insertion benchmark for load testing database writes on unoptimized tables.
"""

import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from uuid import uuid4

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from .database import DatabaseConnection, get_connection_pool, timed_operation
from .utils import calculate_statistics, chunks, format_duration, format_number, generate_random_email, generate_random_string, generate_random_text

console = Console()


class InsertionBenchmark:
    """Benchmark for testing insertion performance on unoptimized tables."""
    
    def __init__(self, db_config: Dict[str, Any], verbose: bool = False):
        self.db_config = db_config
        self.verbose = verbose
        self.lock = threading.Lock()
        
        # Table-specific insert queries and data generators
        self.table_configs = {
            'users': {
                'query': """
                    INSERT INTO benchmark.users (username, email, last_login, status, profile_data)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                'generator': self._generate_user_data
            },
            'posts': {
                'query': """
                    INSERT INTO benchmark.posts (user_id, title, content, updated_at, view_count, tags, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                'generator': self._generate_post_data
            },
            'comments': {
                'query': """
                    INSERT INTO benchmark.comments (post_id, user_id, content, parent_id, likes)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                'generator': self._generate_comment_data
            },
            'events': {
                'query': """
                    INSERT INTO benchmark.events (user_id, event_type, event_data, session_id, ip_address)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                'generator': self._generate_event_data
            }
        }
    
    def run(self, table_name: str, num_records: int, batch_size: int = 1000, 
            num_workers: int = 1) -> Dict[str, Any]:
        """Run the insertion benchmark."""
        if table_name not in self.table_configs:
            raise ValueError(f"Unsupported table: {table_name}")
        
        config = self.table_configs[table_name]
        
        console.print(f"[cyan]Starting insertion benchmark for table '{table_name}'[/cyan]")
        console.print(f"Records: {format_number(num_records)}, Batch size: {format_number(batch_size)}, Workers: {num_workers}")
        
        # Get user/post IDs for foreign key references
        reference_data = self._get_reference_data()
        
        # Generate all data upfront
        console.print("[yellow]Generating test data...[/yellow]")
        with timed_operation("Data generation", self.verbose) as timing:
            all_data = self._generate_batch_data(
                config['generator'], num_records, reference_data
            )
        
        console.print(f"✅ Generated {format_number(len(all_data))} records in {format_duration(timing['duration'])}")
        
        # Split into batches
        batches = list(chunks(all_data, batch_size))
        console.print(f"[yellow]Split into {len(batches)} batches[/yellow]")
        
        # Run benchmark
        with timed_operation(f"Insertion benchmark ({num_workers} workers)", self.verbose) as timing:
            if num_workers == 1:
                batch_times = self._run_single_threaded(config['query'], batches)
            else:
                batch_times = self._run_multi_threaded(config['query'], batches, num_workers)
        
        # Calculate results
        total_duration = timing['duration']
        records_processed = len(all_data)
        records_per_second = records_processed / total_duration if total_duration > 0 else 0
        
        stats = calculate_statistics(batch_times)
        
        results = {
            'table_name': table_name,
            'total_duration': total_duration,
            'records_processed': records_processed,
            'records_per_second': records_per_second,
            'batches_processed': len(batches),
            'avg_batch_time': sum(batch_times) / len(batch_times) if batch_times else 0,
            'batch_size': batch_size,
            'num_workers': num_workers,
            **{f'batch_{k}': v for k, v in stats.items()}
        }
        
        # Add min/max/median for compatibility with CLI display
        if stats:
            results.update({
                'min_time': stats['min'],
                'max_time': stats['max'],
                'median_time': stats['median']
            })
        
        return results
    
    def _get_reference_data(self) -> Dict[str, List[int]]:
        """Get existing IDs for foreign key references."""
        reference_data = {'user_ids': [], 'post_ids': []}
        
        try:
            with DatabaseConnection(**self.db_config) as db:
                # Get user IDs
                user_result = db.execute("SELECT id FROM benchmark.users ORDER BY id LIMIT 1000").fetchall()
                reference_data['user_ids'] = [row['id'] for row in user_result]
                
                # Get post IDs
                post_result = db.execute("SELECT id FROM benchmark.posts ORDER BY id LIMIT 1000").fetchall()
                reference_data['post_ids'] = [row['id'] for row in post_result]
                
        except Exception as e:
            if self.verbose:
                console.print(f"Warning: Could not fetch reference data: {e}", style="yellow")
        
        return reference_data
    
    def _generate_batch_data(self, generator_func, num_records: int, 
                           reference_data: Dict[str, List[int]]) -> List[Tuple]:
        """Generate all data for the benchmark."""
        return [generator_func(reference_data) for _ in range(num_records)]
    
    def _generate_user_data(self, reference_data: Dict[str, List[int]]) -> Tuple:
        """Generate data for users table."""
        username = generate_random_string(12)
        email = generate_random_email()
        last_login = datetime.now() - timedelta(days=random.randint(0, 365))
        status = random.choice(['active', 'inactive', 'pending', 'suspended'])
        profile_data = json.dumps({
            'age': random.randint(18, 80),
            'location': random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR']),
            'preferences': {
                'theme': random.choice(['light', 'dark']),
                'notifications': random.choice([True, False])
            }
        })
        
        return (username, email, last_login, status, profile_data)
    
    def _generate_post_data(self, reference_data: Dict[str, List[int]]) -> Tuple:
        """Generate data for posts table."""
        user_id = random.choice(reference_data['user_ids']) if reference_data['user_ids'] else random.randint(1, 1000)
        title = generate_random_text(3, 8).replace('.', '')  # Remove trailing period for titles
        content = generate_random_text(20, 200)
        updated_at = datetime.now() - timedelta(days=random.randint(0, 30))
        view_count = random.randint(0, 10000)
        tags = [generate_random_string(6) for _ in range(random.randint(1, 5))]
        metadata = json.dumps({
            'category': random.choice(['tech', 'news', 'sports', 'entertainment', 'science']),
            'featured': random.choice([True, False]),
            'word_count': len(content.split())
        })
        
        return (user_id, title, content, updated_at, view_count, tags, metadata)
    
    def _generate_comment_data(self, reference_data: Dict[str, List[int]]) -> Tuple:
        """Generate data for comments table."""
        post_id = random.choice(reference_data['post_ids']) if reference_data['post_ids'] else random.randint(1, 1000)
        user_id = random.choice(reference_data['user_ids']) if reference_data['user_ids'] else random.randint(1, 1000)
        content = generate_random_text(5, 50)
        parent_id = random.randint(1, 100) if random.random() < 0.3 else None  # 30% chance of being a reply
        likes = random.randint(0, 100)
        
        return (post_id, user_id, content, parent_id, likes)
    
    def _generate_event_data(self, reference_data: Dict[str, List[int]]) -> Tuple:
        """Generate data for events table."""
        user_id = random.choice(reference_data['user_ids']) if reference_data['user_ids'] else random.randint(1, 1000)
        event_type = random.choice(['login', 'logout', 'view_post', 'create_post', 'like', 'comment', 'share'])
        event_data = json.dumps({
            'timestamp': datetime.now().isoformat(),
            'user_agent': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'referrer': random.choice(['google.com', 'facebook.com', 'twitter.com', 'direct']),
            'page': f"/page/{random.randint(1, 1000)}"
        })
        session_id = str(uuid4())
        ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        
        return (user_id, event_type, event_data, session_id, ip_address)
    
    def _run_single_threaded(self, query: str, batches: List[List[Tuple]]) -> List[float]:
        """Run insertion benchmark in single-threaded mode."""
        batch_times = []
        
        with DatabaseConnection(**self.db_config) as db:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                transient=False
            ) as progress:
                
                task = progress.add_task("Inserting batches...", total=len(batches))
                
                for i, batch in enumerate(batches):
                    start_time = time.time()
                    
                    with db.transaction():
                        db.execute_many(query, batch)
                    
                    batch_time = time.time() - start_time
                    batch_times.append(batch_time)
                    
                    progress.update(task, completed=i + 1)
        
        return batch_times
    
    def _run_multi_threaded(self, query: str, batches: List[List[Tuple]], 
                          num_workers: int) -> List[float]:
        """Run insertion benchmark in multi-threaded mode."""
        batch_times = []
        completed_batches = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            transient=False
        ) as progress:
            
            task = progress.add_task("Inserting batches...", total=len(batches))
            
            with get_connection_pool(self.db_config, num_workers) as pool:
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    # Submit all batch jobs
                    future_to_batch = {
                        executor.submit(self._execute_batch, pool, query, batch): i
                        for i, batch in enumerate(batches)
                    }
                    
                    # Collect results as they complete
                    for future in as_completed(future_to_batch):
                        batch_time = future.result()
                        
                        with self.lock:
                            batch_times.append(batch_time)
                            completed_batches += 1
                            progress.update(task, completed=completed_batches)
        
        return batch_times
    
    def _execute_batch(self, pool, query: str, batch: List[Tuple]) -> float:
        """Execute a single batch of insertions."""
        start_time = time.time()
        
        with pool.connection() as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(query, batch)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        
        return time.time() - start_time
