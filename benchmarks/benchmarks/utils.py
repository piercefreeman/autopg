"""
Utility functions for benchmarking operations.
"""

import statistics
import time
from typing import Any, Dict, Generator, List, Optional, Union


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 0.001:
        return f"{seconds * 1000000:.1f}μs"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def format_number(num: Union[int, float]) -> str:
    """Format large numbers with appropriate suffixes."""
    if isinstance(num, float) and num < 1:
        return f"{num:.3f}"

    num = int(num) if isinstance(num, float) else num

    if num < 1000:
        return str(num)
    elif num < 1000000:
        return f"{num / 1000:.1f}K"
    elif num < 1000000000:
        return f"{num / 1000000:.1f}M"
    else:
        return f"{num / 1000000000:.1f}B"


def calculate_statistics(values: List[float]) -> Dict[str, float]:
    """Calculate statistical metrics from a list of values."""
    if not values:
        return {}

    sorted_values = sorted(values)

    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "std_dev": statistics.stdev(values) if len(values) > 1 else 0,
        "p95": sorted_values[int(0.95 * len(sorted_values))],
        "p99": sorted_values[int(0.99 * len(sorted_values))],
    }


def chunks(lst: List[Any], n: int) -> Generator[List[Any], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class Timer:
    """Simple timer class for measuring execution time."""

    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self) -> None:
        """Start the timer."""
        self.start_time = time.time()

    def stop(self) -> float:
        """Stop the timer and return elapsed time."""
        self.end_time = time.time()
        if self.start_time is None:
            raise RuntimeError("Timer was not started")
        return self.end_time - self.start_time

    def elapsed(self) -> float:
        """Get elapsed time without stopping the timer."""
        if self.start_time is None:
            raise RuntimeError("Timer was not started")
        return time.time() - self.start_time

    def __enter__(self):
        self.start()
        return self

    def __exit__(
        self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]
    ) -> None:
        self.stop()


def generate_random_string(length: int = 10) -> str:
    """Generate a random string of specified length."""
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def generate_random_email() -> str:
    """Generate a random email address."""
    import random

    domains = ["example.com", "test.org", "demo.net", "sample.co"]
    username = generate_random_string(8)
    domain = random.choice(domains)
    return f"{username}@{domain}"


def generate_random_text(min_words: int = 5, max_words: int = 50) -> str:
    """Generate random text with specified word count range."""
    import random

    words = [
        "lorem",
        "ipsum",
        "dolor",
        "sit",
        "amet",
        "consectetur",
        "adipiscing",
        "elit",
        "sed",
        "do",
        "eiusmod",
        "tempor",
        "incididunt",
        "ut",
        "labore",
        "et",
        "dolore",
        "magna",
        "aliqua",
        "enim",
        "ad",
        "minim",
        "veniam",
        "quis",
        "nostrud",
        "exercitation",
        "ullamco",
        "laboris",
        "nisi",
        "aliquip",
        "ex",
        "ea",
        "commodo",
        "consequat",
        "duis",
        "aute",
        "irure",
        "in",
        "reprehenderit",
        "voluptate",
        "velit",
        "esse",
        "cillum",
        "fugiat",
        "nulla",
        "pariatur",
        "excepteur",
        "sint",
        "occaecat",
        "cupidatat",
        "non",
        "proident",
        "sunt",
        "culpa",
        "qui",
        "officia",
        "deserunt",
        "mollit",
        "anim",
        "id",
        "est",
        "laborum",
    ]

    word_count = random.randint(min_words, max_words)
    selected_words = random.choices(words, k=word_count)

    # Capitalize first word
    if selected_words:
        selected_words[0] = selected_words[0].capitalize()

    return " ".join(selected_words) + "."


def create_progress_callback(total: int, description: str = "Processing"):
    """Create a progress callback function for Rich progress bars."""
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
    )

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=None,
        transient=True,
    )

    task_id = progress.add_task(description, total=total)

    def callback(completed: int):
        progress.update(task_id, completed=completed)

    return progress, callback


def batch_execute_with_progress(
    db: Any, query: str, param_batches: List[List[Any]], description: str = "Executing batches"
) -> List[float]:
    """Execute query batches with progress display and timing."""
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
    )

    batch_times = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        transient=False,
    ) as progress:
        task = progress.add_task(description, total=len(param_batches))

        for i, batch_params in enumerate(param_batches):
            timer = Timer()
            timer.start()

            db.execute_many(query, batch_params)

            batch_time = timer.stop()
            batch_times.append(batch_time)

            progress.update(task, completed=i + 1)

    return batch_times
