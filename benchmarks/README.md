# benchmarks

The goal with our autopg `benchmarks` is to provide us an easy entry point to stress test postgres. This should create a large amount of pg_stat_statements and gives us a reference set of data to optimize our auto-analysis pipeline.

```bash
docker compose up
```

Connect to the benchmark container:

```bash
docker compose exec benchmark bash

$ uv run autopg-bench full --scan-iterations 2000000
```
