# autopg

Auto-optimizations for postgres

## Algorithm

The algorithm is a direct Python conversion from [pgtune](https://pgtune.leopard.in.ua/). If you notice any discrepancies in output from the two tools, please report them to Issues (or better yet - add a test case).

## Getting Started

```bash
uv sync
```

```bash
uv run autopg
```

To test the docker build pipeline locally, run:

```bash
docker build -t autopg .
```

```bash
docker run -e POSTGRES_USER=test_user -e POSTGRES_PASSWORD=test_password autopg
```
