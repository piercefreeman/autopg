# autopg

Auto-optimizations for postgres. This is a proof-of-concept Docker image to automatically optimize the PostgreSQL configuration depending on the host device.

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
docker build --build-arg POSTGRES_VERSION=16 -t autopg .
```

```bash
docker run -e POSTGRES_USER=test_user -e POSTGRES_PASSWORD=test_password autopg
```

## Limitations

- Right now we write the optimization logic in Python so our postgres container relies on having a python interpreter installed. This adds a bit to space overhead and is potentially a security risk. We'd rather bundle a compiled binary that serves the same purpose.
