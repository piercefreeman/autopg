# autopg

![Terminal](./docs/header.png)

Auto-optimizations for postgres. This is a proof-of-concept Docker image to automatically optimize the PostgreSQL configuration depending on the host device.

## Usage

`autopg` should be a direct replacement for using the `postgres` docker image in your architecture - be that Docker, Kubernetes, etc.

For example, in `docker-compose.yml` file, add the following:

```yaml
services:
  postgres:
    image: ghcr.io/piercefreeman/autopg:pg16-latest
    ports:
      - 5432:5432
```

We build images following `{postgres_version}-{autopg_version}` tags. Use this table to find your desired version:

| Postgres Version | Autopg Version | Tag |
| ---------------- | -------------- | --- |
| 17               | 0.1.0          | autopg:17-0.1.0 |
| 16               | 0.1.0          | autopg:16-0.1.0 |
| 15               | 0.1.0          | autopg:15-0.1.0 |
| 14               | 0.1.0          | autopg:14-0.1.0 |
| 13               | 0.1.0          | autopg:13-0.1.0 |
| 12               | 0.1.0          | autopg:12-0.1.0 |
| 11               | 0.1.0          | autopg:11-0.1.0 |
| 10               | 0.1.0          | autopg:10-0.1.0 |

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
