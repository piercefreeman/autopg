# autopgpool

`autopgpool` is a Postgres pooler with opinionated default configurations.

Unlike `autopg`, which is guaranteed to wrap standard Postgres with auto-configuration useful on any device, `autopgpool` is more geared to users that are self hosting postgres and want a lightweight pooling layer out of the box.

It's currently a wrapper on top of the battle hardened [pgbouncer](https://www.pgbouncer.org/), but this is an implementation detail that could change in the future.

## Features

- toml configurable with a single deployment file (mounted via a docker volume typically)
- simple user based access grants to different tables
- automatic md5 calculation of user passwords
- environment variable insertion to let your docker container remain the source of truth for configuration variables

## Basic configuration

You'll minimally need to provide definitions for the remote databases that you want to route into, and the users that you'll use to connect to the pool. We will expand any env variables you include to their current values:

```toml
[[users]]
username = "app_user"
password = "$APP_CLIENT_PASSWORD"
grants = ["main_db"]

[pools.main_db.remote]
host = "127.0.0.1"
port = "5056"
database = "main_db"
username = "main_user"
password = "$MAIN_DB_PASSWORD"
```

For a more complete example config, see config.example.toml. To reference this in docker-compose, do something like the following:

```bash
version: '3'

services:
  pgpool:
    image: ghcr.io/piercefreeman/autopg-pool:latest
    ports:
      - "6432:6432"
    environment:
      - APP_CLIENT_PASSWORD=myapppassword
      - MAIN_DB_PASSWORD=mymaindbpassword
    volumes:
      - ./config.toml:/etc/autopgpool/autopgpool.toml
    restart: unless-stopped
```

### autopgpool vs vanilla pgbouncer

Vanilla pgbouncer requires configuration through `pgbouncer.ini` and `userlist.txt` files that are placed on disk. This works fine for static configuration, but requires you to hard-code in any env variables (and to pre-calculate the md5 hash of your user credentials before deployment).
