# autopgpool

`autopgpool` is a Postgres pooler with opinionated default configurations.

Unlike `autopg`, which is guaranteed to wrap standard Postgres with auto-configuration useful on any device, `autopgpool` is more geared to users that are self hosting postgres and want a lightweight pooling layer out of the box.

It's currently a wrapper on top of the battle hardened [pgbouncer](https://www.pgbouncer.org/), but this is an implementation detail that could change in the future.

## Basic configuration

You'll minimally need to provide definitions for the remote databases that you want to route into, and the users that you'll use to connect to the pool. We will expand any env variables you include to their current values:

```toml
[[users]]
username = "app_user"
password = "$APP_CLIENT_PASSWORD"

[pools.main_db.remote]
host = "127.0.0.1"
port = "5056"
database = "main_db"
username = "main_user"
password = "$MAIN_DB_PASSWORD"
```

For a more complete example config, see config.example.toml.

### autopgpool vs vanilla pgbouncer

Vanilla pgbouncer requires configuration through `pgbouncer.ini` and `userlist.txt` files that are placed on disk. This works fine for static configuration, but requires you to hard-code in any env variables (and to pre-calculate the md5 hash of your user credentials before deployment).
