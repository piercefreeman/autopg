# autopgpool

`autopgpool` is a postgres pooler with opinionated default configurations.

Unlike `autopg`, which is guaranteed to wrap standard postgres with auto-configuration useful on any device, `autopgpool` is more geared to users that are self hosting postgres and want a lightweight pooling layer out of the box.

It's currently a wrapper on top of the battle hardened [pgbouncer](https://www.pgbouncer.org/), but this is an implementation detail that could change in the future.

## Configuration

```toml
```

### autopgpool vs vanilla pgbouncer

Vanilla pgbouncer requires configuration through `pgbouncer.ini` and `userlist.txt` files that are placed on disk. This works fine for static configuration, but requires you to hard-code in any env variables (and to pre-calculate the md5 hash of your user credentials before deployment).
