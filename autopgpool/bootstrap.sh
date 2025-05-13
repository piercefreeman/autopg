#!/bin/sh

echo "Running Autopg for PgBouncer..."

autopgpool generate

echo "Booting PgBouncer..."

# We need autopgpool to be run as the root user to access our internal binary, but
# we should run the pgbouncer binary as a more constrained user.
exec su -c "$*" postgres
