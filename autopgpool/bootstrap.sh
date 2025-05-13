#!/bin/bash

echo "Running Autopg for PgBouncer..."

autopgpool build-config --pg-path /etc/pgbouncer

echo "Booting PgBouncer..."

exec "$@" 