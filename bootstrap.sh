#!/bin/bash

echo "Running Autopg..."

autopg build-config --pg-path /etc/postgresql

echo "Booting PostgreSQL..."

exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
