#!/bin/bash

echo "Running Autopg..."

autopg build-config --pg-path /etc/postgresql

# Launch the webapp in the background if supported
echo "Launching AutoPG webapp..."
autopg webapp &

echo "Booting PostgreSQL..."

exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
