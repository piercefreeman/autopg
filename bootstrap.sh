#!/bin/bash

echo "Hello World"

exec docker-entrypoint.sh postgres
