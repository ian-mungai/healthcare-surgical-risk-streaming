#!/bin/sh
# Turns the PostgreSQL password file that Compose mounts into the password file that libpq reads, in memory-backed storage.
# The password is never in the environment, in an argument or in dagster.yaml. The container never runs as root.
set -eu
umask 077
password=$(cat /run/secrets/dagster_postgres_password)
if [ -z "$password" ]; then
    echo "entrypoint: the PostgreSQL password file is empty" >&2
    exit 1
fi
printf '%s' "$password" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g' -e 's/^/*:*:*:dagster:/' > /run/dagster/pgpass
printf '\n' >> /run/dagster/pgpass
exec "$@"
