#!/bin/sh
set -e

export PGPASSWORD="$POSTGRES_PASSWORD"
export PGUSER="$POSTGRES_USER"
export PGHOST="$POSTGRES_HOST"
export PGPORT="$POSTGRES_PORT"
export PGDATABASE="$POSTGRES_DB"

psql -tc "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'" \
  | grep -q 1 \
|| createdb

exec "$@"
