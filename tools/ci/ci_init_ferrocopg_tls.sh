#!/bin/sh

set -eu

psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -c "CREATE ROLE certuser LOGIN"

hba_rule="hostssl all certuser 0.0.0.0/0 cert"
sed -i "1i${hba_rule}" "$PGDATA/pg_hba.conf"
