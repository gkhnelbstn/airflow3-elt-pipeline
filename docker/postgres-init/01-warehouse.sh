#!/bin/bash
# Runs once, when the pgdata volume is created: a separate role and database for
# the warehouse, next to Airflow's own metadata database.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v pw="$WAREHOUSE_PASSWORD" <<'SQL'
create role warehouse login password :'pw';
create database warehouse owner warehouse;
SQL
