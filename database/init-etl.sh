#!/bin/bash

set -e

psql \
    -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    <<-EOSQL

CREATE USER ${ETL_DB_USER} WITH PASSWORD '${ETL_DB_PASSWORD}';

CREATE DATABASE ${ETL_DB_NAME} OWNER ${ETL_DB_USER};

EOSQL

psql \
    -v ON_ERROR_STOP=1 \
    --username "$ETL_DB_USER" \
    --dbname "$ETL_DB_NAME" \
    -f /docker-entrypoint-initdb.d/schema.sql
