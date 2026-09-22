#!/bin/sh
set -eu

source_password=$(cat /run/secrets/source_runtime_password)

psql --set=ON_ERROR_STOP=1 --set=source_password="$source_password" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE example_app LOGIN PASSWORD :'source_password';
CREATE SCHEMA example AUTHORIZATION example_app;
CREATE TABLE example.orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE example.orders OWNER TO example_app;
GRANT CONNECT ON DATABASE example_source TO example_app;
SQL

# Deliberately no logical-replication role, publication, slot, connector, or topic.
