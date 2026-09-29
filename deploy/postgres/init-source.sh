#!/bin/sh
set -eu

source_password=$(cat /run/secrets/source_runtime_password)
capture_password=$(cat /run/secrets/source_capture_password)

psql --set=ON_ERROR_STOP=1 --set=source_password="$source_password" --set=capture_password="$capture_password" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE example_app LOGIN PASSWORD :'source_password';
CREATE ROLE updatis_capture LOGIN REPLICATION PASSWORD :'capture_password';
CREATE SCHEMA example AUTHORIZATION example_app;
CREATE TABLE example.orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered')),
    amount numeric(20,4),
    binary_value bytea,
    local_time time(6),
    zoned_time timetz(6),
    local_stamp timestamp(6),
    instant timestamptz(6),
    payload jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE example.customers (
    id uuid PRIMARY KEY,
    display_name text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE example.orders OWNER TO example_app;
ALTER TABLE example.customers OWNER TO example_app;
GRANT CONNECT ON DATABASE example_source TO example_app;
GRANT CONNECT ON DATABASE example_source TO updatis_capture;
GRANT USAGE ON SCHEMA example TO updatis_capture;
GRANT SELECT ON TABLE example.orders, example.customers TO updatis_capture;
GRANT CREATE ON DATABASE example_source TO example_app;
SET ROLE example_app;
CREATE PUBLICATION updatis_pub_00000000000000000000000000000001_000000000101
    FOR TABLE example.orders, example.customers WITH (publish = 'insert,update,delete');
RESET ROLE;
SQL
