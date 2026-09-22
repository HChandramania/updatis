#!/bin/sh
set -eu

migration_password=$(cat /run/secrets/metadata_migration_password)
runtime_password=$(cat /run/secrets/metadata_runtime_password)

psql --set=ON_ERROR_STOP=1 --set=migration_password="$migration_password" --set=runtime_password="$runtime_password" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE updatis_migration LOGIN PASSWORD :'migration_password';
CREATE ROLE updatis_runtime LOGIN PASSWORD :'runtime_password';
CREATE SCHEMA updatis_bootstrap;
CREATE TABLE updatis_bootstrap.metadata_instance (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    instance_id text NOT NULL UNIQUE
);
INSERT INTO updatis_bootstrap.metadata_instance (singleton, instance_id)
VALUES (true, 'updatis-metadata-local');
GRANT USAGE ON SCHEMA updatis_bootstrap TO updatis_migration, updatis_runtime;
GRANT SELECT ON updatis_bootstrap.metadata_instance TO updatis_migration, updatis_runtime;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON updatis_bootstrap.metadata_instance
    FROM updatis_migration, updatis_runtime;
GRANT CONNECT ON DATABASE updatis_metadata TO updatis_migration, updatis_runtime;
GRANT CREATE ON SCHEMA public TO updatis_migration;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
