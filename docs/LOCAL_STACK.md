# v0.1-a local stack

This stack implements roadmap issues 01A–01C only. It provides packaging,
separate API and worker processes, isolated source and metadata PostgreSQL
instances, an explicit metadata migration, and configuration validation. Kafka
and Connect are healthy but unconfigured. There is no connector, CDC ingestion,
webhook delivery, retry processing, operational API, CLI, or dashboard.

## Bootstrap

Create `deploy/secrets/` and place a distinct strong value in each of these
files: `metadata_bootstrap_password`, `metadata_migration_password`,
`metadata_runtime_password`, `source_bootstrap_password`, and
`source_runtime_password`. Do not commit that directory.

From the repository root:

```text
docker compose -f deploy/compose.yml up -d --wait metadata-db source-db kafka connect
docker compose -f deploy/compose.yml --profile tools run --rm migrate
docker compose -f deploy/compose.yml up -d --wait api worker
```

The API binds `0.0.0.0:8000` inside its container and Compose publishes it only
as `127.0.0.1:8000` on the host. Infrastructure listeners are private.

API and worker startup never applies migrations. They require the exact known
schema revision and fail readiness when migration has not completed or the
revision is incompatible. Alembic configuration is constructed programmatically
by `updatis.db.migrate`; its environment and version scripts are package data in
the installed wheel, so the command is independent of the current directory.

Ordinary `stop`, `start`, and `down` preserve data. Destruction is intentionally
separate and explicit:

```text
docker compose -f deploy/compose.yml down --volumes
```

That command permanently deletes the local source, metadata, and Kafka volumes.

`init-source.sh` creates only the example database objects and scoped application
role. It does not configure logical replication, publications, slots, Debezium,
captured-data topics, or a functioning capture path.
