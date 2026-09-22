# v0.1-a acceptance record

This record covers roadmap issues 01A–01C only.

Local verification on CPython 3.13.15 completed with 68 passing tests and the
four unchanged strict expected failures that document legacy defects. The suite
covered strict configuration versions and bounds, unsafe URLs and changes,
secret resolution and safe errors, source/metadata identity checks, redaction,
packaged-wheel execution outside the repository, entry-point separation, and
the migration contract. `pip check`, Python bytecode compilation, JSON parsing,
and the working-tree whitespace check also passed.

`docker compose config` succeeded through the integration verifier. The local
Docker daemon was unavailable, so the real PostgreSQL migration, image health,
restart, listener, role-isolation, empty-connector, and no-publication/slot
assertions were not executed locally. The `v01a-compose` CI job runs those
assertions on a Docker-enabled runner. This record must not label that evidence
passed until the job succeeds.

Passing these checks does not demonstrate CDC, ingestion, delivery, retries,
DLQ transitions, ordering behavior, pipeline management, or any v0.1-b feature.

## 01B marker privilege correction

The first hosted `v01a-compose` run exposed a privilege-boundary defect: the
migration role had the intended `SELECT` grant on the immutable metadata marker,
but the migrator queried it with `FOR UPDATE`, which also requires write-level
permission. The marker lookup is now read-only. Both migration and runtime roles
receive only `USAGE` on `updatis_bootstrap` and `SELECT` on
`metadata_instance`; `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` are explicitly
revoked. The bootstrap role retains marker ownership.

Migration serialization uses `pg_advisory_xact_lock` on the caller-owned outer
transaction. That exact connection is supplied to Alembic, whose environment is
forbidden from creating another connection and uses one transaction for the
complete upgrade.

After this correction, the local CPython 3.13.15 suite passed with **71 tests and
4 unchanged strict expected failures**. It includes focused checks for the
read-only marker query, explicit grants/revokes, and advisory-lock connection
lifetime. Bytecode compilation and the working-tree whitespace check passed.

The expanded Compose verifier now asserts both scoped roles can read the marker,
neither can mutate it, the migrator can apply the schema, runtime can read the
Alembic revision without creating tables, and an unmarked database is rejected.
The verifier's Compose configuration step passed locally, but the Docker daemon
was unavailable before containers could start. Those live assertions therefore
remain pending a hosted `v01a-compose` rerun.
