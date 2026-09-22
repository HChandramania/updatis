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

## 01A readiness verification correction

A subsequent hosted `v01a-compose` run completed migration and the bounded
`docker compose up -d --wait` for API and worker, then received a host-loopback
connection refusal from its single readiness request. Reaching that request
means Compose had started both services and their in-container health checks had
passed at least once. The old verifier did not retain enough evidence to tell a
short host-forwarding delay from a container that exited immediately afterward.

The Compose contract still explicitly runs `updatis.api`, binds Uvicorn to
`0.0.0.0:8000` in the container, publishes only
`127.0.0.1:8000:8000`, mounts the runtime configuration and metadata runtime
secret, and defines API and worker health checks. The verifier now validates the
rendered Compose values, checks both containers remain running and healthy,
checks their mounted configuration and secret files, and confirms schema
readiness before testing the host listener.

Host readiness now uses a 60-second monotonic deadline, one-second request
timeouts, and half-second bounded retries for connection failures and HTTP 503.
It does not treat a fixed delay as readiness. An exited or unhealthy API fails
immediately with its Compose status. Before any failure cleanup, the verifier
prints `docker compose ps -a`, JSON health/status data, API logs, worker logs,
metadata database logs, and Docker inspect state; volume cleanup remains in
`finally` afterward.

After this correction, the complete local CPython 3.13.15 suite passed with
**76 tests and 4 unchanged strict expected failures**. New tests cover initial
connection refusal followed by success, temporary HTTP 503 followed by success,
permanent refusal through the deadline, and immediate exited/unhealthy container
status. The Docker integration verifier and diagnostic path were executed
locally, but the unavailable Docker daemon prevented container startup. The
rendered Compose contract passed before that failure, and diagnostics were
printed before cleanup. Live API/worker and host-port evidence remains pending a
hosted `v01a-compose` rerun.

## 01A canonical-schema and host-publication correction

The next hosted run proved the marker DML boundaries and completed migration,
but the host-loopback readiness poll received connection refusals for its full
60-second deadline. Because `docker compose up --wait`, the API health check,
the worker health check, configuration-mount checks, and canonical revision
queries had already succeeded, this was not evidence that Uvicorn needed a
longer startup allowance. The API was healthy inside its container. Its only
network was the externally isolated metadata network, so the host publication
did not have a normal bridge path. The API now joins both the private metadata
network and a separate API-only bridge; the host mapping remains restricted to
`127.0.0.1:8000:8000`. Metadata PostgreSQL remains only on its private network.

The verifier sequence also mixed intentional negative schema queries into the
same PostgreSQL log stream used for the canonical database. In particular, a
pre-migration API probe used the canonical database, while the unmarked probe
used a second database. The verifier now migrates and validates the canonical
database first. Missing-marker, missing-revision, wrong-revision, and unmarked
cases each use a distinct disposable database. It reasserts the canonical
marker, exact Alembic revision, runtime read access, and complete table set after
all negative probes and immediately before API/worker startup.

The verifier now additionally proves port 8000 accepts a connection inside the
API container before polling the host, and checks `docker compose port api 8000`
reports `127.0.0.1:8000`. Failure diagnostics include the exact rendered API
command, effective published port, full API/worker/metadata logs, and Docker
inspect state with exit code, health, entrypoint, and command before cleanup.

After this correction, the complete local CPython 3.13.15 suite passed with
**78 tests and 4 unchanged strict expected failures**. Focused entrypoint,
readiness, negative-schema isolation, and migration tests passed 14/14. The
Docker integration verifier was run locally; rendered Compose validation and
the expanded diagnostic path succeeded, but the unavailable local Docker daemon
prevented container startup. Live evidence remains pending the next hosted
`v01a-compose` run.
