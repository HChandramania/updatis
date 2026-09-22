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
