# Updatis Agent Instructions

## Purpose

Updatis is a PostgreSQL-first change data capture platform. Its first supported
path is:

```text
PostgreSQL source -> Debezium / Kafka Connect -> Kafka -> Python worker -> webhook
```

Updatis owns a separate PostgreSQL metadata store. The metadata database must
never be treated as a captured source.

Read `ROADMAP.md` before planning product work. It defines release boundaries,
phase ordering, dependencies, acceptance intent, current status, and explicit
non-goals. A roadmap item is not implemented merely because it is documented.

## Instruction precedence

Follow instructions in this order:

1. The user's current request.
2. This `AGENTS.md`.
3. The active phase specification and approved design documents.
4. `ROADMAP.md`.
5. Existing architecture, reliability, support, and testing documentation.
6. Existing code conventions.

If two authoritative sources materially conflict, stop before editing and
report the exact conflict and the smallest decision required. Do not silently
choose a new product behavior.

## Required discovery

Before changing code:

1. Confirm the current Git branch and inspect the working tree.
2. Preserve all unrelated and user-owned changes.
3. Read the active phase and its dependencies in `ROADMAP.md`.
4. Read the active phase specification and relevant design/acceptance records.
5. Inspect applicable migrations, contracts, models, repositories, tests,
   Compose services, and CI jobs.
6. State a concise plan containing scope, expected files, invariants, failure
   cases, verification, and explicit non-goals.

Do not repeat broad repository discovery when the relevant paths and contracts
are already identified. Prefer targeted reads and searches.

## Architecture invariants

- PostgreSQL is the only supported source in the initial product path.
- Source PostgreSQL and metadata PostgreSQL use separate instances/services,
  databases, credentials, volumes, network permissions, and lifecycles.
- Metadata tables and migrations exist only in the metadata database.
- Source capture uses Debezium and Kafka; do not replace them without an
  explicitly approved architecture change.
- API and worker processes use one installable Python codebase with separate
  entry points.
- Migrations are executed by a one-shot migration process. API and worker
  startup must not silently migrate the database.
- Runtime containers operate as non-root.
- Secrets are referenced, not stored in exported configuration or logs.
- URLs, endpoints, schema versions, sizes, limits, and state transitions are
  validated conservatively.
- Correctness must survive retries, duplication, restarts, rollback, metadata
  outages, and competing workers where the active phase requires them.
- Durable state, not process memory, is authoritative for correctness.
- Operations, waits, retries, batches, payloads, logs, and diagnostics must be
  explicitly bounded.
- Errors and diagnostics must be actionable, bounded, and redacted.

## Delivery and reliability invariants

- Kafka intake acknowledgement is distinct from destination delivery success.
- Kafka offsets are committed only after the corresponding intake outcome is
  durably recorded.
- Offset commits use explicit topic-partition next offsets.
- Reprocessing the same transport coordinates is idempotent.
- Event identity is derived from immutable captured coordinates, including the
  stream epoch; do not claim semantic deduplication.
- Partition ordering is conservative: later work must not bypass an unresolved
  earlier position.
- Healthy partitions may progress independently when this does not violate an
  ordering lane.
- Destination delivery is at least once. Never claim exactly-once delivery.
- Remote side effects may succeed while acknowledgement is lost; preserve this
  ambiguity rather than fabricating success or failure.
- Lease-protected mutations require both the current owner and fencing token.
- Retry exhaustion must not advance ordering unless the terminal state, DLQ,
  gap, and cursor changes required by the approved phase are committed
  atomically.
- Redrive is never equivalent to restoring the original global order.

## Scope discipline

Implement only the requested phase or issue.

- Do not pull later roadmap behavior into the active phase.
- Do not add speculative abstractions, services, queues, caches, brokers, or
  public APIs.
- Do not perform unrelated refactors or repository-wide formatting.
- Do not broaden supported databases, destinations, data types, or deployment
  claims.
- Do not convert proposed roadmap behavior into documentation that implies it
  already exists.
- Do not change a public contract, durability guarantee, or failure semantic
  without explicit approval and corresponding tests/documentation.

When a prerequisite is missing, implement the smallest compatible prerequisite
or stop for a decision if it would define new product behavior.

## Database and migration rules

- Use forward-only Alembic migrations for schema changes.
- Never edit an already-applied migration.
- Preserve metadata/source isolation.
- Prefer database-enforced uniqueness, referential integrity, valid-state
  constraints, and immutability where practical.
- Use timezone-aware timestamps and PostgreSQL time for lease/expiry decisions.
- Use `BIGINT` for Kafka offsets and monotonically increasing fencing values.
- Make transaction and concurrency boundaries explicit.
- Add indexes only for demonstrated access paths and document those paths.
- Never silently repair, skip, or advance past invalid durable state.

## Code standards

- Follow the existing `src/updatis` package boundaries and local conventions.
- Use typed models and typed operation results at module boundaries.
- Avoid untyped dictionaries when an existing contract or model is suitable.
- Keep state transitions centralized, explicit, and testable.
- Expected concurrency outcomes should return typed results rather than leak raw
  database exceptions.
- Preserve deterministic identifiers, configuration hashing, resource naming,
  and serialization.
- Keep logs structured and bounded; never log secrets or unrestricted payloads.
- Prefer a minimal patch over a broad redesign.

## Testing workflow

During implementation, run the smallest relevant tests first. Run the complete
regression gate only after the focused implementation is stable.

Verification should include, when applicable:

1. Focused unit and repository tests.
2. Focused PostgreSQL/Kafka/Compose integration tests.
3. Complete Python test suite.
4. Contract tests.
5. `python -m pip check`.
6. Python bytecode compilation.
7. JSON configuration and schema parsing.
8. `docker compose config`.
9. `git diff --check`.
10. All verifier jobs from completed predecessor milestones.
11. The verifier introduced by the active milestone.

Rules:

- Do not weaken, delete, skip, or convert tests to expected failures merely to
  obtain a passing result.
- Do not claim a check passed unless it was executed successfully.
- Use unique project/resource names in integration tests.
- Bound every poll, retry, and wait with a timeout.
- Cleanup must be scoped and must not remove unrelated resources.
- Collect bounded diagnostics before failure cleanup.
- Confirm disposable Compose projects, containers, networks, and volumes are
  removed after verification.

## Documentation and evidence

- Design documents describe approved behavior and invariants.
- Acceptance documents contain factual commands and observed results only.
- Preserve prior milestone history; append new evidence instead of rewriting
  earlier claims.
- Clearly distinguish implemented behavior, proposed behavior, test fixtures,
  known limitations, and deferred work.
- Do not include machine-specific absolute paths, secrets, or unredacted
  credentials in tracked documentation.
- Update `ROADMAP.md` status only after the corresponding acceptance gate has
  actually passed.

## Git and remote operations

- Inspect `git status` and the diff before and after editing.
- Do not discard or overwrite unrelated changes.
- Do not commit, push, merge, tag, open a PR, or modify remote state unless the
  user explicitly requests that action.
- Never rewrite shared history to manufacture a PR.
- When asked to stage changes, prefer explicit paths over `git add .`.

## Efficient agent workflow

- Treat repository documents as the durable source of context; do not require
  the user to paste the full roadmap into each prompt.
- Read only files relevant to the active phase and its direct dependencies.
- Keep plans and final reports concise.
- Avoid repeatedly running the complete Docker verification suite while code is
  still changing.
- Do not use web search, browser automation, MCP servers, or external services
  unless the task genuinely requires them.
- Do not spawn subagents unless the user explicitly requests parallel work.

## Required final report

For implementation tasks, report:

1. Scope implemented.
2. Important invariants and failure semantics.
3. Files and migrations changed.
4. Tests and verification commands with exact observed results.
5. Checks not run, failures, or blockers.
6. Explicit confirmation of relevant excluded later-phase behavior.
7. Remaining risks and whether the branch is ready for review.
