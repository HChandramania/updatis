# Updatis Product and Implementation Roadmap

## Status and interpretation

This file is the canonical high-level roadmap for Updatis. Detailed phase
specifications and accepted design documents remain authoritative for exact
schemas, state transitions, limits, and failure behavior.

A roadmap entry describes intended scope. It must not be treated as implemented
until its acceptance gate has passed and factual evidence has been recorded.

Status markers:

- `COMPLETE`: implemented and verified.
- `ACTIVE`: current implementation target.
- `PLANNED`: approved high-level scope, not implemented.
- `UNFROZEN`: release direction exists, but its phase-level design is not yet
  approved.

## Product boundary

The first supported data path is:

```text
PostgreSQL source
  -> Debezium / Kafka Connect
  -> Kafka
  -> Updatis Python worker
  -> webhook destination
```

Updatis uses a separate PostgreSQL metadata store for configuration, captured
event identity, intake checkpoints, delivery state, recovery state, and audit
evidence.

Initial guarantees and limits:

- PostgreSQL-first; no initial MySQL compatibility promise.
- One documented local broker topology is a development topology, not HA.
- At-least-once destination delivery; no exactly-once claim.
- Conservative per-partition ordering.
- Durable intake and explicit offset management.
- One configured webhook destination per pipeline in v0.1.
- Inspectable retries, terminal failures, DLQ state, and permanent gaps by the
  end of v0.1.
- No metadata table may exist in or be captured from a source database.

## Foundation

Objective: replace prototype assumptions with explicit contracts, reproducible
dependencies, regression evidence, and a contributor baseline.

### F01 — Baseline defects and product boundary — COMPLETE

Record observed prototype defects as regression scenarios. Distinguish the
legacy example from supported product modules rather than silently preserving
incorrect behavior.

Acceptance intent:

- Fixtures cover skipped offsets, absent retry increments, zero-recipient
  success, and rejected snapshots.
- Legacy defects are documented as defects, not normalized as product
  semantics.

### F02 — Supported version matrix — COMPLETE

Select, pin, and document tested Python, PostgreSQL, Kafka, Kafka Connect,
Debezium, and client versions.

Acceptance intent:

- Clean dependency installation and imports succeed.
- Minimal container interoperability is verified.
- Direct and transitive dependencies are reproducible.

### F03 — Contracts and supported subset — COMPLETE

Define the event envelope, keys, supported PostgreSQL types, snapshot policy,
topology, initial retry/capacity defaults, and workload assumptions.

Acceptance intent:

- Snapshot, CRUD, delete, composite-key, invalid-type, and unsupported-value
  fixtures are reviewed.
- Numeric limits are tested or clearly marked provisional.

### F04 — Ownership, licence, and provenance — COMPLETE

Confirm repository ownership and licence direction. Record frontend provenance
and fallback behavior without importing unlicensed assets or promising legacy
MySQL support.

### F05 — Test and CI foundation — COMPLETE

Establish reproducible unit/contract commands, CI, isolated ephemeral resources,
and safe cleanup.

Acceptance intent:

- Tests run from a clean checkout.
- Resource names are isolated.
- Failed assertions fail CI.

### F06 — Contributor onboarding — PLANNED, NON-BLOCKING

Create a contributor walkthrough and issue templates based on the approved
product boundary.

This is useful project work but is not a blocker for the v0.1 implementation
sequence.

---

## v0.1 — Reliable local PostgreSQL-to-webhook pipeline

Objective: a developer can start one supported local pipeline, observe snapshot
and streaming CRUD events, deliver them to a webhook with durable ordering and
retry semantics, and diagnose failures and permanent gaps.

This release is a reliable local supported path, not a high-availability or
production-scale guarantee.

Internal slices:

| Slice | Phases | Theme | Status |
| --- | --- | --- | --- |
| v0.1-a | 01A–01C | Runtime, metadata, and configuration | COMPLETE |
| v0.1-b | 01D–01F | PostgreSQL capture and durable intake | COMPLETE |
| v0.1-c | 01G–01I | Ordered webhook delivery and failure semantics | ACTIVE |
| v0.1-d | 01J–01N | Operations, API/CLI, security, and observability | PLANNED |
| v0.1-e | 01O–01P | Integrated proof, example receiver, release gate | PLANNED |

### v0.1-a — Runtime, metadata, and configuration — COMPLETE

#### 01A — Application runtime and Compose topology — COMPLETE

Build the installable application image and separate API/worker entry points.
Provide Compose services with distinct source and metadata PostgreSQL instances,
volumes, credentials, networks, and health checks.

Acceptance intent:

- Clean start and restart succeed.
- Source credentials cannot access metadata.
- Kafka/Connect listeners and health checks are explicit.
- Normal lifecycle commands do not destroy durable data accidentally.

#### 01B — Transactional metadata schema — COMPLETE

Add explicit migrations for pipelines, configuration revisions, ingest records,
events, checkpoints, deliveries, attempts, DLQ entries, ordering gaps, and audit
records.

Acceptance intent:

- An empty metadata database migrates successfully.
- Uniqueness and foreign-key constraints are tested.
- Source PostgreSQL receives no metadata DDL.
- Migration failure blocks startup visibly.

#### 01C — Versioned configuration and secret safety — COMPLETE

Implement schema-validated, versioned configuration, secret references, safe URL
policy, endpoint isolation, redaction, and conservative rejection of unsafe
pipeline updates.

Acceptance intent:

- Invalid limits, URLs, schema versions, secrets, and metadata-source identities
  are rejected.
- Logs, validation failures, and exports do not disclose secret values.

### v0.1-b — PostgreSQL capture and durable intake — COMPLETE

#### 01D — PostgreSQL capture provisioning — COMPLETE

Provision scoped logical-replication credentials, publications, slots, explicit
Kafka topics, and deterministic Debezium connector resources.

Acceptance intent:

- Initial snapshot is followed by streaming changes.
- Only configured source tables are published.
- Kafka internal/data topic settings are explicit.
- Restart preserves publication, slot, Connect offsets, topics, and stream
  epoch.
- Rebootstrap creates a new epoch and capture identities.
- Metadata PostgreSQL has no publication, logical slot, or connector.

#### 01E — Normalization and event identity — COMPLETE

Normalize Debezium records into the approved event envelope. Preserve original
coordinates and keys, supported PostgreSQL values, snapshots, deletes, and
tombstone/control classification.

Acceptance intent:

- Supported snapshot/CRUD/type fixtures produce valid envelopes.
- Identical transport coordinates produce identical event IDs.
- Different epochs or coordinates produce different event IDs.
- Unsupported or malformed values quarantine predictably.
- No semantic deduplication beyond captured transport identity is claimed.

#### 01F — Durable Kafka intake — COMPLETE

Implement explicit consumer ownership, atomic intake persistence and metadata
checkpoint advancement, exact next-offset commits, duplicate recovery,
retention validation, bounded shutdown, and partition independence.

Acceptance intent:

- Failure at offset N cannot commit beyond N.
- A crash after metadata commit and before Kafka commit recovers through
  deduplication.
- Healthy partitions remain independent of a failed partition.
- Rebalance/revocation commits only known durable progress.
- Tombstones advance intake only after durable control classification.
- No delivery, retry, gap, redrive, API, CLI, or dashboard behavior is added.

Verified milestone evidence at completion:

- 95 tests passed.
- 4 documented legacy expected failures remained.
- Dependency, compilation, JSON, Compose, and diff checks passed.
- v0.1-a and v0.1-b Docker verifiers passed.
- Failure coverage included retention loss, invalid checkpoints, metadata
  outage, duplicate recovery, size ceilings, concurrent registration, and
  missing-slot/WAL-loss rejection.

### v0.1-c — Ordered webhook delivery — ACTIVE

Completion of 01G alone does not complete v0.1-c. The slice is complete only
after 01G, 01H, and 01I meet their acceptance gates together.

#### 01G — Partition heads, leases, and fencing — ACTIVE

Implement durable selection of the lowest unresolved delivery head, exclusive
leases, monotonically fenced ownership, and stale-owner rejection.

Required behavior:

- Select the lowest unresolved position per pipeline destination/source
  partition ordering lane.
- Permit at most one intentional in-flight operation per lane.
- Keep N+1 blocked while N remains unresolved.
- Permit other partitions to progress independently.
- Reject renewal, release, or completion from stale lease owners or fencing
  generations.
- Preserve correctness across competition, expiry, restart, rollback, and
  metadata outages.

Explicit exclusions:

- No webhook transport.
- No delivery-attempt budgets or backoff.
- No final success/failure acknowledgement semantics.
- No atomic DLQ/gap/cursor advancement.
- No redrive, public API, CLI, or dashboard.

The detailed 01G specification must define the approved lane identity, lease
defaults, database-clock semantics, fencing representation, and meaning of the
fenced scheduling-completion boundary.

#### 01H — Bounded webhook transport — PLANNED

Implement the actual webhook request boundary and destination-facing protocol.

Required behavior:

- Bounded connection, request, and response handling.
- Explicit 2xx acknowledgement policy.
- Redirect, timeout, DNS/network, and response-size behavior.
- Stable idempotency and signature headers.
- Status/error classification, including ambiguous remote outcomes.
- Example receiver atomically deduplicates stable event IDs.

Explicit exclusions:

- Attempt-budget persistence, retry scheduling, terminal DLQ/gap transitions,
  and final cursor advancement remain 01I.

#### 01I — Attempts, retries, terminal gaps, and cursor advancement — PLANNED

Persist delivery attempts, retry budgets, and backoff. Atomically commit terminal
delivery state, DLQ evidence, permanent ordering gaps, and delivery cursor
advancement.

Required behavior:

- Attempt counts and due times survive crashes.
- Retry policy is bounded and deterministic.
- Exhaustion atomically records terminal state, DLQ, gap, and cursor changes.
- A storage failure blocks advancement.
- Zero delivery never increments acknowledgement counts.
- Later records proceed only after the head becomes terminal under the approved
  reliability contract.

### v0.1-d — Operations, API/CLI, security, and observability — PLANNED

#### 01J — Pause, pressure control, and shutdown — PLANNED

Add distinct pause-capture and pause-delivery controls, high/low-watermark queue
pressure behavior, group-liveness preservation, and bounded graceful/forced
shutdown.

#### 01K — Pipeline management API and CLI — PLANNED

Add authenticated create, start, status, and stop operations. The minimal CLI
must use the same versioned API contract. Repeated commands are idempotent;
stored desired state and observed runtime state remain distinct.

Stopping a pipeline must not silently delete history, slots, topics, or volumes.

#### 01L — Read-only inspection — PLANNED

Add read-only event, DLQ, retry, and ordering-gap inspection through the API and
CLI. Support bounded filtering by pipeline, partition, and stable identity.
Payloads remain redacted or metadata-only by default.

This phase provides diagnosis, not direct SQL editing or redrive.

#### 01M — Security boundaries — PLANNED

Enforce authentication, trusted-proxy behavior, source/metadata privilege
separation, secret redaction, bounded requests, and webhook egress protections
covering SSRF, redirects, DNS changes, networks, and production TLS policy.

#### 01N — Health, state, logs, and counters — PLANNED

Expose process health, pipeline state, structured logs, and bounded worker
counters. Intake, acknowledgement, retry, DLQ, gap, backlog, and ownership
failures must be distinguishable. Do not present process-local state as a
cross-process registry.

Prometheus packaging belongs to v0.2; v0.1 defines the underlying signals.

### v0.1-e — Integrated proof and release gate — PLANNED

#### 01O — Real-stack failure CI and quickstart verification — PLANNED

Add a clean-stack integration gate covering snapshot/CRUD, duplicate intake,
worker crash, metadata outage, poison records, receiver outage, and
two-partition ordering. Verify the documented one-command local quickstart and
safe scoped cleanup.

#### 01P — Example receiver and diagnostics — PLANNED

Provide a small duplicate-safe webhook receiver and a configuration diagnostics
command. Diagnostics must identify missing source prerequisites and dependencies
without exposing secrets.

### v0.1 release gate

v0.1 is complete only when:

- A fresh checkout runs with documented prerequisites and bootstrap steps.
- Snapshot and streaming CRUD reach the example webhook receiver.
- At-least-once, partition-ordering, retry, DLQ, and permanent-gap behavior are
  demonstrated in real-stack CI.
- Metadata isolation is verified.
- Basic operator inspection is usable.
- Dependencies and images are reproducible.
- Ordinary and forced-stop paths leave either safe durable state or a visible,
  actionable error.

---

## v0.2 — Operate and diagnose pipelines — UNFROZEN

Release direction:

- Minimal operations dashboard.
- Connector lifecycle and reconciliation visibility.
- Worker metrics and pipeline health.
- Event, retry, DLQ, and gap inspection.
- Previewed, authorized, and audited redrive workflows.

Important constraint: redrive is a separate bounded recovery operation. It must
not erase permanent gap history or claim restoration of original global order.

The phase-level breakdown for v0.2 is not frozen. Do not invent phase IDs or
implementation commitments without an approved design update.

## v0.3 — Controlled recovery — UNFROZEN

Release direction:

- Historical replay.
- Guarded offset and checkpoint operations.
- Explicit capture/recovery epochs.
- Stronger capacity controls and retention/WAL headroom handling.
- Recovery from lost or replaced capture resources.
- Destination stale-version guidance.
- Performance and storage-growth characterization.

All destructive or history-changing operations require preview, authorization,
audit evidence, explicit warnings, and failure-safe behavior.

The phase-level breakdown for v0.3 is not frozen.

## v0.4 — PostgreSQL destination — UNFROZEN

Add PostgreSQL as a supported destination while preserving stable event identity,
idempotency, ordering, retry, recovery, and destination-specific transaction
semantics.

This release must not weaken the webhook reliability contract or reuse source
credentials for destination writes.

The phase-level breakdown for v0.4 is not frozen.

## v0.5 — Safe deployment, upgrade, and restoration — UNFROZEN

Release direction:

- Deployment hardening.
- Forward-compatible upgrade procedures.
- Migration safety and rollback documentation.
- Backup and restore drills.
- Coherent recovery across metadata, Kafka progress, and capture state.
- Measured recovery point and recovery time objectives.

The phase-level breakdown for v0.5 is not frozen.

## v1.0 — Stable public release — UNFROZEN

Release requirements:

- Stable public contracts and compatibility policy.
- Security review and documented threat boundaries.
- Reproducible release process and provenance.
- Published support envelope and version matrix.
- Performance and resource benchmarks.
- Upgrade, backup, restore, and recovery documentation.
- Contributor and maintainer workflows.
- No reliability claim broader than the evidence demonstrates.

The phase-level breakdown for v1.0 is not frozen.

---

## Current implementation position

```text
Foundation   F01–F05  COMPLETE
Foundation   F06      PLANNED / non-blocking
v0.1-a       01A–01C  COMPLETE
v0.1-b       01D–01F  COMPLETE
v0.1-c       01G      ACTIVE
v0.1-c       01H–01I  PLANNED
v0.1-d       01J–01N  PLANNED
v0.1-e       01O–01P  PLANNED
v0.2+                 UNFROZEN
```

Expected working branch for the active phase:

```text
feat/v01c-01g
```

Update this section only after factual acceptance evidence has passed.
