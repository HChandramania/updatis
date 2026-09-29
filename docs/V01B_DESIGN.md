# v0.1-b implementation design record

Status: approved design for roadmap issues 01D, 01E, and 01F.

This slice adds PostgreSQL `pgoutput` capture, explicit Kafka/Connect resources,
versioned generic normalization, and durable Kafka intake. It does not create
webhook deliveries, delivery attempts, delivery ordering gaps, final DLQ
transitions, retries, redrive, management APIs, an operator CLI, or dashboard
behavior. The legacy MySQL application remains outside the supported path.

## Capture and resource ownership

The source administrator enables `wal_level=logical` and bounded replication
slots/senders. A table-owning provisioning identity creates a publication that
names only configured tables. A separate non-superuser capture login has the
cluster-level PostgreSQL `REPLICATION` attribute plus `CONNECT`, schema `USAGE`,
and table `SELECT`. PostgreSQL cannot scope `REPLICATION` to one database,
publication, or slot, and logical slots have no publication-style owner. Slot
namespacing is therefore procedural and reinforced by dedicated credentials,
network access, deterministic names, resource validation, and exact-name
cleanup guards.

Every stream epoch is a UUID with a distinct connector, slot, topic prefix,
data-topic set, consumer group, and checkpoint namespace. A normal restart
preserves all those resources and Connect offsets. Rebootstrap always creates a
new epoch and never silently replaces missing WAL, a missing/invalid slot, or
Connect progress. Epoch identities are immutable and lifecycle changes are
append-only metadata transitions.

The connector has one task, `snapshot.mode=initial`, `plugin.name=pgoutput`, an
explicit publication and slot, and publication auto-creation disabled. JSON key
and value converters have schemas enabled. The connector uses
`decimal.handling.mode=string`, `binary.handling.mode=base64`,
`time.precision.mode=isostring`, `hstore.handling.mode=json`, and
`include.unknown.datatypes=false`. Real records from the pinned Debezium image,
not handwritten fixtures alone, are the serialization acceptance evidence.

Supported PostgreSQL schema/table/key identifiers are unquoted lowercase ASCII
identifiers matching `[a-z_][a-z0-9_]{0,62}`. Combined with fixed UUID topic
prefix components and dot separators, this makes per-table topic names
collision-free without a custom Kafka Connect topic-naming plugin.

Provisioning stores an immutable source-schema manifest for every table. Each
manifest records every column name, ordinal, PostgreSQL type, nullability, key
membership/order, expected Connect primitive type, expected Debezium logical
type, and a lowercase SHA-256 fingerprint of canonical JSON. Intake compares
the complete emitted row schema with the manifest. Missing, omitted, added,
reordered, or type-changed fields are quarantined; `include.unknown.datatypes`
cannot silently erase a source column.

## Normalization

Snapshot `r`, create `c`, update `u`, and delete `d` become envelope operations
`read`, `create`, `update`, and `delete`. Create requires `after` and null
`before`; snapshot and update require `after`; update/delete permit unavailable
`before`; delete requires null `after`. A Kafka null value is a durable tombstone
control classification and produces no event. Other operations and malformed,
schema-incompatible, unsupported, or oversized inputs produce an immutable
intake discontinuity and no fabricated event.

`occurred_at` is derived only from `payload.source.ts_ms`; top-level
`payload.ts_ms` is retained as connector-processing metadata. Date, time, and
timestamp-without-time-zone values are normalized according to their verified
PostgreSQL type and Debezium logical type and remain offset-free even when
`isostring` emits a `Z`. `timetz` retains an explicit offset; the pinned
Debezium runtime canonicalizes it to UTC with `Z`. `timestamptz` becomes
canonical UTC. Temporal infinity and negative infinity are rejected.
Tests cover fractional precision, pre-epoch values, non-UTC offsets, and DST
boundaries using actual pinned-runtime records.

Event identity is the lowercase hexadecimal SHA-256 of canonical UTF-8 JSON:

```text
["updatis-event-id",1,pipeline_uuid,epoch_uuid,topic,partition,offset]
```

Quarantine identity adds the complete raw-record SHA-256 under the distinct
`updatis-quarantine-id` domain. Raw hashing length-frames key and value and
distinguishes null from empty. Identity does not claim semantic deduplication
across different offsets, connector re-emission, or rebootstrap.

The Kafka serialized-record ceiling is 2 MiB, the intake decoding ceiling is
1 MiB, and the canonical normalized-envelope ceiling is 512 KiB. For a consumed
oversized raw record, intake hashes all bytes, records original key/value
lengths and truncation flags, and stores `bytea` prefixes limited by database
checks to 64 KiB for `raw_key_prefix` and 256 KiB for
`raw_value_prefix`. It records `raw_record_too_large` without parsing. An
oversized normalized envelope records `normalized_envelope_too_large`.

## Durable intake and offsets

The consumer has auto commit disabled and `auto_offset_reset=none`. On first
assignment it queries the earliest retained offset `E`, log end `L`, and group
offset `K`. With no metadata checkpoint, a new epoch requires absent `K` and
atomically stores `initial_offset=next_offset=E`, then explicitly seeks to `E`.
For an existing checkpoint `M`, intake requires `E <= M <= L` and rejects
`K > M`. A missing or older `K` is reconciled by seeking to `M`. Retention loss,
invalid checkpoints, and group progress ahead of metadata stop the epoch rather
than resetting it.

For record offset `N`, one bounded metadata transaction locks the partition
checkpoint. `N > M` is rejected as non-contiguous. `N == M` atomically inserts
the ingest record plus exactly one event, intake discontinuity, or control
classification and advances the checkpoint. `N < M` must match the durable raw
hash and classification and is treated as a duplicate. After the transaction,
the worker commits the resulting durable checkpoint returned by metadata: this
is `N + 1` for a new record and existing `M` for a duplicate. It never commits
backward or beyond durable metadata, and commits an explicit topic-partition
map rather than consumer-wide progress.

An intake discontinuity explicitly records that durable intake advanced over
an input that could not become an event. It is distinct from a delivery
ordering gap and creates no delivery/DLQ terminal state. A metadata failure at
offset N blocks N and every later offset in that partition while other
partitions remain independent. A crash after metadata commit and before Kafka
commit reprocesses, verifies, deduplicates, and commits the existing durable
checkpoint.

Poll calls are bounded to one second and 50 records. Kafka requests use a
10-second timeout, explicit commits a five-second timeout, metadata acquisition
and lock waits five seconds, statements ten seconds, and each metadata
transaction fifteen seconds. Revocation has twenty seconds and shutdown thirty
seconds. Revocation stops new work, completes or rolls back active metadata
work, discards unstaged records, and commits only durable offsets for revoked
partitions. A prolonged metadata outage terminates intake without advancing
Kafka.

## Acceptance criteria

### 01D

- The scoped capture role, table-owner publication, `pgoutput` slot, one-task
  connector, explicit internal topics, and explicit one-partition data topics
  are verified in the Docker stack.
- Snapshot followed by insert/update/delete capture succeeds; tombstones are
  present and classified separately.
- Actual Debezium 3.6.3.Final records prove every configured JSON/type mapping.
- The immutable source-schema manifest detects omitted, added, reordered, and
  type-changed fields.
- Metadata PostgreSQL has no capture publication, slot, role, or connector.
- Concurrent identical registration creates one resource set; conflicting
  registration is rejected.
- Restart preserves slot, publication, Connect offsets, epoch, and topics with
  no resnapshot. WAL/slot loss fails visibly. Rebootstrap uses a new isolated
  epoch and cleanup removes only test-owned resources.

### 01E

- Snapshot/create/update/delete normalize into valid v1 envelopes; tombstones
  create no row event.
- Composite keys preserve configured order, type, and original Kafka key.
- Supported values round-trip without silent stringification; malformed,
  unsupported, schema-drifted, non-finite, and oversized inputs quarantine.
- Time and timestamp remain offset-free, `timetz` retains its offset, and
  `timestamptz` is UTC. Precision, pre-epoch, timezone, DST, and infinity cases
  pass contract and pinned-runtime tests.
- Same coordinates produce the same lowercase hexadecimal event identity;
  another epoch or coordinate produces another identity.
- Raw and normalized size limits, complete hashes, original lengths, bounded
  binary prefixes, and truncation markers are independently verified.

### 01F

- Every new group explicitly initializes from Kafka's earliest retained offset,
  persists it atomically, seeks to it, and supports an empty topic.
- Checkpoints below earliest, above log end, or behind a Kafka commit are
  rejected. Ordinary intake never moves offsets backward.
- Event/control/discontinuity persistence and checkpoint advancement are one
  transaction. Offset commits use the returned durable checkpoint, including
  existing `M` for duplicate input, and are explicitly partition-scoped.
- Failure at N cannot acknowledge beyond N. Crash after persistence and before
  Kafka commit safely deduplicates. Two partitions remain independent.
- Revocation and shutdown commit only known durable positions within their
  deadlines. Retention loss and metadata outage stop safely.
- No webhook request, destination acknowledgement, delivery retry, delivery
  lease, final DLQ transition, delivery ordering gap, redrive, management API,
  operator CLI, dashboard, or 01G behavior occurs.
