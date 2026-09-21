# Initial product contract

Status: Foundation design contract for future implementation. Nothing in the
current application implements this envelope or topology.

The normative machine-readable artifacts are
`contracts/event-envelope-v1.schema.json` and
`contracts/foundation-defaults-v1.schema.json`.

## Envelope and identity

Envelope version 1 contains an immutable `event_id`, `pipeline_id`, operation,
event and ingestion timestamps, an ordered non-empty key, source coordinates,
optional `before`, optional `after`, and metadata. Unknown top-level fields are
rejected in version 1.

`event_id` is the lowercase SHA-256 digest of a versioned canonical encoding of
`pipeline_id`, source stream epoch, Kafka topic, partition, and offset. It
identifies one captured Kafka record. It is stable across retries and unchanged
redrive. It does not deduplicate semantically identical changes emitted at
different offsets or after a new stream epoch.

The ordered key contains one through four columns. Each entry carries its
column name, PostgreSQL type family, and JSON value. The initial key types are
`smallint`, `integer`, `bigint`, `uuid`, `char`, `varchar`, and `text`.
Keyless tables, null keys, floating/decimal keys, arrays, and keys wider than
four columns are rejected before activation.

## Operations

| Debezium operation | Envelope operation | Initial behavior |
| --- | --- | --- |
| `r` | `read` | Initial blocking-snapshot row; delivered like an upsert event |
| `c` | `create` | `after` required; `before` absent |
| `u` | `update` | `after` required; `before` optional because replica identity controls availability |
| `d` | `delete` | Key required; `before` optional |
| Kafka null value | no envelope | Recognized tombstone/control record; classified and advanced without destination delivery or ordering gap |

Unrecognized operations are quarantined by the future product. Incremental and
ad hoc snapshots, truncate events, transaction aggregation, and schema-change
events are outside the first implementation scope.

## PostgreSQL value subset

The initial lossless mapping target is:

| PostgreSQL family | JSON representation |
| --- | --- |
| `boolean` | Boolean |
| `smallint`, `integer`, `bigint` | JSON integer |
| `real`, `double precision` | finite JSON number only |
| `numeric`, `decimal` | canonical decimal string |
| `char`, `varchar`, `text` | string |
| `uuid` | lowercase canonical UUID string |
| `bytea` | base64 string |
| `date`, `time`, `timetz`, `timestamp`, `timestamptz` | ISO 8601 string; offsets retained where the source type has one |
| `json`, `jsonb` | JSON value |
| enum | label string |

Initially unsupported: arrays, ranges/multiranges, network addresses, geometric
types, interval, money, bit strings, composite/domain types, XML, full-text
types, PostGIS, NaN/infinity, and extension-specific types. Unsupported values
must not be silently stringified.

## Initial topology

One pipeline has one PostgreSQL source, one Debezium connector task, selected
tables emitted to per-table Kafka topics with one partition each, one immutable
destination revision, one isolated Updatis metadata PostgreSQL instance, and
one worker deployment. The metadata instance is never a captured source.

This is a proposed v0.1 topology, not infrastructure created by the Foundation
gate. It provides partition-level order only; it does not promise global order,
high availability, or source-transaction atomicity at a destination.

## Provisional defaults and future validation targets

The values below are schema-validated design inputs. They are **not measured
capabilities, release guarantees, or completed reliability behavior**.

| Area | Provisional value |
| --- | --- |
| Maximum normalized event | 524,288 bytes (512 KiB) |
| Typical-event validation target | no more than 65,536 bytes (64 KiB) |
| Retry limit | 10 attempts or 86,400 seconds, whichever occurs first |
| Backoff | 1 second exponential with jitter, capped at 300 seconds |
| Webhook connect / total timeout | 5 / 30 seconds |
| Kafka retention target | 604,800 seconds (7 days) |
| Event and DLQ payload retention target | 2,592,000 seconds (30 days) |
| Inbox high watermark | 100,000 events or 10,737,418,240 bytes (10 GiB), whichever occurs first |
| Resume watermark | 80 percent of the applicable high watermark |
| Delivery concurrency | one in-flight request per partition; at most 8 active partitions per worker |
| Workload validation target | 100 events/s sustained; 500 events/s for 60 seconds; up to 10 tables; up to 1,000,000 initial snapshot rows |

Boundary fixtures prove the schemas accept the declared endpoints and reject
out-of-range configurations. They do not generate these workloads or prove
that storage and delivery can sustain them.
