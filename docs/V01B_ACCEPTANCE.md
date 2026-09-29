# v0.1-b acceptance record

This record covers roadmap issues 01D–01F only. Verification completed locally
on 2026-09-27 using CPython 3.13.15, PostgreSQL 18.6, Apache Kafka 4.3.1, and
Debezium Connect 3.6.3.Final.

The complete Python suite passed with **95 tests and 4 unchanged strict expected
failures** documenting legacy defects. Bytecode compilation, dependency checks,
JSON parsing, Compose rendering, and the working-tree whitespace check passed.

The Docker-backed v0.1-a regression verifier passed after migration through
`0002_durable_intake`. It reverified scoped migration/runtime privileges,
isolated negative-schema databases, API binding and loopback publication,
worker/API health, restart behavior, and explicit volume cleanup.

The Docker-backed v0.1-b verifier passed and proved:

- a table-owner publication, scoped capture role, epoch-specific `pgoutput`
  slot, explicit Kafka topics, and concurrent idempotent connector registration;
- snapshot and streaming create/update/delete records, tombstone classification,
  independent source tables/partitions, and connector restart without a new
  epoch;
- pinned-runtime decimal, base64 binary, JSON, offset-free time/timestamp,
  explicit-offset `timetz`, and UTC `timestamptz` behavior;
- immutable source-schema manifests with exact columns, ordinals, PostgreSQL
  types, key order, expected Debezium types, and lowercase SHA-256 fingerprints;
- independent 1 MiB raw-processing and 512 KiB normalized-envelope limits,
  complete raw hashes and lengths, a 256 KiB retained value prefix, and explicit
  truncation state;
- first-run earliest-offset initialization, two independent checkpoints,
  metadata-outage recovery, invalid-range rejection, and explicit refusal when
  Kafka retention advances beyond durable metadata;
- recovery when durable metadata is one record ahead of Kafka's committed group
  offset, without creating another event, and commit of the resulting durable
  checkpoint;
- visible rejection of a previously registered epoch after its replication slot
  is removed; provisioning does not silently recreate lost WAL state;
- absence of capture publications and slots in metadata PostgreSQL and absence
  of delivery, attempt, or delivery-ordering-gap rows.

The CI workflow now runs the Python suite plus separate `v01a-compose` and
`v01b-compose` Docker jobs. Hosted results are not claimed by this local record.

This acceptance does not cover webhook delivery, retries, delivery leases,
delivery ordering gaps, terminal DLQ transitions, redrive, management APIs, an
operator CLI, dashboard behavior, or roadmap issue 01G and later.
