# Updatis master plan

Status: approved planning direction. Foundation items F01–F05 have been
authorized and implemented; no v0.1 implementation has been authorized.
Audit date: 2026-09-21. Repository baseline: `01f4723` (`secure + clean`).

Updatis will become a self-hosted control plane for configuring, running, monitoring, and recovering CDC pipelines. The first supported path is **PostgreSQL → Debezium → Kafka → Python worker → webhook**, deployed with Docker Compose.

The current repository is a MySQL order-notification prototype with useful Python module boundaries and incomplete reliability mechanisms. It is not a supported CDC platform. The MySQL order application is strictly a legacy example, not evidence of supported MySQL product compatibility. No live CDC path was verified during the audit.

## Planning documents

- [Current state and evidence](docs/CURRENT_STATE.md): inventory, capability classifications, defects, and checks.
- [Product scope](docs/PRODUCT_SCOPE.md): users, product boundary, priorities, and retained work.
- [Target architecture](docs/TARGET_ARCHITECTURE.md): process responsibilities, storage isolation, lifecycle, and security boundaries.
- [Reliability contract](docs/RELIABILITY_CONTRACT.md): partition ordering, at-least-once semantics, exhaustion, ordering gaps, DLQ, redrive, and idempotency.
- [Implementation roadmap](docs/IMPLEMENTATION_ROADMAP.md): ordered epics, issue-sized tasks, dependencies, tests, and release gates.
- [Open-source readiness](docs/OPEN_SOURCE_READINESS.md): documentation, licensing decisions, contributor experience, and releases.
- [Risk register](docs/RISK_REGISTER.md): technical, security, product, and operational risks.

## Approved architectural decisions

1. PostgreSQL is the first supported source; webhooks are the first supported destination. PostgreSQL destination support comes later.
2. Debezium remains the capture engine and Kafka the event backbone.
3. One Python codebase supplies an API process and a separately operated worker process. Avoid additional brokers and unnecessary microservices.
4. Use an Updatis-owned PostgreSQL metadata store isolated from **every captured source database**: a separate service/instance, database, credentials, storage, and lifecycle. Never create metadata tables in a source or capture the metadata store.
5. Persist a durable inbox, delivery state, dead letters, ordering gaps, and audit history. This adapts an existing design concept; it does not certify or reuse its defective implementation unchanged.
6. Process each Kafka partition in order. A transient failure blocks later records in that partition. Advancement past an exhausted record requires an atomic durable dead letter and ordering-gap record. Redrive is separate from normal processing and does not restore original order.
7. Promise at-least-once semantics within documented durability, retention, and terminal-DLQ boundaries. Destinations must tolerate duplicates. Never promise exactly-once delivery or global order.
8. Treat the missing frontend as unavailable. Build a minimal operations dashboard; keep the legacy order UI outside the product contract.

## Phases and release gates

| Phase | User-visible result | Gate |
| --- | --- | --- |
| Foundation | Reproducible contributor baseline and executable contracts | Runtime/image matrix selected; regression fixtures and architecture decisions reviewed |
| v0.1 | One-command local PostgreSQL-to-webhook pipeline | Snapshot and CRUD delivery, failure recovery, durable DLQ/gaps, security defaults, and integration CI pass |
| v0.2 | Operate and diagnose pipelines | Dashboard, connector lifecycle, worker metrics, event/DLQ inspection, and audited redrive work |
| v0.3 | Controlled recovery | Historical replay and guarded offset operations pass recovery tests; schema and retention limits are explicit |
| v0.4 | Extend destinations | PostgreSQL destination and destination contract pass duplicate/order/schema tests |
| v0.5 | Deploy and upgrade safely | Compose upgrades and restore drills pass; optional Kubernetes support has equivalent recovery evidence |
| v1.0 | Stable public release | Compatibility policy, security review, benchmarks, support envelope, and release process are demonstrated |

Baseline authentication, secrets, shutdown, backpressure, migrations, and crash-recovery tests are v0.1 requirements, not deferred production hardening. Versions describe exit criteria, not dates or claims about current maturity. The legacy API's `2.0.0` string is not a product release history.

## Remaining review questions

- Which tested Python/PostgreSQL/Kafka/Debezium versions form the initial support matrix?
- What payload size, queue capacity, Kafka retention, retry budget, and throughput/latency targets define the first supported workload?
- Which SQL types, key shapes, snapshot modes, and schema changes are admitted in v0.1?
- Can ownership and license provenance of all intended contributions be established before adopting Apache 2.0?
- Does recoverable frontend source exist, and is its license compatible? Implementation must not depend on it.

The roadmap supplies conservative behavioral defaults but does not invent performance results or dependency compatibility. Resolve these questions in the foundation gate.

## Principal risks and stop conditions

The audited consumer can acknowledge failed records; retry attempts are not persisted; socket delivery can report success to no recipients. Do not carry those behaviors into the product. Source WAL growth, disk exhaustion, compromised webhook URLs, stale worker ownership, and inconsistent recovery of Kafka plus metadata are major new-design risks.

Do not promote the project until an unfamiliar developer can complete the documented quickstart. Do not label the pipeline reliable until failure tests prove the contract. Do not implement this plan without a separate implementation authorization.
