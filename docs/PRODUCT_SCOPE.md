# Product scope

Status: proposed implementation scope based on the approved 2026-09-21 audit. This document describes future behavior, not current support.

## Product and first user journey

Updatis is an open-source, self-hosted control plane for configuring, running, monitoring, and recovering CDC pipelines. Debezium captures changes; Kafka transports them; Updatis makes the resulting pipeline understandable and operable.

The initial user is a backend developer or small engineering team sending PostgreSQL changes to an application webhook. Data engineers using operational integrations are adjacent users. The first journey is: start the local stack, configure one source/table set and webhook, observe snapshot and change delivery, diagnose a failed delivery, and deliberately recover it.

The smallest coherent wedge is one PostgreSQL source and one webhook destination per pipeline, a supported subset of keyed tables/types, a durable delivery ledger, basic CLI operations, and eventually a focused dashboard. It is not an order-management product. The existing MySQL order application is strictly a legacy example; its existence creates no MySQL support promise.

## Fit with the repository

| Existing work | Decision | Reason |
| --- | --- | --- |
| FastAPI factory, thin routes, services/repositories | Retain patterns; adapt names/contracts only where necessary | Useful separation without extra services |
| Debezium and Kafka | Retain responsibilities; validate and pin compatible versions | Matches intended engine/backbone |
| Durable queue, claims, DLQ concepts | Refactor into isolated PostgreSQL state and tested transitions | Useful concept; current offset/retry/fencing behavior is defective |
| JSON logging and settings | Retain useful conventions; add validation/redaction | Avoid needless rewrites while correcting security/operability gaps |
| Custom metrics/log spans | Replace/export through standard instrumentation incrementally | Current process-local state is not a monitoring system |
| Order models, CRUD, MySQL SQL, socket delivery | Preserve as clearly separated legacy example | Not reusable product contracts; no dual-source support commitment |
| Direct broadcast endpoint | Exclude from supported pipeline path | Bypasses durable state and acknowledgement semantics |
| Empty frontend reference | Treat as unavailable; recover only if provenance/source exists | Product cannot depend on missing code |
| Old readiness plans | Mark historical during later documentation implementation | Prevent conflicting instructions |

The architecture can support the direction after deliberate rework of ingestion, state, and contracts. A small patch to the current order service cannot produce a generic control plane. Keeping MySQL compatibility as a simultaneous product goal would enlarge the test and operational matrix without validating the first wedge.

## Priorities

**Must have for v0.1:** reproducible Compose stack; PostgreSQL capture including snapshots; one webhook destination; isolated metadata store; schema-validated configuration; generic versioned envelope; partition-ordered delivery; durable retries/DLQ/gaps; explicit idempotency and acknowledgement rules; authentication and secret references; outbound-request security; basic CLI and safe DLQ inspection; logs/health/basic delivery counters; migrations; happy-path and failure integration CI; accurate quickstart.

**Should have by v0.2:** usable dashboard, connector lifecycle/status reconciliation, worker/Connect lag visibility, Prometheus-compatible metrics, filters for event/gap inspection, authorized audited redrive, clear incident runbooks, cancellation and progress for operator actions.

**Later:** controlled historical replay and offset tooling (v0.3), PostgreSQL destination and stable destination interface (v0.4), richer deployment/security/instrumentation and optionally Kubernetes (v0.5), compatibility commitments and benchmarks (v1.0).

**Explicitly out of scope for initial releases:** AI, custom streaming engines, managed cloud, multi-region, broad connector catalogues, supported MySQL source compatibility, arbitrary transformations, cross-partition/global order, exactly-once webhook effects, source transaction atomicity at destinations, enterprise tenancy/SSO, and transparent PostgreSQL failover. New scope requires its own evidence and release gate.

## Differentiation and limits

Compared with configuring raw Debezium/Kafka Connect, Updatis should provide a validated complete path to a webhook: setup checks, delivery state, bounded retries, explicit ordering gaps, inspection, and safe recovery in one workflow. Kafka Connect remains the connector runtime; Updatis must not duplicate its capture engine or pretend connector RUNNING status means successful destination delivery.

Relative to other CDC platforms, differentiation is a product hypothesis: a narrow self-hosted operational experience for application developers, with honest recovery semantics. No competitor comparison or market superiority was established by this repository audit. Validate usefulness with new developers completing the quickstart and an outage/redrive exercise, before promotion or catalogue expansion.

Kafka plus Connect plus PostgreSQL imposes resource and operational cost for small teams. One-command installation reduces assembly work; it does not create high availability or remove source-database responsibilities. Publish measured resource requirements and failure limits.

## Success criteria and unresolved product choices

- A new developer completes the documented local flow without undocumented edits; measure setup duration before claiming a five-minute quickstart.
- Every accepted source record has an observable state: queued, in flight, retrying, acknowledged, deliberately filtered, or quarantined with an ordering gap.
- Destination outages neither silently drop records nor cause unbounded source/metadata disk growth without warnings and protective action.
- A developer can explain duplicate handling and show that redrive is not original-order restoration.
- Define supported key shapes, data types, payload sizes, table counts, expected event rates, and recovery objectives in the foundation gate. No numerical performance promise is approved yet.
- Recovering the frontend, adoption of Apache 2.0, precise dependency versions, and external-source deployment examples remain evidence-dependent decisions.

See the [architecture](TARGET_ARCHITECTURE.md), [contract](RELIABILITY_CONTRACT.md), and [risk register](RISK_REGISTER.md) for the corresponding constraints.
