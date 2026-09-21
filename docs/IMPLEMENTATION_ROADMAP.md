# Implementation roadmap

Status: planning only. The architecture defaults were approved on 2026-09-21; no implementation is authorized by this document. Baseline evidence is in [CURRENT_STATE.md](CURRENT_STATE.md).

## How to use this roadmap

Issue IDs below are stable planning identifiers, not existing GitHub issues. Each row is intended to become a bounded issue with the stated dependency and acceptance evidence. Split an issue further if its implementation cannot be independently reviewed. No schedule or staffing estimate is implied.

Priority: **M** = must have for that milestone; **S** = should have, may slip without weakening a must-have contract; **L** = later/conditional work. Milestone exit requires all M tasks and its release gate. Security/correctness requirements cannot be waived by calling them later work.

Dependency order is `Foundation → v0.1 → v0.2 → v0.3 → v0.4 → v0.5 → v1.0` for releases. Independently testable design tasks may run earlier where their explicit dependencies are met. Parallel-work notes describe future implementation opportunities, not authorization to launch agents or implement now.

The [reliability contract](RELIABILITY_CONTRACT.md) is normative. For each issue, include tests, logs/metrics, security impact, and documentation appropriate to its behavior. A successful mocked test alone does not prove a distributed delivery guarantee.

## Foundation gate — establish a trustworthy baseline

**Objective/outcome:** a contributor can reproduce checks, distinguish legacy code from product scope, and review executable behavioral contracts before implementation grows.

**Design/components:** retain useful Python layers; select a supported runtime/image set; introduce test fixtures and development tooling in later implementation. No wholesale rewrite and no requirement to make the legacy MySQL demo production-ready.

**Ordered epics:** F-A evidence and provenance; F-B compatibility and contracts; F-C regression harness and contributor baseline.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| F01 M | Record baseline defects as regression scenarios and distinguish legacy example from product modules | Audit | Fixtures cover skipped offsets, absent retry increments, zero-recipient success, and rejected snapshot; legacy defects are explicitly documented, not silently normalized as expected product behavior |
| F02 M | Select and document tested Python, PostgreSQL, Kafka, Connect, Debezium, and client versions | F01 | Clean install/import and minimal container interoperability checks pass; direct/transitive dependencies reproducible; no reliance on the current drifting virtualenv |
| F03 M | Specify envelope, key/type subset, snapshot policy, supported topology, retry/capacity defaults, and initial workload target | F01 | Sample snapshot/CRUD/delete/composite-key decisions and invalid-type fixtures reviewed; numeric limits justified by tests or clearly marked provisional |
| F04 M | Confirm repository ownership/license provenance and resolve frontend status | Inventory | Record license decision and missing frontend fallback; no unlicensed imported UI assets; no MySQL compatibility promise |
| F05 M | Establish test commands and CI skeleton with isolated ephemeral resources | F02 | Unit/contract suite runs from a clean checkout and CI; integration resources have unique names and safe cleanup; failing assertion fails CI |
| F06 S | Draft contributor walkthrough and issue templates from the new product boundary | F03, F04 | Another contributor can explain service boundaries and run checks using only instructions |

**Parallel:** F04 can run alongside F01–F03; F05 follows version selection; contract fixtures and contributor docs can proceed concurrently after F03.

**Observability/security/docs:** record version diagnostics without secret values; prohibit real source credentials in fixtures; publish decisions and support matrix. **Risks:** dependency incompatibility, unknown licensing, expanding the type matrix too early. **Excluded:** production feature delivery, MySQL support certification, UI polish. **Gate:** F01–F05 reviewed and executable checks reproducible.

## v0.1 — reliable local PostgreSQL-to-webhook pipeline

**Objective/outcome:** a developer starts a local pipeline with one documented command after explicit secret/bootstrap setup, observes snapshot and CRUD events, and can diagnose retries and durable gaps. This release is a reliable local supported path, not an HA production guarantee.

**Technical design/components:** Compose with separate example-source and metadata PostgreSQL instances; API and worker processes from one codebase; durable inbox and per-partition delivery ledger; one destination per pipeline; migrated schema; basic CLI. Refactor patterns from `app/core`, `app/repositories`, `app/services`, and `app/workers`; legacy order code remains outside supported product behavior.

**Dependencies:** foundation must-haves. **Ordered epics:** A deployment/state; B capture/intake; C delivery correctness; D configuration/security/operator entry points; E integrated proof.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 01A M | Build application image/entry points and Compose stack with separate source/metadata services, volumes and private infrastructure network | F02 | Clean start and restart work; source credentials cannot access metadata; Kafka/Connect listeners and health checks validated; no accidental destructive cleanup |
| 01B M | Add metadata migrations for pipeline revisions, events, checkpoints, deliveries, attempts, DLQ, gaps and audit state | F03, F05 | Empty DB migrates; uniqueness/foreign-key constraints tested; source DB receives no metadata DDL; failed migration blocks startup clearly |
| 01C M | Implement versioned configuration validation and secret-reference resolution | F03 | Reject invalid limits/URLs, unsupported schema versions, missing secrets, known metadata-source identity and unsafe changes; exports/logs redact secrets |
| 01D M | Provision explicit Kafka topics/serialization and PostgreSQL connector prerequisites/configuration | 01A, 01C | Snapshot then streaming capture works with scoped account; internal topic/partition/retention settings explicit; restart preserves source progress |
| 01E M | Implement generic envelope, stable captured-record identity and snapshot/tombstone mapping | F03, 01B | Golden fixtures for supported types, keys and operations; duplicate same coordinates yields same identity; unsupported data quarantines predictably |
| 01F M | Implement durable intake and explicit per-partition contiguous commits | 01B, 01D, 01E | Persistence failure at N cannot commit past N; crash after persistence before commit deduplicates; JSON/model errors get durable quarantine; rebalance test passes |
| 01G M | Implement partition-head selection, leases and fencing | 01B | Two competing workers cannot advance conflicting local state; N+1 remains blocked while N retries; another partition progresses; stale-owner completion rejected |
| 01H M | Implement bounded webhook transport and idempotency/signature headers | 01C, 01E | 2xx acknowledgement policy, timeout/redirect/size limits and status classification tested; receiver example atomically deduplicates IDs; ambiguous response recorded |
| 01I M | Persist attempt budgets/backoff and atomic terminal DLQ/gap advancement | 01F, 01G, 01H | Attempts survive crashes; exhaustion atomically writes gap/DLQ/cursor; storage failure blocks advancement; zero delivery never increments acknowledgement count |
| 01J M | Add pause/resume, queue pressure controls and bounded shutdown | 01F, 01G, 01I | Intake pauses at high watermark, resumes below low watermark, maintains group liveness; shutdown/kill preserves recoverability; pause-capture and pause-delivery distinguished |
| 01K M | Add authenticated pipeline create/start/status/stop API and minimal CLI | 01C, 01D, 01J | CLI uses API contract; repeat commands are idempotent; stored and observed status distinguished; stop does not drop history/slots/volumes |
| 01L M | Add read-only event, DLQ and gap inspection API/CLI | 01I, 01K | Filter by pipeline/partition/ID; redact payload by default; show reason and retry state; no SQL edits or unsupported redrive required for diagnosis |
| 01M M | Enforce API/proxy/secret/egress security boundaries | 01A, 01C, 01H | Auth negative tests, spoofed forwarding-header test, webhook SSRF/redirect/DNS/network allowlist cases, source-vs-metadata privilege tests pass; secret values absent from logs |
| 01N M | Expose process health, pipeline state, structured logs and basic worker counters | 01F, 01I, 01J | Intake/ack/DLQ/gap/backlog states distinguishable; worker failure visible independently of API; no assumed cross-process registry sharing; labels bounded |
| 01O M | Add real-stack integration/failure CI and one-command quickstart verification | 01K, 01L, 01M, 01N | Snapshot/CRUD, duplicate intake, worker crash, metadata outage, poison message, receiver outage and two-partition order tests pass on ephemeral stack |
| 01P S | Supply a small example app/receiver and configuration diagnostics command | 01K, 01M | Example proves duplicate-safe receiver behavior; diagnostics identifies source prerequisites and missing dependencies without disclosing secrets |

**Parallel:** 01A/01B/01C can proceed after foundation; 01E and transport design can proceed while Compose work lands. 01G is independent of capture after schema approval. Security cases should be developed with transport, not postponed to the gate. CLI/docs can use the approved API contract while worker integration finishes.

**Acceptance/release gate:** fresh checkout runs with documented prerequisites only; at-least-once/partition/gap contract demonstrated in real-stack CI; metadata isolation verified; basic inspection usable; dependencies and images reproducible. All ordinary and forced-stop paths leave either safe durable state or a visible error. No claim that failure-injection coverage eliminates every distributed-system risk.

**Required observability:** intake throughput and Kafka checkpoint lag, backlog count/age, acknowledgements, retry/terminal counts, gaps, source/metadata health, ownership failures, queue pressure. Prometheus packaging is v0.2, but the underlying signal definitions exist now.

**Security:** authenticated management, private infrastructure listeners, explicit local webhook allowlist, production TLS policy, scoped credentials, secret redaction, bounded requests and payloads. No public anonymous local dashboard by default.

**Documentation:** supported versions/types/topology, one-command quickstart, source prerequisites, idempotent receiver example, all failure/acknowledgement semantics, inspect-only DLQ workflow, reset/teardown data-loss warnings, troubleshooting.

**Risks/tradeoffs:** durable inbox adds database IO/storage and checkpoint coordination; partition blocking limits throughput during retries; local broker is not fault tolerant. **Excluded:** automated redrive mutation, historical replay, broad schema evolution, PostgreSQL destination, Kubernetes, HA, MySQL support, general transformations.

## v0.2 — operable system

**Objective/outcome:** users configure and inspect pipelines in a focused dashboard, see delivery versus capture health, and safely redrive selected failed events.

**Technical design/components:** API-backed dashboard and CLI, idempotent connector reconciler, independent worker metric exports, bounded event queries, recovery jobs and audit records using v0.1 metadata. No separate dashboard state engine.

**Dependencies:** v0.1 contract and state schema. **Ordered epics:** A status/telemetry; B operations UI; C audited recovery.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 02A M | Reconcile desired connector lifecycle and configuration revision with observed Connect state | 01K | Create/pause/resume/restart reconcile idempotently; drift/error/stale observations visible; concurrent admin requests serialized/tested |
| 02B M | Export API/worker Prometheus metrics and accurate ingest/delivery lag views | 01N | Scrapes include separate processes; committed intake lag differs from delivery backlog; cardinality and restarted-counter behavior tested |
| 02C M | Add paginated event/attempt/DLQ/gap query endpoints with payload permissions | 01L | Bounded queries and indexed filters; authorization tests; pruned/unavailable payloads explicit |
| 02D M | Build dashboard pipeline overview/configuration/status and failure detail | 02A, 02B, 02C | Responsive accessible flows; loading/error/empty/stale states; end-to-end test from setup to failed-event diagnosis |
| 02E M | Implement redrive preview, authorized job creation and concurrency guards | 02C | Preview warns original order is not restored; selection/revision fixed; duplicate active selection rejected; actor and budget audited |
| 02F M | Execute bounded redrive with original identity and retained gap history | 02E, 01G, 01I | Normal partition cursor unchanged; original gap persists as recovered late; duplicate/timeout/cancel/restart tests pass; no concurrent normal send for affected partition during a recovery request |
| 02G M | Add CLI/dashboard redrive progress, confirmation and cancel flows | 02D, 02F | User sees count/reason/destination/order warning before action; cancellation stops new attempts and records uncertain in-flight outcome |
| 02H S | Ship alert examples and tested incident exercises | 02B, 02G | Demonstrate capture stopped, destination down, backlog high, gap created, and stale metrics; runbooks use supported API/CLI |

**Parallel:** metrics and reconciler development independent; API query work precedes UI but UI fixtures can be built concurrently. Recovery executor and recovery UI can run in parallel after job contracts are fixed.

**Acceptance/tests:** connector status is not conflated with delivery health; worker metrics survive process separation; users inspect and redrive a DLQ record without DB edits; redrive never erases a gap or changes the original ID. Include browser E2E, access-control tests, pagination/resource tests, and recovery-job crash/cancel tests.

**Observability:** redrive counts/outcomes/age, unresolved/recovered-late gaps, connector observation freshness, source capture lag where measurable, consumer checkpoint lag, delivery age and throughput/failures. Alert on stale signals rather than reporting zero.

**Security/docs:** permission checks for raw payloads, exports, configuration and recovery; no browser exposure of infrastructure credentials; document redrive's idempotency and ordering effects. **Risks:** UI may imply false success, stale metrics hide failures, recovery competes for capacity. **Excluded:** raw offset editing in UI, arbitrary payload mutation, bulk historical replay, comprehensive connector catalogue.

## v0.3 — recovery and correctness

**Objective/outcome:** operators can deliberately replay retained history, diagnose schema changes, and recover from crashes/rebalances without undocumented offset edits.

**Technical design/components:** independent replay groups/jobs, checkpoint preflight and audit, explicit schema compatibility policy, retention headroom/capacity limits. Extend rather than postpone v0.1 ordering/backpressure/shutdown protections.

**Dependencies:** v0.2 recovery jobs and observability. **Ordered epics:** A history/checkpoint safety; B replay and schema; C adversarial validation.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 03A M | Add retained-history/checkpoint preflight and missing-history detection | 02B, 02C | Report available offset ranges and incompatible checkpoints; expired-history request rejected; source/Connect/intake/delivery positions distinguished |
| 03B M | Implement bounded historical replay using separate consumer group/job | 03A, 02F | Live intake offsets untouched; original IDs retained; destination revision selected; replay restart/dedupe/limits tested |
| 03C M | Add guarded live-offset maintenance workflow | 03A, 02E | Requires pause/drain, preview, authorization, checkpoint backup and audit; forward skips create gaps; backward moves are not advertised as redrive |
| 03D M | Define and expose schema-change compatibility outcomes | F03, 02C | Additive compatible changes tested; incompatible types/key changes produce visible block/quarantine; snapshot and delete limitations documented |
| 03E M | Validate pressure/rebalance/fencing and shutdown under sustained failure | 01J, 03B | Slow receiver/metadata failure/worker kill/rebalance matrix meets invariant assertions; memory/queue bounds measured; uncertain HTTP outcomes remain explicit |
| 03F M | Publish receiver idempotency/versioning recipes and executable recovery drills | 03B, 03C, 03D, 03E | Demonstrate late redrive without stale overwrite; replay after retention expiry refuses unsafe continuation; documented operator choices match observed results |
| 03G S | Add capacity planning report from repeatable load runs | 03E | Record hardware/configuration, saturation points, storage growth and recovery lag; no unsupported performance guarantees |

**Parallel:** schema work and history preflight can run independently; load/failure harness can be built alongside replay after contracts are fixed.

**Acceptance/tests:** every offset mutation/replay has explicit bounds and audit; no silent fallback to earliest/latest after history loss; restore/checkpoint mismatch is detected; failure tests cover both state and externally observed requests. Test migration of envelope/configuration versions and redrive of supported old envelopes.

**Observability:** replay progress, source WAL bytes/age where permissions allow, Kafka retention headroom, queued bytes, blocked partition age, lease conflicts, schema incidents. **Security/docs:** privileged recovery actions, audit integrity, destination replay authorization, retention and payload-export policy; publish delivery contract and incident recovery decision tree.

**Risks:** late effects cannot be undone, retained history is finite, long jobs can starve live traffic. **Excluded:** exactly-once claims, transparent failover, arbitrary source offset rewriting, unlimited replay storage, global order.

## v0.4 — extensible platform

**Objective/outcome:** contributors implement a destination against a stable, tested contract; users can synchronize a supported PostgreSQL target.

**Technical design/components:** in-process destination interface with validation/send/outcome/health hooks, shared retry/identity logic, PostgreSQL transactional application and dedupe/version ledger. Do not expose a plugin execution service or general transformation engine.

**Dependencies:** v0.3 envelope/recovery semantics. **Ordered epics:** A destination contract; B PostgreSQL implementation; C contributor stabilization.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 04A M | Extract destination interface and conformance fixtures from webhook behavior | 03F | Existing webhook tests pass unchanged; outcomes/timeouts/cancellation/identity/versioning contract documented |
| 04B M | Specify target table/key/type/delete mapping and provisioning policy | 04A, 03D | Supported subset and rejected mappings explicit; source/target loops and metadata target rejected; arbitrary target DDL not silently executed |
| 04C M | Implement PostgreSQL destination with transactional dedupe and stale-version protection | 04B | Duplicate, late redrive, update/delete and rollback fixtures pass; event effect and dedupe commit atomically; credentials scoped |
| 04D M | Run destination conformance/failure/replay integration matrix | 04C, 03B | Target outage, deadlock, connection loss and crash outcomes meet contract; no unsupported cross-partition transaction claim |
| 04E M | Stabilize configuration migration and publish destination-development guide/examples | 04A, 04D | Example destination passes harness; documented compatibility and failure handling; old supported configuration upgrades tested |
| 04F S | Add additional example integrations using existing webhook interface | 04E | Examples runnable and idempotent; no extra destination support promise |

**Parallel:** interface fixtures and mapping specification can be reviewed concurrently; documentation examples follow frozen interface semantics while PostgreSQL failure tests run.

**Acceptance/tests:** both destinations satisfy the same observable attempt/DLQ/gap contract, with explicit PostgreSQL-specific transactional limits. **Observability:** destination-labelled bounded metrics, target errors/latency and mapping failures. **Security/docs:** target least privilege, connection secret redaction, identifier-safe SQL, feedback-loop prevention; publish extension support/version policy.

**Risks:** destination abstraction may hide different guarantees; SQL type/schema coverage can grow rapidly; source key changes can break ordering. **Excluded:** broad destination catalogue, untrusted runtime plugins, arbitrary transformation DSL, automatic cross-database transaction replication.

## v0.5 — deployment readiness

**Objective/outcome:** operators can deploy, upgrade, observe, back up and restore a documented self-hosted installation within explicit recovery limits.

**Technical design/components:** harden existing auth/secret controls, OpenTelemetry instrumentation, versioned release images/migrations, coordinated metadata/Kafka/Connect recovery procedures. Compose remains supported. Kubernetes is conditional on proven Compose reliability and actual deployment demand.

**Dependencies:** stable state/configuration and recovery workflows. **Ordered epics:** A security/telemetry; B upgrade/backup proof; C optional platform packaging.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 05A M | Harden administrative authorization, credential rotation and audit access | 04E | Rotation without secret logging; revoked credentials rejected; payload/recovery/admin permissions separated; threat-model tests pass |
| 05B M | Add secret-file/provider integration with validated failure behavior | 05A | Missing/rotated/revoked secret fails safely; no secret persistence in exported config, logs or telemetry; local path remains simple |
| 05C M | Add OpenTelemetry instrumentation/export and correlation across pipeline stages | 02B, 04A | API/worker spans correlate by safe IDs, sampling/cardinality bounded, disabled exporter does not break delivery |
| 05D M | Implement/test versioned upgrade and schema/config migration procedures | 04E | Previous supported release upgrades on representative data; incompatible downgrade rejected; backup/rollback limits explicit |
| 05E M | Build metadata/Kafka/Connect backup and restore drills | 03A, 05D | Restored metadata checkpoints compared with Kafka; stale backup never silently skips records; missing WAL/history produces explicit recovery decision; measured RPO/RTO published |
| 05F M | Publish hardened Compose deployment profiles and runbook exercises | 05A, 05B, 05C, 05E | Network isolation, TLS boundary, resource limits, retention, restart and restore exercised on a clean host; single-node limits explicit |
| 05G L | Add Kubernetes manifests/Helm only after deployment gate and demand review | 05F | Equivalent lifecycle/secret/upgrade/restore tests pass; readiness/termination/storage semantics verified; support burden accepted |

**Parallel:** telemetry, authorization hardening and upgrade tooling can proceed independently after their prerequisites. Backup drills depend on migration behavior. Kubernetes follows, not substitutes for, those drills.

**Acceptance/tests:** tested upgrade and restore runbooks, security negative tests, observability degradation tests, and published support topology. **Observability:** telemetry exporter health, credential/config failures, migration status, backup age and restore outcomes. **Security/docs:** threat model, operator hardening, audit retention, secret rotation, breach response, rollback/restore decision trees.

**Risks:** inconsistent backups across durable systems, deployment portability, false HA assumptions, retained sensitive payloads. **Excluded:** managed service, multi-region, enterprise SSO unless separately approved, automatic source failover, Kubernetes release without equivalent proof.

## v1.0 — stable public release

**Objective/outcome:** external teams can rely on a documented support and compatibility contract, repeatable releases, and measured operational limits.

**Technical design/components:** stabilize existing boundaries rather than add new engines; publish supported API/config/envelope/destination versions, deprecation policy, upgrade path and benchmark methodology.

**Dependencies:** must-have deployment and recovery gates; open-source provenance/docs gate. **Ordered epics:** A compatibility/security validation; B measured readiness; C release rehearsal.

| ID / priority | Task | Depends on | Acceptance and required tests |
| --- | --- | --- | --- |
| 10A M | Freeze supported public API/config/envelope/destination contracts and deprecation policy | 04E, 05D | Contract compatibility suite covers previous supported versions; breaking changes explicit and versioned |
| 10B M | Perform security/dependency/image/provenance review and remediate release blockers | 05F, F04 | Findings triaged with owners and evidence; critical release blockers closed; supported vulnerability response documented |
| 10C M | Publish reproducible performance and failure-recovery benchmarks | 03G or equivalent measured runs, 05E | Hardware/data/config documented; latency/throughput/storage/recovery and saturation limits reproducible, not inferred from Compose |
| 10D M | Run independent clean-install, upgrade and incident documentation trials | 10A, 10B, 10C | Unfamiliar contributor completes setup and recovery without hidden instructions; limitations visible in guide |
| 10E M | Rehearse tagged release, artifact provenance, changelog and rollback communication | 10D | Versioned images/artifacts match tag; checks gate publishing; release notes include migration and reliability limits |

**Parallel:** contract review, security review and benchmark work can overlap after v0.5 prerequisites; independent trials and publishing rehearsal follow findings resolution.

**Acceptance/tests:** all supported matrix cells pass, critical risks resolved or scope explicitly narrowed, open-source readiness checklist complete, and releases reproducible. **Observability:** publish alert baselines and operational SLO definitions supported by measurements. **Security/docs:** security review record, disclosure policy, compatibility and maintenance policy, production guide, upgrade/changelog.

**Risks:** overstating support, insufficient maintainership, unmeasured resource demand, unsupported ecosystem drift. **Excluded:** scope expansion to justify the version number, exactly-once claims, community promotion before documentation trials pass.

## Cross-cutting definition of done

Every behavior-changing issue must show a passing meaningful regression/contract test, the corresponding observability signal, and updated public behavior documentation. Infrastructure changes require real-stack evidence. Recovery changes require crash/duplicate/order tests. Security-sensitive endpoints require negative authorization tests. Migrations require upgrade validation.

Do not silently change defaults to make tests pass, reset live offsets as test setup, or use real customer source data in fixtures. Do not count an unverified feature as implemented and working. Preserve useful legacy work while keeping it outside the product support matrix.
