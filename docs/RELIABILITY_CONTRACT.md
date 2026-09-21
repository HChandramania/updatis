# Reliability contract

Status: **proposed normative product contract**, approved for planning on 2026-09-21. The current prototype does not satisfy it. Release claims require the [roadmap](IMPLEMENTATION_ROADMAP.md) tests and published support limits.

## Scope and terminology

The first supported path is PostgreSQL → Debezium → Kafka → an isolated PostgreSQL delivery store → one webhook destination per pipeline. A pipeline binds a source stream epoch, table selection, topic/partition mapping, and destination revision. The metadata store is separate from every captured source database.

- **Ingested:** the original record or a quarantine decision is committed to metadata storage.
- **Acknowledged:** the webhook returned a supported success response and that outcome is durably recorded.
- **Exhausted:** the configured attempt or retry-age budget is reached, or policy classifies an error as terminal.
- **DLQ:** durable quarantine storage for malformed, unsupported, or terminally failed records; not necessarily a Kafka topic.
- **Ordering gap:** a durable declaration that normal partition processing advanced without successful delivery of a particular source record.
- **Redrive:** an operator-authorized new delivery attempt series for selected DLQ records, using the original event identity when the original event is unchanged.

## Delivery semantics: at least once, with explicit terminal outcomes

Updatis uses **at-least-once processing and webhook delivery semantics**. Duplicates can occur after worker restarts, Kafka rebalances, ambiguous timeouts, lost acknowledgements, or a crash between a remote success and a local commit. Exactly-once effects are not promised.

For a valid supported record that has not reached terminal policy, retained durable state is retried until it is acknowledged or terminally quarantined. Bounded retries mean that **not every accepted record is guaranteed to reach the webhook**: a terminal DLQ record is an explicit non-delivery outcome. DLQ placement is never counted as a successful delivery. Eventual delivery after a prolonged outage requires a destination that recovers within policy, or a deliberate redrive while the needed data remains available.

The guarantee assumes durable metadata/Kafka storage, retained required source history, valid configuration, and sufficient capacity. Destroyed volumes, expired history, incompatible restore points, manually removed records, and unacknowledged administrative data deletion fall outside automatic recovery. Such losses must be surfaced; they cannot be disguised as successful processing.

## Partition-level ordered processing

**Normal processing is ordered by Kafka offset within each pipeline destination and source topic partition.** There is no global ordering across partitions, tables, topics, or pipelines, and no guarantee that a source transaction spanning records is applied atomically at the destination.

1. Preserve ingestion order and stage only contiguous offsets for each partition. Store the original topic, partition, offset, source epoch, and source position.
2. A single fenced owner dispatches the partition's lowest unresolved record. At most one normal request is intentionally in flight per partition.
3. A retryable failure blocks later records in that partition. Other partitions may continue independently.
4. Advance normal delivery only after a durable acknowledgement, an explicitly classified non-delivery control record, or the atomic exhaustion/quarantine transition described below.
5. Kafka topic recreation, partition-count changes, key-strategy changes, or connector rebootstrap require an explicit epoch/migration decision. They are not transparent ordering-preserving operations.

Partition-level ordered processing is a scheduler guarantee, **not an unconditional guarantee of destination side-effect order**. A timed-out request may still be executing remotely; lease expiry or restart cannot cancel it. A late duplicate can arrive after a newer event, and redrive intentionally occurs outside the original position. Destinations requiring monotonic state must combine idempotency with per-key version/position checks or reject stale updates. Updatis documents and exposes source coordinates for that purpose.

Kafka keys must be preserved and the supported key/partition strategy documented. Tables lacking a usable key are rejected initially unless an explicit, tested table-level strategy is added. No promise of entity ordering survives an uncoordinated repartition.

## Conditions for advancing past an exhausted record

The default policy is **quarantine and continue after durable recording**. This is an explicit configured behavior, not a silent skip. The attempts and retry-age limits are persisted policy values; exact defaults must be selected and tested in the foundation gate.

Advancement requires all of the following:

1. Durable evidence of the terminal decision: attempt count/age exhausted or a named permanent error classification. Attempt intent is committed before each send; crash recovery cannot reset the budget.
2. The active partition lease/fencing generation is still valid for the state transition.
3. One metadata transaction writes/updates the DLQ entry, writes the ordering-gap record, marks the normal delivery terminal, appends its audit outcome, and advances the partition delivery cursor.
4. The transaction commits successfully. A storage outage, failed gap insert, stale lease, or uncertain commit leaves the partition blocked until its durable state can be reconciled.

Malformed or unsupported input follows the same durable quarantine/gap principle, without pretending an HTTP request occurred. If normalization cannot assign an event identity, use original stream epoch/topic/partition/offset plus raw hash as a quarantine identity. Intake may durably stage that quarantine decision and commit its contiguous intake checkpoint, but it must not advance the delivery cursor past earlier unresolved records. Finalize the ordering-gap advancement only when the quarantined record reaches the delivery head.

A known Kafka tombstone or explicitly supported control/heartbeat record may advance without a delivery gap if its exclusion is part of the documented capture contract and its classification is counted. An unrecognized operation is not silently treated as a control record. Snapshot `r` represents data and must be delivered under the snapshot policy.

There is no v0.1 manual “skip without record” feature. Bulk destructive actions are not equivalent to exhaustion and require a separate future authorization/design.

## Ordering-gap recording and visibility

Each gap records pipeline ID, source epoch, topic/partition/offset, event/quarantine ID, destination revision, reason/stage, first/last failure time, budget/attempt summary, DLQ link, and the normal-processing advancement timestamp. Store enough source position/key metadata to diagnose affected entities, subject to payload-access controls.

- v0.1 provides authenticated API/CLI inspection and a gap counter plus structured log entry. Status shows both that processing is advancing and that delivery is degraded by gaps.
- v0.2 adds dashboard filters, visible unresolved-gap counts, partition detail, links to attempts/DLQ, and alerts for new gaps and rising oldest-failure age.
- Redrive success adds `recovered_late` plus job/acknowledgement references. It does **not** delete the gap or relabel the original normal delivery successful.
- Gap/audit metadata outlives pruned payloads. Retention must expose when payload expiry makes a gap no longer redrivable. Operators must not mistake “recovered late” for “original order preserved.”

Acknowledged, quarantined, pending, and recovered-late counts remain separate. A health endpoint can be live while a pipeline is degraded; dashboards must not collapse those states.

## Event identity and destination idempotency

Assign an immutable ID from pipeline/source stream epoch and original Kafka topic/partition/offset using a documented canonical encoding/hash. An inbox uniqueness constraint deduplicates the same captured record. Store source position separately. Connector-generated duplicates at different Kafka offsets may have different IDs; v0.1 does not claim semantic deduplication across resnapshots or arbitrary connector re-emissions. Any later source-coordinate identity scheme needs connector-specific proofs and migration rules.

Deliver `event_id` in the envelope and a documented idempotency header. Identity is stable across retries and unchanged-event DLQ redrive. A recovery attempt gets a new attempt/job ID, never a silently changed original event ID. Historical replay must preserve original coordinates and identity even if routed through another stream.

Destinations are expected to:

- Atomically persist the idempotency key with the business effect, ideally in the same transaction.
- Return success for an already applied event without repeating its effect.
- Retain deduplication state for at least the published retry/redrive/replay horizon; report limitations if they cannot.
- Use per-key source/version checks if late arrivals would overwrite newer state. An older redriven update must not blindly replace a newer row.
- Return success only after durable application or durable acceptance into a destination-owned queue. A `202` acknowledgement proves only that declared acceptance boundary, not completion of downstream business work.

Updatis cannot enforce idempotency inside a remote service. A timeout can represent a successful side effect with a lost response. Exactly-once business effects require destination cooperation beyond this product contract.

## Webhook acknowledgement, retries, and outages

The initial success class is HTTP 2xx. Redirect following is disabled by default. Retry connection/timeouts, 408, 429, and 5xx under bounded policy; other non-2xx responses are terminal by default unless an explicitly validated destination policy says otherwise. Honoring `Retry-After` is capped by the remaining retry-age budget. Test the final classification table before release.

Use persisted exponential backoff with jitter, connect/read/total timeouts, maximum request and response sizes, maximum attempts, and maximum retry age. Record ambiguous outcomes separately. Redact credentials, response bodies, and row data from default logs. Attempts, delays, and classifications must be observable and survive restarts.

During outages, the partition head retries and later records wait. Bounded ingestion can continue into the inbox. When terminal policy is reached, the durable DLQ/gap transaction permits later normal delivery. A sustained outage may therefore create multiple gaps; alert and allow explicit pipeline pause rather than calling this recovery. A pause does not erase retry state or source-retention obligations.

## DLQ inspection and redrive

DLQ entries contain immutable original payload/reference, source coordinates, event/quarantine identity, destination revision, error classification, attempt history references, gap link, and redrive eligibility. Inspection requires authorization. Payloads may contain sensitive source data; the default list view shows metadata, with explicit access for raw values and export.

v0.1 supports listing, filtering, details, and documented diagnosis. It has **no supported redrive mutation** until v0.2's recovery workflow passes tests. Operators must not edit queue SQL as an unofficial recovery interface.

v0.2 redrive behavior:

1. Preview a bounded selection and show count, original destination revision, age, reason, payload availability, and ordering warning.
2. Validate that the error is corrected, required payload/configuration is available, the destination is authorized, and the selection is not already in an active recovery job.
3. Authorize an audited job with an explicit new retry budget. Preserve the original DLQ entry, gap, event ID, and normal cursor.
4. Execute in a separate rate-limited recovery lane. Initially pause normal dispatch for affected partitions while each redrive request is active to avoid intentional concurrent sends; this still cannot undo already delivered later offsets or remote timed-out requests.
5. Record every attempt and terminal result. On success mark the gap recovered late. On failure retain quarantine and job failure; repeated redrive requires another deliberate action.

**Redrive does not restore original event order.** If normal records 10 and 12 were delivered after record 11 was quarantined, later redriving 11 cannot recreate the original sequence. Receivers may need a state reconciliation or rebuild instead. This warning appears in API documentation, CLI preview/confirmation, and dashboard recovery UI.

Unparseable records require a supported parser/configuration correction before redrive; arbitrary editing of raw payloads is not an initial feature. A transformed/corrected event is a new, explicitly linked event with a new identity, not an invisible mutation. Expired payloads cannot be redriven unless an authorized, verified reconstruction from retained history succeeds.

## Offset ownership and controlled replay

Debezium/Connect owns source positions and its offset topics. The Updatis ingest group owns Kafka intake offsets. The delivery cursor owns webhook progress. These are three separate checkpoints.

Commit Kafka next offsets only after contiguous staging/quarantine for that partition. After a crash between metadata persistence and Kafka commit, deduplicate on re-ingestion. Never commit past a persistence failure. Rebalance handling stops revoked partition work and commits only safe progress; leases fence local delivery state.

v0.3 historical replay uses a separate group/job with explicit retained offset/time bounds, rate limits, destination/configuration revision, and original IDs. It does not casually reset the live consumer group or overwrite normal delivery history. A destination may deduplicate replayed events; “reapply effects” needs an explicit separate policy, not a hidden new ID.

Live offset changes require pause/drain, preview of skipped/repeated ranges, backup/checkpoint evidence, authorization, audit, and post-operation validation. Moving backward may merely hit inbox deduplication and is not a redrive mechanism. Moving forward across uncaptured records requires durable gap accounting. Reject requests outside retained history rather than silently starting at earliest/latest.

## Schema evolution, restarts, and backpressure

- A versioned envelope includes operation, key, schema/table, before/after, timestamps, source coordinates, and snapshot metadata. v0.1 supports an explicit tested SQL type/key subset; unknown versions/types are quarantined with a visible gap.
- Additive nullable changes can be accepted only where lossless mapping is tested. Drops, type changes, primary-key changes, and deletes with unavailable before-images need explicit policies. Do not promise generic DDL replication or full before-images without source prerequisites.
- Connector restart preserves its identity, slot/publication, Connect offsets, and Kafka history. Rebootstrap creates a visible epoch and may produce new identities. Missing WAL/history requires explicit operator recovery, not silent offset reset.
- Configure inbox capacity thresholds and per-partition/concurrency limits. Pause intake under pressure while maintaining Kafka group liveness; alert on backlog age, storage, Kafka retention headroom, and source WAL. Pausing intake does not automatically stop source capture.
- Graceful shutdown stops new claims, bounds in-flight waiting, persists safe outcomes, and relinquishes ownership. Unknown remote outcomes remain retryable/ambiguous. Force termination must be covered by crash tests, not just `finally` blocks.

## Required proof before claiming compliance

Test snapshot plus create/update/delete, duplicate intake, ambiguous HTTP acknowledgement, retry-budget persistence, poison record, exhaustion/gap atomicity, metadata outage, slow receiver, partition isolation, rebalances, stale leases, crashes before/after each checkpoint, and graceful/forced shutdown. Assert that a partition never intentionally sends N+1 while N remains nonterminal. Assert that gap creation precedes advancement and redrive leaves original order/history intact. Test retention exhaustion and checkpoint reconciliation on restore before deployment-readiness claims.
