# v0.1-c / 01G design

Status: implemented design for roadmap phase 01G. This phase adds scheduling ownership only; it does not send or classify webhook requests.

## Ordering and eligibility

A lane is `(pipeline_id, source_stream_epoch, topic, partition)`. Its head is the lowest-offset delivery whose state is `pending` or `attempting`, with delivery ID breaking equal-offset ties. The pinned configuration revision is returned with the claim but does not split the lane. `attempting` and a stored retryable attempt remain unresolved; 01G does not interpret retry due times or implement retry scheduling.

### Design choice and durable ownership

Ranking all unresolved deliveries is result-limited but not work-bounded. Adding an ordering index alone still requires discovering distinct lanes from the backlog. A cached head pointer would require more state-transition maintenance and corruption handling. The smaller chosen design is a durable lane registry with an indexed, derived head; it stores no head pointer.

Forward-only migration `0004_delivery_lanes` follows `0003_partition_leases` unchanged, because 0003 may already have been applied. Forward-only `0005_lane_fairness` changes lane insertion to a finite PostgreSQL-clock position, converts existing `-infinity` positions once, and indexes active leases by lane. 0004 backfills **all** valid existing deliveries from immutable event/ingest coordinates in the migration transaction, then requires non-null lane/offset references. An invalid backfill aborts the migration rather than dropping work. The lane's foreign key uses `(pipelines.id, source_stream_epoch)`, matching the existing ingest identity contract; a previously valid delivery need not have a capture-manifest row.

The metadata-owned `delivery_lanes` table has a UUID primary key and unique `(pipeline_id, source_stream_epoch, topic, partition)` identity. A delivery-insert trigger registers the lane atomically, locks it, and copies the canonical offset into `deliveries.source_offset`. Caller-supplied conflicting coordinates are rejected. Updates cannot change delivery event, pipeline, revision, lane, or offset; lane identity is immutable and lane deletion is rejected. Delivery foreign keys prevent missing registry rows. No process-local lane registry is authoritative. Existing referenced coordinates cannot become stale through supported DML. Empty or fully resolved lanes remain registered and consume candidate capacity; no retirement/cleanup lifecycle is introduced in 01G.

### Bounded acquisition

1. Validate acquisition limit (1 through 100, default 25) and candidate limit (acquisition limit through 100, default 100).
2. In one transaction, select and lock at most `candidate_limit` lanes in `(last_examined_at, id)` order using `delivery_lanes_discovery_idx`. `FOR UPDATE SKIP LOCKED` precedes the effective `LIMIT`, so locked front lanes do not consume the selected page.
3. Update each selected lane's database-clock `last_examined_at` so subsequent calls rotate through the durable registry. Under that lane lock, check `deliveries_lane_active_lease_idx` for any active lease in the lane. This prevents a newly materialized lower offset from gaining a second simultaneous owner.
4. For lanes without an active lease, use `deliveries_lane_head_idx(lane_id, source_offset, id) WHERE state IN ('pending', 'attempting')` to find the true first unresolved ID, without skipping locked delivery rows.
5. Try to lock only that head ID with `SKIP LOCKED`. A locked or actively leased N never exposes N+1. Check earlier discontinuities using their existing unique `(pipeline_id, source_stream_epoch, topic, partition, offset_value)` index.
6. Report an eligible overflowed head as a typed lane-specific invariant failure without changing its delivery row. Acquire up to `limit` healthy heads by atomically incrementing their fence and writing owner/expiry. Return `PARTIAL` when claims and failures coexist. All selected lanes are checked even after claim capacity is reached, so partial outcomes remain visible.

Selection, discovery rotation, and lease mutation commit together. Storage exceptions roll back an owned transaction and propagate; they never return assumed claims. The internal caller-owned connection seam is for transaction composition/testing: results remain provisional until that caller commits, and the caller must roll back on any exception. Independent acquirers serialize on lane/head locks. The deterministic concurrency probe additionally holds a delivery head lock without a lane lock, proving the head lookup cannot skip to its successor while another lane progresses.

The structural bound is on selected candidate lanes and per-lane indexed lookups, independent of unresolved backlog length. Skipping locked lanes can traverse more index entries than the selected limit. It is not a constant bound on physical I/O, index height, MVCC dead tuples, or runtime. Statement/lock/transaction timeouts remain enforced. Newly inserted lanes receive a finite timestamp, so they cannot repeatedly jump ahead of older waiting lanes; ID breaks timestamp ties. A call may return fewer claims or no claims despite work outside its candidate page; callers must not interpret an empty result as global exhaustion. No polling loop is enabled. `last_examined_at` is discovery rotation, not a delivery cursor or retry schedule.

The PostgreSQL probe captures `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for the actual candidate, active-lease, and locked-head queries using 20,001 unresolved deliveries and 2,000 additional registry lanes, with candidate limit 2 and acquisition limit 1. It also captures a candidate plan while two front lanes are held by another connection. It checks the discovery/head/active-lease indexes, `Limit` above `LockRows`, and absence of a global unresolved-delivery sort/window. Observed results belong in the acceptance record; this is not a production performance benchmark.

## Lease lifecycle and fencing

The newly approved provisional defaults are a 30-second lease, renewal at 10 seconds or less remaining, default acquisition limit 25, maximum limit 100, and opaque owner IDs of 1 through 128 characters. A worker should generate one UUID owner ID per process. These defaults were approved for 01G and were not part of the original roadmap.

PostgreSQL `clock_timestamp()` decides acquisition, expiration, and renewal. A successful acquisition writes owner and expiry and atomically increments a nonnegative `BIGINT` fencing generation. Generation zero means never acquired. The maximum `BIGINT` value is reported for that lane before acquisition, so the generation cannot wrap or prevent unrelated selected lanes from being acquired. Renewal takes the same lane lock before checking expiry or extending ownership, preventing a concurrent acquisition from observing an expired lease while its renewal is uncommitted. It requires the exact owner and generation, refuses expired leases, runs only inside the renewal window, and preserves the generation. Expected conflicts return typed outcomes; unexpected database failures propagate and fail closed.

`complete_claim(delivery_id, owner_id, fencing_generation)` is the fenced scheduling-completion boundary. It requires an unexpired exact claim and atomically clears owner and expiry. It does not change delivery state, record an attempt or outcome, advance a cursor, or make N+1 eligible while N remains unresolved. A later acquisition of N increments its generation again.

The migration enforces paired owner/expiry values, owner length, nonnegative generations, and a positive generation for owned rows. Existing delivery foreign keys and state checks continue to apply.

## Materialization and boundaries

`materialize_delivery` is the minimal deterministic insertion primitive for an existing event and immutable configuration revision. The existing `(event_id, configuration_revision_id)` uniqueness makes it idempotent; a repeat with conflicting budgets is rejected. There is no intake fan-out loop. Tombstones, quarantines, and discontinuities do not create deliveries.

01H owns webhook transport and response classification. 01I owns attempt persistence, retry scheduling, terminal outcomes, DLQ transitions, ordering gaps, and cursor advancement. No continuous claim loop is enabled in 01G.
