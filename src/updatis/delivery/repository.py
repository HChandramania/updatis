from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator, Mapping
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from updatis.delivery.models import (
    DEFAULT_ACQUISITION_LIMIT,
    DEFAULT_CANDIDATE_LIMIT,
    MAX_CANDIDATE_LIMIT,
    LaneInvariantFailure,
    LEASE_DURATION_SECONDS,
    LEASE_RENEWAL_THRESHOLD_SECONDS,
    MAX_ACQUISITION_LIMIT,
    MAX_FENCING_GENERATION,
    MAX_OWNER_LENGTH,
    AcquisitionResult,
    DeliveryClaim,
    LeaseMutationResult,
    LeaseOutcome,
    MaterializedDelivery,
)


_LANE_CANDIDATES = """
    SELECT id, pipeline_id, source_stream_epoch, topic, partition
    FROM delivery_lanes
    ORDER BY last_examined_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT :candidate_limit
"""

_LANE_HEAD = """
    SELECT id FROM deliveries
    WHERE lane_id=:lane AND state IN ('pending', 'attempting')
    ORDER BY source_offset, id LIMIT 1
"""

_LOCKED_HEAD = f"""
    WITH head AS MATERIALIZED ({_LANE_HEAD})
    SELECT d.*, d.source_offset AS offset_value,
           (d.lease_owner IS NULL OR d.lease_expires_at <= clock_timestamp()) AS available
    FROM head h JOIN deliveries d ON d.id=h.id
    FOR UPDATE OF d SKIP LOCKED
"""

_ACTIVE_LANE_LEASE = """
    SELECT EXISTS (
        SELECT 1 FROM deliveries
        WHERE lane_id=:lane AND lease_owner IS NOT NULL
          AND lease_expires_at > clock_timestamp()
    )
"""


class DeliveryLeaseRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def _transaction(self, connection: Connection | None) -> Iterator[Connection]:
        if connection is not None:
            yield connection
            return
        with self.engine.begin() as owned:
            yield owned

    @staticmethod
    def _bound(connection: Connection) -> None:
        connection.execute(text("SET LOCAL lock_timeout='5s'"))
        connection.execute(text("SET LOCAL statement_timeout='10s'"))
        connection.execute(text("SET LOCAL transaction_timeout='15s'"))

    @staticmethod
    def _valid_owner(owner_id: str) -> bool:
        return isinstance(owner_id, str) and 1 <= len(owner_id) <= MAX_OWNER_LENGTH

    @staticmethod
    def _claim(row: Mapping[str, object]) -> DeliveryClaim:
        return DeliveryClaim(
            delivery_id=UUID(str(row["id"])), pipeline_id=UUID(str(row["pipeline_id"])),
            event_id=row["event_id"], configuration_revision_id=UUID(str(row["configuration_revision_id"])),
            source_stream_epoch=UUID(str(row["source_stream_epoch"])), topic=row["topic"],
            partition=int(row["partition"]), offset=int(row["offset_value"]),
            owner_id=row["lease_owner"], fencing_generation=int(row["fencing_generation"]),
            lease_expires_at=row["lease_expires_at"],
        )

    def materialize_delivery(
        self, *, event_id: str, configuration_revision_id: UUID,
        max_attempts: int, max_age_seconds: int,
    ) -> MaterializedDelivery:
        if max_attempts <= 0 or max_age_seconds <= 0:
            raise ValueError("delivery budgets must be positive")
        with self.engine.begin() as connection:
            self._bound(connection)
            inserted = connection.execute(text("""
                INSERT INTO deliveries
                    (pipeline_id, event_id, configuration_revision_id, state,
                     max_attempts, max_age_seconds)
                SELECT e.pipeline_id, e.id, cr.id, 'pending', :max_attempts, :max_age
                FROM events e
                JOIN configuration_revisions cr ON cr.id=:revision AND cr.pipeline_id=e.pipeline_id
                WHERE e.id=:event
                ON CONFLICT (event_id, configuration_revision_id) DO NOTHING
                RETURNING id
            """), {"event": event_id, "revision": configuration_revision_id,
                    "max_attempts": max_attempts, "max_age": max_age_seconds}).scalar_one_or_none()
            if inserted is not None:
                return MaterializedDelivery(UUID(str(inserted)), True)
            existing = connection.execute(text("""
                SELECT id, max_attempts, max_age_seconds FROM deliveries
                WHERE event_id=:event AND configuration_revision_id=:revision
            """), {"event": event_id, "revision": configuration_revision_id}).mappings().one_or_none()
            if existing is None:
                raise ValueError("event and pinned configuration revision must exist in the same pipeline")
            if (int(existing["max_attempts"]), int(existing["max_age_seconds"])) != (max_attempts, max_age_seconds):
                raise ValueError("existing delivery budgets differ from materialization request")
            return MaterializedDelivery(UUID(str(existing["id"])), False)

    def acquire_heads(
        self, *, owner_id: str, limit: int = DEFAULT_ACQUISITION_LIMIT,
        candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
        connection: Connection | None = None,
    ) -> AcquisitionResult:
        if not self._valid_owner(owner_id):
            return AcquisitionResult(LeaseOutcome.INVALID_REQUEST, reason="owner_id must contain 1 to 128 characters")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_ACQUISITION_LIMIT:
            return AcquisitionResult(LeaseOutcome.INVALID_REQUEST, reason="acquisition limit must be between 1 and 100")
        if (not isinstance(candidate_limit, int) or isinstance(candidate_limit, bool)
                or not limit <= candidate_limit <= MAX_CANDIDATE_LIMIT):
            return AcquisitionResult(LeaseOutcome.INVALID_REQUEST,
                                     reason="candidate limit must be between acquisition limit and 100")
        with self._transaction(connection) as current:
            self._bound(current)
            lanes = current.execute(text(_LANE_CANDIDATES),
                                    {"candidate_limit": candidate_limit}).mappings().all()
            claims: list[DeliveryClaim] = []
            failures: list[LaneInvariantFailure] = []
            for lane in lanes:
                current.execute(text("""
                    UPDATE delivery_lanes SET last_examined_at=clock_timestamp() WHERE id=:lane
                """), {"lane": lane["id"]})
                if current.execute(text(_ACTIVE_LANE_LEASE), {"lane": lane["id"]}).scalar_one():
                    continue
                # Pick the true head without skipping locks, then lock ONLY that
                # identity. A locked N can never expose N+1.
                row = current.execute(text(_LOCKED_HEAD),
                                      {"lane": lane["id"]}).mappings().one_or_none()
                if row is None or not row["available"]:
                    continue
                blocked = current.execute(text("""
                    SELECT EXISTS (SELECT 1 FROM intake_discontinuities
                    WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch
                      AND topic=:topic AND partition=:partition AND offset_value < :offset)
                """), {"pipeline": lane["pipeline_id"], "epoch": lane["source_stream_epoch"],
                        "topic": lane["topic"], "partition": lane["partition"],
                        "offset": row["source_offset"]}).scalar_one()
                if blocked:
                    continue
                if row["fencing_generation"] == MAX_FENCING_GENERATION:
                    failures.append(LaneInvariantFailure(
                        lane_id=lane["id"], delivery_id=row["id"], pipeline_id=lane["pipeline_id"],
                        source_stream_epoch=lane["source_stream_epoch"], topic=lane["topic"],
                        partition=lane["partition"], reason="fencing generation cannot be incremented",
                    ))
                    continue
                if len(claims) == limit:
                    continue
                acquired = current.execute(text("""
                    UPDATE deliveries SET lease_owner=:owner,
                        lease_expires_at=clock_timestamp() + make_interval(secs => :duration),
                        fencing_generation=fencing_generation + 1, updated_at=clock_timestamp()
                    WHERE id=:id AND state IN ('pending', 'attempting')
                      AND (lease_owner IS NULL OR lease_expires_at <= clock_timestamp())
                      AND fencing_generation < :maximum
                    RETURNING *, source_offset AS offset_value
                """), {"id": row["id"], "owner": owner_id, "duration": LEASE_DURATION_SECONDS,
                        "maximum": MAX_FENCING_GENERATION}).mappings().one_or_none()
                if acquired is not None:
                    claims.append(self._claim(dict(acquired) | {
                        key: lane[key] for key in ("source_stream_epoch", "topic", "partition")
                    }))
            outcome = (LeaseOutcome.PARTIAL if claims and failures else
                       LeaseOutcome.INVARIANT_VIOLATION if failures else
                       LeaseOutcome.ACQUIRED if claims else LeaseOutcome.NO_ELIGIBLE_WORK)
            return AcquisitionResult(outcome, tuple(claims), failures=tuple(failures))

    def renew_claim(self, delivery_id: UUID, owner_id: str, fencing_generation: int) -> LeaseMutationResult:
        if (not self._valid_owner(owner_id) or not isinstance(fencing_generation, int)
                or isinstance(fencing_generation, bool) or fencing_generation <= 0):
            return LeaseMutationResult(LeaseOutcome.INVALID_REQUEST, reason="invalid owner or fencing generation")
        with self.engine.begin() as connection:
            self._bound(connection)
            # Serialize extension of an existing lease with acquisition of any
            # newly materialized lower head in the same lane.
            connection.execute(text("""
                SELECT l.id FROM delivery_lanes l
                JOIN deliveries d ON d.lane_id=l.id
                WHERE d.id=:id FOR UPDATE OF l
            """), {"id": delivery_id}).scalar_one_or_none()
            row = connection.execute(text("""
                UPDATE deliveries
                SET lease_expires_at=clock_timestamp() + make_interval(secs => :duration),
                    updated_at=clock_timestamp()
                WHERE id=:id AND lease_owner=:owner AND fencing_generation=:generation
                  AND lease_expires_at > clock_timestamp()
                  AND lease_expires_at <= clock_timestamp() + make_interval(secs => :threshold)
                RETURNING id, pipeline_id, event_id, configuration_revision_id,
                          lease_owner, lease_expires_at, fencing_generation
            """), {"id": delivery_id, "owner": owner_id, "generation": fencing_generation,
                    "duration": LEASE_DURATION_SECONDS,
                    "threshold": LEASE_RENEWAL_THRESHOLD_SECONDS}).mappings().one_or_none()
            if row is not None:
                return LeaseMutationResult(LeaseOutcome.RENEWED,
                                            self._load_claim_coordinates(connection, dict(row)))
            return self._classify_failed_mutation(connection, delivery_id, owner_id,
                                                  fencing_generation, too_early_is_invalid=True)

    def complete_claim(self, delivery_id: UUID, owner_id: str,
                       fencing_generation: int) -> LeaseMutationResult:
        if (not self._valid_owner(owner_id) or not isinstance(fencing_generation, int)
                or isinstance(fencing_generation, bool) or fencing_generation <= 0):
            return LeaseMutationResult(LeaseOutcome.INVALID_REQUEST, reason="invalid owner or fencing generation")
        with self.engine.begin() as connection:
            self._bound(connection)
            row = connection.execute(text("""
                UPDATE deliveries
                SET lease_owner=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
                WHERE id=:id AND lease_owner=:owner AND fencing_generation=:generation
                  AND lease_expires_at > clock_timestamp()
                RETURNING id
            """), {"id": delivery_id, "owner": owner_id,
                    "generation": fencing_generation}).mappings().one_or_none()
            if row is not None:
                # The returned expiry is null after clearing. Scheduling completion
                # intentionally returns no active claim and changes no delivery outcome.
                return LeaseMutationResult(LeaseOutcome.COMPLETED)
            return self._classify_failed_mutation(connection, delivery_id, owner_id, fencing_generation)

    def _load_claim_coordinates(self, connection: Connection, row: dict) -> DeliveryClaim:
        coordinates = connection.execute(text("""
            SELECT ir.source_stream_epoch, ir.topic, ir.partition, ir.offset_value
            FROM events e JOIN ingest_records ir
              ON ir.id=e.ingest_record_id AND ir.pipeline_id=e.pipeline_id
            WHERE e.id=:event AND e.pipeline_id=:pipeline
        """), {"event": row["event_id"], "pipeline": row["pipeline_id"]}).mappings().one()
        return self._claim(row | dict(coordinates))

    def _classify_failed_mutation(
        self, connection: Connection, delivery_id: UUID, owner_id: str,
        fencing_generation: int, *, too_early_is_invalid: bool = False,
    ) -> LeaseMutationResult:
        row = connection.execute(text("""
            SELECT lease_owner, fencing_generation,
                   lease_expires_at IS NOT NULL AND lease_expires_at <= clock_timestamp() AS expired
            FROM deliveries WHERE id=:id
        """), {"id": delivery_id}).mappings().one_or_none()
        if row is None:
            return LeaseMutationResult(LeaseOutcome.INVARIANT_VIOLATION,
                                       reason="delivery does not exist")
        if row["lease_owner"] != owner_id or int(row["fencing_generation"]) != fencing_generation:
            return LeaseMutationResult(LeaseOutcome.STALE)
        if row["expired"]:
            return LeaseMutationResult(LeaseOutcome.EXPIRED)
        if too_early_is_invalid:
            return LeaseMutationResult(LeaseOutcome.INVALID_REQUEST,
                                       reason="lease is not within the renewal threshold")
        return LeaseMutationResult(LeaseOutcome.STALE)
