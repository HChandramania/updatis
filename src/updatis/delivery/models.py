from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

LEASE_DURATION_SECONDS = 30
LEASE_RENEWAL_THRESHOLD_SECONDS = 10
DEFAULT_ACQUISITION_LIMIT = 25
MAX_ACQUISITION_LIMIT = 100
DEFAULT_CANDIDATE_LIMIT = 100
MAX_CANDIDATE_LIMIT = 100
MAX_FENCING_GENERATION = 9_223_372_036_854_775_807
MAX_OWNER_LENGTH = 128


class LeaseOutcome(StrEnum):
    ACQUIRED = "acquired"
    PARTIAL = "acquired_with_invariant_failures"
    NO_ELIGIBLE_WORK = "no_eligible_work"
    RENEWED = "renewed"
    COMPLETED = "completed"
    STALE = "stale_owner_or_generation"
    EXPIRED = "expired_lease"
    INVALID_REQUEST = "invalid_request"
    INVARIANT_VIOLATION = "invariant_violation"


@dataclass(frozen=True)
class DeliveryClaim:
    delivery_id: UUID
    pipeline_id: UUID
    event_id: str
    configuration_revision_id: UUID
    source_stream_epoch: UUID
    topic: str
    partition: int
    offset: int
    owner_id: str
    fencing_generation: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class LaneInvariantFailure:
    lane_id: UUID
    delivery_id: UUID
    pipeline_id: UUID
    source_stream_epoch: UUID
    topic: str
    partition: int
    reason: str


@dataclass(frozen=True)
class AcquisitionResult:
    outcome: LeaseOutcome
    claims: tuple[DeliveryClaim, ...] = ()
    reason: str | None = None
    failures: tuple[LaneInvariantFailure, ...] = ()


@dataclass(frozen=True)
class LeaseMutationResult:
    outcome: LeaseOutcome
    claim: DeliveryClaim | None = None
    reason: str | None = None


@dataclass(frozen=True)
class MaterializedDelivery:
    delivery_id: UUID
    created: bool
