from __future__ import annotations

from unittest.mock import Mock

from updatis.delivery.models import (
    DEFAULT_ACQUISITION_LIMIT, LEASE_DURATION_SECONDS, LEASE_RENEWAL_THRESHOLD_SECONDS,
    MAX_ACQUISITION_LIMIT, MAX_OWNER_LENGTH, LeaseOutcome,
)
from updatis.delivery.repository import DeliveryLeaseRepository


def test_approved_lease_defaults_are_typed_and_bounded() -> None:
    assert LEASE_DURATION_SECONDS == 30
    assert LEASE_RENEWAL_THRESHOLD_SECONDS == 10
    assert DEFAULT_ACQUISITION_LIMIT == 25
    assert MAX_ACQUISITION_LIMIT == 100
    assert MAX_OWNER_LENGTH == 128
    assert all(isinstance(value, int) for value in (
        LEASE_DURATION_SECONDS, LEASE_RENEWAL_THRESHOLD_SECONDS,
        DEFAULT_ACQUISITION_LIMIT, MAX_ACQUISITION_LIMIT, MAX_OWNER_LENGTH,
    ))


def test_invalid_acquisition_requests_do_not_touch_storage() -> None:
    engine = Mock()
    repository = DeliveryLeaseRepository(engine)
    for owner, limit in (("", 1), ("x" * 129, 1), ("worker", 0), ("worker", -1),
                         ("worker", 101), ("worker", True)):
        result = repository.acquire_heads(owner_id=owner, limit=limit)
        assert result.outcome is LeaseOutcome.INVALID_REQUEST
    engine.begin.assert_not_called()


def test_invalid_candidate_limits_do_not_touch_storage() -> None:
    engine = Mock()
    repository = DeliveryLeaseRepository(engine)
    for candidate_limit in (0, -1, 101, True, "2", 1):
        result = repository.acquire_heads(owner_id="worker", limit=2, candidate_limit=candidate_limit)
        assert result.outcome is LeaseOutcome.INVALID_REQUEST
    engine.begin.assert_not_called()


def test_invalid_mutation_fences_do_not_touch_storage() -> None:
    engine = Mock()
    repository = DeliveryLeaseRepository(engine)
    for generation in (0, -1, True, "1"):
        assert repository.renew_claim(Mock(), "worker", generation).outcome is LeaseOutcome.INVALID_REQUEST
        assert repository.complete_claim(Mock(), "worker", generation).outcome is LeaseOutcome.INVALID_REQUEST
    engine.begin.assert_not_called()
