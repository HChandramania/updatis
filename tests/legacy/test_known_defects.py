from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pymysql
import pytest

from app.core.settings import load_settings
from app.repositories.notification_event_repository import QueuedNotificationEvent
from app.services.notification_delivery_service import NotificationDeliveryService
from app.services.notification_event_factory import (
    NotificationEventParseError,
    build_notification_message,
)
from app.workers import kafka_consumer


KNOWN_DEFECT = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Known legacy defect; this is evidence, not supported Updatis behavior",
)


def test_known_defect_marker_only_accepts_assertion_failures() -> None:
    assert KNOWN_DEFECT.kwargs["raises"] is AssertionError


@pytest.mark.legacy_defect
@KNOWN_DEFECT
def test_legacy_consumer_does_not_commit_past_failed_record(monkeypatch) -> None:
    attempted: list[int] = []
    committed_next_offsets: list[int] = []

    class FakeConsumer:
        current_offset = -1

        def __init__(self, *_args, **_kwargs):
            pass

        def __iter__(self):
            for offset in (0, 1):
                self.current_offset = offset
                yield SimpleNamespace(topic="legacy", partition=0, offset=offset, value=b"{}")

        def commit(self):
            committed_next_offsets.append(self.current_offset + 1)

        def close(self):
            pass

    class FakeDatabase:
        def close(self):
            pass

    def store(_database, _payload, **coordinates):
        attempted.append(coordinates["offset"])
        if coordinates["offset"] == 0:
            raise pymysql.OperationalError("synthetic persistence failure")

    monkeypatch.setattr(kafka_consumer, "KafkaConsumer", FakeConsumer)
    monkeypatch.setattr(kafka_consumer, "_connect_to_database", lambda _settings: FakeDatabase())
    monkeypatch.setattr(kafka_consumer, "_store_notification_event", store)
    monkeypatch.setattr(kafka_consumer, "_record_consumer_lag", lambda _consumer: None)
    monkeypatch.setattr(kafka_consumer, "configure_logging", lambda _path: None)
    monkeypatch.setattr(kafka_consumer.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(kafka_consumer.logger, "exception", lambda *_args, **_kwargs: None)

    kafka_consumer.start_consumer()

    assert attempted == [0]
    assert committed_next_offsets == []


@pytest.mark.legacy_defect
@KNOWN_DEFECT
def test_legacy_claim_persists_incremented_attempt_count() -> None:
    source = inspect.getsource(
        __import__(
            "app.repositories.notification_event_repository", fromlist=["NotificationEventRepository"]
        ).NotificationEventRepository.claim_due_events
    )
    update_section = source.split("UPDATE notification_events", 1)[1].split('"""', 1)[0]
    assert "attempts = attempts + 1" in update_section


@pytest.mark.legacy_defect
@KNOWN_DEFECT
def test_legacy_zero_recipient_broadcast_is_not_marked_delivered() -> None:
    class Repository:
        delivered: list[str] = []

        async def mark_delivered(self, event_id: str):
            self.delivered.append(event_id)

    class Manager:
        async def broadcast(self, _message):
            return 0

    settings = load_settings()
    repository = Repository()
    service = NotificationDeliveryService(repository, Manager(), settings)
    payload = {
        "schema_version": 1,
        "event_id": "a" * 64,
        "event_type": "order_change",
        "action": "INSERT",
        "order_id": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": {"topic": "legacy", "partition": 0, "offset": 1},
        "new_data": {"id": 1},
    }
    event = QueuedNotificationEvent(
        event_id="a" * 64, schema_version=1, event_type="order_change", action="INSERT",
        source_topic="legacy", source_partition=0, source_offset=1,
        payload_json=json.dumps(payload), attempts=1,
    )

    delivered = asyncio.run(service._deliver_event(event))

    assert delivered is False
    assert repository.delivered == []


@pytest.mark.legacy_defect
@KNOWN_DEFECT
def test_legacy_snapshot_read_operation_is_accepted() -> None:
    outcome = None
    try:
        message = build_notification_message(
            {
                "payload": {
                    "op": "r", "before": None, "after": {"id": 1},
                    "source": {"name": "legacy", "db": "db", "table": "orders"},
                    "ts_ms": 1700000000000,
                }
            },
            topic="legacy.orders", partition=0, offset=0,
            received_at=datetime.now(timezone.utc),
        )
        outcome = message.action
    except NotificationEventParseError as error:
        if str(error) != "Unsupported Debezium operation: 'r'":
            raise
        outcome = str(error)

    assert outcome == "READ"
