from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from updatis.capture.manifest import SourceColumn, SourceSchemaManifest
from updatis.normalization.identity import event_id
from updatis.normalization.normalize import normalize_record
from updatis.normalization.temporal import TemporalValueError, normalize_temporal

PIPELINE = UUID("00000000-0000-0000-0000-000000000001")
EPOCH = UUID("00000000-0000-0000-0000-000000000101")
TOPIC = "updatis.00000000000000000000000000000001.00000000000000000000000000000101.example.orders"


@pytest.fixture
def manifest() -> SourceSchemaManifest:
    return SourceSchemaManifest("example", "orders", (
        SourceColumn("id", 1, "bigint", False, 1, "int64"),
        SourceColumn("amount", 2, "numeric(20,4)", False, None, "string"),
        SourceColumn("payload", 3, "jsonb", True, None, "string", "io.debezium.data.Json"),
        SourceColumn("local_time", 4, "time without time zone", False, None, "string", "io.debezium.time.IsoTime"),
        SourceColumn("local_stamp", 5, "timestamp without time zone", False, None, "string", "io.debezium.time.IsoTimestamp"),
        SourceColumn("instant", 6, "timestamp with time zone", False, None, "string", "io.debezium.time.ZonedTimestamp"),
    ))


def record_bytes(manifest: SourceSchemaManifest, op: str = "c", before=None, after_marker=True) -> tuple[bytes, bytes]:
    fields = [
        {"field": item.name, "type": item.debezium_type, "name": item.debezium_logical_type,
         "optional": item.nullable}
        for item in manifest.columns
    ]
    after = None if not after_marker else {
        "id": 7, "amount": "0012.3400", "payload": '{"ok":true}', "local_time": "01:02:03.123456Z",
        "local_stamp": "1969-12-31T23:59:59.123456Z", "instant": "2024-11-03T01:30:00-04:00",
    }
    key = {"schema": {"type": "struct", "fields": [fields[0]]}, "payload": {"id": 7}}
    value = {
        "schema": {"type": "struct", "fields": [
            {"field": "before", "type": "struct", "fields": fields},
            {"field": "after", "type": "struct", "fields": fields},
        ]},
        "payload": {"before": before, "after": after,
                    "source": {"db": "example_source", "schema": "example", "table": "orders",
                               "lsn": 42, "snapshot": "true" if op == "r" else "false",
                               "ts_ms": 1730611800123, "txId": 9},
                    "op": op, "ts_ms": 1730611801123},
    }
    return json.dumps(key).encode(), json.dumps(value).encode()


@pytest.mark.parametrize(("op", "operation"), [("r", "read"), ("c", "create"), ("u", "update")])
def test_supported_events_normalize_with_manifest(manifest: SourceSchemaManifest, op: str, operation: str) -> None:
    key, value = record_bytes(manifest, op)
    result = normalize_record(pipeline_id=PIPELINE, stream_epoch=EPOCH, topic=TOPIC, partition=0,
                              offset=3, key=key, value=value, manifest=manifest,
                              ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert result.classification == "event"
    assert result.envelope["operation"] == operation
    assert result.envelope["after"]["amount"] == "12.34"
    assert result.envelope["after"]["local_time"] == "01:02:03.123456"
    assert result.envelope["after"]["local_stamp"] == "1969-12-31T23:59:59.123456"
    assert result.envelope["after"]["instant"] == "2024-11-03T05:30:00Z"
    assert result.envelope["occurred_at"] == "2024-11-03T05:30:00.123000Z"


def test_delete_without_before_is_supported(manifest: SourceSchemaManifest) -> None:
    key, value = record_bytes(manifest, "d", before=None, after_marker=False)
    result = normalize_record(pipeline_id=PIPELINE, stream_epoch=EPOCH, topic=TOPIC, partition=0,
                              offset=4, key=key, value=value, manifest=manifest)
    assert result.classification == "event" and result.envelope["before"] is None
    assert result.envelope["after"] is None


def test_tombstone_is_control_not_event(manifest: SourceSchemaManifest) -> None:
    key, _ = record_bytes(manifest)
    result = normalize_record(pipeline_id=PIPELINE, stream_epoch=EPOCH, topic=TOPIC, partition=0,
                              offset=5, key=key, value=None, manifest=manifest)
    assert result.classification == "control" and result.event_id is None


def test_schema_omission_becomes_discontinuity(manifest: SourceSchemaManifest) -> None:
    key, value = record_bytes(manifest)
    document = json.loads(value)
    document["schema"]["fields"][1]["fields"].pop()
    result = normalize_record(pipeline_id=PIPELINE, stream_epoch=EPOCH, topic=TOPIC, partition=0,
                              offset=6, key=key, value=json.dumps(document).encode(), manifest=manifest)
    assert result.classification == "quarantine"
    assert result.reason_code == "malformed_record"


def test_oversized_raw_record_keeps_bounded_prefix_and_complete_lengths(manifest: SourceSchemaManifest) -> None:
    value = b"x" * 1_048_577
    result = normalize_record(pipeline_id=PIPELINE, stream_epoch=EPOCH, topic=TOPIC, partition=0,
                              offset=7, key=b"k", value=value, manifest=manifest)
    assert result.reason_code == "raw_record_too_large"
    assert result.raw_value_length == len(value)
    assert len(result.raw_value_prefix) == 262_144 and result.raw_value_truncated
    assert len(result.raw_hash) == 64 and len(result.discontinuity_id) == 64


def test_identity_is_coordinate_and_epoch_stable() -> None:
    first = event_id(PIPELINE, EPOCH, TOPIC, 0, 8)
    assert first == event_id(PIPELINE, EPOCH, TOPIC, 0, 8)
    assert first != event_id(PIPELINE, UUID(int=EPOCH.int + 1), TOPIC, 0, 8)
    assert first != event_id(PIPELINE, EPOCH, TOPIC, 0, 9)
    assert len(first) == 64 and first == first.lower()


def test_temporal_contract_precision_offsets_dst_and_infinity() -> None:
    assert normalize_temporal("time", "io.debezium.time.IsoTime", "23:59:59.999999Z") == "23:59:59.999999"
    assert normalize_temporal("timestamp", "io.debezium.time.IsoTimestamp",
                              "1960-01-02T03:04:05.000001Z") == "1960-01-02T03:04:05.000001"
    assert normalize_temporal("timetz", "io.debezium.time.ZonedTime", "01:30:00-04:00") == "01:30:00-04:00"
    assert normalize_temporal("timestamptz", "io.debezium.time.ZonedTimestamp",
                              "2024-11-03T01:30:00-05:00") == "2024-11-03T06:30:00Z"
    with pytest.raises(TemporalValueError, match="infinity"):
        normalize_temporal("timestamptz", "io.debezium.time.ZonedTimestamp", "infinity")
