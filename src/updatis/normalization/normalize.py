from __future__ import annotations

import base64
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from uuid import UUID

from updatis.capture.manifest import SourceColumn, SourceSchemaManifest, canonical_postgres_type
from updatis.normalization.identity import event_id, quarantine_id, raw_hash
from updatis.normalization.temporal import normalize_temporal

RAW_PROCESSING_LIMIT = 1_048_576
NORMALIZED_ENVELOPE_LIMIT = 524_288
RAW_KEY_PREFIX_LIMIT = 65_536
RAW_VALUE_PREFIX_LIMIT = 262_144


@dataclass(frozen=True)
class NormalizedRecord:
    classification: Literal["event", "control", "quarantine"]
    raw_hash: str
    raw_key_prefix: bytes
    raw_value_prefix: bytes | None
    raw_key_length: int
    raw_value_length: int | None
    raw_key_truncated: bool
    raw_value_truncated: bool
    event_id: str | None = None
    envelope: dict[str, Any] | None = None
    discontinuity_id: str | None = None
    discontinuity_kind: Literal["malformed", "unsupported", "oversized"] | None = None
    reason_code: str | None = None
    error_summary: str | None = None


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _base(
    key: bytes | None, value: bytes | None, record_hash: str, **kwargs: Any
) -> NormalizedRecord:
    key_bytes = key or b""
    return NormalizedRecord(
        raw_hash=record_hash,
        raw_key_prefix=key_bytes[:RAW_KEY_PREFIX_LIMIT],
        raw_value_prefix=None if value is None else value[:RAW_VALUE_PREFIX_LIMIT],
        raw_key_length=len(key_bytes),
        raw_value_length=None if value is None else len(value),
        raw_key_truncated=len(key_bytes) > RAW_KEY_PREFIX_LIMIT,
        raw_value_truncated=value is not None and len(value) > RAW_VALUE_PREFIX_LIMIT,
        **kwargs,
    )


def _quarantine(
    *, pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int, offset: int,
    key: bytes | None, value: bytes | None, kind: Literal["malformed", "unsupported", "oversized"],
    reason: str, summary: str,
) -> NormalizedRecord:
    digest = raw_hash(key, value)
    return _base(
        key, value, digest, classification="quarantine",
        discontinuity_id=quarantine_id(pipeline_id, stream_epoch, topic, partition, offset, digest),
        discontinuity_kind=kind, reason_code=reason, error_summary=summary[:2048],
    )


def _field_map(fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item.get("field")): item for item in fields}


def _decimal(value: str) -> str:
    if value.upper() in {"NAN", "INFINITY", "+INFINITY", "-INFINITY"}:
        raise ValueError("non-finite decimal is unsupported")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal string") from exc
    result = format(number, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    if result in {"-0", ""}:
        result = "0"
    return result


def _convert(column: SourceColumn, value: Any) -> Any:
    if value is None:
        if not column.nullable:
            raise ValueError(f"non-nullable column {column.name} is null")
        return None
    pg = canonical_postgres_type(column.postgres_type)
    if pg == "boolean":
        if type(value) is not bool:
            raise ValueError("boolean value has wrong JSON type")
        return value
    if pg in {"smallint", "integer", "bigint"}:
        if type(value) is not int:
            raise ValueError("integer value has wrong JSON type")
        bounds = {"smallint": (-32768, 32767), "integer": (-2147483648, 2147483647),
                  "bigint": (-9223372036854775808, 9223372036854775807)}[pg]
        if not bounds[0] <= value <= bounds[1]:
            raise ValueError("integer value is outside PostgreSQL range")
        return value
    if pg in {"real", "double precision"}:
        if type(value) not in {int, float} or not math.isfinite(value):
            raise ValueError("floating value must be finite")
        return value
    if pg in {"numeric", "decimal"}:
        if not isinstance(value, str):
            raise ValueError("decimal must use Debezium string encoding")
        return _decimal(value)
    if pg in {"character", "char", "character varying", "varchar", "text"}:
        if not isinstance(value, str):
            raise ValueError("text value has wrong JSON type")
        return value
    if pg == "uuid":
        return str(UUID(str(value))).lower()
    if pg == "bytea":
        if not isinstance(value, str):
            raise ValueError("bytea must use base64 string encoding")
        base64.b64decode(value, validate=True)
        return value
    if pg in {"date", "time", "time without time zone", "timetz", "time with time zone",
              "timestamp", "timestamp without time zone", "timestamptz", "timestamp with time zone"}:
        if not isinstance(value, str):
            raise ValueError("temporal value must use ISO string encoding")
        return normalize_temporal(column.postgres_type, column.debezium_logical_type, value)
    if pg in {"json", "jsonb"}:
        if column.debezium_logical_type != "io.debezium.data.Json" or not isinstance(value, str):
            raise ValueError("JSON value has an unexpected Debezium encoding")
        return json.loads(value)
    if pg.startswith("enum:"):
        if column.debezium_logical_type != "io.debezium.data.Enum" or not isinstance(value, str):
            raise ValueError("enum value has an unexpected Debezium encoding")
        return value
    raise ValueError(f"unsupported PostgreSQL type: {column.postgres_type}")


def _row_fields(envelope_fields: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    field = _field_map(envelope_fields).get(name)
    if not field or field.get("type") != "struct" or not isinstance(field.get("fields"), list):
        raise ValueError(f"Debezium envelope lacks {name} row schema")
    return field["fields"]


def _normalize_row(manifest: SourceSchemaManifest, row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    expected_names = [column.name for column in sorted(manifest.columns, key=lambda item: item.ordinal)]
    if list(row) != expected_names and set(row) != set(expected_names):
        raise ValueError("row fields do not match the source-schema manifest")
    return {column.name: _convert(column, row[column.name]) for column in sorted(manifest.columns, key=lambda item: item.ordinal)}


def normalize_record(
    *, pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int, offset: int,
    key: bytes | None, value: bytes | None, manifest: SourceSchemaManifest,
    ingested_at: datetime | None = None,
) -> NormalizedRecord:
    if partition < 0 or offset < 0:
        raise ValueError("Kafka partition and offset must be non-negative")
    if len(key or b"") + len(value or b"") > RAW_PROCESSING_LIMIT:
        return _quarantine(pipeline_id=pipeline_id, stream_epoch=stream_epoch, topic=topic,
                           partition=partition, offset=offset, key=key, value=value, kind="oversized",
                           reason="raw_record_too_large", summary="raw Kafka record exceeds 1048576 bytes")
    digest = raw_hash(key, value)
    if value is None:
        return _base(key, value, digest, classification="control", reason_code="tombstone")
    try:
        key_doc = json.loads((key or b"").decode("utf-8"))
        value_doc = json.loads(value.decode("utf-8"))
        if set(key_doc) != {"schema", "payload"} or set(value_doc) != {"schema", "payload"}:
            raise ValueError("schema-enabled JSON wrapper is required")
        key_payload = key_doc["payload"]
        payload = value_doc["payload"]
        envelope_schema = value_doc["schema"]
        if not isinstance(key_payload, dict) or not isinstance(payload, dict):
            raise ValueError("Debezium payload must be an object")
        fields = envelope_schema.get("fields")
        if not isinstance(fields, list):
            raise ValueError("Debezium envelope schema fields are missing")
        op = payload.get("op")
        operations = {"r": "read", "c": "create", "u": "update", "d": "delete"}
        if op not in operations:
            raise RuntimeError(f"unsupported Debezium operation: {op!r}")
        source = payload.get("source")
        if not isinstance(source, dict):
            raise ValueError("Debezium source metadata is missing")
        if source.get("schema") != manifest.schema_name or source.get("table") != manifest.table_name:
            raise ValueError("record source does not match the source-schema manifest")
        row_schema_name = "before" if op == "d" and payload.get("after") is None else "after"
        manifest.verify_fields(_row_fields(fields, row_schema_name))
        before = _normalize_row(manifest, payload.get("before"))
        after = _normalize_row(manifest, payload.get("after"))
        if op in {"r", "c", "u"} and after is None:
            raise ValueError("read/create/update requires after row")
        if op == "c" and before is not None:
            raise ValueError("create requires null before row")
        if op == "d" and after is not None:
            raise ValueError("delete requires null after row")
        key_fields = key_doc.get("schema", {}).get("fields")
        if not isinstance(key_fields, list):
            raise ValueError("Debezium key schema fields are missing")
        key_by_name = _field_map(key_fields)
        keys = []
        key_columns = sorted((item for item in manifest.columns if item.key_position is not None), key=lambda item: item.key_position or 0)
        if not 1 <= len(key_columns) <= 4 or list(key_payload) != [item.name for item in key_columns]:
            raise ValueError("Debezium key does not match the source-schema manifest")
        for column in key_columns:
            schema = key_by_name.get(column.name)
            if not schema or schema.get("type") != column.debezium_type or schema.get("name") != column.debezium_logical_type:
                raise ValueError("Debezium key schema does not match the source-schema manifest")
            converted = _convert(column, key_payload[column.name])
            if converted is None:
                raise ValueError("key values cannot be null")
            key_type = canonical_postgres_type(column.postgres_type)
            key_type = {"character": "char", "character varying": "varchar"}.get(key_type, key_type)
            keys.append({"name": column.name, "type": key_type, "value": converted})
        source_ts = source.get("ts_ms")
        if type(source_ts) is not int or source_ts < 0:
            raise ValueError("payload.source.ts_ms must be a non-negative integer")
        occurred = datetime.fromtimestamp(source_ts / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        ingest_time = (ingested_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        identifier = event_id(pipeline_id, stream_epoch, topic, partition, offset)
        snapshot = op == "r" and str(source.get("snapshot")).lower() in {"true", "first", "last"}
        envelope = {
            "schema_version": 1, "event_id": identifier, "pipeline_id": str(pipeline_id),
            "operation": operations[op], "occurred_at": occurred, "ingested_at": ingest_time,
            "key": keys,
            "source": {
                "connector": "postgresql", "stream_epoch": str(stream_epoch), "database": source.get("db"),
                "schema": source.get("schema"), "table": source.get("table"), "topic": topic,
                "partition": partition, "offset": offset,
                "lsn": None if source.get("lsn") is None else str(source.get("lsn")), "snapshot": snapshot,
                "transaction_id": source.get("txId"),
            },
            "before": before, "after": after,
            "metadata": {"connector_processed_at_ms": payload.get("ts_ms"), "schema_fingerprint": manifest.fingerprint},
        }
        if len(_canonical_json(envelope)) > NORMALIZED_ENVELOPE_LIMIT:
            return _quarantine(pipeline_id=pipeline_id, stream_epoch=stream_epoch, topic=topic,
                               partition=partition, offset=offset, key=key, value=value, kind="oversized",
                               reason="normalized_envelope_too_large", summary="normalized envelope exceeds 524288 bytes")
        return _base(key, value, digest, classification="event", event_id=identifier, envelope=envelope)
    except RuntimeError as exc:
        return _quarantine(pipeline_id=pipeline_id, stream_epoch=stream_epoch, topic=topic,
                           partition=partition, offset=offset, key=key, value=value, kind="unsupported",
                           reason="unsupported_record", summary=str(exc))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return _quarantine(pipeline_id=pipeline_id, stream_epoch=stream_epoch, topic=topic,
                           partition=partition, offset=offset, key=key, value=value, kind="malformed",
                           reason="malformed_record", summary=str(exc))
