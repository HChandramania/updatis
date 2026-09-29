from __future__ import annotations

from datetime import datetime, timezone
from updatis.capture.manifest import canonical_postgres_type


class TemporalValueError(ValueError):
    pass


def _reject_infinity(value: str) -> None:
    if value.lower() in {"infinity", "+infinity", "-infinity"}:
        raise TemporalValueError("PostgreSQL temporal infinity is unsupported")


def normalize_temporal(postgres_type: str, logical_type: str | None, value: str) -> str:
    _reject_infinity(value)
    pg = canonical_postgres_type(postgres_type)
    if pg == "date":
        if logical_type != "io.debezium.time.IsoDate":
            raise TemporalValueError("unexpected Debezium logical type for date")
        return value.removesuffix("Z")
    if pg in {"time", "time without time zone"}:
        if logical_type != "io.debezium.time.IsoTime":
            raise TemporalValueError("unexpected Debezium logical type for time")
        return value.removesuffix("Z")
    if pg in {"timestamp", "timestamp without time zone"}:
        if logical_type != "io.debezium.time.IsoTimestamp":
            raise TemporalValueError("unexpected Debezium logical type for timestamp")
        return value.removesuffix("Z")
    if pg in {"timetz", "time with time zone"}:
        if logical_type not in {"io.debezium.time.ZonedTime", "io.debezium.time.IsoTime"}:
            raise TemporalValueError("unexpected Debezium logical type for timetz")
        if not (value.endswith("Z") or "+" in value[1:] or "-" in value[1:]):
            raise TemporalValueError("timetz value has no UTC offset")
        return value
    if pg in {"timestamptz", "timestamp with time zone"}:
        if logical_type not in {"io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp"}:
            raise TemporalValueError("unexpected Debezium logical type for timestamptz")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise TemporalValueError("timestamptz value has no UTC offset")
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise TemporalValueError(f"unsupported temporal PostgreSQL type: {postgres_type}")
