from __future__ import annotations

import hashlib
import json
import struct
from uuid import UUID


def _digest(parts: list[object]) -> str:
    value = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def event_id(pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int, offset: int) -> str:
    return _digest(["updatis-event-id", 1, str(pipeline_id), str(stream_epoch), topic, partition, offset])


def raw_hash(key: bytes | None, value: bytes | None) -> str:
    framed = bytearray()
    for item in (key, value):
        framed.extend(b"\x00" if item is None else b"\x01")
        framed.extend(struct.pack(">Q", 0 if item is None else len(item)))
        if item is not None:
            framed.extend(item)
    return hashlib.sha256(framed).hexdigest()


def quarantine_id(
    pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int, offset: int, record_hash: str
) -> str:
    return _digest([
        "updatis-quarantine-id", 1, str(pipeline_id), str(stream_epoch), topic, partition, offset, record_hash
    ])
