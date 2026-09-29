from __future__ import annotations

import base64
from uuid import UUID


def encode_identifier(value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")
    return f"u{encoded}"


def decode_identifier(value: str) -> str:
    if not value.startswith("u"):
        raise ValueError("encoded PostgreSQL identifier must start with 'u'")
    encoded = value[1:]
    return base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode("utf-8")


def topic_prefix(pipeline_id: UUID, stream_epoch: UUID) -> str:
    return f"updatis.{pipeline_id.hex}.{stream_epoch.hex}"


def data_topic(pipeline_id: UUID, stream_epoch: UUID, schema: str, table: str) -> str:
    for identifier in (schema, table):
        if not identifier or not all(character.islower() or character.isdigit() or character == "_" for character in identifier):
            raise ValueError("topic identifiers must use lowercase ASCII letters, digits, and underscores")
    topic = f"{topic_prefix(pipeline_id, stream_epoch)}.{schema}.{table}"
    if len(topic.encode("utf-8")) > 249:
        raise ValueError("encoded Kafka topic exceeds 249 bytes")
    return topic


def resource_names(pipeline_id: UUID, stream_epoch: UUID) -> dict[str, str]:
    pipeline = pipeline_id.hex
    epoch = stream_epoch.hex[-12:]
    return {
        "connector": f"updatis-pg-{pipeline}-{epoch}",
        "publication": f"updatis_pub_{pipeline}_{epoch}",
        "slot": f"updatis_slot_{pipeline[:24]}_{epoch}",
        "consumer_group": f"updatis-intake-{pipeline}-{stream_epoch.hex}",
        "topic_prefix": topic_prefix(pipeline_id, stream_epoch),
    }
