from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import Engine, text

from updatis.normalization.normalize import NormalizedRecord
from updatis.capture.manifest import SourceColumn, SourceSchemaManifest


class CheckpointError(RuntimeError):
    pass


@dataclass(frozen=True)
class Checkpoint:
    initial_offset: int
    next_offset: int


class IntakeRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load_manifest(
        self, pipeline_id: UUID, stream_epoch: UUID, schema_name: str, table_name: str
    ) -> SourceSchemaManifest:
        with self.engine.connect() as connection:
            columns = connection.execute(text("""
                SELECT columns FROM source_schema_manifests
                WHERE pipeline_id=:pipeline AND stream_epoch=:epoch
                  AND schema_name=:schema AND table_name=:table
            """), {"pipeline": pipeline_id, "epoch": stream_epoch,
                    "schema": schema_name, "table": table_name}).scalar_one()
        return SourceSchemaManifest(schema_name, table_name, tuple(SourceColumn(**item) for item in columns))

    def configuration_revision_id(self, pipeline_id: UUID) -> UUID:
        with self.engine.connect() as connection:
            value = connection.execute(text("""
                SELECT id FROM configuration_revisions
                WHERE pipeline_id=:pipeline ORDER BY revision DESC LIMIT 1
            """), {"pipeline": pipeline_id}).scalar_one()
        return UUID(str(value))

    def initialize_checkpoint(
        self, *, pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int,
        consumer_group: str, earliest: int, log_end: int, kafka_committed: int | None,
    ) -> Checkpoint:
        if earliest < 0 or log_end < earliest:
            raise CheckpointError("Kafka returned an invalid retained offset range")
        with self.engine.begin() as connection:
            connection.execute(text("SET LOCAL lock_timeout = '5s'"))
            connection.execute(text("SET LOCAL statement_timeout = '10s'"))
            connection.execute(text("SET LOCAL transaction_timeout = '15s'"))
            row = connection.execute(text("""
                SELECT initial_offset, next_offset FROM ingest_checkpoints
                WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch AND topic=:topic
                  AND partition=:partition AND consumer_group=:consumer_group
                FOR UPDATE
            """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                    "partition": partition, "consumer_group": consumer_group}).mappings().one_or_none()
            if row is None:
                if kafka_committed is not None:
                    raise CheckpointError("new stream epoch unexpectedly has a committed Kafka group offset")
                connection.execute(text("""
                    INSERT INTO ingest_checkpoints
                        (pipeline_id, source_stream_epoch, topic, partition, consumer_group,
                         initial_offset, next_offset)
                    VALUES (:pipeline, :epoch, :topic, :partition, :consumer_group, :offset, :offset)
                    ON CONFLICT DO NOTHING
                """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                        "partition": partition, "consumer_group": consumer_group, "offset": earliest})
                row = connection.execute(text("""
                    SELECT initial_offset, next_offset FROM ingest_checkpoints
                    WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch AND topic=:topic
                      AND partition=:partition AND consumer_group=:consumer_group FOR UPDATE
                """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                        "partition": partition, "consumer_group": consumer_group}).mappings().one()
            checkpoint = Checkpoint(int(row["initial_offset"]), int(row["next_offset"]))
            if checkpoint.next_offset < earliest:
                raise CheckpointError("durable checkpoint is below Kafka's earliest retained offset")
            if checkpoint.next_offset > log_end:
                raise CheckpointError("durable checkpoint is beyond Kafka's log-end offset")
            if kafka_committed is not None and kafka_committed > checkpoint.next_offset:
                raise CheckpointError("Kafka committed offset is ahead of durable metadata")
            return checkpoint

    def stage(
        self, *, pipeline_id: UUID, stream_epoch: UUID, topic: str, partition: int, offset: int,
        consumer_group: str, configuration_revision_id: UUID, record: NormalizedRecord,
    ) -> int:
        with self.engine.begin() as connection:
            connection.execute(text("SET LOCAL lock_timeout = '5s'"))
            connection.execute(text("SET LOCAL statement_timeout = '10s'"))
            connection.execute(text("SET LOCAL transaction_timeout = '15s'"))
            checkpoint = connection.execute(text("""
                SELECT next_offset FROM ingest_checkpoints
                WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch AND topic=:topic
                  AND partition=:partition AND consumer_group=:consumer_group FOR UPDATE
            """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                    "partition": partition, "consumer_group": consumer_group}).scalar_one_or_none()
            if checkpoint is None:
                raise CheckpointError("partition checkpoint has not been initialized")
            durable = int(checkpoint)
            if offset > durable:
                raise CheckpointError(f"non-contiguous record at {offset}; expected {durable}")
            if offset < durable:
                existing = connection.execute(text("""
                    SELECT raw_hash, classification FROM ingest_records
                    WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch AND topic=:topic
                      AND partition=:partition AND offset_value=:offset
                """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                        "partition": partition, "offset": offset}).mappings().one_or_none()
                if existing is None or existing["raw_hash"] != record.raw_hash or existing["classification"] != record.classification:
                    raise CheckpointError("duplicate Kafka coordinate does not match durable intake")
                return durable
            ingest_id = connection.execute(text("""
                INSERT INTO ingest_records
                    (pipeline_id, source_stream_epoch, topic, partition, offset_value, classification,
                     raw_hash, raw_key_prefix, raw_value_prefix, raw_key_length, raw_value_length,
                     raw_key_truncated, raw_value_truncated)
                VALUES (:pipeline, :epoch, :topic, :partition, :offset, :classification, :raw_hash,
                        :raw_key, :raw_value, :key_length, :value_length, :key_truncated, :value_truncated)
                RETURNING id
            """), {"pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                    "partition": partition, "offset": offset, "classification": record.classification,
                    "raw_hash": record.raw_hash, "raw_key": record.raw_key_prefix,
                    "raw_value": record.raw_value_prefix, "key_length": record.raw_key_length,
                    "value_length": record.raw_value_length, "key_truncated": record.raw_key_truncated,
                    "value_truncated": record.raw_value_truncated}).scalar_one()
            if record.classification == "event":
                assert record.event_id and record.envelope
                connection.execute(text("""
                    INSERT INTO events
                        (id, pipeline_id, ingest_record_id, configuration_revision_id, operation,
                         source_position, envelope, payload_hash, captured_at)
                    VALUES (:id, :pipeline, :ingest, :revision, :operation,
                            CAST(:position AS jsonb), CAST(:envelope AS jsonb), :hash,
                            CAST(:captured AS timestamptz))
                """), {"id": record.event_id, "pipeline": pipeline_id, "ingest": ingest_id,
                        "revision": configuration_revision_id, "operation": record.envelope["operation"],
                        "position": json.dumps(record.envelope["source"], separators=(",", ":")),
                        "envelope": json.dumps(record.envelope, separators=(",", ":")),
                        "hash": record.raw_hash, "captured": record.envelope["occurred_at"]})
            elif record.classification == "quarantine":
                assert record.discontinuity_id and record.discontinuity_kind and record.reason_code
                connection.execute(text("""
                    INSERT INTO intake_discontinuities
                        (id, pipeline_id, source_stream_epoch, ingest_record_id, topic, partition,
                         offset_value, kind, reason_code, error_summary, raw_hash, raw_key_length,
                         raw_value_length, raw_key_truncated, raw_value_truncated, normalizer_version)
                    VALUES (:id, :pipeline, :epoch, :ingest, :topic, :partition, :offset, :kind,
                            :reason, :summary, :hash, :key_length, :value_length,
                            :key_truncated, :value_truncated, 1)
                """), {"id": record.discontinuity_id, "pipeline": pipeline_id, "epoch": stream_epoch,
                        "ingest": ingest_id, "topic": topic, "partition": partition, "offset": offset,
                        "kind": record.discontinuity_kind, "reason": record.reason_code,
                        "summary": record.error_summary or "", "hash": record.raw_hash,
                        "key_length": record.raw_key_length, "value_length": record.raw_value_length,
                        "key_truncated": record.raw_key_truncated, "value_truncated": record.raw_value_truncated})
            durable = offset + 1
            connection.execute(text("""
                UPDATE ingest_checkpoints SET next_offset=:next, updated_at=now()
                WHERE pipeline_id=:pipeline AND source_stream_epoch=:epoch AND topic=:topic
                  AND partition=:partition AND consumer_group=:consumer_group
            """), {"next": durable, "pipeline": pipeline_id, "epoch": stream_epoch, "topic": topic,
                    "partition": partition, "consumer_group": consumer_group})
            return durable
