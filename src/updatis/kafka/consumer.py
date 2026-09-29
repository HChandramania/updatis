from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from uuid import UUID

from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata, TopicPartition

from updatis.capture.names import data_topic, resource_names
from updatis.config.models import PipelineConfigV1
from updatis.intake.repository import IntakeRepository
from updatis.normalization.normalize import normalize_record

logger = logging.getLogger(__name__)


class IntakeRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: KafkaConsumer, repository: IntakeRepository,
                 config: PipelineConfigV1) -> None:
        self.consumer = consumer
        self.repository = repository
        self.config = config
        self.group = resource_names(config.pipeline_id, config.source.stream_epoch)["consumer_group"]
        self.durable: dict[TopicPartition, int] = {}
        self.prepared: dict[TopicPartition, int] = {}

    def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        # Each staged record is committed synchronously before durable is
        # advanced, so revocation has no uncommitted in-memory progress to
        # flush from inside the coordinator callback.
        for partition in revoked:
            self.durable.pop(partition, None)

    def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        for partition in sorted(assigned, key=lambda item: (item.topic, item.partition)):
            if partition not in self.prepared:
                raise RuntimeError(f"assigned Kafka partition was not prepared: {partition}")
            offset = self.prepared[partition]
            self.consumer.seek(partition, offset)
            self.durable[partition] = offset


def _source_identity(value: bytes) -> tuple[str, str]:
    document = json.loads(value.decode("utf-8"))
    payload = document.get("payload")
    source = payload.get("source") if isinstance(payload, dict) else None
    if not isinstance(source, dict) or not isinstance(source.get("schema"), str) or not isinstance(source.get("table"), str):
        raise ValueError("record has no source schema/table identity")
    return source["schema"], source["table"]


def run_consumer(config: PipelineConfigV1, repository: IntakeRepository, bootstrap_servers: str,
                 stop: threading.Event, ready: Callable[[bool], None]) -> None:
    names = resource_names(config.pipeline_id, config.source.stream_epoch)
    topics = [data_topic(config.pipeline_id, config.source.stream_epoch, table.schema_name, table.table_name)
              for table in config.source.tables]
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","), group_id=names["consumer_group"],
        enable_auto_commit=False, auto_offset_reset="none", max_poll_records=50,
        max_poll_interval_ms=300_000, request_timeout_ms=10_000,
        consumer_timeout_ms=1_000, max_partition_fetch_bytes=2_097_152,
        fetch_max_bytes=4_194_304,
    )
    listener = IntakeRebalanceListener(consumer, repository, config)
    partitions: set[TopicPartition] = set()
    for topic in topics:
        topic_partitions = consumer.partitions_for_topic(topic)
        if not topic_partitions:
            raise RuntimeError(f"configured Kafka topic does not exist or has no partitions: {topic}")
        partitions.update(TopicPartition(topic, number) for number in topic_partitions)
    beginnings = consumer.beginning_offsets(partitions)
    ends = consumer.end_offsets(partitions)
    for partition in sorted(partitions, key=lambda item: (item.topic, item.partition)):
        checkpoint = repository.initialize_checkpoint(
            pipeline_id=config.pipeline_id, stream_epoch=config.source.stream_epoch,
            topic=partition.topic, partition=partition.partition, consumer_group=names["consumer_group"],
            earliest=beginnings[partition], log_end=ends[partition],
            kafka_committed=consumer.committed(partition),
        )
        listener.prepared[partition] = checkpoint.next_offset
    consumer.subscribe(topics, listener=listener)
    ready(True)
    try:
        while not stop.is_set():
            batches = consumer.poll(timeout_ms=1_000, max_records=50)
            for partition, records in batches.items():
                if partition not in consumer.assignment():
                    continue
                for message in records:
                    if stop.is_set() or partition not in consumer.assignment():
                        break
                    try:
                        schema_name, table_name = _topic_identity(config, message.topic)
                        manifest = repository.load_manifest(config.pipeline_id, config.source.stream_epoch,
                                                            schema_name, table_name)
                        normalized = normalize_record(
                            pipeline_id=config.pipeline_id, stream_epoch=config.source.stream_epoch,
                            topic=message.topic, partition=message.partition, offset=message.offset,
                            key=message.key, value=message.value, manifest=manifest,
                        )
                    except Exception as exc:
                        logger.exception("failed to classify Kafka record at %s-%s-%s", message.topic,
                                         message.partition, message.offset)
                        raise RuntimeError("Kafka record could not be durably classified") from exc
                    revision = repository.configuration_revision_id(config.pipeline_id)
                    durable = repository.stage(
                        pipeline_id=config.pipeline_id, stream_epoch=config.source.stream_epoch,
                        topic=message.topic, partition=message.partition, offset=message.offset,
                        consumer_group=names["consumer_group"], configuration_revision_id=revision,
                        record=normalized,
                    )
                    previous = listener.durable.get(partition)
                    if previous is not None and durable < previous:
                        raise RuntimeError("durable checkpoint moved backward")
                    consumer.commit(offsets={partition: OffsetAndMetadata(durable, "", -1)})
                    listener.durable[partition] = durable
    finally:
        ready(False)
        consumer.close(autocommit=False, timeout_ms=30_000)


def _topic_identity(config: PipelineConfigV1, topic: str) -> tuple[str, str]:
    for table in config.source.tables:
        if topic == data_topic(config.pipeline_id, config.source.stream_epoch, table.schema_name, table.table_name):
            return table.schema_name, table.table_name
    raise ValueError("Kafka topic is not part of the configured pipeline")
