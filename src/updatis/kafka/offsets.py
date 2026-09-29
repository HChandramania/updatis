from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class OffsetCommitter(Protocol):
    def commit_partition(self, topic: str, partition: int, next_offset: int, timeout_seconds: float) -> None: ...


@dataclass
class DurableOffsetCommitter:
    delegate: OffsetCommitter
    last_committed: dict[tuple[str, int], int] = field(default_factory=dict)

    def commit(self, topic: str, partition: int, durable_next_offset: int) -> None:
        identity = (topic, partition)
        previous = self.last_committed.get(identity)
        if previous is not None and durable_next_offset < previous:
            raise ValueError("ordinary intake cannot commit a Kafka offset backward")
        self.delegate.commit_partition(topic, partition, durable_next_offset, timeout_seconds=5.0)
        self.last_committed[identity] = durable_next_offset
