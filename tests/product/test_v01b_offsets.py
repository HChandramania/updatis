from __future__ import annotations

import pytest

from updatis.kafka.offsets import DurableOffsetCommitter


class FakeCommitter:
    def __init__(self) -> None:
        self.calls = []

    def commit_partition(self, topic: str, partition: int, next_offset: int, timeout_seconds: float) -> None:
        self.calls.append((topic, partition, next_offset, timeout_seconds))


def test_commit_uses_resulting_durable_checkpoint_for_new_and_duplicate_records() -> None:
    delegate = FakeCommitter()
    committer = DurableOffsetCommitter(delegate)
    committer.commit("events", 0, 11)
    committer.commit("events", 0, 11)  # duplicate N=9 can return existing M=11
    assert delegate.calls == [("events", 0, 11, 5.0), ("events", 0, 11, 5.0)]


def test_commit_never_moves_backward_and_partitions_are_independent() -> None:
    delegate = FakeCommitter()
    committer = DurableOffsetCommitter(delegate)
    committer.commit("events", 0, 5)
    committer.commit("events", 1, 2)
    with pytest.raises(ValueError, match="backward"):
        committer.commit("events", 0, 4)
    assert committer.last_committed[("events", 1)] == 2
