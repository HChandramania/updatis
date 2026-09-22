from __future__ import annotations

import pytest

from updatis.config.changes import UnsafeConfigurationChange, validate_revision_change
from updatis.config.models import PipelineConfigV1


def test_identical_revision_resubmission_is_idempotent(pipeline_document: dict) -> None:
    config = PipelineConfigV1.model_validate(pipeline_document)
    validate_revision_change(config, config.model_copy(deep=True))


def test_substantive_change_is_rejected(pipeline_document: dict) -> None:
    current = PipelineConfigV1.model_validate(pipeline_document)
    pipeline_document["destination"]["url"] = "https://new.example/events"
    with pytest.raises(UnsafeConfigurationChange, match="unsupported"):
        validate_revision_change(current, PipelineConfigV1.model_validate(pipeline_document))
