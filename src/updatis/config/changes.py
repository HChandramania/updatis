from __future__ import annotations

from updatis.config.models import PipelineConfigV1


class UnsafeConfigurationChange(ValueError):
    pass


def validate_revision_change(current: PipelineConfigV1, candidate: PipelineConfigV1) -> None:
    """01C permits only idempotent resubmission.

    Pipeline lifecycle and audited backlog reassignment belong to later issues.
    """
    if current != candidate:
        raise UnsafeConfigurationChange(
            "configuration changes are unsupported until pipeline lifecycle behavior is implemented"
        )
