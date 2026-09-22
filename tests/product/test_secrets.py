from __future__ import annotations

import pytest

from updatis.config.models import SecretReference
from updatis.config.secrets import SecretResolutionError, SecretResolver


def test_environment_secret_is_resolved_without_changing_reference(tmp_path) -> None:
    reference = SecretReference(provider="env", name="SOURCE_PASSWORD")
    resolver = SecretResolver(tmp_path, {"SOURCE_PASSWORD": "canary-value"})
    assert resolver.resolve(reference).get_secret_value() == "canary-value"
    assert reference.model_dump() == {"provider": "env", "name": "SOURCE_PASSWORD"}


def test_file_secret_must_remain_beneath_root(tmp_path) -> None:
    reference = SecretReference(provider="file", name="missing")
    with pytest.raises(SecretResolutionError, match="unavailable"):
        SecretResolver(tmp_path).resolve(reference)


def test_empty_and_missing_environment_secrets_are_rejected(tmp_path) -> None:
    reference = SecretReference(provider="env", name="SOURCE_PASSWORD")
    for environ in ({}, {"SOURCE_PASSWORD": ""}):
        with pytest.raises(SecretResolutionError, match="empty or too large"):
            SecretResolver(tmp_path, environ).resolve(reference)
