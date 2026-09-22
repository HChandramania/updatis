from __future__ import annotations

import pytest

from updatis.config.isolation import IsolationValidationError, validate_source_metadata_isolation
from updatis.config.models import PipelineConfigV1, RuntimeConfigV1
from updatis.config.secrets import SecretResolver


def test_distinct_source_and_metadata_are_accepted(pipeline_document: dict, runtime_document: dict) -> None:
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    addresses = {"source-db": {"10.0.0.2"}, "metadata-db": {"10.0.0.3"}, "metadata": {"10.0.0.3"}}
    validate_source_metadata_isolation(pipeline.source.endpoint, runtime, lambda host, port: addresses[host])


@pytest.mark.parametrize("mode", ["identity", "alias", "address", "credential"])
def test_known_metadata_identity_is_rejected(
    pipeline_document: dict, runtime_document: dict, mode: str
) -> None:
    source = pipeline_document["source"]["endpoint"]
    if mode == "identity": source["instance_id"] = "metadata-1"
    if mode == "alias": source["host"] = "metadata"
    if mode == "address": source["host"] = "metadata-proxy"
    if mode == "credential": source["password"] = runtime_document["metadata"]["password"]
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    resolver = lambda host, port: {"10.0.0.3"} if host != "source-db" else {"10.0.0.2"}
    with pytest.raises(IsolationValidationError):
        validate_source_metadata_isolation(pipeline.source.endpoint, runtime, resolver)


def test_unresolved_identity_is_not_treated_as_safe(pipeline_document: dict, runtime_document: dict) -> None:
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    def unavailable(host: str, port: int):
        raise OSError("DNS unavailable")
    with pytest.raises(IsolationValidationError, match="could not be resolved"):
        validate_source_metadata_isolation(pipeline.source.endpoint, runtime, unavailable)


def test_equal_resolved_credentials_are_rejected(pipeline_document: dict, runtime_document: dict, tmp_path) -> None:
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    secrets = SecretResolver(tmp_path, {"SOURCE_PASSWORD": "same", "METADATA_PASSWORD": "same"})
    addresses = {"source-db": {"10.0.0.2"}, "metadata-db": {"10.0.0.3"}, "metadata": {"10.0.0.3"}}
    with pytest.raises(IsolationValidationError, match="resolved credentials"):
        validate_source_metadata_isolation(
            pipeline.source.endpoint, runtime, lambda host, port: addresses[host], secrets
        )
