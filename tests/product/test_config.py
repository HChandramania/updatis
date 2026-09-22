from __future__ import annotations

import copy
import json

import pytest
from pydantic import ValidationError

from updatis.config.models import PipelineConfigV1, RuntimeConfigV1
from updatis.config.loader import ConfigurationValidationError, load_pipeline_config, validate_configuration_bundle
from updatis.config.secrets import SecretResolver


def test_valid_configs_apply_safe_defaults(pipeline_document: dict, runtime_document: dict) -> None:
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    assert pipeline.retry.max_attempts == 10
    assert pipeline.capacity.inflight_per_partition == 1
    assert pipeline.source.connector_tasks == 1
    assert runtime.schema_version == 1


@pytest.mark.parametrize("version", [0, 2, 999])
def test_unsupported_versions_are_rejected(pipeline_document: dict, version: int) -> None:
    pipeline_document["schema_version"] = version
    with pytest.raises(ValidationError):
        PipelineConfigV1.model_validate(pipeline_document)


def test_unknown_and_unsupported_capture_options_are_rejected(pipeline_document: dict) -> None:
    pipeline_document["source"]["publication_name"] = "forbidden"
    with pytest.raises(ValidationError):
        PipelineConfigV1.model_validate(pipeline_document)


@pytest.mark.parametrize("url", [
    "http://receiver.example/events", "https://user:pass@receiver.example/events",
    "https://receiver.example/events?token=secret", "ftp://receiver.example/events",
])
def test_unsafe_destination_urls_are_rejected(pipeline_document: dict, url: str) -> None:
    pipeline_document["destination"]["url"] = url
    with pytest.raises(ValidationError):
        PipelineConfigV1.model_validate(pipeline_document)


def test_local_http_requires_exact_allowlist(pipeline_document: dict) -> None:
    pipeline_document["destination"]["url"] = "http://receiver:8080/events"
    pipeline_document["allow_local_http"] = True
    pipeline_document["local_http_allowlist"] = ["receiver:8080"]
    assert PipelineConfigV1.model_validate(pipeline_document).destination.url.startswith("http://")


def test_cross_field_limits_are_rejected(pipeline_document: dict) -> None:
    pipeline_document["retry"] = {"connect_timeout_seconds": 31, "total_timeout_seconds": 30}
    with pytest.raises(ValidationError):
        PipelineConfigV1.model_validate(pipeline_document)


def test_foundation_defaults_copy_is_exact() -> None:
    from importlib.resources import files
    root = __import__("pathlib").Path(__file__).resolve().parents[2]
    packaged = files("updatis.resources").joinpath("foundation-defaults-v1.json").read_bytes()
    assert packaged == (root / "contracts" / "foundation-defaults-v1.json").read_bytes()


def test_local_http_is_rejected_outside_local_environments(
    pipeline_document: dict, runtime_document: dict, tmp_path
) -> None:
    pipeline_document["destination"]["url"] = "http://receiver:8080/events"
    pipeline_document["allow_local_http"] = True
    pipeline_document["local_http_allowlist"] = ["receiver:8080"]
    runtime_document["environment"] = "production"
    pipeline = PipelineConfigV1.model_validate(pipeline_document)
    runtime = RuntimeConfigV1.model_validate(runtime_document)
    secrets = SecretResolver(tmp_path, {
        "SOURCE_PASSWORD": "source", "METADATA_PASSWORD": "metadata", "SIGNING_SECRET": "signing"
    })
    addresses = {"source-db": {"10.0.0.2"}, "metadata-db": {"10.0.0.3"}, "metadata": {"10.0.0.3"}}
    with pytest.raises(ValueError, match="development and test"):
        validate_configuration_bundle(runtime, pipeline, lambda host, port: addresses[host], secrets)


def test_loader_validation_error_omits_submitted_secret(
    pipeline_document: dict, tmp_path
) -> None:
    pipeline_document["destination"]["signing_secret"] = "canary-secret-value"
    path = tmp_path / "unsafe.json"
    path.write_text(json.dumps(pipeline_document), encoding="utf-8")
    with pytest.raises(ConfigurationValidationError) as captured:
        load_pipeline_config(path)
    assert "canary-secret-value" not in str(captured.value)
