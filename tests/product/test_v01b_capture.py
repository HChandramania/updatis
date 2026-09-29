from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from updatis.capture.connector import build_connector_config, immutable_mismatches
from updatis.capture.manifest import SourceColumn, SourceSchemaManifest
from updatis.capture.names import data_topic, resource_names
from updatis.config.models import PipelineConfigV1


PIPELINE = UUID("00000000-0000-0000-0000-000000000001")
EPOCH = UUID("00000000-0000-0000-0000-000000000101")


def test_resource_names_are_epoch_isolated_and_topic_names_are_unambiguous() -> None:
    first = resource_names(PIPELINE, EPOCH)
    second = resource_names(PIPELINE, UUID("00000000-0000-0000-0000-000000000102"))
    assert first["slot"] != second["slot"]
    assert first["connector"] != second["connector"]
    assert first["consumer_group"] != second["consumer_group"]
    assert data_topic(PIPELINE, EPOCH, "example", "orders").endswith(".example.orders")
    with pytest.raises(ValueError):
        data_topic(PIPELINE, EPOCH, "Example", "orders")


def test_connector_has_exact_serialization_and_progress_properties(pipeline_document: dict) -> None:
    config = PipelineConfigV1.model_validate(pipeline_document)
    _, connector = build_connector_config(config, "${directory:/run/secrets:source_capture_password}")
    assert connector | {
        "key.converter.schemas.enable": "true", "value.converter.schemas.enable": "true",
        "decimal.handling.mode": "string", "binary.handling.mode": "base64",
        "time.precision.mode": "isostring", "hstore.handling.mode": "json",
        "include.unknown.datatypes": "false", "publication.autocreate.mode": "disabled",
        "snapshot.mode": "initial", "tasks.max": "1", "plugin.name": "pgoutput",
    } == connector
    changed = dict(connector, **{"slot.name": "wrong"})
    assert immutable_mismatches(connector, changed) == ["slot.name"]


def test_compose_enables_directory_secret_provider() -> None:
    compose = (Path(__file__).resolve().parents[2] / "deploy" / "compose.yml").read_text(encoding="utf-8")
    assert "CONNECT_CONFIG_PROVIDERS: directory" in compose
    assert (
        "CONNECT_CONFIG_PROVIDERS_DIRECTORY_CLASS: "
        "org.apache.kafka.common.config.provider.DirectoryConfigProvider"
    ) in compose


def test_manifest_fingerprint_and_complete_schema_comparison() -> None:
    manifest = SourceSchemaManifest("public", "orders", (
        SourceColumn("id", 1, "bigint", False, 1, "int64"),
        SourceColumn("note", 2, "text", True, None, "string"),
    ))
    assert len(manifest.fingerprint) == 64 and manifest.fingerprint == manifest.fingerprint.lower()
    manifest.verify_fields([
        {"field": "id", "type": "int64", "optional": False},
        {"field": "note", "type": "string", "optional": True},
    ])
    with pytest.raises(ValueError, match="manifest"):
        manifest.verify_fields([{"field": "id", "type": "int64", "optional": False}])
