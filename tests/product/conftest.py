from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def pipeline_document() -> dict:
    return {
        "schema_version": 1,
        "pipeline_id": "orders",
        "source": {
            "kind": "postgresql",
            "endpoint": {
                "host": "source-db", "port": 5432, "database": "orders", "username": "capture",
                "password": {"provider": "env", "name": "SOURCE_PASSWORD"},
                "instance_id": "source-1",
            },
            "stream_epoch": "epoch-1", "tables": [{
                "schema_name": "public", "table_name": "orders",
                "key_columns": ["id"], "key_types": ["bigint"],
            }],
        },
        "destination": {
            "kind": "webhook", "url": "https://receiver.example/events",
            "signing_secret": {"provider": "env", "name": "SIGNING_SECRET"},
        },
    }


@pytest.fixture
def runtime_document() -> dict:
    return {
        "schema_version": 1,
        "environment": "test",
        "metadata": {
            "host": "metadata-db", "port": 5432, "database": "metadata", "username": "runtime",
            "password": {"provider": "env", "name": "METADATA_PASSWORD"},
            "instance_id": "metadata-1",
        },
        "metadata_aliases": ["metadata"],
        "secrets_directory": "/run/secrets",
        "schema_revision": "0001_metadata",
    }
