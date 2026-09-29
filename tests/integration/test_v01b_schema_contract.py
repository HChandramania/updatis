from __future__ import annotations

from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_durable_intake_migration_contract() -> None:
    module = import_module("updatis.db.migrations.versions.0002_durable_intake")
    assert module.revision == "0002_durable_intake"
    assert module.down_revision == "0001_metadata"
    migration = (ROOT / "src/updatis/db/migrations/versions/0002_durable_intake.py").read_text(encoding="utf-8")
    for fragment in (
        "CREATE TABLE stream_epochs", "CREATE TABLE stream_epoch_transitions",
        "CREATE TABLE source_schema_manifests", "CREATE TABLE intake_discontinuities",
        "raw_key_prefix bytea", "raw_value_prefix bytea", "octet_length(raw_key_prefix) <= 65536",
        "octet_length(raw_value_prefix) <= 262144", "initial_offset bigint",
        "checkpoint cannot move backward", "REVOKE UPDATE ON stream_epochs",
    ):
        assert fragment in migration


def test_v01b_verifier_covers_real_stack_failure_boundaries() -> None:
    verifier = (ROOT / "tests/integration/verify_v01b.py").read_text(encoding="utf-8")
    for fragment in (
        "connector task RUNNING", "initial snapshot intake", "streaming changes and tombstone",
        "worker exit on metadata outage", "pg_replication_slots", "pg_publication_tables",
        "consumer offsets", "ordering_gaps", "source_schema_manifests",
    ):
        assert fragment in verifier
