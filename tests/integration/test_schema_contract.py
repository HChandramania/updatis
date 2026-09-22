from pathlib import Path
from importlib import import_module


ROOT = Path(__file__).resolve().parents[2]


def test_initial_migration_contains_required_entities_and_constraints() -> None:
    assert import_module("updatis.db.migrations.versions.0001_metadata").revision == "0001_metadata"
    migration = (ROOT / "src/updatis/db/migrations/versions/0001_metadata.py").read_text(encoding="utf-8")
    for table in ("pipelines", "configuration_revisions", "ingest_records", "events",
                  "ingest_checkpoints", "deliveries", "delivery_attempts", "dead_letters",
                  "ordering_gaps", "audit_records"):
        assert f"CREATE TABLE {table}" in migration
    assert "ON DELETE CASCADE" not in migration
    assert "UNIQUE (pipeline_id, source_stream_epoch, topic, partition, offset_value)" in migration


def test_downgrade_is_explicitly_non_destructive() -> None:
    migration = (ROOT / "src/updatis/db/migrations/versions/0001_metadata.py").read_text(encoding="utf-8")
    assert "does not provide destructive automatic downgrades" in migration
