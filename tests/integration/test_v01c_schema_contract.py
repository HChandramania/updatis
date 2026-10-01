from __future__ import annotations

from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_partition_lease_migration_contract() -> None:
    module = import_module("updatis.db.migrations.versions.0003_partition_leases")
    assert module.revision == "0003_partition_leases"
    assert module.down_revision == "0002_durable_intake"
    migration = (ROOT / "src/updatis/db/migrations/versions/0003_partition_leases.py").read_text()
    for fragment in (
        "lease_owner text", "lease_expires_at timestamptz", "fencing_generation bigint",
        "deliveries_lease_pair", "length(lease_owner) BETWEEN 1 AND 128",
        "fencing_generation >= 0", "WHERE state IN ('pending', 'attempting')",
    ):
        assert fragment in migration


def test_v01c_verifier_uses_real_concurrent_connections() -> None:
    verifier = (ROOT / "tests/integration/verify_v01c.py").read_text()
    probe = (ROOT / "tests/integration/v01c_probe.py").read_text()
    assert ':/tmp/v01c_probe.py:ro' in verifier
    assert "ThreadPoolExecutor" in probe
    assert "engine.connect()" in probe
    assert "held.wait(5)" in probe and "release.wait(8)" in probe
    assert "assert not holder.done()" in probe


def test_forward_lane_migration_and_bounded_query_contract() -> None:
    module = import_module("updatis.db.migrations.versions.0004_delivery_lanes")
    assert module.revision == "0004_delivery_lanes"
    assert module.down_revision == "0003_partition_leases"
    next_module = import_module("updatis.db.migrations.versions.0005_lane_fairness")
    assert next_module.revision == "0005_lane_fairness"
    assert next_module.down_revision == "0004_delivery_lanes"
    from updatis.delivery.repository import _LANE_CANDIDATES, _LANE_HEAD
    assert _LANE_CANDIDATES.index("SKIP LOCKED") < _LANE_CANDIDATES.index("LIMIT :candidate_limit")
    assert "ORDER BY source_offset, id LIMIT 1" in _LANE_HEAD
    assert "SKIP LOCKED" not in _LANE_HEAD
