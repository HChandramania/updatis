from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_ci_runs_hash_locked_suite_and_container_check() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "--require-hashes -r requirements-dev.lock" in workflow
    assert "python -m pytest" in workflow
    assert "python tests/compatibility/verify.py" in workflow
    assert "continue-on-error" not in workflow


def test_validated_matrix_matches_compatibility_compose() -> None:
    matrix = json.loads(
        (ROOT / "compatibility" / "candidate-matrix.json").read_text(encoding="utf-8")
    )
    compose = (ROOT / "tests" / "compatibility" / "compose.yml").read_text(encoding="utf-8")
    assert matrix["status"] == "validated"
    assert f"postgres:{matrix['postgresql']}" in compose
    assert f"apache/kafka:{matrix['kafka_broker']}" in compose
    assert f"quay.io/debezium/connect:{matrix['debezium']}" in compose
