from pathlib import Path

from tests.integration.verify import CANONICAL_DATABASE, NEGATIVE_DATABASES


ROOT = Path(__file__).resolve().parents[2]


def test_negative_schema_cases_use_distinct_disposable_databases() -> None:
    assert set(NEGATIVE_DATABASES) == {
        "unmarked", "missing_marker", "missing_revision", "wrong_revision"
    }
    assert CANONICAL_DATABASE not in NEGATIVE_DATABASES.values()
    assert len(set(NEGATIVE_DATABASES.values())) == len(NEGATIVE_DATABASES)


def test_canonical_schema_is_rechecked_after_negative_cases() -> None:
    source = (ROOT / "tests/integration/verify.py").read_text(encoding="utf-8")
    negative_end = source.index("# Regression guard: no negative case may damage")
    runtime_start = source.index("# API and worker are started only after", negative_end)
    between = source[negative_end:runtime_start]
    assert "assert_canonical_schema_intact()" in between
    assert "DROP DATABASE updatis_metadata" not in source
