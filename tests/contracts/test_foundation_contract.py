from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "contracts"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _validator(name: str) -> Draft202012Validator:
    return Draft202012Validator(
        _load(ROOT / "contracts" / name), format_checker=FormatChecker()
    )


@pytest.mark.contract
@pytest.mark.parametrize(
    "fixture_name",
    ["create.json", "snapshot-composite-key.json", "update-without-before.json", "delete.json"],
)
def test_positive_envelope_fixtures(fixture_name: str) -> None:
    _validator("event-envelope-v1.schema.json").validate(_load(FIXTURES / fixture_name))


@pytest.mark.contract
@pytest.mark.parametrize(
    ("mutation", "error_fragment"),
    [
        (lambda event: event.update(operation="truncate"), "not one of"),
        (lambda event: event.update(key=[]), "non-empty"),
        (lambda event: event["key"][0].update(type="numeric"), "not one of"),
        (lambda event: event.update(after=None), "not of type 'object'"),
    ],
)
def test_invalid_envelopes_are_rejected(mutation, error_fragment: str) -> None:
    event = _load(FIXTURES / "create.json")
    mutation(event)
    errors = list(_validator("event-envelope-v1.schema.json").iter_errors(event))
    assert errors
    assert error_fragment in errors[0].message


@pytest.mark.contract
@pytest.mark.parametrize(
    ("key_type", "value"),
    [
        ("smallint", 1),
        ("integer", 1),
        ("bigint", 1),
        ("uuid", "123e4567-e89b-12d3-a456-426614174000"),
        ("char", "A"),
        ("varchar", "account-1"),
        ("text", "account-1"),
    ],
)
def test_key_type_accepts_matching_json_value(key_type: str, value) -> None:
    event = _load(FIXTURES / "create.json")
    event["key"] = [{"name": "id", "type": key_type, "value": value}]
    _validator("event-envelope-v1.schema.json").validate(event)


@pytest.mark.contract
@pytest.mark.parametrize(
    ("key_type", "value"),
    [
        ("smallint", "1"),
        ("integer", "1"),
        ("bigint", "1"),
        ("uuid", 1),
        ("uuid", "not-a-uuid"),
        ("char", 1),
        ("varchar", 1),
        ("text", 1),
    ],
)
def test_key_type_rejects_mismatched_json_value(key_type: str, value) -> None:
    event = _load(FIXTURES / "create.json")
    event["key"] = [{"name": "id", "type": key_type, "value": value}]
    assert list(_validator("event-envelope-v1.schema.json").iter_errors(event))


@pytest.mark.contract
def test_event_identity_is_deterministic_and_coordinate_scoped() -> None:
    event = _load(FIXTURES / "create.json")

    def identity(value: dict) -> str:
        canonical = {
            "identity_version": 1,
            "pipeline_id": value["pipeline_id"],
            "stream_epoch": value["source"]["stream_epoch"],
            "topic": value["source"]["topic"],
            "partition": value["source"]["partition"],
            "offset": value["source"]["offset"],
        }
        encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    assert identity(event) == event["event_id"]
    assert identity(event) == identity(copy.deepcopy(event))
    changed = copy.deepcopy(event)
    changed["source"]["offset"] += 1
    assert identity(event) != identity(changed)


@pytest.mark.contract
def test_provisional_defaults_and_boundary_behavior() -> None:
    schema = _validator("foundation-defaults-v1.schema.json")
    defaults = _load(ROOT / "contracts" / "foundation-defaults-v1.json")
    schema.validate(defaults)

    lower = copy.deepcopy(defaults)
    lower["capacity"]["resume_percent"] = 1
    schema.validate(lower)

    upper = copy.deepcopy(defaults)
    upper["payload"]["typical_bytes"] = upper["payload"]["maximum_bytes"]
    schema.validate(upper)

    invalid = copy.deepcopy(defaults)
    invalid["capacity"]["resume_percent"] = 100
    assert list(schema.iter_errors(invalid))


def _assert_default_relationships(defaults: dict) -> None:
    assert defaults["payload"]["typical_bytes"] <= defaults["payload"]["maximum_bytes"]
    assert defaults["retry"]["initial_backoff_seconds"] <= defaults["retry"]["max_backoff_seconds"]
    assert defaults["retry"]["connect_timeout_seconds"] <= defaults["retry"]["total_timeout_seconds"]
    assert defaults["workload_target"]["burst_events_per_second"] >= defaults["workload_target"]["sustained_events_per_second"]


@pytest.mark.contract
def test_provisional_default_relationship_boundaries() -> None:
    defaults = _load(ROOT / "contracts" / "foundation-defaults-v1.json")
    defaults["payload"]["typical_bytes"] = defaults["payload"]["maximum_bytes"]
    defaults["retry"]["initial_backoff_seconds"] = defaults["retry"]["max_backoff_seconds"]
    defaults["retry"]["connect_timeout_seconds"] = defaults["retry"]["total_timeout_seconds"]
    defaults["workload_target"]["burst_events_per_second"] = defaults["workload_target"]["sustained_events_per_second"]
    _assert_default_relationships(defaults)


@pytest.mark.contract
@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["payload"].update(typical_bytes=value["payload"]["maximum_bytes"] + 1),
        lambda value: value["retry"].update(initial_backoff_seconds=value["retry"]["max_backoff_seconds"] + 1),
        lambda value: value["retry"].update(connect_timeout_seconds=value["retry"]["total_timeout_seconds"] + 1),
        lambda value: value["workload_target"].update(burst_events_per_second=value["workload_target"]["sustained_events_per_second"] - 1),
    ],
)
def test_invalid_provisional_default_relationships_are_rejected(mutate) -> None:
    defaults = _load(ROOT / "contracts" / "foundation-defaults-v1.json")
    mutate(defaults)
    with pytest.raises(AssertionError):
        _assert_default_relationships(defaults)


@pytest.mark.contract
def test_candidate_matrix_is_not_mislabeled_as_validated() -> None:
    matrix = _load(ROOT / "compatibility" / "candidate-matrix.json")
    assert matrix["status"] in {"candidate", "validated"}
    if matrix["status"] == "candidate":
        assert "candidate" in (ROOT / "docs" / "SUPPORT_MATRIX.md").read_text(encoding="utf-8").lower()
