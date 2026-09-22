from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_api_binds_all_container_interfaces_and_compose_publishes_loopback_only() -> None:
    api = (ROOT / "src/updatis/api.py").read_text(encoding="utf-8")
    compose = (ROOT / "deploy/compose.yml").read_text(encoding="utf-8")
    assert 'host="0.0.0.0"' in api
    assert '"127.0.0.1:8000:8000"' in compose
    topology = yaml.safe_load(compose)
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert 'ENTRYPOINT ["python", "-m"]' in dockerfile
    assert topology["services"]["api"]["command"] == ["updatis.api"]
    assert 'if __name__ == "__main__":' in api
    assert "main()" in api
    assert 'uvicorn.run("updatis.api:app", host="0.0.0.0", port=8000)' in api
    assert topology["services"]["api"]["networks"] == ["metadata", "api-edge"]
    assert topology["services"]["metadata-db"]["networks"] == ["metadata"]
    assert all(
        "api-edge" not in service.get("networks", [])
        for name, service in topology["services"].items()
        if name != "api"
    )
    assert topology["networks"]["metadata"]["internal"] is True
    assert topology["networks"]["api-edge"] == {}


def test_entrypoints_do_not_apply_migrations() -> None:
    for path in (ROOT / "src/updatis/api.py", ROOT / "src/updatis/worker.py"):
        assert "command.upgrade" not in path.read_text(encoding="utf-8")


def test_source_bootstrap_contains_no_cdc_setup() -> None:
    script = (ROOT / "deploy/postgres/init-source.sh").read_text(encoding="utf-8").lower()
    forbidden = ("create publication", "pg_create_logical_replication_slot", "create_replication_slot")
    assert not any(fragment in script for fragment in forbidden)
