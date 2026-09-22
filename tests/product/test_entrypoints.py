from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_api_binds_all_container_interfaces_and_compose_publishes_loopback_only() -> None:
    api = (ROOT / "src/updatis/api.py").read_text(encoding="utf-8")
    compose = (ROOT / "deploy/compose.yml").read_text(encoding="utf-8")
    assert 'host="0.0.0.0"' in api
    assert '"127.0.0.1:8000:8000"' in compose


def test_entrypoints_do_not_apply_migrations() -> None:
    for path in (ROOT / "src/updatis/api.py", ROOT / "src/updatis/worker.py"):
        assert "command.upgrade" not in path.read_text(encoding="utf-8")


def test_source_bootstrap_contains_no_cdc_setup() -> None:
    script = (ROOT / "deploy/postgres/init-source.sh").read_text(encoding="utf-8").lower()
    forbidden = ("create publication", "pg_create_logical_replication_slot", "create_replication_slot")
    assert not any(fragment in script for fragment in forbidden)
