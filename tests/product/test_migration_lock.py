from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from alembic.config import Config

from updatis.db import migrate as migration_module


def test_advisory_lock_and_alembic_share_the_complete_outer_transaction(monkeypatch) -> None:
    statements: list[str] = []
    transaction_active = False

    class Result:
        def scalar_one(self) -> str:
            return "metadata-test"

    class Connection:
        def execute(self, statement):
            statements.append(str(statement))
            return Result()

    connection = Connection()

    class Engine:
        @contextmanager
        def begin(self):
            nonlocal transaction_active
            transaction_active = True
            try:
                yield connection
            finally:
                transaction_active = False

        def dispose(self):
            pass

    config = Config()
    runtime = SimpleNamespace(
        metadata=SimpleNamespace(instance_id="metadata-test"),
        secrets_directory="unused",
    )
    monkeypatch.setattr(migration_module, "load_runtime_config", lambda path: runtime)
    monkeypatch.setattr(migration_module, "SecretResolver", lambda path: object())
    monkeypatch.setattr(migration_module, "create_metadata_engine", lambda endpoint, resolver: Engine())
    monkeypatch.setattr(migration_module, "build_alembic_config", lambda path: config)

    def upgrade(received_config: Config, revision: str) -> None:
        assert transaction_active
        assert received_config.attributes["connection"] is connection
        assert "pg_advisory_xact_lock" in statements[0]
        assert "FOR UPDATE" not in statements[1]
        assert revision == "head"

    monkeypatch.setattr(migration_module.command, "upgrade", upgrade)
    migration_module.migrate("runtime.json")
    assert not transaction_active
