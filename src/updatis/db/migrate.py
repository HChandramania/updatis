from __future__ import annotations

import argparse
import os
from importlib.resources import files

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from updatis.config.loader import load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import create_metadata_engine, database_url


MIGRATION_LOCK_ID = 87388211725601


def build_alembic_config(runtime_path: str) -> Config:
    """Construct Alembic configuration from resources inside the installed wheel."""
    runtime = load_runtime_config(runtime_path)
    resolver = SecretResolver(runtime.secrets_directory)
    resource = files("updatis.db.migrations")
    config = Config()
    config.set_main_option("sqlalchemy.url", database_url(runtime.metadata, resolver).replace("%", "%%"))
    config.set_main_option("script_location", str(resource))
    return config


def migrate(runtime_path: str) -> None:
    runtime = load_runtime_config(runtime_path)
    resolver = SecretResolver(runtime.secrets_directory)
    engine = create_metadata_engine(runtime.metadata, resolver)
    try:
        with engine.begin() as connection:
            # A transaction-scoped lock on this caller-owned connection remains
            # held through the complete Alembic execution and outer commit.
            connection.execute(text(f"SELECT pg_advisory_xact_lock({MIGRATION_LOCK_ID})"))
            marker = connection.execute(
                text("SELECT instance_id FROM updatis_bootstrap.metadata_instance WHERE singleton")
            ).scalar_one()
            if marker != runtime.metadata.instance_id:
                raise RuntimeError("metadata instance marker does not match runtime configuration")
            config = build_alembic_config(runtime_path)
            # env.py refuses to create an engine and consumes this exact
            # connection, preventing Alembic from escaping the advisory lock.
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply explicit Updatis metadata migrations")
    parser.add_argument("--config", default=os.getenv("UPDATIS_RUNTIME_CONFIG", "/etc/updatis/runtime.json"))
    args = parser.parse_args()
    migrate(args.config)


if __name__ == "__main__":
    main()
