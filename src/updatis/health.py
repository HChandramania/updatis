from __future__ import annotations

from updatis.config.loader import load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import create_metadata_engine
from updatis.db.schema import require_compatible_schema


def check_metadata(runtime_path: str) -> None:
    runtime = load_runtime_config(runtime_path)
    engine = create_metadata_engine(runtime.metadata, SecretResolver(runtime.secrets_directory))
    try:
        require_compatible_schema(engine)
    finally:
        engine.dispose()
