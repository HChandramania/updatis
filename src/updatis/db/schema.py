from __future__ import annotations

from sqlalchemy import Engine, text

EXPECTED_REVISION = "0001_metadata"


class SchemaCompatibilityError(RuntimeError):
    pass


def require_compatible_schema(engine: Engine) -> None:
    try:
        with engine.connect() as connection:
            marker = connection.execute(
                text("SELECT instance_id FROM updatis_bootstrap.metadata_instance WHERE singleton")
            ).scalar_one()
            revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    except Exception as exc:
        raise SchemaCompatibilityError("metadata database is unavailable or has not been explicitly migrated") from exc
    if not marker or revision != EXPECTED_REVISION:
        raise SchemaCompatibilityError(
            f"metadata schema revision is incompatible; expected {EXPECTED_REVISION}"
        )
