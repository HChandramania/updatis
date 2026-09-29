from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SourceColumn:
    name: str
    ordinal: int
    postgres_type: str
    nullable: bool
    key_position: int | None
    debezium_type: str
    debezium_logical_type: str | None = None


def canonical_postgres_type(value: str) -> str:
    normalized = re.sub(r"\(\s*\d+(?:\s*,\s*\d+)?\s*\)", "", value.lower())
    return " ".join(normalized.split())


@dataclass(frozen=True)
class SourceSchemaManifest:
    schema_name: str
    table_name: str
    columns: tuple[SourceColumn, ...]

    def canonical_columns(self) -> list[dict[str, object]]:
        return [asdict(item) for item in sorted(self.columns, key=lambda item: item.ordinal)]

    @property
    def fingerprint(self) -> str:
        document = {
            "schema": self.schema_name,
            "table": self.table_name,
            "columns": self.canonical_columns(),
        }
        canonical = json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def verify_fields(self, fields: list[dict[str, object]]) -> None:
        actual = []
        for ordinal, field in enumerate(fields, start=1):
            actual.append((field.get("field"), ordinal, field.get("type"), field.get("name"), bool(field.get("optional", False))))
        expected = [
            (column.name, column.ordinal, column.debezium_type, column.debezium_logical_type, column.nullable)
            for column in sorted(self.columns, key=lambda item: item.ordinal)
        ]
        if actual != expected:
            raise ValueError("emitted Debezium row schema does not match the provisioned source-schema manifest")
