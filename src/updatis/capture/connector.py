from __future__ import annotations

from uuid import UUID

from updatis.capture.names import resource_names
from updatis.config.models import PipelineConfigV1


IMMUTABLE_CONNECTOR_PROPERTIES = frozenset({
    "database.hostname", "database.port", "database.dbname", "database.user", "plugin.name",
    "slot.name", "publication.name", "topic.prefix", "schema.include.list", "table.include.list",
    "snapshot.mode", "tasks.max", "key.converter", "value.converter",
    "key.converter.schemas.enable", "value.converter.schemas.enable", "decimal.handling.mode",
    "binary.handling.mode", "time.precision.mode", "include.unknown.datatypes",
})


def build_connector_config(config: PipelineConfigV1, password_reference: str) -> tuple[str, dict[str, str]]:
    names = resource_names(config.pipeline_id, config.source.stream_epoch)
    tables = ",".join(f"{item.schema_name}.{item.table_name}" for item in config.source.tables)
    schemas = ",".join(sorted({item.schema_name for item in config.source.tables}))
    endpoint = config.source.endpoint
    result = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": endpoint.host,
        "database.port": str(endpoint.port),
        "database.user": endpoint.username,
        "database.password": password_reference,
        "database.dbname": endpoint.database,
        "plugin.name": "pgoutput",
        "slot.name": names["slot"],
        "publication.name": names["publication"],
        "publication.autocreate.mode": "disabled",
        "topic.prefix": names["topic_prefix"],
        "schema.include.list": schemas,
        "table.include.list": tables,
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "event.processing.failure.handling.mode": "fail",
        "include.schema.changes": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "decimal.handling.mode": "string",
        "binary.handling.mode": "base64",
        "time.precision.mode": "isostring",
        "hstore.handling.mode": "json",
        "include.unknown.datatypes": "false",
    }
    return names["connector"], result


def immutable_mismatches(expected: dict[str, str], actual: dict[str, str]) -> list[str]:
    return sorted(key for key in IMMUTABLE_CONNECTOR_PROPERTIES if actual.get(key) != expected.get(key))
