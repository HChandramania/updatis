from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from sqlalchemy import create_engine, text

from updatis.capture.connector import build_connector_config, immutable_mismatches
from updatis.capture.manifest import SourceColumn, SourceSchemaManifest, canonical_postgres_type
from updatis.capture.names import resource_names
from updatis.config.isolation import validate_source_metadata_isolation
from updatis.config.loader import load_pipeline_config, load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import database_url, create_metadata_engine
from updatis.db.schema import require_compatible_schema


def _logical_types(postgres_type: str) -> tuple[str, str | None]:
    pg = canonical_postgres_type(postgres_type)
    values = {
        "boolean": ("boolean", None), "smallint": ("int16", None), "integer": ("int32", None),
        "bigint": ("int64", None), "real": ("float32", None), "double precision": ("float64", None),
        "numeric": ("string", None), "decimal": ("string", None), "character": ("string", None),
        "character varying": ("string", None), "text": ("string", None),
        "uuid": ("string", "io.debezium.data.Uuid"), "bytea": ("string", None),
        "date": ("string", "io.debezium.time.IsoDate"),
        "time without time zone": ("string", "io.debezium.time.IsoTime"),
        "time with time zone": ("string", "io.debezium.time.ZonedTime"),
        "timestamp without time zone": ("string", "io.debezium.time.IsoTimestamp"),
        "timestamp with time zone": ("string", "io.debezium.time.ZonedTimestamp"),
        "json": ("string", "io.debezium.data.Json"), "jsonb": ("string", "io.debezium.data.Json"),
    }
    if pg not in values:
        raise ValueError(f"unsupported source column type: {postgres_type}")
    return values[pg]


def _request(url: str, *, method: str = "GET", document: dict | None = None) -> tuple[int, dict]:
    body = None if document is None else json.dumps(document).encode("utf-8")
    request = urllib.request.Request(url, data=body, method=method,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read() or b"{}")
        return exc.code, payload


def provision(runtime_path: str, pipeline_path: str, connect_url: str) -> None:
    runtime = load_runtime_config(runtime_path)
    pipeline = load_pipeline_config(pipeline_path)
    resolver = SecretResolver(runtime.secrets_directory)
    validate_source_metadata_isolation(pipeline.source.endpoint, runtime, secret_resolver=resolver)
    metadata = create_metadata_engine(runtime.metadata, resolver)
    require_compatible_schema(metadata)
    source = create_engine(database_url(pipeline.source.endpoint, resolver), pool_pre_ping=True)
    names = resource_names(pipeline.pipeline_id, pipeline.source.stream_epoch)
    manifests: list[SourceSchemaManifest] = []
    with source.connect() as connection:
        publication_tables = set(connection.execute(text("""
            SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname=:publication
        """), {"publication": names["publication"]}).tuples())
        configured = {(item.schema_name, item.table_name) for item in pipeline.source.tables}
        if publication_tables != configured:
            raise RuntimeError("publication tables do not exactly match pipeline configuration")
        for table in pipeline.source.tables:
            rows = connection.execute(text("""
                SELECT a.attname, a.attnum, format_type(a.atttypid, a.atttypmod), a.attnotnull
                FROM pg_attribute a
                JOIN pg_class c ON c.oid=a.attrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=:schema AND c.relname=:table AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """), {"schema": table.schema_name, "table": table.table_name}).tuples().all()
            if not rows:
                raise RuntimeError(f"configured source table does not exist: {table.schema_name}.{table.table_name}")
            key_positions = {name: index + 1 for index, name in enumerate(table.key_columns)}
            columns = []
            for name, ordinal, pg_type, not_null in rows:
                primitive, logical = _logical_types(pg_type)
                columns.append(SourceColumn(name, ordinal, pg_type, not not_null,
                                            key_positions.get(name), primitive, logical))
            if set(key_positions) - {item.name for item in columns}:
                raise RuntimeError("configured key column is absent from source table")
            manifests.append(SourceSchemaManifest(table.schema_name, table.table_name, tuple(columns)))
    document = pipeline.model_dump(mode="json")
    canonical = json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    config_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    with metadata.begin() as connection:
        connection.execute(text("SET LOCAL lock_timeout='5s'"))
        connection.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                           {"key": f"provision:{pipeline.pipeline_id}:{pipeline.source.stream_epoch}"})
        connection.execute(text("""
            INSERT INTO pipelines (id, name, source_instance_id, source_stream_epoch)
            VALUES (:id, :name, :instance, :epoch)
            ON CONFLICT (id) DO NOTHING
        """), {"id": pipeline.pipeline_id, "name": f"pipeline-{pipeline.pipeline_id}",
                "instance": pipeline.source.endpoint.instance_id, "epoch": pipeline.source.stream_epoch})
        existing = connection.execute(text("SELECT source_stream_epoch FROM pipelines WHERE id=:id"),
                                      {"id": pipeline.pipeline_id}).scalar_one()
        if str(existing) != str(pipeline.source.stream_epoch):
            raise RuntimeError("pipeline already belongs to another stream epoch")
        revision = connection.execute(text("""
            INSERT INTO configuration_revisions
                (pipeline_id, revision, schema_version, document, content_hash)
            VALUES (:pipeline, 1, 1, CAST(:document AS jsonb), :hash)
            ON CONFLICT (pipeline_id, revision) DO NOTHING
            RETURNING id
        """), {"pipeline": pipeline.pipeline_id, "document": canonical, "hash": config_hash}).scalar_one_or_none()
        if revision is None:
            stored = connection.execute(text("""
                SELECT id, content_hash FROM configuration_revisions WHERE pipeline_id=:pipeline AND revision=1
            """), {"pipeline": pipeline.pipeline_id}).mappings().one()
            if stored["content_hash"] != config_hash:
                raise RuntimeError("stored pipeline configuration differs from provisioning input")
        connection.execute(text("""
            INSERT INTO stream_epochs
                (pipeline_id, stream_epoch, connector_name, publication_name, slot_name,
                 topic_prefix, consumer_group, snapshot_mode)
            VALUES (:pipeline, :epoch, :connector, :publication, :slot, :prefix, :group, 'initial')
            ON CONFLICT (pipeline_id, stream_epoch) DO NOTHING
        """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch,
                "connector": names["connector"], "publication": names["publication"], "slot": names["slot"],
                "prefix": names["topic_prefix"], "group": names["consumer_group"]})
        epoch_row = connection.execute(text("""
            SELECT connector_name, publication_name, slot_name, topic_prefix, consumer_group
            FROM stream_epochs WHERE pipeline_id=:pipeline AND stream_epoch=:epoch
        """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch}).mappings().one()
        expected_epoch = {
            "connector_name": names["connector"], "publication_name": names["publication"],
            "slot_name": names["slot"], "topic_prefix": names["topic_prefix"],
            "consumer_group": names["consumer_group"],
        }
        if dict(epoch_row) != expected_epoch:
            raise RuntimeError("stored stream epoch resource identities differ from provisioning input")
        for manifest in manifests:
            connection.execute(text("""
                INSERT INTO source_schema_manifests
                    (pipeline_id, stream_epoch, schema_name, table_name, columns, schema_fingerprint)
                VALUES (:pipeline, :epoch, :schema, :table, CAST(:columns AS jsonb), :fingerprint)
                ON CONFLICT (pipeline_id, stream_epoch, schema_name, table_name) DO NOTHING
            """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch,
                    "schema": manifest.schema_name, "table": manifest.table_name,
                    "columns": json.dumps(manifest.canonical_columns(), separators=(",", ":")),
                    "fingerprint": manifest.fingerprint})
            stored_manifest = connection.execute(text("""
                SELECT columns, schema_fingerprint FROM source_schema_manifests
                WHERE pipeline_id=:pipeline AND stream_epoch=:epoch
                  AND schema_name=:schema AND table_name=:table
            """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch,
                    "schema": manifest.schema_name, "table": manifest.table_name}).mappings().one()
            if (stored_manifest["columns"] != manifest.canonical_columns()
                    or stored_manifest["schema_fingerprint"] != manifest.fingerprint):
                raise RuntimeError("stored source-schema manifest differs from the current source schema")
        connection.execute(text("""
            INSERT INTO stream_epoch_transitions (pipeline_id, stream_epoch, transition)
            VALUES (:pipeline, :epoch, 'provisioned') ON CONFLICT DO NOTHING
        """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch})
    name, connector_config = build_connector_config(
        pipeline, "${directory:/run/secrets:source_capture_password}"
    )
    def connector_was_registered() -> bool:
        with metadata.connect() as connection:
            return bool(connection.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM stream_epoch_transitions
                    WHERE pipeline_id=:pipeline AND stream_epoch=:epoch
                      AND transition='connector_registered'
                )
            """), {"pipeline": pipeline.pipeline_id,
                    "epoch": pipeline.source.stream_epoch}).scalar_one())

    def require_usable_slot() -> None:
        with source.connect() as connection:
            slot = connection.execute(text("""
                SELECT plugin, database, wal_status, invalidation_reason
                FROM pg_replication_slots WHERE slot_name=:slot
            """), {"slot": names["slot"]}).mappings().one_or_none()
        if (slot is None or slot["plugin"] != "pgoutput" or slot["database"] != pipeline.source.endpoint.database
                or slot["wal_status"] not in {"reserved", "extended"}
                or slot["invalidation_reason"] is not None):
            raise RuntimeError("registered connector has a missing or unusable PostgreSQL replication slot; declare a new stream epoch")

    def mark_registered() -> None:
        with metadata.begin() as connection:
            connection.execute(text("""
                INSERT INTO stream_epoch_transitions (pipeline_id, stream_epoch, transition)
                VALUES (:pipeline, :epoch, 'connector_registered') ON CONFLICT DO NOTHING
            """), {"pipeline": pipeline.pipeline_id, "epoch": pipeline.source.stream_epoch})

    status, actual = _request(f"{connect_url}/connectors/{name}/config")
    if status == 200:
        differences = immutable_mismatches(connector_config, {str(k): str(v) for k, v in actual.items()})
        if differences:
            raise RuntimeError(f"existing connector immutable configuration differs: {differences}")
        if connector_was_registered():
            require_usable_slot()
        mark_registered()
        return
    if status != 404:
        raise RuntimeError(f"connector lookup failed with HTTP {status}: {actual}")
    status, result = _request(f"{connect_url}/connectors", method="POST",
                              document={"name": name, "config": connector_config})
    if status not in {200, 201, 409}:
        raise RuntimeError(f"connector registration failed with HTTP {status}: {result}")
    if status == 409:
        check_status, check = _request(f"{connect_url}/connectors/{name}/config")
        if check_status != 200 or immutable_mismatches(connector_config, {str(k): str(v) for k, v in check.items()}):
            raise RuntimeError("concurrent connector registration produced conflicting configuration")
    mark_registered()


def main() -> None:
    provision(
        os.getenv("UPDATIS_RUNTIME_CONFIG", "/etc/updatis/runtime.json"),
        os.getenv("UPDATIS_PIPELINE_CONFIG", "/etc/updatis/pipeline.json"),
        os.getenv("UPDATIS_CONNECT_URL", "http://connect:8083"),
    )


if __name__ == "__main__":
    main()
