from __future__ import annotations

from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SecretReference(StrictModel):
    provider: Literal["env", "file"]
    name: Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")]


class PostgresEndpoint(StrictModel):
    host: Annotated[str, Field(min_length=1, max_length=253)]
    port: Annotated[int, Field(ge=1, le=65535)] = 5432
    database: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")]
    username: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")]
    password: SecretReference
    instance_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")]


class RuntimeConfigV1(StrictModel):
    schema_version: Literal[1]
    environment: Literal["development", "test", "staging", "production"] = "development"
    metadata: PostgresEndpoint
    metadata_aliases: tuple[Annotated[str, Field(min_length=1, max_length=253)], ...] = ()
    secrets_directory: str = "/run/secrets"
    schema_revision: Literal["0001_metadata"] = "0001_metadata"


class SourceTable(StrictModel):
    schema_name: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")]
    table_name: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")]
    key_columns: tuple[Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")], ...]
    key_types: tuple[Literal["smallint", "integer", "bigint", "uuid", "char", "varchar", "text"], ...]

    @model_validator(mode="after")
    def validate_keys(self) -> "SourceTable":
        if not 1 <= len(self.key_columns) <= 4:
            raise ValueError("key_columns must contain between one and four columns")
        if len(self.key_columns) != len(self.key_types):
            raise ValueError("key_columns and key_types must have the same length")
        if len(set(self.key_columns)) != len(self.key_columns):
            raise ValueError("key_columns must be unique")
        return self


class SourceConfig(StrictModel):
    kind: Literal["postgresql"]
    endpoint: PostgresEndpoint
    stream_epoch: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")]
    snapshot_mode: Literal["initial"] = "initial"
    connector_tasks: Literal[1] = 1
    partitions_per_table: Literal[1] = 1
    tables: tuple[SourceTable, ...]

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, value: tuple[SourceTable, ...]) -> tuple[SourceTable, ...]:
        if not 1 <= len(value) <= 10:
            raise ValueError("tables must contain between one and ten entries")
        identities = {(item.schema_name, item.table_name) for item in value}
        if len(identities) != len(value):
            raise ValueError("tables must be unique")
        return value


class DestinationConfig(StrictModel):
    kind: Literal["webhook"]
    url: Annotated[str, Field(min_length=1, max_length=2048)]
    signing_secret: SecretReference

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("url must be an absolute HTTP or HTTPS URL")
        if parsed.username or parsed.password or parsed.fragment or parsed.query:
            raise ValueError("url cannot contain credentials, a query, or a fragment")
        return value


class RetryConfig(StrictModel):
    max_attempts: Annotated[int, Field(ge=1, le=100)] = 10
    max_age_seconds: Annotated[int, Field(ge=1, le=604800)] = 86400
    initial_backoff_seconds: Annotated[float, Field(gt=0, le=60)] = 1
    max_backoff_seconds: Annotated[float, Field(ge=1, le=3600)] = 300
    connect_timeout_seconds: Annotated[float, Field(gt=0, le=60)] = 5
    total_timeout_seconds: Annotated[float, Field(gt=0, le=300)] = 30

    @model_validator(mode="after")
    def validate_ordering(self) -> "RetryConfig":
        if self.initial_backoff_seconds > self.max_backoff_seconds:
            raise ValueError("initial_backoff_seconds cannot exceed max_backoff_seconds")
        if self.connect_timeout_seconds > self.total_timeout_seconds:
            raise ValueError("connect_timeout_seconds cannot exceed total_timeout_seconds")
        return self


class CapacityConfig(StrictModel):
    high_watermark_events: Annotated[int, Field(ge=1000, le=10_000_000)] = 100_000
    high_watermark_bytes: Annotated[int, Field(ge=1_048_576, le=1_099_511_627_776)] = 10_737_418_240
    resume_percent: Annotated[int, Field(ge=1, le=99)] = 80
    max_active_partitions: Annotated[int, Field(ge=1, le=64)] = 8
    inflight_per_partition: Literal[1] = 1


class RetentionConfig(StrictModel):
    kafka_seconds: Annotated[int, Field(ge=3600, le=2_592_000)] = 604_800
    payload_seconds: Annotated[int, Field(ge=3600, le=7_776_000)] = 2_592_000


class PipelineConfigV1(StrictModel):
    schema_version: Literal[1]
    pipeline_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")]
    envelope_version: Literal[1] = 1
    source: SourceConfig
    destination: DestinationConfig
    retry: RetryConfig = RetryConfig()
    capacity: CapacityConfig = CapacityConfig()
    retention: RetentionConfig = RetentionConfig()
    allow_local_http: bool = False
    local_http_allowlist: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_destination_policy(self) -> "PipelineConfigV1":
        parsed = urlsplit(self.destination.url)
        authority = parsed.netloc.lower()
        if parsed.scheme == "http":
            if not self.allow_local_http or authority not in {item.lower() for item in self.local_http_allowlist}:
                raise ValueError("HTTP destinations require explicit local development allowlisting")
        return self


class ResolvedPostgresEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True)
    host: str
    port: int
    database: str
    username: str
    password: SecretStr
    instance_id: str
