from __future__ import annotations

import json
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from updatis.config.models import PipelineConfigV1, RuntimeConfigV1
from updatis.config.isolation import Resolver, system_resolver, validate_source_metadata_isolation
from updatis.config.secrets import SecretResolver

T = TypeVar("T", bound=BaseModel)


class ConfigurationValidationError(ValueError):
    pass


def _load(path: str | Path, model: type[T]) -> T:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
        return model.model_validate(document)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationValidationError("configuration could not be read as a JSON document") from exc
    except ValidationError as exc:
        issues = [
            {"location": ".".join(str(part) for part in item["loc"]), "type": item["type"]}
            for item in exc.errors(include_input=False, include_context=False, include_url=False)
        ]
        raise ConfigurationValidationError(f"configuration validation failed: {issues!r}") from None


def load_runtime_config(path: str | Path) -> RuntimeConfigV1:
    return _load(path, RuntimeConfigV1)


def load_pipeline_config(path: str | Path) -> PipelineConfigV1:
    return _load(path, PipelineConfigV1)


def validate_configuration_bundle(
    runtime: RuntimeConfigV1,
    pipeline: PipelineConfigV1,
    endpoint_resolver: Resolver = system_resolver,
    secret_resolver: SecretResolver | None = None,
) -> None:
    secrets = secret_resolver or SecretResolver(runtime.secrets_directory)
    secrets.resolve(runtime.metadata.password)
    secrets.resolve(pipeline.source.endpoint.password)
    secrets.resolve(pipeline.destination.signing_secret)
    validate_source_metadata_isolation(
        pipeline.source.endpoint, runtime, endpoint_resolver, secret_resolver=secrets
    )
    if pipeline.allow_local_http and runtime.environment not in {"development", "test"}:
        raise ValueError("local HTTP destinations are allowed only in development and test")
