from __future__ import annotations

from urllib.parse import quote

from sqlalchemy import Engine, create_engine

from updatis.config.models import PostgresEndpoint
from updatis.config.secrets import SecretResolver


def database_url(endpoint: PostgresEndpoint, resolver: SecretResolver) -> str:
    password = resolver.resolve(endpoint.password).get_secret_value()
    return (
        f"postgresql+psycopg://{quote(endpoint.username, safe='')}:{quote(password, safe='')}"
        f"@{endpoint.host}:{endpoint.port}/{quote(endpoint.database, safe='')}"
    )


def create_metadata_engine(endpoint: PostgresEndpoint, resolver: SecretResolver) -> Engine:
    return create_engine(database_url(endpoint, resolver), pool_pre_ping=True)
