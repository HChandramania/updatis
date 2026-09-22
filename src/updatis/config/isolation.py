from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from updatis.config.models import PostgresEndpoint, RuntimeConfigV1

if TYPE_CHECKING:
    from updatis.config.secrets import SecretResolver


class IsolationValidationError(ValueError):
    pass


Resolver = Callable[[str, int], Iterable[str]]


def system_resolver(host: str, port: int) -> set[str]:
    return {item[4][0] for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}


def _normalized_host(host: str) -> str:
    value = host.rstrip(".").lower()
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError:
        return value


def validate_source_metadata_isolation(
    source: PostgresEndpoint,
    runtime: RuntimeConfigV1,
    resolver: Resolver = system_resolver,
    secret_resolver: "SecretResolver | None" = None,
) -> None:
    metadata = runtime.metadata
    metadata_names = {_normalized_host(metadata.host), *(_normalized_host(alias) for alias in runtime.metadata_aliases)}
    if source.instance_id == metadata.instance_id:
        raise IsolationValidationError("source and metadata instance identities must differ")
    if source.password == metadata.password:
        raise IsolationValidationError("source and metadata credential references must differ")
    if secret_resolver is not None:
        source_secret = secret_resolver.resolve(source.password).get_secret_value()
        metadata_secret = secret_resolver.resolve(metadata.password).get_secret_value()
        if source_secret == metadata_secret:
            raise IsolationValidationError("source and metadata resolved credentials must differ")
    if source.port == metadata.port and _normalized_host(source.host) in metadata_names:
        raise IsolationValidationError("source endpoint resolves to the configured metadata identity")
    try:
        source_addresses = {_normalized_host(item) for item in resolver(source.host, source.port)}
        metadata_addresses = set()
        for host in metadata_names:
            metadata_addresses.update(_normalized_host(item) for item in resolver(host, metadata.port))
    except OSError as exc:
        raise IsolationValidationError("source and metadata endpoint identity could not be resolved") from exc
    if not source_addresses or not metadata_addresses:
        raise IsolationValidationError("source and metadata endpoint identity could not be resolved")
    if source.port == metadata.port and source_addresses.intersection(metadata_addresses):
        raise IsolationValidationError("source and metadata endpoints identify the same PostgreSQL instance")
