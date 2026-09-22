from __future__ import annotations

import os
from pathlib import Path

from pydantic import SecretStr

from updatis.config.models import SecretReference


class SecretResolutionError(ValueError):
    pass


class SecretResolver:
    def __init__(self, secrets_directory: str | Path, environ: dict[str, str] | None = None):
        self._root = Path(secrets_directory).resolve()
        self._environ = os.environ if environ is None else environ

    def resolve(self, reference: SecretReference) -> SecretStr:
        if reference.provider == "env":
            value = self._environ.get(reference.name)
        else:
            candidate = self._root / reference.name
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(self._root)
            except (FileNotFoundError, OSError, ValueError) as exc:
                raise SecretResolutionError(f"secret reference {reference.name!r} is unavailable") from exc
            if not resolved.is_file():
                raise SecretResolutionError(f"secret reference {reference.name!r} is unavailable")
            value = resolved.read_text(encoding="utf-8").rstrip("\r\n")
        if not value or len(value.encode("utf-8")) > 65_536:
            raise SecretResolutionError(f"secret reference {reference.name!r} is empty or too large")
        return SecretStr(value)
