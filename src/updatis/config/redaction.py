from __future__ import annotations

from typing import Any

from pydantic import BaseModel, SecretStr

_SENSITIVE_KEYS = {"password", "secret", "token", "authorization", "credential"}


def redact(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return "[REDACTED]"
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    if isinstance(value, dict):
        if set(value) == {"provider", "name"} and value.get("provider") in {"env", "file"}:
            return dict(value)
        result = {}
        for key, item in value.items():
            is_reference = (
                isinstance(item, dict)
                and set(item) == {"provider", "name"}
                and item.get("provider") in {"env", "file"}
            )
            result[key] = (
                "[REDACTED]"
                if not is_reference and any(word in key.lower() for word in _SENSITIVE_KEYS)
                else redact(item)
            )
        return result
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value
