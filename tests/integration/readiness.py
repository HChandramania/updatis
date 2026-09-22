from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import time
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


class Response(Protocol):
    status: int

    def __enter__(self) -> "Response": ...

    def __exit__(self, *args: object) -> None: ...


OpenUrl = Callable[..., Response]
ServiceState = Callable[[], dict[str, Any]]


class ReadinessError(RuntimeError):
    pass


def _service_failure(state: dict[str, Any]) -> str | None:
    lifecycle = str(state.get("State", state.get("state", "unknown"))).lower()
    health = str(state.get("Health", state.get("health", ""))).lower()
    status = str(state.get("Status", state.get("status", "unknown")))
    if lifecycle in {"dead", "exited", "missing", "removing"}:
        return f"API container is not running: state={lifecycle}, status={status}"
    if health == "unhealthy":
        return f"API container is unhealthy: state={lifecycle}, status={status}"
    return None


def wait_for_http_ready(
    url: str,
    *,
    max_wait_seconds: float = 60.0,
    request_timeout_seconds: float = 1.0,
    retry_interval_seconds: float = 0.25,
    opener: OpenUrl = urlopen,
    monotonic: Callable[[], float] = time.monotonic,
    wait: Callable[[float], None] = time.sleep,
    service_state: ServiceState | None = None,
) -> None:
    """Poll readiness until success or a monotonic, bounded deadline.

    Connection failures and HTTP 503 responses are temporary. A stopped or
    unhealthy container fails immediately so callers can print diagnostics.
    """
    if max_wait_seconds <= 0 or request_timeout_seconds <= 0 or retry_interval_seconds <= 0:
        raise ValueError("readiness timing values must be positive")
    deadline = monotonic() + max_wait_seconds
    last_failure = "no request attempted"

    while True:
        if service_state is not None:
            state = service_state()
            service_failure = _service_failure(state)
            if service_failure:
                raise ReadinessError(service_failure)

        try:
            with opener(url, timeout=request_timeout_seconds) as response:
                if response.status == 200:
                    return
                if response.status == 503:
                    last_failure = "HTTP 503: service is temporarily not ready"
                else:
                    raise ReadinessError(f"unexpected readiness HTTP status {response.status}")
        except HTTPError as exc:
            if exc.code != 503:
                raise ReadinessError(f"unexpected readiness HTTP status {exc.code}") from exc
            last_failure = "HTTP 503: service is temporarily not ready"
        except (URLError, ConnectionError, TimeoutError, OSError) as exc:
            last_failure = f"{exc.__class__.__name__}: {exc}"

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise ReadinessError(
                f"readiness deadline exceeded after {max_wait_seconds:g}s; last failure: {last_failure}"
            )
        wait(min(retry_interval_seconds, remaining))
