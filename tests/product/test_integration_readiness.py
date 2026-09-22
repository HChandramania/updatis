from __future__ import annotations

from urllib.error import URLError

import pytest

from tests.integration.readiness import ReadinessError, wait_for_http_ready


class Response:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> None:
        return None


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def wait(self, seconds: float) -> None:
        self.value += seconds


def sequential_opener(*outcomes):
    remaining = list(outcomes)

    def open_url(url: str, timeout: float):
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return Response(outcome)

    return open_url


def test_initial_connection_refusal_then_success() -> None:
    clock = Clock()
    wait_for_http_ready(
        "http://127.0.0.1:8000/health/ready",
        opener=sequential_opener(URLError("connection refused"), 200),
        monotonic=clock.monotonic,
        wait=clock.wait,
    )
    assert clock.value > 0


def test_temporary_503_then_success() -> None:
    clock = Clock()
    wait_for_http_ready(
        "http://127.0.0.1:8000/health/ready",
        opener=sequential_opener(503, 200),
        monotonic=clock.monotonic,
        wait=clock.wait,
    )
    assert clock.value > 0


def test_permanent_refusal_reaches_monotonic_deadline() -> None:
    clock = Clock()

    def refuse(url: str, timeout: float):
        raise URLError("connection refused")

    with pytest.raises(ReadinessError, match=r"deadline exceeded after 1s.*connection refused"):
        wait_for_http_ready(
            "http://127.0.0.1:8000/health/ready",
            max_wait_seconds=1,
            retry_interval_seconds=0.25,
            opener=refuse,
            monotonic=clock.monotonic,
            wait=clock.wait,
        )
    assert clock.value == 1


@pytest.mark.parametrize(
    ("state", "message"),
    [
        ({"State": "exited", "Status": "Exited (1)"}, r"not running.*Exited \(1\)"),
        ({"State": "running", "Health": "unhealthy", "Status": "Up 4s (unhealthy)"},
         "unhealthy.*Up 4s"),
    ],
)
def test_exited_or_unhealthy_container_fails_with_useful_status(state: dict, message: str) -> None:
    with pytest.raises(ReadinessError, match=message):
        wait_for_http_ready(
            "http://127.0.0.1:8000/health/ready",
            opener=sequential_opener(200),
            service_state=lambda: state,
        )
