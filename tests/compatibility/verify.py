from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "tests" / "compatibility" / "compose.yml"
MATRIX = json.loads((ROOT / "compatibility" / "candidate-matrix.json").read_text(encoding="utf-8"))
PROJECT = f"updatis-foundation-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def run(*args: str, capture: bool = False) -> str:
    command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), *args]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=capture, check=False)
    if result.returncode:
        if capture:
            sys.stderr.write(result.stdout)
            sys.stderr.write(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)
    return result.stdout.strip() if capture else ""


def assert_contains(actual: str, expected: str, component: str) -> None:
    if expected not in actual:
        raise AssertionError(f"{component}: expected {expected!r} in {actual!r}")


def inspect_exact_image(reference: str) -> None:
    result = subprocess.run(
        ["docker", "image", "inspect", reference, "--format", "{{json .RepoTags}}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)
    tags = json.loads(result.stdout)
    if reference not in tags:
        raise AssertionError(f"expected exact image {reference!r}; found tags {tags!r}")


def main() -> None:
    if MATRIX["status"] not in {"candidate", "validated"}:
        raise AssertionError("matrix status must be candidate or validated")
    try:
        run("config", "--quiet")
        run("up", "-d", "--wait", "--wait-timeout", "240")

        postgres = run("exec", "-T", "postgres", "postgres", "--version", capture=True)
        assert_contains(postgres, MATRIX["postgresql"], "PostgreSQL")
        run("exec", "-T", "postgres", "psql", "-U", "foundation", "-d", "foundation", "-c", "SELECT version();")

        kafka = run("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version", capture=True)
        assert_contains(kafka, MATRIX["kafka_broker"], "Kafka")
        run("exec", "-T", "kafka", "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:29092")

        plugins_raw = run("exec", "-T", "connect", "curl", "-fsS", "http://localhost:8083/connector-plugins", capture=True)
        plugins = json.loads(plugins_raw)
        classes = {plugin["class"] for plugin in plugins}
        required = "io.debezium.connector.postgresql.PostgresConnector"
        if required not in classes:
            raise AssertionError(f"Debezium PostgreSQL plugin missing; found {sorted(classes)}")

        connect_info = json.loads(
            run("exec", "-T", "connect", "curl", "-fsS", "http://localhost:8083/", capture=True)
        )
        if connect_info.get("version") != MATRIX["kafka_connect"]:
            raise AssertionError(
                f"Kafka Connect: expected {MATRIX['kafka_connect']!r}; "
                f"reported {connect_info.get('version')!r}"
            )

        expected_tags = {
            f"postgres:{MATRIX['postgresql']}",
            f"apache/kafka:{MATRIX['kafka_broker']}",
            f"quay.io/debezium/connect:{MATRIX['debezium']}",
        }
        for expected in expected_tags:
            inspect_exact_image(expected)

        print("Foundation container interoperability check passed")
    finally:
        run("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
