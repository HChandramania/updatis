from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

try:
    from tests.integration.readiness import wait_for_http_ready
except ModuleNotFoundError:  # Direct script execution sets tests/integration as sys.path[0].
    from readiness import wait_for_http_ready


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.yml"
PROJECT = f"updatis-v01a-{os.getpid()}-{uuid.uuid4().hex[:8]}"
SECRETS = ROOT / "deploy" / "secrets"
SECRET_FILES = (
    "metadata_bootstrap_password", "metadata_migration_password", "metadata_runtime_password",
    "source_bootstrap_password", "source_runtime_password",
)
CANONICAL_DATABASE = "updatis_metadata"
NEGATIVE_DATABASES = {
    "unmarked": "updatis_test_unmarked",
    "missing_marker": "updatis_test_missing_marker",
    "missing_revision": "updatis_test_missing_revision",
    "wrong_revision": "updatis_test_wrong_revision",
}
GENERATED_CONFIGS: list[Path] = []


def run(*args: str, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), *args]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=capture, check=False)
    if check and result.returncode:
        if capture:
            sys.stderr.write(result.stdout)
            sys.stderr.write(result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def exec_sql(service: str, user: str, database: str, sql: str, check: bool = True) -> str:
    result = run("exec", "-T", service, "psql", "-v", "ON_ERROR_STOP=1", "-At",
                 "-U", user, "-d", database, "-c", sql, capture=True, check=check)
    return result.stdout.strip()


def assert_metadata_sql_denied(user: str, sql: str) -> None:
    result = run(
        "exec", "-T", "metadata-db", "psql", "-v", "ON_ERROR_STOP=1", "-U", user,
        "-d", "updatis_metadata", "-c", sql, capture=True, check=False,
    )
    assert result.returncode != 0, f"{user} unexpectedly executed marker mutation: {sql}"


def create_disposable_schema_database(case: str) -> None:
    database = NEGATIVE_DATABASES[case]
    assert database != CANONICAL_DATABASE
    exec_sql("metadata-db", "updatis_bootstrap", "postgres", f"CREATE DATABASE {database}")
    if case == "unmarked":
        exec_sql(
            "metadata-db", "updatis_bootstrap", database,
            "GRANT CREATE ON SCHEMA public TO updatis_migration",
        )
        return

    statements: list[str] = []
    if case != "missing_marker":
        statements.extend([
            "CREATE SCHEMA updatis_bootstrap",
            "CREATE TABLE updatis_bootstrap.metadata_instance ("
            "singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), instance_id text NOT NULL UNIQUE)",
            "INSERT INTO updatis_bootstrap.metadata_instance(singleton, instance_id) "
            "VALUES(true, 'updatis-metadata-local')",
            "GRANT USAGE ON SCHEMA updatis_bootstrap TO updatis_runtime",
            "GRANT SELECT ON updatis_bootstrap.metadata_instance TO updatis_runtime",
        ])
    if case != "missing_revision":
        revision = "0000_wrong" if case == "wrong_revision" else "0001_metadata"
        statements.extend([
            "CREATE TABLE alembic_version (version_num varchar(32) PRIMARY KEY)",
            f"INSERT INTO alembic_version(version_num) VALUES('{revision}')",
            "GRANT SELECT ON alembic_version TO updatis_runtime",
        ])
    exec_sql("metadata-db", "updatis_bootstrap", database, ";".join(statements))


def write_database_config(case: str, *, migration: bool) -> Path:
    source = "migration.example.json" if migration else "runtime.example.json"
    document = json.loads((ROOT / "deploy/config" / source).read_text(encoding="utf-8"))
    document["metadata"]["database"] = NEGATIVE_DATABASES[case]
    purpose = "migration" if migration else "runtime"
    path = SECRETS / f"{case}-{purpose}.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    GENERATED_CONFIGS.append(path)
    return path


def assert_schema_probe_rejected(case: str) -> None:
    config = write_database_config(case, migration=False)
    result = run(
        "run", "--rm", "--no-deps", "--entrypoint", "python", "-v",
        f"{config.resolve()}:/tmp/test-runtime.json:ro", "api", "-c",
        "from updatis.health import check_metadata; check_metadata('/tmp/test-runtime.json')",
        capture=True, check=False,
    )
    assert result.returncode != 0, f"schema probe unexpectedly accepted {case} database"
    assert "SchemaCompatibilityError" in (result.stdout + result.stderr)


def assert_canonical_schema_intact() -> None:
    marker_and_revision = exec_sql(
        "metadata-db", "updatis_runtime", CANONICAL_DATABASE, """
        SELECT (SELECT instance_id FROM updatis_bootstrap.metadata_instance WHERE singleton)
               || ',' ||
               (SELECT version_num FROM alembic_version)
        """,
    )
    assert marker_and_revision == "updatis-metadata-local,0001_metadata"
    tables = set(exec_sql(
        "metadata-db", "updatis_runtime", CANONICAL_DATABASE,
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
    ).splitlines())
    expected = {
        "pipelines", "configuration_revisions", "ingest_records", "events",
        "ingest_checkpoints", "deliveries", "delivery_attempts", "dead_letters",
        "ordering_gaps", "audit_records", "alembic_version",
    }
    assert expected <= tables


def _json_records(raw: str) -> list[dict]:
    if not raw.strip():
        return []
    try:
        value = json.loads(raw)
        return value if isinstance(value, list) else [value]
    except json.JSONDecodeError:
        return [json.loads(line) for line in raw.splitlines() if line.strip()]


def service_state(service: str) -> dict:
    result = run("ps", "-a", "--format", "json", service, capture=True, check=False)
    if result.returncode:
        return {"State": "missing", "Status": result.stderr.strip() or "compose ps failed"}
    records = _json_records(result.stdout)
    return records[0] if records else {"State": "missing", "Status": "container not found"}


def assert_service_healthy(service: str) -> None:
    state = service_state(service)
    lifecycle = str(state.get("State", "")).lower()
    health = str(state.get("Health", "")).lower()
    assert lifecycle == "running", f"{service} is not running: {state}"
    assert health == "healthy", f"{service} is not healthy: {state}"


def assert_compose_contract() -> None:
    rendered = json.loads(run("config", "--format", "json", capture=True).stdout)
    api = rendered["services"]["api"]
    worker = rendered["services"]["worker"]
    assert api["command"] == ["updatis.api"]
    assert worker["command"] == ["updatis.worker"]
    assert api["environment"]["UPDATIS_RUNTIME_CONFIG"] == "/etc/updatis/runtime.json"
    assert worker["environment"]["UPDATIS_RUNTIME_CONFIG"] == "/etc/updatis/runtime.json"
    assert api.get("healthcheck", {}).get("test")
    assert worker.get("healthcheck", {}).get("test")
    assert set(api["networks"]) == {"metadata", "api-edge"}
    assert set(rendered["services"]["metadata-db"]["networks"]) == {"metadata"}
    assert all(
        "api-edge" not in service.get("networks", {})
        for name, service in rendered["services"].items()
        if name != "api"
    )
    assert rendered["networks"]["metadata"]["internal"] is True
    assert not rendered["networks"]["api-edge"].get("internal", False)
    port = api["ports"][0]
    assert str(port["target"]) == "8000"
    assert str(port["published"]) == "8000"
    assert port["host_ip"] == "127.0.0.1"


def assert_runtime_mounts(service: str) -> None:
    run(
        "exec", "-T", service, "python", "-c",
        "from pathlib import Path; "
        "assert Path('/etc/updatis/runtime.json').is_file(); "
        "assert Path('/run/secrets/metadata_runtime_password').is_file()",
    )


def assert_api_listener_and_publication() -> None:
    run(
        "exec", "-T", "api", "python", "-c",
        "import socket; connection=socket.create_connection(('127.0.0.1', 8000), timeout=2); "
        "connection.close()",
    )
    published = run("port", "api", "8000", capture=True).stdout.strip().splitlines()
    assert "127.0.0.1:8000" in published, f"unexpected API port publication: {published!r}"


def print_diagnostics() -> None:
    sys.stderr.write("\n===== docker compose ps -a =====\n")
    ps = run("ps", "-a", capture=True, check=False)
    sys.stderr.write(ps.stdout or ps.stderr)
    sys.stderr.write("\n===== container health/status JSON =====\n")
    status = run("ps", "-a", "--format", "json", capture=True, check=False)
    sys.stderr.write(status.stdout or status.stderr)
    sys.stderr.write("\n===== effective API command and port =====\n")
    rendered = run("config", "--format", "json", capture=True, check=False)
    if rendered.returncode == 0:
        try:
            api = json.loads(rendered.stdout)["services"]["api"]
            sys.stderr.write(f"entrypoint={api.get('entrypoint')} command={api.get('command')}\n")
        except (KeyError, json.JSONDecodeError) as exc:
            sys.stderr.write(f"could not parse rendered API command: {exc}\n")
    else:
        sys.stderr.write(rendered.stderr)
    port = run("port", "api", "8000", capture=True, check=False)
    sys.stderr.write(f"docker compose port api 8000: {port.stdout or port.stderr}\n")
    for service in ("api", "worker", "metadata-db"):
        sys.stderr.write(f"\n===== {service} logs =====\n")
        logs = run("logs", "--no-color", service, capture=True, check=False)
        sys.stderr.write(logs.stdout)
        sys.stderr.write(logs.stderr)
        container_ids = run("ps", "-a", "-q", service, capture=True, check=False).stdout.split()
        for container_id in container_ids:
            inspected = subprocess.run(
                ["docker", "inspect", "--format",
                 "{{json .State}} {{json .Config.Entrypoint}} {{json .Config.Cmd}}", container_id],
                cwd=ROOT, text=True, capture_output=True, check=False,
            )
            sys.stderr.write(f"\n{service} inspect state: {inspected.stdout or inspected.stderr}")


def main() -> None:
    SECRETS.mkdir(exist_ok=True)
    for name in SECRET_FILES:
        (SECRETS / name).write_text(f"test-{name}-{uuid.uuid4().hex}\n", encoding="utf-8")
    try:
        assert_compose_contract()
        run("up", "-d", "--wait", "--wait-timeout", "300", "metadata-db", "source-db", "kafka", "connect")
        for role in ("updatis_migration", "updatis_runtime"):
            assert exec_sql(
                "metadata-db", role, "updatis_metadata",
                "SELECT instance_id FROM updatis_bootstrap.metadata_instance WHERE singleton",
            ) == "updatis-metadata-local"
            privileges = exec_sql(
                "metadata-db", role, "updatis_metadata", """
                SELECT has_schema_privilege(current_user, 'updatis_bootstrap', 'USAGE') || ',' ||
                       has_table_privilege(current_user, 'updatis_bootstrap.metadata_instance', 'SELECT') || ',' ||
                       has_table_privilege(current_user, 'updatis_bootstrap.metadata_instance', 'INSERT') || ',' ||
                       has_table_privilege(current_user, 'updatis_bootstrap.metadata_instance', 'UPDATE') || ',' ||
                       has_table_privilege(current_user, 'updatis_bootstrap.metadata_instance', 'DELETE') || ',' ||
                       has_table_privilege(current_user, 'updatis_bootstrap.metadata_instance', 'TRUNCATE')
                """,
            )
            assert privileges == "true,true,false,false,false,false"
            for mutation in (
                "INSERT INTO updatis_bootstrap.metadata_instance(singleton, instance_id) VALUES(false, 'forbidden')",
                "UPDATE updatis_bootstrap.metadata_instance SET instance_id='forbidden' WHERE singleton",
                "DELETE FROM updatis_bootstrap.metadata_instance WHERE singleton",
                "TRUNCATE updatis_bootstrap.metadata_instance",
            ):
                assert_metadata_sql_denied(role, mutation)

        # Establish the canonical runtime schema before any negative schema
        # probes. Those probes use distinct disposable databases below.
        run("--profile", "tools", "run", "--rm", "migrate")
        # A second explicit run must be a safe no-op.
        run("--profile", "tools", "run", "--rm", "migrate")
        assert_canonical_schema_intact()

        for case in NEGATIVE_DATABASES:
            create_disposable_schema_database(case)
        unmarked_config = write_database_config("unmarked", migration=True)
        unmarked = run(
            "--profile", "tools", "run", "--rm", "--no-deps", "-v",
            f"{unmarked_config.resolve()}:/tmp/unmarked-runtime.json:ro", "migrate",
            "updatis.db.migrate", "--config", "/tmp/unmarked-runtime.json",
            capture=True, check=False,
        )
        assert unmarked.returncode != 0, "migration must reject an unmarked database"
        assert "metadata_instance" in (unmarked.stdout + unmarked.stderr)
        for case in NEGATIVE_DATABASES:
            assert_schema_probe_rejected(case)
        # Regression guard: no negative case may damage the database that API
        # and worker are about to use.
        assert_canonical_schema_intact()

        # API and worker are started only after both migration runs succeeded.
        run("up", "-d", "--wait", "--wait-timeout", "180", "api", "worker")
        assert_service_healthy("api")
        assert_service_healthy("worker")
        assert_api_listener_and_publication()
        assert_runtime_mounts("api")
        assert_runtime_mounts("worker")

        assert exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
            "SELECT version_num FROM alembic_version") == "0001_metadata"
        for role in ("updatis_migration", "updatis_runtime"):
            assert exec_sql("metadata-db", role, "updatis_metadata",
                            "SELECT version_num FROM alembic_version") == "0001_metadata"
        # Docker's API health check has already proven the in-container listener
        # and metadata readiness. This bounded poll separately proves the host
        # loopback publication, tolerating short forwarding propagation delays.
        wait_for_http_ready(
            "http://127.0.0.1:8000/health/ready",
            max_wait_seconds=60,
            request_timeout_seconds=1,
            retry_interval_seconds=0.5,
            service_state=lambda: service_state("api"),
        )
        assert_canonical_schema_intact()
        exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata", """
            INSERT INTO pipelines (id, name, source_instance_id, source_stream_epoch)
            VALUES ('00000000-0000-0000-0000-000000000001', 'constraint-proof', 'source-proof', 'epoch-proof');
            INSERT INTO configuration_revisions
                (id, pipeline_id, revision, schema_version, document, content_hash)
            VALUES ('10000000-0000-0000-0000-000000000001',
                    '00000000-0000-0000-0000-000000000001', 1, 1, '{}', repeat('a', 64));
        """)
        duplicate = run("exec", "-T", "metadata-db", "psql", "-v", "ON_ERROR_STOP=1", "-U",
                        "updatis_bootstrap", "-d", "updatis_metadata", "-c", """
            INSERT INTO configuration_revisions
                (pipeline_id, revision, schema_version, document, content_hash)
            VALUES ('00000000-0000-0000-0000-000000000001', 1, 1, '{}', repeat('b', 64));
        """, capture=True, check=False)
        assert duplicate.returncode != 0, "configuration revision uniqueness must be enforced"
        immutable = run("exec", "-T", "metadata-db", "psql", "-v", "ON_ERROR_STOP=1", "-U",
                        "updatis_bootstrap", "-d", "updatis_metadata", "-c",
                        "UPDATE configuration_revisions SET revision=2", capture=True, check=False)
        assert immutable.returncode != 0, "configuration revisions must be immutable"

        source_tables = exec_sql("source-db", "source_bootstrap", "example_source",
            "SELECT schemaname || '.' || tablename FROM pg_tables ORDER BY 1")
        assert "example.orders" in source_tables and "pipelines" not in source_tables
        assert exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
            "SELECT count(*) FROM pg_roles WHERE rolname='example_app'") == "0"
        assert exec_sql("source-db", "source_bootstrap", "example_source",
            "SELECT count(*) FROM pg_roles WHERE rolname IN ('updatis_runtime','updatis_migration')") == "0"
        assert exec_sql("source-db", "source_bootstrap", "example_source",
            "SELECT count(*) FROM pg_publication WHERE pubname NOT LIKE 'pg_%'") == "0"
        assert exec_sql("source-db", "source_bootstrap", "example_source",
            "SELECT count(*) FROM pg_replication_slots") == "0"

        connectors = run("exec", "-T", "connect", "curl", "-fsS", "http://localhost:8083/connectors",
                         capture=True).stdout.strip()
        assert connectors == "[]"
        topics = run("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                     "localhost:29092", "--list", capture=True).stdout.splitlines()
        assert set(topics) <= {"__consumer_offsets", "updatis_connect_configs", "updatis_connect_offsets",
                               "updatis_connect_statuses"}

        # psql must fail because the runtime role has no CREATE privilege.
        assert_metadata_sql_denied("updatis_runtime", "CREATE TABLE forbidden_runtime_ddl(id integer)")

        run("stop", "api", "worker", "metadata-db", "source-db", "kafka", "connect")
        run("start", "metadata-db", "source-db", "kafka", "connect")
        run("up", "-d", "--wait", "--wait-timeout", "180", "api", "worker")
        assert_service_healthy("api")
        assert_service_healthy("worker")
        assert_api_listener_and_publication()
        wait_for_http_ready(
            "http://127.0.0.1:8000/health/ready",
            max_wait_seconds=60,
            request_timeout_seconds=1,
            retry_interval_seconds=0.5,
            service_state=lambda: service_state("api"),
        )
        assert exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
                        "SELECT version_num FROM alembic_version") == "0001_metadata"
        print("v0.1-a Compose and migration verification passed")
    except BaseException:
        print_diagnostics()
        raise
    finally:
        run("down", "--volumes", "--remove-orphans", check=False)
        for name in SECRET_FILES:
            (SECRETS / name).unlink(missing_ok=True)
        for path in GENERATED_CONFIGS:
            path.unlink(missing_ok=True)
        try:
            SECRETS.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    main()
