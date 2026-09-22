from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import uuid
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.yml"
PROJECT = f"updatis-v01a-{os.getpid()}-{uuid.uuid4().hex[:8]}"
SECRETS = ROOT / "deploy" / "secrets"
SECRET_FILES = (
    "metadata_bootstrap_password", "metadata_migration_password", "metadata_runtime_password",
    "source_bootstrap_password", "source_runtime_password",
)


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


def main() -> None:
    SECRETS.mkdir(exist_ok=True)
    for name in SECRET_FILES:
        (SECRETS / name).write_text(f"test-{name}-{uuid.uuid4().hex}\n", encoding="utf-8")
    try:
        run("config", "--quiet")
        run("up", "-d", "--wait", "--wait-timeout", "300", "metadata-db", "source-db", "kafka", "connect")
        pre_migration = run("run", "--rm", "--no-deps", "api", capture=True, check=False)
        assert pre_migration.returncode != 0, "API must reject an unmigrated metadata database"
        run("--profile", "tools", "run", "--rm", "migrate")
        # A second explicit run must be a safe no-op.
        run("--profile", "tools", "run", "--rm", "migrate")
        run("up", "-d", "--wait", "--wait-timeout", "180", "api", "worker")
        with urllib.request.urlopen("http://127.0.0.1:8000/health/ready", timeout=5) as response:
            assert response.status == 200

        assert exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
            "SELECT version_num FROM alembic_version") == "0001_metadata"
        tables = set(exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
            "SELECT tablename FROM pg_tables WHERE schemaname='public'").splitlines())
        expected = {"pipelines", "configuration_revisions", "ingest_records", "events",
                    "ingest_checkpoints", "deliveries", "delivery_attempts", "dead_letters",
                    "ordering_gaps", "audit_records", "alembic_version"}
        assert expected <= tables
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
        denied_result = run("exec", "-T", "metadata-db", "psql", "-v", "ON_ERROR_STOP=1", "-U",
                            "updatis_runtime", "-d", "updatis_metadata", "-c",
                            "CREATE TABLE forbidden_runtime_ddl(id integer)", capture=True, check=False)
        assert denied_result.returncode != 0

        run("stop", "api", "worker", "metadata-db", "source-db", "kafka", "connect")
        run("start", "metadata-db", "source-db", "kafka", "connect")
        run("up", "-d", "--wait", "--wait-timeout", "180", "api", "worker")
        assert exec_sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
                        "SELECT version_num FROM alembic_version") == "0001_metadata"
        print("v0.1-a Compose and migration verification passed")
    finally:
        run("down", "--volumes", "--remove-orphans", check=False)
        for name in SECRET_FILES:
            (SECRETS / name).unlink(missing_ok=True)
        try:
            SECRETS.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    main()
