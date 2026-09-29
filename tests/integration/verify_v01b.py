from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy/compose.yml"
PROJECT = f"updatis-v01b-{os.getpid()}-{uuid.uuid4().hex[:8]}"
SECRETS_ROOT = ROOT / ".test-secrets"
SECRETS = SECRETS_ROOT / PROJECT
SECRET_FILES = (
    "metadata_bootstrap_password", "metadata_migration_password", "metadata_runtime_password",
    "source_bootstrap_password", "source_runtime_password", "source_capture_password",
)
SECRET_VALUES = {
    "metadata_bootstrap_password": "V01bBootstrapPass123",
    "metadata_migration_password": "V01bMigrationPass123",
    "metadata_runtime_password": "V01bRuntimePass123",
    "source_bootstrap_password": "V01bSourceBootPass123",
    "source_runtime_password": "V01bSourceRunPass123",
    "source_capture_password": "V01bSourceCapturePass123",
}
CONNECTOR = "updatis-pg-00000000000000000000000000000001-000000000101"
GROUP = "updatis-intake-00000000000000000000000000000001-00000000000000000000000000000101"
ORDERS_TOPIC = "updatis.00000000000000000000000000000001.00000000000000000000000000000101.example.orders"


def compose(*args: str, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), *args]
    environment = dict(os.environ, UPDATIS_SECRETS_DIR=str(SECRETS.resolve()),
                       UPDATIS_CAPTURE_ENABLED="true")
    result = subprocess.run(command, cwd=ROOT, env=environment, text=True, capture_output=capture, check=False)
    if check and result.returncode:
        if capture:
            sys.stderr.write(result.stdout + result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def sql(service: str, user: str, database: str, statement: str) -> str:
    return compose("exec", "-T", service, "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                   "-U", user, "-d", database, "-c", statement, capture=True).stdout.strip()


def wait_until(description: str, predicate, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return
        except Exception as exc:  # diagnostics retain the decisive service logs
            last = repr(exc)
        time.sleep(0.5)
    raise AssertionError(f"deadline waiting for {description}; last={last!r}")


def connector_status() -> dict:
    raw = compose("exec", "-T", "connect", "curl", "-fsS",
                  f"http://localhost:8083/connectors/{CONNECTOR}/status", capture=True).stdout
    return json.loads(raw)


def diagnostics() -> None:
    for args, label in [
        (("ps", "-a"), "compose ps"),
        (("logs", "--no-color", "worker"), "worker logs"),
        (("logs", "--no-color", "connect"), "connect logs"),
        (("logs", "--no-color", "kafka"), "kafka logs"),
        (("logs", "--no-color", "source-db"), "source logs"),
        (("logs", "--no-color", "metadata-db"), "metadata logs"),
    ]:
        result = compose(*args, capture=True, check=False)
        sys.stderr.write(f"\n===== {label} =====\n{result.stdout}{result.stderr}")
    status = compose("exec", "-T", "connect", "curl", "-sS",
                     f"http://localhost:8083/connectors/{CONNECTOR}/status", capture=True, check=False)
    sys.stderr.write(f"\n===== connector status =====\n{status.stdout}{status.stderr}")
    offsets = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                      "--bootstrap-server", "kafka:29092", "--group", GROUP, "--describe",
                      capture=True, check=False)
    sys.stderr.write(f"\n===== consumer offsets =====\n{offsets.stdout}{offsets.stderr}")
    for query, label in [
        ("SELECT slot_name,plugin,database,active,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots", "slots"),
        ("SELECT pubname,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication", "publications"),
        ("SELECT * FROM pg_publication_tables ORDER BY 1,2,3", "publication tables"),
    ]:
        try:
            sys.stderr.write(f"\n===== {label} =====\n{sql('source-db','source_bootstrap','example_source',query)}\n")
        except Exception as exc:
            sys.stderr.write(f"{exc!r}\n")


def main() -> None:
    SECRETS.mkdir(parents=True, exist_ok=True)
    for name in SECRET_FILES:
        (SECRETS / name).write_text(SECRET_VALUES[name], encoding="utf-8")
    try:
        compose("up", "-d", "--wait", "--wait-timeout", "300", "metadata-db", "source-db", "kafka", "connect")
        compose("--profile", "tools", "run", "--rm", "migrate")
        sql("source-db", "source_bootstrap", "example_source", """
            INSERT INTO example.orders
                (customer_name,status,amount,binary_value,local_time,zoned_time,local_stamp,instant,payload)
            VALUES ('snapshot','pending',12.3400,decode('00ff','hex'),'23:59:59.123456',
                    '01:30:00-04','1969-12-31 23:59:59.123456','2024-11-03 01:30:00-04','{"snapshot":true}');
            INSERT INTO example.customers(id,display_name)
            VALUES ('10000000-0000-0000-0000-000000000001','snapshot-customer');
        """)
        compose("--profile", "capture", "build", "provision-capture")
        registration_command = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE),
                                "--profile", "capture", "run", "--rm", "provision-capture"]
        registration_environment = dict(os.environ, UPDATIS_SECRETS_DIR=str(SECRETS.resolve()))
        registrations = [subprocess.Popen(registration_command, cwd=ROOT, env=registration_environment, text=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                         for _ in range(2)]
        registration_results = [process.communicate(timeout=180) for process in registrations]
        assert all(process.returncode == 0 for process in registrations), registration_results
        wait_until("connector task RUNNING", lambda: (
            connector_status().get("connector", {}).get("state") == "RUNNING"
            and connector_status().get("tasks", [{}])[0].get("state") == "RUNNING"
        ))
        compose("up", "-d", "--wait", "--wait-timeout", "180", "worker")
        wait_until("initial snapshot intake", lambda: int(sql(
            "metadata-db", "updatis_runtime", "updatis_metadata", "SELECT count(*) FROM events"
        )) >= 2)
        temporal = sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT concat_ws('|', envelope->'after'->>'amount', envelope->'after'->>'binary_value',
                envelope->'after'->>'local_time', envelope->'after'->>'zoned_time',
                envelope->'after'->>'local_stamp', envelope->'after'->>'instant')
            FROM events WHERE operation='read' AND source_position->>'table'='orders'
        """)
        assert temporal == "12.34|AP8=|23:59:59.123456|05:30:00Z|1969-12-31T23:59:59.123456|2024-11-03T05:30:00Z", temporal
        sql("source-db", "source_bootstrap", "example_source", """
            INSERT INTO example.orders(customer_name,status) VALUES ('stream','pending');
            UPDATE example.orders SET status='shipped' WHERE customer_name='stream';
            DELETE FROM example.orders WHERE customer_name='stream';
            UPDATE example.customers SET display_name='changed'
              WHERE id='10000000-0000-0000-0000-000000000001';
        """)
        wait_until("streaming changes and tombstone", lambda: int(sql(
            "metadata-db", "updatis_runtime", "updatis_metadata",
            "SELECT count(*) FROM ingest_records"
        )) >= 7)
        operations = sql("metadata-db", "updatis_runtime", "updatis_metadata",
                         "SELECT operation || ':' || count(*) FROM events GROUP BY operation ORDER BY operation")
        for operation in ("read", "create", "update", "delete"):
            assert f"{operation}:" in operations
        assert int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                       "SELECT count(*) FROM ingest_records WHERE classification='control'")) >= 1
        assert sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT count(*) FROM deliveries d
            UNION ALL SELECT count(*) FROM delivery_attempts
            UNION ALL SELECT count(*) FROM ordering_gaps
        """).splitlines() == ["0", "0", "0"]
        assert int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                       "SELECT count(*) FROM ingest_checkpoints")) == 2
        assert sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT bool_and(initial_offset <= next_offset) FROM ingest_checkpoints
        """) == "t"
        assert sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT count(*) FROM source_schema_manifests
            WHERE jsonb_array_length(columns) > 0 AND schema_fingerprint ~ '^[0-9a-f]{64}$'
        """) == "2"
        assert sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT count(*) FROM stream_epochs WHERE stream_epoch='00000000-0000-0000-0000-000000000101'
        """) == "1"
        assert sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
                   "SELECT count(*) FROM pg_replication_slots") == "0"
        assert sql("metadata-db", "updatis_bootstrap", "updatis_metadata",
                   "SELECT count(*) FROM pg_publication WHERE pubname NOT LIKE 'pg_%'") == "0"
        slot = sql("source-db", "source_bootstrap", "example_source", """
            SELECT plugin || ':' || database FROM pg_replication_slots
            WHERE slot_name='updatis_slot_000000000000000000000000_000000000101'
        """)
        assert slot == "pgoutput:example_source"
        before = sql("source-db", "source_bootstrap", "example_source", """
            SELECT confirmed_flush_lsn::text FROM pg_replication_slots
            WHERE slot_name='updatis_slot_000000000000000000000000_000000000101'
        """)
        compose("restart", "connect")
        wait_until("connector restart", lambda: connector_status().get("tasks", [{}])[0].get("state") == "RUNNING")
        after = sql("source-db", "source_bootstrap", "example_source", """
            SELECT confirmed_flush_lsn::text FROM pg_replication_slots
            WHERE slot_name='updatis_slot_000000000000000000000000_000000000101'
        """)
        assert before and after
        assert int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                       "SELECT count(*) FROM stream_epochs")) == 1
        # Exercise both independent size ceilings with pinned-runtime records.
        sql("source-db", "source_bootstrap", "example_source", """
            INSERT INTO example.orders(customer_name,status,payload)
            VALUES ('normalized-oversized','pending',jsonb_build_object('blob',repeat('x',600000)));
            INSERT INTO example.orders(customer_name,status,payload)
            VALUES ('raw-oversized','pending',jsonb_build_object('blob',repeat('y',1100000)));
        """)
        wait_until("independent oversized classifications", lambda: sql(
            "metadata-db", "updatis_runtime", "updatis_metadata", """
                SELECT count(*) FROM intake_discontinuities
                WHERE reason_code IN ('normalized_envelope_too_large','raw_record_too_large')
            """) == "2")
        assert sql("metadata-db", "updatis_runtime", "updatis_metadata", """
            SELECT r.raw_value_length > octet_length(r.raw_value_prefix)
                   AND octet_length(r.raw_value_prefix)=262144 AND r.raw_value_truncated
            FROM ingest_records r JOIN intake_discontinuities d ON d.ingest_record_id=r.id
            WHERE d.reason_code='raw_record_too_large'
        """) == "t"
        # A metadata outage must stop intake without acknowledging the new
        # source record; restart resumes from the durable metadata checkpoint.
        event_count_before_outage = int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                                            "SELECT count(*) FROM events"))
        compose("stop", "metadata-db")
        sql("source-db", "source_bootstrap", "example_source",
            "INSERT INTO example.orders(customer_name,status) VALUES ('metadata-outage','pending')")
        wait_until("worker exit on metadata outage", lambda: "exited" in compose(
            "ps", "-a", "--format", "json", "worker", capture=True
        ).stdout.lower(), timeout=30)
        compose("start", "metadata-db")
        compose("up", "-d", "--wait", "--wait-timeout", "180", "worker")
        wait_until("outage record durable intake", lambda: int(sql(
            "metadata-db", "updatis_runtime", "updatis_metadata", "SELECT count(*) FROM events"
        )) == event_count_before_outage + 1)

        # Model the exact crash window by moving only Kafka's committed offset
        # one record behind durable metadata while the worker is stopped.
        compose("stop", "worker")
        durable = int(sql("metadata-db", "updatis_runtime", "updatis_metadata", f"""
            SELECT next_offset FROM ingest_checkpoints WHERE topic='{ORDERS_TOPIC}' AND partition=0
        """))
        assert durable > 0
        events_before_duplicate = int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                                          "SELECT count(*) FROM events"))
        compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", "kafka:29092", "--group", GROUP, "--topic", f"{ORDERS_TOPIC}:0",
                "--reset-offsets", "--to-offset", str(durable - 1), "--execute")
        compose("up", "-d", "--wait", "--wait-timeout", "180", "worker")
        wait_until("duplicate recovery commits durable metadata", lambda: f" {durable} " in compose(
            "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:29092", "--group", GROUP, "--describe", capture=True
        ).stdout)
        assert int(sql("metadata-db", "updatis_runtime", "updatis_metadata",
                       "SELECT count(*) FROM events")) == events_before_duplicate

        # Repository validation runs against the migrated Docker database and
        # proves every invalid range is rejected without touching live topics.
        probe = r'''from pathlib import Path
from uuid import UUID
from sqlalchemy import text
from updatis.config.loader import load_runtime_config
from updatis.config.secrets import SecretResolver
from updatis.db.connection import create_metadata_engine
from updatis.intake.repository import CheckpointError, IntakeRepository
r=load_runtime_config('/etc/updatis/runtime.json'); e=create_metadata_engine(r.metadata,SecretResolver(r.secrets_directory)); x=IntakeRepository(e)
p=UUID('00000000-0000-0000-0000-000000000001'); s=UUID('00000000-0000-0000-0000-000000000101')
def initialize(topic): x.initialize_checkpoint(pipeline_id=p,stream_epoch=s,topic=topic,partition=0,consumer_group='acceptance-'+topic,earliest=0,log_end=20,kafka_committed=None)
def rejected(call):
 try: call()
 except CheckpointError: return
 raise AssertionError('invalid checkpoint was accepted')
initialize('acceptance-low'); initialize('acceptance-high'); initialize('acceptance-ahead')
with e.begin() as c:
 c.execute(text("UPDATE ingest_checkpoints SET next_offset=3 WHERE topic='acceptance-low'"))
 c.execute(text("UPDATE ingest_checkpoints SET next_offset=11 WHERE topic='acceptance-high'"))
rejected(lambda:x.initialize_checkpoint(pipeline_id=p,stream_epoch=s,topic='acceptance-low',partition=0,consumer_group='acceptance-acceptance-low',earliest=4,log_end=20,kafka_committed=None))
rejected(lambda:x.initialize_checkpoint(pipeline_id=p,stream_epoch=s,topic='acceptance-high',partition=0,consumer_group='acceptance-acceptance-high',earliest=0,log_end=10,kafka_committed=None))
rejected(lambda:x.initialize_checkpoint(pipeline_id=p,stream_epoch=s,topic='acceptance-ahead',partition=0,consumer_group='acceptance-acceptance-ahead',earliest=0,log_end=20,kafka_committed=1))
print('invalid checkpoint cases rejected')'''
        compose("run", "--rm", "--no-deps", "--entrypoint", "python", "worker", "-c", probe)

        # Advance Kafka retention beyond durable metadata and prove startup
        # refuses to reset. This intentionally damages only the disposable run.
        compose("stop", "worker")
        retention_checkpoint = int(sql("metadata-db", "updatis_runtime", "updatis_metadata", f"""
            SELECT next_offset FROM ingest_checkpoints WHERE topic='{ORDERS_TOPIC}' AND partition=0
        """))
        sql("source-db", "source_bootstrap", "example_source",
            "INSERT INTO example.orders(customer_name,status) VALUES ('retention-loss','pending')")
        wait_until("record beyond stopped checkpoint", lambda: int(compose(
            "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
            "kafka:29092", "--topic", ORDERS_TOPIC, capture=True
        ).stdout.strip().rsplit(":", 1)[1]) > retention_checkpoint)
        deletion = json.dumps({"partitions": [{"topic": ORDERS_TOPIC, "partition": 0,
                                                "offset": retention_checkpoint + 1}], "version": 1})
        compose("exec", "-T", "kafka", "sh", "-c",
                f"printf '%s' '{deletion}' >/tmp/delete.json && /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server kafka:29092 --offset-json-file /tmp/delete.json")
        failed_worker = compose("up", "-d", "--wait", "--wait-timeout", "45", "worker",
                                capture=True, check=False)
        assert failed_worker.returncode != 0
        assert "below Kafka's earliest retained offset" in compose(
            "logs", "--no-color", "worker", capture=True, check=False
        ).stdout

        # A previously registered epoch with a missing slot is rejected. The
        # capture role is never granted ownership needed to recreate it.
        compose("exec", "-T", "connect", "curl", "-fsS", "-X", "PUT",
                f"http://localhost:8083/connectors/{CONNECTOR}/stop")
        wait_until("replication slot inactive", lambda: sql(
            "source-db", "source_bootstrap", "example_source",
            "SELECT NOT active FROM pg_replication_slots WHERE slot_name='updatis_slot_000000000000000000000000_000000000101'"
        ) == "t")
        sql("source-db", "source_bootstrap", "example_source",
            "SELECT pg_drop_replication_slot('updatis_slot_000000000000000000000000_000000000101')")
        lost_wal = compose("--profile", "capture", "run", "--rm", "provision-capture",
                           capture=True, check=False)
        assert lost_wal.returncode != 0
        assert "missing or unusable PostgreSQL replication slot" in lost_wal.stdout + lost_wal.stderr
        print("v0.1-b capture, normalization, and durable intake verification passed")
    except BaseException:
        diagnostics()
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", check=False)
        for name in SECRET_FILES:
            (SECRETS / name).unlink(missing_ok=True)
        try:
            SECRETS.rmdir()
        except OSError:
            pass
        try:
            SECRETS_ROOT.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    main()
