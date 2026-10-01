from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy/compose.yml"
PROBE = ROOT / "tests/integration/v01c_probe.py"
PROJECT = f"updatis-v01c-{os.getpid()}-{uuid.uuid4().hex[:8]}"
FRESH_PROJECT = PROJECT + "-fresh"
SECRETS = ROOT / ".test-secrets" / PROJECT
VALUES = {
    "metadata_bootstrap_password": "V01cBootstrapPass123",
    "metadata_migration_password": "V01cMigrationPass123",
    "metadata_runtime_password": "V01cRuntimePass123",
    "source_bootstrap_password": "V01cSourceBootPass123",
    "source_runtime_password": "V01cSourceRunPass123",
    "source_capture_password": "V01cSourceCapturePass123",
}


def compose(*args: str, capture: bool = False, check: bool = True,
            project: str = PROJECT) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose", "-p", project, "-f", str(COMPOSE), *args]
    environment = dict(os.environ, UPDATIS_SECRETS_DIR=str(SECRETS.resolve()))
    result = subprocess.run(command, cwd=ROOT, env=environment, text=True,
                            capture_output=capture, check=False, timeout=300)
    if check and result.returncode:
        if capture:
            sys.stderr.write(result.stdout + result.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def diagnostics() -> None:
    for args in (("ps", "-a"), ("logs", "--tail", "100", "--no-color", "metadata-db")):
        result = compose(*args, capture=True, check=False)
        sys.stderr.write(f"\n===== {' '.join(args)} =====\n{result.stdout}{result.stderr}")


def run_probe(check: bool = True, outage: bool = False) -> subprocess.CompletedProcess[str]:
    mount = f"{PROBE.resolve()}:/tmp/v01c_probe.py:ro"
    return compose("run", "--rm", "--no-deps", "--volume", mount,
                   "--entrypoint", "python", "worker", "/tmp/v01c_probe.py",
                   *(["--outage"] if outage else []),
                   capture=True, check=check)


def schema_dump(project: str) -> str:
    result = compose("exec", "-T", "metadata-db", "pg_dump", "-U", "updatis_bootstrap",
                     "-d", "updatis_metadata", "--schema-only", "--no-owner",
                     "--no-privileges", project=project, capture=True)
    return "\n".join(line for line in result.stdout.splitlines()
                     if not line.startswith(("\\restrict ", "\\unrestrict ")))


def save_explain(output: str, path: Path) -> None:
    backlog = next(line.split("EXPLAIN: ", 1)[1] for line in output.splitlines()
                   if line.startswith("large-backlog EXPLAIN: "))
    locked = next(line.split("EXPLAIN: ", 1)[1] for line in output.splitlines()
                  if line.startswith("locked-front EXPLAIN: "))
    evidence = {
        "observed_on": datetime.now(timezone.utc).date().isoformat(),
        "database": "PostgreSQL 18.6",
        "fixture_unresolved_rows_in_backlog_lane": 20001,
        "additional_registry_lanes": 2000,
        "candidate_limit": 2, "acquisition_limit": 1,
        "query_source": "v01c_probe.py imports the production _LANE_CANDIDATES, _LOCKED_HEAD, and _ACTIVE_LANE_LEASE constants",
        "plans": json.loads(backlog), "locked_front_lane_plan": json.loads(locked),
        "note": "Fixture UUIDs in plan predicates are redacted; plan metrics are observed values.",
    }
    serialized = json.dumps(evidence, indent=2)
    serialized = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                        "<fixture-lane-id>", serialized)
    path.write_text(serialized + "\n", encoding="utf-8")


def main() -> None:
    SECRETS.mkdir(parents=True, exist_ok=True)
    for name, value in VALUES.items():
        (SECRETS / name).write_text(value, encoding="utf-8")
    try:
        compose("up", "-d", "--wait", "--wait-timeout", "120", "metadata-db")
        compose("build", "migrate", "worker")
        upgrade_mount = f"{(ROOT / 'tests/integration/v01c_upgrade_probe.py').resolve()}:/tmp/v01c_upgrade_probe.py:ro"
        upgraded = compose("--profile", "tools", "run", "--rm", "--no-deps", "--volume", upgrade_mount,
                           "--entrypoint", "python", "migrate", "/tmp/v01c_upgrade_probe.py", capture=True)
        sys.stdout.write(upgraded.stdout)
        compose("--profile", "tools", "run", "--rm", "migrate")
        upgraded_schema = schema_dump(PROJECT)
        compose("up", "-d", "--wait", "--wait-timeout", "120", "metadata-db", project=FRESH_PROJECT)
        compose("build", "migrate", project=FRESH_PROJECT)
        compose("--profile", "tools", "run", "--rm", "migrate", project=FRESH_PROJECT)
        assert schema_dump(FRESH_PROJECT) == upgraded_schema, "fresh and upgraded schemas differ"
        print("fresh install and 0002/0004 upgrade converge on the same schema")
        result = run_probe()
        sys.stdout.write(result.stdout)
        if os.environ.get("UPDATIS_EXPLAIN_OUTPUT"):
            save_explain(result.stdout, Path(os.environ["UPDATIS_EXPLAIN_OUTPUT"]))
        # Expected storage failure must propagate; it cannot produce assumed ownership.
        compose("stop", "metadata-db")
        outage = run_probe(check=False, outage=True)
        assert outage.returncode != 0, "metadata outage unexpectedly granted ownership"
        assert "OperationalError" in outage.stderr or "connection" in outage.stderr.lower()
        print("metadata outage failed closed")
    except Exception:
        diagnostics()
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", capture=True, check=False)
        compose("down", "--volumes", "--remove-orphans", capture=True, check=False, project=FRESH_PROJECT)
        shutil.rmtree(SECRETS, ignore_errors=True)


if __name__ == "__main__":
    main()
