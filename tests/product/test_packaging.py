from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import sys
import zipfile


ROOT = Path(__file__).resolve().parents[2]


def test_wheel_contains_programmatic_alembic_environment(tmp_path: Path) -> None:
    output = tmp_path / "dist"
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(output)],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    wheel = next(output.glob("updatis-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "updatis/db/migrations/env.py" in names
    assert "updatis/db/migrations/versions/0001_metadata.py" in names
    assert "updatis/resources/foundation-defaults-v1.json" in names

    installed = tmp_path / "installed"
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(installed), str(wheel)],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    runtime = tmp_path / "runtime.json"
    runtime.write_text(json.dumps({
        "schema_version": 1,
        "environment": "test",
        "metadata": {
            "host": "metadata.invalid", "database": "metadata", "username": "migration",
            "password": {"provider": "env", "name": "MIGRATION_PASSWORD"},
            "instance_id": "metadata-test",
        },
    }), encoding="utf-8")
    environment = {**os.environ, "PYTHONPATH": str(installed), "MIGRATION_PASSWORD": "canary"}
    command = [sys.executable, "-c",
        "from updatis.db.migrate import build_alembic_config; "
        "c=build_alembic_config(r'" + str(runtime) + "'); "
        "assert c.get_main_option('script_location').replace('\\\\', '/').endswith('updatis/db/migrations')"]
    subprocess.run(command, cwd=tmp_path, env=environment, check=True, capture_output=True, text=True)
