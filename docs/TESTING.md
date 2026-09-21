# Reproducible Foundation checks

The Foundation suite contains contract checks and executable evidence of known
legacy defects. It does not prove a distributed delivery guarantee.

## Clean Python environment

Install CPython 3.13.15 from Python.org and identify its exact interpreter path.
Create an environment outside or inside the checkout, then install with:

```powershell
$python313 = "C:\path\to\Python313\python.exe"
& $python313 -c "import sys; assert sys.version_info[:3] == (3, 13, 15)"
& $python313 -m venv .venv
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements-dev.lock
.venv\Scripts\python.exe -m pytest
```

```bash
python3.13 -c "import sys; assert sys.version_info[:3] == (3, 13, 15)"
python3.13 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-dev.lock
.venv/bin/python -m pytest
```

To update a dependency later, compile locks against the exact validated runtime,
run every gate again, and review the diff:

```powershell
uv pip compile --python .venv\Scripts\python.exe --generate-hashes --no-header requirements.txt -o requirements.lock
uv pip compile --python .venv\Scripts\python.exe --generate-hashes --no-header requirements-dev.txt -o requirements-dev.lock
```

## Test groups

- `python -m pytest tests/contracts` validates schemas, positive/negative
  fixtures, identity determinism, and provisional-default boundaries.
- `python -m pytest tests/legacy` runs four strict expected failures describing
  current legacy defects. An unexpected pass fails the suite and requires a
  deliberate test/status update.
- `python tests/compatibility/verify.py` starts exact candidate containers in a
  uniquely named Compose project, verifies PostgreSQL/Kafka/Connect and the
  PostgreSQL Debezium plugin, then removes containers, network, and volumes.

The compatibility runner requires a working Docker daemon and network access
to pull images. Failure to run it leaves the matrix candidate-only; absence of
Docker is not a passing result.

## Isolation and safety

Contract and legacy tests use synthetic in-memory fakes and no user databases.
The compatibility Compose file uses no host ports, fixed container names,
bind mounts, or external volumes. The runner supplies a random project name and
executes `docker compose down --volumes --remove-orphans` in `finally`.

CI performs the same hash-checked install, pytest suite, and compatibility
runner. Any command failure fails the workflow. Container success demonstrates
only baseline version/plugin interoperability, not CDC capture or delivery.
