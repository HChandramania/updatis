# Foundation gate

Status: F01–F05 implementation record. This gate defines evidence, candidate
compatibility, contracts, provenance, and reproducible tests. It does not
implement a production pipeline.

## Repository boundaries

The following files are the current **legacy MySQL order-notification example**:

- `app/`, `main.py`, `consumer.py`, and `client.py`
- `docker-compose.yml`, `debezium-connector.json`, and `db_update.sql`
- the MySQL setup, order API, WebSocket notification, and old operations docs

They remain available as historical implementation material. Their behavior is
not the Updatis product contract, and their MySQL connector does not establish
supported MySQL compatibility. Four known failures are captured under
`tests/legacy/` as strict expected failures. A future fix must deliberately
remove the expected-failure marker and satisfy the stated invariant.

Future Updatis behavior is described by the approved planning documents and
the versioned Foundation contract under `contracts/`. No module currently
implements that contract. In particular, this gate does not add PostgreSQL
capture, webhook delivery, a metadata store, pipeline APIs, a CLI, retries,
DLQ processing, ordering gaps, connector management, or a dashboard.

## F01–F05 map

| Foundation item | Files and evidence |
| --- | --- |
| F01 | `tests/legacy/`, this boundary statement |
| F02 | `docs/SUPPORT_MATRIX.md`, `compatibility/candidate-matrix.json`, `tests/compatibility/` and dependency locks after validation |
| F03 | `docs/INITIAL_PRODUCT_CONTRACT.md`, schemas and fixtures under `contracts/` and `tests/fixtures/contracts/` |
| F04 | `docs/REPOSITORY_PROVENANCE.md`; frontend-independent tests and contracts |
| F05 | `docs/TESTING.md`, `pyproject.toml`, dependency files, and `.github/workflows/ci.yml` |

The numerical defaults in the contract are provisional design inputs and
future validation targets. Passing schema tests says only that the declared
boundaries are internally consistent; it does not prove throughput, latency,
retention, recovery, or distributed-system guarantees.
