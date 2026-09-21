# Foundation F01–F05 acceptance record

Evaluation date: 2026-09-21. Scope is limited to the Foundation gate.

| Item | Acceptance criterion | Evidence | Result |
| --- | --- | --- | --- |
| F01 | Fixtures cover skipped offsets, absent persisted attempts, zero-recipient success, and rejected snapshot `r` | `tests/legacy/test_known_defects.py`: four strict expected failures | Passed |
| F01 | Legacy example is not product behavior | `README.md` warning and `docs/FOUNDATION.md` boundary | Passed |
| F02 | Clean exact Python install/import and reproducible dependencies | CPython 3.13.15 isolated runtime; hash-checked install; `pip check`; `app` and kafka-python 3.0.11 imports; byte-stable offline lock regeneration | Passed |
| F02 | Minimal exact container interoperability | Isolated Compose runner asserted PostgreSQL 18.6, Kafka 4.3.1, Connect 4.3.0, Debezium 3.6.3.Final image, and PostgreSQL connector plugin; cleanup verified | Passed |
| F03 | Envelope, key/type subset, snapshot/CRUD/delete/tombstone policy, topology and defaults specified | `docs/INITIAL_PRODUCT_CONTRACT.md` and two JSON Schemas | Passed |
| F03 | Positive, composite-key, operation, invalid-type and boundary fixtures reviewed by tests | Contract suite includes snapshot composite key, create/update/delete, invalid operation/key/type/payload shape, identity, and defaults boundaries | Passed |
| F03 | Numerical values are not overstated | Contract labels all numbers provisional design defaults and future validation targets; no load test claim | Passed |
| F04 | Ownership/license decision and frontend status recorded | `docs/REPOSITORY_PROVENANCE.md`; licensing authority unresolved; no license or frontend assets added | Passed with explicit unresolved provenance |
| F04 | Frontend-independent fallback and no MySQL support claim | Foundation tests/CI have no frontend dependency; boundary docs call MySQL legacy-only | Passed |
| F05 | Reproducible local commands and isolated resources | `docs/TESTING.md`, hash locks, unique Compose project runner with `finally` cleanup | Passed |
| F05 | CI skeleton fails on suite/check failure | `.github/workflows/ci.yml` runs hash install, imports, `pip check`, pytest, and compatibility runner without `continue-on-error`; contract test enforces commands | Passed locally; hosted CI has not yet run |

## Executed checks

- Full suite on CPython 3.13.15 after Foundation corrections: **34 passed,
  4 strict expected failures**.
- `pip check`: **no broken requirements**.
- `app` and `kafka` imports: passed with kafka-python 3.0.11.
- Hash-checked dependency install: passed.
- Offline lock regeneration: byte-stable after volatile generator headers were removed.
- Final container interoperability run: passed and removed its containers,
  network, and volumes.
- Compose configuration: valid.
- Working-tree whitespace check and documentation/local-link validation: run
  during final verification; see the completion report for the command result.

The first container run reached healthy PostgreSQL, Kafka, and Connect but the
runner's raw image-output assertion was incorrect; cleanup passed. The assertion
was replaced with exact `docker image inspect` tag checks. The corrected run and
a final run including exact Connect-version validation passed.

## Gate conclusion

F01–F05 meet their bounded acceptance criteria. This does not demonstrate CDC
capture, webhook delivery, offset safety in a new worker, retries, DLQ/gap
behavior, performance, or production readiness. All such work remains in v0.1
or later and has not started.
