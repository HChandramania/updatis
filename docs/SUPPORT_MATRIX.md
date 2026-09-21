# Validated Foundation support matrix

Status: validated as a single Foundation development/test combination on
2026-09-21. This is not a production deployment, performance, CDC correctness,
or long-term compatibility guarantee.

## Candidate combination

| Component | Candidate | Upstream basis |
| --- | --- | --- |
| CPython | 3.13.15 | Bug-fix-supported Python 3.13 series |
| PostgreSQL | 18.6 | Current supported PostgreSQL 18 minor |
| Apache Kafka broker | 4.3.1 | Supported Kafka 4.3 patch release |
| Kafka Connect runtime | 4.3.0 | Runtime against which Debezium 3.6.3 was built/tested |
| Debezium Connect | 3.6.3.Final | Stable Debezium 3.6 patch; tested with PostgreSQL 18 and Kafka/Connect 4.3 |
| PostgreSQL logical decoding | `pgoutput` | Built-in plugin in Debezium's tested matrix |
| Python Kafka client | kafka-python 3.0.11 | Stable client candidate replacing legacy 2.0.2 |

The exact machine-readable pins live in `compatibility/candidate-matrix.json`.
Python application and test dependencies are direct pins in
`requirements*.txt`; `requirements*.lock` contains the successful candidate's
fully resolved, hash-checked transitive set.

## Promotion rule

The candidate matrix becomes the initial validated Foundation pin only when all
of these checks pass for the same combination:

1. CPython reports the candidate major/minor and runs a clean isolated install
   from the generated hash-checked development lock.
2. The application package and Kafka client import successfully.
3. The complete pytest suite passes, with only the four documented strict
   expected failures.
4. The compatibility Compose file starts the exact PostgreSQL, Kafka, and
   Debezium images in an isolated project.
5. PostgreSQL accepts `SELECT version()`, Kafka answers a broker API request,
   Connect becomes healthy, and its plugin list includes
   `io.debezium.connector.postgresql.PostgresConnector`.
6. The compatibility runner always removes its named containers, network, and
   volumes.

If any candidate is incompatible, preserve the failure output. Propose the
newest mutually compatible stable alternative and obtain approval before
changing this table or adopting alternative pins.

## Validation record

| Gate | Result |
| --- | --- |
| Clean Python 3.13.15 install and imports | Passed: official CPython runtime; `app` and kafka-python 3.0.11 imported |
| Hash-checked install | Passed from `requirements-dev.lock`; `pip check` reported no broken requirements |
| Unit/contract/legacy regression suite | Passed at promotion and rechecked after contract corrections: 34 tests, 4 strict expected failures |
| PostgreSQL/Kafka/Connect interoperability | Passed twice after harness correction; final run asserted PostgreSQL 18.6, Kafka 4.3.1, Connect 4.3.0, Debezium image 3.6.3.Final and PostgreSQL connector plugin |
| Isolated cleanup | Passed: named containers, network, and volumes removed in `finally` |
| Candidate promoted to validated pin | Yes, for the bounded Foundation test scope |

### Tooling note

On 2026-09-21, `uv python install 3.13.15` returned `No download found`
for its managed `cpython-3.13.15-windows-x86_64-none` build. Python.org does
publish an official Windows 64-bit 3.13.15 installer, so this is recorded as a
managed-runtime catalog limitation rather than a compatibility failure. The
candidate was validated using Python.org's official Windows embeddable runtime
and PyPA's pip bootstrap. The official silent installer was also attempted but
stalled without creating the target runtime and was stopped; this did not alter
the selected version. No alternative candidate was adopted.

The initial runner correctly proved service health and plugin availability but
misread `docker compose images --format json` as a raw tag string. It failed
after the interoperability checks and cleaned up. The harness was corrected to
inspect exact image tags with `docker image inspect`; the entire combination
then passed, followed by a final pass that also asserted Connect's reported
runtime version.

Upstream selection sources:

- Python 3.13.15 release: <https://www.python.org/downloads/release/python-31315/>
- PostgreSQL supported versions: <https://www.postgresql.org/support/versioning/>
- Kafka supported downloads: <https://kafka.apache.org/community/downloads/>
- Debezium 3.6 matrix and releases: <https://debezium.io/releases/3.6/>

No compatibility is inferred from the old virtual environment, the legacy
Compose file, image availability, or mocked tests.
