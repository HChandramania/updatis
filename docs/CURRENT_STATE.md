# Current state of Updatis

Audit date: 2026-09-21. Baseline: Git commit `01f4723` (`secure + clean`), remote `https://github.com/Spamziesagcan/updatis`.

## Scope and evidence rules

The audit read the available application source, entry points, SQL, dependency pins, Compose and connector configuration, environment example, tracked documentation, package initializers, and available frontend directory. It inspected tracked-file inventory, Git status, submodule metadata, tags, and historical log excerpts. No production code, configuration, or dependency was modified. The existing untracked `backend_project_description.md` was read as a claim, not as committed implementation evidence. Existing Python caches were preserved.

The frontend source is unavailable in this checkout. No automated test suite, CI configuration, application Dockerfile, or migration directory was found. Dependencies inside the local virtual environment are runtime evidence, not repository source.

Classifications used throughout:

| Classification | Meaning |
| --- | --- |
| Implemented and working | Executed successfully in the stated bounded check; not broader production certification |
| Partially implemented | Meaningful code exists but lacks required behavior or verification |
| Present but unverified | Implementation/configuration exists; relevant integrated behavior was not executed |
| Scaffolding or placeholder | Stub, unused path, or interface without the promised behavior |
| Documented but not implemented | Documentation claims a capability without a corresponding implementation |
| Missing | No implementation or applicable configuration found in audited files |
| Broken | A defect was reproduced or follows directly from the inspected code/configuration |

## Directory and component inventory

| Path | Actual responsibility |
| --- | --- |
| [`main.py`](../main.py), [`app/factory.py`](../app/factory.py) | FastAPI entry point, dependency wiring, startup DDL, dispatcher/cleanup tasks, middleware and shutdown |
| [`consumer.py`](../consumer.py), [`app/workers/kafka_consumer.py`](../app/workers/kafka_consumer.py) | Separate synchronous Kafka-to-MySQL ingestion process |
| [`client.py`](../client.py) | WebSocket example listener with heartbeat; no pipeline-management CLI |
| [`app/api/`](../app/api/) | Order CRUD, stats, internal direct broadcast, WebSocket endpoint, liveness/readiness |
| [`app/core/`](../app/core/) | Dataclass settings, dotenv, API-key authentication, rate limiting, JSON logging, custom in-memory metrics/log spans |
| [`app/db/connection.py`](../app/db/connection.py) | Async MySQL pool; creates orders, notification queue, and dead-letter tables |
| [`app/models.py`](../app/models.py) | Order models and order-specific versioned notification envelope |
| [`app/repositories/`](../app/repositories/) | Order SQL and notification persistence/claims/status transitions |
| [`app/services/`](../app/services/) | Order operations, CDC normalization/IDs, queue delivery, direct broadcast |
| [`app/websockets/manager.py`](../app/websockets/manager.py) | Process-local client registry and sequential socket sends |
| [`app/workers/cleanup.py`](../app/workers/cleanup.py), [`notification_dispatcher.py`](../app/workers/notification_dispatcher.py) | Stale connection cleanup and queue-draining loops inside the API |
| [`docker-compose.yml`](../docker-compose.yml) | ZooKeeper, single Kafka broker, Debezium Connect only |
| [`debezium-connector.json`](../debezium-connector.json) | Manually registered MySQL connector for one orders table |
| [`db_update.sql`](../db_update.sql) | Non-idempotent MySQL orders table creation and one sample insert; no database selection |
| `nextjs-order-dashboard` | Empty local directory represented by gitlink `e4ee74b6fda3825cfc6165a91ce5471f3855bed4`; no `.gitmodules` mapping |
| [`requirements.txt`](../requirements.txt), [`.env.example`](../.env.example), [`.gitignore`](../.gitignore) | Direct dependency pins, partial environment reference, minimal ignore rules |
| Root Markdown and `server.log` | Setup/security/operations documents, older implementation plans, tracked historical log |

Package initializers mainly re-export objects. `app/__init__.py` imports the app factory, coupling otherwise small module imports to the backend dependency graph.

## Actual end-to-end path

```text
Order API or external SQL write
  → external MySQL: realtime_orders.orders
  → MySQL Debezium connector
  → Kafka: dbserver.realtime_orders.orders
  → Python ingestion process
  → MySQL: notification_events / notification_dead_letters
  → API background dispatcher
  → that API process's connected WebSocket clients
```

The internal broadcast endpoint is a second path directly to sockets; it bypasses durable ingestion. Order CRUD does not itself emit the CDC notification. No arbitrary source/table configuration, pipeline model, destination registry, webhook request delivery, or PostgreSQL support exists.

This describes code intent and control flow, not a verified live installation. The MySQL application is strictly a legacy example and establishes no supported MySQL product compatibility.

## Capture, transport, and serialization

- The connector uses `io.debezium.connector.mysql.MySqlConnector`, `host.docker.internal:3306`, user `root`, server ID `1`, database `realtime_orders`, and table `orders`. The password is an environment-provider reference. Capture prerequisites are manual. See [connector configuration](../debezium-connector.json).
- Connect storage topics are `my_connect_configs`, `my_connect_offsets`, and `my_connect_statuses`; schema history uses `dbhistory.orders`. Data-topic partitions, retention, explicit converters, and Connect internal-topic replication factors are not provisioned in repository code.
- Compose uses `confluentinc/cp-kafka:7.3.0`, `cp-zookeeper:7.3.0`, and `debezium/connect:2.1`. Kafka advertises internal `kafka:29092` and host `localhost:9092`; schema history instead bootstraps at `kafka:9092`. This needs image-level validation, not an assumption that startup works.
- The worker uses group `notification_event_ingestor`, `auto_offset_reset="earliest"`, `enable_auto_commit=False`, and a configurable poll-record limit. It calls `consumer.commit()` after persistence or quarantine; commits are not explicitly scoped to a single record's partition/offset. There is no explicit rebalance recovery or configurable group per pipeline.
- Kafka bytes are decoded as JSON with a required outer `payload`. Only `c`, `u`, and `d` operations map to notifications; null Kafka values are skipped and committed. The key is ignored. Schema-less payloads, snapshot `r`, generic/composite keys, and schema-change messages are not supported by the parser.
- The normalized envelope has schema version `1`, SHA-256 event ID, `order_change`, an integer order ID, before/after dictionaries, timestamps, source coordinates, and metadata. The ID includes source coordinates and the Debezium event timestamp; it is not proven stable across connector re-emission/resnapshot. Validation accepts any positive schema version without implementing version negotiation. See [models](../app/models.py) and [event factory](../app/services/notification_event_factory.py).

## Capability assessment

| Capability | Classification | Evidence and limit |
| --- | --- | --- |
| Python syntax and backend package import | Implemented and working | All 34 project Python files parsed; `import app` succeeded in the local environment |
| Create-event normalization | Implemented and working | Isolated valid `c` fixture produced `INSERT`, schema version 1; broader type coverage untested |
| Order CRUD and MySQL persistence | Present but unverified | [Order repository](../app/repositories/order_repository.py); no live DB check |
| Debezium/Kafka capture | Present but unverified | Compose and connector definitions; no running Docker daemon |
| PostgreSQL capture, webhook delivery | Missing | No corresponding code/configuration/dependencies |
| Durable notification queue and DLQ | Partially implemented | Startup DDL, inserts, claims, quarantine; no retention, migrations, inspection/redrive API |
| Retry and ingestion correctness | Broken | Failed record is skipped before a later commit; attempts are never persisted |
| Deduplication | Partially implemented | Event-ID primary key and `INSERT IGNORE`; no proof of stable source identity or destination idempotency |
| Snapshot handling | Broken | Isolated `r` fixture raised `Unsupported Debezium operation` |
| WebSocket delivery | Partially implemented | Broadcast code exists; zero-recipient probe still marked event delivered |
| Subscriptions | Scaffolding or placeholder | `subscribe:` branch is `pass` in [WebSocket API](../app/api/websocket.py) |
| Frontend | Broken | Gitlink without mapping; directory empty; `git submodule status` fails |
| Pipeline configuration/lifecycle API and CLI | Missing | Only order-oriented routes and manual connector registration |
| Authentication/rate limits | Partially implemented | [Security service](../app/core/security.py); shared user keys, local buckets, no roles or tenant permissions |
| Secure secrets handling | Partially implemented | Environment placeholders/ignored `.env`; no secret-provider integration, rotation tooling, or comprehensive redaction |
| Structured logging | Present but unverified operationally | JSON formatter and context fields; no retention/rotation configured |
| Metrics and tracing | Partially implemented | Local metrics snapshots and log spans; no exporter or cross-process aggregation |
| Consumer lag in API stats | Documented but not implemented | README claims it; consumer and API have separate metric registries |
| Health checks | Partially implemented | API DB/task checks; no complete pipeline/source/worker/Connect readiness |
| Controlled replay/offset management | Missing | No operator interface or guarded recovery workflow |
| Backpressure and ordered retry guarantees | Missing | Batch sizes exist, but no queue capacity policy, partition blocking, or bounded socket send |
| Graceful shutdown | Partially implemented | API tasks cancel and DB closes; worker has `finally`, no complete drain/signal/rebalance contract |
| One-command reproducible deployment | Broken | Compose omits DB/app/worker/frontend; manual installation and connector setup required |
| Automated tests, CI, benchmark harness | Missing | None in tracked inventory; README only describes manual checks |
| License, governance, contribution/release files | Missing | No LICENSE/CONTRIBUTING/CODE_OF_CONDUCT/SECURITY policy/workflows/tags found; boundary map is not a disclosure policy |

## Correctness and production failure analysis

1. **Skipped records can become acknowledged.** In [consumer error handlers](../app/workers/kafka_consumer.py), reconnect/backoff does not retry or seek the failed record. The next iterator item can succeed and advance the committed offset beyond it. A simulated persistence failure at offset 0 followed by success at 1 produced a commit of next offset 2. This was an isolated execution of the actual function with fake dependencies, not a Kafka integration test.
2. **Retry exhaustion is ineffective.** [Claiming](../app/repositories/notification_event_repository.py) computes `attempts + 1` only on returned objects; none of the queue UPDATE statements persists attempts. Default max-attempts 10 cannot be reached through repeated ordinary failures.
3. **Delivery accounting overstates success.** [Delivery service](../app/services/notification_delivery_service.py) marks delivered even when broadcast returns 0. [Manager](../app/websockets/manager.py) catches per-client failures. No durable recipient cursor/acknowledgement exists. A disconnect can lose notifications from a recipient's perspective.
4. **Multi-process fanout is incorrect.** Queue rows are claimed globally, but socket clients are process-local. The winning dispatcher does not reach other instances' clients. Stale claims have no fencing token; a slow worker can finish after another reclaims the record.
5. **Malformed-input handling is inconsistent.** Unsupported operations enter the ingest DLQ; JSON decoding errors enter DB reconnect handling. Some model errors fall through the generic handler. Snapshot rows are rejected. Neither poison handling nor offsets has integration coverage.
6. **Schema/data constraints are too narrow.** Integer order IDs, order actions, MySQL coordinates, and startup DDL block a generic pipeline model. `INSERT IGNORE` can hide more than deliberate duplicate handling; errors must be distinguished in a replacement.
7. **Resources can grow or stall without bounds.** Queue/DLQ rows and file logs have no retention. Socket sends are sequential without explicit timeout. Metrics retain arbitrary unknown URL labels; rate-limit maps retain client/path buckets. Claim batches may outlive the processing timeout.
8. **Security depends on unimplemented deployment assumptions.** Arbitrary `X-Forwarded-Proto` is trusted by the application; a direct HTTP probe with that header passed the HTTPS check. Internal broadcast shares the public listener. API keys have no scopes/expiry; query-string WebSocket credentials can leak in access logs. Health responses include exception details. Root database credentials and plaintext exposed infrastructure are development defaults, not production isolation.
9. **Configuration validation is incomplete.** Backend lifespan calls `Settings.validate`; the consumer does not. Many pool, timeout, retry, and batch ranges are unchecked. Environment examples omit many knobs. Placeholder values are not rejected. No stable configuration schema exists.

## Documentation contradictions and debt

- [Quickstart](../QUICKSTART.md) uses a placeholder `db-update` clone URL, claims Compose starts MySQL, advertises Python 3.8, and opens a nonexistent dashboard on port 8000. Dataclass `slots=True` alone requires a newer Python. The legacy listener omits mandatory authentication.
- [README](../README.md) and [runbooks](../OPERATIONS_RUNBOOKS.md) claim worker metrics are visible through API stats; no transport exports them. Runbooks lack tested recovery procedures and safe replay boundaries.
- [MySQL setup](../MYSQL_SETUP.md) refers to a Docker MySQL alternative that is absent. Its SQL script invocation does not select a database; the script itself creates none. The displayed PowerShell input-redirection form is also unsuitable for native PowerShell.
- [Older readiness plan](../PRODUCTION_READINESS_PLAN.md) and [phase prompts](../PRODUCTION_PHASE_PROMPTS.md) describe old duplicate routes, monolithic code, and HTTP ingestion that no longer match the current implementation. They are historical, not the active roadmap.
- The untracked backend description claims notifications are not lost on disconnect and describes production reliability without supporting tests. Those claims are contradicted by delivery accounting.
- Unused `poll_interval`, unused repository enqueue/quarantine entry points, duplicated ingestion SQL, and an apparently unused direct `requests` dependency add maintenance ambiguity. Direct internal broadcast bypasses the queue. Preserve only where a defined legacy purpose exists.
- `server.log` is tracked; `.gitignore` ignores `venv/` but not `.venv/` or Python caches. Historical logs are not performance or correctness evidence. Git history was not exhaustively scanned for secrets or provenance.

## Checks and limitations

| Check | Result |
| --- | --- |
| Read-only Python AST parsing | 34 files passed; no bytecode written by audit commands |
| Backend package import | Passed; full app lifespan was not started because it creates tables |
| Kafka package import | Failed on local Python 3.14.2: `No module named 'kafka.vendor.six.moves'` with kafka-python 2.0.2 |
| Dependency consistency | `pip check` passed; installed Pydantic 2.13.3 differs from pinned 2.5.0, so this is not a reproducible lock validation |
| Valid create/snapshot/zero-recipient probes | Create mapping passed; snapshot rejected; zero recipients still marked delivered |
| Retry-state source check | Four queue UPDATE statements; none writes attempts |
| Consumer failure simulation | Failed offset 0 not retried; later next offset 2 committed |
| Forwarded-protocol probe | User-supplied HTTPS header accepted over an HTTP scheme |
| Compose configuration | `docker compose config --quiet` passed with unset `DB_PASSWORD` warning |
| Docker runtime | Daemon unavailable, including outside sandbox; no containers started |
| Frontend/submodule | Empty directory; missing `.gitmodules` mapping confirmed |
| Existing tests/releases | No tracked tests or CI; no Git tags listed |
| File preservation | No application/configuration changes; original untracked files preserved |

No claim is made about successful live CDC, production throughput, vulnerability-free dependencies, secret-free history, frontend functionality, or recoverability of persisted data. Those require the implementation gates in the [roadmap](IMPLEMENTATION_ROADMAP.md).
