# Risk register

Status: planning assessment dated 2026-09-21. “Observed” means supported by the repository audit or isolated checks; “design risk” means a failure to address in the proposed architecture. Likelihood is qualitative exposure, not a measured probability. Owners are proposed responsibility areas, not assigned people.

| ID | Risk / evidence | Severity / likelihood | Mitigation and validation | Owner / gate |
| --- | --- | --- | --- | --- |
| R01 | Observed: consumer advances after persistence failure and can commit past a lost record | Critical / high under outage | Contiguous per-partition staging/commit; crash and outage tests at each checkpoint; 01F | Worker / v0.1 blocker |
| R02 | Observed: attempts are not persisted; default retry exhaustion cannot occur | High / high | Durable pre-send attempt intent, retry-age/attempt budgets and atomic DLQ/gap transition; 01I | Delivery / v0.1 blocker |
| R03 | Observed: zero-recipient socket broadcast marks delivered; API replicas have separate client lists | High / high | Keep sockets/order app legacy; supported webhook acknowledgement contract and separate delivery worker; 01H | Product + delivery / v0.1 |
| R04 | Observed: snapshot `r` rejected; parser/order model too narrow; invalid JSON takes reconnect path | High / high | Generic validated envelope, snapshot/type fixtures, deterministic poison handling; 01E/01F | Capture / v0.1 blocker |
| R05 | Observed: Kafka client cannot import in local Python; installed dependencies drift; Compose unverified | High / high | Tested version matrix, reproducible dependencies/images and clean-stack CI; F02/01O | Build / foundation and v0.1 |
| R06 | Observed: missing frontend submodule source and broken quickstart undermine adoption | High / high | Treat UI unavailable; no frontend dependency for first CLI flow; clean-user trials; 02D/10D | DX / v0.1 docs, v0.2 UI |
| R07 | Design risk: metadata accidentally co-located with/captured as a source, causing coupling or feedback | Critical / medium | Separate services/instances, identities, credentials, volumes and lifecycles; validate known aliases and prohibit metadata capture; isolation tests; 01A/01C | Architecture / v0.1 blocker |
| R08 | Design risk: remote side effect succeeds but acknowledgement is lost; duplicate or late effects | High / inherent | Stable ID, destination transactional dedupe, timeout classification, per-key stale-version guidance; 01H/03F | Delivery + destination owner / every release |
| R09 | Design risk: gap advancement is non-atomic, silently skipping an exhausted record | Critical / medium | Transactionally commit DLQ, gap, terminal state and cursor; inject write/commit failures; 01I | Storage / v0.1 blocker |
| R10 | Design risk: stale lease owner sends/completes after another claims partition | High / medium | Fencing for local transitions, bounded requests, one intentional in-flight request, ambiguous-outcome handling; stale-owner tests; 01G/03E | Worker / v0.1 and v0.3 |
| R11 | Design risk: redrive overwrites newer destination state or is mistaken for original-order replay | High / high without guidance | Permanent gap history, original IDs, preview warning, separate bounded jobs, destination version checks; 02E–02G/03F | Recovery + UX / v0.2 blocker |
| R12 | Design risk: WAL/Kafka/inbox storage exhaustion during destination/capture outages | Critical / medium | Capacity limits, retention headroom, WAL/free-space alerts, pause policy and explicit history-loss recovery; 01J/03A/03E | Operations / v0.1 basics, v0.3 proof |
| R13 | Design risk: old metadata backup restored alongside newer Kafka commits causes silent skips | Critical / medium | Compare independent checkpoints; retain/replay required history; coherent backup/restore drills and measured RPO/RTO; 05E | Operations + storage / v0.5 blocker |
| R14 | Observed/design risk: trusted forwarding headers, broad DB credentials, exposed infrastructure and webhook SSRF | Critical / medium | Trusted-proxy boundary, scoped accounts/private listeners, TLS, DNS/redirect-aware egress restrictions, auth negative tests; 01M/05A | Security / v0.1 blocker |
| R15 | Design risk: row payloads/secrets leak through logs, DLQ inspection, exports or traces | High / medium | Default metadata-only views, payload authorization, redaction, retention, bounded telemetry fields and secret tests; 01L/01M/02C/05B | Security / all releases |
| R16 | Observed: process-local metrics presented as cross-process pipeline health; readiness incomplete | High / high | Worker exporters and explicit observation freshness; separate process health/capture/intake/delivery/gap states; 01N/02B | Observability / v0.1–v0.2 |
| R17 | Design risk: schema/key changes, topic recreation or repartition invalidate identity/order assumptions | High / medium | Explicit type/key subset, stream epochs, compatibility gate, no transparent repartition; F03/03D | Capture + contracts / v0.1–v0.3 |
| R18 | Observed: no tests/CI/migrations/release discipline; future changes regress reliability | High / high | Executable invariants and real-stack CI, migration gates and release provenance; F05/01O/05D/10E | Maintainers / each gate |
| R19 | Design risk: durable inbox doubles event storage/IO and limits throughput | Medium / medium | Benchmark storage growth and bottlenecks; indexed queries, bounded retention/pressure; no premature extra broker; 03G/10C | Performance / v0.3 and v1.0 |
| R20 | Product risk: Kafka/Connect stack too costly for initial users; wrapper offers insufficient value | High / medium | Test setup/diagnosis/recovery journey with new developers; publish resource needs; prioritize narrow webhook workflow; 01O/10D | Product / before promotion |
| R21 | Adoption/legal uncertainty: no license found, frontend provenance unknown, brand/docs inconsistent | High / unresolved | Confirm rights/obligations, then license/notice decision and branding; F04 and readiness checklist | Maintainers / before public release claims |
| R22 | Scope risk: supporting MySQL, broad connectors, HA or Kubernetes before correctness | High / medium | Legacy-only MySQL label, explicit exclusions and release gates; Kubernetes conditional on Compose recovery proof | Product + architecture / all milestones |
| R23 | Design risk: connector restart/rebootstrap mishandles replication identity or missing WAL | High / medium | Preserve slot/publication/Connect offsets; explicit new epoch on rebootstrap; missing-history detection and drills; 01D/03A | Capture / v0.1–v0.3 |
| R24 | Design risk: PostgreSQL destination creates capture feedback loops or stale updates | High / medium | Reject metadata target, detect source/target overlap, explicit mapping, transactional dedupe/version ledger; 04B–04D | Destinations / v0.4 blocker |
| R25 | Maintainer risk: support promises exceed available review/release capacity | Medium / unresolved | Named owners, achievable support/EOL policy, security response process and smaller support matrix | Governance / v1.0 gate |

## Risk treatment and release policy

- Do not carry observed legacy correctness defects into supported product code. Retaining useful structure is not approval to preserve broken semantics.
- At-least-once duplicates and the inability to restore original order by redrive are accepted design limits, not defects to hide. Document them prominently and test the mitigation examples.
- Critical integrity/security risks require evidence of mitigation before their stated gate. If evidence is unavailable, narrow the release scope or delay the claim; do not mark the risk closed because code exists.
- Record an issue owner, linked tests/drills, residual risk and review date for each risk during implementation. Reassess on changes to dependency versions, ordering policy, storage topology, destination interface or authentication.
- Performance, backup RPO/RTO, and maximum safe outage duration remain unmeasured. Set supported targets using actual drills and data before publishing guarantees.

## Unresolved questions and decision points

| Question | Why it matters | Decision point |
| --- | --- | --- |
| Exact runtime/client/image versions? | Current environment cannot run the Kafka client; compatibility is not established | F02 |
| Supported keys/types/snapshot modes and schema changes? | Determines usable source scope and lossless serialization | F03, refined 03D |
| Retry attempt/age budgets, payload limits, queue watermarks and retention periods? | Defines exhaustion, capacity and recoverability; should be measured rather than invented | F03, validated 01O/03G |
| Initial workload, latency/throughput goals and hardware floor? | Determines whether stack cost is acceptable and benchmarks meaningful | F03/03G/10C |
| Is recoverable frontend source available with compatible rights? | Could reduce UI work, but is not required for the approved fallback | F04 |
| Who owns the code and can authorize Apache 2.0? | Public release must not imply a license that has not been granted | F04 |
| Required backup/restore objectives and supported external-source topologies? | Drives operational support envelope | 05E/05F |
| Is Kubernetes actually needed by early users? | Adds maintenance and failure modes without proving pipeline correctness | After 05F |

No unresolved question authorizes modifying runtime configuration or implementing a feature during this planning-only task.
