# v0.1-c / 01G acceptance evidence

Status: overall 01G acceptance remains **provisional/incomplete**, as requested by the review. The earlier regression attempt did not complete because of the Docker/Kafka health failures preserved below. Successful individual checks from the corrected implementation are appended as new evidence; they do not change the milestone acceptance status.

Final acceptance requires successful v0.1-a, v0.1-b, and v0.1-c/01G verifiers in one accepted environment. ROADMAP.md remains ACTIVE for 01G.

The acceptance gate covers typed lease contracts, the complete Python suite, packaging and configuration checks, and all three milestone Docker verifiers. The 01G verifier uses independent SQLAlchemy connections against PostgreSQL to exercise concurrent head acquisition, durable fencing, rollback, restart, expiry, discontinuity blocking, and metadata-outage failure.

Observed on 2026-09-29:

- `python -m pytest tests/product/test_v01c_leases.py tests/integration/test_v01c_schema_contract.py -q`: 5 passed.
- `python -m pytest --basetemp=.pytest-tmp`: 100 passed, 4 expected legacy-defect xfails.
- `python -m pip check`: no broken requirements found.
- `python -m compileall -q src tests`: passed.
- JSON parsing for `deploy/config/*.json` and `src/updatis/resources/*.json`: passed.
- `docker compose -f deploy/compose.yml config --quiet`: passed.
- `git diff --check`: passed.
- `python tests/integration/verify_v01c.py`: passed the real PostgreSQL lease probe and the metadata-outage fail-closed check.
- `python tests/integration/verify.py`: did not complete. Docker marked the Kafka container unhealthy after it had previously become healthy, so Compose refused to start the worker. Diagnostics showed the API running and the worker still in `created` state. Scoped teardown completed.
- `python tests/integration/verify_v01b.py`: did not complete. It exercised snapshot/streaming and metadata-outage behavior, then Docker again marked Kafka unhealthy while restarting the worker after the deliberate outage. Scoped teardown completed.

The first unqualified `python -m pytest` attempt produced seven fixture setup errors because this Windows environment denied access to pytest's default `%TEMP%/pytest-of-onetw` directory. Re-running with the workspace-local `--basetemp` completed successfully.

## Confirmed review corrections - 2026-09-30

The review patch retains migration 0003 unchanged and adds forward-only
`0004_delivery_lanes`. The acquisition contract now distinguishes successful
claims plus lane-specific failures (`PARTIAL`) from a storage exception. No
01H/01I transport, attempt/retry scheduling, terminal transition, DLQ/gap/cursor,
API/CLI/dashboard, or continuous scheduler behavior was added.

Observed checks on the corrected implementation:

- `python -m pytest tests/product/test_v01c_leases.py tests/integration/test_v01c_schema_contract.py -q --basetemp=.pytest-tmp`: **7 passed**.
- `python -m pytest --basetemp=.pytest-tmp`: **102 passed, 4 xfailed in 23.67s**; all xfails are the unchanged documented legacy defects. This includes the contract suite and wheel/migration packaging checks.
- `python -m pip check`: **No broken requirements found.**
- `python -m compileall -q src tests`: exit **0**.
- `docker compose -f deploy/compose.yml config --quiet`: exit **0**.
- `git diff --check`: exit **0**; Git emitted an existing CI-file LF/CRLF advisory.
- JSON validation command below: **10 JSON files parsed**.

```text
python -c "import json; from pathlib import Path; paths=[p for root in ('deploy/config','src/updatis/resources','contracts') for p in Path(root).rglob('*.json')]+[Path('docs/V01C_EXPLAIN.json')]; [json.loads(p.read_text()) for p in paths]; print(f'{len(paths)} JSON files parsed')"
```

The development PostgreSQL runs first exposed an unsupported Python datetime
conversion of the registry's internal `-infinity` timestamp, then a backlog
fixture that depended on older leases not expiring during setup. The query now
returns only needed lane identity fields, and the backlog fixture isolates its
two candidate lanes. These failed development runs are not acceptance passes.

### Observed execution plan and concurrency evidence

The PostgreSQL 18.6 probe used a lane containing **20,001 unresolved deliveries**,
**2,000 additional registry lanes**, candidate limit **2**, and acquisition limit
**1**. [Captured EXPLAIN JSON](V01C_EXPLAIN.json) records the actual candidate and
locked-head queries (`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`, without forcing
planner settings):

- Candidate CTE: `Limit -> Index Only Scan delivery_lanes_discovery_idx`, **2 actual rows**.
- Lock step: bounded candidate sort and lateral per-ID indexed lane lookups.
- Head CTE: `Limit -> Index Only Scan deliveries_lane_head_idx`, **1 actual row**.
- Head lock: indexed delivery identity lookup and `LockRows`.
- Neither query has a sequential scan or `WindowAgg`; the head query has no `Sort`. The lane sort covers only the two selected candidates.

This demonstrates the indexed candidate/head path on this fixture, not constant
I/O, a latency guarantee, or production scalability.

The held-lock test uses independent connections and two thread events: worker 1
holds `SELECT ... FOR UPDATE` on N in an open transaction, signals `held`, and
waits on `release`. Worker 2 acquires lane B while receiving neither N nor N+1.
The test asserts worker 1 is still running, releases/commits it, then verifies N
is acquired and N+1 stays blocked. Waits and PostgreSQL transactions are bounded.

### Final focused Docker verifier

`python -u tests/integration/verify_v01c.py`: **exit 0**, confirmed by a Python
subprocess wrapper capturing stdout/stderr directly. An earlier PowerShell
redirection reported a nonzero shell status despite successful probe output;
the direct subprocess exit-code confirmation removes that ambiguity.

The final run passed the 0002 pre-existing-row upgrade through 0003/0004,
same-owner reacquisition with an incremented generation, expired completion
rejection, unrelated-lease preservation, attempting/retryable head blocking,
overflow isolation with a healthy acquisition in the same call, PostgreSQL
lease/identity constraint rejection, synchronized locked-head overlap,
large-backlog plan assertions, rollback/restart checks, discontinuity blocking,
and a direct `acquire_heads()` call failing closed during metadata outage.

### Predecessor attempts - 2026-10-01

`python -u tests/integration/verify.py`: **exit 0**, one attempt. Output included
`v0.1-a Compose and migration verification passed`; scoped container, network,
and volume teardown completed. The 2026-09-29 failed attempt remains recorded
above and has not been reclassified as a pass.

`python -u tests/integration/verify_v01b.py`: **exit 0**, one attempt. The worker
recovered from the deliberate metadata outage; invalid-checkpoint, retention-loss,
and missing-slot checks completed. Scoped teardown completed. The earlier Kafka
failure did not recur in this attempt; its historical evidence remains above.

The final 01G verifier and both single predecessor attempts returned **0** in
this local Docker Desktop environment. Overall 01G acceptance remains
**provisional/incomplete** per the review request, and ROADMAP.md remains
**ACTIVE**. This record does not promote the milestone or declare its complete
regression gate accepted.

Post-verification inventory used `docker ps -a --format '{{.Names}}'`,
`docker network ls --format '{{.Name}}'`, and
`docker volume ls --format '{{.Name}}'`, filtering names beginning with
`updatis-v01`. Observed result: **containers: []; networks: []; volumes: []**.
No disposable v0.1 containers, networks, or volumes remained. Cached images
were not part of this runtime-resource cleanup.

### Two blocking 01G corrections - 2026-10-01

Forward-only `0005_lane_fairness` adds the per-lane active-lease index, changes
new-lane insertion from `-infinity` to PostgreSQL `clock_timestamp()`, and
converts existing `-infinity` positions once. Acquisition checks for any active
lease under the lane lock before granting a claim. Candidate SQL now applies
`FOR UPDATE SKIP LOCKED` before its effective page limit.

`python -u tests/integration/verify_v01c.py` with
`UPDATIS_EXPLAIN_OUTPUT=docs/V01C_EXPLAIN.json`: **exit 0**. Observed PostgreSQL
18.6 checks include a 0002 pre-existing delivery upgraded through 0003/0004/0005,
identical fresh-install and upgraded schema dumps, late offset-10 materialization
while offset 11 remained leased, independent-lane progress, continuously inserted
new lanes, stable timestamp ties, two held front-lane locks bypassed by a page
of two, reconsideration after lock release, and the earlier 01G lease/ordering,
overflow, outage, and large-backlog checks. The 20,001-row/2,000-extra-lane
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` captured the production candidate,
true-head, and active-lease queries. Candidate plans had `Limit -> LockRows ->
delivery_lanes_discovery_idx`; with two front lanes held, the index scan visited
four rows to return two unlocked lanes. The true-head plan used
`deliveries_lane_head_idx`; the active-lease plan used
`deliveries_lane_active_lease_idx`. Full observed plans, with fixture UUIDs
redacted, are in `V01C_EXPLAIN.json`.

`python -m pytest --basetemp=.pytest-tmp -q`: **exit 0**, 102 passed and four
existing legacy xfails. `python -m pip check`: **exit 0**, no broken
requirements. `python -m compileall -q src tests`, JSON parsing of deployment
examples and EXPLAIN, `docker compose -f deploy/compose.yml config --quiet`,
and `git diff --check`: **exit 0** each. Focused schema, lease, and packaging
tests: **8 passed**.

`python -u tests/integration/verify.py`: **exit 0** in this correction pass.
`python -u tests/integration/verify_v01b.py`: first attempt **exit 1** because
the disposable Kafka container became unhealthy during its restart path;
the project cleaned up. A fresh retry: **exit 0**, including invalid checkpoint,
retention-loss, and capture/intake checks. These are local results; code-review
and CI merge gates remain pending. Overall 01G acceptance remains
**provisional/incomplete**, and ROADMAP.md remains **ACTIVE**.

Post-verification inventory filtered Docker container, network, and volume
names beginning with `updatis-v01`: **0 containers, 0 networks, 0 volumes**.

### Final renewal-race closure and verification - 2026-10-01

Final concurrency review identified a renewal race with newly materialized
lower offsets. `renew_claim` now takes the lane row lock before its fenced
PostgreSQL-clock mutation, serializing it with acquisition. The late
materialization PostgreSQL case additionally renews the offset-11 claim and
confirms offset 10 remains unavailable until completion.

After this final code change, `python -u tests/integration/verify_v01c.py`
with `UPDATIS_EXPLAIN_OUTPUT=docs/V01C_EXPLAIN.json`: **exit 0**; the saved
EXPLAIN was refreshed from this passing run. Fresh and upgraded schema dumps
matched. `python -u tests/integration/verify.py`: **exit 0**;
`python -u tests/integration/verify_v01b.py`: **exit 0**, both rerun against
the final tree. `python -m pytest --basetemp=.pytest-tmp -q`: **exit 0**,
102 passed and four documented legacy xfails. Focused schema and lease tests:
**7 passed**. `python -m pip check`, `python -m compileall -q src tests`,
JSON parsing, `docker compose -f deploy/compose.yml config --quiet`, and
`git diff --check`: **exit 0** each. Final filtered Docker inventory:
**0 containers, 0 networks, 0 volumes** beginning with `updatis-v01`.
Code review and CI merge gates remain pending; 01G acceptance remains
**provisional/incomplete** and ROADMAP.md remains **ACTIVE**.
