# Open-source readiness checklist

Status: planning checklist, not completed work. The repository is already named `updatis`; its README/application branding and quickstart still describe an order demo. No current license was found in tracked files. All items below remain unchecked until demonstrated.

## Identity, ownership, and license — foundation gate

- [ ] Confirm `Spamziesagcan/updatis` is the intended canonical repository; no rename is presently necessary.
- [ ] Align README, package/image names, API description, examples and links with Updatis branding during implementation. Treat the legacy API's `2.0.0` as unrelated to the new product release sequence.
- [ ] Establish ownership/provenance of code and intended assets, including any recovered frontend. Do not infer reuse rights from public visibility.
- [ ] Adopt Apache 2.0 if ownership and existing obligations permit; add the license and appropriate notices after that review. If conflicting obligations exist, resolve them before publishing a licensing claim.
- [ ] Review dependencies and distributed image contents for applicable license/notice obligations; record decisions and third-party notices as needed.
- [ ] Audit tracked files/history for secrets and sensitive logs; remove tracked operational logs from future source distributions and rotate any confirmed exposed credentials. History rewriting, if needed, is a separately reviewed operation.

This plan does not itself add or change the repository license. Confirm legal/provenance questions with the rights holders; licensing work is a release prerequisite.

## README and five-minute quickstart — v0.1 gate

- [ ] Explain the concrete PostgreSQL-to-webhook problem, users, and supported topology before listing technologies.
- [ ] Show the real data path and distinguish source database, metadata store, capture, intake and delivery checkpoints.
- [ ] Provide a tested clean-checkout command sequence with prerequisites, resource needs, secret bootstrap, one-command startup, status, sample change and receiver output.
- [ ] Measure setup with an unfamiliar developer before advertising “five minutes”; distinguish image download time and external PostgreSQL preparation.
- [ ] Include snapshot behavior, supported types/keys, expected duplicate handling, ordering gaps, and terminal DLQ outcomes.
- [ ] Document safe stop/restart and clearly separate destructive reset/volume deletion.
- [ ] State that the MySQL order application is only a legacy example, not supported MySQL compatibility.
- [ ] Link troubleshooting to real CLI/API diagnostics; remove dead dashboard and placeholder clone instructions in a later documentation implementation change.
- [ ] Mark older production-plan/prompt documents historical and link to the master plan so contributors do not follow contradictory roadmaps.

## Architecture and contributor environment — foundation through v0.2

- [ ] Publish component responsibilities, security boundaries, transaction/offset ownership and failure-state diagrams.
- [ ] Record architecture decisions for metadata isolation, partition ordering, gaps, event identity, redrive and destination idempotency.
- [ ] Provide a pinned supported runtime/image matrix, reproducible development dependencies, sample configuration and clean test commands.
- [ ] Separate lightweight unit/contract checks from infrastructure integration/failure suites; explain fixture isolation and safe cleanup.
- [ ] Add `CONTRIBUTING.md`: setup, issue selection, small changes, tests, docs, review expectations, commit format, and disclosure routing.
- [ ] Add `CODE_OF_CONDUCT.md` with named private enforcement contact/process; do not publish an unenforceable template.
- [ ] Add issue templates for bugs (redacted diagnostics/reproduction), feature proposals (user problem/scope), and documentation fixes.
- [ ] Add a PR template covering problem/result, tests, contract/schema/security impact and migration notes.
- [ ] Document how legacy example changes differ from supported product changes; examples must not silently enlarge the support matrix.

## Security and maintainership — before external adoption

- [ ] Add `SECURITY.md` with a working private disclosure channel, supported versions, response expectations and update policy. `SECURITY_BOUNDARY_MAP.md` is not a disclosure policy.
- [ ] Specify who maintains releases, reviews privileged changes, triages issues, and handles vulnerabilities. Set achievable response targets rather than implying 24/7 support.
- [ ] Publish a lightweight governance decision process, ownership of architecture decisions, contributor credit, and succession expectations.
- [ ] Document payload sensitivity, secret injection, webhook egress policy, trusted proxies, least privilege and access to raw DLQ contents.
- [ ] Explain that public examples must use synthetic data and development credentials only.
- [ ] Establish dependency/image update and security-review cadence with CI evidence for compatibility updates.

## Releases and compatibility — v0.1 through v1.0

- [ ] Use semantic versioning for product releases and explicitly version public API, envelope, configuration and destination interface contracts.
- [ ] Explain pre-1.0 breaking-change policy; never silently change event identity, ordering, or retry semantics in a patch release.
- [ ] Maintain a changelog with user-visible changes, security fixes, breaking contracts and migration instructions.
- [ ] Build versioned artifacts/images from tagged source with checksums/provenance and required CI gates; define who may publish.
- [ ] Test schema/configuration upgrades from supported prior releases and state when downgrade requires restoring a backup.
- [ ] Publish supported deployment/runtime matrix, support/EOL policy, known issues and resource limits.
- [ ] Rehearse release, rollback communication and security patch publication before v1.0.
- [ ] Publish benchmark method and results; distinguish local single-node results from production capacity claims.

## Examples, demo and roadmap — after the quickstart gate

- [ ] Supply a runnable webhook receiver demonstrating transactional idempotency and late-event protection.
- [ ] Supply example snapshot/CRUD data and an outage→DLQ→redrive exercise with visible ordering gaps.
- [ ] Add PostgreSQL destination examples only after its supported mapping/contract tests pass.
- [ ] Record a demo from the current released setup; label any roadmap-only UI as a mockup.
- [ ] Publish the phased roadmap with issue dependencies, release gates and explicit exclusions; avoid unsupported dates.
- [ ] Collect feedback from developers who used only the documentation; repair setup/recovery friction before community promotion.

**Promotion gate:** no broad community launch until an unfamiliar developer can run, understand, diagnose and stop the supported local system using only repository documentation. Stars, screenshots and a successful maintainer demo do not satisfy this gate.

## Suitable good-first-issue candidates

Create these only after the relevant contracts/interfaces exist, with bounded acceptance criteria and maintainer guidance:

| Candidate | Prerequisite | Acceptance |
| --- | --- | --- |
| Correct links and terminology in setup documentation | v0.1 quickstart contract | Link check and clean-read walkthrough pass |
| Add a supported SQL-type normalization fixture | Frozen type mapping | New fixture exercises a documented edge case without changing mapping rules |
| Improve CLI help and examples | Stable initial commands | Examples run against the fixture stack and use synthetic credentials |
| Add dashboard empty/error-state accessibility checks | v0.2 UI routes | Keyboard/screen-reader labels and loading/error behavior verified |
| Add a redacted troubleshooting example | Published runbook/metric names | Example diagnoses a reproducible failure with no secret/payload exposure |
| Add webhook integration example using existing contract | v0.2 recovery semantics | Duplicate and late-redrive handling demonstrated |

Offset ownership, gap atomicity, secret handling, lease fencing and migration recovery are not beginner issues without experienced review. See [roadmap](IMPLEMENTATION_ROADMAP.md) for dependencies and [risk register](RISK_REGISTER.md) for release blockers.
