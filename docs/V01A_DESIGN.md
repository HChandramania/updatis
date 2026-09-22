# v0.1-a implementation design record

Status: implemented design for roadmap issues 01A, 01B, and 01C.

The supported package is `src/updatis`; the legacy MySQL application remains in
`app/` and the historical root entry points. One product image runs separate API,
worker, and explicit migration commands. The API and worker share code and never
race to apply DDL.

Metadata and example source databases use different PostgreSQL services,
credentials, volumes, private networks, identities, and lifecycle boundaries.
The initial metadata migration records pipelines, immutable configuration
revisions and captured records, events, contiguous intake checkpoints,
deliveries and attempts, dead letters, ordering gaps, and audit records. Schema
creation does not implement any of their later runtime transitions.

Configuration version 1 rejects unknown fields and versions, unsafe URLs,
unsupported source shapes, invalid limits, absent secrets, known metadata/source
identity overlap, unresolved endpoint identity, and all substantive changes to
an existing configuration. Secret values resolve only from named environment or
bounded file references and remain outside stored/exported configuration.

Endpoint comparison is defense in depth, not universal cluster identification.
Aliases behind unknown proxies cannot be proven distinct by DNS alone; operators
must supply stable instance identities and known metadata aliases. Live source
catalog validation remains part of later capture onboarding.
