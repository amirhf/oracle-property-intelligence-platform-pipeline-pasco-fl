# Oracle builder prompts

These prompts are implementation handoffs for a later authorized phase. They do not authorize work now.

## Oracle-root implementation prompt

```text
You are the sole writer for the Oracle assignment repository.

Read and obey AGENTS.md, ARCHITECTURE.md, ACCEPTANCE.md, UPSTREAMS.lock.json,
the contract lock and schemas, and the relevant installed Elephant skills.
Treat all architecture-control documents and shared contracts as read-only.
Do not change contract bytes, hashes, acceptance outcomes or evaluator evidence
status. Report a conflict to the controller. Never modify a reference repository
or inspect another candidate's work.

Implement the Pasco pipeline, Oracle explorer and shared MCP within this
repository. The local topology is exact: Compose contains only Restate 1.7.2
and PostgreSQL 16; PostgreSQL maps 5433:5432; host port 5432 is untouched; the
Node 22.23.2 services process runs on the host at 9080 with an absolute DATA_DIR
and localhost:5433 DATABASE_URL; Restate registers host.docker.internal:9080.
Do not add a worker Compose service.

Preserve one county-keyed Loader writer and one county-keyed Publish writer.
Capture raw artifacts first, require readiness markers, journal side effects,
and preserve run/source/delta/limitation/publication history. Validate diverse
property types and an approximately 25-property real pilot before starting the
bounded full-county feeder. Keep exact folio identity and reconcile every stage.

Implement the frozen Fact/Evidence semantics and deterministic calculations.
Keep yearBuilt separate from roof-installation facts. Missing roof, ownership,
business, contractor and BBB information must remain explicitly unavailable.
Models never calculate distance, age, duration, matching, sorting or eligibility.

Use exactly two publication domains from docs/publication-plan.md. Do not create
or access the provisional query bucket until owner confirmation is recorded and
credentials are supplied securely. Prefer searchable permit aggregates in the
property Parquet and complete permit records in open-data property documents with
a deterministic permit index. Do not add another publication domain.

Use only the canonical Filebase variables. A dry run performs no upload/IPNS
mutation and does not claim a final remote CID. Do not perform live publication
until Amir reviews the exact passing plan and invokes the hash-bound durable
approval after external prerequisites are satisfied.

Deploy later to the approved prism-pasco-oracle Vercel project label only when
deployment is explicitly authorized. The project must publicly serve the
read-only explorer, /health and /mcp without evaluator credentials and without
local runtime dependencies. Register only the six frozen prism_v1_* tools.
Clients/models never supply SQL. Enforce mandatory structured-input, body,
radius, page, response, deadline, DuckDB concurrency and queue bounds. Prefer
platform-native rate limiting when already available; otherwise implement and
honestly document an instance-local best-effort limiter without adding Redis or
another service solely for rate limiting.

Vendor only clean pinned sources recorded in UPSTREAMS.lock.json, or use a pinned
Git dependency only after a fresh isolated clone proves build/deploy without a
sibling path. Production must reject fixtures and never fall back to them.

Run implementation tests and collect raw verification outputs in a separate
work area for controller review. Do not edit ACCEPTANCE.md outcomes or
docs/evaluator-evidence.md evidence locations. Do not commit, publish, deploy or
open a PR unless the later implementation authorization explicitly includes it.
```
## Integration-verification prompt

```text
Act as a read-only integration verifier after both repository writers report
completion. Do not change either contract, reference repositories, acceptance
outcomes or evaluator evidence files. Do not inspect another candidate.

Verify identical contract bytes/hashes; exact two-service Compose and host Node
topology; clean upstream pins; a real Pasco pilot and two-run idempotency; exactly
two publication domains; dry-run versus remote CID semantics; approved CIDs/IPNS,
PAR1 and reconciled counts; embedded permit lookup; and a public Oracle Vercel
deployment independent of local services.

MCP discovery must list exactly the six prism_v1_* tools and no raw/legacy SQL
surface. Verify all mandatory resource bounds. Report whether per-IP limiting is
platform-native/distributed or instance-local/best-effort without overstating it.

Verify the separate public CRM uses the same ORACLE_MCP_URL, requires no login,
persists leads across refresh in its signed anonymous session, and isolates a
second clean session. Inspect agent traces for structured calls only and no model
calculations. Return observations and artifact locations to the controller; do
not mark criteria proven. Slowking remains last.
```
