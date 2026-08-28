# Oracle repository authority

This repository is the sole implementation and PR target for the Prism Pasco Oracle pipeline, public Oracle explorer, and shared MCP. One designated Oracle writer owns changes after the architecture freeze. Other workstreams may review or request changes but must not edit this repository concurrently.

## Binding sources

Read `ARCHITECTURE.md`, `ACCEPTANCE.md`, `contracts/contract-lock.json`, and the relevant installed Elephant skills before implementation. The Elephant skills are authoritative for pipeline stages, Restate durability, query-DB loading and matching, open-data publication, query-table publication, and MCP deployment. `docs/soofi-restate-exceptions.md` records the approved exceptions to AWS/CDK-oriented Soofi defaults.

Reference repositories are strictly read-only. Never change them. The final build and deployment must not depend on a sibling checkout, an unpublished local package, or an uncommitted patch.

## Frozen topology

- Docker Compose contains exactly Restate 1.7.2 and PostgreSQL 16.
- Restate exposes 8080 and 9070 with stable node identity `restate-1`.
- PostgreSQL maps host `5433` to container `5432`.
- Never bind, stop, reconfigure, or otherwise touch host port 5432.
- The Node 22.23.2 Restate services process runs on the host on port 9080.
- Host services use an absolute `DATA_DIR` and a `DATABASE_URL` using `localhost:5433`.
- Register the host endpoint with Restate through `host.docker.internal:9080`.
- Do not add a pipeline or worker Compose service without a controller-approved architecture change.

## Pipeline rules

- Canonical county slug is `pasco`; appraisal source form is `pasco_appraiser`.
- Capture raw artifacts before transformation and require an atomic readiness marker.
- Exact folio/request identifier is the property identity; digits-only normalization is matching-only.
- `Loader/<county>` is the sole query-database writer.
- `Publish/<county>` is the sole publication planner, approver, uploader, and IPNS mutator.
- External effects must be idempotent and journaled through Restate.
- Validate diverse property types and an approximately 25-property real pilot before scale.
- Preserve run, source, delta, limitation, reconciliation, and publication history.
- Models must not calculate distance, roof age, permit duration, ownership duration, matching, sorting, or lead eligibility.
- Keep `yearBuilt` distinct from roof-installation facts. Proxies must identify their basis.
- Every factual value must retain Fact/Evidence provenance. Missing values stay explicitly unavailable.

## Contract and publication authority

- Contract version is `1.0.0`. The exact MCP schema bytes and hash in `contracts/contract-lock.json` are binding.
- Shared contract changes require controller approval and synchronized byte-identical updates in both assignment repositories.
- The public MCP registers only the six frozen `prism_v1_*` structured tools. Do not expose raw SQL or legacy tools.
- There are exactly two publication domains, documented in `docs/publication-plan.md`.
- Credentials alone never authorize publication.
- No upload or IPNS mutation occurs before validation, dry-run plan review, and Amir's hash-bound `Publish/pasco/approve` action.
- Query bucket confirmation, network keys, credentials, and publication approval are pending external inputs.

## Evidence and claims

- Production must reject fixture sources, known fixture IDs, and `fixture://` URIs.
- Never silently fall back to fixtures, stale bundled data, or local services.
- Never mark an acceptance item proven without an observable artifact in `ACCEPTANCE.md`.
- Report partial coverage, source limitations, unavailable enrichments, dry-run-only status, and deployment blockers exactly.
- Do not claim a final Filebase CID until upload and remote verification confirm it.
- Do not commit secrets, raw credentials, auth headers, local absolute paths, generated data, or unnecessary PII in logs/evidence.
