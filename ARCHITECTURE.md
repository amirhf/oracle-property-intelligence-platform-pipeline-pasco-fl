# Prism Pasco Oracle architecture freeze

Status: approved control baseline. This document defines architecture; it is not implementation or evidence that any runtime or external resource exists.

## Outcome and authority

The Oracle outcome is a durable, incremental Pasco ingestion and publication pipeline that turns official property, permit, ownership, coordinate, contractor, business, and available reputation data into evidence-bearing open-data and query artifacts, a public structured MCP, and a public explorer.

The Oracle repository owns ingestion, normalization, deterministic signals, publication, the shared MCP, and the Oracle explorer. The CRM repository owns only anonymous demo sessions and CRM lead state. CRM reads Oracle exclusively through `ORACLE_MCP_URL`.

## Local mutation plane

Docker Compose contains only:

- `restatedev/restate:1.7.2` on 8080/9070 with node name `restate-1` and persistent `restate-data`.
- `postgres:16` mapped as host `5433` to container `5432` with persistent database data.

The Node 22.23.2 services process runs directly on the macOS host on port 9080. It uses an absolute host `DATA_DIR`, connects to PostgreSQL at `localhost:5433`, and is registered through `host.docker.internal:9080`. Host port 5432 is outside this assignment and must not be touched.

The full registered service set is `CountyIngest`, `IngestChunk`, `Parcel`, `PermitFeed`, `PermitFeedChunk`, `PermitHarvest`, `Loader`, `Publish`, `SunbizIngest`, and `BbbHarvest`. `Loader` and `Publish` are county-keyed single-writer virtual objects.

Raw capture precedes transformation. Atomic readiness markers prevent partial loads. Source and artifact hashes provide change detection. Runs are windowed, idempotent, retryable, and record source counts, deltas, timestamps and limitations. An approximately 25-property real pilot and diverse transform validation must pass before the bounded full-county feeder starts.

## Canonical data and calculations

The normative model is `contracts/canonical-v1.schema.json`. Facts are raw, normalized, derived, or inferred and always carry evidence references when available. Unavailable values contain a reason rather than an invented replacement.

The exact Pasco folio is the identity key. Deterministic IDs are derived from canonical JSON arrays and SHA-256. Models may translate language to validated tool arguments but do not calculate or filter factual signals.

Deterministic code owns:

- WGS84 coordinate normalization and Haversine distance.
- Exact-date or year-precision age calculations.
- Versioned permit status and roofing-relevance rules.
- Whole-UTC-day permit duration.
- Ownership-duration and owner-area classification.
- Entity matching, stable sorting, pagination and eligibility.

`yearBuilt`, `roofInstallationDate`, `roofInstallationYear`, and a proxy-backed `roofAgeSignal` are separate fields. A year-built proxy never becomes an installation fact.

## Database and published data

PostgreSQL is the local normalized query and control store. The exact folio is the parcel uniqueness key. Assignment-owned migrations add pipeline run, source delta and publication records without changing reference repositories.

Publication has exactly two domains:

1. Open data: property documents, embedded permits, evidence, sharded property/permit indexes and run summaries.
2. Query data: one property Parquet row per exact folio, including searchable roofing and permit aggregates.

See `docs/publication-plan.md` for buckets, IPNS, configuration and gates.

## Public hosted data plane

Approved hosting labels:

- Vercel project `prism-pasco-oracle`: public explorer at `/`, public read-only MCP at `/mcp`, and public `/health`.
- No custom domain is required.
- Vercel account/project IDs and deployment URLs remain pending until deployment.

The hosted project reads only public IPNS/IPFS artifacts. It does not use local PostgreSQL, Restate, `DATA_DIR`, source captures, or sibling repositories.

The active MCP v1-family contract revision is `1.1.0`. It supersedes the committed `1.0.0` schema with SHA-256 `714ee037ffca1362870a5135328a783bfe4a0161e7136e09d4d1590894211de7`; the prior hash and source commits remain recorded in `contracts/contract-lock.json` and Git history.

The MCP registers only:

- `prism_v1_get_service_info`
- `prism_v1_get_pipeline_run_summary`
- `prism_v1_search_roofing_opportunities`
- `prism_v1_get_property`
- `prism_v1_get_permit`
- `prism_v1_get_query_schema`

Every advertised tool has a strict structured input and success-output schema. A consuming agent may expose a least-privilege subset of these tools for its workflow, but that does not reduce or alter the six-tool public MCP surface.

Existing Elephant DuckDB machinery may be reused internally, but SQL text is never accepted from a client or model and raw/legacy tools are not registered.

Mandatory resource protections are a 64 KiB request-body limit, 50-mile/80.4672-kilometre radius bound, maximum 100 result rows, maximum 2 MiB serialized response, tool and request deadlines, and bounded DuckDB concurrency/queueing. Platform-native per-IP rate limiting is preferred when already available; otherwise an honestly documented instance-local limiter is acceptable. No extra managed service is introduced solely for rate limiting.

Freshness timestamps and documented source cadence are returned as metadata. Optional caller-provided `observedAtOrAfter` and `publishedAtOrAfter` filters are supported. There is no invented default expiry threshold.

## Reproducibility and pending inputs

`UPSTREAMS.lock.json` records approved source URLs, commits, and tree hashes. No vendor source is copied during the controller phase. Later incorporation excludes repository metadata, builds, secrets, data and unrelated generated files. A pinned Git dependency is allowed only after a fresh isolated clone proves build and deployment without sibling paths.

Pending external inputs:

- Filebase credentials.
- Owner confirmation of the provisional query bucket.
- Open-data and query IPNS network keys.
- Vercel account/project IDs and deployment URL.
- Assignment-sent timestamp.
