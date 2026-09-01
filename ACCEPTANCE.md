# Oracle operational acceptance checklist

Documentation state: no criterion has been evaluated or marked proven.

For every row, record one outcome only after implementation and independent observation:

- ☐ Proven — complete observable evidence satisfies the target.
- ☐ Partially proven — real evidence exists but coverage or behavior is incomplete.
- ☐ Blocked — an identified external dependency prevents proof.
- ☐ Unmet — implementation or evidence does not satisfy the target.

Blank outcome boxes mean not evaluated. “Required” is the acceptance target, not an achieved status.

## Geography, data and durability

### O-01 — Pasco default

- Target: **Required**
- Requirement: County slug `pasco` and source `pasco_appraiser` are used consistently.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-02 — Property coverage

- Target: **Required**
- Requirement: Available appraisal property records are loaded with source/expected counts.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-03 — Permit coverage

- Target: **Required**
- Requirement: Available permit records are loaded with explicit coverage window/range and roofing emphasis.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-04 — Permit state and duration

- Target: **Required**
- Requirement: Raw status/dates, normalized status, open-state basis and deterministic open duration are preserved.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-05 — Ownership

- Target: **Required**
- Requirement: Available owner/mailing/transfer data is loaded; ownership duration and area are derived only when supported.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-06 — Contractors

- Target: **Required**
- Requirement: Available contractor identity/license data is loaded and reconciled with match evidence.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-07 — BBB reputation

- Target: **Required behavior; enrichment where available**
- Requirement: Public BBB evidence is returned when defensibly matched; otherwise explicit unavailable state is returned.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-08 — Business identity

- Target: **Required behavior; enrichment where available**
- Requirement: Sunbiz records retain document identity, source-file date and deterministic match evidence.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-09 — Coordinates

- Target: **Required**
- Requirement: WGS84 property coordinates include source and conversion evidence and support radius queries.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-10 — Roof signals

- Target: **Required**
- Requirement: Direct installation facts and permit/year-built proxies remain distinct, with basis and precision.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-11 — Entity reconciliation

- Target: **Required**
- Requirement: Exact folio identity, duplicate detection, orphan checks and enrichment matching are evidenced.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-12 — Provenance and unavailable semantics

- Target: **Required**
- Requirement: Every factual result carries Fact/Evidence provenance or an explicit unavailable reason.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-13 — Pilot before scale

- Target: **Required**
- Requirement: Diverse transform validation and an approximately 25-property real pilot reconcile before full ingestion starts.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-14 — Incremental and idempotent runs

- Target: **Required**
- Requirement: Windowed/on-demand runs preserve content watermarks and demonstrate new/changed/unchanged/deleted deltas across at least two runs.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-15 — Run history and limitations

- Target: **Required**
- Requirement: Runs expose timestamps, source list, counts, deltas, status and source limitations.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-16 — Full available coverage

- Target: **Required**
- Requirement: Run until all available county data is uploaded, or label exact partial coverage without claiming completion.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

## Infrastructure, publication and access

### O-17 — Canonical local topology

- Target: **Required**
- Requirement: Compose has only Restate/PostgreSQL; host Node uses 9080; PostgreSQL maps 5433:5432; host 5432 is untouched.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-18 — Performance and constraints

- Target: **Should-have**
- Requirement: Chunk timings, throughput, retry/pause state and constrained sources are measured and documented.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-19 — No default mutation-plane cost

- Target: **Required**
- Requirement: Hosted read paths continue when local Restate/PostgreSQL and source processes are stopped.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-20 — Two publication domains

- Target: **Required**
- Requirement: Only the approved open-data and query-data Filebase/IPNS domains exist.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-21 — Durable publication gate

- Target: **Required**
- Requirement: Dry run performs no upload/IPNS mutation; approval binds to exact plan/artifact hashes and changes invalidate it.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-22 — Remote publication verification

- Target: **Required**
- Requirement: Returned CIDs, IPNS resolution, `x-ipfs-roots`, remote counts and immutable prior CIDs are verified.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-23 — DuckDB query table

- Target: **Required**
- Requirement: One row per exact folio, zero null/duplicate folios, `PAR1` validity and database reconciliation.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-24 — Public structured MCP

- Target: **Required**
- Requirement: Public `/mcp` and `/health` register only the six `prism_v1_*` tools and enforce resource bounds.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-25 — Public Oracle explorer

- Target: **Required**
- Requirement: Hosted explorer shows runs, sources, map results, facts/evidence and publication references without evaluator login.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

## Roofing-supporting queries and demo

### O-26 — Radius and roof-age search

- Target: **Required**
- Requirement: Deterministic radius search supports configurable roof age and explicit direct/proxy basis.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-27 — Open and long-open permits

- Target: **Required**
- Requirement: Search and sorting use deterministic open state/duration; detail includes contractor and BBB availability.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-28 — Ownership queries

- Target: **Should-have**
- Requirement: Supported records can be filtered by ownership duration and out-of-county/out-of-state owner area.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-29 — Real hosted agent query

- Target: **Required**
- Requirement: Agent uses validated structured MCP calls over real published Pasco records, with no model calculations or SQL.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

### O-30 — Complete evaluator transcript

- Target: **Required**
- Requirement: Clean-browser demo proves run summary, counts, DuckDB, IPFS/IPNS, explorer and agent with no localhost or fixture dependency.
- Outcome: ☐ Proven ☐ Partially proven ☐ Blocked ☐ Unmet
- Evidence: ______________________________

## Pending scoring input

- Assignment-sent timestamp: **PENDING**
- Speed evidence/status: **PENDING — do not infer**
