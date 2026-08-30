# Oracle public read plane

The hosted read plane has one validated source boundary. The default
owner/canonical policy resolves the configured open-data and query-table IPNS
identities through two fixed public resolvers and requires an agreeing
non-stale CID for each. The closed candidate-only
`candidate_filebase_delegated_v2` policy instead requires the official
Filebase public gateway plus a signature-, identity-, sequence- and
validity-checked record from the compiled IPFS Delegated Routing V1 origin.
Both profiles then verify the immutable publication plan before admitting any
dataset. The read plane does not read PostgreSQL, `DATA_DIR`, fixtures,
arbitrary URLs, filesystem paths or caller SQL.

The open-data name must resolve to the configured `index.json` CID. Traversal
is fixed to `index.json -> shard -> canonical property`; every object is
bounded and verified against its plan SHA-256 and deterministic UnixFS CID.
The query-table name must resolve directly to the configured Parquet CID. The
Parquet is decoded in memory, its established Elephant columns and Oracle
extensions are type-checked, and every `property_cid` must equal the canonical
property CID in the graph. Contract metadata must remain MCP 1.2.0 with schema
hash `9fc112ef8a4e2120593c3dc20c90073b0eb96596817c96112f63fd258bb7c131`.

The public endpoints are read-only:

- `/health` reports service, contract and provider mode.
- `/mcp` exposes exactly the six frozen tools over stateless Streamable HTTP.
- `/` renders the local explorer.
- `/explorer/api/bootstrap`, `/explorer/api/search` and
  `/explorer/api/property` expose bounded explorer views.

The explorer executes the frozen MCP search/property operations and renders a
privacy-reduced projection. It does not render owner names, public mailing
values, phone/email values or contractor identities. Availability, evidence,
coordinate state, `year_built_proxy`, null permit aggregates, coverage mode,
freshness and publication references remain explicit. Sample and partial
coverage are visibly labeled as not complete Pasco County coverage.

Automated tests inject only synthetic bytes through the transport interface.
They do not resolve IPNS or contact Filebase, IPFS gateways or hosting
providers. A separate bounded local smoke check exercised the real
credential-free candidate identities and immutable artifacts. The HTTP
transport remains fail-closed until the exact IPNS identities, root CIDs,
manifest CID/hash and immutable plan CID/file hash from one validated
publication are provided. Candidate mode additionally requires the exact
candidate plan ID/hash and source-plan hash. Hosting requires the intended
public runtime/project identity. None of those inputs authorizes publication or
mutation.

## Candidate preview deployment

`api/index.ts` adapts the same Node request handler to a Vercel Function.
`vercel.json` routes only `/`, `/health`, `/mcp` and the three explorer API
paths to that handler. A hosted deployment uses `NODE_ENV=production` and must
set `ORACLE_MCP_PROVIDER=public-ipns`; local-artifact and fixture fallbacks stay
rejected. The preview requires the complete hash-bound `MCP_PUBLIC_*` and IPNS
configuration listed in `.env.example`. Environment changes require a new
preview deployment. No Vercel project or hosted runtime is selected by this
repository configuration.

Temporary candidate-owned Filebase demonstration of protocol compatibility.
The buckets and IPNS identities are candidate-controlled and are not represented
as Elephant-owned, owner-approved, authoritative-complete, or the final
canonical assessment publication.
