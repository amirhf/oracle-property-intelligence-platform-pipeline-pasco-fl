# Oracle public read plane

The hosted read plane has one validated source boundary. In production it
resolves the configured open-data and query-table IPNS identities through two
fixed public resolvers, requires an agreeing non-stale CID for each, and then
verifies the immutable publication plan before admitting any dataset. It does
not read PostgreSQL, `DATA_DIR`, fixtures, arbitrary URLs, filesystem paths or
caller SQL.

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

Tests inject only synthetic bytes through the transport interface. They do not
resolve IPNS or contact Filebase, IPFS gateways or hosting providers. The real
HTTP transport remains unusable until the exact IPNS identities, root CIDs,
manifest CID/hash and immutable plan CID/file hash from one published plan are
provided. Hosting additionally requires the intended public runtime/project
identity. None of those inputs authorizes publication or mutation.
