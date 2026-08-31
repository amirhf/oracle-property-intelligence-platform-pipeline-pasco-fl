# Candidate source-snapshot readiness

This runbook is a local readiness and handoff record. It does not authorize an
upload, IPNS mutation, approval, Vercel environment change or deployment.

> Candidate-owned, noncanonical Filebase demonstration of the complete parcel
> membership represented by the exact hash-bound August 23, 2026 Pasco
> Property Appraiser source snapshot under owner-assumed snapshot authority. It
> is not represented as Elephant-owned, owner-controlled, independently
> Pasco-certified, or complete under other Pasco reporting definitions. GIS,
> coordinate, related-fact, permit, and contractor coverage is measured and
> reported separately.

Public coverage is `source_snapshot`, not `authoritative_complete`. It means
complete membership represented by the exact bound source bytes under the
owner-assumed snapshot classification. It does not reconcile the separate
335,946 Pasco statistic. Coordinate coverage is partial; related-fact coverage
is reported independently; permit and contractor coverage remains unavailable,
and null must not be presented as zero.

## Hosted read behavior

The hosted service remains read-only. It resolves only the two plan-bound
public IPNS identities, reads immutable objects through compiled public
transports and validates the exact plan, control indexes, object CIDs, SHA-256
values, graph, Parquet schema and MCP contract. It has no PostgreSQL,
filesystem, source-data, local-artifact, fixture, arbitrary-URL, arbitrary-SQL,
upload, approval or IPNS-mutation path.

Cold initialization is intended to admit the compact plan, compact manifest
and the three control indexes without loading every control shard or property.
Property hydration remains bounded to the applicable control shards and graph
objects. Query-table hydration may still decode and retain the complete
325,213-row public query projection on the first data query. The immutable
Parquet is range-read and hash-verified with bounded concurrency and buffering,
but the resulting row index is not yet a disk-backed or page-lazy structure.
Before Vercel cutover, measure a cold first query under the selected Function's
60-second duration and memory allocation, then verify warm-query latency and
memory. A compact cold control path does not by itself prove that first query
fits the hosted runtime.

Final local production-mode verification runs loaded the compact controls in
10–11 ms with five CID reads (40,462 bytes), no property payload reads and no
Parquet range reads. Their first query decoded all 325,213 query rows in
1,675–2,194 ms using 685 bounded range reads (100,163,258 transferred bytes),
two stat calls and a maximum observed process peak RSS of 1,116,143,616 bytes.
Those measurements prove the bounded local reader path, not a hosted memory
allocation. A cold query must still be measured on the selected Vercel
Function before cutover.

## Vercel public-only environment matrix

Set the final values only by copying them from the twice-reproduced target plan
and its immutable compact artifacts. Do not infer or manually recompute a value
in the dashboard. Scope the block to the specifically authorized Preview or
Production environment; a change requires a new deployment and complete
read-plane verification.

| Variable                                  | Required value/source                                                                 |
| ----------------------------------------- | ------------------------------------------------------------------------------------- |
| `ORACLE_MCP_PROVIDER`                     | Literal `public-ipns`                                                                 |
| `MCP_OPEN_DATA_IPNS`                      | Final plan `targets.openData.ipnsNetworkKey`                                          |
| `MCP_QUERY_TABLE_IPNS`                    | Final plan `targets.queryTable.ipnsNetworkKey`                                        |
| `MCP_PUBLIC_RESOLVER_POLICY`              | Literal `candidate_filebase_delegated_v2`                                             |
| `MCP_PUBLIC_CANDIDATE_DEMO_PLAN_ID`       | Final compact plan `planId`                                                           |
| `MCP_PUBLIC_CANDIDATE_DEMO_PLAN_SHA256`   | Final compact plan logical `planSha256`                                               |
| `MCP_PUBLIC_CANDIDATE_SOURCE_PLAN_SHA256` | Bound source plan logical SHA-256                                                     |
| `MCP_PUBLIC_PLAN_CID`                     | Deterministic CID of the final compact plan artifact bytes                            |
| `MCP_PUBLIC_PLAN_SHA256`                  | SHA-256 of the final compact plan artifact bytes, distinct from its logical plan hash |
| `MCP_PUBLIC_MANIFEST_CID`                 | Final compact manifest index CID                                                      |
| `MCP_PUBLIC_MANIFEST_SHA256`              | Final compact manifest index byte SHA-256                                             |
| `MCP_PUBLIC_OPEN_DATA_ROOT_CID`           | Final plan open-data target/root CID                                                  |
| `MCP_PUBLIC_QUERY_TABLE_ROOT_CID`         | Final plan query-table Parquet target CID                                             |
| `MCP_PUBLIC_MAX_CACHE_AGE_SECONDS`        | `300`                                                                                 |
| `MCP_PUBLIC_MAX_JSON_OBJECT_BYTES`        | `16777216` (16 MiB compact-plan/control-index ceiling)                                |
| `MCP_PUBLIC_MAX_PARQUET_BYTES`            | `134217728` (128 MiB; the exact plan binds the smaller actual file)                   |
| `MCP_PUBLIC_MAX_REDIRECTS`                | `2`                                                                                   |
| `MCP_PUBLIC_RETRIES`                      | `1`                                                                                   |
| `MCP_PUBLIC_TRANSPORT_TIMEOUT_MS`         | `20000`                                                                               |
| `MCP_MAX_REQUEST_BYTES`                   | `65536`                                                                               |
| `MCP_MAX_RESPONSE_BYTES`                  | `2097152`                                                                             |
| `MCP_REQUEST_TIMEOUT_MS`                  | `30000`                                                                               |

`NODE_ENV=production` is supplied by Vercel. `MCP_PORT` is not needed by the
Vercel entrypoint. Do not add any `FILEBASE_*`, `CANDIDATE_DEMO_*`, access key,
secret key, Names API token, `DATABASE_URL`, `DATA_DIR`, `MCP_LOCAL_*`, raw IPNS
record or publication-executor variable. In particular, the hosted project
must not receive `CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED`, even with a false
value. No browser-exposed variable is required.

### Reproduced local plan values

Two complete local builds reproduced these values. The executable byte
binding uses the exact 3,457,753,084-byte inventory; the plan separately
reserves 3,474,519,090 bytes so a plan artifact may be as large as its 16 MiB
hard ceiling without changing admission arithmetic.

| Binding                                           | Final value                                                                                                                                                                                                                                              |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Compact plan ID                                   | `snapshotdemo_8bb5cbd74f4b4816e8b4fe54365f48e6`                                                                                                                                                                                                          |
| Compact logical plan SHA-256                      | `b90b64a9f31d672c9deea2d6f3131c8fc627cda2a89c62de53807c63e01a71f5`                                                                                                                                                                                       |
| Compact plan artifact CID / byte SHA-256          | `QmYc1YwAYAm9GhGaATPaLAAxZKSnHDGajpeSMYLeRQZfzg` / `1b40ab64c011db5807d7b7c9305634329d6d43e2370e6728caa0d4c3e62551a9`                                                                                                                                    |
| Compact manifest CID / byte SHA-256               | `QmS5vnqiHLcCFHC6EERBTuffVLwF11RSZmyqTCsYnqBVVq` / `09e2533e17a8e7ff4b9f3fb4c9b037ed418766cc13e7959bf629380f0706c125`                                                                                                                                    |
| Open-data bucket / label / k51 / prior / target   | `cand-amir-pasco-open-data-source-snapshot-demo-v1` / same label / `k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6` / `bafybeieqgp5zh4yfibox2jhfbza442o5voit3tk32a6fywwz3ausidwd2q` / `QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy`   |
| Query-table bucket / label / k51 / prior / target | `cand-amir-pasco-query-table-source-snapshot-demo-v1` / same label / `k51qzi5uqu5di1wl6zp9v2n9j1p6m3zcli0wy58p5ypkjk7qjv38hufawhn9qu` / `bafybeiatknvltt7jcujznmxf6jgizo5f2nbmhhyvw3ksb7edigjguaqn2q` / `QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw` |
| Candidate query-table bytes / SHA-256             | 69,430,565 / `316c4f04748ce54e134f58b4799d32233fa6bdf50898308ab798a060da3097b2`                                                                                                                                                                          |
| Candidate coverage CID / SHA-256 / bytes          | `Qmdimnw4DrusJLzajpBNSkuCJK6Xq23v1921RjavDvq4gN` / `e55455f7d0da745c80a7a75e48b9534aa71a187fdc6254e55f8d07d8eef3e632` / 2,540                                                                                                                            |
| Candidate provenance CID / SHA-256 / bytes        | `QmPgj7zkL8VuBjN42wtcpRWaDg1crwxBAygpH3QZe3awvD` / `d73892bd10f2236518a30094b0ae68c2f538421adeda3a973653180484bd6704` / 31,161                                                                                                                           |
| Object count / exact total bytes                  | 325,312 / 3,457,753,084                                                                                                                                                                                                                                  |
| Successful / maximum / inspection / recovery      | 325,320 / 975,960 / 23,980 / 60 requests; 1,000,000 absolute request ceiling; two retries (three total object attempts)                                                                                                                                  |
| Concurrency / per-request timeout                 | 16 / 20,000 ms                                                                                                                                                                                                                                           |
| Cost estimate / maximum approved spend            | $1.516361548644 incremental successful execution; $4.552421548644 maximum incremental; $12.052421548644 maximum including the disclosed $7.50 Pro monthly plan; $25 immutable spending ceiling                                                           |

The object-inventory index CID is
`QmWgVGagdJzgHHQdYNNY3nMcJzSYwZEtpaaqfiPCQtnzEW`; its full-inventory
commitment is
`1b4f390667ff7cdd82777a34b960790ce97964e90048a5eb4612a7031a776766`.
The open-data root remains
`QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy`; the direct query-table
Parquet root is `QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw`.

## Manual Vercel Firewall configuration

The in-process limiter is bounded defense in depth only. Function instances do
not share its memory. Before enabling the full candidate read plane, configure
one Vercel Firewall rate-limit rule in the existing Oracle project:

1. Open **Project > Firewall > Custom Rules** for the exact Oracle project and
   confirm the intended Preview or Production environment and hostname.
2. Create one OR rule matching raw path exactly `/mcp` **or** raw path prefix
   `/explorer/api/`. Do not add `/health` or the `/` explorer shell.
3. Count by source IP with a fixed 60-second window and 60 requests per window.
   Start with the exceed action set to log, publish the draft and verify normal
   MCP initialization, search and property traffic is not misclassified.
4. After reviewing the firewall log, edit the same rule so excess requests are
   rate-limited (HTTP 429), review `vercel firewall diff`, and explicitly
   publish the draft. Record the rule ID, project, environment, hostname,
   window, threshold, key, action and activation time in the hosted evidence.
5. Re-run `/health`, MCP initialization and `tools/list`, explorer bootstrap,
   one two-page search and one property lookup. Confirm excess data-route
   traffic receives 429 while `/health` and `/` remain outside the rule.

Equivalent non-interactive staging, after separately linking and confirming
the existing project, is:

```sh
vercel firewall rules add "Oracle public data routes: 60/min/IP" \
  --condition '{"type":"raw_path","op":"eq","value":"/mcp"}' \
  --or \
  --condition '{"type":"raw_path","op":"pre","value":"/explorer/api/"}' \
  --action rate_limit \
  --rate-limit-window 60 \
  --rate-limit-requests 60 \
  --rate-limit-keys ip \
  --rate-limit-action log \
  --yes
vercel firewall diff
```

These commands stage a draft; they do not authorize publishing it. Firewall
counters are regional, so this rule is a platform boundary materially stronger
than process-local memory but must not be described as one globally singular
counter. Session 1.5 does not run these commands or change Vercel state.

## Session 2 stop gates

Session 2 must stop before every remote effect unless all of these remain true:

1. Two complete local builds reproduce the plan ID/hash, plan and manifest
   artifact bytes/CIDs, collection roots, inventory, graph, Parquet, counts,
   ordering, object count, byte count and cost/request envelope.
2. The exact target buckets, candidate labels, k51 identities and distinct
   immutable prior CIDs match the sanitized read-only preflight. The protected
   25-property candidate buckets, IPNS identities and immutable objects remain
   separate and unchanged.
3. Account storage, object, request, bandwidth and spending capacity fit the
   immutable hard ceilings. The candidate confirms the currently billed
   Filebase tier is Pro or better; an inferred or stale subscription label is
   insufficient.
4. Durable local state contains one exact unapproved plan in
   `awaiting_approval`, or `awaiting_configuration` until the account-tier
   confirmation is recorded. It contains zero approval, upload effect, IPNS
   intent or mutation records for that plan.
5. A human returns the exact one-line approval statement generated for the
   final plan. It must bind the plan ID/hash, object count/bytes, both buckets,
   labels and k51 identities, both immutable priors and proposed targets,
   plan/manifest roots, request/retry/recovery/concurrency/timeout ceilings,
   spending ceiling and the explicit Pro-or-better confirmation. This runbook
   and the Session 1.5 prompt are not approval.

After exact approval, execution must reload the immutable plan, revalidate its
state and all local bytes, persist upload admission, and remain within every
hard ceiling. Every Filebase-returned CID must equal the deterministic local
CID. Both domain IPNS intents must be durable before the first mutation. Update
open data before query table; preserve each true prior CID; stop without an
automatic overwrite on missing, split, stale, malformed or unexpected-third-CID
evidence. A second-domain failure follows the approved reverse-order recovery
path. The executor must be enabled only for the bounded local operation and
disabled after success or failure. Vercel receives no write configuration.

Only after both targets are publicly verified may a separately authorized
hosted configuration use the public-only matrix above. It must pass the compact
control graph, complete Parquet hash/schema/cardinality and `property_cid`
checks, all six frozen MCP v1.2.0 tools, Explorer privacy, `source_snapshot`
disclosure, null/unavailable permit and contractor semantics, cold first-query
resource measurement, and the staged Firewall verification before promotion.
