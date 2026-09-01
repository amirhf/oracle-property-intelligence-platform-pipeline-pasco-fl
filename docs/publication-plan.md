# Pasco publication plan

This document specifies publication control. Local deterministic graph and
object CIDs have been computed and validated. Provider-returned or remotely
verified CIDs and remote publication-effect records do not yet exist. Buckets,
credentials, IPNS network keys and published artifacts also remain unproven.

## Exactly two domains

### Open data

- Bucket: `elephant-oracle-open-data-pasco`
- IPNS label: `oracle-open-data-pasco`
- IPNS network key: **PENDING**

Contents are one evidence-bearing JSON document per property, sharded property indexes, a deterministic permit-to-property index, embedded complete permit records, run summaries, `index.json`, and `manifest.json`.

### Query data

- Provisional bucket: `elephant-oracle-query-table-pasco`
- Owner confirmation: **PENDING — do not create or access**
- IPNS label: `oracle-query-table-pasco`
- IPNS network key: **PENDING**

The preferred representation is one property query Parquet at the standard `query-tables/pasco/query-table.parquet` path. It contains searchable permit aggregates while full permit records remain in open-data property documents.

A separate permit Parquet is allowed only after measurement proves the preferred representation insufficient. It must then live with the property Parquet under one directory/CID in this same query bucket and IPNS domain. It does not create another bucket or IPNS name. Changing representation invalidates the prior plan and approval.

## Canonical configuration

Required operator-facing names:

- `FILEBASE_ACCESS_KEY`
- `FILEBASE_SECRET_KEY`
- Optional `FILEBASE_ACCESS_KEY_PASCO`
- Optional `FILEBASE_SECRET_KEY_PASCO`
- `FILEBASE_OPEN_DATA_BUCKET_PASCO`
- `FILEBASE_QUERY_TABLE_BUCKET_PASCO`
- Optional `FILEBASE_QUERY_TABLE_IPNS_LABEL`

Pasco-specific credentials override shared credentials. The same credential value is not required under duplicate names.

Manual scripts may receive the following only through an in-process child environment:

- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_BUCKET`
- `S3_ENDPOINT=https://s3.filebase.io`
- `FILEBASE_IPNS_LABEL`
- Derived `FILEBASE_API_TOKEN` when the pinned script requires it

Mapped/derived values are not persisted or logged.

## Dry run

Dry run performs export and validation without S3 PUT, Filebase object creation, IPNS creation or IPNS update. It records:

- Artifact SHA-256 hashes.
- Source, database and export reconciliation counts.
- Schema and schema hash.
- Parquet validity and DuckDB readability.
- Fixture-exclusion result.
- Intended object keys, bucket and label.
- Representation mode and source watermark.
- Publication plan hash.
- `publishedCid: null` and `ipnsMutationPerformed: false`.

The local publication graph uses the pinned `ipfs-only-hash@4.0.0` / `ipfs-unixfs-importer@7.0.3` CIDv0 UnixFS-file profile. A plan is not approvable until every property, shard, root, manifest and Parquet object's bytes, SHA-256 and expected CID are fixed and the complete graph traverses successfully.

The reader-facing graph is fixed as:

```text
open-data IPNS -> index.json CID -> shard CID -> property JSON CID
query-table IPNS -> Parquet CID -> property_cid -> same property JSON CID
```

The expected CIDs are deterministic local artifact identities. Remote execution must still reject a missing or unequal Filebase-returned CID before any IPNS mutation. No real Filebase executor is selected by the local runtime.

Publication plans use schema version `1.1.0`. The plan binds the exact sealed projection materialization (or isolated historical sample run), graph edges and roots, CID and Parquet profiles, complete object inventory and both remote target identities. Sample plans create no authoritative head and are never approvable for authoritative IPNS publication.

## Candidate source-snapshot control representation

The candidate-owned full-source-snapshot demonstration uses a separate,
noncanonical version `2.0.0` plan. It does not relabel the source as
`authoritative_complete`: public coverage is `source_snapshot`, meaning the
complete membership represented by the exact bound source snapshot under the
owner-assumed source classification. The unresolved difference between the
published Pasco statistic and the source snapshot remains disclosed.

Large property inventories are represented as deterministic control
collections rather than one unbounded plan or manifest array. The compact plan
binds a compact manifest and three collections: object inventory, graph edges,
and manifest entries. Each collection has a JSON index containing strictly
ordered, non-overlapping shard ranges and each shard's count, byte size,
SHA-256, expected CID and immutable object key. Shards are newline-delimited
canonical JSON and are limited to 8 MiB; compact indexes are limited to 16 MiB.
The plan binds the collection roots and one full-inventory commitment. Missing,
duplicated, reordered, overlapping, oversized or byte/hash/CID-mismatched
control objects fail closed.

Hosted cold initialization is intended to read only the compact plan, compact
manifest and three bound collection indexes. A property lookup fetches only
the applicable bound inventory/edge/manifest shard plus its root, graph shard
and property objects. Complete local verification streams every control shard
in order and recomputes the collection and full-inventory commitments. The
query-table reader may still materialize the complete 325,213-row query
projection on its first query-table hydration. That first-query memory,
duration and range-request profile must be measured successfully under the
selected Vercel Function limits before a hosted cutover; compact controls alone
do not prove hosted readiness.

The source-snapshot Parquet is bound by exact byte size, SHA-256, CID and schema
hash. Its candidate-specific rows use the same truthful `source_snapshot`
classification as the compact plan and coverage/provenance controls. Hosted
access uses bounded HTTP byte ranges through a concurrency-limited
`AsyncBuffer`; a streaming ordered range pass verifies the complete SHA-256 and
both `PAR1` boundaries without retaining the complete file. Range size, count,
concurrency, total transferred bytes, redirects, retries and aggregate buffered
bytes are bounded by the plan/runtime profile. A server that ignores a required
range, returns a mismatched range, or exceeds a bound is rejected.

The compact representation changes the target-bound plan identity and requires
one new exact approval before any upload or IPNS mutation. Building or
validating it does not authorize a remote effect, alter the protected
25-property candidate publication, or weaken owner/canonical publication gates.
The candidate-only Vercel environment allowlist, firewall configuration and
Session 2 stop gates are recorded in
[candidate source-snapshot readiness](./candidate-source-snapshot-readiness.md).

## Approval and invalidation

Amir is the execution approver only after:

1. The owner confirms bucket/IPNS assignments.
2. Credentials are provided securely.
3. Dry-run hashes, schemas and counts pass.
4. The exact plan is reviewed.

Approval binds to artifact manifest hash, plan hash, schema hash, buckets, labels, object keys, counts, representation mode and source/run watermark. Any change invalidates approval and requires a new dry run and review. Credentials alone never authorize a live action.

Before any future IPNS mutation, `Publish/pasco` must persist immutable intents for both `open_data` and `query_table` in one transaction. Each intent retains the true prior CID, approved target CID and agreeing provider/public resolution evidence. Timeout, split resolution or an unexpected third CID is recorded durably; a third CID is a hard conflict and is never overwritten automatically. The per-record MCP `publicationStatus` remains data-release eligibility and does not approve a plan or authorize a remote effect.

## Approved execution and verification

Only the county-keyed `Publish/pasco` virtual object normally uploads and mutates IPNS. Manual scripts are break-glass operations and still require verified durable approval.

After approval:

1. Upload immutable objects.
2. Record Filebase-returned or remotely confirmed CIDs.
3. Reconcile remote object counts.
4. Update the two approved IPNS labels as applicable.
5. Verify the Filebase names API points to the intended CID.
6. Resolve IPNS and compare `x-ipfs-roots`.
7. Verify property Parquet first and final four bytes are `PAR1`.
8. Reconcile DuckDB counts against the manifest and PostgreSQL.
9. Resolve a property and permit through the public MCP.
10. Record the immutable prior CID and the new run/publication relationship.

Filebase credentials remain **PENDING**. No live publication is authorized by this baseline.

## Assessment trust boundary and executor gate

This checkpoint proves trusted-service application durability only. PostgreSQL
is private to the Oracle service/operator, there is no arbitrary SQL endpoint,
and the assessment does not claim resistance to a malicious holder of Oracle
database credentials. Loader, approval, execution admission and future
irreversible-effect admission share one short transaction-scoped Pasco
projection-head fence. Plans and approvals are immutable, approvals retain the
exact fenced head/snapshot/authoritative-base/materialization/revision, resolver
observations are reconstructible from bounded immutable evidence, and builders
clean only their private contender directories. The production remote executor
remains disabled.

Recovery observations use receipt schema `1.0.0`, a strict closed object that
stores only outcome, bounded HTTP status/byte/latency values, optional SHA-256
digests for provider request identity and response bytes, and an enumerated
error code. Raw request IDs, headers, URLs, bodies, credentials, cookies,
tokens, arbitrary strings and arbitrary keys are not accepted by either the
application or PostgreSQL. The total outcome matrix is NULL-safe at both
boundaries: `resolved` requires HTTP 200–299 and no error; `unavailable`
requires no status and `provider_unavailable`; `http_error` requires HTTP
400–599 and `http_error` or `rate_limited`; `timeout` and `transport_error`
require no status and their matching error code. Migration 013 enforces this
with an exhaustive PostgreSQL `CASE` and `IS [NOT] DISTINCT FROM`, so JSON null
cannot become SQL `UNKNOWN`. Resolution-cycle IDs are derived from the exact
plan, intent, domain, attempt and sequence. Exact derived-cycle replay compares
all canonical observation and receipt bytes; changed evidence conflicts, and a
unique `(intent_id, evidence_sha256)` constraint prevents the same evidence
from being relabeled as another cycle. The two initial domain cycles are
persisted atomically.

Before a real executor can be enabled, a separate reviewed checkpoint must add
and verify all of the following:

- Separate migration-owner and runtime database roles.
- Runtime-table DML revocation and DB-owned publication-transition procedures.
- Hostile direct-SQL tests against the restricted runtime role.
- Authenticated operator approval identity.
- Migration checksums rooted in the committed production baseline.
- Real Filebase-returned CID receipts matching every locally expected CID.
- Approved byte/request/cost, rate, retry and concurrency ceilings.
- Real IPNS mutation, ambiguous-result recovery and public-resolution evidence.

There is no production database or historical production migration 009. The
current local database is development evidence only. A production deployment
must begin from a fresh committed migration sequence; local additive 012→013
convergence does not prove compatibility with an unknown historical production
database.
