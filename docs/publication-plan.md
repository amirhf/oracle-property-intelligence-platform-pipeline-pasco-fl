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
