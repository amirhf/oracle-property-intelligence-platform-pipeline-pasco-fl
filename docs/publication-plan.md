# Pasco publication plan

This document specifies publication control. It does not prove that buckets, credentials, CIDs, IPNS network keys or published artifacts exist.

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

A locally predicted CID is not described as final unless deterministic CAR/UnixFS construction and Filebase preservation of that exact CID are independently proven.

## Approval and invalidation

Amir is the execution approver only after:

1. The owner confirms bucket/IPNS assignments.
2. Credentials are provided securely.
3. Dry-run hashes, schemas and counts pass.
4. The exact plan is reviewed.

Approval binds to artifact manifest hash, plan hash, schema hash, buckets, labels, object keys, counts, representation mode and source/run watermark. Any change invalidates approval and requires a new dry run and review. Credentials alone never authorize a live action.

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
