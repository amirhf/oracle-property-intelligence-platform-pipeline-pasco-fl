# Owner-accepted Pasco appraisal ingestion

## Authority boundary

The August 23, 2026 `parcel.zip` snapshot is accepted for this checkpoint under
the authority class `owner_assumed_authoritative_snapshot`. This is an explicit
human risk-acceptance decision for one exact source object, not an independent
Pasco certification or a claim about every Pasco reporting definition.

Authoritative completeness applies to parcel membership represented by the
exact hash-bound August 23, 2026 Pasco Property Appraiser parcel.zip snapshot.
It does not assert that the archive contains every parcel counted under every
other Pasco reporting definition. GIS, coordinate, related-fact, permit, and
contractor coverage are measured and reported separately.

The official homepage statistic of 335,946 real-property parcels remains
semantically unreconciled with this snapshot. Its date, category boundaries,
and membership rules are not documented well enough to reinterpret or alter
the accepted archive membership.

## Immutable source identity

| Control                                | Accepted value                                                     |
| -------------------------------------- | ------------------------------------------------------------------ |
| Source system                          | `pasco_appraiser`                                                  |
| Official description                   | Parcel Level Detail                                                |
| Source cycle                           | August 23, 2026                                                    |
| ZIP bytes                              | 7,895,623                                                          |
| ZIP SHA-256                            | `bffeead6aa18d9e53e5da9efafa5533b24e7d563b733b1d327bdc0a5cb62cac9` |
| Sole archive entry                     | `parcel.csv`                                                       |
| CSV bytes                              | 53,529,199                                                         |
| CSV SHA-256                            | `8f06fe9ff8969869a606cf85b5a7722bebd247f5ff47b33288689c3aa4160545` |
| Source/parsed/accepted/distinct folios | 325,213                                                            |
| Rejected/duplicate folios              | 0 / 0                                                              |
| Sorted folio-set SHA-256               | `3cb676d4a52a35f7bc2bcf1a13b5a4c1ca5f21c005bb867078dca1a4d428dfab` |

The preparation rejects any mismatch in the ZIP, archive inventory, extracted
CSV, header schema, row counts, folio uniqueness, or sorted membership hash.
The complete-source selection uses `official-parcel-complete-v1`, seed
`not-applicable`, and includes every accepted parcel row.

## Durable implementation

The authority record binds the human decision, source identity, counts,
selection, parser, transform, canonical schema, snapshot format, exclusions,
and unresolved statistic into deterministic hashes. Migration 022 stores that
record immutably and adds immutable Loader batch-audit checkpoints.

Preparation parses CSV inputs as streams and keeps only bounded folio-indexed
join state. It uses already acquired appraiser objects and locally verified GIS
checkpoints; it contains no acquisition fallback. Normalized writes remain in
Loader/pasco under its transaction, advisory lock, and shared projection-head
fence. An authoritative genesis projection uses batched immutable version,
event, and materialization inserts. Exact replay returns the stored Loader
result without duplicating versions, events, facts, checkpoints, or head
movement.

## Completed local development ingestion

The authorized local development run completed and its exact Loader replay
returned the same canonical result and result SHA-256. The sealed identifiers
are:

| Record                           | Identifier or hash                                                 |
| -------------------------------- | ------------------------------------------------------------------ |
| Run                              | `run_4c74edc0e29eacf0cb4de4b45d57428c`                             |
| Prepared input                   | `prepared_9a893e03572008af8d47c628148f4e9a`                        |
| Source snapshot                  | `snapshot_23e94803bfee6453a047595e80f2fc43`                        |
| Scope                            | `scope_055c2b98f0dc74de092e53bacb1d64ce`                           |
| Authority record                 | `authority_2a6e9cb08d4c8fc12082aa30abb35cab`                       |
| Authority decision SHA-256       | `152ba9180b3d1f96a6b416fe4975684df6d6ce7d8a41e6ac9300a754819a9dcb` |
| Completeness evidence SHA-256    | `db1f7164667dd6e57918108759539320ed844ed5d30db4aad715ea072f2cee53` |
| Source snapshot manifest SHA-256 | `7602e8818f38d42946044152e273a3e0f27a380bcc568e305442f639146db073` |
| Materialization                  | `materialization_981835fc695107653fd830e12c2284db`                 |
| Materialization SHA-256          | `ae295083f7efce4575e15bda381253f0dfe29ea4fe2c4e320256242bc80a513a` |
| Loader result SHA-256            | `81d47175f1be5800388517b8b4e12e5998d1ab0375ac648def53667dfa3a1746` |

The genesis reconciliation produced 325,213 new properties, no changed,
unchanged, inactivated, or reactivated properties, and a sealed current state
of 325,213 active and zero inactive properties. It created 325,213 immutable
property versions and materialized 3,309,790 immutable child facts. The child
facts comprise 2,424,295 availability facts, 276,649 building facts for
261,590 properties, 322,261 ownership facts, 24,995 coordinate facts, and
261,590 roof-proxy facts. No canonical property was hard-deleted and no
tombstone was created.

Loader recorded 1,820 deterministic checkpoints: 82 property-version batches,
82 materialized-property batches, 828 fact-version batches, and 828
materialized-fact batches. Two earlier memory-limit failures rolled the Loader
transaction back without leaving a projection head or partial normalized
writes. The successful run then advanced the scope to revision 1 in one
transaction. Replay reused the one completed Loader effect and made no head,
version, fact, or checkpoint change.

The successful run processed 325,213 properties in 8,407,605 milliseconds
(38.68 properties per second), with peak RSS of 1,641,021,440 bytes. The local
development database grew by 8,256,577,536 bytes, from 384,965,655 to
8,641,543,191 bytes, and 23,622,320,128 bytes of disk remained. It made zero
new GIS requests and reused 60 verified local GIS checkpoints.

## Related-source semantics

Building and ownership facts retain normalized source-record hashes, and the
sealed snapshot binds the related source-object inventory. Locally cached GIS
source objects retain their verified checkpoint hashes. Address facts are
reconciled from a hash-bound source object, but the current selected-address
projection does not retain a distinct source-row evidence binding. Independent
observation timestamps for the building, owner, and address objects were not
preserved and remain explicitly unavailable; the parcel timestamp is not
substituted for them. Every related fact is reconciled inside the same sealed
materialization, but its absence does not affect parcel existence. Missing
construction years, addresses, and coordinates remain unavailable.
`year_built_proxy` exists only when a verified construction year exists and
remains explicitly a proxy. Permit and contractor coverage remains unavailable
with null aggregates.

Owner/contact source facts may be stored and included in future immutable
publication artifacts under the accepted public-source policy, but their values
remain suppressed from the public explorer, CRM, and model-visible
projections.

## Publication boundary

The existing public 25-property candidate demo remains sample,
candidate-controlled, and non-authoritative. This checkpoint does not upload
objects, mutate IPNS, alter that plan, or deploy a service.

The full authoritative publication build was deliberately not run. The current
exporter collapses child-fact evidence into property-level appraiser evidence,
can substitute the parcel observation timestamp for related source objects
whose observation time is unavailable, and can construct an available
state-only address (`"FL"`) when the selected address is absent. Those defects
would break fact-specific provenance and unavailable-value semantics. A future
checkpoint must correct and test those projections before building the full
root/shard/property graph or Parquet table. Only then may a separately bounded
full-output resource review create an exact plan-bound approval request. The
completed candidate plan must not be replaced or mutated implicitly.
