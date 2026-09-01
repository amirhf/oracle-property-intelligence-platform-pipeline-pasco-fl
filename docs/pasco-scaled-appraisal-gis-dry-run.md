# Pasco scaled appraisal/GIS dry run

## Scope and safety

This checkpoint is a deterministic appraisal/GIS-only sample, not complete
Pasco County coverage. It does not query Accela, collect permit or contractor
records, run Sunbiz or BBB enrichment, upload to Filebase, create CIDs, or
mutate IPNS. All source and generated records remain under the ignored
`DATA_DIR`.

## Deterministic selection

- Algorithm: `pasco-countywide-proportional-prefix-v1`
- Seed: `prism-pasco-appraisal-gis-scale-2026-08-28`
- Eligibility: exact appraiser folio, a usable site city/address, and a valid
  appraiser construction year.
- Strata: normalized site city, construction-year bucket, and broad appraiser
  property-use group.
- Ordering: within each stratum, rows are ordered by a SHA-256 rank over the
  algorithm, seed, and exact folio. Strata are interleaved by exact rational
  within-stratum position with deterministic SHA-256 tie-breaks.
- Prefix property: the 5,000-property selection is an exact prefix of the
  25,000-property selection. Permit presence or absence is never an input.

The 5,000-property sample covers 12 cities, 173 strata, all five construction
year buckets, and five property-use groups. The 25,000-property sample covers
12 cities, 222 strata, all five construction-year buckets, and five
property-use groups.

## Run results

| Scope  | Run     | Run ID                                 | Coordinates |    New | Changed | Unchanged | GIS network/checkpoints | Duplicates |
| ------ | ------- | -------------------------------------- | ----------: | -----: | ------: | --------: | ----------------------- | ---------: |
| 5,000  | initial | `run_a466660e20b202005b5fb2ecfe4402c2` |       4,998 |  5,000 |       0 |         0 | 10 / 0                  |          0 |
| 5,000  | repeat  | `run_d80c940ff73d42fc762c875eac36f9e5` |       4,998 |      0 |       0 |     5,000 | 0 / 10                  |          0 |
| 25,000 | initial | `run_977bdb295a0b0944fce5674c76c4481e` |      24,995 | 19,997 |       0 |     5,003 | 50 / 0                  |          0 |
| 25,000 | repeat  | `run_aa8fca42c963998c1f43d5c08409e0c7` |      24,995 |      0 |       0 |    25,000 | 0 / 50                  |          0 |

Every scoped property has ownership, a `year_built_proxy` roof signal, and six
explicit unavailable facts. One malformed owner CSV row was rejected without
logging source content. No permit or contractor row exists, and no permit
request was made.

## Local publication dry run

The repeat 25,000-property run produced one schema-validated canonical JSON
document per property, 25 index shards, a manifest, coverage and provenance
documents, an explicitly unavailable permit index, and a real Zstandard
Parquet query table. The output was rebuilt twice with identical hashes.

| Artifact                             | Result                                                             |
| ------------------------------------ | ------------------------------------------------------------------ |
| Canonical property documents         | 25,000 validated                                                   |
| Open-data files                      | 25,031 files / 98,229,993 bytes                                    |
| All dry-run files                    | 25,034                                                             |
| Parquet rows / distinct property IDs | 25,000 / 25,000                                                    |
| Parquet size                         | 4,230,525 bytes                                                    |
| Coordinate rows                      | 24,995                                                             |
| Non-null permit aggregate rows       | 0                                                                  |
| Roof-signal basis                    | `year_built_proxy`: 25,000                                         |
| Manifest SHA-256                     | `f7c044fdd30c19f64fd401571eec7fec75b3b96443441aae12a3fad3e8ac5cec` |
| Parquet SHA-256                      | `a17d830da02b886d7926619b7a1864856c072ea39a8cc8971a914954ba6485be` |
| Parquet schema SHA-256               | `29af46d2b5f8dde5238a61b085172524db74e5e5475e5728ad92b544dc8b619e` |
| Dry-run plan SHA-256                 | `4513255d4f4d3a88ebfb49deb6e5442c10511d94aafd9a39b409a41bd5bf4b42` |

Permit aggregate columns are null and accompanied by explicit unavailable
source fields; null must not be interpreted as zero real-world permits. Bucket
names and IPNS labels in the ignored local plan are provisional. Publishing
requires owner confirmation plus separately supplied Filebase credentials and
explicit upload/IPNS authorization.
