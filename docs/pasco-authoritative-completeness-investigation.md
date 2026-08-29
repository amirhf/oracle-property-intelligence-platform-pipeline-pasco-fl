# Pasco authoritative completeness investigation

Observed on August 29, 2026, from a US network endpoint. This investigation
used a bounded set of official metadata and documentation requests to determine
whether the cached August 23, 2026 Pasco Property Appraiser `parcel.zip` could
support an `authoritative_complete` parcel-membership claim. It did not refresh
the archive, retrieve parcel or GIS records, access permit systems, or perform
an ingestion.

## Disposition

**AUTHORITY_NOT_PROVEN**

The exact archive identity and extraction were verified, but the official
materials inspected did not establish the required coverage definition, supply
an independent control total for the same source cycle, or reconcile the
published 335,946 real-property-parcel figure with the 325,213 folios parsed
from the archive.

## Cached source evidence

| Object       | Bytes      | SHA-256                                                            |
| ------------ | ---------- | ------------------------------------------------------------------ |
| `parcel.zip` | 7,895,623  | `bffeead6aa18d9e53e5da9efafa5533b24e7d563b733b1d327bdc0a5cb62cac9` |
| `parcel.csv` | 53,529,199 | `8f06fe9ff8969869a606cf85b5a7722bebd247f5ff47b33288689c3aa4160545` |

The ZIP contains one `parcel.csv` member. Reading that member directly from the
cached ZIP reproduced the CSV byte count and SHA-256 above, binding the cached
extraction to the cached archive. The sorted folio-set SHA-256 is
`3cb676d4a52a35f7bc2bcf1a13b5a4c1ca5f21c005bb867078dca1a4d428dfab`.

The existing read-only parse reported:

- Source rows: 325,213
- Parsed rows: 325,213
- Accepted rows: 325,213
- Distinct folios: 325,213
- Rejected rows: 0
- Duplicate folios: 0

These internally consistent counts describe the cached file. They are not an
independent official control total and therefore do not, by themselves, prove
completeness.

## Official sources inspected

- The [Pasco Property Appraiser download catalog](https://downloads.pascopa.com/)
  described `parcel.zip` as weekly “Parcel Level Detail,” dated August 23,
  2026, and linked the exact archive.
- The [official FTP directory](https://ftp01.pascopa.com/real_estate/) listed
  `parcel.zip` at exactly 7,895,623 bytes. HEAD metadata for that object reported
  a last-modified time of August 23, 2026 at 11:07:02 UTC and ETag
  `"0f2386ef32dd1:0"`.
- The [official parcel metadata PDF](https://downloads.pascopa.com/metadata/parcel.pdf)
  documented the CSV layout and identified `Parcel_Num` as the parcel ID field.
  It did not define coverage, exclusions, row cardinality, or an expected count.
- The [Pasco Property Appraiser homepage](https://pascopa.com/) displayed
  335,946 “Real Property Parcels.” The page did not bind that figure to the
  August 23 archive or define its membership rules.
- The [Pasco FAQ](https://pascopa.com/information-and-tools/faqs/) documented
  that some protected property owners do not appear on the public website, but
  did not state how the weekly archive handles those properties.
- The [Florida Department of Revenue data portal](https://floridarevenue.com/property/Pages/DataPortal.aspx)
  and its
  [assessment-roll documentation](https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx)
  described preliminary and final annual assessment-roll submissions and
  public-record exclusions. They did not provide metadata or a control total
  for Pasco's weekly August 23 archive.

## Bounded metadata investigation

The investigation used exactly 12 GET or HEAD requests to the permitted
official Pasco Property Appraiser and Florida Department of Revenue domains.
All returned HTTP 200. The requests covered:

1. The Pasco download catalog.
2. The official real-estate directory listing.
3. HEAD metadata for the exact `parcel.zip` object.
4. HEAD metadata for the parcel data dictionary.
5. The parcel data dictionary PDF.
6. The Pasco homepage statistics.
7. The Florida DOR data portal.
8. The Florida DOR assessment-roll documentation.
9. The Pasco FAQ.
10. Pasco legal notices.
11. The Florida DOR data-portal user-guide directory.
12. The official machine-readable listing of that user-guide directory.

No source ZIP, DOR roll, shapefile, parcel record, GIS feature, or permit record
was downloaded or queried. No challenge, CAPTCHA, authentication, public-records
form, or third-party count was used.

## Authority-test failures

### Coverage definition

“Parcel Level Detail” identifies the file's granularity but does not explicitly
establish that the exact weekly file is countywide, complete and unfiltered for
its defined real-property parcel membership. The catalog and data dictionary do
not state that there is exactly one membership row per current parcel or folio,
and they do not document included or excluded parcel categories.

### Independent expected count

No official manifest, sidecar, checksum publication, trailer, control record,
or dated report independently supplied the expected row or folio count for the
August 23, 2026 archive under matching inclusion, exclusion, and timing rules.
The 325,213 count comes from parsing the archive itself and cannot serve as its
own independent completeness proof.

### Unresolved count discrepancy

The homepage figure of 335,946 exceeds the archive's 325,213 distinct folios by
10,733. The homepage does not establish that its date, parcel categories,
active/inactive treatment, subparcel or group-account handling, public-record
exclusions, or working-roll/final-roll boundary matches the weekly archive.
None of the inspected official sources explained the difference, so no
reconciliation may be inferred.

## Required owner/appraiser request

> Please confirm whether the weekly parcel.zip published by the Pasco County
> Property Appraiser on August 23, 2026 is the complete, unfiltered
> real-property parcel-level export for that source cycle. The exact archive is
> 7,895,623 bytes and our read-only parse contains 325,213 distinct folios with
> zero rejected or duplicate rows. Please provide the official expected
> row/folio count for that archive and document any parcel categories included
> or excluded. Please also explain whether the separately published 335,946
> “Real Property Parcels” figure uses a different date or membership
> definition.

## Operational consequence

- Authoritative Pasco appraisal ingestion remains blocked.
- The deterministic 25,000-property dataset remains usable only as a sample; it
  must not be reclassified as complete or authoritative.
- GIS coverage is a separate enrichment dimension and cannot prove appraiser
  parcel-membership completeness.
- Permit and contractor coverage remains explicitly unavailable.
