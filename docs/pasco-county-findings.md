# Pasco pilot source findings

Observed on 2026-08-28 from a US network endpoint. The pilot uses only official,
anonymous Pasco sources. The machine-readable source manifest is
`config/pasco-sources.json`.

The Property Appraiser publishes weekly parcel, building, owner/mailing, and
site-address ZIPs. Their combined compressed size for this pilot is 26,310,070
bytes. Each object was checked with an HTTPS HEAD request before download and is
well below the 5 GiB approval threshold. Real source rows and derived pilot
artifacts remain under the ignored `DATA_DIR`.

Pasco GIS exposes an anonymous ArcGIS REST parcel feature layer keyed by
`HPARCEL`. The pilot requests only the 25 selected folios in one query and asks
ArcGIS for GeoJSON in EPSG:4326. It does not download the 678.61 MB county parcel
ZIP. A deterministic polygon-centroid calculation records its rule and source
geometry provenance.

Pasco County's Accela Citizen Access UI exposed anonymous exact-parcel search
and roofing-specific record types in an interactive browser. The bounded adapter
then detected challenge/CAPTCHA content on its programmatic form GET. Collection
stopped under the access-safety gate after two GET attempts (the initial durable
effect and one Restate retry): no search POST was sent and no additional parcel
was queried. The adapter now classifies that condition as terminal. Permit dates,
status, duration, and contractor identity are therefore unavailable for this
pilot. Municipal permit coverage outside Pasco County's portal is an additional
limitation.

The deterministic sample version is `pasco-pilot-stratified-v1`, with seed
`prism-pasco-real-pilot-2026-08-28`. Eligible rows require an exact folio, a
site address/city, and an appraiser building row with a valid construction year.
Rows are SHA-256 ranked, then greedily selected to add construction-year bucket,
property-use group, and city coverage before filling the remaining 25 positions
by stable hash rank. Permit availability is not an input to selection.
