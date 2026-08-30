import { createHash } from "node:crypto";
import { resourceUsage } from "node:process";
import {
  lstat,
  mkdir,
  readFile,
  stat,
  statfs,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import unzipper from "unzipper";
import { z } from "zod";

import { APPRAISER_FILES } from "../appraiser/acquire.js";
import {
  APPRAISER_HEADERS,
  assertExactCsvHeader,
  loadPilotCandidateData,
  loadSelectedOwners,
} from "../appraiser/parser.js";
import type { ArtifactCapture, PreparedPilot } from "../domain/types.js";
import {
  constructionYear,
  constructionYearBucket,
  propertyUseGroup,
} from "../pilot/sample.js";
import { loadVerifiedLocalPascoCoordinates } from "../gis/pasco.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { propertyId, sha256 } from "../lib/hash.js";
import {
  AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  AUTHORITATIVE_PARCEL_SELECTION_SEED,
  PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
} from "../snapshot/coverage.js";
import {
  createSourceObject,
  bindDataFile,
  verifySourceObjectBindings,
  writePreparedInput,
  writeSourceSnapshot,
  type PreparedInputReference,
} from "../snapshot/model.js";
import {
  buildOwnerAuthorityRecord,
  PASCO_AUTHORITY_CREATED_AT,
  PASCO_PARCEL_CSV_SHA256,
  PASCO_PARCEL_FOLIO_COUNT,
  PASCO_PARCEL_FOLIO_SET_SHA256,
  PASCO_PARCEL_LAST_MODIFIED,
  PASCO_PARCEL_MEMBERSHIP_CLAIM,
  PASCO_PARCEL_SCOPE_MEMBERSHIP_RULE,
  verifyExactPascoParcelSource,
  writeOwnerAuthorityRecord,
} from "./authority.js";

const PROJECTED_DATABASE_GROWTH_BYTES = 12 * 1024 ** 3;
const PROJECTED_PREPARED_BYTES = 2 * 1024 ** 3;
const REQUIRED_DISK_RESERVE_BYTES = 8 * 1024 ** 3;
const VERIFIED_DISK_FLOOR_BYTES =
  PROJECTED_DATABASE_GROWTH_BYTES +
  PROJECTED_PREPARED_BYTES +
  REQUIRED_DISK_RESERVE_BYTES;

const readyMarkerSchema = z.strictObject({
  bytes: z.number().int().positive(),
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
  sourceSystem: z.literal("pasco_appraiser"),
  sourceUrl: z.string().url(),
});

async function localAppraiserInputs(dataDir: string): Promise<{
  artifacts: ArtifactCapture[];
  paths: {
    building: string;
    owners: string;
    parcel: string;
    siteAddresses: string;
  };
}> {
  const raw = path.join(dataDir, "pasco", "raw", "appraiser", "2026-08-23");
  const staging = path.join(
    dataDir,
    "pasco",
    "staging",
    "appraiser",
    "2026-08-23",
  );
  const artifacts: ArtifactCapture[] = [];
  for (const source of APPRAISER_FILES) {
    const localPath = path.join(raw, source.zipName);
    const extractedPath = path.join(staging, source.csvName);
    const readyMarkerPath = `${localPath}.ready.json`;
    const [binding, extractedBinding, extractedStat, marker] =
      await Promise.all([
        createSourceObject({
          dataDir,
          filePath: localPath,
          lastModified:
            source.zipName === "parcel.zip" ? PASCO_PARCEL_LAST_MODIFIED : null,
          sourceIdentifier: source.url,
          sourceSystem: "pasco_appraiser",
          stage: "downloaded_source",
        }),
        bindDataFile(dataDir, extractedPath),
        lstat(extractedPath),
        readFile(readyMarkerPath, "utf8").then((body) =>
          readyMarkerSchema.parse(JSON.parse(body)),
        ),
      ]);
    if (
      binding.sha256 !== source.sha256 ||
      marker.sha256 !== source.sha256 ||
      marker.bytes !== binding.byteSize ||
      marker.sourceUrl !== source.url
    ) {
      throw new DurableInputError("Local appraiser artifact binding mismatch");
    }
    if (extractedStat.isSymbolicLink() || !extractedStat.isFile()) {
      throw new DurableInputError("Local appraiser extraction is not regular");
    }
    const archive = await unzipper.Open.file(localPath);
    if (
      archive.files.length !== 1 ||
      archive.files[0]?.path !== source.csvName
    ) {
      throw new DurableInputError("Local appraiser archive inventory mismatch");
    }
    const extractedHash = createHash("sha256");
    let extractedBytes = 0;
    for await (const chunk of archive.files[0].stream()) {
      const bytes = Buffer.from(chunk as Uint8Array);
      extractedHash.update(bytes);
      extractedBytes += bytes.byteLength;
    }
    if (
      extractedBytes !== extractedBinding.byteSize ||
      extractedHash.digest("hex") !== extractedBinding.sha256
    ) {
      throw new DurableInputError(
        "Local appraiser extraction binding mismatch",
      );
    }
    artifacts.push({
      bytes: binding.byteSize,
      localPath,
      readyMarkerPath,
      sha256: binding.sha256,
      sourceSystem: "pasco_appraiser",
      sourceUrl: source.url,
    });
  }
  return {
    artifacts,
    paths: {
      building: path.join(staging, "building.csv"),
      owners: path.join(staging, "owners.csv"),
      parcel: path.join(staging, "parcel.csv"),
      siteAddresses: path.join(staging, "site_addresses.csv"),
    },
  };
}

function sourceReconciliation(options: {
  accepted: number;
  matchedProperties: number;
  matchedRecords: number;
  membershipCount: number;
  rejected: number;
  source: number;
}) {
  return {
    ambiguous: 0,
    duplicates: 0,
    matchedProperties: options.matchedProperties,
    matchedRecords: options.matchedRecords,
    missingProperties: options.membershipCount - options.matchedProperties,
    rejected: options.rejected,
    source: options.source,
    unmatchedRecords: options.accepted - options.matchedRecords,
  };
}

export async function prepareAuthoritativePasco(options: {
  asOf: string;
  dataDir: string;
  runId: string;
}): Promise<PreparedInputReference> {
  if (options.asOf !== PASCO_PARCEL_LAST_MODIFIED) {
    throw new DurableInputError(
      "Authoritative ingestion as-of must match the fixed source observation",
    );
  }
  const startedAt = performance.now();
  const exactSource = await verifyExactPascoParcelSource(options.dataDir);
  const appraiser = await localAppraiserInputs(options.dataDir);
  if (appraiser.paths.parcel !== exactSource.csvPath) {
    throw new DurableInputError("Authoritative parcel path is inconsistent");
  }
  await Promise.all([
    assertExactCsvHeader(appraiser.paths.parcel, APPRAISER_HEADERS.parcel),
    assertExactCsvHeader(appraiser.paths.building, APPRAISER_HEADERS.building),
    assertExactCsvHeader(appraiser.paths.owners, APPRAISER_HEADERS.owners),
    assertExactCsvHeader(
      appraiser.paths.siteAddresses,
      APPRAISER_HEADERS.siteAddresses,
    ),
  ]);
  const filesystem = await statfs(options.dataDir);
  const diskAvailableBytes = filesystem.bavail * filesystem.bsize;
  if (diskAvailableBytes < VERIFIED_DISK_FLOOR_BYTES) {
    throw new DurableInputError(
      "Authoritative resource preflight failed before normalized writes",
    );
  }

  const loaded = await loadPilotCandidateData(appraiser.paths);
  const parcelCounts = loaded.counts.parcel;
  if (!parcelCounts)
    throw new DurableInputError("Parcel parse count is absent");
  const folios = [...loaded.candidates.keys()].sort();
  const duplicateFolios = parcelCounts.accepted - folios.length;
  const sortedFolioSetSha256 = sha256(`${folios.join("\n")}\n`);
  const authorityRecord = buildOwnerAuthorityRecord({
    accepted: parcelCounts.accepted,
    distinctFolios: folios.length,
    duplicateFolios,
    parsed: parcelCounts.parsed,
    rejected: parcelCounts.rejected,
    sortedFolioSetSha256,
    source: parcelCounts.source,
  });
  const authorityArtifact = await writeOwnerAuthorityRecord(
    options.dataDir,
    authorityRecord,
  );
  const selectedFolios = new Set(folios);
  const ownerResult = await loadSelectedOwners(
    appraiser.paths.owners,
    selectedFolios,
  );
  loaded.counts.owners = ownerResult.count;
  const gis = await loadVerifiedLocalPascoCoordinates(options.dataDir);

  const properties = folios.map((folio) => {
    const candidate = loaded.candidates.get(folio);
    if (!candidate) throw new DurableInputError("Prepared parcel disappeared");
    const yearBuilt = constructionYear(candidate);
    return {
      ...candidate,
      coordinates: gis.coordinates.get(folio) ?? null,
      owners: ownerResult.owners.get(folio) ?? [],
      permits: [],
      propertyId: propertyId(folio),
      rank: sha256(
        JSON.stringify([
          AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
          AUTHORITATIVE_PARCEL_SELECTION_SEED,
          folio,
        ]),
      ),
      useGroup: propertyUseGroup(candidate.parcel.propertyUseDescription),
      yearBucket:
        yearBuilt === null ? "unavailable" : constructionYearBucket(yearBuilt),
      yearBuilt,
    };
  });
  const membership = new Set(folios);
  const matchedBuildings = properties.filter(
    (property) => property.buildings.length > 0,
  ).length;
  const matchedAddresses = properties.filter(
    (property) => property.siteAddress !== null,
  ).length;
  const matchedOwners = properties.filter(
    (property) => property.owners.length > 0,
  ).length;
  const matchedCoordinates = properties.filter(
    (property) => property.coordinates !== null,
  ).length;
  const sourceObjects = [];
  for (const artifact of appraiser.artifacts) {
    sourceObjects.push(
      await createSourceObject({
        dataDir: options.dataDir,
        filePath: artifact.localPath,
        lastModified: artifact.sourceUrl.endsWith("/parcel.zip")
          ? PASCO_PARCEL_LAST_MODIFIED
          : null,
        sourceIdentifier: artifact.sourceUrl,
        sourceSystem: artifact.sourceSystem,
        stage: "downloaded_source",
      }),
    );
  }
  const downloadedByName = new Map(
    sourceObjects.map((object) => [path.basename(object.relativePath), object]),
  );
  for (const [name, filePath] of Object.entries(appraiser.paths)) {
    const zipName =
      name === "siteAddresses" ? "site_addresses.zip" : `${name}.zip`;
    const parent = downloadedByName.get(zipName);
    sourceObjects.push(
      await createSourceObject({
        dataDir: options.dataDir,
        derivedFromSha256: parent?.sha256 ?? null,
        filePath,
        // The cached extraction is cryptographically linked to its ZIP, but
        // only parcel.zip has an independently recorded Last-Modified value.
        observedAt: null,
        sourceIdentifier: `pasco_appraiser:extracted:${
          name === "siteAddresses" ? "siteAddresses" : name
        }`,
        sourceSystem: "pasco_appraiser",
        stage: "extracted_source",
      }),
    );
  }
  for (const artifact of gis.artifacts) {
    sourceObjects.push(
      await createSourceObject({
        dataDir: options.dataDir,
        filePath: artifact.localPath,
        // Ready markers bind bytes/source URLs but do not record acquisition
        // time. Never reuse the parcel membership watermark for GIS evidence.
        observedAt: null,
        sourceIdentifier: artifact.sourceUrl,
        sourceSystem: artifact.sourceSystem,
        stage: "downloaded_source",
      }),
    );
  }
  sourceObjects.push(
    await createSourceObject({
      dataDir: options.dataDir,
      filePath: authorityArtifact.filePath,
      sourceIdentifier: `oracle_owner_authority:${authorityRecord.authorityRecordId}`,
      sourceSystem: "oracle_owner_authority",
      stage: "downloaded_source",
    }),
  );
  await verifySourceObjectBindings(options.dataDir, sourceObjects);
  const sampling = {
    algorithm: AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
    seed: AUTHORITATIVE_PARCEL_SELECTION_SEED,
    selectedRecordSha256: PASCO_PARCEL_FOLIO_SET_SHA256,
    selectionSize: PASCO_PARCEL_FOLIO_COUNT,
  };
  const parcelAuthority = sourceObjects.find(
    (object) =>
      object.sourceIdentifier === PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
  );
  if (!parcelAuthority || parcelAuthority.sha256 !== PASCO_PARCEL_CSV_SHA256) {
    throw new DurableInputError("Official parcel authority object is absent");
  }
  const snapshot = await writeSourceSnapshot({
    asOf: PASCO_PARCEL_LAST_MODIFIED,
    coverage: {
      authoritySourceId: parcelAuthority.sourceId,
      counts: {
        acceptedRecords: PASCO_PARCEL_FOLIO_COUNT,
        expectedSourceRecords: PASCO_PARCEL_FOLIO_COUNT,
        observedSourceRecords: PASCO_PARCEL_FOLIO_COUNT,
        parsedRecords: PASCO_PARCEL_FOLIO_COUNT,
        rejectedRecords: 0,
      },
      membershipRule: PASCO_PARCEL_SCOPE_MEMBERSHIP_RULE,
      previousAuthoritativeSnapshotId: null,
      previousProjectionSnapshotId: null,
      selectionKind: "complete_source",
    },
    createdAt: PASCO_AUTHORITY_CREATED_AT,
    dataDir: options.dataDir,
    sampling,
    sourceObjects,
  });
  const buildingCount = loaded.counts.building;
  const addressCount = loaded.counts.siteAddresses;
  const ownerCount = loaded.counts.owners;
  if (!buildingCount || !addressCount || !ownerCount)
    throw new DurableInputError("Related source counts are incomplete");
  const prepared: PreparedPilot = {
    artifacts: [...appraiser.artifacts, ...gis.artifacts],
    authorityRecord,
    gisMetrics: gis.metrics,
    permitRequestCount: 0,
    properties,
    resourceMetrics: {
      // Runtime observations are deliberately kept outside the immutable
      // prepared bytes so exact rebuilds retain the same identity.
      diskAvailableBytes: VERIFIED_DISK_FLOOR_BYTES,
      elapsedMs: 0,
      peakRssBytes: 0,
      projectedArtifactCount: PASCO_PARCEL_FOLIO_COUNT + 40,
      projectedDatabaseGrowthBytes: PROJECTED_DATABASE_GROWTH_BYTES,
      projectedPreparedBytes: PROJECTED_PREPARED_BYTES,
      requiredDiskReserveBytes: REQUIRED_DISK_RESERVE_BYTES,
    },
    sampleAlgorithm: AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
    sampleSeed: AUTHORITATIVE_PARCEL_SELECTION_SEED,
    selectedRecordSha256: PASCO_PARCEL_FOLIO_SET_SHA256,
    selectionSize: PASCO_PARCEL_FOLIO_COUNT,
    snapshotId: snapshot.manifest.snapshotId,
    snapshotManifestSha256: snapshot.reference.sha256,
    sourceCounts: loaded.counts,
    sourceLimitations: [
      PASCO_PARCEL_MEMBERSHIP_CLAIM,
      "Authority is an explicit owner risk acceptance, not an independently supplied Pasco control total or certification.",
      "The separately published 335,946 real-property-parcel statistic remains semantically unreconciled.",
      "GIS coordinates are reused only from locally hash-verified sample checkpoints; absence is unavailable, not zero.",
      "Permit and contractor coverage remains unavailable.",
    ],
    sourceReconciliation: {
      building: sourceReconciliation({
        accepted: buildingCount.accepted,
        matchedProperties: matchedBuildings,
        matchedRecords: loaded.matchedRows.building,
        membershipCount: membership.size,
        rejected: buildingCount.rejected,
        source: buildingCount.source,
      }),
      coordinates: sourceReconciliation({
        accepted: gis.coordinates.size,
        matchedProperties: matchedCoordinates,
        matchedRecords: [...gis.coordinates.keys()].filter((folio) =>
          membership.has(folio),
        ).length,
        membershipCount: membership.size,
        rejected: 0,
        source: gis.coordinates.size,
      }),
      ownership: sourceReconciliation({
        accepted: ownerCount.accepted,
        matchedProperties: matchedOwners,
        matchedRecords: properties.reduce(
          (total, property) => total + property.owners.length,
          0,
        ),
        membershipCount: membership.size,
        rejected: ownerCount.rejected,
        source: ownerCount.source,
      }),
      siteAddress: sourceReconciliation({
        accepted: addressCount.accepted,
        matchedProperties: matchedAddresses,
        matchedRecords: loaded.matchedRows.siteAddresses,
        membershipCount: membership.size,
        rejected: addressCount.rejected,
        source: addressCount.source,
      }),
    },
  };
  const metricsDirectory = path.join(
    options.dataDir,
    "pasco",
    "operations",
    options.runId,
  );
  await mkdir(metricsDirectory, { recursive: true });
  await writeFile(
    path.join(metricsDirectory, "preparation-resource-metrics.json"),
    `${JSON.stringify({
      diskAvailableBytes,
      elapsedMs: Math.round(performance.now() - startedAt),
      peakRssBytes: resourceUsage().maxRSS * 1_024,
      projectedArtifactCount: PASCO_PARCEL_FOLIO_COUNT + 40,
      projectedDatabaseGrowthBytes: PROJECTED_DATABASE_GROWTH_BYTES,
      projectedPreparedBytes: PROJECTED_PREPARED_BYTES,
      requiredDiskReserveBytes: REQUIRED_DISK_RESERVE_BYTES,
      schemaVersion: "1.0.0",
    })}\n`,
    { encoding: "utf8", mode: 0o600 },
  );
  await stat(exactSource.zipPath);
  return writePreparedInput({
    createdAt: PASCO_AUTHORITY_CREATED_AT,
    dataDir: options.dataDir,
    kind: "authoritative",
    prepared,
    sampling,
    snapshot: snapshot.manifest,
    snapshotReference: snapshot.reference,
  });
}
