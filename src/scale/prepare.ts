import { resourceUsage } from "node:process";
import { mkdir, rename, statfs, writeFile } from "node:fs/promises";
import path from "node:path";

import { ensureAppraiserInputs } from "../appraiser/acquire.js";
import {
  loadPilotCandidateData,
  loadSelectedOwners,
} from "../appraiser/parser.js";
import type { PreparedPilot } from "../domain/types.js";
import { fetchPascoCoordinateBatches } from "../gis/pasco.js";
import { sha256 } from "../lib/hash.js";
import { PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER } from "../snapshot/coverage.js";
import {
  createSourceObject,
  sourceObjectFromArtifact,
  verifySourceObjectBindings,
  writePreparedInput,
  writeSourceSnapshot,
  type PreparedInputReference,
} from "../snapshot/model.js";

// Historical, run-scoped prepared artifacts are retained for read-only
// reconciliation checks. New workflows use content-addressed snapshot paths.
export function scalePreparedPath(dataDir: string, runId: string): string {
  return path.join(dataDir, "pasco", "prepared", runId, "dataset.json");
}
import {
  COUNTYWIDE_SAMPLE_ALGORITHM,
  countywideStratum,
  selectCountywideSample,
} from "./sample.js";

async function atomicWrite(filePath: string, body: string): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(`${filePath}.part`, body, { encoding: "utf8", mode: 0o600 });
  await rename(`${filePath}.part`, filePath);
}

export async function prepareScaleDataset(options: {
  asOf: string;
  dataDir: string;
  runId: string;
  sampleSeed: string;
  selectionSize: number;
}): Promise<PreparedInputReference> {
  if (options.selectionSize !== 5_000 && options.selectionSize !== 25_000) {
    throw new Error("Scale dataset size must be exactly 5,000 or 25,000");
  }
  const startedAt = performance.now();
  const appraiser = await ensureAppraiserInputs(options.dataDir);
  const appraiserObservedAt = "2026-08-23T00:00:00.000Z";
  const downloadedSourceObjects = await Promise.all(
    appraiser.artifacts.map((artifact) =>
      sourceObjectFromArtifact({
        artifact,
        dataDir: options.dataDir,
        observedAt: appraiserObservedAt,
      }),
    ),
  );
  const extractedSourceObjects = await Promise.all(
    Object.entries(appraiser.paths).map(([name, filePath]) => {
      const downloaded = downloadedSourceObjects.find((object) =>
        object.sourceIdentifier.endsWith(`/${name}.zip`),
      );
      return createSourceObject({
        dataDir: options.dataDir,
        derivedFromSha256: downloaded?.sha256 ?? null,
        filePath,
        observedAt: appraiserObservedAt,
        sourceIdentifier: `pasco_appraiser:extracted:${name}`,
        sourceSystem: "pasco_appraiser",
        stage: "extracted_source",
      });
    }),
  );
  const loaded = await loadPilotCandidateData(appraiser.paths);
  const selection = selectCountywideSample(
    loaded.candidates,
    options.sampleSeed,
    options.selectionSize,
  );
  const selectedFolios = new Set(
    selection.map((entry) => entry.parcel.exactFolio),
  );
  const selectionHash = sha256(
    JSON.stringify(selection.map((entry) => entry.parcel.exactFolio).sort()),
  );
  const scopeKey = `countywide-${options.selectionSize}-${selectionHash.slice(0, 16)}`;
  const seedDir = path.join(options.dataDir, "pasco", "seeds", "scales");
  const seedCsvPath = path.join(seedDir, `${scopeKey}.csv`);
  const csvCell = (value: string) => `"${value.replaceAll('"', '""')}"`;
  const seedCsv = [
    "parcel_id,sample_rank,year_bucket,use_group,city",
    ...selection.map((entry) =>
      [
        entry.parcel.exactFolio,
        entry.rank,
        entry.yearBucket,
        entry.useGroup,
        entry.siteAddress?.city ?? "",
      ]
        .map(csvCell)
        .join(","),
    ),
  ].join("\n");
  await atomicWrite(seedCsvPath, `${seedCsv}\n`);

  const stratumCounts = new Map<string, number>();
  for (const entry of selection) {
    const stratum = countywideStratum(entry);
    stratumCounts.set(stratum, (stratumCounts.get(stratum) ?? 0) + 1);
  }
  await atomicWrite(
    path.join(seedDir, `${scopeKey}.manifest.json`),
    `${JSON.stringify({
      algorithm: COUNTYWIDE_SAMPLE_ALGORITHM,
      count: selection.length,
      eligibility:
        "exact folio + site city/address + valid appraiser construction year",
      independentOfPermits: true,
      ordering:
        "size-independent proportional stratum prefix using exact rational rank, then SHA-256 tie-breaks",
      seed: options.sampleSeed,
      selectionHash,
      strata: Object.fromEntries([...stratumCounts].sort()),
    })}\n`,
  );

  const ownerResult = await loadSelectedOwners(
    appraiser.paths.owners,
    selectedFolios,
  );
  loaded.counts.owners = ownerResult.count;
  await verifySourceObjectBindings(options.dataDir, [
    ...downloadedSourceObjects,
    ...extractedSourceObjects,
  ]);
  const gis = await fetchPascoCoordinateBatches({
    batchSize: 500,
    concurrency: 2,
    dataDir: options.dataDir,
    exactFolios: selection.map((entry) => entry.parcel.exactFolio),
    maxRetries: 2,
    scopeKey,
  });
  const properties = selection.map((selected) => ({
    ...selected,
    coordinates: gis.coordinates.get(selected.parcel.exactFolio) ?? null,
    owners: ownerResult.owners.get(selected.parcel.exactFolio) ?? [],
    permits: [],
  }));
  const filesystem = await statfs(options.dataDir);
  const gisObservedAt =
    properties
      .flatMap((entry) => entry.coordinates?.sourceLastUpdate ?? [])
      .filter((value) => Number.isFinite(Date.parse(value)))
      .sort()
      .at(-1) ?? options.asOf;
  const gisSourceObjects = await Promise.all(
    gis.artifacts.map((artifact) =>
      sourceObjectFromArtifact({
        artifact,
        dataDir: options.dataDir,
        observedAt: gisObservedAt,
      }),
    ),
  );
  const sampling = {
    algorithm: COUNTYWIDE_SAMPLE_ALGORITHM,
    seed: options.sampleSeed,
    selectedRecordSha256: selectionHash,
    selectionSize: options.selectionSize,
  };
  const parcelAuthority = extractedSourceObjects.find(
    (object) =>
      object.sourceIdentifier === PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
  );
  if (!parcelAuthority) {
    throw new Error("Prepared snapshot is missing the official parcel object");
  }
  const parcelCounts = loaded.counts.parcel;
  if (!parcelCounts) {
    throw new Error("Prepared snapshot is missing parcel parse counts");
  }
  const snapshot = await writeSourceSnapshot({
    asOf: options.asOf,
    coverage: {
      authoritySourceId: parcelAuthority.sourceId,
      counts: {
        acceptedRecords: parcelCounts.accepted,
        expectedSourceRecords: parcelCounts.source,
        observedSourceRecords: parcelCounts.source,
        parsedRecords: parcelCounts.parsed,
        rejectedRecords: parcelCounts.rejected,
      },
      membershipRule: `${COUNTYWIDE_SAMPLE_ALGORITHM} eligibility and deterministic selection v1`,
      selectionKind: "deterministic_sample",
    },
    dataDir: options.dataDir,
    sampling,
    sourceObjects: [
      ...downloadedSourceObjects,
      ...extractedSourceObjects,
      ...gisSourceObjects,
    ],
  });
  const prepared: PreparedPilot = {
    artifacts: [...appraiser.artifacts, ...gis.artifacts],
    gisMetrics: gis.metrics,
    permitRequestCount: 0,
    properties,
    resourceMetrics: {
      diskAvailableBytes: filesystem.bavail * filesystem.bsize,
      elapsedMs: Math.round(performance.now() - startedAt),
      peakRssBytes: resourceUsage().maxRSS * 1_024,
    },
    sampleAlgorithm: COUNTYWIDE_SAMPLE_ALGORITHM,
    sampleSeed: options.sampleSeed,
    selectedRecordSha256: selectionHash,
    selectionSize: options.selectionSize,
    snapshotId: snapshot.manifest.snapshotId,
    snapshotManifestSha256: snapshot.reference.sha256,
    sourceCounts: loaded.counts,
    sourceLimitations: [
      "Appraisal/GIS-only bounded scale dataset; it is not complete Pasco coverage.",
      "Permit source is unavailable after the Accela challenge stop; missing permits must not be interpreted as none existing.",
      "Contractor source is unavailable because no compliant permit source was established.",
      "Sunbiz and BBB were not collected and remain explicitly unavailable.",
      "This deterministic sample is not authoritative for property absence and cannot inactivate canonical properties.",
    ],
  };
  return writePreparedInput({
    dataDir: options.dataDir,
    kind: "scale",
    prepared,
    sampling,
    snapshot: snapshot.manifest,
    snapshotReference: snapshot.reference,
  });
}
