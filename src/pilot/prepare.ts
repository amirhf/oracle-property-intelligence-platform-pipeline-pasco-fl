import { resourceUsage } from "node:process";
import { mkdir, rename, statfs, writeFile } from "node:fs/promises";
import path from "node:path";

import { ensureAppraiserInputs } from "../appraiser/acquire.js";
import {
  loadPilotCandidateData,
  loadSelectedOwners,
} from "../appraiser/parser.js";
import type { PreparedPilot } from "../domain/types.js";
import { fetchPascoCoordinates } from "../gis/pasco.js";
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
import { PILOT_SAMPLE_ALGORITHM, selectPilot } from "./sample.js";

export async function preparePilot(options: {
  asOf: string;
  dataDir: string;
  runId: string;
  sampleSeed: string;
}): Promise<PreparedInputReference> {
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
  const selection = selectPilot(loaded.candidates, options.sampleSeed);
  const selectedFolios = new Set(
    selection.map((entry) => entry.parcel.exactFolio),
  );
  const seedDir = path.join(options.dataDir, "pasco", "seeds");
  await mkdir(seedDir, { recursive: true });
  const seedCsvPath = path.join(seedDir, "pasco-pilot.csv");
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
  await writeFile(`${seedCsvPath}.part`, `${seedCsv}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(`${seedCsvPath}.part`, seedCsvPath);
  const seedManifestPath = path.join(seedDir, "pasco-pilot.manifest.json");
  await writeFile(
    `${seedManifestPath}.part`,
    JSON.stringify({
      algorithm: PILOT_SAMPLE_ALGORITHM,
      count: selection.length,
      seed: options.sampleSeed,
      strata: selection.map((entry) => ({
        city: entry.siteAddress?.city ?? null,
        rank: entry.rank,
        useGroup: entry.useGroup,
        yearBucket: entry.yearBucket,
      })),
    }),
    { encoding: "utf8", mode: 0o600 },
  );
  await rename(`${seedManifestPath}.part`, seedManifestPath);
  const ownerResult = await loadSelectedOwners(
    appraiser.paths.owners,
    selectedFolios,
  );
  loaded.counts.owners = ownerResult.count;
  await verifySourceObjectBindings(options.dataDir, [
    ...downloadedSourceObjects,
    ...extractedSourceObjects,
  ]);

  const gis = await fetchPascoCoordinates({
    dataDir: options.dataDir,
    exactFolios: [...selectedFolios],
    runId: options.runId,
  });

  const permitRequestCount = 0;
  const properties = selection.map((selected) => ({
    ...selected,
    coordinates: gis.coordinates.get(selected.parcel.exactFolio) ?? null,
    owners: ownerResult.owners.get(selected.parcel.exactFolio) ?? [],
    permits: [],
  }));
  const filesystem = await statfs(options.dataDir);
  const selectedRecordSha256 = sha256(
    JSON.stringify(selection.map((entry) => entry.parcel.exactFolio).sort()),
  );
  const gisSourceObject = await sourceObjectFromArtifact({
    artifact: gis.artifact,
    dataDir: options.dataDir,
    observedAt:
      properties
        .flatMap((entry) => entry.coordinates?.sourceLastUpdate ?? [])
        .filter((value) => Number.isFinite(Date.parse(value)))
        .sort()
        .at(-1) ?? options.asOf,
  });
  const sampling = {
    algorithm: PILOT_SAMPLE_ALGORITHM,
    seed: options.sampleSeed,
    selectedRecordSha256,
    selectionSize: properties.length,
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
      membershipRule:
        "deterministic 25-property stratified appraisal sample eligibility v1",
      selectionKind: "deterministic_sample",
    },
    dataDir: options.dataDir,
    sampling,
    sourceObjects: [
      ...downloadedSourceObjects,
      ...extractedSourceObjects,
      gisSourceObject,
    ],
  });

  const prepared: PreparedPilot = {
    artifacts: [...appraiser.artifacts, gis.artifact],
    gisMetrics: gis.metrics,
    permitRequestCount,
    properties,
    resourceMetrics: {
      diskAvailableBytes: filesystem.bavail * filesystem.bsize,
      elapsedMs: Math.round(performance.now() - startedAt),
      peakRssBytes: resourceUsage().maxRSS * 1_024,
    },
    sampleAlgorithm: PILOT_SAMPLE_ALGORITHM,
    sampleSeed: options.sampleSeed,
    selectedRecordSha256,
    selectionSize: properties.length,
    snapshotId: snapshot.manifest.snapshotId,
    snapshotManifestSha256: snapshot.reference.sha256,
    sourceCounts: loaded.counts,
    sourceLimitations: [
      "Pasco Accela collection stopped after challenge/CAPTCHA content was detected on the anonymous form GET; no permit search POST was sent, and permits and contractors are unavailable for this pilot.",
      "This deterministic sample is not authoritative for property absence and cannot inactivate canonical properties.",
    ],
  };
  return writePreparedInput({
    dataDir: options.dataDir,
    kind: "pilot",
    prepared,
    sampling,
    snapshot: snapshot.manifest,
    snapshotReference: snapshot.reference,
  });
}
