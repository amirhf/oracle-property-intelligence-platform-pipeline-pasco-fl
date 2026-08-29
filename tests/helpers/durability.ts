import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import type {
  PilotRunRequest,
  PreparedPilot,
  PreparedProperty,
} from "../../src/domain/types.js";
import { deterministicId, propertyId, sha256 } from "../../src/lib/hash.js";
import {
  AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  AUTHORITATIVE_PARCEL_SELECTION_SEED,
  PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
} from "../../src/snapshot/coverage.js";
import {
  createSourceObject,
  writePreparedInput,
  writeSourceSnapshot,
  type PreparedInputReference,
  type SourceSnapshotManifest,
} from "../../src/snapshot/model.js";

export const SYNTHETIC_AS_OF = "2026-08-29T00:00:00.000Z";
export const SYNTHETIC_SAMPLE_ALGORITHM = "synthetic-transition-v1";
export const SYNTHETIC_SAMPLE_SEED = "synthetic-non-pii-seed";

export interface SyntheticSnapshot {
  prepared: PreparedPilot;
  reference: PreparedInputReference;
  request: PilotRunRequest;
  snapshot: SourceSnapshotManifest;
}

export interface SyntheticLifecycleSnapshotOptions {
  changedFolios?: readonly string[];
  coverage: "authoritative" | "incomplete" | "sample";
  createdAt?: string;
  folios: readonly string[];
  label: string;
  membershipRule?: string;
  previousAuthoritativeSnapshotId?: string | null;
}

function sourceCount(count: number) {
  return {
    accepted: count,
    parsed: count,
    rejectionReasons: {},
    rejected: 0,
    source: count,
  };
}

function syntheticProperty(
  exactFolio: string,
  changed: boolean,
): PreparedProperty {
  const ordinal = Number(exactFolio.slice(-2));
  const yearBuilt = 1980 + (ordinal % 40);
  return {
    buildings: [
      {
        actualYearBuilt: yearBuilt,
        buildingNumber: "1",
        buildingSection: "1",
        effectiveYearBuilt: yearBuilt,
        exactFolio,
        heatedSquareFeet: 1_000 + ordinal,
        observedCondition: null,
        roofCover: "SYNTHETIC",
        roofStructure: "SYNTHETIC",
        stories: 1,
        totalSquareFeet: 1_200 + ordinal,
        useDescription: "SYNTHETIC RESIDENTIAL",
      },
    ],
    coordinates: {
      latitude: 28 + ordinal / 10_000,
      longitude: -82 - ordinal / 10_000,
      method: "polygon_centroid",
      sourceCrs: "EPSG:4326",
      sourceLastUpdate: SYNTHETIC_AS_OF,
    },
    owners: [
      {
        exactFolio,
        mailingAddress1: "SYNTHETIC MAILING",
        mailingAddress2: null,
        mailingCity: "SYNTHETIC CITY",
        mailingCountry: "US",
        mailingState: "FL",
        mailingZip: "00000",
        ownerName1: "SYNTHETIC OWNER",
        ownerName2: null,
      },
    ],
    parcel: {
      acres: 0.25,
      exactFolio,
      heatedSquareFeet: 1_000 + ordinal,
      homestead: null,
      neighborhoodCode: "SYNTHETIC",
      propertyUseCode: changed ? "SYNTHETIC-CHANGED" : "SYNTHETIC",
      propertyUseDescription: "SYNTHETIC RESIDENTIAL",
      totalSquareFeet: 1_200 + ordinal,
    },
    permits: [],
    propertyId: propertyId(exactFolio),
    rank: ordinal.toString().padStart(4, "0"),
    siteAddress: {
      city: "SYNTHETIC CITY",
      exactFolio,
      siteAddress: `SYNTHETIC SITE ${ordinal}`,
      zipCode: "00000",
    },
    useGroup: "synthetic",
    yearBucket: "synthetic",
    yearBuilt,
  };
}

export function syntheticFolios(label: "a" | "b"): string[] {
  const last = label === "a" ? 25 : 24;
  const folios = Array.from(
    { length: last },
    (_, index) => `SYNTH-${(index + 1).toString().padStart(2, "0")}`,
  );
  return label === "a" ? folios : [...folios, "SYNTH-26"];
}

export async function createSyntheticSnapshot(
  dataDir: string,
  label: "a" | "b",
): Promise<SyntheticSnapshot> {
  const folios = syntheticFolios(label);
  return createSyntheticLifecycleSnapshot(dataDir, {
    changedFolios: label === "b" ? ["SYNTH-02"] : [],
    coverage: "sample",
    folios,
    label: `sample-${label}`,
  });
}

export async function createSyntheticLifecycleSnapshot(
  dataDir: string,
  options: SyntheticLifecycleSnapshotOptions,
): Promise<SyntheticSnapshot> {
  if (options.folios.length !== 25) {
    throw new Error(
      "Synthetic Loader snapshots must contain exactly 25 records",
    );
  }
  const rawPath = path.join(dataDir, "synthetic", `${options.label}.zip`);
  const extractedPath = path.join(dataDir, "synthetic", `${options.label}.csv`);
  await mkdir(path.dirname(rawPath), { recursive: true });
  await writeFile(rawPath, `synthetic-source-${options.label}\n`, {
    mode: 0o600,
  });
  await writeFile(
    extractedPath,
    `${options.folios.join("\n")}\nsynthetic-${options.label}\n`,
    { mode: 0o600 },
  );
  const downloadedSource = await createSourceObject({
    dataDir,
    filePath: rawPath,
    observedAt: SYNTHETIC_AS_OF,
    sourceIdentifier: `https://synthetic.invalid/${options.label}/parcel.zip`,
    sourceSystem: "pasco_appraiser",
    stage: "downloaded_source",
  });
  const parcelSource = await createSourceObject({
    dataDir,
    derivedFromSha256: downloadedSource.sha256,
    filePath: extractedPath,
    observedAt: SYNTHETIC_AS_OF,
    sourceIdentifier: PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
    sourceSystem: "pasco_appraiser",
    stage: "extracted_source",
  });
  const folios = [...options.folios];
  const selectedRecordSha256 = sha256(JSON.stringify([...folios].sort()));
  const completeIntent = options.coverage !== "sample";
  const sampling = {
    algorithm: completeIntent
      ? AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM
      : SYNTHETIC_SAMPLE_ALGORITHM,
    seed: completeIntent
      ? AUTHORITATIVE_PARCEL_SELECTION_SEED
      : SYNTHETIC_SAMPLE_SEED,
    selectedRecordSha256,
    selectionSize: 25,
  };
  const incomplete = options.coverage === "incomplete";
  const snapshotResult = await writeSourceSnapshot({
    asOf: SYNTHETIC_AS_OF,
    coverage: {
      authoritySourceId: parcelSource.sourceId,
      counts: {
        acceptedRecords: 25,
        expectedSourceRecords: incomplete ? 26 : 25,
        observedSourceRecords: incomplete ? 26 : 25,
        parsedRecords: incomplete ? 26 : 25,
        rejectedRecords: incomplete ? 1 : 0,
      },
      membershipRule:
        options.membershipRule ?? "all official parcel rows in Pasco v1",
      previousAuthoritativeSnapshotId:
        options.previousAuthoritativeSnapshotId ?? null,
      selectionKind: completeIntent
        ? "complete_source"
        : "deterministic_sample",
    },
    createdAt: options.createdAt ?? "2026-08-29T00:00:01.000Z",
    dataDir,
    sampling,
    sourceObjects: [downloadedSource, parcelSource],
  });
  const changedFolios = new Set(options.changedFolios ?? []);
  const properties = folios.map((folio) =>
    syntheticProperty(folio, changedFolios.has(folio)),
  );
  const prepared: PreparedPilot = {
    artifacts: [],
    gisMetrics: {
      batchCount: 1,
      batchSize: 25,
      concurrency: 1,
      requestCount: 0,
      retryCount: 0,
      reusedBatchCount: 1,
      statusCounts: { checkpoint: 1 },
    },
    permitRequestCount: 0,
    properties,
    resourceMetrics: {
      diskAvailableBytes: 10 * 1024 ** 3,
      elapsedMs: 1,
      peakRssBytes: 64 * 1024 ** 2,
    },
    sampleAlgorithm: sampling.algorithm,
    sampleSeed: sampling.seed,
    selectedRecordSha256,
    selectionSize: 25,
    snapshotId: snapshotResult.manifest.snapshotId,
    snapshotManifestSha256: snapshotResult.reference.sha256,
    sourceCounts: {
      buildings: sourceCount(25),
      owners: sourceCount(25),
      parcel: {
        ...sourceCount(incomplete ? 26 : 25),
        accepted: 25,
        rejected: incomplete ? 1 : 0,
        rejectionReasons: incomplete ? { synthetic_malformed: 1 } : {},
      },
      siteAddresses: sourceCount(25),
    },
    sourceLimitations: ["Synthetic non-PII temporal transition fixture."],
  };
  const reference = await writePreparedInput({
    createdAt: snapshotResult.manifest.createdAt,
    dataDir,
    kind: "pilot",
    prepared,
    sampling,
    snapshot: snapshotResult.manifest,
    snapshotReference: snapshotResult.reference,
  });
  const workflowId = `pasco-synthetic-${options.label}`;
  const request: PilotRunRequest = {
    asOf: SYNTHETIC_AS_OF,
    county: "pasco",
    runId: deterministicId("run", [
      "1.0.0",
      "pipeline-run",
      "pasco",
      workflowId,
    ]),
    sampleAlgorithm: sampling.algorithm,
    sampleSeed: sampling.seed,
    selectionSize: 25,
    workflowId,
  };
  return {
    prepared,
    reference,
    request,
    snapshot: snapshotResult.manifest,
  };
}

export function syntheticLoaderIdempotencyKey(
  workflowId: string,
  preparedInputId: string,
): string {
  return deterministicId("load", [
    "1.0.0",
    "Loader/pasco",
    workflowId,
    preparedInputId,
  ]);
}
