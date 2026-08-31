import { randomUUID } from "node:crypto";
import { link, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import path from "node:path";

import { z } from "zod";

import { recordCandidateSourceSnapshotDemoPlan } from "../db/candidate-source-snapshot-demo.js";
import { canonicalJson } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import {
  candidateSourceSnapshotPreflightBindingSchema,
  CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS,
  type CandidateSourceSnapshotPreflightBinding,
} from "./candidate-source-snapshot-preflight-binding.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
  CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE_PARQUET,
  CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE,
  CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE,
  CANDIDATE_SOURCE_SNAPSHOT_EXACT_PLAN_ARTIFACT_BYTES,
  CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS,
  CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
  CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS,
  CANDIDATE_SOURCE_SNAPSHOT_PLAN_REQUEST_CEILING,
  PROTECTED_CANDIDATE_SAMPLE_ROLLBACK,
  conservativeCandidateSourceSnapshotPricing,
  createCandidateSourceSnapshotCostEnvelope,
  createCandidateSourceSnapshotDemoPlan,
  createCandidateSourceSnapshotExactUploadBinding,
  createCandidateSourceSnapshotNamespaceId,
  createCandidateSourceSnapshotRequestEnvelope,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotSourceBinding,
  type CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";
import {
  candidateSourceSnapshotPrefixes,
  materializeCandidateSourceSnapshotControlArtifacts,
  type CandidateSourceSnapshotUploadRecord,
} from "./candidate-source-snapshot-controls.js";
import { calculateIpfsCid } from "./ipfs-cid.js";
import type { CompactPublicationManifestIndex } from "./control-artifacts.js";

const cidSchema = z.union([
  z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const targetCidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
export const CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256 =
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.planFileSha256;
export const CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256 =
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.manifestFileSha256;

export interface CandidateSourceSnapshotBuildDescriptor {
  compactManifest: Omit<
    CompactPublicationManifestIndex,
    "manifestEntries" | "version"
  >;
  controlOutputRoot: string;
  planArtifactOutputRoot: string;
  preflight: CandidateSourceSnapshotPreflightBinding;
  source: CandidateSourceSnapshotSourceBinding;
  sourceManifestPath: string;
  sourceManifestFileSha256: string;
  sourcePlanPath: string;
  sourcePlanFileSha256: string;
  targets: {
    openData: {
      bucket: string;
      ipnsLabel: string;
      ipnsNetworkKey: string;
      priorCid: string;
      targetCid: string;
    };
    queryTable: {
      bucket: string;
      ipnsLabel: string;
      ipnsNetworkKey: string;
      priorCid: string;
      targetCid: string;
    };
  };
  version: "1.0.0";
}

export interface CandidateSourceSnapshotBuildResult {
  adoptedExistingControls: boolean;
  exactObjectCount: number;
  exactTotalBytes: number;
  inventoryRootCid: string;
  inventoryRootSha256: string;
  plan: CandidateSourceSnapshotDemoPlan;
  planArtifact: {
    byteSize: number;
    expectedCid: string;
    logicalObjectKey: "candidate-source-snapshot-plan.json";
    remoteObjectKey: string;
    sha256: string;
  };
  planArtifactObjectPath: string;
  recordState: "awaiting_configuration" | null;
}

export const CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST: Omit<
  CompactPublicationManifestIndex,
  "manifestEntries" | "version"
> = {
  classification: {
    canonical: false,
    elephantOwned: false,
    independentlyPascoCertified: false,
    ownerControlled: false,
    publicationClass: CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS,
    resourceOwner: "candidate",
    sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot",
  },
  contracts: {
    canonical: {
      sha256: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.canonicalSchemaSha256,
      version: "1.0.0",
    },
    mcp: {
      sha256: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.mcpSchemaSha256,
      version: "1.2.0",
    },
  },
  county: "pasco",
  coverage: {
    buildings: {
      facts: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.buildingFacts,
      properties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.buildingProperties,
      yearBuiltProxyProperties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.yearBuiltProxyProperties,
    },
    contractors: { availability: "unavailable", facts: 0 },
    coordinates: {
      availableProperties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.coordinateProperties,
      missingProperties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.missingCoordinateProperties,
    },
    membership: "complete_membership_of_exact_source_snapshot_noncanonical",
    ownership: {
      acceptedRows:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.ownershipAcceptedRows,
      malformedRows:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.ownershipMalformedRows,
      properties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.ownershipProperties,
      sourceRows:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.ownershipSourceRows,
    },
    permits: {
      availability: "unavailable",
      facts: 0,
      permitContractorRelationships: 0,
    },
    propertyCount: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.activeProperties,
    siteAddresses: {
      sourceRows: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.siteAddressRows,
      usableProperties:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.siteAddressProperties,
    },
    unresolvedPublishedParcelStatistic: 335_946,
  },
  disclosure: CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE,
  freshness: {
    asOf: "2026-08-23T11:07:02.000Z",
    loadedAt: "2026-08-30T20:52:19.835Z",
    observedAt: "2026-08-23T11:07:02.000Z",
  },
  graph: {
    openDataRootCid: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.openDataRootCid,
    propertyCount: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.activeProperties,
  },
  queryTable: {
    byteSize: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.parquetByteSize,
    expectedCid: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.queryTableCid,
    propertyCount: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE.activeProperties,
    schemaSha256: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.parquetSchemaSha256,
    sha256: CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS.parquetSha256,
  },
  source: {
    authorityClass: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.authorityClass,
    authorityId: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.authorityId,
    materializationId: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.materializationId,
    materializationSha256:
      CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.materializationSha256,
    runId: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.workflowRunId,
    scopeId: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.scopeId,
    selectionSha256: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.folioSetSha256,
    snapshotId: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.snapshotId,
  },
};

function assertFrozenDescriptorBinding(
  descriptor: CandidateSourceSnapshotBuildDescriptor,
): void {
  if (
    canonicalJson(descriptor.source) !==
      canonicalJson(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE) ||
    canonicalJson(descriptor.compactManifest) !==
      canonicalJson(CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST) ||
    descriptor.targets.openData.bucket !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket ||
    descriptor.targets.openData.ipnsLabel !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel ||
    descriptor.targets.openData.ipnsNetworkKey !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey ||
    descriptor.targets.openData.priorCid !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.priorCid ||
    descriptor.targets.openData.targetCid !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.targetCid ||
    descriptor.targets.queryTable.bucket !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket ||
    descriptor.targets.queryTable.ipnsLabel !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel ||
    descriptor.targets.queryTable.ipnsNetworkKey !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey ||
    descriptor.targets.queryTable.priorCid !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.priorCid ||
    descriptor.targets.queryTable.targetCid !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.targetCid
  ) {
    throw new Error(
      "Candidate build descriptor is not the reviewed source/target binding",
    );
  }
}

function safeRoot(value: string, label: string): string {
  const resolved = path.resolve(z.string().min(1).parse(value));
  if (resolved === path.parse(resolved).root) {
    throw new Error(`${label} must not be a filesystem root`);
  }
  return resolved;
}

async function writeImmutablePlanArtifact(options: {
  bytes: Buffer;
  outputRoot: string;
  remoteObjectKey: string;
}): Promise<string> {
  const outputRoot = safeRoot(options.outputRoot, "plan artifact output");
  const finalPath = path.resolve(
    outputRoot,
    ...options.remoteObjectKey.split("/"),
  );
  if (!finalPath.startsWith(`${outputRoot}${path.sep}`)) {
    throw new Error("Candidate plan artifact escaped its output root");
  }
  await mkdir(path.dirname(finalPath), { recursive: true });
  const contender = path.join(
    path.dirname(finalPath),
    `.${path.basename(finalPath)}.contender-${process.pid}-${randomUUID()}`,
  );
  await writeFile(contender, options.bytes, { flag: "wx" });
  try {
    try {
      await link(contender, finalPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const existing = await readFile(finalPath);
      if (!existing.equals(options.bytes)) {
        throw new Error("Existing candidate plan artifact differs", {
          cause: error,
        });
      }
    }
  } finally {
    await unlink(contender).catch(() => undefined);
  }
  return finalPath;
}

function uploadObject(
  record: CandidateSourceSnapshotUploadRecord,
): CandidateSourceSnapshotUploadObject {
  return {
    byteSize: record.byteSize,
    domain: record.domain,
    expectedCid: record.expectedCid,
    logicalObjectKey: record.logicalObjectKey,
    remoteObjectKey: record.remoteObjectKey,
    sha256: record.sha256,
  };
}

export async function buildCandidateSourceSnapshotDemo(options: {
  databaseUrl?: string;
  descriptor: CandidateSourceSnapshotBuildDescriptor;
  record: boolean;
}): Promise<CandidateSourceSnapshotBuildResult> {
  const descriptor = options.descriptor;
  if (descriptor.version !== "1.0.0") {
    throw new Error(
      "Candidate source-snapshot build descriptor is unsupported",
    );
  }
  assertFrozenDescriptorBinding(descriptor);
  if (
    descriptor.sourcePlanFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256 ||
    descriptor.sourceManifestFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256
  ) {
    throw new Error(
      "Candidate build source files are not the frozen publication",
    );
  }
  const preflight = candidateSourceSnapshotPreflightBindingSchema.parse(
    descriptor.preflight,
  );
  const limits = {
    ...CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS,
    maxRequests: CANDIDATE_SOURCE_SNAPSHOT_PLAN_REQUEST_CEILING,
  };
  const pricing = conservativeCandidateSourceSnapshotPricing({
    fixedAccountPlanEvidence: "human_confirmation_required",
    fixedAccountPlanMonthlyUsd: 7.5,
    requestUsdPerThousand: 0.0045,
    storageUsdPerGib: 0.0162,
  });
  const namespaceId = createCandidateSourceSnapshotNamespaceId({
    artifactRepresentationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
    limits,
    pricing,
    source: descriptor.source,
    targets: {
      openData: {
        bucket: descriptor.targets.openData.bucket,
        ipnsLabel: descriptor.targets.openData.ipnsLabel,
        ipnsNetworkKey: descriptor.targets.openData.ipnsNetworkKey,
        priorCid: cidSchema.parse(descriptor.targets.openData.priorCid),
      },
      queryTable: {
        bucket: descriptor.targets.queryTable.bucket,
        ipnsLabel: descriptor.targets.queryTable.ipnsLabel,
        ipnsNetworkKey: descriptor.targets.queryTable.ipnsNetworkKey,
        priorCid: cidSchema.parse(descriptor.targets.queryTable.priorCid),
      },
    },
  });
  const prefixes = candidateSourceSnapshotPrefixes(namespaceId);
  const controlOutputRoot = path.join(
    safeRoot(descriptor.controlOutputRoot, "control output"),
    namespaceId,
  );
  const controls = await materializeCandidateSourceSnapshotControlArtifacts({
    compactManifest: descriptor.compactManifest,
    expectedSourceManifestFileSha256: descriptor.sourceManifestFileSha256,
    expectedSourcePlanFileSha256: descriptor.sourcePlanFileSha256,
    expectedSourceQueryTable: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE_PARQUET,
    namespaceId,
    outputRoot: controlOutputRoot,
    prefixes,
    sourceManifestPath: descriptor.sourceManifestPath,
    sourcePlanPath: descriptor.sourcePlanPath,
  });
  if (
    controls.sourceTargets.openData.expectedCid !==
      descriptor.targets.openData.targetCid ||
    controls.sourceTargets.queryTable.expectedCid !==
      descriptor.targets.queryTable.targetCid
  ) {
    throw new Error(
      "Candidate target CIDs do not match the frozen source inventory roots",
    );
  }
  const maximumObjectCount = controls.uploadWithoutPlan.objectCount + 1;
  const maximumTotalBytes =
    controls.uploadWithoutPlan.bytes +
    CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES;
  const requestEnvelope = createCandidateSourceSnapshotRequestEnvelope({
    limits,
    objectCount: maximumObjectCount,
  });
  const costEnvelope = createCandidateSourceSnapshotCostEnvelope({
    inventoryBytes: maximumTotalBytes,
    limits,
    pricing,
    requestEnvelope,
  });
  const rollbackEvidence = preflight.protectedSampleRollback;
  const planValue = {
    artifactRepresentationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
    classification: {
      canonical: false,
      elephantOwned: false,
      independentlyPascoCertified: false,
      ownerControlled: false,
      publicationClass: CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS,
      resourceOwner: "candidate",
      sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot",
    },
    controlArtifacts: controls.controlArtifacts,
    costEnvelope,
    coverage: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE,
    disclaimer: CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE,
    formatPadding: "",
    inventory: {
      inventoryRootCid:
        controls.controlArtifacts.objectInventory.indexArtifact.expectedCid,
      inventoryRootSha256: controls.controlArtifacts.fullInventoryRootSha256,
      inventoryShardCount: controls.controlArtifacts.objectInventory.shardCount,
      maxObjectBytes: Math.max(
        controls.uploadWithoutPlan.maximumObjectBytes,
        CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
      ),
      objectCount: maximumObjectCount,
      representationVersion: "candidate-upload-inventory-v1",
      totalBytes: maximumTotalBytes,
    },
    limits,
    namespaceId,
    preflight,
    pricing,
    protectedSampleRollback: {
      ambiguityBehavior: "no_mutation_or_rollback",
      cutoverOrder: ["open_data", "query_table"],
      manifest: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest,
      openData: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData,
      plan: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan,
      queryTable: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable,
      rollbackOrder: ["query_table", "open_data"],
      ...rollbackEvidence,
    },
    requestEnvelope,
    source: descriptor.source,
    targets: {
      controlPrefix: prefixes.control,
      openData: {
        ...descriptor.targets.openData,
        domain: "open_data",
        immutablePrefix: prefixes.openData,
        priorCid: cidSchema.parse(descriptor.targets.openData.priorCid),
        targetCid: targetCidSchema.parse(descriptor.targets.openData.targetCid),
      },
      queryTable: {
        ...descriptor.targets.queryTable,
        domain: "query_table",
        immutablePrefix: prefixes.queryTable,
        priorCid: cidSchema.parse(descriptor.targets.queryTable.priorCid),
        targetCid: targetCidSchema.parse(
          descriptor.targets.queryTable.targetCid,
        ),
      },
    },
    version: CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
  } satisfies Parameters<typeof createCandidateSourceSnapshotDemoPlan>[0];
  const unpaddedPlan = createCandidateSourceSnapshotDemoPlan(planValue);
  const unpaddedBytes = Buffer.from(`${canonicalJson(unpaddedPlan)}\n`, "utf8");
  const paddingBytes =
    CANDIDATE_SOURCE_SNAPSHOT_EXACT_PLAN_ARTIFACT_BYTES -
    unpaddedBytes.byteLength;
  if (paddingBytes < 0) {
    throw new Error(
      `Candidate v2.1 plan exceeds the frozen plan-artifact byte binding (${unpaddedBytes.byteLength} > ${CANDIDATE_SOURCE_SNAPSHOT_EXACT_PLAN_ARTIFACT_BYTES})`,
    );
  }
  const plan = createCandidateSourceSnapshotDemoPlan({
    ...planValue,
    formatPadding: " ".repeat(paddingBytes),
  });
  const planBytes = Buffer.from(`${canonicalJson(plan)}\n`, "utf8");
  if (
    planBytes.byteLength !== CANDIDATE_SOURCE_SNAPSHOT_EXACT_PLAN_ARTIFACT_BYTES
  ) {
    throw new Error(
      "Candidate v2.1 plan artifact did not preserve its exact byte binding",
    );
  }
  const planArtifact = {
    byteSize: planBytes.byteLength,
    expectedCid: await calculateIpfsCid(planBytes),
    logicalObjectKey: "candidate-source-snapshot-plan.json" as const,
    remoteObjectKey: `${prefixes.control}candidate-source-snapshot-plan.json`,
    sha256: sha256(planBytes),
  };
  const planArtifactObjectPath = await writeImmutablePlanArtifact({
    bytes: planBytes,
    outputRoot: path.join(
      safeRoot(descriptor.planArtifactOutputRoot, "plan artifact output"),
      namespaceId,
      plan.planId,
    ),
    remoteObjectKey: planArtifact.remoteObjectKey,
  });
  const exactUpload = createCandidateSourceSnapshotExactUploadBinding({
    plan,
    planArtifact,
  });
  const objects = (): AsyncIterable<CandidateSourceSnapshotUploadObject> =>
    (async function* () {
      for await (const record of controls.createUploadRecords()) {
        yield uploadObject(record);
      }
      yield {
        ...planArtifact,
        domain: "open_data" as const,
      };
    })();
  let recordState: "awaiting_configuration" | null = null;
  if (options.record) {
    if (!options.databaseUrl) {
      throw new Error("Database URL is required to record the candidate plan");
    }
    const state = await recordCandidateSourceSnapshotDemoPlan(
      options.databaseUrl,
      { exactUpload, objects: objects(), plan },
    );
    if (state.state !== "awaiting_configuration") {
      throw new Error(
        "Candidate source-snapshot plan did not remain fail-closed",
      );
    }
    recordState = state.state;
  }
  return {
    adoptedExistingControls: controls.adoptedExisting,
    exactObjectCount: exactUpload.exactObjectCount,
    exactTotalBytes: exactUpload.exactTotalBytes,
    inventoryRootCid: plan.inventory.inventoryRootCid,
    inventoryRootSha256: plan.inventory.inventoryRootSha256,
    plan,
    planArtifact,
    planArtifactObjectPath,
    recordState,
  };
}
