import {
  CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE,
  CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE,
  CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE,
  CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS,
  CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
  CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
  CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS,
  PROTECTED_CANDIDATE_SAMPLE_ROLLBACK,
  conservativeCandidateSourceSnapshotPricing,
  createCandidateSourceSnapshotCostEnvelope,
  createCandidateSourceSnapshotDemoPlan,
  createCandidateSourceSnapshotExactUploadBinding,
  createCandidateSourceSnapshotNamespaceId,
  createCandidateSourceSnapshotRequestEnvelope,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotExactUploadBinding,
  type CandidateSourceSnapshotUploadObject,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS,
  CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS,
} from "../../src/publication/candidate-source-snapshot-preflight-binding.js";
import {
  admitCandidateSourceSnapshotPreflightRequest,
  recordCandidateSourceSnapshotPreflightRequestOutcome,
} from "../../src/db/candidate-source-snapshot-demo.js";

const openPrior = CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData;
const queryPrior = CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable;
const openTarget = CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.targetCid;
const queryTarget =
  CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.targetCid;
const sha = (character: string) => character.repeat(64);

function reference(
  collection: "graph_edges" | "manifest_entries" | "object_inventory",
  expectedCid = openTarget,
) {
  return {
    collection,
    entriesSha256: sha("1"),
    entryBytes: 10,
    entryCount: 1,
    indexArtifact: {
      byteSize: 1,
      expectedCid,
      objectKey: `publication-control/${collection}.index.json`,
      sha256: sha("2"),
    },
    integrityRootSha256: sha("3"),
    shardBytes: 10,
    shardCount: 1,
  } as const;
}

export function syntheticCandidateSourceSnapshotDemo(): {
  exactUpload: CandidateSourceSnapshotExactUploadBinding;
  objects: CandidateSourceSnapshotUploadObject[];
  plan: CandidateSourceSnapshotDemoPlan;
} {
  const limits = { ...CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS };
  const pricing = conservativeCandidateSourceSnapshotPricing({
    fixedAccountPlanEvidence: "human_confirmation_required",
    fixedAccountPlanMonthlyUsd: 7.5,
    requestUsdPerThousand: 0.0045,
    storageUsdPerGib: 0.0162,
  });
  const source = { ...CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE };
  const namespaceTargets = {
    openData: {
      bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
      ipnsLabel: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel,
      ipnsNetworkKey:
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey,
      priorCid: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.priorCid,
    },
    queryTable: {
      bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
      ipnsLabel: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel,
      ipnsNetworkKey:
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey,
      priorCid: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.priorCid,
    },
  };
  const namespaceId = createCandidateSourceSnapshotNamespaceId({
    artifactRepresentationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
    limits,
    pricing,
    source,
    targets: namespaceTargets,
  });
  const controlPrefix = `publication-control/source-snapshot-demo-v1/${namespaceId}/`;
  const controlArtifacts = {
    controlBytes: 20,
    controlObjectCount: 1,
    fullInventoryRootSha256: sha("a"),
    graphEdges: {
      ...reference("graph_edges"),
      collection: "graph_edges" as const,
    },
    manifestEntries: {
      ...reference("manifest_entries"),
      collection: "manifest_entries" as const,
    },
    manifestIndex: {
      byteSize: 1,
      expectedCid: openTarget,
      objectKey: `${controlPrefix}manifest.json`,
      sha256: sha("b"),
    },
    objectInventory: {
      ...reference("object_inventory", openTarget),
      collection: "object_inventory" as const,
    },
    payloadBytes: 10,
    payloadObjectCount: 1,
    version: "1.0.0" as const,
  };
  const inventory = {
    inventoryRootCid: openTarget,
    inventoryRootSha256: controlArtifacts.fullInventoryRootSha256,
    inventoryShardCount: 1,
    maxObjectBytes: 100,
    objectCount: 3,
    representationVersion: "candidate-upload-inventory-v1" as const,
    totalBytes:
      controlArtifacts.payloadBytes +
      controlArtifacts.controlBytes +
      CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
  };
  const requestEnvelope = createCandidateSourceSnapshotRequestEnvelope({
    limits,
    objectCount: inventory.objectCount,
  });
  const costEnvelope = createCandidateSourceSnapshotCostEnvelope({
    inventoryBytes: inventory.totalBytes,
    limits,
    pricing,
    requestEnvelope,
  });
  const rollbackEvidence = {
    verificationEvidenceSha256: sha("c"),
    verifiedAt: "2026-08-31T00:00:00.000Z",
  };
  const preflight = {
    buckets: [
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
        conflictingObjectCount: 0 as const,
        domain: "open_data" as const,
        headStatus: "authenticated" as const,
        prefixStatus: "no_conflicting_publication_prefixes" as const,
        storageNetworkStatus: "ipfs_provider_cid_verified" as const,
      },
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
        conflictingObjectCount: 0 as const,
        domain: "query_table" as const,
        headStatus: "authenticated" as const,
        prefixStatus: "no_conflicting_publication_prefixes" as const,
        storageNetworkStatus: "ipfs_provider_cid_verified" as const,
      },
    ] as const,
    capacityProfile: {
      accountBandwidthBytes: 0,
      accountStorageBytes: 0,
      buckets: [
        {
          bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
          domain: "open_data" as const,
          storageBytes: 0,
        },
        {
          bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
          domain: "query_table" as const,
          storageBytes: 0,
        },
      ] as const,
      subscriptionTierStatus: "human_confirmation_required" as const,
    },
    evidenceSha256: sha("d"),
    identities: [
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
        controlCid: openPrior,
        domain: "open_data" as const,
        ipnsLabel: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey,
        officialGatewayCid: openPrior,
        signedRecordCid: openPrior,
      },
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
        controlCid: queryPrior,
        domain: "query_table" as const,
        ipnsLabel:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey,
        officialGatewayCid: queryPrior,
        signedRecordCid: queryPrior,
      },
    ] as const,
    observedAt: "2026-08-31T00:00:00.000Z",
    protectedSampleRollback: rollbackEvidence,
    requestCount: 12,
  };
  const plan = createCandidateSourceSnapshotDemoPlan({
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
    controlArtifacts,
    costEnvelope,
    coverage: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE,
    disclaimer: CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE,
    formatPadding: "",
    inventory,
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
    source,
    targets: {
      controlPrefix,
      openData: {
        ...namespaceTargets.openData,
        domain: "open_data",
        immutablePrefix: `publications/source-snapshot-demo-v1/${namespaceId}/`,
        targetCid: openTarget,
      },
      queryTable: {
        ...namespaceTargets.queryTable,
        domain: "query_table",
        immutablePrefix: `query-tables/source-snapshot-demo-v1/${namespaceId}/`,
        targetCid: queryTarget,
      },
    },
    version: CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
  });
  const planArtifact = {
    byteSize: 100,
    expectedCid: openTarget,
    logicalObjectKey: "candidate-source-snapshot-plan.json" as const,
    remoteObjectKey: `${controlPrefix}candidate-source-snapshot-plan.json`,
    sha256: sha("e"),
  };
  const exactUpload = createCandidateSourceSnapshotExactUploadBinding({
    plan,
    planArtifact,
  });
  const objects: CandidateSourceSnapshotUploadObject[] = [
    {
      byteSize: 10,
      domain: "open_data",
      expectedCid: openTarget,
      logicalObjectKey: "properties/property_test.json",
      remoteObjectKey: `${plan.targets.openData.immutablePrefix}properties/property_test.json`,
      sha256: sha("f"),
    },
    {
      byteSize: 20,
      domain: "open_data",
      expectedCid: openTarget,
      logicalObjectKey: "inventory/index.json",
      remoteObjectKey: `${controlPrefix}inventory/index.json`,
      sha256: sha("0"),
    },
    {
      byteSize: planArtifact.byteSize,
      domain: "open_data",
      expectedCid: planArtifact.expectedCid,
      logicalObjectKey: planArtifact.logicalObjectKey,
      remoteObjectKey: planArtifact.remoteObjectKey,
      sha256: planArtifact.sha256,
    },
  ];
  return { exactUpload, objects, plan };
}

export async function recordSuccessfulCandidateSourceSnapshotPreflight(
  databaseUrl: string,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<void> {
  const requests = [
    ["open_data", "bucket_head", null],
    ["open_data", "names_read", "filebase_control"],
    ["open_data", "public_resolve", "filebase_gateway"],
    ["open_data", "public_resolve", "delegated_ipfs"],
    ["query_table", "bucket_head", null],
    ["query_table", "names_read", "filebase_control"],
    ["query_table", "public_resolve", "filebase_gateway"],
    ["query_table", "public_resolve", "delegated_ipfs"],
  ] as const;
  const admissions = await Promise.all(
    requests.map(
      async ([domain, operationKind, resolver]) =>
        await admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
          attemptSequence: 1,
          domain,
          operationKind,
          planId: plan.planId,
          planSha256: plan.planSha256,
          redirectSequence: 0,
          resolver,
        }),
    ),
  );
  for (const admission of admissions) {
    await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
      admission,
      completedAt: "2026-08-31T00:00:00.000Z",
      outcome: "succeeded",
      receiptSha256: sha("8"),
    });
  }
}
