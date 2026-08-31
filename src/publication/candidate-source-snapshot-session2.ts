import path from "node:path";

import {
  loadCandidateSourceSnapshotDemoExecutionAdmission,
  loadCandidateSourceSnapshotDemoPlan,
  PostgresCandidateSourceSnapshotUploadJournal,
  type CandidateSourceSnapshotDurableState,
} from "../db/candidate-source-snapshot-demo.js";
import {
  buildCandidateSourceSnapshotDemo,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
  type CandidateSourceSnapshotBuildDescriptor,
  type CandidateSourceSnapshotBuildResult,
} from "./candidate-source-snapshot-build.js";
import { loadCandidateSourceSnapshotExecutionConfig } from "./candidate-source-snapshot-executor-config.js";
import {
  BoundCandidateSourceSnapshotLocalObjectSource,
  RealCandidateSourceSnapshotFilebaseTransport,
  type CandidateSourceSnapshotLocalObjectSource,
  type CandidateSourceSnapshotS3CommandExecutor,
} from "./candidate-source-snapshot-filebase.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE_PARQUET,
  createCandidateSourceSnapshotExactUploadBinding,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";
import {
  candidateSourceSnapshotPrefixes,
  materializeCandidateSourceSnapshotControlArtifacts,
  type CandidateSourceSnapshotUploadRecord,
} from "./candidate-source-snapshot-controls.js";
import {
  assertCandidateSourceSnapshotIpnsAdmission,
  executeCandidateSourceSnapshotUploads,
  type CandidateSourceSnapshotUploadSummary,
} from "./candidate-source-snapshot-upload.js";

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

export interface CandidateSourceSnapshotExecutionBundle {
  build: CandidateSourceSnapshotBuildResult;
  createObjects: () => AsyncIterable<CandidateSourceSnapshotUploadObject>;
  localSource: CandidateSourceSnapshotLocalObjectSource;
}

/** Reconstructs the exact immutable inventory and binds every item to one local file. */
export async function prepareCandidateSourceSnapshotExecutionBundle(
  descriptor: CandidateSourceSnapshotBuildDescriptor,
): Promise<CandidateSourceSnapshotExecutionBundle> {
  const build = await buildCandidateSourceSnapshotDemo({
    descriptor,
    record: false,
  });
  const plan = build.plan;
  const prefixes = candidateSourceSnapshotPrefixes(plan.namespaceId);
  const controlRoot = path.join(
    path.resolve(descriptor.controlOutputRoot),
    plan.namespaceId,
  );
  const controls = await materializeCandidateSourceSnapshotControlArtifacts({
    compactManifest: CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST,
    expectedSourceManifestFileSha256:
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
    expectedSourcePlanFileSha256:
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
    expectedSourceQueryTable: CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE_PARQUET,
    namespaceId: plan.namespaceId,
    outputRoot: controlRoot,
    prefixes,
    sourceManifestPath: descriptor.sourceManifestPath,
    sourcePlanPath: descriptor.sourcePlanPath,
  });
  const exact = createCandidateSourceSnapshotExactUploadBinding({
    plan,
    planArtifact: build.planArtifact,
  });
  const createObjects =
    (): AsyncIterable<CandidateSourceSnapshotUploadObject> =>
      (async function* () {
        let count = 0;
        let bytes = 0;
        for await (const record of controls.createUploadRecords()) {
          const object = uploadObject(record);
          count += 1;
          bytes += object.byteSize;
          yield object;
        }
        const planArtifact = {
          ...build.planArtifact,
          domain: "open_data" as const,
        };
        count += 1;
        bytes += planArtifact.byteSize;
        yield planArtifact;
        if (
          count !== exact.exactObjectCount ||
          bytes !== exact.exactTotalBytes
        ) {
          throw new Error(
            "Reconstructed candidate inventory differs from its exact binding",
          );
        }
      })();
  const localSource =
    await BoundCandidateSourceSnapshotLocalObjectSource.create({
      controlRoot,
      plan,
      planArtifactPath: build.planArtifactObjectPath,
      sourcePlanPath: descriptor.sourcePlanPath,
    });
  return { build, createObjects, localSource };
}

export type CandidateSourceSnapshotCutoverResult =
  "verified" | "prior_confirmed_failure" | "ambiguous" | "unexpected_cid";

export interface CandidateSourceSnapshotIpnsCutoverBoundary {
  mutateAndVerify(
    domain: "open_data" | "query_table",
    requestedCid: string,
  ): Promise<CandidateSourceSnapshotCutoverResult>;
  rollbackAndVerify(
    domain: "open_data" | "query_table",
    priorCid: string,
  ): Promise<CandidateSourceSnapshotCutoverResult>;
}

/**
 * Closed cutover ordering for Session 2. Ambiguous/unexpected observations are
 * never overwritten. A definite second-domain failure rolls back the already
 * verified first domain in reverse order.
 */
export async function executeCandidateSourceSnapshotIpnsCutover(input: {
  boundary: CandidateSourceSnapshotIpnsCutoverBoundary;
  intents: readonly {
    domain: "open_data" | "query_table";
    planId: string;
    planSha256: string;
    state: "intent_recorded" | "prior_confirmed";
  }[];
  plan: CandidateSourceSnapshotDemoPlan;
  unverifiedObjectCount: number;
}): Promise<{
  openData: CandidateSourceSnapshotCutoverResult;
  queryTable: CandidateSourceSnapshotCutoverResult | "not_attempted";
}> {
  assertCandidateSourceSnapshotIpnsAdmission(input);
  const openData = await input.boundary.mutateAndVerify(
    "open_data",
    input.plan.targets.openData.targetCid,
  );
  if (openData !== "verified") {
    return { openData, queryTable: "not_attempted" };
  }
  const queryTable = await input.boundary.mutateAndVerify(
    "query_table",
    input.plan.targets.queryTable.targetCid,
  );
  if (queryTable === "prior_confirmed_failure") {
    const rollback = await input.boundary.rollbackAndVerify(
      "open_data",
      input.plan.targets.openData.priorCid,
    );
    if (rollback !== "verified") {
      throw new Error(
        "Candidate reverse rollback did not verify the prior CID",
      );
    }
  }
  // Ambiguous and unexpected-CID states intentionally receive no rollback or
  // follow-up mutation; durable recovery must classify them first.
  return { openData, queryTable };
}

export type CandidateSourceSnapshotSession2Result =
  | {
      planId: string;
      planSha256: string;
      status: "executor_disabled";
    }
  | {
      durableState: CandidateSourceSnapshotDurableState;
      planId: string;
      planSha256: string;
      status: "uploads_verified_ipns_requires_exact_intents";
      summary: CandidateSourceSnapshotUploadSummary;
    };

/**
 * Production-shaped entry point. It reloads the exact durable plan and refuses
 * to arm transport unless durable approval/execution admission already exists.
 * IPNS remains a separate exact-intent cutover through the function above.
 */
export async function executeCandidateSourceSnapshotSession2(input: {
  databaseUrl: string;
  descriptor: CandidateSourceSnapshotBuildDescriptor;
  environment: NodeJS.ProcessEnv;
  s3Executor?: CandidateSourceSnapshotS3CommandExecutor;
}): Promise<CandidateSourceSnapshotSession2Result> {
  const bundle = await prepareCandidateSourceSnapshotExecutionBundle(
    input.descriptor,
  );
  const identity = {
    planId: bundle.build.plan.planId,
    planSha256: bundle.build.plan.planSha256,
  };
  const config = loadCandidateSourceSnapshotExecutionConfig(
    input.environment,
    bundle.build.plan,
  );
  if (!config.enabled) {
    return { ...identity, status: "executor_disabled" };
  }
  const durable = await loadCandidateSourceSnapshotDemoPlan(
    input.databaseUrl,
    identity,
  );
  if (
    durable.plan.planId !== bundle.build.plan.planId ||
    durable.plan.planSha256 !== bundle.build.plan.planSha256 ||
    durable.exactUpload.exactObjectCount !== bundle.build.exactObjectCount ||
    durable.exactUpload.exactTotalBytes !== bundle.build.exactTotalBytes
  ) {
    throw new Error(
      "Durable candidate plan differs from local immutable inputs",
    );
  }
  const admission = await loadCandidateSourceSnapshotDemoExecutionAdmission(
    input.databaseUrl,
    {
      approvalId: config.approvalId,
      ...identity,
    },
  );
  if (
    admission.state.state !== "executing" ||
    admission.state.approvalCount !== 1 ||
    admission.approval.approvalId !== config.approvalId ||
    admission.capacityConfirmation.confirmedPlanName !==
      "Filebase Pro or better" ||
    admission.plan.planId !== durable.plan.planId ||
    admission.plan.planSha256 !== durable.plan.planSha256 ||
    admission.exactUpload.exactObjectCount !==
      durable.exactUpload.exactObjectCount ||
    admission.exactUpload.exactTotalBytes !==
      durable.exactUpload.exactTotalBytes
  ) {
    throw new Error(
      "Candidate execution requires the exact durable approval and executing state",
    );
  }
  const journal = new PostgresCandidateSourceSnapshotUploadJournal(
    input.databaseUrl,
  );
  const transport = new RealCandidateSourceSnapshotFilebaseTransport({
    config,
    ...(input.s3Executor ? { executor: input.s3Executor } : {}),
    source: bundle.localSource,
  });
  const summary = await executeCandidateSourceSnapshotUploads({
    backoff: async (attemptSequence) => {
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(1_000, attemptSequence * 250)),
      );
    },
    executorEnabled: true,
    journal,
    objects: bundle.createObjects(),
    plan: admission.plan,
    transport,
    verifyLocalObject: async (object) =>
      await bundle.localSource.verify(object),
  });
  return {
    durableState: admission.state,
    ...identity,
    status: "uploads_verified_ipns_requires_exact_intents",
    summary,
  };
}
