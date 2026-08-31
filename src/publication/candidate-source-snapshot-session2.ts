import path from "node:path";

import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  loadCandidateSourceSnapshotDemoPlan,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotIpnsRetryAuthorization,
  recordCandidateSourceSnapshotDemoPlan,
} from "../db/candidate-source-snapshot-demo.js";
import {
  completeCandidateSourceSnapshotDemoPlan,
  loadCompletedCandidateSourceSnapshotDemoReplay,
  recordCandidateSourceSnapshotUploadClosure,
  type CandidateSourceSnapshotUploadClosure,
} from "../db/candidate-source-snapshot-completion.js";
import { createCandidateSourceSnapshotApprovalIdentity } from "../db/candidate-source-snapshot-approval.js";
import {
  recordCandidateSourceSnapshotPreflightContinuation,
  type CandidateSourceSnapshotPreflightContinuationAuthorization,
} from "../db/candidate-source-snapshot-preflight-continuation.js";
import type { CandidateSourceSnapshotUploadContinuationAuthorization } from "../db/candidate-source-snapshot-upload-continuation.js";
import { recordCompatibleCandidateSourceSnapshotPlanDerivation } from "../db/candidate-source-snapshot-plan-derivation.js";
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
  executeCandidateSourceSnapshotUploads,
  type CandidateSourceSnapshotUploadSummary,
  type CandidateSourceSnapshotUploadTransport,
} from "./candidate-source-snapshot-upload.js";
import { executeCandidateSourceSnapshotUploadContinuation } from "./candidate-source-snapshot-upload-continuation-runtime.js";
import {
  executeCandidateSourceSnapshotIpnsController,
  type CandidateSourceSnapshotIpnsBoundary,
  type CandidateSourceSnapshotIpnsControllerResult,
  type CandidateSourceSnapshotIpnsIntent,
  type CandidateSourceSnapshotIpnsJournal,
  type CandidateSourceSnapshotIpnsReplayAuthorization,
  type CandidateSourceSnapshotIpnsRollbackAuthorization,
} from "./candidate-source-snapshot-ipns-controller.js";
import { candidateSourceSnapshotRemoteRuntimeFactory } from "./candidate-source-snapshot-remote-runtime.js";
import type { EnabledCandidateSourceSnapshotExecutionConfig } from "./candidate-source-snapshot-executor-config.js";

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
 * Fail-closed compatibility shim. Session 2 IPNS effects are reachable only
 * through executeCandidateSourceSnapshotSession2 and its durable controller.
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
  void input;
  throw new Error(
    "Legacy candidate IPNS cutover is disabled; use the durable Session 2 executor",
  );
}

export type CandidateSourceSnapshotSession2Result =
  | {
      planId: string;
      planSha256: string;
      status: "executor_disabled";
    }
  | {
      cutover: CandidateSourceSnapshotIpnsControllerResult;
      executorEnabled: false;
      planId: string;
      planSha256: string;
      status: "recovery_required";
      summary: CandidateSourceSnapshotUploadSummary;
      uploadClosure: CandidateSourceSnapshotUploadClosure;
    }
  | {
      completedRevision: number;
      cutover: CandidateSourceSnapshotIpnsControllerResult;
      executorEnabled: false;
      planId: string;
      planSha256: string;
      status: "completed";
      summary: CandidateSourceSnapshotUploadSummary;
      uploadClosure: CandidateSourceSnapshotUploadClosure;
    };

export interface CandidateSourceSnapshotSession2Authorization {
  approvedAt: string;
  approverReference: string;
  authorizationStatement: string;
  confirmedAt: string;
  confirmedPlanName: "Filebase Pro" | "Filebase Pro or better";
  confirmerReference: string;
  intendedAt: string;
  implementationCommitSha: string;
  preflightContinuationAuthorization?: CandidateSourceSnapshotPreflightContinuationAuthorization;
  replayAuthorizations?: readonly CandidateSourceSnapshotIpnsReplayAuthorization[];
  rollbackAuthorization?: CandidateSourceSnapshotIpnsRollbackAuthorization;
  uploadContinuationAuthorization?: CandidateSourceSnapshotUploadContinuationAuthorization;
}

/**
 * Network-shaped dependencies are created only after the process-scoped
 * executor flag, exact plan, exact authorization bytes, and local capacity
 * record have all been validated. This interface intentionally contains no
 * default/mock implementation: the production composition root must provide
 * the durable request gate, evidence sink, and closed Filebase/IPNS boundary.
 */
export interface CandidateSourceSnapshotSession2RemoteRuntime {
  readonly boundary: CandidateSourceSnapshotIpnsBoundary;
  readonly journal: CandidateSourceSnapshotIpnsJournal;
  close(): Promise<void>;
  prepareIntents(input: {
    createInitialIntents: () => ReturnType<
      typeof createCandidateSourceSnapshotDemoIpnsIntents
    >;
    intendedAt: string;
    plan: CandidateSourceSnapshotDemoPlan;
    rollbackAuthorization?: CandidateSourceSnapshotIpnsRollbackAuthorization;
    uploadClosure: CandidateSourceSnapshotUploadClosure;
  }): Promise<readonly CandidateSourceSnapshotIpnsIntent[]>;
  readOnlyPreflight(input?: {
    continuationAuthorization?: CandidateSourceSnapshotPreflightContinuationAuthorization;
  }): Promise<void>;
  recordFinalVerification(input: {
    approvalId: string;
    localSource: CandidateSourceSnapshotLocalObjectSource;
    plan: CandidateSourceSnapshotDemoPlan;
    uploadClosure: CandidateSourceSnapshotUploadClosure;
  }): Promise<void>;
}

export type CandidateSourceSnapshotSession2RemoteRuntimeFactory = (input: {
  config: EnabledCandidateSourceSnapshotExecutionConfig;
  databaseUrl: string;
  plan: CandidateSourceSnapshotDemoPlan;
}) => CandidateSourceSnapshotSession2RemoteRuntime;

export interface CandidateSourceSnapshotSession2Dependencies {
  approvePlan: typeof approveCandidateSourceSnapshotDemoPlan;
  beginExecution: typeof beginCandidateSourceSnapshotDemoExecution;
  completePlan: typeof completeCandidateSourceSnapshotDemoPlan;
  confirmCapacity: typeof confirmCandidateSourceSnapshotDemoCapacity;
  createIntents: typeof createCandidateSourceSnapshotDemoIpnsIntents;
  executeIpnsController: typeof executeCandidateSourceSnapshotIpnsController;
  executeUploads: typeof executeCandidateSourceSnapshotUploads;
  executeUploadContinuation: typeof executeCandidateSourceSnapshotUploadContinuation;
  loadCompletedReplay: typeof loadCompletedCandidateSourceSnapshotDemoReplay;
  loadPlan: typeof loadCandidateSourceSnapshotDemoPlan;
  prepareBundle: typeof prepareCandidateSourceSnapshotExecutionBundle;
  recordIpnsRetryAuthorization: typeof recordCandidateSourceSnapshotIpnsRetryAuthorization;
  recordPlan: typeof recordCandidateSourceSnapshotDemoPlan;
  recordPlanDerivation: typeof recordCompatibleCandidateSourceSnapshotPlanDerivation;
  recordPreflightContinuation: typeof recordCandidateSourceSnapshotPreflightContinuation;
  recordUploadClosure: typeof recordCandidateSourceSnapshotUploadClosure;
  remoteRuntimeFactory?: CandidateSourceSnapshotSession2RemoteRuntimeFactory;
  uploadTransportFactory: (input: {
    bundle: CandidateSourceSnapshotExecutionBundle;
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    journal: PostgresCandidateSourceSnapshotUploadJournal;
    s3Executor?: CandidateSourceSnapshotS3CommandExecutor;
  }) => CandidateSourceSnapshotUploadTransport;
}

const defaultSession2Dependencies: CandidateSourceSnapshotSession2Dependencies =
  {
    approvePlan: approveCandidateSourceSnapshotDemoPlan,
    beginExecution: beginCandidateSourceSnapshotDemoExecution,
    completePlan: completeCandidateSourceSnapshotDemoPlan,
    confirmCapacity: confirmCandidateSourceSnapshotDemoCapacity,
    createIntents: createCandidateSourceSnapshotDemoIpnsIntents,
    executeIpnsController: executeCandidateSourceSnapshotIpnsController,
    executeUploads: executeCandidateSourceSnapshotUploads,
    executeUploadContinuation: executeCandidateSourceSnapshotUploadContinuation,
    loadCompletedReplay: loadCompletedCandidateSourceSnapshotDemoReplay,
    loadPlan: loadCandidateSourceSnapshotDemoPlan,
    prepareBundle: prepareCandidateSourceSnapshotExecutionBundle,
    recordIpnsRetryAuthorization:
      recordCandidateSourceSnapshotIpnsRetryAuthorization,
    recordPlan: recordCandidateSourceSnapshotDemoPlan,
    recordPlanDerivation: recordCompatibleCandidateSourceSnapshotPlanDerivation,
    recordPreflightContinuation:
      recordCandidateSourceSnapshotPreflightContinuation,
    recordUploadClosure: recordCandidateSourceSnapshotUploadClosure,
    remoteRuntimeFactory: candidateSourceSnapshotRemoteRuntimeFactory,
    uploadTransportFactory: ({ bundle, config, s3Executor }) =>
      new RealCandidateSourceSnapshotFilebaseTransport({
        config,
        ...(s3Executor ? { executor: s3Executor } : {}),
        source: bundle.localSource,
      }),
  };

/**
 * One fail-closed production entry point for exact authorization replay,
 * resumable uploads, closed IPNS cutover, remote verification, and completion.
 * A disabled executor returns before either remote adapter factory is called.
 */
export async function executeCandidateSourceSnapshotSession2(input: {
  authorization?: CandidateSourceSnapshotSession2Authorization;
  databaseUrl: string;
  dependencies?: Partial<CandidateSourceSnapshotSession2Dependencies>;
  descriptor: CandidateSourceSnapshotBuildDescriptor;
  environment: NodeJS.ProcessEnv;
  executorLeaseHolderToken?: string;
  s3Executor?: CandidateSourceSnapshotS3CommandExecutor;
}): Promise<CandidateSourceSnapshotSession2Result> {
  // Consume the process-scoped opt-in before any local preparation or config
  // parsing can fail. Only the private copy may authorize this invocation;
  // callers always observe the executor disabled on every exit path.
  const executionEnvironment = { ...input.environment };
  input.environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "false";
  const dependencies: CandidateSourceSnapshotSession2Dependencies = {
    ...defaultSession2Dependencies,
    ...input.dependencies,
  };
  const bundle = await dependencies.prepareBundle(input.descriptor);
  const identity = {
    planId: bundle.build.plan.planId,
    planSha256: bundle.build.plan.planSha256,
  };
  const config = loadCandidateSourceSnapshotExecutionConfig(
    executionEnvironment,
    bundle.build.plan,
  );
  if (!config.enabled) {
    return { ...identity, status: "executor_disabled" };
  }
  if (!input.authorization) {
    throw new Error(
      "Candidate execution requires the exact human authorization input",
    );
  }
  if (!dependencies.remoteRuntimeFactory) {
    throw new Error(
      "Candidate production IPNS runtime is unavailable; no remote effect was attempted",
    );
  }
  const approvalIdentity = createCandidateSourceSnapshotApprovalIdentity({
    approvedAt: input.authorization.approvedAt,
    approverReference: input.authorization.approverReference,
    exactUpload: createCandidateSourceSnapshotExactUploadBinding({
      plan: bundle.build.plan,
      planArtifact: bundle.build.planArtifact,
    }),
    implementationCommitSha: input.authorization.implementationCommitSha,
    plan: bundle.build.plan,
    statement: input.authorization.authorizationStatement,
  });
  if (approvalIdentity.approvalId !== config.approvalId) {
    throw new Error(
      "Configured approval ID does not match the exact authorization bytes",
    );
  }
  const exactUpload = createCandidateSourceSnapshotExactUploadBinding({
    plan: bundle.build.plan,
    planArtifact: bundle.build.planArtifact,
  });
  await dependencies.recordPlan(input.databaseUrl, {
    exactUpload,
    objects: bundle.createObjects(),
    plan: bundle.build.plan,
  });
  await dependencies.recordPlanDerivation(input.databaseUrl, {
    derivedPlanId: bundle.build.plan.planId,
    derivedPlanSha256: bundle.build.plan.planSha256,
  });
  await dependencies.confirmCapacity(input.databaseUrl, {
    confirmedAt: input.authorization.confirmedAt,
    confirmedPlanName: input.authorization.confirmedPlanName,
    confirmerReference: input.authorization.confirmerReference,
    ...identity,
  });
  const completedReplay = await dependencies.loadCompletedReplay(
    input.databaseUrl,
    {
      approvalId: approvalIdentity.approvalId,
      implementationCommitSha: input.authorization.implementationCommitSha,
      ...identity,
    },
  );
  if (completedReplay) {
    return {
      ...completedReplay,
      executorEnabled: false,
      ...identity,
      status: "completed",
    };
  }
  // Persist and exactly replay the human approval while the composition is
  // still local-only. No remote adapter may be constructed before this row is
  // durable and byte-for-byte consistent with the supplied authorization.
  const approval = await dependencies.approvePlan(input.databaseUrl, {
    approvedAt: input.authorization.approvedAt,
    approverReference: input.authorization.approverReference,
    authorizationStatement: input.authorization.authorizationStatement,
    implementationCommitSha: input.authorization.implementationCommitSha,
    ...identity,
  });
  if (
    approval.approvalId !== approvalIdentity.approvalId ||
    approval.approvalSha256 !== approvalIdentity.approvalSha256
  ) {
    throw new Error("Durable approval replay differs from exact authorization");
  }
  const continuationAuthorization = input.authorization
    .preflightContinuationAuthorization
    ? await dependencies.recordPreflightContinuation(
        input.databaseUrl,
        input.authorization.preflightContinuationAuthorization,
      )
    : undefined;
  const uploadContinuationAuthorization =
    input.authorization.uploadContinuationAuthorization;
  if (
    uploadContinuationAuthorization &&
    (!input.executorLeaseHolderToken ||
      input.executorLeaseHolderToken.length < 32 ||
      input.executorLeaseHolderToken.length > 512)
  ) {
    throw new Error(
      "Candidate upload continuation requires one private executor lease token",
    );
  }
  let remote: CandidateSourceSnapshotSession2RemoteRuntime | null = null;
  let uploadTransport: CandidateSourceSnapshotUploadTransport | null = null;
  try {
    if (!uploadContinuationAuthorization) {
      remote = dependencies.remoteRuntimeFactory({
        config,
        databaseUrl: input.databaseUrl,
        plan: bundle.build.plan,
      });
      // This bounded, read-only preflight follows durable approval but precedes
      // execution admission and every upload/mutation. A failure leaves the
      // exact approval available for replay with no write effect admitted.
      await remote.readOnlyPreflight({
        ...(continuationAuthorization ? { continuationAuthorization } : {}),
      });
      await dependencies.beginExecution(input.databaseUrl, {
        approvalId: approval.approvalId,
        ...(continuationAuthorization
          ? {
              continuationAuthorizationId:
                continuationAuthorization.authorizationId,
            }
          : {}),
        executorEnabled: true,
        implementationCommitSha: continuationAuthorization
          ? continuationAuthorization.authorizationBinding
              .amendedImplementationCommitSha
          : input.authorization.implementationCommitSha,
        ...identity,
      });
    }
    const durable = await dependencies.loadPlan(input.databaseUrl, identity);
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
    if (
      durable.state.state !== "executing" ||
      durable.state.approvalCount !== 1 ||
      durable.plan.planId !== bundle.build.plan.planId ||
      durable.plan.planSha256 !== bundle.build.plan.planSha256
    ) {
      throw new Error(
        "Candidate execution requires the exact durable approval and executing state",
      );
    }
    let uploadClosure: CandidateSourceSnapshotUploadClosure | undefined;
    let summary: CandidateSourceSnapshotUploadSummary;
    if (uploadContinuationAuthorization) {
      summary = await dependencies.executeUploadContinuation({
        afterUploadsVerified: async () => {
          uploadClosure = await dependencies.recordUploadClosure(
            input.databaseUrl,
            { approvalId: approval.approvalId, ...identity },
          );
        },
        authorization: uploadContinuationAuthorization,
        config,
        createObjects: bundle.createObjects,
        databaseUrl: input.databaseUrl,
        holderToken: input.executorLeaseHolderToken!,
        localSource: bundle.localSource,
        plan: durable.plan,
        ...(input.s3Executor ? { s3Executor: input.s3Executor } : {}),
      });
      if (!uploadClosure) {
        throw new Error(
          "Candidate upload continuation did not persist upload closure",
        );
      }
      remote = dependencies.remoteRuntimeFactory({
        config,
        databaseUrl: input.databaseUrl,
        plan: bundle.build.plan,
      });
    } else {
      const journal = new PostgresCandidateSourceSnapshotUploadJournal(
        input.databaseUrl,
      );
      uploadTransport = dependencies.uploadTransportFactory({
        bundle,
        config,
        journal,
        ...(input.s3Executor ? { s3Executor: input.s3Executor } : {}),
      });
      summary = await dependencies.executeUploads({
        backoff: async (attemptSequence) => {
          await new Promise((resolve) =>
            setTimeout(resolve, Math.min(1_000, attemptSequence * 250)),
          );
        },
        executorEnabled: true,
        journal,
        objects: bundle.createObjects(),
        plan: durable.plan,
        transport: uploadTransport,
        verifyLocalObject: async (object) =>
          await bundle.localSource.verify(object),
      });
      uploadClosure = await dependencies.recordUploadClosure(
        input.databaseUrl,
        { approvalId: approval.approvalId, ...identity },
      );
    }
    if (!remote) {
      throw new Error("Candidate remote runtime was not initialized");
    }
    // A first execution calls createInitialIntents exactly once; a recovery
    // runtime loads the already-advanced intents instead. In either case both
    // immutable domains must be returned prior-confirmed before the controller
    // may perform its first mutation.
    const intents = await remote.prepareIntents({
      createInitialIntents: async () =>
        await dependencies.createIntents(input.databaseUrl, {
          ...identity,
          intendedAt: input.authorization!.intendedAt,
        }),
      intendedAt: input.authorization.intendedAt,
      plan: durable.plan,
      ...(input.authorization.rollbackAuthorization
        ? { rollbackAuthorization: input.authorization.rollbackAuthorization }
        : {}),
      uploadClosure,
    });
    for (const authorization of input.authorization.replayAuthorizations ??
      []) {
      await dependencies.recordIpnsRetryAuthorization(
        input.databaseUrl,
        durable.plan,
        { authorization, direction: "update" },
      );
    }
    if (input.authorization.rollbackAuthorization) {
      await dependencies.recordIpnsRetryAuthorization(
        input.databaseUrl,
        durable.plan,
        {
          authorization: input.authorization.rollbackAuthorization,
          direction: "rollback",
        },
      );
    }
    const cutover = await dependencies.executeIpnsController({
      approvalId: approval.approvalId,
      boundary: remote.boundary,
      executorEnabled: true,
      intents,
      journal: remote.journal,
      plan: durable.plan,
      replayAuthorizations: input.authorization.replayAuthorizations ?? [],
      ...(input.authorization.rollbackAuthorization
        ? { rollbackAuthorization: input.authorization.rollbackAuthorization }
        : {}),
      uploadClosureId: uploadClosure.closureId,
    });
    if (cutover.status !== "completed") {
      return {
        cutover,
        executorEnabled: false,
        ...identity,
        status: "recovery_required",
        summary,
        uploadClosure,
      };
    }
    await remote.recordFinalVerification({
      approvalId: approval.approvalId,
      localSource: bundle.localSource,
      plan: durable.plan,
      uploadClosure,
    });
    const completed = await dependencies.completePlan(input.databaseUrl, {
      cutover,
      summary,
      uploadClosure,
      ...identity,
    });
    const storedResult = await dependencies.loadCompletedReplay(
      input.databaseUrl,
      {
        approvalId: approval.approvalId,
        implementationCommitSha: input.authorization.implementationCommitSha,
        ...identity,
      },
    );
    if (
      !storedResult ||
      storedResult.completedRevision !== completed.revision
    ) {
      throw new Error(
        "Candidate completion did not replay its exact stored result",
      );
    }
    return {
      ...storedResult,
      executorEnabled: false,
      ...identity,
      status: "completed",
    };
  } finally {
    // The opt-in exists only in this process. Close both independently-owned
    // resources on every exit. The concrete upload transport destroys only
    // the S3 client it constructed; an injected executor remains caller-owned.
    try {
      await uploadTransport?.close?.();
    } finally {
      await remote?.close();
    }
  }
}
