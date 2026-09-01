import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableConflictError } from "../lib/durability-errors.js";
import {
  PostgresCandidateSourceSnapshotUploadJournal,
  type CandidateSourceSnapshotUploadJournalLeaseBinding,
} from "../db/candidate-source-snapshot-demo.js";
import {
  acquireCandidateSourceSnapshotExecutorLease,
  candidateSourceSnapshotUploadContinuationAuthorizationSchema,
  candidateSourceSnapshotUploadResumeAuthorizationSchema,
  candidateSourceSnapshotUploadReconciliationComplete,
  heartbeatCandidateSourceSnapshotExecutorLease,
  listCandidateSourceSnapshotUploadContinuationUncertainties,
  loadCandidateSourceSnapshotUploadExecutionPermit,
  recordCandidateSourceSnapshotUploadContinuation,
  recordCandidateSourceSnapshotUploadResumeAuthorization,
  recordCandidateSourceSnapshotUploadReconciliation,
  releaseCandidateSourceSnapshotExecutorLease,
  transitionCandidateSourceSnapshotExecutorLease,
  type CandidateSourceSnapshotExecutorLease,
  type CandidateSourceSnapshotUploadContinuationAuthorization,
  type CandidateSourceSnapshotUploadResumeAuthorization,
} from "../db/candidate-source-snapshot-upload-continuation.js";
import type { EnabledCandidateSourceSnapshotExecutionConfig } from "./candidate-source-snapshot-executor-config.js";
import {
  RealCandidateSourceSnapshotFilebaseTransport,
  type CandidateSourceSnapshotLocalObjectSource,
  type CandidateSourceSnapshotS3CommandExecutor,
  type CandidateSourceSnapshotTransportLimits,
} from "./candidate-source-snapshot-filebase.js";
import type {
  CandidateSourceSnapshotDemoPlan,
  CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";
import {
  CandidateSourceSnapshotUploadError,
  executeCandidateSourceSnapshotUploads,
  type CandidateSourceSnapshotInspectionResult,
  type CandidateSourceSnapshotUploadAttempt,
  type CandidateSourceSnapshotUploadSummary,
  type CandidateSourceSnapshotUploadTransport,
} from "./candidate-source-snapshot-upload.js";

const LEASE_DURATION_MS = 240_000;
const LEASE_HEARTBEAT_MS = 60_000;

function objectIdentity(input: {
  domain: "open_data" | "query_table";
  remoteObjectKey: string;
}): string {
  return `${input.domain}\u001f${input.remoteObjectKey}`;
}

function leaseWindow(now: Date): { expiresAt: string; heartbeatAt: string } {
  return {
    expiresAt: new Date(now.getTime() + LEASE_DURATION_MS).toISOString(),
    heartbeatAt: now.toISOString(),
  };
}

function failClosedInspectionResult(input: {
  attempt: CandidateSourceSnapshotUploadAttempt;
  error: CandidateSourceSnapshotUploadError;
}): CandidateSourceSnapshotInspectionResult {
  return {
    outcome: "ambiguous",
    receiptSha256: canonicalJsonSha256({
      evidenceSha256: input.error.evidence.evidenceSha256,
      inspectionAttemptId: input.attempt.attemptId,
      outcome: "ambiguous",
      schemaVersion:
        "candidate-source-snapshot-continuation-inspection-failure-v1",
    }),
  };
}

export interface CandidateSourceSnapshotUploadContinuationRuntimeDependencies {
  acquireLease: typeof acquireCandidateSourceSnapshotExecutorLease;
  executeUploads: typeof executeCandidateSourceSnapshotUploads;
  heartbeatLease: typeof heartbeatCandidateSourceSnapshotExecutorLease;
  listUncertainties: typeof listCandidateSourceSnapshotUploadContinuationUncertainties;
  loadExecutionPermit: typeof loadCandidateSourceSnapshotUploadExecutionPermit;
  now(): Date;
  reconciliationComplete: typeof candidateSourceSnapshotUploadReconciliationComplete;
  recordAuthorization: typeof recordCandidateSourceSnapshotUploadContinuation;
  recordResumeAuthorization: typeof recordCandidateSourceSnapshotUploadResumeAuthorization;
  recordReconciliation: typeof recordCandidateSourceSnapshotUploadReconciliation;
  releaseLease: typeof releaseCandidateSourceSnapshotExecutorLease;
  transitionLease: typeof transitionCandidateSourceSnapshotExecutorLease;
  transportFactory(input: {
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    executor?: CandidateSourceSnapshotS3CommandExecutor;
    source: CandidateSourceSnapshotLocalObjectSource;
    transportLimits: CandidateSourceSnapshotTransportLimits;
  }): CandidateSourceSnapshotUploadTransport;
}

const defaultDependencies: CandidateSourceSnapshotUploadContinuationRuntimeDependencies =
  {
    acquireLease: acquireCandidateSourceSnapshotExecutorLease,
    executeUploads: executeCandidateSourceSnapshotUploads,
    heartbeatLease: heartbeatCandidateSourceSnapshotExecutorLease,
    listUncertainties:
      listCandidateSourceSnapshotUploadContinuationUncertainties,
    loadExecutionPermit: loadCandidateSourceSnapshotUploadExecutionPermit,
    now: () => new Date(),
    reconciliationComplete: candidateSourceSnapshotUploadReconciliationComplete,
    recordAuthorization: recordCandidateSourceSnapshotUploadContinuation,
    recordResumeAuthorization:
      recordCandidateSourceSnapshotUploadResumeAuthorization,
    recordReconciliation: recordCandidateSourceSnapshotUploadReconciliation,
    releaseLease: releaseCandidateSourceSnapshotExecutorLease,
    transitionLease: transitionCandidateSourceSnapshotExecutorLease,
    transportFactory: ({ config, executor, source, transportLimits }) =>
      new RealCandidateSourceSnapshotFilebaseTransport({
        config,
        ...(executor ? { executor } : {}),
        source,
        transportLimits,
      }),
  };

async function collectUncertainObjects(input: {
  objects: AsyncIterable<CandidateSourceSnapshotUploadObject>;
  uncertainties: readonly {
    domain: "open_data" | "query_table";
    remoteObjectKey: string;
  }[];
}): Promise<Map<string, CandidateSourceSnapshotUploadObject>> {
  const required = new Set(input.uncertainties.map(objectIdentity));
  const selected = new Map<string, CandidateSourceSnapshotUploadObject>();
  for await (const object of input.objects) {
    const identity = objectIdentity(object);
    if (!required.has(identity)) continue;
    if (selected.has(identity)) {
      throw new DurableConflictError(
        "Candidate source-snapshot continuation inventory is duplicated",
      );
    }
    selected.set(identity, object);
  }
  if (selected.size !== required.size) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation inventory is incomplete",
    );
  }
  return selected;
}

async function reconcileUncertainObjects(input: {
  authorization: CandidateSourceSnapshotUploadContinuationAuthorization;
  createObjects(): AsyncIterable<CandidateSourceSnapshotUploadObject>;
  databaseUrl: string;
  holderToken: string;
  journal: PostgresCandidateSourceSnapshotUploadJournal;
  lease: CandidateSourceSnapshotExecutorLease;
  localSource: CandidateSourceSnapshotLocalObjectSource;
  plan: CandidateSourceSnapshotDemoPlan;
  signal: AbortSignal;
  transport: CandidateSourceSnapshotUploadTransport;
  dependencies: CandidateSourceSnapshotUploadContinuationRuntimeDependencies;
}): Promise<void> {
  const uncertainties = await input.dependencies.listUncertainties(
    input.databaseUrl,
    input.authorization.authorizationId,
  );
  if (
    uncertainties.length !==
    input.authorization.authorizationBinding.checkpoint.uncertainObjectCount
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation uncertainty count drifted",
    );
  }
  const objects = await collectUncertainObjects({
    objects: input.createObjects(),
    uncertainties,
  });
  for (const uncertainty of uncertainties) {
    if (input.signal.aborted) {
      throw input.signal.reason ?? new Error("Continuation lease was lost");
    }
    const object = objects.get(objectIdentity(uncertainty));
    if (
      !object ||
      object.byteSize !== uncertainty.expectedBytes ||
      object.expectedCid !== uncertainty.expectedCid ||
      object.sha256 !== uncertainty.expectedSha256
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot uncertain object binding drifted",
      );
    }
    await input.localSource.verify(object);
    let attempts = await input.journal.listAttempts(input.plan, object);
    let sourceAttempt = attempts.find(
      (attempt) => attempt.attemptId === uncertainty.sourceAttemptId,
    );
    if (
      !sourceAttempt ||
      sourceAttempt.requestId !== uncertainty.sourceRequestId
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot uncertainty lost its source attempt",
      );
    }
    if (sourceAttempt.outcome === "request_started") {
      await input.journal.markInterruptedAttemptUnknown(
        input.plan,
        object,
        sourceAttempt,
      );
      attempts = await input.journal.listAttempts(input.plan, object);
      sourceAttempt = attempts.find(
        (attempt) => attempt.attemptId === uncertainty.sourceAttemptId,
      );
    }
    if (
      !sourceAttempt ||
      ![
        "connection_failure",
        "retryable_http_error",
        "timeout_unknown",
      ].includes(sourceAttempt.outcome)
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot uncertainty is not safely inspectable",
      );
    }
    const admission = await input.journal.startInspection(
      input.plan,
      object,
      sourceAttempt,
    );
    let result = admission.replayedResult;
    if (!result) {
      try {
        result = await input.transport.inspectExistingOnce(
          input.plan,
          object,
          input.signal,
        );
      } catch (error) {
        if (!(error instanceof CandidateSourceSnapshotUploadError)) throw error;
        result = failClosedInspectionResult({
          attempt: admission.attempt,
          error,
        });
      }
    }
    const checkpoint = await input.journal.recordInspectionResult(
      input.plan,
      object,
      admission.attempt,
      result,
    );
    if (result.outcome === "mismatch") {
      throw new DurableConflictError(
        "Candidate source-snapshot remote object conflicts with its immutable binding",
      );
    }
    if (result.outcome === "ambiguous") {
      throw new DurableConflictError(
        "Candidate source-snapshot remote object remains ambiguous",
      );
    }
    const reconciliationResult =
      result.outcome === "verified"
        ? ("remote_verified" as const)
        : ("conclusively_absent" as const);
    if (
      (reconciliationResult === "remote_verified" &&
        checkpoint.status !== "verified") ||
      (reconciliationResult === "conclusively_absent" &&
        checkpoint.status !== "admitted")
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot reconciliation checkpoint is inconsistent",
      );
    }
    await input.dependencies.recordReconciliation(input.databaseUrl, {
      authorizationId: input.authorization.authorizationId,
      domain: object.domain,
      executorLeaseId: input.lease.leaseId,
      holderToken: input.holderToken,
      inspectionId: admission.attempt.attemptId,
      planId: input.plan.planId,
      leaseGeneration: input.lease.leaseGeneration,
      receiptSha256: result.receiptSha256,
      recordedAt: input.dependencies.now().toISOString(),
      remoteObjectKey: object.remoteObjectKey,
      result: reconciliationResult,
    });
  }
  if (
    !(await input.dependencies.reconciliationComplete(
      input.databaseUrl,
      input.authorization.authorizationId,
    ))
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation reconciliation is incomplete",
    );
  }
}

/**
 * The sole continuation upload path. The caller has already replayed the
 * immutable primary approval; this function adds no IPNS or completion effect.
 */
export async function executeCandidateSourceSnapshotUploadContinuation(input: {
  afterUploadsVerified?: (
    summary: CandidateSourceSnapshotUploadSummary,
  ) => Promise<void>;
  authorization: CandidateSourceSnapshotUploadContinuationAuthorization;
  config: EnabledCandidateSourceSnapshotExecutionConfig;
  createObjects(): AsyncIterable<CandidateSourceSnapshotUploadObject>;
  databaseUrl: string;
  dependencies?: Partial<CandidateSourceSnapshotUploadContinuationRuntimeDependencies>;
  holderToken: string;
  localSource: CandidateSourceSnapshotLocalObjectSource;
  plan: CandidateSourceSnapshotDemoPlan;
  resumeAuthorization?: CandidateSourceSnapshotUploadResumeAuthorization;
  s3Executor?: CandidateSourceSnapshotS3CommandExecutor;
}): Promise<CandidateSourceSnapshotUploadSummary> {
  const dependencies = { ...defaultDependencies, ...input.dependencies };
  const authorization =
    candidateSourceSnapshotUploadContinuationAuthorizationSchema.parse(
      input.authorization,
    );
  if (
    authorization.authorizationBinding.plan.planId !== input.plan.planId ||
    authorization.authorizationBinding.plan.planSha256 !== input.plan.planSha256
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation plan binding differs",
    );
  }
  const resumeAuthorization = input.resumeAuthorization
    ? candidateSourceSnapshotUploadResumeAuthorizationSchema.parse(
        input.resumeAuthorization,
      )
    : undefined;
  if (
    resumeAuthorization &&
    (resumeAuthorization.authorizationBinding.plan.planId !==
      input.plan.planId ||
      resumeAuthorization.authorizationBinding.plan.planSha256 !==
        input.plan.planSha256 ||
      resumeAuthorization.authorizationBinding.predecessor.authorizationId !==
        authorization.authorizationId ||
      resumeAuthorization.authorizationBinding.predecessor
        .authorizationSha256 !== authorization.authorizationSha256)
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot upload resume binding differs",
    );
  }
  const recorded = await dependencies.recordAuthorization(
    input.databaseUrl,
    authorization,
  );
  if (recorded.authorizationSha256 !== authorization.authorizationSha256) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation authorization replay differs",
    );
  }
  if (resumeAuthorization) {
    const recordedResume = await dependencies.recordResumeAuthorization(
      input.databaseUrl,
      resumeAuthorization,
    );
    if (
      recordedResume.authorizationSha256 !==
      resumeAuthorization.authorizationSha256
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot upload resume authorization replay differs",
      );
    }
  }
  const acquiredAt = dependencies.now();
  let lease = await dependencies.acquireLease(input.databaseUrl, {
    acquiredAt: acquiredAt.toISOString(),
    authorizationId: authorization.authorizationId,
    expiresAt: new Date(acquiredAt.getTime() + LEASE_DURATION_MS).toISOString(),
    holderToken: input.holderToken,
    ...(resumeAuthorization
      ? {
          leaseGeneration:
            resumeAuthorization.authorizationBinding.lease
              .resumeLeaseGeneration,
          persistentExecutorEnabled: false as const,
          resumeAuthorizationId: resumeAuthorization.authorizationId,
        }
      : {}),
  });
  const leaseBinding: CandidateSourceSnapshotUploadJournalLeaseBinding = {
    authorizationId: authorization.authorizationId,
    leaseGeneration: lease.leaseGeneration,
    leaseId: lease.leaseId,
    ...(resumeAuthorization
      ? { resumeAuthorizationId: resumeAuthorization.authorizationId }
      : {}),
  };
  const journal = new PostgresCandidateSourceSnapshotUploadJournal(
    input.databaseUrl,
    leaseBinding,
  );
  const binding = authorization.authorizationBinding.execution;
  const transport = dependencies.transportFactory({
    config: input.config,
    ...(input.s3Executor ? { executor: input.s3Executor } : {}),
    source: input.localSource,
    transportLimits: {
      connectionTimeoutMs: binding.connectionTimeoutMs,
      maxSockets:
        lease.phase === "reconciling" ? 4 : lease.effectiveConcurrency,
      requestTimeoutMs: binding.requestTimeoutMs,
      socketTimeoutMs: binding.socketTimeoutMs,
    },
  });
  let leaseQueue = Promise.resolve();
  let leaseFailure: unknown;
  const abort = new AbortController();
  const queueLeaseOperation = async (
    operation: () => Promise<CandidateSourceSnapshotExecutorLease>,
  ): Promise<CandidateSourceSnapshotExecutorLease> => {
    const queued = leaseQueue.then(operation);
    leaseQueue = queued.then(
      () => undefined,
      () => undefined,
    );
    try {
      lease = await queued;
      return lease;
    } catch (error) {
      leaseFailure = error;
      abort.abort(error);
      throw error;
    }
  };
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  try {
    heartbeat = setInterval(() => {
      const window = leaseWindow(dependencies.now());
      void queueLeaseOperation(
        async () =>
          await dependencies.heartbeatLease(input.databaseUrl, {
            ...window,
            holderToken: input.holderToken,
            leaseGeneration: lease.leaseGeneration,
            leaseId: lease.leaseId,
          }),
      ).catch(() => undefined);
    }, LEASE_HEARTBEAT_MS);
    heartbeat.unref?.();
    if (lease.phase === "reconciling") {
      await reconcileUncertainObjects({
        authorization,
        createObjects: input.createObjects,
        databaseUrl: input.databaseUrl,
        dependencies,
        holderToken: input.holderToken,
        journal,
        lease,
        localSource: input.localSource,
        plan: input.plan,
        signal: abort.signal,
        transport,
      });
      const window = leaseWindow(dependencies.now());
      await queueLeaseOperation(
        async () =>
          await dependencies.transitionLease(input.databaseUrl, {
            ...window,
            holderToken: input.holderToken,
            leaseGeneration: lease.leaseGeneration,
            leaseId: lease.leaseId,
            nextPhase: "upload_4",
            revision: lease.revision,
          }),
      );
    }
    if (
      !(["upload_4", "upload_8", "upload_16"] as const).includes(
        lease.phase as "upload_4" | "upload_8" | "upload_16",
      )
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot executor lease is not upload-capable",
      );
    }
    const permit = await dependencies.loadExecutionPermit(input.databaseUrl, {
      holderToken: input.holderToken,
      leaseGeneration: lease.leaseGeneration,
      leaseId: lease.leaseId,
      planId: input.plan.planId,
      planSha256: input.plan.planSha256,
    });
    transport.setMaxSockets?.(permit.effectiveConcurrency);
    const summary = await dependencies.executeUploads({
      backoff: async (attemptSequence) => {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(1_000, attemptSequence * 250)),
        );
      },
      executionPermit: {
        concurrencyStages: [4, 8, 16],
        initialConcurrency: permit.effectiveConcurrency,
        promotionVerifiedObjectsPerStage:
          binding.promotionVerifiedObjectsPerStage,
        requestTimeoutMs: permit.requestTimeoutMs,
      },
      executorEnabled: true,
      journal,
      objects: input.createObjects(),
      onPromote: async (nextStage) => {
        const window = leaseWindow(dependencies.now());
        const nextPhase = nextStage === 8 ? "upload_8" : "upload_16";
        await queueLeaseOperation(
          async () =>
            await dependencies.transitionLease(input.databaseUrl, {
              ...window,
              holderToken: input.holderToken,
              leaseGeneration: lease.leaseGeneration,
              leaseId: lease.leaseId,
              nextPhase,
              revision: lease.revision,
            }),
        );
        transport.setMaxSockets?.(nextStage);
      },
      plan: input.plan,
      signal: abort.signal,
      transport,
      verifyLocalObject: async (object) =>
        await input.localSource.verify(object),
    });
    await leaseQueue;
    if (leaseFailure !== undefined) throw leaseFailure;
    if (input.afterUploadsVerified) {
      await input.afterUploadsVerified(summary);
      const releasedAt = leaseWindow(dependencies.now());
      await queueLeaseOperation(
        async () =>
          await dependencies.releaseLease(input.databaseUrl, {
            ...releasedAt,
            holderToken: input.holderToken,
            leaseGeneration: lease.leaseGeneration,
            leaseId: lease.leaseId,
            revision: lease.revision,
          }),
      );
    }
    return summary;
  } finally {
    if (heartbeat !== undefined) clearInterval(heartbeat);
    await transport.close?.();
  }
}
