import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  assertCandidateSourceSnapshotObjectNamespace,
  candidateSourceSnapshotRequestCategory,
  candidateSourceSnapshotObjectSchema,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";

export type CandidateSourceSnapshotAttemptOutcome =
  | "absent"
  | "ambiguous"
  | "connection_failure"
  | "inspection_mismatch"
  | "provider_cid_mismatch"
  | "retryable_http_error"
  | "terminal_failure"
  | "timeout_unknown"
  | "verified";

export interface CandidateSourceSnapshotUploadCheckpoint {
  attemptCount: number;
  providerCid: string | null;
  receiptSha256: string | null;
  requestCount: number;
  status:
    "pending" | "admitted" | "outcome_unknown" | "verified" | "failed_terminal";
}

export interface CandidateSourceSnapshotUploadAttempt {
  attemptId: string;
  attemptSequence: number;
  outcome: CandidateSourceSnapshotAttemptOutcome | "request_started";
  operation: "inspect" | "upload";
  recoveryUploadAttemptId: string | null;
  requestId: string;
  /** Database admission time used only to distinguish a live request from crash residue. */
  startedAt?: string;
}

export interface CandidateSourceSnapshotPlanAccounting {
  classAMutationCount: number;
  classBReadCount: number;
  freeOperationCount: number;
  namesApiCount: number;
  publicResolverCount: number;
  requestCostUsd: number;
  requestCount: number;
}

export interface CandidateSourceSnapshotUploadReceipt {
  providerCid: string;
  providerRequestIdHash: string | null;
  receiptSha256: string;
  responseBytes: number;
}

export type CandidateSourceSnapshotInspectionResult =
  | { outcome: "absent" | "ambiguous"; receiptSha256: string }
  | {
      observedBytes: number;
      observedCid: string;
      observedSha256: string;
      outcome: "mismatch" | "verified";
      receiptSha256: string;
    };

export interface CandidateSourceSnapshotUploadJournal {
  admitObject(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint>;
  getCheckpoint(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint>;
  getPlanAccounting(
    plan: CandidateSourceSnapshotDemoPlan,
  ): Promise<CandidateSourceSnapshotPlanAccounting>;
  listAttempts(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<readonly CandidateSourceSnapshotUploadAttempt[]>;
  markInterruptedAttemptUnknown(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
  ): Promise<void>;
  recordAttemptFailure(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome: Exclude<CandidateSourceSnapshotAttemptOutcome, "verified">,
  ): Promise<void>;
  recordInspectionResult(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    result: CandidateSourceSnapshotInspectionResult,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint>;
  recordTerminalFailure(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome: "provider_cid_mismatch" | "terminal_failure",
  ): Promise<void>;
  recordVerified(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    receipt: CandidateSourceSnapshotUploadReceipt,
  ): Promise<void>;
  startAttempt(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attemptSequence: number,
  ): Promise<{
    accounting: CandidateSourceSnapshotPlanAccounting;
    alreadyRecorded: boolean;
    attempt: CandidateSourceSnapshotUploadAttempt;
  }>;
  startInspection(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    recoveryAttempt: CandidateSourceSnapshotUploadAttempt,
  ): Promise<{
    accounting: CandidateSourceSnapshotPlanAccounting;
    attempt: CandidateSourceSnapshotUploadAttempt;
    replayedResult: CandidateSourceSnapshotInspectionResult | null;
  }>;
}

export interface CandidateSourceSnapshotUploadTransport {
  /** Releases transport-owned clients. Injected provider clients remain caller-owned. */
  close?(): Promise<void> | void;
  /** Exactly one bounded Class-B provider read; never mutates the object. */
  inspectExistingOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ): Promise<
    | { outcome: "absent" | "ambiguous"; receiptSha256: string }
    | {
        observedBytes: number;
        observedCid: string;
        observedSha256: string;
        outcome: "mismatch" | "verified";
        receiptSha256: string;
      }
  >;
  /** Exactly one provider request. Retries are owned by the coordinator. */
  uploadOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ): Promise<{
    providerCid: string | null;
    providerRequestIdHash: string | null;
    responseBytes: number;
  }>;
}

export class CandidateSourceSnapshotUploadError extends Error {
  readonly outcome:
    | "connection_failure"
    | "retryable_http_error"
    | "terminal_failure"
    | "timeout_unknown";

  constructor(
    outcome: CandidateSourceSnapshotUploadError["outcome"],
    message = "Candidate source-snapshot upload failed",
  ) {
    super(message);
    this.name = "CandidateSourceSnapshotUploadError";
    this.outcome = outcome;
  }
}

export interface CandidateSourceSnapshotUploadSummary {
  attemptedRequests: number;
  skippedVerified: number;
  recoveredByInspection: number;
  requestCostUsd: number;
  totalObjects: number;
  uploadedAndVerified: number;
}

function objectIdentity(object: CandidateSourceSnapshotUploadObject): string {
  return `${object.domain}:${object.remoteObjectKey}`;
}

async function* asAsyncIterable(
  objects:
    | Iterable<CandidateSourceSnapshotUploadObject>
    | AsyncIterable<CandidateSourceSnapshotUploadObject>,
): AsyncGenerator<CandidateSourceSnapshotUploadObject> {
  for await (const object of objects) yield object;
}

function retryable(outcome: CandidateSourceSnapshotAttemptOutcome): boolean {
  return (
    outcome === "connection_failure" ||
    outcome === "retryable_http_error" ||
    outcome === "timeout_unknown"
  );
}

async function boundedTransportCall<T>(input: {
  call: (signal: AbortSignal) => Promise<T>;
  fatalSignal: AbortSignal;
  timeoutMs: number;
}): Promise<T> {
  if (input.fatalSignal.aborted) {
    throw input.fatalSignal.reason ?? new Error("Candidate upload was aborted");
  }
  const deadline = new AbortController();
  const signal = AbortSignal.any([input.fatalSignal, deadline.signal]);
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      const error = new CandidateSourceSnapshotUploadError("timeout_unknown");
      deadline.abort(error);
      reject(error);
    }, input.timeoutMs);
  });
  try {
    return await Promise.race([input.call(signal), timedOut]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

export async function executeCandidateSourceSnapshotUploads(options: {
  backoff?: (attemptSequence: number) => Promise<void>;
  executorEnabled: true;
  journal: CandidateSourceSnapshotUploadJournal;
  objects:
    | Iterable<CandidateSourceSnapshotUploadObject>
    | AsyncIterable<CandidateSourceSnapshotUploadObject>;
  plan: CandidateSourceSnapshotDemoPlan;
  signal?: AbortSignal;
  transport: CandidateSourceSnapshotUploadTransport;
  verifyLocalObject: (
    object: CandidateSourceSnapshotUploadObject,
  ) => Promise<void>;
}): Promise<CandidateSourceSnapshotUploadSummary> {
  if (options.executorEnabled !== true) {
    throw new Error("Candidate source-snapshot executor is disabled");
  }
  const plan = validateCandidateSourceSnapshotDemoPlan(options.plan);
  const iterator = asAsyncIterable(options.objects)[Symbol.asyncIterator]();
  const fatalController = new AbortController();
  const executionSignal = options.signal
    ? AbortSignal.any([fatalController.signal, options.signal])
    : fatalController.signal;
  let fatalError: unknown;
  const fail = (error: unknown): void => {
    if (fatalError === undefined) {
      fatalError = error;
      fatalController.abort(error);
    }
  };
  const assertRunning = (): void => {
    if (fatalError !== undefined) throw fatalError;
    if (executionSignal.aborted) {
      throw executionSignal.reason ?? new Error("Candidate upload was aborted");
    }
  };
  let iteratorGate: Promise<void> = Promise.resolve();
  const next = async (): Promise<
    IteratorResult<CandidateSourceSnapshotUploadObject>
  > => {
    const result = iteratorGate.then(async () => {
      assertRunning();
      return await iterator.next();
    });
    iteratorGate = result.then(
      () => undefined,
      () => undefined,
    );
    return await result;
  };
  let totalObjects = 0;
  let skippedVerified = 0;
  let uploadedAndVerified = 0;
  let attemptedRequests = 0;
  let recoveredByInspection = 0;
  const seen = new Set<string>();

  const processObject = async (
    rawObject: CandidateSourceSnapshotUploadObject,
  ): Promise<void> => {
    assertRunning();
    const object = candidateSourceSnapshotObjectSchema().parse(rawObject);
    assertCandidateSourceSnapshotObjectNamespace(plan, object);
    const key = objectIdentity(object);
    if (seen.has(key)) {
      throw new Error(
        "Candidate source-snapshot upload inventory is duplicated",
      );
    }
    seen.add(key);
    totalObjects += 1;
    if (totalObjects > plan.inventory.objectCount) {
      throw new Error(
        "Candidate source-snapshot upload inventory exceeds the plan",
      );
    }
    await options.verifyLocalObject(object);
    assertRunning();
    const checkpoint = await options.journal.getCheckpoint(plan, object);
    if (checkpoint.status === "verified") {
      if (
        checkpoint.providerCid !== object.expectedCid ||
        checkpoint.receiptSha256 === null
      ) {
        throw new Error("Verified candidate checkpoint is inconsistent");
      }
      skippedVerified += 1;
      return;
    }
    if (checkpoint.status === "failed_terminal") {
      throw new Error("Candidate source-snapshot object is terminal");
    }
    assertRunning();
    await options.journal.admitObject(plan, object);
    const inspectUnknownUpload = async (
      recoveryAttempt: CandidateSourceSnapshotUploadAttempt,
    ): Promise<"absent" | "verified"> => {
      assertRunning();
      const admitted = await options.journal.startInspection(
        plan,
        object,
        recoveryAttempt,
      );
      validateAccounting(plan, admitted.accounting);
      let inspection: Awaited<
        ReturnType<
          CandidateSourceSnapshotUploadTransport["inspectExistingOnce"]
        >
      >;
      if (admitted.replayedResult) {
        inspection = admitted.replayedResult;
      } else
        try {
          attemptedRequests += 1;
          inspection = await boundedTransportCall({
            call: async (signal) =>
              await options.transport.inspectExistingOnce(plan, object, signal),
            fatalSignal: executionSignal,
            timeoutMs: plan.limits.requestTimeoutMs,
          });
        } catch (error) {
          if (fatalError !== undefined || executionSignal.aborted) throw error;
          if (
            error instanceof CandidateSourceSnapshotUploadError &&
            error.outcome === "timeout_unknown"
          ) {
            inspection = {
              outcome: "ambiguous",
              receiptSha256: canonicalReceiptSha256({
                attemptId: admitted.attempt.attemptId,
                outcome: "timeout_unknown",
                recoveryUploadAttemptId: recoveryAttempt.attemptId,
              }),
            };
          } else {
            throw error;
          }
        }
      const inspectedCheckpoint = admitted.replayedResult
        ? await options.journal.getCheckpoint(plan, object)
        : await options.journal.recordInspectionResult(
            plan,
            object,
            admitted.attempt,
            inspection,
          );
      if (inspection.outcome === "verified") {
        if (
          inspection.observedCid !== object.expectedCid ||
          inspection.observedSha256 !== object.sha256 ||
          inspection.observedBytes !== object.byteSize ||
          inspectedCheckpoint.status !== "verified"
        ) {
          throw new Error("Candidate inspection verification is inconsistent");
        }
        recoveredByInspection += 1;
        return "verified";
      }
      if (inspection.outcome === "mismatch") {
        throw new Error(
          "Candidate remote object conflicts with the immutable plan",
        );
      }
      if (inspection.outcome === "ambiguous") {
        throw new Error("Candidate remote object inspection is ambiguous");
      }
      return "absent";
    };
    const waitForConcurrentUpload = async (
      concurrentAttempt: CandidateSourceSnapshotUploadAttempt,
    ): Promise<CandidateSourceSnapshotUploadAttempt | null> => {
      const admittedAt = concurrentAttempt.startedAt
        ? Date.parse(concurrentAttempt.startedAt)
        : Number.NaN;
      if (!Number.isFinite(admittedAt)) return concurrentAttempt;
      const deadline = admittedAt + plan.limits.requestTimeoutMs;
      for (;;) {
        const concurrentCheckpoint = await options.journal.getCheckpoint(
          plan,
          object,
        );
        if (concurrentCheckpoint.status === "verified") {
          if (
            concurrentCheckpoint.providerCid !== object.expectedCid ||
            concurrentCheckpoint.receiptSha256 === null
          ) {
            throw new Error(
              "Concurrent candidate upload checkpoint is inconsistent",
            );
          }
          skippedVerified += 1;
          return null;
        }
        const replayedAttempt = (
          await options.journal.listAttempts(plan, object)
        ).find(
          (candidate) => candidate.attemptId === concurrentAttempt.attemptId,
        );
        if (!replayedAttempt) {
          throw new Error(
            "Concurrent candidate upload attempt lost its durable identity",
          );
        }
        if (replayedAttempt.outcome !== "request_started") {
          return replayedAttempt;
        }
        if (Date.now() >= deadline) return replayedAttempt;
        await (options.backoff?.(concurrentAttempt.attemptSequence) ??
          new Promise((resolve) => setTimeout(resolve, 25)));
      }
    };
    const priorAttempts = [
      ...(await options.journal.listAttempts(plan, object)),
    ];
    for (const attempt of priorAttempts) {
      if (attempt.outcome === "request_started") {
        const concurrentAttempt = await waitForConcurrentUpload(attempt);
        if (!concurrentAttempt) return;
        Object.assign(attempt, concurrentAttempt);
        if (attempt.outcome === "request_started") {
          await options.journal.markInterruptedAttemptUnknown(
            plan,
            object,
            attempt,
          );
          attempt.outcome = "timeout_unknown";
        }
      }
    }
    const recoveryAttempt = [...priorAttempts]
      .reverse()
      .find(
        (attempt) =>
          attempt.operation === "upload" &&
          attempt.outcome !== "request_started" &&
          retryable(attempt.outcome),
      );
    if (recoveryAttempt) {
      if ((await inspectUnknownUpload(recoveryAttempt)) === "verified") return;
    }
    let attemptsUsed = priorAttempts.filter(
      (attempt) => attempt.operation === "upload",
    ).length;
    const terminal = priorAttempts.find(
      (attempt) =>
        attempt.outcome === "provider_cid_mismatch" ||
        attempt.outcome === "terminal_failure",
    );
    if (terminal)
      throw new Error("Candidate source-snapshot object is terminal");

    for (;;) {
      assertRunning();
      if (attemptsUsed >= plan.limits.maxRetries + 1) {
        throw new Error(
          "Candidate source-snapshot retry allowance is exhausted",
        );
      }
      const attemptSequence = attemptsUsed + 1;
      assertRunning();
      const admitted = await options.journal.startAttempt(
        plan,
        object,
        attemptSequence,
      );
      validateAccounting(plan, admitted.accounting);
      let { attempt } = admitted;
      if (admitted.alreadyRecorded) {
        const concurrentAttempt = await waitForConcurrentUpload(attempt);
        if (!concurrentAttempt) return;
        attempt = concurrentAttempt;
        if (attempt.outcome === "request_started") {
          await options.journal.markInterruptedAttemptUnknown(
            plan,
            object,
            attempt,
          );
          attempt.outcome = "timeout_unknown";
        }
        attemptsUsed = Math.max(attemptsUsed, attempt.attemptSequence);
        if (retryable(attempt.outcome)) {
          if ((await inspectUnknownUpload(attempt)) === "verified") return;
          if (attemptsUsed >= plan.limits.maxRetries + 1) {
            throw new Error(
              "Candidate source-snapshot retry allowance is exhausted",
            );
          }
          continue;
        }
        throw new Error("Candidate source-snapshot object is terminal");
      }
      attemptsUsed += 1;
      attemptedRequests += 1;
      try {
        assertRunning();
        const result = await boundedTransportCall({
          call: async (signal) =>
            await options.transport.uploadOnce(plan, object, signal),
          fatalSignal: executionSignal,
          timeoutMs: plan.limits.requestTimeoutMs,
        });
        if (result.providerCid !== object.expectedCid) {
          await options.journal.recordTerminalFailure(
            plan,
            object,
            attempt,
            "provider_cid_mismatch",
          );
          throw new Error("Filebase returned a missing or mismatched CID");
        }
        const receiptIdentity = {
          attemptId: attempt.attemptId,
          domain: object.domain,
          logicalObjectKey: object.logicalObjectKey,
          providerCid: result.providerCid,
          providerRequestIdHash: result.providerRequestIdHash,
          remoteObjectKey: object.remoteObjectKey,
          responseBytes: result.responseBytes,
        };
        const receipt: CandidateSourceSnapshotUploadReceipt = {
          providerCid: result.providerCid,
          providerRequestIdHash: result.providerRequestIdHash,
          receiptSha256: canonicalReceiptSha256(receiptIdentity),
          responseBytes: z
            .number()
            .int()
            .nonnegative()
            .parse(result.responseBytes),
        };
        await options.journal.recordVerified(plan, object, attempt, receipt);
        uploadedAndVerified += 1;
        return;
      } catch (error) {
        if (!(error instanceof CandidateSourceSnapshotUploadError)) throw error;
        if (!retryable(error.outcome)) {
          await options.journal.recordTerminalFailure(
            plan,
            object,
            attempt,
            "terminal_failure",
          );
          throw error;
        }
        await options.journal.recordAttemptFailure(
          plan,
          object,
          attempt,
          error.outcome,
        );
        if (fatalError !== undefined || executionSignal.aborted) throw error;
        // A transport reset or 5xx can be post-dispatch just as a timeout can.
        // Inspect the immutable object before any retry so an acknowledged provider
        // write is never repeated solely because its response was ambiguous.
        if ((await inspectUnknownUpload(attempt)) === "verified") {
          return;
        }
        if (attemptsUsed >= plan.limits.maxRetries + 1) throw error;
        await (options.backoff?.(attemptSequence) ?? Promise.resolve());
      }
    }
  };

  const workers = Array.from(
    {
      length: Math.min(plan.limits.maxConcurrency, plan.inventory.objectCount),
    },
    async () => {
      try {
        for (;;) {
          const item = await next();
          if (item.done) return;
          await processObject(item.value);
        }
      } catch (error) {
        fail(error);
      }
    },
  );
  await Promise.allSettled(workers);
  if (fatalError !== undefined) throw fatalError;
  if (totalObjects !== plan.inventory.objectCount) {
    throw new Error("Candidate source-snapshot upload inventory is incomplete");
  }
  const accounting = await options.journal.getPlanAccounting(plan);
  validateAccounting(plan, accounting);
  return {
    attemptedRequests,
    recoveredByInspection,
    requestCostUsd: accounting.requestCostUsd,
    skippedVerified,
    totalObjects,
    uploadedAndVerified,
  };
}

function validateAccounting(
  plan: CandidateSourceSnapshotDemoPlan,
  accounting: CandidateSourceSnapshotPlanAccounting,
): void {
  const parsed = z
    .strictObject({
      classAMutationCount: z.number().int().nonnegative(),
      classBReadCount: z.number().int().nonnegative(),
      freeOperationCount: z.number().int().nonnegative(),
      namesApiCount: z.number().int().nonnegative(),
      publicResolverCount: z.number().int().nonnegative(),
      requestCostUsd: z.number().nonnegative(),
      requestCount: z.number().int().nonnegative(),
    })
    .parse(accounting);
  const uploadMaximum = candidateSourceSnapshotRequestCategory(
    plan.requestEnvelope,
    "upload_provider_cid",
  ).maximumRequests;
  const finalVerificationMaximum = candidateSourceSnapshotRequestCategory(
    plan.requestEnvelope,
    "final_credential_free_verification",
  ).maximumRequests;
  const categoryMaximum = (
    category: Parameters<typeof candidateSourceSnapshotRequestCategory>[1],
  ) =>
    candidateSourceSnapshotRequestCategory(plan.requestEnvelope, category)
      .maximumRequests;
  const preflightMaximum = categoryMaximum("bucket_names_preflight");
  const controlMaximum = categoryMaximum("control_public_observation");
  const recoveryMaximum = categoryMaximum("recovery");
  const rollbackMaximum = categoryMaximum("rollback");
  if (
    parsed.requestCount !==
      parsed.classAMutationCount +
        parsed.classBReadCount +
        parsed.freeOperationCount +
        parsed.namesApiCount +
        parsed.publicResolverCount ||
    parsed.requestCount > plan.requestEnvelope.maximumTotalRequests ||
    parsed.classAMutationCount > uploadMaximum ||
    parsed.classBReadCount >
      categoryMaximum("ambiguous_upload_inspection") +
        preflightMaximum +
        finalVerificationMaximum ||
    parsed.namesApiCount >
      preflightMaximum +
        categoryMaximum("names_mutation") +
        controlMaximum +
        recoveryMaximum +
        rollbackMaximum ||
    parsed.publicResolverCount >
      preflightMaximum + controlMaximum + recoveryMaximum + rollbackMaximum ||
    parsed.freeOperationCount !== 0 ||
    parsed.requestCostUsd > plan.costEnvelope.requestUsd.maximumAttempts
  ) {
    throw new Error("Candidate request or cost ceiling is exhausted");
  }
}

function canonicalReceiptSha256(value: unknown): string {
  // Kept local to this boundary so a provider response cannot choose receipt
  // keys or inject unbounded metadata.
  return canonicalJsonSha256(value);
}

export function assertCandidateSourceSnapshotIpnsAdmission(input: {
  intents: readonly {
    domain: "open_data" | "query_table";
    planId: string;
    planSha256: string;
    state: "intent_recorded" | "prior_confirmed";
  }[];
  plan: CandidateSourceSnapshotDemoPlan;
  unverifiedObjectCount: number;
}): void {
  const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
  if (input.unverifiedObjectCount !== 0) {
    throw new Error("IPNS admission requires every immutable object verified");
  }
  if (
    input.intents.length !== 2 ||
    new Set(input.intents.map((intent) => intent.domain)).size !== 2 ||
    input.intents.some(
      (intent) =>
        intent.planId !== plan.planId ||
        intent.planSha256 !== plan.planSha256 ||
        intent.state !== "prior_confirmed",
    )
  ) {
    throw new Error(
      "IPNS admission requires both exact prior-confirmed intents",
    );
  }
}
