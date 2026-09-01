import { describe, expect, it, vi } from "vitest";

import {
  CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS,
  CandidateSourceSnapshotUploadHealthMonitor,
  CandidateSourceSnapshotUploadError,
  executeCandidateSourceSnapshotUploads,
  type CandidateSourceSnapshotPlanAccounting,
  type CandidateSourceSnapshotUploadAttempt,
  type CandidateSourceSnapshotUploadCheckpoint,
  type CandidateSourceSnapshotUploadJournal,
} from "../../src/publication/candidate-source-snapshot-upload.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
  candidateSourceSnapshotRequestCategory,
  createCandidateSourceSnapshotCostEnvelope,
  createCandidateSourceSnapshotDemoPlan,
  createCandidateSourceSnapshotRequestEnvelope,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotUploadObject,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function identity(object: CandidateSourceSnapshotUploadObject) {
  return `${object.domain}:${object.remoteObjectKey}`;
}

function scaledFixture(objectCount: number): {
  objects: CandidateSourceSnapshotUploadObject[];
  plan: CandidateSourceSnapshotDemoPlan;
} {
  const fixture = syntheticCandidateSourceSnapshotDemo();
  const { planId: _planId, planSha256: _planSha256, ...base } = fixture.plan;
  const payloadObjectCount = objectCount - 2;
  const payloadBytes = payloadObjectCount * 10;
  const controlArtifacts = {
    ...base.controlArtifacts,
    payloadBytes,
    payloadObjectCount,
  };
  const inventory = {
    ...base.inventory,
    objectCount,
    totalBytes:
      payloadBytes +
      controlArtifacts.controlBytes +
      CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES,
  };
  const requestEnvelope = createCandidateSourceSnapshotRequestEnvelope({
    limits: base.limits,
    objectCount,
  });
  const costEnvelope = createCandidateSourceSnapshotCostEnvelope({
    inventoryBytes: inventory.totalBytes,
    limits: base.limits,
    pricing: base.pricing,
    requestEnvelope,
  });
  const plan = createCandidateSourceSnapshotDemoPlan({
    ...base,
    controlArtifacts,
    costEnvelope,
    inventory,
    requestEnvelope,
  });
  const payload = Array.from(
    { length: payloadObjectCount },
    (_, index): CandidateSourceSnapshotUploadObject => ({
      byteSize: 10,
      domain: "open_data",
      expectedCid: plan.targets.openData.targetCid,
      logicalObjectKey: `properties/property_${index}.json`,
      remoteObjectKey: `${plan.targets.openData.immutablePrefix}properties/property_${index}.json`,
      sha256: String(index % 10).repeat(64),
    }),
  );
  return {
    objects: [payload, fixture.objects.slice(-2)].flat(),
    plan,
  };
}

class PendingJournal implements CandidateSourceSnapshotUploadJournal {
  readonly accounting: CandidateSourceSnapshotPlanAccounting = {
    classAMutationCount: 0,
    classBReadCount: 0,
    freeOperationCount: 0,
    namesApiCount: 0,
    publicResolverCount: 0,
    requestCostUsd: 0,
    requestCount: 0,
  };
  readonly attempts = new Map<string, CandidateSourceSnapshotUploadAttempt[]>();
  readonly checkpoints = new Map<
    string,
    CandidateSourceSnapshotUploadCheckpoint
  >();

  constructor(
    objects: readonly CandidateSourceSnapshotUploadObject[],
    verified = false,
  ) {
    for (const object of objects) {
      this.checkpoints.set(identity(object), {
        attemptCount: 0,
        providerCid: verified ? object.expectedCid : null,
        receiptSha256: verified ? "a".repeat(64) : null,
        requestCount: 0,
        status: verified ? "verified" : "pending",
      });
    }
  }

  async admitObject(
    _plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    const checkpoint = this.checkpoints.get(identity(object))!;
    if (checkpoint.status === "pending") checkpoint.status = "admitted";
    return checkpoint;
  }

  async getCheckpoint(
    _plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    return this.checkpoints.get(identity(object))!;
  }

  async getPlanAccounting() {
    return this.accounting;
  }

  async listAttempts(
    _plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    return this.attempts.get(identity(object)) ?? [];
  }

  async startAttempt(
    _plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attemptSequence: number,
  ) {
    this.accounting.classAMutationCount += 1;
    this.accounting.requestCount += 1;
    const attempt: CandidateSourceSnapshotUploadAttempt = {
      attemptId: `snapshotdemoattempt_${this.accounting.requestCount.toString(16).padStart(32, "0")}`,
      attemptSequence,
      operation: "upload",
      outcome: "request_started",
      recoveryUploadAttemptId: null,
      requestId: `snapshotdemorequest_${this.accounting.requestCount.toString(16).padStart(32, "0")}`,
      startedAt: new Date().toISOString(),
    };
    this.attempts.set(identity(object), [attempt]);
    return {
      accounting: this.accounting,
      alreadyRecorded: false,
      attempt,
    };
  }

  async recordVerified(
    _plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
  ) {
    attempt.outcome = "verified";
    const checkpoint = this.checkpoints.get(identity(object))!;
    checkpoint.status = "verified";
    checkpoint.providerCid = object.expectedCid;
    checkpoint.receiptSha256 = "b".repeat(64);
  }

  async markInterruptedAttemptUnknown(): Promise<void> {
    throw new Error("unexpected interrupted attempt");
  }

  async recordAttemptFailure(): Promise<void> {
    throw new Error("unexpected attempt failure");
  }

  async recordInspectionResult(): Promise<CandidateSourceSnapshotUploadCheckpoint> {
    throw new Error("unexpected inspection result");
  }

  async recordTerminalFailure(): Promise<void> {
    throw new Error("unexpected terminal failure");
  }

  async startInspection(): ReturnType<
    CandidateSourceSnapshotUploadJournal["startInspection"]
  > {
    throw new Error("unexpected inspection");
  }
}

class RecoveryJournal implements CandidateSourceSnapshotUploadJournal {
  readonly firstKey: string;
  readonly checkpointByKey = new Map<
    string,
    CandidateSourceSnapshotUploadCheckpoint
  >();
  readonly interruptedAttempt: CandidateSourceSnapshotUploadAttempt;
  accounting: CandidateSourceSnapshotPlanAccounting = {
    classAMutationCount: 0,
    classBReadCount: 0,
    freeOperationCount: 0,
    namesApiCount: 0,
    publicResolverCount: 0,
    requestCostUsd: 0,
    requestCount: 0,
  };

  constructor(
    objects: CandidateSourceSnapshotUploadObject[],
    priorOutcome: CandidateSourceSnapshotUploadAttempt["outcome"] = "request_started",
  ) {
    this.firstKey = identity(objects[0]!);
    for (const [index, object] of objects.entries()) {
      this.checkpointByKey.set(identity(object), {
        attemptCount: index === 0 ? 1 : 0,
        providerCid: index === 0 ? null : object.expectedCid,
        receiptSha256: index === 0 ? null : "a".repeat(64),
        requestCount: index === 0 ? 1 : 0,
        status: index === 0 ? "outcome_unknown" : "verified",
      });
    }
    this.interruptedAttempt = {
      attemptId: `snapshotdemoattempt_${"1".repeat(32)}`,
      attemptSequence: 1,
      operation: "upload",
      outcome: priorOutcome,
      recoveryUploadAttemptId: null,
      requestId: `snapshotdemorequest_${"2".repeat(32)}`,
    };
  }

  async getCheckpoint(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    return this.checkpointByKey.get(identity(object))!;
  }

  async admitObject(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    const value = this.checkpointByKey.get(identity(object))!;
    if (value.status === "outcome_unknown") value.status = "admitted";
    return value;
  }

  async getPlanAccounting() {
    return this.accounting;
  }

  async listAttempts(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    return identity(object) === this.firstKey ? [this.interruptedAttempt] : [];
  }

  async markInterruptedAttemptUnknown() {
    this.interruptedAttempt.outcome = "timeout_unknown";
  }

  async startInspection(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    recoveryAttempt: CandidateSourceSnapshotUploadAttempt,
  ) {
    this.accounting = {
      ...this.accounting,
      classBReadCount: this.accounting.classBReadCount + 1,
      requestCostUsd: this.accounting.requestCostUsd + 0.0000045,
      requestCount: this.accounting.requestCount + 1,
    };
    return {
      accounting: this.accounting,
      alreadyRecorded: false,
      attempt: {
        attemptId: `snapshotdemoinspection_${"3".repeat(32)}`,
        attemptSequence: 1,
        operation: "inspect" as const,
        outcome: "request_started" as const,
        recoveryUploadAttemptId: recoveryAttempt.attemptId,
        requestId: `snapshotdemorequest_${"4".repeat(32)}`,
      },
      replayedResult: null,
    };
  }

  async recordInspectionResult(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
    _attempt: unknown,
    _result: Parameters<
      CandidateSourceSnapshotUploadJournal["recordInspectionResult"]
    >[3],
  ) {
    const value = this.checkpointByKey.get(identity(object))!;
    value.status = "verified";
    value.providerCid = object.expectedCid;
    value.receiptSha256 = "b".repeat(64);
    return value;
  }

  async startAttempt(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    _attemptSequence: number,
  ): ReturnType<CandidateSourceSnapshotUploadJournal["startAttempt"]> {
    throw new Error("upload must not run during read-only reconciliation");
  }

  async recordAttemptFailure(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    _attempt: unknown,
    _outcome: unknown,
  ): Promise<void> {
    throw new Error("unexpected attempt failure");
  }

  async recordTerminalFailure(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    _attempt: unknown,
    _outcome: unknown,
  ): Promise<void> {
    throw new Error("unexpected terminal failure");
  }

  async recordVerified(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    _attempt: unknown,
    _receipt: unknown,
  ): Promise<void> {
    throw new Error("unexpected upload verification");
  }
}

class SameRunTimeoutJournal extends RecoveryJournal {
  private uploadSequence = 0;

  constructor(objects: CandidateSourceSnapshotUploadObject[]) {
    super(objects);
    this.checkpointByKey.set(this.firstKey, {
      attemptCount: 0,
      providerCid: null,
      receiptSha256: null,
      requestCount: 0,
      status: "pending",
    });
  }

  override async listAttempts() {
    return [];
  }

  override async startAttempt() {
    this.uploadSequence += 1;
    this.accounting = {
      ...this.accounting,
      classAMutationCount: this.uploadSequence,
      requestCostUsd: this.accounting.requestCostUsd + 0.0000045,
      requestCount: this.accounting.requestCount + 1,
    };
    return {
      accounting: this.accounting,
      alreadyRecorded: false,
      attempt: {
        attemptId: `snapshotdemoattempt_${String(this.uploadSequence).repeat(32)}`,
        attemptSequence: this.uploadSequence,
        operation: "upload" as const,
        outcome: "request_started" as const,
        recoveryUploadAttemptId: null,
        requestId: `snapshotdemorequest_${String(this.uploadSequence + 4).repeat(32)}`,
      },
    };
  }

  override async recordAttemptFailure() {
    this.checkpointByKey.get(this.firstKey)!.status = "outcome_unknown";
  }

  override async recordVerified(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    const value = this.checkpointByKey.get(identity(object))!;
    value.status = "verified";
    value.providerCid = object.expectedCid;
    value.receiptSha256 = "d".repeat(64);
  }

  override async recordTerminalFailure() {
    this.checkpointByKey.get(this.firstKey)!.status = "failed_terminal";
  }

  override async recordInspectionResult(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
    _attempt: unknown,
    result: Parameters<
      CandidateSourceSnapshotUploadJournal["recordInspectionResult"]
    >[3],
  ) {
    const value = this.checkpointByKey.get(identity(object))!;
    value.status =
      result.outcome === "verified"
        ? "verified"
        : result.outcome === "absent"
          ? "admitted"
          : result.outcome === "mismatch"
            ? "failed_terminal"
            : "outcome_unknown";
    if (result.outcome === "verified") {
      value.providerCid = object.expectedCid;
      value.receiptSha256 = "e".repeat(64);
    }
    return value;
  }
}

class ConcurrentUploadJournal extends RecoveryJournal {
  private activeAttempt: CandidateSourceSnapshotUploadAttempt | null = null;

  constructor(objects: CandidateSourceSnapshotUploadObject[]) {
    super(objects);
    this.checkpointByKey.set(this.firstKey, {
      attemptCount: 0,
      providerCid: null,
      receiptSha256: null,
      requestCount: 0,
      status: "pending",
    });
  }

  override async listAttempts(
    _plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    return identity(object) === this.firstKey && this.activeAttempt
      ? [this.activeAttempt]
      : [];
  }

  override async startAttempt(
    _plan: unknown,
    _object: CandidateSourceSnapshotUploadObject,
    attemptSequence: number,
  ): ReturnType<CandidateSourceSnapshotUploadJournal["startAttempt"]> {
    if (this.activeAttempt) {
      return {
        accounting: this.accounting,
        alreadyRecorded: true,
        attempt: this.activeAttempt,
      };
    }
    this.accounting = {
      ...this.accounting,
      classAMutationCount: this.accounting.classAMutationCount + 1,
      requestCostUsd: this.accounting.requestCostUsd + 0.0000045,
      requestCount: this.accounting.requestCount + 1,
    };
    this.activeAttempt = {
      attemptId: `snapshotdemoattempt_${"7".repeat(32)}`,
      attemptSequence,
      operation: "upload",
      outcome: "request_started",
      recoveryUploadAttemptId: null,
      requestId: `snapshotdemorequest_${"8".repeat(32)}`,
      startedAt: new Date().toISOString(),
    };
    return {
      accounting: this.accounting,
      alreadyRecorded: false,
      attempt: this.activeAttempt,
    };
  }

  override async recordVerified(
    plan: unknown,
    object: CandidateSourceSnapshotUploadObject,
  ) {
    if (!this.activeAttempt) throw new Error("missing concurrent attempt");
    this.activeAttempt.outcome = "verified";
    const value = this.checkpointByKey.get(identity(object))!;
    value.status = "verified";
    value.providerCid = object.expectedCid;
    value.receiptSha256 = "e".repeat(64);
  }
}

describe("candidate source-snapshot resumable upload boundary", () => {
  it("derives stable bounded failure evidence without raw transport material", () => {
    const first = new CandidateSourceSnapshotUploadError(
      "timeout_unknown",
      undefined,
      {
        failureClass: "outcome_unknown",
        providerRequestIdHash: "a".repeat(64),
        stage: "put_object_streaming_request",
      },
    );
    const replay = new CandidateSourceSnapshotUploadError(
      "timeout_unknown",
      undefined,
      {
        failureClass: "outcome_unknown",
        providerRequestIdHash: "a".repeat(64),
        stage: "put_object_streaming_request",
      },
    );

    expect(first.evidence).toStrictEqual(replay.evidence);
    expect(first.evidence).toMatchObject({
      failureClass: "outcome_unknown",
      providerRequestIdHash: "a".repeat(64),
      schemaVersion: "candidate-source-snapshot-transport-failure-v1",
      stage: "put_object_streaming_request",
    });
    expect(first.evidence.evidenceSha256).toMatch(/^[a-f0-9]{64}$/);
  });

  it("keeps the future executor fail-closed", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new RecoveryJournal(objects);
    await expect(
      executeCandidateSourceSnapshotUploads({
        executorEnabled: false as true,
        journal,
        objects,
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce: vi.fn(),
        },
        verifyLocalObject: vi.fn(),
      }),
    ).rejects.toThrow("executor is disabled");
  });

  it("preserves and skips every verified object without a transport request", async () => {
    const { objects, plan } = scaledFixture(16);
    const journal = new PendingJournal(objects, true);
    const inspectExistingOnce = vi.fn();
    const uploadOnce = vi.fn();

    await expect(
      executeCandidateSourceSnapshotUploads({
        executionPermit: { maxConcurrency: 4, requestTimeoutMs: 60_000 },
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: { inspectExistingOnce, uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).resolves.toStrictEqual({
      attemptedRequests: 0,
      recoveredByInspection: 0,
      requestCostUsd: 0,
      skippedVerified: 16,
      totalObjects: 16,
      uploadedAndVerified: 0,
    });
    expect(inspectExistingOnce).not.toHaveBeenCalled();
    expect(uploadOnce).not.toHaveBeenCalled();
  });

  it.each([4, 8, 16] as const)(
    "uses only the explicitly permitted concurrency-%i stage",
    async (maxConcurrency) => {
      const { objects, plan } = scaledFixture(16);
      const journal = new PendingJournal(objects);
      let active = 0;
      let peak = 0;
      let release!: () => void;
      let signalStageFull!: () => void;
      const stageFull = new Promise<void>((resolve) => {
        signalStageFull = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const uploadOnce = vi.fn(async (_plan, object) => {
        active += 1;
        peak = Math.max(peak, active);
        if (active === maxConcurrency) signalStageFull();
        await released;
        active -= 1;
        return {
          providerCid: object.expectedCid,
          providerRequestIdHash: null,
          responseBytes: 0,
        };
      });
      const operation = executeCandidateSourceSnapshotUploads({
        executionPermit: { maxConcurrency, requestTimeoutMs: 60_000 },
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce,
        },
        verifyLocalObject: vi.fn(async () => undefined),
      });

      await stageFull;
      expect(active).toBe(maxConcurrency);
      release();
      await expect(operation).resolves.toMatchObject({
        totalObjects: 16,
        uploadedAndVerified: 16,
      });
      expect(peak).toBe(maxConcurrency);
      expect(uploadOnce).toHaveBeenCalledTimes(16);
    },
  );

  it("promotes the closed continuation schedule only after durable stage approval", async () => {
    const objectCount =
      CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS * 2 +
      32;
    const { objects, plan } = scaledFixture(objectCount);
    const journal = new PendingJournal(objects);
    const promotionOrder: (8 | 16)[] = [];
    let authorizedStage: 4 | 8 | 16 = 4;
    let active = 0;
    const peakByStage = new Map<4 | 8 | 16, number>();
    const uploadOnce = vi.fn(async (_plan, object) => {
      active += 1;
      peakByStage.set(
        authorizedStage,
        Math.max(peakByStage.get(authorizedStage) ?? 0, active),
      );
      await new Promise<void>((resolve) => setTimeout(resolve, 1));
      active -= 1;
      return {
        providerCid: object.expectedCid,
        providerRequestIdHash: null,
        responseBytes: 0,
      };
    });

    await expect(
      executeCandidateSourceSnapshotUploads({
        executionPermit: {
          concurrencyStages: [4, 8, 16],
          initialConcurrency: 4,
          promotionVerifiedObjectsPerStage:
            CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS,
          requestTimeoutMs: 60_000,
        },
        executorEnabled: true,
        journal,
        objects,
        onPromote: async (nextStage) => {
          promotionOrder.push(nextStage);
          authorizedStage = nextStage;
        },
        plan,
        transport: { inspectExistingOnce: vi.fn(), uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).resolves.toMatchObject({
      totalObjects: objectCount,
      uploadedAndVerified: objectCount,
    });

    expect(promotionOrder).toStrictEqual([8, 16]);
    expect(peakByStage).toStrictEqual(
      new Map<4 | 8 | 16, number>([
        [4, 4],
        [8, 8],
        [16, 16],
      ]),
    );
  });

  it("rejects staged execution without its durable promotion callback", async () => {
    const { objects, plan } = scaledFixture(16);
    const journal = new PendingJournal(objects);
    const transport = {
      inspectExistingOnce: vi.fn(),
      uploadOnce: vi.fn(),
    };

    await expect(
      executeCandidateSourceSnapshotUploads({
        executionPermit: {
          concurrencyStages: [4, 8, 16],
          initialConcurrency: 4,
          promotionVerifiedObjectsPerStage:
            CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS,
          requestTimeoutMs: 60_000,
        },
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport,
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).rejects.toThrow("requires a durable promotion callback");
    expect(transport.inspectExistingOnce).not.toHaveBeenCalled();
    expect(transport.uploadOnce).not.toHaveBeenCalled();
  });

  it("resumes staged execution from its durable phase without re-running earlier stages", async () => {
    const objectCount =
      CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS + 32;
    const { objects, plan } = scaledFixture(objectCount);
    const journal = new PendingJournal(objects);
    const promotionOrder: (8 | 16)[] = [];

    await expect(
      executeCandidateSourceSnapshotUploads({
        executionPermit: {
          concurrencyStages: [4, 8, 16],
          initialConcurrency: 8,
          promotionVerifiedObjectsPerStage:
            CANDIDATE_SOURCE_SNAPSHOT_CONTINUATION_PROMOTION_VERIFIED_OBJECTS,
          requestTimeoutMs: 60_000,
        },
        executorEnabled: true,
        journal,
        objects,
        onPromote: async (nextStage) => {
          promotionOrder.push(nextStage);
        },
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce: vi.fn(async (_plan, object) => ({
            providerCid: object.expectedCid,
            providerRequestIdHash: null,
            responseBytes: 0,
          })),
        },
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).resolves.toMatchObject({ uploadedAndVerified: objectCount });
    expect(promotionOrder).toStrictEqual([16]);
  });

  it("rejects an execution permit outside the immutable concurrency and continuation timeout bounds", async () => {
    const { objects, plan } = scaledFixture(16);
    const journal = new PendingJournal(objects);
    const transport = {
      inspectExistingOnce: vi.fn(),
      uploadOnce: vi.fn(),
    };
    for (const executionPermit of [
      { maxConcurrency: 17, requestTimeoutMs: 60_000 },
      { maxConcurrency: 4, requestTimeoutMs: 60_001 },
    ]) {
      await expect(
        executeCandidateSourceSnapshotUploads({
          executionPermit,
          executorEnabled: true,
          journal,
          objects,
          plan,
          transport,
          verifyLocalObject: vi.fn(async () => undefined),
        }),
      ).rejects.toThrow();
    }
    expect(transport.inspectExistingOnce).not.toHaveBeenCalled();
    expect(transport.uploadOnce).not.toHaveBeenCalled();
  });

  it("reconciles an ambiguous prior Put with one read and no duplicate upload", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new RecoveryJournal(objects);
    const inspectExistingOnce = vi.fn(async (_plan, object) => ({
      observedBytes: object.byteSize,
      observedCid: object.expectedCid,
      observedSha256: object.sha256,
      outcome: "verified" as const,
      receiptSha256: "c".repeat(64),
    }));
    const uploadOnce = vi.fn();
    const verifyLocalObject = vi.fn(async () => undefined);
    const result = await executeCandidateSourceSnapshotUploads({
      executorEnabled: true,
      journal,
      objects,
      plan,
      transport: { inspectExistingOnce, uploadOnce },
      verifyLocalObject,
    });
    expect(result).toEqual({
      attemptedRequests: 1,
      recoveredByInspection: 1,
      requestCostUsd: 0.0000045,
      skippedVerified: 2,
      totalObjects: 3,
      uploadedAndVerified: 0,
    });
    expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
    expect(uploadOnce).not.toHaveBeenCalled();
    expect(verifyLocalObject).toHaveBeenCalledTimes(3);
  });

  it.each([
    "connection_failure",
    "retryable_http_error",
    "timeout_unknown",
  ] as const)(
    "reconciles a persisted %s before admitting a restart upload",
    async (outcome) => {
      const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
      const journal = new RecoveryJournal(objects, outcome);
      const inspectExistingOnce = vi.fn(async (_plan, object) => ({
        observedBytes: object.byteSize,
        observedCid: object.expectedCid,
        observedSha256: object.sha256,
        outcome: "verified" as const,
        receiptSha256: "6".repeat(64),
      }));
      const uploadOnce = vi.fn();

      await expect(
        executeCandidateSourceSnapshotUploads({
          executorEnabled: true,
          journal,
          objects,
          plan,
          transport: { inspectExistingOnce, uploadOnce },
          verifyLocalObject: vi.fn(async () => undefined),
        }),
      ).resolves.toMatchObject({
        recoveredByInspection: 1,
        totalObjects: 3,
        uploadedAndVerified: 0,
      });
      expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
      expect(uploadOnce).not.toHaveBeenCalled();
    },
  );

  it.each(["verified", "absent", "ambiguous", "mismatch"] as const)(
    "inspects a same-run timeout before deciding %s",
    async (inspectionOutcome) => {
      const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
      const journal = new SameRunTimeoutJournal(objects);
      const first = objects[0]!;
      const uploadOnce = vi
        .fn()
        .mockRejectedValueOnce(
          new CandidateSourceSnapshotUploadError("timeout_unknown"),
        )
        .mockResolvedValue({
          providerCid: first.expectedCid,
          providerRequestIdHash: null,
          responseBytes: 0,
        });
      const inspectExistingOnce = vi.fn(async () =>
        inspectionOutcome === "verified" || inspectionOutcome === "mismatch"
          ? {
              observedBytes: first.byteSize,
              observedCid: first.expectedCid,
              observedSha256:
                inspectionOutcome === "verified"
                  ? first.sha256
                  : "0".repeat(64),
              outcome: inspectionOutcome,
              receiptSha256: "f".repeat(64),
            }
          : {
              outcome: inspectionOutcome,
              receiptSha256: "f".repeat(64),
            },
      );
      const operation = executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: { inspectExistingOnce, uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      });
      if (
        inspectionOutcome === "verified" ||
        inspectionOutcome === "absent" ||
        inspectionOutcome === "ambiguous"
      ) {
        await expect(operation).resolves.toMatchObject({ totalObjects: 3 });
      } else {
        await expect(operation).rejects.toThrow("conflicts");
      }
      expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
      expect(uploadOnce).toHaveBeenCalledTimes(
        inspectionOutcome === "absent" ? 2 : 1,
      );
    },
  );

  it.each([
    "connection_failure",
    "retryable_http_error",
    "timeout_unknown",
  ] as const)(
    "inspects an ambiguous post-dispatch %s before retrying",
    async (outcome) => {
      const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
      const journal = new SameRunTimeoutJournal(objects);
      const first = objects[0]!;
      const uploadOnce = vi
        .fn()
        .mockRejectedValueOnce(new CandidateSourceSnapshotUploadError(outcome))
        .mockResolvedValue({
          providerCid: first.expectedCid,
          providerRequestIdHash: null,
          responseBytes: 0,
        });
      const inspectExistingOnce = vi.fn(async () => ({
        observedBytes: first.byteSize,
        observedCid: first.expectedCid,
        observedSha256: first.sha256,
        outcome: "verified" as const,
        receiptSha256: "a".repeat(64),
      }));

      await expect(
        executeCandidateSourceSnapshotUploads({
          executorEnabled: true,
          journal,
          objects,
          plan,
          transport: { inspectExistingOnce, uploadOnce },
          verifyLocalObject: vi.fn(async () => undefined),
        }),
      ).resolves.toMatchObject({ totalObjects: 3 });
      expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
      expect(uploadOnce).toHaveBeenCalledTimes(1);
    },
  );

  it.each(["connection_failure", "retryable_http_error"] as const)(
    "retries %s only after an absent inspection",
    async (outcome) => {
      const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
      const journal = new SameRunTimeoutJournal(objects);
      const first = objects[0]!;
      const uploadOnce = vi
        .fn()
        .mockRejectedValueOnce(new CandidateSourceSnapshotUploadError(outcome))
        .mockResolvedValue({
          providerCid: first.expectedCid,
          providerRequestIdHash: null,
          responseBytes: 0,
        });
      const inspectExistingOnce = vi.fn(async () => ({
        outcome: "absent" as const,
        receiptSha256: "b".repeat(64),
      }));

      await expect(
        executeCandidateSourceSnapshotUploads({
          executorEnabled: true,
          journal,
          objects,
          plan,
          transport: { inspectExistingOnce, uploadOnce },
          verifyLocalObject: vi.fn(async () => undefined),
        }),
      ).resolves.toMatchObject({ totalObjects: 3 });
      expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
      expect(uploadOnce).toHaveBeenCalledTimes(2);
    },
  );

  it("admits two absent ambiguity inspections before a third and final upload", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new SameRunTimeoutJournal(objects);
    const first = objects[0]!;
    const uploadOnce = vi
      .fn()
      .mockRejectedValueOnce(
        new CandidateSourceSnapshotUploadError("connection_failure"),
      )
      .mockRejectedValueOnce(
        new CandidateSourceSnapshotUploadError("retryable_http_error"),
      )
      .mockResolvedValue({
        providerCid: first.expectedCid,
        providerRequestIdHash: null,
        responseBytes: 0,
      });
    const inspectExistingOnce = vi.fn(async () => ({
      outcome: "absent" as const,
      receiptSha256: "7".repeat(64),
    }));

    await expect(
      executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: { inspectExistingOnce, uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).resolves.toMatchObject({
      attemptedRequests: 5,
      totalObjects: 3,
      uploadedAndVerified: 1,
    });
    expect(uploadOnce).toHaveBeenCalledTimes(3);
    expect(inspectExistingOnce).toHaveBeenCalledTimes(2);
    expect(journal.accounting).toMatchObject({
      classAMutationCount: 3,
      classBReadCount: 2,
      requestCount: 5,
    });
  });

  it("bounds a stalled provider call and persists it as an ambiguous outcome", async () => {
    vi.useFakeTimers();
    try {
      const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
      const journal = new SameRunTimeoutJournal(objects);
      let transportSignal: AbortSignal | undefined;
      let transportSettled = false;
      const uploadOnce = vi.fn(
        async (_plan: unknown, _object: unknown, signal?: AbortSignal) => {
          transportSignal = signal;
          return await new Promise<never>((_resolve, reject) => {
            signal?.addEventListener(
              "abort",
              () => {
                queueMicrotask(() => {
                  transportSettled = true;
                  reject(signal.reason);
                });
              },
              { once: true },
            );
          });
        },
      );
      const inspectExistingOnce = vi.fn(async () => {
        expect(transportSettled).toBe(true);
        return {
          outcome: "ambiguous" as const,
          receiptSha256: "8".repeat(64),
        };
      });
      const operation = executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: { inspectExistingOnce, uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      });
      await vi.advanceTimersByTimeAsync(plan.limits.requestTimeoutMs);
      await expect(operation).resolves.toMatchObject({ totalObjects: 3 });
      expect(transportSignal?.aborted).toBe(true);
      expect(uploadOnce).toHaveBeenCalledTimes(1);
      expect(inspectExistingOnce).toHaveBeenCalledTimes(1);
      expect(journal.accounting).toMatchObject({
        classAMutationCount: 1,
        classBReadCount: 1,
        requestCount: 2,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops after two consecutive five-minute windows exceed one-percent uncertainty", () => {
    const monitor = new CandidateSourceSnapshotUploadHealthMonitor(0);
    for (let index = 0; index < 100; index += 1) monitor.recordRequest(0);
    monitor.recordUncertainty(0);
    monitor.recordUncertainty(0);
    monitor.recordRequest(300_000);
    monitor.recordUncertainty(300_000);
    expect(() => monitor.recordRequest(600_000)).toThrow(
      "uncertainty rate exceeded",
    );
  });

  it("stops after fifteen minutes without a newly verified object", () => {
    const monitor = new CandidateSourceSnapshotUploadHealthMonitor(0);
    monitor.recordRequest(899_999);
    expect(() => monitor.assertHealthy(900_000)).toThrow("progress stalled");
    const progressing = new CandidateSourceSnapshotUploadHealthMonitor(0);
    progressing.recordVerified(899_999);
    expect(() => progressing.assertHealthy(900_000)).not.toThrow();
  });

  it("stops before transport when durable accounting exceeds a hard ceiling", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new SameRunTimeoutJournal(objects);
    const startAttempt = journal.startAttempt.bind(journal);
    journal.startAttempt = vi.fn(async () => {
      const admitted = await startAttempt();
      const count =
        candidateSourceSnapshotRequestCategory(
          plan.requestEnvelope,
          "upload_provider_cid",
        ).maximumRequests + 1;
      admitted.accounting = {
        classAMutationCount: count,
        classBReadCount: 0,
        freeOperationCount: 0,
        namesApiCount: 0,
        publicResolverCount: 0,
        requestCostUsd: 0,
        requestCount: count,
      };
      return admitted;
    });
    const uploadOnce = vi.fn();

    await expect(
      executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce,
        },
        verifyLocalObject: vi.fn(async () => undefined),
      }),
    ).rejects.toThrow("request or cost ceiling is exhausted");
    expect(uploadOnce).not.toHaveBeenCalled();
  });

  it("shares one physical upload when concurrent executions race the same object", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new ConcurrentUploadJournal(objects);
    let releaseUpload!: () => void;
    let signalUploadStarted!: () => void;
    const uploadStarted = new Promise<void>((resolve) => {
      signalUploadStarted = resolve;
    });
    const uploadReleased = new Promise<void>((resolve) => {
      releaseUpload = resolve;
    });
    const uploadOnce = vi.fn(async (_plan, object) => {
      signalUploadStarted();
      await uploadReleased;
      return {
        providerCid: object.expectedCid,
        providerRequestIdHash: null,
        responseBytes: object.byteSize,
      };
    });
    const execute = () =>
      executeCandidateSourceSnapshotUploads({
        backoff: async () =>
          await new Promise((resolve) => setTimeout(resolve, 1)),
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce,
        },
        verifyLocalObject: vi.fn(async () => undefined),
      });

    const winner = execute();
    await uploadStarted;
    const concurrentReplay = execute();
    await new Promise((resolve) => setTimeout(resolve, 5));
    releaseUpload();
    const summaries = await Promise.all([winner, concurrentReplay]);

    expect(uploadOnce).toHaveBeenCalledTimes(1);
    expect(journal.accounting).toMatchObject({
      classAMutationCount: 1,
      requestCount: 1,
    });
    expect(
      summaries.map((summary) => summary.uploadedAndVerified).sort(),
    ).toEqual([0, 1]);
    expect(summaries.map((summary) => summary.skippedVerified).sort()).toEqual([
      2, 3,
    ]);
  });

  it("aborts and drains concurrent workers after the first terminal CID mismatch", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const journal = new SameRunTimeoutJournal(objects);
    const first = objects[0]!;
    const uploadOnce = vi.fn(async () => ({
      providerCid: null,
      providerRequestIdHash: null,
      responseBytes: 0,
    }));
    const verifyLocalObject = vi.fn(
      async (object: CandidateSourceSnapshotUploadObject) => {
        if (identity(object) !== identity(first)) {
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
      },
    );

    await expect(
      executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: {
          inspectExistingOnce: vi.fn(),
          uploadOnce,
        },
        verifyLocalObject,
      }),
    ).rejects.toThrow("missing or mismatched CID");
    expect(uploadOnce).toHaveBeenCalledTimes(1);
    expect(journal.accounting).toMatchObject({
      classAMutationCount: 1,
      classBReadCount: 0,
      requestCount: 1,
    });
  });
});
