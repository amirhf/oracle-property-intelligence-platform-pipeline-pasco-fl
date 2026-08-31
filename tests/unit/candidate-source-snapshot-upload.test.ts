import { describe, expect, it, vi } from "vitest";

import {
  CandidateSourceSnapshotUploadError,
  executeCandidateSourceSnapshotUploads,
  type CandidateSourceSnapshotPlanAccounting,
  type CandidateSourceSnapshotUploadAttempt,
  type CandidateSourceSnapshotUploadCheckpoint,
  type CandidateSourceSnapshotUploadJournal,
} from "../../src/publication/candidate-source-snapshot-upload.js";
import {
  candidateSourceSnapshotRequestCategory,
  type CandidateSourceSnapshotUploadObject,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function identity(object: CandidateSourceSnapshotUploadObject) {
  return `${object.domain}:${object.remoteObjectKey}`;
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
      if (inspectionOutcome === "verified" || inspectionOutcome === "absent") {
        await expect(operation).resolves.toMatchObject({ totalObjects: 3 });
      } else {
        await expect(operation).rejects.toThrow(
          inspectionOutcome === "ambiguous" ? "ambiguous" : "conflicts",
        );
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
      const uploadOnce = vi.fn(
        async (_plan: unknown, _object: unknown, signal?: AbortSignal) => {
          transportSignal = signal;
          return await new Promise<never>(() => undefined);
        },
      );
      const inspectExistingOnce = vi.fn(async () => ({
        outcome: "ambiguous" as const,
        receiptSha256: "8".repeat(64),
      }));
      const operation = executeCandidateSourceSnapshotUploads({
        executorEnabled: true,
        journal,
        objects,
        plan,
        transport: { inspectExistingOnce, uploadOnce },
        verifyLocalObject: vi.fn(async () => undefined),
      });
      const rejection = expect(operation).rejects.toThrow(
        "remote object inspection is ambiguous",
      );

      await vi.advanceTimersByTimeAsync(plan.limits.requestTimeoutMs);
      await rejection;
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
