import { describe, expect, it, vi } from "vitest";

import type { CandidateSourceSnapshotBuildDescriptor } from "../../src/publication/candidate-source-snapshot-build.js";
import {
  createCandidateSourceSnapshotApprovalIdentity,
  renderCandidateSourceSnapshotAuthorizationStatement,
} from "../../src/db/candidate-source-snapshot-approval.js";
import { assertCandidateSourceSnapshotCompletedReplayCompatible } from "../../src/db/candidate-source-snapshot-completion.js";
import {
  executeCandidateSourceSnapshotSession2,
  type CandidateSourceSnapshotExecutionBundle,
  type CandidateSourceSnapshotSession2Dependencies,
  type CandidateSourceSnapshotSession2RemoteRuntimeFactory,
} from "../../src/publication/candidate-source-snapshot-session2.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function syntheticBundle(): CandidateSourceSnapshotExecutionBundle {
  const source = syntheticCandidateSourceSnapshotDemo();
  return {
    build: {
      adoptedExistingControls: true,
      exactObjectCount: source.exactUpload.exactObjectCount,
      exactTotalBytes: source.exactUpload.exactTotalBytes,
      inventoryRootCid: source.plan.inventory.inventoryRootCid,
      inventoryRootSha256: source.plan.inventory.inventoryRootSha256,
      plan: source.plan,
      planArtifact: source.exactUpload.planArtifact,
      planArtifactObjectPath: "synthetic-not-opened.json",
      recordState: null,
    },
    createObjects: () =>
      (async function* () {
        for (const object of source.objects) yield object;
      })(),
    localSource: {
      async openVerifiedStream() {
        throw new Error("disabled execution opened a local object stream");
      },
      async verify() {
        throw new Error("disabled execution verified a local object");
      },
    },
  };
}

function enabledEnvironment(
  bundle: CandidateSourceSnapshotExecutionBundle,
  approvalId: string,
): NodeJS.ProcessEnv {
  const { limits, planId, planSha256, targets } = bundle.build.plan;
  const accessKeyId = "synthetic-access-key";
  const secretAccessKey = "synthetic-secret-key";
  return {
    CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID: accessKeyId,
    CANDIDATE_DEMO_FILEBASE_API_ENDPOINT: "https://api.filebase.io",
    CANDIDATE_DEMO_FILEBASE_API_TOKEN: Buffer.from(
      `${accessKeyId}:${secretAccessKey}`,
      "utf8",
    ).toString("base64"),
    CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.com",
    CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY: secretAccessKey,
    CANDIDATE_DEMO_MAX_BUDGET_USD: String(limits.maxBudgetUsd),
    CANDIDATE_DEMO_MAX_CONCURRENCY: String(limits.maxConcurrency),
    CANDIDATE_DEMO_MAX_OBJECT_BYTES: String(limits.maxObjectBytes),
    CANDIDATE_DEMO_MAX_OBJECTS: String(limits.maxObjects),
    CANDIDATE_DEMO_MAX_REQUESTS: String(limits.maxRequests),
    CANDIDATE_DEMO_MAX_RETRIES: String(limits.maxRetries),
    CANDIDATE_DEMO_MAX_TOTAL_BYTES: String(limits.maxTotalBytes),
    CANDIDATE_DEMO_OPEN_DATA_BUCKET: targets.openData.bucket,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL: targets.openData.ipnsLabel,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY: targets.openData.ipnsNetworkKey,
    CANDIDATE_DEMO_OPEN_DATA_PRIOR_CID: targets.openData.priorCid,
    CANDIDATE_DEMO_OPEN_DATA_TARGET_CID: targets.openData.targetCid,
    CANDIDATE_DEMO_QUERY_TABLE_BUCKET: targets.queryTable.bucket,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL: targets.queryTable.ipnsLabel,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY:
      targets.queryTable.ipnsNetworkKey,
    CANDIDATE_DEMO_QUERY_TABLE_PRIOR_CID: targets.queryTable.priorCid,
    CANDIDATE_DEMO_QUERY_TABLE_TARGET_CID: targets.queryTable.targetCid,
    CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
    CANDIDATE_DEMO_REQUEST_TIMEOUT_MS: String(limits.requestTimeoutMs),
    CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_ID: approvalId,
    CANDIDATE_SOURCE_SNAPSHOT_PLAN_ID: planId,
    CANDIDATE_SOURCE_SNAPSHOT_PLAN_SHA256: planSha256,
  };
}

describe("candidate source-snapshot Session 2 entry point", () => {
  it("disables the process opt-in even when local bundle preparation fails", async () => {
    const environment = {
      CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
    };
    await expect(
      executeCandidateSourceSnapshotSession2({
        databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
        dependencies: {
          prepareBundle: async () => {
            throw new Error("synthetic preparation failure");
          },
        },
        descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
        environment,
      }),
    ).rejects.toThrow("synthetic preparation failure");
    expect(environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED).toBe("false");
  });

  it("disables the process opt-in even when exact config validation fails", async () => {
    const environment = {
      CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
    };
    await expect(
      executeCandidateSourceSnapshotSession2({
        databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
        dependencies: { prepareBundle: async () => syntheticBundle() },
        descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
        environment,
      }),
    ).rejects.toThrow();
    expect(environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED).toBe("false");
  });

  it("returns before constructing any network adapter when the executor is disabled", async () => {
    const bundle = syntheticBundle();
    const prepareBundle = vi.fn(async () => bundle);
    const remoteRuntimeFactory =
      vi.fn() as unknown as CandidateSourceSnapshotSession2RemoteRuntimeFactory;
    const uploadTransportFactory =
      vi.fn() as unknown as CandidateSourceSnapshotSession2Dependencies["uploadTransportFactory"];

    const result = await executeCandidateSourceSnapshotSession2({
      databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
      dependencies: {
        prepareBundle,
        remoteRuntimeFactory,
        uploadTransportFactory,
      },
      descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
      environment: { CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "false" },
    });

    expect(result).toStrictEqual({
      planId: bundle.build.plan.planId,
      planSha256: bundle.build.plan.planSha256,
      status: "executor_disabled",
    });
    expect(prepareBundle).toHaveBeenCalledOnce();
    expect(remoteRuntimeFactory).not.toHaveBeenCalled();
    expect(uploadTransportFactory).not.toHaveBeenCalled();
  });

  it("treats an omitted executor flag as disabled without a network adapter", async () => {
    const bundle = syntheticBundle();
    const remoteRuntimeFactory =
      vi.fn() as unknown as CandidateSourceSnapshotSession2RemoteRuntimeFactory;

    await expect(
      executeCandidateSourceSnapshotSession2({
        databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
        dependencies: {
          prepareBundle: async () => bundle,
          remoteRuntimeFactory,
        },
        descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
        environment: {},
      }),
    ).resolves.toMatchObject({ status: "executor_disabled" });
    expect(remoteRuntimeFactory).not.toHaveBeenCalled();
  });

  it("fails before constructing the production runtime when exact approval identity differs", async () => {
    const bundle = syntheticBundle();
    const approvedAt = "2026-08-31T12:00:00.000Z";
    const approverReference = "operator_test-session-2";
    const authorizationStatement =
      renderCandidateSourceSnapshotAuthorizationStatement(
        bundle.build.plan,
        syntheticCandidateSourceSnapshotDemo().exactUpload,
        "a".repeat(40),
      );
    const environment = enabledEnvironment(
      bundle,
      `snapshotdemoapproval_${"f".repeat(32)}`,
    );
    const remoteRuntimeFactory = vi.fn();

    await expect(
      executeCandidateSourceSnapshotSession2({
        authorization: {
          approvedAt,
          approverReference,
          authorizationStatement,
          confirmedAt: "2026-08-31T11:59:00.000Z",
          confirmedPlanName: "Filebase Pro or better",
          confirmerReference: "operator_test-capacity",
          intendedAt: "2026-08-31T12:01:00.000Z",
          implementationCommitSha: "a".repeat(40),
        },
        databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
        dependencies: {
          prepareBundle: async () => bundle,
          remoteRuntimeFactory,
        },
        descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
        environment,
      }),
    ).rejects.toThrow("does not match the exact authorization bytes");
    expect(remoteRuntimeFactory).not.toHaveBeenCalled();
    expect(environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED).toBe("false");
  });

  it("returns an exact completed replay without constructing a remote adapter", async () => {
    const bundle = syntheticBundle();
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const approvedAt = "2026-08-31T12:00:00.000Z";
    const approverReference = "operator_test-session-2";
    const implementationCommitSha = "a".repeat(40);
    const authorizationStatement =
      renderCandidateSourceSnapshotAuthorizationStatement(
        bundle.build.plan,
        fixture.exactUpload,
        implementationCommitSha,
      );
    const approval = createCandidateSourceSnapshotApprovalIdentity({
      approvedAt,
      approverReference,
      exactUpload: fixture.exactUpload,
      implementationCommitSha,
      plan: bundle.build.plan,
      statement: authorizationStatement,
    });
    const environments = [
      enabledEnvironment(bundle, approval.approvalId),
      enabledEnvironment(bundle, approval.approvalId),
    ];
    const remoteRuntimeFactory = vi.fn();
    const uploadTransportFactory = vi.fn();
    const replay = {
      completedRevision: 9,
      cutover: {
        openData: "target" as const,
        planId: bundle.build.plan.planId,
        planSha256: bundle.build.plan.planSha256,
        queryTable: "target" as const,
        reason: "completed" as const,
        rollback: "not_attempted" as const,
        status: "completed" as const,
      },
      summary: {
        attemptedRequests: fixture.exactUpload.exactObjectCount + 1,
        recoveredByInspection: 1,
        requestCostUsd: 1,
        skippedVerified: 0,
        totalObjects: fixture.exactUpload.exactObjectCount,
        uploadedAndVerified: fixture.exactUpload.exactObjectCount - 1,
      },
      uploadClosure: {
        admittedRequestCostUsd: 1,
        admittedRequestCount: fixture.exactUpload.exactObjectCount,
        approvalId: approval.approvalId,
        closureId: `snapshotdemouploadclosure_${"b".repeat(32)}`,
        closureSha256: "c".repeat(64),
        exactObjectCount: fixture.exactUpload.exactObjectCount,
        exactTotalBytes: fixture.exactUpload.exactTotalBytes,
        planId: bundle.build.plan.planId,
        planSha256: bundle.build.plan.planSha256,
        verifiedAt: "2026-08-31T12:02:00.000Z",
      },
    };
    const loadCompletedReplay = vi.fn(async () => replay);

    const request = (environment: NodeJS.ProcessEnv) =>
      executeCandidateSourceSnapshotSession2({
        authorization: {
          approvedAt,
          approverReference,
          authorizationStatement,
          confirmedAt: "2026-08-31T11:59:00.000Z",
          confirmedPlanName: "Filebase Pro or better",
          confirmerReference: "operator_test-capacity",
          intendedAt: "2026-08-31T12:01:00.000Z",
          implementationCommitSha,
        },
        databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
        dependencies: {
          confirmCapacity: vi.fn() as never,
          loadCompletedReplay,
          prepareBundle: async () => bundle,
          recordPlan: vi.fn() as never,
          recordPlanDerivation: vi.fn() as never,
          remoteRuntimeFactory,
          uploadTransportFactory,
        },
        descriptor: {} as CandidateSourceSnapshotBuildDescriptor,
        environment,
      });
    const [result, concurrentReplay] = await Promise.all(
      environments.map(request),
    );

    expect(result).toStrictEqual({
      ...replay,
      executorEnabled: false,
      planId: bundle.build.plan.planId,
      planSha256: bundle.build.plan.planSha256,
      status: "completed",
    });
    expect(concurrentReplay).toStrictEqual(result);
    expect(loadCompletedReplay).toHaveBeenCalledWith(
      "postgresql://not-contacted.invalid/not-contacted",
      {
        approvalId: approval.approvalId,
        implementationCommitSha,
        planId: bundle.build.plan.planId,
        planSha256: bundle.build.plan.planSha256,
      },
    );
    expect(remoteRuntimeFactory).not.toHaveBeenCalled();
    expect(uploadTransportFactory).not.toHaveBeenCalled();
    expect(loadCompletedReplay).toHaveBeenCalledTimes(2);
    expect(
      environments.every(
        (environment) =>
          environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED === "false",
      ),
    ).toBe(true);
  });

  it("converges a concurrent completion loser on the winner's stored result", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const cutover = {
      openData: "target" as const,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      queryTable: "target" as const,
      reason: "completed" as const,
      rollback: "not_attempted" as const,
      status: "completed" as const,
    };
    const uploadClosure = {
      admittedRequestCostUsd: 1,
      admittedRequestCount: fixture.exactUpload.exactObjectCount,
      approvalId: `snapshotdemoapproval_${"a".repeat(32)}`,
      closureId: `snapshotdemouploadclosure_${"b".repeat(32)}`,
      closureSha256: "c".repeat(64),
      exactObjectCount: fixture.exactUpload.exactObjectCount,
      exactTotalBytes: fixture.exactUpload.exactTotalBytes,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      verifiedAt: "2026-08-31T12:02:00.000Z",
    };
    const stored = {
      completedRevision: 9,
      cutover,
      summary: {
        attemptedRequests: fixture.exactUpload.exactObjectCount,
        recoveredByInspection: 0,
        requestCostUsd: 1,
        skippedVerified: 0,
        totalObjects: fixture.exactUpload.exactObjectCount,
        uploadedAndVerified: fixture.exactUpload.exactObjectCount,
      },
      uploadClosure,
    };
    const loserSummary = {
      attemptedRequests: 0,
      recoveredByInspection: 0,
      requestCostUsd: 1,
      skippedVerified: fixture.exactUpload.exactObjectCount,
      totalObjects: fixture.exactUpload.exactObjectCount,
      uploadedAndVerified: 0,
    };

    expect(() =>
      assertCandidateSourceSnapshotCompletedReplayCompatible(stored, {
        cutover,
        summary: loserSummary,
        uploadClosure,
      }),
    ).not.toThrow();
    expect(() =>
      assertCandidateSourceSnapshotCompletedReplayCompatible(stored, {
        cutover: { ...cutover, planSha256: "d".repeat(64) },
        summary: loserSummary,
        uploadClosure,
      }),
    ).toThrow("completion replay conflicts");
    expect(() =>
      assertCandidateSourceSnapshotCompletedReplayCompatible(stored, {
        cutover,
        summary: loserSummary,
        uploadClosure: { ...uploadClosure, closureSha256: "e".repeat(64) },
      }),
    ).toThrow("completion replay conflicts");
  });
});
