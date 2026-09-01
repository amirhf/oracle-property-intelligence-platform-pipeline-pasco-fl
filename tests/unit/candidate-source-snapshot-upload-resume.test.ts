import { describe, expect, it } from "vitest";

import {
  CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT,
  CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_RESUME_AUTHORIZATION_VERSION,
  createCandidateSourceSnapshotUploadResumeIdentity,
  type CandidateSourceSnapshotUploadResumeBinding,
} from "../../src/db/candidate-source-snapshot-upload-continuation.js";
import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import { sha256 } from "../../src/lib/hash.js";

function binding(): CandidateSourceSnapshotUploadResumeBinding {
  return {
    amendedImplementationCommitSha: "2".repeat(40),
    checkpoint: {
      admittedRecoveryCount: 3,
      admittedRecoverySetSha256: "3".repeat(64),
      futureInspectionCycleCount: 0,
      futureInspectionCycleSetSha256: "4".repeat(64),
      verifiedBytes: 1_232_891_113,
      verifiedObjectCount: 139_861,
      verifiedReceiptSetSha256: "5".repeat(64),
    },
    execution: {
      bufferBodyMaxBytes: 1_048_576,
      connectionTimeoutMs: 15_000,
      concurrencyStages: [4, 8, 16],
      executorLeaseLimit: 1,
      leaseExpiryGraceMs: 30_000,
      maxSocketsStages: [4, 8, 16],
      persistentExecutorEnabled: false,
      promotionVerifiedObjectsPerStage: 64,
      reconciliationRequired: true,
      requestTimeoutMs: 60_000,
      s3Endpoint: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT,
      socketTimeoutMs: 45_000,
    },
    inventory: {
      exactObjectCount: 325_312,
      exactTotalBytes: 3_457_753_084,
      fullInventorySha256: "6".repeat(64),
      inventoryCid: `Qm${"1".repeat(44)}`,
      inventoryRootSha256: "7".repeat(64),
    },
    lease: {
      predecessorLeaseGeneration: 1,
      predecessorLeaseId: `snapshotdemoexecutorlease_${"8".repeat(32)}`,
      resumeLeaseGeneration: 2,
    },
    plan: {
      artifactCid: `Qm${"2".repeat(44)}`,
      artifactSha256: "9".repeat(64),
      planId: `snapshotdemo_${"a".repeat(32)}`,
      planRevision: 4,
      planSha256: "b".repeat(64),
    },
    predecessor: {
      authorizationId: `snapshotdemouploadcontinuation_${"c".repeat(32)}`,
      authorizationSha256: "d".repeat(64),
      implementationCommitSha: "1".repeat(40),
    },
    remainingAllowance: {
      absoluteRequestCeiling: 1_080_000,
      costEnvelopeSha256: "e".repeat(64),
      hardBudgetCeilingUsd: "25.000000000000",
      hardBudgetRemainingUsd: "24.370022500000",
      requestEnvelopeSha256: "f".repeat(64),
      requestsRemaining: 940_005,
    },
    schemaVersion: "candidate-source-snapshot-upload-resume-binding-v1",
    targetsSha256: "0".repeat(64),
  };
}

describe("candidate source-snapshot upload resume identity", () => {
  it("reproduces one exact generation-fenced authorization", () => {
    const first = createCandidateSourceSnapshotUploadResumeIdentity({
      authorizationBinding: binding(),
      authorizedAt: "2026-09-01T00:00:00.000Z",
      authorizerReference: "synthetic-resume-controller",
    });
    const second = createCandidateSourceSnapshotUploadResumeIdentity({
      authorizationBinding: binding(),
      authorizationStatement: first.authorizationStatement,
      authorizedAt: first.authorizedAt,
      authorizerReference: first.authorizerReference,
    });

    expect(second).toEqual(first);
    expect(first.authorizationVersion).toBe(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_RESUME_AUTHORIZATION_VERSION,
    );
    expect(first.authorizationBinding.execution).toMatchObject({
      concurrencyStages: [4, 8, 16],
      persistentExecutorEnabled: false,
      requestTimeoutMs: 60_000,
      s3Endpoint: "https://s3.filebase.io",
    });
    expect(first.authorizationBinding.lease).toEqual({
      predecessorLeaseGeneration: 1,
      predecessorLeaseId: `snapshotdemoexecutorlease_${"8".repeat(32)}`,
      resumeLeaseGeneration: 2,
    });
    expect(first.authorizationBindingSha256).toBe(
      canonicalJsonSha256(first.authorizationBinding),
    );
    expect(first.authorizationStatementSha256).toBe(
      sha256(first.authorizationStatement),
    );
    expect(first.authorizationStatement).toContain(
      "no IPNS operation is authorized",
    );
  });

  it("rejects a non-monotonic lease generation", () => {
    const invalid = binding();
    invalid.lease.resumeLeaseGeneration = 3;
    expect(() =>
      createCandidateSourceSnapshotUploadResumeIdentity({
        authorizationBinding: invalid,
        authorizedAt: "2026-09-01T00:00:00.000Z",
        authorizerReference: "synthetic-resume-controller",
      }),
    ).toThrow("resume lease generation must increment exactly once");
  });
});
