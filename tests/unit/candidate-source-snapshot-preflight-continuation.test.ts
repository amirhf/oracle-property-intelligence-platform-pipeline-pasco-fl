import { describe, expect, it } from "vitest";

import {
  candidateSourceSnapshotPreflightContinuationBindingSchema,
  CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
  CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_BINDING_VERSION,
  createCandidateSourceSnapshotPreflightContinuationIdentity,
  renderCandidateSourceSnapshotPreflightContinuationStatement,
  type CandidateSourceSnapshotPreflightContinuationBinding,
} from "../../src/db/candidate-source-snapshot-preflight-continuation.js";
import { DurableInputError } from "../../src/lib/durability-errors.js";
import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import { deterministicId, sha256 } from "../../src/lib/hash.js";

const authorizedAt = "2026-08-31T18:30:00.000Z";
const authorizerReference = "synthetic-human-controller";

const binding: CandidateSourceSnapshotPreflightContinuationBinding = {
  amendedImplementationCommitSha: "b".repeat(40),
  approval: {
    approvalId: `snapshotdemoapproval_${"2".repeat(32)}`,
    approvalSha256: "3".repeat(64),
    approvedPlanRevision: 2,
    authorizationStatementSha256: "4".repeat(64),
    originalImplementationCommitSha: "a".repeat(40),
  },
  authorizedObservation: {
    authorizedAttemptSequence: 2,
    authorizedOperation: "official_filebase_gateway_resolution",
    domain: "open_data",
    expectedPriorCid: `b${"a".repeat(40)}`,
    expectedTargetCid: `Qm${"A".repeat(44)}`,
    ipnsNetworkKey: `k51${"a".repeat(59)}`,
    maximumNewLogicalObservations: 1,
    resolver: "filebase_gateway",
    resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
    storedOperationKind: "public_resolve",
  },
  failedReceipt: {
    attemptSequence: 1,
    outcome: "terminal_failure",
    receiptSha256: "5".repeat(64),
    redirectSequence: 0,
    requestId: `snapshotdemorequest_${"6".repeat(32)}`,
  },
  plan: {
    artifactCid: `Qm${"B".repeat(44)}`,
    artifactSha256: "7".repeat(64),
    planId: `snapshotdemo_${"8".repeat(32)}`,
    planRevision: 3,
    planSha256: "9".repeat(64),
  },
  remainingAllowance: {
    costEnvelopeSha256: "c".repeat(64),
    hardBudgetUsd: "24.999982000000",
    preflightRequests: 44,
    requestCostUsd: "4.859982000000",
    requestEnvelopeSha256: "d".repeat(64),
    totalRequests: 1_079_996,
  },
  schemaVersion:
    CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_BINDING_VERSION,
};

const expectedStatement = `I authorize exactly one resumable candidate-owned source-snapshot preflight continuation for plan snapshotdemo_${"8".repeat(32)}, logical SHA-256 ${"9".repeat(64)}, at durable plan revision 3, under unchanged primary approval snapshotdemoapproval_${"2".repeat(32)}, approval SHA-256 ${"3".repeat(64)}, and primary authorization-statement SHA-256 ${"4".repeat(64)}; the primary approval's original implementation commit remains ${"a".repeat(40)}, and only amended implementation commit ${"b".repeat(40)} may execute this continuation. This continuation is bound to immutable failed request snapshotdemorequest_${"6".repeat(32)} with receipt SHA-256 ${"5".repeat(64)}, outcome terminal_failure, attempt 1, redirect 0, operation official_filebase_gateway_resolution stored as open_data/public_resolve/filebase_gateway, network key k51${"a".repeat(59)}, immutable prior b${"a".repeat(40)}, approved target Qm${"A".repeat(44)}, resolver policy candidate_source_snapshot_filebase_delegated_v1, and at most 1 new logical observation at attempt 2. It preserves plan artifact CID Qm${"B".repeat(44)} and SHA-256 ${"7".repeat(64)}, request-envelope SHA-256 ${"d".repeat(64)}, and cost-envelope SHA-256 ${"c".repeat(64)}, with 44 bucket-names-preflight requests, 1079996 total requests, USD 4.859982000000 request-cost allowance, and USD 24.999982000000 hard-budget allowance remaining at authorization. The primary approval remains unchanged and every existing receipt remains immutable; this continuation authorizes code-continuation compatibility and only that specified recovery observation, not a different plan, target, resolver policy, artifact, upload, IPNS mutation, rollback, Vercel deployment, owner/canonical publication, or authoritative-complete claim. If the specified observation succeeds, the remaining publication operations already authorized by the unchanged primary approval may continue through amended implementation commit ${"b".repeat(40)}; otherwise execution remains stopped fail-closed. Human authorization reference synthetic-human-controller at 2026-08-31T18:30:00.000Z.`;

describe("candidate source-snapshot preflight continuation authorization", () => {
  it("renders the exact canonical statement with every reviewed binding", () => {
    expect(
      renderCandidateSourceSnapshotPreflightContinuationStatement(
        binding,
        authorizerReference,
        authorizedAt,
      ),
    ).toBe(expectedStatement);
  });

  it("derives a deterministic identity and canonical hashes", () => {
    const first = createCandidateSourceSnapshotPreflightContinuationIdentity({
      authorizationBinding: binding,
      authorizedAt,
      authorizerReference,
    });
    const second = createCandidateSourceSnapshotPreflightContinuationIdentity({
      authorizationBinding: binding,
      authorizedAt,
      authorizerReference,
    });
    const authorizationBindingSha256 = canonicalJsonSha256(binding);
    const authorizationStatementSha256 = sha256(expectedStatement);
    const authorizationSha256 = canonicalJsonSha256({
      authorizationBinding: binding,
      authorizationBindingSha256,
      authorizationStatement: expectedStatement,
      authorizationStatementSha256,
      authorizationVersion:
        CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
      authorizedAt,
      authorizerReference,
    });

    expect(first).toEqual(second);
    expect(first.authorizationBindingSha256).toBe(authorizationBindingSha256);
    expect(first.authorizationStatementSha256).toBe(
      authorizationStatementSha256,
    );
    expect(first.authorizationSha256).toBe(authorizationSha256);
    expect(first.authorizationId).toBe(
      deterministicId("snapshotdemocontinuation", [
        CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
        binding.plan.planId,
        binding.approval.approvalId,
        binding.failedReceipt.requestId,
        authorizationSha256,
      ]),
    );
  });

  it("accepts only the exact caller-supplied authorization statement", () => {
    const authorization =
      createCandidateSourceSnapshotPreflightContinuationIdentity({
        authorizationBinding: binding,
        authorizationStatement: expectedStatement,
        authorizedAt,
        authorizerReference,
      });
    expect(authorization.authorizationStatement).toBe(expectedStatement);

    expect(() =>
      createCandidateSourceSnapshotPreflightContinuationIdentity({
        authorizationBinding: binding,
        authorizationStatement: `${expectedStatement} `,
        authorizedAt,
        authorizerReference,
      }),
    ).toThrowError(DurableInputError);
  });

  it("rejects a continuation that does not change the implementation commit", () => {
    expect(() =>
      candidateSourceSnapshotPreflightContinuationBindingSchema.parse({
        ...binding,
        amendedImplementationCommitSha:
          binding.approval.originalImplementationCommitSha,
      }),
    ).toThrow(/amended implementation commit must differ/);
  });

  it("rejects an authorization attempt that does not immediately follow the failure", () => {
    expect(() =>
      candidateSourceSnapshotPreflightContinuationBindingSchema.parse({
        ...binding,
        authorizedObservation: {
          ...binding.authorizedObservation,
          authorizedAttemptSequence: 3,
        },
      }),
    ).toThrow(/continuation attempt must immediately follow/);
  });
});
