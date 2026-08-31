import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import {
  createCandidateSourceSnapshotApprovalIdentity,
  parseCandidateSourceSnapshotAuthorizationStatement,
  renderCandidateSourceSnapshotAuthorizationBindingStatement,
  renderCandidateSourceSnapshotAuthorizationStatement,
} from "../../src/db/candidate-source-snapshot-approval.js";
import {
  candidateSourceSnapshotRequestCategory,
  CANDIDATE_SOURCE_SNAPSHOT_REQUEST_CATEGORY_ORDER,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const supersededAuthorizationSha256 =
  "6a54c38546f0167246be0476ca24ca0f5682739ec59091df44ce5a2f496d3761";
const implementationCommitSha = "1".repeat(40);

describe("candidate source-snapshot exact human authorization", () => {
  it("binds the exact categorized envelope and protected verification headroom", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const statement = renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
      implementationCommitSha,
    );
    const parsed = parseCandidateSourceSnapshotAuthorizationStatement({
      exactUpload: fixture.exactUpload,
      implementationCommitSha,
      plan: fixture.plan,
      statement,
    });

    expect(Buffer.byteLength(statement, "utf8")).toBeGreaterThan(2_000);
    expect(parsed.authorizationStatementSha256).toBe(
      createHash("sha256").update(statement, "utf8").digest("hex"),
    );
    expect(parsed.authorizationStatementSha256).not.toBe(
      supersededAuthorizationSha256,
    );
    expect(parsed.authorizationBinding.schemaVersion).toBe(
      "candidate-source-snapshot-authorization-binding-v2",
    );
    expect(parsed.authorizationBinding.execution.categoryRequests).toEqual(
      CANDIDATE_SOURCE_SNAPSHOT_REQUEST_CATEGORY_ORDER.map((category) => ({
        category,
        ...candidateSourceSnapshotRequestCategory(
          fixture.plan.requestEnvelope,
          category,
        ),
      })),
    );
    expect(
      parsed.authorizationBinding.execution
        .finalVerificationProtectedHeadroomRequests,
    ).toBe(
      fixture.plan.requestEnvelope.finalVerification.protectedHeadroomRequests,
    );
    expect(statement).toContain("final_credential_free_verification:");
    expect(statement).toContain("protected headroom");
    expect(statement).toContain("disclosure SHA-256");
    expect(statement).toContain(
      `implementation commit SHA ${implementationCommitSha}`,
    );
    expect(statement).toContain(
      `request-envelope SHA-256 ${canonicalJsonSha256(fixture.plan.requestEnvelope)}`,
    );
    expect(statement).toContain(
      `cost-envelope SHA-256 ${canonicalJsonSha256(fixture.plan.costEnvelope)}`,
    );
  });

  it("rejects target drift and every non-exact authorization byte", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const statement = renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
      implementationCommitSha,
    );
    const parsed = parseCandidateSourceSnapshotAuthorizationStatement({
      exactUpload: fixture.exactUpload,
      implementationCommitSha,
      plan: fixture.plan,
      statement,
    });

    expect(() =>
      renderCandidateSourceSnapshotAuthorizationBindingStatement({
        ...parsed.authorizationBinding,
        targets: {
          ...parsed.authorizationBinding.targets,
          openData: {
            ...parsed.authorizationBinding.targets.openData,
            ipnsLabel: "candidate-mismatched-label",
          },
        },
      }),
    ).toThrow("identical bucket and IPNS label");
    expect(() =>
      parseCandidateSourceSnapshotAuthorizationStatement({
        exactUpload: fixture.exactUpload,
        implementationCommitSha,
        plan: fixture.plan,
        statement: `${statement} `,
      }),
    ).toThrow("does not exactly match");
    expect(() =>
      parseCandidateSourceSnapshotAuthorizationStatement({
        exactUpload: fixture.exactUpload,
        implementationCommitSha,
        plan: fixture.plan,
        statement: statement.replace("USD 25", "USD 26"),
      }),
    ).toThrow("does not exactly match");
    expect(() =>
      parseCandidateSourceSnapshotAuthorizationStatement({
        exactUpload: fixture.exactUpload,
        implementationCommitSha: "2".repeat(40),
        plan: fixture.plan,
        statement,
      }),
    ).toThrow("does not exactly match");
  });

  it("derives stable approval identity and changes it for any approval input", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const statement = renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
      implementationCommitSha,
    );
    const input = {
      approvedAt: "2026-08-31T01:02:03.000Z",
      approverReference: "synthetic-human-approver",
      exactUpload: fixture.exactUpload,
      implementationCommitSha,
      plan: fixture.plan,
      statement,
    };
    const first = createCandidateSourceSnapshotApprovalIdentity(input);
    const replay = createCandidateSourceSnapshotApprovalIdentity(input);

    expect(replay).toEqual(first);
    expect(first.approvalVersion).toBe("candidate-source-snapshot-approval-v3");
    expect(first.approvalId).toMatch(/^snapshotdemoapproval_[a-f0-9]{32}$/);
    expect(first.approvalSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(
      createCandidateSourceSnapshotApprovalIdentity({
        ...input,
        implementationCommitSha: "2".repeat(40),
        statement: renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
          "2".repeat(40),
        ),
      }),
    ).not.toMatchObject({
      approvalId: first.approvalId,
      approvalSha256: first.approvalSha256,
    });
    expect(
      createCandidateSourceSnapshotApprovalIdentity({
        ...input,
        approvedAt: "2026-08-31T01:02:04.000Z",
      }),
    ).not.toMatchObject({
      approvalId: first.approvalId,
      approvalSha256: first.approvalSha256,
    });
    expect(() =>
      createCandidateSourceSnapshotApprovalIdentity({
        ...input,
        approvedAt: "2026-08-31T01:02:03+00:00",
      }),
    ).toThrow("canonical UTC");
  });
});
