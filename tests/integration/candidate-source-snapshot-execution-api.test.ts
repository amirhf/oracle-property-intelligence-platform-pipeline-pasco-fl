import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  loadCandidateSourceSnapshotDemoExecutionAdmission,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotDemoPlan,
} from "../../src/db/candidate-source-snapshot-demo.js";
import {
  createCandidateSourceSnapshotApprovalIdentity,
  renderCandidateSourceSnapshotAuthorizationStatement,
} from "../../src/db/candidate-source-snapshot-approval.js";
import { recordCandidateSourceSnapshotUploadClosure } from "../../src/db/candidate-source-snapshot-completion.js";
import { runMigrations } from "../../src/db/migrations.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_execution_api_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(databaseUrl)).toHaveLength(28);
  expect(await runMigrations(databaseUrl)).toEqual([]);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("candidate source-snapshot guarded execution API", () => {
  it("binds capacity, exact human approval, execution, and both derived intents", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    const capacityRequest = {
      confirmedAt: "2026-08-31T00:01:00.000Z",
      confirmedPlanName: "Filebase Pro or better" as const,
      confirmerReference: "synthetic-capacity-controller",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const capacity = await confirmCandidateSourceSnapshotDemoCapacity(
      databaseUrl,
      capacityRequest,
    );
    expect(capacity).toMatchObject({
      approvalCount: 0,
      revision: 2,
      state: "awaiting_approval",
    });
    await expect(
      confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, capacityRequest),
    ).resolves.toEqual(capacity);

    const approvalRequest = {
      approvedAt: "2026-08-31T00:02:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
        ),
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const directApproval = createCandidateSourceSnapshotApprovalIdentity({
      approvedAt: approvalRequest.approvedAt,
      approverReference: approvalRequest.approverReference,
      exactUpload: fixture.exactUpload,
      plan: fixture.plan,
      statement: approvalRequest.authorizationStatement,
    });
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_approvals (
            approval_id, plan_id, plan_sha256, plan_artifact_sha256,
            plan_artifact_cid, plan_artifact_remote_object_key,
            plan_artifact_bytes, approved_plan_revision,
            approver_reference, approved_at, approved_at_iso,
            approval_version, approval_sha256, authorization_statement,
            authorization_statement_sha256, authorization_binding,
            authorization_binding_sha256
          ) VALUES (
            ${directApproval.approvalId}, ${fixture.plan.planId},
            ${fixture.plan.planSha256},
            ${fixture.exactUpload.planArtifact.sha256},
            ${fixture.exactUpload.planArtifact.expectedCid},
            ${fixture.exactUpload.planArtifact.remoteObjectKey},
            ${fixture.exactUpload.planArtifact.byteSize}, ${capacity.revision},
            ${approvalRequest.approverReference}, ${approvalRequest.approvedAt},
            ${approvalRequest.approvedAt}, ${directApproval.approvalVersion},
            ${directApproval.approvalSha256},
            ${`${approvalRequest.authorizationStatement} `},
            ${directApproval.authorizationStatementSha256},
            ${sql.json(directApproval.authorizationBinding as postgres.JSONValue)},
            ${directApproval.authorizationBindingSha256}
          )
        `,
      ).rejects.toThrow("exact authorization");
    } finally {
      await sql.end({ timeout: 5 });
    }
    const approved = await approveCandidateSourceSnapshotDemoPlan(
      databaseUrl,
      approvalRequest,
    );
    expect(approved.state).toMatchObject({
      approvalCount: 1,
      revision: 3,
      state: "approved",
    });
    expect(approved.approvalSha256).toMatch(/^[a-f0-9]{64}$/);
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, approvalRequest),
    ).resolves.toEqual(approved);
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...approvalRequest,
        approverReference: "conflicting-human-approver",
      }),
    ).rejects.toThrow("approval conflicts");
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...approvalRequest,
        authorizationStatement: `${approvalRequest.authorizationStatement} `,
      }),
    ).rejects.toThrow("does not exactly match");

    const verificationSql = postgres(databaseUrl, { max: 1 });
    try {
      const approvalRows = await verificationSql<
        {
          approval_sha256: string;
          authorization_binding_sha256: string;
          authorization_statement_sha256: string;
        }[]
      >`
        SELECT approval_sha256, authorization_binding_sha256,
               authorization_statement_sha256
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${approved.approvalId}
      `;
      expect(approvalRows).toEqual([
        {
          approval_sha256: approved.approvalSha256,
          authorization_binding_sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
          authorization_statement_sha256:
            expect.stringMatching(/^[a-f0-9]{64}$/),
        },
      ]);
      await expect(
        verificationSql`
          UPDATE oracle_candidate_source_snapshot_demo_approvals
          SET authorization_statement = authorization_statement || ' '
          WHERE approval_id = ${approved.approvalId}
        `,
      ).rejects.toThrow("immutable");
      await expect(
        verificationSql`
          DELETE FROM oracle_candidate_source_snapshot_demo_approvals
          WHERE approval_id = ${approved.approvalId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await verificationSql.end({ timeout: 5 });
    }

    const execution = await beginCandidateSourceSnapshotDemoExecution(
      databaseUrl,
      {
        approvalId: approved.approvalId,
        executorEnabled: true,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    expect(execution).toMatchObject({ revision: 4, state: "executing" });

    const uploadJournal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    for (const [index, object] of fixture.objects.entries()) {
      const admission = await uploadJournal.startAttempt(
        fixture.plan,
        object,
        1,
      );
      const responseBytes = index + 1;
      await uploadJournal.recordVerified(
        fixture.plan,
        object,
        admission.attempt,
        {
          providerCid: object.expectedCid,
          providerRequestIdHash: null,
          receiptSha256: expectedCandidateSourceSnapshotUploadReceiptSha256({
            attempt: admission.attempt,
            object,
            providerCid: object.expectedCid,
            providerRequestIdHash: null,
            responseBytes,
          }),
          responseBytes,
        },
      );
    }
    await recordCandidateSourceSnapshotUploadClosure(databaseUrl, {
      approvalId: approved.approvalId,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      verifiedAt: "2026-08-31T00:02:30.000Z",
    });

    const intentRequest = {
      intendedAt: "2026-08-31T00:03:00.000Z",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const intents = await createCandidateSourceSnapshotDemoIpnsIntents(
      databaseUrl,
      intentRequest,
    );
    expect(intents.map((intent) => intent.domain)).toEqual([
      "open_data",
      "query_table",
    ]);
    await expect(
      createCandidateSourceSnapshotDemoIpnsIntents(databaseUrl, intentRequest),
    ).resolves.toEqual(intents);

    const admission = await loadCandidateSourceSnapshotDemoExecutionAdmission(
      databaseUrl,
      {
        approvalId: approved.approvalId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    expect(admission).toMatchObject({
      approval: {
        approvalId: approved.approvalId,
        approvalSha256: approved.approvalSha256,
        authorizationBindingSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
        authorizationStatementSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
      },
      capacityConfirmation: {
        confirmedPlanName: "Filebase Pro or better",
      },
      exactUpload: fixture.exactUpload,
      intents,
      state: { revision: 4, state: "executing" },
      unverifiedObjectCount: 0,
    });
  });
});
