import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  loadCandidateSourceSnapshotDemoExecutionAdmission,
  recordCandidateSourceSnapshotDemoPlan,
} from "../../src/db/candidate-source-snapshot-demo.js";
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
  expect(await runMigrations(databaseUrl)).toHaveLength(26);
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
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const approved = await approveCandidateSourceSnapshotDemoPlan(
      databaseUrl,
      approvalRequest,
    );
    expect(approved.state).toMatchObject({
      approvalCount: 1,
      revision: 3,
      state: "approved",
    });
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, approvalRequest),
    ).resolves.toEqual(approved);
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...approvalRequest,
        approverReference: "conflicting-human-approver",
      }),
    ).rejects.toThrow("approval conflicts");

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
      approval: { approvalId: approved.approvalId },
      capacityConfirmation: {
        confirmedPlanName: "Filebase Pro or better",
      },
      exactUpload: fixture.exactUpload,
      intents,
      state: { revision: 4, state: "executing" },
      unverifiedObjectCount: fixture.objects.length,
    });
  });
});
