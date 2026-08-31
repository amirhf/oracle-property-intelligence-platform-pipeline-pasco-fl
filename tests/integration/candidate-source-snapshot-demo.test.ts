import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  loadCandidateSourceSnapshotDemoPlan,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotDemoPlan,
} from "../../src/db/candidate-source-snapshot-demo.js";
import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import { runMigrations } from "../../src/db/migrations.js";
import {
  recordSuccessfulCandidateSourceSnapshotPreflight,
  syntheticCandidateSourceSnapshotDemo,
} from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_source_snapshot_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(databaseUrl)).toHaveLength(31);
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

describe("candidate source-snapshot v2 durability", () => {
  it("records and replays one fail-closed plan with no authority or effect rows", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const first = await recordCandidateSourceSnapshotDemoPlan(databaseUrl, {
      ...fixture,
      objects: (async function* () {
        for (const object of fixture.objects) yield object;
      })(),
    });
    expect(first).toEqual({
      approvalCount: 0,
      effectCount: 0,
      intentCount: 0,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      revision: 1,
      state: "awaiting_configuration",
    });
    expect(
      await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture),
    ).toEqual(first);
    const loaded = await loadCandidateSourceSnapshotDemoPlan(databaseUrl, {
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });
    expect(loaded.plan).toEqual(fixture.plan);
    expect(loaded.exactUpload).toEqual(fixture.exactUpload);
    expect(loaded.state).toEqual(first);

    const sql = postgres(databaseUrl, { max: 1 });
    try {
      const counts = await sql<
        {
          approvals: string;
          events: string;
          intents: string;
          requests: string;
          non_pending_objects: string;
        }[]
      >`
        SELECT
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_approvals)::text AS approvals,
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_events)::text AS events,
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_ipns_intents)::text AS intents,
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_requests)::text AS requests,
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_objects
            WHERE status <> 'pending')::text AS non_pending_objects
      `;
      expect(counts[0]).toEqual({
        approvals: "0",
        events: "1",
        intents: "0",
        non_pending_objects: "0",
        requests: "0",
      });
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'awaiting_approval', revision = 2
          WHERE plan_id = ${fixture.plan.planId}
        `,
      ).rejects.toThrow(
        "invalid candidate source-snapshot plan state transition",
      );
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET plan_sha256 = ${"f".repeat(64)}, revision = 2
          WHERE plan_id = ${fixture.plan.planId}
        `,
      ).rejects.toThrow("plan identity is immutable");
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_approvals (
            approval_id, plan_id, plan_sha256, plan_artifact_sha256,
            plan_artifact_cid, plan_artifact_remote_object_key,
            plan_artifact_bytes, approved_plan_revision,
            approver_reference, approved_at
          ) VALUES (
            ${`snapshotdemoapproval_${"1".repeat(32)}`},
            ${fixture.plan.planId}, ${fixture.plan.planSha256},
            ${fixture.exactUpload.planArtifact.sha256},
            ${fixture.exactUpload.planArtifact.expectedCid},
            ${fixture.exactUpload.planArtifact.remoteObjectKey},
            ${fixture.exactUpload.planArtifact.byteSize}, 1,
            'synthetic-controller', '2026-08-31T00:00:00.000Z'
          )
        `,
      ).rejects.toThrow(
        "requires the exact v2.1 category authorization binding and statement",
      );
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("rejects conflicting exact artifact replay", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await expect(
      recordCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...fixture,
        exactUpload: {
          ...fixture.exactUpload,
          planArtifact: {
            ...fixture.exactUpload.planArtifact,
            sha256: "1".repeat(64),
          },
        },
      }),
    ).rejects.toThrow("replay conflicts with durable identity");
  });

  it("rejects an inventory replay with matching counts but changed immutable object bindings", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const changedObjects = fixture.objects.map((object, index) =>
      index === 0 ? { ...object, sha256: "c".repeat(64) } : object,
    );
    await expect(
      recordCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...fixture,
        objects: changedObjects,
      }),
    ).rejects.toThrow("replay inventory conflicts with durable objects");
  });

  it("rejects a duplicate replay object even when its aggregate count and bytes could match", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const duplicate = [
      fixture.objects[0]!,
      fixture.objects[0]!,
      fixture.objects[2]!,
    ];
    await expect(
      recordCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...fixture,
        objects: duplicate,
      }),
    ).rejects.toThrow();
  });

  it("durably recovers ambiguous uploads and blocks IPNS without exact upload closure", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
        confirmedAt: "2026-08-31T00:00:00.000Z",
        confirmedPlanName: "Filebase Pro",
        confirmerReference: "synthetic-controller",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      });
      const approval = await approveCandidateSourceSnapshotDemoPlan(
        databaseUrl,
        {
          approvedAt: "2026-08-31T00:00:01.000Z",
          approverReference: "synthetic-controller",
          authorizationStatement:
            renderCandidateSourceSnapshotAuthorizationStatement(
              fixture.plan,
              fixture.exactUpload,
              "1".repeat(40),
            ),
          implementationCommitSha: "1".repeat(40),
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        },
      );
      await recordSuccessfulCandidateSourceSnapshotPreflight(
        databaseUrl,
        fixture.plan,
      );
      await beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
        approvalId: approval.approvalId,
        executorEnabled: true,
        implementationCommitSha: "1".repeat(40),
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      });
      const journal = new PostgresCandidateSourceSnapshotUploadJournal(
        databaseUrl,
      );
      const object = fixture.objects[0]!;
      const admitted = await journal.startAttempt(fixture.plan, object, 1);
      await expect(
        journal.recordAttemptFailure(
          fixture.plan,
          object,
          {
            ...admitted.attempt,
            requestId: `snapshotdemorequest_${"0".repeat(32)}`,
          },
          "connection_failure",
        ),
      ).rejects.toThrow("upload attempt identity is not deterministic");
      await expect(
        journal.recordAttemptFailure(
          fixture.plan,
          object,
          admitted.attempt,
          "connection_failure",
        ),
      ).resolves.toBeUndefined();
      const recoveryAttempt = {
        ...admitted.attempt,
        outcome: "connection_failure" as const,
      };
      const inspection = await journal.startInspection(
        fixture.plan,
        object,
        recoveryAttempt,
      );
      expect(inspection.replayedResult).toBeNull();
      const inspectionReplay = await journal.startInspection(
        fixture.plan,
        object,
        recoveryAttempt,
      );
      expect(inspectionReplay).toMatchObject({
        accounting: {
          classAMutationCount: 1,
          classBReadCount: 3,
          requestCount: 10,
        },
        attempt: inspection.attempt,
        replayedResult: null,
      });
      const absentInspection = {
        outcome: "absent" as const,
        receiptSha256: "7".repeat(64),
      };
      await expect(
        journal.recordInspectionResult(
          fixture.plan,
          object,
          inspection.attempt,
          absentInspection,
        ),
      ).resolves.toMatchObject({ status: "admitted" });
      await expect(
        journal.recordInspectionResult(
          fixture.plan,
          object,
          inspection.attempt,
          absentInspection,
        ),
      ).resolves.toMatchObject({ status: "admitted" });
      await expect(
        journal.recordInspectionResult(
          fixture.plan,
          object,
          inspection.attempt,
          { outcome: "ambiguous", receiptSha256: "8".repeat(64) },
        ),
      ).rejects.toThrow("replay conflicts with durable evidence");
      await expect(
        journal.startInspection(fixture.plan, object, recoveryAttempt),
      ).resolves.toMatchObject({ replayedResult: absentInspection });
      const second = await journal.startAttempt(fixture.plan, object, 2);
      await journal.recordAttemptFailure(
        fixture.plan,
        object,
        second.attempt,
        "retryable_http_error",
      );
      const secondInspection = await journal.startInspection(
        fixture.plan,
        object,
        { ...second.attempt, outcome: "retryable_http_error" },
      );
      await journal.recordInspectionResult(
        fixture.plan,
        object,
        secondInspection.attempt,
        { outcome: "absent", receiptSha256: "9".repeat(64) },
      );
      const third = await journal.startAttempt(fixture.plan, object, 3);
      await expect(
        journal.recordVerified(fixture.plan, object, third.attempt, {
          providerCid: object.expectedCid,
          providerRequestIdHash: null,
          receiptSha256: "0".repeat(64),
          responseBytes: 0,
        }),
      ).rejects.toThrow("receipt does not match its immutable upload attempt");
      await expect(
        journal.recordTerminalFailure(
          fixture.plan,
          object,
          third.attempt,
          "terminal_failure",
        ),
      ).resolves.toBeUndefined();
      const recoveryCounts = await sql<
        {
          attempts: number;
          inspections: number;
          object_requests: number;
        }[]
      >`
        SELECT object.request_count AS object_requests,
          (SELECT count(*)::int
           FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
           WHERE attempt.plan_id = object.plan_id
             AND attempt.domain = object.domain
             AND attempt.remote_object_key = object.remote_object_key) AS attempts,
          (SELECT count(*)::int
           FROM oracle_candidate_source_snapshot_demo_inspection_attempts inspection
           WHERE inspection.plan_id = object.plan_id
             AND inspection.domain = object.domain
             AND inspection.remote_object_key = object.remote_object_key) AS inspections
        FROM oracle_candidate_source_snapshot_demo_objects object
        WHERE object.plan_id = ${fixture.plan.planId}
          AND object.domain = ${object.domain}
          AND object.remote_object_key = ${object.remoteObjectKey}
      `;
      expect(recoveryCounts[0]).toEqual({
        attempts: 3,
        inspections: 2,
        object_requests: 5,
      });
      const intentId = `snapshotdemointent_${"d".repeat(32)}`;
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_ipns_intents (
            intent_id, plan_id, plan_sha256, domain, bucket, ipns_label,
            ipns_network_key, prior_cid, target_cid, intended_at
          ) VALUES (
            ${intentId}, ${fixture.plan.planId}, ${fixture.plan.planSha256},
            'open_data', ${fixture.plan.targets.openData.bucket},
            ${fixture.plan.targets.openData.ipnsLabel},
            ${fixture.plan.targets.openData.ipnsNetworkKey},
            ${fixture.plan.targets.openData.priorCid},
            ${fixture.plan.targets.openData.targetCid},
            '2026-08-31T00:00:02.000Z'
          )
        `,
      ).rejects.toThrow("immutable upload closure");
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
