import postgres from "postgres";
import { describe, expect, it } from "vitest";

import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import {
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  PostgresCandidateSourceSnapshotUploadJournal,
  approveCandidateSourceSnapshotDemoPlan,
  recordCandidateSourceSnapshotDemoPlan,
} from "../../src/db/candidate-source-snapshot-demo.js";
import {
  acquireCandidateSourceSnapshotExecutorLease,
  heartbeatCandidateSourceSnapshotExecutorLease,
  proposeCandidateSourceSnapshotUploadContinuation,
  proposeCandidateSourceSnapshotUploadResume,
  recordCandidateSourceSnapshotUploadContinuation,
  recordCandidateSourceSnapshotUploadReconciliation,
  recordCandidateSourceSnapshotUploadResumeAuthorization,
  transitionCandidateSourceSnapshotExecutorLease,
} from "../../src/db/candidate-source-snapshot-upload-continuation.js";
import { runMigrations } from "../../src/db/migrations.js";
import {
  recordSuccessfulCandidateSourceSnapshotPreflight,
  syntheticCandidateSourceSnapshotDemo,
} from "../helpers/candidate-source-snapshot-demo.js";
import { CandidateSourceSnapshotUploadError } from "../../src/publication/candidate-source-snapshot-upload.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const predecessorCommitSha = "1".repeat(40);
const continuationCommitSha = "2".repeat(40);
const resumeCommitSha = "3".repeat(40);
let schemaSequence = 0;

function timestamp(offsetMs = 0): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

async function withDisposableDatabase<T>(
  callback: (databaseUrl: string) => Promise<T>,
): Promise<T> {
  schemaSequence += 1;
  const schemaName = `candidate_upload_resume_${process.pid}_${Date.now()}_${schemaSequence}`;
  const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  try {
    expect((await runMigrations(databaseUrl)).at(-1)).toBe(
      "035_candidate_source_snapshot_upload_resume.sql",
    );
    expect(await runMigrations(databaseUrl)).toEqual([]);
    return await callback(databaseUrl);
  } finally {
    const cleanup = postgres(adminDatabaseUrl, { max: 1 });
    try {
      await cleanup.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
    } finally {
      await cleanup.end({ timeout: 5 });
    }
  }
}

async function recordExecutingFixture(databaseUrl: string) {
  const fixture = syntheticCandidateSourceSnapshotDemo();
  await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
  await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
    confirmedAt: "2026-08-31T01:00:00.000Z",
    confirmedPlanName: "Filebase Pro or better",
    confirmerReference: "synthetic-capacity-controller",
    planId: fixture.plan.planId,
    planSha256: fixture.plan.planSha256,
  });
  const approval = await approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
    approvedAt: "2026-08-31T01:01:00.000Z",
    approverReference: "synthetic-human-controller",
    authorizationStatement: renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
      predecessorCommitSha,
    ),
    implementationCommitSha: predecessorCommitSha,
    planId: fixture.plan.planId,
    planSha256: fixture.plan.planSha256,
  });
  await recordSuccessfulCandidateSourceSnapshotPreflight(
    databaseUrl,
    fixture.plan,
  );
  await beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
    approvalId: approval.approvalId,
    executorEnabled: true,
    implementationCommitSha: predecessorCommitSha,
    planId: fixture.plan.planId,
    planSha256: fixture.plan.planSha256,
  });
  return fixture;
}

describe("candidate source-snapshot upload resume", () => {
  it("returns exactly three zero-request admitted objects to pending without changing verified evidence", async () => {
    await withDisposableDatabase(async (databaseUrl) => {
      const fixture = await recordExecutingFixture(databaseUrl);
      const sql = postgres(databaseUrl, { max: 1 });
      try {
        await sql`
            UPDATE oracle_candidate_source_snapshot_demo_objects
            SET status = 'admitted', revision = revision + 1
            WHERE plan_id = ${fixture.plan.planId} AND status = 'pending'
          `;
        const before = await sql<
          {
            admitted_count: number;
            attempt_count: string;
            request_count: string;
            verified_bytes: string;
            verified_count: number;
            verified_receipt_set_sha256: string;
          }[]
        >`
            SELECT count(*) FILTER (WHERE status = 'admitted')::integer
                     AS admitted_count,
                   sum(attempt_count)::text AS attempt_count,
                   sum(request_count)::text AS request_count,
                   count(*) FILTER (WHERE status = 'verified')::integer
                     AS verified_count,
                   COALESCE(sum(expected_bytes) FILTER (
                     WHERE status = 'verified'
                   ), 0)::text AS verified_bytes,
                   oracle_css_verified_receipt_set_sha256(
                     ${fixture.plan.planId}
                   ) AS verified_receipt_set_sha256
            FROM oracle_candidate_source_snapshot_demo_objects
            WHERE plan_id = ${fixture.plan.planId}
          `;
        expect(before[0]).toMatchObject({
          admitted_count: 3,
          attempt_count: "0",
          request_count: "0",
          verified_bytes: "0",
          verified_count: 0,
        });

        const recovery = await sql<
          {
            recovery: {
              insertedCount: number;
              recordedCount: number;
              recordedSetSha256: string;
            };
          }[]
        >`
            SELECT oracle_css_record_admitted_recovery(
              ${fixture.plan.planId}, ${resumeCommitSha}, ${timestamp()}
            ) AS recovery
          `;
        expect(recovery[0]?.recovery).toMatchObject({
          insertedCount: 3,
          recordedCount: 3,
        });

        const after = await sql<
          {
            event_count: number;
            pending_count: number;
            verified_bytes: string;
            verified_count: number;
            verified_receipt_set_sha256: string;
          }[]
        >`
            SELECT
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_admitted_recovery_events
               WHERE plan_id = ${fixture.plan.planId}
                 AND disposition = 'returned_pending_no_put') AS event_count,
              count(*) FILTER (WHERE status = 'pending')::integer
                AS pending_count,
              count(*) FILTER (WHERE status = 'verified')::integer
                AS verified_count,
              COALESCE(sum(expected_bytes) FILTER (
                WHERE status = 'verified'
              ), 0)::text AS verified_bytes,
              oracle_css_verified_receipt_set_sha256(
                ${fixture.plan.planId}
              ) AS verified_receipt_set_sha256
            FROM oracle_candidate_source_snapshot_demo_objects
            WHERE plan_id = ${fixture.plan.planId}
          `;
        expect(after[0]).toMatchObject({
          event_count: 3,
          pending_count: 3,
          verified_bytes: before[0]?.verified_bytes,
          verified_count: before[0]?.verified_count,
          verified_receipt_set_sha256: before[0]?.verified_receipt_set_sha256,
        });
      } finally {
        await sql.end({ timeout: 5 });
      }
    });
  }, 60_000);

  it("supersedes one expired lease and quarantines only the uncertain object", async () => {
    await withDisposableDatabase(async (databaseUrl) => {
      const fixture = await recordExecutingFixture(databaseUrl);
      const uncertainObject = fixture.objects[0]!;
      const recoveryOnlyObject = fixture.objects[1]!;
      const unrelatedObject = fixture.objects[2]!;
      const initialJournal = new PostgresCandidateSourceSnapshotUploadJournal(
        databaseUrl,
      );
      const initialAdmission = await initialJournal.startAttempt(
        fixture.plan,
        uncertainObject,
        1,
      );
      await initialJournal.markInterruptedAttemptUnknown(
        fixture.plan,
        uncertainObject,
        initialAdmission.attempt,
      );
      const continuationProposal =
        await proposeCandidateSourceSnapshotUploadContinuation(databaseUrl, {
          amendedImplementationCommitSha: continuationCommitSha,
          authorizedAt: "2026-08-31T01:02:00.000Z",
          authorizerReference: "synthetic-upload-controller",
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          requestTimeoutMs: 60_000,
        });
      const continuation =
        await recordCandidateSourceSnapshotUploadContinuation(
          databaseUrl,
          continuationProposal,
        );

      const initialHolderToken = "initial-holder-token-000000000000001";
      const initialAcquiredAt = timestamp(-120_000);
      const initialExpiresAt = timestamp(120_000);
      const generationOne = await acquireCandidateSourceSnapshotExecutorLease(
        databaseUrl,
        {
          acquiredAt: initialAcquiredAt,
          authorizationId: continuation.authorizationId,
          expiresAt: initialExpiresAt,
          holderToken: initialHolderToken,
        },
      );
      const generationOneJournal =
        new PostgresCandidateSourceSnapshotUploadJournal(databaseUrl, {
          authorizationId: continuation.authorizationId,
          leaseGeneration: 1,
          leaseId: generationOne.leaseId,
        });
      const inspection = await generationOneJournal.startInspection(
        fixture.plan,
        uncertainObject,
        { ...initialAdmission.attempt, outcome: "timeout_unknown" },
      );
      const inspectionReceipt = "9".repeat(64);
      await generationOneJournal.recordInspectionResult(
        fixture.plan,
        uncertainObject,
        inspection.attempt,
        { outcome: "absent", receiptSha256: inspectionReceipt },
      );
      await recordCandidateSourceSnapshotUploadReconciliation(databaseUrl, {
        authorizationId: continuation.authorizationId,
        domain: uncertainObject.domain,
        executorLeaseId: generationOne.leaseId,
        holderToken: initialHolderToken,
        inspectionId: inspection.attempt.attemptId,
        leaseGeneration: 1,
        planId: fixture.plan.planId,
        receiptSha256: inspectionReceipt,
        recordedAt: timestamp(),
        remoteObjectKey: uncertainObject.remoteObjectKey,
        result: "conclusively_absent",
      });

      const sql = postgres(databaseUrl, { max: 1 });
      try {
        await sql`
            UPDATE oracle_candidate_source_snapshot_demo_objects
            SET status = 'admitted', revision = revision + 1
            WHERE plan_id = ${fixture.plan.planId}
              AND domain = ${recoveryOnlyObject.domain}
              AND remote_object_key = ${recoveryOnlyObject.remoteObjectKey}
          `;
        await sql`
            SELECT oracle_css_record_admitted_recovery(
              ${fixture.plan.planId}, ${resumeCommitSha}, ${timestamp()}
            )
          `;
        await sql`
            UPDATE oracle_candidate_source_snapshot_executor_leases
            SET heartbeat_at = ${timestamp(-90_000)},
                expires_at = ${timestamp(-60_000)},
                revision = revision + 1
            WHERE lease_id = ${generationOne.leaseId}
          `;
      } finally {
        await sql.end({ timeout: 5 });
      }

      const resumeProposal = await proposeCandidateSourceSnapshotUploadResume(
        databaseUrl,
        {
          amendedImplementationCommitSha: resumeCommitSha,
          authorizedAt: timestamp(),
          authorizerReference: "synthetic-resume-controller",
          persistentExecutorEnabled: false,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        },
      );
      const resume =
        await recordCandidateSourceSnapshotUploadResumeAuthorization(
          databaseUrl,
          resumeProposal,
        );
      expect(resume.authorizationBinding).toMatchObject({
        checkpoint: { admittedRecoveryCount: 1 },
        lease: {
          predecessorLeaseGeneration: 1,
          resumeLeaseGeneration: 2,
        },
      });

      const acquiredAt = timestamp();
      const expiresAt = timestamp(240_000);
      const tokens = [
        "resume-holder-token-0000000000000001",
        "resume-holder-token-0000000000000002",
      ] as const;
      const contenders = await Promise.allSettled(
        tokens.map(
          async (holderToken) =>
            await acquireCandidateSourceSnapshotExecutorLease(databaseUrl, {
              acquiredAt,
              authorizationId: continuation.authorizationId,
              expiresAt,
              holderToken,
              leaseGeneration: 2,
              persistentExecutorEnabled: false,
              resumeAuthorizationId: resume.authorizationId,
            }),
        ),
      );
      const winners = contenders.flatMap((result, index) =>
        result.status === "fulfilled"
          ? [{ lease: result.value, token: tokens[index]! }]
          : [],
      );
      expect(
        winners,
        contenders
          .map((result) =>
            result.status === "rejected"
              ? String(result.reason)
              : `fulfilled:${result.value.leaseId}`,
          )
          .join("\n"),
      ).toHaveLength(1);
      expect(
        contenders.filter((result) => result.status === "rejected"),
      ).toHaveLength(1);
      const winner = winners[0]!;
      expect(winner.lease).toMatchObject({
        leaseGeneration: 2,
        resumeAuthorizationId: resume.authorizationId,
      });

      await expect(
        heartbeatCandidateSourceSnapshotExecutorLease(databaseUrl, {
          expiresAt: timestamp(240_000),
          heartbeatAt: timestamp(),
          holderToken: initialHolderToken,
          leaseGeneration: 1,
          leaseId: generationOne.leaseId,
        }),
      ).rejects.toThrow("lacks unexpired ownership");
      const generationTwo =
        await transitionCandidateSourceSnapshotExecutorLease(databaseUrl, {
          expiresAt: timestamp(240_000),
          heartbeatAt: timestamp(),
          holderToken: winner.token,
          leaseGeneration: 2,
          leaseId: winner.lease.leaseId,
          nextPhase: "upload_4",
          revision: winner.lease.revision,
        });
      const generationTwoJournal =
        new PostgresCandidateSourceSnapshotUploadJournal(databaseUrl, {
          authorizationId: continuation.authorizationId,
          leaseGeneration: 2,
          leaseId: winner.lease.leaseId,
          resumeAuthorizationId: resume.authorizationId,
        });
      const futureAdmission = await generationTwoJournal.startAttempt(
        fixture.plan,
        uncertainObject,
        2,
      );
      await generationTwoJournal.recordAttemptFailure(
        fixture.plan,
        uncertainObject,
        futureAdmission.attempt,
        "timeout_unknown",
        new CandidateSourceSnapshotUploadError("timeout_unknown", undefined, {
          failureClass: "outcome_unknown",
          providerRequestIdHash: null,
          stage: "transport_deadline",
        }).evidence,
      );
      const futureInspection = await generationTwoJournal.startInspection(
        fixture.plan,
        uncertainObject,
        { ...futureAdmission.attempt, outcome: "timeout_unknown" },
      );

      const unrelatedAdmission = await generationTwoJournal.startAttempt(
        fixture.plan,
        unrelatedObject,
        1,
      );
      const responseBytes = 7;
      await generationTwoJournal.recordVerified(
        fixture.plan,
        unrelatedObject,
        unrelatedAdmission.attempt,
        {
          providerCid: unrelatedObject.expectedCid,
          providerRequestIdHash: null,
          receiptSha256: expectedCandidateSourceSnapshotUploadReceiptSha256({
            attempt: unrelatedAdmission.attempt,
            object: unrelatedObject,
            providerCid: unrelatedObject.expectedCid,
            providerRequestIdHash: null,
            responseBytes,
          }),
          responseBytes,
        },
      );
      await expect(
        generationTwoJournal.startAttempt(fixture.plan, uncertainObject, 3),
      ).rejects.toThrow("lacks reconciled active generation");

      const quarantineSql = postgres(databaseUrl, { max: 1 });
      try {
        const unresolved = await quarantineSql<
          {
            member_count: number;
            reconciled: boolean;
            supersession_count: number;
          }[]
        >`
            SELECT
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members
               WHERE plan_id = ${fixture.plan.planId}) AS member_count,
              oracle_css_upload_resume_is_reconciled(
                ${resume.authorizationId}
              ) AS reconciled,
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_executor_lease_supersession_events
               WHERE plan_id = ${fixture.plan.planId}) AS supersession_count
          `;
        expect(unresolved[0]).toEqual({
          member_count: 1,
          reconciled: false,
          supersession_count: 1,
        });
      } finally {
        await quarantineSql.end({ timeout: 5 });
      }

      await generationTwoJournal.recordInspectionResult(
        fixture.plan,
        uncertainObject,
        futureInspection.attempt,
        { outcome: "absent", receiptSha256: "8".repeat(64) },
      );
      const resolvedSql = postgres(databaseUrl, { max: 1 });
      try {
        const resolved = await resolvedSql<
          {
            generation: number;
            reconciled: boolean;
            resolution_count: number;
          }[]
        >`
            SELECT oracle_css_active_executor_lease_generation(
                     ${fixture.plan.planId}
                   ) AS generation,
                   oracle_css_upload_resume_is_reconciled(
                     ${resume.authorizationId}
                   ) AS reconciled,
                   (SELECT count(*)::integer
                    FROM oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions
                    WHERE plan_id = ${fixture.plan.planId}) AS resolution_count
          `;
        expect(resolved[0]).toEqual({
          generation: 2,
          reconciled: true,
          resolution_count: 1,
        });
      } finally {
        await resolvedSql.end({ timeout: 5 });
      }

      expect(generationTwo).toMatchObject({
        effectiveConcurrency: 4,
        leaseGeneration: 2,
        phase: "upload_4",
      });
    });
  }, 60_000);
});
