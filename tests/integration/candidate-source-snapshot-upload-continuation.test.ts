import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

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
  candidateSourceSnapshotUploadReconciliationComplete,
  heartbeatCandidateSourceSnapshotExecutorLease,
  listCandidateSourceSnapshotUploadContinuationUncertainties,
  loadCandidateSourceSnapshotUploadExecutionPermit,
  proposeCandidateSourceSnapshotUploadContinuation,
  recordCandidateSourceSnapshotUploadContinuation,
  recordCandidateSourceSnapshotUploadReconciliation,
  releaseCandidateSourceSnapshotExecutorLease,
  transitionCandidateSourceSnapshotExecutorLease,
} from "../../src/db/candidate-source-snapshot-upload-continuation.js";
import { runMigrations } from "../../src/db/migrations.js";
import { canonicalJson } from "../../src/lib/canonical-json.js";
import {
  recordSuccessfulCandidateSourceSnapshotPreflight,
  syntheticCandidateSourceSnapshotDemo,
} from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_upload_continuation_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const predecessorCommitSha = "1".repeat(40);
const amendedCommitSha = "2".repeat(40);
const holderToken = "synthetic-private-holder-token-000000000001";

function leaseWindow(offsetMs = 0) {
  const heartbeatAt = new Date(Date.now() + offsetMs);
  return {
    expiresAt: new Date(heartbeatAt.getTime() + 240_000).toISOString(),
    heartbeatAt: heartbeatAt.toISOString(),
  };
}

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect((await runMigrations(databaseUrl)).at(-1)).toBe(
    "035_candidate_source_snapshot_upload_resume.sql",
  );
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

describe("candidate source-snapshot upload continuation", () => {
  it("binds reconciliation before one staged executor lease", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
      confirmedAt: "2026-08-30T00:01:00.000Z",
      confirmedPlanName: "Filebase Pro or better",
      confirmerReference: "synthetic-capacity-controller",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });
    const approval = await approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
      approvedAt: "2026-08-30T00:02:00.000Z",
      approverReference: "synthetic-human-controller",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
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

    const journal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    const verifiedObject = fixture.objects[0]!;
    const verifiedAdmission = await journal.startAttempt(
      fixture.plan,
      verifiedObject,
      1,
    );
    const responseBytes = 7;
    await journal.recordVerified(
      fixture.plan,
      verifiedObject,
      verifiedAdmission.attempt,
      {
        providerCid: verifiedObject.expectedCid,
        providerRequestIdHash: null,
        receiptSha256: expectedCandidateSourceSnapshotUploadReceiptSha256({
          attempt: verifiedAdmission.attempt,
          object: verifiedObject,
          providerCid: verifiedObject.expectedCid,
          providerRequestIdHash: null,
          responseBytes,
        }),
        responseBytes,
      },
    );
    const uncertainObject = fixture.objects[1]!;
    const uncertainAdmission = await journal.startAttempt(
      fixture.plan,
      uncertainObject,
      1,
    );
    await journal.markInterruptedAttemptUnknown(
      fixture.plan,
      uncertainObject,
      uncertainAdmission.attempt,
    );

    const proposed = await proposeCandidateSourceSnapshotUploadContinuation(
      databaseUrl,
      {
        amendedImplementationCommitSha: amendedCommitSha,
        authorizedAt: "2026-08-30T00:03:00.000Z",
        authorizerReference: "synthetic-upload-controller",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        requestTimeoutMs: 60_000,
      },
    );
    expect(proposed.authorizationBinding).toMatchObject({
      checkpoint: {
        uncertainObjectCount: 1,
        verifiedObjectCount: 1,
      },
      execution: {
        concurrencyStages: [4, 8, 16],
        connectionTimeoutMs: 15_000,
        maxSocketsStages: [4, 8, 16],
        promotionVerifiedObjectsPerStage: 64,
        requestTimeoutMs: 60_000,
        s3Endpoint: "https://s3.filebase.io",
        socketTimeoutMs: 45_000,
      },
    });
    expect(proposed.authorizationStatement).toContain(
      `with ${proposed.authorizationBinding.remainingAllowance.requestsRemaining} requests and USD ${proposed.authorizationBinding.remainingAllowance.hardBudgetRemainingUsd} hard-budget allowance remaining at authorization`,
    );
    const identitySql = postgres(databaseUrl, { max: 1 });
    try {
      const identity = await identitySql<
        {
          binding_sha256: string;
          canonical_binding: string;
          statement: string;
          statement_sha256: string;
        }[]
      >`
        SELECT oracle_canonical_jsonb(
                 ${identitySql.json(proposed.authorizationBinding)}::jsonb
               ) AS canonical_binding,
               encode(sha256(convert_to(
                 oracle_canonical_jsonb(
                   ${identitySql.json(proposed.authorizationBinding)}::jsonb
                 ), 'UTF8'
               )), 'hex') AS binding_sha256,
               oracle_css_upload_continuation_statement(
                 ${identitySql.json(proposed.authorizationBinding)}::jsonb,
                 ${proposed.authorizerReference}, ${proposed.authorizedAt}
               ) AS statement,
               encode(sha256(convert_to(
                 oracle_css_upload_continuation_statement(
                   ${identitySql.json(proposed.authorizationBinding)}::jsonb,
                   ${proposed.authorizerReference}, ${proposed.authorizedAt}
                 ), 'UTF8'
               )), 'hex') AS statement_sha256
      `;
      expect(identity[0]).toMatchObject({
        canonical_binding: canonicalJson(proposed.authorizationBinding),
        statement: proposed.authorizationStatement,
        statement_sha256: proposed.authorizationStatementSha256,
      });
    } finally {
      await identitySql.end({ timeout: 5 });
    }
    const authorization = await recordCandidateSourceSnapshotUploadContinuation(
      databaseUrl,
      proposed,
    );
    await expect(
      recordCandidateSourceSnapshotUploadContinuation(databaseUrl, proposed),
    ).resolves.toEqual(authorization);
    const uncertainties =
      await listCandidateSourceSnapshotUploadContinuationUncertainties(
        databaseUrl,
        authorization.authorizationId,
      );
    expect(uncertainties).toHaveLength(1);
    expect(uncertainties[0]).toMatchObject({
      remoteObjectKey: uncertainObject.remoteObjectKey,
      sourceAttemptId: uncertainAdmission.attempt.attemptId,
      uncertaintyKind: "outcome_unknown",
    });

    const expired = leaseWindow(-400_000);
    const initialLease = await acquireCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        acquiredAt: expired.heartbeatAt,
        authorizationId: authorization.authorizationId,
        expiresAt: expired.expiresAt,
        holderToken,
      },
    );
    await expect(
      acquireCandidateSourceSnapshotExecutorLease(databaseUrl, {
        acquiredAt: expired.heartbeatAt,
        authorizationId: authorization.authorizationId,
        expiresAt: expired.expiresAt,
        holderToken: "different-private-holder-token-0000000002",
      }),
    ).rejects.toThrow("already owned");
    const acquired = leaseWindow();
    const lease = await acquireCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        acquiredAt: acquired.heartbeatAt,
        authorizationId: authorization.authorizationId,
        expiresAt: acquired.expiresAt,
        holderToken,
      },
    );
    expect(lease).toMatchObject({
      leaseId: initialLease.leaseId,
      phase: "reconciling",
      revision: initialLease.revision + 1,
    });
    const heartbeatWindow = leaseWindow(500);
    await expect(
      heartbeatCandidateSourceSnapshotExecutorLease(databaseUrl, {
        expiresAt: heartbeatWindow.expiresAt,
        heartbeatAt: heartbeatWindow.heartbeatAt,
        holderToken: "different-private-holder-token-0000000002",
        leaseId: lease.leaseId,
      }),
    ).rejects.toThrow("lacks unexpired ownership");
    const heartbeat = await heartbeatCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        expiresAt: heartbeatWindow.expiresAt,
        heartbeatAt: heartbeatWindow.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
      },
    );
    expect(heartbeat).toMatchObject({
      phase: "reconciling",
      revision: lease.revision + 1,
    });
    const premature = leaseWindow(1_000);
    await expect(
      transitionCandidateSourceSnapshotExecutorLease(databaseUrl, {
        expiresAt: premature.expiresAt,
        heartbeatAt: premature.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
        nextPhase: "upload_4",
        revision: heartbeat.revision,
      }),
    ).rejects.toThrow("lease transition is invalid");

    const continuationJournal =
      new PostgresCandidateSourceSnapshotUploadJournal(databaseUrl, {
        authorizationId: authorization.authorizationId,
        leaseGeneration: 1,
        leaseId: lease.leaseId,
      });
    const frozenSetSql = postgres(databaseUrl, { max: 1 });
    try {
      await expect(
        frozenSetSql.begin(async (transaction) => {
          await transaction`
            UPDATE oracle_candidate_source_snapshot_demo_accounting
            SET request_count = request_count + 1,
                class_b_read_count = class_b_read_count + 1,
                request_cost_usd = request_cost_usd + 0.0000045,
                revision = revision + 1, updated_at = now()
            WHERE plan_id = ${fixture.plan.planId}
          `;
          await transaction`
            UPDATE oracle_candidate_source_snapshot_demo_request_categories
            SET consumed_request_count = consumed_request_count + 1,
                request_cost_usd = request_cost_usd + 0.0000045,
                revision = revision + 1
            WHERE plan_id = ${fixture.plan.planId}
              AND request_category = 'ambiguous_upload_inspection'
          `;
          await transaction`
            INSERT INTO oracle_candidate_source_snapshot_demo_requests (
              request_id, plan_id, operation_class, operation_kind, domain,
              remote_object_key, request_cost_usd, outcome, request_category,
              logical_request_id, attempt_sequence, redirect_sequence,
              upload_continuation_authorization_id, executor_lease_id,
              executor_lease_epoch
            ) VALUES (
              ${`snapshotdemorequest_${"a".repeat(32)}`},
              ${fixture.plan.planId}, 'class_b_read', 'inspect_object',
              ${verifiedObject.domain}, ${verifiedObject.remoteObjectKey},
              0.0000045, 'request_started', 'ambiguous_upload_inspection',
              ${`snapshotdemologicalrequest_${"b".repeat(32)}`}, 1, 0,
              ${authorization.authorizationId}, ${lease.leaseId}, 1
            )
          `;
        }),
      ).rejects.toThrow("outside its frozen uncertainty set");
    } finally {
      await frozenSetSql.end({ timeout: 5 });
    }
    const inspection = await continuationJournal.startInspection(
      fixture.plan,
      uncertainObject,
      { ...uncertainAdmission.attempt, outcome: "timeout_unknown" },
    );
    const inspectionReceipt = "9".repeat(64);
    await continuationJournal.recordInspectionResult(
      fixture.plan,
      uncertainObject,
      inspection.attempt,
      { outcome: "absent", receiptSha256: inspectionReceipt },
    );
    await recordCandidateSourceSnapshotUploadReconciliation(databaseUrl, {
      authorizationId: authorization.authorizationId,
      domain: uncertainObject.domain,
      executorLeaseId: lease.leaseId,
      holderToken,
      inspectionId: inspection.attempt.attemptId,
      planId: fixture.plan.planId,
      receiptSha256: inspectionReceipt,
      recordedAt: new Date().toISOString(),
      remoteObjectKey: uncertainObject.remoteObjectKey,
      result: "conclusively_absent",
    });
    expect(
      await candidateSourceSnapshotUploadReconciliationComplete(
        databaseUrl,
        authorization.authorizationId,
      ),
    ).toBe(true);

    const upload4Window = leaseWindow(2_000);
    const upload4 = await transitionCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        expiresAt: upload4Window.expiresAt,
        heartbeatAt: upload4Window.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
        nextPhase: "upload_4",
        revision: heartbeat.revision,
      },
    );
    const upload8Window = leaseWindow(3_000);
    const upload8 = await transitionCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        expiresAt: upload8Window.expiresAt,
        heartbeatAt: upload8Window.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
        nextPhase: "upload_8",
        revision: upload4.revision,
      },
    );
    const upload16Window = leaseWindow(4_000);
    const upload16 = await transitionCandidateSourceSnapshotExecutorLease(
      databaseUrl,
      {
        expiresAt: upload16Window.expiresAt,
        heartbeatAt: upload16Window.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
        nextPhase: "upload_16",
        revision: upload8.revision,
      },
    );
    await expect(
      loadCandidateSourceSnapshotUploadExecutionPermit(databaseUrl, {
        holderToken,
        leaseId: lease.leaseId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).resolves.toMatchObject({
      effectiveConcurrency: 16,
      maxSockets: 16,
      reconciliationComplete: true,
      requestTimeoutMs: 60_000,
    });

    const sql = postgres(databaseUrl, { max: 1 });
    try {
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_objects
          SET receipt_sha256 = ${"f".repeat(64)}
          WHERE plan_id = ${fixture.plan.planId}
            AND domain = ${verifiedObject.domain}
            AND remote_object_key = ${verifiedObject.remoteObjectKey}
        `,
      ).rejects.toThrow(
        "verified candidate source-snapshot effect is immutable",
      );
      await expect(
        sql`
          DELETE FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
          WHERE authorization_id = ${authorization.authorizationId}
        `,
      ).rejects.toThrow("is immutable");
    } finally {
      await sql.end({ timeout: 5 });
    }

    const releasedWindow = leaseWindow(5_000);
    await expect(
      releaseCandidateSourceSnapshotExecutorLease(databaseUrl, {
        expiresAt: releasedWindow.expiresAt,
        heartbeatAt: releasedWindow.heartbeatAt,
        holderToken,
        leaseId: lease.leaseId,
        revision: upload16.revision,
      }),
    ).resolves.toMatchObject({ effectiveConcurrency: 0, phase: "released" });
  });
});
