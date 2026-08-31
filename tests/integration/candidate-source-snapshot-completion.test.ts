import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  completeCandidateSourceSnapshotDemoPlan,
  recordCandidateSourceSnapshotRemoteCheck,
  recordCandidateSourceSnapshotRemoteVerification,
  recordCandidateSourceSnapshotUploadClosure,
  transitionCandidateSourceSnapshotIpnsIntent,
} from "../../src/db/candidate-source-snapshot-completion.js";
import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotIpnsDomain,
  type CandidateSourceSnapshotIpnsResolver,
} from "../../src/db/candidate-source-snapshot-demo.js";
import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import { runMigrations } from "../../src/db/migrations.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_snapshot_completion_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const resolvers = [
  "filebase_control",
  "filebase_gateway",
  "delegated_ipfs",
] as const satisfies readonly CandidateSourceSnapshotIpnsResolver[];

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

describe("candidate source-snapshot Session 2A completion", () => {
  it("requires exact upload closure, ordered verified intents, and final remote verification", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
      confirmedAt: "2026-08-31T01:00:00.000Z",
      confirmedPlanName: "Filebase Pro or better",
      confirmerReference: "synthetic-capacity-controller",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });
    const approved = await approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
      approvedAt: "2026-08-31T01:01:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
        ),
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });
    await beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
      approvalId: approved.approvalId,
      executorEnabled: true,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });

    await expect(
      createCandidateSourceSnapshotDemoIpnsIntents(databaseUrl, {
        intendedAt: "2026-08-31T01:02:00.000Z",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("immutable upload closure");

    const sql = postgres(databaseUrl, { max: 1 });
    const journal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    try {
      await expect(
        sql.begin(async (transaction) => {
          await transaction`
            UPDATE oracle_candidate_source_snapshot_demo_objects
            SET status = 'admitted', revision = revision + 1
            WHERE plan_id = ${fixture.plan.planId}
          `;
          await transaction`
            UPDATE oracle_candidate_source_snapshot_demo_objects
            SET status = 'verified', provider_cid = expected_cid,
                receipt_sha256 = ${"1".repeat(64)},
                successful_effect_count = 1, revision = revision + 1
            WHERE plan_id = ${fixture.plan.planId}
          `;
          await transaction`
            INSERT INTO oracle_candidate_source_snapshot_demo_upload_closures (
              closure_id, plan_id, plan_sha256, approval_id,
              exact_object_count, exact_total_bytes,
              verified_object_count, verified_total_bytes,
              unresolved_object_count, provider_cid_mismatch_count,
              inventory_root_cid, inventory_root_sha256,
              admitted_request_count, admitted_request_cost_usd,
              closure_sha256, verified_at
            ) VALUES (
              ${`snapshotdemouploadclosure_${"1".repeat(32)}`},
              ${fixture.plan.planId}, ${fixture.plan.planSha256},
              ${approved.approvalId}, ${fixture.exactUpload.exactObjectCount},
              ${fixture.exactUpload.exactTotalBytes},
              ${fixture.exactUpload.exactObjectCount},
              ${fixture.exactUpload.exactTotalBytes}, 0, 0,
              ${fixture.plan.inventory.inventoryRootCid},
              ${fixture.plan.inventory.inventoryRootSha256}, 0, 0,
              ${"2".repeat(64)}, '2026-08-31T01:03:00.000Z'
            )
          `;
        }),
      ).rejects.toThrow("not exact and complete");
    } finally {
      await sql.end({ timeout: 5 });
    }

    for (const [index, object] of fixture.objects.entries()) {
      const admission = await journal.startAttempt(fixture.plan, object, 1);
      const providerRequestIdHash = null;
      const responseBytes = index + 1;
      await journal.recordVerified(fixture.plan, object, admission.attempt, {
        providerCid: object.expectedCid,
        providerRequestIdHash,
        receiptSha256: expectedCandidateSourceSnapshotUploadReceiptSha256({
          attempt: admission.attempt,
          object,
          providerCid: object.expectedCid,
          providerRequestIdHash,
          responseBytes,
        }),
        responseBytes,
      });
    }

    const closureInput = {
      approvalId: approved.approvalId,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      verifiedAt: "2026-08-31T01:03:00.000Z",
    };
    const closure = await recordCandidateSourceSnapshotUploadClosure(
      databaseUrl,
      closureInput,
    );
    expect(closure).toMatchObject({
      exactObjectCount: fixture.exactUpload.exactObjectCount,
      exactTotalBytes: fixture.exactUpload.exactTotalBytes,
    });
    await expect(
      recordCandidateSourceSnapshotUploadClosure(databaseUrl, closureInput),
    ).resolves.toEqual(closure);

    const intents = await createCandidateSourceSnapshotDemoIpnsIntents(
      databaseUrl,
      {
        intendedAt: "2026-08-31T01:04:00.000Z",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    await expect(
      completeCandidateSourceSnapshotDemoPlan(databaseUrl, {
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("lacks remote verification");

    let eventIndex = 0;
    const recordCycle = async (input: {
      classification: "prior" | "target";
      cycleSequence: number;
      domain: CandidateSourceSnapshotIpnsDomain;
      intentId: string;
      observedCid: string;
    }) => {
      for (const resolver of resolvers) {
        const request = await journal.startResolutionRequest(fixture.plan, {
          cycleSequence: input.cycleSequence,
          domain: input.domain,
          intentId: input.intentId,
          resolver,
        });
        eventIndex += 1;
        await journal.recordResolutionObservation(fixture.plan, request, {
          classification: input.classification,
          evidenceSha256: eventIndex.toString(16).padStart(64, "0"),
          observedAt: new Date(Date.now() + eventIndex * 1_000).toISOString(),
          observedCid: input.observedCid,
          requestOutcome: "succeeded",
        });
      }
    };
    const transition = async (input: {
      domain: CandidateSourceSnapshotIpnsDomain;
      expectedRevision: number;
      fromState:
        | "intent_recorded"
        | "prior_confirmed"
        | "update_in_flight"
        | "target_observed";
      intentId: string;
      toState:
        "prior_confirmed" | "update_in_flight" | "target_observed" | "verified";
      transitionedAt?: string;
    }) =>
      await transitionCandidateSourceSnapshotIpnsIntent(databaseUrl, {
        ...input,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        transitionedAt:
          input.transitionedAt ??
          new Date(Date.UTC(2026, 7, 31, 1, 10, eventIndex)).toISOString(),
      });

    for (const intent of intents) {
      const target =
        intent.domain === "open_data"
          ? fixture.plan.targets.openData
          : fixture.plan.targets.queryTable;
      await recordCycle({
        classification: "prior",
        cycleSequence: 1,
        domain: intent.domain,
        intentId: intent.intentId,
        observedCid: target.priorCid,
      });
      const priorTransition = {
        domain: intent.domain,
        expectedRevision: 1,
        fromState: "intent_recorded",
        intentId: intent.intentId,
        toState: "prior_confirmed",
        transitionedAt: new Date(
          Date.UTC(2026, 7, 31, 1, 10, eventIndex),
        ).toISOString(),
      } as const;
      const priorState = await transition(priorTransition);
      await recordCycle({
        classification: "prior",
        cycleSequence: 2,
        domain: intent.domain,
        intentId: intent.intentId,
        observedCid: target.priorCid,
      });
      await expect(transition(priorTransition)).resolves.toEqual(priorState);

      if (intent.domain === "open_data") {
        await expect(
          transition({
            ...priorTransition,
            transitionedAt: new Date(
              Date.parse(priorTransition.transitionedAt) + 1_000,
            ).toISOString(),
          }),
        ).rejects.toThrow("replay conflicts with durable evidence");
        const eventSql = postgres(databaseUrl, { max: 1 });
        try {
          const events = await eventSql<
            {
              event_id: string;
              event_payload: unknown;
              evidence_sha256: string;
            }[]
          >`
            SELECT event_id, event_payload, evidence_sha256
            FROM oracle_candidate_source_snapshot_demo_ipns_events
            WHERE intent_id = ${intent.intentId} AND to_revision = 2
          `;
          expect(events).toHaveLength(1);
          expect(events[0]).toMatchObject({
            event_id: expect.stringMatching(
              /^snapshotdemoipnsevent_[a-f0-9]{32}$/,
            ),
            evidence_sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
          });
          await expect(
            eventSql`
              UPDATE oracle_candidate_source_snapshot_demo_ipns_events
              SET event_payload = '{}'::jsonb
              WHERE event_id = ${events[0]!.event_id}
            `,
          ).rejects.toThrow("immutable");
          await expect(
            eventSql`
              DELETE FROM oracle_candidate_source_snapshot_demo_ipns_events
              WHERE event_id = ${events[0]!.event_id}
            `,
          ).rejects.toThrow("immutable");
          await expect(
            eventSql`
              INSERT INTO oracle_candidate_source_snapshot_demo_ipns_events (
                event_id, intent_id, from_state, to_state, event_sha256,
                recorded_at, event_version, from_revision, to_revision,
                evidence_sha256, event_payload, recorded_at_iso
              ) VALUES (
                ${`snapshotdemoipnsevent_${"f".repeat(32)}`},
                ${intent.intentId}, 'prior_confirmed', 'update_in_flight',
                ${"f".repeat(64)}, '2026-08-31T01:20:00.000Z',
                'candidate-source-snapshot-intent-transition-v1', 2, 3,
                ${"f".repeat(64)}, '{}'::jsonb,
                '2026-08-31T01:20:00.000Z'
              )
            `,
          ).rejects.toThrow("not exact durable evidence");
        } finally {
          await eventSql.end({ timeout: 5 });
        }
      }
    }

    for (const intent of intents) {
      const target =
        intent.domain === "open_data"
          ? fixture.plan.targets.openData
          : fixture.plan.targets.queryTable;
      await transition({
        domain: intent.domain,
        expectedRevision: 2,
        fromState: "prior_confirmed",
        intentId: intent.intentId,
        toState: "update_in_flight",
      });
      const attempt = await journal.startIpnsMutationAttempt(fixture.plan, {
        attemptSequence: 1,
        direction: "update",
        domain: intent.domain,
        intentId: intent.intentId,
        replayAuthorizationSha256: null,
      });
      await journal.recordIpnsMutationOutcome(fixture.plan, attempt, {
        outcome: "acknowledged",
        receiptSha256: (eventIndex + 200).toString(16).padStart(64, "0"),
      });
      await recordCycle({
        classification: "target",
        cycleSequence: 3,
        domain: intent.domain,
        intentId: intent.intentId,
        observedCid: target.targetCid,
      });
      await transition({
        domain: intent.domain,
        expectedRevision: 3,
        fromState: "update_in_flight",
        intentId: intent.intentId,
        toState: "target_observed",
      });
      await transition({
        domain: intent.domain,
        expectedRevision: 4,
        fromState: "target_observed",
        intentId: intent.intentId,
        toState: "verified",
      });
    }

    await expect(
      recordCandidateSourceSnapshotRemoteCheck(databaseUrl, {
        checkKind: "plan_artifact",
        checkedAt: "2026-08-31T02:00:00.000Z",
        evidenceSha256: "a".repeat(64),
        metrics: {},
        observedBytes: fixture.exactUpload.planArtifact.byteSize,
        observedCid: fixture.exactUpload.planArtifact.expectedCid,
        observedSha256: fixture.exactUpload.planArtifact.sha256,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("not authorized by the immutable request envelope");
    const verificationSql = postgres(databaseUrl, { max: 1 });
    try {
      await expect(
        verificationSql`
        INSERT INTO oracle_candidate_source_snapshot_demo_remote_checks (
          check_id, plan_id, plan_sha256, check_kind,
          expected_cid, observed_cid, expected_sha256, observed_sha256,
          expected_bytes, observed_bytes, metrics, evidence_sha256,
          check_payload, check_sha256, checked_at, checked_at_iso
        ) VALUES (
          ${`snapshotdemoremotecheck_${"b".repeat(32)}`},
          ${fixture.plan.planId}, ${fixture.plan.planSha256}, 'plan_artifact',
          ${fixture.exactUpload.planArtifact.expectedCid},
          ${fixture.exactUpload.planArtifact.expectedCid},
          ${fixture.exactUpload.planArtifact.sha256},
          ${fixture.exactUpload.planArtifact.sha256},
          ${fixture.exactUpload.planArtifact.byteSize},
          ${fixture.exactUpload.planArtifact.byteSize},
          ${verificationSql.json({})}, ${"c".repeat(64)},
          ${verificationSql.json({})},
          ${"d".repeat(64)}, '2026-08-31T02:00:00.000Z',
          '2026-08-31T02:00:00.000Z'
        )
        `,
      ).rejects.toThrow("not exact durable evidence");

      await expect(
        recordCandidateSourceSnapshotRemoteVerification(databaseUrl, {
          approvalId: approved.approvalId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          uploadClosureId: closure.closureId,
          verifiedAt: "2026-08-31T02:00:00.000Z",
        }),
      ).rejects.toThrow("not authorized by the immutable request envelope");

      await expect(
        completeCandidateSourceSnapshotDemoPlan(databaseUrl, {
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).rejects.toThrow("lacks remote verification");

      const durableRows = await verificationSql<
        {
          plan_state: string;
          remote_checks: number;
          remote_verifications: number;
        }[]
      >`
      SELECT plan.state AS plan_state,
             (SELECT count(*)::integer
              FROM oracle_candidate_source_snapshot_demo_remote_checks
              WHERE plan_id = plan.plan_id) AS remote_checks,
             (SELECT count(*)::integer
              FROM oracle_candidate_source_snapshot_demo_remote_verifications
              WHERE plan_id = plan.plan_id) AS remote_verifications
      FROM oracle_candidate_source_snapshot_demo_plans plan
      WHERE plan.plan_id = ${fixture.plan.planId}
      `;
      expect(durableRows).toEqual([
        {
          plan_state: "executing",
          remote_checks: 0,
          remote_verifications: 0,
        },
      ]);
    } finally {
      await verificationSql.end({ timeout: 5 });
    }
  });
});
