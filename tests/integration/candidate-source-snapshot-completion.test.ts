import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  admitCandidateSourceSnapshotRemoteRead,
  completeCandidateSourceSnapshotDemoPlan,
  loadExistingCandidateSourceSnapshotRemoteReadAdmission,
  loadCandidateSourceSnapshotRemoteReadReceipt,
  loadCompletedCandidateSourceSnapshotDemoReplay,
  recordCandidateSourceSnapshotRemoteCheck,
  recordCandidateSourceSnapshotRemoteReadReceipt,
  recordCandidateSourceSnapshotRemoteVerification,
  recordCandidateSourceSnapshotUploadClosure,
  transitionCandidateSourceSnapshotIpnsIntent,
} from "../../src/db/candidate-source-snapshot-completion.js";
import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  admitCandidateSourceSnapshotPreflightRequest,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  loadCandidateSourceSnapshotPreflightRequestOutcome,
  loadCandidateSourceSnapshotIpnsIntents,
  loadCandidateSourceSnapshotIpnsIntentState,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotPreflightRequestOutcome,
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
const implementationCommitSha = "1".repeat(40);
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
  expect(await runMigrations(databaseUrl)).toHaveLength(34);
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
    const preflightInput = {
      attemptSequence: 1,
      domain: "open_data" as const,
      operationKind: "bucket_head" as const,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      redirectSequence: 0,
      resolver: null,
    };
    await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
      confirmedAt: "2026-08-31T01:00:00.000Z",
      confirmedPlanName: "Filebase Pro or better",
      confirmerReference: "synthetic-capacity-controller",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    });
    const approvalInput = {
      approvedAt: "2026-08-31T01:01:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
          implementationCommitSha,
        ),
      implementationCommitSha,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const preflightSql = postgres(databaseUrl, { max: 1 });
    try {
      const readinessBefore = await preflightSql<
        { approval_ready: boolean; execution_ready: boolean }[]
      >`
        SELECT
          oracle_candidate_source_snapshot_preflight_is_approval_ready(
            ${fixture.plan.planId}
          ) AS approval_ready,
          oracle_candidate_source_snapshot_preflight_is_execution_ready(
            ${fixture.plan.planId}
          ) AS execution_ready
      `;
      expect(readinessBefore[0]).toEqual({
        approval_ready: true,
        execution_ready: false,
      });
    } finally {
      await preflightSql.end({ timeout: 5 });
    }
    await expect(
      admitCandidateSourceSnapshotPreflightRequest(databaseUrl, preflightInput),
    ).rejects.toThrow("requires an eligible exact plan");
    const approved = await approveCandidateSourceSnapshotDemoPlan(
      databaseUrl,
      approvalInput,
    );
    const concurrentPreflightAdmissions = await Promise.all([
      admitCandidateSourceSnapshotPreflightRequest(databaseUrl, preflightInput),
      admitCandidateSourceSnapshotPreflightRequest(databaseUrl, preflightInput),
    ]);
    expect(
      concurrentPreflightAdmissions
        .map((admission) => admission.alreadyRecorded)
        .sort(),
    ).toEqual([false, true]);
    const preflightAdmission = concurrentPreflightAdmissions.find(
      (admission) => !admission.alreadyRecorded,
    )!;

    await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
      admission: preflightAdmission,
      completedAt: "2026-08-31T01:02:00.000Z",
      outcome: "timeout_unknown",
      receiptSha256: "9".repeat(64),
    });
    await expect(
      loadCandidateSourceSnapshotPreflightRequestOutcome(
        databaseUrl,
        preflightAdmission,
      ),
    ).resolves.toEqual({
      completedAt: "2026-08-31T01:02:00.000Z",
      outcome: "timeout_unknown",
      receiptSha256: "9".repeat(64),
      requestId: preflightAdmission.requestId,
    });
    const retryAdmission = await admitCandidateSourceSnapshotPreflightRequest(
      databaseUrl,
      {
        ...preflightInput,
        attemptSequence: 2,
      },
    );
    await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
      admission: retryAdmission,
      completedAt: "2026-08-31T01:02:10.000Z",
      outcome: "retryable_failure",
      receiptSha256: "7".repeat(64),
    });
    const incompleteSql = postgres(databaseUrl, { max: 1 });
    try {
      const incomplete = await incompleteSql<{ ready: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
          ${fixture.plan.planId}
        ) AS ready
      `;
      expect(incomplete[0]?.ready).toBe(false);
    } finally {
      await incompleteSql.end({ timeout: 5 });
    }
    const successfulAdmission =
      await admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
        ...preflightInput,
        attemptSequence: 3,
      });
    const preflightOutcome = {
      admission: successfulAdmission,
      completedAt: "2026-08-31T01:02:20.000Z",
      outcome: "succeeded" as const,
      receiptSha256: "6".repeat(64),
    };
    await Promise.all([
      recordCandidateSourceSnapshotPreflightRequestOutcome(
        databaseUrl,
        preflightOutcome,
      ),
      recordCandidateSourceSnapshotPreflightRequestOutcome(
        databaseUrl,
        preflightOutcome,
      ),
    ]);
    const remainingPreflight = [
      ["open_data", "names_read", "filebase_control"],
      ["open_data", "public_resolve", "filebase_gateway"],
      ["open_data", "public_resolve", "delegated_ipfs"],
      ["query_table", "bucket_head", null],
      ["query_table", "names_read", "filebase_control"],
      ["query_table", "public_resolve", "filebase_gateway"],
      ["query_table", "public_resolve", "delegated_ipfs"],
    ] as const;
    const remainingAdmissions = await Promise.all(
      remainingPreflight.map(
        async ([domain, operationKind, resolver]) =>
          await admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
            attemptSequence: 1,
            domain,
            operationKind,
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            redirectSequence: 0,
            resolver,
          }),
      ),
    );
    for (const admission of remainingAdmissions) {
      await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
        admission,
        completedAt: "2026-08-31T01:02:30.000Z",
        outcome: "succeeded",
        receiptSha256: "8".repeat(64),
      });
    }
    const readySql = postgres(databaseUrl, { max: 1 });
    try {
      const readinessAfter = await readySql<{ ready: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
          ${fixture.plan.planId}
        ) AS ready
      `;
      expect(readinessAfter[0]?.ready).toBe(true);
    } finally {
      await readySql.end({ timeout: 5 });
    }
    await beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
      approvalId: approved.approvalId,
      executorEnabled: true,
      implementationCommitSha,
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
    };
    const closure = await recordCandidateSourceSnapshotUploadClosure(
      databaseUrl,
      closureInput,
    );
    expect(closure).toMatchObject({
      exactObjectCount: fixture.exactUpload.exactObjectCount,
      exactTotalBytes: fixture.exactUpload.exactTotalBytes,
    });
    const objectTimeSql = postgres(databaseUrl, { max: 1 });
    try {
      const rows = await objectTimeSql<{ verified_at: Date }[]>`
        SELECT max(updated_at) AS verified_at
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${fixture.plan.planId} AND status = 'verified'
      `;
      expect(closure.verifiedAt).toBe(rows[0]?.verified_at.toISOString());
    } finally {
      await objectTimeSql.end({ timeout: 5 });
    }
    await expect(
      recordCandidateSourceSnapshotUploadClosure(databaseUrl, closureInput),
    ).resolves.toEqual(closure);
    await expect(
      loadCompletedCandidateSourceSnapshotDemoReplay(databaseUrl, {
        approvalId: approved.approvalId,
        implementationCommitSha,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).resolves.toBeNull();

    const intents = await createCandidateSourceSnapshotDemoIpnsIntents(
      databaseUrl,
      {
        intendedAt: "2026-08-31T01:04:00.000Z",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    await expect(
      loadCandidateSourceSnapshotIpnsIntents(databaseUrl, {
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).resolves.toMatchObject([
      {
        approvalId: approved.approvalId,
        cutoverPosition: 1,
        domain: "open_data",
        mutationAttemptCount: 0,
        state: "intent_recorded",
        uploadClosureId: closure.closureId,
      },
      {
        approvalId: approved.approvalId,
        cutoverPosition: 2,
        domain: "query_table",
        mutationAttemptCount: 0,
        state: "intent_recorded",
        uploadClosureId: closure.closureId,
      },
    ]);
    for (const intent of intents) {
      await expect(
        loadCandidateSourceSnapshotIpnsIntentState(databaseUrl, {
          domain: intent.domain,
          intentId: intent.intentId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).resolves.toMatchObject({
        approvalId: approved.approvalId,
        domain: intent.domain,
        intentId: intent.intentId,
        mutationAttemptCount: 0,
        priorCid: intent.priorCid,
        revision: 1,
        state: "intent_recorded",
        targetCid: intent.targetCid,
        uploadClosureId: closure.closureId,
      });
    }
    await expect(
      completeCandidateSourceSnapshotDemoPlan(databaseUrl, {
        cutover: {
          openData: "target",
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          queryTable: "target",
          reason: "completed",
          rollback: "not_attempted",
          status: "completed",
        },
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        summary: {
          attemptedRequests: 0,
          recoveredByInspection: 0,
          requestCostUsd: closure.admittedRequestCostUsd,
          skippedVerified: closure.exactObjectCount,
          totalObjects: closure.exactObjectCount,
          uploadedAndVerified: 0,
        },
        uploadClosure: closure,
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
      const observations = [];
      for (const resolver of resolvers) {
        const request = await journal.startResolutionRequest(fixture.plan, {
          cycleSequence: input.cycleSequence,
          domain: input.domain,
          intentId: input.intentId,
          resolver,
        });
        eventIndex += 1;
        observations.push({
          observation: {
            classification: input.classification,
            evidenceSha256: eventIndex.toString(16).padStart(64, "0"),
            observedAt: new Date(Date.now() + eventIndex * 1_000).toISOString(),
            observedCid: input.observedCid,
            requestOutcome: "succeeded" as const,
          },
          request,
        });
      }
      await journal.recordResolutionCycle(fixture.plan, observations);
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
    for (const intent of intents) {
      await expect(
        loadCandidateSourceSnapshotIpnsIntentState(databaseUrl, {
          domain: intent.domain,
          intentId: intent.intentId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).resolves.toMatchObject({
        mutationAttemptCount: 1,
        revision: 5,
        state: "verified",
      });
    }

    await expect(
      recordCandidateSourceSnapshotRemoteCheck(databaseUrl, {
        checkKind: "plan_artifact",
        checkedAt: "2026-08-31T02:00:00.000Z",
        metrics: {},
        observedBytes: fixture.exactUpload.planArtifact.byteSize,
        observedCid: fixture.exactUpload.planArtifact.expectedCid,
        observedSha256: fixture.exactUpload.planArtifact.sha256,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("requires exact verified read receipts");

    const remotePlanObject = fixture.objects.find(
      (object) =>
        object.remoteObjectKey ===
        fixture.exactUpload.planArtifact.remoteObjectKey,
    );
    expect(remotePlanObject).toBeDefined();
    const remoteReadInput = {
      attemptSequence: 1,
      checkKind: "plan_artifact" as const,
      domain: "open_data" as const,
      logicalRequestSequence: 1,
      operationKind: "immutable_artifact_read" as const,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      redirectSequence: 0,
      remoteObjectKey: remotePlanObject!.remoteObjectKey,
    };
    const concurrentAdmissions = await Promise.all([
      admitCandidateSourceSnapshotRemoteRead(databaseUrl, remoteReadInput),
      admitCandidateSourceSnapshotRemoteRead(databaseUrl, {
        ...remoteReadInput,
        logicalRequestSequence: 2,
      }),
    ]);
    expect(
      concurrentAdmissions.map((item) => item.alreadyRecorded).sort(),
    ).toEqual([false, false]);
    await expect(
      admitCandidateSourceSnapshotRemoteRead(databaseUrl, remoteReadInput),
    ).resolves.toMatchObject({ alreadyRecorded: true });
    const [readAdmission, secondReadAdmission] = concurrentAdmissions;
    await expect(
      loadCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, readAdmission!),
    ).resolves.toEqual({
      receipt: null,
      requestId: readAdmission!.requestId,
      requestOutcome: "request_started",
    });
    const receiptInput = {
      admission: readAdmission,
      observedAt: "2026-08-31T02:00:00.000Z",
      outcome: "verified" as const,
      responseBytes: remotePlanObject!.byteSize,
      responseSha256: remotePlanObject!.sha256,
    };
    const concurrentReceipts = await Promise.all([
      recordCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, receiptInput),
      recordCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, receiptInput),
    ]);
    expect(concurrentReceipts[1]).toEqual(concurrentReceipts[0]);
    await expect(
      loadCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, readAdmission!),
    ).resolves.toMatchObject({
      receipt: {
        outcome: "verified",
        receiptSha256: concurrentReceipts[0]!.receiptSha256,
        responseBytes: remotePlanObject!.byteSize,
        responseSha256: remotePlanObject!.sha256,
      },
      requestOutcome: "succeeded",
    });
    await expect(
      recordCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, {
        admission: secondReadAdmission!,
        observedAt: "2026-08-31T02:00:01.000Z",
        outcome: "retryable_failure",
      }),
    ).resolves.toMatchObject({ outcome: "retryable_failure" });

    const redirectInput = {
      ...remoteReadInput,
      logicalRequestSequence: 2,
      redirectSequence: 1,
    };
    const redirectStateSql = postgres(databaseUrl, { max: 1 });
    try {
      const beforeLookup = await redirectStateSql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category =
           'final_credential_free_verification'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      await expect(
        loadExistingCandidateSourceSnapshotRemoteReadAdmission(
          databaseUrl,
          redirectInput,
        ),
      ).resolves.toBeNull();
      const afterMissingLookup = await redirectStateSql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category =
           'final_credential_free_verification'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(afterMissingLookup).toEqual(beforeLookup);

      const redirectAdmission = await admitCandidateSourceSnapshotRemoteRead(
        databaseUrl,
        redirectInput,
      );
      expect(redirectAdmission.alreadyRecorded).toBe(false);
      const afterAdmission = await redirectStateSql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category =
           'final_credential_free_verification'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      await expect(
        loadExistingCandidateSourceSnapshotRemoteReadAdmission(
          databaseUrl,
          redirectInput,
        ),
      ).resolves.toStrictEqual({
        ...redirectAdmission,
        alreadyRecorded: true,
      });
      const afterExistingLookup = await redirectStateSql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category =
           'final_credential_free_verification'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(afterExistingLookup).toEqual(afterAdmission);
      await expect(
        recordCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, {
          admission: redirectAdmission,
          observedAt: "2026-08-31T02:00:02.000Z",
          outcome: "verified",
          responseBytes: remotePlanObject!.byteSize,
          responseSha256: remotePlanObject!.sha256,
        }),
      ).resolves.toMatchObject({ outcome: "verified" });
    } finally {
      await redirectStateSql.end({ timeout: 5 });
    }
    await expect(
      recordCandidateSourceSnapshotRemoteReadReceipt(databaseUrl, {
        ...receiptInput,
        observedAt: "2026-08-31T02:00:03.000Z",
      }),
    ).rejects.toThrow("replay conflicts");

    const requestStateSql = postgres(databaseUrl, { max: 1 });
    try {
      const openRequests = await requestStateSql<{ count: string }[]>`
        SELECT count(*)::text AS count
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE plan_id = ${fixture.plan.planId}
          AND outcome = 'request_started'
      `;
      expect(Number(openRequests[0]?.count ?? -1)).toBe(0);
    } finally {
      await requestStateSql.end({ timeout: 5 });
    }

    const remoteCheckInput = {
      checkKind: "plan_artifact" as const,
      checkedAt: "2026-08-31T02:01:00.000Z",
      metrics: {},
      observedBytes: fixture.exactUpload.planArtifact.byteSize,
      observedCid: fixture.exactUpload.planArtifact.expectedCid,
      observedSha256: fixture.exactUpload.planArtifact.sha256,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const concurrentChecks = await Promise.all([
      recordCandidateSourceSnapshotRemoteCheck(databaseUrl, remoteCheckInput),
      recordCandidateSourceSnapshotRemoteCheck(databaseUrl, remoteCheckInput),
    ]);
    expect(concurrentChecks[1]).toEqual(concurrentChecks[0]);
    expect(concurrentChecks[0]!.evidenceSha256).toMatch(/^[a-f0-9]{64}$/);
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
      ).rejects.toThrow("requires its exact verified read receipt set");

      await expect(
        recordCandidateSourceSnapshotRemoteVerification(databaseUrl, {
          approvalId: approved.approvalId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          uploadClosureId: closure.closureId,
          verifiedAt: "2026-08-31T02:00:00.000Z",
        }),
      ).rejects.toThrow("checks are incomplete");

      await expect(
        completeCandidateSourceSnapshotDemoPlan(databaseUrl, {
          cutover: {
            openData: "target",
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            queryTable: "target",
            reason: "completed",
            rollback: "not_attempted",
            status: "completed",
          },
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          summary: {
            attemptedRequests: 0,
            recoveredByInspection: 0,
            requestCostUsd: closure.admittedRequestCostUsd,
            skippedVerified: closure.exactObjectCount,
            totalObjects: closure.exactObjectCount,
            uploadedAndVerified: 0,
          },
          uploadClosure: closure,
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
          remote_checks: 1,
          remote_verifications: 0,
        },
      ]);
    } finally {
      await verificationSql.end({ timeout: 5 });
    }
  });
});
