import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  loadCandidateSourceSnapshotIpnsIntentState,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotIpnsRetryAuthorization,
  recordCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotIpnsDomain,
  type CandidateSourceSnapshotIpnsResolver,
} from "../../src/db/candidate-source-snapshot-demo.js";
import {
  recordCandidateSourceSnapshotUploadClosure,
  transitionCandidateSourceSnapshotIpnsIntent,
} from "../../src/db/candidate-source-snapshot-completion.js";
import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import { sha256 } from "../../src/lib/hash.js";
import { renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement } from "../../src/publication/candidate-source-snapshot-ipns-controller.js";
import { DurableIpnsBridge } from "../../src/publication/candidate-source-snapshot-remote-runtime.js";
import { runMigrations } from "../../src/db/migrations.js";
import {
  recordSuccessfulCandidateSourceSnapshotPreflight,
  syntheticCandidateSourceSnapshotDemo,
} from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_snapshot_ipns_${process.pid}_${Date.now()}`;
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
  expect(await runMigrations(databaseUrl)).toHaveLength(35);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("candidate source-snapshot IPNS request admission", () => {
  it("requires globally accounted requests, same-cycle evidence, and conclusive reverse rollback ordering", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const timeline = Date.now() + 60_000;
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    const sql = postgres(databaseUrl, { max: 1 });
    const journal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    try {
      await confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, {
        confirmedAt: "2026-08-31T00:00:01.000Z",
        confirmedPlanName: "Filebase Pro",
        confirmerReference: "synthetic-controller",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      });
      const approval = await approveCandidateSourceSnapshotDemoPlan(
        databaseUrl,
        {
          approvedAt: "2026-08-31T00:00:02.000Z",
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
      for (const [index, object] of fixture.objects.entries()) {
        const admission = await journal.startAttempt(fixture.plan, object, 1);
        const responseBytes = index + 1;
        await journal.recordVerified(fixture.plan, object, admission.attempt, {
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
        });
      }
      await recordCandidateSourceSnapshotUploadClosure(databaseUrl, {
        approvalId: approval.approvalId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      });
      const intents = await createCandidateSourceSnapshotDemoIpnsIntents(
        databaseUrl,
        {
          intendedAt: "2026-08-31T00:00:04.000Z",
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        },
      );
      const openIntentId = intents.find(
        (intent) => intent.domain === "open_data",
      )!.intentId;
      const queryIntentId = intents.find(
        (intent) => intent.domain === "query_table",
      )!.intentId;

      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_ipns_attempts (
            attempt_id, request_id, intent_id, plan_id, domain, direction,
            attempt_sequence, requested_cid, outcome
          ) VALUES (
            ${`snapshotdemoipnsattempt_${"7".repeat(32)}`},
            ${`snapshotdemorequest_${"8".repeat(32)}`}, ${openIntentId},
            ${fixture.plan.planId}, 'open_data', 'update', 1,
            ${fixture.plan.targets.openData.targetCid}, 'request_started'
          )
        `,
      ).rejects.toThrow();
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_ipns_observations (
            observation_id, request_id, intent_id, cycle_sequence, resolver,
            classification, observed_cid, evidence_sha256, observed_at
          ) VALUES (
            ${`snapshotdemoipnsobservation_${"9".repeat(32)}`},
            ${`snapshotdemorequest_${"a".repeat(32)}`}, ${openIntentId}, 1,
            'filebase_control', 'prior',
            ${fixture.plan.targets.openData.priorCid}, ${"b".repeat(64)}, now()
          )
        `,
      ).rejects.toThrow();

      const pendingCycles = new Map<
        string,
        Parameters<
          PostgresCandidateSourceSnapshotUploadJournal["recordResolutionCycle"]
        >[1]
      >();
      const recordCycle = async (input: {
        classification: "prior" | "target";
        cycleSequence: number;
        domain: CandidateSourceSnapshotIpnsDomain;
        intentId: string;
        observedAtOffset?: number;
        observedCid: string;
        selectedResolvers?: readonly CandidateSourceSnapshotIpnsResolver[];
      }) => {
        const cycleKey = `${input.intentId}:${input.cycleSequence}`;
        const cycle = [...(pendingCycles.get(cycleKey) ?? [])];
        for (const [index, resolver] of (
          input.selectedResolvers ?? resolvers
        ).entries()) {
          const request = await journal.startResolutionRequest(fixture.plan, {
            cycleSequence: input.cycleSequence,
            domain: input.domain,
            intentId: input.intentId,
            resolver,
          });
          const evidenceIndex = cycle.length;
          cycle.push({
            observation: {
              classification: input.classification,
              evidenceSha256:
                `${input.cycleSequence.toString(16)}${evidenceIndex.toString(16)}`
                  .padEnd(64, "c")
                  .slice(0, 64),
              observedAt: new Date(
                timeline + (input.observedAtOffset ?? 0) + index,
              ).toISOString(),
              observedCid: input.observedCid,
              requestOutcome: "succeeded",
            },
            request,
          });
        }
        pendingCycles.set(cycleKey, cycle);
        if (cycle.length === 3) {
          await journal.recordResolutionCycle(fixture.plan, cycle);
          pendingCycles.delete(cycleKey);
        }
      };
      const transition = async (input: {
        domain: CandidateSourceSnapshotIpnsDomain;
        expectedRevision: number;
        fromState:
          | "intent_recorded"
          | "prior_confirmed"
          | "update_in_flight"
          | "target_observed"
          | "verified"
          | "update_failed_prior_confirmed"
          | "rollback_recorded"
          | "rollback_in_flight";
        intentId: string;
        offset: number;
        toState:
          | "prior_confirmed"
          | "update_in_flight"
          | "target_observed"
          | "verified"
          | "update_failed_prior_confirmed"
          | "rollback_recorded"
          | "rollback_in_flight"
          | "rolled_back";
      }) =>
        await transitionCandidateSourceSnapshotIpnsIntent(databaseUrl, {
          domain: input.domain,
          expectedRevision: input.expectedRevision,
          fromState: input.fromState,
          intentId: input.intentId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          toState: input.toState,
          transitionedAt: new Date(timeline + input.offset).toISOString(),
        });

      await recordCycle({
        classification: "prior",
        cycleSequence: 1,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: -120_000,
        observedCid: fixture.plan.targets.openData.priorCid,
      });
      await recordCycle({
        classification: "prior",
        cycleSequence: 1,
        domain: "query_table",
        intentId: queryIntentId,
        observedAtOffset: -120_000,
        observedCid: fixture.plan.targets.queryTable.priorCid,
      });
      await expect(
        journal.startResolutionRequest(fixture.plan, {
          cycleSequence: 1,
          domain: "open_data",
          intentId: openIntentId,
          resolver: "filebase_control",
        }),
      ).resolves.toMatchObject({ alreadyRecorded: true, outcome: "succeeded" });
      await transition({
        domain: "open_data",
        expectedRevision: 1,
        fromState: "intent_recorded",
        intentId: openIntentId,
        offset: 1_000,
        toState: "prior_confirmed",
      });
      await transition({
        domain: "query_table",
        expectedRevision: 1,
        fromState: "intent_recorded",
        intentId: queryIntentId,
        offset: 1_000,
        toState: "prior_confirmed",
      });

      await transition({
        domain: "open_data",
        expectedRevision: 2,
        fromState: "prior_confirmed",
        intentId: openIntentId,
        offset: 1_100,
        toState: "update_in_flight",
      });
      const openAttempt = await journal.startIpnsMutationAttempt(fixture.plan, {
        attemptSequence: 1,
        direction: "update",
        domain: "open_data",
        intentId: openIntentId,
        replayAuthorizationSha256: null,
      });
      const closedInterrupted =
        await journal.closeInterruptedIpnsMutationAttempt(fixture.plan, {
          direction: "update",
          domain: "open_data",
          intentId: openIntentId,
        });
      expect(closedInterrupted).toMatchObject({
        admission: {
          attemptId: openAttempt.attemptId,
          request: { requestId: openAttempt.request.requestId },
        },
        receiptSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
      });
      await expect(
        journal.closeInterruptedIpnsMutationAttempt(fixture.plan, {
          direction: "update",
          domain: "open_data",
          intentId: openIntentId,
        }),
      ).resolves.toBeNull();
      await expect(
        journal.recordIpnsMutationOutcome(fixture.plan, openAttempt, {
          outcome: "acknowledged",
          receiptSha256: "d".repeat(64),
        }),
      ).rejects.toThrow("conflicts with terminal evidence");
      const updateAuthorizationBase = {
        authorizationId: `snapshotdemoreplay_${"1".repeat(32)}`,
        authorizedAttempt: 2,
        authorizedAt: "2026-08-31T00:00:05.000Z",
        authorizerReference: "synthetic-controller",
        domain: "open_data" as const,
        intentId: openIntentId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        priorCid: fixture.plan.targets.openData.priorCid,
        targetCid: fixture.plan.targets.openData.targetCid,
      };
      const updateAuthorizationStatement =
        renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
          updateAuthorizationBase,
          "update",
        );
      const updateAuthorization = {
        ...updateAuthorizationBase,
        authorizationSha256: sha256(updateAuthorizationStatement),
        authorizationStatement: updateAuthorizationStatement,
      };
      await expect(
        recordCandidateSourceSnapshotIpnsRetryAuthorization(
          databaseUrl,
          fixture.plan,
          { authorization: updateAuthorization, direction: "update" },
        ),
      ).resolves.toEqual(updateAuthorization);
      await expect(
        recordCandidateSourceSnapshotIpnsRetryAuthorization(
          databaseUrl,
          fixture.plan,
          { authorization: updateAuthorization, direction: "update" },
        ),
      ).resolves.toEqual(updateAuthorization);
      await expect(
        recordCandidateSourceSnapshotIpnsRetryAuthorization(
          databaseUrl,
          fixture.plan,
          {
            authorization: {
              ...updateAuthorization,
              authorizerReference: "conflicting-controller",
            },
            direction: "update",
          },
        ),
      ).rejects.toThrow("replay conflicts");
      await recordCycle({
        classification: "target",
        cycleSequence: 2,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 2_000,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["filebase_control", "filebase_gateway"],
      });
      await recordCycle({
        classification: "target",
        cycleSequence: 4,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 2_000,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["delegated_ipfs"],
      });
      await expect(
        transition({
          domain: "open_data",
          expectedRevision: 3,
          fromState: "update_in_flight",
          intentId: openIntentId,
          offset: 3_000,
          toState: "target_observed",
        }),
      ).rejects.toThrow("requires one complete resolution cycle");
      await recordCycle({
        classification: "target",
        cycleSequence: 2,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 2_000,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["delegated_ipfs"],
      });
      await transition({
        domain: "open_data",
        expectedRevision: 3,
        fromState: "update_in_flight",
        intentId: openIntentId,
        offset: 3_000,
        toState: "target_observed",
      });
      await transition({
        domain: "open_data",
        expectedRevision: 4,
        fromState: "target_observed",
        intentId: openIntentId,
        offset: 3_100,
        toState: "verified",
      });

      await transition({
        domain: "query_table",
        expectedRevision: 2,
        fromState: "prior_confirmed",
        intentId: queryIntentId,
        offset: 1_100,
        toState: "update_in_flight",
      });
      await expect(
        transition({
          domain: "open_data",
          expectedRevision: 5,
          fromState: "verified",
          intentId: openIntentId,
          offset: 6_000,
          toState: "rollback_recorded",
        }),
      ).rejects.toThrow("conclusive query-table non-mutation");
      const queryAttempt = await journal.startIpnsMutationAttempt(
        fixture.plan,
        {
          attemptSequence: 1,
          direction: "update",
          domain: "query_table",
          intentId: queryIntentId,
          replayAuthorizationSha256: null,
        },
      );
      await journal.recordIpnsMutationOutcome(fixture.plan, queryAttempt, {
        outcome: "terminal_failure",
        receiptSha256: "e".repeat(64),
      });
      await recordCycle({
        classification: "prior",
        cycleSequence: 2,
        domain: "query_table",
        intentId: queryIntentId,
        observedAtOffset: 4_000,
        observedCid: fixture.plan.targets.queryTable.priorCid,
      });
      await transition({
        domain: "query_table",
        expectedRevision: 3,
        fromState: "update_in_flight",
        intentId: queryIntentId,
        offset: 5_000,
        toState: "update_failed_prior_confirmed",
      });
      await recordCycle({
        classification: "target",
        cycleSequence: 4,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 5_500,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["filebase_control", "filebase_gateway"],
      });
      const bridge = new DurableIpnsBridge({
        databaseUrl,
        plan: fixture.plan,
      });
      await bridge.bindIntents(
        await Promise.all(
          (
            [
              ["open_data", openIntentId],
              ["query_table", queryIntentId],
            ] as const
          ).map(
            async ([domain, intentId]) =>
              await loadCandidateSourceSnapshotIpnsIntentState(databaseUrl, {
                domain,
                intentId,
                planId: fixture.plan.planId,
                planSha256: fixture.plan.planSha256,
              }),
          ),
        ),
      );
      await bridge.beforeFreshnessObservation({
        action: "rollback",
        attemptNumber: 1,
        authorizationId: `snapshotdemoreplay_${"3".repeat(32)}`,
        authorizationSha256: "4".repeat(64),
        commandId: `${fixture.plan.planId}:${openIntentId}:rollback:1`,
        domain: "open_data",
        intentId: openIntentId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        priorCid: fixture.plan.targets.openData.priorCid,
        targetCid: fixture.plan.targets.openData.targetCid,
      });
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
          SET state = 'rolled_back', revision = 7
          WHERE intent_id = ${openIntentId}
        `,
      ).rejects.toThrow(
        "rollback requires conclusive prior evidence and no unresolved attempt",
      );
      await recordCycle({
        classification: "prior",
        cycleSequence: 3,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 7_000,
        observedCid: fixture.plan.targets.openData.priorCid,
      });
      await expect(
        sql.begin(async (transaction) => {
          const updated = await transaction<
            { revision: number; state: string }[]
          >`
            UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
            SET state = 'rolled_back', revision = 7
            WHERE intent_id = ${openIntentId}
            RETURNING state, revision
          `;
          expect(updated[0]).toEqual({ revision: 7, state: "rolled_back" });
          throw new Error("ROLL_BACK_FRESH_PRIOR_PROBE");
        }),
      ).rejects.toThrow("ROLL_BACK_FRESH_PRIOR_PROBE");
      await transition({
        domain: "open_data",
        expectedRevision: 6,
        fromState: "rollback_recorded",
        intentId: openIntentId,
        offset: 8_000,
        toState: "rollback_in_flight",
      });
      const rollbackAttempt = await journal.startIpnsMutationAttempt(
        fixture.plan,
        {
          attemptSequence: 1,
          direction: "rollback",
          domain: "open_data",
          intentId: openIntentId,
          replayAuthorizationSha256: null,
        },
      );
      expect(rollbackAttempt.request.requestId).not.toBe(
        openAttempt.request.requestId,
      );
      await expect(
        journal.closeInterruptedIpnsMutationAttempt(fixture.plan, {
          direction: "rollback",
          domain: "open_data",
          intentId: openIntentId,
        }),
      ).resolves.toMatchObject({
        admission: { attemptId: rollbackAttempt.attemptId },
      });
      const rollbackAuthorizationBase = {
        authorizationId: `snapshotdemoreplay_${"2".repeat(32)}`,
        authorizedAttempt: 2 as const,
        authorizedAt: "2026-08-31T00:00:06.000Z",
        authorizerReference: "synthetic-controller",
        domain: "open_data" as const,
        intentId: openIntentId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        priorCid: fixture.plan.targets.openData.priorCid,
        targetCid: fixture.plan.targets.openData.targetCid,
      };
      const rollbackAuthorizationStatement =
        renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
          rollbackAuthorizationBase,
          "rollback",
        );
      const rollbackAuthorization = {
        ...rollbackAuthorizationBase,
        authorizationSha256: sha256(rollbackAuthorizationStatement),
        authorizationStatement: rollbackAuthorizationStatement,
      };
      await expect(
        recordCandidateSourceSnapshotIpnsRetryAuthorization(
          databaseUrl,
          fixture.plan,
          { authorization: rollbackAuthorization, direction: "rollback" },
        ),
      ).resolves.toEqual(rollbackAuthorization);
      await expect(
        recordCandidateSourceSnapshotIpnsRetryAuthorization(
          databaseUrl,
          fixture.plan,
          { authorization: rollbackAuthorization, direction: "rollback" },
        ),
      ).resolves.toEqual(rollbackAuthorization);
      await recordCycle({
        classification: "prior",
        cycleSequence: 5,
        domain: "open_data",
        intentId: openIntentId,
        observedAtOffset: 9_000,
        observedCid: fixture.plan.targets.openData.priorCid,
      });
      await transition({
        domain: "open_data",
        expectedRevision: 7,
        fromState: "rollback_in_flight",
        intentId: openIntentId,
        offset: 10_000,
        toState: "rolled_back",
      });

      const rows = await sql<
        {
          names_api_count: number;
          public_resolver_count: number;
          request_cost_usd: string;
          request_count: number;
          durable_requests: number;
          retry_authorizations: number;
        }[]
      >`
        SELECT accounting.names_api_count, accounting.public_resolver_count,
               accounting.request_cost_usd::text,
               accounting.request_count,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_demo_requests request
                WHERE request.plan_id = accounting.plan_id) AS durable_requests,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_demo_replay_authorizations replay_authorization
                WHERE replay_authorization.plan_id = accounting.plan_id)
                 AS retry_authorizations
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(rows[0]).toEqual({
        durable_requests: 35,
        names_api_count: 12,
        public_resolver_count: 18,
        request_cost_usd: "0.000157500000",
        request_count: 35,
        retry_authorizations: 2,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
