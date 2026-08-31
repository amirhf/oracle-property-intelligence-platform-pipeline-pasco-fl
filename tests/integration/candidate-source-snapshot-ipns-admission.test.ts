import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

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
import { recordCandidateSourceSnapshotUploadClosure } from "../../src/db/candidate-source-snapshot-completion.js";
import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import { runMigrations } from "../../src/db/migrations.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

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
  expect(await runMigrations(databaseUrl)).toHaveLength(28);
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
            ),
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        },
      );
      await beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
        approvalId: approval.approvalId,
        executorEnabled: true,
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
        verifiedAt: "2026-08-31T00:00:03.000Z",
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

      const recordCycle = async (input: {
        classification: "prior" | "target";
        cycleSequence: number;
        domain: CandidateSourceSnapshotIpnsDomain;
        intentId: string;
        observedCid: string;
        selectedResolvers?: readonly CandidateSourceSnapshotIpnsResolver[];
      }) => {
        for (const [index, resolver] of (
          input.selectedResolvers ?? resolvers
        ).entries()) {
          const request = await journal.startResolutionRequest(fixture.plan, {
            cycleSequence: input.cycleSequence,
            domain: input.domain,
            intentId: input.intentId,
            resolver,
          });
          await journal.recordResolutionObservation(fixture.plan, request, {
            classification: input.classification,
            evidenceSha256:
              `${input.cycleSequence.toString(16)}${index}`.padEnd(64, "c"),
            observedAt: new Date(Date.now() + 1_000 + index).toISOString(),
            observedCid: input.observedCid,
            requestOutcome: "succeeded",
          });
        }
      };

      await recordCycle({
        classification: "prior",
        cycleSequence: 1,
        domain: "open_data",
        intentId: openIntentId,
        observedCid: fixture.plan.targets.openData.priorCid,
      });
      await recordCycle({
        classification: "prior",
        cycleSequence: 1,
        domain: "query_table",
        intentId: queryIntentId,
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
      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'prior_confirmed', revision = 2
        WHERE plan_id = ${fixture.plan.planId}
      `;

      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'update_in_flight', revision = 3
        WHERE intent_id = ${openIntentId}
      `;
      const openAttempt = await journal.startIpnsMutationAttempt(fixture.plan, {
        attemptSequence: 1,
        direction: "update",
        domain: "open_data",
        intentId: openIntentId,
        replayAuthorizationSha256: null,
      });
      await journal.recordIpnsMutationOutcome(fixture.plan, openAttempt, {
        outcome: "acknowledged",
        receiptSha256: "d".repeat(64),
      });
      await recordCycle({
        classification: "target",
        cycleSequence: 2,
        domain: "open_data",
        intentId: openIntentId,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["filebase_control", "filebase_gateway"],
      });
      await recordCycle({
        classification: "target",
        cycleSequence: 3,
        domain: "open_data",
        intentId: openIntentId,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["delegated_ipfs"],
      });
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
          SET state = 'target_observed', revision = 4
          WHERE intent_id = ${openIntentId}
        `,
      ).rejects.toThrow("requires one complete resolution cycle");
      await recordCycle({
        classification: "target",
        cycleSequence: 2,
        domain: "open_data",
        intentId: openIntentId,
        observedCid: fixture.plan.targets.openData.targetCid,
        selectedResolvers: ["delegated_ipfs"],
      });
      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'target_observed', revision = 4
        WHERE intent_id = ${openIntentId}
      `;
      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'verified', revision = 5
        WHERE intent_id = ${openIntentId}
      `;

      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'update_in_flight', revision = 3
        WHERE intent_id = ${queryIntentId}
      `;
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
          SET state = 'rollback_recorded', revision = 6
          WHERE intent_id = ${openIntentId}
        `,
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
        observedCid: fixture.plan.targets.queryTable.priorCid,
      });
      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'update_failed_prior_confirmed', revision = 4
        WHERE intent_id = ${queryIntentId}
      `;
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
          SET state = 'rollback_recorded', revision = 6
          WHERE intent_id = ${openIntentId}
        `,
      ).resolves.toBeDefined();
      await sql`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = 'rollback_in_flight', revision = 7
        WHERE intent_id = ${openIntentId}
      `;
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

      const rows = await sql<
        {
          names_api_count: number;
          public_resolver_count: number;
          request_cost_usd: string;
          request_count: number;
          durable_requests: number;
        }[]
      >`
        SELECT accounting.names_api_count, accounting.public_resolver_count,
               accounting.request_cost_usd::text,
               accounting.request_count,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_demo_requests request
                WHERE request.plan_id = accounting.plan_id) AS durable_requests
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(rows[0]).toEqual({
        durable_requests: 19,
        names_api_count: 7,
        public_resolver_count: 9,
        request_cost_usd: "0.000085500000",
        request_count: 19,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
