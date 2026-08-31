import postgres from "postgres";
import { z } from "zod";

import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";
import {
  candidateSourceSnapshotRequestCategory,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "../publication/candidate-source-snapshot-demo.js";
import type { CandidateSourceSnapshotIpnsControllerResult } from "../publication/candidate-source-snapshot-ipns-controller.js";
import type { CandidateSourceSnapshotUploadSummary } from "../publication/candidate-source-snapshot-upload.js";

const planIdentitySchema = z.strictObject({
  planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
  planSha256: z.string().regex(/^[a-f0-9]{64}$/),
});
const approvalIdSchema = z
  .string()
  .regex(/^snapshotdemoapproval_[a-f0-9]{32}$/);
const intentIdSchema = z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/);
const timestampSchema = z
  .string()
  .regex(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)
  .datetime();
const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);

const intentStateSchema = z.enum([
  "intent_recorded",
  "prior_confirmed",
  "update_in_flight",
  "target_observed",
  "verified",
  "update_ambiguous",
  "unexpected_cid",
  "update_failed_prior_confirmed",
  "rollback_recorded",
  "rollback_in_flight",
  "rollback_ambiguous",
  "rolled_back",
  "manual_intervention_required",
  "failed_terminal",
]);

export type CandidateSourceSnapshotIntentState = z.infer<
  typeof intentStateSchema
>;

const legalIntentTransitions: Readonly<
  Record<
    CandidateSourceSnapshotIntentState,
    readonly CandidateSourceSnapshotIntentState[]
  >
> = {
  failed_terminal: [],
  intent_recorded: ["prior_confirmed"],
  manual_intervention_required: [],
  prior_confirmed: ["update_in_flight"],
  rollback_ambiguous: [
    "rollback_recorded",
    "rolled_back",
    "unexpected_cid",
    "manual_intervention_required",
    "failed_terminal",
  ],
  rollback_in_flight: [
    "rolled_back",
    "rollback_ambiguous",
    "unexpected_cid",
    "failed_terminal",
  ],
  rollback_recorded: ["rollback_in_flight", "rolled_back"],
  rolled_back: [],
  target_observed: ["verified", "rollback_recorded"],
  unexpected_cid: [],
  update_ambiguous: [
    "prior_confirmed",
    "target_observed",
    "unexpected_cid",
    "update_failed_prior_confirmed",
    "manual_intervention_required",
    "failed_terminal",
  ],
  update_failed_prior_confirmed: [],
  update_in_flight: [
    "target_observed",
    "update_ambiguous",
    "unexpected_cid",
    "update_failed_prior_confirmed",
    "failed_terminal",
  ],
  verified: ["rollback_recorded"],
};

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  await transaction`SELECT pg_advisory_xact_lock(
    hashtext('oracle-candidate-source-snapshot-demo-v2'), hashtext('pasco')
  )`;
}

async function loadPlanForUpdate(
  transaction: postgres.TransactionSql,
  identity: z.infer<typeof planIdentitySchema>,
): Promise<{
  plan: CandidateSourceSnapshotDemoPlan;
  revision: number;
  state: string;
}> {
  const rows = await transaction<
    {
      plan_payload: unknown;
      plan_sha256: string;
      revision: number;
      state: string;
    }[]
  >`
    SELECT plan_payload, plan_sha256, revision, state
    FROM oracle_candidate_source_snapshot_demo_plans
    WHERE plan_id = ${identity.planId}
    FOR UPDATE
  `;
  const row = rows[0];
  if (!row || row.plan_sha256 !== identity.planSha256) {
    throw new DurableConflictError(
      "Candidate source-snapshot completion identity is not durable",
    );
  }
  const plan = validateCandidateSourceSnapshotDemoPlan(row.plan_payload);
  if (
    plan.planId !== identity.planId ||
    plan.planSha256 !== identity.planSha256
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot stored plan identity is invalid",
    );
  }
  return { plan, revision: row.revision, state: row.state };
}

export interface CandidateSourceSnapshotUploadClosure {
  admittedRequestCostUsd: number;
  admittedRequestCount: number;
  approvalId: string;
  closureId: string;
  closureSha256: string;
  exactObjectCount: number;
  exactTotalBytes: number;
  planId: string;
  planSha256: string;
  verifiedAt: string;
}

const uploadClosureSchema = planIdentitySchema.extend({
  admittedRequestCostUsd: z.number().nonnegative(),
  admittedRequestCount: z.number().int().nonnegative(),
  approvalId: approvalIdSchema,
  closureId: z.string().regex(/^snapshotdemouploadclosure_[a-f0-9]{32}$/),
  closureSha256: sha256Schema,
  exactObjectCount: z.number().int().positive(),
  exactTotalBytes: z.number().int().positive(),
  verifiedAt: timestampSchema,
});

const uploadSummarySchema = z.strictObject({
  attemptedRequests: z.number().int().nonnegative(),
  recoveredByInspection: z.number().int().nonnegative(),
  requestCostUsd: z.number().nonnegative(),
  skippedVerified: z.number().int().nonnegative(),
  totalObjects: z.number().int().positive(),
  uploadedAndVerified: z.number().int().nonnegative(),
});

const completedCutoverSchema = planIdentitySchema.extend({
  openData: z.literal("target"),
  queryTable: z.literal("target"),
  reason: z.literal("completed"),
  rollback: z.literal("not_attempted"),
  status: z.literal("completed"),
});

const completedReplaySchema = z.strictObject({
  completedRevision: z.number().int().positive(),
  cutover: completedCutoverSchema,
  summary: uploadSummarySchema,
  uploadClosure: uploadClosureSchema,
});

const completionEventSchema = planIdentitySchema.extend({
  result: completedReplaySchema,
  resultSha256: sha256Schema,
  revision: z.number().int().positive(),
  schemaVersion: z.literal("candidate-source-snapshot-completion-event-v2"),
  verificationId: z.string().regex(/^snapshotdemoverification_[a-f0-9]{32}$/),
});

interface CandidateSourceSnapshotUploadClosureRow {
  admitted_request_cost_usd: string | number;
  admitted_request_count: number;
  approval_id: string;
  closure_id: string;
  closure_sha256: string;
  exact_object_count: number;
  exact_total_bytes: string | number;
  plan_sha256: string;
  verified_at: Date;
}

function uploadClosureFromRow(
  planId: string,
  row: CandidateSourceSnapshotUploadClosureRow,
): CandidateSourceSnapshotUploadClosure {
  return {
    admittedRequestCostUsd: Number(row.admitted_request_cost_usd),
    admittedRequestCount: row.admitted_request_count,
    approvalId: row.approval_id,
    closureId: row.closure_id,
    closureSha256: row.closure_sha256,
    exactObjectCount: row.exact_object_count,
    exactTotalBytes: Number(row.exact_total_bytes),
    planId,
    planSha256: row.plan_sha256,
    verifiedAt: row.verified_at.toISOString(),
  };
}

/**
 * Reconciles the durable upload journal; callers cannot supply counts, bytes,
 * CIDs, request totals, or cost. The transaction derives every field from the
 * immutable plan and verified effect rows.
 */
export async function recordCandidateSourceSnapshotUploadClosure(
  databaseUrl: string,
  inputValue: {
    approvalId: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotUploadClosure> {
  const input = planIdentitySchema
    .extend({ approvalId: approvalIdSchema })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, state } = await loadPlanForUpdate(transaction, input);
      if (state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot upload closure requires the executing plan",
        );
      }
      const approvals = await transaction<{ approval_id: string }[]>`
        SELECT approval_id
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${input.approvalId}
          AND plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
      `;
      if (!approvals[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot upload closure lacks exact approval",
        );
      }
      const existing = await transaction<
        CandidateSourceSnapshotUploadClosureRow[]
      >`
        SELECT closure_id, plan_sha256, approval_id, exact_object_count,
               exact_total_bytes, admitted_request_count,
               admitted_request_cost_usd, closure_sha256, verified_at
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].plan_sha256 !== plan.planSha256 ||
          existing[0].approval_id !== input.approvalId
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot upload closure replay conflicts",
          );
        }
        return uploadClosureFromRow(plan.planId, existing[0]);
      }
      const aggregateRows = await transaction<
        {
          actual_bytes: string;
          actual_count: string;
          mismatch_count: string;
          unresolved_count: string;
          verified_at: Date | null;
        }[]
      >`
        SELECT count(*)::text AS actual_count,
               coalesce(sum(expected_bytes), 0)::text AS actual_bytes,
               max(updated_at) FILTER (WHERE status = 'verified') AS verified_at,
               count(*) FILTER (WHERE status <> 'verified')::text AS unresolved_count,
               count(*) FILTER (
                 WHERE status = 'verified' AND provider_cid IS DISTINCT FROM expected_cid
               )::text AS mismatch_count
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${plan.planId}
      `;
      const accountingRows = await transaction<
        { request_cost_usd: string | number; request_count: number }[]
      >`
        SELECT request_count, request_cost_usd
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      const aggregate = aggregateRows[0];
      const accounting = accountingRows[0];
      if (!aggregate || !accounting) {
        throw new DurableConflictError(
          "Candidate source-snapshot upload accounting is incomplete",
        );
      }
      if (
        Number(aggregate.unresolved_count) !== 0 ||
        Number(aggregate.mismatch_count) !== 0 ||
        aggregate.verified_at === null
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot upload closure requires every exact object verification",
        );
      }
      const verifiedAt = aggregate.verified_at.toISOString();
      const payload = {
        admittedRequestCostUsd: Number(accounting.request_cost_usd),
        admittedRequestCount: accounting.request_count,
        approvalId: input.approvalId,
        exactObjectCount: Number(aggregate.actual_count),
        exactTotalBytes: Number(aggregate.actual_bytes),
        inventoryRootCid: plan.inventory.inventoryRootCid,
        inventoryRootSha256: plan.inventory.inventoryRootSha256,
        planId: plan.planId,
        planSha256: plan.planSha256,
        providerCidMismatchCount: Number(aggregate.mismatch_count),
        unresolvedObjectCount: Number(aggregate.unresolved_count),
        verifiedAt,
      };
      const closureSha256 = canonicalJsonSha256(payload);
      const closureId = deterministicId("snapshotdemouploadclosure", [
        "candidate-source-snapshot-upload-closure-v1",
        plan.planId,
        closureSha256,
      ]);
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
            ${closureId}, ${plan.planId}, ${plan.planSha256}, ${input.approvalId},
            ${payload.exactObjectCount}, ${payload.exactTotalBytes},
            ${payload.exactObjectCount}, ${payload.exactTotalBytes},
            ${payload.unresolvedObjectCount}, ${payload.providerCidMismatchCount},
            ${payload.inventoryRootCid}, ${payload.inventoryRootSha256},
            ${payload.admittedRequestCount}, ${payload.admittedRequestCostUsd},
            ${closureSha256}, ${verifiedAt}
          )
        `;
      return {
        admittedRequestCostUsd: payload.admittedRequestCostUsd,
        admittedRequestCount: payload.admittedRequestCount,
        approvalId: input.approvalId,
        closureId,
        closureSha256,
        exactObjectCount: payload.exactObjectCount,
        exactTotalBytes: payload.exactTotalBytes,
        planId: plan.planId,
        planSha256: plan.planSha256,
        verifiedAt,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotCompletedReplay {
  completedRevision: number;
  cutover: CandidateSourceSnapshotIpnsControllerResult;
  summary: CandidateSourceSnapshotUploadSummary;
  uploadClosure: CandidateSourceSnapshotUploadClosure;
}

/**
 * Reconciles a concurrent completion loser with the immutable result written
 * by the winner. Upload counters describe one invocation, so they are not part
 * of completed-state identity: two safe executions may upload and skip
 * different objects while closing over the same durable upload evidence.
 */
export function assertCandidateSourceSnapshotCompletedReplayCompatible(
  storedValue: CandidateSourceSnapshotCompletedReplay,
  requestedValue: {
    cutover: CandidateSourceSnapshotIpnsControllerResult;
    summary: CandidateSourceSnapshotUploadSummary;
    uploadClosure: CandidateSourceSnapshotUploadClosure;
  },
): void {
  const stored = completedReplaySchema.parse(storedValue);
  const requested = z
    .strictObject({
      cutover: completedCutoverSchema,
      summary: uploadSummarySchema,
      uploadClosure: uploadClosureSchema,
    })
    .parse(requestedValue);
  if (
    canonicalJsonSha256(stored.cutover) !==
      canonicalJsonSha256(requested.cutover) ||
    canonicalJsonSha256(stored.uploadClosure) !==
      canonicalJsonSha256(requested.uploadClosure)
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot completion replay conflicts with its stored result",
    );
  }
}

/**
 * Returns the exact effect-free result of replaying an already completed plan.
 * The caller must provide the same approval and implementation commit before
 * it may skip construction of any remote adapter.
 */
export async function loadCompletedCandidateSourceSnapshotDemoReplay(
  databaseUrl: string,
  inputValue: {
    approvalId: string;
    implementationCommitSha: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotCompletedReplay | null> {
  const input = planIdentitySchema
    .extend({
      approvalId: approvalIdSchema,
      implementationCommitSha: z.string().regex(/^[a-f0-9]{40}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const current = await loadPlanForUpdate(transaction, input);
      if (current.state !== "completed") return null;
      const approvals = await transaction<{ approval_id: string }[]>`
        SELECT approval_id
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${input.approvalId}
          AND plan_id = ${input.planId} AND plan_sha256 = ${input.planSha256}
          AND implementation_commit_sha = ${input.implementationCommitSha}
      `;
      const closureRows = await transaction<
        CandidateSourceSnapshotUploadClosureRow[]
      >`
        SELECT closure_id, plan_sha256, approval_id, exact_object_count,
               exact_total_bytes, admitted_request_count,
               admitted_request_cost_usd, closure_sha256, verified_at
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${input.planId} AND plan_sha256 = ${input.planSha256}
      `;
      const closureRow = closureRows[0];
      if (
        !approvals[0] ||
        !closureRow ||
        closureRow.approval_id !== input.approvalId
      ) {
        throw new DurableConflictError(
          "Completed candidate source-snapshot replay lacks its exact approval and upload closure",
        );
      }
      const uploadClosure = uploadClosureFromRow(input.planId, closureRow);
      const evidenceRows = await transaction<
        {
          object_count: string;
          unverified_object_count: string;
          verified_intent_count: string;
          verification_count: string;
        }[]
      >`
        SELECT
          (SELECT count(*)::text
           FROM oracle_candidate_source_snapshot_demo_objects object
           WHERE object.plan_id = ${input.planId}) AS object_count,
          (SELECT count(*)::text
           FROM oracle_candidate_source_snapshot_demo_objects object
           WHERE object.plan_id = ${input.planId}
             AND object.status <> 'verified') AS unverified_object_count,
          (SELECT count(*)::text
           FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
           JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state state
             ON state.intent_id = intent.intent_id
           WHERE intent.plan_id = ${input.planId}
             AND state.state = 'verified') AS verified_intent_count,
          (SELECT count(*)::text
           FROM oracle_candidate_source_snapshot_demo_remote_verifications verification
           WHERE verification.plan_id = ${input.planId}
             AND verification.plan_sha256 = ${input.planSha256}
             AND verification.approval_id = ${input.approvalId}
             AND verification.upload_closure_id = ${uploadClosure.closureId})
            AS verification_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        WHERE accounting.plan_id = ${input.planId}
      `;
      const evidence = evidenceRows[0];
      if (
        !evidence ||
        Number(evidence.object_count) !== uploadClosure.exactObjectCount ||
        Number(evidence.unverified_object_count) !== 0 ||
        Number(evidence.verified_intent_count) !== 2 ||
        Number(evidence.verification_count) !== 1
      ) {
        throw new DurableConflictError(
          "Completed candidate source-snapshot replay evidence is incomplete",
        );
      }
      const events = await transaction<
        { event_sha256: string; metadata: unknown }[]
      >`
        SELECT event_sha256, metadata
        FROM oracle_candidate_source_snapshot_demo_events
        WHERE plan_id = ${input.planId}
          AND event_type = 'publication_completed'
      `;
      if (events.length !== 1) {
        throw new DurableConflictError(
          "Completed candidate source-snapshot replay lacks one exact stored result",
        );
      }
      const event = completionEventSchema.parse(events[0]!.metadata);
      if (
        events[0]!.event_sha256 !== canonicalJsonSha256(event) ||
        event.planId !== input.planId ||
        event.planSha256 !== input.planSha256 ||
        event.revision !== current.revision ||
        event.result.completedRevision !== current.revision ||
        event.resultSha256 !== canonicalJsonSha256(event.result) ||
        canonicalJsonSha256(event.result.uploadClosure) !==
          canonicalJsonSha256(uploadClosure) ||
        event.result.summary.requestCostUsd !==
          uploadClosure.admittedRequestCostUsd
      ) {
        throw new DurableConflictError(
          "Completed candidate source-snapshot stored result conflicts with durable evidence",
        );
      }
      return event.result;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function transitionCandidateSourceSnapshotIpnsIntent(
  databaseUrl: string,
  inputValue: {
    domain: "open_data" | "query_table";
    expectedRevision: number;
    fromState: CandidateSourceSnapshotIntentState;
    intentId: string;
    planId: string;
    planSha256: string;
    toState: CandidateSourceSnapshotIntentState;
    transitionedAt: string;
  },
): Promise<{ revision: number; state: CandidateSourceSnapshotIntentState }> {
  const input = planIdentitySchema
    .extend({
      domain: z.enum(["open_data", "query_table"]),
      expectedRevision: z.number().int().positive(),
      fromState: intentStateSchema,
      intentId: intentIdSchema,
      toState: intentStateSchema,
      transitionedAt: timestampSchema,
    })
    .parse(inputValue);
  if (!legalIntentTransitions[input.fromState].includes(input.toState)) {
    throw new DurableInputError(
      "Candidate source-snapshot IPNS transition is not legal",
    );
  }
  const transitionedAtIso = new Date(input.transitionedAt).toISOString();
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { state: planState } = await loadPlanForUpdate(transaction, input);
      if (planState !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot intent transition requires executing plan",
        );
      }
      const rows = await transaction<
        { revision: number; state: CandidateSourceSnapshotIntentState }[]
      >`
        SELECT intent_state.state, intent_state.revision
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.intent_id = ${input.intentId}
          AND intent.plan_id = ${input.planId}
          AND intent.plan_sha256 = ${input.planSha256}
          AND intent.domain = ${input.domain}
        FOR UPDATE OF intent_state
      `;
      const row = rows[0];
      if (!row) {
        throw new DurableConflictError(
          "Candidate source-snapshot intent transition lacks exact intent",
        );
      }
      if (
        row.state === input.toState &&
        row.revision === input.expectedRevision + 1
      ) {
        const events = await transaction<
          {
            event_id: string;
            event_payload: unknown;
            event_sha256: string;
            event_version: string;
            evidence_sha256: string;
            from_revision: number;
            from_state: string;
            recorded_at_iso: string;
            to_revision: number;
            to_state: string;
          }[]
        >`
          SELECT event_id, event_version, from_revision, to_revision,
                 from_state, to_state, evidence_sha256, event_payload,
                 event_sha256, recorded_at_iso
          FROM oracle_candidate_source_snapshot_demo_ipns_events
          WHERE intent_id = ${input.intentId}
            AND to_revision = ${input.expectedRevision + 1}
        `;
        const event = events[0];
        if (event) {
          const replayPayload = {
            domain: input.domain,
            evidenceSha256: event.evidence_sha256,
            fromState: input.fromState,
            intentId: input.intentId,
            planId: input.planId,
            planSha256: input.planSha256,
            revision: input.expectedRevision + 1,
            toState: input.toState,
            transitionedAt: transitionedAtIso,
          };
          const replayEventSha256 = canonicalJsonSha256(replayPayload);
          const replayEventId = deterministicId("snapshotdemoipnsevent", [
            "candidate-source-snapshot-intent-transition-v1",
            input.intentId,
            replayEventSha256,
          ]);
          if (
            event.event_id === replayEventId &&
            event.event_version ===
              "candidate-source-snapshot-intent-transition-v1" &&
            event.from_revision === input.expectedRevision &&
            event.to_revision === input.expectedRevision + 1 &&
            event.from_state === input.fromState &&
            event.to_state === input.toState &&
            event.recorded_at_iso === transitionedAtIso &&
            event.event_sha256 === replayEventSha256 &&
            canonicalJsonSha256(event.event_payload) ===
              canonicalJsonSha256(replayPayload)
          ) {
            return row;
          }
        }
        throw new DurableConflictError(
          "Candidate source-snapshot intent transition replay conflicts with durable evidence",
        );
      }
      if (
        row.state !== input.fromState ||
        row.revision !== input.expectedRevision
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot intent transition lost its revision",
        );
      }
      const evidenceRows = await transaction<
        {
          evidence_id: string;
          evidence_kind: "attempt" | "observation";
          evidence_sha256: string;
          outcome: string;
        }[]
      >`
        SELECT observation.observation_id AS evidence_id,
               'observation'::text AS evidence_kind,
               observation.evidence_sha256,
               observation.classification AS outcome
        FROM oracle_candidate_source_snapshot_demo_ipns_observations observation
        WHERE observation.intent_id = ${input.intentId}
        UNION ALL
        SELECT attempt.attempt_id AS evidence_id,
               'attempt'::text AS evidence_kind,
               coalesce(attempt.receipt_sha256, repeat('0', 64)) AS evidence_sha256,
               attempt.outcome
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
        WHERE attempt.intent_id = ${input.intentId}
        ORDER BY evidence_kind, evidence_id
      `;
      const evidenceSha256 = canonicalJsonSha256(
        evidenceRows.map((evidence) => ({
          evidenceId: evidence.evidence_id,
          evidenceKind: evidence.evidence_kind,
          evidenceSha256: evidence.evidence_sha256,
          outcome: evidence.outcome,
        })),
      );
      const eventPayload = {
        domain: input.domain,
        evidenceSha256,
        fromState: input.fromState,
        intentId: input.intentId,
        planId: input.planId,
        planSha256: input.planSha256,
        revision: input.expectedRevision + 1,
        toState: input.toState,
        transitionedAt: transitionedAtIso,
      };
      const eventSha256 = canonicalJsonSha256(eventPayload);
      const eventId = deterministicId("snapshotdemoipnsevent", [
        "candidate-source-snapshot-intent-transition-v1",
        input.intentId,
        eventSha256,
      ]);
      const updated = await transaction<
        { revision: number; state: CandidateSourceSnapshotIntentState }[]
      >`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_intent_state
        SET state = ${input.toState}, revision = revision + 1,
            updated_at = ${transitionedAtIso}
        WHERE intent_id = ${input.intentId}
          AND state = ${input.fromState} AND revision = ${input.expectedRevision}
        RETURNING state, revision
      `;
      if (!updated[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot intent transition was not persisted",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_ipns_events (
          event_id, intent_id, from_state, to_state, event_sha256, recorded_at,
          event_version, from_revision, to_revision, evidence_sha256,
          event_payload, recorded_at_iso
        ) VALUES (
          ${eventId}, ${input.intentId}, ${input.fromState}, ${input.toState},
          ${eventSha256}, ${transitionedAtIso},
          'candidate-source-snapshot-intent-transition-v1',
          ${input.expectedRevision}, ${input.expectedRevision + 1},
          ${evidenceSha256},
          ${transaction.json(eventPayload as postgres.JSONValue)},
          ${transitionedAtIso}
        )
      `;
      return updated[0];
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const remoteCheckKindSchema = z.enum([
  "plan_artifact",
  "manifest",
  "inventory",
  "open_data_graph",
  "query_table",
  "coverage",
  "fixture_exclusion",
]);
export type CandidateSourceSnapshotRemoteCheckKind = z.infer<
  typeof remoteCheckKindSchema
>;
export const CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS =
  remoteCheckKindSchema.options;

const remoteReadOperationKindSchema = z.enum([
  "immutable_artifact_read",
  "immutable_artifact_stat",
  "immutable_artifact_range_read",
  "verification_read",
  "search_read",
  "property_read",
]);
const remoteReadOutcomeSchema = z.enum([
  "verified",
  "retryable_failure",
  "timeout_unknown",
  "terminal_failure",
]);

const remoteReadAdmissionInputSchema = planIdentitySchema
  .extend({
    attemptSequence: z.number().int().min(1).max(3),
    byteRangeEnd: z.number().int().nonnegative().nullable().default(null),
    byteRangeStart: z.number().int().nonnegative().nullable().default(null),
    checkKind: remoteCheckKindSchema,
    domain: z.enum(["open_data", "query_table"]),
    logicalRequestSequence: z.number().int().positive().max(8_303),
    operationKind: remoteReadOperationKindSchema,
    redirectSequence: z.number().int().min(0).max(2),
    remoteObjectKey: z.string().min(1).max(2_048),
  })
  .superRefine((value, context) => {
    const hasStart = value.byteRangeStart !== null;
    const hasEnd = value.byteRangeEnd !== null;
    if (
      hasStart !== hasEnd ||
      (hasStart && value.byteRangeEnd! < value.byteRangeStart!) ||
      (value.operationKind === "immutable_artifact_range_read") !== hasStart
    ) {
      context.addIssue({
        code: "custom",
        message: "remote read range and operation kind are inconsistent",
      });
    }
  });

type CandidateSourceSnapshotRemoteReadInput = z.infer<
  typeof remoteReadAdmissionInputSchema
>;

function candidateSourceSnapshotRemoteReadIdentity(
  planId: string,
  input: CandidateSourceSnapshotRemoteReadInput,
): { logicalRequestId: string; requestId: string } {
  const logicalRequestId = deterministicId("snapshotdemologicalrequest", [
    "candidate-source-snapshot-final-read-v1",
    planId,
    input.checkKind,
    input.domain,
    input.remoteObjectKey,
    input.operationKind,
    String(input.logicalRequestSequence),
    String(input.byteRangeStart ?? -1),
    String(input.byteRangeEnd ?? -1),
  ]);
  return {
    logicalRequestId,
    requestId: deterministicId("snapshotdemorequest", [
      "candidate-source-snapshot-final-read-attempt-v1",
      planId,
      logicalRequestId,
      String(input.attemptSequence),
      String(input.redirectSequence),
    ]),
  };
}

export interface CandidateSourceSnapshotRemoteReadAdmission {
  alreadyRecorded: boolean;
  attemptSequence: number;
  byteRangeEnd: number | null;
  byteRangeStart: number | null;
  checkKind: CandidateSourceSnapshotRemoteCheckKind;
  domain: "open_data" | "query_table";
  expectedBytes: number;
  expectedCid: string;
  expectedSha256: string;
  logicalRequestId: string;
  logicalRequestSequence: number;
  operationKind: z.infer<typeof remoteReadOperationKindSchema>;
  outcome:
    | "request_started"
    | "succeeded"
    | "retryable_failure"
    | "timeout_unknown"
    | "terminal_failure";
  planId: string;
  planSha256: string;
  redirectSequence: number;
  remoteObjectKey: string;
  requestId: string;
}

async function loadRemoteReadObject(
  transaction: postgres.TransactionSql,
  input: {
    domain: "open_data" | "query_table";
    planId: string;
    remoteObjectKey: string;
  },
): Promise<{
  expectedBytes: number;
  expectedCid: string;
  expectedSha256: string;
}> {
  const rows = await transaction<
    {
      expected_bytes: string | number;
      expected_cid: string;
      expected_sha256: string;
    }[]
  >`
    SELECT expected_bytes, expected_cid, expected_sha256
    FROM oracle_candidate_source_snapshot_demo_objects
    WHERE plan_id = ${input.planId} AND domain = ${input.domain}
      AND remote_object_key = ${input.remoteObjectKey}
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot remote read is not in the immutable inventory",
    );
  }
  return {
    expectedBytes: Number(rows[0].expected_bytes),
    expectedCid: rows[0].expected_cid,
    expectedSha256: rows[0].expected_sha256,
  };
}

/**
 * Durably reserves one exact credential-free HTTP exchange before transport.
 * Logical sequence is controller-derived and bounded by the immutable plan;
 * callers never supply a durable identifier or cost.
 */
export async function admitCandidateSourceSnapshotRemoteRead(
  databaseUrl: string,
  inputValue: z.input<typeof remoteReadAdmissionInputSchema>,
): Promise<CandidateSourceSnapshotRemoteReadAdmission> {
  const input = remoteReadAdmissionInputSchema.parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, state } = await loadPlanForUpdate(transaction, input);
      if (state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read requires executing state",
        );
      }
      assertRemoteVerificationEnvelopeAuthorized(plan);
      const object = await loadRemoteReadObject(transaction, input);
      if (
        input.byteRangeEnd !== null &&
        input.byteRangeEnd >= object.expectedBytes
      ) {
        throw new DurableInputError(
          "Candidate source-snapshot remote read range exceeds the immutable object",
        );
      }
      const { logicalRequestId, requestId } =
        candidateSourceSnapshotRemoteReadIdentity(plan.planId, input);
      const existing = await transaction<
        {
          attempt_sequence: number;
          logical_request_id: string;
          operation_kind: z.infer<typeof remoteReadOperationKindSchema>;
          outcome: CandidateSourceSnapshotRemoteReadAdmission["outcome"];
          redirect_sequence: number;
          remote_object_key: string;
          request_id: string;
        }[]
      >`
        SELECT request_id, logical_request_id, operation_kind,
               attempt_sequence, redirect_sequence, remote_object_key, outcome
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE request_id = ${requestId}
        FOR UPDATE
      `;
      if (existing[0]) {
        const row = existing[0];
        if (
          row.logical_request_id !== logicalRequestId ||
          row.operation_kind !== input.operationKind ||
          row.attempt_sequence !== input.attemptSequence ||
          row.redirect_sequence !== input.redirectSequence ||
          row.remote_object_key !== input.remoteObjectKey
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot remote read replay conflicts",
          );
        }
        return {
          ...input,
          ...object,
          alreadyRecorded: true,
          logicalRequestId,
          outcome: row.outcome,
          requestId,
        };
      }
      const accounting = await transaction<
        {
          request_cost_usd: string | number;
          request_count: number;
          revision: number;
        }[]
      >`
        SELECT request_count, request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      const row = accounting[0];
      if (
        !row ||
        row.request_count >= plan.requestEnvelope.maximumTotalRequests
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot total request allowance is exhausted",
        );
      }
      const requestCostUsd = 0.0045 / 1_000;
      if (
        Number(row.request_cost_usd) + requestCostUsd >
        plan.costEnvelope.requestUsd.maximumAttempts
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read cost allowance is exhausted",
        );
      }
      const accountingUpdate = await transaction<{ plan_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_accounting
        SET request_count = request_count + 1,
            class_b_read_count = class_b_read_count + 1,
            request_cost_usd = request_cost_usd + ${requestCostUsd},
            revision = revision + 1
        WHERE plan_id = ${plan.planId} AND revision = ${row.revision}
        RETURNING plan_id
      `;
      if (!accountingUpdate[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read lost global admission",
        );
      }
      const categoryUpdate = await transaction<{ plan_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_request_categories
        SET consumed_request_count = consumed_request_count + 1,
            request_cost_usd = request_cost_usd + ${requestCostUsd},
            revision = revision + 1
        WHERE plan_id = ${plan.planId}
          AND request_category = 'final_credential_free_verification'
          AND consumed_request_count < planned_maximum_request_count
        RETURNING plan_id
      `;
      if (!categoryUpdate[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot final-verification allowance is exhausted",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_requests (
          request_id, plan_id, operation_class, operation_kind, domain,
          remote_object_key, request_cost_usd, outcome, request_category,
          logical_request_id, attempt_sequence, redirect_sequence
        ) VALUES (
          ${requestId}, ${plan.planId}, 'class_b_read', ${input.operationKind},
          ${input.domain}, ${input.remoteObjectKey}, ${requestCostUsd},
          'request_started',
          'final_credential_free_verification', ${logicalRequestId},
          ${input.attemptSequence}, ${input.redirectSequence}
        )
      `;
      return {
        ...input,
        ...object,
        alreadyRecorded: false,
        logicalRequestId,
        outcome: "request_started" as const,
        requestId,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

/**
 * Reads one exact existing immutable-read admission without reserving request
 * or cost allowance. Redirect recovery uses this to follow only a child that
 * the original execution already admitted; a missing child remains missing.
 */
export async function loadExistingCandidateSourceSnapshotRemoteReadAdmission(
  databaseUrl: string,
  inputValue: z.input<typeof remoteReadAdmissionInputSchema>,
): Promise<CandidateSourceSnapshotRemoteReadAdmission | null> {
  const input = remoteReadAdmissionInputSchema.parse(inputValue);
  const { logicalRequestId, requestId } =
    candidateSourceSnapshotRemoteReadIdentity(input.planId, input);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<
      {
        attempt_sequence: number;
        expected_bytes: number | string;
        expected_cid: string;
        expected_sha256: string;
        logical_request_id: string;
        operation_kind: z.infer<typeof remoteReadOperationKindSchema>;
        outcome: CandidateSourceSnapshotRemoteReadAdmission["outcome"];
        plan_sha256: string;
        redirect_sequence: number;
        remote_object_key: string;
        request_id: string;
      }[]
    >`
      SELECT request.request_id, request.logical_request_id,
             request.operation_kind, request.attempt_sequence,
             request.redirect_sequence, request.remote_object_key,
             request.outcome, plan.plan_sha256, object.expected_bytes,
             object.expected_cid, object.expected_sha256
      FROM oracle_candidate_source_snapshot_demo_requests request
      JOIN oracle_candidate_source_snapshot_demo_plans plan
        ON plan.plan_id = request.plan_id
      JOIN oracle_candidate_source_snapshot_demo_objects object
        ON object.plan_id = request.plan_id
       AND object.domain = request.domain
       AND object.remote_object_key = request.remote_object_key
      WHERE request.request_id = ${requestId}
        AND request.plan_id = ${input.planId}
        AND request.request_category =
          'final_credential_free_verification'
    `;
    const row = rows[0];
    if (!row) return null;
    if (
      row.plan_sha256 !== input.planSha256 ||
      row.logical_request_id !== logicalRequestId ||
      row.operation_kind !== input.operationKind ||
      row.attempt_sequence !== input.attemptSequence ||
      row.redirect_sequence !== input.redirectSequence ||
      row.remote_object_key !== input.remoteObjectKey
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot existing remote read conflicts",
      );
    }
    return {
      ...input,
      alreadyRecorded: true,
      expectedBytes: Number(row.expected_bytes),
      expectedCid: row.expected_cid,
      expectedSha256: row.expected_sha256,
      logicalRequestId,
      outcome: row.outcome,
      requestId,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotRemoteReadReceipt {
  outcome: z.infer<typeof remoteReadOutcomeSchema>;
  receiptId: string;
  receiptSha256: string;
  requestId: string;
}

export interface CandidateSourceSnapshotRemoteReadResumeState {
  receipt: null | {
    observedAt: string;
    outcome: z.infer<typeof remoteReadOutcomeSchema>;
    receiptId: string;
    receiptSha256: string;
    responseBytes: number | null;
    responseSha256: string | null;
  };
  requestId: string;
  requestOutcome: CandidateSourceSnapshotRemoteReadAdmission["outcome"];
}

/** Loads only durable admission/receipt state; response bytes are never stored. */
export async function loadCandidateSourceSnapshotRemoteReadReceipt(
  databaseUrl: string,
  admission: CandidateSourceSnapshotRemoteReadAdmission,
): Promise<CandidateSourceSnapshotRemoteReadResumeState> {
  planIdentitySchema.parse({
    planId: admission.planId,
    planSha256: admission.planSha256,
  });
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<
      {
        attempt_sequence: number;
        byte_range_end: string | null;
        byte_range_start: string | null;
        check_kind: CandidateSourceSnapshotRemoteCheckKind | null;
        domain: "open_data" | "query_table";
        expected_bytes: string | null;
        expected_cid: string | null;
        expected_sha256: string | null;
        logical_request_id: string;
        observed_at: Date | null;
        operation_kind: z.infer<typeof remoteReadOperationKindSchema>;
        plan_sha256: string;
        receipt_outcome: z.infer<typeof remoteReadOutcomeSchema> | null;
        receipt_payload: unknown | null;
        receipt_response_bytes: string | null;
        receipt_response_sha256: string | null;
        receipt_sha256: string | null;
        redirect_sequence: number;
        remote_object_key: string;
        request_id: string;
        request_outcome: CandidateSourceSnapshotRemoteReadAdmission["outcome"];
        request_receipt_sha256: string | null;
        verification_receipt_id: string | null;
      }[]
    >`
      SELECT request.request_id, request.logical_request_id,
             request.operation_kind, request.domain, request.remote_object_key,
             plan.plan_sha256,
             request.attempt_sequence, request.redirect_sequence,
             request.outcome AS request_outcome,
             request.receipt_sha256 AS request_receipt_sha256,
             receipt.verification_receipt_id, receipt.check_kind,
             receipt.expected_cid, receipt.expected_sha256,
             receipt.expected_bytes::text, receipt.byte_range_start::text,
             receipt.byte_range_end::text,
             receipt.outcome AS receipt_outcome,
             receipt.response_bytes::text AS receipt_response_bytes,
             receipt.response_sha256 AS receipt_response_sha256,
             receipt.receipt_payload, receipt.receipt_sha256,
             receipt.observed_at
      FROM oracle_candidate_source_snapshot_demo_requests request
      JOIN oracle_candidate_source_snapshot_demo_plans plan
        ON plan.plan_id = request.plan_id
      LEFT JOIN oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
        ON receipt.request_id = request.request_id
      WHERE request.request_id = ${admission.requestId}
        AND request.plan_id = ${admission.planId}
        AND request.request_category = 'final_credential_free_verification'
    `;
    const row = rows[0];
    if (
      !row ||
      row.plan_sha256 !== admission.planSha256 ||
      row.logical_request_id !== admission.logicalRequestId ||
      row.operation_kind !== admission.operationKind ||
      row.domain !== admission.domain ||
      row.remote_object_key !== admission.remoteObjectKey ||
      row.attempt_sequence !== admission.attemptSequence ||
      row.redirect_sequence !== admission.redirectSequence
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot remote read resume lacks its exact admission",
      );
    }
    if (row.verification_receipt_id === null) {
      if (
        row.request_outcome !== "request_started" ||
        row.request_receipt_sha256 !== null
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read terminal request lacks its receipt",
        );
      }
      return {
        receipt: null,
        requestId: row.request_id,
        requestOutcome: row.request_outcome,
      };
    }
    const expectedRequestOutcome =
      row.receipt_outcome === "verified" ? "succeeded" : row.receipt_outcome;
    if (
      row.receipt_outcome === null ||
      row.receipt_sha256 === null ||
      row.receipt_payload === null ||
      row.observed_at === null ||
      row.check_kind !== admission.checkKind ||
      row.expected_cid !== admission.expectedCid ||
      row.expected_sha256 !== admission.expectedSha256 ||
      Number(row.expected_bytes) !== admission.expectedBytes ||
      (row.byte_range_start === null ? null : Number(row.byte_range_start)) !==
        admission.byteRangeStart ||
      (row.byte_range_end === null ? null : Number(row.byte_range_end)) !==
        admission.byteRangeEnd ||
      row.request_outcome !== expectedRequestOutcome ||
      row.request_receipt_sha256 !== row.receipt_sha256 ||
      canonicalJsonSha256(row.receipt_payload) !== row.receipt_sha256
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot remote read resume receipt binding is invalid",
      );
    }
    return {
      receipt: {
        observedAt: row.observed_at.toISOString(),
        outcome: row.receipt_outcome,
        receiptId: row.verification_receipt_id,
        receiptSha256: row.receipt_sha256,
        responseBytes:
          row.receipt_response_bytes === null
            ? null
            : Number(row.receipt_response_bytes),
        responseSha256: row.receipt_response_sha256,
      },
      requestId: row.request_id,
      requestOutcome: row.request_outcome,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateSourceSnapshotRemoteReadReceipt(
  databaseUrl: string,
  inputValue: {
    admission: CandidateSourceSnapshotRemoteReadAdmission;
    observedAt: string;
    outcome: z.infer<typeof remoteReadOutcomeSchema>;
    responseBytes?: number | null;
    responseSha256?: string | null;
  },
): Promise<CandidateSourceSnapshotRemoteReadReceipt> {
  const admission = inputValue.admission;
  const observedAt = timestampSchema.parse(inputValue.observedAt);
  const outcome = remoteReadOutcomeSchema.parse(inputValue.outcome);
  const responseBytes = z
    .number()
    .int()
    .nonnegative()
    .nullable()
    .parse(inputValue.responseBytes ?? null);
  const responseSha256 = sha256Schema
    .nullable()
    .parse(inputValue.responseSha256 ?? null);
  if (
    (outcome === "verified" &&
      (responseBytes === null || responseSha256 === null)) ||
    (outcome !== "verified" &&
      (responseBytes !== null || responseSha256 !== null))
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot remote read outcome is inconsistent",
    );
  }
  if (outcome === "verified") {
    const expectedResponseBytes =
      admission.byteRangeStart === null
        ? admission.expectedBytes
        : admission.byteRangeEnd! - admission.byteRangeStart + 1;
    if (
      responseBytes !== expectedResponseBytes ||
      (admission.byteRangeStart === null &&
        responseSha256 !== admission.expectedSha256)
    ) {
      throw new DurableInputError(
        "Candidate source-snapshot verified response does not match its immutable binding",
      );
    }
  }
  const requestOutcome = outcome === "verified" ? "succeeded" : outcome;
  const payload = {
    byteRangeEnd: admission.byteRangeEnd,
    byteRangeStart: admission.byteRangeStart,
    checkKind: admission.checkKind,
    domain: admission.domain,
    expectedBytes: admission.expectedBytes,
    expectedCid: admission.expectedCid,
    expectedSha256: admission.expectedSha256,
    observedAt,
    outcome,
    planId: admission.planId,
    remoteObjectKey: admission.remoteObjectKey,
    requestId: admission.requestId,
    responseBytes,
    responseSha256,
    schemaVersion: "candidate-source-snapshot-remote-read-receipt-v1",
  };
  const receiptSha256 = canonicalJsonSha256(payload);
  const receiptId = deterministicId("snapshotdemoverificationreceipt", [
    "candidate-source-snapshot-remote-read-receipt-v1",
    admission.planId,
    admission.requestId,
    receiptSha256,
  ]);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, state } = await loadPlanForUpdate(transaction, admission);
      if (state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read receipt requires executing state",
        );
      }
      assertRemoteVerificationEnvelopeAuthorized(plan);
      const existing = await transaction<
        {
          outcome: z.infer<typeof remoteReadOutcomeSchema>;
          receipt_sha256: string;
          verification_receipt_id: string;
        }[]
      >`
        SELECT verification_receipt_id, receipt_sha256, outcome
        FROM oracle_candidate_source_snapshot_demo_remote_read_receipts
        WHERE request_id = ${admission.requestId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].verification_receipt_id !== receiptId ||
          existing[0].receipt_sha256 !== receiptSha256 ||
          existing[0].outcome !== outcome
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot remote read receipt replay conflicts",
          );
        }
        return {
          outcome,
          receiptId,
          receiptSha256,
          requestId: admission.requestId,
        };
      }
      const requests = await transaction<
        {
          attempt_sequence: number;
          logical_request_id: string;
          operation_kind: string;
          outcome: string;
          redirect_sequence: number;
          remote_object_key: string;
        }[]
      >`
        SELECT logical_request_id, operation_kind, attempt_sequence,
               redirect_sequence, remote_object_key, outcome
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE request_id = ${admission.requestId}
          AND plan_id = ${plan.planId}
          AND request_category = 'final_credential_free_verification'
        FOR UPDATE
      `;
      const request = requests[0];
      if (
        !request ||
        request.outcome !== "request_started" ||
        request.logical_request_id !== admission.logicalRequestId ||
        request.operation_kind !== admission.operationKind ||
        request.attempt_sequence !== admission.attemptSequence ||
        request.redirect_sequence !== admission.redirectSequence ||
        request.remote_object_key !== admission.remoteObjectKey
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read receipt lacks exact admission",
        );
      }
      const completed = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${requestOutcome}, receipt_sha256 = ${receiptSha256},
            completed_at = ${observedAt}
        WHERE request_id = ${admission.requestId}
          AND outcome = 'request_started'
        RETURNING request_id
      `;
      if (!completed[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote read receipt lost its admission",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_remote_read_receipts (
          verification_receipt_id, request_id, plan_id, check_kind, domain,
          remote_object_key, expected_cid, expected_sha256, expected_bytes,
          byte_range_start, byte_range_end, outcome, response_bytes,
          response_sha256, receipt_payload, receipt_sha256, observed_at,
          observed_at_iso
        ) VALUES (
          ${receiptId}, ${admission.requestId}, ${plan.planId},
          ${admission.checkKind}, ${admission.domain},
          ${admission.remoteObjectKey}, ${admission.expectedCid},
          ${admission.expectedSha256}, ${admission.expectedBytes},
          ${admission.byteRangeStart}, ${admission.byteRangeEnd}, ${outcome},
          ${responseBytes}, ${responseSha256}, ${transaction.json(payload)},
          ${receiptSha256}, ${observedAt}, ${observedAt}
        )
      `;
      return {
        outcome,
        receiptId,
        receiptSha256,
        requestId: admission.requestId,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function assertRemoteVerificationEnvelopeAuthorized(
  plan: CandidateSourceSnapshotDemoPlan,
): void {
  const { maximumRequests, successfulRequests } =
    candidateSourceSnapshotRequestCategory(
      plan.requestEnvelope,
      "final_credential_free_verification",
    );
  const verification = plan.requestEnvelope.finalVerification;
  if (
    successfulRequests !== verification.logicalRequests ||
    maximumRequests !==
      verification.deterministicRequiredMaximumRequests +
        verification.protectedHeadroomRequests ||
    verification.deterministicRequiredMaximumRequests !==
      verification.logicalRequests *
        verification.maximumTransportAttemptsPerLogicalRequest *
        (verification.maximumRedirectsPerAttempt + 1) ||
    verification.protectedHeadroomRequests <= 0 ||
    successfulRequests < CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS.length
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot credential-free remote verification is not authorized by the immutable request envelope",
    );
  }
}

interface RemoteArtifactExpectation {
  expectedBytes: number;
  expectedCid: string;
  expectedSha256: string;
  metrics: unknown;
}

async function loadObjectExpectation(
  transaction: postgres.TransactionSql,
  planId: string,
  expectedCid: string,
): Promise<{ expectedBytes: number; expectedSha256: string }> {
  const rows = await transaction<
    { expected_bytes: string | number; expected_sha256: string }[]
  >`
    SELECT expected_bytes, expected_sha256
    FROM oracle_candidate_source_snapshot_demo_objects
    WHERE plan_id = ${planId} AND expected_cid = ${expectedCid}
    ORDER BY remote_object_key
    LIMIT 1
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot remote check lacks its immutable object",
    );
  }
  return {
    expectedBytes: Number(rows[0].expected_bytes),
    expectedSha256: rows[0].expected_sha256,
  };
}

async function remoteCheckExpectation(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  checkKind: CandidateSourceSnapshotRemoteCheckKind,
): Promise<RemoteArtifactExpectation> {
  if (checkKind === "plan_artifact") {
    const rows = await transaction<
      {
        plan_artifact_bytes: string | number;
        plan_artifact_cid: string;
        plan_artifact_sha256: string;
      }[]
    >`
      SELECT plan_artifact_bytes, plan_artifact_cid, plan_artifact_sha256
      FROM oracle_candidate_source_snapshot_demo_plans
      WHERE plan_id = ${plan.planId}
    `;
    if (!rows[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot plan artifact binding is missing",
      );
    }
    return {
      expectedBytes: Number(rows[0].plan_artifact_bytes),
      expectedCid: rows[0].plan_artifact_cid,
      expectedSha256: rows[0].plan_artifact_sha256,
      metrics: {},
    };
  }
  if (checkKind === "manifest") {
    const artifact = plan.controlArtifacts.manifestIndex;
    return {
      expectedBytes: artifact.byteSize,
      expectedCid: artifact.expectedCid,
      expectedSha256: artifact.sha256,
      metrics: {},
    };
  }
  if (checkKind === "inventory") {
    const reference = plan.controlArtifacts.objectInventory;
    return {
      expectedBytes: reference.indexArtifact.byteSize,
      expectedCid: reference.indexArtifact.expectedCid,
      expectedSha256: reference.indexArtifact.sha256,
      metrics: {
        entryCount: reference.entryCount,
        integrityRootSha256: reference.integrityRootSha256,
        shardCount: reference.shardCount,
      },
    };
  }
  const expectedCid =
    checkKind === "query_table"
      ? plan.targets.queryTable.targetCid
      : plan.targets.openData.targetCid;
  const object = await loadObjectExpectation(
    transaction,
    plan.planId,
    expectedCid,
  );
  const metrics =
    checkKind === "open_data_graph"
      ? { propertyCount: plan.coverage.activeProperties, traversalValid: true }
      : checkKind === "query_table"
        ? {
            distinctPropertyIdCount: plan.coverage.activeProperties,
            nullPropertyIdCount: 0,
            propertyCidCorrespondence: true,
            propertyCount: plan.coverage.activeProperties,
          }
        : checkKind === "coverage"
          ? plan.coverage
          : { fixtureMatchCount: 0 };
  return { ...object, expectedCid, metrics };
}

export interface CandidateSourceSnapshotRemoteCheck {
  checkId: string;
  checkKind: CandidateSourceSnapshotRemoteCheckKind;
  checkSha256: string;
  evidenceSha256: string;
}

interface CandidateSourceSnapshotRemoteReadReceiptSummary {
  outcome: z.infer<typeof remoteReadOutcomeSchema>;
  receiptId: string;
  receiptSha256: string;
  requestId: string;
}

async function loadVerifiedRemoteReadReceiptSet(
  transaction: postgres.TransactionSql,
  input: {
    checkKind: CandidateSourceSnapshotRemoteCheckKind;
    planId: string;
  },
): Promise<{
  receiptCount: number;
  receiptSetSha256: string;
  receipts: CandidateSourceSnapshotRemoteReadReceiptSummary[];
}> {
  const rows = await transaction<
    {
      outcome: z.infer<typeof remoteReadOutcomeSchema>;
      receipt_sha256: string;
      request_id: string;
      verification_receipt_id: string;
    }[]
  >`
    SELECT verification_receipt_id, request_id, receipt_sha256, outcome
    FROM oracle_candidate_source_snapshot_demo_remote_read_receipts
    WHERE plan_id = ${input.planId} AND check_kind = ${input.checkKind}
    ORDER BY request_id
  `;
  if (rows.length === 0 || !rows.some((row) => row.outcome === "verified")) {
    throw new DurableConflictError(
      "Candidate source-snapshot remote check requires exact verified read receipts",
    );
  }
  const receipts = rows.map((row) => ({
    outcome: row.outcome,
    receiptId: row.verification_receipt_id,
    receiptSha256: row.receipt_sha256,
    requestId: row.request_id,
  }));
  return {
    receiptCount: receipts.length,
    receiptSetSha256: canonicalJsonSha256(receipts),
    receipts,
  };
}

export async function recordCandidateSourceSnapshotRemoteCheck(
  databaseUrl: string,
  inputValue: {
    checkKind: CandidateSourceSnapshotRemoteCheckKind;
    checkedAt: string;
    metrics: unknown;
    observedBytes: number;
    observedCid: string;
    observedSha256: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotRemoteCheck> {
  const input = planIdentitySchema
    .extend({
      checkKind: remoteCheckKindSchema,
      checkedAt: timestampSchema,
      metrics: z.unknown(),
      observedBytes: z.number().int().positive(),
      observedCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
      observedSha256: sha256Schema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, state } = await loadPlanForUpdate(transaction, input);
      if (state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot remote check requires executing state",
        );
      }
      assertRemoteVerificationEnvelopeAuthorized(plan);
      const expected = await remoteCheckExpectation(
        transaction,
        plan,
        input.checkKind,
      );
      if (
        input.observedCid !== expected.expectedCid ||
        input.observedSha256 !== expected.expectedSha256 ||
        input.observedBytes !== expected.expectedBytes ||
        canonicalJsonSha256(input.metrics) !==
          canonicalJsonSha256(expected.metrics)
      ) {
        throw new DurableInputError(
          "Candidate source-snapshot remote check does not match immutable evidence",
        );
      }
      const receiptSet = await loadVerifiedRemoteReadReceiptSet(transaction, {
        checkKind: input.checkKind,
        planId: plan.planId,
      });
      const payload = {
        checkKind: input.checkKind,
        checkedAt: input.checkedAt,
        evidenceSha256: receiptSet.receiptSetSha256,
        ...expected,
        metrics: expected.metrics,
        observedBytes: expected.expectedBytes,
        observedCid: expected.expectedCid,
        observedSha256: expected.expectedSha256,
        planId: plan.planId,
        planSha256: plan.planSha256,
        schemaVersion: "candidate-source-snapshot-remote-check-v1",
      };
      const checkSha256 = canonicalJsonSha256(payload);
      const checkId = deterministicId("snapshotdemoremotecheck", [
        "candidate-source-snapshot-remote-check-v1",
        plan.planId,
        input.checkKind,
        checkSha256,
      ]);
      const existing = await transaction<
        {
          check_id: string;
          check_sha256: string;
          verification_receipt_count: number | null;
          verification_receipt_set_sha256: string | null;
        }[]
      >`
        SELECT check_id, check_sha256, verification_receipt_count,
               verification_receipt_set_sha256
        FROM oracle_candidate_source_snapshot_demo_remote_checks
        WHERE plan_id = ${plan.planId} AND check_kind = ${input.checkKind}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].check_id !== checkId ||
          existing[0].check_sha256 !== checkSha256 ||
          existing[0].verification_receipt_count !== receiptSet.receiptCount ||
          existing[0].verification_receipt_set_sha256 !==
            receiptSet.receiptSetSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot remote check replay conflicts",
          );
        }
        return {
          checkId,
          checkKind: input.checkKind,
          checkSha256,
          evidenceSha256: receiptSet.receiptSetSha256,
        };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_remote_checks (
          check_id, plan_id, plan_sha256, check_kind,
          expected_cid, observed_cid, expected_sha256, observed_sha256,
          expected_bytes, observed_bytes, metrics, evidence_sha256,
          check_payload, check_sha256, checked_at, checked_at_iso,
          verification_receipt_set_sha256, verification_receipt_count
        ) VALUES (
          ${checkId}, ${plan.planId}, ${plan.planSha256}, ${input.checkKind},
          ${expected.expectedCid}, ${expected.expectedCid},
          ${expected.expectedSha256}, ${expected.expectedSha256},
          ${expected.expectedBytes}, ${expected.expectedBytes},
          ${transaction.json(expected.metrics as postgres.JSONValue)},
          ${receiptSet.receiptSetSha256},
          ${transaction.json(payload as postgres.JSONValue)}, ${checkSha256},
          ${input.checkedAt}, ${input.checkedAt},
          ${receiptSet.receiptSetSha256}, ${receiptSet.receiptCount}
        )
      `;
      return {
        checkId,
        checkKind: input.checkKind,
        checkSha256,
        evidenceSha256: receiptSet.receiptSetSha256,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function assertFinalRemoteReadClosure(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<void> {
  const expected = candidateSourceSnapshotRequestCategory(
    plan.requestEnvelope,
    "final_credential_free_verification",
  );
  const categories = await transaction<
    {
      consumed_request_count: number;
      planned_maximum_request_count: number;
      planned_successful_request_count: number;
    }[]
  >`
    SELECT planned_successful_request_count, planned_maximum_request_count,
           consumed_request_count
    FROM oracle_candidate_source_snapshot_demo_request_categories
    WHERE plan_id = ${plan.planId}
      AND request_category = 'final_credential_free_verification'
    FOR UPDATE
  `;
  const aggregates = await transaction<
    {
      distinct_logical_request_count: string;
      duplicate_verified_logical_count: string;
      receipt_count: string;
      request_count: string;
      request_without_receipt_count: string;
      started_request_count: string;
      verified_logical_request_count: string;
      verified_receipt_count: string;
    }[]
  >`
    SELECT count(*)::text AS request_count,
           count(DISTINCT request.logical_request_id)::text
             AS distinct_logical_request_count,
           count(*) FILTER (WHERE request.outcome = 'request_started')::text
             AS started_request_count,
           count(*) FILTER (
             WHERE receipt.verification_receipt_id IS NULL
           )::text AS request_without_receipt_count,
           count(receipt.verification_receipt_id)::text AS receipt_count,
           count(*) FILTER (WHERE receipt.outcome = 'verified')::text
             AS verified_receipt_count,
           count(DISTINCT request.logical_request_id) FILTER (
             WHERE receipt.outcome = 'verified'
           )::text AS verified_logical_request_count,
           (SELECT count(*)::text
              FROM (
                SELECT duplicate_request.logical_request_id
                FROM oracle_candidate_source_snapshot_demo_requests
                  duplicate_request
                JOIN oracle_candidate_source_snapshot_demo_remote_read_receipts
                  duplicate_receipt
                  ON duplicate_receipt.request_id = duplicate_request.request_id
                WHERE duplicate_request.plan_id = ${plan.planId}
                  AND duplicate_request.request_category =
                    'final_credential_free_verification'
                  AND duplicate_receipt.outcome = 'verified'
                GROUP BY duplicate_request.logical_request_id
                HAVING count(*) <> 1
              ) duplicate_verified
           ) AS duplicate_verified_logical_count
    FROM oracle_candidate_source_snapshot_demo_requests request
    LEFT JOIN oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
      ON receipt.request_id = request.request_id
    WHERE request.plan_id = ${plan.planId}
      AND request.request_category = 'final_credential_free_verification'
  `;
  const checkAggregates = await transaction<
    {
      check_count: string;
      missing_receipt_binding_count: string;
      receipt_count: string;
    }[]
  >`
    SELECT count(*)::text AS check_count,
           count(*) FILTER (
             WHERE verification_receipt_count IS NULL
                OR verification_receipt_set_sha256 IS NULL
           )::text AS missing_receipt_binding_count,
           coalesce(sum(verification_receipt_count), 0)::text AS receipt_count
    FROM oracle_candidate_source_snapshot_demo_remote_checks
    WHERE plan_id = ${plan.planId}
  `;
  const category = categories[0];
  const aggregate = aggregates[0];
  const checkAggregate = checkAggregates[0];
  const logicalRequests =
    plan.requestEnvelope.finalVerification.logicalRequests;
  if (
    !category ||
    !aggregate ||
    !checkAggregate ||
    category.planned_successful_request_count !== expected.successfulRequests ||
    category.planned_maximum_request_count !== expected.maximumRequests ||
    category.consumed_request_count !== Number(aggregate.request_count) ||
    category.consumed_request_count < logicalRequests ||
    category.consumed_request_count > expected.maximumRequests ||
    Number(aggregate.distinct_logical_request_count) !== logicalRequests ||
    Number(aggregate.verified_logical_request_count) !== logicalRequests ||
    Number(aggregate.verified_receipt_count) !== logicalRequests ||
    Number(aggregate.duplicate_verified_logical_count) !== 0 ||
    Number(aggregate.started_request_count) !== 0 ||
    Number(aggregate.request_without_receipt_count) !== 0 ||
    Number(aggregate.receipt_count) !== Number(aggregate.request_count) ||
    Number(checkAggregate.check_count) !==
      CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS.length ||
    Number(checkAggregate.missing_receipt_binding_count) !== 0 ||
    Number(checkAggregate.receipt_count) !== Number(aggregate.receipt_count)
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot final verification is not closed over its exact receipt set",
    );
  }
}

export async function recordCandidateSourceSnapshotRemoteVerification(
  databaseUrl: string,
  inputValue: {
    approvalId: string;
    planId: string;
    planSha256: string;
    uploadClosureId: string;
    verifiedAt: string;
  },
): Promise<{ verificationId: string; verificationSha256: string }> {
  const input = planIdentitySchema
    .extend({
      approvalId: approvalIdSchema,
      uploadClosureId: z
        .string()
        .regex(/^snapshotdemouploadclosure_[a-f0-9]{32}$/),
      verifiedAt: timestampSchema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, state } = await loadPlanForUpdate(transaction, input);
      if (state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot remote verification requires executing state",
        );
      }
      assertRemoteVerificationEnvelopeAuthorized(plan);
      const checks = await transaction<
        {
          check_id: string;
          check_kind: CandidateSourceSnapshotRemoteCheckKind;
          check_sha256: string;
          evidence_sha256: string;
        }[]
      >`
        SELECT check_id, check_kind, check_sha256, evidence_sha256
        FROM oracle_candidate_source_snapshot_demo_remote_checks
        WHERE plan_id = ${plan.planId}
        ORDER BY check_kind
      `;
      if (
        checks.length !==
          CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS.length ||
        new Set(checks.map((check) => check.check_kind)).size !==
          CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS.length
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot remote verification checks are incomplete",
        );
      }
      await assertFinalRemoteReadClosure(transaction, plan);
      const queryObject = await loadObjectExpectation(
        transaction,
        plan.planId,
        plan.targets.queryTable.targetCid,
      );
      const checkSummaries = checks.map((check) => ({
        checkId: check.check_id,
        checkKind: check.check_kind,
        checkSha256: check.check_sha256,
        evidenceSha256: check.evidence_sha256,
      }));
      const checkSetSha256 = canonicalJsonSha256(checkSummaries);
      const payload = {
        approvalId: input.approvalId,
        checkSetSha256,
        checks: checkSummaries,
        distinctPropertyIdCount: plan.coverage.activeProperties,
        fixtureMatchCount: 0,
        graphTraversalValid: true,
        inventoryRootCid: plan.inventory.inventoryRootCid,
        inventoryRootSha256: plan.inventory.inventoryRootSha256,
        manifestCid: plan.controlArtifacts.manifestIndex.expectedCid,
        manifestSha256: plan.controlArtifacts.manifestIndex.sha256,
        nullPropertyIdCount: 0,
        openDataRootCid: plan.targets.openData.targetCid,
        planId: plan.planId,
        planSha256: plan.planSha256,
        propertyCidCorrespondence: true,
        propertyCount: plan.coverage.activeProperties,
        queryTableBytes: queryObject.expectedBytes,
        queryTableRootCid: plan.targets.queryTable.targetCid,
        queryTableSha256: queryObject.expectedSha256,
        schemaVersion: "candidate-source-snapshot-remote-verification-v2",
        uploadClosureId: input.uploadClosureId,
        verifiedAt: input.verifiedAt,
      };
      const verificationSha256 = canonicalJsonSha256(payload);
      const verificationId = deterministicId("snapshotdemoremoteverification", [
        "candidate-source-snapshot-remote-verification-v2",
        plan.planId,
        verificationSha256,
      ]);
      const existing = await transaction<
        { verification_id: string; verification_sha256: string }[]
      >`
        SELECT verification_id, verification_sha256
        FROM oracle_candidate_source_snapshot_demo_remote_verifications
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].verification_id !== verificationId ||
          existing[0].verification_sha256 !== verificationSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot remote verification replay conflicts",
          );
        }
        return { verificationId, verificationSha256 };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_remote_verifications (
          verification_id, plan_id, plan_sha256, approval_id,
          upload_closure_id, open_data_root_cid, query_table_root_cid,
          manifest_cid, manifest_sha256, inventory_root_cid,
          inventory_root_sha256, query_table_bytes, query_table_sha256,
          property_count, distinct_property_id_count, null_property_id_count,
          property_cid_correspondence, graph_traversal_valid,
          fixture_match_count, verification_payload, check_set_sha256,
          verification_sha256, verified_at
        ) VALUES (
          ${verificationId}, ${plan.planId}, ${plan.planSha256},
          ${input.approvalId}, ${input.uploadClosureId},
          ${plan.targets.openData.targetCid}, ${plan.targets.queryTable.targetCid},
          ${plan.controlArtifacts.manifestIndex.expectedCid},
          ${plan.controlArtifacts.manifestIndex.sha256},
          ${plan.inventory.inventoryRootCid},
          ${plan.inventory.inventoryRootSha256},
          ${queryObject.expectedBytes}, ${queryObject.expectedSha256},
          ${plan.coverage.activeProperties}, ${plan.coverage.activeProperties},
          0, true, true, 0, ${transaction.json(payload)}, ${checkSetSha256},
          ${verificationSha256}, ${input.verifiedAt}
        )
      `;
      return { verificationId, verificationSha256 };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function completeCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  inputValue: {
    cutover: CandidateSourceSnapshotIpnsControllerResult;
    planId: string;
    planSha256: string;
    summary: CandidateSourceSnapshotUploadSummary;
    uploadClosure: CandidateSourceSnapshotUploadClosure;
  },
): Promise<{
  planId: string;
  planSha256: string;
  revision: number;
  state: "completed";
}> {
  const input = planIdentitySchema
    .extend({
      cutover: completedCutoverSchema,
      summary: uploadSummarySchema,
      uploadClosure: uploadClosureSchema,
    })
    .parse(inputValue);
  const identity = planIdentitySchema.parse({
    planId: input.planId,
    planSha256: input.planSha256,
  });
  if (
    input.cutover.planId !== identity.planId ||
    input.cutover.planSha256 !== identity.planSha256 ||
    input.uploadClosure.planId !== identity.planId ||
    input.uploadClosure.planSha256 !== identity.planSha256 ||
    input.summary.totalObjects !== input.uploadClosure.exactObjectCount ||
    input.summary.skippedVerified +
      input.summary.uploadedAndVerified +
      input.summary.recoveredByInspection !==
      input.summary.totalObjects ||
    input.summary.requestCostUsd !== input.uploadClosure.admittedRequestCostUsd
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot completion result is not internally exact",
    );
  }
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const current = await loadPlanForUpdate(transaction, identity);
      if (current.state === "completed") {
        const events = await transaction<
          { event_sha256: string; metadata: unknown }[]
        >`
          SELECT event_sha256, metadata
          FROM oracle_candidate_source_snapshot_demo_events
          WHERE plan_id = ${identity.planId}
            AND event_type = 'publication_completed'
        `;
        if (events.length !== 1) {
          throw new DurableConflictError(
            "Completed candidate source-snapshot plan lacks one stored result",
          );
        }
        const event = completionEventSchema.parse(events[0]!.metadata);
        if (
          events[0]!.event_sha256 !== canonicalJsonSha256(event) ||
          event.resultSha256 !== canonicalJsonSha256(event.result) ||
          event.planId !== identity.planId ||
          event.planSha256 !== identity.planSha256 ||
          event.revision !== current.revision ||
          event.result.completedRevision !== current.revision
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot completion replay conflicts with its stored result",
          );
        }
        assertCandidateSourceSnapshotCompletedReplayCompatible(event.result, {
          cutover: input.cutover,
          summary: input.summary,
          uploadClosure: input.uploadClosure,
        });
        return { ...identity, revision: current.revision, state: "completed" };
      }
      if (current.state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot completion requires executing state",
        );
      }
      const verifications = await transaction<{ verification_id: string }[]>`
        SELECT verification_id
        FROM oracle_candidate_source_snapshot_demo_remote_verifications
        WHERE plan_id = ${identity.planId}
          AND plan_sha256 = ${identity.planSha256}
      `;
      if (!verifications[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot completion lacks remote verification",
        );
      }
      const closureRows = await transaction<
        CandidateSourceSnapshotUploadClosureRow[]
      >`
        SELECT closure_id, plan_sha256, approval_id, exact_object_count,
               exact_total_bytes, admitted_request_count,
               admitted_request_cost_usd, closure_sha256, verified_at
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${identity.planId}
          AND plan_sha256 = ${identity.planSha256}
      `;
      if (
        closureRows.length !== 1 ||
        canonicalJsonSha256(
          uploadClosureFromRow(identity.planId, closureRows[0]!),
        ) !== canonicalJsonSha256(input.uploadClosure)
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot completion upload closure conflicts with durable evidence",
        );
      }
      await assertFinalRemoteReadClosure(transaction, current.plan);
      const updated = await transaction<{ revision: number }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_plans
        SET state = 'completed', revision = revision + 1, updated_at = now()
        WHERE plan_id = ${identity.planId} AND state = 'executing'
          AND revision = ${current.revision}
        RETURNING revision
      `;
      if (!updated[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot completion lost its revision",
        );
      }
      const result = completedReplaySchema.parse({
        completedRevision: updated[0].revision,
        cutover: input.cutover,
        summary: input.summary,
        uploadClosure: input.uploadClosure,
      });
      const eventPayload = completionEventSchema.parse({
        planId: identity.planId,
        planSha256: identity.planSha256,
        result,
        resultSha256: canonicalJsonSha256(result),
        revision: updated[0].revision,
        schemaVersion: "candidate-source-snapshot-completion-event-v2",
        verificationId: verifications[0].verification_id,
      });
      const eventSha256 = canonicalJsonSha256(eventPayload);
      const eventId = deterministicId("snapshotdemoevent", [
        "candidate-source-snapshot-completed-v2",
        identity.planId,
        eventSha256,
      ]);
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_events (
          event_id, plan_id, event_type, event_sha256, metadata
        ) VALUES (
          ${eventId}, ${identity.planId}, 'publication_completed',
          ${eventSha256}, ${transaction.json(eventPayload)}
        )
      `;
      return {
        ...identity,
        revision: updated[0].revision,
        state: "completed" as const,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
