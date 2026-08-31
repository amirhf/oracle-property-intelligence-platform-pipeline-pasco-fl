import postgres from "postgres";
import { z } from "zod";

import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";
import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "../publication/candidate-source-snapshot-demo.js";

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
  rollback_recorded: ["rollback_in_flight"],
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
    verifiedAt: string;
  },
): Promise<CandidateSourceSnapshotUploadClosure> {
  const input = planIdentitySchema
    .extend({ approvalId: approvalIdSchema, verifiedAt: timestampSchema })
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
      const aggregateRows = await transaction<
        {
          actual_bytes: string;
          actual_count: string;
          mismatch_count: string;
          unresolved_count: string;
        }[]
      >`
        SELECT count(*)::text AS actual_count,
               coalesce(sum(expected_bytes), 0)::text AS actual_bytes,
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
        verifiedAt: input.verifiedAt,
      };
      const closureSha256 = canonicalJsonSha256(payload);
      const closureId = deterministicId("snapshotdemouploadclosure", [
        "candidate-source-snapshot-upload-closure-v1",
        plan.planId,
        closureSha256,
      ]);
      const existing = await transaction<
        {
          admitted_request_cost_usd: string | number;
          admitted_request_count: number;
          approval_id: string;
          closure_id: string;
          closure_sha256: string;
          exact_object_count: number;
          exact_total_bytes: string | number;
          verified_at: Date;
        }[]
      >`
        SELECT closure_id, approval_id, exact_object_count, exact_total_bytes,
               admitted_request_count, admitted_request_cost_usd,
               closure_sha256, verified_at
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        const row = existing[0];
        if (
          row.closure_id !== closureId ||
          row.approval_id !== input.approvalId ||
          row.exact_object_count !== payload.exactObjectCount ||
          Number(row.exact_total_bytes) !== payload.exactTotalBytes ||
          row.admitted_request_count !== payload.admittedRequestCount ||
          Number(row.admitted_request_cost_usd) !==
            payload.admittedRequestCostUsd ||
          row.closure_sha256 !== closureSha256 ||
          row.verified_at.toISOString() !== input.verifiedAt
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot upload closure replay conflicts",
          );
        }
      } else {
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
            ${closureSha256}, ${input.verifiedAt}
          )
        `;
      }
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
        verifiedAt: input.verifiedAt,
      };
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

function assertRemoteVerificationEnvelopeAuthorized(
  plan: CandidateSourceSnapshotDemoPlan,
): void {
  if (
    plan.requestEnvelope.successfulExecution.freeOperations <
    CANDIDATE_SOURCE_SNAPSHOT_REQUIRED_REMOTE_CHECKS.length
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

export async function recordCandidateSourceSnapshotRemoteCheck(
  databaseUrl: string,
  inputValue: {
    checkKind: CandidateSourceSnapshotRemoteCheckKind;
    checkedAt: string;
    evidenceSha256: string;
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
      evidenceSha256: sha256Schema,
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
      const payload = {
        checkKind: input.checkKind,
        checkedAt: input.checkedAt,
        evidenceSha256: input.evidenceSha256,
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
        { check_id: string; check_sha256: string }[]
      >`
        SELECT check_id, check_sha256
        FROM oracle_candidate_source_snapshot_demo_remote_checks
        WHERE plan_id = ${plan.planId} AND check_kind = ${input.checkKind}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].check_id !== checkId ||
          existing[0].check_sha256 !== checkSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot remote check replay conflicts",
          );
        }
        return {
          checkId,
          checkKind: input.checkKind,
          checkSha256,
          evidenceSha256: input.evidenceSha256,
        };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_remote_checks (
          check_id, plan_id, plan_sha256, check_kind,
          expected_cid, observed_cid, expected_sha256, observed_sha256,
          expected_bytes, observed_bytes, metrics, evidence_sha256,
          check_payload, check_sha256, checked_at, checked_at_iso
        ) VALUES (
          ${checkId}, ${plan.planId}, ${plan.planSha256}, ${input.checkKind},
          ${expected.expectedCid}, ${expected.expectedCid},
          ${expected.expectedSha256}, ${expected.expectedSha256},
          ${expected.expectedBytes}, ${expected.expectedBytes},
          ${transaction.json(expected.metrics as postgres.JSONValue)},
          ${input.evidenceSha256},
          ${transaction.json(payload as postgres.JSONValue)}, ${checkSha256},
          ${input.checkedAt}, ${input.checkedAt}
        )
      `;
      return {
        checkId,
        checkKind: input.checkKind,
        checkSha256,
        evidenceSha256: input.evidenceSha256,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
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
  identityValue: { planId: string; planSha256: string },
): Promise<{
  planId: string;
  planSha256: string;
  revision: number;
  state: "completed";
}> {
  const identity = planIdentitySchema.parse(identityValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const current = await loadPlanForUpdate(transaction, identity);
      if (current.state === "completed") {
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
      const eventPayload = {
        planId: identity.planId,
        planSha256: identity.planSha256,
        revision: updated[0].revision,
        verificationId: verifications[0].verification_id,
      };
      const eventSha256 = canonicalJsonSha256(eventPayload);
      const eventId = deterministicId("snapshotdemoevent", [
        "candidate-source-snapshot-completed-v1",
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
