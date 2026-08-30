import postgres from "postgres";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import {
  createCandidateDemoPlan,
  validateCandidateDemoPlan,
  type CandidateDemoPlan,
} from "../publication/candidate-demo.js";
import type {
  CandidateDemoExecutionJournal,
  CandidateUploadArtifact,
  CandidateUploadReceipt,
} from "../publication/filebase-executor.js";
import { validatePublicationPlan } from "../publication/plan.js";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const priorCidSchema = z.union([
  cidSchema,
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const identitySchema = z.strictObject({
  demoPlanId: z.string().regex(/^demo_[a-f0-9]{32}$/),
  demoPlanSha256: sha256Schema,
});
export const candidateDemoApprovalSchema = identitySchema.extend({
  approvedAt: z.string().datetime(),
  approverReference: z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/),
});

export interface CandidateDemoState {
  approvalId: string | null;
  demoPlanId: string;
  demoPlanSha256: string;
  revision: number;
  state:
    | "awaiting_configuration"
    | "awaiting_approval"
    | "approved"
    | "executing"
    | "completed"
    | "manual_intervention_required"
    | "failed_terminal";
}

export interface CandidateDemoDurablePlan {
  approval: {
    approvalId: string;
    approvedAt: string;
    approverReference: string;
  } | null;
  plan: CandidateDemoPlan;
  state: CandidateDemoState;
}

const candidateResolutionObservationSchema = z.strictObject({
  cacheAgeSeconds: z.number().int().min(0).max(3_600).nullable(),
  httpStatus: z.number().int().min(100).max(599).nullable(),
  observedAt: z.string().datetime(),
  observedCid: priorCidSchema.nullable(),
  ordinal: z.number().int().min(1).max(4),
  outcome: z.enum([
    "resolved",
    "unavailable",
    "timeout",
    "http_error",
    "transport_error",
  ]),
  resolver: z.enum([
    "filebase_control",
    "filebase_gateway",
    "ipfs_io",
    "dweb_link",
  ]),
  resolverType: z.enum(["control_plane", "public_resolver"]),
  responseBytes: z.number().int().min(0).max(65_536),
  responseSha256: sha256Schema,
});

export type CandidateResolutionObservation = z.infer<
  typeof candidateResolutionObservationSchema
>;
export type CandidateResolutionClassification =
  | "prior_observed"
  | "target_observed"
  | "split"
  | "unavailable"
  | "unexpected_cid";

export interface CandidateResolutionCycleResult {
  classification: CandidateResolutionClassification;
  cycleId: string;
  evidenceSha256: string;
  intentId: string;
  sequence: number;
}

function validateCandidateResolutionObservations(
  value: unknown,
): CandidateResolutionObservation[] {
  const observations = z
    .array(candidateResolutionObservationSchema)
    .min(3)
    .max(4)
    .parse(value);
  const expected =
    observations.length === 4
      ? ([
          "filebase_control",
          "filebase_gateway",
          "ipfs_io",
          "dweb_link",
        ] as const)
      : (["filebase_control", "ipfs_io", "dweb_link"] as const);
  for (let index = 0; index < expected.length; index += 1) {
    const observation = observations[index];
    if (
      observation?.ordinal !== index + 1 ||
      observation.resolver !== expected[index] ||
      observation.resolverType !==
        (index === 0 ? "control_plane" : "public_resolver") ||
      (observation.outcome === "resolved") !==
        (observation.observedCid !== null && observation.httpStatus !== null)
    ) {
      throw new DurableInputError(
        "Candidate resolver observations failed strict validation",
      );
    }
  }
  return observations;
}

export function classifyCandidateResolutionObservations(input: {
  observations: unknown;
  priorCid: string;
  targetCid: string;
}): CandidateResolutionClassification {
  const observations = validateCandidateResolutionObservations(
    input.observations,
  );
  const observed = observations
    .map((entry) => entry.observedCid)
    .filter((entry): entry is string => entry !== null);
  if (
    observed.some(
      (entry) => entry !== input.priorCid && entry !== input.targetCid,
    )
  ) {
    return "unexpected_cid";
  }
  if (observations.some((entry) => entry.outcome !== "resolved")) {
    return "unavailable";
  }
  if (observed.every((entry) => entry === input.targetCid)) {
    return "target_observed";
  }
  if (observed.every((entry) => entry === input.priorCid)) {
    return "prior_observed";
  }
  return "split";
}

interface StateRow {
  approval_id: string | null;
  demo_plan_id: string;
  demo_plan_sha256: string;
  revision: number;
  state: CandidateDemoState["state"];
}

function parse<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new DurableInputError(`${label} failed strict validation`);
  }
  return result.data;
}

function state(row: StateRow): CandidateDemoState {
  return {
    approvalId: row.approval_id,
    demoPlanId: row.demo_plan_id,
    demoPlanSha256: row.demo_plan_sha256,
    revision: row.revision,
    state: row.state,
  };
}

async function recordEvent(
  transaction: postgres.TransactionSql,
  demoPlanId: string,
  eventType: string,
  metadata: postgres.JSONValue,
): Promise<void> {
  const eventSha256 = canonicalJsonSha256({ demoPlanId, eventType, metadata });
  const eventId = deterministicId("demoevent", [
    "1.0.0",
    demoPlanId,
    eventType,
    eventSha256,
  ]);
  await transaction`
    INSERT INTO oracle_candidate_demo_events (
      event_id, demo_plan_id, event_type, event_sha256, metadata
    ) VALUES (
      ${eventId}, ${demoPlanId}, ${eventType}, ${eventSha256},
      ${transaction.json(metadata)}
    )
    ON CONFLICT (demo_plan_id, event_sha256) DO NOTHING
  `;
}

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  await transaction`SELECT pg_advisory_xact_lock(hashtext('oracle-candidate-demo'), hashtext('pasco'))`;
}

async function loadPlan(
  transaction: postgres.TransactionSql,
  identity: z.infer<typeof identitySchema>,
): Promise<CandidateDemoPlan> {
  const rows = await transaction<
    { demo_plan_sha256: string; plan_payload: unknown }[]
  >`
    SELECT demo_plan_sha256, plan_payload
    FROM oracle_candidate_demo_plans
    WHERE demo_plan_id = ${identity.demoPlanId}
  `;
  const row = rows[0];
  if (!row || row.demo_plan_sha256 !== identity.demoPlanSha256) {
    throw new DurableConflictError("Candidate demo plan identity conflict");
  }
  const plan = validateCandidateDemoPlan(row.plan_payload);
  if (
    plan.demoPlanId !== identity.demoPlanId ||
    plan.demoPlanSha256 !== identity.demoPlanSha256
  ) {
    throw new DurableConflictError("Stored candidate demo plan is invalid");
  }
  return plan;
}

async function loadState(
  transaction: postgres.TransactionSql,
  identity: z.infer<typeof identitySchema>,
): Promise<StateRow> {
  const rows = await transaction<StateRow[]>`
    SELECT plan.demo_plan_id, plan.demo_plan_sha256, plan.state, plan.revision,
           approval.approval_id
    FROM oracle_candidate_demo_plans plan
    LEFT JOIN oracle_candidate_demo_approvals approval
      ON approval.demo_plan_id = plan.demo_plan_id
    WHERE plan.demo_plan_id = ${identity.demoPlanId}
    FOR UPDATE OF plan
  `;
  const row = rows[0];
  if (!row || row.demo_plan_sha256 !== identity.demoPlanSha256) {
    throw new DurableConflictError("Candidate demo plan identity conflict");
  }
  return row;
}

export async function loadCandidateDemoDurablePlan(
  databaseUrl: string,
  identityValue: unknown,
): Promise<CandidateDemoDurablePlan> {
  const identity = parse(identitySchema, identityValue, "candidate demo plan");
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const plan = await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const approvals = await transaction<
        {
          approval_id: string;
          approved_at: Date | string;
          approver_reference: string;
        }[]
      >`
        SELECT approval_id, approved_at, approver_reference
        FROM oracle_candidate_demo_approvals
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      const approval = approvals[0];
      return {
        approval: approval
          ? {
              approvalId: approval.approval_id,
              approvedAt: new Date(approval.approved_at).toISOString(),
              approverReference: approval.approver_reference,
            }
          : null,
        plan,
        state: state(current),
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateDemoPlan(
  databaseUrl: string,
  candidateValue: unknown,
  sourceValue: unknown,
): Promise<CandidateDemoState> {
  const candidate = validateCandidateDemoPlan(candidateValue);
  const source = validatePublicationPlan(sourceValue);
  if (
    candidate.sourcePlanId !== source.planId ||
    candidate.sourcePlanSha256 !== source.planSha256 ||
    candidate.coverageMode !== source.coverage.mode
  ) {
    throw new DurableInputError(
      "Candidate demo plan does not match its non-authoritative source plan",
    );
  }
  const expected = await createCandidateDemoPlan({
    limits: candidate.limits,
    preflightEvidenceSha256: candidate.preflightEvidenceSha256,
    preflightObservedAt: candidate.preflightObservedAt,
    sourcePlan: source,
    targets: {
      openData: {
        bucket: candidate.targets.openData.bucket,
        ipnsLabel: candidate.targets.openData.ipnsLabel,
        ipnsNetworkKey: candidate.targets.openData.ipnsNetworkKey,
        priorCid: candidate.targets.openData.priorCid,
      },
      queryTable: {
        bucket: candidate.targets.queryTable.bucket,
        ipnsLabel: candidate.targets.queryTable.ipnsLabel,
        ipnsNetworkKey: candidate.targets.queryTable.ipnsNetworkKey,
        priorCid: candidate.targets.queryTable.priorCid,
      },
    },
  });
  if (
    expected.demoPlanId !== candidate.demoPlanId ||
    expected.demoPlanSha256 !== candidate.demoPlanSha256
  ) {
    throw new DurableInputError(
      "Candidate demo inventory does not match the source publication plan",
    );
  }
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const existing = await transaction<StateRow[]>`
        SELECT demo_plan_id, demo_plan_sha256, state, revision, NULL::text AS approval_id
        FROM oracle_candidate_demo_plans
        WHERE demo_plan_id = ${candidate.demoPlanId}
      `;
      if (existing[0]) {
        if (existing[0].demo_plan_sha256 !== candidate.demoPlanSha256) {
          throw new DurableConflictError("Candidate demo plan replay conflict");
        }
        return state(existing[0]);
      }
      await transaction`
        INSERT INTO oracle_candidate_demo_plans (
          demo_plan_id, demo_plan_sha256, plan_version, source_plan_id,
          source_plan_sha256, coverage_mode, object_count, total_bytes,
          request_limit, budget_limit_usd, plan_payload, state
        ) VALUES (
          ${candidate.demoPlanId}, ${candidate.demoPlanSha256},
          ${candidate.version}, ${candidate.sourcePlanId},
          ${candidate.sourcePlanSha256}, ${candidate.coverageMode},
          ${candidate.objectCount}, ${candidate.totalBytes},
          ${candidate.limits.maxRequests}, ${candidate.limits.maxBudgetUsd},
          ${transaction.json(candidate as unknown as postgres.JSONValue)},
          'awaiting_approval'
        )
      `;
      for (const artifact of candidate.objects) {
        await transaction`
          INSERT INTO oracle_candidate_demo_object_effects (
            demo_plan_id, domain, object_key, expected_sha256, expected_cid,
            expected_bytes, status
          ) VALUES (
            ${candidate.demoPlanId}, ${artifact.domain}, ${artifact.objectKey},
            ${artifact.sha256}, ${artifact.expectedCid}, ${artifact.byteSize},
            'pending'
          )
        `;
      }
      return {
        approvalId: null,
        demoPlanId: candidate.demoPlanId,
        demoPlanSha256: candidate.demoPlanSha256,
        revision: 1,
        state: "awaiting_approval",
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function approveCandidateDemoPlan(
  databaseUrl: string,
  requestValue: unknown,
): Promise<CandidateDemoState> {
  const request = parse(
    candidateDemoApprovalSchema,
    requestValue,
    "candidate demo approval",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, request);
      const current = await loadState(transaction, request);
      if (current.approval_id) {
        const approvals = await transaction<
          { approval_id: string; approver_reference: string }[]
        >`
          SELECT approval_id, approver_reference
          FROM oracle_candidate_demo_approvals
          WHERE demo_plan_id = ${request.demoPlanId}
        `;
        const expectedApprovalId = deterministicId("demoapproval", [
          "1.0.0",
          request.demoPlanId,
          request.demoPlanSha256,
          request.approverReference,
          request.approvedAt,
        ]);
        if (
          approvals[0]?.approval_id !== expectedApprovalId ||
          approvals[0]?.approver_reference !== request.approverReference
        ) {
          throw new DurableConflictError("Candidate demo approval conflict");
        }
        await recordEvent(
          transaction,
          request.demoPlanId,
          "approval_recorded",
          {
            approvalId: expectedApprovalId,
            approvedAt: request.approvedAt,
            approverReference: request.approverReference,
            demoPlanSha256: request.demoPlanSha256,
          },
        );
        return state(current);
      }
      if (current.state !== "awaiting_approval") {
        throw new DurableConflictError("Candidate demo plan is not approvable");
      }
      const approvalId = deterministicId("demoapproval", [
        "1.0.0",
        request.demoPlanId,
        request.demoPlanSha256,
        request.approverReference,
        request.approvedAt,
      ]);
      await transaction`
        INSERT INTO oracle_candidate_demo_approvals (
          approval_id, demo_plan_id, demo_plan_sha256, approver_reference,
          approved_at
        ) VALUES (
          ${approvalId}, ${request.demoPlanId}, ${request.demoPlanSha256},
          ${request.approverReference}, ${request.approvedAt}
        )
      `;
      await recordEvent(transaction, request.demoPlanId, "approval_recorded", {
        approvalId,
        approvedAt: request.approvedAt,
        approverReference: request.approverReference,
        demoPlanSha256: request.demoPlanSha256,
      });
      await transaction`
        UPDATE oracle_candidate_demo_plans
        SET state = 'approved', revision = revision + 1, updated_at = now()
        WHERE demo_plan_id = ${request.demoPlanId}
      `;
      return {
        approvalId,
        demoPlanId: request.demoPlanId,
        demoPlanSha256: request.demoPlanSha256,
        revision: current.revision + 1,
        state: "approved",
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function beginCandidateDemoExecution(
  databaseUrl: string,
  identityValue: unknown,
): Promise<CandidateDemoState> {
  const identity = parse(
    identitySchema,
    identityValue,
    "candidate demo execution",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state === "executing") return state(current);
      if (current.state !== "approved" || current.approval_id === null) {
        throw new DurableConflictError(
          "Candidate demo execution requires exact approval",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_plans
        SET state = 'executing', revision = revision + 1, updated_at = now()
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      await recordEvent(transaction, identity.demoPlanId, "execution_started", {
        demoPlanSha256: identity.demoPlanSha256,
        revision: current.revision + 1,
      });
      return state({
        ...current,
        revision: current.revision + 1,
        state: "executing",
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function checkpointCandidateObjectVerified(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
    objectKey: string;
    providerCid: string;
    receiptSha256: string;
    requestCount?: number;
  },
): Promise<void> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: request.demoPlanId,
      demoPlanSha256: request.demoPlanSha256,
    },
    "candidate object checkpoint",
  );
  parse(cidSchema, request.providerCid, "candidate provider CID");
  parse(sha256Schema, request.receiptSha256, "candidate receipt hash");
  const requestCount = z
    .number()
    .int()
    .positive()
    .max(10)
    .parse(request.requestCount ?? 1);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state !== "executing") {
        throw new DurableConflictError("Candidate demo plan is not executing");
      }
      const changed = await transaction`
        UPDATE oracle_candidate_demo_object_effects
        SET status = 'verified', provider_cid = ${request.providerCid},
            receipt_sha256 = ${request.receiptSha256},
            request_count = request_count + ${requestCount}, updated_at = now()
        WHERE demo_plan_id = ${request.demoPlanId}
          AND domain = ${request.domain}
          AND object_key = ${request.objectKey}
          AND expected_cid = ${request.providerCid}
          AND status IN ('pending', 'in_flight', 'verified')
        RETURNING object_key
      `;
      if (changed.length !== 1) {
        throw new DurableConflictError(
          "Candidate demo provider CID does not match the immutable plan",
        );
      }
      await recordEvent(transaction, request.demoPlanId, "object_verified", {
        domain: request.domain,
        objectKey: request.objectKey,
        providerCid: request.providerCid,
        receiptSha256: request.receiptSha256,
        requestCount,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function markCandidateObjectInFlight(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
    objectKey: string;
  },
): Promise<void> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: request.demoPlanId,
      demoPlanSha256: request.demoPlanSha256,
    },
    "candidate object admission",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state !== "executing") {
        throw new DurableConflictError("Candidate demo plan is not executing");
      }
      const rows = await transaction`
        UPDATE oracle_candidate_demo_object_effects
        SET status = CASE WHEN status = 'pending' THEN 'in_flight' ELSE status END,
            updated_at = now()
        WHERE demo_plan_id = ${request.demoPlanId}
          AND domain = ${request.domain}
          AND object_key = ${request.objectKey}
          AND status IN ('pending', 'in_flight')
        RETURNING object_key
      `;
      if (rows.length !== 1) {
        throw new DurableConflictError(
          "Candidate demo object is not uploadable",
        );
      }
      await recordEvent(transaction, request.demoPlanId, "object_admitted", {
        domain: request.domain,
        objectKey: request.objectKey,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export function createCandidateDemoExecutionJournal(
  databaseUrl: string,
): CandidateDemoExecutionJournal {
  return {
    assertIpnsMutationReady: async (plan, domain) =>
      assertCandidateIpnsMutationReady(
        databaseUrl,
        {
          demoPlanId: plan.demoPlanId,
          demoPlanSha256: plan.demoPlanSha256,
        },
        domain,
      ),
    beforeUpload: async (plan, artifact: CandidateUploadArtifact) =>
      markCandidateObjectInFlight(databaseUrl, {
        demoPlanId: plan.demoPlanId,
        demoPlanSha256: plan.demoPlanSha256,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
      }),
    beforeIpnsMutation: async (plan, domain) =>
      markCandidateIpnsUpdateInFlight(
        databaseUrl,
        {
          demoPlanId: plan.demoPlanId,
          demoPlanSha256: plan.demoPlanSha256,
        },
        domain,
      ),
    beforeIpnsRollback: async (plan, domain) =>
      markCandidateIpnsRollbackInFlight(
        databaseUrl,
        {
          demoPlanId: plan.demoPlanId,
          demoPlanSha256: plan.demoPlanSha256,
        },
        domain,
      ),
    recordUpload: async (
      plan,
      artifact: CandidateUploadArtifact,
      receipt: CandidateUploadReceipt,
    ) =>
      checkpointCandidateObjectVerified(databaseUrl, {
        demoPlanId: plan.demoPlanId,
        demoPlanSha256: plan.demoPlanSha256,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
        providerCid: receipt.cid,
        receiptSha256: receipt.receiptSha256,
        requestCount: receipt.requestCount,
      }),
    recordIpnsVerified: async (plan, domain) =>
      checkpointCandidateIpnsVerified(databaseUrl, {
        demoPlanId: plan.demoPlanId,
        demoPlanSha256: plan.demoPlanSha256,
        domain,
      }),
    recordIpnsRolledBack: async (plan, domain) =>
      checkpointCandidateIpnsRolledBack(databaseUrl, {
        demoPlanId: plan.demoPlanId,
        demoPlanSha256: plan.demoPlanSha256,
        domain,
      }),
  };
}

export async function markCandidateIpnsRollbackInFlight(
  databaseUrl: string,
  identityValue: unknown,
  domain: "open_data" | "query_table",
): Promise<void> {
  const identity = parse(
    identitySchema,
    identityValue,
    "candidate IPNS rollback",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const rows = await transaction<{ state: string }[]>`
        SELECT state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = ${domain}
        FOR UPDATE
      `;
      if (current.state !== "executing" || rows[0]?.state !== "verified") {
        throw new DurableConflictError(
          "Candidate IPNS rollback requires a verified target",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'rollback_in_flight', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = ${domain}
      `;
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "ipns_rollback_in_flight",
        {
          domain,
          demoPlanSha256: identity.demoPlanSha256,
        },
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function checkpointCandidateIpnsPriorConfirmed(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
  },
): Promise<void> {
  const identity = parse(identitySchema, request, "candidate IPNS recovery");
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const changed = await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'prior_confirmed', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId}
          AND domain = ${request.domain}
          AND state = 'update_in_flight'
        RETURNING intent_id
      `;
      if (changed.length !== 1) {
        throw new DurableConflictError(
          "Candidate IPNS prior recovery is not in flight",
        );
      }
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "ipns_prior_reconfirmed",
        {
          domain: request.domain,
          demoPlanSha256: identity.demoPlanSha256,
        },
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function markCandidateIpnsFailedTerminal(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
  },
): Promise<void> {
  const identity = parse(identitySchema, request, "candidate IPNS failure");
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const changed = await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'failed_terminal', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId}
          AND domain = ${request.domain}
          AND state = 'update_in_flight'
        RETURNING intent_id
      `;
      if (changed.length !== 1) {
        throw new DurableConflictError(
          "Candidate IPNS terminal failure is not in flight",
        );
      }
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "ipns_failed_terminal",
        {
          domain: request.domain,
          demoPlanSha256: identity.demoPlanSha256,
        },
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function checkpointCandidateIpnsRolledBack(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
  },
): Promise<void> {
  const identity = parse(identitySchema, request, "candidate IPNS rollback");
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const rows = await transaction<{ state: string }[]>`
        SELECT state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = ${request.domain}
        FOR UPDATE
      `;
      if (rows[0]?.state !== "rollback_in_flight") {
        throw new DurableConflictError(
          "Candidate IPNS rollback checkpoint is not in flight",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'rolled_back', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = ${request.domain}
      `;
      await recordEvent(transaction, identity.demoPlanId, "ipns_rolled_back", {
        domain: request.domain,
        demoPlanSha256: identity.demoPlanSha256,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function markCandidateDemoTerminal(
  databaseUrl: string,
  identityValue: unknown,
  reason: "ambiguous_remote_state" | "second_domain_rolled_back",
): Promise<CandidateDemoState> {
  const identity = parse(
    identitySchema,
    identityValue,
    "candidate terminal state",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state === "completed") {
        throw new DurableConflictError("Completed candidate plan is immutable");
      }
      const terminalState =
        reason === "ambiguous_remote_state"
          ? "manual_intervention_required"
          : "failed_terminal";
      await transaction`
        UPDATE oracle_candidate_demo_plans
        SET state = ${terminalState}, revision = revision + 1, updated_at = now()
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      await recordEvent(transaction, identity.demoPlanId, terminalState, {
        demoPlanSha256: identity.demoPlanSha256,
        reason,
      });
      return state({
        ...current,
        revision: current.revision + 1,
        state: terminalState,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateIpnsIntents(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    evidenceSha256: { openData: string; queryTable: string };
    intendedAt: string;
    priorCid: { openData: string; queryTable: string };
  },
): Promise<void> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: request.demoPlanId,
      demoPlanSha256: request.demoPlanSha256,
    },
    "candidate IPNS intents",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      const plan = await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state !== "executing") {
        throw new DurableConflictError("Candidate demo plan is not executing");
      }
      const objectCounts = await transaction<
        { pending: string | number; total: string | number }[]
      >`
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status != 'verified') AS pending
        FROM oracle_candidate_demo_object_effects
        WHERE demo_plan_id = ${request.demoPlanId}
      `;
      if (
        Number(objectCounts[0]?.total ?? 0) !== plan.objectCount ||
        Number(objectCounts[0]?.pending ?? 1) !== 0
      ) {
        throw new DurableConflictError(
          "Candidate IPNS intent requires every provider CID to be verified",
        );
      }
      for (const [domain, target, priorCid, evidenceSha256] of [
        [
          "open_data",
          plan.targets.openData,
          request.priorCid.openData,
          request.evidenceSha256.openData,
        ],
        [
          "query_table",
          plan.targets.queryTable,
          request.priorCid.queryTable,
          request.evidenceSha256.queryTable,
        ],
      ] as const) {
        parse(priorCidSchema, priorCid, "candidate prior CID");
        parse(sha256Schema, evidenceSha256, "candidate resolution evidence");
        if (priorCid !== target.priorCid) {
          throw new DurableConflictError(
            "Candidate prior CID does not match the immutable plan",
          );
        }
        const intentValue = {
          bucket: target.bucket,
          demoPlanId: plan.demoPlanId,
          demoPlanSha256: plan.demoPlanSha256,
          domain,
          evidenceSha256,
          intendedAt: request.intendedAt,
          ipnsLabel: target.ipnsLabel,
          ipnsNetworkKey: target.ipnsNetworkKey,
          priorCid,
          targetCid: target.targetCid,
        };
        const intentSha256 = canonicalJsonSha256(intentValue);
        const intentId = deterministicId("demointent", [
          "1.0.0",
          plan.demoPlanId,
          domain,
          intentSha256,
        ]);
        await transaction`
          INSERT INTO oracle_candidate_demo_ipns_intents (
            intent_id, intent_sha256, demo_plan_id, demo_plan_sha256, domain,
            bucket, ipns_label, ipns_network_key, prior_cid, target_cid,
            resolution_evidence_sha256, state, intended_at
          ) VALUES (
            ${intentId}, ${intentSha256}, ${plan.demoPlanId},
            ${plan.demoPlanSha256}, ${domain}, ${target.bucket},
            ${target.ipnsLabel}, ${target.ipnsNetworkKey}, ${priorCid},
            ${target.targetCid}, ${evidenceSha256}, 'prior_confirmed',
            ${request.intendedAt}
          )
          ON CONFLICT (demo_plan_id, domain) DO NOTHING
        `;
        const existing = await transaction<
          { intent_id: string; intent_sha256: string }[]
        >`
          SELECT intent_id, intent_sha256
          FROM oracle_candidate_demo_ipns_intents
          WHERE demo_plan_id = ${plan.demoPlanId} AND domain = ${domain}
        `;
        if (
          existing[0]?.intent_id !== intentId ||
          existing[0]?.intent_sha256 !== intentSha256
        ) {
          throw new DurableConflictError("Candidate IPNS intent conflict");
        }
      }
      await recordEvent(transaction, plan.demoPlanId, "ipns_intents_recorded", {
        demoPlanSha256: plan.demoPlanSha256,
        openDataPriorCid: plan.targets.openData.priorCid,
        openDataTargetCid: plan.targets.openData.targetCid,
        queryTablePriorCid: plan.targets.queryTable.priorCid,
        queryTableTargetCid: plan.targets.queryTable.targetCid,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function markCandidateIpnsUpdateInFlight(
  databaseUrl: string,
  identityValue: unknown,
  domain: "open_data" | "query_table",
): Promise<void> {
  const identity = parse(
    identitySchema,
    identityValue,
    "candidate IPNS update",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const intents = await transaction<{ domain: string; state: string }[]>`
        SELECT domain, state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
        ORDER BY domain FOR UPDATE
      `;
      const byDomain = new Map(intents.map((row) => [row.domain, row.state]));
      const admissible =
        domain === "open_data"
          ? byDomain.get("open_data") === "prior_confirmed" &&
            byDomain.get("query_table") === "prior_confirmed"
          : byDomain.get("open_data") === "verified" &&
            byDomain.get("query_table") === "prior_confirmed";
      if (
        current.state !== "executing" ||
        intents.length !== 2 ||
        !admissible
      ) {
        throw new DurableConflictError(
          "Candidate IPNS update violates durable domain ordering",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'update_in_flight', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = ${domain}
      `;
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "ipns_update_in_flight",
        {
          domain,
          demoPlanSha256: identity.demoPlanSha256,
        },
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateResolutionCycle(
  databaseUrl: string,
  requestValue: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
    observations: unknown;
  },
): Promise<CandidateResolutionCycleResult> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: requestValue.demoPlanId,
      demoPlanSha256: requestValue.demoPlanSha256,
    },
    "candidate resolution cycle",
  );
  const domain = z
    .enum(["open_data", "query_table"])
    .parse(requestValue.domain);
  const observations = validateCandidateResolutionObservations(
    requestValue.observations,
  );
  const observationsCanonical = canonicalJson(observations);
  const evidenceSha256 = sha256(observationsCanonical);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const plan = await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.approval_id === null) {
        throw new DurableConflictError(
          "Candidate resolution requires the exact approval",
        );
      }
      const effects = await transaction<
        { pending: string | number; total: string | number }[]
      >`
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status != 'verified') AS pending
        FROM oracle_candidate_demo_object_effects
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      if (
        Number(effects[0]?.total ?? 0) !== plan.objectCount ||
        Number(effects[0]?.pending ?? 1) !== 0
      ) {
        throw new DurableConflictError(
          "Candidate resolution requires all existing object checkpoints",
        );
      }
      const intents = await transaction<
        {
          domain: "open_data" | "query_table";
          intent_id: string;
          prior_cid: string;
          state: string;
          target_cid: string;
        }[]
      >`
        SELECT intent_id, domain, prior_cid, target_cid, state
        FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
        ORDER BY domain
        FOR UPDATE
      `;
      if (intents.length !== 2) {
        throw new DurableConflictError(
          "Candidate resolution requires both immutable intents",
        );
      }
      const intent = intents.find((entry) => entry.domain === domain);
      if (!intent) {
        throw new DurableConflictError(
          "Candidate resolution intent is missing",
        );
      }
      const target =
        domain === "open_data"
          ? plan.targets.openData
          : plan.targets.queryTable;
      if (
        intent.prior_cid !== target.priorCid ||
        intent.target_cid !== target.targetCid
      ) {
        throw new DurableConflictError(
          "Candidate resolution intent no longer matches the plan",
        );
      }
      const classification = classifyCandidateResolutionObservations({
        observations,
        priorCid: intent.prior_cid,
        targetCid: intent.target_cid,
      });
      const replay = await transaction<
        {
          classification: CandidateResolutionClassification;
          cycle_id: string;
          evidence_sha256: string;
          intent_id: string;
          observations_canonical: string;
          sequence: number;
        }[]
      >`
        SELECT cycle_id, intent_id, sequence, classification,
               evidence_sha256, observations_canonical
        FROM oracle_candidate_demo_resolution_cycles
        WHERE intent_id = ${intent.intent_id}
          AND evidence_sha256 = ${evidenceSha256}
      `;
      if (replay[0]) {
        if (
          replay[0].classification !== classification ||
          replay[0].observations_canonical !== observationsCanonical
        ) {
          throw new DurableConflictError(
            "Candidate resolution evidence replay conflict",
          );
        }
        return {
          classification,
          cycleId: replay[0].cycle_id,
          evidenceSha256,
          intentId: intent.intent_id,
          sequence: replay[0].sequence,
        };
      }
      const sequences = await transaction<{ next_sequence: number }[]>`
        SELECT (coalesce(max(sequence), 0) + 1)::int AS next_sequence
        FROM oracle_candidate_demo_resolution_cycles
        WHERE intent_id = ${intent.intent_id}
      `;
      const sequence = sequences[0]?.next_sequence ?? 1;
      const cycleId = deterministicId("democycle", [
        "1.0.0",
        identity.demoPlanId,
        identity.demoPlanSha256,
        intent.intent_id,
        domain,
        String(sequence),
      ]);
      await transaction`
        INSERT INTO oracle_candidate_demo_resolution_cycles (
          cycle_id, intent_id, demo_plan_id, demo_plan_sha256, domain,
          sequence, classification, evidence_sha256, observation_count,
          observations_canonical
        ) VALUES (
          ${cycleId}, ${intent.intent_id}, ${identity.demoPlanId},
          ${identity.demoPlanSha256}, ${domain}, ${sequence},
          ${classification}, ${evidenceSha256}, ${observations.length},
          ${observationsCanonical}
        )
      `;
      const allowedState =
        classification === "target_observed"
          ? ["update_in_flight", "update_ambiguous", "target_observed"]
          : classification === "prior_observed"
            ? ["update_in_flight", "update_ambiguous", "prior_confirmed"]
            : classification === "unexpected_cid"
              ? [
                  "update_in_flight",
                  "update_ambiguous",
                  "prior_confirmed",
                  "unexpected_cid",
                ]
              : ["update_in_flight", "prior_confirmed", "update_ambiguous"];
      if (!allowedState.includes(intent.state)) {
        throw new DurableConflictError(
          "Candidate resolution classification conflicts with intent state",
        );
      }
      const nextIntentState =
        classification === "target_observed"
          ? "target_observed"
          : classification === "prior_observed"
            ? "prior_confirmed"
            : classification === "unexpected_cid"
              ? "unexpected_cid"
              : "update_ambiguous";
      if (intent.state !== nextIntentState) {
        await transaction`
          UPDATE oracle_candidate_demo_ipns_intents
          SET state = ${nextIntentState}, revision = revision + 1
          WHERE intent_id = ${intent.intent_id}
        `;
      }
      const nextPlanState =
        classification === "target_observed" ||
        classification === "prior_observed"
          ? "executing"
          : "manual_intervention_required";
      if (current.state !== nextPlanState) {
        if (
          !["executing", "manual_intervention_required"].includes(current.state)
        ) {
          throw new DurableConflictError(
            "Candidate resolution cannot resume the current plan state",
          );
        }
        await transaction`
          UPDATE oracle_candidate_demo_plans
          SET state = ${nextPlanState}, revision = revision + 1,
              updated_at = now()
          WHERE demo_plan_id = ${identity.demoPlanId}
        `;
      }
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "resolution_cycle_recorded",
        {
          classification,
          cycleId,
          domain,
          evidenceSha256,
          intentId: intent.intent_id,
          observationCount: observations.length,
          sequence,
        },
      );
      return {
        classification,
        cycleId,
        evidenceSha256,
        intentId: intent.intent_id,
        sequence,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateSameTargetReassertionDecision(
  databaseUrl: string,
  requestValue: {
    controllerReference: string;
    cycleId: string;
    decidedAt: string;
    demoPlanId: string;
    demoPlanSha256: string;
  },
): Promise<void> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: requestValue.demoPlanId,
      demoPlanSha256: requestValue.demoPlanSha256,
    },
    "candidate reassertion decision",
  );
  const controllerReference = z
    .string()
    .regex(/^[a-z0-9][a-z0-9_-]{2,127}$/)
    .parse(requestValue.controllerReference);
  const cycleId = z
    .string()
    .regex(/^democycle_[a-f0-9]{32}$/)
    .parse(requestValue.cycleId);
  const decidedAt = z.string().datetime().parse(requestValue.decidedAt);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      const plan = await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const cycles = await transaction<
        { classification: string; observations_canonical: string }[]
      >`
        SELECT classification, observations_canonical
        FROM oracle_candidate_demo_resolution_cycles
        WHERE cycle_id = ${cycleId}
          AND demo_plan_id = ${identity.demoPlanId}
          AND domain = 'open_data'
      `;
      const intents = await transaction<{ state: string }[]>`
        SELECT state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
          AND domain = 'open_data'
        FOR UPDATE
      `;
      const observations = cycles[0]
        ? validateCandidateResolutionObservations(
            JSON.parse(cycles[0].observations_canonical),
          )
        : [];
      const [control, filebaseGateway, ipfsIo, dwebLink] = observations;
      const exactDecisionC =
        observations.length === 4 &&
        cycles[0]?.classification === "split" &&
        control?.observedCid === plan.targets.openData.targetCid &&
        [filebaseGateway, ipfsIo, dwebLink].every(
          (entry) =>
            entry?.outcome === "resolved" &&
            entry.observedCid === plan.targets.openData.priorCid,
        );
      if (
        current.state !== "manual_intervention_required" ||
        intents[0]?.state !== "update_ambiguous" ||
        !exactDecisionC
      ) {
        throw new DurableConflictError(
          "Candidate same-target reassertion does not match decision C",
        );
      }
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "manual_same_target_reassertion_authorized",
        {
          controllerReference,
          cycleId,
          decidedAt,
          demoPlanSha256: identity.demoPlanSha256,
          domain: "open_data",
          targetCid: plan.targets.openData.targetCid,
        },
      );
      await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'prior_confirmed', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId} AND domain = 'open_data'
      `;
      await transaction`
        UPDATE oracle_candidate_demo_plans
        SET state = 'executing', revision = revision + 1, updated_at = now()
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function assertCandidateIpnsMutationReady(
  databaseUrl: string,
  identityValue: unknown,
  domain: "open_data" | "query_table" = "open_data",
): Promise<void> {
  const identity = parse(
    identitySchema,
    identityValue,
    "candidate IPNS readiness",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const rows = await transaction<{ domain: string; state: string }[]>`
        SELECT domain, state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
        ORDER BY domain
      `;
      const byDomain = new Map(rows.map((row) => [row.domain, row.state]));
      const ordered =
        domain === "open_data"
          ? byDomain.get("open_data") === "prior_confirmed" &&
            byDomain.get("query_table") === "prior_confirmed"
          : byDomain.get("open_data") === "verified" &&
            byDomain.get("query_table") === "prior_confirmed";
      if (current.state !== "executing" || rows.length !== 2 || !ordered) {
        throw new DurableConflictError(
          "Candidate IPNS mutation requires durable intents in domain order",
        );
      }
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function checkpointCandidateIpnsVerified(
  databaseUrl: string,
  request: {
    demoPlanId: string;
    demoPlanSha256: string;
    domain: "open_data" | "query_table";
  },
): Promise<void> {
  const identity = parse(
    identitySchema,
    {
      demoPlanId: request.demoPlanId,
      demoPlanSha256: request.demoPlanSha256,
    },
    "candidate IPNS checkpoint",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      const rows = await transaction<{ domain: string; state: string }[]>`
        SELECT domain, state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
        ORDER BY domain
        FOR UPDATE
      `;
      const byDomain = new Map(rows.map((row) => [row.domain, row.state]));
      const predecessorValid =
        request.domain === "open_data"
          ? byDomain.get("open_data") === "target_observed" &&
            byDomain.get("query_table") === "prior_confirmed"
          : byDomain.get("open_data") === "verified" &&
            byDomain.get("query_table") === "target_observed";
      if (current.state !== "executing" || !predecessorValid) {
        throw new DurableConflictError(
          "Candidate IPNS verification violates domain ordering",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'verified', revision = revision + 1
        WHERE demo_plan_id = ${identity.demoPlanId}
          AND domain = ${request.domain}
      `;
      await recordEvent(transaction, identity.demoPlanId, "ipns_verified", {
        domain: request.domain,
        demoPlanSha256: identity.demoPlanSha256,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function completeCandidateDemoPlan(
  databaseUrl: string,
  identityValue: unknown,
): Promise<CandidateDemoState> {
  const identity = parse(identitySchema, identityValue, "candidate completion");
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const plan = await loadPlan(transaction, identity);
      const current = await loadState(transaction, identity);
      if (current.state === "completed") return state(current);
      const objectRows = await transaction<
        { pending: string | number; total: string | number }[]
      >`
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status != 'verified') AS pending
        FROM oracle_candidate_demo_object_effects
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      const intentRows = await transaction<{ domain: string; state: string }[]>`
        SELECT domain, state FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      if (
        current.state !== "executing" ||
        Number(objectRows[0]?.total ?? 0) !== plan.objectCount ||
        Number(objectRows[0]?.pending ?? 1) !== 0 ||
        intentRows.length !== 2 ||
        intentRows.some((row) => row.state !== "verified") ||
        new Set(intentRows.map((row) => row.domain)).size !== 2
      ) {
        throw new DurableConflictError(
          "Candidate completion requires exact verified objects and both IPNS targets",
        );
      }
      await transaction`
        UPDATE oracle_candidate_demo_plans
        SET state = 'completed', revision = revision + 1, updated_at = now()
        WHERE demo_plan_id = ${identity.demoPlanId}
      `;
      await recordEvent(
        transaction,
        identity.demoPlanId,
        "publication_completed",
        {
          demoPlanSha256: identity.demoPlanSha256,
          revision: current.revision + 1,
        },
      );
      return state({
        ...current,
        revision: current.revision + 1,
        state: "completed",
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
