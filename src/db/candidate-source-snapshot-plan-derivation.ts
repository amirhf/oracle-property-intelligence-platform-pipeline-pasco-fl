import postgres from "postgres";
import { z } from "zod";

import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/u);
const planIdSchema = z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/u);
const instantSchema = z
  .string()
  .datetime({ offset: false, precision: 3 })
  .refine((value) => new Date(value).toISOString() === value);

const derivationIdentitySchema = z.strictObject({
  derivationId: z.string().regex(/^snapshotdemoderivation_[a-f0-9]{32}$/u),
  derivationSha256: sha256Schema,
  derivedAt: instantSchema,
  derivedEnvelopeSha256: sha256Schema,
  derivedPlanId: planIdSchema,
  derivedPlanSha256: sha256Schema,
  predecessorEnvelopeSha256: sha256Schema,
  predecessorPlanId: planIdSchema,
  predecessorPlanSha256: sha256Schema,
  reason: z.literal("request_envelope_replacement"),
  schemaVersion: z.literal("candidate-source-snapshot-plan-derivation-v1"),
  sharedBindingSha256: sha256Schema,
});

export type CandidateSourceSnapshotPlanDerivationIdentity = z.infer<
  typeof derivationIdentitySchema
>;

const storedPlanSchema = z.strictObject({
  plan_id: planIdSchema,
  plan_payload: z.record(z.string(), z.unknown()),
  plan_sha256: sha256Schema,
  plan_version: z.enum(["2.0.0", "2.1.0"]),
  request_envelope: z.unknown(),
  revision: z.number().int().positive(),
  state: z.enum([
    "awaiting_configuration",
    "awaiting_approval",
    "approved",
    "executing",
    "completed",
    "manual_intervention_required",
    "failed_terminal",
  ]),
});

type StoredPlan = z.infer<typeof storedPlanSchema>;

function sharedPlanBinding(payload: Record<string, unknown>): unknown {
  const {
    costEnvelope: _costEnvelope,
    formatPadding: _formatPadding,
    limits,
    planId: _planId,
    planSha256: _planSha256,
    requestEnvelope: _requestEnvelope,
    version: _version,
    ...shared
  } = payload;
  const parsedLimits = z.record(z.string(), z.unknown()).parse(limits);
  const { maxRequests: _maxRequests, ...sharedLimits } = parsedLimits;
  return { ...shared, limits: sharedLimits };
}

export function createCandidateSourceSnapshotPlanDerivationIdentity(input: {
  derivedAt: string;
  derivedPlan: Pick<
    StoredPlan,
    "plan_id" | "plan_payload" | "plan_sha256" | "request_envelope"
  >;
  predecessorPlan: Pick<
    StoredPlan,
    "plan_id" | "plan_payload" | "plan_sha256" | "request_envelope"
  >;
}): CandidateSourceSnapshotPlanDerivationIdentity {
  const derivedAt = instantSchema.parse(input.derivedAt);
  const predecessorPlanId = planIdSchema.parse(input.predecessorPlan.plan_id);
  const predecessorPlanSha256 = sha256Schema.parse(
    input.predecessorPlan.plan_sha256,
  );
  const derivedPlanId = planIdSchema.parse(input.derivedPlan.plan_id);
  const derivedPlanSha256 = sha256Schema.parse(input.derivedPlan.plan_sha256);
  const predecessorSharedBinding = sharedPlanBinding(
    input.predecessorPlan.plan_payload,
  );
  const derivedSharedBinding = sharedPlanBinding(
    input.derivedPlan.plan_payload,
  );
  const sharedBindingSha256 = canonicalJsonSha256(predecessorSharedBinding);
  if (
    canonicalJsonSha256(derivedSharedBinding) !== sharedBindingSha256 ||
    predecessorPlanId === derivedPlanId ||
    predecessorPlanSha256 === derivedPlanSha256
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot derivation changes more than its request and cost envelope",
    );
  }
  const predecessorEnvelopeSha256 = canonicalJsonSha256(
    input.predecessorPlan.request_envelope,
  );
  const derivedEnvelopeSha256 = canonicalJsonSha256(
    input.derivedPlan.request_envelope,
  );
  if (predecessorEnvelopeSha256 === derivedEnvelopeSha256) {
    throw new DurableInputError(
      "Candidate source-snapshot derivation requires a changed request envelope",
    );
  }
  const derivationPayload = {
    derivedAt,
    derivedEnvelopeSha256,
    derivedPlanId,
    derivedPlanSha256,
    predecessorEnvelopeSha256,
    predecessorPlanId,
    predecessorPlanSha256,
    reason: "request_envelope_replacement" as const,
    schemaVersion: "candidate-source-snapshot-plan-derivation-v1" as const,
    sharedBindingSha256,
  };
  const derivationSha256 = canonicalJsonSha256(derivationPayload);
  return derivationIdentitySchema.parse({
    ...derivationPayload,
    derivationId: deterministicId("snapshotdemoderivation", [
      derivationPayload.schemaVersion,
      predecessorPlanId,
      derivedPlanId,
      derivationSha256,
    ]),
    derivationSha256,
  });
}

export function assertExactCandidateSourceSnapshotPlanDerivationReplay(
  expected: CandidateSourceSnapshotPlanDerivationIdentity,
  stored: CandidateSourceSnapshotPlanDerivationIdentity,
): void {
  if (canonicalJsonSha256(stored) !== canonicalJsonSha256(expected)) {
    throw new DurableConflictError(
      "Candidate source-snapshot plan derivation replay conflicts with durable identity",
    );
  }
}

interface DerivationRow {
  derivation_id: string;
  derivation_sha256: string;
  derived_at_iso: string;
  derived_envelope_sha256: string;
  derived_plan_id: string;
  derived_plan_sha256: string;
  predecessor_envelope_sha256: string;
  predecessor_plan_id: string;
  predecessor_plan_sha256: string;
  reason: "request_envelope_replacement";
  shared_binding_sha256: string;
}

function rowIdentity(
  row: DerivationRow,
): CandidateSourceSnapshotPlanDerivationIdentity {
  return derivationIdentitySchema.parse({
    derivationId: row.derivation_id,
    derivationSha256: row.derivation_sha256,
    derivedAt: row.derived_at_iso,
    derivedEnvelopeSha256: row.derived_envelope_sha256,
    derivedPlanId: row.derived_plan_id,
    derivedPlanSha256: row.derived_plan_sha256,
    predecessorEnvelopeSha256: row.predecessor_envelope_sha256,
    predecessorPlanId: row.predecessor_plan_id,
    predecessorPlanSha256: row.predecessor_plan_sha256,
    reason: row.reason,
    schemaVersion: "candidate-source-snapshot-plan-derivation-v1",
    sharedBindingSha256: row.shared_binding_sha256,
  });
}

/**
 * Records one effect-free v2.0 -> v2.1 envelope replacement. Migration 029
 * locks both plans, independently reconstructs this identity, terminalizes the
 * predecessor for audit, and rejects any predecessor with approval or effects.
 * This API deliberately records no approval and cannot enter execution.
 */
export async function recordCandidateSourceSnapshotPlanDerivation(
  databaseUrl: string,
  input: {
    derivedAt: string;
    derivedPlanId: string;
    derivedPlanSha256: string;
    predecessorPlanId: string;
    predecessorPlanSha256: string;
  },
): Promise<CandidateSourceSnapshotPlanDerivationIdentity> {
  const parsed = z
    .strictObject({
      derivedAt: instantSchema,
      derivedPlanId: planIdSchema,
      derivedPlanSha256: sha256Schema,
      predecessorPlanId: planIdSchema,
      predecessorPlanSha256: sha256Schema,
    })
    .parse(input);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await transaction`SELECT pg_advisory_xact_lock(
        hashtext('oracle-candidate-source-snapshot-demo-v2'), hashtext('pasco')
      )`;
      const planRows = await transaction<StoredPlan[]>`
        SELECT plan_id, plan_sha256, plan_version, plan_payload,
               request_envelope, state, revision
        FROM oracle_candidate_source_snapshot_demo_plans
        WHERE plan_id IN (${parsed.predecessorPlanId}, ${parsed.derivedPlanId})
        ORDER BY plan_id
        FOR UPDATE
      `;
      const predecessor = planRows.find(
        (plan) => plan.plan_id === parsed.predecessorPlanId,
      );
      const derived = planRows.find(
        (plan) => plan.plan_id === parsed.derivedPlanId,
      );
      if (!predecessor || !derived) {
        throw new DurableInputError(
          "Candidate source-snapshot derivation requires both durable plans",
        );
      }
      const predecessorPlan = storedPlanSchema.parse(predecessor);
      const derivedPlan = storedPlanSchema.parse(derived);
      if (
        predecessorPlan.plan_sha256 !== parsed.predecessorPlanSha256 ||
        derivedPlan.plan_sha256 !== parsed.derivedPlanSha256
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot derivation plan identity conflicts with durable state",
        );
      }
      const identity = createCandidateSourceSnapshotPlanDerivationIdentity({
        derivedAt: parsed.derivedAt,
        derivedPlan,
        predecessorPlan,
      });
      const existing = await transaction<DerivationRow[]>`
        SELECT derivation_id, derivation_sha256, derived_at_iso,
               derived_envelope_sha256, derived_plan_id, derived_plan_sha256,
               predecessor_envelope_sha256, predecessor_plan_id,
               predecessor_plan_sha256, reason, shared_binding_sha256
        FROM oracle_candidate_source_snapshot_demo_plan_derivations
        WHERE predecessor_plan_id = ${parsed.predecessorPlanId}
           OR derived_plan_id = ${parsed.derivedPlanId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (existing.length !== 1) {
          throw new DurableConflictError(
            "Candidate source-snapshot plan derivation identity is not unique",
          );
        }
        assertExactCandidateSourceSnapshotPlanDerivationReplay(
          identity,
          rowIdentity(existing[0]),
        );
        return identity;
      }
      if (
        predecessorPlan.plan_version !== "2.0.0" ||
        ![
          "awaiting_configuration",
          "awaiting_approval",
          "failed_terminal",
        ].includes(predecessorPlan.state) ||
        derivedPlan.plan_version !== "2.1.0" ||
        derivedPlan.state !== "awaiting_configuration" ||
        derivedPlan.revision !== 1
      ) {
        throw new DurableInputError(
          "Candidate source-snapshot derivation requires an audit-only v2 predecessor and unexecuted v2.1 successor",
        );
      }
      const effectRows = await transaction<{ effect_count: string }[]>`
        SELECT (
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_approvals
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_requests
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_upload_attempts
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_inspections
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_ipns_intents
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_upload_closures
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_remote_checks
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_remote_verifications
            WHERE plan_id = ${parsed.predecessorPlanId}) +
          (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_objects
            WHERE plan_id = ${parsed.predecessorPlanId}
              AND (status <> 'pending' OR successful_effect_count <> 0))
        )::text AS effect_count
      `;
      if (Number(effectRows[0]?.effect_count ?? 0) !== 0) {
        throw new DurableConflictError(
          "Candidate source-snapshot predecessor has approval or remote-effect evidence",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_plan_derivations (
          derivation_id, predecessor_plan_id, predecessor_plan_sha256,
          derived_plan_id, derived_plan_sha256, reason,
          shared_binding_sha256, predecessor_envelope_sha256,
          derived_envelope_sha256, derivation_sha256, derived_at,
          derived_at_iso
        ) VALUES (
          ${identity.derivationId}, ${identity.predecessorPlanId},
          ${identity.predecessorPlanSha256}, ${identity.derivedPlanId},
          ${identity.derivedPlanSha256}, ${identity.reason},
          ${identity.sharedBindingSha256},
          ${identity.predecessorEnvelopeSha256},
          ${identity.derivedEnvelopeSha256}, ${identity.derivationSha256},
          ${identity.derivedAt}, ${identity.derivedAt}
        )
      `;
      return identity;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

/**
 * Discovers the sole compatible legacy v2.0 plan for an already-recorded v2.1
 * successor and records the exact effect-free derivation. No compatible
 * predecessor is an explicit no-op; more than one is an ambiguity hard stop.
 * The derivation timestamp comes from the immutable successor plan's verified
 * rollback evidence, so approval-time callers cannot make replay identity
 * depend on wall-clock input.
 */
export async function recordCompatibleCandidateSourceSnapshotPlanDerivation(
  databaseUrl: string,
  inputValue: {
    derivedPlanId: string;
    derivedPlanSha256: string;
  },
): Promise<CandidateSourceSnapshotPlanDerivationIdentity | null> {
  const input = z
    .strictObject({
      derivedPlanId: planIdSchema,
      derivedPlanSha256: sha256Schema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  let predecessors: {
    derived_at: string;
    plan_id: string;
    plan_sha256: string;
  }[];
  try {
    predecessors = await sql<
      { derived_at: string; plan_id: string; plan_sha256: string }[]
    >`
      SELECT predecessor.plan_id, predecessor.plan_sha256,
             derived.plan_payload #>> '{protectedSampleRollback,verifiedAt}'
               AS derived_at
      FROM oracle_candidate_source_snapshot_demo_plans predecessor
      JOIN oracle_candidate_source_snapshot_demo_plans derived
        ON derived.plan_id = ${input.derivedPlanId}
       AND derived.plan_sha256 = ${input.derivedPlanSha256}
      WHERE predecessor.plan_version = '2.0.0'
        AND oracle_candidate_source_snapshot_shared_binding_v1(
              predecessor.plan_payload
            ) = oracle_candidate_source_snapshot_shared_binding_v1(
              derived.plan_payload
            )
      ORDER BY predecessor.plan_id
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
  if (predecessors.length === 0) return null;
  if (predecessors.length !== 1) {
    throw new DurableConflictError(
      "Candidate source-snapshot v2.1 plan has ambiguous compatible predecessors",
    );
  }
  return recordCandidateSourceSnapshotPlanDerivation(databaseUrl, {
    derivedAt: instantSchema.parse(predecessors[0]!.derived_at),
    derivedPlanId: input.derivedPlanId,
    derivedPlanSha256: input.derivedPlanSha256,
    predecessorPlanId: predecessors[0]!.plan_id,
    predecessorPlanSha256: predecessors[0]!.plan_sha256,
  });
}
