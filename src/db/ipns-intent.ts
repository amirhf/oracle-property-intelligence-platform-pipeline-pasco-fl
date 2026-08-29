import postgres from "postgres";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";
import { CIDV0_PATTERN } from "../publication/ipfs-cid.js";
import { validatePublicationPlan } from "../publication/plan.js";
import { pascoPublishAdvisoryLockKey } from "./publication-durability.js";
import { acquirePascoProjectionHeadFence } from "./projection-head-fence.js";

export const IPNS_INTENT_STATES = [
  "intent_recorded",
  "prior_confirmed",
  "update_in_flight",
  "update_ambiguous",
  "mutation_acknowledged",
  "verification_pending",
  "target_observed",
  "verified",
  "rollback_requested",
  "rollback_in_flight",
  "rollback_ambiguous",
  "rollback_verified",
  "manual_intervention_required",
  "cancelled_terminal",
  "failed_terminal",
] as const;

export const IPNS_TRANSITION_EVENTS = [
  "prior_observed",
  "target_observed",
  "split_prior_target",
  "timeout_transport_uncertainty",
  "unexpected_third_cid",
  "update_started",
  "mutation_acknowledged",
  "mutation_failed",
  "verification_pending",
  "verified",
  "rollback_requested",
  "rollback_started",
  "rollback_verified",
  "manual_intervention_required",
  "terminal_cancellation",
  "terminal_failure",
] as const;

export type IpnsIntentState = (typeof IPNS_INTENT_STATES)[number];
export type IpnsTransitionEvent = (typeof IPNS_TRANSITION_EVENTS)[number];
export type IpnsDomain = "open_data" | "query_table";

const TRANSITION_OVERRIDES: Partial<
  Record<IpnsIntentState, Partial<Record<IpnsTransitionEvent, IpnsIntentState>>>
> = {
  intent_recorded: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    terminal_cancellation: "cancelled_terminal",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  manual_intervention_required: {
    manual_intervention_required: "manual_intervention_required",
    terminal_cancellation: "cancelled_terminal",
    terminal_failure: "failed_terminal",
  },
  mutation_acknowledged: {
    prior_observed: "update_ambiguous",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
    verification_pending: "verification_pending",
  },
  prior_confirmed: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    terminal_cancellation: "cancelled_terminal",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
    update_started: "update_in_flight",
  },
  rollback_ambiguous: {
    prior_observed: "rollback_verified",
    rollback_started: "rollback_in_flight",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_in_flight: {
    mutation_acknowledged: "rollback_in_flight",
    mutation_failed: "manual_intervention_required",
    prior_observed: "rollback_verified",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_requested: {
    prior_observed: "rollback_verified",
    rollback_requested: "rollback_requested",
    rollback_started: "rollback_in_flight",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_verified: {
    prior_observed: "rollback_verified",
    rollback_verified: "rollback_verified",
    unexpected_third_cid: "manual_intervention_required",
  },
  target_observed: {
    rollback_requested: "rollback_requested",
    split_prior_target: "verification_pending",
    target_observed: "target_observed",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "verification_pending",
    unexpected_third_cid: "manual_intervention_required",
    verification_pending: "verification_pending",
    verified: "verified",
  },
  update_ambiguous: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  update_in_flight: {
    mutation_acknowledged: "mutation_acknowledged",
    mutation_failed: "failed_terminal",
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  verification_pending: {
    prior_observed: "update_ambiguous",
    split_prior_target: "verification_pending",
    target_observed: "target_observed",
    terminal_failure: "failed_terminal",
    timeout_transport_uncertainty: "verification_pending",
    unexpected_third_cid: "manual_intervention_required",
    verified: "verified",
  },
  verified: {
    rollback_requested: "rollback_requested",
    target_observed: "verified",
    unexpected_third_cid: "manual_intervention_required",
    verified: "verified",
  },
};

/**
 * Total state/event table. A null cell is a deterministic illegal transition;
 * callers cannot invent a recovery edge outside this table.
 */
export const IPNS_TRANSITION_TABLE: Readonly<
  Record<
    IpnsIntentState,
    Readonly<Record<IpnsTransitionEvent, IpnsIntentState | null>>
  >
> = Object.freeze(
  Object.fromEntries(
    IPNS_INTENT_STATES.map((state) => [
      state,
      Object.freeze(
        Object.fromEntries(
          IPNS_TRANSITION_EVENTS.map((event) => [
            event,
            TRANSITION_OVERRIDES[state]?.[event] ?? null,
          ]),
        ) as Record<IpnsTransitionEvent, IpnsIntentState | null>,
      ),
    ]),
  ) as Record<
    IpnsIntentState,
    Readonly<Record<IpnsTransitionEvent, IpnsIntentState | null>>
  >,
);

export function nextIpnsIntentState(
  state: IpnsIntentState,
  event: IpnsTransitionEvent,
): IpnsIntentState {
  const next = IPNS_TRANSITION_TABLE[state][event];
  if (next === null) {
    throw new DurableConflictError(
      `IPNS intent conflict (invalid transition ${state} + ${event})`,
    );
  }
  return next;
}

const cidSchema = z.string().regex(CIDV0_PATTERN);
const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const intentIdSchema = z.string().regex(/^ipnsintent_[a-f0-9]{32}$/);
const attemptIdSchema = z.string().regex(/^ipnsattempt_[a-f0-9]{32}$/);
const recoverySequenceSchema = z.number().int().min(1).max(1_000_000);

export const IPNS_RECOVERY_RECEIPT_SCHEMA_VERSION = "1.0.0" as const;
export const IPNS_RECOVERY_MAX_RESPONSE_BYTES = 16_777_216;
export const IPNS_RECOVERY_MAX_LATENCY_MS = 300_000;

const receiptOutcomeSchema = z.enum([
  "resolved",
  "unavailable",
  "http_error",
  "timeout",
  "transport_error",
]);
const receiptErrorCodeSchema = z.enum([
  "http_error",
  "invalid_response",
  "provider_unavailable",
  "rate_limited",
  "timeout",
  "transport_error",
]);
type ReceiptOutcome = z.infer<typeof receiptOutcomeSchema>;
type ReceiptErrorCode = z.infer<typeof receiptErrorCodeSchema>;

function receiptOutcomeSemanticsAreValid(value: {
  errorCode: ReceiptErrorCode | null;
  httpStatus: number | null;
  outcome: ReceiptOutcome;
}): boolean {
  switch (value.outcome) {
    case "resolved":
      return (
        value.errorCode === null &&
        value.httpStatus !== null &&
        value.httpStatus >= 200 &&
        value.httpStatus <= 299
      );
    case "unavailable":
      return (
        value.errorCode === "provider_unavailable" && value.httpStatus === null
      );
    case "http_error":
      return (
        (value.errorCode === "http_error" ||
          value.errorCode === "rate_limited") &&
        value.httpStatus !== null &&
        value.httpStatus >= 400 &&
        value.httpStatus <= 599
      );
    case "timeout":
      return value.errorCode === "timeout" && value.httpStatus === null;
    case "transport_error":
      return value.errorCode === "transport_error" && value.httpStatus === null;
  }
}

export const ipnsRecoveryReceiptSchema = z
  .strictObject({
    errorCode: receiptErrorCodeSchema.nullable(),
    httpStatus: z.number().int().min(100).max(599).nullable(),
    latencyMs: z.number().int().min(0).max(IPNS_RECOVERY_MAX_LATENCY_MS),
    outcome: receiptOutcomeSchema,
    providerRequestIdHash: sha256Schema.nullable(),
    responseBodyHash: sha256Schema.nullable(),
    responseBytes: z
      .number()
      .int()
      .min(0)
      .max(IPNS_RECOVERY_MAX_RESPONSE_BYTES),
    schemaVersion: z.literal(IPNS_RECOVERY_RECEIPT_SCHEMA_VERSION),
  })
  .superRefine((value, context) => {
    if (!receiptOutcomeSemanticsAreValid(value)) {
      context.addIssue({
        code: "custom",
        message: "receipt outcome, errorCode, and httpStatus disagree",
        path: ["outcome"],
      });
    }
  });

const resolvedObservationSchema = z.strictObject({
  endpointId: z.string().regex(/^[a-z0-9][a-z0-9_.:-]{2,127}$/),
  observedAt: z.string().datetime({ offset: true }),
  observedCid: cidSchema,
  receipt: ipnsRecoveryReceiptSchema,
  resolverKind: z.enum(["filebase_control_plane", "public_gateway"]),
});
const recoveryObservationSchema = z
  .strictObject({
    classification: z.enum(["resolved", "unavailable", "error"]).optional(),
    endpointId: z.string().regex(/^[a-z0-9][a-z0-9_.:-]{2,127}$/),
    observedAt: z.string().datetime({ offset: true }),
    observedCid: cidSchema.nullable().optional(),
    receipt: ipnsRecoveryReceiptSchema,
    resolverKind: z.enum(["filebase_control_plane", "public_gateway"]),
  })
  .superRefine((value, context) => {
    const classification = value.classification ?? "resolved";
    if (classification === "resolved" && !value.observedCid) {
      context.addIssue({
        code: "custom",
        message: "resolved observation requires observedCid",
        path: ["observedCid"],
      });
    }
    if (classification !== "resolved" && value.observedCid !== undefined) {
      context.addIssue({
        code: "custom",
        message: "unavailable/error observation cannot contain observedCid",
        path: ["observedCid"],
      });
    }
    const receiptMatchesClassification =
      (classification === "resolved" && value.receipt.outcome === "resolved") ||
      (classification === "unavailable" &&
        value.receipt.outcome === "unavailable") ||
      (classification === "error" &&
        ["http_error", "timeout", "transport_error"].includes(
          value.receipt.outcome,
        ));
    if (!receiptMatchesClassification) {
      context.addIssue({
        code: "custom",
        message: "observation classification and receipt outcome disagree",
        path: ["receipt", "outcome"],
      });
    }
  });
const evidenceSchema = z.strictObject({
  observations: z.array(resolvedObservationSchema).min(2),
});
const intentTargetSchema = z.strictObject({
  domain: z.enum(["open_data", "query_table"]),
  intendedAt: z.string().datetime({ offset: true }),
  priorCid: cidSchema,
  resolutionEvidence: evidenceSchema,
});
const recordIntentsSchema = z.strictObject({
  county: z.literal("pasco"),
  planId: z.string().regex(/^plan_[a-f0-9]{32}$/),
  planSha256: sha256Schema,
  targets: z.tuple([intentTargetSchema, intentTargetSchema]),
});

export interface IpnsIntentView {
  approvedTargetCid: string;
  domain: IpnsDomain;
  intentId: string;
  intentSha256: string;
  priorCid: string;
  revision: number;
  state: IpnsIntentState;
}

interface IntentRow {
  approved_target_cid: string;
  domain: IpnsDomain;
  intent_id: string;
  intent_sha256: string;
  prior_cid: string;
  publication_plan_id: string;
  publication_plan_sha256: string;
  revision: number;
  state: IpnsIntentState;
}

interface CanonicalResolverObservation {
  classification: "error" | "resolved" | "unavailable";
  endpointId: string;
  observedAt: string;
  observedCid: string | null;
  ordinal: number;
  receipt: z.infer<typeof ipnsRecoveryReceiptSchema>;
  resolverKind: "filebase_control_plane" | "public_gateway";
}

function parse<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const result = schema.safeParse(value);
  if (!result.success) {
    const issue = result.error.issues[0];
    throw new DurableInputError(
      `${label} failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  return result.data;
}

function view(row: IntentRow): IpnsIntentView {
  return {
    approvedTargetCid: row.approved_target_cid,
    domain: row.domain,
    intentId: row.intent_id,
    intentSha256: row.intent_sha256,
    priorCid: row.prior_cid,
    revision: row.revision,
    state: row.state,
  };
}

function terminal(message: string): never {
  throw new DurableConflictError(`IPNS intent conflict (${message})`);
}

function canonicalResolverObservations(
  observations: readonly z.infer<typeof recoveryObservationSchema>[],
): CanonicalResolverObservation[] {
  return observations.map((observation, ordinal) => ({
    classification: observation.classification ?? "resolved",
    endpointId: observation.endpointId,
    observedAt: new Date(observation.observedAt).toISOString(),
    observedCid: observation.observedCid ?? null,
    ordinal,
    receipt: observation.receipt,
    resolverKind: observation.resolverKind,
  }));
}

function canonicalReceiptEvidence(
  observations: readonly CanonicalResolverObservation[],
): readonly {
  ordinal: number;
  receipt: CanonicalResolverObservation["receipt"];
}[] {
  return observations.map(({ ordinal, receipt }) => ({ ordinal, receipt }));
}

function deriveResolutionCycleId(
  row: Pick<
    IntentRow,
    "domain" | "intent_id" | "publication_plan_id" | "publication_plan_sha256"
  >,
  attemptId: string | null,
  recoverySequence: number,
): string {
  return deterministicId("resolution", [
    "1.0.0",
    "Publish/pasco/ipns-resolution-cycle",
    row.publication_plan_id,
    row.publication_plan_sha256,
    row.intent_id,
    row.domain,
    attemptId ?? "none",
    String(recoverySequence),
  ]);
}

function classifyPersistedObservations(
  row: Pick<IntentRow, "approved_target_cid" | "prior_cid">,
  observations: readonly CanonicalResolverObservation[],
): IpnsTransitionEvent {
  const resolved = observations.filter(
    (observation) => observation.classification === "resolved",
  );
  const observed = new Set(
    resolved.map((observation) => observation.observedCid),
  );
  const third = [...observed].find(
    (cid) => cid !== row.prior_cid && cid !== row.approved_target_cid,
  );
  if (third) return "unexpected_third_cid";
  if (observed.has(row.prior_cid) && observed.has(row.approved_target_cid)) {
    return "split_prior_target";
  }
  if (observations.some((item) => item.classification !== "resolved")) {
    return "timeout_transport_uncertainty";
  }
  if (observed.size === 1 && observed.has(row.prior_cid)) {
    return "prior_observed";
  }
  if (observed.size === 1 && observed.has(row.approved_target_cid)) {
    return "target_observed";
  }
  throw new DurableInputError("Recovery observations cannot be classified");
}

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  const [namespaceKey, countyKey] = pascoPublishAdvisoryLockKey();
  await transaction`SELECT pg_advisory_xact_lock(${namespaceKey}, ${countyKey})`;
  await acquirePascoProjectionHeadFence(transaction);
}

async function authorizeIntentMutation(
  transaction: postgres.TransactionSql,
  intentId: string,
): Promise<void> {
  await transaction`SELECT set_config('prism.ipns_intent_id', ${intentId}, true)`;
}

async function rows(
  sql: postgres.Sql | postgres.TransactionSql,
  planId: string,
): Promise<IntentRow[]> {
  return sql<IntentRow[]>`
    SELECT intent.intent_id, intent.intent_sha256, intent.domain,
           intent.prior_cid, intent.approved_target_cid,
           intent.publication_plan_id, intent.publication_plan_sha256,
           state.state, state.revision
    FROM oracle_publication_ipns_intents intent
    JOIN oracle_publication_ipns_intent_state state USING (intent_id)
    WHERE intent.publication_plan_id = ${planId}
    ORDER BY CASE intent.domain WHEN 'open_data' THEN 1 ELSE 2 END
  `;
}

function verifyEvidence(
  domain: IpnsDomain,
  priorCid: string,
  evidence: z.infer<typeof evidenceSchema>,
): string {
  const kinds = new Set(evidence.observations.map((item) => item.resolverKind));
  const cids = new Set(evidence.observations.map((item) => item.observedCid));
  if (
    !kinds.has("filebase_control_plane") ||
    !kinds.has("public_gateway") ||
    cids.size !== 1 ||
    !cids.has(priorCid)
  ) {
    throw new DurableInputError(
      `Initial ${domain} resolution must have provider and public resolvers agreeing on priorCid`,
    );
  }
  return canonicalJsonSha256(evidence);
}

interface ResolutionCycleRow {
  attempt_id: string | null;
  classification: IpnsTransitionEvent;
  cycle_sequence: number;
  domain: IpnsDomain;
  evidence_sha256: string;
  intent_id: string;
  observation_count: number;
  observations_canonical: string;
  receipt_identity_sha256: string;
  receipts_canonical: string;
  resolution_cycle_id: string;
}

interface ResolutionCycleResult {
  event: IpnsTransitionEvent;
  metadata: Record<string, unknown>;
  replay: boolean;
}

function resolutionCycleMatches(
  stored: ResolutionCycleRow,
  expected: ResolutionCycleRow,
): boolean {
  return (
    stored.resolution_cycle_id === expected.resolution_cycle_id &&
    stored.intent_id === expected.intent_id &&
    stored.domain === expected.domain &&
    stored.attempt_id === expected.attempt_id &&
    stored.cycle_sequence === expected.cycle_sequence &&
    stored.evidence_sha256 === expected.evidence_sha256 &&
    stored.observation_count === expected.observation_count &&
    stored.classification === expected.classification &&
    stored.observations_canonical === expected.observations_canonical &&
    stored.receipt_identity_sha256 === expected.receipt_identity_sha256 &&
    stored.receipts_canonical === expected.receipts_canonical
  );
}

async function persistResolutionCycle(
  transaction: postgres.TransactionSql,
  row: IntentRow,
  request: {
    attemptId: string | null;
    observations: readonly z.infer<typeof recoveryObservationSchema>[];
    recoverySequence: number;
  },
): Promise<ResolutionCycleResult> {
  const canonicalObservations = canonicalResolverObservations(
    request.observations,
  );
  const observationsCanonical = canonicalJson(canonicalObservations);
  const evidenceSha256 = canonicalJsonSha256(canonicalObservations);
  const receipts = canonicalReceiptEvidence(canonicalObservations);
  const receiptsCanonical = canonicalJson(receipts);
  const receiptIdentitySha256 = canonicalJsonSha256(receipts);
  const classification = classifyPersistedObservations(
    row,
    canonicalObservations,
  );
  const resolutionCycleId = deriveResolutionCycleId(
    row,
    request.attemptId,
    request.recoverySequence,
  );
  const expected: ResolutionCycleRow = {
    attempt_id: request.attemptId,
    classification,
    cycle_sequence: request.recoverySequence,
    domain: row.domain,
    evidence_sha256: evidenceSha256,
    intent_id: row.intent_id,
    observation_count: canonicalObservations.length,
    observations_canonical: observationsCanonical,
    receipt_identity_sha256: receiptIdentitySha256,
    receipts_canonical: receiptsCanonical,
    resolution_cycle_id: resolutionCycleId,
  };
  const inserted = await transaction`
    INSERT INTO oracle_publication_ipns_resolution_cycles (
      resolution_cycle_id, intent_id, domain, attempt_id, intent_revision,
      cycle_sequence, evidence_sha256, observation_count,
      observations_canonical, classification, receipts_canonical,
      receipt_identity_sha256
    ) VALUES (
      ${resolutionCycleId}, ${row.intent_id}, ${row.domain},
      ${request.attemptId}, ${row.revision}, ${request.recoverySequence},
      ${evidenceSha256}, ${canonicalObservations.length},
      ${observationsCanonical}, ${classification}, ${receiptsCanonical},
      ${receiptIdentitySha256}
    ) ON CONFLICT DO NOTHING
    RETURNING resolution_cycle_id
  `;
  const candidates = await transaction<ResolutionCycleRow[]>`
    SELECT resolution_cycle_id, intent_id, domain, attempt_id,
           cycle_sequence, evidence_sha256, observation_count,
           observations_canonical, classification, receipts_canonical,
           receipt_identity_sha256
    FROM oracle_publication_ipns_resolution_cycles
    WHERE resolution_cycle_id = ${resolutionCycleId}
       OR (intent_id = ${row.intent_id} AND evidence_sha256 = ${evidenceSha256})
       OR (
         intent_id = ${row.intent_id}
         AND domain = ${row.domain}
         AND attempt_id IS NOT DISTINCT FROM ${request.attemptId}
         AND cycle_sequence = ${request.recoverySequence}
       )
    ORDER BY resolution_cycle_id
  `;
  const storedById = candidates.find(
    (candidate) => candidate.resolution_cycle_id === resolutionCycleId,
  );
  const replay = inserted.length === 0;
  if (!storedById || !resolutionCycleMatches(storedById, expected)) {
    const duplicateEvidence = candidates.find(
      (candidate) =>
        candidate.intent_id === row.intent_id &&
        candidate.evidence_sha256 === evidenceSha256 &&
        candidate.resolution_cycle_id !== resolutionCycleId,
    );
    if (duplicateEvidence) {
      terminal(
        "canonical recovery evidence was already recorded under another cycle",
      );
    }
    terminal("resolution cycle identity or evidence changed");
  }
  const persisted = JSON.parse(
    storedById.observations_canonical,
  ) as CanonicalResolverObservation[];
  const persistedReceipts = JSON.parse(
    storedById.receipts_canonical,
  ) as ReturnType<typeof canonicalReceiptEvidence>;
  const reconstructed = classifyPersistedObservations(row, persisted);
  if (
    storedById.observation_count !== persisted.length ||
    storedById.evidence_sha256 !== canonicalJsonSha256(persisted) ||
    storedById.receipt_identity_sha256 !==
      canonicalJsonSha256(persistedReceipts) ||
    canonicalJson(canonicalReceiptEvidence(persisted)) !==
      storedById.receipts_canonical ||
    storedById.classification !== reconstructed
  ) {
    terminal("persisted resolution evidence failed reconstruction");
  }
  const observed = new Set(
    persisted
      .filter((item) => item.classification === "resolved")
      .map((item) => item.observedCid),
  );
  return {
    event: reconstructed,
    metadata: {
      attemptId: request.attemptId,
      evidenceSha256,
      observedCidCount: observed.size,
      receiptIdentitySha256,
      recoverySequence: request.recoverySequence,
      resolutionCycleId,
    },
    replay,
  };
}

export async function recordIpnsIntents(
  databaseUrl: string,
  value: unknown,
): Promise<IpnsIntentView[]> {
  const request = parse(recordIntentsSchema, value, "IPNS intent request");
  if (
    request.targets[0].domain !== "open_data" ||
    request.targets[1].domain !== "query_table"
  ) {
    throw new DurableInputError(
      "IPNS intents must bind open_data then query_table in one request",
    );
  }
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const states = await transaction<
        { plan_id: string; plan_sha256: string; state: string }[]
      >`
        SELECT plan_id, plan_sha256, state FROM oracle_publication_state
        WHERE county = 'pasco' FOR UPDATE
      `;
      const state = states[0];
      if (
        !state ||
        state.plan_id !== request.planId ||
        state.plan_sha256 !== request.planSha256 ||
        !["approved", "executing"].includes(state.state)
      ) {
        terminal("intent requires the exact approved or executing plan");
      }
      await transaction`
        SELECT oracle_assert_publication_approval_binding_fresh(
          ${request.planId}, ${request.planSha256}
        )
      `;
      const plans = await transaction<{ plan_payload: unknown }[]>`
        SELECT plan_payload FROM oracle_publication_plans
        WHERE plan_id = ${request.planId} AND plan_sha256 = ${request.planSha256}
        FOR UPDATE
      `;
      const plan = validatePublicationPlan(plans[0]?.plan_payload);
      if (plan.version !== "1.1.0")
        terminal("legacy plan has no executable graph");

      for (const target of request.targets) {
        const lockedTarget =
          plan.targets[
            target.domain === "open_data" ? "openData" : "queryTable"
          ];
        const approvedTargetCid =
          target.domain === "open_data"
            ? plan.graph.openDataRoot.expectedCid
            : plan.graph.queryTableRoot.expectedCid;
        if (
          !lockedTarget.bucketConfirmed ||
          lockedTarget.bucket === null ||
          lockedTarget.ipnsNetworkKey === null
        ) {
          terminal(`${target.domain} target configuration is incomplete`);
        }
        const resolutionEvidenceSha256 = verifyEvidence(
          target.domain,
          target.priorCid,
          target.resolutionEvidence,
        );
        const identity = {
          approvedTargetCid,
          domain: target.domain,
          intendedAt: target.intendedAt,
          ipnsLabel: lockedTarget.ipnsLabel,
          ipnsNetworkKey: lockedTarget.ipnsNetworkKey,
          priorCid: target.priorCid,
          providerBucket: lockedTarget.bucket,
          providerTargetIdentity: `filebase:${lockedTarget.bucket}`,
          publicationPlanId: request.planId,
          publicationPlanSha256: request.planSha256,
          resolutionEvidence: target.resolutionEvidence,
          resolutionEvidenceSha256,
        };
        const intentSha256 = canonicalJsonSha256(identity);
        const intentId = deterministicId("ipnsintent", [
          "1.1.0",
          "Publish/pasco/ipns-intent",
          request.planId,
          target.domain,
          intentSha256,
        ]);
        await transaction`
          INSERT INTO oracle_publication_ipns_intents (
            intent_id, intent_sha256, publication_plan_id,
            publication_plan_sha256, domain, provider_target_identity,
            provider_bucket, ipns_label, ipns_network_key, prior_cid,
            approved_target_cid, resolution_evidence,
            resolution_evidence_sha256, intended_at
          ) VALUES (
            ${intentId}, ${intentSha256}, ${request.planId},
            ${request.planSha256}, ${target.domain},
            ${identity.providerTargetIdentity}, ${identity.providerBucket},
            ${identity.ipnsLabel}, ${identity.ipnsNetworkKey},
            ${target.priorCid}, ${approvedTargetCid},
            ${transaction.json(target.resolutionEvidence as postgres.JSONValue)},
            ${resolutionEvidenceSha256}, ${target.intendedAt}
          ) ON CONFLICT (publication_plan_id, domain) DO NOTHING
        `;
        const stored = await transaction<
          { intent_id: string; intent_sha256: string }[]
        >`
          SELECT intent_id, intent_sha256
          FROM oracle_publication_ipns_intents
          WHERE publication_plan_id = ${request.planId}
            AND domain = ${target.domain}
        `;
        if (
          stored[0]?.intent_id !== intentId ||
          stored[0]?.intent_sha256 !== intentSha256
        ) {
          terminal(`${target.domain} intent payload changed`);
        }
        await transaction`
          INSERT INTO oracle_publication_ipns_intent_state (
            intent_id, state, revision
          ) VALUES (${intentId}, 'intent_recorded', 1)
          ON CONFLICT (intent_id) DO NOTHING
        `;
        const eventSha256 = canonicalJsonSha256({
          domain: target.domain,
          intentId,
          state: "intent_recorded",
        });
        await transaction`
          INSERT INTO oracle_publication_ipns_intent_events (
            event_id, intent_id, revision, from_state, to_state,
            event_sha256, metadata
          ) VALUES (
            ${deterministicId("ipnsevent", ["1.1.0", intentId, "1", eventSha256])},
            ${intentId}, 1, null, 'intent_recorded', ${eventSha256},
            ${transaction.json({ domain: target.domain })}
          ) ON CONFLICT (intent_id, revision) DO NOTHING
        `;
        const intentRow = await loadIntentForUpdate(transaction, intentId);
        await persistResolutionCycle(transaction, intentRow, {
          attemptId: null,
          observations: target.resolutionEvidence.observations.map(
            (observation) => ({
              ...observation,
              classification: "resolved" as const,
            }),
          ),
          recoverySequence: 0,
        });
        await authorizeIntentMutation(transaction, intentId);
        await transaction`
          UPDATE oracle_publication_ipns_effects SET
            intent_id = ${intentId}, ipns_label = ${identity.ipnsLabel},
            ipns_network_key = ${identity.ipnsNetworkKey},
            prior_cid = ${target.priorCid}, updated_at = now()
          WHERE plan_id = ${request.planId} AND domain = ${target.domain}
            AND status = 'pending'
            AND (intent_id IS NULL OR intent_id = ${intentId})
        `;
      }
      const stored = await rows(transaction, request.planId);
      if (stored.length !== 2)
        terminal("both domain intents were not persisted");
      return stored.map(view);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function transition(
  transaction: postgres.TransactionSql,
  row: IntentRow,
  eventName: IpnsTransitionEvent,
  metadata: Record<string, unknown>,
): Promise<IntentRow> {
  const toState = nextIpnsIntentState(row.state, eventName);
  if (row.state === toState) return row;
  const revision = row.revision + 1;
  const event = {
    event: eventName,
    fromState: row.state,
    intentId: row.intent_id,
    metadata,
    revision,
    toState,
  };
  const eventSha256 = canonicalJsonSha256(event);
  await authorizeIntentMutation(transaction, row.intent_id);
  const updated = await transaction`
    UPDATE oracle_publication_ipns_intent_state SET
      state = ${toState}, revision = ${revision}, updated_at = now()
    WHERE intent_id = ${row.intent_id} AND revision = ${row.revision}
    RETURNING intent_id
  `;
  if (updated.length !== 1) terminal("intent revision changed concurrently");
  await transaction`
    INSERT INTO oracle_publication_ipns_intent_events (
      event_id, intent_id, revision, from_state, to_state,
      event_sha256, metadata
    ) VALUES (
      ${deterministicId("ipnsevent", ["1.1.0", row.intent_id, String(revision), eventSha256])},
      ${row.intent_id}, ${revision}, ${row.state}, ${toState},
      ${eventSha256}, ${transaction.json({ event: eventName, ...metadata } as postgres.JSONValue)}
    )
  `;
  return { ...row, revision, state: toState };
}

async function loadIntentForUpdate(
  transaction: postgres.TransactionSql,
  intentId: string,
): Promise<IntentRow> {
  const found = await transaction<IntentRow[]>`
    SELECT intent.intent_id, intent.intent_sha256, intent.domain,
           intent.prior_cid, intent.approved_target_cid,
           intent.publication_plan_id, intent.publication_plan_sha256,
           state.state, state.revision
    FROM oracle_publication_ipns_intents intent
    JOIN oracle_publication_ipns_intent_state state USING (intent_id)
    WHERE intent.intent_id = ${intentId}
    FOR UPDATE OF state
  `;
  if (!found[0]) terminal("unknown intent");
  await transaction`
    SELECT oracle_assert_publication_approval_binding_fresh(
      ${found[0]!.publication_plan_id},
      ${found[0]!.publication_plan_sha256}
    )
  `;
  return found[0];
}

async function requireAllObjectsVerified(
  transaction: postgres.TransactionSql,
  row: IntentRow,
): Promise<void> {
  const counts = await transaction<{ pending: number; total: number }[]>`
    SELECT count(*)::int AS total,
           count(*) FILTER (WHERE status != 'verified' OR verified_cid != expected_cid)::int AS pending
    FROM oracle_publication_object_effects
    WHERE plan_id = ${row.publication_plan_id} AND domain = ${row.domain}
  `;
  if (!counts[0] || counts[0].total === 0 || counts[0].pending !== 0) {
    throw new DurableInputError(
      `IPNS ${row.domain} mutation requires every uploaded object CID to be verified`,
    );
  }
}

export async function confirmIpnsPrior(
  databaseUrl: string,
  intentId: string,
): Promise<IpnsIntentView> {
  parse(intentIdSchema, intentId, "IPNS intent ID");
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const row = await loadIntentForUpdate(transaction, intentId);
      return view(
        await transition(transaction, row, "prior_observed", {
          resolvedCidSha256: canonicalJsonSha256(row.prior_cid),
        }),
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordIpnsMutationAttempt(
  databaseUrl: string,
  value: unknown,
): Promise<{ attemptId: string; intent: IpnsIntentView }> {
  const request = parse(
    z.strictObject({
      direction: z.enum(["update", "rollback"]),
      intentId: intentIdSchema,
      requestSha256: sha256Schema,
    }),
    value,
    "IPNS mutation attempt",
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      let row = await loadIntentForUpdate(transaction, request.intentId);
      await requireAllObjectsVerified(transaction, row);
      const expectedState =
        request.direction === "update"
          ? "prior_confirmed"
          : "rollback_requested";
      if (row.state !== expectedState) terminal(`attempt from ${row.state}`);
      const all = await rows(transaction, row.publication_plan_id);
      if (all.length !== 2) terminal("both intents must exist before mutation");
      if (request.direction === "update" && row.domain === "query_table") {
        const openData = all.find((item) => item.domain === "open_data");
        if (openData?.state !== "verified") {
          terminal("query_table update requires verified open_data intent");
        }
      }
      if (request.direction === "rollback" && row.domain === "open_data") {
        const queryTable = all.find((item) => item.domain === "query_table");
        if (
          queryTable &&
          ![
            "intent_recorded",
            "manual_intervention_required",
            "prior_confirmed",
            "rollback_verified",
            "failed_terminal",
          ].includes(queryTable.state)
        ) {
          terminal("open_data rollback must follow query_table rollback");
        }
      }
      const targetCid =
        request.direction === "update"
          ? row.approved_target_cid
          : row.prior_cid;
      const revision = row.revision + 1;
      const attemptId = deterministicId("ipnsattempt", [
        "1.1.0",
        row.intent_id,
        String(revision),
        request.direction,
        targetCid,
        request.requestSha256,
      ]);
      await transaction`
        INSERT INTO oracle_publication_ipns_mutation_attempts (
          attempt_id, intent_id, revision, direction, target_cid,
          request_sha256, outcome
        ) VALUES (
          ${attemptId}, ${row.intent_id}, ${revision}, ${request.direction},
          ${targetCid}, ${request.requestSha256}, 'recorded'
        )
      `;
      row = await transition(
        transaction,
        row,
        request.direction === "update" ? "update_started" : "rollback_started",
        { attemptId, direction: request.direction, targetCid },
      );
      return { attemptId, intent: view(row) };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordIpnsMutationReceipt(
  databaseUrl: string,
  value: unknown,
): Promise<IpnsIntentView> {
  const request = parse(
    z.strictObject({
      attemptId: attemptIdSchema,
      intentId: intentIdSchema,
      outcome: z.enum([
        "acknowledged",
        "failed",
        "timeout",
        "transport_ambiguous",
      ]),
      providerReceiptSha256: sha256Schema.nullable(),
    }),
    value,
    "IPNS mutation receipt",
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      let row = await loadIntentForUpdate(transaction, request.intentId);
      const attempts = await transaction<
        {
          attempt_id: string;
          direction: "rollback" | "update";
          target_cid: string;
        }[]
      >`
        SELECT attempt_id, direction, target_cid
        FROM oracle_publication_ipns_mutation_attempts
        WHERE attempt_id = ${request.attemptId} AND intent_id = ${request.intentId}
      `;
      const attempt = attempts[0];
      if (!attempt) terminal("receipt does not match an immutable attempt");
      const receiptIdentity = {
        attemptId: request.attemptId,
        intentId: request.intentId,
        outcome: request.outcome,
        providerReceiptSha256: request.providerReceiptSha256,
      };
      const receiptSha256 = canonicalJsonSha256(receiptIdentity);
      const receiptId = deterministicId("ipnsreceipt", [
        "1.1.0",
        request.attemptId,
        receiptSha256,
      ]);
      await transaction`
        INSERT INTO oracle_publication_ipns_mutation_receipts (
          receipt_id, attempt_id, intent_id, outcome,
          provider_receipt_sha256, receipt_sha256
        ) VALUES (
          ${receiptId}, ${request.attemptId}, ${request.intentId},
          ${request.outcome}, ${request.providerReceiptSha256}, ${receiptSha256}
        ) ON CONFLICT (attempt_id) DO NOTHING
      `;
      const stored = await transaction<{ receipt_sha256: string }[]>`
        SELECT receipt_sha256 FROM oracle_publication_ipns_mutation_receipts
        WHERE attempt_id = ${request.attemptId}
      `;
      if (stored[0]?.receipt_sha256 !== receiptSha256) {
        terminal("mutation receipt payload changed");
      }
      const isRollback = attempt.direction === "rollback";
      const event: IpnsTransitionEvent =
        request.outcome === "acknowledged"
          ? "mutation_acknowledged"
          : request.outcome === "failed"
            ? "mutation_failed"
            : "timeout_transport_uncertainty";
      row = await transition(transaction, row, event, {
        attemptId: request.attemptId,
        outcome: request.outcome,
        receiptSha256,
      });
      if (request.outcome === "acknowledged" && !isRollback) {
        await authorizeIntentMutation(transaction, row.intent_id);
        await transaction`
          UPDATE oracle_publication_ipns_effects SET
            status = 'updated', target_cid = ${row.approved_target_cid},
            mutation_performed = true, public_resolution_verified = false,
            updated_at = now()
          WHERE plan_id = ${row.publication_plan_id} AND domain = ${row.domain}
            AND intent_id = ${row.intent_id} AND status = 'pending'
        `;
      }
      if (request.outcome === "failed") {
        await markPublicationTerminal(
          transaction,
          row,
          isRollback || row.domain === "query_table"
            ? "manual_intervention_required"
            : "failed_terminal",
          isRollback
            ? "ipns_rollback_failed"
            : row.domain === "query_table"
              ? "query_table_update_failed_open_data_rollback_required"
              : "open_data_update_failed",
        );
        if (!isRollback && row.domain === "query_table") {
          const all = await rows(transaction, row.publication_plan_id);
          const openData = all.find((intent) => intent.domain === "open_data");
          if (openData?.state === "verified") {
            const lockedOpenData = await loadIntentForUpdate(
              transaction,
              openData.intent_id,
            );
            await transition(
              transaction,
              lockedOpenData,
              "rollback_requested",
              {
                causeIntentId: row.intent_id,
                reason: "query_table_update_failed",
              },
            );
          }
        }
      }
      return view(row);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordIpnsAmbiguousResult(
  databaseUrl: string,
  value: unknown,
): Promise<IpnsIntentView> {
  const request = parse(
    z.strictObject({
      attemptId: attemptIdSchema.optional(),
      intentId: intentIdSchema,
      reason: z.enum(["timeout", "transport_ambiguous"]),
    }),
    value,
    "IPNS ambiguous result",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const attempts = await sql<{ attempt_id: string }[]>`
      SELECT attempt_id FROM oracle_publication_ipns_mutation_attempts
      WHERE intent_id = ${request.intentId}
      ORDER BY recorded_at DESC LIMIT 1
    `;
    const attemptId = request.attemptId ?? attempts[0]?.attempt_id;
    if (!attemptId) terminal("ambiguous result has no mutation attempt");
    return await recordIpnsMutationReceipt(databaseUrl, {
      attemptId,
      intentId: request.intentId,
      outcome: request.reason,
      providerReceiptSha256: null,
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function recordResolutionCycle(
  transaction: postgres.TransactionSql,
  row: IntentRow,
  request: {
    attemptId: string | null;
    observations: readonly z.infer<typeof recoveryObservationSchema>[];
    recoverySequence: number;
  },
): Promise<ResolutionCycleResult> {
  const kinds = new Set(request.observations.map((item) => item.resolverKind));
  if (!kinds.has("filebase_control_plane") || !kinds.has("public_gateway")) {
    throw new DurableInputError(
      "Recovery needs provider and public observations",
    );
  }
  if (request.attemptId !== null) {
    const attempt = await transaction<{ intent_id: string }[]>`
      SELECT intent_id FROM oracle_publication_ipns_mutation_attempts
      WHERE attempt_id = ${request.attemptId}
    `;
    if (attempt[0]?.intent_id !== row.intent_id) {
      terminal("resolution cycle does not match its mutation attempt");
    }
  }
  return persistResolutionCycle(transaction, row, request);
}

export async function recoverIpnsIntent(
  databaseUrl: string,
  value: unknown,
): Promise<IpnsIntentView> {
  const request = parse(
    z.strictObject({
      attemptId: attemptIdSchema.nullable(),
      intentId: intentIdSchema,
      observations: z.array(recoveryObservationSchema).min(2),
      recoverySequence: recoverySequenceSchema,
    }),
    value,
    "IPNS recovery observation",
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      let row = await loadIntentForUpdate(transaction, request.intentId);
      const classified = await recordResolutionCycle(transaction, row, request);
      if (classified.replay) return view(row);
      row = await transition(
        transaction,
        row,
        classified.event,
        classified.metadata,
      );
      if (row.state === "manual_intervention_required") {
        await markPublicationTerminal(
          transaction,
          row,
          "manual_intervention_required",
          "unexpected_third_cid",
        );
        if (row.domain === "query_table") {
          const all = await rows(transaction, row.publication_plan_id);
          const openData = all.find((intent) => intent.domain === "open_data");
          if (
            openData &&
            ["target_observed", "verification_pending", "verified"].includes(
              openData.state,
            )
          ) {
            const lockedOpenData = await loadIntentForUpdate(
              transaction,
              openData.intent_id,
            );
            await transition(
              transaction,
              lockedOpenData,
              "rollback_requested",
              {
                causeIntentId: row.intent_id,
                reason: "query_table_unexpected_third_cid",
              },
            );
          }
        }
      }
      if (row.state === "rollback_verified") {
        await authorizeIntentMutation(transaction, row.intent_id);
        await transaction`
          UPDATE oracle_publication_ipns_effects SET
            status = 'pending', target_cid = null, mutation_performed = false,
            public_resolution_verified = false, updated_at = now()
          WHERE plan_id = ${row.publication_plan_id} AND domain = ${row.domain}
            AND intent_id = ${row.intent_id}
        `;
      }
      return view(row);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function verifyIpnsTarget(
  databaseUrl: string,
  intentId: string,
): Promise<IpnsIntentView> {
  parse(intentIdSchema, intentId, "IPNS intent ID");
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      let row = await loadIntentForUpdate(transaction, intentId);
      if (row.state === "target_observed") {
        row = await transition(transaction, row, "verification_pending", {
          targetCidSha256: canonicalJsonSha256(row.approved_target_cid),
        });
      }
      row = await transition(transaction, row, "verified", {
        targetCidSha256: canonicalJsonSha256(row.approved_target_cid),
      });
      await authorizeIntentMutation(transaction, row.intent_id);
      await transaction`
        UPDATE oracle_publication_ipns_effects SET
          status = 'verified', target_cid = ${row.approved_target_cid},
          mutation_performed = true, public_resolution_verified = true,
          updated_at = now()
        WHERE plan_id = ${row.publication_plan_id} AND domain = ${row.domain}
          AND intent_id = ${row.intent_id}
          AND status IN ('pending', 'updated', 'verified')
      `;
      return view(row);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordIpnsRollback(
  databaseUrl: string,
  intentId: string,
): Promise<IpnsIntentView> {
  parse(intentIdSchema, intentId, "IPNS intent ID");
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const row = await loadIntentForUpdate(transaction, intentId);
      return view(
        await transition(transaction, row, "rollback_requested", {
          rollbackTargetCidSha256: canonicalJsonSha256(row.prior_cid),
        }),
      );
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function markPublicationTerminal(
  transaction: postgres.TransactionSql,
  row: IntentRow,
  state: "failed_terminal" | "manual_intervention_required",
  reason: string,
): Promise<void> {
  const current = await transaction<{ revision: number; state: string }[]>`
    SELECT revision, state FROM oracle_publication_state
    WHERE county = 'pasco' AND plan_id = ${row.publication_plan_id}
    FOR UPDATE
  `;
  if (!current[0] || current[0].state === state) return;
  const metadata = { intentId: row.intent_id, reason };
  const transitionSha256 = canonicalJsonSha256({
    fromState: current[0].state,
    metadata,
    planId: row.publication_plan_id,
    planSha256: row.publication_plan_sha256,
    toState: state,
  });
  await transaction`
    INSERT INTO oracle_publication_state_events (
      event_id, county, plan_id, plan_sha256, from_state, to_state,
      transition_sha256, metadata
    ) VALUES (
      ${deterministicId("pubstate", ["1.1.0", row.publication_plan_id, state, transitionSha256])},
      'pasco', ${row.publication_plan_id}, ${row.publication_plan_sha256},
      ${current[0].state}, ${state}, ${transitionSha256},
      ${transaction.json(metadata)}
    ) ON CONFLICT (plan_id, to_state, transition_sha256) DO NOTHING
  `;
  await transaction`
    UPDATE oracle_publication_state SET state = ${state},
      terminal_reason = ${reason}, revision = revision + 1, updated_at = now()
    WHERE county = 'pasco' AND plan_id = ${row.publication_plan_id}
  `;
}
