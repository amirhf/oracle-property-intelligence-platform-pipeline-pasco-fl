import postgres from "postgres";
import { z } from "zod";

import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import {
  assertCandidateSourceSnapshotObjectNamespace,
  candidateSourceSnapshotExactUploadBindingSchema,
  candidateSourceSnapshotObjectSchema,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotExactUploadBinding,
  type CandidateSourceSnapshotRequestCategory,
  type CandidateSourceSnapshotUploadObject,
} from "../publication/candidate-source-snapshot-demo.js";
import type {
  CandidateSourceSnapshotPlanAccounting,
  CandidateSourceSnapshotInspectionResult,
  CandidateSourceSnapshotUploadAttempt,
  CandidateSourceSnapshotUploadCheckpoint,
  CandidateSourceSnapshotUploadJournal,
  CandidateSourceSnapshotUploadReceipt,
  CandidateSourceSnapshotTransportFailureEvidence,
} from "../publication/candidate-source-snapshot-upload.js";
import {
  renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement,
  type CandidateSourceSnapshotIpnsReplayAuthorization,
  type CandidateSourceSnapshotIpnsRollbackAuthorization,
} from "../publication/candidate-source-snapshot-ipns-controller.js";
import { createCandidateSourceSnapshotApprovalIdentity } from "./candidate-source-snapshot-approval.js";
import { runCandidateSourceSnapshotFencedPostgresOperation } from "./candidate-source-snapshot-postgres-reconnect.js";

const OBJECT_BATCH_SIZE = 500;

export interface CandidateSourceSnapshotUploadJournalLeaseBinding {
  authorizationId: string;
  leaseGeneration: number;
  leaseId: string;
  resumeAuthorizationId?: string;
}

const uploadJournalLeaseBindingSchema = z.strictObject({
  authorizationId: z
    .string()
    .regex(/^snapshotdemouploadcontinuation_[a-f0-9]{32}$/),
  leaseGeneration: z.number().int().positive(),
  leaseId: z.string().regex(/^snapshotdemoexecutorlease_[a-f0-9]{32}$/),
  resumeAuthorizationId: z
    .string()
    .regex(/^snapshotdemouploadresume_[a-f0-9]{32}$/)
    .optional(),
});

const transportFailureEvidenceSchema = z
  .strictObject({
    evidenceSha256: z.string().regex(/^[a-f0-9]{64}$/),
    failureClass: z.enum(["outcome_unknown", "retryable", "terminal"]),
    providerRequestIdHash: z
      .string()
      .regex(/^[a-f0-9]{64}$/)
      .nullable(),
    schemaVersion: z.literal("candidate-source-snapshot-transport-failure-v1"),
    stage: z.enum([
      "head_object_request",
      "put_object_connection",
      "put_object_provider_response",
      "put_object_streaming_request",
      "transport_deadline",
      "unknown",
    ]),
  })
  .superRefine((value, context) => {
    const { evidenceSha256: _evidenceSha256, ...identity } = value;
    if (canonicalJsonSha256(identity) !== value.evidenceSha256) {
      context.addIssue({
        code: "custom",
        message: "transport failure evidence hash is inconsistent",
      });
    }
  });

export interface CandidateSourceSnapshotDurableState {
  approvalCount: number;
  effectCount: number;
  intentCount: number;
  planId: string;
  planSha256: string;
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

interface PlanRow {
  exact_upload_bytes: string | number;
  exact_upload_object_count: number;
  plan_artifact_bytes: string | number;
  plan_artifact_cid: string;
  plan_artifact_remote_object_key: string;
  plan_artifact_sha256: string;
  plan_payload: unknown;
  plan_sha256: string;
  revision: number;
  state: CandidateSourceSnapshotDurableState["state"];
}

type ObjectIterable =
  | Iterable<CandidateSourceSnapshotUploadObject>
  | AsyncIterable<CandidateSourceSnapshotUploadObject>;

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  await transaction`SELECT pg_advisory_xact_lock(
    hashtext('oracle-candidate-source-snapshot-demo-v2'), hashtext('pasco')
  )`;
}

async function counts(
  transaction: postgres.TransactionSql,
  planId: string,
): Promise<{
  approvalCount: number;
  effectCount: number;
  intentCount: number;
}> {
  const rows = await transaction<
    {
      approval_count: string;
      effect_count: string;
      intent_count: string;
    }[]
  >`
    SELECT
      (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE plan_id = ${planId})::text AS approval_count,
      (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${planId} AND status <> 'pending')::text AS effect_count,
      (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_ipns_intents
        WHERE plan_id = ${planId})::text AS intent_count
  `;
  const row = rows[0];
  return {
    approvalCount: Number(row?.approval_count ?? 0),
    effectCount: Number(row?.effect_count ?? 0),
    intentCount: Number(row?.intent_count ?? 0),
  };
}

function durableState(
  plan: CandidateSourceSnapshotDemoPlan,
  row: Pick<PlanRow, "revision" | "state">,
  durableCounts: Awaited<ReturnType<typeof counts>>,
): CandidateSourceSnapshotDurableState {
  return {
    ...durableCounts,
    planId: plan.planId,
    planSha256: plan.planSha256,
    revision: row.revision,
    state: row.state,
  };
}

function validateStoredPlan(
  row: PlanRow,
  plan: CandidateSourceSnapshotDemoPlan,
  exact: CandidateSourceSnapshotExactUploadBinding,
): void {
  if (
    row.plan_sha256 !== plan.planSha256 ||
    Number(row.exact_upload_object_count) !== exact.exactObjectCount ||
    Number(row.exact_upload_bytes) !== exact.exactTotalBytes ||
    Number(row.plan_artifact_bytes) !== exact.planArtifact.byteSize ||
    row.plan_artifact_cid !== exact.planArtifact.expectedCid ||
    row.plan_artifact_sha256 !== exact.planArtifact.sha256 ||
    row.plan_artifact_remote_object_key !== exact.planArtifact.remoteObjectKey
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot plan replay conflicts with durable identity",
    );
  }
  const stored = validateCandidateSourceSnapshotDemoPlan(row.plan_payload);
  if (stored.planId !== plan.planId || stored.planSha256 !== plan.planSha256) {
    throw new DurableConflictError(
      "Stored candidate source-snapshot plan identity is invalid",
    );
  }
}

async function insertObjectBatch(
  transaction: postgres.TransactionSql,
  rows: {
    domain: string;
    expected_bytes: number;
    expected_cid: string;
    expected_sha256: string;
    logical_object_key: string;
    plan_id: string;
    remote_object_key: string;
    status: "pending";
  }[],
): Promise<void> {
  if (rows.length === 0) return;
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_objects ${transaction(
      rows,
      "plan_id",
      "domain",
      "logical_object_key",
      "remote_object_key",
      "expected_sha256",
      "expected_cid",
      "expected_bytes",
      "status",
    )}
  `;
}

type ReplayInventoryRow = {
  domain: string;
  expected_bytes: number;
  expected_cid: string;
  expected_sha256: string;
  logical_object_key: string;
  remote_object_key: string;
};

/**
 * A durable replay must bind every immutable object identity, not merely an
 * equivalent count and byte total.  Keep the comparison in PostgreSQL so a
 * full 325k-object replay stays bounded by the batch size rather than a
 * process-local object set.
 */
async function assertExactInventoryReplay(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  exact: CandidateSourceSnapshotExactUploadBinding,
  objects: ObjectIterable,
): Promise<void> {
  await transaction.unsafe(`
    CREATE TEMP TABLE candidate_source_snapshot_replay_inventory (
      domain text NOT NULL,
      logical_object_key text NOT NULL,
      remote_object_key text NOT NULL,
      expected_sha256 text NOT NULL,
      expected_cid text NOT NULL,
      expected_bytes bigint NOT NULL,
      PRIMARY KEY (domain, remote_object_key),
      UNIQUE (domain, logical_object_key)
    ) ON COMMIT DROP
  `);

  let count = 0;
  let bytes = 0;
  let planArtifactCount = 0;
  let batch: ReplayInventoryRow[] = [];
  const flush = async (): Promise<void> => {
    if (batch.length === 0) return;
    await transaction`
      INSERT INTO candidate_source_snapshot_replay_inventory ${transaction(
        batch,
        "domain",
        "logical_object_key",
        "remote_object_key",
        "expected_sha256",
        "expected_cid",
        "expected_bytes",
      )}
    `;
    batch = [];
  };

  for await (const value of objects) {
    const object = candidateSourceSnapshotObjectSchema().parse(value);
    assertCandidateSourceSnapshotObjectNamespace(plan, object);
    count += 1;
    bytes += object.byteSize;
    if (count > exact.exactObjectCount || bytes > exact.exactTotalBytes) {
      throw new DurableInputError(
        "Candidate source-snapshot replay inventory exceeds its exact binding",
      );
    }
    if (
      object.remoteObjectKey === exact.planArtifact.remoteObjectKey &&
      object.logicalObjectKey === exact.planArtifact.logicalObjectKey &&
      object.expectedCid === exact.planArtifact.expectedCid &&
      object.sha256 === exact.planArtifact.sha256 &&
      object.byteSize === exact.planArtifact.byteSize
    ) {
      planArtifactCount += 1;
    }
    batch.push({
      domain: object.domain,
      expected_bytes: object.byteSize,
      expected_cid: object.expectedCid,
      expected_sha256: object.sha256,
      logical_object_key: object.logicalObjectKey,
      remote_object_key: object.remoteObjectKey,
    });
    if (batch.length === OBJECT_BATCH_SIZE) await flush();
  }
  await flush();
  if (
    count !== exact.exactObjectCount ||
    bytes !== exact.exactTotalBytes ||
    planArtifactCount !== 1
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot replay inventory conflicts with its exact binding",
    );
  }
  const differences = await transaction<{ count: string }[]>`
    WITH difference AS (
      (SELECT domain, logical_object_key, remote_object_key, expected_sha256,
              expected_cid, expected_bytes
       FROM candidate_source_snapshot_replay_inventory
       EXCEPT
       SELECT domain, logical_object_key, remote_object_key, expected_sha256,
              expected_cid, expected_bytes
       FROM oracle_candidate_source_snapshot_demo_objects
       WHERE plan_id = ${plan.planId})
      UNION ALL
      (SELECT domain, logical_object_key, remote_object_key, expected_sha256,
              expected_cid, expected_bytes
       FROM oracle_candidate_source_snapshot_demo_objects
       WHERE plan_id = ${plan.planId}
       EXCEPT
       SELECT domain, logical_object_key, remote_object_key, expected_sha256,
              expected_cid, expected_bytes
       FROM candidate_source_snapshot_replay_inventory)
    )
    SELECT count(*)::text AS count FROM difference
  `;
  if (Number(differences[0]?.count ?? 0) !== 0) {
    throw new DurableConflictError(
      "Candidate source-snapshot replay inventory conflicts with durable objects",
    );
  }
}

async function recordPlanEvent(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  exact: CandidateSourceSnapshotExactUploadBinding,
): Promise<void> {
  const metadata = {
    exactObjectCount: exact.exactObjectCount,
    exactTotalBytes: exact.exactTotalBytes,
    executorEnabled: false,
    planArtifactCid: exact.planArtifact.expectedCid,
    planArtifactSha256: exact.planArtifact.sha256,
    state: "awaiting_configuration",
  } as const;
  const eventSha256 = canonicalJsonSha256({
    eventType: "plan_recorded_local_only",
    metadata,
    planId: plan.planId,
    planSha256: plan.planSha256,
  });
  const eventId = deterministicId("snapshotdemoevent", [
    "1.0.0",
    plan.planId,
    eventSha256,
  ]);
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_events (
      event_id, plan_id, event_type, event_sha256, metadata
    ) VALUES (
      ${eventId}, ${plan.planId}, 'plan_recorded_local_only', ${eventSha256},
      ${transaction.json(metadata)}
    )
  `;
}

/**
 * Records only the immutable local v2 plan, exact external plan artifact, and
 * pending object inventory. It deliberately creates no approval, request,
 * effect, or IPNS intent and therefore cannot arm the remote executor.
 */
export async function recordCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  input: {
    exactUpload: CandidateSourceSnapshotExactUploadBinding;
    objects: ObjectIterable;
    plan: CandidateSourceSnapshotDemoPlan;
  },
): Promise<CandidateSourceSnapshotDurableState> {
  const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
  const exact = candidateSourceSnapshotExactUploadBindingSchema.parse(
    input.exactUpload,
  );
  if (
    exact.planArtifact.remoteObjectKey !==
    `${plan.targets.controlPrefix}${exact.planArtifact.logicalObjectKey}`
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot plan artifact is outside its namespace",
    );
  }
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const existing = await transaction<PlanRow[]>`
        SELECT plan_sha256, exact_upload_object_count, exact_upload_bytes,
               plan_artifact_bytes, plan_artifact_sha256, plan_artifact_cid,
               plan_artifact_remote_object_key, plan_payload, state, revision
        FROM oracle_candidate_source_snapshot_demo_plans
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        validateStoredPlan(existing[0], plan, exact);
        await assertExactInventoryReplay(
          transaction,
          plan,
          exact,
          input.objects,
        );
        return durableState(
          plan,
          existing[0],
          await counts(transaction, plan.planId),
        );
      }

      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_plans (
          plan_id, plan_sha256, plan_version, publication_class,
          resource_owner, canonical, elephant_owned, owner_controlled,
          independently_pasco_certified, namespace_id, source_scope,
          source_plan_id, source_plan_sha256, authority_id, snapshot_id,
          materialization_id, materialization_sha256,
          maximum_upload_object_count, maximum_upload_bytes,
          exact_upload_object_count, exact_upload_bytes, maximum_object_bytes,
          maximum_concurrency, maximum_retries, request_timeout_ms,
          request_limit, maximum_request_count, budget_limit_usd,
          maximum_total_usd, fixed_account_plan_monthly_usd,
          inventory_root_sha256, inventory_root_cid, inventory_shard_count,
          control_artifacts, request_envelope, cost_envelope,
          capacity_preflight, capacity_preflight_sha256,
          capacity_preflight_observed_at, subscription_tier_status,
          plan_payload, plan_artifact_bytes, plan_artifact_sha256,
          plan_artifact_cid, plan_artifact_logical_object_key,
          plan_artifact_remote_object_key, prepared_with_executor_disabled,
          state, revision
        ) VALUES (
          ${plan.planId}, ${plan.planSha256}, ${plan.version},
          ${plan.classification.publicationClass},
          ${plan.classification.resourceOwner}, false, false, false, false,
          ${plan.namespaceId}, ${plan.classification.sourceScope},
          ${plan.source.sourcePlanId}, ${plan.source.sourcePlanSha256},
          ${plan.source.authorityId}, ${plan.source.snapshotId},
          ${plan.source.materializationId}, ${plan.source.materializationSha256},
          ${plan.inventory.objectCount}, ${plan.inventory.totalBytes},
          ${exact.exactObjectCount}, ${exact.exactTotalBytes},
          ${plan.inventory.maxObjectBytes}, ${plan.limits.maxConcurrency},
          ${plan.limits.maxRetries}, ${plan.limits.requestTimeoutMs},
          ${plan.limits.maxRequests},
          ${plan.requestEnvelope.maximumTotalRequests},
          ${plan.limits.maxBudgetUsd}, ${plan.costEnvelope.maximumTotalUsd},
          ${plan.costEnvelope.fixedAccountPlanMonthlyUsd},
          ${plan.inventory.inventoryRootSha256},
          ${plan.inventory.inventoryRootCid},
          ${plan.inventory.inventoryShardCount},
          ${transaction.json(plan.controlArtifacts)},
          ${transaction.json(plan.requestEnvelope)},
          ${transaction.json(plan.costEnvelope)},
          ${transaction.json(plan.preflight)}, ${plan.preflight.evidenceSha256},
          ${plan.preflight.observedAt},
          ${plan.preflight.capacityProfile.subscriptionTierStatus},
          ${transaction.json(plan as unknown as postgres.JSONValue)},
          ${exact.planArtifact.byteSize}, ${exact.planArtifact.sha256},
          ${exact.planArtifact.expectedCid},
          ${exact.planArtifact.logicalObjectKey},
          ${exact.planArtifact.remoteObjectKey}, true,
          'awaiting_configuration', 1
        )
      `;
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_accounting (plan_id)
        VALUES (${plan.planId})
      `;
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_request_categories ${transaction(
          Object.entries(plan.requestEnvelope.categoryRequests).map(
            ([requestCategory, [successfulRequests, maximumRequests]]) => ({
              plan_id: plan.planId,
              planned_maximum_request_count: maximumRequests,
              planned_successful_request_count: successfulRequests,
              request_category: requestCategory,
            }),
          ),
          "plan_id",
          "request_category",
          "planned_successful_request_count",
          "planned_maximum_request_count",
        )}
      `;

      let objectCount = 0;
      let totalBytes = 0;
      let maximumObjectBytes = 0;
      let planArtifactCount = 0;
      let batch: Parameters<typeof insertObjectBatch>[1] = [];
      for await (const value of input.objects) {
        const object = candidateSourceSnapshotObjectSchema().parse(value);
        assertCandidateSourceSnapshotObjectNamespace(plan, object);
        objectCount += 1;
        totalBytes += object.byteSize;
        maximumObjectBytes = Math.max(maximumObjectBytes, object.byteSize);
        if (
          object.remoteObjectKey === exact.planArtifact.remoteObjectKey &&
          object.logicalObjectKey === exact.planArtifact.logicalObjectKey &&
          object.expectedCid === exact.planArtifact.expectedCid &&
          object.sha256 === exact.planArtifact.sha256 &&
          object.byteSize === exact.planArtifact.byteSize
        ) {
          planArtifactCount += 1;
        }
        if (
          objectCount > exact.exactObjectCount ||
          totalBytes > exact.exactTotalBytes ||
          maximumObjectBytes > plan.limits.maxObjectBytes
        ) {
          throw new DurableInputError(
            "Candidate source-snapshot object inventory exceeds its exact binding",
          );
        }
        batch.push({
          domain: object.domain,
          expected_bytes: object.byteSize,
          expected_cid: object.expectedCid,
          expected_sha256: object.sha256,
          logical_object_key: object.logicalObjectKey,
          plan_id: plan.planId,
          remote_object_key: object.remoteObjectKey,
          status: "pending",
        });
        if (batch.length === OBJECT_BATCH_SIZE) {
          await insertObjectBatch(transaction, batch);
          batch = [];
        }
      }
      await insertObjectBatch(transaction, batch);
      if (
        objectCount !== exact.exactObjectCount ||
        totalBytes !== exact.exactTotalBytes ||
        planArtifactCount !== 1
      ) {
        throw new DurableInputError(
          "Candidate source-snapshot object inventory does not match its exact binding",
        );
      }
      await recordPlanEvent(transaction, plan, exact);
      const noEffects = await counts(transaction, plan.planId);
      if (
        noEffects.approvalCount !== 0 ||
        noEffects.effectCount !== 0 ||
        noEffects.intentCount !== 0
      ) {
        throw new DurableConflictError(
          "Local source-snapshot plan recording created a remote authority record",
        );
      }
      return durableState(
        plan,
        { revision: 1, state: "awaiting_configuration" },
        noEffects,
      );
    });
  } catch (error) {
    if (
      error instanceof DurableInputError ||
      error instanceof DurableConflictError
    )
      throw error;
    if ((error as { code?: string }).code === "23505") {
      throw new DurableConflictError(
        "Candidate source-snapshot object inventory contains a duplicate identity",
      );
    }
    throw error;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  identity: { planId: string; planSha256: string },
): Promise<{
  exactUpload: CandidateSourceSnapshotExactUploadBinding;
  plan: CandidateSourceSnapshotDemoPlan;
  state: CandidateSourceSnapshotDurableState;
}> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<PlanRow[]>`
        SELECT plan_sha256, exact_upload_object_count, exact_upload_bytes,
               plan_artifact_bytes, plan_artifact_sha256, plan_artifact_cid,
               plan_artifact_remote_object_key, plan_payload, state, revision
        FROM oracle_candidate_source_snapshot_demo_plans
        WHERE plan_id = ${identity.planId}
        FOR UPDATE
      `;
      const row = rows[0];
      if (!row || row.plan_sha256 !== identity.planSha256) {
        throw new DurableConflictError(
          "Candidate source-snapshot plan identity conflict",
        );
      }
      const plan = validateCandidateSourceSnapshotDemoPlan(row.plan_payload);
      const exactUpload = candidateSourceSnapshotExactUploadBindingSchema.parse(
        {
          exactObjectCount: Number(row.exact_upload_object_count),
          exactTotalBytes: Number(row.exact_upload_bytes),
          planArtifact: {
            byteSize: Number(row.plan_artifact_bytes),
            expectedCid: row.plan_artifact_cid,
            logicalObjectKey: "candidate-source-snapshot-plan.json",
            remoteObjectKey: row.plan_artifact_remote_object_key,
            sha256: row.plan_artifact_sha256,
          },
        },
      );
      validateStoredPlan(row, plan, exactUpload);
      return {
        exactUpload,
        plan,
        state: durableState(plan, row, await counts(transaction, plan.planId)),
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const operatorReferenceSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/);
const operatorTimestampSchema = z.string().datetime({ offset: true });

function planExactUploadFromRow(
  row: PlanRow,
): CandidateSourceSnapshotExactUploadBinding {
  return candidateSourceSnapshotExactUploadBindingSchema.parse({
    exactObjectCount: Number(row.exact_upload_object_count),
    exactTotalBytes: Number(row.exact_upload_bytes),
    planArtifact: {
      byteSize: Number(row.plan_artifact_bytes),
      expectedCid: row.plan_artifact_cid,
      logicalObjectKey: "candidate-source-snapshot-plan.json",
      remoteObjectKey: row.plan_artifact_remote_object_key,
      sha256: row.plan_artifact_sha256,
    },
  });
}

async function loadExactPlanRowForUpdate(
  transaction: postgres.TransactionSql,
  identity: { planId: string; planSha256: string },
): Promise<{ plan: CandidateSourceSnapshotDemoPlan; row: PlanRow }> {
  const rows = await transaction<PlanRow[]>`
    SELECT plan_sha256, exact_upload_object_count, exact_upload_bytes,
           plan_artifact_bytes, plan_artifact_sha256, plan_artifact_cid,
           plan_artifact_remote_object_key, plan_payload, state, revision
    FROM oracle_candidate_source_snapshot_demo_plans
    WHERE plan_id = ${identity.planId}
    FOR UPDATE
  `;
  const row = rows[0];
  if (!row || row.plan_sha256 !== identity.planSha256) {
    throw new DurableConflictError(
      "Candidate source-snapshot plan identity conflict",
    );
  }
  const plan = validateCandidateSourceSnapshotDemoPlan(row.plan_payload);
  validateStoredPlan(row, plan, planExactUploadFromRow(row));
  return { plan, row };
}

async function recordStateEvent(
  transaction: postgres.TransactionSql,
  input: {
    eventType: string;
    metadata: Record<string, unknown>;
    plan: CandidateSourceSnapshotDemoPlan;
  },
): Promise<void> {
  const eventSha256 = canonicalJsonSha256({
    eventType: input.eventType,
    metadata: input.metadata,
    planId: input.plan.planId,
    planSha256: input.plan.planSha256,
  });
  const eventId = deterministicId("snapshotdemoevent", [
    "candidate-source-snapshot-state-event-v1",
    input.plan.planId,
    eventSha256,
  ]);
  const existing = await transaction<
    { event_sha256: string; event_type: string; metadata: unknown }[]
  >`
    SELECT event_type, event_sha256, metadata
    FROM oracle_candidate_source_snapshot_demo_events
    WHERE event_id = ${eventId}
  `;
  if (existing[0]) {
    if (
      existing[0].event_type !== input.eventType ||
      existing[0].event_sha256 !== eventSha256 ||
      canonicalJsonSha256(existing[0].metadata) !==
        canonicalJsonSha256(input.metadata)
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot state event replay conflicts",
      );
    }
    return;
  }
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_events (
      event_id, plan_id, event_type, event_sha256, metadata
    ) VALUES (
      ${eventId}, ${input.plan.planId}, ${input.eventType}, ${eventSha256},
      ${transaction.json(input.metadata as postgres.JSONValue)}
    )
  `;
}

export async function confirmCandidateSourceSnapshotDemoCapacity(
  databaseUrl: string,
  inputValue: {
    confirmedAt: string;
    confirmedPlanName: "Filebase Pro" | "Filebase Pro or better";
    confirmerReference: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotDurableState> {
  const input = z
    .strictObject({
      confirmedAt: operatorTimestampSchema,
      confirmedPlanName: z.enum(["Filebase Pro", "Filebase Pro or better"]),
      confirmerReference: operatorReferenceSchema,
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      const exact = planExactUploadFromRow(row);
      const confirmation = {
        confirmedAt: input.confirmedAt,
        confirmedPlanName: input.confirmedPlanName,
        confirmerReference: input.confirmerReference,
        planArtifact: exact.planArtifact,
        planId: plan.planId,
        planSha256: plan.planSha256,
      };
      const confirmationSha256 = canonicalJsonSha256(confirmation);
      const confirmationId = deterministicId("snapshotdemocapacity", [
        "candidate-source-snapshot-capacity-v1",
        plan.planId,
        confirmationSha256,
      ]);
      const existing = await transaction<
        {
          confirmation_id: string;
          confirmation_sha256: string;
          confirmed_at: Date;
          confirmed_plan_name: string;
          confirmer_reference: string;
        }[]
      >`
        SELECT confirmation_id, confirmation_sha256, confirmed_plan_name,
               confirmer_reference, confirmed_at
        FROM oracle_candidate_source_snapshot_demo_capacity_confirmations
        WHERE plan_id = ${plan.planId}
      `;
      if (existing[0]) {
        const prior = existing[0];
        if (
          prior.confirmation_id !== confirmationId ||
          prior.confirmation_sha256 !== confirmationSha256 ||
          prior.confirmed_plan_name !== input.confirmedPlanName ||
          prior.confirmer_reference !== input.confirmerReference ||
          prior.confirmed_at.toISOString() !== input.confirmedAt
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot capacity confirmation conflicts",
          );
        }
      } else {
        if (row.state !== "awaiting_configuration") {
          throw new DurableConflictError(
            "Candidate source-snapshot capacity confirmation is no longer admissible",
          );
        }
        await transaction`
          INSERT INTO oracle_candidate_source_snapshot_demo_capacity_confirmations (
            confirmation_id, plan_id, plan_sha256, plan_artifact_sha256,
            plan_artifact_cid, plan_artifact_remote_object_key,
            plan_artifact_bytes, confirmed_plan_name, confirmer_reference,
            confirmed_at, confirmation_sha256
          ) VALUES (
            ${confirmationId}, ${plan.planId}, ${plan.planSha256},
            ${exact.planArtifact.sha256}, ${exact.planArtifact.expectedCid},
            ${exact.planArtifact.remoteObjectKey}, ${exact.planArtifact.byteSize},
            ${input.confirmedPlanName}, ${input.confirmerReference},
            ${input.confirmedAt}, ${confirmationSha256}
          )
        `;
      }
      if (row.state === "awaiting_configuration") {
        const updated = await transaction<{ revision: number }[]>`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'awaiting_approval', revision = revision + 1
          WHERE plan_id = ${plan.planId} AND state = 'awaiting_configuration'
            AND revision = ${row.revision}
          RETURNING revision
        `;
        if (!updated[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot capacity transition lost its plan binding",
          );
        }
        row.state = "awaiting_approval";
        row.revision = updated[0].revision;
      }
      await recordStateEvent(transaction, {
        eventType: "capacity_confirmed",
        metadata: { confirmationId, confirmationSha256 },
        plan,
      });
      return durableState(plan, row, await counts(transaction, plan.planId));
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function assertExactApprovalPreflight(
  transaction: postgres.TransactionSql,
  planId: string,
): Promise<void> {
  await transaction`
    SELECT plan_id
    FROM oracle_candidate_source_snapshot_demo_request_categories
    WHERE plan_id = ${planId}
      AND request_category = 'bucket_names_preflight'
    FOR UPDATE
  `;
  await transaction`
    SELECT request_id
    FROM oracle_candidate_source_snapshot_demo_requests
    WHERE plan_id = ${planId}
      AND request_category = 'bucket_names_preflight'
    ORDER BY request_id
    FOR UPDATE
  `;
  const ready = await transaction<{ ready: boolean }[]>`
    SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
      ${planId}
    ) AS ready
  `;
  if (ready[0]?.ready !== true) {
    throw new DurableConflictError(
      "Candidate source-snapshot approval requires eight exact successful logical preflight receipts with closed bounded retry evidence",
    );
  }
}

async function assertExactApprovalDerivation(
  transaction: postgres.TransactionSql,
  planId: string,
): Promise<void> {
  const rows = await transaction<{ ready: boolean }[]>`
    SELECT oracle_candidate_source_snapshot_derivation_is_approval_ready(
      ${planId}
    ) AS ready
  `;
  if (rows[0]?.ready !== true) {
    throw new DurableConflictError(
      "Candidate source-snapshot approval requires its exact compatible predecessor derivation",
    );
  }
}

export async function approveCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  inputValue: {
    approvedAt: string;
    approverReference: string;
    authorizationStatement: string;
    implementationCommitSha: string;
    planId: string;
    planSha256: string;
  },
): Promise<{
  approvalId: string;
  approvalSha256: string;
  state: CandidateSourceSnapshotDurableState;
}> {
  const input = z
    .strictObject({
      approvedAt: operatorTimestampSchema,
      approverReference: operatorReferenceSchema,
      authorizationStatement: z.string().min(1).max(8_192),
      implementationCommitSha: z.string().regex(/^[a-f0-9]{40}$/),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      await assertExactApprovalDerivation(transaction, plan.planId);
      const exact = planExactUploadFromRow(row);
      const approval = createCandidateSourceSnapshotApprovalIdentity({
        approvedAt: input.approvedAt,
        approverReference: input.approverReference,
        exactUpload: exact,
        implementationCommitSha: input.implementationCommitSha,
        plan,
        statement: input.authorizationStatement,
      });
      const existing = await transaction<
        {
          approval_id: string;
          approval_sha256: string;
          approval_version: string;
          approved_at: Date;
          approved_at_iso: string;
          approved_plan_revision: number;
          approver_reference: string;
          authorization_binding: unknown;
          authorization_binding_sha256: string;
          authorization_statement: string;
          authorization_statement_sha256: string;
          implementation_commit_sha: string;
        }[]
      >`
        SELECT approval_id, approval_sha256, approval_version,
               approved_plan_revision, approver_reference, approved_at,
               approved_at_iso, authorization_statement,
               authorization_statement_sha256, authorization_binding,
               authorization_binding_sha256, implementation_commit_sha
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE plan_id = ${plan.planId}
      `;
      if (existing[0]) {
        if (
          existing[0].approval_id !== approval.approvalId ||
          existing[0].approval_sha256 !== approval.approvalSha256 ||
          existing[0].approval_version !== approval.approvalVersion ||
          existing[0].implementation_commit_sha !==
            input.implementationCommitSha ||
          existing[0].approver_reference !== input.approverReference ||
          existing[0].approved_at.toISOString() !== input.approvedAt ||
          existing[0].approved_at_iso !== input.approvedAt ||
          existing[0].authorization_statement !==
            approval.authorizationStatement ||
          existing[0].authorization_statement_sha256 !==
            approval.authorizationStatementSha256 ||
          existing[0].authorization_binding_sha256 !==
            approval.authorizationBindingSha256 ||
          canonicalJsonSha256(existing[0].authorization_binding) !==
            approval.authorizationBindingSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot approval conflicts with immutable approval",
          );
        }
      } else {
        if (row.state !== "awaiting_approval") {
          throw new DurableConflictError(
            "Candidate source-snapshot plan is not awaiting exact approval",
          );
        }
        await transaction`
          INSERT INTO oracle_candidate_source_snapshot_demo_approvals (
            approval_id, plan_id, plan_sha256, plan_artifact_sha256,
            plan_artifact_cid, plan_artifact_remote_object_key,
            plan_artifact_bytes, approved_plan_revision,
            approver_reference, approved_at, approved_at_iso,
            approval_version, approval_sha256, authorization_statement,
            authorization_statement_sha256, authorization_binding,
            authorization_binding_sha256, implementation_commit_sha
          ) VALUES (
            ${approval.approvalId}, ${plan.planId}, ${plan.planSha256},
            ${exact.planArtifact.sha256}, ${exact.planArtifact.expectedCid},
            ${exact.planArtifact.remoteObjectKey}, ${exact.planArtifact.byteSize},
            ${row.revision}, ${input.approverReference}, ${input.approvedAt},
            ${input.approvedAt}, ${approval.approvalVersion},
            ${approval.approvalSha256}, ${approval.authorizationStatement},
            ${approval.authorizationStatementSha256},
            ${transaction.json(approval.authorizationBinding as postgres.JSONValue)},
            ${approval.authorizationBindingSha256},
            ${input.implementationCommitSha}
          )
        `;
      }
      if (row.state === "awaiting_approval") {
        const updated = await transaction<{ revision: number }[]>`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'approved', revision = revision + 1
          WHERE plan_id = ${plan.planId} AND state = 'awaiting_approval'
            AND revision = ${row.revision}
          RETURNING revision
        `;
        if (!updated[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot approval transition lost its plan binding",
          );
        }
        row.state = "approved";
        row.revision = updated[0].revision;
      }
      await recordStateEvent(transaction, {
        eventType: "plan_approved",
        metadata: {
          approvalId: approval.approvalId,
          approvalSha256: approval.approvalSha256,
          authorizationStatementSha256: approval.authorizationStatementSha256,
        },
        plan,
      });
      return {
        approvalId: approval.approvalId,
        approvalSha256: approval.approvalSha256,
        state: durableState(plan, row, await counts(transaction, plan.planId)),
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function beginCandidateSourceSnapshotDemoExecution(
  databaseUrl: string,
  inputValue: {
    approvalId: string;
    continuationAuthorizationId?: string | null;
    executorEnabled: true;
    implementationCommitSha: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotDurableState> {
  const input = z
    .strictObject({
      approvalId: z.string().regex(/^snapshotdemoapproval_[a-f0-9]{32}$/),
      continuationAuthorizationId: z
        .string()
        .regex(/^snapshotdemocontinuation_[a-f0-9]{32}$/)
        .nullable()
        .default(null),
      executorEnabled: z.literal(true),
      implementationCommitSha: z.string().regex(/^[a-f0-9]{40}$/),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      await assertExactApprovalPreflight(transaction, plan.planId);
      const approvals = await transaction<{ approval_id: string }[]>`
        SELECT approval.approval_id
        FROM oracle_candidate_source_snapshot_demo_approvals approval
        LEFT JOIN oracle_candidate_source_preflight_continuation_authorizations continuation
          ON continuation.authorization_id = ${input.continuationAuthorizationId}
         AND continuation.plan_id = approval.plan_id
         AND continuation.plan_sha256 = approval.plan_sha256
         AND continuation.approval_id = approval.approval_id
        WHERE approval.approval_id = ${input.approvalId}
          AND approval.plan_id = ${plan.planId}
          AND approval.plan_sha256 = ${plan.planSha256}
          AND (
            (${input.continuationAuthorizationId}::text IS NULL AND
             approval.implementation_commit_sha =
               ${input.implementationCommitSha} AND
             NOT EXISTS (
               SELECT 1
               FROM oracle_candidate_source_preflight_continuation_authorizations required_continuation
               WHERE required_continuation.plan_id = approval.plan_id
                 AND required_continuation.plan_sha256 = approval.plan_sha256
             )) OR
            (${input.continuationAuthorizationId}::text IS NOT NULL AND
             continuation.amended_implementation_commit_sha =
               ${input.implementationCommitSha})
          )
      `;
      if (!approvals[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot execution lacks exact approval",
        );
      }
      if (row.state === "approved") {
        const updated = await transaction<{ revision: number }[]>`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'executing', revision = revision + 1
          WHERE plan_id = ${plan.planId} AND state = 'approved'
            AND revision = ${row.revision}
          RETURNING revision
        `;
        if (!updated[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot execution transition lost its plan binding",
          );
        }
        row.state = "executing";
        row.revision = updated[0].revision;
      } else if (row.state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot plan is not executable",
        );
      }
      await recordStateEvent(transaction, {
        eventType: "execution_started",
        metadata: {
          approvalId: input.approvalId,
          continuationAuthorizationId: input.continuationAuthorizationId,
          implementationCommitSha: input.implementationCommitSha,
        },
        plan,
      });
      return durableState(plan, row, await counts(transaction, plan.planId));
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotIpnsIntentRecord {
  approvalId: string;
  cutoverPosition: 1 | 2;
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  priorCid: string;
  resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1";
  rollbackPosition: 1 | 2;
  state: "intent_recorded";
  targetCid: string;
  uploadClosureId: string;
}

export interface CandidateSourceSnapshotDemoExecutionAdmission {
  accounting: CandidateSourceSnapshotPlanAccounting;
  approval: {
    approvalId: string;
    approvalSha256: string;
    approvedAt: string;
    approverReference: string;
    authorizationBindingSha256: string;
    authorizationStatementSha256: string;
    implementationCommitSha: string;
  };
  capacityConfirmation: {
    confirmationId: string;
    confirmationSha256: string;
    confirmedPlanName: "Filebase Pro" | "Filebase Pro or better";
  };
  exactUpload: CandidateSourceSnapshotExactUploadBinding;
  intents: readonly CandidateSourceSnapshotIpnsIntentRecord[];
  plan: CandidateSourceSnapshotDemoPlan;
  state: CandidateSourceSnapshotDurableState;
  unverifiedObjectCount: number;
}

export async function loadCandidateSourceSnapshotDemoExecutionAdmission(
  databaseUrl: string,
  identityValue: {
    approvalId: string;
    implementationCommitSha: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotDemoExecutionAdmission> {
  const identity = z
    .strictObject({
      approvalId: z.string().regex(/^snapshotdemoapproval_[a-f0-9]{32}$/),
      implementationCommitSha: z.string().regex(/^[a-f0-9]{40}$/),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(identityValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(
        transaction,
        identity,
      );
      const approvals = await transaction<
        {
          approval_id: string;
          approval_sha256: string;
          approved_at: Date;
          approver_reference: string;
          authorization_binding_sha256: string;
          authorization_statement_sha256: string;
          implementation_commit_sha: string;
        }[]
      >`
        SELECT approval_id, approval_sha256, approver_reference, approved_at,
               authorization_binding_sha256, authorization_statement_sha256,
               implementation_commit_sha
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${identity.approvalId}
          AND plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
          AND implementation_commit_sha = ${identity.implementationCommitSha}
      `;
      const confirmations = await transaction<
        {
          confirmation_id: string;
          confirmation_sha256: string;
          confirmed_plan_name: "Filebase Pro" | "Filebase Pro or better";
        }[]
      >`
        SELECT confirmation_id, confirmation_sha256, confirmed_plan_name
        FROM oracle_candidate_source_snapshot_demo_capacity_confirmations
        WHERE plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
      `;
      if (!approvals[0] || !confirmations[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot execution admission is incomplete",
        );
      }
      const intentRows = await transaction<
        {
          approval_id: string;
          cutover_position: 1 | 2;
          domain: CandidateSourceSnapshotIpnsDomain;
          intent_id: string;
          prior_cid: string;
          resolver_policy: "candidate_source_snapshot_filebase_delegated_v1";
          rollback_position: 1 | 2;
          state: string;
          target_cid: string;
          upload_closure_id: string;
        }[]
      >`
        SELECT intent.intent_id, intent.approval_id, intent.upload_closure_id,
               intent.resolver_policy, intent.cutover_position,
               intent.rollback_position, intent.domain, intent.prior_cid,
               intent.target_cid, intent_state.state
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.plan_id = ${plan.planId}
        ORDER BY intent.domain
      `;
      const objectRows = await transaction<{ unverified: string }[]>`
        SELECT count(*) FILTER (WHERE status <> 'verified')::text AS unverified
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${plan.planId}
      `;
      const accountingRows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
      `;
      if (!accountingRows[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot accounting is missing",
        );
      }
      const intents = intentRows.map((intent) => {
        if (intent.state !== "intent_recorded") {
          throw new DurableConflictError(
            "Candidate source-snapshot execution admission has advanced IPNS state",
          );
        }
        return {
          approvalId: intent.approval_id,
          cutoverPosition: intent.cutover_position,
          domain: intent.domain,
          intentId: intent.intent_id,
          priorCid: intent.prior_cid,
          resolverPolicy: intent.resolver_policy,
          rollbackPosition: intent.rollback_position,
          state: "intent_recorded" as const,
          targetCid: intent.target_cid,
          uploadClosureId: intent.upload_closure_id,
        };
      });
      return {
        accounting: accounting(accountingRows[0]),
        approval: {
          approvalId: approvals[0].approval_id,
          approvalSha256: approvals[0].approval_sha256,
          approvedAt: approvals[0].approved_at.toISOString(),
          approverReference: approvals[0].approver_reference,
          authorizationBindingSha256: approvals[0].authorization_binding_sha256,
          authorizationStatementSha256:
            approvals[0].authorization_statement_sha256,
          implementationCommitSha: approvals[0].implementation_commit_sha,
        },
        capacityConfirmation: {
          confirmationId: confirmations[0].confirmation_id,
          confirmationSha256: confirmations[0].confirmation_sha256,
          confirmedPlanName: confirmations[0].confirmed_plan_name,
        },
        exactUpload: planExactUploadFromRow(row),
        intents,
        plan,
        state: durableState(plan, row, await counts(transaction, plan.planId)),
        unverifiedObjectCount: Number(objectRows[0]?.unverified ?? 0),
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function createCandidateSourceSnapshotDemoIpnsIntents(
  databaseUrl: string,
  inputValue: {
    intendedAt: string;
    planId: string;
    planSha256: string;
  },
): Promise<readonly CandidateSourceSnapshotIpnsIntentRecord[]> {
  const input = z
    .strictObject({
      intendedAt: operatorTimestampSchema,
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      if (row.state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot intents require the exact executing plan",
        );
      }
      const closureRows = await transaction<
        { approval_id: string; closure_id: string }[]
      >`
        SELECT closure_id, approval_id
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
      `;
      const closure = closureRows[0];
      if (!closure) {
        throw new DurableConflictError(
          "Candidate source-snapshot intents require immutable upload closure",
        );
      }
      const targets = [
        {
          cutoverPosition: 1 as const,
          domain: "open_data" as const,
          rollbackPosition: 2 as const,
          target: plan.targets.openData,
        },
        {
          cutoverPosition: 2 as const,
          domain: "query_table" as const,
          rollbackPosition: 1 as const,
          target: plan.targets.queryTable,
        },
      ];
      const records: CandidateSourceSnapshotIpnsIntentRecord[] = [];
      for (const {
        cutoverPosition,
        domain,
        rollbackPosition,
        target,
      } of targets) {
        const resolverPolicy =
          "candidate_source_snapshot_filebase_delegated_v1" as const;
        const intentId = deterministicId("snapshotdemointent", [
          "candidate-source-snapshot-intent-v1",
          plan.planId,
          plan.planSha256,
          closure.approval_id,
          closure.closure_id,
          domain,
          resolverPolicy,
          String(cutoverPosition),
          String(rollbackPosition),
          target.bucket,
          target.ipnsLabel,
          target.ipnsNetworkKey,
          target.priorCid,
          target.targetCid,
          input.intendedAt,
        ]);
        const existing = await transaction<
          {
            approval_id: string;
            bucket: string;
            cutover_position: number;
            intent_id: string;
            intended_at: Date;
            ipns_label: string;
            ipns_network_key: string;
            prior_cid: string;
            resolver_policy: string;
            rollback_position: number;
            state: string;
            target_cid: string;
            upload_closure_id: string;
          }[]
        >`
          SELECT intent.intent_id, intent.approval_id,
                 intent.upload_closure_id, intent.resolver_policy,
                 intent.cutover_position, intent.rollback_position,
                 intent.bucket, intent.ipns_label,
                 intent.ipns_network_key, intent.prior_cid, intent.target_cid,
                 intent.intended_at, intent_state.state
          FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
          JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
            ON intent_state.intent_id = intent.intent_id
          WHERE intent.plan_id = ${plan.planId} AND intent.domain = ${domain}
        `;
        if (existing[0]) {
          const prior = existing[0];
          if (
            prior.intent_id !== intentId ||
            prior.approval_id !== closure.approval_id ||
            prior.upload_closure_id !== closure.closure_id ||
            prior.resolver_policy !== resolverPolicy ||
            prior.cutover_position !== cutoverPosition ||
            prior.rollback_position !== rollbackPosition ||
            prior.bucket !== target.bucket ||
            prior.ipns_label !== target.ipnsLabel ||
            prior.ipns_network_key !== target.ipnsNetworkKey ||
            prior.prior_cid !== target.priorCid ||
            prior.target_cid !== target.targetCid ||
            prior.intended_at.toISOString() !== input.intendedAt
          ) {
            throw new DurableConflictError(
              "Candidate source-snapshot intent replay conflicts",
            );
          }
          if (prior.state !== "intent_recorded") {
            throw new DurableConflictError(
              "Candidate source-snapshot intent replay cannot replace advanced state",
            );
          }
        } else {
          await transaction`
            INSERT INTO oracle_candidate_source_snapshot_demo_ipns_intents (
              intent_id, plan_id, plan_sha256, approval_id, upload_closure_id,
              resolver_policy, cutover_position, rollback_position,
              domain, bucket, ipns_label, ipns_network_key, prior_cid,
              target_cid, intended_at
            ) VALUES (
              ${intentId}, ${plan.planId}, ${plan.planSha256},
              ${closure.approval_id}, ${closure.closure_id}, ${resolverPolicy},
              ${cutoverPosition}, ${rollbackPosition}, ${domain},
              ${target.bucket}, ${target.ipnsLabel}, ${target.ipnsNetworkKey},
              ${target.priorCid}, ${target.targetCid}, ${input.intendedAt}
            )
          `;
          await transaction`
            INSERT INTO oracle_candidate_source_snapshot_demo_ipns_intent_state (
              intent_id, plan_id, domain, state, revision
            ) VALUES (
              ${intentId}, ${plan.planId}, ${domain}, 'intent_recorded', 1
            )
          `;
        }
        records.push({
          approvalId: closure.approval_id,
          cutoverPosition,
          domain,
          intentId,
          priorCid: target.priorCid,
          resolverPolicy,
          rollbackPosition,
          state: "intent_recorded",
          targetCid: target.targetCid,
          uploadClosureId: closure.closure_id,
        });
      }
      return records;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function failCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  inputValue: {
    planId: string;
    planSha256: string;
    reasonCode: string;
  },
): Promise<CandidateSourceSnapshotDurableState> {
  const input = z
    .strictObject({
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
      reasonCode: z.string().regex(/^[a-z0-9][a-z0-9_]{2,63}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      if (row.state !== "failed_terminal") {
        if (row.state === "completed") {
          throw new DurableConflictError(
            "Completed candidate source-snapshot plan cannot be failed",
          );
        }
        const updated = await transaction<{ revision: number }[]>`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'failed_terminal', revision = revision + 1
          WHERE plan_id = ${plan.planId} AND revision = ${row.revision}
          RETURNING revision
        `;
        if (!updated[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot failure transition lost its plan binding",
          );
        }
        row.state = "failed_terminal";
        row.revision = updated[0].revision;
      }
      await recordStateEvent(transaction, {
        eventType: "plan_failed_terminal",
        metadata: { reasonCode: input.reasonCode },
        plan,
      });
      return durableState(plan, row, await counts(transaction, plan.planId));
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

interface ObjectRow {
  attempt_count: number;
  provider_cid: string | null;
  receipt_sha256: string | null;
  request_count: number;
  revision: number;
  status: CandidateSourceSnapshotUploadCheckpoint["status"];
}

interface AccountingRow {
  class_a_mutation_count: number;
  class_b_read_count: number;
  free_operation_count: number;
  names_api_count: number;
  public_resolver_count: number;
  request_cost_usd: string | number;
  request_count: number;
  revision: number;
}

export type CandidateSourceSnapshotIpnsDomain = "open_data" | "query_table";
export type { CandidateSourceSnapshotRequestCategory };
export type CandidateSourceSnapshotIpnsResolver =
  | "filebase_control"
  | "filebase_gateway"
  | "delegated_ipfs"
  | "ipfs_io"
  | "dweb_link";

export interface CandidateSourceSnapshotRemoteRequestAdmission {
  accounting: CandidateSourceSnapshotPlanAccounting;
  alreadyRecorded: boolean;
  cycleSequence: number | null;
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  operationKind: "names_read" | "names_update" | "public_resolve";
  outcome:
    | "request_started"
    | "succeeded"
    | "ambiguous"
    | "retryable_failure"
    | "timeout_unknown"
    | "terminal_failure";
  requestId: string;
  requestCategory: CandidateSourceSnapshotRequestCategory;
  resolver: CandidateSourceSnapshotIpnsResolver | null;
}

export interface CandidateSourceSnapshotIpnsAttemptAdmission {
  attemptId: string;
  attemptSequence: number;
  direction: "rollback" | "update";
  request: CandidateSourceSnapshotRemoteRequestAdmission;
  requestedCid: string;
}

export interface CandidateSourceSnapshotInterruptedIpnsAttempt {
  admission: CandidateSourceSnapshotIpnsAttemptAdmission;
  receiptSha256: string;
}

interface IpnsIntentRow {
  domain: CandidateSourceSnapshotIpnsDomain;
  intent_id: string;
  plan_id: string;
  prior_cid: string;
  state: string;
  target_cid: string;
}

interface RemoteRequestRow {
  completed_at: Date | null;
  cycle_sequence: number | null;
  domain: CandidateSourceSnapshotIpnsDomain;
  intent_id: string;
  operation_kind: CandidateSourceSnapshotRemoteRequestAdmission["operationKind"];
  outcome: CandidateSourceSnapshotRemoteRequestAdmission["outcome"];
  receipt_sha256: string | null;
  request_id: string;
  request_category: CandidateSourceSnapshotRequestCategory;
  resolver: CandidateSourceSnapshotIpnsResolver | null;
}

const ipnsDomainSchema = z.enum(["open_data", "query_table"]);
const ipnsResolverSchema = z.enum([
  "filebase_control",
  "filebase_gateway",
  "delegated_ipfs",
  "ipfs_io",
  "dweb_link",
]);
const requestCategorySchema = z.enum([
  "upload_provider_cid",
  "ambiguous_upload_inspection",
  "bucket_names_preflight",
  "names_mutation",
  "control_public_observation",
  "recovery",
  "rollback",
  "final_credential_free_verification",
]);
const ipnsRequestOutcomeSchema = z.enum([
  "succeeded",
  "ambiguous",
  "retryable_failure",
  "timeout_unknown",
  "terminal_failure",
]);
const ipnsObservationClassificationSchema = z.enum([
  "prior",
  "target",
  "split",
  "unavailable",
  "unexpected_cid",
]);
const preflightOperationKindSchema = z.enum([
  "bucket_head",
  "bucket_prefix_scan",
  "storage_network_check",
  "account_usage",
  "bucket_usage",
  "names_read",
  "public_resolve",
]);
const preflightRequestOutcomeSchema = z.enum([
  "succeeded",
  "absent",
  "ambiguous",
  "retryable_failure",
  "timeout_unknown",
  "terminal_failure",
]);
const ipnsIntentStateSchema = z.enum([
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

export interface CandidateSourceSnapshotPreflightRequestAdmission {
  alreadyRecorded: boolean;
  attemptSequence: number;
  continuationAuthorizationId: string | null;
  domain: CandidateSourceSnapshotIpnsDomain;
  logicalRequestId: string;
  operationKind: z.infer<typeof preflightOperationKindSchema>;
  outcome: "request_started" | z.infer<typeof preflightRequestOutcomeSchema>;
  planId: string;
  planSha256: string;
  redirectSequence: number;
  requestId: string;
  resolver: Extract<
    CandidateSourceSnapshotIpnsResolver,
    "filebase_control" | "filebase_gateway" | "delegated_ipfs"
  > | null;
}

export interface CandidateSourceSnapshotIpnsIntentStateRecord {
  approvalId: string;
  bucket: string;
  cutoverPosition: 1 | 2;
  domain: CandidateSourceSnapshotIpnsDomain;
  intendedAt: string;
  intentId: string;
  ipnsLabel: string;
  ipnsNetworkKey: string;
  mutationAttemptCount: number;
  rollbackAttemptCount: number;
  planId: string;
  planSha256: string;
  priorCid: string;
  resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1";
  revision: number;
  rollbackPosition: 1 | 2;
  state: z.infer<typeof ipnsIntentStateSchema>;
  targetCid: string;
  updateAttemptCount: number;
  uploadClosureId: string;
}

interface CandidateSourceSnapshotIpnsIntentStateRow {
  approval_id: string;
  bucket: string;
  cutover_position: 1 | 2;
  domain: CandidateSourceSnapshotIpnsDomain;
  intended_at: Date;
  intent_id: string;
  ipns_label: string;
  ipns_network_key: string;
  mutation_attempt_count: string;
  rollback_attempt_count: string;
  prior_cid: string;
  resolver_policy: "candidate_source_snapshot_filebase_delegated_v1";
  revision: number;
  rollback_position: 1 | 2;
  state: string;
  target_cid: string;
  upload_closure_id: string;
  update_attempt_count: string;
}

function ipnsIntentStateRecord(
  plan: CandidateSourceSnapshotDemoPlan,
  row: CandidateSourceSnapshotIpnsIntentStateRow,
): CandidateSourceSnapshotIpnsIntentStateRecord {
  return {
    approvalId: row.approval_id,
    bucket: row.bucket,
    cutoverPosition: row.cutover_position,
    domain: row.domain,
    intendedAt: row.intended_at.toISOString(),
    intentId: row.intent_id,
    ipnsLabel: row.ipns_label,
    ipnsNetworkKey: row.ipns_network_key,
    mutationAttemptCount: Number(row.mutation_attempt_count),
    rollbackAttemptCount: Number(row.rollback_attempt_count),
    planId: plan.planId,
    planSha256: plan.planSha256,
    priorCid: row.prior_cid,
    resolverPolicy: row.resolver_policy,
    revision: row.revision,
    rollbackPosition: row.rollback_position,
    state: ipnsIntentStateSchema.parse(row.state),
    targetCid: row.target_cid,
    updateAttemptCount: Number(row.update_attempt_count),
    uploadClosureId: row.upload_closure_id,
  };
}

function remoteRequestId(input: {
  cycleSequence: number | null;
  identityScope: string;
  intentId: string;
  operationKind: CandidateSourceSnapshotRemoteRequestAdmission["operationKind"];
  planId: string;
  resolver: CandidateSourceSnapshotIpnsResolver | null;
  sequence: number;
}): string {
  return deterministicId("snapshotdemorequest", [
    "candidate-ipns-request-v1",
    input.planId,
    input.intentId,
    input.identityScope,
    input.operationKind,
    String(input.cycleSequence ?? 0),
    input.resolver ?? "none",
    String(input.sequence),
  ]);
}

function ipnsAttemptId(input: {
  direction: "rollback" | "update";
  intentId: string;
  planId: string;
  sequence: number;
}): string {
  return deterministicId("snapshotdemoipnsattempt", [
    "candidate-ipns-attempt-v1",
    input.planId,
    input.intentId,
    input.direction,
    String(input.sequence),
  ]);
}

function ipnsObservationId(input: {
  cycleSequence: number;
  intentId: string;
  resolver: CandidateSourceSnapshotIpnsResolver;
}): string {
  return deterministicId("snapshotdemoipnsobservation", [
    "candidate-ipns-observation-v1",
    input.intentId,
    String(input.cycleSequence),
    input.resolver,
  ]);
}

function checkpoint(row: ObjectRow): CandidateSourceSnapshotUploadCheckpoint {
  return {
    attemptCount: row.attempt_count,
    providerCid: row.provider_cid,
    receiptSha256: row.receipt_sha256,
    requestCount: row.request_count,
    status: row.status,
  };
}

function accounting(row: AccountingRow): CandidateSourceSnapshotPlanAccounting {
  return {
    classAMutationCount: row.class_a_mutation_count,
    classBReadCount: row.class_b_read_count,
    freeOperationCount: row.free_operation_count,
    namesApiCount: row.names_api_count,
    publicResolverCount: row.public_resolver_count,
    requestCostUsd: Number(row.request_cost_usd),
    requestCount: row.request_count,
  };
}

function requestId(input: {
  object: CandidateSourceSnapshotUploadObject;
  operation: "inspect" | "upload";
  plan: CandidateSourceSnapshotDemoPlan;
  sequence: number;
}): string {
  return deterministicId("snapshotdemorequest", [
    "1.0.0",
    input.plan.planId,
    input.object.domain,
    input.object.remoteObjectKey,
    input.operation,
    String(input.sequence),
  ]);
}

function logicalRequestId(input: {
  category: CandidateSourceSnapshotRequestCategory;
  domain: CandidateSourceSnapshotIpnsDomain;
  identity: string;
  operationKind: string;
  planId: string;
}): string {
  return deterministicId("snapshotdemologicalrequest", [
    "candidate-source-snapshot-logical-request-v1",
    input.planId,
    input.category,
    input.domain,
    input.identity,
    input.operationKind,
  ]);
}

async function incrementRequestCategory(
  transaction: postgres.TransactionSql,
  input: {
    category: CandidateSourceSnapshotRequestCategory;
    plan: CandidateSourceSnapshotDemoPlan;
    requestCostUsd: number;
  },
): Promise<void> {
  const updated = await transaction<{ request_category: string }[]>`
    UPDATE oracle_candidate_source_snapshot_demo_request_categories
    SET consumed_request_count = consumed_request_count + 1,
        request_cost_usd = request_cost_usd + ${input.requestCostUsd},
        revision = revision + 1
    WHERE plan_id = ${input.plan.planId}
      AND request_category = ${input.category}
      AND consumed_request_count < planned_maximum_request_count
    RETURNING request_category
  `;
  if (!updated[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot request category allowance is exhausted",
    );
  }
}

function preflightOperationClass(
  operationKind: z.infer<typeof preflightOperationKindSchema>,
): "class_b_read" | "names_api" | "public_resolver" {
  if (operationKind === "names_read") return "names_api";
  if (operationKind === "public_resolve") return "public_resolver";
  return "class_b_read";
}

export async function admitCandidateSourceSnapshotPreflightRequest(
  databaseUrl: string,
  inputValue: {
    attemptSequence: number;
    continuationAuthorizationId?: string | null;
    domain: CandidateSourceSnapshotIpnsDomain;
    operationKind: z.infer<typeof preflightOperationKindSchema>;
    planId: string;
    planSha256: string;
    redirectSequence: number;
    resolver?: Extract<
      CandidateSourceSnapshotIpnsResolver,
      "filebase_control" | "filebase_gateway" | "delegated_ipfs"
    > | null;
  },
): Promise<CandidateSourceSnapshotPreflightRequestAdmission> {
  const input = z
    .strictObject({
      attemptSequence: z.number().int().min(1).max(3),
      continuationAuthorizationId: z
        .string()
        .regex(/^snapshotdemocontinuation_[a-f0-9]{32}$/)
        .nullable()
        .default(null),
      domain: ipnsDomainSchema,
      operationKind: preflightOperationKindSchema,
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
      redirectSequence: z.number().int().min(0).max(2),
      resolver: z
        .enum(["filebase_control", "filebase_gateway", "delegated_ipfs"])
        .nullable()
        .default(null),
    })
    .superRefine((value, context) => {
      const resolverIsValid =
        (value.operationKind === "names_read" &&
          value.resolver === "filebase_control") ||
        (value.operationKind === "public_resolve" &&
          (value.resolver === "filebase_gateway" ||
            value.resolver === "delegated_ipfs")) ||
        (!(["names_read", "public_resolve"] as const).includes(
          value.operationKind as "names_read" | "public_resolve",
        ) &&
          value.resolver === null);
      if (!resolverIsValid) {
        context.addIssue({
          code: "custom",
          message: "preflight resolver does not match its closed operation",
        });
      }
      if (
        value.continuationAuthorizationId !== null &&
        !(
          [2, 3].includes(value.attemptSequence) &&
          value.redirectSequence === 0 &&
          value.domain === "open_data" &&
          value.operationKind === "public_resolve" &&
          value.resolver === "filebase_gateway"
        )
      ) {
        context.addIssue({
          code: "custom",
          message:
            "continuation authorization is restricted to an exact authorized official-gateway observation",
          path: ["continuationAuthorizationId"],
        });
      }
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      if (row.state !== "approved") {
        throw new DurableConflictError(
          "Candidate source-snapshot preflight requires an eligible exact plan",
        );
      }
      const target =
        input.domain === "open_data"
          ? plan.targets.openData
          : plan.targets.queryTable;
      const identity =
        input.operationKind === "names_read" ||
        input.operationKind === "public_resolve"
          ? `${target.ipnsNetworkKey}:${input.resolver}`
          : target.bucket;
      const logicalId = logicalRequestId({
        category: "bucket_names_preflight",
        domain: input.domain,
        identity,
        operationKind: input.operationKind,
        planId: plan.planId,
      });
      const id = deterministicId("snapshotdemorequest", [
        "candidate-source-snapshot-preflight-request-v1",
        plan.planId,
        logicalId,
        String(input.attemptSequence),
        String(input.redirectSequence),
      ]);
      const existing = await transaction<
        {
          attempt_sequence: number;
          continuation_authorization_id: string | null;
          domain: CandidateSourceSnapshotIpnsDomain;
          logical_request_id: string;
          operation_kind: z.infer<typeof preflightOperationKindSchema>;
          outcome: CandidateSourceSnapshotPreflightRequestAdmission["outcome"];
          redirect_sequence: number;
          request_id: string;
          resolver: CandidateSourceSnapshotPreflightRequestAdmission["resolver"];
        }[]
      >`
        SELECT request_id, logical_request_id, domain, operation_kind,
               resolver, attempt_sequence, redirect_sequence, outcome,
               continuation_authorization_id
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE request_id = ${id}
        FOR UPDATE
      `;
      if (existing[0]) {
        const replay = existing[0];
        if (
          replay.logical_request_id !== logicalId ||
          replay.domain !== input.domain ||
          replay.operation_kind !== input.operationKind ||
          replay.resolver !== input.resolver ||
          replay.attempt_sequence !== input.attemptSequence ||
          replay.redirect_sequence !== input.redirectSequence ||
          replay.continuation_authorization_id !==
            input.continuationAuthorizationId
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot preflight admission replay conflicts",
          );
        }
        return {
          ...input,
          alreadyRecorded: true,
          logicalRequestId: logicalId,
          outcome: replay.outcome,
          requestId: id,
        };
      }
      if (input.continuationAuthorizationId === null) {
        const continuationHistory = await transaction<{ request_id: string }[]>`
          SELECT request_id
          FROM oracle_candidate_source_snapshot_demo_requests
          WHERE plan_id = ${plan.planId}
            AND request_category = 'bucket_names_preflight'
            AND logical_request_id = ${logicalId}
            AND continuation_authorization_id IS NOT NULL
          LIMIT 1
          FOR SHARE
        `;
        if (continuationHistory[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot continuation authorizes only its exact named observation",
          );
        }
      }
      const accountingRows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
        FOR UPDATE
      `;
      const current = accountingRows[0];
      if (!current) {
        throw new DurableConflictError(
          "Candidate source-snapshot preflight accounting is missing",
        );
      }
      const requestCostUsd = 0.0045 / 1_000;
      const operationClass = preflightOperationClass(input.operationKind);
      const nextRequestCount = current.request_count + 1;
      const nextRequestCostUsd =
        Number(current.request_cost_usd) + requestCostUsd;
      if (
        nextRequestCount > plan.requestEnvelope.maximumTotalRequests ||
        nextRequestCostUsd > plan.costEnvelope.requestUsd.maximumAttempts
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot preflight request allowance is exhausted",
        );
      }
      const updated = await transaction<{ plan_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_accounting
        SET request_count = request_count + 1,
            class_b_read_count = class_b_read_count +
              ${operationClass === "class_b_read" ? 1 : 0},
            names_api_count = names_api_count +
              ${operationClass === "names_api" ? 1 : 0},
            public_resolver_count = public_resolver_count +
              ${operationClass === "public_resolver" ? 1 : 0},
            request_cost_usd = request_cost_usd + ${requestCostUsd},
            revision = revision + 1
        WHERE plan_id = ${plan.planId} AND revision = ${current.revision}
        RETURNING plan_id
      `;
      if (!updated[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot preflight lost global admission",
        );
      }
      await incrementRequestCategory(transaction, {
        category: "bucket_names_preflight",
        plan,
        requestCostUsd,
      });
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_requests (
          request_id, plan_id, operation_class, operation_kind, domain,
          resolver, request_cost_usd, outcome, request_category,
          logical_request_id, attempt_sequence, redirect_sequence,
          continuation_authorization_id
        ) VALUES (
          ${id}, ${plan.planId}, ${operationClass}, ${input.operationKind},
          ${input.domain}, ${input.resolver}, ${requestCostUsd},
          'request_started', 'bucket_names_preflight', ${logicalId},
          ${input.attemptSequence}, ${input.redirectSequence},
          ${input.continuationAuthorizationId}
        )
      `;
      return {
        ...input,
        alreadyRecorded: false,
        logicalRequestId: logicalId,
        outcome: "request_started" as const,
        requestId: id,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotPreflightRequestOutcomeInput {
  admission: CandidateSourceSnapshotPreflightRequestAdmission;
  completedAt: string;
  outcome: z.infer<typeof preflightRequestOutcomeSchema>;
  receiptSha256: string;
}

async function recordPreflightRequestOutcomeInTransaction(
  transaction: postgres.TransactionSql,
  inputValue: CandidateSourceSnapshotPreflightRequestOutcomeInput,
): Promise<void> {
  const completedAt = operatorTimestampSchema.parse(inputValue.completedAt);
  const outcome = preflightRequestOutcomeSchema.parse(inputValue.outcome);
  const receiptSha256 = z
    .string()
    .regex(/^[a-f0-9]{64}$/)
    .parse(inputValue.receiptSha256);
  const admission = inputValue.admission;
  const { plan } = await loadExactPlanRowForUpdate(transaction, admission);
  const rows = await transaction<
    {
      attempt_sequence: number;
      continuation_authorization_id: string | null;
      logical_request_id: string;
      operation_kind: string;
      outcome: string;
      receipt_sha256: string | null;
      redirect_sequence: number;
      resolver: string | null;
    }[]
  >`
    SELECT logical_request_id, operation_kind, resolver,
           attempt_sequence, redirect_sequence, outcome, receipt_sha256,
           continuation_authorization_id
    FROM oracle_candidate_source_snapshot_demo_requests
    WHERE request_id = ${admission.requestId}
      AND plan_id = ${plan.planId}
      AND request_category = 'bucket_names_preflight'
    FOR UPDATE
  `;
  const row = rows[0];
  if (
    !row ||
    row.logical_request_id !== admission.logicalRequestId ||
    row.operation_kind !== admission.operationKind ||
    row.resolver !== admission.resolver ||
    row.attempt_sequence !== admission.attemptSequence ||
    row.redirect_sequence !== admission.redirectSequence ||
    row.continuation_authorization_id !== admission.continuationAuthorizationId
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot preflight outcome lacks exact admission",
    );
  }
  if (row.outcome !== "request_started") {
    if (row.outcome === outcome && row.receipt_sha256 === receiptSha256) {
      return;
    }
    throw new DurableConflictError(
      "Candidate source-snapshot preflight outcome replay conflicts",
    );
  }
  const updated = await transaction<{ request_id: string }[]>`
    UPDATE oracle_candidate_source_snapshot_demo_requests
    SET outcome = ${outcome}, receipt_sha256 = ${receiptSha256},
        completed_at = ${completedAt}
    WHERE request_id = ${admission.requestId}
      AND outcome = 'request_started'
    RETURNING request_id
  `;
  if (!updated[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot preflight outcome lost its admission",
    );
  }
}

export async function recordCandidateSourceSnapshotPreflightRequestOutcome(
  databaseUrl: string,
  inputValue: CandidateSourceSnapshotPreflightRequestOutcomeInput,
): Promise<void> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      await lock(transaction);
      await recordPreflightRequestOutcomeInTransaction(transaction, inputValue);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadCandidateSourceSnapshotPreflightRequestOutcome(
  databaseUrl: string,
  admission: CandidateSourceSnapshotPreflightRequestAdmission,
): Promise<{
  completedAt: string | null;
  outcome: CandidateSourceSnapshotPreflightRequestAdmission["outcome"];
  receiptSha256: string | null;
  requestId: string;
}> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<
      {
        attempt_sequence: number;
        continuation_authorization_id: string | null;
        completed_at: Date | null;
        domain: CandidateSourceSnapshotIpnsDomain;
        logical_request_id: string;
        operation_kind: string;
        outcome: CandidateSourceSnapshotPreflightRequestAdmission["outcome"];
        receipt_sha256: string | null;
        redirect_sequence: number;
        resolver: CandidateSourceSnapshotPreflightRequestAdmission["resolver"];
      }[]
    >`
      SELECT logical_request_id, domain, operation_kind, resolver,
             attempt_sequence, redirect_sequence, outcome, receipt_sha256,
             completed_at, continuation_authorization_id
      FROM oracle_candidate_source_snapshot_demo_requests
      WHERE request_id = ${admission.requestId}
        AND plan_id = ${admission.planId}
        AND request_category = 'bucket_names_preflight'
    `;
    const row = rows[0];
    if (
      !row ||
      row.logical_request_id !== admission.logicalRequestId ||
      row.domain !== admission.domain ||
      row.operation_kind !== admission.operationKind ||
      row.resolver !== admission.resolver ||
      row.attempt_sequence !== admission.attemptSequence ||
      row.redirect_sequence !== admission.redirectSequence ||
      row.continuation_authorization_id !==
        admission.continuationAuthorizationId ||
      (row.outcome === "request_started") !== (row.completed_at === null) ||
      (row.outcome === "request_started") !== (row.receipt_sha256 === null)
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot preflight resume binding is invalid",
      );
    }
    return {
      completedAt: row.completed_at?.toISOString() ?? null,
      outcome: row.outcome,
      receiptSha256: row.receipt_sha256,
      requestId: admission.requestId,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadCandidateSourceSnapshotIpnsIntentState(
  databaseUrl: string,
  inputValue: {
    domain: CandidateSourceSnapshotIpnsDomain;
    intentId: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotIpnsIntentStateRecord> {
  const input = z
    .strictObject({
      domain: ipnsDomainSchema,
      intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan } = await loadExactPlanRowForUpdate(transaction, input);
      const rows = await transaction<
        CandidateSourceSnapshotIpnsIntentStateRow[]
      >`
        SELECT intent.intent_id, intent.approval_id, intent.upload_closure_id,
               intent.resolver_policy, intent.cutover_position,
               intent.rollback_position, intent.domain, intent.bucket,
               intent.ipns_label, intent.ipns_network_key, intent.prior_cid,
               intent.target_cid, intent.intended_at,
               intent_state.state, intent_state.revision,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
               ) AS mutation_attempt_count,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
                    AND attempt.direction = 'update'
               ) AS update_attempt_count,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
                    AND attempt.direction = 'rollback'
               ) AS rollback_attempt_count
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.intent_id = ${input.intentId}
          AND intent.plan_id = ${plan.planId}
          AND intent.plan_sha256 = ${plan.planSha256}
          AND intent.domain = ${input.domain}
        FOR UPDATE OF intent_state
      `;
      const row = rows[0];
      if (!row) {
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS intent state is not durable",
        );
      }
      return ipnsIntentStateRecord(plan, row);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateSourceSnapshotIpnsRetryAuthorization(
  databaseUrl: string,
  planValue: CandidateSourceSnapshotDemoPlan,
  inputValue: {
    authorization:
      | CandidateSourceSnapshotIpnsReplayAuthorization
      | CandidateSourceSnapshotIpnsRollbackAuthorization;
    direction: "rollback" | "update";
  },
): Promise<
  | CandidateSourceSnapshotIpnsReplayAuthorization
  | CandidateSourceSnapshotIpnsRollbackAuthorization
> {
  const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
  const input = z
    .strictObject({
      authorization: z.strictObject({
        authorizationId: z.string().regex(/^snapshotdemoreplay_[a-f0-9]{32}$/),
        authorizationSha256: z.string().regex(/^[a-f0-9]{64}$/),
        authorizationStatement: z.string().min(1).max(8_192),
        authorizedAttempt: z.number().int().min(1).max(3),
        authorizedAt: z.string().datetime({ offset: true }),
        authorizerReference: z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/),
        domain: ipnsDomainSchema,
        intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
        planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
        planSha256: z.string().regex(/^[a-f0-9]{64}$/),
        priorCid: z
          .string()
          .regex(/^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$/),
        targetCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
      }),
      direction: z.enum(["rollback", "update"]),
    })
    .parse(inputValue);
  const authorization = input.authorization;
  const authorizedAt = new Date(authorization.authorizedAt).toISOString();
  const expectedStatement =
    renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
      authorization,
      input.direction,
    );
  if (
    authorization.authorizedAt !== authorizedAt ||
    authorization.authorizationStatement !== expectedStatement ||
    authorization.authorizationSha256 !== sha256(expectedStatement) ||
    authorization.planId !== plan.planId ||
    authorization.planSha256 !== plan.planSha256 ||
    (input.direction === "rollback" && authorization.domain !== "open_data")
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot retry authorization statement is not exact",
    );
  }
  const requestedCid =
    input.direction === "update"
      ? authorization.targetCid
      : authorization.priorCid;
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { row } = await loadExactPlanRowForUpdate(transaction, plan);
      if (row.state !== "executing") {
        throw new DurableConflictError(
          "Candidate source-snapshot retry authorization requires executing plan",
        );
      }
      const intents = await transaction<
        { prior_cid: string; target_cid: string }[]
      >`
        SELECT prior_cid, target_cid
        FROM oracle_candidate_source_snapshot_demo_ipns_intents
        WHERE intent_id = ${authorization.intentId}
          AND plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
          AND domain = ${authorization.domain}
        FOR UPDATE
      `;
      if (
        intents[0]?.prior_cid !== authorization.priorCid ||
        intents[0]?.target_cid !== authorization.targetCid
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot retry authorization lacks its exact intent",
        );
      }
      const existing = await transaction<
        {
          authorization_id: string;
          authorization_sha256: string;
          authorized_at: Date;
          authorizer_reference: string;
          direction: "rollback" | "update";
          domain: CandidateSourceSnapshotIpnsDomain;
          intent_id: string;
          plan_id: string;
          requested_cid: string;
        }[]
      >`
        SELECT authorization_id, intent_id, plan_id, domain, direction,
               requested_cid, authorization_sha256, authorizer_reference,
               authorized_at
        FROM oracle_candidate_source_snapshot_demo_replay_authorizations
        WHERE authorization_id = ${authorization.authorizationId}
           OR (intent_id = ${authorization.intentId}
               AND direction = ${input.direction}
               AND authorization_sha256 = ${authorization.authorizationSha256})
        FOR UPDATE
      `;
      if (existing.length > 0) {
        const replay = existing[0]!;
        if (
          existing.length !== 1 ||
          replay.authorization_id !== authorization.authorizationId ||
          replay.authorization_sha256 !== authorization.authorizationSha256 ||
          replay.authorized_at.toISOString() !== authorization.authorizedAt ||
          replay.authorizer_reference !== authorization.authorizerReference ||
          replay.direction !== input.direction ||
          replay.domain !== authorization.domain ||
          replay.intent_id !== authorization.intentId ||
          replay.plan_id !== plan.planId ||
          replay.requested_cid !== requestedCid
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot retry authorization replay conflicts",
          );
        }
        return authorization;
      }
      const attemptCounts = await transaction<{ attempt_count: number }[]>`
        SELECT count(*)::integer AS attempt_count
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts
        WHERE intent_id = ${authorization.intentId}
          AND direction = ${input.direction}
      `;
      if (
        authorization.authorizedAttempt !==
        (attemptCounts[0]?.attempt_count ?? 0) + 1
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot retry authorization attempt is not next",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_replay_authorizations (
          authorization_id, intent_id, plan_id, domain, direction,
          requested_cid, authorization_sha256, authorizer_reference,
          authorized_at
        ) VALUES (
          ${authorization.authorizationId}, ${authorization.intentId},
          ${plan.planId}, ${authorization.domain}, ${input.direction},
          ${requestedCid}, ${authorization.authorizationSha256},
          ${authorization.authorizerReference}, ${authorization.authorizedAt}
        )
      `;
      return authorization;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadCandidateSourceSnapshotIpnsIntents(
  databaseUrl: string,
  inputValue: {
    planId: string;
    planSha256: string;
  },
): Promise<readonly CandidateSourceSnapshotIpnsIntentStateRecord[]> {
  const input = z
    .strictObject({
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan } = await loadExactPlanRowForUpdate(transaction, input);
      const rows = await transaction<
        CandidateSourceSnapshotIpnsIntentStateRow[]
      >`
        SELECT intent.intent_id, intent.approval_id, intent.upload_closure_id,
               intent.resolver_policy, intent.cutover_position,
               intent.rollback_position, intent.domain, intent.bucket,
               intent.ipns_label, intent.ipns_network_key, intent.prior_cid,
               intent.target_cid, intent.intended_at,
               intent_state.state, intent_state.revision,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
               ) AS mutation_attempt_count,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
                    AND attempt.direction = 'update'
               ) AS update_attempt_count,
               (SELECT count(*)::text
                  FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
                  WHERE attempt.intent_id = intent.intent_id
                    AND attempt.direction = 'rollback'
               ) AS rollback_attempt_count
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.plan_id = ${plan.planId}
          AND intent.plan_sha256 = ${plan.planSha256}
        ORDER BY intent.cutover_position
        FOR UPDATE OF intent_state
      `;
      if (
        rows.length !== 2 ||
        rows[0]?.domain !== "open_data" ||
        rows[0]?.cutover_position !== 1 ||
        rows[1]?.domain !== "query_table" ||
        rows[1]?.cutover_position !== 2
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS intent set is incomplete",
        );
      }
      const records = rows.map((row) => ipnsIntentStateRecord(plan, row));
      if (
        records[0]?.approvalId !== records[1]?.approvalId ||
        records[0]?.uploadClosureId !== records[1]?.uploadClosureId
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS intent set conflicts",
        );
      }
      return records;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function attemptId(input: {
  object: CandidateSourceSnapshotUploadObject;
  operation: "inspect" | "upload";
  plan: CandidateSourceSnapshotDemoPlan;
  sequence: number;
}): string {
  return deterministicId(
    input.operation === "inspect"
      ? "snapshotdemoinspection"
      : "snapshotdemoattempt",
    [
      "1.0.0",
      input.plan.planId,
      input.object.domain,
      input.object.remoteObjectKey,
      input.operation,
      String(input.sequence),
    ],
  );
}

async function loadObjectForUpdate(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  object: CandidateSourceSnapshotUploadObject,
): Promise<ObjectRow> {
  const rows = await transaction<ObjectRow[]>`
    SELECT status, attempt_count, request_count, provider_cid,
           receipt_sha256, revision
    FROM oracle_candidate_source_snapshot_demo_objects
    WHERE plan_id = ${plan.planId}
      AND domain = ${object.domain}
      AND remote_object_key = ${object.remoteObjectKey}
      AND logical_object_key = ${object.logicalObjectKey}
      AND expected_sha256 = ${object.sha256}
      AND expected_cid = ${object.expectedCid}
      AND expected_bytes = ${object.byteSize}
    FOR UPDATE
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot object is not bound to the durable plan",
    );
  }
  return rows[0];
}

/** A terminal checkpoint may only complete the exact admitted upload attempt. */
async function assertUploadAttemptForUpdate(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  object: CandidateSourceSnapshotUploadObject,
  attempt: CandidateSourceSnapshotUploadAttempt,
): Promise<void> {
  if (attempt.operation !== "upload" || attempt.outcome !== "request_started") {
    throw new DurableInputError(
      "Candidate source-snapshot terminal checkpoint is not an admitted upload",
    );
  }
  const expectedAttemptId = attemptId({
    object,
    operation: "upload",
    plan,
    sequence: attempt.attemptSequence,
  });
  const expectedRequestId = requestId({
    object,
    operation: "upload",
    plan,
    sequence: attempt.attemptSequence,
  });
  if (
    attempt.attemptId !== expectedAttemptId ||
    attempt.requestId !== expectedRequestId
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot upload attempt identity is not deterministic",
    );
  }
  const rows = await transaction<{ attempt_id: string }[]>`
    SELECT attempt.attempt_id
    FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
    JOIN oracle_candidate_source_snapshot_demo_requests request
      ON request.request_id = attempt.request_id
    WHERE attempt.attempt_id = ${attempt.attemptId}
      AND attempt.request_id = ${attempt.requestId}
      AND attempt.plan_id = ${plan.planId}
      AND attempt.domain = ${object.domain}
      AND attempt.remote_object_key = ${object.remoteObjectKey}
      AND attempt.attempt_sequence = ${attempt.attemptSequence}
      AND attempt.outcome = 'request_started'
      AND request.plan_id = ${plan.planId}
      AND request.domain = ${object.domain}
      AND request.remote_object_key = ${object.remoteObjectKey}
      AND request.operation_class = 'class_a_mutation'
      AND request.operation_kind = 'put_object'
      AND request.outcome = 'request_started'
    FOR UPDATE OF attempt, request
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot upload attempt is not the exact admitted durable row",
    );
  }
}

async function assertRecoveryUploadAttemptForInspection(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  object: CandidateSourceSnapshotUploadObject,
  attempt: CandidateSourceSnapshotUploadAttempt,
): Promise<void> {
  if (
    attempt.operation !== "upload" ||
    !["connection_failure", "retryable_http_error", "timeout_unknown"].includes(
      attempt.outcome,
    )
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot inspection requires one ambiguous upload attempt",
    );
  }
  const expectedAttemptId = attemptId({
    object,
    operation: "upload",
    plan,
    sequence: attempt.attemptSequence,
  });
  const expectedRequestId = requestId({
    object,
    operation: "upload",
    plan,
    sequence: attempt.attemptSequence,
  });
  if (
    attempt.attemptId !== expectedAttemptId ||
    attempt.requestId !== expectedRequestId
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot recovery upload attempt identity is not deterministic",
    );
  }
  const rows = await transaction<{ attempt_id: string }[]>`
    SELECT attempt.attempt_id
    FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
    JOIN oracle_candidate_source_snapshot_demo_requests request
      ON request.request_id = attempt.request_id
    WHERE attempt.attempt_id = ${attempt.attemptId}
      AND attempt.request_id = ${attempt.requestId}
      AND attempt.plan_id = ${plan.planId}
      AND attempt.domain = ${object.domain}
      AND attempt.remote_object_key = ${object.remoteObjectKey}
      AND attempt.attempt_sequence = ${attempt.attemptSequence}
      AND attempt.outcome = ${attempt.outcome}
      AND request.plan_id = ${plan.planId}
      AND request.operation_class = 'class_a_mutation'
      AND request.operation_kind = 'put_object'
      AND request.outcome IN ('retryable_failure', 'timeout_unknown')
    FOR UPDATE OF attempt, request
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot inspection recovery attempt is not durable",
    );
  }
}

const uploadReceiptSchema = z.strictObject({
  providerCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  providerRequestIdHash: z
    .string()
    .regex(/^[a-f0-9]{64}$/)
    .nullable(),
  receiptSha256: z.string().regex(/^[a-f0-9]{64}$/),
  responseBytes: z.number().int().nonnegative(),
});

const inspectionResultSchema = z.discriminatedUnion("outcome", [
  z.strictObject({
    outcome: z.enum(["absent", "ambiguous"]),
    receiptSha256: z.string().regex(/^[a-f0-9]{64}$/),
  }),
  z.strictObject({
    observedBytes: z.number().int().min(0).max(536_870_912),
    observedCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
    observedSha256: z.string().regex(/^[a-f0-9]{64}$/),
    outcome: z.enum(["mismatch", "verified"]),
    receiptSha256: z.string().regex(/^[a-f0-9]{64}$/),
  }),
]);

async function assertInspectionAttemptForUpdate(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  object: CandidateSourceSnapshotUploadObject,
  attempt: CandidateSourceSnapshotUploadAttempt,
): Promise<void> {
  if (
    attempt.operation !== "inspect" ||
    attempt.outcome !== "request_started" ||
    attempt.recoveryUploadAttemptId === null
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot inspection result is not an admitted inspection",
    );
  }
  const expectedAttemptId = attemptId({
    object,
    operation: "inspect",
    plan,
    sequence: attempt.attemptSequence,
  });
  const expectedRequestId = requestId({
    object,
    operation: "inspect",
    plan,
    sequence: attempt.attemptSequence,
  });
  if (
    attempt.attemptId !== expectedAttemptId ||
    attempt.requestId !== expectedRequestId
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot inspection attempt identity is not deterministic",
    );
  }
  const rows = await transaction<{ inspection_id: string }[]>`
    SELECT inspection.inspection_id
    FROM oracle_candidate_source_snapshot_demo_inspection_attempts inspection
    JOIN oracle_candidate_source_snapshot_demo_requests request
      ON request.request_id = inspection.request_id
    WHERE inspection.inspection_id = ${attempt.attemptId}
      AND inspection.request_id = ${attempt.requestId}
      AND inspection.recovery_upload_attempt_id = ${attempt.recoveryUploadAttemptId}
      AND inspection.plan_id = ${plan.planId}
      AND inspection.domain = ${object.domain}
      AND inspection.remote_object_key = ${object.remoteObjectKey}
      AND inspection.inspection_sequence = ${attempt.attemptSequence}
      AND inspection.outcome = 'request_started'
      AND request.plan_id = ${plan.planId}
      AND request.operation_class = 'class_b_read'
      AND request.operation_kind = 'inspect_object'
      AND request.outcome = 'request_started'
    FOR UPDATE OF inspection, request
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot inspection is not the exact admitted durable row",
    );
  }
}

export function expectedCandidateSourceSnapshotUploadReceiptSha256(input: {
  attempt: CandidateSourceSnapshotUploadAttempt;
  object: CandidateSourceSnapshotUploadObject;
  providerCid: string;
  providerRequestIdHash: string | null;
  responseBytes: number;
}): string {
  return canonicalJsonSha256({
    attemptId: input.attempt.attemptId,
    domain: input.object.domain,
    logicalObjectKey: input.object.logicalObjectKey,
    providerCid: input.providerCid,
    providerRequestIdHash: input.providerRequestIdHash,
    remoteObjectKey: input.object.remoteObjectKey,
    responseBytes: input.responseBytes,
  });
}

async function assertExecutingPlan(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<void> {
  const rows = await transaction<{ plan_sha256: string; state: string }[]>`
    SELECT plan_sha256, state
    FROM oracle_candidate_source_snapshot_demo_plans
    WHERE plan_id = ${plan.planId}
    FOR UPDATE
  `;
  if (
    rows[0]?.plan_sha256 !== plan.planSha256 ||
    rows[0]?.state !== "executing"
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot upload requires the exact executing plan",
    );
  }
}

async function loadIpnsIntentForUpdate(
  transaction: postgres.TransactionSql,
  plan: CandidateSourceSnapshotDemoPlan,
  intentId: string,
  domain: CandidateSourceSnapshotIpnsDomain,
): Promise<IpnsIntentRow> {
  const rows = await transaction<IpnsIntentRow[]>`
    SELECT intent.intent_id, intent.plan_id, intent.domain, intent.prior_cid,
           intent.target_cid, intent_state.state
    FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
    JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
      ON intent_state.intent_id = intent.intent_id
    WHERE intent.intent_id = ${intentId}
      AND intent.plan_id = ${plan.planId}
      AND intent.plan_sha256 = ${plan.planSha256}
      AND intent.domain = ${domain}
    FOR UPDATE OF intent_state
  `;
  if (!rows[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot IPNS request is not bound to its immutable intent",
    );
  }
  return rows[0];
}

async function admitIpnsRemoteRequest(
  transaction: postgres.TransactionSql,
  input: {
    cycleSequence: number | null;
    domain: CandidateSourceSnapshotIpnsDomain;
    identityScope: string;
    intentId: string;
    operationKind: CandidateSourceSnapshotRemoteRequestAdmission["operationKind"];
    plan: CandidateSourceSnapshotDemoPlan;
    requestCategory: Extract<
      CandidateSourceSnapshotRequestCategory,
      "control_public_observation" | "names_mutation" | "recovery" | "rollback"
    >;
    resolver: CandidateSourceSnapshotIpnsResolver | null;
    sequence: number;
  },
): Promise<CandidateSourceSnapshotRemoteRequestAdmission> {
  await assertExecutingPlan(transaction, input.plan);
  await loadIpnsIntentForUpdate(
    transaction,
    input.plan,
    input.intentId,
    input.domain,
  );
  const id = remoteRequestId({
    cycleSequence: input.cycleSequence,
    identityScope: input.identityScope,
    intentId: input.intentId,
    operationKind: input.operationKind,
    planId: input.plan.planId,
    resolver: input.resolver,
    sequence: input.sequence,
  });
  const existing = await transaction<RemoteRequestRow[]>`
    SELECT request_id, intent_id, domain, operation_kind, request_category,
           cycle_sequence, resolver, outcome, receipt_sha256, completed_at
    FROM oracle_candidate_source_snapshot_demo_requests
    WHERE request_id = ${id}
    FOR UPDATE
  `;
  if (existing[0]) {
    const row = existing[0];
    if (
      row.intent_id !== input.intentId ||
      row.domain !== input.domain ||
      row.operation_kind !== input.operationKind ||
      row.request_category !== input.requestCategory ||
      row.cycle_sequence !== input.cycleSequence ||
      row.resolver !== input.resolver
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot remote request replay conflicts with durable identity",
      );
    }
    const accountingRows = await transaction<AccountingRow[]>`
      SELECT request_count, class_a_mutation_count, class_b_read_count,
             names_api_count, public_resolver_count, free_operation_count,
             request_cost_usd, revision
      FROM oracle_candidate_source_snapshot_demo_accounting
      WHERE plan_id = ${input.plan.planId}
    `;
    if (!accountingRows[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot accounting is missing",
      );
    }
    return {
      accounting: accounting(accountingRows[0]),
      alreadyRecorded: true,
      cycleSequence: row.cycle_sequence,
      domain: row.domain,
      intentId: row.intent_id,
      operationKind: row.operation_kind,
      outcome: row.outcome,
      requestId: row.request_id,
      requestCategory: row.request_category,
      resolver: row.resolver,
    };
  }

  const operationClass =
    input.operationKind === "public_resolve" ? "public_resolver" : "names_api";
  const rows = await transaction<AccountingRow[]>`
    SELECT request_count, class_a_mutation_count, class_b_read_count,
           names_api_count, public_resolver_count, free_operation_count,
           request_cost_usd, revision
    FROM oracle_candidate_source_snapshot_demo_accounting
    WHERE plan_id = ${input.plan.planId}
    FOR UPDATE
  `;
  const current = rows[0];
  if (!current) {
    throw new DurableConflictError(
      "Candidate source-snapshot accounting is missing",
    );
  }
  const requestCostUsd = 0.0045 / 1_000;
  const nextAccounting = accounting({
    ...current,
    names_api_count:
      current.names_api_count + (operationClass === "names_api" ? 1 : 0),
    public_resolver_count:
      current.public_resolver_count +
      (operationClass === "public_resolver" ? 1 : 0),
    request_cost_usd: Number(current.request_cost_usd) + requestCostUsd,
    request_count: current.request_count + 1,
    revision: current.revision + 1,
  });
  const allowedRequestCost = input.plan.costEnvelope.requestUsd.maximumAttempts;
  if (
    nextAccounting.requestCount >
      input.plan.requestEnvelope.maximumTotalRequests ||
    nextAccounting.requestCostUsd > allowedRequestCost
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot remote request or cost allowance is exhausted",
    );
  }
  const accountingUpdate = await transaction<{ plan_id: string }[]>`
    UPDATE oracle_candidate_source_snapshot_demo_accounting
    SET request_count = ${nextAccounting.requestCount},
        names_api_count = ${nextAccounting.namesApiCount},
        public_resolver_count = ${nextAccounting.publicResolverCount},
        request_cost_usd = ${nextAccounting.requestCostUsd},
        revision = revision + 1, updated_at = now()
    WHERE plan_id = ${input.plan.planId} AND revision = ${current.revision}
    RETURNING plan_id
  `;
  if (!accountingUpdate[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot remote request lost its accounting admission",
    );
  }
  await incrementRequestCategory(transaction, {
    category: input.requestCategory,
    plan: input.plan,
    requestCostUsd,
  });
  const logicalId = logicalRequestId({
    category: input.requestCategory,
    domain: input.domain,
    identity: `${input.intentId}:${input.identityScope}:${input.cycleSequence ?? 0}:${input.resolver ?? "none"}`,
    operationKind: input.operationKind,
    planId: input.plan.planId,
  });
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_requests (
      request_id, plan_id, operation_class, operation_kind, intent_id, domain,
      remote_object_key, cycle_sequence, resolver, request_cost_usd, outcome,
      request_category, logical_request_id, attempt_sequence, redirect_sequence
    ) VALUES (
      ${id}, ${input.plan.planId}, ${operationClass}, ${input.operationKind},
      ${input.intentId}, ${input.domain}, NULL, ${input.cycleSequence},
      ${input.resolver}, ${requestCostUsd}, 'request_started',
      ${input.requestCategory}, ${logicalId},
      ${input.operationKind === "names_update" ? input.sequence : 1}, 0
    )
  `;
  return {
    accounting: nextAccounting,
    alreadyRecorded: false,
    cycleSequence: input.cycleSequence,
    domain: input.domain,
    intentId: input.intentId,
    operationKind: input.operationKind,
    outcome: "request_started",
    requestId: id,
    requestCategory: input.requestCategory,
    resolver: input.resolver,
  };
}

async function admitRequest(
  transaction: postgres.TransactionSql,
  input: {
    continuation?: CandidateSourceSnapshotUploadJournalLeaseBinding;
    object: CandidateSourceSnapshotUploadObject;
    operation: "inspect" | "upload";
    plan: CandidateSourceSnapshotDemoPlan;
    recoveryAttempt?: CandidateSourceSnapshotUploadAttempt;
    sequence: number;
  },
): Promise<{
  accounting: CandidateSourceSnapshotPlanAccounting;
  alreadyRecorded: boolean;
  attempt: CandidateSourceSnapshotUploadAttempt;
  replayedResult: CandidateSourceSnapshotInspectionResult | null;
}> {
  await assertExecutingPlan(transaction, input.plan);
  await loadObjectForUpdate(transaction, input.plan, input.object);
  const id = requestId(input);
  const attemptIdValue = attemptId(input);
  if (input.operation === "upload") {
    const existing = await transaction<
      {
        attempt_id: string;
        attempt_sequence: number;
        executor_lease_epoch: number | null;
        executor_lease_id: string | null;
        outcome: CandidateSourceSnapshotUploadAttempt["outcome"];
        request_id: string;
        started_at: Date;
        upload_continuation_authorization_id: string | null;
        upload_resume_authorization_id: string | null;
      }[]
    >`
      SELECT attempt.attempt_id, attempt.request_id, attempt.attempt_sequence,
             attempt.outcome, attempt.started_at,
             request.upload_continuation_authorization_id,
             request.upload_resume_authorization_id,
             request.executor_lease_id, request.executor_lease_epoch
      FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
      JOIN oracle_candidate_source_snapshot_demo_requests request
        ON request.request_id = attempt.request_id
      WHERE attempt.attempt_id = ${attemptIdValue}
        AND attempt.plan_id = ${input.plan.planId}
        AND attempt.domain = ${input.object.domain}
        AND attempt.remote_object_key = ${input.object.remoteObjectKey}
        AND attempt.attempt_sequence = ${input.sequence}
      FOR UPDATE
    `;
    if (existing[0]) {
      const row = existing[0];
      if (
        row.request_id !== id ||
        row.upload_continuation_authorization_id !==
          (input.continuation?.authorizationId ?? null) ||
        row.upload_resume_authorization_id !==
          (input.continuation?.resumeAuthorizationId ?? null) ||
        row.executor_lease_id !== (input.continuation?.leaseId ?? null) ||
        row.executor_lease_epoch !==
          (input.continuation?.leaseGeneration ?? null)
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot upload admission replay conflicts with durable identity",
        );
      }
      const accountingRows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${input.plan.planId}
      `;
      if (!accountingRows[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot accounting is missing",
        );
      }
      return {
        accounting: accounting(accountingRows[0]),
        alreadyRecorded: true,
        attempt: {
          attemptId: row.attempt_id,
          attemptSequence: row.attempt_sequence,
          operation: "upload",
          outcome: row.outcome,
          recoveryUploadAttemptId: null,
          requestId: row.request_id,
          startedAt: row.started_at.toISOString(),
        },
        replayedResult: null,
      };
    }
  }
  if (input.operation === "inspect") {
    if (!input.recoveryAttempt) {
      throw new DurableInputError(
        "Candidate source-snapshot inspection recovery attempt is required",
      );
    }
    await assertRecoveryUploadAttemptForInspection(
      transaction,
      input.plan,
      input.object,
      input.recoveryAttempt,
    );
    if (
      !input.continuation?.resumeAuthorizationId &&
      input.sequence !== input.recoveryAttempt.attemptSequence
    ) {
      throw new DurableInputError(
        "Candidate source-snapshot inspection sequence must match its upload attempt",
      );
    }
    const existing = await transaction<
      {
        inspection_id: string;
        outcome:
          | CandidateSourceSnapshotInspectionResult["outcome"]
          | "request_started";
        observed_bytes: string | number | null;
        observed_cid: string | null;
        observed_sha256: string | null;
        receipt_sha256: string | null;
        recovery_upload_attempt_id: string;
        request_id: string;
        upload_continuation_authorization_id: string | null;
        upload_resume_authorization_id: string | null;
        executor_lease_id: string | null;
        executor_lease_epoch: number | null;
      }[]
    >`
      SELECT attempt.inspection_id, attempt.request_id,
             attempt.recovery_upload_attempt_id, attempt.outcome,
             attempt.observed_cid, attempt.observed_sha256,
             attempt.observed_bytes, attempt.receipt_sha256,
             request.upload_continuation_authorization_id,
             request.upload_resume_authorization_id,
             request.executor_lease_id, request.executor_lease_epoch
      FROM oracle_candidate_source_snapshot_demo_inspection_attempts attempt
      JOIN oracle_candidate_source_snapshot_demo_requests request
        ON request.request_id = attempt.request_id
      WHERE attempt.inspection_id = ${attemptIdValue}
        AND attempt.plan_id = ${input.plan.planId}
        AND attempt.domain = ${input.object.domain}
        AND attempt.remote_object_key = ${input.object.remoteObjectKey}
        AND attempt.inspection_sequence = ${input.sequence}
      FOR UPDATE
    `;
    if (existing[0]) {
      const row = existing[0];
      if (
        row.request_id !== id ||
        row.recovery_upload_attempt_id !== input.recoveryAttempt.attemptId ||
        row.upload_continuation_authorization_id !==
          (input.continuation?.authorizationId ?? null) ||
        row.upload_resume_authorization_id !==
          (input.continuation?.resumeAuthorizationId ?? null) ||
        row.executor_lease_id !== (input.continuation?.leaseId ?? null) ||
        row.executor_lease_epoch !==
          (input.continuation?.leaseGeneration ?? null)
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot inspection admission replay conflicts with durable identity",
        );
      }
      const accountingRows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${input.plan.planId}
      `;
      if (!accountingRows[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot accounting is missing",
        );
      }
      const replayedResult: CandidateSourceSnapshotInspectionResult | null =
        row.outcome === "request_started"
          ? null
          : row.outcome === "absent" || row.outcome === "ambiguous"
            ? {
                outcome: row.outcome,
                receiptSha256: row.receipt_sha256!,
              }
            : {
                observedBytes: Number(row.observed_bytes),
                observedCid: row.observed_cid!,
                observedSha256: row.observed_sha256!,
                outcome: row.outcome,
                receiptSha256: row.receipt_sha256!,
              };
      return {
        accounting: accounting(accountingRows[0]),
        alreadyRecorded: true,
        attempt: {
          attemptId: row.inspection_id,
          attemptSequence: input.sequence,
          operation: "inspect",
          outcome:
            row.outcome === "mismatch" ? "inspection_mismatch" : row.outcome,
          recoveryUploadAttemptId: row.recovery_upload_attempt_id,
          requestId: row.request_id,
        },
        replayedResult,
      };
    }
  }
  const rows = await transaction<AccountingRow[]>`
    SELECT request_count, class_a_mutation_count, class_b_read_count,
           names_api_count, public_resolver_count, free_operation_count,
           request_cost_usd, revision
    FROM oracle_candidate_source_snapshot_demo_accounting
    WHERE plan_id = ${input.plan.planId}
    FOR UPDATE
  `;
  const current = rows[0];
  if (!current) {
    throw new DurableConflictError(
      "Candidate source-snapshot accounting is missing",
    );
  }
  const nextClassA =
    current.class_a_mutation_count + (input.operation === "upload" ? 1 : 0);
  const nextClassB =
    current.class_b_read_count + (input.operation === "inspect" ? 1 : 0);
  const nextRequestCount = current.request_count + 1;
  const requestCostUsd = 0.0045 / 1_000;
  const nextRequestCostUsd = Number(current.request_cost_usd) + requestCostUsd;
  const requestCategory =
    input.operation === "upload"
      ? "upload_provider_cid"
      : "ambiguous_upload_inspection";
  if (
    nextRequestCount > input.plan.requestEnvelope.maximumTotalRequests ||
    nextRequestCostUsd > input.plan.costEnvelope.requestUsd.maximumAttempts
  ) {
    throw new DurableConflictError(
      "Candidate source-snapshot request or cost allowance is exhausted",
    );
  }
  const nextAccounting = accounting({
    ...current,
    class_a_mutation_count: nextClassA,
    class_b_read_count: nextClassB,
    request_cost_usd: nextRequestCostUsd,
    request_count: nextRequestCount,
    revision: current.revision + 1,
  });
  const accountingUpdates = await transaction<{ plan_id: string }[]>`
    UPDATE oracle_candidate_source_snapshot_demo_accounting
    SET request_count = ${nextAccounting.requestCount},
        class_a_mutation_count = ${nextAccounting.classAMutationCount},
        class_b_read_count = ${nextAccounting.classBReadCount},
        request_cost_usd = ${nextAccounting.requestCostUsd},
        revision = revision + 1,
        updated_at = now()
    WHERE plan_id = ${input.plan.planId}
      AND revision = ${current.revision}
    RETURNING plan_id
  `;
  if (!accountingUpdates[0]) {
    throw new DurableConflictError(
      "Candidate source-snapshot request lost its accounting admission",
    );
  }
  await incrementRequestCategory(transaction, {
    category: requestCategory,
    plan: input.plan,
    requestCostUsd,
  });
  const operationClass =
    input.operation === "upload" ? "class_a_mutation" : "class_b_read";
  const operationKind =
    input.operation === "upload" ? "put_object" : "inspect_object";
  const logicalId = logicalRequestId({
    category: requestCategory,
    domain: input.object.domain,
    identity: input.object.remoteObjectKey,
    operationKind,
    planId: input.plan.planId,
  });
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_requests (
      request_id, plan_id, operation_class, operation_kind, domain,
      remote_object_key, request_cost_usd, outcome, request_category,
      logical_request_id, attempt_sequence, redirect_sequence,
      upload_continuation_authorization_id, executor_lease_id,
      executor_lease_epoch, upload_resume_authorization_id
    ) VALUES (
      ${id}, ${input.plan.planId}, ${operationClass}, ${operationKind},
      ${input.object.domain}, ${input.object.remoteObjectKey},
      ${requestCostUsd}, 'request_started', ${requestCategory}, ${logicalId},
      ${input.sequence}, 0, ${input.continuation?.authorizationId ?? null},
      ${input.continuation?.leaseId ?? null},
      ${input.continuation?.leaseGeneration ?? null},
      ${input.continuation?.resumeAuthorizationId ?? null}
    )
  `;
  const attempt: CandidateSourceSnapshotUploadAttempt = {
    attemptId: attemptIdValue,
    attemptSequence: input.sequence,
    operation: input.operation,
    outcome: "request_started",
    recoveryUploadAttemptId:
      input.operation === "inspect" ? input.recoveryAttempt!.attemptId : null,
    requestId: id,
  };
  if (input.operation === "upload") {
    const insertedAttempts = await transaction<{ started_at: Date }[]>`
      INSERT INTO oracle_candidate_source_snapshot_demo_upload_attempts (
        attempt_id, request_id, plan_id, domain, remote_object_key,
        attempt_sequence, outcome, request_count
      ) VALUES (
        ${attempt.attemptId}, ${attempt.requestId}, ${input.plan.planId},
        ${input.object.domain}, ${input.object.remoteObjectKey},
        ${input.sequence}, 'request_started', 1
      )
      RETURNING started_at
    `;
    if (!insertedAttempts[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot upload attempt was not persisted",
      );
    }
    attempt.startedAt = insertedAttempts[0].started_at.toISOString();
    const objectUpdates = await transaction<{ remote_object_key: string }[]>`
      UPDATE oracle_candidate_source_snapshot_demo_objects
      SET status = 'admitted', attempt_count = attempt_count + 1,
          request_count = request_count + 1, revision = revision + 1,
          updated_at = now()
      WHERE plan_id = ${input.plan.planId}
        AND domain = ${input.object.domain}
        AND remote_object_key = ${input.object.remoteObjectKey}
      RETURNING remote_object_key
    `;
    if (!objectUpdates[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot upload admission lost its object binding",
      );
    }
  } else {
    await transaction`
      INSERT INTO oracle_candidate_source_snapshot_demo_inspection_attempts (
        inspection_id, request_id, recovery_upload_attempt_id, plan_id,
        domain, remote_object_key, inspection_sequence, outcome
      ) VALUES (
        ${attempt.attemptId}, ${attempt.requestId},
        ${attempt.recoveryUploadAttemptId}, ${input.plan.planId},
        ${input.object.domain}, ${input.object.remoteObjectKey},
        ${input.sequence}, 'request_started'
      )
    `;
    const objectUpdates = await transaction<{ remote_object_key: string }[]>`
      UPDATE oracle_candidate_source_snapshot_demo_objects
      SET request_count = request_count + 1, revision = revision + 1,
          updated_at = now()
      WHERE plan_id = ${input.plan.planId}
        AND domain = ${input.object.domain}
        AND remote_object_key = ${input.object.remoteObjectKey}
      RETURNING remote_object_key
    `;
    if (!objectUpdates[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot inspection admission lost its object binding",
      );
    }
  }
  return {
    accounting: nextAccounting,
    alreadyRecorded: false,
    attempt,
    replayedResult: null,
  };
}

/** PostgreSQL-backed resumable journal for the future explicitly enabled run. */
export class PostgresCandidateSourceSnapshotUploadJournal implements CandidateSourceSnapshotUploadJournal {
  readonly #continuation:
    CandidateSourceSnapshotUploadJournalLeaseBinding | undefined;
  #transactionGate: Promise<void> = Promise.resolve();

  constructor(
    private readonly databaseUrl: string,
    continuation?: CandidateSourceSnapshotUploadJournalLeaseBinding,
  ) {
    if (!continuation) {
      this.#continuation = undefined;
      return;
    }
    const parsed = uploadJournalLeaseBindingSchema.parse(continuation);
    this.#continuation = {
      authorizationId: parsed.authorizationId,
      leaseGeneration: parsed.leaseGeneration,
      leaseId: parsed.leaseId,
      ...(parsed.resumeAuthorizationId
        ? { resumeAuthorizationId: parsed.resumeAuthorizationId }
        : {}),
    };
  }

  private async transaction<T>(
    operation: (transaction: postgres.TransactionSql) => Promise<T>,
  ): Promise<T> {
    const pending = this.#transactionGate.then(
      async () => await this.transactionOnce(operation),
    );
    this.#transactionGate = pending.then(
      () => undefined,
      () => undefined,
    );
    return await pending;
  }

  private async transactionOnce<T>(
    operation: (transaction: postgres.TransactionSql) => Promise<T>,
  ): Promise<T> {
    if (this.#continuation) {
      const continuation = this.#continuation;
      return await runCandidateSourceSnapshotFencedPostgresOperation({
        dependencies: {
          createSession: () => {
            const sql = postgres(this.databaseUrl, { max: 1 });
            return {
              close: async () => await sql.end({ timeout: 5 }),
              probe: async () => {
                await sql`SELECT 1`;
              },
              transaction: async <Result>(
                callback: (
                  transaction: postgres.TransactionSql,
                ) => Promise<Result>,
              ) =>
                (await sql.begin(
                  async (transaction) => await callback(transaction),
                )) as Result,
            };
          },
          sleep: async (delayMs) => {
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          },
        },
        operation,
        revalidateGeneration: async (transaction) => {
          await transaction`SELECT oracle_css_assert_active_executor_lease(
            ${continuation.leaseId}, ${continuation.leaseGeneration},
            ${continuation.resumeAuthorizationId ?? continuation.authorizationId}
          )`;
        },
      });
    }
    const sql = postgres(this.databaseUrl, { max: 1 });
    try {
      return (await sql.begin(
        async (transaction) => await operation(transaction),
      )) as T;
    } finally {
      await sql.end({ timeout: 5 });
    }
  }

  async startResolutionRequest(
    planValue: CandidateSourceSnapshotDemoPlan,
    inputValue: {
      cycleSequence: number;
      domain: CandidateSourceSnapshotIpnsDomain;
      intentId: string;
      requestCategory?: "control_public_observation" | "recovery" | "rollback";
      resolver: CandidateSourceSnapshotIpnsResolver;
    },
  ): Promise<CandidateSourceSnapshotRemoteRequestAdmission> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const input = z
      .strictObject({
        cycleSequence: z.number().int().min(1).max(32),
        domain: ipnsDomainSchema,
        intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
        requestCategory: z
          .enum(["control_public_observation", "recovery", "rollback"])
          .default("control_public_observation"),
        resolver: ipnsResolverSchema,
      })
      .parse(inputValue);
    const operationKind =
      input.resolver === "filebase_control" ? "names_read" : "public_resolve";
    return await this.transaction(
      async (transaction) =>
        await admitIpnsRemoteRequest(transaction, {
          ...input,
          identityScope: "resolution",
          operationKind,
          plan,
          requestCategory: input.requestCategory,
          sequence: input.cycleSequence,
        }),
    );
  }

  async recordResolutionObservation(
    planValue: CandidateSourceSnapshotDemoPlan,
    requestValue: CandidateSourceSnapshotRemoteRequestAdmission,
    observationValue: {
      classification:
        "prior" | "target" | "split" | "unavailable" | "unexpected_cid";
      evidenceSha256: string;
      observedAt: string;
      observedCid: string | null;
      requestOutcome:
        | "succeeded"
        | "ambiguous"
        | "retryable_failure"
        | "timeout_unknown"
        | "terminal_failure";
    },
  ): Promise<void> {
    void planValue;
    void requestValue;
    void observationValue;
    throw new DurableInputError(
      "Candidate source-snapshot resolver observations must be recorded as one atomic cycle",
    );
  }

  async recordResolutionCycle(
    planValue: CandidateSourceSnapshotDemoPlan,
    inputValues: readonly {
      observation: {
        classification:
          "prior" | "target" | "split" | "unavailable" | "unexpected_cid";
        evidenceSha256: string;
        observedAt: string;
        observedCid: string | null;
        requestOutcome:
          | "succeeded"
          | "ambiguous"
          | "retryable_failure"
          | "timeout_unknown"
          | "terminal_failure";
      };
      request: CandidateSourceSnapshotRemoteRequestAdmission;
    }[],
  ): Promise<void> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    if (inputValues.length !== 3) {
      throw new DurableInputError(
        "Candidate source-snapshot resolution cycle requires exactly three resolver observations",
      );
    }
    const parsed = inputValues.map(({ observation, request }) => {
      const parsedRequest = z
        .strictObject({
          accounting: z.custom<CandidateSourceSnapshotPlanAccounting>(),
          alreadyRecorded: z.boolean(),
          cycleSequence: z.number().int().min(1).max(32),
          domain: ipnsDomainSchema,
          intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
          operationKind: z.enum(["names_read", "public_resolve"]),
          outcome: z.enum([
            "request_started",
            "succeeded",
            "ambiguous",
            "retryable_failure",
            "timeout_unknown",
            "terminal_failure",
          ]),
          requestId: z.string().regex(/^snapshotdemorequest_[a-f0-9]{32}$/),
          requestCategory: requestCategorySchema,
          resolver: z.enum([
            "filebase_control",
            "filebase_gateway",
            "delegated_ipfs",
          ]),
        })
        .parse(request);
      const parsedObservation = z
        .strictObject({
          classification: ipnsObservationClassificationSchema,
          evidenceSha256: z.string().regex(/^[a-f0-9]{64}$/),
          observedAt: z.string().datetime({ offset: true }),
          observedCid: z
            .string()
            .regex(/^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$/)
            .nullable(),
          requestOutcome: ipnsRequestOutcomeSchema,
        })
        .parse(observation);
      const validOutcome =
        (["prior", "target", "unexpected_cid"].includes(
          parsedObservation.classification,
        ) &&
          parsedObservation.requestOutcome === "succeeded") ||
        (parsedObservation.classification === "split" &&
          parsedObservation.requestOutcome === "ambiguous") ||
        (parsedObservation.classification === "unavailable" &&
          ["retryable_failure", "timeout_unknown", "terminal_failure"].includes(
            parsedObservation.requestOutcome,
          ));
      if (!validOutcome) {
        throw new DurableInputError(
          "Candidate source-snapshot observation outcome is inconsistent",
        );
      }
      return { observation: parsedObservation, request: parsedRequest };
    });
    const first = parsed[0]!.request;
    const resolvers = new Set(parsed.map(({ request }) => request.resolver));
    if (
      resolvers.size !== 3 ||
      !["filebase_control", "filebase_gateway", "delegated_ipfs"].every(
        (resolver) => resolvers.has(resolver as typeof first.resolver),
      ) ||
      parsed.some(
        ({ request }) =>
          request.intentId !== first.intentId ||
          request.domain !== first.domain ||
          request.cycleSequence !== first.cycleSequence ||
          request.requestCategory !== first.requestCategory,
      )
    ) {
      throw new DurableInputError(
        "Candidate source-snapshot resolution cycle identity is inconsistent",
      );
    }
    await this.transaction(async (transaction) => {
      for (const value of parsed) {
        await this.recordResolutionObservationInTransaction(
          transaction,
          plan,
          value.request,
          value.observation,
        );
      }
    });
  }

  private async recordResolutionObservationInTransaction(
    transaction: postgres.TransactionSql,
    plan: CandidateSourceSnapshotDemoPlan,
    request: CandidateSourceSnapshotRemoteRequestAdmission & {
      cycleSequence: number;
      resolver: CandidateSourceSnapshotIpnsResolver;
    },
    observation: {
      classification:
        "prior" | "target" | "split" | "unavailable" | "unexpected_cid";
      evidenceSha256: string;
      observedAt: string;
      observedCid: string | null;
      requestOutcome:
        | "succeeded"
        | "ambiguous"
        | "retryable_failure"
        | "timeout_unknown"
        | "terminal_failure";
    },
  ): Promise<void> {
    await assertExecutingPlan(transaction, plan);
    const intent = await loadIpnsIntentForUpdate(
      transaction,
      plan,
      request.intentId,
      request.domain,
    );
    if (
      (observation.classification === "prior" &&
        observation.observedCid !== intent.prior_cid) ||
      (observation.classification === "target" &&
        observation.observedCid !== intent.target_cid) ||
      (["split", "unavailable"].includes(observation.classification) &&
        observation.observedCid !== null) ||
      (observation.classification === "unexpected_cid" &&
        (observation.observedCid === null ||
          [intent.prior_cid, intent.target_cid].includes(
            observation.observedCid,
          )))
    ) {
      throw new DurableInputError(
        "Candidate source-snapshot observation is not bound to the intent",
      );
    }
    const id = ipnsObservationId({
      cycleSequence: request.cycleSequence,
      intentId: request.intentId,
      resolver: request.resolver,
    });
    const existing = await transaction<
      {
        classification: string;
        cycle_sequence: number;
        evidence_sha256: string;
        intent_id: string;
        observed_at: Date;
        observed_cid: string | null;
        request_id: string;
        resolver: string;
      }[]
    >`
        SELECT observation_id, request_id, intent_id, cycle_sequence,
               resolver, classification, observed_cid, evidence_sha256,
               observed_at
        FROM oracle_candidate_source_snapshot_demo_ipns_observations
        WHERE observation_id = ${id}
      `;
    if (existing[0]) {
      const row = existing[0];
      if (
        row.request_id !== request.requestId ||
        row.intent_id !== request.intentId ||
        row.cycle_sequence !== request.cycleSequence ||
        row.resolver !== request.resolver ||
        row.classification !== observation.classification ||
        row.observed_cid !== observation.observedCid ||
        row.evidence_sha256 !== observation.evidenceSha256 ||
        row.observed_at.toISOString() !== observation.observedAt
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot observation replay conflicts with durable evidence",
        );
      }
      return;
    }
    const requestRows = await transaction<RemoteRequestRow[]>`
        SELECT request_id, intent_id, domain, operation_kind, request_category,
               cycle_sequence, resolver, outcome, receipt_sha256, completed_at
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE request_id = ${request.requestId}
          AND plan_id = ${plan.planId}
          AND intent_id = ${request.intentId}
          AND domain = ${request.domain}
          AND cycle_sequence = ${request.cycleSequence}
          AND resolver = ${request.resolver}
          AND request_category = ${request.requestCategory}
        FOR UPDATE
      `;
    if (!requestRows[0] || requestRows[0].outcome !== "request_started") {
      throw new DurableConflictError(
        "Candidate source-snapshot observation lacks its admitted request",
      );
    }
    const completed = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${observation.requestOutcome},
            receipt_sha256 = ${observation.evidenceSha256},
            completed_at = ${observation.observedAt}
        WHERE request_id = ${request.requestId} AND outcome = 'request_started'
        RETURNING request_id
      `;
    if (!completed[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot observation lost its admitted request",
      );
    }
    await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_ipns_observations (
          observation_id, request_id, intent_id, cycle_sequence, resolver,
          classification, observed_cid, evidence_sha256, observed_at
        ) VALUES (
          ${id}, ${request.requestId}, ${request.intentId},
          ${request.cycleSequence}, ${request.resolver},
          ${observation.classification}, ${observation.observedCid},
          ${observation.evidenceSha256}, ${observation.observedAt}
        )
    `;
  }

  async startIpnsMutationAttempt(
    planValue: CandidateSourceSnapshotDemoPlan,
    inputValue: {
      attemptSequence: number;
      direction: "rollback" | "update";
      domain: CandidateSourceSnapshotIpnsDomain;
      intentId: string;
      replayAuthorizationSha256: string | null;
    },
  ): Promise<CandidateSourceSnapshotIpnsAttemptAdmission> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const input = z
      .strictObject({
        attemptSequence: z.number().int().min(1).max(3),
        direction: z.enum(["rollback", "update"]),
        domain: ipnsDomainSchema,
        intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
        replayAuthorizationSha256: z
          .string()
          .regex(/^[a-f0-9]{64}$/)
          .nullable(),
      })
      .parse(inputValue);
    return await this.transaction(async (transaction) => {
      const intent = await loadIpnsIntentForUpdate(
        transaction,
        plan,
        input.intentId,
        input.domain,
      );
      const request = await admitIpnsRemoteRequest(transaction, {
        cycleSequence: null,
        domain: input.domain,
        identityScope: input.direction,
        intentId: input.intentId,
        operationKind: "names_update",
        plan,
        requestCategory:
          input.direction === "rollback"
            ? "rollback"
            : input.attemptSequence === 1
              ? "names_mutation"
              : "recovery",
        resolver: null,
        sequence: input.attemptSequence,
      });
      const requestedCid =
        input.direction === "update" ? intent.target_cid : intent.prior_cid;
      const attemptIdValue = ipnsAttemptId({
        direction: input.direction,
        intentId: input.intentId,
        planId: plan.planId,
        sequence: input.attemptSequence,
      });
      const existing = await transaction<
        {
          attempt_id: string;
          attempt_sequence: number;
          direction: "rollback" | "update";
          replay_authorization_sha256: string | null;
          request_id: string;
          requested_cid: string;
        }[]
      >`
        SELECT attempt_id, request_id, direction, attempt_sequence,
               requested_cid, replay_authorization_sha256
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts
        WHERE attempt_id = ${attemptIdValue}
      `;
      if (existing[0]) {
        const row = existing[0];
        if (
          row.request_id !== request.requestId ||
          row.direction !== input.direction ||
          row.attempt_sequence !== input.attemptSequence ||
          row.requested_cid !== requestedCid ||
          row.replay_authorization_sha256 !== input.replayAuthorizationSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot IPNS attempt replay conflicts with durable identity",
          );
        }
      } else {
        await transaction`
          INSERT INTO oracle_candidate_source_snapshot_demo_ipns_attempts (
            attempt_id, request_id, intent_id, plan_id, domain, direction,
            attempt_sequence, requested_cid, replay_authorization_sha256,
            outcome
          ) VALUES (
            ${attemptIdValue}, ${request.requestId}, ${input.intentId},
            ${plan.planId}, ${input.domain}, ${input.direction},
            ${input.attemptSequence}, ${requestedCid},
            ${input.replayAuthorizationSha256}, 'request_started'
          )
        `;
      }
      return {
        attemptId: attemptIdValue,
        attemptSequence: input.attemptSequence,
        direction: input.direction,
        request,
        requestedCid,
      };
    });
  }

  async closeInterruptedIpnsMutationAttempt(
    planValue: CandidateSourceSnapshotDemoPlan,
    inputValue: {
      direction: "rollback" | "update";
      domain: CandidateSourceSnapshotIpnsDomain;
      intentId: string;
    },
  ): Promise<CandidateSourceSnapshotInterruptedIpnsAttempt | null> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const input = z
      .strictObject({
        direction: z.enum(["rollback", "update"]),
        domain: ipnsDomainSchema,
        intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
      })
      .parse(inputValue);
    return await this.transaction(async (transaction) => {
      await assertExecutingPlan(transaction, plan);
      await loadIpnsIntentForUpdate(
        transaction,
        plan,
        input.intentId,
        input.domain,
      );
      const rows = await transaction<
        {
          attempt_id: string;
          attempt_sequence: number;
          direction: "rollback" | "update";
          request_category: CandidateSourceSnapshotRequestCategory;
          request_id: string;
          requested_cid: string;
        }[]
      >`
        SELECT attempt.attempt_id, attempt.attempt_sequence, attempt.direction,
               attempt.requested_cid, attempt.request_id,
               request.request_category
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
        JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.request_id = attempt.request_id
        WHERE attempt.plan_id = ${plan.planId}
          AND attempt.intent_id = ${input.intentId}
          AND attempt.domain = ${input.domain}
          AND attempt.direction = ${input.direction}
          AND attempt.outcome = 'request_started'
          AND request.outcome = 'request_started'
        ORDER BY attempt.attempt_sequence
        FOR UPDATE OF attempt, request
      `;
      if (rows.length === 0) return null;
      if (rows.length !== 1) {
        throw new DurableConflictError(
          "Candidate source-snapshot recovery found multiple interrupted IPNS attempts",
        );
      }
      const row = rows[0]!;
      const receiptSha256 = canonicalJsonSha256({
        attemptId: row.attempt_id,
        direction: row.direction,
        domain: input.domain,
        intentId: input.intentId,
        outcome: "timeout_unknown",
        planId: plan.planId,
        requestId: row.request_id,
        requestedCid: row.requested_cid,
        schemaVersion: "candidate-source-snapshot-interrupted-ipns-v1",
      });
      const attempts = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_attempts
        SET outcome = 'timeout_unknown', receipt_sha256 = ${receiptSha256},
            completed_at = now()
        WHERE attempt_id = ${row.attempt_id}
          AND request_id = ${row.request_id}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const requests = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = 'timeout_unknown', receipt_sha256 = ${receiptSha256},
            completed_at = now()
        WHERE request_id = ${row.request_id}
          AND outcome = 'request_started'
        RETURNING request_id
      `;
      if (!attempts[0] || !requests[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot interrupted IPNS admission lost its exact request",
        );
      }
      const accountingRows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
      `;
      if (!accountingRows[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot accounting is missing",
        );
      }
      return {
        admission: {
          attemptId: row.attempt_id,
          attemptSequence: row.attempt_sequence,
          direction: row.direction,
          request: {
            accounting: accounting(accountingRows[0]),
            alreadyRecorded: true,
            cycleSequence: null,
            domain: input.domain,
            intentId: input.intentId,
            operationKind: "names_update",
            outcome: "timeout_unknown",
            requestCategory: row.request_category,
            requestId: row.request_id,
            resolver: null,
          },
          requestedCid: row.requested_cid,
        },
        receiptSha256,
      };
    });
  }

  async recordIpnsMutationOutcome(
    planValue: CandidateSourceSnapshotDemoPlan,
    admissionValue: CandidateSourceSnapshotIpnsAttemptAdmission,
    outcomeValue: {
      outcome:
        | "acknowledged"
        | "timeout_unknown"
        | "retryable_failure"
        | "terminal_failure";
      receiptSha256: string;
    },
  ): Promise<void> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const admission = admissionValue;
    const outcome = z
      .strictObject({
        outcome: z.enum([
          "acknowledged",
          "timeout_unknown",
          "retryable_failure",
          "terminal_failure",
        ]),
        receiptSha256: z.string().regex(/^[a-f0-9]{64}$/),
      })
      .parse(outcomeValue);
    await this.transaction(async (transaction) => {
      await assertExecutingPlan(transaction, plan);
      const rows = await transaction<
        {
          outcome: string;
          receipt_sha256: string | null;
          request_id: string;
        }[]
      >`
        SELECT outcome, receipt_sha256, request_id
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts
        WHERE attempt_id = ${admission.attemptId}
          AND request_id = ${admission.request.requestId}
          AND plan_id = ${plan.planId}
          AND intent_id = ${admission.request.intentId}
          AND domain = ${admission.request.domain}
          AND direction = ${admission.direction}
          AND attempt_sequence = ${admission.attemptSequence}
          AND requested_cid = ${admission.requestedCid}
        FOR UPDATE
      `;
      const row = rows[0];
      if (!row) {
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS outcome lacks its admitted attempt",
        );
      }
      if (row.outcome !== "request_started") {
        if (
          row.outcome === outcome.outcome &&
          row.receipt_sha256 === outcome.receiptSha256
        ) {
          return;
        }
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS outcome conflicts with terminal evidence",
        );
      }
      const requestOutcome =
        outcome.outcome === "acknowledged"
          ? "succeeded"
          : outcome.outcome === "timeout_unknown"
            ? "timeout_unknown"
            : outcome.outcome;
      const completedAttempts = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_ipns_attempts
        SET outcome = ${outcome.outcome},
            receipt_sha256 = ${outcome.receiptSha256}, completed_at = now()
        WHERE attempt_id = ${admission.attemptId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequests = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${requestOutcome},
            receipt_sha256 = ${outcome.receiptSha256}, completed_at = now()
        WHERE request_id = ${admission.request.requestId}
          AND outcome = 'request_started'
        RETURNING request_id
      `;
      if (!completedAttempts[0] || !completedRequests[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot IPNS outcome lost its admitted request",
        );
      }
    });
  }

  async getCheckpoint(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    return await this.transaction(async (transaction) =>
      checkpoint(await loadObjectForUpdate(transaction, plan, object)),
    );
  }

  async admitObject(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    return await this.transaction(async (transaction) => {
      await assertExecutingPlan(transaction, plan);
      const row = await loadObjectForUpdate(transaction, plan, object);
      if (row.status === "pending") {
        const updated = await transaction<{ remote_object_key: string }[]>`
          UPDATE oracle_candidate_source_snapshot_demo_objects
          SET status = 'admitted', revision = revision + 1, updated_at = now()
          WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
            AND remote_object_key = ${object.remoteObjectKey}
            AND revision = ${row.revision}
          RETURNING remote_object_key
        `;
        if (!updated[0]) {
          throw new DurableConflictError(
            "Candidate source-snapshot admission lost its object binding",
          );
        }
        row.status = "admitted";
        row.revision += 1;
      }
      return checkpoint(row);
    });
  }

  async getPlanAccounting(
    planValue: CandidateSourceSnapshotDemoPlan,
  ): Promise<CandidateSourceSnapshotPlanAccounting> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    return await this.transaction(async (transaction) => {
      const rows = await transaction<AccountingRow[]>`
        SELECT request_count, class_a_mutation_count, class_b_read_count,
               names_api_count, public_resolver_count, free_operation_count,
               request_cost_usd, revision
        FROM oracle_candidate_source_snapshot_demo_accounting
        WHERE plan_id = ${plan.planId}
      `;
      if (!rows[0]) throw new DurableConflictError("Accounting is missing");
      return accounting(rows[0]);
    });
  }

  async listAttempts(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
  ): Promise<readonly CandidateSourceSnapshotUploadAttempt[]> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    return await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      const rows = await transaction<
        {
          attempt_id: string;
          attempt_sequence: number;
          outcome: CandidateSourceSnapshotUploadAttempt["outcome"];
          request_id: string;
          started_at: Date;
        }[]
      >`
        SELECT upload_attempt.attempt_id, upload_attempt.attempt_sequence,
               upload_attempt.outcome, upload_attempt.request_id,
               upload_attempt.started_at
        FROM oracle_candidate_source_snapshot_demo_upload_attempts upload_attempt
        WHERE upload_attempt.plan_id = ${plan.planId}
          AND upload_attempt.domain = ${object.domain}
          AND upload_attempt.remote_object_key = ${object.remoteObjectKey}
          AND NOT EXISTS (
            SELECT 1
            FROM oracle_candidate_source_snapshot_demo_inspection_attempts inspection
            WHERE inspection.recovery_upload_attempt_id = upload_attempt.attempt_id
              AND (
                EXISTS (
                  SELECT 1
                  FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations reconciliation
                  WHERE reconciliation.inspection_id = inspection.inspection_id
                    AND (
                      (reconciliation.result = 'conclusively_absent' AND
                       inspection.outcome = 'absent') OR
                      (reconciliation.result = 'remote_verified' AND
                       inspection.outcome = 'verified')
                    )
                ) OR EXISTS (
                  SELECT 1
                  FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
                  JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
                    ON resolution.inspection_cycle_id = member.inspection_cycle_id
                   AND resolution.domain = member.domain
                   AND resolution.remote_object_key = member.remote_object_key
                  WHERE member.plan_id = upload_attempt.plan_id
                    AND member.domain = upload_attempt.domain
                    AND member.remote_object_key = upload_attempt.remote_object_key
                    AND member.source_attempt_id = upload_attempt.attempt_id
                    AND resolution.inspection_id = inspection.inspection_id
                    AND (
                      (resolution.result = 'conclusively_absent' AND
                       inspection.outcome = 'absent') OR
                      (resolution.result = 'remote_verified' AND
                       inspection.outcome = 'verified')
                    )
                )
              )
          )
        ORDER BY upload_attempt.attempt_sequence
      `;
      return rows.map((row) => ({
        attemptId: row.attempt_id,
        attemptSequence: row.attempt_sequence,
        operation: "upload" as const,
        outcome: row.outcome,
        recoveryUploadAttemptId: null,
        requestId: row.request_id,
        startedAt: row.started_at.toISOString(),
      }));
    });
  }

  async startAttempt(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attemptSequence: number,
  ) {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    return await this.transaction(
      async (transaction) =>
        await admitRequest(transaction, {
          ...(this.#continuation ? { continuation: this.#continuation } : {}),
          object,
          operation: "upload",
          plan,
          sequence: attemptSequence,
        }),
    );
  }

  async startInspection(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    recoveryAttempt: CandidateSourceSnapshotUploadAttempt,
  ) {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    return await this.transaction(async (transaction) => {
      let inspectionSequence = recoveryAttempt.attemptSequence;
      if (this.#continuation?.resumeAuthorizationId) {
        await transaction`SELECT oracle_css_freeze_upload_inspection_cycle(
          ${plan.planId}, ${this.#continuation.resumeAuthorizationId},
          ${this.#continuation.leaseId},
          ${this.#continuation.leaseGeneration}, now()
        )`;
        const latest = await transaction<
          { inspection_sequence: number; outcome: string }[]
        >`
          SELECT inspection_sequence, outcome
          FROM oracle_candidate_source_snapshot_demo_inspection_attempts
          WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
            AND remote_object_key = ${object.remoteObjectKey}
          ORDER BY inspection_sequence DESC
          LIMIT 1
          FOR UPDATE
        `;
        inspectionSequence =
          latest[0]?.outcome === "request_started"
            ? latest[0].inspection_sequence
            : (latest[0]?.inspection_sequence ?? 0) + 1;
        if (inspectionSequence > 3) {
          throw new DurableConflictError(
            "Candidate source-snapshot inspection allowance is exhausted",
          );
        }
      }
      return await admitRequest(transaction, {
        ...(this.#continuation ? { continuation: this.#continuation } : {}),
        object,
        operation: "inspect",
        plan,
        recoveryAttempt,
        sequence: inspectionSequence,
      });
    });
  }

  async markInterruptedAttemptUnknown(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
  ): Promise<void> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    await this.finishUploadAttempt(plan, object, attempt, "timeout_unknown");
  }

  async recordAttemptFailure(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome:
      | "absent"
      | "ambiguous"
      | "connection_failure"
      | "inspection_mismatch"
      | "provider_cid_mismatch"
      | "retryable_http_error"
      | "terminal_failure"
      | "timeout_unknown",
    evidenceValue?: CandidateSourceSnapshotTransportFailureEvidence,
  ): Promise<void> {
    if (
      ![
        "connection_failure",
        "retryable_http_error",
        "timeout_unknown",
      ].includes(outcome)
    ) {
      throw new DurableInputError("Upload failure outcome is invalid");
    }
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    const evidence = evidenceValue
      ? transportFailureEvidenceSchema.parse(evidenceValue)
      : undefined;
    if (this.#continuation && !evidence) {
      throw new DurableInputError(
        "Continuation upload failure requires fixed transport evidence",
      );
    }
    await this.finishUploadAttempt(
      plan,
      object,
      attempt,
      outcome as
        "connection_failure" | "retryable_http_error" | "timeout_unknown",
      evidence,
    );
  }

  private async finishUploadAttempt(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome: "connection_failure" | "retryable_http_error" | "timeout_unknown",
    evidence?: CandidateSourceSnapshotTransportFailureEvidence,
  ): Promise<void> {
    await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      await assertUploadAttemptForUpdate(transaction, plan, object, attempt);
      const requestOutcome =
        outcome === "timeout_unknown" ? "timeout_unknown" : "retryable_failure";
      const completedAttempt = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_upload_attempts
        SET outcome = ${outcome},
            provider_request_id_hash = ${evidence?.providerRequestIdHash ?? null},
            receipt_sha256 = ${evidence?.evidenceSha256 ?? null},
            transport_stage = ${evidence?.stage ?? null},
            failure_class = ${evidence?.failureClass ?? null},
            completed_at = now()
        WHERE attempt_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${requestOutcome},
            receipt_sha256 = ${evidence?.evidenceSha256 ?? null},
            completed_at = now()
        WHERE request_id = ${attempt.requestId} AND outcome = 'request_started'
        RETURNING request_id
      `;
      const updatedObject = await transaction<{ remote_object_key: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_objects
        SET status = ${outcome === "timeout_unknown" ? "outcome_unknown" : "admitted"},
            revision = revision + 1, updated_at = now()
        WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
          AND remote_object_key = ${object.remoteObjectKey}
          AND logical_object_key = ${object.logicalObjectKey}
          AND expected_sha256 = ${object.sha256}
          AND expected_cid = ${object.expectedCid}
          AND expected_bytes = ${object.byteSize}
        RETURNING remote_object_key
      `;
      if (!completedAttempt[0] || !completedRequest[0] || !updatedObject[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot upload failure checkpoint lost its durable binding",
        );
      }
    });
  }

  async recordTerminalFailure(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome: "provider_cid_mismatch" | "terminal_failure",
    evidenceValue?: CandidateSourceSnapshotTransportFailureEvidence,
  ): Promise<void> {
    if (!["provider_cid_mismatch", "terminal_failure"].includes(outcome)) {
      throw new DurableInputError("Upload terminal outcome is invalid");
    }
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    const evidence = evidenceValue
      ? transportFailureEvidenceSchema.parse(evidenceValue)
      : undefined;
    if (this.#continuation && !evidence) {
      throw new DurableInputError(
        "Continuation terminal failure requires fixed transport evidence",
      );
    }
    await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      await assertUploadAttemptForUpdate(transaction, plan, object, attempt);
      const completedAttempt = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_upload_attempts
        SET outcome = ${outcome},
            provider_request_id_hash = ${evidence?.providerRequestIdHash ?? null},
            receipt_sha256 = ${evidence?.evidenceSha256 ?? null},
            transport_stage = ${evidence?.stage ?? null},
            failure_class = ${evidence?.failureClass ?? null},
            completed_at = now()
        WHERE attempt_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = 'terminal_failure',
            receipt_sha256 = ${evidence?.evidenceSha256 ?? null},
            completed_at = now()
        WHERE request_id = ${attempt.requestId} AND outcome = 'request_started'
        RETURNING request_id
      `;
      const updatedObject = await transaction<{ remote_object_key: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_objects
        SET status = 'failed_terminal', revision = revision + 1,
            updated_at = now()
        WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
          AND remote_object_key = ${object.remoteObjectKey}
          AND logical_object_key = ${object.logicalObjectKey}
          AND expected_sha256 = ${object.sha256}
          AND expected_cid = ${object.expectedCid}
          AND expected_bytes = ${object.byteSize}
        RETURNING remote_object_key
      `;
      if (!completedAttempt[0] || !completedRequest[0] || !updatedObject[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot terminal checkpoint lost its durable binding",
        );
      }
    });
  }

  async recordVerified(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    receipt: CandidateSourceSnapshotUploadReceipt,
  ): Promise<void> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    const parsedReceipt = uploadReceiptSchema.parse(receipt);
    if (parsedReceipt.providerCid !== object.expectedCid) {
      throw new DurableInputError(
        "Verified provider CID does not match the plan",
      );
    }
    if (
      parsedReceipt.receiptSha256 !==
      expectedCandidateSourceSnapshotUploadReceiptSha256({
        attempt,
        object,
        providerCid: parsedReceipt.providerCid,
        providerRequestIdHash: parsedReceipt.providerRequestIdHash,
        responseBytes: parsedReceipt.responseBytes,
      })
    ) {
      throw new DurableInputError(
        "Verified provider receipt does not match its immutable upload attempt",
      );
    }
    await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      await assertUploadAttemptForUpdate(transaction, plan, object, attempt);
      const completedAttempt = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_upload_attempts
        SET outcome = 'verified', provider_cid = ${parsedReceipt.providerCid},
            provider_request_id_hash = ${parsedReceipt.providerRequestIdHash},
            receipt_sha256 = ${parsedReceipt.receiptSha256},
            response_bytes = ${parsedReceipt.responseBytes},
            transport_stage = ${this.#continuation ? "put_object_provider_response" : null},
            failure_class = NULL, completed_at = now()
        WHERE attempt_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = 'succeeded', receipt_sha256 = ${parsedReceipt.receiptSha256},
            completed_at = now()
        WHERE request_id = ${attempt.requestId} AND outcome = 'request_started'
        RETURNING request_id
      `;
      const updatedObject = await transaction<{ remote_object_key: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_objects
        SET status = 'verified', provider_cid = ${parsedReceipt.providerCid},
            receipt_sha256 = ${parsedReceipt.receiptSha256},
            successful_effect_count = 1, revision = revision + 1,
            updated_at = now()
        WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
          AND remote_object_key = ${object.remoteObjectKey}
          AND logical_object_key = ${object.logicalObjectKey}
          AND expected_sha256 = ${object.sha256}
          AND expected_cid = ${object.expectedCid}
          AND expected_bytes = ${object.byteSize}
        RETURNING remote_object_key
      `;
      if (!completedAttempt[0] || !completedRequest[0] || !updatedObject[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot verified checkpoint lost its durable binding",
        );
      }
    });
  }

  async recordInspectionResult(
    planValue: CandidateSourceSnapshotDemoPlan,
    objectValue: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    resultValue: CandidateSourceSnapshotInspectionResult,
  ): Promise<CandidateSourceSnapshotUploadCheckpoint> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    const result = inspectionResultSchema.parse(resultValue);
    return await this.transaction(async (transaction) => {
      const row = await loadObjectForUpdate(transaction, plan, object);
      const existing = await transaction<
        {
          domain: string;
          observed_bytes: string | number | null;
          observed_cid: string | null;
          observed_sha256: string | null;
          outcome: string;
          plan_id: string;
          receipt_sha256: string;
          remote_object_key: string;
          request_id: string;
        }[]
      >`
        SELECT request_id, plan_id, domain, remote_object_key, outcome,
               observed_cid, observed_sha256, observed_bytes, receipt_sha256
        FROM oracle_candidate_source_snapshot_demo_inspections
        WHERE inspection_id = ${attempt.attemptId}
      `;
      if (existing[0]) {
        const prior = existing[0];
        const exactResult =
          prior.request_id === attempt.requestId &&
          prior.plan_id === plan.planId &&
          prior.domain === object.domain &&
          prior.remote_object_key === object.remoteObjectKey &&
          prior.outcome === result.outcome &&
          prior.receipt_sha256 === result.receiptSha256 &&
          prior.observed_cid ===
            ("observedCid" in result ? result.observedCid : null) &&
          prior.observed_sha256 ===
            ("observedSha256" in result ? result.observedSha256 : null) &&
          (prior.observed_bytes === null
            ? null
            : Number(prior.observed_bytes)) ===
            ("observedBytes" in result ? result.observedBytes : null);
        if (!exactResult) {
          throw new DurableConflictError(
            "Candidate source-snapshot inspection result replay conflicts with durable evidence",
          );
        }
        await this.recordResumeInspectionCycleResolution(
          transaction,
          plan,
          object,
          attempt,
          result,
        );
        return checkpoint(await loadObjectForUpdate(transaction, plan, object));
      }
      await assertInspectionAttemptForUpdate(
        transaction,
        plan,
        object,
        attempt,
      );
      if (
        result.outcome === "verified" &&
        (result.observedCid !== object.expectedCid ||
          result.observedSha256 !== object.sha256 ||
          result.observedBytes !== object.byteSize)
      ) {
        throw new DurableInputError(
          "Inspection result does not match the plan",
        );
      }
      const inserted = await transaction<{ inspection_id: string }[]>`
        INSERT INTO oracle_candidate_source_snapshot_demo_inspections (
          inspection_id, request_id, plan_id, domain, remote_object_key,
          outcome, observed_cid, observed_sha256, observed_bytes, receipt_sha256
        ) VALUES (
          ${attempt.attemptId}, ${attempt.requestId}, ${plan.planId},
          ${object.domain}, ${object.remoteObjectKey}, ${result.outcome},
          ${"observedCid" in result ? result.observedCid : null},
          ${"observedSha256" in result ? result.observedSha256 : null},
          ${"observedBytes" in result ? result.observedBytes : null},
          ${result.receiptSha256}
        )
        RETURNING inspection_id
      `;
      if (!inserted[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot inspection result was not persisted",
        );
      }
      const completedInspection = await transaction<
        { inspection_id: string }[]
      >`
        UPDATE oracle_candidate_source_snapshot_demo_inspection_attempts
        SET outcome = ${result.outcome},
            observed_cid = ${"observedCid" in result ? result.observedCid : null},
            observed_sha256 = ${"observedSha256" in result ? result.observedSha256 : null},
            observed_bytes = ${"observedBytes" in result ? result.observedBytes : null},
            receipt_sha256 = ${result.receiptSha256}, completed_at = now()
        WHERE inspection_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND recovery_upload_attempt_id = ${attempt.recoveryUploadAttemptId}
          AND outcome = 'request_started'
        RETURNING inspection_id
      `;
      const requestOutcome =
        result.outcome === "verified"
          ? "succeeded"
          : result.outcome === "mismatch"
            ? "terminal_failure"
            : result.outcome;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${requestOutcome}, receipt_sha256 = ${result.receiptSha256},
            completed_at = now()
        WHERE request_id = ${attempt.requestId} AND outcome = 'request_started'
        RETURNING request_id
      `;
      const status =
        result.outcome === "verified"
          ? "verified"
          : result.outcome === "mismatch"
            ? "failed_terminal"
            : result.outcome === "ambiguous"
              ? "outcome_unknown"
              : "admitted";
      const updatedObject = await transaction<{ remote_object_key: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_objects
        SET status = ${status},
            provider_cid = ${result.outcome === "verified" ? result.observedCid : null},
            receipt_sha256 = ${result.outcome === "verified" ? result.receiptSha256 : row.receipt_sha256},
            successful_effect_count = ${result.outcome === "verified" ? 1 : 0},
            revision = revision + 1, updated_at = now()
        WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
          AND remote_object_key = ${object.remoteObjectKey}
          AND logical_object_key = ${object.logicalObjectKey}
          AND expected_sha256 = ${object.sha256}
          AND expected_cid = ${object.expectedCid}
          AND expected_bytes = ${object.byteSize}
        RETURNING remote_object_key
      `;
      if (
        !completedInspection[0] ||
        !completedRequest[0] ||
        !updatedObject[0]
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot inspection checkpoint lost its durable binding",
        );
      }
      await this.recordResumeInspectionCycleResolution(
        transaction,
        plan,
        object,
        attempt,
        result,
      );
      return checkpoint(await loadObjectForUpdate(transaction, plan, object));
    });
  }

  private async recordResumeInspectionCycleResolution(
    transaction: postgres.TransactionSql,
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    result: CandidateSourceSnapshotInspectionResult,
  ): Promise<void> {
    const continuation = this.#continuation;
    if (
      !continuation?.resumeAuthorizationId ||
      !["verified", "absent"].includes(result.outcome) ||
      !attempt.recoveryUploadAttemptId
    ) {
      return;
    }
    const resolutionResult =
      result.outcome === "verified" ? "remote_verified" : "conclusively_absent";
    const inserted = await transaction<
      {
        inspection_cycle_id: string;
      }[]
    >`
      INSERT INTO oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions (
        inspection_cycle_id, plan_id, domain, remote_object_key,
        inspection_id, result, receipt_sha256, recorded_at
      )
      SELECT cycle.inspection_cycle_id, ${plan.planId}, ${object.domain},
             ${object.remoteObjectKey}, ${attempt.attemptId},
             ${resolutionResult}, ${result.receiptSha256}, now()
      FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
      JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
        ON member.inspection_cycle_id = cycle.inspection_cycle_id
      WHERE cycle.plan_id = ${plan.planId}
        AND cycle.resume_authorization_id = ${continuation.resumeAuthorizationId}
        AND cycle.executor_lease_id = ${continuation.leaseId}
        AND cycle.lease_generation = ${continuation.leaseGeneration}
        AND member.domain = ${object.domain}
        AND member.remote_object_key = ${object.remoteObjectKey}
        AND member.source_attempt_id = ${attempt.recoveryUploadAttemptId}
      ON CONFLICT (inspection_cycle_id, domain, remote_object_key) DO NOTHING
      RETURNING inspection_cycle_id
    `;
    if (inserted[0]) return;
    const existing = await transaction<
      {
        inspection_id: string;
        receipt_sha256: string;
        result: string;
      }[]
    >`
      SELECT resolution.inspection_id, resolution.result,
             resolution.receipt_sha256
      FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
      JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
        ON member.inspection_cycle_id = cycle.inspection_cycle_id
      JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
        ON resolution.inspection_cycle_id = member.inspection_cycle_id
       AND resolution.domain = member.domain
       AND resolution.remote_object_key = member.remote_object_key
      WHERE cycle.plan_id = ${plan.planId}
        AND cycle.resume_authorization_id = ${continuation.resumeAuthorizationId}
        AND member.domain = ${object.domain}
        AND member.remote_object_key = ${object.remoteObjectKey}
        AND member.source_attempt_id = ${attempt.recoveryUploadAttemptId}
    `;
    if (
      existing.length !== 1 ||
      existing[0]!.inspection_id !== attempt.attemptId ||
      existing[0]!.result !== resolutionResult ||
      existing[0]!.receipt_sha256 !== result.receiptSha256
    ) {
      throw new DurableConflictError(
        "Candidate source-snapshot inspection cycle resolution conflicts",
      );
    }
  }
}
