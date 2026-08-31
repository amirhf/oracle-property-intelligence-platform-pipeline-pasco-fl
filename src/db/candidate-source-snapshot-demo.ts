import postgres from "postgres";
import { z } from "zod";

import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";
import {
  assertCandidateSourceSnapshotObjectNamespace,
  candidateSourceSnapshotExactUploadBindingSchema,
  candidateSourceSnapshotObjectSchema,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotExactUploadBinding,
  type CandidateSourceSnapshotUploadObject,
} from "../publication/candidate-source-snapshot-demo.js";
import type {
  CandidateSourceSnapshotPlanAccounting,
  CandidateSourceSnapshotInspectionResult,
  CandidateSourceSnapshotUploadAttempt,
  CandidateSourceSnapshotUploadCheckpoint,
  CandidateSourceSnapshotUploadJournal,
  CandidateSourceSnapshotUploadReceipt,
} from "../publication/candidate-source-snapshot-upload.js";

const OBJECT_BATCH_SIZE = 500;

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

export async function approveCandidateSourceSnapshotDemoPlan(
  databaseUrl: string,
  inputValue: {
    approvedAt: string;
    approverReference: string;
    planId: string;
    planSha256: string;
  },
): Promise<{ approvalId: string; state: CandidateSourceSnapshotDurableState }> {
  const input = z
    .strictObject({
      approvedAt: operatorTimestampSchema,
      approverReference: operatorReferenceSchema,
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
      const approvalId = deterministicId("snapshotdemoapproval", [
        "candidate-source-snapshot-approval-v1",
        plan.planId,
        plan.planSha256,
        input.approverReference,
        input.approvedAt,
      ]);
      const existing = await transaction<
        {
          approval_id: string;
          approved_at: Date;
          approved_plan_revision: number;
          approver_reference: string;
        }[]
      >`
        SELECT approval_id, approved_plan_revision, approver_reference,
               approved_at
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE plan_id = ${plan.planId}
      `;
      if (existing[0]) {
        if (
          existing[0].approval_id !== approvalId ||
          existing[0].approver_reference !== input.approverReference ||
          existing[0].approved_at.toISOString() !== input.approvedAt
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
            approver_reference, approved_at
          ) VALUES (
            ${approvalId}, ${plan.planId}, ${plan.planSha256},
            ${exact.planArtifact.sha256}, ${exact.planArtifact.expectedCid},
            ${exact.planArtifact.remoteObjectKey}, ${exact.planArtifact.byteSize},
            ${row.revision}, ${input.approverReference}, ${input.approvedAt}
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
        metadata: { approvalId },
        plan,
      });
      return {
        approvalId,
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
    executorEnabled: true;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotDurableState> {
  const input = z
    .strictObject({
      approvalId: z.string().regex(/^snapshotdemoapproval_[a-f0-9]{32}$/),
      executorEnabled: z.literal(true),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: z.string().regex(/^[a-f0-9]{64}$/),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const { plan, row } = await loadExactPlanRowForUpdate(transaction, input);
      const approvals = await transaction<{ approval_id: string }[]>`
        SELECT approval_id
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${input.approvalId}
          AND plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
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
        metadata: { approvalId: input.approvalId },
        plan,
      });
      return durableState(plan, row, await counts(transaction, plan.planId));
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotIpnsIntentRecord {
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  priorCid: string;
  state: "intent_recorded";
  targetCid: string;
}

export interface CandidateSourceSnapshotDemoExecutionAdmission {
  accounting: CandidateSourceSnapshotPlanAccounting;
  approval: {
    approvalId: string;
    approvedAt: string;
    approverReference: string;
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
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotDemoExecutionAdmission> {
  const identity = z
    .strictObject({
      approvalId: z.string().regex(/^snapshotdemoapproval_[a-f0-9]{32}$/),
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
          approved_at: Date;
          approver_reference: string;
        }[]
      >`
        SELECT approval_id, approver_reference, approved_at
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${identity.approvalId}
          AND plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
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
          domain: CandidateSourceSnapshotIpnsDomain;
          intent_id: string;
          prior_cid: string;
          state: string;
          target_cid: string;
        }[]
      >`
        SELECT intent.intent_id, intent.domain, intent.prior_cid,
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
          domain: intent.domain,
          intentId: intent.intent_id,
          priorCid: intent.prior_cid,
          state: "intent_recorded" as const,
          targetCid: intent.target_cid,
        };
      });
      return {
        accounting: accounting(accountingRows[0]),
        approval: {
          approvalId: approvals[0].approval_id,
          approvedAt: approvals[0].approved_at.toISOString(),
          approverReference: approvals[0].approver_reference,
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
      const targets = [
        { domain: "open_data" as const, target: plan.targets.openData },
        { domain: "query_table" as const, target: plan.targets.queryTable },
      ];
      const records: CandidateSourceSnapshotIpnsIntentRecord[] = [];
      for (const { domain, target } of targets) {
        const intentId = deterministicId("snapshotdemointent", [
          "candidate-source-snapshot-intent-v1",
          plan.planId,
          plan.planSha256,
          domain,
          target.bucket,
          target.ipnsLabel,
          target.ipnsNetworkKey,
          target.priorCid,
          target.targetCid,
          input.intendedAt,
        ]);
        const existing = await transaction<
          {
            bucket: string;
            intent_id: string;
            intended_at: Date;
            ipns_label: string;
            ipns_network_key: string;
            prior_cid: string;
            state: string;
            target_cid: string;
          }[]
        >`
          SELECT intent.intent_id, intent.bucket, intent.ipns_label,
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
              intent_id, plan_id, plan_sha256, domain, bucket, ipns_label,
              ipns_network_key, prior_cid, target_cid, intended_at
            ) VALUES (
              ${intentId}, ${plan.planId}, ${plan.planSha256}, ${domain},
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
          domain,
          intentId,
          priorCid: target.priorCid,
          state: "intent_recorded",
          targetCid: target.targetCid,
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
  resolver: CandidateSourceSnapshotIpnsResolver | null;
}

export interface CandidateSourceSnapshotIpnsAttemptAdmission {
  attemptId: string;
  attemptSequence: number;
  direction: "rollback" | "update";
  request: CandidateSourceSnapshotRemoteRequestAdmission;
  requestedCid: string;
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

function expectedUploadReceiptSha256(input: {
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

function remoteRequestAllowance(
  plan: CandidateSourceSnapshotDemoPlan,
  operationClass: "names_api" | "public_resolver",
): number {
  if (operationClass === "names_api") {
    return (
      plan.requestEnvelope.maximumAttempts.namesApiOperations +
      plan.requestEnvelope.recoveryAllowance.namesApiOperations
    );
  }
  return (
    plan.requestEnvelope.maximumAttempts.publicResolverOperations +
    plan.requestEnvelope.recoveryAllowance.publicResolverOperations
  );
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
    SELECT request_id, intent_id, domain, operation_kind, cycle_sequence,
           resolver, outcome, receipt_sha256, completed_at
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
  const classCount =
    operationClass === "names_api"
      ? nextAccounting.namesApiCount
      : nextAccounting.publicResolverCount;
  const allowedRequestCost =
    input.plan.costEnvelope.requestUsd.maximumAttempts +
    input.plan.costEnvelope.requestUsd.ambiguousObjectInspections +
    input.plan.costEnvelope.recoveryRequestUsd;
  if (
    classCount > remoteRequestAllowance(input.plan, operationClass) ||
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
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_requests (
      request_id, plan_id, operation_class, operation_kind, intent_id, domain,
      remote_object_key, cycle_sequence, resolver, request_cost_usd, outcome
    ) VALUES (
      ${id}, ${input.plan.planId}, ${operationClass}, ${input.operationKind},
      ${input.intentId}, ${input.domain}, NULL, ${input.cycleSequence},
      ${input.resolver}, ${requestCostUsd}, 'request_started'
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
    resolver: input.resolver,
  };
}

async function admitRequest(
  transaction: postgres.TransactionSql,
  input: {
    object: CandidateSourceSnapshotUploadObject;
    operation: "inspect" | "upload";
    plan: CandidateSourceSnapshotDemoPlan;
    recoveryAttempt?: CandidateSourceSnapshotUploadAttempt;
    sequence: number;
  },
): Promise<{
  accounting: CandidateSourceSnapshotPlanAccounting;
  attempt: CandidateSourceSnapshotUploadAttempt;
  replayedResult: CandidateSourceSnapshotInspectionResult | null;
}> {
  await assertExecutingPlan(transaction, input.plan);
  await loadObjectForUpdate(transaction, input.plan, input.object);
  const id = requestId(input);
  const attemptIdValue = attemptId(input);
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
    if (input.sequence !== input.recoveryAttempt.attemptSequence) {
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
      }[]
    >`
      SELECT attempt.inspection_id, attempt.request_id,
             attempt.recovery_upload_attempt_id, attempt.outcome,
             attempt.observed_cid, attempt.observed_sha256,
             attempt.observed_bytes, attempt.receipt_sha256
      FROM oracle_candidate_source_snapshot_demo_inspection_attempts attempt
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
        row.recovery_upload_attempt_id !== input.recoveryAttempt.attemptId
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
  if (
    nextClassA > input.plan.requestEnvelope.maximumAttempts.classAMutations ||
    nextClassB >
      input.plan.requestEnvelope.ambiguousObjectInspectionAllowance
        .classBReads ||
    nextRequestCount > input.plan.requestEnvelope.maximumTotalRequests ||
    nextRequestCostUsd >
      input.plan.costEnvelope.requestUsd.maximumAttempts +
        input.plan.costEnvelope.requestUsd.ambiguousObjectInspections +
        input.plan.costEnvelope.recoveryRequestUsd
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
  const operationClass =
    input.operation === "upload" ? "class_a_mutation" : "class_b_read";
  const operationKind =
    input.operation === "upload" ? "put_object" : "inspect_object";
  await transaction`
    INSERT INTO oracle_candidate_source_snapshot_demo_requests (
      request_id, plan_id, operation_class, operation_kind, domain,
      remote_object_key, request_cost_usd, outcome
    ) VALUES (
      ${id}, ${input.plan.planId}, ${operationClass}, ${operationKind},
      ${input.object.domain}, ${input.object.remoteObjectKey},
      ${requestCostUsd}, 'request_started'
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
    await transaction`
      INSERT INTO oracle_candidate_source_snapshot_demo_upload_attempts (
        attempt_id, request_id, plan_id, domain, remote_object_key,
        attempt_sequence, outcome, request_count
      ) VALUES (
        ${attempt.attemptId}, ${attempt.requestId}, ${input.plan.planId},
        ${input.object.domain}, ${input.object.remoteObjectKey},
        ${input.sequence}, 'request_started', 1
      )
    `;
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
    attempt,
    replayedResult: null,
  };
}

/** PostgreSQL-backed resumable journal for the future explicitly enabled run. */
export class PostgresCandidateSourceSnapshotUploadJournal implements CandidateSourceSnapshotUploadJournal {
  constructor(private readonly databaseUrl: string) {}

  private async transaction<T>(
    operation: (transaction: postgres.TransactionSql) => Promise<T>,
  ): Promise<T> {
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
      resolver: CandidateSourceSnapshotIpnsResolver;
    },
  ): Promise<CandidateSourceSnapshotRemoteRequestAdmission> {
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const input = z
      .strictObject({
        cycleSequence: z.number().int().min(1).max(32),
        domain: ipnsDomainSchema,
        intentId: z.string().regex(/^snapshotdemointent_[a-f0-9]{32}$/),
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
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const request = z
      .strictObject({
        accounting: z.unknown(),
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
        resolver: ipnsResolverSchema,
      })
      .parse(requestValue);
    const observation = z
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
      .parse(observationValue);
    const validOutcome =
      (["prior", "target", "unexpected_cid"].includes(
        observation.classification,
      ) &&
        observation.requestOutcome === "succeeded") ||
      (observation.classification === "split" &&
        observation.requestOutcome === "ambiguous") ||
      (observation.classification === "unavailable" &&
        ["retryable_failure", "timeout_unknown", "terminal_failure"].includes(
          observation.requestOutcome,
        ));
    if (!validOutcome) {
      throw new DurableInputError(
        "Candidate source-snapshot observation outcome is inconsistent",
      );
    }
    await this.transaction(async (transaction) => {
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
        SELECT request_id, intent_id, domain, operation_kind, cycle_sequence,
               resolver, outcome, receipt_sha256, completed_at
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE request_id = ${request.requestId}
          AND plan_id = ${plan.planId}
          AND intent_id = ${request.intentId}
          AND domain = ${request.domain}
          AND cycle_sequence = ${request.cycleSequence}
          AND resolver = ${request.resolver}
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
    });
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
        }[]
      >`
        SELECT attempt_id, attempt_sequence, outcome, request_id
        FROM oracle_candidate_source_snapshot_demo_upload_attempts
        WHERE plan_id = ${plan.planId} AND domain = ${object.domain}
          AND remote_object_key = ${object.remoteObjectKey}
        ORDER BY attempt_sequence
      `;
      return rows.map((row) => ({
        attemptId: row.attempt_id,
        attemptSequence: row.attempt_sequence,
        operation: "upload" as const,
        outcome: row.outcome,
        recoveryUploadAttemptId: null,
        requestId: row.request_id,
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
    return await this.transaction(
      async (transaction) =>
        await admitRequest(transaction, {
          object,
          operation: "inspect",
          plan,
          recoveryAttempt,
          sequence: recoveryAttempt.attemptSequence,
        }),
    );
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
    await this.finishUploadAttempt(
      plan,
      object,
      attempt,
      outcome as
        "connection_failure" | "retryable_http_error" | "timeout_unknown",
    );
  }

  private async finishUploadAttempt(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    attempt: CandidateSourceSnapshotUploadAttempt,
    outcome: "connection_failure" | "retryable_http_error" | "timeout_unknown",
  ): Promise<void> {
    await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      await assertUploadAttemptForUpdate(transaction, plan, object, attempt);
      const requestOutcome =
        outcome === "timeout_unknown" ? "timeout_unknown" : "retryable_failure";
      const completedAttempt = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_upload_attempts
        SET outcome = ${outcome}, completed_at = now()
        WHERE attempt_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = ${requestOutcome}, completed_at = now()
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
  ): Promise<void> {
    if (!["provider_cid_mismatch", "terminal_failure"].includes(outcome)) {
      throw new DurableInputError("Upload terminal outcome is invalid");
    }
    const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
    const object = candidateSourceSnapshotObjectSchema().parse(objectValue);
    await this.transaction(async (transaction) => {
      await loadObjectForUpdate(transaction, plan, object);
      await assertUploadAttemptForUpdate(transaction, plan, object, attempt);
      const completedAttempt = await transaction<{ attempt_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_upload_attempts
        SET outcome = ${outcome}, completed_at = now()
        WHERE attempt_id = ${attempt.attemptId}
          AND request_id = ${attempt.requestId}
          AND outcome = 'request_started'
        RETURNING attempt_id
      `;
      const completedRequest = await transaction<{ request_id: string }[]>`
        UPDATE oracle_candidate_source_snapshot_demo_requests
        SET outcome = 'terminal_failure', completed_at = now()
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
      expectedUploadReceiptSha256({
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
            response_bytes = ${parsedReceipt.responseBytes}, completed_at = now()
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
        return checkpoint(row);
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
      return checkpoint({
        ...row,
        provider_cid: result.outcome === "verified" ? result.observedCid : null,
        receipt_sha256:
          result.outcome === "verified" ? result.receiptSha256 : null,
        revision: row.revision + 1,
        status,
      });
    });
  }
}
