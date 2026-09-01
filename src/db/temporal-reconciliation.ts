import type postgres from "postgres";

import type { DeltaClassification } from "../domain/reconciliation.js";
import { DurableConflictError } from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";
import {
  PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
  type SnapshotCoverage,
} from "../snapshot/coverage.js";
import type { SourceSnapshotManifest } from "../snapshot/model.js";

const INACTIVATION_REASON =
  "absent_from_authoritative_complete_snapshot" as const;

export interface PropertyTemporalDelta {
  classification: DeltaClassification;
  propertyId: string;
  sourceRecordHash: string;
}

export interface TemporalReconciliationPlan {
  coverage: SnapshotCoverage;
  snapshotId: string;
}

export interface TemporalReconciliationResult {
  activeProperties: number;
  inactiveProperties: number;
  inactivatedProperties: number;
  reactivatedProperties: number;
}

function chunks<T>(values: readonly T[], size = 1_000): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}

function temporalConflict(message: string): never {
  throw new DurableConflictError(
    `Temporal reconciliation conflict (${message})`,
  );
}

export async function prepareTemporalReconciliation(
  transaction: postgres.TransactionSql,
  snapshot: SourceSnapshotManifest,
): Promise<TemporalReconciliationPlan> {
  const coverage = snapshot.coverage;
  if (coverage.mode !== "authoritative_complete") {
    return { coverage, snapshotId: snapshot.snapshotId };
  }
  if (
    coverage.completeness.result !== "passed" ||
    coverage.authoritySource.sourceSystem !== "pasco_appraiser" ||
    coverage.authoritySource.sourceIdentifier !==
      PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER
  ) {
    temporalConflict("invalid authoritative-complete derivation");
  }
  const heads = await transaction<
    {
      current_snapshot_id: string;
      scope_id: string;
    }[]
  >`
    SELECT scope_id, current_snapshot_id
    FROM oracle_authoritative_scope_heads
    WHERE county = 'pasco' AND entity_type = 'property_existence'
    FOR UPDATE
  `;
  const head = heads[0];
  if (!head) {
    if (coverage.previousAuthoritativeSnapshotId !== null) {
      temporalConflict("first authoritative snapshot names a predecessor");
    }
  } else {
    if (head.scope_id !== coverage.scopeId) {
      temporalConflict("authoritative property-existence scope changed");
    }
    if (coverage.previousAuthoritativeSnapshotId !== head.current_snapshot_id) {
      temporalConflict(
        "authoritative snapshot predecessor is not the scope head",
      );
    }
  }
  return { coverage, snapshotId: snapshot.snapshotId };
}

export async function applyTemporalReconciliation(
  transaction: postgres.TransactionSql,
  plan: TemporalReconciliationPlan,
  runId: string,
  deltas: readonly PropertyTemporalDelta[],
): Promise<TemporalReconciliationResult> {
  const coverage = plan.coverage;
  const propertyIds = deltas.map((delta) => delta.propertyId);
  const priorState = new Map<
    string,
    {
      lastReconciledSnapshotId: string;
      lastSourceRecordHash: string;
      status: "active" | "inactive";
    }
  >();
  const canonicalActive = new Map<string, boolean>();

  for (const propertyBatch of chunks(propertyIds)) {
    const states = await transaction<
      {
        last_reconciled_snapshot_id: string;
        last_source_record_hash: string;
        lifecycle_status: "active" | "inactive";
        property_id: string;
      }[]
    >`
      SELECT property_id, lifecycle_status, last_reconciled_snapshot_id,
             last_source_record_hash
      FROM oracle_property_scope_state
      WHERE scope_id = ${coverage.scopeId}
        AND property_id = ANY(${propertyBatch})
    `;
    for (const state of states) {
      priorState.set(state.property_id, {
        lastReconciledSnapshotId: state.last_reconciled_snapshot_id,
        lastSourceRecordHash: state.last_source_record_hash,
        status: state.lifecycle_status,
      });
    }
    const properties = await transaction<
      { is_active: boolean; property_id: string }[]
    >`
      SELECT property_id, is_active
      FROM oracle_properties
      WHERE property_id = ANY(${propertyBatch})
    `;
    for (const property of properties) {
      canonicalActive.set(property.property_id, property.is_active);
    }
  }

  let reactivatedProperties = 0;
  const resetValidFromIds: string[] = [];
  const presentEvents = deltas.map((delta) => {
    const prior = priorState.get(delta.propertyId);
    const reactivated =
      prior?.status === "inactive" ||
      canonicalActive.get(delta.propertyId) === false;
    if (reactivated) reactivatedProperties += 1;
    if (reactivated || delta.classification === "changed") {
      resetValidFromIds.push(delta.propertyId);
    }
    const eventType = reactivated ? "reactivated" : delta.classification;
    return {
      event_id: deterministicId("lifecycle", [
        "1.0.0",
        "property-lifecycle",
        delta.propertyId,
        coverage.scopeId,
        plan.snapshotId,
        eventType,
      ]),
      event_type: eventType,
      previous_snapshot_id: prior?.lastReconciledSnapshotId ?? null,
      previous_source_record_hash: prior?.lastSourceRecordHash ?? null,
      property_id: delta.propertyId,
      reason: null,
      run_id: runId,
      scope_id: coverage.scopeId,
      snapshot_id: plan.snapshotId,
      source_record_hash: delta.sourceRecordHash,
    };
  });

  for (const deltaBatch of chunks(deltas)) {
    const stateRows = deltaBatch.map((delta) => ({
      coverage_mode: coverage.mode,
      first_seen_snapshot_id: plan.snapshotId,
      inactivation_reason: null,
      last_reconciled_snapshot_id: plan.snapshotId,
      last_run_id: runId,
      last_seen_snapshot_id: plan.snapshotId,
      last_source_record_hash: delta.sourceRecordHash,
      lifecycle_status: "active",
      property_id: delta.propertyId,
      scope_id: coverage.scopeId,
      valid_from_snapshot_id: plan.snapshotId,
      valid_to_snapshot_id: null,
    }));
    await transaction`
      INSERT INTO oracle_property_scope_state ${transaction(stateRows)}
      ON CONFLICT (property_id, scope_id) DO UPDATE SET
        coverage_mode = EXCLUDED.coverage_mode,
        lifecycle_status = 'active',
        last_seen_snapshot_id = EXCLUDED.last_seen_snapshot_id,
        last_reconciled_snapshot_id = EXCLUDED.last_reconciled_snapshot_id,
        valid_from_snapshot_id = CASE
          WHEN oracle_property_scope_state.lifecycle_status = 'inactive'
          THEN EXCLUDED.valid_from_snapshot_id
          ELSE oracle_property_scope_state.valid_from_snapshot_id
        END,
        valid_to_snapshot_id = NULL,
        inactivation_reason = NULL,
        last_run_id = EXCLUDED.last_run_id,
        last_source_record_hash = EXCLUDED.last_source_record_hash
    `;
  }

  for (const propertyBatch of chunks(propertyIds)) {
    await transaction`
      UPDATE oracle_properties SET
        is_active = true,
        lifecycle_scope_id = CASE
          WHEN ${coverage.mode} = 'authoritative_complete' THEN ${coverage.scopeId}
          ELSE lifecycle_scope_id
        END,
        inactive_at_snapshot_id = NULL,
        inactivation_reason = NULL
      WHERE property_id = ANY(${propertyBatch})
    `;
  }

  for (const propertyBatch of chunks(resetValidFromIds)) {
    await transaction`
      UPDATE oracle_property_scope_state SET
        valid_from_snapshot_id = ${plan.snapshotId}
      WHERE scope_id = ${coverage.scopeId}
        AND property_id = ANY(${propertyBatch})
    `;
  }

  for (const eventBatch of chunks(presentEvents)) {
    await transaction`
      INSERT INTO oracle_property_lifecycle_events ${transaction(eventBatch)}
      ON CONFLICT (property_id, scope_id, snapshot_id, event_type) DO NOTHING
    `;
  }

  let inactivatedProperties = 0;
  if (coverage.mode === "authoritative_complete") {
    const absent = await transaction<
      {
        last_reconciled_snapshot_id: string;
        last_source_record_hash: string;
        property_id: string;
      }[]
    >`
      SELECT property_id, last_reconciled_snapshot_id, last_source_record_hash
      FROM oracle_property_scope_state
      WHERE scope_id = ${coverage.scopeId}
        AND lifecycle_status = 'active'
        AND NOT (property_id = ANY(${propertyIds}))
      ORDER BY property_id
    `;
    inactivatedProperties = absent.length;
    for (const absentBatch of chunks(absent)) {
      const absentIds = absentBatch.map((row) => row.property_id);
      await transaction`
        UPDATE oracle_property_scope_state SET
          lifecycle_status = 'inactive',
          coverage_mode = 'authoritative_complete',
          last_reconciled_snapshot_id = ${plan.snapshotId},
          valid_to_snapshot_id = ${plan.snapshotId},
          inactivation_reason = ${INACTIVATION_REASON},
          last_run_id = ${runId}
        WHERE scope_id = ${coverage.scopeId}
          AND property_id = ANY(${absentIds})
          AND lifecycle_status = 'active'
      `;
      await transaction`
        UPDATE oracle_properties SET
          is_active = false,
          inactive_at_snapshot_id = ${plan.snapshotId},
          inactivation_reason = ${INACTIVATION_REASON}
        WHERE property_id = ANY(${absentIds})
          AND lifecycle_scope_id = ${coverage.scopeId}
      `;
      const events = absentBatch.map((row) => ({
        event_id: deterministicId("lifecycle", [
          "1.0.0",
          "property-lifecycle",
          row.property_id,
          coverage.scopeId,
          plan.snapshotId,
          "inactivated",
        ]),
        event_type: "inactivated",
        previous_snapshot_id: row.last_reconciled_snapshot_id,
        previous_source_record_hash: row.last_source_record_hash,
        property_id: row.property_id,
        reason: INACTIVATION_REASON,
        run_id: runId,
        scope_id: coverage.scopeId,
        snapshot_id: plan.snapshotId,
        source_record_hash: null,
      }));
      await transaction`
        INSERT INTO oracle_property_lifecycle_events ${transaction(events)}
        ON CONFLICT (property_id, scope_id, snapshot_id, event_type) DO NOTHING
      `;
    }
    await transaction`
      INSERT INTO oracle_authoritative_scope_heads (
        scope_id, county, entity_type, authority_source_system,
        authority_source_identifier, current_snapshot_id, current_run_id
      ) VALUES (
        ${coverage.scopeId}, 'pasco', 'property_existence',
        ${coverage.authoritySource.sourceSystem},
        ${coverage.authoritySource.sourceIdentifier},
        ${plan.snapshotId}, ${runId}
      )
      ON CONFLICT (scope_id) DO UPDATE SET
        current_snapshot_id = EXCLUDED.current_snapshot_id,
        current_run_id = EXCLUDED.current_run_id,
        recorded_at = now()
    `;
  }

  const counts = await transaction<{ active: number; inactive: number }[]>`
    SELECT
      count(*) FILTER (WHERE lifecycle_status = 'active')::int AS active,
      count(*) FILTER (WHERE lifecycle_status = 'inactive')::int AS inactive
    FROM oracle_property_scope_state
    WHERE scope_id = ${coverage.scopeId}
  `;
  return {
    activeProperties: counts[0]?.active ?? 0,
    inactiveProperties: counts[0]?.inactive ?? 0,
    inactivatedProperties,
    reactivatedProperties,
  };
}
