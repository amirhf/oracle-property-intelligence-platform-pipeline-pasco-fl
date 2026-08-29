import type postgres from "postgres";

import type { PilotRunSummary } from "../domain/types.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableConflictError } from "../lib/durability-errors.js";
import { sha256 } from "../lib/hash.js";
import type {
  PreparedInputManifest,
  PreparedInputReference,
  SourceSnapshotManifest,
} from "../snapshot/model.js";

export interface LoaderDurabilityContext {
  idempotencyKey: string;
  preparedManifest: PreparedInputManifest;
  preparedReference: PreparedInputReference;
  requestSha256: string;
  runId: string;
  snapshot: SourceSnapshotManifest;
}

// The two-key PostgreSQL advisory-lock namespace is versioned and stable:
// signedInt32(SHA-256("prism-oracle-loader-v1")[0..4]),
// signedInt32(SHA-256("pasco")[0..4]).
// Current tuple: (-1827269144, -1784306027).
export function pascoLoaderAdvisoryLockKey(): readonly [number, number] {
  const signedInt32 = (hex: string) => {
    const value = Number.parseInt(hex.slice(0, 8), 16);
    return value > 0x7fffffff ? value - 0x1_0000_0000 : value;
  };
  return [
    signedInt32(sha256("prism-oracle-loader-v1")),
    signedInt32(sha256("pasco")),
  ] as const;
}

function conflict(label: string, id: string): never {
  throw new DurableConflictError(`${label} identity conflict (${id})`);
}

export async function beginLoaderEffect(
  transaction: postgres.TransactionSql,
  context: LoaderDurabilityContext,
): Promise<PilotRunSummary | null> {
  const [namespaceKey, countyKey] = pascoLoaderAdvisoryLockKey();
  await transaction`SELECT pg_advisory_xact_lock(${namespaceKey}, ${countyKey})`;

  const snapshot = context.snapshot;
  const prepared = context.preparedManifest;
  const reference = context.preparedReference;

  await transaction`
    INSERT INTO oracle_source_snapshots (
      snapshot_id, county, source_set_id, manifest_version, manifest_sha256,
      observation_start, observation_end, parser_version, transform_version,
      canonical_schema_sha256, sampling, source_objects, manifest_created_at
    ) VALUES (
      ${snapshot.snapshotId}, ${snapshot.county}, ${snapshot.sourceSetId},
      ${snapshot.manifestVersion}, ${prepared.snapshotManifest.sha256},
      ${snapshot.observationWindow.start}, ${snapshot.observationWindow.end},
      ${snapshot.parserVersion}, ${snapshot.transformVersion},
      ${snapshot.canonicalSchemaSha256},
      ${transaction.json(snapshot.sampling as unknown as postgres.JSONValue)},
      ${transaction.json(snapshot.sourceObjects as unknown as postgres.JSONValue)},
      ${snapshot.createdAt}
    )
    ON CONFLICT (snapshot_id) DO NOTHING
  `;
  const snapshotRows = await transaction<
    { manifest_sha256: string; source_set_id: string }[]
  >`
    SELECT manifest_sha256, source_set_id
    FROM oracle_source_snapshots WHERE snapshot_id = ${snapshot.snapshotId}
  `;
  if (
    snapshotRows[0]?.manifest_sha256 !== prepared.snapshotManifest.sha256 ||
    snapshotRows[0]?.source_set_id !== snapshot.sourceSetId
  ) {
    conflict("source snapshot", snapshot.snapshotId);
  }

  await transaction`
    INSERT INTO oracle_prepared_inputs (
      prepared_input_id, snapshot_id, input_kind, manifest_sha256,
      prepared_sha256, prepared_byte_size, manifest_relative_path,
      prepared_relative_path, selected_record_sha256, selection_size,
      manifest_created_at
    ) VALUES (
      ${prepared.preparedInputId}, ${prepared.snapshotId}, ${prepared.kind},
      ${reference.manifest.sha256}, ${prepared.prepared.sha256},
      ${prepared.prepared.byteSize}, ${reference.manifest.relativePath},
      ${prepared.prepared.relativePath},
      ${prepared.sampling.selectedRecordSha256},
      ${prepared.sampling.selectionSize}, ${prepared.createdAt}
    )
    ON CONFLICT (prepared_input_id) DO NOTHING
  `;
  const preparedRows = await transaction<
    { manifest_sha256: string; prepared_sha256: string; snapshot_id: string }[]
  >`
    SELECT manifest_sha256, prepared_sha256, snapshot_id
    FROM oracle_prepared_inputs
    WHERE prepared_input_id = ${prepared.preparedInputId}
  `;
  if (
    preparedRows[0]?.manifest_sha256 !== reference.manifest.sha256 ||
    preparedRows[0]?.prepared_sha256 !== prepared.prepared.sha256 ||
    preparedRows[0]?.snapshot_id !== snapshot.snapshotId
  ) {
    conflict("prepared input", prepared.preparedInputId);
  }

  const runRows = await transaction<
    { request_sha256: string | null; snapshot_id: string | null }[]
  >`
    SELECT request_sha256, snapshot_id FROM oracle_pipeline_runs
    WHERE run_id = ${context.runId} FOR UPDATE
  `;
  if (runRows.length !== 1) conflict("pipeline run", context.runId);
  if (
    (runRows[0]!.request_sha256 !== null &&
      runRows[0]!.request_sha256 !== context.requestSha256) ||
    (runRows[0]!.snapshot_id !== null &&
      runRows[0]!.snapshot_id !== snapshot.snapshotId)
  ) {
    conflict("pipeline run", context.runId);
  }
  await transaction`
    UPDATE oracle_pipeline_runs SET
      snapshot_id = ${snapshot.snapshotId},
      request_sha256 = ${context.requestSha256},
      window_start = ${snapshot.observationWindow.start},
      window_end = ${snapshot.observationWindow.end}
    WHERE run_id = ${context.runId}
  `;

  const effects = await transaction<
    {
      prepared_input_id: string;
      request_sha256: string;
      result_payload: PilotRunSummary | null;
      result_sha256: string | null;
      snapshot_id: string;
      status: "applying" | "completed";
    }[]
  >`
    SELECT request_sha256, snapshot_id, prepared_input_id, status,
           result_payload, result_sha256
    FROM oracle_loader_effects
    WHERE idempotency_key = ${context.idempotencyKey}
  `;
  const existing = effects[0];
  if (existing) {
    if (
      existing.request_sha256 !== context.requestSha256 ||
      existing.snapshot_id !== snapshot.snapshotId ||
      existing.prepared_input_id !== prepared.preparedInputId
    ) {
      conflict("Loader idempotency", context.idempotencyKey);
    }
    if (existing.status !== "completed" || existing.result_payload === null) {
      conflict("Loader incomplete effect", context.idempotencyKey);
    }
    if (
      canonicalJsonSha256(existing.result_payload) !== existing.result_sha256
    ) {
      conflict("Loader stored result", context.idempotencyKey);
    }
    return existing.result_payload;
  }
  await transaction`
    INSERT INTO oracle_loader_effects (
      idempotency_key, request_sha256, run_id, snapshot_id,
      prepared_input_id, status
    ) VALUES (
      ${context.idempotencyKey}, ${context.requestSha256}, ${context.runId},
      ${snapshot.snapshotId}, ${prepared.preparedInputId}, 'applying'
    )
  `;
  return null;
}

export async function completeLoaderEffect(
  transaction: postgres.TransactionSql,
  context: LoaderDurabilityContext,
  result: PilotRunSummary,
): Promise<PilotRunSummary> {
  const resultSha256 = canonicalJsonSha256(result);
  const updated = await transaction`
    UPDATE oracle_loader_effects SET
      status = 'completed',
      result_payload = ${transaction.json(
        result as unknown as postgres.JSONValue,
      )},
      result_sha256 = ${resultSha256},
      completed_at = now()
    WHERE idempotency_key = ${context.idempotencyKey}
      AND request_sha256 = ${context.requestSha256}
      AND status = 'applying'
  `;
  if (updated.count !== 1) {
    conflict("Loader completion", context.idempotencyKey);
  }
  return result;
}
