import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  loadPreparedPilot,
  recordRunStarted,
} from "../../src/db/pilot-repository.js";
import { runMigrations } from "../../src/db/migrations.js";
import { DurableConflictError } from "../../src/lib/durability-errors.js";
import { verifyPreparedInput } from "../../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parsePreparedPilot,
} from "../../src/workflow/schemas.js";
import {
  createSyntheticSnapshot,
  syntheticLoaderIdempotencyKey,
  type SyntheticSnapshot,
} from "../helpers/durability.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `durability_${process.pid}_${Date.now()}`;
const schemaDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
let dataDir: string;
let snapshotA: SyntheticSnapshot;
let snapshotB: SyntheticSnapshot;

async function load(snapshot: SyntheticSnapshot) {
  const verified = await verifyPreparedInput(
    dataDir,
    snapshot.reference,
    parsePreparedPilot,
    snapshot.snapshot.snapshotId,
  );
  return loadPreparedPilot(
    schemaDatabaseUrl,
    snapshot.request,
    verified.prepared,
    {
      idempotencyKey: syntheticLoaderIdempotencyKey(
        snapshot.request.workflowId,
        snapshot.reference.preparedInputId,
      ),
      preparedManifest: verified.manifest,
      preparedReference: verified.reference,
      requestSha256: countyIngestRequestSha256(snapshot.request),
      runId: snapshot.request.runId,
      snapshot: verified.snapshot,
    },
  );
}

beforeAll(async () => {
  dataDir = await mkdtemp(path.join(tmpdir(), "prism-loader-test-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(5);
  expect(await runMigrations(schemaDatabaseUrl)).toEqual([]);
  snapshotA = await createSyntheticSnapshot(dataDir, "a");
  snapshotB = await createSyntheticSnapshot(dataDir, "b");
}, 30_000);

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
    await rm(dataDir, { force: true, recursive: true });
  }
});

describe("Loader/pasco durability", () => {
  it("serializes concurrent Snapshot A calls and returns the stored replay", async () => {
    await recordRunStarted(schemaDatabaseUrl, snapshotA.request);
    const [first, concurrentReplay] = await Promise.all([
      load(snapshotA),
      load(snapshotA),
    ]);
    const restartedReplay = await load(snapshotA);

    expect(concurrentReplay).toEqual(first);
    expect(restartedReplay).toEqual(first);
    expect(first).toMatchObject({
      acceptedProperties: 25,
      changedProperties: 0,
      duplicateProperties: 0,
      newProperties: 25,
      unchangedProperties: 0,
    });

    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const counts = await sql<
        {
          completed_effects: number;
          hashed_results: number;
          properties: number;
          reconciliations: number;
        }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_loader_effects WHERE status = 'completed') AS completed_effects,
          (SELECT count(*)::int FROM oracle_loader_effects WHERE result_sha256 ~ '^[a-f0-9]{64}$') AS hashed_results,
          (SELECT count(*)::int FROM oracle_properties) AS properties,
          (SELECT count(*)::int FROM oracle_reconciliation_outcomes) AS reconciliations
      `;
      expect(counts[0]).toEqual({
        completed_effects: 1,
        hashed_results: 1,
        properties: 25,
        reconciliations: 4,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }
  }, 30_000);

  it("rejects a conflicting Loader payload for Snapshot A", async () => {
    const verifiedB = await verifyPreparedInput(
      dataDir,
      snapshotB.reference,
      parsePreparedPilot,
      snapshotB.snapshot.snapshotId,
    );
    await expect(
      loadPreparedPilot(
        schemaDatabaseUrl,
        snapshotA.request,
        verifiedB.prepared,
        {
          idempotencyKey: syntheticLoaderIdempotencyKey(
            snapshotA.request.workflowId,
            snapshotA.reference.preparedInputId,
          ),
          preparedManifest: verifiedB.manifest,
          preparedReference: verifiedB.reference,
          requestSha256: countyIngestRequestSha256(snapshotA.request),
          runId: snapshotA.request.runId,
          snapshot: verifiedB.snapshot,
        },
      ),
    ).rejects.toBeInstanceOf(DurableConflictError);
    await expect(
      recordRunStarted(schemaDatabaseUrl, {
        ...snapshotA.request,
        sampleSeed: "conflicting-synthetic-seed",
      }),
    ).rejects.toBeInstanceOf(DurableConflictError);
  });

  it("applies Snapshot B once without deleting Snapshot A-only state", async () => {
    await recordRunStarted(schemaDatabaseUrl, snapshotB.request);
    const initial = await load(snapshotB);
    const replay = await load(snapshotB);
    expect(replay).toEqual(initial);
    expect(initial).toMatchObject({
      acceptedProperties: 25,
      changedProperties: 1,
      duplicateProperties: 0,
      newProperties: 1,
      unchangedProperties: 23,
    });

    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const counts = await sql<
        {
          availability: number;
          coordinates: number;
          distinct_properties: number;
          effects: number;
          ownerships: number;
          properties: number;
          reconciliations: number;
          roof_signals: number;
          snapshots: number;
        }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_loader_effects) AS effects,
          (SELECT count(*)::int FROM oracle_source_snapshots) AS snapshots,
          (SELECT count(*)::int FROM oracle_properties) AS properties,
          (SELECT count(DISTINCT property_id)::int FROM oracle_properties) AS distinct_properties,
          (SELECT count(*)::int FROM oracle_coordinates) AS coordinates,
          (SELECT count(*)::int FROM oracle_ownerships) AS ownerships,
          (SELECT count(*)::int FROM oracle_roof_signals) AS roof_signals,
          (SELECT count(*)::int FROM oracle_property_availability) AS availability,
          (SELECT count(*)::int FROM oracle_reconciliation_outcomes) AS reconciliations
      `;
      expect(counts[0]).toEqual({
        availability: 156,
        coordinates: 26,
        distinct_properties: 26,
        effects: 2,
        ownerships: 26,
        properties: 26,
        reconciliations: 8,
        roof_signals: 26,
        snapshots: 2,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
