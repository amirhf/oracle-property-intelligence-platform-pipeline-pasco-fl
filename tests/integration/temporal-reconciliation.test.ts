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
import type { PreparedPilot } from "../../src/domain/types.js";
import { DurableConflictError } from "../../src/lib/durability-errors.js";
import { verifyPreparedInput } from "../../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parsePreparedPilot,
} from "../../src/workflow/schemas.js";
import {
  createSyntheticLifecycleSnapshot,
  syntheticLoaderIdempotencyKey,
  type SyntheticSnapshot,
} from "../helpers/durability.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `temporal_${process.pid}_${Date.now()}`;
const schemaDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
let dataDir: string;

const range = (start: number, end: number) =>
  Array.from(
    { length: end - start + 1 },
    (_, index) => `TEMP-${(start + index).toString().padStart(2, "0")}`,
  );

async function load(
  snapshot: SyntheticSnapshot,
  mutatePrepared?: (prepared: PreparedPilot) => void,
) {
  const verified = await verifyPreparedInput(
    dataDir,
    snapshot.reference,
    parsePreparedPilot,
    snapshot.snapshot.snapshotId,
  );
  const prepared = structuredClone(verified.prepared);
  mutatePrepared?.(prepared);
  return loadPreparedPilot(schemaDatabaseUrl, snapshot.request, prepared, {
    idempotencyKey: syntheticLoaderIdempotencyKey(
      snapshot.request.workflowId,
      snapshot.reference.preparedInputId,
    ),
    preparedManifest: verified.manifest,
    preparedReference: verified.reference,
    requestSha256: countyIngestRequestSha256(snapshot.request),
    runId: snapshot.request.runId,
    snapshot: verified.snapshot,
  });
}

beforeAll(async () => {
  dataDir = await mkdtemp(path.join(tmpdir(), "prism-temporal-test-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(7);
  expect(await runMigrations(schemaDatabaseUrl)).toEqual([]);
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

describe("temporal property current-state reconciliation", () => {
  it("applies A to B concurrently, replays B, reactivates in C, and gates absence", async () => {
    const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: range(1, 25),
      label: "authority-a",
    });
    expect(snapshotA.snapshot.coverage.mode).toBe("authoritative_complete");
    await recordRunStarted(schemaDatabaseUrl, snapshotA.request);
    const resultA = await load(snapshotA);
    expect(resultA).toMatchObject({
      activeProperties: 25,
      changedProperties: 0,
      inactiveProperties: 0,
      inactivatedProperties: 0,
      newProperties: 25,
      reactivatedProperties: 0,
      unchangedProperties: 0,
    });

    const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "authoritative",
      folios: [...range(1, 24), "TEMP-26"],
      label: "authority-b",
      previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
    });
    expect(snapshotB.snapshot.coverage.scopeId).toBe(
      snapshotA.snapshot.coverage.scopeId,
    );
    await recordRunStarted(schemaDatabaseUrl, snapshotB.request);
    const [resultB, concurrentReplayB] = await Promise.all([
      load(snapshotB),
      load(snapshotB),
    ]);
    const restartedReplayB = await load(snapshotB);
    expect(concurrentReplayB).toEqual(resultB);
    expect(restartedReplayB).toEqual(resultB);
    expect(resultB).toMatchObject({
      activeProperties: 25,
      changedProperties: 1,
      inactiveProperties: 1,
      inactivatedProperties: 1,
      newProperties: 1,
      reactivatedProperties: 0,
      unchangedProperties: 23,
    });

    const snapshotC = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "authoritative",
      folios: [...range(1, 23), "TEMP-25", "TEMP-26"],
      label: "authority-c",
      previousAuthoritativeSnapshotId: snapshotB.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotC.request);
    const resultC = await load(snapshotC);
    expect(resultC).toMatchObject({
      activeProperties: 25,
      changedProperties: 0,
      inactiveProperties: 1,
      inactivatedProperties: 1,
      newProperties: 0,
      reactivatedProperties: 1,
      unchangedProperties: 25,
    });

    const failedLoad = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "authoritative",
      folios: [...range(1, 22), "TEMP-25", "TEMP-26", "TEMP-37"],
      label: "authority-failed-transaction",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, failedLoad.request);
    await expect(
      load(failedLoad, (prepared) => {
        const last = prepared.properties.at(-1);
        if (!last?.coordinates) throw new Error("Synthetic coordinate missing");
        last.coordinates.latitude = 999;
      }),
    ).rejects.toThrow();
    const rollbackSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const rollback = await rollbackSql<
        {
          current_snapshot_id: string;
          failed_events: number;
          failed_property: number;
          preserved_active: boolean;
        }[]
      >`
        SELECT
          (SELECT current_snapshot_id FROM oracle_authoritative_scope_heads
           WHERE scope_id = ${snapshotC.snapshot.coverage.scopeId}) AS current_snapshot_id,
          (SELECT count(*)::int FROM oracle_property_lifecycle_events
           WHERE snapshot_id = ${failedLoad.snapshot.snapshotId}) AS failed_events,
          (SELECT count(*)::int FROM oracle_properties
           WHERE property_id = ${failedLoad.prepared.properties.at(-1)!.propertyId}) AS failed_property,
          (SELECT is_active FROM oracle_properties
           WHERE exact_folio = 'TEMP-23') AS preserved_active
      `;
      expect(rollback[0]).toEqual({
        current_snapshot_id: snapshotC.snapshot.snapshotId,
        failed_events: 0,
        failed_property: 0,
        preserved_active: true,
      });
    } finally {
      await rollbackSql.end({ timeout: 5 });
    }

    const incomplete = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "incomplete",
      folios: [...range(1, 20), ...range(27, 31)],
      label: "incomplete-authority",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
    });
    expect(incomplete.snapshot.coverage.mode).toBe("partial");
    expect(incomplete.snapshot.coverage.completeness.result).toBe("failed");
    await recordRunStarted(schemaDatabaseUrl, incomplete.request);
    const incompleteResult = await load(incomplete);
    expect(incompleteResult).toMatchObject({
      activeProperties: 30,
      changedProperties: 0,
      inactiveProperties: 1,
      inactivatedProperties: 0,
      newProperties: 5,
      reactivatedProperties: 0,
      unchangedProperties: 20,
    });

    const sample = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "sample",
      folios: [...range(1, 20), ...range(32, 36)],
      label: "bounded-sample",
    });
    expect(sample.snapshot.coverage.mode).toBe("sample");
    expect(sample.snapshot.coverage.scopeId).not.toBe(
      snapshotA.snapshot.coverage.scopeId,
    );
    await recordRunStarted(schemaDatabaseUrl, sample.request);
    const sampleResult = await load(sample);
    expect(sampleResult).toMatchObject({
      activeProperties: 25,
      changedProperties: 0,
      inactiveProperties: 0,
      inactivatedProperties: 0,
      newProperties: 5,
      reactivatedProperties: 0,
      unchangedProperties: 20,
    });

    const incompatibleScope = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: [...range(1, 23), "TEMP-25", "TEMP-26"],
      label: "different-scope",
      membershipRule: "different authoritative parcel membership v1",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, incompatibleScope.request);
    await expect(load(incompatibleScope)).rejects.toBeInstanceOf(
      DurableConflictError,
    );

    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const lifecycle = await sql<
        {
          active: number;
          changed_events: number;
          inactive: number;
          inactivated_events: number;
          new_events: number;
          reactivated_events: number;
          unchanged_events: number;
        }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_properties WHERE is_active) AS active,
          (SELECT count(*)::int FROM oracle_properties WHERE NOT is_active) AS inactive,
          count(*) FILTER (WHERE event_type = 'new')::int AS new_events,
          count(*) FILTER (WHERE event_type = 'changed')::int AS changed_events,
          count(*) FILTER (WHERE event_type = 'unchanged')::int AS unchanged_events,
          count(*) FILTER (WHERE event_type = 'inactivated')::int AS inactivated_events,
          count(*) FILTER (WHERE event_type = 'reactivated')::int AS reactivated_events
        FROM oracle_property_lifecycle_events
        WHERE scope_id = ${snapshotA.snapshot.coverage.scopeId}
      `;
      expect(lifecycle[0]).toEqual({
        active: 35,
        changed_events: 1,
        inactive: 1,
        inactivated_events: 2,
        new_events: 31,
        reactivated_events: 1,
        unchanged_events: 67,
      });
      const bEffects = await sql<{ effects: number }[]>`
        SELECT count(*)::int AS effects FROM oracle_loader_effects
        WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}
      `;
      expect(bEffects[0]?.effects).toBe(1);
      const boundaries = await sql<
        {
          exact_folio: string;
          lifecycle_status: "active" | "inactive";
          valid_from_snapshot_id: string;
          valid_to_snapshot_id: string | null;
        }[]
      >`
        SELECT property.exact_folio, state.lifecycle_status,
               state.valid_from_snapshot_id, state.valid_to_snapshot_id
        FROM oracle_property_scope_state state
        JOIN oracle_properties property USING (property_id)
        WHERE state.scope_id = ${snapshotA.snapshot.coverage.scopeId}
          AND property.exact_folio = ANY(${["TEMP-02", "TEMP-24", "TEMP-25"]})
        ORDER BY property.exact_folio
      `;
      expect(boundaries).toEqual([
        {
          exact_folio: "TEMP-02",
          lifecycle_status: "active",
          valid_from_snapshot_id: snapshotB.snapshot.snapshotId,
          valid_to_snapshot_id: null,
        },
        {
          exact_folio: "TEMP-24",
          lifecycle_status: "inactive",
          valid_from_snapshot_id: snapshotA.snapshot.snapshotId,
          valid_to_snapshot_id: snapshotC.snapshot.snapshotId,
        },
        {
          exact_folio: "TEMP-25",
          lifecycle_status: "active",
          valid_from_snapshot_id: snapshotC.snapshot.snapshotId,
          valid_to_snapshot_id: null,
        },
      ]);
      await expect(
        sql`
          UPDATE oracle_property_lifecycle_events SET reason = 'changed'
          WHERE snapshot_id = ${snapshotA.snapshot.snapshotId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await sql.end({ timeout: 5 });
    }
  }, 60_000);
});
