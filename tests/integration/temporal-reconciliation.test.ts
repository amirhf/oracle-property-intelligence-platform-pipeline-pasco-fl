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
import {
  DurableConflictError,
  DurableInputError,
} from "../../src/lib/durability-errors.js";
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
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(32);
  expect(await runMigrations(schemaDatabaseUrl)).toEqual([]);
}, 120_000);

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
      observedAt: "2026-08-29T00:00:00.000Z",
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
    const genesisSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const checkpoints = await genesisSql<{ phase: string; rows: number }[]>`
        SELECT phase, sum(row_count)::int AS rows
        FROM oracle_loader_batch_checkpoints
        WHERE source_snapshot_id = ${snapshotA.snapshot.snapshotId}
        GROUP BY phase ORDER BY phase
      `;
      expect(checkpoints).toEqual([
        { phase: "fact_versions", rows: 250 },
        { phase: "materialized_facts", rows: 250 },
        { phase: "materialized_properties", rows: 25 },
        { phase: "property_versions", rows: 25 },
      ]);
      await expect(
        genesisSql`
          UPDATE oracle_loader_batch_checkpoints SET row_count = 1
          WHERE source_snapshot_id = ${snapshotA.snapshot.snapshotId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await genesisSql.end({ timeout: 5 });
    }

    const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "authoritative",
      folios: [...range(1, 24), "TEMP-26"],
      label: "authority-b",
      observedAt: "2026-08-29T01:00:00.000Z",
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
      observedAt: "2026-08-29T02:00:00.000Z",
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
      unchangedProperties: 24,
    });

    const failedLoad = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "authoritative",
      folios: [...range(1, 22), "TEMP-25", "TEMP-26", "TEMP-37"],
      label: "authority-failed-transaction",
      observedAt: "2026-08-29T03:00:00.000Z",
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
    await expect(
      load(failedLoad, (prepared) => {
        prepared.properties[0]!.propertyId =
          "property_00000000000000000000000000000000";
      }),
    ).rejects.toThrow("Projection property identity is malformed");
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
          (SELECT current_snapshot_id FROM oracle_projection_heads
           WHERE scope_id = ${snapshotC.snapshot.coverage.scopeId}) AS current_snapshot_id,
          (SELECT count(*)::int FROM oracle_projection_property_changes
           WHERE snapshot_id = ${failedLoad.snapshot.snapshotId}) AS failed_events,
          (SELECT count(*)::int FROM oracle_property_versions
           WHERE source_snapshot_id = ${failedLoad.snapshot.snapshotId}) AS failed_property,
          (SELECT membership.is_active
           FROM oracle_projection_materialized_properties membership
           JOIN oracle_projection_materializations materialization
             USING (materialization_id)
           JOIN oracle_property_versions version
             ON version.version_id = membership.property_version_id
           WHERE materialization.snapshot_id = ${snapshotC.snapshot.snapshotId}
             AND version.parcel_identifier = 'TEMP-23') AS preserved_active
      `;
      expect(rollback[0]).toEqual({
        current_snapshot_id: snapshotC.snapshot.snapshotId,
        failed_events: 0,
        failed_property: 0,
        preserved_active: true,
      });
      const failedCheckpoints = await rollbackSql<{ count: number }[]>`
        SELECT count(*)::int AS count FROM oracle_loader_batch_checkpoints
        WHERE source_snapshot_id = ${failedLoad.snapshot.snapshotId}
      `;
      expect(failedCheckpoints[0]?.count).toBe(0);
    } finally {
      await rollbackSql.end({ timeout: 5 });
    }

    const incomplete = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["TEMP-02"],
      coverage: "incomplete",
      folios: [...range(1, 20), ...range(27, 31)],
      label: "incomplete-authority",
      observedAt: "2026-08-29T04:00:00.000Z",
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
      folios: [...range(1, 19), "TEMP-24", ...range(32, 36)],
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
      newProperties: 25,
      reactivatedProperties: 0,
      unchangedProperties: 0,
    });
    const sampleGuardSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const guard = await sampleGuardSql<
        {
          authoritative_inactive: boolean;
          current_snapshot_id: string;
          isolated_sample_rows: number;
        }[]
      >`
        SELECT
          (SELECT current_snapshot_id FROM oracle_projection_heads
           WHERE scope_id = ${snapshotA.snapshot.coverage.scopeId}) AS current_snapshot_id,
          (SELECT count(*)::int FROM oracle_sample_property_versions
           WHERE snapshot_id = ${sample.snapshot.snapshotId}
             AND parcel_identifier = 'TEMP-24') AS isolated_sample_rows,
          (SELECT NOT membership.is_active
           FROM oracle_projection_materialized_properties membership
           JOIN oracle_projection_materializations materialization
             USING (materialization_id)
           JOIN oracle_property_versions version
             ON version.version_id = membership.property_version_id
           WHERE materialization.snapshot_id = ${incomplete.snapshot.snapshotId}
             AND version.parcel_identifier = 'TEMP-24') AS authoritative_inactive
      `;
      expect(guard[0]).toEqual({
        authoritative_inactive: true,
        current_snapshot_id: incomplete.snapshot.snapshotId,
        isolated_sample_rows: 1,
      });
    } finally {
      await sampleGuardSql.end({ timeout: 5 });
    }

    const partialReactivation = await createSyntheticLifecycleSnapshot(
      dataDir,
      {
        coordinateMissingFolios: ["TEMP-01"],
        coverage: "incomplete",
        folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
        label: "partial-positive-reactivation",
        observedAt: "2026-08-29T05:00:00.000Z",
        ownerlessFolios: ["TEMP-01"],
        previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
        previousProjectionSnapshotId: incomplete.snapshot.snapshotId,
      },
    );
    await recordRunStarted(schemaDatabaseUrl, partialReactivation.request);
    expect(await load(partialReactivation)).toMatchObject({
      activeProperties: 31,
      changedProperties: 1,
      inactiveProperties: 0,
      inactivatedProperties: 0,
      newProperties: 0,
      reactivatedProperties: 1,
      unchangedProperties: 23,
    });
    const factSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const facts = await factSql<
        {
          missing_availability_facts: number;
          coordinate_facts: number;
          owner_facts: number;
          removed_owner_events: number;
        }[]
      >`
        SELECT
          count(*) FILTER (WHERE fact.fact_type = 'ownership')::int AS owner_facts,
          count(*) FILTER (WHERE fact.fact_type = 'coordinate')::int AS coordinate_facts,
          count(*) FILTER (
            WHERE fact.fact_type = 'availability'
              AND fact.natural_key IN ('coordinates', 'ownership')
          )::int AS missing_availability_facts,
          (SELECT count(*)::int FROM oracle_projection_fact_changes change
           JOIN oracle_property_versions version
             ON version.property_id = change.property_id
           WHERE change.snapshot_id = ${partialReactivation.snapshot.snapshotId}
             AND change.event_type = 'removed'
             AND change.fact_type = 'ownership'
             AND version.parcel_identifier = 'TEMP-01') AS removed_owner_events
        FROM oracle_projection_materialized_facts fact
        JOIN oracle_projection_materializations materialization
          USING (materialization_id)
        JOIN oracle_property_versions version
          ON version.property_id = fact.property_id
        WHERE materialization.snapshot_id = ${partialReactivation.snapshot.snapshotId}
          AND version.parcel_identifier = 'TEMP-01'
      `;
      expect(facts[0]).toEqual({
        coordinate_facts: 1,
        missing_availability_facts: 1,
        owner_facts: 0,
        removed_owner_events: 1,
      });
    } finally {
      await factSql.end({ timeout: 5 });
    }

    for (const invalid of [
      await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "incomplete",
        folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
        label: "partial-missing-predecessor",
        observedAt: "2026-08-29T05:10:00.000Z",
        previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
        previousProjectionSnapshotId: null,
      }),
      await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "incomplete",
        folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
        label: "partial-stale-watermark",
        observedAt: "2026-08-29T04:30:00.000Z",
        previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
        previousProjectionSnapshotId: partialReactivation.snapshot.snapshotId,
      }),
      await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "incomplete",
        folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
        label: "partial-same-time-different-bytes",
        observedAt: "2026-08-29T05:00:00.000Z",
        previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
        previousProjectionSnapshotId: partialReactivation.snapshot.snapshotId,
      }),
    ]) {
      await recordRunStarted(schemaDatabaseUrl, invalid.request);
      await expect(load(invalid)).rejects.toBeInstanceOf(DurableConflictError);
    }

    const wrongAuthority = await createSyntheticLifecycleSnapshot(dataDir, {
      authoritySourceSystem: "pasco_gis",
      coverage: "incomplete",
      folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
      label: "partial-wrong-authority",
      observedAt: "2026-08-29T05:30:00.000Z",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
      previousProjectionSnapshotId: partialReactivation.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, wrongAuthority.request);
    await expect(load(wrongAuthority)).rejects.toBeInstanceOf(
      DurableInputError,
    );

    const staleAuthoritative = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: [...range(1, 23), "TEMP-25", "TEMP-26"],
      label: "stale-authority-after-partial",
      observedAt: "2026-08-29T06:00:00.000Z",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
      previousProjectionSnapshotId: snapshotC.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, staleAuthoritative.request);
    await expect(load(staleAuthoritative)).rejects.toBeInstanceOf(
      DurableConflictError,
    );

    const incompatibleScope = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: [...range(1, 23), "TEMP-25", "TEMP-26"],
      label: "different-scope",
      observedAt: "2026-08-29T05:00:00.000Z",
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
          (SELECT active_count::int FROM oracle_projection_materializations
           WHERE snapshot_id = ${incomplete.snapshot.snapshotId}) AS active,
          (SELECT inactive_count::int FROM oracle_projection_materializations
           WHERE snapshot_id = ${incomplete.snapshot.snapshotId}) AS inactive,
          count(*) FILTER (WHERE event_type = 'new')::int AS new_events,
          count(*) FILTER (WHERE event_type = 'changed')::int AS changed_events,
          count(*) FILTER (WHERE event_type = 'unchanged')::int AS unchanged_events,
          count(*) FILTER (WHERE event_type = 'inactivated')::int AS inactivated_events,
          count(*) FILTER (WHERE event_type = 'reactivated')::int AS reactivated_events
        FROM oracle_projection_property_changes changes
        JOIN oracle_projection_snapshots snapshots USING (snapshot_id)
        WHERE snapshots.scope_id = ${snapshotA.snapshot.coverage.scopeId}
      `;
      expect(lifecycle[0]).toEqual({
        active: 30,
        changed_events: 2,
        inactive: 1,
        inactivated_events: 2,
        new_events: 31,
        reactivated_events: 2,
        unchanged_events: 90,
      });
      const bEffects = await sql<{ effects: number }[]>`
        SELECT count(*)::int AS effects FROM oracle_loader_effects
        WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}
      `;
      expect(bEffects[0]?.effects).toBe(1);
      const boundaries = await sql<
        {
          exact_folio: string;
          is_active: boolean;
          last_event_snapshot_id: string;
        }[]
      >`
        SELECT version.parcel_identifier AS exact_folio,
               membership.is_active,
               (
                 SELECT change.snapshot_id
                 FROM oracle_projection_property_changes change
                 WHERE change.property_id = membership.property_id
                 ORDER BY (
                   SELECT watermark_observed_through
                   FROM oracle_projection_snapshots snapshot
                   WHERE snapshot.snapshot_id = change.snapshot_id
                 ) DESC LIMIT 1
               ) AS last_event_snapshot_id
        FROM oracle_projection_materialized_properties membership
        JOIN oracle_projection_materializations materialization
          USING (materialization_id)
        JOIN oracle_property_versions version
          ON version.version_id = membership.property_version_id
        WHERE materialization.snapshot_id = ${partialReactivation.snapshot.snapshotId}
          AND version.parcel_identifier = ANY(${["TEMP-02", "TEMP-24", "TEMP-25"]})
        ORDER BY version.parcel_identifier
      `;
      expect(boundaries).toEqual([
        {
          exact_folio: "TEMP-02",
          is_active: true,
          last_event_snapshot_id: partialReactivation.snapshot.snapshotId,
        },
        {
          exact_folio: "TEMP-24",
          is_active: true,
          last_event_snapshot_id: partialReactivation.snapshot.snapshotId,
        },
        {
          exact_folio: "TEMP-25",
          is_active: true,
          last_event_snapshot_id: snapshotC.snapshot.snapshotId,
        },
      ]);
      await expect(
        sql`
          UPDATE oracle_projection_property_changes SET reason = 'changed'
          WHERE snapshot_id = ${snapshotA.snapshot.snapshotId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await sql.end({ timeout: 5 });
    }

    const concurrentOptions = {
      coverage: "incomplete" as const,
      folios: [...range(1, 19), "TEMP-24", ...range(27, 31)],
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
      previousProjectionSnapshotId: partialReactivation.snapshot.snapshotId,
    };
    const [concurrentOne, concurrentTwo] = await Promise.all([
      createSyntheticLifecycleSnapshot(dataDir, {
        ...concurrentOptions,
        label: "partial-concurrent-one",
        observedAt: "2026-08-29T06:10:00.000Z",
      }),
      createSyntheticLifecycleSnapshot(dataDir, {
        ...concurrentOptions,
        label: "partial-concurrent-two",
        observedAt: "2026-08-29T06:20:00.000Z",
      }),
    ]);
    await recordRunStarted(schemaDatabaseUrl, concurrentOne.request);
    await recordRunStarted(schemaDatabaseUrl, concurrentTwo.request);
    const concurrent = await Promise.allSettled([
      load(concurrentOne),
      load(concurrentTwo),
    ]);
    expect(
      concurrent.filter((result) => result.status === "fulfilled"),
    ).toHaveLength(1);
    expect(
      concurrent.filter((result) => result.status === "rejected"),
    ).toHaveLength(1);
  }, 600_000);
});
