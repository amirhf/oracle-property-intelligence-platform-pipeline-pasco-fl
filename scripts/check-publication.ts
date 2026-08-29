import "dotenv/config";

import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import postgres from "postgres";

import { loadConfig } from "../services/lib/config.js";
import { deterministicId } from "../src/lib/hash.js";
import { validatePublicationPlan } from "../src/publication/plan.js";

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function collectPropertyIds(value: unknown, result: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectPropertyIds(entry, result);
    return;
  }
  if (value === null || typeof value !== "object") return;
  const record = value as Record<string, unknown>;
  if (typeof record.propertyId === "string") result.add(record.propertyId);
  for (const entry of Object.values(record)) collectPropertyIds(entry, result);
}

const config = loadConfig();
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  "pasco-scale-25000-v1-repeat",
]);
const root = path.join(
  config.dataDir,
  "artifacts",
  "publish",
  "pasco",
  `dry-run-${runId}`,
);
const manifestPath = path.join(root, "open-data", "manifest.json");
const parquetPath = path.join(
  root,
  "query",
  "query-tables",
  "pasco",
  "query-table.parquet",
);
const plan = validatePublicationPlan(
  JSON.parse(
    await readFile(path.join(root, "publication-dry-run-plan.json"), "utf8"),
  ),
);
const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as {
  entries: { propertyId: string }[];
  propertyCount: number;
  selectionHash: string;
};
if (sha256(await readFile(manifestPath)) !== plan.artifacts.manifest.sha256) {
  throw new Error("Publication manifest hash does not match its plan");
}

const fixturePropertyIds = new Set<string>();
for (const filename of (
  await readdir(path.resolve("contracts", "fixtures"))
).sort()) {
  if (!filename.endsWith(".json")) continue;
  collectPropertyIds(
    JSON.parse(
      await readFile(path.resolve("contracts", "fixtures", filename), "utf8"),
    ),
    fixturePropertyIds,
  );
}
const fixtureMatches = manifest.entries.filter((entry) =>
  fixturePropertyIds.has(entry.propertyId),
).length;

const duckdb = await DuckDBInstance.create(":memory:");
const connection = await duckdb.connect();
let parquet: Record<string, string>;
try {
  const reader = await connection.runAndReadAll(`
    SELECT
      count(*)::BIGINT AS rows,
      count(DISTINCT property_id)::BIGINT AS distinct_ids,
      count(*) FILTER (WHERE property_id IS NULL OR property_id = '')::BIGINT AS null_ids,
      count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL)::BIGINT AS coordinate_rows,
      count(*) FILTER (WHERE latitude IS NULL AND longitude IS NULL)::BIGINT AS missing_coordinate_rows,
      count(*) FILTER (WHERE roof_age_basis = 'year_built_proxy')::BIGINT AS proxy_rows,
      count(open_roofing_permit_count)::BIGINT AS non_null_permit_aggregates,
      count(DISTINCT coverage_mode)::BIGINT AS coverage_modes,
      min(coverage_mode) AS coverage_mode,
      count(DISTINCT coverage_scope_id)::BIGINT AS scope_ids,
      count(DISTINCT selection_hash)::BIGINT AS selection_hashes
    FROM read_parquet('${parquetPath.replaceAll("'", "''")}')
  `);
  parquet = reader.getRowObjectsJson()[0] as Record<string, string>;
} finally {
  connection.closeSync();
}

const sql = postgres(config.databaseUrl, { max: 1 });
try {
  const state = await sql<
    {
      approvals: number;
      object_cids: number;
      pending_objects: number;
      plan_id: string;
      state: string;
      ipns_mutations: number;
    }[]
  >`
    SELECT state.plan_id, state.state,
      (SELECT count(*)::int FROM oracle_publication_approvals
       WHERE plan_id = state.plan_id) AS approvals,
      (SELECT count(*)::int FROM oracle_publication_object_effects
       WHERE plan_id = state.plan_id AND status = 'pending') AS pending_objects,
      (SELECT count(*)::int FROM oracle_publication_object_effects
       WHERE plan_id = state.plan_id
         AND (uploaded_cid IS NOT NULL OR verified_cid IS NOT NULL)) AS object_cids,
      (SELECT count(*)::int FROM oracle_publication_ipns_effects
       WHERE plan_id = state.plan_id AND mutation_performed) AS ipns_mutations
    FROM oracle_publication_state state WHERE county = 'pasco'
  `;
  const current = state[0];
  if (
    plan.coverage.mode !== "sample" ||
    manifest.propertyCount !== 25_000 ||
    Number(parquet.rows) !== 25_000 ||
    Number(parquet.distinct_ids) !== 25_000 ||
    Number(parquet.null_ids) !== 0 ||
    Number(parquet.coordinate_rows) !== 24_995 ||
    Number(parquet.missing_coordinate_rows) !== 5 ||
    Number(parquet.proxy_rows) !== 25_000 ||
    Number(parquet.non_null_permit_aggregates) !== 0 ||
    parquet.coverage_mode !== "sample" ||
    Number(parquet.coverage_modes) !== 1 ||
    Number(parquet.scope_ids) !== 1 ||
    Number(parquet.selection_hashes) !== 1 ||
    fixtureMatches !== 0 ||
    plan.approvable ||
    plan.executable ||
    current?.plan_id !== plan.planId ||
    current.state !== "awaiting_configuration" ||
    current.approvals !== 0 ||
    current.object_cids !== 0 ||
    current.ipns_mutations !== 0 ||
    current.pending_objects !== plan.artifacts.objectInventory.length
  ) {
    throw new Error("Publication durability reconciliation failed");
  }
  console.log(
    JSON.stringify(
      {
        activeProperties: plan.counts.activeProperties,
        approvable: plan.approvable,
        coverageMode: plan.coverage.mode,
        distinctPropertyIds: Number(parquet.distinct_ids),
        executable: plan.executable,
        fixtureMatches,
        inactiveProperties: plan.counts.inactiveProperties,
        ipnsMutations: current.ipns_mutations,
        missingCoordinates: Number(parquet.missing_coordinate_rows),
        objectCids: current.object_cids,
        permitAggregateNonNullRows: Number(parquet.non_null_permit_aggregates),
        planId: plan.planId,
        planSha256: plan.planSha256,
        publicationState: current.state,
        selectionHash: manifest.selectionHash,
      },
      null,
      2,
    ),
  );
} finally {
  await sql.end({ timeout: 5 });
}
