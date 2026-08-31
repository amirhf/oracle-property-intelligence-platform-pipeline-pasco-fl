import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";

import postgres from "postgres";
import { afterAll, describe, expect, it } from "vitest";

import { runMigrations } from "../../src/db/migrations.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const migrationDir = path.resolve("migrations");
const createdSchemas: string[] = [];

function databaseUrl(schema: string): string {
  return `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schema}`;
}

async function createSchema(label: string): Promise<string> {
  const schema = `upgrade_${label}_${process.pid}_${createdSchemas.length}`;
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schema}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  createdSchemas.push(schema);
  return schema;
}

async function applyMigrationRange(
  url: string,
  first: number,
  last: number,
): Promise<void> {
  const sql = postgres(url, { max: 1 });
  try {
    await sql`
      CREATE TABLE IF NOT EXISTS oracle_schema_migrations (
        filename text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `;
    for (let number = first; number <= last; number += 1) {
      const prefix = `${number.toString().padStart(3, "0")}_`;
      const filenames = [
        "001_pasco_pilot.sql",
        "002_normalize_run_json.sql",
        "003_property_availability.sql",
        "004_scaled_runs_and_publication.sql",
        "005_snapshot_loader_durability.sql",
        "006_temporal_current_state.sql",
        "007_classify_legacy_run_coverage.sql",
        "008_publication_durability.sql",
        "009_graph_projection_ipns_intent.sql",
        "010_publication_stop_gate_repair.sql",
        "011_assessment_publication_guards.sql",
        "012_strict_recovery_evidence.sql",
        "013_null_safe_recovery_receipts.sql",
        "014_candidate_demo_publication.sql",
        "015_candidate_demo_bootstrap_cid.sql",
        "016_candidate_demo_resolution_evidence.sql",
        "017_candidate_demo_filebase_gateway.sql",
        "018_candidate_demo_resolver_policy.sql",
        "019_candidate_signed_ipns_observation.sql",
        "020_candidate_delegated_resolver_policy.sql",
        "021_candidate_delegated_completion.sql",
        "022_owner_authoritative_ingestion.sql",
        "023_contractor_source_staging.sql",
        "024_contractor_staging_hardening.sql",
        "025_candidate_source_snapshot_demo.sql",
        "026_candidate_source_snapshot_upload_recovery.sql",
        "027_candidate_source_snapshot_approval_binding.sql",
        "028_candidate_source_snapshot_completion.sql",
        "029_candidate_source_snapshot_request_categories.sql",
        "030_candidate_source_snapshot_ipns_crash_recovery.sql",
        "031_candidate_source_snapshot_approval_before_remote.sql",
      ];
      const filename = filenames.find((candidate) =>
        candidate.startsWith(prefix),
      );
      if (!filename) throw new Error(`Missing migration ${prefix}`);
      const body = await readFile(path.join(migrationDir, filename), "utf8");
      await sql.begin(async (transaction) => {
        await transaction.unsafe(body);
        await transaction`
          INSERT INTO oracle_schema_migrations (filename) VALUES (${filename})
        `;
      });
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function seedLegacyPlan(url: string, state: string): Promise<string> {
  const sql = postgres(url, { max: 1 });
  const digest = createHash("sha256").update(state).digest("hex");
  const runId = `run_${digest.slice(0, 32)}`;
  const planId = `plan_${digest.slice(16, 48)}`;
  const planSha256 = digest;
  try {
    await sql`
      INSERT INTO oracle_pipeline_runs (
        run_id, workflow_id, county, sample_algorithm, sample_seed,
        window_start, window_end, as_of, status, selection_size,
        completed_at, coverage_mode
      ) VALUES (
        ${runId}, ${`legacy-${state}`}, 'pasco', 'legacy-sample', 'legacy',
        '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z',
        '2026-01-01T00:00:00.000Z', 'completed', 1,
        '2026-01-01T00:00:00.000Z', 'sample'
      )
    `;
    await sql`
      INSERT INTO oracle_publication_plans (
        plan_id, plan_sha256, plan_version, county, run_id, snapshot_id,
        coverage_mode, scope_id, approvable, executable, plan_payload,
        generated_at
      ) VALUES (
        ${planId}, ${planSha256}, '1.0.0', 'pasco', ${runId}, null,
        'sample', ${`scope_${digest.slice(32)}`},
        ${["awaiting_approval", "approved", "executing", "completed"].includes(state)},
        ${["awaiting_approval", "approved", "executing", "completed"].includes(state)},
        ${sql.json({ version: "1.0.0" })}, '2026-01-01T00:00:00.000Z'
      )
    `;
    await sql`
      INSERT INTO oracle_publication_state (
        county, plan_id, plan_sha256, state, revision, terminal_reason
      ) VALUES (
        'pasco', ${planId}, ${planSha256}, ${state}, 1,
        ${state === "failed_terminal" ? "legacy_local_failure" : null}
      )
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
  return planId;
}

async function schemaSignature(url: string): Promise<readonly unknown[]> {
  const sql = postgres(url, { max: 1 });
  try {
    return await sql`
      SELECT table_name, column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name LIKE 'oracle_%'
      ORDER BY table_name, ordinal_position
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function receiptGuardSignature(url: string): Promise<readonly unknown[]> {
  const sql = postgres(url, { max: 1 });
  try {
    return await sql`
      SELECT 'function' AS object_type,
             procedure.proname AS object_name,
             pg_get_function_arguments(procedure.oid) AS arguments,
             procedure.prosrc AS definition
      FROM pg_proc procedure
      JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
      WHERE namespace.nspname = current_schema()
        AND procedure.proname IN (
          'oracle_ipns_receipt_http_status_between',
          'oracle_ipns_receipt_semantics_are_valid',
          'oracle_guard_ipns_resolution_cycle_receipt_matrix'
        )
      UNION ALL
      SELECT 'trigger' AS object_type,
             trigger.tgname AS object_name,
             '' AS arguments,
             procedure.proname AS definition
      FROM pg_trigger trigger
      JOIN pg_class relation ON relation.oid = trigger.tgrelid
      JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
      JOIN pg_proc procedure ON procedure.oid = trigger.tgfoid
      WHERE namespace.nspname = current_schema()
        AND NOT trigger.tgisinternal
        AND trigger.tgname =
          'oracle_ipns_resolution_cycle_null_safe_receipt_guard'
      ORDER BY object_type, object_name
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    for (const schema of createdSchemas) {
      await admin.unsafe(`DROP SCHEMA ${schema} CASCADE`);
    }
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("migration 010 drift-safe legacy publication policy", () => {
  const nonExecuted = [
    "prepared",
    "validated",
    "awaiting_configuration",
    "awaiting_approval",
    "failed_terminal",
  ] as const;
  for (const state of nonExecuted) {
    it(`invalidates non-executed legacy state ${state} without synthesizing a graph`, async () => {
      const schema = await createSchema(state);
      const url = databaseUrl(schema);
      await applyMigrationRange(url, 1, 8);
      await seedLegacyPlan(url, state);
      await applyMigrationRange(url, 9, 10);
      const sql = postgres(url, { max: 1 });
      try {
        const rows = await sql<
          { graph_objects: number; invalidations: number; state: string }[]
        >`
          SELECT
            (SELECT count(*)::int FROM oracle_publication_graph_objects) AS graph_objects,
            (SELECT count(*)::int FROM oracle_publication_legacy_invalidations) AS invalidations,
            (SELECT state FROM oracle_publication_state WHERE county = 'pasco') AS state
        `;
        expect(rows[0]).toEqual({
          graph_objects: 0,
          invalidations: 1,
          state: "failed_terminal",
        });
      } finally {
        await sql.end({ timeout: 5 });
      }
      expect(await runMigrations(url)).toEqual([
        "011_assessment_publication_guards.sql",
        "012_strict_recovery_evidence.sql",
        "013_null_safe_recovery_receipts.sql",
        "014_candidate_demo_publication.sql",
        "015_candidate_demo_bootstrap_cid.sql",
        "016_candidate_demo_resolution_evidence.sql",
        "017_candidate_demo_filebase_gateway.sql",
        "018_candidate_demo_resolver_policy.sql",
        "019_candidate_signed_ipns_observation.sql",
        "020_candidate_delegated_resolver_policy.sql",
        "021_candidate_delegated_completion.sql",
        "022_owner_authoritative_ingestion.sql",
        "023_contractor_source_staging.sql",
        "024_contractor_staging_hardening.sql",
        "025_candidate_source_snapshot_demo.sql",
        "026_candidate_source_snapshot_upload_recovery.sql",
        "027_candidate_source_snapshot_approval_binding.sql",
        "028_candidate_source_snapshot_completion.sql",
        "029_candidate_source_snapshot_request_categories.sql",
        "030_candidate_source_snapshot_ipns_crash_recovery.sql",
        "031_candidate_source_snapshot_approval_before_remote.sql",
      ]);
      expect(await runMigrations(url)).toEqual([]);
    });
  }

  for (const state of ["approved", "executing", "completed"] as const) {
    it(`blocks legacy state ${state} before any schema repair`, async () => {
      const schema = await createSchema(state);
      const url = databaseUrl(schema);
      await applyMigrationRange(url, 1, 8);
      await seedLegacyPlan(url, state);
      await applyMigrationRange(url, 9, 9);
      await expect(applyMigrationRange(url, 10, 10)).rejects.toThrow(
        "legacy v1.0 publication has approval or external-effect evidence",
      );
      const sql = postgres(url, { max: 1 });
      try {
        const applied = await sql<{ applied: number }[]>`
          SELECT count(*)::int AS applied FROM oracle_schema_migrations
          WHERE filename = '010_publication_stop_gate_repair.sql'
        `;
        expect(applied[0]?.applied).toBe(0);
      } finally {
        await sql.end({ timeout: 5 });
      }
    });
  }

  for (const evidence of ["uploaded_object", "resolved_prior"] as const) {
    it(`blocks non-executed legacy state with ${evidence} evidence`, async () => {
      const schema = await createSchema(evidence);
      const url = databaseUrl(schema);
      await applyMigrationRange(url, 1, 8);
      const planId = await seedLegacyPlan(url, "prepared");
      const sql = postgres(url, { max: 1 });
      try {
        if (evidence === "uploaded_object") {
          await sql`
            INSERT INTO oracle_publication_object_effects (
              plan_id, domain, object_key, expected_sha256,
              expected_byte_size, status, uploaded_cid
            ) VALUES (
              ${planId}, 'open_data', 'index.json', ${"a".repeat(64)},
              1, 'uploaded', 'QmdVN4PkHDK1i6UVAqE9r9tM9AtZnh6YcQ6144VESH2z3u'
            )
          `;
        } else {
          await sql`
            INSERT INTO oracle_publication_ipns_effects (
              plan_id, domain, ipns_label, prior_cid, status
            ) VALUES (
              ${planId}, 'open_data', 'legacy-open-data',
              'QmdVN4PkHDK1i6UVAqE9r9tM9AtZnh6YcQ6144VESH2z3u', 'pending'
            )
          `;
        }
      } finally {
        await sql.end({ timeout: 5 });
      }
      await applyMigrationRange(url, 9, 9);
      await expect(applyMigrationRange(url, 10, 10)).rejects.toThrow(
        "legacy v1.0 publication has approval or external-effect evidence",
      );
    });
  }

  it("converges the supported local 026→031 path and a fresh 001–031 install", async () => {
    const upgradeSchema = await createSchema("converge_upgrade");
    const freshSchema = await createSchema("converge_fresh");
    const upgradeUrl = databaseUrl(upgradeSchema);
    const freshUrl = databaseUrl(freshSchema);
    await applyMigrationRange(upgradeUrl, 1, 26);
    await applyMigrationRange(upgradeUrl, 27, 31);
    await applyMigrationRange(freshUrl, 1, 31);
    expect(await schemaSignature(upgradeUrl)).toEqual(
      await schemaSignature(freshUrl),
    );
    expect(await receiptGuardSignature(upgradeUrl)).toEqual(
      await receiptGuardSignature(freshUrl),
    );
    expect(await runMigrations(upgradeUrl)).toEqual([]);
    expect(await runMigrations(freshUrl)).toEqual([]);
  });

  it("fails closed instead of synthesizing evidence for populated migration-023 staging", async () => {
    const schema = await createSchema("contractor_populated");
    const url = databaseUrl(schema);
    await applyMigrationRange(url, 1, 23);
    const sql = postgres(url, { max: 1 });
    const state = createHash("sha256")
      .update("contractor-populated")
      .digest("hex");
    const runId = `run_${state.slice(0, 32)}`;
    const sourceSha = createHash("sha256").update("source").digest("hex");
    const termsSha = createHash("sha256").update("terms").digest("hex");
    try {
      await sql`
        INSERT INTO oracle_pipeline_runs (
          run_id, workflow_id, county, sample_algorithm, sample_seed,
          window_start, window_end, as_of, status, selection_size, coverage_mode
        ) VALUES (
          ${runId}, 'synthetic-contractor-upgrade', 'pasco', 'synthetic',
          'synthetic', '2026-08-30T00:00:00.000Z',
          '2026-08-30T00:00:00.000Z', '2026-08-30T00:00:00.000Z',
          'completed', 1, 'sample'
        )
      `;
      await sql`
        INSERT INTO oracle_contractor_source_datasets (
          dataset_id, source_run_id, provider, source_classification,
          acquisition_method, coverage_mode, coverage_geography,
          category_filters, license_terms_status,
          license_terms_evidence_sha256,
          license_terms_evidence_relative_path,
          license_terms_evidence_byte_size, source_file_relative_path,
          source_file_byte_size, source_file_sha256, observation_status,
          observation_start, observation_end, retrieval_status, retrieved_at,
          parser_version, transform_version, manifest_sha256,
          manifest_payload, source_count, parsed_count, accepted_count,
          rejected_count, duplicate_count
        ) VALUES (
          ${`contractordataset_${state.slice(0, 32)}`}, ${runId},
          'better_business_bureau', 'third_party', 'owner_supplied_file',
          'partial', 'synthetic', ${sql.json([])}, 'verified_compatible',
          ${termsSha}, 'contractors/terms.txt', 1,
          'contractors/source.jsonl', 1, ${sourceSha}, 'recorded',
          '2026-08-30T00:00:00.000Z', '2026-08-30T00:00:00.000Z',
          'recorded', '2026-08-30T00:00:00.000Z',
          'contractor-jsonl-v1', 'contractor-identity-match-v1', ${state},
          ${sql.json({
            coverageMode: "partial",
            licenseTerms: {
              evidenceFile: {
                byteSize: 1,
                relativePath: "contractors/terms.txt",
                sha256: termsSha,
              },
              evidenceSha256: termsSha,
            },
            provider: "better_business_bureau",
            sourceClassification: "third_party",
            sourceFile: {
              byteSize: 1,
              relativePath: "contractors/source.jsonl",
              sha256: sourceSha,
            },
          })}, 1, 1, 1, 0, 0
        )
      `;
    } finally {
      await sql.end({ timeout: 5 });
    }
    await expect(applyMigrationRange(url, 24, 24)).rejects.toThrow(
      "requires empty legacy table oracle_contractor_source_datasets",
    );
    const verify = postgres(url, { max: 1 });
    try {
      const rows = await verify<{ applied: number; datasets: number }[]>`
        SELECT
          (SELECT count(*)::int FROM oracle_schema_migrations
           WHERE filename = '024_contractor_staging_hardening.sql') AS applied,
          (SELECT count(*)::int FROM oracle_contractor_source_datasets)
            AS datasets
      `;
      expect(rows[0]).toEqual({ applied: 0, datasets: 1 });
    } finally {
      await verify.end({ timeout: 5 });
    }
  });
});
