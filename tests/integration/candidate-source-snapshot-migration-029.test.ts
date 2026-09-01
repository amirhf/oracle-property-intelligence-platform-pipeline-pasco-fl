import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import { deterministicId } from "../../src/lib/hash.js";
import { runMigrations } from "../../src/db/migrations.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_source_snapshot_029_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;

const exactEnvelopePayload = {
  costEnvelope: {
    fixedAccountPlanMonthlyUsd: 7.5,
    incrementalExecutionUsd: 1.553815048644,
    maximumIncrementalUsd: 4.912421548644,
    maximumTotalUsd: 12.412421548644,
    requestUsd: {
      maximumAttempts: 4.86,
      successfulExecution: 1.5013935,
    },
    schemaVersion: "candidate-cost-envelope-v3",
    storageUsd: 0.052421548644,
  },
  inventory: { objectCount: 325_312, totalBytes: 3_474_519_090 },
  limits: {
    maxBudgetUsd: 25,
    maxRequests: 1_080_000,
    maxRetries: 2,
  },
  pricing: { fixedAccountPlan: { monthlyUsd: 7.5 } },
  requestEnvelope: {
    categoryRequests: {
      ambiguous_upload_inspection: [0, 24_000],
      bucket_names_preflight: [8, 48],
      control_public_observation: [18, 42],
      final_credential_free_verification: [8_303, 79_590],
      names_mutation: [2, 2],
      recovery: [0, 338],
      rollback: [0, 44],
      upload_provider_cid: [325_312, 975_936],
    },
    finalVerification: {
      deterministicRequiredMaximumRequests: 74_727,
      logicalRequests: 8_303,
      maximumRedirectsPerAttempt: 2,
      maximumTransportAttemptsPerLogicalRequest: 3,
      nonParquetLogicalRequests: 109,
      parquetLogicalRequests: 8_194,
      protectedHeadroomRequests: 4_863,
      schemaVersion: "candidate-final-verification-budget-v1",
    },
    maximumTotalRequests: 1_080_000,
    schemaVersion: "candidate-request-envelope-v3",
    successfulTotalRequests: 333_643,
  },
  version: "2.1.0",
};

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  const applied = await runMigrations(databaseUrl);
  expect(applied.at(-1)).toBe(
    "035_candidate_source_snapshot_upload_resume.sql",
  );
  expect(await runMigrations(databaseUrl)).toEqual([]);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("candidate source-snapshot migration 029", () => {
  it("reproduces the application remote-check hash and identifier exactly", async () => {
    const payload = {
      checkedAt: "2026-08-31T02:01:00.000Z",
      checkKind: "plan_artifact",
      evidenceSha256: "5".repeat(64),
      expectedBytes: 100,
      expectedCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
      expectedSha256: "e".repeat(64),
      metrics: {},
      observedBytes: 100,
      observedCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
      observedSha256: "e".repeat(64),
      planId: "snapshotdemo_69468ded55defabdd7563f6d3e1df437",
      planSha256: "2".repeat(64),
      schemaVersion: "candidate-source-snapshot-remote-check-v1",
    };
    const applicationSha256 = canonicalJsonSha256(payload);
    const applicationId = deterministicId("snapshotdemoremotecheck", [
      "candidate-source-snapshot-remote-check-v1",
      payload.planId,
      payload.checkKind,
      applicationSha256,
    ]);
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      const rows = await sql<{ check_id: string; check_sha256: string }[]>`
        WITH exact AS (
          SELECT oracle_candidate_source_snapshot_remote_check_sha256_v1(
            ${sql.json(payload)}::jsonb
          ) AS check_sha256
        )
        SELECT check_sha256,
               'snapshotdemoremotecheck_' || substr(encode(sha256(convert_to(
                 oracle_canonical_jsonb(to_jsonb(ARRAY[
                   'candidate-source-snapshot-remote-check-v1',
                   ${payload.planId}, ${payload.checkKind}, check_sha256
                 ])), 'UTF8'
               )), 'hex'), 1, 32) AS check_id
        FROM exact
      `;
      expect(rows[0]).toEqual({
        check_id: applicationId,
        check_sha256: applicationSha256,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("accepts only the exact categorized v2.1 request envelope", async () => {
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      const accepted = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(exactEnvelopePayload)}::jsonb
        ) AS valid
      `;
      expect(accepted[0]?.valid).toBe(true);

      const legacy = structuredClone(exactEnvelopePayload);
      legacy.version = "2.0.0";
      const legacyResult = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(legacy)}::jsonb
        ) AS valid
      `;
      expect(legacyResult[0]?.valid).toBe(false);

      const verificationCannibalized = structuredClone(exactEnvelopePayload);
      verificationCannibalized.requestEnvelope.categoryRequests.final_credential_free_verification =
        [8_303, 79_589];
      const cannibalizedResult = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(verificationCannibalized)}::jsonb
        ) AS valid
      `;
      expect(cannibalizedResult[0]?.valid).toBe(false);

      const expandedCompatibilityField = structuredClone(exactEnvelopePayload);
      Object.assign(expandedCompatibilityField.requestEnvelope, {
        maximumAttempts: { total: 1_055_618 },
      });
      const expandedResult = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(expandedCompatibilityField)}::jsonb
        ) AS valid
      `;
      expect(expandedResult[0]?.valid).toBe(false);

      const syntheticPayload = structuredClone(exactEnvelopePayload);
      syntheticPayload.inventory.objectCount = 3;
      syntheticPayload.inventory.totalBytes = 11_240;
      syntheticPayload.limits.maxRequests = 1_100_000;
      syntheticPayload.requestEnvelope.categoryRequests.upload_provider_cid = [
        3, 9,
      ];
      syntheticPayload.requestEnvelope.categoryRequests.ambiguous_upload_inspection =
        [0, 9];
      syntheticPayload.requestEnvelope.categoryRequests.final_credential_free_verification =
        [8_303, 1_099_508];
      syntheticPayload.requestEnvelope.finalVerification.protectedHeadroomRequests = 1_024_781;
      syntheticPayload.requestEnvelope.maximumTotalRequests = 1_100_000;
      syntheticPayload.requestEnvelope.successfulTotalRequests = 8_334;
      syntheticPayload.costEnvelope.requestUsd = {
        maximumAttempts: 4.95,
        successfulExecution: 0.037503,
      };
      Object.assign(syntheticPayload.costEnvelope, {
        incrementalExecutionUsd: 0.037503169583,
        maximumIncrementalUsd: 4.950000169583,
        maximumTotalUsd: 12.450000169583,
        storageUsd: 0.000000169583,
      });
      const syntheticResult = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(syntheticPayload)}::jsonb
        ) AS valid
      `;
      expect(syntheticResult[0]?.valid).toBe(true);

      const verificationReadsUnbilled = structuredClone(exactEnvelopePayload);
      verificationReadsUnbilled.costEnvelope.requestUsd = {
        maximumAttempts: 4.501845,
        successfulExecution: 1.463976,
      };
      const unbilledResult = await sql<{ valid: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_v21_categories_valid(
          ${sql.json(verificationReadsUnbilled)}::jsonb
        ) AS valid
      `;
      expect(unbilledResult[0]?.valid).toBe(false);

      const expandedCategories = await sql<{ categories: unknown }[]>`
        SELECT oracle_candidate_source_snapshot_expanded_categories_v1(
          ${sql.json(exactEnvelopePayload.requestEnvelope)}::jsonb
        ) AS categories
      `;
      expect(expandedCategories[0]?.categories).toEqual([
        {
          category: "upload_provider_cid",
          maximumRequests: 975_936,
          successfulRequests: 325_312,
        },
        {
          category: "ambiguous_upload_inspection",
          maximumRequests: 24_000,
          successfulRequests: 0,
        },
        {
          category: "bucket_names_preflight",
          maximumRequests: 48,
          successfulRequests: 8,
        },
        {
          category: "names_mutation",
          maximumRequests: 2,
          successfulRequests: 2,
        },
        {
          category: "control_public_observation",
          maximumRequests: 42,
          successfulRequests: 18,
        },
        {
          category: "recovery",
          maximumRequests: 338,
          successfulRequests: 0,
        },
        {
          category: "rollback",
          maximumRequests: 44,
          successfulRequests: 0,
        },
        {
          category: "final_credential_free_verification",
          maximumRequests: 79_590,
          successfulRequests: 8_303,
        },
      ]);
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("installs immutable derivation, category, receipt, and approval-v3 guards", async () => {
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      const tables = await sql<
        {
          derivations: string | null;
          categories: string | null;
          receipts: string | null;
        }[]
      >`
        SELECT
          to_regclass('oracle_candidate_source_snapshot_demo_plan_derivations')::text
            AS derivations,
          to_regclass('oracle_candidate_source_snapshot_demo_request_categories')::text
            AS categories,
          to_regclass('oracle_candidate_source_snapshot_demo_remote_read_receipts')::text
            AS receipts
      `;
      expect(tables[0]).toEqual({
        categories: "oracle_candidate_source_snapshot_demo_request_categories",
        derivations: "oracle_candidate_source_snapshot_demo_plan_derivations",
        receipts: "oracle_candidate_source_snapshot_demo_remote_read_receipts",
      });

      const guards = await sql<{ definition: string; name: string }[]>`
        SELECT procedure.proname AS name, procedure.prosrc AS definition
        FROM pg_proc procedure
        JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = current_schema()
          AND procedure.proname IN (
            'oracle_candidate_source_snapshot_authorization_binding_v2',
            'oracle_candidate_source_snapshot_derivation_is_approval_ready',
            'oracle_candidate_source_snapshot_preflight_is_approval_ready',
            'oracle_candidate_source_snapshot_preflight_is_execution_ready',
            'oracle_guard_candidate_source_snapshot_approval_insert',
            'oracle_guard_candidate_source_snapshot_plan_derivation',
            'oracle_guard_candidate_source_snapshot_request_category',
            'oracle_guard_candidate_source_snapshot_request_insert',
            'oracle_guard_candidate_source_snapshot_remote_read_receipt',
            'oracle_guard_candidate_source_snapshot_remote_verification_receipts'
          )
      `;
      expect(guards).toHaveLength(10);
      const guardByName = new Map(
        guards.map((guard) => [guard.name, guard.definition]),
      );
      for (const name of [
        "oracle_guard_candidate_source_snapshot_request_category",
        "oracle_guard_candidate_source_snapshot_request_insert",
      ]) {
        const definition = guardByName.get(name)!;
        expect(definition).toContain("plan_row.state = 'approved'");
        expect(definition).toContain(
          "NEW.request_category = 'bucket_names_preflight'",
        );
        expect(definition).toContain(
          "plan_row.state IS DISTINCT FROM 'approved'",
        );
        expect(definition).not.toContain(
          "plan_row.state IN ('awaiting_configuration', 'awaiting_approval')",
        );
      }
      const definitions = guards.map((guard) => guard.definition).join("\n");
      expect(definitions).toContain("candidate-source-snapshot-approval-v3");
      expect(definitions).toContain(
        "candidate-source-snapshot-authorization-binding-v2",
      );
      expect(definitions).toContain("implementationCommitSha");
      expect(definitions).toContain("implementation_commit_sha");
      expect(definitions).toContain("request_envelope_replacement");
      expect(definitions).toContain("final_credential_free_verification");
      expect(definitions).toContain(
        "request_row.operation_class IS DISTINCT FROM 'class_b_read'",
      );
      expect(definitions).toContain("exact global and category accounting");
      expect(definitions).toContain("bounded intent-free preflight");
      expect(definitions).toContain("planned_successful_request_count = 8");

      const removedExpandedHelper = await sql<{ helper: string | null }[]>`
        SELECT to_regprocedure(
          'oracle_candidate_source_snapshot_operation_counts_valid(jsonb)'
        )::text AS helper
      `;
      expect(removedExpandedHelper[0]?.helper).toBeNull();
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
