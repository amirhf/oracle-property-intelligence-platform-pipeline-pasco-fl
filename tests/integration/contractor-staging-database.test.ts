import { createHash } from "node:crypto";

import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { runMigrations } from "../../src/db/migrations.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `contractor_staging_${process.pid}_${Date.now()}`;
const schemaDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const runId = "run_11111111111111111111111111111111";
const propertyId = "property_22222222222222222222222222222222";
const parcelId = "parcel_33333333333333333333333333333333";
const permitId = "permit_44444444444444444444444444444444";
const contractorId = "contractor_55555555555555555555555555555555";
const permitSourceKey = "synthetic-permit-source-record";
const permitSourceHash = `sha256:${"6".repeat(64)}`;
const contractorSourceHash = `sha256:${"7".repeat(64)}`;

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function deterministicId(prefix: string, values: readonly string[]): string {
  return `${prefix}_${digest(JSON.stringify(values)).slice(0, 32)}`;
}

function authorizationFixture(label: string) {
  const termsSha256 = digest(`terms:${label}`);
  const payload = {
    acquisitionMethod: "owner_supplied_file",
    approvedAt: "2026-08-30T00:00:00.000Z",
    approverReference: `synthetic-controller-${label}`,
    authorizedSourceOrigins: ["https://example.invalid"],
    categoryFilters: ["synthetic-roofing"],
    coverageGeography: "synthetic-test-only",
    decision: "approved_for_staging",
    policyVersion: "contractor-source-authorization-v1",
    provider: "better_business_bureau",
    sourceClassification: "third_party",
    termsEvidenceFile: {
      byteSize: 45,
      relativePath: `contractors/${label}-terms.txt`,
      sha256: termsSha256,
    },
  };
  const canonical = canonicalJson(payload);
  const sha256 = digest(canonical);
  return {
    authorizationId: deterministicId("contractorauthorization", [
      "contractor-source-authorization-v1",
      "better_business_bureau",
      sha256,
    ]),
    canonical,
    payload,
    sha256,
  };
}

async function insertAuthorization(
  sql: postgres.Sql,
  label: string,
): Promise<ReturnType<typeof authorizationFixture>> {
  const fixture = authorizationFixture(label);
  await sql`
    INSERT INTO oracle_contractor_source_authorizations (
      authorization_id, authorization_sha256, policy_version, provider,
      source_classification, acquisition_method, approver_reference,
      approved_at, coverage_geography, category_filters,
      authorized_source_origins,
      terms_evidence_relative_path, terms_evidence_byte_size,
      terms_evidence_sha256, authorization_payload,
      authorization_canonical_json
    ) VALUES (
      ${fixture.authorizationId}, ${fixture.sha256},
      'contractor-source-authorization-v1', 'better_business_bureau',
      'third_party', 'owner_supplied_file',
      ${fixture.payload.approverReference}, ${fixture.payload.approvedAt},
      ${fixture.payload.coverageGeography},
      ${sql.json(fixture.payload.categoryFilters)},
      ${sql.json(fixture.payload.authorizedSourceOrigins)},
      ${fixture.payload.termsEvidenceFile.relativePath},
      ${fixture.payload.termsEvidenceFile.byteSize},
      ${fixture.payload.termsEvidenceFile.sha256},
      ${sql.json(fixture.payload)}, ${fixture.canonical}
    )
  `;
  return fixture;
}

function datasetFixture(
  label: string,
  authorization: ReturnType<typeof authorizationFixture>,
  mutate?: (payload: Record<string, unknown>) => Record<string, unknown>,
) {
  const sourceSha256 = digest(`source:${label}`);
  const payload: Record<string, unknown> = {
    acquisitionMethod: "owner_supplied_file",
    authorizationId: authorization.authorizationId,
    categoryFilters: ["synthetic-roofing"],
    counts: {
      accepted: 1,
      duplicateProviderIds: 0,
      parsed: 1,
      rejected: 0,
      source: 1,
    },
    coverageGeography: "synthetic-test-only",
    coverageMode: "partial",
    createdAt: "2026-08-30T00:00:00.000Z",
    licenseTerms: {
      evidenceFile: authorization.payload.termsEvidenceFile,
      evidenceSha256: authorization.payload.termsEvidenceFile.sha256,
      status: "verified_compatible",
    },
    manifestVersion: "1.0.0",
    observationWindow: {
      end: "2026-08-30T00:00:00.000Z",
      reason: null,
      start: "2026-08-30T00:00:00.000Z",
      status: "recorded",
    },
    parserVersion: "contractor-jsonl-v1",
    provider: "better_business_bureau",
    recordSchemaVersion: "1.0.0",
    retrieval: {
      at: "2026-08-30T00:00:00.000Z",
      reason: null,
      status: "recorded",
    },
    sourceClassification: "third_party",
    sourceFile: {
      byteSize: 123,
      relativePath: `contractors/${label}.jsonl`,
      sha256: sourceSha256,
    },
    sourceUrls: ["https://example.invalid/synthetic-export"],
    transformVersion: "contractor-identity-match-v1",
  };
  const changed = mutate ? mutate(structuredClone(payload)) : payload;
  const canonical = canonicalJson(changed);
  const sha256 = digest(canonical);
  return {
    canonical,
    datasetId: deterministicId("contractordataset", [
      "1.0.0",
      "better_business_bureau",
      sha256,
    ]),
    payload: changed,
    sha256,
  };
}

async function insertDataset(
  sql: postgres.Sql,
  fixture: ReturnType<typeof datasetFixture>,
  overrides: Partial<{
    canonical: string;
    manifestSha256: string;
    sourcePath: string;
    sourceUrls: unknown;
  }> = {},
): Promise<void> {
  const payload = fixture.payload as {
    acquisitionMethod: string;
    authorizationId: string;
    categoryFilters: string[];
    counts: {
      accepted: number;
      duplicateProviderIds: number;
      parsed: number;
      rejected: number;
      source: number;
    };
    coverageGeography: string;
    licenseTerms: {
      evidenceFile: { byteSize: number; relativePath: string; sha256: string };
      evidenceSha256: string;
      status: string;
    };
    observationWindow: {
      end: string;
      reason: null;
      start: string;
      status: string;
    };
    retrieval: { at: string; reason: null; status: string };
    sourceFile: { byteSize: number; relativePath: string; sha256: string };
    sourceUrls: string[];
  };
  await sql`
    INSERT INTO oracle_contractor_source_datasets (
      dataset_id, source_run_id, provider, source_classification,
      acquisition_method, coverage_mode, coverage_geography, category_filters,
      license_terms_status, license_terms_evidence_sha256,
      license_terms_evidence_relative_path, license_terms_evidence_byte_size,
      source_file_relative_path, source_file_byte_size, source_file_sha256,
      observation_status, observation_start, observation_end,
      observation_unavailable_reason, retrieval_status, retrieved_at,
      retrieval_unavailable_reason, parser_version, transform_version,
      manifest_sha256, manifest_payload, source_count, parsed_count,
      accepted_count, rejected_count, duplicate_count, manifest_version,
      record_schema_version, manifest_created_at, authorization_id, source_urls,
      manifest_canonical_json
    ) VALUES (
      ${fixture.datasetId}, ${runId}, 'better_business_bureau', 'third_party',
      ${payload.acquisitionMethod}, 'partial', ${payload.coverageGeography},
      ${sql.json(payload.categoryFilters)}, ${payload.licenseTerms.status},
      ${payload.licenseTerms.evidenceSha256},
      ${payload.licenseTerms.evidenceFile.relativePath},
      ${payload.licenseTerms.evidenceFile.byteSize},
      ${overrides.sourcePath ?? payload.sourceFile.relativePath},
      ${payload.sourceFile.byteSize}, ${payload.sourceFile.sha256},
      ${payload.observationWindow.status}, ${payload.observationWindow.start},
      ${payload.observationWindow.end}, null, ${payload.retrieval.status},
      ${payload.retrieval.at}, null, 'contractor-jsonl-v1',
      'contractor-identity-match-v1',
      ${overrides.manifestSha256 ?? fixture.sha256},
      ${sql.json(fixture.payload as postgres.JSONValue)},
      ${payload.counts.source}, ${payload.counts.parsed},
      ${payload.counts.accepted}, ${payload.counts.rejected},
      ${payload.counts.duplicateProviderIds}, '1.0.0', '1.0.0',
      ${(fixture.payload as { createdAt: string }).createdAt},
      ${payload.authorizationId},
      ${sql.json((overrides.sourceUrls ?? payload.sourceUrls) as postgres.JSONValue)},
      ${overrides.canonical ?? fixture.canonical}
    )
  `;
}

function recordFixture(
  datasetId: string,
  label: string,
  overrides: Partial<{
    providerRecordId: string;
    sourceUrl: string;
  }> = {},
) {
  const payload = {
    accredited: true,
    businessAddress: "SYNTHETIC ADDRESS",
    businessName: "SYNTHETIC BUSINESS",
    categories: ["synthetic-roofing"],
    licenseIssuer: null,
    licenseJurisdiction: null,
    licenseNumber: null,
    phone: "SYNTHETIC PHONE",
    provider: "better_business_bureau",
    providerRecordId:
      overrides.providerRecordId ?? `synthetic-provider-${label}`,
    rating: "SYNTHETIC RATING",
    schemaVersion: "1.0.0",
    sourceUrl:
      overrides.sourceUrl ?? "https://example.invalid/synthetic-record",
  };
  const canonical = canonicalJson(payload);
  const sha256 = digest(canonical);
  return {
    canonical,
    payload,
    recordVersionId: deterministicId("contractorversion", [
      "1.0.0",
      datasetId,
      payload.provider,
      payload.providerRecordId,
      sha256,
    ]),
    sha256,
  };
}

async function insertRecord(
  sql: postgres.Sql,
  datasetId: string,
  label: string,
  overrides: Partial<{
    providerRecordId: string;
    recordVersionId: string;
    sourceUrl: string;
  }> = {},
): Promise<ReturnType<typeof recordFixture>> {
  const fixture = recordFixture(datasetId, label, overrides);
  await sql`
    INSERT INTO oracle_contractor_source_record_versions (
      record_version_id, dataset_id, provider, provider_record_id,
      legal_business_name, license_number, license_issuer,
      license_jurisdiction, business_address, phone, source_record_sha256,
      source_payload, source_url, source_schema_version,
      source_payload_canonical_json
    ) VALUES (
      ${overrides.recordVersionId ?? fixture.recordVersionId}, ${datasetId}, 'better_business_bureau',
      ${fixture.payload.providerRecordId}, ${fixture.payload.businessName},
      null, null, null, ${fixture.payload.businessAddress},
      ${fixture.payload.phone}, ${fixture.sha256}, ${sql.json(fixture.payload)},
      ${fixture.payload.sourceUrl}, '1.0.0', ${fixture.canonical}
    )
  `;
  return fixture;
}

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(30);
  const sql = postgres(schemaDatabaseUrl, { max: 1 });
  try {
    await sql`
      INSERT INTO oracle_pipeline_runs (
        run_id, workflow_id, county, sample_algorithm, sample_seed,
        window_start, window_end, as_of, status, selection_size, coverage_mode
      ) VALUES (
        ${runId}, 'synthetic-contractor-staging', 'pasco', 'synthetic',
        'synthetic', '2026-08-30T00:00:00.000Z',
        '2026-08-30T00:00:00.000Z', '2026-08-30T00:00:00.000Z',
        'completed', 1, 'sample'
      )
    `;
    await sql`
      INSERT INTO oracle_properties (
        property_id, parcel_id, county, source_system, exact_folio,
        matching_folio_digits, source_record_hash, first_seen_run_id,
        last_seen_run_id
      ) VALUES (
        ${propertyId}, ${parcelId}, 'pasco', 'pasco_appraiser',
        'SYNTHETIC-FOLIO', '0001', ${`sha256:${"8".repeat(64)}`},
        ${runId}, ${runId}
      )
    `;
    await sql`
      INSERT INTO oracle_permits (
        permit_id, property_id, source_record_key, permit_number, record_type,
        normalized_status, roofing_relevance, source_record_hash,
        first_seen_run_id, last_seen_run_id
      ) VALUES (
        ${permitId}, ${propertyId}, ${permitSourceKey}, 'SYNTHETIC-PERMIT',
        'synthetic', 'unknown', true, ${permitSourceHash}, ${runId}, ${runId}
      )
    `;
    await sql`
      INSERT INTO oracle_contractors (
        contractor_id, source_system, source_record_key, name,
        source_record_hash
      ) VALUES (
        ${contractorId}, 'better_business_bureau', 'synthetic-provider-match',
        'SYNTHETIC BUSINESS', ${contractorSourceHash}
      )
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
}, 60_000);

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("contractor staging database identity guards", () => {
  it("accepts one fully bound authorization, dataset and record", async () => {
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const authorization = await insertAuthorization(sql, "valid");
      const dataset = datasetFixture("valid", authorization);
      await expect(insertDataset(sql, dataset)).resolves.toBeUndefined();
      await expect(
        insertRecord(sql, dataset.datasetId, "valid"),
      ).resolves.toMatchObject({
        sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
      });
      await expect(
        sql`UPDATE oracle_contractor_source_authorizations SET approver_reference = 'changed' WHERE authorization_id = ${authorization.authorizationId}`,
      ).rejects.toThrow();
      await expect(
        sql`DELETE FROM oracle_contractor_source_datasets WHERE dataset_id = ${dataset.datasetId}`,
      ).rejects.toThrow();
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it.each([
    [
      "extra manifest key",
      (payload: Record<string, unknown>) => ({ ...payload, extra: true }),
      {},
    ],
    ["unsafe source path", undefined, { sourcePath: "../source.jsonl" }],
    [
      "unsafe source URL",
      undefined,
      { sourceUrls: ["https://example.invalid/export?token=synthetic"] },
    ],
    ["wrong manifest hash", undefined, { manifestSha256: "0".repeat(64) }],
    ["wrong canonical bytes", undefined, { canonical: "{}" }],
    [
      "non-numeric count",
      (payload: Record<string, unknown>) => ({
        ...payload,
        counts: {
          ...(payload.counts as Record<string, unknown>),
          source: "1",
        },
      }),
      {},
    ],
    [
      "non-ISO manifest timestamp",
      (payload: Record<string, unknown>) => ({
        ...payload,
        createdAt: "2026-08-30",
      }),
      {},
    ],
    [
      "recorded observation with a reason",
      (payload: Record<string, unknown>) => ({
        ...payload,
        observationWindow: {
          ...(payload.observationWindow as Record<string, unknown>),
          reason: "not_provided_by_source",
        },
      }),
      {},
    ],
    [
      "unavailable observation with a timestamp",
      (payload: Record<string, unknown>) => ({
        ...payload,
        observationWindow: {
          end: null,
          reason: "not_provided_by_source",
          start: "2026-08-30T00:00:00.000Z",
          status: "unavailable",
        },
      }),
      {},
    ],
    [
      "unsupported retrieval status",
      (payload: Record<string, unknown>) => ({
        ...payload,
        retrieval: {
          at: null,
          reason: "not_provided_by_source",
          status: "unknown",
        },
      }),
      {},
    ],
    [
      "unauthorized source origin",
      (payload: Record<string, unknown>) => ({
        ...payload,
        sourceUrls: ["https://other.invalid/export"],
      }),
      {},
    ],
    [
      "secret-shaped source path",
      (payload: Record<string, unknown>) => ({
        ...payload,
        sourceUrls: ["https://example.invalid/token/synthetic"],
      }),
      {},
    ],
    [
      "inconsistent counts",
      (payload: Record<string, unknown>) => ({
        ...payload,
        counts: {
          accepted: 1,
          duplicateProviderIds: 0,
          parsed: 1,
          rejected: 1,
          source: 1,
        },
      }),
      {},
    ],
  ] as const)(
    "rejects %s through direct SQL",
    async (_name, mutate, overrides) => {
      const sql = postgres(schemaDatabaseUrl, { max: 1 });
      try {
        const label = digest(_name).slice(0, 12);
        const authorization = await insertAuthorization(sql, label);
        const dataset = datasetFixture(label, authorization, mutate);
        await expect(insertDataset(sql, dataset, overrides)).rejects.toThrow();
      } finally {
        await sql.end({ timeout: 5 });
      }
    },
  );

  it("rejects record identity, bounds and origins through direct SQL", async () => {
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const authorization = await insertAuthorization(sql, "record-guards");
      const dataset = datasetFixture("record-guards", authorization);
      await insertDataset(sql, dataset);
      await expect(
        insertRecord(sql, dataset.datasetId, "wrong-id", {
          recordVersionId: `contractorversion_${"0".repeat(32)}`,
        }),
      ).rejects.toThrow();
      await expect(
        insertRecord(sql, dataset.datasetId, "wrong-origin", {
          sourceUrl: "https://other.invalid/record",
        }),
      ).rejects.toThrow("outside its dataset origin");
      await expect(
        insertRecord(sql, dataset.datasetId, "long-provider-id", {
          providerRecordId: "x".repeat(257),
        }),
      ).rejects.toThrow("payload shape is invalid");
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("stores only non-linking identity outcomes and rejects mutable target links", async () => {
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const authorization = await insertAuthorization(sql, "match");
      const dataset = datasetFixture("match", authorization);
      await insertDataset(sql, dataset);
      const record = await insertRecord(sql, dataset.datasetId, "match");
      const payload = {
        candidateSourceRecordSha256: null,
        confidence: 0,
        matchVersion: "contractor-identity-match-v1",
        matchedContractorId: null,
        method: "no_match",
        recordVersionId: record.recordVersionId,
        sourceRecordSha256: record.sha256,
        status: "unmatched",
      };
      const canonical = canonicalJson(payload);
      await expect(sql`
        INSERT INTO oracle_contractor_identity_matches (
          match_id, record_version_id, matched_contractor_id, status, method,
          confidence, evidence_sha256, evidence_payload, match_version,
          source_identity_sha256, candidate_identity_sha256,
          evidence_canonical_json
        ) VALUES (
          ${`contractormatch_${digest("match").slice(0, 32)}`},
          ${record.recordVersionId}, null, 'unmatched',
          'no_match', 0, ${digest(canonical)},
          ${sql.json(payload)}, 'contractor-identity-match-v1',
          ${record.sha256}, null, ${canonical}
        )
      `).resolves.toEqual([]);
      const changed = {
        ...payload,
        candidateSourceRecordSha256: contractorSourceHash,
        confidence: 1,
        matchedContractorId: contractorId,
        method: "exact_provider_identifier",
        status: "linked",
      };
      const changedCanonical = canonicalJson(changed);
      await expect(sql`
        INSERT INTO oracle_contractor_identity_matches (
          match_id, record_version_id, matched_contractor_id, status, method,
          confidence, evidence_sha256, evidence_payload, match_version,
          source_identity_sha256, candidate_identity_sha256,
          evidence_canonical_json
        ) VALUES (
          ${`contractormatch_${digest("changed").slice(0, 32)}`},
          ${record.recordVersionId}, ${contractorId}, 'linked',
          'exact_provider_identifier', 1, ${digest(changedCanonical)},
          ${sql.json(changed)}, 'contractor-identity-match-v1',
          ${record.sha256}, ${changed.candidateSourceRecordSha256},
          ${changedCanonical}
        )
      `).rejects.toThrow();
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("rejects all permit relationships until immutable source versions exist", async () => {
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      await expect(sql`
        INSERT INTO oracle_permit_contractors (
          permit_id, contractor_id, match_basis, match_confidence
        ) VALUES (${permitId}, ${contractorId}, 'name_only', 1)
      `).rejects.toThrow();
      const payload = {
        contractorId,
        evidenceVersion: "permit-contractor-relationship-v1",
        matchBasis: "permit_source_license_number",
        matchConfidence: 1,
        permitId,
        permitSourceRecordHash: permitSourceHash,
        permitSourceRecordKey: permitSourceKey,
        propertyId,
        relationshipRecordId: "synthetic-relationship-record",
        relationshipSourceSha256: `sha256:${"a".repeat(64)}`,
        relationshipSourceSystem: "official_permit_source",
      };
      const canonical = canonicalJson(payload);
      await expect(sql`
        INSERT INTO oracle_permit_contractors (
          permit_id, contractor_id, match_basis, match_confidence, property_id,
          relationship_record_id, relationship_source_system,
          relationship_source_sha256, relationship_evidence_version,
          relationship_evidence_sha256, relationship_evidence_canonical_json
        ) VALUES (
          ${permitId}, ${contractorId}, ${payload.matchBasis}, 1, ${propertyId},
          ${payload.relationshipRecordId}, ${payload.relationshipSourceSystem},
          ${payload.relationshipSourceSha256}, ${payload.evidenceVersion},
          ${digest(canonical)}, ${canonical}
        )
      `).rejects.toThrow("linkage is disabled");
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
