import { mkdtemp, mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  acquireBbbContractorDataset,
  assertParcelContractorRelationshipEvidence,
  buildContractorDatasetManifest,
  buildContractorIdentityMatchEvidence,
  buildContractorSourceAuthorization,
  buildPermitContractorRelationshipEvidence,
  contractorSourceRowSchema,
  contractorSourceObject,
  MAX_CONTRACTOR_JSONL_LINE_BYTES,
  MAX_CONTRACTOR_SOURCE_BYTES,
  CONTRACTOR_RECORD_SCHEMA_VERSION,
  matchContractorIdentity,
  parseVerifiedContractorJsonl,
  validateContractorDatasetManifest,
  type ContractorSourceRow,
} from "../../src/contractors/staging.js";
import { bindDataFile } from "../../src/snapshot/model.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

function sourceRow(overrides: Partial<ContractorSourceRow> = {}) {
  return {
    accredited: true,
    businessAddress: "SYNTHETIC-ADDRESS-A",
    businessName: "SYNTHETIC ROOFING A",
    categories: ["synthetic-roofing"],
    licenseIssuer: null,
    licenseJurisdiction: null,
    licenseNumber: null,
    phone: "SYNTHETIC-PHONE-A",
    provider: "better_business_bureau" as const,
    providerRecordId: "synthetic-provider-a",
    rating: "SYNTHETIC-RATING",
    schemaVersion: CONTRACTOR_RECORD_SCHEMA_VERSION,
    sourceUrl: "https://example.invalid/synthetic-provider-a",
    ...overrides,
  };
}

async function syntheticDataset(rows: unknown[]) {
  const dataDir = await mkdtemp(path.join(tmpdir(), "contractor-staging-"));
  temporaryDirectories.push(dataDir);
  const relativePath = "contractors/synthetic.jsonl";
  const filePath = path.join(dataDir, relativePath);
  const termsPath = path.join(dataDir, "contractors/terms-evidence.txt");
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(
    filePath,
    `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
    "utf8",
  );
  await writeFile(termsPath, "synthetic compatible terms evidence\n", "utf8");
  const sourceFile = await bindDataFile(dataDir, filePath);
  const termsEvidenceFile = await bindDataFile(dataDir, termsPath);
  const authorization = buildContractorSourceAuthorization({
    acquisitionMethod: "owner_supplied_file",
    approvedAt: "2026-08-30T00:00:00.000Z",
    approverReference: "synthetic-controller",
    authorizedSourceOrigins: ["https://example.invalid"],
    categoryFilters: ["synthetic-roofing"],
    coverageGeography: "synthetic-test-only",
    decision: "approved_for_staging",
    policyVersion: "contractor-source-authorization-v1",
    provider: "better_business_bureau",
    sourceClassification: "third_party",
    termsEvidenceFile,
  });
  const source = rows.length;
  const parsed = rows.filter((row) => {
    try {
      JSON.stringify(row);
      return true;
    } catch {
      return false;
    }
  }).length;
  const validRows = rows.filter(
    (row) => contractorSourceRowSchema.safeParse(row).success,
  ) as ContractorSourceRow[];
  const seen = new Set<string>();
  let duplicates = 0;
  for (const row of validRows) {
    if (seen.has(row.providerRecordId)) duplicates += 1;
    else seen.add(row.providerRecordId);
  }
  const accepted = validRows.length - duplicates;
  const manifest = buildContractorDatasetManifest({
    acquisitionMethod: "owner_supplied_file",
    authorizationId: authorization.authorizationId,
    categoryFilters: ["synthetic-roofing"],
    counts: {
      accepted,
      duplicateProviderIds: duplicates,
      parsed,
      rejected: source - accepted,
      source,
    },
    coverageGeography: "synthetic-test-only",
    coverageMode: "partial",
    createdAt: "2026-08-30T00:00:00.000Z",
    licenseTerms: {
      evidenceFile: termsEvidenceFile,
      evidenceSha256: termsEvidenceFile.sha256,
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
    sourceFile,
    sourceUrls: ["https://example.invalid/synthetic-export"],
    transformVersion: "contractor-identity-match-v1",
  });
  return { authorization, dataDir, filePath, manifest, termsPath };
}

describe("contractor and BBB staging", () => {
  it("hash-binds and streams a strict partial third-party dataset", async () => {
    const dataset = await syntheticDataset([
      sourceRow(),
      sourceRow({ providerRecordId: "synthetic-provider-b" }),
      sourceRow({ providerRecordId: "synthetic-provider-b" }),
      { unsupported: true },
    ]);
    const records: unknown[] = [];
    const counts = await parseVerifiedContractorJsonl({
      authorization: dataset.authorization,
      dataDir: dataset.dataDir,
      manifest: dataset.manifest,
      onRecord: (record) => {
        records.push(record);
      },
    });
    expect(counts).toEqual({
      accepted: 2,
      duplicateProviderIds: 1,
      parsed: 4,
      rejected: 2,
      source: 4,
    });
    expect(records).toHaveLength(2);
    expect(JSON.stringify(records)).not.toContain(process.cwd());
  });

  it("rejects tampered manifests and source bytes", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    expect(() =>
      validateContractorDatasetManifest({
        ...dataset.manifest,
        manifestSha256: "0".repeat(64),
      }),
    ).toThrow("identity mismatch");
    await writeFile(dataset.filePath, `${JSON.stringify(sourceRow())}\n `);
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest: dataset.manifest,
        onRecord: () => undefined,
      }),
    ).rejects.toThrow("source file binding mismatch");
  });

  it("requires the hash-bound terms-evidence file before parsing", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    await writeFile(dataset.termsPath, "changed terms evidence\n", "utf8");
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest: dataset.manifest,
        onRecord: () => undefined,
      }),
    ).rejects.toThrow("terms evidence file binding mismatch");
  });

  it("rejects invalid UTF-8 before exposing any record", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    await writeFile(
      dataset.filePath,
      Buffer.concat([
        Buffer.from(`${JSON.stringify(sourceRow())}\n`, "utf8"),
        Buffer.from([0xc3, 0x28, 0x0a]),
      ]),
    );
    const sourceFile = await bindDataFile(dataset.dataDir, dataset.filePath);
    const manifest = buildContractorDatasetManifest({
      ...dataset.manifest.payload,
      counts: {
        accepted: 1,
        duplicateProviderIds: 0,
        parsed: 1,
        rejected: 1,
        source: 2,
      },
      sourceFile,
    });
    const exposed: unknown[] = [];
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest,
        onRecord: (record) => {
          exposed.push(record);
        },
      }),
    ).rejects.toThrow("invalid UTF-8");
    expect(exposed).toEqual([]);
    expect(
      (await readdir(path.dirname(dataset.filePath))).filter((entry) =>
        entry.startsWith(".contractor-validation-"),
      ),
    ).toEqual([]);
  });

  it("validates the complete source before callbacks and cleans its private spool", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    await writeFile(
      dataset.filePath,
      `${JSON.stringify(sourceRow())}\n${"X".repeat(
        MAX_CONTRACTOR_JSONL_LINE_BYTES + 1,
      )}\n`,
      "utf8",
    );
    const sourceFile = await bindDataFile(dataset.dataDir, dataset.filePath);
    const manifest = buildContractorDatasetManifest({
      ...dataset.manifest.payload,
      counts: {
        accepted: 1,
        duplicateProviderIds: 0,
        parsed: 1,
        rejected: 1,
        source: 2,
      },
      sourceFile,
    });
    const exposed: unknown[] = [];
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest,
        onRecord: (record) => {
          exposed.push(record);
        },
      }),
    ).rejects.toThrow("line exceeds byte limit");
    expect(exposed).toEqual([]);
    expect(
      (await readdir(path.dirname(dataset.filePath))).filter((entry) =>
        entry.startsWith(".contractor-validation-"),
      ),
    ).toEqual([]);
  });

  it("requires a distinct terms object and the exact source authorization", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        licenseTerms: {
          evidenceFile: dataset.manifest.payload.sourceFile,
          evidenceSha256: dataset.manifest.payload.sourceFile.sha256,
          status: "verified_compatible",
        },
      }),
    ).toThrow("must be distinct");
    const wrongAuthorization = buildContractorSourceAuthorization({
      ...dataset.authorization.payload,
      coverageGeography: "different-synthetic-scope",
    });
    await expect(
      parseVerifiedContractorJsonl({
        authorization: wrongAuthorization,
        dataDir: dataset.dataDir,
        manifest: dataset.manifest,
        onRecord: () => undefined,
      }),
    ).rejects.toThrow("does not match its source authorization");
  });

  it.each([
    ["absolute source path", { relativePath: "/contractors/source.jsonl" }],
    ["traversing source path", { relativePath: "../source.jsonl" }],
    ["backslash source path", { relativePath: "contractors\\source.jsonl" }],
    ["oversized source", { byteSize: MAX_CONTRACTOR_SOURCE_BYTES + 1 }],
  ])("rejects %s in a future dataset manifest", async (_label, override) => {
    const dataset = await syntheticDataset([sourceRow()]);
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceFile: { ...dataset.manifest.payload.sourceFile, ...override },
      }),
    ).toThrow();
  });

  it("requires credential-free HTTPS source identities", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceUrls: ["http://example.invalid/export"],
      }),
    ).toThrow("credential-free HTTPS URL");
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceUrls: ["https://user:secret@example.invalid/export"],
      }),
    ).toThrow("credential-free HTTPS URL");
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceUrls: ["https://example.invalid/export?token=SYNTHETIC"],
      }),
    ).toThrow("credential-free HTTPS URL");
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceUrls: ["https://example.invalid/export#SYNTHETIC"],
      }),
    ).toThrow("credential-free HTTPS URL");
    expect(() =>
      contractorSourceRowSchema.parse(
        sourceRow({
          sourceUrl: "https://example.invalid/record?access_key=SYNTHETIC",
        }),
      ),
    ).toThrow("credential-free HTTPS URL");
    expect(() =>
      buildContractorDatasetManifest({
        ...dataset.manifest.payload,
        sourceUrls: ["https://example.invalid/token/synthetic"],
      }),
    ).toThrow("secret-shaped path");
    expect(() =>
      buildContractorSourceAuthorization({
        ...dataset.authorization.payload,
        authorizedSourceOrigins: ["https://example.invalid/path"],
      }),
    ).toThrow("exact credential-free HTTPS origin");
  });

  it("rejects a dataset outside its immutable authorized source origins", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    const manifest = buildContractorDatasetManifest({
      ...dataset.manifest.payload,
      sourceUrls: ["https://other.invalid/export"],
    });
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest,
        onRecord: () => undefined,
      }),
    ).rejects.toThrow("does not match its source authorization");
  });

  it("rejects JSONL records above the bounded line size", async () => {
    const dataset = await syntheticDataset([
      sourceRow({ businessName: "X".repeat(MAX_CONTRACTOR_JSONL_LINE_BYTES) }),
    ]);
    await expect(
      parseVerifiedContractorJsonl({
        authorization: dataset.authorization,
        dataDir: dataset.dataDir,
        manifest: dataset.manifest,
        onRecord: () => undefined,
      }),
    ).rejects.toThrow("line exceeds byte limit");
  });

  it("retains explicit unavailable observation and retrieval provenance", async () => {
    const dataset = await syntheticDataset([sourceRow()]);
    const manifest = buildContractorDatasetManifest({
      ...dataset.manifest.payload,
      observationWindow: {
        end: null,
        reason: "not_provided_by_source",
        start: null,
        status: "unavailable",
      },
      retrieval: {
        at: null,
        reason: "not_recorded_during_acquisition",
        status: "unavailable",
      },
    });
    expect(contractorSourceObject(manifest).observedAt).toBeNull();
  });

  it.each([
    [
      "exact license",
      {
        licenseIssuer: "SYNTHETIC ISSUER",
        licenseJurisdiction: "SYNTHETIC-FL",
        licenseNumber: "SYNTHETIC-LICENSE-A",
      },
      {
        licenseIssuer: "synthetic issuer",
        licenseJurisdiction: "synthetic fl",
        licenseNumber: "synthetic license a",
      },
      "exact_license_number",
      "linked",
    ],
    [
      "provider identity",
      { providerRecordId: "provider-a" },
      { providerRecordId: "provider-a" },
      "exact_provider_identifier",
      "linked",
    ],
    ["strong legal identity", {}, {}, "legal_name_address_phone", "linked"],
    [
      "name only",
      { businessAddress: null, phone: null },
      { businessAddress: null, phone: null },
      "name_only_ambiguous",
      "ambiguous",
    ],
    [
      "different identity",
      { businessName: "SYNTHETIC A" },
      { businessName: "SYNTHETIC B" },
      "no_match",
      "unmatched",
    ],
  ] as const)(
    "classifies %s without accepting ambiguous name-only matches",
    (_label, sourceOverrides, candidateOverrides, method, status) => {
      const base = {
        businessAddress: "SYNTHETIC-ADDRESS-A",
        businessName: "SYNTHETIC BUSINESS A",
        licenseIssuer: null,
        licenseJurisdiction: null,
        licenseNumber: null,
        phone: "SYNTHETIC-PHONE-A",
        provider: "better_business_bureau" as const,
        providerRecordId: null,
      };
      expect(
        matchContractorIdentity(
          { ...base, ...sourceOverrides },
          { ...base, ...candidateOverrides },
        ),
      ).toMatchObject({ method, status });
    },
  );

  it("does not match a license number across issuers or jurisdictions", () => {
    const source = {
      businessAddress: null,
      businessName: "SYNTHETIC A",
      licenseIssuer: "SYNTHETIC ISSUER A",
      licenseJurisdiction: "SYNTHETIC-FL",
      licenseNumber: "SYNTHETIC-LICENSE",
      phone: null,
      provider: "official_license_source" as const,
      providerRecordId: null,
    };
    expect(
      matchContractorIdentity(source, {
        ...source,
        businessName: "SYNTHETIC B",
        licenseIssuer: "SYNTHETIC ISSUER B",
      }),
    ).toMatchObject({ method: "no_match", status: "unmatched" });
    expect(
      matchContractorIdentity(source, {
        ...source,
        businessName: "SYNTHETIC B",
        licenseJurisdiction: "SYNTHETIC-GA",
      }),
    ).toMatchObject({ method: "no_match", status: "unmatched" });
  });

  it("requires independent permit/source evidence for a parcel relationship", () => {
    expect(() =>
      assertParcelContractorRelationshipEvidence({
        contractorId: "contractor_00000000000000000000000000000000",
        evidenceVersion: "permit-contractor-relationship-v1",
        matchBasis: "permit_source_contractor_id",
        matchConfidence: 1,
        permitId: null,
        permitSourceRecordHash: `sha256:${"a".repeat(64)}`,
        permitSourceRecordKey: "synthetic-permit-record",
        propertyId: "property_00000000000000000000000000000000",
        relationshipRecordId: null,
        relationshipSourceSha256: null,
        relationshipSourceSystem: "official_permit_source",
      }),
    ).toThrow("explicit permit/source evidence");
    expect(() =>
      assertParcelContractorRelationshipEvidence({
        contractorId: "contractor_00000000000000000000000000000000",
        evidenceVersion: "permit-contractor-relationship-v1",
        matchBasis: "permit_source_contractor_id",
        matchConfidence: 1,
        permitId: "permit_00000000000000000000000000000000",
        permitSourceRecordHash: `sha256:${"a".repeat(64)}`,
        permitSourceRecordKey: "synthetic-permit-record",
        propertyId: "property_00000000000000000000000000000000",
        relationshipRecordId: "synthetic-official-permit-record",
        relationshipSourceSha256: `sha256:${"b".repeat(64)}`,
        relationshipSourceSystem: "official_permit_source",
      }),
    ).toThrow("linkage is disabled");
  });

  it("hash-binds match and permit relationship evidence without raw identities", () => {
    const source = {
      businessAddress: null,
      businessName: "SYNTHETIC SOURCE",
      licenseIssuer: null,
      licenseJurisdiction: null,
      licenseNumber: null,
      phone: null,
      provider: "better_business_bureau" as const,
      providerRecordId: "synthetic-source",
    };
    const candidate = {
      ...source,
      businessName: "SYNTHETIC CANDIDATE",
      providerRecordId: "synthetic-candidate",
    };
    const evidence = buildContractorIdentityMatchEvidence({
      candidate,
      candidateSourceRecordSha256: null,
      matchedContractorId: null,
      recordVersionId: "contractorversion_00000000000000000000000000000000",
      source,
      sourceRecordSha256: "c".repeat(64),
    });
    expect(evidence.evidenceSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.stringify(evidence)).not.toContain("SYNTHETIC BUSINESS");
    expect(() =>
      buildContractorIdentityMatchEvidence({
        candidate: source,
        candidateSourceRecordSha256: `sha256:${"b".repeat(64)}`,
        matchedContractorId: "contractor_00000000000000000000000000000000",
        recordVersionId: "contractorversion_00000000000000000000000000000000",
        source,
        sourceRecordSha256: "c".repeat(64),
      }),
    ).toThrow("linkage is disabled");
    expect(
      buildPermitContractorRelationshipEvidence({
        contractorId: "contractor_00000000000000000000000000000000",
        evidenceVersion: "permit-contractor-relationship-v1",
        matchBasis: "permit_source_license_number",
        matchConfidence: 1,
        permitId: "permit_00000000000000000000000000000000",
        permitSourceRecordHash: `sha256:${"d".repeat(64)}`,
        permitSourceRecordKey: "synthetic-permit-record",
        propertyId: "property_00000000000000000000000000000000",
        relationshipRecordId: "synthetic-relationship-record",
        relationshipSourceSha256: `sha256:${"e".repeat(64)}`,
        relationshipSourceSystem: "official_permit_source",
      }).evidenceSha256,
    ).toMatch(/^[a-f0-9]{64}$/);
  });

  it("keeps remote BBB acquisition disabled", async () => {
    await expect(acquireBbbContractorDataset()).rejects.toThrow(
      "BBB acquisition is disabled",
    );
  });
});
