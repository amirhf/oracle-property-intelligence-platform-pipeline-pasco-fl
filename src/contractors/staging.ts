import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdtemp, open, rm, type FileHandle } from "node:fs/promises";
import path from "node:path";

import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { SourceAccessStopError } from "../lib/access-stop.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";
import {
  bindDataFile,
  resolveBoundDataPath,
  type SourceObject,
} from "../snapshot/model.js";

export const CONTRACTOR_DATASET_MANIFEST_VERSION = "1.0.0";
export const CONTRACTOR_RECORD_SCHEMA_VERSION = "1.0.0";
export const CONTRACTOR_PARSER_VERSION = "contractor-jsonl-v1";
export const CONTRACTOR_MATCH_VERSION = "contractor-identity-match-v1";
export const CONTRACTOR_SOURCE_AUTHORIZATION_VERSION =
  "contractor-source-authorization-v1";
export const CONTRACTOR_RELATIONSHIP_EVIDENCE_VERSION =
  "permit-contractor-relationship-v1";
export const MAX_CONTRACTOR_SOURCE_BYTES = 512 * 1024 * 1024;
export const MAX_CONTRACTOR_TERMS_EVIDENCE_BYTES = 10 * 1024 * 1024;
export const MAX_CONTRACTOR_JSONL_LINE_BYTES = 128 * 1024;
export const MAX_CONTRACTOR_RECORDS = 1_000_000;
export const MAX_CONTRACTOR_PROVIDER_ID_BYTES = 256;

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const isoDateTimeSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine(
    (value) => Number.isFinite(Date.parse(value)),
    "must be a UTC timestamp",
  );
const relativeDataPathSchema = z
  .string()
  .min(1)
  .max(1_000)
  .refine(
    (value) =>
      !value.startsWith("/") &&
      !value.includes("\\") &&
      !value.includes("//") &&
      !value.includes("\0") &&
      value
        .split("/")
        .every(
          (component) =>
            component !== "" && component !== "." && component !== "..",
        ),
    "must be a normalized DATA_DIR-relative path",
  );
const httpsUrlSchema = z
  .string()
  .url()
  .max(2_000)
  .refine((value) => {
    const url = new URL(value);
    return (
      value === value.toLowerCase() &&
      url.protocol === "https:" &&
      url.port === "" &&
      url.username === "" &&
      url.password === "" &&
      url.search === "" &&
      url.hash === ""
    );
  }, "must be a credential-free HTTPS URL")
  .refine(
    (value) =>
      !/(?:^|\/)(?:access[-_]?key|api[-_]?key|authorization|bearer|cookie|password|private[-_]?key|proxy[-_]?authorization|secret|signature|token)(?:[=/:]|$)/i.test(
        new URL(value).pathname,
      ),
    "must not contain a secret-shaped path segment",
  );
const httpsOriginSchema = z
  .string()
  .max(253)
  .refine((value) => {
    try {
      const url = new URL(value);
      return (
        value === value.toLowerCase() &&
        url.protocol === "https:" &&
        url.port === "" &&
        url.username === "" &&
        url.password === "" &&
        url.pathname === "/" &&
        url.search === "" &&
        url.hash === "" &&
        url.origin === value
      );
    } catch {
      return false;
    }
  }, "must be an exact credential-free HTTPS origin");
const sourceFileBindingSchema = z.strictObject({
  byteSize: z.number().int().nonnegative().max(MAX_CONTRACTOR_SOURCE_BYTES),
  relativePath: relativeDataPathSchema,
  sha256: sha256Schema,
});
const termsEvidenceFileBindingSchema = z.strictObject({
  byteSize: z
    .number()
    .int()
    .positive()
    .max(MAX_CONTRACTOR_TERMS_EVIDENCE_BYTES),
  relativePath: relativeDataPathSchema,
  sha256: sha256Schema,
});
const unavailableReasonSchema = z.enum([
  "not_provided_by_source",
  "not_recorded_during_acquisition",
]);
const observationWindowSchema = z.discriminatedUnion("status", [
  z
    .strictObject({
      end: isoDateTimeSchema,
      reason: z.null(),
      start: isoDateTimeSchema,
      status: z.literal("recorded"),
    })
    .refine(
      (value) => Date.parse(value.end) >= Date.parse(value.start),
      "observation end must not precede observation start",
    ),
  z.strictObject({
    end: z.null(),
    reason: unavailableReasonSchema,
    start: z.null(),
    status: z.literal("unavailable"),
  }),
]);
const retrievalSchema = z.discriminatedUnion("status", [
  z.strictObject({
    at: isoDateTimeSchema,
    reason: z.null(),
    status: z.literal("recorded"),
  }),
  z.strictObject({
    at: z.null(),
    reason: unavailableReasonSchema,
    status: z.literal("unavailable"),
  }),
]);

const contractorProviderSchema = z.enum([
  "better_business_bureau",
  "official_license_source",
]);
const acquisitionMethodSchema = z.enum([
  "authorized_api",
  "licensed_export",
  "owner_supplied_file",
]);
const sourceClassificationSchema = z.enum(["official", "third_party"]);
const categoryFiltersSchema = z.array(z.string().min(1).max(200)).max(100);
const coverageGeographySchema = z.string().min(1).max(500);
const nonSecretApproverReferenceSchema = z
  .string()
  .min(1)
  .max(200)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._:@/-]*$/);

const contractorSourceAuthorizationPayloadSchema = z
  .strictObject({
    acquisitionMethod: acquisitionMethodSchema,
    approvedAt: isoDateTimeSchema,
    approverReference: nonSecretApproverReferenceSchema,
    authorizedSourceOrigins: z.array(httpsOriginSchema).min(1).max(100),
    categoryFilters: categoryFiltersSchema,
    coverageGeography: coverageGeographySchema,
    decision: z.literal("approved_for_staging"),
    policyVersion: z.literal(CONTRACTOR_SOURCE_AUTHORIZATION_VERSION),
    provider: contractorProviderSchema,
    sourceClassification: sourceClassificationSchema,
    termsEvidenceFile: termsEvidenceFileBindingSchema,
  })
  .superRefine((value, context) => {
    const expectedClassification =
      value.provider === "better_business_bureau" ? "third_party" : "official";
    if (value.sourceClassification !== expectedClassification) {
      context.addIssue({
        code: "custom",
        message: "Contractor authorization classification is inconsistent",
      });
    }
    if (
      new Set(value.authorizedSourceOrigins).size !==
      value.authorizedSourceOrigins.length
    ) {
      context.addIssue({
        code: "custom",
        message: "Contractor authorization origins must be distinct",
      });
    }
  });

export const contractorSourceAuthorizationSchema = z.strictObject({
  authorizationId: z.string().regex(/^contractorauthorization_[a-f0-9]{32}$/),
  authorizationSha256: sha256Schema,
  payload: contractorSourceAuthorizationPayloadSchema,
});

export const contractorSourceRowSchema = z
  .strictObject({
    accredited: z.boolean().nullable(),
    businessAddress: z.string().min(1).max(1_000).nullable(),
    businessName: z.string().min(1).max(500),
    categories: z.array(z.string().min(1).max(200)).max(100),
    licenseIssuer: z.string().min(1).max(300).nullable(),
    licenseJurisdiction: z.string().min(1).max(200).nullable(),
    licenseNumber: z.string().min(1).max(200).nullable(),
    phone: z.string().min(1).max(100).nullable(),
    provider: contractorProviderSchema,
    providerRecordId: z.string().min(1).max(MAX_CONTRACTOR_PROVIDER_ID_BYTES),
    rating: z.string().min(1).max(100).nullable(),
    schemaVersion: z.literal(CONTRACTOR_RECORD_SCHEMA_VERSION),
    sourceUrl: httpsUrlSchema,
  })
  .superRefine((value, context) => {
    const licenseFields = [
      value.licenseNumber,
      value.licenseIssuer,
      value.licenseJurisdiction,
    ];
    const present = licenseFields.filter((field) => field !== null).length;
    if (present !== 0 && present !== licenseFields.length) {
      context.addIssue({
        code: "custom",
        message:
          "license number, issuer and jurisdiction must be supplied together",
      });
    }
    if (value.provider === "official_license_source" && present !== 3) {
      context.addIssue({
        code: "custom",
        message: "official license records require complete license identity",
      });
    }
  });

const contractorDatasetPayloadSchema = z
  .strictObject({
    acquisitionMethod: acquisitionMethodSchema,
    authorizationId: z.string().regex(/^contractorauthorization_[a-f0-9]{32}$/),
    categoryFilters: categoryFiltersSchema,
    counts: z.strictObject({
      accepted: z.number().int().nonnegative().max(MAX_CONTRACTOR_RECORDS),
      duplicateProviderIds: z
        .number()
        .int()
        .nonnegative()
        .max(MAX_CONTRACTOR_RECORDS),
      parsed: z.number().int().nonnegative().max(MAX_CONTRACTOR_RECORDS),
      rejected: z.number().int().nonnegative().max(MAX_CONTRACTOR_RECORDS),
      source: z.number().int().nonnegative().max(MAX_CONTRACTOR_RECORDS),
    }),
    coverageGeography: coverageGeographySchema,
    coverageMode: z.literal("partial"),
    createdAt: isoDateTimeSchema,
    licenseTerms: z.strictObject({
      evidenceFile: termsEvidenceFileBindingSchema,
      evidenceSha256: sha256Schema,
      status: z.literal("verified_compatible"),
    }),
    manifestVersion: z.literal(CONTRACTOR_DATASET_MANIFEST_VERSION),
    observationWindow: observationWindowSchema,
    parserVersion: z.literal(CONTRACTOR_PARSER_VERSION),
    provider: contractorProviderSchema,
    recordSchemaVersion: z.literal(CONTRACTOR_RECORD_SCHEMA_VERSION),
    retrieval: retrievalSchema,
    sourceClassification: sourceClassificationSchema,
    sourceFile: sourceFileBindingSchema,
    sourceUrls: z.array(httpsUrlSchema).min(1).max(100),
    transformVersion: z.literal(CONTRACTOR_MATCH_VERSION),
  })
  .superRefine((value, context) => {
    if (
      value.counts.source !== value.counts.accepted + value.counts.rejected ||
      value.counts.parsed > value.counts.source ||
      value.counts.accepted > value.counts.parsed ||
      value.counts.duplicateProviderIds > value.counts.rejected ||
      value.counts.accepted + value.counts.duplicateProviderIds >
        value.counts.parsed
    ) {
      context.addIssue({
        code: "custom",
        message: "Contractor manifest count controls are inconsistent",
      });
    }
    if (
      value.sourceFile.relativePath ===
        value.licenseTerms.evidenceFile.relativePath ||
      value.sourceFile.sha256 === value.licenseTerms.evidenceFile.sha256
    ) {
      context.addIssue({
        code: "custom",
        message: "Contractor source and terms evidence must be distinct",
      });
    }
  });

export const contractorDatasetManifestSchema = z.strictObject({
  datasetId: z.string().regex(/^contractordataset_[a-f0-9]{32}$/),
  manifestSha256: sha256Schema,
  payload: contractorDatasetPayloadSchema,
});

export type ContractorSourceRow = z.infer<typeof contractorSourceRowSchema>;
export type ContractorSourceAuthorization = z.infer<
  typeof contractorSourceAuthorizationSchema
>;
export type ContractorDatasetManifest = z.infer<
  typeof contractorDatasetManifestSchema
>;

export interface ContractorSourceRecord extends ContractorSourceRow {
  recordVersionId: string;
  sourceRecordSha256: string;
}

export interface ContractorParseCounts {
  accepted: number;
  duplicateProviderIds: number;
  parsed: number;
  rejected: number;
  source: number;
}

export interface ContractorIdentity {
  businessAddress: string | null;
  businessName: string;
  licenseIssuer: string | null;
  licenseJurisdiction: string | null;
  licenseNumber: string | null;
  phone: string | null;
  provider: ContractorSourceRow["provider"];
  providerRecordId: string | null;
}

export function buildContractorSourceAuthorization(
  payload: z.input<typeof contractorSourceAuthorizationPayloadSchema>,
): ContractorSourceAuthorization {
  const parsed = contractorSourceAuthorizationPayloadSchema.parse(payload);
  const authorizationSha256 = canonicalJsonSha256(parsed);
  return {
    authorizationId: deterministicId("contractorauthorization", [
      CONTRACTOR_SOURCE_AUTHORIZATION_VERSION,
      parsed.provider,
      authorizationSha256,
    ]),
    authorizationSha256,
    payload: parsed,
  };
}

export function validateContractorSourceAuthorization(
  value: unknown,
): ContractorSourceAuthorization {
  const authorization = contractorSourceAuthorizationSchema.parse(value);
  const expected = buildContractorSourceAuthorization(authorization.payload);
  if (
    authorization.authorizationId !== expected.authorizationId ||
    authorization.authorizationSha256 !== expected.authorizationSha256
  ) {
    throw new DurableInputError(
      "Contractor source authorization identity mismatch",
    );
  }
  return authorization;
}

async function verifyFileBinding(options: {
  dataDir: string;
  expected: z.infer<
    typeof sourceFileBindingSchema | typeof termsEvidenceFileBindingSchema
  >;
  label: string;
}): Promise<
  SourceObject | { byteSize: number; relativePath: string; sha256: string }
> {
  const current = await bindDataFile(
    options.dataDir,
    await resolveBoundDataPath(options.dataDir, options.expected.relativePath),
  );
  if (
    current.byteSize !== options.expected.byteSize ||
    current.sha256 !== options.expected.sha256 ||
    current.relativePath !== options.expected.relativePath
  ) {
    throw new DurableInputError(`${options.label} file binding mismatch`);
  }
  return current;
}

async function* readBoundedJsonlLines(
  inputPath: string,
): AsyncGenerator<string> {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const decode = (value: Buffer): string => {
    try {
      return decoder.decode(value);
    } catch {
      throw new DurableInputError("Contractor JSONL contains invalid UTF-8");
    }
  };
  let pending = Buffer.alloc(0);
  for await (const chunkValue of createReadStream(inputPath, {
    highWaterMark: 64 * 1024,
  })) {
    const chunk = Buffer.isBuffer(chunkValue)
      ? chunkValue
      : Buffer.from(chunkValue);
    pending = Buffer.concat([pending, chunk]);
    let newline = pending.indexOf(0x0a);
    while (newline >= 0) {
      let line = pending.subarray(0, newline);
      pending = pending.subarray(newline + 1);
      if (line.at(-1) === 0x0d) line = line.subarray(0, -1);
      if (line.byteLength > MAX_CONTRACTOR_JSONL_LINE_BYTES) {
        throw new DurableInputError("Contractor JSONL line exceeds byte limit");
      }
      yield decode(line);
      newline = pending.indexOf(0x0a);
    }
    if (pending.byteLength > MAX_CONTRACTOR_JSONL_LINE_BYTES) {
      throw new DurableInputError("Contractor JSONL line exceeds byte limit");
    }
  }
  if (pending.byteLength > 0) {
    if (pending.at(-1) === 0x0d) pending = pending.subarray(0, -1);
    if (pending.byteLength > MAX_CONTRACTOR_JSONL_LINE_BYTES) {
      throw new DurableInputError("Contractor JSONL line exceeds byte limit");
    }
    yield decode(pending);
  }
}

function contractorBucket(providerRecordId: string): string {
  return createHash("sha256")
    .update(providerRecordId)
    .digest("hex")
    .slice(0, 2);
}

async function closeHandles(handles: Map<string, FileHandle>): Promise<void> {
  await Promise.all([...handles.values()].map((handle) => handle.close()));
  handles.clear();
}

export interface ContractorIdentityMatch {
  confidence: number;
  evidenceSha256: string;
  method:
    | "exact_license_number"
    | "exact_provider_identifier"
    | "legal_name_address_phone"
    | "name_only_ambiguous"
    | "no_match";
  status: "ambiguous" | "linked" | "unmatched";
}

const contractorIdentityMatchEvidencePayloadSchema = z.strictObject({
  candidateSourceRecordSha256: z
    .string()
    .regex(/^sha256:[a-f0-9]{64}$/)
    .nullable(),
  confidence: z.number().min(0).max(1),
  matchVersion: z.literal(CONTRACTOR_MATCH_VERSION),
  matchedContractorId: z.string().min(1).max(200).nullable(),
  method: z.enum([
    "exact_license_number",
    "exact_provider_identifier",
    "legal_name_address_phone",
    "name_only_ambiguous",
    "no_match",
  ]),
  recordVersionId: z.string().regex(/^contractorversion_[a-f0-9]{32}$/),
  sourceRecordSha256: sha256Schema,
  status: z.enum(["ambiguous", "linked", "unmatched"]),
});

export function buildContractorIdentityMatchEvidence(options: {
  candidateSourceRecordSha256: string | null;
  candidate: ContractorIdentity;
  matchedContractorId: string | null;
  recordVersionId: string;
  source: ContractorIdentity;
  sourceRecordSha256: string;
}) {
  const match = matchContractorIdentity(options.source, options.candidate);
  if (match.status === "linked") {
    throw new DurableInputError(
      "Normalized contractor linkage is disabled pending immutable target versions",
    );
  }
  const payload = contractorIdentityMatchEvidencePayloadSchema.parse({
    candidateSourceRecordSha256: options.candidateSourceRecordSha256,
    confidence: match.confidence,
    matchVersion: CONTRACTOR_MATCH_VERSION,
    matchedContractorId: options.matchedContractorId,
    method: match.method,
    recordVersionId: options.recordVersionId,
    sourceRecordSha256: options.sourceRecordSha256,
    status: match.status,
  });
  if (
    (payload.status === "linked") !== (payload.matchedContractorId !== null) ||
    (payload.status === "linked") !==
      (payload.candidateSourceRecordSha256 !== null)
  ) {
    throw new DurableInputError(
      "Contractor match evidence lacks an exact candidate binding",
    );
  }
  return {
    evidenceSha256: canonicalJsonSha256(payload),
    payload,
  };
}

function normalizeIdentifier(value: string | null): string | null {
  const normalized = value?.replace(/[^a-z0-9]/gi, "").toUpperCase() ?? "";
  return normalized.length > 0 ? normalized : null;
}

function normalizeText(value: string | null): string | null {
  const normalized = value?.trim().replace(/\s+/g, " ").toUpperCase() ?? "";
  return normalized.length > 0 ? normalized : null;
}

export function buildContractorDatasetManifest(
  payload: z.input<typeof contractorDatasetPayloadSchema>,
): ContractorDatasetManifest {
  const parsed = contractorDatasetPayloadSchema.parse(payload);
  const manifestSha256 = canonicalJsonSha256(parsed);
  return {
    datasetId: deterministicId("contractordataset", [
      CONTRACTOR_DATASET_MANIFEST_VERSION,
      parsed.provider,
      manifestSha256,
    ]),
    manifestSha256,
    payload: parsed,
  };
}

export function validateContractorDatasetManifest(
  value: unknown,
): ContractorDatasetManifest {
  const manifest = contractorDatasetManifestSchema.parse(value);
  const expected = buildContractorDatasetManifest(manifest.payload);
  if (
    manifest.datasetId !== expected.datasetId ||
    manifest.manifestSha256 !== expected.manifestSha256
  ) {
    throw new DurableInputError(
      "Contractor dataset manifest identity mismatch",
    );
  }
  const expectedClassification =
    manifest.payload.provider === "better_business_bureau"
      ? "third_party"
      : "official";
  if (manifest.payload.sourceClassification !== expectedClassification) {
    throw new DurableInputError(
      "Contractor provider classification is inconsistent",
    );
  }
  if (
    manifest.payload.licenseTerms.evidenceSha256 !==
    manifest.payload.licenseTerms.evidenceFile.sha256
  ) {
    throw new DurableInputError("Contractor terms evidence identity mismatch");
  }
  return manifest;
}

export async function parseVerifiedContractorJsonl(options: {
  authorization: ContractorSourceAuthorization;
  dataDir: string;
  manifest: ContractorDatasetManifest;
  onRecord: (record: ContractorSourceRecord) => Promise<void> | void;
}): Promise<ContractorParseCounts> {
  const manifest = validateContractorDatasetManifest(options.manifest);
  const authorization = validateContractorSourceAuthorization(
    options.authorization,
  );
  if (
    manifest.payload.authorizationId !== authorization.authorizationId ||
    manifest.payload.provider !== authorization.payload.provider ||
    manifest.payload.sourceClassification !==
      authorization.payload.sourceClassification ||
    manifest.payload.acquisitionMethod !==
      authorization.payload.acquisitionMethod ||
    manifest.payload.coverageGeography !==
      authorization.payload.coverageGeography ||
    canonicalJson(manifest.payload.categoryFilters) !==
      canonicalJson(authorization.payload.categoryFilters) ||
    manifest.payload.sourceUrls.some(
      (value) =>
        !authorization.payload.authorizedSourceOrigins.includes(
          new URL(value).origin,
        ),
    ) ||
    canonicalJson(manifest.payload.licenseTerms.evidenceFile) !==
      canonicalJson(authorization.payload.termsEvidenceFile)
  ) {
    throw new DurableInputError(
      "Contractor dataset does not match its source authorization",
    );
  }
  const current = await verifyFileBinding({
    dataDir: options.dataDir,
    expected: manifest.payload.sourceFile,
    label: "Contractor source",
  });
  await verifyFileBinding({
    dataDir: options.dataDir,
    expected: manifest.payload.licenseTerms.evidenceFile,
    label: "Contractor terms evidence",
  });
  const inputPath = await resolveBoundDataPath(
    options.dataDir,
    current.relativePath,
  );
  const sourceOrigins = new Set(
    manifest.payload.sourceUrls.map((value) => new URL(value).origin),
  );
  const counts: ContractorParseCounts = {
    accepted: 0,
    duplicateProviderIds: 0,
    parsed: 0,
    rejected: 0,
    source: 0,
  };
  const spoolDirectory = await mkdtemp(
    path.join(path.dirname(inputPath), ".contractor-validation-"),
  );
  const handles = new Map<string, FileHandle>();
  const buckets = new Set<string>();
  try {
    // Validate and spool before exposing any record. A late terminal source
    // error therefore cannot leave callback-side effects behind.
    for await (const line of readBoundedJsonlLines(inputPath)) {
      if (line.trim().length === 0) continue;
      counts.source += 1;
      if (counts.source > MAX_CONTRACTOR_RECORDS) {
        throw new DurableInputError("Contractor source exceeds record limit");
      }
      let raw: unknown;
      try {
        raw = JSON.parse(line) as unknown;
        counts.parsed += 1;
      } catch {
        counts.rejected += 1;
        continue;
      }
      const parsed = contractorSourceRowSchema.safeParse(raw);
      if (
        !parsed.success ||
        parsed.data.provider !== manifest.payload.provider ||
        !sourceOrigins.has(new URL(parsed.data.sourceUrl).origin)
      ) {
        counts.rejected += 1;
        continue;
      }
      const bucket = contractorBucket(parsed.data.providerRecordId);
      buckets.add(bucket);
      let handle = handles.get(bucket);
      if (!handle) {
        handle = await open(
          path.join(spoolDirectory, `${bucket}.jsonl`),
          "a",
          0o600,
        );
        handles.set(bucket, handle);
      }
      await handle.write(`${canonicalJson(parsed.data)}\n`);
    }
    await closeHandles(handles);

    // Count exact duplicates one bounded hash bucket at a time. Heap use is
    // independent of the complete accepted-record population.
    for (const bucket of [...buckets].sort()) {
      const providerIds = new Set<string>();
      for await (const line of readBoundedJsonlLines(
        path.join(spoolDirectory, `${bucket}.jsonl`),
      )) {
        const row = contractorSourceRowSchema.parse(JSON.parse(line));
        if (providerIds.has(row.providerRecordId)) {
          counts.duplicateProviderIds += 1;
          counts.rejected += 1;
        } else {
          providerIds.add(row.providerRecordId);
          counts.accepted += 1;
        }
      }
    }
    if (canonicalJson(counts) !== canonicalJson(manifest.payload.counts)) {
      throw new DurableInputError("Contractor source count controls mismatch");
    }

    // Only a fully validated source reaches the caller. A future database
    // Loader must invoke this callback inside its transaction.
    for (const bucket of [...buckets].sort()) {
      const providerIds = new Set<string>();
      for await (const line of readBoundedJsonlLines(
        path.join(spoolDirectory, `${bucket}.jsonl`),
      )) {
        const row = contractorSourceRowSchema.parse(JSON.parse(line));
        if (providerIds.has(row.providerRecordId)) continue;
        providerIds.add(row.providerRecordId);
        const sourceRecordSha256 = canonicalJsonSha256(row);
        await options.onRecord({
          ...row,
          recordVersionId: deterministicId("contractorversion", [
            CONTRACTOR_RECORD_SCHEMA_VERSION,
            manifest.datasetId,
            row.provider,
            row.providerRecordId,
            sourceRecordSha256,
          ]),
          sourceRecordSha256,
        });
      }
    }
    return counts;
  } finally {
    await closeHandles(handles);
    await rm(spoolDirectory, { force: true, recursive: true });
  }
}

export function matchContractorIdentity(
  source: ContractorIdentity,
  candidate: ContractorIdentity,
): ContractorIdentityMatch {
  const sourceLicense = normalizeIdentifier(source.licenseNumber);
  const candidateLicense = normalizeIdentifier(candidate.licenseNumber);
  const sameLicenseAuthority =
    normalizeIdentifier(source.licenseIssuer) !== null &&
    normalizeIdentifier(source.licenseIssuer) ===
      normalizeIdentifier(candidate.licenseIssuer) &&
    normalizeIdentifier(source.licenseJurisdiction) !== null &&
    normalizeIdentifier(source.licenseJurisdiction) ===
      normalizeIdentifier(candidate.licenseJurisdiction);
  const sameLicense =
    sourceLicense !== null &&
    sourceLicense === candidateLicense &&
    sameLicenseAuthority;
  const sameProviderId =
    source.provider === candidate.provider &&
    source.providerRecordId !== null &&
    source.providerRecordId === candidate.providerRecordId;
  const sameName =
    normalizeText(source.businessName) ===
    normalizeText(candidate.businessName);
  const sameAddress =
    normalizeText(source.businessAddress) !== null &&
    normalizeText(source.businessAddress) ===
      normalizeText(candidate.businessAddress);
  const samePhone =
    normalizeIdentifier(source.phone) !== null &&
    normalizeIdentifier(source.phone) === normalizeIdentifier(candidate.phone);
  const method: ContractorIdentityMatch["method"] = sameLicense
    ? "exact_license_number"
    : sameProviderId
      ? "exact_provider_identifier"
      : sameName && sameAddress && samePhone
        ? "legal_name_address_phone"
        : sameName
          ? "name_only_ambiguous"
          : "no_match";
  const status: ContractorIdentityMatch["status"] =
    method === "name_only_ambiguous"
      ? "ambiguous"
      : method === "no_match"
        ? "unmatched"
        : "linked";
  const confidence =
    method === "exact_license_number" || method === "exact_provider_identifier"
      ? 1
      : method === "legal_name_address_phone"
        ? 0.95
        : method === "name_only_ambiguous"
          ? 0.4
          : 0;
  return {
    confidence,
    evidenceSha256: canonicalJsonSha256({
      candidate,
      matchVersion: CONTRACTOR_MATCH_VERSION,
      method,
      source,
      status,
    }),
    method,
    status,
  };
}

const permitContractorRelationshipEvidenceSchema = z.strictObject({
  contractorId: z.string().min(1).max(200),
  evidenceVersion: z.literal(CONTRACTOR_RELATIONSHIP_EVIDENCE_VERSION),
  matchBasis: z.enum([
    "permit_source_contractor_id",
    "permit_source_license_number",
    "permit_source_legal_name",
  ]),
  matchConfidence: z.number().min(0.95).max(1),
  permitId: z.string().regex(/^permit_[a-f0-9]{32}$/),
  permitSourceRecordHash: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  permitSourceRecordKey: z.string().min(1).max(500),
  propertyId: z.string().regex(/^property_[a-f0-9]{32}$/),
  relationshipRecordId: z.string().min(1).max(500),
  relationshipSourceSha256: z.string().regex(/^sha256:[a-f0-9]{64}$/),
  relationshipSourceSystem: z.literal("official_permit_source"),
});

export type PermitContractorRelationshipEvidence = z.infer<
  typeof permitContractorRelationshipEvidenceSchema
>;

export function buildPermitContractorRelationshipEvidence(value: unknown): {
  evidenceSha256: string;
  payload: PermitContractorRelationshipEvidence;
} {
  const payload = permitContractorRelationshipEvidenceSchema.parse(value);
  return { evidenceSha256: canonicalJsonSha256(payload), payload };
}

export function assertParcelContractorRelationshipEvidence(
  value: unknown,
): void {
  try {
    buildPermitContractorRelationshipEvidence(value);
  } catch {
    throw new DurableInputError(
      "Parcel-to-contractor linkage requires explicit permit/source evidence",
    );
  }
  throw new SourceAccessStopError(
    "Parcel-to-contractor linkage is disabled pending immutable permit and contractor source versions",
  );
}

export async function acquireBbbContractorDataset(): Promise<never> {
  throw new SourceAccessStopError(
    "BBB acquisition is disabled pending an authorized API/licensed export, terms review, and bounded request/cost approval",
  );
}

export function contractorSourceObject(
  manifest: ContractorDatasetManifest,
): SourceObject {
  const parsed = validateContractorDatasetManifest(manifest);
  return {
    ...parsed.payload.sourceFile,
    derivedFromSha256: null,
    lastModified: null,
    observedAt:
      parsed.payload.observationWindow.status === "recorded"
        ? parsed.payload.observationWindow.end
        : null,
    sourceId: deterministicId("source", [
      CONTRACTOR_DATASET_MANIFEST_VERSION,
      parsed.payload.provider,
      parsed.payload.sourceFile.sha256,
    ]),
    sourceIdentifier: parsed.payload.sourceUrls[0]!,
    sourceSystem: parsed.payload.provider,
    stage: "downloaded_source",
  };
}
