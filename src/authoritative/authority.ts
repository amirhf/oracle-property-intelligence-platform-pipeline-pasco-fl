import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  link,
  lstat,
  mkdir,
  readFile,
  realpath,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import unzipper from "unzipper";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";
import {
  AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  AUTHORITATIVE_PARCEL_SELECTION_SEED,
} from "../snapshot/coverage.js";
import {
  CANONICAL_SCHEMA_SHA256,
  PASCO_PARSER_VERSION,
  PASCO_TRANSFORM_VERSION,
  SNAPSHOT_MANIFEST_VERSION,
  bindDataFile,
  resolveBoundDataPath,
} from "../snapshot/model.js";

export const OWNER_AUTHORITY_RECORD_VERSION = "1.0.0";
export const OWNER_AUTHORITY_CLASS = "owner_assumed_authoritative_snapshot";
export const PASCO_AUTHORITY_CREATED_AT = "2026-08-30T00:00:00.000Z";
export const PASCO_PARCEL_SOURCE_URL =
  "https://ftp01.pascopa.com/real_estate/parcel.zip";
export const PASCO_PARCEL_ZIP_BYTES = 7_895_623;
export const PASCO_PARCEL_ZIP_SHA256 =
  "bffeead6aa18d9e53e5da9efafa5533b24e7d563b733b1d327bdc0a5cb62cac9";
export const PASCO_PARCEL_CSV_BYTES = 53_529_199;
export const PASCO_PARCEL_CSV_SHA256 =
  "8f06fe9ff8969869a606cf85b5a7722bebd247f5ff47b33288689c3aa4160545";
export const PASCO_PARCEL_FOLIO_COUNT = 325_213;
export const PASCO_PARCEL_FOLIO_SET_SHA256 =
  "3cb676d4a52a35f7bc2bcf1a13b5a4c1ca5f21c005bb867078dca1a4d428dfab";
export const PASCO_PARCEL_LAST_MODIFIED = "2026-08-23T11:07:02.000Z";
export const PASCO_PARCEL_ETAG = '"0f2386ef32dd1:0"';
export const PASCO_PARCEL_MEMBERSHIP_CLAIM =
  "Authoritative completeness applies to parcel membership represented by the exact hash-bound August 23, 2026 Pasco Property Appraiser parcel.zip snapshot. It does not assert that the archive contains every parcel counted under every other Pasco reporting definition. GIS, coordinate, related-fact, permit, and contractor coverage are measured and reported separately.";
export const PASCO_PARCEL_SCOPE_MEMBERSHIP_RULE =
  "pasco_appraiser:owner_accepted_complete_parcel_membership-v1";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const authorityRecordPayloadSchema = z.strictObject({
  archive: z.strictObject({
    entries: z.array(
      z.strictObject({
        byteSize: z.number().int().positive(),
        filename: z.string(),
        sha256: sha256Schema,
      }),
    ),
    etag: z.string(),
    filename: z.literal("parcel.zip"),
    lastModified: z.string().datetime(),
    sha256: sha256Schema,
    sourceCycleDate: z.literal("2026-08-23"),
    sourceDescription: z.literal("Parcel Level Detail"),
    sourceUrl: z.literal(PASCO_PARCEL_SOURCE_URL),
    byteSize: z.literal(PASCO_PARCEL_ZIP_BYTES),
  }),
  authorityClass: z.literal(OWNER_AUTHORITY_CLASS),
  canonicalSchemaSha256: z.literal(CANONICAL_SCHEMA_SHA256),
  counts: z.strictObject({
    accepted: z.literal(PASCO_PARCEL_FOLIO_COUNT),
    distinctFolios: z.literal(PASCO_PARCEL_FOLIO_COUNT),
    duplicateFolios: z.literal(0),
    expected: z.literal(PASCO_PARCEL_FOLIO_COUNT),
    parsed: z.literal(PASCO_PARCEL_FOLIO_COUNT),
    rejected: z.literal(0),
    source: z.literal(PASCO_PARCEL_FOLIO_COUNT),
  }),
  coverageDefinition: z.literal(PASCO_PARCEL_MEMBERSHIP_CLAIM),
  createdAt: z.literal(PASCO_AUTHORITY_CREATED_AT),
  decision: z.strictObject({
    acceptedRisk: z.literal("independent_official_control_total_not_available"),
    decisionTextSha256: sha256Schema,
    ownerAuthorizedControlTotal: z.literal(PASCO_PARCEL_FOLIO_COUNT),
  }),
  exclusions: z.tuple([
    z.literal("gis_and_coordinate_completeness"),
    z.literal("related_fact_completeness"),
    z.literal("permit_and_contractor_coverage"),
    z.literal("all_other_pasco_reporting_definitions"),
  ]),
  parserVersion: z.literal(PASCO_PARSER_VERSION),
  selection: z.strictObject({
    algorithm: z.literal(AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM),
    seed: z.literal(AUTHORITATIVE_PARCEL_SELECTION_SEED),
    selectedRecordSha256: z.literal(PASCO_PARCEL_FOLIO_SET_SHA256),
    selectionSize: z.literal(PASCO_PARCEL_FOLIO_COUNT),
  }),
  snapshotFormatVersion: z.literal(SNAPSHOT_MANIFEST_VERSION),
  sortedFolioSetSha256: z.literal(PASCO_PARCEL_FOLIO_SET_SHA256),
  sourceSystem: z.literal("pasco_appraiser"),
  transformVersion: z.literal(PASCO_TRANSFORM_VERSION),
  unresolvedSemanticDiscrepancy: z.strictObject({
    publishedRealPropertyParcelStatistic: z.literal(335_946),
    status: z.literal("unreconciled_membership_or_timing_semantics"),
  }),
  version: z.literal(OWNER_AUTHORITY_RECORD_VERSION),
});

export type OwnerAuthorityRecordPayload = z.infer<
  typeof authorityRecordPayloadSchema
>;

export interface OwnerAuthorityRecord {
  authorityClass: typeof OWNER_AUTHORITY_CLASS;
  authorityRecordId: string;
  completenessEvidenceSha256: string;
  decisionSha256: string;
  payload: OwnerAuthorityRecordPayload;
}

async function fileSha256(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

function mismatch(label: string): never {
  throw new DurableInputError(`Authoritative source mismatch (${label})`);
}

export async function verifyExactPascoParcelSource(dataDir: string): Promise<{
  csvPath: string;
  zipPath: string;
}> {
  const dataRoot = await realpath(dataDir);
  const rawZipPath = path.join(
    dataRoot,
    "pasco/raw/appraiser/2026-08-23/parcel.zip",
  );
  const rawCsvPath = path.join(
    dataRoot,
    "pasco/staging/appraiser/2026-08-23/parcel.csv",
  );
  const [zipLinkStat, csvLinkStat] = await Promise.all([
    lstat(rawZipPath),
    lstat(rawCsvPath),
  ]);
  if (zipLinkStat.isSymbolicLink() || csvLinkStat.isSymbolicLink())
    mismatch("source symlink");
  const zipPath = await resolveBoundDataPath(
    dataDir,
    "pasco/raw/appraiser/2026-08-23/parcel.zip",
  );
  const csvPath = await resolveBoundDataPath(
    dataDir,
    "pasco/staging/appraiser/2026-08-23/parcel.csv",
  );
  const [zipStat, csvStat, zipHash, csvHash] = await Promise.all([
    stat(zipPath),
    stat(csvPath),
    fileSha256(zipPath),
    fileSha256(csvPath),
  ]);
  if (!zipStat.isFile()) mismatch("zip type");
  if (!csvStat.isFile()) mismatch("csv type");
  if (
    zipStat.size !== PASCO_PARCEL_ZIP_BYTES ||
    zipHash !== PASCO_PARCEL_ZIP_SHA256
  )
    mismatch("zip bytes/hash");
  if (
    csvStat.size !== PASCO_PARCEL_CSV_BYTES ||
    csvHash !== PASCO_PARCEL_CSV_SHA256
  )
    mismatch("csv bytes/hash");
  const archive = await unzipper.Open.file(zipPath);
  if (archive.files.length !== 1 || archive.files[0]?.path !== "parcel.csv")
    mismatch("archive inventory");
  const extractedHash = createHash("sha256");
  let extractedBytes = 0;
  for await (const chunk of archive.files[0].stream()) {
    const bytes = Buffer.from(chunk as Uint8Array);
    extractedHash.update(bytes);
    extractedBytes += bytes.byteLength;
  }
  if (
    extractedBytes !== PASCO_PARCEL_CSV_BYTES ||
    extractedHash.digest("hex") !== PASCO_PARCEL_CSV_SHA256
  )
    mismatch("archive extraction binding");
  return { csvPath, zipPath };
}

export function buildOwnerAuthorityRecord(counts: {
  accepted: number;
  distinctFolios: number;
  duplicateFolios: number;
  parsed: number;
  rejected: number;
  source: number;
  sortedFolioSetSha256: string;
}): OwnerAuthorityRecord {
  if (
    counts.source !== PASCO_PARCEL_FOLIO_COUNT ||
    counts.parsed !== PASCO_PARCEL_FOLIO_COUNT ||
    counts.accepted !== PASCO_PARCEL_FOLIO_COUNT ||
    counts.distinctFolios !== PASCO_PARCEL_FOLIO_COUNT ||
    counts.rejected !== 0 ||
    counts.duplicateFolios !== 0 ||
    counts.sortedFolioSetSha256 !== PASCO_PARCEL_FOLIO_SET_SHA256
  )
    mismatch("row/folio controls");
  const normalizedDecision = {
    authorityClass: OWNER_AUTHORITY_CLASS,
    controlTotal: PASCO_PARCEL_FOLIO_COUNT,
    coverageDefinition: PASCO_PARCEL_MEMBERSHIP_CLAIM,
    exactArchiveSha256: PASCO_PARCEL_ZIP_SHA256,
    unresolvedStatistic: 335_946,
  };
  const decisionSha256 = canonicalJsonSha256(normalizedDecision);
  const payload: OwnerAuthorityRecordPayload = {
    archive: {
      byteSize: PASCO_PARCEL_ZIP_BYTES,
      entries: [
        {
          byteSize: PASCO_PARCEL_CSV_BYTES,
          filename: "parcel.csv",
          sha256: PASCO_PARCEL_CSV_SHA256,
        },
      ],
      etag: PASCO_PARCEL_ETAG,
      filename: "parcel.zip",
      lastModified: PASCO_PARCEL_LAST_MODIFIED,
      sha256: PASCO_PARCEL_ZIP_SHA256,
      sourceCycleDate: "2026-08-23",
      sourceDescription: "Parcel Level Detail",
      sourceUrl: PASCO_PARCEL_SOURCE_URL,
    },
    authorityClass: OWNER_AUTHORITY_CLASS,
    canonicalSchemaSha256: CANONICAL_SCHEMA_SHA256,
    counts: {
      accepted: PASCO_PARCEL_FOLIO_COUNT,
      distinctFolios: PASCO_PARCEL_FOLIO_COUNT,
      duplicateFolios: 0,
      expected: PASCO_PARCEL_FOLIO_COUNT,
      parsed: PASCO_PARCEL_FOLIO_COUNT,
      rejected: 0,
      source: PASCO_PARCEL_FOLIO_COUNT,
    },
    coverageDefinition: PASCO_PARCEL_MEMBERSHIP_CLAIM,
    createdAt: PASCO_AUTHORITY_CREATED_AT,
    decision: {
      acceptedRisk: "independent_official_control_total_not_available",
      decisionTextSha256: decisionSha256,
      ownerAuthorizedControlTotal: PASCO_PARCEL_FOLIO_COUNT,
    },
    exclusions: [
      "gis_and_coordinate_completeness",
      "related_fact_completeness",
      "permit_and_contractor_coverage",
      "all_other_pasco_reporting_definitions",
    ],
    parserVersion: PASCO_PARSER_VERSION,
    selection: {
      algorithm: AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
      seed: AUTHORITATIVE_PARCEL_SELECTION_SEED,
      selectedRecordSha256: PASCO_PARCEL_FOLIO_SET_SHA256,
      selectionSize: PASCO_PARCEL_FOLIO_COUNT,
    },
    snapshotFormatVersion: SNAPSHOT_MANIFEST_VERSION,
    sortedFolioSetSha256: PASCO_PARCEL_FOLIO_SET_SHA256,
    sourceSystem: "pasco_appraiser",
    transformVersion: PASCO_TRANSFORM_VERSION,
    unresolvedSemanticDiscrepancy: {
      publishedRealPropertyParcelStatistic: 335_946,
      status: "unreconciled_membership_or_timing_semantics",
    },
    version: OWNER_AUTHORITY_RECORD_VERSION,
  };
  authorityRecordPayloadSchema.parse(payload);
  const completenessEvidenceSha256 = canonicalJsonSha256(payload);
  return {
    authorityClass: OWNER_AUTHORITY_CLASS,
    authorityRecordId: deterministicId("authority", [
      OWNER_AUTHORITY_RECORD_VERSION,
      OWNER_AUTHORITY_CLASS,
      completenessEvidenceSha256,
    ]),
    completenessEvidenceSha256,
    decisionSha256,
    payload,
  };
}

export function validateOwnerAuthorityRecord(
  value: unknown,
): OwnerAuthorityRecord {
  const envelope = z
    .strictObject({
      authorityClass: z.literal(OWNER_AUTHORITY_CLASS),
      authorityRecordId: z.string().regex(/^authority_[a-f0-9]{32}$/),
      completenessEvidenceSha256: sha256Schema,
      decisionSha256: sha256Schema,
      payload: authorityRecordPayloadSchema,
    })
    .parse(value);
  const expected = buildOwnerAuthorityRecord({
    accepted: envelope.payload.counts.accepted,
    distinctFolios: envelope.payload.counts.distinctFolios,
    duplicateFolios: envelope.payload.counts.duplicateFolios,
    parsed: envelope.payload.counts.parsed,
    rejected: envelope.payload.counts.rejected,
    sortedFolioSetSha256: envelope.payload.sortedFolioSetSha256,
    source: envelope.payload.counts.source,
  });
  if (canonicalJson(expected) !== canonicalJson(envelope))
    mismatch("authority record identity");
  return envelope;
}

export async function writeOwnerAuthorityRecord(
  dataDir: string,
  record: OwnerAuthorityRecord,
): Promise<{ byteSize: number; filePath: string; sha256: string }> {
  validateOwnerAuthorityRecord(record);
  const relativePath = path.join(
    "pasco",
    "authority",
    record.authorityRecordId,
    "record.json",
  );
  const filePath = path.join(dataDir, relativePath);
  const body = `${canonicalJson(record)}\n`;
  await mkdir(path.dirname(filePath), { recursive: true });
  const contender = `${filePath}.${process.pid}.${randomUUID()}.part`;
  await writeFile(contender, body, { encoding: "utf8", mode: 0o600 });
  try {
    await link(contender, filePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    if ((await readFile(filePath, "utf8")) !== body)
      mismatch("immutable authority artifact");
  } finally {
    await rm(contender, { force: true });
  }
  const binding = await bindDataFile(dataDir, filePath);
  return { ...binding, filePath };
}
