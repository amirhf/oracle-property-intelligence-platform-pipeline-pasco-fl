import { randomUUID } from "node:crypto";
import {
  link,
  mkdir,
  readFile,
  realpath,
  unlink,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import {
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
  type HeadBucketCommandOutput,
  type HeadObjectCommandOutput,
  type ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import {
  delegatedIpnsEvidenceSchema,
  MAX_SIGNED_IPNS_RECORD_BYTES,
  observeDelegatedIpnsRecord,
} from "./delegated-ipns.js";
import {
  candidateSourceSnapshotLimitsSchema,
  PROTECTED_CANDIDATE_SAMPLE_ROLLBACK,
  type CandidateSourceSnapshotLimits,
} from "./candidate-source-snapshot-demo.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS,
  candidateSourceSnapshotPreflightBindingSchema,
  type CandidateSourceSnapshotPreflightBinding,
} from "./candidate-source-snapshot-preflight-binding.js";
import { calculateIpfsCid } from "./ipfs-cid.js";

export {
  candidateSourceSnapshotPreflightBindingSchema,
  type CandidateSourceSnapshotPreflightBinding,
} from "./candidate-source-snapshot-preflight-binding.js";

export const CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_VERSION = "1.0.0" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT =
  "https://s3.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT =
  "https://api.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_GATEWAY_ORIGIN =
  "https://ipfs.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_RESPONSE_BYTES = 64 * 1024;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_USAGE_RESPONSE_BYTES = 16 * 1024;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS = 1_000;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_ROLLBACK_OBJECT_BYTES =
  16 * 1024 * 1024;

const EMPTY_RESPONSE_SHA256 = sha256(Buffer.alloc(0));
const TARGET_BUCKET_SCAN_PREFIX = "";
const OPEN_DATA_CONFLICT_PREFIXES = [
  "publications/source-snapshot-demo-v1/",
  "publication-control/source-snapshot-demo-v1/",
] as const;
const QUERY_TABLE_CONFLICT_PREFIXES = [
  "query-tables/source-snapshot-demo-v1/",
] as const;

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.union([
  z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const domainSchema = z.enum(["open_data", "query_table"]);
const networkKeySchema = z.string().regex(/^k51[0-9a-z]{59}$/);
const resourceNameSchema = z
  .string()
  .min(12)
  .max(200)
  .regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/)
  .refine((value) => !/^(elephant|oracle|prism)(-|$)/.test(value));
const responseStatusSchema = z.number().int().min(100).max(599);
const requestCountSchema = z.number().int().positive().max(3);
const byteCountSchema = z
  .number()
  .int()
  .nonnegative()
  .max(Number.MAX_SAFE_INTEGER);

const targetIdentitySchema = z.strictObject({
  bucket: resourceNameSchema,
  ipnsLabel: resourceNameSchema,
  ipnsNetworkKey: networkKeySchema,
});

export interface CandidateSourceSnapshotPreflightConfig {
  apiEndpoint: typeof CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT;
  executorEnabled: false;
  limits: CandidateSourceSnapshotLimits;
  s3AccessKeyId: string;
  s3Endpoint: typeof CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT;
  s3SecretAccessKey: string;
  targets: {
    openData: z.infer<typeof targetIdentitySchema>;
    queryTable: z.infer<typeof targetIdentitySchema>;
  };
}

function required(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for source-snapshot preflight`);
  }
  return value;
}

function finiteNumber(environment: NodeJS.ProcessEnv, name: string): number {
  const value = Number(required(environment, name));
  if (!Number.isFinite(value)) throw new Error(`${name} must be finite`);
  return value;
}

export function loadCandidateSourceSnapshotPreflightConfig(
  environment: NodeJS.ProcessEnv = process.env,
): CandidateSourceSnapshotPreflightConfig {
  if (environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim() !== "false") {
    throw new Error(
      "Source-snapshot preflight requires CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=false",
    );
  }
  if (
    required(environment, "CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT") !==
    CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT
  ) {
    throw new Error(
      "Source-snapshot preflight requires the compiled S3 endpoint",
    );
  }
  if (
    required(environment, "CANDIDATE_DEMO_FILEBASE_API_ENDPOINT") !==
    CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT
  ) {
    throw new Error(
      "Source-snapshot preflight requires the compiled API endpoint",
    );
  }

  const s3AccessKeyId = required(
    environment,
    "CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID",
  );
  const s3SecretAccessKey = required(
    environment,
    "CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY",
  );
  const expectedApiToken = Buffer.from(
    `${s3AccessKeyId}:${s3SecretAccessKey}`,
  ).toString("base64");
  if (
    required(environment, "CANDIDATE_DEMO_FILEBASE_API_TOKEN") !==
    expectedApiToken
  ) {
    throw new Error(
      "Source-snapshot API token must be derived from the configured S3 credentials",
    );
  }

  const limits = candidateSourceSnapshotLimitsSchema.parse({
    maxBudgetUsd: finiteNumber(environment, "CANDIDATE_DEMO_MAX_BUDGET_USD"),
    maxConcurrency: finiteNumber(environment, "CANDIDATE_DEMO_MAX_CONCURRENCY"),
    maxObjectBytes: finiteNumber(
      environment,
      "CANDIDATE_DEMO_MAX_OBJECT_BYTES",
    ),
    maxObjects: finiteNumber(environment, "CANDIDATE_DEMO_MAX_OBJECTS"),
    maxRequests: finiteNumber(environment, "CANDIDATE_DEMO_MAX_REQUESTS"),
    maxRetries: finiteNumber(environment, "CANDIDATE_DEMO_MAX_RETRIES"),
    maxTotalBytes: finiteNumber(environment, "CANDIDATE_DEMO_MAX_TOTAL_BYTES"),
    requestTimeoutMs: finiteNumber(
      environment,
      "CANDIDATE_DEMO_REQUEST_TIMEOUT_MS",
    ),
  });
  const targets = {
    openData: targetIdentitySchema.parse({
      bucket: required(environment, "CANDIDATE_DEMO_OPEN_DATA_BUCKET"),
      ipnsLabel: required(environment, "CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL"),
      ipnsNetworkKey: required(
        environment,
        "CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY",
      ),
    }),
    queryTable: targetIdentitySchema.parse({
      bucket: required(environment, "CANDIDATE_DEMO_QUERY_TABLE_BUCKET"),
      ipnsLabel: required(environment, "CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL"),
      ipnsNetworkKey: required(
        environment,
        "CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY",
      ),
    }),
  };
  if (
    targets.openData.bucket === targets.queryTable.bucket ||
    targets.openData.ipnsLabel === targets.queryTable.ipnsLabel ||
    targets.openData.ipnsNetworkKey === targets.queryTable.ipnsNetworkKey ||
    !targets.openData.bucket.endsWith("-open-data-source-snapshot-demo-v1") ||
    !targets.queryTable.bucket.endsWith("-query-table-source-snapshot-demo-v1")
  ) {
    throw new Error("Source-snapshot targets require distinct resources");
  }
  const protectedBuckets = new Set([
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.bucket,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.bucket,
  ]);
  const protectedLabels = new Set([
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsLabel,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsLabel,
  ]);
  const protectedKeys = new Set([
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsNetworkKey,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsNetworkKey,
  ]);
  for (const target of [targets.openData, targets.queryTable]) {
    if (
      protectedBuckets.has(target.bucket) ||
      protectedLabels.has(target.ipnsLabel) ||
      protectedKeys.has(target.ipnsNetworkKey)
    ) {
      throw new Error(
        "Source-snapshot targets must remain separate from the protected sample",
      );
    }
  }

  return {
    apiEndpoint: CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
    executorEnabled: false,
    limits,
    s3AccessKeyId,
    s3Endpoint: CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
    s3SecretAccessKey,
    targets,
  };
}

const hashedRequestShape = {
  evidenceSha256: sha256Schema,
  httpStatus: responseStatusSchema,
  observedAt: z.string().datetime(),
  requestCount: requestCountSchema,
  responseBytes: byteCountSchema,
  responseSha256: sha256Schema,
};

const bucketHeadEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  responseBytes: z.literal(0),
  responseSha256: z.literal(EMPTY_RESPONSE_SHA256),
  status: z.literal("authenticated"),
});

const prefixScanEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  complete: z.literal(true),
  conflictingObjectCount: z.literal(0),
  listedObjectBytes: byteCountSchema,
  listedObjectCount: z
    .number()
    .int()
    .nonnegative()
    .max(CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS),
  responseBytes: z.literal(0),
  responseSha256: z.literal(EMPTY_RESPONSE_SHA256),
  scanProfile: z.enum([
    "open_data_publication_namespaces",
    "query_table_publication_namespace",
  ]),
  status: z.literal("no_conflicting_publication_prefixes"),
});

const bucketStorageNetworkEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  providerCid: cidSchema,
  responseBytes: z.literal(0),
  responseSha256: z.literal(EMPTY_RESPONSE_SHA256),
  status: z.literal("ipfs_provider_cid_verified"),
});

const targetBucketEvidenceSchema = z.strictObject({
  bucket: resourceNameSchema,
  domain: domainSchema,
  head: bucketHeadEvidenceSchema,
  prefixScan: prefixScanEvidenceSchema,
  storageNetwork: bucketStorageNetworkEvidenceSchema,
  status: z.literal("verified"),
});

const protectedBucketEvidenceSchema = z.strictObject({
  bucket: resourceNameSchema,
  domain: domainSchema,
  head: bucketHeadEvidenceSchema,
  status: z.literal("protected_and_accessible"),
});

const namesEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  matchedIdentityCount: z.number().int().positive().max(8),
  source: z.literal("filebase_names_api_v1"),
  status: z.literal("verified"),
});

const usageRequestEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  bandwidthBytes: byteCountSchema.nullable(),
  bucket: resourceNameSchema.nullable(),
  domain: domainSchema.nullable(),
  source: z.enum([
    "filebase_account_usage_v1",
    "filebase_bucket_storage_usage_v1",
  ]),
  status: z.literal("observed"),
  storageBytes: byteCountSchema,
});

const capacityProfileSchema = z.strictObject({
  account: usageRequestEvidenceSchema.extend({
    bandwidthBytes: byteCountSchema,
    bucket: z.null(),
    domain: z.null(),
    source: z.literal("filebase_account_usage_v1"),
  }),
  buckets: z
    .array(
      usageRequestEvidenceSchema.extend({
        bandwidthBytes: z.null(),
        bucket: resourceNameSchema,
        domain: domainSchema,
        source: z.literal("filebase_bucket_storage_usage_v1"),
      }),
    )
    .length(2),
  status: z.literal("usage_observed_tier_pending"),
  subscriptionTierEvidence: z.literal("human_confirmation_required"),
});

const gatewayEvidenceSchema = z.strictObject({
  ...hashedRequestShape,
  httpStatus: responseStatusSchema.nullable(),
  observedCid: cidSchema.nullable(),
  outcome: z.enum(["matched", "unavailable_diagnostic"]),
  responseBytes: z.literal(0),
  responseSha256: z.literal(EMPTY_RESPONSE_SHA256),
  source: z.literal("filebase_official_ipfs_gateway"),
});

const signedEvidenceSchema = delegatedIpnsEvidenceSchema.extend({
  evidenceSha256: sha256Schema,
});

const controlIdentitySchema = z.strictObject({
  evidenceSha256: sha256Schema,
  observedCid: cidSchema,
  source: z.literal("filebase_names_api_v1"),
  status: z.literal("matched"),
});

const identityEvidenceSchema = z
  .strictObject({
    bucket: resourceNameSchema,
    control: controlIdentitySchema,
    domain: domainSchema,
    ipnsLabel: resourceNameSchema,
    ipnsNetworkKey: networkKeySchema,
    officialGateway: gatewayEvidenceSchema,
    priorCid: cidSchema,
    resolverPolicy: z.literal("candidate_filebase_delegated_v2"),
    role: z.enum(["source_snapshot_target", "protected_rollback_sample"]),
    signedRecord: signedEvidenceSchema,
    status: z.literal("control_and_signed_record_matched"),
  })
  .superRefine((identity, context) => {
    if (
      identity.control.observedCid !== identity.priorCid ||
      identity.signedRecord.observedCid !== identity.priorCid ||
      identity.signedRecord.httpStatus !== 200 ||
      identity.signedRecord.outcome !== "validated" ||
      !["valid_prior", "valid_target"].includes(
        identity.signedRecord.validationResult,
      )
    ) {
      context.addIssue({
        code: "custom",
        message: "identity evidence does not agree on its immutable CID",
      });
    }
    if (
      identity.officialGateway.outcome === "matched" &&
      (identity.officialGateway.observedCid !== identity.priorCid ||
        identity.officialGateway.httpStatus === null ||
        identity.officialGateway.httpStatus < 200 ||
        identity.officialGateway.httpStatus >= 400)
    ) {
      context.addIssue({
        code: "custom",
        message: "official gateway evidence contradicts the immutable CID",
      });
    }
    if (
      identity.officialGateway.outcome === "unavailable_diagnostic" &&
      identity.officialGateway.observedCid !== null
    ) {
      context.addIssue({
        code: "custom",
        message: "unavailable gateway evidence must not assert a CID",
      });
    }
    if (
      identity.role === "source_snapshot_target" &&
      identity.officialGateway.outcome !== "matched"
    ) {
      context.addIssue({
        code: "custom",
        message: "source-snapshot target requires official gateway agreement",
      });
    }
  });

const protectedRollbackEvidenceSchema = z.strictObject({
  controlObjects: z
    .tuple([
      z.strictObject({
        ...hashedRequestShape,
        cid: cidSchema,
        role: z.literal("manifest"),
        sha256: sha256Schema,
        status: z.literal("cid_and_sha256_verified"),
      }),
      z.strictObject({
        ...hashedRequestShape,
        cid: cidSchema,
        role: z.literal("plan"),
        sha256: sha256Schema,
        status: z.literal("cid_and_sha256_verified"),
      }),
    ])
    .readonly(),
  bucketChecks: z.array(protectedBucketEvidenceSchema).length(2),
  identities: z.array(identityEvidenceSchema).length(2),
  manifest: z.strictObject({ cid: cidSchema, sha256: sha256Schema }),
  plan: z.strictObject({ cid: cidSchema, sha256: sha256Schema }),
  status: z.literal("identity_and_control_objects_verified_for_rollback"),
  verificationEvidenceSha256: sha256Schema,
  verifiedAt: z.string().datetime(),
});

const readPolicySchema = z.strictObject({
  accountUsageSource: z.literal("filebase_platform_usage_v1"),
  configuredRequestLimit: z.number().int().positive(),
  delegatedSignedRecordSource: z.literal("ipfs_delegated_routing_v1"),
  endpointPolicy: z.literal("compiled_filebase_read_only_v1"),
  gatewaySource: z.literal("filebase_official_ipfs_gateway"),
  listMaximumKeys: z.literal(CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS),
  logicalOperationCount: z.number().int().positive(),
  maximumRequestCount: z.number().int().positive(),
  maxConcurrency: z.literal(1),
  maxRetries: z.number().int().nonnegative().max(2),
  namesResponseMaximumBytes: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_RESPONSE_BYTES,
  ),
  requestTimeoutMs: z.number().int().min(500).max(20_000),
  rollbackControlObjectMaximumBytes: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_MAX_ROLLBACK_OBJECT_BYTES,
  ),
  signedRecordMaximumBytes: z.literal(MAX_SIGNED_IPNS_RECORD_BYTES),
  usageResponseMaximumBytes: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_MAX_USAGE_RESPONSE_BYTES,
  ),
});

export const candidateSourceSnapshotPreflightEvidenceSchema = z.strictObject({
  capacityProfile: capacityProfileSchema,
  completedAt: z.string().datetime(),
  evidenceSha256: sha256Schema,
  executorEnabled: z.literal(false),
  names: namesEvidenceSchema,
  observedAt: z.string().datetime(),
  protectedSampleRollback: protectedRollbackEvidenceSchema,
  readPolicy: readPolicySchema,
  requestCount: z.number().int().positive(),
  startedAt: z.string().datetime(),
  status: z.literal("ready_for_source_snapshot_planning"),
  targetBuckets: z.array(targetBucketEvidenceSchema).length(2),
  targetIdentities: z.array(identityEvidenceSchema).length(2),
  version: z.literal(CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_VERSION),
});

export type CandidateSourceSnapshotPreflightEvidence = z.infer<
  typeof candidateSourceSnapshotPreflightEvidenceSchema
>;

type RetryDelay = (delayMs: number) => Promise<void>;

async function defaultRetryDelay(delayMs: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

function withEvidenceHash<T extends Record<string, unknown>>(
  value: T,
): T & { evidenceSha256: string } {
  return { ...value, evidenceSha256: canonicalJsonSha256(value) };
}

function assertEvidenceHash(
  value: Record<string, unknown> & { evidenceSha256: string },
  description: string,
): void {
  const { evidenceSha256, ...withoutHash } = value;
  if (canonicalJsonSha256(withoutHash) !== evidenceSha256) {
    throw new Error(`${description} evidence hash is invalid`);
  }
}

function bearer(config: CandidateSourceSnapshotPreflightConfig): string {
  return Buffer.from(
    `${config.s3AccessKeyId}:${config.s3SecretAccessKey}`,
  ).toString("base64");
}

async function boundedJson(
  response: Response,
  maximumBytes: number,
  description: string,
): Promise<{ bytes: number; sha256: string; value: unknown }> {
  const contentLength = response.headers.get("content-length");
  if (
    contentLength !== null &&
    (!/^\d+$/.test(contentLength) || Number(contentLength) > maximumBytes)
  ) {
    throw new Error(`${description} response exceeds its byte limit`);
  }
  if (!response.body) throw new Error(`${description} response is empty`);
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = response.body.getReader();
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw new Error(`${description} response exceeds its byte limit`);
    }
    chunks.push(next.value);
  }
  const bytes = Buffer.concat(chunks, total);
  let value: unknown;
  try {
    value = JSON.parse(bytes.toString("utf8"));
  } catch {
    throw new Error(`${description} response is not valid JSON`);
  }
  return { bytes: bytes.byteLength, sha256: sha256(bytes), value };
}

function retryableStatus(status: number): boolean {
  return status === 429 || [500, 502, 503, 504].includes(status);
}

async function getJsonWithRetries(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  description: string;
  fetchImpl: typeof fetch;
  maximumBytes: number;
  retryDelay: RetryDelay;
  url: URL;
}): Promise<{
  httpStatus: number;
  observedAt: string;
  requestCount: number;
  responseBytes: number;
  responseSha256: string;
  value: unknown;
}> {
  let requestCount = 0;
  for (
    let attempt = 0;
    attempt <= options.config.limits.maxRetries;
    attempt += 1
  ) {
    requestCount += 1;
    let response: Response;
    try {
      response = await options.fetchImpl(options.url, {
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${bearer(options.config)}`,
        },
        method: "GET",
        redirect: "error",
        signal: AbortSignal.timeout(options.config.limits.requestTimeoutMs),
      });
    } catch {
      if (attempt < options.config.limits.maxRetries) {
        await options.retryDelay(50 * (attempt + 1));
        continue;
      }
      throw new Error(`${options.description} failed within its retry bound`);
    }
    if (
      retryableStatus(response.status) &&
      attempt < options.config.limits.maxRetries
    ) {
      await response.body?.cancel().catch(() => undefined);
      await options.retryDelay(50 * (attempt + 1));
      continue;
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      throw new Error(
        `${options.description} returned HTTP ${response.status}`,
      );
    }
    const bounded = await boundedJson(
      response,
      options.maximumBytes,
      options.description,
    );
    return {
      httpStatus: response.status,
      observedAt: new Date().toISOString(),
      requestCount,
      responseBytes: bounded.bytes,
      responseSha256: bounded.sha256,
      value: bounded.value,
    };
  }
  throw new Error(`${options.description} retry accounting is invalid`);
}

async function boundedBytes(
  response: Response,
  maximumBytes: number,
  description: string,
): Promise<Buffer> {
  const contentLength = response.headers.get("content-length");
  if (
    contentLength !== null &&
    (!/^\d+$/.test(contentLength) || Number(contentLength) > maximumBytes)
  ) {
    throw new Error(`${description} response exceeds its byte limit`);
  }
  if (!response.body) throw new Error(`${description} response is empty`);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw new Error(`${description} response exceeds its byte limit`);
    }
    chunks.push(next.value);
  }
  return Buffer.concat(chunks, total);
}

type ProtectedControlObjectEvidence = z.infer<
  typeof protectedRollbackEvidenceSchema
>["controlObjects"][number];

export async function readProtectedControlObject(options: {
  cid: string;
  config: CandidateSourceSnapshotPreflightConfig;
  fetchImpl: typeof fetch;
  retryDelay: RetryDelay;
  role: "manifest" | "plan";
  sha256: string;
}): Promise<ProtectedControlObjectEvidence> {
  const expectedCid = cidSchema.parse(options.cid);
  const expectedSha256 = sha256Schema.parse(options.sha256);
  const url = new URL(
    `/ipfs/${expectedCid}`,
    CANDIDATE_SOURCE_SNAPSHOT_GATEWAY_ORIGIN,
  );
  let requestCount = 0;
  for (
    let attempt = 0;
    attempt <= options.config.limits.maxRetries;
    attempt += 1
  ) {
    requestCount += 1;
    let response: Response;
    try {
      response = await options.fetchImpl(url, {
        headers: { Accept: "application/json" },
        method: "GET",
        redirect: "error",
        signal: AbortSignal.timeout(options.config.limits.requestTimeoutMs),
      });
    } catch {
      if (attempt < options.config.limits.maxRetries) {
        await options.retryDelay(50 * (attempt + 1));
        continue;
      }
      throw new Error(
        `Protected sample ${options.role} read failed within its retry bound`,
      );
    }
    if (
      retryableStatus(response.status) &&
      attempt < options.config.limits.maxRetries
    ) {
      await response.body?.cancel().catch(() => undefined);
      await options.retryDelay(50 * (attempt + 1));
      continue;
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      throw new Error(
        `Protected sample ${options.role} returned HTTP ${response.status}`,
      );
    }
    const bytes = await boundedBytes(
      response,
      CANDIDATE_SOURCE_SNAPSHOT_MAX_ROLLBACK_OBJECT_BYTES,
      `Protected sample ${options.role}`,
    );
    const actualSha256 = sha256(bytes);
    if (actualSha256 !== expectedSha256) {
      throw new Error(`Protected sample ${options.role} SHA-256 mismatch`);
    }
    if ((await calculateIpfsCid(bytes)) !== expectedCid) {
      throw new Error(`Protected sample ${options.role} CID mismatch`);
    }
    return withEvidenceHash({
      cid: expectedCid,
      httpStatus: response.status,
      observedAt: new Date().toISOString(),
      requestCount,
      responseBytes: bytes.byteLength,
      responseSha256: actualSha256,
      role: options.role,
      sha256: expectedSha256,
      status: "cid_and_sha256_verified" as const,
    }) as ProtectedControlObjectEvidence;
  }
  throw new Error(
    "Protected sample control-object retry accounting is invalid",
  );
}

type ProtectedControlObjectReader = typeof readProtectedControlObject;

function attemptsFrom(metadata: { attempts?: number }): number {
  if (metadata.attempts === undefined) {
    throw new Error("Filebase S3 response omitted request-attempt accounting");
  }
  return z.number().int().positive().max(3).parse(metadata.attempts);
}

async function headBucket(options: {
  bucket: string;
  client: S3Client;
  timeoutMs: number;
}): Promise<z.infer<typeof bucketHeadEvidenceSchema>> {
  let response: HeadBucketCommandOutput;
  try {
    response = await options.client.send(
      new HeadBucketCommand({ Bucket: options.bucket }),
      { abortSignal: AbortSignal.timeout(options.timeoutMs) },
    );
  } catch {
    throw new Error("Candidate Filebase bucket authentication failed");
  }
  if (response.$metadata.httpStatusCode !== 200) {
    throw new Error("Candidate Filebase bucket authentication failed");
  }
  return bucketHeadEvidenceSchema.parse(
    withEvidenceHash({
      httpStatus: response.$metadata.httpStatusCode,
      observedAt: new Date().toISOString(),
      requestCount: attemptsFrom(response.$metadata),
      responseBytes: 0 as const,
      responseSha256: EMPTY_RESPONSE_SHA256,
      status: "authenticated" as const,
    }),
  );
}

async function scanPrefixes(options: {
  bucket: string;
  client: S3Client;
  conflictPrefixes: readonly string[];
  profile: z.infer<typeof prefixScanEvidenceSchema>["scanProfile"];
  scanPrefix: string;
  timeoutMs: number;
}): Promise<{
  evidence: z.infer<typeof prefixScanEvidenceSchema>;
  probeObjectKey: string;
}> {
  let response: ListObjectsV2CommandOutput;
  try {
    response = await options.client.send(
      new ListObjectsV2Command({
        Bucket: options.bucket,
        MaxKeys: CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS,
        Prefix: options.scanPrefix,
      }),
      { abortSignal: AbortSignal.timeout(options.timeoutMs) },
    );
  } catch {
    throw new Error("Candidate Filebase prefix scan failed");
  }
  if (response.$metadata.httpStatusCode !== 200) {
    throw new Error("Candidate Filebase prefix scan failed");
  }
  if (response.IsTruncated || response.NextContinuationToken) {
    throw new Error(
      "Candidate Filebase prefix scan is not a complete single page",
    );
  }
  const contents = response.Contents ?? [];
  if (
    contents.length > CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS ||
    (response.KeyCount !== undefined && response.KeyCount !== contents.length)
  ) {
    throw new Error("Candidate Filebase prefix scan count is inconsistent");
  }
  if (contents.length === 0) {
    throw new Error(
      "Candidate Filebase bucket lacks an object for IPFS storage verification",
    );
  }
  let listedObjectBytes = 0;
  let conflictingObjectCount = 0;
  for (const object of contents) {
    if (!object.Key || !object.Key.startsWith(options.scanPrefix)) {
      throw new Error(
        "Candidate Filebase prefix scan returned an invalid entry",
      );
    }
    const size = z
      .number()
      .int()
      .nonnegative()
      .parse(object.Size ?? 0);
    listedObjectBytes += size;
    if (!Number.isSafeInteger(listedObjectBytes)) {
      throw new Error("Candidate Filebase prefix scan byte count is unsafe");
    }
    if (
      options.conflictPrefixes.some((prefix) => object.Key!.startsWith(prefix))
    ) {
      conflictingObjectCount += 1;
    }
  }
  if (conflictingObjectCount !== 0) {
    throw new Error(
      "A conflicting candidate publication prefix already exists",
    );
  }
  return {
    evidence: prefixScanEvidenceSchema.parse(
      withEvidenceHash({
        complete: true as const,
        conflictingObjectCount: 0 as const,
        httpStatus: response.$metadata.httpStatusCode,
        listedObjectBytes,
        listedObjectCount: contents.length,
        observedAt: new Date().toISOString(),
        requestCount: attemptsFrom(response.$metadata),
        responseBytes: 0 as const,
        responseSha256: EMPTY_RESPONSE_SHA256,
        scanProfile: options.profile,
        status: "no_conflicting_publication_prefixes" as const,
      }),
    ),
    probeObjectKey: contents[0]!.Key!,
  };
}

async function verifyBucketStorageNetwork(options: {
  bucket: string;
  client: S3Client;
  objectKey: string;
  timeoutMs: number;
}): Promise<z.infer<typeof bucketStorageNetworkEvidenceSchema>> {
  let response: HeadObjectCommandOutput;
  try {
    response = await options.client.send(
      new HeadObjectCommand({
        Bucket: options.bucket,
        Key: options.objectKey,
      }),
      { abortSignal: AbortSignal.timeout(options.timeoutMs) },
    );
  } catch {
    throw new Error("Candidate Filebase IPFS storage verification failed");
  }
  if (response.$metadata.httpStatusCode !== 200) {
    throw new Error("Candidate Filebase IPFS storage verification failed");
  }
  const providerCid = cidSchema.safeParse(response.Metadata?.cid);
  if (!providerCid.success) {
    throw new Error(
      "Candidate Filebase object omitted verified IPFS CID metadata",
    );
  }
  return bucketStorageNetworkEvidenceSchema.parse(
    withEvidenceHash({
      httpStatus: response.$metadata.httpStatusCode,
      observedAt: new Date().toISOString(),
      providerCid: providerCid.data,
      requestCount: attemptsFrom(response.$metadata),
      responseBytes: 0 as const,
      responseSha256: EMPTY_RESPONSE_SHA256,
      status: "ipfs_provider_cid_verified" as const,
    }),
  );
}

const filebaseNameSchema = z.strictObject({
  cid: cidSchema,
  created_at: z.string(),
  enabled: z.boolean(),
  label: z.string().min(1).max(200),
  network_key: networkKeySchema,
  published_at: z.string().nullable(),
  sequence: z.union([z.number().int().nonnegative(), z.string()]),
  updated_at: z.string(),
});

async function readNames(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  fetchImpl: typeof fetch;
  matchedIdentityCount: number;
  retryDelay: RetryDelay;
}): Promise<{
  evidence: z.infer<typeof namesEvidenceSchema>;
  names: z.infer<typeof filebaseNameSchema>[];
}> {
  const result = await getJsonWithRetries({
    config: options.config,
    description: "Filebase names preflight",
    fetchImpl: options.fetchImpl,
    maximumBytes: CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_RESPONSE_BYTES,
    retryDelay: options.retryDelay,
    url: new URL("/v1/names", CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT),
  });
  const names = z.array(filebaseNameSchema).max(256).parse(result.value);
  return {
    evidence: namesEvidenceSchema.parse(
      withEvidenceHash({
        httpStatus: result.httpStatus,
        matchedIdentityCount: options.matchedIdentityCount,
        observedAt: result.observedAt,
        requestCount: result.requestCount,
        responseBytes: result.responseBytes,
        responseSha256: result.responseSha256,
        source: "filebase_names_api_v1" as const,
        status: "verified" as const,
      }),
    ),
    names,
  };
}

const accountUsageSchema = z.strictObject({
  bandwidth: z.strictObject({ bytes: byteCountSchema }),
  storage: z.strictObject({ bytes: byteCountSchema }),
});
const bucketUsageSchema = z.strictObject({
  storage: z.strictObject({ bytes: byteCountSchema }),
});

async function readUsage(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  domain: "open_data" | "query_table" | null;
  fetchImpl: typeof fetch;
  retryDelay: RetryDelay;
}): Promise<z.infer<typeof usageRequestEvidenceSchema>> {
  const bucket =
    options.domain === "open_data"
      ? options.config.targets.openData.bucket
      : options.domain === "query_table"
        ? options.config.targets.queryTable.bucket
        : null;
  const result = await getJsonWithRetries({
    config: options.config,
    description:
      bucket === null ? "Filebase account usage" : "Filebase bucket usage",
    fetchImpl: options.fetchImpl,
    maximumBytes: CANDIDATE_SOURCE_SNAPSHOT_MAX_USAGE_RESPONSE_BYTES,
    retryDelay: options.retryDelay,
    url:
      bucket === null
        ? new URL("/v1/usage", CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT)
        : new URL(
            `/v1/usage/storage/${encodeURIComponent(bucket)}`,
            CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
          ),
  });
  if (bucket === null) {
    const usage = accountUsageSchema.parse(result.value);
    return usageRequestEvidenceSchema.parse(
      withEvidenceHash({
        bandwidthBytes: usage.bandwidth.bytes,
        bucket: null,
        domain: null,
        httpStatus: result.httpStatus,
        observedAt: result.observedAt,
        requestCount: result.requestCount,
        responseBytes: result.responseBytes,
        responseSha256: result.responseSha256,
        source: "filebase_account_usage_v1" as const,
        status: "observed" as const,
        storageBytes: usage.storage.bytes,
      }),
    );
  }
  const usage = bucketUsageSchema.parse(result.value);
  return usageRequestEvidenceSchema.parse(
    withEvidenceHash({
      bandwidthBytes: null,
      bucket,
      domain: options.domain,
      httpStatus: result.httpStatus,
      observedAt: result.observedAt,
      requestCount: result.requestCount,
      responseBytes: result.responseBytes,
      responseSha256: result.responseSha256,
      source: "filebase_bucket_storage_usage_v1" as const,
      status: "observed" as const,
      storageBytes: usage.storage.bytes,
    }),
  );
}

function cidFromGateway(response: Response): string | null {
  for (const value of [
    response.headers.get("x-ipfs-roots"),
    response.headers.get("x-ipfs-path"),
    response.headers.get("location"),
  ]) {
    const match = value?.match(
      /Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/,
    )?.[0];
    if (match) return cidSchema.parse(match);
  }
  return null;
}

async function observeOfficialGateway(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  expectedCid: string;
  fetchImpl: typeof fetch;
  networkKey: string;
  retryDelay: RetryDelay;
}): Promise<z.infer<typeof gatewayEvidenceSchema>> {
  const observedAt = new Date().toISOString();
  let requestCount = 0;
  let finalResponse: Response | null = null;
  for (
    let attempt = 0;
    attempt <= options.config.limits.maxRetries;
    attempt += 1
  ) {
    requestCount += 1;
    try {
      const response = await options.fetchImpl(
        new URL(
          `/ipns/${networkKeySchema.parse(options.networkKey)}`,
          CANDIDATE_SOURCE_SNAPSHOT_GATEWAY_ORIGIN,
        ),
        {
          headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
          method: "HEAD",
          redirect: "manual",
          signal: AbortSignal.timeout(options.config.limits.requestTimeoutMs),
        },
      );
      if (
        retryableStatus(response.status) &&
        attempt < options.config.limits.maxRetries
      ) {
        await response.body?.cancel().catch(() => undefined);
        await options.retryDelay(50 * (attempt + 1));
        continue;
      }
      finalResponse = response;
      break;
    } catch {
      if (attempt < options.config.limits.maxRetries) {
        await options.retryDelay(50 * (attempt + 1));
        continue;
      }
    }
  }

  const responseCid = finalResponse ? cidFromGateway(finalResponse) : null;
  if (responseCid !== null && responseCid !== options.expectedCid) {
    throw new Error("Official Filebase gateway contradicts the expected CID");
  }
  const eligibleStatus =
    finalResponse !== null &&
    finalResponse.status >= 200 &&
    finalResponse.status < 400;
  const matched = eligibleStatus && responseCid === options.expectedCid;
  const observedCid = matched ? options.expectedCid : null;
  const outcome = matched
    ? ("matched" as const)
    : ("unavailable_diagnostic" as const);
  return gatewayEvidenceSchema.parse(
    withEvidenceHash({
      httpStatus: finalResponse?.status ?? null,
      observedAt,
      observedCid,
      outcome,
      requestCount,
      responseBytes: 0 as const,
      responseSha256: EMPTY_RESPONSE_SHA256,
      source: "filebase_official_ipfs_gateway" as const,
    }),
  );
}

function findName(
  names: readonly z.infer<typeof filebaseNameSchema>[],
  target: { ipnsLabel: string; ipnsNetworkKey: string },
): z.infer<typeof filebaseNameSchema> {
  const related = names.filter(
    (entry) =>
      entry.label === target.ipnsLabel ||
      entry.network_key === target.ipnsNetworkKey,
  );
  if (
    related.length !== 1 ||
    related[0]!.label !== target.ipnsLabel ||
    related[0]!.network_key !== target.ipnsNetworkKey ||
    !related[0]!.enabled
  ) {
    throw new Error(
      "Configured candidate IPNS identity is unavailable or split",
    );
  }
  return related[0]!;
}

type DelegatedObserver = typeof observeDelegatedIpnsRecord;

async function observeIdentity(options: {
  bucket: string;
  config: CandidateSourceSnapshotPreflightConfig;
  domain: "open_data" | "query_table";
  expectedCid: string;
  fetchImpl: typeof fetch;
  ipnsLabel: string;
  ipnsNetworkKey: string;
  names: readonly z.infer<typeof filebaseNameSchema>[];
  namesEvidenceSha256: string;
  observeDelegated: DelegatedObserver;
  retryDelay: RetryDelay;
  role: "source_snapshot_target" | "protected_rollback_sample";
}): Promise<z.infer<typeof identityEvidenceSchema>> {
  const record = findName(options.names, options);
  const priorCid = cidSchema.parse(options.expectedCid);
  if (record.cid !== priorCid) {
    throw new Error("Filebase control plane contradicts the expected CID");
  }
  const officialGateway = await observeOfficialGateway({
    config: options.config,
    expectedCid: priorCid,
    fetchImpl: options.fetchImpl,
    networkKey: options.ipnsNetworkKey,
    retryDelay: options.retryDelay,
  });
  if (
    options.role === "source_snapshot_target" &&
    officialGateway.outcome !== "matched"
  ) {
    throw new Error("Source-snapshot target gateway is unavailable");
  }
  const signedRecord = await options.observeDelegated({
    expectedPriorCid: priorCid,
    expectedTargetCid: priorCid,
    fetchImpl: options.fetchImpl,
    maxRetries: options.config.limits.maxRetries,
    networkKey: options.ipnsNetworkKey,
    retryDelay: options.retryDelay,
    timeoutMs: options.config.limits.requestTimeoutMs,
  });
  if (
    signedRecord.observedCid !== priorCid ||
    signedRecord.httpStatus !== 200 ||
    signedRecord.outcome !== "validated" ||
    !["valid_prior", "valid_target"].includes(signedRecord.validationResult)
  ) {
    throw new Error("Signed IPNS record does not match Filebase control");
  }
  return identityEvidenceSchema.parse({
    bucket: options.bucket,
    control: {
      evidenceSha256: options.namesEvidenceSha256,
      observedCid: priorCid,
      source: "filebase_names_api_v1",
      status: "matched",
    },
    domain: options.domain,
    ipnsLabel: options.ipnsLabel,
    ipnsNetworkKey: options.ipnsNetworkKey,
    officialGateway,
    priorCid,
    resolverPolicy: "candidate_filebase_delegated_v2",
    role: options.role,
    signedRecord: {
      ...signedRecord,
      evidenceSha256: canonicalJsonSha256(signedRecord),
    },
    status: "control_and_signed_record_matched",
  });
}

function targetInputs(config: CandidateSourceSnapshotPreflightConfig) {
  return [
    {
      domain: "open_data" as const,
      expectedPriorCid: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData,
      target: config.targets.openData,
    },
    {
      domain: "query_table" as const,
      expectedPriorCid:
        CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable,
      target: config.targets.queryTable,
    },
  ];
}

function protectedInputs() {
  return [
    {
      domain: "open_data" as const,
      target: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData,
    },
    {
      domain: "query_table" as const,
      target: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable,
    },
  ];
}

function logicalOperationCount(
  config: CandidateSourceSnapshotPreflightConfig,
): number {
  const targetCount = targetInputs(config).length;
  const protectedCount = protectedInputs().length;
  const bucketHeads = targetCount + protectedCount;
  const prefixScans = targetCount;
  const namesReads = 1;
  const usageReads = 1 + targetCount;
  const resolverReads = (targetCount + protectedCount) * 2;
  const targetStorageNetworkReads = targetCount;
  const protectedControlObjectReads = 2;
  return (
    bucketHeads +
    prefixScans +
    targetStorageNetworkReads +
    namesReads +
    usageReads +
    resolverReads +
    protectedControlObjectReads
  );
}

function assertConfigSafe(
  config: CandidateSourceSnapshotPreflightConfig,
): void {
  if (
    config.executorEnabled !== false ||
    config.s3Endpoint !== CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT ||
    config.apiEndpoint !== CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT
  ) {
    throw new Error(
      "Source-snapshot read-only preflight configuration is unsafe",
    );
  }
  candidateSourceSnapshotLimitsSchema.parse(config.limits);
  targetIdentitySchema.parse(config.targets.openData);
  targetIdentitySchema.parse(config.targets.queryTable);
  z.string().min(1).parse(config.s3AccessKeyId);
  z.string().min(1).parse(config.s3SecretAccessKey);
  if (
    config.targets.openData.bucket === config.targets.queryTable.bucket ||
    config.targets.openData.ipnsLabel === config.targets.queryTable.ipnsLabel ||
    config.targets.openData.ipnsNetworkKey ===
      config.targets.queryTable.ipnsNetworkKey ||
    !config.targets.openData.bucket.endsWith(
      "-open-data-source-snapshot-demo-v1",
    ) ||
    !config.targets.queryTable.bucket.endsWith(
      "-query-table-source-snapshot-demo-v1",
    )
  ) {
    throw new Error("Source-snapshot target resource identities are unsafe");
  }
  const protectedResources = new Set([
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.bucket,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsLabel,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsNetworkKey,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.bucket,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsLabel,
    PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsNetworkKey,
  ]);
  if (
    [
      config.targets.openData.bucket,
      config.targets.openData.ipnsLabel,
      config.targets.openData.ipnsNetworkKey,
      config.targets.queryTable.bucket,
      config.targets.queryTable.ipnsLabel,
      config.targets.queryTable.ipnsNetworkKey,
    ].some((resource) => protectedResources.has(resource))
  ) {
    throw new Error(
      "Source-snapshot targets overlap the protected rollback sample",
    );
  }
}

export async function runCandidateSourceSnapshotReadOnlyPreflight(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  fetchImpl?: typeof fetch;
  observeDelegated?: DelegatedObserver;
  readProtectedControl?: ProtectedControlObjectReader;
  retryDelay?: RetryDelay;
  s3Client?: S3Client;
  startedAt?: string;
}): Promise<CandidateSourceSnapshotPreflightEvidence> {
  assertConfigSafe(options.config);
  const config = options.config;
  const logicalRequests = logicalOperationCount(config);
  const maximumRequestCount = logicalRequests * (config.limits.maxRetries + 1);
  if (maximumRequestCount > config.limits.maxRequests) {
    throw new Error(
      "Source-snapshot preflight exceeds the configured request ceiling",
    );
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const retryDelay = options.retryDelay ?? defaultRetryDelay;
  const observeDelegated =
    options.observeDelegated ?? observeDelegatedIpnsRecord;
  const readProtectedControl =
    options.readProtectedControl ?? readProtectedControlObject;
  const s3 =
    options.s3Client ??
    new S3Client({
      credentials: {
        accessKeyId: config.s3AccessKeyId,
        secretAccessKey: config.s3SecretAccessKey,
      },
      endpoint: CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
      forcePathStyle: true,
      maxAttempts: config.limits.maxRetries + 1,
      region: "auto",
    });
  const startedAt = options.startedAt ?? new Date().toISOString();

  const targetBuckets: z.infer<typeof targetBucketEvidenceSchema>[] = [];
  for (const { domain, target } of targetInputs(config)) {
    const head = await headBucket({
      bucket: target.bucket,
      client: s3,
      timeoutMs: config.limits.requestTimeoutMs,
    });
    const prefixScan = await scanPrefixes({
      bucket: target.bucket,
      client: s3,
      conflictPrefixes:
        domain === "open_data"
          ? OPEN_DATA_CONFLICT_PREFIXES
          : QUERY_TABLE_CONFLICT_PREFIXES,
      profile:
        domain === "open_data"
          ? "open_data_publication_namespaces"
          : "query_table_publication_namespace",
      scanPrefix: TARGET_BUCKET_SCAN_PREFIX,
      timeoutMs: config.limits.requestTimeoutMs,
    });
    const storageNetwork = await verifyBucketStorageNetwork({
      bucket: target.bucket,
      client: s3,
      objectKey: prefixScan.probeObjectKey,
      timeoutMs: config.limits.requestTimeoutMs,
    });
    targetBuckets.push({
      bucket: target.bucket,
      domain,
      head,
      prefixScan: prefixScan.evidence,
      storageNetwork,
      status: "verified",
    });
  }

  const protectedBucketChecks: z.infer<typeof protectedBucketEvidenceSchema>[] =
    [];
  for (const { domain, target } of protectedInputs()) {
    protectedBucketChecks.push({
      bucket: target.bucket,
      domain,
      head: await headBucket({
        bucket: target.bucket,
        client: s3,
        timeoutMs: config.limits.requestTimeoutMs,
      }),
      status: "protected_and_accessible",
    });
  }

  const namesResult = await readNames({
    config,
    fetchImpl,
    matchedIdentityCount:
      targetInputs(config).length + protectedInputs().length,
    retryDelay,
  });
  const targetIdentities: z.infer<typeof identityEvidenceSchema>[] = [];
  for (const { domain, expectedPriorCid, target } of targetInputs(config)) {
    targetIdentities.push(
      await observeIdentity({
        bucket: target.bucket,
        config,
        domain,
        expectedCid: expectedPriorCid,
        fetchImpl,
        ipnsLabel: target.ipnsLabel,
        ipnsNetworkKey: target.ipnsNetworkKey,
        names: namesResult.names,
        namesEvidenceSha256: namesResult.evidence.evidenceSha256,
        observeDelegated,
        retryDelay,
        role: "source_snapshot_target",
      }),
    );
  }
  if (targetIdentities[0]!.priorCid === targetIdentities[1]!.priorCid) {
    throw new Error("Source-snapshot target prior CIDs must be distinct");
  }

  const protectedIdentities: z.infer<typeof identityEvidenceSchema>[] = [];
  for (const { domain, target } of protectedInputs()) {
    protectedIdentities.push(
      await observeIdentity({
        bucket: target.bucket,
        config,
        domain,
        expectedCid: target.targetCid,
        fetchImpl,
        ipnsLabel: target.ipnsLabel,
        ipnsNetworkKey: target.ipnsNetworkKey,
        names: namesResult.names,
        namesEvidenceSha256: namesResult.evidence.evidenceSha256,
        observeDelegated,
        retryDelay,
        role: "protected_rollback_sample",
      }),
    );
  }

  const protectedControlObjects = [
    await readProtectedControl({
      cid: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest.cid,
      config,
      fetchImpl,
      retryDelay,
      role: "manifest",
      sha256: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest.sha256,
    }),
    await readProtectedControl({
      cid: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan.cid,
      config,
      fetchImpl,
      retryDelay,
      role: "plan",
      sha256: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan.sha256,
    }),
  ] as const;

  const accountUsage = await readUsage({
    config,
    domain: null,
    fetchImpl,
    retryDelay,
  });
  const bucketUsage = [];
  for (const domain of ["open_data", "query_table"] as const) {
    bucketUsage.push(
      await readUsage({ config, domain, fetchImpl, retryDelay }),
    );
  }
  const capacityProfile = capacityProfileSchema.parse({
    account: accountUsage,
    buckets: bucketUsage,
    status: "usage_observed_tier_pending",
    subscriptionTierEvidence: "human_confirmation_required",
  });

  const completedAt = new Date().toISOString();
  const protectedWithoutHash = {
    bucketChecks: protectedBucketChecks,
    controlObjects: protectedControlObjects,
    identities: protectedIdentities,
    manifest: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest,
    plan: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan,
    status: "identity_and_control_objects_verified_for_rollback" as const,
    verifiedAt: completedAt,
  };
  const protectedSampleRollback = protectedRollbackEvidenceSchema.parse({
    ...protectedWithoutHash,
    verificationEvidenceSha256: canonicalJsonSha256(protectedWithoutHash),
  });

  const requestCount = [
    ...targetBuckets.flatMap((bucket) => [
      bucket.head.requestCount,
      bucket.prefixScan.requestCount,
      bucket.storageNetwork.requestCount,
    ]),
    ...protectedBucketChecks.map((bucket) => bucket.head.requestCount),
    namesResult.evidence.requestCount,
    ...targetIdentities.flatMap((identity) => [
      identity.officialGateway.requestCount,
      identity.signedRecord.requestCount,
    ]),
    ...protectedIdentities.flatMap((identity) => [
      identity.officialGateway.requestCount,
      identity.signedRecord.requestCount,
    ]),
    ...protectedControlObjects.map((object) => object.requestCount),
    accountUsage.requestCount,
    ...bucketUsage.map((usage) => usage.requestCount),
  ].reduce((total, count) => total + count, 0);
  if (
    requestCount > maximumRequestCount ||
    requestCount > config.limits.maxRequests
  ) {
    throw new Error(
      "Source-snapshot preflight request accounting exceeded its bound",
    );
  }

  const withoutHash = {
    capacityProfile,
    completedAt,
    executorEnabled: false as const,
    names: namesResult.evidence,
    observedAt: completedAt,
    protectedSampleRollback,
    readPolicy: {
      accountUsageSource: "filebase_platform_usage_v1" as const,
      configuredRequestLimit: config.limits.maxRequests,
      delegatedSignedRecordSource: "ipfs_delegated_routing_v1" as const,
      endpointPolicy: "compiled_filebase_read_only_v1" as const,
      gatewaySource: "filebase_official_ipfs_gateway" as const,
      listMaximumKeys: CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS,
      logicalOperationCount: logicalRequests,
      maximumRequestCount,
      maxConcurrency: 1 as const,
      maxRetries: config.limits.maxRetries,
      namesResponseMaximumBytes:
        CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_RESPONSE_BYTES,
      requestTimeoutMs: config.limits.requestTimeoutMs,
      rollbackControlObjectMaximumBytes:
        CANDIDATE_SOURCE_SNAPSHOT_MAX_ROLLBACK_OBJECT_BYTES,
      signedRecordMaximumBytes: MAX_SIGNED_IPNS_RECORD_BYTES,
      usageResponseMaximumBytes:
        CANDIDATE_SOURCE_SNAPSHOT_MAX_USAGE_RESPONSE_BYTES,
    },
    requestCount,
    startedAt,
    status: "ready_for_source_snapshot_planning" as const,
    targetBuckets,
    targetIdentities,
    version: CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_VERSION,
  };
  return validateCandidateSourceSnapshotPreflightEvidence({
    ...withoutHash,
    evidenceSha256: canonicalJsonSha256(withoutHash),
  });
}

export function validateCandidateSourceSnapshotPreflightEvidence(
  value: unknown,
): CandidateSourceSnapshotPreflightEvidence {
  const evidence = candidateSourceSnapshotPreflightEvidenceSchema.parse(value);
  const { evidenceSha256, ...withoutHash } = evidence;
  if (canonicalJsonSha256(withoutHash) !== evidenceSha256) {
    throw new Error("Source-snapshot preflight evidence hash is invalid");
  }
  if (
    evidence.observedAt !== evidence.completedAt ||
    evidence.protectedSampleRollback.verifiedAt !== evidence.completedAt ||
    Date.parse(evidence.startedAt) > Date.parse(evidence.completedAt)
  ) {
    throw new Error("Source-snapshot preflight timestamps are inconsistent");
  }
  const orderedDomains = ["open_data", "query_table"] as const;
  for (const [index, domain] of orderedDomains.entries()) {
    const bucket = evidence.targetBuckets[index]!;
    const identity = evidence.targetIdentities[index]!;
    const capacity = evidence.capacityProfile.buckets[index]!;
    const protectedBucket =
      evidence.protectedSampleRollback.bucketChecks[index]!;
    const protectedIdentity =
      evidence.protectedSampleRollback.identities[index]!;
    const expectedProtected =
      domain === "open_data"
        ? PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData
        : PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable;
    const expectedTargetPrior =
      domain === "open_data"
        ? CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData
        : CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable;
    if (
      bucket.domain !== domain ||
      identity.domain !== domain ||
      capacity.domain !== domain ||
      capacity.bucket !== bucket.bucket ||
      identity.bucket !== bucket.bucket ||
      identity.role !== "source_snapshot_target" ||
      identity.priorCid !== expectedTargetPrior
    ) {
      throw new Error("Source-snapshot target domains are inconsistent");
    }
    if (
      protectedBucket.domain !== domain ||
      protectedIdentity.domain !== domain ||
      protectedIdentity.role !== "protected_rollback_sample" ||
      protectedBucket.bucket !== expectedProtected.bucket ||
      protectedIdentity.bucket !== expectedProtected.bucket ||
      protectedIdentity.ipnsLabel !== expectedProtected.ipnsLabel ||
      protectedIdentity.ipnsNetworkKey !== expectedProtected.ipnsNetworkKey ||
      protectedIdentity.priorCid !== expectedProtected.targetCid
    ) {
      throw new Error("Protected sample rollback identity is inconsistent");
    }
  }
  if (
    evidence.targetIdentities[0]!.priorCid ===
      evidence.targetIdentities[1]!.priorCid ||
    evidence.names.matchedIdentityCount !==
      evidence.targetIdentities.length +
        evidence.protectedSampleRollback.identities.length ||
    canonicalJson(evidence.protectedSampleRollback.manifest) !==
      canonicalJson(PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest) ||
    canonicalJson(evidence.protectedSampleRollback.plan) !==
      canonicalJson(PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan)
  ) {
    throw new Error("Source-snapshot identity bindings are inconsistent");
  }
  assertEvidenceHash(evidence.names, "Names response");
  for (const bucket of evidence.targetBuckets) {
    assertEvidenceHash(bucket.head, "Bucket head");
    assertEvidenceHash(bucket.prefixScan, "Prefix scan");
    assertEvidenceHash(bucket.storageNetwork, "Bucket storage network");
  }
  for (const bucket of evidence.protectedSampleRollback.bucketChecks) {
    assertEvidenceHash(bucket.head, "Protected bucket head");
  }
  for (const identity of [
    ...evidence.targetIdentities,
    ...evidence.protectedSampleRollback.identities,
  ]) {
    assertEvidenceHash(identity.officialGateway, "Official gateway");
    assertEvidenceHash(identity.signedRecord, "Signed record");
    if (identity.control.evidenceSha256 !== evidence.names.evidenceSha256) {
      throw new Error(
        "Identity control evidence is not bound to the Names read",
      );
    }
  }
  const expectedControlObjects = [
    {
      ...PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest,
      role: "manifest",
    },
    { ...PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan, role: "plan" },
  ] as const;
  for (const [
    index,
    object,
  ] of evidence.protectedSampleRollback.controlObjects.entries()) {
    const expected = expectedControlObjects[index]!;
    assertEvidenceHash(object, `Protected ${expected.role}`);
    if (
      object.role !== expected.role ||
      object.cid !== expected.cid ||
      object.sha256 !== expected.sha256 ||
      object.responseSha256 !== expected.sha256
    ) {
      throw new Error("Protected rollback control-object binding is invalid");
    }
  }
  assertEvidenceHash(evidence.capacityProfile.account, "Account usage");
  for (const usage of evidence.capacityProfile.buckets) {
    assertEvidenceHash(usage, "Bucket usage");
  }
  const { verificationEvidenceSha256, ...protectedWithoutHash } =
    evidence.protectedSampleRollback;
  if (
    canonicalJsonSha256(protectedWithoutHash) !== verificationEvidenceSha256
  ) {
    throw new Error("Protected rollback evidence hash is invalid");
  }
  const recalculatedRequests = [
    ...evidence.targetBuckets.flatMap((bucket) => [
      bucket.head.requestCount,
      bucket.prefixScan.requestCount,
      bucket.storageNetwork.requestCount,
    ]),
    ...evidence.protectedSampleRollback.bucketChecks.map(
      (bucket) => bucket.head.requestCount,
    ),
    evidence.names.requestCount,
    ...evidence.targetIdentities.flatMap((identity) => [
      identity.officialGateway.requestCount,
      identity.signedRecord.requestCount,
    ]),
    ...evidence.protectedSampleRollback.identities.flatMap((identity) => [
      identity.officialGateway.requestCount,
      identity.signedRecord.requestCount,
    ]),
    ...evidence.protectedSampleRollback.controlObjects.map(
      (object) => object.requestCount,
    ),
    evidence.capacityProfile.account.requestCount,
    ...evidence.capacityProfile.buckets.map((usage) => usage.requestCount),
  ];
  const maximumAttempts = evidence.readPolicy.maxRetries + 1;
  if (recalculatedRequests.some((count) => count > maximumAttempts)) {
    throw new Error("Source-snapshot preflight retry accounting is invalid");
  }
  const recalculatedTotal = recalculatedRequests.reduce(
    (total, count) => total + count,
    0,
  );
  const logicalRequests = recalculatedRequests.length;
  if (
    evidence.requestCount !== recalculatedTotal ||
    evidence.readPolicy.logicalOperationCount !== logicalRequests ||
    evidence.readPolicy.maximumRequestCount !==
      logicalRequests * maximumAttempts ||
    evidence.requestCount > evidence.readPolicy.maximumRequestCount ||
    evidence.readPolicy.maximumRequestCount >
      evidence.readPolicy.configuredRequestLimit
  ) {
    throw new Error("Source-snapshot preflight request accounting is invalid");
  }
  return evidence;
}

export function candidateSourceSnapshotPreflightBinding(
  value: unknown,
): CandidateSourceSnapshotPreflightBinding {
  const evidence = validateCandidateSourceSnapshotPreflightEvidence(value);
  return candidateSourceSnapshotPreflightBindingSchema.parse({
    buckets: evidence.targetBuckets.map((bucket) => ({
      bucket: bucket.bucket,
      conflictingObjectCount: bucket.prefixScan.conflictingObjectCount,
      domain: bucket.domain,
      headStatus: bucket.head.status,
      prefixStatus: bucket.prefixScan.status,
      storageNetworkStatus: bucket.storageNetwork.status,
    })),
    capacityProfile: {
      accountBandwidthBytes: evidence.capacityProfile.account.bandwidthBytes,
      accountStorageBytes: evidence.capacityProfile.account.storageBytes,
      buckets: evidence.capacityProfile.buckets.map((bucket) => ({
        bucket: bucket.bucket,
        domain: bucket.domain,
        storageBytes: bucket.storageBytes,
      })),
      subscriptionTierStatus: evidence.capacityProfile.subscriptionTierEvidence,
    },
    evidenceSha256: evidence.evidenceSha256,
    identities: evidence.targetIdentities.map((identity) => ({
      bucket: identity.bucket,
      controlCid: identity.control.observedCid,
      domain: identity.domain,
      ipnsLabel: identity.ipnsLabel,
      ipnsNetworkKey: identity.ipnsNetworkKey,
      officialGatewayCid: identity.officialGateway.observedCid,
      signedRecordCid: identity.signedRecord.observedCid,
    })),
    observedAt: evidence.observedAt,
    protectedSampleRollback: {
      verificationEvidenceSha256:
        evidence.protectedSampleRollback.verificationEvidenceSha256,
      verifiedAt: evidence.protectedSampleRollback.verifiedAt,
    },
    requestCount: evidence.requestCount,
  });
}

export async function writeCandidateSourceSnapshotPreflightEvidence(options: {
  dataDir: string;
  evidence: CandidateSourceSnapshotPreflightEvidence;
}): Promise<string> {
  const evidence = validateCandidateSourceSnapshotPreflightEvidence(
    options.evidence,
  );
  const requestedRoot = path.resolve(options.dataDir);
  await mkdir(requestedRoot, { mode: 0o700, recursive: true });
  const root = await realpath(requestedRoot);
  const requestedEvidenceDirectory = path.join(
    root,
    "evidence",
    "candidate-source-snapshot-demo",
    "read-only-preflight",
  );
  await mkdir(requestedEvidenceDirectory, { mode: 0o700, recursive: true });
  const evidenceDirectory = await realpath(requestedEvidenceDirectory);
  if (
    evidenceDirectory !== root &&
    !evidenceDirectory.startsWith(`${root}${path.sep}`)
  ) {
    throw new Error("Source-snapshot evidence directory escapes DATA_DIR");
  }
  const finalPath = path.join(
    evidenceDirectory,
    `${evidence.evidenceSha256}.json`,
  );
  const bytes = `${canonicalJson(evidence)}\n`;
  const temporaryPath = `${finalPath}.${process.pid}.${randomUUID()}.part`;
  await writeFile(temporaryPath, bytes, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
  try {
    try {
      await link(temporaryPath, finalPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      if ((await readFile(finalPath, "utf8")) !== bytes) {
        throw new Error(
          "Existing source-snapshot evidence bytes are inconsistent",
          { cause: error },
        );
      }
    }
  } finally {
    await unlink(temporaryPath).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
  return path.relative(root, finalPath);
}
