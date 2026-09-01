import { randomUUID } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { HeadBucketCommand, S3Client } from "@aws-sdk/client-s3";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import {
  type IpnsResolutionObservation,
  type PublicReadTransport,
} from "../mcp/public-ipns-provider.js";
import type { CandidateDemoPreflightConfig } from "./candidate-demo-config.js";
import {
  delegatedIpnsEvidenceSchema,
  observeDelegatedIpnsRecord,
} from "./delegated-ipns.js";

const cidV0Schema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const cidV1Schema = z.string().regex(/^b[a-z2-7]{20,120}$/);
const priorCidSchema = z.union([cidV0Schema, cidV1Schema]);
const filebaseNameSchema = z.strictObject({
  cid: priorCidSchema,
  created_at: z.string(),
  enabled: z.boolean(),
  label: z.string(),
  network_key: z.string().regex(/^k51[0-9a-z]{59}$/),
  published_at: z.string().nullable(),
  sequence: z.union([z.number().int().nonnegative(), z.string()]),
  updated_at: z.string(),
});

export const candidateDemoPreflightEvidenceSchema = z.strictObject({
  apiEndpoint: z.literal("https://api.filebase.io"),
  buckets: z
    .array(
      z.strictObject({
        bucket: z.string(),
        domain: z.enum(["open_data", "query_table"]),
        exists: z.literal(true),
        httpStatus: z.literal(200),
        requestAttempts: z.number().int().positive(),
        storageNetwork: z.literal("ipfs"),
        verification: z.literal(
          "candidate-declared-ipfs-resource-plus-authenticated-filebase-head",
        ),
      }),
    )
    .length(2),
  evidenceSha256: z.string().regex(/^[a-f0-9]{64}$/),
  executorEnabled: z.literal(false),
  identities: z
    .array(
      z.strictObject({
        domain: z.enum(["open_data", "query_table"]),
        ipnsLabel: z.string(),
        ipnsNetworkKey: z.string().regex(/^k51[0-9a-z]{59}$/),
        priorCid: priorCidSchema,
        publicResolverCount: z.number().int().min(1),
        publicResolutionMatched: z.literal(true),
      }),
    )
    .length(2),
  observedAt: z.string().datetime(),
  requestCeiling: z.number().int().positive(),
  s3Endpoint: z.literal("https://s3.filebase.com"),
  version: z.literal("1.0.0"),
});

export type CandidateDemoPreflightEvidence = z.infer<
  typeof candidateDemoPreflightEvidenceSchema
>;

export function validateCandidateDemoPreflightEvidence(
  value: unknown,
): CandidateDemoPreflightEvidence {
  const evidence = candidateDemoPreflightEvidenceSchema.parse(value);
  const { evidenceSha256: _evidenceSha256, ...withoutHash } = evidence;
  if (canonicalJsonSha256(withoutHash) !== evidence.evidenceSha256) {
    throw new Error("Candidate demo preflight evidence hash is invalid");
  }
  return evidence;
}

function timeoutSignal(timeoutMs: number): AbortSignal {
  return AbortSignal.timeout(timeoutMs);
}

async function boundedJson(response: Response, maximumBytes: number) {
  const declared = Number(response.headers.get("content-length") ?? 0);
  if (Number.isFinite(declared) && declared > maximumBytes) {
    throw new Error("Filebase names response exceeds the preflight limit");
  }
  if (!response.body) throw new Error("Filebase names response is empty");
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = response.body.getReader();
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw new Error("Filebase names response exceeds the preflight limit");
    }
    chunks.push(result.value);
  }
  try {
    const bytes = Buffer.concat(chunks, total);
    return {
      bytes: bytes.length,
      sha256: sha256(bytes),
      value: JSON.parse(bytes.toString("utf8")) as unknown,
    };
  } catch {
    throw new Error("Filebase names response is not valid JSON");
  }
}

async function filebaseNames(
  config: CandidateDemoPreflightConfig,
  fetchImpl: typeof fetch,
): Promise<{
  names: z.infer<typeof filebaseNameSchema>[];
  responseBytes: number;
  responseSha256: string;
}> {
  let response: Response;
  try {
    response = await fetchImpl(`${config.apiEndpoint}/v1/names`, {
      headers: {
        Authorization: `Bearer ${config.apiToken}`,
        Accept: "application/json",
      },
      method: "GET",
      redirect: "error",
      signal: timeoutSignal(config.limits.requestTimeoutMs),
    });
  } catch {
    throw new Error("Filebase names preflight failed within its timeout");
  }
  if (!response.ok) {
    throw new Error(
      `Filebase names preflight returned HTTP ${response.status}`,
    );
  }
  const result = await boundedJson(response, 64 * 1024);
  if (!Array.isArray(result.value)) {
    throw new Error("Filebase names preflight returned an invalid list");
  }
  return {
    names: z.array(filebaseNameSchema).parse(result.value),
    responseBytes: result.bytes,
    responseSha256: result.sha256,
  };
}

export interface CandidateDemoResolutionObservation {
  cacheAgeSeconds: number | null;
  httpStatus: number | null;
  observedAt: string;
  observedCid: string | null;
  ordinal: 1 | 2 | 3 | 4;
  outcome:
    "resolved" | "unavailable" | "timeout" | "http_error" | "transport_error";
  resolver: "filebase_control" | "filebase_gateway" | "ipfs_io" | "dweb_link";
  resolverType: "control_plane" | "public_resolver";
  responseBytes: number;
  responseSha256: string;
}

const signedCheckpointEndpointObservationSchema = z.strictObject({
  endpointType: z.enum(["filebase_names_control", "filebase_public_gateway"]),
  httpStatus: z.number().int().min(100).max(599).nullable(),
  latencyMs: z.number().int().min(0).max(120_000),
  observedAt: z.string().datetime(),
  observedCid: priorCidSchema.nullable(),
  outcome: z.enum([
    "resolved",
    "unavailable",
    "http_error",
    "timeout",
    "transport_error",
    "redirect_rejected",
    "response_too_large",
    "malformed_response",
  ]),
  requestCount: z.literal(1),
  responseBytes: z.number().int().min(0).max(65_536),
  responseSha256: z.string().regex(/^[a-f0-9]{64}$/),
});

export const candidateSignedIpnsCheckpointSchema = z.strictObject({
  approvalId: z.string().regex(/^demoapproval_[a-f0-9]{32}$/),
  classification: z.enum([
    "converged",
    "propagation_pending",
    "source_split",
    "source_unavailable",
    "signed_record_invalid",
    "signed_record_expired",
    "unexpected_cid",
  ]),
  delegated: delegatedIpnsEvidenceSchema,
  demoPlanId: z.string().regex(/^demo_[a-f0-9]{32}$/),
  demoPlanSha256: z.string().regex(/^[a-f0-9]{64}$/),
  domain: z.literal("query_table"),
  evidenceSha256: z.string().regex(/^[a-f0-9]{64}$/),
  filebaseControl: signedCheckpointEndpointObservationSchema.extend({
    endpointType: z.literal("filebase_names_control"),
  }),
  filebaseGateway: signedCheckpointEndpointObservationSchema.extend({
    endpointType: z.literal("filebase_public_gateway"),
  }),
  intentId: z.string().regex(/^demointent_[a-f0-9]{32}$/),
  networkKey: z.string().regex(/^k51[0-9a-z]{59}$/),
  policyVersion: z.literal("candidate_signed_ipns_observation_v1"),
  priorCid: priorCidSchema,
  requestCount: z.number().int().min(3).max(4),
  targetCid: cidV0Schema,
});

export type CandidateSignedIpnsCheckpoint = z.infer<
  typeof candidateSignedIpnsCheckpointSchema
>;

export function validateCandidateSignedIpnsCheckpoint(
  value: unknown,
): CandidateSignedIpnsCheckpoint {
  const evidence = candidateSignedIpnsCheckpointSchema.parse(value);
  const { evidenceSha256: _evidenceSha256, ...withoutHash } = evidence;
  if (canonicalJsonSha256(withoutHash) !== evidence.evidenceSha256) {
    throw new Error("Candidate signed IPNS evidence hash is invalid");
  }
  return evidence;
}

type SignedCheckpointEndpointObservation = z.infer<
  typeof signedCheckpointEndpointObservationSchema
>;

function boundedLatency(startedAt: number): number {
  return Math.min(
    120_000,
    Math.max(0, Math.round(performance.now() - startedAt)),
  );
}

function unavailableEndpointObservation(options: {
  endpointType: SignedCheckpointEndpointObservation["endpointType"];
  httpStatus: number | null;
  latencyMs: number;
  observedAt: string;
  outcome: SignedCheckpointEndpointObservation["outcome"];
  responseBytes?: number;
  responseSha256?: string;
}): SignedCheckpointEndpointObservation {
  return signedCheckpointEndpointObservationSchema.parse({
    ...options,
    observedCid: null,
    requestCount: 1,
    responseBytes: options.responseBytes ?? 0,
    responseSha256: options.responseSha256 ?? sha256(Buffer.alloc(0)),
  });
}

async function observeFilebaseControlForSignedCheckpoint(options: {
  config: CandidateDemoPreflightConfig;
  fetchImpl: typeof fetch;
}): Promise<SignedCheckpointEndpointObservation> {
  const startedAt = performance.now();
  const observedAt = new Date().toISOString();
  let response: Response;
  try {
    response = await options.fetchImpl("https://api.filebase.io/v1/names", {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${options.config.apiToken}`,
      },
      method: "GET",
      redirect: "error",
      signal: timeoutSignal(options.config.limits.requestTimeoutMs),
    });
  } catch (error) {
    const timeout =
      error instanceof DOMException &&
      (error.name === "AbortError" || error.name === "TimeoutError");
    return unavailableEndpointObservation({
      endpointType: "filebase_names_control",
      httpStatus: null,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome: timeout ? "timeout" : "transport_error",
    });
  }
  let result: Awaited<ReturnType<typeof boundedJson>>;
  try {
    result = await boundedJson(response, 64 * 1024);
  } catch (error) {
    return unavailableEndpointObservation({
      endpointType: "filebase_names_control",
      httpStatus: response.status,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome:
        error instanceof Error && error.message.includes("exceeds")
          ? "response_too_large"
          : "malformed_response",
    });
  }
  if (!response.ok) {
    return unavailableEndpointObservation({
      endpointType: "filebase_names_control",
      httpStatus: response.status,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome: "http_error",
      responseBytes: result.bytes,
      responseSha256: result.sha256,
    });
  }
  const parsed = z.array(filebaseNameSchema).safeParse(result.value);
  if (!parsed.success) {
    return unavailableEndpointObservation({
      endpointType: "filebase_names_control",
      httpStatus: response.status,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome: "malformed_response",
      responseBytes: result.bytes,
      responseSha256: result.sha256,
    });
  }
  const target = options.config.targets.queryTable;
  const name = parsed.data.find(
    (entry) =>
      entry.label === target.ipnsLabel &&
      entry.network_key === target.ipnsNetworkKey,
  );
  if (!name) {
    return unavailableEndpointObservation({
      endpointType: "filebase_names_control",
      httpStatus: response.status,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome: "unavailable",
      responseBytes: result.bytes,
      responseSha256: result.sha256,
    });
  }
  return signedCheckpointEndpointObservationSchema.parse({
    endpointType: "filebase_names_control",
    httpStatus: response.status,
    latencyMs: boundedLatency(startedAt),
    observedAt,
    observedCid: name.cid,
    outcome: "resolved",
    requestCount: 1,
    responseBytes: result.bytes,
    responseSha256: result.sha256,
  });
}

async function observeFilebaseGatewayForSignedCheckpoint(options: {
  config: CandidateDemoPreflightConfig;
  fetchImpl: typeof fetch;
}): Promise<SignedCheckpointEndpointObservation> {
  const startedAt = performance.now();
  const observedAt = new Date().toISOString();
  const identity = options.config.targets.queryTable.ipnsNetworkKey;
  let response: Response;
  try {
    response = await options.fetchImpl(
      new URL(`https://ipfs.filebase.io/ipns/${identity}`),
      {
        headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
        method: "HEAD",
        redirect: "manual",
        signal: timeoutSignal(options.config.limits.requestTimeoutMs),
      },
    );
  } catch (error) {
    const timeout =
      error instanceof DOMException &&
      (error.name === "AbortError" || error.name === "TimeoutError");
    return unavailableEndpointObservation({
      endpointType: "filebase_public_gateway",
      httpStatus: null,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      outcome: timeout ? "timeout" : "transport_error",
    });
  }
  const observedCid = cidFromResponse(response);
  if (observedCid) {
    return signedCheckpointEndpointObservationSchema.parse({
      endpointType: "filebase_public_gateway",
      httpStatus: response.status,
      latencyMs: boundedLatency(startedAt),
      observedAt,
      observedCid,
      outcome: "resolved",
      requestCount: 1,
      responseBytes: 0,
      responseSha256: sha256(Buffer.alloc(0)),
    });
  }
  return unavailableEndpointObservation({
    endpointType: "filebase_public_gateway",
    httpStatus: response.status,
    latencyMs: boundedLatency(startedAt),
    observedAt,
    outcome:
      response.status >= 300 && response.status < 400
        ? "redirect_rejected"
        : response.status >= 400
          ? "http_error"
          : "unavailable",
  });
}

function classifySignedCheckpoint(input: {
  delegated: z.infer<typeof delegatedIpnsEvidenceSchema>;
  filebaseControl: SignedCheckpointEndpointObservation;
  filebaseGateway: SignedCheckpointEndpointObservation;
  priorCid: string;
  targetCid: string;
}): CandidateSignedIpnsCheckpoint["classification"] {
  const observed = [
    input.filebaseControl.observedCid,
    input.filebaseGateway.observedCid,
    input.delegated.observedCid,
  ].filter((entry): entry is string => entry !== null);
  if (
    observed.some(
      (entry) => entry !== input.priorCid && entry !== input.targetCid,
    ) ||
    input.delegated.validationResult === "unexpected_cid"
  ) {
    return "unexpected_cid";
  }
  if (input.delegated.validationResult === "expired_record") {
    return "signed_record_expired";
  }
  if (
    !["valid_target", "valid_prior"].includes(input.delegated.validationResult)
  ) {
    return "signed_record_invalid";
  }
  if (input.delegated.validationResult === "valid_prior") {
    return "propagation_pending";
  }
  if (
    input.filebaseControl.outcome !== "resolved" ||
    input.filebaseGateway.outcome !== "resolved"
  ) {
    return "source_unavailable";
  }
  if (
    input.filebaseControl.observedCid === input.targetCid &&
    input.filebaseGateway.observedCid === input.targetCid
  ) {
    return "converged";
  }
  return "source_split";
}

export async function observeCandidateSignedIpnsCheckpoint(options: {
  approvalId: string;
  config: CandidateDemoPreflightConfig;
  demoPlanId: string;
  demoPlanSha256: string;
  expectedPriorCid: string;
  expectedTargetCid: string;
  fetchImpl?: typeof fetch;
  intentId: string;
}): Promise<CandidateSignedIpnsCheckpoint> {
  if (options.config.enabled) {
    throw new Error("Signed IPNS observation requires executor disabled");
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const [filebaseControl, filebaseGateway, delegated] = await Promise.all([
    observeFilebaseControlForSignedCheckpoint({
      config: options.config,
      fetchImpl,
    }),
    observeFilebaseGatewayForSignedCheckpoint({
      config: options.config,
      fetchImpl,
    }),
    observeDelegatedIpnsRecord({
      expectedPriorCid: options.expectedPriorCid,
      expectedTargetCid: options.expectedTargetCid,
      fetchImpl,
      maxRetries: options.config.limits.maxRetries > 0 ? 1 : 0,
      networkKey: options.config.targets.queryTable.ipnsNetworkKey,
      timeoutMs: options.config.limits.requestTimeoutMs,
    }),
  ]);
  const withoutHash = {
    approvalId: options.approvalId,
    classification: classifySignedCheckpoint({
      delegated,
      filebaseControl,
      filebaseGateway,
      priorCid: options.expectedPriorCid,
      targetCid: options.expectedTargetCid,
    }),
    delegated,
    demoPlanId: options.demoPlanId,
    demoPlanSha256: options.demoPlanSha256,
    domain: "query_table" as const,
    filebaseControl,
    filebaseGateway,
    intentId: options.intentId,
    networkKey: options.config.targets.queryTable.ipnsNetworkKey,
    policyVersion: "candidate_signed_ipns_observation_v1" as const,
    priorCid: options.expectedPriorCid,
    requestCount:
      filebaseControl.requestCount +
      filebaseGateway.requestCount +
      delegated.requestCount,
    targetCid: options.expectedTargetCid,
  };
  return candidateSignedIpnsCheckpointSchema.parse({
    ...withoutHash,
    evidenceSha256: canonicalJsonSha256(withoutHash),
  });
}

function cidFromResponse(response: Response): string | null {
  for (const value of [
    response.headers.get("x-ipfs-roots"),
    response.headers.get("x-ipfs-path"),
    response.headers.get("location"),
  ]) {
    const cid = value?.match(
      /Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/,
    )?.[0];
    if (cid) return cid;
  }
  return null;
}

async function observePublicResolver(options: {
  config: CandidateDemoPreflightConfig;
  fetchImpl: typeof fetch;
  identity: string;
  ordinal: 2 | 3 | 4;
  resolver: "filebase_gateway" | "ipfs_io" | "dweb_link";
}): Promise<CandidateDemoResolutionObservation> {
  const observedAt = new Date().toISOString();
  const url =
    options.resolver === "filebase_gateway"
      ? new URL(`https://ipfs.filebase.io/ipns/${options.identity}`)
      : options.resolver === "ipfs_io"
        ? new URL(`https://ipfs.io/ipns/${options.identity}`)
        : new URL(`https://${options.identity}.ipns.dweb.link/`);
  try {
    const response = await options.fetchImpl(url, {
      headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
      method: "HEAD",
      redirect: "manual",
      signal: timeoutSignal(options.config.limits.requestTimeoutMs),
    });
    const cid = cidFromResponse(response);
    const age = response.headers.get("age");
    const cacheAgeSeconds =
      age !== null && /^\d+$/.test(age) ? Math.min(Number(age), 3_600) : null;
    const emptyResponseHash = sha256(Buffer.alloc(0));
    if (cid !== null) {
      return {
        cacheAgeSeconds,
        httpStatus: response.status,
        observedAt,
        observedCid: cid,
        ordinal: options.ordinal,
        outcome: "resolved",
        resolver: options.resolver,
        resolverType: "public_resolver",
        responseBytes: 0,
        responseSha256: emptyResponseHash,
      };
    }
    return {
      cacheAgeSeconds,
      httpStatus: response.status,
      observedAt,
      observedCid: null,
      ordinal: options.ordinal,
      outcome: response.status >= 400 ? "http_error" : "unavailable",
      resolver: options.resolver,
      resolverType: "public_resolver",
      responseBytes: 0,
      responseSha256: emptyResponseHash,
    };
  } catch (error) {
    return {
      cacheAgeSeconds: null,
      httpStatus: null,
      observedAt,
      observedCid: null,
      ordinal: options.ordinal,
      outcome:
        error instanceof DOMException &&
        (error.name === "AbortError" || error.name === "TimeoutError")
          ? "timeout"
          : "transport_error",
      resolver: options.resolver,
      resolverType: "public_resolver",
      responseBytes: 0,
      responseSha256: sha256(Buffer.alloc(0)),
    };
  }
}

export async function observeCandidateDemoResolutionCycle(options: {
  config: CandidateDemoPreflightConfig;
  domain: "open_data" | "query_table";
  fetchImpl?: typeof fetch;
}): Promise<readonly CandidateDemoResolutionObservation[]> {
  if (options.config.enabled) {
    throw new Error(
      "Candidate recovery observation requires executor disabled",
    );
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const target =
    options.domain === "open_data"
      ? options.config.targets.openData
      : options.config.targets.queryTable;
  const namesResult = await filebaseNames(options.config, fetchImpl);
  const name = namesResult.names.find(
    (entry) =>
      entry.label === target.ipnsLabel &&
      entry.network_key === target.ipnsNetworkKey,
  );
  const control: CandidateDemoResolutionObservation = {
    cacheAgeSeconds: null,
    httpStatus: name ? 200 : null,
    observedAt: new Date().toISOString(),
    observedCid: name?.cid ?? null,
    ordinal: 1,
    outcome: name ? "resolved" : "unavailable",
    resolver: "filebase_control",
    resolverType: "control_plane",
    responseBytes: namesResult.responseBytes,
    responseSha256: namesResult.responseSha256,
  };
  const [filebaseGateway, ipfsIo, dwebLink] = await Promise.all([
    observePublicResolver({
      config: options.config,
      fetchImpl,
      identity: target.ipnsNetworkKey,
      ordinal: 2,
      resolver: "filebase_gateway",
    }),
    observePublicResolver({
      config: options.config,
      fetchImpl,
      identity: target.ipnsNetworkKey,
      ordinal: 3,
      resolver: "ipfs_io",
    }),
    observePublicResolver({
      config: options.config,
      fetchImpl,
      identity: target.ipnsNetworkKey,
      ordinal: 4,
      resolver: "dweb_link",
    }),
  ]);
  return [control, filebaseGateway, ipfsIo, dwebLink];
}

async function headBucket(
  client: S3Client,
  bucket: string,
  timeoutMs: number,
): Promise<{ attempts: number; httpStatus: number }> {
  const command = new HeadBucketCommand({ Bucket: bucket });
  try {
    const response = await client.send(command, {
      abortSignal: timeoutSignal(timeoutMs),
    });
    if (response.$metadata.httpStatusCode !== 200) {
      throw new Error("unexpected status");
    }
    return {
      attempts: response.$metadata.attempts ?? 1,
      httpStatus: response.$metadata.httpStatusCode,
    };
  } catch {
    throw new Error("Candidate Filebase bucket authentication failed");
  }
}

function publicTransport(
  config: CandidateDemoPreflightConfig,
  fetchImpl: typeof fetch,
): PublicReadTransport {
  const cidFrom = (response: Response): string | null => {
    const values = [
      response.headers.get("x-ipfs-roots"),
      response.headers.get("x-ipfs-path"),
      response.headers.get("location"),
    ].filter((value): value is string => value !== null);
    for (const value of values) {
      const match = value.match(
        /Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/,
      )?.[0];
      if (match) return match;
    }
    return null;
  };
  return {
    readCid: async () => {
      throw new Error("Candidate preflight transport does not read objects");
    },
    resolveIpns: async (
      identity: string,
    ): Promise<readonly IpnsResolutionObservation[]> => {
      if (!/^k51[0-9a-z]{59}$/.test(identity)) {
        throw new Error("Candidate IPNS identity is invalid");
      }
      const endpoints = [
        {
          resolver: "dweb_link",
          url: new URL(`https://${identity}.ipns.dweb.link/`),
        },
      ];
      return await Promise.all(
        endpoints.map(async ({ resolver, url }) => {
          let response: Response;
          try {
            response = await fetchImpl(url, {
              method: "HEAD",
              redirect: "manual",
              signal: timeoutSignal(config.limits.requestTimeoutMs),
            });
          } catch {
            throw new Error(
              `Candidate public IPNS resolver ${resolver} is unavailable`,
            );
          }
          if (!(
            response.ok ||
            (response.status >= 300 && response.status < 400)
          )) {
            throw new Error(
              `Candidate public IPNS resolver ${resolver} returned an unavailable status`,
            );
          }
          const cid = cidFrom(response);
          return {
            cacheAgeSeconds: null,
            cid,
            observedAt: new Date().toISOString(),
            resolver,
            status: cid === null ? "unavailable" : "resolved",
          } as const;
        }),
      );
    },
  };
}

export async function runCandidateDemoReadOnlyPreflight(options: {
  config: CandidateDemoPreflightConfig;
  fetchImpl?: typeof fetch;
  observedAt?: string;
  publicReadTransport?: PublicReadTransport;
  s3Client?: S3Client;
}): Promise<CandidateDemoPreflightEvidence> {
  const { config } = options;
  if (config.enabled || config.s3Endpoint !== "https://s3.filebase.com") {
    throw new Error("Candidate read-only preflight configuration is unsafe");
  }
  const fetchImpl = options.fetchImpl ?? fetch;
  const s3 =
    options.s3Client ??
    new S3Client({
      credentials: {
        accessKeyId: config.s3AccessKeyId,
        secretAccessKey: config.s3SecretAccessKey,
      },
      endpoint: config.s3Endpoint,
      forcePathStyle: true,
      maxAttempts: config.limits.maxRetries + 1,
      region: "us-east-1",
    });
  const [openBucket, queryBucket, namesResult] = await Promise.all([
    headBucket(
      s3,
      config.targets.openData.bucket,
      config.limits.requestTimeoutMs,
    ),
    headBucket(
      s3,
      config.targets.queryTable.bucket,
      config.limits.requestTimeoutMs,
    ),
    filebaseNames(config, fetchImpl),
  ]);
  const readTransport =
    options.publicReadTransport ?? publicTransport(config, fetchImpl);
  const targetInputs = [
    {
      bucket: config.targets.openData.bucket,
      domain: "open_data" as const,
      head: openBucket,
      target: config.targets.openData,
    },
    {
      bucket: config.targets.queryTable.bucket,
      domain: "query_table" as const,
      head: queryBucket,
      target: config.targets.queryTable,
    },
  ];
  const identities = await Promise.all(
    targetInputs.map(async ({ domain, target }) => {
      const record = namesResult.names.find(
        (entry) =>
          entry.label === target.ipnsLabel &&
          entry.network_key === target.ipnsNetworkKey,
      );
      if (!record) {
        throw new Error(
          `Configured candidate ${domain} IPNS identity is unavailable`,
        );
      }
      const observations = await readTransport.resolveIpns(
        target.ipnsNetworkKey,
      );
      if (
        observations.length < 1 ||
        observations.some(
          (observation) =>
            observation.status !== "resolved" || observation.cid !== record.cid,
        )
      ) {
        throw new Error(
          `Candidate ${domain} public IPNS resolution does not match Filebase`,
        );
      }
      return {
        domain,
        ipnsLabel: target.ipnsLabel,
        ipnsNetworkKey: target.ipnsNetworkKey,
        priorCid: record.cid,
        publicResolutionMatched: true as const,
        publicResolverCount: observations.length,
      };
    }),
  );
  if (identities[0]?.priorCid === identities[1]?.priorCid) {
    throw new Error("Candidate demo domains require distinct bootstrap CIDs");
  }
  const withoutHash = {
    apiEndpoint: config.apiEndpoint as "https://api.filebase.io",
    buckets: targetInputs.map(({ bucket, domain, head }) => ({
      bucket,
      domain,
      exists: true as const,
      httpStatus: 200 as const,
      requestAttempts: head.attempts,
      storageNetwork: "ipfs" as const,
      verification:
        "candidate-declared-ipfs-resource-plus-authenticated-filebase-head" as const,
    })),
    executorEnabled: false as const,
    identities,
    observedAt: options.observedAt ?? new Date().toISOString(),
    requestCeiling: 3 * (config.limits.maxRetries + 1) + identities.length,
    s3Endpoint: config.s3Endpoint as "https://s3.filebase.com",
    version: "1.0.0" as const,
  };
  return candidateDemoPreflightEvidenceSchema.parse({
    ...withoutHash,
    evidenceSha256: canonicalJsonSha256(withoutHash),
  });
}

export async function writeCandidateDemoPreflightEvidence(options: {
  dataDir: string;
  evidence: CandidateDemoPreflightEvidence;
}): Promise<string> {
  const evidence = validateCandidateDemoPreflightEvidence(options.evidence);
  const root = path.resolve(options.dataDir);
  const evidenceDir = path.join(
    root,
    "evidence",
    "candidate-demo",
    "filebase-preflight",
  );
  await mkdir(evidenceDir, { mode: 0o700, recursive: true });
  const finalPath = path.join(evidenceDir, `${evidence.evidenceSha256}.json`);
  const temporaryPath = `${finalPath}.${process.pid}.${randomUUID()}.part`;
  await writeFile(temporaryPath, `${canonicalJson(evidence)}\n`, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
  try {
    await rename(temporaryPath, finalPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  return path.relative(root, finalPath);
}
