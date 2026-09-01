import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { finished } from "node:stream/promises";
import { lstat, open, realpath } from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";

import { CID } from "multiformats/cid";

import { canonicalJson } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import type { CandidateSourceSnapshotCarImportAttempt as DurableCarImportAttempt } from "../db/candidate-source-snapshot-car-attempt.js";
import type { CandidateSourceSnapshotCarImportAuthorization } from "../db/candidate-source-snapshot-car-import.js";
import { validateCarV1 } from "./car-v1.js";

export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_ENDPOINT =
  "https://rpc.filebase.io/api/v0/dag/import?pin-roots=true";
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_POLICY =
  "filebase-rpc-car-import-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_PIN_ENDPOINT =
  "https://rpc.filebase.io/api/v0/pin/ls?type=recursive";
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_GATEWAY_ORIGIN =
  "https://ipfs.filebase.io";
const MULTIPART_BOUNDARY = "----prism-candidate-source-snapshot-car-v1";
// dag/import emits one bounded JSON result per CAR root. The reviewed open-data
// CAR has 325,311 roots, so its valid response is necessarily larger than the
// usual small control-plane response while remaining strictly bounded.
const MAX_RESPONSE_BYTES = 128 * 1024 * 1024;
const MAX_CAR_BYTES = 8 * 1024 * 1024 * 1024;
const MAX_CAR_ROOTS = 350_000;
const MAX_HEADER_BYTES = 32 * 1024 * 1024;
const MAX_SECTION_BYTES = 1024 * 1024;
const MAX_GATEWAY_ROOT_BLOCK_BYTES = 1024 * 1024;
const MIN_OVERALL_TIMEOUT_MS = 60_000;
const MAX_OVERALL_TIMEOUT_MS = 4 * 60 * 60 * 1000;
const CIDV0_PATTERN = /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;

export interface CandidateSourceSnapshotCarImportArtifact {
  artifactId: string;
  blockCount: number;
  blockMembershipSha256: string;
  bucketName: string;
  carBytes: number;
  carSha256: string;
  domain: "open_data" | "query_table";
  filePath: string;
  implementationCommitSha: string;
  planId: string;
  planSha256: string;
  roots: readonly string[];
}

export interface CandidateSourceSnapshotCarImportAttempt {
  admission: DurableCarImportAttempt;
  artifact: CandidateSourceSnapshotCarImportArtifact;
  authorization: CandidateSourceSnapshotCarImportAuthorizationBinding;
  signal?: AbortSignal;
}

export interface CandidateSourceSnapshotCarImportAuthorizationBinding extends CandidateSourceSnapshotCarImportAuthorization {
  openDataBucketTokenSha256: string;
  overallTimeoutMs: number;
  queryTableBucketTokenSha256: string;
}

export interface CandidateSourceSnapshotFilebaseCarImportConfig {
  buckets: Readonly<
    Record<
      "open_data" | "query_table",
      { bucketName: string; bucketScopedBearerToken: string }
    >
  >;
}

export interface CandidateSourceSnapshotCarImportHttpEvidence {
  endpointPolicy:
    | "filebase_official_gateway_raw_block_v1"
    | "filebase_rpc_car_import_v1"
    | "filebase_rpc_recursive_pin_list_v1";
  httpStatus: number;
  latencyMs: number;
  providerRequestIdHash: string | null;
  responseBytes: number;
  responseSha256: string;
}

export interface CandidateSourceSnapshotCarImportEvidence {
  gatewayRoot: CandidateSourceSnapshotCarImportHttpEvidence;
  import: CandidateSourceSnapshotCarImportHttpEvidence;
  recursivePin: CandidateSourceSnapshotCarImportHttpEvidence;
  schemaVersion: "candidate-source-snapshot-car-provider-evidence-v1";
}

export type CandidateSourceSnapshotCarImportFailureCode =
  | "caller_aborted"
  | "provider_pin_error"
  | "provider_rejected"
  | "provider_result_invalid"
  | "provider_root_mismatch"
  | "provider_retryable_status"
  | "redirect_rejected"
  | "response_too_large"
  | "stream_integrity_unknown"
  | "timeout_unknown"
  | "transport_unknown";

type CandidateSourceSnapshotCarImportFailureClass =
  "outcome_unknown" | "retryable" | "terminal";

interface CandidateSourceSnapshotCarImportResultBase {
  attemptNumber: number;
  endpointPolicy: typeof CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_POLICY;
  failureStage: "gateway_root" | "import" | "recursive_pin" | null;
  httpStatus: number | null;
  latencyMs: number;
  providerEvidenceSha256: string;
  providerRequestIdHash: string | null;
  responseBytes: number;
  responseSha256: string | null;
}

export type CandidateSourceSnapshotCarImportResult =
  | (CandidateSourceSnapshotCarImportResultBase & {
      failureClass: null;
      observedRoots: string[];
      observedRootsSha256: string;
      outcome: "verified";
      providerImportResult: "expected_root_set_returned";
      sanitizedEvidence: CandidateSourceSnapshotCarImportEvidence;
      /** Ephemeral verified block for the DB gateway-evidence validator. Never persist or log. */
      verifiedRootBlock: Uint8Array;
    })
  | (CandidateSourceSnapshotCarImportResultBase & {
      failureClass: CandidateSourceSnapshotCarImportFailureClass;
      failureCode: CandidateSourceSnapshotCarImportFailureCode;
      outcome: "outcome_unknown" | "retryable_failure" | "terminal_failure";
    });

interface CandidateSourceSnapshotCarImportRequestInit {
  body: Readable | null;
  headers: Readonly<Record<string, string>>;
  method: "GET" | "POST";
  redirect: "manual";
  signal: AbortSignal;
}

export type CandidateSourceSnapshotCarImportFetch = (
  endpoint: URL,
  init: CandidateSourceSnapshotCarImportRequestInit,
) => Promise<Response>;

export interface CandidateSourceSnapshotCarImportInspectionResult {
  inspectedRootBlock: Uint8Array | null;
  inspectionResult: "conclusively_absent" | "present_exact" | "unavailable";
  observedRootSetSha256: string | null;
  pinStatus: "absent" | "pinned" | "unavailable";
  providerEvidenceSha256: string;
  providerHttpStatus: number | null;
  providerRequestIdHash: string | null;
  providerResponseBytes: number | null;
  rootStatus: "absent" | "present_exact" | "unavailable";
}

export class CandidateSourceSnapshotCarImportBoundaryError extends Error {
  readonly code:
    | "attempt_binding_invalid"
    | "authorization_invalid"
    | "car_binding_invalid"
    | "car_validation_failed"
    | "timeout_binding_invalid";

  constructor(code: CandidateSourceSnapshotCarImportBoundaryError["code"]) {
    super(`Candidate CAR import boundary rejected (${code})`);
    this.name = "CandidateSourceSnapshotCarImportBoundaryError";
    this.code = code;
  }
}

class BoundedResponseError extends Error {
  readonly code: "response_stream_failed" | "response_too_large";

  constructor(code: BoundedResponseError["code"]) {
    super("Candidate CAR import response failed its bounded read");
    this.name = "BoundedResponseError";
    this.code = code;
  }
}

function safeInteger(value: number, minimum: number, maximum: number): boolean {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function validatedRoots(values: readonly string[]): string[] {
  if (values.length < 1 || values.length > MAX_CAR_ROOTS) {
    throw new CandidateSourceSnapshotCarImportBoundaryError(
      "car_binding_invalid",
    );
  }
  const roots: string[] = [];
  const unique = new Set<string>();
  for (const value of values) {
    if (!CIDV0_PATTERN.test(value)) {
      throw new CandidateSourceSnapshotCarImportBoundaryError(
        "car_binding_invalid",
      );
    }
    let cid: CID;
    try {
      cid = CID.parse(value);
    } catch {
      throw new CandidateSourceSnapshotCarImportBoundaryError(
        "car_binding_invalid",
      );
    }
    if (cid.version !== 0 || cid.toString() !== value || unique.has(value)) {
      throw new CandidateSourceSnapshotCarImportBoundaryError(
        "car_binding_invalid",
      );
    }
    unique.add(value);
    roots.push(value);
  }
  return roots;
}

function validateAttemptBinding(
  input: CandidateSourceSnapshotCarImportAttempt,
  config: CandidateSourceSnapshotFilebaseCarImportConfig,
): {
  artifact: CandidateSourceSnapshotCarImportArtifact;
  bucketScopedBearerToken: string;
  roots: string[];
} {
  const admission = input.admission;
  const authorization = input.authorization;
  const artifact = input.artifact;
  if (
    admission.isReplay ||
    !safeInteger(authorization.maximumAttemptsPerArtifact, 1, 2) ||
    !safeInteger(
      admission.attemptSequence,
      1,
      authorization.maximumAttemptsPerArtifact,
    ) ||
    admission.artifactId !== artifact.artifactId ||
    admission.authorizationId !== authorization.authorizationId ||
    admission.planId !== artifact.planId ||
    authorization.planId !== artifact.planId ||
    authorization.planSha256 !== artifact.planSha256 ||
    authorization.implementationCommitSha !==
      artifact.implementationCommitSha ||
    authorization.endpoint !==
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_ENDPOINT ||
    authorization.importMethod !== "rpc_dag_import" ||
    !/^snapshotdemocar_[a-f0-9]{32}$/.test(artifact.artifactId) ||
    !/^snapshotdemo_[a-f0-9]{32}$/.test(artifact.planId) ||
    !/^[a-f0-9]{40}$/.test(artifact.implementationCommitSha) ||
    !SHA256_PATTERN.test(artifact.planSha256)
  ) {
    throw new CandidateSourceSnapshotCarImportBoundaryError(
      "attempt_binding_invalid",
    );
  }
  if (
    !safeInteger(
      authorization.overallTimeoutMs,
      MIN_OVERALL_TIMEOUT_MS,
      MAX_OVERALL_TIMEOUT_MS,
    )
  ) {
    throw new CandidateSourceSnapshotCarImportBoundaryError(
      "timeout_binding_invalid",
    );
  }
  const bucket = config.buckets[artifact.domain];
  const authorizedTokenSha256 =
    artifact.domain === "open_data"
      ? authorization.openDataBucketTokenSha256
      : authorization.queryTableBucketTokenSha256;
  if (
    authorization.openDataBucketTokenSha256 ===
      authorization.queryTableBucketTokenSha256 ||
    !SHA256_PATTERN.test(authorization.openDataBucketTokenSha256) ||
    !SHA256_PATTERN.test(authorization.queryTableBucketTokenSha256) ||
    bucket.bucketName !== artifact.bucketName ||
    !/^cand-[a-z0-9-]{3,120}$/.test(bucket.bucketName) ||
    bucket.bucketScopedBearerToken.length < 1 ||
    bucket.bucketScopedBearerToken.length > 8192 ||
    !/^[\x21-\x7e]+$/.test(bucket.bucketScopedBearerToken) ||
    sha256(bucket.bucketScopedBearerToken) !== authorizedTokenSha256 ||
    sha256(config.buckets.open_data.bucketScopedBearerToken) ===
      sha256(config.buckets.query_table.bucketScopedBearerToken)
  ) {
    throw new CandidateSourceSnapshotCarImportBoundaryError(
      "authorization_invalid",
    );
  }
  if (
    !safeInteger(artifact.blockCount, 1, 2_000_000) ||
    !safeInteger(artifact.carBytes, 1, MAX_CAR_BYTES) ||
    !SHA256_PATTERN.test(artifact.carSha256) ||
    !SHA256_PATTERN.test(artifact.blockMembershipSha256)
  ) {
    throw new CandidateSourceSnapshotCarImportBoundaryError(
      "car_binding_invalid",
    );
  }
  return {
    artifact,
    bucketScopedBearerToken: bucket.bucketScopedBearerToken,
    roots: validatedRoots(artifact.roots),
  };
}

function elapsedMilliseconds(startedAt: number): number {
  return Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Date.now() - startedAt));
}

function providerRequestIdHash(response: Response): string | null {
  for (const header of ["x-request-id", "x-amz-request-id"] as const) {
    const value = response.headers.get(header);
    if (value && value.length <= 1024) return sha256(value);
  }
  return null;
}

async function readBoundedResponse(
  response: Response,
  maximumBytes = MAX_RESPONSE_BYTES,
): Promise<{
  bytes: Buffer;
  byteCount: number;
  sha256: string;
}> {
  if (!response.body) {
    const bytes = Buffer.alloc(0);
    return { byteCount: 0, bytes, sha256: sha256(bytes) };
  }
  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let byteCount = 0;
  try {
    while (true) {
      let next: ReadableStreamReadResult<Uint8Array>;
      try {
        next = await reader.read();
      } catch {
        throw new BoundedResponseError("response_stream_failed");
      }
      if (next.done) break;
      const chunk = Buffer.from(next.value);
      byteCount += chunk.byteLength;
      if (byteCount > maximumBytes) {
        await reader.cancel().catch(() => undefined);
        throw new BoundedResponseError("response_too_large");
      }
      chunks.push(chunk);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = Buffer.concat(chunks, byteCount);
  return { byteCount, bytes, sha256: sha256(bytes) };
}

function httpEvidence(
  endpointPolicy: CandidateSourceSnapshotCarImportHttpEvidence["endpointPolicy"],
  response: Response,
  bounded: { byteCount: number; sha256: string },
  latencyMs: number,
): CandidateSourceSnapshotCarImportHttpEvidence {
  return {
    endpointPolicy,
    httpStatus: response.status,
    latencyMs,
    providerRequestIdHash: providerRequestIdHash(response),
    responseBytes: bounded.byteCount,
    responseSha256: bounded.sha256,
  };
}

function responseRecords(bytes: Buffer): unknown[] {
  if (bytes.byteLength === 0) return [];
  const text = bytes.toString("utf8");
  try {
    const parsed = JSON.parse(text) as unknown;
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    const lines = text
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .filter(Boolean);
    try {
      return lines.map((line) => JSON.parse(line) as unknown);
    } catch {
      return [];
    }
  }
}

function objectValue(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function parseRecursivePinRoots(bytes: Buffer): Set<string> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes.toString("utf8")) as unknown;
  } catch {
    return null;
  }
  const keys = objectValue(objectValue(parsed)?.Keys);
  if (!keys) return null;
  const recursive = new Set<string>();
  for (const [cid, value] of Object.entries(keys)) {
    if (CIDV0_PATTERN.test(cid) && objectValue(value)?.Type === "recursive") {
      recursive.add(cid);
    }
  }
  return recursive;
}

function parseProviderRoots(
  bytes: Buffer,
):
  | { code: "invalid" | "pin_error"; roots: string[] }
  | { code: "valid"; roots: string[] } {
  const records = responseRecords(bytes);
  const roots: string[] = [];
  for (const record of records) {
    const root = objectValue(objectValue(record)?.Root);
    const cid = objectValue(root?.Cid)?.["/"];
    const pinError = root?.PinErrorMsg;
    if (
      typeof cid !== "string" ||
      !CIDV0_PATTERN.test(cid) ||
      typeof pinError !== "string"
    ) {
      return { code: "invalid", roots };
    }
    if (typeof pinError === "string" && pinError.length > 0) {
      return { code: "pin_error", roots };
    }
    roots.push(cid);
  }
  return { code: roots.length > 0 ? "valid" : "invalid", roots };
}

function exactRootSet(
  observed: readonly string[],
  expected: readonly string[],
): boolean {
  if (observed.length !== expected.length) return false;
  const observedSet = new Set(observed);
  return (
    observedSet.size === observed.length &&
    expected.every((root) => observedSet.has(root))
  );
}

function responseFailure(input: {
  attemptNumber: number;
  code: CandidateSourceSnapshotCarImportFailureCode;
  evidence?: readonly CandidateSourceSnapshotCarImportHttpEvidence[];
  failureClass: CandidateSourceSnapshotCarImportFailureClass;
  failureStage: "gateway_root" | "import" | "recursive_pin";
  httpStatus: number | null;
  latencyMs: number;
  providerRequestIdHash: string | null;
  responseBytes?: number;
  responseSha256?: string | null;
}): CandidateSourceSnapshotCarImportResult {
  return {
    attemptNumber: input.attemptNumber,
    endpointPolicy: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_POLICY,
    failureClass: input.failureClass,
    failureCode: input.code,
    failureStage: input.failureStage,
    httpStatus: input.httpStatus,
    latencyMs: input.latencyMs,
    outcome:
      input.failureClass === "outcome_unknown"
        ? "outcome_unknown"
        : input.failureClass === "retryable"
          ? "retryable_failure"
          : "terminal_failure",
    providerEvidenceSha256: sha256(
      canonicalJson({
        evidence: input.evidence ?? [],
        failureCode: input.code,
        failureStage: input.failureStage,
        schemaVersion: "candidate-source-snapshot-car-provider-failure-v1",
      }),
    ),
    providerRequestIdHash: input.providerRequestIdHash,
    responseBytes: input.responseBytes ?? 0,
    responseSha256: input.responseSha256 ?? null,
  };
}

function defaultFetch(
  endpoint: URL,
  init: CandidateSourceSnapshotCarImportRequestInit,
): Promise<Response> {
  const request: RequestInit & { duplex?: "half" } = {
    // Node requires duplex for a streamed request body. It is intentionally
    // isolated from the portable RequestInit type exposed by TypeScript.
    duplex: "half",
    headers: init.headers,
    method: init.method,
    redirect: init.redirect,
    signal: init.signal,
  };
  if (init.body !== null) {
    request.body = init.body as unknown as BodyInit;
    request.duplex = "half";
  }
  return fetch(endpoint, request);
}

/**
 * Closed, single-attempt Filebase RPC CAR import primitive. Attempt admission,
 * durable receipts, ambiguous-result recovery, and retry decisions remain the
 * responsibility of the commit-bound publication controller.
 */
export class CandidateSourceSnapshotFilebaseCarImportTransport {
  readonly #config: CandidateSourceSnapshotFilebaseCarImportConfig;
  readonly #fetch: CandidateSourceSnapshotCarImportFetch;

  constructor(options: {
    config: CandidateSourceSnapshotFilebaseCarImportConfig;
    fetch?: CandidateSourceSnapshotCarImportFetch;
  }) {
    this.#config = options.config;
    this.#fetch = options.fetch ?? defaultFetch;
  }

  /** One bounded, read-only inspection after an admitted unknown import. */
  async inspectCar(
    input: CandidateSourceSnapshotCarImportAttempt,
  ): Promise<CandidateSourceSnapshotCarImportInspectionResult> {
    const { bucketScopedBearerToken, roots } = validateAttemptBinding(
      input,
      this.#config,
    );
    if (input.signal?.aborted) {
      return {
        inspectedRootBlock: null,
        inspectionResult: "unavailable",
        observedRootSetSha256: null,
        pinStatus: "unavailable",
        providerEvidenceSha256: sha256(canonicalJson([])),
        providerHttpStatus: null,
        providerRequestIdHash: null,
        providerResponseBytes: null,
        rootStatus: "unavailable",
      };
    }
    const controller = new AbortController();
    const parentAbort = (): void => controller.abort(input.signal?.reason);
    input.signal?.addEventListener("abort", parentAbort, { once: true });
    const timeout = setTimeout(
      () => controller.abort(new Error("Candidate CAR inspection deadline")),
      input.authorization.overallTimeoutMs,
    );
    timeout.unref?.();
    const evidence: CandidateSourceSnapshotCarImportHttpEvidence[] = [];
    try {
      const pinStartedAt = Date.now();
      const pinResponse = await this.#fetch(
        new URL(CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_PIN_ENDPOINT),
        {
          body: null,
          headers: {
            Accept: "application/json",
            Authorization: `Bearer ${bucketScopedBearerToken}`,
            "Cache-Control": "no-store",
          },
          method: "POST",
          redirect: "manual",
          signal: controller.signal,
        },
      );
      if (pinResponse.status !== 200) {
        const bounded = await readBoundedResponse(pinResponse).catch(
          () => null,
        );
        if (bounded) {
          evidence.push(
            httpEvidence(
              "filebase_rpc_recursive_pin_list_v1",
              pinResponse,
              bounded,
              elapsedMilliseconds(pinStartedAt),
            ),
          );
        }
        return {
          inspectedRootBlock: null,
          inspectionResult: "unavailable",
          observedRootSetSha256: null,
          pinStatus: "unavailable",
          providerEvidenceSha256: sha256(canonicalJson(evidence)),
          providerHttpStatus: pinResponse.status,
          providerRequestIdHash: providerRequestIdHash(pinResponse),
          providerResponseBytes: bounded?.byteCount ?? null,
          rootStatus: "unavailable",
        };
      }
      const pinBounded = await readBoundedResponse(pinResponse);
      const pinEvidence = httpEvidence(
        "filebase_rpc_recursive_pin_list_v1",
        pinResponse,
        pinBounded,
        elapsedMilliseconds(pinStartedAt),
      );
      evidence.push(pinEvidence);
      const pinnedRoots = parseRecursivePinRoots(pinBounded.bytes);

      const gatewayStartedAt = Date.now();
      const gatewayResponse = await this.#fetch(
        new URL(
          `/ipfs/${encodeURIComponent(roots[0]!)}?format=raw`,
          CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_GATEWAY_ORIGIN,
        ),
        {
          body: null,
          headers: { Accept: "application/vnd.ipld.raw" },
          method: "GET",
          redirect: "manual",
          signal: controller.signal,
        },
      );
      const gatewayBounded = await readBoundedResponse(
        gatewayResponse,
        MAX_GATEWAY_ROOT_BLOCK_BYTES,
      );
      const gatewayEvidence = httpEvidence(
        "filebase_official_gateway_raw_block_v1",
        gatewayResponse,
        gatewayBounded,
        elapsedMilliseconds(gatewayStartedAt),
      );
      evidence.push(gatewayEvidence);
      const expectedRootSha256 = Buffer.from(
        CID.parse(roots[0]!).multihash.digest,
      ).toString("hex");
      const allPinned =
        pinnedRoots !== null && roots.every((root) => pinnedRoots.has(root));
      const nonePinned =
        pinnedRoots !== null && roots.every((root) => !pinnedRoots.has(root));
      const gatewayExact =
        gatewayResponse.status === 200 &&
        gatewayBounded.sha256 === expectedRootSha256;
      const gatewayAbsent = [404, 410].includes(gatewayResponse.status);
      const evidenceSha256 = sha256(canonicalJson(evidence));
      const requestId =
        pinEvidence.providerRequestIdHash ??
        gatewayEvidence.providerRequestIdHash;
      if (allPinned && gatewayExact) {
        return {
          inspectedRootBlock: new Uint8Array(gatewayBounded.bytes),
          inspectionResult: "present_exact",
          observedRootSetSha256: sha256(canonicalJson(roots)),
          pinStatus: "pinned",
          providerEvidenceSha256: evidenceSha256,
          providerHttpStatus: gatewayResponse.status,
          providerRequestIdHash: requestId,
          providerResponseBytes:
            pinBounded.byteCount + gatewayBounded.byteCount,
          rootStatus: "present_exact",
        };
      }
      if (nonePinned && gatewayAbsent) {
        return {
          inspectedRootBlock: null,
          inspectionResult: "conclusively_absent",
          observedRootSetSha256: null,
          pinStatus: "absent",
          providerEvidenceSha256: evidenceSha256,
          providerHttpStatus: gatewayResponse.status,
          providerRequestIdHash: requestId,
          providerResponseBytes:
            pinBounded.byteCount + gatewayBounded.byteCount,
          rootStatus: "absent",
        };
      }
      return {
        inspectedRootBlock: null,
        inspectionResult: "unavailable",
        observedRootSetSha256: null,
        pinStatus: "unavailable",
        providerEvidenceSha256: evidenceSha256,
        providerHttpStatus: gatewayResponse.status,
        providerRequestIdHash: requestId,
        providerResponseBytes: pinBounded.byteCount + gatewayBounded.byteCount,
        rootStatus: "unavailable",
      };
    } catch {
      return {
        inspectedRootBlock: null,
        inspectionResult: "unavailable",
        observedRootSetSha256: null,
        pinStatus: "unavailable",
        providerEvidenceSha256: sha256(canonicalJson(evidence)),
        providerHttpStatus: null,
        providerRequestIdHash: null,
        providerResponseBytes: null,
        rootStatus: "unavailable",
      };
    } finally {
      clearTimeout(timeout);
      input.signal?.removeEventListener("abort", parentAbort);
    }
  }

  async importCar(
    input: CandidateSourceSnapshotCarImportAttempt,
  ): Promise<CandidateSourceSnapshotCarImportResult> {
    const { artifact, bucketScopedBearerToken, roots } = validateAttemptBinding(
      input,
      this.#config,
    );
    if (input.signal?.aborted) {
      return responseFailure({
        attemptNumber: input.admission.attemptSequence,
        code: "caller_aborted",
        failureClass: "terminal",
        failureStage: "import",
        httpStatus: null,
        latencyMs: 0,
        providerRequestIdHash: null,
      });
    }

    const resolvedPath = path.resolve(artifact.filePath);
    try {
      const [linkState, actualPath] = await Promise.all([
        lstat(resolvedPath),
        realpath(resolvedPath),
      ]);
      if (
        linkState.isSymbolicLink() ||
        !linkState.isFile() ||
        actualPath !== resolvedPath
      ) {
        throw new Error("invalid CAR path");
      }
      const validated = await validateCarV1({
        expectedBlockCount: artifact.blockCount,
        expectedBlockMembershipSha256: artifact.blockMembershipSha256,
        expectedRoots: roots,
        filePath: resolvedPath,
        maxHeaderBytes: MAX_HEADER_BYTES,
        maxSectionBytes: MAX_SECTION_BYTES,
      });
      if (
        validated.byteSize !== artifact.carBytes ||
        validated.sha256 !== artifact.carSha256
      ) {
        throw new Error("invalid CAR identity");
      }
    } catch {
      throw new CandidateSourceSnapshotCarImportBoundaryError(
        "car_validation_failed",
      );
    }

    const prefix = Buffer.from(
      `--${MULTIPART_BOUNDARY}\r\n` +
        'Content-Disposition: form-data; name="file"; filename="candidate.car"\r\n' +
        "Content-Type: application/vnd.ipld.car\r\n\r\n",
      "utf8",
    );
    const suffix = Buffer.from(`\r\n--${MULTIPART_BOUNDARY}--\r\n`, "utf8");
    const contentLength =
      prefix.byteLength + artifact.carBytes + suffix.byteLength;
    const controller = new AbortController();
    const parentAbort = (): void => controller.abort(input.signal?.reason);
    input.signal?.addEventListener("abort", parentAbort, { once: true });
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort(new Error("Candidate CAR import deadline exceeded"));
    }, input.authorization.overallTimeoutMs);
    timeout.unref?.();

    let carBytesRead = 0;
    const carHash = createHash("sha256");
    let carComplete = false;
    const multipart = Readable.from(
      (async function* (): AsyncGenerator<Buffer> {
        yield prefix;
        const handle = await open(
          resolvedPath,
          constants.O_RDONLY | constants.O_NOFOLLOW,
        );
        const fileStream = handle.createReadStream({
          autoClose: true,
          start: 0,
        });
        try {
          for await (const value of fileStream) {
            const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
            carBytesRead += chunk.byteLength;
            carHash.update(chunk);
            yield chunk;
          }
          carComplete = true;
        } finally {
          if (!fileStream.destroyed) fileStream.destroy();
          await finished(fileStream).catch(() => undefined);
        }
        yield suffix;
      })(),
    );
    const startedAt = Date.now();
    let response: Response | null = null;
    try {
      response = await this.#fetch(
        new URL(CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_ENDPOINT),
        {
          body: multipart,
          headers: {
            Accept: "application/json",
            Authorization: `Bearer ${bucketScopedBearerToken}`,
            "Cache-Control": "no-store",
            "Content-Length": String(contentLength),
            "Content-Type": `multipart/form-data; boundary=${MULTIPART_BOUNDARY}`,
          },
          method: "POST",
          redirect: "manual",
          signal: controller.signal,
        },
      );
      await finished(multipart);
      const latencyMs = elapsedMilliseconds(startedAt);
      const requestIdHash = providerRequestIdHash(response);
      if (
        !carComplete ||
        carBytesRead !== artifact.carBytes ||
        carHash.digest("hex") !== artifact.carSha256
      ) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "stream_integrity_unknown",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs,
          providerRequestIdHash: requestIdHash,
        });
      }
      if (response.status >= 300 && response.status < 400) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "redirect_rejected",
          failureClass: "terminal",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs,
          providerRequestIdHash: requestIdHash,
        });
      }
      let bounded: Awaited<ReturnType<typeof readBoundedResponse>>;
      try {
        bounded = await readBoundedResponse(response);
      } catch (error) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code:
            error instanceof BoundedResponseError &&
            error.code === "response_too_large"
              ? "response_too_large"
              : timedOut
                ? "timeout_unknown"
                : "transport_unknown",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
        });
      }
      const responseIdentity = {
        responseBytes: bounded.byteCount,
        responseSha256: bounded.sha256,
      };
      if (response.status === 429) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          // The complete CAR request body has already been streamed. A 429 at
          // this boundary is therefore not safe evidence that no import began.
          code: "transport_unknown",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      if (response.status >= 500) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "transport_unknown",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      if (response.status < 200 || response.status >= 300) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "provider_rejected",
          failureClass: "terminal",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      const parsed = parseProviderRoots(bounded.bytes);
      if (parsed.code === "pin_error") {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "provider_pin_error",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      if (parsed.code === "invalid") {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "provider_result_invalid",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      if (!exactRootSet(parsed.roots, roots)) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code: "provider_root_mismatch",
          failureClass: "outcome_unknown",
          failureStage: "import",
          httpStatus: response.status,
          latencyMs: elapsedMilliseconds(startedAt),
          providerRequestIdHash: requestIdHash,
          ...responseIdentity,
        });
      }
      const importEvidence = httpEvidence(
        "filebase_rpc_car_import_v1",
        response,
        bounded,
        elapsedMilliseconds(startedAt),
      );

      const pinStartedAt = Date.now();
      let pinResponse: Response;
      let pinBounded: Awaited<ReturnType<typeof readBoundedResponse>>;
      try {
        pinResponse = await this.#fetch(
          new URL(CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_PIN_ENDPOINT),
          {
            body: null,
            headers: {
              Accept: "application/json",
              Authorization: `Bearer ${bucketScopedBearerToken}`,
              "Cache-Control": "no-store",
            },
            method: "POST",
            redirect: "manual",
            signal: controller.signal,
          },
        );
        if (pinResponse.status >= 300 && pinResponse.status < 400) {
          return responseFailure({
            attemptNumber: input.admission.attemptSequence,
            code: "redirect_rejected",
            failureClass: "outcome_unknown",
            failureStage: "recursive_pin",
            httpStatus: pinResponse.status,
            latencyMs: elapsedMilliseconds(pinStartedAt),
            providerRequestIdHash: providerRequestIdHash(pinResponse),
            evidence: [importEvidence],
          });
        }
        pinBounded = await readBoundedResponse(pinResponse);
      } catch (error) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code:
            error instanceof BoundedResponseError &&
            error.code === "response_too_large"
              ? "response_too_large"
              : timedOut
                ? "timeout_unknown"
                : "transport_unknown",
          failureClass: "outcome_unknown",
          failureStage: "recursive_pin",
          httpStatus: null,
          latencyMs: elapsedMilliseconds(pinStartedAt),
          providerRequestIdHash: null,
          evidence: [importEvidence],
        });
      }
      const pinEvidence = httpEvidence(
        "filebase_rpc_recursive_pin_list_v1",
        pinResponse,
        pinBounded,
        elapsedMilliseconds(pinStartedAt),
      );
      const pinnedRoots = parseRecursivePinRoots(pinBounded.bytes);
      if (
        pinResponse.status < 200 ||
        pinResponse.status >= 300 ||
        pinnedRoots === null ||
        !roots.every((root) => pinnedRoots.has(root))
      ) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code:
            pinResponse.status >= 500 || pinResponse.status === 429
              ? "transport_unknown"
              : "provider_pin_error",
          failureClass: "outcome_unknown",
          failureStage: "recursive_pin",
          httpStatus: pinResponse.status,
          latencyMs: elapsedMilliseconds(pinStartedAt),
          providerRequestIdHash: pinEvidence.providerRequestIdHash,
          responseBytes: pinBounded.byteCount,
          responseSha256: pinBounded.sha256,
          evidence: [importEvidence, pinEvidence],
        });
      }

      const gatewayStartedAt = Date.now();
      let gatewayResponse: Response;
      let gatewayBounded: Awaited<ReturnType<typeof readBoundedResponse>>;
      try {
        gatewayResponse = await this.#fetch(
          new URL(
            `/ipfs/${encodeURIComponent(roots[0]!)}?format=raw`,
            CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_GATEWAY_ORIGIN,
          ),
          {
            body: null,
            headers: { Accept: "application/vnd.ipld.raw" },
            method: "GET",
            redirect: "manual",
            signal: controller.signal,
          },
        );
        if (gatewayResponse.status >= 300 && gatewayResponse.status < 400) {
          return responseFailure({
            attemptNumber: input.admission.attemptSequence,
            code: "redirect_rejected",
            failureClass: "outcome_unknown",
            failureStage: "gateway_root",
            httpStatus: gatewayResponse.status,
            latencyMs: elapsedMilliseconds(gatewayStartedAt),
            providerRequestIdHash: providerRequestIdHash(gatewayResponse),
            evidence: [importEvidence, pinEvidence],
          });
        }
        gatewayBounded = await readBoundedResponse(
          gatewayResponse,
          MAX_GATEWAY_ROOT_BLOCK_BYTES,
        );
      } catch (error) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code:
            error instanceof BoundedResponseError &&
            error.code === "response_too_large"
              ? "response_too_large"
              : timedOut
                ? "timeout_unknown"
                : "transport_unknown",
          failureClass: "outcome_unknown",
          failureStage: "gateway_root",
          httpStatus: null,
          latencyMs: elapsedMilliseconds(gatewayStartedAt),
          providerRequestIdHash: null,
          evidence: [importEvidence, pinEvidence],
        });
      }
      const gatewayEvidence = httpEvidence(
        "filebase_official_gateway_raw_block_v1",
        gatewayResponse,
        gatewayBounded,
        elapsedMilliseconds(gatewayStartedAt),
      );
      const expectedRootSha256 = Buffer.from(
        CID.parse(roots[0]!).multihash.digest,
      ).toString("hex");
      if (
        gatewayResponse.status !== 200 ||
        gatewayBounded.sha256 !== expectedRootSha256
      ) {
        return responseFailure({
          attemptNumber: input.admission.attemptSequence,
          code:
            gatewayResponse.status >= 500 || gatewayResponse.status === 429
              ? "transport_unknown"
              : "provider_root_mismatch",
          failureClass: "outcome_unknown",
          failureStage: "gateway_root",
          httpStatus: gatewayResponse.status,
          latencyMs: elapsedMilliseconds(gatewayStartedAt),
          providerRequestIdHash: gatewayEvidence.providerRequestIdHash,
          responseBytes: gatewayBounded.byteCount,
          responseSha256: gatewayBounded.sha256,
          evidence: [importEvidence, pinEvidence, gatewayEvidence],
        });
      }
      const sanitizedEvidence: CandidateSourceSnapshotCarImportEvidence = {
        gatewayRoot: gatewayEvidence,
        import: importEvidence,
        recursivePin: pinEvidence,
        schemaVersion: "candidate-source-snapshot-car-provider-evidence-v1",
      };
      const evidenceRequestId =
        importEvidence.providerRequestIdHash ??
        pinEvidence.providerRequestIdHash ??
        gatewayEvidence.providerRequestIdHash;
      return {
        attemptNumber: input.admission.attemptSequence,
        endpointPolicy: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_POLICY,
        failureClass: null,
        failureStage: null,
        httpStatus: response.status,
        latencyMs: elapsedMilliseconds(startedAt),
        observedRoots: [...roots],
        observedRootsSha256: sha256(canonicalJson(roots)),
        outcome: "verified",
        providerImportResult: "expected_root_set_returned",
        providerEvidenceSha256: sha256(canonicalJson(sanitizedEvidence)),
        providerRequestIdHash: evidenceRequestId,
        responseBytes: bounded.byteCount,
        responseSha256: bounded.sha256,
        sanitizedEvidence,
        verifiedRootBlock: new Uint8Array(gatewayBounded.bytes),
      };
    } catch {
      return responseFailure({
        attemptNumber: input.admission.attemptSequence,
        code: timedOut
          ? "timeout_unknown"
          : input.signal?.aborted
            ? "caller_aborted"
            : "transport_unknown",
        failureClass: "outcome_unknown",
        failureStage: "import",
        httpStatus: response?.status ?? null,
        latencyMs: elapsedMilliseconds(startedAt),
        providerRequestIdHash:
          response === null ? null : providerRequestIdHash(response),
      });
    } finally {
      clearTimeout(timeout);
      input.signal?.removeEventListener("abort", parentAbort);
      if (!multipart.destroyed) multipart.destroy();
      await finished(multipart).catch(() => undefined);
      if (response?.body && !response.body.locked) {
        await response.body.cancel().catch(() => undefined);
      }
    }
  }
}
