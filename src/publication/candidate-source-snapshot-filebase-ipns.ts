import { performance } from "node:perf_hooks";

import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import {
  delegatedIpnsEvidenceSchema,
  observeDelegatedIpnsRecord,
} from "./delegated-ipns.js";
import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "./candidate-source-snapshot-demo.js";
import type { EnabledCandidateSourceSnapshotExecutionConfig } from "./candidate-source-snapshot-executor-config.js";
import type {
  CandidateSourceSnapshotIpnsBoundary,
  CandidateSourceSnapshotIpnsDomain,
  CandidateSourceSnapshotIpnsMutationCommand,
  CandidateSourceSnapshotIpnsObservation,
  CandidateSourceSnapshotIpnsRollbackCommand,
} from "./candidate-source-snapshot-ipns-controller.js";

export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN =
  "https://api.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN =
  "https://ipfs.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION =
  "candidate_source_snapshot_filebase_ipns_evidence_v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES = 64 * 1024;
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_GATEWAY_REDIRECTS = 1;

const EMPTY_SHA256 = sha256(Buffer.alloc(0));
const cidSchema = z.union([
  z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const domainSchema = z.enum(["open_data", "query_table"]);
const outcomeSchema = z.enum([
  "accepted",
  "observed",
  "unavailable",
  "http_error",
  "timeout",
  "transport_error",
  "redirect_rejected",
  "response_too_large",
  "malformed_response",
  "identity_mismatch",
  "unexpected_cid",
]);

export const candidateSourceSnapshotFilebaseIpnsReceiptSchema = z
  .strictObject({
    domain: domainSchema,
    endpointType: z.enum([
      "filebase_names_api_v1",
      "filebase_official_ipfs_gateway",
    ]),
    httpStatus: z.number().int().min(100).max(599).nullable(),
    latencyMs: z.number().int().min(0).max(120_000),
    observedAt: z.string().datetime(),
    observedCid: cidSchema.nullable(),
    operation: z.enum(["names_read", "names_update", "public_resolve"]),
    outcome: outcomeSchema,
    providerRequestIdHash: sha256Schema.nullable(),
    receiptSha256: sha256Schema,
    requestCount: z.number().int().min(1).max(6),
    responseBytes: z
      .number()
      .int()
      .min(0)
      .max(CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES),
    responseSha256: sha256Schema,
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    ),
  })
  .superRefine((receipt, context) => {
    if (
      receipt.endpointType === "filebase_names_api_v1" &&
      receipt.operation === "public_resolve"
    ) {
      context.addIssue({
        code: "custom",
        message: "Names API receipt has an invalid operation",
      });
    }
    if (
      receipt.endpointType === "filebase_official_ipfs_gateway" &&
      receipt.operation !== "public_resolve"
    ) {
      context.addIssue({
        code: "custom",
        message: "Gateway receipt has an invalid operation",
      });
    }
    if (
      ["observed", "unexpected_cid"].includes(receipt.outcome) !==
      (receipt.observedCid !== null)
    ) {
      context.addIssue({
        code: "custom",
        message: "Receipt CID and outcome are inconsistent",
      });
    }
    const { receiptSha256, ...identity } = receipt;
    if (canonicalJsonSha256(identity) !== receiptSha256) {
      context.addIssue({
        code: "custom",
        message: "Receipt SHA-256 is invalid",
      });
    }
  });

export type CandidateSourceSnapshotFilebaseIpnsReceipt = z.infer<
  typeof candidateSourceSnapshotFilebaseIpnsReceiptSchema
>;

export const candidateSourceSnapshotDelegatedIpnsReceiptSchema = z
  .strictObject({
    delegatedEvidence: delegatedIpnsEvidenceSchema,
    domain: domainSchema,
    receiptSha256: sha256Schema,
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    ),
  })
  .superRefine((receipt, context) => {
    const { receiptSha256, ...identity } = receipt;
    if (canonicalJsonSha256(identity) !== receiptSha256) {
      context.addIssue({
        code: "custom",
        message: "Delegated receipt SHA-256 is invalid",
      });
    }
  });

export type CandidateSourceSnapshotDelegatedIpnsReceipt = z.infer<
  typeof candidateSourceSnapshotDelegatedIpnsReceiptSchema
>;

export const candidateSourceSnapshotIpnsAggregateEvidenceSchema = z
  .strictObject({
    classification: z.enum([
      "target",
      "prior",
      "split",
      "unavailable",
      "unexpected",
    ]),
    controlReceiptSha256: sha256Schema,
    delegatedReceiptSha256: sha256Schema,
    domain: domainSchema,
    evidenceSha256: sha256Schema,
    gatewayReceiptSha256: sha256Schema,
    observedAt: z.string().datetime(),
    observedCid: cidSchema.nullable(),
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    ),
  })
  .superRefine((evidence, context) => {
    if (
      ["target", "prior", "unexpected"].includes(evidence.classification) !==
      (evidence.observedCid !== null)
    ) {
      context.addIssue({
        code: "custom",
        message: "Aggregate CID and classification are inconsistent",
      });
    }
    const { evidenceSha256, ...identity } = evidence;
    if (canonicalJsonSha256(identity) !== evidenceSha256) {
      context.addIssue({
        code: "custom",
        message: "Aggregate evidence SHA-256 is invalid",
      });
    }
  });

export type CandidateSourceSnapshotIpnsAggregateEvidence = z.infer<
  typeof candidateSourceSnapshotIpnsAggregateEvidenceSchema
>;

export type CandidateSourceSnapshotFilebaseIpnsEvidence =
  | CandidateSourceSnapshotFilebaseIpnsReceipt
  | CandidateSourceSnapshotDelegatedIpnsReceipt
  | CandidateSourceSnapshotIpnsAggregateEvidence;

export interface CandidateSourceSnapshotFilebaseIpnsEvidenceSink {
  record(evidence: CandidateSourceSnapshotFilebaseIpnsEvidence): Promise<void>;
}

export const candidateSourceSnapshotIpnsRequestAdmissionSchema = z.strictObject(
  {
    domain: domainSchema,
    endpointType: z.enum([
      "filebase_names_api_v1",
      "filebase_official_ipfs_gateway",
      "ipfs_delegated_routing_v1",
    ]),
    method: z.enum(["GET", "HEAD", "PUT"]),
    operation: z.enum(["names_read", "names_update", "public_resolve"]),
    requestOrdinal: z.number().int().min(1).max(6),
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    ),
  },
);

export type CandidateSourceSnapshotIpnsRequestAdmission = z.infer<
  typeof candidateSourceSnapshotIpnsRequestAdmissionSchema
>;

export interface CandidateSourceSnapshotFilebaseIpnsRequestGate {
  beforeRequest(
    request: CandidateSourceSnapshotIpnsRequestAdmission,
  ): Promise<void>;
}

type DelegatedObserver = typeof observeDelegatedIpnsRecord;
type RetryDelay = (delayMs: number, signal?: AbortSignal) => Promise<void>;
type Target =
  | CandidateSourceSnapshotDemoPlan["targets"]["openData"]
  | CandidateSourceSnapshotDemoPlan["targets"]["queryTable"];

interface RequestResult {
  latencyMs: number;
  requestCount: number;
  response: Response | null;
  terminalOutcome: "timeout" | "transport_error" | null;
}

interface BoundedResponse {
  bytes: Buffer;
  sha256: string;
}

class ResponseTooLargeError extends Error {}

function elapsed(startedAt: number): number {
  return Math.min(
    120_000,
    Math.max(0, Math.round(performance.now() - startedAt)),
  );
}

function isTimeout(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}

function signalWithTimeout(
  timeoutMs: number,
  parent?: AbortSignal,
): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return parent ? AbortSignal.any([parent, timeout]) : timeout;
}

async function defaultRetryDelay(
  delayMs: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("Operation aborted");
  }
  await new Promise<void>((resolve, reject) => {
    const cleanup = () => signal?.removeEventListener("abort", abort);
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, delayMs);
    const abort = () => {
      clearTimeout(timer);
      cleanup();
      reject(signal?.reason ?? new Error("Operation aborted"));
    };
    signal?.addEventListener("abort", abort, { once: true });
    timer.unref();
  });
}

function isRetryableStatus(status: number): boolean {
  return status === 429 || [500, 502, 503, 504].includes(status);
}

async function requestWithRetries(options: {
  beforeRequest: (requestOrdinal: number) => Promise<void>;
  fetchImpl: typeof fetch;
  init: RequestInit;
  maxRetries: number;
  retryDelay: RetryDelay;
  signal?: AbortSignal;
  timeoutMs: number;
  url: URL;
}): Promise<RequestResult> {
  const startedAt = performance.now();
  let requestCount = 0;
  let finalOutcome: RequestResult["terminalOutcome"] = null;
  for (let attempt = 0; attempt <= options.maxRetries; attempt += 1) {
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Operation aborted");
    }
    requestCount += 1;
    await options.beforeRequest(requestCount);
    try {
      const response = await options.fetchImpl(options.url, {
        ...options.init,
        redirect: "manual",
        signal: signalWithTimeout(options.timeoutMs, options.signal),
      });
      if (isRetryableStatus(response.status) && attempt < options.maxRetries) {
        await response.body?.cancel().catch(() => undefined);
        await options.retryDelay(50 * (attempt + 1), options.signal);
        continue;
      }
      return {
        latencyMs: elapsed(startedAt),
        requestCount,
        response,
        terminalOutcome: null,
      };
    } catch (error) {
      if (options.signal?.aborted) {
        throw options.signal.reason ?? new Error("Operation aborted");
      }
      finalOutcome = isTimeout(error) ? "timeout" : "transport_error";
      if (attempt < options.maxRetries) {
        await options.retryDelay(50 * (attempt + 1), options.signal);
        continue;
      }
    }
  }
  return {
    latencyMs: elapsed(startedAt),
    requestCount,
    response: null,
    terminalOutcome: finalOutcome ?? "transport_error",
  };
}

async function boundedResponse(
  response: Response,
  maximumBytes: number,
  allowEmpty: boolean,
): Promise<BoundedResponse> {
  const contentLength = response.headers.get("content-length");
  if (
    contentLength !== null &&
    (!/^\d+$/.test(contentLength) || Number(contentLength) > maximumBytes)
  ) {
    throw new ResponseTooLargeError("Response exceeds its byte limit");
  }
  if (!response.body) {
    if (allowEmpty) return { bytes: Buffer.alloc(0), sha256: EMPTY_SHA256 };
    throw new TypeError("Response is empty");
  }
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = response.body.getReader();
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw new ResponseTooLargeError("Response exceeds its byte limit");
    }
    chunks.push(next.value);
  }
  if (total === 0 && !allowEmpty) throw new TypeError("Response is empty");
  const bytes = Buffer.concat(chunks, total);
  return { bytes, sha256: sha256(bytes) };
}

function responseRequestIdHash(response: Response): string | null {
  for (const name of ["x-amz-request-id", "x-amz-requestid", "x-request-id"]) {
    const value = response.headers.get(name);
    if (value) return sha256(value);
  }
  return null;
}

function makeReceipt(
  input: Omit<CandidateSourceSnapshotFilebaseIpnsReceipt, "receiptSha256">,
): CandidateSourceSnapshotFilebaseIpnsReceipt {
  return candidateSourceSnapshotFilebaseIpnsReceiptSchema.parse({
    ...input,
    receiptSha256: canonicalJsonSha256(input),
  });
}

const filebaseNameSchema = z.object({
  cid: cidSchema,
  enabled: z.boolean(),
  label: z.string().min(1).max(200),
  network_key: z.string().regex(/^k51[0-9a-z]{59}$/),
});

function targetFor(
  plan: CandidateSourceSnapshotDemoPlan,
  domain: CandidateSourceSnapshotIpnsDomain,
): Target {
  return domain === "open_data"
    ? plan.targets.openData
    : plan.targets.queryTable;
}

function classifyCid(
  cid: string,
  target: Target,
): "observed" | "unexpected_cid" {
  return cid === target.priorCid || cid === target.targetCid
    ? "observed"
    : "unexpected_cid";
}

function exactName(value: unknown, target: Target): string {
  const names = z.array(filebaseNameSchema).max(256).parse(value);
  const related = names.filter(
    (entry) =>
      entry.label === target.ipnsLabel ||
      entry.network_key === target.ipnsNetworkKey,
  );
  if (
    related.length !== 1 ||
    related[0]?.label !== target.ipnsLabel ||
    related[0]?.network_key !== target.ipnsNetworkKey ||
    !related[0]?.enabled
  ) {
    throw new TypeError("Configured candidate IPNS identity is unavailable");
  }
  return related[0].cid;
}

function gatewayCids(response: Response, responseUrl: URL): readonly string[] {
  const values = [
    response.headers.get("x-ipfs-roots"),
    response.headers.get("x-ipfs-path"),
    response.headers.get("location"),
    responseUrl.pathname,
  ];
  const cids = new Set<string>();
  for (const value of values) {
    if (!value) continue;
    for (const match of value.matchAll(
      /Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/g,
    )) {
      const parsed = cidSchema.safeParse(match[0]);
      if (parsed.success) cids.add(parsed.data);
    }
  }
  return [...cids].sort();
}

function validateGatewayRedirect(location: string | null): URL | null {
  if (!location) return null;
  let redirect: URL;
  try {
    redirect = new URL(
      location,
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN,
    );
  } catch {
    return null;
  }
  if (
    redirect.origin !== CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN ||
    redirect.protocol !== "https:" ||
    redirect.username !== "" ||
    redirect.password !== "" ||
    redirect.search !== "" ||
    redirect.hash !== "" ||
    !/^\/ipfs\/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$/.test(
      redirect.pathname,
    )
  ) {
    return null;
  }
  return redirect;
}

function commandMatchesPlan(
  command:
    | CandidateSourceSnapshotIpnsMutationCommand
    | CandidateSourceSnapshotIpnsRollbackCommand,
  plan: CandidateSourceSnapshotDemoPlan,
): Target {
  const target = targetFor(plan, command.domain);
  if (
    command.planId !== plan.planId ||
    command.planSha256 !== plan.planSha256 ||
    command.priorCid !== target.priorCid ||
    command.targetCid !== target.targetCid ||
    (command.action === "rollback" && command.domain !== "open_data")
  ) {
    throw new Error("IPNS command does not match the immutable candidate plan");
  }
  return target;
}

export class CandidateSourceSnapshotFilebaseIpnsAdapter implements CandidateSourceSnapshotIpnsBoundary {
  readonly #apiToken: string;
  readonly #evidenceSink: CandidateSourceSnapshotFilebaseIpnsEvidenceSink;
  readonly #fetch: typeof fetch;
  readonly #observeDelegated: DelegatedObserver;
  readonly #plan: CandidateSourceSnapshotDemoPlan;
  readonly #requestGate: CandidateSourceSnapshotFilebaseIpnsRequestGate;
  readonly #retryDelay: RetryDelay;
  readonly #signal: AbortSignal | undefined;
  readonly #timeoutMs: number;

  constructor(input: {
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    evidenceSink: CandidateSourceSnapshotFilebaseIpnsEvidenceSink;
    fetchImpl: typeof fetch;
    observeDelegated: DelegatedObserver;
    plan: CandidateSourceSnapshotDemoPlan;
    requestGate: CandidateSourceSnapshotFilebaseIpnsRequestGate;
    retryDelay?: RetryDelay;
    signal?: AbortSignal;
  }) {
    const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
    if (
      input.config.enabled !== true ||
      input.config.apiEndpoint !==
        CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN ||
      input.config.planId !== plan.planId ||
      input.config.planSha256 !== plan.planSha256 ||
      input.config.targets.openData.bucket !== plan.targets.openData.bucket ||
      input.config.targets.openData.ipnsLabel !==
        plan.targets.openData.ipnsLabel ||
      input.config.targets.openData.ipnsNetworkKey !==
        plan.targets.openData.ipnsNetworkKey ||
      input.config.targets.openData.priorCid !==
        plan.targets.openData.priorCid ||
      input.config.targets.openData.targetCid !==
        plan.targets.openData.targetCid ||
      input.config.targets.queryTable.bucket !==
        plan.targets.queryTable.bucket ||
      input.config.targets.queryTable.ipnsLabel !==
        plan.targets.queryTable.ipnsLabel ||
      input.config.targets.queryTable.ipnsNetworkKey !==
        plan.targets.queryTable.ipnsNetworkKey ||
      input.config.targets.queryTable.priorCid !==
        plan.targets.queryTable.priorCid ||
      input.config.targets.queryTable.targetCid !==
        plan.targets.queryTable.targetCid ||
      input.config.apiToken.trim() === ""
    ) {
      throw new Error("Filebase IPNS adapter configuration is not exact");
    }
    this.#apiToken = input.config.apiToken;
    this.#evidenceSink = input.evidenceSink;
    this.#fetch = input.fetchImpl;
    this.#observeDelegated = input.observeDelegated;
    this.#plan = plan;
    this.#requestGate = input.requestGate;
    this.#retryDelay = input.retryDelay ?? defaultRetryDelay;
    this.#signal = input.signal;
    this.#timeoutMs = input.config.limits.requestTimeoutMs;
  }

  async readControlPlane(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<CandidateSourceSnapshotFilebaseIpnsReceipt> {
    const target = targetFor(this.#plan, domain);
    const observedAt = new Date().toISOString();
    const result = await requestWithRetries({
      beforeRequest: async (requestOrdinal) =>
        this.#requestGate.beforeRequest(
          candidateSourceSnapshotIpnsRequestAdmissionSchema.parse({
            domain,
            endpointType: "filebase_names_api_v1",
            method: "GET",
            operation: "names_read",
            requestOrdinal,
            schemaVersion:
              CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
          }),
        ),
      fetchImpl: this.#fetch,
      init: {
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${this.#apiToken}`,
        },
        method: "GET",
      },
      // Resolver retries are separate durable recovery cycles. Keeping one
      // physical request per resolver receipt preserves exact request/evidence
      // correspondence across a crash.
      maxRetries: 0,
      retryDelay: this.#retryDelay,
      ...(this.#signal ? { signal: this.#signal } : {}),
      timeoutMs: this.#timeoutMs,
      url: new URL(
        "/v1/names",
        CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN,
      ),
    });
    if (!result.response) {
      return makeReceipt({
        domain,
        endpointType: "filebase_names_api_v1",
        httpStatus: null,
        latencyMs: result.latencyMs,
        observedAt,
        observedCid: null,
        operation: "names_read",
        outcome: result.terminalOutcome ?? "transport_error",
        providerRequestIdHash: null,
        requestCount: result.requestCount,
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
        schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
      });
    }
    const response = result.response;
    const base = {
      domain,
      endpointType: "filebase_names_api_v1" as const,
      httpStatus: response.status,
      latencyMs: result.latencyMs,
      observedAt,
      operation: "names_read" as const,
      providerRequestIdHash: responseRequestIdHash(response),
      requestCount: result.requestCount,
      schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    };
    if (response.status >= 300 && response.status < 400) {
      await response.body?.cancel().catch(() => undefined);
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: "redirect_rejected",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: response.status === 404 ? "unavailable" : "http_error",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
    let bounded: BoundedResponse;
    try {
      bounded = await boundedResponse(
        response,
        CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES,
        false,
      );
    } catch (error) {
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome:
          error instanceof ResponseTooLargeError
            ? "response_too_large"
            : "malformed_response",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
    let value: unknown;
    try {
      value = JSON.parse(bounded.bytes.toString("utf8"));
    } catch {
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: "malformed_response",
        responseBytes: bounded.bytes.byteLength,
        responseSha256: bounded.sha256,
      });
    }
    let observedCid: string;
    try {
      observedCid = exactName(value, target);
    } catch {
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: "identity_mismatch",
        responseBytes: bounded.bytes.byteLength,
        responseSha256: bounded.sha256,
      });
    }
    return makeReceipt({
      ...base,
      observedCid,
      outcome: classifyCid(observedCid, target),
      responseBytes: bounded.bytes.byteLength,
      responseSha256: bounded.sha256,
    });
  }

  async updateControlPlane(
    domain: CandidateSourceSnapshotIpnsDomain,
    requestedCid: string,
  ): Promise<CandidateSourceSnapshotFilebaseIpnsReceipt> {
    const target = targetFor(this.#plan, domain);
    const cid = cidSchema.parse(requestedCid);
    if (cid !== target.priorCid && cid !== target.targetCid) {
      throw new Error(
        "IPNS update CID is outside the immutable candidate plan",
      );
    }
    const observedAt = new Date().toISOString();
    const result = await requestWithRetries({
      beforeRequest: async (requestOrdinal) =>
        this.#requestGate.beforeRequest(
          candidateSourceSnapshotIpnsRequestAdmissionSchema.parse({
            domain,
            endpointType: "filebase_names_api_v1",
            method: "PUT",
            operation: "names_update",
            requestOrdinal,
            schemaVersion:
              CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
          }),
        ),
      fetchImpl: this.#fetch,
      init: {
        body: JSON.stringify({ cid, enabled: true }),
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${this.#apiToken}`,
          "Content-Type": "application/json",
        },
        method: "PUT",
      },
      // A Names update is an irreversible external effect. A timeout or 5xx
      // leaves its result ambiguous, so the closed controller must reconcile
      // the existing intent before any separately authorized replay. Never let
      // the HTTP helper issue an unjournaled blind retry.
      maxRetries: 0,
      retryDelay: this.#retryDelay,
      ...(this.#signal ? { signal: this.#signal } : {}),
      timeoutMs: this.#timeoutMs,
      url: new URL(
        `/v1/names/${encodeURIComponent(target.ipnsLabel)}`,
        CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN,
      ),
    });
    if (!result.response) {
      return makeReceipt({
        domain,
        endpointType: "filebase_names_api_v1",
        httpStatus: null,
        latencyMs: result.latencyMs,
        observedAt,
        observedCid: null,
        operation: "names_update",
        outcome: result.terminalOutcome ?? "transport_error",
        providerRequestIdHash: null,
        requestCount: result.requestCount,
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
        schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
      });
    }
    const response = result.response;
    const base = {
      domain,
      endpointType: "filebase_names_api_v1" as const,
      httpStatus: response.status,
      latencyMs: result.latencyMs,
      observedAt,
      observedCid: null,
      operation: "names_update" as const,
      providerRequestIdHash: responseRequestIdHash(response),
      requestCount: result.requestCount,
      schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    };
    if (response.status >= 300 && response.status < 400) {
      await response.body?.cancel().catch(() => undefined);
      return makeReceipt({
        ...base,
        outcome: "redirect_rejected",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      return makeReceipt({
        ...base,
        outcome: "http_error",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
    try {
      const bounded = await boundedResponse(
        response,
        CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES,
        true,
      );
      return makeReceipt({
        ...base,
        outcome: "accepted",
        responseBytes: bounded.bytes.byteLength,
        responseSha256: bounded.sha256,
      });
    } catch (error) {
      return makeReceipt({
        ...base,
        outcome:
          error instanceof ResponseTooLargeError
            ? "response_too_large"
            : "malformed_response",
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
      });
    }
  }

  async observeOfficialGateway(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<CandidateSourceSnapshotFilebaseIpnsReceipt> {
    const target = targetFor(this.#plan, domain);
    const observedAt = new Date().toISOString();
    const startedAt = performance.now();
    const current = new URL(
      `/ipns/${encodeURIComponent(target.ipnsNetworkKey)}`,
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN,
    );
    const result = await requestWithRetries({
      beforeRequest: async (requestOrdinal) =>
        this.#requestGate.beforeRequest(
          candidateSourceSnapshotIpnsRequestAdmissionSchema.parse({
            domain,
            endpointType: "filebase_official_ipfs_gateway",
            method: "HEAD",
            operation: "public_resolve",
            requestOrdinal,
            schemaVersion:
              CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
          }),
        ),
      fetchImpl: this.#fetch,
      init: {
        headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
        method: "HEAD",
      },
      maxRetries: 0,
      retryDelay: this.#retryDelay,
      ...(this.#signal ? { signal: this.#signal } : {}),
      timeoutMs: this.#timeoutMs,
      url: current,
    });
    if (!result.response) {
      return makeReceipt({
        domain,
        endpointType: "filebase_official_ipfs_gateway",
        httpStatus: null,
        latencyMs: elapsed(startedAt),
        observedAt,
        observedCid: null,
        operation: "public_resolve",
        outcome: result.terminalOutcome ?? "transport_error",
        providerRequestIdHash: null,
        requestCount: result.requestCount,
        responseBytes: 0,
        responseSha256: EMPTY_SHA256,
        schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
      });
    }
    const response = result.response;
    const base = {
      domain,
      endpointType: "filebase_official_ipfs_gateway" as const,
      httpStatus: response.status,
      latencyMs: elapsed(startedAt),
      observedAt,
      operation: "public_resolve" as const,
      providerRequestIdHash: responseRequestIdHash(response),
      requestCount: result.requestCount,
      responseBytes: 0,
      responseSha256: EMPTY_SHA256,
      schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    };
    if (response.status >= 300 && response.status < 400) {
      const redirect = validateGatewayRedirect(
        response.headers.get("location"),
      );
      const cids = gatewayCids(response, current);
      await response.body?.cancel().catch(() => undefined);
      if (!redirect || cids.length !== 1) {
        return makeReceipt({
          ...base,
          observedCid: null,
          outcome: "redirect_rejected",
        });
      }
      return makeReceipt({
        ...base,
        observedCid: cids[0]!,
        outcome: classifyCid(cids[0]!, target),
      });
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: response.status === 404 ? "unavailable" : "http_error",
      });
    }
    const cids = gatewayCids(response, current);
    await response.body?.cancel().catch(() => undefined);
    if (cids.length !== 1) {
      return makeReceipt({
        ...base,
        observedCid: null,
        outcome: "malformed_response",
      });
    }
    return makeReceipt({
      ...base,
      observedCid: cids[0]!,
      outcome: classifyCid(cids[0]!, target),
    });
  }

  async observeIdentity(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<CandidateSourceSnapshotIpnsAggregateEvidence> {
    const target = targetFor(this.#plan, domain);
    const control = await this.readControlPlane(domain);
    await this.#evidenceSink.record(control);
    const gateway = await this.observeOfficialGateway(domain);
    await this.#evidenceSink.record(gateway);
    let delegatedRequestCount = 0;
    const admittedDelegatedFetch: typeof fetch = async (input, init) => {
      delegatedRequestCount += 1;
      await this.#requestGate.beforeRequest(
        candidateSourceSnapshotIpnsRequestAdmissionSchema.parse({
          domain,
          endpointType: "ipfs_delegated_routing_v1",
          method: "GET",
          operation: "public_resolve",
          requestOrdinal: delegatedRequestCount,
          schemaVersion:
            CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
        }),
      );
      return this.#fetch(input, init);
    };
    const delegatedEvidence = await this.#observeDelegated({
      expectedPriorCid: target.priorCid,
      expectedTargetCid: target.targetCid,
      fetchImpl: admittedDelegatedFetch,
      // As with the other two resolvers, retry is represented by a new
      // durable recovery cycle rather than hidden inside one receipt.
      maxRetries: 0,
      networkKey: target.ipnsNetworkKey,
      retryDelay: this.#retryDelay,
      ...(this.#signal ? { signal: this.#signal } : {}),
      timeoutMs: this.#timeoutMs,
    });
    const delegatedReceiptInput = {
      delegatedEvidence,
      domain,
      schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    };
    const delegated = candidateSourceSnapshotDelegatedIpnsReceiptSchema.parse({
      ...delegatedReceiptInput,
      receiptSha256: canonicalJsonSha256(delegatedReceiptInput),
    });
    await this.#evidenceSink.record(delegated);

    const observed = [
      control.observedCid,
      gateway.observedCid,
      delegatedEvidence.observedCid,
    ];
    const thirdCids = observed.filter(
      (cid): cid is string =>
        cid !== null && cid !== target.priorCid && cid !== target.targetCid,
    );
    let classification: CandidateSourceSnapshotIpnsObservation["classification"];
    let observedCid: string | null;
    if (thirdCids.length > 0) {
      classification = "unexpected";
      observedCid = [...new Set(thirdCids)].sort()[0]!;
    } else if (
      control.outcome !== "observed" ||
      gateway.outcome !== "observed" ||
      delegatedEvidence.outcome !== "validated" ||
      !["valid_prior", "valid_target"].includes(
        delegatedEvidence.validationResult,
      ) ||
      observed.some((cid) => cid === null)
    ) {
      classification = "unavailable";
      observedCid = null;
    } else if (observed.every((cid) => cid === target.targetCid)) {
      classification = "target";
      observedCid = target.targetCid;
    } else if (observed.every((cid) => cid === target.priorCid)) {
      classification = "prior";
      observedCid = target.priorCid;
    } else {
      classification = "split";
      observedCid = null;
    }
    const aggregateInput = {
      classification,
      controlReceiptSha256: control.receiptSha256,
      delegatedReceiptSha256: delegated.receiptSha256,
      domain,
      gatewayReceiptSha256: gateway.receiptSha256,
      observedAt: new Date().toISOString(),
      observedCid,
      schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_IPNS_EVIDENCE_VERSION,
    };
    const aggregate = candidateSourceSnapshotIpnsAggregateEvidenceSchema.parse({
      ...aggregateInput,
      evidenceSha256: canonicalJsonSha256(aggregateInput),
    });
    await this.#evidenceSink.record(aggregate);
    return aggregate;
  }

  async mutateAndObserve(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<CandidateSourceSnapshotIpnsObservation> {
    commandMatchesPlan(command, this.#plan);
    const receipt = await this.updateControlPlane(
      command.domain,
      command.targetCid,
    );
    await this.#evidenceSink.record(receipt);
    const aggregate = await this.observeIdentity(command.domain);
    return {
      classification: aggregate.classification,
      observedCid: aggregate.observedCid,
    };
  }

  async rollbackAndObserve(
    command: CandidateSourceSnapshotIpnsRollbackCommand,
  ): Promise<CandidateSourceSnapshotIpnsObservation> {
    commandMatchesPlan(command, this.#plan);
    const receipt = await this.updateControlPlane(
      command.domain,
      command.priorCid,
    );
    await this.#evidenceSink.record(receipt);
    const aggregate = await this.observeIdentity(command.domain);
    return {
      classification: aggregate.classification,
      observedCid: aggregate.observedCid,
    };
  }
}
