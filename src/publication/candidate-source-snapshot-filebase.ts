import { createHash, randomUUID } from "node:crypto";
import { constants, createReadStream } from "node:fs";
import { lstat, open, realpath, stat } from "node:fs/promises";
import { Agent as HttpsAgent } from "node:https";
import path from "node:path";
import type { Readable } from "node:stream";
import { finished } from "node:stream/promises";

import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  type HeadObjectCommandOutput,
  type PutObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import {
  assertCandidateSourceSnapshotObjectNamespace,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";
import type {
  CandidateSourceSnapshotExecutionConfig,
  EnabledCandidateSourceSnapshotExecutionConfig,
} from "./candidate-source-snapshot-executor-config.js";
import {
  CandidateSourceSnapshotUploadError,
  type CandidateSourceSnapshotSocketStage,
  type CandidateSourceSnapshotTransportFailureClass,
  type CandidateSourceSnapshotTransportFailureStage,
  type CandidateSourceSnapshotUploadTransport,
} from "./candidate-source-snapshot-upload.js";
import { calculateIpfsCid, calculateIpfsFileCid } from "./ipfs-cid.js";

const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const BUFFER_BODY_MAX_BYTES = 1024 * 1024;

export interface CandidateSourceSnapshotUploadBodyLease {
  body: Buffer | Readable;
  contentLength: number;
  contentType: "application/json" | "application/vnd.apache.parquet";
  release(): Promise<void>;
}

export interface CandidateSourceSnapshotLocalObjectSource {
  /**
   * Opens one immutable object for a local CAR build without performing a
   * redundant only-hash pass. The CAR builder must verify the streamed byte
   * count, SHA-256, and UnixFS root before it finalizes any output.
   */
  openCarSource?(
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<{
    body: Readable;
    contentLength: number;
    release(): Promise<void>;
  }>;
  /** A fresh, one-attempt body. Implementations must never reuse a consumed stream. */
  openVerifiedUploadBody?(
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadBodyLease>;
  openVerifiedStream(object: CandidateSourceSnapshotUploadObject): Promise<{
    body: Readable;
    contentLength: number;
    contentType: "application/json" | "application/vnd.apache.parquet";
  }>;
  verify(object: CandidateSourceSnapshotUploadObject): Promise<void>;
}

function contained(root: string, candidate: string): boolean {
  return candidate === root || candidate.startsWith(`${root}${path.sep}`);
}

async function canonicalRoot(value: string, label: string): Promise<string> {
  const resolved = path.resolve(value);
  if (resolved === path.parse(resolved).root) {
    throw new Error(`${label} must not be a filesystem root`);
  }
  return await realpath(resolved);
}

async function streamedSha256(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

/**
 * Resolves only plan-derived object keys beneath three already-materialized,
 * immutable roots. No environment value or remote object key can choose an
 * arbitrary local path.
 */
export class BoundCandidateSourceSnapshotLocalObjectSource implements CandidateSourceSnapshotLocalObjectSource {
  readonly #controlRoot: string;
  readonly #plan: CandidateSourceSnapshotDemoPlan;
  readonly #planArtifactPath: string;
  readonly #planArtifactRoot: string;
  readonly #sourceRoot: string;

  private constructor(input: {
    controlRoot: string;
    plan: CandidateSourceSnapshotDemoPlan;
    planArtifactPath: string;
    planArtifactRoot: string;
    sourceRoot: string;
  }) {
    this.#controlRoot = input.controlRoot;
    this.#plan = input.plan;
    this.#planArtifactPath = input.planArtifactPath;
    this.#planArtifactRoot = input.planArtifactRoot;
    this.#sourceRoot = input.sourceRoot;
  }

  static async create(input: {
    controlRoot: string;
    plan: CandidateSourceSnapshotDemoPlan;
    planArtifactPath: string;
    sourcePlanPath: string;
  }): Promise<BoundCandidateSourceSnapshotLocalObjectSource> {
    const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
    const [controlRoot, sourcePlanPath, planArtifactPath] = await Promise.all([
      canonicalRoot(input.controlRoot, "Candidate control root"),
      realpath(path.resolve(input.sourcePlanPath)),
      realpath(path.resolve(input.planArtifactPath)),
    ]);
    const sourceRoot = await canonicalRoot(
      path.dirname(sourcePlanPath),
      "Source publication root",
    );
    const planArtifactRoot = await canonicalRoot(
      path.dirname(planArtifactPath),
      "Candidate plan artifact root",
    );
    return new BoundCandidateSourceSnapshotLocalObjectSource({
      controlRoot,
      plan,
      planArtifactPath,
      planArtifactRoot,
      sourceRoot,
    });
  }

  async #resolve(
    objectValue: CandidateSourceSnapshotUploadObject,
  ): Promise<string> {
    const object = objectValue;
    assertCandidateSourceSnapshotObjectNamespace(this.#plan, object);
    let root: string;
    let candidate: string;
    const planObjectKey = `${this.#plan.targets.controlPrefix}candidate-source-snapshot-plan.json`;
    if (object.remoteObjectKey === planObjectKey) {
      if (object.logicalObjectKey !== "candidate-source-snapshot-plan.json") {
        throw new Error("Candidate plan artifact identity is inconsistent");
      }
      root = this.#planArtifactRoot;
      candidate = this.#planArtifactPath;
    } else if (
      object.remoteObjectKey.startsWith(this.#plan.targets.controlPrefix) ||
      object.domain === "query_table" ||
      (object.domain === "open_data" &&
        ["coverage.json", "provenance.json"].includes(object.logicalObjectKey))
    ) {
      root = this.#controlRoot;
      candidate = path.resolve(root, ...object.remoteObjectKey.split("/"));
    } else {
      root = path.join(this.#sourceRoot, "open-data");
      candidate = path.resolve(root, ...object.logicalObjectKey.split("/"));
    }
    if (!contained(root, candidate)) {
      throw new Error("Candidate local object escaped its immutable root");
    }
    const linkState = await lstat(candidate);
    if (linkState.isSymbolicLink() || !linkState.isFile()) {
      throw new Error(
        "Candidate local object is not an immutable regular file",
      );
    }
    const actual = await realpath(candidate);
    if (!contained(root, actual)) {
      throw new Error(
        "Candidate local object realpath escaped its immutable root",
      );
    }
    return actual;
  }

  async verify(object: CandidateSourceSnapshotUploadObject): Promise<void> {
    const filePath = await this.#resolve(object);
    const state = await stat(filePath);
    if (
      !state.isFile() ||
      state.size !== object.byteSize ||
      state.size > this.#plan.limits.maxObjectBytes
    ) {
      throw new Error("Candidate local object byte binding is invalid");
    }
    const [actualSha256, actualCid] = await Promise.all([
      streamedSha256(filePath),
      calculateIpfsFileCid(filePath),
    ]);
    if (actualSha256 !== object.sha256 || actualCid !== object.expectedCid) {
      throw new Error("Candidate local object hash binding is invalid");
    }
  }

  async openVerifiedStream(object: CandidateSourceSnapshotUploadObject) {
    await this.verify(object);
    const filePath = await this.#resolve(object);
    // O_NOFOLLOW verifies the final path component immediately before opening
    // the upload stream. The provider CID check is the final proof of the exact
    // bytes accepted by Filebase.
    const handle = await open(
      filePath,
      constants.O_RDONLY | constants.O_NOFOLLOW,
    );
    const state = await handle.stat().catch(async (error: unknown) => {
      await handle.close();
      throw error;
    });
    if (!state.isFile() || state.size !== object.byteSize) {
      await handle.close();
      throw new Error("Candidate local object changed before upload");
    }
    const body = handle.createReadStream({ autoClose: true, start: 0 });
    return {
      body,
      contentLength: state.size,
      contentType:
        object.domain === "query_table"
          ? ("application/vnd.apache.parquet" as const)
          : ("application/json" as const),
    };
  }

  async openCarSource(object: CandidateSourceSnapshotUploadObject): Promise<{
    body: Readable;
    contentLength: number;
    release(): Promise<void>;
  }> {
    const filePath = await this.#resolve(object);
    const handle = await open(
      filePath,
      constants.O_RDONLY | constants.O_NOFOLLOW,
    );
    const state = await handle.stat().catch(async (error: unknown) => {
      await handle.close();
      throw error;
    });
    if (
      !state.isFile() ||
      state.size !== object.byteSize ||
      state.size > this.#plan.limits.maxObjectBytes
    ) {
      await handle.close();
      throw new Error("Candidate CAR source byte binding is invalid");
    }
    const body = handle.createReadStream({ autoClose: true, start: 0 });
    let released = false;
    return {
      body,
      contentLength: state.size,
      release: async () => {
        if (released) return;
        released = true;
        if (!body.destroyed) body.destroy();
        await finished(body).catch(() => undefined);
      },
    };
  }

  async openVerifiedUploadBody(
    object: CandidateSourceSnapshotUploadObject,
  ): Promise<CandidateSourceSnapshotUploadBodyLease> {
    await this.verify(object);
    const filePath = await this.#resolve(object);
    const handle = await open(
      filePath,
      constants.O_RDONLY | constants.O_NOFOLLOW,
    );
    const state = await handle.stat().catch(async (error: unknown) => {
      await handle.close();
      throw error;
    });
    if (!state.isFile() || state.size !== object.byteSize) {
      await handle.close();
      throw new Error("Candidate local object changed before upload");
    }
    const contentType =
      object.domain === "query_table"
        ? ("application/vnd.apache.parquet" as const)
        : ("application/json" as const);
    if (state.size <= BUFFER_BODY_MAX_BYTES) {
      try {
        const body = await handle.readFile();
        if (
          body.byteLength !== object.byteSize ||
          sha256(body) !== object.sha256 ||
          (await calculateIpfsCid(body)) !== object.expectedCid
        ) {
          throw new Error(
            "Candidate buffered upload body changed before upload",
          );
        }
        return {
          body,
          contentLength: state.size,
          contentType,
          release: async () => undefined,
        };
      } finally {
        await handle.close();
      }
    }
    const body = handle.createReadStream({ autoClose: true, start: 0 });
    let released = false;
    return {
      body,
      contentLength: state.size,
      contentType,
      release: async () => {
        if (released) return;
        released = true;
        if (!body.destroyed) body.destroy();
        await finished(body).catch(() => undefined);
      },
    };
  }
}

export interface CandidateSourceSnapshotS3CommandResult<Output> {
  output: Output;
  responseHeaders: Readonly<Record<string, string>>;
}

export interface CandidateSourceSnapshotS3CommandExecutor {
  setMaxSockets?(maxSockets: CandidateSourceSnapshotSocketStage): void;
  send(
    command: HeadObjectCommand,
    signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<HeadObjectCommandOutput>>;
  send(
    command: PutObjectCommand,
    signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<PutObjectCommandOutput>>;
}

export interface CandidateSourceSnapshotTransportLimits {
  connectionTimeoutMs: number;
  maxSockets: number;
  requestTimeoutMs: number;
  socketTimeoutMs: number;
}

function assertSocketStageTransition(
  current: number,
  next: CandidateSourceSnapshotSocketStage,
): void {
  if (next === current) return;
  if ((current === 4 && next === 8) || (current === 8 && next === 16)) return;
  throw new Error(
    "Candidate socket limit must follow the closed 4-to-8-to-16 promotion",
  );
}

export class CandidateSourceSnapshotFilebaseTransportError extends CandidateSourceSnapshotUploadError {
  readonly failureClass: CandidateSourceSnapshotTransportFailureClass;
  readonly providerRequestIdHash: string | null;
  readonly stage: CandidateSourceSnapshotTransportFailureStage;

  constructor(input: {
    failureClass: CandidateSourceSnapshotTransportFailureClass;
    outcome: CandidateSourceSnapshotUploadError["outcome"];
    providerRequestIdHash?: string | null;
    stage: CandidateSourceSnapshotTransportFailureStage;
  }) {
    super(input.outcome, undefined, {
      failureClass: input.failureClass,
      providerRequestIdHash: input.providerRequestIdHash ?? null,
      stage: input.stage,
    });
    this.name = "CandidateSourceSnapshotFilebaseTransportError";
    this.failureClass = input.failureClass;
    this.providerRequestIdHash = input.providerRequestIdHash ?? null;
    this.stage = input.stage;
  }
}

function boundedInteger(
  value: number,
  label: string,
  minimum: number,
  maximum: number,
): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${label} is outside the closed transport limit`);
  }
  return value;
}

export function resolveCandidateSourceSnapshotTransportLimits(
  config: EnabledCandidateSourceSnapshotExecutionConfig,
  override?: CandidateSourceSnapshotTransportLimits,
): CandidateSourceSnapshotTransportLimits {
  const requestTimeoutMs = boundedInteger(
    override?.requestTimeoutMs ?? config.limits.requestTimeoutMs,
    "Candidate request timeout",
    500,
    60_000,
  );
  const connectionTimeoutMs = boundedInteger(
    override?.connectionTimeoutMs ??
      Math.max(100, Math.floor(requestTimeoutMs / 4)),
    "Candidate connection timeout",
    100,
    59_998,
  );
  const socketTimeoutMs = boundedInteger(
    override?.socketTimeoutMs ??
      Math.max(200, Math.floor((requestTimeoutMs * 3) / 4)),
    "Candidate socket timeout",
    200,
    59_999,
  );
  const maxSockets = boundedInteger(
    override?.maxSockets ?? Math.min(config.limits.maxConcurrency, 16),
    "Candidate socket limit",
    1,
    16,
  );
  if (
    connectionTimeoutMs >= socketTimeoutMs ||
    socketTimeoutMs >= requestTimeoutMs
  ) {
    throw new Error(
      "Candidate connection, socket, and request timeouts must be strictly ordered",
    );
  }
  return Object.freeze({
    connectionTimeoutMs,
    maxSockets,
    requestTimeoutMs,
    socketTimeoutMs,
  });
}

function deadlineSignal(
  parent: AbortSignal | undefined,
  timeoutMs: number,
): { cleanup(): void; signal: AbortSignal } {
  const controller = new AbortController();
  const abortFromParent = (): void => controller.abort(parent?.reason);
  if (parent?.aborted) abortFromParent();
  else parent?.addEventListener("abort", abortFromParent, { once: true });
  const timer = setTimeout(() => {
    const error = Object.assign(
      new Error("Candidate transport deadline exceeded"),
      {
        code: "ETIMEDOUT",
        name: "TimeoutError",
      },
    );
    controller.abort(error);
  }, timeoutMs);
  timer.unref?.();
  return {
    cleanup: () => {
      clearTimeout(timer);
      parent?.removeEventListener("abort", abortFromParent);
    },
    signal: controller.signal,
  };
}

export class AwsCandidateSourceSnapshotS3CommandExecutor implements CandidateSourceSnapshotS3CommandExecutor {
  readonly #client: S3Client;
  readonly #httpsAgent: HttpsAgent;
  readonly #transportLimits: CandidateSourceSnapshotTransportLimits;
  #closed = false;

  constructor(
    config: EnabledCandidateSourceSnapshotExecutionConfig,
    transportLimits?: CandidateSourceSnapshotTransportLimits,
  ) {
    if (!config.enabled || config.s3Endpoint !== "https://s3.filebase.io") {
      throw new Error("Candidate Filebase S3 execution is not enabled");
    }
    this.#transportLimits = resolveCandidateSourceSnapshotTransportLimits(
      config,
      transportLimits,
    );
    this.#httpsAgent = new HttpsAgent({
      keepAlive: true,
      maxSockets: this.#transportLimits.maxSockets,
    });
    this.#client = new S3Client({
      credentials: {
        accessKeyId: config.s3AccessKeyId,
        secretAccessKey: config.s3SecretAccessKey,
      },
      endpoint: config.s3Endpoint,
      // Smithy's default >=2 MiB Expect middleware replaces a configured agent
      // with an unbounded one. Disable it so every object remains subject to the
      // continuation's explicit keep-alive/maxSockets transport ceiling.
      expectContinueHeader: false,
      forcePathStyle: true,
      maxAttempts: 1,
      region: "auto",
      requestHandler: new NodeHttpHandler({
        connectionTimeout: this.#transportLimits.connectionTimeoutMs,
        httpsAgent: this.#httpsAgent,
        requestTimeout: this.#transportLimits.requestTimeoutMs,
        socketTimeout: this.#transportLimits.socketTimeoutMs,
        throwOnRequestTimeout: true,
      }),
    });
  }

  get transportLimits(): Readonly<CandidateSourceSnapshotTransportLimits> {
    return Object.freeze({
      ...this.#transportLimits,
      maxSockets: this.#httpsAgent.maxSockets,
    });
  }

  setMaxSockets(maxSockets: CandidateSourceSnapshotSocketStage): void {
    assertSocketStageTransition(this.#httpsAgent.maxSockets, maxSockets);
    this.#httpsAgent.maxSockets = maxSockets;
  }

  async send(
    command: HeadObjectCommand | PutObjectCommand,
    signal?: AbortSignal,
  ): Promise<
    CandidateSourceSnapshotS3CommandResult<
      HeadObjectCommandOutput | PutObjectCommandOutput
    >
  > {
    let responseHeaders: Record<string, string> = {};
    const middlewareName = `captureCandidateSourceSnapshot${randomUUID().replaceAll("-", "")}`;
    type MiddlewareResult = { response?: unknown } & Record<string, unknown>;
    type Next = (argumentsValue: unknown) => Promise<MiddlewareResult>;
    const middlewareStack = command.middlewareStack as unknown as {
      add(
        middleware: (
          next: Next,
        ) => (value: unknown) => Promise<MiddlewareResult>,
        options: {
          name: string;
          priority: "low";
          step: "deserialize";
        },
      ): void;
    };
    middlewareStack.add(
      (next: Next) => async (argumentsValue: unknown) => {
        const result = await next(argumentsValue);
        const response = result.response as
          { headers?: Record<string, string> } | undefined;
        responseHeaders = Object.fromEntries(
          Object.entries(response?.headers ?? {}).map(([name, value]) => [
            name.toLowerCase(),
            String(value),
          ]),
        );
        return result;
      },
      { name: middlewareName, priority: "low", step: "deserialize" },
    );
    const deadline = deadlineSignal(
      signal,
      this.#transportLimits.requestTimeoutMs,
    );
    try {
      const output = await this.#client.send(command, {
        abortSignal: deadline.signal,
      });
      if ((output.$metadata.attempts ?? 1) !== 1) {
        throw new Error("S3 client performed an unjournaled internal retry");
      }
      return { output, responseHeaders };
    } finally {
      deadline.cleanup();
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#client.destroy();
  }
}

function requestIdHash(
  output: HeadObjectCommandOutput | PutObjectCommandOutput,
  headers: Readonly<Record<string, string>>,
): string | null {
  const requestId =
    output.$metadata.requestId ??
    headers["x-amz-request-id"] ??
    headers["x-amz-id-2"];
  return requestId ? sha256(requestId) : null;
}

function responseByteCount(headers: Readonly<Record<string, string>>): number {
  const value = Number(headers["content-length"] ?? 0);
  return Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

function providerCid(
  output: HeadObjectCommandOutput | PutObjectCommandOutput,
  headers: Readonly<Record<string, string>>,
): string | null {
  const metadata = "Metadata" in output ? output.Metadata : undefined;
  const value =
    headers["x-amz-meta-cid"] ??
    metadata?.cid ??
    metadata?.["oracle-cid"] ??
    null;
  return typeof value === "string" ? value : null;
}

interface SanitizedTransportErrorEvidence {
  codes: ReadonlySet<string>;
  names: ReadonlySet<string>;
  providerRequestIdHash: string | null;
  statuses: ReadonlySet<number>;
}

function sanitizedTransportErrorEvidence(
  error: unknown,
): SanitizedTransportErrorEvidence {
  const codes = new Set<string>();
  const names = new Set<string>();
  const statuses = new Set<number>();
  let providerRequestIdHash: string | null = null;
  const seen = new Set<object>();
  let current: unknown = error;
  for (
    let depth = 0;
    depth < 6 && current && typeof current === "object";
    depth += 1
  ) {
    if (seen.has(current)) break;
    seen.add(current);
    const value = current as {
      $metadata?: {
        extendedRequestId?: unknown;
        httpStatusCode?: unknown;
        requestId?: unknown;
      };
      $response?: { headers?: Record<string, unknown> };
      cause?: unknown;
      code?: unknown;
      name?: unknown;
    };
    if (typeof value.name === "string") names.add(value.name);
    if (typeof value.code === "string") codes.add(value.code);
    if (
      typeof value.$metadata?.httpStatusCode === "number" &&
      Number.isInteger(value.$metadata.httpStatusCode)
    ) {
      statuses.add(value.$metadata.httpStatusCode);
    }
    if (providerRequestIdHash === null) {
      const headers = value.$response?.headers;
      const candidate =
        value.$metadata?.requestId ??
        value.$metadata?.extendedRequestId ??
        headers?.["x-amz-request-id"] ??
        headers?.["x-amz-id-2"];
      if (typeof candidate === "string" && candidate.length > 0) {
        providerRequestIdHash = sha256(candidate);
      }
    }
    current = value.cause;
  }
  return { codes, names, providerRequestIdHash, statuses };
}

function classifyTransportError(
  error: unknown,
  operation: "head_object" | "put_object",
): CandidateSourceSnapshotFilebaseTransportError {
  if (error instanceof CandidateSourceSnapshotFilebaseTransportError) {
    return error;
  }
  const evidence = sanitizedTransportErrorEvidence(error);
  const stageForRequest =
    operation === "head_object"
      ? ("head_object_request" as const)
      : ("put_object_streaming_request" as const);
  if (error instanceof CandidateSourceSnapshotUploadError) {
    return new CandidateSourceSnapshotFilebaseTransportError({
      failureClass: error.evidence.failureClass,
      outcome: error.outcome,
      providerRequestIdHash:
        error.evidence.providerRequestIdHash ?? evidence.providerRequestIdHash,
      stage:
        error.evidence.stage === "unknown"
          ? stageForRequest
          : error.evidence.stage,
    });
  }
  const streamDisconnected =
    evidence.codes.has("ECONNABORTED") ||
    evidence.codes.has("ECONNRESET") ||
    evidence.codes.has("EPIPE");
  if (
    evidence.names.has("AbortError") ||
    evidence.names.has("TimeoutError") ||
    evidence.codes.has("ABORT_ERR") ||
    streamDisconnected ||
    evidence.codes.has("ETIMEDOUT")
  ) {
    return new CandidateSourceSnapshotFilebaseTransportError({
      failureClass: "outcome_unknown",
      outcome: "timeout_unknown",
      providerRequestIdHash: evidence.providerRequestIdHash,
      stage:
        streamDisconnected && operation === "put_object"
          ? "put_object_streaming_request"
          : evidence.names.has("AbortError") ||
              evidence.names.has("TimeoutError") ||
              evidence.codes.has("ABORT_ERR") ||
              evidence.codes.has("ETIMEDOUT")
            ? "transport_deadline"
            : stageForRequest,
    });
  }
  if (
    [...evidence.statuses].some((status) => status === 429 || status >= 500)
  ) {
    return new CandidateSourceSnapshotFilebaseTransportError({
      failureClass: "retryable",
      outcome: "retryable_http_error",
      providerRequestIdHash: evidence.providerRequestIdHash,
      stage:
        operation === "put_object"
          ? "put_object_provider_response"
          : "head_object_request",
    });
  }
  if (
    ["ECONNREFUSED", "EHOSTUNREACH", "ENETUNREACH", "ENOTFOUND"].some((code) =>
      evidence.codes.has(code),
    )
  ) {
    return new CandidateSourceSnapshotFilebaseTransportError({
      failureClass: "retryable",
      outcome: "connection_failure",
      providerRequestIdHash: evidence.providerRequestIdHash,
      stage:
        operation === "put_object"
          ? "put_object_connection"
          : "head_object_request",
    });
  }
  if (operation === "put_object" && evidence.statuses.size === 0) {
    // Smithy deliberately declines internal retries for streaming payloads. An
    // unclassified error after dispatch cannot prove whether Filebase accepted
    // the immutable object, so it always requires a HEAD reconciliation.
    return new CandidateSourceSnapshotFilebaseTransportError({
      failureClass: "outcome_unknown",
      outcome: "timeout_unknown",
      providerRequestIdHash: evidence.providerRequestIdHash,
      stage: "put_object_streaming_request",
    });
  }
  return new CandidateSourceSnapshotFilebaseTransportError({
    failureClass: "terminal",
    outcome: "terminal_failure",
    providerRequestIdHash: evidence.providerRequestIdHash,
    stage:
      operation === "put_object" && evidence.statuses.size > 0
        ? "put_object_provider_response"
        : stageForRequest,
  });
}

function inspectionReceipt(value: unknown): string {
  return canonicalJsonSha256(value);
}

/** Real Filebase S3 transport. It never retries; coordinator admissions own retries. */
export class RealCandidateSourceSnapshotFilebaseTransport implements CandidateSourceSnapshotUploadTransport {
  readonly #config: EnabledCandidateSourceSnapshotExecutionConfig;
  readonly #executor: CandidateSourceSnapshotS3CommandExecutor;
  readonly #ownedExecutor: AwsCandidateSourceSnapshotS3CommandExecutor | null;
  readonly #source: CandidateSourceSnapshotLocalObjectSource;
  readonly #transportLimits: CandidateSourceSnapshotTransportLimits;
  #maxSockets: number;
  #closed = false;

  constructor(input: {
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    executor?: CandidateSourceSnapshotS3CommandExecutor;
    source: CandidateSourceSnapshotLocalObjectSource;
    transportLimits?: CandidateSourceSnapshotTransportLimits;
  }) {
    if (!input.config.enabled) {
      throw new Error("Candidate source-snapshot executor is disabled");
    }
    this.#config = input.config;
    this.#transportLimits = resolveCandidateSourceSnapshotTransportLimits(
      input.config,
      input.transportLimits,
    );
    this.#maxSockets = this.#transportLimits.maxSockets;
    this.#ownedExecutor = input.executor
      ? null
      : new AwsCandidateSourceSnapshotS3CommandExecutor(
          input.config,
          this.#transportLimits,
        );
    this.#executor = input.executor ?? this.#ownedExecutor!;
    this.#source = input.source;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#ownedExecutor?.close();
  }

  setMaxSockets(maxSockets: CandidateSourceSnapshotSocketStage): void {
    assertSocketStageTransition(this.#maxSockets, maxSockets);
    this.#executor.setMaxSockets?.(maxSockets);
    this.#maxSockets = maxSockets;
  }

  #bucket(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
  ): string {
    const validated = validateCandidateSourceSnapshotDemoPlan(plan);
    if (
      validated.planId !== this.#config.planId ||
      validated.planSha256 !== this.#config.planSha256
    ) {
      throw new Error("Candidate execution plan does not match configuration");
    }
    assertCandidateSourceSnapshotObjectNamespace(validated, object);
    return object.domain === "open_data"
      ? validated.targets.openData.bucket
      : validated.targets.queryTable.bucket;
  }

  async inspectExistingOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ) {
    const bucket = this.#bucket(plan, object);
    await this.#source.verify(object);
    const deadline = deadlineSignal(
      signal,
      this.#transportLimits.requestTimeoutMs,
    );
    try {
      const result = await this.#executor.send(
        new HeadObjectCommand({ Bucket: bucket, Key: object.remoteObjectKey }),
        deadline.signal,
      );
      const observedCid = providerCid(result.output, result.responseHeaders);
      const observedSha256 = result.output.Metadata?.["oracle-sha256"] ?? null;
      const observedBytes = Number(result.output.ContentLength ?? -1);
      const baseReceipt = {
        operation: "head_object",
        providerRequestIdHash: requestIdHash(
          result.output,
          result.responseHeaders,
        ),
        responseBytes: responseByteCount(result.responseHeaders),
      };
      if (
        observedCid === null ||
        observedSha256 === null ||
        !SHA256_PATTERN.test(observedSha256) ||
        !Number.isSafeInteger(observedBytes) ||
        observedBytes < 0
      ) {
        return {
          outcome: "ambiguous" as const,
          receiptSha256: inspectionReceipt({
            ...baseReceipt,
            outcome: "ambiguous",
          }),
        };
      }
      const outcome =
        observedCid === object.expectedCid &&
        observedSha256 === object.sha256 &&
        observedBytes === object.byteSize
          ? ("verified" as const)
          : ("mismatch" as const);
      return {
        observedBytes,
        observedCid,
        observedSha256,
        outcome,
        receiptSha256: inspectionReceipt({
          ...baseReceipt,
          observedBytes,
          observedCid,
          observedSha256,
          outcome,
        }),
      };
    } catch (error) {
      const evidence = sanitizedTransportErrorEvidence(error);
      if (
        evidence.statuses.has(404) ||
        evidence.names.has("NotFound") ||
        evidence.names.has("NoSuchKey")
      ) {
        return {
          outcome: "absent" as const,
          receiptSha256: inspectionReceipt({
            operation: "head_object",
            outcome: "absent",
          }),
        };
      }
      throw classifyTransportError(error, "head_object");
    } finally {
      deadline.cleanup();
    }
  }

  async uploadOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ) {
    const bucket = this.#bucket(plan, object);
    const local = this.#source.openVerifiedUploadBody
      ? await this.#source.openVerifiedUploadBody(object)
      : await (async (): Promise<CandidateSourceSnapshotUploadBodyLease> => {
          const opened = await this.#source.openVerifiedStream(object);
          let released = false;
          return {
            ...opened,
            release: async () => {
              if (released) return;
              released = true;
              if (!opened.body.destroyed) opened.body.destroy();
              await finished(opened.body).catch(() => undefined);
            },
          };
        })();
    const deadline = deadlineSignal(
      signal,
      this.#transportLimits.requestTimeoutMs,
    );
    try {
      const result = await this.#executor.send(
        new PutObjectCommand({
          Body: local.body,
          Bucket: bucket,
          ContentLength: local.contentLength,
          ContentType: local.contentType,
          Key: object.remoteObjectKey,
          Metadata: {
            "oracle-cid": object.expectedCid,
            "oracle-sha256": object.sha256,
          },
        }),
        deadline.signal,
      );
      return {
        providerCid: providerCid(result.output, result.responseHeaders),
        providerRequestIdHash: requestIdHash(
          result.output,
          result.responseHeaders,
        ),
        responseBytes: responseByteCount(result.responseHeaders),
      };
    } catch (error) {
      throw classifyTransportError(error, "put_object");
    } finally {
      deadline.cleanup();
      await local.release();
    }
  }
}

export function createCandidateSourceSnapshotFilebaseTransport(input: {
  config: CandidateSourceSnapshotExecutionConfig;
  executor?: CandidateSourceSnapshotS3CommandExecutor;
  source: CandidateSourceSnapshotLocalObjectSource;
  transportLimits?: CandidateSourceSnapshotTransportLimits;
}): RealCandidateSourceSnapshotFilebaseTransport {
  if (!input.config.enabled) {
    throw new Error("Candidate source-snapshot executor is disabled");
  }
  return new RealCandidateSourceSnapshotFilebaseTransport({
    config: input.config,
    ...(input.executor ? { executor: input.executor } : {}),
    source: input.source,
    ...(input.transportLimits
      ? { transportLimits: input.transportLimits }
      : {}),
  });
}
