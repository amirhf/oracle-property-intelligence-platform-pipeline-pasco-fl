import { createHash, randomUUID } from "node:crypto";
import { constants, createReadStream } from "node:fs";
import { lstat, open, realpath, stat } from "node:fs/promises";
import path from "node:path";
import type { Readable } from "node:stream";

import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  type HeadObjectCommandOutput,
  type PutObjectCommandOutput,
} from "@aws-sdk/client-s3";

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
  type CandidateSourceSnapshotUploadTransport,
} from "./candidate-source-snapshot-upload.js";
import { calculateIpfsFileCid } from "./ipfs-cid.js";

const SHA256_PATTERN = /^[a-f0-9]{64}$/;

export interface CandidateSourceSnapshotLocalObjectSource {
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
}

export interface CandidateSourceSnapshotS3CommandResult<Output> {
  output: Output;
  responseHeaders: Readonly<Record<string, string>>;
}

export interface CandidateSourceSnapshotS3CommandExecutor {
  send(
    command: HeadObjectCommand,
    signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<HeadObjectCommandOutput>>;
  send(
    command: PutObjectCommand,
    signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<PutObjectCommandOutput>>;
}

export class AwsCandidateSourceSnapshotS3CommandExecutor implements CandidateSourceSnapshotS3CommandExecutor {
  readonly #client: S3Client;
  #closed = false;

  constructor(config: EnabledCandidateSourceSnapshotExecutionConfig) {
    if (!config.enabled || config.s3Endpoint !== "https://s3.filebase.com") {
      throw new Error("Candidate Filebase S3 execution is not enabled");
    }
    this.#client = new S3Client({
      credentials: {
        accessKeyId: config.s3AccessKeyId,
        secretAccessKey: config.s3SecretAccessKey,
      },
      endpoint: config.s3Endpoint,
      forcePathStyle: true,
      maxAttempts: 1,
      region: "us-east-1",
    });
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
    const output = await this.#client.send(command, {
      ...(signal ? { abortSignal: signal } : {}),
    });
    if ((output.$metadata.attempts ?? 1) !== 1) {
      throw new Error("S3 client performed an unjournaled internal retry");
    }
    return { output, responseHeaders };
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

function classifyTransportError(
  error: unknown,
): CandidateSourceSnapshotUploadError {
  if (error instanceof CandidateSourceSnapshotUploadError) return error;
  const value = error as {
    $metadata?: { httpStatusCode?: number };
    code?: string;
    name?: string;
  };
  const status = value.$metadata?.httpStatusCode;
  if (
    value.name === "AbortError" ||
    value.name === "TimeoutError" ||
    value.code === "ABORT_ERR"
  ) {
    return new CandidateSourceSnapshotUploadError("timeout_unknown");
  }
  if (status === 429 || (status !== undefined && status >= 500)) {
    return new CandidateSourceSnapshotUploadError("retryable_http_error");
  }
  if (
    [
      "ECONNABORTED",
      "ECONNREFUSED",
      "ECONNRESET",
      "EHOSTUNREACH",
      "ENETUNREACH",
      "ENOTFOUND",
      "EPIPE",
      "ETIMEDOUT",
    ].includes(value.code ?? "")
  ) {
    return new CandidateSourceSnapshotUploadError("connection_failure");
  }
  return new CandidateSourceSnapshotUploadError("terminal_failure");
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
  #closed = false;

  constructor(input: {
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    executor?: CandidateSourceSnapshotS3CommandExecutor;
    source: CandidateSourceSnapshotLocalObjectSource;
  }) {
    if (!input.config.enabled) {
      throw new Error("Candidate source-snapshot executor is disabled");
    }
    this.#config = input.config;
    this.#ownedExecutor = input.executor
      ? null
      : new AwsCandidateSourceSnapshotS3CommandExecutor(input.config);
    this.#executor = input.executor ?? this.#ownedExecutor!;
    this.#source = input.source;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#ownedExecutor?.close();
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

  #signal(signal?: AbortSignal): AbortSignal {
    const timeout = AbortSignal.timeout(this.#config.limits.requestTimeoutMs);
    return signal ? AbortSignal.any([signal, timeout]) : timeout;
  }

  async inspectExistingOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ) {
    const bucket = this.#bucket(plan, object);
    await this.#source.verify(object);
    try {
      const result = await this.#executor.send(
        new HeadObjectCommand({ Bucket: bucket, Key: object.remoteObjectKey }),
        this.#signal(signal),
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
      const value = error as {
        $metadata?: { httpStatusCode?: number };
        name?: string;
      };
      if (
        value.$metadata?.httpStatusCode === 404 ||
        value.name === "NotFound" ||
        value.name === "NoSuchKey"
      ) {
        return {
          outcome: "absent" as const,
          receiptSha256: inspectionReceipt({
            operation: "head_object",
            outcome: "absent",
          }),
        };
      }
      throw classifyTransportError(error);
    }
  }

  async uploadOnce(
    plan: CandidateSourceSnapshotDemoPlan,
    object: CandidateSourceSnapshotUploadObject,
    signal?: AbortSignal,
  ) {
    const bucket = this.#bucket(plan, object);
    const local = await this.#source.openVerifiedStream(object);
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
        this.#signal(signal),
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
      local.body.destroy();
      throw classifyTransportError(error);
    }
  }
}

export function createCandidateSourceSnapshotFilebaseTransport(input: {
  config: CandidateSourceSnapshotExecutionConfig;
  executor?: CandidateSourceSnapshotS3CommandExecutor;
  source: CandidateSourceSnapshotLocalObjectSource;
}): RealCandidateSourceSnapshotFilebaseTransport {
  if (!input.config.enabled) {
    throw new Error("Candidate source-snapshot executor is disabled");
  }
  return new RealCandidateSourceSnapshotFilebaseTransport({
    config: input.config,
    ...(input.executor ? { executor: input.executor } : {}),
    source: input.source,
  });
}
