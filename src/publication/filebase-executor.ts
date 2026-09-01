import { createHash } from "node:crypto";

import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { sha256 } from "../lib/hash.js";
import {
  type CandidateDemoPlan,
  type CandidateDemoTarget,
  validateCandidateDemoPlan,
} from "./candidate-demo.js";
import type { EnabledCandidateDemoConfig } from "./candidate-demo-config.js";
import { calculateIpfsCid } from "./ipfs-cid.js";
import {
  HttpPublicReadTransport,
  type PublicReadTransport,
} from "../mcp/public-ipns-provider.js";

export interface CandidateUploadArtifact {
  bytes: Buffer;
  domain: "open_data" | "query_table";
  expectedCid: string;
  objectKey: string;
  sha256: string;
}

export interface CandidateUploadReceipt {
  cid: string;
  domain: CandidateUploadArtifact["domain"];
  objectKey: string;
  providerRequestIdHash: string | null;
  requestCount: number;
  receiptSha256: string;
  sha256: string;
}

export interface CandidateIpnsRecord {
  cid: string;
  label: string;
  networkKey: string;
}

export interface FilebaseCandidateTransport {
  resolve(target: CandidateDemoTarget, signal?: AbortSignal): Promise<string>;
  update(
    target: CandidateDemoTarget,
    cid?: string,
    signal?: AbortSignal,
  ): Promise<CandidateIpnsRecord>;
  upload(
    target: CandidateDemoTarget,
    artifact: CandidateUploadArtifact,
    signal?: AbortSignal,
  ): Promise<{
    cid: string | null;
    providerRequestIdHash: string | null;
    requestCount?: number;
  }>;
}

export interface CandidateDemoExecutionJournal {
  assertIpnsMutationReady(
    plan: CandidateDemoPlan,
    domain: CandidateUploadArtifact["domain"],
  ): Promise<void>;
  beforeUpload(
    plan: CandidateDemoPlan,
    artifact: CandidateUploadArtifact,
  ): Promise<void>;
  beforeIpnsMutation(
    plan: CandidateDemoPlan,
    domain: CandidateUploadArtifact["domain"],
  ): Promise<void>;
  beforeIpnsRollback(
    plan: CandidateDemoPlan,
    domain: CandidateUploadArtifact["domain"],
  ): Promise<void>;
  recordUpload(
    plan: CandidateDemoPlan,
    artifact: CandidateUploadArtifact,
    receipt: CandidateUploadReceipt,
  ): Promise<void>;
  recordIpnsVerified(
    plan: CandidateDemoPlan,
    domain: CandidateUploadArtifact["domain"],
  ): Promise<void>;
  recordIpnsRolledBack(
    plan: CandidateDemoPlan,
    domain: CandidateUploadArtifact["domain"],
  ): Promise<void>;
}

export interface CandidatePublicIpnsVerifier {
  verify(
    identity: string,
    expectedCid: string,
    signal?: AbortSignal,
  ): Promise<void>;
}

export interface CandidateDemoRequestMetrics {
  namesApiRequests: number;
  publicResolverRequests: number;
  s3Requests: number;
}

export class RealCandidatePublicIpnsVerifier implements CandidatePublicIpnsVerifier {
  readonly #transport: PublicReadTransport;

  constructor(
    config: EnabledCandidateDemoConfig,
    fetchImpl: typeof fetch = fetch,
    metrics?: CandidateDemoRequestMetrics,
  ) {
    const countedFetch: typeof fetch = async (input, init) => {
      if (metrics) metrics.publicResolverRequests += 1;
      return await fetchImpl(input, init);
    };
    this.#transport = new HttpPublicReadTransport(
      {
        maxCacheAgeSeconds: 300,
        maxJsonObjectBytes: 64 * 1024,
        maxParquetBytes: config.limits.maxObjectBytes,
        maxRedirects: 2,
        retries: config.limits.maxRetries,
        transportTimeoutMs: config.limits.requestTimeoutMs,
      },
      countedFetch,
    );
  }

  async verify(
    identity: string,
    expectedCid: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const observations = await this.#transport.resolveIpns(identity, signal);
    if (
      observations.length < 2 ||
      observations.some(
        (entry) =>
          entry.status !== "resolved" ||
          entry.cid !== expectedCid ||
          (entry.cacheAgeSeconds !== null && entry.cacheAgeSeconds > 300),
      )
    ) {
      throw new Error("Candidate public IPNS resolution did not reach target");
    }
  }
}

interface FilebaseName {
  cid: string;
  label: string;
  network_key: string;
}

function targetFor(
  plan: CandidateDemoPlan,
  domain: CandidateUploadArtifact["domain"],
): CandidateDemoTarget {
  return domain === "open_data"
    ? plan.targets.openData
    : plan.targets.queryTable;
}

function timeoutSignal(
  timeoutMs: number,
  parent?: AbortSignal | null,
): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return parent ? AbortSignal.any([parent, timeout]) : timeout;
}

function filebaseName(value: unknown): FilebaseName {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Filebase IPNS response is invalid");
  }
  const record = value as Record<string, unknown>;
  if (
    typeof record.cid !== "string" ||
    typeof record.label !== "string" ||
    typeof record.network_key !== "string"
  ) {
    throw new Error("Filebase IPNS response is invalid");
  }
  return {
    cid: record.cid,
    label: record.label,
    network_key: record.network_key,
  };
}

async function boundedJson(response: Response, maximumBytes = 64 * 1024) {
  const declared = Number(response.headers.get("content-length") ?? 0);
  if (Number.isFinite(declared) && declared > maximumBytes) {
    throw new Error("Filebase response exceeds the configured limit");
  }
  if (!response.body) throw new Error("Filebase response is empty");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw new Error("Filebase response exceeds the configured limit");
    }
    chunks.push(result.value);
  }
  const bytes = Buffer.concat(
    chunks.map((chunk) => Buffer.from(chunk)),
    total,
  );
  try {
    return JSON.parse(bytes.toString("utf8")) as unknown;
  } catch {
    throw new Error("Filebase response is not valid JSON");
  }
}

export class RealFilebaseCandidateTransport implements FilebaseCandidateTransport {
  readonly #apiEndpoint: string;
  readonly #apiToken: string;
  readonly #fetch: typeof fetch;
  readonly #limits: EnabledCandidateDemoConfig["limits"];
  readonly #metrics: CandidateDemoRequestMetrics | undefined;
  readonly #s3: S3Client;

  constructor(
    config: EnabledCandidateDemoConfig,
    options: {
      fetchImpl?: typeof fetch;
      metrics?: CandidateDemoRequestMetrics;
      s3Client?: S3Client;
    } = {},
  ) {
    this.#apiEndpoint = config.apiEndpoint;
    this.#apiToken = config.apiToken;
    this.#fetch = options.fetchImpl ?? fetch;
    this.#limits = config.limits;
    this.#metrics = options.metrics;
    this.#s3 =
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
  }

  async #request(pathname: string, init: RequestInit): Promise<Response> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.#limits.maxRetries; attempt += 1) {
      try {
        if (this.#metrics) this.#metrics.namesApiRequests += 1;
        const response = await this.#fetch(`${this.#apiEndpoint}${pathname}`, {
          ...init,
          headers: {
            Authorization: `Bearer ${this.#apiToken}`,
            "Content-Type": "application/json",
            ...(init.headers ?? {}),
          },
          redirect: "error",
          signal: timeoutSignal(this.#limits.requestTimeoutMs, init.signal),
        });
        if (response.status === 429 || response.status >= 500) {
          if (attempt < this.#limits.maxRetries) continue;
        }
        if (!response.ok) {
          throw new Error(
            `Filebase request failed with HTTP ${response.status}`,
          );
        }
        return response;
      } catch (error) {
        lastError = error;
        if (attempt >= this.#limits.maxRetries) break;
      }
    }
    void lastError;
    throw new Error("Filebase request failed within the bounded retry policy");
  }

  async #record(target: CandidateDemoTarget, signal?: AbortSignal) {
    const response = await this.#request("/v1/names", {
      method: "GET",
      ...(signal ? { signal } : {}),
    });
    const value = await boundedJson(response);
    if (!Array.isArray(value)) throw new Error("Filebase IPNS list is invalid");
    const match = value
      .map(filebaseName)
      .find(
        (entry) =>
          entry.label === target.ipnsLabel &&
          entry.network_key === target.ipnsNetworkKey,
      );
    if (!match) {
      throw new Error("Configured candidate IPNS identity is unavailable");
    }
    return match;
  }

  async resolve(
    target: CandidateDemoTarget,
    signal?: AbortSignal,
  ): Promise<string> {
    return (await this.#record(target, signal)).cid;
  }

  async update(
    target: CandidateDemoTarget,
    requestedCid = target.targetCid,
    signal?: AbortSignal,
  ): Promise<CandidateIpnsRecord> {
    const cid = requestedCid;
    if (cid !== target.targetCid && cid !== target.priorCid) {
      throw new Error(
        "Candidate IPNS update CID is outside the immutable plan",
      );
    }
    await this.#request(`/v1/names/${encodeURIComponent(target.ipnsLabel)}`, {
      body: JSON.stringify({ cid, enabled: true }),
      method: "PUT",
      ...(signal ? { signal } : {}),
    });
    const record = await this.#record(target, signal);
    if (record.cid !== cid) {
      throw new Error("Filebase IPNS target verification failed");
    }
    return {
      cid,
      label: record.label,
      networkKey: record.network_key,
    };
  }

  async upload(
    target: CandidateDemoTarget,
    artifact: CandidateUploadArtifact,
    signal?: AbortSignal,
  ): Promise<{
    cid: string | null;
    providerRequestIdHash: string | null;
    requestCount?: number;
  }> {
    let providerCid: string | null = null;
    let providerRequestId: string | null = null;
    const middlewareName = `captureFilebaseReceipt${createHash("sha256")
      .update(`${artifact.domain}:${artifact.objectKey}`)
      .digest("hex")}`;
    this.#s3.middlewareStack.add(
      (next) => async (argumentsValue) => {
        const result = await next(argumentsValue);
        const response = result.response as
          { headers?: Record<string, string> } | undefined;
        providerCid = response?.headers?.["x-amz-meta-cid"] ?? null;
        providerRequestId = response?.headers?.["x-amz-request-id"] ?? null;
        return result;
      },
      { name: middlewareName, priority: "low", step: "deserialize" },
    );
    try {
      const result = await this.#s3.send(
        new PutObjectCommand({
          Body: artifact.bytes,
          Bucket: target.bucket,
          ContentType:
            artifact.domain === "query_table"
              ? "application/vnd.apache.parquet"
              : "application/json",
          Key: artifact.objectKey,
        }),
        { abortSignal: timeoutSignal(this.#limits.requestTimeoutMs, signal) },
      );
      const attempts = result.$metadata.attempts ?? 1;
      if (this.#metrics) this.#metrics.s3Requests += attempts;
      return {
        cid: providerCid,
        providerRequestIdHash:
          providerRequestId === null ? null : sha256(providerRequestId),
        requestCount: attempts,
      };
    } catch {
      throw new Error("Filebase upload failed within the bounded retry policy");
    } finally {
      this.#s3.middlewareStack.remove(middlewareName);
    }
  }
}

export class CandidateDemoFilebaseExecutor {
  readonly #journal: CandidateDemoExecutionJournal;
  #activeUploads = 0;
  readonly #plan: CandidateDemoPlan;
  readonly #publicVerifier: CandidatePublicIpnsVerifier;
  readonly #transport: FilebaseCandidateTransport;
  readonly #waiters: Array<() => void> = [];

  constructor(
    plan: CandidateDemoPlan,
    transport: FilebaseCandidateTransport,
    journal: CandidateDemoExecutionJournal,
    publicVerifier: CandidatePublicIpnsVerifier,
  ) {
    this.#plan = validateCandidateDemoPlan(plan);
    this.#transport = transport;
    this.#journal = journal;
    this.#publicVerifier = publicVerifier;
  }

  async resolvePrior(
    domain: CandidateUploadArtifact["domain"],
    signal?: AbortSignal,
  ): Promise<string> {
    const target = targetFor(this.#plan, domain);
    const resolved = await this.#transport.resolve(target, signal);
    if (resolved !== target.priorCid) {
      throw new Error("Candidate Filebase prior CID does not match the plan");
    }
    return resolved;
  }

  async updateIpns(
    domain: CandidateUploadArtifact["domain"],
    signal?: AbortSignal,
  ): Promise<CandidateIpnsRecord> {
    const record = await this.updateIpnsControlPlane(domain, signal);
    const target = targetFor(this.#plan, domain);
    await this.#publicVerifier.verify(
      target.ipnsNetworkKey,
      target.targetCid,
      signal,
    );
    await this.#journal.recordIpnsVerified(this.#plan, domain);
    return record;
  }

  async updateIpnsControlPlane(
    domain: CandidateUploadArtifact["domain"],
    signal?: AbortSignal,
  ): Promise<CandidateIpnsRecord> {
    await this.#journal.assertIpnsMutationReady(this.#plan, domain);
    await this.#journal.beforeIpnsMutation(this.#plan, domain);
    const target = targetFor(this.#plan, domain);
    const record = await this.#transport.update(
      target,
      target.targetCid,
      signal,
    );
    if (
      record.cid !== target.targetCid ||
      record.label !== target.ipnsLabel ||
      record.networkKey !== target.ipnsNetworkKey
    ) {
      throw new Error(
        "Candidate Filebase IPNS receipt does not match the plan",
      );
    }
    return record;
  }

  async rollbackIpns(
    domain: CandidateUploadArtifact["domain"],
    signal?: AbortSignal,
  ): Promise<CandidateIpnsRecord> {
    const target = targetFor(this.#plan, domain);
    await this.#journal.beforeIpnsRollback(this.#plan, domain);
    const record = await this.#transport.update(
      target,
      target.priorCid,
      signal,
    );
    if (
      record.cid !== target.priorCid ||
      record.label !== target.ipnsLabel ||
      record.networkKey !== target.ipnsNetworkKey
    ) {
      throw new Error(
        "Candidate IPNS rollback receipt does not match the plan",
      );
    }
    await this.#publicVerifier.verify(
      target.ipnsNetworkKey,
      target.priorCid,
      signal,
    );
    await this.#journal.recordIpnsRolledBack(this.#plan, domain);
    return record;
  }

  async upload(
    artifact: CandidateUploadArtifact,
    signal?: AbortSignal,
  ): Promise<CandidateUploadReceipt> {
    const planned = this.#plan.objects.find(
      (object) =>
        object.domain === artifact.domain &&
        object.objectKey === artifact.objectKey,
    );
    if (
      !planned ||
      planned.byteSize !== artifact.bytes.length ||
      planned.expectedCid !== artifact.expectedCid ||
      planned.sha256 !== artifact.sha256
    ) {
      throw new Error(
        "Candidate demo artifact is not in the approved inventory",
      );
    }
    if (artifact.bytes.length > this.#plan.limits.maxObjectBytes) {
      throw new Error(
        "Candidate demo object exceeds the configured size limit",
      );
    }
    const localSha256 = sha256(artifact.bytes);
    const localCid = await calculateIpfsCid(artifact.bytes);
    if (localSha256 !== artifact.sha256 || localCid !== artifact.expectedCid) {
      throw new Error("Candidate demo artifact bytes do not match the plan");
    }
    await this.#acquireUploadSlot();
    try {
      await this.#journal.beforeUpload(this.#plan, artifact);
      const result = await this.#transport.upload(
        targetFor(this.#plan, artifact.domain),
        artifact,
        signal,
      );
      if (result.cid === null || result.cid !== localCid) {
        throw new Error("Filebase returned a missing or mismatched CID");
      }
      const receipt = {
        cid: result.cid,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
        providerRequestIdHash: result.providerRequestIdHash,
        requestCount: result.requestCount ?? 1,
        sha256: artifact.sha256,
      };
      const completed = {
        ...receipt,
        receiptSha256: sha256(JSON.stringify(receipt)),
      };
      await this.#journal.recordUpload(this.#plan, artifact, completed);
      return completed;
    } finally {
      this.#releaseUploadSlot();
    }
  }

  async #acquireUploadSlot(): Promise<void> {
    if (this.#activeUploads < this.#plan.limits.maxConcurrency) {
      this.#activeUploads += 1;
      return;
    }
    await new Promise<void>((resolve) => this.#waiters.push(resolve));
    this.#activeUploads += 1;
  }

  #releaseUploadSlot(): void {
    this.#activeUploads -= 1;
    this.#waiters.shift()?.();
  }
}

// Production/default construction remains deliberately fail-closed. Callers
// must first load an explicit enabled candidate-demo configuration and pass a
// separately approved, durable CandidateDemoPlan.
export function createCandidateDemoFilebaseExecutor(
  config: EnabledCandidateDemoConfig,
  plan: CandidateDemoPlan,
  journal: CandidateDemoExecutionJournal,
  metrics?: CandidateDemoRequestMetrics,
): CandidateDemoFilebaseExecutor {
  if (!config.enabled) throw new Error("Candidate demo executor is disabled");
  return new CandidateDemoFilebaseExecutor(
    plan,
    new RealFilebaseCandidateTransport(config, {
      ...(metrics ? { metrics } : {}),
    }),
    journal,
    new RealCandidatePublicIpnsVerifier(config, fetch, metrics),
  );
}
