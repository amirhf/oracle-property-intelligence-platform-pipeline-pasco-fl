import { createHash } from "node:crypto";

import type { AsyncBuffer } from "hyparquet";

import type { PublicIpnsProviderConfig } from "./config.js";
import { CIDV0_PATTERN } from "../publication/ipfs-cid.js";

const CIDV1_BASE32_PATTERN = /^b[a-z2-7]{20,120}$/;
const RETRY_BACKOFF_MS = 50;
const CANDIDATE_ARTIFACT_GATEWAY_ORIGIN =
  "https://foolish-green-asp.myfilebase.com";

const GATEWAY_PROFILES = {
  candidate_filebase_delegated_v2: ["https://ipfs.filebase.io"],
  public_two_gateway_v1: ["https://ipfs.io", "https://dweb.link"],
} as const;

export type PublicCidGatewayProfile = keyof typeof GATEWAY_PROFILES;

export type PublicCidRangeErrorCode =
  | "artifact_invalid"
  | "artifact_too_large"
  | "cid_mismatch"
  | "configuration_invalid"
  | "hash_mismatch"
  | "range_budget_exhausted"
  | "range_invalid"
  | "range_unsupported"
  | "redirect_rejected"
  | "timeout"
  | "transport_unavailable";

export class PublicCidRangeError extends Error {
  constructor(
    readonly code: PublicCidRangeErrorCode,
    message: string,
    readonly retryable = false,
  ) {
    super(message);
  }
}

export interface PublicCidObjectMetadata {
  acceptsByteRanges: boolean;
  byteLength: number;
  cid: string;
}

export interface PublicCidRangeTransport {
  statCid(
    cid: string,
    maximumObjectBytes: number,
    signal?: AbortSignal,
  ): Promise<PublicCidObjectMetadata>;
  readCidRange(
    cid: string,
    start: number,
    endExclusive: number,
    expectedObjectBytes: number,
    maximumResponseBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array>;
}

export interface PublicCidRangeBudget {
  maximumObjectBytes: number;
  maximumRangeBytes: number;
  maximumRanges: number;
  maximumTotalRangeBytes: number;
}

export interface PublicCidRangeMetrics {
  requestedBytes: number;
  rangeRequests: number;
}

export interface PublicCidStreamingVerificationBudget extends PublicCidRangeBudget {
  maximumBufferedBytes: number;
  maximumConcurrency: number;
}

type RetryDelay = (milliseconds: number) => Promise<void>;

const defaultRetryDelay: RetryDelay = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function rangeError(
  code: PublicCidRangeErrorCode,
  message: string,
  retryable = false,
): PublicCidRangeError {
  return new PublicCidRangeError(code, message, retryable);
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw rangeError("configuration_invalid", `${label} must be positive`);
  }
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw rangeError("configuration_invalid", `${label} must be non-negative`);
  }
  return value;
}

function validCid(cid: string): boolean {
  return CIDV0_PATTERN.test(cid) || CIDV1_BASE32_PATTERN.test(cid);
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("Operation aborted");
  }
}

function combinedSignal(
  signal: AbortSignal | undefined,
  timeoutMs: number,
): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

function isAbortError(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}

async function retryDelay(
  delay: RetryDelay,
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  throwIfAborted(signal);
  if (!signal) {
    await delay(milliseconds);
    return;
  }
  await Promise.race([
    delay(milliseconds),
    new Promise<never>((_, reject) => {
      signal.addEventListener(
        "abort",
        () => reject(signal.reason ?? new Error("Operation aborted")),
        { once: true },
      );
    }),
  ]);
}

function responseCids(headers: Headers): ReadonlySet<string> {
  return new Set(
    [headers.get("x-ipfs-roots"), headers.get("x-ipfs-path")]
      .filter((value): value is string => value !== null)
      .flatMap((value) =>
        value.match(/Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/g),
      )
      .filter((value): value is string => value !== null),
  );
}

function validateResponseCid(headers: Headers, expectedCid: string): void {
  const observed = responseCids(headers);
  if (observed.size > 0 && !observed.has(expectedCid)) {
    throw rangeError(
      "cid_mismatch",
      "Public range response is not bound to the requested CID",
    );
  }
}

function parsedContentLength(headers: Headers): number | null {
  const value = headers.get("content-length");
  if (value === null) return null;
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw rangeError(
      "artifact_invalid",
      "Public artifact content length is invalid",
    );
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw rangeError(
      "artifact_invalid",
      "Public artifact content length is invalid",
    );
  }
  return parsed;
}

async function boundedBody(
  response: Response,
  maximumBytes: number,
): Promise<Uint8Array> {
  const declared = parsedContentLength(response.headers);
  if (declared !== null && declared > maximumBytes) {
    throw rangeError(
      "artifact_too_large",
      "Public range response exceeds its bound",
    );
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const value = new Uint8Array(declared ?? maximumBytes);
  let total = 0;
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    if (
      total + result.value.byteLength > maximumBytes ||
      total + result.value.byteLength > value.byteLength
    ) {
      await reader.cancel();
      throw rangeError(
        "artifact_too_large",
        "Public range response exceeds its bound",
      );
    }
    value.set(result.value, total);
    total += result.value.byteLength;
  }
  return total === value.byteLength ? value : value.slice(0, total);
}

function allowedCidUrl(
  value: URL,
  cid: string,
  origins: readonly string[],
): boolean {
  return (
    value.protocol === "https:" &&
    origins.includes(value.origin) &&
    value.pathname === `/ipfs/${cid}` &&
    value.username === "" &&
    value.password === "" &&
    value.search === "" &&
    value.hash === ""
  );
}

function contentRange(
  headers: Headers,
): { endInclusive: number; start: number; total: number } | null {
  const value = headers.get("content-range");
  if (value === null) return null;
  const match = /^bytes (0|[1-9][0-9]*)-(0|[1-9][0-9]*)\/(0|[1-9][0-9]*)$/.exec(
    value,
  );
  if (!match) return null;
  const start = Number(match[1]);
  const endInclusive = Number(match[2]);
  const total = Number(match[3]);
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(endInclusive) ||
    !Number.isSafeInteger(total)
  ) {
    return null;
  }
  return { endInclusive, start, total };
}

export class HttpPublicCidRangeTransport implements PublicCidRangeTransport {
  readonly #origins: readonly string[];
  readonly #retryDelay: RetryDelay;

  constructor(
    profile: PublicCidGatewayProfile,
    readonly limits: Pick<
      PublicIpnsProviderConfig["limits"],
      "maxRedirects" | "retries" | "transportTimeoutMs"
    >,
    readonly fetchImplementation: typeof fetch = fetch,
    retryDelayImplementation: RetryDelay = defaultRetryDelay,
    candidateArtifactGatewayBaseUrl?: string,
  ) {
    if (
      candidateArtifactGatewayBaseUrl !== undefined &&
      (profile !== "candidate_filebase_delegated_v2" ||
        candidateArtifactGatewayBaseUrl !== CANDIDATE_ARTIFACT_GATEWAY_ORIGIN)
    ) {
      throw rangeError(
        "configuration_invalid",
        "Candidate artifact gateway is not approved",
      );
    }
    this.#origins = candidateArtifactGatewayBaseUrl
      ? [candidateArtifactGatewayBaseUrl]
      : GATEWAY_PROFILES[profile];
    this.#retryDelay = retryDelayImplementation;
    nonNegativeInteger(limits.maxRedirects, "redirect limit");
    nonNegativeInteger(limits.retries, "retry limit");
    positiveInteger(limits.transportTimeoutMs, "transport timeout");
  }

  async statCid(
    cid: string,
    maximumObjectBytes: number,
    signal?: AbortSignal,
  ): Promise<PublicCidObjectMetadata> {
    this.#validateCidAndMaximum(cid, maximumObjectBytes);
    return this.#attemptGateways(async (origin) => {
      const response = await this.#request(
        new URL(`/ipfs/${cid}`, origin),
        cid,
        "HEAD",
        undefined,
        signal,
      );
      validateResponseCid(response.headers, cid);
      const byteLength = parsedContentLength(response.headers);
      if (byteLength === null) {
        throw rangeError(
          "artifact_invalid",
          "Public artifact content length is unavailable",
        );
      }
      if (byteLength > maximumObjectBytes) {
        throw rangeError(
          "artifact_too_large",
          "Public artifact exceeds its plan-derived bound",
        );
      }
      return {
        acceptsByteRanges: (response.headers.get("accept-ranges") ?? "")
          .split(",")
          .map((value) => value.trim().toLowerCase())
          .includes("bytes"),
        byteLength,
        cid,
      };
    }, signal);
  }

  async readCidRange(
    cid: string,
    start: number,
    endExclusive: number,
    expectedObjectBytes: number,
    maximumResponseBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.#validateCidAndMaximum(cid, expectedObjectBytes);
    nonNegativeInteger(start, "range start");
    positiveInteger(endExclusive, "range end");
    positiveInteger(maximumResponseBytes, "range response bound");
    if (endExclusive <= start || endExclusive > expectedObjectBytes) {
      throw rangeError("range_invalid", "Public artifact range is invalid");
    }
    const requestedBytes = endExclusive - start;
    if (requestedBytes > maximumResponseBytes) {
      throw rangeError(
        "artifact_too_large",
        "Public artifact range exceeds its bound",
      );
    }
    return this.#attemptGateways(async (origin) => {
      const response = await this.#request(
        new URL(`/ipfs/${cid}`, origin),
        cid,
        "GET",
        { Range: `bytes=${start}-${endExclusive - 1}` },
        signal,
      );
      if (response.status !== 206) {
        throw rangeError(
          "range_unsupported",
          "Public gateway did not honor the byte range",
        );
      }
      validateResponseCid(response.headers, cid);
      const observedRange = contentRange(response.headers);
      if (
        observedRange === null ||
        observedRange.start !== start ||
        observedRange.endInclusive !== endExclusive - 1 ||
        observedRange.total !== expectedObjectBytes
      ) {
        throw rangeError(
          "artifact_invalid",
          "Public gateway returned an inconsistent byte range",
        );
      }
      const declared = parsedContentLength(response.headers);
      if (declared !== null && declared !== requestedBytes) {
        throw rangeError(
          "artifact_invalid",
          "Public gateway returned an inconsistent range length",
        );
      }
      let bytes: Uint8Array;
      try {
        bytes = await boundedBody(response, requestedBytes);
      } catch (error) {
        if (error instanceof PublicCidRangeError) throw error;
        if (isAbortError(error)) {
          throw rangeError("timeout", "Public range read timed out", true);
        }
        throw rangeError(
          "transport_unavailable",
          "Public range response failed",
          true,
        );
      }
      if (bytes.byteLength !== requestedBytes) {
        throw rangeError(
          "artifact_invalid",
          "Public gateway returned an incomplete byte range",
        );
      }
      return bytes;
    }, signal);
  }

  #validateCidAndMaximum(cid: string, maximumBytes: number): void {
    if (!validCid(cid)) {
      throw rangeError("configuration_invalid", "Public CID is invalid");
    }
    positiveInteger(maximumBytes, "artifact byte bound");
  }

  async #attemptGateways<T>(
    operation: (origin: string) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    let lastError: unknown;
    for (const origin of this.#origins) {
      for (let attempt = 0; attempt <= this.limits.retries; attempt += 1) {
        throwIfAborted(signal);
        try {
          return await operation(origin);
        } catch (error) {
          throwIfAborted(signal);
          const failure = isAbortError(error)
            ? rangeError("timeout", "Public range read timed out", true)
            : error;
          lastError = failure;
          if (!(failure instanceof PublicCidRangeError) || !failure.retryable) {
            throw failure;
          }
          if (attempt < this.limits.retries) {
            await retryDelay(
              this.#retryDelay,
              RETRY_BACKOFF_MS * (attempt + 1),
              signal,
            );
          }
        }
      }
    }
    if (lastError instanceof PublicCidRangeError) throw lastError;
    throw rangeError(
      "transport_unavailable",
      "Public range transport is unavailable",
      true,
    );
  }

  async #request(
    initial: URL,
    cid: string,
    method: "GET" | "HEAD",
    headers: HeadersInit | undefined,
    signal?: AbortSignal,
  ): Promise<Response> {
    throwIfAborted(signal);
    let current = initial;
    for (
      let redirectCount = 0;
      redirectCount <= this.limits.maxRedirects;
      redirectCount += 1
    ) {
      let response: Response;
      try {
        response = await this.fetchImplementation(current, {
          ...(headers ? { headers } : {}),
          method,
          redirect: "manual",
          signal: combinedSignal(signal, this.limits.transportTimeoutMs),
        });
      } catch (error) {
        throwIfAborted(signal);
        if (isAbortError(error)) {
          throw rangeError("timeout", "Public range read timed out", true);
        }
        throw rangeError(
          "transport_unavailable",
          "Public range request failed",
          true,
        );
      }
      if (response.status >= 300 && response.status < 400) {
        const location = response.headers.get("location");
        if (location === null || redirectCount === this.limits.maxRedirects) {
          throw rangeError(
            "redirect_rejected",
            "Public range redirect was rejected",
          );
        }
        const target = new URL(location, current);
        if (!allowedCidUrl(target, cid, this.#origins)) {
          throw rangeError(
            "redirect_rejected",
            "Public range redirect was rejected",
          );
        }
        current = target;
        continue;
      }
      if (!response.ok) {
        const retryable = [500, 502, 503, 504].includes(response.status);
        throw rangeError(
          "transport_unavailable",
          "Public range gateway returned an unavailable response",
          retryable,
        );
      }
      return response;
    }
    throw rangeError("redirect_rejected", "Public range redirect was rejected");
  }
}

export class BoundedPublicCidAsyncBuffer implements AsyncBuffer {
  readonly byteLength: number;
  readonly #budget: PublicCidStreamingVerificationBudget;
  readonly #cid: string;
  readonly #signal: AbortSignal | undefined;
  readonly #transport: PublicCidRangeTransport;
  readonly #waiters: Array<{
    bytes: number;
    cleanup: () => void;
    reject: (reason?: unknown) => void;
    resolve: (release: () => void) => void;
  }> = [];
  #activeBytes = 0;
  #activeRanges = 0;
  #requestedBytes = 0;
  #rangeRequests = 0;

  private constructor(options: {
    budget: PublicCidStreamingVerificationBudget;
    cid: string;
    signal?: AbortSignal;
    transport: PublicCidRangeTransport;
    byteLength: number;
  }) {
    this.#budget = options.budget;
    this.#cid = options.cid;
    this.#signal = options.signal;
    this.#transport = options.transport;
    this.byteLength = options.byteLength;
  }

  static async create(options: {
    budget: PublicCidStreamingVerificationBudget;
    cid: string;
    expectedByteLength: number;
    signal?: AbortSignal;
    transport: PublicCidRangeTransport;
  }): Promise<BoundedPublicCidAsyncBuffer> {
    validateBudget(options.budget);
    validateStreamingLimits(options.budget);
    positiveInteger(options.expectedByteLength, "expected object size");
    if (options.expectedByteLength > options.budget.maximumObjectBytes) {
      throw rangeError(
        "artifact_too_large",
        "Public artifact exceeds its plan-derived bound",
      );
    }
    const metadata = await options.transport.statCid(
      options.cid,
      options.budget.maximumObjectBytes,
      options.signal,
    );
    if (
      metadata.cid !== options.cid ||
      metadata.byteLength !== options.expectedByteLength ||
      !metadata.acceptsByteRanges
    ) {
      throw rangeError(
        metadata.acceptsByteRanges ? "artifact_invalid" : "range_unsupported",
        "Public artifact size, CID, or range support does not match the immutable plan",
      );
    }
    return new BoundedPublicCidAsyncBuffer({
      budget: options.budget,
      byteLength: metadata.byteLength,
      cid: options.cid,
      transport: options.transport,
      ...(options.signal ? { signal: options.signal } : {}),
    });
  }

  metrics(): PublicCidRangeMetrics {
    return {
      requestedBytes: this.#requestedBytes,
      rangeRequests: this.#rangeRequests,
    };
  }

  async slice(start: number, end = this.byteLength): Promise<ArrayBuffer> {
    nonNegativeInteger(start, "range start");
    nonNegativeInteger(end, "range end");
    if (end < start || end > this.byteLength) {
      throw rangeError("range_invalid", "Public artifact range is invalid");
    }
    if (end === start) return new ArrayBuffer(0);
    const requestedBytes = end - start;
    if (requestedBytes > this.#budget.maximumRangeBytes) {
      throw rangeError(
        "artifact_too_large",
        "Public artifact range exceeds its per-request bound",
      );
    }
    if (
      this.#rangeRequests >= this.#budget.maximumRanges ||
      this.#requestedBytes + requestedBytes >
        this.#budget.maximumTotalRangeBytes
    ) {
      throw rangeError(
        "range_budget_exhausted",
        "Public artifact range budget is exhausted",
      );
    }
    // Reserve synchronously so concurrent slices cannot overrun the shared budget.
    this.#rangeRequests += 1;
    this.#requestedBytes += requestedBytes;
    const release = await this.#acquire(requestedBytes);
    try {
      const value = await this.#transport.readCidRange(
        this.#cid,
        start,
        end,
        this.byteLength,
        this.#budget.maximumRangeBytes,
        this.#signal,
      );
      if (value.byteLength !== requestedBytes) {
        throw rangeError(
          "artifact_invalid",
          "Public range transport returned an inconsistent byte count",
        );
      }
      if (
        value.buffer instanceof ArrayBuffer &&
        value.byteOffset === 0 &&
        value.byteLength === value.buffer.byteLength
      ) {
        return value.buffer;
      }
      return value.buffer.slice(
        value.byteOffset,
        value.byteOffset + value.byteLength,
      ) as ArrayBuffer;
    } finally {
      release();
    }
  }

  async #acquire(bytes: number): Promise<() => void> {
    throwIfAborted(this.#signal);
    return new Promise<() => void>((resolve, reject) => {
      const waiter = { bytes, cleanup: () => undefined, reject, resolve };
      const abort = (): void => {
        const index = this.#waiters.indexOf(waiter);
        if (index >= 0) this.#waiters.splice(index, 1);
        reject(this.#signal?.reason ?? new Error("Operation aborted"));
        this.#pump();
      };
      waiter.cleanup = () => {
        this.#signal?.removeEventListener("abort", abort);
      };
      this.#waiters.push(waiter);
      this.#signal?.addEventListener("abort", abort, { once: true });
      this.#pump();
    });
  }

  #pump(): void {
    while (
      this.#activeRanges < this.#budget.maximumConcurrency &&
      this.#waiters.length > 0
    ) {
      const waiter = this.#waiters[0]!;
      if (
        this.#activeBytes + waiter.bytes >
        this.#budget.maximumBufferedBytes
      ) {
        return;
      }
      this.#waiters.shift();
      waiter.cleanup();
      this.#activeRanges += 1;
      this.#activeBytes += waiter.bytes;
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true;
        this.#activeRanges -= 1;
        this.#activeBytes -= waiter.bytes;
        this.#pump();
      });
    }
  }
}

function validateBudget(budget: PublicCidRangeBudget): void {
  positiveInteger(budget.maximumObjectBytes, "maximum object bytes");
  positiveInteger(budget.maximumRangeBytes, "maximum range bytes");
  positiveInteger(budget.maximumRanges, "maximum ranges");
  positiveInteger(budget.maximumTotalRangeBytes, "maximum total range bytes");
  if (
    budget.maximumRangeBytes > budget.maximumObjectBytes ||
    budget.maximumTotalRangeBytes >
      budget.maximumRangeBytes * budget.maximumRanges
  ) {
    throw rangeError(
      "configuration_invalid",
      "Public artifact range budget is inconsistent",
    );
  }
}

function validateStreamingLimits(
  budget: PublicCidStreamingVerificationBudget,
): void {
  positiveInteger(budget.maximumConcurrency, "range concurrency");
  positiveInteger(budget.maximumBufferedBytes, "range buffered bytes");
  if (
    budget.maximumConcurrency > 4 ||
    budget.maximumBufferedBytes > 32 * 1024 * 1024 ||
    budget.maximumBufferedBytes < budget.maximumRangeBytes
  ) {
    throw rangeError(
      "configuration_invalid",
      "Public streaming range limits are invalid",
    );
  }
}

export async function verifyParquetMagicByRange(
  file: AsyncBuffer,
): Promise<void> {
  if (file.byteLength < 8) {
    throw rangeError("artifact_invalid", "Published Parquet is corrupt");
  }
  const [header, footer] = await Promise.all([
    file.slice(0, 4),
    file.slice(file.byteLength - 4, file.byteLength),
  ]);
  if (
    Buffer.from(header).toString("ascii") !== "PAR1" ||
    Buffer.from(footer).toString("ascii") !== "PAR1"
  ) {
    throw rangeError("artifact_invalid", "Published Parquet is corrupt");
  }
}

/**
 * Verifies an immutable Parquet artifact without ever materializing the whole
 * file. Ranges may resolve concurrently, but bytes are fed to SHA-256 in
 * deterministic offset order.
 */
export async function verifyParquetSha256ByRange(options: {
  budget: PublicCidStreamingVerificationBudget;
  cid: string;
  expectedByteLength: number;
  expectedSha256: string;
  signal?: AbortSignal;
  transport: PublicCidRangeTransport;
}): Promise<PublicCidRangeMetrics & { sha256: string }> {
  validateBudget(options.budget);
  validateStreamingLimits(options.budget);
  const maximumConcurrency = options.budget.maximumConcurrency;
  const maximumBufferedBytes = options.budget.maximumBufferedBytes;
  if (!/^[a-f0-9]{64}$/.test(options.expectedSha256)) {
    throw rangeError(
      "configuration_invalid",
      "Public range verification limits are invalid",
    );
  }
  positiveInteger(options.expectedByteLength, "expected object size");
  if (
    options.expectedByteLength < 8 ||
    options.expectedByteLength > options.budget.maximumObjectBytes ||
    options.expectedByteLength > options.budget.maximumTotalRangeBytes
  ) {
    throw rangeError(
      "artifact_too_large",
      "Public artifact exceeds its verification budget",
    );
  }
  const rangeCount = Math.ceil(
    options.expectedByteLength / options.budget.maximumRangeBytes,
  );
  if (rangeCount > options.budget.maximumRanges) {
    throw rangeError(
      "range_budget_exhausted",
      "Public artifact range count exceeds its verification budget",
    );
  }
  const metadata = await options.transport.statCid(
    options.cid,
    options.budget.maximumObjectBytes,
    options.signal,
  );
  if (
    metadata.cid !== options.cid ||
    metadata.byteLength !== options.expectedByteLength ||
    !metadata.acceptsByteRanges
  ) {
    throw rangeError(
      "range_unsupported",
      "Public artifact does not expose the required immutable byte ranges",
    );
  }

  const hash = createHash("sha256");
  let requestedBytes = 0;
  let rangeRequests = 0;
  let nextOffset = 0;
  let firstFour = Buffer.alloc(0);
  let lastFour = Buffer.alloc(0);
  while (nextOffset < options.expectedByteLength) {
    const batch: Array<{ end: number; start: number }> = [];
    let batchBytes = 0;
    while (
      batch.length < maximumConcurrency &&
      nextOffset < options.expectedByteLength
    ) {
      const end = Math.min(
        nextOffset + options.budget.maximumRangeBytes,
        options.expectedByteLength,
      );
      const size = end - nextOffset;
      if (batch.length > 0 && batchBytes + size > maximumBufferedBytes) break;
      batch.push({ end, start: nextOffset });
      batchBytes += size;
      nextOffset = end;
    }
    if (batch.length === 0) {
      throw rangeError(
        "configuration_invalid",
        "Public range verification cannot schedule a bounded batch",
      );
    }
    const values = await Promise.all(
      batch.map(async (range) => {
        const bytes = await options.transport.readCidRange(
          options.cid,
          range.start,
          range.end,
          options.expectedByteLength,
          options.budget.maximumRangeBytes,
          options.signal,
        );
        if (bytes.byteLength !== range.end - range.start) {
          throw rangeError(
            "artifact_invalid",
            "Public range transport returned an inconsistent byte count",
          );
        }
        return { ...range, bytes };
      }),
    );
    for (const value of values.sort(
      (left, right) => left.start - right.start,
    )) {
      if (firstFour.byteLength < 4) {
        const required = 4 - firstFour.byteLength;
        firstFour = Buffer.concat(
          [firstFour, Buffer.from(value.bytes.subarray(0, required))],
          Math.min(4, firstFour.byteLength + value.bytes.byteLength),
        );
      }
      const tail = Buffer.from(value.bytes.subarray(-4));
      lastFour =
        tail.byteLength === 4
          ? tail
          : Buffer.from(Buffer.concat([lastFour, tail]).subarray(-4));
      hash.update(value.bytes);
      requestedBytes += value.bytes.byteLength;
      rangeRequests += 1;
    }
  }
  if (
    firstFour.toString("ascii") !== "PAR1" ||
    lastFour.toString("ascii") !== "PAR1"
  ) {
    throw rangeError("artifact_invalid", "Published Parquet is corrupt");
  }
  const actualSha256 = hash.digest("hex");
  if (actualSha256 !== options.expectedSha256) {
    throw rangeError("hash_mismatch", "Published Parquet hash mismatch");
  }
  return { requestedBytes, rangeRequests, sha256: actualSha256 };
}
