import { performance } from "node:perf_hooks";

import { multihashToIPNSRoutingKey, unmarshalIPNSRecord } from "ipns";
import { ipnsValidator } from "ipns/validator";
import { CID } from "multiformats/cid";
import { base36 } from "multiformats/bases/base36";
import type { MultihashDigest } from "multiformats/hashes/interface";
import { z } from "zod";

import { sha256 } from "../lib/hash.js";

export const DELEGATED_IPFS_ORIGIN = "https://delegated-ipfs.dev" as const;
export const DELEGATED_IPNS_MEDIA_TYPE =
  "application/vnd.ipfs.ipns-record" as const;
export const DELEGATED_IPNS_POLICY_VERSION =
  "candidate_signed_ipns_observation_v1" as const;
export const MAX_SIGNED_IPNS_RECORD_BYTES = 10 * 1024;

const cidSchema = z.union([
  z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const networkKeySchema = z.string().regex(/^k51[0-9a-z]{59}$/);
const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);

export const delegatedIpnsEvidenceSchema = z.strictObject({
  endpointType: z.literal("ipfs_delegated_routing_v1"),
  httpStatus: z.number().int().min(100).max(599).nullable(),
  latencyMs: z.number().int().min(0).max(120_000),
  observedAt: z.string().datetime(),
  observedCid: cidSchema.nullable(),
  outcome: z.enum([
    "validated",
    "unavailable",
    "http_error",
    "timeout",
    "transport_error",
    "redirect_rejected",
    "content_type_invalid",
    "response_too_large",
    "malformed_record",
    "invalid_signature",
    "identity_mismatch",
    "expired_record",
    "unexpected_cid",
  ]),
  requestCount: z.number().int().min(1).max(3),
  responseBytes: z.number().int().min(0).max(MAX_SIGNED_IPNS_RECORD_BYTES),
  responseSha256: sha256Schema,
  schemaVersion: z.literal(DELEGATED_IPNS_POLICY_VERSION),
  sequence: z
    .string()
    .regex(/^(0|[1-9][0-9]{0,19})$/)
    .nullable(),
  ttlNanoseconds: z
    .string()
    .regex(/^(0|[1-9][0-9]{0,19})$/)
    .nullable(),
  validationResult: z.enum([
    "valid_target",
    "valid_prior",
    "unexpected_cid",
    "unavailable",
    "http_error",
    "timeout",
    "transport_error",
    "redirect_rejected",
    "content_type_invalid",
    "response_too_large",
    "malformed_record",
    "invalid_signature",
    "identity_mismatch",
    "expired_record",
  ]),
  validity: z.string().min(20).max(64).nullable(),
});

export type DelegatedIpnsEvidence = z.infer<typeof delegatedIpnsEvidenceSchema>;

interface ObserveDelegatedIpnsOptions {
  expectedPriorCid: string;
  expectedTargetCid: string;
  fetchImpl?: typeof fetch;
  maxRetries: number;
  networkKey: string;
  retryDelay?: (delayMs: number, signal?: AbortSignal) => Promise<void>;
  signal?: AbortSignal;
  timeoutMs: number;
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

function elapsed(startedAt: number): number {
  return Math.min(
    120_000,
    Math.max(0, Math.round(performance.now() - startedAt)),
  );
}

function baseEvidence(
  startedAt: number,
  observedAt: string,
  requestCount: number,
): Pick<
  DelegatedIpnsEvidence,
  "endpointType" | "latencyMs" | "observedAt" | "requestCount" | "schemaVersion"
> {
  return {
    endpointType: "ipfs_delegated_routing_v1",
    latencyMs: elapsed(startedAt),
    observedAt,
    requestCount,
    schemaVersion: DELEGATED_IPNS_POLICY_VERSION,
  };
}

function emptyEvidence(
  startedAt: number,
  observedAt: string,
  requestCount: number,
  values: Pick<
    DelegatedIpnsEvidence,
    "httpStatus" | "outcome" | "validationResult"
  >,
): DelegatedIpnsEvidence {
  return delegatedIpnsEvidenceSchema.parse({
    ...baseEvidence(startedAt, observedAt, requestCount),
    ...values,
    observedCid: null,
    responseBytes: 0,
    responseSha256: sha256(Buffer.alloc(0)),
    sequence: null,
    ttlNanoseconds: null,
    validity: null,
  });
}

function parseNetworkKey(networkKey: string): CID {
  const parsed = CID.parse(networkKeySchema.parse(networkKey), base36.decoder);
  if (parsed.version !== 1 || parsed.code !== 0x72) {
    throw new Error("Candidate IPNS identity is not a libp2p-key CID");
  }
  return parsed;
}

async function boundedBytes(response: Response): Promise<Uint8Array> {
  const contentLength = response.headers.get("content-length");
  if (
    contentLength !== null &&
    (!/^\d+$/.test(contentLength) ||
      Number(contentLength) > MAX_SIGNED_IPNS_RECORD_BYTES)
  ) {
    throw new RangeError("signed IPNS record exceeds the response limit");
  }
  if (!response.body) throw new TypeError("signed IPNS response is empty");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > MAX_SIGNED_IPNS_RECORD_BYTES) {
      await reader.cancel();
      throw new RangeError("signed IPNS record exceeds the response limit");
    }
    chunks.push(next.value);
  }
  return Buffer.concat(chunks, total);
}

function validationFailure(
  error: unknown,
): Pick<DelegatedIpnsEvidence, "outcome" | "validationResult"> {
  const name = error instanceof Error ? error.constructor.name : "";
  if (name === "RecordExpiredError") {
    return { outcome: "expired_record", validationResult: "expired_record" };
  }
  if (name === "SignatureVerificationError") {
    return {
      outcome: "invalid_signature",
      validationResult: "invalid_signature",
    };
  }
  if (name === "InvalidEmbeddedPublicKeyError") {
    return {
      outcome: "identity_mismatch",
      validationResult: "identity_mismatch",
    };
  }
  if (name === "RecordTooLargeError" || error instanceof RangeError) {
    return {
      outcome: "response_too_large",
      validationResult: "response_too_large",
    };
  }
  return { outcome: "malformed_record", validationResult: "malformed_record" };
}

export async function observeDelegatedIpnsRecord(
  options: ObserveDelegatedIpnsOptions,
): Promise<DelegatedIpnsEvidence> {
  const networkKey = networkKeySchema.parse(options.networkKey);
  const expectedPriorCid = cidSchema.parse(options.expectedPriorCid);
  const expectedTargetCid = cidSchema.parse(options.expectedTargetCid);
  const timeoutMs = z
    .number()
    .int()
    .min(250)
    .max(30_000)
    .parse(options.timeoutMs);
  const maxRetries = z
    .union([z.literal(0), z.literal(1), z.literal(2)])
    .parse(options.maxRetries);
  const identity = parseNetworkKey(networkKey);
  if (identity.multihash.code !== 0 && identity.multihash.code !== 0x12) {
    throw new Error("Candidate IPNS identity uses an unsupported multihash");
  }
  const routingKey = multihashToIPNSRoutingKey(
    identity.multihash as MultihashDigest<0 | 0x12>,
  );
  const endpoint = new URL(
    `/routing/v1/ipns/${encodeURIComponent(networkKey)}`,
    DELEGATED_IPFS_ORIGIN,
  );
  const fetchImpl = options.fetchImpl ?? fetch;
  const observedAt = new Date().toISOString();
  const startedAt = performance.now();
  let requestCount = 0;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Operation aborted");
    }
    requestCount += 1;
    let response: Response;
    try {
      response = await fetchImpl(endpoint, {
        headers: { Accept: DELEGATED_IPNS_MEDIA_TYPE },
        method: "GET",
        redirect: "manual",
        signal: options.signal
          ? AbortSignal.any([options.signal, AbortSignal.timeout(timeoutMs)])
          : AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      if (options.signal?.aborted) {
        throw options.signal.reason ?? new Error("Operation aborted");
      }
      const timeout =
        error instanceof DOMException &&
        (error.name === "AbortError" || error.name === "TimeoutError");
      if (attempt < maxRetries) {
        await (options.retryDelay ?? defaultRetryDelay)(
          50 * (attempt + 1),
          options.signal,
        );
        continue;
      }
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: null,
        outcome: timeout ? "timeout" : "transport_error",
        validationResult: timeout ? "timeout" : "transport_error",
      });
    }

    if (response.status >= 300 && response.status < 400) {
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: response.status,
        outcome: "redirect_rejected",
        validationResult: "redirect_rejected",
      });
    }
    if (response.status === 404) {
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: response.status,
        outcome: "unavailable",
        validationResult: "unavailable",
      });
    }
    if (!response.ok) {
      if (
        attempt < maxRetries &&
        [500, 502, 503, 504].includes(response.status)
      ) {
        await (options.retryDelay ?? defaultRetryDelay)(
          50 * (attempt + 1),
          options.signal,
        );
        continue;
      }
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: response.status,
        outcome: "http_error",
        validationResult: "http_error",
      });
    }
    const contentType = response.headers
      .get("content-type")
      ?.split(";", 1)[0]
      ?.trim();
    if (contentType !== DELEGATED_IPNS_MEDIA_TYPE) {
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: response.status,
        outcome: "content_type_invalid",
        validationResult: "content_type_invalid",
      });
    }

    let bytes: Uint8Array;
    try {
      bytes = await boundedBytes(response);
    } catch (error) {
      const failure = validationFailure(error);
      return emptyEvidence(startedAt, observedAt, requestCount, {
        httpStatus: response.status,
        ...failure,
      });
    }
    const responseSha256 = sha256(Buffer.from(bytes));
    try {
      await ipnsValidator(routingKey, bytes);
      const record = unmarshalIPNSRecord(bytes);
      const match = record.value.match(
        /^\/ipfs\/(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$/,
      );
      if (!match)
        throw new TypeError("signed IPNS value is not one immutable CID");
      const observedCid = cidSchema.parse(match[1]);
      CID.parse(observedCid);
      const validationResult =
        observedCid === expectedTargetCid
          ? "valid_target"
          : observedCid === expectedPriorCid
            ? "valid_prior"
            : "unexpected_cid";
      return delegatedIpnsEvidenceSchema.parse({
        ...baseEvidence(startedAt, observedAt, requestCount),
        httpStatus: response.status,
        observedCid,
        outcome:
          validationResult === "unexpected_cid"
            ? "unexpected_cid"
            : "validated",
        responseBytes: bytes.byteLength,
        responseSha256,
        sequence: record.sequence.toString(),
        ttlNanoseconds: record.ttl?.toString() ?? null,
        validationResult,
        validity: record.validity,
      });
    } catch (error) {
      const failure = validationFailure(error);
      return delegatedIpnsEvidenceSchema.parse({
        ...baseEvidence(startedAt, observedAt, requestCount),
        httpStatus: response.status,
        observedCid: null,
        ...failure,
        responseBytes: bytes.byteLength,
        responseSha256,
        sequence: null,
        ttlNanoseconds: null,
        validity: null,
      });
    }
  }
  throw new Error("Delegated IPNS retry accounting is invalid");
}
