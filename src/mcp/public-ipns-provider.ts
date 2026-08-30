import { createHash } from "node:crypto";

import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

import type { PublicIpnsProviderConfig } from "./config.js";
import { MCP_CONTRACT_VERSION, MCP_SCHEMA_SHA256 } from "./constants.js";
import type { McpContractRegistry } from "./contracts.js";
import type {
  DatasetMetadata,
  JsonObject,
  OracleMcpProvider,
  QueryPropertyRow,
} from "./provider.js";
import { CIDV0_PATTERN, verifyIpfsCid } from "../publication/ipfs-cid.js";
import {
  observeDelegatedIpnsRecord,
  type DelegatedIpnsEvidence,
} from "../publication/delegated-ipns.js";
import {
  type PublicationArtifact,
  type PublicationPlan,
  validatePublicationPlan,
} from "../publication/plan.js";

const IPNS_ID_PATTERN = /^k51[a-z0-9]{20,120}$/;
const CIDV1_BASE32_PATTERN = /^b[a-z2-7]{20,120}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const CANONICAL_SCHEMA_SHA256 =
  "59c6472c2cd6d18041cf72c779fb970a082b00bef09aea724b99687e84198306";
const FIXTURE_PROPERTY_IDS = new Set([
  "property_e72ba795455c19d71ce4cb11f6177a5e",
]);
const PUBLIC_GATEWAYS = [
  { id: "ipfs_io", origin: "https://ipfs.io" },
  { id: "dweb_link", origin: "https://dweb.link" },
] as const;
const FILEBASE_PUBLIC_GATEWAY_ORIGIN = "https://ipfs.filebase.io" as const;
const FILEBASE_ARTIFACT_GATEWAYS = [
  { id: "filebase_public_gateway", origin: FILEBASE_PUBLIC_GATEWAY_ORIGIN },
] as const;
const RETRY_BACKOFF_MS = 50;

type PublicGateway = { id: string; origin: string };
type RetryDelay = (milliseconds: number) => Promise<void>;

const defaultRetryDelay: RetryDelay = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

export type PublicReadErrorCode =
  | "artifact_invalid"
  | "artifact_too_large"
  | "configuration_invalid"
  | "contract_mismatch"
  | "fixture_rejected"
  | "hash_mismatch"
  | "ipns_missing"
  | "ipns_split"
  | "ipns_stale"
  | "ipns_unexpected"
  | "redirect_rejected"
  | "schema_mismatch"
  | "timeout"
  | "transport_unavailable";

export class PublicReadError extends Error {
  constructor(
    readonly code: PublicReadErrorCode,
    message: string,
    readonly retryable = false,
  ) {
    super(message);
  }
}

export interface IpnsResolutionObservation {
  cacheAgeSeconds: number | null;
  cid: string | null;
  observedAt: string;
  resolver: string;
  status: "resolved" | "unavailable";
}

export interface PublicReadTransport {
  readCid(
    cid: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array>;
  resolveIpns(
    identity: string,
    signal?: AbortSignal,
  ): Promise<readonly IpnsResolutionObservation[]>;
}

export type PublicProviderInitializationStage =
  "graph" | "ipns_resolution" | "manifest" | "parquet" | "plan";

function publicError(
  code: PublicReadErrorCode,
  message: string,
  retryable = false,
): PublicReadError {
  return new PublicReadError(code, message, retryable);
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

function responseCid(headers: Headers, location: string | null): string | null {
  const candidates = [
    headers.get("x-ipfs-roots"),
    headers.get("x-ipfs-path"),
    location,
  ]
    .filter((value): value is string => value !== null)
    .flatMap((value) => value.split(/[\s,]+/));
  return (
    candidates
      .flatMap(
        (value) =>
          value.match(/Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/g) ?? [],
      )
      .find(
        (value) =>
          CIDV0_PATTERN.test(value) || CIDV1_BASE32_PATTERN.test(value),
      ) ?? null
  );
}

function allowedGatewayUrl(
  value: URL,
  kind: "ipfs" | "ipns",
  gateways: readonly PublicGateway[],
): boolean {
  return (
    value.protocol === "https:" &&
    gateways.some((gateway) => gateway.origin === value.origin) &&
    (value.pathname.startsWith(`/${kind}/`) ||
      (kind === "ipns" && value.pathname.startsWith("/ipfs/"))) &&
    value.username === "" &&
    value.password === "" &&
    value.search === "" &&
    value.hash === ""
  );
}

async function boundedBody(
  response: Response,
  maximumBytes: number,
): Promise<Uint8Array> {
  const declared = Number(response.headers.get("content-length") ?? 0);
  if (Number.isFinite(declared) && declared > maximumBytes) {
    throw publicError(
      "artifact_too_large",
      "Public artifact exceeds its bound",
    );
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel();
      throw publicError(
        "artifact_too_large",
        "Public artifact exceeds its bound",
      );
    }
    chunks.push(result.value);
  }
  const value = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    value.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return value;
}

export class HttpPublicReadTransport implements PublicReadTransport {
  readonly #gateways: readonly PublicGateway[];
  readonly #retryDelay: RetryDelay;

  constructor(
    readonly limits: PublicIpnsProviderConfig["limits"],
    readonly fetchImplementation: typeof fetch = fetch,
    options: {
      gateways?: readonly PublicGateway[];
      retryDelay?: RetryDelay;
    } = {},
  ) {
    this.#gateways = options.gateways ?? PUBLIC_GATEWAYS;
    this.#retryDelay = options.retryDelay ?? defaultRetryDelay;
  }

  async resolveIpns(
    identity: string,
    signal?: AbortSignal,
  ): Promise<readonly IpnsResolutionObservation[]> {
    return Promise.all(
      this.#gateways.map(async (gateway) => {
        const response = await this.#requestWithRetry(
          new URL(`/ipns/${identity}`, gateway.origin),
          "HEAD",
          "ipns",
          signal,
        );
        const cid = responseCid(
          response.headers,
          response.headers.get("location") ?? response.url,
        );
        const age = response.headers.get("age");
        const cacheAgeSeconds = age === null ? null : Number(age);
        return {
          cacheAgeSeconds:
            cacheAgeSeconds !== null &&
            Number.isInteger(cacheAgeSeconds) &&
            cacheAgeSeconds >= 0
              ? cacheAgeSeconds
              : null,
          cid,
          observedAt: new Date().toISOString(),
          resolver: gateway.id,
          status: cid === null ? "unavailable" : "resolved",
        } as const;
      }),
    );
  }

  async readCid(
    cid: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    let lastError: unknown;
    for (const gateway of this.#gateways) {
      for (let attempt = 0; attempt <= this.limits.retries; attempt += 1) {
        try {
          const response = await this.#request(
            new URL(`/ipfs/${cid}`, gateway.origin),
            "GET",
            "ipfs",
            signal,
          );
          return await boundedBody(response, maximumBytes);
        } catch (error) {
          const failure = isAbortError(error)
            ? publicError("timeout", "Public read timed out", true)
            : error;
          lastError = failure;
          if (!(failure instanceof PublicReadError) || !failure.retryable) {
            throw failure;
          }
          if (attempt < this.limits.retries) {
            await this.#retryDelay(RETRY_BACKOFF_MS * (attempt + 1));
          }
        }
      }
    }
    if (lastError instanceof PublicReadError) throw lastError;
    throw publicError(
      "transport_unavailable",
      "Public artifact transport is unavailable",
    );
  }

  async #requestWithRetry(
    initial: URL,
    method: "GET" | "HEAD",
    kind: "ipfs" | "ipns",
    signal?: AbortSignal,
  ): Promise<Response> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.limits.retries; attempt += 1) {
      try {
        return await this.#request(initial, method, kind, signal);
      } catch (error) {
        lastError = error;
        if (!(error instanceof PublicReadError) || !error.retryable) {
          throw error;
        }
        if (attempt < this.limits.retries) {
          await this.#retryDelay(RETRY_BACKOFF_MS * (attempt + 1));
        }
      }
    }
    if (lastError instanceof PublicReadError) throw lastError;
    throw publicError(
      "transport_unavailable",
      "Public gateway request failed",
      true,
    );
  }

  async #request(
    initial: URL,
    method: "GET" | "HEAD",
    kind: "ipfs" | "ipns",
    signal?: AbortSignal,
  ): Promise<Response> {
    let current = initial;
    for (
      let redirectCount = 0;
      redirectCount <= this.limits.maxRedirects;
      redirectCount += 1
    ) {
      let response: Response;
      try {
        response = await this.fetchImplementation(current, {
          method,
          redirect: "manual",
          signal: combinedSignal(signal, this.limits.transportTimeoutMs),
        });
      } catch (error) {
        if (isAbortError(error)) {
          throw publicError("timeout", "Public read timed out", true);
        }
        throw publicError(
          "transport_unavailable",
          "Public gateway request failed",
          true,
        );
      }
      if (response.status >= 300 && response.status < 400) {
        const location = response.headers.get("location");
        if (location === null || redirectCount === this.limits.maxRedirects) {
          throw publicError(
            "redirect_rejected",
            "Public redirect was rejected",
          );
        }
        const target = new URL(location, current);
        if (!allowedGatewayUrl(target, kind, this.#gateways)) {
          throw publicError(
            "redirect_rejected",
            "Public redirect was rejected",
          );
        }
        current = target;
        continue;
      }
      if (!response.ok) {
        const retryable = [500, 502, 503, 504].includes(response.status);
        throw publicError(
          "transport_unavailable",
          "Public gateway returned an unavailable response",
          retryable,
        );
      }
      return response;
    }
    throw publicError("redirect_rejected", "Public redirect was rejected");
  }
}

function delegatedFailure(evidence: DelegatedIpnsEvidence): PublicReadError {
  if (
    evidence.validationResult === "valid_prior" ||
    evidence.validationResult === "unexpected_cid"
  ) {
    return publicError(
      "ipns_unexpected",
      "Signed public IPNS record resolved to an unexpected CID",
    );
  }
  if (evidence.validationResult === "expired_record") {
    return publicError("ipns_stale", "Signed public IPNS record is expired");
  }
  if (evidence.validationResult === "redirect_rejected") {
    return publicError(
      "redirect_rejected",
      "Signed public IPNS redirect was rejected",
    );
  }
  if (evidence.validationResult === "timeout") {
    return publicError("timeout", "Signed public IPNS resolution timed out");
  }
  return publicError(
    "transport_unavailable",
    "Signed public IPNS record could not be validated",
  );
}

export class CandidateDelegatedPublicReadTransport implements PublicReadTransport {
  readonly #artifactTransport: HttpPublicReadTransport;
  readonly #expectedByIdentity: ReadonlyMap<string, string>;
  readonly #fetchImplementation: typeof fetch;
  readonly #limits: PublicIpnsProviderConfig["limits"];
  readonly #retryDelay: RetryDelay;

  constructor(
    config: PublicIpnsProviderConfig,
    fetchImplementation: typeof fetch = fetch,
    retryDelay: RetryDelay = defaultRetryDelay,
  ) {
    if (config.resolverPolicy !== "candidate_filebase_delegated_v2") {
      throw publicError(
        "configuration_invalid",
        "Candidate delegated transport requires its exact resolver policy",
      );
    }
    this.#limits = config.limits;
    this.#fetchImplementation = fetchImplementation;
    this.#retryDelay = retryDelay;
    this.#artifactTransport = new HttpPublicReadTransport(
      config.limits,
      fetchImplementation,
      {
        gateways: FILEBASE_ARTIFACT_GATEWAYS,
        retryDelay,
      },
    );
    this.#expectedByIdentity = new Map([
      [config.openDataIpns, config.expectedOpenDataRootCid],
      [config.queryTableIpns, config.expectedQueryTableRootCid],
    ]);
  }

  async readCid(
    cid: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    return this.#artifactTransport.readCid(cid, maximumBytes, signal);
  }

  async resolveIpns(
    identity: string,
    signal?: AbortSignal,
  ): Promise<readonly IpnsResolutionObservation[]> {
    const expectedCid = this.#expectedByIdentity.get(identity);
    if (!expectedCid) {
      throw publicError(
        "configuration_invalid",
        "Candidate IPNS identity is not configured",
      );
    }
    const [gateway, delegated] = await Promise.all([
      this.#resolveFilebaseGateway(identity, signal),
      observeDelegatedIpnsRecord({
        expectedPriorCid: expectedCid,
        expectedTargetCid: expectedCid,
        fetchImpl: this.#fetchImplementation,
        maxRetries: this.#limits.retries === 0 ? 0 : 1,
        networkKey: identity,
        timeoutMs: this.#limits.transportTimeoutMs,
      }),
    ]);
    if (
      delegated.validationResult !== "valid_target" ||
      delegated.observedCid !== expectedCid
    ) {
      throw delegatedFailure(delegated);
    }
    return [
      gateway,
      {
        cacheAgeSeconds: null,
        cid: delegated.observedCid,
        observedAt: delegated.observedAt,
        resolver: "ipfs_delegated_signed_record",
        status: "resolved",
      },
    ];
  }

  async #resolveFilebaseGateway(
    identity: string,
    signal?: AbortSignal,
  ): Promise<IpnsResolutionObservation> {
    const url = new URL(
      `/ipns/${encodeURIComponent(identity)}`,
      FILEBASE_PUBLIC_GATEWAY_ORIGIN,
    );
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.#limits.retries; attempt += 1) {
      try {
        const response = await this.#fetchImplementation(url, {
          method: "HEAD",
          redirect: "manual",
          signal: combinedSignal(signal, this.#limits.transportTimeoutMs),
        });
        if (response.status >= 400) {
          const retryable = [500, 502, 503, 504].includes(response.status);
          const error = publicError(
            "transport_unavailable",
            "Official Filebase gateway is unavailable",
            retryable,
          );
          if (!retryable) throw error;
          lastError = error;
          if (attempt < this.#limits.retries) {
            await this.#retryDelay(RETRY_BACKOFF_MS * (attempt + 1));
          }
          continue;
        }
        const location = response.headers.get("location");
        if (location) {
          const redirect = new URL(location, url);
          if (
            redirect.origin !== FILEBASE_PUBLIC_GATEWAY_ORIGIN ||
            !redirect.pathname.startsWith("/ipfs/") ||
            redirect.username !== "" ||
            redirect.password !== "" ||
            redirect.search !== "" ||
            redirect.hash !== ""
          ) {
            throw publicError(
              "redirect_rejected",
              "Official Filebase gateway redirect was rejected",
            );
          }
        }
        const cid = responseCid(response.headers, location);
        return {
          cacheAgeSeconds: null,
          cid,
          observedAt: new Date().toISOString(),
          resolver: "filebase_public_gateway",
          status: cid === null ? "unavailable" : "resolved",
        };
      } catch (error) {
        if (error instanceof PublicReadError) {
          if (!error.retryable) throw error;
          lastError = error;
          if (attempt < this.#limits.retries) {
            await this.#retryDelay(RETRY_BACKOFF_MS * (attempt + 1));
          }
          continue;
        }
        if (isAbortError(error)) {
          lastError = publicError(
            "timeout",
            "Official Filebase gateway resolution timed out",
            true,
          );
        } else {
          lastError = publicError(
            "transport_unavailable",
            "Official Filebase gateway resolution failed",
            true,
          );
        }
        if (attempt < this.#limits.retries) {
          await this.#retryDelay(RETRY_BACKOFF_MS * (attempt + 1));
        }
      }
    }
    if (lastError instanceof PublicReadError) throw lastError;
    throw publicError(
      "transport_unavailable",
      "Official Filebase gateway is unavailable",
      true,
    );
  }
}

function record(value: unknown, label: string): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw publicError("artifact_invalid", `Published ${label} is invalid`);
  }
  return value as JsonObject;
}

function stringValue(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw publicError("artifact_invalid", `Published ${label} is invalid`);
  }
  return value;
}

function numberValue(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw publicError("artifact_invalid", `Published ${label} is invalid`);
  }
  return value;
}

function nullableNumber(value: unknown, label: string): number | null {
  return value === null ? null : numberValue(value, label);
}

function nullableString(value: unknown, label: string): string | null {
  return value === null ? null : stringValue(value, label);
}

function iso(value: unknown, label: string): string {
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString();
  }
  const text = stringValue(value, label);
  const parsed = Date.parse(text);
  if (!Number.isFinite(parsed)) {
    throw publicError("artifact_invalid", `Published ${label} is invalid`);
  }
  return new Date(parsed).toISOString();
}

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function parseJson(bytes: Uint8Array, label: string): JsonObject {
  try {
    return record(JSON.parse(Buffer.from(bytes).toString("utf8")), label);
  } catch (error) {
    if (error instanceof PublicReadError) throw error;
    throw publicError("artifact_invalid", `Published ${label} is invalid JSON`);
  }
}

async function verifyBytes(
  bytes: Uint8Array,
  expected: { cid: string; sha256: string },
): Promise<void> {
  if (sha256(bytes) !== expected.sha256) {
    throw publicError("hash_mismatch", "Published artifact hash mismatch");
  }
  try {
    await verifyIpfsCid(bytes, expected.cid);
  } catch {
    throw publicError("hash_mismatch", "Published artifact CID mismatch");
  }
}

function validateConfig(config: PublicIpnsProviderConfig): void {
  const cids = [
    config.expectedManifestCid,
    config.expectedOpenDataRootCid,
    config.expectedPlanCid,
    config.expectedQueryTableRootCid,
  ];
  if (
    !IPNS_ID_PATTERN.test(config.openDataIpns) ||
    !IPNS_ID_PATTERN.test(config.queryTableIpns) ||
    cids.some((cid) => !CIDV0_PATTERN.test(cid)) ||
    !SHA256_PATTERN.test(config.expectedManifestSha256) ||
    !SHA256_PATTERN.test(config.expectedPlanSha256) ||
    config.openDataIpns.includes("fixture") ||
    config.queryTableIpns.includes("fixture")
  ) {
    throw publicError(
      "configuration_invalid",
      "Public provider configuration is incomplete or invalid",
    );
  }
  const candidateBindingsValid =
    config.resolverPolicy === "candidate_filebase_delegated_v2" &&
    /^demo_[a-f0-9]{32}$/.test(config.candidateDemoPlanId ?? "") &&
    SHA256_PATTERN.test(config.candidateDemoPlanSha256 ?? "") &&
    SHA256_PATTERN.test(config.candidateDemoSourcePlanSha256 ?? "");
  const canonicalBindingsValid =
    config.resolverPolicy === "public_two_gateway_v1" &&
    config.candidateDemoPlanId === null &&
    config.candidateDemoPlanSha256 === null &&
    config.candidateDemoSourcePlanSha256 === null;
  if (!candidateBindingsValid && !canonicalBindingsValid) {
    throw publicError(
      "configuration_invalid",
      "Public resolver policy and plan bindings are inconsistent",
    );
  }
}

function artifact(
  plan: PublicationPlan,
  domain: PublicationArtifact["domain"],
  objectKey: string,
): PublicationArtifact {
  const value = plan.artifacts.objectInventory.find(
    (entry) => entry.domain === domain && entry.objectKey === objectKey,
  );
  if (!value) {
    throw publicError(
      "artifact_invalid",
      "Publication plan object inventory is incomplete",
    );
  }
  return value;
}

async function resolveExpected(
  transport: PublicReadTransport,
  identity: string,
  expectedCid: string,
  maximumCacheAgeSeconds: number,
  signal?: AbortSignal,
): Promise<void> {
  const observations = await transport.resolveIpns(identity, signal);
  if (
    observations.length < 2 ||
    observations.some(
      (entry) =>
        entry.status !== "resolved" ||
        entry.cid === null ||
        !Number.isFinite(Date.parse(entry.observedAt)),
    )
  ) {
    throw publicError("ipns_missing", "Public IPNS resolution is incomplete");
  }
  if (
    observations.some(
      (entry) =>
        entry.cacheAgeSeconds !== null &&
        entry.cacheAgeSeconds > maximumCacheAgeSeconds,
    )
  ) {
    throw publicError("ipns_stale", "Public IPNS resolution is stale");
  }
  const cids = new Set(observations.map((entry) => entry.cid));
  if (cids.size !== 1) {
    throw publicError("ipns_split", "Public IPNS resolvers disagree");
  }
  if (observations[0]?.cid !== expectedCid) {
    throw publicError(
      "ipns_unexpected",
      "Public IPNS resolved to an unexpected CID",
    );
  }
}

interface GraphEntry {
  byteSize: number;
  canonicalPropertyId: string;
  cid: string;
  parcelIdentifier: string;
  publicPropertyId: string;
  sha256: string;
}

function publicPropertyId(canonicalPropertyId: string): string {
  const match = canonicalPropertyId.match(/^property_([a-f0-9]{32})$/);
  if (!match?.[1]) {
    throw publicError("artifact_invalid", "Published property ID is invalid");
  }
  return `prop_${match[1]}`;
}

const REQUIRED_PARQUET_SCHEMA: Readonly<Record<string, string>> = Object.freeze(
  {
    address_city: "BYTE_ARRAY:UTF8",
    address_street: "BYTE_ARRAY:UTF8",
    address_zip: "BYTE_ARRAY:UTF8",
    assessed_value: "DOUBLE:",
    avm_value: "DOUBLE:",
    built_year: "INT64:INT_64",
    contractor_source_availability: "BYTE_ARRAY:UTF8",
    county_name: "BYTE_ARRAY:UTF8",
    coverage_mode: "BYTE_ARRAY:UTF8",
    coverage_scope_id: "BYTE_ARRAY:UTF8",
    exterior_wall_material: "BYTE_ARRAY:UTF8",
    has_bbb_contractor: "BOOLEAN:",
    has_permits: "BOOLEAN:",
    has_sunbiz_tenant: "BOOLEAN:",
    hoa_flag: "BOOLEAN:",
    last_sale_date: "BYTE_ARRAY:UTF8",
    last_sale_price: "DOUBLE:",
    latitude: "DOUBLE:",
    livable_floor_area: "DOUBLE:",
    longitude: "DOUBLE:",
    lot_area_sqft: "DOUBLE:",
    lot_size_acre: "DOUBLE:",
    mailing_city: "BYTE_ARRAY:UTF8",
    mailing_state: "BYTE_ARRAY:UTF8",
    mailing_zip: "BYTE_ARRAY:UTF8",
    market_value: "DOUBLE:",
    maximum_open_roofing_permit_days: "INT32:INT_32",
    observed_at: "INT64:TIMESTAMP_MICROS",
    open_roofing_permit_count: "INT32:INT_32",
    owner_count: "INT64:INT_64",
    owner_name: "BYTE_ARRAY:UTF8",
    owner_occupied: "BOOLEAN:",
    owners_text: "BYTE_ARRAY:UTF8",
    parcel_identifier: "BYTE_ARRAY:UTF8",
    permit_count: "INT64:INT_64",
    permit_source_availability: "BYTE_ARRAY:UTF8",
    property_cid: "BYTE_ARRAY:UTF8",
    property_document_sha256: "BYTE_ARRAY:UTF8",
    property_id: "BYTE_ARRAY:UTF8",
    property_type: "BYTE_ARRAY:UTF8",
    property_usage_type: "BYTE_ARRAY:UTF8",
    published_at: "INT64:TIMESTAMP_MICROS",
    request_identifier: "BYTE_ARRAY:UTF8",
    roof_age_basis: "BYTE_ARRAY:UTF8",
    roof_age_basis_quality: "BYTE_ARRAY:UTF8",
    roof_age_years: "INT32:INT_32",
    roof_covering_material: "BYTE_ARRAY:UTF8",
    selection_hash: "BYTE_ARRAY:UTF8",
    site_city: "BYTE_ARRAY:UTF8",
    source_run_id: "BYTE_ARRAY:UTF8",
    source_snapshot_id: "BYTE_ARRAY:UTF8",
    source_system: "BYTE_ARRAY:UTF8",
    state_code: "BYTE_ARRAY:UTF8",
    subdivision: "BYTE_ARRAY:UTF8",
    total_area: "DOUBLE:",
  },
);

async function readPublicQueryRows(
  bytes: Uint8Array,
  entriesByCanonicalId: ReadonlyMap<string, GraphEntry>,
  plan: PublicationPlan,
): Promise<QueryPropertyRow[]> {
  try {
    return await readPublicQueryRowsUnchecked(
      bytes,
      entriesByCanonicalId,
      plan,
    );
  } catch (error) {
    if (error instanceof PublicReadError) throw error;
    throw publicError(
      "artifact_invalid",
      "Published Parquet could not be decoded",
    );
  }
}

async function readPublicQueryRowsUnchecked(
  bytes: Uint8Array,
  entriesByCanonicalId: ReadonlyMap<string, GraphEntry>,
  plan: PublicationPlan,
): Promise<QueryPropertyRow[]> {
  if (
    bytes.byteLength < 8 ||
    Buffer.from(bytes.subarray(0, 4)).toString("ascii") !== "PAR1" ||
    Buffer.from(bytes.subarray(-4)).toString("ascii") !== "PAR1"
  ) {
    throw publicError("artifact_invalid", "Published Parquet is corrupt");
  }
  const arrayBuffer = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
  const metadata = await parquetMetadataAsync(arrayBuffer);
  const actual = new Map(
    metadata.schema
      .slice(1)
      .map((entry) => [
        entry.name,
        `${entry.type ?? ""}:${entry.converted_type ?? ""}`,
      ]),
  );
  for (const [column, expected] of Object.entries(REQUIRED_PARQUET_SCHEMA)) {
    if (actual.get(column) !== expected) {
      throw publicError(
        "schema_mismatch",
        `Published Parquet column ${column} is missing or incompatible`,
      );
    }
  }
  if (metadata.num_rows !== BigInt(plan.counts.queryTableRows)) {
    throw publicError(
      "schema_mismatch",
      "Published Parquet cardinality does not match the plan",
    );
  }
  const values = await parquetReadObjects({
    columns: [
      "property_id",
      "property_cid",
      "latitude",
      "longitude",
      "roof_age_years",
      "roof_age_basis",
      "roof_age_basis_quality",
      "open_roofing_permit_count",
      "maximum_open_roofing_permit_days",
      "property_document_sha256",
      "site_city",
      "observed_at",
      "published_at",
      "coverage_mode",
      "coverage_scope_id",
      "source_run_id",
      "source_snapshot_id",
      "selection_hash",
      "permit_source_availability",
      "contractor_source_availability",
    ],
    compressors,
    file: arrayBuffer,
  });
  const rows = values.map((value) => {
    const row = record(value, "Parquet row");
    const canonicalPropertyId = stringValue(
      row.property_id,
      "Parquet property ID",
    );
    const entry = entriesByCanonicalId.get(canonicalPropertyId);
    if (
      !entry ||
      row.property_cid !== entry.cid ||
      row.property_document_sha256 !== entry.sha256 ||
      row.coverage_mode !== plan.coverage.mode ||
      row.coverage_scope_id !== plan.coverage.scopeId ||
      row.source_run_id !== plan.coverage.runId ||
      row.source_snapshot_id !== plan.coverage.sourceSnapshotId ||
      row.selection_hash !== plan.coverage.selection.selectedRecordSha256 ||
      row.permit_source_availability !== "unavailable" ||
      row.contractor_source_availability !== "unavailable" ||
      row.open_roofing_permit_count !== null ||
      row.maximum_open_roofing_permit_days !== null
    ) {
      throw publicError(
        "artifact_invalid",
        "Published Parquet row is not bound to the publication graph",
      );
    }
    return {
      canonicalPropertyId,
      latitude: nullableNumber(row.latitude, "latitude"),
      longitude: nullableNumber(row.longitude, "longitude"),
      maximumOpenRoofingPermitDays: null,
      observedAt: iso(row.observed_at, "observed timestamp"),
      openRoofingPermitCount: null,
      propertyDocumentSha256: entry.sha256,
      propertyId: entry.publicPropertyId,
      publishedAt: nullableString(row.published_at, "published timestamp"),
      roofAgeBasis: stringValue(row.roof_age_basis, "roof age basis"),
      roofAgeBasisQuality: stringValue(
        row.roof_age_basis_quality,
        "roof age quality",
      ),
      roofAgeYears: numberValue(row.roof_age_years, "roof age years"),
      siteCity: stringValue(row.site_city, "site city"),
    } satisfies QueryPropertyRow;
  });
  if (
    rows.length !== entriesByCanonicalId.size ||
    new Set(rows.map((row) => row.propertyId)).size !== rows.length
  ) {
    throw publicError(
      "artifact_invalid",
      "Published Parquet and graph cardinality differ",
    );
  }
  return rows.sort((left, right) =>
    left.propertyId.localeCompare(right.propertyId),
  );
}

function collectEvidenceReferences(
  value: unknown,
  references: Set<string>,
): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectEvidenceReferences(entry, references);
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    if (key === "evidenceRefs" && Array.isArray(entry)) {
      for (const reference of entry) {
        if (typeof reference === "string") references.add(reference);
      }
    } else {
      collectEvidenceReferences(entry, references);
    }
  }
}

export class PublicIpnsProvider implements OracleMcpProvider {
  readonly #contracts: McpContractRegistry;
  readonly #entriesByPublicId: ReadonlyMap<string, GraphEntry>;
  readonly #metadata: DatasetMetadata;
  readonly #provenanceArtifactUris: ReadonlySet<string>;
  readonly #queryRows: readonly QueryPropertyRow[];
  readonly #transport: PublicReadTransport;
  readonly #maximumJsonBytes: number;

  private constructor(options: {
    contracts: McpContractRegistry;
    entriesByPublicId: ReadonlyMap<string, GraphEntry>;
    maximumJsonBytes: number;
    metadata: DatasetMetadata;
    provenanceArtifactUris: ReadonlySet<string>;
    queryRows: readonly QueryPropertyRow[];
    transport: PublicReadTransport;
  }) {
    this.#contracts = options.contracts;
    this.#entriesByPublicId = options.entriesByPublicId;
    this.#maximumJsonBytes = options.maximumJsonBytes;
    this.#metadata = options.metadata;
    this.#provenanceArtifactUris = options.provenanceArtifactUris;
    this.#queryRows = options.queryRows;
    this.#transport = options.transport;
  }

  static async create(
    config: PublicIpnsProviderConfig,
    contracts: McpContractRegistry,
    transport?: PublicReadTransport,
    signal?: AbortSignal,
    onStage?: (stage: PublicProviderInitializationStage) => void,
  ): Promise<PublicIpnsProvider> {
    validateConfig(config);
    const activeTransport =
      transport ??
      (config.resolverPolicy === "candidate_filebase_delegated_v2"
        ? new CandidateDelegatedPublicReadTransport(config)
        : new HttpPublicReadTransport(config.limits));
    onStage?.("ipns_resolution");
    await Promise.all([
      resolveExpected(
        activeTransport,
        config.openDataIpns,
        config.expectedOpenDataRootCid,
        config.limits.maxCacheAgeSeconds,
        signal,
      ),
      resolveExpected(
        activeTransport,
        config.queryTableIpns,
        config.expectedQueryTableRootCid,
        config.limits.maxCacheAgeSeconds,
        signal,
      ),
    ]);

    onStage?.("plan");
    const planBytes = await activeTransport.readCid(
      config.expectedPlanCid,
      config.limits.maxJsonObjectBytes,
      signal,
    );
    await verifyBytes(planBytes, {
      cid: config.expectedPlanCid,
      sha256: config.expectedPlanSha256,
    });
    let plan: PublicationPlan;
    try {
      plan = validatePublicationPlan(parseJson(planBytes, "plan"));
    } catch (error) {
      if (error instanceof PublicReadError) throw error;
      throw publicError(
        "contract_mismatch",
        "Published plan failed strict validation",
      );
    }
    const directTargetBinding =
      plan.targets.openData.ipnsNetworkKey === config.openDataIpns &&
      plan.targets.queryTable.ipnsNetworkKey === config.queryTableIpns;
    const candidateTargetBinding =
      config.resolverPolicy === "candidate_filebase_delegated_v2" &&
      plan.targets.openData.ipnsNetworkKey === null &&
      plan.targets.queryTable.ipnsNetworkKey === null &&
      plan.planSha256 === config.candidateDemoSourcePlanSha256;
    if (
      plan.version !== "1.1.0" ||
      plan.contracts.mcp.version !== MCP_CONTRACT_VERSION ||
      plan.contracts.mcp.sha256 !== MCP_SCHEMA_SHA256 ||
      plan.contracts.canonical.sha256 !== CANONICAL_SCHEMA_SHA256 ||
      plan.artifacts.parquet.schemaSha256 !==
        plan.graph.parquetProfile.schemaSha256 ||
      plan.graph.openDataRoot.expectedCid !== config.expectedOpenDataRootCid ||
      plan.graph.queryTableRoot.expectedCid !==
        config.expectedQueryTableRootCid ||
      (!directTargetBinding && !candidateTargetBinding) ||
      !plan.fixtureExclusion.passed ||
      plan.fixtureExclusion.matches !== 0
    ) {
      throw publicError(
        "contract_mismatch",
        "Published plan does not match the configured MCP contract and targets",
      );
    }

    const manifestBinding = artifact(plan, "open_data", "manifest.json");
    if (
      manifestBinding.expectedCid !== config.expectedManifestCid ||
      manifestBinding.sha256 !== config.expectedManifestSha256
    ) {
      throw publicError(
        "configuration_invalid",
        "Configured manifest identity does not match the publication plan",
      );
    }
    const rootBinding = artifact(plan, "open_data", "index.json");
    const queryBinding = artifact(
      plan,
      "query_table",
      "query-tables/pasco/query-table.parquet",
    );
    const coverageBinding = artifact(plan, "open_data", "coverage.json");
    const provenanceBinding = artifact(plan, "open_data", "provenance.json");
    const summaryBinding = artifact(plan, "open_data", "run-summary.json");
    onStage?.("manifest");
    const metadataBytes = await Promise.all(
      [
        manifestBinding,
        rootBinding,
        coverageBinding,
        provenanceBinding,
        summaryBinding,
      ].map(async (binding) => {
        const bytes = await activeTransport.readCid(
          binding.expectedCid,
          config.limits.maxJsonObjectBytes,
          signal,
        );
        await verifyBytes(bytes, {
          cid: binding.expectedCid,
          sha256: binding.sha256,
        });
        return bytes;
      }),
    );
    const manifestBytes = metadataBytes[0];
    const rootBytes = metadataBytes[1];
    const coverageBytes = metadataBytes[2];
    const provenanceBytes = metadataBytes[3];
    const summaryBytes = metadataBytes[4];
    if (
      !manifestBytes ||
      !rootBytes ||
      !coverageBytes ||
      !provenanceBytes ||
      !summaryBytes
    ) {
      throw publicError(
        "artifact_invalid",
        "Published metadata set is incomplete",
      );
    }
    const manifest = parseJson(manifestBytes, "manifest");
    const root = parseJson(rootBytes, "root index");
    const coverage = parseJson(coverageBytes, "coverage metadata");
    const provenance = parseJson(provenanceBytes, "provenance metadata");
    const runSummary = parseJson(summaryBytes, "run summary");
    const permitCoverage = record(coverage.permits, "permit coverage");
    const contractorCoverage = record(
      coverage.contractors,
      "contractor coverage",
    );
    if (
      manifest.contractVersion !== "1.0.0" ||
      manifest.county !== "pasco" ||
      manifest.coverageMode !== plan.coverage.mode ||
      manifest.scopeId !== plan.coverage.scopeId ||
      manifest.selectionHash !== plan.coverage.selection.selectedRecordSha256 ||
      manifest.rootCid !== config.expectedOpenDataRootCid ||
      manifest.sourceRunId !== plan.coverage.runId ||
      manifest.sourceSnapshotId !== plan.coverage.sourceSnapshotId ||
      root.schemaVersion !== "1" ||
      root.county !== "pasco" ||
      root.shardSize !== 10_000 ||
      root.propertyCount !== plan.counts.canonicalDocuments ||
      coverage.coverageMode !== plan.coverage.mode ||
      coverage.scopeId !== plan.coverage.scopeId ||
      coverage.runId !== plan.coverage.runId ||
      permitCoverage.availability !== "unavailable" ||
      contractorCoverage.availability !== "unavailable"
    ) {
      throw publicError(
        "artifact_invalid",
        "Published metadata does not share one coverage identity",
      );
    }

    onStage?.("graph");
    const manifestEntries = manifest.entries;
    const rootShards = root.shards;
    if (!Array.isArray(manifestEntries) || !Array.isArray(rootShards)) {
      throw publicError("artifact_invalid", "Published graph is incomplete");
    }
    const manifestByCanonicalId = new Map<string, GraphEntry>();
    for (const value of manifestEntries) {
      const entry = record(value, "manifest entry");
      const canonicalPropertyId = stringValue(
        entry.propertyId,
        "manifest property ID",
      );
      if (FIXTURE_PROPERTY_IDS.has(canonicalPropertyId)) {
        throw publicError(
          "fixture_rejected",
          "Frozen fixture identifiers are forbidden in production artifacts",
        );
      }
      const parsed: GraphEntry = {
        byteSize: numberValue(entry.bytes, "manifest property bytes"),
        canonicalPropertyId,
        cid: stringValue(entry.cid, "manifest property CID"),
        parcelIdentifier: stringValue(
          entry.parcelIdentifier,
          "manifest parcel identifier",
        ),
        publicPropertyId: publicPropertyId(canonicalPropertyId),
        sha256: stringValue(entry.sha256, "manifest property hash"),
      };
      if (
        !CIDV0_PATTERN.test(parsed.cid) ||
        !SHA256_PATTERN.test(parsed.sha256) ||
        manifestByCanonicalId.has(canonicalPropertyId)
      ) {
        throw publicError(
          "artifact_invalid",
          "Published manifest entry is invalid",
        );
      }
      manifestByCanonicalId.set(canonicalPropertyId, parsed);
    }

    const graphEntries = new Map<string, GraphEntry>();
    for (const [rootIndex, value] of rootShards.entries()) {
      const shardReference = record(value, "root shard reference");
      const shardIndex = numberValue(shardReference.shardIndex, "shard index");
      const shardCid = stringValue(shardReference.shardCid, "shard CID");
      const shardKey = `shards/shard-${String(shardIndex).padStart(4, "0")}.json`;
      const binding = artifact(plan, "open_data", shardKey);
      if (
        shardIndex !== rootIndex ||
        shardCid !== binding.expectedCid ||
        shardReference.count !==
          plan.artifacts.shards.find(
            (candidate) => candidate.objectKey === shardKey,
          )?.propertyCount
      ) {
        throw publicError(
          "artifact_invalid",
          "Published root shard reference is inconsistent",
        );
      }
      const bytes = await activeTransport.readCid(
        shardCid,
        config.limits.maxJsonObjectBytes,
        signal,
      );
      await verifyBytes(bytes, { cid: shardCid, sha256: binding.sha256 });
      const shard = parseJson(bytes, "shard");
      if (
        shard.schemaVersion !== "1" ||
        shard.shardIndex !== shardIndex ||
        shard.count !== shardReference.count ||
        shard.fromParcel !== shardReference.fromParcel ||
        shard.toParcel !== shardReference.toParcel ||
        !Array.isArray(shard.entries) ||
        shard.entries.length !== shard.count
      ) {
        throw publicError("artifact_invalid", "Published shard is invalid");
      }
      for (const entryValue of shard.entries) {
        const shardEntry = record(entryValue, "shard entry");
        const canonicalPropertyId = stringValue(
          shardEntry.propertyId,
          "shard property ID",
        );
        const expected = manifestByCanonicalId.get(canonicalPropertyId);
        if (
          !expected ||
          shardEntry.cid !== expected.cid ||
          shardEntry.fileSizeBytes !== expected.byteSize ||
          shardEntry.parcelIdentifier !== expected.parcelIdentifier ||
          graphEntries.has(canonicalPropertyId)
        ) {
          throw publicError(
            "artifact_invalid",
            "Published root-to-shard graph is inconsistent",
          );
        }
        graphEntries.set(canonicalPropertyId, expected);
      }
    }
    if (
      graphEntries.size !== manifestByCanonicalId.size ||
      graphEntries.size !== plan.counts.canonicalDocuments
    ) {
      throw publicError(
        "artifact_invalid",
        "Published graph traversal is incomplete",
      );
    }

    onStage?.("parquet");
    const parquetBytes = await activeTransport.readCid(
      queryBinding.expectedCid,
      config.limits.maxParquetBytes,
      signal,
    );
    await verifyBytes(parquetBytes, {
      cid: queryBinding.expectedCid,
      sha256: queryBinding.sha256,
    });
    const queryRows = await readPublicQueryRows(
      parquetBytes,
      graphEntries,
      plan,
    );
    const coordinates = queryRows.filter(
      (row) => row.latitude !== null && row.longitude !== null,
    ).length;

    const provenanceSources = provenance.sources;
    if (!Array.isArray(provenanceSources) || provenanceSources.length === 0) {
      throw publicError(
        "artifact_invalid",
        "Published provenance metadata is incomplete",
      );
    }
    const provenanceArtifactUris = new Set(
      provenanceSources.map((source) =>
        stringValue(
          record(source, "provenance source").artifactUri,
          "provenance artifact URI",
        ),
      ),
    );
    const resultCounts = record(runSummary.resultCounts, "run counts");
    const completedAt = iso(root.completedAt, "root completion timestamp");
    const elapsedMs = numberValue(resultCounts.elapsedMs, "run elapsed time");
    const entriesByPublicId = new Map(
      [...graphEntries.values()].map((entry) => [
        entry.publicPropertyId,
        entry,
      ]),
    );
    return new PublicIpnsProvider({
      contracts,
      entriesByPublicId,
      maximumJsonBytes: config.limits.maxJsonObjectBytes,
      metadata: {
        artifactCids: [
          config.expectedOpenDataRootCid,
          config.expectedQueryTableRootCid,
          config.expectedManifestCid,
          config.expectedPlanCid,
        ],
        asOf: iso(plan.freshness.asOf, "plan as-of timestamp"),
        canonicalDocumentCount: graphEntries.size,
        completedAt,
        coordinateCount: coordinates,
        coverageMode: plan.coverage.mode,
        contractorCoverage: "unavailable",
        datasetVersion: `pasco-${plan.coverage.mode}-${plan.planSha256.slice(0, 16)}`,
        fixtureMatches: 0,
        limitations: plan.limitations,
        manifestSha256: manifestBinding.sha256,
        objectCount: plan.artifacts.objectInventory.length,
        parquetSha256: queryBinding.sha256,
        permitCoverage: "unavailable",
        plan: {
          sourceWatermark: {
            appraiserObservedDate: plan.freshness.observedAt.slice(0, 10),
            asOf: plan.freshness.asOf,
            coverageMode: plan.coverage.mode,
            loadedAt: plan.freshness.loadedAt,
            runId: plan.coverage.runId,
            scopeId: plan.coverage.scopeId,
            snapshotId: plan.coverage.sourceSnapshotId,
            workflowId: plan.coverage.workflowId,
          },
        },
        providerMode: "public-ipns",
        publication: {
          candidateDemoPlanId: config.candidateDemoPlanId,
          candidateDemoPlanSha256: config.candidateDemoPlanSha256,
          manifestCid: config.expectedManifestCid,
          openDataIpns: config.openDataIpns,
          openDataRootCid: config.expectedOpenDataRootCid,
          planCid: config.expectedPlanCid,
          planSha256: plan.planSha256,
          queryTableIpns: config.queryTableIpns,
          queryTableRootCid: config.expectedQueryTableRootCid,
          resolverPolicy: config.resolverPolicy,
          scopeId: plan.coverage.scopeId,
          selectionHash: plan.coverage.selection.selectedRecordSha256,
          sourceSnapshotId: plan.coverage.sourceSnapshotId,
        },
        runId: stringValue(runSummary.runId, "run ID"),
        runSummary,
        startedAt: new Date(Date.parse(completedAt) - elapsedMs).toISOString(),
        workflowId: stringValue(runSummary.workflowId, "workflow ID"),
      },
      provenanceArtifactUris,
      queryRows,
      transport: activeTransport,
    });
  }

  async getCanonicalProperty(
    propertyId: string,
    signal?: AbortSignal,
  ): Promise<JsonObject | null> {
    const entry = this.#entriesByPublicId.get(propertyId);
    if (!entry) return null;
    const bytes = await this.#transport.readCid(
      entry.cid,
      Math.min(this.#maximumJsonBytes, entry.byteSize),
      signal,
    );
    await verifyBytes(bytes, { cid: entry.cid, sha256: entry.sha256 });
    const property = parseJson(bytes, "canonical property");
    if (
      property.propertyId !== entry.canonicalPropertyId ||
      this.#contracts.validateCanonical(property).length > 0
    ) {
      throw publicError(
        "contract_mismatch",
        "Published property failed canonical contract validation",
      );
    }
    this.#verifyPropertyProvenance(property);
    return property;
  }

  async getMetadata(signal?: AbortSignal): Promise<DatasetMetadata> {
    if (signal?.aborted) throw signal.reason;
    return structuredClone(this.#metadata);
  }

  async getPermit(
    _permitId: string,
    signal?: AbortSignal,
  ): Promise<JsonObject | null> {
    if (signal?.aborted) throw signal.reason;
    return null;
  }

  async getQueryRows(
    signal?: AbortSignal,
  ): Promise<readonly QueryPropertyRow[]> {
    if (signal?.aborted) throw signal.reason;
    return this.#queryRows;
  }

  #verifyPropertyProvenance(property: JsonObject): void {
    const evidence = property.evidence;
    if (!Array.isArray(evidence) || evidence.length === 0) {
      throw publicError(
        "artifact_invalid",
        "Published property evidence is missing",
      );
    }
    const evidenceIds = new Set<string>();
    for (const value of evidence) {
      const item = record(value, "property evidence");
      const evidenceId = stringValue(item.evidenceId, "evidence ID");
      const artifactUri = stringValue(
        item.sourceArtifactUri,
        "evidence artifact URI",
      );
      const sourceRecordHash = stringValue(
        item.sourceRecordHash,
        "evidence record hash",
      );
      if (
        !this.#provenanceArtifactUris.has(artifactUri) ||
        !/^sha256:[a-f0-9]{64}$/.test(sourceRecordHash)
      ) {
        throw publicError(
          "artifact_invalid",
          "Published property evidence does not resolve",
        );
      }
      evidenceIds.add(evidenceId);
    }
    const references = new Set<string>();
    collectEvidenceReferences(property, references);
    if ([...references].some((reference) => !evidenceIds.has(reference))) {
      throw publicError(
        "artifact_invalid",
        "Published property fact evidence does not resolve",
      );
    }
  }
}
