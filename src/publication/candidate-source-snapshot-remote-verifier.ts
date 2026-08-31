import postgres from "postgres";

import {
  admitCandidateSourceSnapshotRemoteRead,
  loadExistingCandidateSourceSnapshotRemoteReadAdmission,
  loadCandidateSourceSnapshotRemoteReadReceipt,
  recordCandidateSourceSnapshotRemoteCheck,
  recordCandidateSourceSnapshotRemoteReadReceipt,
  type CandidateSourceSnapshotRemoteCheckKind,
  type CandidateSourceSnapshotRemoteReadAdmission,
} from "../db/candidate-source-snapshot-completion.js";
import { sha256 } from "../lib/hash.js";
import type { PublicIpnsProviderConfig } from "../mcp/config.js";
import { McpContractRegistry } from "../mcp/contracts.js";
import type { OracleMcpProvider } from "../mcp/provider.js";
import type {
  PublicCidObjectMetadata,
  PublicCidRangeTransport,
} from "../mcp/public-cid-range.js";
import {
  PublicIpnsProvider,
  readPublicSourceSnapshotQueryTable,
  type IpnsResolutionObservation,
  type PublicReadTransport,
  type PublicSourceSnapshotQueryTableResult,
} from "../mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../mcp/runtime.js";
import {
  readBoundCompactPublicationManifestIndex,
  readBoundControlCollectionIndex,
  verifyShardedControlCollection,
  type CompactPublicationManifestIndex,
  type ControlCollectionIndex,
  type ControlCollectionReference,
  type ControlShardDescriptor,
} from "./control-artifacts.js";
import type { CandidateSourceSnapshotDemoPlan } from "./candidate-source-snapshot-demo.js";
import type { CandidateSourceSnapshotLocalObjectSource } from "./candidate-source-snapshot-filebase.js";
import type { CandidateSourceSnapshotUploadObject } from "./candidate-source-snapshot-demo.js";
import { calculateIpfsCid, CIDV0_PATTERN } from "./ipfs-cid.js";

const IMMUTABLE_GATEWAY_ORIGIN = "https://ipfs.filebase.io";
const PARQUET_LOGICAL_READS = 8_194;
const NON_PARQUET_LOGICAL_READS = 109;
const PROPERTY_SAMPLE_READS = 11;
const MAXIMUM_VERIFICATION_CONCURRENCY = 4;
const RETRY_BACKOFF_MS = 50;

export interface CandidateSourceSnapshotRemoteVerifierObject {
  domain: "open_data" | "query_table";
  expectedBytes: number;
  expectedCid: string;
  expectedSha256: string;
  logicalObjectKey: string;
  remoteObjectKey: string;
}

interface VerifierInventory {
  nonParquet: readonly CandidateSourceSnapshotRemoteVerifierObject[];
  planArtifact: CandidateSourceSnapshotRemoteVerifierObject;
  queryTable: CandidateSourceSnapshotRemoteVerifierObject;
  selectedProperties: readonly CandidateSourceSnapshotRemoteVerifierObject[];
}

export interface CandidateSourceSnapshotCredentialFreeVerifier {
  verify(input: {
    approvalId: string;
    databaseUrl: string;
    plan: CandidateSourceSnapshotDemoPlan;
    localSource: CandidateSourceSnapshotLocalObjectSource;
    uploadClosureId: string;
  }): Promise<void>;
}

export interface CandidateSourceSnapshotRemoteVerifierDependencies {
  fetchImpl?: typeof fetch;
  retryDelay?: (milliseconds: number) => Promise<void>;
}

export interface CandidateSourceSnapshotRemoteReadJournal {
  admit: typeof admitCandidateSourceSnapshotRemoteRead;
  loadExisting: typeof loadExistingCandidateSourceSnapshotRemoteReadAdmission;
  loadReceipt: typeof loadCandidateSourceSnapshotRemoteReadReceipt;
  recordReceipt: typeof recordCandidateSourceSnapshotRemoteReadReceipt;
}

const POSTGRES_REMOTE_READ_JOURNAL: CandidateSourceSnapshotRemoteReadJournal = {
  admit: admitCandidateSourceSnapshotRemoteRead,
  loadExisting: loadExistingCandidateSourceSnapshotRemoteReadAdmission,
  loadReceipt: loadCandidateSourceSnapshotRemoteReadReceipt,
  recordReceipt: recordCandidateSourceSnapshotRemoteReadReceipt,
};

interface ByteRange {
  endExclusive: number;
  start: number;
}

/**
 * Partitions one immutable object into an exact number of non-empty,
 * contiguous ranges. The formula is independent of transport behavior and is
 * consequently safe to bind into durable logical request identities.
 */
export function partitionCandidateSourceSnapshotParquetRanges(
  byteLength: number,
  rangeCount = PARQUET_LOGICAL_READS,
): readonly ByteRange[] {
  if (
    !Number.isSafeInteger(byteLength) ||
    !Number.isSafeInteger(rangeCount) ||
    byteLength < rangeCount ||
    rangeCount <= 0
  ) {
    throw new Error("Parquet verification range partition is invalid");
  }
  const ranges = Array.from({ length: rangeCount }, (_, index) => ({
    endExclusive: Math.floor((byteLength * (index + 1)) / rangeCount),
    start: Math.floor((byteLength * index) / rangeCount),
  }));
  if (
    ranges[0]?.start !== 0 ||
    ranges.at(-1)?.endExclusive !== byteLength ||
    ranges.some(
      (range, index) =>
        range.endExclusive <= range.start ||
        (index > 0 && ranges[index - 1]!.endExclusive !== range.start),
    )
  ) {
    throw new Error("Parquet verification ranges are not an exact partition");
  }
  return ranges;
}

export function candidateSourceSnapshotRemoteCheckKind(
  object: Pick<
    CandidateSourceSnapshotRemoteVerifierObject,
    "logicalObjectKey" | "remoteObjectKey"
  >,
  plan: CandidateSourceSnapshotDemoPlan,
): Exclude<CandidateSourceSnapshotRemoteCheckKind, "query_table"> {
  if (object.logicalObjectKey === "candidate-source-snapshot-plan.json") {
    return "plan_artifact";
  }
  if (
    object.remoteObjectKey === plan.controlArtifacts.manifestIndex.objectKey
  ) {
    return "manifest";
  }
  if (object.logicalObjectKey.startsWith("object_inventory/")) {
    return "inventory";
  }
  if (
    object.logicalObjectKey === "coverage.json" ||
    object.logicalObjectKey === "provenance.json" ||
    object.logicalObjectKey === "run-summary.json"
  ) {
    return "coverage";
  }
  if (
    object.logicalObjectKey.startsWith("manifest_entries/") ||
    object.logicalObjectKey.startsWith("properties/")
  ) {
    return "fixture_exclusion";
  }
  return "open_data_graph";
}

function objectRow(row: {
  domain: string;
  expected_bytes: number | string;
  expected_cid: string;
  expected_sha256: string;
  logical_object_key: string;
  remote_object_key: string;
}): CandidateSourceSnapshotRemoteVerifierObject {
  const expectedBytes = Number(row.expected_bytes);
  if (
    (row.domain !== "open_data" && row.domain !== "query_table") ||
    !Number.isSafeInteger(expectedBytes) ||
    expectedBytes <= 0 ||
    !CIDV0_PATTERN.test(row.expected_cid) ||
    !/^[a-f0-9]{64}$/.test(row.expected_sha256) ||
    row.logical_object_key.length === 0 ||
    row.remote_object_key.length === 0
  ) {
    throw new Error("Durable verification inventory row is invalid");
  }
  return {
    domain: row.domain,
    expectedBytes,
    expectedCid: row.expected_cid,
    expectedSha256: row.expected_sha256,
    logicalObjectKey: row.logical_object_key,
    remoteObjectKey: row.remote_object_key,
  };
}

async function loadVerifierInventory(
  databaseUrl: string,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<VerifierInventory> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const [nonPropertyRows, propertyRows, counts] = await Promise.all([
      sql<
        {
          domain: string;
          expected_bytes: number | string;
          expected_cid: string;
          expected_sha256: string;
          logical_object_key: string;
          remote_object_key: string;
        }[]
      >`
        SELECT domain, expected_bytes, expected_cid, expected_sha256,
               logical_object_key, remote_object_key
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${plan.planId}
          AND domain <> 'query_table'
          AND logical_object_key NOT LIKE 'properties/%'
        ORDER BY domain, remote_object_key
      `,
      sql<
        {
          domain: string;
          expected_bytes: number | string;
          expected_cid: string;
          expected_sha256: string;
          logical_object_key: string;
          remote_object_key: string;
        }[]
      >`
        SELECT domain, expected_bytes, expected_cid, expected_sha256,
               logical_object_key, remote_object_key
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${plan.planId}
          AND domain = 'open_data'
          AND logical_object_key LIKE 'properties/%'
          AND EXISTS (
            SELECT 1
            FROM oracle_projection_materialized_facts fact
            WHERE fact.materialization_id = ${plan.source.materializationId}
              AND fact.property_id = substring(
                logical_object_key FROM 'properties/(property_[a-f0-9]{32})\\.json'
              )
              AND fact.fact_type = 'coordinate'
          )
        ORDER BY remote_object_key
        LIMIT ${PROPERTY_SAMPLE_READS}
      `,
      sql<
        {
          property_count: string;
          query_count: string;
          total_count: string;
        }[]
      >`
        SELECT count(*)::text AS total_count,
               count(*) FILTER (
                 WHERE domain = 'open_data'
                   AND logical_object_key LIKE 'properties/%'
               )::text AS property_count,
               count(*) FILTER (WHERE domain = 'query_table')::text
                 AS query_count
        FROM oracle_candidate_source_snapshot_demo_objects
        WHERE plan_id = ${plan.planId}
      `,
    ]);
    const queryRows = await sql<
      {
        domain: string;
        expected_bytes: number | string;
        expected_cid: string;
        expected_sha256: string;
        logical_object_key: string;
        remote_object_key: string;
      }[]
    >`
      SELECT domain, expected_bytes, expected_cid, expected_sha256,
             logical_object_key, remote_object_key
      FROM oracle_candidate_source_snapshot_demo_objects
      WHERE plan_id = ${plan.planId} AND domain = 'query_table'
      ORDER BY remote_object_key
    `;
    const count = counts[0];
    if (
      !count ||
      Number(count.property_count) !== plan.coverage.activeProperties ||
      Number(count.query_count) !== 1 ||
      Number(count.total_count) !==
        plan.controlArtifacts.payloadObjectCount +
          plan.controlArtifacts.controlObjectCount +
          1 ||
      queryRows.length !== 1 ||
      propertyRows.length !== PROPERTY_SAMPLE_READS
    ) {
      throw new Error("Durable verification inventory cardinality is invalid");
    }
    const base = nonPropertyRows.map(objectRow);
    const selectedProperties = propertyRows.map(objectRow);
    const nonParquet = [...base, ...selectedProperties].sort((left, right) =>
      left.remoteObjectKey < right.remoteObjectKey
        ? -1
        : left.remoteObjectKey > right.remoteObjectKey
          ? 1
          : 0,
    );
    const queryTable = objectRow(queryRows[0]!);
    const planArtifacts = nonParquet.filter(
      (object) =>
        object.logicalObjectKey === "candidate-source-snapshot-plan.json",
    );
    if (
      nonParquet.length !== NON_PARQUET_LOGICAL_READS ||
      planArtifacts.length !== 1 ||
      queryTable.expectedCid !== plan.targets.queryTable.targetCid ||
      nonParquet.filter(
        (object) => object.expectedCid === plan.targets.openData.targetCid,
      ).length !== 1
    ) {
      throw new Error(
        "Durable verification inventory does not match the exact read envelope",
      );
    }
    const kinds = new Set(
      nonParquet.map((object) =>
        candidateSourceSnapshotRemoteCheckKind(object, plan),
      ),
    );
    const requiredKinds: readonly Exclude<
      CandidateSourceSnapshotRemoteCheckKind,
      "query_table"
    >[] = [
      "coverage",
      "fixture_exclusion",
      "inventory",
      "manifest",
      "open_data_graph",
      "plan_artifact",
    ];
    if (requiredKinds.some((kind) => !kinds.has(kind))) {
      throw new Error("Durable verification object roles are incomplete");
    }
    return {
      nonParquet,
      planArtifact: planArtifacts[0]!,
      queryTable,
      selectedProperties,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function combinedSignal(timeoutMs: number): AbortSignal {
  return AbortSignal.timeout(timeoutMs);
}

function timeoutError(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}

async function readBoundedBody(
  response: Response,
  expectedBytes: number,
): Promise<Uint8Array> {
  if (!response.body) throw new Error("Immutable response body is missing");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    total += next.value.byteLength;
    if (total > expectedBytes) {
      await reader.cancel();
      throw new Error("Immutable response exceeds its exact byte binding");
    }
    chunks.push(next.value);
  }
  if (total !== expectedBytes) {
    throw new Error("Immutable response byte count is inconsistent");
  }
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function validateResponseCid(headers: Headers, expectedCid: string): void {
  const values = [headers.get("x-ipfs-roots"), headers.get("x-ipfs-path")]
    .filter((value): value is string => value !== null)
    .flatMap(
      (value) =>
        value.match(/Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120}/g) ?? [],
    );
  if (values.length > 0 && !values.includes(expectedCid)) {
    throw new Error("Immutable response CID header is inconsistent");
  }
}

function immutableUrl(cid: string): URL {
  if (!CIDV0_PATTERN.test(cid)) {
    throw new Error("Immutable verification CID is invalid");
  }
  return new URL(`/ipfs/${cid}`, IMMUTABLE_GATEWAY_ORIGIN);
}

function redirectedUrl(
  response: Response,
  current: URL,
  cid: string,
): URL | null {
  if (![301, 302, 303, 307, 308].includes(response.status)) return null;
  const location = response.headers.get("location");
  if (!location) throw new Error("Immutable redirect lacks a location");
  const next = new URL(location, current);
  if (
    next.origin !== IMMUTABLE_GATEWAY_ORIGIN ||
    next.pathname !== `/ipfs/${cid}` ||
    next.username !== "" ||
    next.password !== "" ||
    next.search !== "" ||
    next.hash !== ""
  ) {
    throw new Error("Immutable redirect escaped its compiled CID origin");
  }
  return next;
}

export class CandidateSourceSnapshotDurableImmutableReader {
  readonly #databaseUrl: string;
  readonly #fetch: typeof fetch;
  readonly #localBytes = new Map<string, Promise<Uint8Array>>();
  readonly #localSource: CandidateSourceSnapshotLocalObjectSource;
  readonly #plan: CandidateSourceSnapshotDemoPlan;
  readonly #journal: CandidateSourceSnapshotRemoteReadJournal;
  readonly #retryDelay: (milliseconds: number) => Promise<void>;

  constructor(input: {
    databaseUrl: string;
    fetchImpl: typeof fetch;
    localSource: CandidateSourceSnapshotLocalObjectSource;
    plan: CandidateSourceSnapshotDemoPlan;
    journal?: CandidateSourceSnapshotRemoteReadJournal;
    retryDelay: (milliseconds: number) => Promise<void>;
  }) {
    this.#databaseUrl = input.databaseUrl;
    this.#fetch = input.fetchImpl;
    this.#localSource = input.localSource;
    this.#plan = input.plan;
    this.#journal = input.journal ?? POSTGRES_REMOTE_READ_JOURNAL;
    this.#retryDelay = input.retryDelay;
  }

  async readFull(input: {
    checkKind: CandidateSourceSnapshotRemoteCheckKind;
    logicalRequestSequence: number;
    object: CandidateSourceSnapshotRemoteVerifierObject;
  }): Promise<Uint8Array> {
    return this.#read({ ...input, range: null });
  }

  async readRange(input: {
    checkKind: "query_table";
    logicalRequestSequence: number;
    object: CandidateSourceSnapshotRemoteVerifierObject;
    range: ByteRange;
  }): Promise<Uint8Array> {
    return this.#read(input);
  }

  async #read(input: {
    checkKind: CandidateSourceSnapshotRemoteCheckKind;
    logicalRequestSequence: number;
    object: CandidateSourceSnapshotRemoteVerifierObject;
    range: ByteRange | null;
  }): Promise<Uint8Array> {
    const maximumAttempts =
      this.#plan.requestEnvelope.finalVerification
        .maximumTransportAttemptsPerLogicalRequest;
    const maximumRedirects =
      this.#plan.requestEnvelope.finalVerification.maximumRedirectsPerAttempt;
    let finalError: Error | null = null;
    for (let attempt = 1; attempt <= maximumAttempts; attempt += 1) {
      let current = immutableUrl(input.object.expectedCid);
      let recordedRedirectChild: CandidateSourceSnapshotRemoteReadAdmission | null =
        null;
      for (let redirect = 0; redirect <= maximumRedirects; redirect += 1) {
        const admission =
          recordedRedirectChild ??
          (await this.#journal.admit(this.#databaseUrl, {
            attemptSequence: attempt,
            ...(input.range
              ? {
                  byteRangeEnd: input.range.endExclusive - 1,
                  byteRangeStart: input.range.start,
                }
              : {}),
            checkKind: input.checkKind,
            domain: input.object.domain,
            logicalRequestSequence: input.logicalRequestSequence,
            operationKind: input.range
              ? "immutable_artifact_range_read"
              : "immutable_artifact_read",
            planId: this.#plan.planId,
            planSha256: this.#plan.planSha256,
            redirectSequence: redirect,
            remoteObjectKey: input.object.remoteObjectKey,
          }));
        recordedRedirectChild = null;
        if (admission.alreadyRecorded) {
          const durable = await this.#journal.loadReceipt(
            this.#databaseUrl,
            admission,
          );
          if (durable.receipt?.outcome === "verified") {
            const bytes = await this.#readLocalObject(input.object);
            const replay = input.range
              ? bytes.slice(input.range.start, input.range.endExclusive)
              : bytes;
            if (
              durable.receipt.responseBytes !== replay.byteLength ||
              durable.receipt.responseSha256 !== sha256(Buffer.from(replay))
            ) {
              throw new Error(
                "Durable verification receipt does not match local replay bytes",
              );
            }
            return replay;
          }
          if (durable.receipt === null) {
            await this.#record(admission, "timeout_unknown");
            finalError = new Error(
              "Interrupted immutable verification request was durably classified",
            );
            break;
          }
          if (
            durable.receipt.outcome === "retryable_failure" &&
            redirect < maximumRedirects
          ) {
            const child = await this.#journal.loadExisting(this.#databaseUrl, {
              attemptSequence: attempt,
              ...(input.range
                ? {
                    byteRangeEnd: input.range.endExclusive - 1,
                    byteRangeStart: input.range.start,
                  }
                : {}),
              checkKind: input.checkKind,
              domain: input.object.domain,
              logicalRequestSequence: input.logicalRequestSequence,
              operationKind: input.range
                ? "immutable_artifact_range_read"
                : "immutable_artifact_read",
              planId: this.#plan.planId,
              planSha256: this.#plan.planSha256,
              redirectSequence: redirect + 1,
              remoteObjectKey: input.object.remoteObjectKey,
            });
            if (child) {
              // The compiled redirect allowlist normalizes every allowed child
              // to this same HTTPS origin and exact /ipfs/<CID> path. Resume
              // only the already-admitted child; this branch never reserves a
              // new request or advances the transport-attempt sequence.
              current = immutableUrl(input.object.expectedCid);
              recordedRedirectChild = child;
              continue;
            }
          }
          finalError = new Error(
            "Immutable verification is resuming after a durable failed attempt",
          );
          break;
        }
        let response: Response;
        try {
          response = await this.#fetch(current, {
            ...(input.range
              ? {
                  headers: {
                    range: `bytes=${input.range.start}-${input.range.endExclusive - 1}`,
                  },
                }
              : {}),
            method: "GET",
            redirect: "manual",
            signal: combinedSignal(this.#plan.limits.requestTimeoutMs),
          });
        } catch (error) {
          const outcome = timeoutError(error)
            ? ("timeout_unknown" as const)
            : ("retryable_failure" as const);
          await this.#record(admission, outcome);
          finalError = new Error(
            outcome === "timeout_unknown"
              ? "Immutable verification request timed out"
              : "Immutable verification transport failed",
          );
          break;
        }
        let next: URL | null;
        try {
          next = redirectedUrl(response, current, input.object.expectedCid);
        } catch (error) {
          await this.#record(admission, "terminal_failure");
          throw error;
        }
        if (next) {
          await this.#record(admission, "retryable_failure");
          if (redirect === maximumRedirects) {
            throw new Error("Immutable verification redirect limit exceeded");
          }
          current = next;
          continue;
        }
        if ([500, 502, 503, 504].includes(response.status)) {
          await this.#record(admission, "retryable_failure");
          finalError = new Error(
            "Immutable verification gateway is unavailable",
          );
          break;
        }
        const expectedStatus = input.range ? 206 : 200;
        if (response.status !== expectedStatus) {
          await this.#record(admission, "terminal_failure");
          throw new Error("Immutable verification response status is invalid");
        }
        try {
          validateResponseCid(response.headers, input.object.expectedCid);
          if (input.range) {
            const expectedContentRange = `bytes ${input.range.start}-${input.range.endExclusive - 1}/${input.object.expectedBytes}`;
            if (
              response.headers.get("content-range") !== expectedContentRange
            ) {
              throw new Error(
                "Immutable range response does not match its exact interval",
              );
            }
          }
          const expectedBytes = input.range
            ? input.range.endExclusive - input.range.start
            : input.object.expectedBytes;
          const bytes = await readBoundedBody(response, expectedBytes);
          const responseSha256 = sha256(Buffer.from(bytes));
          if (
            !input.range &&
            (responseSha256 !== input.object.expectedSha256 ||
              (await calculateIpfsCid(bytes)) !== input.object.expectedCid)
          ) {
            throw new Error(
              "Immutable object bytes failed CID/hash validation",
            );
          }
          await recordCandidateSourceSnapshotRemoteReadReceipt(
            this.#databaseUrl,
            {
              admission,
              observedAt: new Date().toISOString(),
              outcome: "verified",
              responseBytes: bytes.byteLength,
              responseSha256,
            },
          );
          return bytes;
        } catch (error) {
          await this.#record(admission, "terminal_failure");
          throw error;
        }
      }
      if (attempt < maximumAttempts) {
        await this.#retryDelay(RETRY_BACKOFF_MS * attempt);
      }
    }
    throw finalError ?? new Error("Immutable verification attempts exhausted");
  }

  async #readLocalObject(
    object: CandidateSourceSnapshotRemoteVerifierObject,
  ): Promise<Uint8Array> {
    let pending = this.#localBytes.get(object.remoteObjectKey);
    if (!pending) {
      pending = (async () => {
        const uploadObject: CandidateSourceSnapshotUploadObject = {
          byteSize: object.expectedBytes,
          domain: object.domain,
          expectedCid: object.expectedCid,
          logicalObjectKey: object.logicalObjectKey,
          remoteObjectKey: object.remoteObjectKey,
          sha256: object.expectedSha256,
        };
        const opened = await this.#localSource.openVerifiedStream(uploadObject);
        const chunks: Buffer[] = [];
        let total = 0;
        for await (const chunk of opened.body) {
          const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          total += bytes.byteLength;
          if (total > object.expectedBytes) {
            throw new Error("Local replay object exceeds its exact binding");
          }
          chunks.push(bytes);
        }
        const result = Buffer.concat(chunks, total);
        if (
          result.byteLength !== object.expectedBytes ||
          sha256(result) !== object.expectedSha256 ||
          (await calculateIpfsCid(result)) !== object.expectedCid
        ) {
          throw new Error("Local replay object failed its immutable binding");
        }
        return new Uint8Array(result);
      })();
      this.#localBytes.set(object.remoteObjectKey, pending);
    }
    return pending;
  }

  async #record(
    admission: CandidateSourceSnapshotRemoteReadAdmission,
    outcome: "retryable_failure" | "timeout_unknown" | "terminal_failure",
  ): Promise<void> {
    await this.#journal.recordReceipt(this.#databaseUrl, {
      admission,
      observedAt: new Date().toISOString(),
      outcome,
    });
  }
}

class MemoryPublicTransport implements PublicReadTransport {
  readonly #bytesByCid: ReadonlyMap<string, Uint8Array>;
  readonly #plan: CandidateSourceSnapshotDemoPlan;

  constructor(
    bytesByCid: ReadonlyMap<string, Uint8Array>,
    plan: CandidateSourceSnapshotDemoPlan,
  ) {
    this.#bytesByCid = bytesByCid;
    this.#plan = plan;
  }

  async readCid(cid: string, maximumBytes: number): Promise<Uint8Array> {
    const bytes = this.#bytesByCid.get(cid);
    if (!bytes || bytes.byteLength > maximumBytes) {
      throw new Error("Verified immutable object is unavailable in memory");
    }
    return bytes;
  }

  async resolveIpns(
    identity: string,
  ): Promise<readonly IpnsResolutionObservation[]> {
    const target =
      identity === this.#plan.targets.openData.ipnsNetworkKey
        ? this.#plan.targets.openData.targetCid
        : identity === this.#plan.targets.queryTable.ipnsNetworkKey
          ? this.#plan.targets.queryTable.targetCid
          : null;
    if (!target) throw new Error("Verified IPNS identity is unavailable");
    return [
      {
        cacheAgeSeconds: 0,
        cid: target,
        observedAt: new Date().toISOString(),
        resolver: "durable_verified_intent",
        status: "resolved",
      },
    ];
  }
}

class MemoryRangeTransport implements PublicCidRangeTransport {
  readonly #bytes: Uint8Array;
  readonly #cid: string;

  constructor(cid: string, bytes: Uint8Array) {
    this.#bytes = bytes;
    this.#cid = cid;
  }

  async statCid(
    cid: string,
    maximumObjectBytes: number,
  ): Promise<PublicCidObjectMetadata> {
    if (cid !== this.#cid || this.#bytes.byteLength > maximumObjectBytes) {
      throw new Error("Verified Parquet object is unavailable in memory");
    }
    return {
      acceptsByteRanges: true,
      byteLength: this.#bytes.byteLength,
      cid,
    };
  }

  async readCidRange(
    cid: string,
    start: number,
    endExclusive: number,
    expectedObjectBytes: number,
    maximumResponseBytes: number,
  ): Promise<Uint8Array> {
    if (
      cid !== this.#cid ||
      expectedObjectBytes !== this.#bytes.byteLength ||
      start < 0 ||
      endExclusive <= start ||
      endExclusive > this.#bytes.byteLength ||
      endExclusive - start > maximumResponseBytes
    ) {
      throw new Error("Verified Parquet range is invalid");
    }
    return this.#bytes.slice(start, endExclusive);
  }
}

function jsonObject(bytes: Uint8Array, label: string): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(bytes).toString("utf8"));
  } catch {
    throw new Error(`${label} JSON is invalid`);
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} JSON is invalid`);
  }
  return value as Record<string, unknown>;
}

function recordValue(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} is invalid`);
  }
  return value as Record<string, unknown>;
}

function exactString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

function exactNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

function bytesForObject(
  bytesByCid: ReadonlyMap<string, Uint8Array>,
  object: Pick<CandidateSourceSnapshotRemoteVerifierObject, "expectedCid">,
): Uint8Array {
  const bytes = bytesByCid.get(object.expectedCid);
  if (!bytes) throw new Error("Verified immutable object bytes are missing");
  return bytes;
}

function objectByLogicalKey(
  inventory: VerifierInventory,
  key: string,
): CandidateSourceSnapshotRemoteVerifierObject {
  const matches = inventory.nonParquet.filter(
    (object) => object.logicalObjectKey === key,
  );
  if (matches.length !== 1) {
    throw new Error(`Verified object ${key} is missing or duplicated`);
  }
  return matches[0]!;
}

async function readControlIndex(
  reference: ControlCollectionReference,
  inventory: VerifierInventory,
  bytesByCid: ReadonlyMap<string, Uint8Array>,
): Promise<ControlCollectionIndex> {
  const object = inventory.nonParquet.find(
    (candidate) =>
      candidate.remoteObjectKey === reference.indexArtifact.objectKey,
  );
  if (!object) throw new Error("Verified control index is missing");
  return readBoundControlCollectionIndex({
    bytes: bytesForObject(bytesByCid, object),
    reference,
  });
}

function controlShardReader(
  inventory: VerifierInventory,
  bytesByCid: ReadonlyMap<string, Uint8Array>,
): (descriptor: ControlShardDescriptor) => Promise<Uint8Array> {
  return async (descriptor) => {
    const object = inventory.nonParquet.find(
      (candidate) => candidate.remoteObjectKey === descriptor.objectKey,
    );
    if (!object || object.expectedCid !== descriptor.expectedCid) {
      throw new Error("Verified control shard is missing");
    }
    return bytesForObject(bytesByCid, object);
  };
}

async function verifyCompleteGraphAndControls(input: {
  bytesByCid: ReadonlyMap<string, Uint8Array>;
  inventory: VerifierInventory;
  plan: CandidateSourceSnapshotDemoPlan;
  query: PublicSourceSnapshotQueryTableResult;
}): Promise<void> {
  const query = input.query.entries;
  const manifestIndex = await readControlIndex(
    input.plan.controlArtifacts.manifestEntries,
    input.inventory,
    input.bytesByCid,
  );
  const graphIndex = await readControlIndex(
    input.plan.controlArtifacts.graphEdges,
    input.inventory,
    input.bytesByCid,
  );
  const objectInventoryIndex = await readControlIndex(
    input.plan.controlArtifacts.objectInventory,
    input.inventory,
    input.bytesByCid,
  );
  const readShard = controlShardReader(input.inventory, input.bytesByCid);
  const propertyRemoteKeys = new Array<string>(query.length);
  const propertyBytes = new Array<number>(query.length);
  let manifestPosition = 0;
  await verifyShardedControlCollection({
    index: manifestIndex,
    readShard,
    validateValue: (value, key) => {
      const expected = query[manifestPosition];
      const entry = recordValue(value, "manifest entry");
      if (
        !expected ||
        key !==
          `entry:${String(manifestPosition).padStart(9, "0")}:${expected.canonicalPropertyId}` ||
        entry.propertyId !== expected.canonicalPropertyId ||
        entry.parcelIdentifier !== expected.parcelIdentifier ||
        entry.cid !== expected.cid ||
        entry.sha256 !== expected.sha256 ||
        typeof entry.objectKey !== "string" ||
        !entry.objectKey.startsWith(
          `${input.plan.targets.openData.immutablePrefix}properties/`,
        ) ||
        typeof entry.bytes !== "number" ||
        !Number.isSafeInteger(entry.bytes) ||
        entry.bytes <= 0
      ) {
        throw new Error("Manifest/query-table correspondence is invalid");
      }
      propertyRemoteKeys[manifestPosition] = entry.objectKey;
      propertyBytes[manifestPosition] = entry.bytes;
      manifestPosition += 1;
    },
  });
  if (manifestPosition !== query.length) {
    throw new Error("Manifest/query-table cardinality is invalid");
  }

  const rootObject = input.inventory.nonParquet.find(
    (object) => object.expectedCid === input.plan.targets.openData.targetCid,
  );
  if (!rootObject) throw new Error("Verified open-data root is missing");
  const root = jsonObject(
    bytesForObject(input.bytesByCid, rootObject),
    "open-data root",
  );
  if (
    root.schemaVersion !== "1" ||
    root.county !== "pasco" ||
    root.propertyCount !== query.length ||
    root.shardSize !== 10_000 ||
    !Array.isArray(root.shards)
  ) {
    throw new Error("Verified open-data root is invalid");
  }
  const rootShards = root.shards;
  const shardByPosition = new Array<{
    entryIndex: number;
    shardIndex: number;
    shardRemoteKey: string;
  }>(query.length);
  let propertyPosition = 0;
  let previousTo: string | null = null;
  for (const [shardIndex, value] of rootShards.entries()) {
    const reference = recordValue(value, "root shard reference");
    const fromParcel = exactString(reference.fromParcel, "root range start");
    const toParcel = exactString(reference.toParcel, "root range end");
    const count = exactNumber(reference.count, "root shard count");
    const shardCid = exactString(reference.shardCid, "root shard CID");
    if (
      reference.shardIndex !== shardIndex ||
      count <= 0 ||
      fromParcel > toParcel ||
      (previousTo !== null && previousTo >= fromParcel)
    ) {
      throw new Error("Open-data root ranges are invalid");
    }
    previousTo = toParcel;
    const shardLogicalKey = `shards/shard-${String(shardIndex).padStart(4, "0")}.json`;
    const shardObject = objectByLogicalKey(input.inventory, shardLogicalKey);
    if (shardObject.expectedCid !== shardCid) {
      throw new Error("Open-data root/shard CID binding is invalid");
    }
    const shard = jsonObject(
      bytesForObject(input.bytesByCid, shardObject),
      "open-data shard",
    );
    if (
      shard.schemaVersion !== "1" ||
      shard.shardIndex !== shardIndex ||
      shard.count !== count ||
      !Array.isArray(shard.entries) ||
      shard.entries.length !== count
    ) {
      throw new Error("Open-data shard is invalid");
    }
    for (const [entryIndex, entryValue] of shard.entries.entries()) {
      const expected = query[propertyPosition];
      const entry = recordValue(entryValue, "open-data shard entry");
      if (
        !expected ||
        entry.propertyId !== expected.canonicalPropertyId ||
        entry.parcelIdentifier !== expected.parcelIdentifier ||
        entry.cid !== expected.cid ||
        entry.fileSizeBytes !== propertyBytes[propertyPosition]
      ) {
        throw new Error("Root/shard/query-table correspondence is invalid");
      }
      shardByPosition[propertyPosition] = {
        entryIndex,
        shardIndex,
        shardRemoteKey: shardObject.remoteObjectKey,
      };
      propertyPosition += 1;
    }
  }
  if (propertyPosition !== query.length) {
    throw new Error("Open-data graph traversal is incomplete");
  }

  let edgePosition = 0;
  await verifyShardedControlCollection({
    index: graphIndex,
    readShard,
    validateValue: (value, key) => {
      const edge = recordValue(value, "graph edge");
      if (key !== `edge:${String(edgePosition).padStart(9, "0")}`) {
        throw new Error("Graph edge ordering is invalid");
      }
      if (edgePosition < query.length) {
        const expected = query[edgePosition]!;
        const shard = shardByPosition[edgePosition]!;
        if (
          edge.childCid !== expected.cid ||
          edge.childKey !== propertyRemoteKeys[edgePosition] ||
          edge.parentKey !== shard.shardRemoteKey ||
          edge.jsonPointer !== `/entries/${shard.entryIndex}/cid`
        ) {
          throw new Error("Property graph edge is invalid");
        }
      } else {
        const shardIndex = edgePosition - query.length;
        const reference = recordValue(rootShards[shardIndex], "root edge");
        const shardObject = objectByLogicalKey(
          input.inventory,
          `shards/shard-${String(shardIndex).padStart(4, "0")}.json`,
        );
        if (
          edge.childCid !== reference.shardCid ||
          edge.childKey !== shardObject.remoteObjectKey ||
          edge.parentKey !== rootObject.remoteObjectKey ||
          edge.jsonPointer !== `/shards/${shardIndex}/shardCid`
        ) {
          throw new Error("Root graph edge is invalid");
        }
      }
      edgePosition += 1;
    },
  });
  if (edgePosition !== query.length + rootShards.length) {
    throw new Error("Graph edge cardinality is invalid");
  }

  const sortedProperties = [...query].sort((left, right) =>
    left.canonicalPropertyId < right.canonicalPropertyId
      ? -1
      : left.canonicalPropertyId > right.canonicalPropertyId
        ? 1
        : 0,
  );
  const nonPropertyInventory = new Map(
    [...input.inventory.nonParquet, input.inventory.queryTable]
      .filter((object) => !object.logicalObjectKey.startsWith("properties/"))
      .map((object) => [object.remoteObjectKey, object]),
  );
  let inventoryCount = 0;
  let inventoryPropertyPosition = 0;
  await verifyShardedControlCollection({
    index: objectInventoryIndex,
    readShard,
    validateValue: (value, key) => {
      const entry = recordValue(value, "object inventory entry");
      const remoteObjectKey = exactString(
        entry.remoteObjectKey,
        "inventory remote object key",
      );
      if (entry.logicalObjectKey?.toString().startsWith("properties/")) {
        const expected = sortedProperties[inventoryPropertyPosition];
        const manifestPosition = expected?.position;
        if (
          !expected ||
          manifestPosition === undefined ||
          key !== `open_data:${entry.logicalObjectKey}` ||
          entry.expectedCid !== expected.cid ||
          entry.sha256 !== expected.sha256 ||
          entry.byteSize !== propertyBytes[manifestPosition] ||
          remoteObjectKey !== propertyRemoteKeys[manifestPosition]
        ) {
          throw new Error("Property object inventory is invalid");
        }
        inventoryPropertyPosition += 1;
      } else {
        const expected = nonPropertyInventory.get(remoteObjectKey);
        if (
          !expected ||
          key !== `${expected.domain}:${expected.logicalObjectKey}` ||
          entry.expectedCid !== expected.expectedCid ||
          entry.sha256 !== expected.expectedSha256 ||
          entry.byteSize !== expected.expectedBytes
        ) {
          throw new Error("Non-property object inventory is invalid");
        }
      }
      inventoryCount += 1;
    },
  });
  if (
    inventoryCount !== input.plan.controlArtifacts.payloadObjectCount ||
    inventoryPropertyPosition !== query.length
  ) {
    throw new Error("Object inventory cardinality is invalid");
  }
}

function providerConfig(
  plan: CandidateSourceSnapshotDemoPlan,
  inventory: VerifierInventory,
): PublicIpnsProviderConfig {
  return {
    candidateDemoPlanId: plan.planId,
    candidateDemoPlanSha256: plan.planSha256,
    candidateDemoSourcePlanSha256: plan.source.sourcePlanSha256,
    environment: "production",
    expectedManifestCid: plan.controlArtifacts.manifestIndex.expectedCid,
    expectedManifestSha256: plan.controlArtifacts.manifestIndex.sha256,
    expectedOpenDataRootCid: plan.targets.openData.targetCid,
    expectedPlanCid: inventory.planArtifact.expectedCid,
    expectedPlanSha256: inventory.planArtifact.expectedSha256,
    expectedQueryTableRootCid: plan.targets.queryTable.targetCid,
    limits: {
      maxCacheAgeSeconds: 300,
      maxJsonObjectBytes: 32 * 1024 * 1024,
      maxParquetBytes: Math.max(
        128 * 1024 * 1024,
        inventory.queryTable.expectedBytes,
      ),
      maxRedirects:
        plan.requestEnvelope.finalVerification.maximumRedirectsPerAttempt,
      retries:
        plan.requestEnvelope.finalVerification
          .maximumTransportAttemptsPerLogicalRequest - 1,
      transportTimeoutMs: plan.limits.requestTimeoutMs,
    },
    mode: "public-ipns",
    openDataIpns: plan.targets.openData.ipnsNetworkKey,
    queryTableIpns: plan.targets.queryTable.ipnsNetworkKey,
    resolverPolicy: "candidate_filebase_delegated_v2",
  };
}

async function runWithConcurrency<T>(input: {
  concurrency: number;
  items: readonly T[];
  run: (item: T, index: number) => Promise<void>;
}): Promise<void> {
  let next = 0;
  const workers = Array.from(
    { length: Math.min(input.concurrency, input.items.length) },
    async () => {
      for (;;) {
        const index = next;
        next += 1;
        if (index >= input.items.length) return;
        await input.run(input.items[index]!, index);
      }
    },
  );
  await Promise.all(workers);
}

/**
 * Executes the exact plan-bound credential-free closure. Every external GET is
 * admitted before transport and receives an immutable receipt; provider and
 * graph checks subsequently operate only on the verified in-memory bytes.
 */
export class PostgresCandidateSourceSnapshotCredentialFreeVerifier implements CandidateSourceSnapshotCredentialFreeVerifier {
  readonly #fetch: typeof fetch;
  readonly #retryDelay: (milliseconds: number) => Promise<void>;

  constructor(
    dependencies: CandidateSourceSnapshotRemoteVerifierDependencies = {},
  ) {
    this.#fetch = dependencies.fetchImpl ?? fetch;
    this.#retryDelay =
      dependencies.retryDelay ??
      ((milliseconds) =>
        new Promise((resolve) => setTimeout(resolve, milliseconds)));
  }

  async verify(input: {
    approvalId: string;
    databaseUrl: string;
    plan: CandidateSourceSnapshotDemoPlan;
    localSource: CandidateSourceSnapshotLocalObjectSource;
    uploadClosureId: string;
  }): Promise<void> {
    void input.approvalId;
    void input.uploadClosureId;
    const inventory = await loadVerifierInventory(
      input.databaseUrl,
      input.plan,
    );
    const reader = new CandidateSourceSnapshotDurableImmutableReader({
      databaseUrl: input.databaseUrl,
      fetchImpl: this.#fetch,
      localSource: input.localSource,
      plan: input.plan,
      retryDelay: this.#retryDelay,
    });
    const parquet = new Uint8Array(inventory.queryTable.expectedBytes);
    const ranges = partitionCandidateSourceSnapshotParquetRanges(
      parquet.byteLength,
      input.plan.requestEnvelope.finalVerification.parquetLogicalRequests,
    );
    await runWithConcurrency({
      concurrency: Math.min(
        input.plan.limits.maxConcurrency,
        MAXIMUM_VERIFICATION_CONCURRENCY,
      ),
      items: ranges,
      run: async (range, index) => {
        const bytes = await reader.readRange({
          checkKind: "query_table",
          logicalRequestSequence: NON_PARQUET_LOGICAL_READS + index + 1,
          object: inventory.queryTable,
          range,
        });
        parquet.set(bytes, range.start);
      },
    });
    if (
      sha256(Buffer.from(parquet)) !== inventory.queryTable.expectedSha256 ||
      (await calculateIpfsCid(parquet)) !== inventory.queryTable.expectedCid
    ) {
      throw new Error(
        "Reassembled Parquet failed its immutable CID/hash binding",
      );
    }

    const bytesByCid = new Map<string, Uint8Array>([
      [inventory.queryTable.expectedCid, parquet],
    ]);
    for (const [index, object] of inventory.nonParquet.entries()) {
      const bytes = await reader.readFull({
        checkKind: candidateSourceSnapshotRemoteCheckKind(object, input.plan),
        logicalRequestSequence: index + 1,
        object,
      });
      const existing = bytesByCid.get(object.expectedCid);
      if (
        existing &&
        sha256(Buffer.from(existing)) !== sha256(Buffer.from(bytes))
      ) {
        throw new Error("Duplicate immutable CID returned inconsistent bytes");
      }
      bytesByCid.set(object.expectedCid, bytes);
    }

    const manifestBytes = bytesByCid.get(
      input.plan.controlArtifacts.manifestIndex.expectedCid,
    );
    if (!manifestBytes) throw new Error("Verified compact manifest is missing");
    const compactManifest: CompactPublicationManifestIndex =
      await readBoundCompactPublicationManifestIndex({
        binding: input.plan.controlArtifacts.manifestIndex,
        bytes: manifestBytes,
      });
    const rangeTransport = new MemoryRangeTransport(
      inventory.queryTable.expectedCid,
      parquet,
    );
    const config = providerConfig(input.plan, inventory);
    const query = await readPublicSourceSnapshotQueryTable({
      compactManifest,
      config,
      plan: input.plan,
      rangeTransport,
    });
    await verifyCompleteGraphAndControls({
      bytesByCid,
      inventory,
      plan: input.plan,
      query,
    });
    const contracts = await McpContractRegistry.create();
    const provider = await PublicIpnsProvider.create(
      config,
      contracts,
      new MemoryPublicTransport(bytesByCid, input.plan),
      undefined,
      undefined,
      {
        rangeTransport,
        sourceSnapshotQueryTableReader: async () => query,
      },
    );
    const metadata = await provider.getMetadata();
    const queryRows = await provider.getQueryRows();
    if (
      metadata.canonicalDocumentCount !==
        input.plan.coverage.activeProperties ||
      metadata.coordinateCount !== input.plan.coverage.coordinateProperties ||
      metadata.coverageMode !== "source_snapshot" ||
      metadata.permitCoverage !== "unavailable" ||
      metadata.contractorCoverage !== "unavailable" ||
      queryRows.length !== input.plan.coverage.activeProperties
    ) {
      throw new Error("Credential-free provider metadata is inconsistent");
    }
    const selectedIds = new Set(
      inventory.selectedProperties.map((object) => {
        const match = object.logicalObjectKey.match(
          /^properties\/(property_[a-f0-9]{32})\.json$/,
        );
        if (!match?.[1]) {
          throw new Error("Selected property object key is invalid");
        }
        return `prop_${match[1].slice("property_".length)}`;
      }),
    );
    const searchable = queryRows.find(
      (row) =>
        selectedIds.has(row.propertyId) &&
        row.latitude !== null &&
        row.longitude !== null,
    );
    if (!searchable) {
      throw new Error(
        "Credential-free verification lacks a coordinate-bound property sample",
      );
    }
    const semanticProvider: OracleMcpProvider = {
      getCanonicalProperty: async (propertyId, signal) =>
        await provider.getCanonicalProperty(propertyId, signal),
      getMetadata: async (signal) => await provider.getMetadata(signal),
      getPermit: async (permitId, signal) =>
        await provider.getPermit(permitId, signal),
      getQueryRows: async () => [searchable],
    };
    const runtime = new OracleMcpRuntime(semanticProvider, contracts, {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2_097_152,
      requestTimeoutMs: 30_000,
    });
    const search = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      {
        center: {
          kind: "coordinates",
          latitude: searchable.latitude,
          longitude: searchable.longitude,
        },
        county: "pasco",
        filters: {},
        page: { limit: 1 },
        radius: { unit: "km", value: 0.001 },
        sort: "distance_asc",
      },
    );
    const searchData = recordValue(search.result.data, "search result");
    const opportunities = searchData.opportunities;
    if (
      search.isError ||
      !Array.isArray(opportunities) ||
      opportunities.length !== 1 ||
      recordValue(
        recordValue(opportunities[0], "search opportunity").property,
        "search property",
      ).propertyId !== searchable.propertyId
    ) {
      throw new Error("Credential-free semantic search validation failed");
    }
    const propertyLookup = await runtime.execute("prism_v1_get_property", {
      propertyId: searchable.propertyId,
    });
    if (
      propertyLookup.isError ||
      recordValue(propertyLookup.result.data, "property result").propertyId !==
        searchable.propertyId
    ) {
      throw new Error("Credential-free semantic property validation failed");
    }
    for (const propertyId of selectedIds) {
      if (!(await provider.getCanonicalProperty(propertyId))) {
        throw new Error("Selected canonical property is unavailable");
      }
    }

    const checkedAt = new Date().toISOString();
    const rootObject = inventory.nonParquet.find(
      (object) => object.expectedCid === input.plan.targets.openData.targetCid,
    )!;
    const checks: Array<{
      checkKind: CandidateSourceSnapshotRemoteCheckKind;
      metrics: unknown;
      object: CandidateSourceSnapshotRemoteVerifierObject;
    }> = [
      {
        checkKind: "plan_artifact",
        metrics: {},
        object: inventory.planArtifact,
      },
      {
        checkKind: "manifest",
        metrics: {},
        object: inventory.nonParquet.find(
          (object) =>
            object.remoteObjectKey ===
            input.plan.controlArtifacts.manifestIndex.objectKey,
        )!,
      },
      {
        checkKind: "inventory",
        metrics: {
          entryCount: input.plan.controlArtifacts.objectInventory.entryCount,
          integrityRootSha256:
            input.plan.controlArtifacts.objectInventory.integrityRootSha256,
          shardCount: input.plan.controlArtifacts.objectInventory.shardCount,
        },
        object: objectByLogicalKey(inventory, "object_inventory/index.json"),
      },
      {
        checkKind: "open_data_graph",
        metrics: {
          propertyCount: input.plan.coverage.activeProperties,
          traversalValid: true,
        },
        object: rootObject,
      },
      {
        checkKind: "query_table",
        metrics: {
          distinctPropertyIdCount: input.plan.coverage.activeProperties,
          nullPropertyIdCount: 0,
          propertyCidCorrespondence: true,
          propertyCount: input.plan.coverage.activeProperties,
          propertyLookupValidated: true,
          semanticSearchValidated: true,
        },
        object: inventory.queryTable,
      },
      {
        checkKind: "coverage",
        metrics: input.plan.coverage,
        object: rootObject,
      },
      {
        checkKind: "fixture_exclusion",
        metrics: { fixtureMatchCount: 0 },
        object: rootObject,
      },
    ];
    for (const check of checks) {
      await recordCandidateSourceSnapshotRemoteCheck(input.databaseUrl, {
        checkKind: check.checkKind,
        checkedAt,
        metrics: check.metrics,
        observedBytes: check.object.expectedBytes,
        observedCid: check.object.expectedCid,
        observedSha256: check.object.expectedSha256,
        planId: input.plan.planId,
        planSha256: input.plan.planSha256,
      });
    }
  }
}
