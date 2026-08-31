import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { open, readFile, readdir, realpath, stat } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

import {
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
  MCP_TOOL_NAMES,
} from "../src/mcp/constants.js";
import { McpContractRegistry } from "../src/mcp/contracts.js";
import {
  explorerBootstrap,
  explorerProperty,
  explorerSearch,
} from "../src/mcp/explorer.js";
import type { PublicIpnsProviderConfig } from "../src/mcp/config.js";
import {
  PublicIpnsProvider,
  type IpnsResolutionObservation,
  type PublicReadTransport,
} from "../src/mcp/public-ipns-provider.js";
import type {
  PublicCidObjectMetadata,
  PublicCidRangeTransport,
} from "../src/mcp/public-cid-range.js";
import { haversineMeters, OracleMcpRuntime } from "../src/mcp/runtime.js";
import { calculateIpfsCid } from "../src/publication/ipfs-cid.js";
import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "../src/publication/candidate-source-snapshot-demo.js";

const DEFAULT_DESCRIPTOR =
  "data/evidence/candidate-source-snapshot-demo/build-descriptor.json";
const MAX_DESCRIPTOR_BYTES = 2 * 1024 * 1024;
const MAX_DISCOVERED_FILES = 128;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;

type JsonObject = Record<string, unknown>;

interface BuildDescriptor {
  controlOutputRoot: string;
  planArtifactOutputRoot: string;
  sourceManifestFileSha256: string;
  sourceManifestPath: string;
  sourcePlanFileSha256: string;
  sourcePlanPath: string;
  targets: {
    openData: { ipnsNetworkKey: string; targetCid: string };
    queryTable: { ipnsNetworkKey: string; targetCid: string };
  };
  version: "1.0.0";
}

interface BoundFile {
  byteSize: number;
  label: string;
  path: string;
  sourcePayload: boolean;
}

interface ControlArtifact {
  byteSize: number;
  expectedCid: string;
  objectKey: string;
}

interface ControlIndex {
  shards: ControlArtifact[];
}

function invariant(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function object(value: unknown, label: string): JsonObject {
  invariant(
    value !== null && typeof value === "object" && !Array.isArray(value),
    `${label} is invalid`,
  );
  return value as JsonObject;
}

function string(value: unknown, label: string): string {
  invariant(
    typeof value === "string" && value.length > 0,
    `${label} is invalid`,
  );
  return value;
}

function integer(value: unknown, label: string): number {
  invariant(
    Number.isSafeInteger(value) && Number(value) >= 0,
    `${label} is invalid`,
  );
  return Number(value);
}

function parseDescriptor(value: unknown): BuildDescriptor {
  const input = object(value, "build descriptor");
  const targets = object(input.targets, "build descriptor targets");
  const openData = object(
    targets.openData,
    "build descriptor open-data target",
  );
  const queryTable = object(
    targets.queryTable,
    "build descriptor query-table target",
  );
  invariant(
    input.version === "1.0.0",
    "Build descriptor version is unsupported",
  );
  const descriptor: BuildDescriptor = {
    controlOutputRoot: string(
      input.controlOutputRoot,
      "build descriptor control output root",
    ),
    planArtifactOutputRoot: string(
      input.planArtifactOutputRoot,
      "build descriptor plan output root",
    ),
    sourceManifestFileSha256: string(
      input.sourceManifestFileSha256,
      "build descriptor source manifest hash",
    ),
    sourceManifestPath: string(
      input.sourceManifestPath,
      "build descriptor source manifest path",
    ),
    sourcePlanFileSha256: string(
      input.sourcePlanFileSha256,
      "build descriptor source plan hash",
    ),
    sourcePlanPath: string(
      input.sourcePlanPath,
      "build descriptor source plan path",
    ),
    targets: {
      openData: {
        ipnsNetworkKey: string(
          openData.ipnsNetworkKey,
          "build descriptor open-data identity",
        ),
        targetCid: string(openData.targetCid, "build descriptor open-data CID"),
      },
      queryTable: {
        ipnsNetworkKey: string(
          queryTable.ipnsNetworkKey,
          "build descriptor query-table identity",
        ),
        targetCid: string(
          queryTable.targetCid,
          "build descriptor query-table CID",
        ),
      },
    },
    version: "1.0.0",
  };
  invariant(
    SHA256_PATTERN.test(descriptor.sourceManifestFileSha256) &&
      SHA256_PATTERN.test(descriptor.sourcePlanFileSha256),
    "Build descriptor source hashes are invalid",
  );
  return descriptor;
}

function isInside(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}${path.sep}`);
}

async function containedPath(
  dataRoot: string,
  candidate: string,
  kind: "directory" | "file",
): Promise<string> {
  const resolved = await realpath(
    path.isAbsolute(candidate) ? candidate : path.resolve(candidate),
  );
  invariant(isInside(dataRoot, resolved), "Verifier input escapes DATA_DIR");
  const metadata = await stat(resolved);
  invariant(
    kind === "file" ? metadata.isFile() : metadata.isDirectory(),
    `Verifier ${kind} input is invalid`,
  );
  return resolved;
}

async function containedObjectPath(
  dataRoot: string,
  objectRoot: string,
  objectKey: string,
): Promise<string> {
  invariant(
    !path.isAbsolute(objectKey) &&
      objectKey
        .split("/")
        .every((segment) => segment !== ".." && segment !== ""),
    "Publication object key is unsafe",
  );
  return containedPath(
    dataRoot,
    path.join(objectRoot, ...objectKey.split("/")),
    "file",
  );
}

async function sha256File(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.once("error", reject);
    stream.once("end", resolve);
  });
  return hash.digest("hex");
}

async function readJson(
  filePath: string,
  maximumBytes: number,
): Promise<JsonObject> {
  const metadata = await stat(filePath);
  invariant(
    metadata.size <= maximumBytes,
    "Local JSON artifact exceeds its verifier bound",
  );
  return object(
    JSON.parse(await readFile(filePath, "utf8")),
    "local JSON artifact",
  );
}

async function findPlanArtifacts(root: string): Promise<string[]> {
  const pending = [root];
  const matches: string[] = [];
  let discovered = 0;
  while (pending.length > 0) {
    const directory = pending.pop()!;
    const entries = await readdir(directory, { withFileTypes: true });
    for (const entry of entries) {
      discovered += 1;
      invariant(
        discovered <= MAX_DISCOVERED_FILES,
        "Plan artifact tree exceeds its bound",
      );
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) pending.push(target);
      if (
        entry.isFile() &&
        entry.name === "candidate-source-snapshot-plan.json"
      ) {
        matches.push(target);
      }
    }
  }
  invariant(matches.length > 0, "Candidate plan artifact is unavailable");
  return matches.sort();
}

function controlArtifact(value: unknown, label: string): ControlArtifact {
  const input = object(value, label);
  return {
    byteSize: integer(input.byteSize, `${label} byte size`),
    expectedCid: string(input.expectedCid, `${label} CID`),
    objectKey: string(input.objectKey, `${label} object key`),
  };
}

class LocalCidTransport implements PublicReadTransport {
  readonly #files = new Map<string, BoundFile>();
  readonly #resolutions = new Map<string, string>();
  readonly reads: Array<{
    bytes: number;
    label: string;
    sourcePayload: boolean;
  }> = [];

  addResolution(identity: string, cid: string): void {
    this.#resolutions.set(identity, cid);
  }

  async addFile(
    cid: string,
    filePath: string,
    label: string,
    sourcePayload: boolean,
    expectedBytes?: number,
  ): Promise<void> {
    const metadata = await stat(filePath);
    invariant(
      metadata.isFile() && metadata.size > 0,
      `${label} is not a non-empty file`,
    );
    if (expectedBytes !== undefined) {
      invariant(metadata.size === expectedBytes, `${label} byte size changed`);
    }
    const existing = this.#files.get(cid);
    if (existing) {
      invariant(
        existing.byteSize === metadata.size,
        `Duplicate CID binding differs for ${label}`,
      );
      return;
    }
    this.#files.set(cid, {
      byteSize: metadata.size,
      label,
      path: filePath,
      sourcePayload,
    });
  }

  async addVerifiedFile(
    expectedCid: string,
    filePath: string,
    label: string,
    sourcePayload: boolean,
  ): Promise<void> {
    const bytes = await readFile(filePath);
    invariant(
      (await calculateIpfsCid(bytes)) === expectedCid,
      `${label} CID changed`,
    );
    await this.addFile(
      expectedCid,
      filePath,
      label,
      sourcePayload,
      bytes.byteLength,
    );
  }

  async readCid(
    cid: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    if (signal?.aborted) throw signal.reason ?? new Error("Operation aborted");
    const binding = this.#files.get(cid);
    invariant(binding, "Local verifier received an unbound CID read");
    invariant(
      binding.byteSize <= maximumBytes,
      `${binding.label} exceeds its read bound`,
    );
    const bytes = await readFile(binding.path);
    invariant(
      bytes.byteLength === binding.byteSize,
      `${binding.label} changed during verification`,
    );
    invariant(
      (await calculateIpfsCid(bytes)) === cid,
      `${binding.label} no longer matches its CID`,
    );
    this.reads.push({
      bytes: bytes.byteLength,
      label: binding.label,
      sourcePayload: binding.sourcePayload,
    });
    return bytes;
  }

  async resolveIpns(
    identity: string,
  ): Promise<readonly IpnsResolutionObservation[]> {
    const cid = this.#resolutions.get(identity);
    invariant(cid, "Local verifier received an unknown IPNS identity");
    const observedAt = new Date().toISOString();
    return ["local-bound-resolver-a", "local-bound-resolver-b"].map(
      (resolver) => ({
        cacheAgeSeconds: 0,
        cid,
        observedAt,
        resolver,
        status: "resolved" as const,
      }),
    );
  }
}

class LocalParquetRangeTransport implements PublicCidRangeTransport {
  readonly #byteSize: number;
  readonly #cid: string;
  readonly #filePath: string;
  rangeBytes = 0;
  rangeRequests = 0;
  statRequests = 0;

  private constructor(cid: string, filePath: string, byteSize: number) {
    this.#cid = cid;
    this.#filePath = filePath;
    this.#byteSize = byteSize;
  }

  static async create(
    cid: string,
    filePath: string,
  ): Promise<LocalParquetRangeTransport> {
    const metadata = await stat(filePath);
    invariant(
      metadata.isFile() && metadata.size > 8,
      "Local Parquet artifact is invalid",
    );
    const bytes = await readFile(filePath);
    invariant(
      (await calculateIpfsCid(bytes)) === cid,
      "Local Parquet CID changed",
    );
    return new LocalParquetRangeTransport(cid, filePath, metadata.size);
  }

  async statCid(
    cid: string,
    maximumObjectBytes: number,
    signal?: AbortSignal,
  ): Promise<PublicCidObjectMetadata> {
    if (signal?.aborted) throw signal.reason ?? new Error("Operation aborted");
    invariant(cid === this.#cid, "Range transport received an unknown CID");
    invariant(
      this.#byteSize <= maximumObjectBytes,
      "Parquet exceeds its object bound",
    );
    this.statRequests += 1;
    return { acceptsByteRanges: true, byteLength: this.#byteSize, cid };
  }

  async readCidRange(
    cid: string,
    start: number,
    endExclusive: number,
    expectedObjectBytes: number,
    maximumResponseBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    if (signal?.aborted) throw signal.reason ?? new Error("Operation aborted");
    invariant(cid === this.#cid, "Range transport received an unknown CID");
    invariant(
      expectedObjectBytes === this.#byteSize,
      "Parquet size binding changed",
    );
    invariant(
      Number.isSafeInteger(start) &&
        Number.isSafeInteger(endExclusive) &&
        start >= 0 &&
        endExclusive > start &&
        endExclusive <= this.#byteSize &&
        endExclusive - start <= maximumResponseBytes,
      "Requested Parquet range is outside its bound",
    );
    const length = endExclusive - start;
    const handle = await open(this.#filePath, "r");
    try {
      const output = Buffer.allocUnsafe(length);
      const result = await handle.read(output, 0, length, start);
      invariant(
        result.bytesRead === length,
        "Local Parquet range was truncated",
      );
      this.rangeRequests += 1;
      this.rangeBytes += result.bytesRead;
      return output;
    } finally {
      await handle.close();
    }
  }
}

async function addControlCollection(options: {
  controlRoot: string;
  dataRoot: string;
  reference: unknown;
  transport: LocalCidTransport;
}): Promise<void> {
  const reference = object(options.reference, "control collection reference");
  const indexArtifact = controlArtifact(
    reference.indexArtifact,
    "control collection index",
  );
  const indexPath = await containedObjectPath(
    options.dataRoot,
    options.controlRoot,
    indexArtifact.objectKey,
  );
  await options.transport.addFile(
    indexArtifact.expectedCid,
    indexPath,
    `control:${string(reference.collection, "control collection")}:index`,
    false,
    indexArtifact.byteSize,
  );
  const indexValue = (await readJson(
    indexPath,
    1024 * 1024,
  )) as unknown as ControlIndex;
  invariant(
    Array.isArray(indexValue.shards) && indexValue.shards.length > 0,
    "Control index has no shards",
  );
  for (const [position, value] of indexValue.shards.entries()) {
    const shard = controlArtifact(value, "control shard");
    await options.transport.addFile(
      shard.expectedCid,
      await containedObjectPath(
        options.dataRoot,
        options.controlRoot,
        shard.objectKey,
      ),
      `control:${String(reference.collection)}:shard:${position}`,
      false,
      shard.byteSize,
    );
  }
}

function forbiddenExplorerKey(key: string): boolean {
  const normalized = key.toLowerCase().replaceAll(/[^a-z0-9]/g, "");
  return new Set([
    "apn",
    "contractor",
    "contractorid",
    "contractoridentity",
    "contractorname",
    "exactfolio",
    "folio",
    "folioid",
    "folionumber",
    "mailingaddress",
    "mailingaddress1",
    "mailingaddress2",
    "owneremail",
    "ownername",
    "ownerphone",
    "parcel",
    "parcelid",
    "parcelidentifier",
    "parcelnumber",
    "permitid",
    "permitidentifier",
    "permitnumber",
    "phonenumber",
    "requestidentifier",
    "sourcerecordkey",
    "taxparcelid",
  ]).has(normalized);
}

function assertExplorerPrivacy(value: unknown): void {
  if (Array.isArray(value)) {
    for (const entry of value) assertExplorerPrivacy(entry);
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    invariant(
      !forbiddenExplorerKey(key),
      "Explorer projection exposed a forbidden field",
    );
    assertExplorerPrivacy(entry);
  }
}

function resultData(value: JsonObject): JsonObject {
  return object(value.data, "MCP response data");
}

function searchOpportunities(value: JsonObject): JsonObject[] {
  const opportunities = resultData(value).opportunities;
  invariant(
    Array.isArray(opportunities),
    "Search opportunities are unavailable",
  );
  return opportunities.map((entry) => object(entry, "search opportunity"));
}

async function main(): Promise<void> {
  const argumentIndex = process.argv.indexOf("--descriptor");
  invariant(
    argumentIndex < 0 || process.argv[argumentIndex + 1],
    "--descriptor requires a value",
  );
  const dataRoot = await realpath(path.resolve(process.env.DATA_DIR ?? "data"));
  const descriptorPath = await containedPath(
    dataRoot,
    path.resolve(
      argumentIndex < 0 ? DEFAULT_DESCRIPTOR : process.argv[argumentIndex + 1]!,
    ),
    "file",
  );
  const descriptor = parseDescriptor(
    await readJson(descriptorPath, MAX_DESCRIPTOR_BYTES),
  );
  const controlRoot = await containedPath(
    dataRoot,
    descriptor.controlOutputRoot,
    "directory",
  );
  const planArtifactRoot = await containedPath(
    dataRoot,
    descriptor.planArtifactOutputRoot,
    "directory",
  );
  const sourcePlanPath = await containedPath(
    dataRoot,
    descriptor.sourcePlanPath,
    "file",
  );
  const sourceManifestPath = await containedPath(
    dataRoot,
    descriptor.sourceManifestPath,
    "file",
  );
  invariant(
    (await sha256File(sourcePlanPath)) === descriptor.sourcePlanFileSha256,
    "Bound source publication plan bytes changed",
  );
  invariant(
    (await sha256File(sourceManifestPath)) ===
      descriptor.sourceManifestFileSha256,
    "Bound source manifest bytes changed",
  );

  const planCandidates = await Promise.all(
    (await findPlanArtifacts(planArtifactRoot)).map(async (candidate) => ({
      bytes: await readFile(candidate),
      path: candidate,
    })),
  );
  const matchingPlans: Array<{
    bytes: Buffer;
    path: string;
    plan: CandidateSourceSnapshotDemoPlan;
  }> = [];
  for (const candidate of planCandidates) {
    try {
      const parsed = validateCandidateSourceSnapshotDemoPlan(
        JSON.parse(candidate.bytes.toString("utf8")),
      );
      if (
        parsed.targets.openData.targetCid ===
          descriptor.targets.openData.targetCid &&
        parsed.targets.openData.ipnsNetworkKey ===
          descriptor.targets.openData.ipnsNetworkKey &&
        parsed.targets.queryTable.targetCid ===
          descriptor.targets.queryTable.targetCid &&
        parsed.targets.queryTable.ipnsNetworkKey ===
          descriptor.targets.queryTable.ipnsNetworkKey
      ) {
        matchingPlans.push({ ...candidate, plan: parsed });
      }
    } catch {
      // Historical plan artifacts may coexist with the current descriptor.
    }
  }
  invariant(
    matchingPlans.length === 1,
    "Expected exactly one plan bound to the descriptor targets",
  );
  const { bytes: planBytes, path: planArtifactPath, plan } = matchingPlans[0]!;
  const planArtifactCid = await calculateIpfsCid(planBytes);
  const planArtifactSha256 = createHash("sha256")
    .update(planBytes)
    .digest("hex");
  const transport = new LocalCidTransport();
  await transport.addFile(
    planArtifactCid,
    planArtifactPath,
    "candidate-plan",
    false,
    planBytes.byteLength,
  );
  const controlNamespaceRoot = await containedPath(
    dataRoot,
    path.join(controlRoot, plan.namespaceId),
    "directory",
  );

  const manifestArtifact = controlArtifact(
    plan.controlArtifacts.manifestIndex,
    "compact manifest",
  );
  await transport.addFile(
    manifestArtifact.expectedCid,
    await containedObjectPath(
      dataRoot,
      controlNamespaceRoot,
      manifestArtifact.objectKey,
    ),
    "control:compact-manifest",
    false,
    manifestArtifact.byteSize,
  );
  for (const reference of [
    plan.controlArtifacts.manifestEntries,
    plan.controlArtifacts.graphEdges,
    plan.controlArtifacts.objectInventory,
  ]) {
    await addControlCollection({
      controlRoot: controlNamespaceRoot,
      dataRoot,
      reference,
      transport,
    });
  }

  const sourceRoot = path.dirname(sourcePlanPath);
  const openDataRoot = await containedPath(
    dataRoot,
    path.join(sourceRoot, "open-data"),
    "directory",
  );
  const rootPath = await containedPath(
    dataRoot,
    path.join(openDataRoot, "index.json"),
    "file",
  );
  await transport.addVerifiedFile(
    plan.targets.openData.targetCid,
    rootPath,
    "payload:open-data-root",
    true,
  );
  const root = await readJson(rootPath, 1024 * 1024);
  invariant(Array.isArray(root.shards), "Source root has no shard inventory");
  for (const value of root.shards) {
    const shard = object(value, "source root shard");
    const shardIndex = integer(shard.shardIndex, "source shard index");
    const shardCid = string(shard.shardCid, "source shard CID");
    const shardPath = await containedPath(
      dataRoot,
      path.join(
        openDataRoot,
        "shards",
        `shard-${String(shardIndex).padStart(4, "0")}.json`,
      ),
      "file",
    );
    await transport.addFile(
      shardCid,
      shardPath,
      `payload:shard:${shardIndex}`,
      true,
    );
  }
  const provenancePath = await containedObjectPath(
    dataRoot,
    controlNamespaceRoot,
    `${plan.targets.openData.immutablePrefix}provenance.json`,
  );
  const provenanceBytes = await readFile(provenancePath);
  await transport.addFile(
    await calculateIpfsCid(provenanceBytes),
    provenancePath,
    "payload:provenance",
    true,
    provenanceBytes.byteLength,
  );
  const queryPath = await containedObjectPath(
    dataRoot,
    controlNamespaceRoot,
    `${plan.targets.queryTable.immutablePrefix}query-table.parquet`,
  );
  const rangeTransport = await LocalParquetRangeTransport.create(
    plan.targets.queryTable.targetCid,
    queryPath,
  );
  transport.addResolution(
    plan.targets.openData.ipnsNetworkKey,
    plan.targets.openData.targetCid,
  );
  transport.addResolution(
    plan.targets.queryTable.ipnsNetworkKey,
    plan.targets.queryTable.targetCid,
  );

  const config: PublicIpnsProviderConfig = {
    candidateDemoPlanId: plan.planId,
    candidateDemoPlanSha256: plan.planSha256,
    candidateDemoSourcePlanSha256: plan.source.sourcePlanSha256,
    environment: "production",
    expectedManifestCid: manifestArtifact.expectedCid,
    expectedManifestSha256: string(
      object(plan.controlArtifacts.manifestIndex, "compact manifest").sha256,
      "compact manifest hash",
    ),
    expectedOpenDataRootCid: plan.targets.openData.targetCid,
    expectedPlanCid: planArtifactCid,
    expectedPlanSha256: planArtifactSha256,
    expectedQueryTableRootCid: plan.targets.queryTable.targetCid,
    limits: {
      maxCacheAgeSeconds: 300,
      maxJsonObjectBytes: 8 * 1024 * 1024,
      maxParquetBytes: 128 * 1024 * 1024,
      maxRedirects: 0,
      retries: 0,
      transportTimeoutMs: 20_000,
    },
    mode: "public-ipns",
    openDataIpns: plan.targets.openData.ipnsNetworkKey,
    queryTableIpns: plan.targets.queryTable.ipnsNetworkKey,
    resolverPolicy: "candidate_filebase_delegated_v2",
  };
  const contracts = await McpContractRegistry.create();
  const stages: string[] = [];
  let peakRss = process.memoryUsage().rss;
  const memorySampler = setInterval(() => {
    peakRss = Math.max(peakRss, process.memoryUsage().rss);
  }, 25);
  memorySampler.unref();
  const coldStarted = performance.now();
  const provider = await PublicIpnsProvider.create(
    config,
    contracts,
    transport,
    undefined,
    (stage) => stages.push(stage),
    { rangeTransport },
  );
  const coldInitializationMs = performance.now() - coldStarted;
  const coldReads = [...transport.reads];
  const coldStages = [...stages];
  invariant(
    rangeTransport.rangeRequests === 0,
    "Cold initialization read Parquet ranges",
  );
  invariant(
    coldReads.every((read) => !read.sourcePayload),
    "Cold initialization read a source payload object",
  );
  invariant(
    !coldStages.includes("parquet"),
    "Cold initialization entered Parquet stage",
  );
  const metadata = await provider.getMetadata();
  invariant(
    metadata.coverageMode === "source_snapshot",
    "Coverage mode changed",
  );
  invariant(
    metadata.canonicalDocumentCount === plan.coverage.activeProperties &&
      metadata.coordinateCount === plan.coverage.coordinateProperties,
    "Cold metadata coverage differs from the immutable plan",
  );
  invariant(
    metadata.permitCoverage === "unavailable" &&
      metadata.contractorCoverage === "unavailable",
    "Unavailable source semantics changed",
  );

  const firstQueryStarted = performance.now();
  const rows = await provider.getQueryRows();
  const firstQueryMs = performance.now() - firstQueryStarted;
  invariant(
    rows.length === plan.coverage.activeProperties,
    "Parquet row count changed",
  );
  invariant(
    rangeTransport.rangeRequests > 0,
    "First query did not use range-backed Parquet",
  );
  const coordinateRows = rows.filter(
    (row) => row.latitude !== null && row.longitude !== null,
  );
  invariant(
    coordinateRows.length === plan.coverage.coordinateProperties,
    "Parquet coordinate coverage changed",
  );
  const center = coordinateRows[0];
  invariant(
    center !== undefined &&
      center.latitude !== null &&
      center.longitude !== null,
    "Search center unavailable",
  );
  const withinRadius = coordinateRows
    .map((row) => ({
      distance: haversineMeters(
        center.latitude!,
        center.longitude!,
        row.latitude!,
        row.longitude!,
      ),
      row,
    }))
    .filter((entry) => entry.distance <= 80_467.2 + 1e-7)
    .sort(
      (left, right) =>
        left.distance - right.distance ||
        left.row.propertyId.localeCompare(right.row.propertyId),
    );
  invariant(
    withinRadius.length >= 4,
    "Bounded search does not contain four properties",
  );
  for (const entry of withinRadius.slice(0, 8)) {
    const propertyPath = await containedPath(
      dataRoot,
      path.join(
        openDataRoot,
        "properties",
        `${entry.row.canonicalPropertyId}.json`,
      ),
      "file",
    );
    const bytes = await readFile(propertyPath);
    await transport.addFile(
      await calculateIpfsCid(bytes),
      propertyPath,
      "payload:selected-property",
      true,
      bytes.byteLength,
    );
  }
  for (const [position, entry] of withinRadius.slice(0, 4).entries()) {
    try {
      invariant(
        (await provider.getCanonicalProperty(entry.row.propertyId)) !== null,
        "Selected property was not found",
      );
    } catch (error) {
      const code =
        error !== null &&
        typeof error === "object" &&
        "code" in error &&
        typeof error.code === "string"
          ? error.code
          : "unknown";
      throw new Error(
        `Selected property verification failed at position ${position} (${code}:${error instanceof Error ? error.message : "unknown"})`,
        { cause: error },
      );
    }
  }

  const runtime = new OracleMcpRuntime(provider, contracts, {
    maxRequestBytes: 64 * 1024,
    maxResponseBytes: 2 * 1024 * 1024,
    requestTimeoutMs: 30_000,
  });
  const searchRequest = {
    county: "pasco",
    center: {
      kind: "coordinates",
      latitude: center.latitude,
      longitude: center.longitude,
    },
    radius: { unit: "km", value: 80.4672 },
    filters: { matchMode: "all" },
    sort: "distance_asc",
    page: { limit: 2 },
  };
  const firstPage = await runtime.execute(
    "prism_v1_search_roofing_opportunities",
    searchRequest,
  );
  if (firstPage.isError) {
    const failure = object(firstPage.result.error, "first search error");
    const details = object(failure.details ?? {}, "first search error details");
    const recentReads = transport.reads
      .slice(-12)
      .map((read) => read.label)
      .join(",");
    throw new Error(
      `First deterministic search page failed (${String(failure.code)}:${String(failure.dependency ?? "none")}:${String(details.publicReadCode ?? "none")}; reads=${recentReads})`,
    );
  }
  const firstOpportunities = searchOpportunities(firstPage.result);
  invariant(firstOpportunities.length === 2, "First search page is incomplete");
  const cursor = object(firstPage.result.meta, "search metadata").nextCursor;
  invariant(
    typeof cursor === "string" && cursor.length > 0,
    "Search cursor is unavailable",
  );
  const secondPage = await runtime.execute(
    "prism_v1_search_roofing_opportunities",
    { ...searchRequest, page: { cursor, limit: 2 } },
  );
  invariant(!secondPage.isError, "Second deterministic search page failed");
  const secondOpportunities = searchOpportunities(secondPage.result);
  invariant(
    secondOpportunities.length === 2,
    "Second search page is incomplete",
  );
  const replay = await runtime.execute(
    "prism_v1_search_roofing_opportunities",
    searchRequest,
  );
  invariant(
    JSON.stringify(replay.result) === JSON.stringify(firstPage.result),
    "Deterministic search replay changed",
  );
  const firstProperty = object(
    firstOpportunities[0]!.property,
    "first property",
  );
  const propertyId = string(firstProperty.propertyId, "public property ID");
  const property = await runtime.execute("prism_v1_get_property", {
    propertyId,
  });
  invariant(!property.isError, "Property lookup failed");
  const permit = await runtime.execute("prism_v1_get_permit", {
    permitId: `perm_${"f".repeat(32)}`,
  });
  invariant(
    permit.isError,
    "Unavailable permit lookup did not fail explicitly",
  );
  const permitError = object(permit.result.error, "permit error");
  invariant(
    permitError.code === "data_unavailable",
    "Permit failure does not report unavailable coverage",
  );

  const metadataTools = [
    "prism_v1_get_service_info",
    "prism_v1_get_pipeline_run_summary",
    "prism_v1_get_query_schema",
  ] as const;
  for (const tool of metadataTools) {
    const result = await runtime.execute(tool, {});
    invariant(!result.isError, `${tool} failed`);
    const meta = object(result.result.meta, `${tool} metadata`);
    invariant(
      meta.contractVersion === MCP_CONTRACT_VERSION &&
        meta.schemaHash === MCP_SCHEMA_SHA256,
      `${tool} contract identity changed`,
    );
  }
  invariant(MCP_TOOL_NAMES.length === 6, "Frozen MCP tool count changed");
  invariant(
    new Set(MCP_TOOL_NAMES).size === 6,
    "Frozen MCP tools are not unique",
  );

  const [bootstrap, explorerFirstPage, explorerPropertyValue] =
    await Promise.all([
      explorerBootstrap(runtime),
      explorerSearch(runtime, searchRequest),
      explorerProperty(runtime, { propertyId }),
    ]);
  assertExplorerPrivacy(explorerFirstPage);
  assertExplorerPrivacy(explorerPropertyValue);
  const bootstrapPublication = object(
    bootstrap.publication,
    "explorer publication",
  );
  invariant(
    bootstrapPublication.coverageMode === "source_snapshot" &&
      bootstrapPublication.propertyCount === plan.coverage.activeProperties &&
      bootstrapPublication.coordinateCount ===
        plan.coverage.coordinateProperties,
    "Explorer coverage identity changed",
  );
  const bootstrapText = JSON.stringify(bootstrap);
  invariant(
    bootstrapText.includes("owner-assumed") &&
      bootstrapText.includes("not independent Pasco certification") &&
      bootstrapText.includes("Permit and contractor sources are unavailable"),
    "Explorer limitations omit required source-snapshot disclosure",
  );
  clearInterval(memorySampler);
  peakRss = Math.max(peakRss, process.memoryUsage().rss);

  console.log(
    JSON.stringify(
      {
        coldInitialization: {
          cidBytes: coldReads.reduce((sum, read) => sum + read.bytes, 0),
          cidReads: coldReads.length,
          milliseconds: Math.round(coldInitializationMs),
          payloadReads: coldReads.filter((read) => read.sourcePayload).length,
          rangeBytes: 0,
          rangeRequests: 0,
          stages: coldStages,
        },
        coverage: {
          coordinates: metadata.coordinateCount,
          mode: metadata.coverageMode,
          properties: metadata.canonicalDocumentCount,
          contractors: metadata.contractorCoverage,
          permits: metadata.permitCoverage,
        },
        deterministicSearch: {
          firstPage: firstOpportunities.length,
          replayMatched: true,
          secondPage: secondOpportunities.length,
        },
        firstQuery: {
          milliseconds: Math.round(firstQueryMs),
          rangeBytes: rangeTransport.rangeBytes,
          rangeRequests: rangeTransport.rangeRequests,
          rows: rows.length,
          stages: stages.slice(coldStages.length),
          statRequests: rangeTransport.statRequests,
        },
        identity: {
          contractVersion: MCP_CONTRACT_VERSION,
          planArtifactCid,
          planArtifactSha256,
          planId: plan.planId,
          planSha256: plan.planSha256,
          schemaSha256: MCP_SCHEMA_SHA256,
          toolCount: MCP_TOOL_NAMES.length,
        },
        memory: { peakRssBytes: peakRss },
        ok: true,
        privacy: { explorerForbiddenFields: 0 },
      },
      null,
      2,
    ),
  );
}

main().catch((error: unknown) => {
  console.error(
    JSON.stringify({
      error:
        error instanceof Error
          ? error.message.replaceAll(process.cwd(), "<repository>")
          : "Unknown verifier failure",
      ok: false,
    }),
  );
  process.exitCode = 1;
});
