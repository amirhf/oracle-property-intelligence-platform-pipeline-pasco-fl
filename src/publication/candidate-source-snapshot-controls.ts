import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  lstat,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline";

import { DuckDBInstance } from "@duckdb/node-api";
import { z } from "zod";

import {
  createPublicationControlArtifactsBinding,
  shardedControlUploadArtifacts,
  toTypedControlCollectionReference,
  verifyLocalShardedControlCollection,
  writeCompactPublicationManifestIndex,
  writeShardedControlCollection,
  type CompactPublicationManifestIndex,
  type ControlArtifactBinding,
  type ControlEntry,
  type PublicationControlArtifactsBinding,
} from "./control-artifacts.js";
import { canonicalJson } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import { calculateIpfsCid, calculateIpfsFileCid } from "./ipfs-cid.js";
import { publicationArtifactSchema, type PublicationArtifact } from "./plan.js";

const MAX_STREAMED_JSON_ENTRY_BYTES = 2 * 1024 * 1024;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const SEMANTIC_SORT_RUN_BYTES = 4 * 1024 * 1024;
const namespaceIdSchema = z.string().regex(/^snapshotns_[a-f0-9]{32}$/);
const graphEdgeSchema = z.strictObject({
  childCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  childKey: z.string().min(1).max(2_048),
  jsonPointer: z.string().min(1).max(4_096),
  parentKey: z.string().min(1).max(2_048),
});
const manifestEntrySchema = z.strictObject({
  bytes: z.number().int().positive(),
  cid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  objectKey: z.string().min(1).max(2_048),
  parcelIdentifier: z.string().min(1).max(500),
  propertyId: z.string().regex(/^property_[a-f0-9]{32}$/),
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
});

const semanticRecordSchema = z.strictObject({
  key: z.string().min(1).max(4_096),
  value: z.unknown(),
});

type SemanticRecord = z.infer<typeof semanticRecordSchema>;
type DuplicatePolicy = "deduplicate_identical" | "reject";

function selectSemanticRunEntry(
  current: readonly IteratorResult<string>[],
): { encodedValue: string; index: number; record: SemanticRecord } | null {
  let selected: {
    encodedValue: string;
    index: number;
    record: SemanticRecord;
  } | null = null;
  for (const [index, next] of current.entries()) {
    if (next.done) continue;
    const record = semanticRecordSchema.parse(JSON.parse(next.value));
    const encodedValue = canonicalJson(record.value);
    if (
      selected === null ||
      record.key < selected.record.key ||
      (record.key === selected.record.key &&
        encodedValue < selected.encodedValue)
    ) {
      selected = { encodedValue, index, record };
    }
  }
  return selected;
}

/**
 * A bounded external sorter used only while converting the already validated
 * legacy controls. Each in-memory run is capped, and the final merge retains
 * one line per run rather than the countywide inventory.
 */
class ExternalSemanticSorter {
  readonly #duplicatePolicy: DuplicatePolicy;
  readonly #label: string;
  readonly #root: string;
  readonly #runs: string[] = [];
  #finished = false;
  #pending: Array<{ bytes: Buffer; record: SemanticRecord }> = [];
  #pendingBytes = 0;

  constructor(options: {
    duplicatePolicy: DuplicatePolicy;
    label: string;
    root: string;
  }) {
    this.#duplicatePolicy = options.duplicatePolicy;
    this.#label = options.label;
    this.#root = options.root;
  }

  async add(value: SemanticRecord): Promise<void> {
    if (this.#finished) throw new Error("Semantic sorter is already sealed");
    const record = semanticRecordSchema.parse(value);
    const bytes = Buffer.from(`${canonicalJson(record)}\n`, "utf8");
    if (bytes.byteLength > SEMANTIC_SORT_RUN_BYTES) {
      throw new Error("Publication semantic record exceeds its bounded run");
    }
    if (
      this.#pendingBytes > 0 &&
      this.#pendingBytes + bytes.byteLength > SEMANTIC_SORT_RUN_BYTES
    ) {
      await this.#flush();
    }
    this.#pending.push({ bytes, record });
    this.#pendingBytes += bytes.byteLength;
  }

  async finish(): Promise<void> {
    if (this.#finished) return;
    await this.#flush();
    this.#finished = true;
  }

  async *records(): AsyncGenerator<SemanticRecord> {
    await this.finish();
    const readers = this.#runs.map((runPath) =>
      createInterface({
        crlfDelay: Number.POSITIVE_INFINITY,
        input: createReadStream(runPath, { encoding: "utf8" }),
      }),
    );
    const iterators = readers.map((reader) => reader[Symbol.asyncIterator]());
    const current = await Promise.all(
      iterators.map((iterator) => iterator.next()),
    );
    let previousEncodedValue: string | null = null;
    let previousKey: string | null = null;
    try {
      while (true) {
        const selected = selectSemanticRunEntry(current);
        if (selected === null) return;
        current[selected.index] = await iterators[selected.index]!.next();
        if (previousKey === selected.record.key) {
          if (
            this.#duplicatePolicy === "reject" ||
            previousEncodedValue !== selected.encodedValue
          ) {
            throw new Error(
              "Publication semantic records contain a duplicate or conflict",
            );
          }
          continue;
        }
        previousEncodedValue = selected.encodedValue;
        previousKey = selected.record.key;
        yield selected.record;
      }
    } finally {
      for (const reader of readers) reader.close();
    }
  }

  async #flush(): Promise<void> {
    if (this.#pending.length === 0) return;
    this.#pending.sort((left, right) => {
      if (left.record.key !== right.record.key) {
        return left.record.key < right.record.key ? -1 : 1;
      }
      return Buffer.compare(left.bytes, right.bytes);
    });
    const normalized: Buffer[] = [];
    let previousBytes: Buffer | null = null;
    let previousKey: string | null = null;
    for (const item of this.#pending) {
      if (previousKey === item.record.key) {
        if (
          this.#duplicatePolicy === "reject" ||
          previousBytes === null ||
          !previousBytes.equals(item.bytes)
        ) {
          throw new Error(
            "Publication semantic records contain a duplicate or conflict",
          );
        }
        continue;
      }
      normalized.push(item.bytes);
      previousBytes = item.bytes;
      previousKey = item.record.key;
    }
    const runPath = path.join(
      this.#root,
      `${this.#label}-${String(this.#runs.length).padStart(6, "0")}.ndjson`,
    );
    await writeFile(runPath, Buffer.concat(normalized), { flag: "wx" });
    this.#runs.push(runPath);
    this.#pending = [];
    this.#pendingBytes = 0;
  }
}

export interface CandidateSourceSnapshotPrefixes {
  control: string;
  openData: string;
  queryTable: string;
}

export interface CandidateSourceSnapshotUploadRecord {
  byteSize: number;
  domain: "open_data" | "query_table";
  expectedCid: string;
  localLocator:
    | {
        domain: "open_data" | "query_table";
        kind: "source_payload";
        logicalObjectKey: string;
      }
    | {
        generatedObjectKey: string;
        kind: "generated_control" | "generated_payload";
      };
  logicalObjectKey: string;
  remoteObjectKey: string;
  sha256: string;
}

export interface MaterializedCandidateSourceSnapshotControls {
  adoptedExisting: boolean;
  candidatePayloads: {
    coverage: ControlArtifactBinding;
    provenance: ControlArtifactBinding;
    queryTable: ControlArtifactBinding;
  };
  compactManifest: ControlArtifactBinding;
  controlArtifacts: PublicationControlArtifactsBinding;
  controlObjects: readonly ControlArtifactBinding[];
  finalizedObjects: readonly ControlArtifactBinding[];
  createUploadRecords: () => AsyncIterable<CandidateSourceSnapshotUploadRecord>;
  sourceTargets: {
    openData: ControlArtifactBinding;
    queryTable: ControlArtifactBinding;
  };
  uploadWithoutPlan: {
    bytes: number;
    maximumObjectBytes: number;
    objectCount: number;
  };
}

interface CandidateSourceSnapshotReplacementBinding {
  byteSize: number;
  domain: "open_data" | "query_table";
  expectedCid: string;
  generatedObjectKey: string;
  logicalObjectKey: string;
  remoteObjectKey: string;
  sha256: string;
  sourceLogicalObjectKey: string;
}

export function candidateSourceSnapshotPrefixes(
  namespaceIdValue: string,
): CandidateSourceSnapshotPrefixes {
  const namespaceId = namespaceIdSchema.parse(namespaceIdValue);
  return {
    control: `publication-control/source-snapshot-demo-v1/${namespaceId}/`,
    openData: `publications/source-snapshot-demo-v1/${namespaceId}/`,
    queryTable: `query-tables/source-snapshot-demo-v1/${namespaceId}/`,
  };
}

function validatePrefixes(
  namespaceId: string,
  prefixes: CandidateSourceSnapshotPrefixes,
): CandidateSourceSnapshotPrefixes {
  const expected = candidateSourceSnapshotPrefixes(namespaceId);
  if (
    prefixes.control !== expected.control ||
    prefixes.openData !== expected.openData ||
    prefixes.queryTable !== expected.queryTable
  ) {
    throw new Error(
      "Candidate source-snapshot prefixes do not match the namespace identity",
    );
  }
  return expected;
}

/**
 * Streams one known array from a previously validated canonical JSON artifact.
 * It never retains more than one array element plus a short marker window.
 */
async function* streamJsonObjectArray(options: {
  filePath: string;
  marker: string;
}): AsyncGenerator<unknown> {
  const stream = createReadStream(options.filePath, { encoding: "utf8" });
  let markerWindow = "";
  let found = false;
  let done = false;
  let current = "";
  let depth = 0;
  let inString = false;
  let escaped = false;
  try {
    for await (const chunk of stream) {
      const value = String(chunk);
      let start = 0;
      if (!found) {
        markerWindow += value;
        const markerIndex = markerWindow.indexOf(options.marker);
        if (markerIndex === -1) {
          markerWindow = markerWindow.slice(
            Math.max(0, markerWindow.length - options.marker.length + 1),
          );
          continue;
        }
        found = true;
        start = markerIndex + options.marker.length;
        // markerWindow includes all of this chunk; process its suffix exactly
        // once, then release it before continuing to later chunks.
        const suffix = markerWindow.slice(start);
        markerWindow = "";
        for (const parsed of parseCharacters(suffix)) yield parsed;
        if (done) break;
        continue;
      }
      for (const parsed of parseCharacters(value.slice(start))) yield parsed;
      if (done) break;
    }
  } finally {
    stream.destroy();
  }
  if (!found || !done || current.length !== 0 || depth !== 0 || inString) {
    throw new Error("Bound publication control array is incomplete or invalid");
  }

  function* parseCharacters(text: string): Generator<unknown> {
    for (const character of text) {
      if (done) break;
      if (current.length === 0) {
        if (/\s/.test(character) || character === ",") continue;
        if (character === "]") {
          done = true;
          break;
        }
        if (character !== "{") {
          throw new Error("Bound publication control entries must be objects");
        }
        current = character;
        depth = 1;
        continue;
      }
      current += character;
      if (Buffer.byteLength(current, "utf8") > MAX_STREAMED_JSON_ENTRY_BYTES) {
        throw new Error("A bound publication control entry exceeds its limit");
      }
      if (inString) {
        if (escaped) escaped = false;
        else if (character === "\\") escaped = true;
        else if (character === '"') inString = false;
        continue;
      }
      if (character === '"') {
        inString = true;
      } else if (character === "{" || character === "[") {
        depth += 1;
      } else if (character === "}" || character === "]") {
        depth -= 1;
        if (depth < 0) {
          throw new Error("Bound publication control entry is malformed");
        }
        if (depth === 0) {
          const encoded = current;
          current = "";
          yield JSON.parse(encoded) as unknown;
        }
      }
    }
  }
}

function targetObject(
  artifact: PublicationArtifact,
  prefixes: CandidateSourceSnapshotPrefixes,
): CandidateSourceSnapshotUploadRecord | null {
  if (
    artifact.domain === "open_data" &&
    artifact.objectKey === "manifest.json"
  ) {
    return null;
  }
  const queryTableSourcePrefix = "query-tables/pasco/";
  if (
    artifact.domain === "query_table" &&
    !artifact.objectKey.startsWith(queryTableSourcePrefix)
  ) {
    throw new Error(
      "Source query-table object key is outside its bound prefix",
    );
  }
  const logicalObjectKey =
    artifact.domain === "open_data"
      ? artifact.objectKey
      : artifact.objectKey.slice(queryTableSourcePrefix.length);
  const remoteObjectKey =
    artifact.domain === "open_data"
      ? `${prefixes.openData}${logicalObjectKey}`
      : `${prefixes.queryTable}${logicalObjectKey}`;
  return {
    byteSize: artifact.byteSize,
    domain: artifact.domain,
    expectedCid: artifact.expectedCid,
    localLocator: {
      domain: artifact.domain,
      kind: "source_payload",
      logicalObjectKey: artifact.objectKey,
    },
    logicalObjectKey,
    remoteObjectKey,
    sha256: artifact.sha256,
  };
}

async function* sourcePayloadRecords(options: {
  onArtifact?: (artifact: PublicationArtifact) => void;
  prefixes: CandidateSourceSnapshotPrefixes;
  replacements?: ReadonlyMap<string, CandidateSourceSnapshotReplacementBinding>;
  sourcePlanPath: string;
}): AsyncGenerator<CandidateSourceSnapshotUploadRecord> {
  for await (const value of streamJsonObjectArray({
    filePath: options.sourcePlanPath,
    marker: '"objectInventory":[',
  })) {
    const artifact = publicationArtifactSchema.parse(value);
    options.onArtifact?.(artifact);
    const replacement = options.replacements?.get(
      `${artifact.domain}:${artifact.objectKey}`,
    );
    if (replacement) {
      yield {
        byteSize: replacement.byteSize,
        domain: replacement.domain,
        expectedCid: replacement.expectedCid,
        localLocator: {
          generatedObjectKey: replacement.generatedObjectKey,
          kind: "generated_payload",
        },
        logicalObjectKey: replacement.logicalObjectKey,
        remoteObjectKey: replacement.remoteObjectKey,
        sha256: replacement.sha256,
      };
      continue;
    }
    const mapped = targetObject(artifact, options.prefixes);
    if (mapped) yield mapped;
  }
}

const propertySemanticValueSchema = z.strictObject({
  byteSize: z.number().int().positive(),
  cid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  propertyId: z.string().regex(/^property_[a-f0-9]{32}$/),
  sha256: z.string().regex(SHA256_PATTERN),
});
const graphChildSemanticValueSchema = z.strictObject({
  childCid: z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  role: z.enum(["property", "shard"]),
});
const graphParentSemanticValueSchema = z.strictObject({
  role: z.enum(["root", "shard"]),
});

function propertyIdFromObjectKey(objectKey: string): string {
  const match = /^properties\/(property_[a-f0-9]{32})\.json$/.exec(objectKey);
  if (!match?.[1]) {
    throw new Error("Publication property object key is invalid");
  }
  return match[1];
}

function assertShardObjectKey(objectKey: string): void {
  if (!/^shards\/shard-\d{4,}\.json$/.test(objectKey)) {
    throw new Error("Publication shard object key is invalid");
  }
}

async function compareSemanticRecordSets(options: {
  label: string;
  left: ExternalSemanticSorter;
  right: ExternalSemanticSorter;
}): Promise<number> {
  const left = options.left.records()[Symbol.asyncIterator]();
  const right = options.right.records()[Symbol.asyncIterator]();
  let count = 0;
  try {
    while (true) {
      const [leftNext, rightNext] = await Promise.all([
        left.next(),
        right.next(),
      ]);
      if (leftNext.done || rightNext.done) {
        if (leftNext.done !== rightNext.done) {
          throw new Error(
            `${options.label} contains a missing or extra object`,
          );
        }
        return count;
      }
      if (
        leftNext.value.key !== rightNext.value.key ||
        canonicalJson(leftNext.value.value) !==
          canonicalJson(rightNext.value.value)
      ) {
        throw new Error(`${options.label} semantic binding is inconsistent`);
      }
      count += 1;
    }
  } finally {
    await Promise.all([left.return?.(undefined), right.return?.(undefined)]);
  }
}

/**
 * Reconciles the three legacy control surfaces by semantic object identity.
 * Sorting spills into bounded files so the countywide property inventory is
 * never retained in memory. Exact set comparison rejects missing, extra,
 * duplicated, reordered-to-a-different-object, or mismatched bindings even
 * when all collection cardinalities happen to agree.
 */
async function verifySourceControlSemantics(options: {
  sourceManifestPath: string;
  sourcePlanPath: string;
  temporaryRoot: string;
}): Promise<{
  graphChildCount: number;
  graphParentCount: number;
  coverage: PublicationArtifact;
  openDataRoot: PublicationArtifact;
  propertyCount: number;
  provenance: PublicationArtifact;
  queryTable: PublicationArtifact;
}> {
  await mkdir(options.temporaryRoot, { recursive: false });
  const manifestProperties = new ExternalSemanticSorter({
    duplicatePolicy: "reject",
    label: "manifest-properties",
    root: options.temporaryRoot,
  });
  const inventoryProperties = new ExternalSemanticSorter({
    duplicatePolicy: "reject",
    label: "inventory-properties",
    root: options.temporaryRoot,
  });
  const graphChildren = new ExternalSemanticSorter({
    duplicatePolicy: "reject",
    label: "graph-children",
    root: options.temporaryRoot,
  });
  const inventoryChildren = new ExternalSemanticSorter({
    duplicatePolicy: "reject",
    label: "inventory-children",
    root: options.temporaryRoot,
  });
  const graphParents = new ExternalSemanticSorter({
    duplicatePolicy: "deduplicate_identical",
    label: "graph-parents",
    root: options.temporaryRoot,
  });
  const inventoryParents = new ExternalSemanticSorter({
    duplicatePolicy: "reject",
    label: "inventory-parents",
    root: options.temporaryRoot,
  });
  let openDataRoot: PublicationArtifact | null = null;
  let coverage: PublicationArtifact | null = null;
  let provenance: PublicationArtifact | null = null;
  let queryTable: PublicationArtifact | null = null;

  try {
    for await (const value of streamJsonObjectArray({
      filePath: options.sourceManifestPath,
      marker: '"entries":[',
    })) {
      const entry = manifestEntrySchema.parse(value);
      if (propertyIdFromObjectKey(entry.objectKey) !== entry.propertyId) {
        throw new Error(
          "Publication manifest property identity is inconsistent",
        );
      }
      await manifestProperties.add({
        key: entry.objectKey,
        value: propertySemanticValueSchema.parse({
          byteSize: entry.bytes,
          cid: entry.cid,
          propertyId: entry.propertyId,
          sha256: entry.sha256,
        }),
      });
    }

    for await (const value of streamJsonObjectArray({
      filePath: options.sourcePlanPath,
      marker: '"objectInventory":[',
    })) {
      const artifact = publicationArtifactSchema.parse(value);
      if (artifact.domain === "query_table") {
        if (
          artifact.role !== "query_table" ||
          artifact.objectKey !== "query-tables/pasco/query-table.parquet" ||
          queryTable !== null
        ) {
          throw new Error("Publication query-table target is invalid");
        }
        queryTable = artifact;
        continue;
      }
      if (artifact.domain !== "open_data") continue;
      if (
        artifact.role === "metadata" &&
        (artifact.objectKey === "coverage.json" ||
          artifact.objectKey === "provenance.json")
      ) {
        if (artifact.objectKey === "coverage.json") {
          if (coverage !== null) {
            throw new Error("Publication coverage binding is duplicated");
          }
          coverage = artifact;
        } else {
          if (provenance !== null) {
            throw new Error("Publication provenance binding is duplicated");
          }
          provenance = artifact;
        }
      }
      if (artifact.role === "property") {
        const propertyId = propertyIdFromObjectKey(artifact.objectKey);
        await inventoryProperties.add({
          key: artifact.objectKey,
          value: propertySemanticValueSchema.parse({
            byteSize: artifact.byteSize,
            cid: artifact.expectedCid,
            propertyId,
            sha256: artifact.sha256,
          }),
        });
        await inventoryChildren.add({
          key: artifact.objectKey,
          value: graphChildSemanticValueSchema.parse({
            childCid: artifact.expectedCid,
            role: "property",
          }),
        });
      } else if (artifact.role === "shard") {
        assertShardObjectKey(artifact.objectKey);
        await inventoryChildren.add({
          key: artifact.objectKey,
          value: graphChildSemanticValueSchema.parse({
            childCid: artifact.expectedCid,
            role: "shard",
          }),
        });
        await inventoryParents.add({
          key: artifact.objectKey,
          value: graphParentSemanticValueSchema.parse({ role: "shard" }),
        });
      } else if (artifact.role === "root") {
        if (artifact.objectKey !== "index.json" || openDataRoot !== null) {
          throw new Error("Publication root object key is invalid");
        }
        openDataRoot = artifact;
        await inventoryParents.add({
          key: artifact.objectKey,
          value: graphParentSemanticValueSchema.parse({ role: "root" }),
        });
      }
    }

    for await (const value of streamJsonObjectArray({
      filePath: options.sourcePlanPath,
      marker: '"edges":[',
    })) {
      const edge = graphEdgeSchema.parse(value);
      let childRole: "property" | "shard";
      let parentRole: "root" | "shard";
      if (edge.childKey.startsWith("properties/")) {
        propertyIdFromObjectKey(edge.childKey);
        assertShardObjectKey(edge.parentKey);
        if (!/^\/entries\/(?:0|[1-9]\d*)\/cid$/.test(edge.jsonPointer)) {
          throw new Error("Publication property graph pointer is invalid");
        }
        childRole = "property";
        parentRole = "shard";
      } else if (edge.childKey.startsWith("shards/")) {
        assertShardObjectKey(edge.childKey);
        if (
          edge.parentKey !== "index.json" ||
          !/^\/shards\/(?:0|[1-9]\d*)\/shardCid$/.test(edge.jsonPointer)
        ) {
          throw new Error("Publication shard graph pointer is invalid");
        }
        childRole = "shard";
        parentRole = "root";
      } else {
        throw new Error("Publication graph contains an unsupported child");
      }
      await graphChildren.add({
        key: edge.childKey,
        value: graphChildSemanticValueSchema.parse({
          childCid: edge.childCid,
          role: childRole,
        }),
      });
      await graphParents.add({
        key: edge.parentKey,
        value: graphParentSemanticValueSchema.parse({ role: parentRole }),
      });
    }

    const [propertyCount, graphChildCount, graphParentCount] =
      await Promise.all([
        compareSemanticRecordSets({
          label: "Publication manifest/property inventory",
          left: manifestProperties,
          right: inventoryProperties,
        }),
        compareSemanticRecordSets({
          label: "Publication graph child/inventory",
          left: graphChildren,
          right: inventoryChildren,
        }),
        compareSemanticRecordSets({
          label: "Publication graph parent/inventory",
          left: graphParents,
          right: inventoryParents,
        }),
      ]);
    return {
      coverage: publicationArtifactSchema.parse(coverage),
      graphChildCount,
      graphParentCount,
      openDataRoot: publicationArtifactSchema.parse(openDataRoot),
      propertyCount,
      provenance: publicationArtifactSchema.parse(provenance),
      queryTable: publicationArtifactSchema.parse(queryTable),
    };
  } finally {
    await rm(options.temporaryRoot, { force: true, recursive: true });
  }
}

function controlRecord(
  artifact: ControlArtifactBinding,
  controlPrefix: string,
): CandidateSourceSnapshotUploadRecord {
  if (!artifact.objectKey.startsWith(controlPrefix)) {
    throw new Error("Generated control artifact is outside its bound prefix");
  }
  const logicalObjectKey = artifact.objectKey.slice(controlPrefix.length);
  if (!logicalObjectKey) {
    throw new Error("Generated control artifact has no logical object key");
  }
  return {
    byteSize: artifact.byteSize,
    domain: "open_data",
    expectedCid: artifact.expectedCid,
    localLocator: {
      generatedObjectKey: artifact.objectKey,
      kind: "generated_control",
    },
    logicalObjectKey,
    remoteObjectKey: artifact.objectKey,
    sha256: artifact.sha256,
  };
}

type MaterializeCandidateSourceSnapshotControlOptions = {
  compactManifest: Omit<
    CompactPublicationManifestIndex,
    "manifestEntries" | "version"
  >;
  expectedSourceManifestFileSha256: string;
  expectedSourcePlanFileSha256: string;
  expectedSourceQueryTable: {
    byteSize: number;
    expectedCid: string;
    sha256: string;
  };
  namespaceId: string;
  outputRoot: string;
  prefixes: CandidateSourceSnapshotPrefixes;
  sourceManifestPath: string;
  sourcePlanPath: string;
};

interface FileIdentity {
  byteSize: number;
  sha256: string;
}

const sourceCoverageDocumentSchema = z
  .object({
    canonicalProperties: z.number().int().positive(),
    county: z.literal("pasco"),
    coverageMode: z.literal("authoritative_complete"),
    runId: z.string().min(1),
    scopeId: z.string().min(1),
    snapshotId: z.string().min(1),
  })
  .passthrough();
const sourceProvenanceDocumentSchema = z
  .object({
    county: z.literal("pasco"),
    sourceWatermark: z
      .object({
        coverageMode: z.literal("authoritative_complete"),
        runId: z.string().min(1),
        scopeId: z.string().min(1),
        snapshotId: z.string().min(1),
      })
      .passthrough(),
  })
  .passthrough();

function resolvedArtifactPath(options: {
  artifact: PublicationArtifact;
  sourcePlanPath: string;
}): string {
  const sourceRoot = path.resolve(path.dirname(options.sourcePlanPath));
  const domainRoot = path.join(
    sourceRoot,
    options.artifact.domain === "open_data" ? "open-data" : "query",
  );
  const resolved = path.resolve(
    domainRoot,
    ...options.artifact.objectKey.split("/"),
  );
  if (!resolved.startsWith(`${domainRoot}${path.sep}`)) {
    throw new Error("Source publication artifact escaped its immutable root");
  }
  return resolved;
}

function resolvedGeneratedPath(outputRoot: string, objectKey: string): string {
  const root = path.resolve(outputRoot);
  const resolved = path.resolve(root, ...objectKey.split("/"));
  if (!resolved.startsWith(`${root}${path.sep}`)) {
    throw new Error("Candidate publication artifact escaped its private root");
  }
  return resolved;
}

async function assertArtifactFileBinding(options: {
  artifact: PublicationArtifact;
  filePath: string;
}): Promise<void> {
  const identity = await fileIdentity(options.filePath);
  if (
    identity.byteSize !== options.artifact.byteSize ||
    identity.sha256 !== options.artifact.sha256 ||
    (await calculateIpfsFileCid(options.filePath)) !==
      options.artifact.expectedCid
  ) {
    throw new Error("Source publication artifact byte binding is invalid");
  }
}

async function writeCandidateJsonReplacement(options: {
  domain: "open_data";
  logicalObjectKey: "coverage.json" | "provenance.json";
  outputRoot: string;
  prefixes: CandidateSourceSnapshotPrefixes;
  sourceArtifact: PublicationArtifact;
  sourcePlanPath: string;
  transform: (source: unknown) => unknown;
}): Promise<CandidateSourceSnapshotReplacementBinding> {
  const sourcePath = resolvedArtifactPath({
    artifact: options.sourceArtifact,
    sourcePlanPath: options.sourcePlanPath,
  });
  await assertArtifactFileBinding({
    artifact: options.sourceArtifact,
    filePath: sourcePath,
  });
  const source = JSON.parse(await readFile(sourcePath, "utf8")) as unknown;
  const bytes = Buffer.from(
    `${canonicalJson(options.transform(source))}\n`,
    "utf8",
  );
  const remoteObjectKey = `${options.prefixes.openData}${options.logicalObjectKey}`;
  const generatedObjectKey = remoteObjectKey;
  const outputPath = resolvedGeneratedPath(
    options.outputRoot,
    generatedObjectKey,
  );
  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, bytes, { flag: "wx" });
  return {
    byteSize: bytes.byteLength,
    domain: options.domain,
    expectedCid: await calculateIpfsCid(bytes),
    generatedObjectKey,
    logicalObjectKey: options.logicalObjectKey,
    remoteObjectKey,
    sha256: sha256(bytes),
    sourceLogicalObjectKey: options.sourceArtifact.objectKey,
  };
}

function sqlPath(value: string): string {
  return value.replaceAll("'", "''");
}

async function writeCandidateQueryTableReplacement(options: {
  expectedPropertyCount: number;
  expectedSource: MaterializeCandidateSourceSnapshotControlOptions["expectedSourceQueryTable"];
  outputRoot: string;
  prefixes: CandidateSourceSnapshotPrefixes;
  sourceArtifact: PublicationArtifact;
  sourcePlanPath: string;
}): Promise<CandidateSourceSnapshotReplacementBinding> {
  if (
    options.sourceArtifact.byteSize !== options.expectedSource.byteSize ||
    options.sourceArtifact.expectedCid !== options.expectedSource.expectedCid ||
    options.sourceArtifact.sha256 !== options.expectedSource.sha256
  ) {
    throw new Error(
      "Source query table is not the frozen publication artifact",
    );
  }
  const sourcePath = resolvedArtifactPath({
    artifact: options.sourceArtifact,
    sourcePlanPath: options.sourcePlanPath,
  });
  await assertArtifactFileBinding({
    artifact: options.sourceArtifact,
    filePath: sourcePath,
  });
  const logicalObjectKey = "query-table.parquet";
  const remoteObjectKey = `${options.prefixes.queryTable}${logicalObjectKey}`;
  const generatedObjectKey = remoteObjectKey;
  const outputPath = resolvedGeneratedPath(
    options.outputRoot,
    generatedObjectKey,
  );
  await mkdir(path.dirname(outputPath), { recursive: true });
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    await connection.run("SET threads = 1");
    await connection.run("SET preserve_insertion_order = true");
    await connection.run(`
      COPY (
        SELECT * REPLACE (
          CAST('source_snapshot' AS VARCHAR) AS coverage_mode
        )
        FROM read_parquet('${sqlPath(sourcePath)}')
        ORDER BY request_identifier, property_id
      ) TO '${sqlPath(outputPath)}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
    `);
    const validation = await connection.runAndReadAll(`
      SELECT
        count(*)::BIGINT AS row_count,
        count(DISTINCT property_id)::BIGINT AS property_count,
        count(DISTINCT coverage_mode)::BIGINT AS coverage_mode_count,
        min(coverage_mode) AS minimum_coverage_mode,
        max(coverage_mode) AS maximum_coverage_mode
      FROM read_parquet('${sqlPath(outputPath)}')
    `);
    const row = validation.getRowObjectsJson()[0] as
      Record<string, unknown> | undefined;
    if (
      Number(row?.row_count ?? -1) !== options.expectedPropertyCount ||
      Number(row?.property_count ?? -1) !== options.expectedPropertyCount ||
      Number(row?.coverage_mode_count ?? -1) !== 1 ||
      row?.minimum_coverage_mode !== "source_snapshot" ||
      row?.maximum_coverage_mode !== "source_snapshot"
    ) {
      throw new Error(
        "Candidate query table coverage projection is inconsistent",
      );
    }
  } finally {
    connection.closeSync();
  }
  const identity = await fileIdentity(outputPath);
  return {
    ...identity,
    domain: "query_table",
    expectedCid: await calculateIpfsFileCid(outputPath),
    generatedObjectKey,
    logicalObjectKey,
    remoteObjectKey,
    sourceLogicalObjectKey: options.sourceArtifact.objectKey,
  };
}

async function fileIdentity(filePath: string): Promise<FileIdentity> {
  const hash = createHash("sha256");
  let byteSize = 0;
  for await (const chunk of createReadStream(filePath)) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    byteSize += bytes.byteLength;
    hash.update(bytes);
  }
  return { byteSize, sha256: hash.digest("hex") };
}

function assertFileIdentity(
  actual: FileIdentity,
  expectedSha256: string,
  label: string,
): void {
  if (
    !SHA256_PATTERN.test(expectedSha256) ||
    actual.sha256 !== expectedSha256
  ) {
    throw new Error(
      `${label} file SHA-256 does not match its immutable binding`,
    );
  }
}

async function buildCandidateSourceSnapshotControlArtifacts(
  options: MaterializeCandidateSourceSnapshotControlOptions,
): Promise<MaterializedCandidateSourceSnapshotControls> {
  const prefixes = validatePrefixes(options.namespaceId, options.prefixes);
  const sourcePlanIdentityBefore = await fileIdentity(options.sourcePlanPath);
  const sourceManifestIdentityBefore = await fileIdentity(
    options.sourceManifestPath,
  );
  assertFileIdentity(
    sourcePlanIdentityBefore,
    options.expectedSourcePlanFileSha256,
    "Source publication plan",
  );
  assertFileIdentity(
    sourceManifestIdentityBefore,
    options.expectedSourceManifestFileSha256,
    "Source publication manifest",
  );
  const semanticCounts = await verifySourceControlSemantics({
    sourceManifestPath: options.sourceManifestPath,
    sourcePlanPath: options.sourcePlanPath,
    temporaryRoot: path.join(options.outputRoot, ".semantic-reconciliation"),
  });
  const expectedPropertyCount = options.compactManifest.coverage.propertyCount;
  const publicationClassification = {
    ...options.compactManifest.classification,
    coverageMode: "source_snapshot" as const,
    disclosure: options.compactManifest.disclosure,
  };
  const coverageReplacement = await writeCandidateJsonReplacement({
    domain: "open_data",
    logicalObjectKey: "coverage.json",
    outputRoot: options.outputRoot,
    prefixes,
    sourceArtifact: semanticCounts.coverage,
    sourcePlanPath: options.sourcePlanPath,
    transform: (value) => {
      const source = sourceCoverageDocumentSchema.parse(value);
      if (
        source.canonicalProperties !== expectedPropertyCount ||
        source.runId !== options.compactManifest.source.runId ||
        source.scopeId !== options.compactManifest.source.scopeId ||
        source.snapshotId !== options.compactManifest.source.snapshotId
      ) {
        throw new Error(
          "Source coverage document is not bound to the snapshot",
        );
      }
      return {
        ...source,
        coverageMode: "source_snapshot",
        publicationClassification,
        scope:
          "complete membership represented by the exact hash-bound source snapshot; candidate-owned and noncanonical",
        warning: options.compactManifest.disclosure,
      };
    },
  });
  const provenanceReplacement = await writeCandidateJsonReplacement({
    domain: "open_data",
    logicalObjectKey: "provenance.json",
    outputRoot: options.outputRoot,
    prefixes,
    sourceArtifact: semanticCounts.provenance,
    sourcePlanPath: options.sourcePlanPath,
    transform: (value) => {
      const source = sourceProvenanceDocumentSchema.parse(value);
      if (
        source.sourceWatermark.runId !== options.compactManifest.source.runId ||
        source.sourceWatermark.scopeId !==
          options.compactManifest.source.scopeId ||
        source.sourceWatermark.snapshotId !==
          options.compactManifest.source.snapshotId
      ) {
        throw new Error(
          "Source provenance document is not bound to the snapshot",
        );
      }
      return {
        ...source,
        publicationClassification,
        sourceWatermark: {
          ...source.sourceWatermark,
          coverageMode: "source_snapshot",
        },
      };
    },
  });
  const queryTableReplacement = await writeCandidateQueryTableReplacement({
    expectedPropertyCount,
    expectedSource: options.expectedSourceQueryTable,
    outputRoot: options.outputRoot,
    prefixes,
    sourceArtifact: semanticCounts.queryTable,
    sourcePlanPath: options.sourcePlanPath,
  });
  const replacements = new Map(
    [coverageReplacement, provenanceReplacement, queryTableReplacement].map(
      (replacement) => [
        `${replacement.domain}:${replacement.sourceLogicalObjectKey}`,
        replacement,
      ],
    ),
  );
  for (const replacement of replacements.values()) {
    const bytes = await readFile(
      resolvedGeneratedPath(options.outputRoot, replacement.generatedObjectKey),
    );
    if (bytes.includes(Buffer.from("authoritative_complete", "utf8"))) {
      throw new Error(
        "Candidate publication artifact retained legacy coverage semantics",
      );
    }
  }
  let manifestPosition = 0;
  const manifestEntries = await writeShardedControlCollection({
    collection: "manifest_entries",
    entries: (async function* (): AsyncGenerator<ControlEntry> {
      for await (const value of streamJsonObjectArray({
        filePath: options.sourceManifestPath,
        marker: '"entries":[',
      })) {
        const entry = manifestEntrySchema.parse(value);
        yield {
          key: `entry:${String(manifestPosition).padStart(9, "0")}:${entry.propertyId}`,
          value: {
            ...entry,
            objectKey: `${prefixes.openData}${entry.objectKey}`,
          },
        };
        manifestPosition += 1;
      }
    })(),
    objectKeyPrefix: prefixes.control,
    outputRoot: options.outputRoot,
  });
  const compactManifest = await writeCompactPublicationManifestIndex({
    manifest: {
      ...options.compactManifest,
      manifestEntries: toTypedControlCollectionReference(
        manifestEntries,
        "manifest_entries",
      ),
      queryTable: {
        ...options.compactManifest.queryTable,
        byteSize: queryTableReplacement.byteSize,
        expectedCid: queryTableReplacement.expectedCid,
        sha256: queryTableReplacement.sha256,
      },
      version: "2.0.0",
    },
    objectKey: `${prefixes.control}manifest.json`,
    outputRoot: options.outputRoot,
  });

  let edgePosition = 0;
  const graphEdges = await writeShardedControlCollection({
    collection: "graph_edges",
    entries: (async function* (): AsyncGenerator<ControlEntry> {
      for await (const value of streamJsonObjectArray({
        filePath: options.sourcePlanPath,
        marker: '"edges":[',
      })) {
        const edge = graphEdgeSchema.parse(value);
        yield {
          key: `edge:${String(edgePosition).padStart(9, "0")}`,
          value: {
            ...edge,
            childKey: `${prefixes.openData}${edge.childKey}`,
            parentKey: `${prefixes.openData}${edge.parentKey}`,
          },
        };
        edgePosition += 1;
      }
    })(),
    objectKeyPrefix: prefixes.control,
    outputRoot: options.outputRoot,
  });

  let payloadBytes = 0;
  let payloadObjectCount = 0;
  let maximumPayloadObjectBytes = 0;
  let previousLogicalKey: string | null = null;
  let propertyPayloadCount = 0;
  let shardPayloadCount = 0;
  let sourceManifestArtifact: PublicationArtifact | null = null;
  const objectInventory = await writeShardedControlCollection({
    collection: "object_inventory",
    entries: (async function* (): AsyncGenerator<ControlEntry> {
      for await (const record of sourcePayloadRecords({
        onArtifact: (artifact) => {
          if (
            artifact.domain === "open_data" &&
            artifact.objectKey === "manifest.json"
          ) {
            if (
              sourceManifestArtifact !== null ||
              artifact.role !== "manifest"
            ) {
              throw new Error(
                "Source publication manifest binding is duplicated or invalid",
              );
            }
            sourceManifestArtifact = artifact;
          } else if (
            artifact.domain === "open_data" &&
            artifact.role === "property"
          ) {
            propertyPayloadCount += 1;
          } else if (
            artifact.domain === "open_data" &&
            artifact.role === "shard"
          ) {
            shardPayloadCount += 1;
          }
        },
        prefixes,
        replacements,
        sourcePlanPath: options.sourcePlanPath,
      })) {
        const key = `${record.domain}:${record.logicalObjectKey}`;
        if (previousLogicalKey !== null && key <= previousLogicalKey) {
          throw new Error(
            "Source publication inventory is not strictly ordered",
          );
        }
        previousLogicalKey = key;
        payloadBytes += record.byteSize;
        payloadObjectCount += 1;
        maximumPayloadObjectBytes = Math.max(
          maximumPayloadObjectBytes,
          record.byteSize,
        );
        const { localLocator: _localLocator, ...publicRecord } = record;
        yield { key, value: publicRecord };
      }
    })(),
    objectKeyPrefix: prefixes.control,
    outputRoot: options.outputRoot,
  });

  if (sourceManifestArtifact === null) {
    throw new Error("Source publication manifest binding is missing");
  }
  const manifestArtifact = publicationArtifactSchema.parse(
    sourceManifestArtifact,
  );
  if (
    manifestArtifact.sha256 !== options.expectedSourceManifestFileSha256 ||
    manifestArtifact.sha256 !== sourceManifestIdentityBefore.sha256 ||
    manifestArtifact.byteSize !== sourceManifestIdentityBefore.byteSize ||
    (await calculateIpfsFileCid(options.sourceManifestPath)) !==
      manifestArtifact.expectedCid
  ) {
    throw new Error(
      "Source publication manifest does not match its plan artifact binding",
    );
  }
  if (
    propertyPayloadCount !== expectedPropertyCount ||
    manifestPosition !== expectedPropertyCount ||
    manifestEntries.index.entryCount !== expectedPropertyCount ||
    edgePosition !== propertyPayloadCount + shardPayloadCount
  ) {
    throw new Error(
      "Source publication manifest, graph, and inventory cardinalities are inconsistent",
    );
  }
  if (
    semanticCounts.propertyCount !== propertyPayloadCount ||
    semanticCounts.graphChildCount !== edgePosition ||
    semanticCounts.graphParentCount !== shardPayloadCount + 1
  ) {
    throw new Error(
      "Source publication manifest, graph, and inventory semantic counts are inconsistent",
    );
  }
  const openDataTargetRecord = targetObject(
    semanticCounts.openDataRoot,
    prefixes,
  );
  if (
    openDataTargetRecord === null ||
    semanticCounts.openDataRoot.expectedCid !==
      options.compactManifest.graph.openDataRootCid ||
    semanticCounts.queryTable.expectedCid !==
      options.expectedSourceQueryTable.expectedCid ||
    semanticCounts.queryTable.byteSize !==
      options.expectedSourceQueryTable.byteSize ||
    semanticCounts.queryTable.sha256 !== options.expectedSourceQueryTable.sha256
  ) {
    throw new Error(
      "Source graph or candidate query target does not match its immutable binding",
    );
  }
  const sourceTargets = {
    openData: {
      byteSize: openDataTargetRecord.byteSize,
      expectedCid: openDataTargetRecord.expectedCid,
      objectKey: openDataTargetRecord.remoteObjectKey,
      sha256: openDataTargetRecord.sha256,
    },
    queryTable: {
      byteSize: queryTableReplacement.byteSize,
      expectedCid: queryTableReplacement.expectedCid,
      objectKey: queryTableReplacement.remoteObjectKey,
      sha256: queryTableReplacement.sha256,
    },
  } satisfies MaterializedCandidateSourceSnapshotControls["sourceTargets"];

  const [sourcePlanIdentityAfter, sourceManifestIdentityAfter] =
    await Promise.all([
      fileIdentity(options.sourcePlanPath),
      fileIdentity(options.sourceManifestPath),
    ]);
  if (
    sourcePlanIdentityAfter.byteSize !== sourcePlanIdentityBefore.byteSize ||
    sourcePlanIdentityAfter.sha256 !== sourcePlanIdentityBefore.sha256 ||
    sourceManifestIdentityAfter.byteSize !==
      sourceManifestIdentityBefore.byteSize ||
    sourceManifestIdentityAfter.sha256 !== sourceManifestIdentityBefore.sha256
  ) {
    throw new Error(
      "Source publication control files changed during materialization",
    );
  }

  await Promise.all(
    [manifestEntries, graphEdges, objectInventory].map((collection) =>
      verifyLocalShardedControlCollection({
        index: collection.index,
        outputRoot: options.outputRoot,
      }),
    ),
  );
  const controlArtifacts = createPublicationControlArtifactsBinding({
    graphEdges,
    manifestEntries,
    manifestIndex: compactManifest,
    objectInventory,
    payloadBytes,
    payloadObjectCount,
  });
  const generatedControl = [
    ...shardedControlUploadArtifacts(manifestEntries),
    ...shardedControlUploadArtifacts(graphEdges),
    ...shardedControlUploadArtifacts(objectInventory),
    compactManifest,
  ].map((artifact) => controlRecord(artifact, prefixes.control));
  const candidatePayloads = {
    coverage: {
      byteSize: coverageReplacement.byteSize,
      expectedCid: coverageReplacement.expectedCid,
      objectKey: coverageReplacement.remoteObjectKey,
      sha256: coverageReplacement.sha256,
    },
    provenance: {
      byteSize: provenanceReplacement.byteSize,
      expectedCid: provenanceReplacement.expectedCid,
      objectKey: provenanceReplacement.remoteObjectKey,
      sha256: provenanceReplacement.sha256,
    },
    queryTable: {
      byteSize: queryTableReplacement.byteSize,
      expectedCid: queryTableReplacement.expectedCid,
      objectKey: queryTableReplacement.remoteObjectKey,
      sha256: queryTableReplacement.sha256,
    },
  } satisfies MaterializedCandidateSourceSnapshotControls["candidatePayloads"];
  const controlObjects = generatedControl.map((record) => ({
    byteSize: record.byteSize,
    expectedCid: record.expectedCid,
    objectKey: record.remoteObjectKey,
    sha256: record.sha256,
  }));
  const finalizedObjects = [
    ...controlObjects,
    ...Object.values(candidatePayloads),
  ];
  const uploadWithoutPlan = {
    bytes:
      payloadBytes +
      generatedControl.reduce((total, record) => total + record.byteSize, 0),
    maximumObjectBytes: Math.max(
      maximumPayloadObjectBytes,
      ...generatedControl.map((record) => record.byteSize),
    ),
    objectCount: payloadObjectCount + generatedControl.length,
  };

  return {
    adoptedExisting: false,
    candidatePayloads,
    compactManifest,
    controlArtifacts,
    controlObjects,
    createUploadRecords: () =>
      (async function* (): AsyncGenerator<CandidateSourceSnapshotUploadRecord> {
        const [planBeforeReplay, manifestBeforeReplay] = await Promise.all([
          fileIdentity(options.sourcePlanPath),
          fileIdentity(options.sourceManifestPath),
        ]);
        if (
          planBeforeReplay.byteSize !== sourcePlanIdentityBefore.byteSize ||
          planBeforeReplay.sha256 !== sourcePlanIdentityBefore.sha256 ||
          manifestBeforeReplay.byteSize !==
            sourceManifestIdentityBefore.byteSize ||
          manifestBeforeReplay.sha256 !== sourceManifestIdentityBefore.sha256
        ) {
          throw new Error(
            "Source publication control files changed before upload replay",
          );
        }
        yield* sourcePayloadRecords({
          prefixes,
          replacements,
          sourcePlanPath: options.sourcePlanPath,
        });
        for (const record of generatedControl) yield record;
        const [planAfterReplay, manifestAfterReplay] = await Promise.all([
          fileIdentity(options.sourcePlanPath),
          fileIdentity(options.sourceManifestPath),
        ]);
        if (
          planAfterReplay.byteSize !== sourcePlanIdentityBefore.byteSize ||
          planAfterReplay.sha256 !== sourcePlanIdentityBefore.sha256 ||
          manifestAfterReplay.byteSize !==
            sourceManifestIdentityBefore.byteSize ||
          manifestAfterReplay.sha256 !== sourceManifestIdentityBefore.sha256
        ) {
          throw new Error(
            "Source publication control files changed during upload replay",
          );
        }
      })(),
    finalizedObjects,
    sourceTargets,
    uploadWithoutPlan,
  };
}

async function verifyFinalizedControlObjects(
  outputRoot: string,
  artifacts: readonly ControlArtifactBinding[],
): Promise<void> {
  const resolvedRoot = path.resolve(outputRoot);
  const actualKeys: string[] = [];
  const visit = async (relativeDirectory: string): Promise<void> => {
    const entries = await readdir(
      path.join(resolvedRoot, ...relativeDirectory.split("/").filter(Boolean)),
      { withFileTypes: true },
    );
    for (const entry of entries) {
      if (entry.isSymbolicLink()) {
        throw new Error("Existing candidate control output contains a symlink");
      }
      const relativePath = relativeDirectory
        ? `${relativeDirectory}/${entry.name}`
        : entry.name;
      if (entry.isDirectory()) await visit(relativePath);
      else if (entry.isFile()) actualKeys.push(relativePath);
      else throw new Error("Existing candidate control output is invalid");
    }
  };
  await visit("");
  const expectedKeys = artifacts.map((artifact) => artifact.objectKey).sort();
  actualKeys.sort();
  if (JSON.stringify(actualKeys) !== JSON.stringify(expectedKeys)) {
    throw new Error("Existing candidate control inventory differs");
  }
  for (const artifact of artifacts) {
    const filePath = path.resolve(
      resolvedRoot,
      ...artifact.objectKey.split("/"),
    );
    if (!filePath.startsWith(`${resolvedRoot}${path.sep}`)) {
      throw new Error("Candidate control artifact escaped its final root");
    }
    const bytes = await readFile(filePath);
    if (
      bytes.byteLength !== artifact.byteSize ||
      sha256(bytes) !== artifact.sha256 ||
      (await calculateIpfsCid(bytes)) !== artifact.expectedCid
    ) {
      throw new Error("Existing candidate control artifact differs");
    }
  }
}

async function pathExists(value: string): Promise<boolean> {
  try {
    await lstat(value);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

export async function materializeCandidateSourceSnapshotControlArtifacts(
  options: MaterializeCandidateSourceSnapshotControlOptions,
): Promise<MaterializedCandidateSourceSnapshotControls> {
  const finalRoot = path.resolve(options.outputRoot);
  const contender = path.join(
    path.dirname(finalRoot),
    `.${path.basename(finalRoot)}.contender-${process.pid}-${randomUUID()}`,
  );
  await mkdir(path.dirname(finalRoot), { recursive: true });
  await mkdir(contender, { recursive: false });
  try {
    const built = await buildCandidateSourceSnapshotControlArtifacts({
      ...options,
      outputRoot: contender,
    });
    if (await pathExists(finalRoot)) {
      await verifyFinalizedControlObjects(finalRoot, built.finalizedObjects);
      return { ...built, adoptedExisting: true };
    }
    try {
      await rename(contender, finalRoot);
      return built;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
      await verifyFinalizedControlObjects(finalRoot, built.finalizedObjects);
      return { ...built, adoptedExisting: true };
    }
  } finally {
    await rm(contender, { force: true, recursive: true });
  }
}
