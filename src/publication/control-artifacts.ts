import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import { calculateIpfsCid, CIDV0_PATTERN } from "./ipfs-cid.js";

export const PUBLICATION_CONTROL_VERSION = "1.0.0" as const;
export const PUBLICATION_MANIFEST_INDEX_VERSION = "2.0.0" as const;
export const PUBLICATION_CONTROL_REPRESENTATION =
  "oracle-sharded-control-ndjson-v1" as const;
export const PUBLICATION_CONTROL_MAX_INDEX_BYTES = 16 * 1024 * 1024;
export const PUBLICATION_CONTROL_MAX_SHARD_BYTES = 8 * 1024 * 1024;

const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const SAFE_OBJECT_KEY_PATTERN = /^[^\\]+$/;
const objectKeyPrefixSchema = z
  .string()
  .min(2)
  .max(1_024)
  .regex(/^[a-z0-9][A-Za-z0-9._/-]*\/$/)
  .refine(
    (value) =>
      !value.startsWith("/") &&
      !value.includes("//") &&
      !value.includes("\\") &&
      !value.includes("{") &&
      !value.includes("}") &&
      !value.split("/").includes(".."),
    "must be a safe immutable publication prefix",
  );

const controlEntrySchema = z.strictObject({
  key: z.string().min(1).max(4_096),
  value: z.unknown(),
});

const controlShardDescriptorSchema = z.strictObject({
  byteSize: z
    .number()
    .int()
    .positive()
    .max(PUBLICATION_CONTROL_MAX_SHARD_BYTES),
  count: z.number().int().positive(),
  expectedCid: z.string().regex(CIDV0_PATTERN),
  firstKey: z.string().min(1).max(4_096),
  lastKey: z.string().min(1).max(4_096),
  objectKey: z
    .string()
    .min(1)
    .max(2_048)
    .regex(SAFE_OBJECT_KEY_PATTERN)
    .refine(
      (value) => !value.startsWith("/") && !value.split("/").includes(".."),
      "must be a safe publication-relative object key",
    ),
  sha256: z.string().regex(SHA256_PATTERN),
  shardIndex: z.number().int().nonnegative(),
});

export const controlCollectionIndexSchema = z
  .strictObject({
    collection: z.enum(["graph_edges", "manifest_entries", "object_inventory"]),
    descriptorSha256: z.string().regex(SHA256_PATTERN),
    entriesSha256: z.string().regex(SHA256_PATTERN),
    entryBytes: z.number().int().nonnegative(),
    entryCount: z.number().int().nonnegative(),
    integrityRootSha256: z.string().regex(SHA256_PATTERN),
    maximumShardBytes: z
      .number()
      .int()
      .positive()
      .max(PUBLICATION_CONTROL_MAX_SHARD_BYTES),
    representation: z.literal(PUBLICATION_CONTROL_REPRESENTATION),
    shardCount: z.number().int().nonnegative(),
    shards: z.array(controlShardDescriptorSchema),
    version: z.literal(PUBLICATION_CONTROL_VERSION),
  })
  .superRefine((index, context) => {
    if (index.shardCount !== index.shards.length) {
      context.addIssue({
        code: "custom",
        message: "control shard count is inconsistent",
        path: ["shardCount"],
      });
    }
    if ((index.entryCount === 0) !== (index.shardCount === 0)) {
      context.addIssue({
        code: "custom",
        message: "empty control collections must not contain shards",
        path: ["shards"],
      });
    }
    let previousLastKey: string | null = null;
    let count = 0;
    let bytes = 0;
    for (const [position, shard] of index.shards.entries()) {
      if (
        shard.shardIndex !== position ||
        shard.byteSize > index.maximumShardBytes ||
        compareCodeUnits(shard.firstKey, shard.lastKey) > 0 ||
        (previousLastKey !== null &&
          compareCodeUnits(previousLastKey, shard.firstKey) >= 0)
      ) {
        context.addIssue({
          code: "custom",
          message:
            "control shards must be contiguous, strictly ordered, and non-overlapping",
          path: ["shards", position],
        });
        break;
      }
      previousLastKey = shard.lastKey;
      count += shard.count;
      bytes += shard.byteSize;
    }
    if (count !== index.entryCount || bytes !== index.entryBytes) {
      context.addIssue({
        code: "custom",
        message: "control shard totals are inconsistent",
        path: ["shards"],
      });
    }
  });

export type ControlCollection = z.infer<
  typeof controlCollectionIndexSchema
>["collection"];
export type ControlCollectionIndex = z.infer<
  typeof controlCollectionIndexSchema
>;
export type ControlShardDescriptor = ControlCollectionIndex["shards"][number];

export interface ControlEntry<T = unknown> {
  key: string;
  value: T;
}

export interface ControlArtifactBinding {
  byteSize: number;
  expectedCid: string;
  objectKey: string;
  sha256: string;
}

export interface ShardedControlCollection {
  index: ControlCollectionIndex;
  indexArtifact: ControlArtifactBinding;
}

const controlArtifactBindingSchema = z.strictObject({
  byteSize: z.number().int().positive(),
  expectedCid: z.string().regex(CIDV0_PATTERN),
  objectKey: z.string().min(1).max(2_048),
  sha256: z.string().regex(SHA256_PATTERN),
});

const controlCollectionReferenceSchema = z.strictObject({
  collection: controlCollectionIndexSchema.shape.collection,
  entriesSha256: z.string().regex(SHA256_PATTERN),
  entryBytes: z.number().int().nonnegative(),
  entryCount: z.number().int().nonnegative(),
  indexArtifact: controlArtifactBindingSchema,
  integrityRootSha256: z.string().regex(SHA256_PATTERN),
  shardBytes: z.number().int().nonnegative(),
  shardCount: z.number().int().nonnegative(),
});

export const publicationControlArtifactsBindingSchema = z.strictObject({
  controlBytes: z.number().int().nonnegative(),
  controlObjectCount: z.number().int().nonnegative(),
  fullInventoryRootSha256: z.string().regex(SHA256_PATTERN),
  graphEdges: controlCollectionReferenceSchema.extend({
    collection: z.literal("graph_edges"),
  }),
  manifestEntries: controlCollectionReferenceSchema.extend({
    collection: z.literal("manifest_entries"),
  }),
  manifestIndex: controlArtifactBindingSchema,
  objectInventory: controlCollectionReferenceSchema.extend({
    collection: z.literal("object_inventory"),
  }),
  payloadBytes: z.number().int().nonnegative(),
  payloadObjectCount: z.number().int().nonnegative(),
  version: z.literal(PUBLICATION_CONTROL_VERSION),
});

export type PublicationControlArtifactsBinding = z.infer<
  typeof publicationControlArtifactsBindingSchema
>;
export type ControlCollectionReference = z.infer<
  typeof controlCollectionReferenceSchema
>;

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function descriptorIdentity(index: {
  collection: ControlCollection;
  entryBytes: number;
  entryCount: number;
  entriesSha256: string;
  maximumShardBytes: number;
  representation: typeof PUBLICATION_CONTROL_REPRESENTATION;
  shards: readonly ControlShardDescriptor[];
  version: typeof PUBLICATION_CONTROL_VERSION;
}) {
  const descriptorSha256 = canonicalJsonSha256(index.shards);
  const integrityRootSha256 = canonicalJsonSha256({
    collection: index.collection,
    descriptorSha256,
    entriesSha256: index.entriesSha256,
    entryBytes: index.entryBytes,
    entryCount: index.entryCount,
    representation: index.representation,
    version: index.version,
  });
  return { descriptorSha256, integrityRootSha256 };
}

export function validateControlCollectionIndex(
  value: unknown,
): ControlCollectionIndex {
  const index = controlCollectionIndexSchema.parse(value);
  const expected = descriptorIdentity(index);
  if (
    expected.descriptorSha256 !== index.descriptorSha256 ||
    expected.integrityRootSha256 !== index.integrityRootSha256
  ) {
    throw new Error("Publication control collection integrity is invalid");
  }
  return index;
}

export async function readBoundControlCollectionIndex(options: {
  bytes: Uint8Array;
  reference: ControlCollectionReference;
}): Promise<ControlCollectionIndex> {
  const reference = controlCollectionReferenceSchema.parse(options.reference);
  const indexSuffix = `${reference.collection}/index.json`;
  if (!reference.indexArtifact.objectKey.endsWith(indexSuffix)) {
    throw new Error("Publication control index object key is invalid");
  }
  const objectKeyPrefix = objectKeyPrefixSchema.parse(
    reference.indexArtifact.objectKey.slice(0, -indexSuffix.length),
  );
  if (
    options.bytes.byteLength !== reference.indexArtifact.byteSize ||
    options.bytes.byteLength > PUBLICATION_CONTROL_MAX_INDEX_BYTES ||
    sha256(Buffer.from(options.bytes)) !== reference.indexArtifact.sha256 ||
    (await calculateIpfsCid(options.bytes)) !==
      reference.indexArtifact.expectedCid
  ) {
    throw new Error("Publication control index byte binding is invalid");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(options.bytes).toString("utf8"));
  } catch {
    throw new Error("Publication control index JSON is invalid");
  }
  const index = validateControlCollectionIndex(parsed);
  if (
    index.shards.some(
      (descriptor) =>
        descriptor.objectKey !==
        `${objectKeyPrefix}${reference.collection}/part-${String(
          descriptor.shardIndex,
        ).padStart(6, "0")}.ndjson`,
    )
  ) {
    throw new Error("Publication control shard object key is invalid");
  }
  if (
    index.collection !== reference.collection ||
    index.entryCount !== reference.entryCount ||
    index.entryBytes !== reference.entryBytes ||
    index.entriesSha256 !== reference.entriesSha256 ||
    index.integrityRootSha256 !== reference.integrityRootSha256 ||
    index.shardCount !== reference.shardCount ||
    index.shards.reduce(
      (total, descriptor) => total + descriptor.byteSize,
      0,
    ) !== reference.shardBytes
  ) {
    throw new Error(
      "Publication control index does not match its plan binding",
    );
  }
  return index;
}

export function findControlShard(
  indexValue: unknown,
  key: string,
): ControlShardDescriptor | null {
  const index = validateControlCollectionIndex(indexValue);
  let low = 0;
  let high = index.shards.length - 1;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const shard = index.shards[middle]!;
    if (compareCodeUnits(key, shard.firstKey) < 0) {
      high = middle - 1;
    } else if (compareCodeUnits(key, shard.lastKey) > 0) {
      low = middle + 1;
    } else {
      return shard;
    }
  }
  return null;
}

export function toControlCollectionReference(
  collection: ShardedControlCollection,
): ControlCollectionReference {
  const index = validateControlCollectionIndex(collection.index);
  return controlCollectionReferenceSchema.parse({
    collection: index.collection,
    entriesSha256: index.entriesSha256,
    entryBytes: index.entryBytes,
    entryCount: index.entryCount,
    indexArtifact: collection.indexArtifact,
    integrityRootSha256: index.integrityRootSha256,
    shardBytes: index.shards.reduce(
      (total, descriptor) => total + descriptor.byteSize,
      0,
    ),
    shardCount: index.shardCount,
  });
}

export function toTypedControlCollectionReference<
  Collection extends ControlCollection,
>(
  collection: ShardedControlCollection,
  expectedCollection: Collection,
): ControlCollectionReference & { collection: Collection } {
  const reference = toControlCollectionReference(collection);
  if (reference.collection !== expectedCollection) {
    throw new Error("Publication control collection kind is inconsistent");
  }
  return { ...reference, collection: expectedCollection };
}

/**
 * Creates the compact plan-binding value. This binding intentionally excludes
 * the serialized plan artifact itself so plan identity cannot become
 * self-referential; approval stores that artifact CID/SHA alongside the plan.
 */
export function createPublicationControlArtifactsBinding(options: {
  graphEdges: ShardedControlCollection;
  manifestEntries: ShardedControlCollection;
  manifestIndex: ControlArtifactBinding;
  objectInventory: ShardedControlCollection;
  payloadBytes: number;
  payloadObjectCount: number;
}): PublicationControlArtifactsBinding {
  const graphEdges = toControlCollectionReference(options.graphEdges);
  const manifestEntries = toControlCollectionReference(options.manifestEntries);
  const objectInventory = toControlCollectionReference(options.objectInventory);
  const manifestIndex = controlArtifactBindingSchema.parse(
    options.manifestIndex,
  );
  if (objectInventory.entryCount !== options.payloadObjectCount) {
    throw new Error("Publication payload inventory count is inconsistent");
  }
  const controlObjectCount =
    graphEdges.shardCount +
    manifestEntries.shardCount +
    objectInventory.shardCount +
    4;
  const controlBytes = [graphEdges, manifestEntries, objectInventory].reduce(
    (total, collection) =>
      total + collection.shardBytes + collection.indexArtifact.byteSize,
    manifestIndex.byteSize,
  );
  const rootInput = {
    controlBytes,
    controlObjectCount,
    graphEdges,
    manifestEntries,
    manifestIndex,
    objectInventory,
    payloadBytes: options.payloadBytes,
    payloadObjectCount: options.payloadObjectCount,
    version: PUBLICATION_CONTROL_VERSION,
  };
  return publicationControlArtifactsBindingSchema.parse({
    ...rootInput,
    fullInventoryRootSha256: canonicalJsonSha256(rootInput),
  });
}

const compactPublicationManifestIndexSchema = z
  .strictObject({
    classification: z.strictObject({
      canonical: z.literal(false),
      elephantOwned: z.literal(false),
      independentlyPascoCertified: z.literal(false),
      ownerControlled: z.literal(false),
      publicationClass: z.literal("candidate_owned_source_snapshot_demo"),
      resourceOwner: z.literal("candidate"),
      sourceScope: z.literal("exact_hash_bound_2026_08_23_parcel_snapshot"),
    }),
    contracts: z.strictObject({
      canonical: z.strictObject({
        sha256: z.string().regex(SHA256_PATTERN),
        version: z.literal("1.0.0"),
      }),
      mcp: z.strictObject({
        sha256: z.string().regex(SHA256_PATTERN),
        version: z.literal("1.2.0"),
      }),
    }),
    county: z.literal("pasco"),
    coverage: z.strictObject({
      buildings: z.strictObject({
        facts: z.number().int().nonnegative(),
        properties: z.number().int().nonnegative(),
        yearBuiltProxyProperties: z.number().int().nonnegative(),
      }),
      contractors: z.strictObject({
        availability: z.literal("unavailable"),
        facts: z.literal(0),
      }),
      coordinates: z.strictObject({
        availableProperties: z.number().int().nonnegative(),
        missingProperties: z.number().int().nonnegative(),
      }),
      membership: z.literal(
        "complete_membership_of_exact_source_snapshot_noncanonical",
      ),
      ownership: z.strictObject({
        acceptedRows: z.number().int().nonnegative(),
        malformedRows: z.number().int().nonnegative(),
        properties: z.number().int().nonnegative(),
        sourceRows: z.number().int().nonnegative(),
      }),
      permits: z.strictObject({
        availability: z.literal("unavailable"),
        facts: z.literal(0),
        permitContractorRelationships: z.literal(0),
      }),
      propertyCount: z.number().int().positive(),
      siteAddresses: z.strictObject({
        sourceRows: z.number().int().nonnegative(),
        usableProperties: z.number().int().nonnegative(),
      }),
      unresolvedPublishedParcelStatistic: z
        .number()
        .int()
        .positive()
        .nullable(),
    }),
    disclosure: z.string().min(1).max(4_096),
    freshness: z.strictObject({
      asOf: z.string().datetime(),
      loadedAt: z.string().datetime(),
      observedAt: z.string().datetime(),
    }),
    graph: z.strictObject({
      openDataRootCid: z.string().regex(CIDV0_PATTERN),
      propertyCount: z.number().int().positive(),
    }),
    manifestEntries: controlCollectionReferenceSchema.extend({
      collection: z.literal("manifest_entries"),
    }),
    queryTable: z.strictObject({
      byteSize: z.number().int().positive(),
      expectedCid: z.string().regex(CIDV0_PATTERN),
      propertyCount: z.number().int().positive(),
      schemaSha256: z.string().regex(SHA256_PATTERN),
      sha256: z.string().regex(SHA256_PATTERN),
    }),
    source: z.strictObject({
      authorityClass: z.literal("owner_assumed_authoritative_snapshot"),
      authorityId: z.string().regex(/^authority_[a-f0-9]{32}$/),
      materializationId: z.string().regex(/^materialization_[a-f0-9]{32}$/),
      materializationSha256: z.string().regex(SHA256_PATTERN),
      runId: z.string().regex(/^run_[a-f0-9]{32}$/),
      scopeId: z.string().regex(/^scope_[a-f0-9]{32}$/),
      selectionSha256: z.string().regex(SHA256_PATTERN),
      snapshotId: z.string().regex(/^snapshot_[a-f0-9]{32}$/),
    }),
    version: z.literal(PUBLICATION_MANIFEST_INDEX_VERSION),
  })
  .superRefine((manifest, context) => {
    if (
      manifest.coverage.coordinates.availableProperties +
        manifest.coverage.coordinates.missingProperties !==
        manifest.coverage.propertyCount ||
      manifest.coverage.buildings.properties >
        manifest.coverage.buildings.facts ||
      manifest.coverage.buildings.yearBuiltProxyProperties !==
        manifest.coverage.buildings.properties ||
      manifest.coverage.ownership.acceptedRows +
        manifest.coverage.ownership.malformedRows !==
        manifest.coverage.ownership.sourceRows ||
      manifest.coverage.ownership.properties >
        manifest.coverage.ownership.acceptedRows ||
      manifest.coverage.siteAddresses.usableProperties >
        manifest.coverage.propertyCount ||
      manifest.graph.propertyCount !== manifest.coverage.propertyCount ||
      manifest.queryTable.propertyCount !== manifest.coverage.propertyCount ||
      manifest.manifestEntries.entryCount !== manifest.coverage.propertyCount
    ) {
      context.addIssue({
        code: "custom",
        message: "compact manifest publication counts are inconsistent",
        path: ["coverage", "propertyCount"],
      });
    }
  });

export type CompactPublicationManifestIndex = z.infer<
  typeof compactPublicationManifestIndexSchema
>;

export function validateCompactPublicationManifestIndex(
  value: unknown,
): CompactPublicationManifestIndex {
  return compactPublicationManifestIndexSchema.parse(value);
}

export async function readBoundCompactPublicationManifestIndex(options: {
  binding: ControlArtifactBinding;
  bytes: Uint8Array;
}): Promise<CompactPublicationManifestIndex> {
  const binding = controlArtifactBindingSchema.parse(options.binding);
  if (
    options.bytes.byteLength !== binding.byteSize ||
    options.bytes.byteLength > PUBLICATION_CONTROL_MAX_INDEX_BYTES ||
    sha256(Buffer.from(options.bytes)) !== binding.sha256 ||
    (await calculateIpfsCid(options.bytes)) !== binding.expectedCid
  ) {
    throw new Error("Compact publication manifest byte binding is invalid");
  }
  try {
    return validateCompactPublicationManifestIndex(
      JSON.parse(Buffer.from(options.bytes).toString("utf8")),
    );
  } catch (error) {
    throw new Error("Compact publication manifest JSON is invalid", {
      cause: error,
    });
  }
}

export async function writeCompactPublicationManifestIndex(options: {
  manifest: CompactPublicationManifestIndex;
  objectKey: string;
  outputRoot: string;
}): Promise<ControlArtifactBinding> {
  const manifest = compactPublicationManifestIndexSchema.parse(
    options.manifest,
  );
  const objectKey = z
    .string()
    .min(1)
    .max(2_048)
    .regex(SAFE_OBJECT_KEY_PATTERN)
    .refine(
      (value) =>
        !value.startsWith("/") &&
        !value.split("/").includes("..") &&
        value.endsWith("/manifest.json"),
      "must be an immutable manifest index object key",
    )
    .parse(options.objectKey);
  const bytes = Buffer.from(`${canonicalJson(manifest)}\n`, "utf8");
  if (bytes.byteLength > PUBLICATION_CONTROL_MAX_INDEX_BYTES) {
    throw new Error("Compact publication manifest exceeds the hosted bound");
  }
  const filePath = path.join(options.outputRoot, ...objectKey.split("/"));
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, bytes, { flag: "wx" });
  return {
    byteSize: bytes.byteLength,
    expectedCid: await calculateIpfsCid(bytes),
    objectKey,
    sha256: sha256(bytes),
  };
}

export function validatePublicationControlArtifactsBinding(
  value: unknown,
): PublicationControlArtifactsBinding {
  const binding = publicationControlArtifactsBindingSchema.parse(value);
  const { fullInventoryRootSha256: _fullInventoryRootSha256, ...rootInput } =
    binding;
  if (canonicalJsonSha256(rootInput) !== binding.fullInventoryRootSha256) {
    throw new Error("Publication full-inventory commitment is invalid");
  }
  return binding;
}

function encodeEntry(entry: ControlEntry): Buffer {
  const parsed = controlEntrySchema.parse(entry);
  return Buffer.from(`${canonicalJson(parsed)}\n`, "utf8");
}

async function writeShard(options: {
  bytes: Buffer;
  collection: ControlCollection;
  firstKey: string;
  lastKey: string;
  outputRoot: string;
  objectKeyPrefix: string;
  shardIndex: number;
}): Promise<ControlShardDescriptor> {
  const objectKey = `${options.objectKeyPrefix}${options.collection}/part-${String(
    options.shardIndex,
  ).padStart(6, "0")}.ndjson`;
  const filePath = path.join(options.outputRoot, ...objectKey.split("/"));
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, options.bytes, { flag: "wx" });
  return {
    byteSize: options.bytes.byteLength,
    count: options.bytes.reduce(
      (count, byte) => count + (byte === 0x0a ? 1 : 0),
      0,
    ),
    expectedCid: await calculateIpfsCid(options.bytes),
    firstKey: options.firstKey,
    lastKey: options.lastKey,
    objectKey,
    sha256: sha256(options.bytes),
    shardIndex: options.shardIndex,
  };
}

/**
 * Writes a deterministic, bounded NDJSON collection. Callers must provide
 * entries in strict key order; sorting externally keeps full inventories out
 * of hosted and builder memory.
 */
export async function writeShardedControlCollection<T>(options: {
  collection: ControlCollection;
  entries: AsyncIterable<ControlEntry<T>> | Iterable<ControlEntry<T>>;
  maximumShardBytes?: number;
  objectKeyPrefix: string;
  outputRoot: string;
}): Promise<ShardedControlCollection> {
  const maximumShardBytes = z
    .number()
    .int()
    .positive()
    .max(PUBLICATION_CONTROL_MAX_SHARD_BYTES)
    .parse(options.maximumShardBytes ?? PUBLICATION_CONTROL_MAX_SHARD_BYTES);
  const objectKeyPrefix = objectKeyPrefixSchema.parse(options.objectKeyPrefix);
  const allEntriesHash = createHash("sha256");
  const shards: ControlShardDescriptor[] = [];
  let pending: Buffer[] = [];
  let pendingBytes = 0;
  let pendingFirstKey: string | null = null;
  let pendingLastKey: string | null = null;
  let previousKey: string | null = null;
  let entryBytes = 0;
  let entryCount = 0;

  const flush = async (): Promise<void> => {
    if (pending.length === 0) return;
    const bytes = Buffer.concat(pending, pendingBytes);
    shards.push(
      await writeShard({
        bytes,
        collection: options.collection,
        firstKey: pendingFirstKey!,
        lastKey: pendingLastKey!,
        objectKeyPrefix,
        outputRoot: options.outputRoot,
        shardIndex: shards.length,
      }),
    );
    pending = [];
    pendingBytes = 0;
    pendingFirstKey = null;
    pendingLastKey = null;
  };

  for await (const entry of options.entries) {
    if (previousKey !== null && compareCodeUnits(previousKey, entry.key) >= 0) {
      throw new Error(
        "Publication control entries must be unique and strictly ordered",
      );
    }
    const bytes = encodeEntry(entry);
    if (bytes.byteLength > maximumShardBytes) {
      throw new Error("A publication control entry exceeds the shard bound");
    }
    if (
      pendingBytes > 0 &&
      pendingBytes + bytes.byteLength > maximumShardBytes
    ) {
      await flush();
    }
    pendingFirstKey ??= entry.key;
    pendingLastKey = entry.key;
    pending.push(bytes);
    pendingBytes += bytes.byteLength;
    entryBytes += bytes.byteLength;
    entryCount += 1;
    previousKey = entry.key;
    allEntriesHash.update(bytes);
  }
  await flush();

  const entriesSha256 = allEntriesHash.digest("hex");
  const base = {
    collection: options.collection,
    entriesSha256,
    entryBytes,
    entryCount,
    maximumShardBytes,
    representation: PUBLICATION_CONTROL_REPRESENTATION,
    shards,
    version: PUBLICATION_CONTROL_VERSION,
  } as const;
  const identity = descriptorIdentity(base);
  const index = validateControlCollectionIndex({
    ...base,
    ...identity,
    shardCount: shards.length,
  });
  const indexBytes = Buffer.from(`${canonicalJson(index)}\n`, "utf8");
  if (indexBytes.byteLength > PUBLICATION_CONTROL_MAX_INDEX_BYTES) {
    throw new Error("Publication control index exceeds the hosted bound");
  }
  const indexObjectKey = `${objectKeyPrefix}${options.collection}/index.json`;
  const indexPath = path.join(options.outputRoot, ...indexObjectKey.split("/"));
  await mkdir(path.dirname(indexPath), { recursive: true });
  await writeFile(indexPath, indexBytes, { flag: "wx" });
  return {
    index,
    indexArtifact: {
      byteSize: indexBytes.byteLength,
      expectedCid: await calculateIpfsCid(indexBytes),
      objectKey: indexObjectKey,
      sha256: sha256(indexBytes),
    },
  };
}

function parseShard(bytes: Uint8Array): ControlEntry[] {
  const text = Buffer.from(bytes).toString("utf8");
  if (text.length === 0 || !text.endsWith("\n") || text.includes("\r")) {
    throw new Error("Publication control shard framing is invalid");
  }
  return text
    .slice(0, -1)
    .split("\n")
    .map((line) => controlEntrySchema.parse(JSON.parse(line)));
}

async function verifyOneShard(options: {
  bytes: Uint8Array;
  descriptor: ControlShardDescriptor;
}): Promise<ControlEntry[]> {
  if (
    options.bytes.byteLength !== options.descriptor.byteSize ||
    sha256(Buffer.from(options.bytes)) !== options.descriptor.sha256 ||
    (await calculateIpfsCid(options.bytes)) !== options.descriptor.expectedCid
  ) {
    throw new Error("Publication control shard byte binding is invalid");
  }
  const entries = parseShard(options.bytes);
  if (
    entries.length !== options.descriptor.count ||
    entries[0]?.key !== options.descriptor.firstKey ||
    entries.at(-1)?.key !== options.descriptor.lastKey
  ) {
    throw new Error("Publication control shard range/count is invalid");
  }
  let previousKey: string | null = null;
  for (const entry of entries) {
    if (previousKey !== null && compareCodeUnits(previousKey, entry.key) >= 0) {
      throw new Error(
        "Publication control entries are duplicated, reordered, or overlapping",
      );
    }
    previousKey = entry.key;
  }
  return entries;
}

export async function readControlEntryByKey(options: {
  index: unknown;
  key: string;
  readShard: (descriptor: ControlShardDescriptor) => Promise<Uint8Array>;
}): Promise<unknown | null> {
  const descriptor = findControlShard(options.index, options.key);
  if (!descriptor) return null;
  const entries = await verifyOneShard({
    bytes: await options.readShard(descriptor),
    descriptor,
  });
  return entries.find((entry) => entry.key === options.key)?.value ?? null;
}

export async function verifyShardedControlCollection(options: {
  index: unknown;
  readShard: (descriptor: ControlShardDescriptor) => Promise<Uint8Array>;
  validateValue?: (value: unknown, key: string) => void;
}): Promise<ControlCollectionIndex> {
  const index = validateControlCollectionIndex(options.index);
  const entriesHash = createHash("sha256");
  let previousKey: string | null = null;
  let entryCount = 0;
  let entryBytes = 0;
  for (const descriptor of index.shards) {
    const bytes = await options.readShard(descriptor);
    const entries = await verifyOneShard({ bytes, descriptor });
    for (const entry of entries) {
      if (
        previousKey !== null &&
        compareCodeUnits(previousKey, entry.key) >= 0
      ) {
        throw new Error(
          "Publication control entries are duplicated, reordered, or overlapping",
        );
      }
      options.validateValue?.(entry.value, entry.key);
      previousKey = entry.key;
      entryCount += 1;
    }
    entriesHash.update(bytes);
    entryBytes += bytes.byteLength;
  }
  if (
    entryCount !== index.entryCount ||
    entryBytes !== index.entryBytes ||
    entriesHash.digest("hex") !== index.entriesSha256
  ) {
    throw new Error("Publication control collection commitment is invalid");
  }
  return index;
}

export async function verifyLocalShardedControlCollection(options: {
  index: unknown;
  outputRoot: string;
  validateValue?: (value: unknown, key: string) => void;
}): Promise<ControlCollectionIndex> {
  return verifyShardedControlCollection({
    index: options.index,
    readShard: (descriptor) =>
      readFile(
        path.join(options.outputRoot, ...descriptor.objectKey.split("/")),
      ),
    ...(options.validateValue ? { validateValue: options.validateValue } : {}),
  });
}

export function shardedControlUploadArtifacts(
  collection: ShardedControlCollection,
): readonly ControlArtifactBinding[] {
  return [
    ...collection.index.shards.map(
      ({
        count: _count,
        firstKey: _first,
        lastKey: _last,
        shardIndex: _index,
        ...artifact
      }) => artifact,
    ),
    collection.indexArtifact,
  ];
}
