import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { canonicalJson } from "../../src/lib/canonical-json.js";
import {
  createPublicationControlArtifactsBinding,
  readBoundControlCollectionIndex,
  readControlEntryByKey,
  toTypedControlCollectionReference,
  validateControlCollectionIndex,
  validatePublicationControlArtifactsBinding,
  verifyLocalShardedControlCollection,
  verifyShardedControlCollection,
  writeCompactPublicationManifestIndex,
  writeShardedControlCollection,
} from "../../src/publication/control-artifacts.js";

const controlPrefix =
  "publication-control/source-snapshot-demo-v1/snapshotns_00000000000000000000000000000000/";

function entries(prefix: string, count: number) {
  return Array.from({ length: count }, (_, index) => ({
    key: `${prefix}:${String(index).padStart(4, "0")}`,
    value: {
      byteSize: index + 1,
      expectedCid: `synthetic-cid-${index}`,
      objectKey: `properties/property_${String(index).padStart(4, "0")}.json`,
      sha256: String(index).padStart(64, "0"),
    },
  }));
}

const syntheticCid = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
const syntheticSha256 = "0".repeat(64);

async function compactManifest(options: {
  manifestEntries: Parameters<typeof toTypedControlCollectionReference>[0];
  ownershipMalformedRows?: number;
  outputRoot: string;
  propertyCount: number;
}) {
  return writeCompactPublicationManifestIndex({
    manifest: {
      classification: {
        canonical: false,
        elephantOwned: false,
        independentlyPascoCertified: false,
        ownerControlled: false,
        publicationClass: "candidate_owned_source_snapshot_demo",
        resourceOwner: "candidate",
        sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot",
      },
      contracts: {
        canonical: { sha256: syntheticSha256, version: "1.0.0" },
        mcp: { sha256: syntheticSha256, version: "1.2.0" },
      },
      county: "pasco",
      coverage: {
        buildings: {
          facts: options.propertyCount,
          properties: options.propertyCount,
          yearBuiltProxyProperties: options.propertyCount,
        },
        contractors: { availability: "unavailable", facts: 0 },
        coordinates: {
          availableProperties: options.propertyCount,
          missingProperties: 0,
        },
        membership: "complete_membership_of_exact_source_snapshot_noncanonical",
        ownership: {
          acceptedRows: options.propertyCount,
          malformedRows: options.ownershipMalformedRows ?? 0,
          properties: options.propertyCount,
          sourceRows: options.propertyCount,
        },
        permits: {
          availability: "unavailable",
          facts: 0,
          permitContractorRelationships: 0,
        },
        propertyCount: options.propertyCount,
        siteAddresses: {
          sourceRows: options.propertyCount,
          usableProperties: options.propertyCount,
        },
        unresolvedPublishedParcelStatistic: 335_946,
      },
      disclosure: "Synthetic candidate-owned noncanonical test publication.",
      freshness: {
        asOf: "2026-08-23T00:00:00.000Z",
        loadedAt: "2026-08-30T00:00:00.000Z",
        observedAt: "2026-08-23T00:00:00.000Z",
      },
      graph: {
        openDataRootCid: syntheticCid,
        propertyCount: options.propertyCount,
      },
      manifestEntries: toTypedControlCollectionReference(
        options.manifestEntries,
        "manifest_entries",
      ),
      queryTable: {
        byteSize: 4,
        expectedCid: syntheticCid,
        propertyCount: options.propertyCount,
        schemaSha256: syntheticSha256,
        sha256: syntheticSha256,
      },
      source: {
        authorityClass: "owner_assumed_authoritative_snapshot",
        authorityId: `authority_${"0".repeat(32)}`,
        materializationId: `materialization_${"0".repeat(32)}`,
        materializationSha256: syntheticSha256,
        runId: `run_${"0".repeat(32)}`,
        scopeId: `scope_${"0".repeat(32)}`,
        selectionSha256: syntheticSha256,
        snapshotId: `snapshot_${"0".repeat(32)}`,
      },
      version: "2.0.0",
    },
    objectKey: `${controlPrefix}manifest.json`,
    outputRoot: options.outputRoot,
  });
}

describe("sharded publication control artifacts", () => {
  it("builds deterministic bounded collections and one non-self-referential root", async () => {
    const firstRoot = await mkdtemp(path.join(tmpdir(), "control-first-"));
    const secondRoot = await mkdtemp(path.join(tmpdir(), "control-second-"));
    try {
      const values = entries("open_data", 24);
      const firstInventory = await writeShardedControlCollection({
        collection: "object_inventory",
        entries: values,
        maximumShardBytes: 640,
        objectKeyPrefix: controlPrefix,
        outputRoot: firstRoot,
      });
      const secondInventory = await writeShardedControlCollection({
        collection: "object_inventory",
        entries: values,
        maximumShardBytes: 640,
        objectKeyPrefix: controlPrefix,
        outputRoot: secondRoot,
      });
      expect(firstInventory).toEqual(secondInventory);
      expect(firstInventory.index.shardCount).toBeGreaterThan(1);
      expect(
        firstInventory.index.shards.every((shard) => shard.byteSize <= 640),
      ).toBe(true);
      await expect(
        verifyLocalShardedControlCollection({
          index: firstInventory.index,
          outputRoot: firstRoot,
        }),
      ).resolves.toEqual(firstInventory.index);
      const indexBytes = await readFile(
        path.join(
          firstRoot,
          ...firstInventory.indexArtifact.objectKey.split("/"),
        ),
      );

      const graphEdges = await writeShardedControlCollection({
        collection: "graph_edges",
        entries: entries("edge", 3),
        maximumShardBytes: 640,
        objectKeyPrefix: controlPrefix,
        outputRoot: firstRoot,
      });
      const manifestEntries = await writeShardedControlCollection({
        collection: "manifest_entries",
        entries: entries("property", 4),
        maximumShardBytes: 640,
        objectKeyPrefix: controlPrefix,
        outputRoot: firstRoot,
      });
      const manifestIndex = await compactManifest({
        manifestEntries,
        outputRoot: firstRoot,
        propertyCount: 4,
      });
      const binding = createPublicationControlArtifactsBinding({
        graphEdges,
        manifestEntries,
        manifestIndex,
        objectInventory: firstInventory,
        payloadBytes: values.reduce(
          (total, entry) => total + entry.value.byteSize,
          0,
        ),
        payloadObjectCount: values.length,
      });
      expect(binding.controlObjectCount).toBe(
        graphEdges.index.shardCount +
          manifestEntries.index.shardCount +
          firstInventory.index.shardCount +
          4,
      );
      expect(canonicalJson(binding)).not.toContain("planArtifactCid");
      expect(validatePublicationControlArtifactsBinding(binding)).toEqual(
        binding,
      );
      await expect(
        readBoundControlCollectionIndex({
          bytes: indexBytes,
          reference: binding.objectInventory,
        }),
      ).resolves.toEqual(firstInventory.index);
      await expect(
        readBoundControlCollectionIndex({
          bytes: indexBytes,
          reference: {
            ...binding.objectInventory,
            indexArtifact: {
              ...binding.objectInventory.indexArtifact,
              objectKey: `${controlPrefix}graph_edges/index.json`,
            },
          },
        }),
      ).rejects.toThrow("index object key");

      const reads: number[] = [];
      await expect(
        readControlEntryByKey({
          index: firstInventory.index,
          key: values[17]!.key,
          readShard: async (descriptor) => {
            reads.push(descriptor.shardIndex);
            return readFile(
              path.join(firstRoot, ...descriptor.objectKey.split("/")),
            );
          },
        }),
      ).resolves.toEqual(values[17]!.value);
      expect(reads).toHaveLength(1);
    } finally {
      await rm(firstRoot, { force: true, recursive: true });
      await rm(secondRoot, { force: true, recursive: true });
    }
  });

  it("rejects unsorted input before writing a conflicting logical sequence", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "control-order-"));
    try {
      await expect(
        writeShardedControlCollection({
          collection: "manifest_entries",
          entries: [
            { key: "property:0002", value: { value: 2 } },
            { key: "property:0001", value: { value: 1 } },
          ],
          maximumShardBytes: 512,
          objectKeyPrefix: controlPrefix,
          outputRoot: root,
        }),
      ).rejects.toThrow("unique and strictly ordered");
    } finally {
      await rm(root, { force: true, recursive: true });
    }
  });

  it.each([
    "missing",
    "reordered",
    "duplicated",
    "overlap",
    "hash_mismatch",
  ] as const)("rejects %s shard evidence", async (failure) => {
    const root = await mkdtemp(path.join(tmpdir(), `control-${failure}-`));
    try {
      const collection = await writeShardedControlCollection({
        collection: "object_inventory",
        entries: entries("open_data", 18),
        maximumShardBytes: 640,
        objectKeyPrefix: controlPrefix,
        outputRoot: root,
      });
      const bytes = await Promise.all(
        collection.index.shards.map((descriptor) =>
          readFile(path.join(root, ...descriptor.objectKey.split("/"))),
        ),
      );
      if (failure === "hash_mismatch") {
        const first = bytes[0];
        if (!first || first.length === 0) throw new Error("missing test shard");
        first[0] = first[0]! ^ 1;
      }
      if (failure === "reordered") bytes.reverse();
      if (failure === "duplicated") bytes[1] = bytes[0]!;
      const index = structuredClone(collection.index);
      if (failure === "overlap" && index.shards[1]) {
        index.shards[1].firstKey = index.shards[0]!.lastKey;
      }
      if (failure === "missing") bytes.pop();
      const readShard = async (descriptor: (typeof index.shards)[number]) => {
        const value = bytes[descriptor.shardIndex];
        if (!value) throw new Error("missing synthetic shard");
        return value;
      };
      await expect(
        verifyShardedControlCollection({ index, readShard }),
      ).rejects.toThrow();
    } finally {
      await rm(root, { force: true, recursive: true });
    }
  });

  it("rejects descriptor tampering and collection-root tampering", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "control-tamper-"));
    try {
      const collection = await writeShardedControlCollection({
        collection: "graph_edges",
        entries: entries("edge", 2),
        objectKeyPrefix: controlPrefix,
        outputRoot: root,
      });
      const descriptorTamper = structuredClone(collection.index);
      descriptorTamper.shards[0]!.count += 1;
      expect(() => validateControlCollectionIndex(descriptorTamper)).toThrow();
      const oversizedDescriptor = structuredClone(collection.index);
      oversizedDescriptor.shards[0]!.byteSize = 8 * 1024 * 1024 + 1;
      expect(() =>
        validateControlCollectionIndex(oversizedDescriptor),
      ).toThrow();

      const inventory = await writeShardedControlCollection({
        collection: "object_inventory",
        entries: entries("object", 2),
        objectKeyPrefix: controlPrefix,
        outputRoot: root,
      });
      const manifest = await writeShardedControlCollection({
        collection: "manifest_entries",
        entries: entries("property", 2),
        objectKeyPrefix: controlPrefix,
        outputRoot: root,
      });
      const manifestIndex = await compactManifest({
        manifestEntries: manifest,
        outputRoot: root,
        propertyCount: 2,
      });
      const binding = createPublicationControlArtifactsBinding({
        graphEdges: collection,
        manifestEntries: manifest,
        manifestIndex,
        objectInventory: inventory,
        payloadBytes: 3,
        payloadObjectCount: 2,
      });
      expect(() =>
        validatePublicationControlArtifactsBinding({
          ...binding,
          payloadBytes: binding.payloadBytes + 1,
        }),
      ).toThrow("full-inventory commitment");
    } finally {
      await rm(root, { force: true, recursive: true });
    }
  });

  it("rejects inconsistent compact coverage counts", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "control-coverage-"));
    try {
      const manifestEntries = await writeShardedControlCollection({
        collection: "manifest_entries",
        entries: entries("property", 2),
        objectKeyPrefix: controlPrefix,
        outputRoot: root,
      });
      await expect(
        compactManifest({
          manifestEntries,
          ownershipMalformedRows: 1,
          outputRoot: root,
          propertyCount: 2,
        }),
      ).rejects.toThrow("compact manifest publication counts");
    } finally {
      await rm(root, { force: true, recursive: true });
    }
  });

  it("rejects a control entry larger than the independently configured bound", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "control-large-entry-"));
    try {
      await expect(
        writeShardedControlCollection({
          collection: "manifest_entries",
          entries: [
            { key: "property:0001", value: { value: "x".repeat(800) } },
          ],
          maximumShardBytes: 256,
          objectKeyPrefix: controlPrefix,
          outputRoot: root,
        }),
      ).rejects.toThrow("entry exceeds the shard bound");
    } finally {
      await rm(root, { force: true, recursive: true });
    }
  });
});
