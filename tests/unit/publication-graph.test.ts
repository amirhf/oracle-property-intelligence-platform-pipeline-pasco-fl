import { describe, expect, it } from "vitest";
import { importer } from "ipfs-unixfs-importer";
import { z } from "zod";

import {
  buildPublicationGraph,
  publicationCanonicalJson,
  type GraphPropertyInput,
  validatePublicationGraph,
} from "../../src/publication/graph.js";
import {
  calculateIpfsCid,
  calculateIpfsFileCid,
  IPFS_CID_PROFILE,
  IPFS_IMPORTER_OPTIONS,
} from "../../src/publication/ipfs-cid.js";

const timestamp = "2026-08-29T00:00:00.000Z";

async function independentImporterCid(bytes: Uint8Array): Promise<string> {
  const block = {
    get: async () => {
      throw new Error("Golden-only hashing must not read blocks");
    },
    put: async () => {
      throw new Error("Golden-only hashing must not persist blocks");
    },
  };
  let result: string | null = null;
  const content = async function* () {
    yield bytes;
  };
  for await (const imported of importer([{ content: content() }], block, {
    avgChunkSize: 262_144,
    cidVersion: 0,
    chunker: "fixed",
    hashAlg: "sha2-256",
    leafType: "file",
    maxChunkSize: 262_144,
    maxChildrenPerNode: 174,
    minChunkSize: 262_144,
    onlyHash: true,
    rawLeaves: false,
    reduceSingleLeafToSelf: true,
    strategy: "balanced",
    wrapWithDirectory: false,
  })) {
    result = String(imported.cid);
  }
  if (result === null) throw new Error("Independent importer returned no CID");
  return result;
}

// Byte-compatible with elephant-mcp's reader schemas; producer requirements
// are stricter because every CID must be present before plan approval.
const mcpShardSchema = z.object({
  count: z.number().int().positive(),
  entries: z.array(
    z.object({
      cid: z.string().nullable(),
      fileSizeBytes: z.number(),
      parcelIdentifier: z.string(),
      propertyId: z.string(),
    }),
  ),
  fromParcel: z.string(),
  schemaVersion: z.literal("1"),
  shardIndex: z.number().int().nonnegative(),
  toParcel: z.string(),
});
const mcpIndexSchema = z.object({
  completedAt: z.string(),
  county: z.string(),
  exportedAt: z.string(),
  propertyCount: z.number().int().nonnegative(),
  schemaVersion: z.literal("1"),
  shardSize: z.number().int().positive(),
  shards: z.array(
    z.object({
      count: z.number().int().nonnegative(),
      fromParcel: z.string(),
      shardCid: z.string().nullable(),
      shardIndex: z.number().int().nonnegative(),
      toParcel: z.string(),
    }),
  ),
  totalBytes: z.number().nonnegative(),
});

function properties(count = 3): GraphPropertyInput[] {
  return Array.from({ length: count }, (_, index) => ({
    parcelIdentifier: `SYNTH-${String(index + 1).padStart(5, "0")}`,
    propertyId: `property_${String(index + 1).padStart(32, "0")}`,
    value: {
      freshness: { publishedAt: null },
      parcelIdentifier: `SYNTH-${String(index + 1).padStart(5, "0")}`,
      propertyId: `property_${String(index + 1).padStart(32, "0")}`,
    },
  }));
}

describe("Elephant-compatible local IPFS graph", () => {
  it("pins the complete UnixFS profile and golden single/multi-chunk CIDs", async () => {
    expect(IPFS_CID_PROFILE).toEqual({
      cidVersion: 0,
      chunker: "fixed",
      chunkSize: 262_144,
      codec: "dag-pb",
      hashAlg: "sha2-256",
      importer: "ipfs-unixfs-importer@7.0.3",
      layout: "balanced",
      maxChildrenPerNode: 174,
      onlyHash: true,
      rawLeaves: false,
      reduceSingleLeafToSelf: true,
      trickle: false,
      unixfsType: "file",
      version: "ipfs-only-hash@4.0.0",
      wrapWithDirectory: false,
    });
    expect(IPFS_IMPORTER_OPTIONS).toEqual({
      avgChunkSize: 262_144,
      cidVersion: 0,
      chunker: "fixed",
      hashAlg: "sha2-256",
      leafType: "file",
      maxChunkSize: 262_144,
      maxChildrenPerNode: 174,
      minChunkSize: 262_144,
      onlyHash: true,
      rawLeaves: false,
      reduceSingleLeafToSelf: true,
      strategy: "balanced",
      wrapWithDirectory: false,
    });
    await expect(calculateIpfsCid(Buffer.alloc(0))).resolves.toBe(
      "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
    );
    await expect(calculateIpfsCid("hello\n")).resolves.toBe(
      "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN",
    );
    await expect(calculateIpfsCid(Buffer.alloc(300_000, 0x61))).resolves.toBe(
      "QmYCTciJdFNMNUPCHSNS6dKMmUAqkGQ9tQQeGgbELhQQcn",
    );
  });

  it("matches independent golden CIDs at chunk and fan-out boundaries", async () => {
    const vectors = [
      [262_143, "QmdVN4PkHDK1i6UVAqE9r9tM9AtZnh6YcQ6144VESH2z3u"],
      [262_144, "Qma81h2ZqbvJW2EQkiVUZ17aSvNWqAtvUPhh8mQBPU8W7c"],
      [262_145, "QmTaxvXcxpzzaatSEEAYr7t3knkJ6DmTVbr8MjJJWLRWpV"],
      [174 * 262_144, "QmSFFbR63aHfeAutBngoh4rNB94bQNHQ8pTrwpH9wjp33C"],
      [175 * 262_144, "QmdmFwCfNaSQ7PUJYXudBACNCcPY1K3GXDfv4tNWMuwJ1J"],
      [349 * 262_144 + 1, "QmahVJ8ahysM4UuiJauo3D1N7B6aPRxk7ssjfv6izebT3W"],
    ] as const;
    for (const [size, expected] of vectors) {
      const bytes = Buffer.alloc(size, 0x61);
      await expect(independentImporterCid(bytes)).resolves.toBe(expected);
      await expect(calculateIpfsCid(bytes)).resolves.toBe(expected);
    }
  }, 30_000);

  it("reproduces buffer CIDs through the bounded streamed-file importer", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "oracle-cid-"));
    try {
      for (const size of [262_143, 262_144, 262_145, 300_000]) {
        const bytes = Buffer.alloc(size, 0x61);
        const filePath = path.join(directory, `${size}.bin`);
        await writeFile(filePath, bytes);
        await expect(calculateIpfsFileCid(filePath)).resolves.toBe(
          await calculateIpfsCid(bytes),
        );
      }
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });

  it("reproduces every child, shard, root and traversal edge", async () => {
    const first = await buildPublicationGraph({
      completedAt: timestamp,
      exportedAt: timestamp,
      properties: properties(),
    });
    const second = await buildPublicationGraph({
      completedAt: timestamp,
      exportedAt: timestamp,
      properties: [...properties()].reverse(),
    });
    expect(second.rootCid).toBe(first.rootCid);
    expect(
      second.objects.map(({ bytes, ...object }) => ({
        ...object,
        bytes: bytes.toString("hex"),
      })),
    ).toEqual(
      first.objects.map(({ bytes, ...object }) => ({
        ...object,
        bytes: bytes.toString("hex"),
      })),
    );
    expect(first.edges).toHaveLength(4);
    expect(
      first.objects.filter((object) => object.role === "property"),
    ).toHaveLength(3);
    expect(mcpIndexSchema.parse(first.root)).toEqual(first.root);
    expect(first.shards.map((shard) => mcpShardSchema.parse(shard))).toEqual(
      first.shards,
    );
    await expect(validatePublicationGraph(first)).resolves.toBeUndefined();
    expect(first.root.shards.every((shard) => shard.shardCid !== null)).toBe(
      true,
    );
    expect(
      first.shards
        .flatMap((shard) => shard.entries)
        .every((entry) => entry.cid !== null),
    ).toBe(true);
  });

  it("propagates a one-byte logical leaf mutation through its shard and root", async () => {
    const before = await buildPublicationGraph({
      completedAt: timestamp,
      exportedAt: timestamp,
      properties: properties(),
    });
    const mutated = properties();
    mutated[1]!.value = {
      ...(mutated[1]!.value as Record<string, unknown>),
      marker: "x",
    };
    const after = await buildPublicationGraph({
      completedAt: timestamp,
      exportedAt: timestamp,
      properties: mutated,
    });
    expect(after.propertyCids.get(mutated[1]!.propertyId)).not.toBe(
      before.propertyCids.get(mutated[1]!.propertyId),
    );
    expect(
      after.objects.find((object) => object.role === "shard")?.cid,
    ).not.toBe(before.objects.find((object) => object.role === "shard")?.cid);
    expect(after.rootCid).not.toBe(before.rootCid);
  });

  it("rejects invalid canonical values, duplicate parcels and fixture injection", async () => {
    expect(() => publicationCanonicalJson({ missing: undefined })).toThrow(
      "Undefined value",
    );
    const duplicated = properties(2);
    duplicated[1]!.parcelIdentifier = duplicated[0]!.parcelIdentifier;
    await expect(
      buildPublicationGraph({
        completedAt: timestamp,
        exportedAt: timestamp,
        properties: duplicated,
      }),
    ).rejects.toThrow("Duplicate parcelIdentifier");
    const injected = properties(1);
    await expect(
      buildPublicationGraph({
        completedAt: timestamp,
        exportedAt: timestamp,
        fixturePropertyIds: new Set([injected[0]!.propertyId]),
        properties: injected,
      }),
    ).rejects.toThrow("fixture property injection");
  });
});
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
