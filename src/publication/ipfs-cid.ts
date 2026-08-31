import { createRequire } from "node:module";
import { createReadStream } from "node:fs";

const require = createRequire(import.meta.url);

interface IpfsOnlyHashModule {
  of(content: Uint8Array, options: Record<string, unknown>): Promise<string>;
}

interface UnixfsImporterModule {
  importer(
    source: Array<{ content: AsyncIterable<Uint8Array> }>,
    block: {
      get(cid: unknown): Promise<never>;
      put(block: unknown): Promise<never>;
    },
    options: Record<string, unknown>,
  ): AsyncIterable<{ cid: { toString(): string } }>;
}

const ipfsOnlyHash = require("ipfs-only-hash") as IpfsOnlyHashModule;
const unixfsImporter = require("ipfs-unixfs-importer") as UnixfsImporterModule;

export const IPFS_CID_PROFILE = Object.freeze({
  cidVersion: 0 as const,
  chunker: "fixed" as const,
  chunkSize: 262_144,
  codec: "dag-pb" as const,
  hashAlg: "sha2-256" as const,
  importer: "ipfs-unixfs-importer@7.0.3" as const,
  layout: "balanced" as const,
  maxChildrenPerNode: 174,
  onlyHash: true as const,
  rawLeaves: false as const,
  reduceSingleLeafToSelf: true as const,
  trickle: false as const,
  unixfsType: "file" as const,
  version: "ipfs-only-hash@4.0.0" as const,
  wrapWithDirectory: false as const,
});

export const CIDV0_PATTERN = /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/;

export const IPFS_IMPORTER_OPTIONS = Object.freeze({
  avgChunkSize: IPFS_CID_PROFILE.chunkSize,
  cidVersion: IPFS_CID_PROFILE.cidVersion,
  chunker: IPFS_CID_PROFILE.chunker,
  hashAlg: IPFS_CID_PROFILE.hashAlg,
  leafType: "file" as const,
  maxChunkSize: IPFS_CID_PROFILE.chunkSize,
  maxChildrenPerNode: IPFS_CID_PROFILE.maxChildrenPerNode,
  minChunkSize: IPFS_CID_PROFILE.chunkSize,
  onlyHash: IPFS_CID_PROFILE.onlyHash,
  rawLeaves: IPFS_CID_PROFILE.rawLeaves,
  reduceSingleLeafToSelf: IPFS_CID_PROFILE.reduceSingleLeafToSelf,
  strategy: IPFS_CID_PROFILE.layout,
  wrapWithDirectory: IPFS_CID_PROFILE.wrapWithDirectory,
});

export async function calculateIpfsCid(
  bytes: Uint8Array | string,
): Promise<string> {
  const content =
    typeof bytes === "string" ? Buffer.from(bytes, "utf8") : bytes;
  const cid = await ipfsOnlyHash.of(content, IPFS_IMPORTER_OPTIONS);
  if (!CIDV0_PATTERN.test(cid)) {
    throw new Error(`Local UnixFS hashing returned an invalid CIDv0 (${cid})`);
  }
  return cid;
}

export async function calculateIpfsFileCid(filePath: string): Promise<string> {
  const block = {
    get: async (): Promise<never> => {
      throw new Error("Unexpected block read during only-hash import");
    },
    put: async (): Promise<never> => {
      throw new Error("Unexpected block write during only-hash import");
    },
  };
  let cid: string | null = null;
  for await (const result of unixfsImporter.importer(
    [{ content: createReadStream(filePath) }],
    block,
    IPFS_IMPORTER_OPTIONS,
  )) {
    cid = result.cid.toString();
  }
  if (cid === null || !CIDV0_PATTERN.test(cid)) {
    throw new Error("Local streamed UnixFS hashing returned an invalid CIDv0");
  }
  return cid;
}

export async function verifyIpfsCid(
  bytes: Uint8Array | string,
  expectedCid: string,
): Promise<void> {
  if (!CIDV0_PATTERN.test(expectedCid)) {
    throw new Error("Expected CID is not a CIDv0 dag-pb sha2-256 identifier");
  }
  const actual = await calculateIpfsCid(bytes);
  if (actual !== expectedCid) {
    throw new Error(`CID mismatch (expected=${expectedCid}, actual=${actual})`);
  }
}
