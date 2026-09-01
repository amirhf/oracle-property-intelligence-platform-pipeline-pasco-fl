import { createHash, randomUUID } from "node:crypto";
import { link, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import path from "node:path";

import { importer } from "ipfs-unixfs-importer";

import { canonicalJson } from "../lib/canonical-json.js";
import { recordCandidateSourceSnapshotCarArtifact } from "../db/candidate-source-snapshot-car-import.js";
import { sha256 } from "../lib/hash.js";
import type { CandidateSourceSnapshotBuildDescriptor } from "./candidate-source-snapshot-build.js";
import type {
  CandidateSourceSnapshotDemoPlan,
  CandidateSourceSnapshotUploadObject,
} from "./candidate-source-snapshot-demo.js";
import { prepareCandidateSourceSnapshotExecutionBundle } from "./candidate-source-snapshot-session2.js";
import {
  createCarV1Writer,
  validateCarV1,
  type CarV1Identity,
} from "./car-v1.js";
import { IPFS_IMPORTER_OPTIONS } from "./ipfs-cid.js";

const MAX_CAR_HEADER_BYTES = 32 * 1024 * 1024;
const MAX_CAR_SECTION_BYTES = 1024 * 1024;

type PublicationDomain = "open_data" | "query_table";

interface DomainInventory {
  bytes: number;
  count: number;
  members: CandidateSourceSnapshotUploadObject[];
  membershipSha256: string;
  primaryRootCid: string;
  roots: string[];
  rootsSha256: string;
}

export interface CandidateSourceSnapshotCarIdentity {
  blockCount: number;
  blockMembershipSha256: string;
  byteSize: number;
  carFile: string;
  carSha256: string;
  domain: PublicationDomain;
  logicalMemberBytes: number;
  logicalMemberCount: number;
  logicalMembershipSha256: string;
  primaryRootCid: string;
  rootCount: number;
  rootsSha256: string;
}

export interface CandidateSourceSnapshotCarBuildResult {
  cars: readonly [
    CandidateSourceSnapshotCarIdentity,
    CandidateSourceSnapshotCarIdentity,
  ];
  exactObjectCount: number;
  exactTotalBytes: number;
  planId: string;
  planSha256: string;
  resultFile: string;
  schemaVersion: "candidate-source-snapshot-car-build-v1";
}

interface InventoryPass {
  domains: Record<PublicationDomain, DomainInventory>;
  exactObjectCount: number;
  exactTotalBytes: number;
}

function memberRecord(object: CandidateSourceSnapshotUploadObject): string {
  return `${canonicalJson([
    object.domain,
    object.remoteObjectKey,
    object.expectedCid,
    object.sha256,
    object.byteSize,
  ])}\n`;
}

async function inventoryPass(input: {
  createObjects: () => AsyncIterable<CandidateSourceSnapshotUploadObject>;
  expectedObjectCount: number;
  expectedTotalBytes: number;
  plan: CandidateSourceSnapshotDemoPlan;
}): Promise<InventoryPass> {
  const states = {
    open_data: {
      bytes: 0,
      count: 0,
      hash: createHash("sha256"),
      members: [] as CandidateSourceSnapshotUploadObject[],
      roots: [] as string[],
      rootSet: new Set<string>(),
    },
    query_table: {
      bytes: 0,
      count: 0,
      hash: createHash("sha256"),
      members: [] as CandidateSourceSnapshotUploadObject[],
      roots: [] as string[],
      rootSet: new Set<string>(),
    },
  };
  const objectKeys = new Set<string>();
  let exactObjectCount = 0;
  let exactTotalBytes = 0;
  for await (const object of input.createObjects()) {
    const key = `${object.domain}:${object.remoteObjectKey}`;
    if (objectKeys.has(key)) {
      throw new Error("Candidate CAR inventory contains a duplicate object key");
    }
    objectKeys.add(key);
    const state = states[object.domain];
    state.count += 1;
    state.bytes += object.byteSize;
    state.hash.update(memberRecord(object));
    state.members.push(object);
    if (!state.rootSet.has(object.expectedCid)) {
      state.rootSet.add(object.expectedCid);
      state.roots.push(object.expectedCid);
    }
    exactObjectCount += 1;
    exactTotalBytes += object.byteSize;
  }
  if (
    exactObjectCount !== input.expectedObjectCount ||
    exactTotalBytes !== input.expectedTotalBytes
  ) {
    throw new Error("Candidate CAR inventory differs from the immutable plan");
  }
  const finalize = (
    domain: PublicationDomain,
    primaryRootCid: string,
  ): DomainInventory => {
    const state = states[domain];
    if (!state.rootSet.has(primaryRootCid)) {
      throw new Error("Candidate CAR primary root is absent from its inventory");
    }
    const roots = [
      primaryRootCid,
      ...state.roots.filter((cid) => cid !== primaryRootCid),
    ];
    return {
      bytes: state.bytes,
      count: state.count,
      membershipSha256: state.hash.digest("hex"),
      members: state.members,
      primaryRootCid,
      roots,
      rootsSha256: sha256(canonicalJson(roots)),
    };
  };
  return {
    domains: {
      open_data: finalize("open_data", input.plan.targets.openData.targetCid),
      query_table: finalize(
        "query_table",
        input.plan.targets.queryTable.targetCid,
      ),
    },
    exactObjectCount,
    exactTotalBytes,
  };
}

async function writeObjectBlocks(input: {
  appendBlock(cid: string, bytes: Uint8Array): Promise<void>;
  object: CandidateSourceSnapshotUploadObject;
  openSource: NonNullable<
    Awaited<
      ReturnType<typeof prepareCandidateSourceSnapshotExecutionBundle>
    >["localSource"]["openCarSource"]
  >;
  seenBlocks: Set<string>;
}): Promise<void> {
  const source = await input.openSource(input.object);
  if (source.contentLength !== input.object.byteSize) {
    await source.release();
    throw new Error("Candidate CAR source size differs from its inventory");
  }
  const sourceHash = createHash("sha256");
  let sourceBytes = 0;
  let rootCid: string | null = null;
  const content = (async function* (): AsyncGenerator<Uint8Array> {
    for await (const value of source.body) {
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
      sourceHash.update(chunk);
      sourceBytes += chunk.byteLength;
      yield chunk;
    }
  })();
  const block = {
    get: async (): Promise<never> => {
      throw new Error("Candidate CAR importer performed an unexpected read");
    },
    put: async (
      bytes: Uint8Array,
      options?: { cid?: { toString(): string } },
    ): Promise<Uint8Array> => {
      const cid = options?.cid?.toString();
      if (!cid) throw new Error("Candidate CAR importer omitted a block CID");
      if (!input.seenBlocks.has(cid)) {
        await input.appendBlock(cid, bytes);
        input.seenBlocks.add(cid);
      }
      return bytes;
    },
  };
  try {
    for await (const result of importer(
      [{ content }],
      block,
      {
        ...IPFS_IMPORTER_OPTIONS,
        blockWriteConcurrency: 1,
        fileImportConcurrency: 1,
        onlyHash: false,
      },
    )) {
      rootCid = result.cid.toString();
    }
  } finally {
    await source.release();
  }
  if (
    rootCid !== input.object.expectedCid ||
    sourceBytes !== input.object.byteSize ||
    sourceHash.digest("hex") !== input.object.sha256
  ) {
    throw new Error("Candidate CAR object bytes differ from the immutable plan");
  }
}

function carIdentity(input: {
  carFile: string;
  domain: PublicationDomain;
  inventory: DomainInventory;
  validated: CarV1Identity;
}): CandidateSourceSnapshotCarIdentity {
  return {
    blockCount: input.validated.blockCount,
    blockMembershipSha256: input.validated.blockMembershipSha256,
    byteSize: input.validated.byteSize,
    carFile: input.carFile,
    carSha256: input.validated.sha256,
    domain: input.domain,
    logicalMemberBytes: input.inventory.bytes,
    logicalMemberCount: input.inventory.count,
    logicalMembershipSha256: input.inventory.membershipSha256,
    primaryRootCid: input.inventory.primaryRootCid,
    rootCount: input.inventory.roots.length,
    rootsSha256: input.inventory.rootsSha256,
  };
}

async function promoteImmutable(input: {
  contenderPath: string;
  finalPath: string;
  identity: CarV1Identity;
  roots: readonly string[];
}): Promise<void> {
  try {
    await link(input.contenderPath, input.finalPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    const existing = await validateCarV1({
      expectedBlockCount: input.identity.blockCount,
      expectedBlockMembershipSha256: input.identity.blockMembershipSha256,
      expectedRoots: [...input.roots],
      filePath: input.finalPath,
      maxHeaderBytes: MAX_CAR_HEADER_BYTES,
      maxSectionBytes: MAX_CAR_SECTION_BYTES,
    });
    if (
      existing.byteSize !== input.identity.byteSize ||
      existing.sha256 !== input.identity.sha256
    ) {
      throw new Error(
        "Existing candidate CAR differs from deterministic output",
        { cause: error },
      );
    }
  } finally {
    await unlink(input.contenderPath).catch((error: unknown) => {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    });
  }
}

async function promoteResult(input: {
  bytes: Buffer;
  finalPath: string;
}): Promise<void> {
  const contender = `${input.finalPath}.contender-${process.pid}-${randomUUID()}`;
  await writeFile(contender, input.bytes, { flag: "wx" });
  try {
    await link(contender, input.finalPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    const existing = await readFile(input.finalPath);
    if (!existing.equals(input.bytes)) {
      throw new Error("Existing candidate CAR result differs", {
        cause: error,
      });
    }
  } finally {
    await unlink(contender).catch((error: unknown) => {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    });
  }
}

export async function buildCandidateSourceSnapshotCars(input: {
  descriptor: CandidateSourceSnapshotBuildDescriptor;
  outputDirectory: string;
  record?: {
    databaseUrl: string;
    implementationCommitSha: string;
    recordedAt: string;
  };
}): Promise<CandidateSourceSnapshotCarBuildResult> {
  const outputDirectory = path.resolve(input.outputDirectory);
  if (outputDirectory === path.parse(outputDirectory).root) {
    throw new Error("Candidate CAR output must not be a filesystem root");
  }
  await mkdir(outputDirectory, { recursive: true });
  const bundle = await prepareCandidateSourceSnapshotExecutionBundle(
    input.descriptor,
  );
  const openSource = bundle.localSource.openCarSource;
  if (!openSource) {
    throw new Error("Candidate local source does not support streaming CAR input");
  }
  const inventory = await inventoryPass({
    createObjects: bundle.createObjects,
    expectedObjectCount: bundle.build.exactObjectCount,
    expectedTotalBytes: bundle.build.exactTotalBytes,
    plan: bundle.build.plan,
  });
  const nonce = `${process.pid}-${randomUUID()}`;
  const fileNames = {
    open_data: `${bundle.build.plan.planId}-open-data.car`,
    query_table: `${bundle.build.plan.planId}-query-table.car`,
  } as const;
  const contenderPaths = {
    open_data: path.join(outputDirectory, `.${fileNames.open_data}.${nonce}`),
    query_table: path.join(outputDirectory, `.${fileNames.query_table}.${nonce}`),
  };
  const openWriter = await createCarV1Writer({
    maxHeaderBytes: MAX_CAR_HEADER_BYTES,
    maxSectionBytes: MAX_CAR_SECTION_BYTES,
    outputPath: contenderPaths.open_data,
    roots: inventory.domains.open_data.roots,
  });
  let queryWriter: Awaited<ReturnType<typeof createCarV1Writer>>;
  try {
    queryWriter = await createCarV1Writer({
      maxHeaderBytes: MAX_CAR_HEADER_BYTES,
      maxSectionBytes: MAX_CAR_SECTION_BYTES,
      outputPath: contenderPaths.query_table,
      roots: inventory.domains.query_table.roots,
    });
  } catch (error) {
    await openWriter.abort().catch(() => undefined);
    throw error;
  }
  const writers = {
    open_data: openWriter,
    query_table: queryWriter,
  };
  const seenBlocks = {
    open_data: new Set<string>(),
    query_table: new Set<string>(),
  };
  try {
    for await (const object of bundle.createObjects()) {
      const writer = writers[object.domain];
      await writeObjectBlocks({
        appendBlock: writer.appendBlock.bind(writer),
        object,
        openSource: openSource.bind(bundle.localSource),
        seenBlocks: seenBlocks[object.domain],
      });
    }
    const closed = {
      open_data: await writers.open_data.close(),
      query_table: await writers.query_table.close(),
    };
    const validated = {
      open_data: await validateCarV1({
        expectedBlockCount: closed.open_data.blockCount,
        expectedBlockMembershipSha256:
          closed.open_data.blockMembershipSha256,
        expectedRoots: inventory.domains.open_data.roots,
        filePath: contenderPaths.open_data,
        maxHeaderBytes: MAX_CAR_HEADER_BYTES,
        maxSectionBytes: MAX_CAR_SECTION_BYTES,
      }),
      query_table: await validateCarV1({
        expectedBlockCount: closed.query_table.blockCount,
        expectedBlockMembershipSha256:
          closed.query_table.blockMembershipSha256,
        expectedRoots: inventory.domains.query_table.roots,
        filePath: contenderPaths.query_table,
        maxHeaderBytes: MAX_CAR_HEADER_BYTES,
        maxSectionBytes: MAX_CAR_SECTION_BYTES,
      }),
    };
    for (const domain of ["open_data", "query_table"] as const) {
      if (
        validated[domain].byteSize !== closed[domain].byteSize ||
        validated[domain].sha256 !== closed[domain].sha256
      ) {
        throw new Error("Candidate CAR writer and validator identities differ");
      }
      await promoteImmutable({
        contenderPath: contenderPaths[domain],
        finalPath: path.join(outputDirectory, fileNames[domain]),
        identity: validated[domain],
        roots: inventory.domains[domain].roots,
      });
    }
    const result = {
      cars: [
        carIdentity({
          carFile: fileNames.open_data,
          domain: "open_data",
          inventory: inventory.domains.open_data,
          validated: validated.open_data,
        }),
        carIdentity({
          carFile: fileNames.query_table,
          domain: "query_table",
          inventory: inventory.domains.query_table,
          validated: validated.query_table,
        }),
      ],
      exactObjectCount: inventory.exactObjectCount,
      exactTotalBytes: inventory.exactTotalBytes,
      planId: bundle.build.plan.planId,
      planSha256: bundle.build.plan.planSha256,
      resultFile: `${bundle.build.plan.planId}-cars.json`,
      schemaVersion: "candidate-source-snapshot-car-build-v1",
    } satisfies CandidateSourceSnapshotCarBuildResult;
    const resultBytes = Buffer.from(`${canonicalJson(result)}\n`, "utf8");
    await promoteResult({
      bytes: resultBytes,
      finalPath: path.join(outputDirectory, result.resultFile),
    });
    if (input.record) {
      for (const car of result.cars) {
        const domain = car.domain;
        await recordCandidateSourceSnapshotCarArtifact(
          input.record.databaseUrl,
          {
            blockCount: car.blockCount,
            blockMembershipSha256: car.blockMembershipSha256,
            carBytes: car.byteSize,
            carRole: domain,
            carSha256: car.carSha256,
            implementationCommitSha: input.record.implementationCommitSha,
            members: inventory.domains[domain].members.map((member) => ({
              byteSize: member.byteSize,
              domain: member.domain,
              expectedCid: member.expectedCid,
              remoteObjectKey: member.remoteObjectKey,
              sha256: member.sha256,
            })),
            planId: result.planId,
            planSha256: result.planSha256,
            primaryRootCid: car.primaryRootCid,
            recordedAt: input.record.recordedAt,
            roots: inventory.domains[domain].roots,
          },
        );
      }
    }
    return result;
  } catch (error) {
    await Promise.all([
      writers.open_data.abort().catch(() => undefined),
      writers.query_table.abort().catch(() => undefined),
    ]);
    await Promise.all(
      Object.values(contenderPaths).map(async (contenderPath) => {
        await unlink(contenderPath).catch((unlinkError: unknown) => {
          if ((unlinkError as NodeJS.ErrnoException).code !== "ENOENT") {
            throw unlinkError;
          }
        });
      }),
    );
    throw error;
  }
}
