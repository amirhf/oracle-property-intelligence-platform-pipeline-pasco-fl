import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("../../src/publication/candidate-source-snapshot-demo.js", async () => {
  const actual = await vi.importActual<
    typeof import("../../src/publication/candidate-source-snapshot-demo.js")
  >("../../src/publication/candidate-source-snapshot-demo.js");
  return {
    ...actual,
    validateCandidateSourceSnapshotDemoPlan: (value: unknown) => value,
  };
});

import { canonicalJson } from "../../src/lib/canonical-json.js";
import { McpContractRegistry } from "../../src/mcp/contracts.js";
import {
  PublicIpnsProvider,
  type PublicSourceSnapshotGraphEntry,
} from "../../src/mcp/public-ipns-provider.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import {
  toTypedControlCollectionReference,
  writeCompactPublicationManifestIndex,
  writeShardedControlCollection,
  type ControlArtifactBinding,
  type ShardedControlCollection,
} from "../../src/publication/control-artifacts.js";
import {
  OPEN_IPNS,
  QUERY_IPNS,
  syntheticPublicSet,
} from "../helpers/public-ipns.js";

const digest = (bytes: Uint8Array) =>
  createHash("sha256").update(bytes).digest("hex");
const controlPrefix = `publication-control/source-snapshot-demo-v1/snapshotns_${"1".repeat(32)}/`;
const openPrefix = `publications/source-snapshot-demo-v1/snapshotns_${"1".repeat(32)}/`;
const queryPrefix = `query-tables/source-snapshot-demo-v1/snapshotns_${"1".repeat(32)}/`;

describe("compact candidate source-snapshot public provider", () => {
  let contracts: McpContractRegistry;
  let outputRoot: string;

  beforeAll(async () => {
    contracts = await McpContractRegistry.create();
    outputRoot = await mkdtemp(path.join(tmpdir(), "source-snapshot-reader-"));
  });

  afterAll(async () => {
    await rm(outputRoot, { force: true, recursive: true });
  });

  it("cold-loads only compact controls and traverses one property on demand", async () => {
    const source = await syntheticPublicSet({
      candidatePlanBindings: true,
      sourceSnapshotBindings: true,
    });
    const root = JSON.parse(
      Buffer.from(
        source.objects.get(source.config.expectedOpenDataRootCid)!,
      ).toString("utf8"),
    ) as Record<string, unknown>;
    const sourceManifest = JSON.parse(
      Buffer.from(
        source.objects.get(source.config.expectedManifestCid)!,
      ).toString("utf8"),
    ) as { entries: Array<Record<string, unknown>> };
    const rootShards = root.shards as Array<Record<string, unknown>>;
    const shardCid = String(rootShards[0]!.shardCid);
    const shard = JSON.parse(
      Buffer.from(source.objects.get(shardCid)!).toString("utf8"),
    ) as { entries: Array<Record<string, unknown>> };
    const provenanceCid = [...source.objects.entries()].find(([, bytes]) => {
      try {
        const value = JSON.parse(Buffer.from(bytes).toString("utf8")) as {
          sources?: unknown;
        };
        return Array.isArray(value.sources);
      } catch {
        return false;
      }
    })![0];

    const manifestEntries = await writeShardedControlCollection({
      collection: "manifest_entries",
      entries: sourceManifest.entries.map((entry, position) => ({
        key: `entry:${String(position).padStart(9, "0")}:${String(entry.propertyId)}`,
        value: {
          ...entry,
          objectKey: `${openPrefix}${String(entry.objectKey)}`,
        },
      })),
      objectKeyPrefix: controlPrefix,
      outputRoot,
    });
    const graphEdges = await writeShardedControlCollection({
      collection: "graph_edges",
      entries: [
        ...shard.entries.map((entry, position) => ({
          key: `edge:${String(position).padStart(9, "0")}`,
          value: {
            childCid: entry.cid,
            childKey: `${openPrefix}properties/${String(entry.propertyId)}.json`,
            jsonPointer: `/entries/${position}/cid`,
            parentKey: `${openPrefix}shards/shard-0000.json`,
          },
        })),
        {
          key: "edge:000000002",
          value: {
            childCid: shardCid,
            childKey: `${openPrefix}shards/shard-0000.json`,
            jsonPointer: "/shards/0/shardCid",
            parentKey: `${openPrefix}index.json`,
          },
        },
      ],
      objectKeyPrefix: controlPrefix,
      outputRoot,
    });
    const inventoryValues = [
      await inventory(source, "open_data", "index.json", openPrefix),
      await inventory(
        source,
        "open_data",
        "provenance.json",
        openPrefix,
        provenanceCid,
      ),
      ...sourceManifest.entries.map((entry) => ({
        key: `open_data:${String(entry.objectKey)}`,
        value: {
          byteSize: Number(entry.bytes),
          domain: "open_data",
          expectedCid: String(entry.cid),
          logicalObjectKey: String(entry.objectKey),
          remoteObjectKey: `${openPrefix}${String(entry.objectKey)}`,
          sha256: String(entry.sha256),
        },
      })),
      await inventory(
        source,
        "open_data",
        "shards/shard-0000.json",
        openPrefix,
        shardCid,
      ),
      await inventory(
        source,
        "query_table",
        "query-tables/pasco/query-table.parquet",
        queryPrefix,
        source.config.expectedQueryTableRootCid,
      ),
    ].sort((left, right) => left.key.localeCompare(right.key));
    const objectInventory = await writeShardedControlCollection({
      collection: "object_inventory",
      entries: inventoryValues,
      objectKeyPrefix: controlPrefix,
      outputRoot,
    });
    const compactManifest = await writeCompactPublicationManifestIndex({
      manifest: {
        classification: classification(),
        contracts: {
          canonical: {
            sha256:
              "59c6472c2cd6d18041cf72c779fb970a082b00bef09aea724b99687e84198306",
            version: "1.0.0",
          },
          mcp: {
            sha256:
              "9fc112ef8a4e2120593c3dc20c90073b0eb96596817c96112f63fd258bb7c131",
            version: "1.2.0",
          },
        },
        county: "pasco",
        coverage: coverage(2),
        disclosure: "Synthetic candidate-owned source-snapshot reader test.",
        freshness: {
          asOf: "2026-08-29T00:00:00.000Z",
          loadedAt: "2026-08-29T00:00:00.000Z",
          observedAt: "2026-08-29T00:00:00.000Z",
        },
        graph: {
          openDataRootCid: source.config.expectedOpenDataRootCid,
          propertyCount: 2,
        },
        manifestEntries: toTypedControlCollectionReference(
          manifestEntries,
          "manifest_entries",
        ),
        queryTable: {
          byteSize: source.objects.get(source.config.expectedQueryTableRootCid)!
            .byteLength,
          expectedCid: source.config.expectedQueryTableRootCid,
          propertyCount: 2,
          schemaSha256: "8".repeat(64),
          sha256: digest(
            source.objects.get(source.config.expectedQueryTableRootCid)!,
          ),
        },
        source: sourceBinding(),
        version: "2.0.0",
      },
      objectKey: `${controlPrefix}manifest.json`,
      outputRoot,
    });
    await addControlObjects(source.objects, manifestEntries, outputRoot);
    await addControlObjects(source.objects, graphEdges, outputRoot);
    await addControlObjects(source.objects, objectInventory, outputRoot);
    await addArtifact(source.objects, compactManifest, outputRoot);

    const plan = fakePlan({
      compactManifest,
      graphEdges,
      manifestEntries,
      objectInventory,
      openCid: source.config.expectedOpenDataRootCid,
      queryCid: source.config.expectedQueryTableRootCid,
    });
    const planBytes = Buffer.from(`${canonicalJson(plan)}\n`);
    const planCid = await calculateIpfsCid(planBytes);
    source.objects.set(planCid, planBytes);
    const config = {
      ...source.config,
      candidateDemoPlanId: plan.planId,
      candidateDemoPlanSha256: plan.planSha256,
      candidateDemoSourcePlanSha256: plan.source.sourcePlanSha256,
      expectedManifestCid: compactManifest.expectedCid,
      expectedManifestSha256: compactManifest.sha256,
      expectedPlanCid: planCid,
      expectedPlanSha256: digest(planBytes),
    };
    source.transport.resolutions.set(
      OPEN_IPNS,
      resolved(config.expectedOpenDataRootCid),
    );
    source.transport.resolutions.set(
      QUERY_IPNS,
      resolved(config.expectedQueryTableRootCid),
    );
    const bindings = sourceManifest.entries.map((entry, position) => ({
      byteSize: 0,
      canonicalPropertyId: String(entry.propertyId),
      cid: String(entry.cid),
      parcelIdentifier: String(entry.parcelIdentifier),
      position,
      publicPropertyId: String(entry.propertyId).replace("property_", "prop_"),
      sha256: String(entry.sha256),
    })) satisfies PublicSourceSnapshotGraphEntry[];
    const queryResult = {
      entries: bindings,
      queryRows: bindings.map((entry, index) => ({
        canonicalPropertyId: entry.canonicalPropertyId,
        latitude: index === 0 ? 28.3 : null,
        longitude: index === 0 ? -82.4 : null,
        maximumOpenRoofingPermitDays: null,
        observedAt: "2026-08-29T00:00:00.000Z",
        openRoofingPermitCount: null,
        propertyDocumentSha256: entry.sha256,
        propertyId: entry.publicPropertyId,
        publishedAt: null,
        roofAgeBasis: "year_built_proxy",
        roofAgeBasisQuality: "proxy",
        roofAgeYears: 20,
        siteCity: "Synthetic",
      })),
    };
    let rejectFirstLoad!: (error: Error) => void;
    const firstLoad = new Promise<never>((_resolve, reject) => {
      rejectFirstLoad = reject;
    });
    let queryReaderCalls = 0;
    const stages: string[] = [];
    let rangeTouched = false;
    const provider = await PublicIpnsProvider.create(
      config,
      contracts,
      source.transport,
      undefined,
      (stage) => stages.push(stage),
      {
        rangeTransport: {
          readCidRange: async () => {
            rangeTouched = true;
            throw new Error("range test seam must bypass transport");
          },
          statCid: async () => {
            rangeTouched = true;
            throw new Error("range test seam must bypass transport");
          },
        },
        sourceSnapshotQueryTableReader: async () => {
          queryReaderCalls += 1;
          if (queryReaderCalls === 1) return firstLoad;
          return queryResult;
        },
      },
    );
    expect((await provider.getMetadata()).coverageMode).toBe("source_snapshot");
    expect(queryReaderCalls).toBe(0);
    expect(stages).not.toContain("parquet");
    expect(rangeTouched).toBe(false);
    const coldReads = new Set(source.transport.reads);
    expect(coldReads.has(source.config.expectedOpenDataRootCid)).toBe(false);
    expect(coldReads.has(shardCid)).toBe(false);

    const firstQuery = provider.getQueryRows();
    const concurrentQuery = provider.getQueryRows();
    const firstOutcomes = Promise.allSettled([firstQuery, concurrentQuery]);
    await vi.waitFor(() => expect(queryReaderCalls).toBe(1));
    rejectFirstLoad(new Error("synthetic transient range failure"));
    expect(await firstOutcomes).toEqual([
      expect.objectContaining({ status: "rejected" }),
      expect.objectContaining({ status: "rejected" }),
    ]);

    const [queryRows, property] = await Promise.all([
      provider.getQueryRows(),
      provider.getCanonicalProperty(bindings[0]!.publicPropertyId),
    ]);
    expect(queryRows).toEqual(queryResult.queryRows);
    expect(property?.propertyId).toBe(bindings[0]!.canonicalPropertyId);
    expect(queryReaderCalls).toBe(2);
    expect(stages.filter((stage) => stage === "parquet")).toHaveLength(2);

    expect(await provider.getQueryRows()).toBe(queryRows);
    expect(queryReaderCalls).toBe(2);
    expect(source.transport.reads).toContain(
      source.config.expectedOpenDataRootCid,
    );
    expect(source.transport.reads).toContain(shardCid);
    expect(source.transport.reads).toContain(bindings[0]!.cid);
  }, 30_000);
});

function classification() {
  return {
    canonical: false as const,
    elephantOwned: false as const,
    independentlyPascoCertified: false as const,
    ownerControlled: false as const,
    publicationClass: "candidate_owned_source_snapshot_demo" as const,
    resourceOwner: "candidate" as const,
    sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot" as const,
  };
}

function coverage(propertyCount: number) {
  return {
    buildings: { facts: 2, properties: 2, yearBuiltProxyProperties: 2 },
    contractors: { availability: "unavailable" as const, facts: 0 as const },
    coordinates: { availableProperties: 1, missingProperties: 1 },
    membership:
      "complete_membership_of_exact_source_snapshot_noncanonical" as const,
    ownership: {
      acceptedRows: 2,
      malformedRows: 0,
      properties: 2,
      sourceRows: 2,
    },
    permits: {
      availability: "unavailable" as const,
      facts: 0 as const,
      permitContractorRelationships: 0 as const,
    },
    propertyCount,
    siteAddresses: { sourceRows: 2, usableProperties: 2 },
    unresolvedPublishedParcelStatistic: null,
  };
}

function sourceBinding() {
  return {
    authorityClass: "owner_assumed_authoritative_snapshot" as const,
    authorityId: `authority_${"1".repeat(32)}`,
    materializationId: `materialization_${"2".repeat(32)}`,
    materializationSha256: "3".repeat(64),
    runId: `run_${"a".repeat(32)}`,
    scopeId: `scope_${"b".repeat(32)}`,
    selectionSha256: "c".repeat(64),
    snapshotId: `snapshot_${"7".repeat(32)}`,
  };
}

function resolved(cid: string) {
  return ["a", "b"].map((resolver) => ({
    cacheAgeSeconds: 0,
    cid,
    observedAt: "2026-08-29T00:00:00.000Z",
    resolver,
    status: "resolved" as const,
  }));
}

async function inventory(
  source: Awaited<ReturnType<typeof syntheticPublicSet>>,
  domain: "open_data" | "query_table",
  logicalObjectKey: string,
  prefix: string,
  cid?: string,
) {
  const expectedCid = cid ?? source.config.expectedOpenDataRootCid;
  const bytes = source.objects.get(expectedCid)!;
  return {
    key: `${domain}:${logicalObjectKey}`,
    value: {
      byteSize: bytes.byteLength,
      domain,
      expectedCid,
      logicalObjectKey,
      remoteObjectKey: `${prefix}${logicalObjectKey.replace(/^query-tables\/pasco\//, "")}`,
      sha256: digest(bytes),
    },
  };
}

async function addArtifact(
  objects: Map<string, Uint8Array>,
  artifact: ControlArtifactBinding,
  root: string,
) {
  objects.set(
    artifact.expectedCid,
    await readFile(path.join(root, ...artifact.objectKey.split("/"))),
  );
}

async function addControlObjects(
  objects: Map<string, Uint8Array>,
  collection: ShardedControlCollection,
  root: string,
) {
  for (const artifact of [
    ...collection.index.shards,
    collection.indexArtifact,
  ]) {
    await addArtifact(objects, artifact, root);
  }
}

function fakePlan(input: {
  compactManifest: ControlArtifactBinding;
  graphEdges: ShardedControlCollection;
  manifestEntries: ShardedControlCollection;
  objectInventory: ShardedControlCollection;
  openCid: string;
  queryCid: string;
}) {
  const source = sourceBinding();
  return {
    classification: classification(),
    controlArtifacts: {
      graphEdges: toTypedControlCollectionReference(
        input.graphEdges,
        "graph_edges",
      ),
      manifestEntries: toTypedControlCollectionReference(
        input.manifestEntries,
        "manifest_entries",
      ),
      manifestIndex: input.compactManifest,
      objectInventory: toTypedControlCollectionReference(
        input.objectInventory,
        "object_inventory",
      ),
      payloadObjectCount: input.objectInventory.index.entryCount,
    },
    coverage: {
      activeProperties: 2,
      buildingFacts: 2,
      buildingProperties: 2,
      contractorCoverage: "unavailable",
      coordinateProperties: 1,
      duplicatePropertyIdentities: 0,
      fixtureMatches: 0,
      inactiveProperties: 0,
      missingCoordinateProperties: 1,
      ownershipAcceptedRows: 2,
      ownershipMalformedRows: 0,
      ownershipProperties: 2,
      ownershipSourceRows: 2,
      permitCoverage: "unavailable",
      siteAddressProperties: 2,
      siteAddressRows: 2,
      yearBuiltProxyProperties: 2,
    },
    disclaimer: "Synthetic candidate-owned source-snapshot reader test.",
    inventory: { objectCount: input.objectInventory.index.entryCount + 8 },
    limits: {
      maxObjectBytes: 512 * 1024 * 1024,
      maxRequests: 10_000,
      maxTotalBytes: 1024 * 1024 * 1024,
    },
    planId: `snapshotdemo_${"8".repeat(32)}`,
    planSha256: "9".repeat(64),
    source: {
      ...source,
      csvSha256: "d".repeat(64),
      folioSetSha256: source.selectionSha256,
      sourcePlanSha256: "a".repeat(64),
      zipSha256: "e".repeat(64),
      workflowRunId: source.runId,
    },
    targets: {
      controlPrefix,
      openData: {
        immutablePrefix: openPrefix,
        ipnsNetworkKey: OPEN_IPNS,
        targetCid: input.openCid,
      },
      queryTable: {
        immutablePrefix: queryPrefix,
        ipnsNetworkKey: QUERY_IPNS,
        targetCid: input.queryCid,
      },
    },
    version: "2.1.0",
  };
}
