import { sha256 } from "../../src/lib/hash.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import {
  createPublicationPlan,
  type PublicationPlan,
} from "../../src/publication/plan.js";

export async function syntheticSamplePublicationPlan(): Promise<PublicationPlan> {
  const propertyBytes = Buffer.from('{"synthetic":true}\n');
  const shardBytes = Buffer.from('{"syntheticShard":true}\n');
  const rootBytes = Buffer.from('{"syntheticRoot":true}\n');
  const manifestBytes = Buffer.from('{"syntheticManifest":true}\n');
  const coverageBytes = Buffer.from('{"syntheticCoverage":true}\n');
  const provenanceBytes = Buffer.from('{"syntheticProvenance":true}\n');
  const parquetBytes = Buffer.from("PAR1syntheticPAR1");
  const [
    propertyCid,
    shardCid,
    rootCid,
    manifestCid,
    coverageCid,
    provenanceCid,
    parquetCid,
  ] = await Promise.all([
    calculateIpfsCid(propertyBytes),
    calculateIpfsCid(shardBytes),
    calculateIpfsCid(rootBytes),
    calculateIpfsCid(manifestBytes),
    calculateIpfsCid(coverageBytes),
    calculateIpfsCid(provenanceBytes),
    calculateIpfsCid(parquetBytes),
  ] as const);
  const binding = (objectKey: string, bytes: Buffer, expectedCid: string) => ({
    byteSize: bytes.length,
    expectedCid,
    objectKey,
    sha256: sha256(bytes),
  });
  const property = binding(
    "properties/property_synthetic.json",
    propertyBytes,
    propertyCid,
  );
  const shard = binding("shards/shard-0000.json", shardBytes, shardCid);
  const root = binding("index.json", rootBytes, rootCid);
  const manifest = binding("manifest.json", manifestBytes, manifestCid);
  const coverage = binding("coverage.json", coverageBytes, coverageCid);
  const provenance = binding("provenance.json", provenanceBytes, provenanceCid);
  const parquet = binding(
    "query-tables/pasco/query-table.parquet",
    parquetBytes,
    parquetCid,
  );
  const objectInventory = [
    { ...property, domain: "open_data" as const, role: "property" as const },
    { ...shard, domain: "open_data" as const, role: "shard" as const },
    { ...root, domain: "open_data" as const, role: "root" as const },
    { ...manifest, domain: "open_data" as const, role: "manifest" as const },
    { ...coverage, domain: "open_data" as const, role: "metadata" as const },
    { ...provenance, domain: "open_data" as const, role: "metadata" as const },
    {
      ...parquet,
      domain: "query_table" as const,
      role: "query_table" as const,
    },
  ];
  return createPublicationPlan({
    approvable: false,
    artifacts: {
      coverage,
      manifest,
      objectInventory,
      parquet: {
        ...parquet,
        distinctPropertyIds: 1,
        nullPropertyIds: 0,
        rowCount: 1,
        schemaSha256: "8".repeat(64),
      },
      provenance,
      shards: [{ ...shard, propertyCount: 1 }],
    },
    configuration: {
      credentialsAvailable: false,
      missing: [
        "filebase_credentials",
        "open_data_bucket",
        "open_data_ipns_network_key",
        "query_table_bucket",
        "query_table_ipns_network_key",
      ],
    },
    contracts: {
      canonical: { sha256: "5".repeat(64), version: "1.0.0" },
      mcp: { sha256: "9".repeat(64), version: "1.2.0" },
    },
    counts: {
      activeProperties: 1,
      canonicalDocuments: 1,
      coordinateRows: 1,
      inactiveProperties: 0,
      queryTableDistinctPropertyIds: 1,
      queryTableNullPropertyIds: 0,
      queryTableRows: 1,
    },
    county: "pasco",
    coverage: {
      authoritativeHeadSnapshotId: null,
      authoritySourceSystem: "pasco_appraiser",
      completenessResult: "not_applicable",
      entityType: "property_existence",
      mode: "sample",
      predecessorChainSnapshotIds: [],
      runId: `run_${"1".repeat(32)}`,
      scopeId: `scope_${"2".repeat(32)}`,
      selection: {
        algorithm: "synthetic-candidate-demo-sample-v1",
        seed: "synthetic-only",
        selectedRecordSha256: "3".repeat(64),
        selectionSize: 1,
      },
      sourceSnapshotId: null,
      sourceSnapshotManifestSha256: null,
      workflowId: "synthetic-candidate-demo",
    },
    executable: false,
    exportMode: "bounded",
    fixtureExclusion: {
      fixturePropertyIdCount: 13,
      matches: 0,
      passed: true,
    },
    freshness: {
      asOf: "2026-08-30T00:00:00.000Z",
      loadedAt: "2026-08-30T00:00:00.000Z",
      observedAt: "2026-08-30T00:00:00.000Z",
    },
    generatedAt: "2026-08-30T00:00:00.000Z",
    graph: {
      cidProfile: {
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
      },
      edges: [
        {
          childCid: shardCid,
          childKey: shard.objectKey,
          jsonPointer: "/shards/0/shardCid",
          parentKey: root.objectKey,
        },
        {
          childCid: propertyCid,
          childKey: property.objectKey,
          jsonPointer: "/entries/0/cid",
          parentKey: shard.objectKey,
        },
      ],
      openDataRoot: { expectedCid: rootCid, objectKey: "index.json" },
      parquetProfile: {
        compression: "ZSTD",
        duckdbVersion: "synthetic",
        rowGroupSize: 10_000,
        schemaSha256: "8".repeat(64),
      },
      propertyCidCount: 1,
      queryTableRoot: {
        expectedCid: parquetCid,
        objectKey: "query-tables/pasco/query-table.parquet",
      },
      traversalValidated: true,
    },
    limitations: ["Synthetic test plan; no remote publication."],
    projection: {
      authoritativeBaseSnapshotId: null,
      materializationId: `materialization_${"4".repeat(32)}`,
      materializationSha256: "6".repeat(64),
      snapshotContentSha256: null,
    },
    remoteState: {
      openDataIpnsMutationPerformed: false,
      openDataPublishedCid: null,
      queryTableIpnsMutationPerformed: false,
      queryTablePublishedCid: null,
    },
    targets: {
      openData: {
        bucket: null,
        bucketConfirmed: false,
        ipnsLabel: "pending-open-data",
        ipnsNetworkKey: null,
      },
      queryTable: {
        bucket: null,
        bucketConfirmed: false,
        ipnsLabel: "pending-query-table",
        ipnsNetworkKey: null,
      },
    },
    temporalFactLimitation: "Synthetic test plan.",
    version: "1.1.0",
  });
}
