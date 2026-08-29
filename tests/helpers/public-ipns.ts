import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";

import type { PublicIpnsProviderConfig } from "../../src/mcp/config.js";
import type {
  IpnsResolutionObservation,
  PublicReadTransport,
} from "../../src/mcp/public-ipns-provider.js";
import {
  queryTableColumns,
  unavailablePublicationFields,
} from "../../src/publication/dry-run.js";
import {
  buildPublicationGraph,
  publicationCanonicalJson,
} from "../../src/publication/graph.js";
import {
  calculateIpfsCid,
  IPFS_CID_PROFILE,
} from "../../src/publication/ipfs-cid.js";
import {
  createPublicationPlan,
  type PublicationArtifact,
} from "../../src/publication/plan.js";

export const OPEN_IPNS = `k51${"a".repeat(50)}`;
export const QUERY_IPNS = `k51${"b".repeat(50)}`;
export const SYNTHETIC_OWNER_SENTINEL = "SYNTHETIC PRIVATE OWNER SENTINEL";
export const SYNTHETIC_MAILING_SENTINEL = "SYNTHETIC PRIVATE MAILING SENTINEL";
export const SYNTHETIC_PHONE_SENTINEL = "SYNTHETIC PRIVATE PHONE SENTINEL";
export const SYNTHETIC_EMAIL_SENTINEL = "SYNTHETIC PRIVATE EMAIL SENTINEL";

const TIMESTAMP = "2026-08-29T00:00:00.000Z";
const RUN_ID = `run_${"a".repeat(32)}`;
const WORKFLOW_ID = "CountyIngest/pasco/synthetic-public-read";
const SCOPE_ID = `scope_${"b".repeat(32)}`;
const SELECTION_HASH = "c".repeat(64);

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function sqlPath(value: string): string {
  return value.replaceAll("'", "''");
}

function available(value: unknown, evidenceId: string) {
  return {
    availability: "available",
    value,
    class: "raw",
    evidenceRefs: [evidenceId],
  } as const;
}

function unavailable(evidenceId: string) {
  return {
    availability: "unavailable",
    value: null,
    class: "raw",
    reason: "not_provided_by_source",
    evidenceRefs: [evidenceId],
  } as const;
}

function property(index: number, fixture = false) {
  const suffix = fixture
    ? "e72ba795455c19d71ce4cb11f6177a5e"
    : String(index).padStart(32, "0");
  const propertyId = `property_${suffix}`;
  const evidenceId = `evidence_synthetic_${index}`;
  const coordinates =
    index === 2
      ? unavailable(evidenceId)
      : available(
          { latitude: 28.3, longitude: -82.4, crs: "EPSG:4326" },
          evidenceId,
        );
  return {
    entityType: "property",
    contractVersion: "1.0.0",
    propertyId,
    parcelId: `parcel_${String(index).padStart(32, "0")}`,
    county: "pasco",
    sourceSystem: "pasco_appraiser",
    folio: available(`SYNTH-${index}`, evidenceId),
    parcelIdentifier: available(`SYNTH-${index}`, evidenceId),
    situsAddress: available(`SYNTHETIC PROPERTY ${index}`, evidenceId),
    coordinates,
    yearBuilt: available(2000 + index, evidenceId),
    roofInstallationDate: unavailable(evidenceId),
    roofInstallationYear: unavailable(evidenceId),
    roofAgeSignal: {
      availability: "available",
      value: {
        ageYears: 25 - index,
        precision: "year",
        basis: "year_built_proxy",
        basisQuality: "proxy",
        asOf: TIMESTAMP,
      },
      class: "derived",
      evidenceRefs: [evidenceId],
      derivation: {
        rule: "year_difference_proxy",
        ruleVersion: "1.0.0",
        asOf: TIMESTAMP,
        inputs: ["property.yearBuilt"],
      },
    },
    ownership: available(
      [
        {
          ownerName1: SYNTHETIC_OWNER_SENTINEL,
          mailingAddress1: SYNTHETIC_MAILING_SENTINEL,
          phone: SYNTHETIC_PHONE_SENTINEL,
          email: SYNTHETIC_EMAIL_SENTINEL,
        },
      ],
      evidenceId,
    ),
    permits: [],
    evidence: [
      {
        evidenceId,
        sourceSystem: "pasco_appraiser",
        sourceName: "Synthetic appraiser source",
        sourceRecordKey: `SYNTH-${index}`,
        sourceUrl: "https://example.invalid/public-source",
        sourceArtifactUri: "ipfs://synthetic-source-artifact",
        sourceRecordHash: `sha256:${String(index).repeat(64).slice(0, 64)}`,
        observedAt: TIMESTAMP,
        retrievedAt: TIMESTAMP,
        loadedAt: TIMESTAMP,
        publishedCid: null,
      },
    ],
    freshness: {
      observedAt: TIMESTAMP,
      retrievedAt: TIMESTAMP,
      loadedAt: TIMESTAMP,
      publishedAt: null,
      computedAt: TIMESTAMP,
      sourceCadence: "synthetic",
    },
  };
}

async function parquetBytes(
  properties: Array<ReturnType<typeof property>>,
  propertyCids: ReadonlyMap<string, string>,
  options: {
    omitColumn?: string | undefined;
    wrongPropertyCid?: boolean | undefined;
  },
): Promise<Buffer> {
  const directory = path.join(
    tmpdir(),
    `prism-public-ipns-${process.pid}-${randomUUID()}`,
  );
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const ndjsonPath = path.join(directory, "rows.ndjson");
  const parquetPath = path.join(directory, "query.parquet");
  const rows = properties.map((value, index) => {
    const propertyHash = sha256(
      Buffer.from(publicationCanonicalJson(value), "utf8"),
    );
    return {
      property_id: value.propertyId,
      property_cid:
        options.wrongPropertyCid && index === 0
          ? propertyCids.get(properties[1]!.propertyId)
          : propertyCids.get(value.propertyId),
      request_identifier: `SYNTH-${index + 1}`,
      parcel_identifier: `SYNTH-${index + 1}`,
      source_system: "pasco_appraiser",
      county_name: "Pasco",
      state_code: "FL",
      address_street: `SYNTHETIC PROPERTY ${index + 1}`,
      address_city: "SYNTHETIC CITY",
      address_zip: "00000",
      latitude: index === 1 ? null : 28.3,
      longitude: index === 1 ? null : -82.4,
      lot_size_acre: null,
      lot_area_sqft: null,
      exterior_wall_material: null,
      roof_covering_material: null,
      property_type: null,
      property_usage_type: null,
      built_year: 2001 + index,
      livable_floor_area: null,
      total_area: null,
      assessed_value: null,
      market_value: null,
      land_value: null,
      avm_value: null,
      owner_name: null,
      owners_text: null,
      owner_count: null,
      owner_occupied: null,
      last_sale_date: null,
      last_sale_price: null,
      subdivision: null,
      has_permits: null,
      permit_count: null,
      has_sunbiz_tenant: null,
      has_bbb_contractor: null,
      hoa_flag: null,
      parcel_id: value.parcelId,
      county: "pasco",
      exact_folio: `SYNTH-${index + 1}`,
      site_address: `SYNTHETIC PROPERTY ${index + 1}`,
      site_city: "SYNTHETIC CITY",
      site_zip: "00000",
      property_use_code: null,
      property_use_description: null,
      acres: null,
      total_square_feet: null,
      heated_square_feet: null,
      year_built: 2001 + index,
      roof_cover: null,
      roof_structure: null,
      roof_installation_date: null,
      roof_installation_year: null,
      roof_age_years: 24 - index,
      roof_age_basis: "year_built_proxy",
      roof_age_basis_quality: "proxy",
      owner_name_1: null,
      owner_name_2: null,
      mailing_city: null,
      mailing_state: null,
      mailing_zip: null,
      ...unavailablePublicationFields(),
      property_document_sha256: propertyHash,
      source_record_hash: `sha256:${String(index + 1)
        .repeat(64)
        .slice(0, 64)}`,
      coverage_mode: "sample",
      coverage_scope_id: SCOPE_ID,
      source_snapshot_id: null,
      source_run_id: RUN_ID,
      selection_hash: SELECTION_HASH,
      observed_at: TIMESTAMP,
      loaded_at: TIMESTAMP,
      published_at: null,
    };
  });
  await writeFile(
    ndjsonPath,
    `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
  );
  const columns = Object.entries(queryTableColumns())
    .filter(([name]) => name !== options.omitColumn)
    .map(([name, type]) => `'${name}': '${type}'`)
    .join(", ");
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    await connection.run(`
      COPY (
        SELECT * FROM read_json(
          '${sqlPath(ndjsonPath)}',
          format = 'newline_delimited',
          columns = {${columns}}
        ) ORDER BY request_identifier, property_id
      ) TO '${sqlPath(parquetPath)}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
    `);
  } finally {
    connection.closeSync();
  }
  const bytes = await readFile(parquetPath);
  await rm(directory, { recursive: true, force: true });
  return bytes;
}

interface SyntheticPublicSet {
  config: PublicIpnsProviderConfig;
  objects: Map<string, Uint8Array>;
  propertyIds: string[];
  transport: MockPublicReadTransport;
}

async function storedObject(
  objects: Map<string, Uint8Array>,
  bytes: Uint8Array,
): Promise<{ byteSize: number; cid: string; sha256: string }> {
  const cid = await calculateIpfsCid(bytes);
  objects.set(cid, bytes);
  return { byteSize: bytes.byteLength, cid, sha256: sha256(bytes) };
}

function inventoryObject(options: {
  domain: PublicationArtifact["domain"];
  identity: { byteSize: number; cid: string; sha256: string };
  objectKey: string;
  role: PublicationArtifact["role"];
}): PublicationArtifact {
  return {
    byteSize: options.identity.byteSize,
    domain: options.domain,
    expectedCid: options.identity.cid,
    objectKey: options.objectKey,
    role: options.role,
    sha256: options.identity.sha256,
  };
}

export async function syntheticPublicSet(
  options: {
    fixtureProperty?: boolean;
    mcpHash?: string;
    omitParquetColumn?: string;
    wrongPropertyCid?: boolean;
  } = {},
): Promise<SyntheticPublicSet> {
  const objects = new Map<string, Uint8Array>();
  const properties = [property(1, options.fixtureProperty), property(2)];
  const graph = await buildPublicationGraph({
    completedAt: TIMESTAMP,
    exportedAt: TIMESTAMP,
    properties: properties.map((value, index) => ({
      parcelIdentifier: `SYNTH-${index + 1}`,
      propertyId: value.propertyId,
      value,
    })),
  });
  const inventory: PublicationArtifact[] = [];
  for (const value of graph.objects) {
    objects.set(value.cid, value.bytes);
    inventory.push(
      inventoryObject({
        domain: "open_data",
        identity: {
          byteSize: value.byteSize,
          cid: value.cid,
          sha256: value.sha256,
        },
        objectKey: value.key,
        role: value.role,
      }),
    );
  }
  const graphByKey = new Map(graph.objects.map((value) => [value.key, value]));
  const propertyEntries = properties.map((value, index) => {
    const object = graphByKey.get(`properties/${value.propertyId}.json`)!;
    return {
      bytes: object.byteSize,
      cid: object.cid,
      objectKey: object.key,
      parcelIdentifier: `SYNTH-${index + 1}`,
      propertyId: value.propertyId,
      sha256: object.sha256,
    };
  });
  const shardRecords = graph.shards.map((shard) => {
    const key = `shards/shard-${String(shard.shardIndex).padStart(4, "0")}.json`;
    const object = graphByKey.get(key)!;
    return {
      byteSize: object.byteSize,
      expectedCid: object.cid,
      objectKey: key,
      propertyCount: shard.count,
      sha256: object.sha256,
    };
  });
  const metadataValues = {
    coverage: {
      bbb: { availability: "unavailable", reason: "source_not_collected" },
      canonicalProperties: properties.length,
      coverageMode: "sample",
      contractors: {
        availability: "unavailable",
        reason: "source_unavailable",
      },
      coordinates: 1,
      county: "pasco",
      permits: { availability: "unavailable", reason: "source_unavailable" },
      runId: RUN_ID,
      scopeId: SCOPE_ID,
      selection: {
        algorithm: "synthetic-v1",
        seed: "synthetic-seed",
        selectedRecordSha256: SELECTION_HASH,
        selectionSize: properties.length,
      },
      snapshotId: null,
      scope: "synthetic sample",
      sunbiz: { availability: "unavailable", reason: "source_not_collected" },
      warning: "Unavailable does not mean zero.",
    },
    provenance: {
      county: "pasco",
      sources: [
        {
          artifactUri: "ipfs://synthetic-source-artifact",
          files: [{ path: "synthetic/source.bin", sha256: "d".repeat(64) }],
          sourceSystem: "pasco_appraiser",
        },
      ],
      sourceWatermark: {
        appraiserObservedDate: "2026-08-29",
        coverageMode: "sample",
        runId: RUN_ID,
        scopeId: SCOPE_ID,
        snapshotId: null,
        workflowId: WORKFLOW_ID,
      },
      version: "1.0.0",
    },
    runSummary: {
      county: "pasco",
      resultCounts: {
        acceptedProperties: properties.length,
        changedProperties: 0,
        coordinates: 1,
        duplicateProperties: 0,
        elapsedMs: 1_000,
        newProperties: properties.length,
        roofSignals: properties.length,
        selectionSize: properties.length,
        sourceCounts: Object.fromEntries(
          ["parcel", "building", "owners", "siteAddresses"].map((key) => [
            key,
            { source: 2, parsed: 2, accepted: 2, rejected: 0 },
          ]),
        ),
        unchangedProperties: 0,
      },
      runId: RUN_ID,
      workflowId: WORKFLOW_ID,
    },
  };
  const metadataIdentities: Record<
    string,
    { byteSize: number; cid: string; sha256: string }
  > = {};
  for (const [key, value] of Object.entries(metadataValues)) {
    metadataIdentities[key] = await storedObject(
      objects,
      Buffer.from(publicationCanonicalJson(value)),
    );
  }
  const manifest = {
    contractVersion: "1.0.0",
    county: "pasco",
    coverageMode: "sample",
    entries: propertyEntries,
    generatedAt: TIMESTAMP,
    propertyCount: properties.length,
    representation: "canonical-property-json-v1",
    scopeId: SCOPE_ID,
    selectionHash: SELECTION_HASH,
    rootCid: graph.rootCid,
    shards: shardRecords.map((value) => ({
      bytes: value.byteSize,
      expectedCid: value.expectedCid,
      objectKey: value.objectKey,
      propertyCount: value.propertyCount,
      sha256: value.sha256,
    })),
    sourceRunId: RUN_ID,
    sourceSnapshotId: null,
  };
  const manifestIdentity = await storedObject(
    objects,
    Buffer.from(publicationCanonicalJson(manifest)),
  );
  const queryBytes = await parquetBytes(properties, graph.propertyCids, {
    omitColumn: options.omitParquetColumn,
    wrongPropertyCid: options.wrongPropertyCid,
  });
  const queryIdentity = await storedObject(objects, queryBytes);
  inventory.push(
    inventoryObject({
      domain: "open_data",
      identity: manifestIdentity,
      objectKey: "manifest.json",
      role: "manifest",
    }),
  );
  for (const [key, objectKey] of [
    ["coverage", "coverage.json"],
    ["provenance", "provenance.json"],
    ["runSummary", "run-summary.json"],
  ] as const) {
    inventory.push(
      inventoryObject({
        domain: "open_data",
        identity: metadataIdentities[key]!,
        objectKey,
        role: "metadata",
      }),
    );
  }
  inventory.push(
    inventoryObject({
      domain: "query_table",
      identity: queryIdentity,
      objectKey: "query-tables/pasco/query-table.parquet",
      role: "query_table",
    }),
  );
  const binding = (identity: {
    byteSize: number;
    cid: string;
    sha256: string;
  }) => ({
    byteSize: identity.byteSize,
    expectedCid: identity.cid,
    objectKey: "unused",
    sha256: identity.sha256,
  });
  const plan = createPublicationPlan({
    approvable: false,
    artifacts: {
      coverage: {
        ...binding(metadataIdentities.coverage!),
        objectKey: "coverage.json",
      },
      manifest: { ...binding(manifestIdentity), objectKey: "manifest.json" },
      objectInventory: inventory,
      parquet: {
        ...binding(queryIdentity),
        objectKey: "query-tables/pasco/query-table.parquet",
        distinctPropertyIds: properties.length,
        nullPropertyIds: 0,
        rowCount: properties.length,
        schemaSha256: "e".repeat(64),
      },
      provenance: {
        ...binding(metadataIdentities.provenance!),
        objectKey: "provenance.json",
      },
      shards: shardRecords,
    },
    configuration: { credentialsAvailable: true, missing: [] },
    contracts: {
      canonical: {
        sha256:
          "59c6472c2cd6d18041cf72c779fb970a082b00bef09aea724b99687e84198306",
        version: "1.0.0",
      },
      mcp: {
        sha256:
          options.mcpHash ??
          "9fc112ef8a4e2120593c3dc20c90073b0eb96596817c96112f63fd258bb7c131",
        version: "1.2.0",
      },
    },
    counts: {
      activeProperties: properties.length,
      canonicalDocuments: properties.length,
      coordinateRows: 1,
      inactiveProperties: 0,
      queryTableDistinctPropertyIds: properties.length,
      queryTableNullPropertyIds: 0,
      queryTableRows: properties.length,
    },
    county: "pasco",
    coverage: {
      authoritativeHeadSnapshotId: null,
      authoritySourceSystem: "pasco_appraiser",
      completenessResult: "not_applicable",
      entityType: "property_existence",
      mode: "sample",
      predecessorChainSnapshotIds: [],
      runId: RUN_ID,
      scopeId: SCOPE_ID,
      selection: {
        algorithm: "synthetic-v1",
        seed: "synthetic-seed",
        selectedRecordSha256: SELECTION_HASH,
        selectionSize: properties.length,
      },
      sourceSnapshotId: null,
      sourceSnapshotManifestSha256: null,
      workflowId: WORKFLOW_ID,
    },
    executable: false,
    exportMode: "bounded",
    fixtureExclusion: {
      fixturePropertyIdCount: 1,
      matches: 0,
      passed: true,
    },
    freshness: { asOf: TIMESTAMP, loadedAt: TIMESTAMP, observedAt: TIMESTAMP },
    generatedAt: TIMESTAMP,
    graph: {
      cidProfile: { ...IPFS_CID_PROFILE },
      edges: graph.edges,
      openDataRoot: { expectedCid: graph.rootCid, objectKey: "index.json" },
      parquetProfile: {
        compression: "ZSTD",
        duckdbVersion: "@duckdb/node-api@1.5.5-r.4",
        rowGroupSize: 10_000,
        schemaSha256: "e".repeat(64),
      },
      propertyCidCount: properties.length,
      queryTableRoot: {
        expectedCid: queryIdentity.cid,
        objectKey: "query-tables/pasco/query-table.parquet",
      },
      traversalValidated: true,
    },
    limitations: [
      "Synthetic sample; not complete Pasco coverage.",
      "Permit and contractor sources are unavailable; null is not zero.",
    ],
    projection: {
      authoritativeBaseSnapshotId: null,
      materializationId: `materialization_${"f".repeat(32)}`,
      materializationSha256: "1".repeat(64),
      snapshotContentSha256: null,
    },
    remoteState: {
      openDataPublishedCid: null,
      openDataIpnsMutationPerformed: false,
      queryTablePublishedCid: null,
      queryTableIpnsMutationPerformed: false,
    },
    targets: {
      openData: {
        bucket: "synthetic-open-data",
        bucketConfirmed: true,
        ipnsLabel: "synthetic-open-data",
        ipnsNetworkKey: OPEN_IPNS,
      },
      queryTable: {
        bucket: "synthetic-query-table",
        bucketConfirmed: true,
        ipnsLabel: "synthetic-query-table",
        ipnsNetworkKey: QUERY_IPNS,
      },
    },
    temporalFactLimitation: "Synthetic current facts only.",
    version: "1.1.0",
  });
  const planBytes = Buffer.from(publicationCanonicalJson(plan));
  const planIdentity = await storedObject(objects, planBytes);
  const config: PublicIpnsProviderConfig = {
    environment: "test",
    expectedManifestCid: manifestIdentity.cid,
    expectedManifestSha256: manifestIdentity.sha256,
    expectedOpenDataRootCid: graph.rootCid,
    expectedPlanCid: planIdentity.cid,
    expectedPlanSha256: planIdentity.sha256,
    expectedQueryTableRootCid: queryIdentity.cid,
    limits: {
      maxCacheAgeSeconds: 300,
      maxJsonObjectBytes: 8 * 1024 * 1024,
      maxParquetBytes: 16 * 1024 * 1024,
      maxRedirects: 2,
      retries: 1,
      transportTimeoutMs: 1_000,
    },
    mode: "public-ipns",
    openDataIpns: OPEN_IPNS,
    queryTableIpns: QUERY_IPNS,
  };
  const transport = new MockPublicReadTransport(
    objects,
    new Map([
      [OPEN_IPNS, graph.rootCid],
      [QUERY_IPNS, queryIdentity.cid],
    ]),
  );
  return {
    config,
    objects,
    propertyIds: properties.map((value) =>
      value.propertyId.replace("property_", "prop_"),
    ),
    transport,
  };
}

export class MockPublicReadTransport implements PublicReadTransport {
  readonly reads: string[] = [];
  readonly resolutions = new Map<
    string,
    readonly IpnsResolutionObservation[]
  >();

  constructor(
    readonly objects: Map<string, Uint8Array>,
    identities: ReadonlyMap<string, string>,
  ) {
    for (const [identity, cid] of identities) {
      this.resolutions.set(identity, [
        {
          cacheAgeSeconds: 0,
          cid,
          observedAt: TIMESTAMP,
          resolver: "resolver-a",
          status: "resolved",
        },
        {
          cacheAgeSeconds: 0,
          cid,
          observedAt: TIMESTAMP,
          resolver: "resolver-b",
          status: "resolved",
        },
      ]);
    }
  }

  async readCid(cid: string, maximumBytes: number): Promise<Uint8Array> {
    this.reads.push(cid);
    const value = this.objects.get(cid);
    if (!value) throw new Error("synthetic object unavailable");
    if (value.byteLength > maximumBytes) throw new Error("synthetic bound");
    return value;
  }

  async resolveIpns(
    identity: string,
  ): Promise<readonly IpnsResolutionObservation[]> {
    return this.resolutions.get(identity) ?? [];
  }
}
