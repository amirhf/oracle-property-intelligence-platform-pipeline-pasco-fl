import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  mkdir,
  open,
  readFile,
  rename,
  rm,
  stat,
  statfs,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { createInterface } from "node:readline";

import { DuckDBInstance } from "@duckdb/node-api";
import type { Ajv2020 as Ajv2020Class, ErrorObject } from "ajv/dist/2020.js";
import type { FormatsPlugin } from "ajv-formats";
import postgres from "postgres";

import {
  OWNER_AUTHORITY_CLASS,
  PASCO_PARCEL_CSV_SHA256,
  PASCO_PARCEL_FOLIO_COUNT,
  PASCO_PARCEL_FOLIO_SET_SHA256,
  PASCO_PARCEL_LAST_MODIFIED,
  PASCO_PARCEL_MEMBERSHIP_CLAIM,
  PASCO_PARCEL_ZIP_SHA256,
  validateOwnerAuthorityRecord,
} from "../authoritative/authority.js";
import { acquirePascoProjectionHeadFence } from "../db/projection-head-fence.js";
import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { parcelId, sha256 } from "../lib/hash.js";
import {
  sourceObjectSchema,
  verifySourceObjectBindings,
  type SourceObject,
} from "../snapshot/model.js";
import {
  buildMaterializedCanonicalProperty,
  meaningfulSitusAddress,
  type MaterializedPublicationFact,
} from "./canonical-property.js";
import {
  buildPublicationQueryRow,
  contractFixturePropertyIds,
  queryTableColumns,
  resolveExportScope,
  validateElephantQueryTableCompatibility,
  validateParquet,
  type ExportOwnerRow,
  type ExportPropertyRow,
  type ResolvedExportScope,
} from "./dry-run.js";
import {
  PUBLICATION_GRAPH_VERSION,
  PUBLICATION_SHARD_SIZE,
  publicationCanonicalJson,
  type GraphEdge,
  type RootDocument,
  type ShardDocument,
  type ShardEntry,
} from "./graph.js";
import {
  calculateIpfsCid,
  calculateIpfsFileCid,
  IPFS_CID_PROFILE,
} from "./ipfs-cid.js";
import {
  createPublicationPlan,
  publicationConfigurationMissing,
  validatePublicationPlan,
  type PublicationArtifact,
  type PublicationPlan,
  type PublicationTarget,
} from "./plan.js";
import {
  AUTHORITATIVE_BATCH_SIZE,
  AUTHORITATIVE_MAX_RSS_BYTES,
  authoritativePublicationCardinality,
  preflightAuthoritativePublicationResources,
  type PublicationResourcePreflight,
} from "./resource-preflight.js";

const require = createRequire(import.meta.url);
const Ajv2020 = require("ajv/dist/2020.js") as typeof Ajv2020Class;
const addFormats = require("ajv-formats") as FormatsPlugin;

const CONTRACT_VERSION = "1.0.0";
const QUERY_TABLE_KEY = "query-tables/pasco/query-table.parquet";
const DUCKDB_VERSION = "@duckdb/node-api@1.5.5-r.4";

export const AUTHORITATIVE_PUBLICATION_BINDING = Object.freeze({
  authorityRecordId: "authority_2a6e9cb08d4c8fc12082aa30abb35cab",
  loaderEffectId: "load_47cad15c72f5ebb0d3be66adbd73e9f1",
  materializationId: "materialization_981835fc695107653fd830e12c2284db",
  materializationSha256:
    "ae295083f7efce4575e15bda381253f0dfe29ea4fe2c4e320256242bc80a513a",
  resultSha256:
    "81d47175f1be5800388517b8b4e12e5998d1ab0375ac648def53667dfa3a1746",
  runId: "run_4c74edc0e29eacf0cb4de4b45d57428c",
  scopeId: "scope_055c2b98f0dc74de092e53bacb1d64ce",
  snapshotId: "snapshot_23e94803bfee6453a047595e80f2fc43",
} as const);

const EXPECTED_COVERAGE = Object.freeze({
  activeProperties: 325_213,
  buildingFacts: 276_649,
  buildingProperties: 261_590,
  coordinates: 24_995,
  missingCoordinates: 300_218,
  ownershipFacts: 322_261,
  roofSignals: 261_590,
  siteAddressProperties: 282_612,
  siteAddressSourceRows: 361_347,
} as const);

interface CoreRow {
  parcel_identifier: string;
  payload: unknown;
  property_id: string;
  source_record_sha256: string;
  source_run_id: string;
  source_snapshot_id: string;
  version_id: string;
}

interface FactRow {
  evidence_refs: unknown;
  fact_type: string;
  fact_version_id: string;
  natural_key: string;
  payload: unknown;
  property_id: string;
  source_record_sha256: string;
  source_run_id: string;
  source_snapshot_id: string;
}

interface ManifestEntry {
  bytes: number;
  cid: string;
  objectKey: string;
  parcelIdentifier: string;
  propertyId: string;
  sha256: string;
}

interface ShardRecord {
  byteSize: number;
  expectedCid: string;
  objectKey: string;
  propertyCount: number;
  sha256: string;
}

interface AuthorityBinding {
  authorityClass: typeof OWNER_AUTHORITY_CLASS;
  authorityRecordId: string;
  completenessEvidenceSha256: string;
  decisionSha256: string;
  payload: Record<string, unknown>;
  sourceSnapshotManifestSha256: string;
}

interface StableHeadBinding {
  authority: AuthorityBinding;
  headRevision: number;
  predecessorChainSnapshotIds: string[];
  scope: ResolvedExportScope;
}

export interface AuthoritativePublicationSummary {
  adoptedExisting: boolean;
  authorityRecordId: string;
  coverage: {
    activeProperties: number;
    buildingFacts: number;
    buildingProperties: number;
    coordinateProperties: number;
    missingCoordinateProperties: number;
    ownershipFacts: number;
    ownershipProperties: number;
    permitContractorRelationships: 0;
    permits: 0;
    contractors: 0;
    roofSignalProperties: number;
    siteAddressProperties: number;
    siteAddressSourceRows: number;
  };
  durablePublicationStateChanged: false;
  elapsedMs: number;
  inventoryBytes: number;
  inventoryObjectCount: number;
  manifestCid: string;
  manifestSha256: string;
  materializationId: string;
  openDataRootCid: string;
  outputRelativePath: string;
  parquetBytes: number;
  parquetCid: string;
  parquetSha256: string;
  peakRssBytes: number;
  planArtifactBytes: number;
  planArtifactCid: string;
  planArtifactSha256: string;
  planId: string;
  planSha256: string;
  preflight: PublicationResourcePreflight;
  propertyBytes: number;
  propertyCount: number;
  runId: string;
  shardCount: number;
  snapshotId: string;
  sourceObjectCount: number;
}

interface BuildOptions {
  dataDir: string;
  databaseUrl: string;
  onProgress?: (value: {
    elapsedMs: number;
    peakRssBytes: number;
    properties: number;
  }) => void;
  preflightOnly?: boolean;
}

function iso(value: Date | string): string {
  return new Date(value).toISOString();
}

function compare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function sqlPath(value: string): string {
  return value.replaceAll("'", "''");
}

async function fileSha256(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

async function fileBinding(filePath: string) {
  const fileStat = await stat(filePath);
  return {
    byteSize: fileStat.size,
    expectedCid: await calculateIpfsFileCid(filePath),
    sha256: await fileSha256(filePath),
  };
}

function incompleteOwnerTargets(): {
  credentialsAvailable: false;
  openData: PublicationTarget;
  queryTable: PublicationTarget;
} {
  return {
    credentialsAvailable: false,
    openData: {
      bucket: null,
      bucketConfirmed: false,
      ipnsLabel: "unconfigured-owner-open-data-pasco",
      ipnsNetworkKey: null,
    },
    queryTable: {
      bucket: null,
      bucketConfirmed: false,
      ipnsLabel: "unconfigured-owner-query-table-pasco",
      ipnsNetworkKey: null,
    },
  };
}

async function loadStableHeadBinding(
  sql: postgres.Sql,
  dataDir: string,
): Promise<StableHeadBinding> {
  const scope = await resolveExportScope(sql, {
    dataDir,
    databaseUrl: "unused-by-resolver",
    exportMode: "authoritative",
    runId: AUTHORITATIVE_PUBLICATION_BINDING.runId,
  });
  if (
    scope.run.run_id !== AUTHORITATIVE_PUBLICATION_BINDING.runId ||
    scope.run.snapshot_id !== AUTHORITATIVE_PUBLICATION_BINDING.snapshotId ||
    scope.scopeId !== AUTHORITATIVE_PUBLICATION_BINDING.scopeId ||
    scope.materializationId !==
      AUTHORITATIVE_PUBLICATION_BINDING.materializationId ||
    scope.materializationSha256 !==
      AUTHORITATIVE_PUBLICATION_BINDING.materializationSha256 ||
    scope.activeProperties !== EXPECTED_COVERAGE.activeProperties ||
    scope.inactiveProperties !== 0 ||
    scope.authoritativeHeadSnapshotId !==
      AUTHORITATIVE_PUBLICATION_BINDING.snapshotId ||
    scope.authoritativeBaseSnapshotId !==
      AUTHORITATIVE_PUBLICATION_BINDING.snapshotId ||
    scope.coverage?.mode !== "authoritative_complete" ||
    scope.coverage.completeness.result !== "passed" ||
    scope.selectedRecordSha256 !== PASCO_PARCEL_FOLIO_SET_SHA256
  ) {
    throw new Error("Bound authoritative publication identity is inconsistent");
  }
  const sourceObjects = sourceObjectSchema
    .array()
    .parse(scope.snapshot?.source_objects) as SourceObject[];
  await verifySourceObjectBindings(dataDir, sourceObjects);

  const rows = await sql<
    {
      authority_class: string;
      authority_payload: Record<string, unknown>;
      authority_record_id: string;
      completeness_evidence_sha256: string;
      decision_sha256: string;
      head_revision: number;
      result_sha256: string;
      source_snapshot_manifest_sha256: string;
    }[]
  >`
    SELECT authority.authority_record_id, authority.authority_class,
           authority.decision_sha256,
           authority.completeness_evidence_sha256,
           authority.source_snapshot_manifest_sha256,
           authority.authority_payload, head.revision AS head_revision,
           effect.result_sha256
    FROM oracle_source_authority_records authority
    JOIN oracle_projection_heads head
      ON head.scope_id = authority.scope_id
     AND head.current_snapshot_id = authority.source_snapshot_id
    JOIN oracle_loader_effects effect
      ON effect.idempotency_key = ${AUTHORITATIVE_PUBLICATION_BINDING.loaderEffectId}
     AND effect.run_id = authority.source_run_id
     AND effect.snapshot_id = authority.source_snapshot_id
     AND effect.status = 'completed'
    WHERE authority.authority_record_id = ${AUTHORITATIVE_PUBLICATION_BINDING.authorityRecordId}
      AND authority.source_run_id = ${AUTHORITATIVE_PUBLICATION_BINDING.runId}
      AND authority.source_snapshot_id = ${AUTHORITATIVE_PUBLICATION_BINDING.snapshotId}
      AND authority.scope_id = ${AUTHORITATIVE_PUBLICATION_BINDING.scopeId}
  `;
  const row = rows[0];
  if (
    rows.length !== 1 ||
    !row ||
    row.authority_class !== OWNER_AUTHORITY_CLASS ||
    row.result_sha256 !== AUTHORITATIVE_PUBLICATION_BINDING.resultSha256 ||
    row.source_snapshot_manifest_sha256 !== scope.snapshot?.manifest_sha256
  ) {
    throw new Error(
      "Owner-assumed authority or Loader replay binding is invalid",
    );
  }
  const authority = validateOwnerAuthorityRecord({
    authorityClass: row.authority_class,
    authorityRecordId: row.authority_record_id,
    completenessEvidenceSha256: row.completeness_evidence_sha256,
    decisionSha256: row.decision_sha256,
    payload: row.authority_payload,
  });
  if (
    authority.authorityRecordId !==
    AUTHORITATIVE_PUBLICATION_BINDING.authorityRecordId
  ) {
    throw new Error("Owner-assumed authority record changed");
  }
  return {
    authority: {
      ...authority,
      payload: authority.payload as Record<string, unknown>,
      sourceSnapshotManifestSha256: row.source_snapshot_manifest_sha256,
    },
    headRevision: row.head_revision,
    predecessorChainSnapshotIds: scope.predecessorChainSnapshotIds,
    scope,
  };
}

async function resourcePreflight(
  sql: postgres.Sql,
  dataDir: string,
  binding: StableHeadBinding,
): Promise<PublicationResourcePreflight> {
  const counts = await sql<
    {
      fact_count: string;
      fact_payload_bytes: string;
      property_count: string;
      property_payload_bytes: string;
    }[]
  >`
    SELECT
      (SELECT count(*)::text
         FROM oracle_projection_materialized_properties membership
        WHERE membership.materialization_id = ${binding.scope.materializationId}
          AND membership.is_active) AS property_count,
      (SELECT coalesce(sum(pg_column_size(version.payload)), 0)::text
         FROM oracle_projection_materialized_properties membership
         JOIN oracle_property_versions version
           ON version.version_id = membership.property_version_id
        WHERE membership.materialization_id = ${binding.scope.materializationId}
          AND membership.is_active) AS property_payload_bytes,
      (SELECT count(*)::text
         FROM oracle_projection_materialized_facts membership
        WHERE membership.materialization_id = ${binding.scope.materializationId}) AS fact_count,
      (SELECT coalesce(sum(pg_column_size(version.payload) +
                           pg_column_size(version.evidence_refs)), 0)::text
         FROM oracle_projection_materialized_facts membership
         JOIN oracle_child_fact_versions version
           ON version.version_id = membership.fact_version_id
        WHERE membership.materialization_id = ${binding.scope.materializationId}) AS fact_payload_bytes
  `;
  const row = counts[0];
  if (!row) throw new Error("Publication resource counts are unavailable");
  const filesystem = await statfs(dataDir);
  return preflightAuthoritativePublicationResources({
    availableBytes: Math.floor(filesystem.bavail * filesystem.bsize),
    availableFiles: Math.floor(filesystem.ffree),
    factCount: Number(row.fact_count),
    propertyCount: Number(row.property_count),
    sourcePayloadBytes:
      Number(row.property_payload_bytes) + Number(row.fact_payload_bytes),
  });
}

function sourceUrl(
  objects: readonly SourceObject[],
  sourceSystem: string,
  basename: string,
): string | null {
  return (
    objects.find(
      (object) =>
        object.sourceSystem === sourceSystem &&
        object.stage === "downloaded_source" &&
        object.relativePath.endsWith(`/${basename}`),
    )?.sourceIdentifier ?? null
  );
}

function parcelObservedAt(objects: readonly SourceObject[]): string {
  const parcel = objects.find(
    (object) =>
      object.sourceSystem === "pasco_appraiser" &&
      object.stage === "downloaded_source" &&
      object.relativePath.endsWith("/parcel.zip"),
  );
  const value = parcel?.observedAt ?? parcel?.lastModified;
  if (!value || !Number.isFinite(Date.parse(value))) {
    throw new Error("Parcel source observation timestamp is unavailable");
  }
  const observedAt = new Date(value).toISOString();
  if (observedAt !== PASCO_PARCEL_LAST_MODIFIED) {
    throw new Error("Parcel source observation timestamp changed");
  }
  return observedAt;
}

function publicationFacts(
  rows: readonly FactRow[],
): MaterializedPublicationFact[] {
  return rows.map((fact) => ({
    evidenceRefs: fact.evidence_refs,
    factType: fact.fact_type,
    naturalKey: fact.natural_key,
    payload: fact.payload,
    sourceRecordHash: fact.source_record_sha256,
    sourceRunId: fact.source_run_id,
    sourceSnapshotId: fact.source_snapshot_id,
    versionId: fact.fact_version_id,
  }));
}

function ownerRows(
  propertyId: string,
  rows: readonly FactRow[],
): ExportOwnerRow[] {
  return rows
    .filter((fact) => fact.fact_type === "ownership")
    .map((fact) => {
      const owner = fact.payload as {
        mailingAddress1?: string | null;
        mailingAddress2?: string | null;
        mailingCity?: string | null;
        mailingCountry?: string | null;
        mailingState?: string | null;
        mailingZip?: string | null;
        ownerName1?: string | null;
        ownerName2?: string | null;
      };
      return {
        mailing_address_1: owner.mailingAddress1 ?? null,
        mailing_address_2: owner.mailingAddress2 ?? null,
        mailing_city: owner.mailingCity ?? null,
        mailing_country: owner.mailingCountry ?? null,
        mailing_state: owner.mailingState ?? null,
        mailing_zip: owner.mailingZip ?? null,
        owner_name_1: owner.ownerName1 ?? null,
        owner_name_2: owner.ownerName2 ?? null,
        property_id: propertyId,
      };
    });
}

function exportProperty(
  core: CoreRow,
  facts: readonly FactRow[],
): ExportPropertyRow {
  const payload = core.payload as {
    acres?: number | null;
    heatedSquareFeet?: number | null;
    parcel?: {
      countyAssessed?: number | null;
      justValue?: number | null;
      propertyUseCode?: string | null;
      propertyUseDescription?: string | null;
      totalSquareFeet?: number | null;
    };
    siteAddress?: {
      city?: string | null;
      siteAddress?: string | null;
      zipCode?: string | null;
    } | null;
    totalSquareFeet?: number | null;
    yearBuilt?: number | null;
  };
  const coordinate = facts.find((fact) => fact.fact_type === "coordinate");
  const roof = facts.find((fact) => fact.fact_type === "roof_signal");
  const building = facts.find((fact) => fact.fact_type === "building");
  const coordinatePayload = coordinate?.payload as
    | {
        latitude?: number;
        longitude?: number;
        sourceLastUpdate?: string | null;
      }
    | undefined;
  const roofPayload = roof?.payload as
    { ageYears?: number; basis?: string; basisQuality?: string } | undefined;
  const buildingPayload = building?.payload as
    | {
        actualYearBuilt?: number | null;
        roofCover?: string | null;
        roofStructure?: string | null;
      }
    | undefined;
  return {
    acres: payload.acres ?? null,
    actual_year_built: buildingPayload?.actualYearBuilt ?? null,
    assessed_value: payload.parcel?.countyAssessed ?? null,
    coordinate_hash: coordinate?.source_record_sha256 ?? null,
    coordinate_source_last_update: coordinatePayload?.sourceLastUpdate ?? null,
    exact_folio: core.parcel_identifier,
    heated_square_feet: payload.heatedSquareFeet ?? null,
    latitude: coordinatePayload?.latitude ?? null,
    longitude: coordinatePayload?.longitude ?? null,
    market_value: payload.parcel?.justValue ?? null,
    parcel_id: parcelId(core.parcel_identifier),
    property_id: core.property_id,
    property_use_code: payload.parcel?.propertyUseCode ?? null,
    property_use_description: payload.parcel?.propertyUseDescription ?? null,
    roof_age_years: roofPayload?.ageYears ?? null,
    roof_basis: roofPayload?.basis ?? null,
    roof_basis_quality: roofPayload?.basisQuality ?? null,
    roof_cover: buildingPayload?.roofCover ?? null,
    roof_structure: buildingPayload?.roofStructure ?? null,
    site_address: payload.siteAddress?.siteAddress ?? null,
    site_city: payload.siteAddress?.city ?? null,
    site_zip: payload.siteAddress?.zipCode ?? null,
    source_record_hash: core.source_record_sha256,
    total_square_feet:
      payload.totalSquareFeet ?? payload.parcel?.totalSquareFeet ?? null,
    year_built: payload.yearBuilt ?? null,
  };
}

async function writeSortedParquet(
  parquetPath: string,
  ndjsonPath: string,
  spillDirectory: string,
): Promise<void> {
  const columns = Object.entries(queryTableColumns())
    .map(([name, type]) => `'${name}': '${type}'`)
    .join(", ");
  await mkdir(spillDirectory, { recursive: true });
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    await connection.run("SET threads = 1");
    await connection.run("SET preserve_insertion_order = true");
    await connection.run("SET memory_limit = '2048MB'");
    await connection.run(`SET temp_directory = '${sqlPath(spillDirectory)}'`);
    await connection.run(`
      COPY (
        SELECT * FROM read_json(
          '${sqlPath(ndjsonPath)}',
          format = 'newline_delimited',
          columns = {${columns}}
        )
      ) TO '${sqlPath(parquetPath)}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
    `);
  } finally {
    connection.closeSync();
  }
}

async function writeCanonicalFile(
  filePath: string,
  value: unknown,
): Promise<{ byteSize: number; expectedCid: string; sha256: string }> {
  const bytes = Buffer.from(`${canonicalJson(value)}\n`, "utf8");
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, bytes, { flag: "wx" });
  return {
    byteSize: bytes.byteLength,
    expectedCid: await calculateIpfsCid(bytes),
    sha256: sha256(bytes),
  };
}

async function buildManifest(options: {
  binding: StableHeadBinding;
  entriesPath: string;
  loadedAt: string;
  manifestPath: string;
  propertyCount: number;
  rootCid: string;
  shards: readonly ShardRecord[];
}): Promise<void> {
  const file = await open(options.manifestPath, "wx");
  try {
    const prefix = {
      contractVersion: CONTRACT_VERSION,
      county: "pasco",
      coverageMode: "authoritative_complete",
    };
    const prefixJson = JSON.stringify(prefix).slice(0, -1);
    await file.write(`${prefixJson},"entries":[`);
    const lines = createInterface({
      crlfDelay: Infinity,
      input: createReadStream(options.entriesPath, "utf8"),
    });
    let first = true;
    for await (const line of lines) {
      if (line.length === 0) continue;
      await file.write(`${first ? "" : ","}${line}`);
      first = false;
    }
    const suffix = {
      generatedAt: options.loadedAt,
      propertyCount: options.propertyCount,
      representation: "canonical-property-json-v1",
      rootCid: options.rootCid,
      scopeId: options.binding.scope.scopeId,
      selectionHash: options.binding.scope.selectedRecordSha256,
      shards: options.shards.map((shard) => ({
        bytes: shard.byteSize,
        expectedCid: shard.expectedCid,
        objectKey: shard.objectKey,
        propertyCount: shard.propertyCount,
        sha256: shard.sha256,
      })),
      sourceRunId: options.binding.scope.run.run_id,
      sourceSnapshotId: options.binding.scope.run.snapshot_id,
    };
    await file.write(`],${JSON.stringify(suffix).slice(1)}\n`);
  } finally {
    await file.close();
  }
}

async function assertStableHead(
  transaction: postgres.TransactionSql,
  binding: StableHeadBinding,
): Promise<void> {
  const rows: Array<{
    active_count: number;
    authority_record_id: string;
    authoritative_base_snapshot_id: string;
    completeness_evidence_sha256: string;
    current_snapshot_id: string;
    decision_sha256: string;
    manifest_sha256: string;
    materialization_id: string;
    materialization_sha256: string;
    revision: number;
    scope_id: string;
    snapshot_content_sha256: string;
  }> = await transaction`
    SELECT head.scope_id, head.current_snapshot_id,
           head.authoritative_base_snapshot_id, head.revision,
           materialization.materialization_id,
           materialization.materialization_sha256,
           materialization.active_count,
           snapshot.content_sha256 AS snapshot_content_sha256,
           source.manifest_sha256, authority.authority_record_id,
           authority.decision_sha256,
           authority.completeness_evidence_sha256
    FROM oracle_projection_heads head
    JOIN oracle_projection_snapshots snapshot
      ON snapshot.snapshot_id = head.current_snapshot_id
     AND snapshot.sealed
    JOIN oracle_projection_materializations materialization
      ON materialization.snapshot_id = snapshot.snapshot_id
     AND materialization.sealed
    JOIN oracle_source_snapshots source
      ON source.snapshot_id = snapshot.snapshot_id
    JOIN oracle_source_authority_records authority
      ON authority.source_snapshot_id = snapshot.snapshot_id
     AND authority.source_run_id = snapshot.run_id
    WHERE head.scope_id = ${binding.scope.scopeId}
      AND head.current_snapshot_id = ${binding.scope.run.snapshot_id}
      AND head.authoritative_base_snapshot_id = ${binding.scope.authoritativeBaseSnapshotId}
      AND head.revision = ${binding.headRevision}
      AND materialization.materialization_id = ${binding.scope.materializationId}
      AND materialization.materialization_sha256 = ${binding.scope.materializationSha256}
      AND materialization.active_count = ${binding.scope.activeProperties}
      AND snapshot.content_sha256 = ${binding.scope.snapshotContentSha256}
      AND snapshot.coverage_mode = 'authoritative_complete'
      AND snapshot.scope_id = ${binding.scope.scopeId}
      AND source.manifest_sha256 = ${binding.scope.snapshot!.manifest_sha256}
      AND authority.authority_record_id = ${binding.authority.authorityRecordId}
      AND authority.decision_sha256 = ${binding.authority.decisionSha256}
      AND authority.completeness_evidence_sha256 = ${binding.authority.completenessEvidenceSha256}
  `;
  const row = rows[0];
  if (
    rows.length !== 1 ||
    !row ||
    row.scope_id !== binding.scope.scopeId ||
    row.current_snapshot_id !== binding.scope.run.snapshot_id ||
    row.authoritative_base_snapshot_id !==
      binding.scope.authoritativeBaseSnapshotId ||
    row.revision !== binding.headRevision ||
    row.materialization_id !== binding.scope.materializationId ||
    row.materialization_sha256 !== binding.scope.materializationSha256 ||
    row.active_count !== binding.scope.activeProperties ||
    row.snapshot_content_sha256 !== binding.scope.snapshotContentSha256 ||
    row.manifest_sha256 !== binding.scope.snapshot?.manifest_sha256 ||
    row.authority_record_id !== binding.authority.authorityRecordId ||
    row.decision_sha256 !== binding.authority.decisionSha256 ||
    row.completeness_evidence_sha256 !==
      binding.authority.completenessEvidenceSha256
  ) {
    throw new Error(
      "Authoritative projection head advanced during publication build",
    );
  }
}

function artifactBinding(
  inventory: readonly PublicationArtifact[],
  domain: PublicationArtifact["domain"],
  objectKey: string,
) {
  const artifact = inventory.find(
    (candidate) =>
      candidate.domain === domain && candidate.objectKey === objectKey,
  );
  if (!artifact) {
    throw new Error(`Publication inventory is missing ${domain}:${objectKey}`);
  }
  return {
    byteSize: artifact.byteSize,
    expectedCid: artifact.expectedCid,
    objectKey: artifact.objectKey,
    sha256: artifact.sha256,
  };
}

async function verifyArtifactInventory(
  outputRoot: string,
  plan: PublicationPlan,
): Promise<void> {
  for (const artifact of plan.artifacts.objectInventory) {
    const artifactRoot =
      artifact.domain === "open_data"
        ? path.join(outputRoot, "open-data")
        : path.join(outputRoot, "query");
    const artifactPath = path.resolve(artifactRoot, artifact.objectKey);
    if (!artifactPath.startsWith(`${artifactRoot}${path.sep}`)) {
      throw new Error("Publication inventory path escaped its domain root");
    }
    const binding = await fileBinding(artifactPath);
    if (
      binding.byteSize !== artifact.byteSize ||
      binding.sha256 !== artifact.sha256 ||
      binding.expectedCid !== artifact.expectedCid
    ) {
      throw new Error(
        `Immutable publication artifact verification failed (${artifact.domain}:${artifact.objectKey})`,
      );
    }
  }
}

async function promoteOrAdopt(options: {
  binding: StableHeadBinding;
  contender: string;
  finalRoot: string;
  plan: PublicationPlan;
  sql: postgres.Sql;
}): Promise<boolean> {
  let adoptedExisting = false;
  await options.sql.begin(async (transaction) => {
    await transaction`SET LOCAL lock_timeout = '30s'`;
    await acquirePascoProjectionHeadFence(transaction);
    await assertStableHead(transaction, options.binding);
    try {
      await rename(options.contender, options.finalRoot);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
      adoptedExisting = true;
    }
  });
  if (adoptedExisting) {
    const winnerPlanBody = await readFile(
      path.join(options.finalRoot, "publication-plan.json"),
      "utf8",
    );
    const winner = validatePublicationPlan(JSON.parse(winnerPlanBody));
    if (
      winner.planId !== options.plan.planId ||
      winner.planSha256 !== options.plan.planSha256 ||
      winnerPlanBody !== `${canonicalJson(options.plan)}\n`
    ) {
      throw new Error("Existing authoritative publication plan differs");
    }
    await verifyArtifactInventory(options.finalRoot, options.plan);
    await rm(options.contender, { force: true, recursive: true });
    await options.sql.begin(async (transaction) => {
      await transaction`SET LOCAL lock_timeout = '30s'`;
      await acquirePascoProjectionHeadFence(transaction);
      await assertStableHead(transaction, options.binding);
    });
  }
  return adoptedExisting;
}

export async function preflightAuthoritativeLocalPublication(
  options: Pick<BuildOptions, "dataDir" | "databaseUrl">,
): Promise<{
  authorityRecordId: string;
  materializationId: string;
  preflight: PublicationResourcePreflight;
  runId: string;
  snapshotId: string;
}> {
  const sql = postgres(options.databaseUrl, { max: 1 });
  try {
    const binding = await loadStableHeadBinding(sql, options.dataDir);
    const preflight = await resourcePreflight(sql, options.dataDir, binding);
    return {
      authorityRecordId: binding.authority.authorityRecordId,
      materializationId: binding.scope.materializationId!,
      preflight,
      runId: binding.scope.run.run_id,
      snapshotId: binding.scope.run.snapshot_id!,
    };
  } finally {
    await sql.end();
  }
}

export async function buildAuthoritativeLocalPublication(
  options: BuildOptions,
): Promise<AuthoritativePublicationSummary> {
  const startedAt = performance.now();
  const publishBase = path.resolve(
    options.dataDir,
    "artifacts",
    "publish",
    "pasco",
    "authoritative-local",
  );
  const contender = path.join(
    publishBase,
    `.build-${process.pid}-${randomUUID()}`,
  );
  if (!contender.startsWith(`${publishBase}${path.sep}`)) {
    throw new Error("Authoritative publication contender escaped DATA_DIR");
  }
  const sql = postgres(options.databaseUrl, { max: 2 });
  let promoted = false;
  try {
    const binding = await loadStableHeadBinding(sql, options.dataDir);
    const preflight = await resourcePreflight(sql, options.dataDir, binding);
    if (options.preflightOnly) {
      throw new Error(
        "Use preflightAuthoritativeLocalPublication for preflight-only execution",
      );
    }
    await mkdir(path.join(contender, "open-data", "properties"), {
      recursive: true,
    });
    await mkdir(path.join(contender, "open-data", "shards"), {
      recursive: true,
    });
    const queryDir = path.join(contender, "query", "query-tables", "pasco");
    await mkdir(queryDir, { recursive: true });

    const scope = binding.scope;
    const sourceObjects = sourceObjectSchema
      .array()
      .parse(scope.snapshot?.source_objects) as SourceObject[];
    const observedAt = parcelObservedAt(sourceObjects);
    const asOf = iso(scope.run.as_of);
    const loadedAt = iso(scope.run.completed_at);
    const canonicalSchema = JSON.parse(
      await readFile(
        path.resolve("contracts/canonical-v1.schema.json"),
        "utf8",
      ),
    ) as { $id: string };
    const ajv = new Ajv2020({
      allErrors: true,
      allowUnionTypes: true,
      strict: false,
      validateFormats: true,
    });
    addFormats(ajv);
    ajv.addSchema(canonicalSchema);
    const validateCanonical = ajv.compile({
      $ref: `${canonicalSchema.$id}#/$defs/CanonicalProperty`,
    });
    const fixtureIds = await contractFixturePropertyIds();
    const allowedSnapshotIds = new Set(scope.predecessorChainSnapshotIds);
    const appraiserParcelUrl = sourceUrl(
      sourceObjects,
      "pasco_appraiser",
      "parcel.zip",
    );
    const appraiserBuildingUrl = sourceUrl(
      sourceObjects,
      "pasco_appraiser",
      "building.zip",
    );
    const appraiserOwnersUrl = sourceUrl(
      sourceObjects,
      "pasco_appraiser",
      "owners.zip",
    );
    const appraiserSiteAddressUrl = sourceUrl(
      sourceObjects,
      "pasco_appraiser",
      "site_addresses.zip",
    );
    if (
      !appraiserParcelUrl ||
      !appraiserBuildingUrl ||
      !appraiserOwnersUrl ||
      !appraiserSiteAddressUrl
    ) {
      throw new Error(
        "Authoritative appraiser source identities are incomplete",
      );
    }

    const inventory: PublicationArtifact[] = [];
    const edges: GraphEdge[] = [];
    const propertyCids = new Map<string, string>();
    const shardEntries: ShardEntry[] = [];
    const rootShards: RootDocument["shards"] = [];
    const shardRecords: ShardRecord[] = [];
    const entriesPath = path.join(contender, ".manifest-entries.ndjson");
    const queryNdjsonPath = path.join(queryDir, ".query-table.ndjson");
    const entriesFile = await open(entriesPath, "wx");
    const queryFile = await open(queryNdjsonPath, "wx");
    let propertyCount = 0;
    let propertyBytes = 0;
    let peakRssBytes = process.memoryUsage().rss;
    let lastParcel = "";
    let lastPropertyId = "";
    let buildingFacts = 0;
    let buildingProperties = 0;
    let ownershipFacts = 0;
    let ownershipProperties = 0;
    let coordinateProperties = 0;
    let roofSignalProperties = 0;
    let siteAddressProperties = 0;

    const flushShard = async (): Promise<void> => {
      if (shardEntries.length === 0) return;
      const first = shardEntries[0];
      const last = shardEntries.at(-1);
      if (!first || !last) throw new Error("Empty publication shard rejected");
      const shardIndex = rootShards.length;
      const shard: ShardDocument = {
        count: shardEntries.length,
        entries: [...shardEntries],
        fromParcel: first.parcelIdentifier,
        schemaVersion: PUBLICATION_GRAPH_VERSION,
        shardIndex,
        toParcel: last.parcelIdentifier,
      };
      const objectKey = `shards/shard-${String(shardIndex).padStart(4, "0")}.json`;
      const bytes = Buffer.from(publicationCanonicalJson(shard), "utf8");
      const expectedCid = await calculateIpfsCid(bytes);
      const artifactSha256 = sha256(bytes);
      await writeFile(path.join(contender, "open-data", objectKey), bytes, {
        flag: "wx",
      });
      inventory.push({
        byteSize: bytes.byteLength,
        domain: "open_data",
        expectedCid,
        objectKey,
        role: "shard",
        sha256: artifactSha256,
      });
      shardRecords.push({
        byteSize: bytes.byteLength,
        expectedCid,
        objectKey,
        propertyCount: shard.count,
        sha256: artifactSha256,
      });
      rootShards.push({
        count: shard.count,
        fromParcel: shard.fromParcel,
        shardCid: expectedCid,
        shardIndex,
        toParcel: shard.toParcel,
      });
      shard.entries.forEach((entry, index) => {
        edges.push({
          childCid: entry.cid,
          childKey: `properties/${entry.propertyId}.json`,
          jsonPointer: `/entries/${index}/cid`,
          parentKey: objectKey,
        });
      });
      shardEntries.length = 0;
    };

    try {
      while (true) {
        const cores = await sql<CoreRow[]>`
          SELECT membership.property_id, version.parcel_identifier,
                 version.payload, version.source_record_sha256,
                 version.source_run_id, version.source_snapshot_id,
                 version.version_id
          FROM oracle_projection_materialized_properties membership
          JOIN oracle_property_versions version
            ON version.version_id = membership.property_version_id
          WHERE membership.materialization_id = ${scope.materializationId}
            AND membership.is_active
            AND (
              version.parcel_identifier > ${lastParcel}
              OR (
                version.parcel_identifier = ${lastParcel}
                AND membership.property_id > ${lastPropertyId}
              )
            )
          ORDER BY version.parcel_identifier, membership.property_id
          LIMIT ${AUTHORITATIVE_BATCH_SIZE}
        `;
        if (cores.length === 0) break;
        const propertyIds = cores.map((core) => core.property_id);
        const facts = await sql<FactRow[]>`
          SELECT membership.property_id, membership.fact_type,
                 membership.natural_key, version.payload,
                 version.source_record_sha256, version.source_run_id,
                 version.source_snapshot_id, version.evidence_refs,
                 version.version_id AS fact_version_id
          FROM oracle_projection_materialized_facts membership
          JOIN oracle_child_fact_versions version
            ON version.version_id = membership.fact_version_id
          WHERE membership.materialization_id = ${scope.materializationId}
            AND membership.property_id = ANY(${propertyIds})
          ORDER BY membership.property_id, membership.fact_type,
                   membership.natural_key, version.version_id
        `;
        const factsByProperty = new Map<string, FactRow[]>();
        for (const fact of facts) {
          const current = factsByProperty.get(fact.property_id) ?? [];
          current.push(fact);
          factsByProperty.set(fact.property_id, current);
        }
        const manifestLines: string[] = [];
        const queryLines: string[] = [];
        for (const core of cores) {
          if (
            core.parcel_identifier < lastParcel ||
            (core.parcel_identifier === lastParcel &&
              core.property_id <= lastPropertyId)
          ) {
            throw new Error("Authoritative publication ordering regressed");
          }
          if (fixtureIds.has(core.property_id)) {
            throw new Error(
              "Frozen fixture property injection blocks publication",
            );
          }
          if (propertyCids.has(core.property_id)) {
            throw new Error("Duplicate property identity blocks publication");
          }
          if (propertyCount > 0 && core.parcel_identifier === lastParcel) {
            throw new Error("Duplicate parcel identifier blocks publication");
          }
          const propertyFacts = factsByProperty.get(core.property_id) ?? [];
          const property = exportProperty(core, propertyFacts);
          const owners = ownerRows(core.property_id, propertyFacts);
          const canonicalProperty = buildMaterializedCanonicalProperty({
            allowedSnapshotIds,
            asOf,
            core: {
              payload: {
                parcel: (core.payload as { parcel?: unknown }).parcel,
                siteAddress: (core.payload as { siteAddress?: unknown })
                  .siteAddress,
              },
              sourceRunId: core.source_run_id,
              sourceSnapshotId: core.source_snapshot_id,
              versionId: core.version_id,
            },
            facts: publicationFacts(propertyFacts),
            loadedAt,
            parcelObservedAt: observedAt,
            property: {
              exactFolio: property.exact_folio,
              latitude: property.latitude,
              longitude: property.longitude,
              parcelId: property.parcel_id,
              propertyId: property.property_id,
              siteAddress: property.site_address,
              siteCity: property.site_city,
              siteZip: property.site_zip,
              yearBuilt: property.year_built,
            },
            roofSignal: {
              ageYears: property.roof_age_years,
              basis: property.roof_basis,
              basisQuality: property.roof_basis_quality,
            },
            sources: {
              appraiserBuildingUrl,
              appraiserOwnersUrl,
              appraiserParcelUrl,
              appraiserSiteAddressUrl,
              snapshotId: scope.run.snapshot_id!,
            },
          });
          if (!validateCanonical(canonicalProperty)) {
            throw new Error(
              `Canonical property validation failed at ${JSON.stringify(
                validateCanonical.errors?.map(
                  (error: ErrorObject) => error.instancePath,
                ),
              )}`,
            );
          }
          const evidence = (canonicalProperty.evidence ?? []) as Array<{
            observedAt?: unknown;
            sourceName?: unknown;
          }>;
          if (
            evidence.some(
              (item) =>
                typeof item.sourceName === "string" &&
                /(building|ownership|site-address)/i.test(item.sourceName) &&
                item.observedAt !== null,
            )
          ) {
            throw new Error(
              "Related-source evidence contains a fabricated timestamp",
            );
          }
          const propertyBody = Buffer.from(
            publicationCanonicalJson(canonicalProperty),
            "utf8",
          );
          const objectKey = `properties/${property.property_id}.json`;
          const propertySha256 = sha256(propertyBody);
          const propertyCid = await calculateIpfsCid(propertyBody);
          await writeFile(
            path.join(contender, "open-data", objectKey),
            propertyBody,
            { flag: "wx" },
          );
          inventory.push({
            byteSize: propertyBody.byteLength,
            domain: "open_data",
            expectedCid: propertyCid,
            objectKey,
            role: "property",
            sha256: propertySha256,
          });
          propertyCids.set(property.property_id, propertyCid);
          shardEntries.push({
            cid: propertyCid,
            fileSizeBytes: propertyBody.byteLength,
            parcelIdentifier: property.exact_folio,
            propertyId: property.property_id,
          });
          const manifestEntry: ManifestEntry = {
            bytes: propertyBody.byteLength,
            cid: propertyCid,
            objectKey,
            parcelIdentifier: property.exact_folio,
            propertyId: property.property_id,
            sha256: propertySha256,
          };
          manifestLines.push(JSON.stringify(manifestEntry));
          queryLines.push(
            JSON.stringify(
              buildPublicationQueryRow({
                coverageMode: "authoritative_complete",
                loadedAt,
                observedAt,
                owners,
                property,
                propertyCid,
                propertyDocumentSha256: propertySha256,
                runId: scope.run.run_id,
                scopeId: scope.scopeId,
                selectionHash: scope.selectedRecordSha256,
                snapshotId: scope.run.snapshot_id,
              }),
            ),
          );
          const propertyBuildingFacts = propertyFacts.filter(
            (fact) => fact.fact_type === "building",
          ).length;
          const propertyOwnershipFacts = propertyFacts.filter(
            (fact) => fact.fact_type === "ownership",
          ).length;
          buildingFacts += propertyBuildingFacts;
          buildingProperties += propertyBuildingFacts > 0 ? 1 : 0;
          ownershipFacts += propertyOwnershipFacts;
          ownershipProperties += propertyOwnershipFacts > 0 ? 1 : 0;
          coordinateProperties += property.latitude === null ? 0 : 1;
          roofSignalProperties += property.roof_age_years === null ? 0 : 1;
          siteAddressProperties +=
            meaningfulSitusAddress({
              city: property.site_city,
              siteAddress: property.site_address,
              zipCode: property.site_zip,
            }) === null
              ? 0
              : 1;
          propertyBytes += propertyBody.byteLength;
          propertyCount += 1;
          lastParcel = core.parcel_identifier;
          lastPropertyId = core.property_id;
          if (shardEntries.length === PUBLICATION_SHARD_SIZE) {
            await flushShard();
          }
        }
        await entriesFile.write(`${manifestLines.join("\n")}\n`);
        await queryFile.write(`${queryLines.join("\n")}\n`);
        peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
        if (peakRssBytes > AUTHORITATIVE_MAX_RSS_BYTES) {
          throw new Error("Authoritative publication RSS limit exceeded");
        }
        if (
          propertyCount % 10_000 === 0 ||
          cores.length < AUTHORITATIVE_BATCH_SIZE
        ) {
          options.onProgress?.({
            elapsedMs: Math.round(performance.now() - startedAt),
            peakRssBytes,
            properties: propertyCount,
          });
        }
      }
      await flushShard();
    } finally {
      await entriesFile.close();
      await queryFile.close();
    }

    if (propertyCount !== EXPECTED_COVERAGE.activeProperties) {
      throw new Error(
        `Authoritative publication exported ${propertyCount} properties, expected ${EXPECTED_COVERAGE.activeProperties}`,
      );
    }
    const root: RootDocument = {
      completedAt: loadedAt,
      county: "pasco",
      exportedAt: asOf,
      propertyCount,
      schemaVersion: PUBLICATION_GRAPH_VERSION,
      shardSize: PUBLICATION_SHARD_SIZE,
      shards: rootShards,
      totalBytes: propertyBytes,
    };
    const rootBytes = Buffer.from(publicationCanonicalJson(root), "utf8");
    const rootCid = await calculateIpfsCid(rootBytes);
    const rootSha256 = sha256(rootBytes);
    await writeFile(
      path.join(contender, "open-data", "index.json"),
      rootBytes,
      {
        flag: "wx",
      },
    );
    inventory.push({
      byteSize: rootBytes.byteLength,
      domain: "open_data",
      expectedCid: rootCid,
      objectKey: "index.json",
      role: "root",
      sha256: rootSha256,
    });
    rootShards.forEach((shard, index) => {
      edges.push({
        childCid: shard.shardCid,
        childKey: `shards/shard-${String(shard.shardIndex).padStart(4, "0")}.json`,
        jsonPointer: `/shards/${index}/shardCid`,
        parentKey: "index.json",
      });
    });

    const sourceCounts = scope.run.result_counts.sourceCounts;
    const siteAddressSourceRows =
      sourceCounts?.siteAddresses?.accepted ??
      scope.run.result_counts.sourceReconciliation?.siteAddresses?.source ??
      0;
    const coverageSummary = {
      bbb: { availability: "unavailable", reason: "source_not_collected" },
      canonicalProperties: propertyCount,
      contractors: {
        availability: "unavailable",
        reason: "source_unavailable",
      },
      coordinates: coordinateProperties,
      county: "pasco",
      coverageMode: "authoritative_complete",
      membershipAuthority: {
        authorityClass: OWNER_AUTHORITY_CLASS,
        authorityRecordId: binding.authority.authorityRecordId,
        independentlyCertifiedByPasco: false,
        unresolvedPublishedParcelStatistic: 335_946,
      },
      permits: {
        availability: "unavailable",
        reason: "source_unavailable_after_challenge",
      },
      relatedFacts: {
        buildingFacts,
        buildingProperties,
        ownershipFacts,
        ownershipProperties,
        siteAddressProperties,
        siteAddressSourceRows,
      },
      runId: scope.run.run_id,
      scopeId: scope.scopeId,
      selection: {
        algorithm: scope.run.sample_algorithm,
        seed: scope.run.sample_seed,
        selectedRecordSha256: scope.selectedRecordSha256,
        selectionSize: scope.run.selection_size,
      },
      snapshotId: scope.run.snapshot_id,
      scope:
        "owner-assumed authoritative parcel membership for the exact hash-bound source archive",
      sunbiz: {
        availability: "unavailable",
        reason: "source_not_collected",
      },
      warning:
        "Authority applies only to parcel membership in the bound archive. GIS and related-fact coverage are separate. Unavailable permit and contractor fields do not mean zero real-world records.",
    };
    const provenance = {
      authority: {
        authorityClass: binding.authority.authorityClass,
        exactCsvSha256: PASCO_PARCEL_CSV_SHA256,
        exactZipSha256: PASCO_PARCEL_ZIP_SHA256,
        authorityRecordId: binding.authority.authorityRecordId,
        completenessEvidenceSha256:
          binding.authority.completenessEvidenceSha256,
        decisionSha256: binding.authority.decisionSha256,
        independentlyCertifiedByPasco: false,
        membershipClaim: PASCO_PARCEL_MEMBERSHIP_CLAIM,
        sourceSnapshotManifestSha256:
          binding.authority.sourceSnapshotManifestSha256,
        unresolvedSemanticDiscrepancy: {
          acceptedArchiveFolios: PASCO_PARCEL_FOLIO_COUNT,
          publishedRealPropertyParcelStatistic: 335_946,
          status: "unreconciled_membership_or_timing_semantics",
        },
      },
      county: "pasco",
      sources: sourceObjects
        .map((object) => ({
          byteSize: object.byteSize,
          derivedFromSha256: object.derivedFromSha256,
          filename: path.posix.basename(object.relativePath),
          lastModified: object.lastModified,
          observedAt: object.observedAt,
          sha256: object.sha256,
          sourceId: object.sourceId,
          sourceIdentifier: object.sourceIdentifier,
          sourceSystem: object.sourceSystem,
          stage: object.stage,
        }))
        .sort((left, right) => compare(left.sourceId, right.sourceId)),
      sourceWatermark: {
        coverageMode: "authoritative_complete",
        parcelObservedAt: observedAt,
        runId: scope.run.run_id,
        scopeId: scope.scopeId,
        snapshotId: scope.run.snapshot_id,
        workflowId: scope.run.workflow_id,
      },
      sourceSpecificTimestampPolicy: {
        buildingObservedAt: null,
        coordinateObservedAt:
          "sourceLastUpdate when present; otherwise unavailable",
        ownershipObservedAt: null,
        parcelObservedAt: observedAt,
        siteAddressObservedAt: null,
      },
      version: CONTRACT_VERSION,
    };
    const permitIndex = {
      availability: "unavailable",
      county: "pasco",
      contractorCount: 0,
      entries: null,
      permitContractorRelationshipCount: 0,
      permitCount: 0,
      reason: "source_unavailable_after_challenge",
    };
    const runSummary = {
      authorityRecordId: binding.authority.authorityRecordId,
      county: "pasco",
      resultCounts: scope.run.result_counts,
      runId: scope.run.run_id,
      snapshotId: scope.run.snapshot_id,
      workflowId: scope.run.workflow_id,
    };
    const metadata: Array<{
      key: string;
      role: "metadata";
      value: unknown;
    }> = [
      { key: "coverage.json", role: "metadata", value: coverageSummary },
      { key: "provenance.json", role: "metadata", value: provenance },
      { key: "permit-index.json", role: "metadata", value: permitIndex },
      { key: "run-summary.json", role: "metadata", value: runSummary },
    ];
    for (const item of metadata) {
      const bindingValue = await writeCanonicalFile(
        path.join(contender, "open-data", item.key),
        item.value,
      );
      inventory.push({
        ...bindingValue,
        domain: "open_data",
        objectKey: item.key,
        role: item.role,
      });
    }
    const manifestPath = path.join(contender, "open-data", "manifest.json");
    await buildManifest({
      binding,
      entriesPath,
      loadedAt,
      manifestPath,
      propertyCount,
      rootCid,
      shards: shardRecords,
    });
    const manifestBinding = await fileBinding(manifestPath);
    inventory.push({
      ...manifestBinding,
      domain: "open_data",
      objectKey: "manifest.json",
      role: "manifest",
    });
    await rm(entriesPath, { force: true });

    const parquetPath = path.join(queryDir, "query-table.parquet");
    const spillDirectory = path.join(contender, ".duckdb-spill");
    await writeSortedParquet(parquetPath, queryNdjsonPath, spillDirectory);
    await rm(queryNdjsonPath, { force: true });
    await rm(spillDirectory, { force: true, recursive: true });
    const parquetValidation = await validateParquet(parquetPath, propertyCids);
    await validateElephantQueryTableCompatibility(parquetPath);
    if (
      parquetValidation.rows !== propertyCount ||
      parquetValidation.distinctIds !== propertyCount ||
      parquetValidation.nullIds !== 0 ||
      parquetValidation.coordinateRows !== coordinateProperties ||
      parquetValidation.permitAggregateNonNullRows !== 0
    ) {
      throw new Error("Authoritative Parquet reconciliation failed");
    }
    const parquetBinding = await fileBinding(parquetPath);
    inventory.push({
      ...parquetBinding,
      domain: "query_table",
      objectKey: QUERY_TABLE_KEY,
      role: "query_table",
    });
    const schemaSha256 = canonicalJsonSha256(parquetValidation.schema);

    if (
      buildingFacts !== EXPECTED_COVERAGE.buildingFacts ||
      buildingProperties !== EXPECTED_COVERAGE.buildingProperties ||
      ownershipFacts !== EXPECTED_COVERAGE.ownershipFacts ||
      coordinateProperties !== EXPECTED_COVERAGE.coordinates ||
      propertyCount - coordinateProperties !==
        EXPECTED_COVERAGE.missingCoordinates ||
      roofSignalProperties !== EXPECTED_COVERAGE.roofSignals ||
      siteAddressProperties !== EXPECTED_COVERAGE.siteAddressProperties ||
      siteAddressSourceRows !== EXPECTED_COVERAGE.siteAddressSourceRows
    ) {
      throw new Error("Authoritative related-fact coverage changed");
    }
    const expectedCardinality =
      authoritativePublicationCardinality(propertyCount);
    if (
      shardRecords.length !== expectedCardinality.shardCount ||
      edges.length !== expectedCardinality.edgeCount ||
      inventory.length !== expectedCardinality.inventoryObjectCount
    ) {
      throw new Error(
        "Authoritative publication graph cardinality is inconsistent",
      );
    }
    inventory.sort((left, right) =>
      compare(
        `${left.domain}:${left.objectKey}`,
        `${right.domain}:${right.objectKey}`,
      ),
    );
    const targets = incompleteOwnerTargets();
    const missingConfiguration = publicationConfigurationMissing(targets);
    const plan = createPublicationPlan({
      approvable: false,
      artifacts: {
        coverage: artifactBinding(inventory, "open_data", "coverage.json"),
        manifest: artifactBinding(inventory, "open_data", "manifest.json"),
        objectInventory: inventory,
        parquet: {
          ...artifactBinding(inventory, "query_table", QUERY_TABLE_KEY),
          distinctPropertyIds: parquetValidation.distinctIds,
          nullPropertyIds: parquetValidation.nullIds,
          rowCount: parquetValidation.rows,
          schemaSha256,
        },
        provenance: artifactBinding(inventory, "open_data", "provenance.json"),
        shards: shardRecords,
      },
      configuration: {
        credentialsAvailable: false,
        missing: missingConfiguration,
      },
      contracts: {
        canonical: {
          sha256: await fileSha256(
            path.resolve("contracts/canonical-v1.schema.json"),
          ),
          version: "1.0.0",
        },
        mcp: {
          sha256: await fileSha256(
            path.resolve("contracts/mcp-v1.schema.json"),
          ),
          version: "1.2.0",
        },
      },
      counts: {
        activeProperties: propertyCount,
        canonicalDocuments: propertyCount,
        coordinateRows: coordinateProperties,
        inactiveProperties: 0,
        queryTableDistinctPropertyIds: parquetValidation.distinctIds,
        queryTableNullPropertyIds: parquetValidation.nullIds,
        queryTableRows: parquetValidation.rows,
      },
      county: "pasco",
      coverage: {
        authoritativeHeadSnapshotId: scope.authoritativeHeadSnapshotId,
        authoritySourceSystem: "pasco_appraiser",
        completenessResult: "passed",
        entityType: "property_existence",
        mode: "authoritative_complete",
        predecessorChainSnapshotIds: scope.predecessorChainSnapshotIds,
        runId: scope.run.run_id,
        scopeId: scope.scopeId,
        selection: {
          algorithm: scope.run.sample_algorithm,
          seed: scope.run.sample_seed,
          selectedRecordSha256: scope.selectedRecordSha256,
          selectionSize: scope.run.selection_size,
        },
        sourceSnapshotId: scope.run.snapshot_id,
        sourceSnapshotManifestSha256: scope.snapshot?.manifest_sha256 ?? null,
        workflowId: scope.run.workflow_id,
      },
      executable: false,
      exportMode: "authoritative",
      fixtureExclusion: {
        fixturePropertyIdCount: fixtureIds.size,
        matches: 0,
        passed: true,
      },
      freshness: {
        asOf,
        loadedAt,
        observedAt,
      },
      generatedAt: loadedAt,
      graph: {
        cidProfile: { ...IPFS_CID_PROFILE },
        edges,
        openDataRoot: {
          expectedCid: rootCid,
          objectKey: "index.json",
        },
        parquetProfile: {
          compression: "ZSTD",
          duckdbVersion: DUCKDB_VERSION,
          rowGroupSize: 10_000,
          schemaSha256,
        },
        propertyCidCount: propertyCids.size,
        queryTableRoot: {
          expectedCid: parquetBinding.expectedCid,
          objectKey: QUERY_TABLE_KEY,
        },
        traversalValidated: true,
      },
      limitations: [
        "Membership authority is owner-assumed for the exact hash-bound Pasco appraiser archive; independent Pasco certification was not obtained.",
        "The published 335,946 real-property statistic remains unreconciled with the archive's 325,213 distinct folios.",
        "GIS, building, ownership, and site-address coverage are measured separately and do not govern parcel existence.",
        "Permit and contractor sources are unavailable; null aggregates do not mean zero real records.",
        "No Filebase, IPFS, or IPNS effect was performed by this local build.",
        "Owner publication targets, prior IPNS values, provider limits, request prices, recovery budget, and spending ceiling remain unconfigured.",
      ],
      projection: {
        authoritativeBaseSnapshotId: scope.authoritativeBaseSnapshotId,
        materializationId: scope.materializationId!,
        materializationSha256: scope.materializationSha256!,
        snapshotContentSha256: scope.snapshotContentSha256,
      },
      remoteState: {
        openDataIpnsMutationPerformed: false,
        openDataPublishedCid: null,
        queryTableIpnsMutationPerformed: false,
        queryTablePublishedCid: null,
      },
      targets: {
        openData: targets.openData,
        queryTable: targets.queryTable,
      },
      temporalFactLimitation:
        "Every exported core and child fact is selected from one sealed projection materialization; unavailable source observation timestamps remain null.",
      version: "1.1.0",
    });
    const planPath = path.join(contender, "publication-plan.json");
    await writeFile(planPath, `${canonicalJson(plan)}\n`, {
      encoding: "utf8",
      flag: "wx",
    });
    const planArtifactBinding = await fileBinding(planPath);
    validatePublicationPlan(JSON.parse(await readFile(planPath, "utf8")));
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
    if (peakRssBytes > AUTHORITATIVE_MAX_RSS_BYTES) {
      throw new Error("Authoritative plan creation exceeded the RSS limit");
    }
    const finalRoot = path.join(publishBase, "plans", plan.planId);
    await mkdir(path.dirname(finalRoot), { recursive: true });
    const adoptedExisting = await promoteOrAdopt({
      binding,
      contender,
      finalRoot,
      plan,
      sql,
    });
    promoted = true;
    if (!adoptedExisting) {
      await verifyArtifactInventory(finalRoot, plan);
    }
    const outputRelativePath = path
      .relative(options.dataDir, finalRoot)
      .split(path.sep)
      .join("/");
    if (
      outputRelativePath.startsWith("../") ||
      path.isAbsolute(outputRelativePath)
    ) {
      throw new Error("Final authoritative publication escaped DATA_DIR");
    }
    const inventoryBytes = inventory.reduce(
      (total, artifact) => total + artifact.byteSize,
      0,
    );
    return {
      adoptedExisting,
      authorityRecordId: binding.authority.authorityRecordId,
      coverage: {
        activeProperties: propertyCount,
        buildingFacts,
        buildingProperties,
        contractors: 0,
        coordinateProperties,
        missingCoordinateProperties: propertyCount - coordinateProperties,
        ownershipFacts,
        ownershipProperties,
        permitContractorRelationships: 0,
        permits: 0,
        roofSignalProperties,
        siteAddressProperties,
        siteAddressSourceRows,
      },
      durablePublicationStateChanged: false,
      elapsedMs: Math.round(performance.now() - startedAt),
      inventoryBytes,
      inventoryObjectCount: inventory.length,
      manifestCid: manifestBinding.expectedCid,
      manifestSha256: manifestBinding.sha256,
      materializationId: scope.materializationId!,
      openDataRootCid: rootCid,
      outputRelativePath,
      parquetBytes: parquetBinding.byteSize,
      parquetCid: parquetBinding.expectedCid,
      parquetSha256: parquetBinding.sha256,
      peakRssBytes,
      planArtifactBytes: planArtifactBinding.byteSize,
      planArtifactCid: planArtifactBinding.expectedCid,
      planArtifactSha256: planArtifactBinding.sha256,
      planId: plan.planId,
      planSha256: plan.planSha256,
      preflight,
      propertyBytes,
      propertyCount,
      runId: scope.run.run_id,
      shardCount: shardRecords.length,
      snapshotId: scope.run.snapshot_id!,
      sourceObjectCount: sourceObjects.length,
    };
  } finally {
    if (!promoted) {
      await rm(contender, { force: true, recursive: true });
    }
    await sql.end();
  }
}
