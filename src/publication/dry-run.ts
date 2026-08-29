import { createHash } from "node:crypto";
import {
  mkdir,
  readdir,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import path from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import type { Ajv2020 as Ajv2020Class, ErrorObject } from "ajv/dist/2020.js";
import type { FormatsPlugin } from "ajv-formats";
import postgres from "postgres";

import type { PilotRunSummary } from "../domain/types.js";
import {
  recordPublicationPlan,
  type PublicationStateView,
} from "../db/publication-durability.js";
import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import {
  PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
  snapshotCoverageSchema,
  type SnapshotCoverage,
} from "../snapshot/coverage.js";
import {
  createPublicationPlan,
  localIncompletePascoTargets,
  publicationConfigurationMissing,
  type PublicationArtifact,
  type PublicationPlan,
  type PublicationTarget,
} from "./plan.js";

const CONTRACT_VERSION = "1.0.0";
const PROPERTY_SHARD_SIZE = 1_000;
const APPRAISER_ARTIFACT_URI = "artifact://pasco/appraiser/2026-08-23";
const GIS_ARTIFACT_URI = "artifact://pasco/gis/countywide-25000";
const APPRAISER_URL = "https://ftp01.pascopa.com/real_estate/parcel.zip";
const GIS_URL =
  "https://pascogis.pascocountyfl.net/giswebmm/rest/services/PascoMapper/Parcels/MapServer/7";
const require = createRequire(import.meta.url);
const Ajv2020 = require("ajv/dist/2020.js") as typeof Ajv2020Class;
const addFormats = require("ajv-formats") as FormatsPlugin;

interface ExportPropertyRow {
  acres: number | null;
  actual_year_built: number | null;
  coordinate_hash: string | null;
  coordinate_source_last_update: string | null;
  exact_folio: string;
  heated_square_feet: number | null;
  latitude: number | null;
  longitude: number | null;
  parcel_id: string;
  property_id: string;
  property_use_code: string | null;
  property_use_description: string | null;
  roof_age_years: number;
  roof_basis: string;
  roof_basis_quality: string;
  roof_cover: string | null;
  roof_structure: string | null;
  site_address: string;
  site_city: string;
  site_zip: string | null;
  source_record_hash: string;
  total_square_feet: number | null;
  year_built: number;
}

interface ExportOwnerRow {
  mailing_address_1: string | null;
  mailing_address_2: string | null;
  mailing_city: string | null;
  mailing_country: string | null;
  mailing_state: string | null;
  mailing_zip: string | null;
  owner_name_1: string | null;
  owner_name_2: string | null;
  property_id: string;
}

interface ExportRunRow {
  as_of: Date | string;
  completed_at: Date | string;
  coverage_mode: "authoritative_complete" | "partial" | "sample";
  coverage_scope_id: string | null;
  result_counts: PilotRunSummary;
  run_id: string;
  sample_algorithm: string;
  sample_seed: string;
  selection_size: number;
  snapshot_id: string | null;
  workflow_id: string;
}

interface SnapshotRow {
  coverage_metadata: unknown;
  manifest_sha256: string;
  previous_authoritative_snapshot_id: string | null;
  snapshot_id: string;
  source_objects: unknown;
}

interface ManifestEntry {
  bytes: number;
  objectKey: string;
  propertyId: string;
  sha256: string;
}

export interface DryRunSummary {
  activeProperties: number;
  coverageMode: "authoritative_complete" | "partial" | "sample";
  dryRunId: string;
  inactiveProperties: number;
  objectCount: number;
  openDataBytes: number;
  openDataManifestSha256: string;
  outputRoot: string;
  planId: string;
  planSha256: string;
  publicationState: PublicationStateView;
  propertyCount: number;
  queryTableBytes: number;
  queryTableDistinctIds: number;
  queryTableRows: number;
  queryTableSha256: string;
  schemaSha256: string;
}

export interface PublicationTargetConfiguration {
  credentialsAvailable: boolean;
  openData: PublicationTarget;
  queryTable: PublicationTarget;
}

export interface PublicationBuildOptions {
  dataDir: string;
  databaseUrl: string;
  exportMode: "authoritative" | "bounded";
  generatedAt?: string;
  runId: string;
  targets?: PublicationTargetConfiguration;
}

interface ResolvedExportScope {
  activeProperties: number;
  authoritativeHeadSnapshotId: string | null;
  coverage: SnapshotCoverage | null;
  inactiveProperties: number;
  predecessorChainSnapshotIds: string[];
  run: ExportRunRow;
  selectedRecordSha256: string;
  scopeId: string;
  snapshot: SnapshotRow | null;
}

function iso(value: Date | string): string {
  return new Date(value).toISOString();
}

function available(
  value: unknown,
  classification: "derived" | "normalized" | "raw",
  evidenceRefs: string[],
  derivation?: Record<string, unknown>,
) {
  return {
    availability: "available",
    value,
    class: classification,
    evidenceRefs,
    ...(derivation ? { derivation } : {}),
  };
}

function unavailable(
  classification: "derived" | "normalized" | "raw",
  reason:
    "not_provided_by_source" | "source_not_collected" | "source_unavailable",
  evidenceRefs: string[],
) {
  return {
    availability: "unavailable",
    value: null,
    class: classification,
    reason,
    evidenceRefs,
  };
}

async function atomicWrite(filePath: string, body: string | Buffer) {
  await mkdir(path.dirname(filePath), { recursive: true });
  const partPath = `${filePath}.part`;
  await writeFile(partPath, body, { mode: 0o600 });
  await rename(partPath, filePath);
}

async function fileSha256(filePath: string): Promise<string> {
  return createHash("sha256")
    .update(await readFile(filePath))
    .digest("hex");
}

async function directoryMetrics(root: string): Promise<{
  bytes: number;
  files: number;
}> {
  let bytes = 0;
  let files = 0;
  const visit = async (directory: string): Promise<void> => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(entryPath);
      else if (entry.isFile()) {
        files += 1;
        bytes += (await stat(entryPath)).size;
      }
    }
  };
  await visit(root);
  return { bytes, files };
}

function collectPropertyIds(value: unknown, result: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectPropertyIds(entry, result);
    return;
  }
  if (value === null || typeof value !== "object") return;
  const record = value as Record<string, unknown>;
  if (typeof record.propertyId === "string") result.add(record.propertyId);
  for (const entry of Object.values(record)) collectPropertyIds(entry, result);
}

async function contractFixturePropertyIds(): Promise<Set<string>> {
  const result = new Set<string>();
  const fixtureDir = path.resolve("contracts", "fixtures");
  for (const filename of (await readdir(fixtureDir)).sort()) {
    if (!filename.endsWith(".json")) continue;
    collectPropertyIds(
      JSON.parse(await readFile(path.join(fixtureDir, filename), "utf8")),
      result,
    );
  }
  return result;
}

function sqlPath(value: string): string {
  return value.replaceAll("'", "''");
}

export function queryTableColumns(): Record<string, string> {
  return {
    property_id: "VARCHAR",
    parcel_id: "VARCHAR",
    county: "VARCHAR",
    exact_folio: "VARCHAR",
    site_address: "VARCHAR",
    site_city: "VARCHAR",
    site_zip: "VARCHAR",
    latitude: "DOUBLE",
    longitude: "DOUBLE",
    property_use_code: "VARCHAR",
    property_use_description: "VARCHAR",
    acres: "DOUBLE",
    total_square_feet: "DOUBLE",
    heated_square_feet: "DOUBLE",
    year_built: "INTEGER",
    roof_cover: "VARCHAR",
    roof_structure: "VARCHAR",
    roof_installation_date: "DATE",
    roof_installation_year: "INTEGER",
    roof_age_years: "INTEGER",
    roof_age_basis: "VARCHAR",
    roof_age_basis_quality: "VARCHAR",
    owner_name_1: "VARCHAR",
    owner_name_2: "VARCHAR",
    mailing_city: "VARCHAR",
    mailing_state: "VARCHAR",
    mailing_zip: "VARCHAR",
    permit_source_availability: "VARCHAR",
    contractor_source_availability: "VARCHAR",
    sunbiz_source_availability: "VARCHAR",
    bbb_source_availability: "VARCHAR",
    open_roofing_permit_count: "INTEGER",
    maximum_open_roofing_permit_days: "INTEGER",
    property_document_sha256: "VARCHAR",
    property_cid: "VARCHAR",
    source_record_hash: "VARCHAR",
    coverage_mode: "VARCHAR",
    coverage_scope_id: "VARCHAR",
    source_snapshot_id: "VARCHAR",
    source_run_id: "VARCHAR",
    selection_hash: "VARCHAR",
    observed_at: "TIMESTAMPTZ",
    loaded_at: "TIMESTAMPTZ",
    published_at: "TIMESTAMPTZ",
  };
}

export function unavailablePublicationFields() {
  return {
    permit_source_availability: "unavailable",
    contractor_source_availability: "unavailable",
    sunbiz_source_availability: "unavailable",
    bbb_source_availability: "unavailable",
    open_roofing_permit_count: null,
    maximum_open_roofing_permit_days: null,
  } as const;
}

async function writeParquet(
  parquetPath: string,
  ndjsonPath: string,
): Promise<void> {
  const columns = Object.entries(queryTableColumns())
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
        )
        ORDER BY property_id
      ) TO '${sqlPath(parquetPath)}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
    `);
  } finally {
    connection.closeSync();
  }
}

async function validateParquet(parquetPath: string): Promise<{
  coordinateRows: number;
  distinctIds: number;
  nullIds: number;
  permitAggregateNonNullRows: number;
  roofBasis: Record<string, number>;
  rows: number;
  schema: unknown[];
}> {
  const bytes = await readFile(parquetPath);
  if (
    bytes.subarray(0, 4).toString("ascii") !== "PAR1" ||
    bytes.subarray(-4).toString("ascii") !== "PAR1"
  ) {
    throw new Error("Parquet does not have PAR1 header and footer bytes");
  }
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    const summaryReader = await connection.runAndReadAll(`
      SELECT
        count(*)::BIGINT AS rows,
        count(DISTINCT property_id)::BIGINT AS distinct_ids,
        count(*) FILTER (WHERE property_id IS NULL OR property_id = '')::BIGINT AS null_ids,
        count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL)::BIGINT AS coordinate_rows,
        count(open_roofing_permit_count)::BIGINT AS permit_aggregate_non_null_rows
      FROM read_parquet('${sqlPath(parquetPath)}')
    `);
    const summary = summaryReader.getRowObjectsJson()[0] as
      Record<string, string> | undefined;
    const basisReader = await connection.runAndReadAll(`
      SELECT roof_age_basis, count(*)::BIGINT AS count
      FROM read_parquet('${sqlPath(parquetPath)}')
      GROUP BY roof_age_basis ORDER BY roof_age_basis
    `);
    const schemaReader = await connection.runAndReadAll(
      `DESCRIBE SELECT * FROM read_parquet('${sqlPath(parquetPath)}')`,
    );
    return {
      coordinateRows: Number(summary?.coordinate_rows ?? 0),
      distinctIds: Number(summary?.distinct_ids ?? 0),
      nullIds: Number(summary?.null_ids ?? 0),
      permitAggregateNonNullRows: Number(
        summary?.permit_aggregate_non_null_rows ?? 0,
      ),
      roofBasis: Object.fromEntries(
        basisReader.getRowObjectsJson().map((row) => {
          const value = row as Record<string, string>;
          return [value.roof_age_basis, Number(value.count)];
        }),
      ),
      rows: Number(summary?.rows ?? 0),
      schema: schemaReader.getRowObjectsJson(),
    };
  } finally {
    connection.closeSync();
  }
}

function strictCoverage(value: unknown): SnapshotCoverage {
  const parsed = snapshotCoverageSchema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new Error(
      `Publication snapshot coverage is invalid at ${issue?.path.join(".") || "root"}`,
    );
  }
  return parsed.data;
}

function selectedRecordSha256(folios: readonly string[]): string {
  return sha256(JSON.stringify([...folios].sort()));
}

async function resolveExportScope(
  sql: postgres.Sql,
  options: PublicationBuildOptions,
): Promise<ResolvedExportScope> {
  const runs = await sql<ExportRunRow[]>`
    SELECT run_id, workflow_id, as_of, completed_at, result_counts,
           sample_algorithm, sample_seed, selection_size, snapshot_id,
           coverage_mode, coverage_scope_id
    FROM oracle_pipeline_runs
    WHERE run_id = ${options.runId} AND status = 'completed'
  `;
  const run = runs[0];
  if (!run || !run.completed_at || !run.selection_size) {
    throw new Error("Publication requires a completed bounded Pasco run");
  }
  if (options.exportMode === "authoritative") {
    if (
      run.coverage_mode !== "authoritative_complete" ||
      run.snapshot_id === null ||
      run.coverage_scope_id === null
    ) {
      throw new Error(
        "A sample or partial run cannot request authoritative publication",
      );
    }
    const snapshots = await sql<SnapshotRow[]>`
      SELECT snapshot_id, manifest_sha256, coverage_metadata, source_objects,
             previous_authoritative_snapshot_id
      FROM oracle_source_snapshots
      WHERE snapshot_id = ${run.snapshot_id}
    `;
    const snapshot = snapshots[0];
    if (!snapshot) {
      throw new Error("Authoritative publication snapshot is missing");
    }
    const coverage = strictCoverage(snapshot.coverage_metadata);
    if (
      coverage.mode !== "authoritative_complete" ||
      coverage.completeness.result !== "passed" ||
      coverage.scopeId !== run.coverage_scope_id ||
      coverage.authoritySource.sourceSystem !== "pasco_appraiser" ||
      coverage.authoritySource.sourceIdentifier !==
        PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER
    ) {
      throw new Error(
        "Authoritative publication lacks verified official parcel completeness",
      );
    }
    const heads = await sql<
      {
        current_run_id: string;
        current_snapshot_id: string;
        scope_id: string;
      }[]
    >`
      SELECT scope_id, current_snapshot_id, current_run_id
      FROM oracle_authoritative_scope_heads
      WHERE county = 'pasco' AND entity_type = 'property_existence'
    `;
    const head = heads[0];
    if (
      !head ||
      head.scope_id !== coverage.scopeId ||
      head.current_snapshot_id !== run.snapshot_id ||
      head.current_run_id !== run.run_id
    ) {
      throw new Error(
        "Authoritative publication requires the exact current scope head",
      );
    }
    const predecessorChainSnapshotIds: string[] = [];
    const seen = new Set<string>();
    let cursor: string | null = run.snapshot_id;
    while (cursor !== null) {
      if (seen.has(cursor)) {
        throw new Error(
          "Authoritative snapshot predecessor chain contains a cycle",
        );
      }
      seen.add(cursor);
      predecessorChainSnapshotIds.push(cursor);
      const rows: {
        coverage_metadata: unknown;
        previous_authoritative_snapshot_id: string | null;
      }[] = await sql`
        SELECT coverage_metadata, previous_authoritative_snapshot_id
        FROM oracle_source_snapshots WHERE snapshot_id = ${cursor}
      `;
      const predecessor:
        | {
            coverage_metadata: unknown;
            previous_authoritative_snapshot_id: string | null;
          }
        | undefined = rows[0];
      if (!predecessor) {
        throw new Error("Authoritative predecessor snapshot is missing");
      }
      const predecessorCoverage = strictCoverage(predecessor.coverage_metadata);
      if (
        predecessorCoverage.mode !== "authoritative_complete" ||
        predecessorCoverage.scopeId !== coverage.scopeId ||
        predecessorCoverage.completeness.result !== "passed" ||
        predecessorCoverage.previousAuthoritativeSnapshotId !==
          predecessor.previous_authoritative_snapshot_id
      ) {
        throw new Error("Authoritative predecessor chain is inconsistent");
      }
      cursor = predecessor.previous_authoritative_snapshot_id;
    }
    const lifecycleCounts = await sql<{ active: number; inactive: number }[]>`
      SELECT
        count(*) FILTER (WHERE lifecycle_status = 'active')::int AS active,
        count(*) FILTER (WHERE lifecycle_status = 'inactive')::int AS inactive
      FROM oracle_property_scope_state
      WHERE scope_id = ${coverage.scopeId}
    `;
    const activeFolios = await sql<{ exact_folio: string }[]>`
      SELECT property.exact_folio
      FROM oracle_property_scope_state state
      JOIN oracle_properties property USING (property_id)
      WHERE state.scope_id = ${coverage.scopeId}
        AND state.lifecycle_status = 'active'
        AND state.last_reconciled_snapshot_id = ${run.snapshot_id}
        AND property.is_active
        AND property.lifecycle_scope_id = ${coverage.scopeId}
      ORDER BY property.exact_folio
    `;
    if (
      selectedRecordSha256(activeFolios.map((row) => row.exact_folio)) !==
        coverage.selection.selectedRecordSha256 ||
      activeFolios.length !== coverage.selection.selectionSize ||
      activeFolios.length !== (lifecycleCounts[0]?.active ?? 0)
    ) {
      throw new Error(
        "Authoritative active membership does not match the snapshot selection hash",
      );
    }
    return {
      activeProperties: lifecycleCounts[0]?.active ?? 0,
      authoritativeHeadSnapshotId: run.snapshot_id,
      coverage,
      inactiveProperties: lifecycleCounts[0]?.inactive ?? 0,
      predecessorChainSnapshotIds,
      run,
      selectedRecordSha256: coverage.selection.selectedRecordSha256,
      scopeId: coverage.scopeId,
      snapshot,
    };
  }

  if (run.coverage_mode === "authoritative_complete") {
    throw new Error(
      "An authoritative-complete run must use authoritative export mode",
    );
  }
  const membership = await sql<{ exact_folio: string; is_active: boolean }[]>`
    SELECT exact_folio, is_active
    FROM oracle_properties
    WHERE last_seen_run_id = ${run.run_id}
    ORDER BY exact_folio
  `;
  const selectionHash = selectedRecordSha256(
    membership.map((row) => row.exact_folio),
  );
  if (membership.length !== run.selection_size) {
    throw new Error(
      `Bounded publication membership contains ${membership.length} records, expected ${run.selection_size}`,
    );
  }
  let snapshot: SnapshotRow | null = null;
  let coverage: SnapshotCoverage | null = null;
  if (run.snapshot_id !== null) {
    const snapshots = await sql<SnapshotRow[]>`
      SELECT snapshot_id, manifest_sha256, coverage_metadata, source_objects,
             previous_authoritative_snapshot_id
      FROM oracle_source_snapshots
      WHERE snapshot_id = ${run.snapshot_id}
    `;
    snapshot = snapshots[0] ?? null;
    if (!snapshot) throw new Error("Bounded publication snapshot is missing");
    coverage = strictCoverage(snapshot.coverage_metadata);
    if (
      coverage.mode !== run.coverage_mode ||
      coverage.selection.selectedRecordSha256 !== selectionHash ||
      coverage.scopeId !== run.coverage_scope_id
    ) {
      throw new Error("Bounded publication snapshot scope is inconsistent");
    }
  }
  const scopeId =
    coverage?.scopeId ??
    deterministicId("scope", [
      "1.0.0",
      "publication-bounded-scope",
      "pasco",
      run.coverage_mode,
      run.run_id,
      run.sample_algorithm,
      run.sample_seed,
      String(run.selection_size),
      selectionHash,
    ]);
  return {
    activeProperties: membership.filter((row) => row.is_active).length,
    authoritativeHeadSnapshotId: null,
    coverage,
    inactiveProperties: membership.filter((row) => !row.is_active).length,
    predecessorChainSnapshotIds: [],
    run,
    selectedRecordSha256: selectionHash,
    scopeId,
    snapshot,
  };
}

async function publicationInventory(
  openDataRoot: string,
  parquetPath: string,
): Promise<PublicationArtifact[]> {
  const artifacts: PublicationArtifact[] = [];
  const visit = async (directory: string): Promise<void> => {
    for (const entry of (
      await readdir(directory, { withFileTypes: true })
    ).sort((left, right) => left.name.localeCompare(right.name))) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) await visit(entryPath);
      else if (entry.isFile()) {
        artifacts.push({
          byteSize: (await stat(entryPath)).size,
          domain: "open_data",
          objectKey: path
            .relative(openDataRoot, entryPath)
            .split(path.sep)
            .join("/"),
          sha256: await fileSha256(entryPath),
        });
      }
    }
  };
  await visit(openDataRoot);
  artifacts.push({
    byteSize: (await stat(parquetPath)).size,
    domain: "query_table",
    objectKey: "query-tables/pasco/query-table.parquet",
    sha256: await fileSha256(parquetPath),
  });
  return artifacts.sort((left, right) =>
    `${left.domain}:${left.objectKey}`.localeCompare(
      `${right.domain}:${right.objectKey}`,
    ),
  );
}

export async function buildPublicationDryRun(
  options: PublicationBuildOptions,
): Promise<DryRunSummary> {
  const publishBase = path.resolve(
    options.dataDir,
    "artifacts",
    "publish",
    "pasco",
  );
  const outputRoot = path.join(publishBase, `dry-run-${options.runId}`);
  if (!outputRoot.startsWith(`${publishBase}${path.sep}`)) {
    throw new Error("Publication output escaped the Pasco publish directory");
  }
  const sql = postgres(options.databaseUrl, { max: 1 });
  try {
    const scope = await resolveExportScope(sql, options);
    const run = scope.run;
    await rm(outputRoot, { force: true, recursive: true });
    await mkdir(outputRoot, { recursive: true });
    const asOf = iso(run.as_of);
    const loadedAt = iso(run.completed_at);
    const properties = await sql<ExportPropertyRow[]>`
      SELECT
        p.property_id,
        p.parcel_id,
        p.exact_folio,
        p.site_address,
        p.site_city,
        p.site_zip,
        p.property_use_code,
        p.property_use_description,
        p.acres::double precision AS acres,
        p.total_square_feet::double precision AS total_square_feet,
        p.heated_square_feet::double precision AS heated_square_feet,
        p.year_built,
        p.source_record_hash,
        c.latitude,
        c.longitude,
        c.source_last_update AS coordinate_source_last_update,
        c.source_record_hash AS coordinate_hash,
        r.age_years AS roof_age_years,
        r.basis AS roof_basis,
        r.basis_quality AS roof_basis_quality,
        b.actual_year_built,
        b.roof_cover,
        b.roof_structure
      FROM oracle_properties p
      LEFT JOIN oracle_coordinates c
        ON c.property_id = p.property_id
       AND c.last_seen_run_id = ${run.run_id}
      LEFT JOIN oracle_roof_signals r
        ON r.property_id = p.property_id
       AND r.last_seen_run_id = ${run.run_id}
      LEFT JOIN LATERAL (
        SELECT actual_year_built, roof_cover, roof_structure
        FROM oracle_building_signals candidate
        WHERE candidate.property_id = p.property_id
          AND candidate.last_seen_run_id = ${run.run_id}
        ORDER BY building_number, building_section, building_signal_id
        LIMIT 1
      ) b ON true
      WHERE p.is_active
        AND (
          (
            ${options.exportMode === "bounded"}
            AND p.last_seen_run_id = ${run.run_id}
          ) OR (
            ${options.exportMode === "authoritative"}
            AND p.lifecycle_scope_id = ${scope.scopeId}
            AND EXISTS (
              SELECT 1 FROM oracle_property_scope_state state
              WHERE state.property_id = p.property_id
                AND state.scope_id = ${scope.scopeId}
                AND state.lifecycle_status = 'active'
                AND state.last_reconciled_snapshot_id = ${run.snapshot_id}
            )
          )
        )
      ORDER BY p.property_id
    `;
    if (properties.length !== scope.activeProperties) {
      throw new Error(
        `Publication active scope contains ${properties.length} properties, expected ${scope.activeProperties}`,
      );
    }
    const fixturePropertyIds = await contractFixturePropertyIds();
    const fixtureMatches = properties.filter((property) =>
      fixturePropertyIds.has(property.property_id),
    ).length;
    if (fixtureMatches > 0) {
      throw new Error(
        `Publication scope contains ${fixtureMatches} frozen fixture property IDs`,
      );
    }
    const propertyIds = properties.map((row) => row.property_id);
    const ownerRows = await sql<ExportOwnerRow[]>`
      SELECT property_id, owner_name_1, owner_name_2,
             mailing_address_1, mailing_address_2, mailing_city,
             mailing_state, mailing_zip, mailing_country
      FROM oracle_ownerships
      WHERE property_id = ANY(${propertyIds})
        AND last_seen_run_id = ${run.run_id}
      ORDER BY property_id, ownership_id
    `;
    const ownersByProperty = new Map<string, ExportOwnerRow[]>();
    for (const owner of ownerRows) {
      const rows = ownersByProperty.get(owner.property_id) ?? [];
      rows.push(owner);
      ownersByProperty.set(owner.property_id, rows);
    }
    const sourceArtifacts = await sql<
      { local_uri: string; sha256: string; source_system: string }[]
    >`
      SELECT local_uri, sha256, source_system
      FROM oracle_source_artifacts
      WHERE run_id = ${options.runId}
      ORDER BY source_system, local_uri
    `;
    const availabilityCounts = await sql<{ count: number; feature: string }[]>`
      SELECT feature, count(DISTINCT property_id)::int AS count
      FROM oracle_property_availability
      WHERE property_id = ANY(${propertyIds})
        AND last_seen_run_id = ${run.run_id}
      GROUP BY feature ORDER BY feature
    `;
    const requiredUnavailableFeatures = [
      "bbb",
      "contractors",
      "emails",
      "permits",
      "phones",
      "sunbiz",
    ];
    const availabilityByFeature = new Map(
      availabilityCounts.map((row) => [row.feature, row.count]),
    );
    if (
      requiredUnavailableFeatures.some(
        (feature) => availabilityByFeature.get(feature) !== properties.length,
      )
    ) {
      throw new Error(
        "Publication scope lacks current explicit source-availability facts",
      );
    }
    const permitCoverage = await sql<
      { contractors: number; permits: number }[]
    >`
      SELECT
        count(DISTINCT permit.permit_id)::int AS permits,
        count(DISTINCT link.contractor_id)::int AS contractors
      FROM oracle_properties property
      LEFT JOIN oracle_permits permit
        ON permit.property_id = property.property_id
       AND permit.last_seen_run_id = ${run.run_id}
      LEFT JOIN oracle_permit_contractors link
        ON link.permit_id = permit.permit_id
      WHERE property.property_id = ANY(${propertyIds})
    `;
    if (
      permitCoverage[0]?.permits !== 0 ||
      permitCoverage[0]?.contractors !== 0
    ) {
      throw new Error(
        "This publication checkpoint cannot omit available permit or contractor records",
      );
    }

    const snapshotSourceObjects = Array.isArray(scope.snapshot?.source_objects)
      ? scope.snapshot.source_objects.filter(
          (value): value is Record<string, unknown> =>
            value !== null &&
            typeof value === "object" &&
            !Array.isArray(value),
        )
      : [];
    const sourceUrl = (sourceSystem: string, fallback: string) => {
      const value = snapshotSourceObjects.find(
        (object) =>
          object.sourceSystem === sourceSystem &&
          object.stage === "downloaded_source" &&
          typeof object.sourceIdentifier === "string",
      )?.sourceIdentifier;
      return typeof value === "string" && value.startsWith("https://")
        ? value
        : fallback;
    };
    const appraiserArtifactUri = scope.snapshot
      ? `artifact://pasco/snapshot/${scope.snapshot.snapshot_id}/pasco_appraiser`
      : APPRAISER_ARTIFACT_URI;
    const gisArtifactUri = scope.snapshot
      ? `artifact://pasco/snapshot/${scope.snapshot.snapshot_id}/pasco_gis`
      : GIS_ARTIFACT_URI;
    const appraiserSourceUrl = sourceUrl("pasco_appraiser", APPRAISER_URL);
    const gisSourceUrl = sourceUrl("pasco_gis", GIS_URL);
    const observedAt =
      scope.coverage?.sourceObservationWindow.start ??
      "2026-08-23T00:00:00.000Z";

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
    const validateProperty = ajv.compile({
      $ref: `${canonicalSchema.$id}#/$defs/CanonicalProperty`,
    });

    const openDataRoot = path.join(outputRoot, "open-data");
    const entries: ManifestEntry[] = [];
    const queryRows: Record<string, unknown>[] = [];
    for (const property of properties) {
      const appraiserEvidenceId = deterministicId("evidence", [
        CONTRACT_VERSION,
        "evidence",
        property.property_id,
        "pasco_appraiser",
      ]);
      const gisEvidenceId = deterministicId("evidence", [
        CONTRACT_VERSION,
        "evidence",
        property.property_id,
        "pasco_gis",
      ]);
      const owners = ownersByProperty.get(property.property_id) ?? [];
      const address = [
        property.site_address,
        property.site_city,
        "FL",
        property.site_zip,
      ]
        .filter(Boolean)
        .join(", ");
      const evidence = [
        {
          evidenceId: appraiserEvidenceId,
          sourceSystem: "pasco_appraiser",
          sourceName: "Pasco County Property Appraiser bulk working roll",
          sourceRecordKey: property.exact_folio,
          sourceUrl: appraiserSourceUrl,
          sourceArtifactUri: appraiserArtifactUri,
          sourceRecordHash: property.source_record_hash,
          observedAt,
          retrievedAt: asOf,
          loadedAt,
          publishedCid: null,
        },
        ...(property.coordinate_hash
          ? [
              {
                evidenceId: gisEvidenceId,
                sourceSystem: "pasco_gis",
                sourceName: "Pasco County GIS parcel polygon",
                sourceRecordKey: property.exact_folio,
                sourceUrl: gisSourceUrl,
                sourceArtifactUri: gisArtifactUri,
                sourceRecordHash: property.coordinate_hash,
                observedAt: null,
                retrievedAt: asOf,
                loadedAt,
                publishedCid: null,
              },
            ]
          : []),
      ];
      const canonicalProperty = {
        entityType: "property",
        contractVersion: CONTRACT_VERSION,
        propertyId: property.property_id,
        parcelId: property.parcel_id,
        county: "pasco",
        sourceSystem: "pasco_appraiser",
        folio: available(property.exact_folio, "raw", [appraiserEvidenceId]),
        parcelIdentifier: available(property.exact_folio, "raw", [
          appraiserEvidenceId,
        ]),
        situsAddress: available(address, "normalized", [appraiserEvidenceId]),
        coordinates:
          property.latitude === null || property.longitude === null
            ? unavailable("normalized", "not_provided_by_source", [
                appraiserEvidenceId,
              ])
            : available(
                {
                  latitude: property.latitude,
                  longitude: property.longitude,
                  crs: "EPSG:4326",
                },
                "normalized",
                [gisEvidenceId],
              ),
        yearBuilt: available(property.year_built, "raw", [appraiserEvidenceId]),
        roofInstallationDate: unavailable("raw", "not_provided_by_source", [
          appraiserEvidenceId,
        ]),
        roofInstallationYear: unavailable("raw", "not_provided_by_source", [
          appraiserEvidenceId,
        ]),
        roofAgeSignal: available(
          {
            ageYears: property.roof_age_years,
            precision: "year",
            basis: property.roof_basis,
            basisQuality: property.roof_basis_quality,
            asOf,
          },
          "derived",
          [appraiserEvidenceId],
          {
            rule: "year_difference_proxy",
            ruleVersion: CONTRACT_VERSION,
            asOf,
            inputs: ["property.yearBuilt"],
          },
        ),
        ownership:
          owners.length > 0
            ? available(
                owners.map((owner) => ({
                  ownerName1: owner.owner_name_1,
                  ownerName2: owner.owner_name_2,
                  mailingAddress1: owner.mailing_address_1,
                  mailingAddress2: owner.mailing_address_2,
                  mailingCity: owner.mailing_city,
                  mailingState: owner.mailing_state,
                  mailingZip: owner.mailing_zip,
                  mailingCountry: owner.mailing_country,
                })),
                "raw",
                [appraiserEvidenceId],
              )
            : unavailable("raw", "not_provided_by_source", [
                appraiserEvidenceId,
              ]),
        permits: [],
        evidence,
        freshness: {
          observedAt,
          retrievedAt: asOf,
          loadedAt,
          publishedAt: null,
          computedAt: asOf,
          sourceCadence: "weekly appraiser working roll",
        },
      };
      if (!validateProperty(canonicalProperty)) {
        throw new Error(
          `Canonical property validation failed at ${JSON.stringify(validateProperty.errors?.map((error: ErrorObject) => error.instancePath))}`,
        );
      }
      const propertyBody = `${JSON.stringify(canonicalProperty)}\n`;
      const objectKey = `properties/${property.property_id.slice(-2)}/${property.property_id}.json`;
      const propertyPath = path.join(openDataRoot, objectKey);
      await atomicWrite(propertyPath, propertyBody);
      const propertyHash = sha256(propertyBody);
      entries.push({
        bytes: Buffer.byteLength(propertyBody),
        objectKey,
        propertyId: property.property_id,
        sha256: propertyHash,
      });
      const primaryOwner = owners[0];
      queryRows.push({
        property_id: property.property_id,
        parcel_id: property.parcel_id,
        county: "pasco",
        exact_folio: property.exact_folio,
        site_address: property.site_address,
        site_city: property.site_city,
        site_zip: property.site_zip,
        latitude: property.latitude,
        longitude: property.longitude,
        property_use_code: property.property_use_code,
        property_use_description: property.property_use_description,
        acres: property.acres,
        total_square_feet: property.total_square_feet,
        heated_square_feet: property.heated_square_feet,
        year_built: property.year_built,
        roof_cover: property.roof_cover,
        roof_structure: property.roof_structure,
        roof_installation_date: null,
        roof_installation_year: null,
        roof_age_years: property.roof_age_years,
        roof_age_basis: property.roof_basis,
        roof_age_basis_quality: property.roof_basis_quality,
        owner_name_1: primaryOwner?.owner_name_1 ?? null,
        owner_name_2: primaryOwner?.owner_name_2 ?? null,
        mailing_city: primaryOwner?.mailing_city ?? null,
        mailing_state: primaryOwner?.mailing_state ?? null,
        mailing_zip: primaryOwner?.mailing_zip ?? null,
        ...unavailablePublicationFields(),
        property_document_sha256: propertyHash,
        property_cid: null,
        source_record_hash: property.source_record_hash,
        coverage_mode: run.coverage_mode,
        coverage_scope_id: scope.scopeId,
        source_snapshot_id: run.snapshot_id,
        source_run_id: run.run_id,
        selection_hash: scope.selectedRecordSha256,
        observed_at: observedAt,
        loaded_at: loadedAt,
        published_at: null,
      });
    }

    const shardRecords = [];
    for (let index = 0; index < entries.length; index += PROPERTY_SHARD_SIZE) {
      const shardEntries = entries.slice(index, index + PROPERTY_SHARD_SIZE);
      const shardKey = `shards/shard-${String(index / PROPERTY_SHARD_SIZE).padStart(4, "0")}.json`;
      const shardBody = `${JSON.stringify({
        county: "pasco",
        entries: shardEntries,
        propertyCount: shardEntries.length,
        version: CONTRACT_VERSION,
      })}\n`;
      await atomicWrite(path.join(openDataRoot, shardKey), shardBody);
      shardRecords.push({
        bytes: Buffer.byteLength(shardBody),
        objectKey: shardKey,
        propertyCount: shardEntries.length,
        sha256: sha256(shardBody),
      });
    }

    const provenanceFiles = (sourceSystem: string) =>
      scope.snapshot
        ? snapshotSourceObjects
            .filter(
              (object) =>
                object.sourceSystem === sourceSystem &&
                typeof object.relativePath === "string" &&
                typeof object.sha256 === "string",
            )
            .map((object) => ({
              path: object.relativePath as string,
              sha256: object.sha256 as string,
            }))
            .sort((left, right) => left.path.localeCompare(right.path))
        : sourceArtifacts
            .filter((artifact) => artifact.source_system === sourceSystem)
            .map((artifact) => ({
              path: path.relative(
                options.dataDir,
                fileURLToPath(artifact.local_uri),
              ),
              sha256: artifact.sha256,
            }));
    const provenance = {
      county: "pasco",
      sources: [
        {
          artifactUri: appraiserArtifactUri,
          files: provenanceFiles("pasco_appraiser"),
          sourceSystem: "pasco_appraiser",
        },
        {
          artifactUri: gisArtifactUri,
          files: provenanceFiles("pasco_gis"),
          sourceSystem: "pasco_gis",
        },
      ],
      sourceWatermark: {
        appraiserObservedDate: observedAt.slice(0, 10),
        coverageMode: run.coverage_mode,
        runId: run.run_id,
        scopeId: scope.scopeId,
        snapshotId: run.snapshot_id,
        workflowId: run.workflow_id,
      },
      version: CONTRACT_VERSION,
    };
    const dataRoot = await realpath(options.dataDir);
    for (const source of provenance.sources) {
      if (source.files.length === 0) {
        throw new Error(
          `Provenance has no local files for ${source.sourceSystem}`,
        );
      }
      for (const file of source.files) {
        if (
          path.isAbsolute(file.path) ||
          file.path.split(/[\\/]/).includes("..")
        ) {
          throw new Error("Provenance source path escapes DATA_DIR");
        }
        const sourcePath = await realpath(path.resolve(dataRoot, file.path));
        if (
          sourcePath !== dataRoot &&
          !sourcePath.startsWith(`${dataRoot}${path.sep}`)
        ) {
          throw new Error("Provenance source link escapes DATA_DIR");
        }
        await stat(sourcePath);
      }
    }
    const coverage = {
      bbb: { availability: "unavailable", reason: "source_not_collected" },
      canonicalProperties: properties.length,
      coverageMode: run.coverage_mode,
      contractors: {
        availability: "unavailable",
        reason: "source_unavailable",
      },
      coordinates: properties.filter((property) => property.latitude !== null)
        .length,
      county: "pasco",
      permits: {
        availability: "unavailable",
        reason: "source_unavailable_after_challenge",
      },
      runId: run.run_id,
      scopeId: scope.scopeId,
      selection: {
        algorithm: run.sample_algorithm,
        seed: run.sample_seed,
        selectedRecordSha256: scope.selectedRecordSha256,
        selectionSize: run.selection_size,
      },
      snapshotId: run.snapshot_id,
      scope:
        run.coverage_mode === "sample"
          ? "deterministic bounded appraisal/GIS sample"
          : run.coverage_mode === "partial"
            ? "partial appraisal/GIS scope"
            : "authoritative-complete current property scope",
      sunbiz: {
        availability: "unavailable",
        reason: "source_not_collected",
      },
      warning:
        "Unavailable permit and enrichment fields do not mean that no matching real-world records exist.",
    };
    await atomicWrite(
      path.join(openDataRoot, "provenance.json"),
      `${JSON.stringify(provenance)}\n`,
    );
    await atomicWrite(
      path.join(openDataRoot, "coverage.json"),
      `${JSON.stringify(coverage)}\n`,
    );
    await atomicWrite(
      path.join(openDataRoot, "permit-index.json"),
      `${JSON.stringify({
        availability: "unavailable",
        county: "pasco",
        entries: null,
        reason: "source_unavailable_after_challenge",
      })}\n`,
    );
    await atomicWrite(
      path.join(openDataRoot, "run-summary.json"),
      `${JSON.stringify({
        county: "pasco",
        resultCounts: run.result_counts,
        runId: run.run_id,
        workflowId: run.workflow_id,
      })}\n`,
    );
    await atomicWrite(
      path.join(openDataRoot, "index.json"),
      `${JSON.stringify({
        county: "pasco",
        coverage: "coverage.json",
        coverageMode: run.coverage_mode,
        manifest: "manifest.json",
        permitIndex: "permit-index.json",
        propertyCount: entries.length,
        provenance: "provenance.json",
        shards: shardRecords,
        version: CONTRACT_VERSION,
      })}\n`,
    );
    const manifest = {
      contractVersion: CONTRACT_VERSION,
      county: "pasco",
      coverageMode: run.coverage_mode,
      entries,
      generatedAt: loadedAt,
      propertyCount: entries.length,
      representation: "canonical-property-json-v1",
      scopeId: scope.scopeId,
      selectionHash: scope.selectedRecordSha256,
      shards: shardRecords,
      sourceRunId: run.run_id,
      sourceSnapshotId: run.snapshot_id,
    };
    const manifestPath = path.join(openDataRoot, "manifest.json");
    await atomicWrite(manifestPath, `${JSON.stringify(manifest)}\n`);
    const openDataManifestSha256 = await fileSha256(manifestPath);

    const queryDir = path.join(outputRoot, "query", "query-tables", "pasco");
    await mkdir(queryDir, { recursive: true });
    const ndjsonPath = path.join(queryDir, "query-table.ndjson.part");
    await atomicWrite(
      ndjsonPath,
      `${queryRows.map((row) => JSON.stringify(row)).join("\n")}\n`,
    );
    const parquetPath = path.join(queryDir, "query-table.parquet");
    await writeParquet(parquetPath, ndjsonPath);
    await rm(ndjsonPath, { force: true });
    const parquetValidation = await validateParquet(parquetPath);
    if (
      parquetValidation.rows !== entries.length ||
      parquetValidation.distinctIds !== entries.length ||
      parquetValidation.nullIds !== 0 ||
      parquetValidation.permitAggregateNonNullRows !== 0
    ) {
      throw new Error(
        "Parquet cardinality or unavailable-permit validation failed",
      );
    }
    const queryTableSha256 = await fileSha256(parquetPath);
    const queryTableBytes = (await stat(parquetPath)).size;
    const schemaSha256 = canonicalJsonSha256(parquetValidation.schema);

    const openDataMetrics = await directoryMetrics(openDataRoot);
    const validation = {
      activeProperties: scope.activeProperties,
      canonicalDocumentsValidated: entries.length,
      coordinateRows: parquetValidation.coordinateRows,
      coverageMode: run.coverage_mode,
      distinctPropertyIds: parquetValidation.distinctIds,
      fixturePropertyIdCount: fixturePropertyIds.size,
      fixturePropertyIdsExcluded: fixtureMatches === 0,
      manifestPropertyCount: entries.length,
      inactiveProperties: scope.inactiveProperties,
      nullPropertyIds: parquetValidation.nullIds,
      parquetRows: parquetValidation.rows,
      permitAggregateNonNullRows: parquetValidation.permitAggregateNonNullRows,
      provenanceReferencesResolved: true,
      roofSignalBasis: parquetValidation.roofBasis,
      scopeId: scope.scopeId,
      sourceRunId: run.run_id,
      sourceSnapshotId: run.snapshot_id,
    };
    await atomicWrite(
      path.join(outputRoot, "validation.json"),
      `${JSON.stringify(validation)}\n`,
    );

    const inventory = await publicationInventory(openDataRoot, parquetPath);
    const byObjectKey = new Map(
      inventory.map((artifact) => [
        `${artifact.domain}:${artifact.objectKey}`,
        artifact,
      ]),
    );
    const binding = (
      domain: PublicationArtifact["domain"],
      objectKey: string,
    ) => {
      const artifact = byObjectKey.get(`${domain}:${objectKey}`);
      if (!artifact) {
        throw new Error(
          `Publication inventory is missing ${domain}:${objectKey}`,
        );
      }
      return {
        byteSize: artifact.byteSize,
        objectKey: artifact.objectKey,
        sha256: artifact.sha256,
      };
    };
    const targets = options.targets ?? localIncompletePascoTargets();
    const missingConfiguration = publicationConfigurationMissing(targets);
    const generatedAt = options.generatedAt ?? loadedAt;
    const plan: PublicationPlan = createPublicationPlan({
      approvable: missingConfiguration.length === 0,
      artifacts: {
        coverage: binding("open_data", "coverage.json"),
        manifest: binding("open_data", "manifest.json"),
        objectInventory: inventory,
        parquet: {
          ...binding("query_table", "query-tables/pasco/query-table.parquet"),
          distinctPropertyIds: parquetValidation.distinctIds,
          nullPropertyIds: parquetValidation.nullIds,
          rowCount: parquetValidation.rows,
          schemaSha256,
        },
        provenance: binding("open_data", "provenance.json"),
        shards: shardRecords.map((shard) => ({
          byteSize: shard.bytes,
          objectKey: shard.objectKey,
          propertyCount: shard.propertyCount,
          sha256: shard.sha256,
        })),
      },
      configuration: {
        credentialsAvailable: targets.credentialsAvailable,
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
        activeProperties: scope.activeProperties,
        canonicalDocuments: entries.length,
        coordinateRows: parquetValidation.coordinateRows,
        inactiveProperties: scope.inactiveProperties,
        queryTableDistinctPropertyIds: parquetValidation.distinctIds,
        queryTableNullPropertyIds: parquetValidation.nullIds,
        queryTableRows: parquetValidation.rows,
      },
      county: "pasco",
      coverage: {
        authoritativeHeadSnapshotId: scope.authoritativeHeadSnapshotId,
        authoritySourceSystem: "pasco_appraiser",
        completenessResult:
          scope.coverage?.completeness.result ?? "not_applicable",
        entityType: "property_existence",
        mode: run.coverage_mode,
        predecessorChainSnapshotIds: scope.predecessorChainSnapshotIds,
        runId: run.run_id,
        scopeId: scope.scopeId,
        selection: {
          algorithm: run.sample_algorithm,
          seed: run.sample_seed,
          selectedRecordSha256: scope.selectedRecordSha256,
          selectionSize: run.selection_size,
        },
        sourceSnapshotId: run.snapshot_id,
        sourceSnapshotManifestSha256: scope.snapshot?.manifest_sha256 ?? null,
        workflowId: run.workflow_id,
      },
      executable: missingConfiguration.length === 0,
      exportMode: options.exportMode,
      fixtureExclusion: {
        fixturePropertyIdCount: fixturePropertyIds.size,
        matches: 0,
        passed: true,
      },
      freshness: {
        asOf,
        loadedAt,
        observedAt,
      },
      generatedAt,
      limitations: [
        run.coverage_mode === "sample"
          ? "Deterministic bounded appraisal/GIS sample; not complete Pasco coverage."
          : run.coverage_mode === "partial"
            ? "Partial source coverage; absence has no lifecycle meaning."
            : "Authoritative property-existence coverage does not make GIS, ownership, or building sources authoritative for deletion.",
        ...(run.snapshot_id === null
          ? [
              "Historical sample predates snapshot-bound ingestion; sourceSnapshotId remains explicitly null while exact run and selection identity are bound.",
            ]
          : []),
        "Related fact tables are not temporally versioned; only current normalized facts last seen in the selected active run are exported.",
        "Permit and contractor sources are unavailable; null aggregates do not mean zero real records.",
        "Sunbiz and BBB were not collected.",
        "No Filebase, IPFS, or IPNS effect was performed.",
      ],
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
        "Related fact tables lack lifecycle history; current run-matched facts are exported only for active selected properties.",
      version: "1.0.0",
    });
    await atomicWrite(
      path.join(outputRoot, "publication-dry-run-plan.json"),
      `${canonicalJson(plan)}\n`,
    );

    const finalMetrics = await directoryMetrics(outputRoot);
    const dryRunId = deterministicId("dryrun", [
      CONTRACT_VERSION,
      "publication-dry-run",
      run.run_id,
      openDataManifestSha256,
      queryTableSha256,
      plan.planSha256,
    ]);
    const publicationState = await recordPublicationPlan(
      options.databaseUrl,
      plan,
    );
    const summary: DryRunSummary = {
      activeProperties: scope.activeProperties,
      coverageMode: run.coverage_mode,
      dryRunId,
      inactiveProperties: scope.inactiveProperties,
      objectCount: finalMetrics.files,
      openDataBytes: openDataMetrics.bytes,
      openDataManifestSha256,
      outputRoot: path.relative(options.dataDir, outputRoot),
      planId: plan.planId,
      planSha256: plan.planSha256,
      publicationState,
      propertyCount: entries.length,
      queryTableBytes,
      queryTableDistinctIds: parquetValidation.distinctIds,
      queryTableRows: parquetValidation.rows,
      queryTableSha256,
      schemaSha256,
    };
    await sql`
      INSERT INTO oracle_publication_dry_runs (
        dry_run_id, run_id, county, status,
        open_data_manifest_sha256, query_table_sha256, plan_sha256,
        property_count, object_count, result, completed_at,
        plan_id, coverage_mode, scope_id, snapshot_id
      ) VALUES (
        ${dryRunId}, ${run.run_id}, 'pasco', 'validated',
        ${openDataManifestSha256}, ${queryTableSha256}, ${plan.planSha256},
        ${entries.length}, ${finalMetrics.files},
        ${sql.json(summary as unknown as postgres.JSONValue)}, now(),
        ${plan.planId}, ${run.coverage_mode}, ${scope.scopeId}, ${run.snapshot_id}
      )
      ON CONFLICT (run_id, county) DO UPDATE SET
        dry_run_id = EXCLUDED.dry_run_id,
        status = EXCLUDED.status,
        open_data_manifest_sha256 = EXCLUDED.open_data_manifest_sha256,
        query_table_sha256 = EXCLUDED.query_table_sha256,
        plan_sha256 = EXCLUDED.plan_sha256,
        plan_id = EXCLUDED.plan_id,
        coverage_mode = EXCLUDED.coverage_mode,
        scope_id = EXCLUDED.scope_id,
        snapshot_id = EXCLUDED.snapshot_id,
        property_count = EXCLUDED.property_count,
        object_count = EXCLUDED.object_count,
        result = EXCLUDED.result,
        completed_at = EXCLUDED.completed_at
    `;
    return summary;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
