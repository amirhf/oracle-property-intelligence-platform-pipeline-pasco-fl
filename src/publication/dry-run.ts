import { createHash } from "node:crypto";
import {
  mkdir,
  readdir,
  readFile,
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
import { deterministicId, sha256 } from "../lib/hash.js";

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
  result_counts: PilotRunSummary;
  run_id: string;
  workflow_id: string;
}

interface ManifestEntry {
  bytes: number;
  objectKey: string;
  propertyId: string;
  sha256: string;
}

interface DryRunSummary {
  dryRunId: string;
  objectCount: number;
  openDataBytes: number;
  openDataManifestSha256: string;
  outputRoot: string;
  planSha256: string;
  propertyCount: number;
  queryTableBytes: number;
  queryTableDistinctIds: number;
  queryTableRows: number;
  queryTableSha256: string;
  schemaSha256: string;
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

export async function buildPublicationDryRun(options: {
  dataDir: string;
  databaseUrl: string;
  runId: string;
}): Promise<DryRunSummary> {
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
  await rm(outputRoot, { force: true, recursive: true });
  await mkdir(outputRoot, { recursive: true });

  const sql = postgres(options.databaseUrl, { max: 1 });
  try {
    const runs = await sql<ExportRunRow[]>`
      SELECT run_id, workflow_id, as_of, completed_at, result_counts
      FROM oracle_pipeline_runs
      WHERE run_id = ${options.runId} AND status = 'completed'
    `;
    const run = runs[0];
    if (!run || !run.completed_at) {
      throw new Error("Publication requires a completed 25,000-property run");
    }
    if (run.result_counts.selectionSize !== 25_000) {
      throw new Error(
        "Publication dry run is restricted to the 25,000-property dataset",
      );
    }
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
      LEFT JOIN oracle_coordinates c USING (property_id)
      LEFT JOIN oracle_roof_signals r USING (property_id)
      LEFT JOIN LATERAL (
        SELECT actual_year_built, roof_cover, roof_structure
        FROM oracle_building_signals candidate
        WHERE candidate.property_id = p.property_id
        ORDER BY building_number, building_section, building_signal_id
        LIMIT 1
      ) b ON true
      WHERE p.last_seen_run_id = ${options.runId}
      ORDER BY p.property_id
    `;
    if (properties.length !== 25_000) {
      throw new Error(
        `Publication scope contains ${properties.length} properties, expected 25000`,
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
          sourceUrl: APPRAISER_URL,
          sourceArtifactUri: APPRAISER_ARTIFACT_URI,
          sourceRecordHash: property.source_record_hash,
          observedAt: "2026-08-23T00:00:00.000Z",
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
                sourceUrl: GIS_URL,
                sourceArtifactUri: GIS_ARTIFACT_URI,
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
          observedAt: "2026-08-23T00:00:00.000Z",
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
        observed_at: "2026-08-23T00:00:00.000Z",
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

    const provenance = {
      county: "pasco",
      sources: [
        {
          artifactUri: APPRAISER_ARTIFACT_URI,
          files: sourceArtifacts
            .filter((artifact) => artifact.source_system === "pasco_appraiser")
            .map((artifact) => ({
              path: path.relative(
                options.dataDir,
                fileURLToPath(artifact.local_uri),
              ),
              sha256: artifact.sha256,
            })),
          sourceSystem: "pasco_appraiser",
        },
        {
          artifactUri: GIS_ARTIFACT_URI,
          files: sourceArtifacts
            .filter((artifact) => artifact.source_system === "pasco_gis")
            .map((artifact) => ({
              path: path.relative(
                options.dataDir,
                fileURLToPath(artifact.local_uri),
              ),
              sha256: artifact.sha256,
            })),
          sourceSystem: "pasco_gis",
        },
      ],
      sourceWatermark: {
        appraiserObservedDate: "2026-08-23",
        runId: run.run_id,
        workflowId: run.workflow_id,
      },
      version: CONTRACT_VERSION,
    };
    for (const source of provenance.sources) {
      if (source.files.length === 0) {
        throw new Error(
          `Provenance has no local files for ${source.sourceSystem}`,
        );
      }
      for (const file of source.files) {
        await stat(path.join(options.dataDir, file.path));
      }
    }
    const coverage = {
      bbb: { availability: "unavailable", reason: "source_not_collected" },
      canonicalProperties: properties.length,
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
      scope: "deterministic 25,000-property appraisal/GIS sample",
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
      entries,
      generatedAt: loadedAt,
      propertyCount: entries.length,
      representation: "canonical-property-json-v1",
      shards: shardRecords,
      sourceRunId: run.run_id,
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
    const schemaSha256 = sha256(JSON.stringify(parquetValidation.schema));

    const openDataMetrics = await directoryMetrics(openDataRoot);
    const planBody = {
      artifactHashes: {
        canonicalSchema: await fileSha256(
          path.resolve("contracts/canonical-v1.schema.json"),
        ),
        openDataManifest: openDataManifestSha256,
        queryTable: queryTableSha256,
        queryTableSchema: schemaSha256,
      },
      artifacts: {
        openData: {
          bytes: openDataMetrics.bytes,
          objectCount: openDataMetrics.files,
          path: path.relative(options.dataDir, openDataRoot),
        },
        queryTable: {
          bytes: queryTableBytes,
          objectCount: 1,
          path: path.relative(options.dataDir, parquetPath),
          rows: parquetValidation.rows,
        },
      },
      buckets: {
        openData: "elephant-oracle-open-data-pasco",
        queryData:
          "elephant-oracle-query-table-pasco (provisional; owner confirmation pending)",
      },
      county: "pasco",
      expectedFilebaseVariables: [
        "FILEBASE_ACCESS_KEY",
        "FILEBASE_SECRET_KEY",
        "FILEBASE_ACCESS_KEY_PASCO (optional override)",
        "FILEBASE_SECRET_KEY_PASCO (optional override)",
        "FILEBASE_OPEN_DATA_BUCKET_PASCO",
        "FILEBASE_QUERY_TABLE_BUCKET_PASCO",
        "FILEBASE_QUERY_TABLE_IPNS_LABEL (optional)",
      ],
      fixtureExclusion: {
        fixturePropertyIdCount: fixturePropertyIds.size,
        matches: fixtureMatches,
        passed: fixtureMatches === 0,
      },
      intendedObjectKeys: {
        openData: [
          "index.json",
          "manifest.json",
          "coverage.json",
          "provenance.json",
          "permit-index.json",
          "run-summary.json",
          "properties/<property-id-suffix>/<property-id>.json",
          "shards/shard-<number>.json",
        ],
        queryData: ["query-tables/pasco/query-table.parquet"],
      },
      ipnsLabels: {
        openData: "oracle-open-data-pasco",
        queryData: "oracle-query-table-pasco",
      },
      ipnsMutationPerformed: false,
      limitations: [
        "Deterministic 25,000-property appraisal/GIS sample; not complete Pasco coverage.",
        "Permit and contractor sources are unavailable; null aggregates do not mean zero real records.",
        "Sunbiz and BBB were not collected.",
        "No Filebase credentials were required or populated.",
      ],
      propertyCount: entries.length,
      publishedCid: null,
      reconciliation: {
        databaseProperties: properties.length,
        distinctParquetPropertyIds: parquetValidation.distinctIds,
        exportedCanonicalDocuments: entries.length,
        manifestProperties: manifest.propertyCount,
        parquetRows: parquetValidation.rows,
        sourceAcceptedProperties: run.result_counts.acceptedProperties,
      },
      representation: "two-domain-local-dry-run-v1",
      sourceWatermark: {
        appraiserObservedDate: "2026-08-23",
        asOf,
        loadedAt,
        runId: run.run_id,
        workflowId: run.workflow_id,
      },
      sourceRunId: run.run_id,
    };
    const planSha256 = sha256(JSON.stringify(planBody));
    const plan = { ...planBody, planSha256 };
    await atomicWrite(
      path.join(outputRoot, "publication-dry-run-plan.json"),
      `${JSON.stringify(plan)}\n`,
    );
    const validation = {
      canonicalDocumentsValidated: entries.length,
      coordinateRows: parquetValidation.coordinateRows,
      distinctPropertyIds: parquetValidation.distinctIds,
      fixturePropertyIdCount: fixturePropertyIds.size,
      fixturePropertyIdsExcluded: fixtureMatches === 0,
      manifestPropertyCount: entries.length,
      nullPropertyIds: parquetValidation.nullIds,
      parquetRows: parquetValidation.rows,
      permitAggregateNonNullRows: parquetValidation.permitAggregateNonNullRows,
      provenanceReferencesResolved: true,
      roofSignalBasis: parquetValidation.roofBasis,
    };
    await atomicWrite(
      path.join(outputRoot, "validation.json"),
      `${JSON.stringify(validation)}\n`,
    );

    const finalMetrics = await directoryMetrics(outputRoot);
    const dryRunId = deterministicId("dryrun", [
      CONTRACT_VERSION,
      "publication-dry-run",
      run.run_id,
      openDataManifestSha256,
      queryTableSha256,
      planSha256,
    ]);
    const summary: DryRunSummary = {
      dryRunId,
      objectCount: finalMetrics.files,
      openDataBytes: openDataMetrics.bytes,
      openDataManifestSha256,
      outputRoot,
      planSha256,
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
        property_count, object_count, result, completed_at
      ) VALUES (
        ${dryRunId}, ${run.run_id}, 'pasco', 'validated',
        ${openDataManifestSha256}, ${queryTableSha256}, ${planSha256},
        ${entries.length}, ${finalMetrics.files},
        ${sql.json(summary as unknown as postgres.JSONValue)}, now()
      )
      ON CONFLICT (run_id, county) DO UPDATE SET
        dry_run_id = EXCLUDED.dry_run_id,
        status = EXCLUDED.status,
        open_data_manifest_sha256 = EXCLUDED.open_data_manifest_sha256,
        query_table_sha256 = EXCLUDED.query_table_sha256,
        plan_sha256 = EXCLUDED.plan_sha256,
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
