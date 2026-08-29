import { createHash } from "node:crypto";
import { readFile, realpath, stat } from "node:fs/promises";
import path from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";

import type {
  LocalArtifactProviderConfig,
  McpProviderConfig,
  PublicIpnsProviderConfig,
} from "./config.js";
import type { McpContractRegistry } from "./contracts.js";
import { validatePublicationPlan } from "../publication/plan.js";

export type JsonObject = Record<string, unknown>;

export interface QueryPropertyRow {
  canonicalPropertyId: string;
  latitude: number | null;
  longitude: number | null;
  maximumOpenRoofingPermitDays: number | null;
  observedAt: string;
  openRoofingPermitCount: number | null;
  propertyDocumentSha256: string;
  propertyId: string;
  publishedAt: string | null;
  roofAgeBasis: string;
  roofAgeBasisQuality: string;
  roofAgeYears: number;
  siteCity: string;
}

export interface DatasetMetadata {
  artifactCids: string[];
  asOf: string;
  canonicalDocumentCount: number;
  completedAt: string;
  coordinateCount: number;
  contractorCoverage: "available" | "partial" | "unavailable";
  datasetVersion: string;
  fixtureMatches: number;
  manifestSha256: string;
  objectCount: number;
  parquetSha256: string;
  plan: JsonObject;
  providerMode: McpProviderConfig["mode"];
  permitCoverage: "available" | "partial" | "unavailable";
  runId: string;
  runSummary: JsonObject;
  startedAt: string;
  workflowId: string;
}

export interface OracleMcpProvider {
  getCanonicalProperty(
    propertyId: string,
    signal?: AbortSignal,
  ): Promise<JsonObject | null>;
  getMetadata(signal?: AbortSignal): Promise<DatasetMetadata>;
  getPermit(permitId: string, signal?: AbortSignal): Promise<JsonObject | null>;
  getQueryRows(signal?: AbortSignal): Promise<readonly QueryPropertyRow[]>;
}

function ensureNotAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error("Operation aborted");
}

function record(value: unknown, label: string): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Publication ${label} is invalid`);
  }
  return value as JsonObject;
}

function stringValue(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Publication ${label} is invalid`);
  }
  return value;
}

function numberValue(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Publication ${label} is invalid`);
  }
  return value;
}

async function readJson(filePath: string, label: string): Promise<JsonObject> {
  try {
    return record(JSON.parse(await readFile(filePath, "utf8")), label);
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error(`Publication ${label} is not valid JSON`, {
        cause: error,
      });
    }
    throw error;
  }
}

async function sha256File(filePath: string): Promise<string> {
  return createHash("sha256")
    .update(await readFile(filePath))
    .digest("hex");
}

function inside(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}${path.sep}`);
}

function rejectsFixturePath(target: string): boolean {
  const segments = target.split(path.sep);
  return (
    segments.includes("fixtures") ||
    target.endsWith("fixture.json") ||
    target.endsWith("fixtures.json")
  );
}

export async function resolveArtifactPath(
  dataDir: string,
  configuredPath: string,
): Promise<string> {
  const root = await realpath(dataDir);
  const candidate = path.isAbsolute(configuredPath)
    ? path.resolve(configuredPath)
    : path.resolve(root, configuredPath);
  if (!inside(root, candidate) || rejectsFixturePath(candidate)) {
    throw new Error(
      "Configured MCP artifact path is outside DATA_DIR or forbidden",
    );
  }
  let resolved: string;
  try {
    resolved = await realpath(candidate);
  } catch {
    throw new Error("Configured MCP artifact does not exist");
  }
  if (!inside(root, resolved) || rejectsFixturePath(resolved)) {
    throw new Error(
      "Resolved MCP artifact path is outside DATA_DIR or forbidden",
    );
  }
  const metadata = await stat(resolved);
  if (!metadata.isFile())
    throw new Error("Configured MCP artifact is not a file");
  return resolved;
}

export async function verifyParquetMagic(parquetPath: string): Promise<void> {
  const bytes = await readFile(parquetPath);
  if (
    bytes.length < 8 ||
    bytes.subarray(0, 4).toString("ascii") !== "PAR1" ||
    bytes.subarray(-4).toString("ascii") !== "PAR1"
  ) {
    throw new Error("Configured MCP Parquet artifact is corrupt");
  }
}

function sqlPath(value: string): string {
  return value.replaceAll("'", "''");
}

function publicPropertyId(canonicalPropertyId: string): string {
  const match = canonicalPropertyId.match(/^property_([a-f0-9]{32})$/);
  if (!match?.[1])
    throw new Error("Publication contains an invalid property ID");
  return `prop_${match[1]}`;
}

function canonicalPropertyId(propertyId: string): string {
  return `property_${propertyId.slice("prop_".length)}`;
}

function iso(value: unknown, label: string): string {
  const text = stringValue(value, label);
  const timestamp = Date.parse(text);
  if (!Number.isFinite(timestamp))
    throw new Error(`Publication ${label} is invalid`);
  return new Date(timestamp).toISOString();
}

function asNullableNumber(value: unknown, label: string): number | null {
  if (value === null) return null;
  return numberValue(value, label);
}

function asNullableString(value: unknown, label: string): string | null {
  if (value === null) return null;
  return stringValue(value, label);
}

interface ManifestEntry {
  objectKey: string;
  propertyId: string;
  sha256: string;
}

export class LocalArtifactProvider implements OracleMcpProvider {
  readonly #contracts: McpContractRegistry;
  readonly #entriesByPublicId: Map<string, ManifestEntry>;
  readonly #metadata: DatasetMetadata;
  readonly #openDataRoot: string;
  readonly #provenanceArtifactUris: Set<string>;
  readonly #queryRows: readonly QueryPropertyRow[];

  private constructor(options: {
    contracts: McpContractRegistry;
    entriesByPublicId: Map<string, ManifestEntry>;
    metadata: DatasetMetadata;
    openDataRoot: string;
    provenanceArtifactUris: Set<string>;
    queryRows: QueryPropertyRow[];
  }) {
    this.#contracts = options.contracts;
    this.#entriesByPublicId = options.entriesByPublicId;
    this.#metadata = options.metadata;
    this.#openDataRoot = options.openDataRoot;
    this.#provenanceArtifactUris = options.provenanceArtifactUris;
    this.#queryRows = options.queryRows;
  }

  static async create(
    config: LocalArtifactProviderConfig,
    contracts: McpContractRegistry,
  ): Promise<LocalArtifactProvider> {
    const manifestPath = await resolveArtifactPath(
      config.dataDir,
      config.manifestPath,
    );
    const parquetPath = await resolveArtifactPath(
      config.dataDir,
      config.parquetPath,
    );
    if (path.basename(manifestPath) !== "manifest.json") {
      throw new Error("MCP_LOCAL_MANIFEST_PATH must identify manifest.json");
    }
    if (path.extname(parquetPath) !== ".parquet") {
      throw new Error(
        "MCP_LOCAL_PARQUET_PATH must identify a Parquet artifact",
      );
    }
    await verifyParquetMagic(parquetPath);

    const openDataRoot = path.dirname(manifestPath);
    const publicationRoot = path.dirname(openDataRoot);
    if (!inside(publicationRoot, parquetPath)) {
      throw new Error(
        "Manifest and Parquet must belong to one publication set",
      );
    }
    const planPath = await resolveArtifactPath(
      config.dataDir,
      path.join(publicationRoot, "publication-dry-run-plan.json"),
    );
    const validationPath = await resolveArtifactPath(
      config.dataDir,
      path.join(publicationRoot, "validation.json"),
    );
    const coveragePath = await resolveArtifactPath(
      config.dataDir,
      path.join(openDataRoot, "coverage.json"),
    );
    const provenancePath = await resolveArtifactPath(
      config.dataDir,
      path.join(openDataRoot, "provenance.json"),
    );
    const runSummaryPath = await resolveArtifactPath(
      config.dataDir,
      path.join(openDataRoot, "run-summary.json"),
    );

    const [manifest, plan, validation, coverage, provenance, runSummary] =
      await Promise.all([
        readJson(manifestPath, "manifest"),
        readJson(planPath, "plan"),
        readJson(validationPath, "validation"),
        readJson(coveragePath, "coverage"),
        readJson(provenancePath, "provenance"),
        readJson(runSummaryPath, "run summary"),
      ]);
    const manifestSha256 = await sha256File(manifestPath);
    const parquetSha256 = await sha256File(parquetPath);
    const publicationPlan = validatePublicationPlan(plan);
    if (
      publicationPlan.artifacts.manifest.sha256 !== manifestSha256 ||
      publicationPlan.artifacts.parquet.sha256 !== parquetSha256
    ) {
      throw new Error("Publication artifact hash validation failed");
    }
    const fixtureExclusion = publicationPlan.fixtureExclusion;
    if (fixtureExclusion.passed !== true || fixtureExclusion.matches !== 0) {
      throw new Error("Publication fixture isolation validation failed");
    }
    if (validation.fixturePropertyIdsExcluded !== true) {
      throw new Error(
        "Publication validation does not prove fixture isolation",
      );
    }

    const entriesValue = manifest.entries;
    if (!Array.isArray(entriesValue) || manifest.propertyCount !== 25_000) {
      throw new Error(
        "Local MCP provider requires the validated 25,000-row manifest",
      );
    }
    const entriesByPublicId = new Map<string, ManifestEntry>();
    for (const value of entriesValue) {
      const entry = record(value, "manifest entry");
      const parsed: ManifestEntry = {
        objectKey: stringValue(entry.objectKey, "manifest object key"),
        propertyId: stringValue(entry.propertyId, "manifest property ID"),
        sha256: stringValue(entry.sha256, "manifest entry hash"),
      };
      if (
        path.isAbsolute(parsed.objectKey) ||
        parsed.objectKey.split("/").includes("..") ||
        parsed.objectKey.includes("fixtures") ||
        !/^properties\/[a-f0-9]{2}\/property_[a-f0-9]{32}\.json$/.test(
          parsed.objectKey,
        ) ||
        !/^[a-f0-9]{64}$/.test(parsed.sha256)
      ) {
        throw new Error("Publication manifest contains an unsafe entry");
      }
      const publicId = publicPropertyId(parsed.propertyId);
      if (entriesByPublicId.has(publicId)) {
        throw new Error("Publication manifest contains duplicate property IDs");
      }
      entriesByPublicId.set(publicId, parsed);
    }
    if (entriesByPublicId.size !== 25_000) {
      throw new Error("Publication manifest cardinality is invalid");
    }

    const queryRows = await readQueryRows(parquetPath);
    if (queryRows.length !== entriesByPublicId.size) {
      throw new Error("Parquet and manifest cardinality differ");
    }
    const queryIds = new Set(queryRows.map((row) => row.propertyId));
    if (
      queryIds.size !== entriesByPublicId.size ||
      [...entriesByPublicId.keys()].some(
        (propertyId) => !queryIds.has(propertyId),
      )
    ) {
      throw new Error("Parquet and manifest property identifiers differ");
    }
    for (const row of queryRows) {
      if (
        entriesByPublicId.get(row.propertyId)?.sha256 !==
        row.propertyDocumentSha256
      ) {
        throw new Error("Parquet property hashes do not match the manifest");
      }
    }
    const coordinateCount = queryRows.filter(
      (row) => row.latitude !== null && row.longitude !== null,
    ).length;
    if (
      coordinateCount !== 24_995 ||
      queryRows.some(
        (row) =>
          row.roofAgeBasis !== "year_built_proxy" ||
          row.roofAgeBasisQuality !== "proxy" ||
          row.openRoofingPermitCount !== null ||
          row.maximumOpenRoofingPermitDays !== null,
      )
    ) {
      throw new Error(
        "Publication coverage semantics do not match the validated pilot",
      );
    }
    const coveragePermits = record(coverage.permits, "permit coverage");
    const coverageContractors = record(
      coverage.contractors,
      "contractor coverage",
    );
    if (
      coveragePermits.availability !== "unavailable" ||
      coverageContractors.availability !== "unavailable"
    ) {
      throw new Error("Local publication source coverage is inconsistent");
    }

    const provenanceSources = provenance.sources;
    if (!Array.isArray(provenanceSources)) {
      throw new Error("Publication provenance sources are invalid");
    }
    const provenanceArtifactUris = new Set(
      provenanceSources.map((source) =>
        stringValue(
          record(source, "provenance source").artifactUri,
          "artifact URI",
        ),
      ),
    );
    const sourceWatermark = {
      appraiserObservedDate: publicationPlan.freshness.observedAt.slice(0, 10),
      asOf: publicationPlan.freshness.asOf,
      loadedAt: publicationPlan.freshness.loadedAt,
      runId: publicationPlan.coverage.runId,
      workflowId: publicationPlan.coverage.workflowId,
    };
    const reconciliation = publicationPlan.counts;
    const resultCounts = record(runSummary.resultCounts, "run counts");
    const completedAt = iso(sourceWatermark.loadedAt, "loaded timestamp");
    const elapsedMs = numberValue(
      resultCounts.elapsedMs,
      "elapsed milliseconds",
    );
    const runId = stringValue(runSummary.runId, "run ID");
    const workflowId = stringValue(runSummary.workflowId, "workflow ID");
    if (
      runId !== manifest.sourceRunId ||
      runId !== sourceWatermark.runId ||
      workflowId !== sourceWatermark.workflowId ||
      reconciliation.queryTableDistinctPropertyIds !== 25_000
    ) {
      throw new Error("Publication source watermark reconciliation failed");
    }

    return new LocalArtifactProvider({
      contracts,
      entriesByPublicId,
      metadata: {
        artifactCids: [],
        asOf: iso(sourceWatermark.asOf, "as-of timestamp"),
        canonicalDocumentCount: entriesByPublicId.size,
        completedAt,
        coordinateCount,
        contractorCoverage: "unavailable",
        datasetVersion: `pasco-25k-${publicationPlan.planSha256.slice(0, 16)}`,
        fixtureMatches: 0,
        manifestSha256,
        objectCount: publicationPlan.artifacts.objectInventory.length,
        parquetSha256,
        plan: { sourceWatermark },
        providerMode: "local-artifact",
        permitCoverage: "unavailable",
        runId,
        runSummary,
        startedAt: new Date(Date.parse(completedAt) - elapsedMs).toISOString(),
        workflowId,
      },
      openDataRoot,
      provenanceArtifactUris,
      queryRows,
    });
  }

  async getCanonicalProperty(
    propertyId: string,
    signal?: AbortSignal,
  ): Promise<JsonObject | null> {
    ensureNotAborted(signal);
    const entry = this.#entriesByPublicId.get(propertyId);
    if (!entry) return null;
    if (entry.propertyId !== canonicalPropertyId(propertyId)) {
      throw new Error("Publication property identifier mapping is invalid");
    }
    const candidate = path.resolve(this.#openDataRoot, entry.objectKey);
    if (!inside(this.#openDataRoot, candidate)) {
      throw new Error("Publication property path escaped its root");
    }
    const resolved = await realpath(candidate);
    if (!inside(this.#openDataRoot, resolved)) {
      throw new Error("Publication property path escaped through a link");
    }
    const bytes = await readFile(resolved);
    if (createHash("sha256").update(bytes).digest("hex") !== entry.sha256) {
      throw new Error("Publication property hash validation failed");
    }
    const property = record(
      JSON.parse(bytes.toString("utf8")),
      "canonical property",
    );
    if (property.propertyId !== entry.propertyId) {
      throw new Error(
        "Publication property document ID does not match its manifest",
      );
    }
    const failures = this.#contracts.validateCanonical(property);
    if (failures.length > 0) {
      throw new Error(
        "Publication property does not satisfy the canonical contract",
      );
    }
    this.#verifyPropertyProvenance(property);
    ensureNotAborted(signal);
    return property;
  }

  async getMetadata(signal?: AbortSignal): Promise<DatasetMetadata> {
    ensureNotAborted(signal);
    return this.#metadata;
  }

  async getPermit(
    _permitId: string,
    signal?: AbortSignal,
  ): Promise<JsonObject | null> {
    ensureNotAborted(signal);
    return null;
  }

  async getQueryRows(
    signal?: AbortSignal,
  ): Promise<readonly QueryPropertyRow[]> {
    ensureNotAborted(signal);
    return this.#queryRows;
  }

  #verifyPropertyProvenance(property: JsonObject): void {
    const evidence = property.evidence;
    if (!Array.isArray(evidence) || evidence.length === 0) {
      throw new Error("Publication property evidence is missing");
    }
    const evidenceIds = new Set<string>();
    for (const value of evidence) {
      const item = record(value, "property evidence");
      const evidenceId = stringValue(item.evidenceId, "evidence ID");
      const artifactUri = stringValue(
        item.sourceArtifactUri,
        "evidence artifact URI",
      );
      const sourceRecordHash = stringValue(
        item.sourceRecordHash,
        "evidence record hash",
      );
      if (
        !this.#provenanceArtifactUris.has(artifactUri) ||
        !/^sha256:[a-f0-9]{64}$/.test(sourceRecordHash)
      ) {
        throw new Error("Publication property evidence does not resolve");
      }
      evidenceIds.add(evidenceId);
    }
    const references = new Set<string>();
    collectEvidenceReferences(property, references);
    if ([...references].some((reference) => !evidenceIds.has(reference))) {
      throw new Error("Publication property fact evidence does not resolve");
    }
  }
}

function collectEvidenceReferences(
  value: unknown,
  references: Set<string>,
): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectEvidenceReferences(entry, references);
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    if (key === "evidenceRefs" && Array.isArray(entry)) {
      for (const reference of entry) {
        if (typeof reference === "string") references.add(reference);
      }
    } else {
      collectEvidenceReferences(entry, references);
    }
  }
}

async function readQueryRows(parquetPath: string): Promise<QueryPropertyRow[]> {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    const reader = await connection.runAndReadAll(`
      SELECT
        property_id,
        latitude,
        longitude,
        roof_age_years,
        roof_age_basis,
        roof_age_basis_quality,
        open_roofing_permit_count,
        maximum_open_roofing_permit_days,
        property_document_sha256,
        site_city,
        observed_at,
        published_at
      FROM read_parquet('${sqlPath(parquetPath)}')
      ORDER BY property_id
    `);
    return reader.getRowObjectsJson().map((value) => {
      const row = record(value, "Parquet row");
      const canonicalId = stringValue(row.property_id, "Parquet property ID");
      return {
        canonicalPropertyId: canonicalId,
        latitude: asNullableNumber(row.latitude, "latitude"),
        longitude: asNullableNumber(row.longitude, "longitude"),
        maximumOpenRoofingPermitDays: asNullableNumber(
          row.maximum_open_roofing_permit_days,
          "maximum permit duration",
        ),
        observedAt: iso(row.observed_at, "observed timestamp"),
        openRoofingPermitCount: asNullableNumber(
          row.open_roofing_permit_count,
          "open permit count",
        ),
        propertyDocumentSha256: stringValue(
          row.property_document_sha256,
          "property document hash",
        ),
        propertyId: publicPropertyId(canonicalId),
        publishedAt: asNullableString(row.published_at, "published timestamp"),
        roofAgeBasis: stringValue(row.roof_age_basis, "roof age basis"),
        roofAgeBasisQuality: stringValue(
          row.roof_age_basis_quality,
          "roof age quality",
        ),
        roofAgeYears: numberValue(row.roof_age_years, "roof age years"),
        siteCity: stringValue(row.site_city, "site city"),
      };
    });
  } finally {
    connection.closeSync();
  }
}

export class PublicIpnsProvider implements OracleMcpProvider {
  constructor(readonly config: PublicIpnsProviderConfig) {}

  static create(config: PublicIpnsProviderConfig): never {
    void config;
    throw new Error(
      "Public IPNS/IPFS publication is not configured for this local checkpoint",
    );
  }

  getCanonicalProperty(): Promise<JsonObject | null> {
    throw new Error("Public IPNS/IPFS provider is unavailable");
  }

  getMetadata(): Promise<DatasetMetadata> {
    throw new Error("Public IPNS/IPFS provider is unavailable");
  }

  getPermit(): Promise<JsonObject | null> {
    throw new Error("Public IPNS/IPFS provider is unavailable");
  }

  getQueryRows(): Promise<readonly QueryPropertyRow[]> {
    throw new Error("Public IPNS/IPFS provider is unavailable");
  }
}

export async function createMcpProvider(
  config: McpProviderConfig,
  contracts: McpContractRegistry,
): Promise<OracleMcpProvider> {
  return config.mode === "local-artifact"
    ? LocalArtifactProvider.create(config, contracts)
    : PublicIpnsProvider.create(config);
}
