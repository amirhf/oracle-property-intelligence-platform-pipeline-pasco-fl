import { createHash } from "node:crypto";

import type { McpLimits } from "./config.js";
import {
  MAX_CURSOR_BYTES,
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
  MCP_SERVICE_VERSION,
  MCP_TOOL_NAMES,
  type McpToolName,
} from "./constants.js";
import type { McpContractRegistry } from "./contracts.js";
import type {
  DatasetMetadata,
  JsonObject,
  OracleMcpProvider,
  QueryPropertyRow,
} from "./provider.js";
import { PublicReadError } from "./public-ipns-provider.js";
import { projectPublicOwnership } from "./ownership.js";

type ErrorCode =
  | "invalid_argument"
  | "invalid_cursor"
  | "not_found"
  | "county_not_served"
  | "data_unavailable"
  | "dependency_unavailable"
  | "rate_limited"
  | "deadline_exceeded"
  | "internal";

export interface ToolExecutionResult {
  isError: boolean;
  result: JsonObject;
}

interface CoordinateCenter {
  kind: "coordinates";
  latitude: number;
  longitude: number;
}

interface PlaceCenter {
  kind: "place";
  text: string;
}

interface SearchArguments {
  asOf?: string;
  center: CoordinateCenter | PlaceCenter;
  county: "pasco";
  filters: {
    freshness?: {
      observedAtOrAfter?: string;
      publishedAtOrAfter?: string;
    };
    matchMode?: "all" | "any";
    ownership?: {
      operator?: "gt" | "gte";
      ownerArea?: "any" | "out_of_county" | "out_of_state";
      years?: number;
    };
    permit?: {
      minOpenDays?: number;
      openOnly?: boolean;
      roofingOnly?: boolean;
    };
    roofAge?: {
      basis: "direct_only" | "direct_or_proxy";
      operator: "gt" | "gte";
      years: number;
    };
  };
  page: { cursor?: string; limit: number };
  radius: { unit: "mi" | "km"; value: number };
  sort: "distance_asc" | "roof_age_desc" | "permit_open_days_desc";
}

interface SearchCandidate {
  distanceMeters: number;
  matchReasons: string[];
  row: QueryPropertyRow;
}

const PROPERTY_HYDRATION_CONCURRENCY = 8;

async function mapWithConcurrency<Input, Output>(
  values: readonly Input[],
  concurrency: number,
  project: (value: Input) => Promise<Output>,
): Promise<Output[]> {
  const output = new Array<Output>(values.length);
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      for (;;) {
        const index = cursor;
        cursor += 1;
        if (index >= values.length) return;
        output[index] = await project(values[index]!);
      }
    }),
  );
  return output;
}

class RuntimeFailure extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly retryable = false,
    readonly dependency?: string,
    readonly details?: JsonObject,
  ) {
    super(message);
  }
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function stableJson(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function requestId(tool: McpToolName, argumentsValue: unknown): string {
  return `request_${sha256(`${tool}:${stableJson(argumentsValue)}`).slice(0, 24)}`;
}

function errorResult(
  tool: McpToolName,
  argumentsValue: unknown,
  failure: RuntimeFailure,
): JsonObject {
  return {
    ok: false,
    error: {
      code: failure.code,
      message: failure.message,
      retryable: failure.retryable,
      ...(failure.dependency ? { dependency: failure.dependency } : {}),
      ...(failure.details ? { details: failure.details } : {}),
    },
    meta: {
      contractVersion: MCP_CONTRACT_VERSION,
      schemaHash: MCP_SCHEMA_SHA256,
      requestId: requestId(tool, argumentsValue),
    },
  };
}

function availableFact(
  value: unknown,
  classification: "raw" | "normalized" | "derived" | "inferred",
  evidenceRefs: string[],
  derivation?: JsonObject,
): JsonObject {
  return {
    availability: "available",
    value,
    class: classification,
    evidenceRefs,
    ...(derivation ? { derivation } : {}),
  };
}

function unavailableFact(
  classification: "raw" | "normalized" | "derived" | "inferred",
  reason:
    | "not_provided_by_source"
    | "source_not_collected"
    | "source_unavailable"
    | "not_applicable"
    | "ambiguous_match",
  evidenceRefs: string[],
): JsonObject {
  return {
    availability: "unavailable",
    value: null,
    class: classification,
    reason,
    evidenceRefs,
  };
}

function responseMeta(
  metadata: DatasetMetadata,
  asOf = metadata.asOf,
  nextCursor: string | null = null,
): JsonObject {
  return {
    contractVersion: MCP_CONTRACT_VERSION,
    schemaHash: MCP_SCHEMA_SHA256,
    county: "pasco",
    asOf,
    artifactCids: metadata.artifactCids,
    nextCursor,
  };
}

function record(value: unknown, label: string): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new RuntimeFailure(
      "data_unavailable",
      `Published ${label} is unavailable or invalid.`,
      false,
      "local-publication",
    );
  }
  return value as JsonObject;
}

function evidenceIds(property: JsonObject): string[] {
  const evidence = property.evidence;
  if (!Array.isArray(evidence)) return [];
  return evidence.flatMap((entry) => {
    const item = entry as JsonObject;
    return typeof item.evidenceId === "string" ? [item.evidenceId] : [];
  });
}

function freshness(value: unknown): JsonObject {
  const source = record(value, "freshness");
  return {
    observedAt: source.observedAt ?? null,
    retrievedAt: source.retrievedAt,
    loadedAt: source.loadedAt,
    publishedAt: source.publishedAt ?? null,
    computedAt: source.computedAt,
    sourceCadence: source.sourceCadence ?? null,
    cadenceStatus: "within_source_cadence",
  };
}

export function projectPublicProperty(
  canonical: JsonObject,
  publicId: string,
): JsonObject {
  const refs = evidenceIds(canonical);
  const permits = Array.isArray(canonical.permits)
    ? canonical.permits.map((permit) => {
        const value = structuredClone(permit) as JsonObject;
        value.propertyId = publicId;
        return value;
      })
    : [];
  return {
    propertyId: publicId,
    county: "pasco",
    folio: structuredClone(canonical.folio),
    address: structuredClone(canonical.situsAddress),
    coordinates: structuredClone(canonical.coordinates),
    yearBuilt: structuredClone(canonical.yearBuilt),
    roofInstallationDate: structuredClone(canonical.roofInstallationDate),
    roofAgeSignal: structuredClone(canonical.roofAgeSignal),
    ownershipDurationYears: unavailableFact(
      "derived",
      "not_provided_by_source",
      refs.slice(0, 1),
    ),
    ownerArea: unavailableFact(
      "derived",
      "not_provided_by_source",
      refs.slice(0, 1),
    ),
    ownership: projectPublicOwnership(canonical),
    openRoofingPermitCount: unavailableFact(
      "derived",
      "source_unavailable",
      [],
    ),
    maximumOpenRoofingPermitDays: unavailableFact(
      "derived",
      "source_unavailable",
      [],
    ),
    permits,
    evidence: structuredClone(canonical.evidence),
    freshness: freshness(canonical.freshness),
  };
}

function queryCapabilities(): JsonObject {
  return {
    searchableFields: [
      "coordinates",
      "roofAgeSignal.ageYears",
      "roofAgeSignal.basis",
      "permit.isOpen",
      "permit.openDurationDays",
      "ownershipDurationYears",
      "ownerArea",
      "freshness.observedAt",
      "freshness.publishedAt",
    ],
    filterBounds: {
      radius: {
        units: ["mi", "km"],
        exclusiveMinimum: 0,
        maximumMiles: 50,
        maximumKilometers: 80.4672,
      },
      roofAgeYears: {
        minimum: 0,
        maximum: 100,
        operators: ["gt", "gte"],
        bases: ["direct_only", "direct_or_proxy"],
      },
      permitOpenDays: { minimum: 0, maximum: 36_500 },
      ownershipYears: {
        minimum: 0,
        maximum: 500,
        operators: ["gt", "gte"],
      },
    },
    allowedSortKeys: ["distance_asc", "roof_age_desc", "permit_open_days_desc"],
    pagination: {
      minimumLimit: 1,
      maximumLimit: 100,
      cursor: "opaque",
      stableOrdering: true,
    },
    roofAgeSemantics: {
      actualBasis: "roof_installation_date",
      proxyBases: [
        "roof_permit_completion",
        "final_inspection",
        "roof_permit_issue",
        "year_built_proxy",
      ],
      yearBuiltProxyIsActualRoofAge: false,
    },
    coverageSemantics: {
      coordinates: {
        missingExcludedFromRadius: true,
        directLookupPreservesUnavailable: true,
      },
      permits: {
        unavailableDistinctFromZeroMatches: true,
        aggregatesNullWhenUnavailable: true,
      },
      contractors: { unavailableDistinctFromNoMatch: true },
    },
    queryRestrictions: {
      arbitrarySql: false,
      arbitraryFilesystemPaths: false,
      databaseAccess: false,
      publicationMutation: false,
      internalSchemaExposure: false,
    },
  };
}

function sourceCounts(resultCounts: JsonObject): JsonObject[] {
  const values = record(resultCounts.sourceCounts, "source counts");
  const sources = [
    ["pasco_appraiser_parcel", "parcel"],
    ["pasco_appraiser_building", "building"],
    ["pasco_appraiser_owners", "owners"],
    ["pasco_appraiser_site_addresses", "siteAddresses"],
  ] as const;
  const result: JsonObject[] = sources.map(([sourceSystem, key]) => {
    const count = record(values[key], `${key} counts`);
    return {
      sourceSystem,
      sourceRecords: count.source,
      parsedRecords: count.parsed,
      acceptedRecords: count.accepted,
      rejectedRecords: count.rejected,
    };
  });
  const selectionSize = resultCounts.selectionSize as number;
  const coordinates = resultCounts.coordinates as number;
  result.push({
    sourceSystem: "pasco_gis",
    sourceRecords: selectionSize,
    parsedRecords: selectionSize,
    acceptedRecords: coordinates,
    rejectedRecords: selectionSize - coordinates,
  });
  return result;
}

function pipelineSummary(metadata: DatasetMetadata): JsonObject {
  const resultCounts = record(metadata.runSummary.resultCounts, "run counts");
  const properties = resultCounts.acceptedProperties as number;
  const coordinates = resultCounts.coordinates as number;
  const roofSignals = resultCounts.roofSignals as number;
  return {
    county: "pasco",
    runId: metadata.runId,
    workflowId: metadata.workflowId,
    status: "completed",
    startedAt: metadata.startedAt,
    completedAt: metadata.completedAt,
    sourceCounts: sourceCounts(resultCounts),
    reconciliationCounts: {
      canonicalProperties: properties,
      distinctPropertyIds: metadata.canonicalDocumentCount,
      newRecords: resultCounts.newProperties,
      changedRecords: resultCounts.changedProperties,
      unchangedRecords: resultCounts.unchangedProperties,
      duplicateRecords: resultCounts.duplicateProperties,
    },
    coverage: {
      properties: { available: properties, unavailable: 0 },
      coordinates: {
        available: coordinates,
        unavailable: properties - coordinates,
      },
      roofSignals: {
        available: roofSignals,
        unavailable: properties - roofSignals,
        direct: 0,
        proxy: roofSignals,
      },
      permits: {
        status: "unavailable",
        recordCount: null,
        propertyCount: null,
        unavailableReason: "source_unavailable",
      },
      contractors: {
        status: "unavailable",
        recordCount: null,
        propertyCount: null,
        unavailableReason: "source_unavailable",
      },
    },
    publicationArtifacts: {
      status: "dry_run_validated",
      datasetVersion: metadata.datasetVersion,
      artifactCount: metadata.objectCount,
      publishedAt: null,
      artifactCids: metadata.artifactCids,
    },
    unavailableSources: [
      {
        sourceSystem: "permits",
        reason: "source_unavailable",
        message:
          "Official permit source coverage is unavailable for this dataset.",
      },
      {
        sourceSystem: "contractors",
        reason: "source_unavailable",
        message: "Contractor coverage is unavailable with the permit source.",
      },
      {
        sourceSystem: "sunbiz",
        reason: "source_not_collected",
        message: "Sunbiz was not collected for this publication checkpoint.",
      },
      {
        sourceSystem: "bbb",
        reason: "source_not_collected",
        message: "BBB was not collected for this publication checkpoint.",
      },
    ],
  };
}

function serviceFreshness(metadata: DatasetMetadata): JsonObject {
  const planWatermark = record(
    metadata.plan.sourceWatermark,
    "source watermark",
  );
  return {
    observedAt: `${String(planWatermark.appraiserObservedDate)}T00:00:00.000Z`,
    retrievedAt: metadata.asOf,
    loadedAt: metadata.completedAt,
    publishedAt: null,
    computedAt: metadata.asOf,
    sourceCadence: "weekly appraiser working roll",
    cadenceStatus: "within_source_cadence",
  };
}

export function haversineMeters(
  leftLatitude: number,
  leftLongitude: number,
  rightLatitude: number,
  rightLongitude: number,
): number {
  const radians = (degrees: number) => (degrees * Math.PI) / 180;
  const latitudeDelta = radians(rightLatitude - leftLatitude);
  const longitudeDelta = radians(rightLongitude - leftLongitude);
  const left = radians(leftLatitude);
  const right = radians(rightLatitude);
  const value =
    Math.sin(latitudeDelta / 2) ** 2 +
    Math.cos(left) * Math.cos(right) * Math.sin(longitudeDelta / 2) ** 2;
  return 6_371_008.8 * 2 * Math.atan2(Math.sqrt(value), Math.sqrt(1 - value));
}

function normalizePlace(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\bfl(orida)?\b/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function resolveCenter(
  center: SearchArguments["center"],
  rows: readonly QueryPropertyRow[],
  metadata: DatasetMetadata,
): { fact: JsonObject; latitude: number; longitude: number } {
  if (center.kind === "coordinates") {
    return {
      fact: availableFact(
        {
          latitude: center.latitude,
          longitude: center.longitude,
          crs: "EPSG:4326",
        },
        "normalized",
        ["request:search-center"],
        {
          rule: "request_coordinates",
          ruleVersion: MCP_CONTRACT_VERSION,
          inputs: ["search.center.latitude", "search.center.longitude"],
        },
      ),
      latitude: center.latitude,
      longitude: center.longitude,
    };
  }
  const place = normalizePlace(center.text);
  const matching = rows.filter(
    (row) =>
      row.latitude !== null &&
      row.longitude !== null &&
      normalizePlace(row.siteCity) === place,
  );
  if (matching.length === 0) {
    throw new RuntimeFailure(
      "not_found",
      "The requested place is not represented by a deterministic city centroid in this dataset.",
    );
  }
  const latitude =
    matching.reduce((sum, row) => sum + (row.latitude ?? 0), 0) /
    matching.length;
  const longitude =
    matching.reduce((sum, row) => sum + (row.longitude ?? 0), 0) /
    matching.length;
  return {
    fact: availableFact(
      { latitude, longitude, crs: "EPSG:4326" },
      "derived",
      [`publication:${metadata.manifestSha256}`],
      {
        rule: "published_city_coordinate_centroid",
        ruleVersion: MCP_CONTRACT_VERSION,
        asOf: metadata.asOf,
        inputs: ["queryTable.site_city", "queryTable.coordinates"],
      },
    ),
    latitude,
    longitude,
  };
}

function compare(
  operator: "gt" | "gte",
  value: number,
  bound: number,
): boolean {
  return operator === "gt" ? value > bound : value >= bound;
}

function candidateReasons(
  row: QueryPropertyRow,
  filters: SearchArguments["filters"],
): { eligible: boolean; reasons: string[] } {
  const conditions: boolean[] = [];
  const reasons: string[] = [];
  if (filters.roofAge) {
    const basisMatches =
      filters.roofAge.basis === "direct_or_proxy" ||
      row.roofAgeBasisQuality === "direct";
    const matches =
      basisMatches &&
      row.roofAgeYears !== null &&
      compare(
        filters.roofAge.operator,
        row.roofAgeYears,
        filters.roofAge.years,
      );
    conditions.push(matches);
    if (matches) reasons.push("roof_age");
  }
  if (filters.permit) {
    const count = row.openRoofingPermitCount;
    const maximumDays = row.maximumOpenRoofingPermitDays;
    const countMatches =
      (!filters.permit.openOnly && !filters.permit.roofingOnly) ||
      (count !== null && count > 0);
    const durationMatches =
      filters.permit.minOpenDays === undefined ||
      (maximumDays !== null && maximumDays >= filters.permit.minOpenDays);
    const matches = countMatches && durationMatches;
    conditions.push(matches);
    if (matches) reasons.push("permit");
  }
  if (filters.freshness) {
    const observedMatches =
      !filters.freshness.observedAtOrAfter ||
      Date.parse(row.observedAt) >=
        Date.parse(filters.freshness.observedAtOrAfter);
    const publishedMatches =
      !filters.freshness.publishedAtOrAfter ||
      (row.publishedAt !== null &&
        Date.parse(row.publishedAt) >=
          Date.parse(filters.freshness.publishedAtOrAfter));
    conditions.push(observedMatches && publishedMatches);
  }
  const mode = filters.matchMode ?? "all";
  const eligible =
    conditions.length === 0
      ? true
      : mode === "any"
        ? conditions.some(Boolean)
        : conditions.every(Boolean);
  if (eligible && reasons.length === 0) reasons.push("roof_age");
  return { eligible, reasons };
}

function sortCandidates(
  values: SearchCandidate[],
  sort: SearchArguments["sort"],
): void {
  values.sort((left, right) => {
    let difference: number;
    if (sort === "distance_asc") {
      difference = left.distanceMeters - right.distanceMeters;
    } else if (sort === "roof_age_desc") {
      difference =
        (right.row.roofAgeYears ?? -1) - (left.row.roofAgeYears ?? -1);
    } else {
      difference =
        (right.row.maximumOpenRoofingPermitDays ?? -1) -
        (left.row.maximumOpenRoofingPermitDays ?? -1);
    }
    if (Math.abs(difference) > Number.EPSILON) return difference;
    const distanceDifference = left.distanceMeters - right.distanceMeters;
    if (Math.abs(distanceDifference) > Number.EPSILON)
      return distanceDifference;
    return left.row.propertyId.localeCompare(right.row.propertyId);
  });
}

interface CursorPayload {
  offset: number;
  query: string;
  signature: string;
  version: 1;
}

function cursorSignature(query: string, offset: number): string {
  return sha256(`prism-mcp-cursor-v1:${query}:${offset}`).slice(0, 24);
}

function encodeCursor(query: string, offset: number): string {
  const payload: CursorPayload = {
    offset,
    query,
    signature: cursorSignature(query, offset),
    version: 1,
  };
  return Buffer.from(JSON.stringify(payload)).toString("base64url");
}

function decodeCursor(value: string, query: string, maximum: number): number {
  if (
    Buffer.byteLength(value) > MAX_CURSOR_BYTES ||
    !/^[A-Za-z0-9_-]+$/.test(value)
  ) {
    throw new RuntimeFailure(
      "invalid_cursor",
      "The pagination cursor is invalid.",
    );
  }
  try {
    const parsed = JSON.parse(
      Buffer.from(value, "base64url").toString("utf8"),
    ) as CursorPayload | undefined;
    if (
      !parsed ||
      parsed.version !== 1 ||
      parsed.query !== query ||
      !Number.isInteger(parsed.offset) ||
      parsed.offset < 1 ||
      parsed.offset > maximum ||
      parsed.signature !== cursorSignature(parsed.query, parsed.offset) ||
      Object.keys(parsed).sort().join(",") !== "offset,query,signature,version"
    ) {
      throw new Error("invalid");
    }
    return parsed.offset;
  } catch {
    throw new RuntimeFailure(
      "invalid_cursor",
      "The pagination cursor is invalid.",
    );
  }
}

function searchFingerprint(argumentsValue: SearchArguments): string {
  const page = { limit: argumentsValue.page.limit };
  return sha256(stableJson({ ...argumentsValue, page }));
}

function timeoutFailure(): RuntimeFailure {
  return new RuntimeFailure(
    "deadline_exceeded",
    "The MCP request exceeded its bounded execution time.",
    true,
  );
}

function isDeadlineReason(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "TimeoutError") ||
    (error instanceof Error && error.message === "request_deadline")
  );
}

function coverageUnavailable(
  coverage: "ownership" | "permits",
): RuntimeFailure {
  return new RuntimeFailure(
    "data_unavailable",
    `${coverage === "permits" ? "Permit" : "Ownership"} filter coverage is unavailable for this dataset.`,
    false,
    coverage === "permits" ? "pasco-permits" : "pasco-ownership",
    { coverage, type: "coverage_unavailable" },
  );
}

function publicReadFailure(error: PublicReadError): RuntimeFailure {
  return new RuntimeFailure(
    error.retryable ? "dependency_unavailable" : "data_unavailable",
    "The validated public-data publication could not satisfy the request.",
    error.retryable,
    "public-publication",
    { publicReadCode: error.code, type: "public_read_failure" },
  );
}

export class OracleMcpRuntime {
  constructor(
    readonly provider: OracleMcpProvider,
    readonly contracts: McpContractRegistry,
    readonly limits: McpLimits,
  ) {}

  async execute(
    tool: McpToolName,
    argumentsValue: unknown,
    externalSignal?: AbortSignal,
  ): Promise<ToolExecutionResult> {
    const failures = this.contracts.validateInput(tool, argumentsValue);
    if (failures.length > 0) {
      return this.#checkedError(
        tool,
        argumentsValue,
        new RuntimeFailure(
          "invalid_argument",
          "Tool arguments do not satisfy the frozen MCP input schema.",
          false,
          undefined,
          { failures },
        ),
      );
    }
    const controller = new AbortController();
    const abort = () => controller.abort(externalSignal?.reason);
    externalSignal?.addEventListener("abort", abort, { once: true });
    if (externalSignal?.aborted) abort();
    const timeout = setTimeout(
      () => controller.abort(timeoutFailure()),
      this.limits.requestTimeoutMs,
    );
    timeout.unref();
    try {
      const result = await Promise.race([
        this.#executeValidated(
          tool,
          argumentsValue as JsonObject,
          controller.signal,
        ),
        new Promise<never>((_, reject) => {
          controller.signal.addEventListener(
            "abort",
            () => reject(controller.signal.reason ?? timeoutFailure()),
            { once: true },
          );
        }),
      ]);
      const outputFailures = this.contracts.validateOutput(tool, result);
      if (outputFailures.length > 0) {
        return this.#checkedError(
          tool,
          argumentsValue,
          new RuntimeFailure(
            "internal",
            "The generated response failed frozen output validation.",
            false,
            undefined,
            { failures: outputFailures },
          ),
        );
      }
      if (
        Buffer.byteLength(JSON.stringify(result)) > this.limits.maxResponseBytes
      ) {
        return this.#checkedError(
          tool,
          argumentsValue,
          new RuntimeFailure(
            "data_unavailable",
            "The bounded MCP response-size limit was exceeded.",
          ),
        );
      }
      return { isError: false, result };
    } catch (error) {
      const failure =
        error instanceof RuntimeFailure
          ? error
          : isDeadlineReason(error)
            ? timeoutFailure()
            : error instanceof PublicReadError
              ? publicReadFailure(error)
              : new RuntimeFailure(
                  "data_unavailable",
                  "The configured public-data publication could not satisfy the request.",
                  false,
                  "local-publication",
                );
      return this.#checkedError(tool, argumentsValue, failure);
    } finally {
      clearTimeout(timeout);
      externalSignal?.removeEventListener("abort", abort);
    }
  }

  responseSizeFailure(
    tool: McpToolName,
    argumentsValue: unknown,
  ): ToolExecutionResult {
    return this.#checkedError(
      tool,
      argumentsValue,
      new RuntimeFailure(
        "data_unavailable",
        "The bounded MCP response-size limit was exceeded.",
      ),
    );
  }

  async #executeValidated(
    tool: McpToolName,
    argumentsValue: JsonObject,
    signal: AbortSignal,
  ): Promise<JsonObject> {
    const metadata = await this.provider.getMetadata(signal);
    switch (tool) {
      case "prism_v1_get_service_info":
        return {
          ok: true,
          data: {
            serviceVersion: MCP_SERVICE_VERSION,
            contractVersion: MCP_CONTRACT_VERSION,
            activeContractHash: MCP_SCHEMA_SHA256,
            county: "pasco",
            dataset: {
              version: metadata.datasetVersion,
              freshness: serviceFreshness(metadata),
            },
            supportedTools: MCP_TOOL_NAMES,
          },
          meta: responseMeta(metadata),
        };
      case "prism_v1_get_pipeline_run_summary":
        return {
          ok: true,
          data: pipelineSummary(metadata),
          meta: responseMeta(metadata),
        };
      case "prism_v1_get_query_schema":
        return {
          ok: true,
          data: queryCapabilities(),
          meta: responseMeta(metadata),
        };
      case "prism_v1_get_property": {
        const propertyId = argumentsValue.propertyId as string;
        const canonical = await this.provider.getCanonicalProperty(
          propertyId,
          signal,
        );
        if (!canonical) {
          throw new RuntimeFailure(
            "not_found",
            "The requested property was not found.",
          );
        }
        return {
          ok: true,
          data: projectPublicProperty(canonical, propertyId),
          meta: responseMeta(metadata),
        };
      }
      case "prism_v1_get_permit": {
        if (metadata.permitCoverage === "unavailable") {
          throw new RuntimeFailure(
            "data_unavailable",
            "Permit source coverage is unavailable for this dataset.",
            false,
            "pasco-permits",
          );
        }
        const permit = await this.provider.getPermit(
          argumentsValue.permitId as string,
          signal,
        );
        if (!permit) {
          throw new RuntimeFailure(
            "not_found",
            "The requested permit was not found.",
          );
        }
        return { ok: true, data: permit, meta: responseMeta(metadata) };
      }
      case "prism_v1_search_roofing_opportunities":
        return this.#search(
          argumentsValue as unknown as SearchArguments,
          metadata,
          signal,
        );
    }
  }

  async #search(
    argumentsValue: SearchArguments,
    metadata: DatasetMetadata,
    signal: AbortSignal,
  ): Promise<JsonObject> {
    if (
      metadata.permitCoverage === "unavailable" &&
      (argumentsValue.filters.permit !== undefined ||
        argumentsValue.sort === "permit_open_days_desc")
    ) {
      throw coverageUnavailable("permits");
    }
    if (argumentsValue.filters.ownership !== undefined) {
      // The frozen query contract accepts ownership filters, but this
      // publication has neither ownership-duration nor owner-area query facts.
      // An explicit coverage failure is truthful; an empty success would imply
      // that the filter was evaluated against complete data.
      throw coverageUnavailable("ownership");
    }
    const rows = await this.provider.getQueryRows(signal);
    const center = resolveCenter(argumentsValue.center, rows, metadata);
    const radiusMeters =
      argumentsValue.radius.value *
      (argumentsValue.radius.unit === "mi" ? 1609.344 : 1000);
    const candidates: SearchCandidate[] = [];
    for (const row of rows) {
      if (row.latitude === null || row.longitude === null) continue;
      const distanceMeters = haversineMeters(
        center.latitude,
        center.longitude,
        row.latitude,
        row.longitude,
      );
      if (distanceMeters > radiusMeters + 1e-7) continue;
      const match = candidateReasons(row, argumentsValue.filters);
      if (!match.eligible) continue;
      candidates.push({ distanceMeters, matchReasons: match.reasons, row });
    }
    sortCandidates(candidates, argumentsValue.sort);
    const fingerprint = searchFingerprint(argumentsValue);
    const offset = argumentsValue.page.cursor
      ? decodeCursor(argumentsValue.page.cursor, fingerprint, candidates.length)
      : 0;
    const page = candidates.slice(offset, offset + argumentsValue.page.limit);
    const opportunities = await mapWithConcurrency(
      page,
      PROPERTY_HYDRATION_CONCURRENCY,
      async (candidate) => {
        const canonical = await this.provider.getCanonicalProperty(
          candidate.row.propertyId,
          signal,
        );
        if (!canonical) {
          throw new RuntimeFailure(
            "data_unavailable",
            "A query-table property does not resolve to its canonical document.",
            false,
            "local-publication",
          );
        }
        const coordinateFact = record(canonical.coordinates, "coordinates");
        const coordinateReferences = Array.isArray(coordinateFact.evidenceRefs)
          ? (coordinateFact.evidenceRefs as string[])
          : [];
        return {
          property: projectPublicProperty(canonical, candidate.row.propertyId),
          distanceMeters: availableFact(
            candidate.distanceMeters,
            "derived",
            coordinateReferences,
            {
              rule: "haversine_distance",
              ruleVersion: MCP_CONTRACT_VERSION,
              asOf: argumentsValue.asOf ?? metadata.asOf,
              inputs: ["search.center", "property.coordinates"],
            },
          ),
          matchReasons: candidate.matchReasons,
        };
      },
    );
    const nextOffset = offset + page.length;
    const nextCursor =
      nextOffset < candidates.length
        ? encodeCursor(fingerprint, nextOffset)
        : null;
    return {
      ok: true,
      data: { resolvedCenter: center.fact, opportunities },
      meta: responseMeta(
        metadata,
        argumentsValue.asOf ?? metadata.asOf,
        nextCursor,
      ),
    };
  }

  #checkedError(
    tool: McpToolName,
    argumentsValue: unknown,
    failure: RuntimeFailure,
  ): ToolExecutionResult {
    const result = errorResult(tool, argumentsValue, failure);
    if (this.contracts.validateError(result).length > 0) {
      throw new Error("Generated MCP error failed frozen validation");
    }
    return { isError: true, result };
  }
}
