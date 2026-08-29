import { z } from "zod";

import type { PilotRunRequest, PreparedPilot } from "../domain/types.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";
import {
  preparedInputReferenceSchema,
  type PreparedInputReference,
} from "../snapshot/model.js";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const isoDateTimeSchema = z
  .string()
  .refine(
    (value) => Number.isFinite(Date.parse(value)),
    "must be an ISO date-time",
  );
const nullableText = z.string().nullable();
const nullableNumber = z.number().finite().nullable();

export const countyIngestRequestSchema = z.strictObject({
  asOf: isoDateTimeSchema,
  county: z.literal("pasco"),
  expectedSnapshotId: z
    .string()
    .regex(/^snapshot_[a-f0-9]{32}$/)
    .optional(),
  runId: z.string().regex(/^run_[a-f0-9]{32}$/),
  sampleAlgorithm: z.string().min(1).max(200),
  sampleSeed: z.string().min(1).max(500),
  selectionSize: z.union([z.literal(25), z.literal(5_000), z.literal(25_000)]),
  workflowId: z.string().regex(/^pasco-[a-z0-9-]{1,180}$/),
});

export const ingestChunkRequestSchema = countyIngestRequestSchema
  .extend({
    chunkCount: z.literal(1),
    chunkIndex: z.literal(0),
    endExclusive: z.union([z.literal(25), z.literal(5_000), z.literal(25_000)]),
    parentRequestSha256: sha256Schema,
    startIndex: z.literal(0),
  })
  .superRefine((value, context) => {
    if (value.endExclusive !== value.selectionSize) {
      context.addIssue({
        code: "custom",
        message: "endExclusive must equal selectionSize",
        path: ["endExclusive"],
      });
    }
  });

export const loaderRequestSchema = z.strictObject({
  county: z.literal("pasco"),
  idempotencyKey: z.string().regex(/^load_[a-f0-9]{32}$/),
  parentRequestSha256: sha256Schema,
  prepared: preparedInputReferenceSchema,
  request: countyIngestRequestSchema,
});

export type IngestChunkRequest = z.infer<typeof ingestChunkRequestSchema>;
export interface LoaderRequest {
  county: "pasco";
  idempotencyKey: string;
  parentRequestSha256: string;
  prepared: PreparedInputReference;
  request: PilotRunRequest;
}

const sourceParseCountSchema = z.strictObject({
  accepted: z.number().int().nonnegative(),
  parsed: z.number().int().nonnegative(),
  rejectionReasons: z.record(z.string(), z.number().int().nonnegative()),
  rejected: z.number().int().nonnegative(),
  source: z.number().int().nonnegative(),
});

const parcelSchema = z.strictObject({
  acres: nullableNumber,
  exactFolio: z.string().min(1).max(100),
  heatedSquareFeet: nullableNumber,
  homestead: nullableText,
  neighborhoodCode: nullableText,
  propertyUseCode: nullableText,
  propertyUseDescription: nullableText,
  totalSquareFeet: nullableNumber,
});

const buildingSchema = z.strictObject({
  actualYearBuilt: z.number().int().nullable(),
  buildingNumber: z.string().min(1).max(100),
  buildingSection: z.string().min(1).max(100),
  effectiveYearBuilt: z.number().int().nullable(),
  exactFolio: z.string().min(1).max(100),
  heatedSquareFeet: nullableNumber,
  observedCondition: nullableText,
  roofCover: nullableText,
  roofStructure: nullableText,
  stories: nullableNumber,
  totalSquareFeet: nullableNumber,
  useDescription: nullableText,
});

const siteAddressSchema = z
  .strictObject({
    city: nullableText,
    exactFolio: z.string().min(1).max(100),
    siteAddress: z.string().min(1).max(500),
    zipCode: nullableText,
  })
  .nullable();

const ownerSchema = z.strictObject({
  exactFolio: z.string().min(1).max(100),
  mailingAddress1: nullableText,
  mailingAddress2: nullableText,
  mailingCity: nullableText,
  mailingCountry: nullableText,
  mailingState: nullableText,
  mailingZip: nullableText,
  ownerName1: nullableText,
  ownerName2: nullableText,
});

const coordinateSchema = z
  .strictObject({
    latitude: z.number().min(-90).max(90),
    longitude: z.number().min(-180).max(180),
    method: z.literal("polygon_centroid"),
    sourceCrs: z.literal("EPSG:4326"),
    sourceLastUpdate: nullableText,
  })
  .nullable();

const permitSchema = z.strictObject({
  address: nullableText,
  description: nullableText,
  projectName: nullableText,
  recordDate: nullableText,
  recordNumber: z.string().min(1).max(200),
  recordType: z.string().min(1).max(500),
  status: nullableText,
});

const artifactSchema = z.strictObject({
  bytes: z.number().int().nonnegative(),
  localPath: z.string().min(1),
  readyMarkerPath: z.string().min(1),
  sha256: sha256Schema,
  sourceSystem: z.string().min(1).max(200),
  sourceUrl: z.string().min(1).max(2_048),
});

const preparedPropertySchema = z.strictObject({
  buildings: z.array(buildingSchema),
  coordinates: coordinateSchema,
  owners: z.array(ownerSchema),
  parcel: parcelSchema,
  permits: z.array(permitSchema),
  propertyId: z.string().regex(/^property_[a-f0-9]{32}$/),
  rank: z.string().min(1).max(500),
  siteAddress: siteAddressSchema,
  useGroup: z.string().min(1).max(200),
  yearBucket: z.string().min(1).max(200),
  yearBuilt: z.number().int().min(1500).max(3000),
});

export const preparedPilotSchema = z.strictObject({
  artifacts: z.array(artifactSchema),
  gisMetrics: z.strictObject({
    batchCount: z.number().int().nonnegative(),
    batchSize: z.number().int().positive(),
    concurrency: z.number().int().positive(),
    requestCount: z.number().int().nonnegative(),
    retryCount: z.number().int().nonnegative(),
    reusedBatchCount: z.number().int().nonnegative(),
    statusCounts: z.record(z.string(), z.number().int().nonnegative()),
  }),
  permitRequestCount: z.number().int().nonnegative(),
  properties: z.array(preparedPropertySchema),
  resourceMetrics: z.strictObject({
    diskAvailableBytes: z.number().nonnegative(),
    elapsedMs: z.number().int().nonnegative(),
    peakRssBytes: z.number().nonnegative(),
  }),
  sampleAlgorithm: z.string().min(1).max(200),
  sampleSeed: z.string().min(1).max(500),
  selectedRecordSha256: sha256Schema,
  selectionSize: z.number().int().positive(),
  snapshotId: z.string().regex(/^snapshot_[a-f0-9]{32}$/),
  snapshotManifestSha256: sha256Schema,
  sourceCounts: z.record(z.string(), sourceParseCountSchema),
  sourceLimitations: z.array(z.string().min(1).max(2_000)),
});

function parseStrict<T>(schema: z.ZodType<T>, value: unknown, name: string): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new DurableInputError(
      `${name} request failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  return parsed.data;
}

export function parseCountyIngestRequest(value: unknown): PilotRunRequest {
  const request = parseStrict(countyIngestRequestSchema, value, "CountyIngest");
  const normalized =
    request.expectedSnapshotId === undefined
      ? {
          asOf: request.asOf,
          county: request.county,
          runId: request.runId,
          sampleAlgorithm: request.sampleAlgorithm,
          sampleSeed: request.sampleSeed,
          selectionSize: request.selectionSize,
          workflowId: request.workflowId,
        }
      : (request as PilotRunRequest);
  const expectedRunId = deterministicId("run", [
    "1.0.0",
    "pipeline-run",
    "pasco",
    normalized.workflowId,
  ]);
  if (normalized.runId !== expectedRunId) {
    throw new DurableInputError(
      `CountyIngest runId does not match workflow identity (${normalized.workflowId})`,
    );
  }
  return normalized;
}

export function parseIngestChunkRequest(value: unknown): IngestChunkRequest {
  const request = parseStrict(ingestChunkRequestSchema, value, "IngestChunk");
  const {
    chunkCount: _chunkCount,
    chunkIndex: _chunkIndex,
    endExclusive: _endExclusive,
    parentRequestSha256,
    startIndex: _startIndex,
    ...countyRequest
  } = request;
  if (
    countyIngestRequestSha256(parseCountyIngestRequest(countyRequest)) !==
    parentRequestSha256
  ) {
    throw new DurableInputError(
      "IngestChunk parent request hash does not match its payload",
    );
  }
  return request;
}

export function parseLoaderRequest(value: unknown): LoaderRequest {
  const request = parseStrict(loaderRequestSchema, value, "Loader");
  const countyRequest = parseCountyIngestRequest(request.request);
  if (
    countyIngestRequestSha256(countyRequest) !== request.parentRequestSha256
  ) {
    throw new DurableInputError(
      "Loader parent request hash does not match its payload",
    );
  }
  const expectedIdempotencyKey = deterministicId("load", [
    "1.0.0",
    "Loader/pasco",
    countyRequest.workflowId,
    request.prepared.preparedInputId,
  ]);
  if (request.idempotencyKey !== expectedIdempotencyKey) {
    throw new DurableInputError(
      `Loader idempotency key does not match its prepared input (${request.prepared.preparedInputId})`,
    );
  }
  if (
    countyRequest.expectedSnapshotId !== undefined &&
    request.prepared.snapshotId !== countyRequest.expectedSnapshotId
  ) {
    throw new DurableInputError(
      "Loader prepared snapshot does not match the requested snapshot",
    );
  }
  return { ...request, request: countyRequest } as LoaderRequest;
}

export function parsePreparedPilot(value: unknown): PreparedPilot {
  const prepared = parseStrict(preparedPilotSchema, value, "prepared input");
  if (prepared.properties.length !== prepared.selectionSize) {
    throw new DurableInputError(
      "Prepared property count does not match selection size",
    );
  }
  return prepared as PreparedPilot;
}

export function countyIngestRequestSha256(request: PilotRunRequest): string {
  return canonicalJsonSha256(request);
}
