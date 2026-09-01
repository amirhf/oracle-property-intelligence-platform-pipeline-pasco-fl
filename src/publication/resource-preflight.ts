export const AUTHORITATIVE_PROPERTY_LIMIT = 400_000;
export const AUTHORITATIVE_FACT_LIMIT = 4_000_000;
export const AUTHORITATIVE_BATCH_SIZE = 1_000;
export const AUTHORITATIVE_DISK_RESERVE_BYTES = 8 * 1024 ** 3;
export const AUTHORITATIVE_MAX_RSS_BYTES = 6 * 1024 ** 3;

export interface PublicationResourcePreflightInput {
  availableBytes: number;
  availableFiles: number;
  factCount: number;
  propertyCount: number;
  sourcePayloadBytes: number;
}

export interface PublicationResourcePreflight {
  availableBytes: number;
  availableFiles: number;
  batchSize: number;
  estimatedBuildBytes: number;
  factCount: number;
  maxRssBytes: number;
  passed: true;
  projectedFileCountPerBuild: number;
  propertyCount: number;
  requiredPeakBytes: number;
  requiredPeakFiles: number;
  reserveBytes: number;
  sourcePayloadBytes: number;
}

export function authoritativePublicationCardinality(propertyCount: number): {
  edgeCount: number;
  inventoryObjectCount: number;
  shardCount: number;
} {
  if (!Number.isSafeInteger(propertyCount) || propertyCount <= 0) {
    throw new Error("Authoritative publication property count is invalid");
  }
  const shardCount = Math.ceil(propertyCount / 10_000);
  return {
    edgeCount: propertyCount + shardCount,
    // leaves + shards + root + five open-data metadata objects + Parquet
    inventoryObjectCount: propertyCount + shardCount + 7,
    shardCount,
  };
}

export function preflightAuthoritativePublicationResources(
  input: PublicationResourcePreflightInput,
): PublicationResourcePreflight {
  for (const [label, value] of Object.entries(input)) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Authoritative publication ${label} is invalid`);
    }
  }
  if (
    input.propertyCount === 0 ||
    input.propertyCount > AUTHORITATIVE_PROPERTY_LIMIT
  ) {
    throw new Error("Authoritative publication property limit exceeded");
  }
  if (input.factCount > AUTHORITATIVE_FACT_LIMIT) {
    throw new Error("Authoritative publication fact limit exceeded");
  }
  const { inventoryObjectCount } = authoritativePublicationCardinality(
    input.propertyCount,
  );
  // The measured sealed-projection JSON is expanded conservatively for
  // canonical documents, query NDJSON, Parquet, plan/manifest metadata and
  // DuckDB spill. This is a pre-write bound, not an estimate inferred from the
  // historical 25,000 sample.
  const estimatedBuildBytes =
    input.sourcePayloadBytes * 4 + input.propertyCount * 4_096;
  const requiredPeakBytes =
    estimatedBuildBytes * 2 + AUTHORITATIVE_DISK_RESERVE_BYTES;
  const projectedFileCountPerBuild = inventoryObjectCount + 8;
  const requiredPeakFiles = projectedFileCountPerBuild * 2 + 100_000;
  if (input.availableBytes < requiredPeakBytes) {
    throw new Error(
      "Authoritative publication disk preflight failed before artifact creation",
    );
  }
  if (input.availableFiles < requiredPeakFiles) {
    throw new Error(
      "Authoritative publication inode preflight failed before artifact creation",
    );
  }
  return {
    availableBytes: input.availableBytes,
    availableFiles: input.availableFiles,
    batchSize: AUTHORITATIVE_BATCH_SIZE,
    estimatedBuildBytes,
    factCount: input.factCount,
    maxRssBytes: AUTHORITATIVE_MAX_RSS_BYTES,
    passed: true,
    projectedFileCountPerBuild,
    propertyCount: input.propertyCount,
    requiredPeakBytes,
    requiredPeakFiles,
    reserveBytes: AUTHORITATIVE_DISK_RESERVE_BYTES,
    sourcePayloadBytes: input.sourcePayloadBytes,
  };
}
