import { access, lstat, readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";

import { z } from "zod";

import type {
  ArtifactCapture,
  CoordinateResult,
  GisAcquisitionMetrics,
} from "../domain/types.js";
import { captureTextArtifact } from "../lib/artifacts.js";
import { SourceAccessStopError } from "../lib/access-stop.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { sha256 } from "../lib/hash.js";

export const PASCO_GIS_LAYER_URL =
  "https://pascogis.pascocountyfl.net/giswebmm/rest/services/PascoMapper/Parcels/MapServer/7";

type Position = [number, number];

interface GeoJsonFeature {
  geometry: {
    coordinates: Position[][] | Position[][][];
    type: "MultiPolygon" | "Polygon";
  } | null;
  properties: {
    HPARCEL?: string;
    LAST_UPDATE?: number | string | null;
  };
}

interface GeoJsonFeatureCollection {
  exceededTransferLimit?: boolean;
  features: GeoJsonFeature[];
  type: "FeatureCollection";
}

const PASCO_GIS_QUERY_URL = `${PASCO_GIS_LAYER_URL}/query`;
const VERIFIED_LOCAL_GIS_ARTIFACT_COUNT = 60;
const VERIFIED_LOCAL_GIS_BYTES = 13_827_105;
const VERIFIED_LOCAL_GIS_INVENTORY_SHA256 =
  "985c545525eba8022d3f933ed5ea9c97aa8a18777a8d539950942bd918c72365";
const readyMarkerSchema = z.strictObject({
  bytes: z.number().int().positive(),
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
  sourceSystem: z.literal("pasco_gis"),
  sourceUrl: z.literal(PASCO_GIS_QUERY_URL),
});

function ringCentroid(ring: Position[]): {
  area: number;
  latitude: number;
  longitude: number;
} {
  let areaTwice = 0;
  let longitudeNumerator = 0;
  let latitudeNumerator = 0;
  for (let index = 0; index < ring.length - 1; index += 1) {
    const current = ring[index];
    const next = ring[index + 1];
    if (!current || !next) continue;
    const cross = current[0] * next[1] - next[0] * current[1];
    areaTwice += cross;
    longitudeNumerator += (current[0] + next[0]) * cross;
    latitudeNumerator += (current[1] + next[1]) * cross;
  }
  if (Math.abs(areaTwice) < Number.EPSILON) {
    const points = ring.slice(0, -1);
    if (points.length === 0) throw new Error("GIS polygon ring is empty");
    return {
      area: 0,
      latitude:
        points.reduce((sum, position) => sum + position[1], 0) / points.length,
      longitude:
        points.reduce((sum, position) => sum + position[0], 0) / points.length,
    };
  }
  return {
    area: Math.abs(areaTwice / 2),
    latitude: latitudeNumerator / (3 * areaTwice),
    longitude: longitudeNumerator / (3 * areaTwice),
  };
}

export function geometryCentroid(geometry: GeoJsonFeature["geometry"]): {
  latitude: number;
  longitude: number;
} {
  if (!geometry) throw new Error("GIS feature has no geometry");
  const polygons =
    geometry.type === "Polygon"
      ? [geometry.coordinates as Position[][]]
      : (geometry.coordinates as Position[][][]);
  const centroids = polygons
    .map((polygon) => polygon[0])
    .filter((ring): ring is Position[] => Boolean(ring))
    .map(ringCentroid);
  if (centroids.length === 0) throw new Error("GIS feature has no outer rings");
  const totalArea = centroids.reduce((sum, entry) => sum + entry.area, 0);
  const divisor = totalArea > 0 ? totalArea : centroids.length;
  const latitude =
    centroids.reduce(
      (sum, entry) => sum + entry.latitude * (entry.area || 1),
      0,
    ) / divisor;
  const longitude =
    centroids.reduce(
      (sum, entry) => sum + entry.longitude * (entry.area || 1),
      0,
    ) / divisor;
  if (
    !Number.isFinite(latitude) ||
    !Number.isFinite(longitude) ||
    latitude < -90 ||
    latitude > 90 ||
    longitude < -180 ||
    longitude > 180
  ) {
    throw new Error("GIS centroid is outside WGS84 bounds");
  }
  return {
    latitude: Number(latitude.toFixed(7)),
    longitude: Number(longitude.toFixed(7)),
  };
}

export function parsePascoGeoJson(body: string): Map<string, CoordinateResult> {
  const parsed = JSON.parse(body) as GeoJsonFeatureCollection;
  if (parsed.type !== "FeatureCollection" || !Array.isArray(parsed.features)) {
    throw new Error("Unexpected Pasco GIS GeoJSON response");
  }
  if (parsed.exceededTransferLimit) {
    throw new Error(
      "Pasco GIS response exceeded the advertised transfer limit",
    );
  }
  const results = new Map<string, CoordinateResult>();
  for (const feature of parsed.features) {
    const folio = feature.properties.HPARCEL?.trim();
    if (!folio) continue;
    const centroid = geometryCentroid(feature.geometry);
    const rawLastUpdate = feature.properties.LAST_UPDATE;
    const sourceLastUpdate =
      typeof rawLastUpdate === "number"
        ? new Date(rawLastUpdate).toISOString()
        : typeof rawLastUpdate === "string" && rawLastUpdate.length > 0
          ? rawLastUpdate
          : null;
    results.set(folio, {
      ...centroid,
      method: "polygon_centroid",
      sourceCrs: "EPSG:4326",
      sourceLastUpdate,
    });
  }
  return results;
}

export function assertVerifiedLocalPascoGisInventory(
  artifacts: readonly ArtifactCapture[],
): void {
  const inventory = artifacts
    .map((artifact) => ({
      bytes: artifact.bytes,
      sha256: artifact.sha256,
      sourceSystem: artifact.sourceSystem,
      sourceUrl: artifact.sourceUrl,
    }))
    .sort((left, right) => left.sha256.localeCompare(right.sha256));
  const totalBytes = inventory.reduce(
    (sum, artifact) => sum + artifact.bytes,
    0,
  );
  if (
    inventory.length !== VERIFIED_LOCAL_GIS_ARTIFACT_COUNT ||
    totalBytes !== VERIFIED_LOCAL_GIS_BYTES ||
    canonicalJsonSha256(inventory) !== VERIFIED_LOCAL_GIS_INVENTORY_SHA256
  ) {
    throw new DurableInputError(
      "Local GIS inventory does not match the verified checkpoint set",
    );
  }
}

async function existingArtifact(
  finalPath: string,
): Promise<{ artifact: ArtifactCapture; body: string } | null> {
  const readyMarkerPath = `${finalPath}.ready.json`;
  try {
    await access(finalPath);
    await access(readyMarkerPath);
  } catch {
    return null;
  }
  const [body, markerText, fileStat, fileLinkStat, markerLinkStat] =
    await Promise.all([
      readFile(finalPath, "utf8"),
      readFile(readyMarkerPath, "utf8"),
      stat(finalPath),
      lstat(finalPath),
      lstat(readyMarkerPath),
    ]);
  if (
    fileLinkStat.isSymbolicLink() ||
    markerLinkStat.isSymbolicLink() ||
    !fileLinkStat.isFile() ||
    !markerLinkStat.isFile()
  ) {
    throw new DurableInputError("GIS checkpoint is not a regular local file");
  }
  const marker = readyMarkerSchema.parse(JSON.parse(markerText));
  const currentHash = sha256(body);
  if (marker.bytes !== fileStat.size || marker.sha256 !== currentHash) {
    throw new DurableInputError("GIS checkpoint failed hash validation");
  }
  return {
    artifact: {
      bytes: marker.bytes,
      localPath: finalPath,
      readyMarkerPath,
      sha256: marker.sha256,
      sourceSystem: marker.sourceSystem,
      sourceUrl: marker.sourceUrl,
    },
    body,
  };
}

export async function loadVerifiedLocalPascoCoordinates(
  dataDir: string,
): Promise<{
  artifacts: ArtifactCapture[];
  coordinates: Map<string, CoordinateResult>;
  metrics: GisAcquisitionMetrics;
}> {
  const root = path.join(dataDir, "pasco", "raw", "gis", "scales");
  const artifacts: ArtifactCapture[] = [];
  const coordinates = new Map<string, CoordinateResult>();
  const directories = await readdir(root, { withFileTypes: true });
  for (const directory of directories
    .filter((entry) => entry.isDirectory() && !entry.isSymbolicLink())
    .sort((left, right) => left.name.localeCompare(right.name))) {
    const directoryPath = path.join(root, directory.name);
    const entries = await readdir(directoryPath, { withFileTypes: true });
    for (const entry of entries
      .filter(
        (candidate) =>
          candidate.isFile() &&
          !candidate.isSymbolicLink() &&
          candidate.name.endsWith(".geojson"),
      )
      .sort((left, right) => left.name.localeCompare(right.name))) {
      const verified = await existingArtifact(
        path.join(directoryPath, entry.name),
      );
      if (!verified)
        throw new Error("GIS checkpoint is missing its ready marker");
      artifacts.push(verified.artifact);
      for (const [folio, coordinate] of parsePascoGeoJson(verified.body)) {
        const existing = coordinates.get(folio);
        if (
          existing &&
          JSON.stringify(existing) !== JSON.stringify(coordinate)
        ) {
          throw new Error("Conflicting verified GIS coordinates for one folio");
        }
        coordinates.set(folio, coordinate);
      }
    }
  }
  assertVerifiedLocalPascoGisInventory(artifacts);
  return {
    artifacts,
    coordinates,
    metrics: {
      batchCount: artifacts.length,
      batchSize: 500,
      concurrency: 1,
      requestCount: 0,
      retryCount: 0,
      reusedBatchCount: artifacts.length,
      statusCounts: { checkpoint: artifacts.length },
    },
  };
}

function incrementStatus(metrics: GisAcquisitionMetrics, status: string): void {
  metrics.statusCounts[status] = (metrics.statusCounts[status] ?? 0) + 1;
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function fetchCoordinateBatch(options: {
  batchIndex: number;
  dataDir: string;
  exactFolios: readonly string[];
  fetchImpl: typeof fetch;
  maxRetries: number;
  metrics: GisAcquisitionMetrics;
  scopeKey: string;
}): Promise<{
  artifact: ArtifactCapture;
  coordinates: Map<string, CoordinateResult>;
}> {
  const finalPath = path.join(
    options.dataDir,
    "pasco",
    "raw",
    "gis",
    "scales",
    options.scopeKey,
    `batch-${String(options.batchIndex + 1).padStart(5, "0")}.geojson`,
  );
  const cached = await existingArtifact(finalPath);
  if (cached) {
    options.metrics.reusedBatchCount += 1;
    incrementStatus(options.metrics, "checkpoint");
    return {
      artifact: cached.artifact,
      coordinates: parsePascoGeoJson(cached.body),
    };
  }

  const queryUrl = PASCO_GIS_QUERY_URL;
  const escaped = options.exactFolios.map(
    (folio) => `'${folio.replaceAll("'", "''")}'`,
  );
  const body = new URLSearchParams({
    f: "geojson",
    outFields: "HPARCEL,LAST_UPDATE",
    outSR: "4326",
    returnGeometry: "true",
    where: `HPARCEL IN (${escaped.join(",")})`,
  }).toString();

  let lastError: unknown;
  for (let attempt = 0; attempt <= options.maxRetries; attempt += 1) {
    if (attempt > 0) {
      options.metrics.retryCount += 1;
      await wait(250 * 2 ** (attempt - 1));
    }
    options.metrics.requestCount += 1;
    try {
      const response = await options.fetchImpl(queryUrl, {
        body,
        headers: {
          "content-type": "application/x-www-form-urlencoded",
          "user-agent": "Prism-Pasco-Scale/1.0",
        },
        method: "POST",
        signal: AbortSignal.timeout(60_000),
      });
      incrementStatus(options.metrics, String(response.status));
      if (
        response.status === 401 ||
        response.status === 403 ||
        response.status === 429
      ) {
        throw new SourceAccessStopError(
          `Pasco GIS access stop: HTTP ${response.status}`,
        );
      }
      if (!response.ok) {
        const error = new Error(
          `Pasco GIS query failed: HTTP ${response.status}`,
        );
        if (response.status >= 500 && attempt < options.maxRetries) {
          lastError = error;
          continue;
        }
        throw error;
      }
      const responseBody = await response.text();
      if (
        /captcha|verify you are human|access denied|challenge/i.test(
          responseBody,
        )
      ) {
        throw new SourceAccessStopError(
          "Pasco GIS access stop: challenge detected",
        );
      }
      const coordinates = parsePascoGeoJson(responseBody);
      const artifact = await captureTextArtifact({
        body: responseBody,
        finalPath,
        sourceSystem: "pasco_gis",
        sourceUrl: queryUrl,
      });
      return { artifact, coordinates };
    } catch (error) {
      if (error instanceof SourceAccessStopError) throw error;
      lastError = error;
      if (attempt >= options.maxRetries) throw error;
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error("Pasco GIS query failed without an error");
}

export async function fetchPascoCoordinateBatches(options: {
  batchSize?: number;
  concurrency?: number;
  dataDir: string;
  exactFolios: readonly string[];
  fetchImpl?: typeof fetch;
  maxRetries?: number;
  scopeKey: string;
}): Promise<{
  artifacts: ArtifactCapture[];
  coordinates: Map<string, CoordinateResult>;
  metrics: GisAcquisitionMetrics;
}> {
  const batchSize = options.batchSize ?? 500;
  const concurrency = options.concurrency ?? 2;
  const maxRetries = options.maxRetries ?? 2;
  if (options.exactFolios.length === 0) {
    throw new Error("Pasco GIS query requires at least one folio");
  }
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 2_000) {
    throw new Error("Pasco GIS batch size must be between 1 and 2,000");
  }
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 2) {
    throw new Error("Pasco GIS concurrency must be one or two");
  }
  if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 2) {
    throw new Error("Pasco GIS retries must be between zero and two");
  }

  const batches: string[][] = [];
  for (let index = 0; index < options.exactFolios.length; index += batchSize) {
    batches.push(options.exactFolios.slice(index, index + batchSize));
  }
  const metrics: GisAcquisitionMetrics = {
    batchCount: batches.length,
    batchSize,
    concurrency,
    requestCount: 0,
    retryCount: 0,
    reusedBatchCount: 0,
    statusCounts: {},
  };
  const results = new Array<
    Awaited<ReturnType<typeof fetchCoordinateBatch>> | undefined
  >(batches.length);
  let nextIndex = 0;
  const worker = async () => {
    while (nextIndex < batches.length) {
      const batchIndex = nextIndex;
      nextIndex += 1;
      const exactFolios = batches[batchIndex];
      if (!exactFolios) continue;
      results[batchIndex] = await fetchCoordinateBatch({
        batchIndex,
        dataDir: options.dataDir,
        exactFolios,
        fetchImpl: options.fetchImpl ?? fetch,
        maxRetries,
        metrics,
        scopeKey: options.scopeKey,
      });
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(concurrency, batches.length) }, worker),
  );

  const artifacts: ArtifactCapture[] = [];
  const coordinates = new Map<string, CoordinateResult>();
  for (const result of results) {
    if (!result) throw new Error("Pasco GIS batch result is missing");
    artifacts.push(result.artifact);
    for (const [folio, coordinate] of result.coordinates) {
      coordinates.set(folio, coordinate);
    }
  }
  return { artifacts, coordinates, metrics };
}

export async function fetchPascoCoordinates(options: {
  dataDir: string;
  exactFolios: readonly string[];
  runId: string;
}): Promise<{
  artifact: ArtifactCapture;
  coordinates: Map<string, CoordinateResult>;
  metrics: GisAcquisitionMetrics;
}> {
  if (options.exactFolios.length === 0 || options.exactFolios.length > 25) {
    throw new Error(
      "Pasco GIS pilot query must contain between 1 and 25 folios",
    );
  }
  const result = await fetchPascoCoordinateBatches({
    batchSize: 25,
    concurrency: 1,
    dataDir: options.dataDir,
    exactFolios: options.exactFolios,
    scopeKey: options.runId,
  });
  const artifact = result.artifacts[0];
  if (!artifact) throw new Error("Pasco GIS pilot artifact is missing");
  return { artifact, coordinates: result.coordinates, metrics: result.metrics };
}
