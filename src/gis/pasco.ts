import path from "node:path";

import type { ArtifactCapture, CoordinateResult } from "../domain/types.js";
import { captureTextArtifact } from "../lib/artifacts.js";
import { SourceAccessStopError } from "../lib/access-stop.js";

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
  features: GeoJsonFeature[];
  type: "FeatureCollection";
}

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

export async function fetchPascoCoordinates(options: {
  dataDir: string;
  exactFolios: readonly string[];
  runId: string;
}): Promise<{
  artifact: ArtifactCapture;
  coordinates: Map<string, CoordinateResult>;
}> {
  if (options.exactFolios.length === 0 || options.exactFolios.length > 25) {
    throw new Error(
      "Pasco GIS pilot query must contain between 1 and 25 folios",
    );
  }
  const escaped = options.exactFolios.map(
    (folio) => `'${folio.replaceAll("'", "''")}'`,
  );
  const queryUrl = new URL(`${PASCO_GIS_LAYER_URL}/query`);
  queryUrl.searchParams.set("f", "geojson");
  queryUrl.searchParams.set("where", `HPARCEL IN (${escaped.join(",")})`);
  queryUrl.searchParams.set("outFields", "HPARCEL,LAST_UPDATE");
  queryUrl.searchParams.set("returnGeometry", "true");
  queryUrl.searchParams.set("outSR", "4326");

  const response = await fetch(queryUrl, {
    headers: { "user-agent": "Prism-Pasco-Pilot/1.0" },
    signal: AbortSignal.timeout(30_000),
  });
  if (response.status === 403 || response.status === 429) {
    throw new SourceAccessStopError(
      `Pasco GIS access stop: HTTP ${response.status}`,
    );
  }
  if (!response.ok) {
    throw new Error(`Pasco GIS query failed: HTTP ${response.status}`);
  }
  const body = await response.text();
  if (/captcha|verify you are human|access denied/i.test(body)) {
    throw new SourceAccessStopError(
      "Pasco GIS access stop: challenge detected",
    );
  }
  const artifact = await captureTextArtifact({
    body,
    finalPath: path.join(
      options.dataDir,
      "pasco",
      "raw",
      "gis",
      options.runId,
      "pilot-parcels.geojson",
    ),
    sourceSystem: "pasco_gis",
    sourceUrl: queryUrl.toString(),
  });
  return { artifact, coordinates: parsePascoGeoJson(body) };
}
