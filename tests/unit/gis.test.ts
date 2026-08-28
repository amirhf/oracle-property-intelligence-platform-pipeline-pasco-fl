import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it, vi } from "vitest";

import { SourceAccessStopError } from "../../src/lib/access-stop.js";
import {
  fetchPascoCoordinateBatches,
  geometryCentroid,
  parsePascoGeoJson,
} from "../../src/gis/pasco.js";

describe("Pasco coordinate handling", () => {
  it("calculates a deterministic WGS84 polygon centroid", () => {
    expect(
      geometryCentroid({
        coordinates: [
          [
            [-82.4, 28.2],
            [-82.2, 28.2],
            [-82.2, 28.4],
            [-82.4, 28.4],
            [-82.4, 28.2],
          ],
        ],
        type: "Polygon",
      }),
    ).toEqual({ latitude: 28.3, longitude: -82.3 });
  });

  it("rejects out-of-bounds coordinates", () => {
    expect(() =>
      geometryCentroid({
        coordinates: [
          [
            [200, 95],
            [201, 95],
            [201, 96],
            [200, 96],
            [200, 95],
          ],
        ],
        type: "Polygon",
      }),
    ).toThrow("outside WGS84 bounds");
  });

  it("keys GIS results by the exact HPARCEL", () => {
    const result = parsePascoGeoJson(
      JSON.stringify({
        features: [
          {
            geometry: {
              coordinates: [
                [
                  [-82.4, 28.2],
                  [-82.2, 28.2],
                  [-82.2, 28.4],
                  [-82.4, 28.4],
                  [-82.4, 28.2],
                ],
              ],
              type: "Polygon",
            },
            properties: { HPARCEL: "01-23", LAST_UPDATE: null },
          },
        ],
        type: "FeatureCollection",
      }),
    );
    expect(result.get("01-23")?.sourceCrs).toBe("EPSG:4326");
  });

  it("bounds concurrency and reuses hash-verified batch checkpoints", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "pasco-gis-"));
    let active = 0;
    let peakActive = 0;
    const fetchImpl = vi.fn(async (_input: unknown, init?: RequestInit) => {
      active += 1;
      peakActive = Math.max(peakActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      const form = new URLSearchParams(String(init?.body));
      const where = form.get("where") ?? "";
      const folios = [...where.matchAll(/'([^']+)'/g)].map(
        (match) => match[1] ?? "",
      );
      active -= 1;
      return new Response(
        JSON.stringify({
          features: folios.map((folio, index) => ({
            geometry: {
              coordinates: [
                [
                  [-82.4 + index / 1000, 28.2],
                  [-82.3 + index / 1000, 28.2],
                  [-82.3 + index / 1000, 28.3],
                  [-82.4 + index / 1000, 28.3],
                  [-82.4 + index / 1000, 28.2],
                ],
              ],
              type: "Polygon",
            },
            properties: { HPARCEL: folio, LAST_UPDATE: null },
          })),
          type: "FeatureCollection",
        }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;
    try {
      const first = await fetchPascoCoordinateBatches({
        batchSize: 2,
        concurrency: 2,
        dataDir: directory,
        exactFolios: ["A-1", "A-2", "A-3", "A-4", "A-5"],
        fetchImpl,
        maxRetries: 0,
        scopeKey: "unit-test",
      });
      expect(first.metrics).toMatchObject({
        batchCount: 3,
        concurrency: 2,
        requestCount: 3,
        reusedBatchCount: 0,
        retryCount: 0,
        statusCounts: { "200": 3 },
      });
      expect(first.coordinates).toHaveLength(5);
      expect(peakActive).toBeLessThanOrEqual(2);

      const noNetwork = vi.fn(async () => {
        throw new Error("checkpoint reuse should avoid the network");
      }) as unknown as typeof fetch;
      const repeated = await fetchPascoCoordinateBatches({
        batchSize: 2,
        concurrency: 2,
        dataDir: directory,
        exactFolios: ["A-1", "A-2", "A-3", "A-4", "A-5"],
        fetchImpl: noNetwork,
        maxRetries: 0,
        scopeKey: "unit-test",
      });
      expect(repeated.metrics).toMatchObject({
        requestCount: 0,
        reusedBatchCount: 3,
        statusCounts: { checkpoint: 3 },
      });
      expect(noNetwork).not.toHaveBeenCalled();
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });

  it("stops immediately on a 429 without retrying", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "pasco-gis-stop-"));
    const fetchImpl = vi.fn(
      async () => new Response("rate limited", { status: 429 }),
    ) as unknown as typeof fetch;
    try {
      await expect(
        fetchPascoCoordinateBatches({
          dataDir: directory,
          exactFolios: ["A-1"],
          fetchImpl,
          maxRetries: 2,
          scopeKey: "access-stop",
        }),
      ).rejects.toBeInstanceOf(SourceAccessStopError);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });
});
