import { describe, expect, it } from "vitest";

import { geometryCentroid, parsePascoGeoJson } from "../../src/gis/pasco.js";

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
});
