import { describe, expect, it } from "vitest";

import type { PilotCandidate } from "../../src/domain/types.js";
import {
  COUNTYWIDE_SAMPLE_ALGORITHM,
  countywideStratum,
  selectCountywideSample,
} from "../../src/scale/sample.js";

function candidate(
  exactFolio: string,
  city: string,
  year: number,
  propertyUseDescription: string,
): PilotCandidate {
  return {
    buildings: [
      {
        actualYearBuilt: year,
        buildingNumber: "1",
        buildingSection: "1",
        effectiveYearBuilt: null,
        exactFolio,
        heatedSquareFeet: null,
        observedCondition: null,
        roofCover: null,
        roofStructure: null,
        stories: null,
        totalSquareFeet: null,
        useDescription: null,
      },
    ],
    parcel: {
      acres: null,
      exactFolio,
      heatedSquareFeet: null,
      homestead: null,
      neighborhoodCode: null,
      propertyUseCode: null,
      propertyUseDescription,
      totalSquareFeet: null,
    },
    siteAddress: {
      city,
      exactFolio,
      siteAddress: `${exactFolio} TEST ROAD`,
      zipCode: null,
    },
  };
}

describe("countywide scale sampling", () => {
  it("is deterministic, size-independent, and distributes a prefix across strata", () => {
    const cities = ["DADE CITY", "HUDSON", "ZEPHYRHILLS"];
    const years = [1950, 1970, 1990, 2005, 2020];
    const uses = ["Single Family", "Commercial", "Manufactured Home"];
    const candidates = new Map<string, PilotCandidate>();
    let ordinal = 0;
    for (const city of cities) {
      for (const year of years) {
        const use = uses[ordinal % uses.length] ?? "Other";
        for (let copy = 0; copy < 6; copy += 1) {
          const folio = `FOLIO-${String(ordinal).padStart(3, "0")}-${copy}`;
          candidates.set(folio, candidate(folio, city, year, use));
        }
        ordinal += 1;
      }
    }

    const seed = "bounded-scale-test-seed";
    const first = selectCountywideSample(candidates, seed, 20);
    const repeated = selectCountywideSample(candidates, seed, 20);
    const expanded = selectCountywideSample(candidates, seed, 40);

    expect(first).toEqual(repeated);
    expect(expanded.slice(0, first.length)).toEqual(first);
    expect(new Set(first.map(countywideStratum))).toHaveLength(15);
    expect(new Set(first.map((entry) => entry.yearBucket))).toHaveLength(5);
    expect(new Set(first.map((entry) => entry.siteAddress?.city))).toHaveLength(
      3,
    );
    expect(new Set(first.map((entry) => entry.useGroup))).toHaveLength(3);
    expect(COUNTYWIDE_SAMPLE_ALGORITHM).toBe(
      "pasco-countywide-proportional-prefix-v1",
    );
  });
});
