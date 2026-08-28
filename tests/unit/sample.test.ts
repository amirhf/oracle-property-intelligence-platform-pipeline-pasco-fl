import { describe, expect, it } from "vitest";

import type { PilotCandidate } from "../../src/domain/types.js";
import { selectPilot } from "../../src/pilot/sample.js";

function candidate(index: number): PilotCandidate {
  const years = [1945, 1968, 1988, 2005, 2018];
  const uses = [
    "Single Family",
    "Condominium",
    "Manufactured Home",
    "Commercial Office",
    "Agricultural",
  ];
  return {
    buildings: [
      {
        actualYearBuilt: years[index % years.length] ?? 2000,
        buildingNumber: "1",
        buildingSection: "1",
        effectiveYearBuilt: null,
        exactFolio: `FOLIO-${index}`,
        heatedSquareFeet: 1000 + index,
        observedCondition: null,
        roofCover: "SHINGLE",
        roofStructure: null,
        stories: 1,
        totalSquareFeet: 1200 + index,
        useDescription: uses[index % uses.length] ?? "Other",
      },
    ],
    parcel: {
      acres: 0.25,
      exactFolio: `FOLIO-${index}`,
      heatedSquareFeet: 1000,
      homestead: null,
      neighborhoodCode: null,
      propertyUseCode: `${index % 5}`,
      propertyUseDescription: uses[index % uses.length] ?? "Other",
      totalSquareFeet: 1200,
    },
    siteAddress: {
      city: `CITY-${index % 10}`,
      exactFolio: `FOLIO-${index}`,
      siteAddress: `${index} TEST RD`,
      zipCode: "00000",
    },
  };
}

describe("deterministic Pasco pilot selection", () => {
  it("selects the same diverse 25 properties for the same seed", () => {
    const candidates = new Map(
      Array.from({ length: 100 }, (_, index) => [
        `FOLIO-${index}`,
        candidate(index),
      ]),
    );
    const first = selectPilot(candidates, "seed");
    const second = selectPilot(candidates, "seed");
    expect(first.map((row) => row.propertyId)).toEqual(
      second.map((row) => row.propertyId),
    );
    expect(first).toHaveLength(25);
    expect(new Set(first.map((row) => row.yearBucket)).size).toBe(5);
    expect(
      new Set(first.map((row) => row.useGroup)).size,
    ).toBeGreaterThanOrEqual(5);
    expect(
      new Set(first.map((row) => row.siteAddress?.city)).size,
    ).toBeGreaterThanOrEqual(8);
  });
});
