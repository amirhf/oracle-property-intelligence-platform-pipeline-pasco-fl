import { describe, expect, it } from "vitest";

import { sourceRecordHash } from "../../src/lib/hash.js";
import {
  buildMaterializedCanonicalProperty,
  meaningfulSitusAddress,
  type MaterializedPublicationFact,
} from "../../src/publication/canonical-property.js";

const SNAPSHOT = `snapshot_${"a".repeat(32)}`;
const RUN = `run_${"b".repeat(32)}`;

function fact(
  factType: string,
  naturalKey: string,
  payload: unknown,
): MaterializedPublicationFact {
  return {
    evidenceRefs: [sourceRecordHash({ exactFolio: "SYNTHETIC-1" })],
    factType,
    naturalKey,
    payload,
    sourceRecordHash: sourceRecordHash(payload),
    sourceRunId: RUN,
    sourceSnapshotId: SNAPSHOT,
    versionId: `factversion_${sourceRecordHash([factType, naturalKey]).slice(-32)}`,
  };
}

function unavailable(feature: string) {
  return fact("availability", feature, {
    availability: "unavailable",
    feature,
    reason: "not_observed_for_property",
  });
}

function baseFacts() {
  return [
    unavailable("building"),
    unavailable("contractors"),
    unavailable("coordinates"),
    unavailable("ownership"),
    unavailable("permits"),
    unavailable("site_address"),
    unavailable("year_built_proxy"),
  ];
}

function build(options: {
  facts?: MaterializedPublicationFact[];
  latitude?: number | null;
  longitude?: number | null;
  siteAddress?: {
    city: string | null;
    siteAddress: string;
    zipCode: string | null;
  } | null;
  yearBuilt?: number | null;
}) {
  const siteAddress = options.siteAddress ?? null;
  return buildMaterializedCanonicalProperty({
    allowedSnapshotIds: new Set([SNAPSHOT]),
    asOf: "2026-08-23T11:07:02.000Z",
    core: {
      payload: {
        parcel: { exactFolio: "SYNTHETIC-1", useCode: "001" },
        siteAddress,
      },
      sourceRunId: RUN,
      sourceSnapshotId: SNAPSHOT,
      versionId: `propertyversion_${"c".repeat(32)}`,
    },
    facts: options.facts ?? baseFacts(),
    loadedAt: "2026-08-30T00:00:00.000Z",
    parcelObservedAt: "2026-08-23T11:07:02.000Z",
    property: {
      exactFolio: "SYNTHETIC-1",
      latitude: options.latitude ?? null,
      longitude: options.longitude ?? null,
      parcelId: `parcel_${"d".repeat(32)}`,
      propertyId: `property_${"e".repeat(32)}`,
      siteAddress: siteAddress?.siteAddress ?? null,
      siteCity: siteAddress?.city ?? null,
      siteZip: siteAddress?.zipCode ?? null,
      yearBuilt: options.yearBuilt ?? null,
    },
    roofSignal: {
      ageYears: options.yearBuilt === undefined ? null : 10,
      basis: options.yearBuilt === undefined ? null : "year_built_proxy",
      basisQuality: options.yearBuilt === undefined ? null : "proxy",
    },
    sources: {
      appraiserBuildingUrl: "https://official.invalid/building.zip",
      appraiserOwnersUrl: "https://official.invalid/owners.zip",
      appraiserParcelUrl: "https://official.invalid/parcel.zip",
      appraiserSiteAddressUrl: "https://official.invalid/site-address.zip",
      snapshotId: SNAPSHOT,
    },
  });
}

function record(value: unknown): Record<string, unknown> {
  expect(value).toBeTypeOf("object");
  expect(value).not.toBeNull();
  return value as Record<string, unknown>;
}

describe("materialized publication provenance", () => {
  it("keeps parcel-only unavailable facts explicit without fabricating an address", () => {
    const property = build({});
    expect(record(property.situsAddress)).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(record(property.coordinates)).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(record(property.ownership)).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(property.permits).toEqual([]);
    expect(JSON.stringify(property)).not.toContain('"value":"FL"');
  });

  it("binds building, roof proxy, ownership, address and GIS to exact immutable evidence", () => {
    const building = fact("building", "1:1", {
      actualYearBuilt: 2001,
      roofCover: "SYNTHETIC COVER",
      roofStructure: null,
    });
    const roof = fact("roof_signal", "year_built_proxy", {
      ageYears: 25,
      basis: "year_built_proxy",
      basisQuality: "proxy",
    });
    const ownerA = fact("ownership", "owner:0", {
      mailingAddress1: "SYNTHETIC MAIL A",
      ownerName1: "SYNTHETIC OWNER A",
    });
    const ownerB = fact("ownership", "owner:1", {
      mailingAddress1: "SYNTHETIC MAIL B",
      ownerName1: "SYNTHETIC OWNER B",
    });
    const coordinate = fact("coordinate", "pasco_gis:parcel_centroid", {
      latitude: 28.1,
      longitude: -82.4,
      sourceLastUpdate: null,
    });
    const property = build({
      facts: [
        building,
        coordinate,
        ownerA,
        ownerB,
        roof,
        unavailable("contractors"),
        unavailable("permits"),
      ],
      latitude: 28.1,
      longitude: -82.4,
      siteAddress: {
        city: "Synthetic City",
        siteAddress: "1 Synthetic Street",
        zipCode: "00000",
      },
      yearBuilt: 2001,
    });
    const evidence = property.evidence as Array<Record<string, unknown>>;
    const buildingEvidence = evidence.find(
      (entry) => entry.sourceRecordKey === building.versionId,
    );
    const ownerEvidence = evidence.filter((entry) =>
      [ownerA.versionId, ownerB.versionId].includes(
        String(entry.sourceRecordKey),
      ),
    );
    const coordinateEvidence = evidence.find(
      (entry) => entry.sourceRecordKey === coordinate.versionId,
    );
    expect(buildingEvidence).toMatchObject({
      observedAt: null,
      sourceRecordHash: building.sourceRecordHash,
      sourceSystem: "pasco_appraiser",
    });
    expect(ownerEvidence).toHaveLength(2);
    expect(ownerEvidence.every((entry) => entry.observedAt === null)).toBe(
      true,
    );
    expect(coordinateEvidence).toMatchObject({
      observedAt: null,
      sourceRecordHash: coordinate.sourceRecordHash,
      sourceSystem: "pasco_gis",
    });
    expect(record(property.roofAgeSignal).evidenceRefs).toEqual(
      [building.versionId, roof.versionId]
        .map(
          (versionId) =>
            evidence.find((entry) => entry.sourceRecordKey === versionId)!
              .evidenceId,
        )
        .sort(),
    );
    expect(record(property.ownership).evidenceRefs).toHaveLength(2);
    expect(record(property.situsAddress)).toMatchObject({
      availability: "available",
      value: "1 Synthetic Street, Synthetic City, FL, 00000",
    });
  });

  it("treats state-only and punctuation-only addresses as unavailable while preserving honest partials", () => {
    expect(
      meaningfulSitusAddress({
        city: "Florida",
        siteAddress: "FL",
        zipCode: null,
      }),
    ).toBeNull();
    expect(
      meaningfulSitusAddress({ city: " ", siteAddress: "...", zipCode: null }),
    ).toBeNull();
    expect(
      meaningfulSitusAddress({
        city: "Synthetic City",
        siteAddress: " ",
        zipCode: null,
      }),
    ).toBe("Synthetic City, FL");
    const property = build({
      facts: baseFacts().filter((entry) => entry.naturalKey !== "site_address"),
      siteAddress: { city: null, siteAddress: "...", zipCode: null },
    });
    expect(record(property.situsAddress)).toMatchObject({
      availability: "unavailable",
      value: null,
    });
  });

  it("keeps address evidence separate from parcel evidence across selected address variants", () => {
    const first = build({
      facts: baseFacts().filter((entry) => entry.naturalKey !== "site_address"),
      siteAddress: {
        city: "Synthetic City",
        siteAddress: "1 Synthetic Street",
        zipCode: "00000",
      },
    });
    const second = build({
      facts: baseFacts().filter((entry) => entry.naturalKey !== "site_address"),
      siteAddress: {
        city: "Synthetic City",
        siteAddress: "2 Synthetic Street",
        zipCode: "00000",
      },
    });
    const evidence = (property: Record<string, unknown>) =>
      property.evidence as Array<Record<string, unknown>>;
    const bySuffix = (property: Record<string, unknown>, suffix: string) =>
      evidence(property).find((entry) =>
        String(entry.sourceRecordKey).endsWith(suffix),
      );
    expect(bySuffix(first, ":parcel")?.sourceRecordHash).toBe(
      bySuffix(second, ":parcel")?.sourceRecordHash,
    );
    expect(bySuffix(first, ":site-address")?.sourceRecordHash).not.toBe(
      bySuffix(second, ":site-address")?.sourceRecordHash,
    );
  });

  it("rejects malformed ownership facts without a source owner identity", () => {
    const malformed = fact("ownership", "owner:malformed", {
      mailingAddress1: "SYNTHETIC MAIL",
      ownerName1: " ",
      ownerName2: null,
    });
    expect(() =>
      build({
        facts: [
          malformed,
          unavailable("building"),
          unavailable("contractors"),
          unavailable("coordinates"),
          unavailable("permits"),
          unavailable("site_address"),
          unavailable("year_built_proxy"),
        ],
      }),
    ).toThrow("lacks an owner identity");
  });

  it("rejects a fact version from outside the sealed predecessor chain", () => {
    const foreign = fact("ownership", "owner:foreign", {
      ownerName1: "SYNTHETIC FOREIGN",
    });
    foreign.sourceSnapshotId = `snapshot_${"f".repeat(32)}`;
    expect(() =>
      build({
        facts: [
          foreign,
          unavailable("building"),
          unavailable("contractors"),
          unavailable("coordinates"),
          unavailable("permits"),
          unavailable("site_address"),
          unavailable("year_built_proxy"),
        ],
      }),
    ).toThrow("identity is incomplete");
  });
});
