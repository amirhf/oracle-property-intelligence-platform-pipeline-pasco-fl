import { describe, expect, it } from "vitest";

import {
  classifyHashDelta,
  duplicateValueCount,
  reconcilePilotIdentity,
} from "../../src/domain/reconciliation.js";
import {
  deterministicId,
  parcelId,
  permitId,
  propertyId,
} from "../../src/lib/hash.js";

describe("deterministic identifiers and reconciliation", () => {
  it("uses canonical JSON arrays and stable SHA-256 identifiers", () => {
    expect(deterministicId("thing", ["a", "b"])).toBe(
      "thing_0473ef2dc0d324ab659d3580c1134e9d",
    );
    expect(propertyId("01-23-45-6789")).toBe(propertyId("01-23-45-6789"));
    expect(parcelId("01-23-45-6789")).not.toBe(propertyId("01-23-45-6789"));
    expect(permitId("pasco_accela:X-1")).toMatch(/^permit_[a-f0-9]{32}$/);
  });

  it("keeps exact folios distinct even when digits-only matching keys collide", () => {
    expect(propertyId("01-23")).not.toBe(propertyId("0123"));
  });

  it("classifies repeat content as unchanged and detects duplicates", () => {
    expect(classifyHashDelta(null, "sha256:a")).toBe("new");
    expect(classifyHashDelta("sha256:a", "sha256:a")).toBe("unchanged");
    expect(classifyHashDelta("sha256:a", "sha256:b")).toBe("changed");
    expect(duplicateValueCount(["a", "b", "a", "b", "c"])).toBe(2);
    expect(
      reconcilePilotIdentity(Array.from({ length: 25 }, (_, i) => `${i}`)),
    ).toEqual({
      duplicateCount: 0,
      expectedCount: 25,
      ok: true,
      recordCount: 25,
    });
  });
});
