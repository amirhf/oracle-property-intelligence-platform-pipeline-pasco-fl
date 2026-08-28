import { describe, expect, it } from "vitest";

import {
  isRoofingRelevant,
  normalizePermitStatus,
  wholeUtcDays,
  yearBuiltRoofProxy,
} from "../../src/domain/signals.js";
import { parseAccelaSearchResults } from "../../src/permits/accela.js";

describe("roof signal and permit semantics", () => {
  it("never represents construction year as a roof installation fact", () => {
    expect(yearBuiltRoofProxy(1999, "2026-08-28T00:00:00.000Z")).toEqual({
      ageYears: 27,
      asOf: "2026-08-28T00:00:00.000Z",
      basis: "year_built_proxy",
      basisQuality: "proxy",
      precision: "year",
    });
  });

  it("computes whole UTC permit duration deterministically", () => {
    expect(wholeUtcDays("2024-02-28", "2024-03-01T23:59:59Z")).toBe(2);
    expect(() => wholeUtcDays("2025-01-01", "2024-01-01")).toThrow("precedes");
  });

  it("normalizes statuses and roofing relevance with versionable rules", () => {
    expect(normalizePermitStatus("Expired Permit")).toBe("expired");
    expect(normalizePermitStatus("Issued")).toBe("open");
    expect(normalizePermitStatus("Complete")).toBe("closed");
    expect(isRoofingRelevant("Residential Re-Roof", null)).toBe(true);
    expect(isRoofingRelevant("Electrical", "panel replacement")).toBe(false);
  });

  it("parses roofing rows and ignores unrelated rows", () => {
    const rows = parseAccelaSearchResults(`
      <table>
        <tr><th>Date</th><th>Record Number</th><th>Record Type</th><th>Description</th><th>Project Name</th><th>Status</th><th>Address</th></tr>
        <tr><td>01/02/2024</td><td>R-1</td><td>Residential Re-Roof</td><td>Replace roof</td><td></td><td>Issued</td><td>100 TEST RD</td></tr>
        <tr><td>01/03/2024</td><td>E-1</td><td>Electrical</td><td>Panel</td><td></td><td>Closed</td><td>100 TEST RD</td></tr>
      </table>
    `);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.recordNumber).toBe("R-1");
  });
});
