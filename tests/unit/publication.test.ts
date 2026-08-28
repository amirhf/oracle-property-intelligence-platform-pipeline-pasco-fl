import { describe, expect, it } from "vitest";

import {
  queryTableColumns,
  unavailablePublicationFields,
} from "../../src/publication/dry-run.js";

describe("local publication dry run", () => {
  it("keeps the frozen searchable property fields and explicit availability columns", () => {
    expect(queryTableColumns()).toMatchObject({
      property_id: "VARCHAR",
      parcel_id: "VARCHAR",
      county: "VARCHAR",
      exact_folio: "VARCHAR",
      latitude: "DOUBLE",
      longitude: "DOUBLE",
      year_built: "INTEGER",
      roof_age_years: "INTEGER",
      roof_age_basis: "VARCHAR",
      permit_source_availability: "VARCHAR",
      contractor_source_availability: "VARCHAR",
      sunbiz_source_availability: "VARCHAR",
      bbb_source_availability: "VARCHAR",
      open_roofing_permit_count: "INTEGER",
      maximum_open_roofing_permit_days: "INTEGER",
    });
  });

  it("represents unavailable permit aggregates as null rather than zero", () => {
    expect(unavailablePublicationFields()).toEqual({
      permit_source_availability: "unavailable",
      contractor_source_availability: "unavailable",
      sunbiz_source_availability: "unavailable",
      bbb_source_availability: "unavailable",
      open_roofing_permit_count: null,
      maximum_open_roofing_permit_days: null,
    });
  });
});
