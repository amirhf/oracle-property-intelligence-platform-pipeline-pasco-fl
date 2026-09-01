import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import {
  parseBuildingRow,
  parseOwnerRow,
  parseParcelRow,
  parseSiteAddressRow,
  loadSelectedOwners,
} from "../../src/appraiser/parser.js";

describe("Pasco appraiser source parsing", () => {
  it("preserves the exact folio and parses useful appraisal fields", () => {
    const parcel = parseParcelRow({
      Acres: "0.25",
      Hmstd: "YES",
      HstSqFt: "1,500",
      NBHD_Code: "N1",
      Parcel_Num: "12-34-56-7890",
      Prop_Use_Code: "00100",
      Prop_Use_Desc: "Single Family",
      TotSqFt: "2200",
    });
    expect(parcel.exactFolio).toBe("12-34-56-7890");
    expect(parcel.heatedSquareFeet).toBe(1500);
    expect(parcel.acres).toBe(0.25);
  });

  it("parses building, address, and owner rows without inventing fields", () => {
    expect(
      parseBuildingRow({
        Bldg_ActYrBlt: "1988",
        Bldg_EffYrBlt: "2005",
        Bldg_Num: "1",
        Bldg_Roof_Cover_Desc: "SHINGLE",
        Bldg_Section: "1",
        Parcel_Num: "A-1",
      }).roofCover,
    ).toBe("SHINGLE");
    expect(
      parseSiteAddressRow({
        ADDRESS_NUMBER: "100",
        CITY: "DADE CITY",
        PARCEL: "A-1",
        STREET_NAME: "TEST",
        STREET_SUFFIX: "RD",
      }).siteAddress,
    ).toBe("100 TEST RD");
    const owner = parseOwnerRow({ Parcel_Num: "A-1" });
    expect(owner.ownerName1).toBeNull();
    expect(owner.mailingAddress1).toBeNull();
  });

  it("rejects malformed required and numeric values", () => {
    expect(() => parseParcelRow({ Parcel_Num: "" })).toThrow(
      "missing exact folio",
    );
    expect(() =>
      parseParcelRow({ Acres: "not-a-number", Parcel_Num: "A-1" }),
    ).toThrow("invalid number");
    expect(() =>
      parseBuildingRow({ Bldg_ActYrBlt: "1492", Parcel_Num: "A-1" }),
    ).toThrow("invalid construction year");
    expect(() => parseSiteAddressRow({ PARCEL: "A-1" })).toThrow(
      "missing site address",
    );
  });

  it("counts and skips a malformed CSV record without exposing its contents", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "pasco-parser-"));
    const source = path.join(directory, "owners.csv");
    try {
      await writeFile(
        source,
        '"Parcel_Num","Owner_Mail_Name1","Owner_Mail_Addr1"\n"A-1","VALID","VALUE"\n"A-2","BROKEN","UNTERMINATED\n',
      );
      const result = await loadSelectedOwners(source, new Set(["A-1", "A-2"]));
      expect(result.count).toEqual({
        accepted: 1,
        parsed: 1,
        rejectionReasons: { malformed_csv: 1 },
        rejected: 1,
        source: 2,
      });
      expect(result.owners.has("A-2")).toBe(false);
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });
});
