import { createReadStream } from "node:fs";
import { open } from "node:fs/promises";

import { parse } from "csv-parse";

import type {
  BuildingSourceRow,
  OwnerSourceRow,
  ParcelSourceRow,
  PilotCandidate,
  SiteAddressSourceRow,
  SourceParseCount,
  SourceParseCounts,
} from "../domain/types.js";

type CsvRow = Record<string, string>;

export const APPRAISER_HEADERS = {
  building: [
    "Parcel_Num",
    "Bldg_Num",
    "Bldg_Section",
    "Bldg_Style",
    "Bldg_Style_Desc",
    "Bldg_EffYrBlt",
    "Bldg_ActYrBlt",
    "Bldg_Bathrooms",
    "Bldg_Use_Code",
    "Bldg_Use_Desc",
    "Bldg_Quality",
    "Bldg_Quality_Desc",
    "Bldg_ExtWall_1",
    "Bldg_ExtWall_1_Desc",
    "Bldg_ExtWall_2",
    "Bldg_ExtWall_2_Desc",
    "Bldg_Observerd_Condition",
    "Bldg_Roof_Cover",
    "Bldg_Roof_Cover_Desc",
    "Bldg_Roof_Struct",
    "Bldg_Roof_Structure_Desc",
    "Bldg_AC_Type",
    "Bldg_AC_Type_Desc",
    "Bldg_Heat_Type",
    "Bldg_Heat_Type_Desc",
    "Bldg_Heat_System",
    "Bldg_Heat_System_Desc",
    "Bldg_Stories",
    "Bldg_Floor_1",
    "Bldg_Floor_1_Desc",
    "Bldg_Floor_2",
    "Bldg_Floor_2_Desc",
    "Bldg_IntWall_1",
    "Bldg_IntWall_1_Desc",
    "Bldg_IntWall_2",
    "Bldg_IntWall_2_Desc",
    "Bldg_Total_Sqft",
    "Bldg_Heated_Sqft",
    "Bldg_Value",
  ],
  owners: [
    "Parcel_Num",
    "Owner_Mail_Name1",
    "Owner_Mail_Name2",
    "Owner_Mail_Addr1",
    "Owner_Mail_Addr2",
    "Owner_Mail_City",
    "Owner_Mail_State",
    "Owner_Mail_Zip",
    "Owner_Mail_Country",
  ],
  parcel: [
    "Parcel_Num",
    "Tax_Area",
    "Prop_Use_Code",
    "Prop_Use_Desc",
    "NBHD_Code",
    "Hmstd",
    "TotSqFt",
    "HstSqFt",
    "Acres",
    "Just_Value",
    "County_Assessed",
    "County_Exemptions",
    "County_Taxable",
    "School_Assessed",
    "School_Exemptions",
    "School_Taxable",
    "NBHD_Desc",
  ],
  siteAddresses: [
    "PARCEL",
    "ADDRESS_NUMBER",
    "STREET_NAME",
    "STREET_SUFFIX",
    "CITY",
    "ZIP_CODE",
    "UNIT_TYPE",
    "UNIT_IDENTIFIER",
  ],
} as const;

export async function assertExactCsvHeader(
  filePath: string,
  expected: readonly string[],
): Promise<void> {
  const handle = await open(filePath, "r");
  try {
    const buffer = Buffer.alloc(8_192);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const firstLine = buffer
      .subarray(0, bytesRead)
      .toString("utf8")
      .replace(/^\uFEFF/, "")
      .split(/\r?\n/, 1)[0];
    const actual =
      firstLine?.split(",").map((value) => value.replace(/^"|"$/g, "")) ?? [];
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error("appraiser CSV header mismatch");
    }
  } finally {
    await handle.close();
  }
}

function clean(value: string | undefined): string | null {
  const result = value?.trim() ?? "";
  return result.length > 0 ? result : null;
}

function exactFolio(value: string | undefined): string {
  const folio = clean(value);
  if (!folio) throw new Error("missing exact folio");
  return folio;
}

function optionalNumber(value: string | undefined): number | null {
  const normalized = clean(value)?.replaceAll(",", "");
  if (!normalized) return null;
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) throw new Error(`invalid number: ${value}`);
  return parsed;
}

function optionalInteger(value: string | undefined): number | null {
  const parsed = optionalNumber(value);
  if (parsed === null) return null;
  if (!Number.isInteger(parsed)) throw new Error(`invalid integer: ${value}`);
  return parsed;
}

function optionalYear(value: string | undefined): number | null {
  const parsed = optionalInteger(value);
  if (parsed === null || parsed === 0) return null;
  if (parsed < 1700 || parsed > 2026) {
    throw new Error(`invalid construction year: ${value}`);
  }
  return parsed;
}

export function parseParcelRow(row: CsvRow): ParcelSourceRow {
  return {
    acres: optionalNumber(row.Acres),
    countyAssessed: optionalNumber(row.County_Assessed),
    countyTaxable: optionalNumber(row.County_Taxable),
    exactFolio: exactFolio(row.Parcel_Num),
    heatedSquareFeet: optionalNumber(row.HstSqFt),
    homestead: clean(row.Hmstd),
    justValue: optionalNumber(row.Just_Value),
    neighborhoodCode: clean(row.NBHD_Code),
    propertyUseCode: clean(row.Prop_Use_Code),
    propertyUseDescription: clean(row.Prop_Use_Desc),
    totalSquareFeet: optionalNumber(row.TotSqFt),
  };
}

export function parseBuildingRow(row: CsvRow): BuildingSourceRow {
  return {
    actualYearBuilt: optionalYear(row.Bldg_ActYrBlt),
    buildingNumber: clean(row.Bldg_Num) ?? "0",
    buildingSection: clean(row.Bldg_Section) ?? "0",
    effectiveYearBuilt: optionalYear(row.Bldg_EffYrBlt),
    exactFolio: exactFolio(row.Parcel_Num),
    heatedSquareFeet: optionalNumber(row.Bldg_Heated_Sqft),
    observedCondition: clean(
      row.Bldg_Observerd_Condition ?? row.Bldg_Observed_Condition,
    ),
    roofCover: clean(row.Bldg_Roof_Cover_Desc),
    roofStructure: clean(
      row.Bldg_Roof_Structure_Desc ?? row.Bldg_Roof_Struct_Desc,
    ),
    stories: optionalNumber(row.Bldg_Stories),
    totalSquareFeet: optionalNumber(row.Bldg_Total_Sqft),
    useDescription: clean(row.Bldg_Use_Desc),
  };
}

export function parseSiteAddressRow(row: CsvRow): SiteAddressSourceRow {
  const parts = [
    clean(row.ADDRESS_NUMBER),
    clean(row.STREET_NAME),
    clean(row.STREET_SUFFIX),
    clean(row.UNIT_TYPE),
    clean(row.UNIT_IDENTIFIER),
  ].filter((value): value is string => value !== null);
  if (parts.length === 0) throw new Error("missing site address");
  return {
    city: clean(row.CITY),
    exactFolio: exactFolio(row.PARCEL),
    siteAddress: parts.join(" "),
    zipCode: clean(row.ZIP_CODE),
  };
}

export function parseOwnerRow(row: CsvRow): OwnerSourceRow {
  return {
    exactFolio: exactFolio(row.Parcel_Num),
    mailingAddress1: clean(row.Owner_Mail_Addr1),
    mailingAddress2: clean(row.Owner_Mail_Addr2),
    mailingCity: clean(row.Owner_Mail_City),
    mailingCountry: clean(row.Owner_Mail_Country),
    mailingState: clean(row.Owner_Mail_State),
    mailingZip: clean(row.Owner_Mail_Zip),
    ownerName1: clean(row.Owner_Mail_Name1),
    ownerName2: clean(row.Owner_Mail_Name2),
  };
}

async function* csvRows(
  filePath: string,
  onMalformed: () => void,
): AsyncGenerator<CsvRow> {
  const parser = createReadStream(filePath).pipe(
    parse({
      bom: true,
      columns: true,
      on_skip: () => {
        onMalformed();
        return undefined;
      },
      relax_quotes: false,
      skip_records_with_error: true,
      skip_empty_lines: true,
      trim: false,
    }),
  );
  for await (const row of parser) yield row as CsvRow;
}

function emptyCount(): SourceParseCount {
  return {
    accepted: 0,
    parsed: 0,
    rejectionReasons: {},
    rejected: 0,
    source: 0,
  };
}

function recordRejection(count: SourceParseCount, reason: string): void {
  count.rejected += 1;
  count.rejectionReasons[reason] = (count.rejectionReasons[reason] ?? 0) + 1;
}

async function parseInto<T>(options: {
  filePath: string;
  onAccepted: (value: T) => void;
  parseRow: (row: CsvRow) => T;
}): Promise<SourceParseCount> {
  const count = emptyCount();
  for await (const row of csvRows(options.filePath, () => {
    count.source += 1;
    recordRejection(count, "malformed_csv");
  })) {
    count.source += 1;
    try {
      const value = options.parseRow(row);
      count.parsed += 1;
      options.onAccepted(value);
      count.accepted += 1;
    } catch (error) {
      const reason =
        error instanceof Error
          ? error.message.replace(/:.*/, "").replaceAll(" ", "_")
          : "unknown_parse_error";
      recordRejection(count, reason);
    }
  }
  return count;
}

export interface AppraiserInputPaths {
  building: string;
  owners: string;
  parcel: string;
  siteAddresses: string;
}

export async function loadPilotCandidateData(
  paths: AppraiserInputPaths,
): Promise<{
  candidates: Map<string, PilotCandidate>;
  counts: SourceParseCounts;
  matchedRows: { building: number; siteAddresses: number };
}> {
  const candidates = new Map<string, PilotCandidate>();
  const counts: SourceParseCounts = {};
  const matchedRows = { building: 0, siteAddresses: 0 };

  counts.parcel = await parseInto({
    filePath: paths.parcel,
    onAccepted: (parcel) => {
      candidates.set(parcel.exactFolio, {
        buildings: [],
        parcel,
        siteAddress: null,
      });
    },
    parseRow: parseParcelRow,
  });

  counts.building = await parseInto({
    filePath: paths.building,
    onAccepted: (building) => {
      const candidate = candidates.get(building.exactFolio);
      if (!candidate) return;
      candidate.buildings.push(building);
      matchedRows.building += 1;
    },
    parseRow: parseBuildingRow,
  });

  counts.siteAddresses = await parseInto({
    filePath: paths.siteAddresses,
    onAccepted: (siteAddress) => {
      const candidate = candidates.get(siteAddress.exactFolio);
      if (!candidate) return;
      matchedRows.siteAddresses += 1;
      if (
        candidate.siteAddress === null ||
        siteAddress.siteAddress.localeCompare(
          candidate.siteAddress.siteAddress,
        ) < 0
      ) {
        candidate.siteAddress = siteAddress;
      }
    },
    parseRow: parseSiteAddressRow,
  });

  return { candidates, counts, matchedRows };
}

export async function loadSelectedOwners(
  ownersPath: string,
  selectedFolios: ReadonlySet<string>,
): Promise<{
  count: SourceParseCount;
  owners: Map<string, OwnerSourceRow[]>;
}> {
  const owners = new Map<string, OwnerSourceRow[]>();
  const count = await parseInto({
    filePath: ownersPath,
    onAccepted: (owner) => {
      if (!selectedFolios.has(owner.exactFolio)) return;
      const existing = owners.get(owner.exactFolio) ?? [];
      existing.push(owner);
      owners.set(owner.exactFolio, existing);
    },
    parseRow: parseOwnerRow,
  });
  for (const entries of owners.values()) {
    entries.sort((left, right) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right)),
    );
  }
  return { count, owners };
}
