const UTC_DAY_MS = 86_400_000;

export type NormalizedPermitStatus = "closed" | "expired" | "open" | "unknown";

export function isRoofingRelevant(...values: Array<string | null>): boolean {
  const text = values.filter(Boolean).join(" ");
  return /\b(?:re[ -]?roof|roofing|roof over|roof replacement)\b/i.test(text);
}

export function normalizePermitStatus(
  rawStatus: string | null,
): NormalizedPermitStatus {
  const status = rawStatus?.trim().toLowerCase() ?? "";
  if (!status) return "unknown";
  if (/expired/.test(status)) return "expired";
  if (
    /closed|complete|finaled|cancelled|withdrawn|rejected|void/.test(status)
  ) {
    return "closed";
  }
  if (/open|issued|pending|active|review|approved|new/.test(status)) {
    return "open";
  }
  return "unknown";
}

export function wholeUtcDays(startDate: string, asOf: string): number {
  const start = new Date(startDate);
  const end = new Date(asOf);
  if (Number.isNaN(start.valueOf()) || Number.isNaN(end.valueOf())) {
    throw new Error("Permit duration inputs must be valid dates");
  }
  const startUtc = Date.UTC(
    start.getUTCFullYear(),
    start.getUTCMonth(),
    start.getUTCDate(),
  );
  const endUtc = Date.UTC(
    end.getUTCFullYear(),
    end.getUTCMonth(),
    end.getUTCDate(),
  );
  if (endUtc < startUtc) {
    throw new Error("Permit duration as-of date precedes the start date");
  }
  return Math.floor((endUtc - startUtc) / UTC_DAY_MS);
}

export function yearBuiltRoofProxy(yearBuilt: number, asOf: string) {
  const asOfDate = new Date(asOf);
  if (Number.isNaN(asOfDate.valueOf())) throw new Error("Invalid as-of date");
  if (yearBuilt < 1700 || yearBuilt > asOfDate.getUTCFullYear()) {
    throw new Error(`Invalid building year: ${yearBuilt}`);
  }
  return {
    ageYears: asOfDate.getUTCFullYear() - yearBuilt,
    asOf,
    basis: "year_built_proxy" as const,
    basisQuality: "proxy" as const,
    precision: "year" as const,
  };
}
