import type { PilotCandidate, PilotSelectionEntry } from "../domain/types.js";
import { propertyId, sha256 } from "../lib/hash.js";
import {
  constructionYear,
  constructionYearBucket,
  propertyUseGroup,
} from "../pilot/sample.js";

export const COUNTYWIDE_SAMPLE_ALGORITHM =
  "pasco-countywide-proportional-prefix-v1" as const;
export const COUNTYWIDE_SAMPLE_SEED =
  "prism-pasco-appraisal-gis-scale-2026-08-28" as const;

interface RankedCandidate {
  denominator: number;
  numerator: number;
  row: PilotSelectionEntry;
  stratum: string;
  stratumRank: string;
}

export function countywideStratum(entry: PilotSelectionEntry): string {
  const city =
    entry.siteAddress?.city?.trim().replace(/\s+/g, " ").toUpperCase() ??
    "UNKNOWN";
  return [city, entry.yearBucket, entry.useGroup].join("|");
}

function compareFractions(
  left: RankedCandidate,
  right: RankedCandidate,
): number {
  const leftValue = BigInt(left.numerator) * BigInt(right.denominator);
  const rightValue = BigInt(right.numerator) * BigInt(left.denominator);
  if (leftValue < rightValue) return -1;
  if (leftValue > rightValue) return 1;
  return (
    left.stratumRank.localeCompare(right.stratumRank) ||
    left.row.rank.localeCompare(right.row.rank) ||
    left.row.parcel.exactFolio.localeCompare(right.row.parcel.exactFolio)
  );
}

export function selectCountywideSample(
  input: ReadonlyMap<string, PilotCandidate>,
  seed: string,
  size: number,
): PilotSelectionEntry[] {
  if (!Number.isInteger(size) || size <= 0) {
    throw new Error("Countywide sample size must be a positive integer");
  }
  const eligible: PilotSelectionEntry[] = [];
  for (const candidate of input.values()) {
    const yearBuilt = constructionYear(candidate);
    if (yearBuilt === null || !candidate.siteAddress?.city) continue;
    const exactFolio = candidate.parcel.exactFolio;
    eligible.push({
      ...candidate,
      propertyId: propertyId(exactFolio),
      rank: sha256(
        JSON.stringify([COUNTYWIDE_SAMPLE_ALGORITHM, seed, exactFolio]),
      ),
      useGroup: propertyUseGroup(candidate.parcel.propertyUseDescription),
      yearBucket: constructionYearBucket(yearBuilt),
      yearBuilt,
    });
  }
  if (eligible.length < size) {
    throw new Error(`Only ${eligible.length} records are scale-eligible`);
  }

  const strata = new Map<string, PilotSelectionEntry[]>();
  for (const entry of eligible) {
    const key = countywideStratum(entry);
    const rows = strata.get(key) ?? [];
    rows.push(entry);
    strata.set(key, rows);
  }

  const ranked: RankedCandidate[] = [];
  for (const [stratum, rows] of strata) {
    rows.sort(
      (left, right) =>
        left.rank.localeCompare(right.rank) ||
        left.parcel.exactFolio.localeCompare(right.parcel.exactFolio),
    );
    const stratumRank = sha256(
      JSON.stringify([COUNTYWIDE_SAMPLE_ALGORITHM, seed, stratum]),
    );
    rows.forEach((row, index) => {
      ranked.push({
        denominator: rows.length,
        numerator: index + 1,
        row,
        stratum,
        stratumRank,
      });
    });
  }
  ranked.sort(compareFractions);
  return ranked.slice(0, size).map(({ row }) => row);
}
