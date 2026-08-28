import type { PilotCandidate, PilotSelectionEntry } from "../domain/types.js";
import { propertyId, sha256 } from "../lib/hash.js";

export const PILOT_SAMPLE_ALGORITHM = "pasco-pilot-stratified-v1" as const;
export const PILOT_SIZE = 25;

export function constructionYear(candidate: PilotCandidate): number | null {
  const years = candidate.buildings
    .flatMap((building) => [
      building.actualYearBuilt,
      building.effectiveYearBuilt,
    ])
    .filter((value): value is number => value !== null);
  return years.length > 0 ? Math.min(...years) : null;
}

export function constructionYearBucket(year: number): string {
  if (year < 1960) return "pre_1960";
  if (year < 1980) return "1960_1979";
  if (year < 2000) return "1980_1999";
  if (year < 2010) return "2000_2009";
  return "2010_plus";
}

export function propertyUseGroup(value: string | null): string {
  const description = value?.toLowerCase() ?? "";
  if (/single family|residential/.test(description)) return "residential";
  if (/condo|apartment|multi/.test(description)) return "multifamily";
  if (/mobile|manufactured/.test(description)) return "manufactured";
  if (/commercial|office|retail|hotel|motel/.test(description)) {
    return "commercial";
  }
  if (/agric|farm|ranch/.test(description)) return "agricultural";
  return "other";
}

function addBest(
  selected: PilotSelectionEntry[],
  selectedFolios: Set<string>,
  candidates: PilotSelectionEntry[],
  predicate: (candidate: PilotSelectionEntry) => boolean,
): void {
  const next = candidates.find(
    (candidate) =>
      !selectedFolios.has(candidate.parcel.exactFolio) && predicate(candidate),
  );
  if (!next) return;
  selected.push(next);
  selectedFolios.add(next.parcel.exactFolio);
}

export function selectPilot(
  input: ReadonlyMap<string, PilotCandidate>,
  seed: string,
  size = PILOT_SIZE,
): PilotSelectionEntry[] {
  const eligible: PilotSelectionEntry[] = [];
  for (const candidate of input.values()) {
    const yearBuilt = constructionYear(candidate);
    if (yearBuilt === null || !candidate.siteAddress?.city) continue;
    const exactFolio = candidate.parcel.exactFolio;
    eligible.push({
      ...candidate,
      propertyId: propertyId(exactFolio),
      rank: sha256(JSON.stringify([PILOT_SAMPLE_ALGORITHM, seed, exactFolio])),
      useGroup: propertyUseGroup(candidate.parcel.propertyUseDescription),
      yearBucket: constructionYearBucket(yearBuilt),
      yearBuilt,
    });
  }
  eligible.sort(
    (left, right) =>
      left.rank.localeCompare(right.rank) ||
      left.parcel.exactFolio.localeCompare(right.parcel.exactFolio),
  );
  if (eligible.length < size) {
    throw new Error(`Only ${eligible.length} records are pilot-eligible`);
  }

  const selected: PilotSelectionEntry[] = [];
  const selectedFolios = new Set<string>();
  const yearBuckets = [
    ...new Set(eligible.map((row) => row.yearBucket)),
  ].sort();
  const useGroups = [...new Set(eligible.map((row) => row.useGroup))].sort();
  const cityCounts = new Map<string, number>();
  for (const row of eligible) {
    const city = row.siteAddress?.city?.toUpperCase();
    if (city) cityCounts.set(city, (cityCounts.get(city) ?? 0) + 1);
  }
  const targetCities = [...cityCounts]
    .sort(
      ([leftCity, leftCount], [rightCity, rightCount]) =>
        rightCount - leftCount || leftCity.localeCompare(rightCity),
    )
    .slice(0, 8)
    .map(([city]) => city);

  for (const bucket of yearBuckets) {
    addBest(
      selected,
      selectedFolios,
      eligible,
      (row) => row.yearBucket === bucket,
    );
  }
  for (const group of useGroups.slice(0, 6)) {
    addBest(
      selected,
      selectedFolios,
      eligible,
      (row) => row.useGroup === group,
    );
  }
  for (const city of targetCities) {
    addBest(
      selected,
      selectedFolios,
      eligible,
      (row) => row.siteAddress?.city?.toUpperCase() === city,
    );
  }
  while (selected.length < size) {
    addBest(selected, selectedFolios, eligible, () => true);
  }

  return selected
    .slice(0, size)
    .sort((left, right) => left.rank.localeCompare(right.rank));
}
