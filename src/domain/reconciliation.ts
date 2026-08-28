export type DeltaClassification = "changed" | "new" | "unchanged";

export function classifyHashDelta(
  existingHash: string | null,
  incomingHash: string,
): DeltaClassification {
  if (existingHash === null) return "new";
  return existingHash === incomingHash ? "unchanged" : "changed";
}

export function duplicateValueCount(values: readonly string[]): number {
  const counts = new Map<string, number>();
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
  return [...counts.values()].filter((count) => count > 1).length;
}

export function reconcilePilotIdentity(
  exactFolios: readonly string[],
  expectedCount = 25,
): {
  duplicateCount: number;
  expectedCount: number;
  ok: boolean;
  recordCount: number;
} {
  const duplicateCount = duplicateValueCount(exactFolios);
  return {
    duplicateCount,
    expectedCount,
    ok: exactFolios.length === expectedCount && duplicateCount === 0,
    recordCount: exactFolios.length,
  };
}
