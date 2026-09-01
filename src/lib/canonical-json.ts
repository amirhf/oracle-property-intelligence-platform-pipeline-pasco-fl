import { sha256 } from "./hash.js";

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

export function canonicalJson(value: unknown): string {
  const encoded = JSON.stringify(stableValue(value));
  if (encoded === undefined) {
    throw new Error("Value cannot be represented as canonical JSON");
  }
  return encoded;
}

export function canonicalJsonSha256(value: unknown): string {
  return sha256(canonicalJson(value));
}
