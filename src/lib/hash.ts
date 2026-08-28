import { createHash } from "node:crypto";

export function sha256(value: string | Buffer): string {
  return createHash("sha256").update(value).digest("hex");
}

export function canonicalArray(values: readonly string[]): string {
  return JSON.stringify(values);
}

export function deterministicId(
  prefix: string,
  values: readonly string[],
): string {
  if (!/^[a-z]+$/.test(prefix)) {
    throw new Error(`Invalid deterministic identifier prefix: ${prefix}`);
  }
  return `${prefix}_${sha256(canonicalArray(values)).slice(0, 32)}`;
}

export function propertyId(exactFolio: string): string {
  return deterministicId("property", [
    "1.0.0",
    "property",
    "pasco",
    "pasco_appraiser",
    exactFolio,
  ]);
}

export function parcelId(exactFolio: string): string {
  return deterministicId("parcel", [
    "1.0.0",
    "parcel",
    "pasco",
    "pasco_appraiser",
    exactFolio,
  ]);
}

export function permitId(sourceRecordKey: string): string {
  return deterministicId("permit", [
    "1.0.0",
    "permit",
    "pasco",
    "pasco_accela",
    sourceRecordKey,
  ]);
}

export function sourceRecordHash(value: unknown): string {
  return `sha256:${sha256(JSON.stringify(value))}`;
}
