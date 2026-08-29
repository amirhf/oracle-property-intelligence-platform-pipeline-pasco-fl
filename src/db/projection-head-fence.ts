import type postgres from "postgres";

import { sha256 } from "../lib/hash.js";

function signedInt32(hex: string): number {
  const value = Number.parseInt(hex.slice(0, 8), 16);
  return value > 0x7fffffff ? value - 0x1_0000_0000 : value;
}

// One short-lived county fence serializes every projection-head advance with
// approval, execution admission, and future irreversible publication effects.
// It is transaction-scoped and must never be held across provider calls.
export function pascoProjectionHeadFenceKey(): readonly [number, number] {
  return [
    signedInt32(sha256("prism-oracle-projection-head-v1")),
    signedInt32(sha256("pasco")),
  ] as const;
}

export async function acquirePascoProjectionHeadFence(
  transaction: postgres.TransactionSql,
): Promise<void> {
  const [namespaceKey, countyKey] = pascoProjectionHeadFenceKey();
  await transaction`SELECT pg_advisory_xact_lock(${namespaceKey}, ${countyKey})`;
}
