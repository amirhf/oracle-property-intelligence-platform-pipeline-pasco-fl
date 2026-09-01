import { describe, expect, it } from "vitest";

import {
  AUTHORITATIVE_DISK_RESERVE_BYTES,
  authoritativePublicationCardinality,
  preflightAuthoritativePublicationResources,
} from "../../src/publication/resource-preflight.js";

describe("authoritative publication resource preflight", () => {
  it("derives the exact full graph, inventory and edge cardinalities", () => {
    expect(authoritativePublicationCardinality(325_213)).toEqual({
      edgeCount: 325_246,
      inventoryObjectCount: 325_253,
      shardCount: 33,
    });
  });

  it("uses measured sealed-projection bytes and reserves room for two builds", () => {
    const result = preflightAuthoritativePublicationResources({
      availableBytes: 30 * 1024 ** 3,
      availableFiles: 2_000_000,
      factCount: 3_309_790,
      propertyCount: 325_213,
      sourcePayloadBytes: 965_152_878,
    });
    expect(result.estimatedBuildBytes).toBe(965_152_878 * 4 + 325_213 * 4_096);
    expect(result.requiredPeakBytes).toBe(
      result.estimatedBuildBytes * 2 + AUTHORITATIVE_DISK_RESERVE_BYTES,
    );
    expect(result.passed).toBe(true);
  });

  it("fails before artifact creation when disk, inode or input bounds are unsafe", () => {
    const base = {
      availableBytes: 30 * 1024 ** 3,
      availableFiles: 2_000_000,
      factCount: 3_309_790,
      propertyCount: 325_213,
      sourcePayloadBytes: 965_152_878,
    };
    expect(() =>
      preflightAuthoritativePublicationResources({
        ...base,
        availableBytes: 1,
      }),
    ).toThrow("disk preflight failed before artifact creation");
    expect(() =>
      preflightAuthoritativePublicationResources({
        ...base,
        availableFiles: 1,
      }),
    ).toThrow("inode preflight failed before artifact creation");
    expect(() =>
      preflightAuthoritativePublicationResources({
        ...base,
        factCount: 4_000_001,
      }),
    ).toThrow("fact limit exceeded");
  });
});
