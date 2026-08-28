import { describe, expect, it } from "vitest";

import { MCP_SCHEMA_SHA256, MCP_TOOL_NAMES } from "../../src/mcp/constants.js";
import type { OracleMcpProvider } from "../../src/mcp/provider.js";
import { haversineMeters, OracleMcpRuntime } from "../../src/mcp/runtime.js";
import { coordinatesSearch, realMcpHarness } from "../helpers/mcp-real.js";

function resultData(result: Record<string, unknown>): Record<string, unknown> {
  return result.data as Record<string, unknown>;
}

describe("Oracle MCP v1.1.0 runtime", () => {
  it("returns contract-valid metadata for all three metadata tools", async () => {
    const { contracts, runtime } = await realMcpHarness();
    for (const tool of [
      "prism_v1_get_service_info",
      "prism_v1_get_pipeline_run_summary",
      "prism_v1_get_query_schema",
    ] as const) {
      const response = await runtime.execute(tool, {});
      expect(response.isError).toBe(false);
      expect(contracts.validateOutput(tool, response.result)).toEqual([]);
      expect((response.result.meta as Record<string, unknown>).schemaHash).toBe(
        MCP_SCHEMA_SHA256,
      );
    }

    const service = await runtime.execute("prism_v1_get_service_info", {});
    expect(resultData(service.result).supportedTools).toEqual(MCP_TOOL_NAMES);
    const summary = await runtime.execute(
      "prism_v1_get_pipeline_run_summary",
      {},
    );
    const coverage = resultData(summary.result).coverage as Record<
      string,
      Record<string, unknown>
    >;
    expect(coverage.coordinates).toEqual({ available: 24_995, unavailable: 5 });
    expect(coverage.permits!.recordCount).toBeNull();
    expect(coverage.permits!.propertyCount).toBeNull();
    expect(coverage.contractors!.recordCount).toBeNull();
  });

  it("searches real Parquet rows and preserves proxy and unavailable semantics", async () => {
    const { provider, runtime } = await realMcpHarness();
    const rows = await provider.getQueryRows();
    const center = rows.find(
      (row) => row.latitude !== null && row.longitude !== null,
    );
    expect(center).toBeDefined();
    const response = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        latitude: center!.latitude!,
        longitude: center!.longitude!,
        limit: 3,
      }),
    );
    expect(response.isError).toBe(false);
    const opportunities = resultData(response.result).opportunities as Array<
      Record<string, unknown>
    >;
    expect(opportunities.length).toBe(3);
    const property = opportunities[0]!.property as Record<string, unknown>;
    const roof = property.roofAgeSignal as Record<string, unknown>;
    expect((roof.value as Record<string, unknown>).basis).toBe(
      "year_built_proxy",
    );
    expect((roof.value as Record<string, unknown>).basisQuality).toBe("proxy");
    expect(property.openRoofingPermitCount).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "source_unavailable",
    });
    expect(property.maximumOpenRoofingPermitDays).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(property.permits).toEqual([]);
    expect(property).not.toHaveProperty("ownership");
    const evidence = property.evidence as Array<Record<string, unknown>>;
    const evidenceIds = new Set(evidence.map((item) => item.evidenceId));
    expect(evidence.length).toBeGreaterThan(0);
    expect(
      evidence.every(
        (item) =>
          typeof item.sourceArtifactUri === "string" &&
          !item.sourceArtifactUri.startsWith("file:"),
      ),
    ).toBe(true);
    const roofEvidenceRefs = (property.roofAgeSignal as Record<string, unknown>)
      .evidenceRefs as string[];
    expect(
      roofEvidenceRefs.every((reference) => evidenceIds.has(reference)),
    ).toBe(true);
    expect(JSON.stringify(response.result)).not.toContain(process.cwd());
  });

  it("paginates deterministically with a query-bound opaque cursor", async () => {
    const { provider, runtime } = await realMcpHarness();
    const center = (await provider.getQueryRows()).find(
      (row) => row.latitude !== null && row.longitude !== null,
    )!;
    const request = coordinatesSearch({
      latitude: center.latitude!,
      longitude: center.longitude!,
      limit: 2,
    });
    const first = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      request,
    );
    const repeated = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      request,
    );
    expect(first.result).toEqual(repeated.result);
    const cursor = (first.result.meta as Record<string, unknown>)
      .nextCursor as string;
    expect(cursor).toBeTypeOf("string");
    expect(cursor).not.toContain(center.propertyId);
    const second = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        cursor,
        latitude: center.latitude!,
        longitude: center.longitude!,
        limit: 2,
      }),
    );
    const ids = (response: Record<string, unknown>) =>
      (
        resultData(response).opportunities as Array<Record<string, unknown>>
      ).map(
        (opportunity) =>
          (opportunity.property as Record<string, unknown>).propertyId,
      );
    expect(ids(second.result)).not.toEqual(ids(first.result));
  });

  it("includes the exact radius boundary and excludes a point just beyond it", async () => {
    const { contracts, provider } = await realMcpHarness();
    const rows = (await provider.getQueryRows()).filter(
      (row) => row.latitude !== null && row.longitude !== null,
    );
    const center = rows[0]!;
    const target = rows
      .slice(1)
      .map((row) => ({
        row,
        distance: haversineMeters(
          center.latitude!,
          center.longitude!,
          row.latitude!,
          row.longitude!,
        ),
      }))
      .filter((entry) => entry.distance > 1)
      .sort((left, right) => left.distance - right.distance)[0]!;
    const subset: OracleMcpProvider = {
      getCanonicalProperty: (...argumentsValue) =>
        provider.getCanonicalProperty(...argumentsValue),
      getMetadata: (...argumentsValue) =>
        provider.getMetadata(...argumentsValue),
      getPermit: (...argumentsValue) => provider.getPermit(...argumentsValue),
      getQueryRows: async () => [center, target.row],
    };
    const runtime = new OracleMcpRuntime(subset, contracts, {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2 * 1024 * 1024,
      requestTimeoutMs: 10_000,
    });
    const atBoundary = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        latitude: center.latitude!,
        longitude: center.longitude!,
        radiusKilometers: target.distance / 1000,
        limit: 10,
      }),
    );
    const insideIds = (
      resultData(atBoundary.result).opportunities as Array<
        Record<string, unknown>
      >
    ).map(
      (opportunity) =>
        (opportunity.property as Record<string, unknown>).propertyId,
    );
    expect(insideIds).toContain(target.row.propertyId);

    const belowBoundary = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        latitude: center.latitude!,
        longitude: center.longitude!,
        radiusKilometers: (target.distance - 0.01) / 1000,
        limit: 10,
      }),
    );
    const outsideIds = (
      resultData(belowBoundary.result).opportunities as Array<
        Record<string, unknown>
      >
    ).map(
      (opportunity) =>
        (opportunity.property as Record<string, unknown>).propertyId,
    );
    expect(outsideIds).not.toContain(target.row.propertyId);
  });

  it("excludes all five missing coordinates from radius search but preserves direct lookup", async () => {
    const { provider, runtime } = await realMcpHarness();
    const rows = await provider.getQueryRows();
    const missing = rows.filter(
      (row) => row.latitude === null || row.longitude === null,
    );
    expect(missing).toHaveLength(5);
    const center = rows.find(
      (row) => row.latitude !== null && row.longitude !== null,
    )!;
    const search = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        latitude: center.latitude!,
        longitude: center.longitude!,
        limit: 100,
      }),
    );
    const resultIds = new Set(
      (
        resultData(search.result).opportunities as Array<
          Record<string, unknown>
        >
      ).map(
        (opportunity) =>
          (opportunity.property as Record<string, unknown>).propertyId,
      ),
    );
    expect(missing.some((row) => resultIds.has(row.propertyId))).toBe(false);

    const lookup = await runtime.execute("prism_v1_get_property", {
      propertyId: missing[0]!.propertyId,
    });
    expect(lookup.isError).toBe(false);
    expect(resultData(lookup.result).coordinates).toMatchObject({
      availability: "unavailable",
      value: null,
    });
  });

  it("distinguishes zero matches, missing records, and unavailable permit coverage", async () => {
    const { provider, runtime } = await realMcpHarness();
    const center = (await provider.getQueryRows()).find(
      (row) => row.latitude !== null && row.longitude !== null,
    )!;
    const noDirectRoof = coordinatesSearch({
      latitude: center.latitude!,
      longitude: center.longitude!,
    });
    noDirectRoof.filters.roofAge.basis = "direct_only";
    const zero = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      noDirectRoof,
    );
    expect(zero.isError).toBe(false);
    expect(resultData(zero.result).opportunities).toEqual([]);

    const missing = await runtime.execute("prism_v1_get_property", {
      propertyId: "prop_ffffffffffffffffffffffffffffffff",
    });
    expect(missing.isError).toBe(true);
    expect((missing.result.error as Record<string, unknown>).code).toBe(
      "not_found",
    );

    const permit = await runtime.execute("prism_v1_get_permit", {
      permitId: "perm_ffffffffffffffffffffffffffffffff",
    });
    expect(permit.isError).toBe(true);
    expect((permit.result.error as Record<string, unknown>).code).toBe(
      "data_unavailable",
    );
  });

  it("rejects malformed identifiers, cursor tampering, bounds, SQL, paths, and extra fields", async () => {
    const { provider, runtime } = await realMcpHarness();
    for (const argumentsValue of [
      { propertyId: "property_not-valid" },
      {
        propertyId: "prop_00000000000000000000000000000000",
        sql: "select * from internal",
      },
      {
        propertyId: "prop_00000000000000000000000000000000",
        path: "/private/data",
      },
    ]) {
      const result = await runtime.execute(
        "prism_v1_get_property",
        argumentsValue,
      );
      expect(result.isError).toBe(true);
      expect((result.result.error as Record<string, unknown>).code).toBe(
        "invalid_argument",
      );
    }
    const center = (await provider.getQueryRows()).find(
      (row) => row.latitude !== null && row.longitude !== null,
    )!;
    const oversizedRadius = coordinatesSearch({
      latitude: center.latitude!,
      longitude: center.longitude!,
      radiusKilometers: 80.4673,
    });
    const bounds = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      oversizedRadius,
    );
    expect((bounds.result.error as Record<string, unknown>).code).toBe(
      "invalid_argument",
    );
    const invalidCursor = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({
        cursor: "tampered",
        latitude: center.latitude!,
        longitude: center.longitude!,
      }),
    );
    expect((invalidCursor.result.error as Record<string, unknown>).code).toBe(
      "invalid_cursor",
    );
  });

  it("enforces request timeout and response-size bounds", async () => {
    const { contracts, provider } = await realMcpHarness();
    const delayed: OracleMcpProvider = {
      getCanonicalProperty: (...argumentsValue) =>
        provider.getCanonicalProperty(...argumentsValue),
      getMetadata: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        return provider.getMetadata();
      },
      getPermit: (...argumentsValue) => provider.getPermit(...argumentsValue),
      getQueryRows: (...argumentsValue) =>
        provider.getQueryRows(...argumentsValue),
    };
    const timeoutRuntime = new OracleMcpRuntime(delayed, contracts, {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2 * 1024 * 1024,
      requestTimeoutMs: 5,
    });
    const timeout = await timeoutRuntime.execute(
      "prism_v1_get_service_info",
      {},
    );
    expect((timeout.result.error as Record<string, unknown>).code).toBe(
      "deadline_exceeded",
    );

    const boundedRuntime = new OracleMcpRuntime(provider, contracts, {
      maxRequestBytes: 65_536,
      maxResponseBytes: 100,
      requestTimeoutMs: 10_000,
    });
    const oversized = await boundedRuntime.execute(
      "prism_v1_get_service_info",
      {},
    );
    expect(
      (oversized.result.error as Record<string, unknown>).message,
    ).toContain("response-size");
  });
});
