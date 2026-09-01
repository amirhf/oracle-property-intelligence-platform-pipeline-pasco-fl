import { beforeAll, describe, expect, it } from "vitest";

import { McpContractRegistry } from "../../src/mcp/contracts.js";
import {
  HttpPublicReadTransport,
  PublicIpnsProvider,
  PublicReadError,
} from "../../src/mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../../src/mcp/runtime.js";
import { MCP_SCHEMA_SHA256, MCP_TOOL_NAMES } from "../../src/mcp/constants.js";
import {
  MockPublicReadTransport,
  OPEN_IPNS,
  QUERY_IPNS,
  syntheticPublicSet,
} from "../helpers/public-ipns.js";
import { coordinatesSearch } from "../helpers/mcp-real.js";

function errorCode(error: unknown): string | undefined {
  return error instanceof PublicReadError ? error.code : undefined;
}

describe("public IPNS Oracle provider", () => {
  let contracts: McpContractRegistry;

  beforeAll(async () => {
    contracts = await McpContractRegistry.create();
  });

  it("validates two IPNS resolutions, the complete graph, Parquet, and a property leaf", async () => {
    const set = await syntheticPublicSet();
    const provider = await PublicIpnsProvider.create(
      set.config,
      contracts,
      set.transport,
    );
    const metadata = await provider.getMetadata();
    expect(metadata).toMatchObject({
      canonicalDocumentCount: 2,
      coordinateCount: 1,
      coverageMode: "sample",
      providerMode: "public-ipns",
      permitCoverage: "unavailable",
      contractorCoverage: "unavailable",
    });
    expect(metadata.publication).toMatchObject({
      openDataIpns: OPEN_IPNS,
      queryTableIpns: QUERY_IPNS,
      openDataRootCid: set.config.expectedOpenDataRootCid,
      queryTableRootCid: set.config.expectedQueryTableRootCid,
    });
    expect(await provider.getQueryRows()).toHaveLength(2);
    const property = await provider.getCanonicalProperty(set.propertyIds[0]!);
    expect(property?.propertyId).toBe(
      set.propertyIds[0]!.replace("prop_", "property_"),
    );
    expect(set.transport.reads).toContain(set.config.expectedOpenDataRootCid);
    expect(set.transport.reads).toContain(set.config.expectedQueryTableRootCid);
  }, 30_000);

  it("binds a target-null source plan to one explicit candidate authorization", async () => {
    const set = await syntheticPublicSet({ candidatePlanBindings: true });
    const provider = await PublicIpnsProvider.create(
      set.config,
      contracts,
      set.transport,
    );
    const metadata = await provider.getMetadata();
    expect(metadata.publication).toMatchObject({
      candidateDemoPlanId: `demo_${"d".repeat(32)}`,
      candidateDemoPlanSha256: "2".repeat(64),
      resolverPolicy: "candidate_filebase_delegated_v2",
    });

    await expect(
      PublicIpnsProvider.create(
        {
          ...set.config,
          candidateDemoSourcePlanSha256: "3".repeat(64),
        },
        contracts,
        set.transport,
      ),
    ).rejects.toMatchObject({ code: "contract_mismatch" });
  }, 30_000);

  it.each([
    ["missing", () => [], "ipns_missing"],
    [
      "stale",
      (cid: string) => [
        {
          cacheAgeSeconds: 301,
          cid,
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "a",
          status: "resolved" as const,
        },
        {
          cacheAgeSeconds: 0,
          cid,
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "b",
          status: "resolved" as const,
        },
      ],
      "ipns_stale",
    ],
    [
      "split",
      (cid: string) => [
        {
          cacheAgeSeconds: 0,
          cid,
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "a",
          status: "resolved" as const,
        },
        {
          cacheAgeSeconds: 0,
          cid: "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "b",
          status: "resolved" as const,
        },
      ],
      "ipns_split",
    ],
    [
      "unexpected",
      () => [
        {
          cacheAgeSeconds: 0,
          cid: "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "a",
          status: "resolved" as const,
        },
        {
          cacheAgeSeconds: 0,
          cid: "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
          observedAt: "2026-08-29T00:00:00.000Z",
          resolver: "b",
          status: "resolved" as const,
        },
      ],
      "ipns_unexpected",
    ],
  ] as const)(
    "rejects %s resolution",
    async (_name, observations, code) => {
      const set = await syntheticPublicSet();
      set.transport.resolutions.set(
        OPEN_IPNS,
        observations(set.config.expectedOpenDataRootCid),
      );
      await expect(
        PublicIpnsProvider.create(set.config, contracts, set.transport),
      ).rejects.toSatisfy((error: unknown) => errorCode(error) === code);
    },
    30_000,
  );

  it("rejects hash, schema, contract, property-CID, fixture, and incomplete configuration failures", async () => {
    const hashSet = await syntheticPublicSet();
    hashSet.objects.set(
      hashSet.config.expectedPlanCid,
      Buffer.from("tampered plan"),
    );
    await expect(
      PublicIpnsProvider.create(hashSet.config, contracts, hashSet.transport),
    ).rejects.toMatchObject({ code: "hash_mismatch" });

    const schemaSet = await syntheticPublicSet({
      omitParquetColumn: "property_cid",
    });
    await expect(
      PublicIpnsProvider.create(
        schemaSet.config,
        contracts,
        schemaSet.transport,
      ),
    ).rejects.toMatchObject({ code: "schema_mismatch" });

    const contractSet = await syntheticPublicSet({ mcpHash: "0".repeat(64) });
    await expect(
      PublicIpnsProvider.create(
        contractSet.config,
        contracts,
        contractSet.transport,
      ),
    ).rejects.toMatchObject({ code: "contract_mismatch" });

    const cidSet = await syntheticPublicSet({ wrongPropertyCid: true });
    await expect(
      PublicIpnsProvider.create(cidSet.config, contracts, cidSet.transport),
    ).rejects.toMatchObject({ code: "artifact_invalid" });

    const fixtureSet = await syntheticPublicSet({ fixtureProperty: true });
    await expect(
      PublicIpnsProvider.create(
        fixtureSet.config,
        contracts,
        fixtureSet.transport,
      ),
    ).rejects.toMatchObject({ code: "fixture_rejected" });

    await expect(
      PublicIpnsProvider.create(
        { ...fixtureSet.config, expectedPlanCid: "missing" },
        contracts,
        fixtureSet.transport,
      ),
    ).rejects.toMatchObject({ code: "configuration_invalid" });
  }, 60_000);

  it("serves all six frozen tools with proxy and unavailable coverage semantics", async () => {
    const set = await syntheticPublicSet();
    const provider = await PublicIpnsProvider.create(
      set.config,
      contracts,
      set.transport,
    );
    const runtime = new OracleMcpRuntime(provider, contracts, {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2 * 1024 * 1024,
      requestTimeoutMs: 10_000,
    });
    const search = await runtime.execute(
      "prism_v1_search_roofing_opportunities",
      coordinatesSearch({ latitude: 28.3, longitude: -82.4, limit: 1 }),
    );
    const opportunity = (
      (search.result.data as Record<string, unknown>).opportunities as Array<
        Record<string, unknown>
      >
    )[0]!;
    const property = opportunity.property as Record<string, unknown>;
    expect(property.roofAgeSignal).toMatchObject({
      availability: "available",
      value: { basis: "year_built_proxy", basisQuality: "proxy" },
    });
    expect(property.openRoofingPermitCount).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    const results = [
      await runtime.execute("prism_v1_get_service_info", {}),
      await runtime.execute("prism_v1_get_pipeline_run_summary", {}),
      search,
      await runtime.execute("prism_v1_get_property", {
        propertyId: set.propertyIds[0],
      }),
      await runtime.execute("prism_v1_get_permit", {
        permitId: `perm_${"f".repeat(32)}`,
      }),
      await runtime.execute("prism_v1_get_query_schema", {}),
    ];
    expect(results).toHaveLength(MCP_TOOL_NAMES.length);
    for (const result of results) {
      expect(result.result.meta).toMatchObject({
        schemaHash: MCP_SCHEMA_SHA256,
      });
      expect(
        result.isError
          ? contracts.validateError(result.result)
          : contracts.validateOutput(
              MCP_TOOL_NAMES[results.indexOf(result)]!,
              result.result,
            ),
      ).toEqual([]);
    }
  }, 30_000);

  it("bounds redirects, response bytes, retries, and timeouts without accepting URLs", async () => {
    const baseLimits = {
      maxCacheAgeSeconds: 300,
      maxJsonObjectBytes: 1024,
      maxParquetBytes: 1024,
      maxRedirects: 0,
      retries: 0,
      transportTimeoutMs: 100,
    };
    const redirect = new HttpPublicReadTransport(
      baseLimits,
      async () =>
        new Response(null, {
          status: 302,
          headers: { location: "https://example.invalid/ipfs/forbidden" },
        }),
    );
    await expect(
      redirect.readCid("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH", 10),
    ).rejects.toMatchObject({ code: "redirect_rejected" });

    const oversized = new HttpPublicReadTransport(
      baseLimits,
      async () =>
        new Response("0123456789", {
          status: 200,
          headers: { "content-length": "10" },
        }),
    );
    await expect(
      oversized.readCid("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH", 5),
    ).rejects.toMatchObject({ code: "artifact_too_large" });

    const timedOut = new HttpPublicReadTransport(baseLimits, async () => {
      throw new DOMException("timed out", "TimeoutError");
    });
    await expect(
      timedOut.readCid("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH", 5),
    ).rejects.toMatchObject({ code: "timeout" });

    let exhaustedTimeoutAttempts = 0;
    const exhaustedTimeout = new HttpPublicReadTransport(
      { ...baseLimits, retries: 1 },
      async () => {
        exhaustedTimeoutAttempts += 1;
        throw new DOMException("timed out", "TimeoutError");
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      exhaustedTimeout.readCid(
        "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        5,
      ),
    ).rejects.toMatchObject({ code: "timeout", retryable: true });
    expect(exhaustedTimeoutAttempts).toBe(4);

    let timeoutThenSuccessAttempts = 0;
    const timeoutThenSuccess = new HttpPublicReadTransport(
      { ...baseLimits, retries: 1 },
      async () => {
        timeoutThenSuccessAttempts += 1;
        if (timeoutThenSuccessAttempts === 1) {
          throw new DOMException("timed out", "TimeoutError");
        }
        return new Response("ok", { status: 200 });
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      timeoutThenSuccess.readCid(
        "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        5,
      ),
    ).resolves.toEqual(new TextEncoder().encode("ok"));
    expect(timeoutThenSuccessAttempts).toBe(2);

    let bodyTimeoutAttempts = 0;
    const bodyTimeoutThenSuccess = new HttpPublicReadTransport(
      { ...baseLimits, retries: 1 },
      async () => {
        bodyTimeoutAttempts += 1;
        if (bodyTimeoutAttempts === 1) {
          return new Response(
            new ReadableStream({
              pull(controller) {
                controller.error(new DOMException("timed out", "TimeoutError"));
              },
            }),
            { status: 200 },
          );
        }
        return new Response("ok", { status: 200 });
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      bodyTimeoutThenSuccess.readCid(
        "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        5,
      ),
    ).resolves.toEqual(new TextEncoder().encode("ok"));
    expect(bodyTimeoutAttempts).toBe(2);

    let unavailableThenSuccessAttempts = 0;
    const unavailableThenSuccess = new HttpPublicReadTransport(
      { ...baseLimits, retries: 1 },
      async () => {
        unavailableThenSuccessAttempts += 1;
        return unavailableThenSuccessAttempts === 1
          ? new Response(null, { status: 503 })
          : new Response("ok", { status: 200 });
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      unavailableThenSuccess.readCid(
        "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        5,
      ),
    ).resolves.toEqual(new TextEncoder().encode("ok"));
    expect(unavailableThenSuccessAttempts).toBe(2);

    let attempts = 0;
    const retried = new HttpPublicReadTransport(
      { ...baseLimits, retries: 1 },
      async () => {
        attempts += 1;
        return new Response(null, { status: 503 });
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      retried.readCid("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH", 5),
    ).rejects.toMatchObject({ code: "transport_unavailable" });
    expect(attempts).toBe(4);

    let permanentAttempts = 0;
    const permanent = new HttpPublicReadTransport(
      { ...baseLimits, retries: 2 },
      async () => {
        permanentAttempts += 1;
        return new Response(null, { status: 403 });
      },
      { retryDelay: async () => undefined },
    );
    await expect(
      permanent.readCid("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH", 5),
    ).rejects.toMatchObject({
      code: "transport_unavailable",
      retryable: false,
    });
    expect(permanentAttempts).toBe(1);
  });

  it("keeps equivalent synthetic evidence isolated per transport instance", async () => {
    const set = await syntheticPublicSet();
    const isolated = new MockPublicReadTransport(
      new Map(set.objects),
      new Map([
        [OPEN_IPNS, set.config.expectedOpenDataRootCid],
        [QUERY_IPNS, set.config.expectedQueryTableRootCid],
      ]),
    );
    await expect(
      PublicIpnsProvider.create(set.config, contracts, isolated),
    ).resolves.toBeInstanceOf(PublicIpnsProvider);
  }, 30_000);
});
