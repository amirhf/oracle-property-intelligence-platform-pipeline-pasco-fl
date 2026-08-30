import { once } from "node:events";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { McpContractRegistry } from "../../src/mcp/contracts.js";
import { MCP_TOOL_NAMES } from "../../src/mcp/constants.js";
import { PublicIpnsProvider } from "../../src/mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../../src/mcp/runtime.js";
import {
  createOracleMcpHttpServer,
  listenOracleMcpServer,
} from "../../src/mcp/server.js";
import { coordinatesSearch } from "../helpers/mcp-real.js";
import {
  SYNTHETIC_EMAIL_SENTINEL,
  SYNTHETIC_MAILING_SENTINEL,
  SYNTHETIC_OWNER_SENTINEL,
  SYNTHETIC_PHONE_SENTINEL,
  syntheticPublicSet,
} from "../helpers/public-ipns.js";

const SENSITIVE_SENTINELS = [
  SYNTHETIC_OWNER_SENTINEL,
  SYNTHETIC_MAILING_SENTINEL,
  SYNTHETIC_PHONE_SENTINEL,
  SYNTHETIC_EMAIL_SENTINEL,
];

describe("privacy-safe public Oracle explorer", () => {
  let baseUrl: string;
  let client: Client;
  let propertyId: string;
  let publicationPropertyCount: number;
  let server: ReturnType<typeof createOracleMcpHttpServer>;

  beforeAll(async () => {
    const contracts = await McpContractRegistry.create();
    const set = await syntheticPublicSet();
    propertyId = set.propertyIds[0]!;
    publicationPropertyCount = set.propertyIds.length;
    const provider = await PublicIpnsProvider.create(
      set.config,
      contracts,
      set.transport,
    );
    const limits = {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2 * 1024 * 1024,
      requestTimeoutMs: 10_000,
    };
    const runtime = new OracleMcpRuntime(provider, contracts, limits);
    server = createOracleMcpHttpServer({
      contracts,
      limits,
      providerMode: "public-ipns",
      runtime,
    });
    const port = await listenOracleMcpServer(server, 0);
    baseUrl = `http://127.0.0.1:${port}`;
    client = new Client({ name: "public-provider-client", version: "1.0.0" });
    await client.connect(
      new StreamableHTTPClientTransport(
        new URL(`${baseUrl}/mcp`),
      ) as unknown as Transport,
    );
  }, 30_000);

  afterAll(async () => {
    await client?.close();
    if (server?.listening) {
      server.close();
      await once(server, "close");
    }
  });

  it("renders a read-only explorer with explicit sample limitations", async () => {
    const page = await fetch(`${baseUrl}/`);
    expect(page.status).toBe(200);
    const html = await page.text();
    expect(html).toContain("Pasco Oracle explorer");
    expect(html).toContain("not complete Pasco County coverage");
    expect(html).not.toContain("publication approval");
    for (const sentinel of SENSITIVE_SENTINELS) {
      expect(html).not.toContain(sentinel);
    }

    const bootstrap = await fetch(
      `${baseUrl}/explorer/api/bootstrap?vercelRewrite=api-index`,
    );
    expect(bootstrap.status).toBe(200);
    const value = (await bootstrap.json()) as Record<string, unknown>;
    expect(value.publication).toMatchObject({
      coverageMode: "sample",
      propertyCount: publicationPropertyCount,
    });
    expect(JSON.stringify(value)).toContain("not complete Pasco coverage");
    expect(JSON.stringify(value)).not.toContain(process.cwd());
  });

  it("executes validated searches while suppressing owner/contact values", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/search?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(
          coordinatesSearch({ latitude: 28.3, longitude: -82.4, limit: 1 }),
        ),
      },
    );
    expect(response.status).toBe(200);
    const value = (await response.json()) as Record<string, unknown>;
    const serialized = JSON.stringify(value);
    for (const sentinel of SENSITIVE_SENTINELS) {
      expect(serialized).not.toContain(sentinel);
    }
    const opportunity = (
      (value.data as Record<string, unknown>).opportunities as Array<
        Record<string, unknown>
      >
    )[0]!;
    const property = opportunity.property as Record<string, unknown>;
    expect(property).not.toHaveProperty("folio");
    expect(property.ownership).toMatchObject({
      currentOwners: { availability: "available", ownerCount: 1 },
      phone: { availability: "unavailable" },
      email: { availability: "unavailable" },
    });
    expect(property.openRoofingPermitCount).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(property.roofAgeSignal).toMatchObject({
      value: { basis: "year_built_proxy", basisQuality: "proxy" },
    });
  });

  it("returns privacy-safe direct property facts and exposes no write/query surface", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/property?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ propertyId }),
      },
    );
    const serialized = await response.text();
    expect(response.status).toBe(200);
    expect(JSON.parse(serialized)).not.toHaveProperty("data.folio");
    for (const sentinel of SENSITIVE_SENTINELS) {
      expect(serialized).not.toContain(sentinel);
    }
    for (const request of [
      fetch(`${baseUrl}/explorer/api/sql`, { method: "POST", body: "{}" }),
      fetch(`${baseUrl}/publish`, { method: "POST", body: "{}" }),
      fetch(`${baseUrl}/explorer/api/property`, { method: "PUT", body: "{}" }),
      fetch(`${baseUrl}/explorer/api/bootstrap/extra?source=explorer`),
    ]) {
      await expect(request).resolves.toMatchObject({ status: 404 });
    }
  });

  it("preserves request-size limits after pathname routing", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/search?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ oversized: "x".repeat(70_000) }),
      },
    );
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({
      error: "Request body exceeds the limit",
    });
  });

  it("serves all six tools through the official Streamable HTTP client", async () => {
    const listed = await client.listTools();
    expect(listed.tools.map((tool) => tool.name)).toEqual(MCP_TOOL_NAMES);
    const search = await client.callTool({
      name: "prism_v1_search_roofing_opportunities",
      arguments: coordinatesSearch({
        latitude: 28.3,
        longitude: -82.4,
        limit: 1,
      }),
    });
    const calls = [
      await client.callTool({
        name: "prism_v1_get_service_info",
        arguments: {},
      }),
      await client.callTool({
        name: "prism_v1_get_pipeline_run_summary",
        arguments: {},
      }),
      search,
      await client.callTool({
        name: "prism_v1_get_property",
        arguments: { propertyId },
      }),
      await client.callTool({
        name: "prism_v1_get_permit",
        arguments: { permitId: `perm_${"f".repeat(32)}` },
      }),
      await client.callTool({
        name: "prism_v1_get_query_schema",
        arguments: {},
      }),
    ];
    expect(calls).toHaveLength(6);
    expect(calls.filter((call) => call.isError === true)).toHaveLength(1);
  });
});
