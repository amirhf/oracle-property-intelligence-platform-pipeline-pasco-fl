import { once } from "node:events";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { MCP_SCHEMA_SHA256, MCP_TOOL_NAMES } from "../../src/mcp/constants.js";
import {
  createOracleMcpHttpServer,
  listenOracleMcpServer,
} from "../../src/mcp/server.js";
import { coordinatesSearch, realMcpHarness } from "../helpers/mcp-real.js";

describe("stateless MCP Streamable HTTP transport", () => {
  let client: Client;
  let mcpUrl: string;
  let server: ReturnType<typeof createOracleMcpHttpServer>;

  beforeAll(async () => {
    const harness = await realMcpHarness();
    server = createOracleMcpHttpServer({
      contracts: harness.contracts,
      limits: {
        maxRequestBytes: 65_536,
        maxResponseBytes: 2 * 1024 * 1024,
        requestTimeoutMs: 10_000,
      },
      providerMode: "local-artifact",
      runtime: harness.runtime,
    });
    const port = await listenOracleMcpServer(server, 0);
    mcpUrl = `http://127.0.0.1:${port}/mcp`;
    client = new Client({ name: "oracle-mcp-test-client", version: "1.0.0" });
    await client.connect(
      new StreamableHTTPClientTransport(
        new URL(mcpUrl),
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

  it("initializes and advertises exactly six frozen tools with strict schemas", async () => {
    expect(client.getServerVersion()).toMatchObject({
      name: "prism-pasco-oracle-mcp",
      version: "0.1.0",
    });
    const listed = await client.listTools();
    expect(listed.tools.map((tool) => tool.name)).toEqual(MCP_TOOL_NAMES);
    for (const tool of listed.tools) {
      expect(tool.inputSchema).toMatchObject({
        type: "object",
        additionalProperties: false,
      });
      expect(tool.outputSchema).toMatchObject({
        type: "object",
        additionalProperties: false,
      });
    }
  });

  it("calls all six tools through the official Streamable HTTP client", async () => {
    const { provider } = await realMcpHarness();
    const center = (await provider.getQueryRows()).find(
      (row) => row.latitude !== null && row.longitude !== null,
    )!;
    const service = await client.callTool({
      name: "prism_v1_get_service_info",
      arguments: {},
    });
    expect(
      (
        (service.structuredContent as Record<string, unknown>).meta as Record<
          string,
          unknown
        >
      ).schemaHash,
    ).toBe(MCP_SCHEMA_SHA256);
    await client.callTool({
      name: "prism_v1_get_pipeline_run_summary",
      arguments: {},
    });
    const search = await client.callTool({
      name: "prism_v1_search_roofing_opportunities",
      arguments: coordinatesSearch({
        latitude: center.latitude!,
        longitude: center.longitude!,
        limit: 2,
      }),
    });
    const opportunities = (
      (search.structuredContent as Record<string, unknown>).data as Record<
        string,
        unknown
      >
    ).opportunities as Array<Record<string, unknown>>;
    const propertyId = (opportunities[0]!.property as Record<string, unknown>)
      .propertyId as string;
    const property = await client.callTool({
      name: "prism_v1_get_property",
      arguments: { propertyId },
    });
    expect(property.isError).not.toBe(true);
    const permit = await client.callTool({
      name: "prism_v1_get_permit",
      arguments: { permitId: "perm_ffffffffffffffffffffffffffffffff" },
    });
    expect(permit.isError).toBe(true);
    const capabilities = await client.callTool({
      name: "prism_v1_get_query_schema",
      arguments: {},
    });
    expect(capabilities.isError).not.toBe(true);
  });

  it("enforces the HTTP request-body bound", async () => {
    const response = await fetch(mcpUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ oversized: "x".repeat(70_000) }),
    });
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({
      error: "Request body exceeds the limit",
    });
  });
});
