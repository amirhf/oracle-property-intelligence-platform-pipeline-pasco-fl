import { once } from "node:events";
import { request as httpRequest } from "node:http";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
  MCP_TOOL_NAMES,
} from "../../src/mcp/constants.js";
import {
  createOracleMcpHttpServer,
  listenOracleMcpServer,
} from "../../src/mcp/server.js";
import { coordinatesSearch, realMcpHarness } from "../helpers/mcp-real.js";

function responsePayload(
  response: Awaited<ReturnType<Client["callTool"]>>,
): Record<string, unknown> {
  if ("structuredContent" in response && response.structuredContent) {
    return response.structuredContent as Record<string, unknown>;
  }
  const content = (
    Array.isArray(response.content) ? response.content : []
  ).find(
    (item): item is { text: string; type: "text" } =>
      item !== null &&
      typeof item === "object" &&
      item.type === "text" &&
      typeof item.text === "string",
  );
  if (!content) {
    throw new Error("MCP response did not contain a JSON payload");
  }
  return JSON.parse(content.text) as Record<string, unknown>;
}

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
    const summary = await client.callTool({
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
    for (const response of [
      service,
      summary,
      search,
      property,
      permit,
      capabilities,
    ]) {
      expect(responsePayload(response).meta).toMatchObject({
        contractVersion: MCP_CONTRACT_VERSION,
        schemaHash: MCP_SCHEMA_SHA256,
      });
    }
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

  it("rejects JSON-RPC batches before they can bypass the final wire bound", async () => {
    const response = await fetch(mcpUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify([
        { id: 1, jsonrpc: "2.0", method: "tools/list", params: {} },
        { id: 2, jsonrpc: "2.0", method: "tools/list", params: {} },
      ]),
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: "JSON-RPC batches are not supported",
    });
  });

  it("starts the request deadline while a request body is still arriving", async () => {
    const harness = await realMcpHarness();
    const bounded = createOracleMcpHttpServer({
      contracts: harness.contracts,
      limits: {
        maxRequestBytes: 65_536,
        maxResponseBytes: 2 * 1024 * 1024,
        requestTimeoutMs: 20,
      },
      providerMode: "local-artifact",
      runtime: harness.runtime,
    });
    const port = await listenOracleMcpServer(bounded, 0);
    try {
      const outcome = await new Promise<{ body: unknown; status: number }>(
        (resolve, reject) => {
          const request = httpRequest(
            {
              headers: {
                "content-length": "100",
                "content-type": "application/json",
              },
              host: "127.0.0.1",
              method: "POST",
              path: "/mcp",
              port,
            },
            (response) => {
              const chunks: Buffer[] = [];
              response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
              response.on("end", () => {
                resolve({
                  body: JSON.parse(Buffer.concat(chunks).toString("utf8")),
                  status: response.statusCode ?? 0,
                });
              });
            },
          );
          request.once("error", reject);
          request.write("{");
          setTimeout(() => request.end(" ".repeat(99)), 100).unref();
        },
      );
      expect(outcome).toEqual({
        body: { error: "Request deadline exceeded" },
        status: 408,
      });
    } finally {
      bounded.close();
      await once(bounded, "close");
    }
  });

  it("bounds the serialized single-call MCP envelope including duplicated content", async () => {
    const harness = await realMcpHarness();
    const row = (await harness.provider.getQueryRows())[0]!;
    const maximumBytes = 4_000;
    const bounded = createOracleMcpHttpServer({
      contracts: harness.contracts,
      limits: {
        maxRequestBytes: 65_536,
        maxResponseBytes: maximumBytes,
        requestTimeoutMs: 10_000,
      },
      providerMode: "local-artifact",
      runtime: harness.runtime,
    });
    const port = await listenOracleMcpServer(bounded, 0);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/mcp`, {
        method: "POST",
        headers: {
          accept: "application/json, text/event-stream",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          id: 1,
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            arguments: { propertyId: row.propertyId },
            name: "prism_v1_get_property",
          },
        }),
      });
      const body = await response.text();
      expect(Buffer.byteLength(body)).toBeLessThanOrEqual(maximumBytes);
      expect(JSON.parse(body).result).toMatchObject({ isError: true });
      expect(body).toContain("response-size");
    } finally {
      bounded.close();
      await once(bounded, "close");
    }
  });

  it("accepts MCP POST requests when rewrite metadata is present", async () => {
    const queryClient = new Client({
      name: "oracle-mcp-query-test-client",
      version: "1.0.0",
    });
    try {
      await queryClient.connect(
        new StreamableHTTPClientTransport(
          new URL(`${mcpUrl}?vercelRewrite=api-index`),
        ) as unknown as Transport,
      );
      const listed = await queryClient.listTools();
      expect(listed.tools.map((tool) => tool.name)).toEqual(MCP_TOOL_NAMES);
    } finally {
      await queryClient.close();
    }
  });
});
