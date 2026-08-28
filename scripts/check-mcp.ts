import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";

import { MCP_SCHEMA_SHA256, MCP_TOOL_NAMES } from "../src/mcp/constants.js";

function structured(
  value: Awaited<ReturnType<Client["callTool"]>>,
): Record<string, unknown> {
  if (!("structuredContent" in value) || !value.structuredContent) {
    throw new Error("MCP response did not contain structured content");
  }
  return value.structuredContent as Record<string, unknown>;
}

function maskIdentifier(value: string): string {
  return `${value.slice(0, value.indexOf("_") + 1)}…${value.slice(-8)}`;
}

const mcpUrl = process.env.MCP_URL ?? "http://127.0.0.1:9090/mcp";
const client = new Client({
  name: "prism-oracle-verification-client",
  version: "1.0.0",
});
const transport = new StreamableHTTPClientTransport(new URL(mcpUrl));

try {
  await client.connect(transport as unknown as Transport);
  const tools = await client.listTools();
  const names = tools.tools.map((tool) => tool.name);
  if (JSON.stringify(names) !== JSON.stringify(MCP_TOOL_NAMES)) {
    throw new Error(
      "tools/list did not return exactly the frozen six-tool surface",
    );
  }
  const service = structured(
    await client.callTool({ name: MCP_TOOL_NAMES[0], arguments: {} }),
  );
  if (
    (service.meta as Record<string, unknown>).schemaHash !== MCP_SCHEMA_SHA256
  ) {
    throw new Error("Service response did not contain the active schema hash");
  }
  const summary = structured(
    await client.callTool({ name: MCP_TOOL_NAMES[1], arguments: {} }),
  );
  const searchRequest = {
    county: "pasco",
    center: { kind: "place", text: "Dade City, FL" },
    radius: { value: 50, unit: "mi" },
    filters: {
      roofAge: {
        operator: "gte",
        years: 0,
        basis: "direct_or_proxy",
      },
      matchMode: "all",
    },
    sort: "distance_asc",
    page: { limit: 2 },
  };
  const search = structured(
    await client.callTool({
      name: MCP_TOOL_NAMES[2],
      arguments: searchRequest,
    }),
  );
  const opportunities = (search.data as Record<string, unknown>)
    .opportunities as Array<Record<string, unknown>>;
  if (opportunities.length === 0)
    throw new Error("Real-data MCP search was empty");
  const firstProperty = opportunities[0]!.property as Record<string, unknown>;
  const propertyId = firstProperty.propertyId as string;
  const property = structured(
    await client.callTool({
      name: MCP_TOOL_NAMES[3],
      arguments: { propertyId },
    }),
  );
  const permit = await client.callTool({
    name: MCP_TOOL_NAMES[4],
    arguments: { permitId: "perm_ffffffffffffffffffffffffffffffff" },
  });
  const querySchema = structured(
    await client.callTool({ name: MCP_TOOL_NAMES[5], arguments: {} }),
  );
  const cursor = (search.meta as Record<string, unknown>).nextCursor as
    string | null;
  const secondPage = cursor
    ? structured(
        await client.callTool({
          name: MCP_TOOL_NAMES[2],
          arguments: { ...searchRequest, page: { limit: 2, cursor } },
        }),
      )
    : null;
  const directProperty = property.data as Record<string, unknown>;
  const roofSignal = directProperty.roofAgeSignal as Record<string, unknown>;
  const pipeline = summary.data as Record<string, unknown>;
  const coverage = pipeline.coverage as Record<string, Record<string, unknown>>;
  const queryData = querySchema.data as Record<string, unknown>;
  console.log(
    JSON.stringify(
      {
        ok: true,
        url: new URL(mcpUrl).origin + new URL(mcpUrl).pathname,
        tools: names,
        schemaHash: MCP_SCHEMA_SHA256,
        exampleProperty: maskIdentifier(propertyId),
        firstPageCount: opportunities.length,
        secondPageCount: secondPage
          ? (
              (secondPage.data as Record<string, unknown>)
                .opportunities as unknown[]
            ).length
          : 0,
        proxyBasis: (roofSignal.value as Record<string, unknown>).basis,
        coordinateCoverage: coverage.coordinates,
        permitCoverage: coverage.permits,
        permitLookupIsError: permit.isError === true,
        arbitrarySql: (queryData.queryRestrictions as Record<string, unknown>)
          .arbitrarySql,
      },
      null,
      2,
    ),
  );
} finally {
  await client.close();
}
