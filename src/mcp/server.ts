import {
  createServer as createNodeServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from "node:http";

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  type CallToolResult,
} from "@modelcontextprotocol/sdk/types.js";

import type { McpLimits } from "./config.js";
import {
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
  MCP_SERVICE_NAME,
  MCP_SERVICE_VERSION,
  MCP_TOOL_DEFINITIONS,
  MCP_TOOL_NAMES,
  type McpToolName,
} from "./constants.js";
import type { McpContractRegistry } from "./contracts.js";
import {
  explorerBootstrap,
  explorerProperty,
  explorerSearch,
  ORACLE_EXPLORER_HTML,
} from "./explorer.js";
import type { OracleMcpRuntime } from "./runtime.js";

function isToolName(value: string): value is McpToolName {
  return (MCP_TOOL_NAMES as readonly string[]).includes(value);
}

export function createProtocolServer(
  runtime: OracleMcpRuntime,
  contracts: McpContractRegistry,
): Server {
  const server = new Server(
    { name: MCP_SERVICE_NAME, version: MCP_SERVICE_VERSION },
    {
      capabilities: { tools: {} },
      instructions:
        "Read-only Pasco public property data. All filters, distance, sorting, and pagination are deterministic.",
    },
  );

  server.setRequestHandler(ListToolsRequestSchema, () => ({
    tools: MCP_TOOL_NAMES.map((name) => ({
      name,
      description: MCP_TOOL_DEFINITIONS[name].description,
      inputSchema: contracts.inputSchema(name) as {
        type: "object";
        properties?: Record<string, object>;
        required?: string[];
      },
      outputSchema: contracts.outputSchema(name) as {
        type: "object";
        properties?: Record<string, object>;
        required?: string[];
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
      execution: { taskSupport: "forbidden" as const },
    })),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
    const { name, arguments: argumentsValue = {} } = request.params;
    if (!isToolName(name)) throw new Error("Unknown MCP tool");
    const execution = await runtime.execute(name, argumentsValue, extra.signal);
    const result: CallToolResult = {
      content: [{ type: "text", text: JSON.stringify(execution.result) }],
      ...(execution.isError
        ? { isError: true }
        : { structuredContent: execution.result }),
    };
    return result;
  });

  return server;
}

async function readBoundedJson(
  request: IncomingMessage,
  maximumBytes: number,
): Promise<unknown> {
  const declared = Number(request.headers["content-length"] ?? 0);
  if (Number.isFinite(declared) && declared > maximumBytes) {
    throw new Error("request_too_large");
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes <= maximumBytes) chunks.push(buffer);
  }
  if (bytes > maximumBytes) throw new Error("request_too_large");
  if (bytes === 0) throw new Error("invalid_json");
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("invalid_json");
  }
}

function jsonResponse(
  response: ServerResponse,
  status: number,
  value: Record<string, unknown>,
): void {
  response.writeHead(status, {
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(value));
}

function explorerJsonResponse(
  response: ServerResponse,
  value: Record<string, unknown>,
  maximumBytes: number,
): void {
  const body = JSON.stringify(value);
  if (Buffer.byteLength(body) > maximumBytes) {
    jsonResponse(response, 507, {
      error: "Explorer response exceeds the limit",
    });
    return;
  }
  response.writeHead(200, {
    "Cache-Control": "public, max-age=60",
    "Content-Security-Policy":
      "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'",
    "Content-Type": "application/json; charset=utf-8",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
  });
  response.end(body);
}

function explorerHtmlResponse(response: ServerResponse): void {
  response.writeHead(200, {
    "Cache-Control": "public, max-age=300",
    "Content-Security-Policy":
      "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'",
    "Content-Type": "text/html; charset=utf-8",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
  });
  response.end(ORACLE_EXPLORER_HTML);
}

async function handleMcpRequest(
  request: IncomingMessage,
  response: ServerResponse,
  runtime: OracleMcpRuntime,
  contracts: McpContractRegistry,
  limits: McpLimits,
): Promise<void> {
  let parsedBody: unknown;
  try {
    parsedBody = await readBoundedJson(request, limits.maxRequestBytes);
  } catch (error) {
    const tooLarge =
      error instanceof Error && error.message === "request_too_large";
    jsonResponse(response, tooLarge ? 413 : 400, {
      error: tooLarge ? "Request body exceeds the limit" : "Invalid JSON body",
    });
    return;
  }

  const server = createProtocolServer(runtime, contracts);
  const transport = new StreamableHTTPServerTransport({
    enableJsonResponse: true,
  });
  try {
    // Omitting sessionIdGenerator selects the SDK's stateless mode. The cast
    // bridges the SDK declaration's exact-optional mismatch under this repo's
    // stricter TypeScript configuration.
    await server.connect(transport as unknown as Transport);
    await transport.handleRequest(request, response, parsedBody);
  } finally {
    await server.close();
  }
}

export function createOracleMcpHttpServer(options: {
  contracts: McpContractRegistry;
  limits: McpLimits;
  providerMode: "local-artifact" | "public-ipns";
  runtime: OracleMcpRuntime;
}): HttpServer {
  return createNodeServer((request, response) => {
    void (async () => {
      if (request.method === "GET" && request.url === "/") {
        explorerHtmlResponse(response);
        return;
      }
      if (request.method === "GET" && request.url === "/health") {
        jsonResponse(response, 200, {
          status: "ok",
          service: MCP_SERVICE_NAME,
          serviceVersion: MCP_SERVICE_VERSION,
          contractVersion: MCP_CONTRACT_VERSION,
          schemaHash: MCP_SCHEMA_SHA256,
          providerMode: options.providerMode,
        });
        return;
      }
      if (
        request.method === "GET" &&
        request.url === "/explorer/api/bootstrap"
      ) {
        try {
          explorerJsonResponse(
            response,
            await explorerBootstrap(options.runtime),
            options.limits.maxResponseBytes,
          );
        } catch {
          jsonResponse(response, 503, {
            error: "Validated explorer metadata is unavailable",
          });
        }
        return;
      }
      if (
        request.method === "POST" &&
        (request.url === "/explorer/api/search" ||
          request.url === "/explorer/api/property")
      ) {
        let body: unknown;
        try {
          body = await readBoundedJson(request, options.limits.maxRequestBytes);
        } catch (error) {
          const tooLarge =
            error instanceof Error && error.message === "request_too_large";
          jsonResponse(response, tooLarge ? 413 : 400, {
            error: tooLarge
              ? "Request body exceeds the limit"
              : "Invalid JSON body",
          });
          return;
        }
        const result =
          request.url === "/explorer/api/search"
            ? await explorerSearch(options.runtime, body)
            : await explorerProperty(options.runtime, body);
        explorerJsonResponse(response, result, options.limits.maxResponseBytes);
        return;
      }
      if (request.method !== "POST" || request.url !== "/mcp") {
        jsonResponse(response, 404, { error: "Not found" });
        return;
      }
      try {
        await handleMcpRequest(
          request,
          response,
          options.runtime,
          options.contracts,
          options.limits,
        );
      } catch {
        if (!response.headersSent) {
          jsonResponse(response, 500, {
            error: "Internal MCP transport error",
          });
        } else if (!response.writableEnded) {
          response.end();
        }
      }
    })();
  });
}

export async function listenOracleMcpServer(
  server: HttpServer,
  port: number,
  host = "127.0.0.1",
): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("MCP HTTP server did not bind a TCP port");
  }
  return address.port;
}
