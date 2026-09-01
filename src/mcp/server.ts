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
  limits: McpLimits,
  requestSignal?: AbortSignal,
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
    const signal = requestSignal
      ? AbortSignal.any([extra.signal, requestSignal])
      : extra.signal;
    let execution = await runtime.execute(name, argumentsValue, signal);
    const toCallToolResult = (value: typeof execution): CallToolResult => ({
      content: [{ type: "text", text: JSON.stringify(value.result) }],
      ...(value.isError
        ? { isError: true }
        : { structuredContent: value.result }),
    });
    let result = toCallToolResult(execution);
    const serializedBytes = () =>
      Buffer.byteLength(
        JSON.stringify({ id: extra.requestId, jsonrpc: "2.0", result }),
      );
    if (serializedBytes() > limits.maxResponseBytes) {
      execution = runtime.responseSizeFailure(name, argumentsValue);
      result = toCallToolResult(execution);
    }
    if (serializedBytes() > limits.maxResponseBytes) {
      throw new Error("Bounded MCP error envelope exceeds response limit");
    }
    return result;
  });

  return server;
}

class RequestBodyError extends Error {
  constructor(
    readonly code: "invalid_json" | "request_deadline" | "request_too_large",
  ) {
    super(code);
  }
}

async function readBoundedJson(
  request: IncomingMessage,
  maximumBytes: number,
  signal?: AbortSignal,
): Promise<unknown> {
  const declared = Number(request.headers["content-length"] ?? 0);
  if (Number.isFinite(declared) && declared > maximumBytes) {
    throw new RequestBodyError("request_too_large");
  }
  if (signal?.aborted) throw new RequestBodyError("request_deadline");
  const body = await new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    let settled = false;
    const cleanup = () => {
      request.off("data", onData);
      request.off("end", onEnd);
      request.off("error", onError);
      request.off("aborted", onAborted);
      signal?.removeEventListener("abort", onDeadline);
    };
    const finish = (error?: RequestBodyError) => {
      if (settled) return;
      settled = true;
      cleanup();
      request.pause();
      if (error) reject(error);
      else resolve(Buffer.concat(chunks));
    };
    const onData = (chunk: Buffer | string) => {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      bytes += buffer.byteLength;
      if (bytes > maximumBytes) {
        finish(new RequestBodyError("request_too_large"));
        return;
      }
      chunks.push(buffer);
    };
    const onEnd = () => finish();
    const onError = () => finish(new RequestBodyError("invalid_json"));
    const onAborted = () => finish(new RequestBodyError("invalid_json"));
    const onDeadline = () => finish(new RequestBodyError("request_deadline"));
    request.on("data", onData);
    request.once("end", onEnd);
    request.once("error", onError);
    request.once("aborted", onAborted);
    signal?.addEventListener("abort", onDeadline, { once: true });
  });
  if (body.byteLength === 0) throw new RequestBodyError("invalid_json");
  try {
    return JSON.parse(body.toString("utf8"));
  } catch {
    throw new RequestBodyError("invalid_json");
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
  signal: AbortSignal,
): Promise<void> {
  let parsedBody: unknown;
  try {
    parsedBody = await readBoundedJson(request, limits.maxRequestBytes, signal);
  } catch (error) {
    const code =
      error instanceof RequestBodyError ? error.code : "invalid_json";
    jsonResponse(
      response,
      code === "request_too_large"
        ? 413
        : code === "request_deadline"
          ? 408
          : 400,
      {
        error:
          code === "request_too_large"
            ? "Request body exceeds the limit"
            : code === "request_deadline"
              ? "Request deadline exceeded"
              : "Invalid JSON body",
      },
    );
    return;
  }
  if (Array.isArray(parsedBody)) {
    jsonResponse(response, 400, {
      error: "JSON-RPC batches are not supported",
    });
    return;
  }

  const server = createProtocolServer(runtime, contracts, limits, signal);
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
  return createNodeServer(createOracleMcpRequestHandler(options));
}

export function createOracleMcpRequestHandler(options: {
  contracts: McpContractRegistry;
  limits: McpLimits;
  providerMode: "local-artifact" | "public-ipns";
  runtime: OracleMcpRuntime;
}): (
  request: IncomingMessage,
  response: ServerResponse,
  externalSignal?: AbortSignal,
) => Promise<void> {
  return async (request, response, externalSignal) => {
    const pathname = new URL(request.url ?? "/", "http://localhost").pathname;

    if (request.method === "GET" && pathname === "/") {
      explorerHtmlResponse(response);
      return;
    }
    if (request.method === "GET" && pathname === "/health") {
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
    const dataRoute =
      (request.method === "GET" && pathname === "/explorer/api/bootstrap") ||
      (request.method === "POST" &&
        (pathname === "/explorer/api/search" ||
          pathname === "/explorer/api/property" ||
          pathname === "/mcp"));
    if (!dataRoute) {
      jsonResponse(response, 404, { error: "Not found" });
      return;
    }
    const deadline = new AbortController();
    const timer = setTimeout(
      () =>
        deadline.abort(
          new DOMException("Request deadline exceeded", "TimeoutError"),
        ),
      options.limits.requestTimeoutMs,
    );
    timer.unref();
    const requestSignal = externalSignal
      ? AbortSignal.any([externalSignal, deadline.signal])
      : deadline.signal;
    try {
      if (request.method === "GET") {
        try {
          explorerJsonResponse(
            response,
            await explorerBootstrap(options.runtime, requestSignal),
            options.limits.maxResponseBytes,
          );
        } catch {
          jsonResponse(response, 503, {
            error: "Validated explorer metadata is unavailable",
          });
        }
        return;
      }
      if (pathname !== "/mcp") {
        let body: unknown;
        try {
          body = await readBoundedJson(
            request,
            options.limits.maxRequestBytes,
            requestSignal,
          );
        } catch (error) {
          const code =
            error instanceof RequestBodyError ? error.code : "invalid_json";
          jsonResponse(
            response,
            code === "request_too_large"
              ? 413
              : code === "request_deadline"
                ? 408
                : 400,
            {
              error:
                code === "request_too_large"
                  ? "Request body exceeds the limit"
                  : code === "request_deadline"
                    ? "Request deadline exceeded"
                    : "Invalid JSON body",
            },
          );
          return;
        }
        const result =
          pathname === "/explorer/api/search"
            ? await explorerSearch(options.runtime, body, requestSignal)
            : await explorerProperty(options.runtime, body, requestSignal);
        explorerJsonResponse(response, result, options.limits.maxResponseBytes);
        return;
      }
      await handleMcpRequest(
        request,
        response,
        options.runtime,
        options.contracts,
        options.limits,
        requestSignal,
      );
    } catch {
      if (!response.headersSent) {
        jsonResponse(response, 500, {
          error: "Internal MCP transport error",
        });
      } else if (!response.writableEnded) {
        response.end();
      }
    } finally {
      clearTimeout(timer);
    }
  };
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
