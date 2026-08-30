import { randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import { loadMcpConfig } from "../src/mcp/config.js";
import {
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
  MCP_SERVICE_NAME,
  MCP_SERVICE_VERSION,
} from "../src/mcp/constants.js";
import { McpContractRegistry } from "../src/mcp/contracts.js";
import {
  RecoverableHostedInitializer,
  type HostedInitializationContext,
  type HostedInitializationDiagnostic,
} from "../src/mcp/hosted-initializer.js";
import { PublicIpnsProvider } from "../src/mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../src/mcp/runtime.js";
import { createOracleMcpRequestHandler } from "../src/mcp/server.js";

export type HostedRequestHandler = (
  request: IncomingMessage,
  response: ServerResponse,
) => Promise<void>;

type HostedHandlerInitializer = (
  context: HostedInitializationContext,
) => Promise<HostedRequestHandler>;

async function initialize(
  context: HostedInitializationContext,
): Promise<HostedRequestHandler> {
  context.setStage("configuration");
  const config = loadMcpConfig(process.env);
  if (config.provider.mode !== "public-ipns") {
    throw new Error("Hosted Oracle requires the public-ipns provider");
  }
  context.setStage("contracts");
  const contracts = await McpContractRegistry.create();
  const provider = await PublicIpnsProvider.create(
    config.provider,
    contracts,
    undefined,
    undefined,
    context.setStage,
  );
  context.setStage("runtime");
  const runtime = new OracleMcpRuntime(provider, contracts, config.limits);
  return createOracleMcpRequestHandler({
    contracts,
    limits: config.limits,
    providerMode: config.provider.mode,
    runtime,
  });
}

function correlationId(request: IncomingMessage): string {
  const supplied = request.headers["x-vercel-id"];
  if (
    typeof supplied === "string" &&
    /^[A-Za-z0-9:_-]{1,160}$/.test(supplied)
  ) {
    return supplied;
  }
  return `oracle_${randomUUID()}`;
}

function writeJson(
  response: ServerResponse,
  statusCode: number,
  value: unknown,
): void {
  response.writeHead(statusCode, {
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(value));
}

function defaultDiagnosticSink(event: HostedInitializationDiagnostic): void {
  process.stderr.write(`${JSON.stringify(event)}\n`);
}

export function createHostedOracleEntrypoint(
  options: {
    diagnosticSink?: (event: HostedInitializationDiagnostic) => void;
    initialize?: HostedHandlerInitializer;
    now?: () => number;
  } = {},
): HostedRequestHandler {
  const initializer = new RecoverableHostedInitializer({
    diagnosticSink: options.diagnosticSink ?? defaultDiagnosticSink,
    initialize: options.initialize ?? initialize,
    ...(options.now ? { now: options.now } : {}),
  });

  return async (request, response) => {
    const pathname = new URL(request.url ?? "/", "http://localhost").pathname;
    if (request.method === "GET" && pathname === "/health") {
      writeJson(response, 200, {
        status: "ok",
        service: MCP_SERVICE_NAME,
        serviceVersion: MCP_SERVICE_VERSION,
        contractVersion: MCP_CONTRACT_VERSION,
        schemaHash: MCP_SCHEMA_SHA256,
        providerMode: "public-ipns",
        readiness: initializer.readiness(),
      });
      return;
    }

    try {
      const initializedHandler = await initializer.get(correlationId(request));
      await initializedHandler(request, response);
    } catch {
      if (!response.headersSent) {
        writeJson(response, 503, {
          error: "Validated public publication unavailable",
        });
      } else if (!response.writableEnded) {
        response.end();
      }
    }
  };
}

const hostedOracleEntrypoint = createHostedOracleEntrypoint();

export default hostedOracleEntrypoint;
