import type { IncomingMessage, ServerResponse } from "node:http";

import { loadMcpConfig } from "../src/mcp/config.js";
import { McpContractRegistry } from "../src/mcp/contracts.js";
import { PublicIpnsProvider } from "../src/mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../src/mcp/runtime.js";
import { createOracleMcpRequestHandler } from "../src/mcp/server.js";

type RequestHandler = (
  request: IncomingMessage,
  response: ServerResponse,
) => Promise<void>;

let handlerPromise: Promise<RequestHandler> | undefined;

async function initialize(): Promise<RequestHandler> {
  const config = loadMcpConfig(process.env);
  if (config.provider.mode !== "public-ipns") {
    throw new Error("Hosted Oracle requires the public-ipns provider");
  }
  const contracts = await McpContractRegistry.create();
  const provider = await PublicIpnsProvider.create(config.provider, contracts);
  const runtime = new OracleMcpRuntime(provider, contracts, config.limits);
  return createOracleMcpRequestHandler({
    contracts,
    limits: config.limits,
    providerMode: config.provider.mode,
    runtime,
  });
}

export default async function handler(
  request: IncomingMessage,
  response: ServerResponse,
): Promise<void> {
  handlerPromise ??= initialize();
  try {
    await (
      await handlerPromise
    )(request, response);
  } catch {
    if (!response.headersSent) {
      response.writeHead(503, {
        "Cache-Control": "no-store",
        "Content-Type": "application/json; charset=utf-8",
      });
      response.end(
        JSON.stringify({ error: "Validated public publication unavailable" }),
      );
    } else if (!response.writableEnded) {
      response.end();
    }
  }
}
