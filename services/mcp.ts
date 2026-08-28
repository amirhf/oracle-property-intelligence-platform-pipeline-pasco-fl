import "dotenv/config";

import { loadMcpConfig } from "../src/mcp/config.js";
import { McpContractRegistry } from "../src/mcp/contracts.js";
import { createMcpProvider } from "../src/mcp/provider.js";
import { OracleMcpRuntime } from "../src/mcp/runtime.js";
import {
  createOracleMcpHttpServer,
  listenOracleMcpServer,
} from "../src/mcp/server.js";

async function main(): Promise<void> {
  const config = loadMcpConfig();
  const contracts = await McpContractRegistry.create();
  const provider = await createMcpProvider(config.provider, contracts);
  const runtime = new OracleMcpRuntime(provider, contracts, config.limits);
  const server = createOracleMcpHttpServer({
    contracts,
    limits: config.limits,
    providerMode: config.provider.mode,
    runtime,
  });
  const port = await listenOracleMcpServer(server, config.port, config.host);
  console.log(
    JSON.stringify({
      event: "oracle_mcp_started",
      host: config.host,
      port,
      providerMode: config.provider.mode,
    }),
  );

  const close = () => {
    server.close(() => process.exit(0));
  };
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

void main().catch(() => {
  console.error(
    JSON.stringify({
      event: "oracle_mcp_start_failed",
      message: "MCP configuration or publication validation failed",
    }),
  );
  process.exitCode = 1;
});
