import path from "node:path";

import {
  DEFAULT_MAX_REQUEST_BYTES,
  DEFAULT_MAX_RESPONSE_BYTES,
  DEFAULT_MCP_PORT,
  DEFAULT_REQUEST_TIMEOUT_MS,
} from "./constants.js";

export type RuntimeEnvironment = "development" | "test" | "production";

export interface McpLimits {
  maxRequestBytes: number;
  maxResponseBytes: number;
  requestTimeoutMs: number;
}

export interface LocalArtifactProviderConfig {
  dataDir: string;
  environment: "development" | "test";
  manifestPath: string;
  mode: "local-artifact";
  parquetPath: string;
}

export interface PublicIpnsProviderConfig {
  environment: RuntimeEnvironment;
  mode: "public-ipns";
  openDataIpns: string;
  queryTableIpns: string;
}

export type McpProviderConfig =
  LocalArtifactProviderConfig | PublicIpnsProviderConfig;

export interface OracleMcpConfig {
  host: "127.0.0.1";
  limits: McpLimits;
  port: number;
  provider: McpProviderConfig;
}

function requireValue(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function integer(
  environment: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const value = Number(environment[name] ?? fallback);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `${name} must be an integer from ${minimum} through ${maximum}`,
    );
  }
  return value;
}

function runtimeEnvironment(
  environment: NodeJS.ProcessEnv,
): RuntimeEnvironment {
  const value = environment.NODE_ENV?.trim() || "development";
  if (value !== "development" && value !== "test" && value !== "production") {
    throw new Error("NODE_ENV must be development, test, or production");
  }
  return value;
}

function localProvider(
  environment: NodeJS.ProcessEnv,
  nodeEnvironment: RuntimeEnvironment,
): LocalArtifactProviderConfig {
  if (nodeEnvironment === "production") {
    throw new Error("Production rejects the local-artifact MCP provider");
  }
  const dataDir = requireValue(environment, "DATA_DIR");
  if (!path.isAbsolute(dataDir)) {
    throw new Error("DATA_DIR must be an absolute host path");
  }
  return {
    dataDir: path.resolve(dataDir),
    environment: nodeEnvironment,
    manifestPath: requireValue(environment, "MCP_LOCAL_MANIFEST_PATH"),
    mode: "local-artifact",
    parquetPath: requireValue(environment, "MCP_LOCAL_PARQUET_PATH"),
  };
}

function publicProvider(
  environment: NodeJS.ProcessEnv,
  nodeEnvironment: RuntimeEnvironment,
): PublicIpnsProviderConfig {
  return {
    environment: nodeEnvironment,
    mode: "public-ipns",
    openDataIpns: requireValue(environment, "MCP_OPEN_DATA_IPNS"),
    queryTableIpns: requireValue(environment, "MCP_QUERY_TABLE_IPNS"),
  };
}

export function loadMcpConfig(
  environment: NodeJS.ProcessEnv = process.env,
): OracleMcpConfig {
  const nodeEnvironment = runtimeEnvironment(environment);
  const providerMode = requireValue(environment, "ORACLE_MCP_PROVIDER");
  const provider =
    providerMode === "local-artifact"
      ? localProvider(environment, nodeEnvironment)
      : providerMode === "public-ipns"
        ? publicProvider(environment, nodeEnvironment)
        : (() => {
            throw new Error(
              "ORACLE_MCP_PROVIDER must be local-artifact or public-ipns",
            );
          })();

  return {
    host: "127.0.0.1",
    limits: {
      maxRequestBytes: integer(
        environment,
        "MCP_MAX_REQUEST_BYTES",
        DEFAULT_MAX_REQUEST_BYTES,
        1_024,
        1024 * 1024,
      ),
      maxResponseBytes: integer(
        environment,
        "MCP_MAX_RESPONSE_BYTES",
        DEFAULT_MAX_RESPONSE_BYTES,
        16_384,
        8 * 1024 * 1024,
      ),
      requestTimeoutMs: integer(
        environment,
        "MCP_REQUEST_TIMEOUT_MS",
        DEFAULT_REQUEST_TIMEOUT_MS,
        100,
        30_000,
      ),
    },
    port: integer(environment, "MCP_PORT", DEFAULT_MCP_PORT, 1, 65_535),
    provider,
  };
}
