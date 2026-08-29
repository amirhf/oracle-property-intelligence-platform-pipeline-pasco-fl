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
  expectedManifestCid: string;
  expectedManifestSha256: string;
  expectedOpenDataRootCid: string;
  expectedPlanCid: string;
  expectedPlanSha256: string;
  expectedQueryTableRootCid: string;
  limits: {
    maxCacheAgeSeconds: number;
    maxJsonObjectBytes: number;
    maxParquetBytes: number;
    maxRedirects: number;
    retries: number;
    transportTimeoutMs: number;
  };
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
  const openDataIpns = requireValue(environment, "MCP_OPEN_DATA_IPNS");
  const queryTableIpns = requireValue(environment, "MCP_QUERY_TABLE_IPNS");
  return {
    environment: nodeEnvironment,
    expectedManifestCid: requireValue(environment, "MCP_PUBLIC_MANIFEST_CID"),
    expectedManifestSha256: requireValue(
      environment,
      "MCP_PUBLIC_MANIFEST_SHA256",
    ),
    expectedOpenDataRootCid: requireValue(
      environment,
      "MCP_PUBLIC_OPEN_DATA_ROOT_CID",
    ),
    expectedPlanCid: requireValue(environment, "MCP_PUBLIC_PLAN_CID"),
    expectedPlanSha256: requireValue(environment, "MCP_PUBLIC_PLAN_SHA256"),
    expectedQueryTableRootCid: requireValue(
      environment,
      "MCP_PUBLIC_QUERY_TABLE_ROOT_CID",
    ),
    limits: {
      maxCacheAgeSeconds: integer(
        environment,
        "MCP_PUBLIC_MAX_CACHE_AGE_SECONDS",
        300,
        0,
        86_400,
      ),
      maxJsonObjectBytes: integer(
        environment,
        "MCP_PUBLIC_MAX_JSON_OBJECT_BYTES",
        8 * 1024 * 1024,
        16_384,
        32 * 1024 * 1024,
      ),
      maxParquetBytes: integer(
        environment,
        "MCP_PUBLIC_MAX_PARQUET_BYTES",
        128 * 1024 * 1024,
        1024 * 1024,
        512 * 1024 * 1024,
      ),
      maxRedirects: integer(environment, "MCP_PUBLIC_MAX_REDIRECTS", 2, 0, 4),
      retries: integer(environment, "MCP_PUBLIC_RETRIES", 1, 0, 2),
      transportTimeoutMs: integer(
        environment,
        "MCP_PUBLIC_TRANSPORT_TIMEOUT_MS",
        5_000,
        100,
        15_000,
      ),
    },
    mode: "public-ipns",
    openDataIpns,
    queryTableIpns,
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
