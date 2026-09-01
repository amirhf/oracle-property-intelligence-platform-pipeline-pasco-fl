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
  candidateArtifactGatewayBaseUrl?: string | null;
  candidateDemoPlanId: string | null;
  candidateDemoPlanSha256: string | null;
  candidateDemoSourcePlanSha256: string | null;
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
  resolverPolicy: "candidate_filebase_delegated_v2" | "public_two_gateway_v1";
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
  const resolverPolicy =
    environment.MCP_PUBLIC_RESOLVER_POLICY?.trim() || "public_two_gateway_v1";
  if (
    resolverPolicy !== "public_two_gateway_v1" &&
    resolverPolicy !== "candidate_filebase_delegated_v2"
  ) {
    throw new Error(
      "MCP_PUBLIC_RESOLVER_POLICY must be public_two_gateway_v1 or candidate_filebase_delegated_v2",
    );
  }
  const candidateArtifactGatewayBaseUrl =
    environment.MCP_PUBLIC_CANDIDATE_ARTIFACT_GATEWAY_BASE_URL?.trim() || null;
  if (candidateArtifactGatewayBaseUrl !== null) {
    let parsed: URL;
    try {
      parsed = new URL(candidateArtifactGatewayBaseUrl);
    } catch {
      throw new Error(
        "MCP_PUBLIC_CANDIDATE_ARTIFACT_GATEWAY_BASE_URL is invalid",
      );
    }
    if (
      resolverPolicy !== "candidate_filebase_delegated_v2" ||
      parsed.origin !== "https://foolish-green-asp.myfilebase.com" ||
      parsed.pathname !== "/" ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      parsed.search !== "" ||
      parsed.hash !== ""
    ) {
      throw new Error(
        "MCP_PUBLIC_CANDIDATE_ARTIFACT_GATEWAY_BASE_URL must be the approved candidate HTTPS origin",
      );
    }
  }
  const candidateDemoPlanId =
    environment.MCP_PUBLIC_CANDIDATE_DEMO_PLAN_ID?.trim() || null;
  const candidateDemoPlanSha256 =
    environment.MCP_PUBLIC_CANDIDATE_DEMO_PLAN_SHA256?.trim() || null;
  const candidateDemoSourcePlanSha256 =
    environment.MCP_PUBLIC_CANDIDATE_SOURCE_PLAN_SHA256?.trim() || null;
  const candidateBindings = [
    candidateDemoPlanId,
    candidateDemoPlanSha256,
    candidateDemoSourcePlanSha256,
  ];
  if (
    resolverPolicy === "candidate_filebase_delegated_v2" &&
    candidateBindings.some((value) => value === null)
  ) {
    throw new Error(
      "Candidate delegated public reads require the exact candidate and source plan bindings",
    );
  }
  if (
    resolverPolicy === "public_two_gateway_v1" &&
    candidateBindings.some((value) => value !== null)
  ) {
    throw new Error(
      "Candidate plan bindings require candidate_filebase_delegated_v2",
    );
  }
  return {
    candidateArtifactGatewayBaseUrl:
      candidateArtifactGatewayBaseUrl === null
        ? null
        : "https://foolish-green-asp.myfilebase.com",
    candidateDemoPlanId,
    candidateDemoPlanSha256,
    candidateDemoSourcePlanSha256,
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
        30_000,
      ),
    },
    mode: "public-ipns",
    openDataIpns,
    queryTableIpns,
    resolverPolicy,
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

export function loadMcpRequestTimeoutMs(
  environment: NodeJS.ProcessEnv = process.env,
): number {
  return integer(
    environment,
    "MCP_REQUEST_TIMEOUT_MS",
    DEFAULT_REQUEST_TIMEOUT_MS,
    100,
    30_000,
  );
}
