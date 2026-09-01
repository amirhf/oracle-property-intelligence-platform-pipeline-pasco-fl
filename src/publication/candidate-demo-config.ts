import { z } from "zod";

import {
  candidateDemoLimitsSchema,
  type CandidateDemoLimits,
  type CandidateDemoTargetIdentityInput,
} from "./candidate-demo.js";

const SUPPORTED_S3_ENDPOINTS = new Set([
  "https://s3.filebase.com",
  "https://s3.filebase.io",
]);
const SUPPORTED_API_ENDPOINTS = new Set(["https://api.filebase.io"]);

interface DisabledCandidateDemoConfig {
  enabled: false;
}

interface CandidateDemoCredentialedConfig {
  apiEndpoint: string;
  apiToken: string;
  limits: CandidateDemoLimits;
  s3AccessKeyId: string;
  s3Endpoint: string;
  s3SecretAccessKey: string;
  targets: CandidateDemoTargetIdentityInput;
}

export interface EnabledCandidateDemoConfig extends CandidateDemoCredentialedConfig {
  enabled: true;
}

export interface CandidateDemoPreflightConfig extends CandidateDemoCredentialedConfig {
  enabled: false;
}

export type CandidateDemoConfig =
  DisabledCandidateDemoConfig | EnabledCandidateDemoConfig;

function required(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value)
    throw new Error(`${name} is required for candidate demo execution`);
  return value;
}

function numberValue(environment: NodeJS.ProcessEnv, name: string): number {
  const text = required(environment, name);
  const value = Number(text);
  if (!Number.isFinite(value)) throw new Error(`${name} must be numeric`);
  return value;
}

function endpoint(
  environment: NodeJS.ProcessEnv,
  name: string,
  supported: Set<string>,
): string {
  const raw = required(environment, name);
  let normalized: string;
  try {
    const parsed = new URL(raw);
    if (
      parsed.protocol !== "https:" ||
      parsed.username ||
      parsed.password ||
      parsed.search ||
      parsed.hash ||
      (parsed.pathname !== "/" && parsed.pathname !== "")
    ) {
      throw new Error("unsupported endpoint shape");
    }
    normalized = parsed.origin;
  } catch {
    throw new Error(`${name} must be a supported HTTPS Filebase origin`);
  }
  if (!supported.has(normalized)) {
    throw new Error(`${name} must be a supported Filebase endpoint`);
  }
  return normalized;
}

function credentialedConfig(
  environment: NodeJS.ProcessEnv,
): CandidateDemoCredentialedConfig {
  const limits = candidateDemoLimitsSchema.parse({
    maxBudgetUsd: numberValue(environment, "CANDIDATE_DEMO_MAX_BUDGET_USD"),
    maxConcurrency: numberValue(environment, "CANDIDATE_DEMO_MAX_CONCURRENCY"),
    maxObjectBytes: numberValue(environment, "CANDIDATE_DEMO_MAX_OBJECT_BYTES"),
    maxObjects: numberValue(environment, "CANDIDATE_DEMO_MAX_OBJECTS"),
    maxRequests: numberValue(environment, "CANDIDATE_DEMO_MAX_REQUESTS"),
    maxRetries: numberValue(environment, "CANDIDATE_DEMO_MAX_RETRIES"),
    maxTotalBytes: numberValue(environment, "CANDIDATE_DEMO_MAX_TOTAL_BYTES"),
    requestTimeoutMs: numberValue(
      environment,
      "CANDIDATE_DEMO_REQUEST_TIMEOUT_MS",
    ),
    requestUsdPerThousand: numberValue(
      environment,
      "CANDIDATE_DEMO_REQUEST_USD_PER_1000",
    ),
    storageUsdPerGib: numberValue(
      environment,
      "CANDIDATE_DEMO_STORAGE_USD_PER_GIB",
    ),
  });
  const config: CandidateDemoCredentialedConfig = {
    apiEndpoint: endpoint(
      environment,
      "CANDIDATE_DEMO_FILEBASE_API_ENDPOINT",
      SUPPORTED_API_ENDPOINTS,
    ),
    apiToken: required(environment, "CANDIDATE_DEMO_FILEBASE_API_TOKEN"),
    limits,
    s3AccessKeyId: required(
      environment,
      "CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID",
    ),
    s3Endpoint: endpoint(
      environment,
      "CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT",
      SUPPORTED_S3_ENDPOINTS,
    ),
    s3SecretAccessKey: required(
      environment,
      "CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY",
    ),
    targets: {
      openData: {
        bucket: required(environment, "CANDIDATE_DEMO_OPEN_DATA_BUCKET"),
        ipnsLabel: required(environment, "CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL"),
        ipnsNetworkKey: required(
          environment,
          "CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY",
        ),
      },
      queryTable: {
        bucket: required(environment, "CANDIDATE_DEMO_QUERY_TABLE_BUCKET"),
        ipnsLabel: required(
          environment,
          "CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL",
        ),
        ipnsNetworkKey: required(
          environment,
          "CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY",
        ),
      },
    },
  };
  z.string().min(12).parse(config.apiToken);
  const expectedApiToken = Buffer.from(
    `${config.s3AccessKeyId}:${config.s3SecretAccessKey}`,
  ).toString("base64");
  if (config.apiToken !== expectedApiToken) {
    throw new Error(
      "CANDIDATE_DEMO_FILEBASE_API_TOKEN must be derived from the configured S3 credentials",
    );
  }
  const candidateResourceName = z
    .string()
    .min(12)
    .max(200)
    .regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/)
    .refine((value) => !/^(elephant|oracle|prism)(-|$)/.test(value));
  const networkKey = z.string().regex(/^k51[0-9a-z]{59}$/);
  for (const target of [config.targets.openData, config.targets.queryTable]) {
    candidateResourceName.parse(target.bucket);
    candidateResourceName.parse(target.ipnsLabel);
    networkKey.parse(target.ipnsNetworkKey);
  }
  if (
    config.targets.openData.bucket === config.targets.queryTable.bucket ||
    config.targets.openData.ipnsLabel === config.targets.queryTable.ipnsLabel ||
    config.targets.openData.ipnsNetworkKey ===
      config.targets.queryTable.ipnsNetworkKey
  ) {
    throw new Error("Candidate demo domains require distinct resources");
  }
  const prefixFor = (
    openDataName: string,
    queryTableName: string,
  ): string | null => {
    const openSuffix = "-open-data-demo";
    const querySuffix = "-query-table-demo";
    if (
      !openDataName.endsWith(openSuffix) ||
      !queryTableName.endsWith(querySuffix)
    ) {
      return null;
    }
    const openPrefix = openDataName.slice(0, -openSuffix.length);
    const queryPrefix = queryTableName.slice(0, -querySuffix.length);
    return openPrefix.length >= 3 && openPrefix === queryPrefix
      ? openPrefix
      : null;
  };
  if (
    prefixFor(
      config.targets.openData.bucket,
      config.targets.queryTable.bucket,
    ) === null ||
    prefixFor(
      config.targets.openData.ipnsLabel,
      config.targets.queryTable.ipnsLabel,
    ) === null
  ) {
    throw new Error(
      "Candidate demo targets require one candidate-owned prefix and explicit domain roles",
    );
  }
  return config;
}

export function loadCandidateDemoPreflightConfig(
  environment: NodeJS.ProcessEnv = process.env,
): CandidateDemoPreflightConfig {
  if (environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim() !== "false") {
    throw new Error(
      "Candidate demo read-only preflight requires CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=false",
    );
  }
  const config = credentialedConfig(environment);
  if (config.s3Endpoint !== "https://s3.filebase.com") {
    throw new Error(
      "Candidate demo read-only preflight requires https://s3.filebase.com",
    );
  }
  return { ...config, enabled: false };
}

export function loadCandidateDemoConfig(
  environment: NodeJS.ProcessEnv = process.env,
): CandidateDemoConfig {
  const enabled = environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim();
  if (enabled !== "true") {
    if (enabled && enabled !== "false") {
      throw new Error(
        "CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED must be true or false",
      );
    }
    return { enabled: false };
  }
  const config: EnabledCandidateDemoConfig = {
    ...credentialedConfig(environment),
    enabled: true,
  };
  return config;
}
