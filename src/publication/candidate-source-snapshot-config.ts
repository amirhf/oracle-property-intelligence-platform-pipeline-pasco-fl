import { z } from "zod";

import {
  candidateSourceSnapshotLimitsSchema,
  conservativeCandidateSourceSnapshotPricing,
  type CandidateSourceSnapshotLimits,
  type CandidateSourceSnapshotPricing,
} from "./candidate-source-snapshot-demo.js";
import { CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS } from "./candidate-source-snapshot-preflight-binding.js";

const resourceNameSchema = z
  .string()
  .min(12)
  .max(200)
  .regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/)
  .refine((value) => !/^(elephant|oracle|prism)(-|$)/.test(value));
const networkKeySchema = z.string().regex(/^k51[0-9a-z]{59}$/);

export interface CandidateSourceSnapshotPlanningConfig {
  apiEndpoint: "https://api.filebase.io";
  executorEnabled: false;
  limits: CandidateSourceSnapshotLimits;
  pricing: CandidateSourceSnapshotPricing;
  s3Endpoint: "https://s3.filebase.com";
  targets: {
    openData: {
      bucket: string;
      ipnsLabel: string;
      ipnsNetworkKey: string;
    };
    queryTable: {
      bucket: string;
      ipnsLabel: string;
      ipnsNetworkKey: string;
    };
  };
}

function required(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value)
    throw new Error(`${name} is required for source-snapshot planning`);
  return value;
}

function finiteNumber(environment: NodeJS.ProcessEnv, name: string): number {
  const value = Number(required(environment, name));
  if (!Number.isFinite(value)) throw new Error(`${name} must be finite`);
  return value;
}

export function loadCandidateSourceSnapshotPlanningConfig(
  environment: NodeJS.ProcessEnv,
  fixedAccountPlan: {
    evidence:
      "human_confirmation_required" | "human_confirmed" | "provider_api";
    monthlyUsd: number;
  },
): CandidateSourceSnapshotPlanningConfig {
  if (environment.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim() !== "false") {
    throw new Error(
      "Source-snapshot planning requires CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=false",
    );
  }
  if (
    required(environment, "CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT") !==
    "https://s3.filebase.com"
  ) {
    throw new Error(
      "Source-snapshot planning requires https://s3.filebase.com",
    );
  }
  if (
    required(environment, "CANDIDATE_DEMO_FILEBASE_API_ENDPOINT") !==
    "https://api.filebase.io"
  ) {
    throw new Error(
      "Source-snapshot planning requires https://api.filebase.io",
    );
  }

  const limits = candidateSourceSnapshotLimitsSchema.parse({
    maxBudgetUsd: finiteNumber(environment, "CANDIDATE_DEMO_MAX_BUDGET_USD"),
    maxConcurrency: finiteNumber(environment, "CANDIDATE_DEMO_MAX_CONCURRENCY"),
    maxObjectBytes: finiteNumber(
      environment,
      "CANDIDATE_DEMO_MAX_OBJECT_BYTES",
    ),
    maxObjects: finiteNumber(environment, "CANDIDATE_DEMO_MAX_OBJECTS"),
    maxRequests: finiteNumber(environment, "CANDIDATE_DEMO_MAX_REQUESTS"),
    maxRetries: finiteNumber(environment, "CANDIDATE_DEMO_MAX_RETRIES"),
    maxTotalBytes: finiteNumber(environment, "CANDIDATE_DEMO_MAX_TOTAL_BYTES"),
    requestTimeoutMs: finiteNumber(
      environment,
      "CANDIDATE_DEMO_REQUEST_TIMEOUT_MS",
    ),
  });
  const pricing = conservativeCandidateSourceSnapshotPricing({
    fixedAccountPlanEvidence: fixedAccountPlan.evidence,
    fixedAccountPlanMonthlyUsd: fixedAccountPlan.monthlyUsd,
    requestUsdPerThousand: finiteNumber(
      environment,
      "CANDIDATE_DEMO_REQUEST_USD_PER_1000",
    ),
    storageUsdPerGib: finiteNumber(
      environment,
      "CANDIDATE_DEMO_STORAGE_USD_PER_GIB",
    ),
  });
  const targets = {
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
      ipnsLabel: required(environment, "CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL"),
      ipnsNetworkKey: required(
        environment,
        "CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY",
      ),
    },
  };
  for (const target of [targets.openData, targets.queryTable]) {
    resourceNameSchema.parse(target.bucket);
    resourceNameSchema.parse(target.ipnsLabel);
    networkKeySchema.parse(target.ipnsNetworkKey);
  }
  if (
    targets.openData.bucket === targets.queryTable.bucket ||
    targets.openData.ipnsLabel === targets.queryTable.ipnsLabel ||
    targets.openData.ipnsNetworkKey === targets.queryTable.ipnsNetworkKey
  ) {
    throw new Error("Source-snapshot demo domains require distinct resources");
  }
  if (
    targets.openData.bucket !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket ||
    targets.openData.ipnsLabel !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel ||
    targets.openData.ipnsNetworkKey !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey ||
    targets.queryTable.bucket !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket ||
    targets.queryTable.ipnsLabel !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel ||
    targets.queryTable.ipnsNetworkKey !==
      CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey
  ) {
    throw new Error(
      "Source-snapshot planning requires the reviewed candidate targets",
    );
  }
  return {
    apiEndpoint: "https://api.filebase.io",
    executorEnabled: false,
    limits,
    pricing,
    s3Endpoint: "https://s3.filebase.com",
    targets,
  };
}
