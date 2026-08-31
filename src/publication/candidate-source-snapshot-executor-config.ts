import { timingSafeEqual } from "node:crypto";

import { z } from "zod";

import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "./candidate-source-snapshot-demo.js";

const ENABLED_FLAG = "CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED";
const S3_ENDPOINT = "https://s3.filebase.com" as const;
const NAMES_ENDPOINT = "https://api.filebase.io" as const;
const approvalIdSchema = z
  .string()
  .regex(/^snapshotdemoapproval_[a-f0-9]{32}$/);

export interface DisabledCandidateSourceSnapshotExecutionConfig {
  enabled: false;
}

export interface EnabledCandidateSourceSnapshotExecutionConfig {
  apiEndpoint: typeof NAMES_ENDPOINT;
  apiToken: string;
  approvalId: string;
  enabled: true;
  limits: CandidateSourceSnapshotDemoPlan["limits"];
  planId: string;
  planSha256: string;
  s3AccessKeyId: string;
  s3Endpoint: typeof S3_ENDPOINT;
  s3SecretAccessKey: string;
  targets: CandidateSourceSnapshotDemoPlan["targets"];
}

export type CandidateSourceSnapshotExecutionConfig =
  | DisabledCandidateSourceSnapshotExecutionConfig
  | EnabledCandidateSourceSnapshotExecutionConfig;

function required(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value) throw new Error(`${name} is required for candidate execution`);
  return value;
}

function exactNumber(
  environment: NodeJS.ProcessEnv,
  name: string,
  expected: number,
): void {
  const value = Number(required(environment, name));
  if (!Number.isFinite(value) || value !== expected) {
    throw new Error(`${name} does not match the immutable plan`);
  }
}

function exact(environment: NodeJS.ProcessEnv, name: string, expected: string) {
  if (required(environment, name) !== expected) {
    throw new Error(`${name} does not match the immutable plan`);
  }
}

function secretsEqual(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left, "utf8");
  const rightBytes = Buffer.from(right, "utf8");
  return (
    leftBytes.byteLength === rightBytes.byteLength &&
    timingSafeEqual(leftBytes, rightBytes)
  );
}

/**
 * Loads the write boundary only when the explicit flag is exactly `true` and
 * every target, limit, plan identity, approval, and credential binding is
 * present. Missing/false is the normal production and local default.
 */
export function loadCandidateSourceSnapshotExecutionConfig(
  environment: NodeJS.ProcessEnv,
  planValue: CandidateSourceSnapshotDemoPlan,
): CandidateSourceSnapshotExecutionConfig {
  const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
  const flag = environment[ENABLED_FLAG]?.trim();
  if (flag === undefined || flag === "" || flag === "false") {
    return { enabled: false };
  }
  if (flag !== "true") {
    throw new Error(`${ENABLED_FLAG} must be exactly true or false`);
  }

  exact(environment, "CANDIDATE_SOURCE_SNAPSHOT_PLAN_ID", plan.planId);
  exact(environment, "CANDIDATE_SOURCE_SNAPSHOT_PLAN_SHA256", plan.planSha256);
  const approvalId = approvalIdSchema.parse(
    required(environment, "CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_ID"),
  );
  exact(environment, "CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT", S3_ENDPOINT);
  exact(environment, "CANDIDATE_DEMO_FILEBASE_API_ENDPOINT", NAMES_ENDPOINT);

  const targetFields = [
    ["CANDIDATE_DEMO_OPEN_DATA_BUCKET", plan.targets.openData.bucket],
    ["CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL", plan.targets.openData.ipnsLabel],
    [
      "CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY",
      plan.targets.openData.ipnsNetworkKey,
    ],
    ["CANDIDATE_DEMO_OPEN_DATA_PRIOR_CID", plan.targets.openData.priorCid],
    ["CANDIDATE_DEMO_OPEN_DATA_TARGET_CID", plan.targets.openData.targetCid],
    ["CANDIDATE_DEMO_QUERY_TABLE_BUCKET", plan.targets.queryTable.bucket],
    [
      "CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL",
      plan.targets.queryTable.ipnsLabel,
    ],
    [
      "CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY",
      plan.targets.queryTable.ipnsNetworkKey,
    ],
    ["CANDIDATE_DEMO_QUERY_TABLE_PRIOR_CID", plan.targets.queryTable.priorCid],
    [
      "CANDIDATE_DEMO_QUERY_TABLE_TARGET_CID",
      plan.targets.queryTable.targetCid,
    ],
  ] as const;
  for (const [name, expected] of targetFields)
    exact(environment, name, expected);

  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_BUDGET_USD",
    plan.limits.maxBudgetUsd,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_CONCURRENCY",
    plan.limits.maxConcurrency,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_OBJECT_BYTES",
    plan.limits.maxObjectBytes,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_OBJECTS",
    plan.limits.maxObjects,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_REQUESTS",
    plan.limits.maxRequests,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_RETRIES",
    plan.limits.maxRetries,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_MAX_TOTAL_BYTES",
    plan.limits.maxTotalBytes,
  );
  exactNumber(
    environment,
    "CANDIDATE_DEMO_REQUEST_TIMEOUT_MS",
    plan.limits.requestTimeoutMs,
  );

  const accessKeyId = required(
    environment,
    "CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID",
  );
  const secretAccessKey = required(
    environment,
    "CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY",
  );
  const apiToken = required(environment, "CANDIDATE_DEMO_FILEBASE_API_TOKEN");
  const derivedToken = Buffer.from(
    `${accessKeyId}:${secretAccessKey}`,
    "utf8",
  ).toString("base64");
  if (!secretsEqual(apiToken, derivedToken)) {
    throw new Error(
      "Candidate Filebase API token is not derived from the configured S3 credentials",
    );
  }
  if (plan.costEnvelope.maximumTotalUsd > plan.limits.maxBudgetUsd) {
    throw new Error("Candidate plan exceeds its immutable spending ceiling");
  }

  return {
    apiEndpoint: NAMES_ENDPOINT,
    apiToken,
    approvalId,
    enabled: true,
    limits: plan.limits,
    planId: plan.planId,
    planSha256: plan.planSha256,
    s3AccessKeyId: accessKeyId,
    s3Endpoint: S3_ENDPOINT,
    s3SecretAccessKey: secretAccessKey,
    targets: plan.targets,
  };
}
