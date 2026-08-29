import "dotenv/config";

import { loadCandidateDemoPreflightConfig } from "../src/publication/candidate-demo-config.js";
import {
  runCandidateDemoReadOnlyPreflight,
  writeCandidateDemoPreflightEvidence,
} from "../src/publication/candidate-demo-preflight.js";

const REQUIRED_VARIABLES = [
  "CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED",
  "CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT",
  "CANDIDATE_DEMO_FILEBASE_API_ENDPOINT",
  "CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID",
  "CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY",
  "CANDIDATE_DEMO_FILEBASE_API_TOKEN",
  "CANDIDATE_DEMO_OPEN_DATA_BUCKET",
  "CANDIDATE_DEMO_QUERY_TABLE_BUCKET",
  "CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL",
  "CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL",
  "CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY",
  "CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY",
  "CANDIDATE_DEMO_MAX_OBJECTS",
  "CANDIDATE_DEMO_MAX_TOTAL_BYTES",
  "CANDIDATE_DEMO_MAX_OBJECT_BYTES",
  "CANDIDATE_DEMO_MAX_CONCURRENCY",
  "CANDIDATE_DEMO_MAX_RETRIES",
  "CANDIDATE_DEMO_REQUEST_TIMEOUT_MS",
  "CANDIDATE_DEMO_MAX_REQUESTS",
  "CANDIDATE_DEMO_MAX_BUDGET_USD",
  "CANDIDATE_DEMO_STORAGE_USD_PER_GIB",
  "CANDIDATE_DEMO_REQUEST_USD_PER_1000",
] as const;

const variableStatus = REQUIRED_VARIABLES.map((name) => ({
  name,
  status: process.env[name]?.trim()
    ? ("present" as const)
    : ("missing" as const),
}));
if (variableStatus.some((entry) => entry.status === "missing")) {
  console.log(JSON.stringify({ variableStatus }, null, 2));
  throw new Error("Candidate demo environment is incomplete");
}

const dataDir = process.env.DATA_DIR?.trim();
if (!dataDir) throw new Error("DATA_DIR is required for sanitized evidence");
const config = loadCandidateDemoPreflightConfig();
const evidence = await runCandidateDemoReadOnlyPreflight({ config });
const evidencePath = await writeCandidateDemoPreflightEvidence({
  dataDir,
  evidence,
});

console.log(
  JSON.stringify(
    {
      bootstrapCids: Object.fromEntries(
        evidence.identities.map((identity) => [
          identity.domain,
          identity.priorCid,
        ]),
      ),
      bucketChecks: evidence.buckets.map((bucket) => ({
        domain: bucket.domain,
        exists: bucket.exists,
        storageNetwork: bucket.storageNetwork,
      })),
      evidencePath,
      evidenceSha256: evidence.evidenceSha256,
      executorEnabled: evidence.executorEnabled,
      publicResolution: evidence.identities.map((identity) => ({
        domain: identity.domain,
        matched: identity.publicResolutionMatched,
        resolverCount: identity.publicResolverCount,
      })),
      requestCeiling: evidence.requestCeiling,
      s3EndpointVerified: evidence.s3Endpoint === "https://s3.filebase.com",
      variableStatus,
    },
    null,
    2,
  ),
);
