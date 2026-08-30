import "dotenv/config";

import {
  loadCandidateSignedIpnsBinding,
  recordCandidateSignedIpnsObservation,
} from "../src/db/candidate-demo-publication.js";
import { loadCandidateDemoPreflightConfig } from "../src/publication/candidate-demo-config.js";
import { observeCandidateSignedIpnsCheckpoint } from "../src/publication/candidate-demo-preflight.js";
import { loadConfig } from "../services/lib/config.js";

const EXPECTED_PLAN_ID = "demo_23abd9e168dd3d42a4041f630739a730";
const EXPECTED_PLAN_SHA256 =
  "330030f27fc7670f92c3f4bd9853b11147cfdd911aabb3883f68bb5f9ffe83f7";
const EXPECTED_QUERY_NETWORK_KEY =
  "k51qzi5uqu5di2wpp4d4696hjjbpf1ciolkve2duyrrzpsdygh5cywfydtop9n";
const EXPECTED_QUERY_PRIOR =
  "bafybeie5yw5ajrvucfs2qkjkiyz56tb7oevg4coiggojm7v2yvnsbixsem";
const EXPECTED_QUERY_TARGET = "QmSdGz1gZtx4GXxQ41qez6ww6G1Xefy19BPU5vJPEobYUH";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const runtime = loadConfig();
const config = loadCandidateDemoPreflightConfig();
assert(!config.enabled, "Candidate remote executor must remain disabled");
const identity = {
  demoPlanId: EXPECTED_PLAN_ID,
  demoPlanSha256: EXPECTED_PLAN_SHA256,
};
const binding = await loadCandidateSignedIpnsBinding(
  runtime.databaseUrl,
  identity,
);
assert(
  binding.intentState === "update_ambiguous",
  "Query-table intent is not in the expected recovery state",
);
assert(
  binding.networkKey === EXPECTED_QUERY_NETWORK_KEY &&
    config.targets.queryTable.ipnsNetworkKey === EXPECTED_QUERY_NETWORK_KEY,
  "Query-table identity changed",
);
assert(
  binding.priorCid === EXPECTED_QUERY_PRIOR,
  "Query-table immutable prior changed",
);
assert(
  binding.targetCid === EXPECTED_QUERY_TARGET,
  "Query-table approved target changed",
);

const evidence = await observeCandidateSignedIpnsCheckpoint({
  approvalId: binding.approvalId,
  config,
  ...identity,
  expectedPriorCid: binding.priorCid,
  expectedTargetCid: binding.targetCid,
  intentId: binding.intentId,
});
const persisted = await recordCandidateSignedIpnsObservation(
  runtime.databaseUrl,
  evidence,
);

console.log(
  JSON.stringify(
    {
      classification: evidence.classification,
      delegated: {
        httpStatus: evidence.delegated.httpStatus,
        latencyMs: evidence.delegated.latencyMs,
        observedAt: evidence.delegated.observedAt,
        observedCid: evidence.delegated.observedCid,
        requestCount: evidence.delegated.requestCount,
        responseBytes: evidence.delegated.responseBytes,
        responseSha256: evidence.delegated.responseSha256,
        sequence: evidence.delegated.sequence,
        ttlNanoseconds: evidence.delegated.ttlNanoseconds,
        validationResult: evidence.delegated.validationResult,
        validity: evidence.delegated.validity,
      },
      evidenceId: persisted.evidenceId,
      evidenceSha256: persisted.evidenceSha256,
      executorEnabled: false,
      filebaseControl: evidence.filebaseControl,
      filebaseGateway: evidence.filebaseGateway,
      planId: EXPECTED_PLAN_ID,
      planSha256: EXPECTED_PLAN_SHA256,
      requestCount: evidence.requestCount,
    },
    null,
    2,
  ),
);
