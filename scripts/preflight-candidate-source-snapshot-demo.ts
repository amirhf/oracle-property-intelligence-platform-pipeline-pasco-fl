import "dotenv/config";

import {
  candidateSourceSnapshotPreflightBinding,
  loadCandidateSourceSnapshotPreflightConfig,
  runCandidateSourceSnapshotReadOnlyPreflight,
  writeCandidateSourceSnapshotPreflightEvidence,
} from "../src/publication/candidate-source-snapshot-preflight.js";

const dataDir = process.env.DATA_DIR?.trim();
if (!dataDir) {
  throw new Error(
    "DATA_DIR is required for sanitized source-snapshot evidence",
  );
}

const config = loadCandidateSourceSnapshotPreflightConfig();
const evidence = await runCandidateSourceSnapshotReadOnlyPreflight({ config });
const binding = candidateSourceSnapshotPreflightBinding(evidence);
const evidencePath = await writeCandidateSourceSnapshotPreflightEvidence({
  dataDir,
  evidence,
});

console.log(
  JSON.stringify(
    {
      bucketChecks: binding.buckets,
      capacityProfile: binding.capacityProfile,
      evidencePath,
      evidenceSha256: binding.evidenceSha256,
      executorEnabled: false,
      identities: binding.identities,
      observedAt: binding.observedAt,
      protectedSampleRollback: binding.protectedSampleRollback,
      readPolicy: evidence.readPolicy,
      requestCount: binding.requestCount,
      status: evidence.status,
    },
    null,
    2,
  ),
);
