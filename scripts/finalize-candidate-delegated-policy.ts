import "dotenv/config";

import {
  authorizeCandidateDelegatedResolverPolicy,
  CANDIDATE_FILEBASE_DELEGATED_POLICY,
  completeCandidateDemoWithDelegatedPolicy,
  loadCandidateDelegatedResolverPolicyAuthorization,
} from "../src/db/candidate-demo-publication.js";
import { loadConfig } from "../services/lib/config.js";

const binding = {
  approvalId: "demoapproval_636e3632a540f88fa7efd91bf0312394",
  demoPlanId: "demo_23abd9e168dd3d42a4041f630739a730",
  demoPlanSha256:
    "330030f27fc7670f92c3f4bd9853b11147cfdd911aabb3883f68bb5f9ffe83f7",
  policyId: CANDIDATE_FILEBASE_DELEGATED_POLICY,
  queryIntentId: "demointent_42db49a682396de9f41b66c41c952ffa",
  queryNetworkKey:
    "k51qzi5uqu5di2wpp4d4696hjjbpf1ciolkve2duyrrzpsdygh5cywfydtop9n",
  queryPriorCid: "bafybeie5yw5ajrvucfs2qkjkiyz56tb7oevg4coiggojm7v2yvnsbixsem",
  queryTargetCid: "QmSdGz1gZtx4GXxQ41qez6ww6G1Xefy19BPU5vJPEobYUH",
  signedEvidenceId: "demosignedobservation_475daa5b4427fa353a3917ba11f9fba1",
  signedEvidenceSha256:
    "5d1ec67e062f244e64e508a72fc7a273aa36b2eb1b6b940cfac27e7e5bb0e234",
} as const;

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

assert(
  process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim() === "false",
  "Candidate remote executor must remain disabled",
);

const runtime = loadConfig();
let authorization = await loadCandidateDelegatedResolverPolicyAuthorization(
  runtime.databaseUrl,
  {
    demoPlanId: binding.demoPlanId,
    demoPlanSha256: binding.demoPlanSha256,
  },
);
if (!authorization) {
  authorization = await authorizeCandidateDelegatedResolverPolicy(
    runtime.databaseUrl,
    {
      ...binding,
      authorizedAt: new Date().toISOString(),
      authorizerReference: "candidate-controller",
    },
  ).then(async () =>
    loadCandidateDelegatedResolverPolicyAuthorization(runtime.databaseUrl, {
      demoPlanId: binding.demoPlanId,
      demoPlanSha256: binding.demoPlanSha256,
    }),
  );
}
assert(authorization, "Candidate delegated authorization was not persisted");
for (const [key, value] of Object.entries(binding)) {
  assert(
    authorization[key as keyof typeof authorization] === value,
    `Candidate delegated authorization changed ${key}`,
  );
}

const completion = await completeCandidateDemoWithDelegatedPolicy(
  runtime.databaseUrl,
  {
    authorizationId: authorization.authorizationId,
    authorizationSha256: authorization.authorizationSha256,
    demoPlanId: binding.demoPlanId,
    demoPlanSha256: binding.demoPlanSha256,
  },
);

console.log(
  JSON.stringify({
    authorizationId: authorization.authorizationId,
    authorizationSha256: authorization.authorizationSha256,
    completionId: completion.completionId,
    completionSha256: completion.completionSha256,
    executorEnabled: false,
    planId: binding.demoPlanId,
    planSha256: binding.demoPlanSha256,
    policyId: binding.policyId,
    remoteMutationPerformed: completion.remoteMutationPerformed,
    state: completion.state.state,
  }),
);
