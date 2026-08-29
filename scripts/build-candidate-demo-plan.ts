import "dotenv/config";

import { readFile, realpath, writeFile } from "node:fs/promises";
import path from "node:path";

import postgres from "postgres";

import { loadConfig } from "../services/lib/config.js";
import { recordCandidateDemoPlan } from "../src/db/candidate-demo-publication.js";
import type { PublicationStateView } from "../src/db/publication-durability.js";
import { canonicalJson } from "../src/lib/canonical-json.js";
import { deterministicId } from "../src/lib/hash.js";
import { materializeCandidateDemoArtifacts } from "../src/publication/candidate-demo-artifacts.js";
import { loadCandidateDemoPreflightConfig } from "../src/publication/candidate-demo-config.js";
import { validateCandidateDemoPreflightEvidence } from "../src/publication/candidate-demo-preflight.js";
import {
  createCandidateDemoPlan,
  validateCandidateDemoPlan,
} from "../src/publication/candidate-demo.js";
import { buildPublicationDryRun } from "../src/publication/dry-run.js";
import { validatePublicationPlan } from "../src/publication/plan.js";

const evidenceRelativePath = process.argv[2];
const exactRebuild = process.argv.includes("--rebuild-exact");
if (!evidenceRelativePath) {
  throw new Error(
    "Usage: pnpm candidate-demo:plan -- <DATA_DIR-relative-preflight-evidence>",
  );
}

const runtime = loadConfig();
const config = loadCandidateDemoPreflightConfig();
const dataRoot = await realpath(runtime.dataDir);
const evidencePath = await realpath(
  path.resolve(dataRoot, evidenceRelativePath),
);
if (
  evidencePath !== dataRoot &&
  !evidencePath.startsWith(`${dataRoot}${path.sep}`)
) {
  throw new Error("Candidate demo preflight evidence escapes DATA_DIR");
}
const evidence = validateCandidateDemoPreflightEvidence(
  JSON.parse(await readFile(evidencePath, "utf8")),
);
if (
  !exactRebuild &&
  Date.now() - new Date(evidence.observedAt).getTime() > 30 * 60_000
) {
  throw new Error("Candidate demo plan requires a fresh read-only preflight");
}
if (
  evidence.executorEnabled ||
  evidence.s3Endpoint !== config.s3Endpoint ||
  evidence.identities.length !== 2
) {
  throw new Error("Candidate demo preflight evidence is incompatible");
}

const byDomain = new Map(
  evidence.identities.map((identity) => [identity.domain, identity]),
);
const openDataEvidence = byDomain.get("open_data");
const queryTableEvidence = byDomain.get("query_table");
if (
  !openDataEvidence ||
  !queryTableEvidence ||
  openDataEvidence.ipnsLabel !== config.targets.openData.ipnsLabel ||
  openDataEvidence.ipnsNetworkKey !== config.targets.openData.ipnsNetworkKey ||
  queryTableEvidence.ipnsLabel !== config.targets.queryTable.ipnsLabel ||
  queryTableEvidence.ipnsNetworkKey !== config.targets.queryTable.ipnsNetworkKey
) {
  throw new Error(
    "Candidate demo preflight identities do not match configuration",
  );
}
const verifiedOpenDataEvidence = openDataEvidence;
const verifiedQueryTableEvidence = queryTableEvidence;

const workflowId = "pasco-scale-25000-v1-repeat";
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  workflowId,
]);
const noOfficialPlanRecord = async (
  _databaseUrl: string,
  sourcePlanValue: unknown,
): Promise<PublicationStateView> => {
  const sourcePlan = validatePublicationPlan(sourcePlanValue);
  return {
    approvalId: null,
    approvedAt: null,
    approverReference: null,
    planId: sourcePlan.planId,
    planSha256: sourcePlan.planSha256,
    revision: 0,
    state: "awaiting_configuration",
  };
};

async function buildSource() {
  const summary = await buildPublicationDryRun({
    candidateDemoPilot: true,
    dataDir: runtime.dataDir,
    databaseUrl: runtime.databaseUrl,
    exportMode: "bounded",
    persistDryRun: false,
    publicationPlanRecorder: noOfficialPlanRecord,
    runId,
  });
  const sourcePlan = validatePublicationPlan(
    JSON.parse(
      await readFile(
        path.join(
          runtime.dataDir,
          summary.outputRoot,
          "publication-dry-run-plan.json",
        ),
        "utf8",
      ),
    ),
  );
  if (
    summary.propertyCount !== 25 ||
    summary.queryTableRows !== 25 ||
    summary.queryTableDistinctIds !== 25 ||
    summary.coverageMode !== "sample" ||
    sourcePlan.coverage.mode !== "sample" ||
    sourcePlan.coverage.selection.selectionSize !== 25 ||
    sourcePlan.fixtureExclusion.matches !== 0 ||
    sourcePlan.remoteState.openDataIpnsMutationPerformed ||
    sourcePlan.remoteState.queryTableIpnsMutationPerformed
  ) {
    throw new Error("The exact 25-property source publication is invalid");
  }
  return { sourcePlan, summary };
}

function candidatePlanFor(
  sourcePlan: ReturnType<typeof validatePublicationPlan>,
) {
  return createCandidateDemoPlan({
    limits: config.limits,
    preflightEvidenceSha256: evidence.evidenceSha256,
    preflightObservedAt: evidence.observedAt,
    sourcePlan,
    targets: {
      openData: {
        ...config.targets.openData,
        priorCid: verifiedOpenDataEvidence.priorCid,
      },
      queryTable: {
        ...config.targets.queryTable,
        priorCid: verifiedQueryTableEvidence.priorCid,
      },
    },
  });
}

const first = await buildSource();
const firstCandidate = validateCandidateDemoPlan(
  await candidatePlanFor(first.sourcePlan),
);
if (
  exactRebuild &&
  (firstCandidate.demoPlanId !== "demo_23abd9e168dd3d42a4041f630739a730" ||
    firstCandidate.demoPlanSha256 !==
      "330030f27fc7670f92c3f4bd9853b11147cfdd911aabb3883f68bb5f9ffe83f7")
) {
  throw new Error("Exact candidate rebuild changed the approved plan identity");
}
const candidateRoot = await materializeCandidateDemoArtifacts({
  dataDir: runtime.dataDir,
  plan: firstCandidate,
  sourceOutputRoot: first.summary.outputRoot,
});
const second = await buildSource();
const secondCandidate = validateCandidateDemoPlan(
  await candidatePlanFor(second.sourcePlan),
);
if (
  canonicalJson(first.sourcePlan) !== canonicalJson(second.sourcePlan) ||
  canonicalJson(firstCandidate) !== canonicalJson(secondCandidate)
) {
  throw new Error("Candidate demo deterministic rebuild changed the plan");
}
await materializeCandidateDemoArtifacts({
  dataDir: runtime.dataDir,
  plan: secondCandidate,
  sourceOutputRoot: second.summary.outputRoot,
});
const candidatePlanPath = path.join(
  runtime.dataDir,
  candidateRoot,
  "candidate-demo-plan.json",
);
const candidatePlanBytes = `${canonicalJson(firstCandidate)}\n`;
try {
  await writeFile(candidatePlanPath, candidatePlanBytes, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
} catch (error) {
  if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  if ((await readFile(candidatePlanPath, "utf8")) !== candidatePlanBytes) {
    throw new Error("Existing candidate demo plan bytes are inconsistent", {
      cause: error,
    });
  }
}
const state = await recordCandidateDemoPlan(
  runtime.databaseUrl,
  firstCandidate,
  first.sourcePlan,
);
if (
  state.demoPlanId !== firstCandidate.demoPlanId ||
  state.demoPlanSha256 !== firstCandidate.demoPlanSha256 ||
  ![
    "awaiting_approval",
    "approved",
    "executing",
    "manual_intervention_required",
    "failed_terminal",
    "completed",
  ].includes(state.state)
) {
  throw new Error("Candidate demo durable plan replay is inconsistent");
}
const sql = postgres(runtime.databaseUrl, { max: 1 });
let durableCounts:
  | {
      approvals: number;
      intents: number;
      nonPendingEffects: number;
      pendingEffects: number;
    }
  | undefined;
try {
  const rows = await sql<
    {
      approvals: number;
      intents: number;
      non_pending_effects: number;
      pending_effects: number;
    }[]
  >`
    SELECT
      (SELECT count(*)::int FROM oracle_candidate_demo_approvals
       WHERE demo_plan_id = ${firstCandidate.demoPlanId}) AS approvals,
      (SELECT count(*)::int FROM oracle_candidate_demo_ipns_intents
       WHERE demo_plan_id = ${firstCandidate.demoPlanId}) AS intents,
      (SELECT count(*)::int FROM oracle_candidate_demo_object_effects
       WHERE demo_plan_id = ${firstCandidate.demoPlanId}
         AND status != 'pending') AS non_pending_effects,
      (SELECT count(*)::int FROM oracle_candidate_demo_object_effects
       WHERE demo_plan_id = ${firstCandidate.demoPlanId}
         AND status = 'pending') AS pending_effects
  `;
  const row = rows[0];
  durableCounts = row
    ? {
        approvals: row.approvals,
        intents: row.intents,
        nonPendingEffects: row.non_pending_effects,
        pendingEffects: row.pending_effects,
      }
    : undefined;
} finally {
  await sql.end({ timeout: 5 });
}
const initialStop =
  state.state === "awaiting_approval" &&
  durableCounts?.approvals === 0 &&
  durableCounts.intents === 0 &&
  durableCounts.nonPendingEffects === 0 &&
  durableCounts.pendingEffects === firstCandidate.objectCount;
const exactDurableReplay =
  state.state !== "awaiting_approval" &&
  durableCounts?.approvals === 1 &&
  durableCounts.intents === 2 &&
  durableCounts.nonPendingEffects === firstCandidate.objectCount &&
  durableCounts.pendingEffects === 0;
if (!initialStop && !exactDurableReplay) {
  throw new Error("Candidate demo durable stop state is invalid");
}

console.log(
  JSON.stringify(
    {
      coverageMode: firstCandidate.coverageMode,
      demoPlanId: firstCandidate.demoPlanId,
      demoPlanSha256: firstCandidate.demoPlanSha256,
      deterministicRebuild: true,
      durableCounts,
      estimatedBudgetUsd: firstCandidate.estimatedBudgetUsd,
      estimatedRequestCount: firstCandidate.estimatedRequestCount,
      executorEnabled: config.enabled,
      limits: firstCandidate.limits,
      objectCount: firstCandidate.objectCount,
      outputRoot: candidateRoot,
      priorCids: {
        openData: firstCandidate.targets.openData.priorCid,
        queryTable: firstCandidate.targets.queryTable.priorCid,
      },
      propertyCount: first.summary.propertyCount,
      queryTableDistinctIds: first.summary.queryTableDistinctIds,
      queryTableRows: first.summary.queryTableRows,
      sourcePlanId: firstCandidate.sourcePlanId,
      sourcePlanSha256: firstCandidate.sourcePlanSha256,
      state: state.state,
      targetCids: {
        openData: firstCandidate.targets.openData.targetCid,
        queryTable: firstCandidate.targets.queryTable.targetCid,
      },
      totalBytes: firstCandidate.totalBytes,
    },
    null,
    2,
  ),
);
