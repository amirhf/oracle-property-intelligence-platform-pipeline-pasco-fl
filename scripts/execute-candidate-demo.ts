import "dotenv/config";

import {
  approveCandidateDemoPlan,
  beginCandidateDemoExecution,
  checkpointCandidateIpnsPriorConfirmed,
  checkpointCandidateIpnsVerified,
  completeCandidateDemoPlan,
  createCandidateDemoExecutionJournal,
  loadCandidateDemoDurablePlan,
  markCandidateDemoTerminal,
  markCandidateIpnsFailedTerminal,
  recordCandidateIpnsIntents,
  recordCandidateResolutionCycle,
} from "../src/db/candidate-demo-publication.js";
import { canonicalJsonSha256 } from "../src/lib/canonical-json.js";
import { loadCandidateDemoUploadArtifacts } from "../src/publication/candidate-demo-artifacts.js";
import {
  loadCandidateDemoConfig,
  loadCandidateDemoPreflightConfig,
} from "../src/publication/candidate-demo-config.js";
import {
  observeCandidateDemoResolutionCycle,
  runCandidateDemoReadOnlyPreflight,
} from "../src/publication/candidate-demo-preflight.js";
import {
  createCandidateDemoFilebaseExecutor,
  type CandidateDemoRequestMetrics,
} from "../src/publication/filebase-executor.js";
import { validatePublicationPlan } from "../src/publication/plan.js";
import { loadConfig } from "../services/lib/config.js";

const EXPECTED_PLAN_ID = "demo_23abd9e168dd3d42a4041f630739a730";
const EXPECTED_PLAN_SHA256 =
  "330030f27fc7670f92c3f4bd9853b11147cfdd911aabb3883f68bb5f9ffe83f7";
const EXPECTED_PROPERTIES = 25;
const EXPECTED_OBJECTS = 34;
const EXPECTED_BYTES = 178_045;
const APPROVED_COST_CEILING_USD = 0.1181658173277974;
const HARD_BUDGET_USD = 25;

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function totalRequests(metrics: CandidateDemoRequestMetrics): number {
  return (
    metrics.namesApiRequests +
    metrics.publicResolverRequests +
    metrics.s3Requests
  );
}

const identity = {
  demoPlanId: EXPECTED_PLAN_ID,
  demoPlanSha256: EXPECTED_PLAN_SHA256,
};
async function runInitialExecution(): Promise<void> {
  const runtime = loadConfig();
  const enabledConfig = loadCandidateDemoConfig();
  assert(
    enabledConfig.enabled,
    "Candidate demo executor is not explicitly enabled",
  );
  const startedAt = Date.now();
  const durable = await loadCandidateDemoDurablePlan(
    runtime.databaseUrl,
    identity,
  );
  assert(durable.approval !== null, "Exact candidate approval is missing");
  assert(
    durable.state.state === "approved" || durable.state.state === "executing",
    "Candidate plan is not approved for bounded execution",
  );
  await approveCandidateDemoPlan(runtime.databaseUrl, {
    approvedAt: durable.approval.approvedAt,
    approverReference: durable.approval.approverReference,
    ...identity,
  });
  const plan = durable.plan;
  assert(plan.demoPlanId === EXPECTED_PLAN_ID, "Candidate plan ID changed");
  assert(
    plan.demoPlanSha256 === EXPECTED_PLAN_SHA256,
    "Candidate plan SHA-256 changed",
  );
  assert(plan.coverageMode === "sample", "Candidate coverage is not sample");
  assert(
    plan.objectCount === EXPECTED_OBJECTS,
    "Candidate object count changed",
  );
  assert(plan.totalBytes === EXPECTED_BYTES, "Candidate total bytes changed");
  assert(
    plan.estimatedBudgetUsd <= APPROVED_COST_CEILING_USD,
    "Candidate estimated cost exceeds the human-approved bound",
  );
  assert(
    plan.limits.maxBudgetUsd === HARD_BUDGET_USD,
    "Candidate hard spending ceiling changed",
  );
  assert(
    enabledConfig.targets.openData.bucket === plan.targets.openData.bucket &&
      enabledConfig.targets.openData.ipnsLabel ===
        plan.targets.openData.ipnsLabel &&
      enabledConfig.targets.openData.ipnsNetworkKey ===
        plan.targets.openData.ipnsNetworkKey &&
      enabledConfig.targets.queryTable.bucket ===
        plan.targets.queryTable.bucket &&
      enabledConfig.targets.queryTable.ipnsLabel ===
        plan.targets.queryTable.ipnsLabel &&
      enabledConfig.targets.queryTable.ipnsNetworkKey ===
        plan.targets.queryTable.ipnsNetworkKey,
    "Enabled executor targets do not match the durable plan",
  );

  const artifacts = await loadCandidateDemoUploadArtifacts({
    dataDir: runtime.dataDir,
    plan,
  });
  const sourcePlanArtifact = artifacts.find(
    (artifact) => artifact.objectKey === "publication-dry-run-plan.json",
  );
  assert(sourcePlanArtifact, "Candidate source plan artifact is missing");
  const sourcePlan = validatePublicationPlan(
    JSON.parse(sourcePlanArtifact.bytes.toString("utf8")),
  );
  assert(
    sourcePlan.counts.canonicalDocuments === EXPECTED_PROPERTIES &&
      sourcePlan.counts.queryTableRows === EXPECTED_PROPERTIES &&
      sourcePlan.counts.queryTableDistinctPropertyIds === EXPECTED_PROPERTIES,
    "Candidate property count changed",
  );

  const preflightConfig = loadCandidateDemoPreflightConfig({
    ...process.env,
    CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "false",
  });
  const preflight = await runCandidateDemoReadOnlyPreflight({
    config: preflightConfig,
  });
  const observed = new Map(
    preflight.identities.map((entry) => [entry.domain, entry]),
  );
  assert(
    observed.get("open_data")?.priorCid === plan.targets.openData.priorCid &&
      observed.get("query_table")?.priorCid ===
        plan.targets.queryTable.priorCid,
    "Current public IPNS state differs from the immutable plan priors",
  );

  const metrics: CandidateDemoRequestMetrics = {
    namesApiRequests: 0,
    publicResolverRequests: 0,
    s3Requests: 0,
  };
  const executor = createCandidateDemoFilebaseExecutor(
    enabledConfig,
    plan,
    createCandidateDemoExecutionJournal(runtime.databaseUrl),
    metrics,
  );

  function assertRequestAndCostLimits(): number {
    const requests = totalRequests(metrics);
    assert(
      requests <= plan.estimatedRequestCount,
      "Actual request count exceeded the plan",
    );
    assert(
      requests <= plan.limits.maxRequests,
      "Actual request count exceeded the hard limit",
    );
    const cost =
      (plan.totalBytes / 1024 ** 3) * plan.limits.storageUsdPerGib +
      (requests / 1_000) * plan.limits.requestUsdPerThousand;
    assert(
      cost <= APPROVED_COST_CEILING_USD,
      "Actual estimated cost exceeded approval",
    );
    assert(
      cost <= plan.limits.maxBudgetUsd,
      "Actual estimated cost exceeded hard budget",
    );
    return cost;
  }

  async function currentTargets() {
    return await runCandidateDemoReadOnlyPreflight({ config: preflightConfig });
  }

  try {
    await beginCandidateDemoExecution(runtime.databaseUrl, identity);
    const uploadResults = await Promise.allSettled(
      artifacts.map(async (artifact) => await executor.upload(artifact)),
    );
    const uploadFailure = uploadResults.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (uploadFailure) throw uploadFailure.reason;
    assertRequestAndCostLimits();

    await recordCandidateIpnsIntents(runtime.databaseUrl, {
      ...identity,
      evidenceSha256: {
        openData: canonicalJsonSha256({
          domain: "open_data",
          evidenceSha256: preflight.evidenceSha256,
          priorCid: plan.targets.openData.priorCid,
        }),
        queryTable: canonicalJsonSha256({
          domain: "query_table",
          evidenceSha256: preflight.evidenceSha256,
          priorCid: plan.targets.queryTable.priorCid,
        }),
      },
      intendedAt: new Date().toISOString(),
      priorCid: {
        openData: plan.targets.openData.priorCid,
        queryTable: plan.targets.queryTable.priorCid,
      },
    });

    try {
      await executor.updateIpns("open_data");
    } catch (error) {
      let recovery;
      try {
        recovery = await currentTargets();
      } catch {
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "ambiguous_remote_state",
        );
        throw error;
      }
      const current = new Map(
        recovery.identities.map((entry) => [entry.domain, entry.priorCid]),
      );
      if (
        current.get("open_data") === plan.targets.openData.targetCid &&
        current.get("query_table") === plan.targets.queryTable.priorCid
      ) {
        await checkpointCandidateIpnsVerified(runtime.databaseUrl, {
          ...identity,
          domain: "open_data",
        });
      } else if (
        current.get("open_data") === plan.targets.openData.priorCid &&
        current.get("query_table") === plan.targets.queryTable.priorCid
      ) {
        await checkpointCandidateIpnsPriorConfirmed(runtime.databaseUrl, {
          ...identity,
          domain: "open_data",
        });
        throw error;
      } else {
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "ambiguous_remote_state",
        );
        throw error;
      }
    }

    try {
      await executor.updateIpns("query_table");
    } catch (error) {
      let recovery;
      try {
        recovery = await currentTargets();
      } catch {
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "ambiguous_remote_state",
        );
        throw error;
      }
      const current = new Map(
        recovery.identities.map((entry) => [entry.domain, entry.priorCid]),
      );
      if (
        current.get("open_data") === plan.targets.openData.targetCid &&
        current.get("query_table") === plan.targets.queryTable.targetCid
      ) {
        await checkpointCandidateIpnsVerified(runtime.databaseUrl, {
          ...identity,
          domain: "query_table",
        });
      } else if (
        current.get("open_data") === plan.targets.openData.targetCid &&
        current.get("query_table") === plan.targets.queryTable.priorCid
      ) {
        await markCandidateIpnsFailedTerminal(runtime.databaseUrl, {
          ...identity,
          domain: "query_table",
        });
        await executor.rollbackIpns("open_data");
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "second_domain_rolled_back",
        );
        throw error;
      } else {
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "ambiguous_remote_state",
        );
        throw error;
      }
    }
    const actualEstimatedCostUsd = assertRequestAndCostLimits();
    const finalState = await completeCandidateDemoPlan(
      runtime.databaseUrl,
      identity,
    );
    console.log(
      JSON.stringify(
        {
          actualEstimatedCostUsd,
          bytes: plan.totalBytes,
          durationMs: Date.now() - startedAt,
          finalState,
          metrics,
          planId: plan.demoPlanId,
          planSha256: plan.demoPlanSha256,
          priorCids: {
            openData: plan.targets.openData.priorCid,
            queryTable: plan.targets.queryTable.priorCid,
          },
          providerCidVerification: {
            matched: uploadResults.filter(
              (result) => result.status === "fulfilled",
            ).length,
            mismatched: 0,
          },
          targetCids: {
            openData: plan.targets.openData.targetCid,
            queryTable: plan.targets.queryTable.targetCid,
          },
        },
        null,
        2,
      ),
    );
  } finally {
    process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "false";
  }
}

async function runBoundedRecovery(): Promise<void> {
  const runtime = loadConfig();
  process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "false";
  const disabledConfig = loadCandidateDemoPreflightConfig(process.env);
  const durable = await loadCandidateDemoDurablePlan(
    runtime.databaseUrl,
    identity,
  );
  assert(durable.approval !== null, "Exact candidate approval is missing");
  assert(
    durable.approval.approvalId ===
      "demoapproval_636e3632a540f88fa7efd91bf0312394",
    "Candidate approval identity changed",
  );
  const plan = durable.plan;
  assert(plan.demoPlanId === EXPECTED_PLAN_ID, "Candidate plan ID changed");
  assert(
    plan.demoPlanSha256 === EXPECTED_PLAN_SHA256,
    "Candidate plan SHA-256 changed",
  );
  assert(plan.coverageMode === "sample", "Candidate coverage is not sample");
  assert(
    plan.objectCount === EXPECTED_OBJECTS,
    "Candidate object count changed",
  );
  assert(plan.totalBytes === EXPECTED_BYTES, "Candidate total bytes changed");
  assert(
    plan.estimatedBudgetUsd <= APPROVED_COST_CEILING_USD &&
      plan.limits.maxBudgetUsd === HARD_BUDGET_USD,
    "Candidate budget binding changed",
  );
  const metrics: CandidateDemoRequestMetrics = {
    namesApiRequests: 0,
    publicResolverRequests: 0,
    s3Requests: 0,
  };
  const startedAt = Date.now();
  const observations: Array<{
    classification: string;
    cycleId: string;
    domain: "open_data" | "query_table";
    evidenceSha256: string;
    observed: Array<{
      cid: string | null;
      httpStatus: number | null;
      observedAt: string;
      outcome: string;
      resolver: string;
    }>;
  }> = [];

  const observe = async (domain: "open_data" | "query_table") => {
    const values = await observeCandidateDemoResolutionCycle({
      config: disabledConfig,
      domain,
    });
    metrics.namesApiRequests += 1;
    metrics.publicResolverRequests += 2;
    const persisted = await recordCandidateResolutionCycle(
      runtime.databaseUrl,
      {
        ...identity,
        domain,
        observations: values,
      },
    );
    observations.push({
      classification: persisted.classification,
      cycleId: persisted.cycleId,
      domain,
      evidenceSha256: persisted.evidenceSha256,
      observed: values.map((entry) => ({
        cid: entry.observedCid,
        httpStatus: entry.httpStatus,
        observedAt: entry.observedAt,
        outcome: entry.outcome,
        resolver: entry.resolver,
      })),
    });
    return persisted;
  };

  const mutate = async (domain: "open_data" | "query_table") => {
    process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "true";
    try {
      const enabled = loadCandidateDemoConfig();
      assert(enabled.enabled, "Candidate executor did not enable explicitly");
      const executor = createCandidateDemoFilebaseExecutor(
        enabled,
        plan,
        createCandidateDemoExecutionJournal(runtime.databaseUrl),
        metrics,
      );
      return await executor.updateIpnsControlPlane(domain);
    } finally {
      process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "false";
    }
  };

  try {
    let open = await observe("open_data");
    if (open.classification === "prior_observed") {
      await mutate("open_data");
      open = await observe("open_data");
      if (open.classification !== "target_observed") {
        await markCandidateDemoTerminal(
          runtime.databaseUrl,
          identity,
          "ambiguous_remote_state",
        );
      }
    }
    if (open.classification !== "target_observed") {
      console.log(
        JSON.stringify(
          { finalState: "manual_intervention_required", metrics, observations },
          null,
          2,
        ),
      );
      return;
    }
    await checkpointCandidateIpnsVerified(runtime.databaseUrl, {
      ...identity,
      domain: "open_data",
    });

    const queryPrior = await observe("query_table");
    if (queryPrior.classification !== "prior_observed") {
      await markCandidateDemoTerminal(
        runtime.databaseUrl,
        identity,
        "ambiguous_remote_state",
      );
      console.log(
        JSON.stringify(
          { finalState: "manual_intervention_required", metrics, observations },
          null,
          2,
        ),
      );
      return;
    }
    await mutate("query_table");
    const queryTarget = await observe("query_table");
    if (queryTarget.classification !== "target_observed") {
      await markCandidateDemoTerminal(
        runtime.databaseUrl,
        identity,
        "ambiguous_remote_state",
      );
      console.log(
        JSON.stringify(
          { finalState: "manual_intervention_required", metrics, observations },
          null,
          2,
        ),
      );
      return;
    }
    await checkpointCandidateIpnsVerified(runtime.databaseUrl, {
      ...identity,
      domain: "query_table",
    });
    const finalState = await completeCandidateDemoPlan(
      runtime.databaseUrl,
      identity,
    );
    const requestCount = totalRequests(metrics);
    const actualEstimatedCostUsd =
      (plan.totalBytes / 1024 ** 3) * plan.limits.storageUsdPerGib +
      (requestCount / 1_000) * plan.limits.requestUsdPerThousand;
    assert(
      requestCount <= plan.limits.maxRequests,
      "Recovery request limit exceeded",
    );
    assert(
      actualEstimatedCostUsd <= APPROVED_COST_CEILING_USD &&
        actualEstimatedCostUsd <= plan.limits.maxBudgetUsd,
      "Recovery cost limit exceeded",
    );
    console.log(
      JSON.stringify(
        {
          actualEstimatedCostUsd,
          bytesUploaded: 0,
          durationMs: Date.now() - startedAt,
          finalState,
          metrics,
          observations,
          planId: plan.demoPlanId,
          planSha256: plan.demoPlanSha256,
        },
        null,
        2,
      ),
    );
  } finally {
    process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED = "false";
  }
}

if (process.argv.includes("--recover")) {
  await runBoundedRecovery();
} else {
  await runInitialExecution();
}
