import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approveCandidateDemoPlan,
  assertCandidateIpnsMutationReady,
  authorizeCandidateDelegatedResolverPolicy,
  authorizeCandidateResolverPolicy,
  beginCandidateDemoExecution,
  CANDIDATE_FILEBASE_DELEGATED_POLICY,
  CANDIDATE_FILEBASE_DWEB_POLICY,
  checkpointCandidateIpnsVerified,
  checkpointCandidateObjectVerified,
  completeCandidateDemoWithDelegatedPolicy,
  markCandidateIpnsUpdateInFlight,
  recordCandidateDemoPlan,
  recordCandidateIpnsIntents,
  recordCandidateResolutionCycle,
  recordCandidateSignedIpnsObservation,
} from "../../src/db/candidate-demo-publication.js";
import { runMigrations } from "../../src/db/migrations.js";
import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import { createCandidateDemoPlan } from "../../src/publication/candidate-demo.js";
import { syntheticSamplePublicationPlan } from "../helpers/candidate-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_demo_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const priorOpen = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
const priorQuery = "QmYwAPJzv5CZsnAzt8auVZRnGi9VQUg9nHfS3aB2NFv7fC";

function targetObservations(targetCid: string) {
  return ["filebase_control", "filebase_gateway", "ipfs_io", "dweb_link"].map(
    (resolver, index) => ({
      cacheAgeSeconds: index === 0 ? null : 0,
      httpStatus: 200,
      observedAt: `2026-08-30T00:00:0${index}.000Z`,
      observedCid: targetCid,
      ordinal: index + 1,
      outcome: "resolved",
      resolver,
      resolverType: index === 0 ? "control_plane" : "public_resolver",
      responseBytes: index === 0 ? 128 : 0,
      responseSha256: `${index + 1}`.repeat(64),
    }),
  );
}

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(databaseUrl)).toHaveLength(28);
  expect(await runMigrations(databaseUrl)).toEqual([]);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
  }
});

describe("candidate demo publication durability", () => {
  it("keeps sample demo approval/effects/intents separate and orders mutation admission", async () => {
    const sourcePlan = await syntheticSamplePublicationPlan();
    const demoPlan = await createCandidateDemoPlan({
      limits: {
        maxBudgetUsd: 10,
        maxConcurrency: 2,
        maxObjectBytes: 2 * 1024 * 1024,
        maxObjects: 100,
        maxRequests: 1_000,
        maxRetries: 1,
        maxTotalBytes: 10 * 1024 * 1024,
        requestTimeoutMs: 2_000,
        requestUsdPerThousand: 0.01,
        storageUsdPerGib: 0.1,
      },
      preflightEvidenceSha256: "d".repeat(64),
      preflightObservedAt: "2026-08-30T00:00:00.000Z",
      sourcePlan,
      targets: {
        openData: {
          bucket: "candidate-prism-open-data-demo",
          ipnsLabel: "candidate-prism-open-data-demo",
          ipnsNetworkKey: `k51${"2".repeat(59)}`,
          priorCid: priorOpen,
        },
        queryTable: {
          bucket: "candidate-prism-query-table-demo",
          ipnsLabel: "candidate-prism-query-table-demo",
          ipnsNetworkKey: `k51${"3".repeat(59)}`,
          priorCid: priorQuery,
        },
      },
    });
    const recorded = await recordCandidateDemoPlan(
      databaseUrl,
      demoPlan,
      sourcePlan,
    );
    expect(recorded).toMatchObject({
      approvalId: null,
      state: "awaiting_approval",
    });
    await expect(
      beginCandidateDemoExecution(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      }),
    ).rejects.toThrow("requires exact approval");
    const approval = await approveCandidateDemoPlan(databaseUrl, {
      approvedAt: "2026-08-30T00:00:00.000Z",
      approverReference: "synthetic-controller",
      demoPlanId: demoPlan.demoPlanId,
      demoPlanSha256: demoPlan.demoPlanSha256,
    });
    expect(approval.state).toBe("approved");
    const resolverAuthorization = await authorizeCandidateResolverPolicy(
      databaseUrl,
      {
        approvalId: approval.approvalId!,
        authorizedAt: "2026-08-30T00:00:00.500Z",
        authorizerReference: "synthetic-controller",
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        policyId: CANDIDATE_FILEBASE_DWEB_POLICY,
      },
    );
    expect(resolverAuthorization).toMatchObject({
      policyId: CANDIDATE_FILEBASE_DWEB_POLICY,
    });
    await expect(
      authorizeCandidateResolverPolicy(databaseUrl, {
        approvalId: approval.approvalId!,
        authorizedAt: "2026-08-30T00:00:00.501Z",
        authorizerReference: "synthetic-controller",
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        policyId: CANDIDATE_FILEBASE_DWEB_POLICY,
      }),
    ).rejects.toThrow("replay conflict");
    expect(
      await approveCandidateDemoPlan(databaseUrl, {
        approvedAt: "2026-08-30T00:00:00.000Z",
        approverReference: "synthetic-controller",
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      }),
    ).toEqual(approval);
    expect(
      (
        await beginCandidateDemoExecution(databaseUrl, {
          demoPlanId: demoPlan.demoPlanId,
          demoPlanSha256: demoPlan.demoPlanSha256,
        })
      ).state,
    ).toBe("executing");
    await expect(
      assertCandidateIpnsMutationReady(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      }),
    ).rejects.toThrow("durable intents in domain order");
    for (const artifact of demoPlan.objects) {
      await checkpointCandidateObjectVerified(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
        providerCid: artifact.expectedCid,
        receiptSha256: "a".repeat(64),
      });
    }
    await recordCandidateIpnsIntents(databaseUrl, {
      demoPlanId: demoPlan.demoPlanId,
      demoPlanSha256: demoPlan.demoPlanSha256,
      evidenceSha256: {
        openData: "b".repeat(64),
        queryTable: "c".repeat(64),
      },
      intendedAt: "2026-08-30T00:00:01.000Z",
      priorCid: { openData: priorOpen, queryTable: priorQuery },
    });
    await expect(
      assertCandidateIpnsMutationReady(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      }),
    ).resolves.toBeUndefined();
    await expect(
      assertCandidateIpnsMutationReady(
        databaseUrl,
        {
          demoPlanId: demoPlan.demoPlanId,
          demoPlanSha256: demoPlan.demoPlanSha256,
        },
        "query_table",
      ),
    ).rejects.toThrow("domain order");
    await markCandidateIpnsUpdateInFlight(
      databaseUrl,
      {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      },
      "open_data",
    );
    const policyObservations = targetObservations(
      demoPlan.targets.openData.targetCid,
    );
    policyObservations[2] = {
      ...policyObservations[2]!,
      observedCid: priorOpen,
    };
    await expect(
      recordCandidateResolutionCycle(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        domain: "open_data",
        observations: policyObservations,
        resolverPolicyId: CANDIDATE_FILEBASE_DWEB_POLICY,
      }),
    ).resolves.toMatchObject({
      classification: "target_observed",
      sequence: 1,
    });
    await checkpointCandidateIpnsVerified(databaseUrl, {
      demoPlanId: demoPlan.demoPlanId,
      demoPlanSha256: demoPlan.demoPlanSha256,
      domain: "open_data",
    });
    await expect(
      assertCandidateIpnsMutationReady(
        databaseUrl,
        {
          demoPlanId: demoPlan.demoPlanId,
          demoPlanSha256: demoPlan.demoPlanSha256,
        },
        "query_table",
      ),
    ).resolves.toBeUndefined();
    await markCandidateIpnsUpdateInFlight(
      databaseUrl,
      {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      },
      "query_table",
    );
    const splitObservations = targetObservations(
      demoPlan.targets.queryTable.targetCid,
    );
    splitObservations[3] = {
      ...splitObservations[3]!,
      observedCid: priorQuery,
    };
    await expect(
      recordCandidateResolutionCycle(databaseUrl, {
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        domain: "query_table",
        observations: splitObservations,
      }),
    ).resolves.toMatchObject({ classification: "split", sequence: 1 });
    const signedSql = postgres(databaseUrl, { max: 1 });
    let queryIntentId: string;
    try {
      const intentRows = await signedSql<{ intent_id: string }[]>`
        SELECT intent_id FROM oracle_candidate_demo_ipns_intents
        WHERE demo_plan_id = ${demoPlan.demoPlanId} AND domain = 'query_table'
      `;
      queryIntentId = intentRows[0]!.intent_id;
    } finally {
      await signedSql.end({ timeout: 5 });
    }
    const signedWithoutHash = {
      approvalId: approval.approvalId!,
      classification: "converged" as const,
      delegated: {
        endpointType: "ipfs_delegated_routing_v1" as const,
        httpStatus: 200,
        latencyMs: 8,
        observedAt: "2026-08-30T00:00:06.000Z",
        observedCid: demoPlan.targets.queryTable.targetCid,
        outcome: "validated" as const,
        requestCount: 1,
        responseBytes: 256,
        responseSha256: "7".repeat(64),
        schemaVersion: "candidate_signed_ipns_observation_v1" as const,
        sequence: "9",
        ttlNanoseconds: "300000000000",
        validationResult: "valid_target" as const,
        validity: "2026-08-31T00:00:00.000Z",
      },
      demoPlanId: demoPlan.demoPlanId,
      demoPlanSha256: demoPlan.demoPlanSha256,
      domain: "query_table" as const,
      filebaseControl: {
        endpointType: "filebase_names_control" as const,
        httpStatus: 200,
        latencyMs: 5,
        observedAt: "2026-08-30T00:00:04.000Z",
        observedCid: demoPlan.targets.queryTable.targetCid,
        outcome: "resolved" as const,
        requestCount: 1 as const,
        responseBytes: 128,
        responseSha256: "5".repeat(64),
      },
      filebaseGateway: {
        endpointType: "filebase_public_gateway" as const,
        httpStatus: 301,
        latencyMs: 6,
        observedAt: "2026-08-30T00:00:05.000Z",
        observedCid: demoPlan.targets.queryTable.targetCid,
        outcome: "resolved" as const,
        requestCount: 1 as const,
        responseBytes: 0,
        responseSha256: "6".repeat(64),
      },
      intentId: queryIntentId,
      networkKey: demoPlan.targets.queryTable.ipnsNetworkKey,
      policyVersion: "candidate_signed_ipns_observation_v1" as const,
      priorCid: priorQuery,
      requestCount: 3,
      targetCid: demoPlan.targets.queryTable.targetCid,
    };
    const signedEvidence = {
      ...signedWithoutHash,
      evidenceSha256: canonicalJsonSha256(signedWithoutHash),
    };
    const signedResult = await recordCandidateSignedIpnsObservation(
      databaseUrl,
      signedEvidence,
    );
    expect(signedResult).toMatchObject({ classification: "converged" });
    expect(
      await recordCandidateSignedIpnsObservation(databaseUrl, signedEvidence),
    ).toEqual(signedResult);
    const delegatedAuthorization =
      await authorizeCandidateDelegatedResolverPolicy(databaseUrl, {
        approvalId: approval.approvalId!,
        authorizedAt: "2026-08-30T00:00:07.000Z",
        authorizerReference: "synthetic-controller",
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        policyId: CANDIDATE_FILEBASE_DELEGATED_POLICY,
        queryIntentId,
        queryNetworkKey: demoPlan.targets.queryTable.ipnsNetworkKey,
        queryPriorCid: priorQuery,
        queryTargetCid: demoPlan.targets.queryTable.targetCid,
        signedEvidenceId: signedResult.evidenceId,
        signedEvidenceSha256: signedResult.evidenceSha256,
      });
    expect(delegatedAuthorization).toMatchObject({
      policyId: CANDIDATE_FILEBASE_DELEGATED_POLICY,
    });
    expect(
      await authorizeCandidateDelegatedResolverPolicy(databaseUrl, {
        approvalId: approval.approvalId!,
        authorizedAt: "2026-08-30T00:00:07.000Z",
        authorizerReference: "synthetic-controller",
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
        policyId: CANDIDATE_FILEBASE_DELEGATED_POLICY,
        queryIntentId,
        queryNetworkKey: demoPlan.targets.queryTable.ipnsNetworkKey,
        queryPriorCid: priorQuery,
        queryTargetCid: demoPlan.targets.queryTable.targetCid,
        signedEvidenceId: signedResult.evidenceId,
        signedEvidenceSha256: signedResult.evidenceSha256,
      }),
    ).toEqual(delegatedAuthorization);

    const hostile = postgres(databaseUrl, { max: 1 });
    try {
      await expect(hostile`
        UPDATE oracle_candidate_demo_ipns_intents
        SET state = 'verified', revision = revision + 1
        WHERE intent_id = ${queryIntentId}
      `).rejects.toThrow("immutable completion evidence");
      await expect(hostile`
        UPDATE oracle_candidate_demo_plans
        SET state = 'completed', revision = revision + 1
        WHERE demo_plan_id = ${demoPlan.demoPlanId}
      `).rejects.toThrow("exact verified intents");
    } finally {
      await hostile.end({ timeout: 5 });
    }
    const delegatedCompletion = await completeCandidateDemoWithDelegatedPolicy(
      databaseUrl,
      {
        authorizationId: delegatedAuthorization.authorizationId,
        authorizationSha256: delegatedAuthorization.authorizationSha256,
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      },
    );
    expect(delegatedCompletion).toMatchObject({
      remoteMutationPerformed: false,
      state: { state: "completed" },
    });
    expect(
      await completeCandidateDemoWithDelegatedPolicy(databaseUrl, {
        authorizationId: delegatedAuthorization.authorizationId,
        authorizationSha256: delegatedAuthorization.authorizationSha256,
        demoPlanId: demoPlan.demoPlanId,
        demoPlanSha256: demoPlan.demoPlanSha256,
      }),
    ).toEqual(delegatedCompletion);
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      const counts = await sql<
        {
          approvals: number;
          completions: number;
          cycles: number;
          intents: number;
          official_approvals: number;
        }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_candidate_demo_approvals) AS approvals,
          (SELECT count(*)::int FROM oracle_candidate_demo_delegated_completions) AS completions,
          (SELECT count(*)::int FROM oracle_candidate_demo_resolution_cycles) AS cycles,
          (SELECT count(*)::int FROM oracle_candidate_demo_ipns_intents) AS intents,
          (SELECT count(*)::int FROM oracle_publication_approvals) AS official_approvals
      `;
      expect(counts[0]).toEqual({
        approvals: 1,
        completions: 1,
        cycles: 2,
        intents: 2,
        official_approvals: 0,
      });
      const signedCount = await sql<{ count: number }[]>`
        SELECT count(*)::int AS count
        FROM oracle_candidate_demo_signed_ipns_observations
      `;
      expect(signedCount[0]?.count).toBe(1);
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
