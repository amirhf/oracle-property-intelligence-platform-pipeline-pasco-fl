import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  admitCandidateSourceSnapshotPreflightRequest,
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  createCandidateSourceSnapshotDemoIpnsIntents,
  expectedCandidateSourceSnapshotUploadReceiptSha256,
  loadCandidateSourceSnapshotIpnsIntentState,
  loadCandidateSourceSnapshotDemoExecutionAdmission,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotDemoPlan,
} from "../../src/db/candidate-source-snapshot-demo.js";
import {
  createCandidateSourceSnapshotApprovalIdentity,
  renderCandidateSourceSnapshotAuthorizationStatement,
} from "../../src/db/candidate-source-snapshot-approval.js";
import { recordCandidateSourceSnapshotUploadClosure } from "../../src/db/candidate-source-snapshot-completion.js";
import { runMigrations } from "../../src/db/migrations.js";
import { loadCandidateSourceSnapshotExecutionConfig } from "../../src/publication/candidate-source-snapshot-executor-config.js";
import type { CandidateSourceSnapshotDemoPlan } from "../../src/publication/candidate-source-snapshot-demo.js";
import {
  createCandidateSourceSnapshotRemoteRuntime,
  DurableIpnsBridge,
} from "../../src/publication/candidate-source-snapshot-remote-runtime.js";
import type { CandidateSourceSnapshotUploadClosure } from "../../src/db/candidate-source-snapshot-completion.js";
import {
  recordSuccessfulCandidateSourceSnapshotPreflight,
  syntheticCandidateSourceSnapshotDemo,
} from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_execution_api_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const implementationCommitSha = "1".repeat(40);
let executedPlan: CandidateSourceSnapshotDemoPlan;
let executedClosure: CandidateSourceSnapshotUploadClosure;

function enabledEnvironment(plan: CandidateSourceSnapshotDemoPlan) {
  const access = "synthetic-access";
  const secret = "synthetic-secret";
  return {
    CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID: access,
    CANDIDATE_DEMO_FILEBASE_API_ENDPOINT: "https://api.filebase.io",
    CANDIDATE_DEMO_FILEBASE_API_TOKEN: Buffer.from(
      `${access}:${secret}`,
    ).toString("base64"),
    CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.io",
    CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY: secret,
    CANDIDATE_DEMO_MAX_BUDGET_USD: String(plan.limits.maxBudgetUsd),
    CANDIDATE_DEMO_MAX_CONCURRENCY: String(plan.limits.maxConcurrency),
    CANDIDATE_DEMO_MAX_OBJECT_BYTES: String(plan.limits.maxObjectBytes),
    CANDIDATE_DEMO_MAX_OBJECTS: String(plan.limits.maxObjects),
    CANDIDATE_DEMO_MAX_REQUESTS: String(plan.limits.maxRequests),
    CANDIDATE_DEMO_MAX_RETRIES: String(plan.limits.maxRetries),
    CANDIDATE_DEMO_MAX_TOTAL_BYTES: String(plan.limits.maxTotalBytes),
    CANDIDATE_DEMO_OPEN_DATA_BUCKET: plan.targets.openData.bucket,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL: plan.targets.openData.ipnsLabel,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY:
      plan.targets.openData.ipnsNetworkKey,
    CANDIDATE_DEMO_OPEN_DATA_PRIOR_CID: plan.targets.openData.priorCid,
    CANDIDATE_DEMO_OPEN_DATA_TARGET_CID: plan.targets.openData.targetCid,
    CANDIDATE_DEMO_QUERY_TABLE_BUCKET: plan.targets.queryTable.bucket,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL: plan.targets.queryTable.ipnsLabel,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY:
      plan.targets.queryTable.ipnsNetworkKey,
    CANDIDATE_DEMO_QUERY_TABLE_PRIOR_CID: plan.targets.queryTable.priorCid,
    CANDIDATE_DEMO_QUERY_TABLE_TARGET_CID: plan.targets.queryTable.targetCid,
    CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
    CANDIDATE_DEMO_REQUEST_TIMEOUT_MS: String(plan.limits.requestTimeoutMs),
    CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_ID: `snapshotdemoapproval_${"a".repeat(32)}`,
    CANDIDATE_SOURCE_SNAPSHOT_PLAN_ID: plan.planId,
    CANDIDATE_SOURCE_SNAPSHOT_PLAN_SHA256: plan.planSha256,
  };
}

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(databaseUrl)).toHaveLength(34);
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

describe("candidate source-snapshot guarded execution API", () => {
  it("binds capacity, exact human approval, execution, and both derived intents", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    const capacityRequest = {
      confirmedAt: "2026-08-31T00:01:00.000Z",
      confirmedPlanName: "Filebase Pro or better" as const,
      confirmerReference: "synthetic-capacity-controller",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const capacity = await confirmCandidateSourceSnapshotDemoCapacity(
      databaseUrl,
      capacityRequest,
    );
    expect(capacity).toMatchObject({
      approvalCount: 0,
      revision: 2,
      state: "awaiting_approval",
    });
    await expect(
      confirmCandidateSourceSnapshotDemoCapacity(databaseUrl, capacityRequest),
    ).resolves.toEqual(capacity);
    const approvalRequest = {
      approvedAt: "2026-08-31T00:02:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
          implementationCommitSha,
        ),
      implementationCommitSha,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const directApproval = createCandidateSourceSnapshotApprovalIdentity({
      approvedAt: approvalRequest.approvedAt,
      approverReference: approvalRequest.approverReference,
      exactUpload: fixture.exactUpload,
      implementationCommitSha,
      plan: fixture.plan,
      statement: approvalRequest.authorizationStatement,
    });
    await expect(
      admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
        attemptSequence: 1,
        domain: "open_data",
        operationKind: "bucket_head",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        redirectSequence: 0,
        resolver: null,
      }),
    ).rejects.toThrow("requires an eligible exact plan");
    const sql = postgres(databaseUrl, { max: 1 });
    try {
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_request_categories
          SET consumed_request_count = consumed_request_count + 1,
              request_cost_usd = request_cost_usd + 0.0000045,
              revision = revision + 1
          WHERE plan_id = ${fixture.plan.planId}
            AND request_category = 'bucket_names_preflight'
        `,
      ).rejects.toThrow("request category admission is invalid");
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_requests (
            request_id, plan_id, operation_class, operation_kind, domain,
            request_cost_usd, outcome, request_category, logical_request_id,
            attempt_sequence, redirect_sequence
          ) VALUES (
            ${`snapshotdemorequest_${"e".repeat(32)}`},
            ${fixture.plan.planId}, 'class_b_read', 'bucket_head', 'open_data',
            0.0000045, 'request_started', 'bucket_names_preflight',
            ${`snapshotdemologicalrequest_${"d".repeat(32)}`}, 1, 0
          )
        `,
      ).rejects.toThrow(
        "requires the executing v2.1 plan or its bounded intent-free preflight",
      );
      await expect(
        sql`
          INSERT INTO oracle_candidate_source_snapshot_demo_approvals (
            approval_id, plan_id, plan_sha256, plan_artifact_sha256,
            plan_artifact_cid, plan_artifact_remote_object_key,
            plan_artifact_bytes, approved_plan_revision,
            approver_reference, approved_at, approved_at_iso,
            approval_version, approval_sha256, authorization_statement,
            authorization_statement_sha256, authorization_binding,
            authorization_binding_sha256, implementation_commit_sha
          ) VALUES (
            ${directApproval.approvalId}, ${fixture.plan.planId},
            ${fixture.plan.planSha256},
            ${fixture.exactUpload.planArtifact.sha256},
            ${fixture.exactUpload.planArtifact.expectedCid},
            ${fixture.exactUpload.planArtifact.remoteObjectKey},
            ${fixture.exactUpload.planArtifact.byteSize}, ${capacity.revision},
            ${approvalRequest.approverReference}, ${approvalRequest.approvedAt},
            ${approvalRequest.approvedAt}, ${directApproval.approvalVersion},
            ${directApproval.approvalSha256},
            ${`${approvalRequest.authorizationStatement} `},
            ${directApproval.authorizationStatementSha256},
            ${sql.json(directApproval.authorizationBinding as postgres.JSONValue)},
            ${directApproval.authorizationBindingSha256},
            ${implementationCommitSha}
          )
        `,
      ).rejects.toThrow(
        "requires the exact v2.1 category authorization binding and statement",
      );
    } finally {
      await sql.end({ timeout: 5 });
    }
    const approved = await approveCandidateSourceSnapshotDemoPlan(
      databaseUrl,
      approvalRequest,
    );
    expect(approved.state).toMatchObject({
      approvalCount: 1,
      revision: 3,
      state: "approved",
    });
    expect(approved.approvalSha256).toMatch(/^[a-f0-9]{64}$/);
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, approvalRequest),
    ).resolves.toEqual(approved);
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...approvalRequest,
        approverReference: "conflicting-human-approver",
      }),
    ).rejects.toThrow("approval conflicts");
    await expect(
      approveCandidateSourceSnapshotDemoPlan(databaseUrl, {
        ...approvalRequest,
        authorizationStatement: `${approvalRequest.authorizationStatement} `,
      }),
    ).rejects.toThrow("does not exactly match");

    const beforePreflightSql = postgres(databaseUrl, { max: 1 });
    try {
      const beforePreflight = await beforePreflightSql<
        { effects: number; requests: number; state: string }[]
      >`
        SELECT plan.state,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_demo_requests request
                WHERE request.plan_id = plan.plan_id) AS requests,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_demo_upload_attempts effect
                WHERE effect.plan_id = plan.plan_id) AS effects
        FROM oracle_candidate_source_snapshot_demo_plans plan
        WHERE plan.plan_id = ${fixture.plan.planId}
      `;
      expect(beforePreflight).toEqual([
        { effects: 0, requests: 0, state: "approved" },
      ]);
      await expect(
        beforePreflightSql`
          UPDATE oracle_candidate_source_snapshot_demo_plans
          SET state = 'executing', revision = revision + 1
          WHERE plan_id = ${fixture.plan.planId}
        `,
      ).rejects.toThrow("complete preflight evidence");
    } finally {
      await beforePreflightSql.end({ timeout: 5 });
    }
    await expect(
      beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
        approvalId: approved.approvalId,
        executorEnabled: true,
        implementationCommitSha,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("eight exact successful logical preflight receipts");
    await recordSuccessfulCandidateSourceSnapshotPreflight(
      databaseUrl,
      fixture.plan,
    );

    const verificationSql = postgres(databaseUrl, { max: 1 });
    try {
      const approvalRows = await verificationSql<
        {
          approval_sha256: string;
          authorization_binding_sha256: string;
          authorization_statement_sha256: string;
          implementation_commit_sha: string;
        }[]
      >`
        SELECT approval_sha256, authorization_binding_sha256,
               authorization_statement_sha256, implementation_commit_sha
        FROM oracle_candidate_source_snapshot_demo_approvals
        WHERE approval_id = ${approved.approvalId}
      `;
      expect(approvalRows).toEqual([
        {
          approval_sha256: approved.approvalSha256,
          authorization_binding_sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
          authorization_statement_sha256:
            expect.stringMatching(/^[a-f0-9]{64}$/),
          implementation_commit_sha: implementationCommitSha,
        },
      ]);
      await expect(
        verificationSql`
          UPDATE oracle_candidate_source_snapshot_demo_approvals
          SET authorization_statement = authorization_statement || ' '
          WHERE approval_id = ${approved.approvalId}
        `,
      ).rejects.toThrow("immutable");
      await expect(
        verificationSql`
          DELETE FROM oracle_candidate_source_snapshot_demo_approvals
          WHERE approval_id = ${approved.approvalId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await verificationSql.end({ timeout: 5 });
    }

    await expect(
      beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
        approvalId: approved.approvalId,
        executorEnabled: true,
        implementationCommitSha: "2".repeat(40),
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("lacks exact approval");

    const execution = await beginCandidateSourceSnapshotDemoExecution(
      databaseUrl,
      {
        approvalId: approved.approvalId,
        executorEnabled: true,
        implementationCommitSha,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    expect(execution).toMatchObject({ revision: 4, state: "executing" });

    const uploadJournal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    for (const [index, object] of fixture.objects.entries()) {
      const admissions =
        index === 0
          ? await Promise.all([
              uploadJournal.startAttempt(fixture.plan, object, 1),
              new PostgresCandidateSourceSnapshotUploadJournal(
                databaseUrl,
              ).startAttempt(fixture.plan, object, 1),
            ])
          : [await uploadJournal.startAttempt(fixture.plan, object, 1)];
      if (index === 0) {
        expect(
          admissions.map((admission) => admission.alreadyRecorded).sort(),
        ).toEqual([false, true]);
        expect(admissions[1]!.attempt).toEqual(admissions[0]!.attempt);
      }
      const admission =
        admissions.find((candidate) => !candidate.alreadyRecorded) ??
        admissions[0]!;
      const responseBytes = index + 1;
      await uploadJournal.recordVerified(
        fixture.plan,
        object,
        admission.attempt,
        {
          providerCid: object.expectedCid,
          providerRequestIdHash: null,
          receiptSha256: expectedCandidateSourceSnapshotUploadReceiptSha256({
            attempt: admission.attempt,
            object,
            providerCid: object.expectedCid,
            providerRequestIdHash: null,
            responseBytes,
          }),
          responseBytes,
        },
      );
    }
    const uploadRaceSql = postgres(databaseUrl, { max: 1 });
    try {
      const raceRows = await uploadRaceSql<
        { attempts: number; requests: number }[]
      >`
        SELECT
          (SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_demo_upload_attempts
           WHERE plan_id = ${fixture.plan.planId}
             AND remote_object_key = ${fixture.objects[0]!.remoteObjectKey})
            AS attempts,
          (SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_demo_requests
           WHERE plan_id = ${fixture.plan.planId}
             AND remote_object_key = ${fixture.objects[0]!.remoteObjectKey}
             AND operation_kind = 'put_object') AS requests
      `;
      expect(raceRows).toEqual([{ attempts: 1, requests: 1 }]);
    } finally {
      await uploadRaceSql.end({ timeout: 5 });
    }
    executedClosure = await recordCandidateSourceSnapshotUploadClosure(
      databaseUrl,
      {
        approvalId: approved.approvalId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    executedPlan = fixture.plan;

    const intentRequest = {
      intendedAt: "2026-08-31T00:03:00.000Z",
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const intents = await createCandidateSourceSnapshotDemoIpnsIntents(
      databaseUrl,
      intentRequest,
    );
    expect(intents.map((intent) => intent.domain)).toEqual([
      "open_data",
      "query_table",
    ]);
    await expect(
      createCandidateSourceSnapshotDemoIpnsIntents(databaseUrl, intentRequest),
    ).resolves.toEqual(intents);

    const admission = await loadCandidateSourceSnapshotDemoExecutionAdmission(
      databaseUrl,
      {
        approvalId: approved.approvalId,
        implementationCommitSha,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    expect(admission).toMatchObject({
      approval: {
        approvalId: approved.approvalId,
        approvalSha256: approved.approvalSha256,
        authorizationBindingSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
        authorizationStatementSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
        implementationCommitSha,
      },
      capacityConfirmation: {
        confirmedPlanName: "Filebase Pro or better",
      },
      exactUpload: fixture.exactUpload,
      intents,
      state: { revision: 4, state: "executing" },
      unverifiedObjectCount: 0,
    });
    await expect(
      loadCandidateSourceSnapshotDemoExecutionAdmission(databaseUrl, {
        approvalId: approved.approvalId,
        implementationCommitSha: "2".repeat(40),
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      }),
    ).rejects.toThrow("execution admission is incomplete");
  });

  it("uses one shared resolver sequence per cycle and retries only the complete cycle", async () => {
    const config = loadCandidateSourceSnapshotExecutionConfig(
      enabledEnvironment(executedPlan),
      executedPlan,
    );
    if (!config.enabled) throw new Error("synthetic executor was not enabled");
    let namesReads = 0;
    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/names")) {
        namesReads += 1;
        if (namesReads === 1) return new Response(null, { status: 503 });
        return new Response(
          JSON.stringify([
            {
              cid: executedPlan.targets.openData.priorCid,
              enabled: true,
              label: executedPlan.targets.openData.ipnsLabel,
              network_key: executedPlan.targets.openData.ipnsNetworkKey,
            },
            {
              cid: executedPlan.targets.queryTable.priorCid,
              enabled: true,
              label: executedPlan.targets.queryTable.ipnsLabel,
              network_key: executedPlan.targets.queryTable.ipnsNetworkKey,
            },
          ]),
          { headers: { "content-type": "application/json" }, status: 200 },
        );
      }
      const domain = url.includes(executedPlan.targets.openData.ipnsNetworkKey)
        ? "open_data"
        : "query_table";
      const prior =
        domain === "open_data"
          ? executedPlan.targets.openData.priorCid
          : executedPlan.targets.queryTable.priorCid;
      return new Response(null, {
        headers: { "x-ipfs-path": `/ipfs/${prior}` },
        status: 200,
      });
    };
    let delegatedReads = 0;
    const runtime = createCandidateSourceSnapshotRemoteRuntime({
      config,
      databaseUrl,
      dependencies: {
        bucketProbe: {
          close() {},
          async headBucket() {
            throw new Error("replayed preflight must not probe buckets");
          },
        },
        credentialFreeVerifier: {
          async verify() {
            throw new Error("intent preparation must not run final verifier");
          },
        },
        fetchImpl,
        observeDelegated: async ({ expectedPriorCid, fetchImpl }) => {
          delegatedReads += 1;
          if (!fetchImpl) throw new Error("delegated transport is missing");
          await fetchImpl(
            `https://delegated-ipfs.dev/routing/v1/ipns/synthetic-${delegatedReads}`,
          );
          return {
            endpointType: "ipfs_delegated_routing_v1",
            httpStatus: 200,
            latencyMs: 1,
            observedAt: `2026-08-31T00:04:0${delegatedReads}.000Z`,
            observedCid: expectedPriorCid,
            outcome: "validated",
            requestCount: 1,
            responseBytes: 32,
            responseSha256: String(delegatedReads).padStart(64, "0"),
            schemaVersion: "candidate_signed_ipns_observation_v1",
            sequence: String(delegatedReads),
            ttlNanoseconds: "60000000000",
            validationResult: "valid_prior",
            validity: "2026-09-01T00:00:00.000000000Z",
          };
        },
      },
      plan: executedPlan,
    });
    try {
      await runtime.readOnlyPreflight();
      const intents = await runtime.prepareIntents({
        createInitialIntents: async () =>
          await createCandidateSourceSnapshotDemoIpnsIntents(databaseUrl, {
            intendedAt: "2026-08-31T00:03:00.000Z",
            planId: executedPlan.planId,
            planSha256: executedPlan.planSha256,
          }),
        intendedAt: "2026-08-31T00:03:00.000Z",
        plan: executedPlan,
        uploadClosure: executedClosure,
      });
      expect(intents.map((intent) => intent.state)).toEqual([
        "prior_confirmed",
        "prior_confirmed",
      ]);
      const resumed = await runtime.prepareIntents({
        createInitialIntents: async () =>
          await createCandidateSourceSnapshotDemoIpnsIntents(databaseUrl, {
            intendedAt: "2026-08-31T00:03:00.000Z",
            planId: executedPlan.planId,
            planSha256: executedPlan.planSha256,
          }),
        intendedAt: "2026-08-31T00:03:00.000Z",
        plan: executedPlan,
        uploadClosure: executedClosure,
      });
      expect(resumed.map((intent) => intent.state)).toEqual([
        "prior_confirmed",
        "prior_confirmed",
      ]);
      expect(namesReads).toBe(5);
      expect(delegatedReads).toBe(5);
      const sql = postgres(databaseUrl, { max: 1 });
      try {
        const cycles = await sql<
          {
            cycle_sequence: number;
            domain: string;
            resolver_count: number;
          }[]
        >`
          SELECT domain, cycle_sequence,
                 count(DISTINCT resolver)::integer AS resolver_count
          FROM oracle_candidate_source_snapshot_demo_requests
          WHERE plan_id = ${executedPlan.planId}
            AND request_category = 'control_public_observation'
          GROUP BY domain, cycle_sequence
          ORDER BY domain, cycle_sequence
        `;
        expect(cycles).toEqual([
          { cycle_sequence: 1, domain: "open_data", resolver_count: 3 },
          { cycle_sequence: 2, domain: "open_data", resolver_count: 3 },
          { cycle_sequence: 1, domain: "query_table", resolver_count: 3 },
        ]);
        const recoveryCycles = await sql<
          {
            cycle_sequence: number;
            domain: string;
            resolver_count: number;
          }[]
        >`
          SELECT domain, cycle_sequence,
                 count(DISTINCT resolver)::integer AS resolver_count
          FROM oracle_candidate_source_snapshot_demo_requests
          WHERE plan_id = ${executedPlan.planId}
            AND request_category = 'recovery'
          GROUP BY domain, cycle_sequence
          ORDER BY domain, cycle_sequence
        `;
        expect(recoveryCycles).toEqual([
          { cycle_sequence: 3, domain: "open_data", resolver_count: 3 },
          { cycle_sequence: 2, domain: "query_table", resolver_count: 3 },
        ]);
      } finally {
        await sql.end({ timeout: 5 });
      }
    } finally {
      await runtime.close();
    }
  });

  it("atomically checkpoints a resolver cycle and fails closed on an unresolved admitted read", async () => {
    const sql = postgres(databaseUrl, { max: 1 });
    let intentId: string;
    try {
      const rows = await sql<{ intent_id: string }[]>`
        SELECT intent_id
        FROM oracle_candidate_source_snapshot_demo_ipns_intents
        WHERE plan_id = ${executedPlan.planId} AND domain = 'open_data'
      `;
      intentId = rows[0]!.intent_id;
    } finally {
      await sql.end({ timeout: 5 });
    }
    const journal = new PostgresCandidateSourceSnapshotUploadJournal(
      databaseUrl,
    );
    const resolvers = [
      "filebase_control",
      "filebase_gateway",
      "delegated_ipfs",
    ] as const;
    const requests = await Promise.all(
      resolvers.map(
        async (resolver) =>
          await journal.startResolutionRequest(executedPlan, {
            cycleSequence: 4,
            domain: "open_data",
            intentId,
            requestCategory: "recovery",
            resolver,
          }),
      ),
    );
    const observations = requests.map((request, index) => ({
      observation: {
        classification: "prior" as const,
        evidenceSha256: (index + 10).toString(16).padStart(64, "0"),
        observedAt: `2026-08-31T00:05:0${index}.000Z`,
        observedCid: executedPlan.targets.openData.priorCid,
        requestOutcome: "succeeded" as const,
      },
      request,
    }));

    await expect(
      journal.recordResolutionObservation(
        executedPlan,
        observations[2]!.request,
        observations[2]!.observation,
      ),
    ).rejects.toThrow("must be recorded as one atomic cycle");
    await expect(
      journal.recordResolutionCycle(executedPlan, [
        observations[0]!,
        observations[1]!,
        {
          ...observations[2]!,
          request: {
            ...observations[2]!.request,
            requestId: `snapshotdemorequest_${"0".repeat(32)}`,
          },
        },
      ]),
    ).rejects.toThrow("lacks its admitted request");

    const afterConflict = postgres(databaseUrl, { max: 1 });
    try {
      const rows = await afterConflict<
        { observation_count: number; request_started_count: number }[]
      >`
        SELECT
          (SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_demo_ipns_observations
           WHERE intent_id = ${intentId} AND cycle_sequence = 4)
            AS observation_count,
          (SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_demo_requests
           WHERE intent_id = ${intentId} AND cycle_sequence = 4
             AND outcome = 'request_started') AS request_started_count
      `;
      expect(rows).toEqual([
        { observation_count: 0, request_started_count: 3 },
      ]);
    } finally {
      await afterConflict.end({ timeout: 5 });
    }

    await expect(
      journal.recordResolutionCycle(executedPlan, observations),
    ).resolves.toBeUndefined();
    await expect(
      journal.recordResolutionCycle(executedPlan, observations),
    ).resolves.toBeUndefined();

    await journal.startResolutionRequest(executedPlan, {
      cycleSequence: 5,
      domain: "open_data",
      intentId,
      requestCategory: "recovery",
      resolver: "filebase_control",
    });
    const intents = await Promise.all(
      (["open_data", "query_table"] as const).map(async (domain) => {
        const intentSql = postgres(databaseUrl, { max: 1 });
        try {
          const rows = await intentSql<{ intent_id: string }[]>`
            SELECT intent_id
            FROM oracle_candidate_source_snapshot_demo_ipns_intents
            WHERE plan_id = ${executedPlan.planId} AND domain = ${domain}
          `;
          return await loadCandidateSourceSnapshotIpnsIntentState(databaseUrl, {
            domain,
            intentId: rows[0]!.intent_id,
            planId: executedPlan.planId,
            planSha256: executedPlan.planSha256,
          });
        } finally {
          await intentSql.end({ timeout: 5 });
        }
      }),
    );
    const bridge = new DurableIpnsBridge({ databaseUrl, plan: executedPlan });
    await expect(bridge.bindIntents(intents)).rejects.toThrow(
      "Unresolved admitted resolution cycle requires manual reconciliation",
    );
  });
});
