import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  admitCandidateSourceSnapshotPreflightRequest,
  approveCandidateSourceSnapshotDemoPlan,
  beginCandidateSourceSnapshotDemoExecution,
  confirmCandidateSourceSnapshotDemoCapacity,
  recordCandidateSourceSnapshotDemoPlan,
  recordCandidateSourceSnapshotPreflightRequestOutcome,
} from "../../src/db/candidate-source-snapshot-demo.js";
import {
  proposeCandidateSourceSnapshotPreflightContinuation,
  recordCandidateSourceSnapshotPreflightContinuation,
} from "../../src/db/candidate-source-snapshot-preflight-continuation.js";
import { renderCandidateSourceSnapshotAuthorizationStatement } from "../../src/db/candidate-source-snapshot-approval.js";
import { runMigrations } from "../../src/db/migrations.js";
import { loadCandidateSourceSnapshotExecutionConfig } from "../../src/publication/candidate-source-snapshot-executor-config.js";
import { createCandidateSourceSnapshotRemoteRuntime } from "../../src/publication/candidate-source-snapshot-remote-runtime.js";
import type { CandidateSourceSnapshotDemoPlan } from "../../src/publication/candidate-source-snapshot-demo.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `candidate_preflight_continuation_${process.pid}_${Date.now()}`;
const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
const originalImplementationCommitSha = "1".repeat(40);
const amendedImplementationCommitSha = "2".repeat(40);
let disposableSchemaSequence = 0;

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

async function withDisposableDatabase(
  label: string,
  test: (isolatedDatabaseUrl: string) => Promise<void>,
): Promise<void> {
  disposableSchemaSequence += 1;
  const disposableSchema = `${label}_${process.pid}_${Date.now()}_${disposableSchemaSequence}`;
  const isolatedDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${disposableSchema}`;
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${disposableSchema}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  try {
    expect((await runMigrations(isolatedDatabaseUrl)).at(-1)).toBe(
      "035_candidate_source_snapshot_upload_resume.sql",
    );
    await test(isolatedDatabaseUrl);
  } finally {
    const cleanup = postgres(adminDatabaseUrl, { max: 1 });
    try {
      await cleanup.unsafe(`DROP SCHEMA ${disposableSchema} CASCADE`);
    } finally {
      await cleanup.end({ timeout: 5 });
    }
  }
}

async function prepareTerminalGatewayPreflight(isolatedDatabaseUrl: string) {
  const fixture = syntheticCandidateSourceSnapshotDemo();
  await recordCandidateSourceSnapshotDemoPlan(isolatedDatabaseUrl, fixture);
  await confirmCandidateSourceSnapshotDemoCapacity(isolatedDatabaseUrl, {
    confirmedAt: "2026-08-31T01:01:00.000Z",
    confirmedPlanName: "Filebase Pro or better",
    confirmerReference: "synthetic-capacity-controller",
    planId: fixture.plan.planId,
    planSha256: fixture.plan.planSha256,
  });
  const approved = await approveCandidateSourceSnapshotDemoPlan(
    isolatedDatabaseUrl,
    {
      approvedAt: "2026-08-31T01:02:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
          originalImplementationCommitSha,
        ),
      implementationCommitSha: originalImplementationCommitSha,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    },
  );
  const inputs = [
    ["bucket_head", null],
    ["names_read", "filebase_control"],
    ["public_resolve", "filebase_gateway"],
    ["public_resolve", "delegated_ipfs"],
  ] as const;
  const admissions: Array<
    Awaited<ReturnType<typeof admitCandidateSourceSnapshotPreflightRequest>>
  > = [];
  for (const [operationKind, resolver] of inputs) {
    const admission = await admitCandidateSourceSnapshotPreflightRequest(
      isolatedDatabaseUrl,
      {
        attemptSequence: 1,
        domain: "open_data",
        operationKind,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
        redirectSequence: 0,
        resolver,
      },
    );
    admissions.push(admission);
    await recordCandidateSourceSnapshotPreflightRequestOutcome(
      isolatedDatabaseUrl,
      {
        admission,
        completedAt: "2026-08-31T01:03:00.000Z",
        outcome:
          resolver === "filebase_gateway" ? "terminal_failure" : "succeeded",
        receiptSha256:
          resolver === "filebase_gateway" ? "3".repeat(64) : "4".repeat(64),
      },
    );
  }
  const failedGateway = admissions.find(
    (admission) => admission.resolver === "filebase_gateway",
  )!;
  const continuation =
    await proposeCandidateSourceSnapshotPreflightContinuation(
      isolatedDatabaseUrl,
      {
        amendedImplementationCommitSha,
        authorizedAt: "2026-08-31T01:04:00.000Z",
        authorizerReference: "synthetic-continuation-controller",
        failedRequestId: failedGateway.requestId,
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
  return {
    approved,
    continuation: await recordCandidateSourceSnapshotPreflightContinuation(
      isolatedDatabaseUrl,
      continuation,
    ),
    fixture,
  };
}

beforeAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  const applied = await runMigrations(databaseUrl);
  expect(applied.at(-1)).toBe(
    "035_candidate_source_snapshot_upload_resume.sql",
  );
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

describe("candidate source-snapshot preflight continuation", () => {
  it("binds one terminal receipt to one amended-commit continuation", async () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    await recordCandidateSourceSnapshotDemoPlan(databaseUrl, fixture);
    const capacity = await confirmCandidateSourceSnapshotDemoCapacity(
      databaseUrl,
      {
        confirmedAt: "2026-08-31T00:01:00.000Z",
        confirmedPlanName: "Filebase Pro or better",
        confirmerReference: "synthetic-capacity-controller",
        planId: fixture.plan.planId,
        planSha256: fixture.plan.planSha256,
      },
    );
    const openGatewayInput = {
      attemptSequence: 1,
      domain: "open_data" as const,
      operationKind: "public_resolve" as const,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
      redirectSequence: 0,
      resolver: "filebase_gateway" as const,
    };
    await expect(
      admitCandidateSourceSnapshotPreflightRequest(
        databaseUrl,
        openGatewayInput,
      ),
    ).rejects.toThrow("requires an eligible exact plan");

    const approvalInput = {
      approvedAt: "2026-08-31T00:02:00.000Z",
      approverReference: "synthetic-human-approver",
      authorizationStatement:
        renderCandidateSourceSnapshotAuthorizationStatement(
          fixture.plan,
          fixture.exactUpload,
          originalImplementationCommitSha,
        ),
      implementationCommitSha: originalImplementationCommitSha,
      planId: fixture.plan.planId,
      planSha256: fixture.plan.planSha256,
    };
    const approved = await approveCandidateSourceSnapshotDemoPlan(
      databaseUrl,
      approvalInput,
    );
    expect(capacity.revision).toBe(2);
    expect(approved.state).toMatchObject({ revision: 3, state: "approved" });

    const initialRequests = [
      ["bucket_head", null],
      ["names_read", "filebase_control"],
      ["public_resolve", "filebase_gateway"],
      ["public_resolve", "delegated_ipfs"],
    ] as const;
    const initialAdmissions = [];
    for (const [operationKind, resolver] of initialRequests) {
      const admission = await admitCandidateSourceSnapshotPreflightRequest(
        databaseUrl,
        {
          attemptSequence: 1,
          domain: "open_data",
          operationKind,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          redirectSequence: 0,
          resolver,
        },
      );
      initialAdmissions.push(admission);
      await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
        admission,
        completedAt: "2026-08-31T00:03:00.000Z",
        outcome:
          resolver === "filebase_gateway" ? "terminal_failure" : "succeeded",
        receiptSha256:
          resolver === "filebase_gateway" ? "3".repeat(64) : "4".repeat(64),
      });
    }
    const failedGateway = initialAdmissions.find(
      (admission) => admission.resolver === "filebase_gateway",
    )!;

    const sql = postgres(databaseUrl, { max: 4 });
    try {
      const beforeRejectedRetry = await sql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category = 'bucket_names_preflight'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(beforeRejectedRetry[0]).toEqual({
        category_count: 4,
        request_count: 4,
      });
      await expect(
        sql`
          UPDATE oracle_candidate_source_snapshot_demo_requests
          SET outcome = 'succeeded', receipt_sha256 = ${"7".repeat(64)},
              completed_at = '2026-08-31T00:03:30.000Z'
          WHERE request_id = ${failedGateway.requestId}
        `,
      ).rejects.toThrow("immutable or terminal");
      await expect(
        admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
          ...openGatewayInput,
          attemptSequence: 2,
        }),
      ).rejects.toThrow("cannot bypass terminal receipt");
      const afterRejectedRetry = await sql<
        { category_count: number; request_count: number }[]
      >`
        SELECT accounting.request_count,
               category.consumed_request_count AS category_count
        FROM oracle_candidate_source_snapshot_demo_accounting accounting
        JOIN oracle_candidate_source_snapshot_demo_request_categories category
          ON category.plan_id = accounting.plan_id
         AND category.request_category = 'bucket_names_preflight'
        WHERE accounting.plan_id = ${fixture.plan.planId}
      `;
      expect(afterRejectedRetry[0]).toEqual(beforeRejectedRetry[0]);

      const proposed =
        await proposeCandidateSourceSnapshotPreflightContinuation(databaseUrl, {
          amendedImplementationCommitSha,
          authorizedAt: "2026-08-31T00:04:00.000Z",
          authorizerReference: "synthetic-continuation-controller",
          failedRequestId: failedGateway.requestId,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        });
      const recorded = await recordCandidateSourceSnapshotPreflightContinuation(
        databaseUrl,
        proposed,
      );
      await expect(
        recordCandidateSourceSnapshotPreflightContinuation(
          databaseUrl,
          proposed,
        ),
      ).resolves.toEqual(recorded);
      await expect(
        recordCandidateSourceSnapshotPreflightContinuation(databaseUrl, {
          ...proposed,
          authorizerReference: "different-continuation-controller",
        }),
      ).rejects.toThrow("authorization conflicts");
      await expect(
        sql`
          UPDATE oracle_candidate_source_preflight_continuation_authorizations
          SET amended_implementation_commit_sha = ${"5".repeat(40)}
          WHERE authorization_id = ${recorded.authorizationId}
        `,
      ).rejects.toThrow("is immutable");
      await expect(
        sql`
          DELETE FROM oracle_candidate_source_preflight_continuation_authorizations
          WHERE authorization_id = ${recorded.authorizationId}
        `,
      ).rejects.toThrow("is immutable");

      await expect(
        admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
          attemptSequence: 1,
          continuationAuthorizationId: recorded.authorizationId,
          domain: "query_table",
          operationKind: "public_resolve",
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          redirectSequence: 0,
          resolver: "filebase_gateway",
        }),
      ).rejects.toThrow(
        "restricted to an exact authorized official-gateway observation",
      );

      const incomplete = await sql<{ ready: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
          ${fixture.plan.planId}
        ) AS ready
      `;
      expect(incomplete[0]?.ready).toBe(false);
      await expect(
        beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
          approvalId: approved.approvalId,
          continuationAuthorizationId: recorded.authorizationId,
          executorEnabled: true,
          implementationCommitSha: amendedImplementationCommitSha,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).rejects.toThrow("eight exact successful logical preflight receipts");
      const config = loadCandidateSourceSnapshotExecutionConfig(
        enabledEnvironment(fixture.plan),
        fixture.plan,
      );
      if (!config.enabled)
        throw new Error("synthetic executor was not enabled");
      const bucketDomains: string[] = [];
      const networkReads: string[] = [];
      let closeCount = 0;
      const fetchImpl: typeof fetch = async (request) => {
        const url = String(request);
        if (url.endsWith("/v1/names")) {
          networkReads.push("query_table:filebase_control");
          return new Response(
            JSON.stringify([
              {
                cid: fixture.plan.targets.openData.priorCid,
                enabled: true,
                label: fixture.plan.targets.openData.ipnsLabel,
                network_key: fixture.plan.targets.openData.ipnsNetworkKey,
              },
              {
                cid: fixture.plan.targets.queryTable.priorCid,
                enabled: true,
                label: fixture.plan.targets.queryTable.ipnsLabel,
                network_key: fixture.plan.targets.queryTable.ipnsNetworkKey,
              },
            ]),
            { headers: { "content-type": "application/json" }, status: 200 },
          );
        }
        if (url.includes("delegated-ipfs.dev")) {
          networkReads.push("query_table:delegated_ipfs");
          return new Response(null, { status: 200 });
        }
        const domain = url.includes(
          fixture.plan.targets.openData.ipnsNetworkKey,
        )
          ? "open_data"
          : "query_table";
        networkReads.push(`${domain}:filebase_gateway`);
        const prior =
          domain === "open_data"
            ? fixture.plan.targets.openData.priorCid
            : fixture.plan.targets.queryTable.priorCid;
        return new Response(null, {
          headers: { "x-ipfs-path": `/ipfs/${prior}` },
          status: 200,
        });
      };
      const runtime = createCandidateSourceSnapshotRemoteRuntime({
        config,
        databaseUrl,
        dependencies: {
          bucketProbe: {
            close() {
              closeCount += 1;
            },
            async headBucket(domain) {
              bucketDomains.push(domain);
              return {
                completedAt: "2026-08-31T00:05:01.000Z",
                outcome: "succeeded",
                receiptSha256: "7".repeat(64),
              };
            },
          },
          credentialFreeVerifier: {
            async verify() {
              throw new Error("preflight must not run final verification");
            },
          },
          fetchImpl,
          observeDelegated: async ({ expectedPriorCid, fetchImpl }) => {
            expect(expectedPriorCid).toBe(
              fixture.plan.targets.queryTable.priorCid,
            );
            if (!fetchImpl) throw new Error("delegated transport is missing");
            await fetchImpl(
              `https://delegated-ipfs.dev/routing/v1/ipns/${fixture.plan.targets.queryTable.ipnsNetworkKey}`,
            );
            return {
              endpointType: "ipfs_delegated_routing_v1",
              httpStatus: 200,
              latencyMs: 1,
              observedAt: "2026-08-31T00:05:04.000Z",
              observedCid: expectedPriorCid,
              outcome: "validated",
              requestCount: 1,
              responseBytes: 32,
              responseSha256: "8".repeat(64),
              schemaVersion: "candidate_signed_ipns_observation_v1",
              sequence: "1",
              ttlNanoseconds: "60000000000",
              validationResult: "valid_prior",
              validity: "2026-09-01T00:00:00.000000000Z",
            };
          },
        },
        plan: fixture.plan,
      });
      try {
        await runtime.readOnlyPreflight({
          continuationAuthorization: recorded,
        });
      } finally {
        await runtime.close();
      }
      expect(closeCount).toBe(1);
      expect(bucketDomains).toEqual(["query_table"]);
      expect(networkReads).toEqual([
        "open_data:filebase_gateway",
        "query_table:filebase_control",
        "query_table:filebase_gateway",
        "query_table:delegated_ipfs",
      ]);
      const durableRequests = await sql<
        {
          attempt_sequence: number;
          continuation_authorization_id: string | null;
          domain: string;
          operation_kind: string;
          outcome: string;
          resolver: string | null;
        }[]
      >`
        SELECT domain, operation_kind, resolver, attempt_sequence,
               continuation_authorization_id, outcome
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE plan_id = ${fixture.plan.planId}
          AND request_category = 'bucket_names_preflight'
        ORDER BY domain, operation_kind, resolver NULLS FIRST,
                 attempt_sequence
      `;
      expect(durableRequests).toHaveLength(9);
      expect(
        durableRequests.filter(
          (request) =>
            request.domain === "open_data" &&
            request.operation_kind === "public_resolve" &&
            request.resolver === "filebase_gateway",
        ),
      ).toEqual([
        expect.objectContaining({
          attempt_sequence: 1,
          continuation_authorization_id: null,
          outcome: "terminal_failure",
        }),
        expect.objectContaining({
          attempt_sequence: 2,
          continuation_authorization_id: recorded.authorizationId,
          outcome: "succeeded",
        }),
      ]);
      expect(
        durableRequests.filter((request) => request.domain === "query_table"),
      ).toHaveLength(4);
      const successfulAccounting = await sql<
        { category_requests: number; global_requests: number }[]
      >`
        SELECT
          (SELECT request_count
           FROM oracle_candidate_source_snapshot_demo_accounting
           WHERE plan_id = ${fixture.plan.planId}) AS global_requests,
          (SELECT consumed_request_count
           FROM oracle_candidate_source_snapshot_demo_request_categories
           WHERE plan_id = ${fixture.plan.planId}
             AND request_category = 'bucket_names_preflight')
            AS category_requests
      `;
      expect(successfulAccounting).toEqual([
        { category_requests: 9, global_requests: 9 },
      ]);
      const noUploads = await sql<{ uploads: number }[]>`
        SELECT count(*)::integer AS uploads
        FROM oracle_candidate_source_snapshot_demo_upload_attempts
        WHERE plan_id = ${fixture.plan.planId}
      `;
      expect(noUploads[0]?.uploads).toBe(0);
      await expect(
        admitCandidateSourceSnapshotPreflightRequest(databaseUrl, {
          ...openGatewayInput,
          attemptSequence: 3,
        }),
      ).rejects.toThrow("only its exact named observation");
      const complete = await sql<{ ready: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
          ${fixture.plan.planId}
        ) AS ready
      `;
      expect(complete[0]?.ready).toBe(true);
      await expect(
        beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
          approvalId: approved.approvalId,
          executorEnabled: true,
          implementationCommitSha: originalImplementationCommitSha,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).rejects.toThrow("lacks exact approval");
      await expect(
        beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
          approvalId: approved.approvalId,
          continuationAuthorizationId: recorded.authorizationId,
          executorEnabled: true,
          implementationCommitSha: originalImplementationCommitSha,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).rejects.toThrow("lacks exact approval");
      await expect(
        beginCandidateSourceSnapshotDemoExecution(databaseUrl, {
          approvalId: approved.approvalId,
          continuationAuthorizationId: recorded.authorizationId,
          executorEnabled: true,
          implementationCommitSha: amendedImplementationCommitSha,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
        }),
      ).resolves.toMatchObject({ state: "executing" });
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("admits one durable attempt-2 row for concurrent exact continuation replay", async () => {
    await withDisposableDatabase(
      "candidate_preflight_concurrent_resume",
      async (isolatedDatabaseUrl) => {
        const { continuation, fixture } =
          await prepareTerminalGatewayPreflight(isolatedDatabaseUrl);
        const input = {
          attemptSequence: 2,
          continuationAuthorizationId: continuation.authorizationId,
          domain: "open_data" as const,
          operationKind: "public_resolve" as const,
          planId: fixture.plan.planId,
          planSha256: fixture.plan.planSha256,
          redirectSequence: 0,
          resolver: "filebase_gateway" as const,
        };
        const beforeSql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          const before = await beforeSql<
            { category_requests: number; global_requests: number }[]
          >`
            SELECT
              (SELECT request_count
               FROM oracle_candidate_source_snapshot_demo_accounting
               WHERE plan_id = ${fixture.plan.planId}) AS global_requests,
              (SELECT consumed_request_count
               FROM oracle_candidate_source_snapshot_demo_request_categories
               WHERE plan_id = ${fixture.plan.planId}
                 AND request_category = 'bucket_names_preflight')
                AS category_requests
          `;
          expect(before).toEqual([
            { category_requests: 4, global_requests: 4 },
          ]);
        } finally {
          await beforeSql.end({ timeout: 5 });
        }
        const concurrent = await Promise.all([
          admitCandidateSourceSnapshotPreflightRequest(
            isolatedDatabaseUrl,
            input,
          ),
          admitCandidateSourceSnapshotPreflightRequest(
            isolatedDatabaseUrl,
            input,
          ),
        ]);
        expect(
          concurrent.map(({ alreadyRecorded }) => alreadyRecorded).sort(),
        ).toEqual([false, true]);
        expect(new Set(concurrent.map(({ requestId }) => requestId)).size).toBe(
          1,
        );
        const sql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          const rows = await sql<
            {
              continuation_authorization_id: string | null;
              request_count: number;
            }[]
          >`
            SELECT min(continuation_authorization_id) AS
                     continuation_authorization_id,
                   count(*)::integer AS request_count
            FROM oracle_candidate_source_snapshot_demo_requests
            WHERE plan_id = ${fixture.plan.planId}
              AND domain = 'open_data'
              AND operation_kind = 'public_resolve'
              AND resolver = 'filebase_gateway'
              AND attempt_sequence = 2
          `;
          expect(rows).toEqual([
            {
              continuation_authorization_id: continuation.authorizationId,
              request_count: 1,
            },
          ]);
          const after = await sql<
            { category_requests: number; global_requests: number }[]
          >`
            SELECT
              (SELECT request_count
               FROM oracle_candidate_source_snapshot_demo_accounting
               WHERE plan_id = ${fixture.plan.planId}) AS global_requests,
              (SELECT consumed_request_count
               FROM oracle_candidate_source_snapshot_demo_request_categories
               WHERE plan_id = ${fixture.plan.planId}
                 AND request_category = 'bucket_names_preflight')
                AS category_requests
          `;
          expect(after).toEqual([{ category_requests: 5, global_requests: 5 }]);
        } finally {
          await sql.end({ timeout: 5 });
        }
      },
    );
  });

  it("rejects independently mismatched continuation operation bindings", async () => {
    await withDisposableDatabase(
      "candidate_preflight_binding_negatives",
      async (isolatedDatabaseUrl) => {
        const { continuation, fixture } =
          await prepareTerminalGatewayPreflight(isolatedDatabaseUrl);
        const invalidBindings = [
          {
            domain: "query_table" as const,
            operationKind: "public_resolve" as const,
            resolver: "filebase_gateway" as const,
          },
          {
            domain: "open_data" as const,
            operationKind: "names_read" as const,
            resolver: "filebase_control" as const,
          },
          {
            domain: "open_data" as const,
            operationKind: "public_resolve" as const,
            resolver: "delegated_ipfs" as const,
          },
        ];
        for (const invalidBinding of invalidBindings) {
          await expect(
            admitCandidateSourceSnapshotPreflightRequest(isolatedDatabaseUrl, {
              attemptSequence: 2,
              continuationAuthorizationId: continuation.authorizationId,
              ...invalidBinding,
              planId: fixture.plan.planId,
              planSha256: fixture.plan.planSha256,
              redirectSequence: 0,
            }),
          ).rejects.toThrow(
            "continuation authorization is restricted to an exact authorized official-gateway observation",
          );
        }

        const sql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          await expect(
            sql.begin(async (transaction) => {
              await transaction`
                UPDATE oracle_candidate_source_snapshot_demo_accounting
                SET request_count = request_count + 1,
                    public_resolver_count = public_resolver_count + 1,
                    request_cost_usd = request_cost_usd + 0.0000045,
                    revision = revision + 1
                WHERE plan_id = ${fixture.plan.planId}
              `;
              await transaction`
                UPDATE oracle_candidate_source_snapshot_demo_request_categories
                SET consumed_request_count = consumed_request_count + 1,
                    request_cost_usd = request_cost_usd + 0.0000045,
                    revision = revision + 1
                WHERE plan_id = ${fixture.plan.planId}
                  AND request_category = 'bucket_names_preflight'
              `;
              await transaction`
                INSERT INTO oracle_candidate_source_snapshot_demo_requests (
                  request_id, plan_id, operation_class, operation_kind, domain,
                  resolver, request_cost_usd, outcome, request_category,
                  logical_request_id, attempt_sequence, redirect_sequence,
                  continuation_authorization_id
                )
                SELECT ${`snapshotdemorequest_${"f".repeat(32)}`}, plan_id,
                       'public_resolver', operation_kind, 'query_table',
                       resolver, request_cost_usd, 'request_started',
                       request_category, logical_request_id, 2, 0,
                       ${continuation.authorizationId}
                FROM oracle_candidate_source_snapshot_demo_requests
                WHERE request_id = ${continuation.authorizationBinding.failedReceipt.requestId}
              `;
            }),
          ).rejects.toThrow(
            "continuation request lacks its exact receipt and remaining allowance",
          );
          const unauthorizedAttempt = await sql<
            {
              category_requests: number;
              global_requests: number;
              request_count: number;
            }[]
          >`
            SELECT
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_demo_requests
               WHERE plan_id = ${fixture.plan.planId}
                 AND attempt_sequence = 2) AS request_count,
              (SELECT request_count
               FROM oracle_candidate_source_snapshot_demo_accounting
               WHERE plan_id = ${fixture.plan.planId}) AS global_requests,
              (SELECT consumed_request_count
               FROM oracle_candidate_source_snapshot_demo_request_categories
               WHERE plan_id = ${fixture.plan.planId}
                 AND request_category = 'bucket_names_preflight')
                AS category_requests
          `;
          expect(unauthorizedAttempt).toEqual([
            {
              category_requests: 4,
              global_requests: 4,
              request_count: 0,
            },
          ]);
        } finally {
          await sql.end({ timeout: 5 });
        }
      },
    );
  });

  it("does not auto-admit a third observation after an authorized transient outcome", async () => {
    await withDisposableDatabase(
      "candidate_preflight_transient_resume",
      async (isolatedDatabaseUrl) => {
        const { continuation, fixture } =
          await prepareTerminalGatewayPreflight(isolatedDatabaseUrl);
        const second = await admitCandidateSourceSnapshotPreflightRequest(
          isolatedDatabaseUrl,
          {
            attemptSequence: 2,
            continuationAuthorizationId: continuation.authorizationId,
            domain: "open_data",
            operationKind: "public_resolve",
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            redirectSequence: 0,
            resolver: "filebase_gateway",
          },
        );
        await recordCandidateSourceSnapshotPreflightRequestOutcome(
          isolatedDatabaseUrl,
          {
            admission: second,
            completedAt: "2026-08-31T01:05:00.000Z",
            outcome: "timeout_unknown",
            receiptSha256: "5".repeat(64),
          },
        );
        await expect(
          admitCandidateSourceSnapshotPreflightRequest(isolatedDatabaseUrl, {
            attemptSequence: 3,
            domain: "open_data",
            operationKind: "public_resolve",
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            redirectSequence: 0,
            resolver: "filebase_gateway",
          }),
        ).rejects.toThrow("only its exact named observation");

        const sql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          const accounting = await sql<
            { category_requests: number; global_requests: number }[]
          >`
            SELECT
              (SELECT request_count
               FROM oracle_candidate_source_snapshot_demo_accounting
               WHERE plan_id = ${fixture.plan.planId}) AS global_requests,
              (SELECT consumed_request_count
               FROM oracle_candidate_source_snapshot_demo_request_categories
               WHERE plan_id = ${fixture.plan.planId}
                 AND request_category = 'bucket_names_preflight')
                AS category_requests
          `;
          expect(accounting).toEqual([
            { category_requests: 5, global_requests: 5 },
          ]);
        } finally {
          await sql.end({ timeout: 5 });
        }
      },
    );
  });

  it("keeps a terminal authorized gateway observation immutable and blocks query work", async () => {
    await withDisposableDatabase(
      "candidate_preflight_terminal_resume",
      async (isolatedDatabaseUrl) => {
        const { continuation, fixture } =
          await prepareTerminalGatewayPreflight(isolatedDatabaseUrl);
        const config = loadCandidateSourceSnapshotExecutionConfig(
          enabledEnvironment(fixture.plan),
          fixture.plan,
        );
        if (!config.enabled) {
          throw new Error("synthetic executor was not enabled");
        }
        const requestedDomains: string[] = [];
        let closeCount = 0;
        const runtime = createCandidateSourceSnapshotRemoteRuntime({
          config,
          databaseUrl: isolatedDatabaseUrl,
          dependencies: {
            bucketProbe: {
              close() {
                closeCount += 1;
              },
              async headBucket(domain) {
                requestedDomains.push(`${domain}:bucket`);
                throw new Error("query bucket must remain blocked");
              },
            },
            credentialFreeVerifier: {
              async verify() {
                throw new Error("preflight must not run final verification");
              },
            },
            fetchImpl: async (request) => {
              const url = String(request);
              const domain = url.includes(
                fixture.plan.targets.openData.ipnsNetworkKey,
              )
                ? "open_data"
                : "query_table";
              requestedDomains.push(`${domain}:filebase_gateway`);
              return new Response(null, { status: 403 });
            },
            observeDelegated: async () => {
              throw new Error("delegated resolver must remain blocked");
            },
          },
          plan: fixture.plan,
        });
        try {
          await expect(
            runtime.readOnlyPreflight({
              continuationAuthorization: continuation,
            }),
          ).rejects.toThrow(
            "open_data filebase_gateway preflight did not verify its immutable prior",
          );
        } finally {
          await runtime.close();
        }
        expect(closeCount).toBe(1);
        expect(requestedDomains).toEqual(["open_data:filebase_gateway"]);

        const sql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          const attempts = await sql<
            {
              attempt_sequence: number;
              continuation_authorization_id: string | null;
              outcome: string;
              request_id: string;
            }[]
          >`
            SELECT request_id, attempt_sequence,
                   continuation_authorization_id, outcome
            FROM oracle_candidate_source_snapshot_demo_requests
            WHERE plan_id = ${fixture.plan.planId}
              AND domain = 'open_data'
              AND operation_kind = 'public_resolve'
              AND resolver = 'filebase_gateway'
            ORDER BY attempt_sequence
          `;
          expect(attempts).toHaveLength(2);
          expect(attempts[1]).toMatchObject({
            attempt_sequence: 2,
            continuation_authorization_id: continuation.authorizationId,
            outcome: "terminal_failure",
          });
          await expect(
            sql`
              UPDATE oracle_candidate_source_snapshot_demo_requests
              SET outcome = 'succeeded', receipt_sha256 = ${"9".repeat(64)}
              WHERE request_id = ${attempts[1]!.request_id}
            `,
          ).rejects.toThrow("immutable or terminal");
          const blocked = await sql<
            { query_requests: number; ready: boolean; uploads: number }[]
          >`
            SELECT
              oracle_candidate_source_snapshot_preflight_is_execution_ready(
                ${fixture.plan.planId}
              ) AS ready,
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_demo_requests
               WHERE plan_id = ${fixture.plan.planId}
                 AND domain = 'query_table') AS query_requests,
              (SELECT count(*)::integer
               FROM oracle_candidate_source_snapshot_demo_upload_attempts
               WHERE plan_id = ${fixture.plan.planId}) AS uploads
          `;
          expect(blocked).toEqual([
            { query_requests: 0, ready: false, uploads: 0 },
          ]);
        } finally {
          await sql.end({ timeout: 5 });
        }
      },
    );
  });

  it("accepts an exact attempt-3 chain and becomes ready after all eight keys succeed", async () => {
    await withDisposableDatabase(
      "candidate_preflight_attempt_three",
      async (isolatedDatabaseUrl) => {
        const { continuation, fixture } =
          await prepareTerminalGatewayPreflight(isolatedDatabaseUrl);
        const attemptTwo = await admitCandidateSourceSnapshotPreflightRequest(
          isolatedDatabaseUrl,
          {
            attemptSequence: 2,
            continuationAuthorizationId: continuation.authorizationId,
            domain: "open_data",
            operationKind: "public_resolve",
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            redirectSequence: 0,
            resolver: "filebase_gateway",
          },
        );
        await recordCandidateSourceSnapshotPreflightRequestOutcome(
          isolatedDatabaseUrl,
          {
            admission: attemptTwo,
            completedAt: "2026-08-31T01:05:00.000Z",
            outcome: "terminal_failure",
            receiptSha256: "5".repeat(64),
          },
        );
        const secondContinuation =
          await proposeCandidateSourceSnapshotPreflightContinuation(
            isolatedDatabaseUrl,
            {
              amendedImplementationCommitSha: "3".repeat(40),
              authorizedAt: "2026-08-31T01:06:00.000Z",
              authorizerReference: "synthetic-continuation-controller",
              failedRequestId: attemptTwo.requestId,
              planId: fixture.plan.planId,
              planSha256: fixture.plan.planSha256,
            },
          );
        const recordedSecond =
          await recordCandidateSourceSnapshotPreflightContinuation(
            isolatedDatabaseUrl,
            secondContinuation,
          );
        const attemptThree = await admitCandidateSourceSnapshotPreflightRequest(
          isolatedDatabaseUrl,
          {
            attemptSequence: 3,
            continuationAuthorizationId: recordedSecond.authorizationId,
            domain: "open_data",
            operationKind: "public_resolve",
            planId: fixture.plan.planId,
            planSha256: fixture.plan.planSha256,
            redirectSequence: 0,
            resolver: "filebase_gateway",
          },
        );
        await recordCandidateSourceSnapshotPreflightRequestOutcome(
          isolatedDatabaseUrl,
          {
            admission: attemptThree,
            completedAt: "2026-08-31T01:07:00.000Z",
            outcome: "succeeded",
            receiptSha256: "6".repeat(64),
          },
        );

        for (const [operationKind, resolver] of [
          ["bucket_head", null],
          ["names_read", "filebase_control"],
          ["public_resolve", "filebase_gateway"],
          ["public_resolve", "delegated_ipfs"],
        ] as const) {
          const admission = await admitCandidateSourceSnapshotPreflightRequest(
            isolatedDatabaseUrl,
            {
              attemptSequence: 1,
              domain: "query_table",
              operationKind,
              planId: fixture.plan.planId,
              planSha256: fixture.plan.planSha256,
              redirectSequence: 0,
              resolver,
            },
          );
          await recordCandidateSourceSnapshotPreflightRequestOutcome(
            isolatedDatabaseUrl,
            {
              admission,
              completedAt: "2026-08-31T01:08:00.000Z",
              outcome: "succeeded",
              receiptSha256: "7".repeat(64),
            },
          );
        }

        const sql = postgres(isolatedDatabaseUrl, { max: 1 });
        try {
          const result = await sql<{ ready: boolean }[]>`
            SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
              ${fixture.plan.planId}
            ) AS ready
          `;
          expect(result).toEqual([{ ready: true }]);
        } finally {
          await sql.end({ timeout: 5 });
        }
      },
    );
  });
});
