import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approvePublicationPlan,
  beginPublicationExecution,
  checkpointPublicationObjectUploaded,
  checkpointPublicationObjectVerified,
  completePublicationPlan,
  getPublicationState,
} from "../../src/db/publication-durability.js";
import {
  confirmIpnsPrior,
  recordIpnsIntents,
  recordIpnsMutationAttempt,
  recordIpnsMutationReceipt,
  recoverIpnsIntent,
  verifyIpnsTarget,
} from "../../src/db/ipns-intent.js";
import {
  loadPreparedPilot,
  recordRunStarted,
} from "../../src/db/pilot-repository.js";
import { runMigrations } from "../../src/db/migrations.js";
import type { PreparedPilot } from "../../src/domain/types.js";
import { buildPublicationDryRun } from "../../src/publication/dry-run.js";
import { validatePublicationPlan } from "../../src/publication/plan.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import { verifyPreparedInput } from "../../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parsePreparedPilot,
} from "../../src/workflow/schemas.js";
import {
  createSyntheticLifecycleSnapshot,
  syntheticLoaderIdempotencyKey,
  type SyntheticSnapshot,
} from "../helpers/durability.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `ipns_recovery_${process.pid}_${Date.now()}`;
const schemaDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
let dataDir: string;

const resolvedReceipt = {
  errorCode: null,
  httpStatus: 200,
  latencyMs: 1,
  outcome: "resolved" as const,
  providerRequestIdHash: null,
  responseBodyHash: null,
  responseBytes: 0,
  schemaVersion: "1.0.0" as const,
};

const targets = {
  credentialsAvailable: true,
  openData: {
    bucket: "synthetic-open-recovery",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-open-recovery",
    ipnsNetworkKey:
      "k51A23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
  queryTable: {
    bucket: "synthetic-query-recovery",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-query-recovery",
    ipnsNetworkKey:
      "k51B23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
} as const;

async function load(
  snapshot: SyntheticSnapshot,
  databaseUrl = schemaDatabaseUrl,
) {
  const verified = await verifyPreparedInput(
    dataDir,
    snapshot.reference,
    parsePreparedPilot,
    snapshot.snapshot.snapshotId,
  );
  return loadPreparedPilot(
    databaseUrl,
    snapshot.request,
    structuredClone(verified.prepared) as PreparedPilot,
    {
      idempotencyKey: syntheticLoaderIdempotencyKey(
        snapshot.request.workflowId,
        snapshot.reference.preparedInputId,
      ),
      preparedManifest: verified.manifest,
      preparedReference: verified.reference,
      requestSha256: countyIngestRequestSha256(snapshot.request),
      runId: snapshot.request.runId,
      snapshot: verified.snapshot,
    },
  );
}

beforeAll(async () => {
  dataDir = await mkdtemp(path.join(tmpdir(), "oracle-ipns-recovery-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(16);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
    await rm(dataDir, { force: true, recursive: true });
  }
});

describe("IPNS intent crash and third-CID recovery", () => {
  it("hard-stops the plan and rolls back the first domain in reverse order", async () => {
    const snapshot = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: Array.from(
        { length: 25 },
        (_, index) => `REC-${String(index + 1).padStart(2, "0")}`,
      ),
      label: "ipns-recovery-authority",
      observedAt: "2026-08-29T00:00:00.000Z",
    });
    await recordRunStarted(schemaDatabaseUrl, snapshot.request);
    await load(snapshot);
    const dryRun = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      runId: snapshot.request.runId,
      targets,
    });
    const plan = validatePublicationPlan(
      JSON.parse(
        await readFile(
          path.join(
            dataDir,
            dryRun.outputRoot,
            "publication-dry-run-plan.json",
          ),
          "utf8",
        ),
      ),
    );
    const identity = {
      county: "pasco" as const,
      planId: plan.planId,
      planSha256: plan.planSha256,
    };
    await approvePublicationPlan(schemaDatabaseUrl, {
      ...identity,
      approverReference: "synthetic_controller",
    });
    const priorOpen = await calculateIpfsCid("recovery-prior-open\n");
    const priorQuery = await calculateIpfsCid("recovery-prior-query\n");
    const observedAt = "2026-08-29T01:00:00.000Z";
    const evidence = (domain: "open_data" | "query_table", cid: string) => ({
      observations: [
        {
          endpointId: `filebase.${domain}`,
          observedAt,
          observedCid: cid,
          receipt: resolvedReceipt,
          resolverKind: "filebase_control_plane" as const,
        },
        {
          endpointId: `gateway.${domain}`,
          observedAt,
          observedCid: cid,
          receipt: resolvedReceipt,
          resolverKind: "public_gateway" as const,
        },
      ],
    });
    const intents = await recordIpnsIntents(schemaDatabaseUrl, {
      ...identity,
      targets: [
        {
          domain: "open_data",
          intendedAt: observedAt,
          priorCid: priorOpen,
          resolutionEvidence: evidence("open_data", priorOpen),
        },
        {
          domain: "query_table",
          intendedAt: observedAt,
          priorCid: priorQuery,
          resolutionEvidence: evidence("query_table", priorQuery),
        },
      ],
    });
    await beginPublicationExecution({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      publicationRootRelative: dryRun.outputRoot,
      request: identity,
    });
    for (const artifact of plan.artifacts.objectInventory) {
      const checkpoint = {
        ...identity,
        cid: artifact.expectedCid,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
        sha256: artifact.sha256,
      };
      await checkpointPublicationObjectUploaded(schemaDatabaseUrl, checkpoint);
      await checkpointPublicationObjectVerified(schemaDatabaseUrl, checkpoint);
    }
    const openIntent = intents.find((intent) => intent.domain === "open_data")!;
    const queryIntent = intents.find(
      (intent) => intent.domain === "query_table",
    )!;
    const observations = (
      domain: "open_data" | "query_table",
      cid: string,
      cycleObservedAt: string,
    ) => [
      {
        endpointId: `filebase.${domain}`,
        observedAt: cycleObservedAt,
        observedCid: cid,
        receipt: resolvedReceipt,
        resolverKind: "filebase_control_plane" as const,
      },
      {
        endpointId: `gateway.${domain}`,
        observedAt: cycleObservedAt,
        observedCid: cid,
        receipt: resolvedReceipt,
        resolverKind: "public_gateway" as const,
      },
    ];

    await recoverIpnsIntent(schemaDatabaseUrl, {
      attemptId: null,
      intentId: openIntent.intentId,
      observations: [
        observations("open_data", priorOpen, "2026-08-29T01:01:00.000Z")[0],
        observations(
          "open_data",
          plan.graph.openDataRoot.expectedCid,
          "2026-08-29T01:01:00.000Z",
        )[1],
      ],
      recoverySequence: 1,
    });
    await recoverIpnsIntent(schemaDatabaseUrl, {
      attemptId: null,
      intentId: openIntent.intentId,
      observations: [
        {
          classification: "unavailable",
          endpointId: "filebase.open_data",
          observedAt: "2026-08-29T01:02:00.000Z",
          receipt: {
            ...resolvedReceipt,
            errorCode: "provider_unavailable" as const,
            httpStatus: null,
            outcome: "unavailable" as const,
          },
          resolverKind: "filebase_control_plane",
        },
        {
          classification: "error",
          endpointId: "gateway.open_data",
          observedAt: "2026-08-29T01:02:00.000Z",
          receipt: {
            ...resolvedReceipt,
            errorCode: "http_error" as const,
            httpStatus: 503,
            outcome: "http_error" as const,
          },
          resolverKind: "public_gateway",
        },
      ],
      recoverySequence: 2,
    });
    await recoverIpnsIntent(schemaDatabaseUrl, {
      attemptId: null,
      intentId: openIntent.intentId,
      observations: observations(
        "open_data",
        priorOpen,
        "2026-08-29T01:03:00.000Z",
      ),
      recoverySequence: 3,
    });
    const openAttempt = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "update",
      intentId: openIntent.intentId,
      requestSha256: "1".repeat(64),
    });
    // Simulates a crash after the remote mutation but before a provider receipt
    // was checkpointed: recovery observes the approved target and never repeats.
    await recoverIpnsIntent(schemaDatabaseUrl, {
      attemptId: openAttempt.attemptId,
      intentId: openIntent.intentId,
      observations: observations(
        "open_data",
        plan.graph.openDataRoot.expectedCid,
        "2026-08-29T01:04:00.000Z",
      ),
      recoverySequence: 4,
    });
    await verifyIpnsTarget(schemaDatabaseUrl, openIntent.intentId);

    await confirmIpnsPrior(schemaDatabaseUrl, queryIntent.intentId);
    const queryAttempt = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "update",
      intentId: queryIntent.intentId,
      requestSha256: "2".repeat(64),
    });
    const thirdCid = await calculateIpfsCid("unexpected-third-recovery\n");
    const thirdObservations = observations(
      "query_table",
      thirdCid,
      "2026-08-29T01:05:00.000Z",
    );
    const thirdRecovery = {
      attemptId: queryAttempt.attemptId,
      intentId: queryIntent.intentId,
      observations: thirdObservations,
      recoverySequence: 1,
    };
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, thirdRecovery),
    ).resolves.toMatchObject({ state: "manual_intervention_required" });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, thirdRecovery),
    ).resolves.toMatchObject({ state: "manual_intervention_required" });
    expect(await getPublicationState(schemaDatabaseUrl)).toMatchObject({
      state: "manual_intervention_required",
    });
    await expect(
      completePublicationPlan(schemaDatabaseUrl, identity),
    ).rejects.toThrow("exactly two verified intent-ledger resolutions");

    const stateSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const states = await stateSql<
        {
          domain: string;
          prior_cid: string;
          state: string;
        }[]
      >`
        SELECT intent.domain, intent.prior_cid, state.state
        FROM oracle_publication_ipns_intents intent
        JOIN oracle_publication_ipns_intent_state state USING (intent_id)
        WHERE intent.publication_plan_id = ${plan.planId}
        ORDER BY intent.domain
      `;
      expect(states).toEqual([
        {
          domain: "open_data",
          prior_cid: priorOpen,
          state: "rollback_requested",
        },
        {
          domain: "query_table",
          prior_cid: priorQuery,
          state: "manual_intervention_required",
        },
      ]);
    } finally {
      await stateSql.end({ timeout: 5 });
    }

    const rollbackAttempt = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "rollback",
      intentId: openIntent.intentId,
      requestSha256: "3".repeat(64),
    });
    await recordIpnsMutationReceipt(schemaDatabaseUrl, {
      attemptId: rollbackAttempt.attemptId,
      intentId: openIntent.intentId,
      outcome: "acknowledged",
      providerReceiptSha256: "4".repeat(64),
    });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: rollbackAttempt.attemptId,
        intentId: openIntent.intentId,
        observations: observations(
          "open_data",
          priorOpen,
          "2026-08-29T01:06:00.000Z",
        ),
        recoverySequence: 5,
      }),
    ).resolves.toMatchObject({
      priorCid: priorOpen,
      state: "rollback_verified",
    });
    const immutableSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const reconstructed = await immutableSql<
        {
          classification: string;
          count_matches: boolean;
          hash_matches: boolean;
          reconstructed: boolean;
          safe: boolean;
        }[]
      >`
        SELECT cycle.classification,
               cycle.classification = oracle_classify_ipns_observations(
                 cycle.intent_id, cycle.observations_canonical
               ) AS reconstructed,
               cycle.observation_count =
                 jsonb_array_length(cycle.observations_canonical::jsonb)
                 AS count_matches,
               cycle.evidence_sha256 = encode(
                 sha256(convert_to(cycle.observations_canonical, 'UTF8')),
                 'hex'
               ) AS hash_matches,
               cycle.observations_canonical !~*
                 '(authorization|cookie|credential|private.?key|token)'
                 AS safe
        FROM oracle_publication_ipns_resolution_cycles cycle
        WHERE cycle.intent_id IN (${openIntent.intentId}, ${queryIntent.intentId})
        ORDER BY cycle.resolution_cycle_id
      `;
      expect(new Set(reconstructed.map((row) => row.classification))).toEqual(
        new Set([
          "prior_observed",
          "target_observed",
          "split_prior_target",
          "timeout_transport_uncertainty",
          "unexpected_third_cid",
        ]),
      );
      expect(
        reconstructed.every(
          (row) =>
            row.reconstructed &&
            row.count_matches &&
            row.hash_matches &&
            row.safe,
        ),
      ).toBe(true);
      const mutations = [
        () => immutableSql`
          UPDATE oracle_publication_ipns_intent_events
          SET metadata = metadata
          WHERE intent_id = ${openIntent.intentId}
        `,
        () => immutableSql`
          DELETE FROM oracle_publication_ipns_mutation_attempts
          WHERE attempt_id = ${rollbackAttempt.attemptId}
        `,
        () => immutableSql`
          UPDATE oracle_publication_ipns_mutation_receipts
          SET provider_receipt_sha256 = ${"5".repeat(64)}
          WHERE attempt_id = ${rollbackAttempt.attemptId}
        `,
        () => immutableSql`
          DELETE FROM oracle_publication_ipns_resolution_cycles
          WHERE intent_id = ${openIntent.intentId}
        `,
      ];
      for (const mutation of mutations) {
        await expect(mutation()).rejects.toThrow("immutable");
      }
    } finally {
      await immutableSql.end({ timeout: 5 });
    }
    await expect(
      completePublicationPlan(schemaDatabaseUrl, identity),
    ).rejects.toThrow("exactly two verified intent-ledger resolutions");
  }, 120_000);

  it("requests reverse-order rollback after a terminal second-domain failure", async () => {
    const failureSchema = `ipns_second_failure_${process.pid}_${Date.now()}`;
    const failureDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${failureSchema}`;
    const admin = postgres(adminDatabaseUrl, { max: 1 });
    try {
      await admin.unsafe(`CREATE SCHEMA ${failureSchema}`);
    } finally {
      await admin.end({ timeout: 5 });
    }
    try {
      await runMigrations(failureDatabaseUrl);
      const snapshot = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "authoritative",
        folios: Array.from(
          { length: 25 },
          (_, index) => `FAIL-${String(index + 1).padStart(2, "0")}`,
        ),
        label: "ipns-second-domain-failure",
        observedAt: "2026-08-29T02:00:00.000Z",
      });
      await recordRunStarted(failureDatabaseUrl, snapshot.request);
      await load(snapshot, failureDatabaseUrl);
      const dryRun = await buildPublicationDryRun({
        dataDir,
        databaseUrl: failureDatabaseUrl,
        exportMode: "authoritative",
        runId: snapshot.request.runId,
        targets,
      });
      const plan = validatePublicationPlan(
        JSON.parse(
          await readFile(
            path.join(
              dataDir,
              dryRun.outputRoot,
              "publication-dry-run-plan.json",
            ),
            "utf8",
          ),
        ),
      );
      const identity = {
        county: "pasco" as const,
        planId: plan.planId,
        planSha256: plan.planSha256,
      };
      await approvePublicationPlan(failureDatabaseUrl, {
        ...identity,
        approverReference: "synthetic_controller",
      });
      const priorOpen = await calculateIpfsCid("failure-prior-open\n");
      const priorQuery = await calculateIpfsCid("failure-prior-query\n");
      const observedAt = "2026-08-29T03:00:00.000Z";
      const initialEvidence = (
        domain: "open_data" | "query_table",
        cid: string,
      ) => ({
        observations: [
          {
            endpointId: `filebase.${domain}`,
            observedAt,
            observedCid: cid,
            receipt: resolvedReceipt,
            resolverKind: "filebase_control_plane" as const,
          },
          {
            endpointId: `gateway.${domain}`,
            observedAt,
            observedCid: cid,
            receipt: resolvedReceipt,
            resolverKind: "public_gateway" as const,
          },
        ],
      });
      const intents = await recordIpnsIntents(failureDatabaseUrl, {
        ...identity,
        targets: [
          {
            domain: "open_data",
            intendedAt: observedAt,
            priorCid: priorOpen,
            resolutionEvidence: initialEvidence("open_data", priorOpen),
          },
          {
            domain: "query_table",
            intendedAt: observedAt,
            priorCid: priorQuery,
            resolutionEvidence: initialEvidence("query_table", priorQuery),
          },
        ],
      });
      await beginPublicationExecution({
        dataDir,
        databaseUrl: failureDatabaseUrl,
        publicationRootRelative: dryRun.outputRoot,
        request: identity,
      });
      for (const artifact of plan.artifacts.objectInventory) {
        const checkpoint = {
          ...identity,
          cid: artifact.expectedCid,
          domain: artifact.domain,
          objectKey: artifact.objectKey,
          sha256: artifact.sha256,
        };
        await checkpointPublicationObjectUploaded(
          failureDatabaseUrl,
          checkpoint,
        );
        await checkpointPublicationObjectVerified(
          failureDatabaseUrl,
          checkpoint,
        );
      }
      const openIntent = intents.find(
        (intent) => intent.domain === "open_data",
      )!;
      const queryIntent = intents.find(
        (intent) => intent.domain === "query_table",
      )!;
      await confirmIpnsPrior(failureDatabaseUrl, openIntent.intentId);
      const openAttempt = await recordIpnsMutationAttempt(failureDatabaseUrl, {
        direction: "update",
        intentId: openIntent.intentId,
        requestSha256: "6".repeat(64),
      });
      await recordIpnsMutationReceipt(failureDatabaseUrl, {
        attemptId: openAttempt.attemptId,
        intentId: openIntent.intentId,
        outcome: "acknowledged",
        providerReceiptSha256: "7".repeat(64),
      });
      await recoverIpnsIntent(failureDatabaseUrl, {
        attemptId: openAttempt.attemptId,
        intentId: openIntent.intentId,
        observations: [
          {
            endpointId: "filebase.open_data",
            observedAt: "2026-08-29T03:01:00.000Z",
            observedCid: plan.graph.openDataRoot.expectedCid,
            receipt: resolvedReceipt,
            resolverKind: "filebase_control_plane",
          },
          {
            endpointId: "gateway.open_data",
            observedAt: "2026-08-29T03:01:00.000Z",
            observedCid: plan.graph.openDataRoot.expectedCid,
            receipt: resolvedReceipt,
            resolverKind: "public_gateway",
          },
        ],
        recoverySequence: 1,
      });
      await verifyIpnsTarget(failureDatabaseUrl, openIntent.intentId);

      await confirmIpnsPrior(failureDatabaseUrl, queryIntent.intentId);
      const queryAttempt = await recordIpnsMutationAttempt(failureDatabaseUrl, {
        direction: "update",
        intentId: queryIntent.intentId,
        requestSha256: "8".repeat(64),
      });
      await expect(
        recordIpnsMutationReceipt(failureDatabaseUrl, {
          attemptId: queryAttempt.attemptId,
          intentId: queryIntent.intentId,
          outcome: "failed",
          providerReceiptSha256: "9".repeat(64),
        }),
      ).resolves.toMatchObject({ state: "failed_terminal" });
      const sql = postgres(failureDatabaseUrl, { max: 1 });
      try {
        const states = await sql<{ domain: string; state: string }[]>`
          SELECT intent.domain, state.state
          FROM oracle_publication_ipns_intents intent
          JOIN oracle_publication_ipns_intent_state state USING (intent_id)
          ORDER BY intent.domain
        `;
        expect(states).toEqual([
          { domain: "open_data", state: "rollback_requested" },
          { domain: "query_table", state: "failed_terminal" },
        ]);
      } finally {
        await sql.end({ timeout: 5 });
      }
      await expect(
        completePublicationPlan(failureDatabaseUrl, identity),
      ).rejects.toThrow("exactly two verified intent-ledger resolutions");
    } finally {
      const cleanup = postgres(adminDatabaseUrl, { max: 1 });
      try {
        await cleanup.unsafe(`DROP SCHEMA ${failureSchema} CASCADE`);
      } finally {
        await cleanup.end({ timeout: 5 });
      }
    }
  }, 120_000);
});
