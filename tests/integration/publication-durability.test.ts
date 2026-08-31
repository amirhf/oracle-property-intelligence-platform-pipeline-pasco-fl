import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  approvePublicationPlan,
  beginPublicationExecution,
  checkpointPublicationIpnsUpdated,
  checkpointPublicationObjectUploaded,
  checkpointPublicationObjectVerified,
  completePublicationPlan,
  getPublicationState,
  publicationIpnsUpdateReady,
  recordPublicationPlan,
  verifyPublicationIpnsResolution,
} from "../../src/db/publication-durability.js";
import {
  confirmIpnsPrior,
  recordIpnsAmbiguousResult,
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
import { DurableConflictError } from "../../src/lib/durability-errors.js";
import { buildPublicationDryRun } from "../../src/publication/dry-run.js";
import {
  createPublicationPlan,
  validatePublicationPlan,
  type PublicationPlan,
} from "../../src/publication/plan.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import { createRemotePublicationExecutor } from "../../src/publication/remote-boundary.js";
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
const schemaName = `publication_${process.pid}_${Date.now()}`;
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

const range = (start: number, end: number) =>
  Array.from(
    { length: end - start + 1 },
    (_, index) => `PUB-${(start + index).toString().padStart(2, "0")}`,
  );

const targets = {
  credentialsAvailable: true,
  openData: {
    bucket: "synthetic-open-data-test",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-open-data-test",
    ipnsNetworkKey:
      "k51A23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
  queryTable: {
    bucket: "synthetic-query-table-test",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-query-table-test",
    ipnsNetworkKey:
      "k51B23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
} as const;

async function load(
  snapshot: SyntheticSnapshot,
  mutatePrepared?: (prepared: PreparedPilot) => void,
) {
  const verified = await verifyPreparedInput(
    dataDir,
    snapshot.reference,
    parsePreparedPilot,
    snapshot.snapshot.snapshotId,
  );
  const prepared = structuredClone(verified.prepared);
  mutatePrepared?.(prepared);
  return loadPreparedPilot(schemaDatabaseUrl, snapshot.request, prepared, {
    idempotencyKey: syntheticLoaderIdempotencyKey(
      snapshot.request.workflowId,
      snapshot.reference.preparedInputId,
    ),
    preparedManifest: verified.manifest,
    preparedReference: verified.reference,
    requestSha256: countyIngestRequestSha256(snapshot.request),
    runId: snapshot.request.runId,
    snapshot: verified.snapshot,
  });
}

async function planAt(publicationRoot: string): Promise<PublicationPlan> {
  return validatePublicationPlan(
    JSON.parse(
      await readFile(
        path.join(dataDir, publicationRoot, "publication-dry-run-plan.json"),
        "utf8",
      ),
    ),
  );
}

beforeAll(async () => {
  dataDir = await mkdtemp(path.join(tmpdir(), "prism-publication-test-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(31);
  expect(await runMigrations(schemaDatabaseUrl)).toEqual([]);
}, 30_000);

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
    await rm(dataDir, { force: true, recursive: true });
  }
});

describe("lifecycle-aware durable publication", () => {
  it("exports only the current authoritative head and binds approval to exact bytes", async () => {
    const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: range(1, 25),
      label: "publication-authority-a",
      observedAt: "2026-08-29T00:00:00.000Z",
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotA.request);
    await load(snapshotA);
    let releaseInjectedRecorder: (() => void) | undefined;
    const adopted = new Promise<void>((resolve) => {
      releaseInjectedRecorder = resolve;
    });
    const buildOptions = {
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative" as const,
      generatedAt: "2026-08-29T01:00:00.000Z",
      runId: snapshotA.request.runId,
      targets,
    };
    const contenders = await Promise.allSettled([
      buildPublicationDryRun({
        ...buildOptions,
        publicationPlanRecorder: async (databaseUrl, plan) => {
          releaseInjectedRecorder?.();
          return recordPublicationPlan(databaseUrl, plan);
        },
      }),
      buildPublicationDryRun({
        ...buildOptions,
        publicationPlanRecorder: async () => {
          await adopted;
          throw new Error("synthetic database recording failure");
        },
      }),
    ]);
    const fulfilled = contenders.find(
      (
        result,
      ): result is PromiseFulfilledResult<
        Awaited<ReturnType<typeof buildPublicationDryRun>>
      > => result.status === "fulfilled",
    );
    expect(fulfilled).toBeDefined();
    expect(
      contenders.filter((result) => result.status === "rejected"),
    ).toHaveLength(1);
    const exportA = fulfilled!.value;
    const publishBase = path.join(dataDir, "artifacts", "publish", "pasco");
    expect(
      (await readdir(publishBase)).filter((entry) =>
        entry.startsWith(".build-"),
      ),
    ).toEqual([]);
    expect(
      await readFile(
        path.join(dataDir, exportA.outputRoot, "publication-dry-run-plan.json"),
        "utf8",
      ),
    ).toContain(exportA.planId);
    expect(exportA).toMatchObject({
      activeProperties: 25,
      coverageMode: "authoritative_complete",
      inactiveProperties: 0,
      propertyCount: 25,
      publicationState: { state: "awaiting_approval" },
    });

    const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["PUB-02"],
      coverage: "authoritative",
      folios: [...range(1, 24), "PUB-26"],
      label: "publication-authority-b",
      observedAt: "2026-08-29T01:00:00.000Z",
      previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotB.request);
    await load(snapshotB);
    const exportB = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      generatedAt: "2026-08-29T02:00:00.000Z",
      runId: snapshotB.request.runId,
      targets,
    });
    const planB = await planAt(exportB.outputRoot);
    expect(
      validatePublicationPlan({
        ...planB,
        generatedAt: "2030-01-01T00:00:00.000Z",
      }),
    ).toMatchObject({
      planId: planB.planId,
      planSha256: planB.planSha256,
    });
    expect(() =>
      validatePublicationPlan({
        ...planB,
        graph: { ...planB.graph, edges: planB.graph.edges.slice(1) },
      }),
    ).toThrow("publication graph/root binding is incomplete");
    expect(
      await recordPublicationPlan(schemaDatabaseUrl, {
        ...planB,
        generatedAt: "2030-01-01T00:00:00.000Z",
      }),
    ).toMatchObject({
      planId: planB.planId,
      state: "awaiting_approval",
    });
    expect(() =>
      validatePublicationPlan({ ...planB, unexpected: true }),
    ).toThrow("strict validation");
    expect(exportB).toMatchObject({
      activeProperties: 25,
      coverageMode: "authoritative_complete",
      inactiveProperties: 1,
      propertyCount: 25,
    });
    expect(planB.coverage.authoritativeHeadSnapshotId).toBe(
      snapshotB.snapshot.snapshotId,
    );
    const sealedSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      for (const mutation of [
        sealedSql`UPDATE oracle_projection_snapshots SET scope_id = ${`scope_${"f".repeat(32)}`} WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_snapshots SET predecessor_snapshot_id = null WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_snapshots SET authority_source_identifier = 'changed' WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_snapshots SET watermark_observed_through = watermark_observed_through + interval '1 second' WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_snapshots SET content_sha256 = ${"f".repeat(64)} WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_snapshots SET coverage_mode = 'partial' WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`DELETE FROM oracle_projection_snapshots WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_projection_materializations SET active_count = active_count + 1 WHERE snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_property_versions SET payload = '{}'::jsonb WHERE source_snapshot_id = ${snapshotB.snapshot.snapshotId}`,
        sealedSql`UPDATE oracle_child_fact_versions SET payload = '{}'::jsonb WHERE source_snapshot_id = ${snapshotB.snapshot.snapshotId}`,
      ]) {
        await expect(mutation).rejects.toThrow(/sealed|immutable/);
      }
    } finally {
      await sealedSql.end({ timeout: 5 });
    }
    const manifest = JSON.parse(
      await readFile(
        path.join(dataDir, exportB.outputRoot, "open-data", "manifest.json"),
        "utf8",
      ),
    ) as { entries: { propertyId: string }[] };
    expect(
      manifest.entries.some(
        (entry) =>
          entry.propertyId === snapshotA.prepared.properties.at(-1)!.propertyId,
      ),
    ).toBe(false);

    await expect(
      buildPublicationDryRun({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        exportMode: "authoritative",
        runId: snapshotA.request.runId,
        targets,
      }),
    ).rejects.toThrow("exact current scope head");

    const sample = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "sample",
      folios: range(31, 55),
      label: "publication-bounded-sample",
    });
    await recordRunStarted(schemaDatabaseUrl, sample.request);
    await load(sample);
    await expect(
      buildPublicationDryRun({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        exportMode: "authoritative",
        runId: sample.request.runId,
        targets,
      }),
    ).rejects.toThrow("cannot request authoritative publication");
    await expect(
      buildPublicationDryRun({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        exportMode: "bounded",
        runId: sample.request.runId,
        targets,
      }),
    ).rejects.toThrow("Bounded v1.2 snapshot publication is unsupported");

    const executionRequest = {
      county: "pasco",
      planId: planB.planId,
      planSha256: planB.planSha256,
    };
    await expect(
      beginPublicationExecution({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        publicationRootRelative: exportB.outputRoot,
        request: executionRequest,
      }),
    ).rejects.toThrow("requires exact current-plan approval");

    const approvalRequest = {
      ...executionRequest,
      approverReference: "synthetic_controller",
    };
    const approved = await approvePublicationPlan(
      schemaDatabaseUrl,
      approvalRequest,
    );
    expect(approved.state).toBe("approved");
    expect(
      await approvePublicationPlan(schemaDatabaseUrl, approvalRequest),
    ).toEqual(approved);
    const identitySql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      for (const column of [
        "plan_id",
        "plan_sha256",
        "plan_version",
        "county",
        "run_id",
        "snapshot_id",
        "coverage_mode",
        "scope_id",
        "approvable",
        "executable",
        "plan_payload",
        "generated_at",
        "recorded_at",
      ]) {
        await expect(
          identitySql.unsafe(
            `UPDATE oracle_publication_plans SET ${column} = ${column} WHERE plan_id = $1`,
            [planB.planId],
          ),
        ).rejects.toThrow("immutable");
      }
      await expect(
        identitySql`DELETE FROM oracle_publication_plans WHERE plan_id = ${planB.planId}`,
      ).rejects.toThrow("immutable");

      for (const column of [
        "plan_id",
        "approval_id",
        "plan_sha256",
        "request_sha256",
        "approver_reference",
        "approved_at",
        "validated_scope_id",
        "validated_snapshot_id",
        "validated_authoritative_base_snapshot_id",
        "validated_materialization_id",
        "validated_materialization_sha256",
        "validated_head_revision",
        "approval_revision",
      ]) {
        await expect(
          identitySql.unsafe(
            `UPDATE oracle_publication_approvals SET ${column} = ${column} WHERE plan_id = $1`,
            [planB.planId],
          ),
        ).rejects.toThrow("immutable");
      }
      await expect(
        identitySql`DELETE FROM oracle_publication_approvals WHERE plan_id = ${planB.planId}`,
      ).rejects.toThrow("immutable");

      for (const [table, column] of [
        ["oracle_publication_graph_objects", "expected_cid"],
        ["oracle_publication_graph_edges", "child_cid"],
        ["oracle_publication_graph_roots", "expected_cid"],
      ] as const) {
        await expect(
          identitySql.unsafe(
            `UPDATE ${table} SET ${column} = ${column} WHERE plan_id = $1`,
            [planB.planId],
          ),
        ).rejects.toThrow("immutable");
        await expect(
          identitySql.unsafe(`DELETE FROM ${table} WHERE plan_id = $1`, [
            planB.planId,
          ]),
        ).rejects.toThrow("immutable");
      }
    } finally {
      await identitySql.end({ timeout: 5 });
    }
    await expect(
      approvePublicationPlan(schemaDatabaseUrl, {
        ...approvalRequest,
        approverReference: "conflicting_controller",
      }),
    ).rejects.toBeInstanceOf(DurableConflictError);

    const priorOpen = await calculateIpfsCid("synthetic-prior-open\n");
    const priorQuery = await calculateIpfsCid("synthetic-prior-query\n");
    const observedAt = "2026-08-29T03:00:00.000Z";
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
    const intentRequest = {
      county: "pasco" as const,
      planId: planB.planId,
      planSha256: planB.planSha256,
      targets: [
        {
          domain: "open_data" as const,
          intendedAt: observedAt,
          priorCid: priorOpen,
          resolutionEvidence: evidence("open_data", priorOpen),
        },
        {
          domain: "query_table" as const,
          intendedAt: observedAt,
          priorCid: priorQuery,
          resolutionEvidence: evidence("query_table", priorQuery),
        },
      ] as const,
    };
    const intents = await recordIpnsIntents(schemaDatabaseUrl, intentRequest);
    expect(intents).toHaveLength(2);
    expect(await recordIpnsIntents(schemaDatabaseUrl, intentRequest)).toEqual(
      intents,
    );
    for (const [field, assertion] of [
      ["providerTargetIdentity", "filebase:caller-controlled"],
      ["providerBucket", "caller-controlled"],
      ["ipnsLabel", "caller-controlled"],
      ["ipnsNetworkKey", targets.queryTable.ipnsNetworkKey],
      ["approvedTargetCid", planB.graph.queryTableRoot.expectedCid],
    ] as const) {
      await expect(
        recordIpnsIntents(schemaDatabaseUrl, {
          ...intentRequest,
          targets: [
            { ...intentRequest.targets[0], [field]: assertion },
            intentRequest.targets[1],
          ],
        }),
      ).rejects.toThrow("strict validation");
    }
    await expect(
      recordIpnsIntents(schemaDatabaseUrl, {
        ...intentRequest,
        targets: [
          { ...intentRequest.targets[0], domain: "query_table" },
          intentRequest.targets[1],
        ],
      }),
    ).rejects.toThrow("open_data then query_table");
    const intentSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const beforeMutation = await intentSql<
        { attempts: number; intents: number }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_publication_ipns_intents
           WHERE publication_plan_id = ${planB.planId}) AS intents,
          (SELECT count(*)::int FROM oracle_publication_ipns_mutation_attempts) AS attempts
      `;
      expect(beforeMutation[0]).toEqual({ attempts: 0, intents: 2 });
    } finally {
      await intentSql.end({ timeout: 5 });
    }

    const {
      planId: _planId,
      planSha256: _planSha256,
      ...planWithoutIdentity
    } = planB;
    const replacement = createPublicationPlan({
      ...planWithoutIdentity,
      targets: {
        ...planWithoutIdentity.targets,
        openData: {
          ...planWithoutIdentity.targets.openData,
          bucket: "different-approved-target",
        },
      },
    });
    await expect(
      recordPublicationPlan(schemaDatabaseUrl, replacement),
    ).rejects.toThrow("replacement blocked while approved");

    const indexPath = path.join(
      dataDir,
      exportB.outputRoot,
      "open-data",
      "index.json",
    );
    const indexBytes = await readFile(indexPath);
    await writeFile(indexPath, Buffer.concat([indexBytes, Buffer.from("x")]));
    await expect(
      beginPublicationExecution({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        publicationRootRelative: exportB.outputRoot,
        request: executionRequest,
      }),
    ).rejects.toThrow("artifact binding changed");
    await writeFile(indexPath, indexBytes);
    expect(
      await beginPublicationExecution({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        publicationRootRelative: exportB.outputRoot,
        request: executionRequest,
      }),
    ).toMatchObject({ state: "executing" });
    await expect(
      recordPublicationPlan(schemaDatabaseUrl, replacement),
    ).rejects.toThrow("replacement blocked while executing");

    const cids = new Map<string, string>();
    for (const artifact of planB.artifacts.objectInventory) {
      const cid = artifact.expectedCid;
      cids.set(`${artifact.domain}:${artifact.objectKey}`, cid);
      const checkpoint = {
        ...executionRequest,
        cid,
        domain: artifact.domain,
        objectKey: artifact.objectKey,
        sha256: artifact.sha256,
      };
      await checkpointPublicationObjectUploaded(schemaDatabaseUrl, checkpoint);
      await checkpointPublicationObjectVerified(schemaDatabaseUrl, checkpoint);
    }
    const completionGuardSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      await expect(
        completionGuardSql`
          UPDATE oracle_publication_state
          SET state = 'completed', revision = revision + 1
          WHERE county = 'pasco'
        `,
      ).rejects.toThrow("two exact verified intents and effects");
    } finally {
      await completionGuardSql.end({ timeout: 5 });
    }

    let resolutionSequence = 1;
    let resolverObservationSequence = 0;
    const observations = (
      domain: "open_data" | "query_table",
      cids: readonly [string, string],
    ) => {
      resolverObservationSequence += 1;
      const cycleObservedAt = new Date(
        Date.parse(observedAt) + resolverObservationSequence * 1_000,
      ).toISOString();
      return [
        {
          endpointId: `filebase.${domain}`,
          observedAt: cycleObservedAt,
          observedCid: cids[0],
          receipt: resolvedReceipt,
          resolverKind: "filebase_control_plane" as const,
        },
        {
          endpointId: `gateway.${domain}`,
          observedAt: cycleObservedAt,
          observedCid: cids[1],
          receipt: resolvedReceipt,
          resolverKind: "public_gateway" as const,
        },
      ];
    };
    const openIntent = intents.find((intent) => intent.domain === "open_data")!;
    const queryIntent = intents.find(
      (intent) => intent.domain === "query_table",
    )!;
    await expect(
      confirmIpnsPrior(schemaDatabaseUrl, openIntent.intentId),
    ).resolves.toMatchObject({ state: "prior_confirmed" });
    const openAttempt = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "update",
      intentId: openIntent.intentId,
      requestSha256: "1".repeat(64),
    });
    await expect(
      recordIpnsAmbiguousResult(schemaDatabaseUrl, {
        intentId: openIntent.intentId,
        reason: "timeout",
      }),
    ).resolves.toMatchObject({ state: "update_ambiguous" });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: openAttempt.attemptId,
        intentId: openIntent.intentId,
        observations: observations("open_data", [
          planB.graph.openDataRoot.expectedCid,
          planB.graph.openDataRoot.expectedCid,
        ]),
        recoverySequence: resolutionSequence++,
      }),
    ).resolves.toMatchObject({ state: "target_observed" });
    await expect(
      verifyIpnsTarget(schemaDatabaseUrl, openIntent.intentId),
    ).resolves.toMatchObject({ state: "verified" });

    await confirmIpnsPrior(schemaDatabaseUrl, queryIntent.intentId);
    const queryAttempt1 = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "update",
      intentId: queryIntent.intentId,
      requestSha256: "2".repeat(64),
    });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: queryAttempt1.attemptId,
        intentId: queryIntent.intentId,
        observations: observations("query_table", [
          priorQuery,
          planB.graph.queryTableRoot.expectedCid,
        ]),
        recoverySequence: resolutionSequence++,
      }),
    ).resolves.toMatchObject({ state: "update_ambiguous" });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: queryAttempt1.attemptId,
        intentId: queryIntent.intentId,
        observations: observations("query_table", [priorQuery, priorQuery]),
        recoverySequence: resolutionSequence++,
      }),
    ).resolves.toMatchObject({ state: "prior_confirmed" });
    const queryAttempt2 = await recordIpnsMutationAttempt(schemaDatabaseUrl, {
      direction: "update",
      intentId: queryIntent.intentId,
      requestSha256: "3".repeat(64),
    });
    await recordIpnsMutationReceipt(schemaDatabaseUrl, {
      attemptId: queryAttempt2.attemptId,
      intentId: queryIntent.intentId,
      outcome: "acknowledged",
      providerReceiptSha256: "a".repeat(64),
    });
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: queryAttempt2.attemptId,
        intentId: queryIntent.intentId,
        observations: observations("query_table", [
          planB.graph.queryTableRoot.expectedCid,
          planB.graph.queryTableRoot.expectedCid,
        ]),
        recoverySequence: resolutionSequence,
      }),
    ).resolves.toMatchObject({ state: "target_observed" });
    await expect(
      verifyIpnsTarget(schemaDatabaseUrl, queryIntent.intentId),
    ).resolves.toMatchObject({ state: "verified" });
    const thirdCid = await calculateIpfsCid("unexpected-third-cid\n");
    const immutableSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      await expect(
        immutableSql`
          UPDATE oracle_publication_ipns_intents
          SET prior_cid = ${thirdCid}
          WHERE intent_id = ${openIntent.intentId}
        `,
      ).rejects.toThrow("immutable");
    } finally {
      await immutableSql.end({ timeout: 5 });
    }

    const openReady = await publicationIpnsUpdateReady(schemaDatabaseUrl, {
      ...executionRequest,
      domain: "open_data",
    });
    const queryReady = await publicationIpnsUpdateReady(schemaDatabaseUrl, {
      ...executionRequest,
      domain: "query_table",
    });
    expect(openReady.targetCid).toBe(cids.get("open_data:index.json"));
    expect(queryReady.targetCid).toBe(
      cids.get("query_table:query-tables/pasco/query-table.parquet"),
    );
    await expect(
      checkpointPublicationIpnsUpdated(schemaDatabaseUrl, {
        ...executionRequest,
        domain: "open_data",
        networkKey: targets.openData.ipnsNetworkKey,
        priorCid: priorOpen,
        targetCid: openReady.targetCid,
      }),
    ).rejects.toThrow("Legacy IPNS checkpoints are disabled");
    await expect(
      verifyPublicationIpnsResolution(schemaDatabaseUrl, {
        ...executionRequest,
        domain: "open_data",
        resolvedCid: openReady.targetCid,
      }),
    ).rejects.toThrow("Legacy IPNS resolution checkpoints are disabled");
    expect(
      await completePublicationPlan(schemaDatabaseUrl, executionRequest),
    ).toMatchObject({ state: "completed" });
    expect(await getPublicationState(schemaDatabaseUrl)).toMatchObject({
      planId: planB.planId,
      state: "completed",
    });
    expect(() => createRemotePublicationExecutor(planB)).toThrow(
      "no production or local publisher",
    );
  }, 600_000);

  it("rejects stale projection heads at approval and immediately before execution", async () => {
    const headSql = postgres(schemaDatabaseUrl, { max: 1 });
    const heads = await headSql<
      { authoritative_base_snapshot_id: string; current_snapshot_id: string }[]
    >`SELECT current_snapshot_id, authoritative_base_snapshot_id
      FROM oracle_projection_heads LIMIT 1`;
    await headSql.end({ timeout: 5 });
    const prior = heads[0]!;
    const snapshotC = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: [...range(1, 23), "PUB-26", "PUB-27"],
      label: "publication-stale-c",
      observedAt: "2026-08-29T06:00:00.000Z",
      previousAuthoritativeSnapshotId: prior.authoritative_base_snapshot_id,
      previousProjectionSnapshotId: prior.current_snapshot_id,
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotC.request);
    await load(snapshotC);
    const exportC = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      runId: snapshotC.request.runId,
      targets,
    });
    const planC = await planAt(exportC.outputRoot);

    const snapshotD = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["PUB-03"],
      coverage: "authoritative",
      folios: [...range(1, 22), "PUB-26", "PUB-27", "PUB-28"],
      label: "publication-stale-d",
      observedAt: "2026-08-29T07:00:00.000Z",
      previousAuthoritativeSnapshotId: snapshotC.snapshot.snapshotId,
      previousProjectionSnapshotId: snapshotC.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotD.request);
    await load(snapshotD);
    await expect(
      approvePublicationPlan(schemaDatabaseUrl, {
        county: "pasco",
        planId: planC.planId,
        planSha256: planC.planSha256,
        approverReference: "synthetic_controller",
      }),
    ).rejects.toThrow("stale or not sealed");

    const exportD = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      runId: snapshotD.request.runId,
      targets,
    });
    const planD = await planAt(exportD.outputRoot);
    const identityD = {
      county: "pasco" as const,
      planId: planD.planId,
      planSha256: planD.planSha256,
    };
    await expect(
      approvePublicationPlan(schemaDatabaseUrl, {
        ...identityD,
        approverReference: "synthetic_controller",
      }),
    ).resolves.toMatchObject({ state: "approved" });

    const snapshotE = await createSyntheticLifecycleSnapshot(dataDir, {
      changedFolios: ["PUB-04"],
      coverage: "incomplete",
      folios: [...range(1, 22), "PUB-26", "PUB-27", "PUB-29"],
      label: "publication-stale-partial-e",
      observedAt: "2026-08-29T08:00:00.000Z",
      previousAuthoritativeSnapshotId: snapshotD.snapshot.snapshotId,
      previousProjectionSnapshotId: snapshotD.snapshot.snapshotId,
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotE.request);
    await load(snapshotE);
    await expect(
      beginPublicationExecution({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        publicationRootRelative: exportD.outputRoot,
        request: identityD,
      }),
    ).rejects.toThrow("stale or not sealed");
    const planDirectory = path.join(
      dataDir,
      "artifacts",
      "publish",
      "pasco",
      "plans",
    );
    const beforeRejectedReplacement = (await readdir(planDirectory)).sort();
    await expect(
      buildPublicationDryRun({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        exportMode: "authoritative",
        runId: snapshotE.request.runId,
        targets,
      }),
    ).rejects.toThrow("replacement blocked while approved");
    expect((await readdir(planDirectory)).sort()).toEqual(
      beforeRejectedReplacement,
    );

    const fixturePropertyId = "prop_e72ba795455c19d71ce4cb11f6177a5e";
    const fixtureRunId = `run_${"9".repeat(32)}`;
    const fixtureSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      await fixtureSql`
        INSERT INTO oracle_pipeline_runs (
          run_id, workflow_id, county, sample_algorithm, sample_seed,
          selection_size, window_start, window_end, as_of, status,
          completed_at, coverage_mode
        ) VALUES (
          ${fixtureRunId}, 'fixture-injection-e2e', 'pasco',
          'fixture-injection-test', 'synthetic', 1,
          '2026-08-29T00:00:00.000Z', '2026-08-29T00:00:00.000Z',
          '2026-08-29T00:00:00.000Z', 'completed',
          '2026-08-29T00:00:00.000Z', 'sample'
        )
      `;
      await fixtureSql.unsafe(
        "ALTER TABLE oracle_properties DROP CONSTRAINT oracle_properties_property_id_check",
      );
      await fixtureSql`
        INSERT INTO oracle_properties (
          property_id, parcel_id, county, source_system, exact_folio,
          matching_folio_digits, site_address, site_city, site_zip,
          property_use_code, property_use_description, acres,
          total_square_feet, heated_square_feet, year_built,
          source_record_hash, first_seen_run_id, last_seen_run_id
        ) VALUES (
          ${fixturePropertyId}, ${`parcel_${"8".repeat(32)}`}, 'pasco',
          'pasco_appraiser', 'FIXTURE-INJECTION', '', 'SYNTHETIC',
          'SYNTHETIC', '00000', 'SYNTHETIC', 'SYNTHETIC', 1, 1, 1, 2000,
          ${`sha256:${"7".repeat(64)}`}, ${fixtureRunId}, ${fixtureRunId}
        )
      `;
    } finally {
      await fixtureSql.end({ timeout: 5 });
    }
    const beforeFixturePlans = (await readdir(planDirectory)).sort();
    await expect(
      buildPublicationDryRun({
        dataDir,
        databaseUrl: schemaDatabaseUrl,
        exportMode: "bounded",
        runId: fixtureRunId,
        targets,
      }),
    ).rejects.toThrow("frozen fixture property IDs");
    expect((await readdir(planDirectory)).sort()).toEqual(beforeFixturePlans);
    const publishBase = path.dirname(planDirectory);
    expect(
      (await readdir(publishBase)).filter((name) =>
        name.startsWith(`.build-${fixtureRunId}-`),
      ),
    ).toEqual([]);
    const fixtureEvidenceSql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const persisted = await fixtureEvidenceSql<
        { approvals: number; effects: number; plans: number; runs: number }[]
      >`
        SELECT
          (SELECT count(*)::int FROM oracle_publication_plans
           WHERE run_id = ${fixtureRunId}) AS plans,
          (SELECT count(*)::int FROM oracle_publication_dry_runs
           WHERE run_id = ${fixtureRunId}) AS runs,
          (SELECT count(*)::int FROM oracle_publication_approvals approval
           JOIN oracle_publication_plans plan USING (plan_id)
           WHERE plan.run_id = ${fixtureRunId}) AS approvals,
          (SELECT count(*)::int FROM oracle_publication_object_effects effect
           JOIN oracle_publication_plans plan USING (plan_id)
           WHERE plan.run_id = ${fixtureRunId}) AS effects
      `;
      expect(persisted[0]).toEqual({
        approvals: 0,
        effects: 0,
        plans: 0,
        runs: 0,
      });
    } finally {
      await fixtureEvidenceSql.end({ timeout: 5 });
    }
  }, 120_000);
});
