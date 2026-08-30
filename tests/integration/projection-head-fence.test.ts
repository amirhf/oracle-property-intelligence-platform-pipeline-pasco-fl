import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import postgres from "postgres";
import { describe, expect, it } from "vitest";

import {
  approvePublicationPlan,
  beginPublicationExecution,
} from "../../src/db/publication-durability.js";
import {
  loadPreparedPilot,
  recordRunStarted,
} from "../../src/db/pilot-repository.js";
import { runMigrations } from "../../src/db/migrations.js";
import type { PreparedPilot } from "../../src/domain/types.js";
import { buildPublicationDryRun } from "../../src/publication/dry-run.js";
import { validatePublicationPlan } from "../../src/publication/plan.js";
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

const targets = {
  credentialsAvailable: true,
  openData: {
    bucket: "synthetic-fence-open",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-fence-open",
    ipnsNetworkKey:
      "k51A23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
  queryTable: {
    bucket: "synthetic-fence-query",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-fence-query",
    ipnsNetworkKey:
      "k51B23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
} as const;

const folios = (suffix: string) =>
  Array.from(
    { length: 25 },
    (_, index) => `FENCE-${suffix}-${String(index + 1).padStart(2, "0")}`,
  );

async function withEnvironment(
  label: string,
  test: (environment: {
    dataDir: string;
    databaseUrl: string;
  }) => Promise<void>,
): Promise<void> {
  const schema = `projection_fence_${label}_${process.pid}_${Date.now()}`;
  const databaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schema}`;
  const dataDir = await mkdtemp(path.join(tmpdir(), "oracle-fence-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schema}`);
    expect(await runMigrations(databaseUrl)).toHaveLength(21);
    await test({ dataDir, databaseUrl });
  } finally {
    await admin.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin.end({ timeout: 5 });
    await rm(dataDir, { force: true, recursive: true });
  }
}

async function load(
  dataDir: string,
  databaseUrl: string,
  snapshot: SyntheticSnapshot,
  afterProjectionFenceAcquired?: () => Promise<void>,
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
      ...(afterProjectionFenceAcquired ? { afterProjectionFenceAcquired } : {}),
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

async function preparePlan(
  dataDir: string,
  databaseUrl: string,
  snapshot: SyntheticSnapshot,
) {
  const dryRun = await buildPublicationDryRun({
    dataDir,
    databaseUrl,
    exportMode: "authoritative",
    runId: snapshot.request.runId,
    targets,
  });
  const plan = validatePublicationPlan(
    JSON.parse(
      await readFile(
        path.join(dataDir, dryRun.outputRoot, "publication-dry-run-plan.json"),
        "utf8",
      ),
    ),
  );
  return { dryRun, plan };
}

describe("shared Loader/publication projection-head fence", () => {
  it("serializes Loader head advancement ahead of approval", async () => {
    await withEnvironment("approval", async ({ dataDir, databaseUrl }) => {
      const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "authoritative",
        folios: folios("A"),
        label: "fence-approval-a",
        observedAt: "2026-08-29T00:00:00.000Z",
      });
      await recordRunStarted(databaseUrl, snapshotA.request);
      await load(dataDir, databaseUrl, snapshotA);
      const { plan } = await preparePlan(dataDir, databaseUrl, snapshotA);

      const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "authoritative",
        folios: folios("B"),
        label: "fence-approval-b",
        observedAt: "2026-08-29T01:00:00.000Z",
        previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
        previousProjectionSnapshotId: snapshotA.snapshot.snapshotId,
      });
      await recordRunStarted(databaseUrl, snapshotB.request);
      let releaseLoader!: () => void;
      let loaderLocked!: () => void;
      const loaderHasFence = new Promise<void>((resolve) => {
        loaderLocked = resolve;
      });
      const continueLoader = new Promise<void>((resolve) => {
        releaseLoader = resolve;
      });
      const loader = load(dataDir, databaseUrl, snapshotB, async () => {
        loaderLocked();
        await continueLoader;
      });
      await loaderHasFence;
      const approval = approvePublicationPlan(databaseUrl, {
        approverReference: "synthetic_controller",
        county: "pasco",
        planId: plan.planId,
        planSha256: plan.planSha256,
      });
      releaseLoader();
      const loaded = await loader;
      await expect(approval).rejects.toThrow("stale or not sealed");
      expect(await load(dataDir, databaseUrl, snapshotB)).toEqual(loaded);
    });
  }, 120_000);

  it("lets approval commit first without retargeting after Loader advances", async () => {
    await withEnvironment(
      "approval_first",
      async ({ dataDir, databaseUrl }) => {
        const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
          coverage: "authoritative",
          folios: folios("P"),
          label: "fence-approval-first-a",
          observedAt: "2026-08-29T04:00:00.000Z",
        });
        await recordRunStarted(databaseUrl, snapshotA.request);
        await load(dataDir, databaseUrl, snapshotA);
        const { dryRun, plan } = await preparePlan(
          dataDir,
          databaseUrl,
          snapshotA,
        );
        const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
          coverage: "incomplete",
          folios: folios("Q"),
          label: "fence-approval-first-b",
          observedAt: "2026-08-29T05:00:00.000Z",
          previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
          previousProjectionSnapshotId: snapshotA.snapshot.snapshotId,
        });
        await recordRunStarted(databaseUrl, snapshotB.request);

        let releaseApproval!: () => void;
        let approvalLocked!: () => void;
        const approvalHasFence = new Promise<void>((resolve) => {
          approvalLocked = resolve;
        });
        const continueApproval = new Promise<void>((resolve) => {
          releaseApproval = resolve;
        });
        const approval = approvePublicationPlan(
          databaseUrl,
          {
            approverReference: "synthetic_controller",
            county: "pasco",
            planId: plan.planId,
            planSha256: plan.planSha256,
          },
          {
            afterProjectionFenceAcquired: async () => {
              approvalLocked();
              await continueApproval;
            },
          },
        );
        await approvalHasFence;
        const loader = load(dataDir, databaseUrl, snapshotB);
        releaseApproval();
        await expect(approval).resolves.toMatchObject({ state: "approved" });
        await loader;
        await expect(
          beginPublicationExecution({
            dataDir,
            databaseUrl,
            publicationRootRelative: dryRun.outputRoot,
            request: {
              county: "pasco",
              planId: plan.planId,
              planSha256: plan.planSha256,
            },
          }),
        ).rejects.toThrow("stale");
      },
    );
  }, 120_000);

  it("serializes Loader head advancement ahead of execution", async () => {
    await withEnvironment("execution", async ({ dataDir, databaseUrl }) => {
      const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "authoritative",
        folios: folios("X"),
        label: "fence-execution-a",
        observedAt: "2026-08-29T02:00:00.000Z",
      });
      await recordRunStarted(databaseUrl, snapshotA.request);
      await load(dataDir, databaseUrl, snapshotA);
      const { dryRun, plan } = await preparePlan(
        dataDir,
        databaseUrl,
        snapshotA,
      );
      await approvePublicationPlan(databaseUrl, {
        approverReference: "synthetic_controller",
        county: "pasco",
        planId: plan.planId,
        planSha256: plan.planSha256,
      });

      const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "incomplete",
        folios: folios("Y"),
        label: "fence-execution-b",
        observedAt: "2026-08-29T03:00:00.000Z",
        previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
        previousProjectionSnapshotId: snapshotA.snapshot.snapshotId,
      });
      await recordRunStarted(databaseUrl, snapshotB.request);
      let releaseLoader!: () => void;
      let loaderLocked!: () => void;
      const loaderHasFence = new Promise<void>((resolve) => {
        loaderLocked = resolve;
      });
      const continueLoader = new Promise<void>((resolve) => {
        releaseLoader = resolve;
      });
      const loader = load(dataDir, databaseUrl, snapshotB, async () => {
        loaderLocked();
        await continueLoader;
      });
      await loaderHasFence;
      const execution = beginPublicationExecution({
        dataDir,
        databaseUrl,
        publicationRootRelative: dryRun.outputRoot,
        request: {
          county: "pasco",
          planId: plan.planId,
          planSha256: plan.planSha256,
        },
      });
      releaseLoader();
      const loaded = await loader;
      await expect(execution).rejects.toThrow("stale or not sealed");
      expect(await load(dataDir, databaseUrl, snapshotB)).toEqual(loaded);
    });
  }, 120_000);

  it("lets execution admission commit first but rejects stale replay", async () => {
    await withEnvironment(
      "execution_first",
      async ({ dataDir, databaseUrl }) => {
        const snapshotA = await createSyntheticLifecycleSnapshot(dataDir, {
          coverage: "authoritative",
          folios: folios("M"),
          label: "fence-execution-first-a",
          observedAt: "2026-08-29T06:00:00.000Z",
        });
        await recordRunStarted(databaseUrl, snapshotA.request);
        await load(dataDir, databaseUrl, snapshotA);
        const { dryRun, plan } = await preparePlan(
          dataDir,
          databaseUrl,
          snapshotA,
        );
        const identity = {
          county: "pasco" as const,
          planId: plan.planId,
          planSha256: plan.planSha256,
        };
        await approvePublicationPlan(databaseUrl, {
          ...identity,
          approverReference: "synthetic_controller",
        });
        const snapshotB = await createSyntheticLifecycleSnapshot(dataDir, {
          coverage: "incomplete",
          folios: folios("N"),
          label: "fence-execution-first-b",
          observedAt: "2026-08-29T07:00:00.000Z",
          previousAuthoritativeSnapshotId: snapshotA.snapshot.snapshotId,
          previousProjectionSnapshotId: snapshotA.snapshot.snapshotId,
        });
        await recordRunStarted(databaseUrl, snapshotB.request);

        let releaseExecution!: () => void;
        let executionLocked!: () => void;
        const executionHasFence = new Promise<void>((resolve) => {
          executionLocked = resolve;
        });
        const continueExecution = new Promise<void>((resolve) => {
          releaseExecution = resolve;
        });
        const execution = beginPublicationExecution({
          dataDir,
          databaseUrl,
          publicationRootRelative: dryRun.outputRoot,
          request: identity,
          testHooks: {
            afterProjectionFenceAcquired: async () => {
              executionLocked();
              await continueExecution;
            },
          },
        });
        await executionHasFence;
        const loader = load(dataDir, databaseUrl, snapshotB);
        releaseExecution();
        await expect(execution).resolves.toMatchObject({ state: "executing" });
        await loader;
        await expect(
          beginPublicationExecution({
            dataDir,
            databaseUrl,
            publicationRootRelative: dryRun.outputRoot,
            request: identity,
          }),
        ).rejects.toThrow("stale");
      },
    );
  }, 120_000);
});
