import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
  loadPreparedPilot,
  recordRunStarted,
} from "../../src/db/pilot-repository.js";
import { runMigrations } from "../../src/db/migrations.js";
import type { PreparedPilot } from "../../src/domain/types.js";
import { DurableConflictError } from "../../src/lib/durability-errors.js";
import { buildPublicationDryRun } from "../../src/publication/dry-run.js";
import {
  validatePublicationPlan,
  type PublicationPlan,
} from "../../src/publication/plan.js";
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
    ipnsNetworkKey: "k51-synthetic-open-data-test",
  },
  queryTable: {
    bucket: "synthetic-query-table-test",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-query-table-test",
    ipnsNetworkKey: "k51-synthetic-query-table-test",
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
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(8);
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
    });
    await recordRunStarted(schemaDatabaseUrl, snapshotA.request);
    await load(snapshotA);
    const exportA = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      generatedAt: "2026-08-29T01:00:00.000Z",
      runId: snapshotA.request.runId,
      targets,
    });
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
    await expect(
      approvePublicationPlan(schemaDatabaseUrl, {
        ...approvalRequest,
        approverReference: "conflicting_controller",
      }),
    ).rejects.toBeInstanceOf(DurableConflictError);

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
      publicationIpnsUpdateReady(schemaDatabaseUrl, {
        ...executionRequest,
        domain: "open_data",
      }),
    ).rejects.toThrow("every object");

    const cids = new Map<string, string>();
    for (const artifact of planB.artifacts.objectInventory) {
      const cid = `bafytest${artifact.sha256.slice(0, 24)}`;
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
      completePublicationPlan(schemaDatabaseUrl, executionRequest),
    ).rejects.toThrow("both public IPNS resolutions");

    for (const [domain, ready, networkKey] of [
      ["open_data", openReady, targets.openData.ipnsNetworkKey],
      ["query_table", queryReady, targets.queryTable.ipnsNetworkKey],
    ] as const) {
      await checkpointPublicationIpnsUpdated(schemaDatabaseUrl, {
        ...executionRequest,
        domain,
        networkKey,
        priorCid: `bafyprior${domain}`,
        targetCid: ready.targetCid,
      });
      await verifyPublicationIpnsResolution(schemaDatabaseUrl, {
        ...executionRequest,
        domain,
        resolvedCid: ready.targetCid,
      });
    }
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
  }, 120_000);
});
