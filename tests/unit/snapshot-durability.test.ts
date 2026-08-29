import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { DurableInputError } from "../../src/lib/durability-errors.js";
import { pascoLoaderAdvisoryLockKey } from "../../src/db/loader-durability.js";
import { planProjectionFactChanges } from "../../src/db/projection-repository.js";
import {
  resolveBoundDataPath,
  verifyPreparedInput,
} from "../../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parseCountyIngestRequest,
  parseIngestChunkRequest,
  parseLoaderRequest,
  parsePreparedPilot,
} from "../../src/workflow/schemas.js";
import {
  createSyntheticLifecycleSnapshot,
  createSyntheticSnapshot,
  syntheticLoaderIdempotencyKey,
} from "../helpers/durability.js";

const temporaryDirectories: string[] = [];

async function temporaryDataDir(): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), "prism-snapshot-test-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("source snapshot and prepared-input durability", () => {
  it("applies replace-set, positive-upsert, and explicit-tombstone fact semantics", () => {
    const propertyId = "property_00000000000000000000000000000000";
    const result = planProjectionFactChanges({
      facts: [
        {
          collectionSemantics: "replace_set",
          evidenceRefs: ["evidence_new"],
          factType: "ownership",
          naturalKey: "owner:new",
          payload: { state: "available" },
          sourceRecordHash: `sha256:${"a".repeat(64)}`,
        },
        {
          collectionSemantics: "explicit_tombstone",
          evidenceRefs: ["evidence_tombstone"],
          factType: "availability",
          naturalKey: "permits",
          payload: { state: "unavailable" },
          sourceRecordHash: `sha256:${"b".repeat(64)}`,
        },
      ],
      priorFacts: [
        {
          fact_type: "ownership",
          fact_version_id: "factversion_old_owner",
          natural_key: "owner:old",
          property_id: propertyId,
        },
        {
          fact_type: "coordinate",
          fact_version_id: "factversion_coordinate",
          natural_key: "pasco_gis:parcel_centroid",
          property_id: propertyId,
        },
        {
          fact_type: "availability",
          fact_version_id: "factversion_permits",
          natural_key: "permits",
          property_id: propertyId,
        },
      ],
      propertyId,
      replaceSetTypes: ["building", "ownership"],
    });
    expect(
      result.nextFacts.map((fact) => `${fact.fact_type}:${fact.natural_key}`),
    ).toEqual(["coordinate:pasco_gis:parcel_centroid", "ownership:owner:new"]);
    expect(
      result.changes.map((change) => ({
        eventType: change.eventType,
        key: `${change.factType}:${change.naturalKey}`,
      })),
    ).toEqual([
      { eventType: "removed", key: "availability:permits" },
      { eventType: "new", key: "ownership:owner:new" },
      { eventType: "removed", key: "ownership:owner:old" },
    ]);
  });

  it("binds exact bytes and keeps Snapshot A distinct from Snapshot B", async () => {
    const dataDir = await temporaryDataDir();
    const snapshotA = await createSyntheticSnapshot(dataDir, "a");
    const snapshotB = await createSyntheticSnapshot(dataDir, "b");

    expect(snapshotA.snapshot.snapshotId).not.toBe(
      snapshotB.snapshot.snapshotId,
    );
    expect(snapshotA.reference.preparedInputId).not.toBe(
      snapshotB.reference.preparedInputId,
    );
    expect(snapshotA.snapshot.sourceObjects[0]?.sha256).not.toBe(
      snapshotB.snapshot.sourceObjects[0]?.sha256,
    );
    expect(snapshotA.snapshot.coverage).toMatchObject({
      completeness: { result: "not_applicable" },
      mode: "sample",
      selection: { kind: "deterministic_sample", selectionSize: 25 },
    });

    const first = await verifyPreparedInput(
      dataDir,
      snapshotA.reference,
      parsePreparedPilot,
      snapshotA.snapshot.snapshotId,
    );
    const restarted = await verifyPreparedInput(
      dataDir,
      snapshotA.reference,
      parsePreparedPilot,
      snapshotA.snapshot.snapshotId,
    );
    expect(restarted.reference).toEqual(first.reference);
    expect(restarted.prepared).toEqual(first.prepared);

    await expect(
      verifyPreparedInput(
        dataDir,
        snapshotA.reference,
        parsePreparedPilot,
        snapshotB.snapshot.snapshotId,
      ),
    ).rejects.toThrow("Prepared input snapshot mismatch");
  });

  it("excludes creation time from deterministic snapshot identity", async () => {
    const dataDir = await temporaryDataDir();
    const folios = Array.from(
      { length: 25 },
      (_, index) => `IDENT-${(index + 1).toString().padStart(2, "0")}`,
    );
    const first = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "sample",
      createdAt: "2026-08-29T00:00:01.000Z",
      folios,
      label: "identity-created-at",
    });
    const second = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "sample",
      createdAt: "2026-08-29T12:00:00.000Z",
      folios,
      label: "identity-created-at",
    });
    expect(second.snapshot.snapshotId).toBe(first.snapshot.snapshotId);
    expect(second.reference.preparedInputId).toBe(
      first.reference.preparedInputId,
    );
    expect(second.snapshot.createdAt).toBe(first.snapshot.createdAt);
  });

  it("atomically finalizes identical concurrent snapshot preparation", async () => {
    const dataDir = await temporaryDataDir();
    const options = {
      coverage: "sample" as const,
      createdAt: "2026-08-29T00:00:01.000Z",
      folios: Array.from(
        { length: 25 },
        (_, index) => `CONCURRENT-${(index + 1).toString().padStart(2, "0")}`,
      ),
      label: "concurrent-identical-snapshot",
    };
    const [first, second] = await Promise.all([
      createSyntheticLifecycleSnapshot(dataDir, options),
      createSyntheticLifecycleSnapshot(dataDir, options),
    ]);
    expect(second.snapshot).toEqual(first.snapshot);
    expect(second.reference).toEqual(first.reference);
    const finalizedDirectory = path.join(
      dataDir,
      path.dirname(first.reference.manifest.relativePath),
    );
    expect(
      (await readdir(finalizedDirectory)).filter((name) =>
        name.endsWith(".part"),
      ),
    ).toEqual([]);
  });

  it("rejects modified, missing, mismatched, and path-escaping inputs", async () => {
    const dataDir = await temporaryDataDir();
    const snapshot = await createSyntheticSnapshot(dataDir, "a");
    const manifestPath = await resolveBoundDataPath(
      dataDir,
      snapshot.reference.manifest.relativePath,
    );
    const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as {
      prepared: { relativePath: string };
    };
    const preparedPath = await resolveBoundDataPath(
      dataDir,
      manifest.prepared.relativePath,
    );
    await writeFile(preparedPath, "modified\n");

    await expect(
      verifyPreparedInput(
        dataDir,
        snapshot.reference,
        parsePreparedPilot,
        snapshot.snapshot.snapshotId,
      ),
    ).rejects.toThrow("prepared input binding mismatch");
    await expect(
      resolveBoundDataPath(dataDir, "../escape.json"),
    ).rejects.toBeInstanceOf(DurableInputError);
    await expect(resolveBoundDataPath(dataDir, "missing.json")).rejects.toThrow(
      "Bound input is missing",
    );
    await expect(
      verifyPreparedInput(
        dataDir,
        {
          ...snapshot.reference,
          preparedInputId: "prepared_00000000000000000000000000000000",
        },
        parsePreparedPilot,
      ),
    ).rejects.toThrow("Prepared reference identity mismatch");
  });
});

describe("strict durable workflow requests", () => {
  it("accepts a bound CountyIngest, IngestChunk, and Loader request", async () => {
    const dataDir = await temporaryDataDir();
    const snapshot = await createSyntheticSnapshot(dataDir, "a");
    const requestHash = countyIngestRequestSha256(snapshot.request);
    expect(parseCountyIngestRequest(snapshot.request)).toEqual(
      snapshot.request,
    );
    expect(
      parseIngestChunkRequest({
        ...snapshot.request,
        chunkCount: 1,
        chunkIndex: 0,
        endExclusive: 25,
        parentRequestSha256: requestHash,
        startIndex: 0,
      }).parentRequestSha256,
    ).toBe(requestHash);
    expect(
      parseLoaderRequest({
        county: "pasco",
        idempotencyKey: syntheticLoaderIdempotencyKey(
          snapshot.request.workflowId,
          snapshot.reference.preparedInputId,
        ),
        parentRequestSha256: requestHash,
        prepared: snapshot.reference,
        request: snapshot.request,
      }).prepared,
    ).toEqual(snapshot.reference);
    expect(() =>
      parseLoaderRequest({
        county: "pasco",
        idempotencyKey: "load_00000000000000000000000000000000",
        parentRequestSha256: requestHash,
        prepared: snapshot.reference,
        request: snapshot.request,
      }),
    ).toThrow("Loader idempotency key does not match its prepared input");
  });

  it("rejects unknown fields, wrong county, malformed IDs, and invalid bounds", () => {
    const valid = {
      asOf: "2026-08-29T00:00:00.000Z",
      county: "pasco",
      runId: "run_00000000000000000000000000000000",
      sampleAlgorithm: "synthetic-transition-v1",
      sampleSeed: "synthetic",
      selectionSize: 25,
      workflowId: "pasco-synthetic-validation",
    };
    expect(() => parseCountyIngestRequest({ ...valid, extra: true })).toThrow(
      "CountyIngest request failed strict validation",
    );
    expect(() =>
      parseCountyIngestRequest({ ...valid, authoritativeComplete: true }),
    ).toThrow("CountyIngest request failed strict validation");
    expect(() =>
      parseCountyIngestRequest({ ...valid, county: "other" }),
    ).toThrow("CountyIngest request failed strict validation");
    expect(() => parseCountyIngestRequest({ ...valid, runId: "bad" })).toThrow(
      "CountyIngest request failed strict validation",
    );
    expect(() =>
      parseCountyIngestRequest({ ...valid, selectionSize: 26 }),
    ).toThrow("CountyIngest request failed strict validation");
    expect(() =>
      parseIngestChunkRequest({
        ...valid,
        chunkCount: 1,
        chunkIndex: 0,
        endExclusive: 5_000,
        parentRequestSha256: "0".repeat(64),
        startIndex: 0,
      }),
    ).toThrow("IngestChunk request failed strict validation at endExclusive");
  });

  it("uses a stable, two-part Pasco advisory-lock key", () => {
    expect(pascoLoaderAdvisoryLockKey()).toEqual([-1112522388, -1784306027]);
  });
});
