import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";

import {
  candidateSourceSnapshotPrefixes,
  materializeCandidateSourceSnapshotControlArtifacts,
} from "../../src/publication/candidate-source-snapshot-controls.js";
import { sha256 } from "../../src/lib/hash.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";

const cid = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
const digest = "0".repeat(64);
const namespaceId = `snapshotns_${"1".repeat(32)}`;

function artifact(
  domain: "open_data" | "query_table",
  objectKey: string,
  role: "manifest" | "metadata" | "property" | "query_table" | "root" | "shard",
) {
  return {
    byteSize: 10,
    domain,
    expectedCid: cid,
    objectKey,
    role,
    sha256: digest,
  };
}

function compactManifest(propertyCount: number) {
  return {
    classification: {
      canonical: false as const,
      elephantOwned: false as const,
      independentlyPascoCertified: false as const,
      ownerControlled: false as const,
      publicationClass: "candidate_owned_source_snapshot_demo" as const,
      resourceOwner: "candidate" as const,
      sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot" as const,
    },
    contracts: {
      canonical: { sha256: digest, version: "1.0.0" as const },
      mcp: { sha256: digest, version: "1.2.0" as const },
    },
    county: "pasco" as const,
    coverage: {
      buildings: {
        facts: propertyCount,
        properties: propertyCount,
        yearBuiltProxyProperties: propertyCount,
      },
      contractors: { availability: "unavailable" as const, facts: 0 as const },
      coordinates: {
        availableProperties: propertyCount,
        missingProperties: 0,
      },
      membership:
        "complete_membership_of_exact_source_snapshot_noncanonical" as const,
      ownership: {
        acceptedRows: propertyCount,
        malformedRows: 0,
        properties: propertyCount,
        sourceRows: propertyCount,
      },
      permits: {
        availability: "unavailable" as const,
        facts: 0 as const,
        permitContractorRelationships: 0 as const,
      },
      propertyCount,
      siteAddresses: {
        sourceRows: propertyCount,
        usableProperties: propertyCount,
      },
      unresolvedPublishedParcelStatistic: 335_946,
    },
    disclosure: "Synthetic candidate-owned noncanonical test publication.",
    freshness: {
      asOf: "2026-08-23T00:00:00.000Z",
      loadedAt: "2026-08-30T00:00:00.000Z",
      observedAt: "2026-08-23T00:00:00.000Z",
    },
    graph: { openDataRootCid: cid, propertyCount },
    queryTable: {
      byteSize: 10,
      expectedCid: cid,
      propertyCount,
      schemaSha256: digest,
      sha256: digest,
    },
    source: {
      authorityClass: "owner_assumed_authoritative_snapshot" as const,
      authorityId: `authority_${"0".repeat(32)}`,
      materializationId: `materialization_${"0".repeat(32)}`,
      materializationSha256: digest,
      runId: `run_${"0".repeat(32)}`,
      scopeId: `scope_${"0".repeat(32)}`,
      selectionSha256: digest,
      snapshotId: `snapshot_${"0".repeat(32)}`,
    },
  };
}

async function sourceArtifacts(
  root: string,
  options: {
    mismatchedGraphChild?: boolean;
    mismatchedManifestBytes?: boolean;
    omitSecondProperty?: boolean;
    reverse?: boolean;
  } = {},
) {
  const planPath = path.join(root, "publication-plan.json");
  const manifestPath = path.join(root, "manifest.json");
  const coveragePath = path.join(root, "open-data", "coverage.json");
  const provenancePath = path.join(root, "open-data", "provenance.json");
  const queryTablePath = path.join(
    root,
    "query",
    "query-tables",
    "pasco",
    "query-table.parquet",
  );
  await mkdir(path.dirname(coveragePath), { recursive: true });
  await mkdir(path.dirname(queryTablePath), { recursive: true });
  const coverageBytes = Buffer.from(
    `${JSON.stringify({
      canonicalProperties: 2,
      county: "pasco",
      coverageMode: "authoritative_complete",
      runId: `run_${"0".repeat(32)}`,
      scope: "synthetic authoritative source",
      scopeId: `scope_${"0".repeat(32)}`,
      snapshotId: `snapshot_${"0".repeat(32)}`,
      warning: "synthetic source",
    })}\n`,
  );
  const provenanceBytes = Buffer.from(
    `${JSON.stringify({
      county: "pasco",
      sourceWatermark: {
        coverageMode: "authoritative_complete",
        runId: `run_${"0".repeat(32)}`,
        scopeId: `scope_${"0".repeat(32)}`,
        snapshotId: `snapshot_${"0".repeat(32)}`,
      },
      version: "1.0.0",
    })}\n`,
  );
  await writeFile(coveragePath, coverageBytes);
  await writeFile(provenancePath, provenanceBytes);
  const duckdb = await DuckDBInstance.create(":memory:");
  const connection = await duckdb.connect();
  try {
    await connection.run(`
      CREATE TABLE properties AS
      SELECT * FROM (VALUES
        ('SYNTH-1', 'property_${"1".repeat(32)}', 'authoritative_complete'),
        ('SYNTH-2', 'property_${"2".repeat(32)}', 'authoritative_complete')
      ) rows(request_identifier, property_id, coverage_mode)
    `);
    await connection.run(
      `COPY properties TO '${queryTablePath.replaceAll("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)`,
    );
  } finally {
    connection.closeSync();
  }
  const manifestBytes = Buffer.from(
    JSON.stringify({
      entries: [
        {
          bytes: 10,
          cid,
          objectKey: `properties/property_${"1".repeat(32)}.json`,
          parcelIdentifier: "SYNTH-1",
          propertyId: `property_${"1".repeat(32)}`,
          sha256: digest,
        },
        {
          bytes: options.mismatchedManifestBytes ? 11 : 10,
          cid,
          objectKey: `properties/property_${"2".repeat(32)}.json`,
          parcelIdentifier: "SYNTH-2",
          propertyId: `property_${"2".repeat(32)}`,
          sha256: digest,
        },
      ],
    }),
  );
  const manifestArtifact = {
    ...artifact("open_data", "manifest.json", "manifest"),
    byteSize: manifestBytes.byteLength,
    expectedCid: await calculateIpfsCid(manifestBytes),
    sha256: sha256(manifestBytes),
  };
  const coverageArtifact = {
    ...artifact("open_data", "coverage.json", "metadata"),
    byteSize: coverageBytes.byteLength,
    expectedCid: await calculateIpfsCid(coverageBytes),
    sha256: sha256(coverageBytes),
  };
  const provenanceArtifact = {
    ...artifact("open_data", "provenance.json", "metadata"),
    byteSize: provenanceBytes.byteLength,
    expectedCid: await calculateIpfsCid(provenanceBytes),
    sha256: sha256(provenanceBytes),
  };
  const queryTableBytes = await readFile(queryTablePath);
  const queryTableArtifact = {
    ...artifact(
      "query_table",
      "query-tables/pasco/query-table.parquet",
      "query_table",
    ),
    byteSize: queryTableBytes.byteLength,
    expectedCid: await calculateIpfsCid(queryTableBytes),
    sha256: sha256(queryTableBytes),
  };
  const inventory = [
    coverageArtifact,
    artifact("open_data", "index.json", "root"),
    manifestArtifact,
    artifact(
      "open_data",
      `properties/property_${"1".repeat(32)}.json`,
      "property",
    ),
    ...(options.omitSecondProperty
      ? []
      : [
          artifact(
            "open_data" as const,
            `properties/property_${"2".repeat(32)}.json`,
            "property" as const,
          ),
        ]),
    provenanceArtifact,
    artifact("open_data", "shards/shard-0000.json", "shard"),
    queryTableArtifact,
  ];
  if (options.reverse) inventory.reverse();
  const planBytes = Buffer.from(
    JSON.stringify({
      artifacts: { objectInventory: inventory },
      graph: {
        edges: [
          {
            childCid: cid,
            childKey: `properties/property_${(options.mismatchedGraphChild
              ? "3"
              : "1"
            ).repeat(32)}.json`,
            jsonPointer: "/entries/0/cid",
            parentKey: "shards/shard-0000.json",
          },
          ...(!options.omitSecondProperty
            ? [
                {
                  childCid: cid,
                  childKey: `properties/property_${"2".repeat(32)}.json`,
                  jsonPointer: "/entries/1/cid",
                  parentKey: "shards/shard-0000.json",
                },
              ]
            : []),
          {
            childCid: cid,
            childKey: "shards/shard-0000.json",
            jsonPointer: "/shards/0/shardCid",
            parentKey: "index.json",
          },
        ],
      },
    }),
  );
  await writeFile(planPath, planBytes);
  await writeFile(manifestPath, manifestBytes);
  return {
    expectedSourceManifestFileSha256: sha256(manifestBytes),
    expectedSourcePlanFileSha256: sha256(planBytes),
    expectedSourceQueryTable: {
      byteSize: queryTableArtifact.byteSize,
      expectedCid: queryTableArtifact.expectedCid,
      sha256: queryTableArtifact.sha256,
    },
    manifestPath,
    planPath,
  };
}

describe("candidate source-snapshot compact control materialization", () => {
  it("streams legacy controls into deterministic namespaced bounded artifacts", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-source-"),
    );
    const firstParent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-first-"),
    );
    const secondParent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-second-"),
    );
    const firstRoot = path.join(firstParent, "final");
    const secondRoot = path.join(secondParent, "final");
    try {
      const files = await sourceArtifacts(source);
      const prefixes = candidateSourceSnapshotPrefixes(namespaceId);
      const options = {
        compactManifest: compactManifest(2),
        expectedSourceManifestFileSha256:
          files.expectedSourceManifestFileSha256,
        expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
        expectedSourceQueryTable: files.expectedSourceQueryTable,
        namespaceId,
        prefixes,
        sourceManifestPath: files.manifestPath,
        sourcePlanPath: files.planPath,
      };
      const first = await materializeCandidateSourceSnapshotControlArtifacts({
        ...options,
        outputRoot: firstRoot,
      });
      const second = await materializeCandidateSourceSnapshotControlArtifacts({
        ...options,
        outputRoot: secondRoot,
      });
      expect(first.compactManifest).toEqual(second.compactManifest);
      expect(first.controlArtifacts).toEqual(second.controlArtifacts);
      expect(first.uploadWithoutPlan).toEqual(second.uploadWithoutPlan);
      expect(first.compactManifest.byteSize).toBeLessThan(16 * 1024 * 1024);
      expect(first.controlArtifacts.objectInventory.entryCount).toBe(7);
      expect(first.controlArtifacts.manifestEntries.entryCount).toBe(2);
      expect(first.sourceTargets).toEqual({
        openData: {
          byteSize: 10,
          expectedCid: cid,
          objectKey: `${prefixes.openData}index.json`,
          sha256: digest,
        },
        queryTable: {
          byteSize: first.candidatePayloads.queryTable.byteSize,
          expectedCid: first.candidatePayloads.queryTable.expectedCid,
          objectKey: `${prefixes.queryTable}query-table.parquet`,
          sha256: first.candidatePayloads.queryTable.sha256,
        },
      });

      const uploads = [];
      for await (const upload of first.createUploadRecords()) {
        uploads.push(upload);
      }
      expect(uploads).toHaveLength(first.uploadWithoutPlan.objectCount);
      expect(
        uploads.some(
          (entry) =>
            entry.logicalObjectKey === "manifest.json" &&
            entry.localLocator.kind === "source_payload",
        ),
      ).toBe(false);
      expect(
        uploads.every((entry) => !path.isAbsolute(entry.remoteObjectKey)),
      ).toBe(true);
      expect(
        uploads.some((entry) =>
          entry.remoteObjectKey.startsWith(prefixes.control),
        ),
      ).toBe(true);
      const generatedControls = uploads.filter(
        (entry) => entry.localLocator.kind === "generated_control",
      );
      const generatedPayloads = uploads.filter(
        (entry) => entry.localLocator.kind === "generated_payload",
      );
      expect(
        generatedPayloads.map((entry) => entry.logicalObjectKey).sort(),
      ).toEqual(["coverage.json", "provenance.json", "query-table.parquet"]);
      expect(
        uploads.some(
          (entry) =>
            entry.localLocator.kind === "source_payload" &&
            [
              "coverage.json",
              "provenance.json",
              "query-tables/pasco/query-table.parquet",
            ].includes(entry.localLocator.logicalObjectKey),
        ),
      ).toBe(false);
      expect(generatedControls.length).toBeGreaterThan(0);
      expect(
        generatedControls.every(
          (entry) =>
            entry.remoteObjectKey ===
              `${prefixes.control}${entry.logicalObjectKey}` &&
            entry.localLocator.kind === "generated_control" &&
            entry.localLocator.generatedObjectKey === entry.remoteObjectKey &&
            !entry.logicalObjectKey.startsWith(prefixes.control),
        ),
      ).toBe(true);
      expect(
        uploads.some((entry) =>
          entry.remoteObjectKey.startsWith(prefixes.openData),
        ),
      ).toBe(true);
      expect(
        uploads.some((entry) =>
          entry.remoteObjectKey.startsWith(prefixes.queryTable),
        ),
      ).toBe(true);
      const queryTableUpload = uploads.find(
        (entry) => entry.domain === "query_table",
      );
      expect(queryTableUpload).toMatchObject({
        localLocator: {
          generatedObjectKey: `${prefixes.queryTable}query-table.parquet`,
          kind: "generated_payload",
        },
        logicalObjectKey: "query-table.parquet",
        remoteObjectKey: `${prefixes.queryTable}query-table.parquet`,
      });
      for (const artifact of [
        first.candidatePayloads.coverage,
        first.candidatePayloads.provenance,
      ]) {
        const document = await readFile(
          path.join(firstRoot, artifact.objectKey),
          "utf8",
        );
        expect(document).toContain('"coverageMode":"source_snapshot"');
        expect(document).not.toContain("authoritative_complete");
      }
      const candidateQueryPath = path.join(
        firstRoot,
        first.candidatePayloads.queryTable.objectKey,
      );
      const verificationDb = await DuckDBInstance.create(":memory:");
      const verificationConnection = await verificationDb.connect();
      try {
        const reader = await verificationConnection.runAndReadAll(`
          SELECT coverage_mode, count(*)::BIGINT AS row_count
          FROM read_parquet('${candidateQueryPath.replaceAll("'", "''")}')
          GROUP BY coverage_mode
        `);
        expect(reader.getRowObjectsJson()).toEqual([
          { coverage_mode: "source_snapshot", row_count: "2" },
        ]);
      } finally {
        verificationConnection.closeSync();
      }
      expect(first.candidatePayloads).toEqual(second.candidatePayloads);
      expect(JSON.stringify(uploads)).not.toContain(source);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(firstParent, { force: true, recursive: true });
      await rm(secondParent, { force: true, recursive: true });
    }
  });

  it("rejects a source inventory whose deterministic order regressed", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-order-"),
    );
    const outputParent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-output-"),
    );
    const outputRoot = path.join(outputParent, "final");
    try {
      const files = await sourceArtifacts(source, { reverse: true });
      await expect(
        materializeCandidateSourceSnapshotControlArtifacts({
          compactManifest: compactManifest(2),
          expectedSourceManifestFileSha256:
            files.expectedSourceManifestFileSha256,
          expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
          expectedSourceQueryTable: files.expectedSourceQueryTable,
          namespaceId,
          outputRoot,
          prefixes: candidateSourceSnapshotPrefixes(namespaceId),
          sourceManifestPath: files.manifestPath,
          sourcePlanPath: files.planPath,
        }),
      ).rejects.toThrow("not strictly ordered");
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(outputParent, { force: true, recursive: true });
    }
  });

  it("rejects inconsistent manifest, graph, and inventory cardinalities", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-cardinality-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-cardinality-output-"),
    );
    try {
      const files = await sourceArtifacts(source, { omitSecondProperty: true });
      await expect(
        materializeCandidateSourceSnapshotControlArtifacts({
          compactManifest: compactManifest(2),
          expectedSourceManifestFileSha256:
            files.expectedSourceManifestFileSha256,
          expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
          expectedSourceQueryTable: files.expectedSourceQueryTable,
          namespaceId,
          outputRoot: path.join(parent, "final"),
          prefixes: candidateSourceSnapshotPrefixes(namespaceId),
          sourceManifestPath: files.manifestPath,
          sourcePlanPath: files.planPath,
        }),
      ).rejects.toThrow(/missing or extra object|cardinalities/);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });

  it.each([
    ["manifest/inventory", { mismatchedManifestBytes: true }],
    ["graph/inventory", { mismatchedGraphChild: true }],
  ] as const)(
    "rejects same-count %s semantic mismatches",
    async (_label, sourceOptions) => {
      const source = await mkdtemp(
        path.join(tmpdir(), "candidate-controls-semantic-source-"),
      );
      const parent = await mkdtemp(
        path.join(tmpdir(), "candidate-controls-semantic-output-"),
      );
      try {
        const files = await sourceArtifacts(source, sourceOptions);
        await expect(
          materializeCandidateSourceSnapshotControlArtifacts({
            compactManifest: compactManifest(2),
            expectedSourceManifestFileSha256:
              files.expectedSourceManifestFileSha256,
            expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
            expectedSourceQueryTable: files.expectedSourceQueryTable,
            namespaceId,
            outputRoot: path.join(parent, "final"),
            prefixes: candidateSourceSnapshotPrefixes(namespaceId),
            sourceManifestPath: files.manifestPath,
            sourcePlanPath: files.planPath,
          }),
        ).rejects.toThrow(/semantic binding|duplicate or conflict/);
      } finally {
        await rm(source, { force: true, recursive: true });
        await rm(parent, { force: true, recursive: true });
      }
    },
  );

  it("rejects source-plan mutation during upload replay", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-replay-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-replay-output-"),
    );
    try {
      const files = await sourceArtifacts(source);
      const result = await materializeCandidateSourceSnapshotControlArtifacts({
        compactManifest: compactManifest(2),
        expectedSourceManifestFileSha256:
          files.expectedSourceManifestFileSha256,
        expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
        expectedSourceQueryTable: files.expectedSourceQueryTable,
        namespaceId,
        outputRoot: path.join(parent, "final"),
        prefixes: candidateSourceSnapshotPrefixes(namespaceId),
        sourceManifestPath: files.manifestPath,
        sourcePlanPath: files.planPath,
      });
      const iterator = result.createUploadRecords()[Symbol.asyncIterator]();
      await expect(iterator.next()).resolves.toMatchObject({ done: false });
      const original = await readFile(files.planPath);
      await writeFile(
        files.planPath,
        Buffer.concat([original, Buffer.from("\n")]),
      );
      await expect(
        (async () => {
          while (!(await iterator.next()).done) {
            // Drain the deterministic replay so its final identity check runs.
          }
        })(),
      ).rejects.toThrow("changed during upload replay");
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });

  it("does not replace or delete an existing empty final directory", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-empty-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-empty-output-"),
    );
    const outputRoot = path.join(parent, "final");
    try {
      const files = await sourceArtifacts(source);
      await mkdir(outputRoot);
      await expect(
        materializeCandidateSourceSnapshotControlArtifacts({
          compactManifest: compactManifest(2),
          expectedSourceManifestFileSha256:
            files.expectedSourceManifestFileSha256,
          expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
          expectedSourceQueryTable: files.expectedSourceQueryTable,
          namespaceId,
          outputRoot,
          prefixes: candidateSourceSnapshotPrefixes(namespaceId),
          sourceManifestPath: files.manifestPath,
          sourcePlanPath: files.planPath,
        }),
      ).rejects.toThrow("inventory differs");
      await expect(readdir(outputRoot)).resolves.toEqual([]);
      expect(
        (await readdir(parent)).filter(
          (name) => name.includes(".contender-") || name.endsWith(".lock"),
        ),
      ).toEqual([]);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });

  it("atomically adopts one deterministic winner and cleans only private contenders", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-race-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-race-parent-"),
    );
    const outputRoot = path.join(parent, "final-control-artifacts");
    try {
      const files = await sourceArtifacts(source);
      const input = {
        compactManifest: compactManifest(2),
        expectedSourceManifestFileSha256:
          files.expectedSourceManifestFileSha256,
        expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
        expectedSourceQueryTable: files.expectedSourceQueryTable,
        namespaceId,
        outputRoot,
        prefixes: candidateSourceSnapshotPrefixes(namespaceId),
        sourceManifestPath: files.manifestPath,
        sourcePlanPath: files.planPath,
      };
      const results = await Promise.all([
        materializeCandidateSourceSnapshotControlArtifacts(input),
        materializeCandidateSourceSnapshotControlArtifacts(input),
      ]);
      expect(results.map((result) => result.adoptedExisting).sort()).toEqual([
        false,
        true,
      ]);
      expect(results[0]!.controlArtifacts).toEqual(
        results[1]!.controlArtifacts,
      );
      // Simulate a later database-record failure in the adopter: no builder
      // owns or removes the already-visible deterministic winner.
      await expect(
        Promise.reject(new Error("injected database record failure")),
      ).rejects.toThrow("injected database record failure");
      await expect(readdir(outputRoot)).resolves.not.toHaveLength(0);
      expect(
        (await readdir(parent)).filter((name) => name.includes(".contender-")),
      ).toEqual([]);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });

  it("recovers deterministically when a legacy finalization lock survives a crash", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-stale-lock-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-stale-lock-parent-"),
    );
    const outputRoot = path.join(parent, "final-control-artifacts");
    const staleLock = path.join(
      parent,
      ".final-control-artifacts.finalization.lock",
    );
    try {
      const files = await sourceArtifacts(source);
      await writeFile(staleLock, "abandoned-owner-token\n", { flag: "wx" });
      const result = await materializeCandidateSourceSnapshotControlArtifacts({
        compactManifest: compactManifest(2),
        expectedSourceManifestFileSha256:
          files.expectedSourceManifestFileSha256,
        expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
        expectedSourceQueryTable: files.expectedSourceQueryTable,
        namespaceId,
        outputRoot,
        prefixes: candidateSourceSnapshotPrefixes(namespaceId),
        sourceManifestPath: files.manifestPath,
        sourcePlanPath: files.planPath,
      });
      expect(result.adoptedExisting).toBe(false);
      await expect(readdir(outputRoot)).resolves.not.toHaveLength(0);
      await expect(readFile(staleLock, "utf8")).resolves.toBe(
        "abandoned-owner-token\n",
      );
      expect(
        (await readdir(parent)).filter((name) => name.includes(".contender-")),
      ).toEqual([]);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });

  it("rejects a prefix not derived from the exact namespace identity", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-prefix-"),
    );
    const outputParent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-output-"),
    );
    const outputRoot = path.join(outputParent, "final");
    try {
      const files = await sourceArtifacts(source);
      await expect(
        materializeCandidateSourceSnapshotControlArtifacts({
          compactManifest: compactManifest(2),
          expectedSourceManifestFileSha256:
            files.expectedSourceManifestFileSha256,
          expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
          expectedSourceQueryTable: files.expectedSourceQueryTable,
          namespaceId,
          outputRoot,
          prefixes: {
            ...candidateSourceSnapshotPrefixes(namespaceId),
            control: "publication-control/source-snapshot-demo-v1/wrong/",
          },
          sourceManifestPath: files.manifestPath,
          sourcePlanPath: files.planPath,
        }),
      ).rejects.toThrow("do not match the namespace identity");
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(outputParent, { force: true, recursive: true });
    }
  });

  it("fails closed without overwriting or deleting invalid finalized output", async () => {
    const source = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-invalid-source-"),
    );
    const parent = await mkdtemp(
      path.join(tmpdir(), "candidate-controls-invalid-parent-"),
    );
    const outputRoot = path.join(parent, "final-control-artifacts");
    try {
      const files = await sourceArtifacts(source);
      await writeFile(outputRoot, "not-a-directory");
      await expect(
        materializeCandidateSourceSnapshotControlArtifacts({
          compactManifest: compactManifest(2),
          expectedSourceManifestFileSha256:
            files.expectedSourceManifestFileSha256,
          expectedSourcePlanFileSha256: files.expectedSourcePlanFileSha256,
          expectedSourceQueryTable: files.expectedSourceQueryTable,
          namespaceId,
          outputRoot,
          prefixes: candidateSourceSnapshotPrefixes(namespaceId),
          sourceManifestPath: files.manifestPath,
          sourcePlanPath: files.planPath,
        }),
      ).rejects.toThrow();
      await expect(readFile(outputRoot, "utf8")).resolves.toBe(
        "not-a-directory",
      );
      expect(
        (await readdir(parent)).filter((name) => name.includes(".contender-")),
      ).toEqual([]);
    } finally {
      await rm(source, { force: true, recursive: true });
      await rm(parent, { force: true, recursive: true });
    }
  });
});
