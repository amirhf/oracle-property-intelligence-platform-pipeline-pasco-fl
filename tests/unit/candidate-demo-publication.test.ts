import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it, vi } from "vitest";

import { classifyCandidateResolutionObservations } from "../../src/db/candidate-demo-publication.js";
import { canonicalJson } from "../../src/lib/canonical-json.js";
import { sha256 } from "../../src/lib/hash.js";
import { materializeCandidateDemoArtifacts } from "../../src/publication/candidate-demo-artifacts.js";
import {
  CANDIDATE_DEMO_WORDING,
  createCandidateDemoPlan,
} from "../../src/publication/candidate-demo.js";
import { loadCandidateDemoConfig } from "../../src/publication/candidate-demo-config.js";
import {
  CandidateDemoFilebaseExecutor,
  type FilebaseCandidateTransport,
} from "../../src/publication/filebase-executor.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import { syntheticSamplePublicationPlan } from "../helpers/candidate-demo.js";

const keyA = `k51${"2".repeat(59)}`;
const keyB = `k51${"3".repeat(59)}`;
const priorOpen = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
const priorQuery = "QmYwAPJzv5CZsnAzt8auVZRnGi9VQUg9nHfS3aB2NFv7fC";
const preflightEvidenceSha256 = "d".repeat(64);
const preflightObservedAt = "2026-08-30T00:00:00.000Z";

const limits = {
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
};

async function plan() {
  return await createCandidateDemoPlan({
    limits,
    preflightEvidenceSha256,
    preflightObservedAt,
    sourcePlan: await syntheticSamplePublicationPlan(),
    targets: {
      openData: {
        bucket: "candidate-prism-open-data-demo",
        ipnsLabel: "candidate-prism-open-data-demo",
        ipnsNetworkKey: keyA,
        priorCid: priorOpen,
      },
      queryTable: {
        bucket: "candidate-prism-query-table-demo",
        ipnsLabel: "candidate-prism-query-table-demo",
        ipnsNetworkKey: keyB,
        priorCid: priorQuery,
      },
    },
  });
}

describe("candidate-owned Filebase demo boundary", () => {
  it("classifies bounded control-plane and independent public evidence", () => {
    const observations = ["filebase_control", "ipfs_io", "dweb_link"].map(
      (resolver, index) => ({
        cacheAgeSeconds: index === 0 ? null : 0,
        httpStatus: 200,
        observedAt: `2026-08-30T00:00:0${index}.000Z`,
        observedCid: priorOpen,
        ordinal: index + 1,
        outcome: "resolved",
        resolver,
        resolverType: index === 0 ? "control_plane" : "public_resolver",
        responseBytes: 0,
        responseSha256: "a".repeat(64),
      }),
    );
    expect(
      classifyCandidateResolutionObservations({
        observations,
        priorCid: priorOpen,
        targetCid: priorQuery,
      }),
    ).toBe("prior_observed");
    expect(
      classifyCandidateResolutionObservations({
        observations: observations.map((entry, index) => ({
          ...entry,
          observedCid: index === 2 ? priorQuery : entry.observedCid,
        })),
        priorCid: priorOpen,
        targetCid: priorQuery,
      }),
    ).toBe("split");
    expect(
      classifyCandidateResolutionObservations({
        observations: observations.map((entry, index) =>
          index === 2
            ? {
                ...entry,
                httpStatus: 503,
                observedCid: null,
                outcome: "http_error",
              }
            : entry,
        ),
        priorCid: priorOpen,
        targetCid: priorQuery,
      }),
    ).toBe("unavailable");
  });
  it("is disabled by default and validates only fixed Filebase origins", () => {
    expect(loadCandidateDemoConfig({})).toEqual({ enabled: false });
    const accessKey = "synthetic-access";
    const secretKey = "synthetic-secret";
    const base = {
      CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
      CANDIDATE_DEMO_FILEBASE_API_ENDPOINT: "https://api.filebase.io",
      CANDIDATE_DEMO_FILEBASE_API_TOKEN: Buffer.from(
        `${accessKey}:${secretKey}`,
      ).toString("base64"),
      CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID: accessKey,
      CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY: secretKey,
      CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.com",
      CANDIDATE_DEMO_OPEN_DATA_BUCKET: "candidate-prism-open-data-demo",
      CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL: "candidate-prism-open-data-demo",
      CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY: keyA,
      CANDIDATE_DEMO_QUERY_TABLE_BUCKET: "candidate-prism-query-table-demo",
      CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL: "candidate-prism-query-table-demo",
      CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY: keyB,
      CANDIDATE_DEMO_MAX_BUDGET_USD: "10",
      CANDIDATE_DEMO_MAX_CONCURRENCY: "2",
      CANDIDATE_DEMO_MAX_OBJECT_BYTES: "2097152",
      CANDIDATE_DEMO_MAX_OBJECTS: "100",
      CANDIDATE_DEMO_MAX_REQUESTS: "1000",
      CANDIDATE_DEMO_MAX_RETRIES: "1",
      CANDIDATE_DEMO_MAX_TOTAL_BYTES: "10485760",
      CANDIDATE_DEMO_REQUEST_TIMEOUT_MS: "2000",
      CANDIDATE_DEMO_REQUEST_USD_PER_1000: "0.01",
      CANDIDATE_DEMO_STORAGE_USD_PER_GIB: "0.1",
    };
    expect(loadCandidateDemoConfig(base)).toMatchObject({ enabled: true });
    expect(
      loadCandidateDemoConfig({
        ...base,
        CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.io",
      }),
    ).toMatchObject({
      apiEndpoint: "https://api.filebase.io",
      s3Endpoint: "https://s3.filebase.io",
    });
    expect(() =>
      loadCandidateDemoConfig({
        ...base,
        CANDIDATE_DEMO_FILEBASE_API_ENDPOINT: "https://api.filebase.com",
      }),
    ).toThrow("supported Filebase endpoint");
    expect(() =>
      loadCandidateDemoConfig({
        ...base,
        CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://example.invalid",
      }),
    ).toThrow("supported Filebase endpoint");
  });

  it("binds candidate targets and preserves explicit sample status", async () => {
    const candidate = await plan();
    expect(candidate).toMatchObject({
      coverageMode: "sample",
      disclaimer: CANDIDATE_DEMO_WORDING,
      version: "1.0.0",
    });
    expect(candidate.targets.openData.bucket).not.toBe(
      candidate.targets.queryTable.bucket,
    );
    expect(candidate.objects).toContainEqual(
      expect.objectContaining({
        domain: "open_data",
        objectKey: "publication-dry-run-plan.json",
      }),
    );
  });

  it("requires exact local and provider CIDs before returning a receipt", async () => {
    const bytes = Buffer.from('{"synthetic":true}\n');
    const cid = await calculateIpfsCid(bytes);
    const transport: FilebaseCandidateTransport = {
      resolve: vi.fn(async () => cid),
      update: vi.fn(async (target) => ({
        cid: target.targetCid,
        label: target.ipnsLabel,
        networkKey: target.ipnsNetworkKey,
      })),
      upload: vi.fn(async () => ({
        cid,
        providerRequestIdHash: "a".repeat(64),
      })),
    };
    const executor = new CandidateDemoFilebaseExecutor(
      await plan(),
      transport,
      {
        assertIpnsMutationReady: vi.fn(async () => undefined),
        beforeIpnsMutation: vi.fn(async () => undefined),
        beforeIpnsRollback: vi.fn(async () => undefined),
        beforeUpload: vi.fn(async () => undefined),
        recordIpnsVerified: vi.fn(async () => undefined),
        recordIpnsRolledBack: vi.fn(async () => undefined),
        recordUpload: vi.fn(async () => undefined),
      },
      { verify: vi.fn(async () => undefined) },
    );
    await expect(
      executor.upload({
        bytes,
        domain: "open_data",
        expectedCid: cid,
        objectKey: "properties/property_synthetic.json",
        sha256: sha256(bytes),
      }),
    ).resolves.toMatchObject({ cid, providerRequestIdHash: "a".repeat(64) });
    vi.mocked(transport.upload).mockResolvedValueOnce({
      cid: "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
      providerRequestIdHash: null,
    });
    await expect(
      executor.upload({
        bytes,
        domain: "open_data",
        expectedCid: cid,
        objectKey: "properties/property_synthetic.json",
        sha256: sha256(bytes),
      }),
    ).rejects.toThrow("missing or mismatched CID");
  });

  it("rejects configured object, request, byte, and budget overruns", async () => {
    const sourcePlan = await syntheticSamplePublicationPlan();
    expect(() =>
      createCandidateDemoPlan({
        limits: { ...limits, maxObjects: 1 },
        preflightEvidenceSha256,
        preflightObservedAt,
        sourcePlan,
        targets: {
          openData: {
            bucket: "candidate-prism-open-data-demo",
            ipnsLabel: "candidate-prism-open-data-demo",
            ipnsNetworkKey: keyA,
            priorCid: priorOpen,
          },
          queryTable: {
            bucket: "candidate-prism-query-table-demo",
            ipnsLabel: "candidate-prism-query-table-demo",
            ipnsNetworkKey: keyB,
            priorCid: priorQuery,
          },
        },
      }),
    ).rejects.toThrow("hard limit");
  });

  it("copies only verified bytes into a separate deterministic candidate directory", async () => {
    const dataDir = await mkdtemp(path.join(tmpdir(), "candidate-artifacts-"));
    try {
      const sourcePlan = await syntheticSamplePublicationPlan();
      const candidate = await createCandidateDemoPlan({
        limits,
        preflightEvidenceSha256,
        preflightObservedAt,
        sourcePlan,
        targets: {
          openData: {
            bucket: "candidate-prism-open-data-demo",
            ipnsLabel: "candidate-prism-open-data-demo",
            ipnsNetworkKey: keyA,
            priorCid: priorOpen,
          },
          queryTable: {
            bucket: "candidate-prism-query-table-demo",
            ipnsLabel: "candidate-prism-query-table-demo",
            ipnsNetworkKey: keyB,
            priorCid: priorQuery,
          },
        },
      });
      const sourceRoot = path.join(dataDir, "artifacts", "source-plan");
      const bytesByKey = new Map<string, Buffer>([
        [
          "properties/property_synthetic.json",
          Buffer.from('{"synthetic":true}\n'),
        ],
        ["shards/shard-0000.json", Buffer.from('{"syntheticShard":true}\n')],
        ["index.json", Buffer.from('{"syntheticRoot":true}\n')],
        ["manifest.json", Buffer.from('{"syntheticManifest":true}\n')],
        ["coverage.json", Buffer.from('{"syntheticCoverage":true}\n')],
        ["provenance.json", Buffer.from('{"syntheticProvenance":true}\n')],
        [
          "query-tables/pasco/query-table.parquet",
          Buffer.from("PAR1syntheticPAR1"),
        ],
        [
          "publication-dry-run-plan.json",
          Buffer.from(`${canonicalJson(sourcePlan)}\n`),
        ],
      ]);
      for (const object of candidate.objects) {
        const filePath =
          object.objectKey === "publication-dry-run-plan.json"
            ? path.join(sourceRoot, object.objectKey)
            : object.domain === "open_data"
              ? path.join(sourceRoot, "open-data", object.objectKey)
              : path.join(sourceRoot, "query", object.objectKey);
        await mkdir(path.dirname(filePath), { recursive: true });
        await writeFile(filePath, bytesByKey.get(object.objectKey)!);
      }
      const output = await materializeCandidateDemoArtifacts({
        dataDir,
        plan: candidate,
        sourceOutputRoot: path.relative(dataDir, sourceRoot),
      });
      expect(output).toBe(
        path.join(
          "artifacts",
          "candidate-demo",
          "pasco",
          "plans",
          candidate.demoPlanId,
        ),
      );
      expect(
        await readFile(
          path.join(dataDir, output, "open-data", "index.json"),
          "utf8",
        ),
      ).toBe('{"syntheticRoot":true}\n');
      await expect(
        materializeCandidateDemoArtifacts({
          dataDir,
          plan: candidate,
          sourceOutputRoot: "../outside",
        }),
      ).rejects.toThrow();
    } finally {
      await rm(dataDir, { force: true, recursive: true });
    }
  });
});
