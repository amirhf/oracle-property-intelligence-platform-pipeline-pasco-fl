import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  type S3Client,
} from "@aws-sdk/client-s3";
import { describe, expect, it } from "vitest";

import {
  canonicalJson,
  canonicalJsonSha256,
} from "../../src/lib/canonical-json.js";
import { sha256 } from "../../src/lib/hash.js";
import {
  DELEGATED_IPNS_POLICY_VERSION,
  type observeDelegatedIpnsRecord,
} from "../../src/publication/delegated-ipns.js";
import {
  candidateSourceSnapshotPreflightBinding,
  CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
  CANDIDATE_SOURCE_SNAPSHOT_GATEWAY_ORIGIN,
  CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS,
  CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
  loadCandidateSourceSnapshotPreflightConfig,
  readProtectedControlObject,
  runCandidateSourceSnapshotReadOnlyPreflight,
  validateCandidateSourceSnapshotPreflightEvidence,
  writeCandidateSourceSnapshotPreflightEvidence,
  type CandidateSourceSnapshotPreflightConfig,
} from "../../src/publication/candidate-source-snapshot-preflight.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS,
  CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS,
} from "../../src/publication/candidate-source-snapshot-preflight-binding.js";
import { PROTECTED_CANDIDATE_SAMPLE_ROLLBACK } from "../../src/publication/candidate-source-snapshot-demo.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";

const observedAt = "2026-08-31T00:00:00.000Z";
const targetOpenCid = CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData;
const targetQueryCid = CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable;
const unexpectedCid = `Qm${"c".repeat(44)}`;

function config(
  overrides: Partial<CandidateSourceSnapshotPreflightConfig> = {},
): CandidateSourceSnapshotPreflightConfig {
  return {
    apiEndpoint: CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
    executorEnabled: false,
    limits: {
      maxBudgetUsd: 25,
      maxConcurrency: 4,
      maxObjectBytes: 100_000_000,
      maxObjects: 300_000,
      maxRequests: 100,
      maxRetries: 0,
      maxTotalBytes: 4_000_000_000,
      requestTimeoutMs: 1_000,
    },
    s3AccessKeyId: "synthetic-access-key",
    s3Endpoint: CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
    s3SecretAccessKey: "synthetic-secret-key",
    targets: {
      openData: {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
        ipnsLabel: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey,
      },
      queryTable: {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
        ipnsLabel:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey,
      },
    },
    ...overrides,
  };
}

function nameRecord(options: {
  cid: string;
  label: string;
  networkKey: string;
}) {
  return {
    cid: options.cid,
    created_at: observedAt,
    enabled: true,
    label: options.label,
    network_key: options.networkKey,
    published_at: observedAt,
    sequence: 7,
    updated_at: observedAt,
  };
}

function allNames(value: CandidateSourceSnapshotPreflightConfig) {
  return [
    nameRecord({
      cid: targetOpenCid,
      label: value.targets.openData.ipnsLabel,
      networkKey: value.targets.openData.ipnsNetworkKey,
    }),
    nameRecord({
      cid: targetQueryCid,
      label: value.targets.queryTable.ipnsLabel,
      networkKey: value.targets.queryTable.ipnsNetworkKey,
    }),
    nameRecord({
      cid: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.targetCid,
      label: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsLabel,
      networkKey: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsNetworkKey,
    }),
    nameRecord({
      cid: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.targetCid,
      label: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsLabel,
      networkKey: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsNetworkKey,
    }),
  ];
}

interface S3Call {
  bucket: string;
  maxKeys?: number;
  operation: "HeadBucket" | "HeadObject" | "ListObjectsV2";
  prefix?: string;
}

function s3Mock(options?: {
  listForPrefix?: (prefix: string) => {
    contents?: { Key?: string; Size?: number }[];
    isTruncated?: boolean;
    nextContinuationToken?: string;
  };
}) {
  const calls: S3Call[] = [];
  const client = {
    send: async (command: unknown) => {
      if (command instanceof HeadBucketCommand) {
        calls.push({
          bucket: command.input.Bucket!,
          operation: "HeadBucket",
        });
        return { $metadata: { attempts: 1, httpStatusCode: 200 } };
      }
      if (command instanceof ListObjectsV2Command) {
        const prefix = command.input.Prefix!;
        calls.push({
          bucket: command.input.Bucket!,
          maxKeys: command.input.MaxKeys!,
          operation: "ListObjectsV2",
          prefix,
        });
        const listing = options?.listForPrefix?.(prefix) ?? {};
        const contents = listing.contents ?? [
          { Key: "bootstrap/read-only-proof.json", Size: 1 },
        ];
        return {
          $metadata: { attempts: 1, httpStatusCode: 200 },
          Contents: contents,
          IsTruncated: listing.isTruncated ?? false,
          KeyCount: contents.length,
          NextContinuationToken: listing.nextContinuationToken,
        };
      }
      if (command instanceof HeadObjectCommand) {
        calls.push({
          bucket: command.input.Bucket!,
          operation: "HeadObject",
        });
        return {
          $metadata: { attempts: 1, httpStatusCode: 200 },
          Metadata: { cid: targetOpenCid },
        };
      }
      throw new Error("Unexpected S3 operation");
    },
  };
  return { calls, client: client as unknown as S3Client };
}

function protectedControlMock() {
  return async (options: {
    cid: string;
    role: "manifest" | "plan";
    sha256: string;
  }) => {
    const value = {
      cid: options.cid,
      httpStatus: 200,
      observedAt,
      requestCount: 1,
      responseBytes: 1,
      responseSha256: options.sha256,
      role: options.role,
      sha256: options.sha256,
      status: "cid_and_sha256_verified" as const,
    };
    return { ...value, evidenceSha256: canonicalJsonSha256(value) };
  };
}

type DelegatedObserver = typeof observeDelegatedIpnsRecord;

function delegatedMock(options?: {
  mismatchNetworkKey?: string;
  requests?: number;
}) {
  const calls: string[] = [];
  const observe: DelegatedObserver = async (input) => {
    calls.push(input.networkKey);
    const mismatch = input.networkKey === options?.mismatchNetworkKey;
    return {
      endpointType: "ipfs_delegated_routing_v1",
      httpStatus: 200,
      latencyMs: 1,
      observedAt,
      observedCid: mismatch ? unexpectedCid : input.expectedTargetCid,
      outcome: mismatch ? "unexpected_cid" : "validated",
      requestCount: options?.requests ?? 1,
      responseBytes: 128,
      responseSha256: "d".repeat(64),
      schemaVersion: DELEGATED_IPNS_POLICY_VERSION,
      sequence: "7",
      ttlNanoseconds: "60000000000",
      validationResult: mismatch ? "unexpected_cid" : "valid_target",
      validity: "2026-09-01T00:00:00.000000000Z",
    };
  };
  return { calls, observe };
}

function expectedCidByNetworkKey(
  value: CandidateSourceSnapshotPreflightConfig,
) {
  return new Map([
    [value.targets.openData.ipnsNetworkKey, targetOpenCid],
    [value.targets.queryTable.ipnsNetworkKey, targetQueryCid],
    [
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsNetworkKey,
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.targetCid,
    ],
    [
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsNetworkKey,
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.targetCid,
    ],
  ]);
}

function fetchMock(options: {
  config: CandidateSourceSnapshotPreflightConfig;
  gatewayContradiction?: string;
  gatewayUnavailable?: string;
  names?: unknown;
  namesFailures?: number;
}) {
  const calls: Array<{
    authorization: string | null;
    method: string;
    url: string;
  }> = [];
  let namesAttempts = 0;
  const expected = expectedCidByNetworkKey(options.config);
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    calls.push({
      authorization: new Headers(init?.headers).get("authorization"),
      method: init?.method ?? "GET",
      url: url.toString(),
    });
    if (
      url.origin === CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT &&
      url.pathname === "/v1/names"
    ) {
      namesAttempts += 1;
      if (namesAttempts <= (options.namesFailures ?? 0)) {
        return new Response(null, { status: 503 });
      }
      return Response.json(options.names ?? allNames(options.config));
    }
    if (
      url.origin === CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT &&
      url.pathname === "/v1/usage"
    ) {
      return Response.json({
        bandwidth: { bytes: 2_000 },
        storage: { bytes: 3_000 },
      });
    }
    if (
      url.origin === CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT &&
      url.pathname.startsWith("/v1/usage/storage/")
    ) {
      return Response.json({ storage: { bytes: 0 } });
    }
    if (url.origin === CANDIDATE_SOURCE_SNAPSHOT_GATEWAY_ORIGIN) {
      const networkKey = url.pathname.split("/").at(-1)!;
      if (networkKey === options.gatewayUnavailable) {
        return new Response(null, { status: 503 });
      }
      const cid =
        networkKey === options.gatewayContradiction
          ? unexpectedCid
          : expected.get(networkKey);
      if (!cid) throw new Error("Unexpected gateway identity");
      return new Response(null, {
        headers: { "x-ipfs-path": `/ipfs/${cid}` },
        status: 301,
      });
    }
    throw new Error("Unexpected HTTP endpoint");
  };
  return { calls, fetchImpl };
}

function environment() {
  const accessKey = "synthetic-access-key";
  const secretKey = "synthetic-secret-key";
  const value = config();
  return {
    CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID: accessKey,
    CANDIDATE_DEMO_FILEBASE_API_ENDPOINT:
      CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
    CANDIDATE_DEMO_FILEBASE_API_TOKEN: Buffer.from(
      `${accessKey}:${secretKey}`,
    ).toString("base64"),
    CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
    CANDIDATE_DEMO_FILEBASE_SECRET_ACCESS_KEY: secretKey,
    CANDIDATE_DEMO_MAX_BUDGET_USD: "25",
    CANDIDATE_DEMO_MAX_CONCURRENCY: "4",
    CANDIDATE_DEMO_MAX_OBJECT_BYTES: "100000000",
    CANDIDATE_DEMO_MAX_OBJECTS: "300000",
    CANDIDATE_DEMO_MAX_REQUESTS: "100",
    CANDIDATE_DEMO_MAX_RETRIES: "0",
    CANDIDATE_DEMO_MAX_TOTAL_BYTES: "4000000000",
    CANDIDATE_DEMO_OPEN_DATA_BUCKET: value.targets.openData.bucket,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_LABEL: value.targets.openData.ipnsLabel,
    CANDIDATE_DEMO_OPEN_DATA_IPNS_NETWORK_KEY:
      value.targets.openData.ipnsNetworkKey,
    CANDIDATE_DEMO_QUERY_TABLE_BUCKET: value.targets.queryTable.bucket,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_LABEL: value.targets.queryTable.ipnsLabel,
    CANDIDATE_DEMO_QUERY_TABLE_IPNS_NETWORK_KEY:
      value.targets.queryTable.ipnsNetworkKey,
    CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "false",
    CANDIDATE_DEMO_REQUEST_TIMEOUT_MS: "1000",
  } satisfies NodeJS.ProcessEnv;
}

describe("candidate source-snapshot read-only preflight", () => {
  it("allows only bounded reads and emits canonical sanitized plan evidence", async () => {
    const value = config();
    const s3 = s3Mock({
      listForPrefix: () => ({
        contents: [{ Key: "bootstrap/read-only-proof.json", Size: 17 }],
      }),
    });
    const http = fetchMock({ config: value });
    const delegated = delegatedMock();
    const evidence = await runCandidateSourceSnapshotReadOnlyPreflight({
      config: value,
      fetchImpl: http.fetchImpl,
      observeDelegated: delegated.observe,
      readProtectedControl: protectedControlMock(),
      retryDelay: async () => undefined,
      s3Client: s3.client,
      startedAt: observedAt,
    });
    const binding = candidateSourceSnapshotPreflightBinding(evidence);

    expect(evidence.status).toBe("ready_for_source_snapshot_planning");
    expect(evidence.capacityProfile.subscriptionTierEvidence).toBe(
      "human_confirmation_required",
    );
    expect(binding.capacityProfile).toMatchObject({
      accountBandwidthBytes: 2_000,
      accountStorageBytes: 3_000,
      subscriptionTierStatus: "human_confirmation_required",
    });
    expect(binding.buckets).toEqual([
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
        conflictingObjectCount: 0,
        domain: "open_data",
        headStatus: "authenticated",
        prefixStatus: "no_conflicting_publication_prefixes",
        storageNetworkStatus: "ipfs_provider_cid_verified",
      },
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
        conflictingObjectCount: 0,
        domain: "query_table",
        headStatus: "authenticated",
        prefixStatus: "no_conflicting_publication_prefixes",
        storageNetworkStatus: "ipfs_provider_cid_verified",
      },
    ]);
    expect(binding.identities).toEqual([
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
        controlCid: targetOpenCid,
        domain: "open_data",
        ipnsLabel: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey,
        officialGatewayCid: targetOpenCid,
        signedRecordCid: targetOpenCid,
      },
      {
        bucket: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
        controlCid: targetQueryCid,
        domain: "query_table",
        ipnsLabel:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel,
        ipnsNetworkKey:
          CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey,
        officialGatewayCid: targetQueryCid,
        signedRecordCid: targetQueryCid,
      },
    ]);
    expect(evidence.protectedSampleRollback).toMatchObject({
      manifest: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest,
      plan: PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan,
      status: "identity_and_control_objects_verified_for_rollback",
    });

    expect(s3.calls.map((call) => call.operation)).toEqual([
      "HeadBucket",
      "ListObjectsV2",
      "HeadObject",
      "HeadBucket",
      "ListObjectsV2",
      "HeadObject",
      "HeadBucket",
      "HeadBucket",
    ]);
    expect(
      s3.calls.filter((call) => call.operation === "ListObjectsV2"),
    ).toEqual([
      {
        bucket: value.targets.openData.bucket,
        maxKeys: CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS,
        operation: "ListObjectsV2",
        prefix: "",
      },
      {
        bucket: value.targets.queryTable.bucket,
        maxKeys: CANDIDATE_SOURCE_SNAPSHOT_MAX_LIST_KEYS,
        operation: "ListObjectsV2",
        prefix: "",
      },
    ]);
    expect(new Set(http.calls.map((call) => call.method))).toEqual(
      new Set(["GET", "HEAD"]),
    );
    expect(http.calls.filter((call) => call.method === "GET")).toHaveLength(4);
    expect(http.calls.filter((call) => call.method === "HEAD")).toHaveLength(4);
    expect(delegated.calls).toHaveLength(4);
    const observedRequests =
      s3.calls.length + http.calls.length + delegated.calls.length + 2;
    expect(evidence.requestCount).toBe(observedRequests);
    expect(evidence.readPolicy.logicalOperationCount).toBe(observedRequests);
    expect(evidence.readPolicy.maximumRequestCount).toBe(observedRequests);

    const serialized = canonicalJson(evidence);
    expect(serialized).not.toContain("https://");
    expect(serialized).not.toContain(value.s3AccessKeyId);
    expect(serialized).not.toContain(value.s3SecretAccessKey);
    expect(serialized).not.toContain("bootstrap/read-only-proof.json");
    expect(validateCandidateSourceSnapshotPreflightEvidence(evidence)).toEqual(
      evidence,
    );
  });

  it("rejects conflicting or incomplete prefix listings without exposing keys", async () => {
    const value = config();
    const conflictingKey =
      "publications/source-snapshot-demo-v1/existing/private-name.json";
    const conflict = s3Mock({
      listForPrefix: () => ({
        contents: [{ Key: conflictingKey, Size: 1 }],
      }),
    });
    const http = fetchMock({ config: value });
    const delegated = delegatedMock();
    let conflictError: unknown;
    try {
      await runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: http.fetchImpl,
        observeDelegated: delegated.observe,
        readProtectedControl: protectedControlMock(),
        s3Client: conflict.client,
      });
    } catch (error) {
      conflictError = error;
    }
    expect(conflictError).toBeInstanceOf(Error);
    expect((conflictError as Error).message).toContain("conflicting");
    expect((conflictError as Error).message).not.toContain(conflictingKey);
    expect(http.calls).toHaveLength(0);

    for (const additionalConflict of [
      "publication-control/source-snapshot-demo-v1/existing/control.json",
      "query-tables/source-snapshot-demo-v1/existing/table.parquet",
    ]) {
      const additional = s3Mock({
        listForPrefix: (prefix) => ({
          contents: additionalConflict.startsWith(prefix)
            ? [{ Key: additionalConflict, Size: 1 }]
            : [],
        }),
      });
      await expect(
        runCandidateSourceSnapshotReadOnlyPreflight({
          config: value,
          fetchImpl: http.fetchImpl,
          observeDelegated: delegated.observe,
          readProtectedControl: protectedControlMock(),
          s3Client: additional.client,
        }),
      ).rejects.toThrow("conflicting candidate publication prefix");
    }

    const truncated = s3Mock({
      listForPrefix: () => ({
        contents: [],
        isTruncated: true,
        nextContinuationToken: "opaque-token",
      }),
    });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: http.fetchImpl,
        observeDelegated: delegated.observe,
        readProtectedControl: protectedControlMock(),
        s3Client: truncated.client,
      }),
    ).rejects.toThrow("not a complete single page");
  });

  it("treats a protected gateway outage as diagnostic but rejects contradictions", async () => {
    const value = config();
    const unavailableKey =
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData.ipnsNetworkKey;
    const unavailable = fetchMock({
      config: value,
      gatewayUnavailable: unavailableKey,
    });
    const s3 = s3Mock();
    const delegated = delegatedMock();
    const evidence = await runCandidateSourceSnapshotReadOnlyPreflight({
      config: value,
      fetchImpl: unavailable.fetchImpl,
      observeDelegated: delegated.observe,
      readProtectedControl: protectedControlMock(),
      retryDelay: async () => undefined,
      s3Client: s3.client,
    });
    expect(
      evidence.protectedSampleRollback.identities.find(
        (identity) => identity.ipnsNetworkKey === unavailableKey,
      )?.officialGateway.outcome,
    ).toBe("unavailable_diagnostic");

    const contradiction = fetchMock({
      config: value,
      gatewayContradiction: unavailableKey,
    });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: contradiction.fetchImpl,
        observeDelegated: delegated.observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("contradicts the expected CID");
  });

  it("requires target gateway and signed-record agreement", async () => {
    const value = config();
    const targetKey = value.targets.openData.ipnsNetworkKey;
    const gatewayUnavailable = fetchMock({
      config: value,
      gatewayUnavailable: targetKey,
    });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: gatewayUnavailable.fetchImpl,
        observeDelegated: delegatedMock().observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("target gateway is unavailable");

    const signedMismatch = delegatedMock({ mismatchNetworkKey: targetKey });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: fetchMock({ config: value }).fetchImpl,
        observeDelegated: signedMismatch.observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("Signed IPNS record does not match");
  });

  it("rejects a unanimously observed target CID that is not the immutable prior", async () => {
    const value = config();
    const targetKey = value.targets.openData.ipnsNetworkKey;
    const changedNames = allNames(value).map((entry) =>
      entry.network_key === targetKey
        ? { ...entry, cid: unexpectedCid }
        : entry,
    );
    const http = fetchMock({
      config: value,
      gatewayContradiction: targetKey,
      names: changedNames,
    });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: http.fetchImpl,
        observeDelegated: delegatedMock({
          mismatchNetworkKey: targetKey,
        }).observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("control plane contradicts the expected CID");
  });

  it("requires the frozen rollback control and signed-record CIDs", async () => {
    const value = config();
    const protectedKey =
      PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable.ipnsNetworkKey;
    const changedNames = allNames(value).map((entry) =>
      entry.network_key === protectedKey
        ? { ...entry, cid: unexpectedCid }
        : entry,
    );
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: fetchMock({ config: value, names: changedNames }).fetchImpl,
        observeDelegated: delegatedMock().observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("control plane contradicts the expected CID");

    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: value,
        fetchImpl: fetchMock({ config: value }).fetchImpl,
        observeDelegated: delegatedMock({
          mismatchNetworkKey: protectedKey,
        }).observe,
        readProtectedControl: protectedControlMock(),
        retryDelay: async () => undefined,
        s3Client: s3Mock().client,
      }),
    ).rejects.toThrow("Signed IPNS record does not match");
  });

  it("hashes and CID-verifies bounded protected control-object bytes", async () => {
    const bytes = Buffer.from("synthetic protected control object\n", "utf8");
    const expectedCid = await calculateIpfsCid(bytes);
    const expectedSha256 = sha256(bytes);
    const evidence = await readProtectedControlObject({
      cid: expectedCid,
      config: config(),
      fetchImpl: async () =>
        new Response(bytes, {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      retryDelay: async () => undefined,
      role: "manifest",
      sha256: expectedSha256,
    });
    expect(evidence).toMatchObject({
      cid: expectedCid,
      responseBytes: bytes.byteLength,
      responseSha256: expectedSha256,
      role: "manifest",
      sha256: expectedSha256,
      status: "cid_and_sha256_verified",
    });

    await expect(
      readProtectedControlObject({
        cid: expectedCid,
        config: config(),
        fetchImpl: async () => new Response(Buffer.from("changed\n")),
        retryDelay: async () => undefined,
        role: "manifest",
        sha256: expectedSha256,
      }),
    ).rejects.toThrow("SHA-256 mismatch");
  });

  it("derives retry accounting and rejects an insufficient request ceiling before reads", async () => {
    const value = config({
      limits: { ...config().limits, maxRetries: 1 },
    });
    const http = fetchMock({ config: value, namesFailures: 1 });
    const s3 = s3Mock();
    const delegated = delegatedMock();
    const evidence = await runCandidateSourceSnapshotReadOnlyPreflight({
      config: value,
      fetchImpl: http.fetchImpl,
      observeDelegated: delegated.observe,
      readProtectedControl: protectedControlMock(),
      retryDelay: async () => undefined,
      s3Client: s3.client,
    });
    expect(evidence.names.requestCount).toBe(2);
    expect(evidence.requestCount).toBe(
      s3.calls.length + http.calls.length + delegated.calls.length + 2,
    );
    expect(evidence.readPolicy.maximumRequestCount).toBe(
      evidence.readPolicy.logicalOperationCount * 2,
    );

    const tooSmall = config({
      limits: { ...config().limits, maxRequests: 10 },
    });
    const noReadsS3 = s3Mock();
    const noReadsHttp = fetchMock({ config: tooSmall });
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: tooSmall,
        fetchImpl: noReadsHttp.fetchImpl,
        observeDelegated: delegated.observe,
        readProtectedControl: protectedControlMock(),
        s3Client: noReadsS3.client,
      }),
    ).rejects.toThrow("configured request ceiling");
    expect(noReadsS3.calls).toHaveLength(0);
    expect(noReadsHttp.calls).toHaveLength(0);
  });

  it("loads only the exact disabled endpoints and writes immutable DATA_DIR evidence", async () => {
    const env = environment();
    expect(loadCandidateSourceSnapshotPreflightConfig(env)).toMatchObject({
      apiEndpoint: CANDIDATE_SOURCE_SNAPSHOT_API_ENDPOINT,
      executorEnabled: false,
      s3Endpoint: CANDIDATE_SOURCE_SNAPSHOT_S3_ENDPOINT,
    });
    expect(() =>
      loadCandidateSourceSnapshotPreflightConfig({
        ...env,
        CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true",
      }),
    ).toThrow("requires CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=false");
    expect(() =>
      loadCandidateSourceSnapshotPreflightConfig({
        ...env,
        CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.io",
      }),
    ).toThrow("compiled S3 endpoint");

    const value = config();
    const evidence = await runCandidateSourceSnapshotReadOnlyPreflight({
      config: value,
      fetchImpl: fetchMock({ config: value }).fetchImpl,
      observeDelegated: delegatedMock().observe,
      readProtectedControl: protectedControlMock(),
      retryDelay: async () => undefined,
      s3Client: s3Mock().client,
      startedAt: observedAt,
    });
    const dataDir = await mkdtemp(
      path.join(tmpdir(), "candidate-source-snapshot-preflight-"),
    );
    const relativePath = await writeCandidateSourceSnapshotPreflightEvidence({
      dataDir,
      evidence,
    });
    expect(relativePath).toBe(
      path.join(
        "evidence",
        "candidate-source-snapshot-demo",
        "read-only-preflight",
        `${evidence.evidenceSha256}.json`,
      ),
    );
    expect(await readFile(path.join(dataDir, relativePath), "utf8")).toBe(
      `${canonicalJson(evidence)}\n`,
    );
    await expect(
      writeCandidateSourceSnapshotPreflightEvidence({ dataDir, evidence }),
    ).resolves.toBe(relativePath);
  });

  it("rejects an unsafe runtime config before constructing any transport", async () => {
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: {
          ...config(),
          executorEnabled: true,
        } as unknown as CandidateSourceSnapshotPreflightConfig,
      }),
    ).rejects.toThrow("configuration is unsafe");
    await expect(
      runCandidateSourceSnapshotReadOnlyPreflight({
        config: {
          ...config(),
          apiEndpoint: "https://example.invalid",
        } as unknown as CandidateSourceSnapshotPreflightConfig,
      }),
    ).rejects.toThrow("configuration is unsafe");
  });
});
