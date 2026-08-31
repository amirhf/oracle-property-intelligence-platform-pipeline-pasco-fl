import { Readable } from "node:stream";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  type HeadObjectCommandOutput,
  type PutObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { describe, expect, it, vi } from "vitest";

import { sha256 } from "../../src/lib/hash.js";
import { loadCandidateSourceSnapshotExecutionConfig } from "../../src/publication/candidate-source-snapshot-executor-config.js";
import {
  BoundCandidateSourceSnapshotLocalObjectSource,
  AwsCandidateSourceSnapshotS3CommandExecutor,
  RealCandidateSourceSnapshotFilebaseTransport,
  createCandidateSourceSnapshotFilebaseTransport,
  type CandidateSourceSnapshotLocalObjectSource,
  type CandidateSourceSnapshotS3CommandExecutor,
  type CandidateSourceSnapshotS3CommandResult,
} from "../../src/publication/candidate-source-snapshot-filebase.js";
import type {
  CandidateSourceSnapshotDemoPlan,
  CandidateSourceSnapshotUploadObject,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { executeCandidateSourceSnapshotIpnsCutover } from "../../src/publication/candidate-source-snapshot-session2.js";
import { CandidateSourceSnapshotUploadError } from "../../src/publication/candidate-source-snapshot-upload.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function executionEnvironment(
  plan: CandidateSourceSnapshotDemoPlan,
): NodeJS.ProcessEnv {
  const access = "synthetic-access";
  const secret = "synthetic-secret";
  return {
    CANDIDATE_DEMO_FILEBASE_ACCESS_KEY_ID: access,
    CANDIDATE_DEMO_FILEBASE_API_ENDPOINT: "https://api.filebase.io",
    CANDIDATE_DEMO_FILEBASE_API_TOKEN: Buffer.from(
      `${access}:${secret}`,
    ).toString("base64"),
    CANDIDATE_DEMO_FILEBASE_S3_ENDPOINT: "https://s3.filebase.com",
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

function enabledConfig(plan: CandidateSourceSnapshotDemoPlan) {
  const config = loadCandidateSourceSnapshotExecutionConfig(
    executionEnvironment(plan),
    plan,
  );
  if (!config.enabled) throw new Error("test config did not enable");
  return config;
}

class FakeSource implements CandidateSourceSnapshotLocalObjectSource {
  readonly bytes: Buffer;
  readonly verify = vi.fn(async () => undefined);

  constructor(bytes: Buffer) {
    this.bytes = bytes;
  }

  async openVerifiedStream() {
    return {
      body: Readable.from(this.bytes),
      contentLength: this.bytes.byteLength,
      contentType: "application/json" as const,
    };
  }
}

class FakeS3 implements CandidateSourceSnapshotS3CommandExecutor {
  readonly commands: Array<HeadObjectCommand | PutObjectCommand> = [];
  next:
    | CandidateSourceSnapshotS3CommandResult<
        HeadObjectCommandOutput | PutObjectCommandOutput
      >
    | Error
    | { $metadata: { httpStatusCode: number } };

  constructor(
    next:
      | CandidateSourceSnapshotS3CommandResult<
          HeadObjectCommandOutput | PutObjectCommandOutput
        >
      | Error
      | { $metadata: { httpStatusCode: number } },
  ) {
    this.next = next;
  }

  async send(
    command: HeadObjectCommand,
    _signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<HeadObjectCommandOutput>>;
  async send(
    command: PutObjectCommand,
    _signal?: AbortSignal,
  ): Promise<CandidateSourceSnapshotS3CommandResult<PutObjectCommandOutput>>;
  async send(command: HeadObjectCommand | PutObjectCommand) {
    this.commands.push(command);
    if (this.next instanceof Error || !("output" in this.next)) {
      throw this.next;
    }
    return this.next;
  }
}

describe("candidate source-snapshot Session 2 boundary", () => {
  it("is fail-closed by default and rejects an inexact target or secret binding", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    expect(loadCandidateSourceSnapshotExecutionConfig({}, plan)).toStrictEqual({
      enabled: false,
    });
    const source = new FakeSource(Buffer.from("{}\n"));
    expect(() =>
      createCandidateSourceSnapshotFilebaseTransport({
        config: { enabled: false },
        source,
      }),
    ).toThrow("disabled");

    const wrongTarget = executionEnvironment(plan);
    wrongTarget.CANDIDATE_DEMO_QUERY_TABLE_TARGET_CID =
      plan.targets.openData.targetCid;
    expect(() =>
      loadCandidateSourceSnapshotExecutionConfig(wrongTarget, plan),
    ).toThrow("immutable plan");

    const wrongToken = executionEnvironment(plan);
    wrongToken.CANDIDATE_DEMO_FILEBASE_API_TOKEN = "not-derived";
    expect(() =>
      loadCandidateSourceSnapshotExecutionConfig(wrongToken, plan),
    ).toThrow("not derived");
  });

  it("binds source, generated payload/control, and plan objects to verified local streams", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const root = await mkdtemp(path.join(os.tmpdir(), "candidate-session2-"));
    const sourcePlanPath = path.join(root, "source", "publication-plan.json");
    const controlRoot = path.join(root, "controls");
    const planArtifactPath = path.join(root, "plan", "plan.json");
    await mkdir(path.dirname(sourcePlanPath), { recursive: true });
    await mkdir(controlRoot, { recursive: true });
    await mkdir(path.dirname(planArtifactPath), { recursive: true });
    await writeFile(sourcePlanPath, "{}\n");
    const cases = [
      {
        bytes: Buffer.from('{"kind":"source"}\n'),
        domain: "open_data" as const,
        logicalObjectKey: "properties/property_test.json",
        remoteObjectKey: `${plan.targets.openData.immutablePrefix}properties/property_test.json`,
        root: path.join(root, "source", "open-data"),
        path: path.join(
          root,
          "source",
          "open-data",
          "properties",
          "property_test.json",
        ),
      },
      {
        bytes: Buffer.from('{"kind":"replacement"}\n'),
        domain: "open_data" as const,
        logicalObjectKey: "coverage.json",
        remoteObjectKey: `${plan.targets.openData.immutablePrefix}coverage.json`,
        root: controlRoot,
      },
      {
        bytes: Buffer.from('{"kind":"control"}\n'),
        domain: "open_data" as const,
        logicalObjectKey: "object_inventory/index.json",
        remoteObjectKey: `${plan.targets.controlPrefix}object_inventory/index.json`,
        root: controlRoot,
      },
      {
        bytes: Buffer.from('{"kind":"plan"}\n'),
        domain: "open_data" as const,
        logicalObjectKey: "candidate-source-snapshot-plan.json",
        remoteObjectKey: `${plan.targets.controlPrefix}candidate-source-snapshot-plan.json`,
        root: path.dirname(planArtifactPath),
        path: planArtifactPath,
      },
    ];
    for (const item of cases) {
      const filePath =
        item.path ?? path.join(item.root, ...item.remoteObjectKey.split("/"));
      await mkdir(path.dirname(filePath), { recursive: true });
      await writeFile(filePath, item.bytes);
      Object.assign(item, { path: filePath });
    }
    const source = await BoundCandidateSourceSnapshotLocalObjectSource.create({
      controlRoot,
      plan,
      planArtifactPath,
      sourcePlanPath,
    });
    for (const item of cases) {
      const object: CandidateSourceSnapshotUploadObject = {
        byteSize: item.bytes.byteLength,
        domain: item.domain,
        expectedCid: await calculateIpfsCid(item.bytes),
        logicalObjectKey: item.logicalObjectKey,
        remoteObjectKey: item.remoteObjectKey,
        sha256: sha256(item.bytes),
      };
      const opened = await source.openVerifiedStream(object);
      const received: Buffer[] = [];
      for await (const chunk of opened.body) received.push(Buffer.from(chunk));
      expect(Buffer.concat(received)).toStrictEqual(item.bytes);
    }
  });

  it("performs one PutObject request and returns only bounded hashed receipt fields", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const object = objects[0]!;
    const fakeS3 = new FakeS3({
      output: {
        $metadata: { attempts: 1, requestId: "raw-provider-request-id" },
      },
      responseHeaders: {
        "content-length": "12",
        "x-amz-meta-cid": object.expectedCid,
      },
    });
    const source = new FakeSource(Buffer.from("{}\n"));
    const transport = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: fakeS3,
      source,
    });
    const receipt = await transport.uploadOnce(plan, object);
    expect(fakeS3.commands).toHaveLength(1);
    expect(fakeS3.commands[0]).toBeInstanceOf(PutObjectCommand);
    const command = fakeS3.commands[0] as PutObjectCommand;
    expect(command.input).toMatchObject({
      Bucket: plan.targets.openData.bucket,
      ContentLength: source.bytes.byteLength,
      Key: object.remoteObjectKey,
      Metadata: {
        "oracle-cid": object.expectedCid,
        "oracle-sha256": object.sha256,
      },
    });
    expect(receipt).toStrictEqual({
      providerCid: object.expectedCid,
      providerRequestIdHash: sha256("raw-provider-request-id"),
      responseBytes: 12,
    });
    expect(JSON.stringify(receipt)).not.toContain("raw-provider-request-id");
  });

  it("destroys only an owned S3 client and makes transport shutdown idempotent", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const destroy = vi
      .spyOn(S3Client.prototype, "destroy")
      .mockImplementation(() => undefined);
    const owned = new AwsCandidateSourceSnapshotS3CommandExecutor(
      enabledConfig(plan),
    );
    owned.close();
    owned.close();
    expect(destroy).toHaveBeenCalledOnce();
    destroy.mockRestore();

    const injected = new FakeS3({
      output: { $metadata: { attempts: 1 } },
      responseHeaders: {},
    });
    const injectedClose = vi.fn();
    Object.assign(injected, { close: injectedClose });
    const transport = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: injected,
      source: new FakeSource(Buffer.from("{}\n")),
    });
    transport.close();
    transport.close();
    expect(injectedClose).not.toHaveBeenCalled();
  });

  it("verifies HeadObject metadata and classifies bounded transient errors", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const object = objects[0]!;
    const source = new FakeSource(Buffer.from("{}\n"));
    const head = new FakeS3({
      output: {
        $metadata: { attempts: 1, requestId: "head-request" },
        ContentLength: object.byteSize,
        Metadata: {
          cid: object.expectedCid,
          "oracle-sha256": object.sha256,
        },
      },
      responseHeaders: {},
    });
    const transport = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: head,
      source,
    });
    await expect(
      transport.inspectExistingOnce(plan, object),
    ).resolves.toMatchObject({ outcome: "verified" });
    expect(head.commands).toHaveLength(1);

    for (const [failure, expected] of [
      [
        Object.assign(new Error("timeout"), { name: "AbortError" }),
        "timeout_unknown",
      ],
      [{ $metadata: { httpStatusCode: 503 } }, "retryable_http_error"],
    ] as const) {
      const failing = new RealCandidateSourceSnapshotFilebaseTransport({
        config: enabledConfig(plan),
        executor: new FakeS3(failure),
        source,
      });
      await expect(failing.uploadOnce(plan, object)).rejects.toMatchObject({
        outcome: expected,
      } satisfies Partial<CandidateSourceSnapshotUploadError>);
    }
  });

  it("keeps the legacy cutover shim fail closed without invoking a boundary", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const intents = [
      {
        domain: "open_data" as const,
        planId: plan.planId,
        planSha256: plan.planSha256,
        state: "prior_confirmed" as const,
      },
      {
        domain: "query_table" as const,
        planId: plan.planId,
        planSha256: plan.planSha256,
        state: "prior_confirmed" as const,
      },
    ];
    const calls: string[] = [];
    await expect(
      executeCandidateSourceSnapshotIpnsCutover({
        boundary: {
          mutateAndVerify: async (domain) => {
            calls.push(`update:${domain}`);
            return "verified";
          },
          rollbackAndVerify: async (domain) => {
            calls.push(`rollback:${domain}`);
            return "verified";
          },
        },
        intents,
        plan,
        unverifiedObjectCount: 0,
      }),
    ).rejects.toThrow("Legacy candidate IPNS cutover is disabled");
    expect(calls).toEqual([]);
  });
});
