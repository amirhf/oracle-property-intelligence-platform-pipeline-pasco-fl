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
  CandidateSourceSnapshotFilebaseTransportError,
  RealCandidateSourceSnapshotFilebaseTransport,
  createCandidateSourceSnapshotFilebaseTransport,
  resolveCandidateSourceSnapshotTransportLimits,
  type CandidateSourceSnapshotLocalObjectSource,
  type CandidateSourceSnapshotS3CommandExecutor,
  type CandidateSourceSnapshotS3CommandResult,
  type CandidateSourceSnapshotUploadBodyLease,
} from "../../src/publication/candidate-source-snapshot-filebase.js";
import type {
  CandidateSourceSnapshotDemoPlan,
  CandidateSourceSnapshotUploadObject,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { executeCandidateSourceSnapshotIpnsCutover } from "../../src/publication/candidate-source-snapshot-session2.js";
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

class FreshBodySource implements CandidateSourceSnapshotLocalObjectSource {
  readonly bodies: Array<Buffer | Readable> = [];
  readonly bytes: Buffer;
  readonly release = vi.fn(async () => undefined);
  readonly verify = vi.fn(async () => undefined);

  constructor(bytes: Buffer) {
    this.bytes = bytes;
  }

  async openVerifiedStream() {
    return {
      body: Readable.from(Buffer.from(this.bytes)),
      contentLength: this.bytes.byteLength,
      contentType: "application/json" as const,
    };
  }

  async openVerifiedUploadBody(): Promise<CandidateSourceSnapshotUploadBodyLease> {
    const body = Readable.from(Buffer.from(this.bytes));
    this.bodies.push(body);
    return {
      body,
      contentLength: this.bytes.byteLength,
      contentType: "application/json",
      release: this.release,
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

class SequencedS3 implements CandidateSourceSnapshotS3CommandExecutor {
  readonly commands: Array<HeadObjectCommand | PutObjectCommand> = [];
  readonly results: Array<
    CandidateSourceSnapshotS3CommandResult<PutObjectCommandOutput> | Error
  >;

  constructor(
    results: Array<
      CandidateSourceSnapshotS3CommandResult<PutObjectCommandOutput> | Error
    >,
  ) {
    this.results = [...results];
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
    if (command instanceof PutObjectCommand) {
      const body = command.input.Body;
      if (body instanceof Readable) {
        for await (const _chunk of body) {
          // Consume the one-attempt stream just as the SDK transport does.
        }
      }
    }
    const result = this.results.shift();
    if (!result) throw new Error("Synthetic S3 result queue exhausted");
    if (result instanceof Error) throw result;
    return result;
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

      const firstLease = await source.openVerifiedUploadBody(object);
      const secondLease = await source.openVerifiedUploadBody(object);
      expect(Buffer.isBuffer(firstLease.body)).toBe(true);
      expect(Buffer.isBuffer(secondLease.body)).toBe(true);
      expect(firstLease.body).not.toBe(secondLease.body);
      expect(firstLease.body).toStrictEqual(item.bytes);
      expect(secondLease.body).toStrictEqual(item.bytes);
      await firstLease.release();
      await secondLease.release();
    }
  });

  it("opens a new owned file stream for every large-object attempt", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const root = await mkdtemp(
      path.join(os.tmpdir(), "candidate-session2-large-"),
    );
    const sourcePlanPath = path.join(root, "source", "publication-plan.json");
    const controlRoot = path.join(root, "controls");
    const planArtifactPath = path.join(root, "plan", "plan.json");
    const objectPath = path.join(
      root,
      "source",
      "open-data",
      "properties",
      "property_large.json",
    );
    const bytes = Buffer.alloc(1024 * 1024 + 1, 0x61);
    await mkdir(path.dirname(sourcePlanPath), { recursive: true });
    await mkdir(controlRoot, { recursive: true });
    await mkdir(path.dirname(planArtifactPath), { recursive: true });
    await mkdir(path.dirname(objectPath), { recursive: true });
    await writeFile(sourcePlanPath, "{}\n");
    await writeFile(planArtifactPath, "{}\n");
    await writeFile(objectPath, bytes);
    const object: CandidateSourceSnapshotUploadObject = {
      byteSize: bytes.byteLength,
      domain: "open_data",
      expectedCid: await calculateIpfsCid(bytes),
      logicalObjectKey: "properties/property_large.json",
      remoteObjectKey: `${plan.targets.openData.immutablePrefix}properties/property_large.json`,
      sha256: sha256(bytes),
    };
    const source = await BoundCandidateSourceSnapshotLocalObjectSource.create({
      controlRoot,
      plan,
      planArtifactPath,
      sourcePlanPath,
    });

    const first = await source.openVerifiedUploadBody(object);
    const second = await source.openVerifiedUploadBody(object);
    expect(first.body).toBeInstanceOf(Readable);
    expect(second.body).toBeInstanceOf(Readable);
    expect(first.body).not.toBe(second.body);
    await first.release();
    const received: Buffer[] = [];
    for await (const chunk of second.body) received.push(Buffer.from(chunk));
    expect(Buffer.concat(received)).toStrictEqual(bytes);
    await second.release();
  }, 20_000);

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

  it("applies only closed ordered transport limit overrides", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const config = enabledConfig(plan);
    expect(resolveCandidateSourceSnapshotTransportLimits(config)).toEqual({
      connectionTimeoutMs: 5_000,
      maxSockets: 16,
      requestTimeoutMs: 20_000,
      socketTimeoutMs: 15_000,
    });
    const limits = {
      connectionTimeoutMs: 5_000,
      maxSockets: 4,
      requestTimeoutMs: 60_000,
      socketTimeoutMs: 45_000,
    };
    expect(
      resolveCandidateSourceSnapshotTransportLimits(config, limits),
    ).toEqual(limits);
    expect(() =>
      resolveCandidateSourceSnapshotTransportLimits(config, {
        ...limits,
        maxSockets: 17,
      }),
    ).toThrow("closed transport limit");
    expect(() =>
      resolveCandidateSourceSnapshotTransportLimits(config, {
        ...limits,
        requestTimeoutMs: 60_001,
      }),
    ).toThrow("closed transport limit");
    expect(() =>
      resolveCandidateSourceSnapshotTransportLimits(config, {
        ...limits,
        connectionTimeoutMs: 45_000,
      }),
    ).toThrow("strictly ordered");
  });

  it("promotes one keep-alive agent only through the closed socket stages", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const config = enabledConfig(plan);
    const limits = {
      connectionTimeoutMs: 5_000,
      maxSockets: 4,
      requestTimeoutMs: 60_000,
      socketTimeoutMs: 45_000,
    };
    const executor = new AwsCandidateSourceSnapshotS3CommandExecutor(
      config,
      limits,
    );
    expect(executor.transportLimits.maxSockets).toBe(4);
    executor.setMaxSockets(4);
    expect(() => executor.setMaxSockets(16)).toThrow("4-to-8-to-16");
    executor.setMaxSockets(8);
    expect(executor.transportLimits.maxSockets).toBe(8);
    expect(() => executor.setMaxSockets(4)).toThrow("4-to-8-to-16");
    executor.setMaxSockets(16);
    expect(executor.transportLimits.maxSockets).toBe(16);
    executor.close();

    const injected = new FakeS3({
      output: { $metadata: { attempts: 1 } },
      responseHeaders: {},
    });
    const transport = new RealCandidateSourceSnapshotFilebaseTransport({
      config,
      executor: injected,
      source: new FakeSource(Buffer.from("{}\n")),
      transportLimits: limits,
    });
    expect(() => transport.setMaxSockets(4)).not.toThrow();
    expect(() => transport.setMaxSockets(8)).not.toThrow();
    expect(() => transport.setMaxSockets(16)).not.toThrow();
    transport.close();
  });

  it("verifies HeadObject metadata and fails closed for absent, ambiguous, and mismatched objects", async () => {
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

    const absent = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: new FakeS3(
        Object.assign(new Error("synthetic missing"), {
          $metadata: { httpStatusCode: 404 },
          name: "NotFound",
        }),
      ),
      source,
    });
    await expect(
      absent.inspectExistingOnce(plan, object),
    ).resolves.toMatchObject({ outcome: "absent" });

    const mismatch = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: new FakeS3({
        output: {
          $metadata: { attempts: 1 },
          ContentLength: object.byteSize,
          Metadata: {
            cid: plan.targets.queryTable.targetCid,
            "oracle-sha256": object.sha256,
          },
        },
        responseHeaders: {},
      }),
      source,
    });
    await expect(
      mismatch.inspectExistingOnce(plan, object),
    ).resolves.toMatchObject({ outcome: "mismatch" });

    const ambiguous = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor: new FakeS3({
        output: {
          $metadata: { attempts: 1 },
          ContentLength: object.byteSize,
          Metadata: {},
        },
        responseHeaders: {},
      }),
      source,
    });
    await expect(
      ambiguous.inspectExistingOnce(plan, object),
    ).resolves.toMatchObject({ outcome: "ambiguous" });
  });

  it("classifies bounded cause chains without retaining raw provider evidence", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const object = objects[0]!;
    const source = new FakeSource(Buffer.from("{}\n"));
    const rawRequestId = "synthetic-raw-request-id";
    const cases = [
      {
        error: Object.assign(new Error("synthetic timeout"), {
          name: "TimeoutError",
        }),
        evidence: {
          failureClass: "outcome_unknown",
          providerRequestIdHash: null,
          stage: "transport_deadline",
        },
        outcome: "timeout_unknown",
      },
      {
        error: Object.assign(new Error("synthetic reset"), {
          $metadata: { requestId: rawRequestId },
          code: "ECONNRESET",
          name: "TimeoutError",
        }),
        evidence: {
          failureClass: "outcome_unknown",
          providerRequestIdHash: sha256(rawRequestId),
          stage: "put_object_streaming_request",
        },
        outcome: "timeout_unknown",
      },
      {
        error: Object.assign(new Error("synthetic connection failure"), {
          cause: Object.assign(new Error("nested"), { code: "ECONNREFUSED" }),
        }),
        evidence: {
          failureClass: "retryable",
          providerRequestIdHash: null,
          stage: "put_object_connection",
        },
        outcome: "connection_failure",
      },
      {
        error: Object.assign(new Error("synthetic service failure"), {
          $metadata: { httpStatusCode: 503 },
        }),
        evidence: {
          failureClass: "retryable",
          providerRequestIdHash: null,
          stage: "put_object_provider_response",
        },
        outcome: "retryable_http_error",
      },
      {
        error: Object.assign(new Error("synthetic rejected request"), {
          $metadata: { httpStatusCode: 403 },
        }),
        evidence: {
          failureClass: "terminal",
          providerRequestIdHash: null,
          stage: "put_object_provider_response",
        },
        outcome: "terminal_failure",
      },
      {
        error: Object.assign(new Error("synthetic response stream failure"), {
          code: "ERR_STREAM_PREMATURE_CLOSE",
        }),
        evidence: {
          failureClass: "outcome_unknown",
          providerRequestIdHash: null,
          stage: "put_object_streaming_request",
        },
        outcome: "timeout_unknown",
      },
    ] as const;

    for (const testCase of cases) {
      const failing = new RealCandidateSourceSnapshotFilebaseTransport({
        config: enabledConfig(plan),
        executor: new FakeS3(testCase.error),
        source,
      });
      const rejected = await failing
        .uploadOnce(plan, object)
        .catch((error) => error);
      expect(rejected).toBeInstanceOf(
        CandidateSourceSnapshotFilebaseTransportError,
      );
      expect(rejected).toMatchObject({
        evidence: testCase.evidence,
        failureClass: testCase.evidence.failureClass,
        outcome: testCase.outcome,
        providerRequestIdHash: testCase.evidence.providerRequestIdHash,
        stage: testCase.evidence.stage,
      });
      expect(JSON.stringify(rejected)).not.toContain(rawRequestId);
    }
  });

  it("creates and releases a fresh replayable body for every coordinator attempt", async () => {
    const { objects, plan } = syntheticCandidateSourceSnapshotDemo();
    const object = objects[0]!;
    const source = new FreshBodySource(Buffer.from("synthetic body\n"));
    const executor = new SequencedS3([
      Object.assign(new Error("synthetic disconnect after dispatch"), {
        code: "ECONNRESET",
      }),
      {
        output: { $metadata: { attempts: 1 } },
        responseHeaders: { "x-amz-meta-cid": object.expectedCid },
      },
    ]);
    const transport = new RealCandidateSourceSnapshotFilebaseTransport({
      config: enabledConfig(plan),
      executor,
      source,
    });

    await expect(transport.uploadOnce(plan, object)).rejects.toMatchObject({
      outcome: "timeout_unknown",
      stage: "put_object_streaming_request",
    });
    await expect(transport.uploadOnce(plan, object)).resolves.toMatchObject({
      providerCid: object.expectedCid,
    });
    expect(source.bodies).toHaveLength(2);
    expect(source.bodies[0]).not.toBe(source.bodies[1]);
    expect(source.release).toHaveBeenCalledTimes(2);
    expect(executor.commands).toHaveLength(2);
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
