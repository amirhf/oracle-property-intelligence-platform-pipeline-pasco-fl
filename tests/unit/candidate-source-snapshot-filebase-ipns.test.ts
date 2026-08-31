import { generateKeyPair } from "@libp2p/crypto/keys";
import { createIPNSRecord, marshalIPNSRecord } from "ipns";
import { base36 } from "multiformats/bases/base36";
import { CID } from "multiformats/cid";
import { describe, expect, it, vi } from "vitest";

import { sha256 } from "../../src/lib/hash.js";
import {
  DELEGATED_IPNS_MEDIA_TYPE,
  observeDelegatedIpnsRecord,
} from "../../src/publication/delegated-ipns.js";
import {
  loadCandidateSourceSnapshotExecutionConfig,
  type EnabledCandidateSourceSnapshotExecutionConfig,
} from "../../src/publication/candidate-source-snapshot-executor-config.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN,
  CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN,
  CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES,
  CandidateSourceSnapshotFilebaseIpnsAdapter,
  type CandidateSourceSnapshotFilebaseIpnsEvidence,
  type CandidateSourceSnapshotIpnsRequestAdmission,
} from "../../src/publication/candidate-source-snapshot-filebase-ipns.js";
import type { CandidateSourceSnapshotDemoPlan } from "../../src/publication/candidate-source-snapshot-demo.js";
import type { CandidateSourceSnapshotIpnsMutationCommand } from "../../src/publication/candidate-source-snapshot-ipns-controller.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function environment(plan: CandidateSourceSnapshotDemoPlan): NodeJS.ProcessEnv {
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
    environment(plan),
    plan,
  );
  if (!config.enabled) throw new Error("test configuration did not enable");
  return config;
}

function namesResponse(plan: CandidateSourceSnapshotDemoPlan, cid: string) {
  return new Response(
    JSON.stringify([
      {
        cid,
        enabled: true,
        label: plan.targets.openData.ipnsLabel,
        network_key: plan.targets.openData.ipnsNetworkKey,
      },
    ]),
    {
      headers: {
        "content-type": "application/json",
        "x-request-id": "raw-provider-request-id",
      },
      status: 200,
    },
  );
}

function command(
  plan: CandidateSourceSnapshotDemoPlan,
): CandidateSourceSnapshotIpnsMutationCommand {
  return {
    action: "mutate",
    attemptNumber: 1,
    authorizationId: null,
    authorizationSha256: null,
    commandId: `${plan.planId}:intent:mutate:1`,
    domain: "open_data",
    intentId: "synthetic-intent",
    planId: plan.planId,
    planSha256: plan.planSha256,
    priorCid: plan.targets.openData.priorCid,
    targetCid: plan.targets.openData.targetCid,
  };
}

function adapter(options: {
  admissions?: CandidateSourceSnapshotIpnsRequestAdmission[];
  evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[];
  fetchImpl: typeof fetch;
  observeDelegated?: typeof observeDelegatedIpnsRecord;
  plan: CandidateSourceSnapshotDemoPlan;
  requestGate?: (
    request: CandidateSourceSnapshotIpnsRequestAdmission,
  ) => Promise<void>;
}) {
  return new CandidateSourceSnapshotFilebaseIpnsAdapter({
    config: enabledConfig(options.plan),
    evidenceSink: {
      async record(item) {
        options.evidence.push(item);
      },
    },
    fetchImpl: options.fetchImpl,
    observeDelegated:
      options.observeDelegated ??
      (async ({ expectedTargetCid }) => ({
        endpointType: "ipfs_delegated_routing_v1",
        httpStatus: 200,
        latencyMs: 1,
        observedAt: "2026-08-31T00:00:00.000Z",
        observedCid: expectedTargetCid,
        outcome: "validated",
        requestCount: 1,
        responseBytes: 32,
        responseSha256: "a".repeat(64),
        schemaVersion: "candidate_signed_ipns_observation_v1",
        sequence: "7",
        ttlNanoseconds: "60000000000",
        validationResult: "valid_target",
        validity: "2026-09-01T00:00:00.000000000Z",
      })),
    plan: options.plan,
    requestGate: {
      async beforeRequest(request) {
        options.admissions?.push(request);
        await options.requestGate?.(request);
      },
    },
    retryDelay: async () => undefined,
  });
}

describe("candidate source-snapshot Filebase IPNS adapter", () => {
  it("has no default transport and makes no request during construction", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const fetchImpl = vi.fn<typeof fetch>();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    adapter({ evidence, fetchImpl, plan });
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(evidence).toStrictEqual([]);

    expect(
      () =>
        new CandidateSourceSnapshotFilebaseIpnsAdapter({
          config: {
            enabled: false,
          } as unknown as EnabledCandidateSourceSnapshotExecutionConfig,
          evidenceSink: { async record() {} },
          fetchImpl,
          observeDelegated: observeDelegatedIpnsRecord,
          plan,
          requestGate: { async beforeRequest() {} },
        }),
    ).toThrow("configuration is not exact");
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("requires durable admission before every request and a denial prevents transport use", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const fetchImpl = vi.fn<typeof fetch>();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const admissions: CandidateSourceSnapshotIpnsRequestAdmission[] = [];
    const transport = adapter({
      admissions,
      evidence,
      fetchImpl,
      plan,
      requestGate: async () => {
        throw new Error("synthetic admission denied");
      },
    });
    await expect(transport.readControlPlane("open_data")).rejects.toThrow(
      "synthetic admission denied",
    );
    expect(admissions).toStrictEqual([
      {
        domain: "open_data",
        endpointType: "filebase_names_api_v1",
        method: "GET",
        operation: "names_read",
        requestOrdinal: 1,
        schemaVersion: "candidate_source_snapshot_filebase_ipns_evidence_v1",
      },
    ]);
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(evidence).toStrictEqual([]);
  });

  it("reads and updates only the exact Names API identity with bounded hashed receipts", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const calls: Array<{ body: string | null; method: string; url: string }> =
      [];
    const fetchImpl: typeof fetch = async (input, init) => {
      calls.push({
        body: typeof init?.body === "string" ? init.body : null,
        method: init?.method ?? "GET",
        url: String(input),
      });
      return init?.method === "PUT"
        ? new Response(null, {
            headers: { "x-request-id": "raw-update-request-id" },
            status: 200,
          })
        : namesResponse(plan, plan.targets.openData.priorCid);
    };
    const transport = adapter({ evidence, fetchImpl, plan });

    const read = await transport.readControlPlane("open_data");
    const update = await transport.updateControlPlane(
      "open_data",
      plan.targets.openData.targetCid,
    );

    expect(calls).toStrictEqual([
      {
        body: null,
        method: "GET",
        url: `${CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN}/v1/names`,
      },
      {
        body: JSON.stringify({
          cid: plan.targets.openData.targetCid,
          enabled: true,
        }),
        method: "PUT",
        url: `${CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_NAMES_ORIGIN}/v1/names/${plan.targets.openData.ipnsLabel}`,
      },
    ]);
    expect(read).toMatchObject({
      observedCid: plan.targets.openData.priorCid,
      operation: "names_read",
      outcome: "observed",
      providerRequestIdHash: sha256("raw-provider-request-id"),
      requestCount: 1,
    });
    expect(update).toMatchObject({
      observedCid: null,
      operation: "names_update",
      outcome: "accepted",
      providerRequestIdHash: sha256("raw-update-request-id"),
      requestCount: 1,
    });
    const serialized = JSON.stringify([read, update]);
    expect(serialized).not.toContain("synthetic-access");
    expect(serialized).not.toContain("synthetic-secret");
    expect(serialized).not.toContain("raw-provider-request-id");
    expect(serialized).not.toContain("raw-update-request-id");
  });

  it("leaves transient resolver retries to separately journaled recovery cycles", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    let requests = 0;
    const retried = adapter({
      evidence,
      fetchImpl: async () => {
        requests += 1;
        return requests === 1
          ? new Response(null, { status: 503 })
          : namesResponse(plan, plan.targets.openData.targetCid);
      },
      plan,
    });
    await expect(retried.readControlPlane("open_data")).resolves.toMatchObject({
      httpStatus: 503,
      observedCid: null,
      outcome: "http_error",
      requestCount: 1,
    });
    expect(requests).toBe(1);

    const redirected = adapter({
      evidence,
      fetchImpl: async () =>
        new Response(null, {
          headers: { location: "https://example.invalid/secret" },
          status: 302,
        }),
      plan,
    });
    await expect(
      redirected.readControlPlane("open_data"),
    ).resolves.toMatchObject({ outcome: "redirect_rejected", requestCount: 1 });

    const oversized = adapter({
      evidence,
      fetchImpl: async () =>
        new Response(null, {
          headers: {
            "content-length": String(
              CANDIDATE_SOURCE_SNAPSHOT_MAX_NAMES_BYTES + 1,
            ),
          },
          status: 200,
        }),
      plan,
    });
    await expect(
      oversized.readControlPlane("open_data"),
    ).resolves.toMatchObject({
      outcome: "response_too_large",
      requestCount: 1,
    });

    let timeoutRequests = 0;
    const timeout = adapter({
      evidence,
      fetchImpl: async () => {
        timeoutRequests += 1;
        throw new DOMException("synthetic timeout", "TimeoutError");
      },
      plan,
    });
    await expect(timeout.readControlPlane("open_data")).resolves.toMatchObject({
      httpStatus: null,
      outcome: "timeout",
      requestCount: 1,
    });
    expect(timeoutRequests).toBe(1);
  });

  it("never blindly retries a Names mutation after an ambiguous provider response", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const admissions: CandidateSourceSnapshotIpnsRequestAdmission[] = [];
    const fetchImpl = vi.fn<typeof fetch>(
      async () => new Response(null, { status: 503 }),
    );
    const transport = adapter({ admissions, evidence, fetchImpl, plan });

    await expect(
      transport.updateControlPlane(
        "open_data",
        plan.targets.openData.targetCid,
      ),
    ).resolves.toMatchObject({
      httpStatus: 503,
      operation: "names_update",
      outcome: "http_error",
      requestCount: 1,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(admissions).toStrictEqual([
      {
        domain: "open_data",
        endpointType: "filebase_names_api_v1",
        method: "PUT",
        operation: "names_update",
        requestOrdinal: 1,
        schemaVersion: "candidate_source_snapshot_filebase_ipns_evidence_v1",
      },
    ]);
  });

  it("validates one same-origin immutable gateway redirect without following it", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const targetPath = `/ipfs/${plan.targets.openData.targetCid}`;
    const calls: string[] = [];
    const accepted = adapter({
      evidence,
      fetchImpl: async (input) => {
        calls.push(String(input));
        return calls.length === 1
          ? new Response(null, {
              headers: { location: targetPath },
              status: 302,
            })
          : new Response(null, { status: 200 });
      },
      plan,
    });
    await expect(
      accepted.observeOfficialGateway("open_data"),
    ).resolves.toMatchObject({
      observedCid: plan.targets.openData.targetCid,
      outcome: "observed",
      requestCount: 1,
    });
    expect(calls).toStrictEqual([
      `${CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_GATEWAY_ORIGIN}/ipns/${plan.targets.openData.ipnsNetworkKey}`,
    ]);

    const rejected = adapter({
      evidence,
      fetchImpl: async () =>
        new Response(null, {
          headers: { location: `https://example.invalid${targetPath}` },
          status: 302,
        }),
      plan,
    });
    await expect(
      rejected.observeOfficialGateway("open_data"),
    ).resolves.toMatchObject({ outcome: "redirect_rejected", requestCount: 1 });
  });

  it.each([
    ["target", "target"],
    ["prior", "prior"],
    ["split", "split"],
  ] as const)(
    "classifies exact %s resolver agreement as %s",
    async (fixture, expected) => {
      const { plan } = syntheticCandidateSourceSnapshotDemo();
      const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
      const controlCid =
        fixture === "prior"
          ? plan.targets.openData.priorCid
          : plan.targets.openData.targetCid;
      const gatewayCid =
        fixture === "split" ? plan.targets.openData.priorCid : controlCid;
      const transport = adapter({
        evidence,
        fetchImpl: async (input) =>
          String(input).includes("/v1/names")
            ? namesResponse(plan, controlCid)
            : new Response(null, {
                headers: { "x-ipfs-path": `/ipfs/${gatewayCid}` },
                status: 200,
              }),
        observeDelegated: async () => ({
          endpointType: "ipfs_delegated_routing_v1",
          httpStatus: 200,
          latencyMs: 1,
          observedAt: "2026-08-31T00:00:00.000Z",
          observedCid: controlCid,
          outcome: "validated",
          requestCount: 1,
          responseBytes: 32,
          responseSha256: "b".repeat(64),
          schemaVersion: "candidate_signed_ipns_observation_v1",
          sequence: "7",
          ttlNanoseconds: "60000000000",
          validationResult:
            controlCid === plan.targets.openData.targetCid
              ? "valid_target"
              : "valid_prior",
          validity: "2026-09-01T00:00:00.000000000Z",
        }),
        plan,
      });
      await expect(
        transport.observeIdentity("open_data"),
      ).resolves.toMatchObject({
        classification: expected,
        observedCid:
          expected === "split"
            ? null
            : expected === "target"
              ? plan.targets.openData.targetCid
              : plan.targets.openData.priorCid,
      });
      expect(evidence).toHaveLength(4);
    },
  );

  it("uses the vetted signed-record validator and hard-classifies a third CID", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const privateKey = await generateKeyPair("Ed25519");
    const thirdCid = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
    const record = await createIPNSRecord(
      privateKey,
      `/ipfs/${thirdCid}`,
      7,
      60_000,
    );
    const bytes = marshalIPNSRecord(record);
    const networkKey = CID.createV1(
      0x72,
      privateKey.publicKey.toMultihash(),
    ).toString(base36);
    const delegated = await observeDelegatedIpnsRecord({
      expectedPriorCid: plan.targets.openData.priorCid,
      expectedTargetCid: plan.targets.openData.targetCid,
      fetchImpl: async () =>
        new Response(Buffer.from(bytes), {
          headers: { "content-type": DELEGATED_IPNS_MEDIA_TYPE },
          status: 200,
        }),
      maxRetries: 0,
      networkKey,
      timeoutMs: 1_000,
    });
    expect(delegated).toMatchObject({
      observedCid: thirdCid,
      outcome: "unexpected_cid",
      responseSha256: sha256(Buffer.from(bytes)),
      validationResult: "unexpected_cid",
    });

    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const transport = adapter({
      evidence,
      fetchImpl: async (input) =>
        String(input).includes("/v1/names")
          ? namesResponse(plan, plan.targets.openData.targetCid)
          : new Response(null, {
              headers: {
                "x-ipfs-path": `/ipfs/${plan.targets.openData.targetCid}`,
              },
              status: 200,
            }),
      observeDelegated: async () => delegated,
      plan,
    });
    await expect(transport.observeIdentity("open_data")).resolves.toMatchObject(
      {
        classification: "unexpected",
        observedCid: thirdCid,
      },
    );
  });

  it("rejects commands that do not bind the exact immutable plan before any request", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const fetchImpl = vi.fn<typeof fetch>();
    const evidence: CandidateSourceSnapshotFilebaseIpnsEvidence[] = [];
    const transport = adapter({ evidence, fetchImpl, plan });
    await expect(
      transport.mutateAndObserve({
        ...command(plan),
        targetCid: plan.targets.queryTable.targetCid,
      }),
    ).rejects.toThrow("immutable candidate plan");
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(evidence).toStrictEqual([]);
  });
});
