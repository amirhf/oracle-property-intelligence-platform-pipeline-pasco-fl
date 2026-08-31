import { generateKeyPair } from "@libp2p/crypto/keys";
import {
  createIPNSRecord,
  createIPNSRecordWithExpiration,
  marshalIPNSRecord,
  unmarshalIPNSRecord,
} from "ipns";
import { base36 } from "multiformats/bases/base36";
import { CID } from "multiformats/cid";
import { describe, expect, it } from "vitest";

import {
  DELEGATED_IPFS_ORIGIN,
  DELEGATED_IPNS_MEDIA_TYPE,
  MAX_SIGNED_IPNS_RECORD_BYTES,
  observeDelegatedIpnsRecord,
} from "../../src/publication/delegated-ipns.js";
import { observeCandidateSignedIpnsCheckpoint } from "../../src/publication/candidate-demo-preflight.js";
import { CandidateDelegatedPublicReadTransport } from "../../src/mcp/public-ipns-provider.js";

const priorCid = "bafybeie5yw5ajrvucfs2qkjkiyz56tb7oevg4coiggojm7v2yvnsbixsem";
const targetCid = "QmSdGz1gZtx4GXxQ41qez6ww6G1Xefy19BPU5vJPEobYUH";

async function signedRecord(options?: { expiration?: string; value?: string }) {
  const privateKey = await generateKeyPair("Ed25519");
  const record = options?.expiration
    ? await createIPNSRecordWithExpiration(
        privateKey,
        `/ipfs/${options.value ?? targetCid}`,
        7,
        options.expiration,
      )
    : await createIPNSRecord(
        privateKey,
        `/ipfs/${options?.value ?? targetCid}`,
        7,
        60_000,
      );
  return {
    bytes: marshalIPNSRecord(record),
    networkKey: CID.createV1(0x72, privateKey.publicKey.toMultihash()).toString(
      base36,
    ),
  };
}

function response(bytes: Uint8Array, init?: ResponseInit): Response {
  return new Response(Buffer.from(bytes), {
    headers: { "content-type": DELEGATED_IPNS_MEDIA_TYPE },
    status: 200,
    ...init,
  });
}

describe("signed IPNS delegated-routing observation", () => {
  it("uses only the compiled endpoint and cryptographically validates target metadata", async () => {
    const signed = await signedRecord();
    const calls: Array<{
      accept: string | null;
      redirect: RequestRedirect;
      url: string;
    }> = [];
    const evidence = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async (input, init) => {
        calls.push({
          accept: new Headers(init?.headers).get("accept"),
          redirect: init?.redirect ?? "follow",
          url: String(input),
        });
        return response(signed.bytes);
      },
      maxRetries: 0,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(calls).toEqual([
      {
        accept: DELEGATED_IPNS_MEDIA_TYPE,
        redirect: "manual",
        url: `${DELEGATED_IPFS_ORIGIN}/routing/v1/ipns/${signed.networkKey}`,
      },
    ]);
    expect(evidence).toMatchObject({
      httpStatus: 200,
      observedCid: targetCid,
      outcome: "validated",
      requestCount: 1,
      responseBytes: signed.bytes.byteLength,
      sequence: "7",
      validationResult: "valid_target",
    });
    expect(evidence.responseSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(evidence.validity).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("distinguishes the prior and an unexpected third CID", async () => {
    const prior = await signedRecord({ value: priorCid });
    const priorEvidence = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => response(prior.bytes),
      maxRetries: 0,
      networkKey: prior.networkKey,
      timeoutMs: 1_000,
    });
    expect(priorEvidence.validationResult).toBe("valid_prior");

    const thirdCid = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
    const third = await signedRecord({ value: thirdCid });
    const thirdEvidence = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => response(third.bytes),
      maxRetries: 0,
      networkKey: third.networkKey,
      timeoutMs: 1_000,
    });
    expect(thirdEvidence).toMatchObject({
      observedCid: thirdCid,
      outcome: "unexpected_cid",
      validationResult: "unexpected_cid",
    });
  });

  it("rejects identity mismatch, invalid signatures, and expired records", async () => {
    const signed = await signedRecord();
    const other = await signedRecord();
    const identityMismatch = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => response(signed.bytes),
      maxRetries: 0,
      networkKey: other.networkKey,
      timeoutMs: 1_000,
    });
    expect(identityMismatch.validationResult).toBe("invalid_signature");

    const tamperedRecord = unmarshalIPNSRecord(signed.bytes);
    tamperedRecord.signatureV2 = Uint8Array.from(tamperedRecord.signatureV2);
    tamperedRecord.signatureV2[0] = (tamperedRecord.signatureV2[0] ?? 0) ^ 0xff;
    const tampered = marshalIPNSRecord(tamperedRecord);
    const invalidSignature = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => response(tampered),
      maxRetries: 0,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(invalidSignature.validationResult).toBe("invalid_signature");

    const expired = await signedRecord({
      expiration: "2020-01-01T00:00:00.000000000Z",
    });
    const expiredEvidence = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => response(expired.bytes),
      maxRetries: 0,
      networkKey: expired.networkKey,
      timeoutMs: 1_000,
    });
    expect(expiredEvidence.validationResult).toBe("expired_record");
  });

  it("fails closed on redirects, content types, size, timeout, and retry bounds", async () => {
    const signed = await signedRecord();
    const redirect = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () =>
        new Response(null, {
          headers: { location: "https://example.invalid/record" },
          status: 302,
        }),
      maxRetries: 0,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(redirect.validationResult).toBe("redirect_rejected");

    const wrongType = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () =>
        new Response(Buffer.from(signed.bytes), {
          headers: { "content-type": "application/octet-stream" },
        }),
      maxRetries: 0,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(wrongType.validationResult).toBe("content_type_invalid");

    const oversized = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () =>
        new Response(new Uint8Array(MAX_SIGNED_IPNS_RECORD_BYTES + 1), {
          headers: { "content-type": DELEGATED_IPNS_MEDIA_TYPE },
        }),
      maxRetries: 0,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(oversized.validationResult).toBe("response_too_large");

    let attempts = 0;
    const retried = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => {
        attempts += 1;
        return attempts === 1
          ? new Response(null, { status: 503 })
          : response(signed.bytes);
      },
      maxRetries: 1,
      networkKey: signed.networkKey,
      timeoutMs: 1_000,
    });
    expect(retried).toMatchObject({
      requestCount: 2,
      validationResult: "valid_target",
    });

    let threeAttempts = 0;
    const exhausted = await observeDelegatedIpnsRecord({
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl: async () => {
        threeAttempts += 1;
        return new Response(null, { status: 503 });
      },
      maxRetries: 2,
      networkKey: signed.networkKey,
      retryDelay: async () => undefined,
      timeoutMs: 1_000,
    });
    expect(exhausted).toMatchObject({
      httpStatus: 503,
      requestCount: 3,
      validationResult: "http_error",
    });
    expect(threeAttempts).toBe(3);
  });

  it("requires control plane, official gateway, and signed record agreement", async () => {
    const signed = await signedRecord();
    const calls: string[] = [];
    const config = {
      apiEndpoint: "https://api.filebase.io",
      apiToken: "synthetic-token-value",
      enabled: false as const,
      limits: {
        maxBudgetUsd: 25,
        maxConcurrency: 2,
        maxObjectBytes: 1_000_000,
        maxObjects: 100,
        maxRequests: 100,
        maxRetries: 0,
        maxTotalBytes: 10_000_000,
        requestTimeoutMs: 1_000,
        requestUsdPerThousand: 0.01,
        storageUsdPerGib: 0.1,
      },
      s3AccessKeyId: "synthetic-access-key",
      s3Endpoint: "https://s3.filebase.com",
      s3SecretAccessKey: "synthetic-secret-key",
      targets: {
        openData: {
          bucket: "candidate-test-open-data-demo",
          ipnsLabel: "candidate-test-open-data-demo",
          ipnsNetworkKey: `k51${"2".repeat(59)}`,
        },
        queryTable: {
          bucket: "candidate-test-query-table-demo",
          ipnsLabel: "candidate-test-query-table-demo",
          ipnsNetworkKey: signed.networkKey,
        },
      },
    };
    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      calls.push(url);
      if (url === "https://api.filebase.io/v1/names") {
        return Response.json([
          {
            cid: targetCid,
            created_at: "2026-08-30T00:00:00.000Z",
            enabled: true,
            label: config.targets.queryTable.ipnsLabel,
            network_key: signed.networkKey,
            published_at: "2026-08-30T00:00:00.000Z",
            sequence: 7,
            updated_at: "2026-08-30T00:00:00.000Z",
          },
        ]);
      }
      if (url === `https://ipfs.filebase.io/ipns/${signed.networkKey}`) {
        return new Response(null, {
          headers: { "x-ipfs-path": `/ipfs/${targetCid}` },
          status: 301,
        });
      }
      if (
        url === `${DELEGATED_IPFS_ORIGIN}/routing/v1/ipns/${signed.networkKey}`
      ) {
        return response(signed.bytes);
      }
      throw new Error("unexpected endpoint");
    };
    const evidence = await observeCandidateSignedIpnsCheckpoint({
      approvalId: `demoapproval_${"a".repeat(32)}`,
      config,
      demoPlanId: `demo_${"b".repeat(32)}`,
      demoPlanSha256: "c".repeat(64),
      expectedPriorCid: priorCid,
      expectedTargetCid: targetCid,
      fetchImpl,
      intentId: `demointent_${"d".repeat(32)}`,
    });
    expect(evidence).toMatchObject({
      classification: "converged",
      requestCount: 3,
      targetCid,
    });
    expect(new Set(calls)).toEqual(
      new Set([
        "https://api.filebase.io/v1/names",
        `https://ipfs.filebase.io/ipns/${signed.networkKey}`,
        `${DELEGATED_IPFS_ORIGIN}/routing/v1/ipns/${signed.networkKey}`,
      ]),
    );
  });

  it("provides the closed public read-plane resolver profile without credentials", async () => {
    const open = await signedRecord({
      value: "QmVwpAV8hWUr3zsJZijhzUAArgSMhkV1vzmtJaWFMUQ4pj",
    });
    const query = await signedRecord({ value: targetCid });
    const expected = new Map([
      [open.networkKey, "QmVwpAV8hWUr3zsJZijhzUAArgSMhkV1vzmtJaWFMUQ4pj"],
      [query.networkKey, targetCid],
    ]);
    const records = new Map([
      [open.networkKey, open.bytes],
      [query.networkKey, query.bytes],
    ]);
    const calls: string[] = [];
    const filebaseAttempts = new Map<string, number>();
    const fetchImpl: typeof fetch = async (input, init) => {
      const url = new URL(String(input));
      calls.push(`${init?.method ?? "GET"} ${url.origin}${url.pathname}`);
      const identity = url.pathname.split("/").at(-1)!;
      const cid = expected.get(identity)!;
      if (url.origin === "https://ipfs.filebase.io") {
        if (url.pathname.startsWith("/ipfs/")) {
          return new Response("candidate artifact", { status: 200 });
        }
        const attempt = (filebaseAttempts.get(identity) ?? 0) + 1;
        filebaseAttempts.set(identity, attempt);
        if (attempt === 1 && identity === open.networkKey) {
          throw new DOMException("timed out", "TimeoutError");
        }
        if (attempt === 1) return new Response(null, { status: 503 });
        return new Response(null, {
          headers: { "x-ipfs-path": `/ipfs/${cid}` },
          status: 301,
        });
      }
      if (url.origin === DELEGATED_IPFS_ORIGIN) {
        return response(records.get(identity)!);
      }
      throw new Error("unexpected resolver");
    };
    const transport = new CandidateDelegatedPublicReadTransport(
      {
        candidateDemoPlanId: `demo_${"a".repeat(32)}`,
        candidateDemoPlanSha256: "a".repeat(64),
        candidateDemoSourcePlanSha256: "b".repeat(64),
        environment: "test",
        expectedManifestCid: targetCid,
        expectedManifestSha256: "1".repeat(64),
        expectedOpenDataRootCid: expected.get(open.networkKey)!,
        expectedPlanCid: targetCid,
        expectedPlanSha256: "2".repeat(64),
        expectedQueryTableRootCid: targetCid,
        limits: {
          maxCacheAgeSeconds: 300,
          maxJsonObjectBytes: 1024 * 1024,
          maxParquetBytes: 1024 * 1024,
          maxRedirects: 0,
          retries: 1,
          transportTimeoutMs: 1_000,
        },
        mode: "public-ipns",
        openDataIpns: open.networkKey,
        queryTableIpns: query.networkKey,
        resolverPolicy: "candidate_filebase_delegated_v2",
      },
      fetchImpl,
      async () => undefined,
    );
    await expect(transport.resolveIpns(open.networkKey)).resolves.toEqual([
      expect.objectContaining({
        cid: expected.get(open.networkKey),
        resolver: "filebase_public_gateway",
        status: "resolved",
      }),
      expect.objectContaining({
        cid: expected.get(open.networkKey),
        resolver: "ipfs_delegated_signed_record",
        status: "resolved",
      }),
    ]);
    await expect(transport.resolveIpns(query.networkKey)).resolves.toHaveLength(
      2,
    );
    await expect(transport.readCid(targetCid, 100)).resolves.toEqual(
      new TextEncoder().encode("candidate artifact"),
    );
    expect(filebaseAttempts).toEqual(
      new Map([
        [open.networkKey, 2],
        [query.networkKey, 2],
      ]),
    );
    expect(
      calls.filter((call) => call.startsWith(`GET ${DELEGATED_IPFS_ORIGIN}`)),
    ).toHaveLength(2);
    expect(
      calls.filter((call) => call.startsWith("HEAD https://ipfs.filebase.io")),
    ).toHaveLength(4);
    expect(calls.at(-1)).toBe(`GET https://ipfs.filebase.io/ipfs/${targetCid}`);
  });
});
