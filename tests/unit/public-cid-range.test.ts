import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  BoundedPublicCidAsyncBuffer,
  HttpPublicCidRangeTransport,
  PublicCidRangeError,
  type PublicCidRangeTransport,
  verifyParquetMagicByRange,
  verifyParquetSha256ByRange,
} from "../../src/mcp/public-cid-range.js";

const CID = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
const OTHER_CID = "QmYwAPJzv5CZsnAzt8auVZRnGiRAKzMBRzXmvRYEhmu27e";
const PARQUET_BYTES = new TextEncoder().encode("PAR1dataPAR1");
const LIMITS = {
  maxRedirects: 0,
  retries: 0,
  transportTimeoutMs: 100,
};

function code(error: unknown): string | undefined {
  return error instanceof PublicCidRangeError ? error.code : undefined;
}

function requestedRange(init: RequestInit | undefined): string | null {
  return new Headers(init?.headers).get("range");
}

function rangeResponse(
  bytes: Uint8Array,
  start: number,
  endInclusive: number,
  total = PARQUET_BYTES.byteLength,
  cid = CID,
): Response {
  return new Response(bytes.slice(start, endInclusive + 1), {
    status: 206,
    headers: {
      "content-length": String(endInclusive - start + 1),
      "content-range": `bytes ${start}-${endInclusive}/${total}`,
      "x-ipfs-roots": cid,
    },
  });
}

describe("bounded immutable-CID range reads", () => {
  it("exposes a hyparquet-compatible bounded buffer and verifies PAR1 with exact ranges", async () => {
    const requests: Array<{
      method: string;
      range: string | null;
      url: string;
    }> = [];
    const transport = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      LIMITS,
      async (input, init) => {
        requests.push({
          method: init?.method ?? "GET",
          range: requestedRange(init),
          url: String(input),
        });
        if (init?.method === "HEAD") {
          return new Response(null, {
            status: 200,
            headers: {
              "accept-ranges": "bytes",
              "content-length": String(PARQUET_BYTES.byteLength),
              "x-ipfs-roots": CID,
            },
          });
        }
        const range = requestedRange(init);
        if (range === "bytes=0-3") return rangeResponse(PARQUET_BYTES, 0, 3);
        if (range === "bytes=8-11") return rangeResponse(PARQUET_BYTES, 8, 11);
        throw new Error("unexpected synthetic range");
      },
    );
    const file = await BoundedPublicCidAsyncBuffer.create({
      budget: {
        maximumBufferedBytes: 8,
        maximumConcurrency: 2,
        maximumObjectBytes: 12,
        maximumRangeBytes: 4,
        maximumRanges: 2,
        maximumTotalRangeBytes: 8,
      },
      cid: CID,
      expectedByteLength: PARQUET_BYTES.byteLength,
      transport,
    });

    await expect(verifyParquetMagicByRange(file)).resolves.toBeUndefined();
    expect(file.metrics()).toEqual({ requestedBytes: 8, rangeRequests: 2 });
    expect(requests).toEqual([
      {
        method: "HEAD",
        range: null,
        url: `https://ipfs.filebase.io/ipfs/${CID}`,
      },
      {
        method: "GET",
        range: "bytes=0-3",
        url: `https://ipfs.filebase.io/ipfs/${CID}`,
      },
      {
        method: "GET",
        range: "bytes=8-11",
        url: `https://ipfs.filebase.io/ipfs/${CID}`,
      },
    ]);
    await expect(file.slice(4, 8)).rejects.toSatisfy(
      (error: unknown) => code(error) === "range_budget_exhausted",
    );
  });

  it("rejects an ignored range even when the whole object fits in memory", async () => {
    const transport = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      LIMITS,
      async () =>
        new Response(PARQUET_BYTES, {
          status: 200,
          headers: { "content-length": String(PARQUET_BYTES.byteLength) },
        }),
    );

    await expect(
      transport.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).rejects.toSatisfy(
      (error: unknown) => code(error) === "range_unsupported",
    );
  });

  it.each([
    {
      name: "wrong total",
      response: () => rangeResponse(PARQUET_BYTES, 0, 3, 13),
      expected: "artifact_invalid",
    },
    {
      name: "wrong response CID",
      response: () => rangeResponse(PARQUET_BYTES, 0, 3, 12, OTHER_CID),
      expected: "cid_mismatch",
    },
    {
      name: "oversized streaming body",
      response: () =>
        new Response(PARQUET_BYTES, {
          status: 206,
          headers: { "content-range": "bytes 0-3/12" },
        }),
      expected: "artifact_too_large",
    },
  ])("rejects $name", async ({ response, expected }) => {
    const transport = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      LIMITS,
      async () => response(),
    );
    await expect(
      transport.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).rejects.toSatisfy((error: unknown) => code(error) === expected);
  });

  it("rejects redirects beyond the compiled CID gateway profile", async () => {
    const transport = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      { ...LIMITS, maxRedirects: 1 },
      async () =>
        new Response(null, {
          status: 302,
          headers: { location: `https://example.invalid/ipfs/${CID}` },
        }),
    );
    await expect(
      transport.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).rejects.toSatisfy(
      (error: unknown) => code(error) === "redirect_rejected",
    );
  });

  it("retries only bounded transient range failures and reports exact attempts", async () => {
    let attempts = 0;
    const transport = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      { ...LIMITS, retries: 1 },
      async () => {
        attempts += 1;
        return attempts === 1
          ? new Response(null, { status: 503 })
          : rangeResponse(PARQUET_BYTES, 0, 3);
      },
      async () => undefined,
    );
    await expect(
      transport.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).resolves.toEqual(PARQUET_BYTES.slice(0, 4));
    expect(attempts).toBe(2);

    let exhaustedAttempts = 0;
    const exhausted = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      { ...LIMITS, retries: 1 },
      async () => {
        exhaustedAttempts += 1;
        throw new DOMException("timed out", "TimeoutError");
      },
      async () => undefined,
    );
    await expect(
      exhausted.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).rejects.toSatisfy((error: unknown) => code(error) === "timeout");
    expect(exhaustedAttempts).toBe(2);

    let bodyAttempts = 0;
    const bodyFailure = new HttpPublicCidRangeTransport(
      "candidate_filebase_delegated_v2",
      { ...LIMITS, retries: 1 },
      async () => {
        bodyAttempts += 1;
        if (bodyAttempts === 1) {
          return new Response(
            new ReadableStream({
              pull(controller) {
                controller.error(new TypeError("synthetic connection loss"));
              },
            }),
            {
              status: 206,
              headers: { "content-range": "bytes 0-3/12" },
            },
          );
        }
        return rangeResponse(PARQUET_BYTES, 0, 3);
      },
      async () => undefined,
    );
    await expect(
      bodyFailure.readCidRange(CID, 0, 4, PARQUET_BYTES.byteLength, 4),
    ).resolves.toEqual(PARQUET_BYTES.slice(0, 4));
    expect(bodyAttempts).toBe(2);
  });

  it("validates immutable size and reserves concurrent slice budgets before I/O", async () => {
    let release: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const directTransport: PublicCidRangeTransport = {
      readCidRange: async (_cid, start, end) => {
        await blocked;
        return PARQUET_BYTES.slice(start, end);
      },
      statCid: async () => ({
        acceptsByteRanges: true,
        byteLength: PARQUET_BYTES.byteLength,
        cid: CID,
      }),
    };
    const file = await BoundedPublicCidAsyncBuffer.create({
      budget: {
        maximumBufferedBytes: 4,
        maximumConcurrency: 1,
        maximumObjectBytes: 12,
        maximumRangeBytes: 4,
        maximumRanges: 1,
        maximumTotalRangeBytes: 4,
      },
      cid: CID,
      expectedByteLength: 12,
      transport: directTransport,
    });
    const first = file.slice(0, 4);
    await expect(file.slice(4, 8)).rejects.toSatisfy(
      (error: unknown) => code(error) === "range_budget_exhausted",
    );
    release?.();
    await expect(first).resolves.toEqual(PARQUET_BYTES.slice(0, 4).buffer);

    await expect(
      BoundedPublicCidAsyncBuffer.create({
        budget: {
          maximumBufferedBytes: 4,
          maximumConcurrency: 1,
          maximumObjectBytes: 13,
          maximumRangeBytes: 4,
          maximumRanges: 1,
          maximumTotalRangeBytes: 4,
        },
        cid: CID,
        expectedByteLength: 13,
        transport: directTransport,
      }),
    ).rejects.toSatisfy((error: unknown) => code(error) === "artifact_invalid");
  });

  it("rejects corrupt Parquet magic without reading beyond its bounded endpoints", async () => {
    const requests: Array<[number, number | undefined]> = [];
    const bytes = new TextEncoder().encode("NOPEdataPAR1");
    const file = {
      byteLength: bytes.byteLength,
      slice: async (start: number, end?: number) => {
        requests.push([start, end]);
        return bytes.slice(start, end).buffer;
      },
    };
    await expect(verifyParquetMagicByRange(file)).rejects.toSatisfy(
      (error: unknown) => code(error) === "artifact_invalid",
    );
    expect(requests).toEqual([
      [0, 4],
      [8, 12],
    ]);
  });

  it("rejects an injected transport that exceeds the exact requested range", async () => {
    const transport: PublicCidRangeTransport = {
      readCidRange: async () => PARQUET_BYTES,
      statCid: async () => ({
        acceptsByteRanges: true,
        byteLength: PARQUET_BYTES.byteLength,
        cid: CID,
      }),
    };
    const file = await BoundedPublicCidAsyncBuffer.create({
      budget: {
        maximumBufferedBytes: 8,
        maximumConcurrency: 1,
        maximumObjectBytes: 12,
        maximumRangeBytes: 8,
        maximumRanges: 2,
        maximumTotalRangeBytes: 12,
      },
      cid: CID,
      expectedByteLength: 12,
      transport,
    });
    await expect(file.slice(0, 4)).rejects.toSatisfy(
      (error: unknown) => code(error) === "artifact_invalid",
    );
  });

  it("hashes bounded parallel ranges in offset order and verifies Parquet framing", async () => {
    const bytes = new TextEncoder().encode(
      `PAR1${"bounded-range-data".repeat(4)}PAR1`,
    );
    const completions: number[] = [];
    const transport: PublicCidRangeTransport = {
      readCidRange: async (_cid, start, end) => {
        await new Promise((resolve) =>
          setTimeout(resolve, start === 0 ? 5 : 0),
        );
        completions.push(start);
        return bytes.slice(start, end);
      },
      statCid: async () => ({
        acceptsByteRanges: true,
        byteLength: bytes.byteLength,
        cid: CID,
      }),
    };
    const expectedSha256 = createHash("sha256").update(bytes).digest("hex");
    await expect(
      verifyParquetSha256ByRange({
        budget: {
          maximumBufferedBytes: 32,
          maximumConcurrency: 2,
          maximumObjectBytes: bytes.byteLength,
          maximumRangeBytes: 16,
          maximumRanges: Math.ceil(bytes.byteLength / 16),
          maximumTotalRangeBytes: bytes.byteLength,
        },
        cid: CID,
        expectedByteLength: bytes.byteLength,
        expectedSha256,
        transport,
      }),
    ).resolves.toEqual({
      rangeRequests: Math.ceil(bytes.byteLength / 16),
      requestedBytes: bytes.byteLength,
      sha256: expectedSha256,
    });
    expect(completions[0]).not.toBe(0);
  });

  it("rejects altered range bytes despite valid immutable-CID transport metadata", async () => {
    const expected = new TextEncoder().encode("PAR1deterministic-dataPAR1");
    const altered = expected.slice();
    altered[8] = altered[8]! ^ 1;
    const transport: PublicCidRangeTransport = {
      readCidRange: async (_cid, start, end) => altered.slice(start, end),
      statCid: async () => ({
        acceptsByteRanges: true,
        byteLength: expected.byteLength,
        cid: CID,
      }),
    };
    await expect(
      verifyParquetSha256ByRange({
        budget: {
          maximumBufferedBytes: 16,
          maximumConcurrency: 2,
          maximumObjectBytes: expected.byteLength,
          maximumRangeBytes: 8,
          maximumRanges: Math.ceil(expected.byteLength / 8),
          maximumTotalRangeBytes: expected.byteLength,
        },
        cid: CID,
        expectedByteLength: expected.byteLength,
        expectedSha256: createHash("sha256").update(expected).digest("hex"),
        transport,
      }),
    ).rejects.toSatisfy((error: unknown) => code(error) === "hash_mismatch");
  });

  it("rejects range schedules that exceed count, byte, or buffer ceilings", async () => {
    const transport: PublicCidRangeTransport = {
      readCidRange: async () => PARQUET_BYTES,
      statCid: async () => ({
        acceptsByteRanges: true,
        byteLength: PARQUET_BYTES.byteLength,
        cid: CID,
      }),
    };
    const expectedSha256 = createHash("sha256")
      .update(PARQUET_BYTES)
      .digest("hex");
    await expect(
      verifyParquetSha256ByRange({
        budget: {
          maximumBufferedBytes: 4,
          maximumConcurrency: 2,
          maximumObjectBytes: 12,
          maximumRangeBytes: 4,
          maximumRanges: 2,
          maximumTotalRangeBytes: 8,
        },
        cid: CID,
        expectedByteLength: 12,
        expectedSha256,
        transport,
      }),
    ).rejects.toSatisfy(
      (error: unknown) =>
        code(error) === "artifact_too_large" ||
        code(error) === "range_budget_exhausted",
    );
  });
});
