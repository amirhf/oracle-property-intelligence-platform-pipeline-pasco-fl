import { Readable } from "node:stream";

import { describe, expect, it, vi } from "vitest";

import type { CandidateSourceSnapshotRemoteReadAdmission } from "../../src/db/candidate-source-snapshot-completion.js";
import { sha256 } from "../../src/lib/hash.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import {
  CandidateSourceSnapshotDurableImmutableReader,
  type CandidateSourceSnapshotRemoteReadJournal,
  type CandidateSourceSnapshotRemoteVerifierObject,
} from "../../src/publication/candidate-source-snapshot-remote-verifier.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

function recordedAdmission(input: {
  object: CandidateSourceSnapshotRemoteVerifierObject;
  outcome: CandidateSourceSnapshotRemoteReadAdmission["outcome"];
  redirectSequence: number;
}): CandidateSourceSnapshotRemoteReadAdmission {
  return {
    alreadyRecorded: true,
    attemptSequence: 1,
    byteRangeEnd: null,
    byteRangeStart: null,
    checkKind: "open_data_graph",
    domain: input.object.domain,
    expectedBytes: input.object.expectedBytes,
    expectedCid: input.object.expectedCid,
    expectedSha256: input.object.expectedSha256,
    logicalRequestId: `snapshotdemologicalrequest_${"a".repeat(32)}`,
    logicalRequestSequence: 1,
    operationKind: "immutable_artifact_read",
    outcome: input.outcome,
    planId: syntheticCandidateSourceSnapshotDemo().plan.planId,
    planSha256: syntheticCandidateSourceSnapshotDemo().plan.planSha256,
    redirectSequence: input.redirectSequence,
    remoteObjectKey: input.object.remoteObjectKey,
    requestId: `snapshotdemorequest_${String(input.redirectSequence + 1).repeat(32)}`,
  };
}

describe("candidate source-snapshot durable immutable reader", () => {
  it("resumes an admitted redirect child without reserving a new attempt", async () => {
    const bytes = Buffer.from("durable redirect replay\n", "utf8");
    const object: CandidateSourceSnapshotRemoteVerifierObject = {
      domain: "open_data",
      expectedBytes: bytes.byteLength,
      expectedCid: await calculateIpfsCid(bytes),
      expectedSha256: sha256(bytes),
      logicalObjectKey: `properties/property_${"a".repeat(32)}.json`,
      remoteObjectKey: `immutable/properties/property_${"a".repeat(32)}.json`,
    };
    const parent = recordedAdmission({
      object,
      outcome: "retryable_failure",
      redirectSequence: 0,
    });
    const child = recordedAdmission({
      object,
      outcome: "succeeded",
      redirectSequence: 1,
    });
    const admit = vi.fn(async () => parent);
    const loadExisting = vi.fn(async () => child);
    const loadReceipt = vi.fn(
      async (_databaseUrl: string, admission: typeof parent) =>
        admission.requestId === parent.requestId
          ? {
              receipt: {
                observedAt: "2026-08-31T12:00:00.000Z",
                outcome: "retryable_failure" as const,
                receiptId: `snapshotdemoverificationreceipt_${"b".repeat(32)}`,
                receiptSha256: "b".repeat(64),
                responseBytes: null,
                responseSha256: null,
              },
              requestId: parent.requestId,
              requestOutcome: "retryable_failure" as const,
            }
          : {
              receipt: {
                observedAt: "2026-08-31T12:00:01.000Z",
                outcome: "verified" as const,
                receiptId: `snapshotdemoverificationreceipt_${"c".repeat(32)}`,
                receiptSha256: "c".repeat(64),
                responseBytes: bytes.byteLength,
                responseSha256: sha256(bytes),
              },
              requestId: child.requestId,
              requestOutcome: "succeeded" as const,
            },
    );
    const recordReceipt = vi.fn(async () => {
      throw new Error("recorded redirect replay must not write a receipt");
    });
    const journal = {
      admit,
      loadExisting,
      loadReceipt,
      recordReceipt,
    } as unknown as CandidateSourceSnapshotRemoteReadJournal;
    const fetchImpl = vi.fn(async () => {
      throw new Error("recorded redirect replay must not use transport");
    }) as unknown as typeof fetch;
    const retryDelay = vi.fn(async () => undefined);
    const reader = new CandidateSourceSnapshotDurableImmutableReader({
      databaseUrl: "postgresql://not-contacted.invalid/not-contacted",
      fetchImpl,
      journal,
      localSource: {
        openVerifiedStream: async () => ({
          body: Readable.from([bytes]),
          contentLength: bytes.byteLength,
          contentType: "application/json",
        }),
        verify: async () => undefined,
      },
      plan: syntheticCandidateSourceSnapshotDemo().plan,
      retryDelay,
    });

    await expect(
      reader.readFull({
        checkKind: "open_data_graph",
        logicalRequestSequence: 1,
        object,
      }),
    ).resolves.toEqual(new Uint8Array(bytes));
    expect(admit).toHaveBeenCalledOnce();
    expect(admit).toHaveBeenCalledWith(
      "postgresql://not-contacted.invalid/not-contacted",
      expect.objectContaining({ attemptSequence: 1, redirectSequence: 0 }),
    );
    expect(loadExisting).toHaveBeenCalledOnce();
    expect(loadExisting).toHaveBeenCalledWith(
      "postgresql://not-contacted.invalid/not-contacted",
      expect.objectContaining({ attemptSequence: 1, redirectSequence: 1 }),
    );
    expect(loadReceipt).toHaveBeenCalledTimes(2);
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(recordReceipt).not.toHaveBeenCalled();
    expect(retryDelay).not.toHaveBeenCalled();
  });
});
