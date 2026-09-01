import { describe, expect, it, vi } from "vitest";

import { DurableConflictError } from "../../src/lib/durability-errors.js";
import {
  isReconnectableCandidateSourceSnapshotPostgresError,
  runCandidateSourceSnapshotFencedPostgresOperation,
  type CandidateSourceSnapshotPostgresSession,
} from "../../src/db/candidate-source-snapshot-postgres-reconnect.js";

interface SyntheticTransaction {
  connectionSequence: number;
}

function connectionError(code: string, cause?: unknown): Error {
  return Object.assign(new Error("synthetic PostgreSQL connection failure"), {
    ...(cause === undefined ? {} : { cause }),
    code,
  });
}

describe("candidate source-snapshot PostgreSQL reconnect", () => {
  it("recreates a failed client, probes it and rechecks the generation fence", async () => {
    const closes: number[] = [];
    const probes: number[] = [];
    const fences: number[] = [];
    const operations: number[] = [];
    const sleeps: number[] = [];
    let created = 0;

    const result = await runCandidateSourceSnapshotFencedPostgresOperation({
      dependencies: {
        createSession: () => {
          const connectionSequence = ++created;
          return {
            close: async () => {
              closes.push(connectionSequence);
            },
            probe: async () => {
              probes.push(connectionSequence);
            },
            transaction: async <Result>(
              operation: (transaction: SyntheticTransaction) => Promise<Result>,
            ) => await operation({ connectionSequence }),
          } satisfies CandidateSourceSnapshotPostgresSession<SyntheticTransaction>;
        },
        sleep: async (delayMs) => {
          sleeps.push(delayMs);
        },
      },
      operation: async (transaction) => {
        operations.push(transaction.connectionSequence);
        return "verified";
      },
      revalidateGeneration: async (transaction) => {
        fences.push(transaction.connectionSequence);
        if (transaction.connectionSequence === 1) {
          throw connectionError("ECONNRESET");
        }
      },
    });

    expect(result).toBe("verified");
    expect(probes).toEqual([1, 2]);
    expect(fences).toEqual([1, 2]);
    expect(operations).toEqual([2]);
    expect(closes).toEqual([1, 2]);
    expect(sleeps).toEqual([25]);
  });

  it("does not retry a connection failure after the operation starts", async () => {
    const createSession = vi.fn(() => ({
      close: async () => undefined,
      probe: async () => undefined,
      transaction: async <Result>(
        operation: (transaction: SyntheticTransaction) => Promise<Result>,
      ) => {
        await operation({ connectionSequence: 1 });
        throw connectionError("CONNECTION_CLOSED");
      },
    }));
    const operation = vi.fn(async () => "committed-or-unknown");

    await expect(
      runCandidateSourceSnapshotFencedPostgresOperation({
        dependencies: {
          createSession,
          sleep: async () => undefined,
        },
        operation,
        revalidateGeneration: async () => undefined,
      }),
    ).rejects.toMatchObject({ code: "CONNECTION_CLOSED" });

    expect(createSession).toHaveBeenCalledTimes(1);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("never retries durability or SQL invariant failures", async () => {
    const durableSession = vi.fn(() => ({
      close: async () => undefined,
      probe: async () => undefined,
      transaction: async <Result>(
        operation: (transaction: SyntheticTransaction) => Promise<Result>,
      ) => await operation({ connectionSequence: 1 }),
    }));
    await expect(
      runCandidateSourceSnapshotFencedPostgresOperation({
        dependencies: {
          createSession: durableSession,
          sleep: async () => undefined,
        },
        operation: async () => "unreachable",
        revalidateGeneration: async () => {
          throw new DurableConflictError("synthetic stale generation");
        },
      }),
    ).rejects.toThrow("synthetic stale generation");
    expect(durableSession).toHaveBeenCalledTimes(1);

    expect(
      isReconnectableCandidateSourceSnapshotPostgresError(
        connectionError("23514"),
      ),
    ).toBe(false);
    expect(
      isReconnectableCandidateSourceSnapshotPostgresError(
        new DurableConflictError("synthetic conflict"),
      ),
    ).toBe(false);
  });

  it("bounds reconnects to three retries and recognizes a nested closed client", async () => {
    const close = vi.fn(async () => undefined);
    const createSession = vi.fn(() => ({
      close,
      probe: async () => {
        throw connectionError(
          "WRAPPED",
          connectionError("CONNECTION_DESTROYED"),
        );
      },
      transaction: async <Result>(
        _operation: (transaction: SyntheticTransaction) => Promise<Result>,
      ) => {
        throw new Error("unreachable");
      },
    }));
    const sleep = vi.fn(async (_delayMs: number) => undefined);

    await expect(
      runCandidateSourceSnapshotFencedPostgresOperation({
        dependencies: { createSession, sleep },
        operation: async () => "unreachable",
        revalidateGeneration: async () => undefined,
      }),
    ).rejects.toMatchObject({ code: "WRAPPED" });

    expect(createSession).toHaveBeenCalledTimes(4);
    expect(close).toHaveBeenCalledTimes(4);
    expect(sleep.mock.calls.map(([delay]) => delay)).toEqual([25, 100, 250]);
  });
});
