import { isDurabilityTerminalError } from "../lib/durability-errors.js";

const RECONNECTABLE_POSTGRES_CODES = new Set([
  "CONNECTION_CLOSED",
  "CONNECTION_DESTROYED",
  "CONNECTION_ENDED",
  "CONNECT_TIMEOUT",
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
]);

export const CANDIDATE_SOURCE_SNAPSHOT_POSTGRES_RECONNECT_BACKOFF_MS = [
  25, 100, 250,
] as const;

export interface CandidateSourceSnapshotPostgresSession<Transaction> {
  close(): Promise<void>;
  probe(): Promise<void>;
  transaction<Result>(
    operation: (transaction: Transaction) => Promise<Result>,
  ): Promise<Result>;
}

export interface CandidateSourceSnapshotPostgresReconnectDependencies<
  Transaction,
> {
  createSession(): CandidateSourceSnapshotPostgresSession<Transaction>;
  sleep(delayMs: number): Promise<void>;
}

function reconnectableCode(error: unknown): boolean {
  const seen = new Set<unknown>();
  let current = error;
  for (let depth = 0; depth < 8 && current !== null; depth += 1) {
    if (seen.has(current)) return false;
    seen.add(current);
    if (typeof current !== "object") return false;
    const value = current as {
      cause?: unknown;
      code?: unknown;
      errno?: unknown;
    };
    if (
      (typeof value.code === "string" &&
        RECONNECTABLE_POSTGRES_CODES.has(value.code)) ||
      (typeof value.errno === "string" &&
        RECONNECTABLE_POSTGRES_CODES.has(value.errno))
    ) {
      return true;
    }
    current = value.cause;
  }
  return false;
}

export function isReconnectableCandidateSourceSnapshotPostgresError(
  error: unknown,
): boolean {
  return !isDurabilityTerminalError(error) && reconnectableCode(error);
}

/**
 * Reconnect only before the caller's operation begins. A connection failure
 * after that point may have an unknown commit outcome, so it is left for the
 * durable operation ID's normal exact-replay path on a later invocation.
 */
export async function runCandidateSourceSnapshotFencedPostgresOperation<
  Transaction,
  Result,
>(input: {
  dependencies: CandidateSourceSnapshotPostgresReconnectDependencies<Transaction>;
  operation(transaction: Transaction): Promise<Result>;
  revalidateGeneration(transaction: Transaction): Promise<void>;
}): Promise<Result> {
  for (
    let connectionSequence = 0;
    connectionSequence <=
    CANDIDATE_SOURCE_SNAPSHOT_POSTGRES_RECONNECT_BACKOFF_MS.length;
    connectionSequence += 1
  ) {
    let operationStarted = false;
    let session:
      CandidateSourceSnapshotPostgresSession<Transaction> | undefined;
    try {
      session = input.dependencies.createSession();
      await session.probe();
      return await session.transaction(async (transaction) => {
        await input.revalidateGeneration(transaction);
        operationStarted = true;
        return await input.operation(transaction);
      });
    } catch (error) {
      const canRetry =
        !operationStarted &&
        connectionSequence <
          CANDIDATE_SOURCE_SNAPSHOT_POSTGRES_RECONNECT_BACKOFF_MS.length &&
        isReconnectableCandidateSourceSnapshotPostgresError(error);
      if (!canRetry) throw error;
    } finally {
      if (session) {
        // The session is discarded after every failure. A cleanup error cannot
        // turn a known result into a second execution of the operation.
        await session.close().catch(() => undefined);
      }
    }
    await input.dependencies.sleep(
      CANDIDATE_SOURCE_SNAPSHOT_POSTGRES_RECONNECT_BACKOFF_MS[
        connectionSequence
      ]!,
    );
  }
  throw new Error("Candidate source-snapshot PostgreSQL retry was exhausted");
}
