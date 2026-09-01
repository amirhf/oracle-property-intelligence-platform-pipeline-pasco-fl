import {
  PublicReadError,
  type PublicReadErrorCode,
  type PublicProviderInitializationStage,
} from "./public-ipns-provider.js";

export type HostedInitializationStage =
  "configuration" | "contracts" | PublicProviderInitializationStage | "runtime";

export type HostedReadinessStatus =
  "initializing" | "not_checked" | "ready" | "unavailable";

export interface HostedInitializationContext {
  setStage(stage: HostedInitializationStage): void;
}

export interface HostedInitializationDiagnostic {
  attempt: number;
  correlationId: string;
  errorClass: "initialization_error" | "none" | "public_read_error";
  event: "oracle_public_initialization";
  latencyMs: number;
  outcome: "failure" | "success";
  publicReadCode: PublicReadErrorCode | null;
  retryable: boolean;
  stage: HostedInitializationStage;
  terminal: boolean;
}

export interface HostedReadiness {
  errorCode: PublicReadErrorCode | "initialization_failed" | null;
  status: HostedReadinessStatus;
}

type DiagnosticSink = (event: HostedInitializationDiagnostic) => void;

function elapsed(startedAt: number, now: () => number): number {
  return Math.min(3_600_000, Math.max(0, Math.round(now() - startedAt)));
}

function publicReadCode(error: unknown): PublicReadErrorCode | null {
  return error instanceof PublicReadError ? error.code : null;
}

function retryable(error: unknown): boolean {
  return error instanceof PublicReadError && error.retryable;
}

export class RecoverableHostedInitializer<T> {
  readonly #diagnosticSink: DiagnosticSink;
  readonly #initialize: (context: HostedInitializationContext) => Promise<T>;
  readonly #now: () => number;
  #attempt = 0;
  #inFlight: Promise<T> | undefined;
  #readiness: HostedReadiness = { errorCode: null, status: "not_checked" };
  #value: T | undefined;

  constructor(options: {
    diagnosticSink?: DiagnosticSink;
    initialize: (context: HostedInitializationContext) => Promise<T>;
    now?: () => number;
  }) {
    this.#diagnosticSink = options.diagnosticSink ?? (() => undefined);
    this.#initialize = options.initialize;
    this.#now = options.now ?? Date.now;
  }

  get(correlationId: string): Promise<T> {
    if (this.#value !== undefined) return Promise.resolve(this.#value);
    if (this.#inFlight !== undefined) return this.#inFlight;

    this.#attempt += 1;
    const attempt = this.#attempt;
    const startedAt = this.#now();
    let stage: HostedInitializationStage = "configuration";
    this.#readiness = { errorCode: null, status: "initializing" };
    const promise = this.#initialize({
      setStage: (value) => {
        stage = value;
      },
    })
      .then((value) => {
        this.#value = value;
        this.#readiness = { errorCode: null, status: "ready" };
        this.#emit({
          attempt,
          correlationId,
          errorClass: "none",
          event: "oracle_public_initialization",
          latencyMs: elapsed(startedAt, this.#now),
          outcome: "success",
          publicReadCode: null,
          retryable: false,
          stage,
          terminal: false,
        });
        return value;
      })
      .catch((error: unknown) => {
        const code = publicReadCode(error);
        const mayRetry = retryable(error);
        this.#readiness = {
          errorCode: code ?? "initialization_failed",
          status: "unavailable",
        };
        this.#emit({
          attempt,
          correlationId,
          errorClass:
            error instanceof PublicReadError
              ? "public_read_error"
              : "initialization_error",
          event: "oracle_public_initialization",
          latencyMs: elapsed(startedAt, this.#now),
          outcome: "failure",
          publicReadCode: code,
          retryable: mayRetry,
          stage,
          terminal: !mayRetry,
        });
        throw error;
      })
      .finally(() => {
        if (this.#inFlight === promise) this.#inFlight = undefined;
      });
    this.#inFlight = promise;
    return promise;
  }

  readiness(): HostedReadiness {
    return { ...this.#readiness };
  }

  #emit(event: HostedInitializationDiagnostic): void {
    try {
      this.#diagnosticSink(event);
    } catch {
      // Diagnostics must never alter read-plane behavior.
    }
  }
}
