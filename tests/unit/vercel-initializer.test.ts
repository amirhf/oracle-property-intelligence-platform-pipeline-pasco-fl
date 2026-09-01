import type { IncomingMessage, ServerResponse } from "node:http";

import { describe, expect, it } from "vitest";

import {
  createHostedOracleEntrypoint,
  type HostedRequestHandler,
} from "../../api/index.js";
import type { HostedInitializationDiagnostic } from "../../src/mcp/hosted-initializer.js";
import { PublicReadError } from "../../src/mcp/public-ipns-provider.js";

function deferred<T>(): {
  promise: Promise<T>;
  reject(error: unknown): void;
  resolve(value: T): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

const initializedHandler: HostedRequestHandler = async (_request, response) => {
  response.writeHead(204);
  response.end();
};

async function invoke(
  handler: HostedRequestHandler,
  method: string,
  url: string,
): Promise<{ body: unknown; status: number }> {
  let body = "";
  let status = 0;
  const response = {
    end(this: { writableEnded: boolean }, value?: string) {
      body += value ?? "";
      this.writableEnded = true;
    },
    headersSent: false,
    writableEnded: false,
    writeHead(this: { headersSent: boolean }, value: number) {
      status = value;
      this.headersSent = true;
      return this;
    },
  } as unknown as ServerResponse;
  await handler(
    { headers: {}, method, url } as unknown as IncomingMessage,
    response,
  );
  return { body: body ? JSON.parse(body) : null, status };
}

async function readiness(handler: HostedRequestHandler): Promise<{
  errorCode: string | null;
  status: string;
}> {
  const response = await invoke(handler, "GET", "/health");
  expect(response.status).toBe(200);
  const body = response.body as {
    readiness: { errorCode: string | null; status: string };
  };
  return body.readiness;
}

describe("Vercel recoverable initialization", () => {
  it("serves liveness before and during one shared initialization, then caches success", async () => {
    const pending = deferred<HostedRequestHandler>();
    let calls = 0;
    const handler = createHostedOracleEntrypoint({
      diagnosticSink: () => undefined,
      initialize: async ({ setStage }) => {
        calls += 1;
        setStage("ipns_resolution");
        return pending.promise;
      },
    });

    await expect(readiness(handler)).resolves.toEqual({
      errorCode: null,
      status: "not_checked",
    });
    const first = invoke(handler, "POST", "/mcp");
    while (calls === 0) await new Promise((resolve) => setTimeout(resolve, 1));
    const second = invoke(handler, "POST", "/mcp");
    await expect(readiness(handler)).resolves.toEqual({
      errorCode: null,
      status: "initializing",
    });
    pending.resolve(initializedHandler);
    expect((await first).status).toBe(204);
    expect((await second).status).toBe(204);
    expect(calls).toBe(1);
    await expect(readiness(handler)).resolves.toEqual({
      errorCode: null,
      status: "ready",
    });
    expect((await invoke(handler, "POST", "/mcp")).status).toBe(204);
    expect(calls).toBe(1);
  });

  it("clears a shared rejection, reports bounded diagnostics, and recovers", async () => {
    const pending = deferred<HostedRequestHandler>();
    const diagnostics: HostedInitializationDiagnostic[] = [];
    let calls = 0;
    const handler = createHostedOracleEntrypoint({
      diagnosticSink: (event) => diagnostics.push(event),
      initialize: async ({ setStage }) => {
        calls += 1;
        setStage("plan");
        if (calls === 1) return pending.promise;
        return initializedHandler;
      },
    });

    const first = invoke(handler, "POST", "/mcp");
    while (calls === 0) await new Promise((resolve) => setTimeout(resolve, 1));
    const second = invoke(handler, "POST", "/mcp");
    pending.reject(
      new PublicReadError(
        "timeout",
        "authorization=Bearer conspicuous-secret response-body",
        true,
      ),
    );
    expect((await first).status).toBe(503);
    expect((await second).status).toBe(503);
    expect(calls).toBe(1);
    await expect(readiness(handler)).resolves.toEqual({
      errorCode: "timeout",
      status: "unavailable",
    });

    expect((await invoke(handler, "POST", "/mcp")).status).toBe(204);
    expect(calls).toBe(2);
    await expect(readiness(handler)).resolves.toEqual({
      errorCode: null,
      status: "ready",
    });
    expect(diagnostics).toEqual([
      expect.objectContaining({
        attempt: 1,
        errorClass: "public_read_error",
        outcome: "failure",
        publicReadCode: "timeout",
        retryable: true,
        stage: "plan",
        terminal: false,
      }),
      expect.objectContaining({
        attempt: 2,
        errorClass: "none",
        outcome: "success",
        publicReadCode: null,
        stage: "plan",
      }),
    ]);
    expect(JSON.stringify(diagnostics)).not.toMatch(
      /authorization|bearer|conspicuous-secret|response-body/i,
    );
  });

  it("starts the request deadline while a shared cold initialization is pending", async () => {
    const pending = deferred<HostedRequestHandler>();
    const handler = createHostedOracleEntrypoint({
      diagnosticSink: () => undefined,
      initialize: async () => pending.promise,
      requestTimeoutMs: 10,
    });
    const startedAt = performance.now();
    const response = await invoke(handler, "POST", "/mcp");
    expect(response.status).toBe(503);
    expect(performance.now() - startedAt).toBeLessThan(500);
    pending.resolve(initializedHandler);
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
});
