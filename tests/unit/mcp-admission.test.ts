import type { IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import {
  hostedClientKey,
  HostedRateLimitGuard,
  HOSTED_RATE_LIMIT_POLICY,
} from "../../src/mcp/admission.js";

describe("hosted MCP admission guard", () => {
  it("enforces the bounded per-instance window and admits after expiry", () => {
    let now = 1_000;
    const guard = new HostedRateLimitGuard(() => now);
    for (
      let attempt = 0;
      attempt < HOSTED_RATE_LIMIT_POLICY.requestsPerWindow;
      attempt += 1
    ) {
      expect(guard.allow("198.51.100.10")).toBe(true);
    }
    expect(guard.allow("198.51.100.10")).toBe(false);
    now += HOSTED_RATE_LIMIT_POLICY.windowMs;
    expect(guard.allow("198.51.100.10")).toBe(true);
  });

  it("bounds tracked client state and never trusts arbitrary forwarded text", () => {
    const guard = new HostedRateLimitGuard(() => 1_000);
    for (
      let index = 0;
      index < HOSTED_RATE_LIMIT_POLICY.maximumTrackedClients + 10;
      index += 1
    ) {
      expect(guard.allow(`client-${index}`)).toBe(true);
    }
    expect(guard.trackedClients).toBe(
      HOSTED_RATE_LIMIT_POLICY.maximumTrackedClients,
    );
    expect(
      hostedClientKey({
        headers: { "x-forwarded-for": "198.51.100.9, 203.0.113.4" },
      } as unknown as IncomingMessage),
    ).toBe("198.51.100.9");
    expect(
      hostedClientKey({
        headers: { "x-forwarded-for": "secret@example.test" },
      } as unknown as IncomingMessage),
    ).toBe("unattributed");
  });
});
