import type { IncomingMessage } from "node:http";

export const HOSTED_RATE_LIMIT_POLICY = Object.freeze({
  maximumTrackedClients: 4_096,
  requestsPerWindow: 60,
  windowMs: 60_000,
});

interface WindowState {
  count: number;
  expiresAt: number;
}

/**
 * A bounded per-instance guard used as defense in depth. Vercel Functions do
 * not share process memory globally, so the production authority boundary is
 * the matching Vercel Firewall per-IP rule documented in public-read-plane.md.
 */
export class HostedRateLimitGuard {
  readonly #clients = new Map<string, WindowState>();
  readonly #now: () => number;

  constructor(now: () => number = Date.now) {
    this.#now = now;
  }

  allow(client: string): boolean {
    const now = this.#now();
    const existing = this.#clients.get(client);
    if (!existing || existing.expiresAt <= now) {
      this.#evict(now);
      this.#clients.set(client, {
        count: 1,
        expiresAt: now + HOSTED_RATE_LIMIT_POLICY.windowMs,
      });
      return true;
    }
    if (existing.count >= HOSTED_RATE_LIMIT_POLICY.requestsPerWindow) {
      return false;
    }
    existing.count += 1;
    // Refresh insertion order so eviction removes the oldest client window.
    this.#clients.delete(client);
    this.#clients.set(client, existing);
    return true;
  }

  get trackedClients(): number {
    return this.#clients.size;
  }

  #evict(now: number): void {
    for (const [client, value] of this.#clients) {
      if (value.expiresAt <= now) this.#clients.delete(client);
    }
    while (
      this.#clients.size >= HOSTED_RATE_LIMIT_POLICY.maximumTrackedClients
    ) {
      const oldest = this.#clients.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.#clients.delete(oldest);
    }
  }
}

export function hostedClientKey(request: IncomingMessage): string {
  const forwarded =
    request.headers["x-vercel-forwarded-for"] ??
    request.headers["x-forwarded-for"];
  const candidate = Array.isArray(forwarded)
    ? forwarded[0]
    : forwarded?.split(",", 1)[0];
  const value = candidate?.trim() ?? "";
  return /^[0-9a-f:.]{2,64}$/i.test(value) ? value : "unattributed";
}
