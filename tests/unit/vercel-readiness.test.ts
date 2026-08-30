import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

describe("Vercel public read-plane readiness", () => {
  it("routes only the documented read-only Oracle surfaces", async () => {
    const config = JSON.parse(
      await readFile(path.resolve("vercel.json"), "utf8"),
    ) as {
      functions: Record<string, unknown>;
      rewrites: Array<{ destination: string; source: string }>;
    };
    expect(Object.keys(config.functions)).toEqual(["api/index.ts"]);
    expect(config.rewrites).toEqual([
      { destination: "/api/index", source: "/" },
      { destination: "/api/index", source: "/health" },
      { destination: "/api/index", source: "/mcp" },
      { destination: "/api/index", source: "/explorer/api/bootstrap" },
      { destination: "/api/index", source: "/explorer/api/search" },
      { destination: "/api/index", source: "/explorer/api/property" },
    ]);
    expect(JSON.stringify(config)).not.toMatch(
      /filebase|secret|token|database|local-artifact/i,
    );
  });
});
