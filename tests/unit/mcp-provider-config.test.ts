import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { loadMcpConfig } from "../../src/mcp/config.js";
import {
  resolveArtifactPath,
  verifyParquetMagic,
} from "../../src/mcp/provider.js";

describe("Oracle MCP provider isolation", () => {
  it("requires explicit local paths and rejects local artifacts in production", () => {
    expect(() =>
      loadMcpConfig({
        NODE_ENV: "development",
        ORACLE_MCP_PROVIDER: "local-artifact",
        DATA_DIR: path.resolve("data"),
      }),
    ).toThrow("MCP_LOCAL_MANIFEST_PATH is required");
    expect(() =>
      loadMcpConfig({
        NODE_ENV: "production",
        ORACLE_MCP_PROVIDER: "local-artifact",
        DATA_DIR: path.resolve("data"),
        MCP_LOCAL_MANIFEST_PATH: "manifest.json",
        MCP_LOCAL_PARQUET_PATH: "query.parquet",
      }),
    ).toThrow("Production rejects the local-artifact MCP provider");
  });

  it("requires complete hash-bound public publication configuration", () => {
    expect(() =>
      loadMcpConfig({
        NODE_ENV: "production",
        ORACLE_MCP_PROVIDER: "public-ipns",
      }),
    ).toThrow("MCP_OPEN_DATA_IPNS is required");
    expect(() =>
      loadMcpConfig({
        NODE_ENV: "production",
        ORACLE_MCP_PROVIDER: "public-ipns",
        MCP_OPEN_DATA_IPNS: `k51${"a".repeat(50)}`,
        MCP_QUERY_TABLE_IPNS: `k51${"b".repeat(50)}`,
      }),
    ).toThrow("MCP_PUBLIC_MANIFEST_CID is required");
  });

  it("accepts only the closed public resolver profiles", () => {
    const environment = {
      NODE_ENV: "production",
      ORACLE_MCP_PROVIDER: "public-ipns",
      MCP_OPEN_DATA_IPNS: `k51${"a".repeat(59)}`,
      MCP_QUERY_TABLE_IPNS: `k51${"b".repeat(59)}`,
      MCP_PUBLIC_MANIFEST_CID: `Qm${"a".repeat(44)}`,
      MCP_PUBLIC_MANIFEST_SHA256: "a".repeat(64),
      MCP_PUBLIC_OPEN_DATA_ROOT_CID: `Qm${"b".repeat(44)}`,
      MCP_PUBLIC_PLAN_CID: `Qm${"c".repeat(44)}`,
      MCP_PUBLIC_PLAN_SHA256: "b".repeat(64),
      MCP_PUBLIC_QUERY_TABLE_ROOT_CID: `Qm${"d".repeat(44)}`,
      MCP_PUBLIC_CANDIDATE_DEMO_PLAN_ID: `demo_${"e".repeat(32)}`,
      MCP_PUBLIC_CANDIDATE_DEMO_PLAN_SHA256: "c".repeat(64),
      MCP_PUBLIC_CANDIDATE_SOURCE_PLAN_SHA256: "d".repeat(64),
    };
    expect(
      loadMcpConfig({
        ...environment,
        MCP_PUBLIC_RESOLVER_POLICY: "candidate_filebase_delegated_v2",
      }).provider,
    ).toMatchObject({
      resolverPolicy: "candidate_filebase_delegated_v2",
    });
    expect(() =>
      loadMcpConfig({
        ...environment,
        MCP_PUBLIC_RESOLVER_POLICY: "caller-supplied-resolver",
      }),
    ).toThrow("MCP_PUBLIC_RESOLVER_POLICY");
  });

  it("rejects arbitrary, missing, and fixture paths", async () => {
    const dataDir = path.resolve("data");
    await expect(
      resolveArtifactPath(dataDir, "../contracts/mcp-v1.schema.json"),
    ).rejects.toThrow("outside DATA_DIR");
    await expect(
      resolveArtifactPath(
        path.resolve("."),
        "contracts/fixtures/property-response.json",
      ),
    ).rejects.toThrow("forbidden");
    await expect(
      resolveArtifactPath(dataDir, "artifacts/missing-query-table.parquet"),
    ).rejects.toThrow("does not exist");
  });

  it("rejects corrupt Parquet without attempting a fallback", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "prism-mcp-parquet-"));
    const corrupt = path.join(directory, "query-table.parquet");
    await writeFile(corrupt, "not parquet", { mode: 0o600 });
    await expect(verifyParquetMagic(corrupt)).rejects.toThrow("corrupt");
  });
});
