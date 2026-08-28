import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { loadMcpConfig } from "../../src/mcp/config.js";
import { McpContractRegistry } from "../../src/mcp/contracts.js";
import {
  createMcpProvider,
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

  it("requires both public publication names and does not contact IPNS", async () => {
    expect(() =>
      loadMcpConfig({
        NODE_ENV: "production",
        ORACLE_MCP_PROVIDER: "public-ipns",
      }),
    ).toThrow("MCP_OPEN_DATA_IPNS is required");
    const config = loadMcpConfig({
      NODE_ENV: "production",
      ORACLE_MCP_PROVIDER: "public-ipns",
      MCP_OPEN_DATA_IPNS: "k51-public-open-data",
      MCP_QUERY_TABLE_IPNS: "k51-public-query-table",
    });
    const contracts = await McpContractRegistry.create();
    await expect(createMcpProvider(config.provider, contracts)).rejects.toThrow(
      "not configured for this local checkpoint",
    );
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
