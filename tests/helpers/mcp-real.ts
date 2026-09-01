import path from "node:path";

import { McpContractRegistry } from "../../src/mcp/contracts.js";
import { LocalArtifactProvider } from "../../src/mcp/provider.js";
import { OracleMcpRuntime } from "../../src/mcp/runtime.js";

export const REAL_PUBLICATION_ROOT =
  "artifacts/publish/pasco/dry-run-run_aa8fca42c963998c1f43d5c08409e0c7";

let harnessPromise:
  | Promise<{
      contracts: McpContractRegistry;
      provider: LocalArtifactProvider;
      runtime: OracleMcpRuntime;
    }>
  | undefined;

export function realMcpHarness() {
  harnessPromise ??= (async () => {
    const contracts = await McpContractRegistry.create();
    const provider = await LocalArtifactProvider.create(
      {
        dataDir: path.resolve("data"),
        environment: "test",
        manifestPath: `${REAL_PUBLICATION_ROOT}/open-data/manifest.json`,
        mode: "local-artifact",
        parquetPath: `${REAL_PUBLICATION_ROOT}/query/query-tables/pasco/query-table.parquet`,
      },
      contracts,
    );
    return {
      contracts,
      provider,
      runtime: new OracleMcpRuntime(provider, contracts, {
        maxRequestBytes: 65_536,
        maxResponseBytes: 2 * 1024 * 1024,
        requestTimeoutMs: 10_000,
      }),
    };
  })();
  return harnessPromise;
}

export function coordinatesSearch(options: {
  cursor?: string;
  latitude: number;
  limit?: number;
  longitude: number;
  radiusKilometers?: number;
}) {
  return {
    county: "pasco",
    center: {
      kind: "coordinates",
      latitude: options.latitude,
      longitude: options.longitude,
    },
    radius: { value: options.radiusKilometers ?? 80, unit: "km" },
    filters: {
      roofAge: {
        operator: "gte",
        years: 0,
        basis: "direct_or_proxy",
      },
      matchMode: "all",
    },
    sort: "distance_asc",
    page: {
      limit: options.limit ?? 10,
      ...(options.cursor ? { cursor: options.cursor } : {}),
    },
  };
}
