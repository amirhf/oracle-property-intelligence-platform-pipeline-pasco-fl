import "dotenv/config";

import { loadConfig } from "../services/lib/config.js";
import { deterministicId } from "../src/lib/hash.js";
import { buildPublicationDryRun } from "../src/publication/dry-run.js";

const config = loadConfig();
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  "pasco-scale-25000-v1-repeat",
]);
const first = await buildPublicationDryRun({
  dataDir: config.dataDir,
  databaseUrl: config.databaseUrl,
  runId,
});
const second = await buildPublicationDryRun({
  dataDir: config.dataDir,
  databaseUrl: config.databaseUrl,
  runId,
});
const comparable = (value: typeof first) => ({
  objectCount: value.objectCount,
  openDataBytes: value.openDataBytes,
  openDataManifestSha256: value.openDataManifestSha256,
  planSha256: value.planSha256,
  propertyCount: value.propertyCount,
  queryTableBytes: value.queryTableBytes,
  queryTableDistinctIds: value.queryTableDistinctIds,
  queryTableRows: value.queryTableRows,
  queryTableSha256: value.queryTableSha256,
  schemaSha256: value.schemaSha256,
});
if (JSON.stringify(comparable(first)) !== JSON.stringify(comparable(second))) {
  throw new Error("Publication dry-run rerun changed logical content");
}
console.log(
  JSON.stringify(
    {
      deterministic: true,
      first: comparable(first),
      second: comparable(second),
    },
    null,
    2,
  ),
);
