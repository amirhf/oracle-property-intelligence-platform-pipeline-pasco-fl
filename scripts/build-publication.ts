import "dotenv/config";

import { loadConfig } from "../services/lib/config.js";
import { deterministicId } from "../src/lib/hash.js";
import { buildPublicationDryRun } from "../src/publication/dry-run.js";

const config = loadConfig();
const workflowId = "pasco-scale-25000-v1-repeat";
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  workflowId,
]);
const summary = await buildPublicationDryRun({
  dataDir: config.dataDir,
  databaseUrl: config.databaseUrl,
  runId,
});
console.log(JSON.stringify(summary, null, 2));
