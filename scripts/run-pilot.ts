import "dotenv/config";

import { deterministicId } from "../src/lib/hash.js";

const label = process.argv[2];
if (label !== "initial" && label !== "repeat") {
  throw new Error("Usage: pnpm pilot:run <initial|repeat>");
}

const sampleSeed = "prism-pasco-real-pilot-2026-08-28";
const workflowId = `pasco-real-pilot-v1-appraisal-${label}`;
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  workflowId,
]);
const request = {
  asOf: "2026-08-28T00:00:00.000Z",
  county: "pasco",
  runId,
  sampleAlgorithm: "pasco-pilot-stratified-v1",
  sampleSeed,
  selectionSize: 25,
  workflowId,
};
const response = await fetch(
  `http://localhost:8080/CountyIngest/${workflowId}/run`,
  {
    body: JSON.stringify(request),
    headers: { "content-type": "application/json" },
    method: "POST",
    signal: AbortSignal.timeout(15 * 60_000),
  },
);
const body = await response.text();
if (!response.ok) {
  throw new Error(`Pilot workflow failed (${response.status}): ${body}`);
}
console.log(body);
