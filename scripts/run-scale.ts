import "dotenv/config";

import { deterministicId } from "../src/lib/hash.js";
import {
  COUNTYWIDE_SAMPLE_ALGORITHM,
  COUNTYWIDE_SAMPLE_SEED,
} from "../src/scale/sample.js";

const rawSize = process.argv[2];
const label = process.argv[3];
const selectionSize = Number(rawSize);
if (
  (selectionSize !== 5_000 && selectionSize !== 25_000) ||
  (label !== "initial" && label !== "repeat")
) {
  throw new Error("Usage: pnpm scale:run <5000|25000> <initial|repeat>");
}

const workflowId = `pasco-scale-${selectionSize}-v1-${label}`;
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
  sampleAlgorithm: COUNTYWIDE_SAMPLE_ALGORITHM,
  sampleSeed: COUNTYWIDE_SAMPLE_SEED,
  selectionSize,
  workflowId,
};
const response = await fetch(
  `http://localhost:8080/CountyIngest/${workflowId}/run`,
  {
    body: JSON.stringify(request),
    headers: { "content-type": "application/json" },
    method: "POST",
    signal: AbortSignal.timeout(2 * 60 * 60_000),
  },
);
const body = await response.text();
if (!response.ok) {
  throw new Error(`Scale workflow failed (${response.status}): ${body}`);
}
console.log(body);
