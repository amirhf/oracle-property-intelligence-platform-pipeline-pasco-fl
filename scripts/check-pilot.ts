import "dotenv/config";

import postgres from "postgres";

import { loadConfig } from "../services/lib/config.js";

const expectedWorkflows = [
  "pasco-real-pilot-v1-appraisal-initial",
  "pasco-real-pilot-v1-appraisal-repeat",
] as const;
const config = loadConfig();
const sql = postgres(config.databaseUrl, { max: 1 });
try {
  const runs = await sql<
    {
      result_counts: {
        acceptedProperties: number;
        coordinates: number;
        duplicateProperties: number;
        newProperties: number;
        ownership: number;
        permits: number;
        unchangedProperties: number;
      };
      run_id: string;
      status: string;
      workflow_id: string;
    }[]
  >`
    SELECT run_id, workflow_id, status, result_counts
    FROM oracle_pipeline_runs
    WHERE workflow_id = ANY(${[...expectedWorkflows]})
    ORDER BY workflow_id
  `;
  if (runs.length !== 2 || runs.some((run) => run.status !== "completed")) {
    throw new Error("Both bounded pilot runs must be completed");
  }
  const initial = runs.find((run) => run.workflow_id.endsWith("initial"));
  const repeat = runs.find((run) => run.workflow_id.endsWith("repeat"));
  if (!initial || !repeat) throw new Error("Pilot run pair is incomplete");
  for (const run of runs) {
    if (
      run.result_counts.acceptedProperties !== 25 ||
      run.result_counts.coordinates !== 25 ||
      run.result_counts.ownership !== 25 ||
      run.result_counts.duplicateProperties !== 0 ||
      run.result_counts.permits !== 0
    ) {
      throw new Error(`Pilot reconciliation failed for ${run.workflow_id}`);
    }
  }
  if (
    initial.result_counts.newProperties !== 25 ||
    repeat.result_counts.newProperties !== 0 ||
    repeat.result_counts.unchangedProperties !== 25
  ) {
    throw new Error("Pilot idempotency delta reconciliation failed");
  }
  const availability = await sql<{ count: number }[]>`
    SELECT count(*)::int AS count
    FROM oracle_property_availability
    WHERE availability = 'unavailable'
      AND feature = ANY(${[
        "permits",
        "contractors",
        "phones",
        "emails",
        "sunbiz",
        "bbb",
      ]})
  `;
  if (availability[0]?.count !== 150) {
    throw new Error("Per-property unavailable semantics are incomplete");
  }
  console.log(
    JSON.stringify(
      {
        idempotent: true,
        explicitUnavailableFacts: availability[0]?.count ?? 0,
        permitAvailability: "source_unavailable_after_challenge",
        runs: runs.map((run) => ({
          resultCounts: run.result_counts,
          runId: run.run_id,
          workflowId: run.workflow_id,
        })),
      },
      null,
      2,
    ),
  );
} finally {
  await sql.end({ timeout: 5 });
}
