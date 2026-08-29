import "dotenv/config";

import { readFile } from "node:fs/promises";

import postgres from "postgres";

import { loadConfig } from "../services/lib/config.js";
import type { PilotRunSummary, PreparedPilot } from "../src/domain/types.js";
import { sha256 } from "../src/lib/hash.js";
import { scalePreparedPath } from "../src/scale/prepare.js";

const selectionSize = Number(process.argv[2]);
if (selectionSize !== 5_000 && selectionSize !== 25_000) {
  throw new Error("Usage: pnpm scale:check <5000|25000>");
}

const workflows = [
  `pasco-scale-${selectionSize}-v1-initial`,
  `pasco-scale-${selectionSize}-v1-repeat`,
];
const config = loadConfig();
const sql = postgres(config.databaseUrl, { max: 1 });
try {
  const runs = await sql<
    {
      result_counts: PilotRunSummary;
      run_id: string;
      status: string;
      coverage_mode: string;
      workflow_id: string;
    }[]
  >`
    SELECT run_id, workflow_id, status, coverage_mode, result_counts
    FROM oracle_pipeline_runs
    WHERE workflow_id = ANY(${workflows})
    ORDER BY workflow_id
  `;
  if (
    runs.length !== 2 ||
    runs.some(
      (run) => run.status !== "completed" || run.coverage_mode !== "sample",
    )
  ) {
    throw new Error(`Both ${selectionSize}-property scale runs must complete`);
  }
  const initial = runs.find((run) => run.workflow_id.endsWith("initial"));
  const repeat = runs.find((run) => run.workflow_id.endsWith("repeat"));
  if (!initial || !repeat) throw new Error("Scale run pair is incomplete");

  for (const run of runs) {
    const result = run.result_counts;
    if (
      result.acceptedProperties !== selectionSize ||
      result.selectionSize !== selectionSize ||
      result.duplicateProperties !== 0 ||
      result.permits !== 0 ||
      result.permitRequestCount !== 0 ||
      result.roofSignals !== selectionSize ||
      result.explicitUnavailableFacts !== selectionSize * 6
    ) {
      throw new Error(`Scale reconciliation failed for ${run.workflow_id}`);
    }
    const unexpectedStatuses = Object.keys(
      result.gisMetrics.statusCounts,
    ).filter((status) => status !== "200" && status !== "checkpoint");
    if (unexpectedStatuses.length > 0) {
      throw new Error(
        `Unexpected GIS statuses for ${run.workflow_id}: ${unexpectedStatuses.join(",")}`,
      );
    }
  }
  if (
    repeat.result_counts.newProperties !== 0 ||
    repeat.result_counts.changedProperties !== 0 ||
    repeat.result_counts.unchangedProperties !== selectionSize
  ) {
    throw new Error("Scale repeat is not idempotent");
  }

  const preparedPath = scalePreparedPath(config.dataDir, repeat.run_id);
  const [preparedText, readyText] = await Promise.all([
    readFile(preparedPath, "utf8"),
    readFile(`${preparedPath}.ready.json`, "utf8"),
  ]);
  const prepared = JSON.parse(preparedText) as PreparedPilot;
  const ready = JSON.parse(readyText) as {
    properties: number;
    ready: boolean;
    selectionHash: string;
  };
  const selectionHash = sha256(
    JSON.stringify(
      prepared.properties.map((property) => property.parcel.exactFolio).sort(),
    ),
  );
  if (
    prepared.selectionSize !== selectionSize ||
    prepared.properties.length !== selectionSize ||
    ready.ready !== true ||
    ready.properties !== selectionSize ||
    ready.selectionHash !== selectionHash
  ) {
    throw new Error("Scaled prepared checkpoint failed scope validation");
  }
  const scopedPropertyIds = prepared.properties.map(
    (property) => property.propertyId,
  );

  const scopedCounts = await sql<
    {
      active: number;
      canonical: number;
      contractors: number;
      distinct_ids: number;
      inactive: number;
      permits: number;
      tombstones: number;
    }[]
  >`
    SELECT
      count(*)::int AS canonical,
      count(DISTINCT property_id)::int AS distinct_ids,
      count(*) FILTER (WHERE is_active)::int AS active,
      count(*) FILTER (WHERE NOT is_active)::int AS inactive,
      (SELECT count(*)::int FROM oracle_permits) AS permits,
      (SELECT count(*)::int FROM oracle_contractors) AS contractors,
      (SELECT count(*)::int
       FROM oracle_property_lifecycle_events
       WHERE property_id = ANY(${scopedPropertyIds})
         AND event_type = 'inactivated') AS tombstones
    FROM oracle_properties
    WHERE property_id = ANY(${scopedPropertyIds})
  `;
  const counts = scopedCounts[0];
  if (
    counts?.canonical !== selectionSize ||
    counts.distinct_ids !== selectionSize ||
    counts.active !== selectionSize ||
    counts.inactive !== 0 ||
    counts.tombstones !== 0 ||
    counts.permits !== 0 ||
    counts.contractors !== 0
  ) {
    throw new Error("Scaled canonical scope or unavailable sources diverged");
  }

  const maxMissingCoordinates = Math.ceil(selectionSize * 0.02);
  const gatePassed =
    repeat.result_counts.missingCoordinates <= maxMissingCoordinates &&
    repeat.result_counts.diskAvailableBytes >= 5 * 1024 ** 3 &&
    repeat.result_counts.peakRssBytes < 2 * 1024 ** 3;
  if (!gatePassed) {
    throw new Error(
      `${selectionSize}-property resource/coordinate gate failed`,
    );
  }

  console.log(
    JSON.stringify(
      {
        canonicalScope: counts,
        gatePassed,
        selectionHash,
        initial: {
          coverageMode: initial.coverage_mode,
          result: initial.result_counts,
          runId: initial.run_id,
          workflowId: initial.workflow_id,
        },
        repeat: {
          coverageMode: repeat.coverage_mode,
          result: repeat.result_counts,
          runId: repeat.run_id,
          workflowId: repeat.workflow_id,
        },
      },
      null,
      2,
    ),
  );
} finally {
  await sql.end({ timeout: 5 });
}
