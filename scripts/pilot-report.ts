import "dotenv/config";

import postgres from "postgres";

import { loadConfig } from "../services/lib/config.js";

const config = loadConfig();
const sql = postgres(config.databaseUrl, { max: 1 });
try {
  const runs = await sql<
    {
      completed_at: string;
      result_counts: Record<string, unknown>;
      run_id: string;
      status: string;
      workflow_id: string;
    }[]
  >`
    SELECT run_id, workflow_id, status, completed_at, result_counts
    FROM oracle_pipeline_runs
    ORDER BY started_at
  `;
  const duplicates = await sql<{ count: number }[]>`
    SELECT count(*)::int AS count FROM (
      SELECT exact_folio FROM oracle_properties
      GROUP BY exact_folio HAVING count(*) > 1
    ) duplicate_folios
  `;
  const examples = await sql<
    {
      basis: string;
      latitude: number | null;
      longitude: number | null;
      permit_count: number;
      property_id: string;
      property_use_description: string | null;
      year_built: number;
    }[]
  >`
    SELECT p.property_id, p.property_use_description, p.year_built,
           c.latitude, c.longitude, r.basis,
           count(pm.permit_id)::int AS permit_count
    FROM oracle_properties p
    LEFT JOIN oracle_coordinates c USING (property_id)
    LEFT JOIN oracle_roof_signals r USING (property_id)
    LEFT JOIN oracle_permits pm USING (property_id)
    GROUP BY p.property_id, p.property_use_description, p.year_built,
             c.latitude, c.longitude, r.basis
    ORDER BY p.property_id
    LIMIT 3
  `;
  console.log(
    JSON.stringify(
      {
        duplicateExactFolios: duplicates[0]?.count ?? 0,
        examples,
        runs,
      },
      null,
      2,
    ),
  );
} finally {
  await sql.end({ timeout: 5 });
}
