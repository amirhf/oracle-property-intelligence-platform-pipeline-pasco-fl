import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import postgres from "postgres";

export async function runMigrations(
  databaseUrl: string,
  migrationsDir = path.resolve("migrations"),
): Promise<string[]> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql`
      CREATE TABLE IF NOT EXISTS oracle_schema_migrations (
        filename text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `;
    const filenames = (await readdir(migrationsDir))
      .filter((name) => name.endsWith(".sql"))
      .sort();
    const applied: string[] = [];
    for (const filename of filenames) {
      const existing = await sql<{ filename: string }[]>`
        SELECT filename FROM oracle_schema_migrations WHERE filename = ${filename}
      `;
      if (existing.length > 0) continue;
      const migration = await readFile(
        path.join(migrationsDir, filename),
        "utf8",
      );
      await sql.begin(async (transaction) => {
        await transaction.unsafe(migration);
        await transaction`
          INSERT INTO oracle_schema_migrations (filename) VALUES (${filename})
        `;
      });
      applied.push(filename);
    }
    return applied;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
