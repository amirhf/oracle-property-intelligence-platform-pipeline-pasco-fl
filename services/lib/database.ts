import postgres from "postgres";

export interface DatabaseHealth {
  database: string;
  ok: true;
}

export async function checkDatabase(
  databaseUrl: string,
): Promise<DatabaseHealth> {
  const sql = postgres(databaseUrl, {
    connect_timeout: 5,
    idle_timeout: 1,
    max: 1,
  });

  try {
    const rows = await sql<
      { database: string }[]
    >`select current_database() as database`;
    const database = rows[0]?.database;
    if (!database) {
      throw new Error("PostgreSQL health query returned no database name");
    }
    return { database, ok: true };
  } finally {
    await sql.end({ timeout: 1 });
  }
}
