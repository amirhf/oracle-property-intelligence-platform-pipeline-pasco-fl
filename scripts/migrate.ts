import "dotenv/config";

import { loadConfig } from "../services/lib/config.js";
import { runMigrations } from "../src/db/migrations.js";

const config = loadConfig();
const applied = await runMigrations(config.databaseUrl);
console.log(JSON.stringify({ applied, ok: true }, null, 2));
