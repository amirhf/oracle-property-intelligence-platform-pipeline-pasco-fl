import "dotenv/config";

import { access } from "node:fs/promises";

import * as restate from "@restatedev/restate-sdk";

import { loadConfig } from "./lib/config.js";
import { checkDatabase } from "./lib/database.js";
import { createParcelService } from "./parcel.js";

async function main(): Promise<void> {
  const config = loadConfig();
  await access(config.dataDir);
  const database = await checkDatabase(config.databaseUrl);

  restate.serve({
    services: [createParcelService(config.databaseUrl)],
    port: config.servicePort,
  });

  console.log(
    JSON.stringify({
      database: database.database,
      event: "oracle_foundation_started",
      port: config.servicePort,
      services: ["Parcel"],
    }),
  );
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? error.message : "Unknown startup error";
  console.error(
    JSON.stringify({ event: "oracle_foundation_start_failed", message }),
  );
  process.exitCode = 1;
});
