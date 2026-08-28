import "dotenv/config";

import { access } from "node:fs/promises";

import * as restate from "@restatedev/restate-sdk";

import { loadConfig } from "./lib/config.js";
import { checkDatabase } from "./lib/database.js";
import { createParcelService } from "./parcel.js";
import { createPipelineServices } from "./pipeline.js";
import { runMigrations } from "../src/db/migrations.js";

async function main(): Promise<void> {
  const config = loadConfig();
  await access(config.dataDir);
  const database = await checkDatabase(config.databaseUrl);
  await runMigrations(config.databaseUrl);
  const pipeline = createPipelineServices({
    dataDir: config.dataDir,
    databaseUrl: config.databaseUrl,
  });
  const services = [
    pipeline.countyIngest,
    pipeline.ingestChunk,
    createParcelService(config.databaseUrl),
    pipeline.permitFeed,
    pipeline.permitFeedChunk,
    pipeline.permitHarvest,
    pipeline.loader,
    pipeline.publish,
    pipeline.sunbizIngest,
    pipeline.bbbHarvest,
  ];

  restate.serve({
    services,
    port: config.servicePort,
  });

  console.log(
    JSON.stringify({
      database: database.database,
      event: "oracle_foundation_started",
      port: config.servicePort,
      services: [
        "CountyIngest",
        "IngestChunk",
        "Parcel",
        "PermitFeed",
        "PermitFeedChunk",
        "PermitHarvest",
        "Loader",
        "Publish",
        "SunbizIngest",
        "BbbHarvest",
      ],
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
