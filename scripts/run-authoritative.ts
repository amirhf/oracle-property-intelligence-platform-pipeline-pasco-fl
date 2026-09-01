import "dotenv/config";

import { request as httpRequest } from "node:http";
import path from "node:path";

import postgres from "postgres";

import {
  AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  AUTHORITATIVE_PARCEL_SELECTION_SEED,
} from "../src/snapshot/coverage.js";
import {
  canonicalJson,
  canonicalJsonSha256,
} from "../src/lib/canonical-json.js";
import { deterministicId } from "../src/lib/hash.js";
import { bindDataFile } from "../src/snapshot/model.js";
import { countyIngestRequestSha256 } from "../src/workflow/schemas.js";

const label = process.argv[2];
if (label !== "initial" && label !== "replay") {
  throw new Error("Usage: pnpm authoritative:run <initial|replay>");
}
const workflowId = "pasco-authoritative-appraiser-2026-08-23-v1";
const runId = deterministicId("run", [
  "1.0.0",
  "pipeline-run",
  "pasco",
  workflowId,
]);
const request = {
  asOf: "2026-08-23T11:07:02.000Z",
  county: "pasco" as const,
  runId,
  sampleAlgorithm: AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  sampleSeed: AUTHORITATIVE_PARCEL_SELECTION_SEED,
  selectionSize: 325_213 as const,
  workflowId,
};

async function replayRequest() {
  const dataDir = process.env.DATA_DIR;
  const databaseUrl = process.env.DATABASE_URL;
  if (!dataDir || !databaseUrl) {
    throw new Error("Replay requires DATA_DIR and DATABASE_URL");
  }
  const sql = postgres(databaseUrl, {
    connect_timeout: 5,
    idle_timeout: 1,
    max: 1,
  });
  try {
    const effects = await sql<
      {
        idempotency_key: string;
        prepared_input_id: string;
        result_payload: unknown;
        result_sha256: string;
        snapshot_id: string;
        status: string;
      }[]
    >`
      SELECT idempotency_key, prepared_input_id, result_payload,
             result_sha256, snapshot_id, status
      FROM oracle_loader_effects
      WHERE run_id = ${runId}
    `;
    if (effects.length !== 1 || effects[0]?.status !== "completed") {
      throw new Error("Completed authoritative Loader effect is unavailable");
    }
    const effect = effects[0];
    const manifestPath = path.join(
      dataDir,
      "pasco",
      "prepared",
      "snapshots",
      effect.snapshot_id,
      effect.prepared_input_id,
      "manifest.json",
    );
    return {
      body: {
        county: "pasco" as const,
        idempotencyKey: effect.idempotency_key,
        parentRequestSha256: countyIngestRequestSha256(request),
        prepared: {
          kind: "authoritative" as const,
          manifest: await bindDataFile(dataDir, manifestPath),
          preparedInputId: effect.prepared_input_id,
          snapshotId: effect.snapshot_id,
        },
        request,
      },
      expectedPayload: effect.result_payload,
      expectedSha256: effect.result_sha256,
    };
  } finally {
    await sql.end();
  }
}

const replay = label === "replay" ? await replayRequest() : undefined;
const requestBody = JSON.stringify(replay?.body ?? request);
const requestPath =
  label === "replay" ? "/Loader/pasco/load" : `/CountyIngest/${workflowId}/run`;
const response = await new Promise<{ body: string; status: number }>(
  (resolve, reject) => {
    const client = httpRequest(
      {
        headers: {
          "content-length": Buffer.byteLength(requestBody),
          "content-type": "application/json",
        },
        host: "127.0.0.1",
        method: "POST",
        path: requestPath,
        port: 8080,
      },
      (incoming) => {
        const chunks: Buffer[] = [];
        let bytes = 0;
        incoming.on("data", (chunk: Buffer) => {
          bytes += chunk.byteLength;
          if (bytes > 1_048_576) {
            incoming.destroy(
              new Error("Authoritative workflow response exceeds 1 MiB"),
            );
            return;
          }
          chunks.push(chunk);
        });
        incoming.on("end", () => {
          resolve({
            body: Buffer.concat(chunks).toString("utf8"),
            status: incoming.statusCode ?? 500,
          });
        });
        incoming.on("error", reject);
      },
    );
    client.setTimeout(12 * 60 * 60_000, () => {
      client.destroy(new Error("Authoritative workflow exceeded 12 hours"));
    });
    client.on("error", reject);
    client.end(requestBody);
  },
);
if (response.status < 200 || response.status >= 300) {
  throw new Error(`Authoritative ${label} request failed (${response.status})`);
}
if (replay) {
  const replayPayload: unknown = JSON.parse(response.body);
  if (
    canonicalJsonSha256(replayPayload) !== replay.expectedSha256 ||
    canonicalJson(replayPayload) !== canonicalJson(replay.expectedPayload)
  ) {
    throw new Error("Authoritative Loader replay result changed");
  }
}
console.log(response.body);
