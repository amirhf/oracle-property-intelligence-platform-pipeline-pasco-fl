import "dotenv/config";

import {
  buildAuthoritativeLocalPublication,
  preflightAuthoritativeLocalPublication,
} from "../src/publication/authoritative-local.js";

const dataDir = process.env.DATA_DIR?.trim();
const databaseUrl = process.env.DATABASE_URL?.trim();
if (!dataDir || !databaseUrl) {
  throw new Error("DATA_DIR and DATABASE_URL are required");
}
const command = process.argv[2] ?? "build";
if (!new Set(["build", "determinism", "preflight", "verify"]).has(command)) {
  throw new Error(
    "Usage: pnpm authoritative:publication:{preflight|build|verify|determinism}",
  );
}

if (command === "preflight") {
  console.log(
    JSON.stringify(
      await preflightAuthoritativeLocalPublication({ dataDir, databaseUrl }),
      null,
      2,
    ),
  );
} else {
  const run = async () =>
    buildAuthoritativeLocalPublication({
      dataDir,
      databaseUrl,
      onProgress: (progress) => {
        console.log(
          JSON.stringify({ event: "publication_progress", ...progress }),
        );
      },
    });
  const first = await run();
  if (command === "determinism") {
    const second = await run();
    const identity = (value: typeof first) => ({
      inventoryBytes: value.inventoryBytes,
      inventoryObjectCount: value.inventoryObjectCount,
      manifestCid: value.manifestCid,
      manifestSha256: value.manifestSha256,
      openDataRootCid: value.openDataRootCid,
      parquetBytes: value.parquetBytes,
      parquetCid: value.parquetCid,
      parquetSha256: value.parquetSha256,
      planArtifactCid: value.planArtifactCid,
      planArtifactSha256: value.planArtifactSha256,
      planId: value.planId,
      planSha256: value.planSha256,
      propertyBytes: value.propertyBytes,
      propertyCount: value.propertyCount,
      shardCount: value.shardCount,
    });
    if (JSON.stringify(identity(first)) !== JSON.stringify(identity(second))) {
      throw new Error(
        "Authoritative publication deterministic rebuild changed",
      );
    }
    console.log(
      JSON.stringify(
        {
          deterministic: true,
          first: identity(first),
          second: identity(second),
        },
        null,
        2,
      ),
    );
  } else {
    console.log(JSON.stringify(first, null, 2));
  }
}
