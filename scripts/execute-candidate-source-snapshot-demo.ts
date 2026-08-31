import "dotenv/config";

import { readFile } from "node:fs/promises";
import path from "node:path";

import type { CandidateSourceSnapshotBuildDescriptor } from "../src/publication/candidate-source-snapshot-build.js";
import { CandidateSourceSnapshotUploadError } from "../src/publication/candidate-source-snapshot-upload.js";
import { executeCandidateSourceSnapshotSession2 } from "../src/publication/candidate-source-snapshot-session2.js";

async function main(): Promise<void> {
  const descriptorArgument = process.argv[2];
  if (!descriptorArgument) {
    throw new Error(
      "Usage: execute-candidate-source-snapshot-demo <descriptor.json>",
    );
  }
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) throw new Error("DATABASE_URL is required");
  const descriptor = JSON.parse(
    await readFile(path.resolve(descriptorArgument), "utf8"),
  ) as CandidateSourceSnapshotBuildDescriptor;
  const result = await executeCandidateSourceSnapshotSession2({
    databaseUrl,
    descriptor,
    environment: process.env,
  });
  process.stdout.write(
    `${JSON.stringify(
      result.status === "executor_disabled"
        ? {
            executorEnabled: false,
            planId: result.planId,
            planSha256: result.planSha256,
            status: result.status,
          }
        : {
            durableState: result.durableState.state,
            executorEnabled: false,
            status: result.status,
            summary: result.summary,
          },
      null,
      2,
    )}\n`,
  );
}

await main().catch((error: unknown) => {
  const classification =
    error instanceof CandidateSourceSnapshotUploadError
      ? error.outcome
      : error instanceof Error
        ? error.name
        : "UnknownError";
  // The execution CLI deliberately emits no raw exception, environment value,
  // credential, local path, provider body, or remote response.
  process.stderr.write(
    `${JSON.stringify({ classification, status: "failed_closed" })}\n`,
  );
  process.exitCode = 1;
});
