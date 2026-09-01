import "dotenv/config";

import { readFile } from "node:fs/promises";
import path from "node:path";

import type { CandidateSourceSnapshotBuildDescriptor } from "../src/publication/candidate-source-snapshot-build.js";
import { CandidateSourceSnapshotUploadError } from "../src/publication/candidate-source-snapshot-upload.js";
import {
  executeCandidateSourceSnapshotSession2,
  type CandidateSourceSnapshotSession2Authorization,
} from "../src/publication/candidate-source-snapshot-session2.js";

async function main(): Promise<void> {
  const descriptorArgument = process.argv[2];
  if (!descriptorArgument) {
    throw new Error(
      "Usage: execute-candidate-source-snapshot-demo <descriptor.json> [authorization.json]",
    );
  }
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) throw new Error("DATABASE_URL is required");
  const descriptor = JSON.parse(
    await readFile(path.resolve(descriptorArgument), "utf8"),
  ) as CandidateSourceSnapshotBuildDescriptor;
  const executorEnabled =
    process.env.CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED?.trim() === "true";
  const authorizationArgument = process.argv[3];
  if (executorEnabled && !authorizationArgument) {
    throw new Error(
      "An exact authorization JSON file is required when the executor is enabled",
    );
  }
  const authorization =
    executorEnabled && authorizationArgument
      ? (JSON.parse(
          await readFile(path.resolve(authorizationArgument), "utf8"),
        ) as CandidateSourceSnapshotSession2Authorization)
      : undefined;
  const executorLeaseHolderToken =
    process.env.CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_TOKEN?.trim();
  const result = await executeCandidateSourceSnapshotSession2({
    ...(authorization ? { authorization } : {}),
    databaseUrl,
    descriptor,
    environment: process.env,
    ...((authorization?.uploadContinuationAuthorization ||
      authorization?.uploadResumeAuthorization) &&
    executorLeaseHolderToken
      ? { executorLeaseHolderToken }
      : {}),
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
        : result.status === "upload_resume_paused"
          ? {
              executorEnabled: false,
              status: result.status,
              summary: result.summary,
            }
          : {
              cutover: result.cutover,
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
      ? {
          failureClass: error.evidence.failureClass,
          outcome: error.outcome,
          stage: error.evidence.stage,
        }
      : error instanceof Error
        ? { failureClass: "terminal", outcome: error.name, stage: "unknown" }
        : {
            failureClass: "terminal",
            outcome: "UnknownError",
            stage: "unknown",
          };
  // The execution CLI deliberately emits no raw exception, environment value,
  // credential, local path, provider body, or remote response.
  process.stderr.write(
    `${JSON.stringify({ ...classification, status: "failed_closed" })}\n`,
  );
  process.exitCode = 1;
});
