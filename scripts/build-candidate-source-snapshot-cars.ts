import { readFile } from "node:fs/promises";
import path from "node:path";

import {
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
  type CandidateSourceSnapshotBuildDescriptor,
} from "../src/publication/candidate-source-snapshot-build.js";
import { buildCandidateSourceSnapshotCars } from "../src/publication/candidate-source-snapshot-car.js";

const DEFAULT_OUTPUT_DIRECTORY =
  "data/evidence/candidate-source-snapshot-demo/cars";

async function main(): Promise<void> {
  const arguments_ = process.argv.slice(2);
  const descriptorArgument = arguments_[0];
  const outputArgument = arguments_[1];
  const implementationCommitSha = arguments_[2];
  const recordedAt = arguments_[3];
  if (
    !descriptorArgument ||
    arguments_.length > 4 ||
    ((implementationCommitSha || recordedAt) &&
      (!implementationCommitSha || !recordedAt || !outputArgument))
  ) {
    throw new Error(
      "Usage: build-candidate-source-snapshot-cars <descriptor.json> [output-directory [implementation-commit-sha recorded-at]]",
    );
  }
  const databaseUrl = implementationCommitSha
    ? process.env.DATABASE_URL?.trim()
    : undefined;
  if (implementationCommitSha && !databaseUrl) {
    throw new Error("DATABASE_URL is required only for explicit record mode");
  }
  if (
    implementationCommitSha &&
    (!/^[a-f0-9]{40}$/.test(implementationCommitSha) ||
      Number.isNaN(Date.parse(recordedAt!)) ||
      new Date(recordedAt!).toISOString() !== recordedAt)
  ) {
    throw new Error("CAR record mode requires exact commit and UTC timestamp");
  }
  const descriptor = JSON.parse(
    await readFile(path.resolve(descriptorArgument), "utf8"),
  ) as CandidateSourceSnapshotBuildDescriptor;
  if (
    descriptor.sourcePlanFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256 ||
    descriptor.sourceManifestFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256
  ) {
    throw new Error(
      "CAR descriptor does not bind the reviewed source plan and manifest",
    );
  }
  const result = await buildCandidateSourceSnapshotCars({
    descriptor,
    outputDirectory: path.resolve(
      outputArgument ?? DEFAULT_OUTPUT_DIRECTORY,
    ),
    ...(databaseUrl && implementationCommitSha && recordedAt
      ? {
          record: {
            databaseUrl,
            implementationCommitSha,
            recordedAt,
          },
        }
      : {}),
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

await main().catch((error: unknown) => {
  const code =
    error instanceof Error ? error.name : "CandidateSourceSnapshotCarError";
  process.stderr.write(
    `${JSON.stringify({ code, status: "failed_closed" })}\n`,
  );
  process.exitCode = 1;
});
