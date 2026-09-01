import "dotenv/config";

import { readFile } from "node:fs/promises";
import path from "node:path";

import {
  loadCandidateSourceSnapshotCarArtifactRecords,
  loadCandidateSourceSnapshotCarImportAuthorization,
  recordCandidateSourceSnapshotCarImportAuthorization,
} from "../src/db/candidate-source-snapshot-car-import.js";
import { canonicalJson } from "../src/lib/canonical-json.js";
import { sha256 } from "../src/lib/hash.js";
import type { CandidateSourceSnapshotBuildDescriptor } from "../src/publication/candidate-source-snapshot-build.js";
import {
  executeCandidateSourceSnapshotCarImports,
  type CandidateSourceSnapshotCarExecutionArtifact,
} from "../src/publication/candidate-source-snapshot-car-controller.js";
import type { CandidateSourceSnapshotCarBuildResult } from "../src/publication/candidate-source-snapshot-car.js";
import { CandidateSourceSnapshotFilebaseCarImportTransport } from "../src/publication/candidate-source-snapshot-filebase-car.js";
import { prepareCandidateSourceSnapshotExecutionBundle } from "../src/publication/candidate-source-snapshot-session2.js";
import { validateCarV1 } from "../src/publication/car-v1.js";

type AuthorizationInput = Parameters<
  typeof recordCandidateSourceSnapshotCarImportAuthorization
>[1];

const ENABLE_VARIABLE = "CANDIDATE_SOURCE_SNAPSHOT_CAR_EXECUTOR_ENABLED";
const OPEN_TOKEN_VARIABLE =
  "CANDIDATE_SOURCE_SNAPSHOT_OPEN_DATA_CAR_BEARER_TOKEN";
const QUERY_TOKEN_VARIABLE =
  "CANDIDATE_SOURCE_SNAPSHOT_QUERY_TABLE_CAR_BEARER_TOKEN";

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`Required operator variable is absent: ${name}`);
  return value;
}

async function readJson(filePath: string): Promise<unknown> {
  return JSON.parse(await readFile(path.resolve(filePath), "utf8")) as unknown;
}

async function main(): Promise<void> {
  const [descriptorPath, carResultPath, authorizationPath] =
    process.argv.slice(2);
  if (!descriptorPath || !carResultPath || !authorizationPath) {
    throw new Error(
      "Usage: execute-candidate-source-snapshot-cars <descriptor.json> <car-result.json> <authorization.json>",
    );
  }
  if (process.env[ENABLE_VARIABLE]?.trim() !== "true") {
    throw new Error(`Remote CAR execution requires ${ENABLE_VARIABLE}=true`);
  }
  const databaseUrl = requiredEnvironment("DATABASE_URL");
  const openToken = requiredEnvironment(OPEN_TOKEN_VARIABLE);
  const queryToken = requiredEnvironment(QUERY_TOKEN_VARIABLE);
  const descriptor = (await readJson(
    descriptorPath,
  )) as CandidateSourceSnapshotBuildDescriptor;
  const carResult = (await readJson(
    carResultPath,
  )) as CandidateSourceSnapshotCarBuildResult;
  const authorizationInput = (await readJson(
    authorizationPath,
  )) as AuthorizationInput;
  const bundle =
    await prepareCandidateSourceSnapshotExecutionBundle(descriptor);
  if (
    carResult.planId !== bundle.build.plan.planId ||
    carResult.planSha256 !== bundle.build.plan.planSha256 ||
    authorizationInput.planId !== bundle.build.plan.planId ||
    authorizationInput.planSha256 !== bundle.build.plan.planSha256
  ) {
    throw new Error("CAR operator files do not bind the same immutable plan");
  }
  const durableAuthorization =
    await loadCandidateSourceSnapshotCarImportAuthorization(databaseUrl, {
      authorizationStatementSha256: sha256(
        authorizationInput.authorizationStatement,
      ),
      planId: bundle.build.plan.planId,
    });
  if (!durableAuthorization) {
    throw new Error("Exact durable CAR authorization does not exist");
  }
  if (
    durableAuthorization.authorizationStatement !==
      authorizationInput.authorizationStatement ||
    durableAuthorization.openDataBucketTokenSha256 !==
      authorizationInput.openDataBucketTokenSha256 ||
    durableAuthorization.queryTableBucketTokenSha256 !==
      authorizationInput.queryTableBucketTokenSha256 ||
    durableAuthorization.overallTimeoutMs !==
      authorizationInput.overallTimeoutMs ||
    sha256(openToken) !== durableAuthorization.openDataBucketTokenSha256 ||
    sha256(queryToken) !== durableAuthorization.queryTableBucketTokenSha256 ||
    sha256(openToken) === sha256(queryToken)
  ) {
    throw new Error("Durable CAR authorization conflicts with operator input");
  }
  const records = await loadCandidateSourceSnapshotCarArtifactRecords(
    databaseUrl,
    bundle.build.plan.planId,
  );
  if (records.length !== 2 || carResult.cars.length !== 2) {
    throw new Error("Both exact CAR artifacts must already be durable");
  }
  const resultDirectory = path.dirname(path.resolve(carResultPath));
  const artifacts: CandidateSourceSnapshotCarExecutionArtifact[] = [];
  for (const car of carResult.cars) {
    const record = records.find(
      (candidate) => candidate.carRole === car.domain,
    );
    if (!record) throw new Error("Durable CAR artifact is absent");
    const filePath = path.resolve(resultDirectory, car.carFile);
    const validated = await validateCarV1({
      expectedBlockCount: car.blockCount,
      expectedBlockMembershipSha256: car.blockMembershipSha256,
      expectedRoots: record.roots,
      filePath,
      maxHeaderBytes: 32 * 1024 * 1024,
      maxSectionBytes: 1024 * 1024,
    });
    if (
      validated.byteSize !== car.byteSize ||
      validated.sha256 !== car.carSha256 ||
      canonicalJson(validated.roots) !== canonicalJson(record.roots)
    ) {
      throw new Error("Local CAR result no longer matches its exact bytes");
    }
    artifacts.push({
      artifact: {
        artifactId: record.carArtifactId,
        blockCount: car.blockCount,
        blockMembershipSha256: car.blockMembershipSha256,
        bucketName:
          car.domain === "open_data"
            ? bundle.build.plan.targets.openData.bucket
            : bundle.build.plan.targets.queryTable.bucket,
        carBytes: car.byteSize,
        carSha256: car.carSha256,
        domain: car.domain,
        filePath,
        implementationCommitSha: record.implementationCommitSha,
        planId: record.planId,
        planSha256: record.planSha256,
        roots: validated.roots,
      },
      record,
    });
  }
  if (
    artifacts[0]?.artifact.domain !== "open_data" ||
    artifacts[1]?.artifact.domain !== "query_table"
  ) {
    throw new Error("CAR operator order must be open-data then query-table");
  }
  const transport = new CandidateSourceSnapshotFilebaseCarImportTransport({
    config: {
      buckets: {
        open_data: {
          bucketName: bundle.build.plan.targets.openData.bucket,
          bucketScopedBearerToken: openToken,
        },
        query_table: {
          bucketName: bundle.build.plan.targets.queryTable.bucket,
          bucketScopedBearerToken: queryToken,
        },
      },
    },
  });
  const execution = await executeCandidateSourceSnapshotCarImports({
    artifacts: artifacts as [
      CandidateSourceSnapshotCarExecutionArtifact,
      CandidateSourceSnapshotCarExecutionArtifact,
    ],
    authorization: authorizationInput,
    databaseUrl,
    plan: bundle.build.plan,
    transport,
  });
  process.stdout.write(
    `${JSON.stringify({ ...execution, status: execution.closureId ? "completed" : "stopped_fail_closed" })}\n`,
  );
}

await main().catch((error: unknown) => {
  process.stderr.write(
    `${JSON.stringify({ code: error instanceof Error ? error.name : "UnknownError", status: "failed_closed" })}\n`,
  );
  process.exitCode = 1;
});
