import { readFile, statfs } from "node:fs/promises";
import path from "node:path";

import {
  buildCandidateSourceSnapshotDemo,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
  type CandidateSourceSnapshotBuildDescriptor,
} from "../src/publication/candidate-source-snapshot-build.js";

async function main(): Promise<void> {
  const startedAt = process.hrtime.bigint();
  const diskBefore = await statfs(process.cwd());
  let peakRssBytes = process.memoryUsage().rss;
  const memorySampler = setInterval(() => {
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
  }, 100);
  memorySampler.unref();
  const arguments_ = process.argv.slice(2);
  const noRecord = arguments_.includes("--no-record");
  const descriptorArgument = arguments_.find(
    (argument) => argument !== "--no-record",
  );
  if (!descriptorArgument) {
    throw new Error(
      "Usage: build-candidate-source-snapshot-demo <descriptor.json> [--no-record]",
    );
  }
  const descriptorPath = path.resolve(descriptorArgument);
  const descriptor = JSON.parse(
    await readFile(descriptorPath, "utf8"),
  ) as CandidateSourceSnapshotBuildDescriptor;
  if (
    descriptor.sourcePlanFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256 ||
    descriptor.sourceManifestFileSha256 !==
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256
  ) {
    throw new Error(
      "CLI descriptor does not bind the reviewed preliminary source plan and manifest files",
    );
  }
  const databaseUrl = noRecord ? undefined : process.env.DATABASE_URL;
  let result: Awaited<ReturnType<typeof buildCandidateSourceSnapshotDemo>>;
  try {
    result = await buildCandidateSourceSnapshotDemo({
      ...(databaseUrl ? { databaseUrl } : {}),
      descriptor,
      record: !noRecord,
    });
  } finally {
    clearInterval(memorySampler);
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
  }
  const diskAfter = await statfs(process.cwd());
  const available = (value: Awaited<ReturnType<typeof statfs>>) =>
    Number(value.bavail) * Number(value.bsize);
  process.stdout.write(
    `${JSON.stringify(
      {
        adoptedExistingControls: result.adoptedExistingControls,
        availableDiskBytes: available(diskAfter),
        diskBytesConsumed: Math.max(
          0,
          available(diskBefore) - available(diskAfter),
        ),
        durationMs: Number((process.hrtime.bigint() - startedAt) / 1_000_000n),
        exactObjectCount: result.exactObjectCount,
        exactTotalBytes: result.exactTotalBytes,
        executorEnabled: false,
        inventoryRootCid: result.inventoryRootCid,
        inventoryRootSha256: result.inventoryRootSha256,
        planArtifact: {
          byteSize: result.planArtifact.byteSize,
          expectedCid: result.planArtifact.expectedCid,
          logicalObjectKey: result.planArtifact.logicalObjectKey,
          remoteObjectKey: result.planArtifact.remoteObjectKey,
          sha256: result.planArtifact.sha256,
        },
        planId: result.plan.planId,
        planSha256: result.plan.planSha256,
        peakRssBytes,
        recordState: result.recordState,
      },
      null,
      2,
    )}\n`,
  );
}

await main();
