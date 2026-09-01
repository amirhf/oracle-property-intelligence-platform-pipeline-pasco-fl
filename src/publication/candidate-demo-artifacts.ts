import { randomUUID } from "node:crypto";
import {
  mkdir,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import { sha256 } from "../lib/hash.js";
import {
  validateCandidateDemoPlan,
  type CandidateDemoPlan,
} from "./candidate-demo.js";
import type { CandidateUploadArtifact } from "./filebase-executor.js";
import { calculateIpfsCid } from "./ipfs-cid.js";

function inside(root: string, candidate: string): boolean {
  return candidate === root || candidate.startsWith(`${root}${path.sep}`);
}

function sourceObjectPath(
  sourceRoot: string,
  object: CandidateDemoPlan["objects"][number],
): string {
  if (object.objectKey === "publication-dry-run-plan.json") {
    return path.join(sourceRoot, object.objectKey);
  }
  return object.domain === "open_data"
    ? path.join(sourceRoot, "open-data", object.objectKey)
    : path.join(sourceRoot, "query", object.objectKey);
}

function candidateObjectPath(
  candidateRoot: string,
  object: CandidateDemoPlan["objects"][number],
): string {
  if (object.objectKey === "publication-dry-run-plan.json") {
    return path.join(candidateRoot, object.objectKey);
  }
  return object.domain === "open_data"
    ? path.join(candidateRoot, "open-data", object.objectKey)
    : path.join(candidateRoot, "query", object.objectKey);
}

async function verifiedBytes(
  dataRoot: string,
  filePath: string,
  object: CandidateDemoPlan["objects"][number],
  maximumBytes: number,
): Promise<Buffer> {
  const resolved = await realpath(filePath);
  if (!inside(dataRoot, resolved)) {
    throw new Error("Candidate demo source artifact escapes DATA_DIR");
  }
  const metadata = await stat(resolved);
  if (!metadata.isFile() || metadata.size !== object.byteSize) {
    throw new Error("Candidate demo source artifact size does not match plan");
  }
  if (metadata.size > maximumBytes) {
    throw new Error("Candidate demo source artifact exceeds the hard limit");
  }
  const bytes = await readFile(resolved);
  if (
    sha256(bytes) !== object.sha256 ||
    (await calculateIpfsCid(bytes)) !== object.expectedCid
  ) {
    throw new Error("Candidate demo source artifact hash or CID mismatch");
  }
  return bytes;
}

async function verifyCandidateDirectory(
  dataRoot: string,
  candidateRoot: string,
  plan: CandidateDemoPlan,
): Promise<void> {
  const resolved = await realpath(candidateRoot);
  if (!inside(dataRoot, resolved)) {
    throw new Error("Candidate demo artifact directory escapes DATA_DIR");
  }
  for (const object of plan.objects) {
    await verifiedBytes(
      dataRoot,
      candidateObjectPath(resolved, object),
      object,
      plan.limits.maxObjectBytes,
    );
  }
}

export async function materializeCandidateDemoArtifacts(options: {
  dataDir: string;
  plan: CandidateDemoPlan;
  sourceOutputRoot: string;
}): Promise<string> {
  const plan = validateCandidateDemoPlan(options.plan);
  const dataRoot = await realpath(options.dataDir);
  const sourceRoot = await realpath(
    path.resolve(dataRoot, options.sourceOutputRoot),
  );
  if (!inside(dataRoot, sourceRoot)) {
    throw new Error("Candidate demo source publication escapes DATA_DIR");
  }
  const base = path.join(dataRoot, "artifacts", "candidate-demo", "pasco");
  await mkdir(base, { recursive: true, mode: 0o700 });
  const finalRoot = path.join(base, "plans", plan.demoPlanId);
  const contender = path.join(
    base,
    `.build-${plan.demoPlanId}-${process.pid}-${randomUUID()}`,
  );
  await mkdir(contender, { recursive: false, mode: 0o700 });
  try {
    for (const object of plan.objects) {
      const bytes = await verifiedBytes(
        dataRoot,
        sourceObjectPath(sourceRoot, object),
        object,
        plan.limits.maxObjectBytes,
      );
      const destination = candidateObjectPath(contender, object);
      await mkdir(path.dirname(destination), { recursive: true, mode: 0o700 });
      await writeFile(destination, bytes, { flag: "wx", mode: 0o600 });
    }
    await mkdir(path.dirname(finalRoot), { recursive: true, mode: 0o700 });
    try {
      await rename(contender, finalRoot);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
      await verifyCandidateDirectory(dataRoot, finalRoot, plan);
      await rm(contender, { force: true, recursive: true });
    }
    await verifyCandidateDirectory(dataRoot, finalRoot, plan);
    return path.relative(dataRoot, finalRoot);
  } catch (error) {
    await rm(contender, { force: true, recursive: true });
    throw error;
  }
}

export async function loadCandidateDemoUploadArtifacts(options: {
  dataDir: string;
  plan: CandidateDemoPlan;
}): Promise<CandidateUploadArtifact[]> {
  const plan = validateCandidateDemoPlan(options.plan);
  const dataRoot = await realpath(options.dataDir);
  const candidateRoot = await realpath(
    path.join(
      dataRoot,
      "artifacts",
      "candidate-demo",
      "pasco",
      "plans",
      plan.demoPlanId,
    ),
  );
  if (!inside(dataRoot, candidateRoot)) {
    throw new Error("Candidate demo artifact directory escapes DATA_DIR");
  }
  const artifacts: CandidateUploadArtifact[] = [];
  for (const object of plan.objects) {
    const bytes = await verifiedBytes(
      dataRoot,
      candidateObjectPath(candidateRoot, object),
      object,
      plan.limits.maxObjectBytes,
    );
    artifacts.push({
      bytes,
      domain: object.domain,
      expectedCid: object.expectedCid,
      objectKey: object.objectKey,
      sha256: object.sha256,
    });
  }
  return artifacts;
}
