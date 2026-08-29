import { createReadStream } from "node:fs";
import {
  mkdir,
  readFile,
  realpath,
  rename,
  stat,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

import { z } from "zod";

import type { ArtifactCapture, PreparedPilot } from "../domain/types.js";
import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";

export const SNAPSHOT_MANIFEST_VERSION = "1.0.0";
export const PREPARED_INPUT_MANIFEST_VERSION = "1.0.0";
export const CANONICAL_SCHEMA_SHA256 =
  "59c6472c2cd6d18041cf72c779fb970a082b00bef09aea724b99687e84198306";
export const PASCO_SOURCE_SET_ID = "pasco-appraiser-gis";
export const PASCO_PARSER_VERSION = "pasco-appraiser-parser-v1";
export const PASCO_TRANSFORM_VERSION = "pasco-appraisal-gis-preparation-v1";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const snapshotIdSchema = z.string().regex(/^snapshot_[a-f0-9]{32}$/);
const preparedInputIdSchema = z.string().regex(/^prepared_[a-f0-9]{32}$/);
const sourceIdSchema = z.string().regex(/^source_[a-f0-9]{32}$/);
const isoDateTimeSchema = z
  .string()
  .refine(
    (value) => Number.isFinite(Date.parse(value)),
    "must be an ISO date-time",
  );
const nullableDateTimeSchema = isoDateTimeSchema.nullable();
const relativePathSchema = z
  .string()
  .min(1)
  .max(1_024)
  .refine((value) => {
    if (path.isAbsolute(value)) return false;
    return !value.split(/[\\/]/).some((part) => part === "..");
  }, "must be a safe DATA_DIR-relative path");

export const sourceObjectSchema = z.strictObject({
  byteSize: z.number().int().nonnegative(),
  derivedFromSha256: sha256Schema.nullable(),
  lastModified: nullableDateTimeSchema,
  observedAt: nullableDateTimeSchema,
  relativePath: relativePathSchema,
  sha256: sha256Schema,
  sourceId: sourceIdSchema,
  sourceIdentifier: z.string().min(1).max(2_048),
  sourceSystem: z.string().min(1).max(200),
  stage: z.enum(["downloaded_source", "extracted_source"]),
});

export const snapshotSamplingSchema = z.strictObject({
  algorithm: z.string().min(1).max(200),
  seed: z.string().min(1).max(500),
  selectedRecordSha256: sha256Schema,
  selectionSize: z.number().int().positive(),
});

export const sourceSnapshotManifestSchema = z.strictObject({
  canonicalSchemaSha256: sha256Schema,
  county: z.literal("pasco"),
  createdAt: isoDateTimeSchema,
  manifestVersion: z.literal(SNAPSHOT_MANIFEST_VERSION),
  observationWindow: z.strictObject({
    end: isoDateTimeSchema,
    start: isoDateTimeSchema,
  }),
  parserVersion: z.string().min(1).max(200),
  sampling: snapshotSamplingSchema,
  snapshotId: snapshotIdSchema,
  sourceObjects: z.array(sourceObjectSchema).min(1),
  sourceSetId: z.string().min(1).max(200),
  transformVersion: z.string().min(1).max(200),
});

const fileBindingSchema = z.strictObject({
  byteSize: z.number().int().nonnegative(),
  relativePath: relativePathSchema,
  sha256: sha256Schema,
});

export const preparedInputManifestSchema = z.strictObject({
  createdAt: isoDateTimeSchema,
  kind: z.enum(["pilot", "scale"]),
  manifestVersion: z.literal(PREPARED_INPUT_MANIFEST_VERSION),
  prepared: fileBindingSchema,
  preparedInputId: preparedInputIdSchema,
  sampling: snapshotSamplingSchema,
  snapshotId: snapshotIdSchema,
  snapshotManifest: fileBindingSchema,
});

export const preparedInputReferenceSchema = z.strictObject({
  kind: z.enum(["pilot", "scale"]),
  manifest: fileBindingSchema,
  preparedInputId: preparedInputIdSchema,
  snapshotId: snapshotIdSchema,
});

export type SourceObject = z.infer<typeof sourceObjectSchema>;
export type SourceSnapshotManifest = z.infer<
  typeof sourceSnapshotManifestSchema
>;
export type PreparedInputManifest = z.infer<typeof preparedInputManifestSchema>;
export type PreparedInputReference = z.infer<
  typeof preparedInputReferenceSchema
>;

export interface VerifiedPreparedInput {
  manifest: PreparedInputManifest;
  prepared: PreparedPilot;
  reference: PreparedInputReference;
  snapshot: SourceSnapshotManifest;
}

function safeParse<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new DurableInputError(
      `${label} failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  return parsed.data;
}

async function fileSha256(filePath: string): Promise<string> {
  const hash = await import("node:crypto").then(({ createHash }) =>
    createHash("sha256"),
  );
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

function inside(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}${path.sep}`);
}

export async function resolveBoundDataPath(
  dataDir: string,
  relativePath: string,
): Promise<string> {
  safeParse(relativePathSchema, relativePath, "bound input path");
  const root = await realpath(dataDir);
  const candidate = path.resolve(root, relativePath);
  if (!inside(root, candidate)) {
    throw new DurableInputError("Bound input path escapes DATA_DIR");
  }
  let resolved: string;
  try {
    resolved = await realpath(candidate);
  } catch {
    throw new DurableInputError(
      `Bound input is missing (pathHash=${sha256(relativePath).slice(0, 16)})`,
    );
  }
  if (!inside(root, resolved)) {
    throw new DurableInputError("Bound input resolves outside DATA_DIR");
  }
  return resolved;
}

export async function bindDataFile(
  dataDir: string,
  filePath: string,
): Promise<z.infer<typeof fileBindingSchema>> {
  const root = await realpath(dataDir);
  const resolved = await realpath(filePath);
  if (!inside(root, resolved)) {
    throw new DurableInputError("Input file is outside DATA_DIR");
  }
  const metadata = await stat(resolved);
  if (!metadata.isFile()) throw new DurableInputError("Input is not a file");
  return {
    byteSize: metadata.size,
    relativePath: path.relative(root, resolved),
    sha256: await fileSha256(resolved),
  };
}

async function verifyFileBinding(
  dataDir: string,
  binding: z.infer<typeof fileBindingSchema>,
  label: string,
): Promise<string> {
  const parsed = safeParse(fileBindingSchema, binding, `${label} binding`);
  const resolved = await resolveBoundDataPath(dataDir, parsed.relativePath);
  const metadata = await stat(resolved);
  const actualHash = await fileSha256(resolved);
  if (metadata.size !== parsed.byteSize || actualHash !== parsed.sha256) {
    throw new DurableInputError(
      `${label} binding mismatch (expectedHash=${parsed.sha256}, actualHash=${actualHash})`,
    );
  }
  return resolved;
}

function snapshotIdentity(
  manifest: Omit<SourceSnapshotManifest, "snapshotId">,
) {
  const { createdAt: _createdAt, ...identity } = manifest;
  return identity;
}

export function expectedSnapshotId(
  manifest: Omit<SourceSnapshotManifest, "snapshotId">,
): string {
  return deterministicId("snapshot", [
    SNAPSHOT_MANIFEST_VERSION,
    "source-snapshot",
    canonicalJsonSha256(snapshotIdentity(manifest)),
  ]);
}

export function validateSnapshotIdentity(
  value: unknown,
): SourceSnapshotManifest {
  const manifest = safeParse(
    sourceSnapshotManifestSchema,
    value,
    "source snapshot manifest",
  );
  const { snapshotId: _snapshotId, ...withoutId } = manifest;
  if (expectedSnapshotId(withoutId) !== manifest.snapshotId) {
    throw new DurableInputError(
      `Source snapshot identity mismatch (snapshotId=${manifest.snapshotId})`,
    );
  }
  if (manifest.canonicalSchemaSha256 !== CANONICAL_SCHEMA_SHA256) {
    throw new DurableInputError(
      "Source snapshot canonical schema hash mismatch",
    );
  }
  return manifest;
}

async function atomicWrite(filePath: string, body: string): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  const partial = `${filePath}.part`;
  await writeFile(partial, body, { encoding: "utf8", mode: 0o600 });
  await rename(partial, filePath);
}

function observedWindow(
  objects: readonly SourceObject[],
  fallback: string,
): { end: string; start: string } {
  const times = objects
    .flatMap((object) => [object.observedAt, object.lastModified])
    .filter((value): value is string => value !== null)
    .map((value) => new Date(value).toISOString())
    .sort();
  return { start: times[0] ?? fallback, end: times.at(-1) ?? fallback };
}

export async function createSourceObject(options: {
  dataDir: string;
  derivedFromSha256?: string | null;
  filePath: string;
  lastModified?: string | null;
  observedAt?: string | null;
  sourceIdentifier: string;
  sourceSystem: string;
  stage: SourceObject["stage"];
}): Promise<SourceObject> {
  const binding = await bindDataFile(options.dataDir, options.filePath);
  return {
    ...binding,
    derivedFromSha256: options.derivedFromSha256 ?? null,
    lastModified: options.lastModified ?? null,
    observedAt: options.observedAt ?? null,
    sourceId: deterministicId("source", [
      SNAPSHOT_MANIFEST_VERSION,
      options.sourceSystem,
      options.stage,
      options.sourceIdentifier,
      binding.sha256,
    ]),
    sourceIdentifier: options.sourceIdentifier,
    sourceSystem: options.sourceSystem,
    stage: options.stage,
  };
}

export async function sourceObjectFromArtifact(options: {
  artifact: ArtifactCapture;
  dataDir: string;
  observedAt?: string | null;
}): Promise<SourceObject> {
  const object = await createSourceObject({
    dataDir: options.dataDir,
    filePath: options.artifact.localPath,
    ...(options.observedAt === undefined
      ? {}
      : { observedAt: options.observedAt }),
    sourceIdentifier: options.artifact.sourceUrl,
    sourceSystem: options.artifact.sourceSystem,
    stage: "downloaded_source",
  });
  if (
    object.byteSize !== options.artifact.bytes ||
    object.sha256 !== options.artifact.sha256
  ) {
    throw new DurableInputError(
      `Captured artifact binding mismatch (sourceId=${object.sourceId})`,
    );
  }
  return object;
}

export async function verifySourceObjectBindings(
  dataDir: string,
  sourceObjects: readonly SourceObject[],
): Promise<void> {
  await Promise.all(
    sourceObjects.map((object) =>
      verifyFileBinding(
        dataDir,
        {
          byteSize: object.byteSize,
          relativePath: object.relativePath,
          sha256: object.sha256,
        },
        `source object ${object.sourceId}`,
      ),
    ),
  );
}

export async function writeSourceSnapshot(options: {
  asOf: string;
  createdAt?: string;
  dataDir: string;
  sampling: SourceSnapshotManifest["sampling"];
  sourceObjects: SourceObject[];
}): Promise<{
  manifest: SourceSnapshotManifest;
  reference: z.infer<typeof fileBindingSchema>;
}> {
  const sourceObjects = [...options.sourceObjects].sort((left, right) =>
    left.sourceId.localeCompare(right.sourceId),
  );
  const canonicalSchemaBytes = await readFile(
    new URL("../../contracts/canonical-v1.schema.json", import.meta.url),
  );
  if (sha256(canonicalSchemaBytes) !== CANONICAL_SCHEMA_SHA256) {
    throw new DurableInputError("Canonical schema bytes do not match the lock");
  }
  const withoutId: Omit<SourceSnapshotManifest, "snapshotId"> = {
    canonicalSchemaSha256: CANONICAL_SCHEMA_SHA256,
    county: "pasco",
    createdAt: options.createdAt ?? new Date().toISOString(),
    manifestVersion: SNAPSHOT_MANIFEST_VERSION,
    observationWindow: observedWindow(sourceObjects, options.asOf),
    parserVersion: PASCO_PARSER_VERSION,
    sampling: options.sampling,
    sourceObjects,
    sourceSetId: PASCO_SOURCE_SET_ID,
    transformVersion: PASCO_TRANSFORM_VERSION,
  };
  const manifest: SourceSnapshotManifest = {
    ...withoutId,
    snapshotId: expectedSnapshotId(withoutId),
  };
  const relativePath = path.join(
    "pasco",
    "snapshots",
    manifest.snapshotId,
    "manifest.json",
  );
  const filePath = path.join(options.dataDir, relativePath);
  try {
    const existing = validateSnapshotIdentity(
      JSON.parse(await readFile(filePath, "utf8")),
    );
    if (
      canonicalJson(snapshotIdentity(existing)) !==
      canonicalJson(snapshotIdentity(manifest))
    ) {
      throw new DurableInputError(
        `Existing snapshot content conflicts (snapshotId=${manifest.snapshotId})`,
      );
    }
    return {
      manifest: existing,
      reference: await bindDataFile(options.dataDir, filePath),
    };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  await atomicWrite(filePath, `${canonicalJson(manifest)}\n`);
  return {
    manifest,
    reference: await bindDataFile(options.dataDir, filePath),
  };
}

export async function writePreparedInput(options: {
  createdAt?: string;
  dataDir: string;
  kind: PreparedInputManifest["kind"];
  prepared: PreparedPilot;
  sampling: PreparedInputManifest["sampling"];
  snapshot: SourceSnapshotManifest;
  snapshotReference: z.infer<typeof fileBindingSchema>;
}): Promise<PreparedInputReference> {
  const preparedBody = `${canonicalJson(options.prepared)}\n`;
  const preparedSha256 = sha256(preparedBody);
  const preparedInputId = deterministicId("prepared", [
    PREPARED_INPUT_MANIFEST_VERSION,
    "prepared-input",
    options.kind,
    options.snapshot.snapshotId,
    preparedSha256,
    options.sampling.selectedRecordSha256,
  ]);
  const relativePath = path.join(
    "pasco",
    "prepared",
    "snapshots",
    options.snapshot.snapshotId,
    preparedInputId,
    "dataset.json",
  );
  const filePath = path.join(options.dataDir, relativePath);
  try {
    const existing = await readFile(filePath, "utf8");
    if (existing !== preparedBody) {
      throw new DurableInputError(
        `Existing prepared input content conflicts (preparedInputId=${preparedInputId})`,
      );
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    await atomicWrite(filePath, preparedBody);
  }
  const prepared = await bindDataFile(options.dataDir, filePath);
  const manifest: PreparedInputManifest = {
    createdAt: options.createdAt ?? options.snapshot.createdAt,
    kind: options.kind,
    manifestVersion: PREPARED_INPUT_MANIFEST_VERSION,
    prepared,
    preparedInputId,
    sampling: options.sampling,
    snapshotId: options.snapshot.snapshotId,
    snapshotManifest: options.snapshotReference,
  };
  const manifestPath = path.join(path.dirname(filePath), "manifest.json");
  const manifestBody = `${canonicalJson(manifest)}\n`;
  try {
    const existing = await readFile(manifestPath, "utf8");
    if (existing !== manifestBody) {
      throw new DurableInputError(
        `Existing prepared manifest conflicts (preparedInputId=${preparedInputId})`,
      );
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    await atomicWrite(manifestPath, manifestBody);
  }
  return {
    kind: options.kind,
    manifest: await bindDataFile(options.dataDir, manifestPath),
    preparedInputId,
    snapshotId: options.snapshot.snapshotId,
  };
}

export async function verifyPreparedInput(
  dataDir: string,
  value: unknown,
  parsePrepared: (value: unknown) => PreparedPilot,
  expectedSnapshot?: string,
): Promise<VerifiedPreparedInput> {
  const reference = safeParse(
    preparedInputReferenceSchema,
    value,
    "prepared input reference",
  );
  if (expectedSnapshot && reference.snapshotId !== expectedSnapshot) {
    throw new DurableInputError(
      `Prepared input snapshot mismatch (expected=${expectedSnapshot}, actual=${reference.snapshotId})`,
    );
  }
  const manifestPath = await verifyFileBinding(
    dataDir,
    reference.manifest,
    "prepared manifest",
  );
  const manifest = safeParse(
    preparedInputManifestSchema,
    JSON.parse(await readFile(manifestPath, "utf8")),
    "prepared input manifest",
  );
  if (
    manifest.preparedInputId !== reference.preparedInputId ||
    manifest.snapshotId !== reference.snapshotId ||
    manifest.kind !== reference.kind
  ) {
    throw new DurableInputError(
      `Prepared reference identity mismatch (preparedInputId=${reference.preparedInputId})`,
    );
  }
  const snapshotPath = await verifyFileBinding(
    dataDir,
    manifest.snapshotManifest,
    "source snapshot manifest",
  );
  const snapshot = validateSnapshotIdentity(
    JSON.parse(await readFile(snapshotPath, "utf8")),
  );
  if (snapshot.snapshotId !== reference.snapshotId) {
    throw new DurableInputError(
      `Prepared source snapshot mismatch (preparedInputId=${reference.preparedInputId})`,
    );
  }
  await verifySourceObjectBindings(dataDir, snapshot.sourceObjects);
  const preparedPath = await verifyFileBinding(
    dataDir,
    manifest.prepared,
    "prepared input",
  );
  const prepared = parsePrepared(
    JSON.parse(await readFile(preparedPath, "utf8")),
  );
  const selectedRecordSha256 = sha256(
    JSON.stringify(
      prepared.properties.map((property) => property.parcel.exactFolio).sort(),
    ),
  );
  if (
    prepared.snapshotId !== reference.snapshotId ||
    prepared.snapshotManifestSha256 !== manifest.snapshotManifest.sha256 ||
    prepared.selectedRecordSha256 !== manifest.sampling.selectedRecordSha256 ||
    prepared.selectionSize !== manifest.sampling.selectionSize ||
    prepared.sampleAlgorithm !== manifest.sampling.algorithm ||
    prepared.sampleSeed !== manifest.sampling.seed ||
    selectedRecordSha256 !== manifest.sampling.selectedRecordSha256 ||
    canonicalJson(snapshot.sampling) !== canonicalJson(manifest.sampling)
  ) {
    throw new DurableInputError(
      `Prepared dataset metadata mismatch (preparedInputId=${reference.preparedInputId})`,
    );
  }
  for (const artifact of prepared.artifacts) {
    const binding = await bindDataFile(dataDir, artifact.localPath);
    await bindDataFile(dataDir, artifact.readyMarkerPath);
    const sourceObject = snapshot.sourceObjects.find(
      (object) =>
        object.stage === "downloaded_source" &&
        object.relativePath === binding.relativePath &&
        object.sourceIdentifier === artifact.sourceUrl &&
        object.sourceSystem === artifact.sourceSystem,
    );
    if (
      !sourceObject ||
      sourceObject.byteSize !== artifact.bytes ||
      sourceObject.sha256 !== artifact.sha256 ||
      binding.byteSize !== artifact.bytes ||
      binding.sha256 !== artifact.sha256
    ) {
      throw new DurableInputError(
        `Prepared artifact is not bound to its source snapshot (artifactHash=${artifact.sha256})`,
      );
    }
  }
  return { manifest, prepared, reference, snapshot };
}
