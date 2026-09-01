import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";

export const COVERAGE_METADATA_VERSION = "1.1.0";
export const PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER =
  "pasco_appraiser:extracted:parcel";
export const AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM =
  "official-parcel-complete-v1";
export const AUTHORITATIVE_PARCEL_SELECTION_SEED = "not-applicable";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const snapshotIdSchema = z.string().regex(/^snapshot_[a-f0-9]{32}$/);
const isoDateTimeSchema = z
  .string()
  .refine(
    (value) => Number.isFinite(Date.parse(value)),
    "must be an ISO date-time",
  );

const coverageCountsSchema = z.strictObject({
  acceptedRecords: z.number().int().nonnegative(),
  expectedSourceRecords: z.number().int().positive().nullable(),
  observedSourceRecords: z.number().int().nonnegative(),
  parsedRecords: z.number().int().nonnegative(),
  rejectedRecords: z.number().int().nonnegative(),
});

export const snapshotCoverageSchema = z.strictObject({
  authoritySource: z.strictObject({
    derivedFromSha256: sha256Schema,
    sourceId: z.string().regex(/^source_[a-f0-9]{32}$/),
    sourceIdentifier: z.string().min(1).max(2_048),
    sourceSha256: sha256Schema,
    sourceSystem: z.string().min(1).max(200),
  }),
  completeness: z.strictObject({
    evidenceSha256: sha256Schema,
    result: z.enum(["not_applicable", "passed", "failed"]),
    rule: z.literal("property-existence-completeness-v1"),
  }),
  counts: coverageCountsSchema,
  county: z.literal("pasco"),
  entityType: z.literal("property_existence"),
  membershipRule: z.string().min(1).max(500),
  mode: z.enum(["sample", "partial", "authoritative_complete"]),
  previousAuthoritativeSnapshotId: snapshotIdSchema.nullable(),
  previousProjectionSnapshotId: snapshotIdSchema.nullable(),
  scopeId: z.string().regex(/^scope_[a-f0-9]{32}$/),
  selection: z.strictObject({
    algorithm: z.string().min(1).max(200),
    kind: z.enum([
      "deterministic_sample",
      "partial_extract",
      "complete_source",
    ]),
    seed: z.string().min(1).max(500),
    selectedRecordSha256: sha256Schema,
    selectionSize: z.number().int().positive(),
  }),
  sourceObservationWindow: z.strictObject({
    end: isoDateTimeSchema,
    start: isoDateTimeSchema,
  }),
  membershipWatermark: z.strictObject({
    kind: z.literal("pasco-appraiser-observation-end-v1"),
    observedThrough: isoDateTimeSchema,
    sourceObjectSha256: sha256Schema,
  }),
  version: z.literal(COVERAGE_METADATA_VERSION),
});

export type SnapshotCoverage = z.infer<typeof snapshotCoverageSchema>;

export interface CoverageSourceObject {
  derivedFromSha256: string | null;
  sha256: string;
  sourceId: string;
  sourceIdentifier: string;
  sourceSystem: string;
  stage: "downloaded_source" | "extracted_source";
}

export interface CoveragePreparation {
  authoritySourceId: string;
  counts: z.infer<typeof coverageCountsSchema>;
  membershipRule: string;
  previousAuthoritativeSnapshotId?: string | null;
  previousProjectionSnapshotId?: string | null;
  selectionKind: SnapshotCoverage["selection"]["kind"];
}

interface CoverageDerivationOptions {
  authoritySource: CoverageSourceObject;
  counts: SnapshotCoverage["counts"];
  membershipRule: string;
  observationWindow: SnapshotCoverage["sourceObservationWindow"];
  parserVersion: string;
  previousAuthoritativeSnapshotId: string | null;
  previousProjectionSnapshotId: string | null;
  sampling: {
    algorithm: string;
    seed: string;
    selectedRecordSha256: string;
    selectionSize: number;
  };
  selectionKind: SnapshotCoverage["selection"]["kind"];
  transformVersion: string;
}

function completenessPassed(options: CoverageDerivationOptions): boolean {
  const counts = options.counts;
  return (
    options.selectionKind === "complete_source" &&
    options.authoritySource.sourceSystem === "pasco_appraiser" &&
    options.authoritySource.sourceIdentifier ===
      PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER &&
    options.authoritySource.stage === "extracted_source" &&
    options.authoritySource.derivedFromSha256 !== null &&
    options.sampling.algorithm === AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM &&
    options.sampling.seed === AUTHORITATIVE_PARCEL_SELECTION_SEED &&
    counts.expectedSourceRecords !== null &&
    counts.expectedSourceRecords === counts.observedSourceRecords &&
    counts.observedSourceRecords === counts.parsedRecords &&
    counts.parsedRecords === counts.acceptedRecords &&
    counts.acceptedRecords === options.sampling.selectionSize &&
    counts.rejectedRecords === 0
  );
}

function expectedScopeId(options: CoverageDerivationOptions): string {
  const sampleMembership =
    options.selectionKind === "deterministic_sample"
      ? [
          options.selectionKind,
          options.sampling.algorithm,
          options.sampling.seed,
          options.sampling.selectedRecordSha256,
          String(options.sampling.selectionSize),
        ]
      : [];
  return deterministicId("scope", [
    COVERAGE_METADATA_VERSION,
    "coverage-scope",
    "pasco",
    "property_existence",
    options.authoritySource.sourceSystem,
    options.authoritySource.sourceIdentifier,
    options.membershipRule,
    options.parserVersion,
    options.transformVersion,
    ...sampleMembership,
  ]);
}

export function deriveSnapshotCoverage(
  options: CoverageDerivationOptions,
): SnapshotCoverage {
  if (options.authoritySource.derivedFromSha256 === null) {
    throw new DurableInputError(
      "Snapshot coverage authority object lacks downloaded-source lineage",
    );
  }
  const complete = completenessPassed(options);
  const result =
    options.selectionKind === "deterministic_sample"
      ? "not_applicable"
      : complete
        ? "passed"
        : "failed";
  const evidence = {
    authorityDerivedFromSha256: options.authoritySource.derivedFromSha256,
    authoritySourceId: options.authoritySource.sourceId,
    authoritySourceSha256: options.authoritySource.sha256,
    counts: options.counts,
    parserVersion: options.parserVersion,
    rule: "property-existence-completeness-v1",
    selection: options.sampling,
    selectionKind: options.selectionKind,
    transformVersion: options.transformVersion,
  };
  return {
    authoritySource: {
      derivedFromSha256: options.authoritySource.derivedFromSha256,
      sourceId: options.authoritySource.sourceId,
      sourceIdentifier: options.authoritySource.sourceIdentifier,
      sourceSha256: options.authoritySource.sha256,
      sourceSystem: options.authoritySource.sourceSystem,
    },
    completeness: {
      evidenceSha256: canonicalJsonSha256(evidence),
      result,
      rule: "property-existence-completeness-v1",
    },
    counts: options.counts,
    county: "pasco",
    entityType: "property_existence",
    membershipRule: options.membershipRule,
    mode:
      options.selectionKind === "deterministic_sample"
        ? "sample"
        : complete
          ? "authoritative_complete"
          : "partial",
    previousAuthoritativeSnapshotId: options.previousAuthoritativeSnapshotId,
    previousProjectionSnapshotId: options.previousProjectionSnapshotId,
    scopeId: expectedScopeId(options),
    selection: {
      ...options.sampling,
      kind: options.selectionKind,
    },
    sourceObservationWindow: options.observationWindow,
    membershipWatermark: {
      kind: "pasco-appraiser-observation-end-v1",
      observedThrough: options.observationWindow.end,
      sourceObjectSha256: options.authoritySource.sha256,
    },
    version: COVERAGE_METADATA_VERSION,
  };
}

export function validateSnapshotCoverage(options: {
  coverage: unknown;
  observationWindow: SnapshotCoverage["sourceObservationWindow"];
  parserVersion: string;
  sampling: CoverageDerivationOptions["sampling"];
  sourceObjects: readonly CoverageSourceObject[];
  transformVersion: string;
}): SnapshotCoverage {
  const parsed = snapshotCoverageSchema.safeParse(options.coverage);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new DurableInputError(
      `Snapshot coverage failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  const coverage = parsed.data;
  const authoritySource = options.sourceObjects.find(
    (object) => object.sourceId === coverage.authoritySource.sourceId,
  );
  if (!authoritySource) {
    throw new DurableInputError(
      "Snapshot coverage authority object is missing",
    );
  }
  const downloadedParent = options.sourceObjects.find(
    (object) =>
      object.stage === "downloaded_source" &&
      object.sha256 === authoritySource.derivedFromSha256,
  );
  if (!downloadedParent) {
    throw new DurableInputError(
      "Snapshot coverage authority lineage is not bound to a downloaded object",
    );
  }
  const expected = deriveSnapshotCoverage({
    authoritySource,
    counts: coverage.counts,
    membershipRule: coverage.membershipRule,
    observationWindow: options.observationWindow,
    parserVersion: options.parserVersion,
    previousAuthoritativeSnapshotId: coverage.previousAuthoritativeSnapshotId,
    previousProjectionSnapshotId: coverage.previousProjectionSnapshotId,
    sampling: options.sampling,
    selectionKind: coverage.selection.kind,
    transformVersion: options.transformVersion,
  });
  if (canonicalJson(expected) !== canonicalJson(coverage)) {
    throw new DurableInputError("Snapshot coverage derivation mismatch");
  }
  return coverage;
}
