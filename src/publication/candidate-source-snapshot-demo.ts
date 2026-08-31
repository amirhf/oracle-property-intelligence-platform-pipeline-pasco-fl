import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";
import {
  publicationControlArtifactsBindingSchema,
  type PublicationControlArtifactsBinding,
} from "./control-artifacts.js";
import {
  candidateSourceSnapshotPreflightBindingSchema,
  CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS,
  type CandidateSourceSnapshotPreflightBinding,
} from "./candidate-source-snapshot-preflight-binding.js";

export const CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION = "2.0.0";
export const CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION =
  "2.0.0" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS =
  "candidate_owned_source_snapshot_demo";
export const CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE =
  "Candidate-owned, noncanonical Filebase demonstration of the complete parcel membership represented by the exact hash-bound August 23, 2026 Pasco Property Appraiser source snapshot under owner-assumed snapshot authority. It is not represented as Elephant-owned, owner-controlled, independently Pasco-certified, or complete under other Pasco reporting definitions. GIS, coordinate, related-fact, permit, and contractor coverage is measured and reported separately.";

export const CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS = Object.freeze({
  maxBudgetUsd: 25,
  maxConcurrency: 16,
  maxObjectBytes: 536_870_912,
  maxObjects: 350_000,
  maxRequests: 1_000_000,
  maxRetries: 2,
  maxTotalBytes: 4_294_967_296,
  requestTimeoutMs: 20_000,
});

export const CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING = Object.freeze({
  fixedAccountPlanMonthlyUsd: 7.5,
  requestUsdPerThousand: 0.0045,
  storageUsdPerGib: 0.0162,
});
export const CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE = Object.freeze({
  authorityClass: "owner_assumed_authoritative_snapshot" as const,
  authorityId: "authority_2a6e9cb08d4c8fc12082aa30abb35cab",
  csvSha256: "8f06fe9ff8969869a606cf85b5a7722bebd247f5ff47b33288689c3aa4160545",
  folioSetSha256:
    "3cb676d4a52a35f7bc2bcf1a13b5a4c1ca5f21c005bb867078dca1a4d428dfab",
  loaderEffectId: "load_47cad15c72f5ebb0d3be66adbd73e9f1",
  materializationId: "materialization_981835fc695107653fd830e12c2284db",
  materializationSha256:
    "ae295083f7efce4575e15bda381253f0dfe29ea4fe2c4e320256242bc80a513a",
  membershipPropertyCount: 325_213,
  resultSha256:
    "81d47175f1be5800388517b8b4e12e5998d1ab0375ac648def53667dfa3a1746",
  scopeId: "scope_055c2b98f0dc74de092e53bacb1d64ce",
  snapshotId: "snapshot_23e94803bfee6453a047595e80f2fc43",
  sourcePlanId: "plan_3a55deb9f15d67ca028c132490069b9e",
  sourcePlanSha256:
    "97f2ffb14d6c218e1d4863c48d223cd210b73345c0cc1cddc95d7174e040ee69",
  workflowRunId: "run_4c74edc0e29eacf0cb4de4b45d57428c",
  zipSha256: "bffeead6aa18d9e53e5da9efafa5533b24e7d563b733b1d327bdc0a5cb62cac9",
});
export const CANDIDATE_SOURCE_SNAPSHOT_BOUND_ARTIFACTS = Object.freeze({
  canonicalSchemaSha256:
    "59c6472c2cd6d18041cf72c779fb970a082b00bef09aea724b99687e84198306",
  manifestFileSha256:
    "73d632e2fe2b77b1c21d6eb8e1c98c9b3ab01792acb67eff69033677f6f76b45",
  mcpSchemaSha256:
    "9fc112ef8a4e2120593c3dc20c90073b0eb96596817c96112f63fd258bb7c131",
  openDataRootCid: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.targetCid,
  parquetByteSize: 69_430_565,
  parquetSchemaSha256:
    "b136aa3c9724a9acd92ad99e83add241a44323d9cd5b04529c048358f118d5be",
  parquetSha256:
    "316c4f04748ce54e134f58b4799d32233fa6bdf50898308ab798a060da3097b2",
  planFileSha256:
    "d540f432902d08754b8cf39ec78c475bd868ecfddf93a1d4414f2fc8ad66fb53",
  queryTableCid: CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.targetCid,
});
export const CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE_PARQUET = Object.freeze({
  byteSize: 69_429_508,
  expectedCid: "QmWWZ9Us1VH8aRKLSr37fTngp9vmkFpV46SwoV8P5T6MCh",
  schemaSha256:
    "b136aa3c9724a9acd92ad99e83add241a44323d9cd5b04529c048358f118d5be",
  sha256: "c331f7d2162e92f77f34547dde2798ec11f40da22e7f50aa927343b04c3c2d19",
});
export const CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES =
  16 * 1024 * 1024;
export const PROTECTED_CANDIDATE_SAMPLE_ROLLBACK = Object.freeze({
  manifest: {
    cid: "QmUYrFZAR7aTdJYK9Utnskotm9XqevCsAe8kQ6V9C5NE3D",
    sha256: "e8a143d78934a83e3789d3c9fc735348264f3bb0d52ad35be9bb96275ad54098",
  },
  openData: {
    bucket: "cand-amir-pasco-open-data-demo",
    ipnsLabel: "cand-amir-pasco-open-data-demo",
    ipnsNetworkKey:
      "k51qzi5uqu5dgtp96yf29oegxpp8pju3g2ce3cn3wkzu0rusxt8co4r63ew78d",
    targetCid: "QmVwpAV8hWUr3zsJZijhzUAArgSMhkV1vzmtJaWFMUQ4pj",
  },
  plan: {
    cid: "QmXv6o4wxxL1bkb8naaqf3jVoJnyBgVmBwRVGBPPGgQR3R",
    sha256: "8e9fa05ebc99b8cc9a01a0c81611736b59a40c519804aaa17fe108dd9495104b",
  },
  queryTable: {
    bucket: "cand-amir-pasco-query-table-demo",
    ipnsLabel: "cand-amir-pasco-query-table-demo",
    ipnsNetworkKey:
      "k51qzi5uqu5di2wpp4d4696hjjbpf1ciolkve2duyrrzpsdygh5cywfydtop9n",
    targetCid: "QmSdGz1gZtx4GXxQ41qez6ww6G1Xefy19BPU5vJPEobYUH",
  },
});
export const CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_COVERAGE = Object.freeze({
  activeProperties: 325_213,
  buildingFacts: 276_649,
  buildingProperties: 261_590,
  contractorCoverage: "unavailable" as const,
  contractorFacts: 0,
  coordinateProperties: 24_995,
  duplicatePropertyIdentities: 0,
  fixtureMatches: 0,
  inactiveProperties: 0,
  missingCoordinateProperties: 300_218,
  ownershipAcceptedRows: 322_261,
  ownershipMalformedRows: 1,
  ownershipProperties: 322_261,
  ownershipSourceRows: 322_262,
  permitContractorRelationships: 0,
  permitCoverage: "unavailable" as const,
  permitFacts: 0,
  siteAddressProperties: 282_612,
  siteAddressRows: 361_347,
  yearBuiltProxyProperties: 261_590,
});

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const priorCidSchema = z.union([
  cidSchema,
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const idSchema = (prefix: string) =>
  z.string().regex(new RegExp(`^${prefix}_[a-f0-9]{32}$`));
const domainSchema = z.enum(["open_data", "query_table"]);

const safeObjectKeySchema = z
  .string()
  .min(1)
  .max(2_048)
  .refine(
    (value) =>
      !value.startsWith("/") &&
      !value.includes("\\") &&
      !value.split("/").includes("..") &&
      !value.includes("{planId}"),
    "must be a safe logical publication object key",
  );

const candidateResourceNameSchema = z
  .string()
  .min(12)
  .max(200)
  .regex(/^[a-z0-9][a-z0-9-]*[a-z0-9]$/)
  .refine(
    (value) => !/^(elephant|oracle|prism)(-|$)/.test(value),
    "candidate resources must not use an owner/canonical namespace",
  );

const networkKeySchema = z.string().regex(/^k51[0-9a-z]{59}$/);

export const candidateSourceSnapshotLimitsSchema = z.strictObject({
  maxBudgetUsd: z
    .number()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxBudgetUsd),
  maxConcurrency: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxConcurrency),
  maxObjectBytes: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxObjectBytes),
  maxObjects: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxObjects),
  maxRequests: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxRequests),
  maxRetries: z
    .number()
    .int()
    .nonnegative()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxRetries),
  maxTotalBytes: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxTotalBytes),
  requestTimeoutMs: z
    .number()
    .int()
    .min(500)
    .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.requestTimeoutMs),
});

export type CandidateSourceSnapshotLimits = z.infer<
  typeof candidateSourceSnapshotLimitsSchema
>;

const operationRatesSchema = z.strictObject({
  classAMutationUsdPerThousand: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.requestUsdPerThousand,
  ),
  classBReadUsdPerThousand: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.requestUsdPerThousand,
  ),
  freeOperationUsdPerThousand: z.literal(0),
  namesApiUsdPerThousand: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.requestUsdPerThousand,
  ),
  publicResolverUsdPerThousand: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.requestUsdPerThousand,
  ),
});

export const candidateSourceSnapshotPricingSchema = z.strictObject({
  fixedAccountPlan: z.strictObject({
    evidence: z.enum([
      "human_confirmation_required",
      "human_confirmed",
      "provider_api",
    ]),
    monthlyUsd: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.fixedAccountPlanMonthlyUsd,
    ),
    planName: z.literal("Filebase Pro"),
  }),
  operationRates: operationRatesSchema,
  schemaVersion: z.literal("candidate-filebase-cost-v2"),
  storageUsdPerGib: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.storageUsdPerGib,
  ),
});

export type CandidateSourceSnapshotPricing = z.infer<
  typeof candidateSourceSnapshotPricingSchema
>;

const operationCountsSchema = z
  .strictObject({
    classAMutations: z.number().int().nonnegative(),
    classBReads: z.number().int().nonnegative(),
    freeOperations: z.number().int().nonnegative(),
    namesApiOperations: z.number().int().nonnegative(),
    publicResolverOperations: z.number().int().nonnegative(),
    total: z.number().int().nonnegative(),
  })
  .superRefine((counts, context) => {
    const expectedTotal =
      counts.classAMutations +
      counts.classBReads +
      counts.freeOperations +
      counts.namesApiOperations +
      counts.publicResolverOperations;
    if (
      !Number.isSafeInteger(expectedTotal) ||
      counts.total !== expectedTotal
    ) {
      context.addIssue({
        code: "custom",
        message: "operation count total does not match its exact classes",
        path: ["total"],
      });
    }
  });

export const candidateSourceSnapshotRequestEnvelopeSchema = z.strictObject({
  ambiguousObjectInspectionAllowance: operationCountsSchema,
  maximumAttempts: operationCountsSchema,
  maximumTotalRequests: z.number().int().positive(),
  recoveryAllowance: operationCountsSchema.extend({
    observationCyclesPerDomain: z.number().int().positive().max(32),
  }),
  schemaVersion: z.literal("candidate-request-envelope-v2"),
  successfulExecution: operationCountsSchema,
});

export type CandidateSourceSnapshotRequestEnvelope = z.infer<
  typeof candidateSourceSnapshotRequestEnvelopeSchema
>;

export const candidateSourceSnapshotCostEnvelopeSchema = z.strictObject({
  fixedAccountPlanMonthlyUsd: z.number().nonnegative(),
  incrementalExecutionUsd: z.number().nonnegative(),
  maximumIncrementalUsd: z.number().nonnegative(),
  maximumTotalUsd: z.number().nonnegative(),
  recoveryRequestUsd: z.number().nonnegative(),
  requestUsd: z.strictObject({
    ambiguousObjectInspections: z.number().nonnegative(),
    maximumAttempts: z.number().nonnegative(),
    successfulExecution: z.number().nonnegative(),
  }),
  schemaVersion: z.literal("candidate-cost-envelope-v2"),
  storageUsd: z.number().nonnegative(),
});

export type CandidateSourceSnapshotCostEnvelope = z.infer<
  typeof candidateSourceSnapshotCostEnvelopeSchema
>;

export const candidateSourceSnapshotPlanArtifactSchema = z.strictObject({
  byteSize: z
    .number()
    .int()
    .positive()
    .max(CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES),
  expectedCid: cidSchema,
  logicalObjectKey: z.literal("candidate-source-snapshot-plan.json"),
  remoteObjectKey: safeObjectKeySchema,
  sha256: sha256Schema,
});

export type CandidateSourceSnapshotPlanArtifact = z.infer<
  typeof candidateSourceSnapshotPlanArtifactSchema
>;
export type CandidateSourceSnapshotControlArtifacts =
  PublicationControlArtifactsBinding;
export type CandidateSourceSnapshotPreflight =
  CandidateSourceSnapshotPreflightBinding;

const inventoryBindingSchema = z.strictObject({
  inventoryRootCid: cidSchema,
  inventoryRootSha256: sha256Schema,
  inventoryShardCount: z.number().int().positive(),
  maxObjectBytes: z.number().int().positive(),
  objectCount: z.number().int().positive(),
  representationVersion: z.literal("candidate-upload-inventory-v1"),
  totalBytes: z.number().int().positive(),
});

const targetSchema = z.strictObject({
  bucket: candidateResourceNameSchema,
  domain: domainSchema,
  immutablePrefix: safeObjectKeySchema,
  ipnsLabel: candidateResourceNameSchema,
  ipnsNetworkKey: networkKeySchema,
  priorCid: priorCidSchema,
  targetCid: cidSchema,
});

const sourceBindingSchema = z.strictObject({
  authorityClass: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.authorityClass,
  ),
  authorityId: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.authorityId),
  csvSha256: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.csvSha256),
  folioSetSha256: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.folioSetSha256,
  ),
  loaderEffectId: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.loaderEffectId,
  ),
  materializationId: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.materializationId,
  ),
  materializationSha256: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.materializationSha256,
  ),
  membershipPropertyCount: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.membershipPropertyCount,
  ),
  resultSha256: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.resultSha256),
  scopeId: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.scopeId),
  snapshotId: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.snapshotId),
  sourcePlanId: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.sourcePlanId),
  sourcePlanSha256: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.sourcePlanSha256,
  ),
  workflowRunId: z.literal(
    CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.workflowRunId,
  ),
  zipSha256: z.literal(CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE.zipSha256),
});

export type CandidateSourceSnapshotSourceBinding = z.infer<
  typeof sourceBindingSchema
>;

const classificationSchema = z.strictObject({
  canonical: z.literal(false),
  elephantOwned: z.literal(false),
  independentlyPascoCertified: z.literal(false),
  ownerControlled: z.literal(false),
  publicationClass: z.literal(CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_CLASS),
  resourceOwner: z.literal("candidate"),
  sourceScope: z.literal("exact_hash_bound_2026_08_23_parcel_snapshot"),
});

const measuredCoverageSchema = z.strictObject({
  activeProperties: z.literal(325_213),
  buildingFacts: z.literal(276_649),
  buildingProperties: z.literal(261_590),
  contractorCoverage: z.literal("unavailable"),
  contractorFacts: z.literal(0),
  coordinateProperties: z.literal(24_995),
  duplicatePropertyIdentities: z.literal(0),
  fixtureMatches: z.literal(0),
  inactiveProperties: z.literal(0),
  missingCoordinateProperties: z.literal(300_218),
  ownershipAcceptedRows: z.literal(322_261),
  ownershipMalformedRows: z.literal(1),
  ownershipProperties: z.literal(322_261),
  ownershipSourceRows: z.literal(322_262),
  permitContractorRelationships: z.literal(0),
  permitCoverage: z.literal("unavailable"),
  permitFacts: z.literal(0),
  siteAddressProperties: z.literal(282_612),
  siteAddressRows: z.literal(361_347),
  yearBuiltProxyProperties: z.literal(261_590),
});

const protectedSampleTargetSchema = z.strictObject({
  bucket: candidateResourceNameSchema,
  ipnsLabel: candidateResourceNameSchema,
  ipnsNetworkKey: networkKeySchema,
  targetCid: cidSchema,
});

const protectedSampleRollbackSchema = z.strictObject({
  ambiguityBehavior: z.literal("no_mutation_or_rollback"),
  cutoverOrder: z.tuple([z.literal("open_data"), z.literal("query_table")]),
  manifest: z.strictObject({ cid: cidSchema, sha256: sha256Schema }),
  openData: protectedSampleTargetSchema,
  plan: z.strictObject({ cid: cidSchema, sha256: sha256Schema }),
  queryTable: protectedSampleTargetSchema,
  rollbackOrder: z.tuple([z.literal("query_table"), z.literal("open_data")]),
  verificationEvidenceSha256: sha256Schema,
  verifiedAt: z.string().datetime(),
});

export const candidateSourceSnapshotDemoPlanSchema = z
  .strictObject({
    artifactRepresentationVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION,
    ),
    classification: classificationSchema,
    controlArtifacts: publicationControlArtifactsBindingSchema,
    costEnvelope: candidateSourceSnapshotCostEnvelopeSchema,
    coverage: measuredCoverageSchema,
    disclaimer: z.literal(CANDIDATE_SOURCE_SNAPSHOT_DISCLOSURE),
    inventory: inventoryBindingSchema,
    limits: candidateSourceSnapshotLimitsSchema,
    namespaceId: idSchema("snapshotns"),
    planId: idSchema("snapshotdemo"),
    planSha256: sha256Schema,
    pricing: candidateSourceSnapshotPricingSchema,
    preflight: candidateSourceSnapshotPreflightBindingSchema,
    protectedSampleRollback: protectedSampleRollbackSchema,
    requestEnvelope: candidateSourceSnapshotRequestEnvelopeSchema,
    source: sourceBindingSchema,
    targets: z.strictObject({
      controlPrefix: safeObjectKeySchema,
      openData: targetSchema.extend({
        domain: z.literal("open_data"),
      }),
      queryTable: targetSchema.extend({
        domain: z.literal("query_table"),
      }),
    }),
    version: z.literal(CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION),
  })
  .superRefine((plan, context) => {
    const targets = [plan.targets.openData, plan.targets.queryTable];
    if (
      new Set(targets.map((target) => target.bucket)).size !== 2 ||
      new Set(targets.map((target) => target.ipnsLabel)).size !== 2 ||
      new Set(targets.map((target) => target.ipnsNetworkKey)).size !== 2 ||
      new Set(targets.map((target) => target.priorCid)).size !== 2
    ) {
      context.addIssue({
        code: "custom",
        message:
          "source-snapshot demo domains require distinct resources and priors",
        path: ["targets"],
      });
    }
    const preflightIdentities = new Map(
      plan.preflight.identities.map((identity) => [identity.domain, identity]),
    );
    const preflightBuckets = new Map(
      plan.preflight.buckets.map((bucket) => [bucket.domain, bucket]),
    );
    for (const target of targets) {
      const identity = preflightIdentities.get(target.domain);
      const bucket = preflightBuckets.get(target.domain);
      if (
        !identity ||
        !bucket ||
        identity.bucket !== target.bucket ||
        identity.ipnsLabel !== target.ipnsLabel ||
        identity.ipnsNetworkKey !== target.ipnsNetworkKey ||
        bucket.bucket !== target.bucket ||
        identity.controlCid !== target.priorCid ||
        identity.officialGatewayCid !== target.priorCid ||
        identity.signedRecordCid !== target.priorCid
      ) {
        context.addIssue({
          code: "custom",
          message:
            "source-snapshot target does not match its sanitized preflight evidence",
          path: ["preflight", target.domain],
        });
      }
    }
    if (
      plan.targets.openData.bucket !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket ||
      plan.targets.openData.ipnsLabel !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel ||
      plan.targets.openData.ipnsNetworkKey !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey ||
      plan.targets.openData.targetCid !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.targetCid ||
      plan.targets.queryTable.bucket !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket ||
      plan.targets.queryTable.ipnsLabel !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel ||
      plan.targets.queryTable.ipnsNetworkKey !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey ||
      plan.targets.queryTable.targetCid !==
        CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.targetCid
    ) {
      context.addIssue({
        code: "custom",
        message:
          "source-snapshot targets are not the reviewed candidate resources",
        path: ["targets"],
      });
    }
    if (
      plan.preflight.protectedSampleRollback.verificationEvidenceSha256 !==
        plan.protectedSampleRollback.verificationEvidenceSha256 ||
      plan.preflight.protectedSampleRollback.verifiedAt !==
        plan.protectedSampleRollback.verifiedAt
    ) {
      context.addIssue({
        code: "custom",
        message:
          "protected sample rollback does not match sanitized preflight evidence",
        path: ["preflight", "protectedSampleRollback"],
      });
    }
    if (
      plan.protectedSampleRollback.openData.bucket ===
        plan.targets.openData.bucket ||
      plan.protectedSampleRollback.queryTable.bucket ===
        plan.targets.queryTable.bucket ||
      plan.protectedSampleRollback.openData.ipnsNetworkKey ===
        plan.targets.openData.ipnsNetworkKey ||
      plan.protectedSampleRollback.queryTable.ipnsNetworkKey ===
        plan.targets.queryTable.ipnsNetworkKey
    ) {
      context.addIssue({
        code: "custom",
        message: "protected sample rollback resources must remain separate",
        path: ["protectedSampleRollback"],
      });
    }
    for (const [domain, expected] of [
      ["openData", PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.openData],
      ["queryTable", PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.queryTable],
    ] as const) {
      const actual = plan.protectedSampleRollback[domain];
      if (
        actual.bucket !== expected.bucket ||
        actual.ipnsLabel !== expected.ipnsLabel ||
        actual.ipnsNetworkKey !== expected.ipnsNetworkKey ||
        actual.targetCid !== expected.targetCid
      ) {
        context.addIssue({
          code: "custom",
          message:
            "protected sample rollback identity is not the frozen publication",
          path: ["protectedSampleRollback", domain],
        });
      }
    }
    if (
      plan.protectedSampleRollback.manifest.cid !==
        PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest.cid ||
      plan.protectedSampleRollback.manifest.sha256 !==
        PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.manifest.sha256 ||
      plan.protectedSampleRollback.plan.cid !==
        PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan.cid ||
      plan.protectedSampleRollback.plan.sha256 !==
        PROTECTED_CANDIDATE_SAMPLE_ROLLBACK.plan.sha256
    ) {
      context.addIssue({
        code: "custom",
        message:
          "protected sample plan/manifest binding is not the frozen publication",
        path: ["protectedSampleRollback"],
      });
    }
    const reservedObjectCount =
      plan.controlArtifacts.payloadObjectCount +
      plan.controlArtifacts.controlObjectCount +
      1;
    const reservedBytes =
      plan.controlArtifacts.payloadBytes +
      plan.controlArtifacts.controlBytes +
      CANDIDATE_SOURCE_SNAPSHOT_MAX_PLAN_ARTIFACT_BYTES;
    if (
      plan.inventory.objectCount !== reservedObjectCount ||
      plan.inventory.totalBytes !== reservedBytes
    ) {
      context.addIssue({
        code: "custom",
        message:
          "upload envelope must reserve the payload, compact controls, and one bounded plan artifact",
        path: ["inventory"],
      });
    }
    for (const target of targets) {
      if (
        !target.bucket.endsWith(
          `-${target.domain.replace("_", "-")}-source-snapshot-demo-v1`,
        )
      ) {
        context.addIssue({
          code: "custom",
          message: "source-snapshot demo bucket does not identify its domain",
          path: ["targets", target.domain, "bucket"],
        });
      }
    }
    const expectedPrefixes = {
      control: `publication-control/source-snapshot-demo-v1/${plan.namespaceId}/`,
      openData: `publications/source-snapshot-demo-v1/${plan.namespaceId}/`,
      queryTable: `query-tables/source-snapshot-demo-v1/${plan.namespaceId}/`,
    };
    if (
      plan.targets.controlPrefix !== expectedPrefixes.control ||
      plan.targets.openData.immutablePrefix !== expectedPrefixes.openData ||
      plan.targets.queryTable.immutablePrefix !== expectedPrefixes.queryTable
    ) {
      context.addIssue({
        code: "custom",
        message:
          "source-snapshot demo immutable prefixes do not match namespace",
        path: ["targets"],
      });
    }
    const expectedNamespaceId = createCandidateSourceSnapshotNamespaceId({
      artifactRepresentationVersion: plan.artifactRepresentationVersion,
      limits: plan.limits,
      pricing: plan.pricing,
      source: plan.source,
      targets: {
        openData: {
          bucket: plan.targets.openData.bucket,
          ipnsLabel: plan.targets.openData.ipnsLabel,
          ipnsNetworkKey: plan.targets.openData.ipnsNetworkKey,
          priorCid: plan.targets.openData.priorCid,
        },
        queryTable: {
          bucket: plan.targets.queryTable.bucket,
          ipnsLabel: plan.targets.queryTable.ipnsLabel,
          ipnsNetworkKey: plan.targets.queryTable.ipnsNetworkKey,
          priorCid: plan.targets.queryTable.priorCid,
        },
      },
    });
    if (plan.namespaceId !== expectedNamespaceId) {
      context.addIssue({
        code: "custom",
        message: "source-snapshot demo namespace identity is invalid",
        path: ["namespaceId"],
      });
    }
    if (
      plan.inventory.objectCount > plan.limits.maxObjects ||
      plan.inventory.totalBytes > plan.limits.maxTotalBytes ||
      plan.inventory.maxObjectBytes > plan.limits.maxObjectBytes ||
      plan.requestEnvelope.maximumTotalRequests > plan.limits.maxRequests ||
      plan.costEnvelope.maximumTotalUsd > plan.limits.maxBudgetUsd
    ) {
      context.addIssue({
        code: "custom",
        message: "source-snapshot demo exceeds its immutable hard limits",
        path: ["limits"],
      });
    }
    try {
      const expectedRequestEnvelope =
        createCandidateSourceSnapshotRequestEnvelope({
          limits: plan.limits,
          objectCount: plan.inventory.objectCount,
          recoveryObservationCyclesPerDomain:
            plan.requestEnvelope.recoveryAllowance.observationCyclesPerDomain,
        });
      if (
        canonicalJsonSha256(plan.requestEnvelope) !==
        canonicalJsonSha256(expectedRequestEnvelope)
      ) {
        context.addIssue({
          code: "custom",
          message:
            "request envelope does not match the immutable inventory and limits",
          path: ["requestEnvelope"],
        });
      }
      const expectedCostEnvelope = createCandidateSourceSnapshotCostEnvelope({
        inventoryBytes: plan.inventory.totalBytes,
        limits: plan.limits,
        pricing: plan.pricing,
        requestEnvelope: expectedRequestEnvelope,
      });
      if (
        canonicalJsonSha256(plan.costEnvelope) !==
        canonicalJsonSha256(expectedCostEnvelope)
      ) {
        context.addIssue({
          code: "custom",
          message:
            "cost envelope does not match the immutable bytes, requests, limits, and pricing",
          path: ["costEnvelope"],
        });
      }
    } catch {
      context.addIssue({
        code: "custom",
        message: "request or cost envelope cannot be independently derived",
        path: ["requestEnvelope"],
      });
    }
    if (
      plan.source.membershipPropertyCount !==
      plan.coverage.activeProperties + plan.coverage.inactiveProperties
    ) {
      context.addIssue({
        code: "custom",
        message: "source membership and measured coverage are inconsistent",
        path: ["coverage"],
      });
    }
    if (
      plan.controlArtifacts.fullInventoryRootSha256 !==
        plan.inventory.inventoryRootSha256 ||
      plan.controlArtifacts.objectInventory.indexArtifact.expectedCid !==
        plan.inventory.inventoryRootCid ||
      plan.controlArtifacts.objectInventory.shardCount !==
        plan.inventory.inventoryShardCount
    ) {
      context.addIssue({
        code: "custom",
        message:
          "compact control inventory does not match the upload inventory binding",
        path: ["controlArtifacts", "objectInventory"],
      });
    }
  });

export type CandidateSourceSnapshotDemoPlan = z.infer<
  typeof candidateSourceSnapshotDemoPlanSchema
>;
export type CandidateSourceSnapshotUploadObject = {
  byteSize: number;
  domain: "open_data" | "query_table";
  expectedCid: string;
  logicalObjectKey: string;
  remoteObjectKey: string;
  sha256: string;
};

export function assertCandidateSourceSnapshotObjectNamespace(
  plan: CandidateSourceSnapshotDemoPlan,
  object: CandidateSourceSnapshotUploadObject,
): void {
  const expectedRemoteKeys =
    object.domain === "query_table"
      ? [`${plan.targets.queryTable.immutablePrefix}${object.logicalObjectKey}`]
      : [
          `${plan.targets.openData.immutablePrefix}${object.logicalObjectKey}`,
          `${plan.targets.controlPrefix}${object.logicalObjectKey}`,
        ];
  if (!expectedRemoteKeys.includes(object.remoteObjectKey)) {
    throw new Error(
      "Candidate remote object key is outside its immutable namespace",
    );
  }
}

export const candidateSourceSnapshotExactUploadBindingSchema = z.strictObject({
  exactObjectCount: z.number().int().positive(),
  exactTotalBytes: z.number().int().positive(),
  planArtifact: candidateSourceSnapshotPlanArtifactSchema,
});

export type CandidateSourceSnapshotExactUploadBinding = z.infer<
  typeof candidateSourceSnapshotExactUploadBindingSchema
>;

export function createCandidateSourceSnapshotExactUploadBinding(input: {
  plan: CandidateSourceSnapshotDemoPlan;
  planArtifact: CandidateSourceSnapshotPlanArtifact;
}): CandidateSourceSnapshotExactUploadBinding {
  const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
  const planArtifact = candidateSourceSnapshotPlanArtifactSchema.parse(
    input.planArtifact,
  );
  const exactObjectCount =
    plan.controlArtifacts.payloadObjectCount +
    plan.controlArtifacts.controlObjectCount +
    1;
  const exactTotalBytes =
    plan.controlArtifacts.payloadBytes +
    plan.controlArtifacts.controlBytes +
    planArtifact.byteSize;
  if (
    exactObjectCount > plan.inventory.objectCount ||
    exactTotalBytes > plan.inventory.totalBytes ||
    planArtifact.byteSize > plan.limits.maxObjectBytes
  ) {
    throw new Error("Exact candidate upload binding exceeds the plan envelope");
  }
  if (
    planArtifact.remoteObjectKey !==
    `${plan.targets.controlPrefix}${planArtifact.logicalObjectKey}`
  ) {
    throw new Error(
      "Candidate plan artifact remote key is outside its namespace",
    );
  }
  return candidateSourceSnapshotExactUploadBindingSchema.parse({
    exactObjectCount,
    exactTotalBytes,
    planArtifact,
  });
}

const namespaceTargetSchema = targetSchema.pick({
  bucket: true,
  ipnsLabel: true,
  ipnsNetworkKey: true,
  priorCid: true,
});

export function createCandidateSourceSnapshotNamespaceId(input: {
  artifactRepresentationVersion: typeof CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION;
  limits: CandidateSourceSnapshotLimits;
  pricing: CandidateSourceSnapshotPricing;
  source: CandidateSourceSnapshotSourceBinding;
  targets: {
    openData: z.infer<typeof namespaceTargetSchema>;
    queryTable: z.infer<typeof namespaceTargetSchema>;
  };
}): string {
  const identity = {
    artifactRepresentationVersion: z
      .literal(CANDIDATE_SOURCE_SNAPSHOT_ARTIFACT_REPRESENTATION_VERSION)
      .parse(input.artifactRepresentationVersion),
    limits: candidateSourceSnapshotLimitsSchema.parse(input.limits),
    pricing: candidateSourceSnapshotPricingSchema.parse(input.pricing),
    source: sourceBindingSchema.parse(input.source),
    targets: {
      openData: namespaceTargetSchema.parse(input.targets.openData),
      queryTable: namespaceTargetSchema.parse(input.targets.queryTable),
    },
  };
  return deterministicId("snapshotns", [
    "1.0.0",
    "Publish/pasco/candidate-source-snapshot-namespace",
    canonicalJsonSha256(identity),
  ]);
}

type Counts = Omit<z.infer<typeof operationCountsSchema>, "total">;

function withTotal(counts: Counts): z.infer<typeof operationCountsSchema> {
  const total = Object.values(counts).reduce((sum, count) => sum + count, 0);
  if (!Number.isSafeInteger(total)) {
    throw new Error("Candidate request accounting exceeds safe integer bounds");
  }
  return { ...counts, total };
}

function usd(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error("Candidate cost accounting is invalid");
  }
  return Number(value.toFixed(12));
}

function requestCost(
  counts: z.infer<typeof operationCountsSchema>,
  pricing: CandidateSourceSnapshotPricing,
): number {
  const rates = pricing.operationRates;
  return usd(
    (counts.classAMutations * rates.classAMutationUsdPerThousand +
      counts.classBReads * rates.classBReadUsdPerThousand +
      counts.freeOperations * rates.freeOperationUsdPerThousand +
      counts.namesApiOperations * rates.namesApiUsdPerThousand +
      counts.publicResolverOperations * rates.publicResolverUsdPerThousand) /
      1_000,
  );
}

export function createCandidateSourceSnapshotRequestEnvelope(input: {
  limits: CandidateSourceSnapshotLimits;
  objectCount: number;
  recoveryObservationCyclesPerDomain?: number;
}): CandidateSourceSnapshotRequestEnvelope {
  const limits = candidateSourceSnapshotLimitsSchema.parse(input.limits);
  const objectCount = z.number().int().positive().parse(input.objectCount);
  const cycles = z
    .number()
    .int()
    .positive()
    .max(32)
    .parse(input.recoveryObservationCyclesPerDomain ?? 8);
  const successfulExecution = withTotal({
    classAMutations: objectCount,
    classBReads: 0,
    freeOperations: 0,
    namesApiOperations: 4,
    publicResolverOperations: 4,
  });
  const attempts = limits.maxRetries + 1;
  const maximumAttempts = withTotal({
    classAMutations: successfulExecution.classAMutations * attempts,
    classBReads: successfulExecution.classBReads * attempts,
    freeOperations: successfulExecution.freeOperations,
    namesApiOperations: successfulExecution.namesApiOperations * attempts,
    publicResolverOperations:
      successfulExecution.publicResolverOperations * attempts,
  });
  // Each cycle reads one control-plane value and two independent public
  // resolvers per domain. One bounded reverse rollback allowance adds a Names
  // update + confirmation and two public confirmations, each with retries.
  const recoveryAllowance = {
    ...withTotal({
      classAMutations: 0,
      classBReads: 0,
      freeOperations: 0,
      namesApiOperations: cycles * 2 + attempts * 2,
      publicResolverOperations: cycles * 4 + attempts * 2,
    }),
    observationCyclesPerDomain: cycles,
  };
  const ambiguousInspectionCount = Math.min(
    objectCount,
    Math.max(
      0,
      limits.maxRequests - maximumAttempts.total - recoveryAllowance.total,
    ),
  );
  const ambiguousObjectInspectionAllowance = withTotal({
    classAMutations: 0,
    classBReads: ambiguousInspectionCount,
    freeOperations: 0,
    namesApiOperations: 0,
    publicResolverOperations: 0,
  });
  const maximumTotalRequests =
    maximumAttempts.total +
    recoveryAllowance.total +
    ambiguousObjectInspectionAllowance.total;
  if (
    objectCount > limits.maxObjects ||
    maximumTotalRequests > limits.maxRequests
  ) {
    throw new Error(
      "Candidate request envelope exceeds a configured hard limit",
    );
  }
  return candidateSourceSnapshotRequestEnvelopeSchema.parse({
    ambiguousObjectInspectionAllowance,
    maximumAttempts,
    maximumTotalRequests,
    recoveryAllowance,
    schemaVersion: "candidate-request-envelope-v2",
    successfulExecution,
  });
}

export function createCandidateSourceSnapshotCostEnvelope(input: {
  inventoryBytes: number;
  limits: CandidateSourceSnapshotLimits;
  pricing: CandidateSourceSnapshotPricing;
  requestEnvelope: CandidateSourceSnapshotRequestEnvelope;
}): CandidateSourceSnapshotCostEnvelope {
  const limits = candidateSourceSnapshotLimitsSchema.parse(input.limits);
  const pricing = candidateSourceSnapshotPricingSchema.parse(input.pricing);
  const requests = candidateSourceSnapshotRequestEnvelopeSchema.parse(
    input.requestEnvelope,
  );
  const inventoryBytes = z
    .number()
    .int()
    .positive()
    .parse(input.inventoryBytes);
  if (inventoryBytes > limits.maxTotalBytes) {
    throw new Error("Candidate storage exceeds the configured hard limit");
  }
  const storageUsd = usd(
    (inventoryBytes / 1024 ** 3) * pricing.storageUsdPerGib,
  );
  const successfulRequestUsd = requestCost(
    requests.successfulExecution,
    pricing,
  );
  const maximumAttemptRequestUsd = requestCost(
    requests.maximumAttempts,
    pricing,
  );
  const recoveryRequestUsd = requestCost(requests.recoveryAllowance, pricing);
  const ambiguousObjectInspectionUsd = requestCost(
    requests.ambiguousObjectInspectionAllowance,
    pricing,
  );
  const incrementalExecutionUsd = usd(storageUsd + successfulRequestUsd);
  const maximumIncrementalUsd = usd(
    storageUsd +
      maximumAttemptRequestUsd +
      recoveryRequestUsd +
      ambiguousObjectInspectionUsd,
  );
  const maximumTotalUsd = usd(
    maximumIncrementalUsd + pricing.fixedAccountPlan.monthlyUsd,
  );
  if (maximumTotalUsd > limits.maxBudgetUsd) {
    throw new Error("Candidate cost envelope exceeds the configured budget");
  }
  return candidateSourceSnapshotCostEnvelopeSchema.parse({
    fixedAccountPlanMonthlyUsd: pricing.fixedAccountPlan.monthlyUsd,
    incrementalExecutionUsd,
    maximumIncrementalUsd,
    maximumTotalUsd,
    recoveryRequestUsd,
    requestUsd: {
      ambiguousObjectInspections: ambiguousObjectInspectionUsd,
      maximumAttempts: maximumAttemptRequestUsd,
      successfulExecution: successfulRequestUsd,
    },
    schemaVersion: "candidate-cost-envelope-v2",
    storageUsd,
  });
}

export function conservativeCandidateSourceSnapshotPricing(input: {
  fixedAccountPlanEvidence:
    "human_confirmation_required" | "human_confirmed" | "provider_api";
  fixedAccountPlanMonthlyUsd: number;
  requestUsdPerThousand: number;
  storageUsdPerGib: number;
}): CandidateSourceSnapshotPricing {
  if (
    input.fixedAccountPlanMonthlyUsd !==
      CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.fixedAccountPlanMonthlyUsd ||
    input.requestUsdPerThousand !==
      CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.requestUsdPerThousand ||
    input.storageUsdPerGib !==
      CANDIDATE_SOURCE_SNAPSHOT_CONSERVATIVE_PRICING.storageUsdPerGib
  ) {
    throw new Error(
      "Candidate pricing does not match the reviewed conservative rates",
    );
  }
  return candidateSourceSnapshotPricingSchema.parse({
    fixedAccountPlan: {
      evidence: input.fixedAccountPlanEvidence,
      monthlyUsd: input.fixedAccountPlanMonthlyUsd,
      planName: "Filebase Pro",
    },
    operationRates: {
      classAMutationUsdPerThousand: input.requestUsdPerThousand,
      classBReadUsdPerThousand: input.requestUsdPerThousand,
      freeOperationUsdPerThousand: 0,
      namesApiUsdPerThousand: input.requestUsdPerThousand,
      publicResolverUsdPerThousand: input.requestUsdPerThousand,
    },
    schemaVersion: "candidate-filebase-cost-v2",
    storageUsdPerGib: input.storageUsdPerGib,
  });
}

type PlanWithoutIdentity = Omit<
  CandidateSourceSnapshotDemoPlan,
  "planId" | "planSha256"
>;

export function createCandidateSourceSnapshotDemoPlan(
  value: PlanWithoutIdentity,
): CandidateSourceSnapshotDemoPlan {
  if (
    (value.classification.publicationClass as string) ===
    "authoritative_complete"
  ) {
    throw new Error(
      "Candidate source-snapshot demo rejects authoritative_complete",
    );
  }
  const planSha256 = canonicalJsonSha256(value);
  const planId = deterministicId("snapshotdemo", [
    CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
    "Publish/pasco/candidate-source-snapshot-demo",
    planSha256,
  ]);
  return validateCandidateSourceSnapshotDemoPlan({
    ...value,
    planId,
    planSha256,
  });
}

export function validateCandidateSourceSnapshotDemoPlan(
  value: unknown,
): CandidateSourceSnapshotDemoPlan {
  const plan = candidateSourceSnapshotDemoPlanSchema.parse(value);
  const { planId: _planId, planSha256: _planSha256, ...withoutIdentity } = plan;
  const expectedSha256 = canonicalJsonSha256(withoutIdentity);
  const expectedId = deterministicId("snapshotdemo", [
    CANDIDATE_SOURCE_SNAPSHOT_DEMO_PLAN_VERSION,
    "Publish/pasco/candidate-source-snapshot-demo",
    expectedSha256,
  ]);
  if (plan.planSha256 !== expectedSha256 || plan.planId !== expectedId) {
    throw new Error("Candidate source-snapshot plan identity is invalid");
  }
  return plan;
}

export function candidateSourceSnapshotObjectSchema() {
  return z.strictObject({
    byteSize: z
      .number()
      .int()
      .nonnegative()
      .max(CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS.maxObjectBytes),
    domain: domainSchema,
    expectedCid: cidSchema,
    logicalObjectKey: safeObjectKeySchema,
    remoteObjectKey: safeObjectKeySchema,
    sha256: sha256Schema,
  });
}
