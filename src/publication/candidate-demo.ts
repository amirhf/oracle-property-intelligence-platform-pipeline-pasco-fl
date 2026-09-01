import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import { calculateIpfsCid } from "./ipfs-cid.js";
import { validatePublicationPlan, type PublicationPlan } from "./plan.js";

export const CANDIDATE_DEMO_WORDING =
  "Temporary candidate-owned Filebase demonstration of protocol compatibility. The buckets and IPNS identities are candidate-controlled and are not represented as Elephant-owned, owner-approved, or the final canonical assessment publication.";
export const CANDIDATE_DEMO_PLAN_VERSION = "1.0.0";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const priorCidSchema = z.union([
  cidSchema,
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const domainSchema = z.enum(["open_data", "query_table"]);
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

const candidateTargetSchema = z.strictObject({
  bucket: candidateResourceNameSchema,
  domain: domainSchema,
  ipnsLabel: candidateResourceNameSchema,
  ipnsNetworkKey: networkKeySchema,
  priorCid: priorCidSchema,
  targetCid: cidSchema,
});

export const candidateDemoLimitsSchema = z.strictObject({
  maxBudgetUsd: z.number().nonnegative().max(10_000),
  maxConcurrency: z.number().int().min(1).max(16),
  maxObjectBytes: z
    .number()
    .int()
    .positive()
    .max(512 * 1024 * 1024),
  maxObjects: z.number().int().positive().max(100_000),
  maxRequests: z.number().int().positive().max(500_000),
  maxRetries: z.number().int().min(0).max(3),
  maxTotalBytes: z
    .number()
    .int()
    .positive()
    .max(20 * 1024 * 1024 * 1024),
  requestTimeoutMs: z.number().int().min(500).max(30_000),
  requestUsdPerThousand: z.number().nonnegative().max(100),
  storageUsdPerGib: z.number().nonnegative().max(100),
});

export type CandidateDemoLimits = z.infer<typeof candidateDemoLimitsSchema>;

export const candidateDemoPlanSchema = z
  .strictObject({
    coverageMode: z.enum(["sample", "partial"]),
    demoPlanId: z.string().regex(/^demo_[a-f0-9]{32}$/),
    demoPlanSha256: sha256Schema,
    disclaimer: z.literal(CANDIDATE_DEMO_WORDING),
    estimatedBudgetUsd: z.number().nonnegative(),
    estimatedRequestCount: z.number().int().positive(),
    limits: candidateDemoLimitsSchema,
    objectCount: z.number().int().positive(),
    objects: z
      .array(
        z.strictObject({
          byteSize: z.number().int().nonnegative(),
          domain: domainSchema,
          expectedCid: cidSchema,
          objectKey: z
            .string()
            .min(1)
            .max(2_048)
            .refine(
              (value) =>
                !value.startsWith("/") &&
                !value.includes("\\") &&
                !value.split("/").includes(".."),
              "must be a safe publication-relative object key",
            ),
          sha256: sha256Schema,
        }),
      )
      .min(1),
    sourcePlanId: z.string().regex(/^plan_[a-f0-9]{32}$/),
    sourcePlanSha256: sha256Schema,
    preflightEvidenceSha256: sha256Schema,
    preflightObservedAt: z.string().datetime(),
    targets: z.strictObject({
      openData: candidateTargetSchema.extend({
        domain: z.literal("open_data"),
      }),
      queryTable: candidateTargetSchema.extend({
        domain: z.literal("query_table"),
      }),
    }),
    totalBytes: z.number().int().nonnegative(),
    version: z.literal(CANDIDATE_DEMO_PLAN_VERSION),
  })
  .superRefine((plan, context) => {
    const objectKeys = plan.objects.map(
      (object) => `${object.domain}:${object.objectKey}`,
    );
    if (new Set(objectKeys).size !== objectKeys.length) {
      context.addIssue({
        code: "custom",
        message: "candidate demo object inventory contains duplicate keys",
        path: ["objects"],
      });
    }
    if (
      plan.targets.openData.bucket === plan.targets.queryTable.bucket ||
      plan.targets.openData.ipnsLabel === plan.targets.queryTable.ipnsLabel ||
      plan.targets.openData.ipnsNetworkKey ===
        plan.targets.queryTable.ipnsNetworkKey
    ) {
      context.addIssue({
        code: "custom",
        message: "candidate demo domains require distinct resources",
        path: ["targets"],
      });
    }
    const candidatePrefix = (
      openDataName: string,
      queryTableName: string,
    ): string | null => {
      const openSuffix = "-open-data-demo";
      const querySuffix = "-query-table-demo";
      if (
        !openDataName.endsWith(openSuffix) ||
        !queryTableName.endsWith(querySuffix)
      ) {
        return null;
      }
      const openPrefix = openDataName.slice(0, -openSuffix.length);
      const queryPrefix = queryTableName.slice(0, -querySuffix.length);
      return openPrefix.length >= 3 && openPrefix === queryPrefix
        ? openPrefix
        : null;
    };
    if (
      candidatePrefix(
        plan.targets.openData.bucket,
        plan.targets.queryTable.bucket,
      ) === null ||
      candidatePrefix(
        plan.targets.openData.ipnsLabel,
        plan.targets.queryTable.ipnsLabel,
      ) === null
    ) {
      context.addIssue({
        code: "custom",
        message:
          "candidate demo targets require one candidate-owned prefix and explicit open-data-demo/query-table-demo roles",
        path: ["targets"],
      });
    }
    if (plan.targets.openData.priorCid === plan.targets.queryTable.priorCid) {
      context.addIssue({
        code: "custom",
        message: "candidate demo domains require distinct prior CIDs",
        path: ["targets"],
      });
    }
    if (
      plan.objectCount !== plan.objects.length ||
      plan.totalBytes !==
        plan.objects.reduce((total, object) => total + object.byteSize, 0)
    ) {
      context.addIssue({
        code: "custom",
        message: "candidate demo object inventory is inconsistent",
        path: ["objects"],
      });
    }
  });

export type CandidateDemoPlan = z.infer<typeof candidateDemoPlanSchema>;
export type CandidateDemoTarget = CandidateDemoPlan["targets"][
  "openData" | "queryTable"];

export interface CandidateDemoTargetInput {
  openData: {
    bucket: string;
    ipnsLabel: string;
    ipnsNetworkKey: string;
    priorCid: string;
  };
  queryTable: {
    bucket: string;
    ipnsLabel: string;
    ipnsNetworkKey: string;
    priorCid: string;
  };
}

export type CandidateDemoTargetIdentityInput = {
  [Domain in keyof CandidateDemoTargetInput]: Omit<
    CandidateDemoTargetInput[Domain],
    "priorCid"
  >;
};

function identity(
  value: Omit<CandidateDemoPlan, "demoPlanId" | "demoPlanSha256">,
) {
  return value;
}

export async function createCandidateDemoPlan(input: {
  limits: CandidateDemoLimits;
  preflightEvidenceSha256: string;
  preflightObservedAt: string;
  sourcePlan: PublicationPlan;
  targets: CandidateDemoTargetInput;
}): Promise<CandidateDemoPlan> {
  const sourcePlan = validatePublicationPlan(input.sourcePlan);
  if (sourcePlan.coverage.mode === "authoritative_complete") {
    throw new Error(
      "Candidate demo publication rejects authoritative_complete coverage",
    );
  }
  if (
    sourcePlan.coverage.mode === "partial" &&
    sourcePlan.projection.authoritativeBaseSnapshotId === null
  ) {
    throw new Error(
      "Candidate demo partial publication requires an authoritative base",
    );
  }
  const limits = candidateDemoLimitsSchema.parse(input.limits);
  const sourcePlanBytes = Buffer.from(`${canonicalJson(sourcePlan)}\n`);
  const sourcePlanObject = {
    byteSize: sourcePlanBytes.length,
    domain: "open_data" as const,
    expectedCid: await calculateIpfsCid(sourcePlanBytes),
    objectKey: "publication-dry-run-plan.json",
    sha256: sha256(sourcePlanBytes),
  };
  const objects = [
    ...sourcePlan.artifacts.objectInventory.map((artifact) => ({
      byteSize: artifact.byteSize,
      domain: artifact.domain,
      expectedCid: artifact.expectedCid,
      objectKey: artifact.objectKey,
      sha256: artifact.sha256,
    })),
    sourcePlanObject,
  ];
  const objectCount = objects.length;
  const totalBytes = objects.reduce(
    (total, artifact) => total + artifact.byteSize,
    0,
  );
  const maximumObjectBytes = objects.reduce(
    (maximum, artifact) => Math.max(maximum, artifact.byteSize),
    0,
  );
  const requestCount = objectCount + 8 + limits.maxRetries * (objectCount + 4);
  const estimatedBudgetUsd =
    (totalBytes / 1024 ** 3) * limits.storageUsdPerGib +
    (requestCount / 1_000) * limits.requestUsdPerThousand;
  if (
    objectCount > limits.maxObjects ||
    totalBytes > limits.maxTotalBytes ||
    maximumObjectBytes > limits.maxObjectBytes ||
    requestCount > limits.maxRequests ||
    estimatedBudgetUsd > limits.maxBudgetUsd
  ) {
    throw new Error(
      "Candidate demo publication exceeds a configured hard limit",
    );
  }
  const withoutIdentity = identity({
    coverageMode: sourcePlan.coverage.mode,
    disclaimer: CANDIDATE_DEMO_WORDING,
    estimatedBudgetUsd,
    estimatedRequestCount: requestCount,
    limits,
    objectCount,
    objects,
    sourcePlanId: sourcePlan.planId,
    sourcePlanSha256: sourcePlan.planSha256,
    preflightEvidenceSha256: sha256Schema.parse(input.preflightEvidenceSha256),
    preflightObservedAt: z.string().datetime().parse(input.preflightObservedAt),
    targets: {
      openData: {
        ...input.targets.openData,
        domain: "open_data" as const,
        targetCid: sourcePlan.graph.openDataRoot.expectedCid,
      },
      queryTable: {
        ...input.targets.queryTable,
        domain: "query_table" as const,
        targetCid: sourcePlan.graph.queryTableRoot.expectedCid,
      },
    },
    totalBytes,
    version: CANDIDATE_DEMO_PLAN_VERSION,
  });
  const demoPlanSha256 = canonicalJsonSha256(withoutIdentity);
  return candidateDemoPlanSchema.parse({
    ...withoutIdentity,
    demoPlanId: deterministicId("demo", [
      CANDIDATE_DEMO_PLAN_VERSION,
      "Publish/pasco/candidate-demo",
      demoPlanSha256,
    ]),
    demoPlanSha256,
  });
}

export function validateCandidateDemoPlan(value: unknown): CandidateDemoPlan {
  const plan = candidateDemoPlanSchema.parse(value);
  const {
    demoPlanId: _demoPlanId,
    demoPlanSha256: _demoPlanSha256,
    ...withoutIdentity
  } = plan;
  const expectedSha256 = canonicalJsonSha256(identity(withoutIdentity));
  const expectedId = deterministicId("demo", [
    CANDIDATE_DEMO_PLAN_VERSION,
    "Publish/pasco/candidate-demo",
    expectedSha256,
  ]);
  if (
    plan.demoPlanSha256 !== expectedSha256 ||
    plan.demoPlanId !== expectedId
  ) {
    throw new Error("Candidate demo plan identity is invalid");
  }
  return plan;
}
