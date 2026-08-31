import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import {
  candidateSourceSnapshotExactUploadBindingSchema,
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotExactUploadBinding,
} from "../publication/candidate-source-snapshot-demo.js";

export const CANDIDATE_SOURCE_SNAPSHOT_AUTHORIZATION_BINDING_VERSION =
  "candidate-source-snapshot-authorization-binding-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_VERSION =
  "candidate-source-snapshot-approval-v2" as const;

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const priorCidSchema = z.union([
  cidSchema,
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const networkKeySchema = z.string().regex(/^k51[0-9a-z]{59}$/);
const resourceSchema = z.strictObject({
  bucket: z.string().min(1).max(200),
  immutablePrefix: z.string().min(1).max(2_048),
  ipnsLabel: z.string().min(1).max(200),
  ipnsNetworkKey: networkKeySchema,
  priorCid: priorCidSchema,
  targetCid: cidSchema,
});

export const candidateSourceSnapshotAuthorizationBindingSchema = z
  .strictObject({
    classification: z.strictObject({
      canonical: z.literal(false),
      elephantOwned: z.literal(false),
      independentlyPascoCertified: z.literal(false),
      ownerControlled: z.literal(false),
      publicationClass: z.literal("candidate_owned_source_snapshot_demo"),
      resourceOwner: z.literal("candidate"),
      sourceScope: z.literal("exact_hash_bound_2026_08_23_parcel_snapshot"),
    }),
    execution: z.strictObject({
      absoluteRequestCeiling: z.number().int().positive(),
      ambiguousInspectionAllowance: z.number().int().nonnegative(),
      cutoverOrder: z.tuple([z.literal("open_data"), z.literal("query_table")]),
      maximumAttemptCount: z.number().int().positive(),
      maximumAttemptsPerObject: z.number().int().positive(),
      maximumConcurrency: z.number().int().positive(),
      maximumRetries: z.number().int().nonnegative(),
      recoveryAllowance: z.number().int().nonnegative(),
      requestEnvelopeSha256: sha256Schema,
      requestTimeoutMs: z.number().int().positive(),
      spendingCeilingUsd: z.number().positive(),
      successfulRequestCount: z.number().int().positive(),
    }),
    inventory: z.strictObject({
      admissionReservedBytes: z.number().int().positive(),
      costEnvelopeSha256: sha256Schema,
      exactObjectCount: z.number().int().positive(),
      exactTotalBytes: z.number().int().positive(),
      fullInventorySha256: sha256Schema,
      inventoryCid: cidSchema,
      manifestCid: cidSchema,
      manifestSha256: sha256Schema,
      maximumObjectCount: z.number().int().positive(),
      maximumTotalBytes: z.number().int().positive(),
    }),
    plan: z.strictObject({
      artifactByteSize: z.number().int().positive(),
      artifactCid: cidSchema,
      artifactRemoteObjectKey: z.string().min(1).max(2_048),
      artifactSha256: sha256Schema,
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planLogicalSha256: sha256Schema,
    }),
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_AUTHORIZATION_BINDING_VERSION,
    ),
    targets: z.strictObject({
      openData: resourceSchema,
      queryTable: resourceSchema,
    }),
  })
  .superRefine((binding, context) => {
    for (const [domain, target] of [
      ["openData", binding.targets.openData],
      ["queryTable", binding.targets.queryTable],
    ] as const) {
      if (target.bucket !== target.ipnsLabel) {
        context.addIssue({
          code: "custom",
          message:
            "candidate source-snapshot authorization requires identical bucket and IPNS label bindings",
          path: ["targets", domain, "ipnsLabel"],
        });
      }
    }
  });

export type CandidateSourceSnapshotAuthorizationBinding = z.infer<
  typeof candidateSourceSnapshotAuthorizationBindingSchema
>;

function authorizationBinding(
  planValue: CandidateSourceSnapshotDemoPlan,
  exactUploadValue: CandidateSourceSnapshotExactUploadBinding,
): CandidateSourceSnapshotAuthorizationBinding {
  const plan = validateCandidateSourceSnapshotDemoPlan(planValue);
  const exactUpload =
    candidateSourceSnapshotExactUploadBindingSchema.parse(exactUploadValue);
  return candidateSourceSnapshotAuthorizationBindingSchema.parse({
    classification: plan.classification,
    execution: {
      absoluteRequestCeiling: plan.requestEnvelope.maximumTotalRequests,
      ambiguousInspectionAllowance:
        plan.requestEnvelope.ambiguousObjectInspectionAllowance.total,
      cutoverOrder: plan.protectedSampleRollback.cutoverOrder,
      maximumAttemptCount: plan.requestEnvelope.maximumAttempts.total,
      maximumAttemptsPerObject: plan.limits.maxRetries + 1,
      maximumConcurrency: plan.limits.maxConcurrency,
      maximumRetries: plan.limits.maxRetries,
      recoveryAllowance: plan.requestEnvelope.recoveryAllowance.total,
      requestEnvelopeSha256: canonicalJsonSha256(plan.requestEnvelope),
      requestTimeoutMs: plan.limits.requestTimeoutMs,
      spendingCeilingUsd: plan.limits.maxBudgetUsd,
      successfulRequestCount: plan.requestEnvelope.successfulExecution.total,
    },
    inventory: {
      admissionReservedBytes: plan.inventory.totalBytes,
      costEnvelopeSha256: canonicalJsonSha256(plan.costEnvelope),
      exactObjectCount: exactUpload.exactObjectCount,
      exactTotalBytes: exactUpload.exactTotalBytes,
      fullInventorySha256: plan.controlArtifacts.fullInventoryRootSha256,
      inventoryCid: plan.inventory.inventoryRootCid,
      manifestCid: plan.controlArtifacts.manifestIndex.expectedCid,
      manifestSha256: plan.controlArtifacts.manifestIndex.sha256,
      maximumObjectCount: plan.limits.maxObjects,
      maximumTotalBytes: plan.limits.maxTotalBytes,
    },
    plan: {
      artifactByteSize: exactUpload.planArtifact.byteSize,
      artifactCid: exactUpload.planArtifact.expectedCid,
      artifactRemoteObjectKey: exactUpload.planArtifact.remoteObjectKey,
      artifactSha256: exactUpload.planArtifact.sha256,
      planId: plan.planId,
      planLogicalSha256: plan.planSha256,
    },
    schemaVersion: CANDIDATE_SOURCE_SNAPSHOT_AUTHORIZATION_BINDING_VERSION,
    targets: {
      openData: {
        bucket: plan.targets.openData.bucket,
        immutablePrefix: plan.targets.openData.immutablePrefix,
        ipnsLabel: plan.targets.openData.ipnsLabel,
        ipnsNetworkKey: plan.targets.openData.ipnsNetworkKey,
        priorCid: plan.targets.openData.priorCid,
        targetCid: plan.targets.openData.targetCid,
      },
      queryTable: {
        bucket: plan.targets.queryTable.bucket,
        immutablePrefix: plan.targets.queryTable.immutablePrefix,
        ipnsLabel: plan.targets.queryTable.ipnsLabel,
        ipnsNetworkKey: plan.targets.queryTable.ipnsNetworkKey,
        priorCid: plan.targets.queryTable.priorCid,
        targetCid: plan.targets.queryTable.targetCid,
      },
    },
  });
}

export function renderCandidateSourceSnapshotAuthorizationStatement(
  planValue: CandidateSourceSnapshotDemoPlan,
  exactUploadValue: CandidateSourceSnapshotExactUploadBinding,
): string {
  const binding = authorizationBinding(planValue, exactUploadValue);
  return renderCandidateSourceSnapshotAuthorizationBindingStatement(binding);
}

export function renderCandidateSourceSnapshotAuthorizationBindingStatement(
  bindingValue: CandidateSourceSnapshotAuthorizationBinding,
): string {
  const binding =
    candidateSourceSnapshotAuthorizationBindingSchema.parse(bindingValue);
  const { execution, inventory, plan, targets } = binding;
  if (
    execution.maximumRetries !== 2 ||
    execution.maximumAttemptsPerObject !== 3
  ) {
    throw new Error(
      "Candidate source-snapshot authorization statement requires the reviewed two-retry envelope",
    );
  }
  return `I confirm the candidate-controlled Filebase account is Pro or better and supports at least ${inventory.maximumObjectCount} pinned objects, ${inventory.maximumTotalBytes} bytes, two distinct buckets and two distinct IPNS names, and I approve only candidate_owned_source_snapshot_demo plan ${plan.planId} with logical SHA-256 ${plan.planLogicalSha256}, plan artifact SHA-256 ${plan.artifactSha256} and CID ${plan.artifactCid}, exactly ${inventory.exactObjectCount} objects and ${inventory.exactTotalBytes} upload bytes with ${inventory.admissionReservedBytes} admission-reserved bytes, open-data bucket and label ${targets.openData.bucket} under immutable prefix ${targets.openData.immutablePrefix} and network key ${targets.openData.ipnsNetworkKey} from prior ${targets.openData.priorCid} to target ${targets.openData.targetCid}, query-table bucket and label ${targets.queryTable.bucket} under immutable prefix ${targets.queryTable.immutablePrefix} and network key ${targets.queryTable.ipnsNetworkKey} from prior ${targets.queryTable.priorCid} to target ${targets.queryTable.targetCid}, manifest CID ${inventory.manifestCid} and SHA-256 ${inventory.manifestSha256}, inventory CID ${inventory.inventoryCid} and full-inventory SHA-256 ${inventory.fullInventorySha256}, successful request count ${execution.successfulRequestCount}, maximum-attempt count ${execution.maximumAttemptCount}, ambiguous-inspection allowance ${execution.ambiguousInspectionAllowance}, recovery allowance ${execution.recoveryAllowance}, absolute request ceiling ${execution.absoluteRequestCeiling}, two retries, three total object attempts, concurrency ${execution.maximumConcurrency}, ${execution.requestTimeoutMs} ms timeout and USD ${execution.spendingCeilingUsd} spending ceiling for uploading only these immutable objects and then updating only these two candidate IPNS identities in durable open-data-first/query-table-second order after exact provider-CID verification; this authorization is candidate-only and noncanonical and does not authorize or represent Elephant-owned, owner-controlled, owner/canonical, authoritative-complete, independently Pasco-certified, Accela/BBB, production-database, Vercel-deployment or any other publication authority.`;
}

export function parseCandidateSourceSnapshotAuthorizationStatement(input: {
  exactUpload: CandidateSourceSnapshotExactUploadBinding;
  plan: CandidateSourceSnapshotDemoPlan;
  statement: unknown;
}): {
  authorizationBinding: CandidateSourceSnapshotAuthorizationBinding;
  authorizationBindingSha256: string;
  authorizationStatement: string;
  authorizationStatementSha256: string;
} {
  const authorizationStatement = z
    .string()
    .min(1)
    .max(8_192)
    .parse(input.statement);
  const expected = renderCandidateSourceSnapshotAuthorizationStatement(
    input.plan,
    input.exactUpload,
  );
  if (authorizationStatement !== expected) {
    throw new Error(
      "Candidate source-snapshot authorization statement does not exactly match the immutable plan",
    );
  }
  const binding = authorizationBinding(input.plan, input.exactUpload);
  return {
    authorizationBinding: binding,
    authorizationBindingSha256: canonicalJsonSha256(binding),
    authorizationStatement,
    authorizationStatementSha256: sha256(authorizationStatement),
  };
}

export function createCandidateSourceSnapshotApprovalIdentity(input: {
  approvedAt: string;
  approverReference: string;
  exactUpload: CandidateSourceSnapshotExactUploadBinding;
  plan: CandidateSourceSnapshotDemoPlan;
  statement: unknown;
}): ReturnType<typeof parseCandidateSourceSnapshotAuthorizationStatement> & {
  approvalId: string;
  approvalSha256: string;
  approvalVersion: typeof CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_VERSION;
} {
  const approvedAt = z
    .string()
    .datetime({ offset: true })
    .refine(
      (value) => new Date(value).toISOString() === value,
      "approval timestamp must be canonical UTC with millisecond precision",
    )
    .parse(input.approvedAt);
  const approverReference = z
    .string()
    .regex(/^[a-z0-9][a-z0-9_-]{2,127}$/)
    .parse(input.approverReference);
  const parsed = parseCandidateSourceSnapshotAuthorizationStatement({
    exactUpload: input.exactUpload,
    plan: input.plan,
    statement: input.statement,
  });
  const approvalVersion = CANDIDATE_SOURCE_SNAPSHOT_APPROVAL_VERSION;
  const approvalSha256 = canonicalJsonSha256({
    approvalVersion,
    approvedAt,
    approverReference,
    authorizationBinding: parsed.authorizationBinding,
    authorizationBindingSha256: parsed.authorizationBindingSha256,
    authorizationStatement: parsed.authorizationStatement,
    authorizationStatementSha256: parsed.authorizationStatementSha256,
  });
  return {
    ...parsed,
    approvalId: deterministicId("snapshotdemoapproval", [
      approvalVersion,
      input.plan.planId,
      approvalSha256,
    ]),
    approvalSha256,
    approvalVersion,
  };
}
