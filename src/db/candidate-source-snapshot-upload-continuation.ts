import postgres from "postgres";
import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";

export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_BINDING_VERSION =
  "candidate-source-snapshot-upload-continuation-binding-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_AUTHORIZATION_VERSION =
  "candidate-source-snapshot-upload-continuation-authorization-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_VERSION =
  "candidate-source-snapshot-executor-lease-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT =
  "https://s3.filebase.io" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONNECTION_TIMEOUT_MS =
  15_000 as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_SOCKET_TIMEOUT_MS =
  45_000 as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_BUFFER_BODY_MAX_BYTES =
  1_048_576 as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONCURRENCY_STAGES = [
  4, 8, 16,
] as const;
export const CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_PROMOTION_VERIFIED_OBJECTS_PER_STAGE =
  64 as const;

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const commitShaSchema = z.string().regex(/^[a-f0-9]{40}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const timestampSchema = z
  .string()
  .regex(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)
  .refine((value) => new Date(value).toISOString() === value);
const referenceSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/);
const moneySchema = z.string().regex(/^[0-9]+\.[0-9]{12}$/);
const planIdSchema = z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/);
const approvalIdSchema = z
  .string()
  .regex(/^snapshotdemoapproval_[a-f0-9]{32}$/);
const authorizationIdSchema = z
  .string()
  .regex(/^snapshotdemouploadcontinuation_[a-f0-9]{32}$/);
const leaseIdSchema = z
  .string()
  .regex(/^snapshotdemoexecutorlease_[a-f0-9]{32}$/);
const domainSchema = z.enum(["open_data", "query_table"]);

export const candidateSourceSnapshotUploadContinuationBindingSchema = z
  .strictObject({
    amendedImplementationCommitSha: commitShaSchema,
    approval: z.strictObject({
      approvalId: approvalIdSchema,
      approvalSha256: sha256Schema,
      authorizationStatementSha256: sha256Schema,
      originalImplementationCommitSha: commitShaSchema,
    }),
    checkpoint: z.strictObject({
      admittedObjectCount: z.number().int().nonnegative(),
      failedTerminalObjectCount: z.literal(0),
      outcomeUnknownObjectCount: z.number().int().nonnegative(),
      pendingObjectCount: z.number().int().nonnegative(),
      uncertainObjectCount: z.number().int().positive(),
      uncertainSetSha256: sha256Schema,
      verifiedBytes: z.number().int().nonnegative(),
      verifiedObjectCount: z.number().int().nonnegative(),
      verifiedReceiptSetSha256: sha256Schema,
    }),
    execution: z.strictObject({
      bufferBodyMaxBytes: z.literal(
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_BUFFER_BODY_MAX_BYTES,
      ),
      connectionTimeoutMs: z.literal(
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONNECTION_TIMEOUT_MS,
      ),
      concurrencyStages: z.tuple([z.literal(4), z.literal(8), z.literal(16)]),
      executorLeaseLimit: z.literal(1),
      maxSocketsStages: z.tuple([z.literal(4), z.literal(8), z.literal(16)]),
      promotionVerifiedObjectsPerStage: z.literal(
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_PROMOTION_VERIFIED_OBJECTS_PER_STAGE,
      ),
      reconciliationRequired: z.literal(true),
      requestTimeoutMs: z.number().int().gt(45_000).max(60_000),
      s3Endpoint: z.literal(CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT),
      socketTimeoutMs: z.literal(
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_SOCKET_TIMEOUT_MS,
      ),
    }),
    inventory: z.strictObject({
      exactObjectCount: z.number().int().positive(),
      exactTotalBytes: z.number().int().positive(),
      fullInventorySha256: sha256Schema,
      inventoryCid: cidSchema,
      inventoryRootSha256: sha256Schema,
    }),
    plan: z.strictObject({
      artifactCid: cidSchema,
      artifactSha256: sha256Schema,
      planId: planIdSchema,
      planRevision: z.number().int().positive(),
      planSha256: sha256Schema,
    }),
    predecessor: z.strictObject({
      authorizationId: z
        .string()
        .regex(/^snapshotdemocontinuation_[a-f0-9]{32}$/)
        .nullable(),
      authorizationSha256: sha256Schema.nullable(),
      implementationCommitSha: commitShaSchema,
    }),
    remainingAllowance: z.strictObject({
      absoluteRequestCeiling: z.number().int().positive(),
      costEnvelopeSha256: sha256Schema,
      hardBudgetCeilingUsd: moneySchema,
      hardBudgetRemainingUsd: moneySchema,
      requestEnvelopeSha256: sha256Schema,
      requestsRemaining: z.number().int().nonnegative(),
    }),
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_BINDING_VERSION,
    ),
    targetsSha256: sha256Schema,
  })
  .superRefine((value, context) => {
    if (
      (value.predecessor.authorizationId === null) !==
      (value.predecessor.authorizationSha256 === null)
    ) {
      context.addIssue({
        code: "custom",
        message:
          "predecessor authorization identity must be all-null or complete",
        path: ["predecessor"],
      });
    }
    if (
      value.amendedImplementationCommitSha ===
      value.predecessor.implementationCommitSha
    ) {
      context.addIssue({
        code: "custom",
        message: "amended implementation commit must change",
        path: ["amendedImplementationCommitSha"],
      });
    }
  });

export type CandidateSourceSnapshotUploadContinuationBinding = z.infer<
  typeof candidateSourceSnapshotUploadContinuationBindingSchema
>;

export const candidateSourceSnapshotUploadContinuationAuthorizationSchema =
  z.strictObject({
    authorizationBinding:
      candidateSourceSnapshotUploadContinuationBindingSchema,
    authorizationBindingSha256: sha256Schema,
    authorizationId: authorizationIdSchema,
    authorizationSha256: sha256Schema,
    authorizationStatement: z.string().min(1).max(12_000),
    authorizationStatementSha256: sha256Schema,
    authorizationVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_AUTHORIZATION_VERSION,
    ),
    authorizedAt: timestampSchema,
    authorizerReference: referenceSchema,
  });

export type CandidateSourceSnapshotUploadContinuationAuthorization = z.infer<
  typeof candidateSourceSnapshotUploadContinuationAuthorizationSchema
>;

export const candidateSourceSnapshotUploadContinuationUncertaintySchema =
  z.strictObject({
    authorizationId: authorizationIdSchema,
    domain: domainSchema,
    expectedBytes: z.number().int().nonnegative().max(536_870_912),
    expectedCid: cidSchema,
    expectedSha256: sha256Schema,
    planId: planIdSchema,
    remoteObjectKey: z.string().min(1),
    sourceAttemptId: z.string().regex(/^snapshotdemoattempt_[a-f0-9]{32}$/),
    sourceRequestId: z.string().regex(/^snapshotdemorequest_[a-f0-9]{32}$/),
    uncertaintyKind: z.enum(["stale_request_started", "outcome_unknown"]),
  });

export type CandidateSourceSnapshotUploadContinuationUncertainty = z.infer<
  typeof candidateSourceSnapshotUploadContinuationUncertaintySchema
>;

const leasePhaseSchema = z.enum([
  "reconciling",
  "upload_4",
  "upload_8",
  "upload_16",
  "released",
]);

export const candidateSourceSnapshotExecutorLeaseSchema = z.strictObject({
  acquiredAt: timestampSchema,
  authorizationId: authorizationIdSchema,
  effectiveConcurrency: z.union([
    z.literal(0),
    z.literal(4),
    z.literal(8),
    z.literal(16),
  ]),
  expiresAt: timestampSchema,
  heartbeatAt: timestampSchema,
  holderTokenSha256: sha256Schema,
  leaseEpoch: z.literal(1),
  leaseId: leaseIdSchema,
  leaseVersion: z.literal(CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_VERSION),
  phase: leasePhaseSchema,
  planId: planIdSchema,
  revision: z.number().int().positive(),
});

export type CandidateSourceSnapshotExecutorLease = z.infer<
  typeof candidateSourceSnapshotExecutorLeaseSchema
>;

export const candidateSourceSnapshotUploadExecutionPermitSchema =
  z.strictObject({
    authorizationId: authorizationIdSchema,
    authorizationSha256: sha256Schema,
    bufferBodyMaxBytes: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_BUFFER_BODY_MAX_BYTES,
    ),
    connectionTimeoutMs: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONNECTION_TIMEOUT_MS,
    ),
    effectiveConcurrency: z.union([z.literal(4), z.literal(8), z.literal(16)]),
    executorLeaseId: leaseIdSchema,
    leaseEpoch: z.literal(1),
    leaseRevision: z.number().int().positive(),
    maxSockets: z.union([z.literal(4), z.literal(8), z.literal(16)]),
    phase: z.enum(["upload_4", "upload_8", "upload_16"]),
    planId: planIdSchema,
    planSha256: sha256Schema,
    reconciliationComplete: z.literal(true),
    requestTimeoutMs: z.number().int().gt(45_000).max(60_000),
    s3Endpoint: z.literal(CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT),
    socketTimeoutMs: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_SOCKET_TIMEOUT_MS,
    ),
  });

export type CandidateSourceSnapshotUploadExecutionPermit = z.infer<
  typeof candidateSourceSnapshotUploadExecutionPermitSchema
>;

export function renderCandidateSourceSnapshotUploadContinuationStatement(
  bindingValue: CandidateSourceSnapshotUploadContinuationBinding,
  authorizerReferenceValue: string,
  authorizedAtValue: string,
): string {
  const binding =
    candidateSourceSnapshotUploadContinuationBindingSchema.parse(bindingValue);
  const authorizerReference = referenceSchema.parse(authorizerReferenceValue);
  const authorizedAt = timestampSchema.parse(authorizedAtValue);
  return `I authorize exactly one fail-closed candidate-owned source-snapshot upload continuation for plan ${binding.plan.planId}, logical SHA-256 ${binding.plan.planSha256}, at durable plan revision ${binding.plan.planRevision}, under unchanged approval ${binding.approval.approvalId}, approval SHA-256 ${binding.approval.approvalSha256}, from authorized implementation commit ${binding.predecessor.implementationCommitSha} to amended implementation commit ${binding.amendedImplementationCommitSha}. It preserves ${binding.checkpoint.verifiedObjectCount} verified objects and ${binding.checkpoint.verifiedBytes} verified bytes under receipt-set SHA-256 ${binding.checkpoint.verifiedReceiptSetSha256}, and requires reconciliation of ${binding.checkpoint.uncertainObjectCount} uncertain objects under set SHA-256 ${binding.checkpoint.uncertainSetSha256} before any upload. It authorizes request timeout ${binding.execution.requestTimeoutMs} ms over compiled S3 endpoint ${binding.execution.s3Endpoint}, connection timeout ${binding.execution.connectionTimeoutMs} ms, socket timeout ${binding.execution.socketTimeoutMs} ms, immutable-buffer threshold ${binding.execution.bufferBodyMaxBytes} bytes, staged concurrency/maxSockets 4 then 8 then 16 after ${binding.execution.promotionVerifiedObjectsPerStage} newly verified objects per promotion, and exactly one executor lease. It leaves unchanged inventory CID ${binding.inventory.inventoryCid}, inventory SHA-256 ${binding.inventory.fullInventorySha256}, exactly ${binding.inventory.exactObjectCount} objects and ${binding.inventory.exactTotalBytes} bytes, targets SHA-256 ${binding.targetsSha256}, request-envelope SHA-256 ${binding.remainingAllowance.requestEnvelopeSha256}, cost-envelope SHA-256 ${binding.remainingAllowance.costEnvelopeSha256}, absolute request ceiling ${binding.remainingAllowance.absoluteRequestCeiling}, and USD ${binding.remainingAllowance.hardBudgetCeilingUsd} hard spending ceiling, with ${binding.remainingAllowance.requestsRemaining} requests and USD ${binding.remainingAllowance.hardBudgetRemainingUsd} hard-budget allowance remaining at authorization. No object, CID, key, bucket, prefix, IPNS identity, target, request ceiling, or cost ceiling may change; no IPNS operation is authorized by this amendment. Human authorization reference ${authorizerReference} at ${authorizedAt}.`;
}

export function createCandidateSourceSnapshotUploadContinuationIdentity(inputValue: {
  authorizationBinding: CandidateSourceSnapshotUploadContinuationBinding;
  authorizationStatement?: string;
  authorizedAt: string;
  authorizerReference: string;
}): CandidateSourceSnapshotUploadContinuationAuthorization {
  const input = z
    .strictObject({
      authorizationBinding:
        candidateSourceSnapshotUploadContinuationBindingSchema,
      authorizationStatement: z.string().min(1).max(12_000).optional(),
      authorizedAt: timestampSchema,
      authorizerReference: referenceSchema,
    })
    .parse(inputValue);
  const authorizationStatement =
    renderCandidateSourceSnapshotUploadContinuationStatement(
      input.authorizationBinding,
      input.authorizerReference,
      input.authorizedAt,
    );
  if (
    input.authorizationStatement !== undefined &&
    input.authorizationStatement !== authorizationStatement
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot upload continuation statement is not exact",
    );
  }
  const authorizationBindingSha256 = canonicalJsonSha256(
    input.authorizationBinding,
  );
  const authorizationStatementSha256 = sha256(authorizationStatement);
  const authorizationSha256 = canonicalJsonSha256({
    authorizationBinding: input.authorizationBinding,
    authorizationBindingSha256,
    authorizationStatement,
    authorizationStatementSha256,
    authorizationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_AUTHORIZATION_VERSION,
    authorizedAt: input.authorizedAt,
    authorizerReference: input.authorizerReference,
  });
  return candidateSourceSnapshotUploadContinuationAuthorizationSchema.parse({
    authorizationBinding: input.authorizationBinding,
    authorizationBindingSha256,
    authorizationId: deterministicId("snapshotdemouploadcontinuation", [
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_AUTHORIZATION_VERSION,
      input.authorizationBinding.plan.planId,
      input.authorizationBinding.approval.approvalId,
      authorizationSha256,
    ]),
    authorizationSha256,
    authorizationStatement,
    authorizationStatementSha256,
    authorizationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_AUTHORIZATION_VERSION,
    authorizedAt: input.authorizedAt,
    authorizerReference: input.authorizerReference,
  });
}

interface UploadContinuationSourceRow {
  absolute_request_ceiling: number;
  admitted_object_count: number;
  approval_authorization_statement_sha256: string;
  approval_id: string;
  approval_sha256: string;
  cost_envelope_sha256: string;
  exact_object_count: number;
  exact_total_bytes: string;
  failed_terminal_object_count: number;
  full_inventory_sha256: string;
  hard_budget_ceiling_usd: string;
  hard_budget_remaining_usd: string;
  inventory_cid: string;
  inventory_root_sha256: string;
  original_implementation_commit_sha: string;
  outcome_unknown_object_count: number;
  pending_object_count: number;
  plan_artifact_cid: string;
  plan_artifact_sha256: string;
  plan_id: string;
  plan_revision: number;
  plan_sha256: string;
  predecessor_authorization_id: string | null;
  predecessor_authorization_sha256: string | null;
  predecessor_implementation_commit_sha: string;
  request_envelope_sha256: string;
  requests_remaining: number;
  targets_sha256: string;
  uncertain_object_count: number;
  uncertain_set_sha256: string;
  verified_bytes: string;
  verified_object_count: number;
  verified_receipt_set_sha256: string;
}

async function loadUploadContinuationSource(
  transaction: postgres.TransactionSql,
  input: {
    amendedImplementationCommitSha: string;
    planId: string;
    planSha256: string;
    requestTimeoutMs: number;
  },
): Promise<CandidateSourceSnapshotUploadContinuationBinding> {
  const rows = await transaction<UploadContinuationSourceRow[]>`
    WITH uncertain_rows AS (
      SELECT object.domain, object.remote_object_key,
             CASE WHEN stale.attempt_id IS NOT NULL
               THEN 'stale_request_started'::text
               ELSE 'outcome_unknown'::text
             END AS uncertainty_kind,
             COALESCE(stale.request_id, latest.request_id) AS source_request_id,
             COALESCE(stale.attempt_id, latest.attempt_id) AS source_attempt_id,
             object.expected_sha256, object.expected_cid,
             object.expected_bytes
      FROM oracle_candidate_source_snapshot_demo_objects object
      LEFT JOIN LATERAL (
        SELECT attempt.attempt_id, attempt.request_id
        FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
        WHERE attempt.plan_id = object.plan_id
          AND attempt.domain = object.domain
          AND attempt.remote_object_key = object.remote_object_key
          AND attempt.outcome = 'request_started'
        ORDER BY attempt.attempt_sequence DESC
        LIMIT 1
      ) stale ON true
      LEFT JOIN LATERAL (
        SELECT attempt.attempt_id, attempt.request_id
        FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
        WHERE attempt.plan_id = object.plan_id
          AND attempt.domain = object.domain
          AND attempt.remote_object_key = object.remote_object_key
        ORDER BY attempt.attempt_sequence DESC
        LIMIT 1
      ) latest ON true
      WHERE object.plan_id = ${input.planId}
        AND object.status IN ('admitted', 'outcome_unknown')
        AND (stale.attempt_id IS NOT NULL OR object.status = 'outcome_unknown')
        AND COALESCE(stale.attempt_id, latest.attempt_id) IS NOT NULL
    ), uncertain AS (
      SELECT count(*)::integer AS uncertain_object_count,
             encode(sha256(convert_to(COALESCE(string_agg(
               row.domain || chr(31) ||
               encode(sha256(convert_to(row.remote_object_key, 'UTF8')), 'hex') ||
               chr(31) || row.uncertainty_kind || chr(31) ||
               row.source_request_id || chr(31) || row.source_attempt_id ||
               chr(31) || row.expected_sha256 || chr(31) || row.expected_cid ||
               chr(31) || row.expected_bytes::text,
               chr(30) ORDER BY row.domain, row.remote_object_key
             ), ''), 'UTF8')), 'hex') AS uncertain_set_sha256
      FROM uncertain_rows row
    ), verified AS (
      SELECT encode(sha256(convert_to(COALESCE(string_agg(
               object.domain || chr(31) ||
               encode(sha256(convert_to(object.remote_object_key, 'UTF8')), 'hex') ||
               chr(31) || object.expected_sha256 || chr(31) ||
               object.expected_cid || chr(31) || object.expected_bytes::text ||
               chr(31) || object.provider_cid || chr(31) ||
               object.receipt_sha256,
               chr(30) ORDER BY object.domain, object.remote_object_key
             ), ''), 'UTF8')), 'hex') AS verified_receipt_set_sha256
      FROM oracle_candidate_source_snapshot_demo_objects object
      WHERE object.plan_id = ${input.planId} AND object.status = 'verified'
    )
    SELECT plan.plan_id, plan.plan_sha256, plan.revision AS plan_revision,
           plan.plan_artifact_cid, plan.plan_artifact_sha256,
           plan.inventory_root_cid AS inventory_cid,
           plan.inventory_root_sha256,
           plan.exact_upload_object_count AS exact_object_count,
           plan.exact_upload_bytes AS exact_total_bytes,
           plan.maximum_request_count AS absolute_request_ceiling,
           approval.approval_id, approval.approval_sha256,
           approval.authorization_statement_sha256 AS
             approval_authorization_statement_sha256,
           approval.implementation_commit_sha AS
             original_implementation_commit_sha,
           latest.metadata->>'continuationAuthorizationId' AS
             predecessor_authorization_id,
           continuation.authorization_sha256 AS
             predecessor_authorization_sha256,
           latest.metadata->>'implementationCommitSha' AS
             predecessor_implementation_commit_sha,
           counts.verified_object_count, counts.verified_bytes,
           counts.pending_object_count, counts.admitted_object_count,
           counts.outcome_unknown_object_count,
           counts.failed_terminal_object_count,
           uncertain.uncertain_object_count, uncertain.uncertain_set_sha256,
           verified.verified_receipt_set_sha256,
           plan.plan_payload->'controlArtifacts'->>'fullInventoryRootSha256' AS
             full_inventory_sha256,
           encode(sha256(convert_to(
             oracle_canonical_jsonb(plan.plan_payload->'targets'), 'UTF8'
           )), 'hex') AS targets_sha256,
           encode(sha256(convert_to(
             oracle_canonical_jsonb(plan.request_envelope), 'UTF8'
           )), 'hex') AS request_envelope_sha256,
           encode(sha256(convert_to(
             oracle_canonical_jsonb(plan.cost_envelope), 'UTF8'
           )), 'hex') AS cost_envelope_sha256,
           plan.maximum_request_count - accounting.request_count AS
             requests_remaining,
           to_char(plan.budget_limit_usd,
             'FM999999999999990.000000000000') AS hard_budget_ceiling_usd,
           to_char(plan.budget_limit_usd - accounting.request_cost_usd,
             'FM999999999999990.000000000000') AS hard_budget_remaining_usd
    FROM oracle_candidate_source_snapshot_demo_plans plan
    JOIN oracle_candidate_source_snapshot_demo_approvals approval
      ON approval.plan_id = plan.plan_id
     AND approval.plan_sha256 = plan.plan_sha256
    JOIN oracle_candidate_source_snapshot_demo_accounting accounting
      ON accounting.plan_id = plan.plan_id
    CROSS JOIN uncertain
    CROSS JOIN verified
    JOIN LATERAL (
      SELECT event.metadata
      FROM oracle_candidate_source_snapshot_demo_events event
      WHERE event.plan_id = plan.plan_id
        AND event.event_type = 'execution_started'
      ORDER BY event.recorded_at DESC
      LIMIT 1
    ) latest ON true
    LEFT JOIN oracle_candidate_source_preflight_continuation_authorizations continuation
      ON continuation.authorization_id =
         latest.metadata->>'continuationAuthorizationId'
    JOIN LATERAL (
      SELECT count(*) FILTER (WHERE object.status = 'verified')::integer AS
               verified_object_count,
             COALESCE(sum(object.expected_bytes) FILTER (
               WHERE object.status = 'verified'
             ), 0)::bigint AS verified_bytes,
             count(*) FILTER (WHERE object.status = 'pending')::integer AS
               pending_object_count,
             count(*) FILTER (WHERE object.status = 'admitted')::integer AS
               admitted_object_count,
             count(*) FILTER (WHERE object.status = 'outcome_unknown')::integer AS
               outcome_unknown_object_count,
             count(*) FILTER (WHERE object.status = 'failed_terminal')::integer AS
               failed_terminal_object_count
      FROM oracle_candidate_source_snapshot_demo_objects object
      WHERE object.plan_id = plan.plan_id
    ) counts ON true
    WHERE plan.plan_id = ${input.planId}
      AND plan.plan_sha256 = ${input.planSha256}
      AND plan.plan_version = '2.1.0'
      AND plan.state = 'executing'
      AND approval.approval_version = 'candidate-source-snapshot-approval-v3'
    FOR UPDATE OF plan, accounting
  `;
  const row = rows[0];
  if (!row || !row.predecessor_implementation_commit_sha) {
    throw new DurableConflictError(
      "Candidate source-snapshot upload continuation lacks its exact executing plan",
    );
  }
  return candidateSourceSnapshotUploadContinuationBindingSchema.parse({
    amendedImplementationCommitSha: input.amendedImplementationCommitSha,
    approval: {
      approvalId: row.approval_id,
      approvalSha256: row.approval_sha256,
      authorizationStatementSha256: row.approval_authorization_statement_sha256,
      originalImplementationCommitSha: row.original_implementation_commit_sha,
    },
    checkpoint: {
      admittedObjectCount: row.admitted_object_count,
      failedTerminalObjectCount: row.failed_terminal_object_count,
      outcomeUnknownObjectCount: row.outcome_unknown_object_count,
      pendingObjectCount: row.pending_object_count,
      uncertainObjectCount: row.uncertain_object_count,
      uncertainSetSha256: row.uncertain_set_sha256,
      verifiedBytes: Number(row.verified_bytes),
      verifiedObjectCount: row.verified_object_count,
      verifiedReceiptSetSha256: row.verified_receipt_set_sha256,
    },
    execution: {
      bufferBodyMaxBytes:
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_BUFFER_BODY_MAX_BYTES,
      connectionTimeoutMs:
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONNECTION_TIMEOUT_MS,
      concurrencyStages: [
        ...CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONCURRENCY_STAGES,
      ],
      executorLeaseLimit: 1,
      maxSocketsStages: [
        ...CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONCURRENCY_STAGES,
      ],
      promotionVerifiedObjectsPerStage:
        CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_PROMOTION_VERIFIED_OBJECTS_PER_STAGE,
      reconciliationRequired: true,
      requestTimeoutMs: input.requestTimeoutMs,
      s3Endpoint: CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_S3_ENDPOINT,
      socketTimeoutMs: CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_SOCKET_TIMEOUT_MS,
    },
    inventory: {
      exactObjectCount: row.exact_object_count,
      exactTotalBytes: Number(row.exact_total_bytes),
      fullInventorySha256: row.full_inventory_sha256,
      inventoryCid: row.inventory_cid,
      inventoryRootSha256: row.inventory_root_sha256,
    },
    plan: {
      artifactCid: row.plan_artifact_cid,
      artifactSha256: row.plan_artifact_sha256,
      planId: row.plan_id,
      planRevision: row.plan_revision,
      planSha256: row.plan_sha256,
    },
    predecessor: {
      authorizationId: row.predecessor_authorization_id,
      authorizationSha256: row.predecessor_authorization_sha256,
      implementationCommitSha: row.predecessor_implementation_commit_sha,
    },
    remainingAllowance: {
      absoluteRequestCeiling: row.absolute_request_ceiling,
      costEnvelopeSha256: row.cost_envelope_sha256,
      hardBudgetCeilingUsd: row.hard_budget_ceiling_usd,
      hardBudgetRemainingUsd: row.hard_budget_remaining_usd,
      requestEnvelopeSha256: row.request_envelope_sha256,
      requestsRemaining: row.requests_remaining,
    },
    schemaVersion:
      CANDIDATE_SOURCE_SNAPSHOT_UPLOAD_CONTINUATION_BINDING_VERSION,
    targetsSha256: row.targets_sha256,
  });
}

const proposalInputSchema = z.strictObject({
  amendedImplementationCommitSha: commitShaSchema,
  authorizedAt: timestampSchema,
  authorizerReference: referenceSchema,
  planId: planIdSchema,
  planSha256: sha256Schema,
  requestTimeoutMs: z.number().int().gt(45_000).max(60_000),
});

export async function proposeCandidateSourceSnapshotUploadContinuation(
  databaseUrl: string,
  inputValue: z.input<typeof proposalInputSchema>,
): Promise<CandidateSourceSnapshotUploadContinuationAuthorization> {
  const input = proposalInputSchema.parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) =>
      createCandidateSourceSnapshotUploadContinuationIdentity({
        authorizationBinding: await loadUploadContinuationSource(
          transaction,
          input,
        ),
        authorizedAt: input.authorizedAt,
        authorizerReference: input.authorizerReference,
      }),
    );
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function authorizationMatches(
  left: CandidateSourceSnapshotUploadContinuationAuthorization,
  right: CandidateSourceSnapshotUploadContinuationAuthorization,
): boolean {
  return canonicalJsonSha256(left) === canonicalJsonSha256(right);
}

export async function recordCandidateSourceSnapshotUploadContinuation(
  databaseUrl: string,
  authorizationValue: CandidateSourceSnapshotUploadContinuationAuthorization,
): Promise<CandidateSourceSnapshotUploadContinuationAuthorization> {
  const authorization =
    candidateSourceSnapshotUploadContinuationAuthorizationSchema.parse(
      authorizationValue,
    );
  const binding = authorization.authorizationBinding;
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const existing = await transaction<{ authorization_payload: unknown }[]>`
        SELECT authorization_payload
        FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
        WHERE authorization_id = ${authorization.authorizationId}
           OR plan_id = ${binding.plan.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        const replay =
          candidateSourceSnapshotUploadContinuationAuthorizationSchema.parse(
            existing[0].authorization_payload,
          );
        if (!authorizationMatches(replay, authorization)) {
          throw new DurableConflictError(
            "Candidate source-snapshot upload continuation authorization conflicts",
          );
        }
        return replay;
      }
      const expectedBinding = await loadUploadContinuationSource(transaction, {
        amendedImplementationCommitSha: binding.amendedImplementationCommitSha,
        planId: binding.plan.planId,
        planSha256: binding.plan.planSha256,
        requestTimeoutMs: binding.execution.requestTimeoutMs,
      });
      const expected = createCandidateSourceSnapshotUploadContinuationIdentity({
        authorizationBinding: expectedBinding,
        authorizationStatement: authorization.authorizationStatement,
        authorizedAt: authorization.authorizedAt,
        authorizerReference: authorization.authorizerReference,
      });
      if (!authorizationMatches(expected, authorization)) {
        throw new DurableInputError(
          "Candidate source-snapshot upload continuation authorization is not exact",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_upload_continuation_authorizations (
          authorization_id, authorization_version, authorization_sha256,
          plan_id, plan_sha256, plan_revision, approval_id, approval_sha256,
          predecessor_authorization_id, predecessor_authorization_sha256,
          predecessor_implementation_commit_sha,
          amended_implementation_commit_sha, s3_endpoint,
          connection_timeout_ms, socket_timeout_ms, request_timeout_ms,
          buffer_body_max_bytes, authorization_binding,
          authorization_binding_sha256, authorization_statement,
          authorization_statement_sha256, authorizer_reference, authorized_at,
          authorized_at_iso, authorization_payload
        ) VALUES (
          ${authorization.authorizationId},
          ${authorization.authorizationVersion},
          ${authorization.authorizationSha256}, ${binding.plan.planId},
          ${binding.plan.planSha256}, ${binding.plan.planRevision},
          ${binding.approval.approvalId}, ${binding.approval.approvalSha256},
          ${binding.predecessor.authorizationId},
          ${binding.predecessor.authorizationSha256},
          ${binding.predecessor.implementationCommitSha},
          ${binding.amendedImplementationCommitSha},
          ${binding.execution.s3Endpoint},
          ${binding.execution.connectionTimeoutMs},
          ${binding.execution.socketTimeoutMs},
          ${binding.execution.requestTimeoutMs},
          ${binding.execution.bufferBodyMaxBytes},
          ${transaction.json(binding as postgres.JSONValue)},
          ${authorization.authorizationBindingSha256},
          ${authorization.authorizationStatement},
          ${authorization.authorizationStatementSha256},
          ${authorization.authorizerReference}, ${authorization.authorizedAt},
          ${authorization.authorizedAt},
          ${transaction.json(authorization as postgres.JSONValue)}
        )
      `;
      return authorization;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

interface UncertaintyRow {
  authorization_id: string;
  domain: "open_data" | "query_table";
  expected_bytes: string;
  expected_cid: string;
  expected_sha256: string;
  plan_id: string;
  remote_object_key: string;
  source_attempt_id: string;
  source_request_id: string;
  uncertainty_kind: "outcome_unknown" | "stale_request_started";
}

export async function listCandidateSourceSnapshotUploadContinuationUncertainties(
  databaseUrl: string,
  authorizationIdValue: string,
): Promise<readonly CandidateSourceSnapshotUploadContinuationUncertainty[]> {
  const authorizationId = authorizationIdSchema.parse(authorizationIdValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<UncertaintyRow[]>`
      SELECT authorization_id, plan_id, domain, remote_object_key,
             uncertainty_kind, source_request_id, source_attempt_id,
             expected_sha256, expected_cid, expected_bytes
      FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties
      WHERE authorization_id = ${authorizationId}
      ORDER BY domain, remote_object_key
    `;
    return rows.map((row) =>
      candidateSourceSnapshotUploadContinuationUncertaintySchema.parse({
        authorizationId: row.authorization_id,
        domain: row.domain,
        expectedBytes: Number(row.expected_bytes),
        expectedCid: row.expected_cid,
        expectedSha256: row.expected_sha256,
        planId: row.plan_id,
        remoteObjectKey: row.remote_object_key,
        sourceAttemptId: row.source_attempt_id,
        sourceRequestId: row.source_request_id,
        uncertaintyKind: row.uncertainty_kind,
      }),
    );
  } finally {
    await sql.end({ timeout: 5 });
  }
}

interface LeaseRow {
  acquired_at: Date;
  authorization_id: string;
  effective_concurrency: 0 | 4 | 8 | 16;
  expires_at: Date;
  heartbeat_at: Date;
  holder_token_sha256: string;
  lease_epoch: 1;
  lease_id: string;
  lease_version: typeof CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_VERSION;
  phase: z.infer<typeof leasePhaseSchema>;
  plan_id: string;
  revision: number;
}

function leaseFromRow(row: LeaseRow): CandidateSourceSnapshotExecutorLease {
  return candidateSourceSnapshotExecutorLeaseSchema.parse({
    acquiredAt: row.acquired_at.toISOString(),
    authorizationId: row.authorization_id,
    effectiveConcurrency: row.effective_concurrency,
    expiresAt: row.expires_at.toISOString(),
    heartbeatAt: row.heartbeat_at.toISOString(),
    holderTokenSha256: row.holder_token_sha256,
    leaseEpoch: row.lease_epoch,
    leaseId: row.lease_id,
    leaseVersion: row.lease_version,
    phase: row.phase,
    planId: row.plan_id,
    revision: row.revision,
  });
}

const leaseTimingSchema = z
  .strictObject({
    acquiredAt: timestampSchema,
    expiresAt: timestampSchema,
  })
  .superRefine((value, context) => {
    const duration =
      new Date(value.expiresAt).getTime() -
      new Date(value.acquiredAt).getTime();
    if (duration <= 0 || duration > 300_000) {
      context.addIssue({
        code: "custom",
        message:
          "executor lease duration must be positive and at most five minutes",
        path: ["expiresAt"],
      });
    }
  });

export async function acquireCandidateSourceSnapshotExecutorLease(
  databaseUrl: string,
  inputValue: {
    acquiredAt: string;
    authorizationId: string;
    expiresAt: string;
    holderToken: string;
  },
): Promise<CandidateSourceSnapshotExecutorLease> {
  const timing = leaseTimingSchema.parse({
    acquiredAt: inputValue.acquiredAt,
    expiresAt: inputValue.expiresAt,
  });
  const input = z
    .strictObject({
      authorizationId: authorizationIdSchema,
      holderToken: z.string().min(32).max(512),
    })
    .parse({
      authorizationId: inputValue.authorizationId,
      holderToken: inputValue.holderToken,
    });
  const holderTokenSha256 = sha256(input.holderToken);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const authorization = await transaction<
        { authorization_id: string; plan_id: string }[]
      >`
        SELECT authorization_id, plan_id
        FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
        WHERE authorization_id = ${input.authorizationId}
        FOR SHARE
      `;
      if (!authorization[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot executor lease lacks its authorization",
        );
      }
      const leaseId = deterministicId("snapshotdemoexecutorlease", [
        CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_VERSION,
        authorization[0].plan_id,
        input.authorizationId,
      ]);
      const existing = await transaction<
        (LeaseRow & { is_expired: boolean })[]
      >`
        SELECT *, expires_at <= now() AS is_expired
        FROM oracle_candidate_source_snapshot_executor_leases
        WHERE authorization_id = ${input.authorizationId}
           OR plan_id = ${authorization[0].plan_id}
        FOR UPDATE
      `;
      if (existing[0]) {
        const replay = leaseFromRow(existing[0]);
        if (
          replay.leaseId !== leaseId ||
          replay.holderTokenSha256 !== holderTokenSha256 ||
          replay.phase === "released"
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot executor lease is already owned",
          );
        }
        if (existing[0].is_expired) {
          const resumed = await transaction<LeaseRow[]>`
            UPDATE oracle_candidate_source_snapshot_executor_leases
            SET heartbeat_at = ${timing.acquiredAt},
                expires_at = ${timing.expiresAt},
                revision = revision + 1
            WHERE lease_id = ${leaseId}
              AND holder_token_sha256 = ${holderTokenSha256}
              AND phase <> 'released'
              AND expires_at <= now()
              AND revision = ${existing[0].revision}
            RETURNING *
          `;
          if (!resumed[0]) {
            throw new DurableConflictError(
              "Candidate source-snapshot executor lease resume lost ownership",
            );
          }
          return leaseFromRow(resumed[0]);
        }
        return replay;
      }
      const inserted = await transaction<LeaseRow[]>`
        INSERT INTO oracle_candidate_source_snapshot_executor_leases (
          lease_id, lease_version, authorization_id, plan_id,
          holder_token_sha256, lease_epoch, phase, effective_concurrency,
          acquired_at, heartbeat_at, expires_at, revision
        ) VALUES (
          ${leaseId}, ${CANDIDATE_SOURCE_SNAPSHOT_EXECUTOR_LEASE_VERSION},
          ${input.authorizationId}, ${authorization[0].plan_id},
          ${holderTokenSha256}, 1, 'reconciling', 0,
          ${timing.acquiredAt}, ${timing.acquiredAt}, ${timing.expiresAt}, 1
        ) RETURNING *
      `;
      return leaseFromRow(inserted[0]!);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const phaseConcurrency = {
  reconciling: 0,
  released: 0,
  upload_16: 16,
  upload_4: 4,
  upload_8: 8,
} as const;

export async function transitionCandidateSourceSnapshotExecutorLease(
  databaseUrl: string,
  inputValue: {
    expiresAt: string;
    heartbeatAt: string;
    holderToken: string;
    leaseId: string;
    nextPhase: z.infer<typeof leasePhaseSchema>;
    revision: number;
  },
): Promise<CandidateSourceSnapshotExecutorLease> {
  const input = z
    .strictObject({
      expiresAt: timestampSchema,
      heartbeatAt: timestampSchema,
      holderToken: z.string().min(32).max(512),
      leaseId: leaseIdSchema,
      nextPhase: leasePhaseSchema,
      revision: z.number().int().positive(),
    })
    .superRefine((value, context) => {
      const duration =
        new Date(value.expiresAt).getTime() -
        new Date(value.heartbeatAt).getTime();
      if (duration <= 0 || duration > 300_000) {
        context.addIssue({
          code: "custom",
          message: "executor lease extension must be positive and bounded",
          path: ["expiresAt"],
        });
      }
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const updated = await sql<LeaseRow[]>`
      UPDATE oracle_candidate_source_snapshot_executor_leases
      SET phase = ${input.nextPhase},
          effective_concurrency = ${phaseConcurrency[input.nextPhase]},
          heartbeat_at = ${input.heartbeatAt}, expires_at = ${input.expiresAt},
          revision = revision + 1
      WHERE lease_id = ${input.leaseId}
        AND holder_token_sha256 = ${sha256(input.holderToken)}
        AND revision = ${input.revision}
      RETURNING *
    `;
    if (!updated[0]) {
      throw new DurableConflictError(
        "Candidate source-snapshot executor lease transition lost ownership",
      );
    }
    return leaseFromRow(updated[0]);
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function heartbeatCandidateSourceSnapshotExecutorLease(
  databaseUrl: string,
  inputValue: {
    expiresAt: string;
    heartbeatAt: string;
    holderToken: string;
    leaseId: string;
  },
): Promise<CandidateSourceSnapshotExecutorLease> {
  const input = z
    .strictObject({
      expiresAt: timestampSchema,
      heartbeatAt: timestampSchema,
      holderToken: z.string().min(32).max(512),
      leaseId: leaseIdSchema,
    })
    .superRefine((value, context) => {
      const duration =
        new Date(value.expiresAt).getTime() -
        new Date(value.heartbeatAt).getTime();
      if (duration <= 0 || duration > 300_000) {
        context.addIssue({
          code: "custom",
          message: "executor lease heartbeat must be positive and bounded",
          path: ["expiresAt"],
        });
      }
    })
    .parse(inputValue);
  const holderTokenSha256 = sha256(input.holderToken);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const current = await transaction<LeaseRow[]>`
        SELECT * FROM oracle_candidate_source_snapshot_executor_leases
        WHERE lease_id = ${input.leaseId}
          AND phase <> 'released' AND expires_at > now()
        FOR UPDATE
      `;
      if (!current[0] || current[0].holder_token_sha256 !== holderTokenSha256) {
        throw new DurableConflictError(
          "Candidate source-snapshot executor lease heartbeat lacks unexpired ownership",
        );
      }
      const updated = await transaction<LeaseRow[]>`
        UPDATE oracle_candidate_source_snapshot_executor_leases
        SET heartbeat_at = ${input.heartbeatAt}, expires_at = ${input.expiresAt},
            revision = revision + 1
        WHERE lease_id = ${input.leaseId}
          AND holder_token_sha256 = ${holderTokenSha256}
          AND revision = ${current[0].revision}
        RETURNING *
      `;
      if (!updated[0]) {
        throw new DurableConflictError(
          "Candidate source-snapshot executor lease heartbeat lost ownership",
        );
      }
      return leaseFromRow(updated[0]);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function releaseCandidateSourceSnapshotExecutorLease(
  databaseUrl: string,
  inputValue: Omit<
    Parameters<typeof transitionCandidateSourceSnapshotExecutorLease>[1],
    "nextPhase"
  >,
): Promise<CandidateSourceSnapshotExecutorLease> {
  return transitionCandidateSourceSnapshotExecutorLease(databaseUrl, {
    ...inputValue,
    nextPhase: "released",
  });
}

export async function candidateSourceSnapshotUploadReconciliationComplete(
  databaseUrl: string,
  authorizationIdValue: string,
): Promise<boolean> {
  const authorizationId = authorizationIdSchema.parse(authorizationIdValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<{ complete: boolean }[]>`
      SELECT oracle_css_upload_continuation_is_reconciled(
        ${authorizationId}
      ) AS complete
    `;
    return rows[0]?.complete === true;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function recordCandidateSourceSnapshotUploadReconciliation(
  databaseUrl: string,
  inputValue: {
    authorizationId: string;
    domain: "open_data" | "query_table";
    executorLeaseId: string;
    holderToken: string;
    inspectionId: string;
    planId: string;
    receiptSha256: string;
    recordedAt: string;
    remoteObjectKey: string;
    result: "conclusively_absent" | "remote_verified";
  },
): Promise<void> {
  const input = z
    .strictObject({
      authorizationId: authorizationIdSchema,
      domain: domainSchema,
      executorLeaseId: leaseIdSchema,
      holderToken: z.string().min(32).max(512),
      inspectionId: z.string().regex(/^snapshotdemoinspection_[a-f0-9]{32}$/),
      planId: planIdSchema,
      receiptSha256: sha256Schema,
      recordedAt: timestampSchema,
      remoteObjectKey: z.string().min(1),
      result: z.enum(["conclusively_absent", "remote_verified"]),
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.begin(async (transaction) => {
      const lease = await transaction<LeaseRow[]>`
        SELECT * FROM oracle_candidate_source_snapshot_executor_leases
        WHERE lease_id = ${input.executorLeaseId}
        FOR UPDATE
      `;
      if (
        !lease[0] ||
        lease[0].holder_token_sha256 !== sha256(input.holderToken) ||
        lease[0].phase !== "reconciling"
      ) {
        throw new DurableConflictError(
          "Candidate source-snapshot reconciliation lacks its executor lease",
        );
      }
      const existing = await transaction<
        {
          inspection_id: string;
          receipt_sha256: string;
          result: "conclusively_absent" | "remote_verified";
        }[]
      >`
        SELECT inspection_id, result, receipt_sha256
        FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations
        WHERE authorization_id = ${input.authorizationId}
          AND domain = ${input.domain}
          AND remote_object_key = ${input.remoteObjectKey}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].inspection_id !== input.inspectionId ||
          existing[0].result !== input.result ||
          existing[0].receipt_sha256 !== input.receiptSha256
        ) {
          throw new DurableConflictError(
            "Candidate source-snapshot reconciliation conflicts",
          );
        }
        return;
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_upload_continuation_reconciliations (
          authorization_id, plan_id, domain, remote_object_key,
          executor_lease_id, executor_lease_epoch, inspection_id, result,
          receipt_sha256, recorded_at
        ) VALUES (
          ${input.authorizationId}, ${input.planId}, ${input.domain},
          ${input.remoteObjectKey}, ${input.executorLeaseId}, 1,
          ${input.inspectionId}, ${input.result}, ${input.receiptSha256},
          ${input.recordedAt}
        )
      `;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadCandidateSourceSnapshotUploadExecutionPermit(
  databaseUrl: string,
  inputValue: {
    holderToken: string;
    leaseId: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotUploadExecutionPermit> {
  const input = z
    .strictObject({
      holderToken: z.string().min(32).max(512),
      leaseId: leaseIdSchema,
      planId: planIdSchema,
      planSha256: sha256Schema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<
      (LeaseRow & {
        authorization_binding: unknown;
        authorization_sha256: string;
        plan_sha256: string;
        reconciliation_complete: boolean;
      })[]
    >`
      SELECT lease.*, auth.authorization_binding,
             auth.authorization_sha256, plan.plan_sha256,
             oracle_css_upload_continuation_is_reconciled(
               auth.authorization_id
             ) AS reconciliation_complete
      FROM oracle_candidate_source_snapshot_executor_leases lease
      JOIN oracle_candidate_source_snapshot_upload_continuation_authorizations auth
        ON auth.authorization_id = lease.authorization_id
      JOIN oracle_candidate_source_snapshot_demo_plans plan
        ON plan.plan_id = lease.plan_id
      WHERE lease.lease_id = ${input.leaseId}
        AND lease.plan_id = ${input.planId}
        AND plan.plan_sha256 = ${input.planSha256}
        AND lease.holder_token_sha256 = ${sha256(input.holderToken)}
        AND lease.phase IN ('upload_4', 'upload_8', 'upload_16')
        AND lease.expires_at > now()
      FOR SHARE OF lease, auth, plan
    `;
    const row = rows[0];
    if (!row || !row.reconciliation_complete) {
      throw new DurableConflictError(
        "Candidate source-snapshot upload execution permit is not ready",
      );
    }
    const binding =
      candidateSourceSnapshotUploadContinuationBindingSchema.parse(
        row.authorization_binding,
      );
    return candidateSourceSnapshotUploadExecutionPermitSchema.parse({
      authorizationId: row.authorization_id,
      authorizationSha256: row.authorization_sha256,
      bufferBodyMaxBytes: binding.execution.bufferBodyMaxBytes,
      connectionTimeoutMs: binding.execution.connectionTimeoutMs,
      effectiveConcurrency: row.effective_concurrency,
      executorLeaseId: row.lease_id,
      leaseEpoch: row.lease_epoch,
      leaseRevision: row.revision,
      maxSockets: row.effective_concurrency,
      phase: row.phase,
      planId: row.plan_id,
      planSha256: row.plan_sha256,
      reconciliationComplete: true,
      requestTimeoutMs: binding.execution.requestTimeoutMs,
      s3Endpoint: binding.execution.s3Endpoint,
      socketTimeoutMs: binding.execution.socketTimeoutMs,
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
