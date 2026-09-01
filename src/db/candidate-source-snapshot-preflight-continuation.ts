import postgres from "postgres";
import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";

export const CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_BINDING_VERSION =
  "candidate-source-snapshot-preflight-continuation-binding-v1" as const;
export const CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION =
  "candidate-source-snapshot-preflight-continuation-authorization-v1" as const;

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const commitShaSchema = z.string().regex(/^[a-f0-9]{40}$/);
const timestampSchema = z
  .string()
  .regex(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)
  .refine((value) => new Date(value).toISOString() === value);
const referenceSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const priorCidSchema = z.union([
  cidSchema,
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);

export const candidateSourceSnapshotPreflightContinuationBindingSchema = z
  .strictObject({
    amendedImplementationCommitSha: commitShaSchema,
    approval: z.strictObject({
      approvalId: z.string().regex(/^snapshotdemoapproval_[a-f0-9]{32}$/),
      approvalSha256: sha256Schema,
      approvedPlanRevision: z.number().int().positive(),
      authorizationStatementSha256: sha256Schema,
      originalImplementationCommitSha: commitShaSchema,
    }),
    authorizedObservation: z.strictObject({
      authorizedAttemptSequence: z.number().int().min(2).max(3),
      authorizedOperation: z.literal("official_filebase_gateway_resolution"),
      domain: z.literal("open_data"),
      expectedPriorCid: priorCidSchema,
      expectedTargetCid: cidSchema,
      ipnsNetworkKey: z.string().regex(/^k51[0-9a-z]{59}$/),
      maximumNewLogicalObservations: z.literal(1),
      resolver: z.literal("filebase_gateway"),
      resolverPolicy: z.literal(
        "candidate_source_snapshot_filebase_delegated_v1",
      ),
      storedOperationKind: z.literal("public_resolve"),
    }),
    failedReceipt: z.strictObject({
      attemptSequence: z.number().int().min(1).max(2),
      outcome: z.literal("terminal_failure"),
      receiptSha256: sha256Schema,
      redirectSequence: z.number().int().min(0).max(2),
      requestId: z.string().regex(/^snapshotdemorequest_[a-f0-9]{32}$/),
    }),
    plan: z.strictObject({
      artifactCid: cidSchema,
      artifactSha256: sha256Schema,
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planRevision: z.number().int().positive(),
      planSha256: sha256Schema,
    }),
    remainingAllowance: z.strictObject({
      costEnvelopeSha256: sha256Schema,
      hardBudgetUsd: z.string().regex(/^[0-9]+\.[0-9]{12}$/),
      preflightRequests: z.number().int().nonnegative(),
      requestCostUsd: z.string().regex(/^[0-9]+\.[0-9]{12}$/),
      requestEnvelopeSha256: sha256Schema,
      totalRequests: z.number().int().nonnegative(),
    }),
    schemaVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_BINDING_VERSION,
    ),
  })
  .superRefine((value, context) => {
    if (
      value.amendedImplementationCommitSha ===
      value.approval.originalImplementationCommitSha
    ) {
      context.addIssue({
        code: "custom",
        message:
          "amended implementation commit must differ from approval commit",
        path: ["amendedImplementationCommitSha"],
      });
    }
    if (
      value.authorizedObservation.authorizedAttemptSequence !==
      value.failedReceipt.attemptSequence + 1
    ) {
      context.addIssue({
        code: "custom",
        message:
          "continuation attempt must immediately follow the failed receipt",
        path: ["authorizedObservation", "authorizedAttemptSequence"],
      });
    }
  });

export type CandidateSourceSnapshotPreflightContinuationBinding = z.infer<
  typeof candidateSourceSnapshotPreflightContinuationBindingSchema
>;

export const candidateSourceSnapshotPreflightContinuationAuthorizationSchema =
  z.strictObject({
    authorizationBinding:
      candidateSourceSnapshotPreflightContinuationBindingSchema,
    authorizationBindingSha256: sha256Schema,
    authorizationId: z
      .string()
      .regex(/^snapshotdemocontinuation_[a-f0-9]{32}$/),
    authorizationSha256: sha256Schema,
    authorizationStatement: z.string().min(1).max(8_192),
    authorizationStatementSha256: sha256Schema,
    authorizationVersion: z.literal(
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
    ),
    authorizedAt: timestampSchema,
    authorizerReference: referenceSchema,
  });

export type CandidateSourceSnapshotPreflightContinuationAuthorization = z.infer<
  typeof candidateSourceSnapshotPreflightContinuationAuthorizationSchema
>;

export function renderCandidateSourceSnapshotPreflightContinuationStatement(
  bindingValue: CandidateSourceSnapshotPreflightContinuationBinding,
  authorizerReferenceValue: string,
  authorizedAtValue: string,
): string {
  const binding =
    candidateSourceSnapshotPreflightContinuationBindingSchema.parse(
      bindingValue,
    );
  const authorizerReference = referenceSchema.parse(authorizerReferenceValue);
  const authorizedAt = timestampSchema.parse(authorizedAtValue);
  return `I authorize exactly one resumable candidate-owned source-snapshot preflight continuation for plan ${binding.plan.planId}, logical SHA-256 ${binding.plan.planSha256}, at durable plan revision ${binding.plan.planRevision}, under unchanged primary approval ${binding.approval.approvalId}, approval SHA-256 ${binding.approval.approvalSha256}, and primary authorization-statement SHA-256 ${binding.approval.authorizationStatementSha256}; the primary approval's original implementation commit remains ${binding.approval.originalImplementationCommitSha}, and only amended implementation commit ${binding.amendedImplementationCommitSha} may execute this continuation. This continuation is bound to immutable failed request ${binding.failedReceipt.requestId} with receipt SHA-256 ${binding.failedReceipt.receiptSha256}, outcome ${binding.failedReceipt.outcome}, attempt ${binding.failedReceipt.attemptSequence}, redirect ${binding.failedReceipt.redirectSequence}, operation ${binding.authorizedObservation.authorizedOperation} stored as ${binding.authorizedObservation.domain}/${binding.authorizedObservation.storedOperationKind}/${binding.authorizedObservation.resolver}, network key ${binding.authorizedObservation.ipnsNetworkKey}, immutable prior ${binding.authorizedObservation.expectedPriorCid}, approved target ${binding.authorizedObservation.expectedTargetCid}, resolver policy ${binding.authorizedObservation.resolverPolicy}, and at most ${binding.authorizedObservation.maximumNewLogicalObservations} new logical observation at attempt ${binding.authorizedObservation.authorizedAttemptSequence}. It preserves plan artifact CID ${binding.plan.artifactCid} and SHA-256 ${binding.plan.artifactSha256}, request-envelope SHA-256 ${binding.remainingAllowance.requestEnvelopeSha256}, and cost-envelope SHA-256 ${binding.remainingAllowance.costEnvelopeSha256}, with ${binding.remainingAllowance.preflightRequests} bucket-names-preflight requests, ${binding.remainingAllowance.totalRequests} total requests, USD ${binding.remainingAllowance.requestCostUsd} request-cost allowance, and USD ${binding.remainingAllowance.hardBudgetUsd} hard-budget allowance remaining at authorization. The primary approval remains unchanged and every existing receipt remains immutable; this continuation authorizes code-continuation compatibility and only that specified recovery observation, not a different plan, target, resolver policy, artifact, upload, IPNS mutation, rollback, Vercel deployment, owner/canonical publication, or authoritative-complete claim. If the specified observation succeeds, the remaining publication operations already authorized by the unchanged primary approval may continue through amended implementation commit ${binding.amendedImplementationCommitSha}; otherwise execution remains stopped fail-closed. Human authorization reference ${authorizerReference} at ${authorizedAt}.`;
}

export function createCandidateSourceSnapshotPreflightContinuationIdentity(inputValue: {
  authorizationBinding: CandidateSourceSnapshotPreflightContinuationBinding;
  authorizationStatement?: string;
  authorizedAt: string;
  authorizerReference: string;
}): CandidateSourceSnapshotPreflightContinuationAuthorization {
  const input = z
    .strictObject({
      authorizationBinding:
        candidateSourceSnapshotPreflightContinuationBindingSchema,
      authorizationStatement: z.string().min(1).max(8_192).optional(),
      authorizedAt: timestampSchema,
      authorizerReference: referenceSchema,
    })
    .parse(inputValue);
  const authorizationStatement =
    renderCandidateSourceSnapshotPreflightContinuationStatement(
      input.authorizationBinding,
      input.authorizerReference,
      input.authorizedAt,
    );
  if (
    input.authorizationStatement !== undefined &&
    input.authorizationStatement !== authorizationStatement
  ) {
    throw new DurableInputError(
      "Candidate source-snapshot continuation statement is not exact",
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
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
    authorizedAt: input.authorizedAt,
    authorizerReference: input.authorizerReference,
  });
  return candidateSourceSnapshotPreflightContinuationAuthorizationSchema.parse({
    authorizationBinding: input.authorizationBinding,
    authorizationBindingSha256,
    authorizationId: deterministicId("snapshotdemocontinuation", [
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
      input.authorizationBinding.plan.planId,
      input.authorizationBinding.approval.approvalId,
      input.authorizationBinding.failedReceipt.requestId,
      authorizationSha256,
    ]),
    authorizationSha256,
    authorizationStatement,
    authorizationStatementSha256,
    authorizationVersion:
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_AUTHORIZATION_VERSION,
    authorizedAt: input.authorizedAt,
    authorizerReference: input.authorizerReference,
  });
}

interface ContinuationSourceRow {
  approval_authorization_statement_sha256: string;
  approval_id: string;
  approval_sha256: string;
  approved_plan_revision: number;
  cost_envelope: unknown;
  failed_attempt_sequence: number;
  failed_domain: "open_data";
  failed_operation_kind: "public_resolve";
  failed_outcome: "terminal_failure";
  failed_receipt_sha256: string;
  failed_redirect_sequence: number;
  failed_request_id: string;
  failed_resolver: "filebase_gateway";
  hard_budget_remaining_usd: string;
  ipns_network_key: string;
  original_implementation_commit_sha: string;
  plan_artifact_cid: string;
  plan_artifact_sha256: string;
  plan_id: string;
  plan_revision: number;
  plan_sha256: string;
  preflight_requests_remaining: number;
  prior_cid: string;
  request_cost_remaining_usd: string;
  request_envelope: unknown;
  target_cid: string;
  total_requests_remaining: number;
}

async function loadContinuationSource(
  transaction: postgres.TransactionSql,
  input: {
    failedRequestId: string;
    planId: string;
    planSha256: string;
  },
): Promise<ContinuationSourceRow> {
  const rows = await transaction<ContinuationSourceRow[]>`
    SELECT plan.plan_id, plan.plan_sha256, plan.revision AS plan_revision,
           plan.plan_artifact_cid, plan.plan_artifact_sha256,
           plan.request_envelope, plan.cost_envelope,
           approval.approval_id, approval.approval_sha256,
           approval.authorization_statement_sha256 AS
             approval_authorization_statement_sha256,
           approval.approved_plan_revision,
           approval.implementation_commit_sha AS
             original_implementation_commit_sha,
           failed.request_id AS failed_request_id,
           failed.receipt_sha256 AS failed_receipt_sha256,
           failed.outcome AS failed_outcome,
           failed.attempt_sequence AS failed_attempt_sequence,
           failed.redirect_sequence AS failed_redirect_sequence,
           failed.domain AS failed_domain,
           failed.operation_kind AS failed_operation_kind,
           failed.resolver AS failed_resolver,
           plan.plan_payload->'targets'->'openData'->>'ipnsNetworkKey' AS
             ipns_network_key,
           plan.plan_payload->'targets'->'openData'->>'priorCid' AS prior_cid,
           plan.plan_payload->'targets'->'openData'->>'targetCid' AS target_cid,
           category.planned_maximum_request_count -
             category.consumed_request_count AS preflight_requests_remaining,
           plan.maximum_request_count - accounting.request_count AS
             total_requests_remaining,
           to_char(
             (plan.cost_envelope->'requestUsd'->>'maximumAttempts')::numeric -
               accounting.request_cost_usd,
             'FM999999999999990.000000000000'
           ) AS request_cost_remaining_usd,
           to_char(
             plan.budget_limit_usd - accounting.request_cost_usd,
             'FM999999999999990.000000000000'
           ) AS hard_budget_remaining_usd
    FROM oracle_candidate_source_snapshot_demo_plans plan
    JOIN oracle_candidate_source_snapshot_demo_approvals approval
      ON approval.plan_id = plan.plan_id
     AND approval.plan_sha256 = plan.plan_sha256
    JOIN oracle_candidate_source_snapshot_demo_requests failed
      ON failed.plan_id = plan.plan_id
    JOIN oracle_candidate_source_snapshot_demo_request_categories category
      ON category.plan_id = plan.plan_id
     AND category.request_category = 'bucket_names_preflight'
    JOIN oracle_candidate_source_snapshot_demo_accounting accounting
      ON accounting.plan_id = plan.plan_id
    WHERE plan.plan_id = ${input.planId}
      AND plan.plan_sha256 = ${input.planSha256}
      AND plan.state = 'approved'
      AND approval.approval_version =
        'candidate-source-snapshot-approval-v3'
      AND failed.request_id = ${input.failedRequestId}
      AND failed.request_category = 'bucket_names_preflight'
      AND failed.domain = 'open_data'
      AND failed.operation_kind = 'public_resolve'
      AND failed.resolver = 'filebase_gateway'
      AND failed.outcome = 'terminal_failure'
      AND failed.receipt_sha256 IS NOT NULL
      AND approval.approved_at <= failed.started_at
    FOR UPDATE OF plan, category, accounting
  `;
  if (rows.length !== 1) {
    throw new DurableConflictError(
      "Candidate source-snapshot continuation lacks its exact approved failed receipt",
    );
  }
  return rows[0]!;
}

function bindingFromSource(
  row: ContinuationSourceRow,
  amendedImplementationCommitSha: string,
): CandidateSourceSnapshotPreflightContinuationBinding {
  return candidateSourceSnapshotPreflightContinuationBindingSchema.parse({
    amendedImplementationCommitSha,
    approval: {
      approvalId: row.approval_id,
      approvalSha256: row.approval_sha256,
      approvedPlanRevision: row.approved_plan_revision,
      authorizationStatementSha256: row.approval_authorization_statement_sha256,
      originalImplementationCommitSha: row.original_implementation_commit_sha,
    },
    authorizedObservation: {
      authorizedAttemptSequence: row.failed_attempt_sequence + 1,
      authorizedOperation: "official_filebase_gateway_resolution",
      domain: row.failed_domain,
      expectedPriorCid: row.prior_cid,
      expectedTargetCid: row.target_cid,
      ipnsNetworkKey: row.ipns_network_key,
      maximumNewLogicalObservations: 1,
      resolver: row.failed_resolver,
      resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
      storedOperationKind: row.failed_operation_kind,
    },
    failedReceipt: {
      attemptSequence: row.failed_attempt_sequence,
      outcome: row.failed_outcome,
      receiptSha256: row.failed_receipt_sha256,
      redirectSequence: row.failed_redirect_sequence,
      requestId: row.failed_request_id,
    },
    plan: {
      artifactCid: row.plan_artifact_cid,
      artifactSha256: row.plan_artifact_sha256,
      planId: row.plan_id,
      planRevision: row.plan_revision,
      planSha256: row.plan_sha256,
    },
    remainingAllowance: {
      costEnvelopeSha256: canonicalJsonSha256(row.cost_envelope),
      hardBudgetUsd: row.hard_budget_remaining_usd,
      preflightRequests: row.preflight_requests_remaining,
      requestCostUsd: row.request_cost_remaining_usd,
      requestEnvelopeSha256: canonicalJsonSha256(row.request_envelope),
      totalRequests: row.total_requests_remaining,
    },
    schemaVersion:
      CANDIDATE_SOURCE_SNAPSHOT_PREFLIGHT_CONTINUATION_BINDING_VERSION,
  });
}

export async function proposeCandidateSourceSnapshotPreflightContinuation(
  databaseUrl: string,
  inputValue: {
    amendedImplementationCommitSha: string;
    authorizedAt: string;
    authorizerReference: string;
    failedRequestId: string;
    planId: string;
    planSha256: string;
  },
): Promise<CandidateSourceSnapshotPreflightContinuationAuthorization> {
  const input = z
    .strictObject({
      amendedImplementationCommitSha: commitShaSchema,
      authorizedAt: timestampSchema,
      authorizerReference: referenceSchema,
      failedRequestId: z.string().regex(/^snapshotdemorequest_[a-f0-9]{32}$/),
      planId: z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/),
      planSha256: sha256Schema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const source = await loadContinuationSource(transaction, input);
      return createCandidateSourceSnapshotPreflightContinuationIdentity({
        authorizationBinding: bindingFromSource(
          source,
          input.amendedImplementationCommitSha,
        ),
        authorizedAt: input.authorizedAt,
        authorizerReference: input.authorizerReference,
      });
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function authorizationMatches(
  left: CandidateSourceSnapshotPreflightContinuationAuthorization,
  right: CandidateSourceSnapshotPreflightContinuationAuthorization,
): boolean {
  return canonicalJsonSha256(left) === canonicalJsonSha256(right);
}

export async function recordCandidateSourceSnapshotPreflightContinuation(
  databaseUrl: string,
  authorizationValue: CandidateSourceSnapshotPreflightContinuationAuthorization,
): Promise<CandidateSourceSnapshotPreflightContinuationAuthorization> {
  const authorization =
    candidateSourceSnapshotPreflightContinuationAuthorizationSchema.parse(
      authorizationValue,
    );
  const binding = authorization.authorizationBinding;
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const existing = await transaction<{ authorization_payload: unknown }[]>`
        SELECT authorization_payload
        FROM oracle_candidate_source_preflight_continuation_authorizations
        WHERE authorization_id = ${authorization.authorizationId}
           OR failed_request_id = ${binding.failedReceipt.requestId}
        FOR UPDATE
      `;
      if (existing[0]) {
        const replay =
          candidateSourceSnapshotPreflightContinuationAuthorizationSchema.parse(
            existing[0].authorization_payload,
          );
        if (!authorizationMatches(replay, authorization)) {
          throw new DurableConflictError(
            "Candidate source-snapshot continuation authorization conflicts",
          );
        }
        return replay;
      }
      const source = await loadContinuationSource(transaction, {
        failedRequestId: binding.failedReceipt.requestId,
        planId: binding.plan.planId,
        planSha256: binding.plan.planSha256,
      });
      const expected =
        createCandidateSourceSnapshotPreflightContinuationIdentity({
          authorizationBinding: bindingFromSource(
            source,
            binding.amendedImplementationCommitSha,
          ),
          authorizationStatement: authorization.authorizationStatement,
          authorizedAt: authorization.authorizedAt,
          authorizerReference: authorization.authorizerReference,
        });
      if (!authorizationMatches(expected, authorization)) {
        throw new DurableInputError(
          "Candidate source-snapshot continuation authorization is not exact",
        );
      }
      await transaction`
        INSERT INTO oracle_candidate_source_preflight_continuation_authorizations (
          authorization_id, authorization_version, authorization_sha256,
          plan_id, plan_sha256, approval_id, approval_sha256,
          approval_authorization_statement_sha256,
          original_implementation_commit_sha,
          amended_implementation_commit_sha, plan_revision,
          approved_plan_revision, plan_artifact_cid, plan_artifact_sha256,
          failed_request_id, failed_receipt_sha256, authorized_operation,
          domain, operation_kind, resolver, ipns_network_key,
          expected_prior_cid, expected_target_cid, resolver_policy,
          maximum_new_observations, authorized_attempt_sequence,
          request_envelope_sha256, cost_envelope_sha256,
          remaining_preflight_requests, remaining_total_requests,
          remaining_request_cost_usd, remaining_hard_budget_usd,
          authorization_binding, authorization_binding_sha256,
          authorization_statement, authorization_statement_sha256,
          authorizer_reference, authorized_at, authorized_at_iso,
          authorization_payload
        ) VALUES (
          ${authorization.authorizationId},
          ${authorization.authorizationVersion},
          ${authorization.authorizationSha256}, ${binding.plan.planId},
          ${binding.plan.planSha256}, ${binding.approval.approvalId},
          ${binding.approval.approvalSha256},
          ${binding.approval.authorizationStatementSha256},
          ${binding.approval.originalImplementationCommitSha},
          ${binding.amendedImplementationCommitSha},
          ${binding.plan.planRevision}, ${binding.approval.approvedPlanRevision},
          ${binding.plan.artifactCid}, ${binding.plan.artifactSha256},
          ${binding.failedReceipt.requestId},
          ${binding.failedReceipt.receiptSha256},
          ${binding.authorizedObservation.authorizedOperation},
          ${binding.authorizedObservation.domain},
          ${binding.authorizedObservation.storedOperationKind},
          ${binding.authorizedObservation.resolver},
          ${binding.authorizedObservation.ipnsNetworkKey},
          ${binding.authorizedObservation.expectedPriorCid},
          ${binding.authorizedObservation.expectedTargetCid},
          ${binding.authorizedObservation.resolverPolicy},
          ${binding.authorizedObservation.maximumNewLogicalObservations},
          ${binding.authorizedObservation.authorizedAttemptSequence},
          ${binding.remainingAllowance.requestEnvelopeSha256},
          ${binding.remainingAllowance.costEnvelopeSha256},
          ${binding.remainingAllowance.preflightRequests},
          ${binding.remainingAllowance.totalRequests},
          ${binding.remainingAllowance.requestCostUsd},
          ${binding.remainingAllowance.hardBudgetUsd},
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
