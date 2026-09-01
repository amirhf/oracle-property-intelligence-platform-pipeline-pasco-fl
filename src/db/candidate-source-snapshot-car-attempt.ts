import postgres from "postgres";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId } from "../lib/hash.js";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const commitSchema = z.string().regex(/^[a-f0-9]{40}$/);
const planIdSchema = z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/);
const authorizationIdSchema = z
  .string()
  .regex(/^snapshotdemocarauthorization_[a-f0-9]{32}$/);
const artifactIdSchema = z.string().regex(/^snapshotdemocar_[a-f0-9]{32}$/);
const attemptIdSchema = z
  .string()
  .regex(/^snapshotdemocarattempt_[a-f0-9]{32}$/);
const outcomeIdSchema = z
  .string()
  .regex(/^snapshotdemocaroutcome_[a-f0-9]{32}$/);
const timestampSchema = z
  .string()
  .regex(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)
  .datetime();
const httpStatusSchema = z.number().int().min(100).max(599).nullable();
const responseBytesSchema = z.number().int().min(0).max(134_217_728).nullable();

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  await transaction`SELECT pg_advisory_xact_lock(
    hashtext('oracle-candidate-source-snapshot-demo-v2'), hashtext('pasco')
  )`;
}

const startInputSchema = z.strictObject({
  artifactId: artifactIdSchema,
  attemptSequence: z.number().int().min(1).max(2),
  authorizationId: authorizationIdSchema,
  implementationCommitSha: commitSchema,
  planId: planIdSchema,
  planSha256: sha256Schema,
  startedAt: timestampSchema,
});

interface AttemptBindingRow {
  car_bytes: string;
  car_sha256: string;
  endpoint:
    "https://rpc.filebase.io/api/v0/dag/import" | "https://s3.filebase.com";
  import_method: "rpc_dag_import" | "s3_put_import_car";
  member_set_sha256: string;
  primary_root_cid: string;
  root_set_sha256: string;
}

export interface CandidateSourceSnapshotCarImportAttempt {
  artifactId: string;
  attemptId: string;
  attemptSequence: number;
  authorizationId: string;
  isReplay: boolean;
  planId: string;
  requestSha256: string;
  startedAt: string;
}

/**
 * Persists the immutable request_started event. A replay is surfaced so a
 * caller can never mistake a recovered admission for permission to resend.
 */
export async function startCandidateSourceSnapshotCarImportAttempt(
  databaseUrl: string,
  inputValue: z.input<typeof startInputSchema>,
): Promise<CandidateSourceSnapshotCarImportAttempt> {
  const input = startInputSchema.parse(inputValue);
  const attemptVersion =
    "candidate-source-snapshot-car-import-attempt-v1" as const;
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<AttemptBindingRow[]>`
        SELECT artifact.car_sha256, artifact.car_bytes::text,
               artifact.primary_root_cid, artifact.root_set_sha256,
               artifact.member_set_sha256, car_auth.endpoint,
               car_auth.import_method
        FROM oracle_candidate_source_snapshot_car_artifacts artifact
        JOIN oracle_candidate_source_snapshot_car_import_authorizations car_auth
          ON car_auth.plan_id = artifact.plan_id
        WHERE artifact.car_artifact_id = ${input.artifactId}
          AND artifact.plan_id = ${input.planId}
          AND artifact.plan_sha256 = ${input.planSha256}
          AND car_auth.car_authorization_id = ${input.authorizationId}
          AND car_auth.implementation_commit_sha =
            ${input.implementationCommitSha}
        FOR SHARE OF artifact, car_auth
      `;
      const binding = rows[0];
      if (!binding) {
        throw new DurableConflictError("CAR import attempt binding is absent");
      }
      const payload = {
        artifactId: input.artifactId,
        authorizationId: input.authorizationId,
        carBytes: Number(binding.car_bytes),
        carSha256: binding.car_sha256,
        endpoint: binding.endpoint,
        eventKind: "request_started",
        implementationCommitSha: input.implementationCommitSha,
        importMethod: binding.import_method,
        memberSetSha256: binding.member_set_sha256,
        planId: input.planId,
        planSha256: input.planSha256,
        primaryRootCid: binding.primary_root_cid,
        requestAttempt: input.attemptSequence,
        rootSetSha256: binding.root_set_sha256,
        schemaVersion: attemptVersion,
        startedAt: input.startedAt,
      };
      const requestSha256 = canonicalJsonSha256(payload);
      const attemptId = deterministicId("snapshotdemocarattempt", [
        attemptVersion,
        input.planId,
        input.artifactId,
        String(input.attemptSequence),
        requestSha256,
      ]);
      const existing = await transaction<
        { request_payload: postgres.JSONValue; request_sha256: string }[]
      >`
        SELECT request_payload, request_sha256
        FROM oracle_candidate_source_snapshot_car_import_attempts
        WHERE car_import_attempt_id = ${attemptId}
           OR (car_artifact_id = ${input.artifactId}
               AND attempt_sequence = ${input.attemptSequence})
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].request_sha256 !== requestSha256 ||
          canonicalJson(existing[0].request_payload) !== canonicalJson(payload)
        ) {
          throw new DurableConflictError("CAR import attempt replay conflicts");
        }
        return {
          artifactId: input.artifactId,
          attemptId,
          attemptSequence: input.attemptSequence,
          authorizationId: input.authorizationId,
          isReplay: true,
          planId: input.planId,
          requestSha256,
          startedAt: input.startedAt,
        };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_import_attempts (
          car_import_attempt_id, attempt_version, event_kind,
          car_authorization_id, car_artifact_id, plan_id, plan_sha256,
          attempt_sequence, endpoint, import_method, car_sha256, car_bytes,
          primary_root_cid, root_set_sha256, member_set_sha256,
          implementation_commit_sha, started_at, request_payload, request_sha256
        ) VALUES (
          ${attemptId}, ${attemptVersion}, 'request_started',
          ${input.authorizationId}, ${input.artifactId}, ${input.planId},
          ${input.planSha256}, ${input.attemptSequence}, ${binding.endpoint},
          ${binding.import_method}, ${binding.car_sha256}, ${binding.car_bytes},
          ${binding.primary_root_cid}, ${binding.root_set_sha256},
          ${binding.member_set_sha256}, ${input.implementationCommitSha},
          ${input.startedAt}, ${transaction.json(payload as postgres.JSONValue)},
          ${requestSha256}
        )
      `;
      return {
        artifactId: input.artifactId,
        attemptId,
        attemptSequence: input.attemptSequence,
        authorizationId: input.authorizationId,
        isReplay: false,
        planId: input.planId,
        requestSha256,
        startedAt: input.startedAt,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const outcomeSchema = z.enum([
  "verified",
  "retryable_failure",
  "outcome_unknown",
  "terminal_failure",
]);
const providerStatusSchema = z.enum([
  "accepted",
  "caller_aborted",
  "provider_pin_error",
  "provider_rejected",
  "provider_result_invalid",
  "provider_root_mismatch",
  "provider_retryable_status",
  "redirect_rejected",
  "response_too_large",
  "stream_integrity_unknown",
  "timeout_unknown",
  "transport_unknown",
]);
const outcomeInputSchema = z
  .strictObject({
    attemptId: attemptIdSchema,
    observedRootSetSha256: sha256Schema.nullable(),
    outcome: outcomeSchema,
    providerEvidenceSha256: sha256Schema,
    providerHttpStatus: httpStatusSchema,
    providerRequestIdHash: sha256Schema.nullable(),
    providerResponseBytes: responseBytesSchema,
    providerStatus: providerStatusSchema,
    recordedAt: timestampSchema,
  })
  .superRefine((value, context) => {
    const accepted =
      (value.outcome === "verified" &&
        value.providerStatus === "accepted" &&
        value.providerHttpStatus !== null &&
        value.providerHttpStatus >= 200 &&
        value.providerHttpStatus <= 299 &&
        value.observedRootSetSha256 !== null) ||
      (value.outcome === "retryable_failure" &&
        value.observedRootSetSha256 === null &&
        value.providerStatus === "provider_retryable_status" &&
        value.providerHttpStatus === 429) ||
      (value.outcome === "outcome_unknown" &&
        [
          "provider_pin_error",
          "provider_result_invalid",
          "provider_root_mismatch",
          "response_too_large",
          "stream_integrity_unknown",
          "timeout_unknown",
          "transport_unknown",
        ].includes(value.providerStatus) &&
        value.observedRootSetSha256 === null) ||
      (value.outcome === "terminal_failure" &&
        value.observedRootSetSha256 === null &&
        ((value.providerStatus === "caller_aborted" &&
          value.providerHttpStatus === null) ||
          (value.providerStatus === "redirect_rejected" &&
            value.providerHttpStatus !== null &&
            value.providerHttpStatus >= 300 &&
            value.providerHttpStatus <= 399) ||
          (value.providerStatus === "provider_rejected" &&
            value.providerHttpStatus !== null &&
            ((value.providerHttpStatus >= 100 &&
              value.providerHttpStatus <= 199) ||
              (value.providerHttpStatus >= 400 &&
                value.providerHttpStatus <= 499 &&
                value.providerHttpStatus !== 429)))));
    if (!accepted) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "CAR import outcome classification is inconsistent",
      });
    }
  });

interface AttemptRow {
  attempt_sequence: 1 | 2;
  car_artifact_id: string;
  implementation_commit_sha: string;
  plan_id: string;
}

export interface CandidateSourceSnapshotCarImportOutcome {
  artifactId: string;
  attemptId: string;
  attemptSequence: 1 | 2;
  isReplay: boolean;
  outcome: z.infer<typeof outcomeSchema>;
  outcomeId: string;
  outcomeSha256: string;
  planId: string;
}

/** Appends the sole terminal classification for an admitted CAR request. */
export async function recordCandidateSourceSnapshotCarImportOutcome(
  databaseUrl: string,
  inputValue: z.input<typeof outcomeInputSchema>,
): Promise<CandidateSourceSnapshotCarImportOutcome> {
  const input = outcomeInputSchema.parse(inputValue);
  const outcomeVersion =
    "candidate-source-snapshot-car-import-outcome-v1" as const;
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<AttemptRow[]>`
        SELECT attempt_sequence, car_artifact_id, plan_id,
               implementation_commit_sha
        FROM oracle_candidate_source_snapshot_car_import_attempts
        WHERE car_import_attempt_id = ${input.attemptId}
        FOR SHARE
      `;
      const attempt = rows[0];
      if (!attempt) {
        throw new DurableInputError("CAR import attempt does not exist");
      }
      const payload = {
        artifactId: attempt.car_artifact_id,
        attemptId: input.attemptId,
        attemptSequence: attempt.attempt_sequence,
        implementationCommitSha: attempt.implementation_commit_sha,
        observedRootSetSha256: input.observedRootSetSha256,
        outcome: input.outcome,
        planId: attempt.plan_id,
        providerEvidenceSha256: input.providerEvidenceSha256,
        providerHttpStatus: input.providerHttpStatus,
        providerRequestIdHash: input.providerRequestIdHash,
        providerResponseBytes: input.providerResponseBytes,
        providerStatus: input.providerStatus,
        recordedAt: input.recordedAt,
        schemaVersion: outcomeVersion,
      };
      const outcomeSha256 = canonicalJsonSha256(payload);
      const outcomeId = deterministicId("snapshotdemocaroutcome", [
        outcomeVersion,
        input.attemptId,
        outcomeSha256,
      ]);
      const existing = await transaction<
        { outcome_payload: postgres.JSONValue; outcome_sha256: string }[]
      >`
        SELECT outcome_payload, outcome_sha256
        FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
        WHERE car_import_outcome_id = ${outcomeId}
           OR car_import_attempt_id = ${input.attemptId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].outcome_sha256 !== outcomeSha256 ||
          canonicalJson(existing[0].outcome_payload) !== canonicalJson(payload)
        ) {
          throw new DurableConflictError("CAR import outcome replay conflicts");
        }
        return {
          artifactId: attempt.car_artifact_id,
          attemptId: input.attemptId,
          attemptSequence: attempt.attempt_sequence,
          isReplay: true,
          outcome: input.outcome,
          outcomeId,
          outcomeSha256,
          planId: attempt.plan_id,
        };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_import_attempt_outcomes (
          car_import_outcome_id, outcome_version, car_import_attempt_id,
          car_artifact_id, plan_id, attempt_sequence, outcome, provider_status,
          provider_http_status, provider_response_bytes,
          provider_evidence_sha256, provider_request_id_hash,
          observed_root_set_sha256, recorded_at, implementation_commit_sha,
          outcome_payload, outcome_sha256
        ) VALUES (
          ${outcomeId}, ${outcomeVersion}, ${input.attemptId},
          ${attempt.car_artifact_id}, ${attempt.plan_id},
          ${attempt.attempt_sequence}, ${input.outcome}, ${input.providerStatus},
          ${input.providerHttpStatus}, ${input.providerResponseBytes},
          ${input.providerEvidenceSha256}, ${input.providerRequestIdHash},
          ${input.observedRootSetSha256}, ${input.recordedAt},
          ${attempt.implementation_commit_sha},
          ${transaction.json(payload as postgres.JSONValue)}, ${outcomeSha256}
        )
      `;
      return {
        artifactId: attempt.car_artifact_id,
        attemptId: input.attemptId,
        attemptSequence: attempt.attempt_sequence,
        isReplay: false,
        outcome: input.outcome,
        outcomeId,
        outcomeSha256,
        planId: attempt.plan_id,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const inspectionResultSchema = z.enum([
  "conclusively_absent",
  "present_exact",
  "present_unexpected",
  "unavailable",
]);
const rootStatusSchema = z.enum([
  "absent",
  "present_exact",
  "present_unexpected",
  "unavailable",
]);
const pinStatusSchema = z.enum([
  "absent",
  "pinned",
  "pinning",
  "failed",
  "unavailable",
]);
const inspectionInputSchema = z
  .strictObject({
    inspectedAt: timestampSchema,
    inspectionResult: inspectionResultSchema,
    observedRootSetSha256: sha256Schema.nullable(),
    outcomeId: outcomeIdSchema,
    pinStatus: pinStatusSchema,
    providerEvidenceSha256: sha256Schema,
    providerHttpStatus: httpStatusSchema,
    providerRequestIdHash: sha256Schema.nullable(),
    providerResponseBytes: responseBytesSchema,
    rootStatus: rootStatusSchema,
  })
  .superRefine((value, context) => {
    const accepted =
      (value.inspectionResult === "conclusively_absent" &&
        value.rootStatus === "absent" &&
        value.pinStatus === "absent" &&
        value.observedRootSetSha256 === null) ||
      (value.inspectionResult === "present_exact" &&
        value.rootStatus === "present_exact" &&
        ["pinned", "pinning"].includes(value.pinStatus) &&
        value.observedRootSetSha256 !== null) ||
      (value.inspectionResult === "present_unexpected" &&
        value.rootStatus === "present_unexpected" &&
        value.observedRootSetSha256 !== null) ||
      (value.inspectionResult === "unavailable" &&
        (value.rootStatus === "unavailable" ||
          value.pinStatus === "unavailable"));
    if (!accepted) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "CAR import inspection classification is inconsistent",
      });
    }
  });

interface InspectionBindingRow extends AttemptRow {
  car_import_attempt_id: string;
  car_import_outcome_id: string;
  outcome: z.infer<typeof outcomeSchema>;
  root_set_sha256: string;
}

export interface CandidateSourceSnapshotCarImportInspection {
  artifactId: string;
  attemptId: string;
  inspectionId: string;
  inspectionResult: z.infer<typeof inspectionResultSchema>;
  inspectionSha256: string;
  isReplay: boolean;
  outcomeId: string;
  planId: string;
}

/** Records one immutable root-and-pin inspection for an unknown outcome. */
export async function recordCandidateSourceSnapshotCarImportInspection(
  databaseUrl: string,
  inputValue: z.input<typeof inspectionInputSchema>,
): Promise<CandidateSourceSnapshotCarImportInspection> {
  const input = inspectionInputSchema.parse(inputValue);
  const inspectionVersion =
    "candidate-source-snapshot-car-import-inspection-v1" as const;
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<InspectionBindingRow[]>`
        SELECT attempt.car_import_attempt_id, attempt.attempt_sequence,
               attempt.car_artifact_id, attempt.plan_id,
               attempt.implementation_commit_sha, outcome.car_import_outcome_id,
               outcome.outcome, artifact.root_set_sha256
        FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
        JOIN oracle_candidate_source_snapshot_car_import_attempts attempt
          ON attempt.car_import_attempt_id = outcome.car_import_attempt_id
        JOIN oracle_candidate_source_snapshot_car_artifacts artifact
          ON artifact.car_artifact_id = attempt.car_artifact_id
        WHERE outcome.car_import_outcome_id = ${input.outcomeId}
        FOR SHARE OF outcome, attempt, artifact
      `;
      const binding = rows[0];
      if (!binding || binding.outcome !== "outcome_unknown") {
        throw new DurableInputError(
          "CAR import inspection requires an unknown attempt outcome",
        );
      }
      const payload = {
        artifactId: binding.car_artifact_id,
        attemptId: binding.car_import_attempt_id,
        implementationCommitSha: binding.implementation_commit_sha,
        inspectedAt: input.inspectedAt,
        inspectionResult: input.inspectionResult,
        observedRootSetSha256: input.observedRootSetSha256,
        outcomeId: input.outcomeId,
        pinStatus: input.pinStatus,
        planId: binding.plan_id,
        providerEvidenceSha256: input.providerEvidenceSha256,
        providerHttpStatus: input.providerHttpStatus,
        providerRequestIdHash: input.providerRequestIdHash,
        providerResponseBytes: input.providerResponseBytes,
        rootSetSha256: binding.root_set_sha256,
        rootStatus: input.rootStatus,
        schemaVersion: inspectionVersion,
      };
      const inspectionSha256 = canonicalJsonSha256(payload);
      const inspectionId = deterministicId("snapshotdemocarinspection", [
        inspectionVersion,
        binding.car_import_attempt_id,
        input.outcomeId,
        inspectionSha256,
      ]);
      const existing = await transaction<
        { inspection_payload: postgres.JSONValue; inspection_sha256: string }[]
      >`
        SELECT inspection_payload, inspection_sha256
        FROM oracle_candidate_source_snapshot_car_import_inspections
        WHERE car_import_inspection_id = ${inspectionId}
           OR car_import_attempt_id = ${binding.car_import_attempt_id}
           OR car_import_outcome_id = ${input.outcomeId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].inspection_sha256 !== inspectionSha256 ||
          canonicalJson(existing[0].inspection_payload) !==
            canonicalJson(payload)
        ) {
          throw new DurableConflictError(
            "CAR import inspection replay conflicts",
          );
        }
        return {
          artifactId: binding.car_artifact_id,
          attemptId: binding.car_import_attempt_id,
          inspectionId,
          inspectionResult: input.inspectionResult,
          inspectionSha256,
          isReplay: true,
          outcomeId: input.outcomeId,
          planId: binding.plan_id,
        };
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_import_inspections (
          car_import_inspection_id, inspection_version,
          car_import_attempt_id, car_import_outcome_id, car_artifact_id,
          plan_id, root_set_sha256, inspection_result, root_status, pin_status,
          observed_root_set_sha256, provider_http_status,
          provider_response_bytes, provider_evidence_sha256,
          provider_request_id_hash, inspected_at, implementation_commit_sha,
          inspection_payload, inspection_sha256
        ) VALUES (
          ${inspectionId}, ${inspectionVersion},
          ${binding.car_import_attempt_id}, ${input.outcomeId},
          ${binding.car_artifact_id}, ${binding.plan_id},
          ${binding.root_set_sha256}, ${input.inspectionResult},
          ${input.rootStatus}, ${input.pinStatus},
          ${input.observedRootSetSha256}, ${input.providerHttpStatus},
          ${input.providerResponseBytes}, ${input.providerEvidenceSha256},
          ${input.providerRequestIdHash}, ${input.inspectedAt},
          ${binding.implementation_commit_sha},
          ${transaction.json(payload as postgres.JSONValue)},
          ${inspectionSha256}
        )
      `;
      return {
        artifactId: binding.car_artifact_id,
        attemptId: binding.car_import_attempt_id,
        inspectionId,
        inspectionResult: input.inspectionResult,
        inspectionSha256,
        isReplay: false,
        outcomeId: input.outcomeId,
        planId: binding.plan_id,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
