import {
  recordCandidateSourceSnapshotCarImportOutcome,
  recordCandidateSourceSnapshotCarImportInspection,
  startCandidateSourceSnapshotCarImportAttempt,
} from "../db/candidate-source-snapshot-car-attempt.js";
import {
  bulkVerifyCandidateSourceSnapshotCarMembers,
  candidateSourceSnapshotCarRootSetSha256,
  recordCandidateSourceSnapshotCarGatewayEvidence,
  recordCandidateSourceSnapshotCarImportAuthorization,
  recordCandidateSourceSnapshotCarImportReceipt,
  recordCandidateSourceSnapshotCarUploadClosure,
  type CandidateSourceSnapshotCarArtifactRecord,
} from "../db/candidate-source-snapshot-car-import.js";
import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "./candidate-source-snapshot-demo.js";
import {
  CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_ENDPOINT,
  CandidateSourceSnapshotFilebaseCarImportTransport,
  type CandidateSourceSnapshotCarImportArtifact,
  type CandidateSourceSnapshotCarImportAuthorizationBinding,
  type CandidateSourceSnapshotCarImportFailureCode,
} from "./candidate-source-snapshot-filebase-car.js";

type AuthorizationInput = Parameters<
  typeof recordCandidateSourceSnapshotCarImportAuthorization
>[1] & {
  openDataBucketTokenSha256: string;
  overallTimeoutMs: number;
  queryTableBucketTokenSha256: string;
};

export interface CandidateSourceSnapshotCarExecutionArtifact {
  artifact: CandidateSourceSnapshotCarImportArtifact;
  record: CandidateSourceSnapshotCarArtifactRecord;
}

export interface CandidateSourceSnapshotCarExecutionResult {
  closureId: string | null;
  completedDomains: ("open_data" | "query_table")[];
  stoppedDomain: "open_data" | "query_table" | null;
  stoppedOutcome:
    "outcome_unknown" | "retryable_failure" | "terminal_failure" | null;
}

function providerStatus(
  code: CandidateSourceSnapshotCarImportFailureCode,
): Parameters<
  typeof recordCandidateSourceSnapshotCarImportOutcome
>[1]["providerStatus"] {
  return code === "provider_retryable_status" ? "transport_unknown" : code;
}

function assertArtifactBinding(
  plan: CandidateSourceSnapshotDemoPlan,
  authorization: AuthorizationInput,
  value: CandidateSourceSnapshotCarExecutionArtifact,
): void {
  const { artifact, record } = value;
  const target =
    artifact.domain === "open_data"
      ? plan.targets.openData
      : plan.targets.queryTable;
  if (
    artifact.planId !== plan.planId ||
    artifact.planSha256 !== plan.planSha256 ||
    artifact.implementationCommitSha !==
      authorization.implementationCommitSha ||
    artifact.bucketName !== target.bucket ||
    artifact.roots[0] !== target.targetCid ||
    record.carArtifactId !== artifact.artifactId ||
    record.carRole !== artifact.domain ||
    record.planId !== artifact.planId ||
    record.planSha256 !== artifact.planSha256 ||
    record.implementationCommitSha !== artifact.implementationCommitSha ||
    record.primaryRootCid !== artifact.roots[0] ||
    record.rootCount !== artifact.roots.length ||
    record.rootsSha256 !==
      candidateSourceSnapshotCarRootSetSha256(artifact.roots) ||
    record.blockCount !== artifact.blockCount ||
    record.blockMembershipSha256 !== artifact.blockMembershipSha256 ||
    record.carBytes !== artifact.carBytes ||
    record.carSha256 !== artifact.carSha256
  ) {
    throw new Error("Candidate CAR execution artifact binding conflicts");
  }
}

/**
 * Closed open-data-first/query-table-second CAR execution composition. It
 * permits only one inspection-gated second attempt and refuses replayed
 * request admissions.
 */
export async function executeCandidateSourceSnapshotCarImports(input: {
  artifacts: readonly [
    CandidateSourceSnapshotCarExecutionArtifact,
    CandidateSourceSnapshotCarExecutionArtifact,
  ];
  authorization: AuthorizationInput;
  databaseUrl: string;
  now?: () => string;
  plan: CandidateSourceSnapshotDemoPlan;
  transport: CandidateSourceSnapshotFilebaseCarImportTransport;
}): Promise<CandidateSourceSnapshotCarExecutionResult> {
  const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
  if (
    input.authorization.planId !== plan.planId ||
    input.authorization.planSha256 !== plan.planSha256 ||
    input.authorization.endpoint !==
      CANDIDATE_SOURCE_SNAPSHOT_FILEBASE_CAR_IMPORT_ENDPOINT ||
    input.authorization.importMethod !== "rpc_dag_import"
  ) {
    throw new Error("Candidate CAR execution authorization conflicts");
  }
  if (
    input.artifacts[0].artifact.domain !== "open_data" ||
    input.artifacts[1].artifact.domain !== "query_table"
  ) {
    throw new Error("Candidate CAR execution order conflicts");
  }
  input.artifacts.forEach((artifact) =>
    assertArtifactBinding(plan, input.authorization, artifact),
  );
  const authorizationResult =
    await recordCandidateSourceSnapshotCarImportAuthorization(
      input.databaseUrl,
      input.authorization,
    );
  const authorization =
    authorizationResult as CandidateSourceSnapshotCarImportAuthorizationBinding;
  if (
    authorization.openDataBucketTokenSha256 !==
      input.authorization.openDataBucketTokenSha256 ||
    authorization.queryTableBucketTokenSha256 !==
      input.authorization.queryTableBucketTokenSha256 ||
    authorization.overallTimeoutMs !== input.authorization.overallTimeoutMs ||
    authorization.openDataBucketIdentity !== plan.targets.openData.bucket ||
    authorization.queryTableBucketIdentity !== plan.targets.queryTable.bucket
  ) {
    throw new Error("Candidate CAR durable transport authorization conflicts");
  }
  const now = input.now ?? (() => new Date().toISOString());
  const completedDomains: ("open_data" | "query_table")[] = [];

  for (const value of input.artifacts) {
    let proof:
      | {
          attemptId: string;
          outcomeId: string;
          providerRequestIdHash: string | null;
          rootBlock: Uint8Array;
        }
      | undefined;
    let stoppedOutcome: CandidateSourceSnapshotCarExecutionResult["stoppedOutcome"] =
      null;
    for (const attemptSequence of [1, 2] as const) {
      const attempt = await startCandidateSourceSnapshotCarImportAttempt(
        input.databaseUrl,
        {
          artifactId: value.artifact.artifactId,
          attemptSequence,
          authorizationId: authorization.authorizationId,
          implementationCommitSha: authorization.implementationCommitSha,
          planId: plan.planId,
          planSha256: plan.planSha256,
          startedAt: now(),
        },
      );
      if (attempt.isReplay) {
        throw new Error("Candidate CAR request admission replay cannot send");
      }
      const result = await input.transport.importCar({
        admission: attempt,
        artifact: value.artifact,
        authorization,
      });
      const outcome = await recordCandidateSourceSnapshotCarImportOutcome(
        input.databaseUrl,
        {
          attemptId: attempt.attemptId,
          observedRootSetSha256:
            result.outcome === "verified" ? result.observedRootsSha256 : null,
          outcome: result.outcome,
          providerEvidenceSha256: result.providerEvidenceSha256,
          providerHttpStatus: result.httpStatus,
          providerRequestIdHash: result.providerRequestIdHash,
          providerResponseBytes: result.responseBytes,
          providerStatus:
            result.outcome === "verified"
              ? "accepted"
              : providerStatus(result.failureCode),
          recordedAt: now(),
        },
      );
      if (result.outcome === "verified") {
        proof = {
          attemptId: attempt.attemptId,
          outcomeId: outcome.outcomeId,
          providerRequestIdHash:
            result.sanitizedEvidence.gatewayRoot.providerRequestIdHash,
          rootBlock: result.verifiedRootBlock,
        };
        break;
      }
      stoppedOutcome = result.outcome;
      if (result.outcome !== "outcome_unknown") break;
      const inspected = await input.transport.inspectCar({
        admission: attempt,
        artifact: value.artifact,
        authorization,
      });
      const inspection = await recordCandidateSourceSnapshotCarImportInspection(
        input.databaseUrl,
        {
          inspectedAt: now(),
          inspectionResult: inspected.inspectionResult,
          observedRootSetSha256: inspected.observedRootSetSha256,
          outcomeId: outcome.outcomeId,
          pinStatus: inspected.pinStatus,
          providerEvidenceSha256: inspected.providerEvidenceSha256,
          providerHttpStatus: inspected.providerHttpStatus,
          providerRequestIdHash: inspected.providerRequestIdHash,
          providerResponseBytes: inspected.providerResponseBytes,
          rootStatus: inspected.rootStatus,
        },
      );
      if (
        inspection.inspectionResult === "present_exact" &&
        inspected.inspectedRootBlock
      ) {
        proof = {
          attemptId: attempt.attemptId,
          outcomeId: outcome.outcomeId,
          providerRequestIdHash: inspected.providerRequestIdHash,
          rootBlock: inspected.inspectedRootBlock,
        };
        break;
      }
      if (
        inspection.inspectionResult !== "conclusively_absent" ||
        attemptSequence === 2
      ) {
        break;
      }
    }
    if (!proof) {
      return {
        closureId: null,
        completedDomains,
        stoppedDomain: value.artifact.domain,
        stoppedOutcome,
      };
    }
    const gateway = await recordCandidateSourceSnapshotCarGatewayEvidence(
      input.databaseUrl,
      {
        attemptId: proof.attemptId,
        observedAt: now(),
        outcomeId: proof.outcomeId,
        providerHttpStatus: 200,
        providerRequestIdHash: proof.providerRequestIdHash,
        rootBlockBytes: Uint8Array.from(proof.rootBlock),
      },
    );
    const receipt = await recordCandidateSourceSnapshotCarImportReceipt(
      input.databaseUrl,
      {
        authorizationId: authorization.authorizationId,
        carArtifactId: value.artifact.artifactId,
        gatewayEvidenceId: gateway.evidenceId,
        implementationCommitSha: authorization.implementationCommitSha,
        planId: plan.planId,
        planSha256: plan.planSha256,
        verificationTimestamp: now(),
      },
    );
    await bulkVerifyCandidateSourceSnapshotCarMembers(input.databaseUrl, {
      carArtifactId: value.artifact.artifactId,
      carImportReceiptId: receipt.carImportReceiptId,
      planId: plan.planId,
      verifiedAt: now(),
    });
    completedDomains.push(value.artifact.domain);
  }
  const closure = await recordCandidateSourceSnapshotCarUploadClosure(
    input.databaseUrl,
    {
      approvalId: authorization.approvalId,
      planId: plan.planId,
      planSha256: plan.planSha256,
    },
  );
  return {
    closureId: closure.closureId,
    completedDomains,
    stoppedDomain: null,
    stoppedOutcome: null,
  };
}
