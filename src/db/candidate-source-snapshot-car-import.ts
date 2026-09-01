import { createHash } from "node:crypto";

import { CID } from "multiformats/cid";
import postgres from "postgres";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const commitSchema = z.string().regex(/^[a-f0-9]{40}$/);
const cidSchema = z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/);
const planIdSchema = z.string().regex(/^snapshotdemo_[a-f0-9]{32}$/);
const approvalIdSchema = z
  .string()
  .regex(/^snapshotdemoapproval_[a-f0-9]{32}$/);
const timestampSchema = z
  .string()
  .regex(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/)
  .datetime();
const domainSchema = z.enum(["open_data", "query_table"]);
const carRpcEndpoint =
  "https://rpc.filebase.io/api/v0/dag/import?pin-roots=true" as const;
const remoteObjectKeySchema = z
  .string()
  .min(1)
  .max(2048)
  .refine(
    (value) =>
      !value.startsWith("/") &&
      !value.includes("\\") &&
      !value.split("/").includes(".."),
    "Remote object key is unsafe",
  );

const memberSchema = z.strictObject({
  byteSize: z.number().int().min(0).max(536_870_912),
  domain: domainSchema,
  expectedCid: cidSchema,
  remoteObjectKey: remoteObjectKeySchema,
  sha256: sha256Schema,
});

export type CandidateSourceSnapshotCarMember = z.infer<typeof memberSchema>;

export interface CandidateSourceSnapshotCarArtifactRecord {
  blockCount: number;
  blockMembershipSha256: string;
  carArtifactId: string;
  carBytes: number;
  carRole: "open_data" | "query_table";
  carSha256: string;
  implementationCommitSha: string;
  localValidationSha256: string;
  logicalMemberBytes: number;
  logicalMemberCount: number;
  logicalMembershipSha256: string;
  planId: string;
  planSha256: string;
  primaryRootCid: string;
  recordedAt: string;
  rootCount: number;
  rootsSha256: string;
}

function memberLine(member: CandidateSourceSnapshotCarMember): string {
  return `${canonicalJson([
    member.domain,
    member.remoteObjectKey,
    member.expectedCid,
    member.sha256,
    member.byteSize,
  ])}\n`;
}

export function candidateSourceSnapshotCarMemberSetSha256(
  members: readonly CandidateSourceSnapshotCarMember[],
): string {
  const hash = createHash("sha256");
  for (const value of members)
    hash.update(memberLine(memberSchema.parse(value)));
  return hash.digest("hex");
}

export function candidateSourceSnapshotCarRootSetSha256(
  roots: readonly string[],
): string {
  return sha256(canonicalJson(roots.map((root) => cidSchema.parse(root))));
}

function artifactIdentity(input: {
  artifactVersion: string;
  carRole: "open_data" | "query_table";
  carSha256: string;
  memberSetSha256: string;
  planId: string;
  planSha256: string;
  primaryRootCid: string;
  rootSetSha256: string;
}): string {
  return deterministicId("snapshotdemocar", [
    input.artifactVersion,
    input.planId,
    input.planSha256,
    input.carRole,
    input.carSha256,
    input.primaryRootCid,
    input.rootSetSha256,
    input.memberSetSha256,
  ]);
}

async function lock(transaction: postgres.TransactionSql): Promise<void> {
  await transaction`SELECT pg_advisory_xact_lock(
    hashtext('oracle-candidate-source-snapshot-demo-v2'), hashtext('pasco')
  )`;
}

const artifactInputSchema = z.strictObject({
  blockCount: z.number().int().positive(),
  blockMembershipSha256: sha256Schema,
  carBytes: z.number().int().positive().max(8_589_934_592),
  carRole: domainSchema,
  carSha256: sha256Schema,
  implementationCommitSha: commitSchema,
  members: z.array(memberSchema).min(1).max(350_000),
  planId: planIdSchema,
  planSha256: sha256Schema,
  primaryRootCid: cidSchema,
  recordedAt: timestampSchema,
  roots: z.array(cidSchema).min(1).max(350_000),
});

/**
 * Records the locally validated CAR and its exact plan-object membership.
 * This API has no transport and grants no authority to import the CAR.
 */
export async function recordCandidateSourceSnapshotCarArtifact(
  databaseUrl: string,
  inputValue: z.input<typeof artifactInputSchema>,
): Promise<CandidateSourceSnapshotCarArtifactRecord> {
  const input = artifactInputSchema.parse(inputValue);
  if (input.roots[0] !== input.primaryRootCid) {
    throw new DurableInputError("CAR primary root must be first");
  }
  if (new Set(input.roots).size !== input.roots.length) {
    throw new DurableInputError("CAR roots must be distinct");
  }
  if (input.members.some((member) => member.domain !== input.carRole)) {
    throw new DurableInputError(
      "Every CAR member must match its destination domain",
    );
  }
  const memberByCid = new Map<string, CandidateSourceSnapshotCarMember>();
  for (const member of input.members) {
    if (!memberByCid.has(member.expectedCid)) {
      memberByCid.set(member.expectedCid, member);
    }
  }
  if (input.roots.some((root) => !memberByCid.has(root))) {
    throw new DurableInputError(
      "Every distinct CAR root must be an exact member",
    );
  }
  const logicalMembershipSha256 = candidateSourceSnapshotCarMemberSetSha256(
    input.members,
  );
  const rootsSha256 = candidateSourceSnapshotCarRootSetSha256(input.roots);
  const logicalMemberBytes = input.members.reduce(
    (total, member) => total + member.byteSize,
    0,
  );
  const artifactVersion = "candidate-source-snapshot-car-v1";
  const carArtifactId = artifactIdentity({
    artifactVersion,
    carRole: input.carRole,
    carSha256: input.carSha256,
    memberSetSha256: logicalMembershipSha256,
    planId: input.planId,
    planSha256: input.planSha256,
    primaryRootCid: input.primaryRootCid,
    rootSetSha256: rootsSha256,
  });
  const localValidationSha256 = canonicalJsonSha256({
    blockCount: input.blockCount,
    blockMembershipSha256: input.blockMembershipSha256,
    carArtifactId,
    carBytes: input.carBytes,
    carRole: input.carRole,
    carSha256: input.carSha256,
    logicalMemberBytes,
    logicalMemberCount: input.members.length,
    logicalMembershipSha256,
    planId: input.planId,
    planSha256: input.planSha256,
    primaryRootCid: input.primaryRootCid,
    rootCount: input.roots.length,
    rootsSha256,
    schemaVersion: artifactVersion,
  });
  const result: CandidateSourceSnapshotCarArtifactRecord = {
    blockCount: input.blockCount,
    blockMembershipSha256: input.blockMembershipSha256,
    carArtifactId,
    carBytes: input.carBytes,
    carRole: input.carRole,
    carSha256: input.carSha256,
    implementationCommitSha: input.implementationCommitSha,
    localValidationSha256,
    logicalMemberBytes,
    logicalMemberCount: input.members.length,
    logicalMembershipSha256,
    planId: input.planId,
    planSha256: input.planSha256,
    primaryRootCid: input.primaryRootCid,
    recordedAt: input.recordedAt,
    rootCount: input.roots.length,
    rootsSha256,
  };
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const existing = await transaction<
        {
          bucket_identity: string;
          car_sha256: string;
          expected_bucket_identity: string;
          implementation_commit_sha: string;
          local_validation_sha256: string;
          member_count: number;
          member_set_sha256: string;
          recorded_at: Date;
          root_count: number;
          root_set_sha256: string;
          rpc_endpoint: string;
        }[]
      >`
        SELECT artifact.bucket_identity, artifact.car_sha256,
               artifact.implementation_commit_sha,
               artifact.local_validation_sha256, artifact.member_count,
               artifact.member_set_sha256, artifact.recorded_at,
               artifact.root_count, artifact.root_set_sha256,
               artifact.rpc_endpoint,
               CASE artifact.car_role
                 WHEN 'open_data' THEN
                   plan.plan_payload #>> '{targets,openData,bucket}'
                 ELSE plan.plan_payload #>> '{targets,queryTable,bucket}'
               END AS expected_bucket_identity
        FROM oracle_candidate_source_snapshot_car_artifacts artifact
        JOIN oracle_candidate_source_snapshot_demo_plans plan
          ON plan.plan_id = artifact.plan_id
         AND plan.plan_sha256 = artifact.plan_sha256
        WHERE artifact.car_artifact_id = ${carArtifactId}
      `;
      if (existing[0]) {
        if (
          existing[0].car_sha256 !== input.carSha256 ||
          existing[0].bucket_identity !==
            existing[0].expected_bucket_identity ||
          existing[0].implementation_commit_sha !==
            input.implementationCommitSha ||
          existing[0].local_validation_sha256 !== localValidationSha256 ||
          existing[0].member_count !== input.members.length ||
          existing[0].member_set_sha256 !== logicalMembershipSha256 ||
          existing[0].root_count !== input.roots.length ||
          existing[0].root_set_sha256 !== rootsSha256 ||
          existing[0].rpc_endpoint !== carRpcEndpoint ||
          existing[0].recorded_at.toISOString() !== input.recordedAt
        ) {
          throw new DurableConflictError("CAR artifact replay conflicts");
        }
        return result;
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_artifacts (
          car_artifact_id, artifact_version, plan_id, plan_sha256, car_role,
          primary_root_cid, root_count, root_set_sha256, car_sha256, car_bytes,
          block_count, block_set_sha256, member_count, member_logical_bytes,
          member_set_sha256, local_validation_sha256,
          implementation_commit_sha, recorded_at
        ) VALUES (
          ${carArtifactId}, ${artifactVersion}, ${input.planId},
          ${input.planSha256}, ${input.carRole}, ${input.primaryRootCid},
          ${input.roots.length}, ${rootsSha256}, ${input.carSha256},
          ${input.carBytes}, ${input.blockCount},
          ${input.blockMembershipSha256}, ${input.members.length},
          ${logicalMemberBytes}, ${logicalMembershipSha256},
          ${localValidationSha256}, ${input.implementationCommitSha},
          ${input.recordedAt}
        )
      `;
      const roots = input.roots.map((rootCid, index) => {
        const member = memberByCid.get(rootCid)!;
        return {
          car_artifact_id: carArtifactId,
          domain: member.domain,
          plan_id: input.planId,
          remote_object_key: member.remoteObjectKey,
          root_cid: rootCid,
          root_ordinal: index + 1,
          root_role:
            index === 0 ? "approved_target" : "additional_planned_object",
        };
      });
      for (let offset = 0; offset < roots.length; offset += 1_000) {
        const chunk = roots.slice(offset, offset + 1_000);
        await transaction`
          INSERT INTO oracle_candidate_source_snapshot_car_roots ${transaction(
            chunk,
            "car_artifact_id",
            "plan_id",
            "root_ordinal",
            "root_role",
            "domain",
            "remote_object_key",
            "root_cid",
          )}
        `;
      }
      for (let offset = 0; offset < input.members.length; offset += 1_000) {
        const chunk = input.members
          .slice(offset, offset + 1_000)
          .map((member, index) => ({
            car_artifact_id: carArtifactId,
            domain: member.domain,
            expected_bytes: member.byteSize,
            expected_cid: member.expectedCid,
            expected_sha256: member.sha256,
            member_ordinal: offset + index + 1,
            plan_id: input.planId,
            remote_object_key: member.remoteObjectKey,
          }));
        await transaction`
          INSERT INTO oracle_candidate_source_snapshot_car_members ${transaction(
            chunk,
            "car_artifact_id",
            "plan_id",
            "member_ordinal",
            "domain",
            "remote_object_key",
            "expected_sha256",
            "expected_cid",
            "expected_bytes",
          )}
        `;
      }
      const validation = await transaction<
        {
          member_set_sha256: string;
          root_set_sha256: string;
        }[]
      >`
        SELECT oracle_css_car_root_set_sha256(${carArtifactId}) AS root_set_sha256,
               oracle_css_car_member_set_sha256(${carArtifactId}) AS member_set_sha256
      `;
      if (
        validation[0]?.root_set_sha256 !== rootsSha256 ||
        validation[0]?.member_set_sha256 !== logicalMembershipSha256
      ) {
        throw new DurableConflictError("CAR membership validation diverged");
      }
      return result;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const authorizationInputSchema = z
  .strictObject({
    approvalId: approvalIdSchema,
    authorizationStatement: z.string().min(1).max(32_768),
    authorizedAt: timestampSchema,
    endpoint: z.literal(carRpcEndpoint),
    humanReference: z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/),
    implementationCommitSha: commitSchema,
    importMethod: z.literal("rpc_dag_import"),
    ipnsAuthorized: z.literal(true),
    ipnsOrder: z.literal("open_data_then_query_table"),
    finalCredentialFreeVerificationAuthorized: z.literal(true),
    vercelDeploymentAuthorized: z.literal(true),
    maximumAttemptsPerArtifact: z.number().int().min(1).max(2),
    openDataBucketTokenSha256: sha256Schema,
    overallTimeoutMs: z.number().int().min(60_000).max(14_400_000),
    planId: planIdSchema,
    planRevision: z.number().int().positive(),
    planSha256: sha256Schema,
    queryTableBucketTokenSha256: sha256Schema,
    uploadClosureAuthorized: z.literal(true),
  })
  .superRefine((value, context) => {
    if (value.openDataBucketTokenSha256 === value.queryTableBucketTokenSha256) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "CAR import bucket credentials must be distinct",
      });
    }
    for (const requiredBinding of [
      value.openDataBucketTokenSha256,
      value.queryTableBucketTokenSha256,
      String(value.overallTimeoutMs),
    ]) {
      if (!value.authorizationStatement.includes(requiredBinding)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: "CAR import authorization statement omits transport binding",
        });
      }
    }
  });

export interface CandidateSourceSnapshotCarImportAuthorization {
  approvalId: string;
  artifactCount: number;
  artifactSetSha256: string;
  authorizationId: string;
  authorizationStatement: string;
  authorizationStatementSha256: string;
  authorizationVersion: "candidate-source-snapshot-car-authorization-v1";
  authorizedAt: string;
  endpoint: typeof carRpcEndpoint;
  hardSpendingCeilingUsd: 25;
  humanReference: string;
  implementationCommitSha: string;
  importMethod: "rpc_dag_import";
  ipnsAuthorized: true;
  ipnsOrder: "open_data_then_query_table";
  finalCredentialFreeVerificationAuthorized: true;
  vercelDeploymentAuthorized: true;
  maximumAttemptsPerArtifact: number;
  maximumTotalImportAttempts: number;
  openDataBucketIdentity: string;
  openDataBucketTokenSha256: string;
  overallTimeoutMs: number;
  planId: string;
  planRevision: number;
  planSha256: string;
  queryTableBucketIdentity: string;
  queryTableBucketTokenSha256: string;
  totalCarBytes: number;
  uploadClosureAuthorized: true;
}

/** Read-only exact authorization lookup for the closed CAR operator CLI. */
export async function loadCandidateSourceSnapshotCarImportAuthorization(
  databaseUrl: string,
  inputValue: { authorizationStatementSha256: string; planId: string },
): Promise<CandidateSourceSnapshotCarImportAuthorization | null> {
  const input = z
    .strictObject({
      authorizationStatementSha256: sha256Schema,
      planId: planIdSchema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    const rows = await sql<
      {
        approval_id: string;
        artifact_count: number;
        artifact_set_sha256: string;
        authorization_id: string;
        authorization_statement: string;
        authorization_statement_sha256: string;
        authorized_at: Date;
        endpoint: typeof carRpcEndpoint;
        final_credential_free_verification_authorized: boolean;
        hard_spending_ceiling_usd: string;
        human_reference: string;
        implementation_commit_sha: string;
        import_method: "rpc_dag_import";
        ipns_authorized: boolean;
        ipns_order: "open_data_then_query_table";
        maximum_attempts_per_artifact: number;
        maximum_total_import_attempts: number;
        open_data_bucket_identity: string;
        open_data_bucket_token_sha256: string;
        overall_timeout_ms: number;
        plan_revision: number;
        plan_sha256: string;
        query_table_bucket_identity: string;
        query_table_bucket_token_sha256: string;
        total_car_bytes: string;
        upload_closure_authorized: boolean;
        vercel_deployment_authorized: boolean;
      }[]
    >`
      SELECT car_authorization_id AS authorization_id, approval_id,
             artifact_count, artifact_set_sha256, authorization_statement,
             authorization_statement_sha256, authorized_at, endpoint,
             final_credential_free_verification_authorized,
             hard_spending_ceiling_usd::text, human_reference,
             implementation_commit_sha, import_method, ipns_authorized,
             ipns_order, maximum_attempts_per_artifact,
             maximum_total_import_attempts, open_data_bucket_identity,
             open_data_bucket_token_sha256, overall_timeout_ms, plan_revision,
             plan_sha256, query_table_bucket_identity,
             query_table_bucket_token_sha256, total_car_bytes::text,
             upload_closure_authorized, vercel_deployment_authorized
      FROM oracle_candidate_source_snapshot_car_import_authorizations
      WHERE plan_id = ${input.planId}
        AND authorization_statement_sha256 =
          ${input.authorizationStatementSha256}
    `;
    const row = rows[0];
    if (!row) return null;
    return {
      approvalId: row.approval_id,
      artifactCount: row.artifact_count,
      artifactSetSha256: row.artifact_set_sha256,
      authorizationId: row.authorization_id,
      authorizationStatement: row.authorization_statement,
      authorizationStatementSha256: row.authorization_statement_sha256,
      authorizationVersion: "candidate-source-snapshot-car-authorization-v1",
      authorizedAt: row.authorized_at.toISOString(),
      endpoint: row.endpoint,
      finalCredentialFreeVerificationAuthorized:
        row.final_credential_free_verification_authorized as true,
      hardSpendingCeilingUsd: Number(row.hard_spending_ceiling_usd) as 25,
      humanReference: row.human_reference,
      implementationCommitSha: row.implementation_commit_sha,
      importMethod: row.import_method,
      ipnsAuthorized: row.ipns_authorized as true,
      ipnsOrder: row.ipns_order,
      maximumAttemptsPerArtifact: row.maximum_attempts_per_artifact,
      maximumTotalImportAttempts: row.maximum_total_import_attempts,
      openDataBucketIdentity: row.open_data_bucket_identity,
      openDataBucketTokenSha256: row.open_data_bucket_token_sha256,
      overallTimeoutMs: row.overall_timeout_ms,
      planId: input.planId,
      planRevision: row.plan_revision,
      planSha256: row.plan_sha256,
      queryTableBucketIdentity: row.query_table_bucket_identity,
      queryTableBucketTokenSha256: row.query_table_bucket_token_sha256,
      totalCarBytes: Number(row.total_car_bytes),
      uploadClosureAuthorized: row.upload_closure_authorized as true,
      vercelDeploymentAuthorized: row.vercel_deployment_authorized as true,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

/** Read-only artifact lookup; roots are revalidated from the CAR bytes. */
export async function loadCandidateSourceSnapshotCarArtifactRecords(
  databaseUrl: string,
  planIdValue: string,
): Promise<(CandidateSourceSnapshotCarArtifactRecord & { roots: string[] })[]> {
  const planId = planIdSchema.parse(planIdValue);
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    const rows = await sql<
      {
        block_count: number;
        block_membership_sha256: string;
        car_artifact_id: string;
        car_bytes: string;
        car_role: "open_data" | "query_table";
        car_sha256: string;
        implementation_commit_sha: string;
        local_validation_sha256: string;
        member_count: number;
        member_logical_bytes: string;
        member_set_sha256: string;
        plan_sha256: string;
        primary_root_cid: string;
        recorded_at: Date;
        root_count: number;
        root_set_sha256: string;
        roots: string[];
      }[]
    >`
      SELECT car_artifact_id, car_role, plan_sha256,
             implementation_commit_sha, car_sha256, car_bytes::text,
             block_count, block_membership_sha256, primary_root_cid,
             root_count, root_set_sha256, member_count,
             member_logical_bytes::text, member_set_sha256,
             local_validation_sha256, recorded_at,
             (SELECT jsonb_agg(root.root_cid ORDER BY root.root_ordinal)
              FROM oracle_candidate_source_snapshot_car_roots root
              WHERE root.car_artifact_id = artifact.car_artifact_id) AS roots
      FROM oracle_candidate_source_snapshot_car_artifacts artifact
      WHERE artifact.plan_id = ${planId}
      ORDER BY CASE artifact.car_role WHEN 'open_data' THEN 0 ELSE 1 END
    `;
    return rows.map((row) => ({
      blockCount: row.block_count,
      blockMembershipSha256: row.block_membership_sha256,
      carArtifactId: row.car_artifact_id,
      carBytes: Number(row.car_bytes),
      carRole: row.car_role,
      carSha256: row.car_sha256,
      implementationCommitSha: row.implementation_commit_sha,
      localValidationSha256: row.local_validation_sha256,
      logicalMemberBytes: Number(row.member_logical_bytes),
      logicalMemberCount: row.member_count,
      logicalMembershipSha256: row.member_set_sha256,
      planId,
      planSha256: row.plan_sha256,
      primaryRootCid: row.primary_root_cid,
      recordedAt: row.recorded_at.toISOString(),
      rootCount: row.root_count,
      rootsSha256: row.root_set_sha256,
      roots: row.roots,
    }));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

/** Persists the one immutable human authorization before any CAR import. */
export async function recordCandidateSourceSnapshotCarImportAuthorization(
  databaseUrl: string,
  inputValue: z.input<typeof authorizationInputSchema>,
): Promise<CandidateSourceSnapshotCarImportAuthorization> {
  const input = authorizationInputSchema.parse(inputValue);
  const authorizationVersion =
    "candidate-source-snapshot-car-authorization-v1" as const;
  const authorizationStatementSha256 = sha256(input.authorizationStatement);
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const aggregates = await transaction<
        {
          artifact_count: number;
          artifact_set_sha256: string;
          open_data_bucket_identity: string;
          query_table_bucket_identity: string;
          total_car_bytes: string;
        }[]
      >`
        SELECT count(*)::integer AS artifact_count,
               coalesce(sum(car_bytes), 0)::text AS total_car_bytes,
               max(bucket_identity) FILTER (WHERE car_role = 'open_data')
                 AS open_data_bucket_identity,
               max(bucket_identity) FILTER (WHERE car_role = 'query_table')
                 AS query_table_bucket_identity,
               oracle_css_car_artifact_set_sha256(${input.planId})
                 AS artifact_set_sha256
        FROM oracle_candidate_source_snapshot_car_artifacts
        WHERE plan_id = ${input.planId}
      `;
      const aggregate = aggregates[0];
      if (!aggregate || aggregate.artifact_count < 1) {
        throw new DurableConflictError(
          "CAR authorization requires exact recorded artifacts",
        );
      }
      if (aggregate.artifact_count !== 2) {
        throw new DurableConflictError(
          "CAR authorization requires both exact domain artifacts",
        );
      }
      const authorizationId = deterministicId("snapshotdemocarauthorization", [
        authorizationVersion,
        input.planId,
        input.planSha256,
        aggregate.artifact_set_sha256,
        authorizationStatementSha256,
      ]);
      const result: CandidateSourceSnapshotCarImportAuthorization = {
        approvalId: input.approvalId,
        artifactCount: aggregate.artifact_count,
        artifactSetSha256: aggregate.artifact_set_sha256,
        authorizationId,
        authorizationStatement: input.authorizationStatement,
        authorizationStatementSha256,
        authorizationVersion,
        authorizedAt: input.authorizedAt,
        endpoint: input.endpoint,
        hardSpendingCeilingUsd: 25,
        humanReference: input.humanReference,
        implementationCommitSha: input.implementationCommitSha,
        importMethod: input.importMethod,
        ipnsAuthorized: input.ipnsAuthorized,
        ipnsOrder: input.ipnsOrder,
        finalCredentialFreeVerificationAuthorized:
          input.finalCredentialFreeVerificationAuthorized,
        vercelDeploymentAuthorized: input.vercelDeploymentAuthorized,
        maximumAttemptsPerArtifact: input.maximumAttemptsPerArtifact,
        maximumTotalImportAttempts:
          input.maximumAttemptsPerArtifact * aggregate.artifact_count,
        openDataBucketIdentity: aggregate.open_data_bucket_identity,
        openDataBucketTokenSha256: input.openDataBucketTokenSha256,
        overallTimeoutMs: input.overallTimeoutMs,
        planId: input.planId,
        planRevision: input.planRevision,
        planSha256: input.planSha256,
        queryTableBucketIdentity: aggregate.query_table_bucket_identity,
        queryTableBucketTokenSha256: input.queryTableBucketTokenSha256,
        totalCarBytes: Number(aggregate.total_car_bytes),
        uploadClosureAuthorized: true,
      };
      const existing = await transaction<
        {
          approval_id: string;
          authorization_statement: string;
          authorization_statement_sha256: string;
          authorized_at: Date;
          artifact_set_sha256: string;
          endpoint: string;
          final_credential_free_verification_authorized: boolean;
          hard_spending_ceiling_usd: string;
          human_reference: string;
          implementation_commit_sha: string;
          import_method: string;
          ipns_authorized: boolean;
          ipns_order: string;
          maximum_attempts_per_artifact: number;
          maximum_total_import_attempts: number;
          open_data_bucket_identity: string;
          open_data_bucket_token_sha256: string;
          overall_timeout_ms: number;
          plan_revision: number;
          plan_sha256: string;
          query_table_bucket_identity: string;
          query_table_bucket_token_sha256: string;
          upload_closure_authorized: boolean;
          vercel_deployment_authorized: boolean;
        }[]
      >`
        SELECT approval_id, authorization_statement,
               authorization_statement_sha256, authorized_at,
               artifact_set_sha256, endpoint, hard_spending_ceiling_usd::text,
               final_credential_free_verification_authorized,
               human_reference, implementation_commit_sha, import_method,
               ipns_authorized, ipns_order, maximum_attempts_per_artifact,
               maximum_total_import_attempts, open_data_bucket_identity,
               open_data_bucket_token_sha256, overall_timeout_ms,
               plan_revision, plan_sha256, query_table_bucket_identity,
               query_table_bucket_token_sha256,
               upload_closure_authorized, vercel_deployment_authorized
        FROM oracle_candidate_source_snapshot_car_import_authorizations
        WHERE car_authorization_id = ${authorizationId}
           OR plan_id = ${input.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].authorization_statement_sha256 !==
            authorizationStatementSha256 ||
          existing[0].authorization_statement !==
            input.authorizationStatement ||
          existing[0].approval_id !== input.approvalId ||
          existing[0].authorized_at.toISOString() !== input.authorizedAt ||
          existing[0].artifact_set_sha256 !== aggregate.artifact_set_sha256 ||
          existing[0].endpoint !== input.endpoint ||
          existing[0].final_credential_free_verification_authorized !==
            input.finalCredentialFreeVerificationAuthorized ||
          Number(existing[0].hard_spending_ceiling_usd) !== 25 ||
          existing[0].human_reference !== input.humanReference ||
          existing[0].implementation_commit_sha !==
            input.implementationCommitSha ||
          existing[0].import_method !== input.importMethod ||
          existing[0].ipns_authorized !== input.ipnsAuthorized ||
          existing[0].ipns_order !== input.ipnsOrder ||
          existing[0].maximum_attempts_per_artifact !==
            input.maximumAttemptsPerArtifact ||
          existing[0].maximum_total_import_attempts !==
            result.maximumTotalImportAttempts ||
          existing[0].open_data_bucket_identity !==
            result.openDataBucketIdentity ||
          existing[0].open_data_bucket_token_sha256 !==
            result.openDataBucketTokenSha256 ||
          existing[0].overall_timeout_ms !== result.overallTimeoutMs ||
          existing[0].plan_revision !== input.planRevision ||
          existing[0].plan_sha256 !== input.planSha256 ||
          existing[0].query_table_bucket_identity !==
            result.queryTableBucketIdentity ||
          existing[0].query_table_bucket_token_sha256 !==
            result.queryTableBucketTokenSha256 ||
          existing[0].upload_closure_authorized !== true ||
          existing[0].vercel_deployment_authorized !==
            input.vercelDeploymentAuthorized
        ) {
          throw new DurableConflictError("CAR authorization replay conflicts");
        }
        return result;
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_import_authorizations (
          car_authorization_id, authorization_version, plan_id, plan_sha256,
          plan_revision, approval_id, implementation_commit_sha,
          artifact_set_sha256, artifact_count, total_car_bytes, endpoint,
          import_method, maximum_attempts_per_artifact,
          maximum_total_import_attempts, hard_spending_ceiling_usd,
          open_data_bucket_token_sha256, query_table_bucket_token_sha256,
          overall_timeout_ms,
          upload_closure_authorized, ipns_authorized, ipns_order,
          final_credential_free_verification_authorized,
          vercel_deployment_authorized, authorization_statement,
          authorization_statement_sha256, human_reference, authorized_at
        ) VALUES (
          ${authorizationId}, ${authorizationVersion}, ${input.planId},
          ${input.planSha256}, ${input.planRevision}, ${input.approvalId},
          ${input.implementationCommitSha}, ${aggregate.artifact_set_sha256},
          ${aggregate.artifact_count}, ${aggregate.total_car_bytes},
          ${input.endpoint}, ${input.importMethod},
          ${input.maximumAttemptsPerArtifact},
          ${result.maximumTotalImportAttempts}, 25,
          ${input.openDataBucketTokenSha256},
          ${input.queryTableBucketTokenSha256}, ${input.overallTimeoutMs}, true,
          ${input.ipnsAuthorized}, ${input.ipnsOrder},
          ${input.finalCredentialFreeVerificationAuthorized},
          ${input.vercelDeploymentAuthorized}, ${input.authorizationStatement},
          ${authorizationStatementSha256}, ${input.humanReference},
          ${input.authorizedAt}
        )
      `;
      return result;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const gatewayEvidenceIdSchema = z
  .string()
  .regex(/^snapshotdemocargateway_[a-f0-9]{32}$/);
const gatewayEvidenceInputSchema = z.strictObject({
  attemptId: z.string().regex(/^snapshotdemocarattempt_[a-f0-9]{32}$/),
  observedAt: timestampSchema,
  outcomeId: z.string().regex(/^snapshotdemocaroutcome_[a-f0-9]{32}$/),
  providerHttpStatus: z.literal(200),
  providerRequestIdHash: sha256Schema.nullable(),
  rootBlockBytes: z
    .instanceof(Uint8Array)
    .refine(
      (value) => value.byteLength >= 1 && value.byteLength <= 1_048_576,
      "Official gateway root block is outside its byte bound",
    ),
});

export interface CandidateSourceSnapshotCarGatewayEvidence {
  artifactId: string;
  bucketIdentity: string;
  evidenceId: string;
  evidenceSha256: string;
  inspectionId: string | null;
  observedAt: string;
  outcomeId: string;
  planId: string;
  providerProofPath: "positive_inspection" | "verified_outcome";
  rootBlockBytes: number;
  rootBlockSha256: string;
  rootCid: string;
  rpcEndpoint: typeof carRpcEndpoint;
}

/**
 * Persists the bounded identity of an official-gateway raw root block. The raw
 * block is verified against the immutable CID in memory and is never stored.
 */
export async function recordCandidateSourceSnapshotCarGatewayEvidence(
  databaseUrl: string,
  inputValue: z.input<typeof gatewayEvidenceInputSchema>,
): Promise<CandidateSourceSnapshotCarGatewayEvidence> {
  const input = gatewayEvidenceInputSchema.parse(inputValue);
  const evidenceVersion =
    "candidate-source-snapshot-car-gateway-evidence-v1" as const;
  const rootBlock = Buffer.from(input.rootBlockBytes);
  const rootBlockSha256 = sha256(rootBlock);
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<
        {
          bucket_identity: string;
          car_artifact_id: string;
          final_recursive_pin_status: string | null;
          implementation_commit_sha: string;
          inspection_id: string | null;
          inspection_inspected_at: Date | null;
          inspection_result: string | null;
          inspection_sha256: string | null;
          inspection_observed_root_set_sha256: string | null;
          inspection_pin_status: string | null;
          inspection_root_status: string | null;
          observed_root_count: number | null;
          observed_root_set_sha256: string | null;
          outcome: string;
          outcome_sha256: string;
          plan_id: string;
          primary_root_cid: string;
          root_count: number;
          root_set_sha256: string;
          rpc_endpoint: typeof carRpcEndpoint;
        }[]
      >`
        SELECT artifact.car_artifact_id, artifact.plan_id,
               artifact.primary_root_cid, artifact.root_count,
               artifact.root_set_sha256, artifact.bucket_identity,
               artifact.rpc_endpoint, attempt.implementation_commit_sha,
               outcome.outcome, outcome.outcome_sha256,
               outcome.observed_root_count,
               outcome.observed_root_set_sha256,
               outcome.final_recursive_pin_status,
               inspection.car_import_inspection_id AS inspection_id,
               inspection.inspected_at AS inspection_inspected_at,
               inspection.inspection_result,
               inspection.inspection_sha256,
               inspection.observed_root_set_sha256
                 AS inspection_observed_root_set_sha256,
               inspection.pin_status AS inspection_pin_status,
               inspection.root_status AS inspection_root_status
        FROM oracle_candidate_source_snapshot_car_import_attempts attempt
        JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
          ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
        JOIN oracle_candidate_source_snapshot_car_artifacts artifact
          ON artifact.car_artifact_id = attempt.car_artifact_id
         AND artifact.plan_id = attempt.plan_id
        LEFT JOIN oracle_candidate_source_snapshot_car_import_inspections inspection
          ON inspection.car_import_attempt_id = attempt.car_import_attempt_id
         AND inspection.car_import_outcome_id = outcome.car_import_outcome_id
         AND inspection.car_artifact_id = artifact.car_artifact_id
         AND inspection.plan_id = artifact.plan_id
        WHERE attempt.car_import_attempt_id = ${input.attemptId}
          AND outcome.car_import_outcome_id = ${input.outcomeId}
        FOR SHARE OF attempt, outcome, artifact
      `;
      const binding = rows[0];
      const verifiedOutcome =
        binding?.outcome === "verified" &&
        binding.final_recursive_pin_status === "pinned" &&
        binding.observed_root_count === binding.root_count &&
        binding.observed_root_set_sha256 === binding.root_set_sha256 &&
        binding.inspection_id === null;
      const positiveInspection =
        binding?.outcome === "outcome_unknown" &&
        binding.final_recursive_pin_status === null &&
        binding.observed_root_count === null &&
        binding.observed_root_set_sha256 === null &&
        binding.inspection_id !== null &&
        binding.inspection_result === "present_exact" &&
        binding.inspection_root_status === "present_exact" &&
        binding.inspection_pin_status === "pinned" &&
        binding.inspection_observed_root_set_sha256 ===
          binding.root_set_sha256 &&
        binding.inspection_inspected_at !== null &&
        input.observedAt >= binding.inspection_inspected_at.toISOString();
      if (
        !binding ||
        (!verifiedOutcome && !positiveInspection) ||
        binding.rpc_endpoint !== carRpcEndpoint
      ) {
        throw new DurableConflictError(
          "Official gateway evidence requires an exact pinned provider result",
        );
      }
      const providerProofPath = positiveInspection
        ? ("positive_inspection" as const)
        : ("verified_outcome" as const);
      const parsedRoot = CID.parse(binding.primary_root_cid);
      if (
        parsedRoot.version !== 0 ||
        !Buffer.from(parsedRoot.multihash.digest).equals(
          Buffer.from(rootBlockSha256, "hex"),
        )
      ) {
        throw new DurableConflictError(
          "Official gateway root block does not match the immutable CID",
        );
      }
      const payload = {
        artifactId: binding.car_artifact_id,
        bucketIdentity: binding.bucket_identity,
        gatewayOrigin: "https://ipfs.filebase.io",
        gatewayPathPolicy: "immutable_cid_raw_block_v1",
        implementationCommitSha: binding.implementation_commit_sha,
        inspectionId: binding.inspection_id,
        inspectionSha256: binding.inspection_sha256,
        observedAt: input.observedAt,
        outcomeId: input.outcomeId,
        outcomeSha256: binding.outcome_sha256,
        planId: binding.plan_id,
        providerHttpStatus: input.providerHttpStatus,
        providerProofPath,
        providerRequestIdHash: input.providerRequestIdHash,
        rootBlockBytes: rootBlock.byteLength,
        rootBlockSha256,
        rootCid: binding.primary_root_cid,
        rpcEndpoint: binding.rpc_endpoint,
        schemaVersion: evidenceVersion,
        validationResult: "cid_verified",
      };
      const evidenceSha256 = canonicalJsonSha256(payload);
      const evidenceId = deterministicId("snapshotdemocargateway", [
        evidenceVersion,
        binding.plan_id,
        binding.car_artifact_id,
        input.outcomeId,
        evidenceSha256,
        binding.inspection_id ?? "",
      ]);
      const result: CandidateSourceSnapshotCarGatewayEvidence = {
        artifactId: binding.car_artifact_id,
        bucketIdentity: binding.bucket_identity,
        evidenceId,
        evidenceSha256,
        inspectionId: binding.inspection_id,
        observedAt: input.observedAt,
        outcomeId: input.outcomeId,
        planId: binding.plan_id,
        providerProofPath,
        rootBlockBytes: rootBlock.byteLength,
        rootBlockSha256,
        rootCid: binding.primary_root_cid,
        rpcEndpoint: binding.rpc_endpoint,
      };
      const existing = await transaction<
        { evidence_payload: postgres.JSONValue; evidence_sha256: string }[]
      >`
        SELECT evidence_payload, evidence_sha256
        FROM oracle_candidate_source_snapshot_car_gateway_evidence
        WHERE gateway_evidence_id = ${evidenceId}
           OR car_artifact_id = ${binding.car_artifact_id}
           OR car_import_outcome_id = ${input.outcomeId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].evidence_sha256 !== evidenceSha256 ||
          canonicalJson(existing[0].evidence_payload) !== canonicalJson(payload)
        ) {
          throw new DurableConflictError(
            "Official gateway evidence replay conflicts",
          );
        }
        return result;
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_gateway_evidence (
          gateway_evidence_id, evidence_version, car_import_attempt_id,
          car_import_outcome_id, car_artifact_id, plan_id, bucket_identity,
          car_import_inspection_id, inspection_sha256, provider_proof_path,
          rpc_endpoint, gateway_origin, gateway_path_policy, root_cid,
          provider_http_status, provider_request_id_hash, root_block_bytes,
          root_block_sha256, validation_result, observed_at,
          implementation_commit_sha, evidence_payload, evidence_sha256
        ) VALUES (
          ${evidenceId}, ${evidenceVersion}, ${input.attemptId},
          ${input.outcomeId}, ${binding.car_artifact_id}, ${binding.plan_id},
          ${binding.bucket_identity}, ${binding.inspection_id},
          ${binding.inspection_sha256}, ${providerProofPath},
          ${binding.rpc_endpoint},
          'https://ipfs.filebase.io', 'immutable_cid_raw_block_v1',
          ${binding.primary_root_cid}, ${input.providerHttpStatus},
          ${input.providerRequestIdHash}, ${rootBlock.byteLength},
          ${rootBlockSha256}, 'cid_verified', ${input.observedAt},
          ${binding.implementation_commit_sha},
          ${transaction.json(payload as postgres.JSONValue)}, ${evidenceSha256}
        )
      `;
      return result;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const importReceiptInputSchema = z.strictObject({
  authorizationId: z
    .string()
    .regex(/^snapshotdemocarauthorization_[a-f0-9]{32}$/),
  carArtifactId: z.string().regex(/^snapshotdemocar_[a-f0-9]{32}$/),
  gatewayEvidenceId: gatewayEvidenceIdSchema,
  implementationCommitSha: commitSchema,
  planId: planIdSchema,
  planSha256: sha256Schema,
  verificationTimestamp: timestampSchema,
});

export interface CandidateSourceSnapshotCarImportReceipt {
  carArtifactId: string;
  carImportReceiptId: string;
  gatewayEvidenceId: string;
  memberCount: number;
  memberLogicalBytes: number;
  memberSetSha256: string;
  planId: string;
  primaryRootCid: string;
  providerProofPath: "positive_inspection" | "verified_outcome";
  receiptSha256: string;
  reservedRequestCount: number;
  rootCount: number;
  rootSetSha256: string;
  verificationMethod: "car_import_recursively_pinned";
}

/** Records only a fully validated import, recursive pin, and root observation. */
export async function recordCandidateSourceSnapshotCarImportReceipt(
  databaseUrl: string,
  inputValue: z.input<typeof importReceiptInputSchema>,
): Promise<CandidateSourceSnapshotCarImportReceipt> {
  const input = importReceiptInputSchema.parse(inputValue);
  const receiptVersion =
    "candidate-source-snapshot-car-import-receipt-v1" as const;
  const verificationMethod = "car_import_recursively_pinned" as const;
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const rows = await transaction<
        {
          attempt_count: number;
          attempt_request_cost_usd: string;
          attempt_reserved_request_count: number;
          block_count: number;
          bucket_identity: string;
          bucket_token_sha256: string;
          car_bytes: string;
          car_sha256: string;
          car_import_outcome_id: string;
          evidence_sha256: string;
          gateway_evidence_id: string;
          gateway_observed_at: Date;
          inspection_id: string | null;
          inspection_sha256: string | null;
          member_count: number;
          member_logical_bytes: string;
          member_set_sha256: string;
          outcome_sha256: string;
          overall_timeout_ms: number;
          provider_request_id_hash: string | null;
          provider_proof_path: "positive_inspection" | "verified_outcome";
          primary_root_cid: string;
          root_count: number;
          root_set_sha256: string;
          rpc_endpoint: typeof carRpcEndpoint;
        }[]
      >`
        SELECT artifact.block_count, artifact.car_bytes::text,
               artifact.car_sha256, artifact.member_count,
               artifact.member_logical_bytes::text, artifact.member_set_sha256,
               artifact.primary_root_cid, artifact.root_count,
               artifact.root_set_sha256, artifact.bucket_identity,
               artifact.rpc_endpoint, outcome.car_import_outcome_id,
               outcome.outcome_sha256,
               CASE gateway.provider_proof_path
                 WHEN 'positive_inspection' THEN (
                   SELECT inspection.provider_request_id_hash
                   FROM oracle_candidate_source_snapshot_car_import_inspections inspection
                   WHERE inspection.car_import_inspection_id =
                     gateway.car_import_inspection_id
                 )
                 ELSE outcome.provider_request_id_hash
               END AS provider_request_id_hash,
               gateway.gateway_evidence_id, gateway.evidence_sha256,
               gateway.car_import_inspection_id AS inspection_id,
               gateway.inspection_sha256, gateway.provider_proof_path,
               gateway.observed_at AS gateway_observed_at,
               attempt.bucket_token_sha256, attempt.overall_timeout_ms,
               (SELECT count(*)::integer
                FROM oracle_candidate_source_snapshot_car_import_attempts counted
                WHERE counted.car_artifact_id = artifact.car_artifact_id
                  AND counted.plan_id = artifact.plan_id) AS attempt_count,
               (SELECT coalesce(sum(counted.request_cost_usd), 0)::text
                FROM oracle_candidate_source_snapshot_car_import_attempts counted
                WHERE counted.car_artifact_id = artifact.car_artifact_id
                  AND counted.plan_id = artifact.plan_id)
                 AS attempt_request_cost_usd
               ,(SELECT coalesce(sum(counted.reserved_request_count), 0)::integer
                 FROM oracle_candidate_source_snapshot_car_import_attempts counted
                 WHERE counted.car_artifact_id = artifact.car_artifact_id
                   AND counted.plan_id = artifact.plan_id)
                 AS attempt_reserved_request_count
        FROM oracle_candidate_source_snapshot_car_artifacts artifact
        JOIN oracle_candidate_source_snapshot_car_import_authorizations auth
          ON auth.car_authorization_id = ${input.authorizationId}
         AND auth.plan_id = artifact.plan_id
         AND auth.plan_sha256 = artifact.plan_sha256
         AND auth.implementation_commit_sha = ${input.implementationCommitSha}
        JOIN oracle_candidate_source_snapshot_car_gateway_evidence gateway
          ON gateway.gateway_evidence_id = ${input.gatewayEvidenceId}
         AND gateway.car_artifact_id = artifact.car_artifact_id
         AND gateway.plan_id = artifact.plan_id
        JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
          ON outcome.car_import_outcome_id = gateway.car_import_outcome_id
         AND outcome.car_artifact_id = artifact.car_artifact_id
         AND outcome.plan_id = artifact.plan_id
        JOIN oracle_candidate_source_snapshot_car_import_attempts attempt
          ON attempt.car_import_attempt_id = outcome.car_import_attempt_id
         AND attempt.car_artifact_id = artifact.car_artifact_id
         AND attempt.plan_id = artifact.plan_id
        WHERE artifact.car_artifact_id = ${input.carArtifactId}
          AND artifact.plan_id = ${input.planId}
          AND artifact.plan_sha256 = ${input.planSha256}
          AND artifact.rpc_endpoint = ${carRpcEndpoint}
          AND (
            (gateway.provider_proof_path = 'verified_outcome'
             AND outcome.outcome = 'verified'
             AND outcome.final_recursive_pin_status = 'pinned'
             AND outcome.observed_root_count = artifact.root_count
             AND outcome.observed_root_set_sha256 = artifact.root_set_sha256
             AND gateway.car_import_inspection_id IS NULL) OR
            (gateway.provider_proof_path = 'positive_inspection'
             AND outcome.outcome = 'outcome_unknown'
             AND EXISTS (
               SELECT 1
               FROM oracle_candidate_source_snapshot_car_import_inspections inspection
               WHERE inspection.car_import_inspection_id =
                       gateway.car_import_inspection_id
                 AND inspection.car_import_attempt_id =
                       outcome.car_import_attempt_id
                 AND inspection.car_import_outcome_id =
                       outcome.car_import_outcome_id
                 AND inspection.car_artifact_id = artifact.car_artifact_id
                 AND inspection.plan_id = artifact.plan_id
                 AND inspection.inspection_sha256 = gateway.inspection_sha256
                 AND inspection.inspection_result = 'present_exact'
                 AND inspection.root_status = 'present_exact'
                 AND inspection.pin_status = 'pinned'
                 AND inspection.observed_root_set_sha256 = artifact.root_set_sha256
             ))
          )
          AND gateway.validation_result = 'cid_verified'
          AND gateway.implementation_commit_sha =
            ${input.implementationCommitSha}
          AND gateway.root_cid = artifact.primary_root_cid
          AND gateway.bucket_identity = artifact.bucket_identity
          AND gateway.rpc_endpoint = artifact.rpc_endpoint
          AND attempt.attempt_sequence = (
            SELECT max(final_attempt.attempt_sequence)
            FROM oracle_candidate_source_snapshot_car_import_attempts final_attempt
            WHERE final_attempt.car_artifact_id = artifact.car_artifact_id
              AND final_attempt.plan_id = artifact.plan_id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM oracle_candidate_source_snapshot_car_import_attempts dangling
            LEFT JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes classified
              ON classified.car_import_attempt_id = dangling.car_import_attempt_id
            WHERE dangling.car_artifact_id = artifact.car_artifact_id
              AND dangling.plan_id = artifact.plan_id
              AND classified.car_import_outcome_id IS NULL
          )
        FOR SHARE OF artifact, auth, gateway, outcome, attempt
      `;
      const artifact = rows[0];
      if (!artifact) {
        throw new DurableConflictError("CAR import artifact is unavailable");
      }
      if (
        artifact.rpc_endpoint !== carRpcEndpoint ||
        input.verificationTimestamp < artifact.gateway_observed_at.toISOString()
      ) {
        throw new DurableConflictError(
          "CAR import receipt evidence is not final and exact",
        );
      }
      const providerEvidenceSetSha256 = canonicalJsonSha256({
        gatewayEvidenceSha256: artifact.evidence_sha256,
        providerInspectionSha256: artifact.inspection_sha256,
        providerOutcomeSha256: artifact.outcome_sha256,
        providerProofPath: artifact.provider_proof_path,
        schemaVersion: "candidate-source-snapshot-car-provider-evidence-set-v1",
      });
      const payload = {
        authorizationId: input.authorizationId,
        bucketIdentity: artifact.bucket_identity,
        bucketTokenSha256: artifact.bucket_token_sha256,
        carArtifactId: input.carArtifactId,
        carBytes: Number(artifact.car_bytes),
        carSha256: artifact.car_sha256,
        finalRecursivePinStatus: "pinned",
        gatewayEvidenceId: artifact.gateway_evidence_id,
        gatewayEvidenceSha256: artifact.evidence_sha256,
        implementationCommitSha: input.implementationCommitSha,
        overallTimeoutMs: artifact.overall_timeout_ms,
        memberCount: artifact.member_count,
        memberLogicalBytes: Number(artifact.member_logical_bytes),
        memberSetSha256: artifact.member_set_sha256,
        officialGatewayStatus: "verified",
        planId: input.planId,
        planSha256: input.planSha256,
        primaryRootCid: artifact.primary_root_cid,
        providerImportResult: "expected_root_set_returned",
        importAttemptCount: artifact.attempt_count,
        providerInspectionId: artifact.inspection_id,
        providerInspectionSha256: artifact.inspection_sha256,
        providerEvidenceSetSha256,
        providerOutcomeId: artifact.car_import_outcome_id,
        providerOutcomeSha256: artifact.outcome_sha256,
        providerPinIdHash: null,
        providerProofPath: artifact.provider_proof_path,
        providerRequestIdHash: artifact.provider_request_id_hash,
        requestCostUsd: Number(artifact.attempt_request_cost_usd),
        reservedRequestCount: artifact.attempt_reserved_request_count,
        rootBlockValidation: "cid_verified",
        rootCount: artifact.root_count,
        rootObservationSetSha256: artifact.root_set_sha256,
        rootSetSha256: artifact.root_set_sha256,
        rpcEndpoint: artifact.rpc_endpoint,
        schemaVersion: receiptVersion,
        verificationMethod,
        verificationTimestamp: input.verificationTimestamp,
      };
      const receiptSha256 = canonicalJsonSha256(payload);
      const carImportReceiptId = deterministicId("snapshotdemocarreceipt", [
        receiptVersion,
        input.planId,
        input.carArtifactId,
        receiptSha256,
      ]);
      const result: CandidateSourceSnapshotCarImportReceipt = {
        carArtifactId: input.carArtifactId,
        carImportReceiptId,
        gatewayEvidenceId: artifact.gateway_evidence_id,
        memberCount: artifact.member_count,
        memberLogicalBytes: Number(artifact.member_logical_bytes),
        memberSetSha256: artifact.member_set_sha256,
        planId: input.planId,
        primaryRootCid: artifact.primary_root_cid,
        providerProofPath: artifact.provider_proof_path,
        receiptSha256,
        reservedRequestCount: artifact.attempt_reserved_request_count,
        rootCount: artifact.root_count,
        rootSetSha256: artifact.root_set_sha256,
        verificationMethod,
      };
      const existing = await transaction<
        {
          provider_evidence_set_sha256: string;
          receipt_payload: postgres.JSONValue;
          receipt_sha256: string;
        }[]
      >`
        SELECT provider_evidence_set_sha256, receipt_payload, receipt_sha256
        FROM oracle_candidate_source_snapshot_car_import_receipts
        WHERE car_import_receipt_id = ${carImportReceiptId}
           OR car_artifact_id = ${input.carArtifactId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].receipt_sha256 !== receiptSha256 ||
          existing[0].provider_evidence_set_sha256 !==
            providerEvidenceSetSha256 ||
          canonicalJson(existing[0].receipt_payload) !== canonicalJson(payload)
        ) {
          throw new DurableConflictError("CAR import receipt replay conflicts");
        }
        return result;
      }
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_import_receipts (
          car_import_receipt_id, receipt_version, car_authorization_id,
          car_artifact_id, plan_id, plan_sha256, verification_method,
          car_sha256, car_bytes, primary_root_cid, root_count, root_set_sha256,
          root_observation_set_sha256, member_count, member_logical_bytes,
          member_set_sha256, provider_request_id_hash, provider_pin_id_hash,
          provider_evidence_set_sha256, provider_outcome_id,
          gateway_evidence_id, bucket_identity, rpc_endpoint,
          bucket_token_sha256, overall_timeout_ms, provider_proof_path,
          provider_inspection_id, provider_inspection_sha256,
          import_attempt_count, request_cost_usd, provider_import_result,
          reserved_request_count,
          final_recursive_pin_status, official_gateway_status,
          root_block_validation, verification_timestamp,
          implementation_commit_sha, receipt_payload, receipt_sha256
        ) VALUES (
          ${carImportReceiptId}, ${receiptVersion}, ${input.authorizationId},
          ${input.carArtifactId}, ${input.planId}, ${input.planSha256},
          ${verificationMethod}, ${artifact.car_sha256}, ${artifact.car_bytes},
          ${artifact.primary_root_cid}, ${artifact.root_count},
          ${artifact.root_set_sha256}, ${artifact.root_set_sha256},
          ${artifact.member_count}, ${artifact.member_logical_bytes},
          ${artifact.member_set_sha256}, ${artifact.provider_request_id_hash},
          ${null}, ${providerEvidenceSetSha256},
          ${artifact.car_import_outcome_id}, ${artifact.gateway_evidence_id},
          ${artifact.bucket_identity}, ${artifact.rpc_endpoint},
          ${artifact.bucket_token_sha256}, ${artifact.overall_timeout_ms},
          ${artifact.provider_proof_path}, ${artifact.inspection_id},
          ${artifact.inspection_sha256},
          ${artifact.attempt_count},
          ${artifact.attempt_request_cost_usd}, 'expected_root_set_returned',
          ${artifact.attempt_reserved_request_count}, 'pinned',
          'verified', 'cid_verified', ${input.verificationTimestamp},
          ${input.implementationCommitSha},
          ${transaction.json(payload as postgres.JSONValue)}, ${receiptSha256}
        )
      `;
      return result;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotCarBulkVerification {
  bulkVerificationId: string;
  carArtifactId: string;
  carImportReceiptId: string;
  memberCount: number;
  memberLogicalBytes: number;
  memberSetSha256: string;
  newlyVerifiedBytes: number;
  newlyVerifiedCount: number;
  planId: string;
  preservedVerifiedBytes: number;
  preservedVerifiedCount: number;
  resultSha256: string;
  verifiedAt: string;
}

/** Atomically verifies exactly the immutable CAR members, preserving old receipts. */
export async function bulkVerifyCandidateSourceSnapshotCarMembers(
  databaseUrl: string,
  inputValue: {
    carArtifactId: string;
    carImportReceiptId: string;
    planId: string;
    verifiedAt: string;
  },
): Promise<CandidateSourceSnapshotCarBulkVerification> {
  const input = z
    .strictObject({
      carArtifactId: z.string().regex(/^snapshotdemocar_[a-f0-9]{32}$/),
      carImportReceiptId: z
        .string()
        .regex(/^snapshotdemocarreceipt_[a-f0-9]{32}$/),
      planId: planIdSchema,
      verifiedAt: timestampSchema,
    })
    .parse(inputValue);
  const bulkVersion =
    "candidate-source-snapshot-car-bulk-verification-v1" as const;
  const verificationMethod = "car_import_recursively_pinned" as const;
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      await transaction`
        SELECT plan_id FROM oracle_candidate_source_snapshot_demo_plans
        WHERE plan_id = ${input.planId} FOR UPDATE
      `;
      const existing = await transaction<
        {
          bulk_verification_id: string;
          car_artifact_id: string;
          car_import_receipt_id: string;
          member_count: number;
          member_logical_bytes: string;
          member_set_sha256: string;
          newly_verified_bytes: string;
          newly_verified_count: number;
          preserved_verified_bytes: string;
          preserved_verified_count: number;
          result_sha256: string;
          plan_id: string;
          verified_at: Date;
        }[]
      >`
        SELECT bulk_verification_id, car_artifact_id, car_import_receipt_id,
               plan_id, member_count, member_logical_bytes::text,
               member_set_sha256, newly_verified_count,
               newly_verified_bytes::text, preserved_verified_count,
               preserved_verified_bytes::text, result_sha256, verified_at
        FROM oracle_candidate_source_snapshot_car_bulk_verifications
        WHERE car_import_receipt_id = ${input.carImportReceiptId}
           OR car_artifact_id = ${input.carArtifactId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].car_artifact_id !== input.carArtifactId ||
          existing[0].car_import_receipt_id !== input.carImportReceiptId ||
          existing[0].plan_id !== input.planId ||
          existing[0].verified_at.toISOString() !== input.verifiedAt
        ) {
          throw new DurableConflictError(
            "CAR bulk verification replay conflicts",
          );
        }
        return {
          bulkVerificationId: existing[0].bulk_verification_id,
          carArtifactId: input.carArtifactId,
          carImportReceiptId: input.carImportReceiptId,
          memberCount: existing[0].member_count,
          memberLogicalBytes: Number(existing[0].member_logical_bytes),
          memberSetSha256: existing[0].member_set_sha256,
          newlyVerifiedBytes: Number(existing[0].newly_verified_bytes),
          newlyVerifiedCount: existing[0].newly_verified_count,
          planId: input.planId,
          preservedVerifiedBytes: Number(existing[0].preserved_verified_bytes),
          preservedVerifiedCount: existing[0].preserved_verified_count,
          resultSha256: existing[0].result_sha256,
          verifiedAt: existing[0].verified_at.toISOString(),
        };
      }
      const aggregates = await transaction<
        {
          invalid_count: string;
          member_count: string;
          member_logical_bytes: string;
          member_set_sha256: string;
          newly_verified_bytes: string;
          newly_verified_count: string;
          preserved_verified_bytes: string;
          preserved_verified_count: string;
          receipt_sha256: string;
        }[]
      >`
        SELECT count(*)::text AS member_count,
          coalesce(sum(member.expected_bytes), 0)::text AS member_logical_bytes,
          max(artifact.member_set_sha256) AS member_set_sha256,
          count(*) FILTER (WHERE object.status = 'verified')::text
            AS preserved_verified_count,
          coalesce(sum(member.expected_bytes) FILTER (
            WHERE object.status = 'verified'), 0)::text
            AS preserved_verified_bytes,
          count(*) FILTER (WHERE object.status IN (
            'pending', 'outcome_unknown'))::text AS newly_verified_count,
          coalesce(sum(member.expected_bytes) FILTER (
            WHERE object.status IN ('pending', 'outcome_unknown')), 0)::text
            AS newly_verified_bytes,
          count(*) FILTER (WHERE object.status NOT IN (
            'pending', 'outcome_unknown', 'verified'))::text AS invalid_count,
          max(receipt.receipt_sha256) AS receipt_sha256
        FROM oracle_candidate_source_snapshot_car_members member
        JOIN oracle_candidate_source_snapshot_car_artifacts artifact
          ON artifact.car_artifact_id = member.car_artifact_id
        JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
          ON receipt.car_artifact_id = artifact.car_artifact_id
        JOIN oracle_candidate_source_snapshot_demo_objects object
          ON object.plan_id = member.plan_id AND object.domain = member.domain
         AND object.remote_object_key = member.remote_object_key
        WHERE member.car_artifact_id = ${input.carArtifactId}
          AND receipt.car_import_receipt_id = ${input.carImportReceiptId}
          AND member.plan_id = ${input.planId}
      `;
      const aggregate = aggregates[0];
      if (
        !aggregate ||
        Number(aggregate.invalid_count) !== 0 ||
        Number(aggregate.member_count) === 0
      ) {
        throw new DurableConflictError(
          "CAR bulk verification has unresolved exact members",
        );
      }
      const payload = {
        carArtifactId: input.carArtifactId,
        carImportReceiptId: input.carImportReceiptId,
        memberCount: Number(aggregate.member_count),
        memberLogicalBytes: Number(aggregate.member_logical_bytes),
        memberSetSha256: aggregate.member_set_sha256,
        newlyVerifiedBytes: Number(aggregate.newly_verified_bytes),
        newlyVerifiedCount: Number(aggregate.newly_verified_count),
        planId: input.planId,
        preservedVerifiedBytes: Number(aggregate.preserved_verified_bytes),
        preservedVerifiedCount: Number(aggregate.preserved_verified_count),
        schemaVersion: bulkVersion,
        verificationMethod,
        verifiedAt: input.verifiedAt,
      };
      const resultSha256 = canonicalJsonSha256(payload);
      const bulkVerificationId = deterministicId("snapshotdemocarbulk", [
        bulkVersion,
        input.planId,
        input.carArtifactId,
        input.carImportReceiptId,
        resultSha256,
      ]);
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_car_bulk_verifications (
          bulk_verification_id, bulk_version, car_import_receipt_id,
          car_artifact_id, plan_id, verification_method, member_set_sha256,
          member_count, member_logical_bytes, preserved_verified_count,
          preserved_verified_bytes, newly_verified_count, newly_verified_bytes,
          verified_at, result_payload, result_sha256
        ) VALUES (
          ${bulkVerificationId}, ${bulkVersion}, ${input.carImportReceiptId},
          ${input.carArtifactId}, ${input.planId}, ${verificationMethod},
          ${aggregate.member_set_sha256}, ${aggregate.member_count},
          ${aggregate.member_logical_bytes},
          ${aggregate.preserved_verified_count},
          ${aggregate.preserved_verified_bytes}, ${aggregate.newly_verified_count},
          ${aggregate.newly_verified_bytes}, ${input.verifiedAt},
          ${transaction.json(payload as postgres.JSONValue)}, ${resultSha256}
        )
      `;
      await transaction`
        UPDATE oracle_candidate_source_snapshot_demo_objects object
        SET status = 'verified', provider_cid = object.expected_cid,
            receipt_sha256 = ${aggregate.receipt_sha256},
            successful_effect_count = 1,
            car_verification_method = ${verificationMethod},
            car_artifact_id = ${input.carArtifactId},
            car_import_receipt_id = ${input.carImportReceiptId},
            revision = object.revision + 1
        FROM oracle_candidate_source_snapshot_car_members member
        WHERE member.car_artifact_id = ${input.carArtifactId}
          AND member.plan_id = object.plan_id AND member.domain = object.domain
          AND member.remote_object_key = object.remote_object_key
          AND object.status IN ('pending', 'outcome_unknown')
      `;
      return {
        bulkVerificationId,
        carArtifactId: input.carArtifactId,
        carImportReceiptId: input.carImportReceiptId,
        memberCount: payload.memberCount,
        memberLogicalBytes: payload.memberLogicalBytes,
        memberSetSha256: payload.memberSetSha256,
        newlyVerifiedBytes: payload.newlyVerifiedBytes,
        newlyVerifiedCount: payload.newlyVerifiedCount,
        planId: input.planId,
        preservedVerifiedBytes: payload.preservedVerifiedBytes,
        preservedVerifiedCount: payload.preservedVerifiedCount,
        resultSha256,
        verifiedAt: input.verifiedAt,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export interface CandidateSourceSnapshotCarUploadClosure {
  admittedRequestCostUsd: number;
  admittedRequestCount: number;
  approvalId: string;
  closureId: string;
  closureSha256: string;
  exactObjectCount: number;
  exactTotalBytes: number;
  planId: string;
  planSha256: string;
  verifiedAt: string;
}

/** Records closure with individual-request accounting plus final CAR imports. */
export async function recordCandidateSourceSnapshotCarUploadClosure(
  databaseUrl: string,
  inputValue: { approvalId: string; planId: string; planSha256: string },
): Promise<CandidateSourceSnapshotCarUploadClosure> {
  const input = z
    .strictObject({
      approvalId: approvalIdSchema,
      planId: planIdSchema,
      planSha256: sha256Schema,
    })
    .parse(inputValue);
  const sql = postgres(databaseUrl, { max: 1, onnotice: () => undefined });
  try {
    return await sql.begin(async (transaction) => {
      await lock(transaction);
      const existing = await transaction<
        {
          admitted_request_cost_usd: string;
          admitted_request_count: number;
          approval_id: string;
          closure_id: string;
          closure_sha256: string;
          exact_object_count: number;
          exact_total_bytes: string;
          plan_sha256: string;
          verified_at: Date;
        }[]
      >`
        SELECT closure_id, plan_sha256, approval_id, exact_object_count,
               exact_total_bytes::text, admitted_request_count,
               admitted_request_cost_usd::text, closure_sha256, verified_at
        FROM oracle_candidate_source_snapshot_demo_upload_closures
        WHERE plan_id = ${input.planId}
        FOR UPDATE
      `;
      if (existing[0]) {
        if (
          existing[0].plan_sha256 !== input.planSha256 ||
          existing[0].approval_id !== input.approvalId
        ) {
          throw new DurableConflictError("CAR upload closure replay conflicts");
        }
        return {
          admittedRequestCostUsd: Number(existing[0].admitted_request_cost_usd),
          admittedRequestCount: existing[0].admitted_request_count,
          approvalId: existing[0].approval_id,
          closureId: existing[0].closure_id,
          closureSha256: existing[0].closure_sha256,
          exactObjectCount: existing[0].exact_object_count,
          exactTotalBytes: Number(existing[0].exact_total_bytes),
          planId: input.planId,
          planSha256: existing[0].plan_sha256,
          verifiedAt: existing[0].verified_at.toISOString(),
        };
      }
      const rows = await transaction<
        {
          admitted_request_cost_usd: string;
          admitted_request_count: number;
          exact_object_count: string;
          exact_total_bytes: string;
          inventory_root_cid: string;
          inventory_root_sha256: string;
          mismatch_count: string;
          plan_sha256: string;
          unresolved_count: string;
          verified_at: Date | null;
        }[]
      >`
        SELECT plan.plan_sha256, plan.inventory_root_cid,
               plan.inventory_root_sha256,
               count(object.*)::text AS exact_object_count,
               coalesce(sum(object.expected_bytes), 0)::text AS exact_total_bytes,
               count(*) FILTER (WHERE object.status <> 'verified')::text
                 AS unresolved_count,
               count(*) FILTER (WHERE object.status = 'verified' AND
                 object.provider_cid IS DISTINCT FROM object.expected_cid)::text
                 AS mismatch_count,
               max(object.updated_at) FILTER (WHERE object.status = 'verified')
                 AS verified_at,
               accounting.request_count + coalesce((
                 SELECT sum(attempt.reserved_request_count)
                 FROM oracle_candidate_source_snapshot_car_import_attempts attempt
                 WHERE attempt.plan_id = plan.plan_id
               ), 0)::integer AS admitted_request_count,
               (accounting.request_cost_usd + coalesce((
                 SELECT sum(attempt.request_cost_usd)
                 FROM oracle_candidate_source_snapshot_car_import_attempts attempt
                 WHERE attempt.plan_id = plan.plan_id
               ), 0))::text AS admitted_request_cost_usd
        FROM oracle_candidate_source_snapshot_demo_plans plan
        JOIN oracle_candidate_source_snapshot_demo_accounting accounting
          ON accounting.plan_id = plan.plan_id
        JOIN oracle_candidate_source_snapshot_demo_objects object
          ON object.plan_id = plan.plan_id
        WHERE plan.plan_id = ${input.planId}
          AND plan.plan_sha256 = ${input.planSha256}
          AND EXISTS (
            SELECT 1
            FROM oracle_candidate_source_snapshot_demo_approvals approval
            WHERE approval.approval_id = ${input.approvalId}
              AND approval.plan_id = plan.plan_id
              AND approval.plan_sha256 = plan.plan_sha256
          )
        GROUP BY plan.plan_id, accounting.plan_id
      `;
      const row = rows[0];
      if (
        !row ||
        Number(row.unresolved_count) !== 0 ||
        Number(row.mismatch_count) !== 0 ||
        row.verified_at === null
      ) {
        throw new DurableConflictError(
          "CAR upload closure requires all exact objects verified",
        );
      }
      const verifiedAt = row.verified_at.toISOString();
      const payload = {
        admittedRequestCostUsd: Number(row.admitted_request_cost_usd),
        admittedRequestCount: row.admitted_request_count,
        approvalId: input.approvalId,
        exactObjectCount: Number(row.exact_object_count),
        exactTotalBytes: Number(row.exact_total_bytes),
        inventoryRootCid: row.inventory_root_cid,
        inventoryRootSha256: row.inventory_root_sha256,
        planId: input.planId,
        planSha256: input.planSha256,
        providerCidMismatchCount: Number(row.mismatch_count),
        unresolvedObjectCount: Number(row.unresolved_count),
        verifiedAt,
      };
      const closureSha256 = canonicalJsonSha256(payload);
      const closureId = deterministicId("snapshotdemouploadclosure", [
        "candidate-source-snapshot-upload-closure-v1",
        input.planId,
        closureSha256,
      ]);
      await transaction`
        INSERT INTO oracle_candidate_source_snapshot_demo_upload_closures (
          closure_id, plan_id, plan_sha256, approval_id, exact_object_count,
          exact_total_bytes, verified_object_count, verified_total_bytes,
          unresolved_object_count, provider_cid_mismatch_count,
          inventory_root_cid, inventory_root_sha256, admitted_request_count,
          admitted_request_cost_usd, closure_sha256, verified_at
        ) VALUES (
          ${closureId}, ${input.planId}, ${input.planSha256}, ${input.approvalId},
          ${payload.exactObjectCount}, ${payload.exactTotalBytes},
          ${payload.exactObjectCount}, ${payload.exactTotalBytes}, 0, 0,
          ${payload.inventoryRootCid}, ${payload.inventoryRootSha256},
          ${payload.admittedRequestCount}, ${payload.admittedRequestCostUsd},
          ${closureSha256}, ${verifiedAt}
        )
      `;
      return {
        admittedRequestCostUsd: payload.admittedRequestCostUsd,
        admittedRequestCount: payload.admittedRequestCount,
        approvalId: input.approvalId,
        closureId,
        closureSha256,
        exactObjectCount: payload.exactObjectCount,
        exactTotalBytes: payload.exactTotalBytes,
        planId: input.planId,
        planSha256: input.planSha256,
        verifiedAt,
      };
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
