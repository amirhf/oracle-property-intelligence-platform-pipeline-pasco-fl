import { createReadStream } from "node:fs";
import { realpath, stat } from "node:fs/promises";
import path from "node:path";

import postgres from "postgres";
import { z } from "zod";

import { canonicalJson, canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import { deterministicId, sha256 } from "../lib/hash.js";
import {
  validatePublicationPlan,
  type PublicationArtifact,
  type PublicationPlan,
} from "../publication/plan.js";

export const PUBLICATION_STATES = [
  "prepared",
  "validated",
  "awaiting_configuration",
  "awaiting_approval",
  "approved",
  "executing",
  "completed",
  "failed_terminal",
] as const;

export type PublicationState = (typeof PUBLICATION_STATES)[number];
export type PublicationDomain = PublicationArtifact["domain"];

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const planIdSchema = z.string().regex(/^plan_[a-f0-9]{32}$/);
const publicationIdentitySchema = z.strictObject({
  county: z.literal("pasco"),
  planId: planIdSchema,
  planSha256: sha256Schema,
});
export const publicationApprovalRequestSchema =
  publicationIdentitySchema.extend({
    approverReference: z.string().regex(/^[a-z0-9][a-z0-9_-]{2,127}$/),
  });
export const publicationExecutionRequestSchema = publicationIdentitySchema;

export interface PublicationStateView {
  approvalId: string | null;
  approvedAt: string | null;
  approverReference: string | null;
  planId: string;
  planSha256: string;
  revision: number;
  state: PublicationState;
}

interface PublicationStateRow {
  approval_id: string | null;
  approved_at: Date | string | null;
  approver_reference: string | null;
  plan_id: string;
  plan_sha256: string;
  revision: number;
  state: PublicationState;
}

function terminalConflict(message: string): never {
  throw new DurableConflictError(`Publication conflict (${message})`);
}

function parse<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new DurableInputError(
      `${label} failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  return parsed.data;
}

// Stable two-key namespace for Publish/pasco:
// signedInt32(SHA-256("prism-oracle-publish-v1")[0..4]),
// signedInt32(SHA-256("pasco")[0..4]).
export function pascoPublishAdvisoryLockKey(): readonly [number, number] {
  const signedInt32 = (hex: string) => {
    const value = Number.parseInt(hex.slice(0, 8), 16);
    return value > 0x7fffffff ? value - 0x1_0000_0000 : value;
  };
  return [
    signedInt32(sha256("prism-oracle-publish-v1")),
    signedInt32(sha256("pasco")),
  ] as const;
}

async function lockPublish(
  transaction: postgres.TransactionSql,
): Promise<void> {
  const [namespaceKey, countyKey] = pascoPublishAdvisoryLockKey();
  await transaction`SELECT pg_advisory_xact_lock(${namespaceKey}, ${countyKey})`;
}

function iso(value: Date | string | null): string | null {
  return value === null ? null : new Date(value).toISOString();
}

function stateView(row: PublicationStateRow): PublicationStateView {
  return {
    approvalId: row.approval_id,
    approvedAt: iso(row.approved_at),
    approverReference: row.approver_reference,
    planId: row.plan_id,
    planSha256: row.plan_sha256,
    revision: row.revision,
    state: row.state,
  };
}

async function stateRows(
  sql: postgres.Sql | postgres.TransactionSql,
): Promise<PublicationStateRow[]> {
  return sql<PublicationStateRow[]>`
    SELECT state.plan_id, state.plan_sha256, state.state, state.revision,
           approval.approval_id, approval.approver_reference,
           approval.approved_at
    FROM oracle_publication_state state
    LEFT JOIN oracle_publication_approvals approval
      ON approval.plan_id = state.plan_id
    WHERE state.county = 'pasco'
  `;
}

async function recordStateEvent(
  transaction: postgres.TransactionSql,
  plan: PublicationPlan,
  fromState: PublicationState | null,
  toState: PublicationState,
  metadata: Record<string, unknown> = {},
): Promise<void> {
  const transition = {
    county: "pasco",
    fromState,
    metadata,
    planId: plan.planId,
    planSha256: plan.planSha256,
    toState,
  };
  const transitionSha256 = canonicalJsonSha256(transition);
  const eventId = deterministicId("pubstate", [
    "1.0.0",
    "Publish/pasco/state",
    plan.planId,
    toState,
    transitionSha256,
  ]);
  await transaction`
    INSERT INTO oracle_publication_state_events (
      event_id, county, plan_id, plan_sha256, from_state, to_state,
      transition_sha256, metadata
    ) VALUES (
      ${eventId}, 'pasco', ${plan.planId}, ${plan.planSha256}, ${fromState},
      ${toState}, ${transitionSha256},
      ${transaction.json(metadata as postgres.JSONValue)}
    )
    ON CONFLICT (plan_id, to_state, transition_sha256) DO NOTHING
  `;
}

async function loadStoredPlan(
  transaction: postgres.TransactionSql,
  planId: string,
  planSha256: string,
): Promise<PublicationPlan> {
  const rows = await transaction<
    { plan_payload: unknown; plan_sha256: string }[]
  >`
    SELECT plan_payload, plan_sha256 FROM oracle_publication_plans
    WHERE plan_id = ${planId}
  `;
  const row = rows[0];
  if (!row || row.plan_sha256 !== planSha256) {
    terminalConflict(`unknown or mismatched plan ${planId}`);
  }
  const plan = validatePublicationPlan(row.plan_payload);
  if (plan.planId !== planId || plan.planSha256 !== planSha256) {
    terminalConflict(`stored plan identity ${planId}`);
  }
  return plan;
}

export async function recordPublicationPlan(
  databaseUrl: string,
  value: unknown,
): Promise<PublicationStateView> {
  const plan = validatePublicationPlan(value);
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      await transaction`
        INSERT INTO oracle_publication_plans (
          plan_id, plan_sha256, plan_version, county, run_id, snapshot_id,
          coverage_mode, scope_id, approvable, executable, plan_payload,
          generated_at
        ) VALUES (
          ${plan.planId}, ${plan.planSha256}, ${plan.version}, 'pasco',
          ${plan.coverage.runId}, ${plan.coverage.sourceSnapshotId},
          ${plan.coverage.mode}, ${plan.coverage.scopeId}, ${plan.approvable},
          ${plan.executable},
          ${transaction.json(plan as unknown as postgres.JSONValue)},
          ${plan.generatedAt}
        )
        ON CONFLICT (plan_id) DO NOTHING
      `;
      const stored = await loadStoredPlan(
        transaction,
        plan.planId,
        plan.planSha256,
      );
      // generatedAt is intentionally excluded from plan identity. The first
      // recorded metadata timestamp remains durable while rebuilt plans with
      // identical identity replay safely.
      void stored;

      for (const artifact of plan.artifacts.objectInventory) {
        await transaction`
          INSERT INTO oracle_publication_object_effects (
            plan_id, domain, object_key, expected_sha256,
            expected_byte_size, status
          ) VALUES (
            ${plan.planId}, ${artifact.domain}, ${artifact.objectKey},
            ${artifact.sha256}, ${artifact.byteSize}, 'pending'
          )
          ON CONFLICT (plan_id, domain, object_key) DO NOTHING
        `;
      }
      const storedObjects = await transaction<
        {
          domain: PublicationDomain;
          expected_byte_size: string | number;
          expected_sha256: string;
          object_key: string;
        }[]
      >`
        SELECT domain, object_key, expected_sha256, expected_byte_size
        FROM oracle_publication_object_effects
        WHERE plan_id = ${plan.planId}
        ORDER BY domain, object_key
      `;
      const expectedObjects = [...plan.artifacts.objectInventory]
        .sort((left, right) =>
          `${left.domain}:${left.objectKey}`.localeCompare(
            `${right.domain}:${right.objectKey}`,
          ),
        )
        .map((artifact) => ({
          byteSize: artifact.byteSize,
          domain: artifact.domain,
          objectKey: artifact.objectKey,
          sha256: artifact.sha256,
        }));
      const actualObjects = storedObjects.map((artifact) => ({
        byteSize: Number(artifact.expected_byte_size),
        domain: artifact.domain,
        objectKey: artifact.object_key,
        sha256: artifact.expected_sha256,
      }));
      if (canonicalJson(actualObjects) !== canonicalJson(expectedObjects)) {
        terminalConflict(`object inventory ${plan.planId}`);
      }

      for (const [domain, target] of [
        ["open_data", plan.targets.openData],
        ["query_table", plan.targets.queryTable],
      ] as const) {
        await transaction`
          INSERT INTO oracle_publication_ipns_effects (
            plan_id, domain, ipns_label, ipns_network_key, status
          ) VALUES (
            ${plan.planId}, ${domain}, ${target.ipnsLabel},
            ${target.ipnsNetworkKey}, 'pending'
          )
          ON CONFLICT (plan_id, domain) DO NOTHING
        `;
      }

      const existingStateRows = await stateRows(transaction);
      const existingState = existingStateRows[0];
      if (!existingState || existingState.plan_id !== plan.planId) {
        const revision = (existingState?.revision ?? 0) + 1;
        const waitingState: PublicationState = plan.approvable
          ? "awaiting_approval"
          : "awaiting_configuration";
        await recordStateEvent(transaction, plan, null, "prepared");
        await recordStateEvent(transaction, plan, "prepared", "validated");
        await recordStateEvent(
          transaction,
          plan,
          "validated",
          waitingState,
          plan.approvable
            ? { configuration: "complete" }
            : { configuration: "incomplete" },
        );
        await transaction`
          INSERT INTO oracle_publication_state (
            county, plan_id, plan_sha256, state, revision
          ) VALUES (
            'pasco', ${plan.planId}, ${plan.planSha256}, ${waitingState},
            ${revision}
          )
          ON CONFLICT (county) DO UPDATE SET
            plan_id = EXCLUDED.plan_id,
            plan_sha256 = EXCLUDED.plan_sha256,
            state = EXCLUDED.state,
            revision = EXCLUDED.revision,
            terminal_reason = NULL,
            updated_at = now()
        `;
      }
      const result = (await stateRows(transaction))[0];
      if (!result) throw new Error("Publication state was not persisted");
      return stateView(result);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function getPublicationState(
  databaseUrl: string,
): Promise<PublicationStateView | null> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await stateRows(sql);
    return rows[0] ? stateView(rows[0]) : null;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function approvePublicationPlan(
  databaseUrl: string,
  value: unknown,
): Promise<PublicationStateView> {
  const request = parse(
    publicationApprovalRequestSchema,
    value,
    "Publish/pasco approval request",
  );
  const requestSha256 = canonicalJsonSha256(request);
  const approvalId = deterministicId("approval", [
    "1.0.0",
    "Publish/pasco/approve",
    request.planId,
    request.planSha256,
    request.approverReference,
  ]);
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const plan = await loadStoredPlan(
        transaction,
        request.planId,
        request.planSha256,
      );
      if (!plan.approvable || !plan.executable) {
        throw new DurableInputError(
          "Publication plan cannot be approved while configuration is incomplete",
        );
      }
      const current = (await stateRows(transaction))[0];
      if (
        !current ||
        current.plan_id !== request.planId ||
        current.plan_sha256 !== request.planSha256
      ) {
        terminalConflict("approval is not for the current plan");
      }
      const approvals = await transaction<
        {
          approval_id: string;
          request_sha256: string;
        }[]
      >`
        SELECT approval_id, request_sha256
        FROM oracle_publication_approvals
        WHERE plan_id = ${request.planId}
      `;
      const existing = approvals[0];
      if (existing) {
        if (
          existing.approval_id !== approvalId ||
          existing.request_sha256 !== requestSha256
        ) {
          terminalConflict(`approval payload ${request.planId}`);
        }
        return stateView(current);
      }
      if (current.state !== "awaiting_approval") {
        terminalConflict(`approval state ${current.state}`);
      }
      await transaction`
        INSERT INTO oracle_publication_approvals (
          plan_id, approval_id, plan_sha256, request_sha256,
          approver_reference
        ) VALUES (
          ${request.planId}, ${approvalId}, ${request.planSha256},
          ${requestSha256}, ${request.approverReference}
        )
      `;
      await recordStateEvent(
        transaction,
        plan,
        "awaiting_approval",
        "approved",
        { approvalId, approverReference: request.approverReference },
      );
      await transaction`
        UPDATE oracle_publication_state SET
          state = 'approved', revision = revision + 1, updated_at = now()
        WHERE county = 'pasco' AND plan_id = ${request.planId}
      `;
      const result = (await stateRows(transaction))[0];
      if (!result) throw new Error("Approved publication state is missing");
      return stateView(result);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

function inside(root: string, target: string): boolean {
  return target === root || target.startsWith(`${root}${path.sep}`);
}

async function fileSha256(filePath: string): Promise<string> {
  const hash = await import("node:crypto").then(({ createHash }) =>
    createHash("sha256"),
  );
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

export async function verifyPublicationPlanArtifacts(options: {
  dataDir: string;
  plan: PublicationPlan;
  publicationRootRelative: string;
}): Promise<void> {
  const plan = validatePublicationPlan(options.plan);
  if (
    path.isAbsolute(options.publicationRootRelative) ||
    options.publicationRootRelative.split(/[\\/]/).includes("..")
  ) {
    throw new DurableInputError("Publication root must be DATA_DIR-relative");
  }
  const dataRoot = await realpath(options.dataDir);
  const publicationRoot = await realpath(
    path.resolve(dataRoot, options.publicationRootRelative),
  );
  if (!inside(dataRoot, publicationRoot)) {
    throw new DurableInputError("Publication root escapes DATA_DIR");
  }
  const verify = async (artifact: PublicationArtifact) => {
    const relativePath =
      artifact.domain === "open_data"
        ? path.join("open-data", artifact.objectKey)
        : path.join("query", artifact.objectKey);
    const candidate = path.resolve(publicationRoot, relativePath);
    if (!inside(publicationRoot, candidate)) {
      throw new DurableInputError(
        `Publication artifact path escaped (${artifact.domain}:${artifact.objectKey})`,
      );
    }
    const resolved = await realpath(candidate);
    if (!inside(publicationRoot, resolved)) {
      throw new DurableInputError(
        `Publication artifact link escaped (${artifact.domain}:${artifact.objectKey})`,
      );
    }
    const metadata = await stat(resolved);
    const actualSha256 = await fileSha256(resolved);
    if (
      !metadata.isFile() ||
      metadata.size !== artifact.byteSize ||
      actualSha256 !== artifact.sha256
    ) {
      throw new DurableInputError(
        `Publication artifact binding changed (${artifact.domain}:${artifact.objectKey})`,
      );
    }
  };
  const inventory = plan.artifacts.objectInventory;
  for (let index = 0; index < inventory.length; index += 64) {
    await Promise.all(inventory.slice(index, index + 64).map(verify));
  }
}

export async function beginPublicationExecution(options: {
  dataDir: string;
  databaseUrl: string;
  publicationRootRelative: string;
  request: unknown;
}): Promise<PublicationStateView> {
  const request = parse(
    publicationExecutionRequestSchema,
    options.request,
    "Publish/pasco execution request",
  );
  const readSql = postgres(options.databaseUrl, { max: 1 });
  let plan: PublicationPlan;
  try {
    const plans = await readSql<{ plan_payload: unknown }[]>`
      SELECT plan_payload FROM oracle_publication_plans
      WHERE plan_id = ${request.planId} AND plan_sha256 = ${request.planSha256}
    `;
    if (!plans[0]) terminalConflict("execution plan is unknown");
    plan = validatePublicationPlan(plans[0]!.plan_payload);
  } finally {
    await readSql.end({ timeout: 5 });
  }
  await verifyPublicationPlanArtifacts({
    dataDir: options.dataDir,
    plan,
    publicationRootRelative: options.publicationRootRelative,
  });
  const sql = postgres(options.databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const lockedPlan = await loadStoredPlan(
        transaction,
        request.planId,
        request.planSha256,
      );
      if (!lockedPlan.approvable || !lockedPlan.executable) {
        throw new DurableInputError(
          "Publication execution is blocked by incomplete plan configuration",
        );
      }
      const current = (await stateRows(transaction))[0];
      const approvals = await transaction<{ plan_sha256: string }[]>`
        SELECT plan_sha256 FROM oracle_publication_approvals
        WHERE plan_id = ${request.planId}
      `;
      if (
        !current ||
        current.plan_id !== request.planId ||
        current.plan_sha256 !== request.planSha256 ||
        approvals[0]?.plan_sha256 !== request.planSha256
      ) {
        throw new DurableInputError(
          "Publication execution requires exact current-plan approval",
        );
      }
      if (current.state === "executing") return stateView(current);
      if (current.state !== "approved") {
        throw new DurableInputError(
          `Publication cannot execute from state ${current.state}`,
        );
      }
      await recordStateEvent(transaction, lockedPlan, "approved", "executing");
      await transaction`
        UPDATE oracle_publication_state SET
          state = 'executing', revision = revision + 1, updated_at = now()
        WHERE county = 'pasco' AND plan_id = ${request.planId}
      `;
      const result = (await stateRows(transaction))[0];
      if (!result) throw new Error("Executing publication state is missing");
      return stateView(result);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

const objectCheckpointSchema = publicationIdentitySchema.extend({
  cid: z.string().min(8).max(500),
  domain: z.enum(["open_data", "query_table"]),
  objectKey: z.string().min(1).max(2_048),
  sha256: sha256Schema,
});

async function checkpointObject(
  databaseUrl: string,
  value: unknown,
  stage: "uploaded" | "verified",
): Promise<void> {
  const request = parse(
    objectCheckpointSchema,
    value,
    `Publish/pasco object ${stage} checkpoint`,
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const current = (await stateRows(transaction))[0];
      if (
        !current ||
        current.plan_id !== request.planId ||
        current.plan_sha256 !== request.planSha256 ||
        current.state !== "executing"
      ) {
        terminalConflict(`object checkpoint is not for the executing plan`);
      }
      const rows = await transaction<
        {
          expected_sha256: string;
          status: "pending" | "uploaded" | "verified";
          uploaded_cid: string | null;
          verified_cid: string | null;
        }[]
      >`
        SELECT expected_sha256, status, uploaded_cid, verified_cid
        FROM oracle_publication_object_effects
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
          AND object_key = ${request.objectKey}
        FOR UPDATE
      `;
      const object = rows[0];
      if (!object || object.expected_sha256 !== request.sha256) {
        terminalConflict(
          `object binding ${request.domain}:${request.objectKey}`,
        );
      }
      if (stage === "uploaded") {
        if (object.status !== "pending") {
          if (object.uploaded_cid === request.cid) return;
          terminalConflict(
            `uploaded CID ${request.domain}:${request.objectKey}`,
          );
        }
        await transaction`
          UPDATE oracle_publication_object_effects SET
            status = 'uploaded', uploaded_cid = ${request.cid}
          WHERE plan_id = ${request.planId} AND domain = ${request.domain}
            AND object_key = ${request.objectKey} AND status = 'pending'
        `;
        return;
      }
      if (object.status === "verified") {
        if (object.verified_cid === request.cid) return;
        terminalConflict(`verified CID ${request.domain}:${request.objectKey}`);
      }
      if (object.status !== "uploaded" || object.uploaded_cid !== request.cid) {
        terminalConflict(`verification precedes upload ${request.objectKey}`);
      }
      await transaction`
        UPDATE oracle_publication_object_effects SET
          status = 'verified', verified_cid = ${request.cid}, completed_at = now()
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
          AND object_key = ${request.objectKey} AND status = 'uploaded'
      `;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export const checkpointPublicationObjectUploaded = (
  databaseUrl: string,
  value: unknown,
) => checkpointObject(databaseUrl, value, "uploaded");

export const checkpointPublicationObjectVerified = (
  databaseUrl: string,
  value: unknown,
) => checkpointObject(databaseUrl, value, "verified");

const ipnsCheckpointSchema = publicationIdentitySchema.extend({
  domain: z.enum(["open_data", "query_table"]),
  networkKey: z.string().min(1).max(500),
  priorCid: z.string().min(8).max(500).nullable(),
  targetCid: z.string().min(8).max(500),
});

function rootObjectKey(domain: PublicationDomain): string {
  return domain === "open_data"
    ? "index.json"
    : "query-tables/pasco/query-table.parquet";
}

export async function publicationIpnsUpdateReady(
  databaseUrl: string,
  value: unknown,
): Promise<{ objectCount: number; targetCid: string }> {
  const request = parse(
    publicationIdentitySchema.extend({
      domain: z.enum(["open_data", "query_table"]),
    }),
    value,
    "Publish/pasco IPNS readiness request",
  );
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const states = await sql<
      { plan_id: string; plan_sha256: string; state: PublicationState }[]
    >`
      SELECT plan_id, plan_sha256, state FROM oracle_publication_state
      WHERE county = 'pasco'
    `;
    if (
      states[0]?.plan_id !== request.planId ||
      states[0]?.plan_sha256 !== request.planSha256 ||
      states[0]?.state !== "executing"
    ) {
      throw new DurableInputError(
        "IPNS readiness requires the exact executing publication plan",
      );
    }
    const rows = await sql<
      { pending: number; root_cid: string | null; total: number }[]
    >`
      SELECT
        count(*)::int AS total,
        count(*) FILTER (WHERE status != 'verified')::int AS pending,
        max(verified_cid) FILTER (
          WHERE object_key = ${rootObjectKey(request.domain)}
        ) AS root_cid
      FROM oracle_publication_object_effects
      WHERE plan_id = ${request.planId} AND domain = ${request.domain}
    `;
    const row = rows[0];
    if (!row || row.total === 0 || row.pending !== 0 || row.root_cid === null) {
      throw new DurableInputError(
        `IPNS ${request.domain} effect requires every object and root CID to be verified`,
      );
    }
    return { objectCount: row.total, targetCid: row.root_cid };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function checkpointPublicationIpnsUpdated(
  databaseUrl: string,
  value: unknown,
): Promise<void> {
  const request = parse(
    ipnsCheckpointSchema,
    value,
    "Publish/pasco IPNS update checkpoint",
  );
  const ready = await publicationIpnsUpdateReady(databaseUrl, {
    county: request.county,
    domain: request.domain,
    planId: request.planId,
    planSha256: request.planSha256,
  });
  if (ready.targetCid !== request.targetCid) {
    terminalConflict(`IPNS target CID ${request.domain}`);
  }
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const effects = await transaction<
        {
          ipns_network_key: string | null;
          prior_cid: string | null;
          status: "pending" | "updated" | "verified";
          target_cid: string | null;
        }[]
      >`
        SELECT status, ipns_network_key, prior_cid, target_cid
        FROM oracle_publication_ipns_effects
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
        FOR UPDATE
      `;
      const effect = effects[0];
      if (!effect) terminalConflict(`missing IPNS effect ${request.domain}`);
      if (effect.status !== "pending") {
        if (
          effect.target_cid === request.targetCid &&
          effect.prior_cid === request.priorCid &&
          effect.ipns_network_key === request.networkKey
        ) {
          return;
        }
        terminalConflict(`IPNS update replay ${request.domain}`);
      }
      if (
        effect.ipns_network_key !== null &&
        effect.ipns_network_key !== request.networkKey
      ) {
        terminalConflict(`IPNS network key ${request.domain}`);
      }
      await transaction`
        UPDATE oracle_publication_ipns_effects SET
          ipns_network_key = ${request.networkKey},
          prior_cid = ${request.priorCid}, target_cid = ${request.targetCid},
          status = 'updated', mutation_performed = true, updated_at = now()
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
          AND status = 'pending'
      `;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function verifyPublicationIpnsResolution(
  databaseUrl: string,
  value: unknown,
): Promise<void> {
  const request = parse(
    publicationIdentitySchema.extend({
      domain: z.enum(["open_data", "query_table"]),
      resolvedCid: z.string().min(8).max(500),
    }),
    value,
    "Publish/pasco IPNS resolution verification",
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const effects = await transaction<
        {
          status: "pending" | "updated" | "verified";
          target_cid: string | null;
        }[]
      >`
        SELECT status, target_cid FROM oracle_publication_ipns_effects
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
        FOR UPDATE
      `;
      const effect = effects[0];
      if (!effect || effect.target_cid !== request.resolvedCid) {
        terminalConflict(`IPNS public resolution ${request.domain}`);
      }
      if (effect.status === "verified") return;
      if (effect.status !== "updated") {
        throw new DurableInputError(
          `IPNS ${request.domain} cannot verify before mutation`,
        );
      }
      await transaction`
        UPDATE oracle_publication_ipns_effects SET
          status = 'verified', public_resolution_verified = true,
          updated_at = now()
        WHERE plan_id = ${request.planId} AND domain = ${request.domain}
          AND status = 'updated'
      `;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function completePublicationPlan(
  databaseUrl: string,
  value: unknown,
): Promise<PublicationStateView> {
  const request = parse(
    publicationIdentitySchema,
    value,
    "Publish/pasco completion request",
  );
  const sql = postgres(databaseUrl, { max: 2 });
  try {
    return await sql.begin(async (transaction) => {
      await lockPublish(transaction);
      const plan = await loadStoredPlan(
        transaction,
        request.planId,
        request.planSha256,
      );
      const current = (await stateRows(transaction))[0];
      const effects = await transaction<{ verified: number }[]>`
        SELECT count(*) FILTER (
          WHERE status = 'verified' AND public_resolution_verified
        )::int AS verified
        FROM oracle_publication_ipns_effects
        WHERE plan_id = ${request.planId}
      `;
      if (
        !current ||
        current.plan_id !== request.planId ||
        current.state !== "executing" ||
        effects[0]?.verified !== 2
      ) {
        throw new DurableInputError(
          "Publication completion requires both public IPNS resolutions",
        );
      }
      await recordStateEvent(transaction, plan, "executing", "completed");
      await transaction`
        UPDATE oracle_publication_state SET
          state = 'completed', revision = revision + 1, updated_at = now()
        WHERE county = 'pasco' AND plan_id = ${request.planId}
      `;
      const result = (await stateRows(transaction))[0];
      if (!result) throw new Error("Completed publication state is missing");
      return stateView(result);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
