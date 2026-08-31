import {
  validateCandidateSourceSnapshotDemoPlan,
  type CandidateSourceSnapshotDemoPlan,
} from "./candidate-source-snapshot-demo.js";
import { sha256 } from "../lib/hash.js";

export type CandidateSourceSnapshotIpnsDomain = "open_data" | "query_table";

export type CandidateSourceSnapshotIpnsClassification =
  "target" | "prior" | "split" | "unavailable" | "unexpected";

export interface CandidateSourceSnapshotIpnsIntent {
  approvalId: string;
  cutoverPosition: 1 | 2;
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  mutationAttemptCount: number;
  rollbackAttemptCount: number;
  planId: string;
  planSha256: string;
  priorCid: string;
  resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1";
  rollbackPosition: 1 | 2;
  state: string;
  targetCid: string;
  updateAttemptCount: number;
  uploadClosureId: string;
}

export interface CandidateSourceSnapshotIpnsReplayAuthorization {
  authorizationId: string;
  authorizationSha256: string;
  authorizationStatement: string;
  authorizedAttempt: number;
  authorizedAt: string;
  authorizerReference: string;
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  planId: string;
  planSha256: string;
  priorCid: string;
  targetCid: string;
}

export interface CandidateSourceSnapshotIpnsObservation {
  classification: CandidateSourceSnapshotIpnsClassification;
  observedCid?: string | null;
}

export interface CandidateSourceSnapshotIpnsMutationCommand {
  action: "mutate";
  attemptNumber: number;
  authorizationId: string | null;
  authorizationSha256: string | null;
  commandId: string;
  domain: CandidateSourceSnapshotIpnsDomain;
  intentId: string;
  planId: string;
  planSha256: string;
  priorCid: string;
  targetCid: string;
}

export interface CandidateSourceSnapshotIpnsRollbackCommand {
  action: "rollback";
  attemptNumber: 1 | 2 | 3;
  authorizationId: string;
  authorizationSha256: string;
  commandId: string;
  domain: "open_data";
  intentId: string;
  planId: string;
  planSha256: string;
  priorCid: string;
  targetCid: string;
}

export interface CandidateSourceSnapshotIpnsRollbackAuthorization {
  authorizationId: string;
  authorizationSha256: string;
  authorizationStatement: string;
  authorizedAttempt: 1 | 2 | 3;
  authorizedAt: string;
  authorizerReference: string;
  domain: "open_data";
  intentId: string;
  planId: string;
  planSha256: string;
  priorCid: string;
  targetCid: string;
}

type CandidateSourceSnapshotIpnsRetryAuthorization =
  | CandidateSourceSnapshotIpnsReplayAuthorization
  | CandidateSourceSnapshotIpnsRollbackAuthorization;

export function renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
  authorization: Pick<
    CandidateSourceSnapshotIpnsRetryAuthorization,
    | "authorizedAttempt"
    | "domain"
    | "intentId"
    | "planId"
    | "planSha256"
    | "priorCid"
    | "targetCid"
  >,
  direction: "rollback" | "update",
): string {
  return `I authorize candidate source-snapshot IPNS ${direction} attempt ${authorization.authorizedAttempt} for plan ${authorization.planId}, SHA-256 ${authorization.planSha256}, domain ${authorization.domain}, intent ${authorization.intentId}, immutable prior ${authorization.priorCid}, and approved target ${authorization.targetCid}.`;
}

function authorizationEnvelopeIsExact(
  authorization: CandidateSourceSnapshotIpnsRetryAuthorization,
  direction: "rollback" | "update",
): boolean {
  const authorizedAt = new Date(authorization.authorizedAt);
  const statement =
    renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
      authorization,
      direction,
    );
  return (
    /^[a-z0-9][a-z0-9_-]{2,127}$/.test(authorization.authorizerReference) &&
    !Number.isNaN(authorizedAt.valueOf()) &&
    authorizedAt.toISOString() === authorization.authorizedAt &&
    authorization.authorizationStatement === statement &&
    authorization.authorizationSha256 === sha256(statement)
  );
}

export type CandidateSourceSnapshotIpnsCommand =
  | CandidateSourceSnapshotIpnsMutationCommand
  | CandidateSourceSnapshotIpnsRollbackCommand;

export interface CandidateSourceSnapshotIpnsDurableAuthorization {
  authorizationId: string;
  authorizationSha256: string;
}

export interface CandidateSourceSnapshotIpnsBoundary {
  observeIdentity(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<CandidateSourceSnapshotIpnsObservation>;
  mutateAndObserve(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<CandidateSourceSnapshotIpnsObservation>;
  rollbackAndObserve(
    command: CandidateSourceSnapshotIpnsRollbackCommand,
  ): Promise<CandidateSourceSnapshotIpnsObservation>;
}

export interface CandidateSourceSnapshotIpnsJournal {
  beforeFreshnessObservation(
    command: CandidateSourceSnapshotIpnsCommand,
  ): Promise<void>;
  beforeMutation(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<CandidateSourceSnapshotIpnsDurableAuthorization | null>;
  beforeRollback(
    command: CandidateSourceSnapshotIpnsRollbackCommand,
  ): Promise<CandidateSourceSnapshotIpnsDurableAuthorization | null>;
  markRolledBack(
    command: CandidateSourceSnapshotIpnsRollbackCommand,
  ): Promise<void>;
  markVerified(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<void>;
  recordFreshnessObservation(input: {
    command: CandidateSourceSnapshotIpnsCommand;
    observation: Required<CandidateSourceSnapshotIpnsObservation>;
  }): Promise<void>;
  recordObservation(input: {
    command: CandidateSourceSnapshotIpnsCommand;
    observation: Required<CandidateSourceSnapshotIpnsObservation>;
  }): Promise<void>;
}

export interface CandidateSourceSnapshotIpnsControllerResult {
  openData: CandidateSourceSnapshotIpnsClassification | "not_attempted";
  planId: string;
  planSha256: string;
  queryTable: CandidateSourceSnapshotIpnsClassification | "not_attempted";
  reason:
    | "completed"
    | "open_data_prior_observed"
    | "open_data_split"
    | "open_data_unavailable"
    | "open_data_unexpected"
    | "query_table_prior_observed"
    | "query_table_split"
    | "query_table_unavailable"
    | "query_table_unexpected"
    | "rollback_target_observed"
    | "rollback_split"
    | "rollback_unavailable"
    | "rollback_unexpected";
  rollback: CandidateSourceSnapshotIpnsClassification | "not_attempted";
  status: "completed" | "rolled_back" | "stopped";
}

function expectedIntentTarget(
  plan: CandidateSourceSnapshotDemoPlan,
  domain: CandidateSourceSnapshotIpnsDomain,
) {
  return domain === "open_data"
    ? plan.targets.openData
    : plan.targets.queryTable;
}

function validateIntents(
  plan: CandidateSourceSnapshotDemoPlan,
  intents: readonly CandidateSourceSnapshotIpnsIntent[],
  approvalId: string,
  uploadClosureId: string,
): Readonly<
  Record<CandidateSourceSnapshotIpnsDomain, CandidateSourceSnapshotIpnsIntent>
> {
  if (intents.length !== 2) {
    throw new Error("Cutover requires exactly two IPNS intents");
  }
  const byDomain = new Map<
    CandidateSourceSnapshotIpnsDomain,
    CandidateSourceSnapshotIpnsIntent
  >();
  for (const intent of intents) {
    if (byDomain.has(intent.domain)) {
      throw new Error("Cutover intents contain a duplicate domain");
    }
    const target = expectedIntentTarget(plan, intent.domain);
    const cutoverPosition = intent.domain === "open_data" ? 1 : 2;
    const rollbackPosition = intent.domain === "query_table" ? 1 : 2;
    if (
      intent.approvalId !== approvalId ||
      intent.uploadClosureId !== uploadClosureId ||
      intent.resolverPolicy !==
        "candidate_source_snapshot_filebase_delegated_v1" ||
      intent.cutoverPosition !== cutoverPosition ||
      intent.rollbackPosition !== rollbackPosition ||
      intent.intentId.length === 0 ||
      ![
        "prior_confirmed",
        "verified",
        ...(intent.domain === "open_data" ? ["rollback_ambiguous"] : []),
        ...(intent.domain === "query_table"
          ? ["update_failed_prior_confirmed", "rolled_back"]
          : []),
      ].includes(intent.state) ||
      intent.planId !== plan.planId ||
      intent.planSha256 !== plan.planSha256 ||
      intent.priorCid !== target.priorCid ||
      intent.targetCid !== target.targetCid ||
      !Number.isInteger(intent.mutationAttemptCount) ||
      intent.mutationAttemptCount < 0 ||
      !Number.isInteger(intent.updateAttemptCount) ||
      intent.updateAttemptCount < 0 ||
      !Number.isInteger(intent.rollbackAttemptCount) ||
      intent.rollbackAttemptCount < 0 ||
      intent.mutationAttemptCount !==
        intent.updateAttemptCount + intent.rollbackAttemptCount
    ) {
      throw new Error("Cutover intent is not an exact recoverable plan intent");
    }
    byDomain.set(intent.domain, intent);
  }
  const openData = byDomain.get("open_data");
  const queryTable = byDomain.get("query_table");
  if (!openData || !queryTable) {
    throw new Error("Cutover requires both publication domains");
  }
  if (queryTable.state === "verified" && openData.state !== "verified") {
    throw new Error(
      "Query-table verification cannot precede open-data verification",
    );
  }
  if (
    openData.state === "rollback_ambiguous" &&
    ![
      "prior_confirmed",
      "update_failed_prior_confirmed",
      "rolled_back",
    ].includes(queryTable.state)
  ) {
    throw new Error(
      "Open-data rollback recovery requires conclusive query-table non-mutation",
    );
  }
  return { open_data: openData, query_table: queryTable };
}

function validateRollbackAuthorization(
  plan: CandidateSourceSnapshotDemoPlan,
  intent: CandidateSourceSnapshotIpnsIntent,
  authorization: CandidateSourceSnapshotIpnsRollbackAuthorization | undefined,
): CandidateSourceSnapshotIpnsRollbackAuthorization | null {
  if (!authorization) return null;
  if (
    !authorizationEnvelopeIsExact(authorization, "rollback") ||
    !/^snapshotdemoreplay_[a-f0-9]{32}$/.test(authorization.authorizationId) ||
    !/^[a-f0-9]{64}$/.test(authorization.authorizationSha256) ||
    authorization.authorizedAttempt !== intent.rollbackAttemptCount + 1 ||
    authorization.authorizedAttempt > 3 ||
    authorization.domain !== "open_data" ||
    authorization.intentId !== intent.intentId ||
    authorization.planId !== plan.planId ||
    authorization.planSha256 !== plan.planSha256 ||
    authorization.priorCid !== intent.priorCid ||
    authorization.targetCid !== intent.targetCid
  ) {
    throw new Error("IPNS rollback authorization is not exact");
  }
  return authorization;
}

function validateReplayAuthorizations(
  plan: CandidateSourceSnapshotDemoPlan,
  intents: Readonly<
    Record<CandidateSourceSnapshotIpnsDomain, CandidateSourceSnapshotIpnsIntent>
  >,
  authorizations: readonly CandidateSourceSnapshotIpnsReplayAuthorization[],
): ReadonlyMap<
  CandidateSourceSnapshotIpnsDomain,
  CandidateSourceSnapshotIpnsReplayAuthorization
> {
  const expected = Object.values(intents).filter(
    (intent) =>
      intent.state === "prior_confirmed" && intent.updateAttemptCount > 0,
  );
  if (authorizations.length !== expected.length) {
    throw new Error(
      "Every retried IPNS mutation requires one exact replay authorization",
    );
  }
  const seenDomains = new Set<CandidateSourceSnapshotIpnsDomain>();
  const byDomain = new Map<
    CandidateSourceSnapshotIpnsDomain,
    CandidateSourceSnapshotIpnsReplayAuthorization
  >();
  for (const authorization of authorizations) {
    const intent = intents[authorization.domain];
    if (
      !authorizationEnvelopeIsExact(authorization, "update") ||
      seenDomains.has(authorization.domain) ||
      !/^snapshotdemoreplay_[a-f0-9]{32}$/.test(
        authorization.authorizationId,
      ) ||
      !/^[a-f0-9]{64}$/.test(authorization.authorizationSha256) ||
      intent.updateAttemptCount === 0 ||
      authorization.authorizedAttempt !== intent.updateAttemptCount + 1 ||
      authorization.intentId !== intent.intentId ||
      authorization.planId !== plan.planId ||
      authorization.planSha256 !== plan.planSha256 ||
      authorization.priorCid !== intent.priorCid ||
      authorization.targetCid !== intent.targetCid
    ) {
      throw new Error(
        "IPNS replay authorization does not match the exact retry",
      );
    }
    seenDomains.add(authorization.domain);
    byDomain.set(authorization.domain, authorization);
  }
  return byDomain;
}

function mutationCommand(
  plan: CandidateSourceSnapshotDemoPlan,
  intent: CandidateSourceSnapshotIpnsIntent,
  authorization: CandidateSourceSnapshotIpnsReplayAuthorization | undefined,
): CandidateSourceSnapshotIpnsMutationCommand {
  const attemptNumber = intent.updateAttemptCount + 1;
  return {
    action: "mutate",
    attemptNumber,
    authorizationId: authorization?.authorizationId ?? null,
    authorizationSha256: authorization?.authorizationSha256 ?? null,
    commandId: `${plan.planId}:${intent.intentId}:mutate:${attemptNumber}`,
    domain: intent.domain,
    intentId: intent.intentId,
    planId: plan.planId,
    planSha256: plan.planSha256,
    priorCid: intent.priorCid,
    targetCid: intent.targetCid,
  };
}

function rollbackCommand(
  plan: CandidateSourceSnapshotDemoPlan,
  intent: CandidateSourceSnapshotIpnsIntent,
  authorization: CandidateSourceSnapshotIpnsRollbackAuthorization,
): CandidateSourceSnapshotIpnsRollbackCommand {
  return {
    action: "rollback",
    attemptNumber: authorization.authorizedAttempt,
    authorizationId: authorization.authorizationId,
    authorizationSha256: authorization.authorizationSha256,
    commandId: `${plan.planId}:${intent.intentId}:rollback:${authorization.authorizedAttempt}`,
    domain: "open_data",
    intentId: intent.intentId,
    planId: plan.planId,
    planSha256: plan.planSha256,
    priorCid: intent.priorCid,
    targetCid: intent.targetCid,
  };
}

function requireJournalAuthorization(
  command: CandidateSourceSnapshotIpnsCommand,
  authorization: CandidateSourceSnapshotIpnsDurableAuthorization | null,
): void {
  if (command.authorizationId === null) {
    if (authorization !== null || command.authorizationSha256 !== null) {
      throw new Error(
        "First IPNS mutation must not carry durable replay authorization",
      );
    }
    return;
  }
  if (
    command.authorizationSha256 === null ||
    authorization?.authorizationId !== command.authorizationId ||
    authorization.authorizationSha256 !== command.authorizationSha256
  ) {
    throw new Error(
      "IPNS journal did not confirm the exact durable authorization",
    );
  }
}

function validateObservation(
  observation: CandidateSourceSnapshotIpnsObservation,
  command: CandidateSourceSnapshotIpnsCommand,
): Required<CandidateSourceSnapshotIpnsObservation> {
  const observedCid = observation.observedCid ?? null;
  switch (observation.classification) {
    case "target":
      if (observedCid !== command.targetCid) {
        throw new Error(
          "Target observation does not contain the approved target CID",
        );
      }
      break;
    case "prior":
      if (observedCid !== command.priorCid) {
        throw new Error(
          "Prior observation does not contain the immutable prior CID",
        );
      }
      break;
    case "split":
    case "unavailable":
      if (observedCid !== null) {
        throw new Error("Ambiguous IPNS observations cannot assert one CID");
      }
      break;
    case "unexpected":
      if (
        observedCid === null ||
        observedCid === command.priorCid ||
        observedCid === command.targetCid
      ) {
        throw new Error("Unexpected observation must contain a third CID");
      }
      break;
  }
  return { classification: observation.classification, observedCid };
}

function stoppedResult(
  plan: CandidateSourceSnapshotDemoPlan,
  input: Pick<
    CandidateSourceSnapshotIpnsControllerResult,
    "openData" | "queryTable" | "reason" | "rollback"
  >,
): CandidateSourceSnapshotIpnsControllerResult {
  return {
    ...input,
    planId: plan.planId,
    planSha256: plan.planSha256,
    status: "stopped",
  };
}

async function mutate(
  command: CandidateSourceSnapshotIpnsMutationCommand,
  boundary: CandidateSourceSnapshotIpnsBoundary,
  journal: CandidateSourceSnapshotIpnsJournal,
): Promise<Required<CandidateSourceSnapshotIpnsObservation>> {
  await journal.beforeFreshnessObservation(command);
  const freshness = validateObservation(
    await boundary.observeIdentity(command.domain),
    command,
  );
  if (freshness.classification === "target" && command.attemptNumber === 1) {
    throw new Error(
      "IPNS target freshness requires a prior terminal update attempt",
    );
  }
  await journal.recordFreshnessObservation({ command, observation: freshness });
  if (freshness.classification !== "prior") {
    return freshness;
  }
  requireJournalAuthorization(command, await journal.beforeMutation(command));
  const observation = validateObservation(
    await boundary.mutateAndObserve(command),
    command,
  );
  await journal.recordObservation({ command, observation });
  if (observation.classification === "target") {
    await journal.markVerified(command);
  }
  return observation;
}

async function executeRollback(
  command: CandidateSourceSnapshotIpnsRollbackCommand,
  boundary: CandidateSourceSnapshotIpnsBoundary,
  journal: CandidateSourceSnapshotIpnsJournal,
): Promise<Required<CandidateSourceSnapshotIpnsObservation>> {
  await journal.beforeFreshnessObservation(command);
  const freshness = validateObservation(
    await boundary.observeIdentity(command.domain),
    command,
  );
  await journal.recordFreshnessObservation({ command, observation: freshness });
  if (freshness.classification !== "target") {
    return freshness;
  }
  requireJournalAuthorization(command, await journal.beforeRollback(command));
  const observation = validateObservation(
    await boundary.rollbackAndObserve(command),
    command,
  );
  await journal.recordObservation({ command, observation });
  if (observation.classification === "prior") {
    await journal.markRolledBack(command);
  }
  return observation;
}

export async function executeCandidateSourceSnapshotIpnsController(input: {
  approvalId: string;
  boundary: CandidateSourceSnapshotIpnsBoundary;
  executorEnabled: unknown;
  intents: readonly CandidateSourceSnapshotIpnsIntent[];
  journal: CandidateSourceSnapshotIpnsJournal;
  plan: CandidateSourceSnapshotDemoPlan;
  replayAuthorizations?: readonly CandidateSourceSnapshotIpnsReplayAuthorization[];
  rollbackAuthorization?: CandidateSourceSnapshotIpnsRollbackAuthorization;
  uploadClosureId: string;
}): Promise<CandidateSourceSnapshotIpnsControllerResult> {
  if (input.executorEnabled !== true) {
    throw new Error("Candidate IPNS executor is disabled");
  }
  const plan = validateCandidateSourceSnapshotDemoPlan(input.plan);
  const intents = validateIntents(
    plan,
    input.intents,
    input.approvalId,
    input.uploadClosureId,
  );
  if (intents.open_data.state === "rollback_ambiguous") {
    const rollbackAuthorization = validateRollbackAuthorization(
      plan,
      intents.open_data,
      input.rollbackAuthorization,
    );
    if (!rollbackAuthorization) {
      throw new Error(
        "Ambiguous rollback recovery requires exact durable rollback authorization",
      );
    }
    const command = rollbackCommand(
      plan,
      intents.open_data,
      rollbackAuthorization,
    );
    const observation = await executeRollback(
      command,
      input.boundary,
      input.journal,
    );
    if (observation.classification === "prior") {
      return {
        openData: "target",
        planId: plan.planId,
        planSha256: plan.planSha256,
        queryTable: "prior",
        reason: "query_table_prior_observed",
        rollback: "prior",
        status: "rolled_back",
      };
    }
    return stoppedResult(plan, {
      openData: "target",
      queryTable: "prior",
      reason: `rollback_${observation.classification === "target" ? "target_observed" : observation.classification}`,
      rollback: observation.classification,
    });
  }
  const replayAuthorizations = validateReplayAuthorizations(
    plan,
    intents,
    input.replayAuthorizations ?? [],
  );

  const openCommand = mutationCommand(
    plan,
    intents.open_data,
    replayAuthorizations.get("open_data"),
  );
  const openObservation =
    intents.open_data.state === "verified"
      ? ({
          classification: "target",
          observedCid: intents.open_data.targetCid,
        } as const)
      : await mutate(openCommand, input.boundary, input.journal);
  if (openObservation.classification !== "target") {
    return stoppedResult(plan, {
      openData: openObservation.classification,
      queryTable: "not_attempted",
      reason: `open_data_${openObservation.classification === "prior" ? "prior_observed" : openObservation.classification}`,
      rollback: "not_attempted",
    });
  }

  const queryCommand = mutationCommand(
    plan,
    intents.query_table,
    replayAuthorizations.get("query_table"),
  );
  const queryObservation =
    intents.query_table.state === "verified"
      ? ({
          classification: "target",
          observedCid: intents.query_table.targetCid,
        } as const)
      : await mutate(queryCommand, input.boundary, input.journal);
  if (queryObservation.classification === "target") {
    return {
      openData: "target",
      planId: plan.planId,
      planSha256: plan.planSha256,
      queryTable: "target",
      reason: "completed",
      rollback: "not_attempted",
      status: "completed",
    };
  }
  if (queryObservation.classification !== "prior") {
    return stoppedResult(plan, {
      openData: "target",
      queryTable: queryObservation.classification,
      reason: `query_table_${queryObservation.classification}`,
      rollback: "not_attempted",
    });
  }

  const rollbackAuthorization = validateRollbackAuthorization(
    plan,
    intents.open_data,
    input.rollbackAuthorization,
  );
  if (!rollbackAuthorization) {
    return stoppedResult(plan, {
      openData: "target",
      queryTable: "prior",
      reason: "query_table_prior_observed",
      rollback: "not_attempted",
    });
  }

  const rollback = rollbackCommand(
    plan,
    intents.open_data,
    rollbackAuthorization,
  );
  const rollbackObservation = await executeRollback(
    rollback,
    input.boundary,
    input.journal,
  );
  if (rollbackObservation.classification === "prior") {
    return {
      openData: "target",
      planId: plan.planId,
      planSha256: plan.planSha256,
      queryTable: "prior",
      reason: "query_table_prior_observed",
      rollback: "prior",
      status: "rolled_back",
    };
  }
  return stoppedResult(plan, {
    openData: "target",
    queryTable: "prior",
    reason: `rollback_${rollbackObservation.classification === "target" ? "target_observed" : rollbackObservation.classification}`,
    rollback: rollbackObservation.classification,
  });
}
