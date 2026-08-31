import { HeadBucketCommand, S3Client } from "@aws-sdk/client-s3";
import postgres from "postgres";

import {
  admitCandidateSourceSnapshotPreflightRequest,
  loadCandidateSourceSnapshotIpnsIntentState,
  PostgresCandidateSourceSnapshotUploadJournal,
  recordCandidateSourceSnapshotPreflightCycleOutcomes,
  recordCandidateSourceSnapshotPreflightRequestOutcome,
  type CandidateSourceSnapshotIpnsAttemptAdmission,
  type CandidateSourceSnapshotIpnsIntentStateRecord,
  type CandidateSourceSnapshotPreflightRequestAdmission,
  type CandidateSourceSnapshotRemoteRequestAdmission,
} from "../db/candidate-source-snapshot-demo.js";
import {
  recordCandidateSourceSnapshotRemoteVerification,
  transitionCandidateSourceSnapshotIpnsIntent,
} from "../db/candidate-source-snapshot-completion.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { sha256 } from "../lib/hash.js";
import { observeDelegatedIpnsRecord } from "./delegated-ipns.js";
import {
  CandidateSourceSnapshotFilebaseIpnsAdapter,
  type CandidateSourceSnapshotDelegatedIpnsReceipt,
  type CandidateSourceSnapshotFilebaseIpnsEvidence,
  type CandidateSourceSnapshotFilebaseIpnsReceipt,
  type CandidateSourceSnapshotIpnsAggregateEvidence,
  type CandidateSourceSnapshotIpnsRequestAdmission,
} from "./candidate-source-snapshot-filebase-ipns.js";
import type { CandidateSourceSnapshotDemoPlan } from "./candidate-source-snapshot-demo.js";
import type { CandidateSourceSnapshotLocalObjectSource } from "./candidate-source-snapshot-filebase.js";
import type { EnabledCandidateSourceSnapshotExecutionConfig } from "./candidate-source-snapshot-executor-config.js";
import type {
  CandidateSourceSnapshotIpnsCommand,
  CandidateSourceSnapshotIpnsDomain,
  CandidateSourceSnapshotIpnsDurableAuthorization,
  CandidateSourceSnapshotIpnsIntent,
  CandidateSourceSnapshotIpnsJournal,
  CandidateSourceSnapshotIpnsMutationCommand,
  CandidateSourceSnapshotIpnsRollbackCommand,
  CandidateSourceSnapshotIpnsRollbackAuthorization,
} from "./candidate-source-snapshot-ipns-controller.js";
import type {
  CandidateSourceSnapshotSession2RemoteRuntime,
  CandidateSourceSnapshotSession2RemoteRuntimeFactory,
} from "./candidate-source-snapshot-session2.js";
import {
  PostgresCandidateSourceSnapshotCredentialFreeVerifier,
  type CandidateSourceSnapshotCredentialFreeVerifier,
} from "./candidate-source-snapshot-remote-verifier.js";

type IntentState = CandidateSourceSnapshotIpnsIntentStateRecord["state"];

interface BucketEvidence {
  completedAt: string;
  outcome:
    | "succeeded"
    | "absent"
    | "retryable_failure"
    | "timeout_unknown"
    | "terminal_failure";
  receiptSha256: string;
}

export interface CandidateSourceSnapshotBucketProbe {
  close(): void;
  headBucket(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<BucketEvidence>;
}

class AwsCandidateSourceSnapshotBucketProbe implements CandidateSourceSnapshotBucketProbe {
  readonly #client: S3Client;
  readonly #config: EnabledCandidateSourceSnapshotExecutionConfig;

  constructor(config: EnabledCandidateSourceSnapshotExecutionConfig) {
    this.#config = config;
    this.#client = new S3Client({
      credentials: {
        accessKeyId: config.s3AccessKeyId,
        secretAccessKey: config.s3SecretAccessKey,
      },
      endpoint: config.s3Endpoint,
      forcePathStyle: true,
      maxAttempts: 1,
      region: "us-east-1",
    });
  }

  close(): void {
    this.#client.destroy();
  }

  async headBucket(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): Promise<BucketEvidence> {
    const completedAt = new Date().toISOString();
    const bucket =
      domain === "open_data"
        ? this.#config.targets.openData.bucket
        : this.#config.targets.queryTable.bucket;
    try {
      const output = await this.#client.send(
        new HeadBucketCommand({ Bucket: bucket }),
        {
          abortSignal: AbortSignal.timeout(
            this.#config.limits.requestTimeoutMs,
          ),
        },
      );
      if ((output.$metadata.attempts ?? 1) !== 1) {
        throw new Error("S3 client performed an unjournaled internal retry");
      }
      const payload = {
        domain,
        httpStatus: output.$metadata.httpStatusCode ?? 200,
        outcome: "succeeded" as const,
        providerRequestIdHash: output.$metadata.requestId
          ? sha256(output.$metadata.requestId)
          : null,
        schemaVersion: "candidate-source-snapshot-bucket-preflight-v1",
      };
      return {
        completedAt,
        outcome: payload.outcome,
        receiptSha256: canonicalJsonSha256(payload),
      };
    } catch (error) {
      const value = error as {
        $metadata?: { httpStatusCode?: number };
        code?: string;
        name?: string;
      };
      const status = value.$metadata?.httpStatusCode ?? null;
      const timeout =
        value.name === "AbortError" ||
        value.name === "TimeoutError" ||
        value.code === "ABORT_ERR";
      const retryable =
        status === 429 ||
        (status !== null && [500, 502, 503, 504].includes(status)) ||
        [
          "ECONNABORTED",
          "ECONNRESET",
          "EHOSTUNREACH",
          "ENETUNREACH",
          "ENOTFOUND",
          "ETIMEDOUT",
        ].includes(value.code ?? "");
      const outcome = timeout
        ? ("timeout_unknown" as const)
        : status === 404
          ? ("absent" as const)
          : retryable
            ? ("retryable_failure" as const)
            : ("terminal_failure" as const);
      return {
        completedAt,
        outcome,
        receiptSha256: canonicalJsonSha256({
          domain,
          httpStatus: status,
          outcome,
          schemaVersion: "candidate-source-snapshot-bucket-preflight-v1",
        }),
      };
    }
  }
}

type PendingAdmission =
  | {
      kind: "preflight";
      value: CandidateSourceSnapshotPreflightRequestAdmission;
    }
  | {
      kind: "resolution";
      value: CandidateSourceSnapshotRemoteRequestAdmission;
    };

function preflightAdmission(
  pending: PendingAdmission,
): CandidateSourceSnapshotPreflightRequestAdmission {
  if (pending.kind !== "preflight") {
    throw new Error("Resolution cycle does not contain a preflight admission");
  }
  return pending.value;
}

function resolutionAdmission(
  pending: PendingAdmission,
): CandidateSourceSnapshotRemoteRequestAdmission {
  if (pending.kind !== "resolution") {
    throw new Error("Resolution cycle does not contain an intent admission");
  }
  return pending.value;
}

type ResolutionCategory =
  "control_public_observation" | "recovery" | "rollback";
type Resolver = "filebase_control" | "filebase_gateway" | "delegated_ipfs";

interface ActiveResolutionCycle {
  category: ResolutionCategory;
  intentId: string;
  receipts: Map<
    Resolver,
    {
      pending: PendingAdmission;
      result: ReturnType<typeof receiptOutcome>;
    }
  >;
  sequence: number;
}

function endpointKey(
  request: Pick<
    CandidateSourceSnapshotIpnsRequestAdmission,
    "domain" | "endpointType" | "operation"
  >,
): string {
  return `${request.domain}:${request.endpointType}:${request.operation}`;
}

function evidenceKey(
  evidence:
    | CandidateSourceSnapshotFilebaseIpnsReceipt
    | CandidateSourceSnapshotDelegatedIpnsReceipt,
): string {
  if ("delegatedEvidence" in evidence) {
    return `${evidence.domain}:ipfs_delegated_routing_v1:public_resolve`;
  }
  return `${evidence.domain}:${evidence.endpointType}:${evidence.operation}`;
}

function receiptOutcome(
  receipt:
    | CandidateSourceSnapshotFilebaseIpnsReceipt
    | CandidateSourceSnapshotDelegatedIpnsReceipt,
  plan: CandidateSourceSnapshotDemoPlan,
): {
  classification: "prior" | "target" | "unavailable" | "unexpected_cid";
  observedAt: string;
  observedCid: string | null;
  requestOutcome:
    "succeeded" | "retryable_failure" | "timeout_unknown" | "terminal_failure";
  receiptSha256: string;
} {
  if ("delegatedEvidence" in receipt) {
    const delegated = receipt.delegatedEvidence;
    const validation = delegated.validationResult;
    const classification =
      validation === "valid_prior"
        ? "prior"
        : validation === "valid_target"
          ? "target"
          : validation === "unexpected_cid"
            ? "unexpected_cid"
            : "unavailable";
    return {
      classification,
      observedAt: delegated.observedAt,
      observedCid:
        classification === "unavailable" ? null : delegated.observedCid,
      requestOutcome:
        classification !== "unavailable"
          ? "succeeded"
          : validation === "timeout"
            ? "timeout_unknown"
            : ["transport_error", "http_error"].includes(validation)
              ? "retryable_failure"
              : "terminal_failure",
      receiptSha256: receipt.receiptSha256,
    };
  }
  const target =
    receipt.domain === "open_data"
      ? plan.targets.openData
      : plan.targets.queryTable;
  const classification =
    receipt.outcome === "unexpected_cid"
      ? "unexpected_cid"
      : receipt.observedCid === null
        ? "unavailable"
        : receipt.observedCid === target.targetCid
          ? "target"
          : receipt.observedCid === target.priorCid
            ? "prior"
            : "unexpected_cid";
  return {
    classification,
    observedAt: receipt.observedAt,
    observedCid: receipt.observedCid,
    requestOutcome:
      classification !== "unavailable"
        ? "succeeded"
        : receipt.outcome === "timeout"
          ? "timeout_unknown"
          : receipt.outcome === "transport_error" ||
              (receipt.httpStatus !== null &&
                [429, 500, 502, 503, 504].includes(receipt.httpStatus))
            ? "retryable_failure"
            : "terminal_failure",
    receiptSha256: receipt.receiptSha256,
  };
}

export class DurableIpnsBridge implements CandidateSourceSnapshotIpnsJournal {
  readonly #databaseUrl: string;
  readonly #journal: PostgresCandidateSourceSnapshotUploadJournal;
  readonly #activeCycles = new Map<
    CandidateSourceSnapshotIpnsDomain,
    ActiveResolutionCycle
  >();
  readonly #activePreflightCycles = new Map<
    CandidateSourceSnapshotIpnsDomain,
    ActiveResolutionCycle
  >();
  readonly #lastCycleRetryable = new Map<
    CandidateSourceSnapshotIpnsDomain,
    boolean
  >();
  readonly #freshTargets = new Set<CandidateSourceSnapshotIpnsDomain>();
  readonly #nextCycleSequence = new Map<
    CandidateSourceSnapshotIpnsDomain,
    number
  >();
  readonly #nextResolutionCategory = new Map<
    CandidateSourceSnapshotIpnsDomain,
    ResolutionCategory
  >();
  readonly #pending = new Map<string, PendingAdmission>();
  readonly #plan: CandidateSourceSnapshotDemoPlan;
  readonly #states = new Map<
    CandidateSourceSnapshotIpnsDomain,
    CandidateSourceSnapshotIpnsIntentStateRecord
  >();
  #activeMutation: CandidateSourceSnapshotIpnsAttemptAdmission | null = null;
  #mode: "preflight" | "executing" = "preflight";

  constructor(input: {
    databaseUrl: string;
    plan: CandidateSourceSnapshotDemoPlan;
  }) {
    this.#databaseUrl = input.databaseUrl;
    this.#journal = new PostgresCandidateSourceSnapshotUploadJournal(
      input.databaseUrl,
    );
    this.#plan = input.plan;
  }

  async bindIntents(
    intents: readonly CandidateSourceSnapshotIpnsIntentStateRecord[],
  ): Promise<void> {
    if (intents.length !== 2) {
      throw new Error("Runtime requires both exact IPNS intents");
    }
    const sql = postgres(this.#databaseUrl, { max: 1 });
    let sequences: {
      intent_id: string;
      maximum_cycle: number;
      unresolved_request_count: number;
    }[];
    try {
      sequences = await sql<
        {
          intent_id: string;
          maximum_cycle: number;
          unresolved_request_count: number;
        }[]
      >`
        SELECT intent.intent_id,
               greatest(
                 coalesce(max(observation.cycle_sequence), 0),
                 coalesce(max(request.cycle_sequence), 0)
               )::integer AS maximum_cycle,
               count(request.request_id) FILTER (
                 WHERE request.cycle_sequence IS NOT NULL
                   AND request.outcome = 'request_started'
               )::integer AS unresolved_request_count
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        LEFT JOIN oracle_candidate_source_snapshot_demo_ipns_observations
          observation ON observation.intent_id = intent.intent_id
        LEFT JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.intent_id = intent.intent_id
         AND request.cycle_sequence IS NOT NULL
        WHERE intent.plan_id = ${this.#plan.planId}
          AND intent.plan_sha256 = ${this.#plan.planSha256}
        GROUP BY intent.intent_id
      `;
    } finally {
      await sql.end({ timeout: 5 });
    }
    if (sequences.some((row) => row.unresolved_request_count > 0)) {
      throw new Error(
        "Unresolved admitted resolution cycle requires manual reconciliation",
      );
    }
    const sequenceByIntent = new Map(
      sequences.map((row) => [row.intent_id, row.maximum_cycle]),
    );
    this.#states.clear();
    this.#nextCycleSequence.clear();
    for (const intent of intents) {
      this.#states.set(intent.domain, intent);
      this.#nextCycleSequence.set(
        intent.domain,
        sequenceByIntent.get(intent.intentId) ?? 0,
      );
    }
    this.#mode = "executing";
  }

  setNextResolutionCategory(
    domain: CandidateSourceSnapshotIpnsDomain,
    category: ResolutionCategory,
  ): void {
    if (this.#activeCycles.has(domain)) {
      throw new Error("Cannot change a resolution category during a cycle");
    }
    this.#nextResolutionCategory.set(domain, category);
  }

  beginPreflightCycle(
    domain: CandidateSourceSnapshotIpnsDomain,
    attemptSequence: number,
  ): void {
    if (
      this.#mode !== "preflight" ||
      this.#activePreflightCycles.has(domain) ||
      !Number.isInteger(attemptSequence) ||
      attemptSequence < 1 ||
      attemptSequence > 3
    ) {
      throw new Error("Preflight resolution cycle admission is invalid");
    }
    this.#activePreflightCycles.set(domain, {
      category: "control_public_observation",
      intentId: "preflight",
      receipts: new Map(),
      sequence: attemptSequence,
    });
  }

  consumeLastCycleRetryable(
    domain: CandidateSourceSnapshotIpnsDomain,
  ): boolean {
    const value = this.#lastCycleRetryable.get(domain);
    if (value === undefined) {
      throw new Error("Resolution cycle disposition is unavailable");
    }
    this.#lastCycleRetryable.delete(domain);
    return value;
  }

  assertFreshTargets(): void {
    if (
      !this.#freshTargets.has("open_data") ||
      !this.#freshTargets.has("query_table")
    ) {
      throw new Error(
        "Final verification requires fresh target evidence for both IPNS domains",
      );
    }
  }

  async beforeRequest(
    request: CandidateSourceSnapshotIpnsRequestAdmission,
  ): Promise<void> {
    if (request.operation === "names_update") {
      if (
        this.#mode !== "executing" ||
        this.#activeMutation?.request.domain !== request.domain
      ) {
        throw new Error("Names mutation lacks its durable attempt admission");
      }
      return;
    }
    const key = endpointKey(request);
    if (this.#pending.has(key)) {
      throw new Error("Concurrent duplicate remote request admission rejected");
    }
    if (this.#mode === "preflight") {
      const cycle = this.#activePreflightCycles.get(request.domain);
      if (!cycle) {
        throw new Error("Preflight request lacks its shared cycle");
      }
      const resolver =
        request.endpointType === "filebase_names_api_v1"
          ? ("filebase_control" as const)
          : request.endpointType === "filebase_official_ipfs_gateway"
            ? ("filebase_gateway" as const)
            : ("delegated_ipfs" as const);
      const value = await admitCandidateSourceSnapshotPreflightRequest(
        this.#databaseUrl,
        {
          attemptSequence: cycle.sequence,
          domain: request.domain,
          operationKind: request.operation,
          planId: this.#plan.planId,
          planSha256: this.#plan.planSha256,
          redirectSequence: 0,
          resolver,
        },
      );
      if (value.alreadyRecorded) {
        throw new Error(
          value.outcome === "request_started"
            ? "Unresolved admitted preflight resolver read requires manual reconciliation"
            : "Preflight transport cannot replay a completed request without its exact stored receipt",
        );
      }
      this.#pending.set(key, { kind: "preflight", value });
    } else {
      const state = this.#states.get(request.domain);
      if (!state) throw new Error("Resolution lacks its durable intent");
      const resolver =
        request.endpointType === "filebase_names_api_v1"
          ? ("filebase_control" as const)
          : request.endpointType === "filebase_official_ipfs_gateway"
            ? ("filebase_gateway" as const)
            : ("delegated_ipfs" as const);
      let cycle = this.#activeCycles.get(request.domain);
      if (resolver === "filebase_control") {
        if (cycle) {
          throw new Error("A resolution cycle is already active");
        }
        const sequence = (this.#nextCycleSequence.get(request.domain) ?? 0) + 1;
        cycle = {
          category:
            this.#nextResolutionCategory.get(request.domain) ??
            "control_public_observation",
          intentId: state.intentId,
          receipts: new Map(),
          sequence,
        };
        this.#activeCycles.set(request.domain, cycle);
        this.#nextCycleSequence.set(request.domain, sequence);
        this.#nextResolutionCategory.delete(request.domain);
      } else if (!cycle) {
        throw new Error("Resolver request lacks its shared resolution cycle");
      }
      if (cycle.intentId !== state.intentId || cycle.receipts.has(resolver)) {
        throw new Error("Resolution cycle resolver identity conflicts");
      }
      const value = await this.#journal.startResolutionRequest(this.#plan, {
        cycleSequence: cycle.sequence,
        domain: request.domain,
        intentId: state.intentId,
        requestCategory: cycle.category,
        resolver,
      });
      if (value.alreadyRecorded && value.outcome !== "request_started") {
        throw new Error(
          "Resolution transport cannot repeat a completed durable request",
        );
      }
      this.#pending.set(key, { kind: "resolution", value });
    }
  }

  async recordEvidence(
    evidence: CandidateSourceSnapshotFilebaseIpnsEvidence,
  ): Promise<void> {
    if ("evidenceSha256" in evidence) {
      const cycles =
        this.#mode === "preflight"
          ? this.#activePreflightCycles
          : this.#activeCycles;
      const cycle = cycles.get(evidence.domain);
      if (!cycle || cycle.receipts.size !== 3) {
        throw new Error("Aggregate evidence lacks one complete resolver cycle");
      }
      const control = cycle.receipts.get("filebase_control");
      const gateway = cycle.receipts.get("filebase_gateway");
      const delegated = cycle.receipts.get("delegated_ipfs");
      if (
        !control ||
        !gateway ||
        !delegated ||
        control.result.receiptSha256 !== evidence.controlReceiptSha256 ||
        gateway.result.receiptSha256 !== evidence.gatewayReceiptSha256 ||
        delegated.result.receiptSha256 !== evidence.delegatedReceiptSha256
      ) {
        throw new Error(
          "Aggregate evidence does not bind its resolver receipts",
        );
      }
      const values = [control.result, gateway.result, delegated.result];
      const unexpected = values.find(
        (value) => value.classification === "unexpected_cid",
      );
      const expectedClassification = unexpected
        ? "unexpected"
        : values.some((value) => value.classification === "unavailable")
          ? "unavailable"
          : values.every((value) => value.classification === "target")
            ? "target"
            : values.every((value) => value.classification === "prior")
              ? "prior"
              : "split";
      const expectedCid =
        expectedClassification === "target" ||
        expectedClassification === "prior"
          ? values[0]!.observedCid
          : expectedClassification === "unexpected"
            ? unexpected!.observedCid
            : null;
      if (
        evidence.classification !== expectedClassification ||
        evidence.observedCid !== expectedCid
      ) {
        throw new Error("Aggregate evidence classification is inconsistent");
      }
      const unavailable = values.filter(
        (value) => value.classification === "unavailable",
      );
      const retryable =
        evidence.classification === "unavailable" &&
        unavailable.length > 0 &&
        unavailable.every((value) =>
          ["retryable_failure", "timeout_unknown"].includes(
            value.requestOutcome,
          ),
        );
      if (
        control.pending.kind === "preflight" &&
        gateway.pending.kind === "preflight" &&
        delegated.pending.kind === "preflight"
      ) {
        await recordCandidateSourceSnapshotPreflightCycleOutcomes(
          this.#databaseUrl,
          [control, gateway, delegated].map((component) => {
            const result = component.result;
            const cycleOutcome =
              retryable && result.requestOutcome === "succeeded"
                ? ("retryable_failure" as const)
                : result.requestOutcome;
            return {
              admission: preflightAdmission(component.pending),
              completedAt: result.observedAt,
              outcome: cycleOutcome,
              receiptSha256: result.receiptSha256,
            };
          }),
        );
      } else if (
        control.pending.kind === "resolution" &&
        gateway.pending.kind === "resolution" &&
        delegated.pending.kind === "resolution"
      ) {
        await this.#journal.recordResolutionCycle(
          this.#plan,
          [control, gateway, delegated].map((component) => {
            const result = component.result;
            const cycleOutcome =
              retryable && result.requestOutcome === "succeeded"
                ? ("retryable_failure" as const)
                : result.requestOutcome;
            return {
              observation: {
                classification: retryable
                  ? ("unavailable" as const)
                  : result.classification,
                evidenceSha256: result.receiptSha256,
                observedAt: result.observedAt,
                observedCid: retryable ? null : result.observedCid,
                requestOutcome: cycleOutcome,
              },
              request: resolutionAdmission(component.pending),
            };
          }),
        );
      } else {
        throw new Error("Resolution cycle mixes incompatible admissions");
      }
      /* The whole resolver set is durably committed above. Never checkpoint
       * one component at a time: a crash must expose either all three exact
       * receipts or an unresolved admitted cycle that fails closed. */
      cycles.delete(evidence.domain);
      this.#lastCycleRetryable.set(evidence.domain, retryable);
      if (this.#mode === "executing" && evidence.classification === "target") {
        this.#freshTargets.add(evidence.domain);
      }
      return;
    }
    if ("operation" in evidence && evidence.operation === "names_update") {
      const admission = this.#activeMutation;
      if (!admission || admission.request.domain !== evidence.domain) {
        throw new Error("Names mutation receipt lacks its durable admission");
      }
      const outcome =
        evidence.outcome === "accepted"
          ? ("acknowledged" as const)
          : evidence.outcome === "timeout"
            ? ("timeout_unknown" as const)
            : evidence.outcome === "transport_error" ||
                (evidence.httpStatus !== null &&
                  [429, 500, 502, 503, 504].includes(evidence.httpStatus))
              ? ("retryable_failure" as const)
              : ("terminal_failure" as const);
      await this.#journal.recordIpnsMutationOutcome(this.#plan, admission, {
        outcome,
        receiptSha256: evidence.receiptSha256,
      });
      return;
    }
    const key = evidenceKey(evidence);
    const pending = this.#pending.get(key);
    if (!pending) {
      throw new Error("Remote receipt lacks its durable request admission");
    }
    const result = receiptOutcome(evidence, this.#plan);
    const cycle =
      pending.kind === "preflight"
        ? this.#activePreflightCycles.get(evidence.domain)
        : this.#activeCycles.get(evidence.domain);
    const resolver =
      pending.kind === "preflight"
        ? "delegatedEvidence" in evidence
          ? "delegated_ipfs"
          : evidence.endpointType === "filebase_names_api_v1"
            ? "filebase_control"
            : "filebase_gateway"
        : pending.value.resolver;
    if (
      !cycle ||
      cycle.sequence !==
        (pending.kind === "preflight"
          ? pending.value.attemptSequence
          : pending.value.cycleSequence)
    ) {
      throw new Error("Resolver receipt lost its shared resolution cycle");
    }
    if (
      !resolver ||
      (resolver !== "filebase_control" &&
        resolver !== "filebase_gateway" &&
        resolver !== "delegated_ipfs")
    ) {
      throw new Error("Resolver receipt is outside the closed policy");
    }
    if (cycle.receipts.has(resolver)) {
      throw new Error("Resolver receipt conflicts within its cycle");
    }
    cycle.receipts.set(resolver, { pending, result });
    this.#pending.delete(key);
  }

  assertSettled(): void {
    if (
      this.#pending.size > 0 ||
      this.#activeCycles.size > 0 ||
      this.#activePreflightCycles.size > 0
    ) {
      throw new Error("Remote request admissions lack terminal receipts");
    }
  }

  async #transition(
    domain: CandidateSourceSnapshotIpnsDomain,
    fromState: IntentState,
    toState: IntentState,
  ): Promise<void> {
    const current = this.#states.get(domain);
    if (!current || current.state !== fromState) {
      throw new Error("Runtime intent transition lost its exact state");
    }
    const next = await transitionCandidateSourceSnapshotIpnsIntent(
      this.#databaseUrl,
      {
        domain,
        expectedRevision: current.revision,
        fromState,
        intentId: current.intentId,
        planId: this.#plan.planId,
        planSha256: this.#plan.planSha256,
        toState,
        transitionedAt: new Date().toISOString(),
      },
    );
    this.#states.set(domain, { ...current, ...next });
  }

  async closeInterruptedMutation(
    record: CandidateSourceSnapshotIpnsIntentStateRecord,
    direction: "rollback" | "update",
  ): Promise<boolean> {
    const closed = await this.#journal.closeInterruptedIpnsMutationAttempt(
      this.#plan,
      {
        direction,
        domain: record.domain,
        intentId: record.intentId,
      },
    );
    this.#activeMutation = null;
    return closed !== null;
  }

  async beforeFreshnessObservation(
    command: CandidateSourceSnapshotIpnsCommand,
  ): Promise<void> {
    this.setNextResolutionCategory(
      command.domain,
      command.action === "rollback"
        ? "rollback"
        : command.attemptNumber === 1
          ? "control_public_observation"
          : "recovery",
    );
    if (command.action === "rollback") {
      const current = this.#states.get("open_data");
      if (current?.state === "verified") {
        await this.#transition("open_data", "verified", "rollback_recorded");
      } else if (current?.state === "rollback_ambiguous") {
        await this.#transition(
          "open_data",
          "rollback_ambiguous",
          "rollback_recorded",
        );
      } else if (current?.state !== "rollback_recorded") {
        throw new Error(
          "Rollback freshness admission lost its exact durable request state",
        );
      }
    }
  }

  async recordFreshnessObservation(input: {
    command: CandidateSourceSnapshotIpnsCommand;
    observation: {
      classification:
        "target" | "prior" | "split" | "unavailable" | "unexpected";
      observedCid: string | null;
    };
  }): Promise<void> {
    const { command, observation } = input;
    const current = this.#states.get(command.domain);
    if (!current) {
      throw new Error("Freshness observation lacks its durable intent");
    }
    if (command.action === "mutate") {
      if (current.state !== "prior_confirmed") {
        throw new Error("Mutation freshness lost prior-confirmed state");
      }
      if (observation.classification === "prior") return;
      await this.#transition(
        command.domain,
        "prior_confirmed",
        "update_in_flight",
      );
      const next =
        observation.classification === "target"
          ? "target_observed"
          : observation.classification === "unexpected"
            ? "unexpected_cid"
            : "update_ambiguous";
      await this.#transition(command.domain, "update_in_flight", next);
      if (next === "target_observed") {
        await this.#transition(command.domain, "target_observed", "verified");
      }
      return;
    }

    if (
      current.state !== "verified" &&
      current.state !== "rollback_ambiguous" &&
      current.state !== "rollback_recorded"
    ) {
      throw new Error("Rollback freshness lost its exact recovery state");
    }
    if (observation.classification === "target") return;
    if (observation.classification === "prior") {
      if (current.state === "verified") {
        await this.#transition(command.domain, "verified", "rollback_recorded");
        await this.#transition(
          command.domain,
          "rollback_recorded",
          "rolled_back",
        );
      } else {
        await this.#transition(command.domain, current.state, "rolled_back");
      }
      return;
    }
    let fromState: IntentState = current.state;
    if (fromState === "verified") {
      await this.#transition(command.domain, "verified", "rollback_recorded");
      fromState = "rollback_recorded";
    }
    if (fromState === "rollback_recorded") {
      await this.#transition(
        command.domain,
        "rollback_recorded",
        "rollback_in_flight",
      );
      fromState = "rollback_in_flight";
    }
    if (observation.classification === "unexpected") {
      await this.#transition(command.domain, fromState, "unexpected_cid");
    } else if (fromState === "rollback_in_flight") {
      await this.#transition(
        command.domain,
        "rollback_in_flight",
        "rollback_ambiguous",
      );
    }
  }

  async beforeMutation(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<CandidateSourceSnapshotIpnsDurableAuthorization | null> {
    this.setNextResolutionCategory(
      command.domain,
      command.attemptNumber === 1 ? "control_public_observation" : "recovery",
    );
    await this.#transition(
      command.domain,
      "prior_confirmed",
      "update_in_flight",
    );
    const admission = await this.#journal.startIpnsMutationAttempt(this.#plan, {
      attemptSequence: command.attemptNumber,
      direction: "update",
      domain: command.domain,
      intentId: command.intentId,
      replayAuthorizationSha256: command.authorizationSha256,
    });
    this.#activeMutation = admission;
    return command.authorizationId === null
      ? null
      : {
          authorizationId: command.authorizationId,
          authorizationSha256: command.authorizationSha256!,
        };
  }

  async beforeRollback(
    command: CandidateSourceSnapshotIpnsRollbackCommand,
  ): Promise<CandidateSourceSnapshotIpnsDurableAuthorization | null> {
    this.setNextResolutionCategory("open_data", "rollback");
    const current = this.#states.get("open_data");
    if (current?.state !== "rollback_recorded") {
      throw new Error("Rollback admission lost its exact recoverable state");
    }
    await this.#transition(
      "open_data",
      "rollback_recorded",
      "rollback_in_flight",
    );
    this.#activeMutation = await this.#journal.startIpnsMutationAttempt(
      this.#plan,
      {
        attemptSequence: command.attemptNumber,
        direction: "rollback",
        domain: command.domain,
        intentId: command.intentId,
        replayAuthorizationSha256: command.authorizationSha256,
      },
    );
    return {
      authorizationId: command.authorizationId,
      authorizationSha256: command.authorizationSha256,
    };
  }

  async recordObservation(input: {
    command: CandidateSourceSnapshotIpnsCommand;
    observation: {
      classification:
        "target" | "prior" | "split" | "unavailable" | "unexpected";
      observedCid: string | null;
    };
  }): Promise<void> {
    const domain = input.command.domain;
    this.#activeMutation = null;
    if (input.command.action === "rollback") {
      const next =
        input.observation.classification === "prior"
          ? "rolled_back"
          : input.observation.classification === "unexpected"
            ? "unexpected_cid"
            : "rollback_ambiguous";
      await this.#transition(domain, "rollback_in_flight", next);
      return;
    }
    const next =
      input.observation.classification === "target"
        ? "target_observed"
        : input.observation.classification === "unexpected"
          ? "unexpected_cid"
          : "update_ambiguous";
    await this.#transition(domain, "update_in_flight", next);
  }

  async markVerified(
    command: CandidateSourceSnapshotIpnsMutationCommand,
  ): Promise<void> {
    await this.#transition(command.domain, "target_observed", "verified");
  }

  async markRolledBack(): Promise<void> {
    const state = this.#states.get("open_data");
    if (state?.state !== "rolled_back") {
      throw new Error("Rollback did not reach its durable terminal state");
    }
  }
}

async function loadIntentRecords(
  databaseUrl: string,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<readonly CandidateSourceSnapshotIpnsIntentStateRecord[]> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<
      { domain: CandidateSourceSnapshotIpnsDomain; intent_id: string }[]
    >`
      SELECT domain, intent_id
      FROM oracle_candidate_source_snapshot_demo_ipns_intents
      WHERE plan_id = ${plan.planId} AND plan_sha256 = ${plan.planSha256}
      ORDER BY cutover_position
    `;
    if (rows.length !== 2) {
      throw new Error("Runtime requires exactly two durable IPNS intents");
    }
    return await Promise.all(
      rows.map(
        async (row) =>
          await loadCandidateSourceSnapshotIpnsIntentState(databaseUrl, {
            domain: row.domain,
            intentId: row.intent_id,
            planId: plan.planId,
            planSha256: plan.planSha256,
          }),
      ),
    );
  } finally {
    await sql.end({ timeout: 5 });
  }
}

interface PreflightKeyProgress {
  maximumAttempt: number;
  started: number;
  succeeded: number;
}

async function loadPreflightProgress(
  databaseUrl: string,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<{
  byKey: ReadonlyMap<string, PreflightKeyProgress>;
  ready: boolean;
}> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const [readyRows, rows] = await Promise.all([
      sql<{ ready: boolean }[]>`
        SELECT oracle_candidate_source_snapshot_preflight_is_execution_ready(
          ${plan.planId}
        ) AS ready
      `,
      sql<
        {
          domain: CandidateSourceSnapshotIpnsDomain;
          maximum_attempt: number;
          operation_kind: string;
          resolver: string | null;
          started: number;
          succeeded: number;
        }[]
      >`
        SELECT domain, operation_kind, resolver,
               max(attempt_sequence)::integer AS maximum_attempt,
               count(*) FILTER (WHERE outcome = 'request_started')::integer
                 AS started,
               count(*) FILTER (WHERE outcome = 'succeeded')::integer
                 AS succeeded
        FROM oracle_candidate_source_snapshot_demo_requests
        WHERE plan_id = ${plan.planId}
          AND request_category = 'bucket_names_preflight'
        GROUP BY domain, operation_kind, resolver
      `,
    ]);
    return {
      byKey: new Map(
        rows.map((row) => [
          `${row.domain}:${row.operation_kind}:${row.resolver ?? "none"}`,
          {
            maximumAttempt: row.maximum_attempt,
            started: row.started,
            succeeded: row.succeeded,
          },
        ]),
      ),
      ready: readyRows[0]?.ready === true,
    };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function closeInterruptedPreflightRequests(
  databaseUrl: string,
  plan: CandidateSourceSnapshotDemoPlan,
): Promise<number> {
  const sql = postgres(databaseUrl, { max: 1 });
  let rows: {
    attempt_sequence: number;
    domain: CandidateSourceSnapshotIpnsDomain;
    operation_kind: "bucket_head" | "names_read" | "public_resolve";
    redirect_sequence: number;
    resolver: Resolver | null;
  }[];
  try {
    rows = await sql<
      {
        attempt_sequence: number;
        domain: CandidateSourceSnapshotIpnsDomain;
        operation_kind: "bucket_head" | "names_read" | "public_resolve";
        redirect_sequence: number;
        resolver: Resolver | null;
      }[]
    >`
      SELECT domain, operation_kind, resolver, attempt_sequence,
             redirect_sequence
      FROM oracle_candidate_source_snapshot_demo_requests
      WHERE plan_id = ${plan.planId}
        AND request_category = 'bucket_names_preflight'
        AND outcome = 'request_started'
      ORDER BY domain, operation_kind, resolver, attempt_sequence
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
  for (const row of rows) {
    const admission = await admitCandidateSourceSnapshotPreflightRequest(
      databaseUrl,
      {
        attemptSequence: row.attempt_sequence,
        domain: row.domain,
        operationKind: row.operation_kind,
        planId: plan.planId,
        planSha256: plan.planSha256,
        redirectSequence: row.redirect_sequence,
        resolver: row.resolver,
      },
    );
    if (!admission.alreadyRecorded || admission.outcome !== "request_started") {
      throw new Error("Interrupted preflight request identity changed");
    }
    const completedAt = new Date().toISOString();
    await recordCandidateSourceSnapshotPreflightRequestOutcome(databaseUrl, {
      admission,
      completedAt,
      outcome: "timeout_unknown",
      receiptSha256: canonicalJsonSha256({
        completedAt,
        requestId: admission.requestId,
        schemaVersion:
          "candidate-source-snapshot-interrupted-preflight-receipt-v1",
        status: "timeout_unknown",
      }),
    });
  }
  return rows.length;
}

async function transitionIntentRecord(input: {
  databaseUrl: string;
  fromState: IntentState;
  plan: CandidateSourceSnapshotDemoPlan;
  record: CandidateSourceSnapshotIpnsIntentStateRecord;
  toState: IntentState;
  transitionedAt: string;
}): Promise<CandidateSourceSnapshotIpnsIntentStateRecord> {
  const next = await transitionCandidateSourceSnapshotIpnsIntent(
    input.databaseUrl,
    {
      domain: input.record.domain,
      expectedRevision: input.record.revision,
      fromState: input.fromState,
      intentId: input.record.intentId,
      planId: input.plan.planId,
      planSha256: input.plan.planSha256,
      toState: input.toState,
      transitionedAt: input.transitionedAt,
    },
  );
  return { ...input.record, ...next };
}

export interface CandidateSourceSnapshotRemoteRuntimeDependencies {
  bucketProbe?: CandidateSourceSnapshotBucketProbe;
  credentialFreeVerifier?: CandidateSourceSnapshotCredentialFreeVerifier;
  fetchImpl?: typeof fetch;
  observeDelegated?: typeof observeDelegatedIpnsRecord;
}

class ProductionCandidateSourceSnapshotRemoteRuntime implements CandidateSourceSnapshotSession2RemoteRuntime {
  readonly boundary: CandidateSourceSnapshotFilebaseIpnsAdapter;
  readonly journal: DurableIpnsBridge;
  readonly #bucketProbe: CandidateSourceSnapshotBucketProbe;
  readonly #config: EnabledCandidateSourceSnapshotExecutionConfig;
  readonly #databaseUrl: string;
  readonly #plan: CandidateSourceSnapshotDemoPlan;
  readonly #verifier: CandidateSourceSnapshotCredentialFreeVerifier;

  constructor(input: {
    config: EnabledCandidateSourceSnapshotExecutionConfig;
    databaseUrl: string;
    dependencies?: CandidateSourceSnapshotRemoteRuntimeDependencies;
    plan: CandidateSourceSnapshotDemoPlan;
  }) {
    this.#config = input.config;
    this.#databaseUrl = input.databaseUrl;
    this.#plan = input.plan;
    this.journal = new DurableIpnsBridge({
      databaseUrl: input.databaseUrl,
      plan: input.plan,
    });
    this.#bucketProbe =
      input.dependencies?.bucketProbe ??
      new AwsCandidateSourceSnapshotBucketProbe(input.config);
    this.#verifier =
      input.dependencies?.credentialFreeVerifier ??
      new PostgresCandidateSourceSnapshotCredentialFreeVerifier({
        fetchImpl: input.dependencies?.fetchImpl ?? fetch,
      });
    this.boundary = new CandidateSourceSnapshotFilebaseIpnsAdapter({
      config: input.config,
      evidenceSink: {
        record: async (evidence) => this.journal.recordEvidence(evidence),
      },
      fetchImpl: input.dependencies?.fetchImpl ?? fetch,
      observeDelegated:
        input.dependencies?.observeDelegated ?? observeDelegatedIpnsRecord,
      plan: input.plan,
      requestGate: {
        beforeRequest: async (request) => this.journal.beforeRequest(request),
      },
    });
  }

  async close(): Promise<void> {
    this.#bucketProbe.close();
  }

  async #observeWithReadRetries(
    domain: CandidateSourceSnapshotIpnsDomain,
    category: ResolutionCategory,
  ): Promise<CandidateSourceSnapshotIpnsAggregateEvidence> {
    const maximumAttempts = this.#config.limits.maxRetries + 1;
    for (let attempt = 1; attempt <= maximumAttempts; attempt += 1) {
      this.journal.setNextResolutionCategory(domain, category);
      const observation = await this.boundary.observeIdentity(domain);
      const retryable = this.journal.consumeLastCycleRetryable(domain);
      if (
        observation.classification !== "unavailable" ||
        !retryable ||
        attempt === maximumAttempts
      ) {
        return observation;
      }
      await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
    }
    throw new Error("Resolution retry envelope is inconsistent");
  }

  async #observePreflightWithReadRetries(
    domain: CandidateSourceSnapshotIpnsDomain,
    firstAttempt: number,
  ): Promise<CandidateSourceSnapshotIpnsAggregateEvidence> {
    const maximumAttempts = this.#config.limits.maxRetries + 1;
    for (let attempt = firstAttempt; attempt <= maximumAttempts; attempt += 1) {
      this.journal.beginPreflightCycle(domain, attempt);
      const observation = await this.boundary.observeIdentity(domain);
      const retryable = this.journal.consumeLastCycleRetryable(domain);
      if (
        observation.classification !== "unavailable" ||
        !retryable ||
        attempt === maximumAttempts
      ) {
        return observation;
      }
      await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
    }
    throw new Error("Preflight retry envelope is inconsistent");
  }

  async readOnlyPreflight(): Promise<void> {
    let initial = await loadPreflightProgress(this.#databaseUrl, this.#plan);
    if (initial.ready) return;
    if ([...initial.byKey.values()].some((value) => value.started > 0)) {
      await closeInterruptedPreflightRequests(this.#databaseUrl, this.#plan);
      initial = await loadPreflightProgress(this.#databaseUrl, this.#plan);
    }
    for (const domain of ["open_data", "query_table"] as const) {
      const bucket = initial.byKey.get(`${domain}:bucket_head:none`);
      if ((bucket?.started ?? 0) > 0 || (bucket?.succeeded ?? 0) > 1) {
        throw new Error("Concurrent or conflicting bucket preflight rejected");
      }
      if ((bucket?.succeeded ?? 0) === 0) {
        let succeeded = false;
        for (
          let attempt = (bucket?.maximumAttempt ?? 0) + 1;
          attempt <= this.#config.limits.maxRetries + 1;
          attempt += 1
        ) {
          const admission = await admitCandidateSourceSnapshotPreflightRequest(
            this.#databaseUrl,
            {
              attemptSequence: attempt,
              domain,
              operationKind: "bucket_head",
              planId: this.#plan.planId,
              planSha256: this.#plan.planSha256,
              redirectSequence: 0,
              resolver: null,
            },
          );
          if (admission.alreadyRecorded) {
            throw new Error("Concurrent duplicate bucket request rejected");
          }
          const evidence = await this.#bucketProbe.headBucket(domain);
          await recordCandidateSourceSnapshotPreflightRequestOutcome(
            this.#databaseUrl,
            {
              admission,
              completedAt: evidence.completedAt,
              outcome: evidence.outcome,
              receiptSha256: evidence.receiptSha256,
            },
          );
          if (evidence.outcome === "succeeded") {
            succeeded = true;
            break;
          }
          if (
            !["retryable_failure", "timeout_unknown"].includes(evidence.outcome)
          ) {
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
        }
        if (!succeeded) {
          throw new Error(`Candidate ${domain} bucket preflight failed closed`);
        }
      }
      const progress = [
        initial.byKey.get(`${domain}:names_read:filebase_control`),
        initial.byKey.get(`${domain}:public_resolve:filebase_gateway`),
        initial.byKey.get(`${domain}:public_resolve:delegated_ipfs`),
      ];
      if (progress.some((value) => (value?.started ?? 0) > 0)) {
        throw new Error("Concurrent or interrupted IPNS preflight rejected");
      }
      const successful = progress.filter(
        (value) => value?.succeeded === 1,
      ).length;
      if (successful !== 0 && successful !== 3) {
        throw new Error("Partial successful IPNS preflight cannot be replayed");
      }
      if (successful === 3) continue;
      const firstAttempt =
        Math.max(0, ...progress.map((value) => value?.maximumAttempt ?? 0)) + 1;
      const observation = await this.#observePreflightWithReadRetries(
        domain,
        firstAttempt,
      );
      const expected =
        domain === "open_data"
          ? this.#config.targets.openData.priorCid
          : this.#config.targets.queryTable.priorCid;
      if (
        observation.classification !== "prior" ||
        observation.observedCid !== expected
      ) {
        throw new Error(
          `Candidate ${domain} IPNS preflight did not verify its immutable prior`,
        );
      }
    }
    this.journal.assertSettled();
    if (!(await loadPreflightProgress(this.#databaseUrl, this.#plan)).ready) {
      throw new Error("Candidate preflight evidence is not approval-ready");
    }
  }

  async prepareIntents(input: {
    createInitialIntents: () => Promise<unknown>;
    intendedAt: string;
    plan: CandidateSourceSnapshotDemoPlan;
    rollbackAuthorization?: CandidateSourceSnapshotIpnsRollbackAuthorization;
    uploadClosure: { closureId: string };
  }): Promise<readonly CandidateSourceSnapshotIpnsIntent[]> {
    let records: readonly CandidateSourceSnapshotIpnsIntentStateRecord[];
    try {
      await input.createInitialIntents();
      records = await loadIntentRecords(this.#databaseUrl, this.#plan);
    } catch (error) {
      records = await loadIntentRecords(this.#databaseUrl, this.#plan).catch(
        () => {
          throw error;
        },
      );
    }
    await this.journal.bindIntents(records);
    for (const initial of records) {
      let record = initial;
      if (record.state === "target_observed") {
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "recovery",
        );
        if (
          observation.classification !== "target" ||
          observation.observedCid !== record.targetCid
        ) {
          throw new Error(
            `Candidate ${record.domain} target is not freshly verified`,
          );
        }
        await transitionIntentRecord({
          databaseUrl: this.#databaseUrl,
          fromState: "target_observed",
          plan: this.#plan,
          record,
          toState: "verified",
          transitionedAt: new Date().toISOString(),
        });
      } else if (
        record.state === "update_in_flight" ||
        record.state === "update_ambiguous"
      ) {
        const interrupted = await this.journal.closeInterruptedMutation(
          record,
          "update",
        );
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "recovery",
        );
        const observedCid = observation.observedCid;
        if (
          observation.classification === "target" &&
          observedCid === record.targetCid
        ) {
          record = await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState: record.state,
            plan: this.#plan,
            record,
            toState: "target_observed",
            transitionedAt: new Date().toISOString(),
          });
          await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState: "target_observed",
            plan: this.#plan,
            record,
            toState: "verified",
            transitionedAt: new Date().toISOString(),
          });
        } else if (
          observation.classification === "prior" &&
          observedCid === record.priorCid
        ) {
          if (record.state === "update_in_flight") {
            record = await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "update_in_flight",
              plan: this.#plan,
              record,
              toState: "update_ambiguous",
              transitionedAt: new Date().toISOString(),
            });
          }
          await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState: "update_ambiguous",
            plan: this.#plan,
            record,
            toState: "prior_confirmed",
            transitionedAt: new Date().toISOString(),
          });
          if (interrupted) {
            throw new Error(
              `Candidate ${record.domain} interrupted mutation was reconciled at its prior; an exact separately authorized retry is required`,
            );
          }
        } else if (
          observation.classification === "unexpected" &&
          observedCid !== null
        ) {
          await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState: record.state,
            plan: this.#plan,
            record,
            toState: "unexpected_cid",
            transitionedAt: new Date().toISOString(),
          });
          throw new Error(
            `Candidate ${record.domain} recovery observed an unexpected CID`,
          );
        } else {
          if (record.state === "update_in_flight") {
            await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "update_in_flight",
              plan: this.#plan,
              record,
              toState: "update_ambiguous",
              transitionedAt: new Date().toISOString(),
            });
          }
          throw new Error(
            `Candidate ${record.domain} recovery remains ambiguous`,
          );
        }
      } else if (
        record.state === "rollback_recorded" ||
        record.state === "rollback_in_flight" ||
        record.state === "rollback_ambiguous"
      ) {
        if (record.domain !== "open_data") {
          throw new Error("Only open data may enter rollback recovery");
        }
        if (record.state === "rollback_in_flight") {
          await this.journal.closeInterruptedMutation(record, "rollback");
        }
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "rollback",
        );
        if (
          observation.classification === "prior" &&
          observation.observedCid === record.priorCid
        ) {
          await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState: record.state,
            plan: this.#plan,
            record,
            toState: "rolled_back",
            transitionedAt: new Date().toISOString(),
          });
          throw new Error(
            "Candidate open-data rollback recovery reached its immutable prior without another mutation",
          );
        }
        if (
          observation.classification === "unexpected" &&
          observation.observedCid !== null
        ) {
          let fromState = record.state;
          if (fromState === "rollback_recorded") {
            record = await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState,
              plan: this.#plan,
              record,
              toState: "rollback_in_flight",
              transitionedAt: new Date().toISOString(),
            });
            fromState = "rollback_in_flight";
          }
          await transitionIntentRecord({
            databaseUrl: this.#databaseUrl,
            fromState,
            plan: this.#plan,
            record,
            toState: "unexpected_cid",
            transitionedAt: new Date().toISOString(),
          });
          throw new Error(
            "Candidate open-data rollback recovery observed an unexpected CID",
          );
        }
        if (
          observation.classification === "target" &&
          observation.observedCid === record.targetCid
        ) {
          if (record.state === "rollback_recorded") {
            record = await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "rollback_recorded",
              plan: this.#plan,
              record,
              toState: "rollback_in_flight",
              transitionedAt: new Date().toISOString(),
            });
          }
          if (record.state === "rollback_in_flight") {
            await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "rollback_in_flight",
              plan: this.#plan,
              record,
              toState: "rollback_ambiguous",
              transitionedAt: new Date().toISOString(),
            });
          }
          if (!input.rollbackAuthorization) {
            throw new Error(
              "Candidate open-data rollback remains at target and requires exact durable retry authorization",
            );
          }
        } else {
          if (record.state === "rollback_recorded") {
            record = await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "rollback_recorded",
              plan: this.#plan,
              record,
              toState: "rollback_in_flight",
              transitionedAt: new Date().toISOString(),
            });
          }
          if (record.state === "rollback_in_flight") {
            await transitionIntentRecord({
              databaseUrl: this.#databaseUrl,
              fromState: "rollback_in_flight",
              plan: this.#plan,
              record,
              toState: "rollback_ambiguous",
              transitionedAt: new Date().toISOString(),
            });
          }
          throw new Error(
            "Candidate open-data rollback recovery remains ambiguous",
          );
        }
      } else if (record.state === "verified") {
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "recovery",
        );
        if (
          observation.classification !== "target" ||
          observation.observedCid !== record.targetCid
        ) {
          throw new Error(
            `Candidate ${record.domain} verified target is not freshly observable`,
          );
        }
      } else if (record.state === "intent_recorded") {
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "control_public_observation",
        );
        if (
          observation.classification !== "prior" ||
          observation.observedCid !== record.priorCid
        ) {
          throw new Error(
            `Candidate ${record.domain} intent prior was not verified before mutation`,
          );
        }
        await transitionIntentRecord({
          databaseUrl: this.#databaseUrl,
          fromState: "intent_recorded",
          plan: this.#plan,
          record,
          toState: "prior_confirmed",
          transitionedAt: input.intendedAt,
        });
      } else if (record.state === "prior_confirmed") {
        const observation = await this.#observeWithReadRetries(
          record.domain,
          "recovery",
        );
        if (
          observation.classification !== "prior" ||
          observation.observedCid !== record.priorCid
        ) {
          throw new Error(
            `Candidate ${record.domain} intent prior is not freshly recoverable`,
          );
        }
      } else {
        throw new Error(
          `Candidate ${record.domain} intent is not safely recoverable`,
        );
      }
    }
    const prepared = await loadIntentRecords(this.#databaseUrl, this.#plan);
    await this.journal.bindIntents(prepared);
    if (
      prepared.some(
        (intent) =>
          intent.uploadClosureId !== input.uploadClosure.closureId ||
          !["prior_confirmed", "verified", "rollback_ambiguous"].includes(
            intent.state,
          ),
      )
    ) {
      throw new Error(
        "Candidate IPNS recovery requires an exact separate reconciliation before mutation",
      );
    }
    const open = prepared.find((intent) => intent.domain === "open_data");
    const query = prepared.find((intent) => intent.domain === "query_table");
    if (
      !open ||
      !query ||
      (query.state === "verified" && open.state !== "verified") ||
      (open.state === "rollback_ambiguous" &&
        ![
          "prior_confirmed",
          "update_failed_prior_confirmed",
          "rolled_back",
        ].includes(query.state))
    ) {
      throw new Error("Candidate IPNS recovery violates domain ordering");
    }
    return prepared;
  }

  async recordFinalVerification(input: {
    approvalId: string;
    localSource: CandidateSourceSnapshotLocalObjectSource;
    plan: CandidateSourceSnapshotDemoPlan;
    uploadClosure: { closureId: string };
  }): Promise<void> {
    this.journal.assertFreshTargets();
    await this.#verifier.verify({
      approvalId: input.approvalId,
      databaseUrl: this.#databaseUrl,
      localSource: input.localSource,
      plan: input.plan,
      uploadClosureId: input.uploadClosure.closureId,
    });
    await recordCandidateSourceSnapshotRemoteVerification(this.#databaseUrl, {
      approvalId: input.approvalId,
      planId: input.plan.planId,
      planSha256: input.plan.planSha256,
      uploadClosureId: input.uploadClosure.closureId,
      verifiedAt: new Date().toISOString(),
    });
  }
}

export function createCandidateSourceSnapshotRemoteRuntime(input: {
  config: EnabledCandidateSourceSnapshotExecutionConfig;
  databaseUrl: string;
  dependencies?: CandidateSourceSnapshotRemoteRuntimeDependencies;
  plan: CandidateSourceSnapshotDemoPlan;
}): CandidateSourceSnapshotSession2RemoteRuntime {
  return new ProductionCandidateSourceSnapshotRemoteRuntime(input);
}

export const candidateSourceSnapshotRemoteRuntimeFactory: CandidateSourceSnapshotSession2RemoteRuntimeFactory =
  ({ config, databaseUrl, plan }) =>
    createCandidateSourceSnapshotRemoteRuntime({ config, databaseUrl, plan });
