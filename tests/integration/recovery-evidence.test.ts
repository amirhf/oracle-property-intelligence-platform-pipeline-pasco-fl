import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { approvePublicationPlan } from "../../src/db/publication-durability.js";
import {
  IPNS_RECOVERY_MAX_LATENCY_MS,
  IPNS_RECOVERY_MAX_RESPONSE_BYTES,
  ipnsRecoveryReceiptSchema,
  recordIpnsIntents,
  recoverIpnsIntent,
} from "../../src/db/ipns-intent.js";
import {
  loadPreparedPilot,
  recordRunStarted,
} from "../../src/db/pilot-repository.js";
import { runMigrations } from "../../src/db/migrations.js";
import type { PreparedPilot } from "../../src/domain/types.js";
import { canonicalJsonSha256 } from "../../src/lib/canonical-json.js";
import { buildPublicationDryRun } from "../../src/publication/dry-run.js";
import { calculateIpfsCid } from "../../src/publication/ipfs-cid.js";
import { validatePublicationPlan } from "../../src/publication/plan.js";
import { verifyPreparedInput } from "../../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parsePreparedPilot,
} from "../../src/workflow/schemas.js";
import {
  createSyntheticLifecycleSnapshot,
  syntheticLoaderIdempotencyKey,
} from "../helpers/durability.js";

const adminDatabaseUrl =
  process.env.ORACLE_TEST_DATABASE_URL ??
  "postgresql://postgres:elephant@localhost:5433/elephant";
const schemaName = `recovery_evidence_${process.pid}_${Date.now()}`;
const schemaDatabaseUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${schemaName}`;
let dataDir: string;

const targets = {
  credentialsAvailable: true,
  openData: {
    bucket: "synthetic-open-evidence",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-open-evidence",
    ipnsNetworkKey:
      "k51A23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
  queryTable: {
    bucket: "synthetic-query-evidence",
    bucketConfirmed: true,
    ipnsLabel: "synthetic-query-evidence",
    ipnsNetworkKey:
      "k51B23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
  },
} as const;

const resolvedReceipt = {
  errorCode: null,
  httpStatus: 200,
  latencyMs: 7,
  outcome: "resolved" as const,
  providerRequestIdHash: "1".repeat(64),
  responseBodyHash: "2".repeat(64),
  responseBytes: 128,
  schemaVersion: "1.0.0" as const,
};

const receiptOutcomes = [
  "resolved",
  "unavailable",
  "http_error",
  "timeout",
  "transport_error",
] as const;
const receiptErrorCodes = [
  null,
  "http_error",
  "invalid_response",
  "provider_unavailable",
  "rate_limited",
  "timeout",
  "transport_error",
] as const;
const receiptHttpStatuses = [
  null,
  100,
  199,
  200,
  299,
  300,
  399,
  400,
  429,
  599,
  600,
] as const;

type ReceiptMatrixOutcome = (typeof receiptOutcomes)[number];
type ReceiptMatrixErrorCode = (typeof receiptErrorCodes)[number];

function independentReceiptMatrixExpectation(input: {
  errorCode: ReceiptMatrixErrorCode;
  httpStatus: (typeof receiptHttpStatuses)[number];
  outcome: ReceiptMatrixOutcome;
}): boolean {
  switch (input.outcome) {
    case "resolved":
      return (
        input.errorCode === null &&
        input.httpStatus !== null &&
        input.httpStatus >= 200 &&
        input.httpStatus <= 299
      );
    case "unavailable":
      return (
        input.errorCode === "provider_unavailable" && input.httpStatus === null
      );
    case "http_error":
      return (
        (input.errorCode === "http_error" ||
          input.errorCode === "rate_limited") &&
        input.httpStatus !== null &&
        input.httpStatus >= 400 &&
        input.httpStatus <= 599
      );
    case "timeout":
      return input.errorCode === "timeout" && input.httpStatus === null;
    case "transport_error":
      return input.errorCode === "transport_error" && input.httpStatus === null;
  }
}

function independentCanonical(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(independentCanonical).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(
        ([key, entry]) =>
          `${JSON.stringify(key)}:${independentCanonical(entry)}`,
      )
      .join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("non-JSON test value");
  return encoded;
}

function independentSha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function independentCycleId(input: {
  attemptId: string | null;
  domain: string;
  intentId: string;
  planId: string;
  planSha256: string;
  sequence: number;
}): string {
  return `resolution_${independentSha256(
    JSON.stringify([
      "1.0.0",
      "Publish/pasco/ipns-resolution-cycle",
      input.planId,
      input.planSha256,
      input.intentId,
      input.domain,
      input.attemptId ?? "none",
      String(input.sequence),
    ]),
  ).slice(0, 32)}`;
}

function resolverObservations(
  priorCid: string,
  observedAt: string,
  receipt: Record<string, unknown> = resolvedReceipt,
) {
  return [
    {
      endpointId: "filebase.shared",
      observedAt,
      observedCid: priorCid,
      receipt,
      resolverKind: "filebase_control_plane" as const,
    },
    {
      endpointId: "gateway.shared",
      observedAt,
      observedCid: priorCid,
      receipt,
      resolverKind: "public_gateway" as const,
    },
  ];
}

beforeAll(async () => {
  dataDir = await mkdtemp(path.join(tmpdir(), "oracle-recovery-evidence-"));
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`CREATE SCHEMA ${schemaName}`);
  } finally {
    await admin.end({ timeout: 5 });
  }
  expect(await runMigrations(schemaDatabaseUrl)).toHaveLength(24);
});

afterAll(async () => {
  const admin = postgres(adminDatabaseUrl, { max: 1 });
  try {
    await admin.unsafe(`DROP SCHEMA ${schemaName} CASCADE`);
  } finally {
    await admin.end({ timeout: 5 });
    await rm(dataDir, { force: true, recursive: true });
  }
});

describe("strict and cycle-bound IPNS recovery evidence", () => {
  let identity: { county: "pasco"; planId: string; planSha256: string };
  let intentRequest: Parameters<typeof recordIpnsIntents>[1];
  let openIntentId: string;
  let queryIntentId: string;
  let priorCid: string;

  it("atomically records two intent-bound initial cycles and exactly replays them", async () => {
    const snapshot = await createSyntheticLifecycleSnapshot(dataDir, {
      coverage: "authoritative",
      folios: Array.from(
        { length: 25 },
        (_, index) => `EVID-${String(index + 1).padStart(2, "0")}`,
      ),
      label: "strict-recovery-evidence",
      observedAt: "2026-08-29T05:00:00.000Z",
    });
    await recordRunStarted(schemaDatabaseUrl, snapshot.request);
    const verified = await verifyPreparedInput(
      dataDir,
      snapshot.reference,
      parsePreparedPilot,
      snapshot.snapshot.snapshotId,
    );
    await loadPreparedPilot(
      schemaDatabaseUrl,
      snapshot.request,
      structuredClone(verified.prepared) as PreparedPilot,
      {
        idempotencyKey: syntheticLoaderIdempotencyKey(
          snapshot.request.workflowId,
          snapshot.reference.preparedInputId,
        ),
        preparedManifest: verified.manifest,
        preparedReference: verified.reference,
        requestSha256: countyIngestRequestSha256(snapshot.request),
        runId: snapshot.request.runId,
        snapshot: verified.snapshot,
      },
    );
    const dryRun = await buildPublicationDryRun({
      dataDir,
      databaseUrl: schemaDatabaseUrl,
      exportMode: "authoritative",
      runId: snapshot.request.runId,
      targets,
    });
    const plan = validatePublicationPlan(
      JSON.parse(
        await readFile(
          path.join(
            dataDir,
            dryRun.outputRoot,
            "publication-dry-run-plan.json",
          ),
          "utf8",
        ),
      ),
    );
    identity = {
      county: "pasco",
      planId: plan.planId,
      planSha256: plan.planSha256,
    };
    await approvePublicationPlan(schemaDatabaseUrl, {
      ...identity,
      approverReference: "synthetic_controller",
    });
    priorCid = await calculateIpfsCid("shared-prior-cid\n");
    const initialObservations = resolverObservations(
      priorCid,
      "2026-08-29T05:01:00.000Z",
    );
    intentRequest = {
      ...identity,
      targets: [
        {
          domain: "open_data",
          intendedAt: "2026-08-29T05:01:00.000Z",
          priorCid,
          resolutionEvidence: { observations: initialObservations },
        },
        {
          domain: "query_table",
          intendedAt: "2026-08-29T05:01:00.000Z",
          priorCid,
          resolutionEvidence: { observations: initialObservations },
        },
      ],
    };
    const intents = await recordIpnsIntents(schemaDatabaseUrl, intentRequest);
    openIntentId = intents.find(
      (item) => item.domain === "open_data",
    )!.intentId;
    queryIntentId = intents.find(
      (item) => item.domain === "query_table",
    )!.intentId;
    await expect(
      Promise.all([
        recordIpnsIntents(schemaDatabaseUrl, intentRequest),
        recordIpnsIntents(schemaDatabaseUrl, intentRequest),
      ]),
    ).resolves.toEqual([intents, intents]);

    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const cycles = await sql<
        {
          cycles: number;
          distinct_domains: number;
          distinct_intents: number;
          distinct_evidence: number;
          initial_sequences: number;
        }[]
      >`
        SELECT count(*)::int AS cycles,
               count(DISTINCT domain)::int AS distinct_domains,
               count(DISTINCT intent_id)::int AS distinct_intents,
               count(DISTINCT evidence_sha256)::int AS distinct_evidence,
               count(*) FILTER (WHERE cycle_sequence = 0)::int
                 AS initial_sequences
        FROM oracle_publication_ipns_resolution_cycles
      `;
      expect(cycles[0]).toEqual({
        cycles: 2,
        distinct_domains: 2,
        distinct_evidence: 1,
        distinct_intents: 2,
        initial_sequences: 2,
      });
    } finally {
      await sql.end({ timeout: 5 });
    }

    const conflictingSecondDomain = structuredClone(intentRequest) as {
      targets: [
        {
          resolutionEvidence: {
            observations: ReturnType<typeof resolverObservations>;
          };
        },
        {
          resolutionEvidence: {
            observations: ReturnType<typeof resolverObservations>;
          };
        },
      ];
    } & typeof intentRequest;
    conflictingSecondDomain.targets[1].resolutionEvidence.observations =
      resolverObservations(priorCid, "2026-08-29T05:01:01.000Z");
    await expect(
      recordIpnsIntents(schemaDatabaseUrl, conflictingSecondDomain),
    ).rejects.toThrow("query_table intent payload changed");
    const afterConflict = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const count = await afterConflict<{ cycles: number; intents: number }[]>`
        SELECT
          (SELECT count(*)::int FROM oracle_publication_ipns_resolution_cycles)
            AS cycles,
          (SELECT count(*)::int FROM oracle_publication_ipns_intents)
            AS intents
      `;
      expect(count[0]).toEqual({ cycles: 2, intents: 2 });
    } finally {
      await afterConflict.end({ timeout: 5 });
    }
  }, 120_000);

  it("keeps the application and PostgreSQL outcome/error/status matrices identical", async () => {
    const parsedGolden = ipnsRecoveryReceiptSchema.parse(resolvedReceipt);
    const goldenSha256 =
      "8b6e2641e866b4f6542f981baef477b5e29590f5864d76b04a7d4176c9dcdad4";
    expect(canonicalJsonSha256(parsedGolden)).toBe(goldenSha256);
    expect(independentSha256(independentCanonical(resolvedReceipt))).toBe(
      goldenSha256,
    );

    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const intent = await sql<
        {
          domain: string;
          intent_id: string;
          publication_plan_id: string;
          publication_plan_sha256: string;
          revision: number;
        }[]
      >`
        SELECT intent.domain, intent.intent_id, intent.publication_plan_id,
               intent.publication_plan_sha256, state.revision
        FROM oracle_publication_ipns_intents intent
        JOIN oracle_publication_ipns_intent_state state USING (intent_id)
        WHERE intent.intent_id = ${openIntentId}
      `;
      expect(intent).toHaveLength(1);

      let acceptedCount = 0;
      let matrixIndex = 0;
      for (const outcome of receiptOutcomes) {
        for (const errorCode of receiptErrorCodes) {
          for (const httpStatus of receiptHttpStatuses) {
            const expected = independentReceiptMatrixExpectation({
              errorCode,
              httpStatus,
              outcome,
            });
            const receipt = {
              ...resolvedReceipt,
              errorCode,
              httpStatus,
              outcome,
            };
            const applicationAccepted =
              ipnsRecoveryReceiptSchema.safeParse(receipt).success;

            const observedAt = new Date(
              Date.UTC(2026, 7, 29, 6, 0, matrixIndex),
            ).toISOString();
            const observationClassification =
              outcome === "resolved"
                ? "resolved"
                : outcome === "unavailable"
                  ? "unavailable"
                  : "error";
            const canonicalObservations = [
              "filebase_control_plane",
              "public_gateway",
            ].map((resolverKind, ordinal) => ({
              classification: observationClassification,
              endpointId:
                resolverKind === "filebase_control_plane"
                  ? "filebase.matrix"
                  : "gateway.matrix",
              observedAt,
              observedCid: outcome === "resolved" ? priorCid : null,
              ordinal,
              receipt,
              resolverKind,
            }));
            const observationsCanonical = independentCanonical(
              canonicalObservations,
            );
            const receiptsCanonical = independentCanonical(
              canonicalObservations.map(
                ({ ordinal, receipt: observationReceipt }) => ({
                  ordinal,
                  receipt: observationReceipt,
                }),
              ),
            );
            const sequence = 10_000 + matrixIndex;
            const cycleId = independentCycleId({
              attemptId: null,
              domain: intent[0]!.domain,
              intentId: intent[0]!.intent_id,
              planId: intent[0]!.publication_plan_id,
              planSha256: intent[0]!.publication_plan_sha256,
              sequence,
            });
            let postgresAccepted = true;
            try {
              await sql`
                INSERT INTO oracle_publication_ipns_resolution_cycles (
                  resolution_cycle_id, intent_id, domain, attempt_id,
                  intent_revision, cycle_sequence, evidence_sha256,
                  observation_count, observations_canonical, classification,
                  receipts_canonical, receipt_identity_sha256
                ) VALUES (
                  ${cycleId}, ${openIntentId}, ${intent[0]!.domain}, null,
                  ${intent[0]!.revision}, ${sequence},
                  ${independentSha256(observationsCanonical)}, 2,
                  ${observationsCanonical},
                  ${outcome === "resolved" ? "prior_observed" : "timeout_transport_uncertainty"},
                  ${receiptsCanonical},
                  ${independentSha256(receiptsCanonical)}
                )
              `;
            } catch {
              postgresAccepted = false;
            }

            expect(
              {
                applicationAccepted,
                errorCode,
                httpStatus,
                outcome,
                postgresAccepted,
              },
              `receipt matrix mismatch at ${outcome}/${String(errorCode)}/${String(httpStatus)}`,
            ).toEqual({
              applicationAccepted: expected,
              errorCode,
              httpStatus,
              outcome,
              postgresAccepted: expected,
            });
            if (expected) acceptedCount += 1;
            matrixIndex += 1;
          }
        }
      }
      expect(matrixIndex).toBe(385);
      expect(acceptedCount).toBe(11);

      for (const [outcome, httpStatus] of [
        ["unavailable", null],
        ["http_error", 503],
        ["timeout", null],
        ["transport_error", null],
      ] as const) {
        expect(
          ipnsRecoveryReceiptSchema.safeParse({
            ...resolvedReceipt,
            errorCode: null,
            httpStatus,
            outcome,
          }).success,
        ).toBe(false);
      }
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("rolls back the first domain when the second domain cycle conflicts", async () => {
    const collisionSchema = `recovery_atomic_${process.pid}_${Date.now()}`;
    const collisionUrl = `${adminDatabaseUrl}${adminDatabaseUrl.includes("?") ? "&" : "?"}options=-csearch_path%3D${collisionSchema}`;
    const admin = postgres(adminDatabaseUrl, { max: 1 });
    try {
      await admin.unsafe(`CREATE SCHEMA ${collisionSchema}`);
    } finally {
      await admin.end({ timeout: 5 });
    }
    try {
      await runMigrations(collisionUrl);
      const snapshot = await createSyntheticLifecycleSnapshot(dataDir, {
        coverage: "authoritative",
        folios: Array.from(
          { length: 25 },
          (_, index) => `ATOM-${String(index + 1).padStart(2, "0")}`,
        ),
        label: "atomic-second-domain-conflict",
        observedAt: "2026-08-29T06:00:00.000Z",
      });
      await recordRunStarted(collisionUrl, snapshot.request);
      const verified = await verifyPreparedInput(
        dataDir,
        snapshot.reference,
        parsePreparedPilot,
        snapshot.snapshot.snapshotId,
      );
      await loadPreparedPilot(
        collisionUrl,
        snapshot.request,
        structuredClone(verified.prepared) as PreparedPilot,
        {
          idempotencyKey: syntheticLoaderIdempotencyKey(
            snapshot.request.workflowId,
            snapshot.reference.preparedInputId,
          ),
          preparedManifest: verified.manifest,
          preparedReference: verified.reference,
          requestSha256: countyIngestRequestSha256(snapshot.request),
          runId: snapshot.request.runId,
          snapshot: verified.snapshot,
        },
      );
      const dryRun = await buildPublicationDryRun({
        dataDir,
        databaseUrl: collisionUrl,
        exportMode: "authoritative",
        runId: snapshot.request.runId,
        targets,
      });
      const plan = validatePublicationPlan(
        JSON.parse(
          await readFile(
            path.join(
              dataDir,
              dryRun.outputRoot,
              "publication-dry-run-plan.json",
            ),
            "utf8",
          ),
        ),
      );
      const planIdentity = {
        county: "pasco" as const,
        planId: plan.planId,
        planSha256: plan.planSha256,
      };
      await approvePublicationPlan(collisionUrl, {
        ...planIdentity,
        approverReference: "synthetic_controller",
      });
      const sharedPrior = await calculateIpfsCid("atomic-shared-prior\n");
      const intendedAt = "2026-08-29T06:01:00.000Z";
      const requestEvidence = {
        observations: resolverObservations(sharedPrior, intendedAt),
      };
      const resolutionEvidenceSha256 = independentSha256(
        independentCanonical(requestEvidence),
      );
      const lockedTarget = plan.targets.queryTable;
      const queryIdentity = {
        approvedTargetCid: plan.graph.queryTableRoot.expectedCid,
        domain: "query_table",
        intendedAt,
        ipnsLabel: lockedTarget.ipnsLabel,
        ipnsNetworkKey: lockedTarget.ipnsNetworkKey,
        priorCid: sharedPrior,
        providerBucket: lockedTarget.bucket,
        providerTargetIdentity: `filebase:${lockedTarget.bucket}`,
        publicationPlanId: plan.planId,
        publicationPlanSha256: plan.planSha256,
        resolutionEvidence: requestEvidence,
        resolutionEvidenceSha256,
      };
      const queryIntentSha256 = independentSha256(
        independentCanonical(queryIdentity),
      );
      const queryIntentId = `ipnsintent_${independentSha256(
        JSON.stringify([
          "1.1.0",
          "Publish/pasco/ipns-intent",
          plan.planId,
          "query_table",
          queryIntentSha256,
        ]),
      ).slice(0, 32)}`;
      const preseedObservations = resolverObservations(
        sharedPrior,
        "2026-08-29T06:01:01.000Z",
      ).map((observation, ordinal) => ({
        classification: "resolved",
        endpointId: observation.endpointId,
        observedAt: observation.observedAt,
        observedCid: observation.observedCid,
        ordinal,
        receipt: observation.receipt,
        resolverKind: observation.resolverKind,
      }));
      const observationsCanonical = independentCanonical(preseedObservations);
      const receiptsCanonical = independentCanonical(
        preseedObservations.map(({ ordinal, receipt }) => ({
          ordinal,
          receipt,
        })),
      );
      const queryCycleId = independentCycleId({
        attemptId: null,
        domain: "query_table",
        intentId: queryIntentId,
        planId: plan.planId,
        planSha256: plan.planSha256,
        sequence: 0,
      });
      const seed = postgres(collisionUrl, { max: 1 });
      try {
        await seed`
          INSERT INTO oracle_publication_ipns_intents (
            intent_id, intent_sha256, publication_plan_id,
            publication_plan_sha256, domain, provider_target_identity,
            provider_bucket, ipns_label, ipns_network_key, prior_cid,
            approved_target_cid, resolution_evidence,
            resolution_evidence_sha256, intended_at
          ) VALUES (
            ${queryIntentId}, ${queryIntentSha256}, ${plan.planId},
            ${plan.planSha256}, 'query_table',
            ${queryIdentity.providerTargetIdentity},
            ${queryIdentity.providerBucket}, ${queryIdentity.ipnsLabel},
            ${queryIdentity.ipnsNetworkKey}, ${sharedPrior},
            ${queryIdentity.approvedTargetCid},
            ${seed.json(requestEvidence as unknown as postgres.JSONValue)},
            ${resolutionEvidenceSha256},
            ${intendedAt}
          )
        `;
        await seed`
          INSERT INTO oracle_publication_ipns_resolution_cycles (
            resolution_cycle_id, intent_id, domain, attempt_id,
            intent_revision, cycle_sequence, evidence_sha256,
            observation_count, observations_canonical, classification,
            receipts_canonical, receipt_identity_sha256
          ) VALUES (
            ${queryCycleId}, ${queryIntentId}, 'query_table', null,
            1, 0, ${independentSha256(observationsCanonical)}, 2,
            ${observationsCanonical}, 'prior_observed',
            ${receiptsCanonical}, ${independentSha256(receiptsCanonical)}
          )
        `;
      } finally {
        await seed.end({ timeout: 5 });
      }

      await expect(
        recordIpnsIntents(collisionUrl, {
          ...planIdentity,
          targets: [
            {
              domain: "open_data",
              intendedAt,
              priorCid: sharedPrior,
              resolutionEvidence: requestEvidence,
            },
            {
              domain: "query_table",
              intendedAt,
              priorCid: sharedPrior,
              resolutionEvidence: requestEvidence,
            },
          ],
        }),
      ).rejects.toThrow("resolution cycle identity or evidence changed");
      const check = postgres(collisionUrl, { max: 1 });
      try {
        const counts = await check<
          {
            cycles: number;
            events: number;
            open_intents: number;
            query_intents: number;
            states: number;
          }[]
        >`
          SELECT
            (SELECT count(*)::int
             FROM oracle_publication_ipns_intents
             WHERE domain = 'open_data') AS open_intents,
            (SELECT count(*)::int
             FROM oracle_publication_ipns_intents
             WHERE domain = 'query_table') AS query_intents,
            (SELECT count(*)::int
             FROM oracle_publication_ipns_resolution_cycles) AS cycles,
            (SELECT count(*)::int
             FROM oracle_publication_ipns_intent_state) AS states,
            (SELECT count(*)::int
             FROM oracle_publication_ipns_intent_events) AS events
        `;
        expect(counts[0]).toEqual({
          cycles: 1,
          events: 0,
          open_intents: 0,
          query_intents: 1,
          states: 0,
        });
      } finally {
        await check.end({ timeout: 5 });
      }
    } finally {
      const cleanup = postgres(adminDatabaseUrl, { max: 1 });
      try {
        await cleanup.unsafe(`DROP SCHEMA ${collisionSchema} CASCADE`);
      } finally {
        await cleanup.end({ timeout: 5 });
      }
    }
  }, 120_000);

  it("rejects free-form or malformed receipts at the application boundary", async () => {
    const invalidReceipts: Record<string, unknown>[] = [
      { ...resolvedReceipt, authorization: "Bearer forbidden" },
      { ...resolvedReceipt, cookie: "session=forbidden" },
      { ...resolvedReceipt, token: "forbidden" },
      { ...resolvedReceipt, password: "forbidden" },
      { ...resolvedReceipt, accessKey: "forbidden" },
      { ...resolvedReceipt, secretKey: "forbidden" },
      { ...resolvedReceipt, privateKey: "-----BEGIN PRIVATE KEY-----" },
      { ...resolvedReceipt, headers: { "x-arbitrary": "forbidden" } },
      { ...resolvedReceipt, arbitrary: "forbidden" },
      { ...resolvedReceipt, providerRequestIdHash: "malformed" },
      { ...resolvedReceipt, httpStatus: 99 },
      { ...resolvedReceipt, httpStatus: 600 },
      { ...resolvedReceipt, responseBytes: -1 },
      {
        ...resolvedReceipt,
        responseBytes: IPNS_RECOVERY_MAX_RESPONSE_BYTES + 1,
      },
      { ...resolvedReceipt, latencyMs: -1 },
      { ...resolvedReceipt, latencyMs: IPNS_RECOVERY_MAX_LATENCY_MS + 1 },
      { ...resolvedReceipt, oversized: "x".repeat(5_000) },
    ];
    for (const [index, receipt] of invalidReceipts.entries()) {
      await expect(
        recoverIpnsIntent(schemaDatabaseUrl, {
          attemptId: null,
          intentId: openIntentId,
          observations: resolverObservations(
            priorCid,
            `2026-08-29T05:02:${String(index).padStart(2, "0")}.000Z`,
            receipt,
          ),
          recoverySequence: 100 + index,
        }),
      ).rejects.toThrow("strict validation");
    }
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        attemptId: null,
        intentId: openIntentId,
        observations: resolverObservations(
          priorCid,
          "2026-08-29T05:03:00.000Z",
        ),
        recoverySequence: 999,
        resolutionCycleId: `resolution_${"a".repeat(32)}`,
      }),
    ).rejects.toThrow("strict validation");
  });

  it("independently rejects the same malicious receipts through direct SQL", async () => {
    const invalidReceipts: Record<string, unknown>[] = [
      { ...resolvedReceipt, authorization: "Bearer forbidden" },
      { ...resolvedReceipt, cookie: "session=forbidden" },
      { ...resolvedReceipt, token: "forbidden" },
      { ...resolvedReceipt, password: "forbidden" },
      { ...resolvedReceipt, accessKey: "forbidden" },
      { ...resolvedReceipt, secretKey: "forbidden" },
      { ...resolvedReceipt, privateKey: "-----BEGIN PRIVATE KEY-----" },
      { ...resolvedReceipt, headers: { "x-arbitrary": "forbidden" } },
      { ...resolvedReceipt, arbitrary: "forbidden" },
      { ...resolvedReceipt, responseBodyHash: "malformed" },
      { ...resolvedReceipt, httpStatus: 99 },
      { ...resolvedReceipt, httpStatus: 600 },
      { ...resolvedReceipt, responseBytes: -1 },
      {
        ...resolvedReceipt,
        responseBytes: IPNS_RECOVERY_MAX_RESPONSE_BYTES + 1,
      },
      { ...resolvedReceipt, latencyMs: -1 },
      { ...resolvedReceipt, latencyMs: IPNS_RECOVERY_MAX_LATENCY_MS + 1 },
      { ...resolvedReceipt, oversized: "x".repeat(5_000) },
    ];
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const intent = await sql<
        {
          domain: string;
          intent_id: string;
          publication_plan_id: string;
          publication_plan_sha256: string;
          revision: number;
        }[]
      >`
        SELECT intent.domain, intent.intent_id, intent.publication_plan_id,
               intent.publication_plan_sha256, state.revision
        FROM oracle_publication_ipns_intents intent
        JOIN oracle_publication_ipns_intent_state state USING (intent_id)
        WHERE intent.intent_id = ${openIntentId}
      `;
      for (const [index, receipt] of invalidReceipts.entries()) {
        const sequence = 200 + index;
        const canonicalObservations = resolverObservations(
          priorCid,
          `2026-08-29T05:04:${String(index).padStart(2, "0")}.000Z`,
          receipt,
        ).map((observation, ordinal) => ({
          classification: "resolved",
          endpointId: observation.endpointId,
          observedAt: observation.observedAt,
          observedCid: observation.observedCid,
          ordinal,
          receipt,
          resolverKind: observation.resolverKind,
        }));
        const observationsCanonical = independentCanonical(
          canonicalObservations,
        );
        const receiptsCanonical = independentCanonical(
          canonicalObservations.map(({ ordinal, receipt: itemReceipt }) => ({
            ordinal,
            receipt: itemReceipt,
          })),
        );
        const cycleId = independentCycleId({
          attemptId: null,
          domain: intent[0]!.domain,
          intentId: intent[0]!.intent_id,
          planId: intent[0]!.publication_plan_id,
          planSha256: intent[0]!.publication_plan_sha256,
          sequence,
        });
        await expect(
          sql`
            INSERT INTO oracle_publication_ipns_resolution_cycles (
              resolution_cycle_id, intent_id, domain, attempt_id,
              intent_revision, cycle_sequence, evidence_sha256,
              observation_count, observations_canonical, classification,
              receipts_canonical, receipt_identity_sha256
            ) VALUES (
              ${cycleId}, ${openIntentId}, ${intent[0]!.domain}, null,
              ${intent[0]!.revision}, ${sequence},
              ${independentSha256(observationsCanonical)}, 2,
              ${observationsCanonical}, 'prior_observed',
              ${receiptsCanonical}, ${independentSha256(receiptsCanonical)}
            )
          `,
        ).rejects.toThrow(/receipt|observation/i);
      }
    } finally {
      await sql.end({ timeout: 5 });
    }
  });

  it("makes exact replay idempotent and rejects cycle or evidence reuse", async () => {
    const first = {
      attemptId: null,
      intentId: openIntentId,
      observations: resolverObservations(priorCid, "2026-08-29T05:10:00.000Z"),
      recoverySequence: 1,
    };
    const conflicting = {
      ...first,
      observations: resolverObservations(priorCid, "2026-08-29T05:10:01.000Z"),
    };
    const concurrent = await Promise.allSettled([
      recoverIpnsIntent(schemaDatabaseUrl, first),
      recoverIpnsIntent(schemaDatabaseUrl, conflicting),
    ]);
    expect(
      concurrent.filter((item) => item.status === "fulfilled"),
    ).toHaveLength(1);
    expect(
      concurrent.filter((item) => item.status === "rejected"),
    ).toHaveLength(1);
    const winning = concurrent[0]!.status === "fulfilled" ? first : conflicting;
    await expect(
      Promise.all([
        recoverIpnsIntent(schemaDatabaseUrl, winning),
        recoverIpnsIntent(schemaDatabaseUrl, winning),
      ]),
    ).resolves.toEqual([
      expect.objectContaining({ state: "prior_confirmed" }),
      expect.objectContaining({ state: "prior_confirmed" }),
    ]);
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        ...winning,
        recoverySequence: 2,
      }),
    ).rejects.toThrow(
      "canonical recovery evidence was already recorded under another cycle",
    );
    await expect(
      recoverIpnsIntent(schemaDatabaseUrl, {
        ...winning,
        observations:
          winning === first ? conflicting.observations : first.observations,
      }),
    ).rejects.toThrow("resolution cycle identity or evidence changed");
  });

  it("binds the derived cycle to its intent and permits independent equivalent evidence", async () => {
    const sql = postgres(schemaDatabaseUrl, { max: 1 });
    try {
      const initial = await sql<
        {
          domain: string;
          evidence_sha256: string;
          intent_id: string;
          resolution_cycle_id: string;
        }[]
      >`
        SELECT domain, evidence_sha256, intent_id, resolution_cycle_id
        FROM oracle_publication_ipns_resolution_cycles
        WHERE cycle_sequence = 0
        ORDER BY domain
      `;
      expect(initial).toHaveLength(2);
      expect(initial[0]!.evidence_sha256).toBe(initial[1]!.evidence_sha256);
      expect(initial[0]!.intent_id).not.toBe(initial[1]!.intent_id);
      expect(initial[0]!.resolution_cycle_id).not.toBe(
        initial[1]!.resolution_cycle_id,
      );

      const query = await sql<
        {
          domain: string;
          intent_id: string;
          publication_plan_id: string;
          publication_plan_sha256: string;
          revision: number;
        }[]
      >`
        SELECT intent.domain, intent.intent_id, intent.publication_plan_id,
               intent.publication_plan_sha256, state.revision
        FROM oracle_publication_ipns_intents intent
        JOIN oracle_publication_ipns_intent_state state USING (intent_id)
        WHERE intent.intent_id = ${queryIntentId}
      `;
      const openCycleId = initial.find(
        (item) => item.domain === "open_data",
      )!.resolution_cycle_id;
      const canonicalObservations = resolverObservations(
        priorCid,
        "2026-08-29T05:11:00.000Z",
      ).map((observation, ordinal) => ({
        classification: "resolved",
        endpointId: observation.endpointId,
        observedAt: observation.observedAt,
        observedCid: observation.observedCid,
        ordinal,
        receipt: observation.receipt,
        resolverKind: observation.resolverKind,
      }));
      const observationsCanonical = independentCanonical(canonicalObservations);
      const receiptsCanonical = independentCanonical(
        canonicalObservations.map(({ ordinal, receipt }) => ({
          ordinal,
          receipt,
        })),
      );
      await expect(
        sql`
          INSERT INTO oracle_publication_ipns_resolution_cycles (
            resolution_cycle_id, intent_id, domain, attempt_id,
            intent_revision, cycle_sequence, evidence_sha256,
            observation_count, observations_canonical, classification,
            receipts_canonical, receipt_identity_sha256
          ) VALUES (
            ${openCycleId}, ${queryIntentId}, ${query[0]!.domain}, null,
            ${query[0]!.revision}, 500,
            ${independentSha256(observationsCanonical)}, 2,
            ${observationsCanonical}, 'prior_observed',
            ${receiptsCanonical}, ${independentSha256(receiptsCanonical)}
          )
        `,
      ).rejects.toThrow(/server-derived|duplicate key/i);
    } finally {
      await sql.end({ timeout: 5 });
    }
  });
});
