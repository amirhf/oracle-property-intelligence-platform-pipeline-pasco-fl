import { describe, expect, it } from "vitest";

import { sha256 } from "../../src/lib/hash.js";
import {
  executeCandidateSourceSnapshotIpnsController,
  renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement,
  type CandidateSourceSnapshotIpnsBoundary,
  type CandidateSourceSnapshotIpnsClassification,
  type CandidateSourceSnapshotIpnsCommand,
  type CandidateSourceSnapshotIpnsDurableAuthorization,
  type CandidateSourceSnapshotIpnsIntent,
  type CandidateSourceSnapshotIpnsJournal,
  type CandidateSourceSnapshotIpnsObservation,
  type CandidateSourceSnapshotIpnsReplayAuthorization,
  type CandidateSourceSnapshotIpnsRollbackAuthorization,
} from "../../src/publication/candidate-source-snapshot-ipns-controller.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const approvalId = `snapshotdemoapproval_${"a".repeat(32)}`;
const uploadClosureId = `snapshotdemouploadclosure_${"b".repeat(32)}`;
const authorizedAt = "2026-08-31T00:00:00.000Z";
const authorizerReference = "synthetic-controller";

function exactReplayAuthorization(
  input: Omit<
    CandidateSourceSnapshotIpnsReplayAuthorization,
    | "authorizationSha256"
    | "authorizationStatement"
    | "authorizedAt"
    | "authorizerReference"
  >,
): CandidateSourceSnapshotIpnsReplayAuthorization {
  const authorizationStatement =
    renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
      input,
      "update",
    );
  return {
    ...input,
    authorizationSha256: sha256(authorizationStatement),
    authorizationStatement,
    authorizedAt,
    authorizerReference,
  };
}

function exactRollbackAuthorization(
  input: Omit<
    CandidateSourceSnapshotIpnsRollbackAuthorization,
    | "authorizationSha256"
    | "authorizationStatement"
    | "authorizedAt"
    | "authorizerReference"
  >,
): CandidateSourceSnapshotIpnsRollbackAuthorization {
  const authorizationStatement =
    renderCandidateSourceSnapshotIpnsRetryAuthorizationStatement(
      input,
      "rollback",
    );
  return {
    ...input,
    authorizationSha256: sha256(authorizationStatement),
    authorizationStatement,
    authorizedAt,
    authorizerReference,
  };
}

function intents(
  mutationAttemptCount = 0,
): readonly [
  CandidateSourceSnapshotIpnsIntent,
  CandidateSourceSnapshotIpnsIntent,
] {
  const { plan } = syntheticCandidateSourceSnapshotDemo();
  return [
    {
      approvalId,
      cutoverPosition: 1,
      domain: "open_data",
      intentId: "intent_open",
      mutationAttemptCount,
      rollbackAttemptCount: 0,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
      rollbackPosition: 2,
      state: "prior_confirmed",
      targetCid: plan.targets.openData.targetCid,
      updateAttemptCount: mutationAttemptCount,
      uploadClosureId,
    },
    {
      approvalId,
      cutoverPosition: 2,
      domain: "query_table",
      intentId: "intent_query",
      mutationAttemptCount,
      rollbackAttemptCount: 0,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.queryTable.priorCid,
      resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
      rollbackPosition: 1,
      state: "prior_confirmed",
      targetCid: plan.targets.queryTable.targetCid,
      updateAttemptCount: mutationAttemptCount,
      uploadClosureId,
    },
  ];
}

function observation(
  classification: CandidateSourceSnapshotIpnsClassification,
  command: CandidateSourceSnapshotIpnsCommand,
): CandidateSourceSnapshotIpnsObservation {
  return {
    classification,
    observedCid:
      classification === "target"
        ? command.targetCid
        : classification === "prior"
          ? command.priorCid
          : classification === "unexpected"
            ? "third-cid"
            : null,
  };
}

function harness(input: {
  durableAuthorizations?: readonly CandidateSourceSnapshotIpnsDurableAuthorization[];
  mutation?: Partial<
    Record<
      "open_data" | "query_table",
      CandidateSourceSnapshotIpnsClassification
    >
  >;
  mutationFreshness?: Partial<
    Record<
      "open_data" | "query_table",
      CandidateSourceSnapshotIpnsClassification
    >
  >;
  rollback?: CandidateSourceSnapshotIpnsClassification;
  rollbackFreshness?: CandidateSourceSnapshotIpnsClassification;
}) {
  const commands: CandidateSourceSnapshotIpnsCommand[] = [];
  const events: string[] = [];
  const durableAuthorizations = new Map(
    (input.durableAuthorizations ?? []).map((authorization) => [
      authorization.authorizationId,
      authorization,
    ]),
  );
  let freshnessCommand: CandidateSourceSnapshotIpnsCommand | null = null;
  const journal: CandidateSourceSnapshotIpnsJournal = {
    async beforeFreshnessObservation(command) {
      freshnessCommand = command;
      events.push(
        `journal:before-freshness:${command.action}:${command.domain}`,
      );
    },
    async beforeMutation(command) {
      commands.push(command);
      events.push(`journal:before-mutation:${command.domain}`);
      return command.authorizationId === null
        ? null
        : (durableAuthorizations.get(command.authorizationId) ?? null);
    },
    async beforeRollback(command) {
      commands.push(command);
      events.push(`journal:before-rollback:${command.domain}`);
      return durableAuthorizations.get(command.authorizationId) ?? null;
    },
    async markRolledBack(command) {
      events.push(`journal:rolled-back:${command.domain}`);
    },
    async markVerified(command) {
      events.push(`journal:verified:${command.domain}`);
    },
    async recordFreshnessObservation({ command, observation: observed }) {
      events.push(
        `journal:freshness:${command.action}:${command.domain}:${observed.classification}`,
      );
    },
    async recordObservation({ command, observation: observed }) {
      events.push(
        `journal:observation:${command.action}:${command.domain}:${observed.classification}`,
      );
    },
  };
  const boundary: CandidateSourceSnapshotIpnsBoundary = {
    async observeIdentity(domain) {
      events.push(`boundary:observe:${domain}`);
      if (!freshnessCommand || freshnessCommand.domain !== domain) {
        throw new Error("freshness command is unavailable");
      }
      const classification =
        freshnessCommand.action === "rollback"
          ? (input.rollbackFreshness ?? "target")
          : (input.mutationFreshness?.[domain] ?? "prior");
      return observation(classification, freshnessCommand);
    },
    async mutateAndObserve(command) {
      events.push(`boundary:mutate:${command.domain}`);
      return observation(input.mutation?.[command.domain] ?? "target", command);
    },
    async rollbackAndObserve(command) {
      events.push(`boundary:rollback:${command.domain}`);
      return observation(input.rollback ?? "prior", command);
    },
  };
  return { boundary, commands, events, journal };
}

describe("candidate source-snapshot closed IPNS controller", () => {
  it("journals and verifies open data before mutating and verifying the query table", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({});
    const result = await executeCandidateSourceSnapshotIpnsController({
      approvalId,
      boundary: test.boundary,
      executorEnabled: true,
      intents: intents(),
      journal: test.journal,
      plan,
      uploadClosureId,
    });

    expect(result).toStrictEqual({
      openData: "target",
      planId: plan.planId,
      planSha256: plan.planSha256,
      queryTable: "target",
      reason: "completed",
      rollback: "not_attempted",
      status: "completed",
    });
    expect(test.events).toStrictEqual([
      "journal:before-freshness:mutate:open_data",
      "boundary:observe:open_data",
      "journal:freshness:mutate:open_data:prior",
      "journal:before-mutation:open_data",
      "boundary:mutate:open_data",
      "journal:observation:mutate:open_data:target",
      "journal:verified:open_data",
      "journal:before-freshness:mutate:query_table",
      "boundary:observe:query_table",
      "journal:freshness:mutate:query_table:prior",
      "journal:before-mutation:query_table",
      "boundary:mutate:query_table",
      "journal:observation:mutate:query_table:target",
      "journal:verified:query_table",
    ]);
    expect(
      test.commands.map((command) => ({
        authorizationId: command.authorizationId,
        authorizationSha256: command.authorizationSha256,
      })),
    ).toStrictEqual([
      { authorizationId: null, authorizationSha256: null },
      { authorizationId: null, authorizationSha256: null },
    ]);
  });

  it.each(["split", "unavailable"] as const)(
    "stops on an open-data %s observation without a later mutation or rollback",
    async (classification) => {
      const { plan } = syntheticCandidateSourceSnapshotDemo();
      const test = harness({ mutation: { open_data: classification } });
      const result = await executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: intents(),
        journal: test.journal,
        plan,
        uploadClosureId,
      });

      expect(result.status).toBe("stopped");
      expect(result.queryTable).toBe("not_attempted");
      expect(result.rollback).toBe("not_attempted");
      expect(test.events).not.toContain("boundary:mutate:query_table");
      expect(test.events.some((event) => event.includes("rollback"))).toBe(
        false,
      );
    },
  );

  it("hard-stops on an unexpected third CID without overwrite or rollback", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({ mutation: { query_table: "unexpected" } });
    const result = await executeCandidateSourceSnapshotIpnsController({
      approvalId,
      boundary: test.boundary,
      executorEnabled: true,
      intents: intents(),
      journal: test.journal,
      plan,
      uploadClosureId,
    });

    expect(result).toMatchObject({
      queryTable: "unexpected",
      reason: "query_table_unexpected",
      rollback: "not_attempted",
      status: "stopped",
    });
    expect(test.events.some((event) => event.includes("rollback"))).toBe(false);
  });

  it("rejects an already-visible target without a terminal attempt and recovers it after exact durable retry evidence", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const missingAttempt = harness({
      mutationFreshness: { open_data: "target" },
    });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: missingAttempt.boundary,
        executorEnabled: true,
        intents: intents(),
        journal: missingAttempt.journal,
        plan,
        uploadClosureId,
      }),
    ).rejects.toThrow("requires a prior terminal update attempt");
    expect(missingAttempt.events).not.toContain("boundary:mutate:open_data");
    expect(missingAttempt.events).not.toContain(
      "journal:freshness:mutate:open_data:target",
    );

    const recoverableIntents = intents().map((intent) =>
      intent.domain === "open_data"
        ? {
            ...intent,
            mutationAttemptCount: 1,
            updateAttemptCount: 1,
          }
        : intent,
    );
    const authorization = exactReplayAuthorization({
      authorizationId: `snapshotdemoreplay_${"a".repeat(32)}`,
      authorizedAttempt: 2,
      domain: "open_data",
      intentId: recoverableIntents[0]!.intentId,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      targetCid: plan.targets.openData.targetCid,
    });
    const test = harness({
      durableAuthorizations: [authorization],
      mutationFreshness: { open_data: "target" },
    });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: recoverableIntents,
        journal: test.journal,
        plan,
        replayAuthorizations: [authorization],
        uploadClosureId,
      }),
    ).resolves.toMatchObject({ status: "completed" });
    expect(test.events).not.toContain("boundary:mutate:open_data");
    expect(test.events).toContain("boundary:mutate:query_table");
  });

  it("hard-stops a pre-mutation third CID without admitting or sending the PUT", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({
      mutationFreshness: { open_data: "unexpected" },
    });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: intents(),
        journal: test.journal,
        plan,
        uploadClosureId,
      }),
    ).resolves.toMatchObject({
      openData: "unexpected",
      status: "stopped",
    });
    expect(test.events.some((event) => event.includes("before-mutation"))).toBe(
      false,
    );
    expect(test.events.some((event) => event.includes("boundary:mutate"))).toBe(
      false,
    );
  });

  it("performs an ambiguous rollback attempt-2 retry only with its exact durable authorization", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const rollbackIntents = intents(0).map((intent) =>
      intent.domain === "open_data"
        ? {
            ...intent,
            mutationAttemptCount: 2,
            rollbackAttemptCount: 1,
            state: "rollback_ambiguous",
            updateAttemptCount: 1,
          }
        : { ...intent, state: "update_failed_prior_confirmed" },
    );
    const authorization = exactRollbackAuthorization({
      authorizationId: `snapshotdemoreplay_${"9".repeat(32)}`,
      authorizedAttempt: 2,
      domain: "open_data",
      intentId: rollbackIntents[0]!.intentId,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      targetCid: plan.targets.openData.targetCid,
    });
    const missing = harness({ rollback: "prior" });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: missing.boundary,
        executorEnabled: true,
        intents: rollbackIntents,
        journal: missing.journal,
        plan,
        uploadClosureId,
      }),
    ).rejects.toThrow("requires exact durable rollback authorization");
    expect(missing.events).toStrictEqual([]);

    const retry = harness({
      durableAuthorizations: [authorization],
      rollback: "prior",
    });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: retry.boundary,
        executorEnabled: true,
        intents: rollbackIntents,
        journal: retry.journal,
        plan,
        rollbackAuthorization: authorization,
        uploadClosureId,
      }),
    ).resolves.toMatchObject({ status: "rolled_back" });
    expect(retry.commands).toHaveLength(1);
    expect(retry.commands[0]).toMatchObject({
      action: "rollback",
      attemptNumber: 2,
      commandId: `${plan.planId}:${rollbackIntents[0]!.intentId}:rollback:2`,
    });
  });

  it("requires exact authorization before one reverse open-data rollback", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const rollbackAuthorization = exactRollbackAuthorization({
      authorizationId: `snapshotdemoreplay_${"c".repeat(32)}`,
      authorizedAttempt: 1,
      domain: "open_data",
      intentId: intents()[0].intentId,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      targetCid: plan.targets.openData.targetCid,
    });
    const test = harness({
      durableAuthorizations: [rollbackAuthorization],
      mutation: { query_table: "prior" },
      rollback: "prior",
    });
    const result = await executeCandidateSourceSnapshotIpnsController({
      approvalId,
      boundary: test.boundary,
      executorEnabled: true,
      intents: intents(),
      journal: test.journal,
      plan,
      rollbackAuthorization,
      uploadClosureId,
    });

    expect(result).toMatchObject({
      openData: "target",
      queryTable: "prior",
      reason: "query_table_prior_observed",
      rollback: "prior",
      status: "rolled_back",
    });
    expect(test.events.slice(-4)).toStrictEqual([
      "journal:before-rollback:open_data",
      "boundary:rollback:open_data",
      "journal:observation:rollback:open_data:prior",
      "journal:rolled-back:open_data",
    ]);
    expect(
      test.events.filter((event) => event.includes("boundary:rollback")),
    ).toStrictEqual(["boundary:rollback:open_data"]);
    expect(test.commands.at(-1)).toMatchObject({
      action: "rollback",
      authorizationId: rollbackAuthorization.authorizationId,
      authorizationSha256: rollbackAuthorization.authorizationSha256,
    });
  });

  it("stops without rollback when a definite query prior lacks authorization", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({ mutation: { query_table: "prior" } });
    const result = await executeCandidateSourceSnapshotIpnsController({
      approvalId,
      boundary: test.boundary,
      executorEnabled: true,
      intents: intents(),
      journal: test.journal,
      plan,
      uploadClosureId,
    });
    expect(result).toMatchObject({
      reason: "query_table_prior_observed",
      rollback: "not_attempted",
      status: "stopped",
    });
    expect(test.events.some((event) => event.includes("rollback"))).toBe(false);
  });

  it("rejects caller-only rollback authorization before boundary access", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const rollbackAuthorization = exactRollbackAuthorization({
      authorizationId: `snapshotdemoreplay_${"e".repeat(32)}`,
      authorizedAttempt: 1,
      domain: "open_data",
      intentId: intents()[0].intentId,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      targetCid: plan.targets.openData.targetCid,
    });
    const test = harness({ mutation: { query_table: "prior" } });

    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: intents(),
        journal: test.journal,
        plan,
        rollbackAuthorization,
        uploadClosureId,
      }),
    ).rejects.toThrow("journal did not confirm");
    expect(test.events.slice(-1)).toStrictEqual([
      "journal:before-rollback:open_data",
    ]);
    expect(test.events).not.toContain("boundary:rollback:open_data");
  });

  it("rejects a disabled executor before journaling or boundary access", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({});
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: false,
        intents: intents(),
        journal: test.journal,
        plan,
        uploadClosureId,
      }),
    ).rejects.toThrow("disabled");
    expect(test.events).toStrictEqual([]);
  });

  it("rejects a wrong plan binding or non-prior-confirmed intent before effects", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    for (const changed of [
      intents().map((intent, index) =>
        index === 0 ? { ...intent, planSha256: "0".repeat(64) } : intent,
      ),
      intents().map((intent, index) =>
        index === 1 ? { ...intent, state: "intent_recorded" } : intent,
      ),
    ]) {
      const test = harness({});
      await expect(
        executeCandidateSourceSnapshotIpnsController({
          approvalId,
          boundary: test.boundary,
          executorEnabled: true,
          intents: changed,
          journal: test.journal,
          plan,
          uploadClosureId,
        }),
      ).rejects.toThrow("exact recoverable plan intent");
      expect(test.events).toStrictEqual([]);
    }
  });

  it("requires exact explicit authorization before replaying a prior mutation attempt", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const retryIntents = intents(1);
    const test = harness({});
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: retryIntents,
        journal: test.journal,
        plan,
        uploadClosureId,
      }),
    ).rejects.toThrow("replay authorization");
    expect(test.events).toStrictEqual([]);

    const replayAuthorizations: CandidateSourceSnapshotIpnsReplayAuthorization[] =
      retryIntents.map((intent) =>
        exactReplayAuthorization({
          authorizationId: `snapshotdemoreplay_${(intent.domain === "open_data"
            ? "1"
            : "2"
          ).repeat(32)}`,
          authorizedAttempt: 2,
          domain: intent.domain,
          intentId: intent.intentId,
          planId: intent.planId,
          planSha256: intent.planSha256,
          priorCid: intent.priorCid,
          targetCid: intent.targetCid,
        }),
      );
    const callerOnly = harness({});
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: callerOnly.boundary,
        executorEnabled: true,
        intents: retryIntents,
        journal: callerOnly.journal,
        plan,
        replayAuthorizations,
        uploadClosureId,
      }),
    ).rejects.toThrow("journal did not confirm");
    expect(callerOnly.events).toStrictEqual([
      "journal:before-freshness:mutate:open_data",
      "boundary:observe:open_data",
      "journal:freshness:mutate:open_data:prior",
      "journal:before-mutation:open_data",
    ]);

    const durable = harness({ durableAuthorizations: replayAuthorizations });
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: durable.boundary,
        executorEnabled: true,
        intents: retryIntents,
        journal: durable.journal,
        plan,
        replayAuthorizations,
        uploadClosureId,
      }),
    ).resolves.toMatchObject({ status: "completed" });
    expect(
      durable.commands.map((command) => ({
        authorizationId: command.authorizationId,
        authorizationSha256: command.authorizationSha256,
      })),
    ).toStrictEqual(
      replayAuthorizations.map((authorization) => ({
        authorizationId: authorization.authorizationId,
        authorizationSha256: authorization.authorizationSha256,
      })),
    );
  });

  it("rejects a durable authorization hash mismatch before boundary access", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const retryIntents = intents(1);
    const replayAuthorizations: CandidateSourceSnapshotIpnsReplayAuthorization[] =
      retryIntents.map((intent, index) =>
        exactReplayAuthorization({
          authorizationId: `snapshotdemoreplay_${String(index + 5).repeat(32)}`,
          authorizedAttempt: 2,
          domain: intent.domain,
          intentId: intent.intentId,
          planId: intent.planId,
          planSha256: intent.planSha256,
          priorCid: intent.priorCid,
          targetCid: intent.targetCid,
        }),
      );
    const durable = harness({
      durableAuthorizations: replayAuthorizations.map((authorization, index) =>
        index === 0
          ? { ...authorization, authorizationSha256: "f".repeat(64) }
          : authorization,
      ),
    });

    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: durable.boundary,
        executorEnabled: true,
        intents: retryIntents,
        journal: durable.journal,
        plan,
        replayAuthorizations,
        uploadClosureId,
      }),
    ).rejects.toThrow("journal did not confirm");
    expect(durable.events).toStrictEqual([
      "journal:before-freshness:mutate:open_data",
      "boundary:observe:open_data",
      "journal:freshness:mutate:open_data:prior",
      "journal:before-mutation:open_data",
    ]);
  });

  it("does not call the boundary when durable mutation admission fails", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const test = harness({});
    test.journal.beforeMutation = async () => {
      test.events.push("journal:before-mutation:failed");
      throw new Error("journal unavailable");
    };
    await expect(
      executeCandidateSourceSnapshotIpnsController({
        approvalId,
        boundary: test.boundary,
        executorEnabled: true,
        intents: intents(),
        journal: test.journal,
        plan,
        uploadClosureId,
      }),
    ).rejects.toThrow("journal unavailable");
    expect(test.events).toStrictEqual([
      "journal:before-freshness:mutate:open_data",
      "boundary:observe:open_data",
      "journal:freshness:mutate:open_data:prior",
      "journal:before-mutation:failed",
    ]);
  });
});
