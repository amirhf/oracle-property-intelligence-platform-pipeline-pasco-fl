import { describe, expect, it } from "vitest";

import {
  executeCandidateSourceSnapshotIpnsController,
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
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.openData.priorCid,
      resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
      rollbackPosition: 2,
      state: "prior_confirmed",
      targetCid: plan.targets.openData.targetCid,
      uploadClosureId,
    },
    {
      approvalId,
      cutoverPosition: 2,
      domain: "query_table",
      intentId: "intent_query",
      mutationAttemptCount,
      planId: plan.planId,
      planSha256: plan.planSha256,
      priorCid: plan.targets.queryTable.priorCid,
      resolverPolicy: "candidate_source_snapshot_filebase_delegated_v1",
      rollbackPosition: 1,
      state: "prior_confirmed",
      targetCid: plan.targets.queryTable.targetCid,
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
  rollback?: CandidateSourceSnapshotIpnsClassification;
}) {
  const commands: CandidateSourceSnapshotIpnsCommand[] = [];
  const events: string[] = [];
  const durableAuthorizations = new Map(
    (input.durableAuthorizations ?? []).map((authorization) => [
      authorization.authorizationId,
      authorization,
    ]),
  );
  const journal: CandidateSourceSnapshotIpnsJournal = {
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
    async recordObservation({ command, observation: observed }) {
      events.push(
        `journal:observation:${command.action}:${command.domain}:${observed.classification}`,
      );
    },
  };
  const boundary: CandidateSourceSnapshotIpnsBoundary = {
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
      "journal:before-mutation:open_data",
      "boundary:mutate:open_data",
      "journal:observation:mutate:open_data:target",
      "journal:verified:open_data",
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

  it("requires exact authorization before one reverse open-data rollback", async () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const rollbackAuthorization: CandidateSourceSnapshotIpnsRollbackAuthorization =
      {
        authorizationId: `snapshotdemoreplay_${"c".repeat(32)}`,
        authorizationSha256: "d".repeat(64),
        authorizedAttempt: 1,
        domain: "open_data",
        intentId: intents()[0].intentId,
        planId: plan.planId,
        planSha256: plan.planSha256,
        priorCid: plan.targets.openData.priorCid,
        targetCid: plan.targets.openData.targetCid,
      };
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
    const rollbackAuthorization: CandidateSourceSnapshotIpnsRollbackAuthorization =
      {
        authorizationId: `snapshotdemoreplay_${"e".repeat(32)}`,
        authorizationSha256: "f".repeat(64),
        authorizedAttempt: 1,
        domain: "open_data",
        intentId: intents()[0].intentId,
        planId: plan.planId,
        planSha256: plan.planSha256,
        priorCid: plan.targets.openData.priorCid,
        targetCid: plan.targets.openData.targetCid,
      };
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
      ).rejects.toThrow("exact prior-confirmed");
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
      retryIntents.map((intent) => ({
        authorizationId: `snapshotdemoreplay_${(intent.domain === "open_data"
          ? "1"
          : "2"
        ).repeat(32)}`,
        authorizationSha256:
          intent.domain === "open_data" ? "3".repeat(64) : "4".repeat(64),
        authorizedAttempt: 2,
        domain: intent.domain,
        intentId: intent.intentId,
        planId: intent.planId,
        planSha256: intent.planSha256,
        priorCid: intent.priorCid,
        targetCid: intent.targetCid,
      }));
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
      retryIntents.map((intent, index) => ({
        authorizationId: `snapshotdemoreplay_${String(index + 5).repeat(32)}`,
        authorizationSha256: String(index + 7).repeat(64),
        authorizedAttempt: 2,
        domain: intent.domain,
        intentId: intent.intentId,
        planId: intent.planId,
        planSha256: intent.planSha256,
        priorCid: intent.priorCid,
        targetCid: intent.targetCid,
      }));
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
    expect(durable.events).toStrictEqual(["journal:before-mutation:open_data"]);
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
    expect(test.events).toStrictEqual(["journal:before-mutation:failed"]);
  });
});
