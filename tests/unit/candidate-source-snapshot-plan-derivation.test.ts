import { describe, expect, it } from "vitest";

import {
  assertExactCandidateSourceSnapshotPlanDerivationReplay,
  createCandidateSourceSnapshotPlanDerivationIdentity,
} from "../../src/db/candidate-source-snapshot-plan-derivation.js";
import { DurableConflictError } from "../../src/lib/durability-errors.js";

const oldPlanId = `snapshotdemo_${"a".repeat(32)}`;
const newPlanId = `snapshotdemo_${"b".repeat(32)}`;
const oldPlanSha256 = "c".repeat(64);
const newPlanSha256 = "d".repeat(64);

function plan(input: {
  maxRequests: number;
  planId: string;
  planSha256: string;
  requestEnvelope: unknown;
  version: "2.0.0" | "2.1.0";
}) {
  return {
    plan_id: input.planId,
    plan_payload: {
      classification: {
        publicationClass: "candidate_owned_source_snapshot_demo",
      },
      costEnvelope: { maximumTotalUsd: input.version === "2.0.0" ? 6 : 7 },
      formatPadding: input.version === "2.0.0" ? "legacy" : "compact",
      limits: { maxConcurrency: 8, maxRequests: input.maxRequests },
      planId: input.planId,
      planSha256: input.planSha256,
      requestEnvelope: input.requestEnvelope,
      source: { snapshotId: "snapshot_test" },
      version: input.version,
    },
    plan_sha256: input.planSha256,
    request_envelope: input.requestEnvelope,
  };
}

const predecessorPlan = plan({
  maxRequests: 1_000_000,
  planId: oldPlanId,
  planSha256: oldPlanSha256,
  requestEnvelope: { schemaVersion: "candidate-request-envelope-v2" },
  version: "2.0.0",
});
const derivedPlan = plan({
  maxRequests: 1_100_000,
  planId: newPlanId,
  planSha256: newPlanSha256,
  requestEnvelope: {
    categoryRequests: { bucket_names_preflight: [8, 48] },
    schemaVersion: "candidate-request-envelope-v3",
  },
  version: "2.1.0",
});

describe("candidate source-snapshot plan derivation", () => {
  it("derives one deterministic audit identity from the exact v2/v2.1 pair", () => {
    const first = createCandidateSourceSnapshotPlanDerivationIdentity({
      derivedAt: "2026-08-31T12:00:00.000Z",
      derivedPlan,
      predecessorPlan,
    });
    const replay = createCandidateSourceSnapshotPlanDerivationIdentity({
      derivedAt: "2026-08-31T12:00:00.000Z",
      derivedPlan,
      predecessorPlan,
    });

    expect(replay).toEqual(first);
    expect(first.derivationId).toMatch(
      /^snapshotdemoderivation_[a-f0-9]{32}$/u,
    );
    expect(() =>
      assertExactCandidateSourceSnapshotPlanDerivationReplay(first, replay),
    ).not.toThrow();
  });

  it("rejects a conflicting replay instead of replacing audit history", () => {
    const stored = createCandidateSourceSnapshotPlanDerivationIdentity({
      derivedAt: "2026-08-31T12:00:00.000Z",
      derivedPlan,
      predecessorPlan,
    });
    const conflicting = createCandidateSourceSnapshotPlanDerivationIdentity({
      derivedAt: "2026-08-31T12:00:01.000Z",
      derivedPlan,
      predecessorPlan,
    });

    expect(() =>
      assertExactCandidateSourceSnapshotPlanDerivationReplay(
        conflicting,
        stored,
      ),
    ).toThrow(DurableConflictError);
  });

  it("rejects derivation when non-envelope publication identity changes", () => {
    const changed = structuredClone(derivedPlan);
    changed.plan_payload.source = { snapshotId: "snapshot_other" };

    expect(() =>
      createCandidateSourceSnapshotPlanDerivationIdentity({
        derivedAt: "2026-08-31T12:00:00.000Z",
        derivedPlan: changed,
        predecessorPlan,
      }),
    ).toThrow(/changes more than its request and cost envelope/u);
  });
});
