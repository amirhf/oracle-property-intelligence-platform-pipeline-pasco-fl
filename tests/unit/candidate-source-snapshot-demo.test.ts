import { describe, expect, it } from "vitest";

import {
  CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS,
  CANDIDATE_SOURCE_SNAPSHOT_PLAN_REQUEST_CEILING,
  conservativeCandidateSourceSnapshotPricing,
  createCandidateSourceSnapshotCostEnvelope,
  createCandidateSourceSnapshotDemoPlan,
  createCandidateSourceSnapshotRequestEnvelope,
  validateCandidateSourceSnapshotDemoPlan,
} from "../../src/publication/candidate-source-snapshot-demo.js";
import { loadCandidateSourceSnapshotPlanningConfig } from "../../src/publication/candidate-source-snapshot-config.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

describe("candidate source-snapshot demo plan", () => {
  it("binds a separate noncanonical class, exact preflight, and external plan artifact", () => {
    const { exactUpload, plan } = syntheticCandidateSourceSnapshotDemo();
    expect(validateCandidateSourceSnapshotDemoPlan(plan)).toEqual(plan);
    expect(plan.classification).toMatchObject({
      canonical: false,
      elephantOwned: false,
      independentlyPascoCertified: false,
      ownerControlled: false,
      publicationClass: "candidate_owned_source_snapshot_demo",
      resourceOwner: "candidate",
    });
    expect(plan.preflight.capacityProfile.subscriptionTierStatus).toBe(
      "human_confirmation_required",
    );
    expect(plan).not.toHaveProperty("planArtifact");
    expect(exactUpload.planArtifact.remoteObjectKey).toBe(
      `${plan.targets.controlPrefix}candidate-source-snapshot-plan.json`,
    );
  });

  it("prices the preliminary full inventory under explicit conservative classes", () => {
    const limits = {
      ...CANDIDATE_SOURCE_SNAPSHOT_HARD_CEILINGS,
      maxRequests: CANDIDATE_SOURCE_SNAPSHOT_PLAN_REQUEST_CEILING,
    };
    const pricing = conservativeCandidateSourceSnapshotPricing({
      fixedAccountPlanEvidence: "human_confirmation_required",
      fixedAccountPlanMonthlyUsd: 7.5,
      requestUsdPerThousand: 0.0045,
      storageUsdPerGib: 0.0162,
    });
    const requests = createCandidateSourceSnapshotRequestEnvelope({
      limits,
      objectCount: 325_312,
    });
    const cost = createCandidateSourceSnapshotCostEnvelope({
      inventoryBytes: 3_474_519_090,
      limits,
      pricing,
      requestEnvelope: requests,
    });
    const legacyMaximumCost = 3_267_142_549 / 2 ** 30 + (325_254 * 3) / 1_000;
    expect(Math.round(legacyMaximumCost)).toBe(979);
    expect(legacyMaximumCost).toBeGreaterThan(978);
    expect(requests.successfulTotalRequests).toBe(333_643);
    expect(requests.maximumTotalRequests).toBe(1_080_000);
    expect(requests.categoryRequests).toEqual({
      ambiguous_upload_inspection: [0, 24_000],
      bucket_names_preflight: [8, 48],
      control_public_observation: [18, 42],
      final_credential_free_verification: [8_303, 79_590],
      names_mutation: [2, 2],
      recovery: [0, 338],
      rollback: [0, 44],
      upload_provider_cid: [325_312, 975_936],
    });
    expect(requests.finalVerification).toMatchObject({
      deterministicRequiredMaximumRequests: 74_727,
      logicalRequests: 8_303,
      protectedHeadroomRequests: 4_863,
    });
    expect(cost.fixedAccountPlanMonthlyUsd).toBe(7.5);
    expect(cost.requestUsd).toEqual({
      maximumAttempts: 4.86,
      successfulExecution: 1.5013935,
    });
    expect(cost.maximumTotalUsd).toBeLessThan(25);
    expect(cost.maximumTotalUsd).toBe(12.412421548644);
  });

  it("rejects the previously proposed thousand-dollar rates and non-fail-closed config", () => {
    expect(() =>
      conservativeCandidateSourceSnapshotPricing({
        fixedAccountPlanEvidence: "human_confirmation_required",
        fixedAccountPlanMonthlyUsd: 7.5,
        requestUsdPerThousand: 1,
        storageUsdPerGib: 1,
      }),
    ).toThrow("reviewed conservative rates");
    expect(() =>
      loadCandidateSourceSnapshotPlanningConfig(
        { CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED: "true" },
        { evidence: "human_confirmation_required", monthlyUsd: 7.5 },
      ),
    ).toThrow("requires CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=false");
  });

  it("rejects target/preflight drift and protected sample replacement", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    expect(() =>
      validateCandidateSourceSnapshotDemoPlan({
        ...plan,
        preflight: {
          ...plan.preflight,
          identities: plan.preflight.identities.map((identity) =>
            identity.domain === "open_data"
              ? { ...identity, controlCid: plan.targets.openData.targetCid }
              : identity,
          ),
        },
      }),
    ).toThrow();
    expect(() =>
      validateCandidateSourceSnapshotDemoPlan({
        ...plan,
        protectedSampleRollback: {
          ...plan.protectedSampleRollback,
          openData: {
            ...plan.protectedSampleRollback.openData,
            bucket: plan.targets.openData.bucket,
          },
        },
      }),
    ).toThrow();
  });

  it("rejects request-count and cost envelopes that understate immutable execution", () => {
    const { plan } = syntheticCandidateSourceSnapshotDemo();
    const { planId: _planId, planSha256: _planSha256, ...identityInput } = plan;
    expect(() =>
      createCandidateSourceSnapshotDemoPlan({
        ...identityInput,
        requestEnvelope: {
          ...plan.requestEnvelope,
          categoryRequests: {
            ...plan.requestEnvelope.categoryRequests,
            upload_provider_cid: [
              plan.requestEnvelope.categoryRequests.upload_provider_cid[0],
              plan.requestEnvelope.categoryRequests.upload_provider_cid[1] - 1,
            ],
          },
          maximumTotalRequests: plan.requestEnvelope.maximumTotalRequests - 1,
        },
      }),
    ).toThrow("request envelope");
    expect(() =>
      createCandidateSourceSnapshotDemoPlan({
        ...identityInput,
        costEnvelope: {
          ...plan.costEnvelope,
          maximumIncrementalUsd: 0,
          maximumTotalUsd: plan.costEnvelope.fixedAccountPlanMonthlyUsd,
        },
      }),
    ).toThrow("cost envelope");
    expect(() =>
      createCandidateSourceSnapshotDemoPlan({
        ...identityInput,
        requestEnvelope: {
          ...plan.requestEnvelope,
          successfulTotalRequests: 0,
        },
      }),
    ).toThrow();
  });
});
