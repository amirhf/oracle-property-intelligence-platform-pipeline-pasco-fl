import { describe, expect, it } from "vitest";

import {
  IPNS_INTENT_STATES,
  IPNS_TRANSITION_EVENTS,
  IPNS_TRANSITION_TABLE,
  nextIpnsIntentState,
  type IpnsIntentState,
  type IpnsTransitionEvent,
} from "../../src/db/ipns-intent.js";

// Review-owned expectations, intentionally independent from the production
// transition map. Every omitted state/event cell is expected to be illegal.
const EXPECTED: Partial<
  Record<IpnsIntentState, Partial<Record<IpnsTransitionEvent, IpnsIntentState>>>
> = {
  intent_recorded: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  manual_intervention_required: {
    manual_intervention_required: "manual_intervention_required",
  },
  mutation_acknowledged: {
    prior_observed: "update_ambiguous",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
    verification_pending: "verification_pending",
  },
  prior_confirmed: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
    update_started: "update_in_flight",
  },
  rollback_ambiguous: {
    prior_observed: "rollback_verified",
    rollback_started: "rollback_in_flight",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_in_flight: {
    mutation_acknowledged: "rollback_in_flight",
    mutation_failed: "manual_intervention_required",
    prior_observed: "rollback_verified",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_requested: {
    prior_observed: "rollback_verified",
    rollback_requested: "rollback_requested",
    rollback_started: "rollback_in_flight",
    split_prior_target: "rollback_ambiguous",
    target_observed: "rollback_requested",
    timeout_transport_uncertainty: "rollback_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  rollback_verified: {
    prior_observed: "rollback_verified",
    rollback_verified: "rollback_verified",
    unexpected_third_cid: "manual_intervention_required",
  },
  target_observed: {
    rollback_requested: "rollback_requested",
    split_prior_target: "verification_pending",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "verification_pending",
    unexpected_third_cid: "manual_intervention_required",
    verification_pending: "verification_pending",
    verified: "verified",
  },
  update_ambiguous: {
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  update_in_flight: {
    mutation_acknowledged: "mutation_acknowledged",
    mutation_failed: "failed_terminal",
    prior_observed: "prior_confirmed",
    split_prior_target: "update_ambiguous",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "update_ambiguous",
    unexpected_third_cid: "manual_intervention_required",
  },
  verification_pending: {
    prior_observed: "update_ambiguous",
    split_prior_target: "verification_pending",
    target_observed: "target_observed",
    timeout_transport_uncertainty: "verification_pending",
    unexpected_third_cid: "manual_intervention_required",
    verified: "verified",
  },
  verified: {
    rollback_requested: "rollback_requested",
    target_observed: "verified",
    unexpected_third_cid: "manual_intervention_required",
    verified: "verified",
  },
};

describe("total IPNS recovery transition model", () => {
  it("defines every state/observation cell and rejects every illegal cell", () => {
    expect(Object.keys(IPNS_TRANSITION_TABLE).sort()).toEqual(
      [...IPNS_INTENT_STATES].sort(),
    );
    for (const state of IPNS_INTENT_STATES) {
      expect(Object.keys(IPNS_TRANSITION_TABLE[state]).sort()).toEqual(
        [...IPNS_TRANSITION_EVENTS].sort(),
      );
      for (const event of IPNS_TRANSITION_EVENTS) {
        const expected = EXPECTED[state]?.[event];
        // Terminal event behavior is uniform and independently asserted here.
        const terminalFailureStates: readonly IpnsIntentState[] = [
          "intent_recorded",
          "manual_intervention_required",
          "mutation_acknowledged",
          "prior_confirmed",
          "rollback_ambiguous",
          "rollback_in_flight",
          "rollback_requested",
          "target_observed",
          "update_ambiguous",
          "update_in_flight",
          "verification_pending",
        ];
        const uniform =
          event === "terminal_failure" && terminalFailureStates.includes(state)
            ? "failed_terminal"
            : event === "terminal_cancellation" &&
                [
                  "intent_recorded",
                  "manual_intervention_required",
                  "prior_confirmed",
                ].includes(state)
              ? "cancelled_terminal"
              : event === "manual_intervention_required" &&
                  state === "manual_intervention_required"
                ? "manual_intervention_required"
                : undefined;
        const target = expected ?? uniform;
        if (target) {
          expect(nextIpnsIntentState(state, event)).toBe(target);
        } else {
          expect(
            IPNS_TRANSITION_TABLE[state][event],
            `${state} + ${event}`,
          ).toBeNull();
          expect(() => nextIpnsIntentState(state, event)).toThrow(
            "invalid transition",
          );
        }
      }
    }
  });

  it("provides a non-wedging rollback terminal path", () => {
    expect(nextIpnsIntentState("verified", "rollback_requested")).toBe(
      "rollback_requested",
    );
    expect(nextIpnsIntentState("rollback_requested", "rollback_started")).toBe(
      "rollback_in_flight",
    );
    expect(nextIpnsIntentState("rollback_in_flight", "prior_observed")).toBe(
      "rollback_verified",
    );
  });
});
