-- Candidate-only completion from one immutable human authorization and one
-- cryptographically validated signed-IPNS evidence record. This adds no
-- publication authority, intent, upload effect, or remote mutation.

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_delegated_completions (
  completion_id text PRIMARY KEY CHECK (
    completion_id ~ '^demodelegatedcompletion_[a-f0-9]{32}$'
  ),
  completion_sha256 text NOT NULL UNIQUE CHECK (
    completion_sha256 ~ '^[a-f0-9]{64}$'
  ),
  policy_id text NOT NULL CHECK (
    policy_id = 'candidate_filebase_delegated_v2'
  ),
  authorization_id text NOT NULL UNIQUE REFERENCES oracle_candidate_demo_delegated_resolver_policies(authorization_id),
  authorization_sha256 text NOT NULL CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  demo_plan_id text NOT NULL UNIQUE REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL REFERENCES oracle_candidate_demo_approvals(approval_id),
  query_intent_id text NOT NULL UNIQUE REFERENCES oracle_candidate_demo_ipns_intents(intent_id),
  query_target_cid text NOT NULL CHECK (
    query_target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  signed_evidence_id text NOT NULL UNIQUE REFERENCES oracle_candidate_demo_signed_ipns_observations(evidence_id),
  signed_evidence_sha256 text NOT NULL CHECK (
    signed_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  remote_mutation_performed boolean NOT NULL DEFAULT false CHECK (
    remote_mutation_performed = false
  ),
  scope text NOT NULL CHECK (scope = 'candidate_owned_non_authoritative_demo'),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_delegated_completion()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  binding_count integer;
  pending_effects integer;
  total_effects integer;
  expected_effects integer;
BEGIN
  SELECT count(*) INTO binding_count
  FROM oracle_candidate_demo_delegated_resolver_policies policy_auth
  JOIN oracle_candidate_demo_plans plan
    ON plan.demo_plan_id = policy_auth.demo_plan_id
   AND plan.demo_plan_sha256 = policy_auth.demo_plan_sha256
  JOIN oracle_candidate_demo_approvals approval
    ON approval.approval_id = policy_auth.approval_id
   AND approval.demo_plan_id = plan.demo_plan_id
   AND approval.demo_plan_sha256 = plan.demo_plan_sha256
  JOIN oracle_candidate_demo_ipns_intents query_intent
    ON query_intent.intent_id = policy_auth.query_intent_id
   AND query_intent.demo_plan_id = plan.demo_plan_id
   AND query_intent.demo_plan_sha256 = plan.demo_plan_sha256
   AND query_intent.domain = 'query_table'
  JOIN oracle_candidate_demo_ipns_intents open_intent
    ON open_intent.demo_plan_id = plan.demo_plan_id
   AND open_intent.demo_plan_sha256 = plan.demo_plan_sha256
   AND open_intent.domain = 'open_data'
  JOIN oracle_candidate_demo_signed_ipns_observations evidence
    ON evidence.evidence_id = policy_auth.signed_evidence_id
   AND evidence.evidence_sha256 = policy_auth.signed_evidence_sha256
   AND evidence.intent_id = query_intent.intent_id
  WHERE policy_auth.authorization_id = NEW.authorization_id
    AND policy_auth.authorization_sha256 = NEW.authorization_sha256
    AND policy_auth.policy_id = NEW.policy_id
    AND policy_auth.demo_plan_id = NEW.demo_plan_id
    AND policy_auth.demo_plan_sha256 = NEW.demo_plan_sha256
    AND policy_auth.approval_id = NEW.approval_id
    AND policy_auth.query_intent_id = NEW.query_intent_id
    AND policy_auth.query_target_cid = NEW.query_target_cid
    AND policy_auth.signed_evidence_id = NEW.signed_evidence_id
    AND policy_auth.signed_evidence_sha256 = NEW.signed_evidence_sha256
    AND plan.coverage_mode = 'sample'
    AND plan.state = 'manual_intervention_required'
    AND open_intent.state = 'verified'
    AND query_intent.state = 'update_ambiguous'
    AND evidence.classification = 'converged'
    AND evidence.delegated_validation_result = 'valid_target'
    AND evidence.delegated_observed_cid = NEW.query_target_cid
    AND evidence.control_observed_cid = NEW.query_target_cid
    AND evidence.gateway_observed_cid = NEW.query_target_cid;
  IF binding_count IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'candidate delegated completion lacks one exact verified binding';
  END IF;

  SELECT object_count INTO expected_effects
  FROM oracle_candidate_demo_plans
  WHERE demo_plan_id = NEW.demo_plan_id;
  SELECT count(*), count(*) FILTER (WHERE status IS DISTINCT FROM 'verified')
  INTO total_effects, pending_effects
  FROM oracle_candidate_demo_object_effects
  WHERE demo_plan_id = NEW.demo_plan_id;
  IF total_effects IS DISTINCT FROM expected_effects OR
     pending_effects IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'candidate delegated completion requires verified object effects';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_delegated_completion_guard
  ON oracle_candidate_demo_delegated_completions;
CREATE TRIGGER oracle_candidate_delegated_completion_guard
  BEFORE INSERT ON oracle_candidate_demo_delegated_completions
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_delegated_completion();

DROP TRIGGER IF EXISTS oracle_candidate_delegated_completions_immutable
  ON oracle_candidate_demo_delegated_completions;
CREATE TRIGGER oracle_candidate_delegated_completions_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_delegated_completions
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_delegated_intent_verification()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  completion_count integer;
BEGIN
  IF OLD.domain = 'query_table' AND
     OLD.state = 'update_ambiguous' AND
     NEW.state = 'verified' THEN
    SELECT count(*) INTO completion_count
    FROM oracle_candidate_demo_delegated_completions completion
    WHERE completion.demo_plan_id = OLD.demo_plan_id
      AND completion.demo_plan_sha256 = OLD.demo_plan_sha256
      AND completion.query_intent_id = OLD.intent_id
      AND completion.query_target_cid = OLD.target_cid
      AND completion.remote_mutation_performed = false;
    IF completion_count IS DISTINCT FROM 1 THEN
      RAISE EXCEPTION 'delegated query verification requires immutable completion evidence';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_delegated_intent_verification_guard
  ON oracle_candidate_demo_ipns_intents;
CREATE TRIGGER oracle_candidate_delegated_intent_verification_guard
  BEFORE UPDATE ON oracle_candidate_demo_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_delegated_intent_verification();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_delegated_plan_completion()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  completion_count integer;
  verified_intents integer;
BEGIN
  IF OLD.state = 'manual_intervention_required' AND NEW.state = 'completed' THEN
    SELECT count(*) INTO completion_count
    FROM oracle_candidate_demo_delegated_completions completion
    WHERE completion.demo_plan_id = OLD.demo_plan_id
      AND completion.demo_plan_sha256 = OLD.demo_plan_sha256
      AND completion.remote_mutation_performed = false;
    SELECT count(*) INTO verified_intents
    FROM oracle_candidate_demo_ipns_intents intent
    WHERE intent.demo_plan_id = OLD.demo_plan_id
      AND intent.demo_plan_sha256 = OLD.demo_plan_sha256
      AND intent.domain IN ('open_data', 'query_table')
      AND intent.state = 'verified';
    IF completion_count IS DISTINCT FROM 1 OR
       verified_intents IS DISTINCT FROM 2 THEN
      RAISE EXCEPTION 'candidate delegated plan completion requires exact verified intents';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_delegated_plan_completion_guard
  ON oracle_candidate_demo_plans;
CREATE TRIGGER oracle_candidate_delegated_plan_completion_guard
  BEFORE UPDATE ON oracle_candidate_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_delegated_plan_completion();

COMMENT ON TABLE oracle_candidate_demo_delegated_completions IS
  'Candidate-only no-mutation completion evidence; never owner/canonical publication authority.';
