-- Separate human authorization for the signed delegated-IPNS candidate-demo
-- recovery policy. It does not modify the approved plan or grant any
-- owner/canonical publication authority.

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_delegated_resolver_policies (
  authorization_id text PRIMARY KEY CHECK (
    authorization_id ~ '^demodelegatedauthorization_[a-f0-9]{32}$'
  ),
  authorization_sha256 text NOT NULL UNIQUE CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  policy_id text NOT NULL CHECK (
    policy_id = 'candidate_filebase_delegated_v2'
  ),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL REFERENCES oracle_candidate_demo_approvals(approval_id),
  query_intent_id text NOT NULL REFERENCES oracle_candidate_demo_ipns_intents(intent_id),
  query_network_key text NOT NULL CHECK (query_network_key ~ '^k51[0-9a-z]{59}$'),
  query_prior_cid text NOT NULL CHECK (
    oracle_candidate_demo_cid_is_valid(query_prior_cid)
  ),
  query_target_cid text NOT NULL CHECK (
    query_target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  signed_evidence_id text NOT NULL REFERENCES oracle_candidate_demo_signed_ipns_observations(evidence_id),
  signed_evidence_sha256 text NOT NULL CHECK (
    signed_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorizer_reference text NOT NULL CHECK (
    authorizer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  scope text NOT NULL CHECK (scope = 'candidate_owned_non_authoritative_demo'),
  required_authorities text[] NOT NULL CHECK (
    required_authorities = ARRAY[
      'filebase_control', 'filebase_gateway', 'ipfs_delegated_signed_record'
    ]::text[]
  ),
  diagnostic_resolvers text[] NOT NULL CHECK (
    diagnostic_resolvers = ARRAY['dweb_link', 'ipfs_io']::text[]
  ),
  owner_canonical_authority boolean NOT NULL DEFAULT false CHECK (
    owner_canonical_authority = false
  ),
  UNIQUE (demo_plan_id, policy_id)
);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_delegated_policy()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  binding_count integer;
BEGIN
  SELECT count(*) INTO binding_count
  FROM oracle_candidate_demo_plans plan
  JOIN oracle_candidate_demo_approvals approval
    ON approval.demo_plan_id = plan.demo_plan_id
   AND approval.demo_plan_sha256 = plan.demo_plan_sha256
  JOIN oracle_candidate_demo_ipns_intents intent
    ON intent.demo_plan_id = plan.demo_plan_id
   AND intent.demo_plan_sha256 = plan.demo_plan_sha256
   AND intent.domain = 'query_table'
  JOIN oracle_candidate_demo_signed_ipns_observations evidence
    ON evidence.demo_plan_id = plan.demo_plan_id
   AND evidence.demo_plan_sha256 = plan.demo_plan_sha256
   AND evidence.approval_id = approval.approval_id
   AND evidence.intent_id = intent.intent_id
  WHERE plan.demo_plan_id = NEW.demo_plan_id
    AND plan.demo_plan_sha256 = NEW.demo_plan_sha256
    AND plan.coverage_mode = 'sample'
    AND plan.state = 'manual_intervention_required'
    AND approval.approval_id = NEW.approval_id
    AND intent.intent_id = NEW.query_intent_id
    AND intent.ipns_network_key = NEW.query_network_key
    AND intent.prior_cid = NEW.query_prior_cid
    AND intent.target_cid = NEW.query_target_cid
    AND intent.state = 'update_ambiguous'
    AND evidence.evidence_id = NEW.signed_evidence_id
    AND evidence.evidence_sha256 = NEW.signed_evidence_sha256
    AND evidence.classification = 'converged';
  IF binding_count IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'candidate delegated policy lacks exact converged evidence binding';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_delegated_policy_guard
  ON oracle_candidate_demo_delegated_resolver_policies;
CREATE TRIGGER oracle_candidate_delegated_policy_guard
  BEFORE INSERT ON oracle_candidate_demo_delegated_resolver_policies
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_delegated_policy();

DROP TRIGGER IF EXISTS oracle_candidate_delegated_resolver_policies_immutable
  ON oracle_candidate_demo_delegated_resolver_policies;
CREATE TRIGGER oracle_candidate_delegated_resolver_policies_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_delegated_resolver_policies
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation();

COMMENT ON TABLE oracle_candidate_demo_delegated_resolver_policies IS
  'Immutable candidate-only signed-IPNS recovery authorization; never owner/canonical publication authority.';
