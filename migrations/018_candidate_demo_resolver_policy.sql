-- Candidate-only resolver policy authorized by the controller for one exact
-- non-authoritative demo plan. This does not alter owner/canonical publication.

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_resolver_policies (
  policy_id text NOT NULL CHECK (policy_id = 'candidate_filebase_dweb_v1'),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL REFERENCES oracle_candidate_demo_approvals(approval_id),
  authorizer_reference text NOT NULL CHECK (
    authorizer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  scope text NOT NULL CHECK (scope = 'candidate_owned_non_authoritative_demo'),
  required_resolvers text[] NOT NULL CHECK (
    required_resolvers = ARRAY[
      'filebase_control', 'filebase_gateway', 'dweb_link'
    ]::text[]
  ),
  diagnostic_resolver text NOT NULL CHECK (diagnostic_resolver = 'ipfs_io'),
  owner_canonical_authority boolean NOT NULL DEFAULT false CHECK (
    owner_canonical_authority = false
  ),
  authorization_sha256 text NOT NULL CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  PRIMARY KEY (demo_plan_id, policy_id),
  UNIQUE (authorization_sha256)
);

DROP TRIGGER IF EXISTS oracle_candidate_demo_resolver_policies_immutable
  ON oracle_candidate_demo_resolver_policies;
CREATE TRIGGER oracle_candidate_demo_resolver_policies_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_resolver_policies
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation();

ALTER TABLE oracle_candidate_demo_resolution_cycles
  ADD COLUMN IF NOT EXISTS resolver_policy_id text;

ALTER TABLE oracle_candidate_demo_resolution_cycles
  DROP CONSTRAINT IF EXISTS oracle_candidate_demo_cycle_policy_check;
ALTER TABLE oracle_candidate_demo_resolution_cycles
  ADD CONSTRAINT oracle_candidate_demo_cycle_policy_check CHECK (
    resolver_policy_id IS NULL OR
    resolver_policy_id = 'candidate_filebase_dweb_v1'
  );

ALTER TABLE oracle_candidate_demo_resolution_cycles
  DROP CONSTRAINT IF EXISTS oracle_candidate_demo_cycle_policy_fk;
ALTER TABLE oracle_candidate_demo_resolution_cycles
  ADD CONSTRAINT oracle_candidate_demo_cycle_policy_fk
  FOREIGN KEY (demo_plan_id, resolver_policy_id)
  REFERENCES oracle_candidate_demo_resolver_policies(demo_plan_id, policy_id);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_demo_cycle_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_demo_ipns_intents%ROWTYPE;
  policy_count integer;
BEGIN
  SELECT * INTO intent_row
  FROM oracle_candidate_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF NOT FOUND OR
     intent_row.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     intent_row.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     intent_row.domain IS DISTINCT FROM NEW.domain THEN
    RAISE EXCEPTION 'candidate recovery cycle does not match its immutable intent';
  END IF;
  IF NEW.resolver_policy_id IS NOT NULL THEN
    SELECT count(*) INTO policy_count
    FROM oracle_candidate_demo_resolver_policies policy
    JOIN oracle_candidate_demo_approvals approval
      ON approval.approval_id = policy.approval_id
    JOIN oracle_candidate_demo_plans plan
      ON plan.demo_plan_id = policy.demo_plan_id
    WHERE policy.demo_plan_id = NEW.demo_plan_id
      AND policy.demo_plan_sha256 = NEW.demo_plan_sha256
      AND policy.policy_id = NEW.resolver_policy_id
      AND approval.demo_plan_id = NEW.demo_plan_id
      AND approval.demo_plan_sha256 = NEW.demo_plan_sha256
      AND plan.coverage_mode = 'sample';
    IF policy_count IS DISTINCT FROM 1 THEN
      RAISE EXCEPTION 'candidate recovery cycle lacks exact resolver policy authorization';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON TABLE oracle_candidate_demo_resolver_policies IS
  'Immutable candidate-owned demo resolver authorization; never owner/canonical publication authority.';
