-- Candidate-owned demonstration publication state. This is deliberately
-- isolated from oracle_publication_* owner/canonical publication state.

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_plans (
  demo_plan_id text PRIMARY KEY CHECK (demo_plan_id ~ '^demo_[a-f0-9]{32}$'),
  demo_plan_sha256 text NOT NULL UNIQUE CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_version text NOT NULL CHECK (plan_version = '1.0.0'),
  source_plan_id text NOT NULL CHECK (source_plan_id ~ '^plan_[a-f0-9]{32}$'),
  source_plan_sha256 text NOT NULL CHECK (source_plan_sha256 ~ '^[a-f0-9]{64}$'),
  coverage_mode text NOT NULL CHECK (coverage_mode IN ('sample', 'partial')),
  object_count integer NOT NULL CHECK (object_count > 0),
  total_bytes bigint NOT NULL CHECK (total_bytes >= 0),
  request_limit integer NOT NULL CHECK (request_limit > 0),
  budget_limit_usd numeric(12, 6) NOT NULL CHECK (budget_limit_usd >= 0),
  plan_payload jsonb NOT NULL,
  state text NOT NULL CHECK (state IN (
    'awaiting_configuration', 'awaiting_approval', 'approved', 'executing',
    'completed', 'manual_intervention_required', 'failed_terminal'
  )),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_approvals (
  approval_id text PRIMARY KEY CHECK (approval_id ~ '^demoapproval_[a-f0-9]{32}$'),
  demo_plan_id text NOT NULL UNIQUE REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  approver_reference text NOT NULL CHECK (
    approver_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  approved_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_object_effects (
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  object_key text NOT NULL CHECK (object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)'),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_cid text NOT NULL CHECK (expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  expected_bytes bigint NOT NULL CHECK (expected_bytes >= 0),
  status text NOT NULL CHECK (status IN ('pending', 'in_flight', 'verified', 'failed_terminal')),
  provider_cid text CHECK (
    provider_cid IS NULL OR provider_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  request_count integer NOT NULL DEFAULT 0 CHECK (request_count >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (demo_plan_id, domain, object_key),
  CHECK (
    (status IN ('pending', 'in_flight') AND provider_cid IS NULL) OR
    (status = 'verified' AND provider_cid = expected_cid AND receipt_sha256 IS NOT NULL) OR
    status = 'failed_terminal'
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_ipns_intents (
  intent_id text PRIMARY KEY CHECK (intent_id ~ '^demointent_[a-f0-9]{32}$'),
  intent_sha256 text NOT NULL UNIQUE CHECK (intent_sha256 ~ '^[a-f0-9]{64}$'),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  bucket text NOT NULL,
  ipns_label text NOT NULL,
  ipns_network_key text NOT NULL CHECK (ipns_network_key ~ '^k51[1-9A-HJ-NP-Za-km-z]+$'),
  prior_cid text NOT NULL CHECK (prior_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  target_cid text NOT NULL CHECK (target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  resolution_evidence_sha256 text NOT NULL CHECK (
    resolution_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  state text NOT NULL CHECK (state IN (
    'intent_recorded', 'prior_confirmed', 'update_in_flight',
    'target_observed', 'verified', 'update_ambiguous', 'unexpected_cid',
    'rollback_in_flight', 'rolled_back', 'failed_terminal'
  )),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  intended_at timestamptz NOT NULL,
  UNIQUE (demo_plan_id, domain)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_events (
  event_id text PRIMARY KEY CHECK (event_id ~ '^demoevent_[a-f0-9]{32}$'),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  event_type text NOT NULL,
  event_sha256 text NOT NULL CHECK (event_sha256 ~ '^[a-f0-9]{64}$'),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (demo_plan_id, event_sha256)
);

CREATE OR REPLACE FUNCTION oracle_reject_candidate_demo_identity_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_candidate_demo_approvals',
    'oracle_candidate_demo_events'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I_immutable ON %I',
      immutable_table, immutable_table
    );
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_demo_plan_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'candidate demo plan identity is immutable';
  END IF;
  IF OLD.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     OLD.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     OLD.plan_version IS DISTINCT FROM NEW.plan_version OR
     OLD.source_plan_id IS DISTINCT FROM NEW.source_plan_id OR
     OLD.source_plan_sha256 IS DISTINCT FROM NEW.source_plan_sha256 OR
     OLD.coverage_mode IS DISTINCT FROM NEW.coverage_mode OR
     OLD.object_count IS DISTINCT FROM NEW.object_count OR
     OLD.total_bytes IS DISTINCT FROM NEW.total_bytes OR
     OLD.request_limit IS DISTINCT FROM NEW.request_limit OR
     OLD.budget_limit_usd IS DISTINCT FROM NEW.budget_limit_usd OR
     OLD.plan_payload IS DISTINCT FROM NEW.plan_payload OR
     OLD.created_at IS DISTINCT FROM NEW.created_at THEN
    RAISE EXCEPTION 'candidate demo plan identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_demo_plan_identity_immutable
  ON oracle_candidate_demo_plans;
CREATE TRIGGER oracle_candidate_demo_plan_identity_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_demo_plan_identity();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_demo_intent_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'candidate demo IPNS intent identity is immutable';
  END IF;
  IF OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.intent_sha256 IS DISTINCT FROM NEW.intent_sha256 OR
     OLD.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     OLD.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.bucket IS DISTINCT FROM NEW.bucket OR
     OLD.ipns_label IS DISTINCT FROM NEW.ipns_label OR
     OLD.ipns_network_key IS DISTINCT FROM NEW.ipns_network_key OR
     OLD.prior_cid IS DISTINCT FROM NEW.prior_cid OR
     OLD.target_cid IS DISTINCT FROM NEW.target_cid OR
     OLD.resolution_evidence_sha256 IS DISTINCT FROM NEW.resolution_evidence_sha256 OR
     OLD.intended_at IS DISTINCT FROM NEW.intended_at THEN
    RAISE EXCEPTION 'candidate demo IPNS intent identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_demo_intent_identity_immutable
  ON oracle_candidate_demo_ipns_intents;
CREATE TRIGGER oracle_candidate_demo_intent_identity_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_demo_intent_identity();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_demo_object_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.object_key IS DISTINCT FROM NEW.object_key OR
     OLD.expected_sha256 IS DISTINCT FROM NEW.expected_sha256 OR
     OLD.expected_cid IS DISTINCT FROM NEW.expected_cid OR
     OLD.expected_bytes IS DISTINCT FROM NEW.expected_bytes THEN
    RAISE EXCEPTION 'candidate demo object identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_demo_object_identity_immutable
  ON oracle_candidate_demo_object_effects;
CREATE TRIGGER oracle_candidate_demo_object_identity_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_object_effects
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_demo_object_identity();

CREATE INDEX IF NOT EXISTS oracle_candidate_demo_objects_status_idx
  ON oracle_candidate_demo_object_effects(demo_plan_id, domain, status);

COMMENT ON TABLE oracle_candidate_demo_plans IS
  'Candidate-controlled protocol demonstration only; never owner/canonical publication authority.';
