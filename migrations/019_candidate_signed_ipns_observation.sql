-- Candidate-only signed IPNS resolution evidence. This records bounded,
-- normalized metadata and hashes, never the signed record bytes, response
-- bodies, request headers, credentials, or endpoint URLs.

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_signed_ipns_observations (
  evidence_id text PRIMARY KEY CHECK (
    evidence_id ~ '^demosignedobservation_[a-f0-9]{32}$'
  ),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL REFERENCES oracle_candidate_demo_approvals(approval_id),
  intent_id text NOT NULL REFERENCES oracle_candidate_demo_ipns_intents(intent_id),
  domain text NOT NULL CHECK (domain = 'query_table'),
  network_key text NOT NULL CHECK (network_key ~ '^k51[0-9a-z]{59}$'),
  prior_cid text NOT NULL CHECK (
    oracle_candidate_demo_cid_is_valid(prior_cid)
  ),
  target_cid text NOT NULL CHECK (target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  policy_version text NOT NULL CHECK (
    policy_version = 'candidate_signed_ipns_observation_v1'
  ),
  classification text NOT NULL CHECK (classification IN (
    'converged', 'propagation_pending', 'source_split',
    'source_unavailable', 'signed_record_invalid',
    'signed_record_expired', 'unexpected_cid'
  )),
  request_count integer NOT NULL CHECK (request_count BETWEEN 3 AND 4),
  evidence_sha256 text NOT NULL UNIQUE CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),

  control_http_status integer CHECK (control_http_status BETWEEN 100 AND 599),
  control_latency_ms integer NOT NULL CHECK (control_latency_ms BETWEEN 0 AND 120000),
  control_observed_at timestamptz NOT NULL,
  control_observed_cid text CHECK (
    control_observed_cid IS NULL OR
    oracle_candidate_demo_cid_is_valid(control_observed_cid)
  ),
  control_outcome text NOT NULL CHECK (control_outcome IN (
    'resolved', 'unavailable', 'http_error', 'timeout', 'transport_error',
    'redirect_rejected', 'response_too_large', 'malformed_response'
  )),
  control_response_bytes integer NOT NULL CHECK (
    control_response_bytes BETWEEN 0 AND 65536
  ),
  control_response_sha256 text NOT NULL CHECK (
    control_response_sha256 ~ '^[a-f0-9]{64}$'
  ),

  gateway_http_status integer CHECK (gateway_http_status BETWEEN 100 AND 599),
  gateway_latency_ms integer NOT NULL CHECK (gateway_latency_ms BETWEEN 0 AND 120000),
  gateway_observed_at timestamptz NOT NULL,
  gateway_observed_cid text CHECK (
    gateway_observed_cid IS NULL OR
    oracle_candidate_demo_cid_is_valid(gateway_observed_cid)
  ),
  gateway_outcome text NOT NULL CHECK (gateway_outcome IN (
    'resolved', 'unavailable', 'http_error', 'timeout', 'transport_error',
    'redirect_rejected', 'response_too_large', 'malformed_response'
  )),
  gateway_response_bytes integer NOT NULL CHECK (gateway_response_bytes = 0),
  gateway_response_sha256 text NOT NULL CHECK (
    gateway_response_sha256 ~ '^[a-f0-9]{64}$'
  ),

  delegated_http_status integer CHECK (delegated_http_status BETWEEN 100 AND 599),
  delegated_latency_ms integer NOT NULL CHECK (delegated_latency_ms BETWEEN 0 AND 120000),
  delegated_observed_at timestamptz NOT NULL,
  delegated_observed_cid text CHECK (
    delegated_observed_cid IS NULL OR
    oracle_candidate_demo_cid_is_valid(delegated_observed_cid)
  ),
  delegated_outcome text NOT NULL CHECK (delegated_outcome IN (
    'validated', 'unavailable', 'http_error', 'timeout', 'transport_error',
    'redirect_rejected', 'content_type_invalid', 'response_too_large',
    'malformed_record', 'invalid_signature', 'identity_mismatch',
    'expired_record', 'unexpected_cid'
  )),
  delegated_request_count integer NOT NULL CHECK (delegated_request_count BETWEEN 1 AND 2),
  delegated_response_bytes integer NOT NULL CHECK (
    delegated_response_bytes BETWEEN 0 AND 10240
  ),
  delegated_response_sha256 text NOT NULL CHECK (
    delegated_response_sha256 ~ '^[a-f0-9]{64}$'
  ),
  delegated_sequence numeric(20, 0) CHECK (delegated_sequence >= 0),
  delegated_ttl_nanoseconds numeric(20, 0) CHECK (delegated_ttl_nanoseconds >= 0),
  delegated_validation_result text NOT NULL CHECK (
    delegated_validation_result IN (
      'valid_target', 'valid_prior', 'unexpected_cid', 'unavailable',
      'http_error', 'timeout', 'transport_error', 'redirect_rejected',
      'content_type_invalid', 'response_too_large', 'malformed_record',
      'invalid_signature', 'identity_mismatch', 'expired_record'
    )
  ),
  delegated_validity timestamptz,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, evidence_sha256),
  CHECK (request_count = 2 + delegated_request_count),
  CHECK (
    (control_outcome = 'resolved') IS NOT DISTINCT FROM
    (control_observed_cid IS NOT NULL AND control_http_status IS NOT NULL)
  ),
  CHECK (
    (gateway_outcome = 'resolved') IS NOT DISTINCT FROM
    (gateway_observed_cid IS NOT NULL AND gateway_http_status IS NOT NULL)
  )
);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_signed_ipns_observation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  approval_count integer;
  intent_row oracle_candidate_demo_ipns_intents%ROWTYPE;
  pending_effects integer;
  plan_row oracle_candidate_demo_plans%ROWTYPE;
  signed_is_valid boolean;
  third_cid_present boolean;
BEGIN
  SELECT * INTO plan_row
  FROM oracle_candidate_demo_plans
  WHERE demo_plan_id = NEW.demo_plan_id;
  IF NOT FOUND OR
     plan_row.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     plan_row.coverage_mode IS DISTINCT FROM 'sample' OR
     plan_row.state NOT IN ('executing', 'manual_intervention_required') THEN
    RAISE EXCEPTION 'signed IPNS evidence does not match the candidate sample plan';
  END IF;

  SELECT count(*) INTO approval_count
  FROM oracle_candidate_demo_approvals
  WHERE approval_id = NEW.approval_id
    AND demo_plan_id = NEW.demo_plan_id
    AND demo_plan_sha256 = NEW.demo_plan_sha256;
  IF approval_count IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'signed IPNS evidence does not match the exact approval';
  END IF;

  SELECT * INTO intent_row
  FROM oracle_candidate_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF NOT FOUND OR
     intent_row.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     intent_row.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     intent_row.domain IS DISTINCT FROM NEW.domain OR
     intent_row.ipns_network_key IS DISTINCT FROM NEW.network_key OR
     intent_row.prior_cid IS DISTINCT FROM NEW.prior_cid OR
     intent_row.target_cid IS DISTINCT FROM NEW.target_cid OR
     intent_row.state NOT IN ('update_ambiguous', 'target_observed', 'verified') THEN
    RAISE EXCEPTION 'signed IPNS evidence does not match the immutable query intent';
  END IF;

  SELECT count(*) INTO pending_effects
  FROM oracle_candidate_demo_object_effects
  WHERE demo_plan_id = NEW.demo_plan_id AND status IS DISTINCT FROM 'verified';
  IF pending_effects IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'signed IPNS evidence requires all object effects verified';
  END IF;

  signed_is_valid := NEW.delegated_validation_result IN (
    'valid_target', 'valid_prior', 'unexpected_cid'
  );
  IF signed_is_valid IS DISTINCT FROM (
       NEW.delegated_observed_cid IS NOT NULL AND
       NEW.delegated_http_status IS NOT NULL AND
       NEW.delegated_sequence IS NOT NULL AND
       NEW.delegated_validity IS NOT NULL AND
       NEW.delegated_outcome IN ('validated', 'unexpected_cid')
     ) THEN
    RAISE EXCEPTION 'signed IPNS validation metadata is inconsistent';
  END IF;

  IF (NEW.delegated_validation_result = 'valid_target' AND
      NEW.delegated_observed_cid IS DISTINCT FROM NEW.target_cid) OR
     (NEW.delegated_validation_result = 'valid_prior' AND
      NEW.delegated_observed_cid IS DISTINCT FROM NEW.prior_cid) OR
     (NEW.delegated_validation_result = 'expired_record' AND
      NEW.delegated_outcome IS DISTINCT FROM 'expired_record') THEN
    RAISE EXCEPTION 'signed IPNS result does not match its CID or outcome';
  END IF;

  third_cid_present :=
    (NEW.control_observed_cid IS NOT NULL AND
     NEW.control_observed_cid NOT IN (NEW.prior_cid, NEW.target_cid)) OR
    (NEW.gateway_observed_cid IS NOT NULL AND
     NEW.gateway_observed_cid NOT IN (NEW.prior_cid, NEW.target_cid)) OR
    (NEW.delegated_observed_cid IS NOT NULL AND
     NEW.delegated_observed_cid NOT IN (NEW.prior_cid, NEW.target_cid));

  IF (CASE NEW.classification
    WHEN 'converged' THEN
      NEW.control_outcome = 'resolved' AND
      NEW.control_observed_cid = NEW.target_cid AND
      NEW.gateway_outcome = 'resolved' AND
      NEW.gateway_observed_cid = NEW.target_cid AND
      NEW.delegated_validation_result = 'valid_target' AND
      NEW.delegated_observed_cid = NEW.target_cid
    WHEN 'propagation_pending' THEN
      NEW.delegated_validation_result = 'valid_prior' AND
      NEW.delegated_observed_cid = NEW.prior_cid AND NOT third_cid_present
    WHEN 'source_split' THEN
      NEW.delegated_validation_result = 'valid_target' AND
      NEW.control_outcome = 'resolved' AND
      NEW.gateway_outcome = 'resolved' AND
      NOT third_cid_present AND
      (NEW.control_observed_cid IS DISTINCT FROM NEW.target_cid OR
       NEW.gateway_observed_cid IS DISTINCT FROM NEW.target_cid)
    WHEN 'source_unavailable' THEN
      NEW.delegated_validation_result = 'valid_target' AND
      (NEW.control_outcome IS DISTINCT FROM 'resolved' OR
       NEW.gateway_outcome IS DISTINCT FROM 'resolved') AND
      NOT third_cid_present
    WHEN 'signed_record_expired' THEN
      NEW.delegated_validation_result = 'expired_record' AND NOT third_cid_present
    WHEN 'signed_record_invalid' THEN
      NEW.delegated_validation_result NOT IN (
        'valid_target', 'valid_prior', 'unexpected_cid', 'expired_record'
      ) AND NOT third_cid_present
    WHEN 'unexpected_cid' THEN
      third_cid_present OR
      NEW.delegated_validation_result = 'unexpected_cid'
    ELSE false
  END) IS NOT TRUE THEN
    RAISE EXCEPTION 'signed IPNS evidence classification is inconsistent';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_signed_ipns_observation_guard
  ON oracle_candidate_demo_signed_ipns_observations;
CREATE TRIGGER oracle_candidate_signed_ipns_observation_guard
  BEFORE INSERT ON oracle_candidate_demo_signed_ipns_observations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_signed_ipns_observation();

DROP TRIGGER IF EXISTS oracle_candidate_signed_ipns_observations_immutable
  ON oracle_candidate_demo_signed_ipns_observations;
CREATE TRIGGER oracle_candidate_signed_ipns_observations_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_signed_ipns_observations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation();

COMMENT ON TABLE oracle_candidate_demo_signed_ipns_observations IS
  'Bounded signed-IPNS metadata for one candidate-owned query-table recovery; contains no raw record, URL, header, credential, or response body.';
