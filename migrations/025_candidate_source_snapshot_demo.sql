-- Separate v2 candidate-owned source-snapshot demonstration durability.
-- Legacy oracle_candidate_demo_* v1 rows remain untouched and retain their
-- sample/partial semantics. This migration performs no approval or effect.

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_plans (
  plan_id text PRIMARY KEY CHECK (plan_id ~ '^snapshotdemo_[a-f0-9]{32}$'),
  plan_sha256 text NOT NULL UNIQUE CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_version text NOT NULL CHECK (plan_version = '2.0.0'),
  publication_class text NOT NULL CHECK (
    publication_class = 'candidate_owned_source_snapshot_demo'
  ),
  resource_owner text NOT NULL CHECK (resource_owner = 'candidate'),
  canonical boolean NOT NULL CHECK (canonical = false),
  elephant_owned boolean NOT NULL CHECK (elephant_owned = false),
  owner_controlled boolean NOT NULL CHECK (owner_controlled = false),
  independently_pasco_certified boolean NOT NULL CHECK (
    independently_pasco_certified = false
  ),
  namespace_id text NOT NULL CHECK (namespace_id ~ '^snapshotns_[a-f0-9]{32}$'),
  source_scope text NOT NULL CHECK (
    source_scope = 'exact_hash_bound_2026_08_23_parcel_snapshot'
  ),
  source_plan_id text NOT NULL CHECK (source_plan_id ~ '^plan_[a-f0-9]{32}$'),
  source_plan_sha256 text NOT NULL CHECK (source_plan_sha256 ~ '^[a-f0-9]{64}$'),
  authority_id text NOT NULL CHECK (authority_id ~ '^authority_[a-f0-9]{32}$'),
  snapshot_id text NOT NULL CHECK (snapshot_id ~ '^snapshot_[a-f0-9]{32}$'),
  materialization_id text NOT NULL CHECK (
    materialization_id ~ '^materialization_[a-f0-9]{32}$'
  ),
  materialization_sha256 text NOT NULL CHECK (
    materialization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  maximum_upload_object_count integer NOT NULL CHECK (
    maximum_upload_object_count BETWEEN 1 AND 350000
  ),
  maximum_upload_bytes bigint NOT NULL CHECK (
    maximum_upload_bytes BETWEEN 1 AND 4294967296
  ),
  exact_upload_object_count integer NOT NULL CHECK (
    exact_upload_object_count BETWEEN 1 AND maximum_upload_object_count
  ),
  exact_upload_bytes bigint NOT NULL CHECK (
    exact_upload_bytes BETWEEN 1 AND maximum_upload_bytes
  ),
  maximum_object_bytes bigint NOT NULL CHECK (
    maximum_object_bytes BETWEEN 1 AND 536870912
  ),
  maximum_concurrency integer NOT NULL CHECK (
    maximum_concurrency BETWEEN 1 AND 16
  ),
  maximum_retries integer NOT NULL CHECK (maximum_retries BETWEEN 0 AND 2),
  request_timeout_ms integer NOT NULL CHECK (
    request_timeout_ms BETWEEN 500 AND 20000
  ),
  request_limit integer NOT NULL CHECK (request_limit BETWEEN 1 AND 1000000),
  maximum_request_count integer NOT NULL CHECK (
    maximum_request_count BETWEEN 1 AND request_limit
  ),
  budget_limit_usd numeric(12, 9) NOT NULL CHECK (
    budget_limit_usd > 0 AND budget_limit_usd <= 25
  ),
  maximum_total_usd numeric(12, 9) NOT NULL CHECK (
    maximum_total_usd >= 0 AND maximum_total_usd <= budget_limit_usd
  ),
  fixed_account_plan_monthly_usd numeric(12, 9) NOT NULL CHECK (
    fixed_account_plan_monthly_usd >= 0 AND
    fixed_account_plan_monthly_usd <= maximum_total_usd
  ),
  inventory_root_sha256 text NOT NULL CHECK (
    inventory_root_sha256 ~ '^[a-f0-9]{64}$'
  ),
  inventory_root_cid text NOT NULL CHECK (
    inventory_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  inventory_shard_count integer NOT NULL CHECK (inventory_shard_count > 0),
  control_artifacts jsonb NOT NULL,
  request_envelope jsonb NOT NULL,
  cost_envelope jsonb NOT NULL,
  capacity_preflight jsonb NOT NULL,
  capacity_preflight_sha256 text NOT NULL CHECK (
    capacity_preflight_sha256 ~ '^[a-f0-9]{64}$'
  ),
  capacity_preflight_observed_at timestamptz NOT NULL,
  subscription_tier_status text NOT NULL CHECK (
    subscription_tier_status = 'human_confirmation_required'
  ),
  plan_payload jsonb NOT NULL,
  plan_artifact_bytes bigint NOT NULL CHECK (
    plan_artifact_bytes BETWEEN 1 AND 16777216
  ),
  plan_artifact_sha256 text NOT NULL CHECK (
    plan_artifact_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_artifact_cid text NOT NULL CHECK (
    plan_artifact_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  plan_artifact_logical_object_key text NOT NULL CHECK (
    plan_artifact_logical_object_key = 'candidate-source-snapshot-plan.json'
  ),
  plan_artifact_remote_object_key text NOT NULL CHECK (
    plan_artifact_remote_object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)' AND
    length(plan_artifact_remote_object_key) BETWEEN 1 AND 2048
  ),
  prepared_with_executor_disabled boolean NOT NULL CHECK (
    prepared_with_executor_disabled = true
  ),
  state text NOT NULL CHECK (state IN (
    'awaiting_configuration', 'awaiting_approval', 'approved', 'executing',
    'completed', 'manual_intervention_required', 'failed_terminal'
  )),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
  ,UNIQUE (
    plan_id, plan_sha256, plan_artifact_sha256, plan_artifact_cid,
    plan_artifact_remote_object_key, plan_artifact_bytes
  )
  ,CHECK (
    plan_payload->>'planId' = plan_id AND
    plan_payload->>'planSha256' = plan_sha256 AND
    plan_payload->>'version' = plan_version AND
    plan_payload->'classification'->>'publicationClass' = publication_class AND
    plan_payload->'classification'->>'resourceOwner' = resource_owner
  )
  ,CHECK (
    plan_payload->'preflight' = capacity_preflight AND
    plan_payload->'preflight'->>'evidenceSha256' = capacity_preflight_sha256 AND
    plan_payload->'preflight'->'capacityProfile'->>'subscriptionTierStatus' =
      subscription_tier_status AND
    plan_artifact_remote_object_key =
      (plan_payload->'targets'->>'controlPrefix') || plan_artifact_logical_object_key
  )
  ,CHECK (
    (subscription_tier_status = 'human_confirmation_required' AND
      state = 'awaiting_configuration') OR
    state <> 'awaiting_configuration'
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_capacity_confirmations (
  confirmation_id text PRIMARY KEY CHECK (
    confirmation_id ~ '^snapshotdemocapacity_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL UNIQUE,
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_artifact_sha256 text NOT NULL CHECK (
    plan_artifact_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_artifact_cid text NOT NULL CHECK (
    plan_artifact_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  plan_artifact_remote_object_key text NOT NULL,
  plan_artifact_bytes bigint NOT NULL CHECK (
    plan_artifact_bytes BETWEEN 1 AND 16777216
  ),
  confirmed_plan_name text NOT NULL CHECK (
    confirmed_plan_name IN ('Filebase Pro', 'Filebase Pro or better')
  ),
  confirmer_reference text NOT NULL CHECK (
    confirmer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  confirmed_at timestamptz NOT NULL,
  confirmation_sha256 text NOT NULL UNIQUE CHECK (
    confirmation_sha256 ~ '^[a-f0-9]{64}$'
  ),
  FOREIGN KEY (
    plan_id, plan_sha256, plan_artifact_sha256, plan_artifact_cid,
    plan_artifact_remote_object_key, plan_artifact_bytes
  ) REFERENCES oracle_candidate_source_snapshot_demo_plans (
    plan_id, plan_sha256, plan_artifact_sha256, plan_artifact_cid,
    plan_artifact_remote_object_key, plan_artifact_bytes
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_approvals (
  approval_id text PRIMARY KEY CHECK (
    approval_id ~ '^snapshotdemoapproval_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL UNIQUE REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_artifact_sha256 text NOT NULL CHECK (
    plan_artifact_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_artifact_cid text NOT NULL CHECK (
    plan_artifact_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  plan_artifact_remote_object_key text NOT NULL CHECK (
    plan_artifact_remote_object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)' AND
    length(plan_artifact_remote_object_key) BETWEEN 1 AND 2048
  ),
  plan_artifact_bytes bigint NOT NULL CHECK (
    plan_artifact_bytes BETWEEN 1 AND 16777216
  ),
  approved_plan_revision integer NOT NULL CHECK (approved_plan_revision > 0),
  approver_reference text NOT NULL CHECK (
    approver_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  approved_at timestamptz NOT NULL,
  FOREIGN KEY (
    plan_id, plan_sha256, plan_artifact_sha256, plan_artifact_cid,
    plan_artifact_remote_object_key, plan_artifact_bytes
  ) REFERENCES oracle_candidate_source_snapshot_demo_plans (
    plan_id, plan_sha256, plan_artifact_sha256, plan_artifact_cid,
    plan_artifact_remote_object_key, plan_artifact_bytes
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_objects (
  plan_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  logical_object_key text NOT NULL CHECK (
    logical_object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)' AND
    length(logical_object_key) BETWEEN 1 AND 2048
  ),
  remote_object_key text NOT NULL CHECK (
    remote_object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)' AND
    length(remote_object_key) BETWEEN 1 AND 2048
  ),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_bytes bigint NOT NULL CHECK (
    expected_bytes BETWEEN 0 AND 536870912
  ),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'admitted', 'outcome_unknown', 'verified', 'failed_terminal'
  )),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 3),
  request_count integer NOT NULL DEFAULT 0 CHECK (request_count BETWEEN 0 AND 3),
  provider_cid text CHECK (
    provider_cid IS NULL OR provider_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  successful_effect_count integer NOT NULL DEFAULT 0 CHECK (
    successful_effect_count BETWEEN 0 AND 1
  ),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (plan_id, domain, remote_object_key),
  UNIQUE (plan_id, domain, logical_object_key),
  CHECK (
    (status = 'verified' AND provider_cid = expected_cid AND
      receipt_sha256 IS NOT NULL AND successful_effect_count = 1) OR
    (status <> 'verified' AND successful_effect_count = 0)
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_accounting (
  plan_id text PRIMARY KEY REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  request_count integer NOT NULL DEFAULT 0 CHECK (
    request_count BETWEEN 0 AND 1000000
  ),
  class_a_mutation_count integer NOT NULL DEFAULT 0 CHECK (
    class_a_mutation_count >= 0
  ),
  class_b_read_count integer NOT NULL DEFAULT 0 CHECK (
    class_b_read_count >= 0
  ),
  names_api_count integer NOT NULL DEFAULT 0 CHECK (names_api_count >= 0),
  public_resolver_count integer NOT NULL DEFAULT 0 CHECK (
    public_resolver_count >= 0
  ),
  free_operation_count integer NOT NULL DEFAULT 0 CHECK (
    free_operation_count >= 0
  ),
  request_cost_usd numeric(18, 12) NOT NULL DEFAULT 0 CHECK (
    request_cost_usd >= 0
  ),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    request_count = class_a_mutation_count + class_b_read_count +
      names_api_count + public_resolver_count + free_operation_count
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_requests (
  request_id text PRIMARY KEY CHECK (
    request_id ~ '^snapshotdemorequest_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  operation_class text NOT NULL CHECK (operation_class IN (
    'class_a_mutation', 'class_b_read', 'names_api', 'public_resolver',
    'free_operation'
  )),
  operation_kind text NOT NULL CHECK (operation_kind IN (
    'put_object', 'inspect_object', 'names_read', 'names_update',
    'public_resolve'
  )),
  intent_id text,
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text,
  cycle_sequence integer CHECK (cycle_sequence BETWEEN 1 AND 32),
  resolver text CHECK (
    resolver IS NULL OR resolver IN (
      'filebase_control', 'filebase_gateway', 'delegated_ipfs',
      'ipfs_io', 'dweb_link'
    )
  ),
  request_cost_usd numeric(18, 12) NOT NULL CHECK (
    request_cost_usd IN (0, 0.0000045)
  ),
  outcome text NOT NULL CHECK (outcome IN (
    'request_started', 'succeeded', 'absent', 'ambiguous',
    'retryable_failure', 'timeout_unknown', 'terminal_failure'
  )),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK (
    (outcome = 'request_started' AND completed_at IS NULL) OR
    (outcome <> 'request_started' AND completed_at IS NOT NULL)
  ),
  CHECK (
    (operation_kind = 'put_object' AND
      operation_class = 'class_a_mutation' AND intent_id IS NULL AND
      remote_object_key IS NOT NULL AND cycle_sequence IS NULL AND
      resolver IS NULL) OR
    (operation_kind = 'inspect_object' AND
      operation_class = 'class_b_read' AND intent_id IS NULL AND
      remote_object_key IS NOT NULL AND cycle_sequence IS NULL AND
      resolver IS NULL) OR
    (operation_kind = 'names_read' AND operation_class = 'names_api' AND
      intent_id IS NOT NULL AND remote_object_key IS NULL AND
      cycle_sequence IS NOT NULL AND resolver = 'filebase_control') OR
    (operation_kind = 'names_update' AND operation_class = 'names_api' AND
      intent_id IS NOT NULL AND remote_object_key IS NULL AND
      cycle_sequence IS NULL AND resolver IS NULL) OR
    (operation_kind = 'public_resolve' AND
      operation_class = 'public_resolver' AND intent_id IS NOT NULL AND
      remote_object_key IS NULL AND cycle_sequence IS NOT NULL AND
      resolver IN ('filebase_gateway', 'delegated_ipfs', 'ipfs_io', 'dweb_link'))
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_upload_attempts (
  attempt_id text PRIMARY KEY CHECK (
    attempt_id ~ '^snapshotdemoattempt_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  plan_id text NOT NULL,
  domain text NOT NULL,
  remote_object_key text NOT NULL,
  attempt_sequence integer NOT NULL CHECK (attempt_sequence BETWEEN 1 AND 3),
  outcome text NOT NULL CHECK (outcome IN (
    'request_started', 'connection_failure', 'retryable_http_error',
    'timeout_unknown', 'provider_cid_mismatch', 'terminal_failure', 'verified'
  )),
  request_count integer NOT NULL CHECK (request_count = 1),
  provider_cid text CHECK (
    provider_cid IS NULL OR provider_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  provider_request_id_hash text CHECK (
    provider_request_id_hash IS NULL OR
    provider_request_id_hash ~ '^[a-f0-9]{64}$'
  ),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  response_bytes integer CHECK (response_bytes IS NULL OR response_bytes >= 0),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (plan_id, domain, remote_object_key, attempt_sequence),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(plan_id, domain, remote_object_key),
  CHECK (
    (outcome = 'request_started' AND completed_at IS NULL AND
      provider_cid IS NULL AND receipt_sha256 IS NULL) OR
    (outcome = 'verified' AND completed_at IS NOT NULL AND
      provider_cid IS NOT NULL AND receipt_sha256 IS NOT NULL) OR
    (outcome NOT IN ('request_started', 'verified') AND completed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_inspections (
  inspection_id text PRIMARY KEY CHECK (
    inspection_id ~ '^snapshotdemoinspection_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  plan_id text NOT NULL,
  domain text NOT NULL,
  remote_object_key text NOT NULL,
  outcome text NOT NULL CHECK (outcome IN (
    'absent', 'verified', 'ambiguous', 'mismatch'
  )),
  observed_cid text CHECK (
    observed_cid IS NULL OR observed_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  observed_sha256 text CHECK (
    observed_sha256 IS NULL OR observed_sha256 ~ '^[a-f0-9]{64}$'
  ),
  observed_bytes bigint CHECK (
    observed_bytes IS NULL OR observed_bytes BETWEEN 0 AND 536870912
  ),
  receipt_sha256 text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(plan_id, domain, remote_object_key)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_ipns_intents (
  intent_id text PRIMARY KEY CHECK (
    intent_id ~ '^snapshotdemointent_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  bucket text NOT NULL,
  ipns_label text NOT NULL,
  ipns_network_key text NOT NULL CHECK (ipns_network_key ~ '^k51[0-9a-z]{59}$'),
  prior_cid text NOT NULL CHECK (
    prior_cid ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
  ),
  target_cid text NOT NULL CHECK (
    target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  intended_at timestamptz NOT NULL,
  UNIQUE (plan_id, domain)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_ipns_intent_state (
  intent_id text PRIMARY KEY REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id),
  plan_id text NOT NULL,
  domain text NOT NULL,
  state text NOT NULL CHECK (state IN (
    'intent_recorded', 'prior_confirmed', 'update_in_flight',
    'target_observed', 'verified', 'update_ambiguous', 'unexpected_cid',
    'update_failed_prior_confirmed',
    'rollback_recorded', 'rollback_in_flight', 'rollback_ambiguous',
    'rolled_back', 'manual_intervention_required', 'failed_terminal'
  )),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (plan_id, domain),
  FOREIGN KEY (plan_id, domain)
    REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(plan_id, domain)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_replay_authorizations (
  authorization_id text PRIMARY KEY CHECK (
    authorization_id ~ '^snapshotdemoreplay_[a-f0-9]{32}$'
  ),
  intent_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id),
  plan_id text NOT NULL,
  domain text NOT NULL,
  direction text NOT NULL CHECK (direction IN ('update', 'rollback')),
  requested_cid text NOT NULL CHECK (
    requested_cid ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
  ),
  authorization_sha256 text NOT NULL CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorizer_reference text NOT NULL CHECK (
    authorizer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  UNIQUE (intent_id, direction, authorization_sha256),
  FOREIGN KEY (plan_id, domain)
    REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(plan_id, domain)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_ipns_attempts (
  attempt_id text PRIMARY KEY CHECK (
    attempt_id ~ '^snapshotdemoipnsattempt_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  intent_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id),
  plan_id text NOT NULL,
  domain text NOT NULL,
  direction text NOT NULL CHECK (direction IN ('update', 'rollback')),
  attempt_sequence integer NOT NULL CHECK (attempt_sequence BETWEEN 1 AND 3),
  requested_cid text NOT NULL CHECK (
    requested_cid ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
  ),
  replay_authorization_sha256 text CHECK (
    replay_authorization_sha256 IS NULL OR
    replay_authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  outcome text NOT NULL CHECK (outcome IN (
    'request_started', 'acknowledged', 'timeout_unknown',
    'retryable_failure', 'terminal_failure'
  )),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (intent_id, direction, attempt_sequence),
  FOREIGN KEY (plan_id, domain)
    REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(plan_id, domain),
  FOREIGN KEY (intent_id, direction, replay_authorization_sha256)
    REFERENCES oracle_candidate_source_snapshot_demo_replay_authorizations(
      intent_id, direction, authorization_sha256
    ),
  CHECK (
    (outcome = 'request_started' AND completed_at IS NULL) OR
    (outcome <> 'request_started' AND completed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_ipns_observations (
  observation_id text PRIMARY KEY CHECK (
    observation_id ~ '^snapshotdemoipnsobservation_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  intent_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id),
  cycle_sequence integer NOT NULL CHECK (cycle_sequence BETWEEN 1 AND 32),
  resolver text NOT NULL CHECK (
    resolver IN ('filebase_control', 'filebase_gateway', 'delegated_ipfs', 'ipfs_io', 'dweb_link')
  ),
  classification text NOT NULL CHECK (
    classification IN ('prior', 'target', 'split', 'unavailable', 'unexpected_cid')
  ),
  observed_cid text CHECK (
    observed_cid IS NULL OR
    observed_cid ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
  ),
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  observed_at timestamptz NOT NULL,
  UNIQUE (intent_id, cycle_sequence, resolver)
);

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD CONSTRAINT oracle_candidate_source_snapshot_request_intent_fk
  FOREIGN KEY (intent_id)
  REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_ipns_events (
  event_id text PRIMARY KEY CHECK (
    event_id ~ '^snapshotdemoipnsevent_[a-f0-9]{32}$'
  ),
  intent_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_ipns_intents(intent_id),
  from_state text,
  to_state text NOT NULL,
  event_sha256 text NOT NULL CHECK (event_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, event_sha256)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_events (
  event_id text PRIMARY KEY CHECK (
    event_id ~ '^snapshotdemoevent_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  event_type text NOT NULL,
  event_sha256 text NOT NULL CHECK (event_sha256 ~ '^[a-f0-9]{64}$'),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (plan_id, event_sha256)
);

CREATE INDEX IF NOT EXISTS oracle_candidate_source_snapshot_objects_status_idx
  ON oracle_candidate_source_snapshot_demo_objects(plan_id, status, domain, remote_object_key);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state <> 'awaiting_configuration' OR NEW.revision <> 1 OR
     NEW.prepared_with_executor_disabled IS DISTINCT FROM true OR
     NEW.subscription_tier_status <> 'human_confirmation_required' THEN
    RAISE EXCEPTION 'candidate source-snapshot plan must begin fail-closed awaiting configuration';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_plan_insert_guard
  ON oracle_candidate_source_snapshot_demo_plans;
CREATE TRIGGER oracle_candidate_source_snapshot_plan_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_plan_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_plan_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     OLD.plan_version IS DISTINCT FROM NEW.plan_version OR
     OLD.publication_class IS DISTINCT FROM NEW.publication_class OR
     OLD.resource_owner IS DISTINCT FROM NEW.resource_owner OR
     OLD.canonical IS DISTINCT FROM NEW.canonical OR
     OLD.elephant_owned IS DISTINCT FROM NEW.elephant_owned OR
     OLD.owner_controlled IS DISTINCT FROM NEW.owner_controlled OR
     OLD.independently_pasco_certified IS DISTINCT FROM NEW.independently_pasco_certified OR
     OLD.namespace_id IS DISTINCT FROM NEW.namespace_id OR
     OLD.source_scope IS DISTINCT FROM NEW.source_scope OR
     OLD.source_plan_id IS DISTINCT FROM NEW.source_plan_id OR
     OLD.source_plan_sha256 IS DISTINCT FROM NEW.source_plan_sha256 OR
     OLD.authority_id IS DISTINCT FROM NEW.authority_id OR
     OLD.snapshot_id IS DISTINCT FROM NEW.snapshot_id OR
     OLD.materialization_id IS DISTINCT FROM NEW.materialization_id OR
     OLD.materialization_sha256 IS DISTINCT FROM NEW.materialization_sha256 OR
     OLD.maximum_upload_object_count IS DISTINCT FROM NEW.maximum_upload_object_count OR
     OLD.maximum_upload_bytes IS DISTINCT FROM NEW.maximum_upload_bytes OR
     OLD.exact_upload_object_count IS DISTINCT FROM NEW.exact_upload_object_count OR
     OLD.exact_upload_bytes IS DISTINCT FROM NEW.exact_upload_bytes OR
     OLD.maximum_object_bytes IS DISTINCT FROM NEW.maximum_object_bytes OR
     OLD.maximum_concurrency IS DISTINCT FROM NEW.maximum_concurrency OR
     OLD.maximum_retries IS DISTINCT FROM NEW.maximum_retries OR
     OLD.request_timeout_ms IS DISTINCT FROM NEW.request_timeout_ms OR
     OLD.request_limit IS DISTINCT FROM NEW.request_limit OR
     OLD.maximum_request_count IS DISTINCT FROM NEW.maximum_request_count OR
     OLD.budget_limit_usd IS DISTINCT FROM NEW.budget_limit_usd OR
     OLD.maximum_total_usd IS DISTINCT FROM NEW.maximum_total_usd OR
     OLD.fixed_account_plan_monthly_usd IS DISTINCT FROM NEW.fixed_account_plan_monthly_usd OR
     OLD.inventory_root_sha256 IS DISTINCT FROM NEW.inventory_root_sha256 OR
     OLD.inventory_root_cid IS DISTINCT FROM NEW.inventory_root_cid OR
     OLD.inventory_shard_count IS DISTINCT FROM NEW.inventory_shard_count OR
     OLD.control_artifacts IS DISTINCT FROM NEW.control_artifacts OR
     OLD.request_envelope IS DISTINCT FROM NEW.request_envelope OR
     OLD.cost_envelope IS DISTINCT FROM NEW.cost_envelope OR
     OLD.capacity_preflight IS DISTINCT FROM NEW.capacity_preflight OR
     OLD.capacity_preflight_sha256 IS DISTINCT FROM NEW.capacity_preflight_sha256 OR
     OLD.capacity_preflight_observed_at IS DISTINCT FROM NEW.capacity_preflight_observed_at OR
     OLD.subscription_tier_status IS DISTINCT FROM NEW.subscription_tier_status OR
     OLD.plan_payload IS DISTINCT FROM NEW.plan_payload OR
     OLD.plan_artifact_bytes IS DISTINCT FROM NEW.plan_artifact_bytes OR
     OLD.plan_artifact_sha256 IS DISTINCT FROM NEW.plan_artifact_sha256 OR
     OLD.plan_artifact_cid IS DISTINCT FROM NEW.plan_artifact_cid OR
     OLD.plan_artifact_logical_object_key IS DISTINCT FROM NEW.plan_artifact_logical_object_key OR
     OLD.plan_artifact_remote_object_key IS DISTINCT FROM NEW.plan_artifact_remote_object_key OR
     OLD.prepared_with_executor_disabled IS DISTINCT FROM NEW.prepared_with_executor_disabled OR
     OLD.created_at IS DISTINCT FROM NEW.created_at THEN
    RAISE EXCEPTION 'candidate source-snapshot plan identity is immutable';
  END IF;
  IF OLD.revision + 1 IS DISTINCT FROM NEW.revision OR
     OLD.state IS NOT DISTINCT FROM NEW.state THEN
    RAISE EXCEPTION 'candidate source-snapshot plan transition revision is invalid';
  END IF;
  IF NOT (
    (OLD.state = 'awaiting_configuration' AND NEW.state = 'awaiting_approval' AND
      EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_capacity_confirmations confirmation
        WHERE confirmation.plan_id = OLD.plan_id
          AND confirmation.plan_sha256 = OLD.plan_sha256
          AND confirmation.plan_artifact_sha256 = OLD.plan_artifact_sha256
          AND confirmation.plan_artifact_cid = OLD.plan_artifact_cid
          AND confirmation.plan_artifact_remote_object_key = OLD.plan_artifact_remote_object_key
          AND confirmation.plan_artifact_bytes = OLD.plan_artifact_bytes
      )) OR
    (OLD.state = 'awaiting_approval' AND NEW.state = 'approved' AND
      EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_approvals approval
        WHERE approval.plan_id = OLD.plan_id
          AND approval.plan_sha256 = OLD.plan_sha256
          AND approval.plan_artifact_sha256 = OLD.plan_artifact_sha256
          AND approval.plan_artifact_cid = OLD.plan_artifact_cid
          AND approval.plan_artifact_remote_object_key = OLD.plan_artifact_remote_object_key
          AND approval.plan_artifact_bytes = OLD.plan_artifact_bytes
          AND approval.approved_plan_revision = OLD.revision
      )) OR
    (OLD.state = 'approved' AND NEW.state = 'executing') OR
    (OLD.state = 'executing' AND NEW.state = 'completed' AND
      NOT EXISTS (
        SELECT 1 FROM oracle_candidate_source_snapshot_demo_objects object
        WHERE object.plan_id = OLD.plan_id AND object.status <> 'verified'
      ) AND
      (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.plan_id = OLD.plan_id AND intent_state.state = 'verified') = 2) OR
    (OLD.state IN ('awaiting_configuration', 'awaiting_approval', 'approved', 'executing') AND
      NEW.state = 'failed_terminal') OR
    (OLD.state IN ('approved', 'executing') AND
      NEW.state = 'manual_intervention_required') OR
    (OLD.state = 'manual_intervention_required' AND
      NEW.state = 'failed_terminal')
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot plan state transition';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_plan_identity_immutable
  ON oracle_candidate_source_snapshot_demo_plans;
CREATE TRIGGER oracle_candidate_source_snapshot_plan_identity_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_plan_identity();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_object()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.logical_object_key IS DISTINCT FROM NEW.logical_object_key OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.expected_sha256 IS DISTINCT FROM NEW.expected_sha256 OR
     OLD.expected_cid IS DISTINCT FROM NEW.expected_cid OR
     OLD.expected_bytes IS DISTINCT FROM NEW.expected_bytes THEN
    RAISE EXCEPTION 'candidate source-snapshot object identity is immutable';
  END IF;
  IF OLD.status = 'verified' AND OLD IS DISTINCT FROM NEW THEN
    RAISE EXCEPTION 'verified candidate source-snapshot effect is immutable';
  END IF;
  IF OLD.revision + 1 IS DISTINCT FROM NEW.revision THEN
    RAISE EXCEPTION 'candidate source-snapshot object revision is invalid';
  END IF;
  IF NOT (
    OLD.status = NEW.status OR
    (OLD.status = 'pending' AND NEW.status = 'admitted') OR
    (OLD.status = 'admitted' AND NEW.status IN (
      'outcome_unknown', 'verified', 'failed_terminal'
    )) OR
    (OLD.status = 'outcome_unknown' AND NEW.status IN (
      'admitted', 'verified', 'failed_terminal'
    ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot object transition';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_object_guard
  ON oracle_candidate_source_snapshot_demo_objects;
CREATE TRIGGER oracle_candidate_source_snapshot_object_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_objects
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_object();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_accounting()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_request_cost numeric(18, 12);
BEGIN
  expected_request_cost := CASE
    WHEN NEW.free_operation_count = OLD.free_operation_count + 1
      THEN 0::numeric
    ELSE 0.0000045::numeric
  END;
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.revision + 1 IS DISTINCT FROM NEW.revision OR
     NEW.request_count IS DISTINCT FROM OLD.request_count + 1 OR
     NEW.request_cost_usd IS DISTINCT FROM OLD.request_cost_usd + expected_request_cost OR
     ((NEW.class_a_mutation_count - OLD.class_a_mutation_count) +
      (NEW.class_b_read_count - OLD.class_b_read_count) +
      (NEW.names_api_count - OLD.names_api_count) +
      (NEW.public_resolver_count - OLD.public_resolver_count) +
      (NEW.free_operation_count - OLD.free_operation_count)) IS DISTINCT FROM 1 OR
     NEW.class_a_mutation_count < OLD.class_a_mutation_count OR
     NEW.class_b_read_count < OLD.class_b_read_count OR
     NEW.names_api_count < OLD.names_api_count OR
     NEW.public_resolver_count < OLD.public_resolver_count OR
     NEW.free_operation_count < OLD.free_operation_count THEN
    RAISE EXCEPTION 'candidate source-snapshot accounting update is invalid';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_accounting_guard
  ON oracle_candidate_source_snapshot_demo_accounting;
CREATE TRIGGER oracle_candidate_source_snapshot_accounting_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_accounting
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_accounting();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
  existing_request_count integer;
  existing_class_a integer;
  existing_class_b integer;
  existing_names integer;
  existing_public integer;
  existing_free integer;
  existing_cost numeric(18, 12);
  allowed_class_a integer;
  allowed_class_b integer;
  allowed_names integer;
  allowed_public integer;
  allowed_free integer;
  allowed_request_cost numeric(18, 12);
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_row.state <> 'executing' THEN
    RAISE EXCEPTION 'candidate source-snapshot remote request requires the executing plan';
  END IF;

  IF NEW.intent_id IS NOT NULL THEN
    SELECT * INTO STRICT intent_row
    FROM oracle_candidate_source_snapshot_demo_ipns_intents
    WHERE intent_id = NEW.intent_id;
    IF intent_row.plan_id IS DISTINCT FROM NEW.plan_id OR
       intent_row.domain IS DISTINCT FROM NEW.domain THEN
      RAISE EXCEPTION 'candidate source-snapshot remote request intent binding mismatch';
    END IF;
  END IF;

  IF NEW.request_cost_usd IS DISTINCT FROM (CASE
       WHEN NEW.operation_class = 'free_operation' THEN 0::numeric
       ELSE 0.0000045::numeric
     END) THEN
    RAISE EXCEPTION 'candidate source-snapshot remote request cost is invalid';
  END IF;

  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT
    count(*)::integer,
    count(*) FILTER (WHERE operation_class = 'class_a_mutation')::integer,
    count(*) FILTER (WHERE operation_class = 'class_b_read')::integer,
    count(*) FILTER (WHERE operation_class = 'names_api')::integer,
    count(*) FILTER (WHERE operation_class = 'public_resolver')::integer,
    count(*) FILTER (WHERE operation_class = 'free_operation')::integer,
    COALESCE(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO existing_request_count, existing_class_a, existing_class_b,
       existing_names, existing_public, existing_free, existing_cost
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE plan_id = NEW.plan_id;

  IF accounting_row.request_count IS DISTINCT FROM existing_request_count + 1 OR
     accounting_row.class_a_mutation_count IS DISTINCT FROM (
       existing_class_a +
       CASE WHEN NEW.operation_class = 'class_a_mutation' THEN 1 ELSE 0 END
     ) OR
     accounting_row.class_b_read_count IS DISTINCT FROM (
       existing_class_b +
       CASE WHEN NEW.operation_class = 'class_b_read' THEN 1 ELSE 0 END
     ) OR
     accounting_row.names_api_count IS DISTINCT FROM (
       existing_names +
       CASE WHEN NEW.operation_class = 'names_api' THEN 1 ELSE 0 END
     ) OR
     accounting_row.public_resolver_count IS DISTINCT FROM (
       existing_public +
       CASE WHEN NEW.operation_class = 'public_resolver' THEN 1 ELSE 0 END
     ) OR
     accounting_row.free_operation_count IS DISTINCT FROM (
       existing_free +
       CASE WHEN NEW.operation_class = 'free_operation' THEN 1 ELSE 0 END
     ) OR
     accounting_row.request_cost_usd IS DISTINCT FROM existing_cost + NEW.request_cost_usd THEN
    RAISE EXCEPTION 'candidate source-snapshot remote request lacks exact global accounting admission';
  END IF;

  allowed_class_a := (plan_row.request_envelope->'maximumAttempts'->>'classAMutations')::integer;
  allowed_class_b := (plan_row.request_envelope->'ambiguousObjectInspectionAllowance'->>'classBReads')::integer;
  allowed_names :=
    (plan_row.request_envelope->'maximumAttempts'->>'namesApiOperations')::integer +
    (plan_row.request_envelope->'recoveryAllowance'->>'namesApiOperations')::integer;
  allowed_public :=
    (plan_row.request_envelope->'maximumAttempts'->>'publicResolverOperations')::integer +
    (plan_row.request_envelope->'recoveryAllowance'->>'publicResolverOperations')::integer;
  allowed_free :=
    (plan_row.request_envelope->'maximumAttempts'->>'freeOperations')::integer +
    (plan_row.request_envelope->'recoveryAllowance'->>'freeOperations')::integer;
  allowed_request_cost :=
    (plan_row.cost_envelope->'requestUsd'->>'maximumAttempts')::numeric +
    (plan_row.cost_envelope->'requestUsd'->>'ambiguousObjectInspections')::numeric +
    (plan_row.cost_envelope->>'recoveryRequestUsd')::numeric;
  IF accounting_row.class_a_mutation_count > allowed_class_a OR
     accounting_row.class_b_read_count > allowed_class_b OR
     accounting_row.names_api_count > allowed_names OR
     accounting_row.public_resolver_count > allowed_public OR
     accounting_row.free_operation_count > allowed_free OR
     accounting_row.request_count > plan_row.maximum_request_count OR
     accounting_row.request_cost_usd > allowed_request_cost THEN
    RAISE EXCEPTION 'candidate source-snapshot remote request exceeds its plan allowance';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_request_insert_guard
  ON oracle_candidate_source_snapshot_demo_requests;
CREATE TRIGGER oracle_candidate_source_snapshot_request_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_requests
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_request_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.attempt_id IS DISTINCT FROM NEW.attempt_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.request_count IS DISTINCT FROM NEW.request_count OR
     OLD.started_at IS DISTINCT FROM NEW.started_at THEN
    RAISE EXCEPTION 'candidate source-snapshot upload attempt identity is immutable';
  END IF;
  IF OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION 'candidate source-snapshot upload attempt is already terminal';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_attempt_guard
  ON oracle_candidate_source_snapshot_demo_upload_attempts;
CREATE TRIGGER oracle_candidate_source_snapshot_attempt_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_upload_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_attempt();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.operation_class IS DISTINCT FROM NEW.operation_class OR
     OLD.operation_kind IS DISTINCT FROM NEW.operation_kind OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.cycle_sequence IS DISTINCT FROM NEW.cycle_sequence OR
     OLD.resolver IS DISTINCT FROM NEW.resolver OR
     OLD.request_cost_usd IS DISTINCT FROM NEW.request_cost_usd OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION 'candidate source-snapshot request is immutable or terminal';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_request_guard
  ON oracle_candidate_source_snapshot_demo_requests;
CREATE TRIGGER oracle_candidate_source_snapshot_request_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_requests
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_request();

CREATE OR REPLACE FUNCTION oracle_reject_candidate_source_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_capacity_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_row.state <> 'awaiting_configuration' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.plan_artifact_sha256 IS DISTINCT FROM plan_row.plan_artifact_sha256 OR
     NEW.plan_artifact_cid IS DISTINCT FROM plan_row.plan_artifact_cid OR
     NEW.plan_artifact_remote_object_key IS DISTINCT FROM plan_row.plan_artifact_remote_object_key OR
     NEW.plan_artifact_bytes IS DISTINCT FROM plan_row.plan_artifact_bytes THEN
    RAISE EXCEPTION 'candidate source-snapshot capacity confirmation binding mismatch';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_capacity_insert_guard
  ON oracle_candidate_source_snapshot_demo_capacity_confirmations;
CREATE TRIGGER oracle_candidate_source_snapshot_capacity_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_capacity_confirmations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_capacity_insert();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_capacity_immutable
  ON oracle_candidate_source_snapshot_demo_capacity_confirmations;
CREATE TRIGGER oracle_candidate_source_snapshot_capacity_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_capacity_confirmations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_approval_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_row.state <> 'awaiting_approval' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.plan_artifact_sha256 IS DISTINCT FROM plan_row.plan_artifact_sha256 OR
     NEW.plan_artifact_cid IS DISTINCT FROM plan_row.plan_artifact_cid OR
     NEW.plan_artifact_remote_object_key IS DISTINCT FROM plan_row.plan_artifact_remote_object_key OR
     NEW.plan_artifact_bytes IS DISTINCT FROM plan_row.plan_artifact_bytes OR
     NEW.approved_plan_revision IS DISTINCT FROM plan_row.revision OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_capacity_confirmations confirmation
       WHERE confirmation.plan_id = plan_row.plan_id
         AND confirmation.plan_sha256 = plan_row.plan_sha256
         AND confirmation.plan_artifact_sha256 = plan_row.plan_artifact_sha256
         AND confirmation.plan_artifact_cid = plan_row.plan_artifact_cid
         AND confirmation.plan_artifact_remote_object_key = plan_row.plan_artifact_remote_object_key
         AND confirmation.plan_artifact_bytes = plan_row.plan_artifact_bytes
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot approval is not bound to a confirmed immutable plan';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_approval_insert_guard
  ON oracle_candidate_source_snapshot_demo_approvals;
CREATE TRIGGER oracle_candidate_source_snapshot_approval_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_approvals
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_approval_insert();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_inspection_immutable
  ON oracle_candidate_source_snapshot_demo_inspections;
CREATE TRIGGER oracle_candidate_source_snapshot_inspection_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_inspections
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_approval_immutable
  ON oracle_candidate_source_snapshot_demo_approvals;
CREATE TRIGGER oracle_candidate_source_snapshot_approval_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_approvals
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_intent_immutable
  ON oracle_candidate_source_snapshot_demo_ipns_intents;
CREATE TRIGGER oracle_candidate_source_snapshot_intent_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_replay_authorization_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
BEGIN
  SELECT * INTO STRICT intent_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF NEW.plan_id IS DISTINCT FROM intent_row.plan_id OR
     NEW.domain IS DISTINCT FROM intent_row.domain OR
     NEW.requested_cid IS DISTINCT FROM (CASE NEW.direction
       WHEN 'update' THEN intent_row.target_cid
       WHEN 'rollback' THEN intent_row.prior_cid
       ELSE NULL
     END) THEN
    RAISE EXCEPTION 'candidate source-snapshot replay authorization binding mismatch';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_replay_authorization_insert_guard
  ON oracle_candidate_source_snapshot_demo_replay_authorizations;
CREATE TRIGGER oracle_candidate_source_snapshot_replay_authorization_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_replay_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_replay_authorization_insert();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_replay_authorization_immutable
  ON oracle_candidate_source_snapshot_demo_replay_authorizations;
CREATE TRIGGER oracle_candidate_source_snapshot_replay_authorization_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_replay_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_intent_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  target jsonb;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  target := CASE NEW.domain
    WHEN 'open_data' THEN plan_row.plan_payload->'targets'->'openData'
    WHEN 'query_table' THEN plan_row.plan_payload->'targets'->'queryTable'
    ELSE NULL
  END;
  IF plan_row.state <> 'executing' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     target IS NULL OR
     NEW.bucket IS DISTINCT FROM target->>'bucket' OR
     NEW.ipns_label IS DISTINCT FROM target->>'ipnsLabel' OR
     NEW.ipns_network_key IS DISTINCT FROM target->>'ipnsNetworkKey' OR
     NEW.prior_cid IS DISTINCT FROM target->>'priorCid' OR
     NEW.target_cid IS DISTINCT FROM target->>'targetCid' OR
     NOT EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.plan_id = plan_row.plan_id
         AND approval.plan_sha256 = plan_row.plan_sha256
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot intent is not derived from the approved executing plan';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_intent_insert_guard
  ON oracle_candidate_source_snapshot_demo_ipns_intents;
CREATE TRIGGER oracle_candidate_source_snapshot_intent_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_intent_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_intent_state_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
BEGIN
  SELECT * INTO STRICT intent_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF NEW.plan_id IS DISTINCT FROM intent_row.plan_id OR
     NEW.domain IS DISTINCT FROM intent_row.domain OR
     NEW.state IS DISTINCT FROM 'intent_recorded' OR
     NEW.revision IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'candidate source-snapshot intent state must begin from its exact immutable intent';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_intent_state_insert_guard
  ON oracle_candidate_source_snapshot_demo_ipns_intent_state;
CREATE TRIGGER oracle_candidate_source_snapshot_intent_state_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_intent_state
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_intent_state_insert();

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_has_complete_resolution_cycle(
  checked_intent_id text,
  checked_classification text,
  require_after_latest_attempt boolean
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  WITH intent AS (
    SELECT prior_cid, target_cid
    FROM oracle_candidate_source_snapshot_demo_ipns_intents
    WHERE intent_id = checked_intent_id
  ), latest_attempt AS (
    SELECT max(completed_at) AS completed_at
    FROM oracle_candidate_source_snapshot_demo_ipns_attempts
    WHERE intent_id = checked_intent_id
      AND completed_at IS NOT NULL
  )
  SELECT EXISTS (
    SELECT observation.cycle_sequence
    FROM oracle_candidate_source_snapshot_demo_ipns_observations observation
    CROSS JOIN intent
    CROSS JOIN latest_attempt
    WHERE observation.intent_id = checked_intent_id
      AND observation.resolver IN (
        'filebase_control', 'filebase_gateway', 'delegated_ipfs'
      )
      AND (
        require_after_latest_attempt IS NOT TRUE OR
        (latest_attempt.completed_at IS NOT NULL AND
          observation.observed_at >= latest_attempt.completed_at)
      )
    GROUP BY observation.cycle_sequence, intent.prior_cid, intent.target_cid
    HAVING count(*) = 3
       AND count(*) FILTER (
         WHERE observation.classification = checked_classification
           AND observation.observed_cid = CASE checked_classification
             WHEN 'prior' THEN intent.prior_cid
             WHEN 'target' THEN intent.target_cid
             ELSE NULL
           END
       ) = 3
  )
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_intent_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.revision + 1 <> NEW.revision THEN
    RAISE EXCEPTION 'candidate source-snapshot intent state identity/revision is invalid';
  END IF;
  IF NOT (
    (OLD.state = 'intent_recorded' AND NEW.state = 'prior_confirmed') OR
    (OLD.state = 'prior_confirmed' AND NEW.state = 'update_in_flight') OR
    (OLD.state = 'update_in_flight' AND NEW.state IN (
      'target_observed', 'update_ambiguous', 'unexpected_cid',
      'update_failed_prior_confirmed', 'failed_terminal'
    )) OR
    (OLD.state = 'update_ambiguous' AND NEW.state IN (
      'prior_confirmed', 'target_observed', 'unexpected_cid',
      'update_failed_prior_confirmed', 'manual_intervention_required',
      'failed_terminal'
    )) OR
    (OLD.state = 'target_observed' AND NEW.state IN ('verified', 'rollback_recorded')) OR
    (OLD.state = 'verified' AND NEW.state = 'rollback_recorded') OR
    (OLD.state = 'rollback_recorded' AND NEW.state = 'rollback_in_flight') OR
    (OLD.state = 'rollback_in_flight' AND NEW.state IN (
      'rolled_back', 'rollback_ambiguous', 'unexpected_cid', 'failed_terminal'
    )) OR
    (OLD.state = 'rollback_ambiguous' AND NEW.state IN (
      'rollback_recorded', 'rolled_back', 'unexpected_cid',
      'manual_intervention_required', 'failed_terminal'
    ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot IPNS transition';
  END IF;
  IF NEW.state = 'prior_confirmed' AND NOT
    oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'prior', OLD.state <> 'intent_recorded'
    ) THEN
    RAISE EXCEPTION 'candidate source-snapshot prior confirmation requires one complete resolution cycle';
  END IF;
  IF NEW.state IN ('target_observed', 'verified') AND (
    NOT oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'target', true
    ) OR NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'update'
        AND attempt.outcome <> 'request_started'
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot target state requires one complete resolution cycle';
  END IF;
  IF NEW.state = 'update_failed_prior_confirmed' AND (
    NOT oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'prior', true
    ) OR NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'update'
        AND attempt.outcome IN (
          'timeout_unknown', 'retryable_failure', 'terminal_failure'
        )
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot failed update requires terminal attempt and later complete prior evidence';
  END IF;
  IF NEW.state = 'rolled_back' AND (
    NOT oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'prior', true
    ) OR NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'rollback'
        AND attempt.outcome <> 'request_started'
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot rollback requires one complete later prior resolution cycle';
  END IF;
  IF NEW.state = 'rollback_recorded' AND NEW.domain = 'open_data' AND EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_ipns_intents query_intent
    JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state query_state
      ON query_state.intent_id = query_intent.intent_id
    WHERE query_intent.plan_id = NEW.plan_id
      AND query_intent.domain = 'query_table'
      AND query_state.state NOT IN (
        'intent_recorded', 'prior_confirmed',
        'update_failed_prior_confirmed', 'rolled_back'
      )
  ) THEN
    RAISE EXCEPTION 'open-data rollback requires conclusive query-table non-mutation or rollback';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_intent_state_guard
  ON oracle_candidate_source_snapshot_demo_ipns_intent_state;
CREATE TRIGGER oracle_candidate_source_snapshot_intent_state_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_ipns_intent_state
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_intent_state();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_ipns_observation_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  SELECT * INTO STRICT intent_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.request_id;
  IF (NEW.classification = 'target' AND
      NEW.observed_cid IS DISTINCT FROM intent_row.target_cid) OR
     (NEW.classification = 'prior' AND
      NEW.observed_cid IS DISTINCT FROM intent_row.prior_cid) OR
     (NEW.classification IN ('split', 'unavailable') AND
      NEW.observed_cid IS NOT NULL) OR
     (NEW.classification = 'unexpected_cid' AND
      (NEW.observed_cid IS NULL OR NEW.observed_cid IN (intent_row.prior_cid, intent_row.target_cid))) THEN
    RAISE EXCEPTION 'candidate source-snapshot IPNS observation is not bound to its immutable intent';
  END IF;
  IF request_row.intent_id IS DISTINCT FROM NEW.intent_id OR
     request_row.plan_id IS DISTINCT FROM intent_row.plan_id OR
     request_row.domain IS DISTINCT FROM intent_row.domain OR
     request_row.cycle_sequence IS DISTINCT FROM NEW.cycle_sequence OR
     request_row.resolver IS DISTINCT FROM NEW.resolver OR
     request_row.receipt_sha256 IS DISTINCT FROM NEW.evidence_sha256 OR
     (NEW.resolver = 'filebase_control' AND
       (request_row.operation_class IS DISTINCT FROM 'names_api' OR
        request_row.operation_kind IS DISTINCT FROM 'names_read')) OR
     (NEW.resolver <> 'filebase_control' AND
       (request_row.operation_class IS DISTINCT FROM 'public_resolver' OR
        request_row.operation_kind IS DISTINCT FROM 'public_resolve')) OR
     (NEW.classification IN ('prior', 'target', 'unexpected_cid') AND
       request_row.outcome IS DISTINCT FROM 'succeeded') OR
     (NEW.classification = 'split' AND
       request_row.outcome IS DISTINCT FROM 'ambiguous') OR
     (NEW.classification = 'unavailable' AND
       request_row.outcome NOT IN (
         'retryable_failure', 'timeout_unknown', 'terminal_failure'
       )) THEN
    RAISE EXCEPTION 'candidate source-snapshot IPNS observation lacks its exact admitted terminal request';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_ipns_observation_insert_guard
  ON oracle_candidate_source_snapshot_demo_ipns_observations;
CREATE TRIGGER oracle_candidate_source_snapshot_ipns_observation_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_observations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_ipns_observation_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_ipns_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  intent_state text;
BEGIN
  SELECT * INTO STRICT intent_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  SELECT state INTO STRICT intent_state
  FROM oracle_candidate_source_snapshot_demo_ipns_intent_state
  WHERE intent_id = NEW.intent_id;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.request_id;
  IF NEW.plan_id IS DISTINCT FROM intent_row.plan_id OR
     NEW.domain IS DISTINCT FROM intent_row.domain OR
     request_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     request_row.intent_id IS DISTINCT FROM NEW.intent_id OR
     request_row.domain IS DISTINCT FROM NEW.domain OR
     request_row.operation_class IS DISTINCT FROM 'names_api' OR
     request_row.operation_kind IS DISTINCT FROM 'names_update' OR
     request_row.outcome IS DISTINCT FROM 'request_started' THEN
    RAISE EXCEPTION 'candidate source-snapshot IPNS attempt binding mismatch';
  END IF;
  IF (SELECT count(*) FROM oracle_candidate_source_snapshot_demo_ipns_intents
      WHERE plan_id = NEW.plan_id) <> 2 THEN
    RAISE EXCEPTION 'both candidate source-snapshot intents must exist before mutation';
  END IF;
  IF EXISTS (
    SELECT 1 FROM oracle_candidate_source_snapshot_demo_objects
    WHERE plan_id = NEW.plan_id AND status <> 'verified'
  ) THEN
    RAISE EXCEPTION 'every candidate source-snapshot object must be verified before mutation';
  END IF;
  IF NEW.direction = 'update' THEN
    IF NEW.requested_cid IS DISTINCT FROM intent_row.target_cid OR
       intent_state <> 'update_in_flight' THEN
      RAISE EXCEPTION 'candidate source-snapshot update is not admitted';
    END IF;
    IF NEW.domain = 'query_table' AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_intents open_intent
      JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state open_state
        ON open_state.intent_id = open_intent.intent_id
      WHERE open_intent.plan_id = NEW.plan_id
        AND open_intent.domain = 'open_data'
        AND open_state.state = 'verified'
    ) THEN
      RAISE EXCEPTION 'query-table mutation requires verified open-data intent';
    END IF;
  ELSE
    IF NEW.requested_cid IS DISTINCT FROM intent_row.prior_cid OR
       intent_state <> 'rollback_in_flight' THEN
      RAISE EXCEPTION 'candidate source-snapshot rollback is not admitted';
    END IF;
    IF NEW.domain = 'open_data' AND EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_intents query_intent
      JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state query_state
        ON query_state.intent_id = query_intent.intent_id
      WHERE query_intent.plan_id = NEW.plan_id
        AND query_intent.domain = 'query_table'
        AND query_state.state NOT IN (
          'intent_recorded', 'prior_confirmed',
          'update_failed_prior_confirmed', 'rolled_back'
        )
    ) THEN
      RAISE EXCEPTION 'open-data rollback requires query-table rollback first';
    END IF;
  END IF;
  IF EXISTS (
    SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_attempts
    WHERE intent_id = NEW.intent_id AND direction = NEW.direction
  ) AND NEW.replay_authorization_sha256 IS NULL THEN
    RAISE EXCEPTION 'same-target IPNS replay requires durable authorization';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_ipns_attempt_insert_guard
  ON oracle_candidate_source_snapshot_demo_ipns_attempts;
CREATE TRIGGER oracle_candidate_source_snapshot_ipns_attempt_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_ipns_attempt_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_ipns_attempt_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.attempt_id IS DISTINCT FROM NEW.attempt_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.direction IS DISTINCT FROM NEW.direction OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.requested_cid IS DISTINCT FROM NEW.requested_cid OR
     OLD.replay_authorization_sha256 IS DISTINCT FROM NEW.replay_authorization_sha256 OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION 'candidate source-snapshot IPNS attempt is immutable or terminal';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_ipns_attempt_update_guard
  ON oracle_candidate_source_snapshot_demo_ipns_attempts;
CREATE TRIGGER oracle_candidate_source_snapshot_ipns_attempt_update_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_ipns_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_ipns_attempt_update();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_ipns_observation_immutable
  ON oracle_candidate_source_snapshot_demo_ipns_observations;
CREATE TRIGGER oracle_candidate_source_snapshot_ipns_observation_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_ipns_observations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_ipns_event_immutable
  ON oracle_candidate_source_snapshot_demo_ipns_events;
CREATE TRIGGER oracle_candidate_source_snapshot_ipns_event_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_ipns_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_event_immutable
  ON oracle_candidate_source_snapshot_demo_events;
CREATE TRIGGER oracle_candidate_source_snapshot_event_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

COMMENT ON TABLE oracle_candidate_source_snapshot_demo_plans IS
  'Separate v2 candidate-owned, noncanonical exact-source-snapshot demonstration plans; never owner/canonical publication authority.';
