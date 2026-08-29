CREATE TABLE IF NOT EXISTS oracle_publication_plans (
  plan_id text PRIMARY KEY CHECK (plan_id ~ '^plan_[a-f0-9]{32}$'),
  plan_sha256 text NOT NULL UNIQUE CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_version text NOT NULL CHECK (plan_version = '1.0.0'),
  county text NOT NULL CHECK (county = 'pasco'),
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id),
  coverage_mode text NOT NULL CHECK (
    coverage_mode IN ('sample', 'partial', 'authoritative_complete')
  ),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  approvable boolean NOT NULL,
  executable boolean NOT NULL,
  plan_payload jsonb NOT NULL,
  generated_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    coverage_mode != 'authoritative_complete' OR snapshot_id IS NOT NULL
  )
);

CREATE TABLE IF NOT EXISTS oracle_publication_state (
  county text PRIMARY KEY CHECK (county = 'pasco'),
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  state text NOT NULL CHECK (state IN (
    'prepared',
    'validated',
    'awaiting_configuration',
    'awaiting_approval',
    'approved',
    'executing',
    'completed',
    'failed_terminal'
  )),
  revision integer NOT NULL CHECK (revision > 0),
  terminal_reason text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (state = 'failed_terminal' AND terminal_reason IS NOT NULL) OR
    (state != 'failed_terminal' AND terminal_reason IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS oracle_publication_state_events (
  event_id text PRIMARY KEY CHECK (event_id ~ '^pubstate_[a-f0-9]{32}$'),
  county text NOT NULL CHECK (county = 'pasco'),
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  from_state text,
  to_state text NOT NULL CHECK (to_state IN (
    'prepared',
    'validated',
    'awaiting_configuration',
    'awaiting_approval',
    'approved',
    'executing',
    'completed',
    'failed_terminal'
  )),
  transition_sha256 text NOT NULL CHECK (
    transition_sha256 ~ '^[a-f0-9]{64}$'
  ),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (plan_id, to_state, transition_sha256)
);

CREATE TABLE IF NOT EXISTS oracle_publication_approvals (
  plan_id text PRIMARY KEY REFERENCES oracle_publication_plans(plan_id),
  approval_id text NOT NULL UNIQUE CHECK (
    approval_id ~ '^approval_[a-f0-9]{32}$'
  ),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  approver_reference text NOT NULL CHECK (
    approver_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  approved_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_publication_object_effects (
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  object_key text NOT NULL CHECK (
    object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)'
  ),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_byte_size bigint NOT NULL CHECK (expected_byte_size >= 0),
  status text NOT NULL CHECK (status IN ('pending', 'uploaded', 'verified')),
  uploaded_cid text,
  verified_cid text,
  completed_at timestamptz,
  PRIMARY KEY (plan_id, domain, object_key),
  CHECK (
    (status = 'pending' AND uploaded_cid IS NULL AND verified_cid IS NULL AND completed_at IS NULL) OR
    (status = 'uploaded' AND uploaded_cid IS NOT NULL AND verified_cid IS NULL AND completed_at IS NULL) OR
    (status = 'verified' AND uploaded_cid IS NOT NULL AND verified_cid = uploaded_cid AND completed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_effects (
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  ipns_label text NOT NULL,
  ipns_network_key text,
  prior_cid text,
  target_cid text,
  status text NOT NULL CHECK (status IN ('pending', 'updated', 'verified')),
  mutation_performed boolean NOT NULL DEFAULT false,
  public_resolution_verified boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (plan_id, domain),
  CHECK (
    (status = 'pending' AND NOT mutation_performed AND NOT public_resolution_verified AND target_cid IS NULL) OR
    (status = 'updated' AND mutation_performed AND NOT public_resolution_verified AND target_cid IS NOT NULL) OR
    (status = 'verified' AND mutation_performed AND public_resolution_verified AND target_cid IS NOT NULL)
  )
);

ALTER TABLE oracle_publication_dry_runs
  ADD COLUMN IF NOT EXISTS plan_id text REFERENCES oracle_publication_plans(plan_id),
  ADD COLUMN IF NOT EXISTS coverage_mode text,
  ADD COLUMN IF NOT EXISTS scope_id text,
  ADD COLUMN IF NOT EXISTS snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id);

CREATE OR REPLACE FUNCTION oracle_reject_publication_state_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'oracle_publication_state_events is immutable';
END;
$$;

DROP TRIGGER IF EXISTS oracle_publication_state_events_immutable
  ON oracle_publication_state_events;
CREATE TRIGGER oracle_publication_state_events_immutable
  BEFORE UPDATE OR DELETE ON oracle_publication_state_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_publication_state_event_mutation();

CREATE INDEX IF NOT EXISTS oracle_publication_objects_status_idx
  ON oracle_publication_object_effects(plan_id, domain, status);
CREATE INDEX IF NOT EXISTS oracle_publication_state_events_plan_idx
  ON oracle_publication_state_events(plan_id, recorded_at);
