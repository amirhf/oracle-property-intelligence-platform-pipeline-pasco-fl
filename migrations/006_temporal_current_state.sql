ALTER TABLE oracle_source_snapshots
  ADD COLUMN IF NOT EXISTS coverage_mode text,
  ADD COLUMN IF NOT EXISTS scope_id text,
  ADD COLUMN IF NOT EXISTS entity_type text,
  ADD COLUMN IF NOT EXISTS authority_source_system text,
  ADD COLUMN IF NOT EXISTS authority_source_identifier text,
  ADD COLUMN IF NOT EXISTS coverage_metadata jsonb,
  ADD COLUMN IF NOT EXISTS previous_authoritative_snapshot_id text
    REFERENCES oracle_source_snapshots(snapshot_id);

-- Historical bounded runs predate coverage manifests. Give each such snapshot
-- an isolated sample scope so it can never participate in absence-based
-- reconciliation.
UPDATE oracle_source_snapshots
SET
  coverage_mode = COALESCE(coverage_mode, 'sample'),
  scope_id = COALESCE(scope_id, 'scope_' || md5('legacy-sample:' || snapshot_id)),
  entity_type = COALESCE(entity_type, 'property_existence'),
  authority_source_system = COALESCE(authority_source_system, 'pasco_appraiser'),
  authority_source_identifier = COALESCE(
    authority_source_identifier,
    'legacy_sample_without_authority_claim'
  ),
  coverage_metadata = COALESCE(
    coverage_metadata,
    jsonb_build_object(
      'classification', 'legacy_sample',
      'deactivationEligible', false,
      'mode', 'sample'
    )
  );

ALTER TABLE oracle_source_snapshots
  ALTER COLUMN coverage_mode SET NOT NULL,
  ALTER COLUMN scope_id SET NOT NULL,
  ALTER COLUMN entity_type SET NOT NULL,
  ALTER COLUMN authority_source_system SET NOT NULL,
  ALTER COLUMN authority_source_identifier SET NOT NULL,
  ALTER COLUMN coverage_metadata SET NOT NULL;

ALTER TABLE oracle_source_snapshots
  ADD CONSTRAINT oracle_source_snapshots_coverage_mode_check
    CHECK (coverage_mode IN ('sample', 'partial', 'authoritative_complete')),
  ADD CONSTRAINT oracle_source_snapshots_scope_id_check
    CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  ADD CONSTRAINT oracle_source_snapshots_entity_type_check
    CHECK (entity_type = 'property_existence');

ALTER TABLE oracle_properties
  ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS lifecycle_scope_id text,
  ADD COLUMN IF NOT EXISTS inactive_at_snapshot_id text
    REFERENCES oracle_source_snapshots(snapshot_id),
  ADD COLUMN IF NOT EXISTS inactivation_reason text;

ALTER TABLE oracle_properties
  ADD CONSTRAINT oracle_properties_lifecycle_state_check CHECK (
    (is_active AND inactive_at_snapshot_id IS NULL AND inactivation_reason IS NULL)
    OR
    (
      NOT is_active
      AND inactive_at_snapshot_id IS NOT NULL
      AND inactivation_reason = 'absent_from_authoritative_complete_snapshot'
    )
  );

CREATE TABLE IF NOT EXISTS oracle_authoritative_scope_heads (
  scope_id text PRIMARY KEY CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  county text NOT NULL CHECK (county = 'pasco'),
  entity_type text NOT NULL CHECK (entity_type = 'property_existence'),
  authority_source_system text NOT NULL CHECK (
    authority_source_system = 'pasco_appraiser'
  ),
  authority_source_identifier text NOT NULL,
  current_snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  current_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (county, entity_type)
);

CREATE TABLE IF NOT EXISTS oracle_property_scope_state (
  property_id text NOT NULL REFERENCES oracle_properties(property_id),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  coverage_mode text NOT NULL CHECK (
    coverage_mode IN ('sample', 'partial', 'authoritative_complete')
  ),
  lifecycle_status text NOT NULL CHECK (lifecycle_status IN ('active', 'inactive')),
  first_seen_snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  last_seen_snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  last_reconciled_snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  valid_from_snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  valid_to_snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id),
  inactivation_reason text,
  last_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_source_record_hash text NOT NULL CHECK (
    last_source_record_hash ~ '^sha256:[a-f0-9]{64}$'
  ),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (property_id, scope_id),
  CHECK (
    (lifecycle_status = 'active' AND valid_to_snapshot_id IS NULL AND inactivation_reason IS NULL)
    OR
    (
      lifecycle_status = 'inactive'
      AND valid_to_snapshot_id IS NOT NULL
      AND inactivation_reason = 'absent_from_authoritative_complete_snapshot'
    )
  )
);

CREATE TABLE IF NOT EXISTS oracle_property_lifecycle_events (
  event_id text PRIMARY KEY CHECK (event_id ~ '^lifecycle_[a-f0-9]{32}$'),
  property_id text NOT NULL REFERENCES oracle_properties(property_id),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  previous_snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id),
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  event_type text NOT NULL CHECK (
    event_type IN ('new', 'changed', 'unchanged', 'inactivated', 'reactivated')
  ),
  source_record_hash text CHECK (
    source_record_hash IS NULL OR source_record_hash ~ '^sha256:[a-f0-9]{64}$'
  ),
  previous_source_record_hash text CHECK (
    previous_source_record_hash IS NULL
    OR previous_source_record_hash ~ '^sha256:[a-f0-9]{64}$'
  ),
  reason text,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (property_id, scope_id, snapshot_id, event_type)
);

CREATE OR REPLACE FUNCTION oracle_reject_lifecycle_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'oracle_property_lifecycle_events is immutable';
END;
$$;

DROP TRIGGER IF EXISTS oracle_property_lifecycle_events_immutable
  ON oracle_property_lifecycle_events;
CREATE TRIGGER oracle_property_lifecycle_events_immutable
  BEFORE UPDATE OR DELETE ON oracle_property_lifecycle_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_lifecycle_event_mutation();

CREATE INDEX IF NOT EXISTS oracle_properties_active_idx
  ON oracle_properties(is_active, property_id);
CREATE INDEX IF NOT EXISTS oracle_property_scope_state_status_idx
  ON oracle_property_scope_state(scope_id, lifecycle_status, property_id);
CREATE INDEX IF NOT EXISTS oracle_property_lifecycle_events_snapshot_idx
  ON oracle_property_lifecycle_events(snapshot_id, event_type);
