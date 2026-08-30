-- Owner-accepted authority evidence and bounded Loader batch audit records for
-- the exact hash-bound Pasco appraisal snapshot. This is additive and does not
-- reclassify any historical sample run or publication.

ALTER TABLE oracle_prepared_inputs
  DROP CONSTRAINT IF EXISTS oracle_prepared_inputs_input_kind_check;
ALTER TABLE oracle_prepared_inputs
  ADD CONSTRAINT oracle_prepared_inputs_input_kind_check
  CHECK (input_kind IN ('pilot', 'scale', 'authoritative'));

CREATE TABLE IF NOT EXISTS oracle_source_authority_records (
  authority_record_id text PRIMARY KEY CHECK (
    authority_record_id ~ '^authority_[a-f0-9]{32}$'
  ),
  authority_class text NOT NULL CHECK (
    authority_class = 'owner_assumed_authoritative_snapshot'
  ),
  source_snapshot_id text NOT NULL UNIQUE
    REFERENCES oracle_source_snapshots(snapshot_id),
  source_run_id text NOT NULL UNIQUE REFERENCES oracle_pipeline_runs(run_id),
  source_system text NOT NULL CHECK (source_system = 'pasco_appraiser'),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  decision_sha256 text NOT NULL CHECK (decision_sha256 ~ '^[a-f0-9]{64}$'),
  completeness_evidence_sha256 text NOT NULL CHECK (
    completeness_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  source_snapshot_manifest_sha256 text NOT NULL CHECK (
    source_snapshot_manifest_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authority_payload jsonb NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (authority_payload->>'authorityClass')
      IS NOT DISTINCT FROM authority_class
  ),
  CHECK (
    (authority_payload->>'sourceSystem') IS NOT DISTINCT FROM source_system
  ),
  CHECK (
    ((authority_payload #>> '{counts,expected}')::integer)
      IS NOT DISTINCT FROM 325213
  ),
  CHECK (
    ((authority_payload #>> '{counts,accepted}')::integer)
      IS NOT DISTINCT FROM 325213
  ),
  CHECK (
    ((authority_payload #>> '{counts,rejected}')::integer)
      IS NOT DISTINCT FROM 0
  ),
  CHECK (
    ((authority_payload #>> '{counts,duplicateFolios}')::integer)
      IS NOT DISTINCT FROM 0
  )
);

CREATE TABLE IF NOT EXISTS oracle_loader_batch_checkpoints (
  checkpoint_id text PRIMARY KEY CHECK (
    checkpoint_id ~ '^checkpoint_[a-f0-9]{32}$'
  ),
  source_snapshot_id text NOT NULL
    REFERENCES oracle_projection_snapshots(snapshot_id),
  source_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  phase text NOT NULL CHECK (phase IN (
    'property_versions', 'fact_versions', 'materialized_properties',
    'materialized_facts'
  )),
  batch_index integer NOT NULL CHECK (batch_index >= 0),
  row_count integer NOT NULL CHECK (row_count > 0),
  first_property_id text NOT NULL,
  last_property_id text NOT NULL,
  batch_sha256 text NOT NULL CHECK (batch_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_snapshot_id, phase, batch_index)
);

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_source_authority_records', 'oracle_loader_batch_checkpoints'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I_immutable ON %I',
      immutable_table, immutable_table
    );
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END;
$$;
