ALTER TABLE oracle_pipeline_runs
  ADD COLUMN IF NOT EXISTS selection_size integer CHECK (selection_size > 0),
  ADD COLUMN IF NOT EXISTS database_size_before_bytes bigint CHECK (
    database_size_before_bytes IS NULL OR database_size_before_bytes >= 0
  );

CREATE TABLE IF NOT EXISTS oracle_publication_dry_runs (
  dry_run_id text PRIMARY KEY CHECK (dry_run_id ~ '^dryrun_[a-f0-9]{32}$'),
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  county text NOT NULL CHECK (county = 'pasco'),
  status text NOT NULL CHECK (status IN ('building', 'validated', 'failed')),
  open_data_manifest_sha256 text CHECK (
    open_data_manifest_sha256 IS NULL OR
    open_data_manifest_sha256 ~ '^[a-f0-9]{64}$'
  ),
  query_table_sha256 text CHECK (
    query_table_sha256 IS NULL OR query_table_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_sha256 text CHECK (
    plan_sha256 IS NULL OR plan_sha256 ~ '^[a-f0-9]{64}$'
  ),
  property_count integer NOT NULL CHECK (property_count >= 0),
  object_count integer NOT NULL DEFAULT 0 CHECK (object_count >= 0),
  result jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (run_id, county)
);
