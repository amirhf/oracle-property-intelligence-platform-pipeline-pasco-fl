ALTER TABLE oracle_pipeline_runs
  ADD COLUMN IF NOT EXISTS coverage_mode text NOT NULL DEFAULT 'sample',
  ADD COLUMN IF NOT EXISTS coverage_scope_id text;

ALTER TABLE oracle_pipeline_runs
  ADD CONSTRAINT oracle_pipeline_runs_coverage_mode_check CHECK (
    coverage_mode IN ('sample', 'partial', 'authoritative_complete')
  ),
  ADD CONSTRAINT oracle_pipeline_runs_coverage_scope_id_check CHECK (
    coverage_scope_id IS NULL OR coverage_scope_id ~ '^scope_[a-f0-9]{32}$'
  );

-- Runs created before snapshot-bound ingestion are the bounded pilot/5k/25k
-- selections. The default intentionally classifies them as samples; no
-- authoritative scope or absence semantics are inferred retroactively.
UPDATE oracle_pipeline_runs
SET coverage_mode = 'sample', coverage_scope_id = NULL
WHERE snapshot_id IS NULL;
