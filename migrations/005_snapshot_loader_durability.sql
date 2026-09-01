CREATE TABLE IF NOT EXISTS oracle_workflow_requests (
  idempotency_key text PRIMARY KEY,
  service_name text NOT NULL,
  handler_name text NOT NULL,
  request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  request_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_source_snapshots (
  snapshot_id text PRIMARY KEY CHECK (snapshot_id ~ '^snapshot_[a-f0-9]{32}$'),
  county text NOT NULL CHECK (county = 'pasco'),
  source_set_id text NOT NULL,
  manifest_version text NOT NULL,
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
  observation_start timestamptz NOT NULL,
  observation_end timestamptz NOT NULL,
  parser_version text NOT NULL,
  transform_version text NOT NULL,
  canonical_schema_sha256 text NOT NULL CHECK (
    canonical_schema_sha256 ~ '^[a-f0-9]{64}$'
  ),
  sampling jsonb NOT NULL,
  source_objects jsonb NOT NULL,
  manifest_created_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_prepared_inputs (
  prepared_input_id text PRIMARY KEY CHECK (
    prepared_input_id ~ '^prepared_[a-f0-9]{32}$'
  ),
  snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  input_kind text NOT NULL CHECK (input_kind IN ('pilot', 'scale')),
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
  prepared_sha256 text NOT NULL CHECK (prepared_sha256 ~ '^[a-f0-9]{64}$'),
  prepared_byte_size bigint NOT NULL CHECK (prepared_byte_size >= 0),
  manifest_relative_path text NOT NULL CHECK (
    manifest_relative_path !~ '(^/|(^|/)\.\.(/|$))'
  ),
  prepared_relative_path text NOT NULL CHECK (
    prepared_relative_path !~ '(^/|(^|/)\.\.(/|$))'
  ),
  selected_record_sha256 text NOT NULL CHECK (
    selected_record_sha256 ~ '^[a-f0-9]{64}$'
  ),
  selection_size integer NOT NULL CHECK (selection_size > 0),
  manifest_created_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_loader_effects (
  idempotency_key text PRIMARY KEY CHECK (
    idempotency_key ~ '^load_[a-f0-9]{32}$'
  ),
  request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  snapshot_id text NOT NULL REFERENCES oracle_source_snapshots(snapshot_id),
  prepared_input_id text NOT NULL REFERENCES oracle_prepared_inputs(prepared_input_id),
  status text NOT NULL CHECK (status IN ('applying', 'completed')),
  result_payload jsonb,
  result_sha256 text CHECK (
    result_sha256 IS NULL OR result_sha256 ~ '^[a-f0-9]{64}$'
  ),
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK (
    (status = 'applying' AND result_payload IS NULL AND result_sha256 IS NULL) OR
    (status = 'completed' AND result_payload IS NOT NULL AND result_sha256 IS NOT NULL)
  )
);

ALTER TABLE oracle_pipeline_runs
  ADD COLUMN IF NOT EXISTS snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id),
  ADD COLUMN IF NOT EXISTS request_sha256 text CHECK (
    request_sha256 IS NULL OR request_sha256 ~ '^[a-f0-9]{64}$'
  );

ALTER TABLE oracle_source_artifacts
  ADD COLUMN IF NOT EXISTS snapshot_id text REFERENCES oracle_source_snapshots(snapshot_id),
  ADD COLUMN IF NOT EXISTS prepared_input_id text REFERENCES oracle_prepared_inputs(prepared_input_id);

CREATE INDEX IF NOT EXISTS oracle_pipeline_runs_snapshot_idx
  ON oracle_pipeline_runs(snapshot_id);

CREATE INDEX IF NOT EXISTS oracle_loader_effects_snapshot_idx
  ON oracle_loader_effects(snapshot_id, prepared_input_id);
