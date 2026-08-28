CREATE TABLE IF NOT EXISTS oracle_pipeline_runs (
  run_id text PRIMARY KEY,
  workflow_id text NOT NULL UNIQUE,
  county text NOT NULL CHECK (county = 'pasco'),
  sample_algorithm text NOT NULL,
  sample_seed text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  as_of timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  source_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  limitations jsonb NOT NULL DEFAULT '[]'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS oracle_pipeline_attempts (
  attempt_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id) ON DELETE CASCADE,
  service_name text NOT NULL,
  handler_name text NOT NULL,
  attempt_number integer NOT NULL CHECK (attempt_number > 0),
  status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  error_code text,
  UNIQUE (run_id, service_name, handler_name, attempt_number)
);

CREATE TABLE IF NOT EXISTS oracle_source_artifacts (
  artifact_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id) ON DELETE CASCADE,
  source_system text NOT NULL,
  source_url text NOT NULL,
  local_uri text NOT NULL,
  ready_marker_uri text NOT NULL,
  byte_size bigint NOT NULL CHECK (byte_size >= 0),
  sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
  retrieved_at timestamptz NOT NULL,
  UNIQUE (run_id, source_url, sha256, local_uri)
);

CREATE TABLE IF NOT EXISTS oracle_properties (
  property_id text PRIMARY KEY CHECK (property_id ~ '^property_[a-f0-9]{32}$'),
  parcel_id text NOT NULL UNIQUE CHECK (parcel_id ~ '^parcel_[a-f0-9]{32}$'),
  county text NOT NULL CHECK (county = 'pasco'),
  source_system text NOT NULL CHECK (source_system = 'pasco_appraiser'),
  exact_folio text NOT NULL UNIQUE,
  matching_folio_digits text NOT NULL,
  site_address text,
  site_city text,
  site_zip text,
  property_use_code text,
  property_use_description text,
  acres numeric,
  total_square_feet numeric,
  heated_square_feet numeric,
  year_built integer,
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_ownerships (
  ownership_id text PRIMARY KEY CHECK (ownership_id ~ '^ownership_[a-f0-9]{32}$'),
  property_id text NOT NULL REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  owner_name_1 text,
  owner_name_2 text,
  mailing_address_1 text,
  mailing_address_2 text,
  mailing_city text,
  mailing_state text,
  mailing_zip text,
  mailing_country text,
  phone_availability text NOT NULL DEFAULT 'unavailable',
  email_availability text NOT NULL DEFAULT 'unavailable',
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  UNIQUE (property_id, source_record_hash)
);

CREATE TABLE IF NOT EXISTS oracle_coordinates (
  coordinate_id text PRIMARY KEY CHECK (coordinate_id ~ '^coordinate_[a-f0-9]{32}$'),
  property_id text NOT NULL UNIQUE REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
  longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
  crs text NOT NULL CHECK (crs = 'EPSG:4326'),
  provenance text NOT NULL,
  conversion_rule text NOT NULL,
  source_last_update text,
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS oracle_building_signals (
  building_signal_id text PRIMARY KEY CHECK (building_signal_id ~ '^building_[a-f0-9]{32}$'),
  property_id text NOT NULL REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  building_number text NOT NULL,
  building_section text NOT NULL,
  actual_year_built integer,
  effective_year_built integer,
  use_description text,
  roof_cover text,
  roof_structure text,
  observed_condition text,
  stories numeric,
  total_square_feet numeric,
  heated_square_feet numeric,
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  UNIQUE (property_id, building_number, building_section, source_record_hash)
);

CREATE TABLE IF NOT EXISTS oracle_roof_signals (
  roof_signal_id text PRIMARY KEY CHECK (roof_signal_id ~ '^roof_[a-f0-9]{32}$'),
  property_id text NOT NULL REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  basis text NOT NULL CHECK (basis IN ('roof_installation_date', 'roof_permit_completion', 'final_inspection', 'roof_permit_issue', 'year_built_proxy')),
  basis_quality text NOT NULL CHECK (basis_quality IN ('direct', 'proxy')),
  precision text NOT NULL CHECK (precision IN ('day', 'year')),
  basis_date date,
  basis_year integer,
  age_years integer NOT NULL CHECK (age_years BETWEEN 0 AND 500),
  as_of timestamptz NOT NULL,
  derivation_rule text NOT NULL,
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  UNIQUE (property_id, basis, source_record_hash)
);

CREATE TABLE IF NOT EXISTS oracle_permits (
  permit_id text PRIMARY KEY CHECK (permit_id ~ '^permit_[a-f0-9]{32}$'),
  property_id text NOT NULL REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  source_record_key text NOT NULL UNIQUE,
  permit_number text NOT NULL,
  record_type text NOT NULL,
  description text,
  project_name text,
  raw_status text,
  normalized_status text NOT NULL CHECK (normalized_status IN ('open', 'closed', 'expired', 'unknown')),
  source_record_date date,
  application_date date,
  issue_date date,
  final_date date,
  close_date date,
  is_open boolean,
  open_start_date date,
  open_start_basis text,
  open_duration_days integer CHECK (open_duration_days IS NULL OR open_duration_days >= 0),
  roofing_relevance boolean NOT NULL,
  contractor_availability text NOT NULL DEFAULT 'unavailable',
  contractor_unavailable_reason text NOT NULL DEFAULT 'not_provided_by_source',
  bbb_availability text NOT NULL DEFAULT 'unavailable',
  business_identity_availability text NOT NULL DEFAULT 'unavailable',
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS oracle_contractors (
  contractor_id text PRIMARY KEY,
  source_system text NOT NULL,
  source_record_key text NOT NULL,
  name text,
  license_number text,
  phone text,
  email text,
  source_record_hash text NOT NULL CHECK (source_record_hash ~ '^sha256:[a-f0-9]{64}$'),
  UNIQUE (source_system, source_record_key)
);

CREATE TABLE IF NOT EXISTS oracle_permit_contractors (
  permit_id text NOT NULL REFERENCES oracle_permits(permit_id) ON DELETE CASCADE,
  contractor_id text NOT NULL REFERENCES oracle_contractors(contractor_id) ON DELETE CASCADE,
  match_basis text NOT NULL,
  match_confidence numeric NOT NULL CHECK (match_confidence BETWEEN 0 AND 1),
  PRIMARY KEY (permit_id, contractor_id)
);

CREATE TABLE IF NOT EXISTS oracle_reconciliation_outcomes (
  reconciliation_id text PRIMARY KEY CHECK (reconciliation_id ~ '^reconciliation_[a-f0-9]{32}$'),
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id) ON DELETE CASCADE,
  check_name text NOT NULL,
  status text NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
  observed_count integer NOT NULL CHECK (observed_count >= 0),
  expected_count integer,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, check_name)
);

CREATE TABLE IF NOT EXISTS oracle_effect_journal (
  effect_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id) ON DELETE CASCADE,
  effect_name text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  status text NOT NULL CHECK (status IN ('planned', 'completed', 'failed')),
  result_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS oracle_properties_matching_folio_idx
  ON oracle_properties(matching_folio_digits);
CREATE INDEX IF NOT EXISTS oracle_permits_property_idx
  ON oracle_permits(property_id);
CREATE INDEX IF NOT EXISTS oracle_ownerships_property_idx
  ON oracle_ownerships(property_id);
