BEGIN;

-- Additive, pre-import hardening for the candidate CAR ledger. The ledger is
-- intentionally required to be empty: no historical CAR evidence is rewritten
-- and this migration performs no provider request or publication mutation.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM oracle_candidate_source_snapshot_car_artifacts) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_attempts
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_inspections
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_receipts
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_bulk_verifications
     ) THEN
    RAISE EXCEPTION
      'CAR exact-evidence migration requires an empty CAR evidence ledger';
  END IF;
END;
$$;

-- Remove the pre-pin-roots endpoint checks. The new checks below permit only
-- the exact compiled Filebase RPC endpoint used by the closed transport.
DO $$
DECLARE
  constraint_row record;
BEGIN
  FOR constraint_row IN
    SELECT constraint_entry.conname
    FROM pg_constraint constraint_entry
    WHERE constraint_entry.conrelid =
      'oracle_candidate_source_snapshot_car_import_authorizations'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_get_constraintdef(constraint_entry.oid) LIKE
        '%https://rpc.filebase.io/api/v0/dag/import%'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_car_import_authorizations DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;
  FOR constraint_row IN
    SELECT constraint_entry.conname
    FROM pg_constraint constraint_entry
    WHERE constraint_entry.conrelid =
      'oracle_candidate_source_snapshot_car_import_attempts'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_get_constraintdef(constraint_entry.oid) LIKE
        '%https://rpc.filebase.io/api/v0/dag/import%'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_car_import_attempts DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_car_artifacts
  ADD COLUMN bucket_identity text NOT NULL,
  ADD COLUMN rpc_endpoint text NOT NULL,
  ADD CONSTRAINT oracle_css_car_artifact_bucket_check CHECK (
    length(bucket_identity) BETWEEN 1 AND 255
  ),
  ADD CONSTRAINT oracle_css_car_artifact_rpc_endpoint_check CHECK (
    rpc_endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true'
  );

ALTER TABLE oracle_candidate_source_snapshot_car_import_authorizations
  ADD COLUMN open_data_bucket_identity text NOT NULL,
  ADD COLUMN query_table_bucket_identity text NOT NULL,
  ADD COLUMN open_data_bucket_token_sha256 text NOT NULL,
  ADD COLUMN query_table_bucket_token_sha256 text NOT NULL,
  ADD COLUMN overall_timeout_ms integer NOT NULL,
  ADD CONSTRAINT oracle_css_car_authorization_bucket_check CHECK (
    length(open_data_bucket_identity) BETWEEN 1 AND 255 AND
    length(query_table_bucket_identity) BETWEEN 1 AND 255 AND
    open_data_bucket_identity <> query_table_bucket_identity
  ),
  ADD CONSTRAINT oracle_css_car_authorization_token_check CHECK (
    open_data_bucket_token_sha256 ~ '^[a-f0-9]{64}$' AND
    query_table_bucket_token_sha256 ~ '^[a-f0-9]{64}$' AND
    open_data_bucket_token_sha256 <> query_table_bucket_token_sha256
  ),
  ADD CONSTRAINT oracle_css_car_authorization_timeout_check CHECK (
    overall_timeout_ms BETWEEN 60000 AND 14400000
  ),
  ADD CONSTRAINT oracle_css_car_authorization_rpc_endpoint_check CHECK (
    endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true' AND
    import_method = 'rpc_dag_import'
  );

ALTER TABLE oracle_candidate_source_snapshot_car_import_attempts
  ADD COLUMN bucket_identity text NOT NULL,
  ADD COLUMN bucket_token_sha256 text NOT NULL,
  ADD COLUMN overall_timeout_ms integer NOT NULL,
  ADD COLUMN reserved_request_count integer NOT NULL DEFAULT 5,
  ADD COLUMN request_cost_usd numeric(18, 12) NOT NULL
    DEFAULT 0.000022500000,
  ADD CONSTRAINT oracle_css_car_attempt_bucket_check CHECK (
    length(bucket_identity) BETWEEN 1 AND 255 AND
    bucket_token_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_css_car_attempt_timeout_check CHECK (
    overall_timeout_ms BETWEEN 60000 AND 14400000
  ),
  ADD CONSTRAINT oracle_css_car_attempt_request_reservation_check CHECK (
    reserved_request_count = 5
  ),
  ADD CONSTRAINT oracle_css_car_attempt_rpc_endpoint_check CHECK (
    endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true' AND
    import_method = 'rpc_dag_import'
  ),
  ADD CONSTRAINT oracle_css_car_attempt_cost_check CHECK (
    request_cost_usd = 0.000022500000
  );

ALTER TABLE oracle_candidate_source_snapshot_car_import_attempt_outcomes
  ADD COLUMN bucket_identity text NOT NULL,
  ADD COLUMN rpc_endpoint text NOT NULL,
  ADD COLUMN observed_root_count integer,
  ADD COLUMN final_recursive_pin_status text,
  ADD CONSTRAINT oracle_css_car_outcome_bucket_check CHECK (
    length(bucket_identity) BETWEEN 1 AND 255
  ),
  ADD CONSTRAINT oracle_css_car_outcome_rpc_endpoint_check CHECK (
    rpc_endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true'
  ),
  ADD CONSTRAINT oracle_css_car_outcome_provider_proof_check CHECK (
    (outcome = 'verified' AND observed_root_count > 0 AND
      observed_root_set_sha256 IS NOT NULL AND
      final_recursive_pin_status = 'pinned') OR
    (outcome <> 'verified' AND observed_root_count IS NULL AND
      final_recursive_pin_status IS NULL)
  );

CREATE TABLE oracle_candidate_source_snapshot_car_gateway_evidence (
  gateway_evidence_id text PRIMARY KEY CHECK (
    gateway_evidence_id ~ '^snapshotdemocargateway_[a-f0-9]{32}$'
  ),
  evidence_version text NOT NULL CHECK (
    evidence_version = 'candidate-source-snapshot-car-gateway-evidence-v1'
  ),
  car_import_attempt_id text NOT NULL,
  car_import_outcome_id text NOT NULL UNIQUE,
  car_import_inspection_id text,
  inspection_sha256 text CHECK (
    inspection_sha256 IS NULL OR inspection_sha256 ~ '^[a-f0-9]{64}$'
  ),
  provider_proof_path text NOT NULL CHECK (
    provider_proof_path IN ('verified_outcome', 'positive_inspection')
  ),
  car_artifact_id text NOT NULL UNIQUE,
  plan_id text NOT NULL,
  bucket_identity text NOT NULL CHECK (
    length(bucket_identity) BETWEEN 1 AND 255
  ),
  rpc_endpoint text NOT NULL CHECK (
    rpc_endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true'
  ),
  gateway_origin text NOT NULL CHECK (
    gateway_origin = 'https://ipfs.filebase.io'
  ),
  gateway_path_policy text NOT NULL CHECK (
    gateway_path_policy = 'immutable_cid_raw_block_v1'
  ),
  root_cid text NOT NULL CHECK (
    root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  provider_http_status integer NOT NULL CHECK (provider_http_status = 200),
  provider_request_id_hash text CHECK (
    provider_request_id_hash IS NULL OR
    provider_request_id_hash ~ '^[a-f0-9]{64}$'
  ),
  root_block_bytes integer NOT NULL CHECK (
    root_block_bytes BETWEEN 1 AND 1048576
  ),
  root_block_sha256 text NOT NULL CHECK (
    root_block_sha256 ~ '^[a-f0-9]{64}$'
  ),
  validation_result text NOT NULL CHECK (validation_result = 'cid_verified'),
  observed_at timestamptz NOT NULL,
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  evidence_payload jsonb NOT NULL,
  evidence_sha256 text NOT NULL UNIQUE CHECK (
    evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  CHECK (
    (provider_proof_path = 'verified_outcome' AND
      car_import_inspection_id IS NULL AND inspection_sha256 IS NULL) OR
    (provider_proof_path = 'positive_inspection' AND
      car_import_inspection_id IS NOT NULL AND inspection_sha256 IS NOT NULL)
  ),
  UNIQUE (
    gateway_evidence_id, car_import_outcome_id, car_artifact_id, plan_id
  ),
  FOREIGN KEY (
    car_import_outcome_id, car_import_attempt_id, car_artifact_id, plan_id
  ) REFERENCES oracle_candidate_source_snapshot_car_import_attempt_outcomes(
    car_import_outcome_id, car_import_attempt_id, car_artifact_id, plan_id
  ),
  FOREIGN KEY (car_import_inspection_id)
    REFERENCES oracle_candidate_source_snapshot_car_import_inspections(
      car_import_inspection_id
    )
);

ALTER TABLE oracle_candidate_source_snapshot_car_import_receipts
  DROP CONSTRAINT oracle_css_car_receipt_provider_identifier_check,
  ADD COLUMN provider_outcome_id text NOT NULL,
  ADD COLUMN gateway_evidence_id text NOT NULL,
  ADD COLUMN bucket_identity text NOT NULL,
  ADD COLUMN rpc_endpoint text NOT NULL,
  ADD COLUMN bucket_token_sha256 text NOT NULL,
  ADD COLUMN overall_timeout_ms integer NOT NULL,
  ADD COLUMN reserved_request_count integer NOT NULL,
  ADD COLUMN provider_proof_path text NOT NULL,
  ADD COLUMN provider_inspection_id text,
  ADD COLUMN provider_inspection_sha256 text,
  ADD CONSTRAINT oracle_css_car_receipt_bucket_check CHECK (
    length(bucket_identity) BETWEEN 1 AND 255 AND
    bucket_token_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_css_car_receipt_timeout_check CHECK (
    overall_timeout_ms BETWEEN 60000 AND 14400000
  ),
  ADD CONSTRAINT oracle_css_car_receipt_request_reservation_check CHECK (
    reserved_request_count BETWEEN 5 AND 10 AND
    reserved_request_count = import_attempt_count * 5
  ),
  ADD CONSTRAINT oracle_css_car_receipt_proof_path_check CHECK (
    (provider_proof_path = 'verified_outcome' AND
      provider_inspection_id IS NULL AND
      provider_inspection_sha256 IS NULL) OR
    (provider_proof_path = 'positive_inspection' AND
      provider_inspection_id IS NOT NULL AND
      provider_inspection_sha256 ~ '^[a-f0-9]{64}$')
  ),
  ADD CONSTRAINT oracle_css_car_receipt_rpc_endpoint_check CHECK (
    rpc_endpoint =
      'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true'
  ),
  ADD CONSTRAINT oracle_css_car_receipt_outcome_fk FOREIGN KEY (
    provider_outcome_id
  ) REFERENCES oracle_candidate_source_snapshot_car_import_attempt_outcomes(
    car_import_outcome_id
  ),
  ADD CONSTRAINT oracle_css_car_receipt_gateway_fk FOREIGN KEY (
    gateway_evidence_id, provider_outcome_id, car_artifact_id, plan_id
  ) REFERENCES oracle_candidate_source_snapshot_car_gateway_evidence(
    gateway_evidence_id, car_import_outcome_id, car_artifact_id, plan_id
  ),
  ADD CONSTRAINT oracle_css_car_receipt_inspection_fk FOREIGN KEY (
    provider_inspection_id
  ) REFERENCES oracle_candidate_source_snapshot_car_import_inspections(
    car_import_inspection_id
  );

CREATE OR REPLACE FUNCTION oracle_css_car_artifact_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    artifact.car_role || chr(31) || artifact.car_artifact_id || chr(31) ||
    artifact.car_sha256 || chr(31) || artifact.car_bytes::text || chr(31) ||
    artifact.primary_root_cid || chr(31) || artifact.root_set_sha256 ||
    chr(31) || artifact.member_set_sha256 || chr(31) ||
    artifact.member_count::text || chr(31) ||
    artifact.member_logical_bytes::text || chr(31) ||
    artifact.bucket_identity || chr(31) || artifact.rpc_endpoint,
    chr(30) ORDER BY artifact.car_role
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_car_artifacts artifact
  WHERE artifact.plan_id = checked_plan_id;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_artifact_binding_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_bucket text;
BEGIN
  SELECT CASE NEW.car_role
    WHEN 'open_data' THEN plan.plan_payload #>> '{targets,openData,bucket}'
    ELSE plan.plan_payload #>> '{targets,queryTable,bucket}'
  END INTO STRICT expected_bucket
  FROM oracle_candidate_source_snapshot_demo_plans plan
  WHERE plan.plan_id = NEW.plan_id AND plan.plan_sha256 = NEW.plan_sha256;
  IF expected_bucket IS NULL OR
     (NEW.bucket_identity IS NOT NULL AND
       NEW.bucket_identity IS DISTINCT FROM expected_bucket) OR
     (NEW.rpc_endpoint IS NOT NULL AND NEW.rpc_endpoint IS DISTINCT FROM
       'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true') THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR artifact bucket is not exact';
  END IF;
  NEW.bucket_identity := expected_bucket;
  NEW.rpc_endpoint :=
    'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true';
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_artifact_042_binding_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_artifacts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_artifact_binding_042();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_authorization_binding_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_open_bucket text;
  expected_query_bucket text;
BEGIN
  SELECT plan.plan_payload #>> '{targets,openData,bucket}',
         plan.plan_payload #>> '{targets,queryTable,bucket}'
  INTO STRICT expected_open_bucket, expected_query_bucket
  FROM oracle_candidate_source_snapshot_demo_plans plan
  WHERE plan.plan_id = NEW.plan_id AND plan.plan_sha256 = NEW.plan_sha256;
  IF expected_open_bucket IS NULL OR expected_query_bucket IS NULL OR
     expected_open_bucket = expected_query_bucket OR
     NEW.open_data_bucket_token_sha256 IS NULL OR
     NEW.query_table_bucket_token_sha256 IS NULL OR
     NEW.open_data_bucket_token_sha256 =
       NEW.query_table_bucket_token_sha256 OR
     NEW.overall_timeout_ms NOT BETWEEN 60000 AND 14400000 OR
     position(NEW.open_data_bucket_token_sha256 IN
       NEW.authorization_statement) = 0 OR
     position(NEW.query_table_bucket_token_sha256 IN
       NEW.authorization_statement) = 0 OR
     position(NEW.overall_timeout_ms::text IN NEW.authorization_statement) = 0 OR
     (NEW.open_data_bucket_identity IS NOT NULL AND
       NEW.open_data_bucket_identity IS DISTINCT FROM expected_open_bucket) OR
     (NEW.query_table_bucket_identity IS NOT NULL AND
       NEW.query_table_bucket_identity IS DISTINCT FROM expected_query_bucket) OR
     NEW.endpoint IS DISTINCT FROM
       'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true' OR
     NEW.import_method IS DISTINCT FROM 'rpc_dag_import' OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_artifacts artifact
       WHERE artifact.plan_id = NEW.plan_id AND (
         artifact.bucket_identity IS DISTINCT FROM (CASE artifact.car_role
           WHEN 'open_data' THEN expected_open_bucket
           ELSE expected_query_bucket
         END) OR artifact.rpc_endpoint IS DISTINCT FROM NEW.endpoint
       )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR authorization bucket is not exact';
  END IF;
  NEW.open_data_bucket_identity := expected_open_bucket;
  NEW.query_table_bucket_identity := expected_query_bucket;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_authorization_042_binding_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_authorizations
  FOR EACH ROW
  EXECUTE FUNCTION oracle_guard_css_car_authorization_binding_042();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_attempt_binding_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_bucket text;
  expected_bucket_token_sha256 text;
  expected_endpoint text;
  expected_overall_timeout_ms integer;
  hard_spending_ceiling_usd numeric(18, 12);
  maximum_request_count integer;
  existing_reserved_request_count integer;
  existing_reserved_request_cost numeric(18, 12);
  admitted_request_count integer;
  admitted_request_cost numeric(18, 12);
BEGIN
  SELECT artifact.bucket_identity, artifact.rpc_endpoint,
         CASE artifact.car_role
           WHEN 'open_data' THEN auth.open_data_bucket_token_sha256
           ELSE auth.query_table_bucket_token_sha256
         END,
         auth.overall_timeout_ms, auth.hard_spending_ceiling_usd
  INTO STRICT expected_bucket, expected_endpoint,
    expected_bucket_token_sha256, expected_overall_timeout_ms,
    hard_spending_ceiling_usd
  FROM oracle_candidate_source_snapshot_car_artifacts artifact
  JOIN oracle_candidate_source_snapshot_car_import_authorizations auth
    ON auth.car_authorization_id = NEW.car_authorization_id
   AND auth.plan_id = artifact.plan_id
  WHERE artifact.car_artifact_id = NEW.car_artifact_id
    AND artifact.plan_id = NEW.plan_id;
  SELECT plan.maximum_request_count, accounting.request_count,
         accounting.request_cost_usd
  INTO STRICT maximum_request_count, admitted_request_count,
    admitted_request_cost
  FROM oracle_candidate_source_snapshot_demo_plans plan
  JOIN oracle_candidate_source_snapshot_demo_accounting accounting
    ON accounting.plan_id = plan.plan_id
  WHERE plan.plan_id = NEW.plan_id
  FOR UPDATE OF plan, accounting;
  SELECT coalesce(sum(attempt.reserved_request_count), 0)::integer,
         coalesce(sum(attempt.request_cost_usd), 0)::numeric(18, 12)
  INTO existing_reserved_request_count, existing_reserved_request_cost
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.plan_id = NEW.plan_id;
  IF (NEW.bucket_identity IS NOT NULL AND
       NEW.bucket_identity IS DISTINCT FROM expected_bucket) OR
     (NEW.bucket_token_sha256 IS NOT NULL AND
       NEW.bucket_token_sha256 IS DISTINCT FROM
         expected_bucket_token_sha256) OR
     (NEW.overall_timeout_ms IS NOT NULL AND
       NEW.overall_timeout_ms IS DISTINCT FROM
         expected_overall_timeout_ms) OR
     (NEW.reserved_request_count IS NOT NULL AND
       NEW.reserved_request_count IS DISTINCT FROM 5) OR
     (NEW.request_cost_usd IS NOT NULL AND
       NEW.request_cost_usd IS DISTINCT FROM 0.000022500000) OR
     admitted_request_count + existing_reserved_request_count + 5 >
       maximum_request_count OR
     admitted_request_cost + existing_reserved_request_cost +
       0.000022500000 > hard_spending_ceiling_usd OR
     NEW.endpoint IS DISTINCT FROM expected_endpoint OR
     expected_endpoint IS DISTINCT FROM
       'https://rpc.filebase.io/api/v0/dag/import?pin-roots=true' THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR attempt bucket is not exact';
  END IF;
  NEW.bucket_identity := expected_bucket;
  NEW.bucket_token_sha256 := expected_bucket_token_sha256;
  NEW.overall_timeout_ms := expected_overall_timeout_ms;
  NEW.reserved_request_count := 5;
  NEW.request_cost_usd := 0.000022500000;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_attempt_042_binding_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_attempt_binding_042();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_outcome_binding_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_row oracle_candidate_source_snapshot_car_import_attempts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE car_import_attempt_id = NEW.car_import_attempt_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = attempt_row.car_artifact_id;
  IF (NEW.bucket_identity IS NOT NULL AND NEW.bucket_identity IS DISTINCT FROM
        attempt_row.bucket_identity) OR
     (NEW.rpc_endpoint IS NOT NULL AND NEW.rpc_endpoint IS DISTINCT FROM
        attempt_row.endpoint) OR
     (NEW.outcome = 'verified' AND (
       NEW.observed_root_set_sha256 IS DISTINCT FROM artifact_row.root_set_sha256 OR
       (NEW.observed_root_count IS NOT NULL AND
         NEW.observed_root_count IS DISTINCT FROM artifact_row.root_count) OR
       (NEW.final_recursive_pin_status IS NOT NULL AND
         NEW.final_recursive_pin_status IS DISTINCT FROM 'pinned')
     )) OR (NEW.outcome <> 'verified' AND (
       NEW.observed_root_count IS NOT NULL OR
       NEW.final_recursive_pin_status IS NOT NULL
     )) THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR provider outcome is not exact';
  END IF;
  NEW.bucket_identity := attempt_row.bucket_identity;
  NEW.rpc_endpoint := attempt_row.endpoint;
  IF NEW.outcome = 'verified' THEN
    NEW.observed_root_count := artifact_row.root_count;
    NEW.final_recursive_pin_status := 'pinned';
  ELSE
    NEW.observed_root_count := NULL;
    NEW.final_recursive_pin_status := NULL;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_outcome_042_binding_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_car_import_attempt_outcomes
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_outcome_binding_042();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_gateway_evidence_insert_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_row oracle_candidate_source_snapshot_car_import_attempts%ROWTYPE;
  outcome_row
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  inspection_row
    oracle_candidate_source_snapshot_car_import_inspections%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  expected_payload jsonb;
  expected_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE car_import_attempt_id = NEW.car_import_attempt_id;
  SELECT * INTO STRICT outcome_row
  FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
  WHERE car_import_outcome_id = NEW.car_import_outcome_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  IF NEW.car_import_inspection_id IS NOT NULL THEN
    SELECT * INTO STRICT inspection_row
    FROM oracle_candidate_source_snapshot_car_import_inspections
    WHERE car_import_inspection_id = NEW.car_import_inspection_id;
  END IF;
  expected_payload := jsonb_build_object(
    'artifactId', NEW.car_artifact_id,
    'bucketIdentity', artifact_row.bucket_identity,
    'gatewayOrigin', NEW.gateway_origin,
    'gatewayPathPolicy', NEW.gateway_path_policy,
    'implementationCommitSha', attempt_row.implementation_commit_sha,
    'inspectionId', NEW.car_import_inspection_id,
    'inspectionSha256', NEW.inspection_sha256,
    'observedAt', to_char(NEW.observed_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'outcomeId', NEW.car_import_outcome_id,
    'outcomeSha256', outcome_row.outcome_sha256,
    'planId', artifact_row.plan_id,
    'providerHttpStatus', NEW.provider_http_status,
    'providerProofPath', NEW.provider_proof_path,
    'providerRequestIdHash', NEW.provider_request_id_hash,
    'rootBlockBytes', NEW.root_block_bytes,
    'rootBlockSha256', NEW.root_block_sha256,
    'rootCid', artifact_row.primary_root_cid,
    'rpcEndpoint', artifact_row.rpc_endpoint,
    'schemaVersion', NEW.evidence_version,
    'validationResult', NEW.validation_result
  );
  expected_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocargateway_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.evidence_version, artifact_row.plan_id, NEW.car_artifact_id,
      NEW.car_import_outcome_id, expected_sha256,
      coalesce(NEW.car_import_inspection_id, '')
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF outcome_row.car_import_attempt_id IS DISTINCT FROM
       NEW.car_import_attempt_id OR
     outcome_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     outcome_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     NOT (
       (NEW.provider_proof_path = 'verified_outcome' AND
        NEW.car_import_inspection_id IS NULL AND
        NEW.inspection_sha256 IS NULL AND
        outcome_row.outcome = 'verified' AND
        outcome_row.observed_root_count = artifact_row.root_count AND
        outcome_row.observed_root_set_sha256 = artifact_row.root_set_sha256 AND
        outcome_row.final_recursive_pin_status = 'pinned') OR
       (NEW.provider_proof_path = 'positive_inspection' AND
        outcome_row.outcome = 'outcome_unknown' AND
        outcome_row.observed_root_count IS NULL AND
        outcome_row.observed_root_set_sha256 IS NULL AND
        outcome_row.final_recursive_pin_status IS NULL AND
        inspection_row.car_import_inspection_id IS NOT NULL AND
        inspection_row.car_import_attempt_id = NEW.car_import_attempt_id AND
        inspection_row.car_import_outcome_id = NEW.car_import_outcome_id AND
        inspection_row.car_artifact_id = NEW.car_artifact_id AND
        inspection_row.plan_id = NEW.plan_id AND
        inspection_row.inspection_sha256 = NEW.inspection_sha256 AND
        inspection_row.inspection_result = 'present_exact' AND
        inspection_row.root_status = 'present_exact' AND
        inspection_row.pin_status = 'pinned' AND
        inspection_row.observed_root_set_sha256 = artifact_row.root_set_sha256)
     ) OR
     NEW.bucket_identity IS DISTINCT FROM artifact_row.bucket_identity OR
     NEW.rpc_endpoint IS DISTINCT FROM artifact_row.rpc_endpoint OR
     NEW.root_cid IS DISTINCT FROM artifact_row.primary_root_cid OR
     NEW.implementation_commit_sha IS DISTINCT FROM
       attempt_row.implementation_commit_sha OR
     NEW.observed_at < outcome_row.recorded_at OR
     (NEW.provider_proof_path = 'positive_inspection' AND
       NEW.observed_at < inspection_row.inspected_at) OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_plans plan
       WHERE plan.plan_id = NEW.plan_id AND plan.state = 'executing'
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) OR
     NEW.evidence_payload IS DISTINCT FROM expected_payload OR
     NEW.evidence_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.gateway_evidence_id IS DISTINCT FROM expected_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR gateway evidence is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_gateway_evidence_042_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_gateway_evidence
  FOR EACH ROW
  EXECUTE FUNCTION oracle_guard_css_car_gateway_evidence_insert_042();

CREATE TRIGGER oracle_css_car_gateway_evidence_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_car_gateway_evidence
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

-- Receipt values are derived from the immutable attempt/outcome and gateway
-- evidence rows. A caller cannot promote status strings or hashes into proof.
CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  authorization_row
    oracle_candidate_source_snapshot_car_import_authorizations%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  outcome_row
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  gateway_row oracle_candidate_source_snapshot_car_gateway_evidence%ROWTYPE;
  artifact_attempt_count integer;
  artifact_reserved_request_count integer;
  artifact_attempt_cost numeric(18, 12);
  total_attempt_count integer;
  total_reserved_request_count integer;
  total_attempt_cost numeric(18, 12);
  expected_provider_request_id_hash text;
  expected_provider_evidence_sha256 text;
  expected_payload jsonb;
  expected_receipt_sha256 text;
  expected_receipt_id text;
BEGIN
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_car_import_authorizations
  WHERE car_authorization_id = NEW.car_authorization_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT outcome_row
  FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
  WHERE car_import_outcome_id = NEW.provider_outcome_id;
  SELECT * INTO STRICT gateway_row
  FROM oracle_candidate_source_snapshot_car_gateway_evidence
  WHERE gateway_evidence_id = NEW.gateway_evidence_id;
  IF gateway_row.provider_proof_path = 'positive_inspection' THEN
    SELECT inspection.provider_request_id_hash
    INTO STRICT expected_provider_request_id_hash
    FROM oracle_candidate_source_snapshot_car_import_inspections inspection
    WHERE inspection.car_import_inspection_id =
      gateway_row.car_import_inspection_id;
  ELSE
    expected_provider_request_id_hash := outcome_row.provider_request_id_hash;
  END IF;
  SELECT count(*)::integer,
         coalesce(sum(reserved_request_count), 0)::integer,
         coalesce(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO artifact_attempt_count, artifact_reserved_request_count,
    artifact_attempt_cost
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE car_artifact_id = NEW.car_artifact_id AND plan_id = NEW.plan_id;
  SELECT count(*)::integer,
         coalesce(sum(reserved_request_count), 0)::integer,
         coalesce(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO total_attempt_count, total_reserved_request_count, total_attempt_cost
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE plan_id = NEW.plan_id;
  expected_provider_evidence_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'gatewayEvidenceSha256', gateway_row.evidence_sha256,
      'providerInspectionSha256', gateway_row.inspection_sha256,
      'providerOutcomeSha256', outcome_row.outcome_sha256,
      'providerProofPath', gateway_row.provider_proof_path,
      'schemaVersion',
        'candidate-source-snapshot-car-provider-evidence-set-v1'
    )), 'UTF8'
  )), 'hex');
  expected_payload := jsonb_build_object(
    'authorizationId', NEW.car_authorization_id,
    'bucketIdentity', artifact_row.bucket_identity,
    'bucketTokenSha256', NEW.bucket_token_sha256,
    'carArtifactId', NEW.car_artifact_id,
    'carBytes', artifact_row.car_bytes,
    'carSha256', artifact_row.car_sha256,
    'finalRecursivePinStatus', 'pinned',
    'gatewayEvidenceId', gateway_row.gateway_evidence_id,
    'gatewayEvidenceSha256', gateway_row.evidence_sha256,
    'implementationCommitSha', NEW.implementation_commit_sha,
    'overallTimeoutMs', NEW.overall_timeout_ms,
    'memberCount', artifact_row.member_count,
    'memberLogicalBytes', artifact_row.member_logical_bytes,
    'memberSetSha256', artifact_row.member_set_sha256,
    'officialGatewayStatus', 'verified',
    'planId', artifact_row.plan_id,
    'planSha256', artifact_row.plan_sha256,
    'primaryRootCid', artifact_row.primary_root_cid,
    'providerImportResult', 'expected_root_set_returned',
    'importAttemptCount', artifact_attempt_count,
    'providerInspectionId', gateway_row.car_import_inspection_id,
    'providerInspectionSha256', gateway_row.inspection_sha256,
    'providerEvidenceSetSha256', expected_provider_evidence_sha256,
    'providerOutcomeId', outcome_row.car_import_outcome_id,
    'providerOutcomeSha256', outcome_row.outcome_sha256,
    'providerPinIdHash', NULL,
    'providerProofPath', gateway_row.provider_proof_path,
    'providerRequestIdHash', expected_provider_request_id_hash,
    'requestCostUsd', artifact_attempt_cost::double precision,
    'reservedRequestCount', artifact_reserved_request_count,
    'rootBlockValidation', 'cid_verified',
    'rootCount', artifact_row.root_count,
    'rootObservationSetSha256', artifact_row.root_set_sha256,
    'rootSetSha256', artifact_row.root_set_sha256,
    'rpcEndpoint', artifact_row.rpc_endpoint,
    'schemaVersion', NEW.receipt_version,
    'verificationMethod', NEW.verification_method,
    'verificationTimestamp', to_char(
      NEW.verification_timestamp AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
  expected_receipt_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_receipt_id := 'snapshotdemocarreceipt_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.receipt_version, artifact_row.plan_id, NEW.car_artifact_id,
      expected_receipt_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  IF authorization_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     authorization_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     authorization_row.implementation_commit_sha IS DISTINCT FROM
       NEW.implementation_commit_sha OR
     authorization_row.endpoint IS DISTINCT FROM artifact_row.rpc_endpoint OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     artifact_row.car_sha256 IS DISTINCT FROM NEW.car_sha256 OR
     artifact_row.car_bytes IS DISTINCT FROM NEW.car_bytes OR
     artifact_row.primary_root_cid IS DISTINCT FROM NEW.primary_root_cid OR
     artifact_row.root_count IS DISTINCT FROM NEW.root_count OR
     artifact_row.root_set_sha256 IS DISTINCT FROM NEW.root_set_sha256 OR
     artifact_row.root_count IS DISTINCT FROM (
       SELECT count(*)::integer
       FROM oracle_candidate_source_snapshot_car_roots root
       WHERE root.car_artifact_id = artifact_row.car_artifact_id
     ) OR
     artifact_row.root_set_sha256 IS DISTINCT FROM
       oracle_css_car_root_set_sha256(artifact_row.car_artifact_id) OR
     artifact_row.member_count IS DISTINCT FROM NEW.member_count OR
     artifact_row.member_logical_bytes IS DISTINCT FROM
       NEW.member_logical_bytes OR
     artifact_row.member_set_sha256 IS DISTINCT FROM NEW.member_set_sha256 OR
     artifact_row.member_count IS DISTINCT FROM (
       SELECT count(*)::integer
       FROM oracle_candidate_source_snapshot_car_members member
       WHERE member.car_artifact_id = artifact_row.car_artifact_id
     ) OR
     artifact_row.member_logical_bytes IS DISTINCT FROM (
       SELECT coalesce(sum(member.expected_bytes), 0)::bigint
       FROM oracle_candidate_source_snapshot_car_members member
       WHERE member.car_artifact_id = artifact_row.car_artifact_id
     ) OR
     artifact_row.member_set_sha256 IS DISTINCT FROM
       oracle_css_car_member_set_sha256(artifact_row.car_artifact_id) OR
     artifact_row.bucket_identity IS DISTINCT FROM (CASE artifact_row.car_role
       WHEN 'open_data' THEN authorization_row.open_data_bucket_identity
       ELSE authorization_row.query_table_bucket_identity
     END) OR
     NEW.bucket_token_sha256 IS DISTINCT FROM (CASE artifact_row.car_role
       WHEN 'open_data' THEN authorization_row.open_data_bucket_token_sha256
       ELSE authorization_row.query_table_bucket_token_sha256
     END) OR
     NEW.overall_timeout_ms IS DISTINCT FROM
       authorization_row.overall_timeout_ms OR
     NEW.receipt_version IS DISTINCT FROM
       'candidate-source-snapshot-car-import-receipt-v1' OR
     NEW.verification_method IS DISTINCT FROM
       'car_import_recursively_pinned' OR
     NEW.provider_import_result IS DISTINCT FROM
       'expected_root_set_returned' OR
     NEW.final_recursive_pin_status IS DISTINCT FROM 'pinned' OR
     NEW.official_gateway_status IS DISTINCT FROM 'verified' OR
     NEW.root_block_validation IS DISTINCT FROM 'cid_verified' OR
     NEW.root_observation_set_sha256 IS DISTINCT FROM
       artifact_row.root_set_sha256 OR
     artifact_attempt_count IS DISTINCT FROM NEW.import_attempt_count OR
     artifact_attempt_count > authorization_row.maximum_attempts_per_artifact OR
     total_attempt_count > authorization_row.maximum_total_import_attempts OR
     artifact_attempt_cost IS DISTINCT FROM NEW.request_cost_usd OR
     artifact_reserved_request_count IS DISTINCT FROM
       NEW.reserved_request_count OR
     accounting_row.request_count + total_reserved_request_count >
       plan_row.maximum_request_count OR
     accounting_row.request_cost_usd + total_attempt_cost >
       authorization_row.hard_spending_ceiling_usd OR
     outcome_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     outcome_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     NOT (
       (gateway_row.provider_proof_path = 'verified_outcome' AND
        gateway_row.car_import_inspection_id IS NULL AND
        gateway_row.inspection_sha256 IS NULL AND
        outcome_row.outcome = 'verified' AND
        outcome_row.observed_root_count = artifact_row.root_count AND
        outcome_row.observed_root_set_sha256 = artifact_row.root_set_sha256 AND
        outcome_row.final_recursive_pin_status = 'pinned') OR
       (gateway_row.provider_proof_path = 'positive_inspection' AND
        outcome_row.outcome = 'outcome_unknown' AND EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_car_import_inspections inspection
          WHERE inspection.car_import_inspection_id =
                  gateway_row.car_import_inspection_id
            AND inspection.car_import_attempt_id =
                  outcome_row.car_import_attempt_id
            AND inspection.car_import_outcome_id =
                  outcome_row.car_import_outcome_id
            AND inspection.car_artifact_id = artifact_row.car_artifact_id
            AND inspection.plan_id = artifact_row.plan_id
            AND inspection.inspection_sha256 = gateway_row.inspection_sha256
            AND inspection.inspection_result = 'present_exact'
            AND inspection.root_status = 'present_exact'
            AND inspection.pin_status = 'pinned'
            AND inspection.observed_root_set_sha256 = artifact_row.root_set_sha256
        ))
     ) OR
     gateway_row.car_import_outcome_id IS DISTINCT FROM
       outcome_row.car_import_outcome_id OR
     gateway_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     gateway_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     gateway_row.bucket_identity IS DISTINCT FROM artifact_row.bucket_identity OR
     gateway_row.rpc_endpoint IS DISTINCT FROM artifact_row.rpc_endpoint OR
     gateway_row.root_cid IS DISTINCT FROM artifact_row.primary_root_cid OR
     gateway_row.validation_result IS DISTINCT FROM 'cid_verified' OR
     NEW.provider_outcome_id IS DISTINCT FROM
       outcome_row.car_import_outcome_id OR
     NEW.gateway_evidence_id IS DISTINCT FROM gateway_row.gateway_evidence_id OR
     NEW.bucket_identity IS DISTINCT FROM artifact_row.bucket_identity OR
     NEW.rpc_endpoint IS DISTINCT FROM artifact_row.rpc_endpoint OR
     NEW.provider_proof_path IS DISTINCT FROM
       gateway_row.provider_proof_path OR
     NEW.provider_inspection_id IS DISTINCT FROM
       gateway_row.car_import_inspection_id OR
     NEW.provider_inspection_sha256 IS DISTINCT FROM
       gateway_row.inspection_sha256 OR
     NEW.provider_evidence_set_sha256 IS DISTINCT FROM
       expected_provider_evidence_sha256 OR
     NEW.provider_request_id_hash IS DISTINCT FROM
       expected_provider_request_id_hash OR
     NEW.provider_pin_id_hash IS NOT NULL OR
     NEW.root_observation_set_sha256 IS DISTINCT FROM
       artifact_row.root_set_sha256 OR
     NEW.verification_timestamp < gateway_row.observed_at OR
     NEW.receipt_payload IS DISTINCT FROM expected_payload OR
     NEW.receipt_sha256 IS DISTINCT FROM expected_receipt_sha256 OR
     NEW.car_import_receipt_id IS DISTINCT FROM expected_receipt_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR import receipt is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_admission()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_state text;
BEGIN
  SELECT state INTO STRICT plan_state
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_state IS DISTINCT FROM 'executing' OR NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations auth
       JOIN oracle_candidate_source_snapshot_car_artifacts artifact
         ON artifact.plan_id = auth.plan_id
        AND artifact.car_artifact_id = NEW.car_artifact_id
       JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
         ON outcome.car_import_outcome_id = NEW.provider_outcome_id
        AND outcome.car_artifact_id = artifact.car_artifact_id
        AND outcome.plan_id = artifact.plan_id
       JOIN oracle_candidate_source_snapshot_car_import_attempts attempt
         ON attempt.car_import_attempt_id = outcome.car_import_attempt_id
        AND attempt.car_artifact_id = artifact.car_artifact_id
        AND attempt.plan_id = artifact.plan_id
       JOIN oracle_candidate_source_snapshot_car_gateway_evidence gateway
         ON gateway.gateway_evidence_id = NEW.gateway_evidence_id
        AND gateway.car_import_outcome_id = outcome.car_import_outcome_id
        AND gateway.car_artifact_id = artifact.car_artifact_id
        AND gateway.plan_id = artifact.plan_id
       WHERE auth.car_authorization_id = NEW.car_authorization_id
         AND auth.plan_id = NEW.plan_id
         AND auth.plan_sha256 = NEW.plan_sha256
         AND auth.implementation_commit_sha = NEW.implementation_commit_sha
         AND auth.endpoint = artifact.rpc_endpoint
         AND artifact.bucket_identity = NEW.bucket_identity
         AND NEW.bucket_token_sha256 = CASE artifact.car_role
           WHEN 'open_data' THEN auth.open_data_bucket_token_sha256
           ELSE auth.query_table_bucket_token_sha256
         END
         AND NEW.overall_timeout_ms = auth.overall_timeout_ms
         AND attempt.bucket_token_sha256 = NEW.bucket_token_sha256
         AND attempt.overall_timeout_ms = NEW.overall_timeout_ms
         AND NEW.reserved_request_count = NEW.import_attempt_count * 5
         AND artifact.rpc_endpoint = NEW.rpc_endpoint
         AND (
           (gateway.provider_proof_path = 'verified_outcome'
            AND outcome.outcome = 'verified'
            AND outcome.observed_root_count = artifact.root_count
            AND outcome.observed_root_set_sha256 = artifact.root_set_sha256
            AND outcome.final_recursive_pin_status = 'pinned'
            AND gateway.car_import_inspection_id IS NULL) OR
           (gateway.provider_proof_path = 'positive_inspection'
            AND outcome.outcome = 'outcome_unknown'
            AND EXISTS (
              SELECT 1
              FROM oracle_candidate_source_snapshot_car_import_inspections inspection
              WHERE inspection.car_import_inspection_id =
                      gateway.car_import_inspection_id
                AND inspection.car_import_attempt_id =
                      outcome.car_import_attempt_id
                AND inspection.car_import_outcome_id =
                      outcome.car_import_outcome_id
                AND inspection.car_artifact_id = artifact.car_artifact_id
                AND inspection.plan_id = artifact.plan_id
                AND inspection.inspection_sha256 = gateway.inspection_sha256
                AND inspection.inspection_result = 'present_exact'
                AND inspection.root_status = 'present_exact'
                AND inspection.pin_status = 'pinned'
                AND inspection.observed_root_set_sha256 = artifact.root_set_sha256
            ))
         )
         AND gateway.bucket_identity = artifact.bucket_identity
         AND gateway.rpc_endpoint = artifact.rpc_endpoint
         AND gateway.root_cid = artifact.primary_root_cid
         AND gateway.validation_result = 'cid_verified'
         AND NEW.provider_proof_path = gateway.provider_proof_path
         AND NEW.provider_inspection_id IS NOT DISTINCT FROM
           gateway.car_import_inspection_id
         AND NEW.provider_inspection_sha256 IS NOT DISTINCT FROM
           gateway.inspection_sha256
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR receipt is not admissible';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_attempts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_count integer;
  outcome_count integer;
  final_attempt_sequence integer;
  final_outcome
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  unresolved_unknown_count integer;
BEGIN
  SELECT count(*)::integer, max(attempt_sequence)
  INTO attempt_count, final_attempt_sequence
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id;
  SELECT count(*)::integer INTO outcome_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id;
  SELECT outcome.* INTO final_outcome
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id
    AND attempt.attempt_sequence = final_attempt_sequence;
  SELECT count(*)::integer INTO unresolved_unknown_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id
    AND outcome.outcome = 'outcome_unknown'
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_car_import_inspections inspection
      WHERE inspection.car_import_attempt_id = attempt.car_import_attempt_id
        AND inspection.car_import_outcome_id = outcome.car_import_outcome_id
        AND inspection.inspection_result IN (
          'conclusively_absent', 'present_exact'
        )
    );
  IF attempt_count IS DISTINCT FROM NEW.import_attempt_count OR
     outcome_count IS DISTINCT FROM attempt_count OR
     unresolved_unknown_count IS DISTINCT FROM 0 OR
     final_outcome.car_import_outcome_id IS DISTINCT FROM
       NEW.provider_outcome_id OR
     NOT (
       (NEW.provider_proof_path = 'verified_outcome' AND
        final_outcome.outcome = 'verified' AND
        final_outcome.observed_root_set_sha256 = NEW.root_set_sha256 AND
        final_outcome.final_recursive_pin_status = 'pinned') OR
       (NEW.provider_proof_path = 'positive_inspection' AND
        final_outcome.outcome = 'outcome_unknown' AND EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_car_import_inspections inspection
          WHERE inspection.car_import_inspection_id =
                  NEW.provider_inspection_id
            AND inspection.car_import_attempt_id =
                  final_outcome.car_import_attempt_id
            AND inspection.car_import_outcome_id =
                  final_outcome.car_import_outcome_id
            AND inspection.car_artifact_id = NEW.car_artifact_id
            AND inspection.plan_id = NEW.plan_id
            AND inspection.inspection_sha256 =
                  NEW.provider_inspection_sha256
            AND inspection.inspection_result = 'present_exact'
            AND inspection.root_status = 'present_exact'
            AND inspection.pin_status = 'pinned'
            AND inspection.observed_root_set_sha256 = NEW.root_set_sha256
        ))
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR receipt lacks exact attempts';
  END IF;
  RETURN NEW;
END;
$$;

-- Preserve the full closure invariant from migration 039 while charging the
-- five durably reserved remote operations for each CAR attempt, covering the
-- normal import/pin/gateway path plus a possible pin/gateway inspection.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_upload_closure()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  actual_count bigint;
  actual_bytes bigint;
  unresolved_count bigint;
  mismatch_count bigint;
  unjournaled_count bigint;
  car_request_count integer;
  car_request_cost numeric(18, 12);
  total_request_count integer;
  total_request_cost numeric(18, 12);
  expected_payload jsonb;
  expected_closure_sha256 text;
  expected_closure_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*), coalesce(sum(expected_bytes), 0),
         count(*) FILTER (WHERE status <> 'verified'),
         count(*) FILTER (
           WHERE status = 'verified' AND provider_cid IS DISTINCT FROM expected_cid
         )
  INTO actual_count, actual_bytes, unresolved_count, mismatch_count
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id;
  SELECT count(*) INTO unjournaled_count
  FROM oracle_candidate_source_snapshot_demo_objects object
  WHERE object.plan_id = NEW.plan_id
    AND object.status = 'verified'
    AND NOT (
      EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
        JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.request_id = attempt.request_id
        WHERE attempt.plan_id = object.plan_id
          AND attempt.domain = object.domain
          AND attempt.remote_object_key = object.remote_object_key
          AND attempt.outcome = 'verified'
          AND attempt.provider_cid = object.expected_cid
          AND attempt.receipt_sha256 = object.receipt_sha256
          AND request.plan_id = object.plan_id
          AND request.operation_kind = 'put_object'
          AND request.outcome = 'succeeded'
          AND request.receipt_sha256 = object.receipt_sha256
      ) OR EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_inspections inspection
        JOIN oracle_candidate_source_snapshot_demo_inspection_attempts inspection_attempt
          ON inspection_attempt.inspection_id = inspection.inspection_id
        JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.request_id = inspection.request_id
        JOIN oracle_candidate_source_snapshot_demo_upload_attempts recovery_attempt
          ON recovery_attempt.attempt_id =
             inspection_attempt.recovery_upload_attempt_id
        WHERE inspection.plan_id = object.plan_id
          AND inspection.domain = object.domain
          AND inspection.remote_object_key = object.remote_object_key
          AND inspection.outcome = 'verified'
          AND inspection.observed_cid = object.expected_cid
          AND inspection.observed_sha256 = object.expected_sha256
          AND inspection.observed_bytes = object.expected_bytes
          AND inspection.receipt_sha256 = object.receipt_sha256
          AND inspection_attempt.outcome = 'verified'
          AND request.operation_kind = 'inspect_object'
          AND request.outcome = 'succeeded'
          AND request.receipt_sha256 = object.receipt_sha256
          AND recovery_attempt.outcome IN (
            'connection_failure', 'retryable_http_error', 'timeout_unknown'
          )
      ) OR EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_car_members member
        JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
          ON receipt.car_artifact_id = member.car_artifact_id
         AND receipt.plan_id = member.plan_id
        JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
          ON bulk.car_import_receipt_id = receipt.car_import_receipt_id
         AND bulk.car_artifact_id = member.car_artifact_id
         AND bulk.plan_id = member.plan_id
        WHERE member.plan_id = object.plan_id
          AND member.domain = object.domain
          AND member.remote_object_key = object.remote_object_key
          AND member.expected_sha256 = object.expected_sha256
          AND member.expected_cid = object.expected_cid
          AND member.expected_bytes = object.expected_bytes
          AND object.car_verification_method = 'car_import_recursively_pinned'
          AND object.car_artifact_id = member.car_artifact_id
          AND object.car_import_receipt_id = receipt.car_import_receipt_id
          AND object.receipt_sha256 = receipt.receipt_sha256
          AND receipt.verification_method = 'car_import_recursively_pinned'
          AND bulk.verification_method = 'car_import_recursively_pinned'
      )
    );
  SELECT coalesce(sum(receipt.reserved_request_count), 0)::integer,
         coalesce(sum(receipt.request_cost_usd), 0)::numeric(18, 12)
  INTO car_request_count, car_request_cost
  FROM oracle_candidate_source_snapshot_car_import_receipts receipt
  WHERE receipt.plan_id = NEW.plan_id;
  total_request_count := accounting_row.request_count + car_request_count;
  total_request_cost := accounting_row.request_cost_usd + car_request_cost;
  expected_payload := jsonb_build_object(
    'admittedRequestCostUsd', total_request_cost::double precision,
    'admittedRequestCount', total_request_count,
    'approvalId', NEW.approval_id,
    'exactObjectCount', actual_count,
    'exactTotalBytes', actual_bytes,
    'inventoryRootCid', plan_row.inventory_root_cid,
    'inventoryRootSha256', plan_row.inventory_root_sha256,
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'providerCidMismatchCount', mismatch_count,
    'unresolvedObjectCount', unresolved_count,
    'verifiedAt', to_char(NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  );
  expected_closure_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_closure_id := 'snapshotdemouploadclosure_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-closure-v1',
      plan_row.plan_id,
      expected_closure_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  IF plan_row.state IS DISTINCT FROM 'executing' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.exact_object_count IS DISTINCT FROM plan_row.exact_upload_object_count OR
     NEW.exact_total_bytes IS DISTINCT FROM plan_row.exact_upload_bytes OR
     NEW.verified_object_count IS DISTINCT FROM actual_count::integer OR
     NEW.verified_total_bytes IS DISTINCT FROM actual_bytes OR
     actual_count IS DISTINCT FROM plan_row.exact_upload_object_count::bigint OR
     actual_bytes IS DISTINCT FROM plan_row.exact_upload_bytes OR
     unresolved_count IS DISTINCT FROM 0::bigint OR
     mismatch_count IS DISTINCT FROM 0::bigint OR
     unjournaled_count IS DISTINCT FROM 0::bigint OR
     NEW.unresolved_object_count IS DISTINCT FROM 0 OR
     NEW.provider_cid_mismatch_count IS DISTINCT FROM 0 OR
     NEW.inventory_root_cid IS DISTINCT FROM plan_row.inventory_root_cid OR
     NEW.inventory_root_sha256 IS DISTINCT FROM plan_row.inventory_root_sha256 OR
     NEW.admitted_request_count IS DISTINCT FROM total_request_count OR
     NEW.admitted_request_cost_usd IS DISTINCT FROM total_request_cost OR
     NEW.closure_sha256 IS DISTINCT FROM expected_closure_sha256 OR
     NEW.closure_id IS DISTINCT FROM expected_closure_id OR
     total_request_count > plan_row.maximum_request_count OR
     total_request_cost > plan_row.budget_limit_usd OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.approval_id = NEW.approval_id
         AND approval.plan_id = NEW.plan_id
         AND approval.plan_sha256 = NEW.plan_sha256
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.operation_kind IN ('put_object', 'inspect_object')
         AND request.outcome = 'request_started'
         AND NOT EXISTS (
           SELECT 1
           FROM oracle_candidate_source_snapshot_demo_objects object
           JOIN oracle_candidate_source_snapshot_car_members member
             ON member.plan_id = object.plan_id
            AND member.domain = object.domain
            AND member.remote_object_key = object.remote_object_key
           JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
             ON bulk.car_artifact_id = object.car_artifact_id
            AND bulk.car_import_receipt_id = object.car_import_receipt_id
            AND bulk.plan_id = object.plan_id
           WHERE object.plan_id = request.plan_id
             AND object.domain = request.domain
             AND object.remote_object_key = request.remote_object_key
             AND object.status = 'verified'
             AND object.car_verification_method =
               'car_import_recursively_pinned'
             AND member.expected_sha256 = object.expected_sha256
             AND member.expected_cid = object.expected_cid
             AND member.expected_bytes = object.expected_bytes
         )
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot upload closure is not exact and complete';
  END IF;
  RETURN NEW;
END;
$$;

-- Closure counts admitted CAR requests directly, rather than only successful
-- receipts, and rejects every unclassified or unreconciled request.
CREATE OR REPLACE FUNCTION oracle_guard_css_car_closure_attempts_042()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  artifact_count integer;
  receipt_count integer;
  attempt_count integer;
  receipt_attempt_count integer;
  attempt_reserved_request_count integer;
  receipt_reserved_request_count integer;
  attempt_cost numeric(18, 12);
  receipt_attempt_cost numeric(18, 12);
  dangling_count integer;
  unresolved_unknown_count integer;
BEGIN
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*)::integer INTO artifact_count
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE plan_id = NEW.plan_id;
  SELECT count(*)::integer, coalesce(sum(import_attempt_count), 0)::integer,
         coalesce(sum(reserved_request_count), 0)::integer,
         coalesce(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO receipt_count, receipt_attempt_count, receipt_reserved_request_count,
    receipt_attempt_cost
  FROM oracle_candidate_source_snapshot_car_import_receipts
  WHERE plan_id = NEW.plan_id;
  SELECT count(*)::integer,
         coalesce(sum(reserved_request_count), 0)::integer,
         coalesce(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO attempt_count, attempt_reserved_request_count, attempt_cost
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE plan_id = NEW.plan_id;
  SELECT count(*)::integer INTO dangling_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  LEFT JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.plan_id = NEW.plan_id
    AND outcome.car_import_outcome_id IS NULL;
  SELECT count(*)::integer INTO unresolved_unknown_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.plan_id = NEW.plan_id AND outcome.outcome = 'outcome_unknown'
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_car_import_inspections inspection
      WHERE inspection.car_import_attempt_id = attempt.car_import_attempt_id
        AND inspection.car_import_outcome_id = outcome.car_import_outcome_id
        AND inspection.inspection_result IN (
          'conclusively_absent', 'present_exact'
        )
    );
  IF artifact_count IS DISTINCT FROM 2 OR receipt_count IS DISTINCT FROM 2 OR
     attempt_count IS DISTINCT FROM receipt_attempt_count OR
     attempt_reserved_request_count IS DISTINCT FROM
       receipt_reserved_request_count OR
     attempt_cost IS DISTINCT FROM receipt_attempt_cost OR
     dangling_count IS DISTINCT FROM 0 OR
     unresolved_unknown_count IS DISTINCT FROM 0 OR
     NEW.admitted_request_count IS DISTINCT FROM
       accounting_row.request_count + attempt_reserved_request_count OR
     NEW.admitted_request_cost_usd IS DISTINCT FROM
       accounting_row.request_cost_usd + attempt_cost THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload closure has unresolved CAR requests';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_closure_042_attempt_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_upload_closures
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_closure_attempts_042();

COMMIT;
