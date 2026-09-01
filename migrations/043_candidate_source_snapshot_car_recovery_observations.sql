BEGIN;

-- Preserve every CAR observation while permitting the bounded recovery window
-- that follows an initially unavailable inspection. The exact observation
-- payload remains immutable and content addressed; only the two historical
-- one-observation uniqueness constraints are relaxed.
DO $$
DECLARE
  constraint_row record;
BEGIN
  FOR constraint_row IN
    SELECT constraint_entry.conname
    FROM pg_constraint constraint_entry
    WHERE constraint_entry.conrelid =
      'oracle_candidate_source_snapshot_car_import_inspections'::regclass
      AND constraint_entry.contype = 'u'
      AND pg_get_constraintdef(constraint_entry.oid) IN (
        'UNIQUE (car_import_attempt_id)',
        'UNIQUE (car_import_outcome_id)'
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_car_import_inspections DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_import_inspection_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  outcome_row
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  attempt_row oracle_candidate_source_snapshot_car_import_attempts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_state text;
  prior_inspection_count integer;
  latest_inspected_at timestamptz;
  expected_payload jsonb;
  expected_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT outcome_row
  FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
  WHERE car_import_outcome_id = NEW.car_import_outcome_id;
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE car_import_attempt_id = NEW.car_import_attempt_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT state INTO STRICT plan_state
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*)::integer, max(inspection.inspected_at)
  INTO prior_inspection_count, latest_inspected_at
  FROM oracle_candidate_source_snapshot_car_import_inspections inspection
  WHERE inspection.car_import_attempt_id = NEW.car_import_attempt_id
    AND inspection.car_import_outcome_id = NEW.car_import_outcome_id;
  expected_payload := jsonb_build_object(
    'artifactId', NEW.car_artifact_id,
    'attemptId', NEW.car_import_attempt_id,
    'implementationCommitSha', NEW.implementation_commit_sha,
    'inspectedAt', to_char(NEW.inspected_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'inspectionResult', NEW.inspection_result,
    'observedRootSetSha256', NEW.observed_root_set_sha256,
    'outcomeId', NEW.car_import_outcome_id,
    'pinStatus', NEW.pin_status,
    'planId', NEW.plan_id,
    'providerEvidenceSha256', NEW.provider_evidence_sha256,
    'providerHttpStatus', NEW.provider_http_status,
    'providerRequestIdHash', NEW.provider_request_id_hash,
    'providerResponseBytes', NEW.provider_response_bytes,
    'rootSetSha256', NEW.root_set_sha256,
    'rootStatus', NEW.root_status,
    'schemaVersion', NEW.inspection_version
  );
  expected_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocarinspection_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.inspection_version, NEW.car_import_attempt_id,
      NEW.car_import_outcome_id, expected_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF plan_state IS DISTINCT FROM 'executing' OR
     outcome_row.outcome IS DISTINCT FROM 'outcome_unknown' OR
     outcome_row.car_import_attempt_id IS DISTINCT FROM
       NEW.car_import_attempt_id OR
     outcome_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     outcome_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     attempt_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     attempt_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     NEW.root_set_sha256 IS DISTINCT FROM artifact_row.root_set_sha256 OR
     (NEW.inspection_result = 'present_exact' AND
       NEW.observed_root_set_sha256 IS DISTINCT FROM artifact_row.root_set_sha256) OR
     (NEW.inspection_result = 'present_unexpected' AND
       NEW.observed_root_set_sha256 IS NOT DISTINCT FROM artifact_row.root_set_sha256) OR
     NEW.implementation_commit_sha IS DISTINCT FROM
       attempt_row.implementation_commit_sha OR
     NEW.inspected_at < outcome_row.recorded_at OR
     prior_inspection_count >= 4 OR
     (latest_inspected_at IS NOT NULL AND
       NEW.inspected_at <= latest_inspected_at) OR
     NEW.inspection_payload IS DISTINCT FROM expected_payload OR
     NEW.inspection_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.car_import_inspection_id IS DISTINCT FROM expected_id OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR import inspection is not exact';
  END IF;
  RETURN NEW;
END;
$$;

COMMIT;
