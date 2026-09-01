BEGIN;

-- Additive repair for the locally-applied migration 040. The repair is valid
-- only before any CAR artifact, authorization, request, receipt, or bulk
-- verification exists and performs no remote operation.

DO $$
BEGIN
  IF EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_artifacts
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_attempts
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_inspections
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_import_receipts
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_bulk_verifications
     ) THEN
    RAISE EXCEPTION
      'CAR attempt drift repair requires an empty CAR evidence ledger';
  END IF;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_car_import_attempt_outcomes
  DROP CONSTRAINT
    oracle_candidate_source_snapshot_car_impo_provider_status_check,
  DROP CONSTRAINT
    oracle_candidate_source_snapshot_car_import_attempt_outco_check,
  ADD CONSTRAINT oracle_css_car_attempt_outcome_provider_status_check CHECK (
    provider_status IN (
      'accepted', 'caller_aborted', 'provider_pin_error', 'provider_rejected',
      'provider_result_invalid', 'provider_root_mismatch',
      'provider_retryable_status', 'redirect_rejected', 'response_too_large',
      'stream_integrity_unknown', 'timeout_unknown', 'transport_unknown'
    )
  ),
  ADD CONSTRAINT oracle_css_car_attempt_outcome_matrix_check CHECK (
    (outcome = 'verified' AND provider_status = 'accepted' AND
      provider_http_status IS NOT NULL AND
      provider_http_status BETWEEN 200 AND 299 AND
      observed_root_set_sha256 IS NOT NULL) OR
    (outcome = 'retryable_failure' AND
      provider_status = 'provider_retryable_status' AND
      provider_http_status IS NOT NULL AND provider_http_status = 429 AND
      observed_root_set_sha256 IS NULL) OR
    (outcome = 'outcome_unknown' AND provider_status IN (
      'provider_pin_error', 'provider_result_invalid',
      'provider_root_mismatch', 'response_too_large',
      'stream_integrity_unknown', 'timeout_unknown', 'transport_unknown'
    ) AND observed_root_set_sha256 IS NULL) OR
    (outcome = 'terminal_failure' AND (
      (provider_status = 'caller_aborted' AND provider_http_status IS NULL) OR
      (provider_status = 'redirect_rejected' AND
        provider_http_status IS NOT NULL AND
        provider_http_status BETWEEN 300 AND 399) OR
      (provider_status = 'provider_rejected' AND
        provider_http_status IS NOT NULL AND (
          provider_http_status BETWEEN 100 AND 199 OR
          (provider_http_status BETWEEN 400 AND 499 AND
            provider_http_status <> 429)
        ))
    ) AND observed_root_set_sha256 IS NULL)
  );

-- These narrow guards make the three post-review invariants independent of
-- which pre-repair migration-040 function body was applied locally.
CREATE OR REPLACE FUNCTION oracle_guard_css_car_attempt_global_cap_041()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  maximum_total_attempts integer;
  existing_attempt_count integer;
BEGIN
  SELECT car_auth.maximum_total_import_attempts
  INTO STRICT maximum_total_attempts
  FROM oracle_candidate_source_snapshot_car_import_authorizations car_auth
  JOIN oracle_candidate_source_snapshot_demo_plans publication_plan
    ON publication_plan.plan_id = car_auth.plan_id
  WHERE car_auth.car_authorization_id = NEW.car_authorization_id
    AND car_auth.plan_id = NEW.plan_id
    AND car_auth.plan_sha256 = NEW.plan_sha256
    AND car_auth.implementation_commit_sha =
      NEW.implementation_commit_sha
    AND publication_plan.state = 'executing'
  FOR UPDATE OF publication_plan;

  SELECT count(*)::integer INTO existing_attempt_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.plan_id = NEW.plan_id;

  IF existing_attempt_count + 1 > maximum_total_attempts THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR total attempt ceiling exceeded';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_attempt_041_global_cap_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_attempt_global_cap_041();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_outcome_chronology_041()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_started_at timestamptz;
BEGIN
  SELECT attempt.started_at INTO STRICT attempt_started_at
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.car_import_attempt_id = NEW.car_import_attempt_id
    AND attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id
    AND attempt.attempt_sequence = NEW.attempt_sequence
    AND attempt.implementation_commit_sha = NEW.implementation_commit_sha;

  IF NEW.recorded_at < attempt_started_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR outcome predates its request';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_outcome_041_chronology_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_car_import_attempt_outcomes
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_outcome_chronology_041();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_inspection_chronology_041()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  outcome_recorded_at timestamptz;
BEGIN
  SELECT outcome.recorded_at INTO STRICT outcome_recorded_at
  FROM oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
  JOIN oracle_candidate_source_snapshot_car_import_attempts attempt
    ON attempt.car_import_attempt_id = outcome.car_import_attempt_id
  WHERE outcome.car_import_outcome_id = NEW.car_import_outcome_id
    AND outcome.car_import_attempt_id = NEW.car_import_attempt_id
    AND outcome.car_artifact_id = NEW.car_artifact_id
    AND outcome.plan_id = NEW.plan_id
    AND attempt.implementation_commit_sha = NEW.implementation_commit_sha;

  IF NEW.inspected_at < outcome_recorded_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot CAR inspection predates its outcome';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_inspection_041_chronology_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_inspections
  FOR EACH ROW
  EXECUTE FUNCTION oracle_guard_css_car_inspection_chronology_041();

COMMIT;
