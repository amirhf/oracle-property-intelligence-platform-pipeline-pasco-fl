BEGIN;

-- Append-only pre-request and ambiguity evidence for CAR imports. This
-- migration performs no request and grants no authority beyond the exact CAR
-- authorization already stored by migrations 038-039.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_car_import_receipts
  ) THEN
    RAISE EXCEPTION
      'CAR attempt ledger cannot follow an existing import receipt';
  END IF;
END;
$$;

CREATE TABLE oracle_candidate_source_snapshot_car_import_attempts (
  car_import_attempt_id text PRIMARY KEY CHECK (
    car_import_attempt_id ~ '^snapshotdemocarattempt_[a-f0-9]{32}$'
  ),
  attempt_version text NOT NULL CHECK (
    attempt_version = 'candidate-source-snapshot-car-import-attempt-v1'
  ),
  event_kind text NOT NULL CHECK (event_kind = 'request_started'),
  car_authorization_id text NOT NULL,
  car_artifact_id text NOT NULL,
  plan_id text NOT NULL,
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  attempt_sequence integer NOT NULL CHECK (attempt_sequence BETWEEN 1 AND 2),
  endpoint text NOT NULL CHECK (endpoint IN (
    'https://rpc.filebase.io/api/v0/dag/import',
    'https://s3.filebase.com'
  )),
  import_method text NOT NULL CHECK (
    import_method IN ('rpc_dag_import', 's3_put_import_car')
  ),
  car_sha256 text NOT NULL CHECK (car_sha256 ~ '^[a-f0-9]{64}$'),
  car_bytes bigint NOT NULL CHECK (car_bytes > 0),
  primary_root_cid text NOT NULL CHECK (
    primary_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  root_set_sha256 text NOT NULL CHECK (root_set_sha256 ~ '^[a-f0-9]{64}$'),
  member_set_sha256 text NOT NULL CHECK (member_set_sha256 ~ '^[a-f0-9]{64}$'),
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  started_at timestamptz NOT NULL,
  request_payload jsonb NOT NULL,
  request_sha256 text NOT NULL UNIQUE CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  UNIQUE (car_import_attempt_id, car_artifact_id, plan_id),
  UNIQUE (car_artifact_id, attempt_sequence),
  FOREIGN KEY (car_authorization_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_import_authorizations(
      car_authorization_id, plan_id
    ),
  FOREIGN KEY (car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_artifacts(
      car_artifact_id, plan_id
    )
);

CREATE TABLE oracle_candidate_source_snapshot_car_import_attempt_outcomes (
  car_import_outcome_id text PRIMARY KEY CHECK (
    car_import_outcome_id ~ '^snapshotdemocaroutcome_[a-f0-9]{32}$'
  ),
  outcome_version text NOT NULL CHECK (
    outcome_version = 'candidate-source-snapshot-car-import-outcome-v1'
  ),
  car_import_attempt_id text NOT NULL UNIQUE,
  car_artifact_id text NOT NULL,
  plan_id text NOT NULL,
  attempt_sequence integer NOT NULL CHECK (attempt_sequence BETWEEN 1 AND 2),
  outcome text NOT NULL CHECK (outcome IN (
    'verified', 'retryable_failure', 'outcome_unknown', 'terminal_failure'
  )),
  provider_status text NOT NULL CHECK (provider_status IN (
    'accepted', 'caller_aborted', 'provider_pin_error', 'provider_rejected',
    'provider_result_invalid', 'provider_root_mismatch',
    'provider_retryable_status', 'redirect_rejected', 'response_too_large',
    'stream_integrity_unknown', 'timeout_unknown', 'transport_unknown'
  )),
  provider_http_status integer CHECK (
    provider_http_status IS NULL OR provider_http_status BETWEEN 100 AND 599
  ),
  provider_response_bytes bigint CHECK (
    provider_response_bytes IS NULL OR
    provider_response_bytes BETWEEN 0 AND 134217728
  ),
  provider_evidence_sha256 text NOT NULL CHECK (
    provider_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  provider_request_id_hash text CHECK (
    provider_request_id_hash IS NULL OR
    provider_request_id_hash ~ '^[a-f0-9]{64}$'
  ),
  observed_root_set_sha256 text CHECK (
    observed_root_set_sha256 IS NULL OR
    observed_root_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  recorded_at timestamptz NOT NULL,
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  outcome_payload jsonb NOT NULL,
  outcome_sha256 text NOT NULL UNIQUE CHECK (outcome_sha256 ~ '^[a-f0-9]{64}$'),
  CHECK (
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
    ) AND
      observed_root_set_sha256 IS NULL) OR
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
  ),
  UNIQUE (car_import_outcome_id, car_import_attempt_id, car_artifact_id, plan_id),
  FOREIGN KEY (car_import_attempt_id, car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_import_attempts(
      car_import_attempt_id, car_artifact_id, plan_id
    )
);

CREATE TABLE oracle_candidate_source_snapshot_car_import_inspections (
  car_import_inspection_id text PRIMARY KEY CHECK (
    car_import_inspection_id ~ '^snapshotdemocarinspection_[a-f0-9]{32}$'
  ),
  inspection_version text NOT NULL CHECK (
    inspection_version = 'candidate-source-snapshot-car-import-inspection-v1'
  ),
  car_import_attempt_id text NOT NULL UNIQUE,
  car_import_outcome_id text NOT NULL UNIQUE,
  car_artifact_id text NOT NULL,
  plan_id text NOT NULL,
  root_set_sha256 text NOT NULL CHECK (root_set_sha256 ~ '^[a-f0-9]{64}$'),
  inspection_result text NOT NULL CHECK (inspection_result IN (
    'conclusively_absent', 'present_exact', 'present_unexpected', 'unavailable'
  )),
  root_status text NOT NULL CHECK (root_status IN (
    'absent', 'present_exact', 'present_unexpected', 'unavailable'
  )),
  pin_status text NOT NULL CHECK (pin_status IN (
    'absent', 'pinned', 'pinning', 'failed', 'unavailable'
  )),
  observed_root_set_sha256 text CHECK (
    observed_root_set_sha256 IS NULL OR
    observed_root_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  provider_http_status integer CHECK (
    provider_http_status IS NULL OR provider_http_status BETWEEN 100 AND 599
  ),
  provider_response_bytes bigint CHECK (
    provider_response_bytes IS NULL OR
    provider_response_bytes BETWEEN 0 AND 134217728
  ),
  provider_evidence_sha256 text NOT NULL CHECK (
    provider_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  provider_request_id_hash text CHECK (
    provider_request_id_hash IS NULL OR
    provider_request_id_hash ~ '^[a-f0-9]{64}$'
  ),
  inspected_at timestamptz NOT NULL,
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  inspection_payload jsonb NOT NULL,
  inspection_sha256 text NOT NULL UNIQUE CHECK (
    inspection_sha256 ~ '^[a-f0-9]{64}$'
  ),
  CHECK (
    (inspection_result = 'conclusively_absent' AND root_status = 'absent' AND
      pin_status = 'absent' AND observed_root_set_sha256 IS NULL) OR
    (inspection_result = 'present_exact' AND root_status = 'present_exact' AND
      pin_status IN ('pinned', 'pinning') AND
      observed_root_set_sha256 IS NOT NULL) OR
    (inspection_result = 'present_unexpected' AND
      root_status = 'present_unexpected' AND
      observed_root_set_sha256 IS NOT NULL) OR
    (inspection_result = 'unavailable' AND (
      root_status = 'unavailable' OR pin_status = 'unavailable'
    ))
  ),
  UNIQUE (
    car_import_inspection_id, car_import_attempt_id, car_artifact_id, plan_id
  ),
  FOREIGN KEY (
    car_import_outcome_id, car_import_attempt_id, car_artifact_id, plan_id
  ) REFERENCES oracle_candidate_source_snapshot_car_import_attempt_outcomes(
    car_import_outcome_id, car_import_attempt_id, car_artifact_id, plan_id
  )
);

CREATE OR REPLACE FUNCTION oracle_guard_css_car_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  authorization_row
    oracle_candidate_source_snapshot_car_import_authorizations%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_state text;
  existing_attempt_count integer;
  prior_outcome text;
  expected_payload jsonb;
  expected_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_car_import_authorizations
  WHERE car_authorization_id = NEW.car_authorization_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT state INTO STRICT plan_state
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*)::integer INTO existing_attempt_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.plan_id = NEW.plan_id;
  expected_payload := jsonb_build_object(
    'artifactId', artifact_row.car_artifact_id,
    'authorizationId', authorization_row.car_authorization_id,
    'carBytes', artifact_row.car_bytes,
    'carSha256', artifact_row.car_sha256,
    'endpoint', authorization_row.endpoint,
    'eventKind', NEW.event_kind,
    'implementationCommitSha', NEW.implementation_commit_sha,
    'importMethod', authorization_row.import_method,
    'memberSetSha256', artifact_row.member_set_sha256,
    'planId', artifact_row.plan_id,
    'planSha256', artifact_row.plan_sha256,
    'primaryRootCid', artifact_row.primary_root_cid,
    'requestAttempt', NEW.attempt_sequence,
    'rootSetSha256', artifact_row.root_set_sha256,
    'schemaVersion', NEW.attempt_version,
    'startedAt', to_char(NEW.started_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  );
  expected_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocarattempt_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.attempt_version, artifact_row.plan_id, artifact_row.car_artifact_id,
      NEW.attempt_sequence::text, expected_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF NEW.attempt_sequence = 2 THEN
    SELECT outcome.outcome INTO prior_outcome
    FROM oracle_candidate_source_snapshot_car_import_attempts attempt
    JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
      ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
    WHERE attempt.car_artifact_id = NEW.car_artifact_id
      AND attempt.attempt_sequence = 1;
  END IF;
  IF plan_state IS DISTINCT FROM 'executing' OR
     authorization_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     authorization_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     authorization_row.implementation_commit_sha IS DISTINCT FROM
       NEW.implementation_commit_sha OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     NEW.endpoint IS DISTINCT FROM authorization_row.endpoint OR
     NEW.import_method IS DISTINCT FROM authorization_row.import_method OR
     NEW.car_sha256 IS DISTINCT FROM artifact_row.car_sha256 OR
     NEW.car_bytes IS DISTINCT FROM artifact_row.car_bytes OR
     NEW.primary_root_cid IS DISTINCT FROM artifact_row.primary_root_cid OR
     NEW.root_set_sha256 IS DISTINCT FROM artifact_row.root_set_sha256 OR
     NEW.member_set_sha256 IS DISTINCT FROM artifact_row.member_set_sha256 OR
     NEW.attempt_sequence > authorization_row.maximum_attempts_per_artifact OR
     existing_attempt_count + 1 >
       authorization_row.maximum_total_import_attempts OR
     (NEW.attempt_sequence = 2 AND
       prior_outcome IS DISTINCT FROM 'retryable_failure' AND NOT (
       prior_outcome IS NOT DISTINCT FROM 'outcome_unknown' AND EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_car_import_attempts prior_attempt
         JOIN oracle_candidate_source_snapshot_car_import_inspections inspection
           ON inspection.car_import_attempt_id = prior_attempt.car_import_attempt_id
         WHERE prior_attempt.car_artifact_id = NEW.car_artifact_id
           AND prior_attempt.attempt_sequence = 1
           AND inspection.inspection_result = 'conclusively_absent'
           AND inspection.root_set_sha256 = artifact_row.root_set_sha256
       )
     )) OR
     NEW.request_payload IS DISTINCT FROM expected_payload OR
     NEW.request_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.car_import_attempt_id IS DISTINCT FROM expected_id OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_receipts receipt
       WHERE receipt.car_artifact_id = NEW.car_artifact_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR import attempt is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_attempt_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_attempt_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_attempt_outcome_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_row oracle_candidate_source_snapshot_car_import_attempts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_state text;
  expected_payload jsonb;
  expected_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_car_import_attempts
  WHERE car_import_attempt_id = NEW.car_import_attempt_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = attempt_row.car_artifact_id;
  SELECT state INTO STRICT plan_state
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = attempt_row.plan_id
  FOR UPDATE;
  expected_payload := jsonb_build_object(
    'artifactId', attempt_row.car_artifact_id,
    'attemptId', attempt_row.car_import_attempt_id,
    'attemptSequence', attempt_row.attempt_sequence,
    'implementationCommitSha', NEW.implementation_commit_sha,
    'observedRootSetSha256', NEW.observed_root_set_sha256,
    'outcome', NEW.outcome,
    'planId', attempt_row.plan_id,
    'providerEvidenceSha256', NEW.provider_evidence_sha256,
    'providerHttpStatus', NEW.provider_http_status,
    'providerRequestIdHash', NEW.provider_request_id_hash,
    'providerResponseBytes', NEW.provider_response_bytes,
    'providerStatus', NEW.provider_status,
    'recordedAt', to_char(NEW.recorded_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'schemaVersion', NEW.outcome_version
  );
  expected_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocaroutcome_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.outcome_version, attempt_row.car_import_attempt_id,
      expected_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF plan_state IS DISTINCT FROM 'executing' OR
     NEW.car_artifact_id IS DISTINCT FROM attempt_row.car_artifact_id OR
     NEW.plan_id IS DISTINCT FROM attempt_row.plan_id OR
     NEW.attempt_sequence IS DISTINCT FROM attempt_row.attempt_sequence OR
     NEW.implementation_commit_sha IS DISTINCT FROM
       attempt_row.implementation_commit_sha OR
     NEW.recorded_at < attempt_row.started_at OR
     (NEW.outcome = 'verified' AND
       NEW.observed_root_set_sha256 IS DISTINCT FROM artifact_row.root_set_sha256) OR
     NEW.outcome_payload IS DISTINCT FROM expected_payload OR
     NEW.outcome_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.car_import_outcome_id IS DISTINCT FROM expected_id OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR import outcome is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_attempt_outcome_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_attempt_outcomes
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_attempt_outcome_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_import_inspection_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  outcome_row
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  attempt_row oracle_candidate_source_snapshot_car_import_attempts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_state text;
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
    RAISE EXCEPTION 'candidate source-snapshot CAR import inspection is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_import_inspection_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_inspections
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_import_inspection_insert();

-- A final CAR receipt must close the exact durable attempt sequence.
CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_attempts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_count integer;
  final_outcome
    oracle_candidate_source_snapshot_car_import_attempt_outcomes%ROWTYPE;
  exact_positive_inspection boolean;
BEGIN
  SELECT count(*)::integer INTO attempt_count
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id;
  SELECT outcome.* INTO final_outcome
  FROM oracle_candidate_source_snapshot_car_import_attempts attempt
  JOIN oracle_candidate_source_snapshot_car_import_attempt_outcomes outcome
    ON outcome.car_import_attempt_id = attempt.car_import_attempt_id
  WHERE attempt.car_artifact_id = NEW.car_artifact_id
    AND attempt.plan_id = NEW.plan_id
  ORDER BY attempt.attempt_sequence DESC
  LIMIT 1;
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_car_import_inspections inspection
    WHERE inspection.car_import_attempt_id = final_outcome.car_import_attempt_id
      AND inspection.car_import_outcome_id = final_outcome.car_import_outcome_id
      AND inspection.car_artifact_id = NEW.car_artifact_id
      AND inspection.plan_id = NEW.plan_id
      AND inspection.inspection_result = 'present_exact'
      AND inspection.root_status = 'present_exact'
      AND inspection.pin_status = 'pinned'
      AND inspection.root_set_sha256 = NEW.root_set_sha256
      AND inspection.observed_root_set_sha256 = NEW.root_set_sha256
  ) INTO exact_positive_inspection;
  IF attempt_count IS DISTINCT FROM NEW.import_attempt_count OR
     NOT (
       (final_outcome.outcome IS NOT DISTINCT FROM 'verified' AND
        final_outcome.observed_root_set_sha256 IS NOT DISTINCT FROM
          NEW.root_set_sha256) OR
       (final_outcome.outcome IS NOT DISTINCT FROM 'outcome_unknown' AND
        exact_positive_inspection)
     ) OR
     (NEW.provider_request_id_hash IS NOT NULL AND
       final_outcome.provider_request_id_hash IS DISTINCT FROM
         NEW.provider_request_id_hash) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR receipt lacks exact attempts';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_receipt_attempt_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_receipt_attempts();

CREATE TRIGGER oracle_css_car_attempt_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_car_import_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_attempt_outcome_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_car_import_attempt_outcomes
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_import_inspection_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_car_import_inspections
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

COMMIT;
