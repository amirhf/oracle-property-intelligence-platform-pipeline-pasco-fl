-- Allow a successor executor generation to inspect, but never rewrite, an
-- interrupted predecessor PUT when that exact attempt is already frozen into
-- the successor generation's immutable inspection cycle.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  upload_row oracle_candidate_source_snapshot_demo_upload_attempts%ROWTYPE;
  expected_inspection_sequence integer;
  predecessor_is_frozen boolean;
BEGIN
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.request_id;
  SELECT * INTO STRICT upload_row
  FROM oracle_candidate_source_snapshot_demo_upload_attempts
  WHERE attempt_id = NEW.recovery_upload_attempt_id;
  SELECT COALESCE(max(candidate.inspection_sequence), 0) + 1
  INTO expected_inspection_sequence
  FROM oracle_candidate_source_snapshot_demo_inspection_attempts candidate
  WHERE candidate.plan_id = NEW.plan_id
    AND candidate.domain = NEW.domain
    AND candidate.remote_object_key = NEW.remote_object_key;
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      ON member.inspection_cycle_id = cycle.inspection_cycle_id
    WHERE cycle.plan_id = NEW.plan_id
      AND cycle.resume_authorization_id =
        request_row.upload_resume_authorization_id
      AND cycle.executor_lease_id = request_row.executor_lease_id
      AND cycle.lease_generation = request_row.executor_lease_epoch
      AND member.domain = NEW.domain
      AND member.remote_object_key = NEW.remote_object_key
      AND member.source_attempt_id = upload_row.attempt_id
  ) INTO predecessor_is_frozen;
  IF request_row.executor_lease_id IS NOT NULL AND
     request_row.executor_lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(NEW.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection belongs to a fenced generation';
  END IF;
  IF NEW.outcome IS DISTINCT FROM 'request_started' OR
     (request_row.upload_resume_authorization_id IS NULL AND
       NEW.inspection_sequence IS DISTINCT FROM upload_row.attempt_sequence) OR
     (request_row.upload_resume_authorization_id IS NOT NULL AND
       NEW.inspection_sequence IS DISTINCT FROM expected_inspection_sequence) OR
     request_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     request_row.domain IS DISTINCT FROM NEW.domain OR
     request_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     request_row.operation_class IS DISTINCT FROM 'class_b_read' OR
     request_row.operation_kind IS DISTINCT FROM 'inspect_object' OR
     request_row.outcome IS DISTINCT FROM 'request_started' OR
     upload_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     upload_row.domain IS DISTINCT FROM NEW.domain OR
     upload_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     NOT (
       upload_row.outcome IN (
         'connection_failure', 'retryable_http_error', 'timeout_unknown'
       ) OR
       (upload_row.outcome = 'request_started' AND predecessor_is_frozen)
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection admission binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;
