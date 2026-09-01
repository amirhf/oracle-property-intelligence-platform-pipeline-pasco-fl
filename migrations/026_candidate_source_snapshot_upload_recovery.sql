-- Durable recovery admissions for candidate source-snapshot object inspection.
-- Migration 025 is already applied in local development and remains immutable.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_inspections
  ) THEN
    RAISE EXCEPTION
      'migration 026 cannot infer the upload-attempt binding of historical candidate inspections';
  END IF;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_objects
  DROP CONSTRAINT IF EXISTS oracle_candidate_source_snapshot_demo_objects_request_count_check;
-- PostgreSQL truncates the anonymous migration-025 CHECK name to 63 bytes.
ALTER TABLE oracle_candidate_source_snapshot_demo_objects
  DROP CONSTRAINT IF EXISTS oracle_candidate_source_snapshot_demo_objec_request_count_check;
ALTER TABLE oracle_candidate_source_snapshot_demo_objects
  ADD CONSTRAINT oracle_candidate_source_snapshot_demo_objects_request_count_v2_check
  CHECK (request_count BETWEEN 0 AND 6);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_inspection_attempts (
  inspection_id text PRIMARY KEY CHECK (
    inspection_id ~ '^snapshotdemoinspection_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  recovery_upload_attempt_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_upload_attempts(attempt_id),
  plan_id text NOT NULL,
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  inspection_sequence integer NOT NULL CHECK (
    inspection_sequence BETWEEN 1 AND 3
  ),
  outcome text NOT NULL CHECK (outcome IN (
    'request_started', 'absent', 'verified', 'ambiguous', 'mismatch'
  )),
  observed_cid text CHECK (
    observed_cid IS NULL OR observed_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  observed_sha256 text CHECK (
    observed_sha256 IS NULL OR observed_sha256 ~ '^[a-f0-9]{64}$'
  ),
  observed_bytes bigint CHECK (
    observed_bytes IS NULL OR observed_bytes BETWEEN 0 AND 536870912
  ),
  receipt_sha256 text CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (plan_id, domain, remote_object_key, inspection_sequence),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    ),
  CHECK (
    (outcome = 'request_started' AND completed_at IS NULL AND
      observed_cid IS NULL AND observed_sha256 IS NULL AND
      observed_bytes IS NULL AND receipt_sha256 IS NULL) OR
    (outcome <> 'request_started' AND completed_at IS NOT NULL AND
      receipt_sha256 IS NOT NULL)
  )
);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  upload_row oracle_candidate_source_snapshot_demo_upload_attempts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.request_id;
  SELECT * INTO STRICT upload_row
  FROM oracle_candidate_source_snapshot_demo_upload_attempts
  WHERE attempt_id = NEW.recovery_upload_attempt_id;

  IF NEW.outcome IS DISTINCT FROM 'request_started' OR
     NEW.inspection_sequence IS DISTINCT FROM upload_row.attempt_sequence OR
     request_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     request_row.domain IS DISTINCT FROM NEW.domain OR
     request_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     request_row.operation_class IS DISTINCT FROM 'class_b_read' OR
     request_row.operation_kind IS DISTINCT FROM 'inspect_object' OR
     request_row.outcome IS DISTINCT FROM 'request_started' OR
     upload_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     upload_row.domain IS DISTINCT FROM NEW.domain OR
     upload_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     upload_row.outcome NOT IN (
       'connection_failure', 'retryable_http_error', 'timeout_unknown'
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection admission binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_inspection_attempt_insert_guard
  ON oracle_candidate_source_snapshot_demo_inspection_attempts;
CREATE TRIGGER oracle_candidate_source_snapshot_inspection_attempt_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_inspection_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.inspection_id IS DISTINCT FROM NEW.inspection_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.recovery_upload_attempt_id IS DISTINCT FROM NEW.recovery_upload_attempt_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.inspection_sequence IS DISTINCT FROM NEW.inspection_sequence OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome IS DISTINCT FROM 'request_started' OR
     NEW.outcome IS NOT DISTINCT FROM 'request_started' OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_inspections inspection
       WHERE inspection.inspection_id = OLD.inspection_id
         AND inspection.request_id = OLD.request_id
         AND inspection.plan_id = OLD.plan_id
         AND inspection.domain = OLD.domain
         AND inspection.remote_object_key = OLD.remote_object_key
         AND inspection.outcome = NEW.outcome
         AND inspection.observed_cid IS NOT DISTINCT FROM NEW.observed_cid
         AND inspection.observed_sha256 IS NOT DISTINCT FROM NEW.observed_sha256
         AND inspection.observed_bytes IS NOT DISTINCT FROM NEW.observed_bytes
         AND inspection.receipt_sha256 = NEW.receipt_sha256
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection attempt is immutable or lacks its exact result';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_inspection_attempt_guard
  ON oracle_candidate_source_snapshot_demo_inspection_attempts;
CREATE TRIGGER oracle_candidate_source_snapshot_inspection_attempt_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_inspection_attempts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_row oracle_candidate_source_snapshot_demo_inspection_attempts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_demo_inspection_attempts
  WHERE inspection_id = NEW.inspection_id
  FOR UPDATE;
  IF attempt_row.request_id IS DISTINCT FROM NEW.request_id OR
     attempt_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     attempt_row.domain IS DISTINCT FROM NEW.domain OR
     attempt_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     attempt_row.outcome IS DISTINCT FROM 'request_started' THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection result lacks its exact admission';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_inspection_insert_guard
  ON oracle_candidate_source_snapshot_demo_inspections;
CREATE TRIGGER oracle_candidate_source_snapshot_inspection_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_inspections
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_inspection_insert();

CREATE INDEX IF NOT EXISTS oracle_candidate_source_snapshot_inspection_attempt_object_idx
  ON oracle_candidate_source_snapshot_demo_inspection_attempts(
    plan_id, domain, remote_object_key, inspection_sequence
  );
