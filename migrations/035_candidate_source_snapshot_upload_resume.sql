BEGIN;

-- This migration preserves migration-034 history. It adds one fenced resume
-- generation, deterministic local admitted-object recovery, and append-only
-- inspection cycles for uncertainty created after the original continuation.

CREATE TABLE oracle_candidate_source_snapshot_admitted_recovery_events (
  recovery_event_id text CHECK (
    recovery_event_id ~ '^snapshotdemoadmittedrecovery_[a-f0-9]{32}$'
  ),
  recovery_version text NOT NULL CHECK (
    recovery_version = 'candidate-source-snapshot-admitted-recovery-v1'
  ),
  plan_id text NOT NULL,
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  source_request_id text
    REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  source_attempt_id text
    REFERENCES oracle_candidate_source_snapshot_demo_upload_attempts(attempt_id),
  disposition text NOT NULL CHECK (
    disposition IN ('returned_pending_no_put', 'inspection_required')
  ),
  evidence jsonb NOT NULL,
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  amended_implementation_commit_sha text NOT NULL CHECK (
    amended_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  event_sha256 text NOT NULL UNIQUE CHECK (event_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL,
  recorded_at_iso text NOT NULL CHECK (
    recorded_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND recorded_at_iso::timestamptz = recorded_at
  ),
  PRIMARY KEY (recovery_event_id),
  UNIQUE (plan_id, domain, remote_object_key),
  UNIQUE (source_request_id),
  UNIQUE (source_attempt_id),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

-- Forward declaration issue: the classifier references the table. Recreate it
-- after table creation so PostgreSQL validates and binds the relation now.
CREATE OR REPLACE FUNCTION oracle_css_current_admitted_recovery_rows(
  checked_plan_id text
)
RETURNS TABLE (
  plan_id text,
  domain text,
  remote_object_key text,
  source_request_id text,
  source_attempt_id text,
  expected_sha256 text,
  expected_cid text,
  expected_bytes bigint,
  disposition text,
  evidence jsonb,
  evidence_sha256 text
) LANGUAGE sql STABLE STRICT AS $$
  WITH admitted AS (
    SELECT object.plan_id, object.domain, object.remote_object_key,
           object.expected_sha256, object.expected_cid,
           object.expected_bytes, object.attempt_count, object.request_count,
           attempt.request_id, attempt.attempt_id,
           attempt.attempt_sequence, attempt.outcome AS attempt_outcome,
           attempt.transport_stage, attempt.failure_class,
           attempt.provider_request_id_hash, attempt.provider_cid,
           attempt.response_bytes,
           attempt.completed_at AS attempt_completed_at,
           request.outcome AS request_outcome,
           request.completed_at AS request_completed_at,
           EXISTS (
             SELECT 1
             FROM oracle_candidate_source_snapshot_demo_upload_attempts open_attempt
             JOIN oracle_candidate_source_snapshot_demo_requests open_request
               ON open_request.request_id = open_attempt.request_id
             WHERE open_attempt.plan_id = object.plan_id
               AND open_attempt.domain = object.domain
               AND open_attempt.remote_object_key = object.remote_object_key
               AND (
                 open_attempt.outcome = 'request_started' OR
                 open_request.outcome = 'request_started'
               )
           ) AS has_open_request
    FROM oracle_candidate_source_snapshot_demo_objects object
    LEFT JOIN LATERAL (
      SELECT candidate.*
      FROM oracle_candidate_source_snapshot_demo_upload_attempts candidate
      WHERE candidate.plan_id = object.plan_id
        AND candidate.domain = object.domain
        AND candidate.remote_object_key = object.remote_object_key
      ORDER BY candidate.attempt_sequence DESC
      LIMIT 1
    ) attempt ON true
    LEFT JOIN oracle_candidate_source_snapshot_demo_requests request
      ON request.request_id = attempt.request_id
    WHERE object.plan_id = checked_plan_id
      AND object.status = 'admitted'
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_admitted_recovery_events prior
        WHERE prior.plan_id = object.plan_id
          AND prior.domain = object.domain
          AND prior.remote_object_key = object.remote_object_key
      )
  ), classified AS (
    SELECT admitted.*,
      CASE WHEN (
        admitted.attempt_count = 0 AND admitted.request_count = 0 AND
        admitted.attempt_id IS NULL AND admitted.request_id IS NULL AND
        admitted.provider_cid IS NULL AND admitted.response_bytes IS NULL AND
        admitted.has_open_request IS NOT TRUE
      )
      THEN 'returned_pending_no_put'::text
      ELSE 'inspection_required'::text END AS disposition
    FROM admitted
  ), evidence AS (
    SELECT classified.*,
      jsonb_build_object(
        'attemptOutcome', classified.attempt_outcome,
        'attemptCount', classified.attempt_count,
        'attemptSequence', classified.attempt_sequence,
        'failureClass', classified.failure_class,
        'hasOpenRequest', classified.has_open_request,
        'providerRequestIdObserved',
          classified.provider_request_id_hash IS NOT NULL,
        'requestOutcome', classified.request_outcome,
        'requestCount', classified.request_count,
        'schemaVersion',
          'candidate-source-snapshot-admitted-recovery-evidence-v1',
        'transportStage', classified.transport_stage
      ) AS evidence
    FROM classified
  )
  SELECT evidence.plan_id, evidence.domain, evidence.remote_object_key,
         evidence.request_id, evidence.attempt_id,
         evidence.expected_sha256, evidence.expected_cid,
         evidence.expected_bytes, evidence.disposition, evidence.evidence,
         encode(sha256(convert_to(
           oracle_canonical_jsonb(evidence.evidence), 'UTF8'
         )), 'hex')
  FROM evidence;
$$;

CREATE OR REPLACE FUNCTION oracle_css_admitted_recovery_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    row.domain || chr(31) ||
    encode(sha256(convert_to(row.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || COALESCE(row.source_request_id, '') ||
    chr(31) || COALESCE(row.source_attempt_id, '') ||
    chr(31) || row.disposition || chr(31) || row.evidence_sha256 ||
    chr(31) || row.expected_sha256 || chr(31) || row.expected_cid ||
    chr(31) || row.expected_bytes::text,
    chr(30) ORDER BY row.domain, row.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_css_current_admitted_recovery_rows(checked_plan_id) row;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_admitted_recovery_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected record;
  expected_event_sha text;
  expected_event_id text;
BEGIN
  SELECT * INTO STRICT expected
  FROM oracle_css_current_admitted_recovery_rows(NEW.plan_id) row
  WHERE row.domain = NEW.domain
    AND row.remote_object_key = NEW.remote_object_key;
  expected_event_sha := encode(sha256(convert_to(oracle_canonical_jsonb(
    jsonb_build_object(
      'amendedImplementationCommitSha',
        NEW.amended_implementation_commit_sha,
      'disposition', expected.disposition,
      'domain', expected.domain,
      'evidenceSha256', expected.evidence_sha256,
      'expectedBytes', expected.expected_bytes,
      'expectedCid', expected.expected_cid,
      'expectedSha256', expected.expected_sha256,
      'planId', expected.plan_id,
      'remoteObjectKeySha256', encode(sha256(convert_to(
        expected.remote_object_key, 'UTF8'
      )), 'hex'),
      'schemaVersion', 'candidate-source-snapshot-admitted-recovery-event-v1',
      'sourceAttemptId', expected.source_attempt_id,
      'sourceRequestId', expected.source_request_id
    )
  ), 'UTF8')), 'hex');
  expected_event_id := 'snapshotdemoadmittedrecovery_' || substr(encode(
    sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-admitted-recovery-v1',
      expected.plan_id, expected.domain,
      encode(sha256(convert_to(expected.remote_object_key, 'UTF8')), 'hex'),
      COALESCE(expected.source_attempt_id, ''), expected_event_sha
    ])), 'UTF8')), 'hex'), 1, 32);
  IF NEW.recovery_event_id IS DISTINCT FROM expected_event_id OR
     NEW.recovery_version IS DISTINCT FROM
       'candidate-source-snapshot-admitted-recovery-v1' OR
     NEW.source_request_id IS DISTINCT FROM expected.source_request_id OR
     NEW.source_attempt_id IS DISTINCT FROM expected.source_attempt_id OR
     NEW.disposition IS DISTINCT FROM expected.disposition OR
     NEW.evidence IS DISTINCT FROM expected.evidence OR
     NEW.evidence_sha256 IS DISTINCT FROM expected.evidence_sha256 OR
     NEW.event_sha256 IS DISTINCT FROM expected_event_sha OR
     NEW.recorded_at_iso::timestamptz IS DISTINCT FROM NEW.recorded_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot admitted recovery is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_admitted_recovery_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_admitted_recovery_events
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_admitted_recovery_insert();
CREATE TRIGGER oracle_css_admitted_recovery_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_admitted_recovery_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

-- The application inserts one event per row returned by the classifier in one
-- transaction. Only conclusive pre-dispatch failures may return to pending.
CREATE OR REPLACE FUNCTION oracle_css_finalize_admitted_recovery()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.disposition = 'returned_pending_no_put' THEN
    UPDATE oracle_candidate_source_snapshot_demo_objects
    SET status = 'pending', revision = revision + 1, updated_at = now()
    WHERE plan_id = NEW.plan_id AND domain = NEW.domain
      AND remote_object_key = NEW.remote_object_key AND status = 'admitted';
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'candidate source-snapshot admitted recovery lost its object';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_finalize_admitted_recovery_event
  AFTER INSERT ON oracle_candidate_source_snapshot_admitted_recovery_events
  FOR EACH ROW EXECUTE FUNCTION oracle_css_finalize_admitted_recovery();

CREATE OR REPLACE FUNCTION oracle_css_record_admitted_recovery(
  checked_plan_id text,
  checked_amended_commit_sha text,
  checked_recorded_at timestamptz
)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  candidate record;
  expected_event_sha text;
  expected_event_id text;
  inserted_count integer := 0;
  final_count integer;
  final_sha text;
BEGIN
  IF checked_amended_commit_sha !~ '^[a-f0-9]{40}$' THEN
    RAISE EXCEPTION 'candidate source-snapshot recovery commit is invalid';
  END IF;
  PERFORM plan_id
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = checked_plan_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate source-snapshot recovery plan is absent';
  END IF;

  FOR candidate IN
    SELECT *
    FROM oracle_css_current_admitted_recovery_rows(checked_plan_id)
    ORDER BY domain, remote_object_key
  LOOP
    expected_event_sha := encode(sha256(convert_to(oracle_canonical_jsonb(
      jsonb_build_object(
        'amendedImplementationCommitSha', checked_amended_commit_sha,
        'disposition', candidate.disposition,
        'domain', candidate.domain,
        'evidenceSha256', candidate.evidence_sha256,
        'expectedBytes', candidate.expected_bytes,
        'expectedCid', candidate.expected_cid,
        'expectedSha256', candidate.expected_sha256,
        'planId', candidate.plan_id,
        'remoteObjectKeySha256', encode(sha256(convert_to(
          candidate.remote_object_key, 'UTF8'
        )), 'hex'),
        'schemaVersion',
          'candidate-source-snapshot-admitted-recovery-event-v1',
        'sourceAttemptId', candidate.source_attempt_id,
        'sourceRequestId', candidate.source_request_id
      )
    ), 'UTF8')), 'hex');
    expected_event_id := 'snapshotdemoadmittedrecovery_' || substr(encode(
      sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
        'candidate-source-snapshot-admitted-recovery-v1', candidate.plan_id,
        candidate.domain, encode(sha256(convert_to(
          candidate.remote_object_key, 'UTF8'
        )), 'hex'), COALESCE(candidate.source_attempt_id, ''),
        expected_event_sha
      ])), 'UTF8')), 'hex'), 1, 32);
    INSERT INTO oracle_candidate_source_snapshot_admitted_recovery_events (
      recovery_event_id, recovery_version, plan_id, domain,
      remote_object_key, source_request_id, source_attempt_id, disposition,
      evidence, evidence_sha256, amended_implementation_commit_sha,
      event_sha256, recorded_at, recorded_at_iso
    ) VALUES (
      expected_event_id, 'candidate-source-snapshot-admitted-recovery-v1',
      candidate.plan_id, candidate.domain, candidate.remote_object_key,
      candidate.source_request_id, candidate.source_attempt_id,
      candidate.disposition, candidate.evidence, candidate.evidence_sha256,
      checked_amended_commit_sha, expected_event_sha, checked_recorded_at,
      to_char(checked_recorded_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    );
    inserted_count := inserted_count + 1;
  END LOOP;

  SELECT count(*)::integer,
         encode(sha256(convert_to(COALESCE(string_agg(
           event.domain || chr(31) ||
           encode(sha256(convert_to(event.remote_object_key, 'UTF8')), 'hex') ||
           chr(31) || COALESCE(event.source_request_id, '') || chr(31) ||
           COALESCE(event.source_attempt_id, '') || chr(31) ||
           event.disposition || chr(31) || event.evidence_sha256 || chr(31) ||
           event.event_sha256,
           chr(30) ORDER BY event.domain, event.remote_object_key
         ), ''), 'UTF8')), 'hex')
  INTO final_count, final_sha
  FROM oracle_candidate_source_snapshot_admitted_recovery_events event
  WHERE event.plan_id = checked_plan_id;
  RETURN jsonb_build_object(
    'insertedCount', inserted_count,
    'recordedCount', final_count,
    'recordedSetSha256', final_sha
  );
END;
$$;

-- One immutable resume authorization may supersede an expired generation.
CREATE TABLE oracle_candidate_source_snapshot_upload_resume_authorizations (
  resume_authorization_id text PRIMARY KEY CHECK (
    resume_authorization_id ~ '^snapshotdemouploadresume_[a-f0-9]{32}$'
  ),
  authorization_version text NOT NULL CHECK (
    authorization_version =
      'candidate-source-snapshot-upload-resume-authorization-v1'
  ),
  authorization_sha256 text NOT NULL UNIQUE CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_revision integer NOT NULL CHECK (plan_revision > 0),
  predecessor_authorization_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_upload_continuation_authorizations(
      authorization_id
    ),
  predecessor_authorization_sha256 text NOT NULL CHECK (
    predecessor_authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  predecessor_lease_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_executor_leases(lease_id),
  predecessor_lease_generation integer NOT NULL CHECK (
    predecessor_lease_generation > 0
  ),
  resume_lease_generation integer NOT NULL CHECK (
    resume_lease_generation > predecessor_lease_generation
  ),
  amended_implementation_commit_sha text NOT NULL CHECK (
    amended_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  verified_object_count integer NOT NULL CHECK (verified_object_count >= 0),
  verified_bytes bigint NOT NULL CHECK (verified_bytes >= 0),
  verified_receipt_set_sha256 text NOT NULL CHECK (
    verified_receipt_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  admitted_recovery_count integer NOT NULL CHECK (admitted_recovery_count >= 0),
  admitted_recovery_set_sha256 text NOT NULL CHECK (
    admitted_recovery_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  future_inspection_cycle_count integer NOT NULL CHECK (
    future_inspection_cycle_count >= 0
  ),
  future_inspection_cycle_set_sha256 text NOT NULL CHECK (
    future_inspection_cycle_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorization_binding jsonb NOT NULL,
  authorization_binding_sha256 text NOT NULL CHECK (
    authorization_binding_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorization_statement text NOT NULL CHECK (
    octet_length(authorization_statement) BETWEEN 1 AND 12000 AND
    authorization_statement !~ E'[\r\n]'
  ),
  authorization_statement_sha256 text NOT NULL CHECK (
    authorization_statement_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorizer_reference text NOT NULL CHECK (
    authorizer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  authorized_at_iso text NOT NULL CHECK (
    authorized_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND authorized_at_iso::timestamptz = authorized_at
  ),
  authorization_payload jsonb NOT NULL,
  UNIQUE (plan_id, resume_lease_generation),
  UNIQUE (predecessor_lease_id)
);

ALTER TABLE oracle_candidate_source_snapshot_executor_leases
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_leases_authorization_id_key,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor__authorization_id_key,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_leases_plan_id_key,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_leases_lease_version_check,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_l_lease_version_check,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_leases_lease_epoch_check,
  DROP CONSTRAINT IF EXISTS
    oracle_candidate_source_snapshot_executor_lea_lease_epoch_check,
  ADD COLUMN resume_authorization_id text REFERENCES
    oracle_candidate_source_snapshot_upload_resume_authorizations(
      resume_authorization_id
    ),
  ADD CONSTRAINT oracle_css_executor_lease_version_v2_check CHECK (
    lease_version IN (
      'candidate-source-snapshot-executor-lease-v1',
      'candidate-source-snapshot-executor-lease-v2'
    )
  ),
  ADD CONSTRAINT oracle_css_executor_lease_generation_check CHECK (
    lease_epoch > 0
  ),
  ADD CONSTRAINT oracle_css_executor_lease_authorization_kind_check CHECK (
    (lease_version = 'candidate-source-snapshot-executor-lease-v1' AND
      authorization_id IS NOT NULL AND resume_authorization_id IS NULL AND
      lease_epoch = 1) OR
    (lease_version = 'candidate-source-snapshot-executor-lease-v2' AND
      authorization_id IS NOT NULL AND resume_authorization_id IS NOT NULL AND
      lease_epoch > 1)
  ),
  ADD CONSTRAINT oracle_css_executor_lease_plan_generation_unique
    UNIQUE (plan_id, lease_epoch);

CREATE TABLE oracle_candidate_source_snapshot_executor_lease_supersession_events (
  supersession_event_id text PRIMARY KEY CHECK (
    supersession_event_id ~ '^snapshotdemoleasesupersession_[a-f0-9]{32}$'
  ),
  supersession_version text NOT NULL CHECK (
    supersession_version =
      'candidate-source-snapshot-executor-lease-supersession-v1'
  ),
  plan_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_demo_plans(plan_id),
  resume_authorization_id text NOT NULL UNIQUE REFERENCES
    oracle_candidate_source_snapshot_upload_resume_authorizations(
      resume_authorization_id
    ),
  predecessor_lease_id text NOT NULL UNIQUE REFERENCES
    oracle_candidate_source_snapshot_executor_leases(lease_id),
  predecessor_lease_generation integer NOT NULL CHECK (
    predecessor_lease_generation > 0
  ),
  successor_lease_id text NOT NULL UNIQUE REFERENCES
    oracle_candidate_source_snapshot_executor_leases(lease_id),
  successor_lease_generation integer NOT NULL CHECK (
    successor_lease_generation = predecessor_lease_generation + 1
  ),
  predecessor_expires_at timestamptz NOT NULL,
  expiry_grace_ms integer NOT NULL CHECK (expiry_grace_ms = 30000),
  superseded_at timestamptz NOT NULL,
  holder_token_sha256 text NOT NULL CHECK (
    holder_token_sha256 ~ '^[a-f0-9]{64}$'
  ),
  event_sha256 text NOT NULL UNIQUE CHECK (event_sha256 ~ '^[a-f0-9]{64}$')
);

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  DROP CONSTRAINT IF EXISTS oracle_css_request_executor_lease_binding_check,
  ADD COLUMN upload_resume_authorization_id text REFERENCES
    oracle_candidate_source_snapshot_upload_resume_authorizations(
      resume_authorization_id
    ),
  ADD CONSTRAINT oracle_css_request_executor_lease_binding_v2_check CHECK (
    (upload_continuation_authorization_id IS NULL AND
      upload_resume_authorization_id IS NULL AND
      executor_lease_id IS NULL AND executor_lease_epoch IS NULL) OR
    (upload_continuation_authorization_id IS NOT NULL AND
      upload_resume_authorization_id IS NULL AND
      executor_lease_id IS NOT NULL AND executor_lease_epoch = 1) OR
    (upload_continuation_authorization_id IS NOT NULL AND
      upload_resume_authorization_id IS NOT NULL AND
      executor_lease_id IS NOT NULL AND executor_lease_epoch > 1)
  );

-- Cycle rows and membership are append-only. Resolution is a separate
-- append-only row, so canonical membership can never be rewritten.
CREATE TABLE oracle_candidate_source_snapshot_upload_inspection_cycles (
  inspection_cycle_id text PRIMARY KEY CHECK (
    inspection_cycle_id ~ '^snapshotdemoinspectioncycle_[a-f0-9]{32}$'
  ),
  cycle_version text NOT NULL CHECK (
    cycle_version = 'candidate-source-snapshot-upload-inspection-cycle-v1'
  ),
  plan_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_demo_plans(plan_id),
  resume_authorization_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_upload_resume_authorizations(
      resume_authorization_id
    ),
  executor_lease_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_executor_leases(lease_id),
  lease_generation integer NOT NULL CHECK (lease_generation > 1),
  cycle_sequence integer NOT NULL CHECK (cycle_sequence > 0),
  member_count integer NOT NULL CHECK (member_count > 0),
  membership_sha256 text NOT NULL CHECK (
    membership_sha256 ~ '^[a-f0-9]{64}$'
  ),
  created_at timestamptz NOT NULL,
  UNIQUE (plan_id, lease_generation, cycle_sequence),
  UNIQUE (plan_id, membership_sha256)
);

CREATE TABLE oracle_candidate_source_snapshot_upload_inspection_cycle_members (
  inspection_cycle_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_upload_inspection_cycles(
      inspection_cycle_id
    ),
  plan_id text NOT NULL,
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  uncertainty_kind text NOT NULL CHECK (
    uncertainty_kind IN ('stale_request_started', 'outcome_unknown')
  ),
  source_request_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_demo_requests(request_id),
  source_attempt_id text NOT NULL REFERENCES
    oracle_candidate_source_snapshot_demo_upload_attempts(attempt_id),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_bytes bigint NOT NULL CHECK (
    expected_bytes BETWEEN 0 AND 536870912
  ),
  PRIMARY KEY (inspection_cycle_id, domain, remote_object_key),
  UNIQUE (plan_id, source_attempt_id),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE TABLE oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions (
  inspection_cycle_id text NOT NULL,
  plan_id text NOT NULL,
  domain text NOT NULL,
  remote_object_key text NOT NULL,
  inspection_id text NOT NULL UNIQUE REFERENCES
    oracle_candidate_source_snapshot_demo_inspections(inspection_id),
  result text NOT NULL CHECK (
    result IN ('remote_verified', 'conclusively_absent')
  ),
  receipt_sha256 text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (inspection_cycle_id, domain, remote_object_key),
  FOREIGN KEY (inspection_cycle_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_upload_inspection_cycle_members(
      inspection_cycle_id, domain, remote_object_key
    ),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE OR REPLACE FUNCTION oracle_css_recorded_admitted_recovery_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    event.domain || chr(31) ||
    encode(sha256(convert_to(event.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || COALESCE(event.source_request_id, '') || chr(31) ||
    COALESCE(event.source_attempt_id, '') || chr(31) || event.disposition ||
    chr(31) || event.evidence_sha256 || chr(31) || event.event_sha256,
    chr(30) ORDER BY event.domain, event.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_admitted_recovery_events event
  WHERE event.plan_id = checked_plan_id;
$$;

CREATE OR REPLACE FUNCTION oracle_css_future_inspection_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    event.domain || chr(31) ||
    encode(sha256(convert_to(event.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || COALESCE(event.source_request_id, '') || chr(31) ||
    COALESCE(event.source_attempt_id, '') || chr(31) || event.evidence_sha256,
    chr(30) ORDER BY event.domain, event.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_admitted_recovery_events event
  WHERE event.plan_id = checked_plan_id
    AND event.disposition = 'inspection_required';
$$;

CREATE OR REPLACE FUNCTION oracle_css_active_executor_lease_generation(
  checked_plan_id text
)
RETURNS integer LANGUAGE sql STABLE STRICT AS $$
  SELECT max(lease_epoch)
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE plan_id = checked_plan_id;
$$;

CREATE OR REPLACE FUNCTION oracle_css_assert_active_executor_lease(
  checked_lease_id text,
  checked_generation integer,
  checked_authorization_id text
)
RETURNS void LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  checked_plan_id text;
BEGIN
  SELECT lease.plan_id INTO STRICT checked_plan_id
  FROM oracle_candidate_source_snapshot_executor_leases
  lease
  WHERE lease.lease_id = checked_lease_id;
  -- Supersession takes this same plan row FOR UPDATE before inserting the next
  -- generation. Holding a shared lock through the caller's transaction closes
  -- the revalidation-to-write race for every old-generation mutation.
  PERFORM plan.plan_id
  FROM oracle_candidate_source_snapshot_demo_plans plan
  WHERE plan.plan_id = checked_plan_id
  FOR SHARE;
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = checked_lease_id
  FOR SHARE;
  IF lease_row.lease_epoch IS DISTINCT FROM checked_generation OR
     lease_row.lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(lease_row.plan_id) OR
     lease_row.phase = 'released' OR lease_row.expires_at <= now() OR
     COALESCE(lease_row.resume_authorization_id,
       lease_row.authorization_id) IS DISTINCT FROM
       checked_authorization_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease generation is not active';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_css_current_upload_cycle_uncertain_rows(
  checked_plan_id text
)
RETURNS TABLE (
  plan_id text,
  domain text,
  remote_object_key text,
  uncertainty_kind text,
  source_request_id text,
  source_attempt_id text,
  expected_sha256 text,
  expected_cid text,
  expected_bytes bigint
) LANGUAGE sql STABLE STRICT AS $$
  SELECT object.plan_id, object.domain, object.remote_object_key,
    CASE WHEN attempt.outcome = 'request_started'
      THEN 'stale_request_started'::text
      ELSE 'outcome_unknown'::text END,
    attempt.request_id, attempt.attempt_id, object.expected_sha256,
    object.expected_cid, object.expected_bytes
  FROM oracle_candidate_source_snapshot_demo_objects object
  JOIN LATERAL (
    SELECT candidate.*
    FROM oracle_candidate_source_snapshot_demo_upload_attempts candidate
    WHERE candidate.plan_id = object.plan_id
      AND candidate.domain = object.domain
      AND candidate.remote_object_key = object.remote_object_key
    ORDER BY candidate.attempt_sequence DESC
    LIMIT 1
  ) attempt ON true
  JOIN oracle_candidate_source_snapshot_demo_requests request
    ON request.request_id = attempt.request_id
  WHERE object.plan_id = checked_plan_id
    AND (
      (object.status = 'admitted' AND attempt.outcome = 'request_started' AND
        request.outcome = 'request_started') OR
      (object.status = 'outcome_unknown' AND
        attempt.outcome = 'timeout_unknown' AND
        request.outcome = 'timeout_unknown') OR
      (object.status = 'admitted' AND EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_admitted_recovery_events recovery
        WHERE recovery.plan_id = object.plan_id
          AND recovery.domain = object.domain
          AND recovery.remote_object_key = object.remote_object_key
          AND recovery.disposition = 'inspection_required'
          AND recovery.source_attempt_id = attempt.attempt_id
      ))
    )
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      WHERE member.plan_id = object.plan_id
        AND member.source_attempt_id = attempt.attempt_id
    );
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_cycle_uncertain_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    row.domain || chr(31) ||
    encode(sha256(convert_to(row.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || row.uncertainty_kind || chr(31) || row.source_request_id ||
    chr(31) || row.source_attempt_id || chr(31) || row.expected_sha256 ||
    chr(31) || row.expected_cid || chr(31) || row.expected_bytes::text,
    chr(30) ORDER BY row.domain, row.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_css_current_upload_cycle_uncertain_rows(checked_plan_id) row;
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_inspection_cycle_is_reconciled(
  checked_cycle_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1 FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    WHERE cycle.inspection_cycle_id = checked_cycle_id
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
    WHERE member.inspection_cycle_id = checked_cycle_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
        WHERE resolution.inspection_cycle_id = member.inspection_cycle_id
          AND resolution.domain = member.domain
          AND resolution.remote_object_key = member.remote_object_key
      )
  );
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_resume_is_reconciled(
  checked_resume_authorization_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND oracle_css_upload_continuation_is_reconciled(
        resume.predecessor_authorization_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    WHERE cycle.resume_authorization_id = checked_resume_authorization_id
      AND NOT oracle_css_upload_inspection_cycle_is_reconciled(
        cycle.inspection_cycle_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN oracle_candidate_source_snapshot_admitted_recovery_events recovery
      ON recovery.plan_id = resume.plan_id
     AND recovery.disposition = 'inspection_required'
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
          ON member.inspection_cycle_id = cycle.inspection_cycle_id
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          ON resolution.inspection_cycle_id = member.inspection_cycle_id
         AND resolution.domain = member.domain
         AND resolution.remote_object_key = member.remote_object_key
        WHERE cycle.resume_authorization_id = resume.resume_authorization_id
          AND member.plan_id = recovery.plan_id
          AND member.domain = recovery.domain
          AND member.remote_object_key = recovery.remote_object_key
          AND member.source_attempt_id = recovery.source_attempt_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN LATERAL oracle_css_current_upload_cycle_uncertain_rows(resume.plan_id)
      uncertain ON true
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
  );
$$;

-- Initial admission is separate from runtime quarantine. A quarantined object
-- blocks only itself and final closure; it must not stop unrelated pending
-- objects from making progress under the same fenced executor generation.
CREATE OR REPLACE FUNCTION oracle_css_upload_resume_admission_is_reconciled(
  checked_resume_authorization_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND oracle_css_upload_continuation_is_reconciled(
        resume.predecessor_authorization_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN oracle_candidate_source_snapshot_admitted_recovery_events recovery
      ON recovery.plan_id = resume.plan_id
     AND recovery.disposition = 'inspection_required'
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
          ON member.inspection_cycle_id = cycle.inspection_cycle_id
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          ON resolution.inspection_cycle_id = member.inspection_cycle_id
         AND resolution.domain = member.domain
         AND resolution.remote_object_key = member.remote_object_key
        WHERE cycle.resume_authorization_id = resume.resume_authorization_id
          AND member.plan_id = recovery.plan_id
          AND member.domain = recovery.domain
          AND member.remote_object_key = recovery.remote_object_key
          AND member.source_attempt_id = recovery.source_attempt_id
      )
  );
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_inspection_cycle_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  member_count integer;
  membership_sha text;
  expected_sequence integer;
  expected_cycle_id text;
BEGIN
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.executor_lease_id
  FOR UPDATE;
  PERFORM oracle_css_assert_active_executor_lease(
    NEW.executor_lease_id, NEW.lease_generation,
    NEW.resume_authorization_id
  );
  SELECT count(*)::integer,
         oracle_css_upload_cycle_uncertain_set_sha256(NEW.plan_id)
  INTO member_count, membership_sha
  FROM oracle_css_current_upload_cycle_uncertain_rows(NEW.plan_id);
  SELECT COALESCE(max(cycle_sequence), 0) + 1
  INTO expected_sequence
  FROM oracle_candidate_source_snapshot_upload_inspection_cycles
  WHERE plan_id = NEW.plan_id AND lease_generation = NEW.lease_generation;
  expected_cycle_id := 'snapshotdemoinspectioncycle_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-inspection-cycle-v1', NEW.plan_id,
      NEW.resume_authorization_id, NEW.lease_generation::text,
      expected_sequence::text, membership_sha
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  IF member_count < 1 OR
     lease_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     lease_row.resume_authorization_id IS DISTINCT FROM
       NEW.resume_authorization_id OR
     lease_row.lease_epoch IS DISTINCT FROM NEW.lease_generation OR
     NEW.cycle_version IS DISTINCT FROM
       'candidate-source-snapshot-upload-inspection-cycle-v1' OR
     NEW.cycle_sequence IS DISTINCT FROM expected_sequence OR
     NEW.member_count IS DISTINCT FROM member_count OR
     NEW.membership_sha256 IS DISTINCT FROM membership_sha OR
     NEW.inspection_cycle_id IS DISTINCT FROM expected_cycle_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload inspection cycle is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_inspection_cycle_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_upload_inspection_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_upload_inspection_cycle_insert();

CREATE OR REPLACE FUNCTION oracle_css_populate_upload_inspection_cycle()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO oracle_candidate_source_snapshot_upload_inspection_cycle_members (
    inspection_cycle_id, plan_id, domain, remote_object_key,
    uncertainty_kind, source_request_id, source_attempt_id, expected_sha256,
    expected_cid, expected_bytes
  )
  SELECT NEW.inspection_cycle_id, row.plan_id, row.domain,
         row.remote_object_key, row.uncertainty_kind, row.source_request_id,
         row.source_attempt_id, row.expected_sha256, row.expected_cid,
         row.expected_bytes
  FROM oracle_css_current_upload_cycle_uncertain_rows(NEW.plan_id) row
  ORDER BY row.domain, row.remote_object_key;
  IF (SELECT count(*)
      FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      WHERE member.inspection_cycle_id = NEW.inspection_cycle_id) IS DISTINCT FROM
     NEW.member_count THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload inspection membership drifted';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_inspection_cycle_populate_members
  AFTER INSERT ON oracle_candidate_source_snapshot_upload_inspection_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_css_populate_upload_inspection_cycle();

CREATE OR REPLACE FUNCTION oracle_css_freeze_upload_inspection_cycle(
  checked_plan_id text,
  checked_resume_authorization_id text,
  checked_lease_id text,
  checked_generation integer,
  checked_created_at timestamptz
)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  existing_cycle_id text;
  member_count integer;
  membership_sha text;
  next_sequence integer;
  cycle_id text;
BEGIN
  PERFORM plan_id
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = checked_plan_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate source-snapshot inspection plan is absent';
  END IF;
  PERFORM oracle_css_assert_active_executor_lease(
    checked_lease_id, checked_generation,
    checked_resume_authorization_id
  );
  SELECT count(*)::integer,
         oracle_css_upload_cycle_uncertain_set_sha256(checked_plan_id)
  INTO member_count, membership_sha
  FROM oracle_css_current_upload_cycle_uncertain_rows(checked_plan_id);
  IF member_count < 1 THEN
    SELECT cycle.inspection_cycle_id INTO existing_cycle_id
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      ON member.inspection_cycle_id = cycle.inspection_cycle_id
    WHERE cycle.plan_id = checked_plan_id
      AND cycle.resume_authorization_id = checked_resume_authorization_id
      AND cycle.executor_lease_id = checked_lease_id
      AND cycle.lease_generation = checked_generation
      AND NOT oracle_css_upload_inspection_cycle_is_reconciled(
        cycle.inspection_cycle_id
      )
    ORDER BY cycle.cycle_sequence DESC
    LIMIT 1;
    IF existing_cycle_id IS NULL THEN
      RAISE EXCEPTION
        'candidate source-snapshot upload inspection has no durable uncertainty';
    END IF;
    RETURN existing_cycle_id;
  END IF;
  SELECT COALESCE(max(cycle_sequence), 0) + 1
  INTO next_sequence
  FROM oracle_candidate_source_snapshot_upload_inspection_cycles
  WHERE plan_id = checked_plan_id AND lease_generation = checked_generation;
  cycle_id := 'snapshotdemoinspectioncycle_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-inspection-cycle-v1', checked_plan_id,
      checked_resume_authorization_id, checked_generation::text,
      next_sequence::text, membership_sha
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  INSERT INTO oracle_candidate_source_snapshot_upload_inspection_cycles (
    inspection_cycle_id, cycle_version, plan_id, resume_authorization_id,
    executor_lease_id, lease_generation, cycle_sequence, member_count,
    membership_sha256, created_at
  ) VALUES (
    cycle_id, 'candidate-source-snapshot-upload-inspection-cycle-v1',
    checked_plan_id, checked_resume_authorization_id, checked_lease_id,
    checked_generation, next_sequence, member_count, membership_sha,
    checked_created_at
  );
  RETURN cycle_id;
END;
$$;

CREATE TRIGGER oracle_css_upload_inspection_cycle_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_inspection_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_upload_inspection_cycle_member_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_inspection_cycle_members
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_upload_inspection_cycle_resolution_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

-- Preserve all old legal transitions and add only the exact, immutable
-- no-PUT recovery event as a reason to return an admitted object to pending.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_object()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.logical_object_key IS DISTINCT FROM NEW.logical_object_key OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.expected_sha256 IS DISTINCT FROM NEW.expected_sha256 OR
     OLD.expected_cid IS DISTINCT FROM NEW.expected_cid OR
     OLD.expected_bytes IS DISTINCT FROM NEW.expected_bytes THEN
    RAISE EXCEPTION 'candidate source-snapshot object identity is immutable';
  END IF;
  IF OLD.status = 'verified' AND OLD IS DISTINCT FROM NEW THEN
    RAISE EXCEPTION 'verified candidate source-snapshot effect is immutable';
  END IF;
  IF OLD.revision + 1 IS DISTINCT FROM NEW.revision THEN
    RAISE EXCEPTION 'candidate source-snapshot object revision is invalid';
  END IF;
  IF NOT (
    OLD.status = NEW.status OR
    (OLD.status = 'pending' AND NEW.status = 'admitted') OR
    (OLD.status = 'admitted' AND NEW.status IN (
      'outcome_unknown', 'verified', 'failed_terminal'
    )) OR
    (OLD.status = 'outcome_unknown' AND NEW.status IN (
      'admitted', 'verified', 'failed_terminal'
    )) OR
    (OLD.status IN ('admitted', 'outcome_unknown') AND
      NEW.status = 'pending' AND (
        EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations reconciliation
          WHERE reconciliation.plan_id = OLD.plan_id
            AND reconciliation.domain = OLD.domain
            AND reconciliation.remote_object_key = OLD.remote_object_key
            AND reconciliation.result = 'conclusively_absent'
        ) OR EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_admitted_recovery_events recovery
          WHERE recovery.plan_id = OLD.plan_id
            AND recovery.domain = OLD.domain
            AND recovery.remote_object_key = OLD.remote_object_key
            AND recovery.disposition = 'returned_pending_no_put'
        ) OR EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          WHERE resolution.plan_id = OLD.plan_id
            AND resolution.domain = OLD.domain
            AND resolution.remote_object_key = OLD.remote_object_key
            AND resolution.result = 'conclusively_absent'
        )
      ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot object transition';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_resume_binding(
  checked_plan_id text,
  checked_plan_sha256 text,
  checked_amended_commit_sha text,
  checked_persistent_executor_enabled boolean
)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  authorization_row
    oracle_candidate_source_snapshot_upload_continuation_authorizations%ROWTYPE;
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  verified_count integer;
  verified_bytes bigint;
  recovery_count integer;
  future_count integer;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = checked_plan_id AND plan_sha256 = checked_plan_sha256;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = checked_plan_id;
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
  WHERE plan_id = checked_plan_id;
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE plan_id = checked_plan_id
  ORDER BY lease_epoch DESC
  LIMIT 1;
  SELECT count(*)::integer,
         COALESCE(sum(expected_bytes), 0)::bigint
  INTO verified_count, verified_bytes
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = checked_plan_id AND status = 'verified';
  SELECT count(*)::integer INTO recovery_count
  FROM oracle_candidate_source_snapshot_admitted_recovery_events
  WHERE plan_id = checked_plan_id;
  SELECT count(*)::integer INTO future_count
  FROM oracle_candidate_source_snapshot_admitted_recovery_events
  WHERE plan_id = checked_plan_id AND disposition = 'inspection_required';

  IF checked_persistent_executor_enabled IS DISTINCT FROM false OR
     checked_amended_commit_sha !~ '^[a-f0-9]{40}$' OR
     checked_amended_commit_sha IS NOT DISTINCT FROM
       authorization_row.amended_implementation_commit_sha OR
     plan_row.plan_version IS DISTINCT FROM '2.1.0' OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     recovery_count < 1 OR
     EXISTS (
       SELECT 1 FROM oracle_css_current_admitted_recovery_rows(checked_plan_id)
     ) OR
     lease_row.expires_at + interval '30 seconds' > now() OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_objects object
       WHERE object.plan_id = checked_plan_id
         AND object.status = 'failed_terminal'
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
       WHERE attempt.plan_id = checked_plan_id
         AND attempt.outcome IN ('provider_cid_mismatch', 'terminal_failure')
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = checked_plan_id
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = checked_plan_id
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload resume binding is not ready';
  END IF;

  RETURN jsonb_build_object(
    'amendedImplementationCommitSha', checked_amended_commit_sha,
    'checkpoint', jsonb_build_object(
      'admittedRecoveryCount', recovery_count,
      'admittedRecoverySetSha256',
        oracle_css_recorded_admitted_recovery_set_sha256(checked_plan_id),
      'futureInspectionCycleCount', future_count,
      'futureInspectionCycleSetSha256',
        oracle_css_future_inspection_set_sha256(checked_plan_id),
      'verifiedBytes', verified_bytes,
      'verifiedObjectCount', verified_count,
      'verifiedReceiptSetSha256',
        oracle_css_verified_receipt_set_sha256(checked_plan_id)
    ),
    'execution', authorization_row.authorization_binding->'execution' ||
      jsonb_build_object(
        'leaseExpiryGraceMs', 30000,
        'persistentExecutorEnabled', false
      ),
    'inventory', authorization_row.authorization_binding->'inventory',
    'lease', jsonb_build_object(
      'predecessorLeaseGeneration', lease_row.lease_epoch,
      'predecessorLeaseId', lease_row.lease_id,
      'resumeLeaseGeneration', lease_row.lease_epoch + 1
    ),
    'plan', authorization_row.authorization_binding->'plan',
    'predecessor', jsonb_build_object(
      'authorizationId', authorization_row.authorization_id,
      'authorizationSha256', authorization_row.authorization_sha256,
      'implementationCommitSha',
        authorization_row.amended_implementation_commit_sha
    ),
    'remainingAllowance', jsonb_build_object(
      'absoluteRequestCeiling', plan_row.maximum_request_count,
      'costEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.cost_envelope), 'UTF8'
      )), 'hex'),
      'hardBudgetCeilingUsd', to_char(
        plan_row.budget_limit_usd, 'FM999999999999990.000000000000'
      ),
      'hardBudgetRemainingUsd', to_char(
        plan_row.budget_limit_usd - accounting_row.request_cost_usd,
        'FM999999999999990.000000000000'
      ),
      'requestEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.request_envelope), 'UTF8'
      )), 'hex'),
      'requestsRemaining',
        plan_row.maximum_request_count - accounting_row.request_count
    ),
    'schemaVersion', 'candidate-source-snapshot-upload-resume-binding-v1',
    'targetsSha256', authorization_row.authorization_binding->>'targetsSha256'
  );
END;
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_resume_statement(
  checked_binding jsonb,
  checked_authorizer_reference text,
  checked_authorized_at_iso text
)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
  RETURN
    'I authorize exactly one fail-closed candidate-owned source-snapshot upload resume for plan ' ||
    (checked_binding->'plan'->>'planId') || ', logical SHA-256 ' ||
    (checked_binding->'plan'->>'planSha256') || ', at durable plan revision ' ||
    (checked_binding->'plan'->>'planRevision') || ', from continuation authorization ' ||
    (checked_binding->'predecessor'->>'authorizationId') || ', SHA-256 ' ||
    (checked_binding->'predecessor'->>'authorizationSha256') ||
    ', and implementation commit ' ||
    (checked_binding->'predecessor'->>'implementationCommitSha') ||
    ' to amended implementation commit ' ||
    (checked_binding->>'amendedImplementationCommitSha') ||
    '. It preserves ' ||
    (checked_binding->'checkpoint'->>'verifiedObjectCount') ||
    ' verified objects and ' ||
    (checked_binding->'checkpoint'->>'verifiedBytes') ||
    ' verified bytes under receipt-set SHA-256 ' ||
    (checked_binding->'checkpoint'->>'verifiedReceiptSetSha256') ||
    '; it binds ' ||
    (checked_binding->'checkpoint'->>'admittedRecoveryCount') ||
    ' admitted-object recovery records under SHA-256 ' ||
    (checked_binding->'checkpoint'->>'admittedRecoverySetSha256') ||
    ' and ' ||
    (checked_binding->'checkpoint'->>'futureInspectionCycleCount') ||
    ' future inspection-cycle members under SHA-256 ' ||
    (checked_binding->'checkpoint'->>'futureInspectionCycleSetSha256') ||
    '. It supersedes expired executor lease ' ||
    (checked_binding->'lease'->>'predecessorLeaseId') || ' generation ' ||
    (checked_binding->'lease'->>'predecessorLeaseGeneration') ||
    ' with generation ' ||
    (checked_binding->'lease'->>'resumeLeaseGeneration') ||
    ' only after ' || (checked_binding->'execution'->>'leaseExpiryGraceMs') ||
    ' ms expiry grace while the persistent executor flag is false. It retains compiled S3 endpoint ' ||
    (checked_binding->'execution'->>'s3Endpoint') ||
    ', connection timeout ' ||
    (checked_binding->'execution'->>'connectionTimeoutMs') ||
    ' ms, socket timeout ' ||
    (checked_binding->'execution'->>'socketTimeoutMs') ||
    ' ms, request timeout ' ||
    (checked_binding->'execution'->>'requestTimeoutMs') ||
    ' ms, immutable-buffer threshold ' ||
    (checked_binding->'execution'->>'bufferBodyMaxBytes') ||
    ' bytes, staged concurrency/maxSockets 4 then 8 then 16 after ' ||
    (checked_binding->'execution'->>'promotionVerifiedObjectsPerStage') ||
    ' newly verified objects per promotion, exactly one executor lease, inventory CID ' ||
    (checked_binding->'inventory'->>'inventoryCid') ||
    ', inventory SHA-256 ' ||
    (checked_binding->'inventory'->>'fullInventorySha256') || ', exactly ' ||
    (checked_binding->'inventory'->>'exactObjectCount') || ' objects and ' ||
    (checked_binding->'inventory'->>'exactTotalBytes') ||
    ' bytes, targets SHA-256 ' || (checked_binding->>'targetsSha256') ||
    ', request-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'requestEnvelopeSha256') ||
    ', cost-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'costEnvelopeSha256') ||
    ', absolute request ceiling ' ||
    (checked_binding->'remainingAllowance'->>'absoluteRequestCeiling') ||
    ', and USD ' ||
    (checked_binding->'remainingAllowance'->>'hardBudgetCeilingUsd') ||
    ' hard spending ceiling, with ' ||
    (checked_binding->'remainingAllowance'->>'requestsRemaining') ||
    ' requests and USD ' ||
    (checked_binding->'remainingAllowance'->>'hardBudgetRemainingUsd') ||
    ' hard-budget allowance remaining at authorization. No object, CID, key, bucket, prefix, IPNS identity, target, request ceiling, cost ceiling, or existing verified receipt may change; no IPNS operation is authorized. Human authorization reference ' ||
    checked_authorizer_reference || ' at ' || checked_authorized_at_iso || '.';
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_resume_authorization_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_binding jsonb;
  expected_binding_sha text;
  expected_statement text;
  expected_statement_sha text;
  expected_authorization_sha text;
  expected_authorization_id text;
  expected_payload jsonb;
BEGIN
  expected_binding := oracle_css_upload_resume_binding(
    NEW.plan_id, NEW.plan_sha256, NEW.amended_implementation_commit_sha,
    false
  );
  expected_binding_sha := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_binding), 'UTF8'
  )), 'hex');
  expected_statement := oracle_css_upload_resume_statement(
    expected_binding, NEW.authorizer_reference, NEW.authorized_at_iso
  );
  expected_statement_sha := encode(sha256(convert_to(
    expected_statement, 'UTF8'
  )), 'hex');
  expected_authorization_sha := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'authorizationBinding', expected_binding,
      'authorizationBindingSha256', expected_binding_sha,
      'authorizationStatement', expected_statement,
      'authorizationStatementSha256', expected_statement_sha,
      'authorizationVersion',
        'candidate-source-snapshot-upload-resume-authorization-v1',
      'authorizedAt', NEW.authorized_at_iso,
      'authorizerReference', NEW.authorizer_reference
    )), 'UTF8'
  )), 'hex');
  expected_authorization_id := 'snapshotdemouploadresume_' || substr(encode(
    sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-resume-authorization-v1',
      NEW.plan_id, expected_binding->'predecessor'->>'authorizationId',
      expected_binding->'lease'->>'resumeLeaseGeneration',
      expected_authorization_sha
    ])), 'UTF8')), 'hex'), 1, 32);
  expected_payload := jsonb_build_object(
    'authorizationBinding', expected_binding,
    'authorizationBindingSha256', expected_binding_sha,
    'authorizationId', expected_authorization_id,
    'authorizationSha256', expected_authorization_sha,
    'authorizationStatement', expected_statement,
    'authorizationStatementSha256', expected_statement_sha,
    'authorizationVersion',
      'candidate-source-snapshot-upload-resume-authorization-v1',
    'authorizedAt', NEW.authorized_at_iso,
    'authorizerReference', NEW.authorizer_reference
  );
  IF NEW.authorization_version IS DISTINCT FROM
       'candidate-source-snapshot-upload-resume-authorization-v1' OR
     NEW.authorization_sha256 IS DISTINCT FROM expected_authorization_sha OR
     NEW.resume_authorization_id IS DISTINCT FROM expected_authorization_id OR
     NEW.plan_revision IS DISTINCT FROM
       (expected_binding->'plan'->>'planRevision')::integer OR
     NEW.predecessor_authorization_id IS DISTINCT FROM
       expected_binding->'predecessor'->>'authorizationId' OR
     NEW.predecessor_authorization_sha256 IS DISTINCT FROM
       expected_binding->'predecessor'->>'authorizationSha256' OR
     NEW.predecessor_lease_id IS DISTINCT FROM
       expected_binding->'lease'->>'predecessorLeaseId' OR
     NEW.predecessor_lease_generation IS DISTINCT FROM
       (expected_binding->'lease'->>'predecessorLeaseGeneration')::integer OR
     NEW.resume_lease_generation IS DISTINCT FROM
       (expected_binding->'lease'->>'resumeLeaseGeneration')::integer OR
     NEW.verified_object_count IS DISTINCT FROM
       (expected_binding->'checkpoint'->>'verifiedObjectCount')::integer OR
     NEW.verified_bytes IS DISTINCT FROM
       (expected_binding->'checkpoint'->>'verifiedBytes')::bigint OR
     NEW.verified_receipt_set_sha256 IS DISTINCT FROM
       expected_binding->'checkpoint'->>'verifiedReceiptSetSha256' OR
     NEW.admitted_recovery_count IS DISTINCT FROM
       (expected_binding->'checkpoint'->>'admittedRecoveryCount')::integer OR
     NEW.admitted_recovery_set_sha256 IS DISTINCT FROM
       expected_binding->'checkpoint'->>'admittedRecoverySetSha256' OR
     NEW.future_inspection_cycle_count IS DISTINCT FROM
       (expected_binding->'checkpoint'->>'futureInspectionCycleCount')::integer OR
     NEW.future_inspection_cycle_set_sha256 IS DISTINCT FROM
       expected_binding->'checkpoint'->>'futureInspectionCycleSetSha256' OR
     NEW.authorization_binding IS DISTINCT FROM expected_binding OR
     NEW.authorization_binding_sha256 IS DISTINCT FROM expected_binding_sha OR
     NEW.authorization_statement IS DISTINCT FROM expected_statement OR
     NEW.authorization_statement_sha256 IS DISTINCT FROM expected_statement_sha OR
     NEW.authorization_payload IS DISTINCT FROM expected_payload OR
     NEW.authorized_at_iso::timestamptz IS DISTINCT FROM NEW.authorized_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload resume authorization is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_resume_authorization_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_upload_resume_authorizations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_upload_resume_authorization_insert();
CREATE TRIGGER oracle_css_upload_resume_authorization_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_resume_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_css_executor_lease_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  continuation_row
    oracle_candidate_source_snapshot_upload_continuation_authorizations%ROWTYPE;
  resume_row
    oracle_candidate_source_snapshot_upload_resume_authorizations%ROWTYPE;
  predecessor_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  expected_lease_id text;
BEGIN
  SELECT * INTO STRICT continuation_row
  FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
  WHERE authorization_id = NEW.authorization_id
  FOR SHARE;
  IF NEW.lease_version = 'candidate-source-snapshot-executor-lease-v1' THEN
    expected_lease_id := 'snapshotdemoexecutorlease_' || substr(encode(sha256(
      convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
        'candidate-source-snapshot-executor-lease-v1',
        continuation_row.plan_id, continuation_row.authorization_id
      ])), 'UTF8')
    ), 'hex'), 1, 32);
    IF NEW.resume_authorization_id IS NOT NULL OR NEW.lease_epoch <> 1 THEN
      RAISE EXCEPTION
        'candidate source-snapshot executor lease binding is invalid';
    END IF;
  ELSE
    SELECT * INTO STRICT resume_row
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations
    WHERE resume_authorization_id = NEW.resume_authorization_id
    FOR SHARE;
    SELECT * INTO STRICT predecessor_row
    FROM oracle_candidate_source_snapshot_executor_leases
    WHERE lease_id = resume_row.predecessor_lease_id
    FOR UPDATE;
    expected_lease_id := 'snapshotdemoexecutorlease_' || substr(encode(sha256(
      convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
        'candidate-source-snapshot-executor-lease-v2', resume_row.plan_id,
        resume_row.resume_authorization_id,
        resume_row.resume_lease_generation::text
      ])), 'UTF8')
    ), 'hex'), 1, 32);
    IF NEW.lease_version IS DISTINCT FROM
         'candidate-source-snapshot-executor-lease-v2' OR
       NEW.authorization_id IS DISTINCT FROM
         resume_row.predecessor_authorization_id OR
       NEW.plan_id IS DISTINCT FROM resume_row.plan_id OR
       NEW.lease_epoch IS DISTINCT FROM resume_row.resume_lease_generation OR
       predecessor_row.lease_epoch IS DISTINCT FROM
         resume_row.predecessor_lease_generation OR
       predecessor_row.lease_epoch IS DISTINCT FROM
         oracle_css_active_executor_lease_generation(NEW.plan_id) OR
       predecessor_row.phase = 'released' OR
       predecessor_row.expires_at + interval '30 seconds' > now() OR
       NEW.acquired_at < predecessor_row.expires_at + interval '30 seconds' THEN
      RAISE EXCEPTION
        'candidate source-snapshot executor lease is not safely supersedable';
    END IF;
  END IF;
  IF NEW.lease_id IS DISTINCT FROM expected_lease_id OR
     NEW.plan_id IS DISTINCT FROM continuation_row.plan_id OR
     NEW.phase IS DISTINCT FROM 'reconciling' OR
     NEW.effective_concurrency IS DISTINCT FROM 0 OR
     NEW.revision IS DISTINCT FROM 1 OR
     NEW.heartbeat_at IS DISTINCT FROM NEW.acquired_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_executor_lease_update()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  valid_stage_transition boolean;
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.lease_id IS DISTINCT FROM NEW.lease_id OR
     OLD.lease_version IS DISTINCT FROM NEW.lease_version OR
     OLD.authorization_id IS DISTINCT FROM NEW.authorization_id OR
     OLD.resume_authorization_id IS DISTINCT FROM NEW.resume_authorization_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.holder_token_sha256 IS DISTINCT FROM NEW.holder_token_sha256 OR
     OLD.lease_epoch IS DISTINCT FROM NEW.lease_epoch OR
     OLD.lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(OLD.plan_id) OR
     OLD.acquired_at IS DISTINCT FROM NEW.acquired_at OR
     OLD.revision + 1 IS DISTINCT FROM NEW.revision OR
     OLD.phase = 'released' THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease identity is immutable or fenced';
  END IF;
  valid_stage_transition :=
    (OLD.phase = NEW.phase AND
      OLD.effective_concurrency = NEW.effective_concurrency) OR
    (OLD.phase = 'reconciling' AND NEW.phase = 'upload_4' AND
      NEW.effective_concurrency = 4 AND
      CASE WHEN OLD.resume_authorization_id IS NULL
        THEN oracle_css_upload_continuation_is_reconciled(OLD.authorization_id)
        ELSE oracle_css_upload_resume_admission_is_reconciled(
          OLD.resume_authorization_id
        )
      END) OR
    (OLD.phase = 'upload_4' AND NEW.phase = 'upload_8' AND
      NEW.effective_concurrency = 8) OR
    (OLD.phase = 'upload_8' AND NEW.phase = 'upload_16' AND
      NEW.effective_concurrency = 16) OR
    (NEW.phase = 'released' AND NEW.effective_concurrency = 0);
  IF NOT valid_stage_transition OR
     NEW.heartbeat_at < OLD.heartbeat_at OR
     NEW.expires_at <= NEW.heartbeat_at OR
     NEW.expires_at > NEW.heartbeat_at + interval '5 minutes' THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease transition is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_css_record_lease_supersession()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  resume_row
    oracle_candidate_source_snapshot_upload_resume_authorizations%ROWTYPE;
  predecessor_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  expected_event_sha text;
  expected_event_id text;
BEGIN
  IF NEW.lease_version <> 'candidate-source-snapshot-executor-lease-v2' THEN
    RETURN NEW;
  END IF;
  SELECT * INTO STRICT resume_row
  FROM oracle_candidate_source_snapshot_upload_resume_authorizations
  WHERE resume_authorization_id = NEW.resume_authorization_id;
  SELECT * INTO STRICT predecessor_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = resume_row.predecessor_lease_id;
  expected_event_sha := encode(sha256(convert_to(oracle_canonical_jsonb(
    jsonb_build_object(
      'expiryGraceMs', 30000,
      'holderTokenSha256', NEW.holder_token_sha256,
      'planId', NEW.plan_id,
      'predecessorExpiresAt', to_char(
        predecessor_row.expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
      ),
      'predecessorLeaseGeneration', predecessor_row.lease_epoch,
      'predecessorLeaseId', predecessor_row.lease_id,
      'resumeAuthorizationId', resume_row.resume_authorization_id,
      'schemaVersion',
        'candidate-source-snapshot-executor-lease-supersession-v1',
      'successorLeaseGeneration', NEW.lease_epoch,
      'successorLeaseId', NEW.lease_id,
      'supersededAt', to_char(
        NEW.acquired_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
      )
    )
  ), 'UTF8')), 'hex');
  expected_event_id := 'snapshotdemoleasesupersession_' || substr(encode(
    sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-executor-lease-supersession-v1',
      NEW.plan_id, predecessor_row.lease_id, NEW.lease_id,
      expected_event_sha
    ])), 'UTF8')), 'hex'), 1, 32);
  INSERT INTO oracle_candidate_source_snapshot_executor_lease_supersession_events (
    supersession_event_id, supersession_version, plan_id,
    resume_authorization_id, predecessor_lease_id,
    predecessor_lease_generation, successor_lease_id,
    successor_lease_generation, predecessor_expires_at, expiry_grace_ms,
    superseded_at, holder_token_sha256, event_sha256
  ) VALUES (
    expected_event_id,
    'candidate-source-snapshot-executor-lease-supersession-v1', NEW.plan_id,
    resume_row.resume_authorization_id, predecessor_row.lease_id,
    predecessor_row.lease_epoch, NEW.lease_id, NEW.lease_epoch,
    predecessor_row.expires_at, 30000, NEW.acquired_at,
    NEW.holder_token_sha256, expected_event_sha
  );
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_lease_supersession_event_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  resume_row
    oracle_candidate_source_snapshot_upload_resume_authorizations%ROWTYPE;
  predecessor_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  successor_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  expected_event_sha text;
  expected_event_id text;
BEGIN
  SELECT * INTO STRICT resume_row
  FROM oracle_candidate_source_snapshot_upload_resume_authorizations
  WHERE resume_authorization_id = NEW.resume_authorization_id;
  SELECT * INTO STRICT predecessor_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.predecessor_lease_id;
  SELECT * INTO STRICT successor_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.successor_lease_id;
  expected_event_sha := encode(sha256(convert_to(oracle_canonical_jsonb(
    jsonb_build_object(
      'expiryGraceMs', 30000,
      'holderTokenSha256', successor_row.holder_token_sha256,
      'planId', successor_row.plan_id,
      'predecessorExpiresAt', to_char(
        predecessor_row.expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
      ),
      'predecessorLeaseGeneration', predecessor_row.lease_epoch,
      'predecessorLeaseId', predecessor_row.lease_id,
      'resumeAuthorizationId', resume_row.resume_authorization_id,
      'schemaVersion',
        'candidate-source-snapshot-executor-lease-supersession-v1',
      'successorLeaseGeneration', successor_row.lease_epoch,
      'successorLeaseId', successor_row.lease_id,
      'supersededAt', to_char(
        successor_row.acquired_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
      )
    )
  ), 'UTF8')), 'hex');
  expected_event_id := 'snapshotdemoleasesupersession_' || substr(encode(
    sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-executor-lease-supersession-v1',
      successor_row.plan_id, predecessor_row.lease_id,
      successor_row.lease_id, expected_event_sha
    ])), 'UTF8')), 'hex'), 1, 32);
  IF NEW.supersession_event_id IS DISTINCT FROM expected_event_id OR
     NEW.supersession_version IS DISTINCT FROM
       'candidate-source-snapshot-executor-lease-supersession-v1' OR
     NEW.plan_id IS DISTINCT FROM successor_row.plan_id OR
     NEW.predecessor_lease_generation IS DISTINCT FROM
       predecessor_row.lease_epoch OR
     NEW.successor_lease_generation IS DISTINCT FROM
       successor_row.lease_epoch OR
     NEW.predecessor_expires_at IS DISTINCT FROM predecessor_row.expires_at OR
     NEW.expiry_grace_ms IS DISTINCT FROM 30000 OR
     NEW.superseded_at IS DISTINCT FROM successor_row.acquired_at OR
     NEW.holder_token_sha256 IS DISTINCT FROM
       successor_row.holder_token_sha256 OR
     NEW.event_sha256 IS DISTINCT FROM expected_event_sha OR
     successor_row.resume_authorization_id IS DISTINCT FROM
       resume_row.resume_authorization_id OR
     resume_row.predecessor_lease_id IS DISTINCT FROM predecessor_row.lease_id
  THEN
    RAISE EXCEPTION
      'candidate source-snapshot lease supersession event is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_executor_lease_supersession_event
  AFTER INSERT ON oracle_candidate_source_snapshot_executor_leases
  FOR EACH ROW EXECUTE FUNCTION oracle_css_record_lease_supersession();
CREATE TRIGGER oracle_css_executor_lease_supersession_insert_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_executor_lease_supersession_events
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_lease_supersession_event_insert();
CREATE TRIGGER oracle_css_executor_lease_supersession_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_executor_lease_supersession_events
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_inspection_cycle_member_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  cycle_row oracle_candidate_source_snapshot_upload_inspection_cycles%ROWTYPE;
  expected record;
BEGIN
  SELECT * INTO STRICT cycle_row
  FROM oracle_candidate_source_snapshot_upload_inspection_cycles
  WHERE inspection_cycle_id = NEW.inspection_cycle_id;
  SELECT * INTO STRICT expected
  FROM oracle_css_current_upload_cycle_uncertain_rows(NEW.plan_id) row
  WHERE row.domain = NEW.domain
    AND row.remote_object_key = NEW.remote_object_key;
  IF cycle_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     NEW.uncertainty_kind IS DISTINCT FROM expected.uncertainty_kind OR
     NEW.source_request_id IS DISTINCT FROM expected.source_request_id OR
     NEW.source_attempt_id IS DISTINCT FROM expected.source_attempt_id OR
     NEW.expected_sha256 IS DISTINCT FROM expected.expected_sha256 OR
     NEW.expected_cid IS DISTINCT FROM expected.expected_cid OR
     NEW.expected_bytes IS DISTINCT FROM expected.expected_bytes THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection member is not durable uncertainty';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_inspection_cycle_member_insert_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_upload_inspection_cycle_members
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_upload_inspection_cycle_member_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_cycle_resolution_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  member_row
    oracle_candidate_source_snapshot_upload_inspection_cycle_members%ROWTYPE;
  cycle_row oracle_candidate_source_snapshot_upload_inspection_cycles%ROWTYPE;
  inspection_row oracle_candidate_source_snapshot_demo_inspections%ROWTYPE;
  attempt_row
    oracle_candidate_source_snapshot_demo_inspection_attempts%ROWTYPE;
BEGIN
  SELECT * INTO STRICT member_row
  FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members
  WHERE inspection_cycle_id = NEW.inspection_cycle_id
    AND domain = NEW.domain AND remote_object_key = NEW.remote_object_key;
  SELECT * INTO STRICT cycle_row
  FROM oracle_candidate_source_snapshot_upload_inspection_cycles
  WHERE inspection_cycle_id = NEW.inspection_cycle_id;
  SELECT * INTO STRICT inspection_row
  FROM oracle_candidate_source_snapshot_demo_inspections
  WHERE inspection_id = NEW.inspection_id;
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_demo_inspection_attempts
  WHERE inspection_id = NEW.inspection_id;
  PERFORM oracle_css_assert_active_executor_lease(
    cycle_row.executor_lease_id, cycle_row.lease_generation,
    cycle_row.resume_authorization_id
  );
  IF member_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     attempt_row.recovery_upload_attempt_id IS DISTINCT FROM
       member_row.source_attempt_id OR
     inspection_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     inspection_row.domain IS DISTINCT FROM NEW.domain OR
     inspection_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     inspection_row.receipt_sha256 IS DISTINCT FROM NEW.receipt_sha256 OR
     NOT (
       (NEW.result = 'remote_verified' AND
         inspection_row.outcome = 'verified' AND
         inspection_row.observed_cid = member_row.expected_cid AND
         inspection_row.observed_sha256 = member_row.expected_sha256 AND
         inspection_row.observed_bytes = member_row.expected_bytes) OR
       (NEW.result = 'conclusively_absent' AND
         inspection_row.outcome = 'absent')
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload inspection resolution is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_cycle_resolution_insert_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_upload_cycle_resolution_insert();

CREATE OR REPLACE FUNCTION oracle_css_finalize_upload_cycle_resolution()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.result = 'conclusively_absent' THEN
    UPDATE oracle_candidate_source_snapshot_demo_objects
    SET status = 'pending', revision = revision + 1, updated_at = now()
    WHERE plan_id = NEW.plan_id AND domain = NEW.domain
      AND remote_object_key = NEW.remote_object_key
      AND status IN ('admitted', 'outcome_unknown');
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'candidate source-snapshot upload cycle resolution lost its object';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_finalize_upload_cycle_resolution
  AFTER INSERT
  ON oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions
  FOR EACH ROW EXECUTE FUNCTION oracle_css_finalize_upload_cycle_resolution();

-- A verified HEAD checkpoint and its cycle resolution are committed together
-- by the application, but closure remains independently fail-closed if a
-- crash or direct trusted-service retry ever leaves that pair incomplete.
CREATE OR REPLACE FUNCTION oracle_guard_css_upload_closure_quarantine()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      ON member.inspection_cycle_id = cycle.inspection_cycle_id
    LEFT JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
      ON resolution.inspection_cycle_id = member.inspection_cycle_id
     AND resolution.domain = member.domain
     AND resolution.remote_object_key = member.remote_object_key
    WHERE cycle.plan_id = NEW.plan_id
      AND resolution.inspection_cycle_id IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM oracle_css_current_upload_cycle_uncertain_rows(NEW.plan_id)
  ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload closure has unresolved quarantine';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_closure_quarantine_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_upload_closures
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_upload_closure_quarantine();

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_continuation_request_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
  active_authorization_id text;
BEGIN
  IF NEW.request_category NOT IN (
       'upload_provider_cid', 'ambiguous_upload_inspection'
     ) OR NEW.executor_lease_id IS NULL THEN
    RETURN NEW;
  END IF;
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.executor_lease_id
  FOR UPDATE;
  active_authorization_id := COALESCE(
    lease_row.resume_authorization_id, lease_row.authorization_id
  );
  PERFORM oracle_css_assert_active_executor_lease(
    NEW.executor_lease_id, NEW.executor_lease_epoch,
    active_authorization_id
  );
  IF lease_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     lease_row.authorization_id IS DISTINCT FROM
       NEW.upload_continuation_authorization_id OR
     lease_row.resume_authorization_id IS DISTINCT FROM
       NEW.upload_resume_authorization_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot request lacks its exact active lease';
  END IF;
  IF NEW.request_category = 'ambiguous_upload_inspection' THEN
    IF lease_row.resume_authorization_id IS NULL THEN
      IF lease_row.phase <> 'reconciling' OR NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties uncertain
        WHERE uncertain.authorization_id = lease_row.authorization_id
          AND uncertain.plan_id = NEW.plan_id
          AND uncertain.domain = NEW.domain
          AND uncertain.remote_object_key = NEW.remote_object_key
      ) THEN
        RAISE EXCEPTION
          'candidate source-snapshot inspection is outside its frozen uncertainty set';
      END IF;
    ELSIF NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
      JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
        ON member.inspection_cycle_id = cycle.inspection_cycle_id
      WHERE cycle.plan_id = NEW.plan_id
        AND cycle.resume_authorization_id = lease_row.resume_authorization_id
        AND cycle.executor_lease_id = lease_row.lease_id
        AND cycle.lease_generation = lease_row.lease_epoch
        AND member.domain = NEW.domain
        AND member.remote_object_key = NEW.remote_object_key
        AND NOT EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          WHERE resolution.inspection_cycle_id = member.inspection_cycle_id
            AND resolution.domain = member.domain
            AND resolution.remote_object_key = member.remote_object_key
        )
    ) THEN
      RAISE EXCEPTION
        'candidate source-snapshot inspection is outside its frozen cycle';
    END IF;
  ELSIF lease_row.phase NOT IN ('upload_4', 'upload_8', 'upload_16') OR
    (lease_row.resume_authorization_id IS NULL AND
      NOT oracle_css_upload_continuation_is_reconciled(
        lease_row.authorization_id
      )) OR
    (lease_row.resume_authorization_id IS NOT NULL AND
      (NOT oracle_css_upload_resume_admission_is_reconciled(
         lease_row.resume_authorization_id
       ) OR EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
         LEFT JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
           ON resolution.inspection_cycle_id = member.inspection_cycle_id
          AND resolution.domain = member.domain
          AND resolution.remote_object_key = member.remote_object_key
         JOIN oracle_candidate_source_snapshot_upload_inspection_cycles cycle
           ON cycle.inspection_cycle_id = member.inspection_cycle_id
         WHERE cycle.resume_authorization_id = lease_row.resume_authorization_id
           AND member.domain = NEW.domain
           AND member.remote_object_key = NEW.remote_object_key
           AND resolution.inspection_cycle_id IS NULL
       ))) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload lacks reconciled active generation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.operation_class IS DISTINCT FROM NEW.operation_class OR
     OLD.operation_kind IS DISTINCT FROM NEW.operation_kind OR
     OLD.request_category IS DISTINCT FROM NEW.request_category OR
     OLD.logical_request_id IS DISTINCT FROM NEW.logical_request_id OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.redirect_sequence IS DISTINCT FROM NEW.redirect_sequence OR
     OLD.continuation_authorization_id IS DISTINCT FROM
       NEW.continuation_authorization_id OR
     OLD.upload_continuation_authorization_id IS DISTINCT FROM
       NEW.upload_continuation_authorization_id OR
     OLD.upload_resume_authorization_id IS DISTINCT FROM
       NEW.upload_resume_authorization_id OR
     OLD.executor_lease_id IS DISTINCT FROM NEW.executor_lease_id OR
     OLD.executor_lease_epoch IS DISTINCT FROM NEW.executor_lease_epoch OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.cycle_sequence IS DISTINCT FROM NEW.cycle_sequence OR
     OLD.resolver IS DISTINCT FROM NEW.resolver OR
     OLD.request_cost_usd IS DISTINCT FROM NEW.request_cost_usd OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION
      'candidate source-snapshot request is immutable or terminal';
  END IF;
  IF OLD.executor_lease_id IS NOT NULL AND
     OLD.executor_lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(OLD.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot request belongs to a fenced generation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.attempt_id IS DISTINCT FROM NEW.attempt_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.request_count IS DISTINCT FROM NEW.request_count OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload attempt identity is immutable';
  END IF;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = OLD.request_id;
  IF request_row.executor_lease_id IS NOT NULL AND
     request_row.executor_lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(OLD.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload attempt belongs to a fenced generation';
  END IF;
  IF request_row.upload_continuation_authorization_id IS NOT NULL AND (
       NEW.transport_stage IS NULL OR
       (NEW.outcome = 'verified' AND (
         NEW.transport_stage <> 'put_object_provider_response' OR
         NEW.failure_class IS NOT NULL
       )) OR
       (NEW.outcome <> 'verified' AND NEW.failure_class IS NULL)
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot continuation attempt lacks fixed transport evidence';
  END IF;
  RETURN NEW;
END;
$$;

-- Inspection admissions and results are executor mutations as well. Recheck
-- the request's generation at each durable boundary so an expired process
-- cannot finish an inspection after a successor lease has fenced it out.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  upload_row oracle_candidate_source_snapshot_demo_upload_attempts%ROWTYPE;
  expected_inspection_sequence integer;
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
     upload_row.outcome NOT IN (
       'connection_failure', 'retryable_http_error', 'timeout_unknown'
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection admission binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  attempt_row oracle_candidate_source_snapshot_demo_inspection_attempts%ROWTYPE;
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  SELECT * INTO STRICT attempt_row
  FROM oracle_candidate_source_snapshot_demo_inspection_attempts
  WHERE inspection_id = NEW.inspection_id
  FOR UPDATE;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = attempt_row.request_id;
  IF request_row.executor_lease_id IS NOT NULL AND
     request_row.executor_lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(NEW.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection result belongs to a fenced generation';
  END IF;
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

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_inspection_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = OLD.request_id;
  IF request_row.executor_lease_id IS NOT NULL AND
     request_row.executor_lease_epoch IS DISTINCT FROM
       oracle_css_active_executor_lease_generation(OLD.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection attempt belongs to a fenced generation';
  END IF;
  IF TG_OP = 'DELETE' OR
     OLD.inspection_id IS DISTINCT FROM NEW.inspection_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.recovery_upload_attempt_id IS DISTINCT FROM
       NEW.recovery_upload_attempt_id OR
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

COMMENT ON TABLE oracle_candidate_source_snapshot_admitted_recovery_events IS
  'Immutable local-only classification of admitted upload rows; only conclusive pre-dispatch failures may return to pending.';
COMMENT ON TABLE oracle_candidate_source_snapshot_upload_inspection_cycles IS
  'Append-only, hash-bound membership for post-continuation upload ambiguity; this table grants no IPNS authority.';
COMMENT ON TABLE oracle_candidate_source_snapshot_executor_lease_supersession_events IS
  'Immutable fencing evidence for one expired lease generation superseded after a commit-bound resume authorization and grace period.';

COMMIT;
