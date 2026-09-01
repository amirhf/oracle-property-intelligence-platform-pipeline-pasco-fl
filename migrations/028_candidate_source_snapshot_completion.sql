-- Additive Session 2A completion gates for the candidate-owned source-snapshot
-- demonstration. This migration records no approval or remote effect and does
-- not enable the executor.

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_upload_closures (
  closure_id text PRIMARY KEY CHECK (
    closure_id ~ '^snapshotdemouploadclosure_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_approvals(approval_id),
  exact_object_count integer NOT NULL CHECK (exact_object_count > 0),
  exact_total_bytes bigint NOT NULL CHECK (exact_total_bytes > 0),
  verified_object_count integer NOT NULL CHECK (verified_object_count > 0),
  verified_total_bytes bigint NOT NULL CHECK (verified_total_bytes > 0),
  unresolved_object_count integer NOT NULL CHECK (unresolved_object_count = 0),
  provider_cid_mismatch_count integer NOT NULL CHECK (
    provider_cid_mismatch_count = 0
  ),
  inventory_root_cid text NOT NULL CHECK (
    inventory_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  inventory_root_sha256 text NOT NULL CHECK (
    inventory_root_sha256 ~ '^[a-f0-9]{64}$'
  ),
  admitted_request_count integer NOT NULL CHECK (
    admitted_request_count BETWEEN 0 AND 1000000
  ),
  admitted_request_cost_usd numeric(18, 12) NOT NULL CHECK (
    admitted_request_cost_usd >= 0 AND admitted_request_cost_usd <= 25
  ),
  closure_sha256 text NOT NULL UNIQUE CHECK (
    closure_sha256 ~ '^[a-f0-9]{64}$'
  ),
  verified_at timestamptz NOT NULL,
  UNIQUE (closure_id, plan_id, plan_sha256, approval_id)
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents
  ) THEN
    RAISE EXCEPTION
      'migration 028 requires zero pre-existing candidate source-snapshot intents';
  END IF;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_ipns_events
  ALTER COLUMN from_state SET NOT NULL,
  ADD COLUMN event_version text NOT NULL CHECK (
    event_version = 'candidate-source-snapshot-intent-transition-v1'
  ),
  ADD COLUMN from_revision integer NOT NULL CHECK (from_revision > 0),
  ADD COLUMN to_revision integer NOT NULL CHECK (
    to_revision = from_revision + 1
  ),
  ADD COLUMN evidence_sha256 text NOT NULL CHECK (
    evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD COLUMN event_payload jsonb NOT NULL,
  ADD COLUMN recorded_at_iso text NOT NULL CHECK (
    recorded_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND recorded_at_iso::timestamptz = recorded_at
  ),
  ADD CONSTRAINT oracle_css_ipns_event_revision_unique
    UNIQUE (intent_id, to_revision);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_ipns_event_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
  state_row oracle_candidate_source_snapshot_demo_ipns_intent_state%ROWTYPE;
  expected_evidence_sha256 text;
  expected_payload jsonb;
  expected_event_sha256 text;
  expected_event_id text;
BEGIN
  SELECT * INTO STRICT intent_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  SELECT * INTO STRICT state_row
  FROM oracle_candidate_source_snapshot_demo_ipns_intent_state
  WHERE intent_id = NEW.intent_id
  FOR UPDATE;

  SELECT encode(sha256(convert_to(oracle_canonical_jsonb(
           coalesce(jsonb_agg(jsonb_build_object(
             'evidenceId', evidence.evidence_id,
             'evidenceKind', evidence.evidence_kind,
             'evidenceSha256', evidence.evidence_sha256,
             'outcome', evidence.outcome
           ) ORDER BY evidence.evidence_kind, evidence.evidence_id), '[]'::jsonb)
         ), 'UTF8')), 'hex')
  INTO expected_evidence_sha256
  FROM (
    SELECT observation.observation_id AS evidence_id,
           'observation'::text AS evidence_kind,
           observation.evidence_sha256,
           observation.classification AS outcome
    FROM oracle_candidate_source_snapshot_demo_ipns_observations observation
    WHERE observation.intent_id = NEW.intent_id
    UNION ALL
    SELECT attempt.attempt_id AS evidence_id,
           'attempt'::text AS evidence_kind,
           coalesce(attempt.receipt_sha256, repeat('0', 64)) AS evidence_sha256,
           attempt.outcome
    FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
    WHERE attempt.intent_id = NEW.intent_id
  ) evidence;

  expected_payload := jsonb_build_object(
    'domain', intent_row.domain,
    'evidenceSha256', expected_evidence_sha256,
    'fromState', NEW.from_state,
    'intentId', NEW.intent_id,
    'planId', intent_row.plan_id,
    'planSha256', intent_row.plan_sha256,
    'revision', NEW.to_revision,
    'toState', NEW.to_state,
    'transitionedAt', NEW.recorded_at_iso
  );
  expected_event_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_event_id := 'snapshotdemoipnsevent_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-intent-transition-v1',
      NEW.intent_id,
      expected_event_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF NEW.event_version IS DISTINCT FROM
       'candidate-source-snapshot-intent-transition-v1' OR
     state_row.state IS DISTINCT FROM NEW.to_state OR
     state_row.revision IS DISTINCT FROM NEW.to_revision OR
     NOT (
       (NEW.from_revision = 1 AND NEW.from_state = 'intent_recorded') OR
       EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_demo_ipns_events previous
         WHERE previous.intent_id = NEW.intent_id
           AND previous.to_revision = NEW.from_revision
           AND previous.to_state = NEW.from_state
       )
     ) OR
     NEW.evidence_sha256 IS DISTINCT FROM expected_evidence_sha256 OR
     NEW.event_payload IS DISTINCT FROM expected_payload OR
     NEW.event_sha256 IS DISTINCT FROM expected_event_sha256 OR
     NEW.event_id IS DISTINCT FROM expected_event_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot IPNS event is not exact durable evidence';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_css_ipns_event_insert_guard
  ON oracle_candidate_source_snapshot_demo_ipns_events;
CREATE TRIGGER oracle_css_ipns_event_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_events
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_ipns_event_insert();

ALTER TABLE oracle_candidate_source_snapshot_demo_ipns_intents
  ADD COLUMN approval_id text NOT NULL,
  ADD COLUMN upload_closure_id text NOT NULL,
  ADD COLUMN resolver_policy text NOT NULL CHECK (
    resolver_policy = 'candidate_source_snapshot_filebase_delegated_v1'
  ),
  ADD COLUMN cutover_position integer NOT NULL CHECK (
    (domain = 'open_data' AND cutover_position = 1) OR
    (domain = 'query_table' AND cutover_position = 2)
  ),
  ADD COLUMN rollback_position integer NOT NULL CHECK (
    (domain = 'query_table' AND rollback_position = 1) OR
    (domain = 'open_data' AND rollback_position = 2)
  ),
  ADD CONSTRAINT oracle_candidate_source_snapshot_intent_closure_fk
    FOREIGN KEY (upload_closure_id, plan_id, plan_sha256, approval_id)
    REFERENCES oracle_candidate_source_snapshot_demo_upload_closures (
      closure_id, plan_id, plan_sha256, approval_id
    );

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_remote_checks (
  check_id text PRIMARY KEY CHECK (
    check_id ~ '^snapshotdemoremotecheck_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  check_kind text NOT NULL CHECK (check_kind IN (
    'plan_artifact', 'manifest', 'inventory', 'open_data_graph',
    'query_table', 'coverage', 'fixture_exclusion'
  )),
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  observed_cid text NOT NULL CHECK (
    observed_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  observed_sha256 text NOT NULL CHECK (observed_sha256 ~ '^[a-f0-9]{64}$'),
  expected_bytes bigint NOT NULL CHECK (expected_bytes > 0),
  observed_bytes bigint NOT NULL CHECK (observed_bytes > 0),
  metrics jsonb NOT NULL,
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  check_payload jsonb NOT NULL,
  check_sha256 text NOT NULL UNIQUE CHECK (check_sha256 ~ '^[a-f0-9]{64}$'),
  checked_at timestamptz NOT NULL,
  checked_at_iso text NOT NULL CHECK (
    checked_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND checked_at_iso::timestamptz = checked_at
  ),
  UNIQUE (plan_id, check_kind)
);

CREATE TABLE IF NOT EXISTS oracle_candidate_source_snapshot_demo_remote_verifications (
  verification_id text PRIMARY KEY CHECK (
    verification_id ~ '^snapshotdemoremoteverification_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_approvals(approval_id),
  upload_closure_id text NOT NULL UNIQUE,
  open_data_root_cid text NOT NULL CHECK (
    open_data_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  query_table_root_cid text NOT NULL CHECK (
    query_table_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  manifest_cid text NOT NULL CHECK (
    manifest_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
  inventory_root_cid text NOT NULL CHECK (
    inventory_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  inventory_root_sha256 text NOT NULL CHECK (
    inventory_root_sha256 ~ '^[a-f0-9]{64}$'
  ),
  query_table_bytes bigint NOT NULL CHECK (query_table_bytes > 0),
  query_table_sha256 text NOT NULL CHECK (
    query_table_sha256 ~ '^[a-f0-9]{64}$'
  ),
  property_count integer NOT NULL CHECK (property_count > 0),
  distinct_property_id_count integer NOT NULL CHECK (
    distinct_property_id_count = property_count
  ),
  null_property_id_count integer NOT NULL CHECK (null_property_id_count = 0),
  property_cid_correspondence boolean NOT NULL CHECK (
    property_cid_correspondence = true
  ),
  graph_traversal_valid boolean NOT NULL CHECK (graph_traversal_valid = true),
  fixture_match_count integer NOT NULL CHECK (fixture_match_count = 0),
  verification_payload jsonb NOT NULL,
  check_set_sha256 text NOT NULL CHECK (
    check_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  verification_sha256 text NOT NULL UNIQUE CHECK (
    verification_sha256 ~ '^[a-f0-9]{64}$'
  ),
  verified_at timestamptz NOT NULL,
  UNIQUE (
    verification_id, plan_id, plan_sha256, approval_id, upload_closure_id
  ),
  FOREIGN KEY (
    upload_closure_id, plan_id, plan_sha256, approval_id
  ) REFERENCES oracle_candidate_source_snapshot_demo_upload_closures (
    closure_id, plan_id, plan_sha256, approval_id
  )
);

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_check()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  object_row oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  expected_cid text;
  expected_sha256 text;
  expected_bytes bigint;
  expected_metrics jsonb;
  expected_payload jsonb;
  expected_check_sha256 text;
  expected_check_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  IF NEW.check_kind = 'plan_artifact' THEN
    expected_cid := plan_row.plan_artifact_cid;
    expected_sha256 := plan_row.plan_artifact_sha256;
    expected_bytes := plan_row.plan_artifact_bytes;
    expected_metrics := '{}'::jsonb;
  ELSIF NEW.check_kind = 'manifest' THEN
    expected_cid := plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'expectedCid';
    expected_sha256 := plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'sha256';
    expected_bytes := (plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'byteSize')::bigint;
    expected_metrics := '{}'::jsonb;
  ELSIF NEW.check_kind = 'inventory' THEN
    expected_cid := plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'expectedCid';
    expected_sha256 := plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'sha256';
    expected_bytes := (plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'byteSize')::bigint;
    expected_metrics := jsonb_build_object(
      'entryCount', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'entryCount',
      'integrityRootSha256', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'integrityRootSha256',
      'shardCount', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'shardCount'
    );
  ELSE
    expected_cid := CASE NEW.check_kind
      WHEN 'query_table' THEN
        plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
      ELSE plan_row.plan_payload->'targets'->'openData'->>'targetCid'
    END;
    SELECT * INTO STRICT object_row
    FROM oracle_candidate_source_snapshot_demo_objects object
    WHERE object.plan_id = NEW.plan_id
      AND object.expected_cid = CASE NEW.check_kind
        WHEN 'query_table' THEN
          plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
        ELSE plan_row.plan_payload->'targets'->'openData'->>'targetCid'
      END
    ORDER BY remote_object_key
    LIMIT 1;
    expected_sha256 := object_row.expected_sha256;
    expected_bytes := object_row.expected_bytes;
    expected_metrics := CASE NEW.check_kind
      WHEN 'open_data_graph' THEN jsonb_build_object(
        'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties',
        'traversalValid', true
      )
      WHEN 'query_table' THEN jsonb_build_object(
        'distinctPropertyIdCount',
          plan_row.plan_payload->'coverage'->'activeProperties',
        'nullPropertyIdCount', 0,
        'propertyCidCorrespondence', true,
        'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties'
      )
      WHEN 'coverage' THEN plan_row.plan_payload->'coverage'
      WHEN 'fixture_exclusion' THEN jsonb_build_object('fixtureMatchCount', 0)
      ELSE NULL
    END;
  END IF;

  expected_payload := jsonb_build_object(
    'checkKind', NEW.check_kind,
    'checkedAt', NEW.checked_at_iso,
    'evidenceSha256', NEW.evidence_sha256,
    'expectedBytes', expected_bytes,
    'expectedCid', expected_cid,
    'expectedSha256', expected_sha256,
    'metrics', expected_metrics,
    'observedBytes', expected_bytes,
    'observedCid', expected_cid,
    'observedSha256', expected_sha256,
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'schemaVersion', 'candidate-source-snapshot-remote-check-v1'
  );
  expected_check_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_check_id := 'snapshotdemoremotecheck_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-remote-check-v1',
      plan_row.plan_id,
      NEW.check_kind,
      expected_check_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF coalesce((plan_row.plan_payload->'requestEnvelope'->
       'successfulExecution'->>'freeOperations')::integer, 0) < 7 OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     (SELECT count(*)
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state state
          ON state.intent_id = intent.intent_id
        WHERE intent.plan_id = NEW.plan_id
          AND state.state = 'verified') IS DISTINCT FROM 2::bigint OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
         AND closure.plan_sha256 = NEW.plan_sha256
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.outcome = 'request_started'
     ) OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.expected_cid IS DISTINCT FROM expected_cid OR
     NEW.observed_cid IS DISTINCT FROM expected_cid OR
     NEW.expected_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.observed_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.expected_bytes IS DISTINCT FROM expected_bytes OR
     NEW.observed_bytes IS DISTINCT FROM expected_bytes OR
     NEW.metrics IS DISTINCT FROM expected_metrics OR
     NEW.check_payload IS DISTINCT FROM expected_payload OR
     NEW.check_sha256 IS DISTINCT FROM expected_check_sha256 OR
     NEW.check_id IS DISTINCT FROM expected_check_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote check is not exact durable evidence';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_candidate_source_snapshot_remote_check_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_remote_checks
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_remote_check();

CREATE TRIGGER oracle_candidate_source_snapshot_remote_check_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_remote_checks
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

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
      )
    );

  expected_payload := jsonb_build_object(
    'admittedRequestCostUsd', accounting_row.request_cost_usd::double precision,
    'admittedRequestCount', accounting_row.request_count,
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
     NEW.admitted_request_count IS DISTINCT FROM accounting_row.request_count OR
     NEW.admitted_request_cost_usd IS DISTINCT FROM accounting_row.request_cost_usd OR
     NEW.closure_sha256 IS DISTINCT FROM expected_closure_sha256 OR
     NEW.closure_id IS DISTINCT FROM expected_closure_id OR
     accounting_row.request_count > plan_row.maximum_request_count OR
     accounting_row.request_cost_usd > plan_row.budget_limit_usd OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.approval_id = NEW.approval_id
         AND approval.plan_id = NEW.plan_id
         AND approval.plan_sha256 = NEW.plan_sha256
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.operation_kind IN ('put_object', 'inspect_object')
         AND request.outcome = 'request_started'
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot upload closure is not exact and complete';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_upload_closure_guard
  ON oracle_candidate_source_snapshot_demo_upload_closures;
CREATE TRIGGER oracle_candidate_source_snapshot_upload_closure_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_upload_closures
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_upload_closure();

CREATE TRIGGER oracle_candidate_source_snapshot_upload_closure_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_upload_closures
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_require_candidate_source_snapshot_upload_closure()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.resolver_policy IS DISTINCT FROM
       'candidate_source_snapshot_filebase_delegated_v1' OR
     NEW.cutover_position IS DISTINCT FROM (CASE NEW.domain
       WHEN 'open_data' THEN 1 WHEN 'query_table' THEN 2 ELSE NULL END) OR
     NEW.rollback_position IS DISTINCT FROM (CASE NEW.domain
       WHEN 'query_table' THEN 1 WHEN 'open_data' THEN 2 ELSE NULL END) OR
     NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_upload_closures closure
    JOIN oracle_candidate_source_snapshot_demo_approvals approval
      ON approval.approval_id = closure.approval_id
    WHERE closure.closure_id = NEW.upload_closure_id
      AND closure.approval_id = NEW.approval_id
      AND closure.plan_id = NEW.plan_id
      AND closure.plan_sha256 = NEW.plan_sha256
      AND approval.plan_id = NEW.plan_id
      AND approval.plan_sha256 = NEW.plan_sha256
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot intents require immutable upload closure';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_intent_upload_closure_guard
  ON oracle_candidate_source_snapshot_demo_ipns_intents;
CREATE TRIGGER oracle_candidate_source_snapshot_intent_upload_closure_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_require_candidate_source_snapshot_upload_closure();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_verification()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  query_object oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  expected_checks jsonb;
  expected_check_set_sha256 text;
  expected_payload jsonb;
  expected_verification_sha256 text;
  expected_verification_id text;
  check_count bigint;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT query_object
  FROM oracle_candidate_source_snapshot_demo_objects object
  WHERE object.plan_id = NEW.plan_id
    AND object.expected_cid =
      plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
  ORDER BY object.remote_object_key
  LIMIT 1;
  SELECT count(*), jsonb_agg(jsonb_build_object(
           'checkId', check_row.check_id,
           'checkKind', check_row.check_kind,
           'checkSha256', check_row.check_sha256,
           'evidenceSha256', check_row.evidence_sha256
         ) ORDER BY check_row.check_kind)
  INTO check_count, expected_checks
  FROM oracle_candidate_source_snapshot_demo_remote_checks check_row
  WHERE check_row.plan_id = NEW.plan_id;
  expected_check_set_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_checks), 'UTF8'
  )), 'hex');
  expected_payload := jsonb_build_object(
    'approvalId', NEW.approval_id,
    'checkSetSha256', expected_check_set_sha256,
    'checks', expected_checks,
    'distinctPropertyIdCount',
      plan_row.plan_payload->'coverage'->'activeProperties',
    'fixtureMatchCount', 0,
    'graphTraversalValid', true,
    'inventoryRootCid', plan_row.inventory_root_cid,
    'inventoryRootSha256', plan_row.inventory_root_sha256,
    'manifestCid', plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->'expectedCid',
    'manifestSha256', plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->'sha256',
    'nullPropertyIdCount', 0,
    'openDataRootCid', plan_row.plan_payload->'targets'->
      'openData'->'targetCid',
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'propertyCidCorrespondence', true,
    'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties',
    'queryTableBytes', query_object.expected_bytes,
    'queryTableRootCid', plan_row.plan_payload->'targets'->
      'queryTable'->'targetCid',
    'queryTableSha256', query_object.expected_sha256,
    'schemaVersion', 'candidate-source-snapshot-remote-verification-v2',
    'uploadClosureId', NEW.upload_closure_id,
    'verifiedAt', to_char(NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  );
  expected_verification_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_verification_id := 'snapshotdemoremoteverification_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-remote-verification-v2',
      plan_row.plan_id,
      expected_verification_sha256
    ])), 'UTF8')), 'hex'), 1, 32);
  IF coalesce((plan_row.plan_payload->'requestEnvelope'->
       'successfulExecution'->>'freeOperations')::integer, 0) < 7 OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     check_count IS DISTINCT FROM 7::bigint OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.open_data_root_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'targetCid' OR
     NEW.query_table_root_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'queryTable'->>'targetCid' OR
     NEW.manifest_cid IS DISTINCT FROM
       plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'expectedCid' OR
     NEW.manifest_sha256 IS DISTINCT FROM
       plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'sha256' OR
     NEW.inventory_root_cid IS DISTINCT FROM plan_row.inventory_root_cid OR
     NEW.inventory_root_sha256 IS DISTINCT FROM plan_row.inventory_root_sha256 OR
     NEW.query_table_bytes IS DISTINCT FROM query_object.expected_bytes OR
     NEW.query_table_sha256 IS DISTINCT FROM query_object.expected_sha256 OR
     NEW.property_count IS DISTINCT FROM
       (plan_row.plan_payload->'coverage'->>'activeProperties')::integer OR
     NEW.distinct_property_id_count IS DISTINCT FROM NEW.property_count OR
     NEW.null_property_id_count IS DISTINCT FROM 0 OR
     NEW.property_cid_correspondence IS DISTINCT FROM true OR
     NEW.graph_traversal_valid IS DISTINCT FROM true OR
     NEW.fixture_match_count IS DISTINCT FROM 0 OR
     NEW.check_set_sha256 IS DISTINCT FROM expected_check_set_sha256 OR
     NEW.verification_payload IS DISTINCT FROM expected_payload OR
     NEW.verification_sha256 IS DISTINCT FROM expected_verification_sha256 OR
     NEW.verification_id IS DISTINCT FROM expected_verification_id OR
     (SELECT count(*)
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.plan_id = NEW.plan_id
          AND intent_state.state = 'verified') IS DISTINCT FROM 2::bigint OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
       WHERE attempt.plan_id = NEW.plan_id
         AND attempt.outcome = 'request_started'
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.outcome = 'request_started'
     ) OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.closure_id = NEW.upload_closure_id
         AND closure.plan_id = NEW.plan_id
         AND closure.plan_sha256 = NEW.plan_sha256
         AND closure.approval_id = NEW.approval_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot remote verification is incomplete or mismatched';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_remote_verification_guard
  ON oracle_candidate_source_snapshot_demo_remote_verifications;
CREATE TRIGGER oracle_candidate_source_snapshot_remote_verification_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_remote_verifications
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_remote_verification();

CREATE TRIGGER oracle_candidate_source_snapshot_remote_verification_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_demo_remote_verifications
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_require_candidate_source_snapshot_remote_verification_for_completion()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state = 'executing' AND NEW.state = 'completed' AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_remote_verifications verification
    WHERE verification.plan_id = OLD.plan_id
      AND verification.plan_sha256 = OLD.plan_sha256
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot completion requires immutable remote verification';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_source_snapshot_completion_verification_guard
  ON oracle_candidate_source_snapshot_demo_plans;
CREATE TRIGGER oracle_candidate_source_snapshot_completion_verification_guard
  BEFORE UPDATE ON oracle_candidate_source_snapshot_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_require_candidate_source_snapshot_remote_verification_for_completion();
