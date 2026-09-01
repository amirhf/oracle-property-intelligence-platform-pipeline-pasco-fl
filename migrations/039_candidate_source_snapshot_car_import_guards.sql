BEGIN;

-- Additive repair for the locally-applied CAR evidence schema. No CAR import,
-- IPNS mutation, publication completion, or historical receipt rewrite occurs.

ALTER TABLE oracle_candidate_source_snapshot_car_import_authorizations
  DROP CONSTRAINT oracle_candidate_source_snapshot_car_impo_ipns_authorized_check;

ALTER TABLE oracle_candidate_source_snapshot_car_import_authorizations
  ADD CONSTRAINT oracle_css_car_authorization_ipns_scope_check
    CHECK (ipns_authorized = true),
  ADD COLUMN ipns_order text NOT NULL CHECK (
    ipns_order = 'open_data_then_query_table'
  ),
  ADD COLUMN final_credential_free_verification_authorized boolean NOT NULL
    CHECK (final_credential_free_verification_authorized = true),
  ADD COLUMN vercel_deployment_authorized boolean NOT NULL
    CHECK (vercel_deployment_authorized = true);

ALTER TABLE oracle_candidate_source_snapshot_car_import_receipts
  ADD COLUMN provider_evidence_set_sha256 text NOT NULL CHECK (
    provider_evidence_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_css_car_receipt_provider_identifier_check CHECK (
    provider_request_id_hash IS NOT NULL OR provider_pin_id_hash IS NOT NULL
  );

-- Roots are inserted before members by the recording API, so the exact
-- same-CAR membership binding is deferred to transaction completion.
ALTER TABLE oracle_candidate_source_snapshot_car_roots
  ADD CONSTRAINT oracle_css_car_root_exact_member_fkey
  FOREIGN KEY (car_artifact_id, plan_id, domain, remote_object_key)
  REFERENCES oracle_candidate_source_snapshot_car_members(
    car_artifact_id, plan_id, domain, remote_object_key
  ) DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_domain_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_role text;
BEGIN
  SELECT car_role INTO STRICT expected_role
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id
    AND plan_id = NEW.plan_id;
  IF NEW.domain IS DISTINCT FROM expected_role THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR domain is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_root_domain_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_roots
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_domain_insert();

CREATE TRIGGER oracle_css_car_member_domain_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_members
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_domain_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_authorization_shape()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.ipns_authorized IS DISTINCT FROM true OR
     NEW.ipns_order IS DISTINCT FROM 'open_data_then_query_table' OR
     NEW.final_credential_free_verification_authorized IS DISTINCT FROM true OR
     NEW.vercel_deployment_authorized IS DISTINCT FROM true OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_artifacts artifact
       WHERE artifact.plan_id = NEW.plan_id AND (
         artifact.root_count IS DISTINCT FROM (
           SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_car_roots root
           WHERE root.car_artifact_id = artifact.car_artifact_id
         ) OR NOT EXISTS (
           SELECT 1
           FROM oracle_candidate_source_snapshot_car_roots root
           WHERE root.car_artifact_id = artifact.car_artifact_id
             AND root.root_ordinal = 1
             AND root.root_role = 'approved_target'
             AND root.root_cid = artifact.primary_root_cid
             AND root.domain = artifact.car_role
         ) OR EXISTS (
           SELECT 1
           FROM oracle_candidate_source_snapshot_car_roots root
           WHERE root.car_artifact_id = artifact.car_artifact_id
             AND (root.domain IS DISTINCT FROM artifact.car_role OR
                  root.root_ordinal > artifact.root_count OR
                  (root.root_ordinal > 1 AND
                   root.root_role IS DISTINCT FROM 'additional_planned_object'))
         ) OR EXISTS (
           SELECT 1
           FROM oracle_candidate_source_snapshot_car_members member
           WHERE member.car_artifact_id = artifact.car_artifact_id
             AND member.domain IS DISTINCT FROM artifact.car_role
         )
       )
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR authorization shape is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_authorization_shape_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_authorization_shape();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_admission()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_state text;
BEGIN
  SELECT state INTO STRICT plan_state
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_state IS DISTINCT FROM 'executing' OR
     NEW.provider_evidence_set_sha256 IS NULL OR
     (NEW.provider_request_id_hash IS NULL AND NEW.provider_pin_id_hash IS NULL) OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations auth
       WHERE auth.car_authorization_id = NEW.car_authorization_id
         AND auth.plan_id = NEW.plan_id
         AND auth.plan_sha256 = NEW.plan_sha256
         AND auth.implementation_commit_sha = NEW.implementation_commit_sha
         AND auth.ipns_authorized = true
         AND auth.ipns_order = 'open_data_then_query_table'
         AND auth.final_credential_free_verification_authorized = true
         AND auth.vercel_deployment_authorized = true
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

CREATE TRIGGER oracle_css_car_receipt_admission_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_receipt_admission();

-- A completed recursive CAR import is stronger evidence for the exact member
-- than a historical per-object request_started row. The immutable request is
-- preserved; it no longer blocks the exact member batch.
CREATE OR REPLACE FUNCTION oracle_guard_css_car_bulk_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  receipt_row oracle_candidate_source_snapshot_car_import_receipts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  actual_member_count integer;
  actual_member_bytes bigint;
  preserved_count integer;
  preserved_bytes bigint;
  new_count integer;
  new_bytes bigint;
  invalid_count integer;
  expected_payload jsonb;
  expected_result_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT receipt_row
  FROM oracle_candidate_source_snapshot_car_import_receipts
  WHERE car_import_receipt_id = NEW.car_import_receipt_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT count(*)::integer, coalesce(sum(member.expected_bytes), 0)::bigint,
         count(*) FILTER (WHERE object.status = 'verified')::integer,
         coalesce(sum(member.expected_bytes)
           FILTER (WHERE object.status = 'verified'), 0)::bigint,
         count(*) FILTER (
           WHERE object.status IN ('pending', 'outcome_unknown')
         )::integer,
         coalesce(sum(member.expected_bytes) FILTER (
           WHERE object.status IN ('pending', 'outcome_unknown')
         ), 0)::bigint,
         count(*) FILTER (WHERE object.status NOT IN (
           'pending', 'outcome_unknown', 'verified'
         ))::integer
  INTO actual_member_count, actual_member_bytes,
       preserved_count, preserved_bytes, new_count, new_bytes, invalid_count
  FROM oracle_candidate_source_snapshot_car_members member
  JOIN oracle_candidate_source_snapshot_demo_objects object
    ON object.plan_id = member.plan_id AND object.domain = member.domain
   AND object.remote_object_key = member.remote_object_key
  WHERE member.car_artifact_id = NEW.car_artifact_id;
  expected_payload := jsonb_build_object(
    'carArtifactId', NEW.car_artifact_id,
    'carImportReceiptId', NEW.car_import_receipt_id,
    'memberCount', artifact_row.member_count,
    'memberLogicalBytes', artifact_row.member_logical_bytes,
    'memberSetSha256', artifact_row.member_set_sha256,
    'newlyVerifiedBytes', new_bytes,
    'newlyVerifiedCount', new_count,
    'planId', artifact_row.plan_id,
    'preservedVerifiedBytes', preserved_bytes,
    'preservedVerifiedCount', preserved_count,
    'schemaVersion', NEW.bulk_version,
    'verificationMethod', NEW.verification_method,
    'verifiedAt', to_char(
      NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
  expected_result_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocarbulk_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.bulk_version, artifact_row.plan_id, NEW.car_artifact_id,
      NEW.car_import_receipt_id, expected_result_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF receipt_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     receipt_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.member_set_sha256 IS DISTINCT FROM NEW.member_set_sha256 OR
     artifact_row.member_count IS DISTINCT FROM NEW.member_count OR
     artifact_row.member_logical_bytes IS DISTINCT FROM
       NEW.member_logical_bytes OR
     actual_member_count IS DISTINCT FROM NEW.member_count OR
     actual_member_bytes IS DISTINCT FROM NEW.member_logical_bytes OR
     preserved_count IS DISTINCT FROM NEW.preserved_verified_count OR
     preserved_bytes IS DISTINCT FROM NEW.preserved_verified_bytes OR
     new_count IS DISTINCT FROM NEW.newly_verified_count OR
     new_bytes IS DISTINCT FROM NEW.newly_verified_bytes OR
     invalid_count <> 0 OR
     NEW.result_payload IS DISTINCT FROM expected_payload OR
     NEW.result_sha256 IS DISTINCT FROM expected_result_sha256 OR
     NEW.bulk_verification_id IS DISTINCT FROM expected_id THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR bulk verification is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_car_bulk_complete()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  car_count integer;
  car_bytes bigint;
  preserved_count integer;
  preserved_bytes bigint;
BEGIN
  SELECT count(*) FILTER (
           WHERE object.car_import_receipt_id = NEW.car_import_receipt_id AND
             object.car_verification_method = 'car_import_recursively_pinned'
         )::integer,
         coalesce(sum(object.expected_bytes) FILTER (
           WHERE object.car_import_receipt_id = NEW.car_import_receipt_id AND
             object.car_verification_method = 'car_import_recursively_pinned'
         ), 0)::bigint,
         count(*) FILTER (
           WHERE object.car_import_receipt_id IS NULL AND
             object.car_verification_method IS NULL
         )::integer,
         coalesce(sum(object.expected_bytes) FILTER (
           WHERE object.car_import_receipt_id IS NULL AND
             object.car_verification_method IS NULL
         ), 0)::bigint
  INTO car_count, car_bytes, preserved_count, preserved_bytes
  FROM oracle_candidate_source_snapshot_car_members member
  JOIN oracle_candidate_source_snapshot_demo_objects object
    ON object.plan_id = member.plan_id AND object.domain = member.domain
   AND object.remote_object_key = member.remote_object_key
  WHERE member.car_artifact_id = NEW.car_artifact_id
    AND object.status = 'verified'
    AND object.provider_cid = object.expected_cid
    AND object.successful_effect_count = 1;
  IF car_count IS DISTINCT FROM NEW.newly_verified_count OR
     car_bytes IS DISTINCT FROM NEW.newly_verified_bytes OR
     preserved_count IS DISTINCT FROM NEW.preserved_verified_count OR
     preserved_bytes IS DISTINCT FROM NEW.preserved_verified_bytes OR
     car_count + preserved_count IS DISTINCT FROM NEW.member_count THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR bulk verification did not close exactly';
  END IF;
  RETURN NULL;
END;
$$;

-- Preserve all historical request rows, but accept a stale started request
-- only when that exact object now has the stronger committed CAR proof.
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
  SELECT coalesce(sum(receipt.import_attempt_count), 0)::integer,
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

COMMIT;
