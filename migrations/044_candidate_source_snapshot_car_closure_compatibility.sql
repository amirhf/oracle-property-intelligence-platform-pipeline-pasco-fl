BEGIN;

-- A verified object may retain its historical individual receipt identity while
-- also being covered by a later exact CAR import. Closure therefore derives
-- CAR coverage from the immutable member, artifact, receipt, gateway, and bulk
-- evidence rows instead of requiring the object's optional CAR pointer columns.
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
        JOIN oracle_candidate_source_snapshot_car_artifacts artifact
          ON artifact.car_artifact_id = member.car_artifact_id
         AND artifact.plan_id = member.plan_id
        JOIN oracle_candidate_source_snapshot_demo_plans car_plan
          ON car_plan.plan_id = artifact.plan_id
         AND car_plan.plan_sha256 = artifact.plan_sha256
        JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
          ON receipt.car_artifact_id = artifact.car_artifact_id
         AND receipt.plan_id = artifact.plan_id
         AND receipt.plan_sha256 = artifact.plan_sha256
        JOIN oracle_candidate_source_snapshot_car_gateway_evidence gateway
          ON gateway.gateway_evidence_id = receipt.gateway_evidence_id
         AND gateway.car_import_outcome_id = receipt.provider_outcome_id
         AND gateway.car_artifact_id = artifact.car_artifact_id
         AND gateway.plan_id = artifact.plan_id
        JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
          ON bulk.car_import_receipt_id = receipt.car_import_receipt_id
         AND bulk.car_artifact_id = artifact.car_artifact_id
         AND bulk.plan_id = artifact.plan_id
        WHERE member.plan_id = object.plan_id
          AND member.domain = object.domain
          AND member.remote_object_key = object.remote_object_key
          AND member.domain = artifact.car_role
          AND member.expected_sha256 = object.expected_sha256
          AND member.expected_cid = object.expected_cid
          AND member.expected_bytes = object.expected_bytes
          AND artifact.primary_root_cid = CASE artifact.car_role
            WHEN 'open_data' THEN
              car_plan.plan_payload #>> '{targets,openData,targetCid}'
            ELSE car_plan.plan_payload #>> '{targets,queryTable,targetCid}'
          END
          AND receipt.verification_method = 'car_import_recursively_pinned'
          AND receipt.car_sha256 = artifact.car_sha256
          AND receipt.car_bytes = artifact.car_bytes
          AND receipt.primary_root_cid = artifact.primary_root_cid
          AND receipt.root_count = artifact.root_count
          AND receipt.root_set_sha256 = artifact.root_set_sha256
          AND receipt.root_observation_set_sha256 = artifact.root_set_sha256
          AND receipt.member_set_sha256 = artifact.member_set_sha256
          AND receipt.member_count = artifact.member_count
          AND receipt.member_logical_bytes = artifact.member_logical_bytes
          AND receipt.provider_import_result = 'expected_root_set_returned'
          AND receipt.final_recursive_pin_status = 'pinned'
          AND receipt.official_gateway_status = 'verified'
          AND receipt.root_block_validation = 'cid_verified'
          AND receipt.bucket_identity = artifact.bucket_identity
          AND receipt.rpc_endpoint = artifact.rpc_endpoint
          AND gateway.bucket_identity = artifact.bucket_identity
          AND gateway.rpc_endpoint = artifact.rpc_endpoint
          AND gateway.root_cid = artifact.primary_root_cid
          AND gateway.provider_http_status = 200
          AND gateway.validation_result = 'cid_verified'
          AND bulk.verification_method = 'car_import_recursively_pinned'
          AND bulk.member_set_sha256 = artifact.member_set_sha256
          AND bulk.member_count = artifact.member_count
          AND bulk.member_logical_bytes = artifact.member_logical_bytes
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
           JOIN oracle_candidate_source_snapshot_car_artifacts artifact
             ON artifact.car_artifact_id = member.car_artifact_id
            AND artifact.plan_id = member.plan_id
           JOIN oracle_candidate_source_snapshot_demo_plans car_plan
             ON car_plan.plan_id = artifact.plan_id
            AND car_plan.plan_sha256 = artifact.plan_sha256
           JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
             ON receipt.car_artifact_id = artifact.car_artifact_id
            AND receipt.plan_id = artifact.plan_id
            AND receipt.plan_sha256 = artifact.plan_sha256
           JOIN oracle_candidate_source_snapshot_car_gateway_evidence gateway
             ON gateway.gateway_evidence_id = receipt.gateway_evidence_id
            AND gateway.car_import_outcome_id = receipt.provider_outcome_id
            AND gateway.car_artifact_id = artifact.car_artifact_id
            AND gateway.plan_id = artifact.plan_id
           JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
             ON bulk.car_import_receipt_id = receipt.car_import_receipt_id
            AND bulk.car_artifact_id = artifact.car_artifact_id
            AND bulk.plan_id = artifact.plan_id
           WHERE object.plan_id = request.plan_id
             AND object.domain = request.domain
             AND object.remote_object_key = request.remote_object_key
             AND object.status = 'verified'
             AND object.provider_cid = object.expected_cid
             AND member.domain = artifact.car_role
             AND member.expected_sha256 = object.expected_sha256
             AND member.expected_cid = object.expected_cid
             AND member.expected_bytes = object.expected_bytes
             AND artifact.primary_root_cid = CASE artifact.car_role
               WHEN 'open_data' THEN
                 car_plan.plan_payload #>> '{targets,openData,targetCid}'
               ELSE car_plan.plan_payload #>> '{targets,queryTable,targetCid}'
             END
             AND receipt.verification_method =
               'car_import_recursively_pinned'
             AND receipt.car_sha256 = artifact.car_sha256
             AND receipt.car_bytes = artifact.car_bytes
             AND receipt.primary_root_cid = artifact.primary_root_cid
             AND receipt.root_count = artifact.root_count
             AND receipt.root_set_sha256 = artifact.root_set_sha256
             AND receipt.root_observation_set_sha256 =
               artifact.root_set_sha256
             AND receipt.member_set_sha256 = artifact.member_set_sha256
             AND receipt.member_count = artifact.member_count
             AND receipt.member_logical_bytes = artifact.member_logical_bytes
             AND receipt.provider_import_result =
               'expected_root_set_returned'
             AND receipt.final_recursive_pin_status = 'pinned'
             AND receipt.official_gateway_status = 'verified'
             AND receipt.root_block_validation = 'cid_verified'
             AND receipt.bucket_identity = artifact.bucket_identity
             AND receipt.rpc_endpoint = artifact.rpc_endpoint
             AND gateway.bucket_identity = artifact.bucket_identity
             AND gateway.rpc_endpoint = artifact.rpc_endpoint
             AND gateway.root_cid = artifact.primary_root_cid
             AND gateway.provider_http_status = 200
             AND gateway.validation_result = 'cid_verified'
             AND bulk.verification_method =
               'car_import_recursively_pinned'
             AND bulk.member_set_sha256 = artifact.member_set_sha256
             AND bulk.member_count = artifact.member_count
             AND bulk.member_logical_bytes = artifact.member_logical_bytes
         )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload closure is not exact and complete';
  END IF;
  RETURN NEW;
END;
$$;

COMMIT;
