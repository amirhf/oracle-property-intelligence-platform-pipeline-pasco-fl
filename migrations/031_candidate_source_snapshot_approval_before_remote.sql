BEGIN;

-- Migration 030 admitted bounded preflight reads before approval. Refuse to
-- inherit any such evidence unless an exact v3 approval was already durable
-- before the request began; never reinterpret legacy remote evidence.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_requests request
    JOIN oracle_candidate_source_snapshot_demo_plans plan
      ON plan.plan_id = request.plan_id
    WHERE request.request_category = 'bucket_names_preflight'
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_approvals approval
        WHERE approval.plan_id = request.plan_id
          AND approval.plan_sha256 = plan.plan_sha256
          AND approval.approval_version =
            'candidate-source-snapshot-approval-v3'
          AND approval.implementation_commit_sha ~ '^[a-f0-9]{40}$'
          AND approval.authorization_binding_sha256 ~ '^[a-f0-9]{64}$'
          AND approval.authorization_statement_sha256 ~ '^[a-f0-9]{64}$'
          AND approval.approval_sha256 ~ '^[a-f0-9]{64}$'
          AND approval.approved_at <= request.started_at
      )
  ) THEN
    RAISE EXCEPTION
      'migration 031 rejects preflight evidence without an earlier exact approval';
  END IF;
END;
$$;

-- Human authorization is a local durable fact. Preserve migration 029's
-- complete eight-receipt validator under an execution-specific name so the
-- approval row can be inserted before any remote adapter is constructed.
ALTER FUNCTION oracle_candidate_source_snapshot_preflight_is_approval_ready(text)
  RENAME TO oracle_candidate_source_snapshot_preflight_is_execution_ready;

CREATE FUNCTION oracle_candidate_source_snapshot_preflight_is_approval_ready(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT COALESCE(EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_plans plan
    WHERE plan.plan_id = checked_plan_id
      AND plan.plan_version = '2.1.0'
      AND plan.state = 'awaiting_approval'
      AND oracle_candidate_source_snapshot_has_exact_categories(plan.plan_id)
      AND oracle_candidate_source_snapshot_derivation_is_approval_ready(
        plan.plan_id
      )
  ), false);
$$;

-- The bounded intent-free preflight runs only after the exact approval row is
-- durable, while the plan is approved but not executing. All other request
-- categories remain execution-only. Amend only those closed predicates and
-- fail migration if the committed baseline drifts.
DO $$
DECLARE
  definition text;
  rewritten text;
BEGIN
  SELECT pg_get_functiondef(
    'oracle_guard_candidate_source_snapshot_request_insert()'::regprocedure
  ) INTO definition;
  rewritten := replace(
    definition,
    'plan_row.state IN (''awaiting_configuration'', ''awaiting_approval'')',
    'plan_row.state = ''approved'''
  );
  IF rewritten IS NOT DISTINCT FROM definition THEN
    RAISE EXCEPTION
      'migration 031 could not bind the approved-state preflight predicate';
  END IF;
  definition := rewritten;
  rewritten := replace(
    definition,
    'IF plan_row.plan_version <> ''2.1.0'' OR',
    $request_guard$IF plan_row.plan_version <> '2.1.0' OR
     (
       NEW.request_category = 'bucket_names_preflight' AND
       plan_row.state IS DISTINCT FROM 'approved'
     ) OR$request_guard$
  );
  IF rewritten IS NOT DISTINCT FROM definition THEN
    RAISE EXCEPTION
      'migration 031 could not make preflight requests approved-state only';
  END IF;
  EXECUTE rewritten;

  SELECT pg_get_functiondef(
    'oracle_guard_candidate_source_snapshot_request_category()'::regprocedure
  ) INTO definition;
  rewritten := replace(
    definition,
    'plan_row.state IN (''awaiting_configuration'', ''awaiting_approval'')',
    'plan_row.state = ''approved'''
  );
  IF rewritten IS NOT DISTINCT FROM definition THEN
    RAISE EXCEPTION
      'migration 031 could not bind the approved-state preflight category predicate';
  END IF;
  definition := rewritten;
  rewritten := replace(
    definition,
    'IF NOT (',
    $category_guard$IF (
       NEW.request_category = 'bucket_names_preflight' AND
       plan_row.state IS DISTINCT FROM 'approved'
     ) OR NOT ($category_guard$
  );
  IF rewritten IS NOT DISTINCT FROM definition THEN
    RAISE EXCEPTION
      'migration 031 could not make preflight category accounting approved-state only';
  END IF;
  EXECUTE rewritten;
END;
$$;

-- Direct state entry into executing remains fail-closed until the eight exact
-- preflight receipts are complete. Approval itself has no remote dependency.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_v21_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.plan_version = '2.0.0' AND
     OLD.state IS DISTINCT FROM NEW.state AND
     NEW.state IN (
       'awaiting_approval', 'approved', 'executing', 'completed'
     ) THEN
    RAISE EXCEPTION
      'legacy candidate source-snapshot plans retain audit state but cannot gain approval or execution authority';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'awaiting_configuration' AND
     NEW.state = 'awaiting_approval' AND
     NOT oracle_candidate_source_snapshot_has_exact_categories(OLD.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot approval readiness requires all exact request categories';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'approved' AND NEW.state = 'executing' AND (
       NOT oracle_candidate_source_snapshot_preflight_is_execution_ready(
         OLD.plan_id
       ) OR
       NOT EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_demo_approvals approval
         WHERE approval.plan_id = OLD.plan_id
           AND approval.plan_sha256 = OLD.plan_sha256
           AND approval.approval_version =
             'candidate-source-snapshot-approval-v3'
           AND approval.approved_plan_revision = OLD.revision - 1
       )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot execution requires exact approval v3 and complete preflight evidence';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'executing' AND NEW.state = 'completed' AND NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_verifications verification
       WHERE verification.plan_id = OLD.plan_id
         AND verification.plan_sha256 = OLD.plan_sha256
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot completion requires receipt-bound final verification';
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION oracle_candidate_source_snapshot_preflight_is_approval_ready(text) IS
  'Local-only approval readiness; performs no remote request and requires no remote receipt.';
COMMENT ON FUNCTION oracle_candidate_source_snapshot_preflight_is_execution_ready(text) IS
  'Exact eight-request bucket, control-plane, gateway, and delegated preflight evidence required before executing.';

COMMIT;
