BEGIN;

-- Migration 032 deliberately permits a continuation at attempt 3, but its
-- execution-readiness predicate required every authorized successor to
-- succeed immediately. Preserve the immutable receipts and authorization
-- guards while allowing a terminal attempt 2 to be superseded by its exact,
-- independently authorized attempt 3.
CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_preflight_is_execution_ready(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  WITH allowed_keys(domain, operation_kind, resolver) AS (
    VALUES
      ('open_data'::text, 'bucket_head'::text, NULL::text),
      ('open_data', 'names_read', 'filebase_control'),
      ('open_data', 'public_resolve', 'filebase_gateway'),
      ('open_data', 'public_resolve', 'delegated_ipfs'),
      ('query_table', 'bucket_head', NULL),
      ('query_table', 'names_read', 'filebase_control'),
      ('query_table', 'public_resolve', 'filebase_gateway'),
      ('query_table', 'public_resolve', 'delegated_ipfs')
  ), preflight AS (
    SELECT request.*
    FROM oracle_candidate_source_snapshot_demo_requests request
    WHERE request.plan_id = checked_plan_id
      AND request.request_category = 'bucket_names_preflight'
  )
  SELECT COALESCE(
    EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_plans plan
      JOIN oracle_candidate_source_snapshot_demo_approvals approval
        ON approval.plan_id = plan.plan_id
       AND approval.plan_sha256 = plan.plan_sha256
      JOIN oracle_candidate_source_snapshot_demo_request_categories category
        ON category.plan_id = plan.plan_id
       AND category.request_category = 'bucket_names_preflight'
      WHERE plan.plan_id = checked_plan_id
        AND plan.plan_version = '2.1.0'
        AND approval.approval_version =
          'candidate-source-snapshot-approval-v3'
        AND category.planned_successful_request_count = 8
        AND category.consumed_request_count BETWEEN 8 AND 48
        AND category.consumed_request_count = (SELECT count(*) FROM preflight)
    )
    AND (SELECT count(DISTINCT logical_request_id) FROM preflight) = 8
    AND NOT EXISTS (
      SELECT 1
      FROM preflight request
      WHERE request.intent_id IS NOT NULL
         OR request.receipt_sha256 IS NULL
         OR request.outcome = 'request_started'
         OR NOT EXISTS (
           SELECT 1
           FROM allowed_keys allowed
           WHERE allowed.domain = request.domain
             AND allowed.operation_kind = request.operation_kind
             AND allowed.resolver IS NOT DISTINCT FROM request.resolver
         )
         OR (
           request.outcome NOT IN (
             'succeeded', 'retryable_failure', 'timeout_unknown'
           ) AND NOT (
             request.outcome = 'terminal_failure' AND EXISTS (
               SELECT 1
               FROM oracle_candidate_source_preflight_continuation_authorizations continuation
               JOIN preflight successor
                 ON successor.continuation_authorization_id =
                      continuation.authorization_id
                AND successor.plan_id = request.plan_id
                AND successor.logical_request_id = request.logical_request_id
                AND successor.domain = request.domain
                AND successor.operation_kind = request.operation_kind
                AND successor.resolver IS NOT DISTINCT FROM request.resolver
                AND successor.attempt_sequence =
                      continuation.authorized_attempt_sequence
                AND successor.outcome IN ('succeeded', 'terminal_failure')
                AND successor.receipt_sha256 IS NOT NULL
               WHERE continuation.failed_request_id = request.request_id
                 AND continuation.plan_id = request.plan_id
             )
           )
         )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM preflight request
      WHERE request.continuation_authorization_id IS NOT NULL
        AND (
          request.outcome NOT IN ('succeeded', 'terminal_failure') OR
          request.receipt_sha256 IS NULL OR
          NOT EXISTS (
            SELECT 1
            FROM oracle_candidate_source_preflight_continuation_authorizations continuation
            WHERE continuation.authorization_id =
                    request.continuation_authorization_id
              AND continuation.plan_id = request.plan_id
              AND continuation.domain = request.domain
              AND continuation.operation_kind = request.operation_kind
              AND continuation.resolver IS NOT DISTINCT FROM request.resolver
              AND continuation.authorized_attempt_sequence =
                    request.attempt_sequence
          )
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_preflight_continuation_authorizations continuation
      WHERE continuation.plan_id = checked_plan_id
        AND NOT EXISTS (
          SELECT 1
          FROM preflight successor
          WHERE successor.continuation_authorization_id =
                  continuation.authorization_id
            AND successor.outcome IN ('succeeded', 'terminal_failure')
            AND successor.receipt_sha256 IS NOT NULL
        )
    )
    AND NOT EXISTS (
      SELECT request.domain, request.operation_kind, request.resolver
      FROM preflight request
      GROUP BY request.domain, request.operation_kind, request.resolver
      HAVING count(DISTINCT request.logical_request_id) <> 1 OR
             count(*) FILTER (WHERE request.outcome = 'succeeded') <> 1
    )
    AND (
      SELECT count(*)
      FROM (
        SELECT request.domain, request.operation_kind, request.resolver
        FROM preflight request
        GROUP BY request.domain, request.operation_kind, request.resolver
      ) exact_preflight_key
    ) = 8
    AND NOT EXISTS (
      SELECT 1
      FROM preflight first_request
      JOIN preflight second_request
        ON second_request.logical_request_id = first_request.logical_request_id
       AND (
         second_request.domain,
         second_request.operation_kind,
         second_request.resolver
       ) IS DISTINCT FROM (
         first_request.domain,
         first_request.operation_kind,
         first_request.resolver
       )
    ),
    false
  );
$$;

COMMENT ON FUNCTION oracle_candidate_source_snapshot_preflight_is_execution_ready(text)
  IS 'Requires eight exact preflight keys with one success each; terminal receipts are tolerated only through a gap-free, exact, immutable continuation chain ending in success by attempt 3.';

COMMIT;
