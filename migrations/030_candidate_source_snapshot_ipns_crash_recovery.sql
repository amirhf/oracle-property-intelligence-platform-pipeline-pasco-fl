BEGIN;

-- Migration 029's compact v2.1 validator predates the mandatory resolver
-- freshness cycle immediately before each of the two Names API mutations.
-- Preserve that complete validator under a versioned private name and validate
-- the additive six successful observations by normalizing only those derived
-- fields before delegating to it.
ALTER FUNCTION oracle_candidate_source_snapshot_v21_categories_valid(jsonb)
  RENAME TO oracle_candidate_source_snapshot_v21_categories_valid_v29;

CREATE FUNCTION oracle_candidate_source_snapshot_v21_categories_valid(
  checked_payload jsonb
)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  WITH expected AS (
    SELECT
      (checked_payload->'inventory'->>'objectCount')::integer + 8331
        AS successful_requests,
      (checked_payload->'costEnvelope'->>'storageUsd')::numeric AS storage_usd
  ), normalized_request AS (
    SELECT jsonb_set(
      jsonb_set(
        checked_payload,
        '{requestEnvelope,categoryRequests,control_public_observation}',
        '[12, 42]'::jsonb,
        false
      ),
      '{requestEnvelope,successfulTotalRequests}',
      to_jsonb(expected.successful_requests - 6),
      false
    ) AS payload,
    expected.successful_requests,
    expected.storage_usd
    FROM expected
  ), normalized_cost AS (
    SELECT jsonb_set(
      jsonb_set(
        payload,
        '{costEnvelope,requestUsd,successfulExecution}',
        to_jsonb((successful_requests - 6) * 0.0000045::numeric),
        false
      ),
      '{costEnvelope,incrementalExecutionUsd}',
      to_jsonb(round(
        storage_usd + (successful_requests - 6) * 0.0000045::numeric,
        12
      )),
      false
    ) AS payload,
    successful_requests
    FROM normalized_request
  )
  SELECT COALESCE(
    checked_payload->'requestEnvelope'->'categoryRequests'->
      'control_public_observation' = '[18, 42]'::jsonb
    AND (checked_payload->'requestEnvelope'->>
      'successfulTotalRequests')::integer = normalized_cost.successful_requests
    AND (checked_payload->'costEnvelope'->'requestUsd'->>
      'successfulExecution')::numeric =
        normalized_cost.successful_requests * 0.0000045::numeric
    AND (checked_payload->'costEnvelope'->>
      'incrementalExecutionUsd')::numeric = round(
        (checked_payload->'costEnvelope'->>'storageUsd')::numeric +
        normalized_cost.successful_requests * 0.0000045::numeric,
        12
      )
    AND oracle_candidate_source_snapshot_v21_categories_valid_v29(
      normalized_cost.payload
    ),
    false
  )
  FROM normalized_cost;
$$;

-- A crash can leave a rollback intent durably recorded before any Names API
-- request admission exists. If a later complete resolver cycle proves that the
-- immutable prior is already current, recovery must be able to finish without
-- fabricating a provider request. All other rollback paths continue to require
-- a terminal admitted attempt and post-attempt evidence.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_intent_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.revision + 1 <> NEW.revision THEN
    RAISE EXCEPTION 'candidate source-snapshot intent state identity/revision is invalid';
  END IF;
  IF NOT (
    (OLD.state = 'intent_recorded' AND NEW.state = 'prior_confirmed') OR
    (OLD.state = 'prior_confirmed' AND NEW.state = 'update_in_flight') OR
    (OLD.state = 'update_in_flight' AND NEW.state IN (
      'target_observed', 'update_ambiguous', 'unexpected_cid',
      'update_failed_prior_confirmed', 'failed_terminal'
    )) OR
    (OLD.state = 'update_ambiguous' AND NEW.state IN (
      'prior_confirmed', 'target_observed', 'unexpected_cid',
      'update_failed_prior_confirmed', 'manual_intervention_required',
      'failed_terminal'
    )) OR
    (OLD.state = 'target_observed' AND NEW.state IN ('verified', 'rollback_recorded')) OR
    (OLD.state = 'verified' AND NEW.state = 'rollback_recorded') OR
    (OLD.state = 'rollback_recorded' AND NEW.state IN (
      'rollback_in_flight', 'rolled_back'
    )) OR
    (OLD.state = 'rollback_in_flight' AND NEW.state IN (
      'rolled_back', 'rollback_ambiguous', 'unexpected_cid', 'failed_terminal'
    )) OR
    (OLD.state = 'rollback_ambiguous' AND NEW.state IN (
      'rollback_recorded', 'rolled_back', 'unexpected_cid',
      'manual_intervention_required', 'failed_terminal'
    ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot IPNS transition';
  END IF;
  IF NEW.state = 'prior_confirmed' AND NOT
    oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'prior', OLD.state <> 'intent_recorded'
    ) THEN
    RAISE EXCEPTION 'candidate source-snapshot prior confirmation requires one complete resolution cycle';
  END IF;
  IF NEW.state IN ('target_observed', 'verified') AND (
    NOT oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'target', true
    ) OR NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'update'
        AND attempt.outcome <> 'request_started'
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot target state requires one complete resolution cycle';
  END IF;
  IF NEW.state = 'update_failed_prior_confirmed' AND (
    NOT oracle_candidate_source_snapshot_has_complete_resolution_cycle(
      NEW.intent_id, 'prior', true
    ) OR NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'update'
        AND attempt.outcome IN (
          'timeout_unknown', 'retryable_failure', 'terminal_failure'
        )
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot failed update requires terminal attempt and later complete prior evidence';
  END IF;
  IF NEW.state = 'rolled_back' AND (
    NOT (
      (OLD.state = 'rollback_recorded' AND EXISTS (
        SELECT observation.cycle_sequence
        FROM oracle_candidate_source_snapshot_demo_ipns_events transition_event
        JOIN oracle_candidate_source_snapshot_demo_ipns_intents intent
          ON intent.intent_id = transition_event.intent_id
        JOIN oracle_candidate_source_snapshot_demo_ipns_observations observation
          ON observation.intent_id = transition_event.intent_id
        WHERE transition_event.intent_id = NEW.intent_id
          AND transition_event.to_state = 'rollback_recorded'
          AND transition_event.to_revision = OLD.revision
          AND observation.observed_at >= transition_event.recorded_at
          AND observation.resolver IN (
            'filebase_control', 'filebase_gateway', 'delegated_ipfs'
          )
        GROUP BY observation.cycle_sequence, intent.prior_cid
        HAVING count(*) = 3
           AND count(*) FILTER (
             WHERE observation.classification = 'prior'
               AND observation.observed_cid = intent.prior_cid
           ) = 3
      )) OR
      (OLD.state <> 'rollback_recorded' AND
       oracle_candidate_source_snapshot_has_complete_resolution_cycle(
         NEW.intent_id, 'prior', true
       ) AND EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
        WHERE attempt.intent_id = NEW.intent_id
          AND attempt.direction = 'rollback'
          AND attempt.outcome <> 'request_started'
      ))
    ) OR EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
      WHERE attempt.intent_id = NEW.intent_id
        AND attempt.direction = 'rollback'
        AND attempt.outcome = 'request_started'
    )
  ) THEN
    RAISE EXCEPTION 'candidate source-snapshot rollback requires conclusive prior evidence and no unresolved attempt';
  END IF;
  IF NEW.state = 'rollback_recorded' AND NEW.domain = 'open_data' AND EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_ipns_intents query_intent
    JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state query_state
      ON query_state.intent_id = query_intent.intent_id
    WHERE query_intent.plan_id = NEW.plan_id
      AND query_intent.domain = 'query_table'
      AND query_state.state NOT IN (
        'intent_recorded', 'prior_confirmed',
        'update_failed_prior_confirmed', 'rolled_back'
      )
  ) THEN
    RAISE EXCEPTION 'open-data rollback requires conclusive query-table non-mutation or rollback';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

COMMIT;
