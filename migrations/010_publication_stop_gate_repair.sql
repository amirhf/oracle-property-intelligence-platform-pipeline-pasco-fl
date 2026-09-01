-- Additive repair for migration 009. Migration 009 has already been applied to
-- the local Oracle database and must never be rewritten or silently replayed.

CREATE TABLE IF NOT EXISTS oracle_publication_legacy_invalidations (
  invalidation_id text PRIMARY KEY CHECK (
    invalidation_id ~ '^legacyinvalidate_[a-f0-9]{32}$'
  ),
  plan_id text NOT NULL UNIQUE REFERENCES oracle_publication_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  previous_state text,
  reason text NOT NULL CHECK (reason = 'legacy_v1_0_requires_v1_1_rebuild'),
  invalidated_at timestamptz NOT NULL DEFAULT now()
);

-- Fail closed when a v1.0 plan could have produced an external effect. No CID
-- or graph binding is synthesized and no graph-less v1.0 payload is resumed.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_publication_plans plan
    LEFT JOIN oracle_publication_state state ON state.plan_id = plan.plan_id
    WHERE plan.plan_version = '1.0.0'
      AND (
        state.state IN ('approved', 'executing', 'completed') OR
        EXISTS (
          SELECT 1 FROM oracle_publication_approvals approval
          WHERE approval.plan_id = plan.plan_id
        ) OR
        EXISTS (
          SELECT 1 FROM oracle_publication_object_effects effect
          WHERE effect.plan_id = plan.plan_id
            AND (
              effect.status != 'pending' OR effect.uploaded_cid IS NOT NULL OR
              effect.verified_cid IS NOT NULL OR effect.completed_at IS NOT NULL
            )
        ) OR
        EXISTS (
          SELECT 1 FROM oracle_publication_ipns_effects effect
          WHERE effect.plan_id = plan.plan_id
            AND (
              effect.status != 'pending' OR effect.mutation_performed OR
              effect.public_resolution_verified OR effect.prior_cid IS NOT NULL OR
              effect.target_cid IS NOT NULL
            )
        ) OR
        EXISTS (
          SELECT 1 FROM oracle_publication_ipns_intents intent
          WHERE intent.publication_plan_id = plan.plan_id
        )
      )
  ) THEN
    RAISE EXCEPTION
      'migration 010 blocked: legacy v1.0 publication has approval or external-effect evidence';
  END IF;
END;
$$;

INSERT INTO oracle_publication_legacy_invalidations (
  invalidation_id, plan_id, plan_sha256, previous_state, reason
)
SELECT
  'legacyinvalidate_' || md5(plan.plan_id || ':v1.1-rebuild'),
  plan.plan_id,
  plan.plan_sha256,
  state.state,
  'legacy_v1_0_requires_v1_1_rebuild'
FROM oracle_publication_plans plan
LEFT JOIN oracle_publication_state state ON state.plan_id = plan.plan_id
WHERE plan.plan_version = '1.0.0'
ON CONFLICT (plan_id) DO NOTHING;

UPDATE oracle_publication_state state
SET state = 'failed_terminal',
    terminal_reason = 'legacy_v1_0_requires_v1_1_rebuild',
    revision = revision + 1,
    updated_at = now()
FROM oracle_publication_plans plan
WHERE state.plan_id = plan.plan_id
  AND plan.plan_version = '1.0.0'
  AND state.state IN (
    'prepared', 'validated', 'awaiting_configuration', 'awaiting_approval',
    'failed_terminal'
  );

CREATE OR REPLACE FUNCTION oracle_reject_immutable_publication_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS oracle_publication_legacy_invalidations_immutable
  ON oracle_publication_legacy_invalidations;
CREATE TRIGGER oracle_publication_legacy_invalidations_immutable
  BEFORE UPDATE OR DELETE ON oracle_publication_legacy_invalidations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation();

ALTER TABLE oracle_publication_state
  DROP CONSTRAINT IF EXISTS oracle_publication_state_state_check;
ALTER TABLE oracle_publication_state
  ADD CONSTRAINT oracle_publication_state_state_check CHECK (state IN (
    'prepared', 'validated', 'awaiting_configuration', 'awaiting_approval',
    'approved', 'executing', 'completed', 'manual_intervention_required',
    'failed_terminal'
  ));
ALTER TABLE oracle_publication_state
  DROP CONSTRAINT IF EXISTS oracle_publication_state_check;
ALTER TABLE oracle_publication_state
  ADD CONSTRAINT oracle_publication_state_terminal_reason_check CHECK (
    (state IN ('failed_terminal', 'manual_intervention_required') AND
      terminal_reason IS NOT NULL) OR
    (state NOT IN ('failed_terminal', 'manual_intervention_required') AND
      terminal_reason IS NULL)
  );

ALTER TABLE oracle_publication_state_events
  DROP CONSTRAINT IF EXISTS oracle_publication_state_events_to_state_check;
ALTER TABLE oracle_publication_state_events
  ADD CONSTRAINT oracle_publication_state_events_to_state_check CHECK (
    to_state IN (
      'prepared', 'validated', 'awaiting_configuration', 'awaiting_approval',
      'approved', 'executing', 'completed', 'manual_intervention_required',
      'failed_terminal'
    )
  );

ALTER TABLE oracle_publication_ipns_intents
  ADD COLUMN IF NOT EXISTS provider_bucket text;
UPDATE oracle_publication_ipns_intents intent
SET provider_bucket = CASE intent.domain
  WHEN 'open_data' THEN plan.plan_payload #>> '{targets,openData,bucket}'
  WHEN 'query_table' THEN plan.plan_payload #>> '{targets,queryTable,bucket}'
END
FROM oracle_publication_plans plan
WHERE plan.plan_id = intent.publication_plan_id
  AND intent.provider_bucket IS NULL;
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_publication_ipns_intents intent
    JOIN oracle_publication_plans plan
      ON plan.plan_id = intent.publication_plan_id
    JOIN oracle_publication_graph_roots root
      ON root.plan_id = intent.publication_plan_id
     AND root.domain = intent.domain
    WHERE intent.provider_bucket IS NULL OR
      intent.provider_target_identity != ('filebase:' || intent.provider_bucket) OR
      intent.ipns_label != CASE intent.domain
        WHEN 'open_data' THEN plan.plan_payload #>> '{targets,openData,ipnsLabel}'
        ELSE plan.plan_payload #>> '{targets,queryTable,ipnsLabel}'
      END OR
      intent.ipns_network_key != CASE intent.domain
        WHEN 'open_data' THEN plan.plan_payload #>> '{targets,openData,ipnsNetworkKey}'
        ELSE plan.plan_payload #>> '{targets,queryTable,ipnsNetworkKey}'
      END OR
      intent.approved_target_cid != root.expected_cid
  ) THEN
    RAISE EXCEPTION
      'migration 010 blocked: existing IPNS intent target differs from locked plan';
  END IF;
END;
$$;
ALTER TABLE oracle_publication_ipns_intents
  ALTER COLUMN provider_bucket SET NOT NULL;

ALTER TABLE oracle_publication_ipns_effects
  ADD COLUMN IF NOT EXISTS intent_id text;
ALTER TABLE oracle_publication_ipns_effects
  DROP CONSTRAINT IF EXISTS oracle_publication_ipns_effect_intent_fk;
ALTER TABLE oracle_publication_ipns_effects
  ADD CONSTRAINT oracle_publication_ipns_effect_intent_fk
  FOREIGN KEY (intent_id) REFERENCES oracle_publication_ipns_intents(intent_id);

ALTER TABLE oracle_publication_ipns_intent_state
  DROP CONSTRAINT IF EXISTS oracle_publication_ipns_intent_state_state_check;
ALTER TABLE oracle_publication_ipns_intent_state
  ADD CONSTRAINT oracle_publication_ipns_intent_state_state_check CHECK (
    state IN (
      'intent_recorded', 'prior_confirmed', 'update_in_flight',
      'update_ambiguous', 'mutation_acknowledged', 'verification_pending',
      'target_observed', 'verified', 'rollback_requested',
      'rollback_in_flight', 'rollback_ambiguous', 'rollback_verified',
      'manual_intervention_required', 'cancelled_terminal', 'failed_terminal'
    )
  );

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_mutation_receipts (
  receipt_id text PRIMARY KEY CHECK (receipt_id ~ '^ipnsreceipt_[a-f0-9]{32}$'),
  attempt_id text NOT NULL UNIQUE
    REFERENCES oracle_publication_ipns_mutation_attempts(attempt_id),
  intent_id text NOT NULL REFERENCES oracle_publication_ipns_intents(intent_id),
  outcome text NOT NULL CHECK (
    outcome IN ('acknowledged', 'failed', 'timeout', 'transport_ambiguous')
  ),
  provider_receipt_sha256 text CHECK (
    provider_receipt_sha256 IS NULL OR
    provider_receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  receipt_sha256 text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_resolution_cycles (
  resolution_cycle_id text PRIMARY KEY CHECK (
    resolution_cycle_id ~ '^resolution_[a-f0-9]{32}$'
  ),
  intent_id text NOT NULL REFERENCES oracle_publication_ipns_intents(intent_id),
  attempt_id text REFERENCES oracle_publication_ipns_mutation_attempts(attempt_id),
  intent_revision integer NOT NULL CHECK (intent_revision > 0),
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  observation_count integer NOT NULL CHECK (observation_count >= 2),
  observed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, attempt_id, resolution_cycle_id)
);

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_publication_ipns_mutation_receipts',
    'oracle_publication_ipns_resolution_cycles'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_immutable ON %I', immutable_table, immutable_table);
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_projection_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.sealed THEN
      RAISE EXCEPTION 'sealed projection snapshot is immutable';
    END IF;
    RETURN OLD;
  END IF;
  IF OLD.sealed THEN
    RAISE EXCEPTION 'sealed projection snapshot is immutable';
  END IF;
  IF NOT NEW.sealed OR
     (to_jsonb(NEW) - 'sealed') != (to_jsonb(OLD) - 'sealed') THEN
    RAISE EXCEPTION 'projection snapshot permits only guarded unsealed-to-sealed transition';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_projection_snapshots_guarded_seal
  ON oracle_projection_snapshots;
CREATE TRIGGER oracle_projection_snapshots_guarded_seal
  BEFORE UPDATE OR DELETE ON oracle_projection_snapshots
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_projection_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_ipns_intent_state_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('prism.ipns_intent_id', true) IS DISTINCT FROM NEW.intent_id THEN
    RAISE EXCEPTION 'IPNS intent state must use the authoritative intent ledger';
  END IF;
  IF NEW.revision != OLD.revision + 1 THEN
    RAISE EXCEPTION 'IPNS intent revision must advance exactly once';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_ipns_intent_state_guard
  ON oracle_publication_ipns_intent_state;
CREATE TRIGGER oracle_ipns_intent_state_guard
  BEFORE UPDATE ON oracle_publication_ipns_intent_state
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_ipns_intent_state_update();

CREATE OR REPLACE FUNCTION oracle_guard_ipns_effect_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.intent_id IS NULL OR
     current_setting('prism.ipns_intent_id', true) IS DISTINCT FROM NEW.intent_id THEN
    RAISE EXCEPTION 'IPNS effect must be checkpointed through its authoritative intent';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_ipns_effect_intent_guard
  ON oracle_publication_ipns_effects;
CREATE TRIGGER oracle_ipns_effect_intent_guard
  BEFORE UPDATE OR DELETE ON oracle_publication_ipns_effects
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_ipns_effect_update();

CREATE OR REPLACE FUNCTION oracle_guard_ipns_intent_target()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  payload jsonb;
  expected_bucket text;
  expected_label text;
  expected_network_key text;
  expected_target_cid text;
  expected_plan_sha text;
BEGIN
  SELECT plan_payload, plan_sha256
  INTO payload, expected_plan_sha
  FROM oracle_publication_plans
  WHERE plan_id = NEW.publication_plan_id;
  IF payload IS NULL OR expected_plan_sha != NEW.publication_plan_sha256 THEN
    RAISE EXCEPTION 'IPNS intent plan identity mismatch';
  END IF;
  expected_bucket := CASE NEW.domain
    WHEN 'open_data' THEN payload #>> '{targets,openData,bucket}'
    ELSE payload #>> '{targets,queryTable,bucket}'
  END;
  expected_label := CASE NEW.domain
    WHEN 'open_data' THEN payload #>> '{targets,openData,ipnsLabel}'
    ELSE payload #>> '{targets,queryTable,ipnsLabel}'
  END;
  expected_network_key := CASE NEW.domain
    WHEN 'open_data' THEN payload #>> '{targets,openData,ipnsNetworkKey}'
    ELSE payload #>> '{targets,queryTable,ipnsNetworkKey}'
  END;
  SELECT expected_cid INTO expected_target_cid
  FROM oracle_publication_graph_roots
  WHERE plan_id = NEW.publication_plan_id AND domain = NEW.domain;
  IF expected_bucket IS NULL OR expected_label IS NULL OR
     expected_network_key IS NULL OR expected_target_cid IS NULL OR
     NEW.provider_bucket != expected_bucket OR
     NEW.provider_target_identity != ('filebase:' || expected_bucket) OR
     NEW.ipns_label != expected_label OR
     NEW.ipns_network_key != expected_network_key OR
     NEW.approved_target_cid != expected_target_cid THEN
    RAISE EXCEPTION 'IPNS intent target must derive from the locked publication plan';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_ipns_intent_target_guard
  ON oracle_publication_ipns_intents;
CREATE TRIGGER oracle_ipns_intent_target_guard
  BEFORE INSERT ON oracle_publication_ipns_intents
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_ipns_intent_target();

CREATE OR REPLACE FUNCTION oracle_assert_publication_projection_fresh(
  checked_plan_id text,
  checked_plan_sha256 text
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  payload jsonb;
  version text;
  checked_mode text;
  checked_snapshot_id text;
  checked_scope_id text;
  checked_materialization_id text;
  checked_materialization_sha text;
  checked_authoritative_base text;
  checked_snapshot_content_sha text;
  actual_predecessor_chain jsonb;
BEGIN
  SELECT plan_payload, plan_version INTO payload, version
  FROM oracle_publication_plans
  WHERE plan_id = checked_plan_id AND plan_sha256 = checked_plan_sha256;
  IF payload IS NULL OR version != '1.1.0' THEN
    RAISE EXCEPTION 'publication requires a current v1.1 plan';
  END IF;
  checked_mode := payload #>> '{coverage,mode}';
  IF checked_mode = 'sample' THEN
    RAISE EXCEPTION 'sample publication cannot be approved or executed';
  END IF;
  checked_snapshot_id := payload #>> '{coverage,sourceSnapshotId}';
  checked_scope_id := payload #>> '{coverage,scopeId}';
  checked_materialization_id := payload #>> '{projection,materializationId}';
  checked_materialization_sha := payload #>> '{projection,materializationSha256}';
  checked_authoritative_base := payload #>> '{projection,authoritativeBaseSnapshotId}';
  checked_snapshot_content_sha := payload #>> '{projection,snapshotContentSha256}';
  WITH RECURSIVE predecessor_chain AS (
    SELECT snapshot.snapshot_id, snapshot.predecessor_snapshot_id,
           0 AS depth, ARRAY[snapshot.snapshot_id]::text[] AS visited
    FROM oracle_projection_snapshots snapshot
    WHERE snapshot.snapshot_id = checked_snapshot_id
    UNION ALL
    SELECT predecessor.snapshot_id, predecessor.predecessor_snapshot_id,
           chain.depth + 1, chain.visited || predecessor.snapshot_id
    FROM predecessor_chain chain
    JOIN oracle_projection_snapshots predecessor
      ON predecessor.snapshot_id = chain.predecessor_snapshot_id
    WHERE NOT predecessor.snapshot_id = ANY(chain.visited)
  )
  SELECT coalesce(jsonb_agg(snapshot_id ORDER BY depth), '[]'::jsonb)
  INTO actual_predecessor_chain
  FROM predecessor_chain;
  IF checked_snapshot_id IS NULL OR NOT EXISTS (
    SELECT 1
    FROM oracle_projection_heads head
    JOIN oracle_projection_snapshots snapshot
      ON snapshot.snapshot_id = head.current_snapshot_id
    JOIN oracle_projection_snapshots authoritative_base_snapshot
      ON authoritative_base_snapshot.snapshot_id =
         head.authoritative_base_snapshot_id
    JOIN oracle_projection_materializations materialization
      ON materialization.snapshot_id = snapshot.snapshot_id
    JOIN oracle_source_snapshots source_snapshot
      ON source_snapshot.snapshot_id = snapshot.snapshot_id
    WHERE head.scope_id = checked_scope_id
      AND head.current_snapshot_id = checked_snapshot_id
      AND head.authoritative_base_snapshot_id = checked_authoritative_base
      AND snapshot.sealed
      AND snapshot.scope_id = checked_scope_id
      AND snapshot.coverage_mode = checked_mode
      AND snapshot.content_sha256 = checked_snapshot_content_sha
      AND authoritative_base_snapshot.sealed
      AND authoritative_base_snapshot.coverage_mode = 'authoritative_complete'
      AND materialization.materialization_id = checked_materialization_id
      AND materialization.materialization_sha256 = checked_materialization_sha
      AND materialization.sealed
      AND source_snapshot.manifest_sha256 =
          payload #>> '{coverage,sourceSnapshotManifestSha256}'
      AND source_snapshot.coverage_metadata #>> '{selection,algorithm}' =
          payload #>> '{coverage,selection,algorithm}'
      AND source_snapshot.coverage_metadata #>> '{selection,seed}' =
          payload #>> '{coverage,selection,seed}'
      AND source_snapshot.coverage_metadata #>> '{selection,selectedRecordSha256}' =
          payload #>> '{coverage,selection,selectedRecordSha256}'
      AND (source_snapshot.coverage_metadata #>> '{selection,selectionSize}')::integer =
          (payload #>> '{coverage,selection,selectionSize}')::integer
      AND actual_predecessor_chain =
          payload #> '{coverage,predecessorChainSnapshotIds}'
  ) THEN
    RAISE EXCEPTION 'publication projection is stale or not sealed';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_publication_approval_freshness()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM oracle_assert_publication_projection_fresh(NEW.plan_id, NEW.plan_sha256);
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_publication_approval_freshness_guard
  ON oracle_publication_approvals;
CREATE TRIGGER oracle_publication_approval_freshness_guard
  BEFORE INSERT ON oracle_publication_approvals
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_publication_approval_freshness();

CREATE OR REPLACE FUNCTION oracle_guard_publication_state_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  exact_intents integer;
  verified_intents integer;
  verified_effects integer;
BEGIN
  IF NEW.state = 'executing' AND OLD.state IS DISTINCT FROM 'executing' THEN
    PERFORM oracle_assert_publication_projection_fresh(NEW.plan_id, NEW.plan_sha256);
  END IF;
  IF NEW.state = 'completed' AND OLD.state IS DISTINCT FROM 'completed' THEN
    SELECT
      count(*)::int,
      count(*) FILTER (
        WHERE intent.publication_plan_sha256 = NEW.plan_sha256
          AND state.state = 'verified'
      )::int
    INTO exact_intents, verified_intents
    FROM oracle_publication_ipns_intents intent
    JOIN oracle_publication_ipns_intent_state state
      ON state.intent_id = intent.intent_id
    WHERE intent.publication_plan_id = NEW.plan_id
      AND intent.domain IN ('open_data', 'query_table');

    SELECT count(*)::int INTO verified_effects
    FROM oracle_publication_ipns_effects effect
    JOIN oracle_publication_ipns_intents intent
      ON intent.intent_id = effect.intent_id
      AND intent.publication_plan_id = effect.plan_id
      AND intent.domain = effect.domain
    JOIN oracle_publication_ipns_intent_state state
      ON state.intent_id = intent.intent_id
    WHERE effect.plan_id = NEW.plan_id
      AND effect.status = 'verified'
      AND effect.mutation_performed
      AND effect.public_resolution_verified
      AND effect.target_cid = intent.approved_target_cid
      AND state.state = 'verified';

    IF exact_intents != 2 OR verified_intents != 2 OR verified_effects != 2 THEN
      RAISE EXCEPTION 'publication completion requires two exact verified intents and effects';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_publication_state_transition_guard
  ON oracle_publication_state;
CREATE TRIGGER oracle_publication_state_transition_guard
  BEFORE UPDATE ON oracle_publication_state
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_publication_state_transition();

CREATE OR REPLACE FUNCTION oracle_guard_publication_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_state text; current_plan text; unresolved integer;
BEGIN
  IF NEW.plan_version = '1.0.0' THEN
    RAISE EXCEPTION 'new legacy v1.0 publication plans are disabled';
  END IF;
  SELECT state, plan_id INTO current_state, current_plan
  FROM oracle_publication_state WHERE county = 'pasco' FOR UPDATE;
  IF current_plan IS NOT NULL AND current_plan != NEW.plan_id THEN
    IF current_state IN ('approved', 'executing', 'manual_intervention_required') THEN
      RAISE EXCEPTION 'publication plan replacement blocked while %', current_state;
    END IF;
    SELECT count(*) INTO unresolved
    FROM oracle_publication_ipns_intents intent
    JOIN oracle_publication_ipns_intent_state state USING (intent_id)
    WHERE intent.publication_plan_id = current_plan
      AND state.state NOT IN ('verified', 'rollback_verified', 'cancelled_terminal', 'failed_terminal');
    IF unresolved > 0 THEN
      RAISE EXCEPTION 'publication plan replacement blocked by unresolved IPNS intent';
    END IF;
    SELECT count(*) INTO unresolved
    FROM oracle_publication_object_effects
    WHERE plan_id = current_plan AND status != 'pending';
    IF unresolved > 0 AND current_state NOT IN ('completed', 'failed_terminal') THEN
      RAISE EXCEPTION 'publication plan replacement blocked by remote object effects';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE INDEX IF NOT EXISTS oracle_ipns_receipts_intent_idx
  ON oracle_publication_ipns_mutation_receipts(intent_id, recorded_at);
CREATE INDEX IF NOT EXISTS oracle_ipns_cycles_intent_idx
  ON oracle_publication_ipns_resolution_cycles(intent_id, observed_at);
