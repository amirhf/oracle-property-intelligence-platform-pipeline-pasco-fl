-- Assessment trust-model guards. PostgreSQL is private to the trusted Oracle
-- service/operator. A real executor still requires separate migration/runtime
-- roles, DML revocation, and DB-owned transition procedures.

ALTER TABLE oracle_publication_approvals
  ADD COLUMN IF NOT EXISTS validated_scope_id text,
  ADD COLUMN IF NOT EXISTS validated_snapshot_id text,
  ADD COLUMN IF NOT EXISTS validated_authoritative_base_snapshot_id text,
  ADD COLUMN IF NOT EXISTS validated_materialization_id text,
  ADD COLUMN IF NOT EXISTS validated_materialization_sha256 text,
  ADD COLUMN IF NOT EXISTS validated_head_revision integer,
  ADD COLUMN IF NOT EXISTS approval_revision integer NOT NULL DEFAULT 1;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM oracle_publication_approvals
    WHERE validated_scope_id IS NULL
       OR validated_snapshot_id IS NULL
       OR validated_authoritative_base_snapshot_id IS NULL
       OR validated_materialization_id IS NULL
       OR validated_materialization_sha256 IS NULL
       OR validated_head_revision IS NULL
  ) THEN
    RAISE EXCEPTION
      'migration 011 blocked: an approval lacks reconstructible projection binding';
  END IF;
END;
$$;

ALTER TABLE oracle_publication_approvals
  ALTER COLUMN validated_scope_id SET NOT NULL,
  ALTER COLUMN validated_snapshot_id SET NOT NULL,
  ALTER COLUMN validated_authoritative_base_snapshot_id SET NOT NULL,
  ALTER COLUMN validated_materialization_id SET NOT NULL,
  ALTER COLUMN validated_materialization_sha256 SET NOT NULL,
  ALTER COLUMN validated_head_revision SET NOT NULL,
  ADD CONSTRAINT oracle_publication_approvals_scope_check
    CHECK (validated_scope_id ~ '^scope_[a-f0-9]{32}$'),
  ADD CONSTRAINT oracle_publication_approvals_snapshot_check
    CHECK (validated_snapshot_id ~ '^snapshot_[a-f0-9]{32}$'),
  ADD CONSTRAINT oracle_publication_approvals_authoritative_base_check
    CHECK (validated_authoritative_base_snapshot_id ~ '^snapshot_[a-f0-9]{32}$'),
  ADD CONSTRAINT oracle_publication_approvals_materialization_check
    CHECK (validated_materialization_id ~ '^materialization_[a-f0-9]{32}$'),
  ADD CONSTRAINT oracle_publication_approvals_materialization_sha_check
    CHECK (validated_materialization_sha256 ~ '^[a-f0-9]{64}$'),
  ADD CONSTRAINT oracle_publication_approvals_head_revision_check
    CHECK (validated_head_revision > 0),
  ADD CONSTRAINT oracle_publication_approvals_revision_check
    CHECK (approval_revision = 1);

CREATE OR REPLACE FUNCTION oracle_assert_publication_approval_binding_fresh(
  checked_plan_id text,
  checked_plan_sha256 text
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  payload jsonb;
BEGIN
  PERFORM oracle_assert_publication_projection_fresh(
    checked_plan_id,
    checked_plan_sha256
  );
  SELECT plan_payload INTO payload
  FROM oracle_publication_plans
  WHERE plan_id = checked_plan_id AND plan_sha256 = checked_plan_sha256;

  IF payload IS NULL OR NOT EXISTS (
    SELECT 1
    FROM oracle_publication_approvals approval
    JOIN oracle_projection_heads head
      ON head.scope_id = approval.validated_scope_id
    JOIN oracle_projection_materializations materialization
      ON materialization.materialization_id =
         approval.validated_materialization_id
    WHERE approval.plan_id = checked_plan_id
      AND approval.plan_sha256 = checked_plan_sha256
      AND approval.validated_scope_id = payload #>> '{coverage,scopeId}'
      AND approval.validated_snapshot_id =
          payload #>> '{coverage,sourceSnapshotId}'
      AND approval.validated_authoritative_base_snapshot_id =
          payload #>> '{projection,authoritativeBaseSnapshotId}'
      AND approval.validated_materialization_id =
          payload #>> '{projection,materializationId}'
      AND approval.validated_materialization_sha256 =
          payload #>> '{projection,materializationSha256}'
      AND head.current_snapshot_id = approval.validated_snapshot_id
      AND head.authoritative_base_snapshot_id =
          approval.validated_authoritative_base_snapshot_id
      AND head.revision = approval.validated_head_revision
      AND materialization.snapshot_id = approval.validated_snapshot_id
      AND materialization.materialization_sha256 =
          approval.validated_materialization_sha256
      AND materialization.sealed
  ) THEN
    RAISE EXCEPTION 'publication approval projection binding is stale';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_publication_approval_freshness()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  payload jsonb;
BEGIN
  PERFORM oracle_assert_publication_projection_fresh(NEW.plan_id, NEW.plan_sha256);
  SELECT plan_payload INTO payload
  FROM oracle_publication_plans
  WHERE plan_id = NEW.plan_id AND plan_sha256 = NEW.plan_sha256;
  IF payload IS NULL OR NOT EXISTS (
    SELECT 1
    FROM oracle_projection_heads head
    JOIN oracle_projection_materializations materialization
      ON materialization.snapshot_id = head.current_snapshot_id
    WHERE head.scope_id = NEW.validated_scope_id
      AND head.current_snapshot_id = NEW.validated_snapshot_id
      AND head.authoritative_base_snapshot_id =
          NEW.validated_authoritative_base_snapshot_id
      AND head.revision = NEW.validated_head_revision
      AND materialization.materialization_id = NEW.validated_materialization_id
      AND materialization.materialization_sha256 =
          NEW.validated_materialization_sha256
      AND materialization.sealed
      AND NEW.validated_scope_id = payload #>> '{coverage,scopeId}'
      AND NEW.validated_snapshot_id = payload #>> '{coverage,sourceSnapshotId}'
      AND NEW.validated_authoritative_base_snapshot_id =
          payload #>> '{projection,authoritativeBaseSnapshotId}'
      AND NEW.validated_materialization_id =
          payload #>> '{projection,materializationId}'
      AND NEW.validated_materialization_sha256 =
          payload #>> '{projection,materializationSha256}'
  ) THEN
    RAISE EXCEPTION 'approval must persist the exact fenced projection binding';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_publication_approval_freshness_guard
  ON oracle_publication_approvals;
CREATE TRIGGER oracle_publication_approval_freshness_guard
  BEFORE INSERT ON oracle_publication_approvals
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_publication_approval_freshness();

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_publication_plans',
    'oracle_publication_approvals'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I_identity_immutable ON %I',
      immutable_table,
      immutable_table
    );
    EXECUTE format(
      'CREATE TRIGGER %I_identity_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation()',
      immutable_table,
      immutable_table
    );
  END LOOP;
END;
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM oracle_publication_ipns_resolution_cycles) THEN
    RAISE EXCEPTION
      'migration 011 blocked: digest-only recovery cycles require manual review';
  END IF;
END;
$$;

ALTER TABLE oracle_publication_ipns_resolution_cycles
  ADD COLUMN IF NOT EXISTS observations_canonical text,
  ADD COLUMN IF NOT EXISTS classification text;

ALTER TABLE oracle_publication_ipns_resolution_cycles
  ALTER COLUMN observations_canonical SET NOT NULL,
  ALTER COLUMN classification SET NOT NULL,
  ADD CONSTRAINT oracle_ipns_resolution_cycles_classification_check CHECK (
    classification IN (
      'prior_observed', 'target_observed', 'split_prior_target',
      'timeout_transport_uncertainty', 'unexpected_third_cid'
    )
  ),
  ADD CONSTRAINT oracle_ipns_resolution_cycles_evidence_size_check CHECK (
    octet_length(observations_canonical) BETWEEN 2 AND 32768
  );

CREATE OR REPLACE FUNCTION oracle_classify_ipns_observations(
  checked_intent_id text,
  observations_text text
)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  observations jsonb;
  prior text;
  target text;
  resolved_count integer;
  nonresolved_count integer;
  unknown_count integer;
  prior_count integer;
  target_count integer;
BEGIN
  observations := observations_text::jsonb;
  IF jsonb_typeof(observations) != 'array' THEN
    RAISE EXCEPTION 'resolution observations must be a JSON array';
  END IF;
  SELECT prior_cid, approved_target_cid INTO prior, target
  FROM oracle_publication_ipns_intents WHERE intent_id = checked_intent_id;
  IF prior IS NULL OR target IS NULL THEN
    RAISE EXCEPTION 'resolution intent is unknown';
  END IF;
  SELECT
    count(*) FILTER (WHERE item->>'classification' = 'resolved')::int,
    count(*) FILTER (WHERE item->>'classification' != 'resolved')::int,
    count(*) FILTER (
      WHERE item->>'classification' = 'resolved'
        AND item->>'observedCid' NOT IN (prior, target)
    )::int,
    count(*) FILTER (
      WHERE item->>'classification' = 'resolved'
        AND item->>'observedCid' = prior
    )::int,
    count(*) FILTER (
      WHERE item->>'classification' = 'resolved'
        AND item->>'observedCid' = target
    )::int
  INTO resolved_count, nonresolved_count, unknown_count, prior_count, target_count
  FROM jsonb_array_elements(observations) item;

  IF unknown_count > 0 THEN RETURN 'unexpected_third_cid'; END IF;
  IF prior_count > 0 AND target_count > 0 THEN RETURN 'split_prior_target'; END IF;
  IF nonresolved_count > 0 OR resolved_count = 0 THEN
    RETURN 'timeout_transport_uncertainty';
  END IF;
  IF prior_count = resolved_count THEN RETURN 'prior_observed'; END IF;
  IF target_count = resolved_count THEN RETURN 'target_observed'; END IF;
  RAISE EXCEPTION 'resolution observations cannot be classified';
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_ipns_resolution_cycle_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  observations jsonb;
  actual_classification text;
BEGIN
  observations := NEW.observations_canonical::jsonb;
  IF jsonb_typeof(observations) != 'array' OR
     jsonb_array_length(observations) != NEW.observation_count THEN
    RAISE EXCEPTION 'resolution evidence count mismatch';
  END IF;
  IF encode(sha256(convert_to(NEW.observations_canonical, 'UTF8')), 'hex') !=
     NEW.evidence_sha256 THEN
    RAISE EXCEPTION 'resolution evidence SHA-256 mismatch';
  END IF;
  IF EXISTS (
    SELECT 1 FROM jsonb_array_elements(observations) item
    WHERE (item->>'ordinal')::integer < 0
       OR item->>'endpointId' IS NULL
       OR item->>'resolverKind' NOT IN ('filebase_control_plane', 'public_gateway')
       OR item->>'classification' NOT IN ('resolved', 'unavailable', 'error')
       OR octet_length(coalesce((item->'receiptMetadata')::text, '{}')) > 4096
  ) OR (
    SELECT count(DISTINCT (item->>'ordinal')::integer)
    FROM jsonb_array_elements(observations) item
  ) != NEW.observation_count THEN
    RAISE EXCEPTION 'resolution evidence is malformed or reuses an ordinal';
  END IF;
  actual_classification := oracle_classify_ipns_observations(
    NEW.intent_id,
    NEW.observations_canonical
  );
  IF actual_classification != NEW.classification THEN
    RAISE EXCEPTION 'resolution evidence classification mismatch';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_ipns_resolution_cycle_evidence_guard
  ON oracle_publication_ipns_resolution_cycles;
CREATE TRIGGER oracle_ipns_resolution_cycle_evidence_guard
  BEFORE INSERT ON oracle_publication_ipns_resolution_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_ipns_resolution_cycle_evidence();

DROP TRIGGER IF EXISTS oracle_publication_ipns_resolution_cycles_immutable
  ON oracle_publication_ipns_resolution_cycles;
CREATE TRIGGER oracle_publication_ipns_resolution_cycles_immutable
  BEFORE UPDATE OR DELETE ON oracle_publication_ipns_resolution_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_publication_state_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  exact_intents integer;
  verified_intents integer;
  verified_effects integer;
BEGIN
  IF NEW.state = 'executing' AND OLD.state IS DISTINCT FROM 'executing' THEN
    PERFORM oracle_assert_publication_approval_binding_fresh(
      NEW.plan_id,
      NEW.plan_sha256
    );
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
      RAISE EXCEPTION
        'publication completion requires two exact verified intents and effects';
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

COMMENT ON FUNCTION oracle_assert_publication_approval_binding_fresh(text, text)
IS 'Trusted-service assessment guard. Reacquire the shared application projection-head fence before calling; real execution additionally requires DB-owned procedures and role separation.';
