-- Additive publication graph, immutable projection, and pre-mutation IPNS
-- durability. Historical rows and the legacy 25k sample remain unchanged.

ALTER TABLE oracle_publication_plans
  DROP CONSTRAINT IF EXISTS oracle_publication_plans_plan_version_check;
ALTER TABLE oracle_publication_plans
  ADD CONSTRAINT oracle_publication_plans_plan_version_check
  CHECK (plan_version IN ('1.0.0', '1.1.0'));

CREATE TABLE IF NOT EXISTS oracle_projection_snapshots (
  snapshot_id text PRIMARY KEY REFERENCES oracle_source_snapshots(snapshot_id),
  run_id text NOT NULL UNIQUE REFERENCES oracle_pipeline_runs(run_id),
  county text NOT NULL CHECK (county = 'pasco'),
  coverage_mode text NOT NULL CHECK (
    coverage_mode IN ('partial', 'authoritative_complete')
  ),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  predecessor_snapshot_id text REFERENCES oracle_projection_snapshots(snapshot_id),
  authoritative_base_snapshot_id text REFERENCES oracle_projection_snapshots(snapshot_id),
  authority_source_system text NOT NULL CHECK (
    authority_source_system = 'pasco_appraiser'
  ),
  authority_source_identifier text NOT NULL,
  watermark_kind text NOT NULL CHECK (
    watermark_kind = 'pasco-appraiser-observation-end-v1'
  ),
  watermark_observed_through timestamptz NOT NULL,
  watermark_source_object_sha256 text NOT NULL CHECK (
    watermark_source_object_sha256 ~ '^[a-f0-9]{64}$'
  ),
  content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  completeness_evidence_sha256 text CHECK (
    completeness_evidence_sha256 IS NULL OR
    completeness_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  sealed boolean NOT NULL DEFAULT false,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (
    authority_source_identifier,
    watermark_observed_through,
    watermark_source_object_sha256
  ),
  CHECK (
    (coverage_mode = 'partial' AND completeness_evidence_sha256 IS NULL) OR
    (coverage_mode = 'authoritative_complete' AND completeness_evidence_sha256 IS NOT NULL)
  ),
  CHECK (
    authoritative_base_snapshot_id IS NULL OR predecessor_snapshot_id IS NOT NULL
  )
);

CREATE TABLE IF NOT EXISTS oracle_projection_heads (
  scope_id text PRIMARY KEY CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  county text NOT NULL CHECK (county = 'pasco'),
  current_snapshot_id text NOT NULL UNIQUE
    REFERENCES oracle_projection_snapshots(snapshot_id),
  authoritative_base_snapshot_id text
    REFERENCES oracle_projection_snapshots(snapshot_id),
  revision integer NOT NULL CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_sample_observation_sets (
  snapshot_id text PRIMARY KEY REFERENCES oracle_source_snapshots(snapshot_id),
  run_id text NOT NULL UNIQUE REFERENCES oracle_pipeline_runs(run_id),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  selection_sha256 text NOT NULL CHECK (selection_sha256 ~ '^[a-f0-9]{64}$'),
  selection_algorithm text NOT NULL,
  selection_seed text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_sample_property_versions (
  version_id text PRIMARY KEY CHECK (
    version_id ~ '^propertyversion_[a-f0-9]{32}$'
  ),
  snapshot_id text NOT NULL REFERENCES oracle_sample_observation_sets(snapshot_id),
  property_id text NOT NULL,
  parcel_identifier text NOT NULL,
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
  payload jsonb NOT NULL,
  source_record_sha256 text NOT NULL CHECK (
    source_record_sha256 ~ '^sha256:[a-f0-9]{64}$'
  ),
  UNIQUE (snapshot_id, property_id),
  UNIQUE (snapshot_id, parcel_identifier)
);

CREATE TABLE IF NOT EXISTS oracle_sample_fact_versions (
  version_id text PRIMARY KEY CHECK (version_id ~ '^factversion_[a-f0-9]{32}$'),
  snapshot_id text NOT NULL REFERENCES oracle_sample_observation_sets(snapshot_id),
  property_version_id text NOT NULL
    REFERENCES oracle_sample_property_versions(version_id),
  fact_type text NOT NULL,
  natural_key text NOT NULL,
  collection_semantics text NOT NULL CHECK (
    collection_semantics IN ('replace_set', 'positive_upsert', 'explicit_tombstone')
  ),
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
  payload jsonb NOT NULL,
  evidence_refs jsonb NOT NULL,
  UNIQUE (snapshot_id, property_version_id, fact_type, natural_key)
);

CREATE TABLE IF NOT EXISTS oracle_property_versions (
  version_id text PRIMARY KEY CHECK (
    version_id ~ '^propertyversion_[a-f0-9]{32}$'
  ),
  property_id text NOT NULL,
  parcel_identifier text NOT NULL,
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
  payload jsonb NOT NULL,
  source_record_sha256 text NOT NULL CHECK (
    source_record_sha256 ~ '^sha256:[a-f0-9]{64}$'
  ),
  source_snapshot_id text NOT NULL REFERENCES oracle_projection_snapshots(snapshot_id),
  source_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  UNIQUE (property_id, payload_sha256, source_record_sha256)
);

CREATE TABLE IF NOT EXISTS oracle_child_fact_versions (
  version_id text PRIMARY KEY CHECK (version_id ~ '^factversion_[a-f0-9]{32}$'),
  property_id text NOT NULL,
  fact_type text NOT NULL,
  natural_key text NOT NULL,
  collection_semantics text NOT NULL CHECK (
    collection_semantics IN ('replace_set', 'positive_upsert', 'explicit_tombstone')
  ),
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
  payload jsonb NOT NULL,
  source_record_sha256 text NOT NULL CHECK (
    source_record_sha256 ~ '^sha256:[a-f0-9]{64}$'
  ),
  source_snapshot_id text NOT NULL REFERENCES oracle_projection_snapshots(snapshot_id),
  source_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  evidence_refs jsonb NOT NULL,
  UNIQUE (
    property_id, fact_type, natural_key, payload_sha256, source_record_sha256
  )
);

CREATE TABLE IF NOT EXISTS oracle_projection_property_changes (
  snapshot_id text NOT NULL REFERENCES oracle_projection_snapshots(snapshot_id),
  property_id text NOT NULL,
  event_type text NOT NULL CHECK (
    event_type IN ('new', 'changed', 'unchanged', 'inactivated', 'reactivated')
  ),
  from_version_id text REFERENCES oracle_property_versions(version_id),
  to_version_id text REFERENCES oracle_property_versions(version_id),
  reason text,
  PRIMARY KEY (snapshot_id, property_id)
);

CREATE TABLE IF NOT EXISTS oracle_projection_fact_changes (
  snapshot_id text NOT NULL REFERENCES oracle_projection_snapshots(snapshot_id),
  property_id text NOT NULL,
  fact_type text NOT NULL,
  natural_key text NOT NULL,
  event_type text NOT NULL CHECK (
    event_type IN ('new', 'changed', 'unchanged', 'removed')
  ),
  from_version_id text REFERENCES oracle_child_fact_versions(version_id),
  to_version_id text REFERENCES oracle_child_fact_versions(version_id),
  PRIMARY KEY (snapshot_id, property_id, fact_type, natural_key)
);

CREATE TABLE IF NOT EXISTS oracle_projection_materializations (
  materialization_id text PRIMARY KEY CHECK (
    materialization_id ~ '^materialization_[a-f0-9]{32}$'
  ),
  snapshot_id text NOT NULL UNIQUE REFERENCES oracle_projection_snapshots(snapshot_id),
  scope_id text NOT NULL CHECK (scope_id ~ '^scope_[a-f0-9]{32}$'),
  materialization_sha256 text NOT NULL UNIQUE CHECK (
    materialization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  property_count integer NOT NULL CHECK (property_count >= 0),
  active_count integer NOT NULL CHECK (active_count >= 0),
  inactive_count integer NOT NULL CHECK (inactive_count >= 0),
  sealed boolean NOT NULL DEFAULT true CHECK (sealed),
  sealed_at timestamptz NOT NULL DEFAULT now(),
  CHECK (property_count = active_count + inactive_count)
);

CREATE TABLE IF NOT EXISTS oracle_projection_materialized_properties (
  materialization_id text NOT NULL
    REFERENCES oracle_projection_materializations(materialization_id),
  property_id text NOT NULL,
  property_version_id text NOT NULL REFERENCES oracle_property_versions(version_id),
  is_active boolean NOT NULL,
  inactivated_at_snapshot_id text REFERENCES oracle_projection_snapshots(snapshot_id),
  inactivation_watermark timestamptz,
  PRIMARY KEY (materialization_id, property_id),
  UNIQUE (materialization_id, property_version_id),
  CHECK (
    (is_active AND inactivated_at_snapshot_id IS NULL AND inactivation_watermark IS NULL) OR
    (NOT is_active AND inactivated_at_snapshot_id IS NOT NULL AND inactivation_watermark IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS oracle_projection_materialized_facts (
  materialization_id text NOT NULL
    REFERENCES oracle_projection_materializations(materialization_id),
  property_id text NOT NULL,
  fact_type text NOT NULL,
  natural_key text NOT NULL,
  fact_version_id text NOT NULL REFERENCES oracle_child_fact_versions(version_id),
  PRIMARY KEY (materialization_id, property_id, fact_type, natural_key),
  UNIQUE (materialization_id, fact_version_id),
  FOREIGN KEY (materialization_id, property_id)
    REFERENCES oracle_projection_materialized_properties(materialization_id, property_id)
);

CREATE TABLE IF NOT EXISTS oracle_publication_graph_objects (
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  object_key text NOT NULL CHECK (object_key !~ '(^/|(^|/)\.\.(/|$)|\\\\)'),
  object_role text NOT NULL CHECK (
    object_role IN ('property', 'shard', 'root', 'manifest', 'metadata', 'query_table')
  ),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_byte_size bigint NOT NULL CHECK (expected_byte_size >= 0),
  expected_cid text NOT NULL CHECK (expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  PRIMARY KEY (plan_id, domain, object_key),
  UNIQUE (plan_id, expected_cid)
);

CREATE TABLE IF NOT EXISTS oracle_publication_graph_edges (
  plan_id text NOT NULL,
  parent_domain text NOT NULL DEFAULT 'open_data' CHECK (parent_domain = 'open_data'),
  parent_key text NOT NULL,
  child_domain text NOT NULL DEFAULT 'open_data' CHECK (child_domain = 'open_data'),
  child_key text NOT NULL,
  child_cid text NOT NULL CHECK (child_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  json_pointer text NOT NULL,
  PRIMARY KEY (plan_id, parent_key, json_pointer),
  FOREIGN KEY (plan_id, parent_domain, parent_key)
    REFERENCES oracle_publication_graph_objects(plan_id, domain, object_key),
  FOREIGN KEY (plan_id, child_domain, child_key)
    REFERENCES oracle_publication_graph_objects(plan_id, domain, object_key)
);

CREATE TABLE IF NOT EXISTS oracle_publication_graph_roots (
  plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  object_key text NOT NULL,
  expected_cid text NOT NULL CHECK (expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  PRIMARY KEY (plan_id, domain),
  FOREIGN KEY (plan_id, domain, object_key)
    REFERENCES oracle_publication_graph_objects(plan_id, domain, object_key)
);

ALTER TABLE oracle_publication_object_effects
  ADD COLUMN IF NOT EXISTS expected_cid text,
  ADD COLUMN IF NOT EXISTS graph_object_key text;
ALTER TABLE oracle_publication_object_effects
  ADD CONSTRAINT oracle_publication_effect_expected_cid_check CHECK (
    expected_cid IS NULL OR expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  ADD CONSTRAINT oracle_publication_effect_graph_fk FOREIGN KEY (
    plan_id, domain, graph_object_key
  ) REFERENCES oracle_publication_graph_objects(plan_id, domain, object_key);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_intents (
  intent_id text PRIMARY KEY CHECK (intent_id ~ '^ipnsintent_[a-f0-9]{32}$'),
  intent_sha256 text NOT NULL UNIQUE CHECK (intent_sha256 ~ '^[a-f0-9]{64}$'),
  publication_plan_id text NOT NULL REFERENCES oracle_publication_plans(plan_id),
  publication_plan_sha256 text NOT NULL CHECK (publication_plan_sha256 ~ '^[a-f0-9]{64}$'),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  provider_target_identity text NOT NULL,
  ipns_label text NOT NULL,
  ipns_network_key text NOT NULL CHECK (ipns_network_key ~ '^k51[1-9A-HJ-NP-Za-km-z]+$'),
  prior_cid text NOT NULL CHECK (prior_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  approved_target_cid text NOT NULL CHECK (
    approved_target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  resolution_evidence jsonb NOT NULL,
  resolution_evidence_sha256 text NOT NULL CHECK (
    resolution_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  intended_at timestamptz NOT NULL,
  UNIQUE (publication_plan_id, domain),
  FOREIGN KEY (publication_plan_id, domain)
    REFERENCES oracle_publication_graph_roots(plan_id, domain)
);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_intent_state (
  intent_id text PRIMARY KEY REFERENCES oracle_publication_ipns_intents(intent_id),
  state text NOT NULL CHECK (state IN (
    'intent_recorded', 'prior_confirmed', 'update_in_flight',
    'update_ambiguous', 'target_observed', 'verified',
    'rollback_recorded', 'rollback_in_flight', 'rollback_ambiguous',
    'rolled_back', 'unexpected_cid', 'failed_terminal'
  )),
  revision integer NOT NULL CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_intent_events (
  event_id text PRIMARY KEY CHECK (event_id ~ '^ipnsevent_[a-f0-9]{32}$'),
  intent_id text NOT NULL REFERENCES oracle_publication_ipns_intents(intent_id),
  revision integer NOT NULL CHECK (revision > 0),
  from_state text,
  to_state text NOT NULL,
  event_sha256 text NOT NULL CHECK (event_sha256 ~ '^[a-f0-9]{64}$'),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, revision),
  UNIQUE (intent_id, event_sha256)
);

CREATE TABLE IF NOT EXISTS oracle_publication_ipns_mutation_attempts (
  attempt_id text PRIMARY KEY CHECK (attempt_id ~ '^ipnsattempt_[a-f0-9]{32}$'),
  intent_id text NOT NULL REFERENCES oracle_publication_ipns_intents(intent_id),
  revision integer NOT NULL CHECK (revision > 0),
  direction text NOT NULL CHECK (direction IN ('update', 'rollback')),
  target_cid text NOT NULL CHECK (target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'),
  request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  outcome text NOT NULL CHECK (
    outcome IN ('recorded', 'returned', 'timeout', 'transport_ambiguous')
  ),
  receipt_sha256 text CHECK (receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, revision)
);

CREATE OR REPLACE FUNCTION oracle_reject_immutable_publication_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$;

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_sample_observation_sets', 'oracle_sample_property_versions',
    'oracle_sample_fact_versions', 'oracle_property_versions',
    'oracle_child_fact_versions', 'oracle_projection_property_changes',
    'oracle_projection_fact_changes', 'oracle_projection_materializations',
    'oracle_projection_materialized_properties',
    'oracle_projection_materialized_facts', 'oracle_publication_graph_objects',
    'oracle_publication_graph_edges', 'oracle_publication_graph_roots',
    'oracle_publication_ipns_intents', 'oracle_publication_ipns_intent_events',
    'oracle_publication_ipns_mutation_attempts'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_immutable ON %I', immutable_table, immutable_table);
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_publication_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_state text; current_plan text; unresolved integer;
BEGIN
  SELECT state, plan_id INTO current_state, current_plan
  FROM oracle_publication_state WHERE county = 'pasco' FOR UPDATE;
  IF current_plan IS NOT NULL AND current_plan != NEW.plan_id THEN
    IF current_state IN ('approved', 'executing') THEN
      RAISE EXCEPTION 'publication plan replacement blocked while %', current_state;
    END IF;
    SELECT count(*) INTO unresolved
    FROM oracle_publication_ipns_intents i
    JOIN oracle_publication_ipns_intent_state s USING (intent_id)
    WHERE i.publication_plan_id = current_plan
      AND s.state NOT IN ('verified', 'rolled_back', 'failed_terminal');
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
DROP TRIGGER IF EXISTS oracle_publication_plan_insert_guard ON oracle_publication_plans;
CREATE TRIGGER oracle_publication_plan_insert_guard
  BEFORE INSERT ON oracle_publication_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_publication_plan_insert();

CREATE OR REPLACE FUNCTION oracle_guard_publication_approval()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_version text;
  approvable boolean;
  stored_plan_sha256 text;
  payload jsonb;
  object_count integer;
  expected_object_count integer;
  edge_count integer;
  expected_edge_count integer;
  root_count integer;
  mismatch_count integer;
BEGIN
  SELECT p.plan_version, p.approvable, p.plan_sha256, p.plan_payload
  INTO plan_version, approvable, stored_plan_sha256, payload
  FROM oracle_publication_plans p WHERE p.plan_id = NEW.plan_id;
  IF stored_plan_sha256 IS NULL OR NEW.plan_sha256 != stored_plan_sha256 OR
     payload->>'planId' != NEW.plan_id OR
     payload->>'planSha256' != stored_plan_sha256 THEN
    RAISE EXCEPTION 'publication approval does not match the stored plan identity';
  END IF;
  IF NOT approvable THEN RAISE EXCEPTION 'publication plan is not approvable'; END IF;
  IF plan_version = '1.1.0' THEN
    SELECT count(*) INTO object_count FROM oracle_publication_graph_objects
      WHERE plan_id = NEW.plan_id;
    expected_object_count := jsonb_array_length(
      payload #> '{artifacts,objectInventory}'
    );
    SELECT count(*) INTO edge_count FROM oracle_publication_graph_edges
      WHERE plan_id = NEW.plan_id;
    expected_edge_count := jsonb_array_length(payload #> '{graph,edges}');
    SELECT count(*) INTO root_count FROM oracle_publication_graph_roots
      WHERE plan_id = NEW.plan_id;
    IF object_count != expected_object_count OR
       edge_count != expected_edge_count OR root_count != 2 THEN
      RAISE EXCEPTION 'publication graph is incomplete';
    END IF;

    SELECT count(*) INTO mismatch_count
    FROM jsonb_array_elements(payload #> '{artifacts,objectInventory}') item
    WHERE NOT EXISTS (
      SELECT 1 FROM oracle_publication_graph_objects object
      WHERE object.plan_id = NEW.plan_id
        AND object.domain = item->>'domain'
        AND object.object_key = item->>'objectKey'
        AND object.object_role = item->>'role'
        AND object.expected_sha256 = item->>'sha256'
        AND object.expected_byte_size = (item->>'byteSize')::bigint
        AND object.expected_cid = item->>'expectedCid'
    );
    IF mismatch_count != 0 THEN
      RAISE EXCEPTION 'publication graph objects do not match plan payload';
    END IF;

    SELECT count(*) INTO mismatch_count
    FROM jsonb_array_elements(payload #> '{graph,edges}') edge
    WHERE NOT EXISTS (
      SELECT 1 FROM oracle_publication_graph_edges stored_edge
      WHERE stored_edge.plan_id = NEW.plan_id
        AND stored_edge.parent_key = edge->>'parentKey'
        AND stored_edge.child_key = edge->>'childKey'
        AND stored_edge.child_cid = edge->>'childCid'
        AND stored_edge.json_pointer = edge->>'jsonPointer'
    );
    IF mismatch_count != 0 THEN
      RAISE EXCEPTION 'publication graph edges do not match plan payload';
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM oracle_publication_graph_roots root
      WHERE root.plan_id = NEW.plan_id AND root.domain = 'open_data'
        AND root.object_key = payload #>> '{graph,openDataRoot,objectKey}'
        AND root.expected_cid = payload #>> '{graph,openDataRoot,expectedCid}'
    ) OR NOT EXISTS (
      SELECT 1 FROM oracle_publication_graph_roots root
      WHERE root.plan_id = NEW.plan_id AND root.domain = 'query_table'
        AND root.object_key = payload #>> '{graph,queryTableRoot,objectKey}'
        AND root.expected_cid = payload #>> '{graph,queryTableRoot,expectedCid}'
    ) THEN
      RAISE EXCEPTION 'publication graph roots do not match plan payload';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS oracle_publication_approval_graph_guard ON oracle_publication_approvals;
CREATE TRIGGER oracle_publication_approval_graph_guard
  BEFORE INSERT ON oracle_publication_approvals
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_publication_approval();

CREATE INDEX IF NOT EXISTS oracle_projection_snapshots_scope_watermark_idx
  ON oracle_projection_snapshots(scope_id, watermark_observed_through);
CREATE INDEX IF NOT EXISTS oracle_projection_materialized_active_idx
  ON oracle_projection_materialized_properties(materialization_id, is_active, property_id);
CREATE INDEX IF NOT EXISTS oracle_ipns_intent_plan_state_idx
  ON oracle_publication_ipns_intents(publication_plan_id, domain);
