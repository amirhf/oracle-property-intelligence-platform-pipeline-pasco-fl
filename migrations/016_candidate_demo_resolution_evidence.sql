-- Immutable, bounded resolver evidence for candidate-demo IPNS recovery.
-- This is isolated from the owner/canonical publication ledger and contains no
-- credentials, URLs, response bodies, headers, or other free-form metadata.

CREATE OR REPLACE FUNCTION oracle_candidate_demo_cid_is_valid(value text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT value ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$';
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_demo_observations_are_valid(
  observations jsonb,
  expected_count integer
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  item jsonb;
  keys text[];
  ordinal integer := 0;
BEGIN
  IF jsonb_typeof(observations) IS DISTINCT FROM 'array' OR
     jsonb_array_length(observations) IS DISTINCT FROM expected_count OR
     expected_count IS DISTINCT FROM 3 THEN
    RETURN false;
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(observations) LOOP
    ordinal := ordinal + 1;
    IF jsonb_typeof(item) IS DISTINCT FROM 'object' THEN
      RETURN false;
    END IF;
    SELECT array_agg(key ORDER BY key) INTO keys
    FROM jsonb_object_keys(item) AS key;
    IF keys IS DISTINCT FROM ARRAY[
      'cacheAgeSeconds', 'httpStatus', 'observedAt', 'observedCid',
      'ordinal', 'outcome', 'resolver', 'resolverType', 'responseBytes',
      'responseSha256'
    ]::text[] THEN
      RETURN false;
    END IF;
    IF (item->>'ordinal') !~ '^[0-9]+$' OR
       (item->>'ordinal')::integer IS DISTINCT FROM ordinal OR
       item->>'resolver' NOT IN ('filebase_control', 'ipfs_io', 'dweb_link') OR
       item->>'resolverType' NOT IN ('control_plane', 'public_resolver') OR
       ((item->>'resolver' = 'filebase_control') IS DISTINCT FROM
         (item->>'resolverType' = 'control_plane')) OR
       item->>'outcome' NOT IN (
         'resolved', 'unavailable', 'timeout', 'http_error', 'transport_error'
       ) OR
       (item->>'observedAt') IS NULL OR
       (item->>'observedAt') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$' OR
       (item->>'responseSha256') !~ '^[a-f0-9]{64}$' OR
       jsonb_typeof(item->'responseBytes') IS DISTINCT FROM 'number' OR
       (item->>'responseBytes') !~ '^[0-9]+$' OR
       (item->>'responseBytes')::numeric > 65536 OR
       NOT (
         jsonb_typeof(item->'httpStatus') = 'null' OR
         (jsonb_typeof(item->'httpStatus') = 'number' AND
          (item->>'httpStatus') ~ '^[0-9]+$' AND
          (item->>'httpStatus')::integer BETWEEN 100 AND 599)
       ) OR
       NOT (
         jsonb_typeof(item->'cacheAgeSeconds') = 'null' OR
         (jsonb_typeof(item->'cacheAgeSeconds') = 'number' AND
          (item->>'cacheAgeSeconds') ~ '^[0-9]+$' AND
          (item->>'cacheAgeSeconds')::integer BETWEEN 0 AND 3600)
       ) OR
       NOT (
         jsonb_typeof(item->'observedCid') = 'null' OR
         (jsonb_typeof(item->'observedCid') = 'string' AND
          oracle_candidate_demo_cid_is_valid(item->>'observedCid'))
       ) THEN
      RETURN false;
    END IF;
    IF item->>'outcome' = 'resolved' THEN
      IF jsonb_typeof(item->'observedCid') IS DISTINCT FROM 'string' OR
         jsonb_typeof(item->'httpStatus') IS DISTINCT FROM 'number' THEN
        RETURN false;
      END IF;
    ELSE
      IF jsonb_typeof(item->'observedCid') IS DISTINCT FROM 'null' THEN
        RETURN false;
      END IF;
    END IF;
  END LOOP;
  RETURN true;
END;
$$;

CREATE TABLE IF NOT EXISTS oracle_candidate_demo_resolution_cycles (
  cycle_id text PRIMARY KEY CHECK (cycle_id ~ '^democycle_[a-f0-9]{32}$'),
  intent_id text NOT NULL REFERENCES oracle_candidate_demo_ipns_intents(intent_id),
  demo_plan_id text NOT NULL REFERENCES oracle_candidate_demo_plans(demo_plan_id),
  demo_plan_sha256 text NOT NULL CHECK (demo_plan_sha256 ~ '^[a-f0-9]{64}$'),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  sequence integer NOT NULL CHECK (sequence > 0),
  classification text NOT NULL CHECK (classification IN (
    'prior_observed', 'target_observed', 'split', 'unavailable', 'unexpected_cid'
  )),
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  observation_count integer NOT NULL CHECK (observation_count = 3),
  observations_canonical text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (intent_id, sequence),
  UNIQUE (intent_id, evidence_sha256),
  CHECK (
    oracle_candidate_demo_observations_are_valid(
      observations_canonical::jsonb,
      observation_count
    )
  )
);

DROP TRIGGER IF EXISTS oracle_candidate_demo_resolution_cycles_immutable
  ON oracle_candidate_demo_resolution_cycles;
CREATE TRIGGER oracle_candidate_demo_resolution_cycles_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_demo_resolution_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_demo_identity_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_demo_cycle_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  intent_row oracle_candidate_demo_ipns_intents%ROWTYPE;
BEGIN
  SELECT * INTO intent_row
  FROM oracle_candidate_demo_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF NOT FOUND OR
     intent_row.demo_plan_id IS DISTINCT FROM NEW.demo_plan_id OR
     intent_row.demo_plan_sha256 IS DISTINCT FROM NEW.demo_plan_sha256 OR
     intent_row.domain IS DISTINCT FROM NEW.domain THEN
    RAISE EXCEPTION 'candidate recovery cycle does not match its immutable intent';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_candidate_demo_cycle_binding_guard
  ON oracle_candidate_demo_resolution_cycles;
CREATE TRIGGER oracle_candidate_demo_cycle_binding_guard
  BEFORE INSERT ON oracle_candidate_demo_resolution_cycles
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_demo_cycle_binding();

COMMENT ON TABLE oracle_candidate_demo_resolution_cycles IS
  'Immutable candidate-demo recovery observations; bounded metadata only, never credentials or response bodies.';
