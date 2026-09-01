-- Preserve historical three-observation recovery evidence while requiring the
-- official Filebase gateway in every new four-observation recovery cycle.

CREATE OR REPLACE FUNCTION oracle_candidate_demo_observations_are_valid(
  observations jsonb,
  expected_count integer
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  expected_resolver text;
  item jsonb;
  keys text[];
  ordinal integer := 0;
BEGIN
  IF jsonb_typeof(observations) IS DISTINCT FROM 'array' OR
     jsonb_array_length(observations) IS DISTINCT FROM expected_count OR
     expected_count NOT IN (3, 4) THEN
    RETURN false;
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(observations) LOOP
    ordinal := ordinal + 1;
    expected_resolver := CASE
      WHEN ordinal = 1 THEN 'filebase_control'
      WHEN expected_count = 4 AND ordinal = 2 THEN 'filebase_gateway'
      WHEN ordinal = expected_count - 1 THEN 'ipfs_io'
      WHEN ordinal = expected_count THEN 'dweb_link'
      ELSE NULL
    END;
    IF jsonb_typeof(item) IS DISTINCT FROM 'object' OR
       expected_resolver IS NULL THEN
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
       item->>'resolver' IS DISTINCT FROM expected_resolver OR
       item->>'resolverType' IS DISTINCT FROM
         (CASE WHEN ordinal = 1 THEN 'control_plane' ELSE 'public_resolver' END) OR
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
    ELSIF jsonb_typeof(item->'observedCid') IS DISTINCT FROM 'null' THEN
      RETURN false;
    END IF;
  END LOOP;
  RETURN true;
END;
$$;

DO $$
DECLARE
  constraint_row record;
BEGIN
  FOR constraint_row IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'oracle_candidate_demo_resolution_cycles'::regclass
      AND contype = 'c'
      AND (
        pg_get_constraintdef(oid) LIKE '%observation_count = 3%' OR
        pg_get_constraintdef(oid) LIKE '%oracle_candidate_demo_observations_are_valid%'
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_demo_resolution_cycles DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;
END;
$$;

ALTER TABLE oracle_candidate_demo_resolution_cycles
  ADD CONSTRAINT oracle_candidate_demo_resolution_count_check
    CHECK (observation_count IN (3, 4)),
  ADD CONSTRAINT oracle_candidate_demo_resolution_evidence_check
    CHECK (
      oracle_candidate_demo_observations_are_valid(
        observations_canonical::jsonb,
        observation_count
      )
    );

COMMENT ON FUNCTION oracle_candidate_demo_observations_are_valid(jsonb, integer) IS
  'Validates legacy 3-resolver evidence and new control+Filebase+ipfs.io+dweb.link evidence without free-form metadata.';
