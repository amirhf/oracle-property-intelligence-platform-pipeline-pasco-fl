-- Strict, reconstructible recovery receipts and server-derived, intent-bound
-- resolution-cycle identities. Migrations 009-011 are immutable local history;
-- this additive migration requires their development evidence tables to be empty.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM oracle_publication_ipns_resolution_cycles) OR
     EXISTS (SELECT 1 FROM oracle_publication_ipns_mutation_receipts) THEN
    RAISE EXCEPTION
      'migration 012 blocked: existing recovery cycles or receipts require manual review';
  END IF;
END;
$$;

ALTER TABLE oracle_publication_ipns_resolution_cycles
  ADD COLUMN IF NOT EXISTS domain text,
  ADD COLUMN IF NOT EXISTS cycle_sequence integer,
  ADD COLUMN IF NOT EXISTS receipts_canonical text,
  ADD COLUMN IF NOT EXISTS receipt_identity_sha256 text;

ALTER TABLE oracle_publication_ipns_resolution_cycles
  ALTER COLUMN domain SET NOT NULL,
  ALTER COLUMN cycle_sequence SET NOT NULL,
  ALTER COLUMN receipts_canonical SET NOT NULL,
  ALTER COLUMN receipt_identity_sha256 SET NOT NULL,
  ADD CONSTRAINT oracle_ipns_resolution_cycles_domain_check CHECK (
    domain IN ('open_data', 'query_table')
  ),
  ADD CONSTRAINT oracle_ipns_resolution_cycles_sequence_check CHECK (
    cycle_sequence BETWEEN 0 AND 1000000
  ),
  ADD CONSTRAINT oracle_ipns_resolution_cycles_receipt_sha_check CHECK (
    receipt_identity_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_ipns_resolution_cycles_receipts_size_check CHECK (
    octet_length(receipts_canonical) BETWEEN 2 AND 16384
  );

CREATE UNIQUE INDEX oracle_ipns_resolution_cycles_intent_evidence_unique
  ON oracle_publication_ipns_resolution_cycles(intent_id, evidence_sha256);
CREATE UNIQUE INDEX oracle_ipns_resolution_cycles_bound_sequence_unique
  ON oracle_publication_ipns_resolution_cycles(
    intent_id,
    domain,
    coalesce(attempt_id, ''),
    cycle_sequence
  );

CREATE OR REPLACE FUNCTION oracle_expected_ipns_resolution_cycle_id(
  checked_intent_id text,
  checked_attempt_id text,
  checked_sequence integer
)
RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
  identity_json text;
  intent_record oracle_publication_ipns_intents%ROWTYPE;
BEGIN
  SELECT * INTO intent_record
  FROM oracle_publication_ipns_intents
  WHERE intent_id = checked_intent_id;
  IF intent_record.intent_id IS NULL THEN
    RAISE EXCEPTION 'resolution intent is unknown';
  END IF;
  identity_json :=
    '["1.0.0","Publish/pasco/ipns-resolution-cycle","' ||
    intent_record.publication_plan_id || '","' ||
    intent_record.publication_plan_sha256 || '","' ||
    intent_record.intent_id || '","' || intent_record.domain || '","' ||
    coalesce(checked_attempt_id, 'none') || '","' ||
    checked_sequence::text || '"]';
  RETURN 'resolution_' || substring(
    encode(sha256(convert_to(identity_json, 'UTF8')), 'hex') FROM 1 FOR 32
  );
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_ipns_resolution_cycle_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  actual_classification text;
  actual_domain text;
  expected_receipts jsonb;
  item jsonb;
  observations jsonb;
  receipt jsonb;
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

  SELECT domain INTO actual_domain
  FROM oracle_publication_ipns_intents
  WHERE intent_id = NEW.intent_id;
  IF actual_domain IS NULL OR actual_domain != NEW.domain THEN
    RAISE EXCEPTION 'resolution cycle domain does not match its intent';
  END IF;
  IF NEW.attempt_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM oracle_publication_ipns_mutation_attempts attempt
    WHERE attempt.attempt_id = NEW.attempt_id
      AND attempt.intent_id = NEW.intent_id
  ) THEN
    RAISE EXCEPTION 'resolution cycle attempt does not match its intent';
  END IF;
  IF NEW.resolution_cycle_id != oracle_expected_ipns_resolution_cycle_id(
    NEW.intent_id,
    NEW.attempt_id,
    NEW.cycle_sequence
  ) THEN
    RAISE EXCEPTION 'resolution cycle identity is not server-derived';
  END IF;

  FOR item IN SELECT value FROM jsonb_array_elements(observations) LOOP
    IF jsonb_typeof(item) != 'object' OR (
      SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(item) key
    ) != ARRAY[
      'classification', 'endpointId', 'observedAt', 'observedCid',
      'ordinal', 'receipt', 'resolverKind'
    ]::text[] THEN
      RAISE EXCEPTION 'resolution observation contains missing or additional keys';
    END IF;
    IF jsonb_typeof(item->'classification') != 'string' OR
       item->>'classification' NOT IN ('resolved', 'unavailable', 'error') OR
       jsonb_typeof(item->'endpointId') != 'string' OR
       item->>'endpointId' !~ '^[a-z0-9][a-z0-9_.:-]{2,127}$' OR
       jsonb_typeof(item->'observedAt') != 'string' OR
       jsonb_typeof(item->'ordinal') != 'number' OR
       item->>'ordinal' !~ '^[0-9]+$' OR
       (item->>'ordinal')::integer < 0 OR
       jsonb_typeof(item->'resolverKind') != 'string' OR
       item->>'resolverKind' NOT IN (
         'filebase_control_plane', 'public_gateway'
       ) THEN
      RAISE EXCEPTION 'resolution observation has invalid field types or values';
    END IF;
    PERFORM (item->>'observedAt')::timestamptz;
    IF (item->>'classification' = 'resolved' AND (
          jsonb_typeof(item->'observedCid') != 'string' OR
          item->>'observedCid' !~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
        )) OR
       (item->>'classification' != 'resolved' AND
          jsonb_typeof(item->'observedCid') != 'null') THEN
      RAISE EXCEPTION 'resolution observation CID conflicts with classification';
    END IF;

    receipt := item->'receipt';
    IF jsonb_typeof(receipt) != 'object' OR (
      SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(receipt) key
    ) != ARRAY[
      'errorCode', 'httpStatus', 'latencyMs', 'outcome',
      'providerRequestIdHash', 'responseBodyHash', 'responseBytes',
      'schemaVersion'
    ]::text[] THEN
      RAISE EXCEPTION 'recovery receipt contains missing or additional keys';
    END IF;
    IF octet_length(receipt::text) > 1024 OR
       jsonb_typeof(receipt->'schemaVersion') != 'string' OR
       receipt->>'schemaVersion' != '1.0.0' OR
       jsonb_typeof(receipt->'outcome') != 'string' OR
       receipt->>'outcome' NOT IN (
         'resolved', 'unavailable', 'http_error', 'timeout', 'transport_error'
       ) OR
       NOT (
         jsonb_typeof(receipt->'httpStatus') = 'null' OR (
           jsonb_typeof(receipt->'httpStatus') = 'number' AND
           receipt->>'httpStatus' ~ '^[0-9]+$' AND
           (receipt->>'httpStatus')::integer BETWEEN 100 AND 599
         )
       ) OR
       jsonb_typeof(receipt->'responseBytes') != 'number' OR
       receipt->>'responseBytes' !~ '^[0-9]+$' OR
       (receipt->>'responseBytes')::numeric NOT BETWEEN 0 AND 16777216 OR
       jsonb_typeof(receipt->'latencyMs') != 'number' OR
       receipt->>'latencyMs' !~ '^[0-9]+$' OR
       (receipt->>'latencyMs')::numeric NOT BETWEEN 0 AND 300000 OR
       NOT (
         jsonb_typeof(receipt->'providerRequestIdHash') = 'null' OR (
           jsonb_typeof(receipt->'providerRequestIdHash') = 'string' AND
           receipt->>'providerRequestIdHash' ~ '^[a-f0-9]{64}$'
         )
       ) OR
       NOT (
         jsonb_typeof(receipt->'responseBodyHash') = 'null' OR (
           jsonb_typeof(receipt->'responseBodyHash') = 'string' AND
           receipt->>'responseBodyHash' ~ '^[a-f0-9]{64}$'
         )
       ) OR
       NOT (
         jsonb_typeof(receipt->'errorCode') = 'null' OR (
           jsonb_typeof(receipt->'errorCode') = 'string' AND
           receipt->>'errorCode' IN (
             'http_error', 'invalid_response', 'provider_unavailable',
             'rate_limited', 'timeout', 'transport_error'
           )
         )
       ) THEN
      RAISE EXCEPTION 'recovery receipt failed strict bounded validation';
    END IF;
    IF (receipt->>'outcome' = 'resolved' AND
          jsonb_typeof(receipt->'errorCode') != 'null') OR
       (receipt->>'outcome' = 'unavailable' AND
          receipt->>'errorCode' != 'provider_unavailable') OR
       (receipt->>'outcome' = 'http_error' AND (
          jsonb_typeof(receipt->'httpStatus') = 'null' OR
          receipt->>'errorCode' NOT IN ('http_error', 'rate_limited')
        )) OR
       (receipt->>'outcome' = 'timeout' AND
          receipt->>'errorCode' != 'timeout') OR
       (receipt->>'outcome' = 'transport_error' AND
          receipt->>'errorCode' != 'transport_error') OR
       (item->>'classification' = 'resolved' AND
          receipt->>'outcome' != 'resolved') OR
       (item->>'classification' = 'unavailable' AND
          receipt->>'outcome' != 'unavailable') OR
       (item->>'classification' = 'error' AND
          receipt->>'outcome' NOT IN (
            'http_error', 'timeout', 'transport_error'
          )) THEN
      RAISE EXCEPTION 'recovery receipt semantics conflict with observation';
    END IF;
  END LOOP;

  IF (
    SELECT count(DISTINCT (entry->>'ordinal')::integer)
    FROM jsonb_array_elements(observations) entry
  ) != NEW.observation_count THEN
    RAISE EXCEPTION 'resolution evidence reuses an ordinal';
  END IF;
  SELECT jsonb_agg(
    jsonb_build_object(
      'ordinal', (entry->>'ordinal')::integer,
      'receipt', entry->'receipt'
    ) ORDER BY (entry->>'ordinal')::integer
  ) INTO expected_receipts
  FROM jsonb_array_elements(observations) entry;
  IF NEW.receipts_canonical::jsonb != expected_receipts OR
     encode(sha256(convert_to(NEW.receipts_canonical, 'UTF8')), 'hex') !=
       NEW.receipt_identity_sha256 THEN
    RAISE EXCEPTION 'resolution receipt identity mismatch';
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

COMMENT ON TABLE oracle_publication_ipns_resolution_cycles IS
  'Immutable, intent-bound recovery cycles with strict non-secret receipts; identical evidence cannot be replayed under another cycle for one intent.';
