-- Migration 013: make receipt outcome/error/status validation total and NULL-safe.
-- Migration 012 has already been applied to the local assessment database, so
-- this repair is additive and deliberately refuses to reinterpret evidence.

DO $$
DECLARE
  cycle_count bigint;
  observation_count bigint;
  receipt_count bigint;
BEGIN
  SELECT count(*),
         coalesce(sum(jsonb_array_length(observations_canonical::jsonb)), 0)
    INTO cycle_count, observation_count
  FROM oracle_publication_ipns_resolution_cycles;
  SELECT count(*) INTO receipt_count
  FROM oracle_publication_ipns_mutation_receipts;

  IF cycle_count IS DISTINCT FROM 0 OR
     observation_count IS DISTINCT FROM 0 OR
     receipt_count IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION
      'migration 013 requires empty recovery evidence tables (cycles %, observations %, receipts %)',
      cycle_count, observation_count, receipt_count;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_ipns_receipt_http_status_between(
  checked_status jsonb,
  minimum_status integer,
  maximum_status integer
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  status_text text;
BEGIN
  IF jsonb_typeof(checked_status) IS DISTINCT FROM 'number' THEN
    RETURN false;
  END IF;
  status_text := checked_status #>> '{}';
  IF status_text IS NULL OR status_text !~ '^[0-9]+$' THEN
    RETURN false;
  END IF;
  RETURN (status_text::numeric BETWEEN minimum_status AND maximum_status)
    IS NOT DISTINCT FROM true;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_ipns_receipt_semantics_are_valid(
  receipt jsonb
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  error_code text;
  outcome text;
BEGIN
  IF jsonb_typeof(receipt) IS DISTINCT FROM 'object' THEN
    RETURN false;
  END IF;
  outcome := receipt->>'outcome';
  error_code := receipt->>'errorCode';

  RETURN CASE outcome
    WHEN 'resolved' THEN
      jsonb_typeof(receipt->'errorCode') IS NOT DISTINCT FROM 'null' AND
      oracle_ipns_receipt_http_status_between(
        receipt->'httpStatus', 200, 299
      ) IS NOT DISTINCT FROM true
    WHEN 'unavailable' THEN
      jsonb_typeof(receipt->'errorCode') IS NOT DISTINCT FROM 'string' AND
      error_code IS NOT DISTINCT FROM 'provider_unavailable' AND
      jsonb_typeof(receipt->'httpStatus') IS NOT DISTINCT FROM 'null'
    WHEN 'http_error' THEN
      jsonb_typeof(receipt->'errorCode') IS NOT DISTINCT FROM 'string' AND
      (
        error_code IS NOT DISTINCT FROM 'http_error' OR
        error_code IS NOT DISTINCT FROM 'rate_limited'
      ) AND
      oracle_ipns_receipt_http_status_between(
        receipt->'httpStatus', 400, 599
      ) IS NOT DISTINCT FROM true
    WHEN 'timeout' THEN
      jsonb_typeof(receipt->'errorCode') IS NOT DISTINCT FROM 'string' AND
      error_code IS NOT DISTINCT FROM 'timeout' AND
      jsonb_typeof(receipt->'httpStatus') IS NOT DISTINCT FROM 'null'
    WHEN 'transport_error' THEN
      jsonb_typeof(receipt->'errorCode') IS NOT DISTINCT FROM 'string' AND
      error_code IS NOT DISTINCT FROM 'transport_error' AND
      jsonb_typeof(receipt->'httpStatus') IS NOT DISTINCT FROM 'null'
    ELSE false
  END;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_ipns_resolution_cycle_receipt_matrix()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  item jsonb;
  observations jsonb;
BEGIN
  observations := NEW.observations_canonical::jsonb;
  IF jsonb_typeof(observations) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'resolution observations must be an array';
  END IF;

  FOR item IN SELECT value FROM jsonb_array_elements(observations) LOOP
    IF oracle_ipns_receipt_semantics_are_valid(item->'receipt')
         IS DISTINCT FROM true THEN
      RAISE EXCEPTION
        'recovery receipt outcome, errorCode and httpStatus conflict';
    END IF;
  END LOOP;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_ipns_resolution_cycle_null_safe_receipt_guard
  ON oracle_publication_ipns_resolution_cycles;
CREATE TRIGGER oracle_ipns_resolution_cycle_null_safe_receipt_guard
  BEFORE INSERT ON oracle_publication_ipns_resolution_cycles
  FOR EACH ROW
  EXECUTE FUNCTION oracle_guard_ipns_resolution_cycle_receipt_matrix();

COMMENT ON FUNCTION oracle_ipns_receipt_semantics_are_valid(jsonb) IS
  'Total NULL-safe receipt outcome/error/status matrix matching the Oracle application validator.';
