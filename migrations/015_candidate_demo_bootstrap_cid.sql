-- Candidate IPNS bootstrap objects may use CIDv1 while the frozen Oracle graph
-- continues to require CIDv0 for every target and published object.

DO $$
DECLARE
  invalid_rows integer;
  constraint_name text;
BEGIN
  SELECT count(*)::integer INTO invalid_rows
  FROM oracle_candidate_demo_ipns_intents
  WHERE prior_cid !~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
     OR ipns_network_key !~ '^k51[0-9a-z]{59}$';
  IF invalid_rows != 0 THEN
    RAISE EXCEPTION 'candidate demo intent rows are incompatible with CID/IPNS constraints';
  END IF;

  SELECT conname INTO constraint_name
  FROM pg_constraint
  WHERE conrelid = 'oracle_candidate_demo_ipns_intents'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%ipns_network_key%'
  LIMIT 1;
  IF constraint_name IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE oracle_candidate_demo_ipns_intents DROP CONSTRAINT %I',
      constraint_name
    );
  END IF;

  constraint_name := NULL;
  SELECT conname INTO constraint_name
  FROM pg_constraint
  WHERE conrelid = 'oracle_candidate_demo_ipns_intents'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%prior_cid%'
  LIMIT 1;
  IF constraint_name IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE oracle_candidate_demo_ipns_intents DROP CONSTRAINT %I',
      constraint_name
    );
  END IF;
END;
$$;

ALTER TABLE oracle_candidate_demo_ipns_intents
  ADD CONSTRAINT oracle_candidate_demo_intent_network_key_check
  CHECK (ipns_network_key ~ '^k51[0-9a-z]{59}$'),
  ADD CONSTRAINT oracle_candidate_demo_intent_prior_cid_check
  CHECK (prior_cid ~ '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$');
