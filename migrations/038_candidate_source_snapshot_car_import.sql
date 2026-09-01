BEGIN;

-- Additive, fail-closed CAR import evidence for the already-approved candidate
-- source-snapshot plan. This migration performs no remote request, grants no
-- IPNS authority, and preserves every existing per-object receipt.

CREATE TABLE oracle_candidate_source_snapshot_car_artifacts (
  car_artifact_id text PRIMARY KEY CHECK (
    car_artifact_id ~ '^snapshotdemocar_[a-f0-9]{32}$'
  ),
  artifact_version text NOT NULL CHECK (
    artifact_version = 'candidate-source-snapshot-car-v1'
  ),
  plan_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  car_role text NOT NULL CHECK (car_role IN ('open_data', 'query_table')),
  primary_root_cid text NOT NULL CHECK (
    primary_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  root_count integer NOT NULL CHECK (root_count BETWEEN 1 AND 350000),
  root_set_sha256 text NOT NULL CHECK (root_set_sha256 ~ '^[a-f0-9]{64}$'),
  car_sha256 text NOT NULL CHECK (car_sha256 ~ '^[a-f0-9]{64}$'),
  car_bytes bigint NOT NULL CHECK (car_bytes BETWEEN 1 AND 8589934592),
  block_count integer NOT NULL CHECK (block_count > 0),
  block_set_sha256 text NOT NULL CHECK (block_set_sha256 ~ '^[a-f0-9]{64}$'),
  member_count integer NOT NULL CHECK (member_count > 0),
  member_logical_bytes bigint NOT NULL CHECK (member_logical_bytes > 0),
  member_set_sha256 text NOT NULL CHECK (member_set_sha256 ~ '^[a-f0-9]{64}$'),
  local_validation_sha256 text NOT NULL CHECK (
    local_validation_sha256 ~ '^[a-f0-9]{64}$'
  ),
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  recorded_at timestamptz NOT NULL,
  UNIQUE (car_artifact_id, plan_id),
  UNIQUE (plan_id, car_role),
  UNIQUE (plan_id, car_sha256)
);

CREATE TABLE oracle_candidate_source_snapshot_car_roots (
  car_artifact_id text NOT NULL,
  plan_id text NOT NULL,
  root_ordinal integer NOT NULL CHECK (root_ordinal > 0),
  root_role text NOT NULL CHECK (
    root_role IN ('approved_target', 'additional_planned_object')
  ),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  root_cid text NOT NULL CHECK (
    root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  PRIMARY KEY (car_artifact_id, root_ordinal),
  UNIQUE (car_artifact_id, root_cid),
  UNIQUE (car_artifact_id, plan_id, domain, remote_object_key),
  FOREIGN KEY (car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_artifacts(
      car_artifact_id, plan_id
    ),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE UNIQUE INDEX oracle_css_car_one_primary_root_idx
  ON oracle_candidate_source_snapshot_car_roots(car_artifact_id)
  WHERE root_role = 'approved_target';

CREATE TABLE oracle_candidate_source_snapshot_car_members (
  car_artifact_id text NOT NULL,
  plan_id text NOT NULL,
  member_ordinal integer NOT NULL CHECK (member_ordinal > 0),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_bytes bigint NOT NULL CHECK (expected_bytes BETWEEN 0 AND 536870912),
  PRIMARY KEY (car_artifact_id, member_ordinal),
  UNIQUE (car_artifact_id, plan_id, domain, remote_object_key),
  UNIQUE (plan_id, domain, remote_object_key),
  FOREIGN KEY (car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_artifacts(
      car_artifact_id, plan_id
    ),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE OR REPLACE FUNCTION oracle_css_car_root_set_sha256(
  checked_car_artifact_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(oracle_canonical_jsonb(COALESCE(
    jsonb_agg(root.root_cid ORDER BY root.root_ordinal), '[]'::jsonb
  )), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_car_roots root
  WHERE root.car_artifact_id = checked_car_artifact_id;
$$;

CREATE OR REPLACE FUNCTION oracle_css_car_member_set_sha256(
  checked_car_artifact_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    oracle_canonical_jsonb(jsonb_build_array(
      member.domain, member.remote_object_key, member.expected_cid,
      member.expected_sha256, member.expected_bytes
    )) || chr(10),
    '' ORDER BY member.member_ordinal
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_car_members member
  WHERE member.car_artifact_id = checked_car_artifact_id;
$$;

CREATE OR REPLACE FUNCTION oracle_css_car_artifact_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    artifact.car_role || chr(31) || artifact.car_artifact_id || chr(31) ||
    artifact.car_sha256 || chr(31) || artifact.car_bytes::text || chr(31) ||
    artifact.primary_root_cid || chr(31) || artifact.root_set_sha256 ||
    chr(31) || artifact.member_set_sha256 || chr(31) ||
    artifact.member_count::text || chr(31) ||
    artifact.member_logical_bytes::text,
    chr(30) ORDER BY artifact.car_role
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_car_artifacts artifact
  WHERE artifact.plan_id = checked_plan_id;
$$;

CREATE TABLE oracle_candidate_source_snapshot_car_import_authorizations (
  car_authorization_id text PRIMARY KEY CHECK (
    car_authorization_id ~ '^snapshotdemocarauthorization_[a-f0-9]{32}$'
  ),
  authorization_version text NOT NULL CHECK (
    authorization_version = 'candidate-source-snapshot-car-authorization-v1'
  ),
  plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_revision integer NOT NULL CHECK (plan_revision > 0),
  approval_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_approvals(approval_id),
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  artifact_set_sha256 text NOT NULL CHECK (
    artifact_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  artifact_count integer NOT NULL CHECK (artifact_count = 2),
  total_car_bytes bigint NOT NULL CHECK (total_car_bytes > 0),
  endpoint text NOT NULL CHECK (endpoint IN (
    'https://rpc.filebase.io/api/v0/dag/import',
    'https://s3.filebase.com'
  )),
  import_method text NOT NULL CHECK (import_method IN (
    'rpc_dag_import', 's3_put_import_car'
  )),
  maximum_attempts_per_artifact integer NOT NULL CHECK (
    maximum_attempts_per_artifact BETWEEN 1 AND 2
  ),
  maximum_total_import_attempts integer NOT NULL CHECK (
    maximum_total_import_attempts BETWEEN 1 AND 4
  ),
  hard_spending_ceiling_usd numeric(18, 12) NOT NULL CHECK (
    hard_spending_ceiling_usd = 25.000000000000
  ),
  upload_closure_authorized boolean NOT NULL CHECK (
    upload_closure_authorized = true
  ),
  ipns_authorized boolean NOT NULL CHECK (ipns_authorized = false),
  authorization_statement text NOT NULL CHECK (
    length(authorization_statement) BETWEEN 1 AND 32768
  ),
  authorization_statement_sha256 text NOT NULL UNIQUE CHECK (
    authorization_statement_sha256 ~ '^[a-f0-9]{64}$'
  ),
  human_reference text NOT NULL CHECK (
    human_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  CHECK (
    (endpoint = 'https://rpc.filebase.io/api/v0/dag/import' AND
      import_method = 'rpc_dag_import') OR
    (endpoint = 'https://s3.filebase.com' AND
      import_method = 's3_put_import_car')
  ),
  UNIQUE (car_authorization_id, plan_id)
);

CREATE TABLE oracle_candidate_source_snapshot_car_import_receipts (
  car_import_receipt_id text PRIMARY KEY CHECK (
    car_import_receipt_id ~ '^snapshotdemocarreceipt_[a-f0-9]{32}$'
  ),
  receipt_version text NOT NULL CHECK (
    receipt_version = 'candidate-source-snapshot-car-import-receipt-v1'
  ),
  car_authorization_id text NOT NULL,
  car_artifact_id text NOT NULL UNIQUE,
  plan_id text NOT NULL,
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  verification_method text NOT NULL CHECK (
    verification_method = 'car_import_recursively_pinned'
  ),
  car_sha256 text NOT NULL CHECK (car_sha256 ~ '^[a-f0-9]{64}$'),
  car_bytes bigint NOT NULL CHECK (car_bytes > 0),
  primary_root_cid text NOT NULL CHECK (
    primary_root_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  root_count integer NOT NULL CHECK (root_count > 0),
  root_set_sha256 text NOT NULL CHECK (root_set_sha256 ~ '^[a-f0-9]{64}$'),
  root_observation_set_sha256 text NOT NULL CHECK (
    root_observation_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  member_count integer NOT NULL CHECK (member_count > 0),
  member_logical_bytes bigint NOT NULL CHECK (member_logical_bytes > 0),
  member_set_sha256 text NOT NULL CHECK (member_set_sha256 ~ '^[a-f0-9]{64}$'),
  provider_request_id_hash text CHECK (
    provider_request_id_hash IS NULL OR
    provider_request_id_hash ~ '^[a-f0-9]{64}$'
  ),
  provider_pin_id_hash text CHECK (
    provider_pin_id_hash IS NULL OR provider_pin_id_hash ~ '^[a-f0-9]{64}$'
  ),
  import_attempt_count integer NOT NULL CHECK (import_attempt_count BETWEEN 1 AND 2),
  request_cost_usd numeric(18, 12) NOT NULL CHECK (
    request_cost_usd >= 0 AND request_cost_usd <= 25
  ),
  provider_import_result text NOT NULL CHECK (
    provider_import_result = 'expected_root_set_returned'
  ),
  final_recursive_pin_status text NOT NULL CHECK (
    final_recursive_pin_status = 'pinned'
  ),
  official_gateway_status text NOT NULL CHECK (
    official_gateway_status = 'verified'
  ),
  root_block_validation text NOT NULL CHECK (
    root_block_validation = 'cid_verified'
  ),
  verification_timestamp timestamptz NOT NULL,
  implementation_commit_sha text NOT NULL CHECK (
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  receipt_payload jsonb NOT NULL,
  receipt_sha256 text NOT NULL UNIQUE CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
  UNIQUE (car_import_receipt_id, car_artifact_id, plan_id),
  FOREIGN KEY (car_authorization_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_import_authorizations(
      car_authorization_id, plan_id
    ),
  FOREIGN KEY (car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_artifacts(
      car_artifact_id, plan_id
    )
);

CREATE TABLE oracle_candidate_source_snapshot_car_bulk_verifications (
  bulk_verification_id text PRIMARY KEY CHECK (
    bulk_verification_id ~ '^snapshotdemocarbulk_[a-f0-9]{32}$'
  ),
  bulk_version text NOT NULL CHECK (
    bulk_version = 'candidate-source-snapshot-car-bulk-verification-v1'
  ),
  car_import_receipt_id text NOT NULL UNIQUE,
  car_artifact_id text NOT NULL UNIQUE,
  plan_id text NOT NULL,
  verification_method text NOT NULL CHECK (
    verification_method = 'car_import_recursively_pinned'
  ),
  member_set_sha256 text NOT NULL CHECK (member_set_sha256 ~ '^[a-f0-9]{64}$'),
  member_count integer NOT NULL CHECK (member_count > 0),
  member_logical_bytes bigint NOT NULL CHECK (member_logical_bytes > 0),
  preserved_verified_count integer NOT NULL CHECK (preserved_verified_count >= 0),
  preserved_verified_bytes bigint NOT NULL CHECK (preserved_verified_bytes >= 0),
  newly_verified_count integer NOT NULL CHECK (newly_verified_count >= 0),
  newly_verified_bytes bigint NOT NULL CHECK (newly_verified_bytes >= 0),
  verified_at timestamptz NOT NULL,
  result_payload jsonb NOT NULL,
  result_sha256 text NOT NULL UNIQUE CHECK (result_sha256 ~ '^[a-f0-9]{64}$'),
  CHECK (preserved_verified_count + newly_verified_count = member_count),
  CHECK (preserved_verified_bytes + newly_verified_bytes = member_logical_bytes),
  FOREIGN KEY (car_import_receipt_id, car_artifact_id, plan_id)
    REFERENCES oracle_candidate_source_snapshot_car_import_receipts(
      car_import_receipt_id, car_artifact_id, plan_id
    )
);

ALTER TABLE oracle_candidate_source_snapshot_demo_objects
  ADD COLUMN car_verification_method text CHECK (
    car_verification_method IS NULL OR
    car_verification_method = 'car_import_recursively_pinned'
  ),
  ADD COLUMN car_artifact_id text,
  ADD COLUMN car_import_receipt_id text,
  ADD CONSTRAINT oracle_css_object_car_verification_shape_check CHECK (
    (car_verification_method IS NULL AND car_artifact_id IS NULL AND
      car_import_receipt_id IS NULL) OR
    (car_verification_method = 'car_import_recursively_pinned' AND
      car_artifact_id IS NOT NULL AND car_import_receipt_id IS NOT NULL)
  ),
  ADD CONSTRAINT oracle_css_object_car_member_fk FOREIGN KEY (
    car_artifact_id, plan_id, domain, remote_object_key
  ) REFERENCES oracle_candidate_source_snapshot_car_members(
    car_artifact_id, plan_id, domain, remote_object_key
  ),
  ADD CONSTRAINT oracle_css_object_car_receipt_fk FOREIGN KEY (
    car_import_receipt_id, car_artifact_id, plan_id
  ) REFERENCES oracle_candidate_source_snapshot_car_import_receipts(
    car_import_receipt_id, car_artifact_id, plan_id
  );

CREATE OR REPLACE FUNCTION oracle_guard_css_car_artifact_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  expected_primary_root text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR SHARE;
  expected_primary_root := CASE NEW.car_role
    WHEN 'open_data' THEN
      plan_row.plan_payload->'targets'->'openData'->>'targetCid'
    ELSE
      plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
  END;
  expected_id := 'snapshotdemocar_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.artifact_version, NEW.plan_id, NEW.plan_sha256, NEW.car_role,
      NEW.car_sha256, NEW.primary_root_cid, NEW.root_set_sha256,
      NEW.member_set_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF plan_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     NEW.primary_root_cid IS DISTINCT FROM expected_primary_root OR
     NEW.car_artifact_id IS DISTINCT FROM expected_id OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR artifact is not locally admissible';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_artifact_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_artifacts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_artifact_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_root_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  object_cid text;
BEGIN
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT expected_cid INTO STRICT object_cid
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id AND domain = NEW.domain
    AND remote_object_key = NEW.remote_object_key;
  IF artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     object_cid IS DISTINCT FROM NEW.root_cid OR
     (NEW.root_role = 'approved_target' AND (
       NEW.root_ordinal <> 1 OR
       NEW.root_cid IS DISTINCT FROM artifact_row.primary_root_cid
     )) OR
     (NEW.root_role = 'additional_planned_object' AND NEW.root_ordinal = 1) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations auth_row
       WHERE auth_row.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR root is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_root_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_roots
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_root_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_member_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  artifact_plan_id text;
  object_row oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
BEGIN
  SELECT plan_id INTO STRICT artifact_plan_id
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT * INTO STRICT object_row
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id AND domain = NEW.domain
    AND remote_object_key = NEW.remote_object_key;
  IF artifact_plan_id IS DISTINCT FROM NEW.plan_id OR
     object_row.expected_sha256 IS DISTINCT FROM NEW.expected_sha256 OR
     object_row.expected_cid IS DISTINCT FROM NEW.expected_cid OR
     object_row.expected_bytes IS DISTINCT FROM NEW.expected_bytes OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_import_authorizations auth_row
       WHERE auth_row.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR member is not an exact plan object';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_member_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_members
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_member_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_authorization_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  artifact_count integer;
  total_car_bytes bigint;
  total_member_count bigint;
  total_member_bytes bigint;
  expected_artifact_set_sha256 text;
  expected_statement_sha256 text;
  expected_authorization_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*)::integer, coalesce(sum(car_bytes), 0)::bigint,
         coalesce(sum(member_count), 0)::bigint,
         coalesce(sum(member_logical_bytes), 0)::bigint
  INTO artifact_count, total_car_bytes, total_member_count, total_member_bytes
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE plan_id = NEW.plan_id;
  expected_artifact_set_sha256 := oracle_css_car_artifact_set_sha256(NEW.plan_id);
  expected_statement_sha256 := encode(sha256(convert_to(
    NEW.authorization_statement, 'UTF8'
  )), 'hex');
  expected_authorization_id := 'snapshotdemocarauthorization_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.authorization_version, NEW.plan_id, NEW.plan_sha256,
      expected_artifact_set_sha256, expected_statement_sha256
    ])), 'UTF8')), 'hex'), 1, 32);
  IF plan_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     plan_row.revision IS DISTINCT FROM NEW.plan_revision OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     artifact_count IS DISTINCT FROM 2 OR
     artifact_count IS DISTINCT FROM NEW.artifact_count OR
     total_car_bytes IS DISTINCT FROM NEW.total_car_bytes OR
     total_member_count IS DISTINCT FROM
       plan_row.exact_upload_object_count::bigint OR
     total_member_bytes IS DISTINCT FROM plan_row.exact_upload_bytes OR
     expected_artifact_set_sha256 IS DISTINCT FROM NEW.artifact_set_sha256 OR
     expected_statement_sha256 IS DISTINCT FROM
       NEW.authorization_statement_sha256 OR
     expected_authorization_id IS DISTINCT FROM NEW.car_authorization_id OR
     NEW.maximum_total_import_attempts IS DISTINCT FROM
       NEW.maximum_attempts_per_artifact * NEW.artifact_count OR
     NEW.hard_spending_ceiling_usd IS DISTINCT FROM plan_row.budget_limit_usd OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.approval_id = NEW.approval_id
         AND approval.plan_id = NEW.plan_id
         AND approval.plan_sha256 = NEW.plan_sha256
     ) OR NOT EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_artifacts artifact
       WHERE artifact.plan_id = NEW.plan_id AND artifact.car_role = 'open_data'
     ) OR NOT EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_car_artifacts artifact
       WHERE artifact.plan_id = NEW.plan_id AND artifact.car_role = 'query_table'
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_objects object
       WHERE object.plan_id = NEW.plan_id AND NOT EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_car_members member
         WHERE member.plan_id = object.plan_id
           AND member.domain = object.domain
           AND member.remote_object_key = object.remote_object_key
           AND member.expected_sha256 = object.expected_sha256
           AND member.expected_cid = object.expected_cid
           AND member.expected_bytes = object.expected_bytes
       )
     ) OR EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_artifacts artifact
       WHERE artifact.plan_id = NEW.plan_id AND (
         artifact.implementation_commit_sha IS DISTINCT FROM
           NEW.implementation_commit_sha OR
         artifact.root_count IS DISTINCT FROM (
           SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_car_roots root
           WHERE root.car_artifact_id = artifact.car_artifact_id
         ) OR artifact.root_set_sha256 IS DISTINCT FROM
           oracle_css_car_root_set_sha256(artifact.car_artifact_id) OR
         artifact.member_count IS DISTINCT FROM (
           SELECT count(*)::integer
           FROM oracle_candidate_source_snapshot_car_members member
           WHERE member.car_artifact_id = artifact.car_artifact_id
         ) OR artifact.member_logical_bytes IS DISTINCT FROM (
           SELECT coalesce(sum(member.expected_bytes), 0)::bigint
           FROM oracle_candidate_source_snapshot_car_members member
           WHERE member.car_artifact_id = artifact.car_artifact_id
         ) OR artifact.member_set_sha256 IS DISTINCT FROM
           oracle_css_car_member_set_sha256(artifact.car_artifact_id)
       )
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR authorization is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_authorization_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_authorization_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_receipt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  authorization_row
    oracle_candidate_source_snapshot_car_import_authorizations%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  existing_import_attempt_count integer;
  existing_request_cost numeric(18, 12);
  expected_payload jsonb;
  expected_receipt_sha256 text;
  expected_receipt_id text;
BEGIN
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_car_import_authorizations
  WHERE car_authorization_id = NEW.car_authorization_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT coalesce(sum(import_attempt_count), 0)::integer,
         coalesce(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO existing_import_attempt_count, existing_request_cost
  FROM oracle_candidate_source_snapshot_car_import_receipts
  WHERE plan_id = NEW.plan_id;
  expected_payload := jsonb_build_object(
    'authorizationId', NEW.car_authorization_id,
    'carArtifactId', NEW.car_artifact_id,
    'carBytes', artifact_row.car_bytes,
    'carSha256', artifact_row.car_sha256,
    'finalRecursivePinStatus', NEW.final_recursive_pin_status,
    'implementationCommitSha', NEW.implementation_commit_sha,
    'memberCount', artifact_row.member_count,
    'memberLogicalBytes', artifact_row.member_logical_bytes,
    'memberSetSha256', artifact_row.member_set_sha256,
    'officialGatewayStatus', NEW.official_gateway_status,
    'planId', artifact_row.plan_id,
    'planSha256', artifact_row.plan_sha256,
    'primaryRootCid', artifact_row.primary_root_cid,
    'providerImportResult', NEW.provider_import_result,
    'importAttemptCount', NEW.import_attempt_count,
    'providerPinIdHash', NEW.provider_pin_id_hash,
    'providerRequestIdHash', NEW.provider_request_id_hash,
    'requestCostUsd', NEW.request_cost_usd::double precision,
    'rootBlockValidation', NEW.root_block_validation,
    'rootCount', artifact_row.root_count,
    'rootObservationSetSha256', NEW.root_observation_set_sha256,
    'rootSetSha256', artifact_row.root_set_sha256,
    'schemaVersion', NEW.receipt_version,
    'verificationMethod', NEW.verification_method,
    'verificationTimestamp', to_char(
      NEW.verification_timestamp AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
  expected_receipt_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_receipt_id := 'snapshotdemocarreceipt_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.receipt_version, artifact_row.plan_id, NEW.car_artifact_id,
      expected_receipt_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  IF authorization_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     authorization_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     authorization_row.implementation_commit_sha IS DISTINCT FROM
       NEW.implementation_commit_sha OR
     NEW.import_attempt_count > authorization_row.maximum_attempts_per_artifact OR
     existing_import_attempt_count + NEW.import_attempt_count >
       authorization_row.maximum_total_import_attempts OR
     accounting_row.request_count + existing_import_attempt_count +
       NEW.import_attempt_count > plan_row.maximum_request_count OR
     accounting_row.request_cost_usd + existing_request_cost +
       NEW.request_cost_usd > authorization_row.hard_spending_ceiling_usd OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     artifact_row.car_sha256 IS DISTINCT FROM NEW.car_sha256 OR
     artifact_row.car_bytes IS DISTINCT FROM NEW.car_bytes OR
     artifact_row.primary_root_cid IS DISTINCT FROM NEW.primary_root_cid OR
     artifact_row.root_count IS DISTINCT FROM NEW.root_count OR
     artifact_row.root_set_sha256 IS DISTINCT FROM NEW.root_set_sha256 OR
     NEW.root_observation_set_sha256 IS DISTINCT FROM
       artifact_row.root_set_sha256 OR
     artifact_row.root_count IS DISTINCT FROM (
       SELECT count(*)::integer
       FROM oracle_candidate_source_snapshot_car_roots root
       WHERE root.car_artifact_id = artifact_row.car_artifact_id
     ) OR artifact_row.root_set_sha256 IS DISTINCT FROM
       oracle_css_car_root_set_sha256(artifact_row.car_artifact_id) OR
     artifact_row.member_count IS DISTINCT FROM NEW.member_count OR
     artifact_row.member_logical_bytes IS DISTINCT FROM
       NEW.member_logical_bytes OR
     artifact_row.member_set_sha256 IS DISTINCT FROM NEW.member_set_sha256 OR
     artifact_row.member_count IS DISTINCT FROM (
       SELECT count(*)::integer
       FROM oracle_candidate_source_snapshot_car_members member
       WHERE member.car_artifact_id = artifact_row.car_artifact_id
     ) OR artifact_row.member_logical_bytes IS DISTINCT FROM (
       SELECT coalesce(sum(member.expected_bytes), 0)::bigint
       FROM oracle_candidate_source_snapshot_car_members member
       WHERE member.car_artifact_id = artifact_row.car_artifact_id
     ) OR artifact_row.member_set_sha256 IS DISTINCT FROM
       oracle_css_car_member_set_sha256(artifact_row.car_artifact_id) OR
     NEW.receipt_payload IS DISTINCT FROM expected_payload OR
     NEW.receipt_sha256 IS DISTINCT FROM expected_receipt_sha256 OR
     NEW.car_import_receipt_id IS DISTINCT FROM expected_receipt_id THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR import receipt is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_receipt_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_import_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_receipt_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_bulk_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  receipt_row oracle_candidate_source_snapshot_car_import_receipts%ROWTYPE;
  artifact_row oracle_candidate_source_snapshot_car_artifacts%ROWTYPE;
  actual_member_count integer;
  actual_member_bytes bigint;
  preserved_count integer;
  preserved_bytes bigint;
  new_count integer;
  new_bytes bigint;
  invalid_count integer;
  expected_payload jsonb;
  expected_result_sha256 text;
  expected_id text;
BEGIN
  SELECT * INTO STRICT receipt_row
  FROM oracle_candidate_source_snapshot_car_import_receipts
  WHERE car_import_receipt_id = NEW.car_import_receipt_id;
  SELECT * INTO STRICT artifact_row
  FROM oracle_candidate_source_snapshot_car_artifacts
  WHERE car_artifact_id = NEW.car_artifact_id;
  SELECT count(*)::integer, coalesce(sum(member.expected_bytes), 0)::bigint,
         count(*) FILTER (WHERE object.status = 'verified')::integer,
         coalesce(sum(member.expected_bytes)
           FILTER (WHERE object.status = 'verified'), 0)::bigint,
         count(*) FILTER (
           WHERE object.status IN ('pending', 'outcome_unknown')
         )::integer,
         coalesce(sum(member.expected_bytes) FILTER (
           WHERE object.status IN ('pending', 'outcome_unknown')
         ), 0)::bigint,
         count(*) FILTER (WHERE object.status NOT IN (
           'pending', 'outcome_unknown', 'verified'
         ))::integer
  INTO actual_member_count, actual_member_bytes,
       preserved_count, preserved_bytes, new_count, new_bytes, invalid_count
  FROM oracle_candidate_source_snapshot_car_members member
  JOIN oracle_candidate_source_snapshot_demo_objects object
    ON object.plan_id = member.plan_id AND object.domain = member.domain
   AND object.remote_object_key = member.remote_object_key
  WHERE member.car_artifact_id = NEW.car_artifact_id;
  expected_payload := jsonb_build_object(
    'carArtifactId', NEW.car_artifact_id,
    'carImportReceiptId', NEW.car_import_receipt_id,
    'memberCount', artifact_row.member_count,
    'memberLogicalBytes', artifact_row.member_logical_bytes,
    'memberSetSha256', artifact_row.member_set_sha256,
    'newlyVerifiedBytes', new_bytes,
    'newlyVerifiedCount', new_count,
    'planId', artifact_row.plan_id,
    'preservedVerifiedBytes', preserved_bytes,
    'preservedVerifiedCount', preserved_count,
    'schemaVersion', NEW.bulk_version,
    'verificationMethod', NEW.verification_method,
    'verifiedAt', to_char(
      NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
  expected_result_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_id := 'snapshotdemocarbulk_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      NEW.bulk_version, artifact_row.plan_id, NEW.car_artifact_id,
      NEW.car_import_receipt_id, expected_result_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  IF receipt_row.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
     receipt_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     artifact_row.member_set_sha256 IS DISTINCT FROM NEW.member_set_sha256 OR
     artifact_row.member_count IS DISTINCT FROM NEW.member_count OR
     artifact_row.member_logical_bytes IS DISTINCT FROM
       NEW.member_logical_bytes OR
     actual_member_count IS DISTINCT FROM NEW.member_count OR
     actual_member_bytes IS DISTINCT FROM NEW.member_logical_bytes OR
     preserved_count IS DISTINCT FROM NEW.preserved_verified_count OR
     preserved_bytes IS DISTINCT FROM NEW.preserved_verified_bytes OR
     new_count IS DISTINCT FROM NEW.newly_verified_count OR
     new_bytes IS DISTINCT FROM NEW.newly_verified_bytes OR
     invalid_count <> 0 OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_car_members member
       JOIN oracle_candidate_source_snapshot_demo_requests request
         ON request.plan_id = member.plan_id
        AND request.domain = member.domain
        AND request.remote_object_key = member.remote_object_key
       WHERE member.car_artifact_id = NEW.car_artifact_id
         AND request.outcome = 'request_started'
     ) OR
     NEW.result_payload IS DISTINCT FROM expected_payload OR
     NEW.result_sha256 IS DISTINCT FROM expected_result_sha256 OR
     NEW.bulk_verification_id IS DISTINCT FROM expected_id THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR bulk verification is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_car_bulk_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_car_bulk_verifications
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_bulk_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_car_bulk_complete()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  car_count integer;
  car_bytes bigint;
  preserved_count integer;
  preserved_bytes bigint;
BEGIN
  SELECT count(*) FILTER (
           WHERE object.car_import_receipt_id = NEW.car_import_receipt_id AND
             object.car_verification_method = 'car_import_recursively_pinned'
         )::integer,
         coalesce(sum(object.expected_bytes) FILTER (
           WHERE object.car_import_receipt_id = NEW.car_import_receipt_id AND
             object.car_verification_method = 'car_import_recursively_pinned'
         ), 0)::bigint,
         count(*) FILTER (
           WHERE object.car_import_receipt_id IS NULL AND
             object.car_verification_method IS NULL
         )::integer,
         coalesce(sum(object.expected_bytes) FILTER (
           WHERE object.car_import_receipt_id IS NULL AND
             object.car_verification_method IS NULL
         ), 0)::bigint
  INTO car_count, car_bytes, preserved_count, preserved_bytes
  FROM oracle_candidate_source_snapshot_car_members member
  JOIN oracle_candidate_source_snapshot_demo_objects object
    ON object.plan_id = member.plan_id AND object.domain = member.domain
   AND object.remote_object_key = member.remote_object_key
  WHERE member.car_artifact_id = NEW.car_artifact_id
    AND object.status = 'verified'
    AND object.provider_cid = object.expected_cid
    AND object.successful_effect_count = 1;
  IF car_count IS DISTINCT FROM NEW.newly_verified_count OR
     car_bytes IS DISTINCT FROM NEW.newly_verified_bytes OR
     preserved_count IS DISTINCT FROM NEW.preserved_verified_count OR
     car_count + preserved_count IS DISTINCT FROM NEW.member_count THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR bulk verification did not close exactly';
  END IF;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER oracle_css_car_bulk_complete_guard
  AFTER INSERT ON oracle_candidate_source_snapshot_car_bulk_verifications
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_car_bulk_complete();

CREATE TRIGGER oracle_css_car_artifact_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_car_artifacts
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_root_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_car_roots
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_member_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_car_members
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_authorization_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_car_import_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_receipt_immutable
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_car_import_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();
CREATE TRIGGER oracle_css_car_bulk_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_car_bulk_verifications
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

-- Preserve every historical object transition and permit the one new path
-- only when an exact immutable CAR receipt and exact member batch already
-- exist in the same transaction.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_object()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  is_car_transition boolean;
BEGIN
  is_car_transition :=
    OLD.status IN ('pending', 'outcome_unknown') AND
    NEW.status = 'verified' AND
    OLD.car_verification_method IS NULL AND
    NEW.car_verification_method = 'car_import_recursively_pinned' AND
    NEW.car_artifact_id IS NOT NULL AND NEW.car_import_receipt_id IS NOT NULL AND
    NEW.provider_cid = NEW.expected_cid AND NEW.receipt_sha256 IS NOT NULL AND
    NEW.successful_effect_count = 1 AND
    EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_car_members member
      JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
        ON receipt.car_artifact_id = member.car_artifact_id
       AND receipt.plan_id = member.plan_id
      JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
        ON bulk.car_import_receipt_id = receipt.car_import_receipt_id
       AND bulk.car_artifact_id = member.car_artifact_id
       AND bulk.plan_id = member.plan_id
      WHERE member.car_artifact_id = NEW.car_artifact_id
        AND member.plan_id = NEW.plan_id AND member.domain = NEW.domain
        AND member.remote_object_key = NEW.remote_object_key
        AND member.expected_sha256 = NEW.expected_sha256
        AND member.expected_cid = NEW.expected_cid
        AND member.expected_bytes = NEW.expected_bytes
        AND receipt.car_import_receipt_id = NEW.car_import_receipt_id
        AND receipt.receipt_sha256 = NEW.receipt_sha256
        AND receipt.verification_method = 'car_import_recursively_pinned'
        AND bulk.verification_method = 'car_import_recursively_pinned'
    );
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.logical_object_key IS DISTINCT FROM NEW.logical_object_key OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.expected_sha256 IS DISTINCT FROM NEW.expected_sha256 OR
     OLD.expected_cid IS DISTINCT FROM NEW.expected_cid OR
     OLD.expected_bytes IS DISTINCT FROM NEW.expected_bytes THEN
    RAISE EXCEPTION 'candidate source-snapshot object identity is immutable';
  END IF;
  IF OLD.status = 'verified' AND OLD IS DISTINCT FROM NEW THEN
    RAISE EXCEPTION 'verified candidate source-snapshot effect is immutable';
  END IF;
  IF OLD.revision + 1 IS DISTINCT FROM NEW.revision THEN
    RAISE EXCEPTION 'candidate source-snapshot object revision is invalid';
  END IF;
  IF (OLD.car_verification_method IS DISTINCT FROM NEW.car_verification_method OR
      OLD.car_artifact_id IS DISTINCT FROM NEW.car_artifact_id OR
      OLD.car_import_receipt_id IS DISTINCT FROM NEW.car_import_receipt_id) AND
     NOT is_car_transition THEN
    RAISE EXCEPTION 'candidate source-snapshot CAR verification binding is invalid';
  END IF;
  IF NOT (
    is_car_transition OR OLD.status = NEW.status OR
    (OLD.status = 'pending' AND NEW.status = 'admitted') OR
    (OLD.status = 'admitted' AND NEW.status IN (
      'outcome_unknown', 'verified', 'failed_terminal'
    )) OR
    (OLD.status = 'outcome_unknown' AND NEW.status IN (
      'admitted', 'verified', 'failed_terminal'
    )) OR
    (OLD.status IN ('admitted', 'outcome_unknown') AND
      NEW.status = 'pending' AND (
        EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations reconciliation
          WHERE reconciliation.plan_id = OLD.plan_id
            AND reconciliation.domain = OLD.domain
            AND reconciliation.remote_object_key = OLD.remote_object_key
            AND reconciliation.result = 'conclusively_absent'
        ) OR EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_admitted_recovery_events recovery
          WHERE recovery.plan_id = OLD.plan_id
            AND recovery.domain = OLD.domain
            AND recovery.remote_object_key = OLD.remote_object_key
            AND recovery.disposition = 'returned_pending_no_put'
        ) OR EXISTS (
          SELECT 1
          FROM oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          WHERE resolution.plan_id = OLD.plan_id
            AND resolution.domain = OLD.domain
            AND resolution.remote_object_key = OLD.remote_object_key
            AND resolution.result = 'conclusively_absent'
        )
      ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot object transition';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- Closure accepts either the unchanged individual PUT/inspection journal or
-- the exact immutable CAR member + final receipt + committed bulk batch.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_upload_closure()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  actual_count bigint;
  actual_bytes bigint;
  unresolved_count bigint;
  mismatch_count bigint;
  unjournaled_count bigint;
  car_request_count integer;
  car_request_cost numeric(18, 12);
  total_request_count integer;
  total_request_cost numeric(18, 12);
  expected_payload jsonb;
  expected_closure_sha256 text;
  expected_closure_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT count(*), coalesce(sum(expected_bytes), 0),
         count(*) FILTER (WHERE status <> 'verified'),
         count(*) FILTER (
           WHERE status = 'verified' AND provider_cid IS DISTINCT FROM expected_cid
         )
  INTO actual_count, actual_bytes, unresolved_count, mismatch_count
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id;
  SELECT count(*) INTO unjournaled_count
  FROM oracle_candidate_source_snapshot_demo_objects object
  WHERE object.plan_id = NEW.plan_id
    AND object.status = 'verified'
    AND NOT (
      EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
        JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.request_id = attempt.request_id
        WHERE attempt.plan_id = object.plan_id
          AND attempt.domain = object.domain
          AND attempt.remote_object_key = object.remote_object_key
          AND attempt.outcome = 'verified'
          AND attempt.provider_cid = object.expected_cid
          AND attempt.receipt_sha256 = object.receipt_sha256
          AND request.plan_id = object.plan_id
          AND request.operation_kind = 'put_object'
          AND request.outcome = 'succeeded'
          AND request.receipt_sha256 = object.receipt_sha256
      ) OR EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_inspections inspection
        JOIN oracle_candidate_source_snapshot_demo_inspection_attempts inspection_attempt
          ON inspection_attempt.inspection_id = inspection.inspection_id
        JOIN oracle_candidate_source_snapshot_demo_requests request
          ON request.request_id = inspection.request_id
        JOIN oracle_candidate_source_snapshot_demo_upload_attempts recovery_attempt
          ON recovery_attempt.attempt_id =
             inspection_attempt.recovery_upload_attempt_id
        WHERE inspection.plan_id = object.plan_id
          AND inspection.domain = object.domain
          AND inspection.remote_object_key = object.remote_object_key
          AND inspection.outcome = 'verified'
          AND inspection.observed_cid = object.expected_cid
          AND inspection.observed_sha256 = object.expected_sha256
          AND inspection.observed_bytes = object.expected_bytes
          AND inspection.receipt_sha256 = object.receipt_sha256
          AND inspection_attempt.outcome = 'verified'
          AND request.operation_kind = 'inspect_object'
          AND request.outcome = 'succeeded'
          AND request.receipt_sha256 = object.receipt_sha256
          AND recovery_attempt.outcome IN (
            'connection_failure', 'retryable_http_error', 'timeout_unknown'
          )
      ) OR EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_car_members member
        JOIN oracle_candidate_source_snapshot_car_import_receipts receipt
          ON receipt.car_artifact_id = member.car_artifact_id
         AND receipt.plan_id = member.plan_id
        JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
          ON bulk.car_import_receipt_id = receipt.car_import_receipt_id
         AND bulk.car_artifact_id = member.car_artifact_id
         AND bulk.plan_id = member.plan_id
        WHERE member.plan_id = object.plan_id
          AND member.domain = object.domain
          AND member.remote_object_key = object.remote_object_key
          AND member.expected_sha256 = object.expected_sha256
          AND member.expected_cid = object.expected_cid
          AND member.expected_bytes = object.expected_bytes
          AND object.car_verification_method = 'car_import_recursively_pinned'
          AND object.car_artifact_id = member.car_artifact_id
          AND object.car_import_receipt_id = receipt.car_import_receipt_id
          AND object.receipt_sha256 = receipt.receipt_sha256
          AND receipt.verification_method = 'car_import_recursively_pinned'
          AND bulk.verification_method = 'car_import_recursively_pinned'
      )
    );
  SELECT coalesce(sum(receipt.import_attempt_count), 0)::integer,
         coalesce(sum(receipt.request_cost_usd), 0)::numeric(18, 12)
  INTO car_request_count, car_request_cost
  FROM oracle_candidate_source_snapshot_car_import_receipts receipt
  WHERE receipt.plan_id = NEW.plan_id;
  total_request_count := accounting_row.request_count + car_request_count;
  total_request_cost := accounting_row.request_cost_usd + car_request_cost;

  expected_payload := jsonb_build_object(
    'admittedRequestCostUsd', total_request_cost::double precision,
    'admittedRequestCount', total_request_count,
    'approvalId', NEW.approval_id,
    'exactObjectCount', actual_count,
    'exactTotalBytes', actual_bytes,
    'inventoryRootCid', plan_row.inventory_root_cid,
    'inventoryRootSha256', plan_row.inventory_root_sha256,
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'providerCidMismatchCount', mismatch_count,
    'unresolvedObjectCount', unresolved_count,
    'verifiedAt', to_char(NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  );
  expected_closure_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_closure_id := 'snapshotdemouploadclosure_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-closure-v1',
      plan_row.plan_id,
      expected_closure_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF plan_row.state IS DISTINCT FROM 'executing' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.exact_object_count IS DISTINCT FROM plan_row.exact_upload_object_count OR
     NEW.exact_total_bytes IS DISTINCT FROM plan_row.exact_upload_bytes OR
     NEW.verified_object_count IS DISTINCT FROM actual_count::integer OR
     NEW.verified_total_bytes IS DISTINCT FROM actual_bytes OR
     actual_count IS DISTINCT FROM plan_row.exact_upload_object_count::bigint OR
     actual_bytes IS DISTINCT FROM plan_row.exact_upload_bytes OR
     unresolved_count IS DISTINCT FROM 0::bigint OR
     mismatch_count IS DISTINCT FROM 0::bigint OR
     unjournaled_count IS DISTINCT FROM 0::bigint OR
     NEW.unresolved_object_count IS DISTINCT FROM 0 OR
     NEW.provider_cid_mismatch_count IS DISTINCT FROM 0 OR
     NEW.inventory_root_cid IS DISTINCT FROM plan_row.inventory_root_cid OR
     NEW.inventory_root_sha256 IS DISTINCT FROM plan_row.inventory_root_sha256 OR
     NEW.admitted_request_count IS DISTINCT FROM total_request_count OR
     NEW.admitted_request_cost_usd IS DISTINCT FROM total_request_cost OR
     NEW.closure_sha256 IS DISTINCT FROM expected_closure_sha256 OR
     NEW.closure_id IS DISTINCT FROM expected_closure_id OR
     total_request_count > plan_row.maximum_request_count OR
     total_request_cost > plan_row.budget_limit_usd OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.approval_id = NEW.approval_id
         AND approval.plan_id = NEW.plan_id
         AND approval.plan_sha256 = NEW.plan_sha256
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.operation_kind IN ('put_object', 'inspect_object')
         AND request.outcome = 'request_started'
     ) THEN
    RAISE EXCEPTION 'candidate source-snapshot upload closure is not exact and complete';
  END IF;
  RETURN NEW;
END;
$$;

-- A prior unresolved per-object inspection is superseded only by the new,
-- stronger exact CAR proof for that same member. All other quarantine remains
-- a closure blocker.
CREATE OR REPLACE FUNCTION oracle_guard_css_upload_closure_quarantine()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_members member
      ON member.inspection_cycle_id = cycle.inspection_cycle_id
    LEFT JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
      ON resolution.inspection_cycle_id = member.inspection_cycle_id
     AND resolution.domain = member.domain
     AND resolution.remote_object_key = member.remote_object_key
    WHERE cycle.plan_id = NEW.plan_id
      AND resolution.inspection_cycle_id IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_demo_objects object
        JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
          ON bulk.car_artifact_id = object.car_artifact_id
         AND bulk.car_import_receipt_id = object.car_import_receipt_id
         AND bulk.plan_id = object.plan_id
        WHERE object.plan_id = member.plan_id
          AND object.domain = member.domain
          AND object.remote_object_key = member.remote_object_key
          AND object.status = 'verified'
          AND object.car_verification_method =
            'car_import_recursively_pinned'
      )
  ) OR EXISTS (
    SELECT 1
    FROM oracle_css_current_upload_cycle_uncertain_rows(NEW.plan_id) uncertain
    WHERE NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_objects object
      JOIN oracle_candidate_source_snapshot_car_bulk_verifications bulk
        ON bulk.car_artifact_id = object.car_artifact_id
       AND bulk.car_import_receipt_id = object.car_import_receipt_id
       AND bulk.plan_id = object.plan_id
      WHERE object.plan_id = uncertain.plan_id
        AND object.domain = uncertain.domain
        AND object.remote_object_key = uncertain.remote_object_key
        AND object.status = 'verified'
        AND object.car_verification_method =
          'car_import_recursively_pinned'
    )
  ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload closure has unresolved quarantine';
  END IF;
  RETURN NEW;
END;
$$;

COMMIT;
