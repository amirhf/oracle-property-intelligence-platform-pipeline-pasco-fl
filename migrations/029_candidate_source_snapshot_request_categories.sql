-- Additive Session 2A request-category, plan-derivation, and receipt-bound
-- verification guards. Migrations 025 through 028 remain immutable. Legacy
-- v2.0.0 rows remain readable audit evidence but cannot gain new approval or
-- execution authority after this migration.

DO $$
DECLARE
  constraint_row record;
BEGIN
  FOR constraint_row IN
    SELECT constraint_name
    FROM information_schema.table_constraints
    WHERE table_schema = current_schema()
      AND table_name = 'oracle_candidate_source_snapshot_demo_plans'
      AND constraint_type = 'CHECK'
      AND constraint_name IN (
        SELECT constraint_row_source.conname
        FROM pg_constraint constraint_row_source
        JOIN pg_class relation ON relation.oid = constraint_row_source.conrelid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = current_schema()
          AND relation.relname = 'oracle_candidate_source_snapshot_demo_plans'
          AND (
            pg_get_constraintdef(constraint_row_source.oid) LIKE '%plan_version =%2.0.0%'
            OR pg_get_constraintdef(constraint_row_source.oid) LIKE '%request_limit%1000000%'
          )
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_demo_plans DROP CONSTRAINT %I',
      constraint_row.constraint_name
    );
  END LOOP;

  FOR constraint_row IN
    SELECT constraint_row_source.conname AS constraint_name
    FROM pg_constraint constraint_row_source
    JOIN pg_class relation ON relation.oid = constraint_row_source.conrelid
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'oracle_candidate_source_snapshot_demo_accounting'
      AND constraint_row_source.contype = 'c'
      AND pg_get_constraintdef(constraint_row_source.oid) LIKE '%request_count%1000000%'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_demo_accounting DROP CONSTRAINT %I',
      constraint_row.constraint_name
    );
  END LOOP;

  FOR constraint_row IN
    SELECT constraint_row_source.conname AS constraint_name
    FROM pg_constraint constraint_row_source
    JOIN pg_class relation ON relation.oid = constraint_row_source.conrelid
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'oracle_candidate_source_snapshot_demo_upload_closures'
      AND constraint_row_source.contype = 'c'
      AND pg_get_constraintdef(constraint_row_source.oid) LIKE '%admitted_request_count%1000000%'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_demo_upload_closures DROP CONSTRAINT %I',
      constraint_row.constraint_name
    );
  END LOOP;

  FOR constraint_row IN
    SELECT constraint_row_source.conname AS constraint_name
    FROM pg_constraint constraint_row_source
    JOIN pg_class relation ON relation.oid = constraint_row_source.conrelid
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'oracle_candidate_source_snapshot_demo_approvals'
      AND constraint_row_source.contype = 'c'
      AND pg_get_constraintdef(constraint_row_source.oid) LIKE '%approval_version%candidate-source-snapshot-approval-v2%'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_demo_approvals DROP CONSTRAINT %I',
      constraint_row.constraint_name
    );
  END LOOP;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_plans
  ADD CONSTRAINT oracle_css_plan_version_v21_check
    CHECK (plan_version IN ('2.0.0', '2.1.0')),
  ADD CONSTRAINT oracle_css_request_limit_v21_check
    CHECK (request_limit BETWEEN 1 AND 1100000);

ALTER TABLE oracle_candidate_source_snapshot_demo_accounting
  ADD CONSTRAINT oracle_css_accounting_request_count_v21_check
    CHECK (request_count BETWEEN 0 AND 1100000);

ALTER TABLE oracle_candidate_source_snapshot_demo_upload_closures
  ADD CONSTRAINT oracle_css_closure_request_count_v21_check
    CHECK (admitted_request_count BETWEEN 0 AND 1100000);

ALTER TABLE oracle_candidate_source_snapshot_demo_approvals
  ADD COLUMN implementation_commit_sha text CHECK (
    implementation_commit_sha IS NULL OR
    implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  ADD CONSTRAINT oracle_css_approval_version_v3_check
    CHECK (approval_version IN (
      'candidate-source-snapshot-approval-v2',
      'candidate-source-snapshot-approval-v3'
    ));

CREATE TABLE oracle_candidate_source_snapshot_demo_plan_derivations (
  derivation_id text PRIMARY KEY CHECK (
    derivation_id ~ '^snapshotdemoderivation_[a-f0-9]{32}$'
  ),
  predecessor_plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  predecessor_plan_sha256 text NOT NULL CHECK (
    predecessor_plan_sha256 ~ '^[a-f0-9]{64}$'
  ),
  derived_plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  derived_plan_sha256 text NOT NULL CHECK (
    derived_plan_sha256 ~ '^[a-f0-9]{64}$'
  ),
  reason text NOT NULL CHECK (reason = 'request_envelope_replacement'),
  shared_binding_sha256 text NOT NULL CHECK (
    shared_binding_sha256 ~ '^[a-f0-9]{64}$'
  ),
  predecessor_envelope_sha256 text NOT NULL CHECK (
    predecessor_envelope_sha256 ~ '^[a-f0-9]{64}$'
  ),
  derived_envelope_sha256 text NOT NULL CHECK (
    derived_envelope_sha256 ~ '^[a-f0-9]{64}$'
  ),
  derivation_sha256 text NOT NULL UNIQUE CHECK (
    derivation_sha256 ~ '^[a-f0-9]{64}$'
  ),
  derived_at timestamptz NOT NULL,
  derived_at_iso text NOT NULL CHECK (
    derived_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND derived_at_iso::timestamptz = derived_at
  ),
  CHECK (predecessor_plan_id <> derived_plan_id),
  CHECK (predecessor_plan_sha256 <> derived_plan_sha256),
  CHECK (predecessor_envelope_sha256 <> derived_envelope_sha256)
);

CREATE TABLE oracle_candidate_source_snapshot_demo_request_categories (
  plan_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  request_category text NOT NULL CHECK (request_category IN (
    'upload_provider_cid',
    'ambiguous_upload_inspection',
    'bucket_names_preflight',
    'names_mutation',
    'control_public_observation',
    'recovery',
    'rollback',
    'final_credential_free_verification'
  )),
  planned_successful_request_count integer NOT NULL CHECK (
    planned_successful_request_count BETWEEN 0 AND 1100000
  ),
  planned_maximum_request_count integer NOT NULL CHECK (
    planned_maximum_request_count BETWEEN 0 AND 1100000 AND
    planned_maximum_request_count >= planned_successful_request_count
  ),
  consumed_request_count integer NOT NULL DEFAULT 0 CHECK (
    consumed_request_count BETWEEN 0 AND planned_maximum_request_count
  ),
  request_cost_usd numeric(18, 12) NOT NULL DEFAULT 0 CHECK (
    request_cost_usd >= 0 AND request_cost_usd <= 25
  ),
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (plan_id, request_category)
);

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD COLUMN request_category text,
  ADD COLUMN logical_request_id text,
  ADD COLUMN attempt_sequence integer,
  ADD COLUMN redirect_sequence integer;

DO $$
DECLARE
  constraint_row record;
BEGIN
  FOR constraint_row IN
    SELECT constraint_row_source.conname AS constraint_name
    FROM pg_constraint constraint_row_source
    JOIN pg_class relation ON relation.oid = constraint_row_source.conrelid
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND relation.relname = 'oracle_candidate_source_snapshot_demo_requests'
      AND constraint_row_source.contype = 'c'
  LOOP
    EXECUTE format(
      'ALTER TABLE oracle_candidate_source_snapshot_demo_requests DROP CONSTRAINT %I',
      constraint_row.constraint_name
    );
  END LOOP;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD CONSTRAINT oracle_css_request_id_v21_check CHECK (
    request_id ~ '^snapshotdemorequest_[a-f0-9]{32}$'
  ),
  ADD CONSTRAINT oracle_css_request_class_v21_check CHECK (
    operation_class IN (
      'class_a_mutation', 'class_b_read', 'names_api', 'public_resolver',
      'free_operation'
    )
  ),
  ADD CONSTRAINT oracle_css_request_kind_v21_check CHECK (
    operation_kind IN (
      'put_object', 'inspect_object', 'names_read', 'names_update',
      'public_resolve', 'bucket_head', 'bucket_prefix_scan',
      'storage_network_check', 'account_usage', 'bucket_usage',
      'immutable_artifact_read', 'immutable_artifact_stat',
      'immutable_artifact_range_read', 'verification_read', 'search_read',
      'property_read'
    )
  ),
  ADD CONSTRAINT oracle_css_request_category_v21_check CHECK (
    request_category IS NULL OR request_category IN (
      'upload_provider_cid',
      'ambiguous_upload_inspection',
      'bucket_names_preflight',
      'names_mutation',
      'control_public_observation',
      'recovery',
      'rollback',
      'final_credential_free_verification'
    )
  ),
  ADD CONSTRAINT oracle_css_logical_request_id_v21_check CHECK (
    logical_request_id IS NULL OR
    logical_request_id ~ '^snapshotdemologicalrequest_[a-f0-9]{32}$'
  ),
  ADD CONSTRAINT oracle_css_request_attempt_v21_check CHECK (
    attempt_sequence IS NULL OR attempt_sequence BETWEEN 1 AND 3
  ),
  ADD CONSTRAINT oracle_css_request_redirect_v21_check CHECK (
    redirect_sequence IS NULL OR redirect_sequence BETWEEN 0 AND 2
  ),
  ADD CONSTRAINT oracle_css_request_domain_v21_check CHECK (
    domain IN ('open_data', 'query_table')
  ),
  ADD CONSTRAINT oracle_css_request_cycle_v21_check CHECK (
    cycle_sequence IS NULL OR cycle_sequence BETWEEN 1 AND 32
  ),
  ADD CONSTRAINT oracle_css_request_resolver_v21_check CHECK (
    resolver IS NULL OR resolver IN (
      'filebase_control', 'filebase_gateway', 'delegated_ipfs',
      'ipfs_io', 'dweb_link'
    )
  ),
  ADD CONSTRAINT oracle_css_request_cost_v21_check CHECK (
    request_cost_usd IN (0, 0.0000045)
  ),
  ADD CONSTRAINT oracle_css_request_outcome_v21_check CHECK (
    outcome IN (
      'request_started', 'succeeded', 'absent', 'ambiguous',
      'retryable_failure', 'timeout_unknown', 'terminal_failure'
    )
  ),
  ADD CONSTRAINT oracle_css_request_receipt_v21_check CHECK (
    receipt_sha256 IS NULL OR receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_css_request_completion_v21_check CHECK (
    (outcome = 'request_started' AND completed_at IS NULL) OR
    (outcome <> 'request_started' AND completed_at IS NOT NULL)
  ),
  ADD CONSTRAINT oracle_css_request_operation_binding_v21_check CHECK (
    (request_category IS NULL AND
      operation_kind IN (
        'put_object', 'inspect_object', 'names_read', 'names_update',
        'public_resolve'
      )) OR
    (request_category = 'upload_provider_cid' AND
      operation_kind = 'put_object' AND
      operation_class = 'class_a_mutation' AND intent_id IS NULL AND
      remote_object_key IS NOT NULL AND cycle_sequence IS NULL AND
      resolver IS NULL) OR
    (request_category = 'ambiguous_upload_inspection' AND
      operation_kind = 'inspect_object' AND
      operation_class = 'class_b_read' AND intent_id IS NULL AND
      remote_object_key IS NOT NULL AND cycle_sequence IS NULL AND
      resolver IS NULL) OR
    (request_category = 'bucket_names_preflight' AND (
      (operation_kind IN (
         'bucket_head', 'bucket_prefix_scan', 'storage_network_check',
         'account_usage', 'bucket_usage'
       ) AND operation_class = 'class_b_read' AND intent_id IS NULL) OR
      (operation_kind = 'names_read' AND operation_class = 'names_api') OR
      (operation_kind = 'public_resolve' AND
       operation_class = 'public_resolver')
    )) OR
    (request_category = 'names_mutation' AND
      operation_kind = 'names_update' AND operation_class = 'names_api' AND
      intent_id IS NOT NULL) OR
    (request_category = 'control_public_observation' AND
      intent_id IS NOT NULL AND (
        (operation_kind = 'names_read' AND operation_class = 'names_api') OR
        (operation_kind = 'public_resolve' AND
         operation_class = 'public_resolver')
      )) OR
    (request_category IN ('recovery', 'rollback') AND
      intent_id IS NOT NULL AND (
        (operation_kind IN ('names_read', 'names_update') AND
         operation_class = 'names_api') OR
        (operation_kind = 'public_resolve' AND
         operation_class = 'public_resolver')
      )) OR
    (request_category = 'final_credential_free_verification' AND
      operation_kind IN (
        'immutable_artifact_read', 'immutable_artifact_stat',
        'immutable_artifact_range_read', 'verification_read', 'search_read',
        'property_read'
      ) AND operation_class = 'class_b_read' AND intent_id IS NULL)
  );

CREATE TABLE oracle_candidate_source_snapshot_demo_remote_read_receipts (
  verification_receipt_id text PRIMARY KEY CHECK (
    verification_receipt_id ~
      '^snapshotdemoverificationreceipt_[a-f0-9]{32}$'
  ),
  request_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  plan_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  check_kind text NOT NULL CHECK (check_kind IN (
    'plan_artifact', 'manifest', 'inventory', 'open_data_graph',
    'query_table', 'coverage', 'fixture_exclusion'
  )),
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_bytes bigint NOT NULL CHECK (expected_bytes > 0),
  byte_range_start bigint CHECK (byte_range_start IS NULL OR byte_range_start >= 0),
  byte_range_end bigint CHECK (
    byte_range_end IS NULL OR byte_range_end >= byte_range_start
  ),
  outcome text NOT NULL CHECK (outcome IN (
    'verified', 'retryable_failure', 'timeout_unknown', 'terminal_failure'
  )),
  response_bytes bigint CHECK (response_bytes IS NULL OR response_bytes >= 0),
  response_sha256 text CHECK (
    response_sha256 IS NULL OR response_sha256 ~ '^[a-f0-9]{64}$'
  ),
  receipt_payload jsonb NOT NULL,
  receipt_sha256 text NOT NULL UNIQUE CHECK (
    receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  observed_at timestamptz NOT NULL,
  observed_at_iso text NOT NULL CHECK (
    observed_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND observed_at_iso::timestamptz = observed_at
  ),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    ),
  CHECK (
    (byte_range_start IS NULL AND byte_range_end IS NULL) OR
    (byte_range_start IS NOT NULL AND byte_range_end IS NOT NULL)
  ),
  CHECK (
    (outcome = 'verified' AND response_bytes IS NOT NULL AND
      response_sha256 IS NOT NULL) OR
    (outcome <> 'verified')
  )
);

ALTER TABLE oracle_candidate_source_snapshot_demo_remote_checks
  ADD COLUMN verification_receipt_set_sha256 text,
  ADD COLUMN verification_receipt_count integer,
  ADD CONSTRAINT oracle_css_remote_check_receipt_set_sha_check CHECK (
    verification_receipt_set_sha256 IS NULL OR
    verification_receipt_set_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT oracle_css_remote_check_receipt_count_check CHECK (
    verification_receipt_count IS NULL OR verification_receipt_count > 0
  );

CREATE INDEX oracle_css_remote_read_receipts_plan_kind_idx
  ON oracle_candidate_source_snapshot_demo_remote_read_receipts(
    plan_id, check_kind, request_id
  );

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_operation_counts_valid(
  checked_counts jsonb
)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT COALESCE(
    jsonb_typeof(checked_counts) = 'object'
    AND checked_counts ?& ARRAY[
      'classAMutations', 'classBReads', 'freeOperations',
      'namesApiOperations', 'publicResolverOperations', 'total'
    ]
    AND (SELECT count(*) FROM jsonb_object_keys(checked_counts)) = 6
    AND (checked_counts->>'classAMutations')::integer >= 0
    AND (checked_counts->>'classBReads')::integer >= 0
    AND (checked_counts->>'freeOperations')::integer >= 0
    AND (checked_counts->>'namesApiOperations')::integer >= 0
    AND (checked_counts->>'publicResolverOperations')::integer >= 0
    AND (checked_counts->>'total')::integer =
      (checked_counts->>'classAMutations')::integer +
      (checked_counts->>'classBReads')::integer +
      (checked_counts->>'freeOperations')::integer +
      (checked_counts->>'namesApiOperations')::integer +
      (checked_counts->>'publicResolverOperations')::integer,
    false
  );
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_v21_categories_valid(
  checked_payload jsonb
)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT COALESCE(
    checked_payload->>'version' = '2.1.0'
    AND checked_payload->'requestEnvelope'->>'schemaVersion' =
      'candidate-request-envelope-v3'
    AND jsonb_typeof(
      checked_payload->'requestEnvelope'->'categoryRequests'
    ) = 'array'
    AND jsonb_array_length(
      checked_payload->'requestEnvelope'->'categoryRequests'
    ) = 8
    AND oracle_candidate_source_snapshot_operation_counts_valid(
      checked_payload->'requestEnvelope'->'successfulExecution'
    )
    AND oracle_candidate_source_snapshot_operation_counts_valid(
      checked_payload->'requestEnvelope'->'maximumAttempts'
    )
    AND oracle_candidate_source_snapshot_operation_counts_valid(
      checked_payload->'requestEnvelope'->
        'ambiguousObjectInspectionAllowance'
    )
    AND oracle_candidate_source_snapshot_operation_counts_valid(
      (checked_payload->'requestEnvelope'->'recoveryAllowance') -
        'observationCyclesPerDomain'
    )
    AND checked_payload->'requestEnvelope'->'categoryRequests'->0->>'category' =
      'upload_provider_cid'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->1->>'category' =
      'ambiguous_upload_inspection'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->2->>'category' =
      'bucket_names_preflight'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->3->>'category' =
      'names_mutation'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->4->>'category' =
      'control_public_observation'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->5->>'category' =
      'recovery'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->6->>'category' =
      'rollback'
    AND checked_payload->'requestEnvelope'->'categoryRequests'->7->>'category' =
      'final_credential_free_verification'
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        checked_payload->'requestEnvelope'->'categoryRequests'
      ) category
      WHERE (SELECT count(*) FROM jsonb_object_keys(category)) <> 3
         OR jsonb_typeof(category->'successfulRequests') <> 'number'
         OR jsonb_typeof(category->'maximumRequests') <> 'number'
         OR (category->>'successfulRequests')::integer < 0
         OR (category->>'maximumRequests')::integer <
            (category->>'successfulRequests')::integer
    )
    AND (
      SELECT sum((category->>'successfulRequests')::integer)
      FROM jsonb_array_elements(
        checked_payload->'requestEnvelope'->'categoryRequests'
      ) category
    ) = (checked_payload->'requestEnvelope'->
      'successfulExecution'->>'total')::integer
    AND (
      SELECT sum((category->>'maximumRequests')::integer)
      FROM jsonb_array_elements(
        checked_payload->'requestEnvelope'->'categoryRequests'
      ) category
    ) = (checked_payload->'requestEnvelope'->>'maximumTotalRequests')::integer
    AND (checked_payload->'requestEnvelope'->'maximumAttempts'->>'total')::integer +
      (checked_payload->'requestEnvelope'->
        'ambiguousObjectInspectionAllowance'->>'total')::integer +
      (checked_payload->'requestEnvelope'->'recoveryAllowance'->>'total')::integer =
      (checked_payload->'requestEnvelope'->>'maximumTotalRequests')::integer
    AND (checked_payload->'requestEnvelope'->>'maximumTotalRequests')::integer
      BETWEEN 1 AND 1100000
    AND jsonb_typeof(
      checked_payload->'requestEnvelope'->'finalVerification'
    ) = 'object'
    AND checked_payload->'requestEnvelope'->'finalVerification' ?& ARRAY[
      'deterministicRequiredMaximumRequests', 'logicalRequests',
      'maximumRedirectsPerAttempt',
      'maximumTransportAttemptsPerLogicalRequest',
      'nonParquetLogicalRequests', 'parquetLogicalRequests',
      'protectedHeadroomRequests', 'schemaVersion'
    ]
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'requestEnvelope'->'finalVerification'
    )) = 8
    AND checked_payload->'requestEnvelope'->'finalVerification'->>
      'schemaVersion' = 'candidate-final-verification-budget-v1'
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'maximumRedirectsPerAttempt')::integer = 2
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'maximumTransportAttemptsPerLogicalRequest')::integer = 3
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'nonParquetLogicalRequests')::integer = 109
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'parquetLogicalRequests')::integer = 8194
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'logicalRequests')::integer =
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'nonParquetLogicalRequests')::integer +
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'parquetLogicalRequests')::integer
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'logicalRequests')::integer =
      (checked_payload->'requestEnvelope'->'categoryRequests'->7->>
        'successfulRequests')::integer
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'deterministicRequiredMaximumRequests')::integer > 0
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'protectedHeadroomRequests')::integer > 0
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'deterministicRequiredMaximumRequests')::integer +
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'protectedHeadroomRequests')::integer =
      (checked_payload->'requestEnvelope'->'categoryRequests'->7->>
        'maximumRequests')::integer
    AND (checked_payload->'requestEnvelope'->'categoryRequests'->0->>
      'maximumRequests')::integer =
      (checked_payload->'requestEnvelope'->'maximumAttempts'->>
        'classAMutations')::integer
    AND (checked_payload->'requestEnvelope'->'categoryRequests'->1->>
      'maximumRequests')::integer =
      (checked_payload->'requestEnvelope'->
        'ambiguousObjectInspectionAllowance'->>'total')::integer
    AND (checked_payload->'requestEnvelope'->'categoryRequests'->5->>
      'maximumRequests')::integer +
      (checked_payload->'requestEnvelope'->'categoryRequests'->6->>
        'maximumRequests')::integer =
      (checked_payload->'requestEnvelope'->'recoveryAllowance'->>'total')::integer
    AND (checked_payload->'requestEnvelope'->'categoryRequests'->7->>
      'maximumRequests')::integer =
      (checked_payload->'requestEnvelope'->'maximumAttempts'->>
        'freeOperations')::integer
    AND (checked_payload->'requestEnvelope'->'categoryRequests'->7->>
      'successfulRequests')::integer =
      (checked_payload->'requestEnvelope'->'successfulExecution'->>
        'freeOperations')::integer
    AND checked_payload->'costEnvelope'->>'schemaVersion' =
      'candidate-cost-envelope-v3',
    false
  );
$$;

-- Compact v3 is the authoritative v2.1 representation. It deliberately
-- replaces the expanded compatibility aggregates above so the frozen plan
-- artifact remains exactly 11,210 bytes.
CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_v21_categories_valid(
  checked_payload jsonb
)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT COALESCE(
    checked_payload->>'version' = '2.1.0'
    AND checked_payload->'requestEnvelope'->>'schemaVersion' =
      'candidate-request-envelope-v3'
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'requestEnvelope'
    )) = 5
    AND checked_payload->'requestEnvelope' ?& ARRAY[
      'categoryRequests', 'finalVerification', 'maximumTotalRequests',
      'schemaVersion', 'successfulTotalRequests'
    ]
    AND jsonb_typeof(
      checked_payload->'requestEnvelope'->'categoryRequests'
    ) = 'object'
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'requestEnvelope'->'categoryRequests'
    )) = 8
    AND checked_payload->'requestEnvelope'->'categoryRequests' ?& ARRAY[
      'upload_provider_cid', 'ambiguous_upload_inspection',
      'bucket_names_preflight', 'names_mutation',
      'control_public_observation', 'recovery', 'rollback',
      'final_credential_free_verification'
    ]
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_each(
        checked_payload->'requestEnvelope'->'categoryRequests'
      ) category(category_name, request_counts)
      WHERE jsonb_typeof(request_counts) <> 'array'
         OR jsonb_array_length(request_counts) <> 2
         OR jsonb_typeof(request_counts->0) <> 'number'
         OR jsonb_typeof(request_counts->1) <> 'number'
         OR (request_counts->>0)::integer < 0
         OR (request_counts->>1)::integer < (request_counts->>0)::integer
    )
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'upload_provider_cid' = jsonb_build_array(
        (checked_payload->'inventory'->>'objectCount')::integer,
        (checked_payload->'inventory'->>'objectCount')::integer *
          ((checked_payload->'limits'->>'maxRetries')::integer + 1)
      )
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'ambiguous_upload_inspection' = jsonb_build_array(
        0,
        least(
          24000,
          (checked_payload->'inventory'->>'objectCount')::integer *
            ((checked_payload->'limits'->>'maxRetries')::integer + 1)
        )
      )
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'bucket_names_preflight' = '[8, 48]'::jsonb
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'names_mutation' = '[2, 2]'::jsonb
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'control_public_observation' = '[12, 42]'::jsonb
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'recovery' = '[0, 338]'::jsonb
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'rollback' = '[0, 44]'::jsonb
    AND checked_payload->'requestEnvelope'->'categoryRequests'->
      'final_credential_free_verification' = jsonb_build_array(
        8303,
        (checked_payload->'limits'->>'maxRequests')::integer -
          (
            (checked_payload->'inventory'->>'objectCount')::integer *
              ((checked_payload->'limits'->>'maxRetries')::integer + 1) +
            least(
              24000,
              (checked_payload->'inventory'->>'objectCount')::integer *
                ((checked_payload->'limits'->>'maxRetries')::integer + 1)
            ) +
            48 + 2 + 42 + 338 + 44
          )
      )
    AND (checked_payload->'requestEnvelope'->>'successfulTotalRequests')::integer =
      (checked_payload->'inventory'->>'objectCount')::integer + 8325
    AND (checked_payload->'requestEnvelope'->>'maximumTotalRequests')::integer =
      (checked_payload->'limits'->>'maxRequests')::integer
    AND (checked_payload->'requestEnvelope'->>'maximumTotalRequests')::integer
      BETWEEN 1 AND 1100000
    AND jsonb_typeof(
      checked_payload->'requestEnvelope'->'finalVerification'
    ) = 'object'
    AND checked_payload->'requestEnvelope'->'finalVerification' ?& ARRAY[
      'deterministicRequiredMaximumRequests', 'logicalRequests',
      'maximumRedirectsPerAttempt',
      'maximumTransportAttemptsPerLogicalRequest',
      'nonParquetLogicalRequests', 'parquetLogicalRequests',
      'protectedHeadroomRequests', 'schemaVersion'
    ]
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'requestEnvelope'->'finalVerification'
    )) = 8
    AND checked_payload->'requestEnvelope'->'finalVerification'->>
      'schemaVersion' = 'candidate-final-verification-budget-v1'
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'maximumRedirectsPerAttempt')::integer = 2
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'maximumTransportAttemptsPerLogicalRequest')::integer = 3
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'nonParquetLogicalRequests')::integer = 109
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'parquetLogicalRequests')::integer = 8194
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'logicalRequests')::integer = 8303
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'logicalRequests')::integer =
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'nonParquetLogicalRequests')::integer +
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'parquetLogicalRequests')::integer
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'deterministicRequiredMaximumRequests')::integer =
        (checked_payload->'requestEnvelope'->'finalVerification'->>
          'logicalRequests')::integer *
        (checked_payload->'requestEnvelope'->'finalVerification'->>
          'maximumTransportAttemptsPerLogicalRequest')::integer *
        ((checked_payload->'requestEnvelope'->'finalVerification'->>
          'maximumRedirectsPerAttempt')::integer + 1)
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'protectedHeadroomRequests')::integer > 0
    AND (checked_payload->'requestEnvelope'->'finalVerification'->>
      'deterministicRequiredMaximumRequests')::integer +
      (checked_payload->'requestEnvelope'->'finalVerification'->>
        'protectedHeadroomRequests')::integer =
      (checked_payload->'requestEnvelope'->'categoryRequests'->
        'final_credential_free_verification'->>1)::integer
    AND checked_payload->'costEnvelope'->>'schemaVersion' =
      'candidate-cost-envelope-v3'
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'costEnvelope'
    )) = 7
    AND checked_payload->'costEnvelope' ?& ARRAY[
      'fixedAccountPlanMonthlyUsd', 'incrementalExecutionUsd',
      'maximumIncrementalUsd', 'maximumTotalUsd', 'requestUsd',
      'schemaVersion', 'storageUsd'
    ]
    AND (SELECT count(*) FROM jsonb_object_keys(
      checked_payload->'costEnvelope'->'requestUsd'
    )) = 2
    AND checked_payload->'costEnvelope'->'requestUsd' ?& ARRAY[
      'maximumAttempts', 'successfulExecution'
    ]
    AND jsonb_typeof(checked_payload->'costEnvelope'->'requestUsd'->
      'maximumAttempts') = 'number'
    AND jsonb_typeof(checked_payload->'costEnvelope'->'requestUsd'->
      'successfulExecution') = 'number'
    AND (checked_payload->'costEnvelope'->'requestUsd'->>
      'successfulExecution')::numeric >= 0
    AND (checked_payload->'costEnvelope'->'requestUsd'->>
      'successfulExecution')::numeric =
      (checked_payload->'requestEnvelope'->>
        'successfulTotalRequests')::integer * 0.0000045::numeric
    AND (checked_payload->'costEnvelope'->'requestUsd'->>
      'maximumAttempts')::numeric >=
      (checked_payload->'costEnvelope'->'requestUsd'->>
        'successfulExecution')::numeric
    AND (checked_payload->'costEnvelope'->'requestUsd'->>
      'maximumAttempts')::numeric =
      (checked_payload->'requestEnvelope'->>
        'maximumTotalRequests')::integer * 0.0000045::numeric
    AND (checked_payload->'costEnvelope'->>'storageUsd')::numeric = round(
      (checked_payload->'inventory'->>'totalBytes')::numeric /
        1073741824::numeric * 0.0162::numeric,
      12
    )
    AND (checked_payload->'costEnvelope'->>
      'fixedAccountPlanMonthlyUsd')::numeric =
      (checked_payload->'pricing'->'fixedAccountPlan'->>'monthlyUsd')::numeric
    AND (checked_payload->'costEnvelope'->>
      'incrementalExecutionUsd')::numeric = round(
      (checked_payload->'costEnvelope'->>'storageUsd')::numeric +
        (checked_payload->'costEnvelope'->'requestUsd'->>
          'successfulExecution')::numeric,
      12
    )
    AND (checked_payload->'costEnvelope'->>
      'maximumIncrementalUsd')::numeric = round(
      (checked_payload->'costEnvelope'->>'storageUsd')::numeric +
        (checked_payload->'costEnvelope'->'requestUsd'->>
          'maximumAttempts')::numeric,
      12
    )
    AND (checked_payload->'costEnvelope'->>'maximumTotalUsd')::numeric = round(
      (checked_payload->'costEnvelope'->>'maximumIncrementalUsd')::numeric +
        (checked_payload->'costEnvelope'->>
          'fixedAccountPlanMonthlyUsd')::numeric,
      12
    )
    AND (checked_payload->'costEnvelope'->>'maximumTotalUsd')::numeric <=
      (checked_payload->'limits'->>'maxBudgetUsd')::numeric,
    false
  );
$$;

-- The compact envelope is now the only v2.1 authority. The temporary helper
-- used while defining the superseded expanded shape must not remain callable.
DROP FUNCTION oracle_candidate_source_snapshot_operation_counts_valid(jsonb);

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_shared_binding_v1(
  checked_payload jsonb
)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT (
    checked_payload - ARRAY[
      'planId', 'planSha256', 'version', 'requestEnvelope', 'costEnvelope',
      'formatPadding', 'limits'
    ]::text[]
  ) || jsonb_build_object(
    'limits', (checked_payload->'limits') - 'maxRequests'::text
  );
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_v21_plan_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.plan_version <> '2.1.0' OR
     NOT oracle_candidate_source_snapshot_v21_categories_valid(
       NEW.plan_payload
     ) OR
     NEW.request_envelope IS DISTINCT FROM
       NEW.plan_payload->'requestEnvelope' OR
     NEW.cost_envelope IS DISTINCT FROM NEW.plan_payload->'costEnvelope' OR
     NEW.maximum_request_count IS DISTINCT FROM
       (NEW.request_envelope->>'maximumTotalRequests')::integer OR
     NEW.request_limit > 1100000 OR
     NEW.maximum_request_count > NEW.request_limit THEN
    RAISE EXCEPTION
      'new candidate source-snapshot plans require the exact v2.1 categorized request envelope';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oracle_css_v21_plan_insert_guard
  ON oracle_candidate_source_snapshot_demo_plans;
CREATE TRIGGER oracle_css_v21_plan_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_v21_plan_insert();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_plan_derivation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  predecessor oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  derived oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  expected_shared_binding_sha256 text;
  expected_predecessor_envelope_sha256 text;
  expected_derived_envelope_sha256 text;
  expected_derivation_sha256 text;
  expected_derivation_id text;
BEGIN
  SELECT * INTO STRICT predecessor
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.predecessor_plan_id
  FOR UPDATE;
  SELECT * INTO STRICT derived
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.derived_plan_id
  FOR UPDATE;

  expected_shared_binding_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(
      oracle_candidate_source_snapshot_shared_binding_v1(
        predecessor.plan_payload
      )
    ), 'UTF8'
  )), 'hex');
  expected_predecessor_envelope_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(predecessor.request_envelope), 'UTF8'
  )), 'hex');
  expected_derived_envelope_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(derived.request_envelope), 'UTF8'
  )), 'hex');
  expected_derivation_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'derivedAt', NEW.derived_at_iso,
      'derivedEnvelopeSha256', expected_derived_envelope_sha256,
      'derivedPlanId', derived.plan_id,
      'derivedPlanSha256', derived.plan_sha256,
      'predecessorEnvelopeSha256', expected_predecessor_envelope_sha256,
      'predecessorPlanId', predecessor.plan_id,
      'predecessorPlanSha256', predecessor.plan_sha256,
      'reason', 'request_envelope_replacement',
      'schemaVersion', 'candidate-source-snapshot-plan-derivation-v1',
      'sharedBindingSha256', expected_shared_binding_sha256
    )), 'UTF8'
  )), 'hex');
  expected_derivation_id := 'snapshotdemoderivation_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-plan-derivation-v1',
      predecessor.plan_id,
      derived.plan_id,
      expected_derivation_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF predecessor.plan_version <> '2.0.0' OR
     predecessor.state NOT IN (
       'awaiting_configuration', 'awaiting_approval', 'failed_terminal'
     ) OR
     derived.plan_version <> '2.1.0' OR
     derived.state <> 'awaiting_configuration' OR
     derived.revision <> 1 OR
     oracle_candidate_source_snapshot_shared_binding_v1(
       predecessor.plan_payload
     ) IS DISTINCT FROM
       oracle_candidate_source_snapshot_shared_binding_v1(
         derived.plan_payload
       ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
       WHERE attempt.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_inspections inspection
       WHERE inspection.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_checks check_row
       WHERE check_row.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_verifications verification
       WHERE verification.plan_id = predecessor.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_objects object
       WHERE object.plan_id = predecessor.plan_id
         AND (
           object.status <> 'pending' OR object.successful_effect_count <> 0
         )
     ) OR
     NEW.predecessor_plan_sha256 IS DISTINCT FROM predecessor.plan_sha256 OR
     NEW.derived_plan_sha256 IS DISTINCT FROM derived.plan_sha256 OR
     NEW.shared_binding_sha256 IS DISTINCT FROM
       expected_shared_binding_sha256 OR
     NEW.predecessor_envelope_sha256 IS DISTINCT FROM
       expected_predecessor_envelope_sha256 OR
     NEW.derived_envelope_sha256 IS DISTINCT FROM
       expected_derived_envelope_sha256 OR
     NEW.derivation_sha256 IS DISTINCT FROM expected_derivation_sha256 OR
     NEW.derivation_id IS DISTINCT FROM expected_derivation_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot plan derivation is not an exact effect-free request-envelope replacement';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_plan_derivation_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_plan_derivations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_plan_derivation();

CREATE OR REPLACE FUNCTION oracle_finalize_candidate_source_snapshot_plan_derivation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  transition_payload jsonb;
  transition_sha256 text;
  transition_id text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_demo_plans plan
    WHERE plan.plan_id = NEW.predecessor_plan_id
      AND plan.state <> 'failed_terminal'
  ) THEN
    UPDATE oracle_candidate_source_snapshot_demo_plans
    SET state = 'failed_terminal', revision = revision + 1
    WHERE plan_id = NEW.predecessor_plan_id;
  END IF;

  transition_payload := jsonb_build_object(
    'derivationId', NEW.derivation_id,
    'derivedPlanId', NEW.derived_plan_id,
    'derivedPlanSha256', NEW.derived_plan_sha256,
    'predecessorPlanId', NEW.predecessor_plan_id,
    'predecessorPlanSha256', NEW.predecessor_plan_sha256,
    'reason', 'request_envelope_superseded',
    'schemaVersion', 'candidate-source-snapshot-plan-supersession-v1'
  );
  transition_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(transition_payload), 'UTF8'
  )), 'hex');
  transition_id := 'snapshotdemoevent_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-plan-supersession-v1',
      NEW.predecessor_plan_id,
      transition_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);
  INSERT INTO oracle_candidate_source_snapshot_demo_events (
    event_id, plan_id, event_type, event_sha256, metadata, recorded_at
  ) VALUES (
    transition_id, NEW.predecessor_plan_id, 'request_envelope_superseded',
    transition_sha256, transition_payload, NEW.derived_at
  );
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_plan_derivation_finalize
  AFTER INSERT ON oracle_candidate_source_snapshot_demo_plan_derivations
  FOR EACH ROW EXECUTE FUNCTION oracle_finalize_candidate_source_snapshot_plan_derivation();

CREATE TRIGGER oracle_css_plan_derivation_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_demo_plan_derivations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request_category()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  category_binding jsonb;
  expected_incremental_cost numeric(18, 12);
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'candidate source-snapshot request category is immutable';
  END IF;

  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  category_binding :=
    plan_row.request_envelope->'categoryRequests'->NEW.request_category;

  IF plan_row.plan_version <> '2.1.0' OR
     NOT oracle_candidate_source_snapshot_v21_categories_valid(
       plan_row.plan_payload
     ) OR
     NEW.planned_successful_request_count IS DISTINCT FROM
       (category_binding->>0)::integer OR
     NEW.planned_maximum_request_count IS DISTINCT FROM
       (category_binding->>1)::integer THEN
    RAISE EXCEPTION
      'candidate source-snapshot request category does not match its immutable plan';
  END IF;

  IF TG_OP = 'INSERT' THEN
    IF NEW.consumed_request_count <> 0 OR NEW.request_cost_usd <> 0 OR
       NEW.revision <> 1 THEN
      RAISE EXCEPTION
        'candidate source-snapshot request category must begin unused';
    END IF;
    RETURN NEW;
  END IF;

  expected_incremental_cost := 0.0000045::numeric;
  IF NOT (
       plan_row.state = 'executing' OR
       (
         NEW.request_category = 'bucket_names_preflight' AND
         plan_row.state IN ('awaiting_configuration', 'awaiting_approval')
       )
     ) OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.request_category IS DISTINCT FROM NEW.request_category OR
     OLD.planned_successful_request_count IS DISTINCT FROM
       NEW.planned_successful_request_count OR
     OLD.planned_maximum_request_count IS DISTINCT FROM
       NEW.planned_maximum_request_count OR
     NEW.consumed_request_count IS DISTINCT FROM
       OLD.consumed_request_count + 1 OR
     NEW.request_cost_usd IS DISTINCT FROM
       OLD.request_cost_usd + expected_incremental_cost OR
     NEW.revision IS DISTINCT FROM OLD.revision + 1 THEN
    RAISE EXCEPTION
      'candidate source-snapshot request category admission is invalid';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_request_category_guard
  BEFORE INSERT OR UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_demo_request_categories
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_request_category();

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_has_exact_categories(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT COALESCE(
    (SELECT count(*) = 8
       FROM oracle_candidate_source_snapshot_demo_request_categories category
       WHERE category.plan_id = checked_plan_id)
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_request_categories category
      JOIN oracle_candidate_source_snapshot_demo_plans plan
        ON plan.plan_id = category.plan_id
      WHERE category.plan_id = checked_plan_id
        AND (
          plan.request_envelope->'categoryRequests'->
            category.request_category IS NULL OR
          category.planned_successful_request_count IS DISTINCT FROM
            ((plan.request_envelope->'categoryRequests'->
              category.request_category)->>0)::integer OR
          category.planned_maximum_request_count IS DISTINCT FROM
            ((plan.request_envelope->'categoryRequests'->
              category.request_category)->>1)::integer
        )
    ),
    false
  );
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_v21_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.plan_version = '2.0.0' AND
     OLD.state IS DISTINCT FROM NEW.state AND
     NEW.state IN (
       'awaiting_approval', 'approved', 'executing', 'completed'
     ) THEN
    RAISE EXCEPTION
      'legacy candidate source-snapshot plans retain audit state but cannot gain approval or execution authority';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'awaiting_configuration' AND
     NEW.state = 'awaiting_approval' AND
     NOT oracle_candidate_source_snapshot_has_exact_categories(OLD.plan_id) THEN
    RAISE EXCEPTION
      'candidate source-snapshot approval readiness requires all exact request categories';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'approved' AND NEW.state = 'executing' AND NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_approvals approval
       WHERE approval.plan_id = OLD.plan_id
         AND approval.plan_sha256 = OLD.plan_sha256
         AND approval.approval_version =
           'candidate-source-snapshot-approval-v3'
         AND approval.approved_plan_revision = OLD.revision - 1
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot execution requires exact approval v3';
  END IF;

  IF OLD.plan_version = '2.1.0' AND
     OLD.state = 'executing' AND NEW.state = 'completed' AND NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_verifications verification
       WHERE verification.plan_id = OLD.plan_id
         AND verification.plan_sha256 = OLD.plan_sha256
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot completion requires receipt-bound final verification';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_029_candidate_source_snapshot_v21_state_guard
  BEFORE UPDATE ON oracle_candidate_source_snapshot_demo_plans
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_v21_state();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_accounting()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_request_cost numeric(18, 12);
BEGIN
  expected_request_cost := CASE
    WHEN NEW.free_operation_count = OLD.free_operation_count + 1
      THEN 0::numeric
    ELSE 0.0000045::numeric
  END;
  IF TG_OP = 'DELETE' OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.revision + 1 IS DISTINCT FROM NEW.revision OR
     NEW.request_count IS DISTINCT FROM OLD.request_count + 1 OR
     NEW.request_count > 1100000 OR
     NEW.request_cost_usd IS DISTINCT FROM
       OLD.request_cost_usd + expected_request_cost OR
     ((NEW.class_a_mutation_count - OLD.class_a_mutation_count) +
      (NEW.class_b_read_count - OLD.class_b_read_count) +
      (NEW.names_api_count - OLD.names_api_count) +
      (NEW.public_resolver_count - OLD.public_resolver_count) +
      (NEW.free_operation_count - OLD.free_operation_count)) IS DISTINCT FROM 1 OR
     NEW.class_a_mutation_count < OLD.class_a_mutation_count OR
     NEW.class_b_read_count < OLD.class_b_read_count OR
     NEW.names_api_count < OLD.names_api_count OR
     NEW.public_resolver_count < OLD.public_resolver_count OR
     NEW.free_operation_count < OLD.free_operation_count THEN
    RAISE EXCEPTION 'candidate source-snapshot accounting update is invalid';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  category_row oracle_candidate_source_snapshot_demo_request_categories%ROWTYPE;
  intent_row oracle_candidate_source_snapshot_demo_ipns_intents%ROWTYPE;
  existing_request_count integer;
  existing_class_a integer;
  existing_class_b integer;
  existing_names integer;
  existing_public integer;
  existing_free integer;
  existing_cost numeric(18, 12);
  existing_category_count integer;
  existing_category_cost numeric(18, 12);
  allowed_request_cost numeric(18, 12);
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  IF plan_row.plan_version <> '2.1.0' OR
     NOT (
       plan_row.state = 'executing' OR
       (
         NEW.request_category = 'bucket_names_preflight' AND
         NEW.intent_id IS NULL AND
         plan_row.state IN ('awaiting_configuration', 'awaiting_approval')
       )
     ) OR
     NEW.request_category IS NULL OR NEW.logical_request_id IS NULL OR
     NEW.attempt_sequence IS NULL OR NEW.redirect_sequence IS NULL THEN
    RAISE EXCEPTION
      'candidate source-snapshot categorized request requires the executing v2.1 plan or its bounded intent-free preflight';
  END IF;

  IF NEW.intent_id IS NOT NULL THEN
    SELECT * INTO STRICT intent_row
    FROM oracle_candidate_source_snapshot_demo_ipns_intents
    WHERE intent_id = NEW.intent_id;
    IF intent_row.plan_id IS DISTINCT FROM NEW.plan_id OR
       intent_row.domain IS DISTINCT FROM NEW.domain THEN
      RAISE EXCEPTION
        'candidate source-snapshot remote request intent binding mismatch';
    END IF;
  END IF;

  IF NEW.request_category = 'bucket_names_preflight' AND
     NEW.intent_id IS NOT NULL THEN
    RAISE EXCEPTION
      'candidate source-snapshot bucket and names preflight is intent-free';
  END IF;

  IF NEW.request_category = 'final_credential_free_verification' AND (
       NOT EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_demo_upload_closures closure
         WHERE closure.plan_id = NEW.plan_id
           AND closure.plan_sha256 = plan_row.plan_sha256
       ) OR
       (SELECT count(*)
          FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
          JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state state
            ON state.intent_id = intent.intent_id
          WHERE intent.plan_id = NEW.plan_id
            AND state.state = 'verified') IS DISTINCT FROM 2::bigint OR
       EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_demo_requests request
         WHERE request.plan_id = NEW.plan_id
           AND request.outcome = 'request_started'
           AND request.request_category IS DISTINCT FROM
             'final_credential_free_verification'
       )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot final verification requires closed uploads and both verified IPNS intents';
  END IF;

  IF NEW.request_category <> 'final_credential_free_verification' AND EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.request_category =
           'final_credential_free_verification'
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot final verification is an exclusive terminal request phase';
  END IF;

  IF NEW.request_cost_usd IS DISTINCT FROM (CASE
       WHEN NEW.operation_class = 'free_operation' THEN 0::numeric
       ELSE 0.0000045::numeric
     END) THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote request cost is invalid';
  END IF;

  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT category_row
  FROM oracle_candidate_source_snapshot_demo_request_categories
  WHERE plan_id = NEW.plan_id AND request_category = NEW.request_category
  FOR UPDATE;

  SELECT
    count(*)::integer,
    count(*) FILTER (WHERE operation_class = 'class_a_mutation')::integer,
    count(*) FILTER (WHERE operation_class = 'class_b_read')::integer,
    count(*) FILTER (WHERE operation_class = 'names_api')::integer,
    count(*) FILTER (WHERE operation_class = 'public_resolver')::integer,
    count(*) FILTER (WHERE operation_class = 'free_operation')::integer,
    COALESCE(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO existing_request_count, existing_class_a, existing_class_b,
       existing_names, existing_public, existing_free, existing_cost
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE plan_id = NEW.plan_id;

  SELECT count(*)::integer,
         COALESCE(sum(request_cost_usd), 0)::numeric(18, 12)
  INTO existing_category_count, existing_category_cost
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE plan_id = NEW.plan_id
    AND request_category = NEW.request_category;

  IF accounting_row.request_count IS DISTINCT FROM existing_request_count + 1 OR
     accounting_row.class_a_mutation_count IS DISTINCT FROM (
       existing_class_a +
       CASE WHEN NEW.operation_class = 'class_a_mutation' THEN 1 ELSE 0 END
     ) OR
     accounting_row.class_b_read_count IS DISTINCT FROM (
       existing_class_b +
       CASE WHEN NEW.operation_class = 'class_b_read' THEN 1 ELSE 0 END
     ) OR
     accounting_row.names_api_count IS DISTINCT FROM (
       existing_names +
       CASE WHEN NEW.operation_class = 'names_api' THEN 1 ELSE 0 END
     ) OR
     accounting_row.public_resolver_count IS DISTINCT FROM (
       existing_public +
       CASE WHEN NEW.operation_class = 'public_resolver' THEN 1 ELSE 0 END
     ) OR
     accounting_row.free_operation_count IS DISTINCT FROM (
       existing_free +
       CASE WHEN NEW.operation_class = 'free_operation' THEN 1 ELSE 0 END
     ) OR
     accounting_row.request_cost_usd IS DISTINCT FROM
       existing_cost + NEW.request_cost_usd OR
     category_row.consumed_request_count IS DISTINCT FROM
       existing_category_count + 1 OR
     category_row.request_cost_usd IS DISTINCT FROM
       existing_category_cost + NEW.request_cost_usd THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote request lacks exact global and category accounting admission';
  END IF;

  allowed_request_cost :=
    (plan_row.cost_envelope->'requestUsd'->>'maximumAttempts')::numeric;

  IF accounting_row.request_count > plan_row.maximum_request_count OR
     accounting_row.request_cost_usd > allowed_request_cost OR
     category_row.consumed_request_count >
       category_row.planned_maximum_request_count THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote request exceeds its exact category or plan allowance';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_request()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.operation_class IS DISTINCT FROM NEW.operation_class OR
     OLD.operation_kind IS DISTINCT FROM NEW.operation_kind OR
     OLD.request_category IS DISTINCT FROM NEW.request_category OR
     OLD.logical_request_id IS DISTINCT FROM NEW.logical_request_id OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.redirect_sequence IS DISTINCT FROM NEW.redirect_sequence OR
     OLD.intent_id IS DISTINCT FROM NEW.intent_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.cycle_sequence IS DISTINCT FROM NEW.cycle_sequence OR
     OLD.resolver IS DISTINCT FROM NEW.resolver OR
     OLD.request_cost_usd IS DISTINCT FROM NEW.request_cost_usd OR
     OLD.started_at IS DISTINCT FROM NEW.started_at OR
     OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION
      'candidate source-snapshot request is immutable or terminal';
  END IF;
  RETURN NEW;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD CONSTRAINT oracle_css_request_logical_attempt_unique UNIQUE (
    plan_id, request_category, logical_request_id, attempt_sequence,
    redirect_sequence
  );

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_read_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  object_row oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  expected_payload jsonb;
  expected_receipt_sha256 text;
  expected_receipt_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.request_id;
  SELECT * INTO STRICT object_row
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id
    AND domain = NEW.domain
    AND remote_object_key = NEW.remote_object_key;

  expected_payload := jsonb_build_object(
    'byteRangeEnd', NEW.byte_range_end,
    'byteRangeStart', NEW.byte_range_start,
    'checkKind', NEW.check_kind,
    'domain', NEW.domain,
    'expectedBytes', object_row.expected_bytes,
    'expectedCid', object_row.expected_cid,
    'expectedSha256', object_row.expected_sha256,
    'observedAt', NEW.observed_at_iso,
    'outcome', NEW.outcome,
    'planId', plan_row.plan_id,
    'remoteObjectKey', object_row.remote_object_key,
    'requestId', request_row.request_id,
    'responseBytes', NEW.response_bytes,
    'responseSha256', NEW.response_sha256,
    'schemaVersion', 'candidate-source-snapshot-remote-read-receipt-v1'
  );
  expected_receipt_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_receipt_id := 'snapshotdemoverificationreceipt_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-remote-read-receipt-v1',
      plan_row.plan_id,
      request_row.request_id,
      expected_receipt_sha256
    ])), 'UTF8')), 'hex'), 1, 32);

  IF plan_row.plan_version <> '2.1.0' OR
     plan_row.state <> 'executing' OR
     request_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     request_row.request_category IS DISTINCT FROM
       'final_credential_free_verification' OR
     request_row.operation_class IS DISTINCT FROM 'class_b_read' OR
     request_row.outcome IS DISTINCT FROM (CASE NEW.outcome
       WHEN 'verified' THEN 'succeeded'
       WHEN 'retryable_failure' THEN 'retryable_failure'
       WHEN 'timeout_unknown' THEN 'timeout_unknown'
       WHEN 'terminal_failure' THEN 'terminal_failure'
       ELSE NULL
     END) OR
     request_row.receipt_sha256 IS DISTINCT FROM
       expected_receipt_sha256 OR
     request_row.remote_object_key IS DISTINCT FROM
       NEW.remote_object_key OR
     NEW.expected_cid IS DISTINCT FROM object_row.expected_cid OR
     NEW.expected_sha256 IS DISTINCT FROM object_row.expected_sha256 OR
     NEW.expected_bytes IS DISTINCT FROM object_row.expected_bytes OR
     NEW.receipt_payload IS DISTINCT FROM expected_payload OR
     NEW.receipt_sha256 IS DISTINCT FROM expected_receipt_sha256 OR
     NEW.verification_receipt_id IS DISTINCT FROM expected_receipt_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote read receipt is not exact admitted verification evidence';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_remote_read_receipt_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_demo_remote_read_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_remote_read_receipt();

CREATE TRIGGER oracle_css_remote_read_receipt_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_demo_remote_read_receipts
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

-- Migration 028 guarded the same immutable check shape through the expanded
-- v2.0 request envelope. Preserve every identity check while reading the
-- compact v2.1 final-verification category.
CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_remote_check_sha256_v1(
  checked_payload jsonb
)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
  canonical_payload text;
BEGIN
  IF NOT oracle_jsonb_exact_keys(checked_payload, ARRAY[
    'checkedAt', 'checkKind', 'evidenceSha256', 'expectedBytes',
    'expectedCid', 'expectedSha256', 'metrics', 'observedBytes',
    'observedCid', 'observedSha256', 'planId', 'planSha256',
    'schemaVersion'
  ]) THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote check payload keys are invalid';
  END IF;

  -- src/lib/canonical-json.ts sorts this fixed camel-case schema with
  -- JavaScript localeCompare. PostgreSQL's C collation reverses only the
  -- checkedAt/checkKind pair, so serialize the closed payload explicitly in
  -- the application order instead of weakening the independent hash check.
  canonical_payload :=
    '{"checkedAt":' || oracle_canonical_jsonb(checked_payload->'checkedAt') ||
    ',"checkKind":' || oracle_canonical_jsonb(checked_payload->'checkKind') ||
    ',"evidenceSha256":' ||
      oracle_canonical_jsonb(checked_payload->'evidenceSha256') ||
    ',"expectedBytes":' ||
      oracle_canonical_jsonb(checked_payload->'expectedBytes') ||
    ',"expectedCid":' ||
      oracle_canonical_jsonb(checked_payload->'expectedCid') ||
    ',"expectedSha256":' ||
      oracle_canonical_jsonb(checked_payload->'expectedSha256') ||
    ',"metrics":' || oracle_canonical_jsonb(checked_payload->'metrics') ||
    ',"observedBytes":' ||
      oracle_canonical_jsonb(checked_payload->'observedBytes') ||
    ',"observedCid":' ||
      oracle_canonical_jsonb(checked_payload->'observedCid') ||
    ',"observedSha256":' ||
      oracle_canonical_jsonb(checked_payload->'observedSha256') ||
    ',"planId":' || oracle_canonical_jsonb(checked_payload->'planId') ||
    ',"planSha256":' ||
      oracle_canonical_jsonb(checked_payload->'planSha256') ||
    ',"schemaVersion":' ||
      oracle_canonical_jsonb(checked_payload->'schemaVersion') || '}';
  RETURN encode(sha256(convert_to(canonical_payload, 'UTF8')), 'hex');
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_check()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  object_row oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  expected_cid text;
  expected_sha256 text;
  expected_bytes bigint;
  expected_metrics jsonb;
  expected_payload jsonb;
  expected_check_sha256 text;
  expected_check_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  IF NEW.check_kind = 'plan_artifact' THEN
    expected_cid := plan_row.plan_artifact_cid;
    expected_sha256 := plan_row.plan_artifact_sha256;
    expected_bytes := plan_row.plan_artifact_bytes;
    expected_metrics := '{}'::jsonb;
  ELSIF NEW.check_kind = 'manifest' THEN
    expected_cid := plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'expectedCid';
    expected_sha256 := plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'sha256';
    expected_bytes := (plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->>'byteSize')::bigint;
    expected_metrics := '{}'::jsonb;
  ELSIF NEW.check_kind = 'inventory' THEN
    expected_cid := plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'expectedCid';
    expected_sha256 := plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'sha256';
    expected_bytes := (plan_row.plan_payload->'controlArtifacts'->
      'objectInventory'->'indexArtifact'->>'byteSize')::bigint;
    expected_metrics := jsonb_build_object(
      'entryCount', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'entryCount',
      'integrityRootSha256', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'integrityRootSha256',
      'shardCount', plan_row.plan_payload->'controlArtifacts'->
        'objectInventory'->'shardCount'
    );
  ELSE
    expected_cid := CASE NEW.check_kind
      WHEN 'query_table' THEN
        plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
      ELSE plan_row.plan_payload->'targets'->'openData'->>'targetCid'
    END;
    SELECT * INTO STRICT object_row
    FROM oracle_candidate_source_snapshot_demo_objects object
    WHERE object.plan_id = NEW.plan_id
      AND object.expected_cid = CASE NEW.check_kind
        WHEN 'query_table' THEN
          plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
        ELSE plan_row.plan_payload->'targets'->'openData'->>'targetCid'
      END
    ORDER BY remote_object_key
    LIMIT 1;
    expected_sha256 := object_row.expected_sha256;
    expected_bytes := object_row.expected_bytes;
    expected_metrics := CASE NEW.check_kind
      WHEN 'open_data_graph' THEN jsonb_build_object(
        'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties',
        'traversalValid', true
      )
      WHEN 'query_table' THEN jsonb_build_object(
        'distinctPropertyIdCount',
          plan_row.plan_payload->'coverage'->'activeProperties',
        'nullPropertyIdCount', 0,
        'propertyCidCorrespondence', true,
        'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties'
      )
      WHEN 'coverage' THEN plan_row.plan_payload->'coverage'
      WHEN 'fixture_exclusion' THEN jsonb_build_object('fixtureMatchCount', 0)
      ELSE NULL
    END;
  END IF;

  expected_payload := jsonb_build_object(
    'checkKind', NEW.check_kind,
    'checkedAt', NEW.checked_at_iso,
    'evidenceSha256', NEW.evidence_sha256,
    'expectedBytes', expected_bytes,
    'expectedCid', expected_cid,
    'expectedSha256', expected_sha256,
    'metrics', expected_metrics,
    'observedBytes', expected_bytes,
    'observedCid', expected_cid,
    'observedSha256', expected_sha256,
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'schemaVersion', 'candidate-source-snapshot-remote-check-v1'
  );
  expected_check_sha256 :=
    oracle_candidate_source_snapshot_remote_check_sha256_v1(expected_payload);
  expected_check_id := 'snapshotdemoremotecheck_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-remote-check-v1',
      plan_row.plan_id,
      NEW.check_kind,
      expected_check_sha256
    ])), 'UTF8'
  )), 'hex'), 1, 32);

  IF coalesce((plan_row.plan_payload->'requestEnvelope'->
       'categoryRequests'->'final_credential_free_verification'->>0)::integer,
       0) < 7 OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     (SELECT count(*)
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state state
          ON state.intent_id = intent.intent_id
        WHERE intent.plan_id = NEW.plan_id
          AND state.state = 'verified') IS DISTINCT FROM 2::bigint OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
         AND closure.plan_sha256 = NEW.plan_sha256
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.outcome = 'request_started'
     ) OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.expected_cid IS DISTINCT FROM expected_cid OR
     NEW.observed_cid IS DISTINCT FROM expected_cid OR
     NEW.expected_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.observed_sha256 IS DISTINCT FROM expected_sha256 OR
     NEW.expected_bytes IS DISTINCT FROM expected_bytes OR
     NEW.observed_bytes IS DISTINCT FROM expected_bytes OR
     NEW.metrics IS DISTINCT FROM expected_metrics OR
     NEW.check_payload IS DISTINCT FROM expected_payload OR
     NEW.check_sha256 IS DISTINCT FROM expected_check_sha256 OR
     NEW.check_id IS DISTINCT FROM expected_check_id THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote check is not exact durable evidence';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_check_receipts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  expected_receipt_count integer;
  expected_receipt_set_sha256 text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  SELECT count(*)::integer,
         encode(sha256(convert_to(oracle_canonical_jsonb(
           coalesce(jsonb_agg(jsonb_build_object(
             'outcome', receipt.outcome,
             'receiptId', receipt.verification_receipt_id,
             'receiptSha256', receipt.receipt_sha256,
             'requestId', receipt.request_id
           ) ORDER BY receipt.request_id), '[]'::jsonb)
         ), 'UTF8')), 'hex')
  INTO expected_receipt_count, expected_receipt_set_sha256
  FROM oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
  WHERE receipt.plan_id = NEW.plan_id
    AND receipt.check_kind = NEW.check_kind;

  IF plan_row.plan_version <> '2.1.0' OR
     expected_receipt_count < 1 OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
       WHERE receipt.plan_id = NEW.plan_id
         AND receipt.check_kind = NEW.check_kind
         AND receipt.outcome = 'verified'
     ) OR
     NEW.verification_receipt_count IS DISTINCT FROM
       expected_receipt_count OR
     NEW.verification_receipt_set_sha256 IS DISTINCT FROM
       expected_receipt_set_sha256 OR
     NEW.evidence_sha256 IS DISTINCT FROM expected_receipt_set_sha256 THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote check requires its exact verified read receipt set';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_029_candidate_source_snapshot_remote_check_receipts
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_remote_checks
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_remote_check_receipts();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_verification_receipts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  expected_logical_requests integer;
  terminal_request_count integer;
  verified_logical_request_count integer;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  expected_logical_requests := (
    plan_row.request_envelope->'finalVerification'->>'logicalRequests'
  )::integer;

  SELECT count(*)::integer
  INTO terminal_request_count
  FROM oracle_candidate_source_snapshot_demo_requests request
  WHERE request.plan_id = NEW.plan_id
    AND request.request_category =
      'final_credential_free_verification'
    AND request.outcome <> 'request_started';

  SELECT count(DISTINCT request.logical_request_id)::integer
  INTO verified_logical_request_count
  FROM oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
  JOIN oracle_candidate_source_snapshot_demo_requests request
    ON request.request_id = receipt.request_id
  WHERE receipt.plan_id = NEW.plan_id
    AND receipt.outcome = 'verified';

  IF plan_row.plan_version <> '2.1.0' OR
     terminal_request_count < expected_logical_requests OR
     verified_logical_request_count IS DISTINCT FROM
       expected_logical_requests OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
       JOIN oracle_candidate_source_snapshot_demo_requests request
         ON request.request_id = receipt.request_id
       WHERE receipt.plan_id = NEW.plan_id
         AND receipt.outcome = 'verified'
       GROUP BY request.logical_request_id
       HAVING count(*) <> 1
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.request_category =
           'final_credential_free_verification'
         AND NOT EXISTS (
           SELECT 1
           FROM oracle_candidate_source_snapshot_demo_remote_read_receipts receipt
           WHERE receipt.request_id = request.request_id
         )
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_remote_checks check_row
       WHERE check_row.plan_id = NEW.plan_id
         AND (
           check_row.verification_receipt_count IS NULL OR
           check_row.verification_receipt_set_sha256 IS NULL
         )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot final verification is not closed over admitted remote read receipts';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_029_candidate_source_snapshot_remote_verification_receipts
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_remote_verifications
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_candidate_source_snapshot_remote_verification_receipts();

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_remote_verification()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  query_object oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  expected_checks jsonb;
  expected_check_set_sha256 text;
  expected_payload jsonb;
  expected_verification_sha256 text;
  expected_verification_id text;
  check_count bigint;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT query_object
  FROM oracle_candidate_source_snapshot_demo_objects object
  WHERE object.plan_id = NEW.plan_id
    AND object.expected_cid =
      plan_row.plan_payload->'targets'->'queryTable'->>'targetCid'
  ORDER BY object.remote_object_key
  LIMIT 1;
  SELECT count(*), jsonb_agg(jsonb_build_object(
           'checkId', check_row.check_id,
           'checkKind', check_row.check_kind,
           'checkSha256', check_row.check_sha256,
           'evidenceSha256', check_row.evidence_sha256
         ) ORDER BY check_row.check_kind)
  INTO check_count, expected_checks
  FROM oracle_candidate_source_snapshot_demo_remote_checks check_row
  WHERE check_row.plan_id = NEW.plan_id;
  expected_check_set_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_checks), 'UTF8'
  )), 'hex');
  expected_payload := jsonb_build_object(
    'approvalId', NEW.approval_id,
    'checkSetSha256', expected_check_set_sha256,
    'checks', expected_checks,
    'distinctPropertyIdCount',
      plan_row.plan_payload->'coverage'->'activeProperties',
    'fixtureMatchCount', 0,
    'graphTraversalValid', true,
    'inventoryRootCid', plan_row.inventory_root_cid,
    'inventoryRootSha256', plan_row.inventory_root_sha256,
    'manifestCid', plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->'expectedCid',
    'manifestSha256', plan_row.plan_payload->'controlArtifacts'->
      'manifestIndex'->'sha256',
    'nullPropertyIdCount', 0,
    'openDataRootCid', plan_row.plan_payload->'targets'->
      'openData'->'targetCid',
    'planId', plan_row.plan_id,
    'planSha256', plan_row.plan_sha256,
    'propertyCidCorrespondence', true,
    'propertyCount', plan_row.plan_payload->'coverage'->'activeProperties',
    'queryTableBytes', query_object.expected_bytes,
    'queryTableRootCid', plan_row.plan_payload->'targets'->
      'queryTable'->'targetCid',
    'queryTableSha256', query_object.expected_sha256,
    'schemaVersion', 'candidate-source-snapshot-remote-verification-v2',
    'uploadClosureId', NEW.upload_closure_id,
    'verifiedAt', to_char(NEW.verified_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
  );
  expected_verification_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_payload), 'UTF8'
  )), 'hex');
  expected_verification_id := 'snapshotdemoremoteverification_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-remote-verification-v2',
      plan_row.plan_id,
      expected_verification_sha256
    ])), 'UTF8')), 'hex'), 1, 32);
  IF coalesce((plan_row.plan_payload->'requestEnvelope'->
       'categoryRequests'->'final_credential_free_verification'->>0)::integer,
       0) < 7 OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     check_count IS DISTINCT FROM 7::bigint OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.open_data_root_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'targetCid' OR
     NEW.query_table_root_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'queryTable'->>'targetCid' OR
     NEW.manifest_cid IS DISTINCT FROM
       plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'expectedCid' OR
     NEW.manifest_sha256 IS DISTINCT FROM
       plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'sha256' OR
     NEW.inventory_root_cid IS DISTINCT FROM plan_row.inventory_root_cid OR
     NEW.inventory_root_sha256 IS DISTINCT FROM plan_row.inventory_root_sha256 OR
     NEW.query_table_bytes IS DISTINCT FROM query_object.expected_bytes OR
     NEW.query_table_sha256 IS DISTINCT FROM query_object.expected_sha256 OR
     NEW.property_count IS DISTINCT FROM
       (plan_row.plan_payload->'coverage'->>'activeProperties')::integer OR
     NEW.distinct_property_id_count IS DISTINCT FROM NEW.property_count OR
     NEW.null_property_id_count IS DISTINCT FROM 0 OR
     NEW.property_cid_correspondence IS DISTINCT FROM true OR
     NEW.graph_traversal_valid IS DISTINCT FROM true OR
     NEW.fixture_match_count IS DISTINCT FROM 0 OR
     NEW.check_set_sha256 IS DISTINCT FROM expected_check_set_sha256 OR
     NEW.verification_payload IS DISTINCT FROM expected_payload OR
     NEW.verification_sha256 IS DISTINCT FROM expected_verification_sha256 OR
     NEW.verification_id IS DISTINCT FROM expected_verification_id OR
     (SELECT count(*)
        FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
        JOIN oracle_candidate_source_snapshot_demo_ipns_intent_state intent_state
          ON intent_state.intent_id = intent.intent_id
        WHERE intent.plan_id = NEW.plan_id
          AND intent_state.state = 'verified') IS DISTINCT FROM 2::bigint OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_ipns_attempts attempt
       WHERE attempt.plan_id = NEW.plan_id
         AND attempt.outcome = 'request_started'
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests request
       WHERE request.plan_id = NEW.plan_id
         AND request.outcome = 'request_started'
     ) OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.closure_id = NEW.upload_closure_id
         AND closure.plan_id = NEW.plan_id
         AND closure.plan_sha256 = NEW.plan_sha256
         AND closure.approval_id = NEW.approval_id
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot remote verification is incomplete or mismatched';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_expanded_categories_v1(
  checked_request_envelope jsonb
)
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT jsonb_build_array(
    jsonb_build_object(
      'category', 'upload_provider_cid',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'upload_provider_cid'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'upload_provider_cid'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'ambiguous_upload_inspection',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'ambiguous_upload_inspection'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'ambiguous_upload_inspection'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'bucket_names_preflight',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'bucket_names_preflight'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'bucket_names_preflight'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'names_mutation',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'names_mutation'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'names_mutation'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'control_public_observation',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'control_public_observation'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'control_public_observation'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'recovery',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->'recovery'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->'recovery'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'rollback',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->'rollback'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->'rollback'->>1)::integer
    ),
    jsonb_build_object(
      'category', 'final_credential_free_verification',
      'successfulRequests',
        (checked_request_envelope->'categoryRequests'->
          'final_credential_free_verification'->>0)::integer,
      'maximumRequests',
        (checked_request_envelope->'categoryRequests'->
          'final_credential_free_verification'->>1)::integer
    )
  );
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_authorization_binding_v2(
  checked_plan_id text,
  checked_implementation_commit_sha text
)
RETURNS jsonb LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = checked_plan_id;
  RETURN jsonb_build_object(
    'classification', plan_row.plan_payload->'classification',
    'execution', jsonb_build_object(
      'absoluteRequestCeiling',
        plan_row.plan_payload->'requestEnvelope'->'maximumTotalRequests',
      'ambiguousInspectionAllowance',
        to_jsonb((plan_row.plan_payload->'requestEnvelope'->
          'categoryRequests'->'ambiguous_upload_inspection'->>1)::integer),
      'categoryRequests',
        oracle_candidate_source_snapshot_expanded_categories_v1(
          plan_row.plan_payload->'requestEnvelope'
        ),
      'cutoverOrder',
        plan_row.plan_payload->'protectedSampleRollback'->'cutoverOrder',
      'finalVerificationLogicalRequests',
        plan_row.plan_payload->'requestEnvelope'->'finalVerification'->
          'logicalRequests',
      'finalVerificationRequiredMaximumRequests',
        plan_row.plan_payload->'requestEnvelope'->'finalVerification'->
          'deterministicRequiredMaximumRequests',
      'finalVerificationProtectedHeadroomRequests',
        plan_row.plan_payload->'requestEnvelope'->'finalVerification'->
          'protectedHeadroomRequests',
      'maximumEstimatedCostUsd',
        plan_row.plan_payload->'costEnvelope'->'maximumTotalUsd',
      'maximumAttemptCount',
        to_jsonb(
          (plan_row.plan_payload->'requestEnvelope'->>
            'maximumTotalRequests')::integer -
          (plan_row.plan_payload->'requestEnvelope'->'categoryRequests'->
            'ambiguous_upload_inspection'->>1)::integer -
          (plan_row.plan_payload->'requestEnvelope'->'categoryRequests'->
            'recovery'->>1)::integer -
          (plan_row.plan_payload->'requestEnvelope'->'categoryRequests'->
            'rollback'->>1)::integer
        ),
      'maximumAttemptsPerObject',
        to_jsonb((plan_row.plan_payload->'limits'->>'maxRetries')::integer + 1),
      'maximumConcurrency',
        plan_row.plan_payload->'limits'->'maxConcurrency',
      'maximumRetries', plan_row.plan_payload->'limits'->'maxRetries',
      'recoveryAllowance',
        to_jsonb(
          (plan_row.plan_payload->'requestEnvelope'->'categoryRequests'->
            'recovery'->>1)::integer +
          (plan_row.plan_payload->'requestEnvelope'->'categoryRequests'->
            'rollback'->>1)::integer
        ),
      'requestEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.plan_payload->'requestEnvelope'),
        'UTF8'
      )), 'hex'),
      'requestTimeoutMs',
        plan_row.plan_payload->'limits'->'requestTimeoutMs',
      'spendingCeilingUsd',
        plan_row.plan_payload->'limits'->'maxBudgetUsd',
      'successfulRequestCount',
        plan_row.plan_payload->'requestEnvelope'->'successfulTotalRequests'
    ),
    'inventory', jsonb_build_object(
      'admissionReservedBytes', plan_row.plan_payload->'inventory'->'totalBytes',
      'costEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.plan_payload->'costEnvelope'), 'UTF8'
      )), 'hex'),
      'exactObjectCount', to_jsonb(plan_row.exact_upload_object_count),
      'exactTotalBytes', to_jsonb(plan_row.exact_upload_bytes),
      'fullInventorySha256',
        plan_row.plan_payload->'controlArtifacts'->'fullInventoryRootSha256',
      'inventoryCid', plan_row.plan_payload->'inventory'->'inventoryRootCid',
      'manifestCid',
        plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->'expectedCid',
      'manifestSha256',
        plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->'sha256',
      'maximumObjectCount', plan_row.plan_payload->'limits'->'maxObjects',
      'maximumTotalBytes', plan_row.plan_payload->'limits'->'maxTotalBytes'
    ),
    'implementationCommitSha', checked_implementation_commit_sha,
    'plan', jsonb_build_object(
      'artifactByteSize', to_jsonb(plan_row.plan_artifact_bytes),
      'artifactCid', to_jsonb(plan_row.plan_artifact_cid),
      'artifactRemoteObjectKey',
        to_jsonb(plan_row.plan_artifact_remote_object_key),
      'artifactSha256', to_jsonb(plan_row.plan_artifact_sha256),
      'planId', to_jsonb(plan_row.plan_id),
      'planLogicalSha256', to_jsonb(plan_row.plan_sha256),
      'disclosureSha256', encode(sha256(convert_to(
        plan_row.plan_payload->>'disclaimer', 'UTF8'
      )), 'hex')
    ),
    'schemaVersion', 'candidate-source-snapshot-authorization-binding-v2',
    'targets', jsonb_build_object(
      'openData', jsonb_build_object(
        'bucket', plan_row.plan_payload->'targets'->'openData'->'bucket',
        'immutablePrefix',
          plan_row.plan_payload->'targets'->'openData'->'immutablePrefix',
        'ipnsLabel',
          plan_row.plan_payload->'targets'->'openData'->'ipnsLabel',
        'ipnsNetworkKey',
          plan_row.plan_payload->'targets'->'openData'->'ipnsNetworkKey',
        'priorCid', plan_row.plan_payload->'targets'->'openData'->'priorCid',
        'targetCid', plan_row.plan_payload->'targets'->'openData'->'targetCid'
      ),
      'queryTable', jsonb_build_object(
        'bucket', plan_row.plan_payload->'targets'->'queryTable'->'bucket',
        'immutablePrefix',
          plan_row.plan_payload->'targets'->'queryTable'->'immutablePrefix',
        'ipnsLabel',
          plan_row.plan_payload->'targets'->'queryTable'->'ipnsLabel',
        'ipnsNetworkKey',
          plan_row.plan_payload->'targets'->'queryTable'->'ipnsNetworkKey',
        'priorCid', plan_row.plan_payload->'targets'->'queryTable'->'priorCid',
        'targetCid', plan_row.plan_payload->'targets'->'queryTable'->'targetCid'
      )
    )
  );
END;
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_authorization_statement_v2(
  checked_plan_id text,
  checked_implementation_commit_sha text
)
RETURNS text LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  categories jsonb;
  final_verification jsonb;
  category_statement text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = checked_plan_id;
  categories := oracle_candidate_source_snapshot_expanded_categories_v1(
    plan_row.request_envelope
  );
  final_verification := plan_row.request_envelope->'finalVerification';
  SELECT string_agg(format(
           '%s:%s/%s',
           category->>'category',
           category->>'successfulRequests',
           category->>'maximumRequests'
         ), ',' ORDER BY ordinal)
  INTO category_statement
  FROM jsonb_array_elements(categories) WITH ORDINALITY
    AS entry(category, ordinal);
  RETURN format(
    'I confirm the candidate-controlled Filebase account is Pro or better and supports at least %s pinned objects, %s bytes, two distinct buckets and two distinct IPNS names, and I approve only candidate_owned_source_snapshot_demo plan %s with logical SHA-256 %s, implementation commit SHA %s, plan artifact SHA-256 %s, CID %s and %s bytes, disclosure SHA-256 %s, request-envelope SHA-256 %s, cost-envelope SHA-256 %s, exactly %s objects and %s upload bytes with %s admission-reserved bytes, open-data bucket and label %s under immutable prefix %s and network key %s from prior %s to target %s, query-table bucket and label %s under immutable prefix %s and network key %s from prior %s to target %s, manifest CID %s and SHA-256 %s, inventory CID %s and full-inventory SHA-256 %s, request categories successful/maximum %s, successful request count %s, maximum-attempt count %s, ambiguous-inspection allowance %s, recovery allowance %s, final credential-free verification %s logical requests/%s required maximum/%s protected headroom, absolute request ceiling %s, maximum estimated cost USD %s, two retries, three total object attempts, concurrency %s, %s ms timeout and USD %s hard spending ceiling for uploading only these immutable objects and then updating only these two candidate IPNS identities in durable open-data-first/query-table-second order after exact provider-CID verification; this authorization is candidate-only and noncanonical and does not authorize or represent Elephant-owned, owner-controlled, owner/canonical, authoritative-complete, independently Pasco-certified, Accela/BBB, production-database, Vercel-deployment or any other publication authority.',
    plan_row.plan_payload->'limits'->>'maxObjects',
    plan_row.plan_payload->'limits'->>'maxTotalBytes',
    plan_row.plan_id,
    plan_row.plan_sha256,
    checked_implementation_commit_sha,
    plan_row.plan_artifact_sha256,
    plan_row.plan_artifact_cid,
    plan_row.plan_artifact_bytes,
    encode(sha256(convert_to(
      plan_row.plan_payload->>'disclaimer', 'UTF8'
    )), 'hex'),
    encode(sha256(convert_to(
      oracle_canonical_jsonb(plan_row.request_envelope), 'UTF8'
    )), 'hex'),
    encode(sha256(convert_to(
      oracle_canonical_jsonb(plan_row.cost_envelope), 'UTF8'
    )), 'hex'),
    plan_row.exact_upload_object_count,
    plan_row.exact_upload_bytes,
    plan_row.plan_payload->'inventory'->>'totalBytes',
    plan_row.plan_payload->'targets'->'openData'->>'bucket',
    plan_row.plan_payload->'targets'->'openData'->>'immutablePrefix',
    plan_row.plan_payload->'targets'->'openData'->>'ipnsNetworkKey',
    plan_row.plan_payload->'targets'->'openData'->>'priorCid',
    plan_row.plan_payload->'targets'->'openData'->>'targetCid',
    plan_row.plan_payload->'targets'->'queryTable'->>'bucket',
    plan_row.plan_payload->'targets'->'queryTable'->>'immutablePrefix',
    plan_row.plan_payload->'targets'->'queryTable'->>'ipnsNetworkKey',
    plan_row.plan_payload->'targets'->'queryTable'->>'priorCid',
    plan_row.plan_payload->'targets'->'queryTable'->>'targetCid',
    plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'expectedCid',
    plan_row.plan_payload->'controlArtifacts'->'manifestIndex'->>'sha256',
    plan_row.plan_payload->'inventory'->>'inventoryRootCid',
    plan_row.plan_payload->'controlArtifacts'->>'fullInventoryRootSha256',
    category_statement,
    plan_row.request_envelope->>'successfulTotalRequests',
    (plan_row.request_envelope->>'maximumTotalRequests')::integer -
      (plan_row.request_envelope->'categoryRequests'->
        'ambiguous_upload_inspection'->>1)::integer -
      (plan_row.request_envelope->'categoryRequests'->'recovery'->>1)::integer -
      (plan_row.request_envelope->'categoryRequests'->'rollback'->>1)::integer,
    plan_row.request_envelope->'categoryRequests'->
      'ambiguous_upload_inspection'->>1,
    (plan_row.request_envelope->'categoryRequests'->'recovery'->>1)::integer +
      (plan_row.request_envelope->'categoryRequests'->'rollback'->>1)::integer,
    final_verification->>'logicalRequests',
    final_verification->>'deterministicRequiredMaximumRequests',
    final_verification->>'protectedHeadroomRequests',
    plan_row.request_envelope->>'maximumTotalRequests',
    plan_row.cost_envelope->>'maximumTotalUsd',
    plan_row.plan_payload->'limits'->>'maxConcurrency',
    plan_row.plan_payload->'limits'->>'requestTimeoutMs',
    plan_row.plan_payload->'limits'->>'maxBudgetUsd'
  );
END;
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_derivation_is_approval_ready(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  WITH derived AS (
    SELECT plan_id, plan_sha256, plan_payload
    FROM oracle_candidate_source_snapshot_demo_plans
    WHERE plan_id = checked_plan_id AND plan_version = '2.1.0'
  ), compatible_predecessor AS (
    SELECT predecessor.plan_id, predecessor.plan_sha256
    FROM oracle_candidate_source_snapshot_demo_plans predecessor
    CROSS JOIN derived
    WHERE predecessor.plan_version = '2.0.0'
      AND oracle_candidate_source_snapshot_shared_binding_v1(
            predecessor.plan_payload
          ) = oracle_candidate_source_snapshot_shared_binding_v1(
            derived.plan_payload
          )
  )
  SELECT COALESCE(
    (SELECT count(*) FROM compatible_predecessor) = 0 OR
    (
      (SELECT count(*) FROM compatible_predecessor) = 1 AND
      EXISTS (
        SELECT 1
        FROM compatible_predecessor predecessor
        CROSS JOIN derived
        JOIN oracle_candidate_source_snapshot_demo_plan_derivations derivation
          ON derivation.predecessor_plan_id = predecessor.plan_id
         AND derivation.predecessor_plan_sha256 = predecessor.plan_sha256
         AND derivation.derived_plan_id = derived.plan_id
         AND derivation.derived_plan_sha256 = derived.plan_sha256
      )
    ),
    false
  );
$$;

CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_preflight_is_approval_ready(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT COALESCE(
    EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_request_categories category
      WHERE category.plan_id = checked_plan_id
        AND category.request_category = 'bucket_names_preflight'
        AND category.planned_successful_request_count = 8
        AND category.consumed_request_count BETWEEN 8 AND 48
        AND category.consumed_request_count = (
          SELECT count(*)
          FROM oracle_candidate_source_snapshot_demo_requests request
          WHERE request.plan_id = checked_plan_id
            AND request.request_category = 'bucket_names_preflight'
        )
    )
    AND (SELECT count(DISTINCT request.logical_request_id)
           FROM oracle_candidate_source_snapshot_demo_requests request
           WHERE request.plan_id = checked_plan_id
             AND request.request_category = 'bucket_names_preflight') = 8
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_requests request
      WHERE request.plan_id = checked_plan_id
        AND request.request_category = 'bucket_names_preflight'
        AND (
          request.intent_id IS NOT NULL OR
          request.outcome NOT IN (
            'succeeded', 'retryable_failure', 'timeout_unknown'
          ) OR
          request.receipt_sha256 IS NULL OR
          NOT (
            (request.domain = 'open_data' AND
             request.operation_kind = 'bucket_head' AND
             request.resolver IS NULL) OR
            (request.domain = 'open_data' AND
             request.operation_kind = 'names_read' AND
             request.resolver = 'filebase_control') OR
            (request.domain = 'open_data' AND
             request.operation_kind = 'public_resolve' AND
             request.resolver = 'filebase_gateway') OR
            (request.domain = 'open_data' AND
             request.operation_kind = 'public_resolve' AND
             request.resolver = 'delegated_ipfs') OR
            (request.domain = 'query_table' AND
             request.operation_kind = 'bucket_head' AND
             request.resolver IS NULL) OR
            (request.domain = 'query_table' AND
             request.operation_kind = 'names_read' AND
             request.resolver = 'filebase_control') OR
            (request.domain = 'query_table' AND
             request.operation_kind = 'public_resolve' AND
             request.resolver = 'filebase_gateway') OR
            (request.domain = 'query_table' AND
             request.operation_kind = 'public_resolve' AND
             request.resolver = 'delegated_ipfs')
          )
        )
    )
    AND NOT EXISTS (
      SELECT request.domain, request.operation_kind, request.resolver
      FROM oracle_candidate_source_snapshot_demo_requests request
      WHERE request.plan_id = checked_plan_id
        AND request.request_category = 'bucket_names_preflight'
      GROUP BY request.domain, request.operation_kind, request.resolver
      HAVING count(DISTINCT request.logical_request_id) <> 1 OR
             count(*) FILTER (WHERE request.outcome = 'succeeded') <> 1
    )
    AND (SELECT count(*)
           FROM (
             SELECT request.domain, request.operation_kind, request.resolver
             FROM oracle_candidate_source_snapshot_demo_requests request
             WHERE request.plan_id = checked_plan_id
               AND request.request_category = 'bucket_names_preflight'
             GROUP BY request.domain, request.operation_kind, request.resolver
           ) exact_preflight_key) = 8
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_requests first_request
      JOIN oracle_candidate_source_snapshot_demo_requests second_request
        ON second_request.plan_id = first_request.plan_id
       AND second_request.request_category = first_request.request_category
       AND second_request.logical_request_id = first_request.logical_request_id
       AND (
         second_request.domain,
         second_request.operation_kind,
         second_request.resolver
       ) IS DISTINCT FROM (
         first_request.domain,
         first_request.operation_kind,
         first_request.resolver
       )
      WHERE first_request.plan_id = checked_plan_id
        AND first_request.request_category = 'bucket_names_preflight'
    ),
    false
  );
$$;

CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_approval_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  expected_binding jsonb;
  expected_binding_sha256 text;
  expected_statement text;
  expected_statement_sha256 text;
  expected_approval_sha256 text;
  expected_approval_id text;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  expected_binding :=
    oracle_candidate_source_snapshot_authorization_binding_v2(
      NEW.plan_id, NEW.implementation_commit_sha
    );
  expected_binding_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_binding), 'UTF8'
  )), 'hex');
  expected_statement :=
    oracle_candidate_source_snapshot_authorization_statement_v2(
      NEW.plan_id, NEW.implementation_commit_sha
    );
  expected_statement_sha256 := encode(sha256(convert_to(
    expected_statement, 'UTF8'
  )), 'hex');
  expected_approval_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'approvalVersion', 'candidate-source-snapshot-approval-v3',
      'approvedAt', NEW.approved_at_iso,
      'approverReference', NEW.approver_reference,
      'authorizationBinding', expected_binding,
      'authorizationBindingSha256', expected_binding_sha256,
      'authorizationStatement', expected_statement,
      'authorizationStatementSha256', expected_statement_sha256
    )), 'UTF8'
  )), 'hex');
  expected_approval_id := 'snapshotdemoapproval_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-approval-v3',
      plan_row.plan_id,
      expected_approval_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF plan_row.plan_version <> '2.1.0' OR
     plan_row.state <> 'awaiting_approval' OR
     NOT oracle_candidate_source_snapshot_derivation_is_approval_ready(
       plan_row.plan_id
     ) OR
     NOT oracle_candidate_source_snapshot_preflight_is_approval_ready(
       plan_row.plan_id
     ) OR
     plan_row.plan_payload->'limits'->>'maxRetries' IS DISTINCT FROM '2' OR
     (plan_row.plan_payload->'limits'->>'maxRetries')::integer + 1 <> 3 OR
     plan_row.plan_payload->'targets'->'openData'->>'bucket' IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'ipnsLabel' OR
     plan_row.plan_payload->'targets'->'queryTable'->>'bucket' IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'queryTable'->>'ipnsLabel' OR
     NOT oracle_candidate_source_snapshot_v21_categories_valid(
       plan_row.plan_payload
     ) OR
     NOT oracle_candidate_source_snapshot_has_exact_categories(
       plan_row.plan_id
     ) OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_plan_derivations derivation
       WHERE derivation.predecessor_plan_id = plan_row.plan_id
     ) OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.plan_artifact_sha256 IS DISTINCT FROM plan_row.plan_artifact_sha256 OR
     NEW.plan_artifact_cid IS DISTINCT FROM plan_row.plan_artifact_cid OR
     NEW.plan_artifact_remote_object_key IS DISTINCT FROM
       plan_row.plan_artifact_remote_object_key OR
     NEW.plan_artifact_bytes IS DISTINCT FROM plan_row.plan_artifact_bytes OR
     NEW.approved_plan_revision IS DISTINCT FROM plan_row.revision OR
     NEW.approval_version IS DISTINCT FROM
       'candidate-source-snapshot-approval-v3' OR
     NEW.implementation_commit_sha IS NULL OR
     NEW.implementation_commit_sha !~ '^[a-f0-9]{40}$' OR
     NEW.approved_at_iso::timestamptz IS DISTINCT FROM NEW.approved_at OR
     NEW.authorization_binding IS DISTINCT FROM expected_binding OR
     NEW.authorization_binding_sha256 IS DISTINCT FROM
       expected_binding_sha256 OR
     NEW.authorization_statement IS DISTINCT FROM expected_statement OR
     NEW.authorization_statement_sha256 IS DISTINCT FROM
       expected_statement_sha256 OR
     NEW.approval_sha256 IS DISTINCT FROM expected_approval_sha256 OR
     NEW.approval_id IS DISTINCT FROM expected_approval_id OR
     NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_capacity_confirmations confirmation
       WHERE confirmation.plan_id = plan_row.plan_id
         AND confirmation.plan_sha256 = plan_row.plan_sha256
         AND confirmation.plan_artifact_sha256 =
           plan_row.plan_artifact_sha256
         AND confirmation.plan_artifact_cid = plan_row.plan_artifact_cid
         AND confirmation.plan_artifact_remote_object_key =
           plan_row.plan_artifact_remote_object_key
         AND confirmation.plan_artifact_bytes = plan_row.plan_artifact_bytes
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot approval requires the exact v2.1 category authorization binding and statement';
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON TABLE oracle_candidate_source_snapshot_demo_plan_derivations IS
  'Immutable effect-free supersession from one legacy v2.0 request envelope to one exact categorized v2.1 plan; insertion terminalizes only the predecessor plan.';
COMMENT ON TABLE oracle_candidate_source_snapshot_demo_request_categories IS
  'Plan-bound request-category allowances and consumed counts. Upload categories cannot consume credential-free final-verification capacity.';
COMMENT ON TABLE oracle_candidate_source_snapshot_demo_remote_read_receipts IS
  'Immutable request-bound evidence for every credential-free final-verification read; raw response bodies and credentials are forbidden.';
