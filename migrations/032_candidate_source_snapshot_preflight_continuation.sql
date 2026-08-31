BEGIN;

-- One immutable, human-authorized continuation may supersede one exact
-- terminal preflight receipt. This is deliberately separate from the primary
-- approval: it neither changes that approval nor broadens its remote effects.
CREATE TABLE oracle_candidate_source_preflight_continuation_authorizations (
  authorization_id text PRIMARY KEY CHECK (
    authorization_id ~ '^snapshotdemocontinuation_[a-f0-9]{32}$'
  ),
  authorization_version text NOT NULL CHECK (
    authorization_version =
      'candidate-source-snapshot-preflight-continuation-authorization-v1'
  ),
  authorization_sha256 text NOT NULL UNIQUE CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  approval_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_approvals(approval_id),
  approval_sha256 text NOT NULL CHECK (
    approval_sha256 ~ '^[a-f0-9]{64}$'
  ),
  approval_authorization_statement_sha256 text NOT NULL CHECK (
    approval_authorization_statement_sha256 ~ '^[a-f0-9]{64}$'
  ),
  original_implementation_commit_sha text NOT NULL CHECK (
    original_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  amended_implementation_commit_sha text NOT NULL CHECK (
    amended_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  plan_revision integer NOT NULL CHECK (plan_revision > 0),
  approved_plan_revision integer NOT NULL CHECK (
    approved_plan_revision > 0
  ),
  plan_artifact_cid text NOT NULL CHECK (
    plan_artifact_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  plan_artifact_sha256 text NOT NULL CHECK (
    plan_artifact_sha256 ~ '^[a-f0-9]{64}$'
  ),
  failed_request_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  failed_receipt_sha256 text NOT NULL CHECK (
    failed_receipt_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorized_operation text NOT NULL CHECK (
    authorized_operation = 'official_filebase_gateway_resolution'
  ),
  domain text NOT NULL CHECK (domain = 'open_data'),
  operation_kind text NOT NULL CHECK (operation_kind = 'public_resolve'),
  resolver text NOT NULL CHECK (resolver = 'filebase_gateway'),
  ipns_network_key text NOT NULL CHECK (
    ipns_network_key ~ '^k51[0-9a-z]{59}$'
  ),
  expected_prior_cid text NOT NULL CHECK (
    expected_prior_cid ~
      '^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,120})$'
  ),
  expected_target_cid text NOT NULL CHECK (
    expected_target_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  resolver_policy text NOT NULL CHECK (
    resolver_policy = 'candidate_source_snapshot_filebase_delegated_v1'
  ),
  maximum_new_observations integer NOT NULL CHECK (
    maximum_new_observations = 1
  ),
  authorized_attempt_sequence integer NOT NULL CHECK (
    authorized_attempt_sequence BETWEEN 2 AND 3
  ),
  request_envelope_sha256 text NOT NULL CHECK (
    request_envelope_sha256 ~ '^[a-f0-9]{64}$'
  ),
  cost_envelope_sha256 text NOT NULL CHECK (
    cost_envelope_sha256 ~ '^[a-f0-9]{64}$'
  ),
  remaining_preflight_requests integer NOT NULL CHECK (
    remaining_preflight_requests BETWEEN 1 AND 48
  ),
  remaining_total_requests integer NOT NULL CHECK (
    remaining_total_requests BETWEEN 1 AND 1100000
  ),
  remaining_request_cost_usd numeric(18, 12) NOT NULL CHECK (
    remaining_request_cost_usd >= 0 AND remaining_request_cost_usd <= 25
  ),
  remaining_hard_budget_usd numeric(18, 12) NOT NULL CHECK (
    remaining_hard_budget_usd >= 0 AND remaining_hard_budget_usd <= 25
  ),
  authorization_binding jsonb NOT NULL,
  authorization_binding_sha256 text NOT NULL CHECK (
    authorization_binding_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorization_statement text NOT NULL CHECK (
    octet_length(authorization_statement) BETWEEN 1 AND 8192 AND
    authorization_statement !~ E'[\r\n]'
  ),
  authorization_statement_sha256 text NOT NULL CHECK (
    authorization_statement_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorizer_reference text NOT NULL CHECK (
    authorizer_reference ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
  ),
  authorized_at timestamptz NOT NULL,
  authorized_at_iso text NOT NULL CHECK (
    authorized_at_iso ~
      '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    AND authorized_at_iso::timestamptz = authorized_at
  ),
  authorization_payload jsonb NOT NULL,
  CHECK (
    original_implementation_commit_sha <>
      amended_implementation_commit_sha
  ),
  CHECK (plan_revision = approved_plan_revision + 1)
);

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD COLUMN continuation_authorization_id text,
  ADD CONSTRAINT oracle_css_request_continuation_authorization_fk
    FOREIGN KEY (continuation_authorization_id)
    REFERENCES oracle_candidate_source_preflight_continuation_authorizations(
      authorization_id
    ),
  ADD CONSTRAINT oracle_css_request_continuation_category_check CHECK (
    continuation_authorization_id IS NULL OR
    request_category = 'bucket_names_preflight'
  );

CREATE UNIQUE INDEX oracle_css_request_continuation_authorization_unique
  ON oracle_candidate_source_snapshot_demo_requests(
    continuation_authorization_id
  )
  WHERE continuation_authorization_id IS NOT NULL;

CREATE OR REPLACE FUNCTION oracle_css_preflight_continuation_statement(
  checked_binding jsonb,
  checked_authorizer_reference text,
  checked_authorized_at_iso text
)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
  RETURN
    'I authorize exactly one resumable candidate-owned source-snapshot preflight continuation for plan ' ||
    (checked_binding->'plan'->>'planId') || ', logical SHA-256 ' ||
    (checked_binding->'plan'->>'planSha256') ||
    ', at durable plan revision ' ||
    (checked_binding->'plan'->>'planRevision') ||
    ', under unchanged primary approval ' ||
    (checked_binding->'approval'->>'approvalId') || ', approval SHA-256 ' ||
    (checked_binding->'approval'->>'approvalSha256') ||
    ', and primary authorization-statement SHA-256 ' ||
    (checked_binding->'approval'->>'authorizationStatementSha256') ||
    '; the primary approval''s original implementation commit remains ' ||
    (checked_binding->'approval'->>'originalImplementationCommitSha') ||
    ', and only amended implementation commit ' ||
    (checked_binding->>'amendedImplementationCommitSha') ||
    ' may execute this continuation. This continuation is bound to immutable failed request ' ||
    (checked_binding->'failedReceipt'->>'requestId') ||
    ' with receipt SHA-256 ' ||
    (checked_binding->'failedReceipt'->>'receiptSha256') || ', outcome ' ||
    (checked_binding->'failedReceipt'->>'outcome') || ', attempt ' ||
    (checked_binding->'failedReceipt'->>'attemptSequence') || ', redirect ' ||
    (checked_binding->'failedReceipt'->>'redirectSequence') ||
    ', operation ' ||
    (checked_binding->'authorizedObservation'->>'authorizedOperation') ||
    ' stored as ' ||
    (checked_binding->'authorizedObservation'->>'domain') || '/' ||
    (checked_binding->'authorizedObservation'->>'storedOperationKind') || '/' ||
    (checked_binding->'authorizedObservation'->>'resolver') ||
    ', network key ' ||
    (checked_binding->'authorizedObservation'->>'ipnsNetworkKey') ||
    ', immutable prior ' ||
    (checked_binding->'authorizedObservation'->>'expectedPriorCid') ||
    ', approved target ' ||
    (checked_binding->'authorizedObservation'->>'expectedTargetCid') ||
    ', resolver policy ' ||
    (checked_binding->'authorizedObservation'->>'resolverPolicy') ||
    ', and at most ' ||
    (checked_binding->'authorizedObservation'->>'maximumNewLogicalObservations') ||
    ' new logical observation at attempt ' ||
    (checked_binding->'authorizedObservation'->>'authorizedAttemptSequence') ||
    '. It preserves plan artifact CID ' ||
    (checked_binding->'plan'->>'artifactCid') || ' and SHA-256 ' ||
    (checked_binding->'plan'->>'artifactSha256') ||
    ', request-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'requestEnvelopeSha256') ||
    ', and cost-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'costEnvelopeSha256') ||
    ', with ' ||
    (checked_binding->'remainingAllowance'->>'preflightRequests') ||
    ' bucket-names-preflight requests, ' ||
    (checked_binding->'remainingAllowance'->>'totalRequests') ||
    ' total requests, USD ' ||
    (checked_binding->'remainingAllowance'->>'requestCostUsd') ||
    ' request-cost allowance, and USD ' ||
    (checked_binding->'remainingAllowance'->>'hardBudgetUsd') ||
    ' hard-budget allowance remaining at authorization. The primary approval remains unchanged and every existing receipt remains immutable; this continuation authorizes code-continuation compatibility and only that specified recovery observation, not a different plan, target, resolver policy, artifact, upload, IPNS mutation, rollback, Vercel deployment, owner/canonical publication, or authoritative-complete claim. If the specified observation succeeds, the remaining publication operations already authorized by the unchanged primary approval may continue through amended implementation commit ' ||
    (checked_binding->>'amendedImplementationCommitSha') ||
    '; otherwise execution remains stopped fail-closed. Human authorization reference ' ||
    checked_authorizer_reference || ' at ' || checked_authorized_at_iso || '.';
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_preflight_continuation_authorization_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  approval_row oracle_candidate_source_snapshot_demo_approvals%ROWTYPE;
  failed_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  category_row oracle_candidate_source_snapshot_demo_request_categories%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  expected_binding jsonb;
  expected_binding_sha256 text;
  expected_statement text;
  expected_statement_sha256 text;
  expected_authorization_sha256 text;
  expected_authorization_id text;
  expected_authorization_payload jsonb;
  expected_request_cost_remaining numeric(18, 12);
  expected_hard_budget_remaining numeric(18, 12);
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT approval_row
  FROM oracle_candidate_source_snapshot_demo_approvals
  WHERE approval_id = NEW.approval_id
  FOR SHARE;
  SELECT * INTO STRICT failed_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = NEW.failed_request_id
  FOR SHARE;
  SELECT * INTO STRICT category_row
  FROM oracle_candidate_source_snapshot_demo_request_categories
  WHERE plan_id = NEW.plan_id
    AND request_category = 'bucket_names_preflight'
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  expected_request_cost_remaining :=
    (plan_row.cost_envelope->'requestUsd'->>'maximumAttempts')::numeric -
      accounting_row.request_cost_usd;
  expected_hard_budget_remaining :=
    plan_row.budget_limit_usd - accounting_row.request_cost_usd;
  expected_binding := jsonb_build_object(
    'amendedImplementationCommitSha', NEW.amended_implementation_commit_sha,
    'approval', jsonb_build_object(
      'approvalId', approval_row.approval_id,
      'approvalSha256', approval_row.approval_sha256,
      'approvedPlanRevision', approval_row.approved_plan_revision,
      'authorizationStatementSha256',
        approval_row.authorization_statement_sha256,
      'originalImplementationCommitSha',
        approval_row.implementation_commit_sha
    ),
    'authorizedObservation', jsonb_build_object(
      'authorizedAttemptSequence', failed_row.attempt_sequence + 1,
      'authorizedOperation', 'official_filebase_gateway_resolution',
      'domain', failed_row.domain,
      'expectedPriorCid',
        plan_row.plan_payload->'targets'->'openData'->>'priorCid',
      'expectedTargetCid',
        plan_row.plan_payload->'targets'->'openData'->>'targetCid',
      'ipnsNetworkKey',
        plan_row.plan_payload->'targets'->'openData'->>'ipnsNetworkKey',
      'maximumNewLogicalObservations', 1,
      'resolver', failed_row.resolver,
      'resolverPolicy',
        'candidate_source_snapshot_filebase_delegated_v1',
      'storedOperationKind', failed_row.operation_kind
    ),
    'failedReceipt', jsonb_build_object(
      'attemptSequence', failed_row.attempt_sequence,
      'outcome', failed_row.outcome,
      'receiptSha256', failed_row.receipt_sha256,
      'redirectSequence', failed_row.redirect_sequence,
      'requestId', failed_row.request_id
    ),
    'plan', jsonb_build_object(
      'artifactCid', plan_row.plan_artifact_cid,
      'artifactSha256', plan_row.plan_artifact_sha256,
      'planId', plan_row.plan_id,
      'planRevision', plan_row.revision,
      'planSha256', plan_row.plan_sha256
    ),
    'remainingAllowance', jsonb_build_object(
      'costEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.cost_envelope), 'UTF8'
      )), 'hex'),
      'hardBudgetUsd', to_char(
        expected_hard_budget_remaining,
        'FM999999999999990.000000000000'
      ),
      'preflightRequests',
        category_row.planned_maximum_request_count -
          category_row.consumed_request_count,
      'requestCostUsd', to_char(
        expected_request_cost_remaining,
        'FM999999999999990.000000000000'
      ),
      'requestEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.request_envelope), 'UTF8'
      )), 'hex'),
      'totalRequests',
        plan_row.maximum_request_count - accounting_row.request_count
    ),
    'schemaVersion',
      'candidate-source-snapshot-preflight-continuation-binding-v1'
  );
  expected_binding_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_binding), 'UTF8'
  )), 'hex');
  expected_statement :=
    oracle_css_preflight_continuation_statement(
      expected_binding, NEW.authorizer_reference, NEW.authorized_at_iso
    );
  expected_statement_sha256 := encode(sha256(convert_to(
    expected_statement, 'UTF8'
  )), 'hex');
  expected_authorization_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'authorizationBinding', expected_binding,
      'authorizationBindingSha256', expected_binding_sha256,
      'authorizationStatement', expected_statement,
      'authorizationStatementSha256', expected_statement_sha256,
      'authorizationVersion',
        'candidate-source-snapshot-preflight-continuation-authorization-v1',
      'authorizedAt', NEW.authorized_at_iso,
      'authorizerReference', NEW.authorizer_reference
    )), 'UTF8'
  )), 'hex');
  expected_authorization_id := 'snapshotdemocontinuation_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-preflight-continuation-authorization-v1',
      plan_row.plan_id,
      approval_row.approval_id,
      failed_row.request_id,
      expected_authorization_sha256
    ])), 'UTF8')), 'hex'), 1, 32);
  expected_authorization_payload := jsonb_build_object(
    'authorizationBinding', expected_binding,
    'authorizationBindingSha256', expected_binding_sha256,
    'authorizationId', expected_authorization_id,
    'authorizationSha256', expected_authorization_sha256,
    'authorizationStatement', expected_statement,
    'authorizationStatementSha256', expected_statement_sha256,
    'authorizationVersion',
      'candidate-source-snapshot-preflight-continuation-authorization-v1',
    'authorizedAt', NEW.authorized_at_iso,
    'authorizerReference', NEW.authorizer_reference
  );

  IF plan_row.plan_version IS DISTINCT FROM '2.1.0' OR
     plan_row.state IS DISTINCT FROM 'approved' OR
     plan_row.revision IS DISTINCT FROM NEW.plan_revision OR
     approval_row.plan_id IS DISTINCT FROM plan_row.plan_id OR
     approval_row.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     approval_row.approval_version IS DISTINCT FROM
       'candidate-source-snapshot-approval-v3' OR
     approval_row.approved_plan_revision IS DISTINCT FROM
       NEW.approved_plan_revision OR
     approval_row.approved_at > failed_row.started_at OR
     failed_row.completed_at IS NULL OR
     NEW.authorized_at < failed_row.completed_at OR
     failed_row.plan_id IS DISTINCT FROM plan_row.plan_id OR
     failed_row.request_category IS DISTINCT FROM
       'bucket_names_preflight' OR
     failed_row.domain IS DISTINCT FROM 'open_data' OR
     failed_row.operation_kind IS DISTINCT FROM 'public_resolve' OR
     failed_row.resolver IS DISTINCT FROM 'filebase_gateway' OR
     failed_row.outcome IS DISTINCT FROM 'terminal_failure' OR
     failed_row.receipt_sha256 IS NULL OR
     failed_row.attempt_sequence IS NULL OR
     failed_row.redirect_sequence IS NULL OR
     failed_row.intent_id IS NOT NULL OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_requests successor
       WHERE successor.plan_id = failed_row.plan_id
         AND successor.request_category = failed_row.request_category
         AND successor.logical_request_id = failed_row.logical_request_id
         AND successor.attempt_sequence > failed_row.attempt_sequence
     ) OR
     NEW.authorization_version IS DISTINCT FROM
       'candidate-source-snapshot-preflight-continuation-authorization-v1' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.approval_sha256 IS DISTINCT FROM approval_row.approval_sha256 OR
     NEW.approval_authorization_statement_sha256 IS DISTINCT FROM
       approval_row.authorization_statement_sha256 OR
     NEW.original_implementation_commit_sha IS DISTINCT FROM
       approval_row.implementation_commit_sha OR
     NEW.plan_artifact_cid IS DISTINCT FROM plan_row.plan_artifact_cid OR
     NEW.plan_artifact_sha256 IS DISTINCT FROM
       plan_row.plan_artifact_sha256 OR
     NEW.failed_receipt_sha256 IS DISTINCT FROM
       failed_row.receipt_sha256 OR
     NEW.authorized_operation IS DISTINCT FROM
       'official_filebase_gateway_resolution' OR
     NEW.domain IS DISTINCT FROM failed_row.domain OR
     NEW.operation_kind IS DISTINCT FROM failed_row.operation_kind OR
     NEW.resolver IS DISTINCT FROM failed_row.resolver OR
     NEW.ipns_network_key IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'ipnsNetworkKey' OR
     NEW.expected_prior_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'priorCid' OR
     NEW.expected_target_cid IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'targetCid' OR
     NEW.resolver_policy IS DISTINCT FROM
       'candidate_source_snapshot_filebase_delegated_v1' OR
     NEW.maximum_new_observations IS DISTINCT FROM 1 OR
     NEW.authorized_attempt_sequence IS DISTINCT FROM
       failed_row.attempt_sequence + 1 OR
     NEW.request_envelope_sha256 IS DISTINCT FROM
       expected_binding->'remainingAllowance'->>'requestEnvelopeSha256' OR
     NEW.cost_envelope_sha256 IS DISTINCT FROM
       expected_binding->'remainingAllowance'->>'costEnvelopeSha256' OR
     NEW.remaining_preflight_requests IS DISTINCT FROM
       (expected_binding->'remainingAllowance'->>'preflightRequests')::integer OR
     NEW.remaining_total_requests IS DISTINCT FROM
       (expected_binding->'remainingAllowance'->>'totalRequests')::integer OR
     NEW.remaining_request_cost_usd IS DISTINCT FROM
       expected_request_cost_remaining OR
     NEW.remaining_hard_budget_usd IS DISTINCT FROM
       expected_hard_budget_remaining OR
     NEW.authorized_at_iso::timestamptz IS DISTINCT FROM NEW.authorized_at OR
     NEW.authorization_binding IS DISTINCT FROM expected_binding OR
     NEW.authorization_binding_sha256 IS DISTINCT FROM
       expected_binding_sha256 OR
     NEW.authorization_statement IS DISTINCT FROM expected_statement OR
     NEW.authorization_statement_sha256 IS DISTINCT FROM
       expected_statement_sha256 OR
     NEW.authorization_sha256 IS DISTINCT FROM
       expected_authorization_sha256 OR
     NEW.authorization_id IS DISTINCT FROM expected_authorization_id OR
     NEW.authorization_payload IS DISTINCT FROM expected_authorization_payload THEN
    RAISE EXCEPTION
      'candidate source-snapshot preflight continuation authorization is not exact';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_preflight_continuation_authorization_insert_guard
  BEFORE INSERT
  ON oracle_candidate_source_preflight_continuation_authorizations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_preflight_continuation_authorization_insert();

CREATE TRIGGER oracle_css_preflight_continuation_authorization_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_preflight_continuation_authorizations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_guard_css_preflight_continuation_request_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  approval_row oracle_candidate_source_snapshot_demo_approvals%ROWTYPE;
  failed_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
  authorization_row oracle_candidate_source_preflight_continuation_authorizations%ROWTYPE;
  category_row oracle_candidate_source_snapshot_demo_request_categories%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  preceding_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  IF NEW.request_category IS DISTINCT FROM 'bucket_names_preflight' THEN
    IF NEW.continuation_authorization_id IS NOT NULL THEN
      RAISE EXCEPTION
        'candidate source-snapshot continuation may authorize only bucket and names preflight';
    END IF;
    RETURN NEW;
  END IF;

  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT approval_row
  FROM oracle_candidate_source_snapshot_demo_approvals
  WHERE plan_id = NEW.plan_id
    AND plan_sha256 = plan_row.plan_sha256
    AND approval_version = 'candidate-source-snapshot-approval-v3'
  FOR SHARE;

  IF plan_row.plan_version IS DISTINCT FROM '2.1.0' OR
     plan_row.state IS DISTINCT FROM 'approved' OR
     approval_row.approved_at > NEW.started_at OR
     NEW.intent_id IS NOT NULL THEN
    RAISE EXCEPTION
      'candidate source-snapshot preflight continuation requires the exact approved intent-free plan';
  END IF;

  SELECT * INTO preceding_row
  FROM oracle_candidate_source_snapshot_demo_requests request
  WHERE request.plan_id = NEW.plan_id
    AND request.request_category = NEW.request_category
    AND request.logical_request_id = NEW.logical_request_id
  ORDER BY request.attempt_sequence DESC, request.redirect_sequence DESC
  LIMIT 1
  FOR SHARE;

  IF NEW.continuation_authorization_id IS NULL THEN
    IF preceding_row.request_id IS NULL THEN
      IF NEW.attempt_sequence IS DISTINCT FROM 1 OR
         NEW.redirect_sequence IS DISTINCT FROM 0 THEN
        RAISE EXCEPTION
          'candidate source-snapshot initial preflight admission must start at attempt one';
      END IF;
    ELSIF preceding_row.outcome NOT IN (
            'retryable_failure', 'timeout_unknown'
          ) OR
          preceding_row.receipt_sha256 IS NULL OR
          preceding_row.attempt_sequence + 1 IS DISTINCT FROM
            NEW.attempt_sequence OR
          NEW.redirect_sequence IS DISTINCT FROM 0 THEN
      RAISE EXCEPTION
        'candidate source-snapshot preflight cannot bypass terminal receipt continuation authorization';
    END IF;
    RETURN NEW;
  END IF;

  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_preflight_continuation_authorizations
  WHERE authorization_id = NEW.continuation_authorization_id
  FOR SHARE;
  SELECT * INTO STRICT failed_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = authorization_row.failed_request_id
  FOR SHARE;
  SELECT * INTO STRICT category_row
  FROM oracle_candidate_source_snapshot_demo_request_categories
  WHERE plan_id = NEW.plan_id
    AND request_category = 'bucket_names_preflight'
  FOR UPDATE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;

  IF authorization_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     authorization_row.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     authorization_row.approval_id IS DISTINCT FROM approval_row.approval_id OR
     authorization_row.approval_sha256 IS DISTINCT FROM
       approval_row.approval_sha256 OR
     authorization_row.plan_revision IS DISTINCT FROM plan_row.revision OR
     authorization_row.approved_plan_revision IS DISTINCT FROM
       approval_row.approved_plan_revision OR
     authorization_row.domain IS DISTINCT FROM NEW.domain OR
     authorization_row.operation_kind IS DISTINCT FROM NEW.operation_kind OR
     authorization_row.resolver IS DISTINCT FROM NEW.resolver OR
     authorization_row.authorized_attempt_sequence IS DISTINCT FROM
       NEW.attempt_sequence OR
     authorization_row.maximum_new_observations IS DISTINCT FROM 1 OR
     NEW.redirect_sequence IS DISTINCT FROM 0 OR
     NEW.logical_request_id IS DISTINCT FROM failed_row.logical_request_id OR
     NEW.started_at < authorization_row.authorized_at OR
     failed_row.outcome IS DISTINCT FROM 'terminal_failure' OR
     failed_row.receipt_sha256 IS DISTINCT FROM
       authorization_row.failed_receipt_sha256 OR
     failed_row.attempt_sequence + 1 IS DISTINCT FROM NEW.attempt_sequence OR
     category_row.planned_maximum_request_count -
       category_row.consumed_request_count IS DISTINCT FROM
         authorization_row.remaining_preflight_requests - 1 OR
     plan_row.maximum_request_count - accounting_row.request_count IS DISTINCT FROM
       authorization_row.remaining_total_requests - 1 OR
     (plan_row.cost_envelope->'requestUsd'->>'maximumAttempts')::numeric -
       accounting_row.request_cost_usd IS DISTINCT FROM
         authorization_row.remaining_request_cost_usd - NEW.request_cost_usd OR
     plan_row.budget_limit_usd - accounting_row.request_cost_usd IS DISTINCT FROM
       authorization_row.remaining_hard_budget_usd - NEW.request_cost_usd THEN
    RAISE EXCEPTION
      'candidate source-snapshot continuation request lacks its exact receipt and remaining allowance';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_preflight_continuation_request_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_requests
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_preflight_continuation_request_insert();

-- Preserve the original terminal update protocol while making the new
-- continuation identity immutable with every other request identity field.
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
     OLD.continuation_authorization_id IS DISTINCT FROM
       NEW.continuation_authorization_id OR
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

-- Complete preflight now tolerates only the one specifically authorized
-- terminal receipt and only after its exact successor receipt succeeded.
CREATE OR REPLACE FUNCTION oracle_candidate_source_snapshot_preflight_is_execution_ready(
  checked_plan_id text
)
RETURNS boolean LANGUAGE sql STABLE AS $$
  WITH allowed_keys(domain, operation_kind, resolver) AS (
    VALUES
      ('open_data'::text, 'bucket_head'::text, NULL::text),
      ('open_data', 'names_read', 'filebase_control'),
      ('open_data', 'public_resolve', 'filebase_gateway'),
      ('open_data', 'public_resolve', 'delegated_ipfs'),
      ('query_table', 'bucket_head', NULL),
      ('query_table', 'names_read', 'filebase_control'),
      ('query_table', 'public_resolve', 'filebase_gateway'),
      ('query_table', 'public_resolve', 'delegated_ipfs')
  ), preflight AS (
    SELECT request.*
    FROM oracle_candidate_source_snapshot_demo_requests request
    WHERE request.plan_id = checked_plan_id
      AND request.request_category = 'bucket_names_preflight'
  )
  SELECT COALESCE(
    EXISTS (
      SELECT 1
      FROM oracle_candidate_source_snapshot_demo_plans plan
      JOIN oracle_candidate_source_snapshot_demo_approvals approval
        ON approval.plan_id = plan.plan_id
       AND approval.plan_sha256 = plan.plan_sha256
      JOIN oracle_candidate_source_snapshot_demo_request_categories category
        ON category.plan_id = plan.plan_id
       AND category.request_category = 'bucket_names_preflight'
      WHERE plan.plan_id = checked_plan_id
        AND plan.plan_version = '2.1.0'
        AND approval.approval_version =
          'candidate-source-snapshot-approval-v3'
        AND category.planned_successful_request_count = 8
        AND category.consumed_request_count BETWEEN 8 AND 48
        AND category.consumed_request_count = (SELECT count(*) FROM preflight)
    )
    AND (SELECT count(DISTINCT logical_request_id) FROM preflight) = 8
    AND NOT EXISTS (
      SELECT 1
      FROM preflight request
      WHERE request.intent_id IS NOT NULL
         OR request.receipt_sha256 IS NULL
         OR request.outcome = 'request_started'
         OR NOT EXISTS (
           SELECT 1
           FROM allowed_keys allowed
           WHERE allowed.domain = request.domain
             AND allowed.operation_kind = request.operation_kind
             AND allowed.resolver IS NOT DISTINCT FROM request.resolver
         )
         OR (
           request.outcome NOT IN (
             'succeeded', 'retryable_failure', 'timeout_unknown'
           ) AND NOT (
             request.outcome = 'terminal_failure' AND EXISTS (
               SELECT 1
               FROM oracle_candidate_source_preflight_continuation_authorizations continuation
               JOIN preflight successor
                 ON successor.continuation_authorization_id =
                      continuation.authorization_id
                AND successor.plan_id = request.plan_id
                AND successor.logical_request_id = request.logical_request_id
                AND successor.domain = request.domain
                AND successor.operation_kind = request.operation_kind
                AND successor.resolver IS NOT DISTINCT FROM request.resolver
                AND successor.attempt_sequence =
                      continuation.authorized_attempt_sequence
                AND successor.outcome = 'succeeded'
                AND successor.receipt_sha256 IS NOT NULL
               WHERE continuation.failed_request_id = request.request_id
                 AND continuation.plan_id = request.plan_id
             )
           )
         )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM preflight request
      WHERE request.continuation_authorization_id IS NOT NULL
        AND (
          request.outcome <> 'succeeded' OR
          NOT EXISTS (
            SELECT 1
            FROM oracle_candidate_source_preflight_continuation_authorizations continuation
            WHERE continuation.authorization_id =
                    request.continuation_authorization_id
              AND continuation.plan_id = request.plan_id
              AND continuation.domain = request.domain
              AND continuation.operation_kind = request.operation_kind
              AND continuation.resolver IS NOT DISTINCT FROM request.resolver
              AND continuation.authorized_attempt_sequence =
                    request.attempt_sequence
          )
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM oracle_candidate_source_preflight_continuation_authorizations continuation
      WHERE continuation.plan_id = checked_plan_id
        AND NOT EXISTS (
          SELECT 1
          FROM preflight successor
          WHERE successor.continuation_authorization_id =
                  continuation.authorization_id
            AND successor.outcome = 'succeeded'
        )
    )
    AND NOT EXISTS (
      SELECT request.domain, request.operation_kind, request.resolver
      FROM preflight request
      GROUP BY request.domain, request.operation_kind, request.resolver
      HAVING count(DISTINCT request.logical_request_id) <> 1 OR
             count(*) FILTER (WHERE request.outcome = 'succeeded') <> 1
    )
    AND (
      SELECT count(*)
      FROM (
        SELECT request.domain, request.operation_kind, request.resolver
        FROM preflight request
        GROUP BY request.domain, request.operation_kind, request.resolver
      ) exact_preflight_key
    ) = 8
    AND NOT EXISTS (
      SELECT 1
      FROM preflight first_request
      JOIN preflight second_request
        ON second_request.logical_request_id = first_request.logical_request_id
       AND (
         second_request.domain,
         second_request.operation_kind,
         second_request.resolver
       ) IS DISTINCT FROM (
         first_request.domain,
         first_request.operation_kind,
         first_request.resolver
       )
    ),
    false
  );
$$;

COMMENT ON TABLE oracle_candidate_source_preflight_continuation_authorizations IS
  'Immutable exact human authorization for one receipt-bound candidate source-snapshot preflight continuation; it does not replace or broaden the primary approval.';
COMMENT ON FUNCTION oracle_candidate_source_snapshot_preflight_is_execution_ready(text) IS
  'Exact eight-key preflight evidence, allowing one immutable terminal receipt only after its uniquely authorization-bound successor succeeds.';

COMMIT;
