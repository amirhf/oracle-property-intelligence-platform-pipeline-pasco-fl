BEGIN;

-- A transport continuation is an immutable, human-authored amendment to the
-- execution mechanics of one already-approved plan. It cannot alter the plan,
-- its inventory, targets, request envelope, cost ceiling, or existing effects.
CREATE OR REPLACE FUNCTION oracle_css_verified_receipt_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    object.domain || chr(31) ||
    encode(sha256(convert_to(object.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || object.expected_sha256 || chr(31) || object.expected_cid ||
    chr(31) || object.expected_bytes::text || chr(31) ||
    object.provider_cid || chr(31) || object.receipt_sha256,
    chr(30) ORDER BY object.domain, object.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_candidate_source_snapshot_demo_objects object
  WHERE object.plan_id = checked_plan_id
    AND object.status = 'verified';
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_uncertain_rows(
  checked_plan_id text
)
RETURNS TABLE (
  plan_id text,
  domain text,
  remote_object_key text,
  uncertainty_kind text,
  source_request_id text,
  source_attempt_id text,
  expected_sha256 text,
  expected_cid text,
  expected_bytes bigint
) LANGUAGE sql STABLE STRICT AS $$
  SELECT object.plan_id, object.domain, object.remote_object_key,
    CASE WHEN stale.attempt_id IS NOT NULL
      THEN 'stale_request_started'::text
      ELSE 'outcome_unknown'::text
    END,
    COALESCE(stale.request_id, latest.request_id),
    COALESCE(stale.attempt_id, latest.attempt_id),
    object.expected_sha256, object.expected_cid, object.expected_bytes
  FROM oracle_candidate_source_snapshot_demo_objects object
  LEFT JOIN LATERAL (
    SELECT attempt.attempt_id, attempt.request_id
    FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
    WHERE attempt.plan_id = object.plan_id
      AND attempt.domain = object.domain
      AND attempt.remote_object_key = object.remote_object_key
      AND attempt.outcome = 'request_started'
    ORDER BY attempt.attempt_sequence DESC
    LIMIT 1
  ) stale ON true
  LEFT JOIN LATERAL (
    SELECT attempt.attempt_id, attempt.request_id
    FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
    WHERE attempt.plan_id = object.plan_id
      AND attempt.domain = object.domain
      AND attempt.remote_object_key = object.remote_object_key
    ORDER BY attempt.attempt_sequence DESC
    LIMIT 1
  ) latest ON true
  WHERE object.plan_id = checked_plan_id
    AND object.status IN ('admitted', 'outcome_unknown')
    AND (
      stale.attempt_id IS NOT NULL OR object.status = 'outcome_unknown'
    )
    AND COALESCE(stale.attempt_id, latest.attempt_id) IS NOT NULL;
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_uncertain_set_sha256(
  checked_plan_id text
)
RETURNS text LANGUAGE sql STABLE STRICT AS $$
  SELECT encode(sha256(convert_to(COALESCE(string_agg(
    uncertain.domain || chr(31) ||
    encode(sha256(convert_to(uncertain.remote_object_key, 'UTF8')), 'hex') ||
    chr(31) || uncertain.uncertainty_kind || chr(31) ||
    uncertain.source_request_id || chr(31) || uncertain.source_attempt_id ||
    chr(31) || uncertain.expected_sha256 || chr(31) ||
    uncertain.expected_cid || chr(31) || uncertain.expected_bytes::text,
    chr(30) ORDER BY uncertain.domain, uncertain.remote_object_key
  ), ''), 'UTF8')), 'hex')
  FROM oracle_css_upload_uncertain_rows(checked_plan_id) uncertain;
$$;

CREATE TABLE oracle_candidate_source_snapshot_upload_continuation_authorizations (
  authorization_id text PRIMARY KEY CHECK (
    authorization_id ~ '^snapshotdemouploadcontinuation_[a-f0-9]{32}$'
  ),
  authorization_version text NOT NULL CHECK (
    authorization_version =
      'candidate-source-snapshot-upload-continuation-authorization-v1'
  ),
  authorization_sha256 text NOT NULL UNIQUE CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[a-f0-9]{64}$'),
  plan_revision integer NOT NULL CHECK (plan_revision > 0),
  approval_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_approvals(approval_id),
  approval_sha256 text NOT NULL CHECK (approval_sha256 ~ '^[a-f0-9]{64}$'),
  predecessor_authorization_id text,
  predecessor_authorization_sha256 text CHECK (
    predecessor_authorization_sha256 IS NULL OR
    predecessor_authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  predecessor_implementation_commit_sha text NOT NULL CHECK (
    predecessor_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  amended_implementation_commit_sha text NOT NULL CHECK (
    amended_implementation_commit_sha ~ '^[a-f0-9]{40}$'
  ),
  s3_endpoint text NOT NULL CHECK (
    s3_endpoint = 'https://s3.filebase.io'
  ),
  connection_timeout_ms integer NOT NULL CHECK (
    connection_timeout_ms = 15000
  ),
  socket_timeout_ms integer NOT NULL CHECK (
    socket_timeout_ms = 45000
  ),
  request_timeout_ms integer NOT NULL CHECK (
    request_timeout_ms BETWEEN 20000 AND 60000
  ),
  buffer_body_max_bytes integer NOT NULL CHECK (
    buffer_body_max_bytes = 1048576
  ),
  authorization_binding jsonb NOT NULL,
  authorization_binding_sha256 text NOT NULL CHECK (
    authorization_binding_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorization_statement text NOT NULL CHECK (
    octet_length(authorization_statement) BETWEEN 1 AND 12000 AND
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
    predecessor_implementation_commit_sha <>
      amended_implementation_commit_sha
  ),
  CHECK (
    (predecessor_authorization_id IS NULL) =
      (predecessor_authorization_sha256 IS NULL)
  )
);

CREATE TABLE oracle_candidate_source_snapshot_upload_continuation_uncertainties (
  authorization_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_upload_continuation_authorizations(
      authorization_id
    ),
  plan_id text NOT NULL,
  domain text NOT NULL CHECK (domain IN ('open_data', 'query_table')),
  remote_object_key text NOT NULL,
  uncertainty_kind text NOT NULL CHECK (
    uncertainty_kind IN ('stale_request_started', 'outcome_unknown')
  ),
  source_request_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_requests(request_id),
  source_attempt_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_demo_upload_attempts(attempt_id),
  expected_sha256 text NOT NULL CHECK (expected_sha256 ~ '^[a-f0-9]{64}$'),
  expected_cid text NOT NULL CHECK (
    expected_cid ~ '^Qm[1-9A-HJ-NP-Za-km-z]{44}$'
  ),
  expected_bytes bigint NOT NULL CHECK (
    expected_bytes BETWEEN 0 AND 536870912
  ),
  PRIMARY KEY (authorization_id, domain, remote_object_key),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE TABLE oracle_candidate_source_snapshot_executor_leases (
  lease_id text PRIMARY KEY CHECK (
    lease_id ~ '^snapshotdemoexecutorlease_[a-f0-9]{32}$'
  ),
  lease_version text NOT NULL CHECK (
    lease_version = 'candidate-source-snapshot-executor-lease-v1'
  ),
  authorization_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_upload_continuation_authorizations(
      authorization_id
    ),
  plan_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_plans(plan_id),
  holder_token_sha256 text NOT NULL CHECK (
    holder_token_sha256 ~ '^[a-f0-9]{64}$'
  ),
  lease_epoch integer NOT NULL CHECK (lease_epoch = 1),
  phase text NOT NULL CHECK (
    phase IN ('reconciling', 'upload_4', 'upload_8', 'upload_16', 'released')
  ),
  effective_concurrency integer NOT NULL CHECK (
    (phase = 'reconciling' AND effective_concurrency = 0) OR
    (phase = 'upload_4' AND effective_concurrency = 4) OR
    (phase = 'upload_8' AND effective_concurrency = 8) OR
    (phase = 'upload_16' AND effective_concurrency = 16) OR
    (phase = 'released' AND effective_concurrency = 0)
  ),
  acquired_at timestamptz NOT NULL,
  heartbeat_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
  CHECK (
    heartbeat_at >= acquired_at AND expires_at > heartbeat_at AND
    expires_at <= heartbeat_at + interval '5 minutes'
  )
);

ALTER TABLE oracle_candidate_source_snapshot_demo_requests
  ADD COLUMN upload_continuation_authorization_id text,
  ADD COLUMN executor_lease_id text,
  ADD COLUMN executor_lease_epoch integer,
  ADD CONSTRAINT oracle_css_request_upload_continuation_fk
    FOREIGN KEY (upload_continuation_authorization_id)
    REFERENCES oracle_candidate_source_snapshot_upload_continuation_authorizations(
      authorization_id
    ),
  ADD CONSTRAINT oracle_css_request_executor_lease_fk
    FOREIGN KEY (executor_lease_id)
    REFERENCES oracle_candidate_source_snapshot_executor_leases(lease_id),
  ADD CONSTRAINT oracle_css_request_executor_lease_binding_check CHECK (
    (upload_continuation_authorization_id IS NULL AND
      executor_lease_id IS NULL AND executor_lease_epoch IS NULL) OR
    (upload_continuation_authorization_id IS NOT NULL AND
      executor_lease_id IS NOT NULL AND executor_lease_epoch = 1)
  );

-- Nullable for historical attempts. Every continuation-owned terminal attempt
-- must populate this closed, sanitized transport evidence instead of storing a
-- raw SDK error, URL, object key, or provider response.
ALTER TABLE oracle_candidate_source_snapshot_demo_upload_attempts
  ADD COLUMN transport_stage text,
  ADD COLUMN failure_class text,
  ADD CONSTRAINT oracle_css_upload_attempt_transport_stage_check CHECK (
    transport_stage IS NULL OR transport_stage IN (
      'head_object_request', 'put_object_connection',
      'put_object_provider_response', 'put_object_streaming_request',
      'transport_deadline', 'unknown'
    )
  ),
  ADD CONSTRAINT oracle_css_upload_attempt_failure_class_check CHECK (
    failure_class IS NULL OR failure_class IN (
      'outcome_unknown', 'retryable', 'terminal'
    )
  );

CREATE TABLE oracle_candidate_source_snapshot_upload_continuation_reconciliations (
  authorization_id text NOT NULL,
  plan_id text NOT NULL,
  domain text NOT NULL,
  remote_object_key text NOT NULL,
  executor_lease_id text NOT NULL
    REFERENCES oracle_candidate_source_snapshot_executor_leases(lease_id),
  executor_lease_epoch integer NOT NULL CHECK (executor_lease_epoch = 1),
  inspection_id text NOT NULL UNIQUE
    REFERENCES oracle_candidate_source_snapshot_demo_inspections(inspection_id),
  result text NOT NULL CHECK (
    result IN ('remote_verified', 'conclusively_absent')
  ),
  receipt_sha256 text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (authorization_id, domain, remote_object_key),
  FOREIGN KEY (authorization_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_upload_continuation_uncertainties(
      authorization_id, domain, remote_object_key
    ),
  FOREIGN KEY (plan_id, domain, remote_object_key)
    REFERENCES oracle_candidate_source_snapshot_demo_objects(
      plan_id, domain, remote_object_key
    )
);

CREATE OR REPLACE FUNCTION oracle_css_upload_continuation_statement(
  checked_binding jsonb,
  checked_authorizer_reference text,
  checked_authorized_at_iso text
)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
  RETURN
    'I authorize exactly one fail-closed candidate-owned source-snapshot upload continuation for plan ' ||
    (checked_binding->'plan'->>'planId') || ', logical SHA-256 ' ||
    (checked_binding->'plan'->>'planSha256') || ', at durable plan revision ' ||
    (checked_binding->'plan'->>'planRevision') || ', under unchanged approval ' ||
    (checked_binding->'approval'->>'approvalId') || ', approval SHA-256 ' ||
    (checked_binding->'approval'->>'approvalSha256') ||
    ', from authorized implementation commit ' ||
    (checked_binding->'predecessor'->>'implementationCommitSha') ||
    ' to amended implementation commit ' ||
    (checked_binding->>'amendedImplementationCommitSha') ||
    '. It preserves ' ||
    (checked_binding->'checkpoint'->>'verifiedObjectCount') ||
    ' verified objects and ' ||
    (checked_binding->'checkpoint'->>'verifiedBytes') ||
    ' verified bytes under receipt-set SHA-256 ' ||
    (checked_binding->'checkpoint'->>'verifiedReceiptSetSha256') ||
    ', and requires reconciliation of ' ||
    (checked_binding->'checkpoint'->>'uncertainObjectCount') ||
    ' uncertain objects under set SHA-256 ' ||
    (checked_binding->'checkpoint'->>'uncertainSetSha256') ||
    ' before any upload. It authorizes request timeout ' ||
    (checked_binding->'execution'->>'requestTimeoutMs') ||
    ' ms over compiled S3 endpoint ' ||
    (checked_binding->'execution'->>'s3Endpoint') ||
    ', connection timeout ' ||
    (checked_binding->'execution'->>'connectionTimeoutMs') ||
    ' ms, socket timeout ' ||
    (checked_binding->'execution'->>'socketTimeoutMs') ||
    ' ms, immutable-buffer threshold ' ||
    (checked_binding->'execution'->>'bufferBodyMaxBytes') ||
    ' bytes, staged concurrency/maxSockets 4 then 8 then 16 after ' ||
    (checked_binding->'execution'->>'promotionVerifiedObjectsPerStage') ||
    ' newly verified objects per promotion, and exactly one executor lease. It leaves unchanged inventory CID ' ||
    (checked_binding->'inventory'->>'inventoryCid') ||
    ', inventory SHA-256 ' ||
    (checked_binding->'inventory'->>'fullInventorySha256') ||
    ', exactly ' ||
    (checked_binding->'inventory'->>'exactObjectCount') || ' objects and ' ||
    (checked_binding->'inventory'->>'exactTotalBytes') ||
    ' bytes, targets SHA-256 ' || (checked_binding->>'targetsSha256') ||
    ', request-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'requestEnvelopeSha256') ||
    ', cost-envelope SHA-256 ' ||
    (checked_binding->'remainingAllowance'->>'costEnvelopeSha256') ||
    ', absolute request ceiling ' ||
    (checked_binding->'remainingAllowance'->>'absoluteRequestCeiling') ||
    ', and USD ' ||
    (checked_binding->'remainingAllowance'->>'hardBudgetCeilingUsd') ||
    ' hard spending ceiling, with ' ||
    (checked_binding->'remainingAllowance'->>'requestsRemaining') ||
    ' requests and USD ' ||
    (checked_binding->'remainingAllowance'->>'hardBudgetRemainingUsd') ||
    ' hard-budget allowance remaining at authorization. No object, CID, key, bucket, prefix, IPNS identity, target, request ceiling, or cost ceiling may change; no IPNS operation is authorized by this amendment. Human authorization reference ' ||
    checked_authorizer_reference || ' at ' || checked_authorized_at_iso || '.';
END;
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_continuation_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  plan_row oracle_candidate_source_snapshot_demo_plans%ROWTYPE;
  approval_row oracle_candidate_source_snapshot_demo_approvals%ROWTYPE;
  accounting_row oracle_candidate_source_snapshot_demo_accounting%ROWTYPE;
  predecessor_id text;
  predecessor_sha text;
  predecessor_commit text;
  expected_binding jsonb;
  expected_binding_sha text;
  expected_statement text;
  expected_statement_sha text;
  expected_authorization_sha text;
  expected_authorization_id text;
  expected_payload jsonb;
  verified_count integer;
  verified_bytes bigint;
  pending_count integer;
  admitted_count integer;
  unknown_count integer;
  terminal_count integer;
  uncertain_count integer;
BEGIN
  SELECT * INTO STRICT plan_row
  FROM oracle_candidate_source_snapshot_demo_plans
  WHERE plan_id = NEW.plan_id
  FOR UPDATE;
  SELECT * INTO STRICT approval_row
  FROM oracle_candidate_source_snapshot_demo_approvals
  WHERE approval_id = NEW.approval_id
  FOR SHARE;
  SELECT * INTO STRICT accounting_row
  FROM oracle_candidate_source_snapshot_demo_accounting
  WHERE plan_id = NEW.plan_id
  FOR SHARE;

  SELECT event.metadata->>'continuationAuthorizationId',
         continuation.authorization_sha256,
         event.metadata->>'implementationCommitSha'
  INTO predecessor_id, predecessor_sha, predecessor_commit
  FROM oracle_candidate_source_snapshot_demo_events event
  LEFT JOIN oracle_candidate_source_preflight_continuation_authorizations continuation
    ON continuation.authorization_id =
       event.metadata->>'continuationAuthorizationId'
  WHERE event.plan_id = NEW.plan_id
    AND event.event_type = 'execution_started'
  ORDER BY event.recorded_at DESC
  LIMIT 1;
  predecessor_commit := COALESCE(
    predecessor_commit, approval_row.implementation_commit_sha
  );

  SELECT
    count(*) FILTER (WHERE status = 'verified')::integer,
    COALESCE(sum(expected_bytes) FILTER (WHERE status = 'verified'), 0)::bigint,
    count(*) FILTER (WHERE status = 'pending')::integer,
    count(*) FILTER (WHERE status = 'admitted')::integer,
    count(*) FILTER (WHERE status = 'outcome_unknown')::integer,
    count(*) FILTER (WHERE status = 'failed_terminal')::integer
  INTO verified_count, verified_bytes, pending_count, admitted_count,
       unknown_count, terminal_count
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id;
  SELECT count(*)::integer INTO uncertain_count
  FROM oracle_css_upload_uncertain_rows(NEW.plan_id);

  expected_binding := jsonb_build_object(
    'amendedImplementationCommitSha', NEW.amended_implementation_commit_sha,
    'approval', jsonb_build_object(
      'approvalId', approval_row.approval_id,
      'approvalSha256', approval_row.approval_sha256,
      'authorizationStatementSha256',
        approval_row.authorization_statement_sha256,
      'originalImplementationCommitSha',
        approval_row.implementation_commit_sha
    ),
    'checkpoint', jsonb_build_object(
      'admittedObjectCount', admitted_count,
      'failedTerminalObjectCount', terminal_count,
      'outcomeUnknownObjectCount', unknown_count,
      'pendingObjectCount', pending_count,
      'uncertainObjectCount', uncertain_count,
      'uncertainSetSha256',
        oracle_css_upload_uncertain_set_sha256(NEW.plan_id),
      'verifiedBytes', verified_bytes,
      'verifiedObjectCount', verified_count,
      'verifiedReceiptSetSha256',
        oracle_css_verified_receipt_set_sha256(NEW.plan_id)
    ),
    'execution', jsonb_build_object(
      'bufferBodyMaxBytes', NEW.buffer_body_max_bytes,
      'connectionTimeoutMs', NEW.connection_timeout_ms,
      'concurrencyStages', jsonb_build_array(4, 8, 16),
      'executorLeaseLimit', 1,
      'maxSocketsStages', jsonb_build_array(4, 8, 16),
      'promotionVerifiedObjectsPerStage', 64,
      'reconciliationRequired', true,
      'requestTimeoutMs', NEW.request_timeout_ms,
      's3Endpoint', NEW.s3_endpoint,
      'socketTimeoutMs', NEW.socket_timeout_ms
    ),
    'inventory', jsonb_build_object(
      'exactObjectCount', plan_row.exact_upload_object_count,
      'exactTotalBytes', plan_row.exact_upload_bytes,
      'fullInventorySha256',
        plan_row.plan_payload->'controlArtifacts'->>'fullInventoryRootSha256',
      'inventoryCid', plan_row.inventory_root_cid,
      'inventoryRootSha256', plan_row.inventory_root_sha256
    ),
    'plan', jsonb_build_object(
      'artifactCid', plan_row.plan_artifact_cid,
      'artifactSha256', plan_row.plan_artifact_sha256,
      'planId', plan_row.plan_id,
      'planRevision', plan_row.revision,
      'planSha256', plan_row.plan_sha256
    ),
    'predecessor', jsonb_build_object(
      'authorizationId', predecessor_id,
      'authorizationSha256', predecessor_sha,
      'implementationCommitSha', predecessor_commit
    ),
    'remainingAllowance', jsonb_build_object(
      'absoluteRequestCeiling', plan_row.maximum_request_count,
      'costEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.cost_envelope), 'UTF8'
      )), 'hex'),
      'hardBudgetCeilingUsd', to_char(
        plan_row.budget_limit_usd, 'FM999999999999990.000000000000'
      ),
      'hardBudgetRemainingUsd', to_char(
        plan_row.budget_limit_usd - accounting_row.request_cost_usd,
        'FM999999999999990.000000000000'
      ),
      'requestEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.request_envelope), 'UTF8'
      )), 'hex'),
      'requestsRemaining',
        plan_row.maximum_request_count - accounting_row.request_count
    ),
    'schemaVersion',
      'candidate-source-snapshot-upload-continuation-binding-v1',
    'targetsSha256', encode(sha256(convert_to(
      oracle_canonical_jsonb(plan_row.plan_payload->'targets'), 'UTF8'
    )), 'hex')
  );
  expected_binding_sha := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_binding), 'UTF8'
  )), 'hex');
  expected_statement := oracle_css_upload_continuation_statement(
    expected_binding, NEW.authorizer_reference, NEW.authorized_at_iso
  );
  expected_statement_sha := encode(sha256(convert_to(
    expected_statement, 'UTF8'
  )), 'hex');
  expected_authorization_sha := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'authorizationBinding', expected_binding,
      'authorizationBindingSha256', expected_binding_sha,
      'authorizationStatement', expected_statement,
      'authorizationStatementSha256', expected_statement_sha,
      'authorizationVersion',
        'candidate-source-snapshot-upload-continuation-authorization-v1',
      'authorizedAt', NEW.authorized_at_iso,
      'authorizerReference', NEW.authorizer_reference
    )), 'UTF8'
  )), 'hex');
  expected_authorization_id := 'snapshotdemouploadcontinuation_' ||
    substr(encode(sha256(convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-upload-continuation-authorization-v1',
      plan_row.plan_id,
      approval_row.approval_id,
      expected_authorization_sha
    ])), 'UTF8')), 'hex'), 1, 32);
  expected_payload := jsonb_build_object(
    'authorizationBinding', expected_binding,
    'authorizationBindingSha256', expected_binding_sha,
    'authorizationId', expected_authorization_id,
    'authorizationSha256', expected_authorization_sha,
    'authorizationStatement', expected_statement,
    'authorizationStatementSha256', expected_statement_sha,
    'authorizationVersion',
      'candidate-source-snapshot-upload-continuation-authorization-v1',
    'authorizedAt', NEW.authorized_at_iso,
    'authorizerReference', NEW.authorizer_reference
  );

  IF plan_row.plan_version IS DISTINCT FROM '2.1.0' OR
     plan_row.state IS DISTINCT FROM 'executing' OR
     plan_row.plan_sha256 IS DISTINCT FROM NEW.plan_sha256 OR
     plan_row.revision IS DISTINCT FROM NEW.plan_revision OR
     approval_row.plan_id IS DISTINCT FROM plan_row.plan_id OR
     approval_row.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     approval_row.approval_version IS DISTINCT FROM
       'candidate-source-snapshot-approval-v3' OR
     predecessor_commit IS NULL OR
     predecessor_commit IS DISTINCT FROM
       NEW.predecessor_implementation_commit_sha OR
     predecessor_id IS DISTINCT FROM NEW.predecessor_authorization_id OR
     predecessor_sha IS DISTINCT FROM NEW.predecessor_authorization_sha256 OR
     NEW.s3_endpoint IS DISTINCT FROM 'https://s3.filebase.io' OR
     NEW.connection_timeout_ms IS DISTINCT FROM 15000 OR
     NEW.socket_timeout_ms IS DISTINCT FROM 45000 OR
     NEW.connection_timeout_ms >= NEW.socket_timeout_ms OR
     NEW.socket_timeout_ms >= NEW.request_timeout_ms OR
     NEW.buffer_body_max_bytes IS DISTINCT FROM 1048576 OR
     terminal_count IS DISTINCT FROM 0 OR uncertain_count < 1 OR
     EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_demo_upload_attempts attempt
       WHERE attempt.plan_id = NEW.plan_id
         AND attempt.outcome IN ('provider_cid_mismatch', 'terminal_failure')
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_upload_closures closure
       WHERE closure.plan_id = NEW.plan_id
     ) OR
     EXISTS (
       SELECT 1 FROM oracle_candidate_source_snapshot_demo_ipns_intents intent
       WHERE intent.plan_id = NEW.plan_id
     ) OR
     NEW.authorization_version IS DISTINCT FROM
       'candidate-source-snapshot-upload-continuation-authorization-v1' OR
     NEW.authorization_binding IS DISTINCT FROM expected_binding OR
     NEW.authorization_binding_sha256 IS DISTINCT FROM expected_binding_sha OR
     NEW.authorization_statement IS DISTINCT FROM expected_statement OR
     NEW.authorization_statement_sha256 IS DISTINCT FROM expected_statement_sha OR
     NEW.authorization_sha256 IS DISTINCT FROM expected_authorization_sha OR
     NEW.authorization_id IS DISTINCT FROM expected_authorization_id OR
     NEW.authorization_payload IS DISTINCT FROM expected_payload OR
     NEW.authorized_at_iso::timestamptz IS DISTINCT FROM NEW.authorized_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload continuation is not the exact immutable recovery binding';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_continuation_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_upload_continuation_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_upload_continuation_insert();

CREATE OR REPLACE FUNCTION oracle_css_populate_upload_continuation_uncertainties()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO oracle_candidate_source_snapshot_upload_continuation_uncertainties (
    authorization_id, plan_id, domain, remote_object_key, uncertainty_kind,
    source_request_id, source_attempt_id, expected_sha256, expected_cid,
    expected_bytes
  )
  SELECT NEW.authorization_id, uncertain.plan_id, uncertain.domain,
         uncertain.remote_object_key, uncertain.uncertainty_kind,
         uncertain.source_request_id, uncertain.source_attempt_id,
         uncertain.expected_sha256, uncertain.expected_cid,
         uncertain.expected_bytes
  FROM oracle_css_upload_uncertain_rows(NEW.plan_id) uncertain;
  IF (SELECT count(*)
      FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties
      WHERE authorization_id = NEW.authorization_id) IS DISTINCT FROM
     (NEW.authorization_binding->'checkpoint'->>'uncertainObjectCount')::bigint THEN
    RAISE EXCEPTION
      'candidate source-snapshot continuation uncertainty membership drifted';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_continuation_populate_uncertainties
  AFTER INSERT ON oracle_candidate_source_snapshot_upload_continuation_authorizations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_css_populate_upload_continuation_uncertainties();

CREATE TRIGGER oracle_css_upload_continuation_authorization_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_continuation_authorizations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE TRIGGER oracle_css_upload_continuation_uncertainty_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_continuation_uncertainties
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE TRIGGER oracle_css_upload_continuation_reconciliation_immutable
  BEFORE UPDATE OR DELETE
  ON oracle_candidate_source_snapshot_upload_continuation_reconciliations
  FOR EACH ROW EXECUTE FUNCTION oracle_reject_candidate_source_snapshot_mutation();

CREATE OR REPLACE FUNCTION oracle_css_upload_continuation_is_reconciled(
  checked_authorization_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_continuation_authorizations auth
    WHERE auth.authorization_id = checked_authorization_id
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties uncertain
    WHERE uncertain.authorization_id = checked_authorization_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations reconciliation
        WHERE reconciliation.authorization_id = uncertain.authorization_id
          AND reconciliation.domain = uncertain.domain
          AND reconciliation.remote_object_key = uncertain.remote_object_key
      )
  );
$$;

CREATE OR REPLACE FUNCTION oracle_guard_css_executor_lease_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  authorization_row
    oracle_candidate_source_snapshot_upload_continuation_authorizations%ROWTYPE;
  expected_lease_id text;
BEGIN
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
  WHERE authorization_id = NEW.authorization_id
  FOR SHARE;
  expected_lease_id := 'snapshotdemoexecutorlease_' || substr(encode(sha256(
    convert_to(oracle_canonical_jsonb(to_jsonb(ARRAY[
      'candidate-source-snapshot-executor-lease-v1',
      authorization_row.plan_id,
      authorization_row.authorization_id
    ])), 'UTF8')
  ), 'hex'), 1, 32);
  IF NEW.lease_id IS DISTINCT FROM expected_lease_id OR
     NEW.lease_version IS DISTINCT FROM
       'candidate-source-snapshot-executor-lease-v1' OR
     NEW.plan_id IS DISTINCT FROM authorization_row.plan_id OR
     NEW.lease_epoch IS DISTINCT FROM 1 OR
     NEW.phase IS DISTINCT FROM 'reconciling' OR
     NEW.effective_concurrency IS DISTINCT FROM 0 OR
     NEW.revision IS DISTINCT FROM 1 OR
     NEW.heartbeat_at IS DISTINCT FROM NEW.acquired_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_executor_lease_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_executor_leases
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_executor_lease_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_executor_lease_update()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  valid_stage_transition boolean;
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.lease_id IS DISTINCT FROM NEW.lease_id OR
     OLD.lease_version IS DISTINCT FROM NEW.lease_version OR
     OLD.authorization_id IS DISTINCT FROM NEW.authorization_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.holder_token_sha256 IS DISTINCT FROM NEW.holder_token_sha256 OR
     OLD.lease_epoch IS DISTINCT FROM NEW.lease_epoch OR
     OLD.acquired_at IS DISTINCT FROM NEW.acquired_at OR
     OLD.revision + 1 IS DISTINCT FROM NEW.revision OR
     OLD.phase = 'released' THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease identity is immutable';
  END IF;
  valid_stage_transition :=
    (OLD.phase = NEW.phase AND
      OLD.effective_concurrency = NEW.effective_concurrency) OR
    (OLD.phase = 'reconciling' AND NEW.phase = 'upload_4' AND
      NEW.effective_concurrency = 4 AND
      oracle_css_upload_continuation_is_reconciled(OLD.authorization_id)) OR
    (OLD.phase = 'upload_4' AND NEW.phase = 'upload_8' AND
      NEW.effective_concurrency = 8) OR
    (OLD.phase = 'upload_8' AND NEW.phase = 'upload_16' AND
      NEW.effective_concurrency = 16) OR
    (NEW.phase = 'released' AND NEW.effective_concurrency = 0);
  IF NOT valid_stage_transition OR
     NEW.heartbeat_at < OLD.heartbeat_at OR
     NEW.expires_at <= NEW.heartbeat_at OR
     NEW.expires_at > NEW.heartbeat_at + interval '5 minutes' THEN
    RAISE EXCEPTION
      'candidate source-snapshot executor lease transition is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_executor_lease_update_guard
  BEFORE UPDATE OR DELETE ON oracle_candidate_source_snapshot_executor_leases
  FOR EACH ROW EXECUTE FUNCTION oracle_guard_css_executor_lease_update();

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_continuation_request_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  authorization_row
    oracle_candidate_source_snapshot_upload_continuation_authorizations%ROWTYPE;
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
BEGIN
  IF NEW.request_category NOT IN (
       'upload_provider_cid', 'ambiguous_upload_inspection'
     ) OR NOT EXISTS (
       SELECT 1
       FROM oracle_candidate_source_snapshot_upload_continuation_authorizations auth
       WHERE auth.plan_id = NEW.plan_id
     ) THEN
    RETURN NEW;
  END IF;
  SELECT * INTO STRICT authorization_row
  FROM oracle_candidate_source_snapshot_upload_continuation_authorizations
  WHERE authorization_id = NEW.upload_continuation_authorization_id
    AND plan_id = NEW.plan_id;
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.executor_lease_id
  FOR UPDATE;
  IF NEW.request_category = 'ambiguous_upload_inspection' AND (
       lease_row.phase <> 'reconciling' OR
       NOT EXISTS (
         SELECT 1
         FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties uncertain
         WHERE uncertain.authorization_id = authorization_row.authorization_id
           AND uncertain.plan_id = NEW.plan_id
           AND uncertain.domain = NEW.domain
           AND uncertain.remote_object_key = NEW.remote_object_key
       )
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot inspection is outside its frozen uncertainty set';
  END IF;
  IF lease_row.authorization_id IS DISTINCT FROM
       authorization_row.authorization_id OR
     lease_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     lease_row.lease_epoch IS DISTINCT FROM NEW.executor_lease_epoch OR
     lease_row.phase = 'released' OR lease_row.expires_at <= now() OR
     (NEW.request_category = 'upload_provider_cid' AND (
       lease_row.phase NOT IN ('upload_4', 'upload_8', 'upload_16') OR
       NOT oracle_css_upload_continuation_is_reconciled(
         authorization_row.authorization_id
       )
     )) THEN
    RAISE EXCEPTION
      'candidate source-snapshot request lacks the active reconciled executor lease';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_continuation_request_insert_guard
  BEFORE INSERT ON oracle_candidate_source_snapshot_demo_requests
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_upload_continuation_request_insert();

CREATE OR REPLACE FUNCTION oracle_guard_css_upload_continuation_reconciliation_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  uncertain_row
    oracle_candidate_source_snapshot_upload_continuation_uncertainties%ROWTYPE;
  inspection_row oracle_candidate_source_snapshot_demo_inspections%ROWTYPE;
  object_row oracle_candidate_source_snapshot_demo_objects%ROWTYPE;
  lease_row oracle_candidate_source_snapshot_executor_leases%ROWTYPE;
BEGIN
  SELECT * INTO STRICT uncertain_row
  FROM oracle_candidate_source_snapshot_upload_continuation_uncertainties
  WHERE authorization_id = NEW.authorization_id
    AND domain = NEW.domain
    AND remote_object_key = NEW.remote_object_key;
  SELECT * INTO STRICT inspection_row
  FROM oracle_candidate_source_snapshot_demo_inspections
  WHERE inspection_id = NEW.inspection_id;
  SELECT * INTO STRICT object_row
  FROM oracle_candidate_source_snapshot_demo_objects
  WHERE plan_id = NEW.plan_id AND domain = NEW.domain
    AND remote_object_key = NEW.remote_object_key
  FOR UPDATE;
  SELECT * INTO STRICT lease_row
  FROM oracle_candidate_source_snapshot_executor_leases
  WHERE lease_id = NEW.executor_lease_id
  FOR UPDATE;
  IF uncertain_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     inspection_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     inspection_row.domain IS DISTINCT FROM NEW.domain OR
     inspection_row.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     inspection_row.receipt_sha256 IS DISTINCT FROM NEW.receipt_sha256 OR
     lease_row.authorization_id IS DISTINCT FROM NEW.authorization_id OR
     lease_row.plan_id IS DISTINCT FROM NEW.plan_id OR
     lease_row.lease_epoch IS DISTINCT FROM NEW.executor_lease_epoch OR
     lease_row.phase IS DISTINCT FROM 'reconciling' OR
     lease_row.expires_at <= now() OR
     NOT (
       (NEW.result = 'remote_verified' AND
         inspection_row.outcome = 'verified' AND
         inspection_row.observed_cid = uncertain_row.expected_cid AND
         inspection_row.observed_sha256 = uncertain_row.expected_sha256 AND
         inspection_row.observed_bytes = uncertain_row.expected_bytes AND
         object_row.status = 'verified') OR
       (NEW.result = 'conclusively_absent' AND
         inspection_row.outcome = 'absent' AND
         object_row.status IN ('admitted', 'outcome_unknown'))
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot reconciliation is not exact and conclusive';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_continuation_reconciliation_insert_guard
  BEFORE INSERT
  ON oracle_candidate_source_snapshot_upload_continuation_reconciliations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_guard_css_upload_continuation_reconciliation_insert();

CREATE OR REPLACE FUNCTION oracle_css_finalize_absent_upload_reconciliation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.result = 'conclusively_absent' THEN
    UPDATE oracle_candidate_source_snapshot_demo_objects
    SET status = 'pending', revision = revision + 1, updated_at = now()
    WHERE plan_id = NEW.plan_id AND domain = NEW.domain
      AND remote_object_key = NEW.remote_object_key
      AND status IN ('admitted', 'outcome_unknown');
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'candidate source-snapshot absent reconciliation lost its object';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_css_upload_continuation_finalize_absent
  AFTER INSERT
  ON oracle_candidate_source_snapshot_upload_continuation_reconciliations
  FOR EACH ROW EXECUTE FUNCTION
    oracle_css_finalize_absent_upload_reconciliation();

-- Preserve every prior transition while allowing only a conclusive,
-- immutable continuation reconciliation to return an uncertain object to
-- pending for its next ordinary attempt.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_object()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
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
  IF NOT (
    OLD.status = NEW.status OR
    (OLD.status = 'pending' AND NEW.status = 'admitted') OR
    (OLD.status = 'admitted' AND NEW.status IN (
      'outcome_unknown', 'verified', 'failed_terminal'
    )) OR
    (OLD.status = 'outcome_unknown' AND NEW.status IN (
      'admitted', 'verified', 'failed_terminal'
    )) OR
    (OLD.status IN ('admitted', 'outcome_unknown') AND
      NEW.status = 'pending' AND
      EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_continuation_reconciliations reconciliation
        WHERE reconciliation.plan_id = OLD.plan_id
          AND reconciliation.domain = OLD.domain
          AND reconciliation.remote_object_key = OLD.remote_object_key
          AND reconciliation.result = 'conclusively_absent'
      ))
  ) THEN
    RAISE EXCEPTION 'invalid candidate source-snapshot object transition';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- Make the newly added execution identities immutable on terminal request
-- updates without disturbing the historical preflight-continuation column.
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
     OLD.upload_continuation_authorization_id IS DISTINCT FROM
       NEW.upload_continuation_authorization_id OR
     OLD.executor_lease_id IS DISTINCT FROM NEW.executor_lease_id OR
     OLD.executor_lease_epoch IS DISTINCT FROM NEW.executor_lease_epoch OR
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

-- Historical terminal attempts remain valid with NULL fixed evidence. A new
-- continuation-owned attempt must atomically terminalize with its closed stage
-- and failure classification, after which the existing one-way transition
-- makes that evidence immutable.
CREATE OR REPLACE FUNCTION oracle_guard_candidate_source_snapshot_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  request_row oracle_candidate_source_snapshot_demo_requests%ROWTYPE;
BEGIN
  IF TG_OP = 'DELETE' OR
     OLD.attempt_id IS DISTINCT FROM NEW.attempt_id OR
     OLD.request_id IS DISTINCT FROM NEW.request_id OR
     OLD.plan_id IS DISTINCT FROM NEW.plan_id OR
     OLD.domain IS DISTINCT FROM NEW.domain OR
     OLD.remote_object_key IS DISTINCT FROM NEW.remote_object_key OR
     OLD.attempt_sequence IS DISTINCT FROM NEW.attempt_sequence OR
     OLD.request_count IS DISTINCT FROM NEW.request_count OR
     OLD.started_at IS DISTINCT FROM NEW.started_at THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload attempt identity is immutable';
  END IF;
  IF OLD.outcome <> 'request_started' OR NEW.outcome = 'request_started' THEN
    RAISE EXCEPTION
      'candidate source-snapshot upload attempt is already terminal';
  END IF;
  SELECT * INTO STRICT request_row
  FROM oracle_candidate_source_snapshot_demo_requests
  WHERE request_id = OLD.request_id;
  IF request_row.upload_continuation_authorization_id IS NOT NULL AND (
       NEW.transport_stage IS NULL OR
       (NEW.outcome = 'verified' AND (
         NEW.transport_stage <> 'put_object_provider_response' OR
         NEW.failure_class IS NOT NULL
       )) OR
       (NEW.outcome <> 'verified' AND NEW.failure_class IS NULL)
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot continuation attempt lacks fixed transport evidence';
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON TABLE oracle_candidate_source_snapshot_upload_continuation_authorizations IS
  'Immutable human authorization for one reconciliation-first transport amendment; it changes no plan, object, target, request ceiling, cost ceiling, or IPNS authority.';
COMMENT ON TABLE oracle_candidate_source_snapshot_executor_leases IS
  'One stable, bounded executor ownership lease for an authorized transport continuation; no lease permits IPNS work.';

COMMIT;
