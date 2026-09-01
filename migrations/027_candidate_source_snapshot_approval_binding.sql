-- Exact, immutable Session 2A human-authorization binding for the separate
-- candidate-owned source-snapshot demonstration. Migrations 025 and 026 stay
-- immutable; no approval or remote effect is synthesized here.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM oracle_candidate_source_snapshot_demo_approvals
  ) THEN
    RAISE EXCEPTION
      'migration 027 requires zero legacy candidate source-snapshot approvals';
  END IF;
END;
$$;

ALTER TABLE oracle_candidate_source_snapshot_demo_approvals
  ADD COLUMN approval_version text NOT NULL,
  ADD COLUMN approval_sha256 text NOT NULL,
  ADD COLUMN approved_at_iso text NOT NULL,
  ADD COLUMN authorization_statement text NOT NULL,
  ADD COLUMN authorization_statement_sha256 text NOT NULL,
  ADD COLUMN authorization_binding jsonb NOT NULL,
  ADD COLUMN authorization_binding_sha256 text NOT NULL,
  ADD CONSTRAINT oracle_css_approval_version_check
    CHECK (approval_version = 'candidate-source-snapshot-approval-v2'),
  ADD CONSTRAINT oracle_css_approval_sha256_check
    CHECK (approval_sha256 ~ '^[a-f0-9]{64}$'),
  ADD CONSTRAINT oracle_css_approved_at_iso_check
    CHECK (
      approved_at_iso ~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
      AND approved_at_iso::timestamptz = approved_at
    ),
  ADD CONSTRAINT oracle_css_authorization_statement_check
    CHECK (
      octet_length(authorization_statement) BETWEEN 1 AND 8192
      AND authorization_statement !~ E'[\r\n]'
    ),
  ADD CONSTRAINT oracle_css_authorization_statement_sha_check
    CHECK (authorization_statement_sha256 ~ '^[a-f0-9]{64}$'),
  ADD CONSTRAINT oracle_css_authorization_binding_sha_check
    CHECK (authorization_binding_sha256 ~ '^[a-f0-9]{64}$'),
  ADD CONSTRAINT oracle_css_approval_sha_unique
    UNIQUE (approval_sha256);

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

  expected_binding := jsonb_build_object(
    'classification', plan_row.plan_payload->'classification',
    'execution', jsonb_build_object(
      'absoluteRequestCeiling',
        plan_row.plan_payload->'requestEnvelope'->'maximumTotalRequests',
      'ambiguousInspectionAllowance',
        plan_row.plan_payload->'requestEnvelope'->
          'ambiguousObjectInspectionAllowance'->'total',
      'cutoverOrder',
        plan_row.plan_payload->'protectedSampleRollback'->'cutoverOrder',
      'maximumAttemptCount',
        plan_row.plan_payload->'requestEnvelope'->'maximumAttempts'->'total',
      'maximumAttemptsPerObject',
        to_jsonb((plan_row.plan_payload->'limits'->>'maxRetries')::integer + 1),
      'maximumConcurrency',
        plan_row.plan_payload->'limits'->'maxConcurrency',
      'maximumRetries', plan_row.plan_payload->'limits'->'maxRetries',
      'recoveryAllowance',
        plan_row.plan_payload->'requestEnvelope'->'recoveryAllowance'->'total',
      'requestEnvelopeSha256', encode(sha256(convert_to(
        oracle_canonical_jsonb(plan_row.plan_payload->'requestEnvelope'),
        'UTF8'
      )), 'hex'),
      'requestTimeoutMs', plan_row.plan_payload->'limits'->'requestTimeoutMs',
      'spendingCeilingUsd', plan_row.plan_payload->'limits'->'maxBudgetUsd',
      'successfulRequestCount',
        plan_row.plan_payload->'requestEnvelope'->'successfulExecution'->'total'
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
    'plan', jsonb_build_object(
      'artifactByteSize', to_jsonb(plan_row.plan_artifact_bytes),
      'artifactCid', to_jsonb(plan_row.plan_artifact_cid),
      'artifactRemoteObjectKey',
        to_jsonb(plan_row.plan_artifact_remote_object_key),
      'artifactSha256', to_jsonb(plan_row.plan_artifact_sha256),
      'planId', to_jsonb(plan_row.plan_id),
      'planLogicalSha256', to_jsonb(plan_row.plan_sha256)
    ),
    'schemaVersion', 'candidate-source-snapshot-authorization-binding-v1',
    'targets', jsonb_build_object(
      'openData', jsonb_build_object(
        'bucket', plan_row.plan_payload->'targets'->'openData'->'bucket',
        'immutablePrefix',
          plan_row.plan_payload->'targets'->'openData'->'immutablePrefix',
        'ipnsLabel', plan_row.plan_payload->'targets'->'openData'->'ipnsLabel',
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

  expected_binding_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(expected_binding), 'UTF8'
  )), 'hex');

  IF plan_row.plan_payload->'limits'->>'maxRetries' IS DISTINCT FROM '2' OR
     (plan_row.plan_payload->'limits'->>'maxRetries')::integer + 1 <> 3 OR
     plan_row.plan_payload->'targets'->'openData'->>'bucket' IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'openData'->>'ipnsLabel' OR
     plan_row.plan_payload->'targets'->'queryTable'->>'bucket' IS DISTINCT FROM
       plan_row.plan_payload->'targets'->'queryTable'->>'ipnsLabel' THEN
    RAISE EXCEPTION
      'candidate source-snapshot authorization plan is outside the reviewed statement grammar';
  END IF;

  expected_statement := format(
    'I confirm the candidate-controlled Filebase account is Pro or better and supports at least %s pinned objects, %s bytes, two distinct buckets and two distinct IPNS names, and I approve only candidate_owned_source_snapshot_demo plan %s with logical SHA-256 %s, plan artifact SHA-256 %s and CID %s, exactly %s objects and %s upload bytes with %s admission-reserved bytes, open-data bucket and label %s under immutable prefix %s and network key %s from prior %s to target %s, query-table bucket and label %s under immutable prefix %s and network key %s from prior %s to target %s, manifest CID %s and SHA-256 %s, inventory CID %s and full-inventory SHA-256 %s, successful request count %s, maximum-attempt count %s, ambiguous-inspection allowance %s, recovery allowance %s, absolute request ceiling %s, two retries, three total object attempts, concurrency %s, %s ms timeout and USD %s spending ceiling for uploading only these immutable objects and then updating only these two candidate IPNS identities in durable open-data-first/query-table-second order after exact provider-CID verification; this authorization is candidate-only and noncanonical and does not authorize or represent Elephant-owned, owner-controlled, owner/canonical, authoritative-complete, independently Pasco-certified, Accela/BBB, production-database, Vercel-deployment or any other publication authority.',
    plan_row.plan_payload->'limits'->>'maxObjects',
    plan_row.plan_payload->'limits'->>'maxTotalBytes',
    plan_row.plan_id,
    plan_row.plan_sha256,
    plan_row.plan_artifact_sha256,
    plan_row.plan_artifact_cid,
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
    plan_row.plan_payload->'requestEnvelope'->'successfulExecution'->>'total',
    plan_row.plan_payload->'requestEnvelope'->'maximumAttempts'->>'total',
    plan_row.plan_payload->'requestEnvelope'->
      'ambiguousObjectInspectionAllowance'->>'total',
    plan_row.plan_payload->'requestEnvelope'->'recoveryAllowance'->>'total',
    plan_row.plan_payload->'requestEnvelope'->>'maximumTotalRequests',
    plan_row.plan_payload->'limits'->>'maxConcurrency',
    plan_row.plan_payload->'limits'->>'requestTimeoutMs',
    plan_row.plan_payload->'limits'->>'maxBudgetUsd'
  );
  expected_statement_sha256 := encode(sha256(convert_to(
    expected_statement, 'UTF8'
  )), 'hex');

  expected_approval_sha256 := encode(sha256(convert_to(
    oracle_canonical_jsonb(jsonb_build_object(
      'approvalVersion', 'candidate-source-snapshot-approval-v2',
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
      'candidate-source-snapshot-approval-v2',
      plan_row.plan_id,
      expected_approval_sha256
    ])), 'UTF8')
  ), 'hex'), 1, 32);

  IF plan_row.state <> 'awaiting_approval' OR
     NEW.plan_sha256 IS DISTINCT FROM plan_row.plan_sha256 OR
     NEW.plan_artifact_sha256 IS DISTINCT FROM plan_row.plan_artifact_sha256 OR
     NEW.plan_artifact_cid IS DISTINCT FROM plan_row.plan_artifact_cid OR
     NEW.plan_artifact_remote_object_key IS DISTINCT FROM
       plan_row.plan_artifact_remote_object_key OR
     NEW.plan_artifact_bytes IS DISTINCT FROM plan_row.plan_artifact_bytes OR
     NEW.approved_plan_revision IS DISTINCT FROM plan_row.revision OR
     NEW.approval_version IS DISTINCT FROM
       'candidate-source-snapshot-approval-v2' OR
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
         AND confirmation.plan_artifact_sha256 = plan_row.plan_artifact_sha256
         AND confirmation.plan_artifact_cid = plan_row.plan_artifact_cid
         AND confirmation.plan_artifact_remote_object_key =
           plan_row.plan_artifact_remote_object_key
         AND confirmation.plan_artifact_bytes = plan_row.plan_artifact_bytes
     ) THEN
    RAISE EXCEPTION
      'candidate source-snapshot approval is not bound to the exact authorization and confirmed immutable plan';
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON COLUMN oracle_candidate_source_snapshot_demo_approvals.authorization_statement IS
  'Exact byte-for-byte human statement rendered from the immutable candidate source-snapshot plan; never a normalized paraphrase.';
COMMENT ON COLUMN oracle_candidate_source_snapshot_demo_approvals.approval_sha256 IS
  'Canonical SHA-256 binding statement, structured authorization, approver reference, timestamp, and immutable plan identity.';
