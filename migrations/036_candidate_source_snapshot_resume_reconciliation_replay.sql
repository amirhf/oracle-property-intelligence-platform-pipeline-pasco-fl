-- A conclusive, immutable inspection resolution remains valid when a later
-- fenced executor generation supersedes an expired lease. The source attempt,
-- object identity, receipt, and expected bytes/CID/hash remain guarded by the
-- existing cycle-resolution constraints.

CREATE OR REPLACE FUNCTION oracle_css_upload_resume_is_reconciled(
  checked_resume_authorization_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND oracle_css_upload_continuation_is_reconciled(
        resume.predecessor_authorization_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_inspection_cycles cycle
    WHERE cycle.resume_authorization_id = checked_resume_authorization_id
      AND NOT oracle_css_upload_inspection_cycle_is_reconciled(
        cycle.inspection_cycle_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN oracle_candidate_source_snapshot_admitted_recovery_events recovery
      ON recovery.plan_id = resume.plan_id
     AND recovery.disposition = 'inspection_required'
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          ON resolution.inspection_cycle_id = member.inspection_cycle_id
         AND resolution.domain = member.domain
         AND resolution.remote_object_key = member.remote_object_key
        WHERE member.plan_id = recovery.plan_id
          AND member.domain = recovery.domain
          AND member.remote_object_key = recovery.remote_object_key
          AND member.source_attempt_id = recovery.source_attempt_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN LATERAL oracle_css_current_upload_cycle_uncertain_rows(resume.plan_id)
      uncertain ON true
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
  );
$$;

CREATE OR REPLACE FUNCTION oracle_css_upload_resume_admission_is_reconciled(
  checked_resume_authorization_id text
)
RETURNS boolean LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND oracle_css_upload_continuation_is_reconciled(
        resume.predecessor_authorization_id
      )
  ) AND NOT EXISTS (
    SELECT 1
    FROM oracle_candidate_source_snapshot_upload_resume_authorizations resume
    JOIN oracle_candidate_source_snapshot_admitted_recovery_events recovery
      ON recovery.plan_id = resume.plan_id
     AND recovery.disposition = 'inspection_required'
    WHERE resume.resume_authorization_id = checked_resume_authorization_id
      AND NOT EXISTS (
        SELECT 1
        FROM oracle_candidate_source_snapshot_upload_inspection_cycle_members member
        JOIN oracle_candidate_source_snapshot_upload_inspection_cycle_resolutions resolution
          ON resolution.inspection_cycle_id = member.inspection_cycle_id
         AND resolution.domain = member.domain
         AND resolution.remote_object_key = member.remote_object_key
        WHERE member.plan_id = recovery.plan_id
          AND member.domain = recovery.domain
          AND member.remote_object_key = recovery.remote_object_key
          AND member.source_attempt_id = recovery.source_attempt_id
      )
  );
$$;
