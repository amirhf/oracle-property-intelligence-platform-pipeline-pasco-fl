-- Fail-closed hardening for the disabled contractor/BBB staging boundary.
-- No source authorization or dataset is seeded. The assessment-local tables
-- must be empty because there is no version-aware upgrade path for evidence
-- that predates these bindings.

DO $$
DECLARE
  populated_table text;
  populated_count bigint;
BEGIN
  FOREACH populated_table IN ARRAY ARRAY[
    'oracle_contractor_source_datasets',
    'oracle_contractor_source_record_versions',
    'oracle_contractor_identity_matches',
    'oracle_permit_contractors'
  ] LOOP
    EXECUTE format('SELECT count(*) FROM %I', populated_table)
      INTO populated_count;
    IF populated_count <> 0 THEN
      RAISE EXCEPTION
        'Contractor staging hardening requires empty legacy table %',
        populated_table;
    END IF;
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_canonical_jsonb(value jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
  value_type text := jsonb_typeof(value);
  encoded text;
BEGIN
  IF value_type = 'object' THEN
    SELECT '{' || coalesce(
      string_agg(to_json(key)::text || ':' || oracle_canonical_jsonb(child),
                 ',' ORDER BY key COLLATE "C"),
      ''
    ) || '}'
      INTO encoded
      FROM jsonb_each(value) AS entry(key, child);
    RETURN encoded;
  ELSIF value_type = 'array' THEN
    SELECT '[' || coalesce(
      string_agg(oracle_canonical_jsonb(child), ',' ORDER BY ordinal),
      ''
    ) || ']'
      INTO encoded
      FROM jsonb_array_elements(value) WITH ORDINALITY AS entry(child, ordinal);
    RETURN encoded;
  END IF;
  RETURN value::text;
END;
$$;

CREATE OR REPLACE FUNCTION oracle_jsonb_exact_keys(
  value jsonb,
  expected_keys text[]
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT jsonb_typeof(value) = 'object'
    AND (SELECT array_agg(key ORDER BY key COLLATE "C")
         FROM jsonb_object_keys(value) AS entry(key))
      IS NOT DISTINCT FROM
        (SELECT array_agg(key ORDER BY key COLLATE "C")
         FROM unnest(expected_keys) AS entry(key));
$$;

CREATE OR REPLACE FUNCTION oracle_contractor_relative_path_valid(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT length(value) BETWEEN 1 AND 1000
    AND value !~ '^/'
    AND value !~ '^[A-Za-z]:'
    AND value !~ '\\'
    AND value NOT LIKE '%//%'
    AND value !~ '(^|/)\.\.?($|/)';
$$;

CREATE OR REPLACE FUNCTION oracle_contractor_https_url_valid(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT length(value) BETWEEN 9 AND 2000
    AND value = lower(value)
    AND value ~ '^https://[a-z0-9][a-z0-9.-]*[a-z0-9](/[^?#[:space:]@]*)?$'
    AND value !~ 'https://[^/]*(\.\.|\.-|-\.)'
    AND value !~* '(^|/)(access[-_]?key|api[-_]?key|authorization|bearer|cookie|password|private[-_]?key|proxy[-_]?authorization|secret|signature|token)([=/:]|$)';
$$;

CREATE OR REPLACE FUNCTION oracle_contractor_https_origin_valid(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT length(value) BETWEEN 9 AND 253
    AND oracle_contractor_https_url_valid(value)
    AND value ~ '^https://[a-z0-9][a-z0-9.-]*[a-z0-9]$';
$$;

CREATE OR REPLACE FUNCTION oracle_contractor_https_origin(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT substring(value FROM '^(https://[^/]+)');
$$;

CREATE OR REPLACE FUNCTION oracle_contractor_iso_timestamp_valid(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT value ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$';
$$;

CREATE TABLE IF NOT EXISTS oracle_contractor_source_authorizations (
  authorization_id text PRIMARY KEY CHECK (
    authorization_id ~ '^contractorauthorization_[a-f0-9]{32}$'
  ),
  authorization_sha256 text NOT NULL CHECK (
    authorization_sha256 ~ '^[a-f0-9]{64}$'
  ),
  policy_version text NOT NULL CHECK (
    policy_version = 'contractor-source-authorization-v1'
  ),
  provider text NOT NULL CHECK (
    provider IN ('better_business_bureau', 'official_license_source')
  ),
  source_classification text NOT NULL CHECK (
    source_classification IN ('official', 'third_party')
  ),
  acquisition_method text NOT NULL CHECK (
    acquisition_method IN (
      'authorized_api', 'licensed_export', 'owner_supplied_file'
    )
  ),
  approver_reference text NOT NULL CHECK (
    length(approver_reference) BETWEEN 1 AND 200
    AND approver_reference ~ '^[a-zA-Z0-9][a-zA-Z0-9._:@/-]*$'
  ),
  approved_at timestamptz NOT NULL,
  coverage_geography text NOT NULL CHECK (
    length(coverage_geography) BETWEEN 1 AND 500
  ),
  category_filters jsonb NOT NULL CHECK (
    jsonb_typeof(category_filters) = 'array'
    AND jsonb_array_length(category_filters) <= 100
  ),
  authorized_source_origins jsonb NOT NULL CHECK (
    jsonb_typeof(authorized_source_origins) = 'array'
    AND jsonb_array_length(authorized_source_origins) BETWEEN 1 AND 100
  ),
  terms_evidence_relative_path text NOT NULL CHECK (
    oracle_contractor_relative_path_valid(terms_evidence_relative_path)
  ),
  terms_evidence_byte_size bigint NOT NULL CHECK (
    terms_evidence_byte_size BETWEEN 1 AND 10485760
  ),
  terms_evidence_sha256 text NOT NULL CHECK (
    terms_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  authorization_payload jsonb NOT NULL,
  authorization_canonical_json text NOT NULL CHECK (
    length(authorization_canonical_json) BETWEEN 2 AND 131072
  ),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION oracle_validate_contractor_authorization_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  expected_id text;
  category_value jsonb;
  origin_value jsonb;
BEGIN
  IF NOT oracle_jsonb_exact_keys(NEW.authorization_payload, ARRAY[
    'acquisitionMethod', 'approvedAt', 'approverReference',
    'authorizedSourceOrigins', 'categoryFilters', 'coverageGeography',
    'decision', 'policyVersion', 'provider', 'sourceClassification',
    'termsEvidenceFile'
  ]) OR NOT oracle_jsonb_exact_keys(
    NEW.authorization_payload->'termsEvidenceFile',
    ARRAY['byteSize', 'relativePath', 'sha256']
  ) OR jsonb_typeof(NEW.authorization_payload->'approvedAt') IS DISTINCT FROM
       'string'
    OR NOT oracle_contractor_iso_timestamp_valid(
      NEW.authorization_payload->>'approvedAt'
    )
    OR jsonb_typeof(NEW.authorization_payload->'authorizedSourceOrigins')
       IS DISTINCT FROM 'array'
    OR jsonb_typeof(NEW.authorization_payload->'categoryFilters')
       IS DISTINCT FROM 'array'
    OR jsonb_typeof(NEW.authorization_payload#>'{termsEvidenceFile,byteSize}')
       IS DISTINCT FROM 'number'
    OR jsonb_typeof(NEW.authorization_payload#>'{termsEvidenceFile,relativePath}')
       IS DISTINCT FROM 'string'
    OR jsonb_typeof(NEW.authorization_payload#>'{termsEvidenceFile,sha256}')
       IS DISTINCT FROM 'string'
    OR EXISTS (
      SELECT 1 FROM jsonb_each(NEW.authorization_payload) entry
      WHERE entry.key NOT IN (
        'approvedAt', 'authorizedSourceOrigins', 'categoryFilters',
        'termsEvidenceFile'
      ) AND jsonb_typeof(entry.value) IS DISTINCT FROM 'string'
    ) THEN
    RAISE EXCEPTION 'Contractor authorization payload shape is invalid';
  END IF;
  IF NEW.authorization_canonical_json IS DISTINCT FROM
       oracle_canonical_jsonb(NEW.authorization_payload)
    OR NEW.authorization_payload IS DISTINCT FROM
       NEW.authorization_canonical_json::jsonb
    OR NEW.authorization_sha256 IS DISTINCT FROM encode(
      sha256(convert_to(NEW.authorization_canonical_json, 'UTF8')), 'hex'
    ) THEN
    RAISE EXCEPTION 'Contractor authorization canonical identity mismatch';
  END IF;
  expected_id := 'contractorauthorization_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      'contractor-source-authorization-v1',
      NEW.provider,
      NEW.authorization_sha256
    ]::text[])), 'UTF8')), 'hex'), 1, 32);
  IF NEW.authorization_id IS DISTINCT FROM expected_id
    OR NEW.policy_version IS DISTINCT FROM
       NEW.authorization_payload->>'policyVersion'
    OR NEW.provider IS DISTINCT FROM NEW.authorization_payload->>'provider'
    OR NEW.source_classification IS DISTINCT FROM
       NEW.authorization_payload->>'sourceClassification'
    OR NEW.acquisition_method IS DISTINCT FROM
       NEW.authorization_payload->>'acquisitionMethod'
    OR NEW.approver_reference IS DISTINCT FROM
       NEW.authorization_payload->>'approverReference'
    OR NEW.approved_at IS DISTINCT FROM
       (NEW.authorization_payload->>'approvedAt')::timestamptz
    OR NEW.coverage_geography IS DISTINCT FROM
       NEW.authorization_payload->>'coverageGeography'
    OR NEW.category_filters IS DISTINCT FROM
       NEW.authorization_payload->'categoryFilters'
    OR NEW.authorized_source_origins IS DISTINCT FROM
       NEW.authorization_payload->'authorizedSourceOrigins'
    OR NEW.terms_evidence_relative_path IS DISTINCT FROM
       NEW.authorization_payload#>>'{termsEvidenceFile,relativePath}'
    OR NEW.terms_evidence_byte_size IS DISTINCT FROM
       (NEW.authorization_payload#>>'{termsEvidenceFile,byteSize}')::bigint
    OR NEW.terms_evidence_sha256 IS DISTINCT FROM
       NEW.authorization_payload#>>'{termsEvidenceFile,sha256}'
    OR NEW.authorization_payload->>'decision' IS DISTINCT FROM
       'approved_for_staging'
    OR (NEW.provider = 'better_business_bureau') IS DISTINCT FROM
       (NEW.source_classification = 'third_party') THEN
    RAISE EXCEPTION 'Contractor authorization binding is invalid';
  END IF;
  FOR category_value IN SELECT value FROM jsonb_array_elements(NEW.category_filters)
  LOOP
    IF jsonb_typeof(category_value) IS DISTINCT FROM 'string'
      OR length(category_value#>>'{}') NOT BETWEEN 1 AND 200 THEN
      RAISE EXCEPTION 'Contractor authorization category is invalid';
    END IF;
  END LOOP;
  FOR origin_value IN
    SELECT value FROM jsonb_array_elements(NEW.authorized_source_origins)
  LOOP
    IF jsonb_typeof(origin_value) IS DISTINCT FROM 'string'
      OR NOT oracle_contractor_https_origin_valid(origin_value#>>'{}') THEN
      RAISE EXCEPTION 'Contractor authorization source origin is invalid';
    END IF;
  END LOOP;
  IF (SELECT count(*) FROM jsonb_array_elements(NEW.authorized_source_origins))
       IS DISTINCT FROM
     (SELECT count(DISTINCT value)
      FROM jsonb_array_elements(NEW.authorized_source_origins)) THEN
    RAISE EXCEPTION 'Contractor authorization source origins must be distinct';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_contractor_source_authorizations_validate
BEFORE INSERT ON oracle_contractor_source_authorizations
FOR EACH ROW EXECUTE FUNCTION oracle_validate_contractor_authorization_v1();

CREATE TRIGGER oracle_contractor_source_authorizations_immutable
BEFORE UPDATE OR DELETE ON oracle_contractor_source_authorizations
FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation();

ALTER TABLE oracle_contractor_source_datasets
  ADD COLUMN manifest_version text NOT NULL DEFAULT '1.0.0' CHECK (
    manifest_version = '1.0.0'
  ),
  ADD COLUMN record_schema_version text NOT NULL DEFAULT '1.0.0' CHECK (
    record_schema_version = '1.0.0'
  ),
  ADD COLUMN manifest_created_at timestamptz NOT NULL,
  ADD COLUMN authorization_id text NOT NULL REFERENCES
    oracle_contractor_source_authorizations(authorization_id),
  ADD COLUMN source_urls jsonb NOT NULL,
  ADD COLUMN manifest_canonical_json text NOT NULL CHECK (
    length(manifest_canonical_json) BETWEEN 2 AND 1048576
  );

ALTER TABLE oracle_contractor_source_datasets
  ADD CONSTRAINT oracle_contractor_source_path_strict CHECK (
    oracle_contractor_relative_path_valid(source_file_relative_path)
  ),
  ADD CONSTRAINT oracle_contractor_terms_path_strict CHECK (
    oracle_contractor_relative_path_valid(license_terms_evidence_relative_path)
  ),
  ADD CONSTRAINT oracle_contractor_source_terms_distinct CHECK (
    source_file_relative_path IS DISTINCT FROM
      license_terms_evidence_relative_path
    AND source_file_sha256 IS DISTINCT FROM license_terms_evidence_sha256
  );

CREATE OR REPLACE FUNCTION oracle_validate_contractor_dataset_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  authorization_row oracle_contractor_source_authorizations%ROWTYPE;
  expected_id text;
  url_value jsonb;
BEGIN
  IF NOT oracle_jsonb_exact_keys(NEW.manifest_payload, ARRAY[
    'acquisitionMethod', 'authorizationId', 'categoryFilters', 'counts',
    'coverageGeography', 'coverageMode', 'createdAt', 'licenseTerms',
    'manifestVersion', 'observationWindow', 'parserVersion', 'provider',
    'recordSchemaVersion', 'retrieval', 'sourceClassification', 'sourceFile',
    'sourceUrls', 'transformVersion'
  ]) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload->'counts',
    ARRAY['accepted', 'duplicateProviderIds', 'parsed', 'rejected', 'source']
  ) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload->'licenseTerms',
    ARRAY['evidenceFile', 'evidenceSha256', 'status']
  ) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload#>'{licenseTerms,evidenceFile}',
    ARRAY['byteSize', 'relativePath', 'sha256']
  ) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload->'sourceFile',
    ARRAY['byteSize', 'relativePath', 'sha256']
  ) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload->'observationWindow',
    ARRAY['end', 'reason', 'start', 'status']
  ) OR NOT oracle_jsonb_exact_keys(
    NEW.manifest_payload->'retrieval', ARRAY['at', 'reason', 'status']
  ) OR jsonb_typeof(NEW.manifest_payload->'createdAt') IS DISTINCT FROM 'string'
    OR NOT oracle_contractor_iso_timestamp_valid(
      NEW.manifest_payload->>'createdAt'
    )
    OR jsonb_typeof(NEW.manifest_payload->'categoryFilters')
       IS DISTINCT FROM 'array'
    OR jsonb_typeof(NEW.manifest_payload->'sourceUrls')
       IS DISTINCT FROM 'array'
    OR EXISTS (
      SELECT 1 FROM jsonb_each(NEW.manifest_payload->'counts') entry
      WHERE jsonb_typeof(entry.value) IS DISTINCT FROM 'number'
    )
    OR jsonb_typeof(NEW.manifest_payload#>'{sourceFile,byteSize}')
       IS DISTINCT FROM 'number'
    OR jsonb_typeof(NEW.manifest_payload#>'{sourceFile,relativePath}')
       IS DISTINCT FROM 'string'
    OR jsonb_typeof(NEW.manifest_payload#>'{sourceFile,sha256}')
       IS DISTINCT FROM 'string'
    OR jsonb_typeof(
      NEW.manifest_payload#>'{licenseTerms,evidenceFile,byteSize}'
    ) IS DISTINCT FROM 'number'
    OR jsonb_typeof(
      NEW.manifest_payload#>'{licenseTerms,evidenceFile,relativePath}'
    ) IS DISTINCT FROM 'string'
    OR jsonb_typeof(
      NEW.manifest_payload#>'{licenseTerms,evidenceFile,sha256}'
    ) IS DISTINCT FROM 'string' THEN
    RAISE EXCEPTION 'Contractor dataset manifest shape is invalid';
  END IF;
  IF NEW.manifest_canonical_json IS DISTINCT FROM
       oracle_canonical_jsonb(NEW.manifest_payload)
    OR NEW.manifest_payload IS DISTINCT FROM NEW.manifest_canonical_json::jsonb
    OR NEW.manifest_sha256 IS DISTINCT FROM encode(
      sha256(convert_to(NEW.manifest_canonical_json, 'UTF8')), 'hex'
    ) THEN
    RAISE EXCEPTION 'Contractor dataset manifest canonical identity mismatch';
  END IF;
  expected_id := 'contractordataset_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      '1.0.0', NEW.provider, NEW.manifest_sha256
    ]::text[])), 'UTF8')), 'hex'), 1, 32);
  IF NEW.dataset_id IS DISTINCT FROM expected_id
    OR NEW.manifest_version IS DISTINCT FROM
       NEW.manifest_payload->>'manifestVersion'
    OR NEW.record_schema_version IS DISTINCT FROM
       NEW.manifest_payload->>'recordSchemaVersion'
    OR NEW.manifest_created_at IS DISTINCT FROM
       (NEW.manifest_payload->>'createdAt')::timestamptz
    OR NEW.authorization_id IS DISTINCT FROM
       NEW.manifest_payload->>'authorizationId'
    OR NEW.provider IS DISTINCT FROM NEW.manifest_payload->>'provider'
    OR NEW.source_classification IS DISTINCT FROM
       NEW.manifest_payload->>'sourceClassification'
    OR NEW.acquisition_method IS DISTINCT FROM
       NEW.manifest_payload->>'acquisitionMethod'
    OR NEW.coverage_mode IS DISTINCT FROM
       NEW.manifest_payload->>'coverageMode'
    OR NEW.coverage_geography IS DISTINCT FROM
       NEW.manifest_payload->>'coverageGeography'
    OR NEW.category_filters IS DISTINCT FROM
       NEW.manifest_payload->'categoryFilters'
    OR NEW.source_urls IS DISTINCT FROM NEW.manifest_payload->'sourceUrls'
    OR NEW.source_file_relative_path IS DISTINCT FROM
       NEW.manifest_payload#>>'{sourceFile,relativePath}'
    OR NEW.source_file_byte_size IS DISTINCT FROM
       (NEW.manifest_payload#>>'{sourceFile,byteSize}')::bigint
    OR NEW.source_file_sha256 IS DISTINCT FROM
       NEW.manifest_payload#>>'{sourceFile,sha256}'
    OR NEW.license_terms_status IS DISTINCT FROM
       NEW.manifest_payload#>>'{licenseTerms,status}'
    OR NEW.license_terms_evidence_sha256 IS DISTINCT FROM
       NEW.manifest_payload#>>'{licenseTerms,evidenceSha256}'
    OR NEW.license_terms_evidence_relative_path IS DISTINCT FROM
       NEW.manifest_payload#>>'{licenseTerms,evidenceFile,relativePath}'
    OR NEW.license_terms_evidence_byte_size IS DISTINCT FROM
       (NEW.manifest_payload#>>'{licenseTerms,evidenceFile,byteSize}')::bigint
    OR NEW.source_count IS DISTINCT FROM
       (NEW.manifest_payload#>>'{counts,source}')::integer
    OR NEW.parsed_count IS DISTINCT FROM
       (NEW.manifest_payload#>>'{counts,parsed}')::integer
    OR NEW.accepted_count IS DISTINCT FROM
       (NEW.manifest_payload#>>'{counts,accepted}')::integer
    OR NEW.rejected_count IS DISTINCT FROM
       (NEW.manifest_payload#>>'{counts,rejected}')::integer
    OR NEW.duplicate_count IS DISTINCT FROM
       (NEW.manifest_payload#>>'{counts,duplicateProviderIds}')::integer
    OR NEW.parser_version IS DISTINCT FROM
       NEW.manifest_payload->>'parserVersion'
    OR NEW.transform_version IS DISTINCT FROM
       NEW.manifest_payload->>'transformVersion'
    OR jsonb_typeof(NEW.source_urls) IS DISTINCT FROM 'array'
    OR jsonb_array_length(NEW.source_urls) NOT BETWEEN 1 AND 100
    OR NEW.source_count > 1000000 THEN
    RAISE EXCEPTION 'Contractor dataset manifest binding is invalid';
  END IF;
  IF NEW.observation_status = 'recorded' THEN
    IF NEW.manifest_payload#>>'{observationWindow,status}' IS DISTINCT FROM
         'recorded'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,start}')
         IS DISTINCT FROM 'string'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,end}')
         IS DISTINCT FROM 'string'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,reason}')
         IS DISTINCT FROM 'null'
      OR NOT oracle_contractor_iso_timestamp_valid(
        NEW.manifest_payload#>>'{observationWindow,start}'
      )
      OR NOT oracle_contractor_iso_timestamp_valid(
        NEW.manifest_payload#>>'{observationWindow,end}'
      )
      OR NEW.observation_start IS DISTINCT FROM
         (NEW.manifest_payload#>>'{observationWindow,start}')::timestamptz
      OR NEW.observation_end IS DISTINCT FROM
         (NEW.manifest_payload#>>'{observationWindow,end}')::timestamptz
      OR NEW.observation_unavailable_reason IS NOT NULL THEN
      RAISE EXCEPTION 'Contractor dataset observation binding is invalid';
    END IF;
  ELSIF NEW.observation_status = 'unavailable' THEN
    IF NEW.manifest_payload#>>'{observationWindow,status}' IS DISTINCT FROM
         'unavailable'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,start}')
         IS DISTINCT FROM 'null'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,end}')
         IS DISTINCT FROM 'null'
      OR jsonb_typeof(NEW.manifest_payload#>'{observationWindow,reason}')
         IS DISTINCT FROM 'string'
      OR NEW.observation_start IS NOT NULL
      OR NEW.observation_end IS NOT NULL
      OR NEW.observation_unavailable_reason IS DISTINCT FROM
         NEW.manifest_payload#>>'{observationWindow,reason}' THEN
      RAISE EXCEPTION 'Contractor dataset observation binding is invalid';
    END IF;
  ELSE
    RAISE EXCEPTION 'Contractor dataset observation status is invalid';
  END IF;
  IF NEW.retrieval_status = 'recorded' THEN
    IF NEW.manifest_payload#>>'{retrieval,status}' IS DISTINCT FROM 'recorded'
      OR jsonb_typeof(NEW.manifest_payload#>'{retrieval,at}')
         IS DISTINCT FROM 'string'
      OR jsonb_typeof(NEW.manifest_payload#>'{retrieval,reason}')
         IS DISTINCT FROM 'null'
      OR NOT oracle_contractor_iso_timestamp_valid(
        NEW.manifest_payload#>>'{retrieval,at}'
      )
      OR NEW.retrieved_at IS DISTINCT FROM
         (NEW.manifest_payload#>>'{retrieval,at}')::timestamptz
      OR NEW.retrieval_unavailable_reason IS NOT NULL THEN
      RAISE EXCEPTION 'Contractor dataset retrieval binding is invalid';
    END IF;
  ELSIF NEW.retrieval_status = 'unavailable' THEN
    IF NEW.manifest_payload#>>'{retrieval,status}' IS DISTINCT FROM 'unavailable'
      OR jsonb_typeof(NEW.manifest_payload#>'{retrieval,at}')
         IS DISTINCT FROM 'null'
      OR jsonb_typeof(NEW.manifest_payload#>'{retrieval,reason}')
         IS DISTINCT FROM 'string'
      OR NEW.retrieved_at IS NOT NULL
      OR NEW.retrieval_unavailable_reason IS DISTINCT FROM
         NEW.manifest_payload#>>'{retrieval,reason}' THEN
      RAISE EXCEPTION 'Contractor dataset retrieval binding is invalid';
    END IF;
  ELSE
    RAISE EXCEPTION 'Contractor dataset retrieval status is invalid';
  END IF;
  FOR url_value IN SELECT value FROM jsonb_array_elements(NEW.source_urls)
  LOOP
    IF jsonb_typeof(url_value) IS DISTINCT FROM 'string'
      OR NOT oracle_contractor_https_url_valid(url_value#>>'{}') THEN
      RAISE EXCEPTION 'Contractor dataset source URL is invalid';
    END IF;
  END LOOP;
  SELECT * INTO authorization_row
  FROM oracle_contractor_source_authorizations
  WHERE authorization_id = NEW.authorization_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Contractor dataset authorization binding is invalid';
  END IF;
  IF authorization_row.provider IS DISTINCT FROM NEW.provider
    OR authorization_row.source_classification IS DISTINCT FROM
       NEW.source_classification
    OR authorization_row.acquisition_method IS DISTINCT FROM NEW.acquisition_method
    OR authorization_row.coverage_geography IS DISTINCT FROM NEW.coverage_geography
    OR authorization_row.category_filters IS DISTINCT FROM NEW.category_filters
    OR authorization_row.terms_evidence_relative_path IS DISTINCT FROM
       NEW.license_terms_evidence_relative_path
    OR authorization_row.terms_evidence_byte_size IS DISTINCT FROM
       NEW.license_terms_evidence_byte_size
    OR authorization_row.terms_evidence_sha256 IS DISTINCT FROM
       NEW.license_terms_evidence_sha256 THEN
    RAISE EXCEPTION 'Contractor dataset authorization binding is invalid';
  END IF;
  FOR url_value IN SELECT value FROM jsonb_array_elements(NEW.source_urls)
  LOOP
    IF NOT (
      authorization_row.authorized_source_origins ?
      oracle_contractor_https_origin(url_value#>>'{}')
    ) THEN
      RAISE EXCEPTION 'Contractor dataset source origin is not authorized';
    END IF;
  END LOOP;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_contractor_source_datasets_validate_v1
BEFORE INSERT ON oracle_contractor_source_datasets
FOR EACH ROW EXECUTE FUNCTION oracle_validate_contractor_dataset_v1();

ALTER TABLE oracle_contractor_source_record_versions
  ADD COLUMN source_url text NOT NULL CHECK (
    oracle_contractor_https_url_valid(source_url)
  ),
  ADD COLUMN source_schema_version text NOT NULL DEFAULT '1.0.0' CHECK (
    source_schema_version = '1.0.0'
  ),
  ADD COLUMN source_payload_canonical_json text NOT NULL CHECK (
    length(source_payload_canonical_json) BETWEEN 2 AND 131072
  );

CREATE OR REPLACE FUNCTION oracle_validate_contractor_record_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  dataset_provider text;
  dataset_source_urls jsonb;
  expected_id text;
  category_value jsonb;
BEGIN
  IF NOT oracle_jsonb_exact_keys(NEW.source_payload, ARRAY[
    'accredited', 'businessAddress', 'businessName', 'categories',
    'licenseIssuer', 'licenseJurisdiction', 'licenseNumber', 'phone',
    'provider', 'providerRecordId', 'rating', 'schemaVersion', 'sourceUrl'
  ]) OR jsonb_typeof(NEW.source_payload->'accredited') NOT IN ('boolean', 'null')
    OR jsonb_typeof(NEW.source_payload->'businessAddress') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'businessName') IS DISTINCT FROM 'string'
    OR jsonb_typeof(NEW.source_payload->'categories') IS DISTINCT FROM 'array'
    OR jsonb_typeof(NEW.source_payload->'licenseIssuer') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'licenseJurisdiction') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'licenseNumber') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'phone') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'provider') IS DISTINCT FROM 'string'
    OR jsonb_typeof(NEW.source_payload->'providerRecordId') IS DISTINCT FROM
       'string'
    OR jsonb_typeof(NEW.source_payload->'rating') NOT IN ('string', 'null')
    OR jsonb_typeof(NEW.source_payload->'schemaVersion') IS DISTINCT FROM
       'string'
    OR jsonb_typeof(NEW.source_payload->'sourceUrl') IS DISTINCT FROM 'string'
    OR char_length(NEW.source_payload->>'providerRecordId') NOT BETWEEN 1 AND 256
    OR char_length(NEW.source_payload->>'businessName') NOT BETWEEN 1 AND 500
    OR (jsonb_typeof(NEW.source_payload->'businessAddress') = 'string'
      AND char_length(NEW.source_payload->>'businessAddress') NOT BETWEEN 1 AND 1000)
    OR (jsonb_typeof(NEW.source_payload->'licenseIssuer') = 'string'
      AND char_length(NEW.source_payload->>'licenseIssuer') NOT BETWEEN 1 AND 300)
    OR (jsonb_typeof(NEW.source_payload->'licenseJurisdiction') = 'string'
      AND char_length(NEW.source_payload->>'licenseJurisdiction') NOT BETWEEN 1 AND 200)
    OR (jsonb_typeof(NEW.source_payload->'licenseNumber') = 'string'
      AND char_length(NEW.source_payload->>'licenseNumber') NOT BETWEEN 1 AND 200)
    OR (jsonb_typeof(NEW.source_payload->'phone') = 'string'
      AND char_length(NEW.source_payload->>'phone') NOT BETWEEN 1 AND 100)
    OR (jsonb_typeof(NEW.source_payload->'rating') = 'string'
      AND char_length(NEW.source_payload->>'rating') NOT BETWEEN 1 AND 100)
    OR jsonb_array_length(NEW.source_payload->'categories') > 100 THEN
    RAISE EXCEPTION 'Contractor source record payload shape is invalid';
  END IF;
  FOR category_value IN
    SELECT value FROM jsonb_array_elements(NEW.source_payload->'categories')
  LOOP
    IF jsonb_typeof(category_value) IS DISTINCT FROM 'string'
      OR char_length(category_value#>>'{}') NOT BETWEEN 1 AND 200 THEN
      RAISE EXCEPTION 'Contractor source record category is invalid';
    END IF;
  END LOOP;
  expected_id := 'contractorversion_' || substr(encode(sha256(convert_to(
    oracle_canonical_jsonb(to_jsonb(ARRAY[
      '1.0.0', NEW.dataset_id, NEW.provider, NEW.provider_record_id,
      NEW.source_record_sha256
    ]::text[])), 'UTF8')), 'hex'), 1, 32);
  IF NEW.record_version_id IS DISTINCT FROM expected_id
    OR NEW.source_payload_canonical_json IS DISTINCT FROM
       oracle_canonical_jsonb(NEW.source_payload)
    OR NEW.source_payload IS DISTINCT FROM NEW.source_payload_canonical_json::jsonb
    OR NEW.source_record_sha256 IS DISTINCT FROM encode(
      sha256(convert_to(NEW.source_payload_canonical_json, 'UTF8')), 'hex'
    ) OR NEW.provider IS DISTINCT FROM NEW.source_payload->>'provider'
    OR NEW.provider_record_id IS DISTINCT FROM
       NEW.source_payload->>'providerRecordId'
    OR NEW.legal_business_name IS DISTINCT FROM
       NEW.source_payload->>'businessName'
    OR NEW.business_address IS DISTINCT FROM
       NEW.source_payload->>'businessAddress'
    OR NEW.phone IS DISTINCT FROM NEW.source_payload->>'phone'
    OR NEW.license_number IS DISTINCT FROM NEW.source_payload->>'licenseNumber'
    OR NEW.license_issuer IS DISTINCT FROM NEW.source_payload->>'licenseIssuer'
    OR NEW.license_jurisdiction IS DISTINCT FROM
       NEW.source_payload->>'licenseJurisdiction'
    OR NEW.source_schema_version IS DISTINCT FROM
       NEW.source_payload->>'schemaVersion'
    OR NEW.source_url IS DISTINCT FROM NEW.source_payload->>'sourceUrl'
    OR NOT oracle_contractor_https_url_valid(NEW.source_url) THEN
    RAISE EXCEPTION 'Contractor source record binding is invalid';
  END IF;
  SELECT provider, source_urls
    INTO STRICT dataset_provider, dataset_source_urls
  FROM oracle_contractor_source_datasets
  WHERE dataset_id = NEW.dataset_id;
  IF dataset_provider IS DISTINCT FROM NEW.provider
    OR NOT EXISTS (
      SELECT 1 FROM jsonb_array_elements_text(dataset_source_urls) url
      WHERE oracle_contractor_https_origin(url) =
            oracle_contractor_https_origin(NEW.source_url)
    ) THEN
    RAISE EXCEPTION 'Contractor source record is outside its dataset origin';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_contractor_source_record_versions_validate_v1
BEFORE INSERT ON oracle_contractor_source_record_versions
FOR EACH ROW EXECUTE FUNCTION oracle_validate_contractor_record_v1();

ALTER TABLE oracle_contractor_identity_matches
  ADD COLUMN match_version text NOT NULL DEFAULT
    'contractor-identity-match-v1' CHECK (
      match_version = 'contractor-identity-match-v1'
    ),
  ADD COLUMN source_identity_sha256 text NOT NULL CHECK (
    source_identity_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD COLUMN candidate_identity_sha256 text CHECK (
    candidate_identity_sha256 IS NULL
    OR candidate_identity_sha256 ~ '^sha256:[a-f0-9]{64}$'
  ),
  ADD COLUMN evidence_canonical_json text NOT NULL CHECK (
    length(evidence_canonical_json) BETWEEN 2 AND 131072
  );

CREATE OR REPLACE FUNCTION oracle_validate_contractor_match_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  source_hash text;
BEGIN
  IF NEW.status = 'linked'
    OR NEW.matched_contractor_id IS NOT NULL
    OR NEW.candidate_identity_sha256 IS NOT NULL THEN
    RAISE EXCEPTION
      'Normalized contractor linkage is disabled pending immutable target versions';
  END IF;
  SELECT source_record_sha256 INTO STRICT source_hash
  FROM oracle_contractor_source_record_versions
  WHERE record_version_id = NEW.record_version_id;
  IF NOT oracle_jsonb_exact_keys(NEW.evidence_payload, ARRAY[
    'candidateSourceRecordSha256', 'confidence', 'matchVersion',
    'matchedContractorId', 'method', 'recordVersionId',
    'sourceRecordSha256', 'status'
  ]) OR NEW.evidence_canonical_json IS DISTINCT FROM
       oracle_canonical_jsonb(NEW.evidence_payload)
    OR NEW.evidence_payload IS DISTINCT FROM NEW.evidence_canonical_json::jsonb
    OR NEW.evidence_sha256 IS DISTINCT FROM encode(
      sha256(convert_to(NEW.evidence_canonical_json, 'UTF8')), 'hex'
    ) OR NEW.match_version IS DISTINCT FROM
       NEW.evidence_payload->>'matchVersion'
    OR NEW.record_version_id IS DISTINCT FROM
       NEW.evidence_payload->>'recordVersionId'
    OR NEW.matched_contractor_id IS DISTINCT FROM
       NEW.evidence_payload->>'matchedContractorId'
    OR NEW.status IS DISTINCT FROM NEW.evidence_payload->>'status'
    OR NEW.method IS DISTINCT FROM NEW.evidence_payload->>'method'
    OR NEW.confidence IS DISTINCT FROM
       (NEW.evidence_payload->>'confidence')::numeric
    OR NEW.source_identity_sha256 IS DISTINCT FROM source_hash
    OR NEW.source_identity_sha256 IS DISTINCT FROM
       NEW.evidence_payload->>'sourceRecordSha256'
    OR NEW.candidate_identity_sha256 IS DISTINCT FROM
       NEW.evidence_payload->>'candidateSourceRecordSha256'
    OR (NEW.status = 'linked') IS DISTINCT FROM
       (NEW.matched_contractor_id IS NOT NULL) THEN
    RAISE EXCEPTION 'Contractor match evidence binding is invalid';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER oracle_contractor_identity_matches_validate_v1
BEFORE INSERT ON oracle_contractor_identity_matches
FOR EACH ROW EXECUTE FUNCTION oracle_validate_contractor_match_v1();

ALTER TABLE oracle_permit_contractors
  ADD COLUMN property_id text NOT NULL REFERENCES oracle_properties(property_id),
  ADD COLUMN relationship_record_id text NOT NULL CHECK (
    length(relationship_record_id) BETWEEN 1 AND 500
  ),
  ADD COLUMN relationship_source_system text NOT NULL CHECK (
    relationship_source_system = 'official_permit_source'
  ),
  ADD COLUMN relationship_source_sha256 text NOT NULL CHECK (
    relationship_source_sha256 ~ '^sha256:[a-f0-9]{64}$'
  ),
  ADD COLUMN relationship_evidence_version text NOT NULL CHECK (
    relationship_evidence_version = 'permit-contractor-relationship-v1'
  ),
  ADD COLUMN relationship_evidence_sha256 text NOT NULL CHECK (
    relationship_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  ADD COLUMN relationship_evidence_canonical_json text NOT NULL CHECK (
    length(relationship_evidence_canonical_json) BETWEEN 2 AND 131072
  ),
  ADD CONSTRAINT oracle_permit_contractor_match_basis_closed CHECK (
    match_basis IN (
      'permit_source_contractor_id', 'permit_source_license_number',
      'permit_source_legal_name'
    ) AND match_confidence >= 0.95
  );

CREATE OR REPLACE FUNCTION oracle_validate_permit_contractor_relationship_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION
    'Permit-contractor linkage is disabled pending immutable source versions';
END;
$$;

CREATE TRIGGER oracle_permit_contractors_validate_v1
BEFORE INSERT ON oracle_permit_contractors
FOR EACH ROW EXECUTE FUNCTION oracle_validate_permit_contractor_relationship_v1();

CREATE TRIGGER oracle_permit_contractors_immutable
BEFORE UPDATE OR DELETE ON oracle_permit_contractors
FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation();
