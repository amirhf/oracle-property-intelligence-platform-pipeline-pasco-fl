-- Immutable staging/provenance for separately scoped contractor-directory
-- sources. No source is seeded by this migration and no parcel relationship is
-- inferred from directory or BBB proximity/presence.

CREATE TABLE IF NOT EXISTS oracle_contractor_source_datasets (
  dataset_id text PRIMARY KEY CHECK (
    dataset_id ~ '^contractordataset_[a-f0-9]{32}$'
  ),
  source_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
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
  coverage_mode text NOT NULL CHECK (coverage_mode = 'partial'),
  coverage_geography text NOT NULL,
  category_filters jsonb NOT NULL,
  license_terms_status text NOT NULL CHECK (
    license_terms_status = 'verified_compatible'
  ),
  license_terms_evidence_sha256 text NOT NULL CHECK (
    license_terms_evidence_sha256 ~ '^[a-f0-9]{64}$'
  ),
  license_terms_evidence_relative_path text NOT NULL,
  license_terms_evidence_byte_size bigint NOT NULL CHECK (
    license_terms_evidence_byte_size BETWEEN 1 AND 10485760
  ),
  source_file_relative_path text NOT NULL,
  source_file_byte_size bigint NOT NULL CHECK (
    source_file_byte_size BETWEEN 0 AND 536870912
  ),
  source_file_sha256 text NOT NULL CHECK (
    source_file_sha256 ~ '^[a-f0-9]{64}$'
  ),
  observation_status text NOT NULL CHECK (
    observation_status IN ('recorded', 'unavailable')
  ),
  observation_start timestamptz,
  observation_end timestamptz,
  observation_unavailable_reason text CHECK (
    observation_unavailable_reason IN (
      'not_provided_by_source', 'not_recorded_during_acquisition'
    )
  ),
  retrieval_status text NOT NULL CHECK (
    retrieval_status IN ('recorded', 'unavailable')
  ),
  retrieved_at timestamptz,
  retrieval_unavailable_reason text CHECK (
    retrieval_unavailable_reason IN (
      'not_provided_by_source', 'not_recorded_during_acquisition'
    )
  ),
  parser_version text NOT NULL CHECK (parser_version = 'contractor-jsonl-v1'),
  transform_version text NOT NULL CHECK (
    transform_version = 'contractor-identity-match-v1'
  ),
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
  manifest_payload jsonb NOT NULL,
  source_count integer NOT NULL CHECK (source_count >= 0),
  parsed_count integer NOT NULL CHECK (parsed_count >= 0),
  accepted_count integer NOT NULL CHECK (accepted_count >= 0),
  rejected_count integer NOT NULL CHECK (rejected_count >= 0),
  duplicate_count integer NOT NULL CHECK (duplicate_count >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (observation_status = 'recorded'
      AND observation_start IS NOT NULL
      AND observation_end IS NOT NULL
      AND observation_end >= observation_start
      AND observation_unavailable_reason IS NULL)
    OR (observation_status = 'unavailable'
      AND observation_start IS NULL
      AND observation_end IS NULL
      AND observation_unavailable_reason IS NOT NULL)
  ),
  CHECK (
    (retrieval_status = 'recorded'
      AND retrieved_at IS NOT NULL
      AND retrieval_unavailable_reason IS NULL)
    OR (retrieval_status = 'unavailable'
      AND retrieved_at IS NULL
      AND retrieval_unavailable_reason IS NOT NULL)
  ),
  CHECK (
    source_file_relative_path !~ '(^|/)\.\.?($|/)'
    AND source_file_relative_path !~ '^/'
    AND source_file_relative_path !~ '\\\\'
    AND source_file_relative_path NOT LIKE '%//%'
  ),
  CHECK (
    license_terms_evidence_relative_path !~ '(^|/)\.\.?($|/)'
    AND license_terms_evidence_relative_path !~ '^/'
    AND license_terms_evidence_relative_path !~ '\\\\'
    AND license_terms_evidence_relative_path NOT LIKE '%//%'
  ),
  CHECK (
    (
      jsonb_typeof(manifest_payload) IS NOT DISTINCT FROM 'object'
      AND manifest_payload ? 'licenseTerms'
      AND jsonb_typeof(manifest_payload -> 'licenseTerms')
        IS NOT DISTINCT FROM 'object'
      AND (manifest_payload -> 'licenseTerms') ? 'evidenceSha256'
      AND jsonb_typeof(manifest_payload -> 'licenseTerms' -> 'evidenceSha256')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload #>> '{licenseTerms,evidenceSha256}')
        IS NOT DISTINCT FROM license_terms_evidence_sha256
      AND (manifest_payload -> 'licenseTerms') ? 'evidenceFile'
      AND jsonb_typeof(manifest_payload -> 'licenseTerms' -> 'evidenceFile')
        IS NOT DISTINCT FROM 'object'
      AND (manifest_payload #> '{licenseTerms,evidenceFile}') ? 'sha256'
      AND jsonb_typeof(
        manifest_payload #> '{licenseTerms,evidenceFile,sha256}'
      ) IS NOT DISTINCT FROM 'string'
      AND (manifest_payload #>> '{licenseTerms,evidenceFile,sha256}')
        IS NOT DISTINCT FROM license_terms_evidence_sha256
      AND (manifest_payload #> '{licenseTerms,evidenceFile}') ? 'relativePath'
      AND jsonb_typeof(
        manifest_payload #> '{licenseTerms,evidenceFile,relativePath}'
      ) IS NOT DISTINCT FROM 'string'
      AND (manifest_payload #>> '{licenseTerms,evidenceFile,relativePath}')
        IS NOT DISTINCT FROM license_terms_evidence_relative_path
      AND (manifest_payload #> '{licenseTerms,evidenceFile}') ? 'byteSize'
      AND jsonb_typeof(
        manifest_payload #> '{licenseTerms,evidenceFile,byteSize}'
      ) IS NOT DISTINCT FROM 'number'
      AND (manifest_payload #>> '{licenseTerms,evidenceFile,byteSize}')
        IS NOT DISTINCT FROM license_terms_evidence_byte_size::text
    ) IS TRUE
  ),
  CHECK (source_count = accepted_count + rejected_count),
  CHECK (parsed_count <= source_count),
  CHECK (accepted_count <= parsed_count),
  CHECK (duplicate_count <= rejected_count),
  CHECK (accepted_count + duplicate_count <= parsed_count),
  CHECK (
    (provider = 'better_business_bureau'
      AND source_classification = 'third_party')
    OR (provider = 'official_license_source'
      AND source_classification = 'official')
  ),
  CHECK (
    (
      manifest_payload ? 'sourceFile'
      AND jsonb_typeof(manifest_payload -> 'sourceFile')
        IS NOT DISTINCT FROM 'object'
      AND (manifest_payload -> 'sourceFile') ? 'relativePath'
      AND jsonb_typeof(manifest_payload -> 'sourceFile' -> 'relativePath')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload -> 'sourceFile' ->> 'relativePath')
        IS NOT DISTINCT FROM source_file_relative_path
      AND (manifest_payload -> 'sourceFile') ? 'sha256'
      AND jsonb_typeof(manifest_payload -> 'sourceFile' -> 'sha256')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload -> 'sourceFile' ->> 'sha256')
        IS NOT DISTINCT FROM source_file_sha256
      AND (manifest_payload -> 'sourceFile') ? 'byteSize'
      AND jsonb_typeof(manifest_payload -> 'sourceFile' -> 'byteSize')
        IS NOT DISTINCT FROM 'number'
      AND (manifest_payload -> 'sourceFile' ->> 'byteSize')
        IS NOT DISTINCT FROM source_file_byte_size::text
    ) IS TRUE
  ),
  CHECK (
    (
      manifest_payload ? 'provider'
      AND jsonb_typeof(manifest_payload -> 'provider')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload ->> 'provider') IS NOT DISTINCT FROM provider
      AND manifest_payload ? 'sourceClassification'
      AND jsonb_typeof(manifest_payload -> 'sourceClassification')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload ->> 'sourceClassification')
        IS NOT DISTINCT FROM source_classification
      AND manifest_payload ? 'coverageMode'
      AND jsonb_typeof(manifest_payload -> 'coverageMode')
        IS NOT DISTINCT FROM 'string'
      AND (manifest_payload ->> 'coverageMode')
        IS NOT DISTINCT FROM coverage_mode
    ) IS TRUE
  ),
  UNIQUE (dataset_id, provider),
  UNIQUE (provider, source_file_sha256)
);

CREATE TABLE IF NOT EXISTS oracle_contractor_source_record_versions (
  record_version_id text PRIMARY KEY CHECK (
    record_version_id ~ '^contractorversion_[a-f0-9]{32}$'
  ),
  dataset_id text NOT NULL
    REFERENCES oracle_contractor_source_datasets(dataset_id),
  provider text NOT NULL CHECK (
    provider IN ('better_business_bureau', 'official_license_source')
  ),
  provider_record_id text NOT NULL,
  legal_business_name text NOT NULL,
  license_number text,
  license_issuer text,
  license_jurisdiction text,
  business_address text,
  phone text,
  source_record_sha256 text NOT NULL CHECK (
    source_record_sha256 ~ '^[a-f0-9]{64}$'
  ),
  source_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (license_number IS NULL
      AND license_issuer IS NULL
      AND license_jurisdiction IS NULL)
    OR (license_number IS NOT NULL
      AND license_issuer IS NOT NULL
      AND license_jurisdiction IS NOT NULL)
  ),
  CHECK (provider <> 'official_license_source' OR license_number IS NOT NULL),
  CHECK (
    (
      jsonb_typeof(source_payload) IS NOT DISTINCT FROM 'object'
      AND source_payload ? 'provider'
      AND jsonb_typeof(source_payload -> 'provider')
        IS NOT DISTINCT FROM 'string'
      AND (source_payload ->> 'provider') IS NOT DISTINCT FROM provider
      AND source_payload ? 'providerRecordId'
      AND jsonb_typeof(source_payload -> 'providerRecordId')
        IS NOT DISTINCT FROM 'string'
      AND (source_payload ->> 'providerRecordId')
        IS NOT DISTINCT FROM provider_record_id
    ) IS TRUE
  ),
  CHECK (
    (
      source_payload ? 'licenseNumber'
      AND (
        (license_number IS NULL
          AND jsonb_typeof(source_payload -> 'licenseNumber')
            IS NOT DISTINCT FROM 'null')
        OR (license_number IS NOT NULL
          AND jsonb_typeof(source_payload -> 'licenseNumber')
            IS NOT DISTINCT FROM 'string'
          AND (source_payload ->> 'licenseNumber')
            IS NOT DISTINCT FROM license_number)
      )
    ) IS TRUE
  ),
  CHECK (
    (
      source_payload ? 'licenseIssuer'
      AND (
        (license_issuer IS NULL
          AND jsonb_typeof(source_payload -> 'licenseIssuer')
            IS NOT DISTINCT FROM 'null')
        OR (license_issuer IS NOT NULL
          AND jsonb_typeof(source_payload -> 'licenseIssuer')
            IS NOT DISTINCT FROM 'string'
          AND (source_payload ->> 'licenseIssuer')
            IS NOT DISTINCT FROM license_issuer)
      )
    ) IS TRUE
  ),
  CHECK (
    (
      source_payload ? 'licenseJurisdiction'
      AND (
        (license_jurisdiction IS NULL
          AND jsonb_typeof(source_payload -> 'licenseJurisdiction')
            IS NOT DISTINCT FROM 'null')
        OR (license_jurisdiction IS NOT NULL
          AND jsonb_typeof(source_payload -> 'licenseJurisdiction')
            IS NOT DISTINCT FROM 'string'
          AND (source_payload ->> 'licenseJurisdiction')
            IS NOT DISTINCT FROM license_jurisdiction)
      )
    ) IS TRUE
  ),
  FOREIGN KEY (dataset_id, provider)
    REFERENCES oracle_contractor_source_datasets(dataset_id, provider),
  UNIQUE (dataset_id, provider_record_id)
);

CREATE TABLE IF NOT EXISTS oracle_contractor_identity_matches (
  match_id text PRIMARY KEY CHECK (match_id ~ '^contractormatch_[a-f0-9]{32}$'),
  record_version_id text NOT NULL UNIQUE
    REFERENCES oracle_contractor_source_record_versions(record_version_id),
  matched_contractor_id text REFERENCES oracle_contractors(contractor_id),
  status text NOT NULL CHECK (
    status IN ('linked', 'ambiguous', 'unmatched')
  ),
  method text NOT NULL CHECK (
    method IN (
      'exact_license_number', 'exact_provider_identifier',
      'legal_name_address_phone', 'name_only_ambiguous', 'no_match'
    )
  ),
  confidence numeric NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  evidence_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (status = 'linked' AND matched_contractor_id IS NOT NULL
      AND method IN (
        'exact_license_number', 'exact_provider_identifier',
        'legal_name_address_phone'
      ) AND confidence >= 0.95)
    OR (status = 'ambiguous' AND matched_contractor_id IS NULL
      AND method = 'name_only_ambiguous')
    OR (status = 'unmatched' AND matched_contractor_id IS NULL
      AND method = 'no_match' AND confidence = 0)
  )
);

DO $$
DECLARE immutable_table text;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'oracle_contractor_source_datasets',
    'oracle_contractor_source_record_versions',
    'oracle_contractor_identity_matches'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I_immutable ON %I',
      immutable_table, immutable_table
    );
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION oracle_reject_immutable_publication_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END;
$$;
