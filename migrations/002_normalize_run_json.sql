UPDATE oracle_pipeline_runs
SET source_counts = (source_counts #>> '{}')::jsonb
WHERE jsonb_typeof(source_counts) = 'string';

UPDATE oracle_pipeline_runs
SET result_counts = (result_counts #>> '{}')::jsonb
WHERE jsonb_typeof(result_counts) = 'string';

UPDATE oracle_pipeline_runs
SET limitations = (limitations #>> '{}')::jsonb
WHERE jsonb_typeof(limitations) = 'string';
