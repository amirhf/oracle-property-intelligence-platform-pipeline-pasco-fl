CREATE TABLE IF NOT EXISTS oracle_property_availability (
  property_id text NOT NULL REFERENCES oracle_properties(property_id) ON DELETE CASCADE,
  feature text NOT NULL CHECK (feature IN ('permits', 'contractors', 'phones', 'emails', 'sunbiz', 'bbb')),
  availability text NOT NULL CHECK (availability = 'unavailable'),
  reason text NOT NULL CHECK (reason IN (
    'not_provided_by_source',
    'source_not_collected',
    'source_unavailable',
    'not_applicable',
    'ambiguous_match'
  )),
  first_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  last_seen_run_id text NOT NULL REFERENCES oracle_pipeline_runs(run_id),
  PRIMARY KEY (property_id, feature)
);

INSERT INTO oracle_property_availability (
  property_id, feature, availability, reason,
  first_seen_run_id, last_seen_run_id
)
SELECT
  property.property_id,
  unavailable.feature,
  'unavailable',
  unavailable.reason,
  property.first_seen_run_id,
  property.last_seen_run_id
FROM oracle_properties AS property
CROSS JOIN (
  VALUES
    ('permits', 'source_unavailable'),
    ('contractors', 'source_unavailable'),
    ('phones', 'not_provided_by_source'),
    ('emails', 'not_provided_by_source'),
    ('sunbiz', 'source_not_collected'),
    ('bbb', 'source_not_collected')
) AS unavailable(feature, reason)
ON CONFLICT (property_id, feature) DO NOTHING;
