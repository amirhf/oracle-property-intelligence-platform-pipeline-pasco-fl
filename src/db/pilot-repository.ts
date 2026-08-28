import { pathToFileURL } from "node:url";

import postgres from "postgres";

import type {
  PilotRunRequest,
  PilotRunSummary,
  PreparedPilot,
} from "../domain/types.js";
import {
  normalizePermitStatus,
  wholeUtcDays,
  yearBuiltRoofProxy,
} from "../domain/signals.js";
import { classifyHashDelta } from "../domain/reconciliation.js";
import {
  deterministicId,
  parcelId,
  permitId,
  sourceRecordHash,
} from "../lib/hash.js";

const PIPELINE_LIMITATIONS = [
  "Pilot only: 25 deterministically selected properties, not countywide coverage.",
  "Pasco Accela coverage can exclude incorporated-city permit systems.",
  "Pasco Accela collection stopped after challenge/CAPTCHA content was detected; permit search results and contractor identity are unavailable.",
  "Sunbiz and BBB are intentionally not collected in this checkpoint.",
] as const;

function parseUsDate(value: string | null): string | null {
  if (!value) return null;
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value.trim());
  if (!match) return null;
  const [, month, day, year] = match;
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  if (Number.isNaN(date.valueOf())) return null;
  return date.toISOString().slice(0, 10);
}

export async function recordRunStarted(
  databaseUrl: string,
  request: PilotRunRequest,
): Promise<void> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql`
      INSERT INTO oracle_pipeline_runs (
        run_id, workflow_id, county, sample_algorithm, sample_seed,
        window_start, window_end, as_of, status, limitations
      ) VALUES (
        ${request.runId}, ${request.workflowId}, 'pasco',
        'pasco-pilot-stratified-v1', ${request.sampleSeed},
        '2026-08-23T00:00:00.000Z', '2026-08-23T23:59:59.999Z',
        ${request.asOf}, 'running', ${sql.json([...PIPELINE_LIMITATIONS])}
      )
      ON CONFLICT (run_id) DO NOTHING
    `;
    const attemptId = deterministicId("attempt", [
      "1.0.0",
      "attempt",
      request.runId,
      "CountyIngest",
      "run",
      "1",
    ]);
    await sql`
      INSERT INTO oracle_pipeline_attempts (
        attempt_id, run_id, service_name, handler_name, attempt_number, status
      ) VALUES (${attemptId}, ${request.runId}, 'CountyIngest', 'run', 1, 'running')
      ON CONFLICT (attempt_id) DO NOTHING
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function markRunFailed(
  databaseUrl: string,
  runId: string,
  errorCode: string,
): Promise<void> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql`
      UPDATE oracle_pipeline_runs
      SET status = 'failed', completed_at = now()
      WHERE run_id = ${runId}
    `;
    await sql`
      UPDATE oracle_pipeline_attempts
      SET status = 'failed', completed_at = now(), error_code = ${errorCode}
      WHERE run_id = ${runId} AND status = 'running'
    `;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function loadPreparedPilot(
  databaseUrl: string,
  request: PilotRunRequest,
  prepared: PreparedPilot,
): Promise<PilotRunSummary> {
  if (prepared.properties.length !== 25) {
    throw new Error(`Pilot load requires exactly 25 properties`);
  }
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const attemptId = deterministicId("attempt", [
        "1.0.0",
        "attempt",
        request.runId,
        "Loader",
        "load",
        "1",
      ]);
      await transaction`
        INSERT INTO oracle_pipeline_attempts (
          attempt_id, run_id, service_name, handler_name, attempt_number, status
        ) VALUES (${attemptId}, ${request.runId}, 'Loader', 'load', 1, 'running')
        ON CONFLICT (attempt_id) DO NOTHING
      `;

      for (const artifact of prepared.artifacts) {
        const artifactId = deterministicId("artifact", [
          "1.0.0",
          "source-artifact",
          request.runId,
          artifact.sourceSystem,
          artifact.sourceUrl,
          artifact.sha256,
          artifact.localPath,
        ]);
        await transaction`
          INSERT INTO oracle_source_artifacts (
            artifact_id, run_id, source_system, source_url, local_uri,
            ready_marker_uri, byte_size, sha256, retrieved_at
          ) VALUES (
            ${artifactId}, ${request.runId}, ${artifact.sourceSystem},
            ${artifact.sourceUrl}, ${pathToFileURL(artifact.localPath).toString()},
            ${pathToFileURL(artifact.readyMarkerPath).toString()},
            ${artifact.bytes}, ${artifact.sha256}, ${request.asOf}
          )
          ON CONFLICT (artifact_id) DO NOTHING
        `;
      }

      let newProperties = 0;
      let changedProperties = 0;
      let unchangedProperties = 0;
      for (const entry of prepared.properties) {
        const exactFolio = entry.parcel.exactFolio;
        const propertyHash = sourceRecordHash({
          buildings: entry.buildings,
          coordinates: entry.coordinates,
          owners: entry.owners,
          parcel: entry.parcel,
          siteAddress: entry.siteAddress,
        });
        const existing = await transaction<{ source_record_hash: string }[]>`
          SELECT source_record_hash FROM oracle_properties
          WHERE exact_folio = ${exactFolio}
        `;
        const delta = classifyHashDelta(
          existing[0]?.source_record_hash ?? null,
          propertyHash,
        );
        if (delta === "new") newProperties += 1;
        else if (delta === "unchanged") unchangedProperties += 1;
        else changedProperties += 1;

        await transaction`
          INSERT INTO oracle_properties (
            property_id, parcel_id, county, source_system, exact_folio,
            matching_folio_digits, site_address, site_city, site_zip,
            property_use_code, property_use_description, acres,
            total_square_feet, heated_square_feet, year_built,
            source_record_hash, first_seen_run_id, last_seen_run_id
          ) VALUES (
            ${entry.propertyId}, ${parcelId(exactFolio)}, 'pasco',
            'pasco_appraiser', ${exactFolio}, ${exactFolio.replace(/\D/g, "")},
            ${entry.siteAddress?.siteAddress ?? null},
            ${entry.siteAddress?.city ?? null}, ${entry.siteAddress?.zipCode ?? null},
            ${entry.parcel.propertyUseCode},
            ${entry.parcel.propertyUseDescription}, ${entry.parcel.acres},
            ${entry.parcel.totalSquareFeet}, ${entry.parcel.heatedSquareFeet},
            ${entry.yearBuilt}, ${propertyHash}, ${request.runId}, ${request.runId}
          )
          ON CONFLICT (property_id) DO UPDATE SET
            site_address = EXCLUDED.site_address,
            site_city = EXCLUDED.site_city,
            site_zip = EXCLUDED.site_zip,
            property_use_code = EXCLUDED.property_use_code,
            property_use_description = EXCLUDED.property_use_description,
            acres = EXCLUDED.acres,
            total_square_feet = EXCLUDED.total_square_feet,
            heated_square_feet = EXCLUDED.heated_square_feet,
            year_built = EXCLUDED.year_built,
            source_record_hash = EXCLUDED.source_record_hash,
            last_seen_run_id = EXCLUDED.last_seen_run_id,
            updated_at = now()
        `;

        const unavailableFeatures = [
          ["permits", "source_unavailable"],
          ["contractors", "source_unavailable"],
          ["phones", "not_provided_by_source"],
          ["emails", "not_provided_by_source"],
          ["sunbiz", "source_not_collected"],
          ["bbb", "source_not_collected"],
        ] as const;
        for (const [feature, reason] of unavailableFeatures) {
          await transaction`
            INSERT INTO oracle_property_availability (
              property_id, feature, availability, reason,
              first_seen_run_id, last_seen_run_id
            ) VALUES (
              ${entry.propertyId}, ${feature}, 'unavailable', ${reason},
              ${request.runId}, ${request.runId}
            )
            ON CONFLICT (property_id, feature) DO UPDATE SET
              availability = EXCLUDED.availability,
              reason = EXCLUDED.reason,
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        for (const owner of entry.owners) {
          const ownerHash = sourceRecordHash(owner);
          const ownershipId = deterministicId("ownership", [
            "1.0.0",
            "ownership",
            entry.propertyId,
            ownerHash,
          ]);
          await transaction`
            INSERT INTO oracle_ownerships (
              ownership_id, property_id, owner_name_1, owner_name_2,
              mailing_address_1, mailing_address_2, mailing_city,
              mailing_state, mailing_zip, mailing_country,
              source_record_hash, first_seen_run_id, last_seen_run_id
            ) VALUES (
              ${ownershipId}, ${entry.propertyId}, ${owner.ownerName1},
              ${owner.ownerName2}, ${owner.mailingAddress1},
              ${owner.mailingAddress2}, ${owner.mailingCity},
              ${owner.mailingState}, ${owner.mailingZip},
              ${owner.mailingCountry}, ${ownerHash}, ${request.runId},
              ${request.runId}
            )
            ON CONFLICT (ownership_id) DO UPDATE SET
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        if (entry.coordinates) {
          const coordinateHash = sourceRecordHash(entry.coordinates);
          const coordinateId = deterministicId("coordinate", [
            "1.0.0",
            "coordinate",
            entry.propertyId,
            "pasco_gis",
          ]);
          await transaction`
            INSERT INTO oracle_coordinates (
              coordinate_id, property_id, latitude, longitude, crs,
              provenance, conversion_rule, source_last_update,
              source_record_hash, first_seen_run_id, last_seen_run_id
            ) VALUES (
              ${coordinateId}, ${entry.propertyId},
              ${entry.coordinates.latitude}, ${entry.coordinates.longitude},
              'EPSG:4326', 'pasco_gis_parcel_polygon',
              'arcgis_outsr_4326_polygon_centroid_v1',
              ${entry.coordinates.sourceLastUpdate}, ${coordinateHash},
              ${request.runId}, ${request.runId}
            )
            ON CONFLICT (coordinate_id) DO UPDATE SET
              latitude = EXCLUDED.latitude,
              longitude = EXCLUDED.longitude,
              source_last_update = EXCLUDED.source_last_update,
              source_record_hash = EXCLUDED.source_record_hash,
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        for (const building of entry.buildings) {
          const buildingHash = sourceRecordHash(building);
          const buildingId = deterministicId("building", [
            "1.0.0",
            "building",
            entry.propertyId,
            building.buildingNumber,
            building.buildingSection,
            buildingHash,
          ]);
          await transaction`
            INSERT INTO oracle_building_signals (
              building_signal_id, property_id, building_number,
              building_section, actual_year_built, effective_year_built,
              use_description, roof_cover, roof_structure, observed_condition,
              stories, total_square_feet, heated_square_feet,
              source_record_hash, first_seen_run_id, last_seen_run_id
            ) VALUES (
              ${buildingId}, ${entry.propertyId}, ${building.buildingNumber},
              ${building.buildingSection}, ${building.actualYearBuilt},
              ${building.effectiveYearBuilt}, ${building.useDescription},
              ${building.roofCover}, ${building.roofStructure},
              ${building.observedCondition}, ${building.stories},
              ${building.totalSquareFeet}, ${building.heatedSquareFeet},
              ${buildingHash}, ${request.runId}, ${request.runId}
            )
            ON CONFLICT (building_signal_id) DO UPDATE SET
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        const roofSignal = yearBuiltRoofProxy(entry.yearBuilt, request.asOf);
        const roofHash = sourceRecordHash(roofSignal);
        const roofSignalId = deterministicId("roof", [
          "1.0.0",
          "roof-signal",
          entry.propertyId,
          roofSignal.basis,
        ]);
        await transaction`
          INSERT INTO oracle_roof_signals (
            roof_signal_id, property_id, basis, basis_quality, precision,
            basis_year, age_years, as_of, derivation_rule,
            source_record_hash, first_seen_run_id, last_seen_run_id
          ) VALUES (
            ${roofSignalId}, ${entry.propertyId}, ${roofSignal.basis},
            ${roofSignal.basisQuality}, ${roofSignal.precision},
            ${entry.yearBuilt}, ${roofSignal.ageYears}, ${request.asOf},
            'roof_age.year_built_proxy.v1', ${roofHash}, ${request.runId},
            ${request.runId}
          )
          ON CONFLICT (roof_signal_id) DO UPDATE SET
            age_years = EXCLUDED.age_years,
            as_of = EXCLUDED.as_of,
            source_record_hash = EXCLUDED.source_record_hash,
            last_seen_run_id = EXCLUDED.last_seen_run_id
        `;

        for (const permit of entry.permits) {
          const sourceRecordKey = `pasco_accela:${permit.recordNumber}`;
          const normalizedStatus = normalizePermitStatus(permit.status);
          const sourceRecordDate = parseUsDate(permit.recordDate);
          const isOpen =
            normalizedStatus === "open"
              ? true
              : normalizedStatus === "closed" || normalizedStatus === "expired"
                ? false
                : null;
          const openStartDate = isOpen ? sourceRecordDate : null;
          const openDurationDays =
            openStartDate === null
              ? null
              : wholeUtcDays(openStartDate, request.asOf);
          const permitHash = sourceRecordHash(permit);
          await transaction`
            INSERT INTO oracle_permits (
              permit_id, property_id, source_record_key, permit_number,
              record_type, description, project_name, raw_status,
              normalized_status, source_record_date, is_open,
              open_start_date, open_start_basis, open_duration_days,
              roofing_relevance, source_record_hash,
              first_seen_run_id, last_seen_run_id
            ) VALUES (
              ${permitId(sourceRecordKey)}, ${entry.propertyId},
              ${sourceRecordKey}, ${permit.recordNumber}, ${permit.recordType},
              ${permit.description}, ${permit.projectName}, ${permit.status},
              ${normalizedStatus}, ${sourceRecordDate}, ${isOpen},
              ${openStartDate},
              ${openStartDate ? "record_entered_date_proxy" : null},
              ${openDurationDays}, true, ${permitHash},
              ${request.runId}, ${request.runId}
            )
            ON CONFLICT (permit_id) DO UPDATE SET
              raw_status = EXCLUDED.raw_status,
              normalized_status = EXCLUDED.normalized_status,
              source_record_date = EXCLUDED.source_record_date,
              is_open = EXCLUDED.is_open,
              open_start_date = EXCLUDED.open_start_date,
              open_start_basis = EXCLUDED.open_start_basis,
              open_duration_days = EXCLUDED.open_duration_days,
              source_record_hash = EXCLUDED.source_record_hash,
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }
      }

      const propertyIds = prepared.properties.map((entry) => entry.propertyId);
      const countRows = await transaction<
        {
          coordinates: number;
          ownership: number;
          permits: number;
        }[]
      >`
        SELECT
          (SELECT count(DISTINCT property_id)::int FROM oracle_coordinates WHERE property_id = ANY(${propertyIds})) AS coordinates,
          (SELECT count(DISTINCT property_id)::int FROM oracle_ownerships WHERE property_id = ANY(${propertyIds})) AS ownership,
          (SELECT count(*)::int FROM oracle_permits WHERE property_id = ANY(${propertyIds})) AS permits
      `;
      const duplicateRows = await transaction<{ duplicate_count: number }[]>`
        SELECT count(*)::int AS duplicate_count
        FROM (
          SELECT exact_folio FROM oracle_properties
          GROUP BY exact_folio HAVING count(*) > 1
        ) duplicates
      `;
      const roofRows = await transaction<{ basis: string; count: number }[]>`
        SELECT basis, count(*)::int AS count
        FROM oracle_roof_signals
        WHERE property_id = ANY(${propertyIds})
        GROUP BY basis ORDER BY basis
      `;
      const roofSignalBasis = Object.fromEntries(
        roofRows.map((row) => [row.basis, row.count]),
      );
      const rejectedRecords = Object.values(prepared.sourceCounts).reduce(
        (sum, count) => sum + count.rejected,
        0,
      );
      const counts = countRows[0] ?? {
        coordinates: 0,
        ownership: 0,
        permits: 0,
      };
      const summary: PilotRunSummary = {
        acceptedProperties: prepared.properties.length,
        changedProperties,
        coordinates: counts.coordinates,
        duplicateProperties: duplicateRows[0]?.duplicate_count ?? 0,
        newProperties,
        ownership: counts.ownership,
        permitRequestCount: prepared.permitRequestCount,
        permits: counts.permits,
        rejectedRecords,
        roofSignalBasis,
        runId: request.runId,
        sourceCounts: prepared.sourceCounts,
        unchangedProperties,
        workflowId: request.workflowId,
      };

      const reconciliations = [
        ["pilot_property_count", prepared.properties.length, 25],
        ["duplicate_exact_folio", summary.duplicateProperties, 0],
        ["coordinate_coverage", summary.coordinates, 25],
        ["ownership_coverage", summary.ownership, 25],
      ] as const;
      for (const [checkName, observed, expected] of reconciliations) {
        const status = observed === expected ? "pass" : "warn";
        const reconciliationId = deterministicId("reconciliation", [
          "1.0.0",
          "reconciliation",
          request.runId,
          checkName,
        ]);
        await transaction`
          INSERT INTO oracle_reconciliation_outcomes (
            reconciliation_id, run_id, check_name, status,
            observed_count, expected_count
          ) VALUES (
            ${reconciliationId}, ${request.runId}, ${checkName}, ${status},
            ${observed}, ${expected}
          )
          ON CONFLICT (reconciliation_id) DO UPDATE SET
            status = EXCLUDED.status,
            observed_count = EXCLUDED.observed_count,
            expected_count = EXCLUDED.expected_count
        `;
      }

      await transaction`
        UPDATE oracle_pipeline_runs SET
          status = 'completed', completed_at = now(),
          source_counts = ${transaction.json(
            prepared.sourceCounts as unknown as postgres.JSONValue,
          )},
          result_counts = ${transaction.json(
            summary as unknown as postgres.JSONValue,
          )},
          limitations = ${transaction.json([
            ...PIPELINE_LIMITATIONS,
            ...prepared.sourceLimitations,
          ])}
        WHERE run_id = ${request.runId}
      `;
      await transaction`
        UPDATE oracle_pipeline_attempts
        SET status = 'completed', completed_at = now()
        WHERE run_id = ${request.runId} AND status = 'running'
      `;
      return summary;
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}

export async function getPilotRunSummary(
  databaseUrl: string,
  runId: string,
): Promise<PilotRunSummary | null> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    const rows = await sql<{ result_counts: PilotRunSummary }[]>`
      SELECT result_counts FROM oracle_pipeline_runs
      WHERE run_id = ${runId} AND status = 'completed'
    `;
    return rows[0]?.result_counts ?? null;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
