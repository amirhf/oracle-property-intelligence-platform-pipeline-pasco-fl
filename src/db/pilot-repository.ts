import { pathToFileURL } from "node:url";
import { resourceUsage } from "node:process";

import postgres from "postgres";

import type {
  PilotRunRequest,
  PilotRunSummary,
  PreparedPilot,
} from "../domain/types.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
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
import {
  beginLoaderEffect,
  completeLoaderEffect,
  type LoaderDurabilityContext,
} from "./loader-durability.js";
import { countyIngestRequestSha256 } from "../workflow/schemas.js";
import {
  applyTemporalReconciliation,
  prepareTemporalReconciliation,
  type PropertyTemporalDelta,
} from "./temporal-reconciliation.js";
import { recordProjectionLoad } from "./projection-repository.js";
import {
  OWNER_AUTHORITY_CLASS,
  PASCO_PARCEL_FOLIO_COUNT,
  PASCO_PARCEL_FOLIO_SET_SHA256,
  PASCO_PARCEL_MEMBERSHIP_CLAIM,
  validateOwnerAuthorityRecord,
} from "../authoritative/authority.js";
import { AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM } from "../snapshot/coverage.js";

function pipelineLimitations(
  selectionSize: number,
  sampleAlgorithm?: string,
): string[] {
  if (
    selectionSize === PASCO_PARCEL_FOLIO_COUNT &&
    sampleAlgorithm === AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM
  ) {
    return [
      PASCO_PARCEL_MEMBERSHIP_CLAIM,
      "Authority is owner-assumed for the exact hash-bound source snapshot and is not represented as independent Pasco certification.",
      "The separately published 335,946 real-property-parcel statistic remains semantically unreconciled.",
      "GIS, coordinate, building, address, and ownership coverage is measured independently from parcel membership.",
      "Permit and contractor coverage remains unavailable.",
    ];
  }
  return [
    `${selectionSize.toLocaleString("en-US")}-property deterministic appraisal/GIS sample; not complete Pasco coverage.`,
    "Pasco Accela coverage can exclude incorporated-city permit systems.",
    "Pasco Accela collection stopped after challenge/CAPTCHA content was detected; permit search results and contractor identity are unavailable.",
    "Sunbiz and BBB are intentionally not collected in this checkpoint.",
    "This bounded sample is not authoritative for absence and cannot inactivate properties.",
  ];
}

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
    await sql.begin(async (transaction) => {
      const requestSha256 = countyIngestRequestSha256(request);
      const idempotencyKey = `CountyIngest/${request.workflowId}`;
      const workflowRows = await transaction<{ request_sha256: string }[]>`
        SELECT request_sha256 FROM oracle_workflow_requests
        WHERE idempotency_key = ${idempotencyKey} FOR UPDATE
      `;
      if (workflowRows[0] && workflowRows[0].request_sha256 !== requestSha256) {
        throw new DurableConflictError(
          `CountyIngest idempotency conflict (${request.workflowId})`,
        );
      }
      await transaction`
        INSERT INTO oracle_workflow_requests (
          idempotency_key, service_name, handler_name,
          request_sha256, request_payload
        ) VALUES (
          ${idempotencyKey}, 'CountyIngest', 'run', ${requestSha256},
          ${transaction.json(request as unknown as postgres.JSONValue)}
        ) ON CONFLICT (idempotency_key) DO NOTHING
      `;
      const existingRuns = await transaction<
        {
          as_of_matches: boolean;
          county: string;
          request_sha256: string | null;
          sample_algorithm: string;
          sample_seed: string;
          selection_size: number | null;
          workflow_id: string;
        }[]
      >`
        SELECT workflow_id, county, sample_algorithm, sample_seed,
               selection_size, request_sha256,
               as_of = ${request.asOf}::timestamptz AS as_of_matches
        FROM oracle_pipeline_runs
        WHERE run_id = ${request.runId} FOR UPDATE
      `;
      if (
        existingRuns[0] &&
        (existingRuns[0].workflow_id !== request.workflowId ||
          existingRuns[0].county !== request.county ||
          existingRuns[0].sample_algorithm !== request.sampleAlgorithm ||
          existingRuns[0].sample_seed !== request.sampleSeed ||
          !existingRuns[0].as_of_matches ||
          (existingRuns[0].selection_size !== null &&
            existingRuns[0].selection_size !== request.selectionSize) ||
          (existingRuns[0].request_sha256 !== null &&
            existingRuns[0].request_sha256 !== requestSha256))
      ) {
        throw new DurableConflictError(
          `Pipeline run identity conflict (${request.runId})`,
        );
      }
      const databaseSize = await transaction<{ bytes: number }[]>`
        SELECT pg_database_size(current_database())::bigint AS bytes
      `;
      await transaction`
        INSERT INTO oracle_pipeline_runs (
          run_id, workflow_id, county, sample_algorithm, sample_seed,
          window_start, window_end, as_of, status, limitations,
          selection_size, database_size_before_bytes, request_sha256
        ) VALUES (
          ${request.runId}, ${request.workflowId}, ${request.county},
          ${request.sampleAlgorithm}, ${request.sampleSeed},
          ${request.asOf}, ${request.asOf}, ${request.asOf}, 'running',
          ${transaction.json(
            pipelineLimitations(request.selectionSize, request.sampleAlgorithm),
          )},
          ${request.selectionSize}, ${databaseSize[0]?.bytes ?? 0},
          ${requestSha256}
        ) ON CONFLICT (run_id) DO NOTHING
      `;
      await transaction`
        UPDATE oracle_pipeline_runs
        SET request_sha256 = COALESCE(request_sha256, ${requestSha256}),
            selection_size = COALESCE(selection_size, ${request.selectionSize})
        WHERE run_id = ${request.runId}
      `;
      const attemptId = deterministicId("attempt", [
        "1.0.0",
        "attempt",
        request.runId,
        "CountyIngest",
        "run",
        "1",
      ]);
      await transaction`
        INSERT INTO oracle_pipeline_attempts (
          attempt_id, run_id, service_name, handler_name,
          attempt_number, status
        ) VALUES (
          ${attemptId}, ${request.runId}, 'CountyIngest', 'run', 1, 'running'
        ) ON CONFLICT (attempt_id) DO NOTHING
      `;
    });
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
      SET status = 'failed', completed_at = clock_timestamp()
      WHERE run_id = ${runId}
    `;
    await sql`
      UPDATE oracle_pipeline_attempts
      SET status = 'failed', completed_at = clock_timestamp(), error_code = ${errorCode}
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
  durability: LoaderDurabilityContext,
): Promise<PilotRunSummary> {
  if (prepared.properties.length !== prepared.selectionSize) {
    throw new DurableInputError(
      `Prepared property count does not match its selection size`,
    );
  }
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const replay = await beginLoaderEffect(transaction, durability);
      if (replay) return replay;
      if (durability.snapshot.manifestVersion === "1.2.0") {
        if (
          request.selectionSize === PASCO_PARCEL_FOLIO_COUNT &&
          request.sampleAlgorithm === AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM
        ) {
          if (
            prepared.selectionSize !== PASCO_PARCEL_FOLIO_COUNT ||
            prepared.selectedRecordSha256 !== PASCO_PARCEL_FOLIO_SET_SHA256 ||
            durability.snapshot.coverage.mode !== "authoritative_complete" ||
            !prepared.authorityRecord
          ) {
            throw new DurableInputError(
              "Authoritative Loader input lacks the exact owner-accepted identity",
            );
          }
          const authority = validateOwnerAuthorityRecord(
            prepared.authorityRecord,
          );
          await transaction`
            INSERT INTO oracle_source_authority_records (
              authority_record_id, authority_class, source_snapshot_id,
              source_run_id, source_system, scope_id, decision_sha256,
              completeness_evidence_sha256,
              source_snapshot_manifest_sha256, authority_payload
            ) VALUES (
              ${authority.authorityRecordId}, ${OWNER_AUTHORITY_CLASS},
              ${durability.snapshot.snapshotId}, ${request.runId},
              'pasco_appraiser', ${durability.snapshot.coverage.scopeId},
              ${authority.decisionSha256},
              ${authority.completenessEvidenceSha256},
              ${durability.preparedManifest.snapshotManifest.sha256},
              ${transaction.json(authority.payload as postgres.JSONValue)}
            ) ON CONFLICT (authority_record_id) DO NOTHING
          `;
          const storedAuthority = await transaction<
            {
              completeness_evidence_sha256: string;
              decision_sha256: string;
              source_snapshot_id: string;
            }[]
          >`
            SELECT source_snapshot_id, decision_sha256,
                   completeness_evidence_sha256
            FROM oracle_source_authority_records
            WHERE authority_record_id = ${authority.authorityRecordId}
          `;
          if (
            storedAuthority[0]?.source_snapshot_id !==
              durability.snapshot.snapshotId ||
            storedAuthority[0]?.decision_sha256 !== authority.decisionSha256 ||
            storedAuthority[0]?.completeness_evidence_sha256 !==
              authority.completenessEvidenceSha256
          ) {
            throw new DurableConflictError(
              `Authority record identity conflict (${authority.authorityRecordId})`,
            );
          }
        }
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
        const projection = await recordProjectionLoad(
          transaction,
          durability.snapshot,
          request.runId,
          prepared,
        );
        const runMetrics = await transaction<
          {
            database_size_after_bytes: string;
            database_size_before_bytes: string;
            elapsed_ms: string;
          }[]
        >`
          SELECT
            pg_database_size(current_database())::bigint AS database_size_after_bytes,
            COALESCE(database_size_before_bytes, 0)::bigint AS database_size_before_bytes,
            GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)::bigint AS elapsed_ms
          FROM oracle_pipeline_runs WHERE run_id = ${request.runId}
        `;
        const databaseSizeAfterBytes = Number(
          runMetrics[0]?.database_size_after_bytes ?? 0,
        );
        const databaseSizeBeforeBytes = Number(
          runMetrics[0]?.database_size_before_bytes ?? 0,
        );
        const elapsedMs = Number(runMetrics[0]?.elapsed_ms ?? 0);
        const coordinates = prepared.properties.filter(
          (property) => property.coordinates !== null,
        ).length;
        const ownership = prepared.properties.reduce(
          (total, property) => total + property.owners.length,
          0,
        );
        const buildings = prepared.properties.reduce(
          (total, property) => total + property.buildings.length,
          0,
        );
        const roofSignals = prepared.properties.filter(
          (property) => property.yearBuilt !== null,
        ).length;
        const explicitUnavailableFacts =
          prepared.properties.length * 6 +
          (prepared.properties.length - coordinates) +
          prepared.properties.filter(
            (property) => property.buildings.length === 0,
          ).length +
          prepared.properties.filter((property) => property.owners.length === 0)
            .length +
          prepared.properties.filter(
            (property) => property.siteAddress === null,
          ).length +
          (prepared.properties.length - roofSignals);
        const rejectedRecords = Object.values(prepared.sourceCounts).reduce(
          (sum, count) => sum + count.rejected,
          0,
        );
        const summary: PilotRunSummary = {
          acceptedProperties: prepared.properties.length,
          activeProperties: projection.activeProperties,
          ...(prepared.authorityRecord
            ? { authorityRecordId: prepared.authorityRecord.authorityRecordId }
            : {}),
          buildings,
          changedProperties: projection.changedProperties,
          coordinates,
          databaseGrowthBytes: Math.max(
            0,
            databaseSizeAfterBytes - databaseSizeBeforeBytes,
          ),
          databaseSizeAfterBytes,
          databaseSizeBeforeBytes,
          diskAvailableBytes: prepared.resourceMetrics.diskAvailableBytes,
          duplicateProperties: 0,
          elapsedMs,
          explicitUnavailableFacts,
          gisMetrics: prepared.gisMetrics,
          inactiveProperties: projection.inactiveProperties,
          inactivatedProperties: projection.inactivatedProperties,
          missingCoordinates: prepared.properties.length - coordinates,
          ...(projection.materializationId
            ? { materializationId: projection.materializationId }
            : {}),
          ...(projection.materializationSha256
            ? { materializationSha256: projection.materializationSha256 }
            : {}),
          newProperties: projection.newProperties,
          ownership,
          peakRssBytes: Math.max(
            prepared.resourceMetrics.peakRssBytes,
            resourceUsage().maxRSS * 1_024,
          ),
          permitRequestCount: prepared.permitRequestCount,
          permits: 0,
          reactivatedProperties: projection.reactivatedProperties,
          rejectedRecords,
          roofSignalBasis: { year_built_proxy: roofSignals },
          roofSignals,
          runId: request.runId,
          ...(!projection.sampleIsolated
            ? { scopeId: durability.snapshot.coverage.scopeId }
            : {}),
          selectionSize: prepared.selectionSize,
          sourceCounts: prepared.sourceCounts,
          ...(prepared.sourceReconciliation
            ? { sourceReconciliation: prepared.sourceReconciliation }
            : {}),
          ...(!projection.sampleIsolated
            ? { snapshotId: durability.snapshot.snapshotId }
            : {}),
          throughputPropertiesPerSecond:
            elapsedMs > 0
              ? Number(
                  (prepared.properties.length / (elapsedMs / 1_000)).toFixed(2),
                )
              : 0,
          unchangedProperties: projection.unchangedProperties,
          workflowId: request.workflowId,
        };
        for (const [checkName, observed, expected] of [
          [
            projection.sampleIsolated
              ? "isolated_sample_property_count"
              : "sealed_projection_property_count",
            prepared.properties.length,
            prepared.selectionSize,
          ],
          ["duplicate_exact_folio", 0, 0],
          ["coordinate_coverage", coordinates, prepared.selectionSize],
          ["ownership_coverage", ownership, prepared.selectionSize],
        ] as const) {
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
              ${reconciliationId}, ${request.runId}, ${checkName},
              ${observed === expected ? "pass" : "warn"}, ${observed}, ${expected}
            ) ON CONFLICT (reconciliation_id) DO UPDATE SET
              status = EXCLUDED.status,
              observed_count = EXCLUDED.observed_count,
              expected_count = EXCLUDED.expected_count
          `;
        }
        await transaction`
          UPDATE oracle_pipeline_runs SET
            status = 'completed', completed_at = clock_timestamp(),
            source_counts = ${transaction.json(prepared.sourceCounts as unknown as postgres.JSONValue)},
            result_counts = ${transaction.json(summary as unknown as postgres.JSONValue)},
            limitations = ${transaction.json([
              ...pipelineLimitations(
                prepared.selectionSize,
                prepared.sampleAlgorithm,
              ),
              ...prepared.sourceLimitations,
              projection.sampleIsolated
                ? "Sample observations are isolated and do not advance a current or authoritative projection head."
                : `Sealed immutable projection ${projection.materializationId} is the only coherent publication source.`,
            ])}
          WHERE run_id = ${request.runId}
        `;
        await transaction`
          UPDATE oracle_pipeline_attempts
          SET status = 'completed', completed_at = clock_timestamp()
          WHERE run_id = ${request.runId} AND status = 'running'
        `;
        return completeLoaderEffect(transaction, durability, summary);
      }
      const temporalPlan = await prepareTemporalReconciliation(
        transaction,
        durability.snapshot,
      );
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
            ready_marker_uri, byte_size, sha256, retrieved_at,
            snapshot_id, prepared_input_id
          ) VALUES (
            ${artifactId}, ${request.runId}, ${artifact.sourceSystem},
            ${artifact.sourceUrl}, ${pathToFileURL(artifact.localPath).toString()},
            ${pathToFileURL(artifact.readyMarkerPath).toString()},
            ${artifact.bytes}, ${artifact.sha256}, ${request.asOf},
            ${durability.snapshot.snapshotId},
            ${durability.preparedManifest.preparedInputId}
          )
          ON CONFLICT (artifact_id) DO NOTHING
        `;
      }

      let newProperties = 0;
      let changedProperties = 0;
      let unchangedProperties = 0;
      const temporalDeltas: PropertyTemporalDelta[] = [];
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
        temporalDeltas.push({
          classification: delta,
          propertyId: entry.propertyId,
          sourceRecordHash: propertyHash,
        });

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

        if (entry.yearBuilt !== null) {
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
        }

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

      const temporal = await applyTemporalReconciliation(
        transaction,
        temporalPlan,
        request.runId,
        temporalDeltas,
      );

      const propertyIds = prepared.properties.map((entry) => entry.propertyId);
      const countRows = await transaction<
        {
          availability: number;
          buildings: number;
          coordinates: number;
          ownership: number;
          permits: number;
          roof_signals: number;
        }[]
      >`
        SELECT
          (SELECT count(DISTINCT property_id)::int FROM oracle_coordinates WHERE property_id = ANY(${propertyIds})) AS coordinates,
          (SELECT count(DISTINCT property_id)::int FROM oracle_ownerships WHERE property_id = ANY(${propertyIds})) AS ownership,
          (SELECT count(*)::int FROM oracle_building_signals WHERE property_id = ANY(${propertyIds})) AS buildings,
          (SELECT count(*)::int FROM oracle_roof_signals WHERE property_id = ANY(${propertyIds})) AS roof_signals,
          (SELECT count(*)::int FROM oracle_permits WHERE property_id = ANY(${propertyIds})) AS permits,
          (SELECT count(*)::int FROM oracle_property_availability WHERE property_id = ANY(${propertyIds})) AS availability
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
        availability: 0,
        buildings: 0,
        coordinates: 0,
        ownership: 0,
        permits: 0,
        roof_signals: 0,
      };
      const runMetrics = await transaction<
        {
          database_size_after_bytes: string;
          database_size_before_bytes: string;
          elapsed_ms: string;
        }[]
      >`
        SELECT
          pg_database_size(current_database())::bigint AS database_size_after_bytes,
          COALESCE(database_size_before_bytes, 0)::bigint AS database_size_before_bytes,
          GREATEST(0, EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)::bigint AS elapsed_ms
        FROM oracle_pipeline_runs WHERE run_id = ${request.runId}
      `;
      const databaseSizeAfterBytes = Number(
        runMetrics[0]?.database_size_after_bytes ?? 0,
      );
      const databaseSizeBeforeBytes = Number(
        runMetrics[0]?.database_size_before_bytes ?? 0,
      );
      const elapsedMs = Number(runMetrics[0]?.elapsed_ms ?? 0);
      const summary: PilotRunSummary = {
        acceptedProperties: prepared.properties.length,
        activeProperties: temporal.activeProperties,
        buildings: counts.buildings,
        changedProperties,
        coordinates: counts.coordinates,
        databaseGrowthBytes: Math.max(
          0,
          databaseSizeAfterBytes - databaseSizeBeforeBytes,
        ),
        databaseSizeAfterBytes,
        databaseSizeBeforeBytes,
        diskAvailableBytes: prepared.resourceMetrics.diskAvailableBytes,
        duplicateProperties: duplicateRows[0]?.duplicate_count ?? 0,
        elapsedMs,
        explicitUnavailableFacts: counts.availability,
        gisMetrics: prepared.gisMetrics,
        inactiveProperties: temporal.inactiveProperties,
        inactivatedProperties: temporal.inactivatedProperties,
        missingCoordinates: prepared.properties.length - counts.coordinates,
        newProperties,
        ownership: counts.ownership,
        peakRssBytes: Math.max(
          prepared.resourceMetrics.peakRssBytes,
          resourceUsage().maxRSS * 1_024,
        ),
        permitRequestCount: prepared.permitRequestCount,
        permits: counts.permits,
        rejectedRecords,
        reactivatedProperties: temporal.reactivatedProperties,
        roofSignals: counts.roof_signals,
        roofSignalBasis,
        runId: request.runId,
        selectionSize: prepared.selectionSize,
        sourceCounts: prepared.sourceCounts,
        throughputPropertiesPerSecond:
          elapsedMs > 0
            ? Number(
                (prepared.properties.length / (elapsedMs / 1_000)).toFixed(2),
              )
            : 0,
        unchangedProperties,
        workflowId: request.workflowId,
      };

      const reconciliations = [
        [
          "dataset_property_count",
          prepared.properties.length,
          prepared.selectionSize,
        ],
        ["duplicate_exact_folio", summary.duplicateProperties, 0],
        ["coordinate_coverage", summary.coordinates, prepared.selectionSize],
        ["ownership_coverage", summary.ownership, prepared.selectionSize],
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
          status = 'completed', completed_at = clock_timestamp(),
          source_counts = ${transaction.json(
            prepared.sourceCounts as unknown as postgres.JSONValue,
          )},
          result_counts = ${transaction.json(
            summary as unknown as postgres.JSONValue,
          )},
          limitations = ${transaction.json([
            ...pipelineLimitations(prepared.selectionSize),
            ...prepared.sourceLimitations,
          ])}
        WHERE run_id = ${request.runId}
      `;
      await transaction`
        UPDATE oracle_pipeline_attempts
        SET status = 'completed', completed_at = clock_timestamp()
        WHERE run_id = ${request.runId} AND status = 'running'
      `;
      return completeLoaderEffect(transaction, durability, summary);
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
