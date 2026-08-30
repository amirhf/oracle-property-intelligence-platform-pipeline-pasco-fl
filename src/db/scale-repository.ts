import { resourceUsage } from "node:process";
import { pathToFileURL } from "node:url";

import postgres from "postgres";

import type {
  PilotRunRequest,
  PilotRunSummary,
  PreparedPilot,
} from "../domain/types.js";
import { classifyHashDelta } from "../domain/reconciliation.js";
import { yearBuiltRoofProxy } from "../domain/signals.js";
import { DurableInputError } from "../lib/durability-errors.js";
import { deterministicId, parcelId, sourceRecordHash } from "../lib/hash.js";
import {
  beginLoaderEffect,
  completeLoaderEffect,
  type LoaderDurabilityContext,
} from "./loader-durability.js";
import {
  applyTemporalReconciliation,
  prepareTemporalReconciliation,
  type PropertyTemporalDelta,
} from "./temporal-reconciliation.js";

const UNAVAILABLE_FEATURES = [
  ["permits", "source_unavailable"],
  ["contractors", "source_unavailable"],
  ["phones", "not_provided_by_source"],
  ["emails", "not_provided_by_source"],
  ["sunbiz", "source_not_collected"],
  ["bbb", "source_not_collected"],
] as const;

function batches<T>(values: readonly T[], size: number): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}

function uniqueBy<T>(values: readonly T[], key: (value: T) => string): T[] {
  return [...new Map(values.map((value) => [key(value), value])).values()];
}

function limitations(selectionSize: number): string[] {
  return [
    `${selectionSize.toLocaleString("en-US")}-property deterministic appraisal/GIS sample; not complete Pasco coverage.`,
    "Permit source unavailable after the Accela challenge stop; missing permits do not mean none exist.",
    "Contractor source unavailable because no compliant permit source was established.",
    "Sunbiz and BBB were not collected and remain explicitly unavailable.",
    "This bounded sample is not authoritative for absence and cannot inactivate properties.",
  ];
}

export async function loadPreparedScale(
  databaseUrl: string,
  request: PilotRunRequest,
  prepared: PreparedPilot,
  durability: LoaderDurabilityContext,
): Promise<PilotRunSummary> {
  if (durability.snapshot.manifestVersion === "1.2.0") {
    throw new DurableInputError(
      "Versioned snapshots must use the sealed projection Loader path",
    );
  }
  if (prepared.selectionSize !== 5_000 && prepared.selectionSize !== 25_000) {
    throw new DurableInputError(
      "Scaled load requires exactly 5,000 or 25,000 properties",
    );
  }
  if (prepared.properties.length !== prepared.selectionSize) {
    throw new DurableInputError(
      "Prepared property count does not match scale selection size",
    );
  }
  if (prepared.properties.some((entry) => entry.permits.length > 0)) {
    throw new DurableInputError(
      "Scaled appraisal/GIS load must not contain permit records",
    );
  }

  const sql = postgres(databaseUrl, { max: 1 });
  try {
    return await sql.begin(async (transaction) => {
      const replay = await beginLoaderEffect(transaction, durability);
      if (replay) return replay;
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

      const existingHashes = new Map<string, string>();
      for (const propertyBatch of batches(prepared.properties, 2_000)) {
        const rows = await transaction<
          { property_id: string; source_record_hash: string }[]
        >`
          SELECT property_id, source_record_hash
          FROM oracle_properties
          WHERE property_id = ANY(${propertyBatch.map((entry) => entry.propertyId)})
        `;
        for (const row of rows) {
          existingHashes.set(row.property_id, row.source_record_hash);
        }
      }

      let newProperties = 0;
      let changedProperties = 0;
      let unchangedProperties = 0;
      const temporalDeltas: PropertyTemporalDelta[] = [];
      for (const propertyBatch of batches(prepared.properties, 500)) {
        const propertyRows = propertyBatch.map((entry) => {
          const exactFolio = entry.parcel.exactFolio;
          const propertyHash = sourceRecordHash({
            buildings: entry.buildings,
            coordinates: entry.coordinates,
            owners: entry.owners,
            parcel: entry.parcel,
            siteAddress: entry.siteAddress,
          });
          const delta = classifyHashDelta(
            existingHashes.get(entry.propertyId) ?? null,
            propertyHash,
          );
          if (delta === "new") newProperties += 1;
          else if (delta === "changed") changedProperties += 1;
          else unchangedProperties += 1;
          temporalDeltas.push({
            classification: delta,
            propertyId: entry.propertyId,
            sourceRecordHash: propertyHash,
          });
          return {
            acres: entry.parcel.acres,
            county: "pasco",
            exact_folio: exactFolio,
            first_seen_run_id: request.runId,
            heated_square_feet: entry.parcel.heatedSquareFeet,
            last_seen_run_id: request.runId,
            matching_folio_digits: exactFolio.replace(/\D/g, ""),
            parcel_id: parcelId(exactFolio),
            property_id: entry.propertyId,
            property_use_code: entry.parcel.propertyUseCode,
            property_use_description: entry.parcel.propertyUseDescription,
            site_address: entry.siteAddress?.siteAddress ?? null,
            site_city: entry.siteAddress?.city ?? null,
            site_zip: entry.siteAddress?.zipCode ?? null,
            source_record_hash: propertyHash,
            source_system: "pasco_appraiser",
            total_square_feet: entry.parcel.totalSquareFeet,
            year_built: entry.yearBuilt,
          };
        });
        await transaction`
          INSERT INTO oracle_properties ${transaction(propertyRows)}
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

        const availabilityRows = propertyBatch.flatMap((entry) =>
          UNAVAILABLE_FEATURES.map(([feature, reason]) => ({
            availability: "unavailable",
            feature,
            first_seen_run_id: request.runId,
            last_seen_run_id: request.runId,
            property_id: entry.propertyId,
            reason,
          })),
        );
        await transaction`
          INSERT INTO oracle_property_availability ${transaction(availabilityRows)}
          ON CONFLICT (property_id, feature) DO UPDATE SET
            availability = EXCLUDED.availability,
            reason = EXCLUDED.reason,
            last_seen_run_id = EXCLUDED.last_seen_run_id
        `;

        const ownershipRows = uniqueBy(
          propertyBatch.flatMap((entry) =>
            entry.owners.map((owner) => {
              const ownerHash = sourceRecordHash(owner);
              return {
                first_seen_run_id: request.runId,
                last_seen_run_id: request.runId,
                mailing_address_1: owner.mailingAddress1,
                mailing_address_2: owner.mailingAddress2,
                mailing_city: owner.mailingCity,
                mailing_country: owner.mailingCountry,
                mailing_state: owner.mailingState,
                mailing_zip: owner.mailingZip,
                owner_name_1: owner.ownerName1,
                owner_name_2: owner.ownerName2,
                ownership_id: deterministicId("ownership", [
                  "1.0.0",
                  "ownership",
                  entry.propertyId,
                  ownerHash,
                ]),
                property_id: entry.propertyId,
                source_record_hash: ownerHash,
              };
            }),
          ),
          (row) => row.ownership_id,
        );
        if (ownershipRows.length > 0) {
          await transaction`
            INSERT INTO oracle_ownerships ${transaction(ownershipRows)}
            ON CONFLICT (ownership_id) DO UPDATE SET
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        const coordinateRows = propertyBatch.flatMap((entry) => {
          if (!entry.coordinates) return [];
          const coordinateHash = sourceRecordHash(entry.coordinates);
          return [
            {
              conversion_rule: "arcgis_outsr_4326_polygon_centroid_v1",
              coordinate_id: deterministicId("coordinate", [
                "1.0.0",
                "coordinate",
                entry.propertyId,
                "pasco_gis",
              ]),
              crs: "EPSG:4326",
              first_seen_run_id: request.runId,
              last_seen_run_id: request.runId,
              latitude: entry.coordinates.latitude,
              longitude: entry.coordinates.longitude,
              property_id: entry.propertyId,
              provenance: "pasco_gis_parcel_polygon",
              source_last_update: entry.coordinates.sourceLastUpdate,
              source_record_hash: coordinateHash,
            },
          ];
        });
        if (coordinateRows.length > 0) {
          await transaction`
            INSERT INTO oracle_coordinates ${transaction(coordinateRows)}
            ON CONFLICT (coordinate_id) DO UPDATE SET
              latitude = EXCLUDED.latitude,
              longitude = EXCLUDED.longitude,
              source_last_update = EXCLUDED.source_last_update,
              source_record_hash = EXCLUDED.source_record_hash,
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        const buildingRows = uniqueBy(
          propertyBatch.flatMap((entry) =>
            entry.buildings.map((building) => {
              const buildingHash = sourceRecordHash(building);
              return {
                actual_year_built: building.actualYearBuilt,
                building_number: building.buildingNumber,
                building_section: building.buildingSection,
                building_signal_id: deterministicId("building", [
                  "1.0.0",
                  "building",
                  entry.propertyId,
                  building.buildingNumber,
                  building.buildingSection,
                  buildingHash,
                ]),
                effective_year_built: building.effectiveYearBuilt,
                first_seen_run_id: request.runId,
                heated_square_feet: building.heatedSquareFeet,
                last_seen_run_id: request.runId,
                observed_condition: building.observedCondition,
                property_id: entry.propertyId,
                roof_cover: building.roofCover,
                roof_structure: building.roofStructure,
                source_record_hash: buildingHash,
                stories: building.stories,
                total_square_feet: building.totalSquareFeet,
                use_description: building.useDescription,
              };
            }),
          ),
          (row) => row.building_signal_id,
        );
        if (buildingRows.length > 0) {
          await transaction`
            INSERT INTO oracle_building_signals ${transaction(buildingRows)}
            ON CONFLICT (building_signal_id) DO UPDATE SET
              last_seen_run_id = EXCLUDED.last_seen_run_id
          `;
        }

        const roofRows = propertyBatch.flatMap((entry) => {
          if (entry.yearBuilt === null) return [];
          const roofSignal = yearBuiltRoofProxy(entry.yearBuilt, request.asOf);
          return [
            {
              age_years: roofSignal.ageYears,
              as_of: request.asOf,
              basis: roofSignal.basis,
              basis_quality: roofSignal.basisQuality,
              basis_year: entry.yearBuilt,
              derivation_rule: "roof_age.year_built_proxy.v1",
              first_seen_run_id: request.runId,
              last_seen_run_id: request.runId,
              precision: roofSignal.precision,
              property_id: entry.propertyId,
              roof_signal_id: deterministicId("roof", [
                "1.0.0",
                "roof-signal",
                entry.propertyId,
                roofSignal.basis,
              ]),
              source_record_hash: sourceRecordHash(roofSignal),
            },
          ];
        });
        if (roofRows.length > 0)
          await transaction`
          INSERT INTO oracle_roof_signals ${transaction(roofRows)}
          ON CONFLICT (roof_signal_id) DO UPDATE SET
            age_years = EXCLUDED.age_years,
            as_of = EXCLUDED.as_of,
            source_record_hash = EXCLUDED.source_record_hash,
            last_seen_run_id = EXCLUDED.last_seen_run_id
        `;
      }

      const temporal = await applyTemporalReconciliation(
        transaction,
        temporalPlan,
        request.runId,
        temporalDeltas,
      );

      const propertyIds = prepared.properties.map((entry) => entry.propertyId);
      const counts = await transaction<
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
      const duplicates = await transaction<{ count: number }[]>`
        SELECT count(*)::int AS count FROM (
          SELECT exact_folio FROM oracle_properties
          GROUP BY exact_folio HAVING count(*) > 1
        ) duplicate_folios
      `;
      const roofBasisRows = await transaction<
        { basis: string; count: number }[]
      >`
        SELECT basis, count(*)::int AS count
        FROM oracle_roof_signals
        WHERE property_id = ANY(${propertyIds})
        GROUP BY basis ORDER BY basis
      `;
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
          GREATEST(0, EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint AS elapsed_ms
        FROM oracle_pipeline_runs WHERE run_id = ${request.runId}
      `;
      const observed = counts[0] ?? {
        availability: 0,
        buildings: 0,
        coordinates: 0,
        ownership: 0,
        permits: 0,
        roof_signals: 0,
      };
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
        buildings: observed.buildings,
        changedProperties,
        coordinates: observed.coordinates,
        databaseGrowthBytes: Math.max(
          0,
          databaseSizeAfterBytes - databaseSizeBeforeBytes,
        ),
        databaseSizeAfterBytes,
        databaseSizeBeforeBytes,
        diskAvailableBytes: prepared.resourceMetrics.diskAvailableBytes,
        duplicateProperties: duplicates[0]?.count ?? 0,
        elapsedMs,
        explicitUnavailableFacts: observed.availability,
        gisMetrics: prepared.gisMetrics,
        inactiveProperties: temporal.inactiveProperties,
        inactivatedProperties: temporal.inactivatedProperties,
        missingCoordinates: prepared.properties.length - observed.coordinates,
        newProperties,
        ownership: observed.ownership,
        peakRssBytes: Math.max(
          prepared.resourceMetrics.peakRssBytes,
          resourceUsage().maxRSS * 1_024,
        ),
        permitRequestCount: prepared.permitRequestCount,
        permits: observed.permits,
        rejectedRecords: Object.values(prepared.sourceCounts).reduce(
          (sum, count) => sum + count.rejected,
          0,
        ),
        reactivatedProperties: temporal.reactivatedProperties,
        roofSignalBasis: Object.fromEntries(
          roofBasisRows.map((row) => [row.basis, row.count]),
        ),
        roofSignals: observed.roof_signals,
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

      const reconciliationChecks = [
        [
          "dataset_property_count",
          summary.acceptedProperties,
          prepared.selectionSize,
        ],
        ["duplicate_exact_folio", summary.duplicateProperties, 0],
        ["coordinate_coverage", summary.coordinates, prepared.selectionSize],
        ["ownership_coverage", summary.ownership, prepared.selectionSize],
        [
          "explicit_unavailable_coverage",
          summary.explicitUnavailableFacts,
          prepared.selectionSize * UNAVAILABLE_FEATURES.length,
        ],
        ["permit_record_count", summary.permits, 0],
      ] as const;
      for (const [checkName, count, expected] of reconciliationChecks) {
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
            ${count === expected ? "pass" : "warn"}, ${count}, ${expected}
          )
          ON CONFLICT (reconciliation_id) DO UPDATE SET
            status = EXCLUDED.status,
            observed_count = EXCLUDED.observed_count,
            expected_count = EXCLUDED.expected_count
        `;
      }

      await transaction`
        UPDATE oracle_pipeline_runs SET
          status = 'completed',
          completed_at = now(),
          source_counts = ${transaction.json(
            prepared.sourceCounts as unknown as postgres.JSONValue,
          )},
          result_counts = ${transaction.json(
            summary as unknown as postgres.JSONValue,
          )},
          limitations = ${transaction.json([
            ...limitations(prepared.selectionSize),
            ...prepared.sourceLimitations,
          ])}
        WHERE run_id = ${request.runId}
      `;
      await transaction`
        UPDATE oracle_pipeline_attempts
        SET status = 'completed', completed_at = now()
        WHERE run_id = ${request.runId} AND status = 'running'
      `;
      return completeLoaderEffect(transaction, durability, summary);
    });
  } finally {
    await sql.end({ timeout: 5 });
  }
}
