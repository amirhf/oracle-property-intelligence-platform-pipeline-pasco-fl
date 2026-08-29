import type postgres from "postgres";

import type { PreparedPilot, PreparedProperty } from "../domain/types.js";
import { yearBuiltRoofProxy } from "../domain/signals.js";
import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import {
  DurableConflictError,
  DurableInputError,
} from "../lib/durability-errors.js";
import {
  deterministicId,
  propertyId as canonicalPropertyId,
  sourceRecordHash,
} from "../lib/hash.js";
import {
  PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER,
  type SnapshotCoverage,
} from "../snapshot/coverage.js";
import type { SourceSnapshotManifest } from "../snapshot/model.js";

export type FactCollectionSemantics =
  "explicit_tombstone" | "positive_upsert" | "replace_set";

export interface ProjectionFactInput {
  collectionSemantics: FactCollectionSemantics;
  evidenceRefs: string[];
  factType: string;
  naturalKey: string;
  payload: unknown;
  sourceRecordHash: string;
}

export interface ProjectionLoadResult {
  activeProperties: number;
  changedProperties: number;
  inactiveProperties: number;
  inactivatedProperties: number;
  materializationId: string | null;
  materializationSha256: string | null;
  newProperties: number;
  reactivatedProperties: number;
  sampleIsolated: boolean;
  unchangedProperties: number;
}

interface MaterializedPropertyRow {
  inactivated_at_snapshot_id: string | null;
  inactivation_watermark: Date | string | null;
  is_active: boolean;
  property_id: string;
  property_version_id: string;
}

interface MaterializedFactRow {
  fact_type: string;
  fact_version_id: string;
  natural_key: string;
  property_id: string;
}

export interface PlannedFactChange {
  eventType: "changed" | "new" | "removed" | "unchanged";
  fact: ProjectionFactInput | null;
  fromVersionId: string | null;
  naturalKey: string;
  factType: string;
  toVersionId: string | null;
}

function conflict(message: string): never {
  throw new DurableConflictError(`Projection conflict (${message})`);
}

function codeUnitCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function propertyPayload(entry: PreparedProperty): Record<string, unknown> {
  return {
    acres: entry.parcel.acres,
    exactFolio: entry.parcel.exactFolio,
    heatedSquareFeet: entry.parcel.heatedSquareFeet,
    parcel: entry.parcel,
    propertyId: entry.propertyId,
    siteAddress: entry.siteAddress,
    totalSquareFeet: entry.parcel.totalSquareFeet,
    yearBuilt: entry.yearBuilt,
  };
}

function factsFor(
  entry: PreparedProperty,
  observedThrough: string,
): ProjectionFactInput[] {
  const evidenceRefs = [
    sourceRecordHash({ exactFolio: entry.parcel.exactFolio }),
  ];
  const facts: ProjectionFactInput[] = [];
  entry.owners.forEach((owner, index) => {
    facts.push({
      collectionSemantics: "replace_set",
      evidenceRefs,
      factType: "ownership",
      naturalKey: `owner:${index}:${sourceRecordHash(owner)}`,
      payload: owner,
      sourceRecordHash: sourceRecordHash(owner),
    });
  });
  entry.buildings.forEach((building) => {
    facts.push({
      collectionSemantics: "replace_set",
      evidenceRefs,
      factType: "building",
      naturalKey: `${building.buildingNumber}:${building.buildingSection}`,
      payload: building,
      sourceRecordHash: sourceRecordHash(building),
    });
  });
  if (entry.coordinates) {
    facts.push({
      collectionSemantics: "positive_upsert",
      evidenceRefs,
      factType: "coordinate",
      naturalKey: "pasco_gis:parcel_centroid",
      payload: entry.coordinates,
      sourceRecordHash: sourceRecordHash(entry.coordinates),
    });
  }
  const roof = yearBuiltRoofProxy(entry.yearBuilt, observedThrough);
  facts.push({
    collectionSemantics: "positive_upsert",
    evidenceRefs,
    factType: "roof_signal",
    naturalKey: "year_built_proxy",
    payload: roof,
    sourceRecordHash: sourceRecordHash(roof),
  });
  for (const [feature, reason] of [
    ["permits", "source_unavailable"],
    ["contractors", "source_unavailable"],
    ["phones", "not_provided_by_source"],
    ["emails", "not_provided_by_source"],
    ["sunbiz", "source_not_collected"],
    ["bbb", "source_not_collected"],
  ] as const) {
    const payload = { availability: "unavailable", feature, reason };
    facts.push({
      collectionSemantics: "positive_upsert",
      evidenceRefs,
      factType: "availability",
      naturalKey: feature,
      payload,
      sourceRecordHash: sourceRecordHash(payload),
    });
  }
  return facts.sort((left, right) =>
    codeUnitCompare(
      `${left.factType}\u0000${left.naturalKey}\u0000${left.sourceRecordHash}`,
      `${right.factType}\u0000${right.naturalKey}\u0000${right.sourceRecordHash}`,
    ),
  );
}

function propertySourceHash(entry: PreparedProperty): string {
  return sourceRecordHash(propertyPayload(entry));
}

function factVersionId(propertyId: string, fact: ProjectionFactInput): string {
  return deterministicId("factversion", [
    "1.0.0",
    "child-fact-version",
    propertyId,
    fact.factType,
    fact.naturalKey,
    canonicalJsonSha256(fact.payload),
    fact.sourceRecordHash,
  ]);
}

export function planProjectionFactChanges(options: {
  facts: readonly ProjectionFactInput[];
  priorFacts: readonly MaterializedFactRow[];
  propertyId: string;
  replaceSetTypes: readonly string[];
}): {
  changes: PlannedFactChange[];
  nextFacts: MaterializedFactRow[];
} {
  const replaceSetTypes = new Set(options.replaceSetTypes);
  const keyFor = (factType: string, naturalKey: string) =>
    `${factType}\u0000${naturalKey}`;
  const next = new Map(
    options.priorFacts.map((fact) => [
      keyFor(fact.fact_type, fact.natural_key),
      { ...fact },
    ]),
  );
  const changes = new Map<string, PlannedFactChange>();
  const currentReplaceKeys = new Set(
    options.facts
      .filter((fact) => fact.collectionSemantics === "replace_set")
      .map((fact) => keyFor(fact.factType, fact.naturalKey)),
  );
  for (const [key, prior] of next) {
    if (replaceSetTypes.has(prior.fact_type) && !currentReplaceKeys.has(key)) {
      next.delete(key);
      changes.set(key, {
        eventType: "removed",
        fact: null,
        factType: prior.fact_type,
        fromVersionId: prior.fact_version_id,
        naturalKey: prior.natural_key,
        toVersionId: null,
      });
    }
  }
  for (const fact of options.facts) {
    const key = keyFor(fact.factType, fact.naturalKey);
    const prior = next.get(key);
    if (fact.collectionSemantics === "explicit_tombstone") {
      next.delete(key);
      changes.set(key, {
        eventType: "removed",
        fact,
        factType: fact.factType,
        fromVersionId: prior?.fact_version_id ?? null,
        naturalKey: fact.naturalKey,
        toVersionId: null,
      });
      continue;
    }
    const toVersionId = factVersionId(options.propertyId, fact);
    changes.set(key, {
      eventType: prior
        ? prior.fact_version_id === toVersionId
          ? "unchanged"
          : "changed"
        : "new",
      fact,
      factType: fact.factType,
      fromVersionId: prior?.fact_version_id ?? null,
      naturalKey: fact.naturalKey,
      toVersionId,
    });
    next.set(key, {
      fact_type: fact.factType,
      fact_version_id: toVersionId,
      natural_key: fact.naturalKey,
      property_id: options.propertyId,
    });
  }
  return {
    changes: [...changes.values()].sort((left, right) =>
      codeUnitCompare(
        `${left.factType}\u0000${left.naturalKey}`,
        `${right.factType}\u0000${right.naturalKey}`,
      ),
    ),
    nextFacts: [...next.values()].sort((left, right) =>
      codeUnitCompare(
        `${left.fact_type}\u0000${left.natural_key}`,
        `${right.fact_type}\u0000${right.natural_key}`,
      ),
    ),
  };
}

async function recordSample(
  transaction: postgres.TransactionSql,
  snapshot: SourceSnapshotManifest,
  runId: string,
  prepared: PreparedPilot,
): Promise<ProjectionLoadResult> {
  await transaction`
    INSERT INTO oracle_sample_observation_sets (
      snapshot_id, run_id, scope_id, selection_sha256,
      selection_algorithm, selection_seed
    ) VALUES (
      ${snapshot.snapshotId}, ${runId}, ${snapshot.coverage.scopeId},
      ${snapshot.sampling.selectedRecordSha256}, ${snapshot.sampling.algorithm},
      ${snapshot.sampling.seed}
    ) ON CONFLICT (snapshot_id) DO NOTHING
  `;
  for (const entry of prepared.properties) {
    const payload = propertyPayload(entry);
    const payloadSha256 = canonicalJsonSha256(payload);
    const versionId = deterministicId("propertyversion", [
      "1.0.0",
      "sample-property-version",
      snapshot.snapshotId,
      entry.propertyId,
      payloadSha256,
    ]);
    await transaction`
      INSERT INTO oracle_sample_property_versions (
        version_id, snapshot_id, property_id, parcel_identifier,
        payload_sha256, payload, source_record_sha256
      ) VALUES (
        ${versionId}, ${snapshot.snapshotId}, ${entry.propertyId},
        ${entry.parcel.exactFolio}, ${payloadSha256},
        ${transaction.json(payload as postgres.JSONValue)},
        ${propertySourceHash(entry)}
      ) ON CONFLICT (snapshot_id, property_id) DO NOTHING
    `;
    for (const fact of factsFor(entry, snapshot.observationWindow.end)) {
      const factSha256 = canonicalJsonSha256(fact.payload);
      const factVersionId = deterministicId("factversion", [
        "1.0.0",
        "sample-fact-version",
        snapshot.snapshotId,
        entry.propertyId,
        fact.factType,
        fact.naturalKey,
        factSha256,
      ]);
      await transaction`
        INSERT INTO oracle_sample_fact_versions (
          version_id, snapshot_id, property_version_id, fact_type,
          natural_key, collection_semantics, payload_sha256, payload,
          evidence_refs
        ) VALUES (
          ${factVersionId}, ${snapshot.snapshotId}, ${versionId},
          ${fact.factType}, ${fact.naturalKey}, ${fact.collectionSemantics},
          ${factSha256}, ${transaction.json(fact.payload as postgres.JSONValue)},
          ${transaction.json(fact.evidenceRefs)}
        ) ON CONFLICT (snapshot_id, property_version_id, fact_type, natural_key)
          DO NOTHING
      `;
    }
  }
  return {
    activeProperties: prepared.properties.length,
    changedProperties: 0,
    inactiveProperties: 0,
    inactivatedProperties: 0,
    materializationId: null,
    materializationSha256: null,
    newProperties: prepared.properties.length,
    reactivatedProperties: 0,
    sampleIsolated: true,
    unchangedProperties: 0,
  };
}

function validateAuthority(coverage: SnapshotCoverage): void {
  if (
    coverage.authoritySource.sourceSystem !== "pasco_appraiser" ||
    coverage.authoritySource.sourceIdentifier !==
      PASCO_PARCEL_AUTHORITY_SOURCE_IDENTIFIER
  ) {
    throw new DurableInputError(
      "Projection membership requires verified official Pasco appraiser evidence",
    );
  }
  if (
    coverage.mode === "authoritative_complete" &&
    coverage.completeness.result !== "passed"
  ) {
    throw new DurableInputError(
      "Authoritative projection requires verified completeness evidence",
    );
  }
}

export async function recordProjectionLoad(
  transaction: postgres.TransactionSql,
  snapshot: SourceSnapshotManifest,
  runId: string,
  prepared: PreparedPilot,
): Promise<ProjectionLoadResult> {
  const preparedPropertyIds = new Set<string>();
  const preparedParcels = new Set<string>();
  for (const entry of prepared.properties) {
    if (entry.coordinates) {
      if (
        entry.coordinates.latitude < -90 ||
        entry.coordinates.latitude > 90 ||
        entry.coordinates.longitude < -180 ||
        entry.coordinates.longitude > 180
      ) {
        throw new DurableInputError(
          `Projection coordinate is outside EPSG:4326 bounds (propertyId=${entry.propertyId})`,
        );
      }
    }
    if (
      entry.parcel.exactFolio.length === 0 ||
      entry.propertyId !== canonicalPropertyId(entry.parcel.exactFolio)
    ) {
      throw new DurableInputError("Projection property identity is malformed");
    }
    if (
      preparedPropertyIds.has(entry.propertyId) ||
      preparedParcels.has(entry.parcel.exactFolio)
    ) {
      throw new DurableInputError("Projection property identity is duplicated");
    }
    preparedPropertyIds.add(entry.propertyId);
    preparedParcels.add(entry.parcel.exactFolio);
  }
  if (snapshot.coverage.mode === "sample") {
    return recordSample(transaction, snapshot, runId, prepared);
  }
  validateAuthority(snapshot.coverage);
  const coverage = snapshot.coverage;
  const heads = await transaction<
    {
      authoritative_base_snapshot_id: string | null;
      current_snapshot_id: string;
      revision: number;
    }[]
  >`
    SELECT current_snapshot_id, authoritative_base_snapshot_id, revision
    FROM oracle_projection_heads WHERE scope_id = ${coverage.scopeId}
    FOR UPDATE
  `;
  const head = heads[0];
  const predecessor = coverage.previousProjectionSnapshotId;
  if ((head?.current_snapshot_id ?? null) !== predecessor) {
    conflict("predecessor is not the current projection head");
  }
  if (coverage.mode === "authoritative_complete") {
    const expectedBase = head?.authoritative_base_snapshot_id ?? null;
    if (coverage.previousAuthoritativeSnapshotId !== expectedBase) {
      conflict("authoritative predecessor is not the current base");
    }
  }
  if (head) {
    const priorRows = await transaction<
      {
        watermark_observed_through: Date | string;
        watermark_source_object_sha256: string;
      }[]
    >`
      SELECT watermark_observed_through, watermark_source_object_sha256
      FROM oracle_projection_snapshots
      WHERE snapshot_id = ${head.current_snapshot_id}
    `;
    const prior = priorRows[0];
    if (!prior) conflict("projection head snapshot is missing");
    const priorTime = new Date(prior.watermark_observed_through).valueOf();
    const nextTime = new Date(
      coverage.membershipWatermark.observedThrough,
    ).valueOf();
    if (nextTime < priorTime) conflict("membership watermark moved backwards");
    if (
      nextTime === priorTime &&
      prior.watermark_source_object_sha256 !==
        coverage.membershipWatermark.sourceObjectSha256
    ) {
      conflict("same-time membership watermark has different source bytes");
    }
  }

  const contentSha256 = canonicalJsonSha256(
    prepared.properties.map((entry) => ({
      propertyId: entry.propertyId,
      sourceRecordHash: propertySourceHash(entry),
    })),
  );
  await transaction`
    INSERT INTO oracle_projection_snapshots (
      snapshot_id, run_id, county, coverage_mode, scope_id,
      predecessor_snapshot_id, authoritative_base_snapshot_id,
      authority_source_system, authority_source_identifier, watermark_kind,
      watermark_observed_through, watermark_source_object_sha256,
      content_sha256, completeness_evidence_sha256, sealed
    ) VALUES (
      ${snapshot.snapshotId}, ${runId}, 'pasco', ${coverage.mode},
      ${coverage.scopeId}, ${predecessor},
      ${head?.authoritative_base_snapshot_id ?? null},
      ${coverage.authoritySource.sourceSystem},
      ${coverage.authoritySource.sourceIdentifier},
      ${coverage.membershipWatermark.kind},
      ${coverage.membershipWatermark.observedThrough},
      ${coverage.membershipWatermark.sourceObjectSha256}, ${contentSha256},
      ${coverage.mode === "authoritative_complete" ? coverage.completeness.evidenceSha256 : null},
      false
    )
  `;

  const priorMaterializations = predecessor
    ? await transaction<{ materialization_id: string }[]>`
        SELECT materialization_id FROM oracle_projection_materializations
        WHERE snapshot_id = ${predecessor}
      `
    : [];
  const priorMaterializationId = priorMaterializations[0]?.materialization_id;
  const priorProperties = priorMaterializationId
    ? await transaction<MaterializedPropertyRow[]>`
        SELECT property_id, property_version_id, is_active,
               inactivated_at_snapshot_id, inactivation_watermark
        FROM oracle_projection_materialized_properties
        WHERE materialization_id = ${priorMaterializationId}
      `
    : [];
  const priorFacts = priorMaterializationId
    ? await transaction<MaterializedFactRow[]>`
        SELECT property_id, fact_type, natural_key, fact_version_id
        FROM oracle_projection_materialized_facts
        WHERE materialization_id = ${priorMaterializationId}
      `
    : [];
  const propertyState = new Map(
    priorProperties.map((row) => [row.property_id, { ...row }]),
  );
  const factState = new Map(
    priorFacts.map((row) => [
      `${row.property_id}\u0000${row.fact_type}\u0000${row.natural_key}`,
      { ...row },
    ]),
  );
  const observedIds = new Set<string>();
  const counts = {
    changed: 0,
    inactivated: 0,
    new: 0,
    reactivated: 0,
    unchanged: 0,
  };

  for (const entry of prepared.properties) {
    observedIds.add(entry.propertyId);
    const payload = propertyPayload(entry);
    const payloadSha256 = canonicalJsonSha256(payload);
    const sourceHash = propertySourceHash(entry);
    const versionId = deterministicId("propertyversion", [
      "1.0.0",
      "property-version",
      entry.propertyId,
      payloadSha256,
      sourceHash,
    ]);
    await transaction`
      INSERT INTO oracle_property_versions (
        version_id, property_id, parcel_identifier, payload_sha256, payload,
        source_record_sha256, source_snapshot_id, source_run_id
      ) VALUES (
        ${versionId}, ${entry.propertyId}, ${entry.parcel.exactFolio},
        ${payloadSha256}, ${transaction.json(payload as postgres.JSONValue)},
        ${sourceHash}, ${snapshot.snapshotId}, ${runId}
      ) ON CONFLICT (version_id) DO NOTHING
    `;
    const prior = propertyState.get(entry.propertyId);
    let eventType: "changed" | "new" | "reactivated" | "unchanged";
    if (!prior) eventType = "new";
    else if (!prior.is_active) {
      const inactiveAt = prior.inactivation_watermark
        ? new Date(prior.inactivation_watermark).valueOf()
        : Number.POSITIVE_INFINITY;
      const observedAt = new Date(
        coverage.membershipWatermark.observedThrough,
      ).valueOf();
      if (observedAt <= inactiveAt)
        conflict(
          "partial reactivation evidence is not newer than inactivation",
        );
      eventType = "reactivated";
    } else if (prior.property_version_id === versionId) eventType = "unchanged";
    else eventType = "changed";
    counts[eventType] += 1;
    propertyState.set(entry.propertyId, {
      inactivated_at_snapshot_id: null,
      inactivation_watermark: null,
      is_active: true,
      property_id: entry.propertyId,
      property_version_id: versionId,
    });
    await transaction`
      INSERT INTO oracle_projection_property_changes (
        snapshot_id, property_id, event_type, from_version_id, to_version_id,
        reason
      ) VALUES (
        ${snapshot.snapshotId}, ${entry.propertyId}, ${eventType},
        ${prior?.property_version_id ?? null}, ${versionId}, null
      )
    `;

    const facts = factsFor(entry, snapshot.observationWindow.end);
    const factPlan = planProjectionFactChanges({
      facts,
      priorFacts: [...factState.values()].filter(
        (fact) => fact.property_id === entry.propertyId,
      ),
      propertyId: entry.propertyId,
      replaceSetTypes: ["building", "ownership"],
    });
    for (const [key, fact] of factState) {
      if (fact.property_id === entry.propertyId) factState.delete(key);
    }
    for (const fact of factPlan.nextFacts) {
      factState.set(
        `${entry.propertyId}\u0000${fact.fact_type}\u0000${fact.natural_key}`,
        fact,
      );
    }
    for (const change of factPlan.changes) {
      if (change.fact && change.toVersionId) {
        const factSha256 = canonicalJsonSha256(change.fact.payload);
        await transaction`
          INSERT INTO oracle_child_fact_versions (
            version_id, property_id, fact_type, natural_key,
            collection_semantics, payload_sha256, payload, source_record_sha256,
            source_snapshot_id, source_run_id, evidence_refs
          ) VALUES (
            ${change.toVersionId}, ${entry.propertyId}, ${change.fact.factType},
            ${change.fact.naturalKey}, ${change.fact.collectionSemantics},
            ${factSha256},
            ${transaction.json(change.fact.payload as postgres.JSONValue)},
            ${change.fact.sourceRecordHash}, ${snapshot.snapshotId}, ${runId},
            ${transaction.json(change.fact.evidenceRefs)}
          ) ON CONFLICT (version_id) DO NOTHING
        `;
      }
      await transaction`
        INSERT INTO oracle_projection_fact_changes (
          snapshot_id, property_id, fact_type, natural_key, event_type,
          from_version_id, to_version_id
        ) VALUES (
          ${snapshot.snapshotId}, ${entry.propertyId}, ${change.factType},
          ${change.naturalKey}, ${change.eventType},
          ${change.fromVersionId}, ${change.toVersionId}
        ) ON CONFLICT (snapshot_id, property_id, fact_type, natural_key)
          DO NOTHING
      `;
    }
  }

  if (coverage.mode === "authoritative_complete") {
    for (const [propertyId, prior] of propertyState) {
      if (prior.is_active && !observedIds.has(propertyId)) {
        prior.is_active = false;
        prior.inactivated_at_snapshot_id = snapshot.snapshotId;
        prior.inactivation_watermark =
          coverage.membershipWatermark.observedThrough;
        counts.inactivated += 1;
        await transaction`
          INSERT INTO oracle_projection_property_changes (
            snapshot_id, property_id, event_type, from_version_id,
            to_version_id, reason
          ) VALUES (
            ${snapshot.snapshotId}, ${propertyId}, 'inactivated',
            ${prior.property_version_id}, ${prior.property_version_id},
            'absent_from_authoritative_complete_snapshot'
          )
        `;
      }
    }
  }

  const sortedProperties = [...propertyState.values()].sort((left, right) =>
    codeUnitCompare(left.property_id, right.property_id),
  );
  const sortedFacts = [...factState.values()].sort((left, right) =>
    codeUnitCompare(
      `${left.property_id}\u0000${left.fact_type}\u0000${left.natural_key}`,
      `${right.property_id}\u0000${right.fact_type}\u0000${right.natural_key}`,
    ),
  );
  const materializationSha256 = canonicalJsonSha256({
    facts: sortedFacts.map((fact) => fact.fact_version_id),
    properties: sortedProperties.map((property) => ({
      active: property.is_active,
      propertyId: property.property_id,
      versionId: property.property_version_id,
    })),
    scopeId: coverage.scopeId,
    snapshotId: snapshot.snapshotId,
  });
  const materializationId = deterministicId("materialization", [
    "1.0.0",
    "sealed-projection",
    materializationSha256,
  ]);
  const activeCount = sortedProperties.filter(
    (property) => property.is_active,
  ).length;
  await transaction`
    INSERT INTO oracle_projection_materializations (
      materialization_id, snapshot_id, scope_id, materialization_sha256,
      property_count, active_count, inactive_count, sealed
    ) VALUES (
      ${materializationId}, ${snapshot.snapshotId}, ${coverage.scopeId},
      ${materializationSha256}, ${sortedProperties.length}, ${activeCount},
      ${sortedProperties.length - activeCount}, true
    )
  `;
  for (const property of sortedProperties) {
    await transaction`
      INSERT INTO oracle_projection_materialized_properties (
        materialization_id, property_id, property_version_id, is_active,
        inactivated_at_snapshot_id, inactivation_watermark
      ) VALUES (
        ${materializationId}, ${property.property_id},
        ${property.property_version_id}, ${property.is_active},
        ${property.inactivated_at_snapshot_id}, ${property.inactivation_watermark}
      )
    `;
  }
  for (const fact of sortedFacts) {
    await transaction`
      INSERT INTO oracle_projection_materialized_facts (
        materialization_id, property_id, fact_type, natural_key,
        fact_version_id
      ) VALUES (
        ${materializationId}, ${fact.property_id}, ${fact.fact_type},
        ${fact.natural_key}, ${fact.fact_version_id}
      )
    `;
  }
  const authoritativeBase =
    coverage.mode === "authoritative_complete"
      ? snapshot.snapshotId
      : (head?.authoritative_base_snapshot_id ?? null);
  if (head) {
    const updated = await transaction`
      UPDATE oracle_projection_heads SET
        current_snapshot_id = ${snapshot.snapshotId},
        authoritative_base_snapshot_id = ${authoritativeBase},
        revision = revision + 1, updated_at = now()
      WHERE scope_id = ${coverage.scopeId}
        AND current_snapshot_id = ${predecessor}
        AND revision = ${head.revision}
      RETURNING scope_id
    `;
    if (updated.length !== 1) conflict("concurrent projection CAS lost");
  } else {
    await transaction`
      INSERT INTO oracle_projection_heads (
        scope_id, county, current_snapshot_id,
        authoritative_base_snapshot_id, revision
      ) VALUES (
        ${coverage.scopeId}, 'pasco', ${snapshot.snapshotId},
        ${authoritativeBase}, 1
      )
    `;
  }
  await transaction`
    UPDATE oracle_projection_snapshots SET sealed = true
    WHERE snapshot_id = ${snapshot.snapshotId} AND NOT sealed
  `;
  const activeProperties = sortedProperties.filter(
    (row) => row.is_active,
  ).length;
  return {
    activeProperties,
    changedProperties: counts.changed,
    inactiveProperties: sortedProperties.length - activeProperties,
    inactivatedProperties: counts.inactivated,
    materializationId,
    materializationSha256,
    newProperties: counts.new,
    reactivatedProperties: counts.reactivated,
    sampleIsolated: false,
    unchangedProperties: counts.unchanged,
  };
}

export async function loadSealedProjectionForPublication(
  sql: postgres.Sql | postgres.TransactionSql,
  snapshotId: string,
): Promise<{
  facts: Array<{
    evidenceRefs: unknown;
    factType: string;
    naturalKey: string;
    payload: unknown;
    propertyId: string;
    versionId: string;
  }>;
  materializationId: string;
  materializationSha256: string;
  properties: Array<{
    isActive: boolean;
    parcelIdentifier: string;
    payload: unknown;
    propertyId: string;
    versionId: string;
  }>;
}> {
  const materializations = await sql<
    { materialization_id: string; materialization_sha256: string }[]
  >`
    SELECT materialization_id, materialization_sha256
    FROM oracle_projection_materializations
    WHERE snapshot_id = ${snapshotId} AND sealed
  `;
  const materialization = materializations[0];
  if (!materialization)
    throw new DurableInputError(
      "Publication requires one sealed projection materialization",
    );
  const properties = await sql<
    {
      is_active: boolean;
      parcel_identifier: string;
      payload: unknown;
      property_id: string;
      property_version_id: string;
    }[]
  >`
    SELECT membership.property_id, membership.property_version_id,
           membership.is_active, version.parcel_identifier, version.payload
    FROM oracle_projection_materialized_properties membership
    JOIN oracle_property_versions version
      ON version.version_id = membership.property_version_id
    WHERE membership.materialization_id = ${materialization.materialization_id}
    ORDER BY version.parcel_identifier, membership.property_id
  `;
  const facts = await sql<
    {
      evidence_refs: unknown;
      fact_type: string;
      fact_version_id: string;
      natural_key: string;
      payload: unknown;
      property_id: string;
    }[]
  >`
    SELECT membership.property_id, membership.fact_type,
           membership.natural_key, membership.fact_version_id,
           version.payload, version.evidence_refs
    FROM oracle_projection_materialized_facts membership
    JOIN oracle_child_fact_versions version
      ON version.version_id = membership.fact_version_id
    WHERE membership.materialization_id = ${materialization.materialization_id}
    ORDER BY membership.property_id, membership.fact_type, membership.natural_key
  `;
  return {
    facts: facts.map((fact) => ({
      evidenceRefs: fact.evidence_refs,
      factType: fact.fact_type,
      naturalKey: fact.natural_key,
      payload: fact.payload,
      propertyId: fact.property_id,
      versionId: fact.fact_version_id,
    })),
    materializationId: materialization.materialization_id,
    materializationSha256: materialization.materialization_sha256,
    properties: properties.map((property) => ({
      isActive: property.is_active,
      parcelIdentifier: property.parcel_identifier,
      payload: property.payload,
      propertyId: property.property_id,
      versionId: property.property_version_id,
    })),
  };
}
