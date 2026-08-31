import { deterministicId, sourceRecordHash } from "../lib/hash.js";

const CONTRACT_VERSION = "1.0.0";

export interface MaterializedPublicationFact {
  evidenceRefs: unknown;
  factType: string;
  naturalKey: string;
  payload: unknown;
  sourceRecordHash: string;
  sourceRunId: string;
  sourceSnapshotId: string;
  versionId: string;
}

export interface MaterializedPublicationProperty {
  exactFolio: string;
  latitude: number | null;
  longitude: number | null;
  parcelId: string;
  propertyId: string;
  siteAddress: string | null;
  siteCity: string | null;
  siteZip: string | null;
  yearBuilt: number | null;
}

export interface PublicationEvidenceSources {
  appraiserBuildingUrl: string | null;
  appraiserOwnersUrl: string | null;
  appraiserParcelUrl: string | null;
  appraiserSiteAddressUrl: string | null;
  snapshotId: string;
}

interface CoreBinding {
  payload: {
    parcel: unknown;
    siteAddress: unknown;
  };
  sourceRunId: string;
  sourceSnapshotId: string;
  versionId: string;
}

interface CanonicalFact {
  availability: "available" | "unavailable";
  class: "derived" | "normalized" | "raw";
  derivation?: Record<string, unknown>;
  evidenceRefs: string[];
  reason?:
    "not_provided_by_source" | "source_not_collected" | "source_unavailable";
  value: unknown;
}

interface Evidence {
  evidenceId: string;
  loadedAt: string;
  observedAt: string | null;
  publishedCid: null;
  retrievedAt: string;
  sourceArtifactUri: string;
  sourceName: string;
  sourceRecordHash: string;
  sourceRecordKey: string;
  sourceSystem: string;
  sourceUrl: string | null;
}

function available(
  value: unknown,
  classification: CanonicalFact["class"],
  evidenceRefs: string[],
  derivation?: Record<string, unknown>,
): CanonicalFact {
  return {
    availability: "available",
    value,
    class: classification,
    evidenceRefs,
    ...(derivation ? { derivation } : {}),
  };
}

function unavailable(
  classification: CanonicalFact["class"],
  reason: NonNullable<CanonicalFact["reason"]>,
  evidenceRefs: string[],
): CanonicalFact {
  return {
    availability: "unavailable",
    value: null,
    class: classification,
    reason,
    evidenceRefs,
  };
}

function text(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().replaceAll(/\s+/g, " ");
  if (!/[\p{L}\p{N}]/u.test(normalized)) return null;
  return /^(?:fl|florida)$/i.test(normalized) ? null : normalized;
}

export function meaningfulSitusAddress(parts: {
  city: string | null;
  siteAddress: string | null;
  zipCode: string | null;
}): string | null {
  const street = text(parts.siteAddress);
  const city = text(parts.city);
  const zip = text(parts.zipCode);
  if (street === null && city === null && zip === null) return null;
  return [street, city, "FL", zip].filter((value) => value !== null).join(", ");
}

function factEvidenceId(fact: MaterializedPublicationFact): string {
  return deterministicId("evidence", [
    CONTRACT_VERSION,
    "projection-fact-evidence",
    fact.versionId,
    fact.sourceRecordHash,
  ]);
}

function exactEvidenceRefs(value: unknown): string[] {
  if (!Array.isArray(value)) {
    throw new Error("Materialized publication fact evidence refs are invalid");
  }
  const refs = value.map((entry) => {
    if (typeof entry !== "string" || entry.length === 0) {
      throw new Error(
        "Materialized publication fact evidence refs are invalid",
      );
    }
    return entry;
  });
  return [...new Set(refs)].sort();
}

function evidenceForFact(
  fact: MaterializedPublicationFact,
  loadedAt: string,
  sources: PublicationEvidenceSources,
): Evidence {
  // Validate the immutable upstream evidence binding even though the frozen
  // canonical Evidence shape cannot embed the full source-ref array.
  exactEvidenceRefs(fact.evidenceRefs);
  const appraiser =
    fact.factType === "building" || fact.factType === "ownership";
  const coordinate = fact.factType === "coordinate";
  const sourceUrl =
    fact.factType === "building"
      ? sources.appraiserBuildingUrl
      : fact.factType === "ownership"
        ? sources.appraiserOwnersUrl
        : null;
  const coordinatePayload = coordinate
    ? (fact.payload as { sourceLastUpdate?: unknown })
    : null;
  const sourceLastUpdate = coordinatePayload?.sourceLastUpdate;
  const observedAt =
    typeof sourceLastUpdate === "string" &&
    Number.isFinite(Date.parse(sourceLastUpdate))
      ? new Date(sourceLastUpdate).toISOString()
      : null;
  return {
    evidenceId: factEvidenceId(fact),
    sourceSystem: coordinate
      ? "pasco_gis"
      : appraiser
        ? "pasco_appraiser"
        : "oracle_projection",
    sourceName: coordinate
      ? "Pasco County GIS parcel coordinate observation"
      : fact.factType === "building"
        ? "Pasco County Property Appraiser building record"
        : fact.factType === "ownership"
          ? "Pasco County Property Appraiser ownership record"
          : `Oracle sealed projection ${fact.factType} fact`,
    sourceRecordKey: fact.versionId,
    sourceUrl,
    sourceArtifactUri: `artifact://pasco/projection/${fact.sourceSnapshotId}/${fact.versionId}`,
    sourceRecordHash: fact.sourceRecordHash,
    observedAt,
    // Related-source acquisition timestamps were not recorded. The required
    // canonical retrieval field therefore binds the immutable normalized-load
    // checkpoint, while observedAt remains explicitly null.
    retrievedAt: loadedAt,
    loadedAt,
    publishedCid: null,
  };
}

function coreEvidence(
  options: {
    hash: string;
    keySuffix: "parcel" | "site-address";
    observedAt: string | null;
    sourceName: string;
    sourceUrl: string | null;
  },
  core: CoreBinding,
  loadedAt: string,
): Evidence {
  const evidenceId = deterministicId("evidence", [
    CONTRACT_VERSION,
    "projection-core-evidence",
    core.versionId,
    options.keySuffix,
    options.hash,
  ]);
  return {
    evidenceId,
    sourceSystem: "pasco_appraiser",
    sourceName: options.sourceName,
    sourceRecordKey: `${core.versionId}:${options.keySuffix}`,
    sourceUrl: options.sourceUrl,
    sourceArtifactUri: `artifact://pasco/projection/${core.sourceSnapshotId}/${core.versionId}/${options.keySuffix}`,
    sourceRecordHash: options.hash,
    observedAt: options.observedAt,
    retrievedAt: loadedAt,
    loadedAt,
    publishedCid: null,
  };
}

function factsByType(
  facts: readonly MaterializedPublicationFact[],
  type: string,
): MaterializedPublicationFact[] {
  return facts.filter((fact) => fact.factType === type);
}

function availabilityFact(
  facts: readonly MaterializedPublicationFact[],
  feature: string,
): MaterializedPublicationFact {
  const fact = facts.find(
    (candidate) =>
      candidate.factType === "availability" && candidate.naturalKey === feature,
  );
  if (!fact) {
    throw new Error(`Materialized publication lacks ${feature} availability`);
  }
  return fact;
}

function optionalAvailabilityFact(
  facts: readonly MaterializedPublicationFact[],
  feature: string,
): MaterializedPublicationFact | null {
  return (
    facts.find(
      (candidate) =>
        candidate.factType === "availability" &&
        candidate.naturalKey === feature,
    ) ?? null
  );
}

export function buildMaterializedCanonicalProperty(options: {
  allowedSnapshotIds: ReadonlySet<string>;
  asOf: string;
  core: CoreBinding;
  facts: readonly MaterializedPublicationFact[];
  loadedAt: string;
  parcelObservedAt: string;
  property: MaterializedPublicationProperty;
  roofSignal: {
    ageYears: number | null;
    basis: string | null;
    basisQuality: string | null;
  };
  sources: PublicationEvidenceSources;
}): Record<string, unknown> {
  if (!options.allowedSnapshotIds.has(options.core.sourceSnapshotId)) {
    throw new Error("Materialized property version is outside the bound chain");
  }
  for (const fact of options.facts) {
    if (
      !options.allowedSnapshotIds.has(fact.sourceSnapshotId) ||
      fact.sourceRunId.length === 0 ||
      fact.versionId.length === 0
    ) {
      throw new Error("Materialized publication fact identity is incomplete");
    }
  }

  const parcelEvidence = coreEvidence(
    {
      hash: sourceRecordHash(options.core.payload.parcel),
      keySuffix: "parcel",
      observedAt: options.parcelObservedAt,
      sourceName: "Pasco County Property Appraiser parcel record",
      sourceUrl: options.sources.appraiserParcelUrl,
    },
    options.core,
    options.loadedAt,
  );
  const address = meaningfulSitusAddress({
    city: options.property.siteCity,
    siteAddress: options.property.siteAddress,
    zipCode: options.property.siteZip,
  });
  const addressEvidence =
    options.core.payload.siteAddress === null
      ? null
      : coreEvidence(
          {
            hash: sourceRecordHash(options.core.payload.siteAddress),
            keySuffix: "site-address",
            observedAt: null,
            sourceName: "Pasco County Property Appraiser site-address record",
            sourceUrl: options.sources.appraiserSiteAddressUrl,
          },
          options.core,
          options.loadedAt,
        );
  const factEvidence = new Map(
    options.facts.map((fact) => [
      fact.versionId,
      evidenceForFact(fact, options.loadedAt, options.sources),
    ]),
  );
  const evidenceFor = (fact: MaterializedPublicationFact) => {
    const evidence = factEvidence.get(fact.versionId);
    if (!evidence) throw new Error("Materialized fact evidence is unresolved");
    return evidence;
  };
  const evidenceIds = (facts: readonly MaterializedPublicationFact[]) =>
    facts.map((fact) => evidenceFor(fact).evidenceId).sort();

  const buildingFacts = factsByType(options.facts, "building");
  const ownershipFacts = factsByType(options.facts, "ownership");
  for (const fact of ownershipFacts) {
    const owner = fact.payload as {
      ownerName1?: unknown;
      ownerName2?: unknown;
    };
    if (text(owner.ownerName1) === null && text(owner.ownerName2) === null) {
      throw new Error("Materialized ownership fact lacks an owner identity");
    }
  }
  const coordinateFact = factsByType(options.facts, "coordinate")[0] ?? null;
  const roofFact = factsByType(options.facts, "roof_signal")[0] ?? null;
  const unavailableRef = (feature: string) =>
    evidenceFor(availabilityFact(options.facts, feature)).evidenceId;
  const ownershipValues = ownershipFacts.map((fact) => fact.payload);
  const referenced = new Map<string, Evidence>();
  const include = (evidence: Evidence) =>
    referenced.set(evidence.evidenceId, evidence);
  include(parcelEvidence);
  if (addressEvidence) include(addressEvidence);
  for (const fact of [
    ...buildingFacts,
    ...ownershipFacts,
    ...(coordinateFact ? [coordinateFact] : []),
    ...(roofFact ? [roofFact] : []),
    availabilityFact(options.facts, "permits"),
    availabilityFact(options.facts, "contractors"),
    ...(address === null
      ? [optionalAvailabilityFact(options.facts, "site_address")].filter(
          (fact): fact is MaterializedPublicationFact => fact !== null,
        )
      : []),
    ...(coordinateFact === null
      ? [availabilityFact(options.facts, "coordinates")]
      : []),
    ...(ownershipFacts.length === 0
      ? [availabilityFact(options.facts, "ownership")]
      : []),
    ...(buildingFacts.length === 0
      ? [availabilityFact(options.facts, "building")]
      : []),
    ...(roofFact === null
      ? [availabilityFact(options.facts, "year_built_proxy")]
      : []),
  ]) {
    include(evidenceFor(fact));
  }

  const buildingEvidenceRefs =
    buildingFacts.length > 0
      ? evidenceIds(buildingFacts)
      : [unavailableRef("building")];
  return {
    entityType: "property",
    contractVersion: CONTRACT_VERSION,
    propertyId: options.property.propertyId,
    parcelId: options.property.parcelId,
    county: "pasco",
    sourceSystem: "pasco_appraiser",
    folio: available(options.property.exactFolio, "raw", [
      parcelEvidence.evidenceId,
    ]),
    parcelIdentifier: available(options.property.exactFolio, "raw", [
      parcelEvidence.evidenceId,
    ]),
    situsAddress:
      address === null
        ? unavailable("normalized", "not_provided_by_source", [
            addressEvidence?.evidenceId ?? unavailableRef("site_address"),
          ])
        : available(address, "normalized", [addressEvidence!.evidenceId]),
    coordinates:
      coordinateFact === null ||
      options.property.latitude === null ||
      options.property.longitude === null
        ? unavailable("normalized", "not_provided_by_source", [
            unavailableRef("coordinates"),
          ])
        : available(
            {
              latitude: options.property.latitude,
              longitude: options.property.longitude,
              crs: "EPSG:4326",
            },
            "normalized",
            [evidenceFor(coordinateFact).evidenceId],
          ),
    yearBuilt:
      options.property.yearBuilt === null
        ? unavailable("raw", "not_provided_by_source", [
            unavailableRef("year_built_proxy"),
          ])
        : available(options.property.yearBuilt, "raw", buildingEvidenceRefs),
    roofInstallationDate: unavailable(
      "raw",
      "not_provided_by_source",
      buildingEvidenceRefs,
    ),
    roofInstallationYear: unavailable(
      "raw",
      "not_provided_by_source",
      buildingEvidenceRefs,
    ),
    roofAgeSignal:
      roofFact === null ||
      options.roofSignal.ageYears === null ||
      options.roofSignal.basis === null ||
      options.roofSignal.basisQuality === null
        ? unavailable("derived", "not_provided_by_source", [
            unavailableRef("year_built_proxy"),
          ])
        : available(
            {
              ageYears: options.roofSignal.ageYears,
              precision: "year",
              basis: options.roofSignal.basis,
              basisQuality: options.roofSignal.basisQuality,
              asOf: options.asOf,
            },
            "derived",
            evidenceIds([...buildingFacts, roofFact]),
            {
              rule: "year_difference_proxy",
              ruleVersion: CONTRACT_VERSION,
              asOf: options.asOf,
              inputs: ["property.yearBuilt"],
            },
          ),
    ownership:
      ownershipValues.length > 0
        ? available(ownershipValues, "raw", evidenceIds(ownershipFacts))
        : unavailable("raw", "not_provided_by_source", [
            unavailableRef("ownership"),
          ]),
    permits: [],
    evidence: [...referenced.values()].sort((left, right) =>
      left.evidenceId < right.evidenceId
        ? -1
        : left.evidenceId > right.evidenceId
          ? 1
          : 0,
    ),
    freshness: {
      observedAt: options.parcelObservedAt,
      retrievedAt: options.loadedAt,
      loadedAt: options.loadedAt,
      publishedAt: null,
      computedAt: options.asOf,
      sourceCadence: "weekly appraiser working roll",
    },
  };
}
