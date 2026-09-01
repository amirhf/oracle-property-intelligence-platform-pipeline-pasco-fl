import type { JsonObject } from "./provider.js";

type SourceClass = "normalized" | "raw";
type UnavailableReason =
  | "ambiguous_match"
  | "not_applicable"
  | "not_provided_by_source"
  | "source_not_collected"
  | "source_unavailable";

const PRIVACY = Object.freeze({
  accuracyQualification: "source_reported_not_independently_verified",
  publicationStatus: "approved_for_publication",
  recordNature: "official_public_record",
});

function isRecord(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function sourceClass(value: unknown): SourceClass | null {
  return value === "raw" || value === "normalized" ? value : null;
}

function unavailableReason(value: unknown): UnavailableReason {
  return value === "ambiguous_match" ||
    value === "not_applicable" ||
    value === "not_provided_by_source" ||
    value === "source_not_collected" ||
    value === "source_unavailable"
    ? value
    : "source_unavailable";
}

function evidenceIds(canonical: JsonObject): Set<string> {
  if (!Array.isArray(canonical.evidence)) return new Set();
  return new Set(
    canonical.evidence.flatMap((value) => {
      if (!isRecord(value) || typeof value.evidenceId !== "string") return [];
      return [value.evidenceId];
    }),
  );
}

function evidenceReferences(
  fact: JsonObject,
  availableEvidence: Set<string>,
): string[] {
  if (!Array.isArray(fact.evidenceRefs)) return [];
  const references = [
    ...new Set(
      fact.evidenceRefs.filter(
        (value): value is string => typeof value === "string",
      ),
    ),
  ];
  if (references.some((reference) => !availableEvidence.has(reference))) {
    throw new Error("Canonical ownership evidence does not resolve");
  }
  return references;
}

function availableFact(
  value: unknown,
  classification: SourceClass,
  evidenceRefs: string[],
): JsonObject {
  if (evidenceRefs.length === 0) {
    throw new Error("Available public ownership requires canonical evidence");
  }
  return {
    availability: "available",
    value,
    class: classification,
    evidenceRefs,
  };
}

function unavailableFact(
  classification: SourceClass,
  reason: UnavailableReason,
  evidenceRefs: string[],
): JsonObject {
  return {
    availability: "unavailable",
    value: null,
    class: classification,
    reason,
    evidenceRefs,
  };
}

function text(value: unknown, maximum = 500): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  if (cleaned.length === 0) return null;
  if (cleaned.length > maximum) {
    throw new Error(
      "Canonical public ownership text exceeds the contract bound",
    );
  }
  return cleaned;
}

function sourceRecords(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function normalizedAddress(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function sourceTextFact(
  value: unknown,
  classification: SourceClass,
  evidenceRefs: string[],
): JsonObject {
  const available = text(value);
  return available === null
    ? unavailableFact(classification, "not_provided_by_source", evidenceRefs)
    : availableFact(available, classification, evidenceRefs);
}

function currentOwners(
  records: JsonObject[],
  classification: SourceClass,
  evidenceRefs: string[],
  fallbackReason: UnavailableReason,
): JsonObject {
  const names = [
    ...new Set(
      records.flatMap((owner) =>
        [text(owner.ownerName1), text(owner.ownerName2)].filter(
          (value): value is string => value !== null,
        ),
      ),
    ),
  ];
  if (names.length === 0) {
    return unavailableFact(classification, fallbackReason, evidenceRefs);
  }
  return availableFact(
    names.map((displayName) => ({ displayName, evidenceRefs })),
    classification,
    evidenceRefs,
  );
}

function classification(
  records: JsonObject[],
  factClass: SourceClass,
  evidenceRefs: string[],
): JsonObject {
  const values = [
    ...new Set(
      records.flatMap((owner) =>
        [
          text(owner.ownershipClassification),
          text(owner.classification),
        ].filter((value): value is string => value !== null),
      ),
    ),
  ];
  if (values.length === 0) {
    return unavailableFact(factClass, "not_provided_by_source", evidenceRefs);
  }
  if (values.length > 1) {
    return unavailableFact(factClass, "ambiguous_match", evidenceRefs);
  }
  return availableFact(values[0], factClass, evidenceRefs);
}

function publicMailingAddress(
  canonical: JsonObject,
  records: JsonObject[],
  classification: SourceClass,
  evidenceRefs: string[],
): JsonObject {
  const candidates = records.map((owner) => ({
    addressLines: [
      text(owner.mailingAddress1, 200),
      text(owner.mailingAddress2, 200),
    ].filter((value): value is string => value !== null),
    country: text(owner.mailingCountry),
    locality: text(owner.mailingCity),
    postalCode: text(owner.mailingZip),
    region: text(owner.mailingState),
  }));
  const selected = candidates.find(
    (candidate) =>
      candidate.addressLines.length > 0 ||
      candidate.country !== null ||
      candidate.locality !== null ||
      candidate.postalCode !== null ||
      candidate.region !== null,
  );
  if (!selected) {
    return unavailableFact(
      classification,
      "not_provided_by_source",
      evidenceRefs,
    );
  }

  const situsFact = isRecord(canonical.situsAddress)
    ? canonical.situsAddress
    : undefined;
  const situs =
    situsFact?.availability === "available" ? text(situsFact.value) : null;
  const lineText = selected.addressLines.join(" ");
  const fullMailing = [
    lineText,
    selected.locality,
    selected.region,
    selected.postalCode,
    selected.country,
  ]
    .filter((value): value is string => value !== null && value.length > 0)
    .join(" ");
  if (
    situs !== null &&
    [lineText, fullMailing].some(
      (candidate) =>
        candidate.length > 0 &&
        normalizedAddress(candidate) === normalizedAddress(situs),
    )
  ) {
    return unavailableFact(classification, "ambiguous_match", evidenceRefs);
  }

  return availableFact(
    {
      addressLines:
        selected.addressLines.length === 0
          ? unavailableFact(
              classification,
              "not_provided_by_source",
              evidenceRefs,
            )
          : availableFact(selected.addressLines, classification, evidenceRefs),
      locality: sourceTextFact(selected.locality, classification, evidenceRefs),
      region: sourceTextFact(selected.region, classification, evidenceRefs),
      postalCode: sourceTextFact(
        selected.postalCode,
        classification,
        evidenceRefs,
      ),
      country: sourceTextFact(selected.country, classification, evidenceRefs),
    },
    classification,
    evidenceRefs,
  );
}

export function projectPublicOwnership(canonical: JsonObject): JsonObject {
  const ownership = isRecord(canonical.ownership) ? canonical.ownership : {};
  const availableEvidence = evidenceIds(canonical);
  const references = evidenceReferences(ownership, availableEvidence);
  const rawClass = sourceClass(ownership.class);
  const factClass = rawClass ?? "raw";
  const isSourceBacked =
    ownership.availability === "available" && rawClass !== null;
  const records = isSourceBacked ? sourceRecords(ownership.value) : [];
  const fallbackReason =
    ownership.availability === "unavailable"
      ? unavailableReason(ownership.reason)
      : isSourceBacked
        ? "not_provided_by_source"
        : "ambiguous_match";

  return {
    currentOwners: currentOwners(
      records,
      factClass,
      references,
      fallbackReason,
    ),
    classification: isSourceBacked
      ? classification(records, factClass, references)
      : unavailableFact(factClass, fallbackReason, references),
    publicMailingAddress: isSourceBacked
      ? publicMailingAddress(canonical, records, factClass, references)
      : unavailableFact(factClass, fallbackReason, references),
    phone: unavailableFact(factClass, "not_provided_by_source", references),
    email: unavailableFact(factClass, "not_provided_by_source", references),
    privacy: { ...PRIVACY },
  };
}
