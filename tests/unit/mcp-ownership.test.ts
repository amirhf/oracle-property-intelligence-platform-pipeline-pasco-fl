import { describe, expect, it } from "vitest";

import type { JsonObject } from "../../src/mcp/provider.js";
import { projectPublicOwnership } from "../../src/mcp/ownership.js";

const EVIDENCE_ID = "evidence_synthetic_ownership";

function canonicalOwnership(
  value: JsonObject[] | null,
  options: {
    availability?: "available" | "unavailable";
    classification?: "derived" | "inferred" | "normalized" | "raw";
    evidenceRefs?: string[];
    reason?: string;
    situsAddress?: string;
  } = {},
): JsonObject {
  const availability = options.availability ?? "available";
  return {
    evidence: [{ evidenceId: EVIDENCE_ID }],
    situsAddress: {
      availability: "available",
      value: options.situsAddress ?? "SYNTHETIC SITUS ONLY",
      class: "raw",
      evidenceRefs: [EVIDENCE_ID],
    },
    ownership:
      availability === "available"
        ? {
            availability,
            value,
            class: options.classification ?? "raw",
            evidenceRefs: options.evidenceRefs ?? [EVIDENCE_ID],
          }
        : {
            availability,
            value: null,
            class: options.classification ?? "raw",
            reason: options.reason ?? "not_provided_by_source",
            evidenceRefs: options.evidenceRefs ?? [EVIDENCE_ID],
          },
  };
}

function fact(value: unknown): JsonObject {
  return value as JsonObject;
}

describe("MCP v1.2 public ownership projection", () => {
  it("preserves multiple source owners and resolves evidence for each name", () => {
    const ownership = projectPublicOwnership(
      canonicalOwnership([
        {
          ownerName1: "SYNTHETIC OWNER ALPHA",
          ownerName2: "SYNTHETIC OWNER BETA",
        },
      ]),
    );
    const currentOwners = fact(ownership.currentOwners);
    expect(currentOwners.availability).toBe("available");
    const names = currentOwners.value as JsonObject[];
    expect(names).toHaveLength(2);
    expect(
      names.every(
        (owner) =>
          typeof owner.displayName === "string" &&
          (owner.evidenceRefs as string[]).includes(EVIDENCE_ID),
      ),
    ).toBe(true);
  });

  it("represents unavailable current owners without names", () => {
    const ownership = projectPublicOwnership(
      canonicalOwnership(null, { availability: "unavailable" }),
    );
    expect(ownership.currentOwners).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "not_provided_by_source",
    });
  });

  it("projects partial mailing components independently with evidence", () => {
    const ownership = projectPublicOwnership(
      canonicalOwnership([
        {
          ownerName1: "SYNTHETIC OWNER",
          mailingAddress1: "SYNTHETIC MAILING LINE",
          mailingState: "FL",
        },
      ]),
    );
    const mailing = fact(ownership.publicMailingAddress);
    expect(mailing.availability).toBe("available");
    const components = fact(mailing.value);
    expect(fact(components.addressLines)).toMatchObject({
      availability: "available",
      evidenceRefs: [EVIDENCE_ID],
    });
    expect(fact(components.region)).toMatchObject({
      availability: "available",
      evidenceRefs: [EVIDENCE_ID],
    });
    for (const key of ["locality", "postalCode", "country"]) {
      expect(fact(components[key])).toMatchObject({
        availability: "unavailable",
        value: null,
        reason: "not_provided_by_source",
      });
    }
  });

  it("never substitutes the situs address for public mailing data", () => {
    const missing = projectPublicOwnership(
      canonicalOwnership([{ ownerName1: "SYNTHETIC OWNER" }], {
        situsAddress: "SYNTHETIC SITUS ONLY",
      }),
    );
    expect(missing.publicMailingAddress).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "not_provided_by_source",
    });

    const collision = projectPublicOwnership(
      canonicalOwnership(
        [
          {
            ownerName1: "SYNTHETIC OWNER",
            mailingAddress1: "SYNTHETIC SITUS ONLY",
          },
        ],
        { situsAddress: "SYNTHETIC SITUS ONLY" },
      ),
    );
    expect(collision.publicMailingAddress).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "ambiguous_match",
    });
  });

  it("publishes classification only when explicitly source-backed", () => {
    const absent = projectPublicOwnership(
      canonicalOwnership([{ ownerName1: "SYNTHETIC OWNER LLC" }]),
    );
    expect(absent.classification).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "not_provided_by_source",
    });

    const explicit = projectPublicOwnership(
      canonicalOwnership([
        {
          ownerName1: "SYNTHETIC OWNER",
          ownershipClassification: "source-reported class",
        },
      ]),
    );
    expect(explicit.classification).toMatchObject({
      availability: "available",
      class: "raw",
      evidenceRefs: [EVIDENCE_ID],
    });
  });

  it("does not expose unsupported or inferred contact facts", () => {
    const sourceFields = projectPublicOwnership(
      canonicalOwnership([
        {
          ownerName1: "SYNTHETIC OWNER",
          phone: "unsupported-contact-value",
          email: "unsupported-contact-value",
        },
      ]),
    );
    expect(sourceFields.phone).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "not_provided_by_source",
    });
    expect(sourceFields.email).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "not_provided_by_source",
    });

    const inferred = projectPublicOwnership(
      canonicalOwnership([{ ownerName1: "SYNTHETIC OWNER" }], {
        classification: "inferred",
      }),
    );
    expect(inferred.currentOwners).toMatchObject({
      availability: "unavailable",
      value: null,
      reason: "ambiguous_match",
    });
    expect(inferred.classification).toMatchObject({
      availability: "unavailable",
      value: null,
    });
  });

  it("returns the exact frozen privacy constants", () => {
    const ownership = projectPublicOwnership(
      canonicalOwnership([{ ownerName1: "SYNTHETIC OWNER" }]),
    );
    expect(ownership.privacy).toEqual({
      accuracyQualification: "source_reported_not_independently_verified",
      publicationStatus: "approved_for_publication",
      recordNature: "official_public_record",
    });
  });

  it("rejects available ownership whose evidence does not resolve", () => {
    expect(() =>
      projectPublicOwnership(
        canonicalOwnership([{ ownerName1: "SYNTHETIC OWNER" }], {
          evidenceRefs: ["evidence_missing"],
        }),
      ),
    ).toThrow("Canonical ownership evidence does not resolve");
  });
});
