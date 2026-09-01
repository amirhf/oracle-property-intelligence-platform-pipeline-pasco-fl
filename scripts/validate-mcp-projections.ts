import { McpContractRegistry } from "../src/mcp/contracts.js";
import {
  MCP_CONTRACT_VERSION,
  MCP_SCHEMA_SHA256,
} from "../src/mcp/constants.js";
import { projectPublicProperty } from "../src/mcp/runtime.js";
import { realMcpHarness } from "../tests/helpers/mcp-real.js";

type JsonObject = Record<string, unknown>;

interface ProjectionCounts {
  classificationAvailable: number;
  classificationUnavailable: number;
  currentOwnersAvailable: number;
  currentOwnersUnavailable: number;
  emailAvailable: number;
  emailUnavailable: number;
  evidenceFailures: number;
  mailingAddressAvailable: number;
  mailingAddressUnavailable: number;
  mailingComponentsAvailable: number;
  mailingComponentsUnavailable: number;
  ownerNamesAvailable: number;
  phoneAvailable: number;
  phoneUnavailable: number;
  projectedProperties: number;
  schemaFailures: number;
  situsMailingCollisions: number;
}

function record(value: unknown): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Projection validation encountered an invalid object");
  }
  return value as JsonObject;
}

function normalizeAddress(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function factValue(value: JsonObject): unknown {
  return value.availability === "available" ? value.value : null;
}

function countFact(
  value: JsonObject,
  available: keyof ProjectionCounts,
  unavailable: keyof ProjectionCounts,
  counts: ProjectionCounts,
): void {
  counts[value.availability === "available" ? available : unavailable] += 1;
}

function validateReferences(property: JsonObject): number {
  const evidence = property.evidence as JsonObject[];
  const ids = new Set(evidence.map((item) => item.evidenceId as string));
  let failures = 0;
  const visit = (value: unknown): void => {
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    if (value === null || typeof value !== "object") return;
    for (const [key, entry] of Object.entries(value)) {
      if (key === "evidenceRefs" && Array.isArray(entry)) {
        failures += entry.filter(
          (reference) => typeof reference !== "string" || !ids.has(reference),
        ).length;
      } else {
        visit(entry);
      }
    }
  };
  visit(property.ownership);
  return failures;
}

function mailingText(mailing: JsonObject): string {
  if (mailing.availability !== "available") return "";
  const value = record(mailing.value);
  return ["addressLines", "locality", "region", "postalCode", "country"]
    .flatMap((key) => {
      const component = record(value[key]);
      const content = factValue(component);
      return Array.isArray(content) ? content : [content];
    })
    .filter((value): value is string => typeof value === "string")
    .join(" ");
}

const counts: ProjectionCounts = {
  classificationAvailable: 0,
  classificationUnavailable: 0,
  currentOwnersAvailable: 0,
  currentOwnersUnavailable: 0,
  emailAvailable: 0,
  emailUnavailable: 0,
  evidenceFailures: 0,
  mailingAddressAvailable: 0,
  mailingAddressUnavailable: 0,
  mailingComponentsAvailable: 0,
  mailingComponentsUnavailable: 0,
  ownerNamesAvailable: 0,
  phoneAvailable: 0,
  phoneUnavailable: 0,
  projectedProperties: 0,
  schemaFailures: 0,
  situsMailingCollisions: 0,
};

const { provider } = await realMcpHarness();
const contracts = await McpContractRegistry.create();
const metadata = await provider.getMetadata();
const rows = await provider.getQueryRows();
let index = 0;

await Promise.all(
  Array.from({ length: 16 }, async () => {
    while (true) {
      const current = index++;
      const row = rows[current];
      if (!row) return;
      const canonical = await provider.getCanonicalProperty(row.propertyId);
      if (!canonical) throw new Error("Canonical projection input is missing");
      const property = projectPublicProperty(canonical, row.propertyId);
      const result = {
        ok: true,
        data: property,
        meta: {
          contractVersion: MCP_CONTRACT_VERSION,
          schemaHash: MCP_SCHEMA_SHA256,
          county: "pasco",
          asOf: metadata.asOf,
          artifactCids: metadata.artifactCids,
          nextCursor: null,
        },
      };
      if (
        contracts.validateOutput("prism_v1_get_property", result).length > 0
      ) {
        counts.schemaFailures += 1;
      }
      const ownership = record(property.ownership);
      const currentOwners = record(ownership.currentOwners);
      const classification = record(ownership.classification);
      const mailing = record(ownership.publicMailingAddress);
      const phone = record(ownership.phone);
      const email = record(ownership.email);
      countFact(
        currentOwners,
        "currentOwnersAvailable",
        "currentOwnersUnavailable",
        counts,
      );
      countFact(
        classification,
        "classificationAvailable",
        "classificationUnavailable",
        counts,
      );
      countFact(
        mailing,
        "mailingAddressAvailable",
        "mailingAddressUnavailable",
        counts,
      );
      countFact(phone, "phoneAvailable", "phoneUnavailable", counts);
      countFact(email, "emailAvailable", "emailUnavailable", counts);
      if (currentOwners.availability === "available") {
        counts.ownerNamesAvailable += (currentOwners.value as unknown[]).length;
      }
      if (mailing.availability === "available") {
        const components = record(mailing.value);
        for (const key of [
          "addressLines",
          "locality",
          "region",
          "postalCode",
          "country",
        ]) {
          counts[
            record(components[key]).availability === "available"
              ? "mailingComponentsAvailable"
              : "mailingComponentsUnavailable"
          ] += 1;
        }
        const situs = record(property.address);
        const situsText = normalizeAddress(factValue(situs));
        const mailingValue = normalizeAddress(mailingText(mailing));
        if (situsText.length > 0 && situsText === mailingValue) {
          counts.situsMailingCollisions += 1;
        }
      }
      counts.evidenceFailures += validateReferences(property);
      counts.projectedProperties += 1;
    }
  }),
);

if (
  rows.length !== 25_000 ||
  counts.projectedProperties !== 25_000 ||
  counts.schemaFailures !== 0 ||
  counts.evidenceFailures !== 0 ||
  counts.situsMailingCollisions !== 0 ||
  counts.phoneAvailable !== 0 ||
  counts.emailAvailable !== 0
) {
  throw new Error(
    `MCP v1.2 aggregate projection validation failed: ${JSON.stringify(counts)}`,
  );
}

console.log(
  JSON.stringify(
    {
      contractVersion: MCP_CONTRACT_VERSION,
      schemaHash: MCP_SCHEMA_SHA256,
      propertyRows: rows.length,
      counts,
    },
    null,
    2,
  ),
);
