import "dotenv/config";

import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import postgres from "postgres";

import { sourceRecordHash } from "../src/lib/hash.js";
import { AUTHORITATIVE_PUBLICATION_BINDING } from "../src/publication/authoritative-local.js";
import { meaningfulSitusAddress } from "../src/publication/canonical-property.js";
import { calculateIpfsCid } from "../src/publication/ipfs-cid.js";
import {
  validatePublicationPlan,
  type PublicationPlan,
} from "../src/publication/plan.js";

const SAMPLE_SIZE = 128;
const dataDir = process.env.DATA_DIR?.trim();
const databaseUrl = process.env.DATABASE_URL?.trim();
if (!dataDir || !databaseUrl) {
  throw new Error("DATA_DIR and DATABASE_URL are required");
}

const plansRoot = path.resolve(
  dataDir,
  "artifacts",
  "publish",
  "pasco",
  "authoritative-local",
  "plans",
);
let selected: { plan: PublicationPlan; planDirectory: string } | undefined;
for (const directory of (await readdir(plansRoot)).sort()) {
  const planDirectory = path.join(plansRoot, directory);
  const plan = validatePublicationPlan(
    JSON.parse(
      await readFile(path.join(planDirectory, "publication-plan.json"), "utf8"),
    ),
  );
  if (
    plan.coverage.runId === AUTHORITATIVE_PUBLICATION_BINDING.runId &&
    plan.projection.materializationId ===
      AUTHORITATIVE_PUBLICATION_BINDING.materializationId
  ) {
    if (selected) throw new Error("Multiple bound authoritative plans found");
    selected = { plan, planDirectory };
  }
}
if (!selected) throw new Error("Bound authoritative publication is missing");

const sql = postgres(databaseUrl, { max: 1 });
try {
  const cores = await sql<
    Array<{
      parcel_identifier: string;
      payload: Record<string, unknown>;
      property_id: string;
      source_snapshot_id: string;
      version_id: string;
    }>
  >`
    SELECT version.property_id, version.parcel_identifier, version.payload,
           version.source_snapshot_id, version.version_id
    FROM oracle_projection_materialized_properties membership
    JOIN oracle_property_versions version
      ON version.version_id = membership.property_version_id
    WHERE membership.materialization_id = ${AUTHORITATIVE_PUBLICATION_BINDING.materializationId}
      AND membership.is_active
    ORDER BY md5(version.property_id || ${selected.plan.coverage.selection.selectedRecordSha256}),
             version.property_id
    LIMIT ${SAMPLE_SIZE}
  `;
  if (cores.length !== SAMPLE_SIZE) {
    throw new Error("Deterministic reconciliation sample is incomplete");
  }
  const propertyIds = cores.map((core) => core.property_id);
  const facts = await sql<
    Array<{
      fact_type: string;
      payload: Record<string, unknown>;
      property_id: string;
      source_record_sha256: string;
      source_snapshot_id: string;
      version_id: string;
    }>
  >`
    SELECT membership.property_id, version.fact_type, version.payload,
           version.source_record_sha256, version.source_snapshot_id,
           version.version_id
    FROM oracle_projection_materialized_facts membership
    JOIN oracle_child_fact_versions version
      ON version.version_id = membership.fact_version_id
    WHERE membership.materialization_id = ${AUTHORITATIVE_PUBLICATION_BINDING.materializationId}
      AND membership.property_id = ANY(${propertyIds})
    ORDER BY membership.property_id, membership.fact_type,
             membership.natural_key, version.version_id
  `;
  const factsByProperty = new Map<string, Array<(typeof facts)[number]>>();
  for (const fact of facts) {
    const current = factsByProperty.get(fact.property_id) ?? [];
    current.push(fact);
    factsByProperty.set(fact.property_id, current);
  }
  const inventory = new Map(
    selected.plan.artifacts.objectInventory.map((artifact) => [
      `${artifact.domain}:${artifact.objectKey}`,
      artifact,
    ]),
  );
  let checkedFacts = 0;
  let unavailableAddresses = 0;
  let missingCoordinates = 0;
  for (const core of cores) {
    if (
      core.source_snapshot_id !== AUTHORITATIVE_PUBLICATION_BINDING.snapshotId
    ) {
      throw new Error("Sample core escaped the sealed snapshot");
    }
    const objectKey = `properties/${core.property_id}.json`;
    const artifact = inventory.get(`open_data:${objectKey}`);
    if (!artifact) throw new Error("Sample property is absent from the plan");
    const bytes = await readFile(
      path.join(selected.planDirectory, "open-data", objectKey),
    );
    if (
      bytes.byteLength !== artifact.byteSize ||
      createHash("sha256").update(bytes).digest("hex") !== artifact.sha256 ||
      (await calculateIpfsCid(bytes)) !== artifact.expectedCid
    ) {
      throw new Error("Sample property byte/CID binding failed");
    }
    const property = JSON.parse(bytes.toString("utf8")) as Record<
      string,
      unknown
    >;
    if (
      property.propertyId !== core.property_id ||
      property.permits === undefined
    ) {
      throw new Error("Sample canonical property identity is invalid");
    }
    if (!Array.isArray(property.permits) || property.permits.length !== 0) {
      throw new Error("Sample property fabricated permit coverage");
    }
    const evidence = property.evidence as Array<Record<string, unknown>>;
    const byKey = new Map(
      evidence.map((entry) => [String(entry.sourceRecordKey), entry]),
    );
    const parcel = core.payload.parcel;
    const parcelEvidence = byKey.get(`${core.version_id}:parcel`);
    if (
      !parcelEvidence ||
      parcelEvidence.sourceRecordHash !== sourceRecordHash(parcel) ||
      parcelEvidence.observedAt !== "2026-08-23T11:07:02.000Z"
    ) {
      throw new Error("Sample parcel provenance binding failed");
    }
    const sourceAddress = core.payload.siteAddress as {
      city?: string | null;
      siteAddress?: string | null;
      zipCode?: string | null;
    } | null;
    const expectedAddress = meaningfulSitusAddress({
      city: sourceAddress?.city ?? null,
      siteAddress: sourceAddress?.siteAddress ?? null,
      zipCode: sourceAddress?.zipCode ?? null,
    });
    const addressFact = property.situsAddress as Record<string, unknown>;
    if (
      (expectedAddress === null &&
        (addressFact.availability !== "unavailable" ||
          addressFact.value !== null)) ||
      (expectedAddress !== null && addressFact.value !== expectedAddress)
    ) {
      throw new Error("Sample address availability is inconsistent");
    }
    unavailableAddresses += expectedAddress === null ? 1 : 0;
    const propertyFacts = factsByProperty.get(core.property_id) ?? [];
    for (const fact of propertyFacts) {
      if (
        fact.source_snapshot_id !== AUTHORITATIVE_PUBLICATION_BINDING.snapshotId
      ) {
        throw new Error("Sample child fact escaped the sealed snapshot");
      }
      const factEvidence = byKey.get(fact.version_id);
      if (fact.fact_type !== "availability") {
        if (
          !factEvidence ||
          factEvidence.sourceRecordHash !== fact.source_record_sha256
        ) {
          throw new Error("Sample child-fact provenance binding failed");
        }
        const expectedObservedAt =
          fact.fact_type === "coordinate" &&
          typeof fact.payload.sourceLastUpdate === "string"
            ? new Date(fact.payload.sourceLastUpdate).toISOString()
            : null;
        if (factEvidence.observedAt !== expectedObservedAt) {
          throw new Error("Sample child-fact timestamp was fabricated");
        }
      }
      checkedFacts += 1;
    }
    missingCoordinates += propertyFacts.some(
      (fact) => fact.fact_type === "coordinate",
    )
      ? 0
      : 1;
  }
  const sampleSelectionSha256 = createHash("sha256")
    .update(`${propertyIds.join("\n")}\n`)
    .digest("hex");
  console.log(
    JSON.stringify(
      {
        checkedFacts,
        missingCoordinates,
        planId: selected.plan.planId,
        properties: cores.length,
        sampleSelectionSha256,
        unavailableAddresses,
      },
      null,
      2,
    ),
  );
} finally {
  await sql.end();
}
