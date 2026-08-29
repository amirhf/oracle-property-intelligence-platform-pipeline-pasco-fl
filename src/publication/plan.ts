import { z } from "zod";

import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import { deterministicId } from "../lib/hash.js";
import { CIDV0_PATTERN, IPFS_CID_PROFILE } from "./ipfs-cid.js";

export const PUBLICATION_PLAN_VERSION = "1.1.0";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.string().regex(CIDV0_PATTERN);
const runIdSchema = z.string().regex(/^run_[a-f0-9]{32}$/);
const snapshotIdSchema = z.string().regex(/^snapshot_[a-f0-9]{32}$/);
const scopeIdSchema = z.string().regex(/^scope_[a-f0-9]{32}$/);
const planIdSchema = z.string().regex(/^plan_[a-f0-9]{32}$/);
const isoDateTimeSchema = z
  .string()
  .refine(
    (value) => Number.isFinite(Date.parse(value)),
    "must be an ISO date-time",
  );
const objectKeySchema = z
  .string()
  .min(1)
  .max(2_048)
  .refine(
    (value) =>
      !value.startsWith("/") &&
      !value.split(/[\\/]/).includes("..") &&
      !value.includes("\\"),
    "must be a safe publication-relative object key",
  );

export const publicationArtifactSchema = z.strictObject({
  byteSize: z.number().int().nonnegative(),
  domain: z.enum(["open_data", "query_table"]),
  expectedCid: cidSchema,
  objectKey: objectKeySchema,
  role: z.enum([
    "property",
    "shard",
    "root",
    "manifest",
    "metadata",
    "query_table",
  ]),
  sha256: sha256Schema,
});

const fileBindingSchema = z.strictObject({
  byteSize: z.number().int().nonnegative(),
  expectedCid: cidSchema,
  objectKey: objectKeySchema,
  sha256: sha256Schema,
});

const publicationTargetSchema = z.strictObject({
  bucket: z.string().min(1).max(200).nullable(),
  bucketConfirmed: z.boolean(),
  ipnsLabel: z.string().min(1).max(200),
  ipnsNetworkKey: z.string().min(1).max(500).nullable(),
});

const graphEdgeSchema = z.strictObject({
  childCid: cidSchema,
  childKey: objectKeySchema,
  jsonPointer: z.string().regex(/^\/(?:[^/~]|~[01])+(?:\/(?:[^/~]|~[01])+)*$/),
  parentKey: objectKeySchema,
});

const cidProfileSchema = z.strictObject({
  cidVersion: z.literal(0),
  chunker: z.literal("fixed"),
  chunkSize: z.literal(262_144),
  codec: z.literal("dag-pb"),
  hashAlg: z.literal("sha2-256"),
  importer: z.literal("ipfs-unixfs-importer@7.0.3"),
  layout: z.literal("balanced"),
  maxChildrenPerNode: z.literal(174),
  onlyHash: z.literal(true),
  rawLeaves: z.literal(false),
  reduceSingleLeafToSelf: z.literal(true),
  trickle: z.literal(false),
  unixfsType: z.literal("file"),
  version: z.literal("ipfs-only-hash@4.0.0"),
  wrapWithDirectory: z.literal(false),
});

const parquetProfileSchema = z.strictObject({
  compression: z.literal("ZSTD"),
  duckdbVersion: z.string().min(1).max(100),
  rowGroupSize: z.literal(10_000),
  schemaSha256: sha256Schema,
});

export const publicationPlanSchema = z
  .strictObject({
    approvable: z.boolean(),
    artifacts: z.strictObject({
      coverage: fileBindingSchema,
      manifest: fileBindingSchema,
      objectInventory: z.array(publicationArtifactSchema).min(1),
      parquet: fileBindingSchema.extend({
        distinctPropertyIds: z.number().int().nonnegative(),
        nullPropertyIds: z.number().int().nonnegative(),
        rowCount: z.number().int().nonnegative(),
        schemaSha256: sha256Schema,
      }),
      provenance: fileBindingSchema,
      shards: z.array(
        fileBindingSchema.extend({
          propertyCount: z.number().int().positive(),
        }),
      ),
    }),
    configuration: z.strictObject({
      credentialsAvailable: z.boolean(),
      missing: z.array(
        z.enum([
          "filebase_credentials",
          "open_data_bucket",
          "open_data_ipns_network_key",
          "query_table_bucket",
          "query_table_ipns_network_key",
        ]),
      ),
    }),
    contracts: z.strictObject({
      canonical: z.strictObject({
        sha256: sha256Schema,
        version: z.literal("1.0.0"),
      }),
      mcp: z.strictObject({
        sha256: sha256Schema,
        version: z.literal("1.2.0"),
      }),
    }),
    counts: z.strictObject({
      activeProperties: z.number().int().nonnegative(),
      canonicalDocuments: z.number().int().nonnegative(),
      coordinateRows: z.number().int().nonnegative(),
      inactiveProperties: z.number().int().nonnegative(),
      queryTableDistinctPropertyIds: z.number().int().nonnegative(),
      queryTableNullPropertyIds: z.number().int().nonnegative(),
      queryTableRows: z.number().int().nonnegative(),
    }),
    county: z.literal("pasco"),
    coverage: z.strictObject({
      authoritativeHeadSnapshotId: snapshotIdSchema.nullable(),
      authoritySourceSystem: z.literal("pasco_appraiser"),
      completenessResult: z.enum(["not_applicable", "passed", "failed"]),
      entityType: z.literal("property_existence"),
      mode: z.enum(["sample", "partial", "authoritative_complete"]),
      predecessorChainSnapshotIds: z.array(snapshotIdSchema),
      runId: runIdSchema,
      scopeId: scopeIdSchema,
      selection: z.strictObject({
        algorithm: z.string().min(1).max(200),
        seed: z.string().min(1).max(500),
        selectedRecordSha256: sha256Schema,
        selectionSize: z.number().int().positive(),
      }),
      sourceSnapshotId: snapshotIdSchema.nullable(),
      sourceSnapshotManifestSha256: sha256Schema.nullable(),
      workflowId: z.string().min(1).max(200),
    }),
    executable: z.boolean(),
    exportMode: z.enum(["bounded", "authoritative"]),
    fixtureExclusion: z.strictObject({
      fixturePropertyIdCount: z.number().int().nonnegative(),
      matches: z.literal(0),
      passed: z.literal(true),
    }),
    freshness: z.strictObject({
      asOf: isoDateTimeSchema,
      loadedAt: isoDateTimeSchema,
      observedAt: isoDateTimeSchema,
    }),
    generatedAt: isoDateTimeSchema,
    graph: z.strictObject({
      cidProfile: cidProfileSchema,
      edges: z.array(graphEdgeSchema),
      openDataRoot: z.strictObject({
        expectedCid: cidSchema,
        objectKey: z.literal("index.json"),
      }),
      parquetProfile: parquetProfileSchema,
      propertyCidCount: z.number().int().nonnegative(),
      queryTableRoot: z.strictObject({
        expectedCid: cidSchema,
        objectKey: z.literal("query-tables/pasco/query-table.parquet"),
      }),
      traversalValidated: z.literal(true),
    }),
    limitations: z.array(z.string().min(1).max(1_000)),
    planId: planIdSchema,
    planSha256: sha256Schema,
    projection: z.strictObject({
      authoritativeBaseSnapshotId: snapshotIdSchema.nullable(),
      materializationId: z.string().regex(/^materialization_[a-f0-9]{32}$/),
      materializationSha256: sha256Schema,
      snapshotContentSha256: sha256Schema.nullable(),
    }),
    remoteState: z.strictObject({
      openDataPublishedCid: z.null(),
      openDataIpnsMutationPerformed: z.literal(false),
      queryTablePublishedCid: z.null(),
      queryTableIpnsMutationPerformed: z.literal(false),
    }),
    targets: z.strictObject({
      openData: publicationTargetSchema,
      queryTable: publicationTargetSchema,
    }),
    temporalFactLimitation: z.string().min(1).max(1_000),
    version: z.literal(PUBLICATION_PLAN_VERSION),
  })
  .superRefine((plan, context) => {
    const inventory = new Map<
      string,
      z.infer<typeof publicationArtifactSchema>
    >();
    for (const artifact of plan.artifacts.objectInventory) {
      const key = `${artifact.domain}:${artifact.objectKey}`;
      if (inventory.has(key))
        context.addIssue({
          code: "custom",
          message: "publication object inventory contains a duplicate key",
          path: ["artifacts", "objectInventory"],
        });
      inventory.set(key, artifact);
    }
    const graphObjects = new Map(
      [...inventory.values()]
        .filter((artifact) => artifact.domain === "open_data")
        .map((artifact) => [artifact.objectKey, artifact]),
    );
    for (const edge of plan.graph.edges) {
      const child = graphObjects.get(edge.childKey);
      const parent = graphObjects.get(edge.parentKey);
      if (!parent || !child || child.expectedCid !== edge.childCid) {
        context.addIssue({
          code: "custom",
          message: "publication graph edge is not bound to inventory",
          path: ["graph", "edges"],
        });
        break;
      }
    }
    const root = inventory.get("open_data:index.json");
    const parquet = inventory.get(
      "query_table:query-tables/pasco/query-table.parquet",
    );
    const graphComplete =
      root?.expectedCid === plan.graph.openDataRoot.expectedCid &&
      root.role === "root" &&
      parquet?.expectedCid === plan.graph.queryTableRoot.expectedCid &&
      parquet.role === "query_table" &&
      plan.graph.propertyCidCount === plan.counts.canonicalDocuments &&
      plan.graph.edges.length ===
        plan.counts.canonicalDocuments + plan.artifacts.shards.length;
    if (!graphComplete)
      context.addIssue({
        code: "custom",
        message: "publication graph/root binding is incomplete",
        path: ["graph"],
      });

    const missingConfiguration =
      !plan.configuration.credentialsAvailable ||
      plan.configuration.missing.length > 0 ||
      !plan.targets.openData.bucketConfirmed ||
      !plan.targets.queryTable.bucketConfirmed ||
      plan.targets.openData.bucket === null ||
      plan.targets.queryTable.bucket === null ||
      plan.targets.openData.ipnsNetworkKey === null ||
      plan.targets.queryTable.ipnsNetworkKey === null;
    const coverageEligible =
      plan.coverage.mode === "authoritative_complete" ||
      (plan.coverage.mode === "partial" &&
        plan.projection.authoritativeBaseSnapshotId !== null);
    const shouldExecute =
      !missingConfiguration && graphComplete && coverageEligible;
    if (
      plan.approvable !== shouldExecute ||
      plan.executable !== shouldExecute
    ) {
      context.addIssue({
        code: "custom",
        message:
          "publication approval flags do not match graph, target, and coverage eligibility",
        path: ["approvable"],
      });
    }
    if (plan.exportMode === "authoritative") {
      const currentProjection =
        plan.coverage.sourceSnapshotId !== null &&
        plan.coverage.sourceSnapshotManifestSha256 !== null &&
        plan.coverage.authoritativeHeadSnapshotId ===
          plan.coverage.sourceSnapshotId;
      const eligibleCoverage =
        (plan.coverage.mode === "authoritative_complete" &&
          plan.coverage.completenessResult === "passed" &&
          plan.projection.authoritativeBaseSnapshotId ===
            plan.coverage.sourceSnapshotId) ||
        (plan.coverage.mode === "partial" &&
          plan.projection.authoritativeBaseSnapshotId !== null);
      if (
        !currentProjection ||
        !eligibleCoverage ||
        plan.projection.snapshotContentSha256 === null
      ) {
        context.addIssue({
          code: "custom",
          message:
            "current projection publication lacks a verified authoritative base and exact head",
          path: ["coverage"],
        });
      }
    } else if (plan.coverage.mode === "authoritative_complete") {
      context.addIssue({
        code: "custom",
        message: "bounded publication cannot claim authoritative coverage",
        path: ["coverage", "mode"],
      });
    }
  });

export type PublicationArtifact = z.infer<typeof publicationArtifactSchema>;
export type PublicationPlan = z.infer<typeof publicationPlanSchema>;
export type PublicationTarget = z.infer<typeof publicationTargetSchema>;
export type PublicationPlanInput = Omit<
  PublicationPlan,
  "generatedAt" | "planId" | "planSha256"
> & { generatedAt: string };

function planIdentity(plan: Omit<PublicationPlan, "planId" | "planSha256">) {
  const { generatedAt: _generatedAt, ...identity } = plan;
  return identity;
}

export function expectedPublicationPlanSha256(
  plan: Omit<PublicationPlan, "planId" | "planSha256">,
): string {
  return canonicalJsonSha256(planIdentity(plan));
}

export function createPublicationPlan(
  input: PublicationPlanInput,
): PublicationPlan {
  const planSha256 = expectedPublicationPlanSha256(input);
  return validatePublicationPlan({
    ...input,
    planId: deterministicId("plan", [
      PUBLICATION_PLAN_VERSION,
      "Publish/pasco",
      planSha256,
    ]),
    planSha256,
  });
}

export function validatePublicationPlan(value: unknown): PublicationPlan {
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    (value as Record<string, unknown>).version === "1.0.0"
  ) {
    const record = value as Record<string, unknown>;
    const expectedKeys = new Set([
      "approvable",
      "artifacts",
      "configuration",
      "contracts",
      "counts",
      "county",
      "coverage",
      "executable",
      "exportMode",
      "fixtureExclusion",
      "freshness",
      "generatedAt",
      "limitations",
      "planId",
      "planSha256",
      "remoteState",
      "targets",
      "temporalFactLimitation",
      "version",
    ]);
    if (
      Object.keys(record).some((key) => !expectedKeys.has(key)) ||
      typeof record.planId !== "string" ||
      typeof record.planSha256 !== "string" ||
      typeof record.generatedAt !== "string"
    ) {
      throw new Error("Historical publication plan failed strict validation");
    }
    const {
      generatedAt: _generatedAt,
      planId: _planId,
      planSha256: _planSha256,
      ...identity
    } = record;
    const expectedSha256 = canonicalJsonSha256(identity);
    const expectedPlanId = deterministicId("plan", [
      "1.0.0",
      "Publish/pasco",
      expectedSha256,
    ]);
    if (
      record.planSha256 !== expectedSha256 ||
      record.planId !== expectedPlanId
    ) {
      throw new Error("Historical publication plan identity is invalid");
    }
    // v1.0 is read-only historical compatibility for the existing local MCP
    // artifact. New plans can only be created as v1.1.0.
    return record as unknown as PublicationPlan;
  }
  const parsed = publicationPlanSchema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new Error(
      `Publication plan failed strict validation at ${issue?.path.join(".") || "root"}: ${issue?.message ?? "invalid"}`,
    );
  }
  const plan = parsed.data;
  const { planId: _planId, planSha256: _planSha256, ...withoutIdentity } = plan;
  const expectedSha256 = expectedPublicationPlanSha256(withoutIdentity);
  const expectedPlanId = deterministicId("plan", [
    PUBLICATION_PLAN_VERSION,
    "Publish/pasco",
    expectedSha256,
  ]);
  if (plan.planSha256 !== expectedSha256 || plan.planId !== expectedPlanId)
    throw new Error("Publication plan deterministic identity is invalid");
  return plan;
}

export function publicationConfigurationMissing(options: {
  credentialsAvailable: boolean;
  openData: PublicationTarget;
  queryTable: PublicationTarget;
}): PublicationPlan["configuration"]["missing"] {
  const missing: PublicationPlan["configuration"]["missing"] = [];
  if (!options.credentialsAvailable) missing.push("filebase_credentials");
  if (!options.openData.bucket || !options.openData.bucketConfirmed)
    missing.push("open_data_bucket");
  if (!options.openData.ipnsNetworkKey)
    missing.push("open_data_ipns_network_key");
  if (!options.queryTable.bucket || !options.queryTable.bucketConfirmed)
    missing.push("query_table_bucket");
  if (!options.queryTable.ipnsNetworkKey)
    missing.push("query_table_ipns_network_key");
  return missing;
}

export function localIncompletePascoTargets() {
  return {
    credentialsAvailable: false as const,
    openData: {
      bucket: "elephant-oracle-open-data-pasco",
      bucketConfirmed: true,
      ipnsLabel: "oracle-open-data-pasco",
      ipnsNetworkKey: null,
    },
    queryTable: {
      bucket: null,
      bucketConfirmed: false,
      ipnsLabel: "oracle-query-table-pasco",
      ipnsNetworkKey: null,
    },
  };
}

export const pinnedPublicationCidProfile = () => ({ ...IPFS_CID_PROFILE });
