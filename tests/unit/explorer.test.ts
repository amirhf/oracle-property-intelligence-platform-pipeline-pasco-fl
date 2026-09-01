import { once } from "node:events";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { McpContractRegistry } from "../../src/mcp/contracts.js";
import { MCP_TOOL_NAMES } from "../../src/mcp/constants.js";
import { PublicIpnsProvider } from "../../src/mcp/public-ipns-provider.js";
import { OracleMcpRuntime } from "../../src/mcp/runtime.js";
import {
  explorerBootstrap,
  explorerProperty,
  explorerSearch,
} from "../../src/mcp/explorer.js";
import {
  createOracleMcpHttpServer,
  listenOracleMcpServer,
} from "../../src/mcp/server.js";
import { coordinatesSearch } from "../helpers/mcp-real.js";
import {
  SYNTHETIC_EMAIL_SENTINEL,
  SYNTHETIC_MAILING_SENTINEL,
  SYNTHETIC_OWNER_SENTINEL,
  SYNTHETIC_PHONE_SENTINEL,
  syntheticPublicSet,
} from "../helpers/public-ipns.js";

const SENSITIVE_SENTINELS = [
  SYNTHETIC_OWNER_SENTINEL,
  SYNTHETIC_MAILING_SENTINEL,
  SYNTHETIC_PHONE_SENTINEL,
  SYNTHETIC_EMAIL_SENTINEL,
  "SYNTH-1",
  "CONTRACTOR-SENTINEL",
  "PERMIT-SENTINEL",
];

const FORBIDDEN_EXPLORER_KEYS = new Set([
  "apn",
  "contractor",
  "contractorbusinessname",
  "contractorcompany",
  "contractorid",
  "contractoridentity",
  "contractorlicense",
  "contractorname",
  "exactfolio",
  "folio",
  "folioid",
  "folionumber",
  "mailingaddress",
  "mailingaddress1",
  "mailingaddress2",
  "mailingcity",
  "mailingpostalcode",
  "mailingstate",
  "mailingzipcode",
  "ownerdisplayname",
  "owneremail",
  "ownername",
  "ownername1",
  "ownername2",
  "ownerphone",
  "parcel",
  "parcelid",
  "parcelidentifier",
  "parcelnumber",
  "parcelno",
  "permitid",
  "permitidentifier",
  "permitnumber",
  "permitno",
  "phonenumber",
  "propertyfolio",
  "requestidentifier",
  "sourcerecordkey",
  "taxparcelid",
  "taxparcelnumber",
]);

function assertPrivacySafeExplorerJson(value: unknown): void {
  const forbiddenPaths: string[] = [];
  const visit = (candidate: unknown, path: string): void => {
    if (Array.isArray(candidate)) {
      candidate.forEach((entry, index) => visit(entry, `${path}[${index}]`));
      return;
    }
    if (candidate === null || typeof candidate !== "object") return;
    for (const [key, entry] of Object.entries(
      candidate as Record<string, unknown>,
    )) {
      const normalizedKey = key.toLowerCase().replaceAll(/[^a-z0-9]/g, "");
      if (FORBIDDEN_EXPLORER_KEYS.has(normalizedKey)) {
        forbiddenPaths.push(`${path}.${key}`);
      }
      visit(entry, `${path}.${key}`);
    }
  };
  visit(value, "$response");
  expect(forbiddenPaths).toEqual([]);
  const serialized = JSON.stringify(value);
  for (const sentinel of SENSITIVE_SENTINELS) {
    expect(serialized).not.toContain(sentinel);
  }
}

describe("privacy-safe public Oracle explorer", () => {
  let baseUrl: string;
  let client: Client;
  let publicationCoordinateCount: number;
  let propertyId: string;
  let publicationPropertyCount: number;
  let server: ReturnType<typeof createOracleMcpHttpServer>;

  beforeAll(async () => {
    const contracts = await McpContractRegistry.create();
    const set = await syntheticPublicSet({ candidatePlanBindings: true });
    propertyId = set.propertyIds[0]!;
    publicationPropertyCount = set.propertyIds.length;
    const provider = await PublicIpnsProvider.create(
      set.config,
      contracts,
      set.transport,
    );
    publicationCoordinateCount = (await provider.getMetadata()).coordinateCount;
    const limits = {
      maxRequestBytes: 65_536,
      maxResponseBytes: 2 * 1024 * 1024,
      requestTimeoutMs: 10_000,
    };
    const runtime = new OracleMcpRuntime(provider, contracts, limits);
    server = createOracleMcpHttpServer({
      contracts,
      limits,
      providerMode: "public-ipns",
      runtime,
    });
    const port = await listenOracleMcpServer(server, 0);
    baseUrl = `http://127.0.0.1:${port}`;
    client = new Client({ name: "public-provider-client", version: "1.0.0" });
    await client.connect(
      new StreamableHTTPClientTransport(
        new URL(`${baseUrl}/mcp`),
      ) as unknown as Transport,
    );
  }, 30_000);

  afterAll(async () => {
    await client?.close();
    if (server?.listening) {
      server.close();
      await once(server, "close");
    }
  });

  it("renders a read-only explorer with explicit sample limitations", async () => {
    const page = await fetch(`${baseUrl}/`);
    expect(page.status).toBe(200);
    const html = await page.text();
    expect(html).toContain("Pasco Oracle explorer");
    expect(html).toContain("not complete Pasco County coverage");
    expect(html).not.toContain("publication approval");
    for (const sentinel of SENSITIVE_SENTINELS) {
      expect(html).not.toContain(sentinel);
    }

    const bootstrap = await fetch(
      `${baseUrl}/explorer/api/bootstrap?vercelRewrite=api-index`,
    );
    expect(bootstrap.status).toBe(200);
    const value = (await bootstrap.json()) as Record<string, unknown>;
    expect(value.publication).toMatchObject({
      candidateDemo: {
        objectCount: expect.any(Number),
        propertyCount: publicationPropertyCount,
        providerCidVerification: {
          mismatchCount: 0,
          status: "all_matched",
        },
        remoteStatus: "candidate_filebase_ipns_active",
      },
      coverageMode: "sample",
      coordinateCount: publicationCoordinateCount,
      propertyCount: publicationPropertyCount,
    });
    const serialized = JSON.stringify(value);
    expect(serialized).toContain("not complete Pasco coverage");
    expect(serialized).toContain(
      "candidate-owned Filebase objects and candidate-owned IPNS identities",
    );
    expect(serialized).toContain("historical_local_source_plan");
    expect(serialized).not.toContain(
      "No Filebase, IPFS, or IPNS effect was performed.",
    );
    expect(serialized).not.toContain(process.cwd());
  });

  it("executes validated searches while suppressing owner/contact values", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/search?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(
          coordinatesSearch({ latitude: 28.3, longitude: -82.4, limit: 1 }),
        ),
      },
    );
    expect(response.status).toBe(200);
    const value = (await response.json()) as Record<string, unknown>;
    assertPrivacySafeExplorerJson(value);
    const opportunity = (
      (value.data as Record<string, unknown>).opportunities as Array<
        Record<string, unknown>
      >
    )[0]!;
    const property = opportunity.property as Record<string, unknown>;
    expect(property.ownership).toMatchObject({
      currentOwners: { availability: "available", ownerCount: 1 },
      phone: { availability: "unavailable" },
      email: { availability: "unavailable" },
    });
    expect(property.openRoofingPermitCount).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(property.roofAgeSignal).toMatchObject({
      value: { basis: "year_built_proxy", basisQuality: "proxy" },
    });
  });

  it("returns privacy-safe direct property facts and exposes no write/query surface", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/property?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ propertyId }),
      },
    );
    const serialized = await response.text();
    expect(response.status).toBe(200);
    assertPrivacySafeExplorerJson(JSON.parse(serialized));
    for (const request of [
      fetch(`${baseUrl}/explorer/api/sql`, { method: "POST", body: "{}" }),
      fetch(`${baseUrl}/publish`, { method: "POST", body: "{}" }),
      fetch(`${baseUrl}/explorer/api/property`, { method: "PUT", body: "{}" }),
      fetch(`${baseUrl}/explorer/api/bootstrap/extra?source=explorer`),
    ]) {
      await expect(request).resolves.toMatchObject({ status: 404 });
    }
  });

  it.each([
    ["search", explorerSearch],
    ["property", explorerProperty],
  ])(
    "removes nested forbidden aliases and sentinels from %s JSON",
    async (kind, project) => {
      const hostileProperty = {
        APN: "SYNTH-1",
        contractor: {
          name: "CONTRACTOR-SENTINEL",
        },
        evidence: [
          {
            evidenceId: `evidence_${"a".repeat(32)}`,
            sourceRecordKey: "SYNTH-1",
          },
        ],
        folioNumber: "SYNTH-1",
        mailingAddress1: SYNTHETIC_MAILING_SENTINEL,
        ownerName1: SYNTHETIC_OWNER_SENTINEL,
        ownership: {
          classification: { availability: "unavailable" },
          currentOwners: {
            availability: "available",
            value: [{ ownerName: SYNTHETIC_OWNER_SENTINEL }],
          },
          email: { availability: "unavailable" },
          phone: { availability: "unavailable" },
          publicMailingAddress: {
            availability: "available",
            value: { line1: SYNTHETIC_MAILING_SENTINEL },
          },
        },
        parcelNumber: "SYNTH-1",
        taxParcelId: "SYNTH-1",
        permits: [
          {
            permitNumber: "PERMIT-SENTINEL",
            requestIdentifier: "PERMIT-SENTINEL",
          },
        ],
        propertyId: `prop_${"b".repeat(32)}`,
      };
      const result =
        kind === "search"
          ? { data: { opportunities: [{ property: hostileProperty }] } }
          : { data: hostileProperty };
      const mockRuntime = {
        execute: async () => ({ isError: false, result }),
      } as unknown as OracleMcpRuntime;
      const projected = await project(mockRuntime, {});
      assertPrivacySafeExplorerJson(projected);
    },
  );

  it("preserves request-size limits after pathname routing", async () => {
    const response = await fetch(
      `${baseUrl}/explorer/api/search?source=explorer`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ oversized: "x".repeat(70_000) }),
      },
    );
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({
      error: "Request body exceeds the limit",
    });
  });

  it("serves all six tools through the official Streamable HTTP client", async () => {
    const listed = await client.listTools();
    expect(listed.tools.map((tool) => tool.name)).toEqual(MCP_TOOL_NAMES);
    const search = await client.callTool({
      name: "prism_v1_search_roofing_opportunities",
      arguments: coordinatesSearch({
        latitude: 28.3,
        longitude: -82.4,
        limit: 1,
      }),
    });
    const calls = [
      await client.callTool({
        name: "prism_v1_get_service_info",
        arguments: {},
      }),
      await client.callTool({
        name: "prism_v1_get_pipeline_run_summary",
        arguments: {},
      }),
      search,
      await client.callTool({
        name: "prism_v1_get_property",
        arguments: { propertyId },
      }),
      await client.callTool({
        name: "prism_v1_get_permit",
        arguments: { permitId: `perm_${"f".repeat(32)}` },
      }),
      await client.callTool({
        name: "prism_v1_get_query_schema",
        arguments: {},
      }),
    ];
    expect(calls).toHaveLength(6);
    expect(calls.filter((call) => call.isError === true)).toHaveLength(1);
  });
});

describe("candidate source-snapshot explorer status", () => {
  it("separates active remote evidence from historical local-plan status", async () => {
    const execute = async () => ({
      isError: false,
      result: { data: {} },
    });
    const runtime = {
      execute,
      provider: {
        getMetadata: async () => ({
          artifactCids: [],
          asOf: "2026-08-23T11:07:02.000Z",
          canonicalDocumentCount: 325_213,
          completedAt: "2026-08-30T20:52:19.835Z",
          coordinateCount: 24_995,
          coverageMode: "source_snapshot",
          contractorCoverage: "unavailable",
          datasetVersion: "snapshotdemo_1",
          fixtureMatches: 0,
          limitations: [],
          manifestSha256: "1".repeat(64),
          objectCount: 325_312,
          parquetSha256: "2".repeat(64),
          plan: {},
          providerMode: "public-ipns",
          publication: {
            candidateDemoPlanId:
              "snapshotdemo_87e3253348cedf80ecba1d716791dd16",
            candidateDemoPlanSha256:
              "1f98bdf9fa8269fd64b26314fd93aa9bbbf7850390176612366a8989975583ee",
            manifestCid: `Qm${"1".repeat(44)}`,
            openDataIpns:
              "k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6",
            openDataRootCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
            planCid: "QmcxZWB8W2asaZDNNXi1WyprzQT8cMKmen7FW8fbGiivTW",
            planSha256:
              "1f98bdf9fa8269fd64b26314fd93aa9bbbf7850390176612366a8989975583ee",
            queryTableIpns:
              "k51qzi5uqu5dlj11ik6bpomd7581ipkp9h2sm6gpadwqx6zkjyl2h32osd7rgm",
            queryTableRootCid: "QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw",
            resolverPolicy: "candidate_filebase_delegated_v2",
            scopeId: "scope_1",
            selectionHash: "5".repeat(64),
            sourceSnapshotId: "snapshot_1",
          },
          permitCoverage: "unavailable",
          runId: "run_1",
          runSummary: {},
          startedAt: "2026-08-30T20:52:19.835Z",
          workflowId: "workflow_1",
        }),
      },
    } as unknown as OracleMcpRuntime;

    const value = await explorerBootstrap(runtime);
    expect(value.publication).toMatchObject({
      candidateDemo: {
        activeCandidatePublication: true,
        evidence: {
          carBulkVerificationCount: 2,
          closureId:
            "snapshotdemouploadclosure_95a8b6c2a3a8a6ff4825d3fb2286ab8b",
          sha256:
            "b56927ed06ddb3007ce1aa85f236f49727bbe6697ad0feefb22cf967da5a4ff4",
        },
        providerCidVerification: {
          matchedObjectCount: 325_312,
          mismatchCount: 0,
          status: "inventory_verified",
        },
        remoteResources: {
          filebase: { status: "roots_pinned_and_bulk_membership_verified" },
          ipns: { status: "active_and_runtime_signed_resolved" },
        },
        remoteStatus: "candidate_filebase_ipns_active",
      },
      coverageMode: "source_snapshot",
    });
    const serialized = JSON.stringify(value);
    expect(serialized).toContain("historical_local_source_plan");
    expect(serialized).toContain(
      "individual receipt linkage or exact immutable CAR membership",
    );
    expect(serialized).not.toContain("planned_not_uploaded");
    expect(serialized).not.toContain("planned_not_mutated");
    expect(serialized).not.toContain("awaiting_configuration_unpublished");
  });
});
