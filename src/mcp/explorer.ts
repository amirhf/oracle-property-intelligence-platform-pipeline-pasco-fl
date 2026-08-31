import type { McpToolName } from "./constants.js";
import type { JsonObject } from "./provider.js";
import type { OracleMcpRuntime, ToolExecutionResult } from "./runtime.js";

function record(value: unknown): JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : {};
}

function data(result: ToolExecutionResult): JsonObject {
  return record(result.result.data);
}

function ownershipSummary(value: unknown): JsonObject {
  const ownership = record(value);
  const currentOwners = record(ownership.currentOwners);
  const owners = Array.isArray(currentOwners.value)
    ? currentOwners.value.length
    : null;
  return {
    currentOwners: {
      availability: currentOwners.availability ?? "unavailable",
      ownerCount: owners,
      reason: currentOwners.reason ?? null,
    },
    classification: {
      availability: record(ownership.classification).availability,
      reason: record(ownership.classification).reason ?? null,
    },
    publicMailingAddress: {
      availability: record(ownership.publicMailingAddress).availability,
      reason: record(ownership.publicMailingAddress).reason ?? null,
    },
    phone: {
      availability: record(ownership.phone).availability,
      reason: record(ownership.phone).reason ?? null,
    },
    email: {
      availability: record(ownership.email).availability,
      reason: record(ownership.email).reason ?? null,
    },
    privacy: structuredClone(ownership.privacy ?? {}),
  };
}

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

const HISTORICAL_NO_REMOTE_EFFECT =
  /^No Filebase, IPFS, or IPNS effect was performed\.?$/i;

function normalizedKey(value: string): string {
  return value.toLowerCase().replaceAll(/[^a-z0-9]/g, "");
}

function removeForbiddenExplorerFields(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => removeForbiddenExplorerFields(entry));
  }
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as JsonObject)
      .filter(([key]) => !FORBIDDEN_EXPLORER_KEYS.has(normalizedKey(key)))
      .map(([key, entry]) => [key, removeForbiddenExplorerFields(entry)]),
  );
}

function privacySafeProperty(value: unknown): JsonObject {
  const property = structuredClone(record(value));
  property.ownership = ownershipSummary(property.ownership);
  return record(removeForbiddenExplorerFields(property));
}

function privacySafeResult(result: ToolExecutionResult): JsonObject {
  const response = structuredClone(result.result);
  const responseData = record(response.data);
  if (Array.isArray(responseData.opportunities)) {
    responseData.opportunities = responseData.opportunities.map((value) => {
      const opportunity = record(value);
      return {
        ...opportunity,
        property: privacySafeProperty(opportunity.property),
      };
    });
  } else if (typeof responseData.propertyId === "string") {
    response.data = privacySafeProperty(responseData);
  }
  return record(removeForbiddenExplorerFields(response));
}

function candidatePipelineSummary(
  result: ToolExecutionResult,
  hasCandidateDemo: boolean,
): JsonObject {
  const summary = structuredClone(data(result));
  if (!hasCandidateDemo) return summary;
  summary.publicationArtifacts = {
    ...record(summary.publicationArtifacts),
    activeCandidatePublication: false,
    description:
      "Historical local source-plan and dry-run evidence; this is not the status or object inventory of the active candidate publication.",
    evidenceScope: "historical_local_source_plan",
  };
  return summary;
}

async function execute(
  runtime: OracleMcpRuntime,
  tool: McpToolName,
  argumentsValue: JsonObject,
  signal?: AbortSignal,
): Promise<ToolExecutionResult> {
  return runtime.execute(tool, argumentsValue, signal);
}

export async function explorerBootstrap(
  runtime: OracleMcpRuntime,
  signal?: AbortSignal,
): Promise<JsonObject> {
  const [service, pipeline, capabilities, metadata] = await Promise.all([
    execute(runtime, "prism_v1_get_service_info", {}, signal),
    execute(runtime, "prism_v1_get_pipeline_run_summary", {}, signal),
    execute(runtime, "prism_v1_get_query_schema", {}, signal),
    runtime.provider.getMetadata(signal),
  ]);
  if (service.isError || pipeline.isError || capabilities.isError) {
    throw new Error("Explorer metadata is unavailable");
  }
  const hasCandidateDemo = metadata.publication.candidateDemoPlanId !== null;
  const isSourceSnapshotPlan =
    metadata.publication.candidateDemoPlanId?.startsWith("snapshotdemo_") ===
    true;
  const candidateObjectCount =
    metadata.objectCount +
    (metadata.publication.planCid === null || isSourceSnapshotPlan ? 0 : 1);
  const limitations = hasCandidateDemo
    ? metadata.limitations
        .filter((limitation) => !HISTORICAL_NO_REMOTE_EFFECT.test(limitation))
        .concat(
          isSourceSnapshotPlan
            ? [
                "This target-bound candidate plan is locally validated but unapproved; its public metadata records no Filebase upload or IPNS mutation.",
              ]
            : [
                "The active public demo uses candidate-owned Filebase objects and candidate-owned IPNS identities.",
                "No Filebase or IPNS mutation occurred during the recent routing and explorer-display deployments; the earlier candidate publication effects are reported separately.",
              ],
        )
    : metadata.limitations;
  return {
    service: data(service),
    pipeline: candidatePipelineSummary(pipeline, hasCandidateDemo),
    capabilities: data(capabilities),
    publication: {
      candidateDemo: !hasCandidateDemo
        ? null
        : {
            coordinateCount: metadata.coordinateCount,
            objectCount: candidateObjectCount,
            planId: metadata.publication.candidateDemoPlanId,
            planSha256: metadata.publication.candidateDemoPlanSha256,
            propertyCount: metadata.canonicalDocumentCount,
            providerCidVerification: {
              description: isSourceSnapshotPlan
                ? "Deterministic local CIDs are plan-bound. Provider-returned CID receipts do not yet exist for this unapproved plan."
                : `All ${candidateObjectCount} provider-returned CIDs matched deterministic local CIDs.`,
              matchedObjectCount: isSourceSnapshotPlan
                ? null
                : candidateObjectCount,
              mismatchCount: isSourceSnapshotPlan ? null : 0,
              status: isSourceSnapshotPlan ? "not_executed" : "all_matched",
            },
            publicationTimestamp: {
              availability: "unavailable",
              reason: "not_recorded_in_public_candidate_metadata",
              value: null,
            },
            remoteResources: {
              filebase: {
                objectCount: candidateObjectCount,
                ownership: "candidate_owned",
                status: isSourceSnapshotPlan
                  ? "planned_not_uploaded"
                  : "uploaded_and_cid_verified",
              },
              ipns: {
                identityCount: 2,
                ownership: "candidate_owned",
                status: isSourceSnapshotPlan
                  ? "planned_not_mutated"
                  : "updated_and_publicly_resolved",
              },
            },
            remoteStatus: isSourceSnapshotPlan
              ? "awaiting_configuration_unpublished"
              : "candidate_filebase_ipns_active",
            resolverPolicy: metadata.publication.resolverPolicy,
            disclosure: isSourceSnapshotPlan
              ? "Candidate-owned, noncanonical Filebase demonstration of the complete parcel membership represented by the exact hash-bound August 23, 2026 Pasco Property Appraiser source snapshot under owner-assumed snapshot authority. It is not represented as Elephant-owned, owner-controlled, independently Pasco-certified, or complete under other Pasco reporting definitions. GIS, coordinate, related-fact, permit, and contractor coverage is measured and reported separately."
              : "Temporary candidate-owned Filebase demonstration of protocol compatibility. The buckets and IPNS identities are candidate-controlled and are not represented as Elephant-owned, owner-approved, authoritative-complete, or the final canonical assessment publication.",
          },
      coverageMode: metadata.coverageMode,
      coordinateCount: metadata.coordinateCount,
      propertyCount: metadata.canonicalDocumentCount,
      scopeId: metadata.publication.scopeId,
      sourceSnapshotId: metadata.publication.sourceSnapshotId,
      selectionHash: metadata.publication.selectionHash,
      manifest: {
        cid: metadata.publication.manifestCid,
        sha256: metadata.manifestSha256,
      },
      graph: {
        openDataIpns: metadata.publication.openDataIpns,
        rootCid: metadata.publication.openDataRootCid,
      },
      queryTable: {
        ipns: metadata.publication.queryTableIpns,
        rootCid: metadata.publication.queryTableRootCid,
        sha256: metadata.parquetSha256,
      },
      freshness: {
        asOf: metadata.asOf,
        completedAt: metadata.completedAt,
      },
      limitations,
    },
  };
}

export async function explorerSearch(
  runtime: OracleMcpRuntime,
  argumentsValue: unknown,
  signal?: AbortSignal,
): Promise<JsonObject> {
  const result = await runtime.execute(
    "prism_v1_search_roofing_opportunities",
    argumentsValue,
    signal,
  );
  return privacySafeResult(result);
}

export async function explorerProperty(
  runtime: OracleMcpRuntime,
  argumentsValue: unknown,
  signal?: AbortSignal,
): Promise<JsonObject> {
  const result = await runtime.execute(
    "prism_v1_get_property",
    argumentsValue,
    signal,
  );
  return privacySafeResult(result);
}

export const ORACLE_EXPLORER_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pasco Oracle public explorer</title>
  <style>
    :root { color-scheme: light; font-family: ui-sans-serif, system-ui, sans-serif; background: #f4f7f3; color: #13251d; }
    body { margin: 0; }
    main { width: min(1120px, calc(100% - 32px)); margin: 0 auto; padding: 36px 0 72px; }
    header, section { background: #fff; border: 1px solid #d8e2dc; border-radius: 16px; padding: 24px; margin-bottom: 18px; box-shadow: 0 8px 30px #1633210d; }
    h1, h2 { margin-top: 0; }
    .eyebrow { color: #2f6e4d; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; font-size: 12px; }
    .notice { border-left: 4px solid #c58919; padding-left: 14px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
    label { display: grid; gap: 6px; font-weight: 650; }
    input, button { font: inherit; padding: 10px 12px; border: 1px solid #aebdb4; border-radius: 9px; }
    button { background: #174f37; color: #fff; cursor: pointer; border-color: #174f37; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; background: #f5f7f5; padding: 14px; border-radius: 10px; max-height: 520px; overflow: auto; }
    #map { width: 100%; min-height: 300px; border-radius: 12px; background: #e7eee9; }
    .point { fill: #c35e2d; stroke: #fff; stroke-width: 1.5; }
    .muted { color: #52645a; }
  </style>
</head>
<body>
<main>
  <header>
    <div class="eyebrow">Read-only official public-record plane</div>
    <h1>Pasco Oracle explorer</h1>
    <p class="notice" id="coverage">Loading coverage identity…</p>
    <p class="notice" id="candidate-disclosure" hidden></p>
    <p class="muted">Parcel and folio identifiers, owner names, mailing-address values, phones, emails, contractor identities and permit numbers are not returned by this explorer. Missing permit coverage is unavailable—not zero.</p>
  </header>
  <section>
    <h2>Dataset and pipeline</h2>
    <div class="grid" id="metadata"></div>
  </section>
  <section>
    <h2>Map search</h2>
    <form id="search-form" class="grid">
      <label>Latitude<input name="latitude" type="number" min="-90" max="90" step="any" required></label>
      <label>Longitude<input name="longitude" type="number" min="-180" max="180" step="any" required></label>
      <label>Radius (km)<input name="radius" type="number" min="0.001" max="80.4672" step="any" value="10" required></label>
      <label>Maximum results<input name="limit" type="number" min="1" max="100" value="20" required></label>
      <button type="submit">Search validated public data</button>
    </form>
    <svg id="map" viewBox="0 0 800 360" role="img" aria-label="Search result coordinate plot"></svg>
    <pre id="search-result">No search submitted.</pre>
  </section>
  <section>
    <h2>Property lookup</h2>
    <form id="property-form" class="grid">
      <label>Public property ID<input name="propertyId" pattern="prop_[a-f0-9]{32}" required></label>
      <button type="submit">Get property facts</button>
    </form>
    <pre id="property-result">No property requested.</pre>
  </section>
</main>
<script>
const show = (id, value) => document.getElementById(id).textContent = JSON.stringify(value, null, 2);
const post = async (path, body) => {
  const response = await fetch(path, { method: 'POST', headers: {'content-type':'application/json'}, body: JSON.stringify(body) });
  return response.json();
};
const factValue = value => value && value.availability === 'available' ? value.value : null;
const plot = opportunities => {
  const svg = document.getElementById('map');
  svg.replaceChildren();
  const points = opportunities.map(item => factValue(item.property.coordinates)).filter(Boolean);
  if (!points.length) return;
  const lats = points.map(point => point.latitude), lons = points.map(point => point.longitude);
  const minLat = Math.min(...lats), maxLat = Math.max(...lats), minLon = Math.min(...lons), maxLon = Math.max(...lons);
  points.forEach(point => {
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('cx', String(24 + ((point.longitude - minLon) / (maxLon - minLon || 1)) * 752));
    circle.setAttribute('cy', String(336 - ((point.latitude - minLat) / (maxLat - minLat || 1)) * 312));
    circle.setAttribute('r', '5'); circle.setAttribute('class', 'point'); svg.append(circle);
  });
};
fetch('/explorer/api/bootstrap').then(response => response.json()).then(value => {
  document.getElementById('coverage').textContent = value.publication.coverageMode === 'authoritative_complete'
    ? 'Authoritative-complete coverage for the displayed sealed scope.'
    : value.publication.coverageMode === 'source_snapshot'
      ? 'Complete membership of the exact hash-bound source snapshot only. This candidate-owned demonstration is not authoritative-complete Pasco County coverage.'
      : value.publication.coverageMode + ' coverage only. This is not complete Pasco County coverage.';
  if (value.publication.candidateDemo) {
    const disclosure = document.getElementById('candidate-disclosure');
    disclosure.hidden = false;
    disclosure.textContent = value.publication.candidateDemo.disclosure;
  }
  const metadata = document.getElementById('metadata');
  const values = {
    contract: value.service.contractVersion,
    dataset: value.service.dataset.version,
    properties: value.publication.propertyCount,
    coordinates: value.publication.coordinateCount,
    candidateObjects: value.publication.candidateDemo?.objectCount ?? 'not applicable',
    remoteStatus: value.publication.candidateDemo?.remoteStatus ?? 'not applicable',
    permits: value.pipeline.coverage.permits.status,
    freshness: value.publication.freshness.asOf
  };
  Object.entries(values).forEach(([key, entry]) => {
    const item = document.createElement('div');
    const title = document.createElement('strong'); title.textContent = key;
    const text = document.createElement('div'); text.textContent = String(entry);
    item.append(title, text); metadata.append(item);
  });
}).catch(() => { document.getElementById('coverage').textContent = 'Validated publication metadata is unavailable.'; });
document.getElementById('search-form').addEventListener('submit', async event => {
  event.preventDefault(); const form = new FormData(event.currentTarget);
  const value = await post('/explorer/api/search', {
    county:'pasco', center:{kind:'coordinates', latitude:Number(form.get('latitude')), longitude:Number(form.get('longitude'))},
    radius:{value:Number(form.get('radius')),unit:'km'}, filters:{roofAge:{operator:'gte',years:0,basis:'direct_or_proxy'},matchMode:'all'},
    sort:'distance_asc', page:{limit:Number(form.get('limit'))}
  });
  show('search-result', value); plot(value.data?.opportunities || []);
});
document.getElementById('property-form').addEventListener('submit', async event => {
  event.preventDefault(); const form = new FormData(event.currentTarget);
  show('property-result', await post('/explorer/api/property', {propertyId:String(form.get('propertyId'))}));
});
</script>
</body>
</html>`;
