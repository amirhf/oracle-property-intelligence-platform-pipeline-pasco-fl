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

function privacySafeProperty(value: unknown): JsonObject {
  const property = structuredClone(record(value));
  property.ownership = ownershipSummary(property.ownership);
  return property;
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
  return response;
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
  return {
    service: data(service),
    pipeline: data(pipeline),
    capabilities: data(capabilities),
    publication: {
      candidateDemo:
        metadata.publication.candidateDemoPlanId === null
          ? null
          : {
              planId: metadata.publication.candidateDemoPlanId,
              planSha256: metadata.publication.candidateDemoPlanSha256,
              resolverPolicy: metadata.publication.resolverPolicy,
              disclosure:
                "Temporary candidate-owned Filebase demonstration of protocol compatibility. The buckets and IPNS identities are candidate-controlled and are not represented as Elephant-owned, owner-approved, authoritative-complete, or the final canonical assessment publication.",
            },
      coverageMode: metadata.coverageMode,
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
      limitations: metadata.limitations,
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
    <p class="muted">Owner names, mailing-address values, phones, emails and contractor identities are not rendered by this explorer. Missing permit coverage is unavailable—not zero.</p>
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
    : value.publication.coverageMode + ' coverage only. This is not complete Pasco County coverage.';
  const metadata = document.getElementById('metadata');
  const values = {
    contract: value.service.contractVersion,
    dataset: value.service.datasetVersion,
    properties: value.publication.propertyCount,
    coordinates: value.pipeline.coverage.coordinates.available,
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
