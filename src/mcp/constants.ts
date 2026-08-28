export const MCP_CONTRACT_VERSION = "1.1.0";
export const MCP_SCHEMA_SHA256 =
  "1ef6f43072bc93ee8557aa9fcd0ce55eab26560fe4d061fac7c9388b2d0301c5";
export const MCP_SERVICE_NAME = "prism-pasco-oracle-mcp";
export const MCP_SERVICE_VERSION = "0.1.0";

export const MCP_TOOL_NAMES = [
  "prism_v1_get_service_info",
  "prism_v1_get_pipeline_run_summary",
  "prism_v1_search_roofing_opportunities",
  "prism_v1_get_property",
  "prism_v1_get_permit",
  "prism_v1_get_query_schema",
] as const;

export type McpToolName = (typeof MCP_TOOL_NAMES)[number];

export const MCP_TOOL_DEFINITIONS: Record<
  McpToolName,
  { description: string; input: string; output: string }
> = {
  prism_v1_get_service_info: {
    description:
      "Return the active Pasco public-data service and dataset metadata.",
    input: "ServiceInfoArguments",
    output: "ServiceInfoSuccessResult",
  },
  prism_v1_get_pipeline_run_summary: {
    description: "Return the latest completed Pasco publication source run.",
    input: "PipelineRunSummaryArguments",
    output: "PipelineRunSummarySuccessResult",
  },
  prism_v1_search_roofing_opportunities: {
    description:
      "Search the bounded Pasco public dataset with deterministic geospatial and roof-signal filters.",
    input: "SearchArguments",
    output: "SearchSuccessResult",
  },
  prism_v1_get_property: {
    description:
      "Look up one Pasco public property by its MCP property identifier.",
    input: "PropertyArguments",
    output: "PropertySuccessResult",
  },
  prism_v1_get_permit: {
    description:
      "Look up one public permit when permit source coverage is available.",
    input: "PermitArguments",
    output: "PermitSuccessResult",
  },
  prism_v1_get_query_schema: {
    description:
      "Return the frozen public query capabilities and safety limits.",
    input: "QuerySchemaArguments",
    output: "QuerySchemaSuccessResult",
  },
};

export const DEFAULT_MCP_PORT = 9090;
export const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_REQUEST_BYTES = 64 * 1024;
export const DEFAULT_MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
export const MAX_CURSOR_BYTES = 512;
