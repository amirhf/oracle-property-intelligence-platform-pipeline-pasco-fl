import { describe, expect, it } from "vitest";

import {
  queryTableColumns,
  unavailablePublicationFields,
} from "../../src/publication/dry-run.js";
import { verifyPublicationUploadReceipt } from "../../src/publication/remote-boundary.js";
import {
  parsePreparePublicationRequest,
  parsePublicationApprovalRequest,
  parsePublicationStatusRequest,
} from "../../src/publication/requests.js";

describe("local publication dry run", () => {
  it("keeps the frozen searchable property fields and explicit availability columns", () => {
    expect(queryTableColumns()).toMatchObject({
      property_id: "VARCHAR",
      parcel_id: "VARCHAR",
      county: "VARCHAR",
      exact_folio: "VARCHAR",
      latitude: "DOUBLE",
      longitude: "DOUBLE",
      year_built: "INTEGER",
      roof_age_years: "INTEGER",
      roof_age_basis: "VARCHAR",
      permit_source_availability: "VARCHAR",
      contractor_source_availability: "VARCHAR",
      sunbiz_source_availability: "VARCHAR",
      bbb_source_availability: "VARCHAR",
      open_roofing_permit_count: "INTEGER",
      maximum_open_roofing_permit_days: "INTEGER",
      coverage_mode: "VARCHAR",
      coverage_scope_id: "VARCHAR",
      source_snapshot_id: "VARCHAR",
      source_run_id: "VARCHAR",
      selection_hash: "VARCHAR",
    });
  });

  it("represents unavailable permit aggregates as null rather than zero", () => {
    expect(unavailablePublicationFields()).toEqual({
      permit_source_availability: "unavailable",
      contractor_source_availability: "unavailable",
      sunbiz_source_availability: "unavailable",
      bbb_source_availability: "unavailable",
      open_roofing_permit_count: null,
      maximum_open_roofing_permit_days: null,
    });
  });

  it("strictly validates Publish/pasco requests", () => {
    expect(
      parsePreparePublicationRequest({
        county: "pasco",
        exportMode: "bounded",
        runId: `run_${"a".repeat(32)}`,
      }),
    ).toMatchObject({ exportMode: "bounded" });
    expect(() =>
      parsePreparePublicationRequest({
        county: "pasco",
        exportMode: "authoritative",
        runId: `run_${"a".repeat(32)}`,
        authoritative: true,
      }),
    ).toThrow("strict validation");
    expect(() =>
      parsePublicationApprovalRequest({
        approverReference: "controller",
        county: "pasco",
        planId: `plan_${"b".repeat(32)}`,
        planSha256: "c".repeat(64),
        unexpected: true,
      }),
    ).toThrow("strict validation");
    expect(parsePublicationStatusRequest({})).toEqual({});
    expect(() => parsePublicationStatusRequest({ county: "pasco" })).toThrow(
      "strict validation",
    );
  });

  it("treats a missing or mismatched provider CID as terminal", () => {
    const artifact = {
      byteSize: 1,
      domain: "open_data" as const,
      expectedCid: "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
      objectKey: "index.json",
      role: "root" as const,
      sha256: "a".repeat(64),
    };
    expect(() =>
      verifyPublicationUploadReceipt(artifact, {
        cid: null,
        domain: "open_data",
        objectKey: "index.json",
        sha256: artifact.sha256,
      }),
    ).toThrow("Terminal publication CID mismatch");
    expect(() =>
      verifyPublicationUploadReceipt(artifact, {
        cid: "QmYCTciJdFNMNUPCHSNS6dKMmUAqkGQ9tQQeGgbELhQQcn",
        domain: "open_data",
        objectKey: "index.json",
        sha256: artifact.sha256,
      }),
    ).toThrow("Terminal publication CID mismatch");
  });
});
