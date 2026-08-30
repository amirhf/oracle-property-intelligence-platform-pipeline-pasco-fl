import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { createLoaderOperationSingleFlight } from "../../services/pipeline.js";
import {
  APPRAISER_HEADERS,
  assertExactCsvHeader,
} from "../../src/appraiser/parser.js";
import {
  buildOwnerAuthorityRecord,
  OWNER_AUTHORITY_CLASS,
  PASCO_PARCEL_FOLIO_COUNT,
  PASCO_PARCEL_FOLIO_SET_SHA256,
  PASCO_PARCEL_MEMBERSHIP_CLAIM,
  PASCO_PARCEL_SCOPE_MEMBERSHIP_RULE,
  validateOwnerAuthorityRecord,
  writeOwnerAuthorityRecord,
} from "../../src/authoritative/authority.js";
import { deterministicId } from "../../src/lib/hash.js";
import { assertVerifiedLocalPascoGisInventory } from "../../src/gis/pasco.js";
import {
  AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
  AUTHORITATIVE_PARCEL_SELECTION_SEED,
} from "../../src/snapshot/coverage.js";
import { parseCountyIngestRequest } from "../../src/workflow/schemas.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

function exactCounts() {
  return {
    accepted: PASCO_PARCEL_FOLIO_COUNT,
    distinctFolios: PASCO_PARCEL_FOLIO_COUNT,
    duplicateFolios: 0,
    parsed: PASCO_PARCEL_FOLIO_COUNT,
    rejected: 0,
    sortedFolioSetSha256: PASCO_PARCEL_FOLIO_SET_SHA256,
    source: PASCO_PARCEL_FOLIO_COUNT,
  };
}

describe("owner-accepted authoritative Pasco source", () => {
  it("builds one exact deterministic immutable authority record", () => {
    const first = buildOwnerAuthorityRecord(exactCounts());
    const second = buildOwnerAuthorityRecord(exactCounts());
    expect(second).toEqual(first);
    expect(validateOwnerAuthorityRecord(first)).toEqual(first);
    expect(first).toMatchObject({
      authorityClass: OWNER_AUTHORITY_CLASS,
      payload: {
        coverageDefinition: PASCO_PARCEL_MEMBERSHIP_CLAIM,
        counts: {
          accepted: 325_213,
          distinctFolios: 325_213,
          duplicateFolios: 0,
          rejected: 0,
        },
        unresolvedSemanticDiscrepancy: {
          publishedRealPropertyParcelStatistic: 335_946,
        },
      },
    });
  });

  it.each([
    ["source", 325_212],
    ["parsed", 325_212],
    ["accepted", 325_212],
    ["distinctFolios", 325_212],
    ["rejected", 1],
    ["duplicateFolios", 1],
    ["sortedFolioSetSha256", "0".repeat(64)],
  ] as const)("rejects mismatched %s control", (field, value) => {
    expect(() =>
      buildOwnerAuthorityRecord({ ...exactCounts(), [field]: value }),
    ).toThrow("Authoritative source mismatch");
  });

  it.each([
    ["archive sha", ["payload", "archive", "sha256"], "0".repeat(64)],
    ["archive bytes", ["payload", "archive", "byteSize"], 1],
    [
      "CSV sha",
      ["payload", "archive", "entries", "0", "sha256"],
      "0".repeat(64),
    ],
    ["parser", ["payload", "parserVersion"], "other-parser"],
    ["schema", ["payload", "canonicalSchemaSha256"], "0".repeat(64)],
    ["authority", ["payload", "authorityClass"], "caller-asserted"],
  ] as const)("rejects tampered %s", (_label, keys, replacement) => {
    const record = structuredClone(
      buildOwnerAuthorityRecord(exactCounts()),
    ) as unknown as Record<string, unknown> | unknown[];
    let cursor = record;
    for (const key of keys.slice(0, -1)) {
      cursor = (cursor as Record<string, unknown>)[key] as
        Record<string, unknown> | unknown[];
    }
    (cursor as Record<string, unknown>)[keys.at(-1)!] = replacement;
    expect(() => validateOwnerAuthorityRecord(record)).toThrow();
  });

  it("rejects caller promotion and binds the exact complete-source request", () => {
    const workflowId = "pasco-authoritative-appraiser-2026-08-23-v1";
    const valid = {
      asOf: "2026-08-23T11:07:02.000Z",
      county: "pasco",
      runId: deterministicId("run", [
        "1.0.0",
        "pipeline-run",
        "pasco",
        workflowId,
      ]),
      sampleAlgorithm: AUTHORITATIVE_PARCEL_SELECTION_ALGORITHM,
      sampleSeed: AUTHORITATIVE_PARCEL_SELECTION_SEED,
      selectionSize: PASCO_PARCEL_FOLIO_COUNT,
      workflowId,
    };
    expect(parseCountyIngestRequest(valid)).toEqual(valid);
    expect(PASCO_PARCEL_SCOPE_MEMBERSHIP_RULE).toBe(
      "pasco_appraiser:owner_accepted_complete_parcel_membership-v1",
    );
    expect(() =>
      parseCountyIngestRequest({ ...valid, sampleSeed: "caller-promoted" }),
    ).toThrow("fixed complete-source selection identity");
    expect(() =>
      parseCountyIngestRequest({ ...valid, selectionSize: 25 }),
    ).toThrow("bounded selection");
    expect(() =>
      parseCountyIngestRequest({
        ...valid,
        asOf: "2026-08-24T11:07:02.000Z",
      }),
    ).toThrow("fixed source observation");
  });

  it("atomically adopts identical concurrent authority records", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "authority-record-"));
    temporaryDirectories.push(directory);
    const record = buildOwnerAuthorityRecord(exactCounts());
    const [first, second] = await Promise.all([
      writeOwnerAuthorityRecord(directory, record),
      writeOwnerAuthorityRecord(directory, record),
    ]);
    expect(second).toEqual(first);
    const artifactDirectory = path.dirname(first.filePath);
    expect((await readdir(artifactDirectory)).sort()).toEqual(["record.json"]);
  });

  it("accepts exact appraiser headers and rejects schema drift", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "authority-header-"));
    temporaryDirectories.push(directory);
    const filePath = path.join(directory, "parcel.csv");
    await writeFile(
      filePath,
      `${APPRAISER_HEADERS.parcel.map((header) => `"${header}"`).join(",")}\n`,
    );
    await expect(
      assertExactCsvHeader(filePath, APPRAISER_HEADERS.parcel),
    ).resolves.toBeUndefined();
    await writeFile(filePath, '"Parcel_Num","Unexpected"\n');
    await expect(
      assertExactCsvHeader(filePath, APPRAISER_HEADERS.parcel),
    ).rejects.toThrow("header mismatch");
  });

  it("rejects an unpinned local GIS checkpoint inventory", () => {
    expect(() => assertVerifiedLocalPascoGisInventory([])).toThrow(
      "verified checkpoint set",
    );
  });
});

describe("authoritative Loader process single-flight", () => {
  it("shares one exact in-flight operation across concurrent Restate replays", async () => {
    const singleFlight = createLoaderOperationSingleFlight();
    let starts = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const operation = async () => {
      starts += 1;
      await gate;
      return { result: "stored" };
    };

    const first = singleFlight.run("load_exact", "binding_exact", operation);
    const replay = singleFlight.run("load_exact", "binding_exact", operation);
    await Promise.resolve();
    expect(starts).toBe(1);
    release();
    await expect(Promise.all([first, replay])).resolves.toEqual([
      { result: "stored" },
      { result: "stored" },
    ]);

    await expect(
      singleFlight.run("load_exact", "binding_exact", async () => {
        starts += 1;
        return { result: "database-replay" };
      }),
    ).resolves.toEqual({ result: "database-replay" });
    expect(starts).toBe(2);
  });

  it("rejects a conflicting concurrent binding without starting it", async () => {
    const singleFlight = createLoaderOperationSingleFlight();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = singleFlight.run("load_exact", "binding_a", async () => {
      await gate;
      return "first";
    });
    let conflictingStarts = 0;

    await expect(
      singleFlight.run("load_exact", "binding_b", async () => {
        conflictingStarts += 1;
        return "conflict";
      }),
    ).rejects.toThrow("does not match the in-flight input");
    expect(conflictingStarts).toBe(0);
    release();
    await expect(first).resolves.toBe("first");
  });

  it("clears a rejected operation so a later exact replay can recover", async () => {
    const singleFlight = createLoaderOperationSingleFlight();
    await expect(
      singleFlight.run("load_exact", "binding_exact", async () => {
        throw new Error("transient local failure");
      }),
    ).rejects.toThrow("transient local failure");

    await expect(
      singleFlight.run("load_exact", "binding_exact", async () => "recovered"),
    ).resolves.toBe("recovered");
  });
});
