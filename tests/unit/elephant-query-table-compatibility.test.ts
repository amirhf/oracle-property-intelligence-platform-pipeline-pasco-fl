import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { DuckDBInstance, type DuckDBValue } from "@duckdb/node-api";
import { afterEach, describe, expect, it } from "vitest";

import { validateElephantQueryTableCompatibility } from "../../src/publication/dry-run.js";

// Independent test-owned snapshot of the unchanged Elephant reader/producer
// contract. These expectations intentionally do not import Oracle's production
// column map or query list.
const EXPECTED_COLUMNS = {
  address_city: "VARCHAR",
  address_street: "VARCHAR",
  address_zip: "VARCHAR",
  assessed_value: "DOUBLE",
  avm_value: "DOUBLE",
  built_year: "BIGINT",
  county_name: "VARCHAR",
  exterior_wall_material: "VARCHAR",
  has_bbb_contractor: "BOOLEAN",
  has_permits: "BOOLEAN",
  has_sunbiz_tenant: "BOOLEAN",
  hoa_flag: "BOOLEAN",
  land_value: "DOUBLE",
  last_sale_date: "VARCHAR",
  last_sale_price: "DOUBLE",
  latitude: "DOUBLE",
  livable_floor_area: "DOUBLE",
  longitude: "DOUBLE",
  lot_area_sqft: "DOUBLE",
  lot_size_acre: "DOUBLE",
  market_value: "DOUBLE",
  owner_count: "BIGINT",
  owner_name: "VARCHAR",
  owner_occupied: "BOOLEAN",
  owners_text: "VARCHAR",
  parcel_identifier: "VARCHAR",
  permit_count: "BIGINT",
  property_cid: "VARCHAR",
  property_id: "VARCHAR",
  property_type: "VARCHAR",
  property_usage_type: "VARCHAR",
  request_identifier: "VARCHAR",
  roof_covering_material: "VARCHAR",
  source_system: "VARCHAR",
  state_code: "VARCHAR",
  subdivision: "VARCHAR",
  total_area: "DOUBLE",
} as const;

const UNCHANGED_ELEPHANT_QUERIES = [
  {
    params: ["PARCEL-1"],
    sql: "SELECT property_cid FROM properties WHERE parcel_identifier = $1 LIMIT 1",
  },
  {
    params: ["property-1"],
    sql: "SELECT property_cid FROM properties WHERE property_id = $1 LIMIT 1",
  },
  { params: [], sql: "SELECT count(*) AS c FROM properties" },
  {
    params: [2, 0],
    sql: `SELECT property_id, parcel_identifier, property_cid, county_name,
                 address_street, address_city, address_zip, market_value, owner_name
          FROM properties
          ORDER BY parcel_identifier
          LIMIT $1 OFFSET $2`,
  },
  {
    params: [],
    sql: `SELECT count(*) AS c, any_value(county_name) AS county,
                 any_value(state_code) AS state FROM properties`,
  },
  {
    params: [-90, 90, -180, 180],
    sql: `SELECT parcel_identifier, request_identifier, latitude, longitude,
                 avm_value, property_type
          FROM properties
          WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            AND latitude BETWEEN $1 AND $2
            AND longitude BETWEEN $3 AND $4`,
  },
  { params: [], sql: "DESCRIBE properties" },
  {
    params: [],
    sql: "SELECT count(*) FROM properties WHERE lot_size_acre > 2 AND address_city ILIKE 'jupiter'",
  },
  {
    params: [],
    sql: "SELECT count(*) FROM properties WHERE owners_text ILIKE '%SMITH, JOHN%'",
  },
  {
    params: [],
    sql: "SELECT count(*) FROM properties WHERE address_zip = '33410' AND exterior_wall_material ILIKE '%concrete%'",
  },
] as const;

const temporaryRoots: string[] = [];

async function temporaryParquet(schemaSql: string): Promise<string> {
  const root = await mkdtemp(path.join(tmpdir(), "oracle-elephant-contract-"));
  temporaryRoots.push(root);
  const parquetPath = path.join(root, "query-table.parquet");
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    await connection.run(schemaSql);
    await connection.run(
      `COPY properties TO '${parquetPath.replaceAll("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)`,
    );
  } finally {
    connection.closeSync();
  }
  return parquetPath;
}

afterEach(async () => {
  await Promise.all(
    temporaryRoots
      .splice(0)
      .map((root) => rm(root, { force: true, recursive: true })),
  );
});

describe("unchanged Elephant query-table consumer contract", () => {
  it("anchors every exercised projection to the read-only Elephant consumers", async () => {
    const references = await Promise.all([
      readFile(
        path.resolve("..", "elephant-mcp", "src", "lib", "duckdbQuery.ts"),
        "utf8",
      ),
      readFile(
        path.resolve("..", "elephant-mcp", "src", "tools", "oracleOpenData.ts"),
        "utf8",
      ),
      readFile(
        path.resolve("..", "elephant-mcp", "src", "tools", "oracleGeo.ts"),
        "utf8",
      ),
      readFile(
        path.resolve(
          "..",
          "elephant-query-db",
          "scripts",
          "run-query-table-export.ts",
        ),
        "utf8",
      ),
    ]);
    const authoritativeSource = references
      .join("\n")
      .replaceAll(/\s+/g, " ")
      .toLowerCase();
    for (const requiredFragment of [
      'const column = isparcel ? "parcel_identifier" : "property_id"',
      "select property_cid from ${properties_view} where ${column} = $1 limit 1",
      "select count(*) as c from ${properties_view}",
      "property_id, parcel_identifier, property_cid, county_name, address_street, address_city, address_zip, market_value, owner_name",
      "any_value(county_name) as county",
      "parcel_identifier, request_identifier, latitude, longitude, avm_value, property_type",
      "describe ${config.view}",
      "lot_size_acre > 2 and address_city ilike 'jupiter'",
      "owners_text ilike '%smith, john%'",
      "address_zip = '33410' and exterior_wall_material ilike '%concrete%'",
    ]) {
      expect(authoritativeSource).toContain(requiredFragment);
    }
  });

  it("reproduces the original Oracle binder defect against the legacy schema", async () => {
    const parquetPath = await temporaryParquet(`
      CREATE TABLE properties AS
      SELECT 'property-1'::VARCHAR AS property_id,
             'PARCEL-1'::VARCHAR AS parcel_id,
             'pasco'::VARCHAR AS county,
             'PARCEL-1'::VARCHAR AS exact_folio
    `);

    await expect(
      validateElephantQueryTableCompatibility(parquetPath),
    ).rejects.toThrow(/parcel_identifier|property_cid|Binder/i);
  });

  it("runs every unchanged fixed Elephant projection and common query", async () => {
    const definitions = Object.entries(EXPECTED_COLUMNS)
      .map(([name, type]) => `${name} ${type}`)
      .join(", ");
    const parquetPath = await temporaryParquet(
      `CREATE TABLE properties (${definitions})`,
    );
    const instance = await DuckDBInstance.create(":memory:");
    const connection = await instance.connect();
    try {
      await connection.run(
        `CREATE VIEW properties AS SELECT * FROM read_parquet('${parquetPath.replaceAll("'", "''")}')`,
      );
      for (const query of UNCHANGED_ELEPHANT_QUERIES) {
        await expect(
          connection.runAndReadAll(query.sql, [
            ...(query.params as readonly DuckDBValue[]),
          ]),
        ).resolves.toBeDefined();
      }
      const described = await connection.runAndReadAll("DESCRIBE properties");
      const actual = Object.fromEntries(
        described
          .getRowObjectsJson()
          .map((row) => [
            String(row.column_name),
            String(row.column_type).toUpperCase(),
          ]),
      );
      expect(actual).toEqual(EXPECTED_COLUMNS);
    } finally {
      connection.closeSync();
    }
  });
});
