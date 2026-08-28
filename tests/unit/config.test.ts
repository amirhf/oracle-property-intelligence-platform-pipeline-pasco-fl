import { describe, expect, it } from "vitest";

import { FOUNDATION_PORTS, loadConfig } from "../../services/lib/config.js";

const validEnvironment: NodeJS.ProcessEnv = {
  DATA_DIR: "/tmp/prism-pasco-oracle-data",
  DATABASE_URL: "postgresql://postgres:elephant@localhost:5433/elephant",
  SERVICE_PORT: "9080",
};

describe("foundation configuration", () => {
  it("accepts only the frozen local topology", () => {
    expect(loadConfig(validEnvironment)).toEqual({
      dataDir: "/tmp/prism-pasco-oracle-data",
      databaseUrl: validEnvironment.DATABASE_URL,
      servicePort: FOUNDATION_PORTS.service,
    });
  });

  it("rejects a relative DATA_DIR", () => {
    expect(() => loadConfig({ ...validEnvironment, DATA_DIR: "data" })).toThrow(
      "DATA_DIR must be an absolute host path",
    );
  });

  it("rejects protected host port 5432", () => {
    expect(() =>
      loadConfig({
        ...validEnvironment,
        DATABASE_URL: "postgresql://postgres:elephant@localhost:5432/elephant",
      }),
    ).toThrow("DATABASE_URL must use host port 5433");
  });

  it("rejects a non-local database host", () => {
    expect(() =>
      loadConfig({
        ...validEnvironment,
        DATABASE_URL:
          "postgresql://postgres:elephant@example.com:5433/elephant",
      }),
    ).toThrow("DATABASE_URL must use the local foundation PostgreSQL host");
  });

  it("rejects any host service port other than 9080", () => {
    expect(() =>
      loadConfig({ ...validEnvironment, SERVICE_PORT: "9081" }),
    ).toThrow("SERVICE_PORT must be 9080");
  });
});
