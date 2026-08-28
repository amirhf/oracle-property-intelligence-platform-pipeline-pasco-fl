import path from "node:path";

export const FOUNDATION_PORTS = {
  postgres: 5433,
  restateIngress: 8080,
  restateAdmin: 9070,
  service: 9080,
} as const;

export interface FoundationConfig {
  dataDir: string;
  databaseUrl: string;
  servicePort: typeof FOUNDATION_PORTS.service;
}

function requireValue(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function validateDatabaseUrl(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
    throw new Error("DATABASE_URL must use the postgres protocol");
  }
  if (url.hostname !== "localhost" && url.hostname !== "127.0.0.1") {
    throw new Error(
      "DATABASE_URL must use the local foundation PostgreSQL host",
    );
  }
  if (Number(url.port) !== FOUNDATION_PORTS.postgres) {
    throw new Error(
      `DATABASE_URL must use host port ${FOUNDATION_PORTS.postgres}`,
    );
  }
  return value;
}

export function loadConfig(
  environment: NodeJS.ProcessEnv = process.env,
): FoundationConfig {
  const dataDir = requireValue(environment, "DATA_DIR");
  if (!path.isAbsolute(dataDir)) {
    throw new Error("DATA_DIR must be an absolute host path");
  }

  const requestedPort = Number(
    environment.SERVICE_PORT ?? FOUNDATION_PORTS.service,
  );
  if (requestedPort !== FOUNDATION_PORTS.service) {
    throw new Error(`SERVICE_PORT must be ${FOUNDATION_PORTS.service}`);
  }

  return {
    dataDir: path.resolve(dataDir),
    databaseUrl: validateDatabaseUrl(requireValue(environment, "DATABASE_URL")),
    servicePort: FOUNDATION_PORTS.service,
  };
}
