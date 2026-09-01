import { connect } from "node:net";

import { loadConfig } from "../services/lib/config.js";
import { checkDatabase } from "../services/lib/database.js";

interface HttpHealth {
  body: string;
  ok: boolean;
  status: number;
  url: string;
}

async function fetchHealth(
  url: string,
  init?: RequestInit,
): Promise<HttpHealth> {
  const response = await fetch(url, {
    ...init,
    signal: AbortSignal.timeout(5_000),
  });
  return {
    body: await response.text(),
    ok: response.ok,
    status: response.status,
    url,
  };
}

async function checkTcp(
  host: string,
  port: number,
): Promise<{ host: string; ok: true; port: number }> {
  return new Promise((resolve, reject) => {
    const socket = connect({ host, port });
    socket.setTimeout(5_000);
    socket.once("connect", () => {
      socket.destroy();
      resolve({ host, ok: true, port });
    });
    socket.once("error", reject);
    socket.once("timeout", () => {
      socket.destroy();
      reject(new Error(`Timed out connecting to ${host}:${port}`));
    });
  });
}

const config = loadConfig();
const checks = {
  database: await checkDatabase(config.databaseUrl),
  restate: await fetchHealth("http://localhost:9070/health"),
  service: await checkTcp("127.0.0.1", config.servicePort),
  serviceDatabase: await fetchHealth("http://localhost:8080/Parcel/health", {
    body: "{}",
    headers: { "content-type": "application/json" },
    method: "POST",
  }),
};

const ok = checks.restate.ok && checks.serviceDatabase.ok;
console.log(JSON.stringify({ checks, ok }, null, 2));
if (!ok) process.exitCode = 1;
