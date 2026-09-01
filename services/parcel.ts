import * as restate from "@restatedev/restate-sdk";

import { checkDatabase } from "./lib/database.js";

export function createParcelService(databaseUrl: string) {
  return restate.service({
    name: "Parcel",
    handlers: {
      health: async (ctx: restate.Context, _request: Record<string, never>) =>
        ctx.run("postgres-health", () => checkDatabase(databaseUrl)),
    },
  });
}
