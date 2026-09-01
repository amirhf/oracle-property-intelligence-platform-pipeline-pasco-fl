import "dotenv/config";

import { loadConfig } from "../services/lib/config.js";
import { ensureAppraiserInputs } from "../src/appraiser/acquire.js";

const config = loadConfig();
const result = await ensureAppraiserInputs(config.dataDir);
console.log(
  JSON.stringify(
    {
      artifacts: result.artifacts.map((artifact) => ({
        bytes: artifact.bytes,
        filename: artifact.localPath.split("/").at(-1),
        sha256: artifact.sha256,
      })),
      ok: true,
    },
    null,
    2,
  ),
);
