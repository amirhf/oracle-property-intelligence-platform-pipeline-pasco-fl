import { createHash } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";

import type { ArtifactCapture } from "../domain/types.js";

export async function captureTextArtifact(options: {
  body: string;
  finalPath: string;
  sourceSystem: string;
  sourceUrl: string;
}): Promise<ArtifactCapture> {
  const { body, finalPath, sourceSystem, sourceUrl } = options;
  const bytes = Buffer.byteLength(body);
  const hash = createHash("sha256").update(body).digest("hex");
  const partPath = `${finalPath}.part`;
  const readyMarkerPath = `${finalPath}.ready.json`;

  await mkdir(path.dirname(finalPath), { recursive: true });
  await writeFile(partPath, body, { encoding: "utf8", mode: 0o600 });
  await rename(partPath, finalPath);
  await writeFile(
    `${readyMarkerPath}.part`,
    `${JSON.stringify({ bytes, sha256: hash, sourceSystem, sourceUrl })}\n`,
    { encoding: "utf8", mode: 0o600 },
  );
  await rename(`${readyMarkerPath}.part`, readyMarkerPath);

  return {
    bytes,
    localPath: finalPath,
    readyMarkerPath,
    sha256: hash,
    sourceSystem,
    sourceUrl,
  };
}
