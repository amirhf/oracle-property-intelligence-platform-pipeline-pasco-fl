import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { access, mkdir, rename, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import unzipper from "unzipper";

import type { ArtifactCapture } from "../domain/types.js";
import { SourceAccessStopError } from "../lib/access-stop.js";

const MAX_OBJECT_BYTES = 5 * 1024 * 1024 * 1024;

export const APPRAISER_FILES = [
  {
    csvName: "parcel.csv",
    sha256: "bffeead6aa18d9e53e5da9efafa5533b24e7d563b733b1d327bdc0a5cb62cac9",
    url: "https://ftp01.pascopa.com/real_estate/parcel.zip",
    zipName: "parcel.zip",
  },
  {
    csvName: "building.csv",
    sha256: "2713bc38194e30f80bfa96bb4f80b3adc9401cb932e13167a8628fd8dc66c3b9",
    url: "https://ftp01.pascopa.com/real_estate/building.zip",
    zipName: "building.zip",
  },
  {
    csvName: "owners.csv",
    sha256: "001d84eac29f74225a825f8d74b06854338db96ac5a7719c5f8dce9bfafc2bf3",
    url: "https://ftp01.pascopa.com/real_estate/owners.zip",
    zipName: "owners.zip",
  },
  {
    csvName: "site_addresses.csv",
    sha256: "42c30161bde57047d9522bc10827fe491c0fc54b75be92856dff1629e2b92505",
    url: "https://ftp01.pascopa.com/real_estate/site_addresses.zip",
    zipName: "site_addresses.zip",
  },
] as const;

async function fileHash(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

async function exists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function inspectRemote(url: string): Promise<{
  bytes: number;
  lastModified: string | null;
}> {
  const response = await fetch(url, {
    method: "HEAD",
    signal: AbortSignal.timeout(30_000),
  });
  if (response.status === 403 || response.status === 429) {
    throw new SourceAccessStopError(
      `Pasco appraiser access stop: HTTP ${response.status}`,
    );
  }
  if (!response.ok) {
    throw new Error(`Pasco appraiser metadata failed: HTTP ${response.status}`);
  }
  const bytes = Number(response.headers.get("content-length"));
  if (!Number.isSafeInteger(bytes) || bytes <= 0) {
    throw new Error(`Pasco appraiser object size is unavailable: ${url}`);
  }
  if (bytes > MAX_OBJECT_BYTES) {
    throw new SourceAccessStopError(
      `Approval required: Pasco appraiser object is ${bytes} bytes (>5 GiB)`,
    );
  }
  return { bytes, lastModified: response.headers.get("last-modified") };
}

async function ensureZip(options: {
  expectedSha256: string;
  targetPath: string;
  url: string;
}): Promise<{ bytes: number; sha256: string }> {
  if (await exists(options.targetPath)) {
    const currentStat = await stat(options.targetPath);
    const currentHash = await fileHash(options.targetPath);
    if (currentHash !== options.expectedSha256) {
      throw new Error(
        `Existing appraiser capture differs from the pinned pilot object: ${path.basename(options.targetPath)}`,
      );
    }
    return { bytes: currentStat.size, sha256: currentHash };
  }

  const metadata = await inspectRemote(options.url);
  const response = await fetch(options.url, {
    headers: { "user-agent": "Prism-Pasco-Pilot/1.0" },
    signal: AbortSignal.timeout(300_000),
  });
  if (response.status === 403 || response.status === 429) {
    throw new SourceAccessStopError(
      `Pasco appraiser access stop: HTTP ${response.status}`,
    );
  }
  if (!response.ok || !response.body) {
    throw new Error(`Pasco appraiser download failed: HTTP ${response.status}`);
  }
  const partPath = `${options.targetPath}.part`;
  await pipeline(
    Readable.fromWeb(response.body as never),
    createWriteStream(partPath, { mode: 0o600 }),
  );
  const downloaded = await stat(partPath);
  const downloadedHash = await fileHash(partPath);
  if (
    downloaded.size !== metadata.bytes ||
    downloadedHash !== options.expectedSha256
  ) {
    throw new Error(`Downloaded appraiser object failed size/hash validation`);
  }
  await rename(partPath, options.targetPath);
  return { bytes: downloaded.size, sha256: downloadedHash };
}

async function ensureExtracted(
  zipPath: string,
  csvName: string,
  targetPath: string,
): Promise<void> {
  if (await exists(targetPath)) return;
  const archive = await unzipper.Open.file(zipPath);
  const entry = archive.files.find((file) => file.path === csvName);
  if (!entry) throw new Error(`${csvName} is missing from ${zipPath}`);
  const partPath = `${targetPath}.part`;
  await pipeline(entry.stream(), createWriteStream(partPath, { mode: 0o600 }));
  await rename(partPath, targetPath);
}

export async function ensureAppraiserInputs(dataDir: string): Promise<{
  artifacts: ArtifactCapture[];
  paths: {
    building: string;
    owners: string;
    parcel: string;
    siteAddresses: string;
  };
}> {
  const rawDir = path.join(dataDir, "pasco", "raw", "appraiser", "2026-08-23");
  const stagingDir = path.join(
    dataDir,
    "pasco",
    "staging",
    "appraiser",
    "2026-08-23",
  );
  await mkdir(rawDir, { recursive: true });
  await mkdir(stagingDir, { recursive: true });
  const artifacts: ArtifactCapture[] = [];

  for (const file of APPRAISER_FILES) {
    const zipPath = path.join(rawDir, file.zipName);
    const verified = await ensureZip({
      expectedSha256: file.sha256,
      targetPath: zipPath,
      url: file.url,
    });
    const readyMarkerPath = `${zipPath}.ready.json`;
    await writeFile(
      `${readyMarkerPath}.part`,
      `${JSON.stringify({ bytes: verified.bytes, sha256: verified.sha256, sourceSystem: "pasco_appraiser", sourceUrl: file.url })}\n`,
      { encoding: "utf8", mode: 0o600 },
    );
    await rename(`${readyMarkerPath}.part`, readyMarkerPath);
    artifacts.push({
      bytes: verified.bytes,
      localPath: zipPath,
      readyMarkerPath,
      sha256: verified.sha256,
      sourceSystem: "pasco_appraiser",
      sourceUrl: file.url,
    });
    await ensureExtracted(
      zipPath,
      file.csvName,
      path.join(stagingDir, file.csvName),
    );
  }

  return {
    artifacts,
    paths: {
      building: path.join(stagingDir, "building.csv"),
      owners: path.join(stagingDir, "owners.csv"),
      parcel: path.join(stagingDir, "parcel.csv"),
      siteAddresses: path.join(stagingDir, "site_addresses.csv"),
    },
  };
}
