import { resourceUsage } from "node:process";
import { mkdir, rename, statfs, writeFile } from "node:fs/promises";
import path from "node:path";

import { ensureAppraiserInputs } from "../appraiser/acquire.js";
import {
  loadPilotCandidateData,
  loadSelectedOwners,
} from "../appraiser/parser.js";
import type { PreparedPilot } from "../domain/types.js";
import { fetchPascoCoordinates } from "../gis/pasco.js";
import { PILOT_SAMPLE_ALGORITHM, selectPilot } from "./sample.js";

export async function preparePilot(options: {
  asOf: string;
  dataDir: string;
  runId: string;
  sampleSeed: string;
}): Promise<PreparedPilot> {
  const startedAt = performance.now();
  const appraiser = await ensureAppraiserInputs(options.dataDir);
  const loaded = await loadPilotCandidateData(appraiser.paths);
  const selection = selectPilot(loaded.candidates, options.sampleSeed);
  const selectedFolios = new Set(
    selection.map((entry) => entry.parcel.exactFolio),
  );
  const seedDir = path.join(options.dataDir, "pasco", "seeds");
  await mkdir(seedDir, { recursive: true });
  const seedCsvPath = path.join(seedDir, "pasco-pilot.csv");
  const csvCell = (value: string) => `"${value.replaceAll('"', '""')}"`;
  const seedCsv = [
    "parcel_id,sample_rank,year_bucket,use_group,city",
    ...selection.map((entry) =>
      [
        entry.parcel.exactFolio,
        entry.rank,
        entry.yearBucket,
        entry.useGroup,
        entry.siteAddress?.city ?? "",
      ]
        .map(csvCell)
        .join(","),
    ),
  ].join("\n");
  await writeFile(`${seedCsvPath}.part`, `${seedCsv}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(`${seedCsvPath}.part`, seedCsvPath);
  const seedManifestPath = path.join(seedDir, "pasco-pilot.manifest.json");
  await writeFile(
    `${seedManifestPath}.part`,
    JSON.stringify({
      algorithm: PILOT_SAMPLE_ALGORITHM,
      count: selection.length,
      seed: options.sampleSeed,
      strata: selection.map((entry) => ({
        city: entry.siteAddress?.city ?? null,
        rank: entry.rank,
        useGroup: entry.useGroup,
        yearBucket: entry.yearBucket,
      })),
    }),
    { encoding: "utf8", mode: 0o600 },
  );
  await rename(`${seedManifestPath}.part`, seedManifestPath);
  const ownerResult = await loadSelectedOwners(
    appraiser.paths.owners,
    selectedFolios,
  );
  loaded.counts.owners = ownerResult.count;

  const gis = await fetchPascoCoordinates({
    dataDir: options.dataDir,
    exactFolios: [...selectedFolios],
    runId: options.runId,
  });

  const permitRequestCount = 0;
  const properties = selection.map((selected) => ({
    ...selected,
    coordinates: gis.coordinates.get(selected.parcel.exactFolio) ?? null,
    owners: ownerResult.owners.get(selected.parcel.exactFolio) ?? [],
    permits: [],
  }));
  const filesystem = await statfs(options.dataDir);

  const prepared: PreparedPilot = {
    artifacts: [...appraiser.artifacts, gis.artifact],
    gisMetrics: gis.metrics,
    permitRequestCount,
    properties,
    resourceMetrics: {
      diskAvailableBytes: filesystem.bavail * filesystem.bsize,
      elapsedMs: Math.round(performance.now() - startedAt),
      peakRssBytes: resourceUsage().maxRSS * 1_024,
    },
    sampleAlgorithm: PILOT_SAMPLE_ALGORITHM,
    sampleSeed: options.sampleSeed,
    selectionSize: properties.length,
    sourceCounts: loaded.counts,
    sourceLimitations: [
      "Pasco Accela collection stopped after challenge/CAPTCHA content was detected on the anonymous form GET; no permit search POST was sent, and permits and contractors are unavailable for this pilot.",
    ],
  };
  const preparedDir = path.join(
    options.dataDir,
    "pasco",
    "prepared",
    options.runId,
  );
  await mkdir(preparedDir, { recursive: true });
  const preparedPath = path.join(preparedDir, "pilot.json");
  await writeFile(`${preparedPath}.part`, JSON.stringify(prepared), {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(`${preparedPath}.part`, preparedPath);
  await writeFile(
    `${preparedPath}.ready.json.part`,
    `${JSON.stringify({ properties: prepared.properties.length, ready: true })}\n`,
    { encoding: "utf8", mode: 0o600 },
  );
  await rename(`${preparedPath}.ready.json.part`, `${preparedPath}.ready.json`);
  return prepared;
}
