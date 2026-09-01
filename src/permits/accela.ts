import path from "node:path";

import { load } from "cheerio";

import type { AccelaPermitRow, ArtifactCapture } from "../domain/types.js";
import { captureTextArtifact } from "../lib/artifacts.js";
import { SourceAccessStopError } from "../lib/access-stop.js";
import { isRoofingRelevant } from "../domain/signals.js";

export const PASCO_ACCELA_URL =
  "https://aca-prod.accela.com/PASCO/Cap/CapHome.aspx?module=Permits&TabName=Permits";

function normalizedText(value: string): string | null {
  const result = value.replace(/\s+/g, " ").trim();
  return result.length > 0 ? result : null;
}

export function parseAccelaSearchResults(body: string): AccelaPermitRow[] {
  const $ = load(body);
  const records: AccelaPermitRow[] = [];
  $("table").each((_tableIndex, table) => {
    const headers = $(table)
      .find("tr")
      .first()
      .find("th,td")
      .toArray()
      .map((cell) => normalizedText($(cell).text())?.toLowerCase() ?? "");
    const recordNumberIndex = headers.findIndex((value) =>
      /record number/.test(value),
    );
    const recordTypeIndex = headers.findIndex((value) =>
      /record type/.test(value),
    );
    if (recordNumberIndex < 0 || recordTypeIndex < 0) return;
    const findIndex = (pattern: RegExp) =>
      headers.findIndex((value) => pattern.test(value));
    const dateIndex = findIndex(/^date$/);
    const descriptionIndex = findIndex(/^description$/);
    const projectIndex = findIndex(/project name/);
    const statusIndex = findIndex(/^status$/);
    const addressIndex = findIndex(/^address$/);

    $(table)
      .find("tr")
      .slice(1)
      .each((_rowIndex, row) => {
        const cells = $(row).find("td").toArray();
        const read = (index: number): string | null =>
          index < 0 || !cells[index]
            ? null
            : normalizedText($(cells[index]).text());
        const recordNumber = read(recordNumberIndex);
        const recordType = read(recordTypeIndex);
        if (!recordNumber || !recordType) return;
        const candidate: AccelaPermitRow = {
          address: read(addressIndex),
          description: read(descriptionIndex),
          projectName: read(projectIndex),
          recordDate: read(dateIndex),
          recordNumber,
          recordType,
          status: read(statusIndex),
        };
        if (
          isRoofingRelevant(
            candidate.recordType,
            candidate.description,
            candidate.projectName,
          )
        ) {
          records.push(candidate);
        }
      });
  });
  return records.sort((left, right) =>
    left.recordNumber.localeCompare(right.recordNumber),
  );
}

function challengeReason(status: number, body: string): string | null {
  if (status === 403) return "HTTP 403";
  if (status === 429) return "HTTP 429";
  if (/captcha|verify you are human|access denied|cf-chl/i.test(body)) {
    return "challenge or CAPTCHA content";
  }
  return null;
}

function responseCookies(response: Response): string {
  const headers = response.headers as Headers & {
    getSetCookie?: () => string[];
  };
  const values = headers.getSetCookie?.() ?? [];
  return values.map((value) => value.split(";", 1)[0]).join("; ");
}

function buildSearchForm(
  html: string,
  exactFolio: string,
  asOf: string,
): { action: string; form: URLSearchParams } {
  const $ = load(html);
  const formElement = $("form#aspnetForm");
  const action = formElement.attr("action");
  if (!action) throw new Error("Accela search form action is missing");
  const form = new URLSearchParams();
  formElement.find("input[type=hidden][name]").each((_index, element) => {
    const name = $(element).attr("name");
    if (name) form.set(name, $(element).attr("value") ?? "");
  });
  form.set("__EVENTTARGET", "ctl00$PlaceHolderMain$btnNewSearch");
  form.set("__EVENTARGUMENT", "");
  form.set("ctl00$PlaceHolderMain$ddlSearchType", "GeneralSearch");
  form.set(
    "ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate",
    "09/02/2006",
  );
  const asOfDate = new Date(asOf);
  const endDate = `${String(asOfDate.getUTCMonth() + 1).padStart(2, "0")}/${String(asOfDate.getUTCDate()).padStart(2, "0")}/${asOfDate.getUTCFullYear()}`;
  form.set("ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate", endDate);
  form.set("ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo", exactFolio);
  return { action: new URL(action, PASCO_ACCELA_URL).toString(), form };
}

export async function fetchAccelaPermits(options: {
  asOf: string;
  dataDir: string;
  exactFolio: string;
  propertyId: string;
  runId: string;
}): Promise<{
  artifacts: ArtifactCapture[];
  permits: AccelaPermitRow[];
  requestCount: 2;
}> {
  const headers = { "user-agent": "Prism-Pasco-Pilot/1.0" };
  const formResponse = await fetch(PASCO_ACCELA_URL, {
    headers,
    redirect: "follow",
    signal: AbortSignal.timeout(30_000),
  });
  const formBody = await formResponse.text();
  const formChallenge = challengeReason(formResponse.status, formBody);
  if (formChallenge) {
    throw new SourceAccessStopError(
      `Pasco Accela access stop: ${formChallenge}`,
    );
  }
  if (!formResponse.ok) {
    throw new Error(`Pasco Accela form failed: HTTP ${formResponse.status}`);
  }
  const formCapture = await captureTextArtifact({
    body: formBody,
    finalPath: path.join(
      options.dataDir,
      "pasco",
      "raw",
      "permits",
      options.runId,
      `${options.propertyId}-form.html`,
    ),
    sourceSystem: "pasco_accela",
    sourceUrl: PASCO_ACCELA_URL,
  });
  const { action, form } = buildSearchForm(
    formBody,
    options.exactFolio,
    options.asOf,
  );
  const searchResponse = await fetch(action, {
    body: form,
    headers: {
      ...headers,
      cookie: responseCookies(formResponse),
      "content-type": "application/x-www-form-urlencoded",
      referer: formResponse.url,
    },
    method: "POST",
    redirect: "follow",
    signal: AbortSignal.timeout(45_000),
  });
  const resultBody = await searchResponse.text();
  const resultChallenge = challengeReason(searchResponse.status, resultBody);
  if (resultChallenge) {
    throw new SourceAccessStopError(
      `Pasco Accela access stop: ${resultChallenge}`,
    );
  }
  if (!searchResponse.ok) {
    throw new Error(
      `Pasco Accela search failed: HTTP ${searchResponse.status}`,
    );
  }
  const resultCapture = await captureTextArtifact({
    body: resultBody,
    finalPath: path.join(
      options.dataDir,
      "pasco",
      "raw",
      "permits",
      options.runId,
      `${options.propertyId}-results.html`,
    ),
    sourceSystem: "pasco_accela",
    sourceUrl: action,
  });
  return {
    artifacts: [formCapture, resultCapture],
    permits: parseAccelaSearchResults(resultBody),
    requestCount: 2,
  };
}
