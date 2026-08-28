import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";

import type {
  Ajv2020 as Ajv2020Class,
  ErrorObject,
  ValidateFunction,
} from "ajv/dist/2020.js";
import type { FormatsPlugin } from "ajv-formats";

const require = createRequire(import.meta.url);
const Ajv2020 = require("ajv/dist/2020.js") as typeof Ajv2020Class;
const addFormats = require("ajv-formats") as FormatsPlugin;

export const FIXTURE_DEFINITIONS = {
  "error-response.json": "ErrorResponseFixture",
  "permit-request.json": "PermitRequestFixture",
  "permit-response.json": "PermitResponseFixture",
  "pipeline-run-summary-request.json": "PipelineRunSummaryRequestFixture",
  "pipeline-run-summary-response.json": "PipelineRunSummaryResponseFixture",
  "property-request.json": "PropertyRequestFixture",
  "property-response.json": "PropertyResponseFixture",
  "query-schema-request.json": "QuerySchemaRequestFixture",
  "query-schema-response.json": "QuerySchemaResponseFixture",
  "search-request.json": "SearchRequestFixture",
  "search-response.json": "SearchResponseFixture",
  "service-info-request.json": "ServiceInfoRequestFixture",
  "service-info-response.json": "ServiceInfoResponseFixture",
} as const;

export type FixtureName = keyof typeof FIXTURE_DEFINITIONS;

interface McpSchema {
  $id: string;
}

interface LockedFile {
  path: string;
  sha256: string;
}

interface ContractLock {
  canonicalSchema: LockedFile;
  mcpSchema: LockedFile;
  sharedFixtures: LockedFile[];
}

export interface ValidationFailure {
  errors: ErrorObject[];
  fixture: FixtureName;
}

export interface ContractValidationResult {
  fixtureCount: number;
  fixtureFailures: ValidationFailure[];
  hashFailures: Array<{ actual: string; expected: string; path: string }>;
}

async function readJson<T>(filePath: string): Promise<T> {
  return JSON.parse(await readFile(filePath, "utf8")) as T;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function semanticError(instancePath: string, keyword: string): ErrorObject {
  return {
    instancePath,
    schemaPath: `#/$semantic/${keyword}`,
    keyword,
    params: {},
    message: "violates the MCP ownership publication semantics",
  };
}

function normalizeAddress(value: string): string {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function propertyRecords(
  fixture: FixtureName,
  value: unknown,
): Array<{ path: string; property: Record<string, unknown> }> {
  if (
    !isRecord(value) ||
    !isRecord(value.result) ||
    !isRecord(value.result.data)
  ) {
    return [];
  }
  if (fixture === "property-response.json") {
    return [{ path: "/result/data", property: value.result.data }];
  }
  if (fixture !== "search-response.json") return [];
  const opportunities = value.result.data.opportunities;
  if (!Array.isArray(opportunities)) return [];
  return opportunities.flatMap((opportunity, index) =>
    isRecord(opportunity) && isRecord(opportunity.property)
      ? [
          {
            path: `/result/data/opportunities/${index}/property`,
            property: opportunity.property,
          },
        ]
      : [],
  );
}

function ownershipSemanticErrors(
  fixture: FixtureName,
  value: unknown,
): ErrorObject[] {
  const errors: ErrorObject[] = [];
  for (const { path, property } of propertyRecords(fixture, value)) {
    if (!isRecord(property.ownership)) continue;
    const evidenceIds = new Set(
      Array.isArray(property.evidence)
        ? property.evidence.flatMap((evidence) =>
            isRecord(evidence) && typeof evidence.evidenceId === "string"
              ? [evidence.evidenceId]
              : [],
          )
        : [],
    );
    const visit = (entry: unknown, entryPath: string): void => {
      if (Array.isArray(entry)) {
        entry.forEach((child, index) => visit(child, `${entryPath}/${index}`));
        return;
      }
      if (!isRecord(entry)) return;
      if (Array.isArray(entry.evidenceRefs)) {
        for (const reference of entry.evidenceRefs) {
          if (typeof reference === "string" && !evidenceIds.has(reference)) {
            errors.push(
              semanticError(`${entryPath}/evidenceRefs`, "evidenceReference"),
            );
            break;
          }
        }
      }
      Object.entries(entry).forEach(([key, child]) =>
        visit(child, `${entryPath}/${key}`),
      );
    };
    visit(property.ownership, `${path}/ownership`);

    const situs = isRecord(property.address)
      ? property.address.value
      : undefined;
    const mailing = isRecord(property.ownership.publicMailingAddress)
      ? property.ownership.publicMailingAddress
      : undefined;
    if (
      typeof situs !== "string" ||
      mailing?.availability !== "available" ||
      !isRecord(mailing.value)
    ) {
      continue;
    }
    const mailingValue = mailing.value;
    const componentValue = (name: string): unknown => {
      const fact = mailingValue[name];
      return isRecord(fact) && fact.availability === "available"
        ? fact.value
        : undefined;
    };
    const addressLines = componentValue("addressLines");
    const lineText = Array.isArray(addressLines)
      ? addressLines
          .filter((line): line is string => typeof line === "string")
          .join(" ")
      : "";
    const fullMailing = [
      lineText,
      componentValue("locality"),
      componentValue("region"),
      componentValue("postalCode"),
      componentValue("country"),
    ]
      .filter((component): component is string => typeof component === "string")
      .join(" ");
    const normalizedSitus = normalizeAddress(situs);
    if (
      normalizedSitus.length > 0 &&
      [lineText, fullMailing].some(
        (candidate) => normalizeAddress(candidate) === normalizedSitus,
      )
    ) {
      errors.push(
        semanticError(
          `${path}/ownership/publicMailingAddress`,
          "situsMailingDistinct",
        ),
      );
    }
  }
  return errors;
}

export async function sha256File(filePath: string): Promise<string> {
  return createHash("sha256")
    .update(await readFile(filePath))
    .digest("hex");
}

export class FrozenContractValidator {
  readonly #root: string;
  readonly #validators = new Map<FixtureName, ValidateFunction>();

  private constructor(root: string) {
    this.#root = root;
  }

  static async create(root = process.cwd()): Promise<FrozenContractValidator> {
    const validator = new FrozenContractValidator(path.resolve(root));
    const schemaPath = path.join(
      validator.#root,
      "contracts/mcp-v1.schema.json",
    );
    const schema = await readJson<McpSchema>(schemaPath);
    const ajv = new Ajv2020({
      allErrors: true,
      allowUnionTypes: true,
      strict: false,
      validateFormats: true,
    });
    addFormats(ajv);
    ajv.addSchema(schema);

    for (const [fixture, definition] of Object.entries(
      FIXTURE_DEFINITIONS,
    ) as Array<[FixtureName, string]>) {
      validator.#validators.set(
        fixture,
        ajv.compile({ $ref: `${schema.$id}#/$defs/${definition}` }),
      );
    }
    return validator;
  }

  async loadFixture(fixture: FixtureName): Promise<unknown> {
    return readJson(path.join(this.#root, "contracts/fixtures", fixture));
  }

  validateFixture(
    fixture: FixtureName,
    value: unknown,
  ): ValidationFailure | undefined {
    const validate = this.#validators.get(fixture);
    if (!validate) {
      throw new Error(`No validator compiled for ${fixture}`);
    }
    const schemaValid = validate(value);
    const semanticErrors = schemaValid
      ? ownershipSemanticErrors(fixture, value)
      : [];
    if (schemaValid && semanticErrors.length === 0) {
      return undefined;
    }
    return {
      errors: [...structuredClone(validate.errors ?? []), ...semanticErrors],
      fixture,
    };
  }

  async validateAll(): Promise<ContractValidationResult> {
    const fixtureFailures: ValidationFailure[] = [];
    for (const fixture of Object.keys(FIXTURE_DEFINITIONS) as FixtureName[]) {
      const failure = this.validateFixture(
        fixture,
        await this.loadFixture(fixture),
      );
      if (failure) fixtureFailures.push(failure);
    }

    return {
      fixtureCount: Object.keys(FIXTURE_DEFINITIONS).length,
      fixtureFailures,
      hashFailures: await this.verifyLockedHashes(),
    };
  }

  async verifyLockedHashes(): Promise<
    ContractValidationResult["hashFailures"]
  > {
    const lock = await readJson<ContractLock>(
      path.join(this.#root, "contracts/contract-lock.json"),
    );
    const lockedFiles = [
      lock.mcpSchema,
      lock.canonicalSchema,
      ...lock.sharedFixtures,
    ];
    const failures: ContractValidationResult["hashFailures"] = [];

    for (const locked of lockedFiles) {
      const actual = await sha256File(path.join(this.#root, locked.path));
      if (actual !== locked.sha256) {
        failures.push({ actual, expected: locked.sha256, path: locked.path });
      }
    }
    return failures;
  }
}
