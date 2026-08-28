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
    if (validate(value)) {
      return undefined;
    }
    return {
      errors: structuredClone(validate.errors ?? []),
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
