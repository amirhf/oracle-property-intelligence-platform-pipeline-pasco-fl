import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import type {
  Ajv2020 as Ajv2020Class,
  ErrorObject,
  ValidateFunction,
} from "ajv/dist/2020.js";
import type { FormatsPlugin } from "ajv-formats";

import {
  MCP_SCHEMA_SHA256,
  MCP_TOOL_DEFINITIONS,
  MCP_TOOL_NAMES,
  type McpToolName,
} from "./constants.js";

const require = createRequire(import.meta.url);
const Ajv2020 = require("ajv/dist/2020.js") as typeof Ajv2020Class;
const addFormats = require("ajv-formats") as FormatsPlugin;

interface JsonSchema {
  $defs: Record<string, Record<string, unknown>>;
  $id: string;
  [key: string]: unknown;
}

function schemaPath(): string {
  return fileURLToPath(
    new URL("../../contracts/mcp-v1.schema.json", import.meta.url),
  );
}

function canonicalSchemaPath(): string {
  return fileURLToPath(
    new URL("../../contracts/canonical-v1.schema.json", import.meta.url),
  );
}

function collectReferences(value: unknown, references: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectReferences(entry, references);
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    if (key === "$ref" && typeof entry === "string") {
      const match = entry.match(/^#\/\$defs\/(.+)$/);
      if (match?.[1]) references.add(match[1]);
    } else {
      collectReferences(entry, references);
    }
  }
}

function dereferenceAlias(
  definitions: JsonSchema["$defs"],
  name: string,
): Record<string, unknown> {
  const definition = definitions[name];
  if (!definition) throw new Error(`Frozen MCP definition ${name} is missing`);
  const keys = Object.keys(definition);
  if (keys.length === 1 && typeof definition.$ref === "string") {
    const match = definition.$ref.match(/^#\/\$defs\/(.+)$/);
    if (match?.[1]) return dereferenceAlias(definitions, match[1]);
  }
  return structuredClone(definition);
}

function bundledDefinition(
  schema: JsonSchema,
  name: string,
): Record<string, unknown> {
  const root = dereferenceAlias(schema.$defs, name);
  const pending = new Set<string>();
  collectReferences(root, pending);
  const bundled: Record<string, Record<string, unknown>> = {};
  while (pending.size > 0) {
    const current = pending.values().next().value as string;
    pending.delete(current);
    if (bundled[current]) continue;
    const definition = schema.$defs[current];
    if (!definition) {
      throw new Error(`Frozen MCP reference ${current} is missing`);
    }
    bundled[current] = structuredClone(definition);
    const nested = new Set<string>();
    collectReferences(definition, nested);
    for (const reference of nested) {
      if (!bundled[reference]) pending.add(reference);
    }
  }
  return Object.keys(bundled).length > 0 ? { ...root, $defs: bundled } : root;
}

export interface ContractFailure {
  instancePath: string;
  keyword: string;
}

export class McpContractRegistry {
  readonly #canonicalValidator: ValidateFunction;
  readonly #errorValidator: ValidateFunction;
  readonly #inputValidators = new Map<McpToolName, ValidateFunction>();
  readonly #outputValidators = new Map<McpToolName, ValidateFunction>();
  readonly #schema: JsonSchema;

  private constructor(
    schema: JsonSchema,
    canonicalValidator: ValidateFunction,
    errorValidator: ValidateFunction,
    validators: Array<{
      input: ValidateFunction;
      output: ValidateFunction;
      tool: McpToolName;
    }>,
  ) {
    this.#schema = schema;
    this.#canonicalValidator = canonicalValidator;
    this.#errorValidator = errorValidator;
    for (const entry of validators) {
      this.#inputValidators.set(entry.tool, entry.input);
      this.#outputValidators.set(entry.tool, entry.output);
    }
  }

  static async create(): Promise<McpContractRegistry> {
    const bytes = await readFile(schemaPath());
    const actualHash = createHash("sha256").update(bytes).digest("hex");
    if (actualHash !== MCP_SCHEMA_SHA256) {
      throw new Error(
        "The active MCP contract hash does not match the frozen lock",
      );
    }
    const schema = JSON.parse(bytes.toString("utf8")) as JsonSchema;
    const ajv = new Ajv2020({
      allErrors: true,
      allowUnionTypes: true,
      strict: false,
      validateFormats: true,
    });
    addFormats(ajv);
    ajv.addSchema(schema);
    const validators = MCP_TOOL_NAMES.map((tool) => {
      const definition = MCP_TOOL_DEFINITIONS[tool];
      return {
        input: ajv.compile({
          $ref: `${schema.$id}#/$defs/${definition.input}`,
        }),
        output: ajv.compile({
          $ref: `${schema.$id}#/$defs/${definition.output}`,
        }),
        tool,
      };
    });
    const errorValidator = ajv.compile({
      $ref: `${schema.$id}#/$defs/ErrorResponseFixture/properties/result`,
    });

    const canonicalSchema = JSON.parse(
      await readFile(canonicalSchemaPath(), "utf8"),
    ) as { $id: string };
    const canonicalAjv = new Ajv2020({
      allErrors: true,
      allowUnionTypes: true,
      strict: false,
      validateFormats: true,
    });
    addFormats(canonicalAjv);
    canonicalAjv.addSchema(canonicalSchema);
    const canonicalValidator = canonicalAjv.compile({
      $ref: `${canonicalSchema.$id}#/$defs/CanonicalProperty`,
    });
    return new McpContractRegistry(
      schema,
      canonicalValidator,
      errorValidator,
      validators,
    );
  }

  inputSchema(tool: McpToolName): Record<string, unknown> {
    return bundledDefinition(this.#schema, MCP_TOOL_DEFINITIONS[tool].input);
  }

  outputSchema(tool: McpToolName): Record<string, unknown> {
    return bundledDefinition(this.#schema, MCP_TOOL_DEFINITIONS[tool].output);
  }

  validateCanonical(value: unknown): ContractFailure[] {
    return this.#validate(this.#canonicalValidator, value);
  }

  validateError(value: unknown): ContractFailure[] {
    return this.#validate(this.#errorValidator, value);
  }

  validateInput(tool: McpToolName, value: unknown): ContractFailure[] {
    const validator = this.#inputValidators.get(tool);
    if (!validator) throw new Error(`No input validator for ${tool}`);
    return this.#validate(validator, value);
  }

  validateOutput(tool: McpToolName, value: unknown): ContractFailure[] {
    const validator = this.#outputValidators.get(tool);
    if (!validator) throw new Error(`No output validator for ${tool}`);
    return this.#validate(validator, value);
  }

  #validate(validator: ValidateFunction, value: unknown): ContractFailure[] {
    if (validator(value)) return [];
    return (validator.errors ?? []).map((error: ErrorObject) => ({
      instancePath: error.instancePath,
      keyword: error.keyword,
    }));
  }
}
