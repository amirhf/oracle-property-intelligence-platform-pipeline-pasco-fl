import { z } from "zod";

import { DurableInputError } from "../lib/durability-errors.js";
import {
  publicationApprovalRequestSchema,
  publicationExecutionRequestSchema,
} from "../db/publication-durability.js";

const runIdSchema = z.string().regex(/^run_[a-f0-9]{32}$/);

export const preparePublicationRequestSchema = z.strictObject({
  county: z.literal("pasco"),
  exportMode: z.enum(["bounded", "authoritative"]),
  runId: runIdSchema,
});

export const publicationStatusRequestSchema = z.strictObject({});

function parse<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new DurableInputError(
      `${label} failed strict validation at ${issue?.path.join(".") || "root"}`,
    );
  }
  return parsed.data;
}

export const parsePreparePublicationRequest = (value: unknown) =>
  parse(
    preparePublicationRequestSchema,
    value,
    "Publish/pasco prepare request",
  );

export const parsePublicationApprovalRequest = (value: unknown) =>
  parse(
    publicationApprovalRequestSchema,
    value,
    "Publish/pasco approval request",
  );

export const parsePublicationExecutionRequest = (value: unknown) =>
  parse(
    publicationExecutionRequestSchema,
    value,
    "Publish/pasco execution request",
  );

export const parsePublicationStatusRequest = (value: unknown) =>
  parse(publicationStatusRequestSchema, value, "Publish/pasco status request");
