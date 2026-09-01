export class DurableInputError extends Error {
  readonly errorCode = 400;
}

export class DurableConflictError extends Error {
  readonly errorCode = 409;
}

export function isDurabilityTerminalError(
  error: unknown,
): error is DurableInputError | DurableConflictError {
  return (
    error instanceof DurableInputError || error instanceof DurableConflictError
  );
}
