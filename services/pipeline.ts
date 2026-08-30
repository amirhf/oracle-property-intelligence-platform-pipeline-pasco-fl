import * as restate from "@restatedev/restate-sdk";

import {
  approvePublicationPlan,
  getPublicationState,
} from "../src/db/publication-durability.js";
import {
  loadPreparedPilot,
  markRunFailed,
  recordRunStarted,
} from "../src/db/pilot-repository.js";
import { loadPreparedScale } from "../src/db/scale-repository.js";
import { SourceAccessStopError } from "../src/lib/access-stop.js";
import {
  isDurabilityTerminalError,
  DurableInputError,
} from "../src/lib/durability-errors.js";
import { deterministicId } from "../src/lib/hash.js";
import { buildPublicationDryRun } from "../src/publication/dry-run.js";
import {
  parsePreparePublicationRequest,
  parsePublicationApprovalRequest,
  parsePublicationStatusRequest,
} from "../src/publication/requests.js";
import { preparePilot } from "../src/pilot/prepare.js";
import { prepareAuthoritativePasco } from "../src/authoritative/prepare.js";
import { prepareScaleDataset } from "../src/scale/prepare.js";
import { verifyPreparedInput } from "../src/snapshot/model.js";
import {
  countyIngestRequestSha256,
  parseCountyIngestRequest,
  parseIngestChunkRequest,
  parseLoaderRequest,
  parsePreparedPilot,
} from "../src/workflow/schemas.js";

interface PipelineDependencies {
  dataDir: string;
  databaseUrl: string;
}

interface InFlightLoaderOperation {
  binding: string;
  promise: Promise<unknown>;
}

export interface LoaderOperationSingleFlight {
  run<T>(
    idempotencyKey: string,
    binding: string,
    operation: () => Promise<T>,
  ): Promise<T>;
}

/**
 * Restate can replay an invocation after its HTTP/2 stream is aborted while a
 * non-cancellable local database operation is still finishing. Share that
 * exact operation inside one service process so replay handlers do not each
 * parse and retain the same large prepared input. PostgreSQL remains the
 * durable authority and the advisory lock still serializes across processes.
 */
export function createLoaderOperationSingleFlight(): LoaderOperationSingleFlight {
  const inFlight = new Map<string, InFlightLoaderOperation>();

  return {
    async run<T>(
      idempotencyKey: string,
      binding: string,
      operation: () => Promise<T>,
    ): Promise<T> {
      const existing = inFlight.get(idempotencyKey);
      if (existing) {
        if (existing.binding !== binding) {
          throw new DurableInputError(
            "Concurrent Loader replay does not match the in-flight input",
          );
        }
        return existing.promise as Promise<T>;
      }

      const promise = operation();
      inFlight.set(idempotencyKey, { binding, promise });
      try {
        return await promise;
      } finally {
        const current = inFlight.get(idempotencyKey);
        if (current?.promise === promise) inFlight.delete(idempotencyKey);
      }
    },
  };
}

function terminalError(error: unknown): restate.TerminalError | undefined {
  if (isDurabilityTerminalError(error)) {
    return new restate.TerminalError(error.message, {
      errorCode: error.errorCode,
    });
  }
  if (error instanceof SourceAccessStopError) {
    return new restate.TerminalError(error.message, { errorCode: 503 });
  }
  return undefined;
}

export function createPipelineServices(dependencies: PipelineDependencies) {
  const loaderSingleFlight = createLoaderOperationSingleFlight();
  const ingestChunk = restate.workflow({
    name: "IngestChunk",
    handlers: {
      run: async (ctx: restate.WorkflowContext, input: unknown) => {
        const request = parseIngestChunkRequest(input);
        if (ctx.key !== `${request.workflowId}-chunk-0001`) {
          throw new DurableInputError(
            "IngestChunk workflow key does not match the parent workflow",
          );
        }
        const {
          chunkCount: _chunkCount,
          chunkIndex: _chunkIndex,
          endExclusive: _endExclusive,
          parentRequestSha256,
          startIndex: _startIndex,
          ...countyRequestValue
        } = request;
        const countyRequest = parseCountyIngestRequest(countyRequestValue);
        if (countyIngestRequestSha256(countyRequest) !== parentRequestSha256) {
          throw new DurableInputError(
            "IngestChunk parent request hash does not match its payload",
          );
        }
        const prepared = await ctx.run(
          request.selectionSize === 25
            ? "hash-bound-pilot-preparation"
            : request.selectionSize === 325_213
              ? "hash-bound-authoritative-preparation"
              : "hash-bound-scale-preparation",
          async () =>
            request.selectionSize === 25
              ? preparePilot({
                  asOf: request.asOf,
                  dataDir: dependencies.dataDir,
                  runId: request.runId,
                  sampleSeed: request.sampleSeed,
                })
              : request.selectionSize === 325_213
                ? prepareAuthoritativePasco({
                    asOf: request.asOf,
                    dataDir: dependencies.dataDir,
                    runId: request.runId,
                  })
                : prepareScaleDataset({
                    asOf: request.asOf,
                    dataDir: dependencies.dataDir,
                    runId: request.runId,
                    sampleSeed: request.sampleSeed,
                    selectionSize: request.selectionSize,
                  }),
        );
        if (
          request.expectedSnapshotId !== undefined &&
          prepared.snapshotId !== request.expectedSnapshotId
        ) {
          throw new DurableInputError(
            `Prepared snapshot does not match expected identity (expected=${request.expectedSnapshotId}, actual=${prepared.snapshotId})`,
          );
        }
        return prepared;
      },
    },
    options: { asTerminalError: terminalError },
  });

  const loader = restate.object({
    name: "Loader",
    handlers: {
      load: async (ctx: restate.ObjectContext, input: unknown) => {
        const request = parseLoaderRequest(input);
        if (ctx.key !== "pasco") {
          throw new DurableInputError(
            "Loader must be invoked with the Pasco county key",
          );
        }
        const requestSha256 = countyIngestRequestSha256(request.request);
        if (requestSha256 !== request.parentRequestSha256) {
          throw new DurableInputError(
            "Loader parent request hash does not match its payload",
          );
        }
        if (
          request.request.expectedSnapshotId !== undefined &&
          request.prepared.snapshotId !== request.request.expectedSnapshotId
        ) {
          throw new DurableInputError(
            "Loader prepared snapshot does not match the requested snapshot",
          );
        }
        return ctx.run("verify-and-apply-hash-bound-input", () =>
          loaderSingleFlight.run(
            request.idempotencyKey,
            [
              requestSha256,
              request.prepared.preparedInputId,
              request.prepared.snapshotId,
              request.prepared.manifest.sha256,
            ].join(":"),
            async () => {
              const verified = await verifyPreparedInput(
                dependencies.dataDir,
                request.prepared,
                parsePreparedPilot,
                request.prepared.snapshotId,
              );
              if (
                verified.prepared.sampleAlgorithm !==
                  request.request.sampleAlgorithm ||
                verified.prepared.sampleSeed !== request.request.sampleSeed ||
                verified.prepared.selectionSize !==
                  request.request.selectionSize
              ) {
                throw new DurableInputError(
                  "Prepared sampling metadata does not match the Loader request",
                );
              }
              const durability = {
                idempotencyKey: request.idempotencyKey,
                preparedManifest: verified.manifest,
                preparedReference: verified.reference,
                requestSha256,
                runId: request.request.runId,
                snapshot: verified.snapshot,
              };
              return verified.snapshot.manifestVersion === "1.2.0" ||
                verified.reference.kind === "pilot"
                ? loadPreparedPilot(
                    dependencies.databaseUrl,
                    request.request,
                    verified.prepared,
                    durability,
                  )
                : loadPreparedScale(
                    dependencies.databaseUrl,
                    request.request,
                    verified.prepared,
                    durability,
                  );
            },
          ),
        );
      },
    },
    options: { asTerminalError: terminalError },
  });

  const countyIngest = restate.workflow({
    name: "CountyIngest",
    handlers: {
      run: async (ctx: restate.WorkflowContext, input: unknown) => {
        const request = parseCountyIngestRequest(input);
        if (request.workflowId !== ctx.key) {
          throw new DurableInputError(
            "workflowId must equal the Restate workflow key",
          );
        }
        await ctx.run("record-pipeline-run-start", () =>
          recordRunStarted(dependencies.databaseUrl, request),
        );
        try {
          const parentRequestSha256 = countyIngestRequestSha256(request);
          const prepared = await ctx
            .workflowClient(ingestChunk, `${ctx.key}-chunk-0001`)
            .run({
              ...request,
              chunkCount: 1,
              chunkIndex: 0,
              endExclusive: request.selectionSize,
              parentRequestSha256,
              startIndex: 0,
            });
          const idempotencyKey = deterministicId("load", [
            "1.0.0",
            "Loader/pasco",
            request.workflowId,
            prepared.preparedInputId,
          ]);
          return await ctx.objectClient(loader, "pasco").load({
            county: "pasco",
            idempotencyKey,
            parentRequestSha256,
            prepared,
            request,
          });
        } catch (error) {
          if (
            !isDurabilityTerminalError(error) &&
            !(error instanceof SourceAccessStopError) &&
            !(error instanceof restate.TerminalError)
          ) {
            throw error;
          }
          const message = error.message;
          await ctx.run("record-pipeline-run-failure", () =>
            markRunFailed(
              dependencies.databaseUrl,
              request.runId,
              message.slice(0, 160),
            ),
          );
          if (error instanceof restate.TerminalError) throw error;
          const converted = terminalError(error);
          if (!converted) throw error;
          throw converted;
        }
      },
    },
    options: { asTerminalError: terminalError },
  });

  const permitFeedChunk = restate.workflow({
    name: "PermitFeedChunk",
    handlers: {
      run: async (
        _ctx: restate.WorkflowContext,
        request: { runId: string },
      ) => ({
        enabled: false,
        mode: "source_unavailable_after_challenge",
        runId: request.runId,
      }),
    },
  });

  const permitFeed = restate.workflow({
    name: "PermitFeed",
    handlers: {
      run: async (ctx: restate.WorkflowContext, request: { runId: string }) =>
        ctx
          .workflowClient(permitFeedChunk, `${ctx.key}-chunk-0001`)
          .run(request),
    },
  });

  const statusService = (
    name: "BbbHarvest" | "PermitHarvest" | "SunbizIngest",
  ) =>
    restate.service({
      name,
      handlers: {
        status: async (
          _ctx: restate.Context,
          _request: Record<string, never>,
        ) => ({
          enabled: false,
          mode:
            name === "PermitHarvest"
              ? "source_unavailable_after_challenge"
              : "not-collected",
        }),
      },
    });

  const publish = restate.object({
    name: "Publish",
    handlers: {
      approve: async (ctx: restate.ObjectContext, input: unknown) => {
        if (ctx.key !== "pasco") {
          throw new DurableInputError(
            "Publish must be invoked with the Pasco county key",
          );
        }
        const request = parsePublicationApprovalRequest(input);
        return ctx.run("approve-exact-publication-plan", () =>
          approvePublicationPlan(dependencies.databaseUrl, request),
        );
      },
      prepare: async (ctx: restate.ObjectContext, input: unknown) => {
        if (ctx.key !== "pasco") {
          throw new DurableInputError(
            "Publish must be invoked with the Pasco county key",
          );
        }
        const request = parsePreparePublicationRequest(input);
        return ctx.run("prepare-lifecycle-aware-publication-plan", () =>
          buildPublicationDryRun({
            dataDir: dependencies.dataDir,
            databaseUrl: dependencies.databaseUrl,
            exportMode: request.exportMode,
            runId: request.runId,
          }),
        );
      },
      status: async (ctx: restate.ObjectContext, input: unknown) => {
        if (ctx.key !== "pasco") {
          throw new DurableInputError(
            "Publish must be invoked with the Pasco county key",
          );
        }
        parsePublicationStatusRequest(input);
        return ctx.run("read-publication-state", () =>
          getPublicationState(dependencies.databaseUrl),
        );
      },
    },
    options: { asTerminalError: terminalError },
  });

  return {
    countyIngest,
    ingestChunk,
    loader,
    permitFeed,
    permitFeedChunk,
    permitHarvest: statusService("PermitHarvest"),
    publish,
    sunbizIngest: statusService("SunbizIngest"),
    bbbHarvest: statusService("BbbHarvest"),
  };
}
