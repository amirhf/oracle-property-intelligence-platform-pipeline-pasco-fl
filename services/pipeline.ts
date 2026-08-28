import * as restate from "@restatedev/restate-sdk";

import type { PilotRunRequest, PreparedPilot } from "../src/domain/types.js";
import {
  loadPreparedPilot,
  markRunFailed,
  recordRunStarted,
} from "../src/db/pilot-repository.js";
import { preparePilot } from "../src/pilot/prepare.js";
import { SourceAccessStopError } from "../src/lib/access-stop.js";

interface PipelineDependencies {
  dataDir: string;
  databaseUrl: string;
}

export function createPipelineServices(dependencies: PipelineDependencies) {
  const ingestChunk = restate.workflow({
    name: "IngestChunk",
    handlers: {
      run: async (
        ctx: restate.WorkflowContext,
        request: PilotRunRequest,
      ): Promise<PreparedPilot> =>
        ctx.run("raw-first-pilot-preparation", () =>
          preparePilot({
            asOf: request.asOf,
            dataDir: dependencies.dataDir,
            runId: request.runId,
            sampleSeed: request.sampleSeed,
          }),
        ),
    },
    options: {
      asTerminalError: (error) =>
        error instanceof SourceAccessStopError
          ? new restate.TerminalError(error.message, { errorCode: 503 })
          : undefined,
    },
  });

  const loader = restate.object({
    name: "Loader",
    handlers: {
      load: async (
        ctx: restate.ObjectContext,
        input: { prepared: PreparedPilot; request: PilotRunRequest },
      ) =>
        ctx.run("postgres-single-writer-load", () =>
          loadPreparedPilot(
            dependencies.databaseUrl,
            input.request,
            input.prepared,
          ),
        ),
    },
  });

  const countyIngest = restate.workflow({
    name: "CountyIngest",
    handlers: {
      run: async (ctx: restate.WorkflowContext, request: PilotRunRequest) => {
        if (request.workflowId !== ctx.key) {
          throw new restate.TerminalError(
            "workflowId must equal the Restate workflow key",
            { errorCode: 400 },
          );
        }
        await ctx.run("record-pilot-run-start", () =>
          recordRunStarted(dependencies.databaseUrl, request),
        );
        try {
          const prepared = await ctx
            .workflowClient(ingestChunk, `${ctx.key}-chunk-0001`)
            .run(request);
          return await ctx
            .objectClient(loader, "pasco")
            .load({ prepared, request });
        } catch (error) {
          const message =
            error instanceof Error ? error.message : "unknown pipeline failure";
          await ctx.run("record-pilot-run-failure", () =>
            markRunFailed(
              dependencies.databaseUrl,
              request.runId,
              message.slice(0, 160),
            ),
          );
          throw new restate.TerminalError(message, { errorCode: 503 });
        }
      },
    },
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
      status: async (
        _ctx: restate.ObjectContext,
        _request: Record<string, never>,
      ) => ({ enabled: false, reason: "publication_not_authorized" }),
    },
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
