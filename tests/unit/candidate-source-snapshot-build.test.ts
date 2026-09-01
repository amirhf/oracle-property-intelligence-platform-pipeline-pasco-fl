import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildCandidateSourceSnapshotDemo,
  CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
  CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
  type CandidateSourceSnapshotBuildDescriptor,
} from "../../src/publication/candidate-source-snapshot-build.js";
import { CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE } from "../../src/publication/candidate-source-snapshot-demo.js";
import { CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS } from "../../src/publication/candidate-source-snapshot-preflight-binding.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const roots: string[] = [];
const alternateCid = "QmVwpAV8hWUr3zsJZijhzUAArgSMhkV1vzmtJaWFMUQ4pj";

afterEach(async () => {
  await Promise.all(
    roots.splice(0).map((root) => rm(root, { recursive: true })),
  );
});

function frozenDescriptor(
  root: string,
): CandidateSourceSnapshotBuildDescriptor {
  const fixture = syntheticCandidateSourceSnapshotDemo();
  return {
    compactManifest: structuredClone(
      CANDIDATE_SOURCE_SNAPSHOT_BOUND_COMPACT_MANIFEST,
    ),
    controlOutputRoot: path.join(root, "controls"),
    planArtifactOutputRoot: path.join(root, "plan-artifact"),
    preflight: fixture.plan.preflight,
    source: { ...CANDIDATE_SOURCE_SNAPSHOT_BOUND_SOURCE },
    sourceManifestFileSha256:
      CANDIDATE_SOURCE_SNAPSHOT_SOURCE_MANIFEST_FILE_SHA256,
    sourceManifestPath: path.join(root, "missing-source-manifest.json"),
    sourcePlanFileSha256: CANDIDATE_SOURCE_SNAPSHOT_SOURCE_PLAN_FILE_SHA256,
    sourcePlanPath: path.join(root, "missing-source-plan.json"),
    targets: {
      openData: { ...CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData },
      queryTable: { ...CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable },
    },
    version: "1.0.0",
  };
}

describe("candidate source-snapshot descriptor build", () => {
  it.each([
    {
      label: "source plan file hash",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) => ({
        ...descriptor,
        sourcePlanFileSha256: "0".repeat(64),
      }),
      expected: "source files are not the frozen publication",
    },
    {
      label: "source manifest file hash",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) => ({
        ...descriptor,
        sourceManifestFileSha256: "0".repeat(64),
      }),
      expected: "source files are not the frozen publication",
    },
    {
      label: "source identity tuple",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) =>
        ({
          ...descriptor,
          source: {
            ...descriptor.source,
            authorityId: `authority_${"f".repeat(32)}`,
          },
        }) as unknown as CandidateSourceSnapshotBuildDescriptor,
      expected: "not the reviewed source/target binding",
    },
    {
      label: "compact manifest identity",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) => ({
        ...descriptor,
        compactManifest: {
          ...descriptor.compactManifest,
          graph: {
            ...descriptor.compactManifest.graph,
            propertyCount: descriptor.compactManifest.graph.propertyCount - 1,
          },
        },
      }),
      expected: "not the reviewed source/target binding",
    },
    {
      label: "open-data target root",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) => ({
        ...descriptor,
        targets: {
          ...descriptor.targets,
          openData: {
            ...descriptor.targets.openData,
            targetCid: alternateCid,
          },
        },
      }),
      expected: "not the reviewed source/target binding",
    },
    {
      label: "query-table target root",
      mutate: (descriptor: CandidateSourceSnapshotBuildDescriptor) => ({
        ...descriptor,
        targets: {
          ...descriptor.targets,
          queryTable: {
            ...descriptor.targets.queryTable,
            targetCid: alternateCid,
          },
        },
      }),
      expected: "not the reviewed source/target binding",
    },
  ])("rejects arbitrary $label before materialization", async (testCase) => {
    const root = await mkdtemp(path.join(tmpdir(), "snapshot-demo-gate-"));
    roots.push(root);
    const descriptor = testCase.mutate(frozenDescriptor(root));
    await expect(
      buildCandidateSourceSnapshotDemo({ descriptor, record: false }),
    ).rejects.toThrow(testCase.expected);
    await expect(readdir(root)).resolves.toEqual([]);
  });
});
