import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  candidateSourceSnapshotAuthorizationBindingSchema,
  createCandidateSourceSnapshotApprovalIdentity,
  parseCandidateSourceSnapshotAuthorizationStatement,
  renderCandidateSourceSnapshotAuthorizationBindingStatement,
  renderCandidateSourceSnapshotAuthorizationStatement,
} from "../../src/db/candidate-source-snapshot-approval.js";
import { syntheticCandidateSourceSnapshotDemo } from "../helpers/candidate-source-snapshot-demo.js";

const controllingAuthorizationStatement =
  "I confirm the candidate-controlled Filebase account is Pro or better and supports at least 350000 pinned objects, 4294967296 bytes, two distinct buckets and two distinct IPNS names, and I approve only candidate_owned_source_snapshot_demo plan snapshotdemo_8bb5cbd74f4b4816e8b4fe54365f48e6 with logical SHA-256 b90b64a9f31d672c9deea2d6f3131c8fc627cda2a89c62de53807c63e01a71f5, plan artifact SHA-256 1b40ab64c011db5807d7b7c9305634329d6d43e2370e6728caa0d4c3e62551a9 and CID QmYc1YwAYAm9GhGaATPaLAAxZKSnHDGajpeSMYLeRQZfzg, exactly 325312 objects and 3457753084 upload bytes with 3474519090 admission-reserved bytes, open-data bucket and label cand-amir-pasco-open-data-source-snapshot-demo-v1 under immutable prefix publications/source-snapshot-demo-v1/snapshotns_ff11a5f549e4f085ec05186a1f51e701/ and network key k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6 from prior bafybeieqgp5zh4yfibox2jhfbza442o5voit3tk32a6fywwz3ausidwd2q to target QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy, query-table bucket and label cand-amir-pasco-query-table-source-snapshot-demo-v1 under immutable prefix query-tables/source-snapshot-demo-v1/snapshotns_ff11a5f549e4f085ec05186a1f51e701/ and network key k51qzi5uqu5di1wl6zp9v2n9j1p6m3zcli0wy58p5ypkjk7qjv38hufawhn9qu from prior bafybeiatknvltt7jcujznmxf6jgizo5f2nbmhhyvw3ksb7edigjguaqn2q to target QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw, manifest CID QmS5vnqiHLcCFHC6EERBTuffVLwF11RSZmyqTCsYnqBVVq and SHA-256 09e2533e17a8e7ff4b9f3fb4c9b037ed418766cc13e7959bf629380f0706c125, inventory CID QmWgVGagdJzgHHQdYNNY3nMcJzSYwZEtpaaqfiPCQtnzEW and full-inventory SHA-256 1b4f390667ff7cdd82777a34b960790ce97964e90048a5eb4612a7031a776766, successful request count 325320, maximum-attempt count 975960, ambiguous-inspection allowance 23980, recovery allowance 60, absolute request ceiling 1000000, two retries, three total object attempts, concurrency 16, 20000 ms timeout and USD 25 spending ceiling for uploading only these immutable objects and then updating only these two candidate IPNS identities in durable open-data-first/query-table-second order after exact provider-CID verification; this authorization is candidate-only and noncanonical and does not authorize or represent Elephant-owned, owner-controlled, owner/canonical, authoritative-complete, independently Pasco-certified, Accela/BBB, production-database, Vercel-deployment or any other publication authority.";

const controllingAuthorizationBinding =
  candidateSourceSnapshotAuthorizationBindingSchema.parse({
    classification: {
      canonical: false,
      elephantOwned: false,
      independentlyPascoCertified: false,
      ownerControlled: false,
      publicationClass: "candidate_owned_source_snapshot_demo",
      resourceOwner: "candidate",
      sourceScope: "exact_hash_bound_2026_08_23_parcel_snapshot",
    },
    execution: {
      absoluteRequestCeiling: 1_000_000,
      ambiguousInspectionAllowance: 23_980,
      cutoverOrder: ["open_data", "query_table"],
      maximumAttemptCount: 975_960,
      maximumAttemptsPerObject: 3,
      maximumConcurrency: 16,
      maximumRetries: 2,
      recoveryAllowance: 60,
      requestEnvelopeSha256:
        "97a10ef9d77e0ac431c548686e0ab737036ecc4fafff38069a73c0dc2571d868",
      requestTimeoutMs: 20_000,
      spendingCeilingUsd: 25,
      successfulRequestCount: 325_320,
    },
    inventory: {
      admissionReservedBytes: 3_474_519_090,
      costEnvelopeSha256:
        "8f42d05f6c2c2cdf3f0904cb657bab5ac684898373f113d2066cf16a262799df",
      exactObjectCount: 325_312,
      exactTotalBytes: 3_457_753_084,
      fullInventorySha256:
        "1b4f390667ff7cdd82777a34b960790ce97964e90048a5eb4612a7031a776766",
      inventoryCid: "QmWgVGagdJzgHHQdYNNY3nMcJzSYwZEtpaaqfiPCQtnzEW",
      manifestCid: "QmS5vnqiHLcCFHC6EERBTuffVLwF11RSZmyqTCsYnqBVVq",
      manifestSha256:
        "09e2533e17a8e7ff4b9f3fb4c9b037ed418766cc13e7959bf629380f0706c125",
      maximumObjectCount: 350_000,
      maximumTotalBytes: 4_294_967_296,
    },
    plan: {
      artifactByteSize: 11_210,
      artifactCid: "QmYc1YwAYAm9GhGaATPaLAAxZKSnHDGajpeSMYLeRQZfzg",
      artifactRemoteObjectKey:
        "publication-control/source-snapshot-demo-v1/snapshotns_ff11a5f549e4f085ec05186a1f51e701/candidate-source-snapshot-plan.json",
      artifactSha256:
        "1b40ab64c011db5807d7b7c9305634329d6d43e2370e6728caa0d4c3e62551a9",
      planId: "snapshotdemo_8bb5cbd74f4b4816e8b4fe54365f48e6",
      planLogicalSha256:
        "b90b64a9f31d672c9deea2d6f3131c8fc627cda2a89c62de53807c63e01a71f5",
    },
    schemaVersion: "candidate-source-snapshot-authorization-binding-v1",
    targets: {
      openData: {
        bucket: "cand-amir-pasco-open-data-source-snapshot-demo-v1",
        immutablePrefix:
          "publications/source-snapshot-demo-v1/snapshotns_ff11a5f549e4f085ec05186a1f51e701/",
        ipnsLabel: "cand-amir-pasco-open-data-source-snapshot-demo-v1",
        ipnsNetworkKey:
          "k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6",
        priorCid: "bafybeieqgp5zh4yfibox2jhfbza442o5voit3tk32a6fywwz3ausidwd2q",
        targetCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
      },
      queryTable: {
        bucket: "cand-amir-pasco-query-table-source-snapshot-demo-v1",
        immutablePrefix:
          "query-tables/source-snapshot-demo-v1/snapshotns_ff11a5f549e4f085ec05186a1f51e701/",
        ipnsLabel: "cand-amir-pasco-query-table-source-snapshot-demo-v1",
        ipnsNetworkKey:
          "k51qzi5uqu5di1wl6zp9v2n9j1p6m3zcli0wy58p5ypkjk7qjv38hufawhn9qu",
        priorCid: "bafybeiatknvltt7jcujznmxf6jgizo5f2nbmhhyvw3ksb7edigjguaqn2q",
        targetCid: "QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw",
      },
    },
  });

describe("candidate source-snapshot exact human authorization", () => {
  it("matches the byte-exact controlling authorization independently", () => {
    const rendered = renderCandidateSourceSnapshotAuthorizationBindingStatement(
      controllingAuthorizationBinding,
    );

    expect(rendered).toBe(controllingAuthorizationStatement);
    expect(Buffer.byteLength(rendered, "utf8")).toBe(2_424);
    expect(createHash("sha256").update(rendered, "utf8").digest("hex")).toBe(
      "6a54c38546f0167246be0476ca24ca0f5682739ec59091df44ce5a2f496d3761",
    );
  });

  it("rejects a bucket and IPNS label mismatch before rendering", () => {
    expect(() =>
      renderCandidateSourceSnapshotAuthorizationBindingStatement({
        ...controllingAuthorizationBinding,
        targets: {
          ...controllingAuthorizationBinding.targets,
          openData: {
            ...controllingAuthorizationBinding.targets.openData,
            ipnsLabel: "candidate-mismatched-label",
          },
        },
      }),
    ).toThrow("identical bucket and IPNS label");
  });

  it("accepts only the byte-exact statement rendered from the immutable plan", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const statement = renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
    );
    const parsed = parseCandidateSourceSnapshotAuthorizationStatement({
      exactUpload: fixture.exactUpload,
      plan: fixture.plan,
      statement,
    });

    expect(parsed.authorizationStatement).toBe(statement);
    expect(parsed.authorizationStatementSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(parsed.authorizationBinding).toMatchObject({
      classification: {
        canonical: false,
        publicationClass: "candidate_owned_source_snapshot_demo",
        resourceOwner: "candidate",
      },
      inventory: {
        exactObjectCount: fixture.exactUpload.exactObjectCount,
        exactTotalBytes: fixture.exactUpload.exactTotalBytes,
      },
      plan: {
        planId: fixture.plan.planId,
        planLogicalSha256: fixture.plan.planSha256,
      },
      targets: {
        openData: {
          bucket: fixture.plan.targets.openData.bucket,
          targetCid: fixture.plan.targets.openData.targetCid,
        },
        queryTable: {
          bucket: fixture.plan.targets.queryTable.bucket,
          targetCid: fixture.plan.targets.queryTable.targetCid,
        },
      },
    });
    expect(() =>
      parseCandidateSourceSnapshotAuthorizationStatement({
        exactUpload: fixture.exactUpload,
        plan: fixture.plan,
        statement: `${statement} `,
      }),
    ).toThrow("does not exactly match");
    expect(() =>
      parseCandidateSourceSnapshotAuthorizationStatement({
        exactUpload: fixture.exactUpload,
        plan: fixture.plan,
        statement: statement.replace("USD 25", "USD 26"),
      }),
    ).toThrow("does not exactly match");
  });

  it("derives stable approval identity and changes it for any approval input", () => {
    const fixture = syntheticCandidateSourceSnapshotDemo();
    const statement = renderCandidateSourceSnapshotAuthorizationStatement(
      fixture.plan,
      fixture.exactUpload,
    );
    const input = {
      approvedAt: "2026-08-31T01:02:03.000Z",
      approverReference: "synthetic-human-approver",
      exactUpload: fixture.exactUpload,
      plan: fixture.plan,
      statement,
    };
    const first = createCandidateSourceSnapshotApprovalIdentity(input);
    const replay = createCandidateSourceSnapshotApprovalIdentity(input);

    expect(replay).toEqual(first);
    expect(first.approvalId).toMatch(/^snapshotdemoapproval_[a-f0-9]{32}$/);
    expect(first.approvalSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(
      createCandidateSourceSnapshotApprovalIdentity({
        ...input,
        approvedAt: "2026-08-31T01:02:04.000Z",
      }),
    ).not.toMatchObject({
      approvalId: first.approvalId,
      approvalSha256: first.approvalSha256,
    });
    expect(() =>
      createCandidateSourceSnapshotApprovalIdentity({
        ...input,
        approvedAt: "2026-08-31T01:02:03+00:00",
      }),
    ).toThrow("canonical UTC");
  });
});
