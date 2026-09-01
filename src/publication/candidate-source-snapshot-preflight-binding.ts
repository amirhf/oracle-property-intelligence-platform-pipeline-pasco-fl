import { z } from "zod";

const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
const cidSchema = z.union([
  z.string().regex(/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/),
  z.string().regex(/^b[a-z2-7]{20,120}$/),
]);
const byteCountSchema = z
  .number()
  .int()
  .nonnegative()
  .max(Number.MAX_SAFE_INTEGER);

export const CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS = Object.freeze({
  openData: "bafybeieqgp5zh4yfibox2jhfbza442o5voit3tk32a6fywwz3ausidwd2q",
  queryTable: "bafybeiatknvltt7jcujznmxf6jgizo5f2nbmhhyvw3ksb7edigjguaqn2q",
});

export const CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS = Object.freeze({
  openData: Object.freeze({
    bucket: "cand-amir-pasco-open-data-source-snapshot-demo-v1",
    ipnsLabel: "cand-amir-pasco-open-data-source-snapshot-demo-v1",
    ipnsNetworkKey:
      "k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6",
    priorCid: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData,
    targetCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
  }),
  queryTable: Object.freeze({
    bucket: "cand-amir-pasco-query-table-source-snapshot-demo-v1",
    ipnsLabel: "cand-amir-pasco-query-table-source-snapshot-demo-v1",
    ipnsNetworkKey:
      "k51qzi5uqu5di1wl6zp9v2n9j1p6m3zcli0wy58p5ypkjk7qjv38hufawhn9qu",
    priorCid: CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable,
    targetCid: "QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw",
  }),
});

const bucketBindingShape = {
  conflictingObjectCount: z.literal(0),
  headStatus: z.literal("authenticated"),
  prefixStatus: z.literal("no_conflicting_publication_prefixes"),
  storageNetworkStatus: z.literal("ipfs_provider_cid_verified"),
};
const capacityBucketShape = { storageBytes: byteCountSchema };
const identityBindingShape = {
  controlCid: cidSchema,
  officialGatewayCid: cidSchema,
  signedRecordCid: cidSchema,
};

export const candidateSourceSnapshotPreflightBindingSchema = z
  .strictObject({
    buckets: z
      .tuple([
        z.strictObject({
          ...bucketBindingShape,
          bucket: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
          ),
          domain: z.literal("open_data"),
        }),
        z.strictObject({
          ...bucketBindingShape,
          bucket: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
          ),
          domain: z.literal("query_table"),
        }),
      ])
      .readonly(),
    capacityProfile: z.strictObject({
      accountBandwidthBytes: byteCountSchema,
      accountStorageBytes: byteCountSchema,
      buckets: z
        .tuple([
          z.strictObject({
            ...capacityBucketShape,
            bucket: z.literal(
              CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
            ),
            domain: z.literal("open_data"),
          }),
          z.strictObject({
            ...capacityBucketShape,
            bucket: z.literal(
              CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
            ),
            domain: z.literal("query_table"),
          }),
        ])
        .readonly(),
      subscriptionTierStatus: z.literal("human_confirmation_required"),
    }),
    evidenceSha256: sha256Schema,
    identities: z
      .tuple([
        z.strictObject({
          ...identityBindingShape,
          bucket: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.bucket,
          ),
          domain: z.literal("open_data"),
          ipnsLabel: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsLabel,
          ),
          ipnsNetworkKey: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.openData.ipnsNetworkKey,
          ),
        }),
        z.strictObject({
          ...identityBindingShape,
          bucket: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.bucket,
          ),
          domain: z.literal("query_table"),
          ipnsLabel: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsLabel,
          ),
          ipnsNetworkKey: z.literal(
            CANDIDATE_SOURCE_SNAPSHOT_TARGET_BINDINGS.queryTable.ipnsNetworkKey,
          ),
        }),
      ])
      .readonly(),
    observedAt: z.string().datetime(),
    protectedSampleRollback: z.strictObject({
      verificationEvidenceSha256: sha256Schema,
      verifiedAt: z.string().datetime(),
    }),
    requestCount: z.number().int().positive(),
  })
  .superRefine((binding, context) => {
    for (const [index, identity] of binding.identities.entries()) {
      const expectedPrior =
        identity.domain === "open_data"
          ? CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.openData
          : CANDIDATE_SOURCE_SNAPSHOT_EXPECTED_PRIOR_CIDS.queryTable;
      if (
        identity.controlCid !== identity.officialGatewayCid ||
        identity.controlCid !== identity.signedRecordCid ||
        identity.controlCid !== expectedPrior
      ) {
        context.addIssue({
          code: "custom",
          message:
            "preflight identity sources must agree on the exact immutable prior CID",
          path: ["identities", index],
        });
      }
    }
    if (binding.identities[0].controlCid === binding.identities[1].controlCid) {
      context.addIssue({
        code: "custom",
        message: "preflight domains require distinct prior CIDs",
        path: ["identities"],
      });
    }
  });

export type CandidateSourceSnapshotPreflightBinding = z.infer<
  typeof candidateSourceSnapshotPreflightBindingSchema
>;
