import { canonicalJsonSha256 } from "../lib/canonical-json.js";
import type { DatasetMetadata } from "./provider.js";

export const CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_EVIDENCE = Object.freeze({
  artifactSetSha256:
    "c7c293492e440f264cc1add2a21c13f211f09d42702cbfa1828f936573fad03c",
  carVerifications: [
    {
      bulkResultSha256:
        "cb694967a563429a141baa66959f0102999ea1ec88201d7123e28d73e5255a07",
      bulkVerificationId:
        "snapshotdemocarbulk_84b1f09165ab807375cd89fa818376e4",
      bulkVerifiedAt: "2026-09-01T13:25:03.334Z",
      carBytes: 3_416_498_957,
      carSha256:
        "8707930f35822233d069b1a09868a8a358db3cd744244d4412e5d16b1220aad5",
      domain: "open_data",
      gatewayEvidenceId:
        "snapshotdemocargateway_667b3f0347e0eb9abbbaa7f6d903206d",
      gatewayEvidenceSha256:
        "f3f84b2be77e0318758b1d1ff9cd5b2b78e4b125617f6a528a660b3bbe20f7e9",
      gatewayObservedAt: "2026-09-01T13:24:56.654Z",
      memberCount: 325_311,
      memberLogicalBytes: 3_388_322_519,
      memberSetSha256:
        "15ed8afaef87966e2742fa88d9380030eff00d8df695e76079b6e51f258d9363",
      receiptId: "snapshotdemocarreceipt_1dc716fea2aac496ec3871dfd9ab4f8f",
      receiptSha256:
        "25c7d70bae0dd1804d338a1dcabede7836e63874160b01deb42a733296fd1aac",
      rootCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
    },
    {
      bulkResultSha256:
        "dc2f5c8305306b9aee99634ca2a901946b43eb9a5b6d3d06e4a9810a24289578",
      bulkVerificationId:
        "snapshotdemocarbulk_c1f6eb6632c30900f00ea23eb68e1c18",
      bulkVerifiedAt: "2026-09-01T13:30:08.477Z",
      carBytes: 69_457_094,
      carSha256:
        "01de5df6e0770c45ef6cd3abf70902bb3a7f1052daea2ec29d67401c765bcff5",
      domain: "query_table",
      gatewayEvidenceId:
        "snapshotdemocargateway_9ca49c17c95f2b806f31574d5f9bdffe",
      gatewayEvidenceSha256:
        "1fd4cb382b1b499b259a9a9222481a9d79fda052d88d9c84ecfb4f6dfd51a462",
      gatewayObservedAt: "2026-09-01T13:30:08.385Z",
      memberCount: 1,
      memberLogicalBytes: 69_430_565,
      memberSetSha256:
        "a41aacc5ce15eb1595d43067cdccb4d49802b29c5fb9f6bc047f1f9bf810a700",
      receiptId: "snapshotdemocarreceipt_e3cd56e057d1ca8521f2de86828982ed",
      receiptSha256:
        "0a650db0ab882e3ac88c9dafa4658288cc12acf217a7a42b277bb06ca699e48b",
      rootCid: "QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw",
    },
  ],
  ipns: {
    openData: {
      networkKey:
        "k51qzi5uqu5dme2zfev56k5s15i20si9ke4l6mjnv6qpgd4disfprli0gr66x6",
      targetCid: "QmVqEfh8BwE8QXAyhoNSVprSB726eYynfQtZWUxXh3r1sy",
    },
    queryTable: {
      networkKey:
        "k51qzi5uqu5dlj11ik6bpomd7581ipkp9h2sm6gpadwqx6zkjyl2h32osd7rgm",
      targetCid: "QmPH58KURSVWdbmBMb3gBTexs5a1EKxKpKD4QfTdW24Cdw",
    },
    resolverPolicy: "candidate_filebase_delegated_v2",
    verification:
      "provider readiness requires each configured identity to resolve to its exact target through the candidate control-plane, public-gateway, and signed delegated-record checks",
  },
  plan: {
    artifactCid: "QmcxZWB8W2asaZDNNXi1WyprzQT8cMKmen7FW8fbGiivTW",
    artifactSha256:
      "15e3225b3db098fa580d1643e10c2455959395111107d4e73a7e4b3128defd90",
    id: "snapshotdemo_87e3253348cedf80ecba1d716791dd16",
    logicalSha256:
      "1f98bdf9fa8269fd64b26314fd93aa9bbbf7850390176612366a8989975583ee",
  },
  publication: {
    canonical: false,
    independentlyPascoCertified: false,
    publicationClass: "candidate_owned_source_snapshot_demo",
    resourceOwner: "candidate",
  },
  schemaVersion: "candidate-source-snapshot-publication-evidence-v1",
  uploadClosure: {
    closureId: "snapshotdemouploadclosure_95a8b6c2a3a8a6ff4825d3fb2286ab8b",
    closureSha256:
      "922d92b2eae98749e92e367ed61ff0d7cb7d0cc5d59f6a4341de521571dad84f",
    logicalBytes: 3_457_753_084,
    logicalObjectCount: 325_312,
    providerCidMismatchCount: 0,
    unresolvedObjectCount: 0,
    verifiedAt: "2026-09-01T13:30:08.485Z",
  },
});

export const CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_EVIDENCE_SHA256 =
  "cdf73f2a7bf3a31cf8dd7333d5b04e7e34da33227dd90997feb4502e2526a591";

export function validatedCandidateSourceSnapshotPublicationEvidence(
  metadata: DatasetMetadata,
): typeof CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_EVIDENCE | null {
  const evidence = CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_EVIDENCE;
  if (
    canonicalJsonSha256(evidence) !==
      CANDIDATE_SOURCE_SNAPSHOT_PUBLICATION_EVIDENCE_SHA256 ||
    metadata.publication.candidateDemoPlanId !== evidence.plan.id ||
    metadata.publication.candidateDemoPlanSha256 !==
      evidence.plan.logicalSha256 ||
    metadata.publication.planCid !== evidence.plan.artifactCid ||
    metadata.publication.openDataIpns !== evidence.ipns.openData.networkKey ||
    metadata.publication.openDataRootCid !== evidence.ipns.openData.targetCid ||
    metadata.publication.queryTableIpns !==
      evidence.ipns.queryTable.networkKey ||
    metadata.publication.queryTableRootCid !==
      evidence.ipns.queryTable.targetCid ||
    metadata.publication.resolverPolicy !== evidence.ipns.resolverPolicy ||
    metadata.objectCount !== evidence.uploadClosure.logicalObjectCount
  ) {
    return null;
  }
  return evidence;
}
