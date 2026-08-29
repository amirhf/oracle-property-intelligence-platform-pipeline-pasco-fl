import type { PublicationArtifact, PublicationPlan } from "./plan.js";

export interface PublicationUploadReceipt {
  cid: string;
  domain: PublicationArtifact["domain"];
  objectKey: string;
  sha256: string;
}

export interface PublicationIpnsReceipt {
  domain: PublicationArtifact["domain"];
  networkKey: string;
  priorCid: string | null;
  targetCid: string;
}

export interface RemotePublicationExecutor {
  resolveIpns(
    domain: PublicationArtifact["domain"],
    signal?: AbortSignal,
  ): Promise<string | null>;
  updateIpns(
    domain: PublicationArtifact["domain"],
    targetCid: string,
    signal?: AbortSignal,
  ): Promise<PublicationIpnsReceipt>;
  upload(
    artifact: PublicationArtifact,
    signal?: AbortSignal,
  ): Promise<PublicationUploadReceipt>;
  verifyCid(
    receipt: PublicationUploadReceipt,
    signal?: AbortSignal,
  ): Promise<boolean>;
}

export function createRemotePublicationExecutor(_plan: PublicationPlan): never {
  throw new Error(
    "Remote publication is not configured; no production or local publisher is available",
  );
}
