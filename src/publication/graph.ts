import { sha256 } from "../lib/hash.js";
import { calculateIpfsCid, CIDV0_PATTERN, verifyIpfsCid } from "./ipfs-cid.js";

export const PUBLICATION_GRAPH_VERSION = "1";
export const PUBLICATION_SHARD_SIZE = 10_000;

type JsonPrimitive = boolean | null | number | string;
export type StrictJson =
  JsonPrimitive | StrictJson[] | { [key: string]: StrictJson };

export interface GraphPropertyInput {
  parcelIdentifier: string;
  propertyId: string;
  value: unknown;
}

export interface GraphObject {
  byteSize: number;
  bytes: Buffer;
  cid: string;
  key: string;
  role: "property" | "shard" | "root";
  sha256: string;
}

export interface GraphEdge {
  childCid: string;
  childKey: string;
  jsonPointer: string;
  parentKey: string;
}

export interface ShardEntry {
  cid: string;
  fileSizeBytes: number;
  parcelIdentifier: string;
  propertyId: string;
}

export interface ShardDocument {
  count: number;
  entries: ShardEntry[];
  fromParcel: string;
  schemaVersion: "1";
  shardIndex: number;
  toParcel: string;
}

export interface RootDocument {
  completedAt: string;
  county: "pasco";
  exportedAt: string;
  propertyCount: number;
  schemaVersion: "1";
  shardSize: 10_000;
  shards: Array<{
    count: number;
    fromParcel: string;
    shardCid: string;
    shardIndex: number;
    toParcel: string;
  }>;
  totalBytes: number;
}

export interface PublicationGraph {
  edges: GraphEdge[];
  objects: GraphObject[];
  propertyCids: Map<string, string>;
  root: RootDocument;
  rootCid: string;
  rootKey: "index.json";
  shards: ShardDocument[];
}

function codeUnitCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function strictCanonicalValue(value: unknown, pointer = "$"): StrictJson {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error(`Non-finite number at ${pointer}`);
    return value;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      if (!(index in value)) throw new Error(`Sparse array at ${pointer}`);
    }
    return value.map((entry, index) =>
      strictCanonicalValue(entry, `${pointer}/${index}`),
    );
  }
  if (typeof value !== "object" || value === undefined) {
    throw new Error(`Non-JSON value at ${pointer}`);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new Error(`Non-plain JSON object at ${pointer}`);
  }
  const source = value as Record<string, unknown>;
  const target: Record<string, StrictJson> = {};
  for (const key of Object.keys(source).sort(codeUnitCompare)) {
    if (source[key] === undefined)
      throw new Error(`Undefined value at ${pointer}/${key}`);
    target[key] = strictCanonicalValue(source[key], `${pointer}/${key}`);
  }
  return target;
}

export function publicationCanonicalJson(value: unknown): string {
  return `${JSON.stringify(strictCanonicalValue(value), null, 2)}\n`;
}

function assertTimestamp(value: string, label: string): void {
  if (new Date(value).toISOString() !== value) {
    throw new Error(`${label} must be an immutable millisecond UTC timestamp`);
  }
}

async function objectFor(
  key: string,
  role: GraphObject["role"],
  value: unknown,
): Promise<GraphObject> {
  const bytes = Buffer.from(publicationCanonicalJson(value), "utf8");
  return {
    byteSize: bytes.byteLength,
    bytes,
    cid: await calculateIpfsCid(bytes),
    key,
    role,
    sha256: sha256(bytes),
  };
}

export async function buildPublicationGraph(options: {
  completedAt: string;
  exportedAt: string;
  fixturePropertyIds?: ReadonlySet<string>;
  properties: readonly GraphPropertyInput[];
}): Promise<PublicationGraph> {
  assertTimestamp(options.exportedAt, "exportedAt");
  assertTimestamp(options.completedAt, "completedAt");
  const properties = [...options.properties].sort((left, right) =>
    codeUnitCompare(
      `${left.parcelIdentifier}\u0000${left.propertyId}`,
      `${right.parcelIdentifier}\u0000${right.propertyId}`,
    ),
  );
  const propertyIds = new Set<string>();
  const parcelIdentifiers = new Set<string>();
  for (const property of properties) {
    if (propertyIds.has(property.propertyId))
      throw new Error("Duplicate propertyId blocks publication");
    if (parcelIdentifiers.has(property.parcelIdentifier)) {
      throw new Error("Duplicate parcelIdentifier blocks publication");
    }
    if (options.fixturePropertyIds?.has(property.propertyId)) {
      throw new Error("Frozen fixture property injection blocks publication");
    }
    propertyIds.add(property.propertyId);
    parcelIdentifiers.add(property.parcelIdentifier);
  }

  const objects: GraphObject[] = [];
  const edges: GraphEdge[] = [];
  const propertyCids = new Map<string, string>();
  const entries: ShardEntry[] = [];
  for (const property of properties) {
    const key = `properties/${property.propertyId}.json`;
    const object = await objectFor(key, "property", property.value);
    objects.push(object);
    propertyCids.set(property.propertyId, object.cid);
    entries.push({
      cid: object.cid,
      fileSizeBytes: object.byteSize,
      parcelIdentifier: property.parcelIdentifier,
      propertyId: property.propertyId,
    });
  }

  const shards: ShardDocument[] = [];
  const rootShards: RootDocument["shards"] = [];
  for (
    let offset = 0;
    offset < entries.length;
    offset += PUBLICATION_SHARD_SIZE
  ) {
    const shardIndex = offset / PUBLICATION_SHARD_SIZE;
    const shardEntries = entries.slice(offset, offset + PUBLICATION_SHARD_SIZE);
    const first = shardEntries[0];
    const last = shardEntries.at(-1);
    if (!first || !last) throw new Error("Empty shards are prohibited");
    const shard: ShardDocument = {
      count: shardEntries.length,
      entries: shardEntries,
      fromParcel: first.parcelIdentifier,
      schemaVersion: PUBLICATION_GRAPH_VERSION,
      shardIndex,
      toParcel: last.parcelIdentifier,
    };
    const key = `shards/shard-${String(shardIndex).padStart(4, "0")}.json`;
    const object = await objectFor(key, "shard", shard);
    objects.push(object);
    shards.push(shard);
    rootShards.push({
      count: shard.count,
      fromParcel: shard.fromParcel,
      shardCid: object.cid,
      shardIndex,
      toParcel: shard.toParcel,
    });
    shard.entries.forEach((entry, index) => {
      edges.push({
        childCid: entry.cid,
        childKey: `properties/${entry.propertyId}.json`,
        jsonPointer: `/entries/${index}/cid`,
        parentKey: key,
      });
    });
  }
  const root: RootDocument = {
    completedAt: options.completedAt,
    county: "pasco",
    exportedAt: options.exportedAt,
    propertyCount: entries.length,
    schemaVersion: PUBLICATION_GRAPH_VERSION,
    shardSize: PUBLICATION_SHARD_SIZE,
    shards: rootShards,
    totalBytes: entries.reduce(
      (total, entry) => total + entry.fileSizeBytes,
      0,
    ),
  };
  const rootObject = await objectFor("index.json", "root", root);
  objects.push(rootObject);
  rootShards.forEach((shard, index) => {
    edges.push({
      childCid: shard.shardCid,
      childKey: `shards/shard-${String(shard.shardIndex).padStart(4, "0")}.json`,
      jsonPointer: `/shards/${index}/shardCid`,
      parentKey: "index.json",
    });
  });
  const graph = {
    edges,
    objects,
    propertyCids,
    root,
    rootCid: rootObject.cid,
    rootKey: "index.json" as const,
    shards,
  };
  await validatePublicationGraph(graph);
  return graph;
}

export async function validatePublicationGraph(
  graph: PublicationGraph,
): Promise<void> {
  const objects = new Map(graph.objects.map((object) => [object.key, object]));
  if (objects.size !== graph.objects.length)
    throw new Error("Graph contains duplicate object keys");
  const rootObject = objects.get(graph.rootKey);
  if (
    !rootObject ||
    rootObject.cid !== graph.rootCid ||
    rootObject.role !== "root"
  ) {
    throw new Error("Graph root binding is invalid");
  }
  const reached = new Set<string>();
  for (const object of graph.objects) {
    if (!CIDV0_PATTERN.test(object.cid))
      throw new Error(`Graph object CID is invalid (${object.key})`);
    if (
      object.byteSize !== object.bytes.byteLength ||
      sha256(object.bytes) !== object.sha256
    ) {
      throw new Error(`Graph object byte binding is invalid (${object.key})`);
    }
    await verifyIpfsCid(object.bytes, object.cid);
  }
  for (const edge of graph.edges) {
    const parent = objects.get(edge.parentKey);
    const child = objects.get(edge.childKey);
    if (!parent || !child || child.cid !== edge.childCid)
      throw new Error(
        "Graph edge references an unknown or mismatched child CID",
      );
    if (!parent.bytes.toString("utf8").includes(`"${edge.childCid}"`)) {
      throw new Error("Graph parent bytes do not embed the declared child CID");
    }
    reached.add(edge.childKey);
  }
  const leaves = graph.objects.filter((object) => object.role === "property");
  if (leaves.some((leaf) => !reached.has(leaf.key)))
    throw new Error("Graph has an unreachable property leaf");
  if (
    leaves.length !== graph.root.propertyCount ||
    leaves.length !== graph.propertyCids.size
  ) {
    throw new Error("Graph property cardinality is inconsistent");
  }
  for (let index = 1; index < graph.root.shards.length; index += 1) {
    const previous = graph.root.shards[index - 1];
    const current = graph.root.shards[index];
    if (
      !previous ||
      !current ||
      codeUnitCompare(previous.toParcel, current.fromParcel) >= 0
    ) {
      throw new Error("Graph shard parcel ranges overlap or are not monotonic");
    }
  }
}
