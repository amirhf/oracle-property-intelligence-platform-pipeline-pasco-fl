import { createHash, type Hash } from "node:crypto";
import { mkdir, open, unlink, type FileHandle } from "node:fs/promises";
import path from "node:path";

import { CID } from "multiformats/cid";

const CIDV0_BYTE_LENGTH = 34;
const DAG_PB_CODE = 0x70;
const SHA2_256_CODE = 0x12;
const SHA2_256_DIGEST_LENGTH = 32;

export const DEFAULT_MAX_CAR_V1_HEADER_BYTES = 32 * 1024 * 1024;
export const DEFAULT_MAX_CAR_V1_SECTION_BYTES = 2 * 1024 * 1024;

export interface CarV1Identity {
  blockCount: number;
  blockMembershipSha256: string;
  byteSize: number;
  carBytes: number;
  carSha256: string;
  roots: string[];
  sha256: string;
}

export interface CarV1Writer {
  abort(): Promise<void>;
  appendBlock(cid: string, bytes: Uint8Array): Promise<void>;
  close(): Promise<CarV1Identity>;
  finalize(): Promise<CarV1Identity>;
  putBlock(cid: string, bytes: Uint8Array): Promise<void>;
}

interface CarV1Bounds {
  maxHeaderBytes: number;
  maxSectionBytes: number;
}

function positiveBound(value: number | undefined, fallback: number): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new Error("CARv1 byte bounds must be positive safe integers");
  }
  return resolved;
}

function bounds(options: {
  maxHeaderBytes?: number;
  maxSectionBytes?: number;
}): CarV1Bounds {
  return {
    maxHeaderBytes: positiveBound(
      options.maxHeaderBytes,
      DEFAULT_MAX_CAR_V1_HEADER_BYTES,
    ),
    maxSectionBytes: positiveBound(
      options.maxSectionBytes,
      DEFAULT_MAX_CAR_V1_SECTION_BYTES,
    ),
  };
}

function parseCidV0(value: string): CID {
  let cid: CID;
  try {
    cid = CID.parse(value);
  } catch {
    throw new Error("CARv1 block identity is not a valid CID");
  }
  if (
    cid.version !== 0 ||
    cid.code !== DAG_PB_CODE ||
    cid.multihash.code !== SHA2_256_CODE ||
    cid.multihash.digest.byteLength !== SHA2_256_DIGEST_LENGTH ||
    cid.bytes.byteLength !== CIDV0_BYTE_LENGTH ||
    cid.toString() !== value
  ) {
    throw new Error("CARv1 supports only canonical dag-pb sha2-256 CIDv0");
  }
  return cid;
}

function verifiedBlock(cidValue: string, bytesValue: Uint8Array): {
  bytes: Buffer;
  cid: CID;
  cidBytes: Buffer;
} {
  const cid = parseCidV0(cidValue);
  const bytes = Buffer.from(
    bytesValue.buffer,
    bytesValue.byteOffset,
    bytesValue.byteLength,
  );
  const digest = createHash("sha256").update(bytes).digest();
  if (!digest.equals(Buffer.from(cid.multihash.digest))) {
    throw new Error("CARv1 block bytes do not match their declared CID");
  }
  return { bytes, cid, cidBytes: Buffer.from(cid.bytes) };
}

function encodeUnsigned(value: number): Buffer {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("CARv1 integer must be an unsigned safe integer");
  }
  if (value < 24) return Buffer.from([value]);
  if (value <= 0xff) return Buffer.from([24, value]);
  if (value <= 0xffff) {
    const encoded = Buffer.allocUnsafe(3);
    encoded[0] = 25;
    encoded.writeUInt16BE(value, 1);
    return encoded;
  }
  if (value <= 0xffff_ffff) {
    const encoded = Buffer.allocUnsafe(5);
    encoded[0] = 26;
    encoded.writeUInt32BE(value, 1);
    return encoded;
  }
  const encoded = Buffer.allocUnsafe(9);
  encoded[0] = 27;
  encoded.writeBigUInt64BE(BigInt(value), 1);
  return encoded;
}

function encodeCborLength(majorType: number, value: number): Buffer {
  const encoded = encodeUnsigned(value);
  encoded[0] = encoded[0]! | (majorType << 5);
  return encoded;
}

function encodeCborText(value: string): Buffer {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([encodeCborLength(3, bytes.byteLength), bytes]);
}

function encodeCarHeader(
  rootValues: readonly string[],
  maximumByteLength: number,
): Buffer {
  if (rootValues.length === 0) {
    throw new Error("CARv1 requires at least one root");
  }
  const mapPrefix = Buffer.from([0xa2]);
  const rootsKey = encodeCborText("roots");
  const rootsPrefix = encodeCborLength(4, rootValues.length);
  const cidTag = Buffer.from([0xd8, 0x2a]);
  const cidBytesPrefix = encodeCborLength(2, CIDV0_BYTE_LENGTH + 1);
  const versionKey = encodeCborText("version");
  const version = Buffer.from([1]);
  const encodedRootBytes =
    cidTag.byteLength +
    cidBytesPrefix.byteLength +
    1 +
    CIDV0_BYTE_LENGTH;
  const byteLength =
    mapPrefix.byteLength +
    rootsKey.byteLength +
    rootsPrefix.byteLength +
    rootValues.length * encodedRootBytes +
    versionKey.byteLength +
    version.byteLength;
  if (!Number.isSafeInteger(byteLength)) {
    throw new Error("CARv1 header byte length is not a safe integer");
  }
  if (byteLength > maximumByteLength) {
    throw new Error("CARv1 header exceeds its configured byte bound");
  }
  const header = Buffer.allocUnsafe(byteLength);
  let offset = 0;
  const append = (bytes: Uint8Array): void => {
    header.set(bytes, offset);
    offset += bytes.byteLength;
  };
  append(mapPrefix);
  append(rootsKey);
  append(rootsPrefix);
  const uniqueRoots = new Set<string>();
  for (const rootValue of rootValues) {
    const root = parseCidV0(rootValue);
    if (uniqueRoots.has(rootValue)) {
      throw new Error("CARv1 root list contains a duplicate CID");
    }
    uniqueRoots.add(rootValue);
    append(cidTag);
    append(cidBytesPrefix);
    header[offset] = 0;
    offset += 1;
    append(root.bytes);
  }
  append(versionKey);
  append(version);
  if (offset !== header.byteLength) {
    throw new Error("CARv1 header encoding did not consume its exact buffer");
  }
  return header;
}

function encodeUvarint(value: number): Buffer {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("CARv1 varint must be an unsigned safe integer");
  }
  const output: number[] = [];
  let remaining = BigInt(value);
  do {
    let byte = Number(remaining & 0x7fn);
    remaining >>= 7n;
    if (remaining !== 0n) byte |= 0x80;
    output.push(byte);
  } while (remaining !== 0n);
  return Buffer.from(output);
}

function updateMembership(hash: Hash, cidBytes: Buffer): void {
  hash.update(encodeUvarint(cidBytes.byteLength));
  hash.update(cidBytes);
}

async function writeAll(handle: FileHandle, bytes: Buffer): Promise<void> {
  let offset = 0;
  while (offset < bytes.byteLength) {
    const result = await handle.write(
      bytes,
      offset,
      bytes.byteLength - offset,
      null,
    );
    if (result.bytesWritten <= 0) {
      throw new Error("CARv1 output made no write progress");
    }
    offset += result.bytesWritten;
  }
}

class FileCarV1Writer implements CarV1Writer {
  readonly #bounds: CarV1Bounds;
  readonly #carHash = createHash("sha256");
  readonly #handle: FileHandle;
  readonly #membershipHash = createHash("sha256");
  readonly #outputPath: string;
  readonly #roots: string[];
  readonly #unwrittenRoots: Set<string>;
  readonly #writtenCids = new Set<string>();
  #blockCount = 0;
  #busy = false;
  #byteSize = 0;
  #finished = false;

  private constructor(input: {
    bounds: CarV1Bounds;
    handle: FileHandle;
    outputPath: string;
    roots: string[];
  }) {
    this.#bounds = input.bounds;
    this.#handle = input.handle;
    this.#outputPath = input.outputPath;
    this.#roots = input.roots;
    this.#unwrittenRoots = new Set(input.roots);
  }

  static async create(input: {
    bounds: CarV1Bounds;
    outputPath: string;
    roots: readonly string[];
  }): Promise<FileCarV1Writer> {
    const outputPath = path.resolve(input.outputPath);
    if (outputPath === path.parse(outputPath).root) {
      throw new Error("CARv1 output cannot be a filesystem root");
    }
    const roots = [...input.roots];
    const header = encodeCarHeader(roots, input.bounds.maxHeaderBytes);
    await mkdir(path.dirname(outputPath), { recursive: true });
    const handle = await open(outputPath, "wx");
    const writer = new FileCarV1Writer({
      bounds: input.bounds,
      handle,
      outputPath,
      roots,
    });
    try {
      await writer.#write(encodeUvarint(header.byteLength));
      await writer.#write(header);
      return writer;
    } catch (error) {
      await writer.abort();
      throw error;
    }
  }

  async #write(bytes: Buffer): Promise<void> {
    await writeAll(this.#handle, bytes);
    this.#carHash.update(bytes);
    this.#byteSize += bytes.byteLength;
  }

  async abort(): Promise<void> {
    if (this.#finished) return;
    this.#finished = true;
    await this.#handle.close().catch(() => undefined);
    await unlink(this.#outputPath).catch(() => undefined);
  }

  async appendBlock(cidValue: string, bytesValue: Uint8Array): Promise<void> {
    if (this.#finished) throw new Error("CARv1 writer is already closed");
    if (this.#busy) {
      throw new Error("CARv1 block writes must be sequentially awaited");
    }
    this.#busy = true;
    try {
      const { bytes, cid, cidBytes } = verifiedBlock(cidValue, bytesValue);
      const cidString = cid.toString();
      if (this.#writtenCids.has(cidString)) {
        throw new Error("CARv1 contains a duplicate block CID");
      }
      const sectionLength = cidBytes.byteLength + bytes.byteLength;
      if (sectionLength > this.#bounds.maxSectionBytes) {
        throw new Error("CARv1 block section exceeds its configured byte bound");
      }
      await this.#write(encodeUvarint(sectionLength));
      await this.#write(cidBytes);
      await this.#write(bytes);
      this.#writtenCids.add(cidString);
      this.#unwrittenRoots.delete(cidString);
      updateMembership(this.#membershipHash, cidBytes);
      this.#blockCount += 1;
    } catch (error) {
      await this.abort();
      throw error;
    } finally {
      this.#busy = false;
    }
  }

  async close(): Promise<CarV1Identity> {
    if (this.#finished) throw new Error("CARv1 writer is already closed");
    if (this.#busy) throw new Error("CARv1 writer has an active block write");
    if (this.#unwrittenRoots.size !== 0) {
      await this.abort();
      throw new Error("CARv1 is missing one or more declared root blocks");
    }
    this.#finished = true;
    try {
      await this.#handle.sync();
    } finally {
      await this.#handle.close();
    }
    const sha256 = this.#carHash.digest("hex");
    return {
      blockCount: this.#blockCount,
      blockMembershipSha256: this.#membershipHash.digest("hex"),
      byteSize: this.#byteSize,
      carBytes: this.#byteSize,
      carSha256: sha256,
      roots: [...this.#roots],
      sha256,
    };
  }

  async finalize(): Promise<CarV1Identity> {
    return await this.close();
  }

  async putBlock(cid: string, bytes: Uint8Array): Promise<void> {
    await this.appendBlock(cid, bytes);
  }
}

export async function createCarV1Writer(input: {
  maxHeaderBytes?: number;
  maxSectionBytes?: number;
  outputPath: string;
  roots: readonly string[];
}): Promise<CarV1Writer> {
  return await FileCarV1Writer.create({
    bounds: bounds(input),
    outputPath: input.outputPath,
    roots: input.roots,
  });
}

export async function createCarWriter(
  outputPath: string,
  roots: readonly string[],
  options: { maxHeaderBytes?: number; maxSectionBytes?: number } = {},
): Promise<CarV1Writer> {
  return await createCarV1Writer({ outputPath, roots, ...options });
}

class BufferedFileReader {
  readonly #buffer = Buffer.allocUnsafe(64 * 1024);
  readonly #carHash = createHash("sha256");
  readonly #handle: FileHandle;
  readonly #size: number;
  #bufferLength = 0;
  #bufferOffset = 0;
  #consumed = 0;
  #readOffset = 0;

  constructor(handle: FileHandle, size: number) {
    this.#handle = handle;
    this.#size = size;
  }

  get consumed(): number {
    return this.#consumed;
  }

  get size(): number {
    return this.#size;
  }

  async #refill(): Promise<void> {
    if (this.#readOffset >= this.#size) {
      throw new Error("CARv1 ended before its declared section length");
    }
    const requested = Math.min(
      this.#buffer.byteLength,
      this.#size - this.#readOffset,
    );
    const result = await this.#handle.read(
      this.#buffer,
      0,
      requested,
      this.#readOffset,
    );
    if (result.bytesRead <= 0) {
      throw new Error("CARv1 file made no read progress");
    }
    this.#bufferLength = result.bytesRead;
    this.#bufferOffset = 0;
    this.#readOffset += result.bytesRead;
    this.#carHash.update(this.#buffer.subarray(0, result.bytesRead));
  }

  async readByte(): Promise<number> {
    if (this.#bufferOffset >= this.#bufferLength) await this.#refill();
    const value = this.#buffer[this.#bufferOffset];
    if (value === undefined) throw new Error("CARv1 byte read failed");
    this.#bufferOffset += 1;
    this.#consumed += 1;
    return value;
  }

  async readExactly(length: number): Promise<Buffer> {
    if (
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > this.#size - this.#consumed
    ) {
      throw new Error("CARv1 section length exceeds the remaining file");
    }
    const output = Buffer.allocUnsafe(length);
    let outputOffset = 0;
    while (outputOffset < length) {
      if (this.#bufferOffset >= this.#bufferLength) await this.#refill();
      const available = this.#bufferLength - this.#bufferOffset;
      const take = Math.min(available, length - outputOffset);
      this.#buffer.copy(
        output,
        outputOffset,
        this.#bufferOffset,
        this.#bufferOffset + take,
      );
      this.#bufferOffset += take;
      this.#consumed += take;
      outputOffset += take;
    }
    return output;
  }

  digest(): string {
    if (this.#consumed !== this.#size) {
      throw new Error("CARv1 validation did not consume the complete file");
    }
    return this.#carHash.digest("hex");
  }
}

async function readUvarint(
  reader: BufferedFileReader,
  maximum: number,
): Promise<number> {
  const raw: number[] = [];
  let value = 0n;
  for (let index = 0; index < 10; index += 1) {
    const byte = await reader.readByte();
    raw.push(byte);
    value |= BigInt(byte & 0x7f) << BigInt(index * 7);
    if ((byte & 0x80) === 0) {
      if (value > BigInt(Number.MAX_SAFE_INTEGER) || value > BigInt(maximum)) {
        throw new Error("CARv1 varint exceeds its configured byte bound");
      }
      const decoded = Number(value);
      if (!Buffer.from(raw).equals(encodeUvarint(decoded))) {
        throw new Error("CARv1 varint is not minimally encoded");
      }
      return decoded;
    }
  }
  throw new Error("CARv1 varint is unterminated or too wide");
}

export async function validateCarV1(input: {
  expectedBlockCount?: number;
  expectedBlockMembershipSha256?: string;
  expectedRoots: readonly string[];
  filePath: string;
  maxHeaderBytes?: number;
  maxSectionBytes?: number;
}): Promise<CarV1Identity> {
  const limit = bounds(input);
  const expectedRoots = [...input.expectedRoots];
  const expectedHeader = encodeCarHeader(expectedRoots, limit.maxHeaderBytes);
  if (
    input.expectedBlockCount !== undefined &&
    (!Number.isSafeInteger(input.expectedBlockCount) ||
      input.expectedBlockCount < 0)
  ) {
    throw new Error("Expected CARv1 block count is invalid");
  }
  if (
    input.expectedBlockMembershipSha256 !== undefined &&
    !/^[a-f0-9]{64}$/.test(input.expectedBlockMembershipSha256)
  ) {
    throw new Error("Expected CARv1 membership SHA-256 is invalid");
  }
  const filePath = path.resolve(input.filePath);
  const handle = await open(filePath, "r");
  try {
    const state = await handle.stat();
    if (!state.isFile() || !Number.isSafeInteger(state.size) || state.size <= 0) {
      throw new Error("CARv1 input is not a bounded regular file");
    }
    const reader = new BufferedFileReader(handle, state.size);
    const headerLength = await readUvarint(reader, limit.maxHeaderBytes);
    if (headerLength === 0) throw new Error("CARv1 header is empty");
    const header = await reader.readExactly(headerLength);
    if (!header.equals(expectedHeader)) {
      throw new Error("CARv1 header does not match the exact expected roots");
    }

    const rootBlocksMissing = new Set(expectedRoots);
    const blockCids = new Set<string>();
    const membershipHash = createHash("sha256");
    let blockCount = 0;
    while (reader.consumed < reader.size) {
      const sectionLength = await readUvarint(reader, limit.maxSectionBytes);
      if (sectionLength <= CIDV0_BYTE_LENGTH) {
        throw new Error("CARv1 block section has no block bytes");
      }
      const section = await reader.readExactly(sectionLength);
      const cidBytes = section.subarray(0, CIDV0_BYTE_LENGTH);
      let cid: CID;
      try {
        cid = CID.decode(cidBytes);
      } catch {
        throw new Error("CARv1 section contains an invalid CID");
      }
      const cidString = cid.toString();
      parseCidV0(cidString);
      if (!Buffer.from(cid.bytes).equals(cidBytes)) {
        throw new Error("CARv1 section CID is not canonical CIDv0");
      }
      if (blockCids.has(cidString)) {
        throw new Error("CARv1 contains a duplicate block CID");
      }
      const blockBytes = section.subarray(CIDV0_BYTE_LENGTH);
      verifiedBlock(cidString, blockBytes);
      blockCids.add(cidString);
      rootBlocksMissing.delete(cidString);
      updateMembership(membershipHash, Buffer.from(cid.bytes));
      blockCount += 1;
    }
    if (rootBlocksMissing.size !== 0) {
      throw new Error("CARv1 is missing one or more declared root blocks");
    }
    if (
      input.expectedBlockCount !== undefined &&
      blockCount !== input.expectedBlockCount
    ) {
      throw new Error("CARv1 block count does not match its expected binding");
    }
    const blockMembershipSha256 = membershipHash.digest("hex");
    if (
      input.expectedBlockMembershipSha256 !== undefined &&
      blockMembershipSha256 !== input.expectedBlockMembershipSha256
    ) {
      throw new Error("CARv1 block membership does not match its expected binding");
    }
    const sha256 = reader.digest();
    return {
      blockCount,
      blockMembershipSha256,
      byteSize: state.size,
      carBytes: state.size,
      carSha256: sha256,
      roots: expectedRoots,
      sha256,
    };
  } finally {
    await handle.close();
  }
}

export const validateCar = validateCarV1;
