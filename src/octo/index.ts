import { createDecipheriv, createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { open, stat } from "node:fs/promises";
import { basename } from "node:path";
import { Readable } from "node:stream";
import { WebSocket } from "ws";
import { generateKeyPair, sharedKey } from "curve25519-js";
import type {
  BotIdentity,
  ChannelTransport,
  ChatTarget,
  GroupContext,
  InboundMessage,
  SendReceipt,
} from "../contracts.ts";
import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import {
  buildExecutionCard,
  type ExecutionCardCapabilities,
  type ExecutionSnapshot,
} from "../execution-card.ts";

type Logger = (message: string) => void;

export type OctoTransportOptions = {
  apiUrl: string;
  botToken: string;
  wsUrl?: string;
  logger?: Logger;
};

type Registration = {
  uid: string;
  name: string;
  imToken: string;
  wsUrl: string;
  ownerUid?: string;
};


type WireFrame = {
  packetType: number;
  flags: number;
  body: Uint8Array;
};
type DecodedInbound = {
  setting: number;
  senderId: string;
  rawChannelId: string;
  channelType: number;
  messageId: bigint;
  messageSeq: number;
  payload: unknown;
};

type Curve25519KeyPair = {
  public: Uint8Array;
  private: Uint8Array;
};

type SocketState = {
  socket: WebSocket;
  decoder: FrameDecoder;
  clientKey: Curve25519KeyPair;
  authenticated: boolean;
  serverVersion: number;
  aesKey: Buffer | undefined;
  aesIv: Buffer | undefined;
  handshakeTimer: NodeJS.Timeout | undefined;
  pingTimer: NodeJS.Timeout | undefined;
  pongTimer: NodeJS.Timeout | undefined;
  closed: boolean;
};

type HttpResult = {
  status: number;
  body: Uint8Array;
};

type ImageInfo = {
  contentType: string;
  width: number;
  height: number;
};

type PresignedUpload = {
  uploadUrl: string;
  downloadUrl: string;
  contentType: string;
  contentDisposition: string | undefined;
};

const CONNECT_PACKET = 1;
const CONNACK_PACKET = 2;
const RECV_PACKET = 5;
const RECVACK_PACKET = 6;
const PING_PACKET = 7;
const PONG_PACKET = 8;
const DISCONNECT_PACKET = 9;
const PROTOCOL_VERSION = 4;
const DEVICE_FLAG = 0;
const TEXT_MESSAGE_TYPE = 1;
const IMAGE_MESSAGE_TYPE = 2;
const FILE_MESSAGE_TYPE = 8;
const INTERACTIVE_CARD_MESSAGE_TYPE = 17;
const CARD_PROFILE = "octo/v1";
const CARD_VERSION = "1.5";
const MAX_TEXT_BYTES = 3_000;
const MAX_GROUP_CONTEXT_BYTES = 64 * 1024;
const MAX_HTTP_BODY_BYTES = 2 * 1024 * 1024;
const HTTP_TIMEOUT_MS = 15_000;
const HANDSHAKE_TIMEOUT_MS = 15_000;
const RECONNECT_BASE_MS = 100;
const RECONNECT_MAX_MS = 30_000;
const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;
const FRAME_MAX_BYTES = 16 * 1024 * 1024;
const AES_BLOCK_BYTES = 16;

class OctoError extends Error {
  readonly status: number | undefined;
  readonly permanent: boolean;

  constructor(message: string, options: { status?: number; permanent?: boolean } = {}) {
    super(message);
    this.name = "OctoError";
    this.status = options.status;
    this.permanent = options.permanent ?? false;
  }
}

class ByteWriter {
  private readonly chunks: Buffer[] = [];

  byte(value: number): this {
    if (!Number.isInteger(value) || value < 0 || value > 0xff) {
      throw new OctoError("Octo protocol byte is out of range");
    }
    const chunk = Buffer.allocUnsafe(1);
    chunk.writeUInt8(value, 0);
    this.chunks.push(chunk);
    return this;
  }

  int32(value: number): this {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw new OctoError("Octo protocol int32 is out of range");
    }
    const chunk = Buffer.allocUnsafe(4);
    chunk.writeUInt32BE(value, 0);
    this.chunks.push(chunk);
    return this;
  }

  int64(value: bigint): this {
    if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
      throw new OctoError("Octo protocol int64 is out of range");
    }
    const chunk = Buffer.allocUnsafe(8);
    chunk.writeBigUInt64BE(value, 0);
    this.chunks.push(chunk);
    return this;
  }

  string(value: string): this {
    const encoded = Buffer.from(value, "utf8");
    if (encoded.byteLength > 0xffff) {
      throw new OctoError("Octo protocol string exceeds 65535 bytes");
    }
    const length = Buffer.allocUnsafe(2);
    length.writeUInt16BE(encoded.byteLength, 0);
    this.chunks.push(length, encoded);
    return this;
  }

  raw(value: Uint8Array): this {
    this.chunks.push(Buffer.from(value));
    return this;
  }

  finish(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

class ByteReader {
  private offset = 0;

  constructor(private readonly data: Uint8Array) {}

  get remainingLength(): number {
    return this.data.byteLength - this.offset;
  }

  byte(): number {
    this.require(1);
    return this.data[this.offset++]!;
  }

  int32(): number {
    this.require(4);
    const value = Buffer.from(this.data.buffer, this.data.byteOffset + this.offset, 4).readUInt32BE(0);
    this.offset += 4;
    return value;
  }

  int64(): bigint {
    this.require(8);
    const value = Buffer.from(this.data.buffer, this.data.byteOffset + this.offset, 8).readBigUInt64BE(0);
    this.offset += 8;
    return value;
  }

  string(): string {
    this.require(2);
    const length = Buffer.from(this.data.buffer, this.data.byteOffset + this.offset, 2).readUInt16BE(0);
    this.offset += 2;
    this.require(length);
    const value = new TextDecoder("utf-8", { fatal: true }).decode(
      this.data.slice(this.offset, this.offset + length),
    );
    this.offset += length;
    return value;
  }

  bytes(): Uint8Array {
    const value = this.data.slice(this.offset);
    this.offset = this.data.byteLength;
    return value;
  }

  private require(length: number): void {
    if (length < 0 || this.offset + length > this.data.byteLength) {
      throw new OctoError("Octo protocol packet is truncated");
    }
  }
}

class FrameDecoder {
  private buffer = Buffer.alloc(0);

  push(data: Uint8Array): WireFrame[] {
    if (data.byteLength > 0) this.buffer = Buffer.concat([this.buffer, Buffer.from(data)]);
    const frames: WireFrame[] = [];
    let offset = 0;
    while (offset < this.buffer.byteLength) {
      const header = this.buffer[offset]!;
      const packetType = header >> 4;
      if (packetType === PING_PACKET || packetType === PONG_PACKET) {
        frames.push({ packetType, flags: header & 0x0f, body: new Uint8Array() });
        offset += 1;
        continue;
      }
      const length = readVariableLength(this.buffer, offset + 1);
      if (length === undefined) break;
      if (length.value > FRAME_MAX_BYTES) throw new OctoError("Octo protocol frame exceeds safe limit");
      const bodyStart = length.next;
      const bodyEnd = bodyStart + length.value;
      if (bodyEnd > this.buffer.byteLength) break;
      frames.push({ packetType, flags: header & 0x0f, body: this.buffer.slice(bodyStart, bodyEnd) });
      offset = bodyEnd;
    }
    if (offset > 0) this.buffer = this.buffer.slice(offset);
    return frames;
  }
}

function readVariableLength(data: Uint8Array, start: number): { value: number; next: number } | undefined {
  let value = 0;
  let multiplier = 1;
  let offset = start;
  for (let index = 0; index < 5; index += 1) {
    if (offset >= data.byteLength) return undefined;
    const digit = data[offset++]!;
    value += (digit & 0x7f) * multiplier;
    if ((digit & 0x80) === 0) return { value, next: offset };
    multiplier *= 0x80;
  }
  throw new OctoError("Octo protocol variable-length integer is invalid");
}

function encodeVariableLength(length: number): Buffer {
  if (!Number.isSafeInteger(length) || length < 0 || length > FRAME_MAX_BYTES) {
    throw new OctoError("Octo protocol frame length is invalid");
  }
  const bytes: number[] = [];
  do {
    let digit = length % 0x80;
    length = Math.floor(length / 0x80);
    if (length > 0) digit |= 0x80;
    bytes.push(digit);
  } while (length > 0);
  return Buffer.from(bytes);
}

function encodeFrame(packetType: number, body: Uint8Array, flags = 0): Buffer {
  return Buffer.concat([
    Buffer.from([(packetType << 4) | flags]),
    encodeVariableLength(body.byteLength),
    Buffer.from(body),
  ]);
}

function encodeControlFrame(packetType: number): Buffer {
  return Buffer.from([packetType << 4]);
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (Array.isArray(value)) return Uint8Array.from(Buffer.concat(value.map((part) => Buffer.from(part as Uint8Array))));
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new OctoError("Octo WebSocket message is not binary");
}


function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new OctoError(`Octo response has invalid ${field}`);
  }
  return value;
}


function apiUrl(base: string, path: string): string {
  try {
    return new URL(path, base.endsWith("/") ? base : `${base}/`).toString();
  } catch {
    throw new OctoError("Octo API URL is invalid");
  }
}

function authHeaders(token: string): HeadersInit {
  return { Authorization: `Bearer ${token}` };
}

async function readBody(response: Response, maxBytes: number): Promise<Uint8Array> {
  if (!response.body) {
    const text = await response.text();
    const bytes = Buffer.from(text, "utf8");
    if (bytes.byteLength > maxBytes) throw new OctoError(`Octo response exceeds ${maxBytes} bytes`);
    return bytes;
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      if (part.value.byteLength > maxBytes - total) {
        await reader.cancel();
        throw new OctoError(`Octo response exceeds ${maxBytes} bytes`);
      }
      total += part.value.byteLength;
      chunks.push(part.value);
    }
  } finally {
    reader.releaseLock();
  }
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function decodeUtf8(bytes: Uint8Array, label: string): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new OctoError(`Octo ${label} response is not valid UTF-8`);
  }
}

function parseJson(bytes: Uint8Array, label: string): unknown {
  try {
    return JSON.parse(decodeUtf8(bytes, label)) as unknown;
  } catch (error) {
    if (error instanceof OctoError) throw error;
    throw new OctoError(`Octo ${label} response is not valid JSON`);
  }
}

function parseJsonWithInt64(bytes: Uint8Array, label: string): unknown {
  const text = decodeUtf8(bytes, label);
  try {
    // sendMessage currently returns a JSON number, which must not pass through Number.
    return JSON.parse(text.replace(/("message_id"\s*:\s*)(-?\d+)/g, "$1\"$2\"")) as unknown;
  } catch {
    throw new OctoError(`Octo ${label} response is not valid JSON`);
  }
}

async function requestWithTimeout(
  url: string,
  init: RequestInit,
  label: string,
  signal: AbortSignal | undefined,
  maxBytes: number,
): Promise<HttpResult> {
  const controller = new AbortController();
  let externallyAborted = false;
  const abortFromCaller = (): void => {
    externallyAborted = true;
    controller.abort(signal?.reason);
  };
  if (signal?.aborted) abortFromCaller();
  else signal?.addEventListener("abort", abortFromCaller, { once: true });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, HTTP_TIMEOUT_MS);
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    const body = await readBody(response, maxBytes);
    return { status: response.status, body };
  } catch (error) {
    if (error instanceof OctoError) throw error;
    if (timedOut) throw new OctoError(`Octo ${label} request timed out`);
    if (externallyAborted || signal?.aborted) throw new OctoError(`Octo ${label} request was aborted`);
    throw new OctoError(`Octo ${label} request failed`);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abortFromCaller);
  }
}

function httpStatusError(label: string, status: number): OctoError {
  const permanent = status === 401 || status === 403;
  return new OctoError(`Octo ${label} request failed with HTTP ${status}`, { status, permanent });
}

function responseUrl(value: unknown, field: string): string {
  const valueString = requiredString(value, field);
  try {
    const parsed = new URL(valueString);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      throw new Error("unsupported URL scheme");
    }
  } catch {
    throw new OctoError(`Octo response has invalid ${field}`);
  }
  return valueString;
}
function webSocketUrl(value: unknown, field: string): string {
  const valueString = requiredString(value, field);
  try {
    const parsed = new URL(valueString);
    if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") throw new Error("unsupported URL scheme");
  } catch {
    throw new OctoError(`Octo response has invalid ${field}`);
  }
  return valueString;
}

function deriveAesKey(privateKey: Uint8Array, serverPublicKey: Uint8Array): Buffer {
  const secret = sharedKey(privateKey, serverPublicKey);
  const keyHex = createHash("md5").update(Buffer.from(secret).toString("base64"), "utf8").digest("hex").slice(0, 16);
  return Buffer.from(keyHex, "utf8");
}

function decryptPayload(encryptedBase64: Uint8Array, key: Buffer, iv: Buffer): unknown {
  if (iv.byteLength !== AES_BLOCK_BYTES || key.byteLength !== AES_BLOCK_BYTES) {
    throw new OctoError("Octo message encryption parameters are invalid");
  }
  let ciphertext: Buffer;
  try {
    const encoded = decodeUtf8(encryptedBase64, "message");
    ciphertext = Buffer.from(encoded, "base64");
    if (ciphertext.byteLength === 0 || ciphertext.byteLength % AES_BLOCK_BYTES !== 0) {
      throw new Error("invalid ciphertext length");
    }
    const decipher = createDecipheriv("aes-128-cbc", key, iv);
    return JSON.parse(Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString("utf8")) as unknown;
  } catch {
    throw new OctoError("Octo message payload could not be decrypted");
  }
}

function imageInfo(header: Uint8Array): ImageInfo | undefined {
  if (header.byteLength >= 24
    && header[0] === 0x89 && header[1] === 0x50 && header[2] === 0x4e && header[3] === 0x47
    && header[4] === 0x0d && header[5] === 0x0a && header[6] === 0x1a && header[7] === 0x0a
    && header[12] === 0x49 && header[13] === 0x48 && header[14] === 0x44 && header[15] === 0x52) {
    const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
    const width = view.getUint32(16);
    const height = view.getUint32(20);
    if (width > 0 && height > 0) return { contentType: "image/png", width, height };
  }
  if (header.byteLength >= 10 && header[0] === 0x47 && header[1] === 0x49 && header[2] === 0x46
    && (header[3] === 0x38) && (header[4] === 0x37 || header[4] === 0x39) && header[5] === 0x61) {
    const view = new DataView(header.buffer, header.byteOffset, header.byteLength);
    const width = view.getUint16(6, true);
    const height = view.getUint16(8, true);
    if (width > 0 && height > 0) return { contentType: "image/gif", width, height };
  }
  if (header.byteLength >= 30 && header[0] === 0xff && header[1] === 0xd8 && header[2] === 0xff) {
    let offset = 2;
    while (offset + 8 < header.byteLength) {
      if (header[offset] !== 0xff) {
        offset += 1;
        continue;
      }
      const marker = header[offset + 1]!;
      offset += 2;
      if (marker === 0xd8 || marker === 0xd9) continue;
      if (offset + 2 > header.byteLength) break;
      const segmentLength = (header[offset]! << 8) | header[offset + 1]!;
      if (segmentLength < 2 || offset + segmentLength > header.byteLength) break;
      const isStartOfFrame = (marker >= 0xc0 && marker <= 0xc3)
        || (marker >= 0xc5 && marker <= 0xc7)
        || (marker >= 0xc9 && marker <= 0xcb)
        || (marker >= 0xcd && marker <= 0xcf);
      if (isStartOfFrame && segmentLength >= 7) {
        const height = (header[offset + 3]! << 8) | header[offset + 4]!;
        const width = (header[offset + 5]! << 8) | header[offset + 6]!;
        if (width > 0 && height > 0) return { contentType: "image/jpeg", width, height };
      }
      offset += segmentLength;
    }
  }
  if (header.byteLength >= 30 && header[0] === 0x52 && header[1] === 0x49 && header[2] === 0x46 && header[3] === 0x46
    && header[8] === 0x57 && header[9] === 0x45 && header[10] === 0x42 && header[11] === 0x50) {
    if (header[12] === 0x56 && header[13] === 0x50 && header[14] === 0x38 && header[15] === 0x20 && header.byteLength >= 30) {
      const widthBits = (header[26]! | (header[27]! << 8)) & 0x3fff;
      const heightBits = (header[28]! | (header[29]! << 8)) & 0x3fff;
      const width = widthBits + 1;
      const height = heightBits + 1;
      return { contentType: "image/webp", width, height };
    }
    if (header[12] === 0x56 && header[13] === 0x50 && header[14] === 0x38 && header[15] === 0x58 && header.byteLength >= 30) {
      const width = 1 + header[24]! + (header[25]! << 8) + (header[26]! << 16);
      const height = 1 + header[27]! + (header[28]! << 8) + (header[29]! << 16);
      return { contentType: "image/webp", width, height };
    }
    if (header[12] === 0x56 && header[13] === 0x50 && header[14] === 0x38 && header[15] === 0x4c
      && header.byteLength >= 25 && header[20] === 0x2f) {
      const width = 1 + ((header[21]! | (header[22]! << 8)) & 0x3fff);
      const height = 1 + (((header[22]! >> 6) | (header[23]! << 2) | (header[24]! << 10)) & 0x3fff);
      return { contentType: "image/webp", width, height };
    }
  }
  return undefined;
}

async function readFileHeader(path: string, maxBytes = 64 * 1024): Promise<Uint8Array> {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(maxBytes);
    const { bytesRead } = await handle.read(buffer, 0, maxBytes, 0);
    return buffer.subarray(0, bytesRead);
  } finally {
    await handle.close();
  }
}

function mentionEntities(payload: JsonRecord): Array<{ uid: string; offset: number; length: number }> {
  const mention = payload.mention;
  if (!isJsonRecord(mention) || !Array.isArray(mention.entities)) return [];
  const entities: Array<{ uid: string; offset: number; length: number }> = [];
  for (const raw of mention.entities) {
    if (!isJsonRecord(raw) || typeof raw.uid !== "string" || raw.uid.length === 0) continue;
    if (!Number.isSafeInteger(raw.offset) || !Number.isSafeInteger(raw.length)) continue;
    const offset = raw.offset as number;
    const length = raw.length as number;
    if (offset < 0 || length <= 0) continue;
    entities.push({ uid: raw.uid, offset, length });
  }
  return entities;
}

function mentionUids(payload: JsonRecord): string[] {
  const mention = payload.mention;
  if (!isJsonRecord(mention)) return [];
  const entityUids: string[] = [];
  if (Array.isArray(mention.entities)) {
    for (const raw of mention.entities) {
      if (!isJsonRecord(raw) || typeof raw.uid !== "string" || raw.uid.length === 0) continue;
      if (!entityUids.includes(raw.uid)) entityUids.push(raw.uid);
    }
  }
  if (entityUids.length > 0) return entityUids;
  const uids: string[] = [];
  if (Array.isArray(mention.uids)) {
    for (const raw of mention.uids) {
      if (typeof raw !== "string" || raw.length === 0 || uids.includes(raw)) continue;
      uids.push(raw);
    }
  }
  return uids;
}

function normalizedMessage(data: DecodedInbound, botUid: string, botName: string): InboundMessage {
  if (!(data.channelType === 1 || data.channelType === 2 || data.channelType === 5)) {
    throw new OctoError(`Octo message channel type ${data.channelType} is unsupported`);
  }
  if (!isJsonRecord(data.payload)) throw new OctoError("Octo message payload has invalid schema");
  const text = typeof data.payload.content === "string" ? data.payload.content : "";
  const supported = data.payload.type === TEXT_MESSAGE_TYPE;
  const entities = mentionEntities(data.payload);
  const mentionedUserIds = mentionUids(data.payload);
  let normalizedText = text;
  const botEntity = entities.find((entity) => entity.uid === botUid
    && entity.offset + entity.length <= text.length
    && text.slice(entity.offset, entity.offset + entity.length).startsWith("@"));
  if (botEntity) {
    normalizedText = text.slice(0, botEntity.offset) + text.slice(botEntity.offset + botEntity.length);
    if (botEntity.offset === 0) normalizedText = normalizedText.replace(/^[\s:：]+/u, "");
  } else if (mentionedUserIds.includes(botUid)) {
    const prefix = `@${botName}`;
    const next = text[prefix.length];
    if (text.startsWith(prefix) && (next === undefined || /[\s:：]/u.test(next))) {
      normalizedText = text.slice(prefix.length).replace(/^[\s:：]+/u, "");
    }
  }
  const channelId = data.channelType === 1 ? data.senderId : data.rawChannelId;
  if (channelId.length === 0) throw new OctoError("Octo group message has no channel id");
  return {
    channelId,
    channelType: data.channelType as InboundMessage["channelType"],
    messageId: data.messageId.toString(),
    senderId: data.senderId,
    text: normalizedText,
    mentionedUserIds,
    supported,
  };
}

function encodeConnect(registration: Registration, clientKey: Curve25519KeyPair): Buffer {
  const deviceId = `omp-${randomBytes(12).toString("hex")}`;
  const body = new ByteWriter()
    .byte(PROTOCOL_VERSION)
    .byte(DEVICE_FLAG)
    .string(deviceId)
    .string(registration.uid)
    .string(registration.imToken)
    .int64(BigInt(Date.now()))
    .string(Buffer.from(clientKey.public).toString("base64"))
    .finish();
  return encodeFrame(CONNECT_PACKET, body);
}

function encodeRecvAck(messageId: bigint, messageSeq: number): Buffer {
  return encodeFrame(RECVACK_PACKET, new ByteWriter().int64(messageId).int32(messageSeq).finish());
}

function contextPath(channelId: string, channelType: ChatTarget["channelType"]): string {
  if ((channelType as number) === 5) {
    const separator = channelId.indexOf("____");
    if (separator <= 0 || separator + 4 >= channelId.length) {
      throw new OctoError("Octo thread target must contain a parent and topic id");
    }
    const groupId = channelId.slice(0, separator);
    const threadId = channelId.slice(separator + 4);
    return `/v1/bot/groups/${encodeURIComponent(groupId)}/threads/${encodeURIComponent(threadId)}/md`;
  }
  return `/v1/bot/groups/${encodeURIComponent(channelId)}/md`;
}

function contextFromResponse(value: unknown): GroupContext {
  if (!isJsonRecord(value)
    || typeof value.content !== "string"
    || !Number.isSafeInteger(value.version)
    || (value.version as number) < 0
    || (typeof value.updated_at !== "string" && value.updated_at !== null)
    || typeof value.updated_by !== "string") {
    throw new OctoError("Octo group context response has invalid schema");
  }
  return {
    present: value.content.length > 0,
    content: value.content,
    version: value.version as number,
    updatedAt: value.updated_at as string | null,
    updatedBy: value.updated_by,
  };
}

function parseVersion(value: unknown): { version: number } {
  if (!isJsonRecord(value) || !Number.isSafeInteger(value.version) || (value.version as number) < 0) {
    throw new OctoError("Octo group context update response has invalid schema");
  }
  return { version: value.version as number };
}

function stringArray(value: unknown, required = false): string[] | undefined | null {
  if (value === undefined) return required ? null : undefined;
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) return null;
  return value as string[];
}

function capabilityLimit(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || (value as number) <= 0) {
    throw new OctoError("Octo card profile limits contain an invalid value");
  }
  return value as number;
}

type NormalizedCardLimits = NonNullable<ExecutionCardCapabilities["limits"]>;

function cardProfileLimits(value: unknown): NormalizedCardLimits | undefined | null {
  if (value === undefined) return undefined;
  if (!isJsonRecord(value)) return null;
  const maxNodes = capabilityLimit(value.max_nodes);
  const maxDepth = capabilityLimit(value.max_depth);
  const maxPayloadBytes = capabilityLimit(value.max_payload_bytes);
  return {
    ...(maxNodes === undefined ? {} : { maxNodes }),
    ...(maxDepth === undefined ? {} : { maxDepth }),
    ...(maxPayloadBytes === undefined ? {} : { maxPayloadBytes }),
  };
}

function cardProfileCapabilities(value: unknown): ExecutionCardCapabilities | null {
  if (!isJsonRecord(value)) return null;
  const profile = value;
  const profiles = stringArray(profile.profiles, true);
  const elements = stringArray(profile.elements);
  const actions = stringArray(profile.actions);
  if (profiles === null || profiles === undefined || elements === null || actions === null) return null;
  const config = isJsonRecord(profile.config) ? profile.config : {};
  let limits: NormalizedCardLimits | undefined | null;
  try {
    limits = cardProfileLimits(profile.limits);
  } catch (error) {
    if (error instanceof OctoError) return null;
    throw error;
  }
  if (limits === null) return null;
  const enabled = (profile.enabled === true || profile.enabled === 1)
    && config.card_enabled === true
    && config.display_enabled === true
    && config.reasoning_enabled === true;
  return {
    enabled,
    profiles,
    cardVersion: typeof profile.card_version === "string" ? profile.card_version : "",
    ...(elements === undefined ? {} : { elements }),
    ...(actions === undefined ? {} : { actions }),
    ...(limits === undefined ? {} : { limits }),
  };
}

type CardStats = {
  nodes: number;
  maxDepth: number;
};

function cardStats(value: unknown, depth = 0): CardStats {
  if (Array.isArray(value)) {
    let nodes = 0;
    let maxDepth = depth;
    for (const child of value) {
      const childStats = cardStats(child, depth);
      nodes += childStats.nodes;
      maxDepth = Math.max(maxDepth, childStats.maxDepth);
    }
    return { nodes, maxDepth };
  }
  if (value === null || typeof value !== "object") return { nodes: 0, maxDepth: depth };
  let nodes = 1;
  let maxDepth = depth + 1;
  for (const child of Object.values(value)) {
    const childStats = cardStats(child, depth + 1);
    nodes += childStats.nodes;
    maxDepth = Math.max(maxDepth, childStats.maxDepth);
  }
  return { nodes, maxDepth };
}

function cardWithinLimits(
  envelope: JsonRecord,
  limits: NormalizedCardLimits,
): boolean {
  const maxPayloadBytes = limits.maxPayloadBytes;
  if (maxPayloadBytes !== undefined
    && Buffer.byteLength(JSON.stringify(envelope), "utf8") > maxPayloadBytes) {
    return false;
  }
  const stats = cardStats(envelope);
  return (limits.maxNodes === undefined || stats.nodes <= limits.maxNodes)
    && (limits.maxDepth === undefined || stats.maxDepth <= limits.maxDepth);
}

function executionCardEnvelope(
  snapshot: ExecutionSnapshot,
  capabilities: ExecutionCardCapabilities,
): JsonRecord | null {
  const built = buildExecutionCard(snapshot, capabilities);
  if (!built) return null;
  const envelope: JsonRecord = {
    type: INTERACTIVE_CARD_MESSAGE_TYPE,
    profile: CARD_PROFILE,
    card_version: CARD_VERSION,
    card: built.card,
    plain: built.plain,
  };
  return cardWithinLimits(envelope, capabilities.limits ?? {}) ? envelope : null;
}

async function cardProfileCapabilitiesFromApi(
  apiBase: string,
  botToken: string,
  signal: AbortSignal | undefined,
): Promise<ExecutionCardCapabilities | null> {
  const response = await requestWithTimeout(
    apiUrl(apiBase, "/v1/bot/card/profile"),
    { method: "GET", headers: authHeaders(botToken) },
    "card profile",
    signal,
    MAX_HTTP_BODY_BYTES,
  );
  if (response.status === 404) return null;
  if (response.status < 200 || response.status >= 300) {
    throw httpStatusError("card profile", response.status);
  }
  try {
    return cardProfileCapabilities(parseJson(response.body, "card profile"));
  } catch (error) {
    if (error instanceof OctoError) return null;
    throw error;
  }
}


export class OctoTransport implements ChannelTransport {
  private readonly apiBase: string;
  private readonly botToken: string;
  private readonly configuredWsUrl: string | undefined;
  private readonly logger: Logger;
  private registration: Registration | undefined;
  private started = false;
  private stopped = true;
  private startPromise: Promise<void> | undefined;
  private startResolve: (() => void) | undefined;
  private startReject: ((error: Error) => void) | undefined;
  private onMessage: ((message: InboundMessage) => Promise<void>) | undefined;
  private onOnline: (() => void) | undefined;
  private currentSocket: SocketState | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private reconnectAttempt = 0;
  private inboundChain: Promise<void> = Promise.resolve();
  private cardCapabilities: ExecutionCardCapabilities | undefined;

  constructor(options: OctoTransportOptions) {
    if (!options.apiUrl || !options.botToken) throw new OctoError("Octo API URL and bot token are required");
    this.apiBase = options.apiUrl;
    this.botToken = options.botToken;
    this.configuredWsUrl = options.wsUrl;
    this.logger = options.logger ?? (() => {});
  }

  async initialize(): Promise<BotIdentity> {
    const registration = await this.registerBot();
    this.registration = registration;
    return {
      uid: registration.uid,
      name: registration.name,
      ...(registration.ownerUid === undefined ? {} : { ownerUid: registration.ownerUid }),
    };
  }

  start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void> {
    if (!this.registration) return Promise.reject(new OctoError("Octo transport must initialize before start"));
    if (this.startPromise) return this.startPromise;
    this.started = true;
    this.stopped = false;
    this.onMessage = onMessage;
    this.onOnline = onOnline;
    const deferred = Promise.withResolvers<void>();
    this.startPromise = deferred.promise;
    this.startResolve = deferred.resolve;
    this.startReject = deferred.reject;
    void this.openSocket(false);
    return deferred.promise;
  }

  async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    validateTarget(target);
    if (Buffer.byteLength(text, "utf8") > MAX_TEXT_BYTES) {
      throw new OctoError(`Octo text message exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
    }
    return this.sendMessage(target, { type: TEXT_MESSAGE_TYPE, content: text }, clientMsgNo, signal);
  }

  async editText(
    target: ChatTarget,
    messageId: string,
    text: string,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo text message id is required");
    if (Buffer.byteLength(text, "utf8") > MAX_TEXT_BYTES) {
      throw new OctoError(`Octo text message exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
    }
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/message/edit"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          message_id: messageId,
          channel_id: target.channelId,
          channel_type: target.channelType,
          content_edit: JSON.stringify({ type: TEXT_MESSAGE_TYPE, content: text }),
        }),
      },
      "message edit",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("message edit", response.status);
    }
  }

  async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    validateTarget(target);
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/typing"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
        }),
      },
      "typing",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("typing", response.status);
  }

  async sendProgressCard(
    target: ChatTarget,
    progress: ExecutionSnapshot,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    const capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal);
    this.cardCapabilities = capabilities ?? undefined;
    if (!capabilities) return null;
    const content = executionCardEnvelope(progress, capabilities);
    if (!content) return null;
    return this.sendMessage(target, content, clientMsgNo, signal);
  }

  async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ExecutionSnapshot,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo progress card message id is required");
    if (!Number.isSafeInteger(cardSeq) || cardSeq <= 0) {
      throw new OctoError("Octo progress card sequence must be a positive safe integer");
    }
    let capabilities = this.cardCapabilities;
    if (!capabilities) {
      capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal) ?? undefined;
      this.cardCapabilities = capabilities;
    }
    if (!capabilities) {
      throw new OctoError("Octo execution card profile does not support progress cards");
    }
    const content = executionCardEnvelope(progress, capabilities);
    if (!content) {
      throw new OctoError("Octo execution card profile does not support this progress card");
    }
    content.card_seq = cardSeq;
    if (progress.state === "running") content.transient = true;
    if (!cardWithinLimits(content, capabilities.limits ?? {})) {
      throw new OctoError("Octo execution card profile does not support this progress card");
    }
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/message/edit"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          message_id: messageId,
          channel_id: target.channelId,
          channel_type: target.channelType,
          content_edit: JSON.stringify(content),
        }),
      },
      "message edit",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("message edit", response.status);
    }
  }

  async getGroupContext(target: ChatTarget): Promise<GroupContext | null> {
    validateTarget(target);
    if (target.channelType === 1) return null;
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, contextPath(target.channelId, target.channelType)),
      { method: "GET", headers: authHeaders(this.botToken) },
      "group context",
      undefined,
      MAX_GROUP_CONTEXT_BYTES,
    );
    if (response.status === 404) {
      return { present: false, content: "", version: 0, updatedAt: null, updatedBy: "" };
    }
    if (response.status < 200 || response.status >= 300) throw httpStatusError("group context", response.status);
    return contextFromResponse(parseJson(response.body, "group context"));
  }

  async updateGroupContext(target: ChatTarget, content: string, signal?: AbortSignal): Promise<{ version: number }> {
    validateTarget(target);
    if (target.channelType === 1) throw new OctoError("DM does not support group context updates");
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, contextPath(target.channelId, target.channelType)),
      {
        method: "PUT",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({ content }),
      },
      "group context update",
      signal,
      MAX_GROUP_CONTEXT_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("group context update", response.status);
    return parseVersion(parseJson(response.body, "group context update"));
  }

  async sendFile(
    target: ChatTarget,
    path: string,
    kind: "image" | "file",
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    validateTarget(target);
    if (kind !== "image" && kind !== "file") {
      throw new OctoError("Octo file kind is unsupported", { permanent: true });
    }
    const file = await stat(path);
    if (!file.isFile()) throw new OctoError("Octo file path is not a regular file", { permanent: true });
    if (!Number.isSafeInteger(file.size) || file.size < 0) throw new OctoError("Octo file size is invalid", { permanent: true });
    const name = basename(path);
    if (name.length === 0) throw new OctoError("Octo file name is empty", { permanent: true });
    const header = await readFileHeader(path);
    const detected = imageInfo(header);
    if (kind === "image" && !detected) {
      throw new OctoError("Octo image bytes have an unsupported MIME format or invalid dimensions", { permanent: true });
    }
    const contentType = kind === "image" ? detected!.contentType : "application/octet-stream";
    const presigned = await this.requestPresigned(name, file.size, contentType, signal);
    await this.uploadFile(path, file.size, presigned, signal);
    const payload: JsonRecord = {
      type: kind === "image" ? IMAGE_MESSAGE_TYPE : FILE_MESSAGE_TYPE,
      url: presigned.downloadUrl,
      name,
      size: file.size,
    };
    if (kind === "image") {
      payload.width = detected!.width;
      payload.height = detected!.height;
    }
    return this.sendMessage(target, payload, clientMsgNo, signal);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.cardCapabilities = undefined;
    this.started = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const socket = this.currentSocket;
    this.currentSocket = undefined;
    if (socket) await this.closeSocketAndWait(socket);
    const reject = this.startReject;
    this.startReject = undefined;
    this.startResolve = undefined;
    this.startPromise = undefined;
    if (reject) reject(new OctoError("Octo transport stopped"));
  }

  private async registerBot(): Promise<Registration> {
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/register"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: "{}",
      },
      "bot register",
      undefined,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("bot register", response.status);
    const value = parseJson(response.body, "bot register");
    if (!isJsonRecord(value)) throw new OctoError("Octo bot register response has invalid schema");
    const uid = requiredString(value.robot_id, "robot_id");
    const name = typeof value.name === "string" && value.name.length > 0 ? value.name : uid;
    const imToken = requiredString(value.im_token, "im_token");
    const ownerUid = typeof value.owner_uid === "string" && value.owner_uid.trim().length > 0
      ? value.owner_uid.trim()
      : undefined;
    const advertisedWsUrl = typeof value.ws_url === "string" ? value.ws_url : undefined;
    const wsUrl = this.configuredWsUrl ?? requiredString(advertisedWsUrl, "ws_url");
    webSocketUrl(wsUrl, "ws_url");
    return {
      uid,
      name,
      imToken,
      wsUrl,
      ...(ownerUid === undefined ? {} : { ownerUid }),
    };
  }

  private async requestPresigned(
    name: string,
    size: number,
    contentType: string,
    signal: AbortSignal | undefined,
  ): Promise<PresignedUpload> {
    const url = new URL(apiUrl(this.apiBase, "/v1/bot/upload/presigned"));
    url.searchParams.set("filename", name);
    url.searchParams.set("fileSize", String(size));
    url.searchParams.set("contentType", contentType);
    const response = await requestWithTimeout(
      url.toString(),
      { method: "GET", headers: authHeaders(this.botToken) },
      "presigned upload",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("presigned upload", response.status);
    const value = parseJson(response.body, "presigned upload");
    if (!isJsonRecord(value)) throw new OctoError("Octo presigned upload response has invalid schema");
    return {
      uploadUrl: responseUrl(value.uploadUrl, "uploadUrl"),
      downloadUrl: responseUrl(value.downloadUrl, "downloadUrl"),
      contentType: requiredString(value.contentType, "contentType"),
      contentDisposition: typeof value.contentDisposition === "string" ? value.contentDisposition : undefined,
    };
  }

  private async uploadFile(
    path: string,
    size: number,
    presigned: PresignedUpload,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const stream = createReadStream(path);
    const headers: Record<string, string> = {
      "Content-Type": presigned.contentType,
      "Content-Length": String(size),
      ...(presigned.contentDisposition ? { "Content-Disposition": presigned.contentDisposition } : {}),
    };
    const response = await requestWithTimeout(
      presigned.uploadUrl,
      {
        method: "PUT",
        headers,
        body: Readable.toWeb(stream) as unknown as BodyInit,
        duplex: "half",
      } as RequestInit & { duplex: "half" },
      "presigned upload transfer",
      signal,
      MAX_HTTP_BODY_BYTES,
    ).catch((error: unknown) => {
      stream.destroy();
      throw error;
    });
    if (response.status < 200 || response.status >= 300) throw httpStatusError("presigned upload transfer", response.status);
  }

  private async sendMessage(
    target: ChatTarget,
    payload: JsonRecord,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/sendMessage"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
          payload,
          client_msg_no: clientMsgNo,
        }),
      },
      "send message",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("send message", response.status);
    const value = parseJsonWithInt64(response.body, "send message");
    if (!isJsonRecord(value)) throw new OctoError("Octo send message response has invalid schema");
    if (value.message_id === undefined || value.message_id === null) return {};
    if (typeof value.message_id !== "string" && typeof value.message_id !== "number") {
      throw new OctoError("Octo send message response has invalid message_id");
    }
    return { messageId: String(value.message_id) };
  }

  private async openSocket(reconnecting: boolean): Promise<void> {
    if (this.stopped || !this.registration) return;
    if (reconnecting) {
      try {
        this.registration = await this.registerBot();
        if (this.stopped) return;
      } catch (error) {
        if (this.isPermanent(error)) {
          this.failPermanently(error);
          return;
        }
        this.logger(this.safeError(error, "Octo re-registration failed"));
        this.scheduleReconnect();
        return;
      }
    }
    const registration = this.registration;
    let ws: WebSocket;
    try {
      ws = new WebSocket(registration.wsUrl);
    } catch (error) {
      this.logger(this.safeError(error, "Octo WebSocket connection failed"));
      this.scheduleReconnect();
      return;
    }
    ws.binaryType = "arraybuffer";
    const state: SocketState = {
      socket: ws,
      decoder: new FrameDecoder(),
      clientKey: generateKeyPair(randomBytes(32)),
      authenticated: false,
      serverVersion: PROTOCOL_VERSION,
      aesKey: undefined,
      aesIv: undefined,
      handshakeTimer: undefined,
      pingTimer: undefined,
      pongTimer: undefined,
      closed: false,
    };
    this.currentSocket = state;
    state.handshakeTimer = setTimeout(() => {
      this.logger("Octo WebSocket handshake timed out");
      this.closeSocket(state);
    }, HANDSHAKE_TIMEOUT_MS);
    ws.on("open", () => {
      if (this.currentSocket !== state || this.stopped) return;
      try {
        ws.send(encodeConnect(registration, state.clientKey));
      } catch (error) {
        this.logger(this.safeError(error, "Octo WebSocket handshake failed"));
        this.closeSocket(state);
      }
    });
    ws.on("message", (data) => {
      if (this.currentSocket !== state || this.stopped) return;
      try {
        const frames = state.decoder.push(toBytes(data));
        for (const frame of frames) this.handleFrame(state, frame);
      } catch (error) {
        this.logger(this.safeError(error, "Octo WebSocket frame failed"));
        this.closeSocket(state);
      }
    });
    ws.on("error", (error) => {
      if (this.currentSocket === state && !this.stopped) this.logger(this.safeError(error, "Octo WebSocket error"));
    });
    ws.on("close", () => {
      this.finalizeSocket(state);
    });
  }
  private handleFrame(state: SocketState, frame: WireFrame): void {
    switch (frame.packetType) {
      case CONNACK_PACKET:
        this.handleConnack(state, frame);
        return;
      case RECV_PACKET:
        if (!state.authenticated || !state.aesKey || !state.aesIv) return;
        this.handleRecv(state, frame.body);
        return;
      case PING_PACKET:
        if (state.socket.readyState === WebSocket.OPEN) state.socket.send(encodeControlFrame(PONG_PACKET));
        return;
      case PONG_PACKET:
        clearTimeout(state.pongTimer);
        state.pongTimer = undefined;
        return;
      case DISCONNECT_PACKET:
        this.handleDisconnect(state, frame.body);
        return;
      default:
        throw new OctoError(`Octo WebSocket packet type ${frame.packetType} is unsupported`);
    }
  }

  private handleConnack(state: SocketState, frame: WireFrame): void {
    if (state.authenticated) throw new OctoError("Octo WebSocket sent duplicate CONNACK");
    const body = new ByteReader(frame.body);
    const hasServerVersion = (frame.flags & 0x01) !== 0;
    const serverVersion = hasServerVersion ? body.byte() : 0;
    void body.int64();
    const reason = body.byte();
    if (reason !== 1) {
      const message = reason === 0
        ? "Octo WebSocket was kicked by the server"
        : `Octo WebSocket authentication failed with reason ${reason}`;
      this.failPermanently(new OctoError(message, { permanent: true }));
      return;
    }
    const serverKeyText = body.string();
    const saltText = body.string();
    if (serverVersion >= 4) void body.int64();
    let serverKey: Uint8Array;
    try {
      serverKey = Uint8Array.from(Buffer.from(serverKeyText, "base64"));
    } catch {
      throw new OctoError("Octo CONNACK server key is invalid");
    }
    if (serverKey.byteLength !== 32) throw new OctoError("Octo CONNACK server key has invalid length");
    const iv = Buffer.from(saltText, "utf8");
    if (iv.byteLength < AES_BLOCK_BYTES) throw new OctoError("Octo CONNACK salt has invalid length");
    state.serverVersion = serverVersion;
    state.aesKey = deriveAesKey(state.clientKey.private, serverKey);
    state.aesIv = iv.subarray(0, AES_BLOCK_BYTES);
    state.authenticated = true;
    clearTimeout(state.handshakeTimer);
    state.handshakeTimer = undefined;
    clearInterval(state.pingTimer);
    state.pingTimer = setInterval(() => {
      if (state.socket.readyState !== WebSocket.OPEN) {
        this.closeSocket(state);
        return;
      }
      if (state.pongTimer !== undefined) {
        this.logger("Octo WebSocket heartbeat timed out");
        this.closeSocket(state);
        return;
      }
      state.pongTimer = setTimeout(() => {
        state.pongTimer = undefined;
        this.closeSocket(state);
      }, PONG_TIMEOUT_MS);
      state.socket.send(encodeControlFrame(PING_PACKET));
    }, PING_INTERVAL_MS);
    this.reconnectAttempt = 0;
    const resolve = this.startResolve;
    if (resolve) {
      this.startResolve = undefined;
      this.startReject = undefined;
      resolve();
    }
    try {
      this.onOnline?.();
    } catch (error) {
      this.logger(this.safeError(error, "Octo online callback failed"));
    }
  }
  private handleDisconnect(state: SocketState, frameBody: Uint8Array): void {
    let reason = 0;
    try {
      if (frameBody.byteLength > 0) reason = new ByteReader(frameBody).byte();
    } catch {
      reason = 0;
    }
    const message = reason === 0
      ? "Octo WebSocket was kicked by the server"
      : `Octo WebSocket disconnected with reason ${reason}`;
    this.failPermanently(new OctoError(message, { permanent: true }));
  }

  private handleRecv(state: SocketState, frameBody: Uint8Array): void {
    const reader = new ByteReader(frameBody);
    try {
      const setting = reader.byte();
      void reader.string();
      const senderId = reader.string();
      const rawChannelId = reader.string();
      const channelType = reader.byte();
      if (channelType === 1
        && rawChannelId.includes("@")
        && !rawChannelId.includes(this.registration?.uid ?? "")) {
        this.logger("Octo DM channel id failed the bot identity check");
        return;
      }
      if (state.serverVersion >= 3) void reader.int32();
      void reader.string();
      const messageId = reader.int64();
      const messageSeq = reader.int32();
      void reader.int32();
      if ((setting & 0x08) !== 0) void reader.string();
      const payload = decryptPayload(reader.bytes(), state.aesKey!, state.aesIv!);
      const normalized = normalizedMessage(
        { setting, senderId, rawChannelId, channelType, messageId, messageSeq, payload },
        this.registration?.uid ?? "",
        this.registration?.name ?? "",
      );
      const handler = this.onMessage;
      if (!handler) return;
      this.inboundChain = this.inboundChain.then(async () => {
        try {
          await handler(normalized);
          if (state.socket.readyState === WebSocket.OPEN && this.currentSocket === state) {
            state.socket.send(encodeRecvAck(messageId, messageSeq));
          }
        } catch (error) {
          this.logger(this.safeError(error, `Octo inbound handler failed for message ${messageId.toString()}`));
        }
      });
    } catch (error) {
      this.logger(error instanceof OctoError ? error.message : "Octo inbound message is invalid");
    }
  }

  private closeSocket(state: SocketState): void {
    if (state.closed) return;
    this.clearSocketTimers(state);
    try {
      if (state.socket.readyState === WebSocket.OPEN || state.socket.readyState === WebSocket.CONNECTING) {
        state.socket.close();
      } else {
        this.finalizeSocket(state);
      }
    } catch {
      this.finalizeSocket(state);
    }
  }
  private async closeSocketAndWait(state: SocketState): Promise<void> {
    if (state.closed || state.socket.readyState === WebSocket.CLOSED) {
      state.closed = true;
      return;
    }
    const deferred = Promise.withResolvers<void>();
    let done = false;
    let timer: NodeJS.Timeout | undefined;
    const finish = (): void => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      state.socket.off("close", finish);
      deferred.resolve();
    };
    state.socket.once("close", finish);
    this.closeSocket(state);
    if (!done) {
      timer = setTimeout(() => {
        state.socket.terminate();
        this.finalizeSocket(state);
        finish();
      }, 1_000);
      await deferred.promise;
    }
  }

  private finalizeSocket(state: SocketState): void {
    if (state.closed) return;
    state.closed = true;
    this.clearSocketTimers(state);
    if (this.currentSocket !== state) return;
    this.currentSocket = undefined;
    if (!this.stopped) this.scheduleReconnect();
  }

  private clearSocketTimers(state: SocketState): void {
    clearTimeout(state.handshakeTimer);
    clearInterval(state.pingTimer);
    clearTimeout(state.pongTimer);
    state.handshakeTimer = undefined;
    state.pingTimer = undefined;
    state.pongTimer = undefined;
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) return;
    const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** Math.min(this.reconnectAttempt, 8));
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.openSocket(true);
    }, delay);
  }

  private failPermanently(error: unknown): void {
    const octoError = error instanceof OctoError ? error : new OctoError("Octo transport authentication failed", { permanent: true });
    this.stopped = true;
    this.cardCapabilities = undefined;
    this.started = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    const state = this.currentSocket;
    this.currentSocket = undefined;
    if (state) this.closeSocket(state);
    const reject = this.startReject;
    this.startReject = undefined;
    this.startResolve = undefined;
    if (reject) reject(octoError);
    else this.logger(octoError.message);
  }

  private isPermanent(error: unknown): boolean {
    return error instanceof OctoError && error.permanent;
  }

  private safeError(error: unknown, fallback: string): string {
    if (error instanceof OctoError) return error.message;
    return fallback;
  }
}

function validateTarget(target: ChatTarget): void {
  if (!target || typeof target.channelId !== "string" || target.channelId.length === 0) {
    throw new OctoError("Octo target channel id is required");
  }
  const channelType = target.channelType as number;
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) {
    throw new OctoError("Octo target channel type is unsupported");
  }
}

export function createOctoTransport(options: OctoTransportOptions): ChannelTransport {
  return new OctoTransport(options);
}
