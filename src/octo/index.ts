import { createDecipheriv, createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import { basename, dirname, extname } from "node:path";
import { Readable } from "node:stream";
import { WebSocket } from "ws";
import { generateKeyPair, sharedKey } from "curve25519-js";
import type {
  AttachmentDownload,
  BotCardAction,
  BotEvent,
  BotEventFetch,
  BotIdentity,
  CardFrame,
  CardProfile,
  ChannelTransport,
  ChatTarget,
  DeleteThreadOptions,
  GroupContext,
  GroupInfo,
  GroupInfoPatch,
  GroupMember,
  GroupMentionPref,
  HistoryFetch,
  HistoryMessage,
  InboundAttachment,
  InboundAttachmentKind,
  InboundMessage,
  InboundQuote,
  SendReceipt,
  ThreadSummary,
} from "../contracts.ts";
import { ATTACHMENT_DOWNLOAD_TIMEOUT_MS, AttachmentError } from "../attachments.ts";
import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import {
  buildExecutionCard,
  type ExecutionCardCapabilities,
  type ExecutionSnapshot,
} from "../execution-card.ts";
import { threadParentGroupId } from "../thread-target.ts";

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
  timestamp: number;
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
const GIF_MESSAGE_TYPE = 3;
const VOICE_MESSAGE_TYPE = 4;
const VIDEO_MESSAGE_TYPE = 5;
const LOCATION_MESSAGE_TYPE = 6;
const CONTACT_CARD_MESSAGE_TYPE = 7;
const FILE_MESSAGE_TYPE = 8;
/** Not an octo-lib enum value, but clients do send files as type 9. */
const FILE_ALIAS_MESSAGE_TYPE = 9;
const MERGE_FORWARD_MESSAGE_TYPE = 11;
const RICH_TEXT_MESSAGE_TYPE = 14;
const INTERACTIVE_CARD_MESSAGE_TYPE = 17;
/**
 * Types whose payload text *is* the message input: an empty text is a legitimate, if
 * contentless, turn (`@bot` with nothing else), so these stay supported and run normally
 * instead of being reported as an unsupported message type. Types outside this set carry
 * their input as media, so they stay supported only when something was actually extracted.
 */
const TEXT_SHAPED_MESSAGE_TYPES: ReadonlySet<number> = new Set([
  TEXT_MESSAGE_TYPE,
  RICH_TEXT_MESSAGE_TYPE,
  INTERACTIVE_CARD_MESSAGE_TYPE,
]);
const RICH_TEXT_IMAGE_BLOCK = "image";
const RICH_TEXT_IMAGE_PLACEHOLDER = "[图片]";
/** 交互卡片 `plain` 缺失时的降级文本，与 octo-server Decision 8 的 `[卡片]` 占位符一致。 */
const CARD_PLACEHOLDER = "[卡片]";
const MERGE_FORWARD_HEADER = "[合并转发]";
const MAX_MERGE_FORWARD_MESSAGES = 30;
const MAX_MERGE_FORWARD_CODE_POINTS = 4_000;
const MAX_RICH_TEXT_CODE_POINTS = 2_000;
/** 引用块只承载文本，超出部分截断：引用是上下文，不是第二份正文。 */
const MAX_QUOTED_CODE_POINTS = 2_000;
/** Random bytes appended to the temp file a download writes before renaming it into place. */
const ATTACHMENT_PART_BYTES = 6;
/** Audio MIME types the transcription endpoint understands; anything else goes as opaque bytes. */
const AUDIO_CONTENT_TYPES: Record<string, string> = {
  ".amr": "audio/amr",
  ".m4a": "audio/mp4",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
  ".ogg": "audio/ogg",
  ".aac": "audio/aac",
};
const CARD_PROFILE = "octo/v1";
const CARD_VERSION = "1.5";
const MAX_TEXT_BYTES = 3_000;
/** Transcribing anything larger is pointless and would only buffer a big file in memory. */
const MAX_TRANSCRIBE_BYTES = 20 * 1024 * 1024;
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
  /** Server error identifier from the response body (`error.code`), when the body carries one. */
  readonly code: string | undefined;

  constructor(
    message: string,
    options: { status?: number; permanent?: boolean; code?: string } = {},
  ) {
    super(message);
    this.name = "OctoError";
    this.status = options.status;
    this.permanent = options.permanent ?? false;
    this.code = options.code;
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

type RequestDeadline = {
  signal: AbortSignal;
  /** `true` when this deadline fired rather than the caller cancelling. */
  timedOut: () => boolean;
  /** `true` when the caller's signal was aborted before or during the request. */
  aborted: () => boolean;
  release: () => void;
};

/**
 * Abort signal for one HTTP request: the caller's cancellation plus a deadline, keeping
 * track of which of the two fired so the failure can be classified. Attachment downloads
 * use it with their own timeout instead of `requestWithTimeout`, which buffers whole bodies.
 */
function requestDeadline(caller: AbortSignal | undefined, timeoutMs: number): RequestDeadline {
  const controller = new AbortController();
  let externalAbort = false;
  const abortFromCaller = (): void => {
    externalAbort = true;
    controller.abort(caller?.reason);
  };
  if (caller?.aborted) abortFromCaller();
  else caller?.addEventListener("abort", abortFromCaller, { once: true });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  return {
    signal: controller.signal,
    timedOut: () => timedOut,
    aborted: () => externalAbort || caller?.aborted === true,
    release: () => {
      clearTimeout(timer);
      caller?.removeEventListener("abort", abortFromCaller);
    },
  };
}

/** Size of an attachment an earlier delivery already stored; `0` when there is none. */
async function storedBytes(path: string): Promise<number> {
  try {
    const info = await stat(path);
    return info.isFile() && Number.isSafeInteger(info.size) && info.size > 0 ? info.size : 0;
  } catch {
    return 0;
  }
}

async function requestWithTimeout(
  url: string,
  init: RequestInit,
  label: string,
  signal: AbortSignal | undefined,
  maxBytes: number,
  timeoutMs = HTTP_TIMEOUT_MS,
): Promise<HttpResult> {
  const deadline = requestDeadline(signal, timeoutMs);
  try {
    const response = await fetch(url, { ...init, signal: deadline.signal });
    const body = await readBody(response, maxBytes);
    return { status: response.status, body };
  } catch (error) {
    if (error instanceof OctoError) throw error;
    if (deadline.timedOut()) throw new OctoError(`Octo ${label} request timed out`);
    if (deadline.aborted()) throw new OctoError(`Octo ${label} request was aborted`);
    throw new OctoError(`Octo ${label} request failed`);
  } finally {
    deadline.release();
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

type MentionEntity = { uid: string; offset: number; length: number };

function mentionEntities(payload: JsonRecord): MentionEntity[] {
  const mention = payload.mention;
  if (!isJsonRecord(mention) || !Array.isArray(mention.entities)) return [];
  const entities: MentionEntity[] = [];
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

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

/**
 * Removes the bot mention wherever the client placed it. Group clients render
 * `@bot /command args` and `/command args @bot` alike, and a mention may also
 * arrive as a bare uid list without spans.
 */
function stripBotMention(
  text: string,
  botUid: string,
  botName: string,
  entities: readonly MentionEntity[],
  mentionedUserIds: readonly string[],
): string {
  const spans = entities
    .filter((entity) => entity.uid === botUid
      && entity.offset + entity.length <= text.length
      && text.slice(entity.offset, entity.offset + entity.length).startsWith("@"))
    .sort((left, right) => right.offset - left.offset);
  if (spans.length > 0) {
    let stripped = text;
    for (const span of spans) {
      stripped = stripped.slice(0, span.offset) + stripped.slice(span.offset + span.length);
    }
    return trimMentionSeam(
      stripped,
      spans.some((span) => span.offset === 0),
      spans.some((span) => span.offset + span.length === text.length),
    );
  }
  if (!mentionedUserIds.includes(botUid)) return text;
  const tokens = [botName, botUid].filter((value) => value.length > 0).map(escapeRegExp);
  if (tokens.length === 0) return text;
  const pattern = new RegExp(`(^|\\s)@(?:${tokens.join("|")})(?=$|[\\s:：])`, "gu");
  let touchesStart = false;
  let touchesEnd = false;
  const stripped = text.replace(pattern, (match: string, lead: string, offset: number) => {
    if (offset === 0) touchesStart = true;
    if (offset + match.length === text.length) touchesEnd = true;
    return lead;
  });
  return stripped === text ? text : trimMentionSeam(stripped, touchesStart, touchesEnd);
}

function trimMentionSeam(text: string, touchesStart: boolean, touchesEnd: boolean): string {
  let trimmed = text;
  if (touchesStart) trimmed = trimmed.replace(/^[\s:：]+/u, "");
  if (touchesEnd) trimmed = trimmed.replace(/\s+$/u, "");
  return trimmed;
}

/**
 * Sequences and second timestamps arrive as unsigned 32-bit integers. Values that cannot
 * take part in history accounting (missing/zero) collapse to the `0` unknown sentinel.
 */
function knownIntOrZero(value: number): number {
  return Number.isSafeInteger(value) && value > 0 ? value : 0;
}

type ExtractedContent = {
  text: string;
  attachments: InboundAttachment[];
};

/**
 * Absolute http/https URL, or `undefined` when the value is missing, relative, or uses
 * another scheme. Octo falls back to a relative `file/preview/...` path when it cannot
 * mint a download URL, and that route needs a login the bot does not have — so relative
 * references are unusable, never resolved against a guessed base.
 */
function absoluteHttpUrl(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length === 0) return undefined;
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return undefined;
  } catch {
    return undefined;
  }
  return value;
}

function textField(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/** Client-declared numbers are advisory: anything not finite and positive counts as unknown. */
function declaredNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}

/** Types 8/9 carry the name and its extension in separate fields on some clients. */
function fileName(payload: JsonRecord): string {
  const name = textField(payload.name);
  const extension = textField(payload.extension).replace(/^\./u, "").trim();
  if (name.length === 0 || !/^[A-Za-z0-9]{1,12}$/u.test(extension)) return name;
  return name.toLowerCase().endsWith(`.${extension.toLowerCase()}`) ? name : `${name}.${extension}`;
}

/** Types 2/8/9 carry the user's words in `caption` and fall back to `content`. */
function captionOrContent(payload: JsonRecord): string {
  const caption = textField(payload.caption);
  return caption.length > 0 ? caption : textField(payload.content);
}

/** Voice and video clients disagree on the duration field, so the first usable one wins. */
function declaredDuration(payload: JsonRecord): number {
  for (const field of ["second", "timeTrad", "duration"] as const) {
    const seconds = declaredNumber(payload[field]);
    if (seconds > 0) return seconds;
  }
  return 0;
}

/**
 * Records one attachment candidate. The URL is taken verbatim: whether it is usable is
 * decided by `normalizedMessage` (live path) and `downloadAttachment` (fetch path), so
 * this table only ever answers "what did the payload point at".
 */
function pushAttachment(
  list: InboundAttachment[],
  url: unknown,
  kind: InboundAttachmentKind,
  fields: { name?: unknown; size?: unknown; durationSeconds?: number } = {},
): void {
  if (typeof url !== "string" || url.length === 0) return;
  list.push({
    url,
    kind,
    name: textField(fields.name),
    size: declaredNumber(fields.size),
    durationSeconds: declaredNumber(fields.durationSeconds),
  });
}

/** Type 6 coordinates live under two naming conventions across clients. */
function locationText(payload: JsonRecord): string {
  const latitude = scalarText(payload.latitude ?? payload.lat);
  const longitude = scalarText(payload.longitude ?? payload.lng ?? payload.lon);
  const details = [scalarText(payload.address), scalarText(payload.title)]
    .filter((part) => part.length > 0);
  const coordinates = latitude.length === 0 && longitude.length === 0 ? "" : `${latitude},${longitude}`;
  const line = coordinates.length === 0 ? "" : `[位置信息: ${coordinates}]`;
  return [line, ...details].filter((part) => part.length > 0).join(" ");
}

/** Type 7 renders one line so the agent sees the contact without a download. */
function contactText(payload: JsonRecord): string {
  const name = textField(payload.name);
  const uid = textField(payload.uid);
  if (name.length === 0 && uid.length === 0) return "";
  if (name.length === 0) return `[名片: ${uid}]`;
  if (uid.length === 0) return `[名片: ${name}]`;
  return `[名片: ${name} (${uid})]`;
}

/** Coordinates may arrive as numbers or as strings; both keep their written form. */
function scalarText(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return "";
}

/** Counts code points rather than UTF-16 units, so a cap means what it says. */
function codePointLength(text: string): number {
  let length = text.length;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    const next = text.charCodeAt(index + 1);
    if (code >= 0xd800 && code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
      length -= 1;
      index += 1;
    }
  }
  return length;
}

/** Truncates at a code point boundary without splitting a surrogate pair. */
function truncateCodePoints(text: string, maximum: number): string {
  let seen = 0;
  let offset = 0;
  for (const character of text) {
    if (seen >= maximum) return text.slice(0, offset);
    seen += 1;
    offset += character.length;
  }
  return text;
}

/** Media that a forwarded chat or an unreadable block cannot deliver is named, not fetched. */
function mediaPlaceholder(type: unknown): string {
  switch (type) {
    case IMAGE_MESSAGE_TYPE:
    case GIF_MESSAGE_TYPE:
      return "[图片]";
    case VOICE_MESSAGE_TYPE:
      return "[语音]";
    case VIDEO_MESSAGE_TYPE:
      return "[视频]";
    case FILE_MESSAGE_TYPE:
    case FILE_ALIAS_MESSAGE_TYPE:
      return "[文件]";
    default:
      return "";
  }
}

/**
 * Type 11 carries a forwarded chat: a header line plus one line per inner message,
 * expanded exactly one level deep and bounded in message count and total text. Inner
 * media never downloads — it stays a placeholder — and a forward inside a forward is
 * rendered as the header alone instead of recursing.
 */
function mergeForwardText(payload: JsonRecord): string {
  const names = new Map<string, string>();
  const users: unknown[] = Array.isArray(payload.users) ? payload.users : [];
  for (const raw of users) {
    if (!isJsonRecord(raw)) continue;
    const uid = textField(raw.uid);
    const name = textField(raw.name);
    if (uid.length > 0 && name.length > 0) names.set(uid, name);
  }
  const messages: unknown[] = Array.isArray(payload.msgs) ? payload.msgs : [];
  const lines: string[] = [MERGE_FORWARD_HEADER];
  let budget = MAX_MERGE_FORWARD_CODE_POINTS;
  let position = 0;
  const limit = Math.min(messages.length, MAX_MERGE_FORWARD_MESSAGES);
  for (; position < limit; position += 1) {
    const raw = messages[position];
    const line = isJsonRecord(raw) ? forwardedLine(raw, names) : undefined;
    if (line === undefined) continue;
    const cost = codePointLength(line) + 1;
    if (cost > budget) break;
    budget -= cost;
    lines.push(line);
  }
  const omitted = messages.length - position;
  if (omitted > 0) lines.push(`…（其余 ${omitted} 条省略）`);
  return lines.join("\n");
}

/** One inner line of a forwarded chat: `sender: text`. */
function forwardedLine(message: JsonRecord, names: ReadonlyMap<string, string>): string | undefined {
  const payload = message.payload;
  if (!isJsonRecord(payload)) return undefined;
  const text = forwardedMessageText(payload);
  if (text.length === 0) return undefined;
  const sender = textField(message.from_uid);
  const label = names.get(sender) ?? sender;
  return label.length === 0 ? text : `${label}: ${text}`;
}

/** Inner text of one forwarded message; nested media and forwards never expand further. */
function forwardedMessageText(payload: JsonRecord): string {
  if (payload.type === MERGE_FORWARD_MESSAGE_TYPE) return MERGE_FORWARD_HEADER;
  const content = messageContent(payload);
  const text = content === undefined ? "" : content.text.trim();
  return text.length > 0 ? text : mediaPlaceholder(payload.type);
}

/**
 * Type 14 keeps the server-generated `plain` as the authoritative text, while the blocks
 * still decide which images and files travel with the message. Without `plain` (old
 * clients, or a client-sent payload the server has not rewritten) the blocks are
 * flattened with `[图片]` standing in for every embedded image.
 */
function richTextContent(payload: JsonRecord): ExtractedContent {
  const attachments: InboundAttachment[] = [];
  const parts: string[] = [];
  if (Array.isArray(payload.content)) {
    const blocks: unknown[] = payload.content;
    for (const raw of blocks) {
      if (!isJsonRecord(raw)) continue;
      if (raw.type === RICH_TEXT_IMAGE_BLOCK) {
        pushAttachment(attachments, raw.url, "image", { name: raw.name, size: raw.size });
        parts.push(RICH_TEXT_IMAGE_PLACEHOLDER);
        continue;
      }
      // 未知块按 octo-lib 的宽容解析处理：有 text 就取 text，其余忽略（写入端 write-strict
      // 会拒绝未知块类型，所以这里只可能是前向兼容的降级路径）。
      const text = textField(raw.text);
      if (text.length > 0) parts.push(text);
    }
  } else {
    // Before blocks existed, `content` was the whole text.
    const legacy = textField(payload.content);
    if (legacy.length > 0) parts.push(legacy);
  }
  const plain = textField(payload.plain);
  return {
    text: plain.length > 0 ? plain : truncateCodePoints(parts.join(""), MAX_RICH_TEXT_CODE_POINTS),
    attachments,
  };
}

/**
 * Type table for one inbound payload: the text and media a message contributes. The text
 * is exactly what the client or server wrote (no mention stripping here — that applies to
 * the extracted text afterwards), and `undefined` means the type yields no usable input,
 * which is what keeps the "unsupported message type" refusal for stickers and notifications.
 */
function messageContent(payload: JsonRecord): ExtractedContent | undefined {
  const attachments: InboundAttachment[] = [];
  switch (payload.type) {
    case TEXT_MESSAGE_TYPE:
      return { text: textField(payload.content), attachments };
    case IMAGE_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "image", { name: payload.name, size: payload.size });
      return { text: captionOrContent(payload), attachments };
    case GIF_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "image", { name: payload.name });
      return { text: "", attachments };
    case VOICE_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "audio", {
        name: payload.name,
        durationSeconds: declaredDuration(payload),
      });
      return { text: "", attachments };
    case VIDEO_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "video", {
        name: payload.name,
        size: payload.size,
        durationSeconds: declaredDuration(payload),
      });
      return { text: "", attachments };
    case LOCATION_MESSAGE_TYPE:
      return { text: locationText(payload), attachments };
    case CONTACT_CARD_MESSAGE_TYPE:
      return { text: contactText(payload), attachments };
    case FILE_MESSAGE_TYPE:
    case FILE_ALIAS_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "file", { name: fileName(payload), size: payload.size });
      return { text: captionOrContent(payload), attachments };
    case MERGE_FORWARD_MESSAGE_TYPE:
      return { text: mergeForwardText(payload), attachments };
    case RICH_TEXT_MESSAGE_TYPE:
      return richTextContent(payload);
    case INTERACTIVE_CARD_MESSAGE_TYPE: {
      // 服务端保证 `plain` 非空；真为空时给占位符，避免把空正文交给 Agent。
      const plain = textField(payload.plain);
      return { text: plain.length > 0 ? plain : CARD_PLACEHOLDER, attachments };
    }
    default:
      return undefined;
  }
}

/**
 * A reply carries the quoted message inline: `payload.reply.payload` is a full payload of
 * its own (the real client sends the original type, blocks, and `plain`), so the same type
 * table reads it. Only depth 1 is read — a quoted message that itself quotes something
 * contributes just its own content — and the quoted text is bounded like rich text.
 */
function quotedMessage(
  payload: JsonRecord,
): { quote: InboundQuote; attachments: InboundAttachment[] } | null {
  const reply = payload.reply;
  if (!isJsonRecord(reply)) return null;
  const quotedPayload = reply.payload;
  if (!isJsonRecord(quotedPayload)) return null;
  const content = messageContent(quotedPayload);
  if (content === undefined) return null;
  // 引用里的相对 URL 与正文一样不可用：它们不参与「这条引用有没有内容」的判定。
  const attachments = content.attachments.filter(
    (attachment) => absoluteHttpUrl(attachment.url) !== undefined,
  );
  const text = truncateCodePoints(content.text, MAX_QUOTED_CODE_POINTS).trim();
  if (text.length === 0 && attachments.length === 0) return null;
  const senderName = reply.from_name;
  return {
    quote: { senderName: typeof senderName === "string" ? senderName : "", text },
    attachments,
  };
}

function normalizedMessage(data: DecodedInbound, botUid: string, botName: string): InboundMessage {
  if (!(data.channelType === 1 || data.channelType === 2 || data.channelType === 5)) {
    throw new OctoError(`Octo message channel type ${data.channelType} is unsupported`);
  }
  if (!isJsonRecord(data.payload)) throw new OctoError("Octo message payload has invalid schema");
  const content = messageContent(data.payload);
  const quoted = quotedMessage(data.payload);
  // Unusable references never become attachments, so a message left with neither text nor
  // attachments stays unsupported — the same answer as before attachments existed. Text-shaped
  // payloads are the exception: `@bot` alone still runs a normal (empty) turn.
  const attachments: InboundAttachment[] = [];
  const seenUrls = new Set<string>();
  for (const attachment of [...content?.attachments ?? [], ...quoted?.attachments ?? []]) {
    const url = absoluteHttpUrl(attachment.url);
    // 引用消息里的同一张图不重复落盘；回复自己的附件时以本消息的条目为准。
    if (url === undefined || seenUrls.has(url)) continue;
    seenUrls.add(url);
    attachments.push(attachment);
  }
  const entities = mentionEntities(data.payload);
  const mentionedUserIds = mentionUids(data.payload);
  const text = stripBotMention(content?.text ?? "", botUid, botName, entities, mentionedUserIds);
  const payloadType = data.payload["type"];
  const textShaped = typeof payloadType === "number" && TEXT_SHAPED_MESSAGE_TYPES.has(payloadType);
  const supported = content !== undefined
    && (textShaped || text.trim().length > 0 || attachments.length > 0);
  const channelId = data.channelType === 1 ? data.senderId : data.rawChannelId;
  if (channelId.length === 0) throw new OctoError("Octo group message has no channel id");
  return {
    channelId,
    channelType: data.channelType as InboundMessage["channelType"],
    messageId: data.messageId.toString(),
    senderId: data.senderId,
    text,
    mentionedUserIds,
    attachments,
    quoted: quoted?.quote ?? null,
    supported,
    messageSeq: knownIntOrZero(data.messageSeq),
    messageTimestamp: knownIntOrZero(data.timestamp) * 1000,
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
    const groupId = threadParentGroupId(channelId);
    if (groupId === null) {
      throw new OctoError("Octo thread target must contain a parent and topic id");
    }
    const threadId = channelId.slice(groupId.length + 4);
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

function managementId(value: string, label: string): string {
  const id = value.trim();
  if (id.length === 0 || id.length > 128 || id.includes("/")) {
    throw new OctoError(`Octo ${label} is invalid`, { permanent: true });
  }
  return id;
}

function groupPath(groupId: string): string {
  return `/v1/bot/groups/${encodeURIComponent(managementId(groupId, "group id"))}`;
}

function threadPath(groupId: string, shortId: string): string {
  return `${groupPath(groupId)}/threads/${encodeURIComponent(managementId(shortId, "thread id"))}`;
}

/**
 * Body of a failed bot API call, reduced to a single bounded line. The server message is
 * operator-facing text about the request itself; it never carries bot credentials.
 */
function serverErrorMessage(body: Uint8Array): string | null {
  let value: unknown;
  try {
    value = JSON.parse(decodeUtf8(body, "error"));
  } catch {
    return null;
  }
  if (!isJsonRecord(value)) return null;
  const nested = isJsonRecord(value["error"]) ? value["error"]["message"] : undefined;
  const message = typeof nested === "string" && nested.length > 0 ? nested : value["msg"];
  if (typeof message !== "string" || message.length === 0) return null;
  const single = message.replace(/\s+/gu, " ").trim();
  return single.length > 200 ? `${single.slice(0, 200)}…` : single;
}

/**
 * Machine-readable identifier of a failed bot API call (`error.code`, e.g.
 * `err.server.bot_api.store_failed`). Transient server failures are recognized by this
 * code even when the wire status alone would not say so.
 */
function serverErrorCode(body: Uint8Array): string | undefined {
  let value: unknown;
  try {
    value = JSON.parse(decodeUtf8(body, "error"));
  } catch {
    return undefined;
  }
  if (!isJsonRecord(value)) return undefined;
  const nested = isJsonRecord(value["error"]) ? value["error"]["code"] : undefined;
  const code = typeof nested === "string" && nested.length > 0 ? nested : value["code"];
  return typeof code === "string" && code.length > 0 ? code : undefined;
}

function managementError(label: string, status: number, body: Uint8Array): OctoError {
  const detail = serverErrorMessage(body);
  const code = serverErrorCode(body);
  // The bot API answers every error with the same legacy wire status 400, so the body's
  // machine code is what separates a request mistake (`err.server.bot_api.request_invalid`)
  // from an internal failure (`err.server.bot_api.store_failed`); keep it in the message so
  // the distinction survives into logs and tool results.
  const reason = code === undefined ? `HTTP ${status}` : `HTTP ${status} (${code})`;
  return new OctoError(
    detail === null
      ? `Octo ${label} request failed with ${reason}`
      : `Octo ${label} failed with ${reason}: ${detail}`,
    {
      status,
      permanent: status === 400 || status === 401 || status === 403 || status === 404,
      ...(code === undefined ? {} : { code }),
    },
  );
}

async function botApiRequest(
  apiBase: string,
  botToken: string,
  label: string,
  path: string,
  method: "GET" | "POST" | "PUT" | "DELETE",
  body: JsonRecord | undefined,
  signal: AbortSignal | undefined,
  options: DeleteThreadOptions = {},
): Promise<unknown> {
  const response = await requestWithTimeout(
    apiUrl(apiBase, path),
    {
      method,
      headers:
        body === undefined
          ? authHeaders(botToken)
          : { ...authHeaders(botToken), "Content-Type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      // Bun pools keep-alive connections per origin; an explicit `keepalive: false` spends a
      // dedicated connection on this request instead of reusing a pooled socket. Retries ask
      // for it so they cannot run on the connection state the failed attempt ran on.
      ...(options.freshConnection === true ? { keepalive: false } : {}),
    },
    label,
    signal,
    MAX_HTTP_BODY_BYTES,
  );
  if (response.status < 200 || response.status >= 300) {
    throw managementError(label, response.status, response.body);
  }
  if (response.body.byteLength === 0) return null;
  return parseJson(response.body, label);
}

function groupInfoFromResponse(value: unknown): GroupInfo {
  if (!isJsonRecord(value)) {
    throw new OctoError("Octo group info response has invalid schema");
  }
  const creator = value["creator"];
  const notice = value["notice"];
  const name = value["name"];
  const status = value["status"];
  const createdAt = value["created_at"];
  return {
    groupId: requiredString(value["group_no"], "group info group_no"),
    name: typeof name === "string" ? name : "",
    notice: typeof notice === "string" ? notice : "",
    creator: typeof creator === "string" ? creator : "",
    status: typeof status === "number" && Number.isFinite(status) ? status : 0,
    createdAt: typeof createdAt === "string" ? createdAt : "",
  };
}

/**
 * Reads the server's 免@ decision. `effective` is authoritative; the legacy 0/1
 * `no_mention` field carries the same AND-combined value for adapters that predate
 * it, so an older server that omits `effective` still works. Any other shape is a
 * schema error, which keeps a bot that must be @mentioned from being let through.
 */
function mentionPrefFromResponse(value: unknown): GroupMentionPref {
  if (!isJsonRecord(value)) {
    throw new OctoError("Octo group mention preference response has invalid schema");
  }
  const effective = value["effective"];
  if (typeof effective === "boolean") return { effective };
  const noMention = value["no_mention"];
  if (typeof noMention === "number" && Number.isFinite(noMention)) {
    return { effective: noMention === 1 };
  }
  throw new OctoError("Octo group mention preference response has invalid schema");
}

function memberFromResponse(value: unknown, label: string): GroupMember {
  if (!isJsonRecord(value)) {
    throw new OctoError(`Octo ${label} response has invalid schema`);
  }
  const name = value["name"];
  const role = value["role"];
  const joinedAt = value["created_at"];
  return {
    uid: requiredString(value["uid"], `${label} uid`),
    name: typeof name === "string" ? name : "",
    role: typeof role === "number" && Number.isFinite(role) ? role : 0,
    robot: value["robot"] === 1 || value["robot"] === true,
    joinedAt: typeof joinedAt === "string" ? joinedAt : "",
  };
}

function membersFromResponse(value: unknown, label: string): GroupMember[] {
  if (!Array.isArray(value)) {
    throw new OctoError(`Octo ${label} response has invalid schema`);
  }
  return value.map((entry) => memberFromResponse(entry, label));
}

function threadFromResponse(value: unknown): ThreadSummary {
  if (!isJsonRecord(value)) {
    throw new OctoError("Octo thread response has invalid schema");
  }
  const shortId = requiredString(value["short_id"], "thread short_id");
  const groupId = value["group_no"];
  const channelId = value["channel_id"];
  const name = value["name"];
  const creatorUid = value["creator_uid"];
  const status = value["status"];
  const memberCount = value["member_count"];
  const messageCount = value["message_count"];
  const createdAt = value["created_at"];
  const updatedAt = value["updated_at"];
  const resolvedGroupId = typeof groupId === "string" ? groupId : "";
  return {
    shortId,
    groupId: resolvedGroupId,
    channelId:
      typeof channelId === "string" && channelId.length > 0
        ? channelId
        : `${resolvedGroupId}____${shortId}`,
    name: typeof name === "string" ? name : "",
    creatorUid: typeof creatorUid === "string" ? creatorUid : "",
    status: typeof status === "number" && Number.isFinite(status) ? status : 0,
    memberCount: typeof memberCount === "number" && Number.isFinite(memberCount) ? memberCount : 0,
    messageCount:
      typeof messageCount === "number" && Number.isFinite(messageCount) ? messageCount : 0,
    createdAt: typeof createdAt === "string" ? createdAt : "",
    updatedAt: typeof updatedAt === "string" ? updatedAt : "",
  };
}

function threadsFromResponse(value: unknown): ThreadSummary[] {
  if (!Array.isArray(value)) {
    throw new OctoError("Octo thread list response has invalid schema");
  }
  return value.map(threadFromResponse);
}

/**
 * `POST /v1/bot/messages/sync` answers with base64-encoded JSON payloads that follow the
 * same shape as WS frames. Rows are skipped individually when they carry no extractable
 * text or come from the bot itself, so one malformed row cannot fail the whole page.
 */
function historyFromResponse(value: unknown, botUid: string): HistoryMessage[] {
  if (!isJsonRecord(value) || !Array.isArray(value.messages)) {
    throw new OctoError("Octo message sync response has invalid schema");
  }
  const messages: HistoryMessage[] = [];
  for (const entry of value.messages) {
    const message = historyMessage(entry, botUid);
    if (message) messages.push(message);
  }
  return messages.sort((left, right) => left.seq - right.seq);
}

function historyMessage(value: unknown, botUid: string): HistoryMessage | undefined {
  if (!isJsonRecord(value)) return undefined;
  const seq = value.message_seq;
  const senderId = value.from_uid;
  const timestamp = value.timestamp;
  const payload = value.payload;
  if (!Number.isSafeInteger(seq) || (seq as number) <= 0) return undefined;
  if (typeof senderId !== "string" || senderId.length === 0 || senderId === botUid) return undefined;
  if (!Number.isSafeInteger(timestamp) || (timestamp as number) <= 0) return undefined;
  if (typeof payload !== "string") return undefined;
  const envelope = decodedHistoryPayload(payload);
  if (!isJsonRecord(envelope)) return undefined;
  // Same type table as the live path, text only: history never downloads attachments and
  // never checks URLs, so a stale or relative reference cannot trigger network work.
  const text = messageContent(envelope)?.text ?? "";
  if (text.trim().length === 0) return undefined;
  return {
    seq: seq as number,
    senderId,
    text,
    timestamp: (timestamp as number) * 1000,
  };
}

function decodedHistoryPayload(payload: string): unknown {
  try {
    return JSON.parse(Buffer.from(payload, "base64").toString("utf8")) as unknown;
  } catch {
    return undefined;
  }
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
  const displayEnabled = (profile.enabled === true || profile.enabled === 1)
    && config.card_enabled === true
    && config.display_enabled === true;
  const enabled = displayEnabled && config.reasoning_enabled === true;
  const interactive = displayEnabled
    && config.interaction_enabled === true
    && profiles.includes("octo/v2");
  return {
    enabled,
    interactive,
    profiles,
    cardVersion: typeof profile.card_version === "string" ? profile.card_version : "",
    ...(elements === undefined ? {} : { elements }),
    ...(actions === undefined ? {} : { actions }),
    ...(limits === undefined ? {} : { limits }),
  };
}

/** `octo/v2` needs the interaction switch; `octo/v1` display frames need the existing gate. */
function cardProfileSupported(
  capabilities: ExecutionCardCapabilities,
  profile: CardProfile,
): boolean {
  if (!capabilities.profiles.includes(profile)) return false;
  return profile === "octo/v2" ? capabilities.interactive : capabilities.enabled;
}

const BOT_EVENT_MAX_LIMIT = 100;
/** The server clamps the hold to [0, 30]; staying below keeps a cancellation responsive. */
const BOT_EVENT_MAX_WAIT_SECONDS = 25;
const BOT_EVENT_TIMEOUT_MARGIN_MS = 10_000;

function eventString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** Event ids are queue counters; they arrive as JSON numbers and are compared, never computed. */
function botEventId(value: unknown): number | null {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (typeof value === "string" && /^\d+$/u.test(value)) {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Parses the frozen `card_action` envelope. Anything that does not match exactly — another event
 * type, a missing field, an unknown channel — is left to the caller as an unclassified event so
 * it can be acknowledged instead of blocking the queue.
 */
function cardActionFromEvent(event: JsonRecord): BotCardAction | null {
  if (event.event_type !== "card_action" || !isJsonRecord(event.event_data)) return null;
  const data = event.event_data;
  const messageId = eventString(data.message_id);
  const channelId = eventString(data.channel_id);
  const actionId = eventString(data.action_id);
  const operatorUid = eventString(data.operator_uid);
  const channelType = data.channel_type;
  if (messageId === null || channelId === null || actionId === null || operatorUid === null) {
    return null;
  }
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) return null;
  const inputs: Record<string, string> = {};
  if (isJsonRecord(data.inputs)) {
    for (const [key, value] of Object.entries(data.inputs)) {
      if (typeof value === "string") inputs[key] = value;
    }
  }
  return { messageId, channelId, channelType, actionId, operatorUid, inputs };
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
  options: { cardSeq?: number; transient?: boolean } = {},
): JsonRecord | null {
  const built = buildExecutionCard(snapshot, capabilities);
  if (!built) return null;
  return cardFrameEnvelope(
    { card: built.card, plain: built.plain, profile: CARD_PROFILE },
    capabilities,
    options,
  );
}

/**
 * Builds the wire envelope every card path shares (progress and ask cards alike) and refuses
 * frames the deployment's declared limits cannot carry.
 */
function cardFrameEnvelope(
  frame: CardFrame,
  capabilities: ExecutionCardCapabilities,
  options: { cardSeq?: number; transient?: boolean } = {},
): JsonRecord | null {
  const envelope: JsonRecord = {
    type: INTERACTIVE_CARD_MESSAGE_TYPE,
    profile: frame.profile,
    card_version: CARD_VERSION,
    card: frame.card,
    plain: frame.plain,
  };
  if (options.cardSeq !== undefined) envelope.card_seq = options.cardSeq;
  if (options.transient === true) envelope.transient = true;
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
    const content = executionCardEnvelope(progress, capabilities, {
      cardSeq,
      ...(progress.state === "running" ? { transient: true } : {}),
    });
    if (!content) {
      throw new OctoError("Octo execution card profile does not support this progress card");
    }
    await this.postCardEdit(target, messageId, content, signal);
  }

  async sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    validateTarget(target);
    const capabilities = await this.cardCapabilitiesFor(signal);
    if (!capabilities || !cardProfileSupported(capabilities, frame.profile)) return null;
    const content = cardFrameEnvelope(frame, capabilities);
    if (!content) return null;
    return this.sendMessage(target, content, clientMsgNo, signal);
  }

  async editCard(
    target: ChatTarget,
    messageId: string,
    frame: CardFrame,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo card message id is required");
    if (!Number.isSafeInteger(cardSeq) || cardSeq <= 0) {
      throw new OctoError("Octo card sequence must be a positive safe integer");
    }
    const capabilities = await this.cardCapabilitiesFor(signal);
    if (!capabilities || !cardProfileSupported(capabilities, frame.profile)) {
      throw new OctoError("Octo card profile does not support this card");
    }
    const content = cardFrameEnvelope(frame, capabilities, { cardSeq });
    if (!content) throw new OctoError("Octo card profile does not support this card");
    await this.postCardEdit(target, messageId, content, signal);
  }

  async fetchBotEvents(fetch: BotEventFetch): Promise<BotEvent[] | null> {
    const afterEventId = Number.isSafeInteger(fetch.afterEventId) && fetch.afterEventId > 0
      ? fetch.afterEventId
      : 0;
    const limit = Math.min(Math.max(Math.trunc(fetch.limit), 1), BOT_EVENT_MAX_LIMIT);
    const waitSeconds = Math.min(Math.max(Math.trunc(fetch.waitSeconds), 0), BOT_EVENT_MAX_WAIT_SECONDS);
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/events"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: afterEventId, limit, wait: waitSeconds }),
      },
      "bot events",
      fetch.signal,
      MAX_HTTP_BODY_BYTES,
      waitSeconds * 1000 + BOT_EVENT_TIMEOUT_MARGIN_MS,
    );
    if (response.status === 404) return null;
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("bot events", response.status);
    }
    const value = parseJsonWithInt64(response.body, "bot events");
    if (!isJsonRecord(value) || !Array.isArray(value.results)) {
      throw new OctoError("Octo bot events response has invalid schema");
    }
    const events: BotEvent[] = [];
    for (const entry of value.results) {
      if (!isJsonRecord(entry)) continue;
      const eventId = botEventId(entry.event_id);
      if (eventId === null) continue;
      const action = cardActionFromEvent(entry);
      events.push(action ? { eventId, kind: "card_action", action } : { eventId, kind: "other" });
    }
    return events;
  }

  async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    if (!Number.isSafeInteger(eventId) || eventId < 0) {
      throw new OctoError("Octo bot event id must be a non-negative safe integer");
    }
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, `/v1/bot/events/${eventId}/ack`),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: "{}",
      },
      "bot event ack",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("bot event ack", response.status);
    }
  }

  /**
   * Card capability snapshot. A probe that answered is reused for the life of the process; a
   * failed probe is retried, so a transient outage cannot degrade the chat to text-only cards.
   */
  private async cardCapabilitiesFor(signal?: AbortSignal): Promise<ExecutionCardCapabilities | undefined> {
    if (this.cardCapabilities) return this.cardCapabilities;
    const capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal);
    if (capabilities) this.cardCapabilities = capabilities;
    return capabilities ?? undefined;
  }

  /** Shared card edit path: progress and ask cards differ only in the frame they build. */
  private async postCardEdit(
    target: ChatTarget,
    messageId: string,
    content: JsonRecord,
    signal?: AbortSignal,
  ): Promise<void> {
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

  async fetchGroupMessages(target: ChatTarget, fetch: HistoryFetch): Promise<HistoryMessage[]> {
    validateTarget(target);
    if (target.channelType === 1) throw new OctoError("DM does not support group message history");
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/messages/sync"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
          start_message_seq: fetch.sinceSeq,
          end_message_seq: fetch.untilSeq,
          limit: fetch.limit,
          pull_mode: 1,
        }),
      },
      "message sync",
      fetch.signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("message sync", response.status);
    return historyFromResponse(parseJson(response.body, "message sync"), this.registration?.uid ?? "");
  }

  /**
   * Streams one inbound attachment to `path` and returns the stored byte count. A message
   * that was already accepted once reuses the file left by that delivery instead of
   * downloading again, and a failed transfer leaves neither a partial file nor a temp file.
   */
  async downloadAttachment(request: AttachmentDownload): Promise<number> {
    const stored = await storedBytes(request.path);
    if (stored > 0) return stored;
    const url = absoluteHttpUrl(request.url);
    if (url === undefined) {
      throw new AttachmentError("failed", "Octo attachment URL is not an absolute http(s) URL");
    }
    const deadline = requestDeadline(request.signal, ATTACHMENT_DOWNLOAD_TIMEOUT_MS);
    const temporary = `${request.path}.part-${randomBytes(ATTACHMENT_PART_BYTES).toString("hex")}`;
    try {
      await mkdir(dirname(request.path), { recursive: true, mode: 0o700 });
      const response = await fetch(url, { signal: deadline.signal, redirect: "follow" });
      if (response.status < 200 || response.status >= 300) {
        throw new AttachmentError("failed", `Octo attachment download answered HTTP ${response.status}`);
      }
      const declared = Number(response.headers.get("content-length"));
      if (Number.isFinite(declared) && declared > request.maxBytes) {
        throw new AttachmentError(
          "oversize",
          `Octo attachment declares ${declared} bytes, over the ${request.maxBytes} byte limit`,
        );
      }
      if (!response.body) throw new AttachmentError("failed", "Octo attachment response has no body");
      const handle = await open(temporary, "w", 0o600);
      let written = 0;
      try {
        const reader = response.body.getReader();
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) break;
          written += chunk.value.byteLength;
          if (written > request.maxBytes) {
            throw new AttachmentError("oversize", `Octo attachment exceeds the ${request.maxBytes} byte limit`);
          }
          let offset = 0;
          while (offset < chunk.value.byteLength) {
            const { bytesWritten } = await handle.write(
              chunk.value,
              offset,
              chunk.value.byteLength - offset,
            );
            if (bytesWritten <= 0) throw new AttachmentError("failed", "Octo attachment write stalled");
            offset += bytesWritten;
          }
        }
      } finally {
        await handle.close();
      }
      // Only a fully written file becomes the attachment a task prompt may point at.
      await rename(temporary, request.path);
      return written;
    } catch (error) {
      await rm(temporary, { force: true }).catch(() => undefined);
      if (error instanceof AttachmentError) throw error;
      if (deadline.timedOut() || deadline.aborted()) {
        throw new AttachmentError("timeout", "Octo attachment download timed out");
      }
      throw new AttachmentError("failed", "Octo attachment download failed");
    } finally {
      deadline.release();
    }
  }

  /**
   * Best-effort server-side transcription of a saved audio file. The endpoint forwards to
   * the deployment's ASR service; when that service is missing or unhappy the audio file
   * still reaches the agent, so every failure degrades to "no transcription".
   */
  async transcribeAudio(path: string, signal?: AbortSignal): Promise<string | null> {
    const deadline = requestDeadline(signal, ATTACHMENT_DOWNLOAD_TIMEOUT_MS);
    try {
      // Real voice notes are small; anything past the cap is not worth buffering into memory
      // and no ASR service would accept it.
      if ((await stat(path)).size > MAX_TRANSCRIBE_BYTES) return null;
      const audio = await readFile(path);
      const name = basename(path);
      const contentType = AUDIO_CONTENT_TYPES[extname(name).toLowerCase()] ?? "application/octet-stream";
      const form = new FormData();
      form.append("mode", "edit");
      form.append("audio", new Blob([audio], { type: contentType }), name);
      const response = await fetch(apiUrl(this.apiBase, "/v1/bot/voice/transcribe"), {
        method: "POST",
        headers: authHeaders(this.botToken),
        body: form,
        signal: deadline.signal,
      });
      // Drain the body either way: an unread stream would hold the connection open.
      const body = await readBody(response, MAX_HTTP_BODY_BYTES);
      if (response.status < 200 || response.status >= 300) return null;
      const value = parseJson(body, "voice transcribe");
      if (!isJsonRecord(value)) return null;
      const text = textField(value.text);
      return text.trim().length > 0 ? text : null;
    } catch {
      return null;
    } finally {
      deadline.release();
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

  async getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo> {
    return groupInfoFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group info",
        groupPath(groupId),
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getGroupMentionPref(groupId: string, signal?: AbortSignal): Promise<GroupMentionPref> {
    return mentionPrefFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group mention preference",
        `${groupPath(groupId)}/mention_pref`,
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getGroupMembers(groupId: string, signal?: AbortSignal): Promise<GroupMember[]> {
    return membersFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group members",
        `${groupPath(groupId)}/members`,
        "GET",
        undefined,
        signal,
      ),
      "group members",
    );
  }

  async updateGroupInfo(groupId: string, patch: GroupInfoPatch, signal?: AbortSignal): Promise<void> {
    const body: JsonRecord = {};
    if (patch.name !== undefined) body.name = patch.name;
    if (patch.notice !== undefined) body.notice = patch.notice;
    if (Object.keys(body).length === 0) {
      throw new OctoError("Octo group update requires a name or a notice", { permanent: true });
    }
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "group info update",
      `${groupPath(groupId)}/info`,
      "PUT",
      body,
      signal,
    );
  }

  async listThreads(groupId: string, signal?: AbortSignal): Promise<ThreadSummary[]> {
    return threadsFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread list",
        `${groupPath(groupId)}/threads`,
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary> {
    return threadFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread create",
        `${groupPath(groupId)}/threads`,
        "POST",
        { name },
        signal,
      ),
    );
  }

  async getThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<ThreadSummary> {
    return threadFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread info",
        threadPath(groupId, shortId),
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getThreadMembers(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
  ): Promise<GroupMember[]> {
    return membersFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread members",
        `${threadPath(groupId, shortId)}/members`,
        "GET",
        undefined,
        signal,
      ),
      "thread members",
    );
  }

  async joinThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread join",
      `${threadPath(groupId, shortId)}/join`,
      "POST",
      undefined,
      signal,
    );
  }

  async leaveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread leave",
      `${threadPath(groupId, shortId)}/leave`,
      "POST",
      undefined,
      signal,
    );
  }

  async deleteThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
    options: DeleteThreadOptions = {},
  ): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread delete",
      threadPath(groupId, shortId),
      "DELETE",
      undefined,
      signal,
      options,
    );
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
    let header = "";
    try {
      const setting = reader.byte();
      void reader.string();
      const senderId = reader.string();
      const rawChannelId = reader.string();
      const channelType = reader.byte();
      header = `channelType=${channelType} sender=${senderId} channel=${rawChannelId}`;
      if (channelType === 1
        && rawChannelId.includes("@")
        && !rawChannelId.includes(this.registration?.uid ?? "")) {
        this.logger(`Octo DM channel id failed the bot identity check (${header})`);
        return;
      }
      if (state.serverVersion >= 3) void reader.int32();
      void reader.string();
      const messageId = reader.int64();
      const messageSeq = reader.int32();
      const timestamp = reader.int32();
      if ((setting & 0x08) !== 0) void reader.string();
      const payload = decryptPayload(reader.bytes(), state.aesKey!, state.aesIv!);
      const normalized = normalizedMessage(
        { setting, senderId, rawChannelId, channelType, messageId, messageSeq, timestamp, payload },
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
      const detail = error instanceof OctoError ? error.message : "Octo inbound message is invalid";
      this.logger(header.length > 0 ? `${detail} (${header})` : detail);
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
