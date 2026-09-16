import { createDecipheriv, createHash, randomBytes } from "node:crypto";
import { sharedKey } from "curve25519-js";
import { OctoError } from "./error.ts";

/**
 * Octo 加密协议的字节层：变长帧编解码、Curve25519 会话密钥、AES 载荷解密，以及 CONNECT /
 * RECVACK 两类握手帧的构造。这里只做字节与密码学，不碰连接、重连或消息语义。
 */
/** 一个 Bot 身份注册成功后拿到的连接信息。 */
export type Registration = {
  uid: string;
  name: string;
  imToken: string;
  wsUrl: string;
  ownerUid?: string;
};

/** 一个已解码的协议帧。 */
export type WireFrame = {
  packetType: number;
  flags: number;
  body: Uint8Array;
};

/** Curve25519 会话密钥对。 */
export type Curve25519KeyPair = {
  public: Uint8Array;
  private: Uint8Array;
};

// 协议包类型与握手版本。
export const CONNECT_PACKET = 1;
export const CONNACK_PACKET = 2;
export const RECV_PACKET = 5;
export const RECVACK_PACKET = 6;
export const PING_PACKET = 7;
export const PONG_PACKET = 8;
export const DISCONNECT_PACKET = 9;
export const PROTOCOL_VERSION = 4;
export const DEVICE_FLAG = 0;

/** 单个协议帧与单个 AES 分组的边界。 */
export const FRAME_MAX_BYTES = 16 * 1024 * 1024;
export const AES_BLOCK_BYTES = 16;

export class ByteWriter {
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

export class ByteReader {
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

export class FrameDecoder {
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

export function readVariableLength(data: Uint8Array, start: number): { value: number; next: number } | undefined {
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

export function encodeVariableLength(length: number): Buffer {
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

export function encodeFrame(packetType: number, body: Uint8Array, flags = 0): Buffer {
  return Buffer.concat([
    Buffer.from([(packetType << 4) | flags]),
    encodeVariableLength(body.byteLength),
    Buffer.from(body),
  ]);
}

export function encodeControlFrame(packetType: number): Buffer {
  return Buffer.from([packetType << 4]);
}

export function toBytes(value: unknown): Uint8Array {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (Array.isArray(value)) return Uint8Array.from(Buffer.concat(value.map((part) => Buffer.from(part as Uint8Array))));
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new OctoError("Octo WebSocket message is not binary");
}



export function deriveAesKey(privateKey: Uint8Array, serverPublicKey: Uint8Array): Buffer {
  const secret = sharedKey(privateKey, serverPublicKey);
  const keyHex = createHash("md5").update(Buffer.from(secret).toString("base64"), "utf8").digest("hex").slice(0, 16);
  return Buffer.from(keyHex, "utf8");
}

export function decryptPayload(encryptedBase64: Uint8Array, key: Buffer, iv: Buffer): unknown {
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

export function encodeConnect(registration: Registration, clientKey: Curve25519KeyPair): Buffer {
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

export function encodeRecvAck(messageId: bigint, messageSeq: number): Buffer {
  return encodeFrame(RECVACK_PACKET, new ByteWriter().int64(messageId).int32(messageSeq).finish());
}

/** 帧与响应体一律按严格 UTF-8 解码：坏字节是协议错误，不静默替换。 */
export function decodeUtf8(bytes: Uint8Array, label: string): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new OctoError(`Octo ${label} response is not valid UTF-8`);
  }
}
