import { createReadStream } from "node:fs";
import { open, stat } from "node:fs/promises";
import { OctoError } from "./error.ts";
import { decodeUtf8 } from "./protocol.ts";
import { isJsonRecord } from "./type-guards.ts";

/**
 * 与 Octo 服务端说话的 HTTP 层：请求超时与取消、响应体读取与解析、下载与上传用的响应字段，
 * 以及图片文件头嗅探。这里不涉及连接生命周期，也不解释 Bot API 的响应语义。
 */
export type HttpResult = {
  status: number;
  body: Uint8Array;
};

export type ImageInfo = {
  contentType: string;
  width: number;
  height: number;
};

export type PresignedUpload = {
  uploadUrl: string;
  downloadUrl: string;
  contentType: string;
  contentDisposition: string | undefined;
};

/** Random bytes appended to the temp file a download writes before renaming it into place. */
export const ATTACHMENT_PART_BYTES = 6;
export const MAX_HTTP_BODY_BYTES = 2 * 1024 * 1024;
export const HTTP_TIMEOUT_MS = 15_000;
export const HANDSHAKE_TIMEOUT_MS = 15_000;

export function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new OctoError(`Octo response has invalid ${field}`);
  }
  return value;
}


export function apiUrl(base: string, path: string): string {
  try {
    return new URL(path, base.endsWith("/") ? base : `${base}/`).toString();
  } catch {
    throw new OctoError("Octo API URL is invalid");
  }
}

export function authHeaders(token: string): HeadersInit {
  return { Authorization: `Bearer ${token}` };
}

export async function readBody(response: Response, maxBytes: number): Promise<Uint8Array> {
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

export function parseJson(bytes: Uint8Array, label: string): unknown {
  try {
    return JSON.parse(decodeUtf8(bytes, label)) as unknown;
  } catch (error) {
    if (error instanceof OctoError) throw error;
    throw new OctoError(`Octo ${label} response is not valid JSON`);
  }
}

export function parseJsonWithInt64(bytes: Uint8Array, label: string): unknown {
  const text = decodeUtf8(bytes, label);
  try {
    // sendMessage currently returns a JSON number, which must not pass through Number.
    return JSON.parse(text.replace(/("message_id"\s*:\s*)(-?\d+)/g, "$1\"$2\"")) as unknown;
  } catch {
    throw new OctoError(`Octo ${label} response is not valid JSON`);
  }
}

export type RequestDeadline = {
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
export function requestDeadline(caller: AbortSignal | undefined, timeoutMs: number): RequestDeadline {
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
export async function storedBytes(path: string): Promise<number> {
  try {
    const info = await stat(path);
    return info.isFile() && Number.isSafeInteger(info.size) && info.size > 0 ? info.size : 0;
  } catch {
    return 0;
  }
}

export async function requestWithTimeout(
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

/**
 * Body of a failed bot API call, reduced to a single bounded line. The server message is
 * operator-facing text about the request itself; it never carries bot credentials.
 */
export function serverErrorMessage(body: Uint8Array): string | null {
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
export function serverErrorCode(body: Uint8Array): string | undefined {
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

/**
 * One HTTP failure, carrying the bot API's own answer whenever the body is one. Every bot API
 * error is a JSON envelope with a machine code (most of them travel as the legacy compatibility
 * 400 on the wire), so a response that is not an envelope did not come from the API at all: the
 * deployment's edge answers such a request with its own status and page. Those refusals keep
 * `status` set and `code` undefined, which is what lets callers tell an edge refusal from a bot
 * API refusal — an edge refusal is shaped by the request body and is transient, not a verdict on
 * the request's meaning.
 *
 * `permanent` is the caller's retry verdict for this status. The default marks only 401/403 final,
 * which leaves a body-shaped 400 to the delivery layer's own retry budget; a call path with a
 * stricter reading (management calls) passes its own.
 */
export function httpStatusError(
  label: string,
  status: number,
  body?: Uint8Array,
  permanent = status === 401 || status === 403,
): OctoError {
  if (body === undefined || body.byteLength === 0) {
    return new OctoError(`Octo ${label} request failed with HTTP ${status}`, { status, permanent });
  }
  const code = serverErrorCode(body);
  const detail = serverErrorMessage(body);
  if (code === undefined && detail === null) {
    return new OctoError(
      `Octo ${label} request failed with HTTP ${status} (non-bot-API response body)`,
      { status, permanent },
    );
  }
  const reason = code === undefined ? `HTTP ${status}` : `HTTP ${status} (${code})`;
  return new OctoError(
    detail === null
      ? `Octo ${label} request failed with ${reason}`
      : `Octo ${label} request failed with ${reason}: ${detail}`,
    { status, permanent, ...(code === undefined ? {} : { code }) },
  );
}

export function responseUrl(value: unknown, field: string): string {
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
export function webSocketUrl(value: unknown, field: string): string {
  const valueString = requiredString(value, field);
  try {
    const parsed = new URL(valueString);
    if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") throw new Error("unsupported URL scheme");
  } catch {
    throw new OctoError(`Octo response has invalid ${field}`);
  }
  return valueString;
}

export function imageInfo(header: Uint8Array): ImageInfo | undefined {
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

export async function readFileHeader(path: string, maxBytes = 64 * 1024): Promise<Uint8Array> {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(maxBytes);
    const { bytesRead } = await handle.read(buffer, 0, maxBytes, 0);
    return buffer.subarray(0, bytesRead);
  } finally {
    await handle.close();
  }
}

