import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isRecord } from "./type-guards.ts";
import { hasExtendedAcl } from "./private-file-acl.ts";

export interface StatusSnapshot {
  version: 1;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
  sampledAt: string;
}

export type StatusResult =
  | { state: "online"; snapshot: StatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: StatusSnapshot };

export type ChatChannelType = 1 | 2 | 5;

export interface ChatTarget {
  channelType: ChatChannelType;
  channelId: string;
}

export type ChatExecutionState = "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";

export interface ChatStatusSnapshot {
  version: 1;
  sampledAt: string;
  channelType: ChatChannelType;
  channelId: string;
  state: ChatExecutionState;
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
}

export type ChatStatusResult =
  | { state: "online"; snapshot: ChatStatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatStatusSnapshot };

export interface StatusClient {
  get(signal?: AbortSignal): Promise<StatusResult>;
  getChat(target: ChatTarget, signal?: AbortSignal): Promise<ChatStatusResult>;
}

export interface StatusClientOptions {
  configPath: string;
}

const MAX_CONFIG_BYTES = 16 * 1024;
const MAX_RESPONSE_BYTES = 64 * 1024;
const REQUEST_TIMEOUT_MS = 3_000;
const MAX_BASE_URL_LENGTH = 2_048;
const MAX_TOKEN_LENGTH = 512;
const BEARER_TOKEN = /^[A-Za-z0-9._~+\/-]+=*$/;

interface ClientConfig {
  baseUrl: URL;
  token: string;
  fingerprint: string;
}

type ConfigRead =
  | { state: "ready"; config: ClientConfig }
  | { state: "missing" | "invalid" };

function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;
  return typeof error.code === "string" ? error.code : undefined;
}

function parseBaseUrl(value: unknown): URL | undefined {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_BASE_URL_LENGTH || value.trim() !== value) {
    return undefined;
  }

  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return undefined;
  }

  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.hostname.length === 0 ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    url.search.length > 0 ||
    url.hash.length > 0
  ) {
    return undefined;
  }
  return url;
}

function parseConfig(value: unknown): ClientConfig | undefined {
  if (!isRecord(value)) return undefined;
  const keys = Object.keys(value);
  if (keys.length !== 2 || !keys.includes("baseUrl") || !keys.includes("token")) return undefined;

  const baseUrl = parseBaseUrl(value.baseUrl);
  const token = value.token;
  if (
    baseUrl === undefined ||
    typeof token !== "string" ||
    token.length < 32 ||
    token.length > MAX_TOKEN_LENGTH ||
    !BEARER_TOKEN.test(token)
  ) {
    return undefined;
  }

  return {
    baseUrl,
    token,
    fingerprint: createHash("sha256").update(baseUrl.href).update("\0").update(token).digest("hex"),
  };
}

async function readConfig(configPath: string): Promise<ConfigRead> {
  const uid = process.getuid?.();
  if (uid === undefined) return { state: "invalid" };

  let file;
  try {
    file = await open(configPath, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
  } catch (error) {
    return errorCode(error) === "ENOENT" ? { state: "missing" } : { state: "invalid" };
  }

  try {
    const info = await file.stat();
    if (
      !info.isFile() ||
      info.uid !== uid ||
      (info.mode & 0o7777) !== 0o600 ||
      (await hasExtendedAcl(file.fd))
    ) {
      return { state: "invalid" };
    }

    const bytes = new Uint8Array(MAX_CONFIG_BYTES + 1);
    let length = 0;
    while (length <= MAX_CONFIG_BYTES) {
      const result = await file.read(bytes, length, bytes.byteLength - length, null);
      if (result.bytesRead === 0) break;
      length += result.bytesRead;
      if (length > MAX_CONFIG_BYTES) return { state: "invalid" };
    }

    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length));
    } catch {
      return { state: "invalid" };
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      return { state: "invalid" };
    }

    const config = parseConfig(parsed);
    return config === undefined ? { state: "invalid" } : { state: "ready", config };
  } catch {
    return { state: "invalid" };
  } finally {
    await file.close().catch(() => undefined);
  }
}

function isUtcIsoDateTime(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?Z$/.exec(value);
  if (match === null) return false;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  if (
    month < 1 || month > 12 ||
    day < 1 ||
    hour > 23 || minute > 59 || second > 59
  ) {
    return false;
  }

  const calendarDate = new Date(0);
  calendarDate.setUTCFullYear(year, month - 1, day);
  calendarDate.setUTCHours(hour, minute, second, 0);
  return calendarDate.getUTCFullYear() === year &&
    calendarDate.getUTCMonth() === month - 1 &&
    calendarDate.getUTCDate() === day &&
    Number.isFinite(Date.parse(value));
}

function parseSnapshot(value: unknown): StatusSnapshot | undefined {
  if (!isRecord(value)) return undefined;
  const { version, pid, botUid, ownerUid, runningSessions, queuedTasks, sampledAt } = value;
  if (
    version !== 1 ||
    !Number.isSafeInteger(pid) || (pid as number) < 0 ||
    (botUid !== null && typeof botUid !== "string") ||
    (ownerUid !== null && typeof ownerUid !== "string") ||
    !Number.isSafeInteger(runningSessions) || (runningSessions as number) < 0 ||
    !Number.isSafeInteger(queuedTasks) || (queuedTasks as number) < 0 ||
    !isUtcIsoDateTime(sampledAt)
  ) {
    return undefined;
  }
  return { version: 1, pid: pid as number, botUid, ownerUid, runningSessions: runningSessions as number, queuedTasks: queuedTasks as number, sampledAt };
}

function parseChatSnapshot(value: unknown, target: ChatTarget): ChatStatusSnapshot | undefined {
  if (!isRecord(value)) return undefined;
  const { version, sampledAt, channelType, channelId, state, model, cwd, queuedTasks, pendingQuestions } = value;
  if (
    version !== 1 ||
    channelType !== target.channelType ||
    channelId !== target.channelId ||
    !isUtcIsoDateTime(sampledAt) ||
    (state !== "none" && state !== "idle" && state !== "queued" && state !== "running" &&
      state !== "completed" && state !== "failed" && state !== "cancelled" && state !== "interrupted") ||
    (model !== null && typeof model !== "string") ||
    (cwd !== null && typeof cwd !== "string") ||
    !Number.isSafeInteger(queuedTasks) || (queuedTasks as number) < 0 ||
    !Number.isSafeInteger(pendingQuestions) || (pendingQuestions as number) < 0
  ) {
    return undefined;
  }
  return {
    version: 1,
    sampledAt,
    channelType: target.channelType,
    channelId: target.channelId,
    state: state as ChatExecutionState,
    model,
    cwd,
    queuedTasks: queuedTasks as number,
    pendingQuestions: pendingQuestions as number,
  };
}

function endpointUrl(baseUrl: URL): URL {
  const url = new URL(baseUrl.href);
  const prefix = url.pathname.replace(/\/+$/, "");
  url.pathname = `${prefix}/v1/status`;
  return url;
}

function chatEndpointUrl(baseUrl: URL, target: ChatTarget): URL {
  const url = new URL(baseUrl.href);
  const prefix = url.pathname.replace(/\/+$/, "");
  url.pathname = `${prefix}/v1/chats/status`;
  url.searchParams.set("channelType", String(target.channelType));
  url.searchParams.set("channelId", target.channelId);
  return url;
}

function snapshotCopy(snapshot: StatusSnapshot): StatusSnapshot {
  return { ...snapshot };
}

function chatSnapshotCopy(snapshot: ChatStatusSnapshot): ChatStatusSnapshot {
  return { ...snapshot };
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

async function readBoundedResponse(response: Response): Promise<Uint8Array | undefined> {
  const lengthHeader = response.headers.get("content-length");
  if (lengthHeader !== null && /^\d+$/.test(lengthHeader) && Number(lengthHeader) > MAX_RESPONSE_BYTES) {
    void response.body?.cancel().catch(() => undefined);
    return undefined;
  }

  const reader = response.body?.getReader();
  if (reader === undefined) return undefined;

  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        return undefined;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function isJsonContentType(response: Response): boolean {
  const contentType = response.headers.get("content-type");
  if (contentType === null) return false;
  const mediaType = contentType.split(";", 1)[0]?.trim().toLowerCase();
  return mediaType === "application/json" || mediaType?.endsWith("+json") === true;
}

type SnapshotRequestResult<T> =
  | { state: "online"; snapshot: T }
  | { state: "offline" | "unauthorized" | "invalid" };

async function requestSnapshot<T>(
  url: URL,
  token: string,
  signal: AbortSignal | undefined,
  parse: (value: unknown) => T | undefined,
): Promise<SnapshotRequestResult<T>> {
  const controller = new AbortController();
  const onAbort = (): void => controller.abort(signal?.reason);
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) onAbort();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    throwIfAborted(signal);
    const response = await fetch(url, {
      method: "GET",
      headers: { accept: "application/json", authorization: `Bearer ${token}` },
      redirect: "manual",
      cache: "no-store",
      signal: controller.signal,
    });
    throwIfAborted(signal);

    if (response.status === 401 || response.status === 403) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "unauthorized" };
    }
    if (response.status < 200 || response.status >= 300) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "offline" };
    }
    if (!isJsonContentType(response)) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "invalid" };
    }

    const body = await readBoundedResponse(response);
    throwIfAborted(signal);
    if (body === undefined) return { state: "invalid" };

    let parsed: unknown;
    try {
      parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
    } catch {
      return { state: "invalid" };
    }

    const snapshot = parse(parsed);
    if (snapshot === undefined) return { state: "invalid" };
    throwIfAborted(signal);
    return { state: "online", snapshot };
  } catch {
    if (signal?.aborted) throwIfAborted(signal);
    return { state: "offline" };
  } finally {
    clearTimeout(timeout);
    signal?.removeEventListener("abort", onAbort);
  }
}

export function createStatusClient(options: StatusClientOptions): StatusClient {
  let configuredFingerprint: string | undefined;
  let configGeneration = 0;
  let lastSuccess: StatusSnapshot | undefined;
  const chatLastSuccess = new Map<string, { snapshot: ChatStatusSnapshot; requestId: number }>();
  let nextRequestId = 0;
  let lastSuccessRequestId = 0;

  const updateConfig = (fingerprint: string | undefined): number => {
    if (fingerprint !== configuredFingerprint) {
      configuredFingerprint = fingerprint;
      configGeneration += 1;
      lastSuccess = undefined;
      chatLastSuccess.clear();
      lastSuccessRequestId = 0;
    }
    return configGeneration;
  };

  const failure = (state: "unconfigured" | "offline" | "unauthorized" | "invalid", generation?: number): StatusResult => {
    const cached = generation === configGeneration ? lastSuccess : undefined;
    return cached === undefined ? { state } : { state, lastSuccess: snapshotCopy(cached) };
  };

  const chatFailure = (
    state: "unconfigured" | "offline" | "unauthorized" | "invalid",
    generation: number | undefined,
    key: string,
  ): ChatStatusResult => {
    const cached = generation === configGeneration ? chatLastSuccess.get(key)?.snapshot : undefined;
    return cached === undefined ? { state } : { state, lastSuccess: chatSnapshotCopy(cached) };
  };

  return {
    async get(signal?: AbortSignal): Promise<StatusResult> {
      throwIfAborted(signal);
      const loaded = await readConfig(options.configPath);
      throwIfAborted(signal);

      if (loaded.state !== "ready") {
        updateConfig(undefined);
        return loaded.state === "missing" ? { state: "unconfigured" } : { state: "invalid" };
      }

      const { baseUrl, token, fingerprint } = loaded.config;
      const generation = updateConfig(fingerprint);
      const requestId = ++nextRequestId;
      const result = await requestSnapshot(endpointUrl(baseUrl), token, signal, parseSnapshot);
      if (result.state !== "online") return failure(result.state, generation);

      if (generation === configGeneration && requestId > lastSuccessRequestId) {
        lastSuccess = result.snapshot;
        lastSuccessRequestId = requestId;
      }
      return { state: "online", snapshot: snapshotCopy(result.snapshot) };
    },

    async getChat(target: ChatTarget, signal?: AbortSignal): Promise<ChatStatusResult> {
      throwIfAborted(signal);
      const loaded = await readConfig(options.configPath);
      throwIfAborted(signal);

      if (loaded.state !== "ready") {
        updateConfig(undefined);
        return loaded.state === "missing" ? { state: "unconfigured" } : { state: "invalid" };
      }

      const { baseUrl, token, fingerprint } = loaded.config;
      const generation = updateConfig(fingerprint);
      const key = JSON.stringify([target.channelType, target.channelId]);
      const requestId = ++nextRequestId;
      const result = await requestSnapshot(
        chatEndpointUrl(baseUrl, target),
        token,
        signal,
        (value) => parseChatSnapshot(value, target),
      );
      if (result.state !== "online") return chatFailure(result.state, generation, key);

      const cached = chatLastSuccess.get(key);
      if (generation === configGeneration && (cached === undefined || requestId > cached.requestId)) {
        chatLastSuccess.set(key, { snapshot: result.snapshot, requestId });
      }
      return { state: "online", snapshot: chatSnapshotCopy(result.snapshot) };
    },
  };
}
