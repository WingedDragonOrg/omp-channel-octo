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
  zedWorkspaceUrl?: string | null;
  workspaceCwds?: string[];
  queuedTasks: number;
  pendingQuestions: number;
}

export type ChatStatusResult =
  | { state: "online"; snapshot: ChatStatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatStatusSnapshot };

export interface ChatDetailsContext {
  source: "live" | "persisted" | "unavailable";
  stale: boolean;
  usedTokens: number | null;
  contextWindow: number | null;
  percent: number | null;
  model: string | null;
  observedAt: string | null;
  note: string;
}

export interface ChatDetailsTask {
  workspaceId: string;
  taskId: string;
  title: string | null;
  status: string | null;
  url: string | null;
}

export interface ChatDetailsSnapshot {
  version: 1;
  sampledAt: string;
  channelType: ChatChannelType;
  channelId: string;
  context: ChatDetailsContext;
  references: { pr: string[]; mr: string[] };
  tasks: ChatDetailsTask[];
}

export type ChatDetailsResult =
  | { state: "online"; snapshot: ChatDetailsSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatDetailsSnapshot };

export interface ChatCommandSubcommand {
  name: string;
  description?: string;
  hint?: string;
}

export interface ChatCommand {
  name: string;
  source: "channel" | "omp";
  description?: string;
  hint?: string;
  aliases?: string[];
  subcommands?: ChatCommandSubcommand[];
}

export interface ChatCommandsSnapshot {
  version: 1;
  channelType: ChatChannelType;
  channelId: string;
  commands: ChatCommand[];
  omp: "available" | "unavailable";
}

export type ChatCommandsResult =
  | { state: "online"; snapshot: ChatCommandsSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatCommandsSnapshot }
  | { state: "not_found" };

export interface StatusClient {
  get(signal?: AbortSignal): Promise<StatusResult>;
  getChat(target: ChatTarget, signal?: AbortSignal): Promise<ChatStatusResult>;
  getChatDetails(target: ChatTarget, signal?: AbortSignal): Promise<ChatDetailsResult>;
  getChatCommands(target: ChatTarget, signal?: AbortSignal): Promise<ChatCommandsResult>;
}

export interface StatusClientOptions {
  configPath: string;
}

const MAX_CONFIG_BYTES = 16 * 1024;
const MAX_RESPONSE_BYTES = 64 * 1024;
const MAX_COMMANDS_RESPONSE_BYTES = 512 * 1024;
const REQUEST_TIMEOUT_MS = 3_000;
// An idle chat probe may cold-start OMP before the catalog is available; allow the documented 30s path to finish.
const COMMANDS_REQUEST_TIMEOUT_MS = 60_000;
const MAX_BASE_URL_LENGTH = 2_048;
const MAX_TOKEN_LENGTH = 512;
const BEARER_TOKEN = /^[A-Za-z0-9._~+\/-]+=*$/;
const STATUS_BASE_URL_ENVIRONMENT = "OCTOBUDDY_HTTP_BASE_URL";
const STATUS_TOKEN_ENVIRONMENT = "OCTOBUDDY_HTTP_TOKEN";

interface ClientConfig {
  baseUrl: URL;
  token: string;
  fingerprint: string;
}

type ConfigRead =
  | { state: "ready"; config: ClientConfig }
  | { state: "missing" | "invalid" };

interface FileConfig {
  baseUrl: unknown;
  token: unknown;
}

type FileConfigRead =
  | { state: "ready"; fields: FileConfig }
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

function buildConfig(baseUrlValue: unknown, tokenValue: unknown): ClientConfig | undefined {
  const baseUrl = parseBaseUrl(baseUrlValue);
  if (
    baseUrl === undefined ||
    typeof tokenValue !== "string" ||
    tokenValue.length < 32 ||
    tokenValue.length > MAX_TOKEN_LENGTH ||
    !BEARER_TOKEN.test(tokenValue)
  ) {
    return undefined;
  }

  return {
    baseUrl,
    token: tokenValue,
    fingerprint: createHash("sha256").update(baseUrl.href).update("\0").update(tokenValue).digest("hex"),
  };
}

function environmentValue(name: string): string | undefined {
  const value = process.env[name];
  return value === undefined || value.length === 0 ? undefined : value;
}

// Environment values override the private file per field. When both are present the file is not opened.
async function loadConfig(configPath: string): Promise<ConfigRead> {
  const baseUrl = environmentValue(STATUS_BASE_URL_ENVIRONMENT);
  const token = environmentValue(STATUS_TOKEN_ENVIRONMENT);

  if (baseUrl !== undefined && token !== undefined) {
    const config = buildConfig(baseUrl, token);
    return config === undefined ? { state: "invalid" } : { state: "ready", config };
  }

  const file = await readFileConfig(configPath);
  if (file.state !== "ready") return file;
  // Only the merged values are validated, so an invalid file field overwritten by the environment is ignored.
  const config = buildConfig(
    baseUrl !== undefined ? baseUrl : file.fields.baseUrl,
    token !== undefined ? token : file.fields.token,
  );
  return config === undefined ? { state: "invalid" } : { state: "ready", config };
}

async function readFileConfig(configPath: string): Promise<FileConfigRead> {
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

    if (!isRecord(parsed)) return { state: "invalid" };
    const keys = Object.keys(parsed);
    if (keys.length !== 2 || !keys.includes("baseUrl") || !keys.includes("token")) return { state: "invalid" };
    return { state: "ready", fields: { baseUrl: parsed.baseUrl, token: parsed.token } };
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
  const { version, sampledAt, channelType, channelId, state, model, cwd, zedWorkspaceUrl, workspaceCwds, queuedTasks, pendingQuestions } = value;
  if (
    version !== 1 ||
    channelType !== target.channelType ||
    channelId !== target.channelId ||
    !isUtcIsoDateTime(sampledAt) ||
    (state !== "none" && state !== "idle" && state !== "queued" && state !== "running" &&
      state !== "completed" && state !== "failed" && state !== "cancelled" && state !== "interrupted") ||
    (model !== null && typeof model !== "string") ||
    (cwd !== null && typeof cwd !== "string") ||
    (zedWorkspaceUrl !== undefined && zedWorkspaceUrl !== null && typeof zedWorkspaceUrl !== "string") ||
    (workspaceCwds !== undefined && (!Array.isArray(workspaceCwds) || workspaceCwds.some((path) => typeof path !== "string" || !path.startsWith("/")))) ||
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
    ...(zedWorkspaceUrl === undefined ? {} : { zedWorkspaceUrl: zedWorkspaceUrl as string | null }),
    ...(workspaceCwds === undefined ? {} : { workspaceCwds: workspaceCwds as string[] }),
    queuedTasks: queuedTasks as number,
    pendingQuestions: pendingQuestions as number,
  };
}

function parseChatDetailsSnapshot(value: unknown, target: ChatTarget): ChatDetailsSnapshot | undefined {
  if (!isRecord(value) || !isRecord(value.context) || !isRecord(value.references) || !Array.isArray(value.tasks)) {
    return undefined;
  }

  const { version, sampledAt, channelType, channelId, context, references, tasks } = value;
  const {
    source,
    stale,
    usedTokens,
    contextWindow,
    percent,
    model,
    observedAt,
    note,
  } = context;
  const { pr, mr } = references;
  const nullableString = (entry: unknown): boolean => entry === null || typeof entry === "string";

  if (
    version !== 1 ||
    channelType !== target.channelType ||
    channelId !== target.channelId ||
    !isUtcIsoDateTime(sampledAt) ||
    (source !== "live" && source !== "persisted" && source !== "unavailable") ||
    typeof stale !== "boolean" ||
    (usedTokens !== null &&
      (typeof usedTokens !== "number" || !Number.isFinite(usedTokens) || usedTokens < 0)) ||
    (contextWindow !== null &&
      (typeof contextWindow !== "number" || !Number.isFinite(contextWindow) || contextWindow < 0)) ||
    (percent !== null && (typeof percent !== "number" || !Number.isFinite(percent) || percent < 0)) ||
    !nullableString(model) ||
    (observedAt !== null && !isUtcIsoDateTime(observedAt)) ||
    typeof note !== "string" ||
    !Array.isArray(pr) || !pr.every((entry) => typeof entry === "string") ||
    !Array.isArray(mr) || !mr.every((entry) => typeof entry === "string") ||
    !tasks.every((task) =>
      isRecord(task) &&
      typeof task.workspaceId === "string" &&
      typeof task.taskId === "string" &&
      nullableString(task.title) &&
      nullableString(task.status) &&
      nullableString(task.url))
  ) {
    return undefined;
  }

  return {
    version: 1,
    sampledAt,
    channelType: target.channelType,
    channelId: target.channelId,
    context: {
      source,
      stale,
      usedTokens: usedTokens as number | null,
      contextWindow: contextWindow as number | null,
      percent: percent as number | null,
      model: model as string | null,
      observedAt,
      note,
    },
    references: { pr: [...pr] as string[], mr: [...mr] as string[] },
    tasks: tasks.map((task) => {
      const value = task as Record<string, unknown>;
      return {
        workspaceId: value.workspaceId as string,
        taskId: value.taskId as string,
        title: value.title as string | null,
        status: value.status as string | null,
        url: value.url as string | null,
      };
    }),
  };
}

function parseChatCommand(value: unknown): ChatCommand | undefined {
  if (!isRecord(value)) return undefined;
  const { name, source, description, hint, aliases, subcommands } = value;
  if (
    typeof name !== "string" || name.length === 0 || name.length > 128 || /\s/.test(name) ||
    (source !== "channel" && source !== "omp") ||
    (description !== undefined && typeof description !== "string") ||
    (hint !== undefined && typeof hint !== "string")
  ) {
    return undefined;
  }
  if (
    aliases !== undefined &&
    (!Array.isArray(aliases) || aliases.length > 100 ||
      !aliases.every((alias) => typeof alias === "string" && alias.length > 0 && alias.length <= 128 && !/\s/.test(alias)))
  ) {
    return undefined;
  }

  let parsedSubcommands: ChatCommandSubcommand[] | undefined;
  if (subcommands !== undefined) {
    if (!Array.isArray(subcommands) || subcommands.length > 500) return undefined;
    parsedSubcommands = [];
    for (const subcommand of subcommands) {
      if (!isRecord(subcommand)) return undefined;
      const subName = subcommand.name;
      if (
        typeof subName !== "string" || subName.length === 0 || subName.length > 128 ||
        !/^\S+(?: \S+)*$/.test(subName)
      ) {
        return undefined;
      }
      const subDescription = subcommand.description;
      if (subDescription !== undefined && typeof subDescription !== "string") return undefined;
      const subHint = subcommand.hint;
      if (subHint !== undefined && typeof subHint !== "string") return undefined;
      parsedSubcommands.push({
        name: subName,
        ...(subDescription === undefined ? {} : { description: subDescription }),
        ...(subHint === undefined ? {} : { hint: subHint }),
      });
    }
  }

  return {
    name,
    source,
    ...(description === undefined ? {} : { description }),
    ...(hint === undefined ? {} : { hint }),
    ...(aliases === undefined ? {} : { aliases: aliases as string[] }),
    ...(parsedSubcommands === undefined ? {} : { subcommands: parsedSubcommands }),
  };
}

function parseChatCommandsSnapshot(value: unknown, target: ChatTarget): ChatCommandsSnapshot | undefined {
  if (!isRecord(value)) return undefined;
  const { version, channelType, channelId, commands, omp } = value;
  if (
    version !== 1 ||
    channelType !== target.channelType ||
    channelId !== target.channelId ||
    !Array.isArray(commands) ||
    commands.length > 2_000 ||
    (omp !== "available" && omp !== "unavailable")
  ) {
    return undefined;
  }

  const parsedCommands: ChatCommand[] = [];
  for (const command of commands) {
    const parsed = parseChatCommand(command);
    if (parsed === undefined) return undefined;
    parsedCommands.push(parsed);
  }
  return {
    version: 1,
    channelType: target.channelType,
    channelId: target.channelId,
    commands: parsedCommands,
    omp,
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

function chatDetailsEndpointUrl(baseUrl: URL, target: ChatTarget): URL {
  const url = new URL(baseUrl.href);
  const prefix = url.pathname.replace(/\/+$/, "");
  url.pathname = `${prefix}/v1/chats/details`;
  url.searchParams.set("channelType", String(target.channelType));
  url.searchParams.set("channelId", target.channelId);
  return url;
}

function chatCommandsEndpointUrl(baseUrl: URL, target: ChatTarget): URL {
  const url = new URL(baseUrl.href);
  const prefix = url.pathname.replace(/\/+$/, "");
  url.pathname = `${prefix}/v1/chats/commands`;
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

function chatDetailsSnapshotCopy(snapshot: ChatDetailsSnapshot): ChatDetailsSnapshot {
  return {
    ...snapshot,
    context: { ...snapshot.context },
    references: { pr: [...snapshot.references.pr], mr: [...snapshot.references.mr] },
    tasks: snapshot.tasks.map((task) => ({ ...task })),
  };
}

function chatCommandsSnapshotCopy(snapshot: ChatCommandsSnapshot): ChatCommandsSnapshot {
  return {
    ...snapshot,
    commands: snapshot.commands.map((command) => ({
      ...command,
      ...(command.aliases === undefined ? {} : { aliases: [...command.aliases] }),
      ...(command.subcommands === undefined ? {} : { subcommands: command.subcommands.map((subcommand) => ({ ...subcommand })) }),
    })),
  };
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

async function readBoundedResponse(response: Response, maxBytes = MAX_RESPONSE_BYTES): Promise<Uint8Array | undefined> {
  const lengthHeader = response.headers.get("content-length");
  if (lengthHeader !== null && /^\d+$/.test(lengthHeader) && Number(lengthHeader) > maxBytes) {
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
      if (length > maxBytes) {
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
  | { state: "offline" | "unauthorized" | "invalid"; notFound?: true };

async function requestSnapshot<T>(
  url: URL,
  token: string,
  signal: AbortSignal | undefined,
  parse: (value: unknown) => T | undefined,
  timeoutMs = REQUEST_TIMEOUT_MS,
  maxBytes = MAX_RESPONSE_BYTES,
): Promise<SnapshotRequestResult<T>> {
  const controller = new AbortController();
  const onAbort = (): void => controller.abort(signal?.reason);
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) onAbort();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

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
    if (response.status === 404) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "offline", notFound: true };
    }
    if (response.status < 200 || response.status >= 300) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "offline" };
    }
    if (!isJsonContentType(response)) {
      void response.body?.cancel().catch(() => undefined);
      return { state: "invalid" };
    }

    const body = await readBoundedResponse(response, maxBytes);
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
  const chatDetailsLastSuccess = new Map<string, { snapshot: ChatDetailsSnapshot; requestId: number }>();
  const chatCommandsLastSuccess = new Map<string, { snapshot: ChatCommandsSnapshot; requestId: number }>();
  let nextRequestId = 0;
  let lastSuccessRequestId = 0;

  const updateConfig = (fingerprint: string | undefined): number => {
    if (fingerprint !== configuredFingerprint) {
      configuredFingerprint = fingerprint;
      configGeneration += 1;
      lastSuccess = undefined;
      chatLastSuccess.clear();
      chatDetailsLastSuccess.clear();
      chatCommandsLastSuccess.clear();
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

  const chatDetailsFailure = (
    state: "unconfigured" | "offline" | "unauthorized" | "invalid",
    generation: number | undefined,
    key: string,
  ): ChatDetailsResult => {
    const cached = generation === configGeneration ? chatDetailsLastSuccess.get(key)?.snapshot : undefined;
    return cached === undefined ? { state } : { state, lastSuccess: chatDetailsSnapshotCopy(cached) };
  };

  const chatCommandsFailure = (
    state: "unconfigured" | "offline" | "unauthorized" | "invalid",
    generation: number | undefined,
    key: string,
  ): ChatCommandsResult => {
    const cached = generation === configGeneration ? chatCommandsLastSuccess.get(key)?.snapshot : undefined;
    return cached === undefined ? { state } : { state, lastSuccess: chatCommandsSnapshotCopy(cached) };
  };

  return {
    async get(signal?: AbortSignal): Promise<StatusResult> {
      throwIfAborted(signal);
      const loaded = await loadConfig(options.configPath);
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
      const loaded = await loadConfig(options.configPath);
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

    async getChatDetails(target: ChatTarget, signal?: AbortSignal): Promise<ChatDetailsResult> {
      throwIfAborted(signal);
      const loaded = await loadConfig(options.configPath);
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
        chatDetailsEndpointUrl(baseUrl, target),
        token,
        signal,
        (value) => parseChatDetailsSnapshot(value, target),
      );
      if (result.state !== "online") return chatDetailsFailure(result.state, generation, key);

      const cached = chatDetailsLastSuccess.get(key);
      if (generation === configGeneration && (cached === undefined || requestId > cached.requestId)) {
        chatDetailsLastSuccess.set(key, { snapshot: result.snapshot, requestId });
      }
      return { state: "online", snapshot: chatDetailsSnapshotCopy(result.snapshot) };
    },

    async getChatCommands(target: ChatTarget, signal?: AbortSignal): Promise<ChatCommandsResult> {
      throwIfAborted(signal);
      const loaded = await loadConfig(options.configPath);
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
        chatCommandsEndpointUrl(baseUrl, target),
        token,
        signal,
        (value) => parseChatCommandsSnapshot(value, target),
        COMMANDS_REQUEST_TIMEOUT_MS,
        MAX_COMMANDS_RESPONSE_BYTES,
      );
      if (result.state !== "online") {
        if (result.notFound) {
          const cached = chatCommandsLastSuccess.get(key);
          if (generation === configGeneration && (cached === undefined || requestId > cached.requestId)) {
            chatCommandsLastSuccess.delete(key);
          }
          return { state: "not_found" };
        }
        return chatCommandsFailure(result.state, generation, key);
      }

      const cached = chatCommandsLastSuccess.get(key);
      if (generation === configGeneration && (cached === undefined || requestId > cached.requestId)) {
        chatCommandsLastSuccess.set(key, { snapshot: result.snapshot, requestId });
      }
      return { state: "online", snapshot: chatCommandsSnapshotCopy(result.snapshot) };
    },
  };
}
