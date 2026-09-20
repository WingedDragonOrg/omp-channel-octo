import type {
  ChatTarget,
  DeleteThreadOptions,
  GroupContext,
  GroupInfo,
  GroupMember,
  GroupMentionPref,
  HistoryMessage,
  ThreadSummary,
  UserInfo,
} from "../contracts.ts";
import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import { OctoError } from "./error.ts";
import {
  MAX_HTTP_BODY_BYTES,
  authHeaders,
  apiUrl,
  httpStatusError,
  parseJson,
  parseJsonWithInt64,
  requestWithTimeout,
  requiredString,
} from "./http.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";

/** 群共享说明（GROUP.md / THREAD.md）的服务端上限。 */
export const MAX_GROUP_CONTEXT_BYTES = 64 * 1024;

/**
 * Bot API 的一层：管理端点（群、线程、共享说明）、响应形状的校验与归一，以及失败的
 * 单行错误（含错误码）。网络细节在 `./http.ts`，连接生命周期在 `./index.ts`。
 */
export function contextPath(channelId: string, channelType: ChatTarget["channelType"]): string {
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

export function contextFromResponse(value: unknown): GroupContext {
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

export function parseVersion(value: unknown): { version: number } {
  if (!isJsonRecord(value) || !Number.isSafeInteger(value.version) || (value.version as number) < 0) {
    throw new OctoError("Octo group context update response has invalid schema");
  }
  return { version: value.version as number };
}

export function managementId(value: string, label: string): string {
  const id = value.trim();
  if (id.length === 0 || id.length > 128 || id.includes("/")) {
    throw new OctoError(`Octo ${label} is invalid`, { permanent: true });
  }
  return id;
}

export function groupPath(groupId: string): string {
  return `/v1/bot/groups/${encodeURIComponent(managementId(groupId, "group id"))}`;
}

export function threadPath(groupId: string, shortId: string): string {
  return `${groupPath(groupId)}/threads/${encodeURIComponent(managementId(shortId, "thread id"))}`;
}

/**
 * Failed management call: it reads the same body as every other bot API failure, with this path's
 * own retry policy — a 4xx is the request's verdict (the bot API reports every error as the legacy
 * wire 400, permanent or not), while a 5xx stays retryable.
 */
export function managementError(label: string, status: number, body: Uint8Array): OctoError {
  return httpStatusError(
    label,
    status,
    body,
    status === 400 || status === 401 || status === 403 || status === 404,
  );
}

export async function botApiRequest(
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

export function groupInfoFromResponse(value: unknown): GroupInfo {
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
export function mentionPrefFromResponse(value: unknown): GroupMentionPref {
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

/**
 * `GET /v1/bot/user/info`: the server strips a space-prefixed uid (`s<digits>_<uid>`) before
 * resolving and answers with the bare uid, so the response uid is what the caller asked for
 * modulo that prefix. The display name is genuinely optional — an account may have none — and
 * an unresolvable uid is an HTTP failure, never an empty object.
 */
export function userInfoFromResponse(value: unknown, label: string): UserInfo {
  if (!isJsonRecord(value)) {
    throw new OctoError(`Octo ${label} response has invalid schema`);
  }
  const name = value["name"];
  return {
    uid: requiredString(value["uid"], `${label} uid`),
    name: typeof name === "string" ? name : "",
  };
}

export function memberFromResponse(value: unknown, label: string): GroupMember {
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

export function membersFromResponse(value: unknown, label: string): GroupMember[] {
  if (!Array.isArray(value)) {
    throw new OctoError(`Octo ${label} response has invalid schema`);
  }
  return value.map((entry) => memberFromResponse(entry, label));
}

export function threadFromResponse(value: unknown): ThreadSummary {
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

export function threadsFromResponse(value: unknown): ThreadSummary[] {
  if (!Array.isArray(value)) {
    throw new OctoError("Octo thread list response has invalid schema");
  }
  return value.map(threadFromResponse);
}
