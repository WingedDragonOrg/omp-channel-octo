import { OctoError } from "../octo/error.ts";
import {
  HTTP_TIMEOUT_MS,
  MAX_HTTP_BODY_BYTES,
  apiUrl as buildApiUrl,
  authHeaders,
  httpStatusError,
  parseJson,
  readBody,
  requestDeadline,
  serverErrorCode,
  serverErrorMessage,
} from "../octo/http.ts";
import { isJsonRecord, type JsonRecord } from "../octo/type-guards.ts";
import {
  threadChannelId,
  threadParentGroupId,
  threadShortId,
} from "../threads/thread-target.ts";

/**
 * Read-only remote surfaces for cross-chat discovery. Every value returned here is the
 * service's own JSON shape (snake_case fields), validated at the boundary but never
 * renamed or completed with guessed values. A failed read or invalid response throws an
 * `OctoError` carrying the wire status and server error code when the service supplied
 * them; an unreachable, cancelled or oversized response is never converted into an empty
 * result. Per-page budgets and page walks belong to the discovery coordinator.
 */

export type DiscoveryChannelType = 1 | 2 | 5;

/** One row of `GET /v1/bot/groups`; the endpoint itself has no pagination. */
export interface DiscoveryGroup {
  group_no: string;
  name: string;
  space_id?: string;
  [key: string]: unknown;
}

/**
 * One bot thread row/detail. `channel_id` is checked against the canonical
 * `<group_no>____<short_id>` identity. The bot thread list only exposes active threads;
 * thread detail still succeeds for archived threads.
 */
export interface DiscoveryThread {
  short_id: string;
  group_no: string;
  channel_id: string;
  name: string;
  status: number;
  group_name?: string;
  creator_uid?: string;
  channel_type?: number;
  member_count?: number;
  message_count?: number;
  last_message_at?: string;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

/** Paged bot thread list: `count` is the total number of active threads in the group. */
export interface DiscoveryThreadPage {
  count: number;
  list: DiscoveryThread[];
}

/** One message hit from a search response (`snippet` is server-highlighted text). */
export interface DiscoveryMessageHit {
  message_id: string;
  channel_id: string;
  channel_type?: DiscoveryChannelType;
  sender_id: string;
  sent_at: string;
  message_seq?: number;
  message_kind?: string;
  snippet?: string;
  sender_name?: string;
  sender_avatar_url?: string;
  [key: string]: unknown;
}

/** The shared cursor envelope of the message-search endpoints. */
export interface DiscoverySearchPagination {
  has_more: boolean;
  next_cursor: string;
}

export interface DiscoveryMessagePage {
  data: DiscoveryMessageHit[];
  pagination: DiscoverySearchPagination;
}

/**
 * One bucket of the cross-channel aggregation. `match_count` and `total_groups` are
 * pre-visibility approximations; `pagination.has_more` means the aggregation bucket was
 * truncated, not that `next_cursor` can continue it.
 */
export interface DiscoveryGroupBucket {
  channel_id: string;
  channel_type: DiscoveryChannelType;
  group_name: string;
  match_count: number;
  match_count_approx: boolean;
  latest_at: string;
  preview: DiscoveryMessageHit[];
  parent_group_no?: string;
  thread_id?: string;
  thread_name?: string;
  [key: string]: unknown;
}

export interface DiscoveryGroupSearchData {
  sequence: number;
  query_id: string;
  total_groups: number;
  total_groups_approx: boolean;
  groups: DiscoveryGroupBucket[];
}

export interface DiscoveryGroupSearchPage {
  data: DiscoveryGroupSearchData;
  pagination: DiscoverySearchPagination;
}

/** Fleet's page envelope for workspace and task lists. */
export interface DiscoveryLoopPagination {
  total: number;
  page: number;
  page_size: number;
}

/** One workspace list row (public list normalizes `id` to `workspace_id`). */
export interface DiscoveryWorkspaceRow {
  workspace_id: string;
  name: string;
  slug: string;
  description?: string | null;
  context?: string | null;
  settings?: unknown;
  repos?: unknown;
  issue_prefix?: string;
  avatar_url?: string | null;
  status?: string;
  external_version?: number;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface DiscoveryWorkspacePage {
  data: DiscoveryWorkspaceRow[];
  pagination: DiscoveryLoopPagination;
}

/**
 * Raw workspace detail. The current Fleet detail handler returns `id`; compatible shapes
 * use `workspace_id`. Both are accepted when they agree with the requested UUID.
 */
export interface DiscoveryWorkspaceDetail {
  id?: string;
  workspace_id?: string;
  name: string;
  slug: string;
  description?: string | null;
  context?: string | null;
  settings?: unknown;
  repos?: unknown;
  issue_prefix?: string;
  avatar_url?: string | null;
  status?: string;
  external_version?: number;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

/** One task row of `GET /fleet/api/v1/tasks/_search`. */
export interface DiscoveryTask {
  task_id: string;
  workspace_id?: string;
  identifier?: string;
  title: string;
  status: string;
  description?: string | null;
  priority?: string | null;
  assignee_type?: string | null;
  assignee_id?: string | null;
  assignee_octo_uid?: string | null;
  project_id?: string | null;
  parent_task_id?: string | null;
  start_date?: string | null;
  due_date?: string | null;
  created_at?: string;
  updated_at?: string;
  [key: string]: unknown;
}

export interface DiscoveryTaskPage {
  data: DiscoveryTask[];
  pagination: DiscoveryLoopPagination;
}

/** A chat identity whose current read access must be proven before it is returned. */
export interface DiscoveryTarget {
  channelType: 2 | 5;
  channelId: string;
}

/**
 * A group probe has no group detail to return: the thread-list handler that proves active
 * membership exposes only threads. A thread probe returns the real detail, including the
 * display name and parent group.
 */
export type DiscoveryTargetVerification =
  | { channelType: 2; channelId: string }
  | { channelType: 5; channelId: string; thread: DiscoveryThread };

export type DiscoveryFetch = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface DiscoveryRemoteOptions {
  apiUrl: string;
  botToken: string;
  fetch?: DiscoveryFetch;
}

export interface DiscoveryRemote {
  listGroups(signal?: AbortSignal): Promise<DiscoveryGroup[]>;
  listThreads(groupId: string, page: number, signal?: AbortSignal): Promise<DiscoveryThreadPage>;
  searchGroups(query: string, signal?: AbortSignal): Promise<DiscoveryGroupSearchPage>;
  searchMessages(
    query: string,
    channelType: DiscoveryChannelType,
    channelId: string,
    cursor: string | undefined,
    signal?: AbortSignal,
  ): Promise<DiscoveryMessagePage>;
  listWorkspaces(page: number, signal?: AbortSignal): Promise<DiscoveryWorkspacePage>;
  searchTasks(
    workspaceId: string,
    query: string,
    page: number,
    signal?: AbortSignal,
  ): Promise<DiscoveryTaskPage>;
  verifyTarget(target: DiscoveryTarget, signal?: AbortSignal): Promise<DiscoveryTargetVerification>;
  verifyWorkspace(workspaceId: string, signal?: AbortSignal): Promise<DiscoveryWorkspaceDetail>;
}

const OCTO_GROUP_LIST_PATH = "/v1/bot/groups";
const OCTO_GROUP_SEARCH_PATH = "/v1/bot/messages/_search_global_groups";
const OCTO_MESSAGE_SEARCH_PATH = "/v1/bot/messages/_search";
const LOOP_WORKSPACE_LIST_PATH = "/fleet/api/v1/workspaces";
const LOOP_TASK_SEARCH_PATH = "/fleet/api/v1/tasks/_search";
const LOOP_ACCEPT = "application/vnd.octo+json";
const THREAD_PAGE_SIZE = 100;
const LOOP_PAGE_SIZE = 100;
const SEARCH_PAGE_SIZE = 100;
const MAX_SEARCH_KEYWORD_CODE_POINTS = 64;
const MAX_PATH_COMPONENT_LENGTH = 128;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

type Source = "octo" | "loop";

function sourcePrefix(source: Source): string {
  return source === "loop" ? "Loop" : "Octo";
}

function invalidSchema(source: Source, label: string, field?: string): never {
  throw new OctoError(
    `${sourcePrefix(source)} ${label} response has invalid schema${field === undefined ? "" : ` (${field})`}`,
  );
}

function invalidPagination(source: Source, label: string): never {
  throw new OctoError(`${sourcePrefix(source)} ${label} response has invalid pagination`);
}

function inputError(message: string): never {
  throw new OctoError(message, { permanent: true });
}

function requiredText(
  value: unknown,
  field: string,
  source: Source,
  label: string,
  allowEmpty = false,
): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    return invalidSchema(source, label, field);
  }
  return value;
}

function requiredNonNegativeInteger(
  value: unknown,
  field: string,
  source: Source,
  label: string,
): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    return invalidSchema(source, label, field);
  }
  return value as number;
}

function requiredFiniteNumber(
  value: unknown,
  field: string,
  source: Source,
  label: string,
): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return invalidSchema(source, label, field);
  }
  return value;
}

function requiredBoolean(value: unknown, field: string, source: Source, label: string): boolean {
  if (typeof value !== "boolean") return invalidSchema(source, label, field);
  return value;
}

function optionalText(
  record: JsonRecord,
  field: string,
  source: Source,
  label: string,
  allowEmpty = true,
): string | undefined {
  if (!Object.hasOwn(record, field)) return undefined;
  return requiredText(record[field], field, source, label, allowEmpty);
}

function optionalNullableText(
  record: JsonRecord,
  field: string,
  source: Source,
  label: string,
): string | null | undefined {
  if (!Object.hasOwn(record, field)) return undefined;
  const value = record[field];
  if (value === null) return null;
  return requiredText(value, field, source, label, true);
}

function optionalNonNegativeInteger(
  record: JsonRecord,
  field: string,
  source: Source,
  label: string,
): number | undefined {
  if (!Object.hasOwn(record, field)) return undefined;
  return requiredNonNegativeInteger(record[field], field, source, label);
}

function optionalFiniteNumber(
  record: JsonRecord,
  field: string,
  source: Source,
  label: string,
): number | undefined {
  if (!Object.hasOwn(record, field)) return undefined;
  return requiredFiniteNumber(record[field], field, source, label);
}

function requiredChannelType(value: unknown, source: Source, label: string): DiscoveryChannelType {
  if (value !== 1 && value !== 2 && value !== 5) return invalidSchema(source, label);
  return value;
}

function validatePage(page: number): number {
  if (!Number.isSafeInteger(page) || page < 1) inputError("Discovery page must be a positive integer");
  return page;
}

function validateWorkspaceId(workspaceId: string): string {
  if (!UUID_PATTERN.test(workspaceId)) inputError("Discovery workspace ID must be a UUID");
  return workspaceId;
}

function validatePathComponent(value: string, label: string): string {
  if (typeof value !== "string"
    || value.length === 0
    || value.length > MAX_PATH_COMPONENT_LENGTH
    || value.includes("/")) {
    inputError(`Discovery ${label} is invalid`);
  }
  return encodeURIComponent(value);
}

function validateSearchQuery(query: string): string {
  if (typeof query !== "string" || query.trim().length === 0) {
    inputError("Discovery query is required");
  }
  if ([...query].length > MAX_SEARCH_KEYWORD_CODE_POINTS) {
    inputError(`Discovery query must be at most ${MAX_SEARCH_KEYWORD_CODE_POINTS} Unicode code points`);
  }
  return query;
}

function validateOptionalSearchQuery(query: string): string {
  if (typeof query !== "string") inputError("Discovery query must be a string");
  if (query.length > 0 && [...query].length > MAX_SEARCH_KEYWORD_CODE_POINTS) {
    inputError(`Discovery query must be at most ${MAX_SEARCH_KEYWORD_CODE_POINTS} Unicode code points`);
  }
  return query;
}

function validateChannelIdentity(
  channelType: DiscoveryChannelType,
  channelId: string,
): DiscoveryChannelType {
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) {
    inputError("Discovery channel type must be 1, 2, or 5");
  }
  if (typeof channelId !== "string" || channelId.length === 0) {
    inputError("Discovery channel ID is required");
  }
  return channelType;
}

function validateTaskQuery(query: string): string {
  if (typeof query !== "string" || query.trim().length === 0) {
    inputError("Discovery task query is required");
  }
  return query;
}

function permanentStatus(status: number): boolean {
  return status === 400 || status === 401 || status === 403 || status === 404;
}

function loopStatusError(label: string, status: number, body: Uint8Array): OctoError {
  const permanent = permanentStatus(status);
  if (body.byteLength === 0) {
    return new OctoError(`Loop ${label} request failed with HTTP ${status}`, { status, permanent });
  }
  const code = serverErrorCode(body);
  const detail = serverErrorMessage(body);
  if (code === undefined && detail === null) {
    return new OctoError(
      `Loop ${label} request failed with HTTP ${status} (non-Loop response body)`,
      { status, permanent },
    );
  }
  const reason = code === undefined ? `HTTP ${status}` : `HTTP ${status} (${code})`;
  const message = detail === null
    ? `Loop ${label} request failed with ${reason}`
    : `Loop ${label} request failed with ${reason}: ${detail}`;
  return new OctoError(message, {
    status,
    permanent,
    ...(code === undefined ? {} : { code }),
  });
}

function httpErrorFor(source: Source, label: string, status: number, body: Uint8Array): OctoError {
  if (source === "loop") return loopStatusError(label, status, body);
  return httpStatusError(label, status, body, permanentStatus(status));
}

function assertRecord(value: unknown, source: Source, label: string): JsonRecord {
  if (!isJsonRecord(value)) return invalidSchema(source, label);
  return value;
}

function groupFromRecord(value: unknown): DiscoveryGroup {
  const label = "discovery group list";
  const record = assertRecord(value, "octo", label);
  requiredText(record["group_no"], "group_no", "octo", label);
  requiredText(record["name"], "name", "octo", label, true);
  optionalText(record, "space_id", "octo", label);
  return record as unknown as DiscoveryGroup;
}

function threadFromRecord(
  value: unknown,
  label: string,
  expectedGroup: string,
  expectedShort?: string,
): DiscoveryThread {
  const record = assertRecord(value, "octo", label);
  const shortId = requiredText(record["short_id"], "short_id", "octo", label);
  const groupNo = requiredText(record["group_no"], "group_no", "octo", label);
  if (groupNo !== expectedGroup || (expectedShort !== undefined && shortId !== expectedShort)) {
    return invalidSchema("octo", label);
  }
  const channelId = requiredText(record["channel_id"], "channel_id", "octo", label);
  if (channelId !== threadChannelId(groupNo, shortId)) return invalidSchema("octo", label);
  requiredText(record["name"], "name", "octo", label);
  requiredNonNegativeInteger(record["status"], "status", "octo", label);

  optionalText(record, "group_name", "octo", label);
  optionalText(record, "creator_uid", "octo", label);
  if (Object.hasOwn(record, "channel_type")) {
    const channelType = requiredFiniteNumber(record["channel_type"], "channel_type", "octo", label);
    if (channelType !== 5) return invalidSchema("octo", label);
  }
  optionalNonNegativeInteger(record, "member_count", "octo", label);
  optionalNonNegativeInteger(record, "message_count", "octo", label);
  optionalText(record, "last_message_at", "octo", label);
  optionalText(record, "created_at", "octo", label);
  optionalText(record, "updated_at", "octo", label);
  return record as unknown as DiscoveryThread;
}

function threadPageFromResponse(
  value: unknown,
  label: string,
  expectedGroup: string,
  maxItems: number,
): DiscoveryThreadPage {
  const record = assertRecord(value, "octo", label);
  const count = requiredNonNegativeInteger(record["count"], "count", "octo", label);
  const list = record["list"];
  if (!Array.isArray(list) || list.length > maxItems) return invalidSchema("octo", label);
  return {
    count,
    list: list.map((entry) => threadFromRecord(entry, label, expectedGroup)),
  };
}

function messageHitFromRecord(
  value: unknown,
  label: string,
  expected?: { channelType: DiscoveryChannelType; channelId: string },
): DiscoveryMessageHit {
  const record = assertRecord(value, "octo", label);
  requiredText(record["message_id"], "message_id", "octo", label);
  const channelId = requiredText(record["channel_id"], "channel_id", "octo", label);
  requiredText(record["sender_id"], "sender_id", "octo", label);
  requiredText(record["sent_at"], "sent_at", "octo", label);

  if (Object.hasOwn(record, "channel_type")) {
    const channelType = requiredChannelType(record["channel_type"], "octo", label);
    if (expected !== undefined && channelType !== expected.channelType) {
      return invalidSchema("octo", label);
    }
  }
  optionalFiniteNumber(record, "message_seq", "octo", label);
  optionalText(record, "message_kind", "octo", label);
  optionalText(record, "snippet", "octo", label);
  optionalText(record, "sender_name", "octo", label);
  optionalText(record, "sender_avatar_url", "octo", label);
  if (expected !== undefined && channelId !== expected.channelId) return invalidSchema("octo", label);
  return record as unknown as DiscoveryMessageHit;
}

function messagePageFromResponse(
  value: unknown,
  label: string,
  expected: { channelType: DiscoveryChannelType; channelId: string },
): DiscoveryMessagePage {
  const record = assertRecord(value, "octo", label);
  const data = record["data"];
  if (!Array.isArray(data) || data.length > SEARCH_PAGE_SIZE) {
    return invalidSchema("octo", label);
  }
  const pagination = assertRecord(record["pagination"], "octo", label);
  const hasMore = requiredBoolean(pagination["has_more"], "has_more", "octo", label);
  const nextCursor = requiredText(pagination["next_cursor"], "next_cursor", "octo", label, true);
  if (hasMore && nextCursor.length === 0) return invalidPagination("octo", label);
  return {
    data: data.map((hit) => messageHitFromRecord(hit, label, expected)),
    pagination: { has_more: hasMore, next_cursor: nextCursor },
  };
}

function groupBucketFromRecord(value: unknown): DiscoveryGroupBucket {
  const label = "discovery group search";
  const record = assertRecord(value, "octo", label);
  const channelId = requiredText(record["channel_id"], "channel_id", "octo", label);
  const channelType = requiredChannelType(record["channel_type"], "octo", label);
  requiredText(record["group_name"], "group_name", "octo", label, true);
  requiredNonNegativeInteger(record["match_count"], "match_count", "octo", label);
  requiredBoolean(record["match_count_approx"], "match_count_approx", "octo", label);
  requiredText(record["latest_at"], "latest_at", "octo", label, true);
  const preview = record["preview"];
  if (!Array.isArray(preview)) return invalidSchema("octo", label);
  for (const hit of preview) messageHitFromRecord(hit, label);
  const parentGroupNo = optionalText(record, "parent_group_no", "octo", label, false);
  const threadId = optionalText(record, "thread_id", "octo", label, false);
  optionalText(record, "thread_name", "octo", label);
  if (channelType === 2 && parentGroupNo !== undefined && parentGroupNo !== channelId) {
    return invalidSchema("octo", label);
  }
  if (channelType === 5 && parentGroupNo !== undefined && threadId !== undefined
    && channelId !== threadChannelId(parentGroupNo, threadId)) {
    return invalidSchema("octo", label);
  }
  return record as unknown as DiscoveryGroupBucket;
}

function groupSearchPageFromResponse(value: unknown): DiscoveryGroupSearchPage {
  const label = "discovery group search";
  const record = assertRecord(value, "octo", label);
  const data = assertRecord(record["data"], "octo", label);
  const pagination = assertRecord(record["pagination"], "octo", label);
  requiredFiniteNumber(data["sequence"], "sequence", "octo", label);
  requiredText(data["query_id"], "query_id", "octo", label);
  requiredNonNegativeInteger(data["total_groups"], "total_groups", "octo", label);
  requiredBoolean(data["total_groups_approx"], "total_groups_approx", "octo", label);
  const groups = data["groups"];
  if (!Array.isArray(groups)) return invalidSchema("octo", label);
  requiredBoolean(pagination["has_more"], "has_more", "octo", label);
  const nextCursor = requiredText(pagination["next_cursor"], "next_cursor", "octo", label, true);
  if (nextCursor.length > 0) return invalidPagination("octo", label);
  for (const group of groups) groupBucketFromRecord(group);
  return record as unknown as DiscoveryGroupSearchPage;
}

function workspaceRowFromRecord(value: unknown): DiscoveryWorkspaceRow {
  const label = "discovery workspace list";
  const record = assertRecord(value, "loop", label);
  const workspaceId = requiredText(record["workspace_id"], "workspace_id", "loop", label);
  if (!UUID_PATTERN.test(workspaceId)) return invalidSchema("loop", label);
  requiredText(record["name"], "name", "loop", label);
  requiredText(record["slug"], "slug", "loop", label);
  optionalNullableText(record, "description", "loop", label);
  optionalNullableText(record, "context", "loop", label);
  optionalText(record, "issue_prefix", "loop", label);
  optionalNullableText(record, "avatar_url", "loop", label);
  optionalText(record, "status", "loop", label);
  optionalFiniteNumber(record, "external_version", "loop", label);
  optionalText(record, "created_at", "loop", label);
  optionalText(record, "updated_at", "loop", label);
  return record as unknown as DiscoveryWorkspaceRow;
}

function validateLoopPagination(
  value: unknown,
  label: string,
  expectedPage: number,
): DiscoveryLoopPagination {
  const pagination = assertRecord(value, "loop", label);
  const total = requiredNonNegativeInteger(pagination["total"], "total", "loop", label);
  const page = requiredNonNegativeInteger(pagination["page"], "page", "loop", label);
  const pageSize = requiredNonNegativeInteger(pagination["page_size"], "page_size", "loop", label);
  if (page !== expectedPage || pageSize !== LOOP_PAGE_SIZE) return invalidPagination("loop", label);
  return { total, page, page_size: pageSize };
}

function loopPageFromResponse<T>(
  value: unknown,
  label: string,
  expectedPage: number,
  mapRow: (row: unknown) => T,
): { data: T[]; pagination: DiscoveryLoopPagination } {
  const record = assertRecord(value, "loop", label);
  const rows = record["data"];
  if (!Array.isArray(rows) || rows.length > LOOP_PAGE_SIZE) return invalidSchema("loop", label);
  return {
    data: rows.map(mapRow),
    pagination: validateLoopPagination(record["pagination"], label, expectedPage),
  };
}

function taskFromRecord(value: unknown, expectedWorkspace: string): DiscoveryTask {
  const label = "discovery task search";
  const record = assertRecord(value, "loop", label);
  const taskId = requiredText(record["task_id"], "task_id", "loop", label);
  if (!UUID_PATTERN.test(taskId)) return invalidSchema("loop", label);
  requiredText(record["title"], "title", "loop", label);
  requiredText(record["status"], "status", "loop", label);
  if (Object.hasOwn(record, "workspace_id")) {
    const workspaceId = requiredText(record["workspace_id"], "workspace_id", "loop", label);
    if (!UUID_PATTERN.test(workspaceId)
      || workspaceId.toLowerCase() !== expectedWorkspace.toLowerCase()) {
      return invalidSchema("loop", label);
    }
  }
  optionalText(record, "identifier", "loop", label);
  for (const field of [
    "description",
    "priority",
    "assignee_type",
    "assignee_id",
    "assignee_octo_uid",
    "project_id",
    "parent_task_id",
    "start_date",
    "due_date",
  ]) {
    optionalNullableText(record, field, "loop", label);
  }
  optionalText(record, "created_at", "loop", label);
  optionalText(record, "updated_at", "loop", label);
  optionalFiniteNumber(record, "version", "loop", label);
  return record as unknown as DiscoveryTask;
}

function workspaceDetailFromResponse(
  value: unknown,
  expectedWorkspace: string,
): DiscoveryWorkspaceDetail {
  const label = "discovery workspace";
  const record = assertRecord(value, "loop", label);
  const hasId = Object.hasOwn(record, "id");
  const hasWorkspaceId = Object.hasOwn(record, "workspace_id");
  if (!hasId && !hasWorkspaceId) return invalidSchema("loop", label);
  const id = hasId ? requiredText(record["id"], "id", "loop", label) : undefined;
  const workspaceId = hasWorkspaceId
    ? requiredText(record["workspace_id"], "workspace_id", "loop", label)
    : undefined;
  if (id !== undefined && !UUID_PATTERN.test(id)) return invalidSchema("loop", label);
  if (workspaceId !== undefined && !UUID_PATTERN.test(workspaceId)) {
    return invalidSchema("loop", label);
  }
  if (id !== undefined && workspaceId !== undefined
    && id.toLowerCase() !== workspaceId.toLowerCase()) {
    return invalidSchema("loop", label);
  }
  const responseId = id ?? workspaceId;
  if (responseId === undefined || responseId.toLowerCase() !== expectedWorkspace.toLowerCase()) {
    return invalidSchema("loop", label);
  }
  requiredText(record["name"], "name", "loop", label);
  requiredText(record["slug"], "slug", "loop", label);
  optionalNullableText(record, "description", "loop", label);
  optionalNullableText(record, "context", "loop", label);
  optionalText(record, "issue_prefix", "loop", label);
  optionalNullableText(record, "avatar_url", "loop", label);
  optionalText(record, "status", "loop", label);
  optionalFiniteNumber(record, "external_version", "loop", label);
  optionalText(record, "created_at", "loop", label);
  optionalText(record, "updated_at", "loop", label);
  return record as unknown as DiscoveryWorkspaceDetail;
}

export function createDiscoveryRemote(options: DiscoveryRemoteOptions): DiscoveryRemote {
  let apiUrl: string;
  try {
    const parsed = new URL(options.apiUrl);
    if ((parsed.protocol !== "http:" && parsed.protocol !== "https:")
      || parsed.username.length > 0 || parsed.password.length > 0) {
      throw new Error("unsupported API origin");
    }
    apiUrl = options.apiUrl;
  } catch {
    throw new OctoError("Discovery API URL is invalid", { permanent: true });
  }
  if (typeof options.botToken !== "string" || options.botToken.length === 0) {
    throw new OctoError("Discovery bot token is required", { permanent: true });
  }
  const fetchImpl = options.fetch ?? globalThis.fetch;

  async function requestJson(
    path: string,
    init: RequestInit,
    source: Source,
    label: string,
    signal: AbortSignal | undefined,
  ): Promise<unknown> {
    const deadline = requestDeadline(signal, HTTP_TIMEOUT_MS);
    try {
      const response = await fetchImpl(buildApiUrl(apiUrl, path), {
        ...init,
        signal: deadline.signal,
      });
      const body = await readBody(response, MAX_HTTP_BODY_BYTES);
      if (response.status < 200 || response.status >= 300) {
        throw httpErrorFor(source, label, response.status, body);
      }
      return parseJson(body, label);
    } catch (error) {
      if (error instanceof OctoError) throw error;
      if (deadline.timedOut()) {
        throw new OctoError(`${sourcePrefix(source)} ${label} request timed out`);
      }
      if (deadline.aborted()) {
        throw new OctoError(`${sourcePrefix(source)} ${label} request was aborted`);
      }
      throw new OctoError(`${sourcePrefix(source)} ${label} request failed`);
    } finally {
      deadline.release();
    }
  }

  function octoHeaders(): HeadersInit {
    return authHeaders(options.botToken);
  }

  function loopHeaders(workspaceId?: string): HeadersInit {
    return {
      ...authHeaders(options.botToken),
      Accept: LOOP_ACCEPT,
      ...(workspaceId === undefined ? {} : { "X-Workspace-ID": workspaceId }),
    };
  }

  async function listGroups(signal?: AbortSignal): Promise<DiscoveryGroup[]> {
    const value = await requestJson(
      OCTO_GROUP_LIST_PATH,
      { method: "GET", headers: octoHeaders() },
      "octo",
      "discovery group list",
      signal,
    );
    if (!Array.isArray(value)) return invalidSchema("octo", "discovery group list");
    return value.map(groupFromRecord);
  }

  async function listThreads(
    groupId: string,
    page: number,
    signal?: AbortSignal,
  ): Promise<DiscoveryThreadPage> {
    const safePage = validatePage(page);
    const group = validatePathComponent(groupId, "group ID");
    const query = new URLSearchParams({
      page_index: String(safePage),
      page_size: String(THREAD_PAGE_SIZE),
    });
    const value = await requestJson(
      `/v1/bot/groups/${group}/threads?${query.toString()}`,
      { method: "GET", headers: octoHeaders() },
      "octo",
      "discovery thread list",
      signal,
    );
    return threadPageFromResponse(value, "discovery thread list", groupId, THREAD_PAGE_SIZE);
  }

  async function searchGroups(
    query: string,
    signal?: AbortSignal,
  ): Promise<DiscoveryGroupSearchPage> {
    const keyword = validateSearchQuery(query);
    const value = await requestJson(
      OCTO_GROUP_SEARCH_PATH,
      {
        method: "POST",
        headers: { ...octoHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({ keyword }),
      },
      "octo",
      "discovery group search",
      signal,
    );
    return groupSearchPageFromResponse(value);
  }

  async function searchMessages(
    query: string,
    channelType: DiscoveryChannelType,
    channelId: string,
    cursor: string | undefined,
    signal?: AbortSignal,
  ): Promise<DiscoveryMessagePage> {
    const keyword = validateOptionalSearchQuery(query);
    const type = validateChannelIdentity(channelType, channelId);
    if (cursor !== undefined && typeof cursor !== "string") {
      inputError("Discovery message cursor must be a string");
    }
    const body: Record<string, unknown> = {
      keyword,
      channel_type: type,
      channel_id: channelId,
      page_size: SEARCH_PAGE_SIZE,
      ...(cursor === undefined || cursor.length === 0 ? {} : { cursor }),
    };
    const value = await requestJson(
      OCTO_MESSAGE_SEARCH_PATH,
      {
        method: "POST",
        headers: { ...octoHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
      "octo",
      "discovery message search",
      signal,
    );
    return messagePageFromResponse(value, "discovery message search", {
      channelType: type,
      channelId,
    });
  }

  async function listWorkspaces(
    page: number,
    signal?: AbortSignal,
  ): Promise<DiscoveryWorkspacePage> {
    const safePage = validatePage(page);
    const query = new URLSearchParams({
      page: String(safePage),
      page_size: String(LOOP_PAGE_SIZE),
    });
    const value = await requestJson(
      `${LOOP_WORKSPACE_LIST_PATH}?${query.toString()}`,
      { method: "GET", headers: loopHeaders() },
      "loop",
      "discovery workspace list",
      signal,
    );
    return loopPageFromResponse(
      value,
      "discovery workspace list",
      safePage,
      workspaceRowFromRecord,
    );
  }

  async function searchTasks(
    workspaceId: string,
    query: string,
    page: number,
    signal?: AbortSignal,
  ): Promise<DiscoveryTaskPage> {
    const workspace = validateWorkspaceId(workspaceId);
    const text = validateTaskQuery(query);
    const safePage = validatePage(page);
    const params = new URLSearchParams({
      q: text,
      page: String(safePage),
      page_size: String(LOOP_PAGE_SIZE),
    });
    const value = await requestJson(
      `${LOOP_TASK_SEARCH_PATH}?${params.toString()}`,
      { method: "GET", headers: loopHeaders(workspace) },
      "loop",
      "discovery task search",
      signal,
    );
    return loopPageFromResponse(
      value,
      "discovery task search",
      safePage,
      (row) => taskFromRecord(row, workspace),
    );
  }

  async function verifyTarget(
    target: DiscoveryTarget,
    signal?: AbortSignal,
  ): Promise<DiscoveryTargetVerification> {
    if (!isJsonRecord(target)) inputError("Discovery target is invalid");
    const channelType = target["channelType"];
    const channelId = target["channelId"];
    if (channelType !== 2 && channelType !== 5) {
      inputError("Discovery channel type must be 2 or 5");
    }
    if (typeof channelId !== "string" || channelId.length === 0) {
      inputError("Discovery channel ID is required");
    }

    if (channelType === 2) {
      const group = validatePathComponent(channelId, "group ID");
      const query = new URLSearchParams({ page_index: "1", page_size: "1" });
      const value = await requestJson(
        `/v1/bot/groups/${group}/threads?${query.toString()}`,
        { method: "GET", headers: octoHeaders() },
        "octo",
        "discovery group access",
        signal,
      );
      threadPageFromResponse(value, "discovery group access", channelId, 1);
      return { channelType: 2, channelId };
    }

    const parentGroupId = threadParentGroupId(channelId);
    const shortId = threadShortId(channelId);
    if (parentGroupId === null || shortId === null) {
      inputError("Discovery thread target must contain a parent and topic id");
    }
    const group = validatePathComponent(parentGroupId, "group ID");
    const thread = validatePathComponent(shortId, "thread ID");
    const value = await requestJson(
      `/v1/bot/groups/${group}/threads/${thread}`,
      { method: "GET", headers: octoHeaders() },
      "octo",
      "discovery thread access",
      signal,
    );
    const detail = threadFromRecord(value, "discovery thread access", parentGroupId, shortId);
    return { channelType: 5, channelId, thread: detail };
  }

  async function verifyWorkspace(
    workspaceId: string,
    signal?: AbortSignal,
  ): Promise<DiscoveryWorkspaceDetail> {
    const workspace = validateWorkspaceId(workspaceId);
    const value = await requestJson(
      `${LOOP_WORKSPACE_LIST_PATH}/${encodeURIComponent(workspace)}`,
      { method: "GET", headers: loopHeaders(workspace) },
      "loop",
      "discovery workspace",
      signal,
    );
    return workspaceDetailFromResponse(value, workspace);
  }

  return {
    listGroups,
    listThreads,
    searchGroups,
    searchMessages,
    listWorkspaces,
    searchTasks,
    verifyTarget,
    verifyWorkspace,
  };
}
