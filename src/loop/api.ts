import { OctoError } from "../octo/error.ts";
import {
  MAX_HTTP_BODY_BYTES,
  apiUrl as buildApiUrl,
  authHeaders,
  parseJson,
  requestWithTimeout,
} from "../octo/http.ts";
import { isJsonRecord, type JsonRecord } from "../octo/type-guards.ts";
import type { LoopReadPort, LoopTask, LoopWorkspace } from "./types.ts";

const API_PREFIX = "/fleet/api/v1";
const WORKSPACE_LIST_PATH = `${API_PREFIX}/workspaces`;
const TASK_LIST_PATH = `${API_PREFIX}/tasks`;
const PAGE_SIZE = 100;
const LOOP_ACCEPT = "application/vnd.octo+json";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
const TASK_FIELDS = [
  "workspace_id",
  "title",
  "description",
  "status",
  "priority",
  "assignee_type",
  "assignee_id",
  "assignee_octo_uid",
  "project_id",
  "parent_task_id",
  "start_date",
  "due_date",
  "created_at",
  "updated_at",
] as const;
const TASK_REQUIRED_TEXT_FIELDS = ["title", "status", "priority", "created_at", "updated_at"] as const;
const TASK_NULLABLE_TEXT_FIELDS = [
  "description",
  "assignee_type",
  "assignee_id",
  "assignee_octo_uid",
  "project_id",
  "parent_task_id",
  "start_date",
  "due_date",
] as const;

interface PageResult {
  rows: JsonRecord[];
  total: number;
}

function invalidResponse(label: string): never {
  throw new OctoError(`Loop ${label} response has invalid schema`);
}

function invalidPagination(label: string): never {
  throw new OctoError(`Loop ${label} response has invalid pagination`);
}

function requiredText(value: unknown, label: string, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    return invalidResponse(label);
  }
  return value;
}

function requiredUuid(value: unknown, label: string): string {
  const id = requiredText(value, label);
  if (!UUID_PATTERN.test(id)) return invalidResponse(label);
  return id;
}

function sameUuid(left: string, right: string): boolean {
  return left.toLowerCase() === right.toLowerCase();
}

function requireUuidInput(value: string, label: string): string {
  if (!UUID_PATTERN.test(value)) {
    throw new OctoError(`Loop ${label} must be a UUID`, { permanent: true });
  }
  return value;
}

function nullableText(record: JsonRecord, field: string, label: string): string | null {
  const value = record[field];
  if (value === null) return null;
  return requiredText(value, label, field === "description");
}

function responseData(value: unknown, label: string): JsonRecord {
  if (!isJsonRecord(value) || !isJsonRecord(value.data)) return invalidResponse(label);
  return value.data;
}

function parsePage(value: unknown, label: string, expectedPage: number): PageResult {
  if (!isJsonRecord(value) || !Array.isArray(value.data) || !isJsonRecord(value.pagination)) {
    return invalidResponse(label);
  }
  const { total, page, page_size: pageSize } = value.pagination;
  if (!Number.isSafeInteger(total) || (total as number) < 0
    || page !== expectedPage || !Number.isSafeInteger(page)
    || pageSize !== PAGE_SIZE) {
    return invalidPagination(label);
  }
  if (value.data.length > PAGE_SIZE) return invalidPagination(label);

  const rows: JsonRecord[] = [];
  for (const row of value.data) {
    if (!isJsonRecord(row)) return invalidResponse(label);
    rows.push(row);
  }
  return { rows, total: total as number };
}

async function readAllPages(
  label: string,
  getPage: (page: number) => Promise<unknown>,
): Promise<PageResult> {
  const rows: JsonRecord[] = [];
  let total: number | undefined;
  let pageCount: number | undefined;

  for (let page = 1; pageCount === undefined || page <= pageCount; page += 1) {
    const current = parsePage(await getPage(page), label, page);
    if (total === undefined) {
      total = current.total;
      pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
    } else if (current.total !== total) {
      throw new OctoError(`Loop ${label} pagination changed during the read`);
    }
    rows.push(...current.rows);
  }

  if (total === undefined) return invalidPagination(label);
  return { rows, total };
}

function workspaceFromRecord(record: JsonRecord, expectedId?: string): LoopWorkspace {
  let workspaceId: string;
  if (expectedId === undefined) {
    workspaceId = requiredUuid(record.workspace_id, "workspace");
    if (Object.hasOwn(record, "id") && !sameUuid(requiredUuid(record.id, "workspace"), workspaceId)) {
      return invalidResponse("workspace");
    }
  } else {
    const hasId = Object.hasOwn(record, "id");
    const hasWorkspaceId = Object.hasOwn(record, "workspace_id");
    if (!hasId && !hasWorkspaceId) return invalidResponse("workspace");

    const recordId = hasId ? requiredUuid(record.id, "workspace") : undefined;
    const legacyId = hasWorkspaceId ? requiredUuid(record.workspace_id, "workspace") : undefined;
    if (recordId !== undefined && legacyId !== undefined && !sameUuid(recordId, legacyId)) {
      return invalidResponse("workspace");
    }
    const detailId = recordId ?? legacyId;
    if (detailId === undefined || !sameUuid(detailId, expectedId)) return invalidResponse("workspace");
    workspaceId = detailId;
  }
  return {
    workspaceId,
    name: requiredText(record.name, "workspace"),
    slug: requiredText(record.slug, "workspace"),
  };
}

function validateTaskScope(record: JsonRecord, workspaceId: string, expectedTaskId?: string): string {
  const taskId = requiredUuid(record.task_id, "task");
  if (expectedTaskId !== undefined && !sameUuid(taskId, expectedTaskId)) return invalidResponse("task");
  if (Object.hasOwn(record, "workspace_id")) {
    const responseWorkspaceId = requiredUuid(record.workspace_id, "task");
    if (!sameUuid(responseWorkspaceId, workspaceId)) return invalidResponse("task");
  }
  return taskId;
}

function validatePresentTaskFields(record: JsonRecord): void {
  for (const field of TASK_REQUIRED_TEXT_FIELDS) {
    if (Object.hasOwn(record, field)) requiredText(record[field], "task");
  }
  for (const field of TASK_NULLABLE_TEXT_FIELDS) {
    if (Object.hasOwn(record, field)) nullableText(record, field, "task");
  }
  if (Object.hasOwn(record, "workspace_id")) requiredUuid(record.workspace_id, "task");
}

function taskFromRecord(record: JsonRecord, workspaceId: string, expectedTaskId?: string): LoopTask {
  const taskId = validateTaskScope(record, workspaceId, expectedTaskId);
  for (const field of TASK_FIELDS) {
    if (!Object.hasOwn(record, field)) return invalidResponse("task");
  }
  return {
    taskId,
    title: requiredText(record.title, "task"),
    description: nullableText(record, "description", "task"),
    status: requiredText(record.status, "task"),
    priority: requiredText(record.priority, "task"),
    assigneeType: nullableText(record, "assignee_type", "task"),
    assigneeId: nullableText(record, "assignee_id", "task"),
    assigneeOctoUid: nullableText(record, "assignee_octo_uid", "task"),
    projectId: nullableText(record, "project_id", "task"),
    parentTaskId: nullableText(record, "parent_task_id", "task"),
    startDate: nullableText(record, "start_date", "task"),
    dueDate: nullableText(record, "due_date", "task"),
    createdAt: requiredText(record.created_at, "task"),
    updatedAt: requiredText(record.updated_at, "task"),
  };
}

function workspaceSlugFromDeepLink(selector: string, apiOrigin: string): string {
  let url: URL;
  try {
    url = new URL(selector);
  } catch {
    throw new OctoError("Loop workspace URL must be a same-origin Fleet issue or project link", { permanent: true });
  }
  if (url.origin !== apiOrigin || url.username.length > 0 || url.password.length > 0) {
    throw new OctoError("Loop workspace URL must be a same-origin Fleet issue or project link", { permanent: true });
  }

  const segments = url.pathname.replace(/\/+$/u, "").split("/").filter(Boolean);
  if (segments.length !== 4 || segments[0] !== "fleet"
    || (segments[2] !== "issues" && segments[2] !== "projects")) {
    throw new OctoError("Loop workspace URL must be a same-origin Fleet issue or project link", { permanent: true });
  }
  let slug: string;
  let resourceId: string;
  try {
    slug = decodeURIComponent(segments[1]!);
    resourceId = decodeURIComponent(segments[3]!);
  } catch {
    throw new OctoError("Loop workspace URL must be a same-origin Fleet issue or project link", { permanent: true });
  }
  if (slug.length === 0 || resourceId.length === 0 || /[\\/]/u.test(slug) || /[\\/]/u.test(resourceId)) {
    throw new OctoError("Loop workspace URL must be a same-origin Fleet issue or project link", { permanent: true });
  }
  return slug;
}

export function createLoopReadPort({ apiUrl, botToken }: { apiUrl: string; botToken: string }): LoopReadPort {
  let apiOrigin: string;
  try {
    const parsed = new URL(apiUrl);
    if ((parsed.protocol !== "http:" && parsed.protocol !== "https:")
      || parsed.username.length > 0 || parsed.password.length > 0) {
      throw new Error("unsupported API origin");
    }
    apiOrigin = parsed.origin;
  } catch {
    throw new OctoError("Loop API URL is invalid", { permanent: true });
  }
  if (botToken.length === 0) throw new OctoError("Loop bot token is required", { permanent: true });

  async function getJson(
    path: string,
    label: string,
    workspaceId: string | undefined,
    signal: AbortSignal | undefined,
  ): Promise<unknown> {
    const result = await requestWithTimeout(
      buildApiUrl(apiUrl, path),
      {
        method: "GET",
        headers: {
          ...authHeaders(botToken),
          Accept: LOOP_ACCEPT,
          ...(workspaceId === undefined ? {} : { "X-Workspace-ID": workspaceId }),
        },
      },
      `Loop ${label}`,
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (result.status < 200 || result.status >= 300) {
      throw new OctoError(`Loop ${label} request failed with HTTP ${result.status}`, {
        status: result.status,
        permanent: result.status === 400 || result.status === 401 || result.status === 403 || result.status === 404,
      });
    }
    return parseJson(result.body, `Loop ${label}`);
  }

  async function readWorkspace(workspaceId: string, signal?: AbortSignal): Promise<LoopWorkspace> {
    requireUuidInput(workspaceId, "workspace ID");
    const response = await getJson(
      `${WORKSPACE_LIST_PATH}/${encodeURIComponent(workspaceId)}`,
      "workspace",
      workspaceId,
      signal,
    );
    if (!isJsonRecord(response)) return invalidResponse("workspace");
    return workspaceFromRecord(response, workspaceId);
  }

  async function listWorkspaces(signal?: AbortSignal): Promise<LoopWorkspace[]> {
    const pageSet = await readAllPages("workspace list", (page) => {
      const query = new URLSearchParams({ page: String(page), page_size: String(PAGE_SIZE) });
      return getJson(`${WORKSPACE_LIST_PATH}?${query}`, "workspace list", undefined, signal);
    });
    const workspaces = new Map<string, LoopWorkspace>();
    for (const row of pageSet.rows) {
      const workspace = workspaceFromRecord(row);
      const key = workspace.workspaceId.toLowerCase();
      const previous = workspaces.get(key);
      if (previous !== undefined && (previous.name !== workspace.name || previous.slug !== workspace.slug)) {
        throw new OctoError("Loop workspace list returned conflicting duplicate records");
      }
      workspaces.set(key, previous ?? workspace);
    }
    if (workspaces.size !== pageSet.total) {
      throw new OctoError("Loop workspace list pagination was incomplete");
    }
    return [...workspaces.values()];
  }

  async function readUniqueWorkspaceMatch(matches: LoopWorkspace[], signal?: AbortSignal): Promise<LoopWorkspace> {
    if (matches.length === 0) throw new OctoError("Loop workspace was not found", { permanent: true });
    if (matches.length > 1) {
      const ids = matches.map(({ workspaceId }) => workspaceId).join(", ");
      throw new OctoError(`Loop workspace selector is ambiguous; candidate IDs: ${ids}`, { permanent: true });
    }
    return readWorkspace(matches[0]!.workspaceId, signal);
  }

  async function resolveByNameOrSlug(value: string, signal?: AbortSignal): Promise<LoopWorkspace> {
    const matches = (await listWorkspaces(signal))
      .filter((workspace) => workspace.name === value || workspace.slug === value);
    return readUniqueWorkspaceMatch(matches, signal);
  }

  async function resolveBySlug(value: string, signal?: AbortSignal): Promise<LoopWorkspace> {
    const matches = (await listWorkspaces(signal)).filter((workspace) => workspace.slug === value);
    return readUniqueWorkspaceMatch(matches, signal);
  }

  async function readTask(workspaceId: string, taskId: string, signal?: AbortSignal): Promise<LoopTask> {
    requireUuidInput(workspaceId, "workspace ID");
    requireUuidInput(taskId, "task ID");
    const response = await getJson(
      `${TASK_LIST_PATH}/${encodeURIComponent(taskId)}`,
      "task detail",
      workspaceId,
      signal,
    );
    return taskFromRecord(responseData(response, "task"), workspaceId, taskId);
  }

  async function listTasks(workspaceId: string, signal?: AbortSignal): Promise<LoopTask[]> {
    requireUuidInput(workspaceId, "workspace ID");
    const pageSet = await readAllPages("task list", (page) => {
      const query = new URLSearchParams({
        page: String(page),
        page_size: String(PAGE_SIZE),
        sort: "created_at",
        direction: "asc",
      });
      return getJson(`${TASK_LIST_PATH}?${query}`, "task list", workspaceId, signal);
    });

    const records = new Map<string, { taskId: string; record: JsonRecord }>();
    for (const record of pageSet.rows) {
      const taskId = validateTaskScope(record, workspaceId);
      validatePresentTaskFields(record);
      const key = taskId.toLowerCase();
      if (!records.has(key)) records.set(key, { taskId, record });
    }
    if (records.size !== pageSet.total) {
      throw new OctoError("Loop task list pagination was incomplete");
    }


    const tasks: LoopTask[] = [];
    for (const { taskId, record } of records.values()) {
      if (TASK_FIELDS.some((field) => !Object.hasOwn(record, field))) {
        tasks.push(await readTask(workspaceId, taskId, signal));
      } else {
        tasks.push(taskFromRecord(record, workspaceId, taskId));
      }
    }
    return tasks;
  }

  async function resolveWorkspace(selector: string, signal?: AbortSignal): Promise<LoopWorkspace> {
    const value = selector.trim();
    if (value.length === 0) throw new OctoError("Loop workspace selector is required", { permanent: true });
    if (UUID_PATTERN.test(value)) return readWorkspace(value, signal);
    if (/^[a-z][a-z0-9+.-]*:\/\//iu.test(value)) {
      const slug = workspaceSlugFromDeepLink(value, apiOrigin);
      return resolveBySlug(slug, signal);
    }
    if (value.startsWith("/") || value.startsWith("\\")) {
      throw new OctoError("Loop workspace selector is invalid", { permanent: true });
    }
    return resolveByNameOrSlug(value, signal);
  }

  return { resolveWorkspace, listTasks, getTask: readTask };
}
