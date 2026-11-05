import assert from "node:assert/strict";
import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LoopController } from "../src/loop/controller.ts";
import { createLoopReadPort } from "../src/loop/api.ts";
import type { LoopBinding, LoopInput, LoopTask } from "../src/loop/types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";

const API_TOKEN = "local-loop-http-fixture-token";
const BOT_UID = "loop-http-integration-bot";
const OWNER_UID = "loop-http-integration-owner";
const GROUP_ID = "loop-http-integration-group";
const WORKSPACE_ID = "00000000-0000-4000-8000-000000000001";
const OTHER_WORKSPACE_ID = "00000000-0000-4000-8000-000000000002";
const TASK_ID = "00000000-0000-4000-8000-000000000101";

interface ApiState {
  workspaceList: Record<string, unknown>[];
  workspaceDetails: Map<string, Record<string, unknown>>;
  taskLists: Map<string, Record<string, unknown>[]>;
  taskDetails: Map<string, Record<string, unknown>>;
}

interface CapturedRequest {
  method: string;
  url: URL;
  authorization: string | null;
  workspaceId: string | null;
}

interface ApiFixture {
  apiUrl: string;
  requests: CapturedRequest[];
  stop(): void;
}

interface PersistedState {
  binding: LoopBinding | null;
  inputs: LoopInput[];
  snapshots: Array<{ task_id: string; task_json: string; business_json: string }>;
}

function emptyApiState(): ApiState {
  return {
    workspaceList: [],
    workspaceDetails: new Map(),
    taskLists: new Map(),
    taskDetails: new Map(),
  };
}

function workspaceListRecord(
  workspaceId: string,
  name: string,
  slug: string,
): Record<string, unknown> {
  return {
    workspace_id: workspaceId,
    name,
    slug,
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: "LOOP",
    avatar_url: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
}

function workspaceDetailRecord(
  id: string,
  name: string,
  slug: string,
): Record<string, unknown> {
  return {
    id,
    name,
    slug,
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: "LOOP",
    avatar_url: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
}

function taskRecord(
  workspaceId: string,
  taskId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    task_id: taskId,
    workspace_id: workspaceId,
    title: "Prepare the release",
    description: "Collect the release notes",
    status: "in_progress",
    priority: "high",
    assignee_type: "member",
    assignee_id: "fleet-owner-user-id",
    assignee_octo_uid: OWNER_UID,
    project_id: null,
    parent_task_id: null,
    start_date: "2026-11-01",
    due_date: "2026-11-30",
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-02T00:00:00Z",
    ...overrides,
  };
}

function loopTask(): LoopTask {
  return {
    taskId: TASK_ID,
    title: "Prepare the release",
    description: "Collect the release notes",
    status: "in_progress",
    priority: "high",
    assigneeType: "member",
    assigneeId: "fleet-owner-user-id",
    assigneeOctoUid: OWNER_UID,
    projectId: null,
    parentTaskId: null,
    startDate: "2026-11-01",
    dueDate: "2026-11-30",
    createdAt: "2026-10-01T00:00:00Z",
    updatedAt: "2026-10-02T00:00:00Z",
  };
}

function pageEnvelope(rows: Record<string, unknown>[], page: number): Response {
  const pageSize = 100;
  const start = (page - 1) * pageSize;
  return Response.json({
    data: rows.slice(start, start + pageSize),
    pagination: { total: rows.length, page, page_size: pageSize },
  });
}

function startApiFixture(state: ApiState): ApiFixture {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        url,
        authorization: request.headers.get("authorization"),
        workspaceId: request.headers.get("x-workspace-id"),
      });

      if (url.pathname === "/fleet/api/v1/workspaces") {
        const page = Number(url.searchParams.get("page"));
        return pageEnvelope(state.workspaceList, page);
      }
      if (url.pathname.startsWith("/fleet/api/v1/workspaces/")) {
        const workspaceId = decodeURIComponent(url.pathname.slice("/fleet/api/v1/workspaces/".length));
        const detail = state.workspaceDetails.get(workspaceId);
        return detail === undefined
          ? Response.json({ error: { code: "NOT_FOUND", message: "workspace not found" } }, { status: 404 })
          : Response.json(detail);
      }
      if (url.pathname === "/fleet/api/v1/tasks") {
        const workspaceId = request.headers.get("x-workspace-id");
        const page = Number(url.searchParams.get("page"));
        return pageEnvelope(workspaceId === null ? [] : state.taskLists.get(workspaceId) ?? [], page);
      }
      if (url.pathname.startsWith("/fleet/api/v1/tasks/")) {
        const taskId = decodeURIComponent(url.pathname.slice("/fleet/api/v1/tasks/".length));
        const detail = state.taskDetails.get(taskId);
        return detail === undefined
          ? Response.json({ error: { code: "NOT_FOUND", message: "task not found" } }, { status: 404 })
          : Response.json({ data: detail });
      }
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    },
  });

  return {
    apiUrl: server.url.origin,
    requests,
    stop: () => server.stop(true),
  };
}

async function withHarness(
  state: ApiState,
  run: (harness: {
    root: string;
    api: ApiFixture;
    store: ChannelStore;
    controller: LoopController;
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-loop-http-"));
  const api = startApiFixture(state);
  let store: ChannelStore | undefined;
  let controller: LoopController | undefined;
  try {
    store = await openChannelStore(root);
    store.setOwnerId(BOT_UID, OWNER_UID);
    controller = new LoopController({
      store,
      read: createLoopReadPort({ apiUrl: api.apiUrl, botToken: API_TOKEN }),
      botUid: BOT_UID,
      validateTarget: async () => {},
      deliver: async () => "not-accepted",
      isStopping: () => false,
      notifyFault: async () => {},
    });
    await controller.recover();
    await run({ root, api, store, controller });
  } finally {
    await controller?.stop();
    api.stop();
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
}

function readPersistedState(store: ChannelStore): PersistedState {
  const database = new Database(store.path, { readonly: true });
  try {
    return {
      binding: store.getLoopBinding(BOT_UID, GROUP_ID),
      inputs: store.listLoopInputs(BOT_UID),
      snapshots: database.query(
        `SELECT task_id, task_json, business_json FROM loop_task_snapshots
         WHERE bot_uid = ? AND group_id = ? ORDER BY workspace_id, task_id`,
      ).all(BOT_UID, GROUP_ID) as PersistedState["snapshots"],
    };
  } finally {
    database.close();
  }
}

test("LoopController binds a workspace from local HTTP and persists its paginated task baseline", async () => {
  const state = emptyApiState();
  const tasks = Array.from({ length: 101 }, (_, index) => {
    const taskId = index === 0
      ? TASK_ID
      : `00000000-0000-4000-8000-${String(101 + index).padStart(12, "0")}`;
    return taskRecord(WORKSPACE_ID, taskId);
  });
  const incompleteTask = tasks[0]!;
  delete incompleteTask.description;
  const taskDetail = taskRecord(WORKSPACE_ID, TASK_ID);
  state.workspaceList = [workspaceListRecord(WORKSPACE_ID, "Platform Ops", "platform-ops")];
  state.workspaceDetails.set(WORKSPACE_ID, workspaceDetailRecord(WORKSPACE_ID, "Platform Ops", "platform-ops"));
  state.taskLists.set(WORKSPACE_ID, tasks);
  state.taskDetails.set(TASK_ID, taskDetail);

  await withHarness(state, async ({ api, controller, store }) => {
    const result = await controller.bind(GROUP_ID, "Platform Ops");

    expect(result.changed).toBe(true);
    expect(result.binding).toMatchObject({
      botUid: BOT_UID,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE_ID,
      name: "Platform Ops",
      slug: "platform-ops",
      version: 1,
    });
    expect(store.getLoopBinding(BOT_UID, GROUP_ID)?.workspaceId).toBe(WORKSPACE_ID);

    const baseline = store.listLoopInputs(BOT_UID).filter((input) => input.kind === "baseline");
    expect(baseline).toHaveLength(6);
    for (const input of baseline) {
      expect(input).toMatchObject({
        groupId: GROUP_ID,
        workspaceId: WORKSPACE_ID,
        taskId: null,
      });
    }
    const baselineTasks = baseline.flatMap((input) => input.tasks);
    expect(baselineTasks).toHaveLength(101);
    expect(baselineTasks.find(({ taskId }) => taskId === TASK_ID)).toEqual(loopTask());

    const persisted = readPersistedState(store);
    expect(persisted.snapshots).toHaveLength(101);
    expect(JSON.parse(persisted.snapshots.find(({ task_id }) => task_id === TASK_ID)!.task_json)).toEqual(loopTask());
    expect(api.requests.map(({ url }) => url.pathname)).toEqual([
      "/fleet/api/v1/workspaces",
      `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
      "/fleet/api/v1/tasks",
      "/fleet/api/v1/tasks",
      `/fleet/api/v1/tasks/${TASK_ID}`,
    ]);
    const taskListRequests = api.requests.filter(({ url }) => url.pathname === "/fleet/api/v1/tasks");
    expect(taskListRequests.map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2"]);
    expect(taskListRequests.map(({ url }) => url.searchParams.get("page_size"))).toEqual(["100", "100"]);
    for (const request of api.requests) {
      expect(request.method).toBe("GET");
      expect(request.authorization).toBe(`Bearer ${API_TOKEN}`);
      expect(request.workspaceId).toBe(
        request.url.pathname === "/fleet/api/v1/workspaces" ? null : WORKSPACE_ID,
      );
    }
  });
});

test("ambiguous workspace selectors and mismatched detail reads leave the persisted binding and baseline unchanged", async () => {
  const state = emptyApiState();
  state.workspaceDetails.set(WORKSPACE_ID, workspaceDetailRecord(WORKSPACE_ID, "Existing Workspace", "existing-workspace"));
  state.taskLists.set(WORKSPACE_ID, [taskRecord(WORKSPACE_ID, TASK_ID)]);

  await withHarness(state, async ({ api, controller, store }) => {
    await controller.bind(GROUP_ID, WORKSPACE_ID);
    const beforeFailure = readPersistedState(store);
    expect(beforeFailure.binding?.workspaceId).toBe(WORKSPACE_ID);
    expect(beforeFailure.inputs.filter((input) => input.kind === "baseline")).toHaveLength(1);
    expect(beforeFailure.snapshots).toHaveLength(1);

    state.workspaceList = [
      workspaceListRecord(WORKSPACE_ID, "Cross Field Selector", "existing-workspace"),
      workspaceListRecord(OTHER_WORKSPACE_ID, "Other Workspace", "Cross Field Selector"),
    ];
    await assert.rejects(controller.bind(GROUP_ID, "Cross Field Selector"), /ambiguous|multiple/i);
    expect(readPersistedState(store)).toEqual(beforeFailure);

    const replacementId = "00000000-0000-4000-8000-000000000003";
    state.workspaceDetails.set(
      replacementId,
      workspaceDetailRecord(OTHER_WORKSPACE_ID, "Replacement", "replacement"),
    );
    await assert.rejects(controller.bind(GROUP_ID, replacementId), /workspace|identity|schema/i);
    expect(readPersistedState(store)).toEqual(beforeFailure);

    expect(api.requests.some(({ url }) => url.pathname === "/fleet/api/v1/workspaces")).toBe(true);
    expect(api.requests.some(({ url, workspaceId }) =>
      url.pathname === `/fleet/api/v1/workspaces/${replacementId}` && workspaceId === replacementId)).toBe(true);
  });
});

test("a failed task-detail read during replacement leaves the existing workspace snapshot intact", async () => {
  const state = emptyApiState();
  state.workspaceDetails.set(WORKSPACE_ID, workspaceDetailRecord(WORKSPACE_ID, "Existing Workspace", "existing-workspace"));
  state.taskLists.set(WORKSPACE_ID, [taskRecord(WORKSPACE_ID, TASK_ID)]);

  await withHarness(state, async ({ api, controller, store }) => {
    await controller.bind(GROUP_ID, WORKSPACE_ID);
    const beforeFailure = readPersistedState(store);

    const replacementId = "00000000-0000-4000-8000-000000000003";
    state.workspaceDetails.set(replacementId, workspaceDetailRecord(replacementId, "Replacement", "replacement"));
    const incompleteTask = taskRecord(replacementId, TASK_ID);
    delete incompleteTask.description;
    state.taskLists.set(replacementId, [incompleteTask]);
    state.taskDetails.set(TASK_ID, taskRecord(OTHER_WORKSPACE_ID, TASK_ID));

    await assert.rejects(controller.bind(GROUP_ID, replacementId), /invalid|task|workspace/i);
    expect(readPersistedState(store)).toEqual(beforeFailure);
    expect(api.requests.some(({ url, workspaceId }) =>
      url.pathname === `/fleet/api/v1/tasks/${TASK_ID}` && workspaceId === replacementId)).toBe(true);
  });
});
