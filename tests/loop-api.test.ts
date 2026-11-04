import assert from "node:assert/strict";
import { describe, it } from "bun:test";
import { createLoopReadPort } from "../src/loop/api.ts";

const TOKEN = "loop-api-fixture-token";
const WORKSPACE_ID = "00000000-0000-4000-8000-000000000001";
const OTHER_WORKSPACE_ID = "00000000-0000-4000-8000-000000000002";
const TASK_ID = "00000000-0000-4000-8000-000000000101";
const OTHER_TASK_ID = "00000000-0000-4000-8000-000000000102";
const FLEET_MEMBER_UUID = "00000000-0000-4000-8000-000000000201";
const MEMBER_OCTO_UID = "octo-user-member-17";

interface CapturedRequest {
  method: string;
  url: URL;
  authorization: string | null;
  accept: string | null;
  workspaceId: string | null;
}

interface ApiFixture {
  apiUrl: string;
  requests: CapturedRequest[];
  stop(): void;
}

function startApiFixture(
  handler: (request: Request, url: URL) => Response | Promise<Response>,
): ApiFixture {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        url,
        authorization: request.headers.get("authorization"),
        accept: request.headers.get("accept"),
        workspaceId: request.headers.get("x-workspace-id"),
      });
      return handler(request, url);
    },
  });
  return {
    apiUrl: server.url.origin,
    requests,
    stop: () => server.stop(true),
  };
}

function workspaceRecord(
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
  workspaceId: string,
  name: string,
  slug: string,
): Record<string, unknown> {
  const record = workspaceRecord(workspaceId, name, slug);
  delete record.workspace_id;
  record.id = workspaceId;
  return record;
}

function taskRecord(taskId: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    task_id: taskId,
    workspace_id: WORKSPACE_ID,
    title: `Task ${taskId}`,
    description: "Task description",
    status: "in_progress",
    priority: "high",
    assignee_type: "expert",
    assignee_id: OTHER_WORKSPACE_ID,
    assignee_octo_uid: null,
    project_id: OTHER_TASK_ID,
    parent_task_id: TASK_ID,
    start_date: "2026-11-01",
    due_date: "2026-11-30",
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-02T00:00:00Z",
    version: 1,
    ...overrides,
  };
}

function pageEnvelope(
  data: Record<string, unknown>[],
  page: number,
  total: number,
  pageSize = 100,
): Response {
  return Response.json({
    data,
    pagination: { total, page, page_size: pageSize },
  });
}

function workspaceEnvelope(data: Record<string, unknown>): Response {
  return Response.json({ data });
}

describe("Loop REST read port", () => {
  it("reads a UUID workspace and a task directly without enumerating workspaces", async () => {
    const workspace = workspaceRecord(WORKSPACE_ID, "Platform Ops", "platform-ops");
    const task = taskRecord(TASK_ID, {
      assignee_type: "member",
      assignee_id: FLEET_MEMBER_UUID,
      assignee_octo_uid: MEMBER_OCTO_UID,
    });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) {
        return workspaceEnvelope(workspace);
      }
      if (url.pathname === `/fleet/api/v1/tasks/${TASK_ID}`) {
        return workspaceEnvelope(task);
      }
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await port.resolveWorkspace(WORKSPACE_ID), {
        workspaceId: WORKSPACE_ID,
        name: "Platform Ops",
        slug: "platform-ops",
      });
      assert.notEqual(FLEET_MEMBER_UUID, MEMBER_OCTO_UID);
      assert.deepEqual(await port.getTask(WORKSPACE_ID, TASK_ID), {
        taskId: TASK_ID,
        title: `Task ${TASK_ID}`,
        description: "Task description",
        status: "in_progress",
        priority: "high",
        assigneeType: "member",
        assigneeId: FLEET_MEMBER_UUID,
        assigneeOctoUid: MEMBER_OCTO_UID,
        projectId: OTHER_TASK_ID,
        parentTaskId: TASK_ID,
        startDate: "2026-11-01",
        dueDate: "2026-11-30",
        createdAt: "2026-10-01T00:00:00Z",
        updatedAt: "2026-10-02T00:00:00Z",
      });
      assert.equal(fixture.requests.length, 2);
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), [
        `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
        `/fleet/api/v1/tasks/${TASK_ID}`,
      ]);
      for (const request of fixture.requests) {
        assert.equal(request.method, "GET");
        assert.equal(request.authorization, `Bearer ${TOKEN}`);
        assert.equal(request.workspaceId, WORKSPACE_ID);
        assert.equal(request.accept, "application/vnd.octo+json");
        assert.equal(request.url.searchParams.has("token"), false);
      }
    } finally {
      fixture.stop();
    }
  });

  it("resolves UUID, name, bare slug, and same-origin deep links using workspace_id list rows and id details", async () => {
    const workspace = workspaceRecord(WORKSPACE_ID, "Platform Ops", "platform-ops");
    const detail = workspaceDetailRecord(WORKSPACE_ID, "Platform Ops", "platform-ops");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") return pageEnvelope([workspace], 1, 1);
      if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) return workspaceEnvelope(detail);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      const expected = {
        workspaceId: WORKSPACE_ID,
        name: "Platform Ops",
        slug: "platform-ops",
      };
      for (const selector of [
        WORKSPACE_ID,
        "Platform Ops",
        "platform-ops",
        `${fixture.apiUrl}/fleet/platform-ops/issues/LOOP-12`,
      ]) {
        assert.deepEqual(await port.resolveWorkspace(selector), expected);
      }
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), [
        `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
        "/fleet/api/v1/workspaces",
        `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
        "/fleet/api/v1/workspaces",
        `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
        "/fleet/api/v1/workspaces",
        `/fleet/api/v1/workspaces/${WORKSPACE_ID}`,
      ]);
    } finally {
      fixture.stop();
    }
  });

  it("accepts matching id and legacy workspace_id identities in workspace details", async () => {
    const detail = {
      ...workspaceDetailRecord(WORKSPACE_ID, "Platform Ops", "platform-ops"),
      workspace_id: WORKSPACE_ID,
    };
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) return workspaceEnvelope(detail);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await port.resolveWorkspace(WORKSPACE_ID), {
        workspaceId: WORKSPACE_ID,
        name: "Platform Ops",
        slug: "platform-ops",
      });
    } finally {
      fixture.stop();
    }
  });

  it("rejects workspace details with a wrong or conflicting identity, or no identity", async () => {
    const details = [
      workspaceDetailRecord(OTHER_WORKSPACE_ID, "Platform Ops", "platform-ops"),
      workspaceRecord(OTHER_WORKSPACE_ID, "Platform Ops", "platform-ops"),
      {
        ...workspaceDetailRecord(WORKSPACE_ID, "Platform Ops", "platform-ops"),
        workspace_id: OTHER_WORKSPACE_ID,
      },
      { name: "Platform Ops", slug: "platform-ops" },
    ];

    for (const detail of details) {
      const fixture = startApiFixture((_request, url) => {
        if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) return workspaceEnvelope(detail);
        return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
      });

      try {
        const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
        await assert.rejects(port.resolveWorkspace(WORKSPACE_ID));
        assert.equal(fixture.requests.length, 1);
      } finally {
        fixture.stop();
      }
    }
  });

  it("rejects a workspace list row whose identity fields conflict", async () => {
    const row = { ...workspaceRecord(WORKSPACE_ID, "Platform Ops", "platform-ops"), id: OTHER_WORKSPACE_ID };
    const fixture = startApiFixture((_request, url) => url.pathname === "/fleet/api/v1/workspaces"
      ? pageEnvelope([row], 1, 1)
      : workspaceEnvelope(workspaceDetailRecord(WORKSPACE_ID, "Platform Ops", "platform-ops")));
    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace("Platform Ops"), /invalid.*schema/i);
    } finally {
      fixture.stop();
    }
  });

  it("rejects a selector matching one workspace name and another workspace slug", async () => {
    const first = workspaceRecord(WORKSPACE_ID, "Shared selector", "first-workspace");
    const second = workspaceRecord(OTHER_WORKSPACE_ID, "Other workspace", "Shared selector");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") return pageEnvelope([first, second], 1, 2);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace("Shared selector"), (error: unknown) => {
        assert.match(String(error), /multiple|duplicate|ambiguous/i);
        assert.match(String(error), new RegExp(WORKSPACE_ID));
        assert.match(String(error), new RegExp(OTHER_WORKSPACE_ID));
        return true;
      });
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), ["/fleet/api/v1/workspaces"]);
    } finally {
      fixture.stop();
    }
  });

  it("rejects an unknown workspace selector after reading the authorized list", async () => {
    const workspace = workspaceRecord(WORKSPACE_ID, "Platform Ops", "platform-ops");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") return pageEnvelope([workspace], 1, 1);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace("Unknown workspace"));
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), ["/fleet/api/v1/workspaces"]);
    } finally {
      fixture.stop();
    }
  });

  it("resolves an exact workspace name only after reading every page", async () => {
    const workspaces = Array.from({ length: 101 }, (_, index) =>
      workspaceRecord(
        `00000000-0000-4000-8000-${String(index + 10).padStart(12, "0")}`,
        `Workspace ${index}`,
        `workspace-${index}`,
      ));
    workspaces[100] = workspaceRecord(OTHER_WORKSPACE_ID, "Platform Operations", "platform-ops");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") {
        const page = Number(url.searchParams.get("page"));
        return pageEnvelope(workspaces.slice((page - 1) * 100, page * 100), page, workspaces.length);
      }
      if (url.pathname === `/fleet/api/v1/workspaces/${OTHER_WORKSPACE_ID}`) {
        return workspaceEnvelope(workspaces[100]!);
      }
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await port.resolveWorkspace("Platform Operations"), {
        workspaceId: OTHER_WORKSPACE_ID,
        name: "Platform Operations",
        slug: "platform-ops",
      });
      const listRequests = fixture.requests.filter(({ url }) => url.pathname === "/fleet/api/v1/workspaces");
      assert.deepEqual(listRequests.map(({ url }) => url.searchParams.get("page")), ["1", "2"]);
      assert.ok(listRequests.every(({ url }) => url.searchParams.get("page_size") === "100"));
      assert.equal(fixture.requests.at(-1)?.url.pathname, `/fleet/api/v1/workspaces/${OTHER_WORKSPACE_ID}`);
    } finally {
      fixture.stop();
    }
  });

  it("reports duplicate exact names across pages with all candidate IDs", async () => {
    const workspaces = Array.from({ length: 101 }, (_, index) =>
      workspaceRecord(
        `00000000-0000-4000-8000-${String(index + 20).padStart(12, "0")}`,
        `Workspace ${index}`,
        `workspace-${index}`,
      ));
    const firstId = "00000000-0000-4000-8000-000000000120";
    const secondId = "00000000-0000-4000-8000-000000000121";
    workspaces[99] = workspaceRecord(firstId, "Shared Name", "shared-name-1");
    workspaces[100] = workspaceRecord(secondId, "Shared Name", "shared-name-2");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname !== "/fleet/api/v1/workspaces") {
        return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
      }
      const page = Number(url.searchParams.get("page"));
      return pageEnvelope(workspaces.slice((page - 1) * 100, page * 100), page, workspaces.length);
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace("Shared Name"), (error: unknown) => {
        assert.match(String(error), /multiple|duplicate|ambiguous/i);
        assert.match(String(error), new RegExp(firstId));
        assert.match(String(error), new RegExp(secondId));
        return true;
      });
      assert.deepEqual(
        fixture.requests.map(({ url }) => url.searchParams.get("page")),
        ["1", "2"],
      );
      assert.ok(fixture.requests.every(({ method }) => method === "GET"));
    } finally {
      fixture.stop();
    }
  });

  it("accepts only same-origin issue and project deep links with a workspace slug", async () => {
    const workspace = workspaceRecord(WORKSPACE_ID, "Platform Ops", "platform-ops");
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") return pageEnvelope([workspace], 1, 1);
      if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) return workspaceEnvelope(workspace);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      for (const path of [
        "/fleet/platform-ops/issues/LOOP-12",
        "/fleet/platform-ops/projects/00000000-0000-4000-8000-000000000300",
      ]) {
        assert.deepEqual(await port.resolveWorkspace(`${fixture.apiUrl}${path}`), {
          workspaceId: WORKSPACE_ID,
          name: "Platform Ops",
          slug: "platform-ops",
        });
      }
    } finally {
      fixture.stop();
    }
  });

  it("rejects external and workspace-less URLs without fetching their addresses", async () => {
    const target = startApiFixture(() => Response.json({ data: [] }));
    const other = startApiFixture(() => Response.json({ data: [] }));

    try {
      const port = createLoopReadPort({ apiUrl: target.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace(`${other.apiUrl}/fleet/elsewhere/issues/LOOP-1`));
      await assert.rejects(port.resolveWorkspace(`${target.apiUrl}/loop`));
      await assert.rejects(port.resolveWorkspace(`${target.apiUrl}/unknown/path`));
      assert.equal(target.requests.length, 0);
      assert.equal(other.requests.length, 0);
    } finally {
      target.stop();
      other.stop();
    }
  });

  it("paginates tasks at 100 in created_at ascending order and deduplicates task IDs", async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) =>
      taskRecord(`00000000-0000-4000-8000-${String(index + 1_000).padStart(12, "0")}`, {
        title: `Task ${index}`,
      }));
    const repeatedTask = firstPage[99]!;
    const finalTask = taskRecord("00000000-0000-4000-8000-000000001100", {
      title: "Task 100",
      assignee_type: "member",
      assignee_id: FLEET_MEMBER_UUID,
      assignee_octo_uid: MEMBER_OCTO_UID,
    });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname !== "/fleet/api/v1/tasks") {
        return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
      }
      const page = Number(url.searchParams.get("page"));
      if (page === 1) return pageEnvelope(firstPage, page, 101);
      return pageEnvelope([repeatedTask, finalTask], page, 101);
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      const tasks = await port.listTasks(WORKSPACE_ID);
      assert.equal(tasks.length, 101);
      assert.equal(tasks[0]?.taskId, "00000000-0000-4000-8000-000000001000");
      assert.equal(tasks.at(-1)?.taskId, finalTask.task_id);
      assert.equal(tasks[99]?.title, "Task 99");
      assert.equal(tasks[100]?.title, "Task 100");
      assert.notEqual(FLEET_MEMBER_UUID, MEMBER_OCTO_UID);
      assert.equal(tasks[0]?.assigneeOctoUid, null);
      assert.equal(tasks[100]?.assigneeId, FLEET_MEMBER_UUID);
      assert.equal(tasks[100]?.assigneeOctoUid, MEMBER_OCTO_UID);
      assert.deepEqual(fixture.requests.map(({ url }) => url.searchParams.get("page")), ["1", "2"]);
      for (const request of fixture.requests) {
        assert.equal(request.method, "GET");
        assert.equal(request.authorization, `Bearer ${TOKEN}`);
        assert.equal(request.workspaceId, WORKSPACE_ID);
        assert.equal(request.accept, "application/vnd.octo+json");
        assert.equal(request.url.searchParams.get("page_size"), "100");
        assert.equal(request.url.searchParams.get("sort"), "created_at");
        assert.equal(request.url.searchParams.get("direction"), "asc");
      }
    } finally {
      fixture.stop();
    }
  });

  it("rejects malformed task list envelopes instead of returning an empty baseline", async () => {
    const fixture = startApiFixture(() => Response.json({ data: [] }));

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.listTasks(WORKSPACE_ID), /invalid|schema|pagination/i);
      assert.equal(fixture.requests.length, 1);
    } finally {
      fixture.stop();
    }
  });

  it("supplements omitted list fields from task detail instead of treating them as null", async () => {
    const listRecord = taskRecord(TASK_ID);
    delete listRecord.description;
    delete listRecord.assignee_type;
    const completeRecord = taskRecord(TASK_ID, {
      description: "Recovered from detail",
      assignee_type: null,
      assignee_id: null,
    });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/tasks") return pageEnvelope([listRecord], 1, 1);
      if (url.pathname === `/fleet/api/v1/tasks/${TASK_ID}`) return workspaceEnvelope(completeRecord);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      const [task] = await port.listTasks(WORKSPACE_ID);
      assert.equal(task?.description, "Recovered from detail");
      assert.equal(task?.assigneeType, null);
      assert.equal(task?.assigneeId, null);
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), [
        "/fleet/api/v1/tasks",
        `/fleet/api/v1/tasks/${TASK_ID}`,
      ]);
    } finally {
      fixture.stop();
    }
  });

  it("fetches task detail when the list omits assignee_octo_uid", async () => {
    const listRecord = taskRecord(TASK_ID);
    delete listRecord.assignee_octo_uid;
    const detailRecord = taskRecord(TASK_ID, {
      assignee_type: "member",
      assignee_id: FLEET_MEMBER_UUID,
      assignee_octo_uid: MEMBER_OCTO_UID,
    });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/tasks") return pageEnvelope([listRecord], 1, 1);
      if (url.pathname === `/fleet/api/v1/tasks/${TASK_ID}`) return workspaceEnvelope(detailRecord);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      const [task] = await port.listTasks(WORKSPACE_ID);
      assert.equal(task?.assigneeType, "member");
      assert.equal(task?.assigneeId, FLEET_MEMBER_UUID);
      assert.equal(task?.assigneeOctoUid, MEMBER_OCTO_UID);
      assert.deepEqual(fixture.requests.map(({ url }) => url.pathname), [
        "/fleet/api/v1/tasks",
        `/fleet/api/v1/tasks/${TASK_ID}`,
      ]);
    } finally {
      fixture.stop();
    }
  });

  it("rejects a task when assignee_octo_uid is not a nullable string", async () => {
    const invalidTask = taskRecord(TASK_ID, { assignee_octo_uid: 42 });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/fleet/api/v1/tasks/${TASK_ID}`) return workspaceEnvelope(invalidTask);
      return Response.json({ error: { code: "NOT_FOUND", message: "not found" } }, { status: 404 });
    });

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.getTask(WORKSPACE_ID, TASK_ID), /invalid.*schema/i);
      assert.equal(fixture.requests.length, 1);
    } finally {
      fixture.stop();
    }
  });

  it("fails explicitly on workspace permission errors without echoing secrets or task text", async () => {
    const privateTaskText = "private incident recovery details";
    const fixture = startApiFixture(() =>
      Response.json({
        error: {
          code: "FORBIDDEN",
          message: `workspace access denied; token=${TOKEN}; task=${privateTaskText}`,
        },
      }, { status: 403 }));

    try {
      const port = createLoopReadPort({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace(WORKSPACE_ID), (error: unknown) => {
        assert.match(String(error), /403|FORBIDDEN/i);
        assert.equal(String(error).includes(TOKEN), false);
        assert.equal(String(error).includes(privateTaskText), false);
        return true;
      });
      assert.equal(fixture.requests.length, 1);
      assert.equal(fixture.requests[0]?.method, "GET");
      assert.equal(fixture.requests[0]?.url.toString().includes(TOKEN), false);
    } finally {
      fixture.stop();
    }
  });

  it("honors caller cancellation and rejects response bodies above the HTTP bound", async () => {
    const { promise: received, resolve: receivedRequest } = Promise.withResolvers<void>();
    const cancellationFixture = startApiFixture(() => {
      receivedRequest();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{"data":['));
        },
      }), { headers: { "content-type": "application/json" } });
    });

    try {
      const port = createLoopReadPort({ apiUrl: cancellationFixture.apiUrl, botToken: TOKEN });
      const controller = new AbortController();
      const pending = port.listTasks(WORKSPACE_ID, controller.signal);
      await received;
      controller.abort();
      await assert.rejects(pending, /abort/i);
    } finally {
      cancellationFixture.stop();
    }

    const oversizedFixture = startApiFixture(() =>
      new Response(new Uint8Array(2 * 1024 * 1024 + 1), { headers: { "content-type": "application/json" } }));
    try {
      const port = createLoopReadPort({ apiUrl: oversizedFixture.apiUrl, botToken: TOKEN });
      await assert.rejects(port.resolveWorkspace(WORKSPACE_ID), /exceeds 2097152 bytes/i);
    } finally {
      oversizedFixture.stop();
    }
  });
});
