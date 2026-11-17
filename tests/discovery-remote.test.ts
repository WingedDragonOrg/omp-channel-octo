import assert from "node:assert/strict";
import { describe, it } from "bun:test";
import { createDiscoveryRemote } from "../src/discovery/remote.ts";
import { OctoError } from "../src/octo/error.ts";

const TOKEN = "discovery-remote-fixture-token";
const GROUP_A = "group-alpha";
const GROUP_B = "group-beta";
const THREAD_SHORT = "900001";
const THREAD_CHANNEL = `${GROUP_A}____${THREAD_SHORT}`;
const WORKSPACE_ID = "00000000-0000-4000-8000-000000000001";
const OTHER_WORKSPACE_ID = "00000000-0000-4000-8000-000000000002";
const TASK_ID = "00000000-0000-4000-8000-000000000101";
const CURSOR = "opaque-cursor-1";

interface CapturedRequest {
  method: string;
  url: URL;
  authorization: string | null;
  accept: string | null;
  workspaceId: string | null;
  contentType: string | null;
  body: string;
}

interface ApiFixture {
  apiUrl: string;
  requests: CapturedRequest[];
  stop(): void;
}

function startApiFixture(
  handler: (request: Request, url: URL, body: string) => Response | Promise<Response>,
): ApiFixture {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = request.method === "GET" ? "" : await request.text();
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        url,
        authorization: request.headers.get("authorization"),
        accept: request.headers.get("accept"),
        workspaceId: request.headers.get("x-workspace-id"),
        contentType: request.headers.get("content-type"),
        body,
      });
      return handler(request, url, body);
    },
  });
  return {
    apiUrl: server.url.origin,
    requests,
    stop: () => server.stop(true),
  };
}

function threadRecord(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    short_id: THREAD_SHORT,
    group_no: GROUP_A,
    group_name: "Alpha group",
    channel_id: THREAD_CHANNEL,
    channel_type: 5,
    name: "Design discussion",
    creator_uid: "owner-1",
    source_message_id: 7001,
    status: 1,
    member_count: 4,
    message_count: 12,
    last_message_content: "latest design note",
    last_message_sender_name: "Owner",
    last_message_at: "2026-11-17T10:00:00Z",
    has_thread_md: true,
    thread_md_version: 2,
    thread_md_updated_at: "2026-11-17T09:00:00Z",
    mute: null,
    created_at: "2026-11-01T00:00:00Z",
    updated_at: "2026-11-17T10:00:00Z",
    ...overrides,
  };
}

function messageHit(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    message_id: "900000000000000001",
    message_seq: 42,
    message_kind: "text",
    snippet: "design review notes",
    sender_id: "owner-1",
    sender_name: "Owner",
    sent_at: "2026-11-16T08:00:00Z",
    channel_id: GROUP_A,
    channel_type: 2,
    ...overrides,
  };
}

function groupBucket(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    channel_id: THREAD_CHANNEL,
    channel_type: 5,
    parent_group_no: GROUP_A,
    group_name: "Alpha group",
    thread_id: THREAD_SHORT,
    thread_name: "Design discussion",
    match_count: 2,
    match_count_approx: true,
    latest_at: "2026-11-16T08:00:00Z",
    preview: [messageHit({ channel_id: THREAD_CHANNEL, channel_type: 5 })],
    ...overrides,
  };
}

function groupSearchEnvelope(): Record<string, unknown> {
  return {
    data: {
      sequence: 7,
      query_id: "query-fixture-1",
      total_groups: 1,
      total_groups_approx: true,
      groups: [groupBucket()],
    },
    pagination: { has_more: false, next_cursor: "" },
  };
}

function workspaceRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    workspace_id: WORKSPACE_ID,
    name: "Design workspace",
    slug: "design",
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: "DES",
    avatar_url: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
    ...overrides,
  };
}

function workspaceDetail(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: WORKSPACE_ID,
    name: "Design workspace",
    slug: "design",
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: "DES",
    avatar_url: null,
    status: "active",
    external_version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
    ...overrides,
  };
}

function taskRecord(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    task_id: TASK_ID,
    workspace_id: WORKSPACE_ID,
    identifier: "DES-1",
    title: "Cross-chat association discovery",
    description: "Track the remote discovery surface.",
    status: "in_progress",
    priority: "high",
    assignee_type: "expert",
    assignee_id: "00000000-0000-4000-8000-000000000201",
    assignee_octo_uid: null,
    project_id: null,
    parent_task_id: null,
    start_date: null,
    due_date: null,
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-02T00:00:00Z",
    version: 1,
    ...overrides,
  };
}

function requestJson(request: CapturedRequest): Record<string, unknown> {
  assert.notEqual(request.body, "");
  const value: unknown = JSON.parse(request.body);
  assert(value !== null && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}

function errorWith(status: number, code: string, message: string): Response {
  return Response.json({ error: { code, message } }, { status });
}

describe("remote discovery read port", () => {
  it("reads the complete bot group list from the unpaged bot endpoint", async () => {
    const groups = [
      { group_no: GROUP_A, name: "Alpha group", space_id: "space-1" },
      { group_no: GROUP_B, name: "Beta group" },
    ];
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/v1/bot/groups") return Response.json(groups);
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.listGroups(), groups);
      assert.equal(fixture.requests.length, 1);
      const [request] = fixture.requests;
      assert.equal(request?.method, "GET");
      assert.equal(request?.url.pathname, "/v1/bot/groups");
      assert.deepEqual([...request!.url.searchParams.keys()], []);
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);
    } finally {
      fixture.stop();
    }
  });

  it("requests one paged thread page and rejects threads with a foreign or inconsistent identity", async () => {
    const page = { count: 150, list: [threadRecord()] };
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/v1/bot/groups/${GROUP_A}/threads`) return Response.json(page);
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.listThreads(GROUP_A, 2), page);
      assert.equal(fixture.requests.length, 1);
      const [request] = fixture.requests;
      assert.equal(request?.url.searchParams.get("page_index"), "2");
      assert.equal(request?.url.searchParams.get("page_size"), "100");
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);

      await assert.rejects(() => remote.listThreads(GROUP_A, 0), /page/i);
      assert.equal(fixture.requests.length, 1);
    } finally {
      fixture.stop();
    }

    const foreign = startApiFixture(() =>
      Response.json({
        count: 1,
        list: [
          threadRecord({
            group_no: GROUP_B,
            channel_id: `${GROUP_B}____${THREAD_SHORT}`,
          }),
        ],
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: foreign.apiUrl, botToken: TOKEN });
      await assert.rejects(() => remote.listThreads(GROUP_A, 1), /invalid schema/);
    } finally {
      foreign.stop();
    }
  });

  it("searches matching chats through the global groups endpoint without OBO", async () => {
    const envelope = groupSearchEnvelope();
    const fixture = startApiFixture((_request, url, body) => {
      if (url.pathname !== "/v1/bot/messages/_search_global_groups") {
        return errorWith(404, "NOT_FOUND", "not found");
      }
      const parsed: unknown = JSON.parse(body);
      assert(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed));
      assert.deepEqual(parsed, { keyword: "design review" });
      return Response.json(envelope);
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.searchGroups("design review"), envelope);
      assert.equal(fixture.requests.length, 1);
      const [request] = fixture.requests;
      assert.equal(request?.method, "POST");
      assert.equal(request?.url.pathname, "/v1/bot/messages/_search_global_groups");
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);
      assert.equal(request?.contentType, "application/json");
      assert.equal(Object.hasOwn(requestJson(request!), "on_behalf_of"), false);

      await assert.rejects(() => remote.searchGroups("   "), /query/i);
      await assert.rejects(() => remote.searchGroups("x".repeat(65)), /64/);
      assert.equal(fixture.requests.length, 1);
    } finally {
      fixture.stop();
    }

    const invalidType = startApiFixture(() =>
      Response.json({
        ...groupSearchEnvelope(),
        data: {
          ...(groupSearchEnvelope().data as Record<string, unknown>),
          groups: [groupBucket({ channel_type: 3 })],
        },
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: invalidType.apiUrl, botToken: TOKEN });
      await assert.rejects(() => remote.searchGroups("design"), /invalid schema/);
    } finally {
      invalidType.stop();
    }
  });

  it("pages scoped message hits with the opaque cursor and validates channel identity", async () => {
    const first = {
      data: [messageHit()],
      pagination: { has_more: true, next_cursor: CURSOR },
    };
    const second = {
      data: [messageHit({ message_id: "900000000000000002" })],
      pagination: { has_more: false, next_cursor: "" },
    };
    const fixture = startApiFixture((_request, url, body) => {
      if (url.pathname !== "/v1/bot/messages/_search") {
        return errorWith(404, "NOT_FOUND", "not found");
      }
      const parsed: unknown = JSON.parse(body);
      assert(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed));
      const requestBody = parsed as Record<string, unknown>;
      assert.equal(Object.hasOwn(requestBody, "on_behalf_of"), false);
      assert.deepEqual(
        {
          channel_type: requestBody.channel_type,
          channel_id: requestBody.channel_id,
          keyword: requestBody.keyword,
          page_size: requestBody.page_size,
        },
        { channel_type: 2, channel_id: GROUP_A, keyword: "release plan", page_size: 100 },
      );
      return Response.json(requestBody.cursor === CURSOR ? second : first);
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(
        await remote.searchMessages("release plan", 2, GROUP_A, undefined),
        first,
      );
      assert.equal(Object.hasOwn(requestJson(fixture.requests[0]!), "cursor"), false);
      assert.deepEqual(await remote.searchMessages("release plan", 2, GROUP_A, CURSOR), second);
      assert.equal(requestJson(fixture.requests[1]!).cursor, CURSOR);
      assert.equal(fixture.requests[1]?.authorization, `Bearer ${TOKEN}`);
    } finally {
      fixture.stop();
    }

    const badPagination = startApiFixture(() =>
      Response.json({ data: [], pagination: { has_more: true, next_cursor: "" } }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: badPagination.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.searchMessages("release", 2, GROUP_A, undefined),
        /pagination/,
      );
    } finally {
      badPagination.stop();
    }

    const foreignHit = startApiFixture(() =>
      Response.json({
        data: [messageHit({ channel_id: GROUP_B })],
        pagination: { has_more: false, next_cursor: "" },
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: foreignHit.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.searchMessages("release", 2, GROUP_A, undefined),
        /invalid schema/,
      );
    } finally {
      foreignHit.stop();
    }
  });

  it("lists one fleet workspace page with its stable envelope", async () => {
    const page = {
      data: [workspaceRow()],
      pagination: { total: 3, page: 2, page_size: 100 },
    };
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === "/fleet/api/v1/workspaces") return Response.json(page);
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.listWorkspaces(2), page);
      assert.equal(fixture.requests.length, 1);
      const [request] = fixture.requests;
      assert.equal(request?.url.searchParams.get("page"), "2");
      assert.equal(request?.url.searchParams.get("page_size"), "100");
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);
      assert.equal(request?.accept, "application/vnd.octo+json");
      assert.equal(request?.workspaceId, null);
    } finally {
      fixture.stop();
    }

    const wrongPage = startApiFixture(() =>
      Response.json({
        data: [],
        pagination: { total: 0, page: 1, page_size: 100 },
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: wrongPage.apiUrl, botToken: TOKEN });
      await assert.rejects(() => remote.listWorkspaces(2), /pagination/);
    } finally {
      wrongPage.stop();
    }
  });

  it("searches tasks in one workspace with X-Workspace-ID and rejects foreign task rows", async () => {
    const page = {
      data: [taskRecord()],
      pagination: { total: 4, page: 2, page_size: 100 },
    };
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname !== "/fleet/api/v1/tasks/_search") {
        return errorWith(404, "NOT_FOUND", "not found");
      }
      return Response.json(page);
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.searchTasks(WORKSPACE_ID, "discovery design", 2), page);
      const [request] = fixture.requests;
      assert.equal(request?.method, "GET");
      assert.equal(request?.url.searchParams.get("q"), "discovery design");
      assert.equal(request?.url.searchParams.get("page"), "2");
      assert.equal(request?.url.searchParams.get("page_size"), "100");
      assert.equal(request?.workspaceId, WORKSPACE_ID);
      assert.equal(request?.accept, "application/vnd.octo+json");
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);
    } finally {
      fixture.stop();
    }

    const foreignTask = startApiFixture(() =>
      Response.json({
        data: [taskRecord({ workspace_id: OTHER_WORKSPACE_ID })],
        pagination: { total: 1, page: 1, page_size: 100 },
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: foreignTask.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.searchTasks(WORKSPACE_ID, "discovery", 1),
        /invalid schema/,
      );
    } finally {
      foreignTask.stop();
    }
  });

  it("verifies group access through the active-member thread probe, not group get", async () => {
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/v1/bot/groups/${GROUP_A}/threads`) {
        assert.equal(url.searchParams.get("page_index"), "1");
        assert.equal(url.searchParams.get("page_size"), "1");
        return Response.json({ count: 0, list: [] });
      }
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(
        await remote.verifyTarget({ channelType: 2, channelId: GROUP_A }),
        { channelType: 2, channelId: GROUP_A },
      );
      assert.equal(fixture.requests.length, 1);
      assert.equal(fixture.requests[0]?.url.pathname, `/v1/bot/groups/${GROUP_A}/threads`);
      assert.equal(fixture.requests[0]?.authorization, `Bearer ${TOKEN}`);
    } finally {
      fixture.stop();
    }

    const denied = startApiFixture(() =>
      errorWith(403, "err.server.bot_api.not_group_member", "not an active group member"),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: denied.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.verifyTarget({ channelType: 2, channelId: GROUP_A }),
        (error: unknown) => {
          assert(error instanceof OctoError);
          assert.equal(error.status, 403);
          assert.equal(error.code, "err.server.bot_api.not_group_member");
          assert.match(error.message, /HTTP 403/);
          return true;
        },
      );
    } finally {
      denied.stop();
    }
  });

  it("verifies a thread through its detail endpoint, including archived threads", async () => {
    const archived = threadRecord({ status: 2 });
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/v1/bot/groups/${GROUP_A}/threads/${THREAD_SHORT}`) {
        assert.equal(url.search, "");
        return Response.json(archived);
      }
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(
        await remote.verifyTarget({ channelType: 5, channelId: THREAD_CHANNEL }),
        { channelType: 5, channelId: THREAD_CHANNEL, thread: archived },
      );
      assert.equal(fixture.requests.length, 1);
      assert.equal(
        fixture.requests[0]?.url.pathname,
        `/v1/bot/groups/${GROUP_A}/threads/${THREAD_SHORT}`,
      );
      await assert.rejects(
        () => remote.verifyTarget({ channelType: 5, channelId: "not-a-thread" }),
        /thread/i,
      );
      assert.equal(fixture.requests.length, 1);
    } finally {
      fixture.stop();
    }

    const mismatched = startApiFixture(() =>
      Response.json(threadRecord({ channel_id: `${GROUP_A}____other` })),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: mismatched.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.verifyTarget({ channelType: 5, channelId: THREAD_CHANNEL }),
        /invalid schema/,
      );
    } finally {
      mismatched.stop();
    }
  });

  it("verifies Loop workspace readability with X-Workspace-ID and returns the raw detail", async () => {
    const detail = workspaceDetail();
    const fixture = startApiFixture((_request, url) => {
      if (url.pathname === `/fleet/api/v1/workspaces/${WORKSPACE_ID}`) {
        return Response.json(detail);
      }
      return errorWith(404, "NOT_FOUND", "not found");
    });

    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      assert.deepEqual(await remote.verifyWorkspace(WORKSPACE_ID), detail);
      const [request] = fixture.requests;
      assert.equal(request?.method, "GET");
      assert.equal(request?.url.pathname, `/fleet/api/v1/workspaces/${WORKSPACE_ID}`);
      assert.equal(request?.workspaceId, WORKSPACE_ID);
      assert.equal(request?.accept, "application/vnd.octo+json");
      assert.equal(request?.authorization, `Bearer ${TOKEN}`);
    } finally {
      fixture.stop();
    }

    const denied = startApiFixture(() =>
      errorWith(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required"),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: denied.apiUrl, botToken: TOKEN });
      await assert.rejects(
        () => remote.verifyWorkspace(WORKSPACE_ID),
        (error: unknown) => {
          assert(error instanceof OctoError);
          assert.equal(error.status, 403);
          assert.equal(error.code, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED");
          return true;
        },
      );
    } finally {
      denied.stop();
    }
  });

  it("rejects invalid selectors before any remote request", async () => {
    const fixture = startApiFixture(() => errorWith(404, "NOT_FOUND", "not found"));
    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      await assert.rejects(() => remote.listThreads(GROUP_A, 0), /page/i);
      await assert.rejects(() => remote.listWorkspaces(-1), /page/i);
      await assert.rejects(() => remote.searchTasks("workspace-x", "query", 1), /workspace/i);
      await assert.rejects(
        () => remote.searchMessages("query", 4 as never, GROUP_A, undefined),
        /channel type/i,
      );
      await assert.rejects(
        () => remote.searchMessages("query", 2, "", undefined),
        /channel/i,
      );
      await assert.rejects(() => remote.searchTasks(WORKSPACE_ID, "   ", 1), /query/i);
      await assert.rejects(
        () => remote.verifyTarget({ channelType: 1, channelId: "peer" } as never),
        /channel type/i,
      );
      await assert.rejects(() => remote.verifyWorkspace("not-a-uuid"), /UUID/i);
      assert.equal(fixture.requests.length, 0);
    } finally {
      fixture.stop();
    }
  });

  it("propagates cancellation and oversized response failures instead of returning an empty result", async () => {
    const fixture = startApiFixture(() => Response.json([{ group_no: GROUP_A, name: "Alpha" }]));
    try {
      const remote = createDiscoveryRemote({ apiUrl: fixture.apiUrl, botToken: TOKEN });
      const controller = new AbortController();
      controller.abort();
      await assert.rejects(() => remote.listGroups(controller.signal), /aborted/i);
    } finally {
      fixture.stop();
    }

    const oversized = startApiFixture(() =>
      new Response(JSON.stringify([{ group_no: GROUP_A, name: "x".repeat(2 * 1024 * 1024 + 1) }]), {
        headers: { "content-type": "application/json" },
      }),
    );
    try {
      const remote = createDiscoveryRemote({ apiUrl: oversized.apiUrl, botToken: TOKEN });
      await assert.rejects(() => remote.listGroups(), /exceeds/i);
    } finally {
      oversized.stop();
    }

    const network = startApiFixture(() => errorWith(500, "INTERNAL", "should not be reached"));
    try {
      const remote = createDiscoveryRemote({
        apiUrl: network.apiUrl,
        botToken: TOKEN,
        fetch: async () => {
          throw new TypeError("network disconnected");
        },
      });
      await assert.rejects(() => remote.listGroups(), /request failed/i);
    } finally {
      network.stop();
    }
  });

  it("validates the API origin and token at construction", () => {
    assert.throws(
      () => createDiscoveryRemote({ apiUrl: "ftp://octo.example", botToken: TOKEN }),
      /API URL is invalid/,
    );
    assert.throws(
      () => createDiscoveryRemote({ apiUrl: "https://octo.example", botToken: "" }),
      /token is required/,
    );
  });
});
