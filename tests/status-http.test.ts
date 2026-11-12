import { expect, test } from "bun:test";

interface ServiceSnapshot {
  version: 1;
  sampledAt: string;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
}

interface ChatTarget {
  channelType: number;
  channelId: string;
}

interface ChatStatusSnapshot extends ChatTarget {
  version: 1;
  sampledAt: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
}

interface ChatDetailsSnapshot extends ChatTarget {
  version: 1;
  sampledAt: string;
  context: {
    source: "live" | "persisted" | "unavailable";
    stale: boolean;
    usedTokens: number | null;
    contextWindow: number | null;
    percent: number | null;
    model: string | null;
    observedAt: string | null;
    note: string;
  };
  references: { pr: string[]; mr: string[] };
  tasks: { workspaceId: string; taskId: string; title: string | null; status: string | null; url: string | null }[];
}

interface StatusHandlerOptions {
  token: string;
  statusSnapshot: () => ServiceSnapshot;
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot;
  chatDetailsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<ChatDetailsSnapshot>;
  fileLinks?: { resolvers: Array<{ type: "zed"; mode: "ssh"; hostname: string; username: string; port: number } | { type: "zed"; mode: "local" }> };
}

type StatusHandler = (request: Request) => Promise<Response>;
type CreateStatusHandler = (options: StatusHandlerOptions) => StatusHandler;

const STATUS_HTTP_MODULE = "../src/status/http.ts";
const TOKEN = "status-http-test-token-0123456789abcdef";
const SERVICE_SNAPSHOT: ServiceSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T08:30:00.000Z",
  pid: 4321,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 2,
  queuedTasks: 3,
};
const CHAT_SNAPSHOT: ChatStatusSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T08:31:00.000Z",
  channelType: 5,
  channelId: "group-42____thread-9",
  state: "running",
  model: "fixture/next-worker",
  cwd: "/tmp/status-workspace",
  queuedTasks: 2,
  pendingQuestions: 1,
};

const DETAILS_SNAPSHOT: ChatDetailsSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T08:32:00.000Z",
  channelType: 5,
  channelId: "group-42____thread-9",
  context: {
    source: "live",
    stale: false,
    usedTokens: 4_096,
    contextWindow: 8_192,
    percent: 50,
    model: "fixture/context",
    observedAt: "2026-11-11T08:31:59.000Z",
    note: "fixture usage only",
  },
  references: {
    pr: ["https://github.com/example/service/pull/17"],
    mr: ["https://gitlab.example/team/service/-/merge_requests/42"],
  },
  tasks: [{
    workspaceId: "workspace-1",
    taskId: "task-1",
    title: "Fixture task",
    status: "in_progress",
    url: null,
  }],
};

async function loadCreateStatusHandler(): Promise<CreateStatusHandler> {
  let statusModule: { createStatusHandler?: unknown } | undefined;
  let importFailure: unknown;
  try {
    statusModule = await import(STATUS_HTTP_MODULE) as { createStatusHandler?: unknown };
  } catch (error) {
    importFailure = error;
  }

  // Keep a missing entrypoint as a focused test assertion rather than a module-load failure.
  expect(statusModule).toBeDefined();
  if (!statusModule) {
    const detail = importFailure instanceof Error ? importFailure.message : String(importFailure);
    throw new Error(`Expected ${STATUS_HTTP_MODULE} to export createStatusHandler; dynamic import failed: ${detail}`);
  }

  expect(typeof statusModule.createStatusHandler).toBe("function");
  return statusModule.createStatusHandler as CreateStatusHandler;
}

async function makeHandler(
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot = target => ({
    ...CHAT_SNAPSHOT,
    channelType: target.channelType,
    channelId: target.channelId,
  }),
  chatDetailsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<ChatDetailsSnapshot> =
    async target => ({
      ...DETAILS_SNAPSHOT,
      channelType: target.channelType,
      channelId: target.channelId,
    }),
  fileLinks?: StatusHandlerOptions["fileLinks"],
): Promise<StatusHandler> {
  const createStatusHandler = await loadCreateStatusHandler();
  return createStatusHandler({
    token: TOKEN,
    statusSnapshot: () => SERVICE_SNAPSHOT,
    chatStatusSnapshot,
    chatDetailsSnapshot,
    ...(fileLinks ? { fileLinks } : {}),
  });
}

function makeRequest(
  path: string,
  options: { method?: string; authorization?: string | undefined; signal?: AbortSignal } = {},
): Request {
  const headers = new Headers({ origin: "https://octobuddy.example" });
  if (options.authorization !== undefined) headers.set("authorization", options.authorization);
  if (options.method === "OPTIONS") {
    headers.set("access-control-request-method", "GET");
    headers.set("access-control-request-headers", "authorization");
  }
  return new Request(`http://channel-status.test${path}`, {
    method: options.method ?? "GET",
    headers,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
}

function expectResponseHeaders(response: Response): void {
  expect(response.headers.get("cache-control")).toBe("no-store");
  const corsHeaders = [...response.headers.keys()].filter(name => name.startsWith("access-control-"));
  expect(corsHeaders).toEqual([]);
}

async function expectErrorResponse(response: Response, status: number): Promise<unknown> {
  expect(response.status).toBe(status);
  expectResponseHeaders(response);
  const body: unknown = await response.json();
  expect(body).toEqual({ error: { code: expect.any(String) } });
  expect(JSON.stringify(body)).not.toContain(TOKEN);
  return body;
}

test("GET /v1/status returns the service snapshot as JSON", async () => {
  const handler = await makeHandler();
  const response = await handler(makeRequest("/v1/status", { authorization: `Bearer ${TOKEN}` }));

  expect(response.status).toBe(200);
  expectResponseHeaders(response);
  expect(await response.json()).toEqual(SERVICE_SNAPSHOT);
});

test("GET /v1/chats/status resolves the exact query target and returns its snapshot", async () => {
  const handler = await makeHandler(target => ({
    ...CHAT_SNAPSHOT,
    channelType: target.channelType,
    channelId: target.channelId,
  }));
  const response = await handler(makeRequest(
    "/v1/chats/status?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));

  expect(response.status).toBe(200);
  expectResponseHeaders(response);
  expect(await response.json()).toEqual({ ...CHAT_SNAPSHOT, zedWorkspaceUrl: null });
});

test("chat status supplies the configured SSH workspace URL independent of the HTTP host", async () => {
  const handler = await makeHandler(undefined, undefined, {
    resolvers: [{ type: "zed", mode: "ssh", hostname: "ssh-host", username: "dev", port: 2222 }],
  });
  const response = await handler(makeRequest(
    "/v1/chats/status?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));
  expect((await response.json() as { zedWorkspaceUrl: string }).zedWorkspaceUrl)
    .toBe("zed://ssh/dev@ssh-host:2222/tmp/status-workspace");
});

test("chat status encodes a local workspace path containing literal URL characters", async () => {
  const handler = await makeHandler(
    target => ({ ...CHAT_SNAPSHOT, ...target, cwd: "/tmp/shared #1%20" }),
    undefined,
    { resolvers: [{ type: "zed", mode: "local" }] },
  );
  const response = await handler(makeRequest(
    "/v1/chats/status?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));
  expect((await response.json() as { zedWorkspaceUrl: string }).zedWorkspaceUrl)
    .toBe("zed://file/tmp/shared%20%231%2520");
});

for (const [description, authorization] of [
  ["missing Authorization", undefined],
  ["incorrect Bearer token", `Bearer ${TOKEN}-wrong`],
  ["non-Bearer authorization scheme", `Basic ${TOKEN}`],
] as const) {
  test(`GET /v1/status returns 401 for ${description}`, async () => {
    const handler = await makeHandler();
    const response = await handler(makeRequest("/v1/status", { authorization }));

    await expectErrorResponse(response, 401);
  });
}

test("an unauthenticated non-GET request is rejected before route processing", async () => {
  const handler = await makeHandler();
  const response = await handler(makeRequest("/v1/status", { method: "POST" }));
  await expectErrorResponse(response, 401);
});

for (const [description, query] of [
  ["missing channelType", "channelId=group-42"],
  ["non-integer channelType", "channelType=not-a-number&channelId=group-42"],
  ["fractional channelType", "channelType=1.5&channelId=group-42"],
  ["unsupported channelType", "channelType=3&channelId=group-42"],
  ["negative channelType", "channelType=-1&channelId=group-42"],
  ["missing channelId", "channelType=1"],
  ["empty channelId", "channelType=1&channelId="],
] as const) {
  test(`GET /v1/chats/status returns 400 for ${description}`, async () => {
    const handler = await makeHandler();
    const response = await handler(makeRequest(
      `/v1/chats/status?${query}`,
      { authorization: `Bearer ${TOKEN}` },
    ));

    const body = await expectErrorResponse(response, 400);
    expect(body).toEqual({ error: { code: "invalid_request" } });
  });
}

test("unknown paths return 404 without leaking snapshot data", async () => {
  const handler = await makeHandler();
  const response = await handler(makeRequest("/v1/unknown", { authorization: `Bearer ${TOKEN}` }));

  await expectErrorResponse(response, 404);
});

for (const method of ["POST", "OPTIONS"] as const) {
  test(`${method} is rejected with 405`, async () => {
    const handler = await makeHandler();
    const response = await handler(makeRequest("/v1/status", {
      method,
      authorization: `Bearer ${TOKEN}`,
    }));
    await expectErrorResponse(response, 405);
  });
}

test("GET /v1/chats/details awaits an exact target and returns the restricted details snapshot", async () => {
  const target = { channelType: 5, channelId: "group-42____thread-9" };
  const calls: { target: ChatTarget; signal: AbortSignal }[] = [];
  let statusCalls = 0;
  const handler = await makeHandler(
    () => {
      statusCalls += 1;
      return CHAT_SNAPSHOT;
    },
    async (actualTarget, signal) => {
      calls.push({ target: actualTarget, signal });
      await Promise.resolve();
      return { ...DETAILS_SNAPSHOT, ...actualTarget };
    },
  );
  const request = makeRequest(
    `/v1/chats/details?channelType=${target.channelType}&channelId=${target.channelId}`,
    { authorization: `Bearer ${TOKEN}` },
  );

  const response = await handler(request);

  expect(response.status).toBe(200);
  expectResponseHeaders(response);
  expect(await response.json()).toEqual(DETAILS_SNAPSHOT);
  expect(calls).toEqual([{ target, signal: request.signal }]);
  expect(statusCalls).toBe(0);
});

test("GET /v1/chats/details rejects unauthenticated requests before resolving details", async () => {
  let called = false;
  const handler = await makeHandler(undefined, async () => {
    called = true;
    return DETAILS_SNAPSHOT;
  });

  const response = await handler(makeRequest(
    "/v1/chats/details?channelType=5&channelId=group-42____thread-9",
  ));

  await expectErrorResponse(response, 401);
  expect(called).toBe(false);
});

for (const [description, query] of [
  ["missing channelType", "channelId=group-42"],
  ["unsupported channelType", "channelType=3&channelId=group-42"],
  ["duplicate channel identity", "channelType=5&channelType=5&channelId=group-42"],
  ["unexpected query parameter", "channelType=5&channelId=group-42&botUid=other"],
] as const) {
  test(`GET /v1/chats/details returns 400 for ${description}`, async () => {
    let called = false;
    const handler = await makeHandler(undefined, async () => {
      called = true;
      return DETAILS_SNAPSHOT;
    });

    const response = await handler(makeRequest(
      `/v1/chats/details?${query}`,
      { authorization: `Bearer ${TOKEN}` },
    ));

    const body = await expectErrorResponse(response, 400);
    expect(body).toEqual({ error: { code: "invalid_request" } });
    expect(called).toBe(false);
  });
}

test("GET /v1/chats/details rejects a mismatched snapshot identity", async () => {
  const handler = await makeHandler(undefined, async () => ({
    ...DETAILS_SNAPSHOT,
    channelId: "another-thread",
  }));

  const response = await handler(makeRequest(
    "/v1/chats/details?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));

  const body = await expectErrorResponse(response, 500);
  expect(body).toEqual({ error: { code: "internal_error" } });
});

test("GET /v1/chats/details hides asynchronous callback errors", async () => {
  const handler = await makeHandler(undefined, async () => {
    throw new Error("private details failure");
  });

  const response = await handler(makeRequest(
    "/v1/chats/details?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));

  const body = await expectErrorResponse(response, 500);
  expect(body).toEqual({ error: { code: "internal_error" } });
  expect(JSON.stringify(body)).not.toContain("private details failure");
});

test("GET /v1/chats/details forwards request cancellation", async () => {
  const requestController = new AbortController();
  const callbackStarted = Promise.withResolvers<void>();
  const releaseCallback = Promise.withResolvers<void>();
  let callbackSignal: AbortSignal | undefined;
  let callbackObservedAbort = false;
  const handler = await makeHandler(undefined, async (_target, signal) => {
    callbackSignal = signal;
    signal.addEventListener("abort", () => {
      callbackObservedAbort = true;
    }, { once: true });
    callbackStarted.resolve();
    expect(callbackSignal).toBe(request.signal);
    await releaseCallback.promise;
    signal.throwIfAborted();
    throw new Error("private details failure");
  });
  const request = makeRequest(
    "/v1/chats/details?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}`, signal: requestController.signal },
  );
  const responsePromise = handler(request);
  const routeOutcome = await Promise.race([
    callbackStarted.promise.then(() => null),
    responsePromise,
  ]);
  expect(routeOutcome).toBeNull();
  requestController.abort(new DOMException("request cancelled", "AbortError"));
  releaseCallback.resolve();

  const response = await responsePromise;

  const body = await expectErrorResponse(response, 500);
  expect(body).toEqual({ error: { code: "internal_error" } });
  expect(callbackObservedAbort).toBe(true);
});
