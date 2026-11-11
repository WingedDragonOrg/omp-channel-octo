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

interface StatusHandlerOptions {
  token: string;
  statusSnapshot: () => ServiceSnapshot;
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot;
}

type StatusHandler = (request: Request) => Response;
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
): Promise<StatusHandler> {
  const createStatusHandler = await loadCreateStatusHandler();
  return createStatusHandler({
    token: TOKEN,
    statusSnapshot: () => SERVICE_SNAPSHOT,
    chatStatusSnapshot,
  });
}

function makeRequest(
  path: string,
  options: { method?: string; authorization?: string | undefined } = {},
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
  const response = handler(makeRequest("/v1/status", { authorization: `Bearer ${TOKEN}` }));

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
  const response = handler(makeRequest(
    "/v1/chats/status?channelType=5&channelId=group-42____thread-9",
    { authorization: `Bearer ${TOKEN}` },
  ));

  expect(response.status).toBe(200);
  expectResponseHeaders(response);
  expect(await response.json()).toEqual(CHAT_SNAPSHOT);
});

for (const [description, authorization] of [
  ["missing Authorization", undefined],
  ["incorrect Bearer token", `Bearer ${TOKEN}-wrong`],
  ["non-Bearer authorization scheme", `Basic ${TOKEN}`],
] as const) {
  test(`GET /v1/status returns 401 for ${description}`, async () => {
    const handler = await makeHandler();
    const response = handler(makeRequest("/v1/status", { authorization }));

    await expectErrorResponse(response, 401);
  });
}

test("an unauthenticated non-GET request is rejected before route processing", async () => {
  const handler = await makeHandler();
  const response = handler(makeRequest("/v1/status", { method: "POST" }));
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
    const response = handler(makeRequest(
      `/v1/chats/status?${query}`,
      { authorization: `Bearer ${TOKEN}` },
    ));

    const body = await expectErrorResponse(response, 400);
    expect(body).toEqual({ error: { code: "invalid_request" } });
  });
}

test("unknown paths return 404 without leaking snapshot data", async () => {
  const handler = await makeHandler();
  const response = handler(makeRequest("/v1/unknown", { authorization: `Bearer ${TOKEN}` }));

  await expectErrorResponse(response, 404);
});

for (const method of ["POST", "OPTIONS"] as const) {
  test(`${method} is rejected with 405`, async () => {
    const handler = await makeHandler();
    const response = handler(makeRequest("/v1/status", {
      method,
      authorization: `Bearer ${TOKEN}`,
    }));

    await expectErrorResponse(response, 405);
  });
}
