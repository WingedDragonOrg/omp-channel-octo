import { expect, test } from "bun:test";
import * as statusModule from "../src/status/http.ts";

interface ChatTarget {
  channelType: number;
  channelId: string;
}

interface ChatCommandSuggestion {
  name: string;
  source: "channel" | "omp";
  description?: string;
  hint?: string;
  aliases?: string[];
  subcommands?: { name: string; description?: string; hint?: string }[];
}

interface ChatCommandsSnapshot extends ChatTarget {
  version: 1;
  commands: ChatCommandSuggestion[];
  omp: "available" | "unavailable";
}

interface StatusHandlerOptions {
  token: string;
  statusSnapshot: () => unknown;
  chatStatusSnapshot: (target: ChatTarget) => unknown;
  chatDetailsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<unknown>;
  chatCommandsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<ChatCommandsSnapshot>;
}

type StatusHandler = (request: Request) => Promise<Response>;
type CreateStatusHandler = (options: StatusHandlerOptions) => StatusHandler;

const TOKEN = "command-directory-test-token-0123456789abcdef";
const TARGET = { channelType: 1, channelId: "owner-17" };
const SNAPSHOT: ChatCommandsSnapshot = {
  version: 1,
  ...TARGET,
  commands: [
    { name: "status", source: "channel", description: "查看当前状态" },
    {
      name: "suggestion-fixture",
      source: "omp",
      hint: "<query>",
      subcommands: [{ name: "list", description: "List matching entries", hint: "<query>" }],
    },
  ],
  omp: "available",
};

function makeRequest(authorization?: string): Request {
  const headers = new Headers({ origin: "https://octobuddy.example" });
  if (authorization !== undefined) headers.set("authorization", authorization);
  return new Request("http://channel-status.test/v1/chats/commands?channelType=1&channelId=owner-17", { headers });
}

test("GET /v1/chats/commands authenticates and resolves the exact Bot-DM identity", async () => {
  const createStatusHandler = statusModule.createStatusHandler as unknown as CreateStatusHandler;

  const targets: ChatTarget[] = [];
  const signals: AbortSignal[] = [];
  const handler = createStatusHandler({
    token: TOKEN,
    statusSnapshot: () => ({}),
    chatStatusSnapshot: target => target,
    chatDetailsSnapshot: async target => target,
    chatCommandsSnapshot: async (target, signal) => {
      targets.push(target);
      signals.push(signal);
      return { ...SNAPSHOT, ...target };
    },
  });

  const unauthorized = await handler(makeRequest());
  expect(unauthorized.status).toBe(401);
  expect(await unauthorized.json()).toEqual({ error: { code: "unauthorized" } });
  expect(targets).toEqual([]);
  expect(signals).toEqual([]);

  const ambiguousTarget = await handler(new Request(
    "http://channel-status.test/v1/chats/commands?channelType=1&channelId=owner-17&channelId=other",
    { headers: { authorization: `Bearer ${TOKEN}` } },
  ));
  expect(ambiguousTarget.status).toBe(400);
  expect(targets).toEqual([]);
  expect(signals).toEqual([]);

  const authorizedRequest = makeRequest(`Bearer ${TOKEN}`);
  const response = await handler(authorizedRequest);
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toEqual(SNAPSHOT);
  expect(targets).toEqual([TARGET]);
  expect(signals).toEqual([authorizedRequest.signal]);
});
