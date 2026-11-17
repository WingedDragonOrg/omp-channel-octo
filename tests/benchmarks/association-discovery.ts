import assert from "node:assert/strict";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import { openChannelStore } from "../../src/store/index.ts";
import contextExtension from "../../src/omp/context-extension.ts";
import type { LoopTask, LoopWorkspace } from "../../src/loop/types.ts";
import type { ThreadReference } from "../../src/references/types.ts";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// 功能验收基准：真实 channel CLI 子进程 + 本地加密 Octo 协议夹具 + 本地 Octo/Fleet HTTP 网关
// + 本地 HTTPS Decisions 夹具 + 本地 RPC worker。发现逻辑本身走生产 octo_discover host tool，
// 不替换、不 mock 发现实现；只有远端服务和模型协议是本地夹具。所有账号、消息、数据库、
// 证书与进程都属于临时目录，运行结束即删除，绝不解碰常驻 bot 或真实群/Loop。
//
// 本文件同时是 RPC worker 入口：CLI 以 --worker 启动它，它按生产 RPC 协议发起
// host_tool_call，并按工具返回的查询 ID/页码回放候选页与模型过滤页。
// 群/线程 prompt 在本地 worker 中调用同一生产 context-extension hook，由真实 handler 写 prepared receipt。
// Fleet 后台 task scan 同样按 Loop 客户端的 GET /tasks 分页协议应答；被拒工作区仍返回真实 403。

const checkout = resolve(import.meta.dir, "../..");
const BENCHMARK_PATH = import.meta.path;
const DISCOVERY_KEY = "synthetic-discovery-key";
const KEY_ENV = "OMP_CHANNEL_OCTO_DISCOVERY_KEY";
const OWNER_SAFE_TEXT = "DISCOVERY_DONE";
const MAX_WORKER_TIMEOUT_MS = 60_000;

type Json = Record<string, unknown>;

function isJson(value: unknown): value is Json {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function jsonObject(value: unknown, label: string): Json {
  if (!isJson(value)) throw new Error(`${label} is not a JSON object`);
  return value;
}

function stringField(record: Json | undefined, key: string): string | undefined {
  const value = record?.[key];
  return typeof value === "string" ? value : undefined;
}

function numberField(record: Json | undefined, key: string): number | undefined {
  const value = record?.[key];
  return typeof value === "number" ? value : undefined;
}

function textResult(value: unknown): string | null {
  const result = isJson(value) ? value.result : undefined;
  const content = isJson(result) ? result.content : undefined;
  if (!Array.isArray(content)) return null;
  const entry = content.find((item) => isJson(item) && item.type === "text");
  return isJson(entry) && typeof entry.text === "string" ? entry.text : null;
}

function candidateIds(result: Json): string[] {
  if (!Array.isArray(result.candidates)) throw new Error("octo_discover result has no candidates array");
  return result.candidates.flatMap((entry) => {
    const candidate = jsonObject(entry, "discovery candidate");
    const channelId = candidate.channel_id ?? candidate.channelId;
    return typeof channelId === "string" ? [channelId] : [];
  });
}

function candidatesOf(result: Json): Json[] {
  if (!Array.isArray(result.candidates)) throw new Error("octo_discover result has no candidates array");
  return result.candidates.map((entry) => jsonObject(entry, "discovery candidate"));
}

function evidenceKinds(candidate: Json): string[] {
  if (!Array.isArray(candidate.evidence)) return [];
  return candidate.evidence.flatMap((entry) => {
    if (typeof entry === "string") return [entry];
    const record = isJson(entry) ? entry : {};
    const kind = record.kind ?? record.source ?? record.type;
    return typeof kind === "string" ? [kind] : [];
  });
}

function evidenceEntries(candidate: Json, kind: string): Json[] {
  if (!Array.isArray(candidate.evidence)) return [];
  return candidate.evidence.filter((entry): entry is Json =>
    isJson(entry) && (entry.kind === kind || entry.source === kind || entry.type === kind));
}

function readEntry(candidate: Json): Json {
  const read = candidate.read;
  if (isJson(read)) return read;
  throw new Error(`discovery candidate has no read entry: ${JSON.stringify(candidate)}`);
}

function assertNoTokens(value: unknown, tokens: readonly string[], label: string): void {
  const text = JSON.stringify(value);
  const leaked = tokens.filter((token) => token.length > 0 && text.includes(token));
  assert.deepEqual(leaked, [], `${label} leaked forbidden identity: ${leaked.join(", ")}`);
}

function modelSucceeded(status: string | undefined): boolean {
  return status !== undefined && /^(?:complete|completed|ok|success)$/iu.test(status);
}

function parseResult(text: string): Json {
  return jsonObject(JSON.parse(text) as unknown, "octo_discover result");
}

function auditEntries(path: string): Json[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split("\n").filter(Boolean).map((line) =>
    jsonObject(JSON.parse(line) as unknown, "benchmark audit row"));
}

function auditText(entries: Json[], prompt: string, kind: string): string {
  const entry = [...entries].reverse().find((candidate) => candidate.prompt === prompt && candidate.kind === kind);
  if (entry === undefined || typeof entry.text !== "string") throw new Error(`${prompt} has no ${kind} result`);
  return entry.text;
}

// ─── Worker 模式 ────────────────────────────────────────────────────────────

interface WorkerPlan {
  search: Json;
  gateFirstCandidatePage?: boolean;
}

function workerFlag(name: string): string | undefined {
  const index = process.argv.lastIndexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}

async function runWorker(): Promise<void> {
  const auditPath = workerFlag("--audit");
  const plansPath = workerFlag("--plans");
  const gateDir = workerFlag("--gate");
  if (!auditPath || !plansPath || !gateDir) throw new Error("worker flags are required");
  const resumedSession = workerFlag("--resume");
  const sessionFile = resolve(resumedSession ?? join(
    workerFlag("--session-dir") ?? tmpdir(),
    `association-discovery-${process.pid}.jsonl`,
  ));
  let sessionId = `association-discovery-${process.pid}`;
  if (resumedSession !== undefined) {
    const firstLine = readFileSync(sessionFile, "utf8").split(/\r?\n/u).find(Boolean);
    if (firstLine === undefined) throw new Error("resumed benchmark session has no header");
    const header = jsonObject(JSON.parse(firstLine) as unknown, "resumed benchmark session header");
    if (header.type !== "session" || typeof header.id !== "string") {
      throw new Error("resumed benchmark session has an invalid header");
    }
    sessionId = header.id;
  }
  const plans = JSON.parse(readFileSync(plansPath, "utf8")) as Record<string, WorkerPlan>;
  const extensionHandlers = new Map<string, unknown>();
  contextExtension({
    on(event, handler) {
      extensionHandlers.set(event, handler);
    },
  });
  const audit = (entry: Json): void => {
    appendFileSync(auditPath, `${JSON.stringify({ pid: process.pid, ...entry })}\n`, "utf8");
  };
  const emit = (frame: Json): void => {
    process.stdout.write(`${JSON.stringify(frame)}\n`);
  };
  const reply = (command: Json, data?: unknown): void => {
    emit({ type: "response", id: command.id, command: command.type, success: true, data });
  };
  let tools = new Map<string, Json>();
  let pending: { id: string; resolve: (frame: Json) => void } | null = null;
  let promptText = "";
  let finished = false;
  let callSequence = 0;

  function assistant(text: string): void {
    const message = { id: `discovery-benchmark-${callSequence}`, role: "assistant", content: [{ type: "text", text }] };
    emit({ type: "message_start", message: { ...message, content: [] } });
    emit({
      type: "message_update",
      message,
      assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text, partial: message },
    });
    emit({ type: "message_end", message });
    emit({ type: "agent_end", messages: [message], willContinue: false });
  }

  function finish(text: string): void {
    if (finished) return;
    finished = true;
    assistant(text);
    audit({ kind: "done", prompt: promptText, text });
  }

  function callTool(arguments_: Json): Promise<{ text: string; toolCallId: string }> {
    const requestId = `discovery-host-${++callSequence}`;
    const toolCallId = `discovery-call-${callSequence}`;
    const {
      promise,
      resolve: resolveCall,
      reject: rejectCall,
    } = Promise.withResolvers<{ text: string; toolCallId: string }>();
    pending = {
      id: requestId,
      resolve: (frame) => {
        pending = null;
        const text = textResult(frame);
        if (frame.isError === true) {
          rejectCall(new Error(text ?? "host tool reported an error"));
          return;
        }
        if (text === null) {
          rejectCall(new Error("host tool result carried no text"));
          return;
        }
        resolveCall({ text, toolCallId });
      },
    };
    audit({ kind: "tool_call", prompt: promptText, arguments: arguments_ });
    emit({ type: "tool_execution_start", toolCallId, toolName: "octo_discover", args: arguments_ });
    emit({ type: "host_tool_call", id: requestId, toolCallId, toolName: "octo_discover", arguments: arguments_ });
    return promise;
  }

  async function waitGate(name: string): Promise<void> {
    const gatePath = join(gateDir!, `${name}.go`);
    const deadline = Date.now() + 15_000;
    while (!existsSync(gatePath)) {
      if (Date.now() > deadline) throw new Error(`benchmark gate ${name} was not opened`);
      await Bun.sleep(25);
    }
  }

  async function executePlan(plan: WorkerPlan): Promise<void> {
    if (!tools.has("octo_discover")) {
      audit({ kind: "missing_tool", prompt: promptText, tools: [...tools.keys()] });
      finish("MISSING_TOOL");
      return;
    }
    try {
      const search = await callTool(plan.search);
      audit({ kind: "search", prompt: promptText, text: search.text });
      emit({ type: "tool_execution_end", toolCallId: search.toolCallId, toolName: "octo_discover" });
      const result = parseResult(search.text);
      const queryId = stringField(result, "query_id");
      if (queryId === undefined) throw new Error("search result has no query_id");
      const page = isJson(result.page) ? result.page : {};
      let position = page.has_more === true ? numberField(page, "next_position") : undefined;
      let gated = false;
      while (position !== undefined) {
        if (plan.gateFirstCandidatePage === true && !gated) {
          gated = true;
          audit({ kind: "page_ready", prompt: promptText, position });
          await waitGate(promptText);
        }
        const requested = position;
        const next = await callTool({ action: "page", query_id: queryId, collection: "candidates", position: requested });
        audit({ kind: "page", prompt: promptText, position: requested, text: next.text });
        emit({ type: "tool_execution_end", toolCallId: next.toolCallId, toolName: "octo_discover" });
        const pageResult = parseResult(next.text);
        const nextPage = isJson(pageResult.page) ? pageResult.page : {};
        position = nextPage.has_more === true ? numberField(nextPage, "next_position") : undefined;
      }
      const filtered = isJson(result.filtered) ? result.filtered : undefined;
      if (filtered !== undefined && numberField(filtered, "count") !== undefined && (numberField(filtered, "count") ?? 0) > 0) {
        const filteredPosition = numberField(filtered, "next_position") ?? 1;
        const filteredPage = await callTool({
          action: "page",
          query_id: queryId,
          collection: "filtered",
          position: filteredPosition,
        });
        audit({ kind: "filtered", prompt: promptText, position: filteredPosition, text: filteredPage.text });
        emit({ type: "tool_execution_end", toolCallId: filteredPage.toolCallId, toolName: "octo_discover" });
      }
      finish(OWNER_SAFE_TEXT);
    } catch (error) {
      audit({ kind: "error", prompt: promptText, message: error instanceof Error ? error.message : String(error) });
      finish("DISCOVERY_FAILED");
    }
  }

  async function onPrompt(command: Json): Promise<void> {
    promptText = typeof command.message === "string" ? command.message : "";
    const plan = plans[promptText];
    finished = false;
    reply(command, { agentInvoked: true });
    audit({ kind: "prompt_tools", prompt: promptText, definitions: [...tools.values()] });
    try {
      const beforeAgentStart = extensionHandlers.get("before_agent_start") as
        | ((event: Json, context: {
          sessionManager: { getSessionId: () => string; getBranch: () => Json[] };
        }) => unknown | Promise<unknown>)
        | undefined;
      if (beforeAgentStart === undefined) {
        throw new Error("production context extension did not register before_agent_start");
      }
      await beforeAgentStart(
        { source: "rpc", sessionId },
        { sessionManager: { getSessionId: () => sessionId, getBranch: () => [] } },
      );
      audit({ kind: "context_extension", prompt: promptText, hook: "before_agent_start" });
    } catch (error) {
      audit({ kind: "context_extension_error", prompt: promptText, message: error instanceof Error ? error.message : String(error) });
      finish("CONTEXT_EXTENSION_FAILED");
      return;
    }
    emit({ type: "agent_start" });
    if (plan === undefined) {
      audit({ kind: "unsupported_prompt", prompt: promptText });
      finish("UNSUPPORTED_PROMPT");
      return;
    }
    void executePlan(plan);
  }

  function handle(command: Json): void {
    const type = stringField(command, "type") ?? "";
    if (type === "host_tool_result") {
      if (pending !== null && command.id === pending.id) pending.resolve(command);
      else audit({ kind: "unexpected_host_tool_result", id: command.id });
      return;
    }
    switch (type) {
      case "new_session":
        if (resumedSession === undefined) {
          mkdirSync(dirname(sessionFile), { recursive: true });
          writeFileSync(sessionFile, `${JSON.stringify({
            type: "session",
            version: 3,
            id: sessionId,
            timestamp: new Date().toISOString(),
            cwd: process.cwd(),
          })}\n`, { flag: "wx" });
        }
        reply(command, { cancelled: false });
        return;
      case "get_state":
        reply(command, {
          sessionId,
          sessionFile,
          cwd: process.cwd(),
          model: { provider: "fixture", id: "offline" },
          isStreaming: false,
        });
        return;
      case "set_host_tools": {
        const next = new Map<string, Json>();
        const definitions = Array.isArray(command.tools) ? command.tools : [];
        for (const definition of definitions) {
          if (isJson(definition) && typeof definition.name === "string") next.set(definition.name, definition);
        }
        tools = next;
        audit({ kind: "tools_update", definitions: [...tools.values()] });
        reply(command, { toolNames: [...tools.keys()] });
        return;
      }
      case "abort":
        reply(command);
        finish("DISCOVERY_ABORTED");
        return;
      case "set_subagent_subscription":
        reply(command, { level: command.level });
        return;
      case "prompt":
        void onPrompt(command);
        return;
      default:
        reply(command, {});
    }
  }

  let exiting = false;
  const exitSoon = (): void => {
    if (exiting) return;
    exiting = true;
    setTimeout(() => process.exit(0), 200);
  };
  process.on("SIGTERM", exitSoon);
  process.stdin.on("end", exitSoon);
  let input = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk: string) => {
    input += chunk;
    for (;;) {
      const end = input.indexOf("\n");
      if (end < 0) break;
      const line = input.slice(0, end);
      input = input.slice(end + 1);
      if (line.trim().length > 0) handle(JSON.parse(line) as Json);
    }
  });
  process.stdin.resume();
  emit({ type: "ready", protocolVersion: 1, supportedProtocolVersions: [1], maxFrameBytes: 1_048_576 });
}

// ─── 夹具常量 ───────────────────────────────────────────────────────────────

const GROUP_A = "discovery-group-a";
const GROUP_B = "discovery-group-b";
const GROUP_P = "discovery-group-pages";
const GROUP_WS = "discovery-group-workspace";
const GROUP_DENIED = "discovery-group-secret";
const THREAD_A = `${GROUP_A}____topic-a`;
const THREAD_B = `${GROUP_B}____topic-b`;
const THREAD_KEEP = `${GROUP_A}____topic-keep`;
const THREAD_FILTER = `${GROUP_A}____topic-filter`;
const THREAD_MSG = `${GROUP_A}____topic-msg`;
const THREAD_WSDENIED = `${GROUP_WS}____topic-wsdenied`;
const THREAD_DENIED = `${GROUP_DENIED}____topic-secret`;
const THREAD_OTHER_BOT = `${GROUP_A}____topic-other`;
const GROUP_A_NAME = "Discovery Alpha";
const GROUP_B_NAME = "Discovery Beta";
const GROUP_P_NAME = "Discovery Pages";
const GROUP_WS_NAME = "Discovery Workspace";
const GROUP_DENIED_NAME = "SECRET_GROUP_NAME";
const THREAD_NAMES = new Map<string, string>([
  [THREAD_A, "Service association design"],
  [THREAD_B, "Delivery progress"],
  [THREAD_KEEP, "KEEP_EXPLICIT candidate"],
  [THREAD_FILTER, "FILTER_ME candidate"],
  [THREAD_MSG, "Service association notes"],
  [THREAD_WSDENIED, "Workspace evidence candidate"],
  [THREAD_DENIED, "SECRET_TARGET_NAME"],
  [THREAD_OTHER_BOT, "Unrelated note"],
]);
const PAGE_THREADS = Array.from({ length: 25 }, (_, index) => `${GROUP_P}____topic-${String(index).padStart(2, "0")}`);
const PAGE_THREAD_NAMES = new Map(PAGE_THREADS.map((thread, index) => [thread, `Page item ${index}`]));
const WORKSPACE_A = "00000000-0000-4000-8000-0000000000a1";
const WORKSPACE_P = "00000000-0000-4000-8000-0000000000a2";
const WORKSPACE_DENIED = "00000000-0000-4000-8000-0000000000a3";
const TASK_A = "00000000-0000-4000-8000-0000000000b1";
const TASK_KEEP = "00000000-0000-4000-8000-0000000000b2";
const TASK_SECRET_TARGET = "00000000-0000-4000-8000-0000000000b3";
const TASK_DENIED_WORKSPACE = "00000000-0000-4000-8000-0000000000b4";
const TASK_OTHER_BOT = "00000000-0000-4000-8000-0000000000b5";
const OTHER_BOT_UID = "other-discovery-bot";
const PAGE_TASKS = Array.from({ length: 25 }, (_, index) =>
  `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`);
const PR_URL = "https://github.com/example/service/pull/42";
const KEEP_REF_URL = "https://github.com/example/service/pull/43";
const WSDENIED_REF_URL = "https://github.com/example/service/pull/44";
const SECRET_PR_URL = "https://github.com/private/secret/pull/7";
const FILTER_REPO = "https://github.com/example/filter-service";
const SECRET_TASK_TITLE = "SECRET_LOOP_TITLE";
const MESSAGE_ID = "900000000000000001";
const MESSAGE_ID_2 = "900000000000000002";
const GROUP_MESSAGE_ID = "900000000000000003";
const GROUP_MESSAGE_ID_2 = "900000000000000004";
const SECRET_TOKENS = [
  GROUP_DENIED,
  THREAD_DENIED,
  GROUP_DENIED_NAME,
  "SECRET_TARGET_NAME",
  SECRET_TASK_TITLE,
  WORKSPACE_DENIED,
  TASK_SECRET_TARGET,
  TASK_DENIED_WORKSPACE,
  OTHER_BOT_UID,
  SECRET_PR_URL,
];

function loopTask(taskId: string, title: string): LoopTask {
  return {
    taskId,
    title,
    description: null,
    status: "todo",
    priority: "normal",
    assigneeType: null,
    assigneeId: null,
    assigneeOctoUid: null,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-17T00:00:00.000Z",
    updatedAt: "2026-11-17T00:00:00.000Z",
  };
}

function loopWorkspace(workspaceId: string, name: string, slug: string): LoopWorkspace {
  return { workspaceId, name, slug };
}

function reference(kind: "pr" | "mr", url: string): ThreadReference {
  return { kind, url };
}

// ─── 本地 Octo/Fleet 网关 ───────────────────────────────────────────────────

interface GatewayRequest {
  method: string;
  pathname: string;
  search: string;
  authorization: string | null;
  workspaceId: string | null;
  body: string;
}

interface GatewayThread {
  shortId: string;
  groupId: string;
  channelId: string;
  name: string;
  status: number;
}

interface GatewayTask {
  taskId: string;
  workspaceId: string;
  identifier: string;
  title: string;
  description: string;
  status: string;
  priority: string;
}

interface DiscoveryGateway {
  url: string;
  requests: GatewayRequest[];
  deniedThreads: Set<string>;
  denyThread(channelId: string): void;
  close(): void;
}

function threadRecord(groupId: string, shortId: string, name: string): GatewayThread {
  return { shortId, groupId, channelId: `${groupId}____${shortId}`, name, status: 1 };
}

function taskRecord(workspaceId: string, taskId: string, identifier: string, title: string): GatewayTask {
  return {
    taskId,
    workspaceId,
    identifier,
    title,
    description: `${title} acceptance notes`,
    status: "in_progress",
    priority: "high",
  };
}

function wireThread(thread: GatewayThread, groupName: string): Json {
  return {
    short_id: thread.shortId,
    group_no: thread.groupId,
    group_name: groupName,
    channel_id: thread.channelId,
    channel_type: 5,
    name: thread.name,
    creator_uid: "owner-1",
    source_message_id: 7001,
    status: thread.status,
    member_count: 4,
    message_count: 12,
    last_message_content: "fixture latest note",
    last_message_sender_name: "Owner",
    last_message_at: "2026-11-17T10:00:00Z",
    has_thread_md: false,
    thread_md_version: 0,
    thread_md_updated_at: null,
    mute: null,
    created_at: "2026-11-01T00:00:00Z",
    updated_at: "2026-11-17T10:00:00Z",
  };
}

function wireMessage(
  messageId: string,
  channelId: string,
  snippet: string,
  channelType: 2 | 5 = 5,
): Json {
  return {
    message_id: messageId,
    message_seq: Number(messageId.slice(-4)),
    message_kind: "text",
    snippet,
    sender_id: "owner-1",
    sender_name: "Owner",
    sent_at: "2026-11-16T08:00:00Z",
    channel_id: channelId,
    channel_type: channelType,
  };
}

function wireTask(task: GatewayTask): Json {
  return {
    task_id: task.taskId,
    workspace_id: task.workspaceId,
    identifier: task.identifier,
    title: task.title,
    description: task.description,
    status: task.status,
    priority: task.priority,
    assignee_type: "member",
    assignee_id: "owner-user-uid",
    assignee_octo_uid: null,
    project_id: null,
    parent_task_id: null,
    start_date: null,
    due_date: null,
    created_at: "2026-10-01T00:00:00Z",
    updated_at: "2026-10-02T00:00:00Z",
    version: 1,
  };
}

function workspaceRow(workspaceId: string, name: string, slug: string): Json {
  return {
    workspace_id: workspaceId,
    name,
    slug,
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: slug.toUpperCase().slice(0, 3),
    avatar_url: null,
  };
}

function workspaceDetail(workspaceId: string, name: string, slug: string): Json {
  return {
    id: workspaceId,
    name,
    slug,
    description: null,
    context: null,
    settings: {},
    repos: [],
    issue_prefix: slug.toUpperCase().slice(0, 3),
    avatar_url: null,
    status: "active",
    external_version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
  };
}

function errorResponse(status: number, code: string, message: string): Response {
  return Response.json({ error: { code, message }, msg: message, status }, { status });
}

function startDiscoveryGateway(fixture: OctoProtocolFixture): DiscoveryGateway {
  const requests: GatewayRequest[] = [];
  const deniedThreads = new Set<string>();
  const groups = new Map<string, { name: string; threads: GatewayThread[] }>([
    [GROUP_A, { name: GROUP_A_NAME, threads: [
      threadRecord(GROUP_A, "topic-a", THREAD_NAMES.get(THREAD_A)!),
      threadRecord(GROUP_A, "topic-keep", THREAD_NAMES.get(THREAD_KEEP)!),
      threadRecord(GROUP_A, "topic-filter", THREAD_NAMES.get(THREAD_FILTER)!),
      threadRecord(GROUP_A, "topic-msg", THREAD_NAMES.get(THREAD_MSG)!),
      threadRecord(GROUP_A, "topic-other", THREAD_NAMES.get(THREAD_OTHER_BOT)!),
    ] }],
    [GROUP_B, { name: GROUP_B_NAME, threads: [
      threadRecord(GROUP_B, "topic-b", THREAD_NAMES.get(THREAD_B)!),
    ] }],
    [GROUP_WS, { name: GROUP_WS_NAME, threads: [
      threadRecord(GROUP_WS, "topic-wsdenied", THREAD_NAMES.get(THREAD_WSDENIED)!),
    ] }],
    [GROUP_P, { name: GROUP_P_NAME, threads: PAGE_THREADS.map((channelId) => {
      const shortId = channelId.slice(GROUP_P.length + 4);
      return threadRecord(GROUP_P, shortId, PAGE_THREAD_NAMES.get(channelId)!);
    }) }],
  ]);
  const threadIndex = new Map<string, GatewayThread>();
  for (const group of groups.values()) {
    for (const thread of group.threads) threadIndex.set(thread.channelId, thread);
  }
  const workspaceTasks = new Map<string, GatewayTask[]>([
    [WORKSPACE_A, [
      taskRecord(WORKSPACE_A, TASK_A, "DS-1", "Service association design task"),
      taskRecord(WORKSPACE_A, TASK_KEEP, "DS-2", "KEEP_EXPLICIT task"),
    ]],
    [WORKSPACE_P, PAGE_TASKS.map((taskId, index) =>
      taskRecord(WORKSPACE_P, taskId, `PG-${index + 1}`, `Page item ${index}`))],
    [WORKSPACE_DENIED, [
      taskRecord(WORKSPACE_DENIED, TASK_SECRET_TARGET, "SEC-1", SECRET_TASK_TITLE),
      taskRecord(WORKSPACE_DENIED, TASK_DENIED_WORKSPACE, "SEC-2", "Denied workspace task"),
    ]],
  ]);
  const workspaceNames = new Map<string, { name: string; slug: string }>([
    [WORKSPACE_A, { name: "Discovery design", slug: "discovery-design" }],
    [WORKSPACE_P, { name: "Discovery pages", slug: "discovery-pages" }],
    [WORKSPACE_DENIED, { name: "Secret workspace", slug: "secret-workspace" }],
  ]);

  function targetDenied(channelId: string): boolean {
    return deniedThreads.has(channelId);
  }

  function groupThreads(groupId: string, url: URL): Response {
    if (groupId === GROUP_DENIED) {
      return errorResponse(403, "err.server.bot_api.not_group_member", "not an active group member");
    }
    const group = groups.get(groupId);
    if (group === undefined) return errorResponse(404, "NOT_FOUND", "group not found");
    const visible = group.threads.filter((thread) => !targetDenied(thread.channelId));
    const pageSizeText = url.searchParams.get("page_size");
    const pageIndexText = url.searchParams.get("page_index");
    if (pageSizeText === null && pageIndexText === null) {
      return Response.json(visible.map((thread) => wireThread(thread, group.name)));
    }
    const pageSize = Number(pageSizeText ?? "100");
    const pageIndex = Number(pageIndexText ?? "1");
    if (!Number.isInteger(pageSize) || pageSize < 1 || !Number.isInteger(pageIndex) || pageIndex < 1) {
      return errorResponse(400, "err.server.bot_api.request_invalid", "invalid pagination");
    }
    const start = (pageIndex - 1) * pageSize;
    return Response.json({
      count: visible.length,
      list: visible.slice(start, start + pageSize).map((thread) => wireThread(thread, group.name)),
    });
  }

  function threadDetail(groupId: string, shortId: string): Response {
    if (groupId === GROUP_DENIED) {
      return errorResponse(403, "err.server.bot_api.not_group_member", "thread not readable");
    }
    const thread = threadIndex.get(`${groupId}____${shortId}`);
    if (thread === undefined) return errorResponse(404, "NOT_FOUND", "thread not found");
    const group = groups.get(groupId);
    if (group === undefined) return errorResponse(403, "err.server.bot_api.not_group_member", "group not readable");
    if (targetDenied(thread.channelId)) {
      return errorResponse(403, "err.server.bot_api.not_group_member", "thread not readable");
    }
    return Response.json(wireThread(thread, group.name));
  }

  function globalGroupBuckets(keyword: string): Json {
    const empty = { sequence: 1, query_id: "fixture-empty", total_groups: 0, total_groups_approx: true, groups: [] };
    if (!keyword.includes("service")) return empty;
    const bucket = (channelId: string, withPreview: boolean): Json | null => {
      const thread = threadIndex.get(channelId);
      if (thread === undefined || targetDenied(channelId)) return null;
      return {
        channel_id: channelId,
        channel_type: 5,
        parent_group_no: thread.groupId,
        group_name: groups.get(thread.groupId)!.name,
        thread_id: thread.shortId,
        thread_name: thread.name,
        match_count: 2,
        match_count_approx: true,
        latest_at: "2026-11-16T08:00:00Z",
        preview: withPreview ? [wireMessage(MESSAGE_ID, channelId, "service association fixture hit")] : [],
      };
    };
    const buckets: Json[] = [bucket(THREAD_MSG, false), bucket(THREAD_A, true)]
      .filter((entry): entry is Json => entry !== null);
    buckets.push({
      channel_id: GROUP_B,
      channel_type: 2,
      group_name: GROUP_B_NAME,
      match_count: 1,
      match_count_approx: true,
      latest_at: "2026-11-16T08:00:00Z",
      preview: [],
    });
    return {
      sequence: 1,
      query_id: "fixture-groups-1",
      total_groups: buckets.length,
      total_groups_approx: true,
      groups: buckets,
    };
  }

  function channelMessages(body: Json): Response {
    const channelType = body.channel_type;
    const channelId = body.channel_id;
    if ((channelType !== 2 && channelType !== 5) || typeof channelId !== "string") {
      return errorResponse(400, "err.server.bot_api.request_invalid", "invalid channel");
    }
    if (targetDenied(channelId)) return errorResponse(403, "err.server.bot_api.not_group_member", "channel not readable");
    const isKnownTarget = channelType === 2
      ? channelId === GROUP_B
      : channelId === THREAD_MSG;
    if (!isKnownTarget) return Response.json({ data: [], pagination: { has_more: false, next_cursor: "" } });
    if (body.keyword !== "service association") {
      return errorResponse(400, "err.server.bot_api.request_invalid", "unexpected search keyword");
    }
    const cursor = body.cursor;
    if (cursor === undefined || cursor === null) {
      return Response.json({
        data: [wireMessage(
          channelType === 2 ? GROUP_MESSAGE_ID : MESSAGE_ID,
          channelId,
          "first page service hit",
          channelType,
        )],
        pagination: { has_more: true, next_cursor: "fixture-cursor-2" },
      });
    }
    if (cursor === "fixture-cursor-2") {
      return Response.json({
        data: [wireMessage(
          channelType === 2 ? GROUP_MESSAGE_ID_2 : MESSAGE_ID_2,
          channelId,
          "second page service hit",
          channelType,
        )],
        pagination: { has_more: false, next_cursor: "" },
      });
    }
    return errorResponse(400, "err.server.bot_api.request_invalid", "invalid cursor");
  }

  function fleetResponse(url: URL, workspaceId: string | null): Response {
    if (url.pathname === "/fleet/api/v1/workspaces") {
      const pageSize = Number(url.searchParams.get("page_size") ?? "100");
      const page = Number(url.searchParams.get("page") ?? "1");
      const rows = [WORKSPACE_A, WORKSPACE_P].map((id) => {
        const name = workspaceNames.get(id)!;
        return workspaceRow(id, name.name, name.slug);
      });
      return Response.json({
        data: page === 1 ? rows : [],
        pagination: { total: rows.length, page, page_size: pageSize },
      });
    }
    // Loop's background scan uses the paginated GET task-list endpoint; _search below is a separate contract.
    if (url.pathname === "/fleet/api/v1/tasks") {
      if (workspaceId === null) {
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      const tasks = workspaceTasks.get(workspaceId);
      if (tasks === undefined || workspaceId === WORKSPACE_DENIED) {
      // Preserve actual membership failures for deliberately denied workspaces; never return an empty success page.
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      const pageSize = Number(url.searchParams.get("page_size"));
      const page = Number(url.searchParams.get("page"));
      if (
        pageSize !== 100
        || !Number.isSafeInteger(page)
        || page < 1
        || url.searchParams.get("sort") !== "created_at"
        || url.searchParams.get("direction") !== "asc"
      ) {
        return errorResponse(400, "INVALID_PAGINATION", "invalid task-list pagination");
      }
      return Response.json({
        data: tasks.slice((page - 1) * pageSize, page * pageSize).map(wireTask),
        pagination: { total: tasks.length, page, page_size: pageSize },
      });
    }
    const workspaceMatch = /^\/fleet\/api\/v1\/workspaces\/([^/]+)$/u.exec(url.pathname);
    if (workspaceMatch?.[1] !== undefined) {
      const id = decodeURIComponent(workspaceMatch[1]);
      const name = workspaceNames.get(id);
      if (name === undefined) return errorResponse(404, "WORKSPACE_NOT_FOUND", "workspace not found");
      if (workspaceId !== id || id === WORKSPACE_DENIED) {
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      return Response.json(workspaceDetail(id, name.name, name.slug));
    }
    if (url.pathname === "/fleet/api/v1/tasks/_search") {
      if (workspaceId === null) {
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      const tasks = workspaceTasks.get(workspaceId);
      if (tasks === undefined || workspaceId === WORKSPACE_DENIED) {
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      const query = url.searchParams.get("q") ?? "";
      const pageSize = Number(url.searchParams.get("page_size") ?? "100");
      const page = Number(url.searchParams.get("page") ?? "1");
      const matched = tasks.filter((task) =>
        query.length === 0 || task.title.toLowerCase().includes(query.toLowerCase()));
      return Response.json({
        data: matched.slice((page - 1) * pageSize, page * pageSize).map(wireTask),
        pagination: { total: matched.length, page, page_size: pageSize },
      });
    }
    const taskMatch = /^\/fleet\/api\/v1\/tasks\/([^/]+)$/u.exec(url.pathname);
    if (taskMatch?.[1] !== undefined) {
      const taskId = decodeURIComponent(taskMatch[1]);
      if (workspaceId === null) {
        return errorResponse(403, "BOT_WORKSPACE_MEMBERSHIP_REQUIRED", "workspace membership required");
      }
      const tasks = workspaceTasks.get(workspaceId);
      const task = tasks?.find((candidate) => candidate.taskId === taskId);
      if (task === undefined) return errorResponse(404, "TASK_NOT_FOUND", "task not found");
      return Response.json(wireTask(task));
    }
    return errorResponse(404, "NOT_FOUND", "not found");
  }

  async function proxy(url: URL, request: Request, body?: string): Promise<Response> {
    const target = new URL(`${url.pathname}${url.search}`, fixture.apiUrl);
    const headers = new Headers();
    for (const [key, value] of request.headers) {
      if (key === "host" || key === "content-length" || key === "connection" || key === "accept-encoding") continue;
      headers.set(key, value);
    }
    const payload = body ?? (request.method === "GET" || request.method === "HEAD" ? undefined : await request.text());
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      ...(payload === undefined ? {} : { body: payload }),
      redirect: "manual",
    });
    const responseHeaders = new Headers(upstream.headers);
    for (const key of ["content-encoding", "content-length", "transfer-encoding", "connection"]) responseHeaders.delete(key);
    const bytes = new Uint8Array(await upstream.arrayBuffer());
    return new Response(bytes.byteLength === 0 ? null : bytes, { status: upstream.status, headers: responseHeaders });
  }

  async function route(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const body = request.method === "GET" || request.method === "HEAD" ? undefined : await request.text();
    const bodyText = body ?? "";
    const authorization = request.headers.get("authorization");
    const workspaceId = request.headers.get("x-workspace-id");
    if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/fleet/")) {
      requests.push({
        method: request.method,
        pathname: url.pathname,
        search: url.search,
        authorization,
        workspaceId,
        body: bodyText,
      });
    }
    if (url.pathname === "/v1/bot/register") return proxy(url, request, body);
    if (!url.pathname.startsWith("/v1/") && !url.pathname.startsWith("/fleet/")) {
      return proxy(url, request, body);
    }
    if (authorization !== `Bearer ${fixture.botToken}`) {
      return errorResponse(401, "err.server.bot_api.auth_failed", "invalid bot token");
    }
    if (request.method === "GET" && url.pathname === "/v1/bot/groups") {
      return Response.json([...groups.entries()].map(([groupId, group]) => ({
        group_no: groupId,
        name: group.name,
        space_id: "space-fixture",
      })));
    }
    const groupMatch = /^\/v1\/bot\/groups\/([^/]+)\/threads$/u.exec(url.pathname);
    if (groupMatch?.[1] !== undefined) return groupThreads(decodeURIComponent(groupMatch[1]), url);
    const threadMatch = /^\/v1\/bot\/groups\/([^/]+)\/threads\/([^/]+)$/u.exec(url.pathname);
    if (threadMatch?.[1] !== undefined && threadMatch[2] !== undefined) {
      return threadDetail(decodeURIComponent(threadMatch[1]), decodeURIComponent(threadMatch[2]));
    }
    if (request.method === "POST" && url.pathname === "/v1/bot/messages/_search_global_groups") {
      const parsed = jsonObject(JSON.parse(bodyText) as unknown, "group search body");
      const keyword = stringField(parsed, "keyword") ?? "";
      if (keyword.trim().length === 0 || Array.from(keyword).length > 64) {
        return errorResponse(400, "VALIDATION_ERROR", "keyword must be 1..64 code points");
      }
      return Response.json({ data: globalGroupBuckets(keyword), pagination: { has_more: false, next_cursor: "" } });
    }
    if (request.method === "POST" && url.pathname === "/v1/bot/messages/_search") {
      return channelMessages(jsonObject(JSON.parse(bodyText) as unknown, "message search body"));
    }
    if (url.pathname.startsWith("/fleet/")) return fleetResponse(url, workspaceId);
    return proxy(url, request, body);
  }

  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: route });
  return {
    url: server.url.origin,
    requests,
    deniedThreads,
    denyThread: (channelId: string) => { deniedThreads.add(channelId); },
    close: () => server.stop(true),
  };
}

// ─── HTTPS Decisions 夹具 ───────────────────────────────────────────────────

interface DecisionCapture {
  input: string;
  query: string;
  candidates: Array<{ id: string; text: string }>;
  answers: string[];
}

interface DecisionFixture {
  url: string;
  requests: DecisionCapture[];
  inputs: string[];
  state: { mode: "filter" | "fail" };
  close(): void;
}

function startDecisionFixture(keyPath: string, certificatePath: string): DecisionFixture {
  const requests: DecisionCapture[] = [];
  const inputs: string[] = [];
  const state: { mode: "filter" | "fail" } = { mode: "filter" };
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: { key: Bun.file(keyPath), cert: Bun.file(certificatePath) },
    async fetch(request) {
      const url = new URL(request.url);
      if (request.method !== "POST" || url.pathname !== "/v1/decisions") {
        return errorResponse(404, "NOT_FOUND", "not found");
      }
      if (request.headers.get("authorization") !== `Bearer ${DISCOVERY_KEY}`) {
        return errorResponse(401, "AUTH_FAILED", "invalid decision key");
      }
      const body = jsonObject(JSON.parse(await request.text()) as unknown, "decision request");
      const input = stringField(body, "input");
      if (input === undefined) return errorResponse(400, "VALIDATION_ERROR", "input is required");
      inputs.push(input);
      let stateObject: Json = {};
      try {
        stateObject = jsonObject(JSON.parse(input) as unknown, "decision input");
      } catch {
        return errorResponse(400, "VALIDATION_ERROR", "input must be JSON");
      }
      const query = stringField(stateObject, "query") ?? "";
      const rawCandidates = Array.isArray(stateObject.candidates) ? stateObject.candidates : [];
      const candidates = rawCandidates.map((entry) => {
        const candidate = jsonObject(entry, "decision candidate");
        return { id: stringField(candidate, "id") ?? "", text: stringField(candidate, "text") ?? "" };
      });
      if (state.mode === "fail") {
        return errorResponse(500, "DECISION_FAILED", "fixture decision failure");
      }
      const rejected = (candidate: { id: string; text: string }): boolean =>
        candidate.id === THREAD_FILTER
        || candidate.id === THREAD_KEEP
        || candidate.text.includes("FILTER_ME")
        || candidate.text.includes("KEEP_EXPLICIT");
      const answers = candidates.map((candidate) => {
        const relevant = !rejected(candidate);
        return {
          name: candidate.id,
          type: "choice",
          choice: relevant ? "relevant" : "not_relevant",
          probabilities: [
            { value: "relevant", probability: relevant ? 0.91 : 0.06 },
            { value: "not_relevant", probability: relevant ? 0.09 : 0.94 },
          ],
        };
      });
      requests.push({ input, query, candidates, answers: answers.map((answer) => answer.name) });
      return Response.json({ model: "fixture-discovery-model", answers, usage: { input_tokens: 10, output_tokens: 4 } });
    },
  });
  return {
    url: `https://127.0.0.1:${server.port}`,
    requests,
    inputs,
    state,
    close: () => server.stop(true),
  };
}

async function createDecisionCertificate(root: string): Promise<{ keyPath: string; certificatePath: string }> {
  const configurationPath = join(root, "tls.conf");
  await writeFile(configurationPath, [
    "[req]",
    "prompt=no",
    "distinguished_name=dn",
    "x509_extensions=v3",
    "[dn]",
    "CN=localhost",
    "[v3]",
    "subjectAltName=DNS:localhost,IP:127.0.0.1",
    "basicConstraints=critical,CA:TRUE",
    "keyUsage=keyCertSign,digitalSignature,keyEncipherment",
  ].join("\n"));
  const keyPath = join(root, "decision-key.pem");
  const certificatePath = join(root, "decision-certificate.pem");
  const child = Bun.spawn([
    "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-x509", "-days", "1",
    "-keyout", keyPath, "-out", certificatePath, "-config", configurationPath,
  ], { stdout: "ignore", stderr: "pipe" });
  const error = await new Response(child.stderr).text();
  assert.equal(await child.exited, 0, error);
  return { keyPath, certificatePath };
}

// ─── 基准主体 ───────────────────────────────────────────────────────────────

async function seedLocalAssociations(stateDir: string, botUid: string, filterPath: string): Promise<void> {
  const store = await openChannelStore(stateDir);
  try {
    const workspaceA = loopWorkspace(WORKSPACE_A, "Discovery design", "discovery-design");
    const workspaceP = loopWorkspace(WORKSPACE_P, "Discovery pages", "discovery-pages");
    const workspaceDenied = loopWorkspace(WORKSPACE_DENIED, "Secret workspace", "secret-workspace");
    store.bindLoopWorkspace(botUid, GROUP_A, workspaceA, [
      loopTask(TASK_A, "Service association design task"),
      loopTask(TASK_KEEP, "KEEP_EXPLICIT task"),
    ]);
    store.bindLoopTask({ botUid, groupId: GROUP_A, workspaceId: WORKSPACE_A, taskId: TASK_A }, THREAD_A);
    store.bindLoopTask({ botUid, groupId: GROUP_A, workspaceId: WORKSPACE_A, taskId: TASK_KEEP }, THREAD_KEEP);

    store.bindLoopWorkspace(
      botUid,
      GROUP_P,
      workspaceP,
      PAGE_TASKS.map((taskId, index) => loopTask(taskId, `Page item ${index}`)),
    );
    for (const [index, taskId] of PAGE_TASKS.entries()) {
      store.bindLoopTask(
        { botUid, groupId: GROUP_P, workspaceId: WORKSPACE_P, taskId },
        PAGE_THREADS[index]!,
      );
    }

    store.bindLoopWorkspace(botUid, GROUP_WS, workspaceDenied, [
      loopTask(TASK_SECRET_TARGET, SECRET_TASK_TITLE),
    ]);
    store.bindLoopTask(
      { botUid, groupId: GROUP_WS, workspaceId: WORKSPACE_DENIED, taskId: TASK_SECRET_TARGET },
      THREAD_WSDENIED,
    );
    store.bindLoopWorkspace(botUid, GROUP_DENIED, workspaceDenied, [
      loopTask(TASK_DENIED_WORKSPACE, "Denied workspace task"),
    ]);
    store.bindLoopTask(
      { botUid, groupId: GROUP_DENIED, workspaceId: WORKSPACE_DENIED, taskId: TASK_DENIED_WORKSPACE },
      THREAD_DENIED,
    );

    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_B }, [reference("pr", PR_URL)]);
    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_KEEP }, [reference("pr", KEEP_REF_URL)]);
    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_WSDENIED }, [reference("pr", WSDENIED_REF_URL)]);
    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_DENIED }, [reference("pr", SECRET_PR_URL)]);

    store.saveThreadWorkspace({
      botUid,
      channelType: 5,
      channelId: THREAD_FILTER,
      layout: "single",
      status: "ready",
      entryPath: join(filterPath, "entry.ts"),
      workspaceRoot: filterPath,
      projects: [{
        name: "filter-service",
        repo: FILTER_REPO,
        path: filterPath,
        branch: "main",
        created: true,
      }],
      error: null,
    });

    const otherBotUid = OTHER_BOT_UID;
    store.bindLoopWorkspace(otherBotUid, GROUP_A, workspaceA, [
      loopTask(TASK_OTHER_BOT, "Other bot task"),
    ]);
    store.bindLoopTask(
      { botUid: otherBotUid, groupId: GROUP_A, workspaceId: WORKSPACE_A, taskId: TASK_OTHER_BOT },
      THREAD_OTHER_BOT,
    );
    store.addThreadReferences(otherBotUid, { channelType: 5, channelId: THREAD_OTHER_BOT }, [reference("pr", PR_URL)]);
  } finally {
    store.close();
  }
}

interface PromptCase {
  prompt: string;
  text: string;
  target: "dm" | "group";
}

async function main(): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-discovery-benchmark-")));
  let fixtureForCleanup: OctoProtocolFixture | undefined;
  let gatewayForCleanup: DiscoveryGateway | undefined;
  let decisionsForCleanup: DecisionFixture | undefined;
  try {
  const stateDir = join(root, "state");
  const sessionsDir = join(root, "sessions");
  const binDir = join(root, "bin");
  const gateDir = join(root, "gates");
  const filterPath = join(root, "fixture-project");
  await mkdir(stateDir);
  await mkdir(sessionsDir);
  await mkdir(binDir);
  await mkdir(gateDir);
  await mkdir(filterPath);
  const certificate = await createDecisionCertificate(root);
  const fixture = fixtureForCleanup = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  SECRET_TOKENS.push(fixture.botToken, DISCOVERY_KEY);
  const gateway = gatewayForCleanup = startDiscoveryGateway(fixture);
  const decisions = decisionsForCleanup = startDecisionFixture(certificate.keyPath, certificate.certificatePath);
  const children: Bun.Subprocess[] = [];
  const outputs: Promise<string>[] = [];
  let database: Database | undefined;
  let succeeded = false;

  const plans = {
    "discovery:local": {
      search: {
        action: "search",
        query: "service association",
        workspace_id: WORKSPACE_A,
        task_id: TASK_A,
        reference_url: PR_URL,
        repo: FILTER_REPO,
        path: filterPath,
      },
    },
    "discovery:pages": {
      search: { action: "search", query: "pagination-only", workspace_id: WORKSPACE_P },
      gateFirstCandidatePage: true,
    },
    "discovery:workspace-denied": {
      search: {
        action: "search",
        query: "workspace association",
        workspace_id: WORKSPACE_DENIED,
        reference_url: WSDENIED_REF_URL,
      },
    },
    "discovery:denied": {
      search: {
        action: "search",
        query: "secret association",
        reference_url: SECRET_PR_URL,
        workspace_id: WORKSPACE_DENIED,
        task_id: TASK_SECRET_TARGET,
      },
    },
    "discovery:bot-isolation": {
      search: { action: "search", query: "service association", reference_url: PR_URL },
    },
    "discovery:model": {
      search: {
        action: "search",
        query: "service association",
        reference_url: KEEP_REF_URL,
        workspace_id: WORKSPACE_A,
        repo: FILTER_REPO,
        path: filterPath,
      },
    },
    "discovery:model-fail": {
      search: {
        action: "search",
        query: "service association",
        reference_url: KEEP_REF_URL,
        workspace_id: WORKSPACE_A,
        repo: FILTER_REPO,
        path: filterPath,
      },
    },
  };
  const plansPath = join(root, "plans.json");
  await writeFile(plansPath, JSON.stringify(plans), "utf8");
  await writeFile(join(binDir, "octo-cli"), "#!/bin/sh\nexit 127\n", { mode: 0o755 });

  const baseEnvironment = {
    HOME: root,
    PATH: `${binDir}:/usr/bin:/bin`,
    TMPDIR: root,
    LANG: "C",
    TZ: "UTC",
    PI_NO_TITLE: "1",
    PI_CODING_AGENT_DIR: join(root, "agent"),
    OMP_CHANNEL_OCTO_HTTP_ENABLED: "false",
    NODE_EXTRA_CA_CERTS: certificate.certificatePath,
    [KEY_ENV]: DISCOVERY_KEY,
  } as Record<string, string>;

  function configPathFor(withDecisions: boolean): string {
    return join(root, withDecisions ? "config-decisions.json" : "config.json");
  }

  function writeConfig(withDecisions: boolean, auditPath: string): Promise<void> {
    const common = {
      apiUrl: gateway.url,
      botToken: fixture.botToken,
      ownerId: fixture.ownerUid,
      allowedGroupIds: [GROUP_A, GROUP_B, GROUP_P, GROUP_WS],
      stateDir,
      sessionsDir,
      ompCommand: process.execPath,
      ompExtraArgs: [BENCHMARK_PATH, "--worker", "--audit", auditPath, "--plans", plansPath, "--gate", gateDir],
      startupTimeoutMs: 20_000,
      cancelTimeoutMs: 5_000,
      groupHistory: { enabled: false, maxMessages: 50 },
      watch: { enabled: false },
      ...(withDecisions
        ? {
          decisions: {
            enabled: true,
            protocol: "openai-decisions",
            baseUrl: decisions.url,
            model: "fixture-discovery-model",
            apiKeyEnv: KEY_ENV,
          },
        }
        : {}),
    };
    return writeFile(configPathFor(withDecisions), JSON.stringify(common), { mode: 0o600 });
  }

  async function launch(withDecisions: boolean, auditPath: string): Promise<{ child: Bun.Subprocess; connection: FixtureConnection }> {
    await writeConfig(withDecisions, auditPath);
    const child = Bun.spawn(
      [process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPathFor(withDecisions)],
      {
        cwd: root,
        env: baseEnvironment,
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    children.push(child);
    outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
      .then((parts) => parts.join("\n")));
    const connection = await fixture.waitForConnection(children.length, 30_000);
    await waitForFixture(() => connection.ready ? true : undefined, 15_000, "channel CLI did not authenticate");
    return { child, connection };
  }

  async function exited(child: Bun.Subprocess, timeoutMs = 15_000): Promise<number> {
    const deadline = Promise.withResolvers<never>();
    const timer = setTimeout(() => deadline.reject(new Error("benchmark CLI exit deadline exceeded")), timeoutMs);
    try {
      return await Promise.race([child.exited, deadline.promise]);
    } finally {
      clearTimeout(timer);
    }
  }

  async function shutdown(child: Bun.Subprocess): Promise<void> {
    if (child.exitCode !== null) return;
    child.kill("SIGTERM");
    assert.equal(await exited(child), 0);
  }

  let messageSequence = 0;
  function send(
    connection: FixtureConnection,
    target: "dm" | "group",
    text: string,
    senderUid = fixture.ownerUid,
  ): string {
    const messageId = String(++messageSequence + 10_000);
    if (target === "dm") {
      fixture.sendInbound(connection, {
        fromUid: senderUid,
        channelId: `${senderUid}@${fixture.botUid}`,
        channelType: 1,
        text,
        messageId,
      });
    } else {
      fixture.sendInbound(connection, {
        fromUid: senderUid,
        channelId: GROUP_A,
        channelType: 2,
        text,
        messageId,
        mention: { uids: [fixture.botUid] },
      });
    }
    return messageId;
  }

  async function waitWorkerDone(auditPath: string, prompt: string): Promise<Json[]> {
    const entries = await waitForFixture(() => {
      const current = auditEntries(auditPath);
      const failure = current.find((entry) =>
        entry.prompt === prompt && (entry.kind === "error" || entry.kind === "missing_tool"));
      if (failure !== undefined) throw new Error(`${prompt} failed: ${JSON.stringify(failure)}`);
      return current.some((entry) => entry.prompt === prompt && entry.kind === "done") ? current : undefined;
    }, MAX_WORKER_TIMEOUT_MS, `${prompt} worker turn did not finish`);
    const entriesAfter = auditEntries(auditPath);
    assert.ok(entries.length >= 0);
    return entriesAfter;
  }

  async function waitTask(db: Database, messageId: string): Promise<void> {
    await waitForFixture(() => {
      const row = db.query("SELECT state FROM tasks WHERE message_id = ?").get(messageId) as { state?: string } | null;
      return row?.state === "completed" ? true : undefined;
    }, 30_000, `inbound ${messageId} did not complete`);
  }

  async function runCases(
    child: Bun.Subprocess,
    connection: FixtureConnection,
    auditPath: string,
    cases: readonly PromptCase[],
    onReady?: (prompt: string) => Promise<void> | void,
  ): Promise<void> {
    for (const testCase of cases) {
      const messageId = send(connection, testCase.target, testCase.text);
      if (onReady !== undefined) await onReady(testCase.prompt);
      await waitWorkerDone(auditPath, testCase.prompt);
      await waitTask(database!, messageId);
    }
    assert.equal(child.exitCode, null, "benchmark CLI exited during discovery cases");
  }

  try {
    await seedLocalAssociations(stateDir, fixture.botUid, filterPath);
    database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    const auditPath = join(root, "audit-run1.jsonl");
    const first = await launch(false, auditPath);
    const unauthorizedId = send(first.connection, "dm", "discovery:denied", "unauthorized-discovery-user");
    await waitForFixture(
      () => first.connection.recvAcks.some((ack) => ack.messageId === unauthorizedId) ? true : undefined,
      5_000,
      "unauthorized inbound was not acknowledged",
    );
    await Bun.sleep(200);
    assert.equal(
      auditEntries(auditPath).some((entry) => entry.kind === "search" && entry.prompt === "discovery:denied"),
      false,
      "unauthorized sender reached octo_discover",
    );
    assert.equal(
      database.query("SELECT message_id FROM tasks WHERE message_id = ?").get(unauthorizedId),
      null,
      "unauthorized sender created a task",
    );
    const firstCases: readonly PromptCase[] = [
      { prompt: "discovery:local", text: "discovery:local", target: "dm" },
      { prompt: "discovery:pages", text: "discovery:pages", target: "dm" },
      { prompt: "discovery:workspace-denied", text: "discovery:workspace-denied", target: "dm" },
      { prompt: "discovery:denied", text: "discovery:denied", target: "dm" },
      { prompt: "discovery:bot-isolation", text: "discovery:bot-isolation", target: "group" },
    ];
    await runCases(first.child, first.connection, auditPath, firstCases, async (prompt) => {
      if (prompt !== "discovery:pages") return;
      const entries = await waitForFixture(() => {
        const current = auditEntries(auditPath);
        return current.some((entry) => entry.kind === "page_ready" && entry.prompt === prompt) ? current : undefined;
      }, MAX_WORKER_TIMEOUT_MS, "pagination worker did not pause before requesting its continuation");
      const search = parseResult(auditText(entries, prompt, "search"));
      const pageOneIds = candidateIds(search);
      const revoked = PAGE_THREADS.find((channelId) => !pageOneIds.includes(channelId));
      assert.ok(revoked, "pagination fixture did not leave a page-two candidate");
      gateway.denyThread(revoked);
      await writeFile(join(gateDir, `${prompt}.go`), "go");
    });

    // 1. 本地 + 远端证据、读取入口、无模型回退。
    {
      const entries = auditEntries(auditPath);
      const result = parseResult(auditText(entries, "discovery:local", "search"));
      const candidates = candidatesOf(result);
      const ids = candidateIds(result);
      const byId = new Map(candidates.map((candidate) => [String(candidate.channel_id ?? candidate.channelId), candidate]));
      assert.ok(ids.includes(THREAD_A), "local Loop route candidate is missing");
      assert.ok(ids.includes(THREAD_B), "local recorded reference candidate is missing");
      assert.ok(ids.includes(THREAD_FILTER), "local workspace candidate is missing");
      const design = byId.get(THREAD_A)!;
      const delivery = byId.get(THREAD_B)!;
      const workspace = byId.get(THREAD_FILTER)!;
      assert.ok(evidenceKinds(design).includes("loop_route"), "explicit Loop route evidence missing");
      assert.ok(evidenceKinds(delivery).includes("recorded_reference"), "recorded PR evidence missing");
      assert.ok(evidenceKinds(workspace).includes("workspace"), "workspace evidence missing");
      const loopEvidence = evidenceEntries(design, "loop_route")[0]!;
      assert.equal(stringField(loopEvidence, "workspace_id"), WORKSPACE_A);
      assert.equal(stringField(loopEvidence, "task_id"), TASK_A);
      assert.equal(stringField(loopEvidence, "state"), "active");
      const recorded = evidenceEntries(delivery, "recorded_reference")[0]!;
      assert.equal(stringField(recorded, "url"), PR_URL);
      assert.equal(recorded.model_identified, true);
      readEntry(design);
      readEntry(delivery);
      const messageCandidate = ids.includes(THREAD_MSG) ? byId.get(THREAD_MSG)! : undefined;
      assert.ok(messageCandidate, "remote message-search candidate is missing");
      const messageEvidence = evidenceEntries(messageCandidate, "message_match");
      assert.ok(messageEvidence.length > 0, "message_match evidence is missing");
      const messageIds = new Set(messageEvidence.flatMap((entry) => {
        const id = stringField(entry, "message_id");
        return id === undefined ? [] : [id];
      }));
      assert.deepEqual([...messageIds].sort(), [MESSAGE_ID, MESSAGE_ID_2].sort(), "message cursor pages were not combined");
      const groupCandidate = byId.get(GROUP_B);
      assert.ok(groupCandidate, "type-2 message bucket lost its group identity");
      assert.equal(numberField(groupCandidate, "channel_type"), 2, "group message was remapped to a thread");
      assert.ok(evidenceKinds(groupCandidate).includes("message_match"), "group message evidence is missing");
      assert.equal(numberField(isJson(result.filtered) ? result.filtered : {}, "count"), 0, "disabled decisions filtered candidates");
      const sources = jsonObject(result.sources, "discovery sources");
      assert.ok(isJson(sources.message_search), "message search source status is missing");
      assert.notEqual(stringField(sources.message_search, "state"), "failed", "message search source failed");
      assert.equal(numberField(sources.message_search, "requests"), 4, "group and thread message cursor pages were not fetched");
      assert.ok(isJson(result.permissions), "permission coverage is missing");
      assert.ok(isJson(result.budgets), "declared budgets are missing");
      assert.ok(
        [WORKSPACE_A, WORKSPACE_P].every((workspaceId) => gateway.requests.some((request) =>
          request.method === "GET"
          && request.pathname === "/fleet/api/v1/tasks"
          && request.workspaceId === workspaceId
          && new URLSearchParams(request.search).get("page") === "1"
          && new URLSearchParams(request.search).get("page_size") === "100"
          && new URLSearchParams(request.search).get("sort") === "created_at"
          && new URLSearchParams(request.search).get("direction") === "asc",
        )),
        "Loop task scan did not use the paginated Fleet task-list contract",
      );
      const toolEntry = entries.find((entry) =>
        entry.kind === "prompt_tools" && entry.prompt === "discovery:local");
      assert.ok(toolEntry && Array.isArray(toolEntry.definitions), "worker did not receive host tools");
      const discovery = toolEntry.definitions.find((definition) =>
        isJson(definition) && definition.name === "octo_discover");
      assert.ok(isJson(discovery), "octo_discover was not advertised");
      const parameters = jsonObject(discovery.parameters, "octo_discover parameters");
      const properties = jsonObject(parameters.properties, "octo_discover properties");
      assert.ok(isJson(properties.action), "action schema is missing");
      assert.ok(
        Array.isArray(properties.action.enum)
        && properties.action.enum.includes("search")
        && properties.action.enum.includes("page"),
        "search/page actions are missing",
      );
      assert.ok(isJson(properties.query), "search query schema is missing");
      assert.ok(isJson(properties.query_id), "page query_id schema is missing");
      assert.equal(Object.hasOwn(properties, "bot_uid"), false, "tool accepted a bot selector");
      assert.equal(Object.hasOwn(properties, "token"), false, "tool accepted a token selector");
      const localCalls = entries.filter((entry) => entry.kind === "tool_call" && entry.prompt === "discovery:local")
        .map((entry) => jsonObject(entry.arguments, "discovery tool call"));
      assert.ok(
        localCalls.some((call) => call.action === "search" && call.query === "service association"),
        "search call did not carry the required query",
      );
    }

    // 2. 分页与续页权限复验。
    {
      const entries = auditEntries(auditPath);
      const firstPage = parseResult(auditText(entries, "discovery:pages", "search"));
      const pageOneIds = candidateIds(firstPage);
      assert.equal(pageOneIds.length, 20, "first candidate page was not bounded to 20");
      const page = jsonObject(firstPage.page, "first page metadata");
      assert.equal(numberField(page, "page_size"), 20, "first page size is not declared");
      assert.equal(page.has_more, true, "first page did not advertise continuation");
      assert.equal(numberField(page, "next_position"), 2, "first page continuation position is wrong");
      const pageTwo = parseResult(auditText(entries, "discovery:pages", "page"));
      const continuationCall = entries
        .filter((entry) => entry.kind === "tool_call" && entry.prompt === "discovery:pages")
        .map((entry) => jsonObject(entry.arguments, "pagination tool call"))
        .find((call) => call.action === "page");
      assert.ok(continuationCall, "worker did not request a page action");
      assert.equal(continuationCall.query_id, firstPage.query_id, "page call used a different query_id");
      assert.equal(continuationCall.position, 2, "page call used the wrong continuation position");
      assert.equal(Object.hasOwn(continuationCall, "query"), false, "page call incorrectly required a search query");
      const pageTwoIds = candidateIds(pageTwo);
      const pageTwoMeta = jsonObject(pageTwo.page, "second page metadata");
      assert.equal(numberField(pageTwoMeta, "position"), 2, "second page position is wrong");
      assert.equal(pageTwoMeta.has_more, false, "second page unexpectedly continued");
      const revoked = [...gateway.deniedThreads][0];
      assert.ok(revoked !== undefined, "benchmark did not revoke a page-two target");
      assert.equal(pageTwoIds.includes(revoked), false, "revoked target remained in the continuation page");
      assertNoTokens(pageTwo, [revoked, ...SECRET_TOKENS], "continuation page");
      assert.deepEqual(
        [...new Set([...pageOneIds, ...pageTwoIds])].sort(),
        PAGE_THREADS.filter((channelId) => channelId !== revoked).sort(),
        "continuation pages did not cover exactly the still-readable candidates",
      );
      assert.equal(pageTwoIds.length, 4, "revoked target was not the only removed page-two candidate");
    }

    // 3. Loop workspace 失权时移除证据但不泄露，目标仍可由其他证据返回。
    {
      const entries = auditEntries(auditPath);
      const result = parseResult(auditText(entries, "discovery:workspace-denied", "search"));
      const ids = candidateIds(result);
      assert.ok(ids.includes(THREAD_WSDENIED), "readable target with non-Loop evidence was dropped");
      const candidate = candidatesOf(result).find((entry) => String(entry.channel_id ?? entry.channelId) === THREAD_WSDENIED)!;
      assert.ok(evidenceKinds(candidate).includes("recorded_reference"), "non-Loop evidence was lost");
      assert.equal(evidenceKinds(candidate).includes("loop_route"), false, "denied Loop workspace evidence was returned");
      assertNoTokens(result, SECRET_TOKENS, "workspace-denied result");
    }

    // 4. 无权目标及其本地残存记录完全不出现在结果中。
    {
      const entries = auditEntries(auditPath);
      const result = parseResult(auditText(entries, "discovery:denied", "search"));
      const ids = candidateIds(result);
      assert.equal(ids.includes(THREAD_DENIED), false, "denied target leaked");
      assertNoTokens(result, SECRET_TOKENS, "denied search");
      const sources = isJson(result.sources) ? result.sources : {};
      const permissionsState = stringField(isJson(result.permissions) ? result.permissions : {}, "state")
        ?? stringField(isJson(sources.permissions) ? sources.permissions : {}, "state");
      assert.notEqual(permissionsState, "complete", "failed permission verification was reported as complete");
      assert.ok(
        gateway.requests.some((request) =>
          request.pathname === `/v1/bot/groups/${GROUP_DENIED}/threads/topic-secret`
          || request.pathname === `/v1/bot/groups/${GROUP_DENIED}/threads`),
        "benchmark never attempted the denied target verification",
      );
    }

    // 5. 不同 bot 的本地记录不泄露；群聊也可调用同一工具。
    {
      const entries = auditEntries(auditPath);
      const result = parseResult(auditText(entries, "discovery:bot-isolation", "search"));
      const ids = candidateIds(result);
      assert.ok(ids.includes(THREAD_B), "own recorded reference was not found from a group chat");
      assert.equal(ids.includes(THREAD_OTHER_BOT), false, "another bot's local association leaked");
      for (const candidate of candidatesOf(result)) {
        for (const entry of Array.isArray(candidate.evidence) ? candidate.evidence : []) {
          if (!isJson(entry)) continue;
          assert.notEqual(stringField(entry, "task_id"), TASK_OTHER_BOT, "another bot's Loop task leaked");
        }
      }
      assertNoTokens(result, [...SECRET_TOKENS, TASK_OTHER_BOT, THREAD_OTHER_BOT], "other bot search");
    }

    await shutdown(first.child);

    database.close();
    database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    const auditPathTwo = join(root, "audit-run2.jsonl");
    const second = await launch(true, auditPathTwo);
    const decisionsEnabledCases: readonly PromptCase[] = [
      { prompt: "discovery:model", text: "discovery:model", target: "dm" },
    ];
    await runCases(second.child, second.connection, auditPathTwo, decisionsEnabledCases);

    // 6. 已配置 Decisions：授权后排序/过滤，过滤集合独立分页，明确关联不被过滤，模型看不到无权目标。
    {
      const entries = auditEntries(auditPathTwo);
      const result = parseResult(auditText(entries, "discovery:model", "search"));
      const ids = candidateIds(result);
      assert.ok(ids.includes(THREAD_A), "normal candidate missing after model evaluation");
      assert.ok(ids.includes(THREAD_KEEP), "explicit Loop/reference candidate was filtered out");
      assert.equal(ids.includes(THREAD_FILTER), false, "model-filtered candidate remained in the normal page");
      const filtered = jsonObject(result.filtered, "filtered metadata");
      assert.ok((numberField(filtered, "count") ?? 0) > 0, "filtered candidate count is missing");
      const model = jsonObject(result.model, "model status");
      assert.ok(modelSucceeded(stringField(model, "status")), "model success was not reported");
      const filteredPage = parseResult(auditText(entries, "discovery:model", "filtered"));
      const filteredIds = candidateIds(filteredPage);
      assert.ok(filteredIds.includes(THREAD_FILTER), "filtered candidate cannot be recovered from its separate page");
      assert.equal(filteredIds.includes(THREAD_KEEP), false, "explicit candidate appeared in the filtered page");
      assertNoTokens(result, SECRET_TOKENS, "model-normal page");
      assertNoTokens(filteredPage, SECRET_TOKENS, "model-filtered page");
      assert.ok(decisions.requests.length > 0, "model was never called");
      const latest = decisions.requests.at(-1)!;
      assert.ok(latest.query.includes("service association"), "model query was not the discovery query");
      assert.ok(latest.candidates.length >= 3, "model did not receive the explicit candidates");
      assert.equal(latest.candidates.length, latest.answers.length, "model answer count did not match candidates");
      assert.equal(new Set(latest.candidates.map((candidate) => candidate.id)).size, latest.candidates.length);
      assertNoTokens(latest.input, SECRET_TOKENS, "model input");
    }

    // 7. 模型失败时保留全部候选并把处理标为未完成。
    decisions.state.mode = "fail";
    const failureCases: readonly PromptCase[] = [
      { prompt: "discovery:model-fail", text: "discovery:model-fail", target: "dm" },
    ];
    await runCases(second.child, second.connection, auditPathTwo, failureCases);
    {
      const entries = auditEntries(auditPathTwo);
      const result = parseResult(auditText(entries, "discovery:model-fail", "search"));
      const ids = candidateIds(result);
      assert.ok(ids.includes(THREAD_FILTER), "model failure silently dropped a candidate");
      assert.ok(ids.includes(THREAD_KEEP), "model failure dropped an explicit candidate");
      assert.ok(ids.includes(THREAD_A), "model failure dropped a local candidate");
      const filtered = isJson(result.filtered) ? result.filtered : {};
      assert.equal(numberField(filtered, "count"), 0, "failed model still reported filtered candidates");
      const model = jsonObject(result.model, "failed model status");
      const status = stringField(model, "status");
      assert.ok(status !== undefined && !modelSucceeded(status), "model failure was reported as a completed evaluation");
      assertNoTokens(result, SECRET_TOKENS, "model-failure result");
    }
    assert.equal(decisions.inputs.length, 2, "model-failure request did not reach the Decisions fixture");
    assertNoTokens(decisions.inputs.at(-1), SECRET_TOKENS, "model-failure input");

    await shutdown(second.child);
    const cliLogs = (await Promise.all(outputs)).join("\n");
    for (const [groupId, workspaceId] of [[GROUP_A, WORKSPACE_A], [GROUP_P, WORKSPACE_P]] as const) {
      assert.equal(
        cliLogs.includes(`[loop] task scan failed group=${groupId} workspace=${workspaceId}`),
        false,
        `authorized Loop task scan failed for workspace ${workspaceId}`,
      );
    }
    const protectedRequests = gateway.requests.filter((request) => request.pathname !== "/v1/bot/register");
    assert.ok(protectedRequests.length > 0, "discovery made no protected remote request");
    assert.ok(
      protectedRequests.every((request) => request.authorization === `Bearer ${fixture.botToken}`),
      "a protected remote request omitted or changed the fixture authorization",
    );
    succeeded = true;
  } finally {
    for (const child of children) {
      if (child.exitCode !== null) continue;
      child.kill("SIGTERM");
      try {
        await exited(child, 5_000);
      } catch {
        child.kill("SIGKILL");
        await child.exited;
      }
    }
    database?.close();
    const logs = (await Promise.all(outputs)).join("\n");
    if (!succeeded) console.error(logs.replaceAll(fixture.botToken, "[fixture-token]").replaceAll(DISCOVERY_KEY, "[fixture-key]"));
    assert.ok(!logs.includes(fixture.botToken), "benchmark CLI leaked its fixture bot token");
    assert.ok(!logs.includes(DISCOVERY_KEY), "benchmark CLI leaked its decision key");
  }

  } finally {
    gatewayForCleanup?.close();
    decisionsForCleanup?.close();
    await fixtureForCleanup?.close();
    await rm(root, { recursive: true, force: true });
  }
}

await (process.argv.includes("--worker") ? runWorker() : main());
