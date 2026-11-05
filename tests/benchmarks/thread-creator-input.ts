import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import { openChannelStore } from "../../src/store/index.ts";
import {
  createOctoProtocolFixture,
  type FixtureConnection,
  type FixtureManagementThread,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Offline end-to-end acceptance: installed OMP RPC, real parent-side thread creation and one
// child-thread host-tool call, an isolated parent queue, and loopback Octo/model fixtures.
// No resident service, real Octo API, or external model provider is used.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-thread-creator-input-")));
const stateDir = join(root, "state");
const parentCwd = join(root, "parent-cwd");
const childCwd = join(root, "child-cwd");
const auditPath = join(root, "omp-audit.jsonl");
const groupId = "thread-creator-input-smoke";
const runId = `${process.pid}-${Date.now()}`;
const childCreationMarker = `THREAD_CREATOR_CREATE_CHILD_${runId}`;
const sourceThreadName = `thread-creator-input-source-${runId}`;
const parentHoldMarker = `THREAD_CREATOR_PARENT_HOLD_${runId}`;
const childRequestMarker = `THREAD_CREATOR_CHILD_REQUEST_${runId}`;
const creatorInputBody = `THREAD_CREATOR_INPUT_BODY_${runId}`;
const childAction = "thread-send-parent";
const questionId = `thread-creator-input-open-ask-${runId}`;
const waitTimeoutMs = 45_000;

interface Gate {
  promise: Promise<void>;
  resolve(): void;
  released: boolean;
}
type ModelStep =
  | { kind: "text"; text: string }
  | { kind: "tool"; name: string; toolCallId: string; arguments: Record<string, unknown> };
interface ModelRequest {
  index: number;
  serialized: string;
}
type AuditRow = {
  kind: string;
  at?: number;
  pid?: number;
  wrapperPid?: number;
  direction?: "to-omp" | "from-omp";
  type?: string;
  op?: string;
  id?: string;
  toolName?: string;
  toolCallId?: string;
  command?: string;
  args?: string[];
  arguments?: unknown;
  isError?: boolean;
  exitCode?: number;
};
type Target = { channelType: 2 | 5; channelId: string };
type Binding = { session_id: string; cwd: string; session_file: string | null };
type TaskRow = {
  id: string;
  session_id: string;
  state: string;
};

const parent: Target = { channelType: 2, channelId: groupId };
let fixture: OctoProtocolFixture | undefined;
let connection: FixtureConnection | undefined;
let stopModel: (() => void) | undefined;
let modelError: string | undefined;
let sourceChannelId = "";
let db: Database | undefined;
let cli: Bun.Subprocess | undefined;
let cliOutput: Promise<string> | undefined;
let wrapperPath = "";
let succeeded = false;
let nextMessageId = 20_000;
let threadCreationCallIssued = false;
let toolCallIssued = false;
let creatorPromptCalls = 0;
let parentHoldCalls = 0;
let childFollowupWithReceipt = false;
const modelRequests: ModelRequest[] = [];
const gates: Gate[] = [];

function gate(): Gate {
  const deferred = Promise.withResolvers<void>();
  let released = false;
  const result: Gate = {
    promise: deferred.promise,
    get released() { return released; },
    resolve() {
      if (released) return;
      released = true;
      deferred.resolve();
    },
  };
  gates.push(result);
  return result;
}

const parentGate = gate();

function recordModelError(message: string): Response {
  modelError ??= message;
  return new Response(message, { status: 400 });
}

function completion(step: ModelStep, index: number): Response {
  const isTool = step.kind === "tool";
  const delta = isTool
    ? {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: step.toolCallId,
          type: "function",
          function: { name: step.name, arguments: JSON.stringify(step.arguments) },
        }],
      }
    : { role: "assistant", content: step.text };
  const id = `thread-creator-input-${index}`;
  const chunks = [
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta, finish_reason: null }],
    },
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: {}, finish_reason: isTool ? "tool_calls" : "stop" }],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  ];
  return new Response(chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function advertisesAction(body: Record<string, unknown>, toolName: string, actionName: string): boolean {
  if (!Array.isArray(body.tools)) return false;
  return body.tools.some((value) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
    const tool = value as Record<string, unknown>;
    const fn = tool.function !== null && typeof tool.function === "object" && !Array.isArray(tool.function)
      ? tool.function as Record<string, unknown>
      : undefined;
    const name = typeof tool.name === "string" ? tool.name : fn?.name;
    const parameters = tool.parameters ?? fn?.parameters;
    if (name !== toolName || parameters === null || typeof parameters !== "object" || Array.isArray(parameters)) return false;
    const properties = (parameters as Record<string, unknown>).properties;
    if (properties === null || typeof properties !== "object" || Array.isArray(properties)) return false;
    const action = (properties as Record<string, unknown>).action;
    if (action === null || typeof action !== "object" || Array.isArray(action)) return false;
    const values = (action as Record<string, unknown>).enum;
    return Array.isArray(values) && values.includes(actionName);
  });
}

function assertCreatorPrompt(serialized: string, sourceChannelId: string): void {
  assert.ok(serialized.includes(creatorInputBody), "Parent model prompt omitted the accepted creator input");
  assert.ok(serialized.includes(sourceChannelId), "Parent model prompt omitted the real source child channel identity");
  assert.match(serialized, /system|系统/iu, "Creator input lacks an explicit system/source context");
  assert.match(serialized, /source|来源/iu, "Creator input lacks source metadata");
  assert.match(serialized, /(?:not|non)[\s\S]{0,40}user|(?:非|不是|并非)[^\n]{0,32}用户/iu,
    "Creator input is not explicitly distinguished from a user message");
  assert.match(serialized, /(?:not|non)[\s\S]{0,40}answer|(?:非|不作为|不是)[^\n]{0,32}答案/iu,
    "Creator input is not explicitly distinguished from an ask answer");
  assert.match(serialized,
    /(?:not|non)[\s\S]{0,48}authori[sz]|(?:不构成|不代表|不得视为|不可视为|不是)[^\n]{0,36}授权/iu,
    "Creator input is not explicitly distinguished from authorization");
}

async function finiteModel(request: Request): Promise<Response> {
  if (request.method !== "POST") return recordModelError(`Unexpected local model method ${request.method}`);
  if (new URL(request.url).pathname !== "/v1/chat/completions") {
    return recordModelError(`Unexpected local model route ${new URL(request.url).pathname}`);
  }
  let body: Record<string, unknown>;
  try {
    const parsed: unknown = await request.json();
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return recordModelError("Local finite model received a non-object request");
    }
    body = parsed as Record<string, unknown>;
  } catch {
    return recordModelError("Local finite model received invalid JSON");
  }

  const serialized = JSON.stringify(body.messages ?? body);
  modelRequests.push({ index: modelRequests.length + 1, serialized });
  try {
    // A child continuation includes both the original child request and the tool arguments. Route
    // it before the parent-input marker, which is intentionally present inside those arguments.
    if (serialized.includes(childRequestMarker)) {
      if (!toolCallIssued) {
        assert.ok(advertisesAction(body, "octo_management", childAction),
          "Real child OMP prompt did not advertise thread-send-parent");
        toolCallIssued = true;
        return completion({
          kind: "tool",
          name: "octo_management",
          toolCallId: `creator_native_${runId}`,
          arguments: { action: childAction, text: creatorInputBody },
        }, modelRequests.length);
      }
      if (serialized.toLowerCase().includes("queued") && serialized.includes(groupId)) {
        childFollowupWithReceipt = true;
      }
      return completion({ kind: "text", text: `THREAD_CREATOR_CHILD_DONE_${runId}` }, modelRequests.length);
    }

    // This branch wins over the old parent marker, which remains in OMP history on the queued turn.
    if (serialized.includes(creatorInputBody)) {
      creatorPromptCalls += 1;
      assert.equal(creatorPromptCalls, 1, "Parent received duplicate creator-input model turns");
      assert.ok(sourceChannelId.length > 0, "Child source channel identity was not initialized");
      assertCreatorPrompt(serialized, sourceChannelId);
      return completion({ kind: "text", text: `THREAD_CREATOR_PARENT_INPUT_DONE_${runId}` }, modelRequests.length);
    }

    if (serialized.includes(parentHoldMarker)) {
      parentHoldCalls += 1;
      assert.equal(parentHoldCalls, 1, "Parent hold task unexpectedly made another model request");
      await parentGate.promise;
      return completion({ kind: "text", text: `THREAD_CREATOR_PARENT_HOLD_DONE_${runId}` }, modelRequests.length);
    }

    if (serialized.includes(childCreationMarker)) {
      if (!threadCreationCallIssued) {
        assert.ok(advertisesAction(body, "octo_management", "create-thread"),
          "Real parent OMP prompt did not advertise create-thread");
        threadCreationCallIssued = true;
        return completion({
          kind: "tool",
          name: "octo_management",
          toolCallId: `thread_create_${runId}`,
          arguments: { action: "create-thread", name: sourceThreadName },
        }, modelRequests.length);
      }
      return completion({ kind: "text", text: `THREAD_CREATOR_CHILD_CREATED_${runId}` }, modelRequests.length);
    }

    return recordModelError("Local finite model received an unknown task prompt");
  } catch (error) {
    return recordModelError(error instanceof Error ? error.message : String(error));
  }
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

function auditRows(path: string): AuditRow[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split("\n").flatMap((line) => {
    if (!line.trim()) return [];
    try { return [JSON.parse(line) as AuditRow]; } catch { return []; }
  });
}

function sentText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}

async function wait(description: string, predicate: () => boolean, timeoutMs = waitTimeoutMs): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (modelError !== undefined) throw new Error(`Local model fixture failed: ${modelError}`);
    if (predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error(`${description} timed out after ${timeoutMs} ms`);
}

async function exited(child: Bun.Subprocess, timeoutMs = 10_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([child.exited, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Thread creator benchmark CLI exit deadline exceeded")), timeoutMs);
    })]);
  } finally {
    clearTimeout(timer);
  }
}

function send(target: Target, text: string): string {
  const messageId = String(++nextMessageId);
  const mentionText = `@${fixture!.botName}`;
  fixture!.sendInbound(connection!, {
    fromUid: fixture!.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text: `${mentionText} ${text}`,
    mention: { entities: [{ uid: fixture!.botUid, offset: 0, length: mentionText.length }] },
    messageId,
  });
  return messageId;
}

function binding(target: Target): Binding | null {
  return db!.query(
    "SELECT session_id, cwd, session_file FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(fixture!.botUid, target.channelType, target.channelId) as Binding | null;
}

function taskForMessage(messageId: string): TaskRow | null {
  return db!.query(
    "SELECT id, session_id, state FROM tasks WHERE message_id = ?",
  ).get(messageId) as TaskRow | null;
}

function creatorTasks(): TaskRow[] {
  return db!.query(
    `SELECT id, session_id, state FROM tasks
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND instr(text, ?) > 0 ORDER BY accepted_at, rowid`,
  ).all(fixture!.botUid, parent.channelType, parent.channelId, creatorInputBody) as TaskRow[];
}

async function listRemoteThreads(group: string): Promise<FixtureManagementThread[]> {
  const response = await fetch(`${fixture!.apiUrl}/v1/bot/groups/${encodeURIComponent(group)}/threads`, {
    headers: { authorization: `Bearer ${fixture!.botToken}` },
  });
  assert.equal(response.status, 200, "Loopback fixture thread listing failed");
  const threads: unknown = await response.json();
  assert.ok(Array.isArray(threads), "Loopback fixture returned an invalid thread list");
  return threads as FixtureManagementThread[];
}

async function bindTarget(target: Target, cwd: string, label: string): Promise<Binding> {
  const messageId = send(target, `/new ${cwd}`);
  await wait(`${label} binding did not become durable`, () => binding(target)?.cwd === cwd
    && fixture!.sentMessages.some((message) => message.body.channel_id === target.channelId
      && sentText(message).includes(`cwd=${cwd}`)));
  assert.equal(taskForMessage(messageId), null, `${label} /new control command unexpectedly created a model task`);
  const result = binding(target);
  assert.ok(result, `${label} binding is missing`);
  return result;
}

async function launch(): Promise<void> {
  const configPath = join(root, "config.json");
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture!.apiUrl,
    botToken: fixture!.botToken,
    ownerId: fixture!.ownerUid,
    allowedGroupIds: [groupId],
    stateDir,
    sessionsDir: join(root, "sessions"),
    ompCommand: wrapperPath,
    ompExtraArgs: [
      "--model", "local-thread-creator/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", join(root, "native.yml"),
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });
  cli = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: join(root, "tmp"),
      PATH: `${join(root, "bin")}${process.platform === "win32" ? ";" : ":"}/usr/bin:/bin`,
      PI_CODING_AGENT_DIR: join(root, "agent"),
      PI_NO_TITLE: "1",
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = cli.stdout;
  const stderr = cli.stderr;
  if (!(stdout instanceof ReadableStream) || !(stderr instanceof ReadableStream)) {
    throw new Error("Channel CLI did not expose piped stdout and stderr streams");
  }
  cliOutput = Promise.all([new Response(stdout).text(), new Response(stderr).text()])
    .then((parts) => parts.join("\n"));
  connection = await fixture!.waitForConnection(1, 15_000);
  await wait("Channel CLI failed to authenticate with the loopback protocol fixture", () => connection!.ready, 15_000);
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
}

try {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must bind loopback only");
  await Promise.all([
    mkdir(join(root, "bin"), { recursive: true }),
    mkdir(join(root, "tmp"), { recursive: true }),
    mkdir(join(root, "agent"), { recursive: true }),
    mkdir(parentCwd, { recursive: true }),
    mkdir(childCwd, { recursive: true }),
    writeFile(auditPath, ""),
  ]);
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  wrapperPath = join(root, "bin", "real-omp.ts");
  const wrapperSource = `#!${process.execPath}
import { appendFileSync } from "node:fs";
const auditPath = ${JSON.stringify(auditPath)};
const command = ${JSON.stringify(realOmp)};
const args = process.argv.slice(2);
const child = Bun.spawn([command, ...args], { stdin: "pipe", stdout: "pipe", stderr: "inherit", env: process.env });
function append(row) { appendFileSync(auditPath, JSON.stringify({ at: Number(process.hrtime.bigint()) / 1_000_000, ...row }) + "\\n"); }
function wire(direction, line) {
  try {
    const frame = JSON.parse(line);
    const row = { kind: "wire", direction, pid: child.pid, wrapperPid: process.pid };
    for (const key of ["type", "op", "id", "toolName", "toolCallId"]) {
      if (typeof frame[key] === "string") row[key] = frame[key];
    }
    for (const key of ["arguments", "args"]) {
      if (frame[key] !== undefined) row[key] = frame[key];
    }
    if (typeof frame.isError === "boolean") row.isError = frame.isError;
    append(row);
  } catch {}
}
append({ kind: "spawn", pid: child.pid, wrapperPid: process.pid, command, args });
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => { try { child.kill(signal); } catch {} });
}
const reader = Bun.stdin.stream().getReader();
const inputDone = (async () => {
  const decoder = new TextDecoder();
  let pending = "";
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) break;
      pending += decoder.decode(result.value, { stream: true });
      let newline;
      while ((newline = pending.indexOf("\\n")) >= 0) {
        const line = pending.slice(0, newline);
        pending = pending.slice(newline + 1);
        if (line.trim()) wire("to-omp", line);
        try { child.stdin.write(line + "\\n"); } catch { return; }
      }
    }
    pending += decoder.decode();
    if (pending.trim()) {
      wire("to-omp", pending);
      try { child.stdin.write(pending); } catch {}
    }
  } finally {
    try { child.stdin.end(); } catch {}
    try { reader.releaseLock(); } catch {}
  }
})();
const outputDone = (async () => {
  const decoder = new TextDecoder();
  let pending = "";
  for await (const bytes of child.stdout) {
    process.stdout.write(bytes);
    pending += decoder.decode(bytes, { stream: true });
    let newline;
    while ((newline = pending.indexOf("\\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      if (line.trim()) wire("from-omp", line);
    }
  }
  pending += decoder.decode();
  if (pending.trim()) wire("from-omp", pending);
})();
const code = await child.exited;
try { await reader.cancel(); } catch {}
await Promise.all([inputDone, outputDone]);
append({ kind: "exit", pid: child.pid, wrapperPid: process.pid, exitCode: code });
process.exitCode = code ?? 1;
`;
  await writeFile(wrapperPath, wrapperSource, { mode: 0o755 });
  const modelServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: finiteModel });
  stopModel = () => { modelServer.stop(true); };
  if (modelServer.port === undefined) throw new Error("Local finite model did not expose a loopback port");
  const modelPort = modelServer.port;
  assert.equal(new URL(`http://127.0.0.1:${modelPort}/v1`).hostname, "127.0.0.1");
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: {
    "local-thread-creator": {
      baseUrl: `http://127.0.0.1:${modelPort}/v1`,
      apiKey: "local-only-placeholder",
      api: "openai-completions",
      models: [{
        id: "probe",
        name: "Local Thread Creator Acceptance",
        reasoning: false,
        input: ["text"],
        contextWindow: 32768,
        maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }],
    },
  } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");

  await launch();
  const parentBinding = await bindTarget(parent, parentCwd, "Parent");
  const createThreadMessageId = send(parent, childCreationMarker);
  await wait("Parent did not create its child through the real management tool", () =>
    threadCreationCallIssued && taskForMessage(createThreadMessageId)?.state === "completed");
  const createdThreads = (await listRemoteThreads(groupId)).filter((thread) => thread.name === sourceThreadName);
  assert.equal(createdThreads.length, 1, "Parent create-thread action must allocate exactly one child");
  assert.ok(fixture!.managementRequests.some((request) => request.method === "POST"
    && request.path === `/v1/bot/groups/${groupId}/threads`
    && request.body.name === sourceThreadName), "Child was not allocated by the real parent management action");
  const remoteThread = createdThreads[0]!;
  const child: Target = { channelType: 5, channelId: remoteThread.channel_id };
  sourceChannelId = child.channelId;
  const childBinding = await bindTarget(child, childCwd, "Child thread");
  assert.notEqual(parentBinding.session_id, childBinding.session_id, "Parent and child must use independent OMP sessions");
  const listen = db!.query(
    "SELECT creator_channel_type, creator_channel_id FROM thread_listens WHERE bot_uid = ? AND channel_id = ?",
  ).get(fixture!.botUid, child.channelId) as { creator_channel_type: number; creator_channel_id: string } | null;
  assert.deepEqual(listen, { creator_channel_type: 2, creator_channel_id: groupId },
    "Real create-thread management action did not persist its creator relation");

  const parentMessageId = send(parent, parentHoldMarker);
  await wait("Parent OMP did not enter its held model request", () => parentHoldCalls === 1
    && taskForMessage(parentMessageId)?.state === "started");
  assert.equal(parentGate.released, false, "Parent hold gate was unexpectedly released");

  const store = await openChannelStore(stateDir);
  try {
    store.createQuestion({
      id: questionId,
      taskId: "thread-creator-input-pending-ask",
      botUid: fixture!.botUid,
      channelType: parent.channelType,
      channelId: parent.channelId,
      payload: JSON.stringify({ questions: [{ question: "Should the parent proceed?" }] }),
    });
  } finally {
    store.close();
  }

  const childMessageId = send(child, childRequestMarker);
  await wait("Child OMP did not complete the creator-input tool call", () => toolCallIssued
    && taskForMessage(childMessageId)?.state === "completed"
    && childFollowupWithReceipt);
  await wait("Creator input was not durably accepted in the parent queue", () => creatorTasks().some((task) => task.state === "queued"));
  const queuedCreatorTasks = creatorTasks();
  assert.equal(queuedCreatorTasks.length, 1, "Child tool call must create exactly one independent parent task");
  const queuedCreatorTask = queuedCreatorTasks[0]!;
  assert.equal(queuedCreatorTask.state, "queued", "Creator input did not queue behind the active parent turn");
  assert.equal(queuedCreatorTask.session_id, parentBinding.session_id,
    "Creator input was not accepted into the parent's existing session");
  assert.equal(parentHoldCalls, 1, "Creator input altered or restarted the active parent model turn");
  assert.equal(creatorPromptCalls, 0, "Creator input was dispatched before the active parent turn settled");
  const pendingQuestion = db!.query("SELECT state, answer FROM questions WHERE id = ?").get(questionId) as {
    state: string;
    answer: string | null;
  } | null;
  assert.deepEqual(pendingQuestion, { state: "pending", answer: null },
    "Creator input answered or consumed the parent's pending ask");

  parentGate.resolve();
  await wait("Parent did not execute the queued creator input", () => creatorPromptCalls === 1
    && queuedCreatorTasks.length === 1
    && creatorTasks()[0]?.state === "completed");
  const completedCreatorTask = creatorTasks()[0]!;
  assert.equal(completedCreatorTask.id, queuedCreatorTask.id, "Creator input ran as a different task than the queued acceptance");
  assert.equal(completedCreatorTask.session_id, parentBinding.session_id);
  assert.equal(taskForMessage(parentMessageId)?.state, "completed", "The held parent turn did not complete first");
  assert.deepEqual(
    db!.query("SELECT state, answer FROM questions WHERE id = ?").get(questionId),
    { state: "pending", answer: null },
    "Queued creator input changed the pending ask while executing",
  );
  assert.equal(modelRequests.filter((request) => request.serialized.includes(parentHoldMarker)
    && !request.serialized.includes(creatorInputBody)).length, 1,
    "Held parent turn must issue exactly one model request before the queued creator input");
  assert.equal(modelRequests.filter((request) => request.serialized.includes(creatorInputBody)).length, 2,
    "Expected exactly one child tool-result continuation and one parent creator-input prompt");
  const parentPrompt = modelRequests.find((request) => request.serialized.includes(creatorInputBody)
    && !request.serialized.includes(childRequestMarker));
  assert.ok(parentPrompt, "No actual parent provider request carried the creator-input body");
  assertCreatorPrompt(parentPrompt.serialized, child.channelId);
  assert.equal(fixture!.sentMessages.some((message) => sentText(message).includes(creatorInputBody)), false,
    "Creator input was echoed as an ordinary Octo chat message instead of being queued internally");

  const audit = auditRows(auditPath);
  const calls = audit.filter((row) => row.kind === "wire" && row.direction === "from-omp"
    && row.type === "host_tool_call" && row.toolName === "octo_management");
  assert.equal(calls.length, 2, "Expected one parent create-thread and one child creator-input tool call");
  const createCall = calls.find((row) =>
    (row.arguments as Record<string, unknown> | undefined)?.action === "create-thread");
  const creatorCall = calls.find((row) =>
    (row.arguments as Record<string, unknown> | undefined)?.action === childAction);
  assert.ok(createCall, "Parent did not use the native create-thread tool call");
  assert.ok(creatorCall, "Child did not use the native thread-send-parent tool call");
  assert.ok(createCall.toolCallId, "Native create-thread call omitted its toolCallId identity");
  assert.ok(creatorCall.toolCallId, "Native creator-input call omitted its toolCallId identity");
  assert.equal((createCall.arguments as Record<string, unknown>).name, sourceThreadName);
  assert.equal((creatorCall.arguments as Record<string, unknown>).text, creatorInputBody,
    "Native OMP host call changed the explicit creator-input body");
  for (const call of [createCall, creatorCall]) {
    const toolEvents = audit.filter((row) => row.kind === "wire" && row.direction === "from-omp"
      && (row.type === "tool_execution_start" || row.type === "tool_execution_end")
      && row.toolName === "octo_management" && row.toolCallId === call.toolCallId);
    assert.ok(toolEvents.some((row) => row.type === "tool_execution_start"), "Native management tool execution did not start");
    assert.ok(toolEvents.some((row) => row.type === "tool_execution_end"), "Native management tool execution did not finish");
    assert.notEqual(toolEvents.find((row) => row.type === "tool_execution_end")?.isError, true,
      "Native management tool execution returned an error");
  }

  const workerCount = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
  assert.ok(workerCount.count > 0, "Acceptance did not exercise real OMP RPC worker processes");
  assert.equal(modelRequests.length, 6, "Finite model must handle create-thread, held parent, child tool, and queued parent turns");
  assert.equal(modelError, undefined);
  assert.equal(cli!.exitCode, null, "Channel CLI exited before its graceful shutdown");
  cli!.kill("SIGTERM");
  assert.equal(await exited(cli!), 0, "Channel CLI did not stop cleanly on SIGTERM");
  await wait("Thread creator benchmark left a native OMP worker running", () => {
    const active = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return active.count === 0;
  });
  const spawnRows = auditRows(auditPath).filter((row) => row.kind === "spawn");
  assert.ok(spawnRows.length >= 2, "Expected real OMP workers for both parent and child sessions");
  const finalAudit = auditRows(auditPath);
  for (const spawn of spawnRows) {
    assert.ok(finalAudit.some((row) => row.kind === "exit" && row.pid === spawn.pid),
      `Real OMP process ${spawn.pid} has no confirmed exit record`);
    if (typeof spawn.pid === "number") assert.equal(alive(spawn.pid), false, `Real OMP process ${spawn.pid} remained alive`);
    if (typeof spawn.wrapperPid === "number") assert.equal(alive(spawn.wrapperPid), false, `OMP audit relay ${spawn.wrapperPid} remained alive`);
  }
  const logs = await cliOutput!;
  assert.ok(!logs.includes(fixture!.botToken), "Channel CLI leaked the loopback fixture token");
  succeeded = true;
  console.log("METRIC creator_input_native_tool_calls=1");
  console.log("METRIC creator_input_parent_tasks=1");
  console.log("METRIC creator_input_parent_queue_before_release=1");
  console.log("METRIC parent_thread_creation_native_tool_calls=1");
  console.log("METRIC creator_input_source_metadata=1");
  console.log("METRIC creator_input_pending_ask_answers=0");
  console.log(`METRIC creator_input_local_model_requests=${modelRequests.length}`);
} catch (error) {
  console.error(JSON.stringify({
    modelRequests: modelRequests.map(({ index }) => ({ index })),
    modelError: modelError ?? null,
    sentMessages: fixture?.sentMessages.slice(-12).map((message) => ({
      channelId: message.body.channel_id,
      textLength: sentText(message).length,
    })),
    ompAudit: auditRows(auditPath).slice(-30).map(({ kind, direction, type, pid, wrapperPid, toolName, toolCallId, exitCode }) => ({
      kind, direction, type, pid, wrapperPid, toolName, toolCallId, exitCode,
    })),
    tasks: db?.query("SELECT channel_type, channel_id, message_id, state, error_text FROM tasks ORDER BY accepted_at").all(),
  }));
  throw error;
} finally {
  for (const held of gates) held.resolve();
  if (cli?.exitCode === null) {
    cli.kill("SIGTERM");
    try { await exited(cli, 10_000); }
    catch { cli.kill("SIGKILL"); await cli.exited; }
  }
  if (cli) await cli.exited;
  db?.close();
  if (fixture) await fixture.close();
  stopModel?.();
  const ownedPids = new Set<number>();
  for (const row of auditRows(auditPath).filter((entry) => entry.kind === "spawn")) {
    if (typeof row.pid === "number") ownedPids.add(row.pid);
    if (typeof row.wrapperPid === "number") ownedPids.add(row.wrapperPid);
  }
  for (const pid of ownedPids) {
    if (!alive(pid)) continue;
    const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "command="], { stdout: "pipe" });
    const command = await new Response(ps.stdout).text();
    await ps.exited;
    if (!command.includes(root) || (!command.includes(realOmp) && !command.includes(wrapperPath))) continue;
    process.kill(pid, "SIGKILL");
    for (let attempt = 0; attempt < 40 && alive(pid); attempt += 1) await Bun.sleep(25);
    assert.equal(alive(pid), false, `Owned OMP or audit relay process ${pid} survived emergency cleanup`);
  }
  const logs = cliOutput === undefined ? "" : await cliOutput;
  if (!succeeded) console.error(logs.replaceAll(fixture?.botToken ?? "\u0000", "[fixture-token]").slice(-12_000));
  await rm(root, { recursive: true, force: true });
}
