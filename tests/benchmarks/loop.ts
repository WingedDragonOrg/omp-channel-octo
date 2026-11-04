import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelStore } from "../../src/store/index.ts";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection, type FixtureSentMessage } from "../fixtures/octo-protocol-server.ts";

// Opt-in acceptance: installed OMP + real channel CLI + localhost model, Loop REST and encrypted IM.
// All credentials, profiles, sessions and remote resources belong to disposable local fixtures.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND");
const omp = resolve(requestedOmp);
if (process.env.TZ !== "UTC") {
  const child = Bun.spawn([process.execPath, import.meta.path], {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", TZ: "UTC", OMP_REAL_COMMAND: omp },
    stdin: "ignore", stdout: "inherit", stderr: "inherit",
  });
  process.exit(await child.exited);
}
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-loop-acceptance-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const workspaceId = "11111111-1111-4111-8111-111111111111";
const loopTaskId = "22222222-2222-4222-8222-222222222222";
const otherTaskId = "33333333-3333-4333-8333-333333333333";
const unassignedTaskId = "44444444-4444-4444-8444-444444444444";
const ownerFleetUserId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const otherFleetUserId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const otherOctoUid = "loop-other-member-octo-uid";
const unassignedTitle = "LOOP_SMOKE_UNASSIGNED";
const groupId = "loop-smoke-group";
const stateDir = join(root, "state");
const children: Bun.Subprocess[] = [];
const outputs: Promise<string>[] = [];
const reads: { path: string; workspace: string | null; method: string }[] = [];
const modelCalls: { kind: string; call: number; tools: string[] }[] = [];
const mainPrompts: string[] = [];
const perKind = new Map<string, number>();
let connection: FixtureConnection;
let store: ChannelStore | undefined;
let db: Database | undefined;
let modelError: string | undefined;
let sequence = 40_000;
let phase: "initial" | "changed" = "initial";
let succeeded = false;
const task = {
  task_id: loopTaskId, workspace_id: workspaceId, title: "LOOP_SMOKE_EXISTING", description: "Read-only local acceptance task",
  status: "todo", priority: "medium", assignee_type: "member" as string | null,
  assignee_id: ownerFleetUserId as string | null, assignee_octo_uid: fixture.ownerUid as string | null,
  project_id: null, parent_task_id: null, start_date: null, due_date: null,
  created_at: "2026-11-01T00:00:00Z", updated_at: "2026-11-01T00:00:00Z",
};
const otherTask = {
  task_id: otherTaskId, workspace_id: workspaceId, title: "LOOP_SMOKE_OTHER", description: "LOOP_SMOKE_OTHER_DESCRIPTION",
  status: "todo", priority: "medium", assignee_type: "member" as string | null,
  assignee_id: otherFleetUserId as string | null, assignee_octo_uid: otherOctoUid as string | null,
  project_id: null, parent_task_id: null, start_date: null, due_date: null,
  created_at: "2026-11-01T00:00:00Z", updated_at: "2026-11-01T00:00:00Z",
};
const unassignedTask = {
  task_id: unassignedTaskId, workspace_id: workspaceId, title: unassignedTitle, description: "LOOP_SMOKE_UNASSIGNED_DESCRIPTION",
  status: "todo", priority: "medium", assignee_type: null as string | null,
  assignee_id: null as string | null, assignee_octo_uid: null as string | null,
  project_id: null, parent_task_id: null, start_date: null, due_date: null,
  created_at: "2026-11-01T00:00:00Z", updated_at: "2026-11-01T00:00:00Z",
};
const loopTasks = [task, otherTask, unassignedTask];
const irrelevantTaskIds = [otherTaskId, unassignedTaskId];
const irrelevantTaskInputs = [
  otherTaskId, unassignedTaskId, "LOOP_SMOKE_OTHER", "LOOP_SMOKE_OTHER_DESCRIPTION",
  unassignedTitle, "LOOP_SMOKE_UNASSIGNED_DESCRIPTION",
];
const workspace = { workspace_id: workspaceId, name: "Loop acceptance", slug: "loop-acceptance" };

function completion(text: string, tool?: Record<string, unknown>): Response {
  const delta = tool ? { role: "assistant", tool_calls: [{ index: 0, ...tool }] } : { role: "assistant", content: text };
  const chunks = [
    { id: `loop-${modelCalls.length}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    { id: `loop-${modelCalls.length}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: tool ? "tool_calls" : "stop" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } });
}
function management(callId: string, args: Record<string, unknown>): Response {
  return completion("", { id: callId, type: "function", function: { name: "octo_management", arguments: JSON.stringify(args) } });
}
const model = Bun.serve({
  hostname: "127.0.0.1", port: 0,
  async fetch(request) {
    const body = await request.json() as { messages?: { role: string; content: unknown }[]; tools?: { function?: { name?: string } }[] };
    const messages = body.messages ?? [];
    const messagesJson = JSON.stringify(messages);
    const lastUser = [...messages].reverse().find(message =>
      message.role === "user" && /LOOP_SMOKE_(?:EXISTING|UPDATED|RESTARTED|CHILD_INITIAL)/.test(JSON.stringify(message.content)));
    const text = JSON.stringify(lastUser?.content ?? "");
    const kind = text.includes("LOOP_SMOKE_CHILD_INITIAL") ? "child-initial"
      : phase === "changed" ? "child-change" : "main-baseline";
    const leakedIrrelevantInputs = irrelevantTaskInputs.filter(token => messagesJson.includes(token));
    if (kind === "main-baseline") {
      mainPrompts.push(messagesJson);
      if (leakedIrrelevantInputs.length > 0) {
        modelError = `Unrelated task input leaked into main prompt: ${leakedIrrelevantInputs.join(",")}`;
        return new Response(modelError, { status: 400 });
      }
    }
    if (leakedIrrelevantInputs.length > 0) {
      modelError = `Unrelated task input leaked into agent prompt: ${leakedIrrelevantInputs.join(",")}`;
      return new Response(modelError, { status: 400 });
    }
    const call = (perKind.get(kind) ?? 0) + 1;
    perKind.set(kind, call);
    const tools = (body.tools ?? []).flatMap(tool => tool.function?.name ? [tool.function.name] : []);
    modelCalls.push({ kind, call, tools });
    if (kind === "main-baseline" && call === 1) {
      if (!tools.includes("octo_management")) {
        modelError = "Installed OMP did not receive scoped management host tools";
        return new Response(modelError, { status: 400 });
      }
      return management("loop-create-stable", { action: "create-thread", name: "Loop acceptance task", task_id: loopTaskId, listen: "merge" });
    }
    if (kind === "main-baseline" && call === 2) {
      const route = store!.getLoopTaskRoute({ botUid: fixture.botUid, groupId, workspaceId, taskId: loopTaskId });
      if (!route) {
        modelError = "Create host tool returned without durable task association";
        return new Response(modelError, { status: 400 });
      }
      return management("loop-dispatch-stable", { action: "thread-dispatch", short_id: route.threadChannelId.split("____")[1], task_id: loopTaskId, text: "LOOP_SMOKE_CHILD_INITIAL" });
    }
    if (kind === "main-baseline" && call === 3) return completion("LOOP_SMOKE_MAIN_BOUND");
    if (kind === "child-initial" && call === 1) return completion("LOOP_SMOKE_CHILD_DONE");
    if (kind === "child-change" && call === 1 && text.includes("LOOP_SMOKE_UPDATED")) return completion("LOOP_SMOKE_UPDATE_DONE");
    if (kind === "child-change" && call === 2 && text.includes("LOOP_SMOKE_RESTARTED")) return completion("LOOP_SMOKE_RESTART_DONE");
    modelError = `Unexpected finite model request: ${kind}/${call}: ${text.slice(-400)}`;
    return new Response(modelError, { status: 400 });
  },
});
const gateway = Bun.serve({
  hostname: "127.0.0.1", port: 0,
  async fetch(request) {
    const url = new URL(request.url);
    if (!url.pathname.startsWith("/fleet/api/")) {
      return fetch(new Request(new URL(url.pathname + url.search, fixture.apiUrl), request));
    }
    reads.push({ path: url.pathname + url.search, workspace: request.headers.get("X-Workspace-ID"), method: request.method });
    assert.equal(request.method, "GET", "Awareness must not write Loop state");
    assert.equal(request.headers.get("Authorization"), `Bearer ${fixture.botToken}`);
    assert.equal(request.headers.get("X-Workspace-ID"), workspaceId);
    if (url.pathname === `/fleet/api/v1/workspaces/${workspaceId}`) return Response.json({ data: workspace });
    if (url.pathname === "/fleet/api/v1/tasks") {
      const page = Number(url.searchParams.get("page") ?? 1);
      const pageSize = Number(url.searchParams.get("page_size") ?? 20);
      const start = (page - 1) * pageSize;
      return Response.json({ data: loopTasks.slice(start, start + pageSize), pagination: { page, page_size: pageSize, total: loopTasks.length } });
    }
    const detailTask = loopTasks.find(candidate => url.pathname === `/fleet/api/v1/tasks/${candidate.task_id}`);
    if (detailTask) return Response.json({ data: detailTask });
    throw new Error(`Unexpected Loop discovery request ${url.pathname}`);
  },
});
function messageText(message: FixtureSentMessage): string {
  const payload = message.body.payload;
  if (payload && typeof payload === "object" && "content" in payload && typeof payload.content === "string") return payload.content;
  return "";
}
function sentText(): string[] {
  return fixture.sentMessages.map(messageText);
}
async function wait(label: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined);
    return predicate() ? true : undefined;
  }, timeoutMs, label);
}
function send(text: string): void {
  fixture.sendInbound(connection, {
    fromUid: fixture.ownerUid, channelId: groupId, channelType: 2, text,
    messageId: String(++sequence), mention: { uids: [fixture.botUid] },
  });
}
async function launch(): Promise<Bun.Subprocess> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", join(root, "config.json")], {
    cwd: root,
    env: { HOME: root, TMPDIR: root, PATH: `${join(root, "bin")}:/usr/bin:/bin`, PI_CODING_AGENT_DIR: join(root, "agent"), PI_NO_TITLE: "1", LANG: "C", TZ: "UTC" },
    stdin: "ignore", stdout: "pipe", stderr: "pipe",
  });
  children.push(child);
  outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(children.length, 15_000);
  await wait("Local CLI failed to authenticate", () => connection.ready);
  await wait("CLI omitted startup notice", () => sentText().some(text => text.includes("omp-channel-octo 已启动") && text.includes(`PID=${child.pid}`)));
  return child;
}
async function stop(child: Bun.Subprocess): Promise<void> {
  child.kill("SIGTERM");
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("CLI shutdown timed out")), 15_000);
  try {
    const code = await Promise.race([child.exited, timeout.promise]);
    assert.equal(code, 0);
  } finally { clearTimeout(timer); }
}
function taskCount(channelType: number): number {
  const row = db!.query("SELECT COUNT(*) AS count FROM tasks WHERE channel_type = ?").get(channelType) as { count: number };
  return row.count;
}
function taskSnapshot(taskId: string): { updated_at: number; task_json: string } | null {
  return db!.query<{ updated_at: number; task_json: string }, [string, string, string, string]>(
    "SELECT updated_at, task_json FROM loop_task_snapshots WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?",
  ).get(fixture.botUid, groupId, workspaceId, taskId) ?? null;
}
function threadCreateCount(): number {
  return fixture.managementRequests.filter(request => request.method === "POST" && /\/threads$/.test(request.path)).length;
}
async function settled(): Promise<void> {
  await wait("Worker or queued task remained after completion", () => {
    const active = db!.query("SELECT COUNT(*) AS count FROM tasks WHERE state IN ('started', 'queued')").get() as { count: number };
    const live = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return active.count === 0 && live.count === 0;
  });
}
try {
  await mkdir(join(root, "bin"));
  await mkdir(join(root, "agent"));
  await mkdir(join(root, "cwd"));
  for (const command of ["octo-cli", "super"]) await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: { "local-loop": {
    baseUrl: `http://127.0.0.1:${model.port}/v1`, apiKey: "local-placeholder", api: "openai-completions",
    models: [{ id: "probe", name: "Probe", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
  await writeFile(join(root, "config.json"), JSON.stringify({
    apiUrl: `http://127.0.0.1:${gateway.port}`, wsUrl: fixture.wsUrl, botToken: fixture.botToken, ownerId: fixture.ownerUid,
    allowedGroupIds: [groupId], stateDir, sessionsDir: join(root, "sessions"), ompCommand: omp,
    ompExtraArgs: ["--model", "local-loop/probe", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools", "--config", join(root, "native.yml")],
    startupTimeoutMs: 30_000, cancelTimeoutMs: 3_000, groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });
  let child = await launch();
  store = await ChannelStore.open(join(stateDir, "channel.sqlite"));
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  send(`/new ${join(root, "cwd")}`);
  await wait("Real OMP session was not created", () => sentText().some(text => text.includes("已创建会话")));
  send(`/bind loop ${workspaceId}`);
  await wait("Baseline did not create, bind and dispatch via native OMP tools", () => sentText().includes("LOOP_SMOKE_MAIN_BOUND") && sentText().includes("LOOP_SMOKE_CHILD_DONE"));
  await settled();
  assert.notEqual(ownerFleetUserId, fixture.ownerUid, "Fleet member UUID must stay distinct from the Octo UID");
  assert.equal(task.assignee_type, "member");
  assert.equal(task.assignee_id, ownerFleetUserId);
  assert.equal(task.assignee_octo_uid, fixture.ownerUid);
  const route = store.getLoopTaskRoute({ botUid: fixture.botUid, groupId, workspaceId, taskId: loopTaskId });
  assert.ok(route);
  assert.ok(taskSnapshot(loopTaskId), "Initial owner-assigned task was not snapshotted");
  const otherSnapshotAtBaseline = taskSnapshot(otherTaskId);
  const unassignedSnapshotAtBaseline = taskSnapshot(unassignedTaskId);
  assert.ok(otherSnapshotAtBaseline, "Other-member task was omitted from the workspace snapshot");
  assert.ok(unassignedSnapshotAtBaseline, "Unassigned task was omitted from the workspace snapshot");
  assert.equal(mainPrompts.length, 3, "Baseline should require exactly the native create, dispatch, and completion prompts");
  assert.ok(mainPrompts.every(prompt => irrelevantTaskIds.every(id => !prompt.includes(id))), "Main baseline prompt contained an unrelated task ID");
  assert.ok(mainPrompts.every(prompt => irrelevantTaskInputs.every(input => !prompt.includes(input))), "Main baseline prompt contained unrelated task data");
  const mainTasks = taskCount(2);
  const childTasks = taskCount(5);
  const callsAfterInitial = modelCalls.length;
  const threadCreatesAfterInitial = threadCreateCount();
  assert.equal(threadCreatesAfterInitial, 1, "Native baseline created more than one child thread");
  const receiptAt = fixture.sentMessages.length;
  send(`/bind loop ${workspaceId}`);
  await wait("Idempotent rebind had no receipt", () => fixture.sentMessages.slice(receiptAt).some(message => messageText(message).includes(workspaceId)));
  assert.equal(modelCalls.length, callsAfterInitial, "Same workspace bind repeated initial agent work");
  const messagesAfterRebind = fixture.sentMessages.length;
  otherTask.title = "LOOP_SMOKE_OTHER_UPDATED";
  otherTask.updated_at = "2026-11-04T00:00:00Z";
  unassignedTask.title = "LOOP_SMOKE_UNASSIGNED_UPDATED";
  unassignedTask.updated_at = "2026-11-04T00:00:00Z";
  await wait("Real periodic scan did not advance ignored-task snapshots", () => {
    const otherSnapshot = taskSnapshot(otherTaskId);
    const unassignedSnapshot = taskSnapshot(unassignedTaskId);
    return otherSnapshot !== null && unassignedSnapshot !== null
      && JSON.parse(otherSnapshot.task_json).title === "LOOP_SMOKE_OTHER_UPDATED"
      && JSON.parse(unassignedSnapshot.task_json).title === "LOOP_SMOKE_UNASSIGNED_UPDATED";
  }, 90_000);
  await settled();
  assert.equal(modelCalls.length, callsAfterInitial, "Changes to unassigned or other-member tasks started or steered an agent");
  assert.equal(taskCount(2), mainTasks);
  assert.equal(taskCount(5), childTasks);
  assert.equal(threadCreateCount(), threadCreatesAfterInitial);
  assert.equal(fixture.sentMessages.length, messagesAfterRebind, "Ignored task changes produced a user-visible agent side effect");
  phase = "changed";
  task.title = "LOOP_SMOKE_UPDATED";
  task.assignee_type = "member";
  task.assignee_id = otherFleetUserId;
  task.assignee_octo_uid = otherOctoUid;
  task.updated_at = "2026-11-04T00:00:00Z";
  await wait("Periodic Loop scan did not reach associated thread without a command or restart", () => sentText().includes("LOOP_SMOKE_UPDATE_DONE"), 90_000);
  await settled();
  assert.equal(modelCalls.length, callsAfterInitial + 1, "Reassigned associated task was not delivered exactly once");
  assert.deepEqual(modelCalls.slice(callsAfterInitial).map(call => call.kind), ["child-change"]);
  assert.equal(taskCount(2), mainTasks, "An associated task update woke the main group");
  assert.equal(taskCount(5), childTasks + 1, "Expected one independent thread update task");
  assert.deepEqual(store.getLoopTaskRoute({ botUid: fixture.botUid, groupId, workspaceId, taskId: loopTaskId }), route);
  const updateDelivery = fixture.sentMessages.find(message => messageText(message) === "LOOP_SMOKE_UPDATE_DONE");
  assert.equal(updateDelivery?.body.channel_id, route.threadChannelId);
  assert.equal(threadCreateCount(), threadCreatesAfterInitial, "A task update allocated another thread");
  await stop(child);
  task.title = "LOOP_SMOKE_RESTARTED";
  task.updated_at = "2026-11-05T00:00:00Z";
  child = await launch();
  await wait("Restored Loop scan did not reach associated thread", () => sentText().includes("LOOP_SMOKE_RESTART_DONE"));
  await settled();
  assert.equal(taskCount(2), mainTasks, "Restored associated update woke the main group");
  assert.equal(taskCount(5), childTasks + 2, "Restored scan did not produce exactly one new child task");
  assert.equal(threadCreateCount(), threadCreatesAfterInitial);
  const updates = taskCount(5);
  const snapshotBeforeRestart = taskSnapshot(loopTaskId)!;
  await stop(child);
  child = await launch();
  await wait("Restart never committed its unchanged workspace scan", () => {
    const snapshot = taskSnapshot(loopTaskId);
    return snapshot !== null && snapshot.updated_at > snapshotBeforeRestart.updated_at;
  });
  await wait("Restart left undelivered Loop input", () => store!.listLoopInputs(fixture.botUid).length === 0);
  await settled();
  assert.equal(taskCount(2), mainTasks);
  assert.equal(taskCount(5), updates, "Unchanged restart replayed an accepted task update");
  assert.equal(modelCalls.length, callsAfterInitial + 2);
  assert.equal(reads.filter(read => /^\/fleet\/api\/v1\/workspaces(?:\?|$)/.test(read.path)).length, 0, "UUID bind or polling enumerated workspaces");
  const taskListReads = reads.filter(read => read.path.startsWith("/fleet/api/v1/tasks?"));
  assert.ok(taskListReads.length >= 4);
  assert.ok(taskListReads.every(read => {
    const url = new URL(read.path, "http://fixture");
    return ["assignee_type", "assignee_id", "assignee_octo_uid"].every(key => !url.searchParams.has(key));
  }), "Workspace task scans were filtered by assignee and could omit already-bound work");
  assert.ok(mainPrompts.every(prompt => irrelevantTaskIds.every(id => !prompt.includes(id))));
  assert.ok(mainPrompts.every(prompt => irrelevantTaskInputs.every(input => !prompt.includes(input))));
  await stop(child);
  const logs = (await Promise.all(outputs)).join("\n");
  assert.ok(!logs.includes(fixture.botToken), "Credential leaked to CLI output");
  console.log(JSON.stringify({ modelRequests: modelCalls.length, loopReads: reads.length, threadCreates: threadCreateCount(), mainTasks, childTasks: updates, ignoredTasks: 2, ignoredTaskSideEffects: 0, periodicTaskUpdates: 1, restarts: 2, duplicateInputs: 0, parentUpdateInputs: 0 }));
  succeeded = true;
} catch (error) {
  console.error(JSON.stringify({ root, modelError, modelCalls, reads }, null, 2));
  throw error;
} finally {
  for (const child of children) if (child.exitCode === null) child.kill("SIGTERM");
  await Promise.allSettled(children.map(child => child.exited));
  if (!succeeded) for (const output of await Promise.all(outputs)) console.error(output);
  db?.close();
  store?.close();
  gateway.stop(true);
  model.stop(true);
  await fixture.close();
  if (succeeded) await rm(root, { recursive: true, force: true });
}
