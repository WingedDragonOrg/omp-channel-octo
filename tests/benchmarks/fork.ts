import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import { openChannelStore } from "../../src/store/index.ts";
import {
  createOctoProtocolFixture,
  type FixtureConnection,
  type FixtureManagementThread,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Offline end-to-end acceptance: installed OMP, the real channel CLI, an SSE model on loopback,
// and the encrypted Octo protocol fixture. No real provider, bot, or daemon connection is used.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
// OMP/Bun process identities and ps timestamps must use the same timezone from process startup.
if (process.env.TZ !== "UTC") {
  const child = Bun.spawn([process.execPath, import.meta.path], {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", TZ: "UTC", OMP_REAL_COMMAND: realOmp },
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
  });
  process.exitCode = await child.exited;
  process.exit();
}

const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-fork-benchmark-")));
const stateDir = join(root, "state");
const repo = join(root, "shared-project");
const auditPath = join(root, "omp-audit.jsonl");
const gitAuditPath = join(root, "git-audit.log");
const cliChildren: Bun.Subprocess[] = [];
const cliOutputs: Promise<string>[] = [];
const modelRequests: ModelRequest[] = [];
const scripts = new Map<string, ModelStep[]>();
const modelCallCounts = new Map<string, number>();
const gates: Gate[] = [];
const metric = { commandGroup: 0, commandThread: 0, toolGroup: 0, toolThread: 0, tan: 0, archiveMerge: 0, archiveRemove: 0 };
let fixture: OctoProtocolFixture | undefined;
let modelPort = 0;
let stopModel: (() => void) | undefined;
let connection: FixtureConnection | undefined;
let db: Database | undefined;
let modelError: string | undefined;
let gitPath = "";
let messageSequence = 10_000;
let succeeded = false;

interface Gate {
  promise: Promise<void>;
  released: boolean;
  resolve(): void;
}
function monotonicNow(): number {
  return Number(process.hrtime.bigint()) / 1_000_000;
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
function assertExited(pid: number | undefined, description: string): asserts pid is number {
  assert.ok(Number.isSafeInteger(pid), `${description} omitted a valid PID`);
  try {
    process.kill(pid!, 0);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
    throw error;
  }
  throw new Error(description);
}

type ModelStep =
  | { kind: "text"; text: string; hold?: Gate }
  | { kind: "fork-tool"; name: string; toolCallId: string };
interface ModelRequest {
  label: string;
  call: number;
  at: number;
  tools: string[];
  body: Record<string, unknown>;
}
type AuditRow = {
  kind: string;
  at: number;
  pid?: number;
  wrapperPid?: number;
  direction?: "to-omp" | "from-omp";
  type?: string;
  op?: string;
  id?: string;
  toolName?: string;
  toolCallId?: string;
  args?: string[];
  command?: string;
  exitCode?: number;
};
type Target = { channelType: 2 | 5; channelId: string };
type Binding = { session_id: string; cwd: string; session_file: string | null };
type Case = {
  id: string;
  kind: "command" | "tool";
  sourceKind: "group" | "thread";
  archive?: "merge" | "remove";
};

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

function completion(step: ModelStep, index: number): Response {
  const delta = step.kind === "fork-tool"
    ? {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: step.toolCallId,
          type: "function",
          function: { name: "fork", arguments: JSON.stringify({ name: step.name }) },
        }],
      }
    : { role: "assistant", content: step.text };
  const id = `fork-smoke-${index}`;
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
      choices: [{ index: 0, delta: {}, finish_reason: step.kind === "fork-tool" ? "tool_calls" : "stop" }],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function requestLabel(body: Record<string, unknown>): string | undefined {
  const serialized = JSON.stringify(body.messages ?? body);
  return [...serialized.matchAll(/FORK_SMOKE_TASK:([A-Za-z0-9_-]+)/gu)].at(-1)?.[1];
}

function modelTools(body: Record<string, unknown>): string[] {
  if (!Array.isArray(body.tools)) return [];
  return body.tools.flatMap(entry => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return [];
    const tool = entry as Record<string, unknown>;
    if (typeof tool.name === "string") return [tool.name];
    if (!tool.function || typeof tool.function !== "object" || Array.isArray(tool.function)) return [];
    const name = (tool.function as Record<string, unknown>).name;
    return typeof name === "string" ? [name] : [];
  });
}

function recordModelError(message: string): Response {
  modelError ??= message;
  return new Response(message, { status: 400 });
}

function sentText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}

function sentFor(target: Target): string[] {
  return fixture!.sentMessages
    .filter(message => message.body.channel_id === target.channelId)
    .map(sentText);
}

function send(target: Target, text: string): string {
  const messageId = String(++messageSequence);
  const mention = { entities: [{ uid: fixture!.botUid, offset: 0, length: `@${fixture!.botName}`.length }] };
  fixture!.sendInbound(connection!, {
    fromUid: fixture!.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text: `@${fixture!.botName} ${text}`,
    mention,
    messageId,
  });
  return messageId;
}

function binding(target: Target): Binding | null {
  const row = db!.query(
    "SELECT session_id, cwd, session_file FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(fixture!.botUid, target.channelType, target.channelId) as Binding | null;
  return row;
}

function task(messageId: string): { state: string; error_text: string | null } | null {
  return db!.query("SELECT state, error_text FROM tasks WHERE message_id = ?").get(messageId) as {
    state: string;
    error_text: string | null;
  } | null;
}

function inbound(messageId: string): { kind: string; command_state: string } | null {
  return db!.query("SELECT kind, command_state FROM inbound WHERE message_id = ?").get(messageId) as {
    kind: string;
    command_state: string;
  } | null;
}

function rows(path: string): AuditRow[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split("\n").flatMap(line => {
    if (!line.trim()) return [];
    try { return [JSON.parse(line) as AuditRow]; } catch { return []; }
  });
}

async function wait(description: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
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
      timer = setTimeout(() => reject(new Error("Channel CLI did not exit")), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

async function run(argv: string[], cwd?: string): Promise<string> {
  const child = Bun.spawn(argv, { ...(cwd === undefined ? {} : { cwd }), stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  assert.equal(code, 0, `${argv.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function digest(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

async function sqliteFork(target: Target) {
  const store = await openChannelStore(stateDir);
  try {
    return store.getThreadForkByTarget(fixture!.botUid, target);
  } finally {
    store.close();
  }
}

async function threadRequest(groupId: string, method: "GET" | "POST", body?: Record<string, unknown>): Promise<FixtureManagementThread[]> {
  const response = await fetch(`${fixture!.apiUrl}/v1/bot/groups/${encodeURIComponent(groupId)}/threads`, {
    method,
    headers: {
      authorization: `Bearer ${fixture!.botToken}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  assert.equal(response.status, 200, `Local fixture thread ${method} failed`);
  const value: unknown = await response.json();
  return Array.isArray(value) ? value as FixtureManagementThread[] : [value as FixtureManagementThread];
}

async function sourceTargetFor(testCase: Case): Promise<Target> {
  const groupId = `fork-smoke-${testCase.id}`;
  if (testCase.sourceKind === "group") return { channelType: 2, channelId: groupId };
  const created = await threadRequest(groupId, "POST", { name: `source-${testCase.id}` });
  const thread = created[0];
  assert.ok(thread?.channel_id, `Fixture failed to create source thread for ${testCase.id}`);
  return { channelType: 5, channelId: thread.channel_id };
}

function sourceGroup(target: Target): string {
  return target.channelType === 2 ? target.channelId : target.channelId.split("____", 1)[0]!;
}

async function sourceBinding(target: Target, repoPath: string): Promise<Binding> {
  const at = fixture!.sentMessages.length;
  const id = send(target, `/new ${repoPath}`);
  await wait(`/new did not bind ${target.channelId}`, () => {
    const row = binding(target);
    return row !== null
      && row.cwd === repoPath
      && sentFor(target).slice(0).length > 0
      && fixture!.sentMessages.slice(at).some(message => message.body.channel_id === target.channelId && sentText(message).includes(`cwd=${repoPath}`));
  });
  assert.equal(task(id), null, "/new must remain a local control command");
  const row = binding(target);
  assert.ok(row, "Source session was not bound");
  assert.equal(row.cwd, repoPath);
  return row;
}

async function waitTask(messageId: string, expected: "started" | "completed"): Promise<void> {
  await wait(`Task ${messageId} did not become ${expected}`, () => {
    const row = task(messageId);
    if (row && ["failed", "cancelled", "interrupted"].includes(row.state)) {
      throw new Error(`Task ${messageId} ended as ${row.state}: ${row.error_text ?? "no detail"}`);
    }
    return row?.state === expected;
  });
}

function messageContains(target: Target, marker: string, since: number): boolean {
  return fixture!.sentMessages.slice(since).some(message =>
    message.body.channel_id === target.channelId && sentText(message).includes(marker),
  );
}

async function launch(): Promise<void> {
  const configPath = join(root, "config.json");
  const groups = [...cases.map(testCase => `fork-smoke-${testCase.id}`), tanGroupId];
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture!.apiUrl,
    botToken: fixture!.botToken,
    ownerId: fixture!.ownerUid,
    allowedGroupIds: groups,
    stateDir,
    sessionsDir: join(root, "sessions"),
    ompCommand: join(root, "bin", "real-omp.ts"),
    ompExtraArgs: [
      "--model", "local-fork/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", join(root, "native.yml"),
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });
  const cli = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
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
  cliChildren.push(cli);
  cliOutputs.push(Promise.all([new Response(cli.stdout).text(), new Response(cli.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture!.waitForConnection(1, 15_000);
  await wait("Channel CLI failed to authenticate", () => connection!.ready, 15_000);
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
}

async function bindTarget(name: string, targetGroup: string, sourceRecord: Binding): Promise<{ target: Target; thread: FixtureManagementThread; binding: Binding }> {
  const record = await waitForRemoteThread(targetGroup, name);
  const target: Target = { channelType: 5, channelId: record.channel_id };
  await wait(`Fork target ${record.channel_id} was not durably bound`, () => binding(target) !== null);
  const row = binding(target)!;
  assert.ok(row.session_file, "Fork target binding omitted the native fork session file");
  assert.notEqual(row.session_id, sourceRecord.session_id, "Fork target reused the source session id");
  assert.notEqual(row.session_file, sourceRecord.session_file, "Fork target reused the source session file");
  assert.equal(row.cwd, sourceRecord.cwd, "Fork target did not retain the canonical shared cwd");
  assert.equal(row.cwd, await realpath(repo));
  const forkRecord = await sqliteFork(target);
  assert.equal(row.session_id, forkRecord?.sessionId, "Ready fork record and target binding disagree");
  assert.equal(row.session_file, forkRecord?.sessionFile, "Store fork session file and target binding disagree");
  return { target, thread: record, binding: row };
}

async function waitForRemoteThread(groupId: string, name: string): Promise<FixtureManagementThread> {
  await wait(`Octo fixture did not receive the fork thread create for ${name}`, () => fixture!.managementRequests.some(request =>
    request.method === "POST"
      && request.path === `/v1/bot/groups/${groupId}/threads`
      && request.body.name === name,
  ));
  const threads = await threadRequest(groupId, "GET");
  const found = threads.filter(thread => thread.name === name);
  assert.equal(found.length, 1, `Expected exactly one remote fork thread named ${name}`);
  return found[0]!;
}


async function prepEvidence(
  sourceFile: string,
  targetFile: string,
  forkTarget: Target,
  toolCallId?: string,
  expectTargetPrompt = false,
): Promise<{
  prep: AuditRow;
  prepExit: AuditRow;
  audit: AuditRow[];
}> {
  const audit = rows(auditPath);
  const forkStarts = audit.filter(row => row.kind === "spawn" && row.args?.includes("--fork"));
  const matching = forkStarts.filter(row => {
    const args = row.args ?? [];
    const index = args.indexOf("--fork");
    return index >= 0 && args[index + 1] === sourceFile;
  });
  assert.equal(matching.length, 1, "Expected one installed OMP --fork process for the exact source session file");
  const prep = matching[0]!;
  const args = prep.args!;
  assert.ok(!args.includes("--resume"), "Native preparation must use --fork, never --resume");
  assert.equal(prep.command, realOmp, "Fork preparation did not invoke the configured real OMP executable");
  const prepPid = prep.pid;
  assertExited(prepPid, "Native fork preparation process remained alive after stop confirmation");
  const prepExitIndex = audit.findIndex(row => row.kind === "exit" && row.pid === prepPid);
  assert.ok(prepExitIndex >= 0, "Fork preparation has no confirmed real OMP process exit");
  const prepExit = audit[prepExitIndex]!;
  const forkRecord = await sqliteFork(forkTarget);
  const targetStarts = audit.filter(row => row.kind === "spawn" && row.args?.some((arg, index) => arg === "--resume" && row.args?.[index + 1] === targetFile));
  assert.ok(forkRecord, "Fork preparation record is missing");
  assert.equal(forkRecord.phase, "ready", "Target was published before its durable fork record was ready");
  const sessionDirIndex = args.indexOf("--session-dir");
  const preparationSessionDir = sessionDirIndex >= 0 ? args[sessionDirIndex + 1] : undefined;
  assert.ok(preparationSessionDir, "Fork preparation did not set its isolated OMP session directory");
  assert.ok(preparationSessionDir.startsWith(forkRecord.preparationDir), "Fork preparation session directory escaped its operation directory");
  assert.notEqual(preparationSessionDir, dirname(sourceFile), "Fork preparation reused the source session directory");
  const prepWire = audit.filter(row => row.kind === "wire" && row.pid === prep.pid);
  assert.equal(
    prepWire.filter(row => row.direction === "to-omp" && (row.type === "prompt" || row.op === "prompt")).length,
    0,
    "Fork preparation dispatched a user prompt to OMP",
  );
  assert.equal(
    targetStarts.length,
    expectTargetPrompt ? 1 : 0,
    expectTargetPrompt ? "Fork target did not start for its accepted request" : "Fork target worker auto-started before the next user message",
  );

  if (toolCallId !== undefined) {
    const calls = audit.filter(row => row.kind === "wire" && row.direction === "from-omp"
      && row.type === "host_tool_call" && row.toolName === "fork" && row.toolCallId === toolCallId);
    assert.equal(calls.length, 1, "Expected exactly one native OMP host fork call");
    const call = calls[0]!;
    const hostToolPid = call.pid;
    assert.ok(hostToolPid, "Native host fork call omitted the real source-worker PID");
    const executions = audit.filter(row => row.kind === "wire" && row.direction === "from-omp"
      && (row.type === "tool_execution_start" || row.type === "tool_execution_end")
      && row.toolName === "fork" && row.toolCallId === toolCallId);
    assert.ok(executions.length >= 2, "Native fork tool execution was not reported on the OMP wire");
    assert.ok(executions.every(row => row.toolCallId === toolCallId), "Native fork tool lifecycle changed toolCallId");
    const hostResultIndex = audit.findIndex(row => row.kind === "wire" && row.pid === hostToolPid
      && row.direction === "to-omp" && row.type === "host_tool_result");
    assert.ok(hostResultIndex > prepExitIndex, "Fork result reached OMP before preparation exit was confirmed");
  }
  return { prep, prepExit, audit };
}

function targetResumeEvidence(targetFile: string, prepExit: AuditRow): AuditRow {
  const audit = rows(auditPath);
  const targetStarts = audit.filter(row => row.kind === "spawn" && row.args?.some((arg, index) => arg === "--resume" && row.args?.[index + 1] === targetFile));
  assert.equal(targetStarts.length, 1, "Target worker did not resume exactly once from the exact fork session file");
  const targetStart = targetStarts[0]!;
  assert.equal(targetStart.command, realOmp, "Target resume did not invoke the installed OMP executable");
  const targetStartIndex = audit.indexOf(targetStart);
  const prepExitIndex = audit.findIndex(row => row.kind === "exit" && row.pid === prepExit.pid);
  assert.ok(prepExitIndex >= 0 && prepExitIndex < targetStartIndex, "Target resume started before the fork preparation process exited");
  const targetArgs = targetStart.args!;
  assert.equal(targetArgs[targetArgs.indexOf("--resume") + 1], targetFile);
  return targetStart;
}

async function waitForTargetWorkerAndOutputDrain(target: Target, sessionId: string, ompPid: number): Promise<void> {
  await wait(`Target session ${sessionId} did not stop and drain its output`, () => {
    const ledger = db!.query(
      "SELECT COUNT(*) AS total, SUM(CASE WHEN state != 'stopped' THEN 1 ELSE 0 END) AS active FROM worker_ledger WHERE bot_uid = ? AND session_id = ?",
    ).get(fixture!.botUid, sessionId) as { total: number; active: number | null };
    const outbox = db!.query(
      "SELECT COUNT(*) AS count FROM outbox WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND state IN ('pending', 'unknown')",
    ).get(fixture!.botUid, target.channelType, target.channelId) as { count: number };
    const ompOutputDrained = rows(auditPath).some(row => row.kind === "exit" && row.pid === ompPid);
    return ledger.total > 0 && (ledger.active ?? 0) === 0 && outbox.count === 0 && ompOutputDrained;
  });
}


async function assertNoWorkspaceResources(target: Target, gitBefore: { branch: string; worktrees: string }): Promise<void> {
  const workspaceRows = db!.query("SELECT COUNT(*) AS count FROM thread_workspaces WHERE bot_uid = ? AND channel_id = ?")
    .get(fixture!.botUid, target.channelId) as { count: number };
  assert.equal(workspaceRows.count, 0, "A shared-cwd fork target acquired workspace ownership");
  assert.equal(await run([gitPath, "-C", repo, "branch", "--show-current"]), gitBefore.branch, "Fork changed the source branch");
  assert.equal(await run([gitPath, "-C", repo, "worktree", "list", "--porcelain"]), gitBefore.worktrees, "Fork created or removed a Git worktree");
}

async function archiveOnly(
  target: Target,
  operation: "merge" | "remove",
  source: Target,
  sourceSessionFile: string,
  targetSessionFile: string,
  sourceBytes: Buffer,
  forkBytes: Buffer,
  branchBefore: { branch: string; worktrees: string },
): Promise<void> {
  const sourceMessages = sentFor(source).length;
  const sourceGroupTarget: Target = { channelType: 2, channelId: sourceGroup(source) };
  const sourceGroupMessages = sentFor(sourceGroupTarget).length;
  const targetMessages = fixture!.sentMessages.length;
  const managementStart = fixture!.managementRequests.length;
  const commandId = send(target, `/thread ${operation}`);
  const archivePath = `/v1/bot/groups/${sourceGroup(target)}/threads/${target.channelId.split("____")[1]}/archive`;
  await wait(`Fork /thread ${operation} did not archive its remote child`, () =>
    fixture!.managementRequests.some(request => request.method === "POST" && request.path === archivePath)
      && task(commandId) === null
      && messageContains(target, "归档", targetMessages),
  );
  const inboundRow = inbound(commandId);
  assert.deepEqual(inboundRow, { kind: "command", command_state: "completed" }, `/thread ${operation} unexpectedly dispatched a model task`);
  const targetReceipt = fixture!.sentMessages.slice(targetMessages).filter(message => message.body.channel_id === target.channelId).map(sentText).join("\n");
  assert.match(targetReceipt, /归档/u, "Fork cleanup receipt did not describe archive-only behavior");
  assert.doesNotMatch(targetReceipt, /合并成功|工作区已删除|分支已删除/u, "Fork cleanup claimed Git/workspace cleanup");
  assert.equal(sentFor(source).length, sourceMessages, "Archive-only cleanup sent a pseudo merge notification to the source chat");
  assert.equal(sentFor(sourceGroupTarget).length, sourceGroupMessages, "Archive-only cleanup sent a pseudo merge notification to the parent group");
  const operationRequests = fixture!.managementRequests.slice(managementStart);
  const archiveRequests = operationRequests.filter(request => request.method === "POST" && request.path === archivePath);
  assert.equal(archiveRequests.length, 1, "Archive-only cleanup did not issue exactly one remote archive request");
  assert.equal(operationRequests.filter(request => request.method === "DELETE").length, 0,
    "Archive-only cleanup issued a remote delete instead of archiving the child");
  const remainingThreads = await threadRequest(sourceGroup(target), "GET");
  const retained = remainingThreads.filter(thread => thread.short_id === target.channelId.split("____")[1]);
  assert.equal(retained.length, 1, "Archive-only cleanup removed the remote child record");
  assert.equal(retained[0]!.channel_id, target.channelId);
  assert.equal(retained[0]!.archived, true, "Remote child record was retained without being marked archived");
  const mergeEvents = db!.query("SELECT COUNT(*) AS count FROM thread_merge_events WHERE bot_uid = ? AND channel_id = ?")
    .get(fixture!.botUid, target.channelId) as { count: number };
  assert.equal(mergeEvents.count, 0, "Archive-only cleanup emitted a Git merge event");
  assert.equal(await readFile(join(repo, "shared-marker.txt"), "utf8"), "shared directory remains\n");
  assert.equal(await digest(sourceSessionFile), createHash("sha256").update(sourceBytes).digest("hex"));
  assert.equal(await digest(targetSessionFile), createHash("sha256").update(forkBytes).digest("hex"));
  assert.ok(existsSync(repo), "Archive-only cleanup deleted the shared directory");
  await assertNoWorkspaceResources(target, branchBefore);
  const gitCommands = readFileSync(gitAuditPath, "utf8").split("\n").filter(Boolean);
  assert.ok(!gitCommands.some(command => /(?:^|\s)merge(?:\s|$)|worktree\s+(?:remove|prune)|branch\s+-D/u.test(command)),
    "Archive-only cleanup invoked a Git merge or destructive worktree command");
  if (operation === "merge") metric.archiveMerge += 1;
  else metric.archiveRemove += 1;
}


async function exercise(testCase: Case, gitBefore: { branch: string; worktrees: string }): Promise<void> {
  const source = await sourceTargetFor(testCase);
  const sourceGroupId = sourceGroup(source);
  const canonicalRepo = await realpath(repo);
  const sourceAtStart = await sourceBinding(source, canonicalRepo);
  const priorToken = `FORK_PRIOR_TOKEN_${testCase.id}`;
  const priorLabel = `prior-${testCase.id}`;
  const priorReply = `PRIOR_SAVED_${testCase.id}`;
  const targetLabel = `target-${testCase.id}`;
  const targetReply = `TARGET_DONE_${testCase.id}`;
  scripts.set(priorLabel, [{ kind: "text", text: priorReply }]);
  scripts.set(targetLabel, [{ kind: "text", text: targetReply }]);

  const priorStart = fixture!.sentMessages.length;
  const priorMessageId = send(source, `FORK_SMOKE_TASK:${priorLabel} ${priorToken}`);
  await wait(`Prior source round for ${testCase.id} did not finish`, () => task(priorMessageId)?.state === "completed" && messageContains(source, priorReply, priorStart));
  const afterPrior = binding(source)!;
  assert.equal(afterPrior.session_id, sourceAtStart.session_id, "Prior turn unexpectedly changed source session identity");
  assert.ok(afterPrior.session_file, "Prior source round did not persist a real session file");
  assert.ok((await readFile(afterPrior.session_file, "utf8")).includes(priorToken), "Unique source history token was not persisted in OMP's session file");

  const heldGate = gate();
  const heldLabel = `held-${testCase.id}`;
  const heldReply = `SOURCE_DONE_${testCase.id}`;
  if (testCase.kind === "command") {
    scripts.set(heldLabel, [{ kind: "text", text: heldReply, hold: heldGate }]);
  } else {
    scripts.set(heldLabel, [
      { kind: "fork-tool", name: `fork-${testCase.id}`, toolCallId: `stable-fork-call-${testCase.id}` },
      { kind: "text", text: heldReply, hold: heldGate },
    ]);
  }
  const heldMessageId = send(source, `FORK_SMOKE_TASK:${heldLabel} SOURCE_HELD_${testCase.id}`);
  if (testCase.kind === "command") {
    await wait(`Source model reply for ${testCase.id} was not held`, () => modelRequests.some(request => request.label === heldLabel));
  } else {
    await wait(`Native OMP did not call fork for ${testCase.id}`, () => rows(auditPath).some(row =>
      row.kind === "wire" && row.direction === "from-omp" && row.type === "host_tool_call"
        && row.toolName === "fork" && row.toolCallId === `stable-fork-call-${testCase.id}`,
    ));
  }
  await waitTask(heldMessageId, "started");
  if (testCase.kind === "tool") {
    await wait(`Fork host tool did not return before the source model gate for ${testCase.id}`, () =>
      modelRequests.some(request => request.label === heldLabel && request.call === 2),
    );
  }
  const sourceHeldBinding = binding(source)!;
  assert.deepEqual(sourceHeldBinding, afterPrior, "Source binding changed before the independent fork operation");
  assert.ok(sourceHeldBinding.session_file);
  const sourceBytes = await readFile(sourceHeldBinding.session_file);
  const sourceDigest = await digest(sourceHeldBinding.session_file);
  assert.ok(sourceBytes.includes(Buffer.from(priorToken)), "Source historical token disappeared before fork");
  const sourceSessionId = sourceHeldBinding.session_id;
  const sourceSessionFile = sourceHeldBinding.session_file;
  const caseMessageCount = fixture!.sentMessages.length;
  const modelCountBeforeFork = modelRequests.length;
  const name = `fork-${testCase.id}`;
  let toolCallId: string | undefined;
  let forkCommandId: string | undefined;
  let sourceToolFollowup: ModelRequest | undefined;

  if (testCase.kind === "command") {
    forkCommandId = send(source, `/fork ${name}`);
    await wait(`Group/thread /fork command for ${testCase.id} did not return before source release`, () =>
      messageContains(source, name, caseMessageCount)
        && modelRequests.some(request => request.label === heldLabel)
        && fixture!.managementRequests.some(request => request.method === "POST" && request.path === `/v1/bot/groups/${sourceGroupId}/threads` && request.body.name === name),
    );
    assert.deepEqual(inbound(forkCommandId), { kind: "command", command_state: "completed" }, "/fork command was not handled as a command");
    assert.equal(task(forkCommandId), null, "/fork command created a model task");
    assert.equal(modelRequests.length, modelCountBeforeFork, "/fork command dispatched an automatic source or target model prompt");
  }
  const { target, thread: remoteThread, binding: targetBinding } = await bindTarget(name, sourceGroupId, sourceHeldBinding);
  assert.equal(remoteThread.group_no, sourceGroupId, "Fork thread was not created under the source's parent group");
  assert.notEqual(remoteThread.channel_id, source.channelId, "Fork target reused its source channel id");
  assert.equal(await digest(sourceSessionFile), sourceDigest, "Independent fork operation modified source session bytes");
  assert.deepEqual(binding(source), sourceHeldBinding, "Independent fork operation modified source chat identity");
  assert.equal((await sqliteFork(target))?.phase, "ready");
  await assertNoWorkspaceResources(target, gitBefore);
  assert.ok(!rows(auditPath).some(row => row.kind === "spawn" && row.args?.some((arg, index) => arg === "--resume" && row.args?.[index + 1] === targetBinding!.session_file)),
    "Fork auto-dispatched a target prompt before the target received a user message");
  assert.ok(!modelRequests.some(request => request.label === targetLabel), "Fork made a model call before the target received a user message");

  const operation = await sqliteFork(target);
  assert.equal(operation?.phase, "ready");
  assert.equal(operation?.sourceSessionId, sourceSessionId);
  assert.equal(operation?.sourceSessionFile, sourceSessionFile);
  assert.equal(operation?.cwd, canonicalRepo);
  assert.equal(operation?.sessionId, targetBinding.session_id);
  assert.equal(operation?.sessionFile, targetBinding.session_file);
  assert.equal(operation?.thread?.channelId, target.channelId);
  assert.equal(operation?.thread?.groupId, sourceGroupId);
  assert.equal((operation?.preparationDir ?? "").startsWith(stateDir), true, "Fork preparation directory escaped isolated state");
  assert.ok(operation?.preparationDir);

  if (testCase.kind === "command") {
    const receipt = fixture!.sentMessages.slice(caseMessageCount).filter(message => message.body.channel_id === source.channelId).map(sentText).join("\n");
    assert.ok(receipt.includes(name), "Command receipt omitted the real fork name");
    assert.ok(receipt.includes(remoteThread.short_id), "Command receipt omitted the real thread short_id");
    assert.ok(receipt.includes(targetBinding.session_id), "Command receipt omitted the fork session id");
    assert.ok(receipt.includes(canonicalRepo), "Command receipt omitted the canonical cwd");
    assert.match(receipt, /目录共享/u, "Command receipt omitted the shared-directory warning");
  } else {
    toolCallId = `stable-fork-call-${testCase.id}`;
    sourceToolFollowup = modelRequests.find(request => request.label === heldLabel && request.call === 2);
    assert.ok(sourceToolFollowup, "Missing finite follow-up model request after the native fork tool result");
    const result = JSON.stringify(sourceToolFollowup.body.messages ?? sourceToolFollowup.body);
    for (const fragment of [name, remoteThread.short_id, targetBinding.session_id, canonicalRepo, "目录共享"]) {
      assert.ok(result.includes(fragment), `Native fork tool result omitted ${fragment}`);
    }
    assert.ok(sourceToolFollowup.at >= modelRequests.find(request => request.label === heldLabel && request.call === 1)!.at);
  }

  const prep = await prepEvidence(sourceSessionFile, targetBinding.session_file!, target, toolCallId);
  const forkStartTime = prep.prep.at;
  const prepStopTime = prep.prepExit.at;
  assert.equal(modelRequests.some(request => request.at >= forkStartTime && request.at < prepStopTime), false,
    "A model request occurred while the native OMP --fork preparation process was active");
  assert.equal(modelRequests.length, modelCountBeforeFork, "Fork preparation invoked a model before the next target user message");
  if (testCase.kind === "tool") {
    const sourceFollowup = modelRequests.find(request => request.label === heldLabel && request.call === 2)!;
    const hostToolCall = prep.audit.find(row => row.direction === "from-omp"
      && row.type === "host_tool_call" && row.toolCallId === toolCallId);
    assert.ok(hostToolCall, "Native OMP host tool call is missing from the audited source worker");
    const sourceWorkerPid = hostToolCall.pid;
    assert.ok(sourceWorkerPid, "Native OMP host tool call omitted the source worker PID");
    const resultWire = prep.audit.find(row => row.kind === "wire" && row.pid === sourceWorkerPid
      && row.direction === "to-omp" && row.type === "host_tool_result");
    assert.ok(resultWire, "Native fork result was never returned to OMP");
    assert.ok(sourceFollowup.at >= resultWire.at, "Source model continued before the native host fork result was returned");
  }

  const targetStartMessages = fixture!.sentMessages.length;
  const targetMessageId = send(target, `FORK_SMOKE_TASK:${targetLabel} TARGET_READ_${testCase.id}`);
  await wait(`Forked target ${testCase.id} did not resume the inherited session`, () =>
    modelRequests.some(request => request.label === targetLabel && JSON.stringify(request.body.messages ?? request.body).includes(priorToken)),
  );
  const targetModelRequest = modelRequests.find(request => request.label === targetLabel)!;
  assert.ok(JSON.stringify(targetModelRequest.body.messages ?? targetModelRequest.body).includes(priorToken),
    "Target model request did not contain the unique persisted source history token");
  await wait(`Forked target ${testCase.id} did not complete its response`, () =>
    task(targetMessageId)?.state === "completed" && messageContains(target, targetReply, targetStartMessages),
  );
  const targetBytesPath = targetBinding.session_file!;
  const targetStart = targetResumeEvidence(targetBytesPath, prep.prepExit);
  assert.ok(targetStart.pid, "Target resume omitted the real OMP process PID");
  await waitForTargetWorkerAndOutputDrain(target, targetBinding.session_id, targetStart.pid);
  const forkBytes = await readFile(targetBytesPath);
  const forkDigest = await digest(targetBytesPath);
  assert.deepEqual(binding(source), sourceHeldBinding, "Target conversation changed source chat/session identity");
  assert.equal(await digest(sourceSessionFile), sourceDigest, "Independent target conversation changed source session bytes");
  assert.equal((await readFile(sourceSessionFile)).equals(sourceBytes), true, "Source session file bytes changed while its response gate was held");
  await assertNoWorkspaceResources(target, gitBefore);


  if (testCase.archive !== undefined) {
    await archiveOnly(target, testCase.archive, source, sourceSessionFile, targetBytesPath, sourceBytes, forkBytes, gitBefore);
  }
  assert.ok(existsSync(sourceSessionFile), "Fork operations removed the source session file");
  assert.ok(existsSync(targetBytesPath), "Fork operations removed the fork session file");
  assert.equal(await digest(targetBytesPath), forkDigest, "Archive changed fork session contents");
  assert.deepEqual(binding(source), sourceHeldBinding, "Archive changed the held source binding");
  assert.equal(task(heldMessageId)?.state, "started", "Source completed before the harness released its finite model gate");
  assert.equal(heldGate.released, false, "Source model gate was released before fork/archive acceptance completed");

  heldGate.resolve();
  await wait(`Source turn for ${testCase.id} did not complete after releasing its model gate`, () =>
    task(heldMessageId)?.state === "completed" && messageContains(source, heldReply, caseMessageCount),
  );
  assert.deepEqual(binding(source), sourceHeldBinding, "Source session identity changed after its held turn completed");
  assert.ok((await readFile(sourceSessionFile, "utf8")).includes(priorToken), "Source history disappeared after its turn completed");
  const rowsAfter = rows(auditPath);
  assert.ok(!rowsAfter.some(row => row.kind === "spawn" && row.command !== realOmp), "An audited OMP process did not use the configured executable");
  if (testCase.kind === "command") metric[testCase.sourceKind === "group" ? "commandGroup" : "commandThread"] += 1;
  else metric[testCase.sourceKind === "group" ? "toolGroup" : "toolThread"] += 1;
}

const tanGroupId = "fork-smoke-tan-command";

async function exerciseTan(gitBefore: { branch: string; worktrees: string }): Promise<void> {
  const source: Target = { channelType: 2, channelId: tanGroupId };
  const canonicalRepo = await realpath(repo);
  const sourceAtStart = await sourceBinding(source, canonicalRepo);
  const priorToken = "FORK_PRIOR_TOKEN_tan-command";
  const priorLabel = "prior-tan-command";
  const priorReply = "PRIOR_SAVED_tan-command";
  const heldLabel = "held-tan-command";
  const heldReply = "SOURCE_DONE_tan-command";
  const targetLabel = "target-tan-command";
  const targetReply = "TAN_TARGET_DONE";
  const targetRequestText = `FORK_SMOKE_TASK:${targetLabel} TAN_TARGET_REQUEST_ONLY`;
  const tanCommand = `/tan ${targetRequestText}`;
  scripts.set(priorLabel, [{ kind: "text", text: priorReply }]);

  const priorStart = fixture!.sentMessages.length;
  const priorMessageId = send(source, `FORK_SMOKE_TASK:${priorLabel} ${priorToken}`);
  await wait("The /tan source history turn did not finish", () =>
    task(priorMessageId)?.state === "completed" && messageContains(source, priorReply, priorStart),
  );
  const afterPrior = binding(source)!;
  assert.equal(afterPrior.session_id, sourceAtStart.session_id, "/tan source history unexpectedly changed session identity");
  assert.ok(afterPrior.session_file, "/tan source history did not persist a real session file");
  assert.ok((await readFile(afterPrior.session_file, "utf8")).includes(priorToken), "Unique /tan source history token was not persisted");

  const heldGate = gate();
  scripts.set(heldLabel, [{ kind: "text", text: heldReply, hold: heldGate }]);
  const heldStart = fixture!.sentMessages.length;
  const heldMessageId = send(source, `FORK_SMOKE_TASK:${heldLabel} SOURCE_HELD_tan-command`);
  await wait("The /tan source round did not enter its held model request", () =>
    modelRequests.some(request => request.label === heldLabel),
  );
  await waitTask(heldMessageId, "started");

  const sourceHeldBinding = binding(source)!;
  assert.deepEqual(sourceHeldBinding, afterPrior, "/tan changed source binding before creating the target");
  assert.ok(sourceHeldBinding.session_file);
  const sourceBytes = await readFile(sourceHeldBinding.session_file);
  const sourceDigest = await digest(sourceHeldBinding.session_file);
  assert.ok(sourceBytes.includes(Buffer.from(priorToken)), "The held /tan source session lost its persisted history");
  const sourceSessionFile = sourceHeldBinding.session_file;
  const sourceSessionId = sourceHeldBinding.session_id;
  const sourceMessageStart = fixture!.sentMessages.length;
  const modelCountBeforeTan = modelRequests.length;
  const managementStart = fixture!.managementRequests.length;
  const existingThreads = await threadRequest(tanGroupId, "GET");
  const existingShortIds = new Set(existingThreads.map(thread => thread.short_id));
  const targetGate = gate();
  scripts.set(targetLabel, [{ kind: "text", text: targetReply, hold: targetGate }]);

  const tanMessageId = send(source, tanCommand);
  await wait("The /tan command did not create a target and dispatch its request", () =>
    fixture!.managementRequests.slice(managementStart).some(request =>
      request.method === "POST" && request.path === `/v1/bot/groups/${tanGroupId}/threads`,
    )
      && modelRequests.some(request => request.label === targetLabel),
  );

  const remoteThreads = await threadRequest(tanGroupId, "GET");
  const newThreads = remoteThreads.filter(thread => !existingShortIds.has(thread.short_id));
  assert.equal(newThreads.length, 1, "/tan did not create exactly one target thread");
  const { target, thread, binding: targetBinding } = await bindTarget(newThreads[0]!.name, tanGroupId, sourceHeldBinding);
  assert.equal(thread.short_id, newThreads[0]!.short_id);
  assert.equal(thread.group_no, tanGroupId, "/tan target was not created under its source group");
  assert.notEqual(target.channelId, source.channelId, "/tan reused the source channel");
  assert.equal(await digest(sourceSessionFile), sourceDigest, "/tan preparation modified the held source session file");
  assert.deepEqual(binding(source), sourceHeldBinding, "/tan preparation modified the source chat binding");
  assert.equal((await sqliteFork(target))?.phase, "ready", "/tan target was not backed by a ready durable fork record");
  await assertNoWorkspaceResources(target, gitBefore);

  const operation = await sqliteFork(target);
  assert.equal(operation?.sourceSessionId, sourceSessionId, "/tan fork record captured a different source session id");
  assert.equal(operation?.sourceSessionFile, sourceSessionFile, "/tan fork record captured a different source session file");
  assert.equal(operation?.cwd, canonicalRepo, "/tan fork record changed the source cwd");
  assert.equal(operation?.sessionId, targetBinding.session_id, "/tan fork record and target binding session ids disagree");
  assert.equal(operation?.sessionFile, targetBinding.session_file, "/tan fork record and target binding session files disagree");
  assert.equal(operation?.thread?.channelId, target.channelId, "/tan fork record and target thread disagree");

  const targetRequest = modelRequests.find(request => request.label === targetLabel);
  assert.ok(targetRequest, "/tan target never reached the finite local model");
  const targetPrompt = JSON.stringify(targetRequest.body.messages ?? targetRequest.body);
  assert.ok(targetPrompt.includes(priorToken), "/tan target prompt did not inherit the persisted source history");
  assert.ok(targetPrompt.includes(targetRequestText), "/tan target prompt omitted the exact materialized user request");
  assert.ok(!targetPrompt.includes(tanCommand), "The target model received the raw /tan command instead of its request");

  const targetTaskRows = db!.query(
    "SELECT message_id, text FROM tasks WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).all(fixture!.botUid, target.channelType, target.channelId) as Array<{ message_id: string; text: string }>;
  assert.equal(targetTaskRows.length, 1, "/tan did not durably accept exactly one target task");
  const targetTask = targetTaskRows[0]!;
  assert.equal(targetTask.text, targetRequestText, "/tan target task did not contain only the materialized user request");
  await waitTask(targetTask.message_id, "started");
  const prep = await prepEvidence(sourceSessionFile, targetBinding.session_file!, target, undefined, true);

  const prepStartTime = prep.prep.at;
  const prepStopTime = prep.prepExit.at;
  assert.equal(
    modelRequests.some(request => request.at >= prepStartTime && request.at < prepStopTime),
    false,
    "A /tan target model request occurred while native OMP preparation was active",
  );
  targetResumeEvidence(targetBinding.session_file!, prep.prepExit);
  assert.ok(prep.prepExit.at < targetRequest.at, "/tan target model request started before preparation exit was confirmed");
  assert.equal(
    modelRequests.slice(modelCountBeforeTan).some(request => JSON.stringify(request.body.messages ?? request.body).includes(tanCommand)),
    false,
    "The source or target model received the raw /tan command",
  );

  await wait("The source /tan ingress waited for the held target task instead of completing", () =>
    inbound(tanMessageId)?.command_state === "completed"
      && task(targetTask.message_id)?.state === "started"
      && messageContains(source, thread.short_id, sourceMessageStart),
  );
  assert.deepEqual(inbound(tanMessageId), { kind: "command", command_state: "completed" }, "/tan was not completed as a local command");
  assert.equal(task(tanMessageId), null, "/tan created a source model task");
  assert.equal(targetGate.released, false, "Target task completed before proving /tan ingress was independent of it");
  assert.equal(task(heldMessageId)?.state, "started", "The source task did not remain held while the target started");
  assert.deepEqual(binding(source), sourceHeldBinding, "/tan dispatch changed the source binding");
  assert.equal(await digest(sourceSessionFile), sourceDigest, "/tan dispatch changed the held source session bytes");

  targetGate.resolve();
  const targetStartMessages = fixture!.sentMessages.length;
  await wait("The /tan target did not finish after its model gate was released", () =>
    task(targetTask.message_id)?.state === "completed" && messageContains(target, targetReply, targetStartMessages),
  );
  assert.deepEqual(binding(source), sourceHeldBinding, "The completed /tan target changed the source binding");
  assert.equal(await digest(sourceSessionFile), sourceDigest, "The completed /tan target changed the source session bytes");
  assert.equal(task(heldMessageId)?.state, "started", "The source task completed before its held model gate was released");
  assert.equal(heldGate.released, false, "The source model gate was released before the target completed");

  const sent = fixture!.sentMessages.slice(sourceMessageStart).filter(message =>
    message.body.channel_id === source.channelId || message.body.channel_id === target.channelId,
  );
  assert.ok(sent.every(message => !sentText(message).includes(tanCommand)), "A source or target receipt echoed the raw /tan command");
  heldGate.resolve();
  await wait("The held /tan source turn did not finish after releasing its model gate", () =>
    task(heldMessageId)?.state === "completed" && messageContains(source, heldReply, heldStart),
  );
  assert.deepEqual(binding(source), sourceHeldBinding, "The /tan source session identity changed after its held turn completed");
  assert.ok((await readFile(sourceSessionFile, "utf8")).includes(priorToken), "/tan source history disappeared after completion");
  assert.equal(
    modelRequests.length,
    modelCountBeforeTan + 1,
    "The raw /tan command triggered an extra source or target model request",
  );
  metric.tan += 1;
}


const cases: Case[] = [
  { id: "command-group", kind: "command", sourceKind: "group", archive: "merge" },
  { id: "command-thread", kind: "command", sourceKind: "thread", archive: "remove" },
  { id: "tool-group", kind: "tool", sourceKind: "group" },
  { id: "tool-thread", kind: "tool", sourceKind: "thread" },
];

async function setup(): Promise<{ branch: string; worktrees: string }> {
  await mkdir(join(root, "bin"), { recursive: true });
  await mkdir(join(root, "tmp"), { recursive: true });
  await mkdir(join(root, "agent"), { recursive: true });
  await mkdir(repo, { recursive: true });
  await writeFile(auditPath, "");
  await writeFile(gitAuditPath, "");
  gitPath = Bun.which("git") ?? "";
  assert.ok(gitPath, "A local Git executable is required to prove the fork creates no worktree");
  await run([gitPath, "init", "-q", "-b", "main"], repo);
  await run([gitPath, "-C", repo, "-c", "user.name=Fork Smoke", "-c", "user.email=fork-smoke@example.invalid", "commit", "--allow-empty", "-q", "-m", "fixture"]);
  await writeFile(join(repo, "shared-marker.txt"), "shared directory remains\n");
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  await writeFile(join(root, "bin", "git"), `#!/bin/sh\nprintf '%s\\n' "$*" >> ${JSON.stringify(gitAuditPath)}\nexec ${JSON.stringify(gitPath)} "$@"\n`, { mode: 0o755 });

  const wrapperPath = join(root, "bin", "real-omp.ts");
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
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: {
    "local-fork": {
      baseUrl: `http://127.0.0.1:${modelPort}/v1`,
      apiKey: "local-only-placeholder",
      api: "openai-completions",
      models: [{
        id: "probe",
        name: "Local Fork Acceptance",
        reasoning: false,
        input: ["text"],
        contextWindow: 32768,
        maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }],
    },
  } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
  const branch = await run([gitPath, "-C", repo, "branch", "--show-current"]);
  const worktrees = await run([gitPath, "-C", repo, "worktree", "list", "--porcelain"]);
  return { branch, worktrees };
}

try {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must bind loopback only");
  const modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      if (request.method !== "POST") return new Response("not found", { status: 404 });
      let body: Record<string, unknown>;
      try { body = await request.json() as Record<string, unknown>; }
      catch { return recordModelError("Local finite model received a non-JSON request"); }
      const label = requestLabel(body);
      if (!label) return recordModelError("Model request omitted a finite fork benchmark task marker");
      const step = scripts.get(label)?.shift();
      if (!step) return recordModelError(`Finite model script exhausted or unknown label ${label}`);
      const call = (modelCallCounts.get(label) ?? 0) + 1;
      modelCallCounts.set(label, call);
      const tools = modelTools(body);
      modelRequests.push({ label, call, at: monotonicNow(), tools, body });
      if (step.kind === "fork-tool" && !tools.includes("fork")) {
        return recordModelError(`Native model request ${label} did not advertise the fork host tool`);
      }
      if (step.kind === "text" && step.hold) await step.hold.promise;
      return completion(step, modelRequests.length);
    },
  });
  stopModel = () => modelServer.stop(true);
  const modelServerPort = modelServer.port;
  if (modelServerPort === undefined) throw new Error("Local finite model did not expose a loopback port");
  modelPort = modelServerPort;

  const gitBefore = await setup();
  await launch();
  for (const testCase of cases) await exercise(testCase, gitBefore);
  await exerciseTan(gitBefore);

  assert.equal(modelError, undefined);
  assert.equal(modelRequests.length, 17, "The finite four-path plus /tan workload made an unexpected number of model requests");
  for (const [label, remaining] of scripts) assert.equal(remaining.length, 0, `Finite script ${label} was not fully consumed`);
  assert.deepEqual(metric, { commandGroup: 1, commandThread: 1, toolGroup: 1, toolThread: 1, tan: 1, archiveMerge: 1, archiveRemove: 1 });
  assert.equal(rows(auditPath).filter(row => row.kind === "spawn" && row.args?.includes("--fork")).length, 5,
    "Expected one exact native OMP fork process per existing path and /tan");
  assert.equal(rows(auditPath).filter(row => row.kind === "spawn" && row.args?.includes("--fork")
    && !rows(auditPath).some(exitRow => exitRow.kind === "exit" && exitRow.pid === row.pid)).length, 0,
  "At least one real OMP fork preparation lacked a confirmed child-process exit");
  const hostForkCalls = rows(auditPath).filter(row => row.kind === "wire" && row.direction === "from-omp" && row.type === "host_tool_call" && row.toolName === "fork");
  assert.equal(hostForkCalls.length, 2, "Expected exactly two native host fork tool calls");
  assert.equal(new Set(hostForkCalls.map(row => row.toolCallId)).size, 2, "Native host toolCallId values were not stable and unique across tool paths");
  assert.ok(!readFileSync(gitAuditPath, "utf8").split("\n").filter(Boolean).some(command => /(?:^|\s)merge(?:\s|$)|worktree\s+(?:remove|prune)|branch\s+-D/u.test(command)),
    "Fork smoke unexpectedly performed Git merge/worktree cleanup");
  assert.equal(cliChildren.length, 1, "Expected exactly one real channel CLI process");
  assert.ok(cliChildren.every(child => child.exitCode === null), "Real channel CLI exited before its graceful shutdown");
  await wait("Benchmark left a native OMP worker running", () => {
    const active = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return active.count === 0;
  });
  const recordedPids = new Set<number>();
  for (const row of rows(auditPath).filter(entry => entry.kind === "spawn")) {
    if (typeof row.pid === "number") recordedPids.add(row.pid);
    if (typeof row.wrapperPid === "number") recordedPids.add(row.wrapperPid);
  }
  for (const pid of recordedPids) {
    assert.equal(isAlive(pid), false, `Real OMP or audit relay process ${pid} remained alive after its owning task settled`);
  }
  for (const child of cliChildren) {
    assert.equal(child.exitCode, null, "Real channel CLI exited before its graceful shutdown");
    child.kill("SIGTERM");
    assert.equal(await exited(child), 0, "Real channel CLI did not stop cleanly on SIGTERM");
  }
  const logs = (await Promise.all(cliOutputs)).join("\n");
  assert.ok(!logs.includes(fixture.botToken), "Channel CLI leaked its local fixture bot token");
  succeeded = true;
  console.log(`METRIC fork_command_group=${metric.commandGroup}`);
  console.log(`METRIC fork_command_thread=${metric.commandThread}`);
  console.log(`METRIC fork_tool_group=${metric.toolGroup}`);
  console.log(`METRIC fork_tool_thread=${metric.toolThread}`);
  console.log(`METRIC tan_auto_prompt_dispatches=${metric.tan}`);
  console.log(`METRIC fork_target_real_resumes=${metric.commandGroup + metric.commandThread + metric.toolGroup + metric.toolThread + metric.tan}`);
  console.log(`METRIC fork_preparation_stop_before_resume=5`);
  console.log(`METRIC fork_preparation_prompt_dispatches=0`);
  console.log(`METRIC fork_existing_paths_auto_prompt_dispatches=0`);
  console.log(`METRIC fork_source_session_identity_or_held_bytes_changes=0`);
  console.log(`METRIC fork_archive_only_merge=${metric.archiveMerge}`);
  console.log(`METRIC fork_archive_only_remove=${metric.archiveRemove}`);
  console.log(`METRIC fork_native_tool_calls=${hostForkCalls.length}`);
  console.log(`METRIC fork_model_requests=${modelRequests.length}`);
} catch (error) {
  console.error(JSON.stringify({
    modelRequests: modelRequests.map(({ label, call }) => ({ label, call })),
    modelError: modelError ?? null,
    sentMessages: fixture?.sentMessages.slice(-12).map(message => ({ channelId: message.body.channel_id, text: sentText(message) })),
    ompAudit: rows(auditPath).slice(-30),
    threadForks: db?.query("SELECT channel_type, channel_id, session_id, cwd, session_file FROM chats ORDER BY rowid").all(),
  }));
  throw error;
} finally {
  for (const held of gates) held.resolve();
  for (const child of cliChildren) {
    if (child.exitCode !== null) continue;
    child.kill("SIGTERM");
    try { await exited(child, 10_000); }
    catch { child.kill("SIGKILL"); await child.exited; }
  }
  await Promise.allSettled(cliChildren.map(child => child.exited));
  db?.close();
  if (fixture) await fixture.close();
  stopModel?.();
  const ownedPids = new Set<number>();
  for (const row of rows(auditPath).filter(entry => entry.kind === "spawn")) {
    if (typeof row.pid === "number") ownedPids.add(row.pid);
    if (typeof row.wrapperPid === "number") ownedPids.add(row.wrapperPid);
  }
  for (const pid of ownedPids) {
    if (!isAlive(pid)) continue;
    const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "command="], { stdout: "pipe" });
    const command = await new Response(ps.stdout).text();
    await ps.exited;
    if (!command.includes(root) || (!command.includes(realOmp) && !command.includes(join(root, "bin", "real-omp.ts")))) continue;
    process.kill(pid, "SIGKILL");
    for (let attempt = 0; attempt < 40 && isAlive(pid); attempt += 1) await Bun.sleep(25);
    assert.equal(isAlive(pid), false, `Owned OMP or audit relay process ${pid} survived emergency cleanup`);
  }
  const outputs = await Promise.allSettled(cliOutputs);
  if (!succeeded) {
    const logs = outputs.flatMap(result => result.status === "fulfilled" ? [result.value] : []).join("\n");
    console.error(logs.replaceAll(fixture?.botToken ?? "\u0000", "[fixture-token]").slice(-12_000));
  }
  await rm(root, { recursive: true, force: true });
}
