import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, readlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { ThreadWorkspaceRecord } from "../../src/store/store-records.ts";
import { openChannelStore } from "../../src/store/index.ts";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type FixtureManagementThread,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Offline real-CLI acceptance. Run `bun tests/benchmarks/thread-workspace-expansion.ts` (or set
// OMP_REAL_COMMAND). A deterministic localhost model, the encrypted local Octo protocol fixture,
// an isolated profile/state/session tree, and temporary Git repositories are the only resources.
// The local model returns a real OpenAI tool-call response; installed OMP must turn it into native
// octo_management RPC calls. No host tool is invoked directly, and a missing native call fails.
// No resident service, production Bot, external provider, or business repository is reachable.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const requestedGit = Bun.which("git");
assert.ok(requestedGit, "Git is required for the isolated worktree acceptance");
const gitExecutable = resolve(requestedGit);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-thread-workspace-expansion-")));
const binDir = join(root, "bin");
const stateDir = join(root, "state");
const sessionsDir = join(root, "sessions");
const agentDir = join(root, "agent");
const configPath = join(root, "config.json");
const nativeConfigPath = join(root, "native.yml");
const ompAuditPath = join(root, "omp-audit.jsonl");
const ompWrapperPath = join(binDir, "omp-audit.ts");
const groupId = `workspace-expansion-${process.pid}-${Date.now()}`;
const runId = `${process.pid}-${Date.now()}`;
const seedMarker = `EXPANSION_SEED:${runId}`;
const seedReply = `EXPANSION_SEED_REPLY:${runId}`;
const addMarker = `EXPANSION_ADD:${runId}`;
const addReply = `EXPANSION_ADD_REPLY:${runId}`;
const infoMarker = `EXPANSION_INFO:${runId}`;
const infoReply = `EXPANSION_INFO_REPLY:${runId}`;
const continueMarker = `EXPANSION_CONTINUE:${runId}`;
const afterRemoveInfoMarker = `EXPANSION_AFTER_REMOVE_INFO:${runId}`;
const afterRemoveInfoReply = `EXPANSION_AFTER_REMOVE_INFO_REPLY:${runId}`;
const continueReply = `EXPANSION_CONTINUE_REPLY:${runId}`;
const originalProjectName = "original-service";
const addedProjectName = "added-service";
const originalBranch = `expansion-original-${runId}`;
const worktreeName = `thread-workspace-expansion-${runId}`;
const originalRepoPath = join(root, "repo-original");
const originalWorktreePath = join(root, "existing-single-worktree", "original");
const addedRepoPath = join(root, "repo-added");
const originalTrackedText = `original tracked dirty content ${runId}\n`;
const originalUntrackedName = `uncommitted-${runId}.txt`;
const originalUntrackedText = `original untracked content ${runId}\n`;
const modelRequests: Array<{ label: string; body: Record<string, unknown> }> = [];
const modelCalls = new Map<string, number>();
let modelError: string | undefined;
let fixture: OctoProtocolFixture | undefined;
let modelServer: ReturnType<typeof Bun.serve> | undefined;
let channel: Bun.Subprocess | undefined;
let channelOutput: Promise<string> | undefined;
let connection: FixtureConnection | undefined;
let database: Database | undefined;
let target: { channelType: 5; channelId: string } | undefined;
let thread: FixtureManagementThread | undefined;
let expectedEntryPath: string | undefined;
let expectedWorkspaceRoot: string | undefined;
let acceptedOperationId: string | undefined;
let removedThreadInfo: Record<string, unknown> | undefined;
let readyThreadInfo: Record<string, unknown> | undefined;
let messageSequence = 80_000;
let successful = false;

const setupEnv: Record<string, string> = {
  HOME: root,
  TMPDIR: root,
  PATH: [binDir, dirname(realOmp), dirname(gitExecutable), dirname(process.execPath), "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"].join(":"),
  LANG: "C",
  TZ: "UTC",
  PI_CODING_AGENT_DIR: agentDir,
  PI_NO_TITLE: "1",
  XDG_CONFIG_HOME: join(root, "xdg-config"),
  XDG_CACHE_HOME: join(root, "xdg-cache"),
  XDG_DATA_HOME: join(root, "xdg-data"),
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_TERMINAL_PROMPT: "0",
  NO_PROXY: "127.0.0.1,localhost",
  HTTP_PROXY: "",
  HTTPS_PROXY: "",
  ALL_PROXY: "",
};

interface ToolStep {
  readonly kind: "tool";
  readonly name: "octo_management";
  readonly toolCallId: string;
  readonly args: Record<string, unknown>;
}
interface TextStep {
  readonly kind: "text";
  readonly text: string;
}
type ModelStep = ToolStep | TextStep;
type WorktreeRow = { path: string; branch: string | null };
type BindingRow = { session_id: string; cwd: string; session_file: string | null; cwd_pinned: number };
type WorkspaceSqlRow = {
  layout: string;
  status: string;
  entry_path: string | null;
  workspace_root: string | null;
  projects: string;
  error: string | null;
};
type OperationSqlRow = {
  id: string;
  status: string;
  phase: string;
  candidate_workspace_json: string;
};
type WireAuditRow = {
  kind?: string;
  direction?: string;
  type?: string;
  toolName?: string;
  toolCallId?: string;
  arguments?: unknown;
  isError?: boolean;
  pid?: number;
  exitCode?: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function response(step: ModelStep, index: number): Response {
  const delta = step.kind === "tool"
    ? {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: step.toolCallId,
          type: "function",
          function: { name: step.name, arguments: JSON.stringify(step.args) },
        }],
      }
    : { role: "assistant", content: step.text };
  const id = `thread-workspace-expansion-${runId}-${index}`;
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
      choices: [{ index: 0, delta: {}, finish_reason: step.kind === "tool" ? "tool_calls" : "stop" }],
      usage: { prompt_tokens: 12, completion_tokens: 8, total_tokens: 20 },
    },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function serializedMessages(body: Record<string, unknown>): string {
  return JSON.stringify(body.messages ?? body);
}

function lastToolResult(body: Record<string, unknown>): string | undefined {
  if (!Array.isArray(body.messages)) return undefined;
  for (const candidate of [...body.messages].reverse()) {
    if (!isRecord(candidate) || candidate.role !== "tool") continue;
    if (typeof candidate.content === "string") return candidate.content;
  }
  return undefined;
}

function assertManagementActionAdvertised(body: Record<string, unknown>, action: string): void {
  assert.ok(Array.isArray(body.tools), "Real OMP model request omitted the host-tool catalog");
  const managementTool = body.tools.find((candidate) => {
    if (!isRecord(candidate)) return false;
    if (candidate.name === "octo_management") return true;
    return isRecord(candidate.function) && candidate.function.name === "octo_management";
  });
  assert.ok(managementTool, "Real OMP model request did not expose octo_management");
  assert.ok(JSON.stringify(managementTool).includes(action), `Real OMP did not expose octo_management action ${action}`);
}

function parseToolJson(body: Record<string, unknown>, description: string): Record<string, unknown> {
  const result = lastToolResult(body);
  assert.ok(result, `Real OMP continuation omitted the ${description} host-tool result`);
  let parsed: unknown;
  try {
    parsed = JSON.parse(result);
  } catch {
    throw new Error(`${description} host-tool result was not JSON: ${result.slice(0, 500)}`);
  }
  assert.ok(isRecord(parsed), `${description} host-tool result was not an object`);
  return parsed;
}

function projectFromInfo(info: Record<string, unknown>, name: string): Record<string, unknown> {
  const workspace = info.workspace;
  assert.ok(isRecord(workspace), "thread-info omitted workspace details");
  assert.ok(Array.isArray(workspace.projects), "thread-info workspace omitted projects");
  const project = workspace.projects.find(value => isRecord(value) && value.name === name);
  assert.ok(isRecord(project), `thread-info omitted persisted project ${name}`);
  return project;
}

function assertThreadInfoMatchesExpected(info: Record<string, unknown>, stage: "ready" | "after-remove"): void {
  const workspace = info.workspace;
  assert.ok(isRecord(workspace), "thread-info omitted the persisted workspace");
  assert.equal(workspace.layout, "shared", "thread-info did not report the converted shared layout");
  assert.equal(workspace.cwd, expectedEntryPath, "thread-info cwd differs from the shared entry");
  assert.ok(Array.isArray(workspace.projects), "thread-info projects are missing");
  const original = projectFromInfo(info, originalProjectName);
  assert.equal(original.repo, originalRepoPath, "thread-info changed the original repository identity");
  assert.equal(original.path, originalWorktreePath, "thread-info changed the original absolute worktree path");
  assert.equal(original.branch, originalBranch, "thread-info changed the original branch");
  assert.equal(original.created, true, "thread-info lost managed ownership of the original worktree");
  assert.equal(original.accessPath, join(expectedEntryPath!, originalProjectName), "thread-info omitted the original project's entry alias");
  assert.equal(original.accessTarget, originalWorktreePath, "thread-info alias target differs from the original worktree");
  if (stage === "ready") {
    assert.equal(workspace.status, "ready", "thread-info did not report the completed workspace as ready");
    assert.equal(workspace.ready, true, "thread-info did not validate the actual ready worktrees");
    assert.equal(workspace.projects.length, 2, "thread-info omitted the appended project");
    const added = projectFromInfo(info, addedProjectName);
    assert.equal(added.repo, addedRepoPath, "thread-info changed the appended repository identity");
    assert.equal(added.created, true, "thread-info did not record the appended worktree as managed");
    assert.equal(added.path, join(expectedEntryPath!, addedProjectName), "thread-info appended project at an unexpected path");
    assert.equal(typeof added.branch, "string", "thread-info omitted the appended project's real branch");
  } else {
    assert.equal(workspace.status, "failed", "thread-info did not expose incomplete safe cleanup");
    assert.equal(workspace.ready, false, "thread-info incorrectly reported a partially cleaned workspace as ready");
  }
}

function findScenario(body: Record<string, unknown>): string | undefined {
  const text = serializedMessages(body);
  if (text.includes(afterRemoveInfoMarker)) return "after-remove-info";
  if (text.includes(continueMarker)) return "continue";
  if (text.includes(infoMarker)) return "info";
  if (text.includes(addMarker)) return "add";
  if (text.includes(seedMarker)) return "seed";
  return undefined;
}

async function finiteModel(request: Request): Promise<Response> {
  const url = new URL(request.url);
  if (url.hostname !== "127.0.0.1") {
    modelError ??= `Finite model received a non-loopback request: ${request.method} ${request.url}`;
    return new Response(modelError, { status: 400 });
  }
  if (request.method === "GET" && url.pathname === "/v1/models") {
    return Response.json({ object: "list", data: [{ id: "probe", object: "model" }] });
  }
  if (request.method !== "POST" || url.pathname !== "/v1/chat/completions") {
    modelError ??= `Unexpected finite-model route ${request.method} ${url.pathname}`;
    return new Response(modelError, { status: 404 });
  }
  let body: Record<string, unknown>;
  try {
    const parsed: unknown = await request.json();
    if (!isRecord(parsed)) throw new Error("request body is not an object");
    body = parsed;
  } catch (error) {
    modelError ??= `Invalid finite-model request: ${String(error)}`;
    return new Response(modelError, { status: 400 });
  }
  const label = findScenario(body);
  const serialized = serializedMessages(body);
  if (!label) {
    modelError ??= "Real OMP sent a model request outside the finite workspace-expansion script";
    return new Response(modelError, { status: 400 });
  }
  const calls = (modelCalls.get(label) ?? 0) + 1;
  modelCalls.set(label, calls);
  modelRequests.push({ label, body });
  try {
    if (label === "seed") {
      assert.equal(calls, 1, "The initial real OMP history seed unexpectedly repeated");
      return response({ kind: "text", text: seedReply }, modelRequests.length);
    }
    if (label === "add") {
      if (calls === 1) {
        assertManagementActionAdvertised(body, "thread-workspace-add");
        return response({
          kind: "tool",
          name: "octo_management",
          toolCallId: `workspace_add_${runId}`,
          args: {
            action: "thread-workspace-add",
            worktree_name: worktreeName,
            projects: [{ name: addedProjectName, repo: addedRepoPath }],
          },
        }, modelRequests.length);
      }
      if (calls === 2) {
        const accepted = parseToolJson(body, "thread-workspace-add");
        assert.equal(accepted.accepted, true, "thread-workspace-add was not durably accepted");
        assert.equal(accepted.status, "queued", "thread-workspace-add did not return its queued acceptance receipt");
        assert.equal(accepted.queued, true, "thread-workspace-add acceptance was not marked queued");
        assert.equal(typeof accepted.operation_id, "string", "thread-workspace-add omitted its operation identity");
        acceptedOperationId = accepted.operation_id as string;
        return response({ kind: "text", text: addReply }, modelRequests.length);
      }
      throw new Error("The finite model saw an unexpected repeated workspace-add turn");
    }
    if (label === "info") {
      if (calls === 1) {
        assertManagementActionAdvertised(body, "thread-info");
        return response({
          kind: "tool",
          name: "octo_management",
          toolCallId: `workspace_info_${runId}`,
          args: { action: "thread-info" },
        }, modelRequests.length);
      }
      if (calls === 2) {
        const info = parseToolJson(body, "thread-info");
        assertThreadInfoMatchesExpected(info, "ready");
        readyThreadInfo = info;
        return response({ kind: "text", text: infoReply }, modelRequests.length);
      }
      throw new Error("The finite model saw an unexpected repeated ready thread-info turn");
    }
    if (label === "after-remove-info") {
      if (calls === 1) {
        assertManagementActionAdvertised(body, "thread-info");
        return response({
          kind: "tool",
          name: "octo_management",
          toolCallId: `workspace_after_remove_info_${runId}`,
          args: { action: "thread-info", short_id: thread!.short_id },
        }, modelRequests.length);
      }
      if (calls === 2) {
        const info = parseToolJson(body, "post-removal thread-info");
        assertThreadInfoMatchesExpected(info, "after-remove");
        removedThreadInfo = info;
        return response({ kind: "text", text: afterRemoveInfoReply }, modelRequests.length);
      }
      throw new Error("The finite model saw an unexpected repeated post-removal thread-info turn");
    }
    assert.equal(label, "continue");
    assert.equal(calls, 1, "The post-move continuation unexpectedly repeated");
    assert.ok(serialized.includes(seedMarker), "The real OMP session history lost the pre-move user input");
    assert.ok(serialized.includes(seedReply), "The real OMP session history lost the pre-move assistant reply");
    return response({ kind: "text", text: continueReply }, modelRequests.length);
  } catch (error) {
    modelError ??= error instanceof Error ? error.message : String(error);
    return new Response(modelError, { status: 400 });
  }
}

function sentText(message: FixtureSentMessage): string {
  const payload = message.body.payload;
  return isRecord(payload) && typeof payload.content === "string" ? payload.content : "";
}

function sentFor(targetToRead: { channelType: number; channelId: string }): string[] {
  return fixture!.sentMessages
    .filter(message => message.body.channel_id === targetToRead.channelId)
    .map(sentText);
}

function binding(targetToRead: { channelType: number; channelId: string }): BindingRow | null {
  return database!.query(
    "SELECT session_id, cwd, session_file, cwd_pinned FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(fixture!.botUid, targetToRead.channelType, targetToRead.channelId) as BindingRow | null;
}

function taskState(messageId: string): string | null {
  const row = database!.query("SELECT state FROM tasks WHERE message_id = ?").get(messageId) as { state: string } | null;
  return row?.state ?? null;
}

function workspaceRow(targetToRead: { channelType: number; channelId: string }): WorkspaceSqlRow | null {
  return database!.query(
    "SELECT layout, status, entry_path, workspace_root, projects, error FROM thread_workspaces WHERE bot_uid = ? AND channel_id = ?",
  ).get(fixture!.botUid, targetToRead.channelId) as WorkspaceSqlRow | null;
}

function workspaceProjects(row: WorkspaceSqlRow): Record<string, unknown>[] {
  const parsed: unknown = JSON.parse(row.projects);
  assert.ok(Array.isArray(parsed), "SQLite thread workspace projects are not an array");
  assert.ok(parsed.every(isRecord), "SQLite thread workspace projects contain an invalid record");
  return parsed;
}

function operationRow(operationId: string): OperationSqlRow | null {
  return database!.query(
    "SELECT id, status, phase, candidate_workspace_json FROM thread_workspace_operations WHERE id = ?",
  ).get(operationId) as OperationSqlRow | null;
}

function taskCountWithActiveWorkers(): number {
  return (database!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number }).count;
}

function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

function auditRows(): WireAuditRow[] {
  if (!existsSync(ompAuditPath)) return [];
  return readFileSync(ompAuditPath, "utf8").split("\n").flatMap(line => {
    if (!line.trim()) return [];
    try { return [JSON.parse(line) as WireAuditRow]; } catch { return []; }
  });
}

async function waitFor(label: string, predicate: () => boolean, timeoutMs = 60_000): Promise<void> {
  await waitForFixture(() => {
    if (modelError !== undefined) throw new Error(`Finite localhost model failed: ${modelError}`);
    return predicate() ? true : undefined;
  }, timeoutMs, label);
}

async function boundedExit(child: Bun.Subprocess, label: string, timeoutMs = 30_000): Promise<number> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      child.exited,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function run(argv: string[], cwd: string, timeoutMs = 20_000): Promise<string> {
  const child = Bun.spawn(argv, {
    cwd,
    env: setupEnv,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const output = Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const [stdout, stderr, code] = await Promise.race([
      output,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${argv.join(" ")} exceeded ${timeoutMs} ms`)), timeoutMs);
      }),
    ]);
    if (code !== 0) throw new Error(`${argv.join(" ")} failed (${code}): ${stderr.trim()}`);
    return stdout.trim();
  } catch (error) {
    if (child.exitCode === null) {
      child.kill("SIGTERM");
      await Promise.race([child.exited, Bun.sleep(500)]);
      if (child.exitCode === null) child.kill("SIGKILL");
    }
    await child.exited.catch(() => undefined);
    await output.catch(() => undefined);
    throw error;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function git(cwd: string, ...args: string[]): Promise<string> {
  return run([gitExecutable, "-C", cwd, ...args], root);
}

async function createRepository(path: string, name: string): Promise<string> {
  await mkdir(path, { recursive: true });
  await run([gitExecutable, "-C", path, "init", "--quiet", "-b", "main"], root);
  await run([gitExecutable, "-C", path, "config", "user.name", "Workspace Expansion Fixture"], root);
  await run([gitExecutable, "-C", path, "config", "user.email", "workspace-expansion@example.invalid"], root);
  await run([gitExecutable, "-C", path, "config", "commit.gpgsign", "false"], root);
  await writeFile(join(path, "baseline.txt"), `isolated baseline ${name}\n`, "utf8");
  await git(path, "add", "baseline.txt");
  await git(path, "commit", "--quiet", "-m", `isolated baseline ${name}`);
  return realpath(path);
}

function parseWorktrees(output: string): WorktreeRow[] {
  const rows: WorktreeRow[] = [];
  let current: WorktreeRow | undefined;
  const flush = (): void => {
    if (current) rows.push(current);
    current = undefined;
  };
  for (const field of output.split("\0")) {
    if (field.length === 0) {
      flush();
    } else if (field.startsWith("worktree ")) {
      flush();
      current = { path: field.slice("worktree ".length), branch: null };
    } else if (current && field.startsWith("branch ")) {
      current.branch = field.slice("branch ".length).replace(/^refs\/heads\//u, "");
    }
  }
  flush();
  return rows;
}

function send(targetToSend: { channelType: 2 | 5; channelId: string }, text: string): string {
  assert.ok(connection?.ready, "The local encrypted Octo connection is not ready");
  const messageId = String(++messageSequence);
  const mentionText = `@${fixture!.botName}`;
  fixture!.sendInbound(connection, {
    fromUid: fixture!.ownerUid,
    channelId: targetToSend.channelId,
    channelType: targetToSend.channelType,
    text: `${mentionText} ${text}`,
    mention: { entities: [{ uid: fixture!.botUid, offset: 0, length: mentionText.length }] },
    messageId,
    messageSeq: messageSequence,
  });
  return messageId;
}

async function waitAck(messageId: string): Promise<void> {
  await waitFor(`local Octo fixture did not ACK message ${messageId}`, () =>
    connection!.recvAcks.some(ack => ack.messageId === messageId),
  );
}

async function startChannel(): Promise<void> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "start", "--config", configPath], {
    cwd: root,
    env: setupEnv,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  channel = child;
  channelOutput = Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]).then(parts => parts.join("\n"));
  connection = await fixture!.waitForConnection(1, 20_000);
  await waitForFixture(() => connection!.ready ? true : undefined, 15_000, "real channel CLI did not authenticate to local encrypted Octo");
  assert.equal(connection.connect?.uid, fixture!.botUid, "CLI authenticated as a non-fixture Bot");
  assert.ok(connection.aesKey && connection.aesIv, "Octo fixture connection did not negotiate encryption");
  database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
}

async function createRemoteThread(): Promise<FixtureManagementThread> {
  const created = await fetch(`${fixture!.apiUrl}/v1/bot/groups/${encodeURIComponent(groupId)}/threads`, {
    method: "POST",
    headers: { authorization: `Bearer ${fixture!.botToken}`, "content-type": "application/json" },
    body: JSON.stringify({ name: `workspace expansion ${runId}` }),
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(created.status, 200, "loopback Octo fixture could not create the temporary thread");
  const row: unknown = await created.json();
  assert.ok(isRecord(row) && typeof row.short_id === "string" && typeof row.channel_id === "string");
  assert.equal(row.group_no, groupId, "fixture created the temporary thread in the wrong group");
  assert.equal(row.channel_id, `${groupId}____${row.short_id}`);
  return row as unknown as FixtureManagementThread;
}

async function installIsolatedProfile(): Promise<void> {
  for (const directory of [
    binDir,
    agentDir,
    join(root, "tmp"),
    join(root, "xdg-config"),
    join(root, "xdg-cache"),
    join(root, "xdg-data"),
    stateDir,
    sessionsDir,
    dirname(originalWorktreePath),
  ]) await mkdir(directory, { recursive: true });
  await writeFile(ompAuditPath, "", "utf8");
  for (const executable of ["octo-cli", "super"]) {
    await writeFile(join(binDir, executable), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  }
  const nativeOmpWrapper = `#!${process.execPath}
import { appendFileSync } from "node:fs";
const auditPath = ${JSON.stringify(ompAuditPath)};
const command = ${JSON.stringify(realOmp)};
const args = process.argv.slice(2);
const child = Bun.spawn([command, ...args], { stdin: "pipe", stdout: "pipe", stderr: "inherit", env: process.env });
function append(row) { appendFileSync(auditPath, JSON.stringify({ at: Number(process.hrtime.bigint()) / 1_000_000, ...row }) + "\\n"); }
function wire(direction, line) {
  try {
    const frame = JSON.parse(line);
    const row = { kind: "wire", direction, pid: child.pid, wrapperPid: process.pid };
    for (const key of ["type", "op", "id", "toolName", "toolCallId"]) if (typeof frame[key] === "string") row[key] = frame[key];
    for (const key of ["arguments", "args"]) if (frame[key] !== undefined) row[key] = frame[key];
    if (typeof frame.isError === "boolean") row.isError = frame.isError;
    append(row);
  } catch {}
}
append({ kind: "spawn", pid: child.pid, wrapperPid: process.pid, command, args });
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(signal, () => { try { child.kill(signal); } catch {} });
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
    if (pending.trim()) { wire("to-omp", pending); try { child.stdin.write(pending); } catch {} }
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
  await writeFile(ompWrapperPath, nativeOmpWrapper, { mode: 0o700 });
  const provider = {
    baseUrl: `http://127.0.0.1:${modelServer!.port}/v1`,
    apiKey: "thread-workspace-expansion-local-only",
    api: "openai-completions",
    models: [{
      id: "probe",
      name: "Workspace Expansion Offline Probe",
      reasoning: false,
      input: ["text"],
      contextWindow: 32_768,
      maxTokens: 1_024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }],
  };
  await writeFile(join(agentDir, "models.yml"), JSON.stringify({ providers: { "local-thread-workspace-expansion": provider } }), "utf8");
  await writeFile(nativeConfigPath, "autoRetry:\n  enabled: false\n", "utf8");
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture!.apiUrl,
    botToken: fixture!.botToken,
    ownerId: fixture!.ownerUid,
    allowedGroupIds: [groupId],
    stateDir,
    sessionsDir,
    ompCommand: ompWrapperPath,
    ompExtraArgs: [
      "--approval-mode", "yolo",
      "--model", "local-thread-workspace-expansion/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", nativeConfigPath,
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });
}

async function seedInitialSingleWorkspace(activeThread: { channelType: 5; channelId: string }): Promise<void> {
  const store = await openChannelStore(stateDir);
  try {
    store.ensureBinding({
      botUid: fixture!.botUid,
      channelType: 2,
      channelId: groupId,
      sessionId: `group-${runId}`,
      cwd: originalRepoPath,
      sessionFile: null,
      cwdPinned: false,
    });
    const currentBinding = store.getBinding(fixture!.botUid, activeThread);
    assert.ok(currentBinding, "The real /new command did not persist an initial thread binding");
    assert.equal(currentBinding.cwd, originalWorktreePath);
    assert.ok(currentBinding.sessionId.length > 0, "The initial session has no OMP identity");
    assert.ok(currentBinding.sessionFile, "The initial OMP turn did not create an exact session file");
    assert.ok(existsSync(currentBinding.sessionFile), "The initial OMP session file is missing");
    const sessionText = await readFile(currentBinding.sessionFile, "utf8");
    assert.ok(sessionText.includes(seedMarker), "Real OMP did not persist the seed user input to its session history");
    const workspace: ThreadWorkspaceRecord = {
      botUid: fixture!.botUid,
      channelType: 5,
      channelId: activeThread.channelId,
      layout: "single",
      status: "ready",
      entryPath: null,
      workspaceRoot: null,
      projects: [{
        name: originalProjectName,
        repo: originalRepoPath,
        path: originalWorktreePath,
        branch: originalBranch,
        created: true,
      }],
      error: null,
    };
    store.saveThreadWorkspace(workspace);
  } finally {
    store.close();
  }
}

async function startAndSeedSession(activeThread: { channelType: 5; channelId: string }): Promise<BindingRow> {
  const newId = send(activeThread, `/new ${originalWorktreePath}`);
  await waitAck(newId);
  await waitFor("the thread /new command did not persist its original absolute cwd", () => {
    const row = binding(activeThread);
    return row?.cwd === originalWorktreePath
      && fixture!.sentMessages.some(message => message.body.channel_id === activeThread.channelId
        && sentText(message).includes(`cwd=${originalWorktreePath}`));
  });
  const initialPromptId = send(activeThread, seedMarker);
  await waitAck(initialPromptId);
  await waitFor("the real OMP did not finish the pre-move history seed", () =>
    taskState(initialPromptId) === "completed" && sentFor(activeThread).includes(seedReply),
  );
  await waitFor("the seed OMP worker did not finish stopping", () => taskCountWithActiveWorkers() === 0);
  const original = binding(activeThread);
  assert.ok(original, "No persisted initial thread binding exists");
  assert.equal(original.cwd, originalWorktreePath);
  assert.ok(original.session_id.length > 0);
  assert.ok(original.session_file && existsSync(original.session_file), "The initial OMP session file is not durable");
  return original;
}

async function waitForWorkspaceOperation(operationId: string): Promise<void> {
  await waitFor("the real thread-workspace-add operation did not settle", () => {
    const operation = operationRow(operationId);
    if (operation === null) return false;
    if (["failed", "cancelled", "unknown"].includes(operation.status)) {
      throw new Error(`Workspace-add operation ended ${operation.status} at ${operation.phase}`);
    }
    return operation.status === "complete";
  }, 150_000);
  const operation = operationRow(operationId);
  assert.ok(operation, "SQLite omitted the completed workspace-add operation");
  assert.equal(operation.status, "complete");
  assert.equal(operation.phase, "committed", "Workspace-add was not atomically committed");
}

async function removeChannel(): Promise<void> {
  if (!channel || channel.exitCode !== null) return;
  channel.kill("SIGTERM");
  const code = await boundedExit(channel, "real channel CLI SIGTERM shutdown", 30_000);
  assert.equal(code, 0, "real channel CLI did not exit cleanly after SIGTERM");
  await waitFor("a real OMP worker remained active after CLI shutdown", () => taskCountWithActiveWorkers() === 0, 15_000);
}

async function main(): Promise<void> {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "encrypted Octo fixture must bind only to loopback");
  modelServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: finiteModel });
  assert.ok(modelServer.port, "finite localhost model did not expose an ephemeral port");
  assert.equal(new URL(`http://127.0.0.1:${modelServer.port}/v1`).hostname, "127.0.0.1");

  const originalRepo = await createRepository(originalRepoPath, "original");
  assert.equal(originalRepo, originalRepoPath);
  await mkdir(dirname(originalWorktreePath), { recursive: true });
  await git(originalRepo, "worktree", "add", "--quiet", "-b", originalBranch, originalWorktreePath, "main");
  await writeFile(join(originalWorktreePath, "baseline.txt"), originalTrackedText, "utf8");
  await writeFile(join(originalWorktreePath, originalUntrackedName), originalUntrackedText, "utf8");
  const originalStatusBefore = await git(originalWorktreePath, "status", "--porcelain=v1", "--untracked-files=all");
  assert.notEqual(originalStatusBefore, "", "the single-worktree fixture must contain uncommitted data");
  const addedRepo = await createRepository(addedRepoPath, "added");
  assert.equal(addedRepo, addedRepoPath);

  await installIsolatedProfile();
  await startChannel();
  thread = await createRemoteThread();
  target = { channelType: 5, channelId: thread.channel_id };
  assert.equal(thread.group_no, groupId);
  const initialBinding = await startAndSeedSession(target);
  await seedInitialSingleWorkspace(target);
  const initialSessionFile = initialBinding.session_file!;
  const initialSessionId = initialBinding.session_id;
  const originalSessionBytes = await readFile(initialSessionFile);
  assert.ok(originalSessionBytes.includes(Buffer.from(seedMarker)), "The source session history has no persisted seed marker");

  const addMessageId = send(target, addMarker);
  await waitAck(addMessageId);
  await waitFor("real OMP did not finish the native workspace-add tool turn", () =>
    taskState(addMessageId) === "completed" && sentFor(target!).includes(addReply),
    90_000,
  );
  assert.ok(acceptedOperationId, "Real OMP did not return the durable thread-workspace-add acceptance receipt");
  assert.ok(modelCalls.get("add") === 2, "The model did not observe both the workspace-add native call and its host result");
  await waitForWorkspaceOperation(acceptedOperationId);

  const convertedRow = workspaceRow(target);
  assert.ok(convertedRow, "SQLite lost the registered thread workspace after expansion");
  assert.equal(convertedRow.layout, "shared");
  assert.equal(convertedRow.status, "ready");
  assert.ok(convertedRow.entry_path, "SQLite omitted the shared entry path");
  assert.ok(convertedRow.workspace_root, "SQLite omitted the owned task workspace root");
  expectedEntryPath = convertedRow.entry_path;
  expectedWorkspaceRoot = convertedRow.workspace_root;
  assert.equal(dirname(expectedEntryPath), expectedWorkspaceRoot, "shared entry is not a direct child of its owned root");
  assert.ok(expectedWorkspaceRoot.startsWith(join(stateDir, "thread-workspaces") + "/"), "shared task root escaped the isolated state directory");
  assert.ok(existsSync(expectedWorkspaceRoot) && existsSync(expectedEntryPath), "the committed shared workspace entry is missing");

  const movedBinding = binding(target);
  assert.ok(movedBinding, "SQLite lost the thread binding after native /move");
  assert.equal(movedBinding.cwd, expectedEntryPath, "SQLite binding did not move to the shared entry");
  assert.equal(movedBinding.session_id, initialSessionId, "native /move replaced the existing OMP session ID");
  assert.ok(movedBinding.cwd_pinned === 1, "the migrated thread cwd is not pinned");
  assert.ok(movedBinding.session_file && existsSync(movedBinding.session_file), "SQLite does not point at the exact migrated OMP session file");
  assert.ok((await readFile(movedBinding.session_file, "utf8")).includes(seedMarker), "native /move lost the persisted OMP conversation history");
  const operation = operationRow(acceptedOperationId);
  assert.ok(operation, "SQLite omitted the workspace expansion journal");
  const candidate = JSON.parse(operation.candidate_workspace_json) as Record<string, unknown>;
  assert.equal(candidate.entryPath, expectedEntryPath, "expansion journal and workspace row disagree about entry ownership");
  assert.equal(candidate.workspaceRoot, expectedWorkspaceRoot, "expansion journal and workspace row disagree about task-root ownership");

  const projects = workspaceProjects(convertedRow);
  assert.equal(projects.length, 2, "SQLite did not persist both original and added project ownership records");
  const originalProject = projects.find(project => project.name === originalProjectName);
  const addedProject = projects.find(project => project.name === addedProjectName);
  assert.ok(originalProject, "SQLite omitted the original project ownership");
  assert.ok(addedProject, "SQLite omitted the appended project ownership");
  assert.equal(originalProject.repo, originalRepoPath);
  assert.equal(originalProject.path, originalWorktreePath, "SQLite changed the original absolute worktree path");
  assert.equal(originalProject.branch, originalBranch, "SQLite changed the original Git branch");
  assert.equal(originalProject.created, true);
  assert.equal(originalProject.accessPath, join(expectedEntryPath, originalProjectName));
  assert.equal(originalProject.accessTarget, originalWorktreePath);
  assert.equal(addedProject.repo, addedRepoPath);
  assert.equal(addedProject.created, true);
  assert.equal(addedProject.path, join(expectedEntryPath, addedProjectName));
  assert.equal(typeof addedProject.branch, "string");

  const aliasPath = join(expectedEntryPath, originalProjectName);
  const aliasStat = await lstat(aliasPath);
  assert.ok(aliasStat.isSymbolicLink(), "converted original project is not exposed through a symlink alias");
  assert.equal(await readlink(aliasPath), originalWorktreePath, "original project alias does not point to the preserved absolute worktree");
  assert.equal(await realpath(aliasPath), originalWorktreePath);
  assert.equal(await realpath(originalWorktreePath), originalWorktreePath, "original absolute worktree path was replaced");
  assert.equal(await git(originalWorktreePath, "branch", "--show-current"), originalBranch, "original branch changed during conversion");
  assert.equal(await git(originalWorktreePath, "status", "--porcelain=v1", "--untracked-files=all"), originalStatusBefore,
    "single-to-shared conversion changed original uncommitted Git state");
  assert.equal(await readFile(join(originalWorktreePath, "baseline.txt"), "utf8"), originalTrackedText);
  assert.equal(await readFile(join(originalWorktreePath, originalUntrackedName), "utf8"), originalUntrackedText);
  const originalWorktrees = parseWorktrees(await git(originalRepoPath, "worktree", "list", "--porcelain", "-z"));
  assert.ok(originalWorktrees.some(row => row.path === originalWorktreePath && row.branch === originalBranch),
    "Git does not list the original path and branch after conversion");

  const addedWorktreePath = addedProject.path as string;
  const addedBranch = addedProject.branch as string;
  assert.equal(await realpath(addedWorktreePath), addedWorktreePath, "appended path is not a real directory");
  assert.equal(await git(addedWorktreePath, "rev-parse", "--show-toplevel"), addedWorktreePath,
    "appended project path is not an independent Git worktree root");
  assert.equal(await git(addedWorktreePath, "branch", "--show-current"), addedBranch,
    "appended project branch differs from persisted ownership");
  const addedWorktrees = parseWorktrees(await git(addedRepoPath, "worktree", "list", "--porcelain", "-z"));
  assert.ok(addedWorktrees.some(row => row.path === addedWorktreePath && row.branch === addedBranch),
    "Git does not list the appended real worktree with its persisted branch");

  const infoMessageId = send(target, infoMarker);
  await waitAck(infoMessageId);
  await waitFor("real OMP did not finish the native thread-info tool call", () =>
    taskState(infoMessageId) === "completed" && sentFor(target!).includes(infoReply),
  );
  assert.ok(readyThreadInfo, "Real OMP did not observe the thread-info tool result");
  assertThreadInfoMatchesExpected(readyThreadInfo, "ready");
  const infoWorkspace = readyThreadInfo.workspace as Record<string, unknown>;
  assert.equal(infoWorkspace.status, convertedRow.status);
  assert.deepEqual(infoWorkspace.projects, projects, "real thread-info output differs from SQLite project ownership");
  assert.ok(fixture.managementRequests.some(request => request.method === "GET"
    && request.path === `/v1/bot/groups/${groupId}/threads/${thread!.short_id}`),
  "thread-info did not query the local Octo fixture for the current remote thread");

  const continueMessageId = send(target, continueMarker);
  await waitAck(continueMessageId);
  await waitFor("the exact moved OMP session did not continue its real history", () =>
    taskState(continueMessageId) === "completed" && sentFor(target!).includes(continueReply),
  );
  assert.ok((modelCalls.get("continue") ?? 0) === 1, "post-move session made an unexpected model call count");
  assert.ok(modelRequests.some(request => request.label === "continue"
    && serializedMessages(request.body).includes(seedMarker)
    && serializedMessages(request.body).includes(seedReply)),
  "finite localhost model did not receive the original conversation history after native /move");
  assert.equal(binding(target)?.session_id, initialSessionId, "continuation changed the original session identity");
  const continuedBinding = binding(target);
  assert.ok(continuedBinding?.session_file && existsSync(continuedBinding.session_file));
  assert.ok((await readFile(continuedBinding.session_file, "utf8")).includes(seedMarker),
    "continued OMP history is absent from the exact session file");
  assert.ok(originalSessionBytes.includes(Buffer.from(seedMarker)), "the source session snapshot was not a real persisted OMP transcript");

  // Exercise the non-forced local removal safety gate against the converted multi-project record.
  // The dirty original worktree must remain in its original location and its symlink must not be
  // recursively followed; any clean sibling project that is safely released is recorded per-project.
  await waitFor("the continued OMP worker did not finish stopping", () => taskCountWithActiveWorkers() === 0);
  const managementCountBeforeRemove = fixture.managementRequests.length;
  const removalStart = fixture.sentMessages.length;
  const removeMessageId = send(target, "/thread remove");
  await waitAck(removeMessageId);
  await waitFor("non-forced /thread remove did not report the dirty-worktree safety refusal", () =>
    fixture!.sentMessages.slice(removalStart).some(message => message.body.channel_id === target!.channelId
      && sentText(message).includes("本地工作区清理未完整完成")),
    150_000,
  );
  assert.equal(fixture.managementRequests.slice(managementCountBeforeRemove).some(request =>
    request.method === "DELETE" && request.path.endsWith(`/threads/${thread!.short_id}`)), false,
  "/thread remove unexpectedly deleted the remote thread");
  assert.equal(await realpath(originalWorktreePath), originalWorktreePath, "default removal deleted the dirty original worktree");
  assert.equal(await git(originalWorktreePath, "branch", "--show-current"), originalBranch, "default removal changed the original branch");
  assert.equal(await git(originalWorktreePath, "status", "--porcelain=v1", "--untracked-files=all"), originalStatusBefore,
    "default removal altered the original uncommitted state");
  assert.equal(await readFile(join(originalWorktreePath, "baseline.txt"), "utf8"), originalTrackedText);
  assert.equal(await readFile(join(originalWorktreePath, originalUntrackedName), "utf8"), originalUntrackedText);
  assert.equal(await lstat(aliasPath).then(stat => stat.isSymbolicLink()), true,
    "default removal followed or removed the alias while the original worktree remained owned");
  assert.equal(await readlink(aliasPath), originalWorktreePath, "default removal changed the retained alias target");
  assert.ok(existsSync(expectedWorkspaceRoot) && existsSync(expectedEntryPath), "partial cleanup removed the still-owned shared entry");
  const afterRemovalRow = workspaceRow(target);
  assert.ok(afterRemovalRow, "safe removal discarded the persisted workspace record");
  assert.equal(afterRemovalRow.status, "failed", "partial deletion was incorrectly reported ready");
  assert.equal(afterRemovalRow.entry_path, expectedEntryPath, "partial cleanup lost the owned entry path");
  const afterRemovalProjects = workspaceProjects(afterRemovalRow);
  const retainedOriginal = afterRemovalProjects.find(project => project.name === originalProjectName);
  assert.ok(retainedOriginal?.created === true, "partial cleanup lost original worktree ownership");
  assert.equal(retainedOriginal.path, originalWorktreePath);
  assert.equal(retainedOriginal.branch, originalBranch);
  for (const project of afterRemovalProjects) {
    if (project.name === originalProjectName) continue;
    if (project.created === true) {
      assert.ok(typeof project.path === "string" && existsSync(project.path),
        `SQLite claims project ${String(project.name)} exists without its real worktree`);
      assert.ok(typeof project.branch === "string");
      assert.ok(parseWorktrees(await git(project.repo as string, "worktree", "list", "--porcelain", "-z"))
        .some(row => row.path === project.path && row.branch === project.branch),
      `Git and SQLite disagree about retained project ${String(project.name)}`);
    } else {
      assert.equal(project.path, null, `released project ${String(project.name)} retained a stale path`);
      assert.equal(project.branch, null, `released project ${String(project.name)} retained a stale branch`);
    }
  }
  const originalRepoWorktreesAfterRemove = parseWorktrees(await git(originalRepoPath, "worktree", "list", "--porcelain", "-z"));
  assert.ok(originalRepoWorktreesAfterRemove.some(row => row.path === originalWorktreePath && row.branch === originalBranch),
    "Git no longer tracks the dirty original worktree after safe removal");

  const groupTarget = { channelType: 2 as const, channelId: groupId };
  const afterRemoveInfoMessageId = send(groupTarget, afterRemoveInfoMarker);
  await waitAck(afterRemoveInfoMessageId);
  await waitFor("real OMP did not finish native thread-info after safe partial cleanup", () =>
    taskState(afterRemoveInfoMessageId) === "completed" && sentFor(groupTarget).includes(afterRemoveInfoReply),
  );
  assert.ok(removedThreadInfo, "Real OMP did not observe thread-info after safe partial cleanup");
  assertThreadInfoMatchesExpected(removedThreadInfo, "after-remove");
  const removedInfoWorkspace = removedThreadInfo.workspace as Record<string, unknown>;
  assert.deepEqual(removedInfoWorkspace.projects, afterRemovalProjects,
    "post-removal thread-info differs from SQLite's retained and released project ownership");
  const calls = auditRows().filter(row => row.kind === "wire" && row.direction === "from-omp"
    && row.type === "host_tool_call" && row.toolName === "octo_management");
  const actions = calls.map(row => isRecord(row.arguments) ? row.arguments.action : undefined);
  assert.deepEqual(actions, ["thread-workspace-add", "thread-info", "thread-info"],
    "real installed OMP did not invoke exactly the scripted native expansion and both thread-info tools");
  for (const call of calls) {
    assert.ok(call.toolCallId, "native OMP host-tool call omitted its call identity");
    const events = auditRows().filter(row => row.kind === "wire" && row.direction === "from-omp"
      && (row.type === "tool_execution_start" || row.type === "tool_execution_end")
      && row.toolName === "octo_management" && row.toolCallId === call.toolCallId);
    assert.ok(events.some(row => row.type === "tool_execution_start"), "native host-tool execution did not start");
    const end = events.find(row => row.type === "tool_execution_end");
    assert.ok(end, "native host-tool execution did not finish");
    assert.notEqual(end.isError, true, "native OMP host-tool execution returned an error");
  }
  assert.ok(fixture.registerRequests.length > 0, "real CLI did not register to its local protocol fixture");
  assert.ok(fixture.registerRequests.every(row => row.path.startsWith("/v1/bot/register")),
    "real CLI attempted a non-fixture Bot registration route");
  assert.ok(fixture.connections.every(item => Boolean(item.aesKey && item.aesIv)), "an Octo connection did not negotiate local encryption");
  assert.ok(modelRequests.length >= 6, "finite localhost model did not handle seed, tool calls, and moved-session continuation");
  assert.ok(modelRequests.every(({ body }) => typeof body.model === "string" && body.model.includes("probe")),
    "a real OMP request did not use the configured local-only model");
  assert.equal(modelError, undefined);
  await waitFor("the final OMP worker did not finish stopping", () => taskCountWithActiveWorkers() === 0);

  await removeChannel();
  const spawnPids = auditRows().filter(row => row.kind === "spawn").map(row => row.pid).filter((pid): pid is number => Number.isSafeInteger(pid));
  await waitFor("an audited OMP child remains alive after shutdown", () => spawnPids.every(pid => !processAlive(pid)));
  const output = channelOutput;
  assert.ok(output, "real channel CLI output stream was not captured");
  const logs = await output;
  assert.ok(!logs.includes(fixture.botToken), "isolated CLI output leaked the local fixture Bot token");
  successful = true;
  console.log(`SUMMARY ${JSON.stringify({
    command: "bun tests/benchmarks/thread-workspace-expansion.ts",
    realOmp,
    encryptedLocalOctoFixture: true,
    localhostModelRequests: modelRequests.length,
    nativeManagementActions: actions,
    convertedSingleToShared: true,
    originalSessionIdPreserved: initialSessionId,
    originalWorktree: originalWorktreePath,
    sharedEntry: expectedEntryPath,
    appendedWorktree: addedWorktreePath,
    deletionSafety: "dirty original worktree, branch, files, and alias retained",
    realProviderOrBot: false,
  })}`);
}

try {
  await main();
} catch (error) {
  console.error(JSON.stringify({
    error: error instanceof Error ? error.message : String(error),
    modelError,
    thread: thread ? { shortId: thread.short_id, channelId: thread.channel_id } : null,
    modelRequests: modelRequests.map(({ label }) => label),
    nativeToolCalls: auditRows().filter(row => row.kind === "wire" && row.direction === "from-omp"
      && row.type === "host_tool_call"),
    sentMessages: fixture?.sentMessages.slice(-12).map(sentText),
  }));
  throw error;
} finally {
  if (channel && channel.exitCode === null) {
    channel.kill("SIGTERM");
    try {
      await boundedExit(channel, "benchmark cleanup CLI shutdown", 20_000);
    } catch {
      channel.kill("SIGKILL");
      await channel.exited;
    }
  }
  if (!successful && channelOutput && channel?.exitCode !== null) {
    const logs = await channelOutput.catch(() => "");
    console.error(logs.replaceAll(fixture?.botToken ?? "never-a-token", "[fixture-token]").slice(-12_000));
  }
  database?.close();
  modelServer?.stop(true);
  if (fixture) await fixture.close();
  await rm(root, { recursive: true, force: true });
}
