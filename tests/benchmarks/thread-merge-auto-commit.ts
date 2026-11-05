import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { ThreadWorkspaceRecord } from "../../src/store/store-records.ts";
import { openChannelStore } from "../../src/store/index.ts";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Offline black-box acceptance: a real channel CLI, real installed OMP and Worktrunk, real Git
// worktrees, and a finite loopback-only model. Octo is a local encrypted protocol fixture; no real
// bot registration, resident service, repository, or model credential is used.
// Run: bun tests/benchmarks/thread-merge-auto-commit.ts
const ompCandidate = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(ompCandidate, "Install OMP or set OMP_REAL_COMMAND to its executable");
const wtCandidate = process.env.WT_REAL_COMMAND ?? Bun.which("wt");
assert.ok(wtCandidate, "Install Worktrunk or set WT_REAL_COMMAND to its executable");
const gitCandidate = Bun.which("git");
assert.ok(gitCandidate, "Git is required for this acceptance benchmark");
const gitExecutable = await realpath(gitCandidate);

const omp = await realpath(ompCandidate);
const realWt = await realpath(wtCandidate);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-thread-merge-auto-commit-")));
const bin = join(root, "bin");
const stateDir = join(root, "state");
const sessionsDir = join(root, "sessions");
const repoRoot = join(root, "repos");
const configPath = join(root, "config.json");
const agentDir = join(root, "agent");
const nativeConfig = join(root, "native.yml");
const ompAuditCommand = join(bin, "omp-audit.ts");
const ompAuditPath = join(root, "native-omp.jsonl");
const ompStderrPath = join(root, "native-omp.stderr.log");
const ompStdoutPath = join(root, "native-omp.stdout.log");
const wtConfigDir = join(root, "worktrunk-config");
const wtAuditPath = join(root, "wt-merge-before.tsv");
const failingGitHooksDir = join(root, "failing-git-hooks");
const modelRequests: Array<{ scenario: string; method: string; path: string; body: Record<string, unknown> }> = [];
const scenarioRuns = new Map<string, ScenarioRun>();
let fixture: OctoProtocolFixture | undefined;
let modelServer: { port: number | undefined; stop(closeActiveConnections?: boolean): void } | undefined;
let channel: Bun.Subprocess | undefined;
let channelLogs: Promise<string> | undefined;
let connection: FixtureConnection | undefined;
let database: Database | undefined;
let modelError: string | undefined;
let activeScenario: ScenarioRun | undefined;
let messageSequence = 20_000;

const GROUP_ID = "thread-merge-auto-commit-group";
const pendingWorkersQuery = "SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'";

type ScenarioMode = "commit" | "hook-failure" | "hold";
type Deferred = { readonly promise: Promise<void>; resolve(): void };
type ScenarioRun = {
  readonly name: string;
  readonly mode: ScenarioMode;
  readonly worktree: string;
  readonly requests: Record<string, unknown>[];
  readonly entered: Deferred;
  readonly release: Deferred;
};
type ThreadTarget = { channelType: 5; channelId: string };
type MergeWorld = {
  readonly name: string;
  readonly target: ThreadTarget;
  readonly repo: string;
  readonly worktree: string;
  readonly branch: string;
  readonly baselineHead: string;
  readonly filename: string;
  readonly content: string;
};
type FixtureThread = {
  short_id: string;
  channel_id: string;
};

function deferred(): { promise: Promise<void>; resolve(): void } {
  const value = Promise.withResolvers<void>();
  return { promise: value.promise, resolve: () => value.resolve() };
}

function completion(text: string, tool?: { name: string; args: Record<string, unknown>; id: string }): Response {
  const delta = tool === undefined
    ? { role: "assistant", content: text }
    : {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: tool.id,
          type: "function",
          function: { name: tool.name, arguments: JSON.stringify(tool.args) },
        }],
      };
  const id = `thread-merge-auto-commit-${modelRequests.length}`;
  const chunks = [
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: {}, finish_reason: tool === undefined ? "stop" : "tool_calls" }],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function proposeCommitToolCall(body: Record<string, unknown>, scenario: ScenarioRun): { name: string; args: Record<string, unknown> } {
  const tools = Array.isArray(body.tools) ? body.tools : [];
  const proposal = tools.find(value =>
    isRecord(value) && isRecord(value.function) && value.function.name === "propose_commit",
  );
  if (!isRecord(proposal) || !isRecord(proposal.function)) {
    return noProposeCommitTool(tools, "the real OMP model request did not expose propose_commit");
  }
  const fn = proposal.function;
  const parameters = isRecord(fn.parameters) ? fn.parameters : {};
  const properties = isRecord(parameters.properties) ? parameters.properties : {};
  const args: Record<string, unknown> = {
    i: `Proposing fixture commit for ${scenario.name}`,
    type: "chore",
    scope: null,
    summary: `updated ${scenario.name} thread merge fixture`,
    details: [],
    issue_refs: [],
  };
  const unsupported = Object.keys(args).filter(key => !(key in properties));
  const required = Array.isArray(parameters.required)
    ? parameters.required.filter((key): key is string => typeof key === "string")
    : [];
  const missing = required.filter(key => !(key in args));
  if (unsupported.length > 0 || missing.length > 0) {
    throw new Error(`propose_commit schema mismatch; unsupported fields: ${unsupported.join(", ") || "none"}; missing required fields: ${missing.join(", ") || "none"}`);
  }
  return { name: "propose_commit", args };
}

function noProposeCommitTool(tools: unknown[], reason: string): never {
  const discovered = tools.flatMap(value => isRecord(value) && isRecord(value.function) && typeof value.function.name === "string"
    ? [value.function.name]
    : []);
  throw new Error(`${reason}; advertised tools: ${discovered.join(", ") || "none"}`);
}

async function run(command: string, args: string[], cwd: string, env: Record<string, string>): Promise<string> {
  const child = Bun.spawn([command, ...args], {
    cwd,
    env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`${command} ${args.join(" ")} failed (${exitCode}): ${stderr}`);
  return stdout.trim();
}

async function git(cwd: string, ...args: string[]): Promise<string> {
  return run("git", ["-C", cwd, ...args], root, isolatedEnv);
}

async function wait(label: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  await waitForFixture(() => {
    if (modelError !== undefined) throw new Error(modelError);
    return predicate() ? true : undefined;
  }, timeoutMs, label);
}

function sentTexts(): string[] {
  return fixture!.sentMessages.map(message => {
    const payload = message.body.payload;
    return isRecord(payload) && typeof payload.content === "string" ? payload.content : "";
  });
}

function activeWorkerCount(): number {
  return database!.query<{ count: number }, []>(pendingWorkersQuery).get()!.count;
}

async function readAuditRows(): Promise<Array<{ branch: string; head: string; status: string }>> {
  try {
    return (await readFile(wtAuditPath, "utf8")).trimEnd().split("\n").filter(Boolean).map(line => {
      const [branch = "", head = "", ...status] = line.split("\t");
      return { branch, head, status: status.join("\t") };
    });
  } catch {
    return [];
  }
}
async function readTextTail(path: string, maxChars = 12_000): Promise<string> {
  const text = await readFile(path, "utf8").catch(() => "");
  if (text.length <= maxChars) return text;
  return `[earlier output omitted: ${text.length - maxChars} characters]\n${text.slice(-maxChars)}`;
}

async function createWorld(name: string, filename: string, content: string): Promise<MergeWorld> {
  const repo = join(repoRoot, name);
  const worktree = join(root, "worktrees", name);
  const branch = `thread-merge-${name}`;
  await mkdir(repo, { recursive: true });
  await git(repo, "init", "--quiet", "-b", "main");
  await git(repo, "config", "user.name", "Thread Merge Acceptance");
  await git(repo, "config", "user.email", "thread-merge@example.invalid");
  await git(repo, "config", "commit.gpgsign", "false");
  await git(repo, "config", "core.hooksPath", join(root, "empty-git-hooks"));
  await mkdir(join(root, "empty-git-hooks"), { recursive: true });
  await writeFile(join(repo, "baseline.txt"), `isolated baseline ${name}\n`, "utf8");
  await git(repo, "add", "baseline.txt");
  await git(repo, "commit", "--quiet", "-m", `isolated baseline ${name}`);
  const baselineHead = await git(repo, "rev-parse", "HEAD");
  await mkdir(dirname(worktree), { recursive: true });
  await git(repo, "worktree", "add", "--quiet", "-b", branch, worktree, "main");
  await writeFile(join(worktree, filename), content, "utf8");
  if (name === "failure") {
    await mkdir(failingGitHooksDir, { recursive: true });
    await writeFile(
      join(failingGitHooksDir, "pre-commit"),
      "#!/bin/sh\nprintf '%s\\n' 'THREAD_MERGE_PRE_COMMIT_REJECTED' >> \"$0.invoked\"\nprintf '%s\\n' 'THREAD_MERGE_PRE_COMMIT_REJECTED' >&2\nexit 1\n",
      { mode: 0o700 },
    );
    await git(repo, "config", "core.hooksPath", failingGitHooksDir);
  }
  const created = await fetch(`${fixture!.apiUrl}/v1/bot/groups/${GROUP_ID}/threads`, {
    method: "POST",
    headers: { authorization: `Bearer ${fixture!.botToken}`, "content-type": "application/json" },
    body: JSON.stringify({ name: `acceptance ${name}` }),
  });
  assert.equal(created.status, 200, `local Octo fixture could not create thread ${name}`);
  const thread = await created.json() as FixtureThread;
  assert.ok(thread.short_id && thread.channel_id, "local Octo fixture returned an incomplete thread record");
  return {
    name,
    target: { channelType: 5, channelId: thread.channel_id },
    repo: await realpath(repo),
    worktree: await realpath(worktree),
    branch,
    baselineHead,
    filename,
    content,
  };
}

async function seedWorld(world: MergeWorld): Promise<void> {
  const store = await openChannelStore(stateDir);
  try {
    store.ensureBinding({
      botUid: fixture!.botUid,
      ...world.target,
      cwd: world.worktree,
      sessionId: `thread-merge-${world.name}`,
      sessionFile: null,
      cwdPinned: true,
    });
    store.setChatModel(fixture!.botUid, world.target, "local-thread-merge/probe");
    const ownership: ThreadWorkspaceRecord = {
      botUid: fixture!.botUid,
      channelType: 5,
      channelId: world.target.channelId,
      layout: "single",
      status: "ready",
      entryPath: null,
      projects: [{
        name: `acceptance ${world.name}`,
        repo: world.repo,
        path: world.worktree,
        branch: world.branch,
        created: true,
      }],
      error: null,
    };
    store.saveThreadWorkspace(ownership);
  } finally {
    store.close();
  }
}

function send(target: ThreadTarget, text: string): string {
  const messageId = String(++messageSequence);
  fixture!.sendInbound(connection!, {
    fromUid: fixture!.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text,
    messageId,
    messageSeq: messageSequence,
    mention: { uids: [fixture!.botUid] },
  });
  return messageId;
}

async function waitAck(messageId: string): Promise<void> {
  await wait(`channel did not ACK ${messageId}`, () => connection!.recvAcks.some(ack => ack.messageId === messageId));
}

async function waitForWorkersStopped(): Promise<void> {
  await wait("an OMP worker remained active after the thread operation", () => activeWorkerCount() === 0);
}

async function startChannel(): Promise<void> {
  const launched = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "start", "--config", configPath], {
    cwd: root,
    env: isolatedEnv,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  channel = launched;
  channelLogs = Promise.all([
    new Response(launched.stdout).text(),
    new Response(launched.stderr).text(),
  ]).then(parts => parts.join("\n"));
  connection = await fixture!.waitForConnection(1, 20_000);
  await wait("channel CLI did not authenticate its local encrypted Octo connection", () => connection!.ready);
  assert.equal(connection.connect?.uid, fixture!.botUid, "CLI authenticated a bot other than the disposable fixture identity");
  database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
}

async function childExit(child: Bun.Subprocess, timeoutMs = 20_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      child.exited,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`managed channel process ${child.pid} did not exit within ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function stopChannel(): Promise<number> {
  assert.ok(channel, "the channel CLI was not started");
  if (channel.exitCode === null) channel.kill("SIGTERM");
  const code = await childExit(channel);
  assert.equal(code, 0, "the real channel CLI did not exit cleanly after SIGTERM");
  return code;
}

async function runScenario(world: MergeWorld, mode: ScenarioMode): Promise<ScenarioRun> {
  const run: ScenarioRun = {
    name: world.name,
    mode,
    worktree: world.worktree,
    requests: [],
    entered: deferred(),
    release: deferred(),
  };
  scenarioRuns.set(world.name, run);
  activeScenario = run;
  const mergeStart = fixture!.sentMessages.length;
  const messageId = send(world.target, "/thread merge");
  await waitAck(messageId);
  await wait(`real OMP did not ask the loopback model for ${world.name}`, () => {
    const failure = sentTexts().slice(mergeStart).find(text => /OMP\s*原生提交失败|native commit failed/iu.test(text));
    if (failure) throw new Error(`real OMP failed before requesting the local model: ${failure}`);
    return run.requests.length > 0;
  });
  if (mode === "commit") {
    await wait(`real Worktrunk did not archive successful thread ${world.name}`, () => fixture!.managementRequests.some(request =>
      request.method === "POST" && request.path.endsWith("/archive")
      && request.path.includes(world.target.channelId.slice(world.target.channelId.indexOf("____") + 4)),
    ));
    await waitForWorkersStopped();
    const rows = await readAuditRows();
    const invocation = rows.filter(row => row.branch === world.branch);
    assert.equal(invocation.length, 1, "successful dirty merge did not invoke the real Worktrunk binary exactly once");
    assert.notEqual(invocation[0]!.head, world.baselineHead, "Worktrunk started before OMP created a commit");
    assert.equal(invocation[0]!.status, "", "Worktrunk started while OMP left staged, unstaged, or untracked changes");
    assert.equal(await git(world.repo, "show", `main:${world.filename}`), world.content.trimEnd(), "merged target omitted the untracked input file");
    assert.notEqual(await git(world.repo, "rev-parse", "main"), world.baselineHead, "successful merge did not advance the target branch");
    assert.equal(await git(world.repo, "branch", "--list", world.branch), "", "real Worktrunk did not remove the merged source branch");
    assert.equal(existsSync(world.worktree), false, "real Worktrunk did not remove the merged source worktree");
    assert.ok(run.requests.length >= 1, "OMP never reached the localhost model");
    console.log(`CASE commit-then-real-wt-merge ${JSON.stringify({ branch: world.branch, headAtWtStart: invocation[0]!.head, cleanAtWtStart: true, targetFile: world.filename, wtBinary: realWt })}`);
  } else if (mode === "hook-failure") {
    await wait("native commit failure was not reported to the channel", () =>
      sentTexts().slice(mergeStart).some(text => /提交.*(?:失败|未完成|未能|错误)|(?:失败|未完成|未能|错误).*提交/iu.test(text)),
    );
    await waitForWorkersStopped();
    assert.equal((await readAuditRows()).filter(row => row.branch === world.branch).length, 0, "Worktrunk ran after native OMP commit failure");
    assert.equal(await git(world.worktree, "rev-parse", "HEAD"), world.baselineHead, "failed OMP commit changed the source HEAD");
    assert.ok((await git(world.worktree, "status", "--porcelain", "--untracked-files=all")).includes(world.filename), "failed OMP commit did not preserve the dirty worktree");
    assert.equal(await readFile(join(world.worktree, world.filename), "utf8"), world.content, "failed OMP commit changed the staged fixture file");
    const hookMarker = await readFile(join(failingGitHooksDir, "pre-commit.invoked"), "utf8").catch(() => "");
    assert.ok(hookMarker.includes("THREAD_MERGE_PRE_COMMIT_REJECTED"), "the isolated Git pre-commit hook did not run");
    assert.equal(fixture!.managementRequests.some(request => request.method === "POST" && request.path.endsWith("/archive") && request.path.includes(world.target.channelId.split("____")[1]!)), false, "failed OMP commit archived its thread");
    console.log(`CASE commit-failure-no-merge ${JSON.stringify({ branch: world.branch, headUnchanged: true, dirtyWorktreePreserved: true, wtStarted: false })}`);
  } else {
    assert.equal(mode, "hold");
    await run.entered.promise;
    const cancelAt = fixture!.sentMessages.length;
    const cancelId = send(world.target, "/cancel");
    await waitAck(cancelId);
    await wait("/cancel did not confirm cancellation of the automatic commit", () =>
      sentTexts().slice(cancelAt).some(text => /取消|cancel/iu.test(text)),
    );
    run.release.resolve();
    await waitForWorkersStopped();
    assert.equal((await readAuditRows()).filter(row => row.branch === world.branch).length, 0, "Worktrunk ran after /cancel stopped the automatic commit");
    assert.equal(await git(world.worktree, "rev-parse", "HEAD"), world.baselineHead, "cancelled OMP commit changed the source HEAD");
    assert.ok((await git(world.worktree, "status", "--porcelain", "--untracked-files=all")).includes(world.filename), "cancelled OMP commit did not preserve the dirty worktree");
    console.log(`CASE cancel-before-merge ${JSON.stringify({ branch: world.branch, modelRequestHeld: true, headUnchanged: true, wtStarted: false })}`);
  }
  activeScenario = undefined;
  return run;
}

const isolatedEnv: Record<string, string> = {
  HOME: root,
  TMPDIR: root,
  PATH: [bin, dirname(omp), dirname(realWt), dirname(gitExecutable), dirname(process.execPath), "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"].join(":"),
  LANG: "C",
  TZ: "UTC",
  PI_CODING_AGENT_DIR: agentDir,
  PI_NO_TITLE: "1",
  XDG_CONFIG_HOME: join(root, "xdg-config"),
  XDG_CACHE_HOME: join(root, "xdg-cache"),
  XDG_DATA_HOME: join(root, "xdg-data"),
  WORKTRUNK_CONFIG_DIR: wtConfigDir,
  WORKTRUNK_CONFIG: join(wtConfigDir, "config.toml"),
  THREAD_MERGE_REAL_WT: realWt,
  THREAD_MERGE_WT_AUDIT: wtAuditPath,
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_TERMINAL_PROMPT: "0",
  NO_PROXY: "127.0.0.1,localhost",
  HTTP_PROXY: "",
  HTTPS_PROXY: "",
  ALL_PROXY: "",
};

try {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo protocol fixture must bind to loopback");
  const localModelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const url = new URL(request.url);
      if (request.method !== "POST" || url.hostname !== "127.0.0.1") {
        modelError = `unexpected non-loopback model request ${request.method} ${request.url}`;
        return new Response(modelError, { status: 400 });
      }
      if (url.pathname !== "/v1/chat/completions") {
        modelError = `unexpected local model route ${request.method} ${url.pathname}`;
        return new Response(modelError, { status: 404 });
      }
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        modelError = "real OMP sent invalid JSON to the local model fixture";
        return new Response(modelError, { status: 400 });
      }
      const nativeCommitRequest = Array.isArray(body.tools) && body.tools.some(tool =>
        isRecord(tool) && isRecord(tool.function) && tool.function.name === "propose_commit",
      );
      if (!nativeCommitRequest) return completion("FIXTURE_ORDINARY_TURN_FINISHED");
      const run = activeScenario;
      if (!run) {
        modelError = "real OMP made a native commit model request outside an active /thread merge scenario";
        return new Response(modelError, { status: 400 });
      }
      modelRequests.push({ scenario: run.name, method: request.method, path: url.pathname, body });
      run.requests.push(body);
      run.entered.resolve();
      if (run.mode === "hold") {
        await run.release.promise;
        return completion(`FIXTURE_CANCELLED_${run.name}`);
      }
      if (run.requests.length > 1) return completion(`FIXTURE_COMMIT_FINISHED_${run.name}`);
      try {
        const tool = proposeCommitToolCall(body, run);
        return completion("", { ...tool, id: `thread-merge-${run.name}-proposal` });
      } catch (error) {
        modelError = error instanceof Error ? error.message : String(error);
        return new Response(modelError, { status: 501 });
      }
    },
  });
  modelServer = localModelServer;
  const localModelPort = localModelServer.port;
  assert.ok(localModelPort, "loopback model fixture did not bind a port");

  for (const directory of [bin, agentDir, repoRoot, join(root, "worktrees"), wtConfigDir, join(root, "empty-git-hooks"), join(root, "xdg-config"), join(root, "xdg-cache"), join(root, "xdg-data")]) {
    await mkdir(directory, { recursive: true });
  }
  await writeFile(join(bin, "octo-cli"), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  await writeFile(join(bin, "super"), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  const wtShim = join(bin, "wt");
  await writeFile(wtShim, String.raw`#!/bin/sh
set -eu
if [ "$1" = merge ]; then
  branch=$(git branch --show-current)
  head=$(git rev-parse HEAD)
  status=$(git status --porcelain --untracked-files=all)
  printf '%s\t%s\t%s\n' "$branch" "$head" "$status" >> "$THREAD_MERGE_WT_AUDIT"
fi
exec "$THREAD_MERGE_REAL_WT" "$@"
`, { mode: 0o700 });
  await writeFile(wtAuditPath, "", "utf8");
  await writeFile(join(wtConfigDir, "config.toml"), "", "utf8");
  await writeFile(ompAuditCommand, `#!${process.execPath}
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
const realOmp = ${JSON.stringify(omp)};
const auditPath = ${JSON.stringify(ompAuditPath)};
const stderrPath = ${JSON.stringify(ompStderrPath)};
const stdoutPath = ${JSON.stringify(ompStdoutPath)};
const args = process.argv.slice(2);
const agentDir = process.env.PI_CODING_AGENT_DIR ?? "";
const modelsPath = agentDir ? join(agentDir, "models.yml") : "";
function record(value) { appendFileSync(auditPath, JSON.stringify(value) + "\\n"); }
function argValue(flag) { const index = args.indexOf(flag); return index < 0 ? null : args[index + 1] ?? null; }
const safeArgs = args.map((arg, index) => arg.startsWith("--api-key=")
  ? "--api-key=[redacted]"
  : args[index - 1] === "--api-key" ? "[redacted]" : arg);
record({
  kind: "spawn",
  pid: process.pid,
  args: safeArgs,
  profile: {
    home: process.env.HOME ?? null,
    ompProfile: process.env.OMP_PROFILE ?? null,
    piProfile: process.env.PI_PROFILE ?? null,
    agentDir: agentDir || null,
    modelsPath: modelsPath || null,
    modelsExists: modelsPath ? existsSync(modelsPath) : false,
    model: argValue("--model"),
    config: argValue("--config"),
  },
});
let child;
try {
  child = Bun.spawn([realOmp, ...args], { stdin: "pipe", stdout: "pipe", stderr: "pipe", env: process.env });
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  record({ kind: "spawn-error", message });
  process.stderr.write(message + "\\n");
  process.exit(1);
}
let stdoutCapturedBytes = 0;
let stderrCapturedBytes = 0;
const maxCapturedBytes = 64 * 1024;
const stdoutDone = (async () => {
  const reader = child.stdout.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (args.includes("commit") && stdoutCapturedBytes < maxCapturedBytes) {
        const captured = value.subarray(0, maxCapturedBytes - stdoutCapturedBytes);
        appendFileSync(stdoutPath, captured);
        stdoutCapturedBytes += captured.byteLength;
      }
      process.stdout.write(value);
    }
  } finally {
    reader.releaseLock();
  }
})();
const stderrDone = (async () => {
  const reader = child.stderr.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (stderrCapturedBytes < maxCapturedBytes) {
        const captured = value.subarray(0, maxCapturedBytes - stderrCapturedBytes);
        appendFileSync(stderrPath, captured);
        stderrCapturedBytes += captured.byteLength;
      }
      process.stderr.write(value);
    }
  } finally {
    reader.releaseLock();
  }
})();
const stdinDone = (async () => {
  const reader = Bun.stdin.stream().getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      try { child.stdin.write(value); } catch { break; }
    }
  } finally {
    reader.releaseLock();
    try { child.stdin.end(); } catch {}
  }
})();
const exitCode = await child.exited;
await Promise.all([stdoutDone, stderrDone, stdinDone]);
record({
  kind: "exit",
  pid: child.pid,
  exitCode: exitCode ?? 1,
  stdoutCapturedBytes,
  stderrCapturedBytes,
  outputMayBeTruncated: stdoutCapturedBytes === maxCapturedBytes || stderrCapturedBytes === maxCapturedBytes,
});
process.exitCode = exitCode ?? 1;
`, { mode: 0o700 });
  await writeFile(join(agentDir, "models.yml"), JSON.stringify({ providers: {
    "local-thread-merge": {
      baseUrl: `http://127.0.0.1:${localModelPort}/v1`,
      apiKey: "local-thread-merge-fixture-only",
      api: "openai-completions",
      models: [{
        id: "probe",
        name: "Thread Merge Local Probe",
        reasoning: false,
        input: ["text"],
        contextWindow: 32_768,
        maxTokens: 2_048,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }],
    },
  } }));
  await writeFile(nativeConfig, "autoRetry:\n  enabled: false\n");
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [GROUP_ID],
    stateDir,
    sessionsDir,
    ompCommand: ompAuditCommand,
    ompExtraArgs: [
      "--approval-mode", "yolo",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", nativeConfig,
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  await startChannel();
  const success = await createWorld("success", "untracked-success.txt", "committed only by the real OMP native commit tool\n");
  const failure = await createWorld("failure", "untracked-failure.txt", "must remain dirty after pre-commit hook failure\n");
  const cancelled = await createWorld("cancelled", "untracked-cancelled.txt", "must remain dirty after cancellation\n");
  await Promise.all([seedWorld(success), seedWorld(failure), seedWorld(cancelled)]);

  await runScenario(success, "commit");
  await runScenario(failure, "hook-failure");
  await runScenario(cancelled, "hold");
  await waitForWorkersStopped();
  await stopChannel();
  assert.ok(modelRequests.length >= 3, "the isolated localhost model did not receive all three OMP commit attempts");
  assert.ok(modelRequests.every(({ body }) => {
    const base = body;
    return typeof base.model === "string" && base.model.includes("probe");
  }), "an OMP request did not use the isolated local probe model");
  assert.ok(fixture.registerRequests.length > 0, "the local encrypted Octo fixture did not receive channel authentication");
  assert.ok(fixture.registerRequests.every(request => request.path.startsWith("/v1/bot/register")), "channel attempted a non-fixture registration route");
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1");
  console.log(`SUMMARY ${JSON.stringify({ realOmp: omp, realWorktrunk: realWt, isolatedOctoFixture: true, localhostModelRequests: modelRequests.length, cases: ["commit-then-real-wt-merge", "commit-failure-no-merge", "cancel-before-merge"], realBotRegistered: false })}`);
} catch (error) {
  console.error(JSON.stringify({
    localModelRequests: modelRequests.map(({ scenario, method, path, body }) => ({
      scenario,
      method,
      path,
      model: typeof body.model === "string" ? body.model : null,
      tools: Array.isArray(body.tools)
        ? body.tools.flatMap(tool => isRecord(tool) && isRecord(tool.function) && typeof tool.function.name === "string" ? [tool.function.name] : [])
        : [],
      toolFlow: Array.isArray(body.messages)
        ? body.messages.flatMap(message => {
          if (!isRecord(message)) return [];
          const role = typeof message.role === "string" ? message.role : "";
          const toolCalls = Array.isArray(message.tool_calls)
            ? message.tool_calls.flatMap(call => isRecord(call) && isRecord(call.function) && typeof call.function.name === "string" ? [call.function.name] : [])
            : [];
          if (role !== "tool" && role !== "function" && toolCalls.length === 0) return [];
          return [{
            role,
            name: typeof message.name === "string" ? message.name : null,
            toolCallId: typeof message.tool_call_id === "string" ? message.tool_call_id : null,
            toolCalls,
            contentPreview: typeof message.content === "string" ? message.content.slice(0, 300) : null,
          }];
        }).slice(-16)
        : [],
      proposeCommitSchema: Array.isArray(body.tools)
        ? body.tools.flatMap(tool => isRecord(tool) && isRecord(tool.function) && tool.function.name === "propose_commit" ? [tool.function] : [])
        : [],
    })),
    nativeOmpAudit: await readTextTail(ompAuditPath),
    nativeOmpStdout: await readTextTail(ompStdoutPath),
    nativeOmpStderr: await readTextTail(ompStderrPath),
    sentMessages: fixture ? sentTexts().slice(-12) : [],
    wtInvocations: await readAuditRows().catch(() => []),
    error: error instanceof Error ? error.message : String(error),
  }));
  if (channel && channel.exitCode === null) channel.kill("SIGTERM");
  if (channel) await childExit(channel).catch(() => {});
  if (channelLogs && channel?.exitCode !== null) console.error((await channelLogs).slice(-12_000));
  throw error;
} finally {
  for (const run of scenarioRuns.values()) run.release.resolve();
  if (channel && channel.exitCode === null) {
    channel.kill("SIGTERM");
    try {
      await childExit(channel);
    } catch {
      channel.kill("SIGKILL");
      await channel.exited;
    }
  }
  database?.close();
  modelServer?.stop(true);
  if (fixture) await fixture.close();
  await rm(root, { recursive: true, force: true });
}
