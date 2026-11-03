import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ChannelService,
  ChatTarget,
  CoreServiceConfig,
  OmpWorker,
  WorkerFactory,
  WorkerProcess,
  WorktreePort,
} from "../../src/contracts.ts";
import type { ChannelStore } from "../../src/store/index.ts";
import { createChannelService } from "../../src/service.ts";
import { createWorkerFactory } from "../../src/omp/index.ts";
import { createThreadWorkspaceBatch } from "../../src/workspace/thread-workspaces.ts";
import { createWorktreePort } from "../../src/workspace/worktree.ts";
import { openChannelStore } from "../../src/store/index.ts";
import { FakeTransport, inbound } from "../fixtures/core-fakes.ts";

// Offline acceptance: real installed OMP RPC children talk only to this finite localhost model
// and the in-memory FakeTransport. Run `bun tests/benchmarks/concurrency.ts` (or set
// OMP_REAL_COMMAND); no Octo registration or external model/provider credentials are used.
// Every concurrency hold is released by an explicit gate, not by a timing sleep.
const ompCommand = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(ompCommand, "Install OMP or set OMP_REAL_COMMAND to its executable");
// Process identities use ps timestamps; establish the timezone before Bun initializes libc.
if (process.env.TZ !== "UTC") {
  const child = Bun.spawn([process.execPath, import.meta.path], {
    env: { ...process.env, TZ: "UTC", LANG: "C" },
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
  });
  process.exit(await child.exited);
}
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-concurrency-benchmark-")));
const stateDir = join(root, "tmp", "state");
const parentCwd = join(root, "tmp");
const repoAlpha = join(root, "repo-alpha");
const repoBeta = join(root, "repo-beta");
const audit: AuditRow[] = [];
const processes = new Map<number, WorkerProcess>();
const activeSessions = new Map<string, number>();
const modelRequests: ModelRequest[] = [];
const modelGates = new Map<string, VoidDeferred>();
const activeModelHolds = new Set<string>();
const perLabelModelCalls = new Map<string, number>();
const taskLabels = [
  "ancestor-parent", "ancestor-child", "reverse-child", "reverse-parent", "project-alpha", "project-beta",
  "sibling-left", "sibling-right", "same-cwd-one", "same-cwd-two", "same-session-owner",
  "same-session-waiter", "delete-blocked", "delete-after",
] as const;
const expectedModelCalls = new Map<string, number>(
  taskLabels.map((label): [string, number] => [label, label.startsWith("delete-") ? 2 : 1]),
);
const subprocessBaseline = new Set<number>();
const ownedSubprocesses = new Map<number, string>();
const removeCalls: Array<{ path: string; activeHolds: string[] }> = [];
let modelError: string | undefined;
let service: ChannelService | undefined;
let store: ChannelStore | undefined;
let db: Database | undefined;
let stopModelServer: (() => void) | undefined;
let localModelPort: number | undefined;
let transport: FakeTransport | undefined;
let threadTarget: ChatTarget | undefined;
let threadShortId: string | undefined;
let workspaceEvidence: { entryPath: string; alphaPath: string; betaPath: string } | undefined;
let succeeded = false;
let messageSequence = 0;

interface VoidDeferred {
  promise: Promise<void>;
  resolve(value?: void | PromiseLike<void>): void;
  reject(reason?: unknown): void;
}

interface AuditRow {
  kind: string;
  [key: string]: unknown;
}
interface ModelRequest {
  label: string;
  path: string;
  toolNames: string[];
  call: number;
}
interface ProcessRow {
  pid: number;
  ppid: number;
  identity: string;
  command: string;
}

function record(kind: string, details: Record<string, unknown> = {}): void {
  audit.push({ kind, at: Date.now(), ...details });
}

function bounded<T>(promise: Promise<T>, label: string, timeoutMs = 45_000): Promise<T> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
  return Promise.race([promise, timeout.promise]).finally(() => clearTimeout(timer));
}

async function waitFor(label: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (modelError !== undefined) throw new Error(`Local model fixture failed: ${modelError}`);
    if (predicate()) return;
    const tick = Promise.withResolvers<void>();
    setImmediate(tick.resolve);
    await tick.promise;
  }
  throw new Error(`${label} timed out after ${timeoutMs} ms`);
}

async function run(argv: string[], cwd?: string): Promise<string> {
  const child = Bun.spawn(argv, {
    ...(cwd === undefined ? {} : { cwd }),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  assert.equal(code, 0, `${argv.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function processTable(): Promise<ProcessRow[]> {
  const child = Bun.spawn(["/bin/ps", "-axo", "pid=,ppid=,lstart=,command="], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  assert.equal(code, 0, `process snapshot failed: ${stderr}`);
  return stdout.trim().split("\n").flatMap((line) => {
    const fields = line.trim().split(/\s+/u);
    if (fields.length < 8) return [];
    const pid = Number(fields[0]);
    const ppid = Number(fields[1]);
    if (!Number.isSafeInteger(pid) || !Number.isSafeInteger(ppid)) return [];
    return [{ pid, ppid, identity: fields.slice(2, 7).join(" "), command: fields.slice(7).join(" ") }];
  });
}

async function captureOwnedSubprocesses(): Promise<void> {
  const rows = await processTable();
  const descendants = new Set([process.pid]);
  for (let changed = true; changed;) {
    changed = false;
    for (const row of rows) {
      if (!descendants.has(row.ppid) || descendants.has(row.pid)) continue;
      descendants.add(row.pid);
      changed = true;
      if (!subprocessBaseline.has(row.pid)) ownedSubprocesses.set(row.pid, row.identity);
    }
  }
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

function response(label: string, toolCall?: { name: string; args: Record<string, unknown>; id: string }): Response {
  const delta = toolCall === undefined
    ? { role: "assistant", content: `CONCURRENCY_EFFECT:${label}` }
    : {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: toolCall.id,
          type: "function",
          function: { name: toolCall.name, arguments: JSON.stringify(toolCall.args) },
        }],
      };
  const id = `concurrency-${label}-${perLabelModelCalls.get(label) ?? 0}`;
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
      choices: [{ index: 0, delta: {}, finish_reason: toolCall === undefined ? "stop" : "tool_calls" }],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  ];
  return new Response(chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function requestLabel(body: Record<string, unknown>): string | undefined {
  const serialized = JSON.stringify(body.messages ?? body);
  const matches = [...serialized.matchAll(/CONCURRENCY_TASK:([A-Za-z0-9_-]+)/gu)];
  return matches.at(-1)?.[1];
}

function modelTools(body: Record<string, unknown>): string[] {
  if (!Array.isArray(body.tools)) return [];
  return body.tools.flatMap((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return [];
    const tool = entry as Record<string, unknown>;
    if (typeof tool.name === "string") return [tool.name];
    if (!tool.function || typeof tool.function !== "object" || Array.isArray(tool.function)) return [];
    const name = (tool.function as Record<string, unknown>).name;
    return typeof name === "string" ? [name] : [];
  });
}


async function waitForModelRequest(label: string): Promise<void> {
  await waitFor(`model request ${label}`, () => modelRequests.some((request) => request.label === label));
}



function message(target: ChatTarget, messageId: string, text: string) {
  return inbound({
    ...target,
    messageId,
    senderId: "owner-concurrency",
    text,
    mentionedUserIds: target.channelType === 2 || target.channelType === 5 ? ["bot-concurrency"] : [],
  });
}

function taskState(messageId: string): string | undefined {
  const row = db!.query("SELECT state FROM tasks WHERE message_id = ?").get(messageId) as { state: string } | null;
  return row?.state;
}

async function taskCompleted(messageId: string, label: string): Promise<void> {
  await waitFor(`task ${label} completion`, () => {
    const state = taskState(messageId);
    if (state === "failed" || state === "cancelled" || state === "interrupted") {
      throw new Error(`task ${label} ended as ${state}`);
    }
    return state === "completed";
  });
}

async function submit(target: ChatTarget, label: string): Promise<string> {
  const messageId = `concurrency-${++messageSequence}`;
  await service!.handleInbound(message(target, messageId, `CONCURRENCY_TASK:${label}`));
  return messageId;
}

async function prepareSession(target: ChatTarget, cwd: string): Promise<void> {
  await mkdir(cwd, { recursive: true });
  const messageId = `concurrency-new-${++messageSequence}`;
  const sentBefore = transport!.sent.length;
  await service!.handleInbound(message(target, messageId, `/new ${cwd}`));
  const binding = store!.getBinding("bot-concurrency", target);
  if (binding === null) {
    const replies = transport!.sent.slice(sentBefore).map((sent) => sent.text);
    throw new Error(
      `missing session binding for ${target.channelType}:${target.channelId}; owner=${store!.getOwnerId("bot-concurrency")}; replies=${JSON.stringify(replies)}; modelError=${modelError ?? "none"}; modelRequests=${JSON.stringify(modelRequests.map((request) => request.label))}; audit=${JSON.stringify(audit.slice(-8))}`,
    );
  }
  assert.equal(binding.cwd, await realpath(cwd), "explicit /new did not preserve its requested cwd");
  if (!binding.sessionFile) throw new Error("real OMP /new must persist a session file");
  assert.ok(await Bun.file(binding.sessionFile).exists(), "real OMP session state file must exist on disk");
}

function taskStart(label: string): AuditRow {
  const row = audit.find((entry) => entry.kind === "task_start" && entry.label === label);
  assert.ok(row, `real OMP worker did not start task ${label}`);
  return row;
}

async function assertTaskStarted(label: string, expectedCwd: string): Promise<void> {
  await waitForModelRequest(label);
  await captureOwnedSubprocesses();
  const row = taskStart(label);
  assert.equal(row.cwd, await realpath(expectedCwd), `${label} started in the wrong cwd`);
  assert.equal(typeof row.pid, "number");
  assert.equal(typeof row.sessionFile, "string");
  const pid = row.pid as number;
  assert.ok(audit.some((entry) => entry.kind === "process_start" && entry.pid === pid), `${label} lacks actual OMP process-start evidence`);
  assert.ok(audit.some((entry) => entry.kind === "session_hold" && entry.pid === pid && entry.sessionFile === row.sessionFile), `${label} lacks session ownership evidence`);
  assert.equal(activeSessions.get(row.sessionFile as string), pid, `${label} session was not held while OMP was running`);
  assert.ok(isAlive(pid), `${label} OMP child must remain alive behind its model gate`);
}

async function prepareRepository(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
  await run(["git", "init", "-q"], path);
  await run(["git", "-C", path, "-c", "user.name=Concurrency Smoke", "-c", "user.email=smoke@example.invalid", "commit", "--allow-empty", "-q", "-m", "fixture"], path);
}

const previousEnvironment = new Map<string, string | undefined>();
const previousProcessEnvironment = new Map<string, string | undefined>();
function setEnvironment(key: string, value: string | undefined): void {
  if (!previousEnvironment.has(key)) previousEnvironment.set(key, Bun.env[key]);
  if (!previousProcessEnvironment.has(key)) previousProcessEnvironment.set(key, process.env[key]);
  if (value === undefined) {
    delete Bun.env[key];
    delete process.env[key];
  } else {
    Bun.env[key] = value;
    process.env[key] = value;
  }
}

async function setupLocalModel(): Promise<void> {
  await mkdir(join(root, "agent"), { recursive: true });
  await mkdir(join(root, "bin"), { recursive: true });
  for (const key of new Set([...Object.keys(Bun.env), ...Object.keys(process.env)])) setEnvironment(key, undefined);
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  setEnvironment("HOME", root);
  setEnvironment("TMPDIR", root);
  setEnvironment("PATH", `${join(root, "bin")}:/usr/bin:/bin`);
  setEnvironment("LANG", "C");
  setEnvironment("TZ", "UTC");
  setEnvironment("PI_CODING_AGENT_DIR", join(root, "agent"));
  setEnvironment("PI_NO_TITLE", "1");
  const provider = {
    baseUrl: `http://127.0.0.1:${localModelPort!}/v1`,
    apiKey: "local-only-placeholder",
    api: "openai-completions",
    models: [{
      id: "probe",
      name: "Concurrency Probe",
      reasoning: false,
      input: ["text"],
      contextWindow: 32768,
      maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }],
  };
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: { "local-concurrency": provider } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
}

async function main(): Promise<void> {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        modelError = `non-JSON local model request ${request.method} ${new URL(request.url).pathname}`;
        return new Response("invalid model request", { status: 400 });
      }
      const label = requestLabel(body);
      if (!label) {
        modelError = `model request lacked a concurrency task marker at ${new URL(request.url).pathname}`;
        return new Response("missing task marker", { status: 400 });
      }
      const maxCalls = expectedModelCalls.get(label);
      if (maxCalls === undefined) {
        modelError = `unknown concurrency task label ${label}`;
        return new Response(modelError, { status: 400 });
      }
      const call = (perLabelModelCalls.get(label) ?? 0) + 1;
      if (call > maxCalls) {
        modelError = `finite model script exhausted for ${label}`;
        return new Response(modelError, { status: 400 });
      }
      perLabelModelCalls.set(label, call);
      const toolNames = modelTools(body);
      modelRequests.push({ label, path: new URL(request.url).pathname, toolNames, call });
      const gate = modelGates.get(label);
      if (gate) {
        activeModelHolds.add(label);
        try {
          await bounded(gate.promise, `model gate ${label}`);
        } finally {
          activeModelHolds.delete(label);
        }
      }
      if ((label === "delete-blocked" || label === "delete-after") && call === 1) {
        if (!toolNames.includes("octo_management")) {
          modelError = "real OMP did not expose octo_management to the local model";
          return new Response(modelError, { status: 400 });
        }
        if (!store!.getThreadWorkspace("bot-concurrency", threadTarget!)) {
          modelError = "shared thread workspace ownership disappeared before delete tool call";
          return new Response(modelError, { status: 400 });
        }
        if (!threadShortId) {
          modelError = "fake transport thread identity is unavailable";
          return new Response(modelError, { status: 400 });
        }
        return response(label, {
          name: "octo_management",
          id: `delete-${label}`,
          args: { action: "thread-delete", short_id: threadShortId },
        });
      }
      return response(label);
    },
  });
  localModelPort = server.port;
  stopModelServer = () => server.stop(true);
  await setupLocalModel();

  await mkdir(parentCwd, { recursive: true });
  await mkdir(stateDir, { recursive: true });
  await prepareRepository(repoAlpha);
  await prepareRepository(repoBeta);

  const worktreeCommand = join(root, "omp-worktree-command");
  await writeFile(worktreeCommand, `#!/bin/sh
set -eu
[ "$1" = worktree ] && [ "$2" = add ] || exit 64
path=$3
shift 3
branch=
repo=
base=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -b) branch=$2; shift 2 ;;
    -C) repo=$2; shift 2 ;;
    -q) shift ;;
    *) base=$1; shift ;;
  esac
done
[ -n "$branch" ] && [ -n "$repo" ] || exit 65
if [ -n "$base" ]; then exec git -C "$repo" worktree add -q -b "$branch" "$path" "$base"; fi
exec git -C "$repo" worktree add -q -b "$branch" "$path"
`, { mode: 0o755 });
  const realWorktrees = createWorktreePort({
    command: worktreeCommand,
    worktrunkCandidates: [join(root, "no-worktrunk-installed")],
    createTimeoutMs: 15_000,
    removeTimeoutMs: 15_000,
  });
  const worktrees: WorktreePort = {
    worktrunk: (signal) => realWorktrees.worktrunk(signal),
    create: (request, signal) => realWorktrees.create(request, signal),
    planMerge: (request, signal) => realWorktrees.planMerge(request, signal),
    merge: (request, signal) => realWorktrees.merge(request, signal),
    ...(realWorktrees.recoverMergeProcess === undefined
      ? {}
      : { recoverMergeProcess: (identity) => realWorktrees.recoverMergeProcess!(identity) }),
    remove: async (request, signal) => {
      removeCalls.push({ path: request.path, activeHolds: [...activeModelHolds].sort() });
      return realWorktrees.remove(request, signal);
    },
  };

  const bot = { uid: "bot-concurrency", name: "Offline Concurrency Bot" };
  const ownerId = "owner-concurrency";
  const activeGroupId = "group-concurrency";
  const groupTarget: ChatTarget = { channelType: 2, channelId: activeGroupId };
  const activeTransport = new FakeTransport({ bot, startOnline: true });
  transport = activeTransport;
  const config: CoreServiceConfig = {
    ownerId,
    allowedGroupIds: [activeGroupId],
    stateDir,
    sessionsDir: join(root, "sessions"),
    homeDir: root,
    groupHistory: { enabled: false, maxMessages: 50 },
    attachments: { enabled: false, maxBytes: 1024 },
  };
  const realFactory = createWorkerFactory({
    ompCommand: ompCommand!,
    extraArgs: [
      "--model", "local-concurrency/probe", "--no-skills", "--no-rules", "--no-context-files",
      "--no-lsp", "--no-tools", "--config", join(root, "native.yml"),
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
  });
  const workerFactory: WorkerFactory = {
    sessionRootsFor: (cwd) => realFactory.sessionRootsFor!(cwd),
    async create(options) {
      let processInfo: WorkerProcess | undefined;
      const worker = await realFactory.create({
        ...options,
        onSpawn(info) {
          processInfo = info;
          processes.set(info.pid, info);
          record("process_start", { pid: info.pid, pgid: info.pgid, cwd: info.cwd, identity: info.identity });
          options.onSpawn(info);
        },
      });
      await captureOwnedSubprocesses();
      const sessionFile = worker.sessionFile;
      const sessionId = worker.sessionId;
      const pid = processInfo?.pid;
      if (pid !== undefined) {
        const previous = activeSessions.get(sessionFile);
        if (previous !== undefined) record("session_overlap", { sessionFile, pid, previousPid: previous });
        activeSessions.set(sessionFile, pid);
        record("session_hold", { pid, cwd: options.cwd, sessionFile, sessionId });
      }
      let exitRecorded = false;
      return new Proxy(worker, {
        get(target, property) {
          if (property === "prompt") {
            return (text: string, onEvent: Parameters<OmpWorker["prompt"]>[1], context?: Parameters<OmpWorker["prompt"]>[2]) => {
              const match = text.match(/CONCURRENCY_TASK:([A-Za-z0-9_-]+)/u);
              const label = match?.[1] ?? null;
              record("task_start", { label, pid, cwd: options.cwd, sessionFile, sessionId, text });
              return target.prompt(text, (event) => {
                if (event.type === "text" && event.final && event.text.length > 0) {
                  record("effect", { label, pid, cwd: options.cwd, sessionFile, sessionId, text: event.text });
                }
                onEvent(event);
              }, context);
            };
          }
          if (property === "stop") {
            return async () => {
              const stopped = await target.stop();
              if (stopped && !exitRecorded && pid !== undefined) {
                exitRecorded = true;
                record("session_release", { pid, sessionFile, sessionId });
                if (activeSessions.get(sessionFile) === pid) activeSessions.delete(sessionFile);
                record("process_exit", { pid, pgid: processInfo?.pgid ?? null, cwd: processInfo?.cwd ?? options.cwd, identity: processInfo?.identity ?? null });
              }
              return stopped;
            };
          }
          const value = Reflect.get(target, property, target) as unknown;
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    },
    createCommit: (options) => realFactory.createCommit(options),
    recover: (processInfo) => realFactory.recover(processInfo),
  };

  service = await createChannelService({ config, transport: activeTransport, workerFactory, worktrees });
  await service!.start();
  const activeStore = await openChannelStore(stateDir);
  store = activeStore;
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });

  const createdThread = await createThreadWorkspaceBatch(
    { stateDir, transport: activeTransport, worktrees, store: activeStore, botUid: bot.uid },
    activeGroupId,
    "Shared concurrency workspace",
    {
      layout: "shared",
      worktreeNameSeed: "concurrency-smoke",
      projects: [
        { name: "alpha", repo: await realpath(repoAlpha) },
        { name: "beta", repo: await realpath(repoBeta) },
      ],
      background: "offline concurrency smoke workspace",
    },
    new AbortController().signal,
    (thread) => activeStore.ensureThreadListen({
      botUid: bot.uid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: activeGroupId,
      threadName: thread.name,
      listen: "off",
    }),
  );
  assert.equal(createdThread.ready, true, `shared temporary Git workspace creation failed: ${createdThread.error ?? "unknown"}`);
  const thread = createdThread.threads[0];
  assert.ok(thread, "shared workspace did not create its thread");
  const activeThreadTarget: ChatTarget = { channelType: 5, channelId: thread.thread.channelId };
  threadTarget = activeThreadTarget;
  threadShortId = thread.thread.shortId;
  const ownership = activeStore.getThreadWorkspace(bot.uid, activeThreadTarget);
  assert.ok(ownership, "persisted shared workspace ownership is missing");
  assert.equal(ownership.layout, "shared");
  if (ownership.entryPath === null) throw new Error("shared workspace entry path is missing");
  const entryPath = ownership.entryPath;
  const alphaProject = ownership.projects.find((project) => project.name === "alpha");
  const betaProject = ownership.projects.find((project) => project.name === "beta");
  if (!alphaProject?.path || !betaProject?.path) throw new Error("both shared project worktrees must exist");
  const alphaPath = alphaProject.path;
  const betaPath = betaProject.path;
  workspaceEvidence = { entryPath, alphaPath, betaPath };
  assert.ok(alphaPath.startsWith(`${entryPath}/`));
  assert.ok(betaPath.startsWith(`${entryPath}/`));
  assert.notEqual(alphaPath, betaPath);
  for (const path of [alphaPath, betaPath]) assert.equal(await realpath(path), path);
  const alphaWorktrees = await run(["git", "-C", repoAlpha, "worktree", "list", "--porcelain"]);
  const betaWorktrees = await run(["git", "-C", repoBeta, "worktree", "list", "--porcelain"]);
  assert.ok(alphaWorktrees.split("\n").includes(`worktree ${alphaPath}`));
  assert.ok(betaWorktrees.split("\n").includes(`worktree ${betaPath}`));

  await prepareSession(groupTarget, parentCwd);
  await prepareSession(activeThreadTarget, entryPath);
  const projectAlphaTarget: ChatTarget = { channelType: 1, channelId: "dm-project-alpha" };
  const projectBetaTarget: ChatTarget = { channelType: 1, channelId: "dm-project-beta" };
  const siblingLeft = join(root, "siblings", "left");
  const siblingRight = join(root, "siblings", "right");
  const sameCwd = join(root, "same-cwd");
  await prepareSession(projectAlphaTarget, alphaPath);
  await prepareSession(projectBetaTarget, betaPath);
  const siblingLeftTarget: ChatTarget = { channelType: 1, channelId: "dm-sibling-left" };
  const siblingRightTarget: ChatTarget = { channelType: 1, channelId: "dm-sibling-right" };
  await prepareSession(siblingLeftTarget, siblingLeft);
  await prepareSession(siblingRightTarget, siblingRight);
  const sameCwdOneTarget: ChatTarget = { channelType: 1, channelId: "dm-same-cwd-one" };
  const sameCwdTwoTarget: ChatTarget = { channelType: 1, channelId: "dm-same-cwd-two" };
  await prepareSession(sameCwdOneTarget, sameCwd);
  await prepareSession(sameCwdTwoTarget, sameCwd);
  const sameSessionOwnerTarget: ChatTarget = { channelType: 1, channelId: "dm-same-session-owner" };
  await prepareSession(sameSessionOwnerTarget, join(root, "same-session"));
  const independentTargets = [
    groupTarget, activeThreadTarget, projectAlphaTarget, projectBetaTarget, siblingLeftTarget,
    siblingRightTarget, sameCwdOneTarget, sameCwdTwoTarget, sameSessionOwnerTarget,
  ];
  const independentSessions = independentTargets.map((target) => {
    const binding = activeStore.getBinding(bot.uid, target);
    if (binding === null || !binding.sessionFile || !binding.sessionId) {
      throw new Error(`independent chat ${target.channelType}:${target.channelId} lacks its own real OMP session`);
    }
    return { sessionFile: binding.sessionFile, sessionId: binding.sessionId };
  });
  assert.equal(new Set(independentSessions.map((session) => session.sessionFile)).size, independentTargets.length);
  assert.equal(new Set(independentSessions.map((session) => session.sessionId)).size, independentTargets.length);


  for (const label of taskLabels) {
    if (!label.startsWith("delete-")) modelGates.set(label, Promise.withResolvers<void>());
  }

  const ancestorParentId = await submit(groupTarget, "ancestor-parent");
  await assertTaskStarted("ancestor-parent", parentCwd);
  assert.ok(entryPath.startsWith(`${parentCwd}/`), "the held parent cwd must be an ancestor of the owned shared workspace");

  const ancestorChildId = await submit(activeThreadTarget, "ancestor-child");
  await assertTaskStarted("ancestor-child", entryPath);
  assert.equal(taskState(ancestorParentId), "started");
  assert.ok(activeModelHolds.has("ancestor-parent"), "parent's real model request must remain held while its child runs");
  modelGates.get("ancestor-child")!.resolve();
  await taskCompleted(ancestorChildId, "ancestor-child");
  assert.equal(taskState(ancestorParentId), "started", "child completion must not wait for its held parent");
  assert.ok(activeModelHolds.has("ancestor-parent"));
  assert.ok(isAlive(taskStart("ancestor-parent").pid as number), "the actual parent OMP process must remain live through child completion");
  modelGates.get("ancestor-parent")!.resolve();
  await taskCompleted(ancestorParentId, "ancestor-parent");

  const reverseChildId = await submit(activeThreadTarget, "reverse-child");
  await assertTaskStarted("reverse-child", entryPath);
  const reverseParentId = await submit(groupTarget, "reverse-parent");
  await assertTaskStarted("reverse-parent", parentCwd);
  assert.equal(taskState(reverseChildId), "started");
  assert.ok(activeModelHolds.has("reverse-child"), "workspace child must remain held while its ancestor runs");
  modelGates.get("reverse-parent")!.resolve();
  await taskCompleted(reverseParentId, "reverse-parent");
  assert.equal(taskState(reverseChildId), "started", "ancestor completion must not wait for its held child");
  assert.ok(activeModelHolds.has("reverse-child"));
  assert.ok(isAlive(taskStart("reverse-child").pid as number), "the actual workspace child process must remain live through ancestor completion");

  const projectAlphaId = await submit(projectAlphaTarget, "project-alpha");
  const projectBetaId = await submit(projectBetaTarget, "project-beta");
  await assertTaskStarted("project-alpha", alphaPath);
  await assertTaskStarted("project-beta", betaPath);
  assert.ok(activeModelHolds.has("reverse-child") && activeModelHolds.has("project-alpha") && activeModelHolds.has("project-beta"), "entry and project model requests must overlap");
  modelGates.get("project-alpha")!.resolve();
  await taskCompleted(projectAlphaId, "project-alpha");
  assert.equal(taskState(reverseChildId), "started", "project completion must not wait for its held shared entry");
  assert.ok(activeModelHolds.has("reverse-child") && activeModelHolds.has("project-beta"));
  assert.ok(isAlive(taskStart("reverse-child").pid as number), "the actual shared-entry process must remain live through project completion");
  modelGates.get("reverse-child")!.resolve();
  await taskCompleted(reverseChildId, "reverse-child");

  const siblingLeftId = await submit(siblingLeftTarget, "sibling-left");
  const siblingRightId = await submit(siblingRightTarget, "sibling-right");
  await assertTaskStarted("sibling-left", siblingLeft);
  await assertTaskStarted("sibling-right", siblingRight);
  assert.ok(activeModelHolds.has("sibling-left") && activeModelHolds.has("sibling-right"), "unrelated sibling tasks must overlap behind their model gates");
  const sameCwdOneId = await submit(sameCwdOneTarget, "same-cwd-one");
  const sameCwdTwoId = await submit(sameCwdTwoTarget, "same-cwd-two");
  await assertTaskStarted("same-cwd-one", sameCwd);
  await assertTaskStarted("same-cwd-two", sameCwd);
  assert.ok(activeModelHolds.has("same-cwd-one") && activeModelHolds.has("same-cwd-two"), "independent sessions on the same cwd must overlap behind their model gates");
  for (const label of ["sibling-left", "sibling-right", "same-cwd-one", "same-cwd-two"]) {
    modelGates.get(label)!.resolve();
  }
  for (const [id, label] of [
    [siblingLeftId, "sibling-left"], [siblingRightId, "sibling-right"],
    [sameCwdOneId, "same-cwd-one"], [sameCwdTwoId, "same-cwd-two"],
  ] as const) await taskCompleted(id, label);

  const sameSessionOwnerId = await submit(sameSessionOwnerTarget, "same-session-owner");
  await assertTaskStarted("same-session-owner", join(root, "same-session"));
  const ownerBinding = activeStore.getBinding(bot.uid, sameSessionOwnerTarget);
  if (ownerBinding === null || ownerBinding.sessionFile === null || ownerBinding.sessionId === null) {
    throw new Error("same-session owner must have an authoritative OMP session file and session id");
  }
  const sameSessionWaiterTarget: ChatTarget = { channelType: 1, channelId: "dm-same-session-waiter" };
  activeStore.ensureBinding({ ...ownerBinding, ...sameSessionWaiterTarget });
  const sameSessionWaiterId = await submit(sameSessionWaiterTarget, "same-session-waiter");
  await waitFor("same-session task queued behind its exact session", () => taskState(sameSessionWaiterId) === "queued");
  const waitingFence = Promise.withResolvers<void>();
  setImmediate(waitingFence.resolve);
  await waitingFence.promise;
  assert.equal(perLabelModelCalls.get("same-session-waiter") ?? 0, 0, "same-session waiter must not issue a model request before owner settlement");
  assert.equal(audit.some((row) => row.kind === "task_start" && row.label === "same-session-waiter"), false);
  assert.equal(taskState(sameSessionOwnerId), "started");
  modelGates.get("same-session-owner")!.resolve();
  await taskCompleted(sameSessionOwnerId, "same-session-owner");
  await assertTaskStarted("same-session-waiter", join(root, "same-session"));
  modelGates.get("same-session-waiter")!.resolve();
  await taskCompleted(sameSessionWaiterId, "same-session-waiter");
  assert.equal(activeStore.getBinding(bot.uid, sameSessionWaiterTarget)?.sessionId, ownerBinding.sessionId, "forced binding must retain the exact same session id");

  // Exercise production cleanup while one real project worker holds its Git worktree.
  const blockedDeleteId = await submit(groupTarget, "delete-blocked");
  await waitForModelRequest("delete-blocked");
  await taskCompleted(blockedDeleteId, "delete-blocked");
  assert.ok(activeTransport.managementCalls.some((call) => call.action === "thread-delete" && call.shortId === thread.thread.shortId), "local host tool did not execute the thread-delete action");
  assert.equal(removeCalls.length, 0, "Git worktree removal ran while the shared beta project worker was active");
  assert.equal(await realpath(alphaPath), alphaPath, "busy delete must preserve the alpha worktree too");
  assert.equal(await realpath(betaPath), betaPath, "busy beta worktree must remain on disk");
  assert.ok(activeModelHolds.has("project-beta"), "beta project must still own its real model request during delete");

  modelGates.get("project-beta")!.resolve();
  await taskCompleted(projectBetaId, "project-beta");
  const allowedDeleteId = await submit(groupTarget, "delete-after");
  await waitForModelRequest("delete-after");
  await taskCompleted(allowedDeleteId, "delete-after");
  assert.equal(removeCalls.length, 2, "settled shared workspace must remove both actual Git worktrees");
  assert.ok(removeCalls.every((call) => call.activeHolds.length === 0), "actual Git removal ran while a model-gated worker still held a workspace");
  assert.equal(existsSync(alphaPath), false, "real Git remove must delete the alpha worktree directory");
  assert.equal(existsSync(betaPath), false, "real Git remove must delete the beta worktree directory");
  assert.equal(existsSync(entryPath), false, "empty shared entry directory must be removed");
  const remainingAlpha = await run(["git", "-C", repoAlpha, "worktree", "list", "--porcelain"]);
  const remainingBeta = await run(["git", "-C", repoBeta, "worktree", "list", "--porcelain"]);
  assert.equal(remainingAlpha.split("\n").includes(`worktree ${alphaPath}`), false, "Git must forget the removed alpha worktree");
  assert.equal(remainingBeta.split("\n").includes(`worktree ${betaPath}`), false, "Git must forget the removed beta worktree");

  for (const label of taskLabels) {
    const effects = audit.filter((row) => row.kind === "effect" && row.label === label);
    assert.equal(effects.length, 1, `${label} must produce exactly one real OMP assistant effect`);
    assert.equal(String(effects[0]?.text).includes(`CONCURRENCY_EFFECT:${label}`), true, `${label} effect evidence is missing its deterministic reply`);
  }
  assert.equal(modelRequests.length, taskLabels.length + 2, "local finite model served an unexpected number of requests");
  assert.equal(audit.filter((row) => row.kind === "session_overlap").length, 0, "different sessions overlapped unexpectedly");
  assert.equal(activeStore.getBinding(bot.uid, sameSessionWaiterTarget)?.sessionFile, ownerBinding.sessionFile, "forced binding must retain the exact same session file");
  assert.equal(perLabelModelCalls.get("delete-blocked"), 2, "blocked delete must receive the host-tool result and finish locally");
  assert.equal(perLabelModelCalls.get("delete-after"), 2, "safe delete must receive the host-tool result and finish locally");
  assert.equal(activeModelHolds.size, 0, "all explicit local model gates must be released");
  succeeded = true;
}

function restoreEnvironment(): void {
  for (const [key, value] of previousEnvironment) {
    if (value === undefined) delete Bun.env[key];
    else Bun.env[key] = value;
  }
  for (const [key, value] of previousProcessEnvironment) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
}

try {
  const baseline = await processTable();
  for (const row of baseline) subprocessBaseline.add(row.pid);
  await main();
} finally {
  for (const gate of modelGates.values()) gate.resolve();
  try {
    await captureOwnedSubprocesses().catch(() => {});
    if (service) {
      try {
        await bounded(service.stop(), "CoreService shutdown", 60_000);
      } catch (error) {
        for (const [pid, processInfo] of processes) {
          if (!isAlive(pid)) continue;
          const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "command="], { stdout: "pipe" });
          const command = await new Response(ps.stdout).text();
          await ps.exited;
          if (command.includes(root)) {
            try { process.kill(-processInfo.pgid, "SIGKILL"); } catch (killError) {
              if ((killError as NodeJS.ErrnoException).code !== "ESRCH") throw killError;
            }
          }
        }
        await bounded(service.stop(), "CoreService emergency shutdown", 5_000).catch(() => {});
        if (!succeeded) console.error(`CoreService cleanup needed emergency worker termination: ${String(error)}`);
      }
    }
    stopModelServer?.();
    db?.close();
    store?.close();
    const remainingRows = await processTable().catch(() => []);
    const remaining = [...ownedSubprocesses].filter(([pid, identity]) => remainingRows.some((row) => row.pid === pid && row.identity === identity));
    const liveWorkers = [...processes.keys()].filter(isAlive);
    if (succeeded) {
      assert.equal(liveWorkers.length, 0, `real OMP worker PIDs remain alive: ${liveWorkers.join(",")}`);
      assert.equal(remaining.length, 0, `child/watchdog subprocesses remain alive: ${remaining.map(([pid]) => pid).join(",")}`);
      const starts = audit.filter((row) => row.kind === "process_start").length;
      const exits = audit.filter((row) => row.kind === "process_exit").length;
      assert.equal(exits, starts, "every real OMP child must have a confirmed exit");
      console.log(`METRIC omp_command=${ompCommand}`);
      console.log(`METRIC model_requests=${modelRequests.length}`);
      console.log(`METRIC task_effects=${audit.filter((row) => row.kind === "effect").length}`);
      console.log(`METRIC real_omp_process_starts=${starts}`);
      console.log(`METRIC confirmed_process_exits=${exits}`);
      console.log(`METRIC session_holds=${audit.filter((row) => row.kind === "session_hold").length}`);
      console.log(`METRIC session_overlaps=${audit.filter((row) => row.kind === "session_overlap").length}`);
      console.log(`METRIC busy_worktree_remove_calls=${removeCalls.filter((call) => call.activeHolds.length > 0).length}`);
      console.log(`METRIC safe_git_worktree_remove_calls=${removeCalls.length}`);
      console.log(`METRIC residual_owned_subprocesses=${remaining.length}`);
      if (!workspaceEvidence) throw new Error("successful concurrency smoke lacks shared workspace evidence");
      console.log(`EVIDENCE ${JSON.stringify({
        ...workspaceEvidence,
        modelRequestLabels: modelRequests.map((request) => request.label),
        managementActions: transport!.managementCalls.map((call) => call.action),
        removeCalls,
        taskStarts: audit.filter((row) => row.kind === "task_start").map((row) => ({
          label: row.label, pid: row.pid, cwd: row.cwd, sessionFile: row.sessionFile, sessionId: row.sessionId,
        })),
        processStarts: audit.filter((row) => row.kind === "process_start"),
        sessionHolds: audit.filter((row) => row.kind === "session_hold"),
        sessionReleases: audit.filter((row) => row.kind === "session_release"),
        processExits: audit.filter((row) => row.kind === "process_exit"),
        effects: audit.filter((row) => row.kind === "effect").map((row) => ({ label: row.label, pid: row.pid })),
      })}`);
    }
  } finally {
    restoreEnvironment();
    await rm(root, { recursive: true, force: true });
  }
}
