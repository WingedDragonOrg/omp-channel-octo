import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { recvIngressSourceId } from "../../src/inbound/ingress-types.ts";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Offline black-box acceptance: real CLI + encrypted Octo WebSocket + real OMP RPC child + real
// linked Git worktrees. The wt executable below is an explicit local wt-compatible runner (real
// `git merge`, controlled pre-merge gate), NOT the installed Worktrunk CLI. No provider, bot
// registration, resident daemon, or live Octo service is reachable from the CLI's isolated env.
const checkout = resolve(import.meta.dir, "../..");
const ompCommand = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(ompCommand, "Install OMP or set OMP_REAL_COMMAND to its executable");

const root = await realpath(await mkdtemp(join(tmpdir(), "oco-ingress-acceptance-")));
const bin = join(root, "bin");
const stateDir = join(root, "state");
const sessionsDir = join(root, "sessions");
const repo = join(root, "repo");
const mainWorktree = repo;
const worktreesDir = join(root, "worktrees");
const gateDir = join(root, "gates");
const auditPath = join(root, "wt-audit.tsv");
const configPath = join(root, "config.json");
const localAgentDir = join(root, "agent");
const nativeConfig = join(root, "native.yml");
const ompWrapperPath = join(bin, "omp-controlled");
const ompStartGate = join(root, "omp-start-gate");
const stateDbPath = join(stateDir, "channel.sqlite");
const ownerGroup = "ingress-fixture-group";
const children: Bun.Subprocess[] = [];
const outputPromises: Promise<string[]>[] = [];
const deliveries: Array<{ connection: FixtureConnection; messageId: string; label: string }> = [];
const modelRequests: ModelRequest[] = [];
const modelGates = new Map<string, Deferred>();
let modelError: string | undefined;
let mediaRequests = 0;
let mediaGate = deferred();
let database: Database | undefined;
let fixture: OctoProtocolFixture | undefined;
let localServer: ReturnType<typeof Bun.serve> | undefined;
let active: CliRun | undefined;
let messageSequence = 5_000;
let succeeded = false;

interface Deferred {
  promise: Promise<void>;
  resolve(): void;
}
interface ModelRequest {
  label: string;
  path: string;
  body: string;
  call: number;
}
interface ChatTarget {
  channelType: 1 | 2 | 5;
  channelId: string;
}
interface CliRun {
  child: Bun.Subprocess;
  connection: FixtureConnection;
}
interface IngressJobRow {
  source_id: string;
  source_kind: string;
  ack_ready: number;
  state: string;
  lane: string;
  operation_checkpoint_json: string | null;
  prepared_text: string | null;
  spool_paths_json: string | null;
  consumer_kind: string | null;
  consumer_id: string | null;
}
interface HostMergeRow {
  id: string;
  source_id: string;
  phase: string;
  pid: number | null;
  pgid: number | null;
  outcome_kind: string | null;
  resource_cwds: string | null;
}
interface TaskRow {
  session_id: string | null;
  state: string;
  result_text: string | null;
}
interface GateProcess {
  branch: string;
  attempt: number;
  runnerPid: number;
  hookPid: number;
  sleeperPid: number;
  pgid: number;
}
interface RunnerEvent {
  kind: "START" | "MERGED";
  branch: string;
  attempt: number;
  pid: number;
  detail: string;
}
interface ProcessRow {
  pid: number;
  ppid: number;
  pgid: number;
  command: string;
}

function deferred(): Deferred {
  const pending = Promise.withResolvers<void>();
  return { promise: pending.promise, resolve: () => pending.resolve() };
}

async function waitUntil(label: string, predicate: () => boolean, timeoutMs = 20_000): Promise<void> {
  await waitForFixture(() => {
    if (modelError !== undefined) throw new Error(`Local fixture failed: ${modelError}`);
    return predicate() ? true : undefined;
  }, timeoutMs, label);
}

async function waitValue<T>(label: string, read: () => T | null | undefined, timeoutMs = 20_000): Promise<T> {
  return await waitForFixture(() => {
    if (modelError !== undefined) throw new Error(`Local fixture failed: ${modelError}`);
    const value = read();
    return value === null || value === undefined ? undefined : value;
  }, timeoutMs, label);
}
async function waitUntilAsync(label: string, predicate: () => Promise<boolean>, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (modelError !== undefined) throw new Error(`Local fixture failed: ${modelError}`);
    if (await predicate()) return;
    const tick = Promise.withResolvers<void>();
    setTimeout(() => tick.resolve(undefined), 5);
    await tick.promise;
  }
  throw new Error(`${label} timed out after ${timeoutMs}ms`);
}

async function waitValueAsync<T>(
  label: string,
  read: () => Promise<T | null | undefined>,
  timeoutMs = 20_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (modelError !== undefined) throw new Error(`Local fixture failed: ${modelError}`);
    const value = await read();
    if (value !== null && value !== undefined) return value;
    const tick = Promise.withResolvers<void>();
    setTimeout(() => tick.resolve(undefined), 5);
    await tick.promise;
  }
  throw new Error(`${label} timed out after ${timeoutMs}ms`);
}

async function run(argv: string[], cwd = root, env: NodeJS.ProcessEnv = setupEnv): Promise<string> {
  const child = Bun.spawn(argv, {
    cwd,
    env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  assert.equal(code, 0, `${argv.join(" ")} failed (cwd=${cwd}): ${stderr}`);
  return stdout.trim();
}

function messageText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}

function targetMessage(message: FixtureSentMessage, target: ChatTarget): boolean {
  return Number(message.body.channel_type) === target.channelType && message.body.channel_id === target.channelId;
}

function sentText(target: ChatTarget, after = 0): string[] {
  return fixture!.sentMessages.slice(after)
    .filter((message) => targetMessage(message, target) && messageText(message).length > 0)
    .map(messageText);
}

async function waitReply(
  target: ChatTarget,
  after: number,
  label: string,
  predicate: (text: string) => boolean,
  timeoutMs = 20_000,
): Promise<string> {
  return await waitValue(label, () => sentText(target, after).find(predicate), timeoutMs);
}

function dbQuery<T>(sql: string, ...values: (string | number | null)[]): T | null {
  assert.ok(database, "SQLite observation connection is not open");
  return database.query(sql).get(...values) as T | null;
}

function ingressJob(target: ChatTarget, messageId: string): IngressJobRow | null {
  return dbQuery<IngressJobRow>(
    "SELECT source_id, source_kind, ack_ready, state, lane, operation_checkpoint_json, prepared_text, spool_paths_json, consumer_kind, consumer_id FROM ingress_jobs WHERE bot_uid = ? AND source_kind = 'recv' AND source_id = ?",
    fixture!.botUid,
    recvIngressSourceId(target.channelType, target.channelId, messageId),
  );
}

function hostMerge(messageId: string): HostMergeRow | null {
  return dbQuery<HostMergeRow>(
    "SELECT id, source_id, phase, pid, pgid, outcome_kind, resource_cwds FROM host_merge_leases WHERE bot_uid = ? AND channel_type = 5 AND source_id = ? ORDER BY created_at DESC LIMIT 1",
    fixture!.botUid,
    messageId,
  );
}

function hostMergeCount(messageId: string): number {
  return Number(dbQuery<{ count: number }>(
    "SELECT COUNT(*) AS count FROM host_merge_leases WHERE bot_uid = ? AND channel_type = 5 AND source_id = ?",
    fixture!.botUid,
    messageId,
  )?.count ?? 0);
}

function taskRow(messageId: string): TaskRow | null {
  return dbQuery<TaskRow>("SELECT session_id, state, result_text FROM tasks WHERE message_id = ?", messageId);
}

function ackCount(connection: FixtureConnection, messageId: string): number {
  return connection.recvAcks.filter((ack) => ack.messageId === messageId).length;
}

async function waitAck(connection: FixtureConnection, messageId: string, label: string): Promise<void> {
  await waitUntil(`${label}: RECVACK`, () => ackCount(connection, messageId) >= 1, 15_000);
  assert.equal(ackCount(connection, messageId), 1, `${label} must receive exactly one message-specific ACK`);
}

function nextMessageId(): string {
  messageSequence += 1;
  return String(messageSequence);
}

function send(
  connection: FixtureConnection,
  target: ChatTarget,
  text: string,
  label: string,
  payload?: Record<string, unknown>,
): string {
  const messageId = nextMessageId();
  fixture!.sendInbound(connection, {
    fromUid: fixture!.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text,
    messageId,
    ...(target.channelType === 1 ? {} : { mention: { uids: [fixture!.botUid] } }),
    ...(payload === undefined ? {} : { payload }),
  });
  deliveries.push({ connection, messageId, label });
  return messageId;
}

function replay(
  connection: FixtureConnection,
  target: ChatTarget,
  text: string,
  messageId: string,
): void {
  fixture!.sendInbound(connection, {
    fromUid: fixture!.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text,
    messageId,
    ...(target.channelType === 1 ? {} : { mention: { uids: [fixture!.botUid] } }),
  });
}

async function waitReplayAck(connection: FixtureConnection, messageId: string, label: string): Promise<void> {
  const before = ackCount(connection, messageId);
  await waitUntil(`${label}: replay RECVACK`, () => ackCount(connection, messageId) > before, 15_000);
  assert.equal(ackCount(connection, messageId), before + 1, `${label} replay must receive exactly one message-specific ACK`);
}

function binding(target: ChatTarget): { session_id: string; cwd: string; session_file: string | null; cwd_pinned: number } | null {
  return dbQuery(
    "SELECT session_id, cwd, session_file, cwd_pinned FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
    fixture!.botUid,
    target.channelType,
    target.channelId,
  );
}

async function bindSession(connection: FixtureConnection, target: ChatTarget, cwd: string, label: string): Promise<void> {
  const before = fixture!.sentMessages.length;
  const id = send(connection, target, `/new ${cwd}`, `bind-${label}`);
  await waitAck(connection, id, `bind-${label}`);
  const expectedCwd = await realpath(cwd);
  const row = await waitValue(`persisted ${label} binding`, () => {
    const current = binding(target);
    return current?.cwd === expectedCwd ? current : undefined;
  });
  assert.ok(row.session_file, `${label} must persist its session file`);
  assert.ok(await Bun.file(row.session_file).exists(), `${label} session must exist on disk`);
  const reply = await waitReply(target, before, `visible /new receipt ${label}`, (text) => text.startsWith("已创建会话 "), 10_000);
  if (target.channelType === 5) {
    const writable = new Database(stateDbPath);
    try {
      assert.equal(writable.query(
        "UPDATE chats SET cwd_pinned = 1 WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
      ).run(fixture!.botUid, target.channelType, target.channelId).changes, 1);
    } finally {
      writable.close();
    }
  }
  console.log(`BIND ${label} ${JSON.stringify({ cwd: row.cwd, sessionFileExists: true, ackCount: ackCount(connection, id), reply })}`);
}

async function launch(): Promise<CliRun> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "start", "--config", configPath], {
    cwd: root,
    env: setupEnv,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  outputPromises.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]));
  const connection = await fixture!.waitForConnection(fixture!.connections.length + 1, 20_000);
  await waitForFixture(() => connection.ready ? true : undefined, 10_000, "CLI did not authenticate its encrypted Octo connection");
  assert.equal(connection.connect?.uid, fixture!.botUid, "CLI authenticated the wrong fixture Bot");
  if (!database) database = new Database(stateDbPath, { readonly: true });
  const running = { child, connection };
  active = running;
  console.log(`CLI_READY ${JSON.stringify({ pid: child.pid, connection: fixture!.connections.indexOf(connection) + 1, encrypted: Boolean(connection.aesKey && connection.aesIv) })}`);
  return running;
}

async function childExit(child: Bun.Subprocess, timeoutMs = 30_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      child.exited,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`CLI process ${child.pid} did not exit within ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function stopActive(mode: "SIGTERM" | "SIGKILL"): Promise<number> {
  assert.ok(active, "No active CLI to stop");
  const run = active;
  if (run.child.exitCode === null) run.child.kill(mode);
  const code = await childExit(run.child, mode === "SIGTERM" ? 30_000 : 15_000);
  if (mode === "SIGTERM") assert.equal(code, 0, "graceful CLI SIGTERM must exit cleanly");
  active = undefined;
  console.log(`CLI_STOP ${JSON.stringify({ pid: run.child.pid, mode, exitCode: code })}`);
  return code;
}

function modelLabel(body: string): string {
  const markers = [...body.matchAll(/INGRESS_(?:HOLD|QUERY|MEDIA):([A-Za-z0-9_-]+)/gu)];
  return markers.at(-1)?.[1] ?? "unlabelled";
}

function localModelResponse(label: string, call: number): Response {
  const id = `ingress-${label}-${call}`;
  const chunks = [
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: { role: "assistant", content: `INGRESS_REPLY:${label}` }, finish_reason: null }],
    },
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 9, completion_tokens: 3, total_tokens: 12 },
    },
  ];
  return new Response(chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function holdModel(label: string): void {
  assert.ok(!modelGates.has(label), `model gate ${label} already exists`);
  modelGates.set(label, deferred());
}

function releaseModel(label: string): void {
  modelGates.get(label)?.resolve();
}

function modelCallCount(label: string): number {
  return modelRequests.filter((request) => request.label === label).length;
}

async function waitModel(label: string, call = 1): Promise<ModelRequest> {
  return await waitValue(`local model request ${label} #${call}`, () => modelRequests.filter((request) => request.label === label)[call - 1]);
}

async function prepareRepo(branches: readonly string[]): Promise<Map<string, string>> {
  await mkdir(worktreesDir, { recursive: true });
  await mkdir(join(root, "git-hooks"), { recursive: true });
  await run(["git", "init", "-q", "--initial-branch=main", repo]);
  await run(["git", "-C", repo, "config", "user.name", "Ingress Acceptance"]);
  await run(["git", "-C", repo, "config", "user.email", "ingress@example.invalid"]);
  await run(["git", "-C", repo, "config", "core.hooksPath", join(root, "git-hooks")]);
  await writeFile(join(repo, "baseline.txt"), "offline ingress acceptance baseline\n");
  await writeFile(join(repo, "concurrent-merge.txt"), "base merge content\n");
  await run(["git", "-C", repo, "add", "baseline.txt", "concurrent-merge.txt"]);
  await run(["git", "-C", repo, "commit", "-q", "-m", "ingress fixture baseline"]);

  const paths = new Map<string, string>();
  for (const branch of branches) {
    const path = join(worktreesDir, branch);
    await run(["git", "-C", repo, "worktree", "add", "-q", "-b", branch, path, "main"]);
    const conflictingBranch = branch === "ingress-waiting-first" || branch === "ingress-waiting-second";
    const sourceFile = conflictingBranch ? "concurrent-merge.txt" : `${branch}.txt`;
    const sourceText = conflictingBranch ? `change from ${branch}\n` : `source commit for ${branch}\n`;
    await writeFile(join(path, sourceFile), sourceText);
    await run(["git", "-C", path, "add", sourceFile]);
    await run(["git", "-C", path, "commit", "-q", "-m", "ingress fixture source " + branch]);
    paths.set(branch, await realpath(path));
  }
  return paths;
}

async function installWorktrunkCompatibleRunner(): Promise<string> {
  const hookPath = join(root, "controlled-pre-merge-hook.sh");
  const runnerPath = join(bin, "wt");
  await writeFile(hookPath, [
    "#!/bin/sh",
    "set -eu",
    "branch=$1",
    "attempt=$2",
    "prefix=$INGRESS_GATE_DIR/$branch.$attempt",
    "sleep 3600 &",
    "sleeper=$!",
    "printf '%s\\n' \"$$\" > \"$prefix.hook.pid\"",
    "printf '%s\\n' \"$sleeper\" > \"$prefix.sleep.pid\"",
    "wait \"$sleeper\" || true",
  ].join("\n"), { mode: 0o700 });
  await writeFile(runnerPath, [
    "#!/bin/sh",
    "set -eu",
    'if [ "${1:-}" = "--version" ]; then printf "wt-compatible ingress fixture (not Worktrunk)\\n"; exit 0; fi',
    'if [ "${1:-}" = "config" ] && [ "${2:-}" = "state" ] && [ "${3:-}" = "default-branch" ]; then printf "main\\n"; exit 0; fi',
    'if [ "${1:-}" != "merge" ]; then printf "unsupported wt-compatible command\\n" >&2; exit 64; fi',
    "shift",
    "target=",
    'while [ "$#" -gt 0 ]; do',
    '  case "$1" in',
    '    --format|--stage) shift 2 ;;',
    '    --*) shift ;;',
    '    *) target=$1; shift ;;',
    "  esac",
    "done",
    '[ -z "$target" ] || [ "$target" = "main" ] || { printf "unexpected merge target\\n" >&2; exit 65; }',
    "branch=$(git branch --show-current)",
    "source=$(pwd -P)",
    'count_path="$INGRESS_GATE_DIR/$branch.count"',
    'if [ -f "$count_path" ]; then attempt=$(($(cat "$count_path") + 1)); else attempt=1; fi',
    'printf "%s\\n" "$attempt" > "$count_path"',
    'printf "START\\t%s\\t%s\\t%s\\t%s\\n" "$branch" "$attempt" "$$" "$source" >> "$INGRESS_AUDIT"',
    'prefix="$INGRESS_GATE_DIR/$branch.$attempt"',
    'printf "%s\\n" "$$" > "$prefix.runner.pid"',
    ': > "$prefix.started"',
    'if [ -f "$prefix.hold" ]; then',
    '  sh "$INGRESS_HOOK_SCRIPT" "$branch" "$attempt" &',
    '  hook=$!',
    '  printf "%s\\n" "$hook" > "$prefix.runner-child.pid"',
    '  wait "$hook"',
    "fi",
    'git -C "$INGRESS_MAIN_WORKTREE" merge --no-ff --no-edit -m "ingress fixture merge $branch" "$branch" >/dev/null',
    'commit=$(git -C "$INGRESS_MAIN_WORKTREE" rev-parse HEAD)',
    'git -C "$INGRESS_MAIN_WORKTREE" worktree remove --force "$source" >/dev/null 2>&1',
    'git -C "$INGRESS_MAIN_WORKTREE" branch -d "$branch" >/dev/null 2>&1',
    'printf "MERGED\\t%s\\t%s\\t%s\\t%s\\n" "$branch" "$attempt" "$$" "$commit" >> "$INGRESS_AUDIT"',
    'printf "{\\"branch\\":\\"%s\\",\\"target\\":\\"main\\",\\"squashed\\":false,\\"rebased\\":false,\\"committed\\":true,\\"removed\\":true}\\n" "$branch"',
  ].join("\n"), { mode: 0o700 });
  return hookPath;
}

async function installLocalFilesAndEnvironment(): Promise<void> {
  await mkdir(bin, { recursive: true });
  await mkdir(gateDir, { recursive: true });
  await mkdir(localAgentDir, { recursive: true });
  await mkdir(join(root, "xdg-config"), { recursive: true });
  await writeFile(join(root, "real-omp-command"), `${ompCommand}\n`);
  await writeFile(ompWrapperPath, [
    "#!/bin/sh",
    "set -eu",
    'root=$(CDPATH= cd "$(dirname "$0")/.." && pwd -P)',
    'gate="$root/omp-start-gate"',
    'if [ -f "$gate.armed" ] && mv "$gate.armed" "$gate.started" 2>/dev/null; then',
    '  printf "%s\\n" "$$" > "$gate.pid"',
    '  while [ ! -e "$gate.release" ]; do',
    '    [ ! -e "$gate.abort" ] || exit 97',
    "    /bin/sleep 0.05",
    "  done",
    "fi",
    'real_command=$(/bin/cat "$root/real-omp-command")',
    'exec "$real_command" "$@"',
  ].join("\n"), { mode: 0o700 });
  await mkdir(join(root, "xdg-cache"), { recursive: true });
  await mkdir(join(root, "xdg-data"), { recursive: true });
  await mkdir(join(root, "worktrunk-config"), { recursive: true });
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(bin, command), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  }
  const provider = {
    baseUrl: `http://127.0.0.1:${localServer!.port}/v1`,
    apiKey: "local-only-placeholder",
    api: "openai-completions",
    models: [{
      id: "probe",
      name: "Ingress Offline Probe",
      reasoning: false,
      input: ["text"],
      contextWindow: 32768,
      maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }],
  };
  await writeFile(join(localAgentDir, "models.yml"), JSON.stringify({ providers: { "local-ingress": provider } }));
  await writeFile(nativeConfig, "autoRetry:\n  enabled: false\n");
  const hookPath = await installWorktrunkCompatibleRunner();

  const path = `${bin}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`;
  setupEnv = {
    HOME: root,
    TMPDIR: root,
    PATH: path,
    LANG: "C",
    TZ: "UTC",
    XDG_CONFIG_HOME: join(root, "xdg-config"),
    XDG_CACHE_HOME: join(root, "xdg-cache"),
    XDG_DATA_HOME: join(root, "xdg-data"),
    WORKTRUNK_CONFIG_DIR: join(root, "worktrunk-config"),
    WORKTRUNK_CONFIG: join(root, "worktrunk-config", "config.toml"),
    PI_CODING_AGENT_DIR: localAgentDir,
    PI_NO_TITLE: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_TERMINAL_PROMPT: "0",
    INGRESS_GATE_DIR: gateDir,
    INGRESS_AUDIT: auditPath,
    INGRESS_HOOK_SCRIPT: hookPath,
    INGRESS_MAIN_WORKTREE: mainWorktree,
  };
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture!.apiUrl,
    botToken: fixture!.botToken,
    ownerId: fixture!.ownerUid,
    allowedGroupIds: [ownerGroup],
    stateDir,
    sessionsDir,
    ompCommand: ompWrapperPath,
    ompExtraArgs: [
      "--model", "local-ingress/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools",
      "--config", nativeConfig,
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
    attachments: { enabled: true, maxBytes: 1_048_576 },
  }), { mode: 0o600 });
}

let setupEnv: NodeJS.ProcessEnv = {};

async function makeGate(branch: string, attempt = 1): Promise<void> {
  const prefix = join(gateDir, `${branch}.${attempt}`);
  await writeFile(`${prefix}.hold`, "held\n");
}

async function gateProcess(branch: string, attempt = 1): Promise<GateProcess> {
  const prefix = join(gateDir, `${branch}.${attempt}`);
  await waitUntil(`${branch} pre-merge gate started`, () =>
    existsSync(`${prefix}.started`)
    && existsSync(`${prefix}.runner.pid`)
    && existsSync(`${prefix}.hook.pid`)
    && existsSync(`${prefix}.sleep.pid`), 30_000);
  const runnerPid = Number((await readFile(`${prefix}.runner.pid`, "utf8")).trim());
  const hookPid = Number((await readFile(`${prefix}.hook.pid`, "utf8")).trim());
  const sleeperPid = Number((await readFile(`${prefix}.sleep.pid`, "utf8")).trim());
  assert.ok([runnerPid, hookPid, sleeperPid].every((pid) => Number.isSafeInteger(pid) && pid > 0), `${branch} gate must record its owned PIDs`);
  const pgid = await processGroupId(runnerPid);
  assert.equal(await processGroupId(hookPid), pgid, `${branch} hook must share the guarded process group`);
  assert.equal(await processGroupId(sleeperPid), pgid, `${branch} hook descendant must share the guarded process group`);
  return { branch, attempt, runnerPid, hookPid, sleeperPid, pgid };
}

async function releaseGate(gate: GateProcess): Promise<void> {
  assert.ok(isAlive(gate.sleeperPid), `${gate.branch} explicit gate process is already gone`);
  process.kill(gate.sleeperPid, "SIGTERM");
}

function runnerEvents(): RunnerEvent[] {
  if (!existsSync(auditPath)) return [];
  return readFileSync(auditPath, "utf8").split("\n").filter(Boolean).map((line) => {
    const [kind, branch, attempt, pid, detail] = line.split("\t");
    return { kind: kind as RunnerEvent["kind"], branch: branch!, attempt: Number(attempt), pid: Number(pid), detail: detail ?? "" };
  });
}

function runnerCount(branch: string, kind: RunnerEvent["kind"]): number {
  return runnerEvents().filter((event) => event.branch === branch && event.kind === kind).length;
}

async function waitRunnerGate(branch: string, attempt = 1): Promise<GateProcess> {
  await waitUntil(`${branch} runner invocation`, () => runnerCount(branch, "START") >= attempt, 30_000);
  return await gateProcess(branch, attempt);
}

async function processGroupId(pid: number): Promise<number> {
  const value = await run(["/bin/ps", "-p", String(pid), "-o", "pgid="]);
  const pgid = Number(value);
  assert.ok(Number.isSafeInteger(pgid) && pgid > 0, `could not read process group for owned PID ${pid}`);
  return pgid;
}

async function processGroupMembers(pgid: number): Promise<number[]> {
  const output = await run(["/bin/ps", "-axo", "pid=,pgid="]);
  return output.split("\n").flatMap((line) => {
    const [pidText, pgidText] = line.trim().split(/\s+/u);
    const pid = Number(pidText);
    return Number(pgidText) === pgid && Number.isSafeInteger(pid) ? [pid] : [];
  });
}

async function waitGroupGone(gate: GateProcess): Promise<void> {
  await waitUntilAsync(
    `${gate.branch} entire pre-merge process group exited`,
    async () => (await processGroupMembers(gate.pgid)).length === 0,
    15_000,
  );
  assert.deepEqual(await processGroupMembers(gate.pgid), [], `${gate.branch} guarded hook descendants must all exit`);
}

function isAlive(pid: number): boolean {
  assert.ok(Number.isSafeInteger(pid) && pid > 0, `refusing to signal invalid PID ${pid}`);
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function waitPidsGone(gate: GateProcess): Promise<void> {
  await waitUntilAsync(`${gate.branch} tracked hook PIDs exited`, async () =>
    !isAlive(gate.runnerPid)
    && !isAlive(gate.hookPid)
    && !isAlive(gate.sleeperPid)
    && (await processGroupMembers(gate.pgid)).length === 0, 15_000);
  await waitGroupGone(gate);
}
async function waitTaskStarted(messageId: string, label: string): Promise<void> {
  await waitUntil(`${label} task starts`, () => taskRow(messageId)?.state === "started", 20_000);
}

async function waitTaskCompleted(messageId: string, label: string): Promise<void> {
  await waitUntil(`${label} task completes`, () => {
    const task = taskRow(messageId);
    if (task && ["failed", "cancelled", "interrupted"].includes(task.state)) {
      throw new Error(`${label} task ended as ${task.state}: ${task.result_text ?? ""}`);
    }
    return task?.state === "completed";
  }, 45_000);
}

async function mainHead(): Promise<string> {
  return await run(["git", "-C", mainWorktree, "rev-parse", "HEAD"]);
}

async function mainCommitCount(): Promise<number> {
  return Number(await run(["git", "-C", mainWorktree, "rev-list", "--count", "main"]));
}

async function assertWaitingIngress(target: ChatTarget, messageId: string, label: string): Promise<IngressJobRow> {
  const row = await waitValue(`${label} durable ACKed ingress job`, () => {
    const current = ingressJob(target, messageId);
    return current && Number(current.ack_ready) === 1 ? current : undefined;
  });
  assert.ok(row.operation_checkpoint_json, `${label} waiting operation must keep its ingress checkpoint`);
  return row;
}

async function createUnknownLease(holder: ChatTarget, holderCwd: string): Promise<void> {
  assert.ok(database);
  database.close();
  database = undefined;
  const writer = new Database(stateDbPath);
  const now = Date.now();
  writer.query(`
    INSERT INTO host_merge_leases (
      id, bot_uid, channel_type, channel_id, source_id, operation_kind, cwd, resource_cwds,
      phase, pid, pgid, process_identity, source_text, operator_note, sender_id, message_seq,
      receipt_outbox_id, outcome_kind, outcome_json, created_at, updated_at
    ) VALUES (?, ?, 5, ?, ?, 'merge', ?, ?, 'unknown', NULL, NULL, NULL, ?, '', ?, 1, NULL, NULL, NULL, ?, ?)
  `).run(
    "ingress-benchmark-injected-unknown",
    fixture!.botUid,
    holder.channelId,
    "injected-unknown-operation",
    holderCwd,
    JSON.stringify([holderCwd, mainWorktree]),
    "/thread merge",
    fixture!.ownerUid,
    now,
    now,
  );
  writer.close();
  database = new Database(stateDbPath, { readonly: true });
  const row = dbQuery<HostMergeRow>("SELECT id, source_id, phase, pid, pgid, outcome_kind, resource_cwds FROM host_merge_leases WHERE id = ?", "ingress-benchmark-injected-unknown");
  assert.equal(row?.phase, "unknown", "test must inject a durable merge outcome whose execution result is unknown");
  console.log(`UNKNOWN_INJECTION ${JSON.stringify({ channel: holder.channelId, resources: JSON.parse(row!.resource_cwds!) })}`);
}

function safeSegment(value: string): string {
  const cleaned = value.replace(/[^A-Za-z0-9._-]/gu, "_").replace(/^\.+/u, "");
  return (cleaned.length === 0 ? "unknown" : cleaned).slice(0, 120);
}

async function filesUnder(directory: string): Promise<string[]> {
  if (!existsSync(directory)) return [];
  const files: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await filesUnder(path));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}


async function setupAndRun(): Promise<void> {
  const branches = [
    "ingress-waiting-first", "ingress-waiting-second", "ingress-cancel-spawned", "ingress-cancel-waiter",
    "ingress-graceful-wait", "ingress-sigkill-wait", "ingress-unknown-holder", "ingress-unknown-candidate",
  ];
  const paths = await prepareRepo(branches);
  const groupTarget = (shortId: string): ChatTarget => ({ channelType: 5, channelId: `${ownerGroup}____${shortId}` });
  const mainChat: ChatTarget = { channelType: 1, channelId: fixture!.ownerUid };
  const waitingFirstChat = groupTarget("waiting-first");
  const waitingSecondChat = groupTarget("waiting-second");
  const cancelSpawnedChat = groupTarget("cancel-spawned");
  const cancelWaiterChat = groupTarget("cancel-waiter");
  const gracefulChat = groupTarget("graceful-wait");
  const sigkillChat = groupTarget("sigkill-wait");
  const mediaChat = groupTarget("media-spool");
  const queryChat = groupTarget("independent-query");
  const unknownHolderChat = groupTarget("unknown-holder");
  const unknownNewChat = groupTarget("unknown-new-recovery");
  const unknownCandidateChat = groupTarget("unknown-candidate");

  const queryCwd = join(root, "query-cwd");
  const mediaCwd = join(root, "media-cwd");
  await mkdir(queryCwd, { recursive: true });
  await mkdir(mediaCwd, { recursive: true });
  const unknownNewCwd = join(root, "unknown-new-recovery-cwd");
  await mkdir(unknownNewCwd, { recursive: true });

  const initialCommitCount = await mainCommitCount();
  assert.equal(initialCommitCount, 1, "fixture main starts with exactly its baseline commit");
  for (const branch of [
    "ingress-waiting-first", "ingress-waiting-second",
    "ingress-cancel-spawned", "ingress-cancel-waiter",
    "ingress-graceful-wait", "ingress-sigkill-wait", "ingress-unknown-candidate",
  ]) {
    await makeGate(branch);
  }

  const first = await launch();
  await bindSession(first.connection, mainChat, mainWorktree, "main-resource");
  await bindSession(first.connection, waitingFirstChat, paths.get("ingress-waiting-first")!, "waiting-first");
  await bindSession(first.connection, waitingSecondChat, paths.get("ingress-waiting-second")!, "waiting-second");
  await bindSession(first.connection, cancelSpawnedChat, paths.get("ingress-cancel-spawned")!, "cancel-spawned");
  await bindSession(first.connection, cancelWaiterChat, paths.get("ingress-cancel-waiter")!, "cancel-waiter");
  await bindSession(first.connection, gracefulChat, paths.get("ingress-graceful-wait")!, "graceful-wait");
  await bindSession(first.connection, sigkillChat, paths.get("ingress-sigkill-wait")!, "sigkill-wait");
  await bindSession(first.connection, mediaChat, mediaCwd, "media-spool");
  await bindSession(first.connection, queryChat, queryCwd, "independent-query");
  await bindSession(first.connection, unknownCandidateChat, paths.get("ingress-unknown-candidate")!, "unknown-candidate");

  await concurrentMergeOutcomeFlow(first, waitingFirstChat, waitingSecondChat, queryChat, paths);
  await waitingMergeRecoveryFlow("graceful", gracefulChat, gracefulChat, paths.get("ingress-graceful-wait")!);
  await waitingMergeRecoveryFlow("sigkill", sigkillChat, sigkillChat, paths.get("ingress-sigkill-wait")!);
  await mediaSpoolRecoveryFlow(mediaChat, queryChat);
  await cancelSpawnedMergeFlow(active!, cancelSpawnedChat, cancelWaiterChat, queryChat, paths);
  await unknownMergeOutcomeFlow(unknownHolderChat, unknownCandidateChat, paths.get("ingress-unknown-holder")!);
  await unknownNewRecoveryFlow(unknownNewChat, unknownNewCwd);

  assert.equal(
    await mainCommitCount(),
    initialCommitCount + 10,
    "five successful merges add exactly one source commit and one merge commit each; the deterministic conflict adds none",
  );
  assert.equal(
    runnerEvents().filter((event) => event.kind === "MERGED").length,
    5,
    "only the first normal merge, two recovered merges, the surviving canceled-flow merge, and unknown-ledger candidate may produce Git effects",
  );
}

async function concurrentMergeOutcomeFlow(
  current: CliRun,
  firstTarget: ChatTarget,
  secondTarget: ChatTarget,
  queryTarget: ChatTarget,
  paths: Map<string, string>,
): Promise<void> {
  const firstBranch = "ingress-waiting-first";
  const secondBranch = "ingress-waiting-second";
  const firstCwd = paths.get(firstBranch)!;
  const secondCwd = paths.get(secondBranch)!;
  const firstHead = await mainHead();
  const firstCount = await mainCommitCount();
  const mergeBefore = fixture!.sentMessages.length;
  const firstMergeId = send(current.connection, firstTarget, "/thread merge", "merge-held-first-concurrent");
  await waitAck(current.connection, firstMergeId, "first held concurrent merge");
  const firstGate = await waitRunnerGate(firstBranch);
  const firstLease = await waitValue("first concurrent merge process ledger", () => {
    const row = hostMerge(firstMergeId);
    return row?.phase === "running" && row.pid && row.pgid ? row : undefined;
  });
  assert.equal(firstLease.pid, firstLease.pgid, "first merge ledger must identify its owned process group");
  assert.equal(firstGate.pgid, firstLease.pgid, "first controlled hook must share the host-owned process group");
  assert.equal(firstGate.runnerPid, firstLease.pid, "first merge ledger must own the wt-compatible runner PID");
  assert.deepEqual(
    new Set(JSON.parse(firstLease.resource_cwds!) as string[]),
    new Set([firstCwd, mainWorktree]),
    "first merge ledger records its source and checked-out target paths",
  );
  assert.equal(await mainHead(), firstHead, "the held first pre-merge hook must not change Git HEAD");

  const secondMergeId = send(current.connection, secondTarget, "/thread merge", "merge-same-target-concurrent");
  await waitAck(current.connection, secondMergeId, "second concurrent same-target merge");
  const secondGate = await waitRunnerGate(secondBranch);
  const secondLease = await waitValue("second concurrent merge process ledger", () => {
    const row = hostMerge(secondMergeId);
    return row?.phase === "running" && row.pid && row.pgid ? row : undefined;
  });
  assert.equal(secondLease.pid, secondGate.runnerPid);
  assert.equal(secondLease.pgid, secondGate.pgid);
  assert.notEqual(secondLease.pgid, firstLease.pgid, "independent merge runners must own separate process groups");
  assert.notEqual(secondLease.pid, firstLease.pid, "same-target merges must be dispatched as independent processes");
  assert.deepEqual(
    new Set(JSON.parse(secondLease.resource_cwds!) as string[]),
    new Set([secondCwd, mainWorktree]),
    "second merge ledger records its source and checked-out target without reserving them",
  );
  const firstIngress = ingressJob(firstTarget, firstMergeId);
  const secondIngress = ingressJob(secondTarget, secondMergeId);
  assert.equal(Number(firstIngress?.ack_ready), 1, "first merge is durably ACKed");
  assert.equal(Number(secondIngress?.ack_ready), 1, "second merge is durably ACKed while the first is still running");
  assert.ok(isAlive(firstGate.sleeperPid), "first pre-merge hook remains held after the second runner starts");
  assert.ok(isAlive(secondGate.sleeperPid), "both same-target merge process groups overlap before either Git effect");

  const statusBefore = fixture!.sentMessages.length;
  const statusId = send(current.connection, firstTarget, "/status", "same-chat-status-during-concurrent-merges");
  await waitAck(current.connection, statusId, "same-chat status during overlapping merge runners");
  const statusReply = await waitReply(firstTarget, statusBefore, "same-chat status during overlapping merge runners", (text) => text.length > 0);
  assert.ok(isAlive(firstGate.sleeperPid), "same-chat status completes before either merge hook is released");
  assert.ok(isAlive(secondGate.sleeperPid), "same-chat status does not cancel the concurrent merge");

  const queryBefore = fixture!.sentMessages.length;
  const queryId = send(current.connection, queryTarget, "INGRESS_QUERY:other-chat-live", "independent-chat-query-during-concurrent-merges");
  await waitAck(current.connection, queryId, "independent ordinary query during overlapping merge runners");
  const queryReply = await waitReply(queryTarget, queryBefore, "independent model reply during overlapping merge runners", (text) => text.includes("INGRESS_REPLY:other-chat-live"));
  assert.ok(isAlive(firstGate.sleeperPid), "unrelated ordinary query completes while both merge hooks remain held");
  assert.ok(isAlive(secondGate.sleeperPid), "unrelated ordinary query leaves both merge process groups alive");
  assert.equal(await mainHead(), firstHead, "neither held runner may mutate Git before its hook is released");
  assert.equal(await mainCommitCount(), firstCount, "overlapping runners have not changed the target commit count");

  await releaseGate(firstGate);
  await waitPidsGone(firstGate);
  await waitReply(firstTarget, mergeBefore, "first concurrent merge completion receipt", (text) => text.includes("子区合并成功"));
  await waitUntil("first concurrent merge Git effect recorded", () => runnerCount(firstBranch, "MERGED") === 1);
  assert.equal(await mainCommitCount(), firstCount + 2, "first merge adds exactly its source and merge commits");
  assert.ok(isAlive(secondGate.sleeperPid), "second accepted merge remains independently held after first settles");
  const firstCompletionLease = hostMerge(firstMergeId);
  assert.ok(
    firstCompletionLease === null || firstCompletionLease.outcome_kind === "succeeded",
    "first merge must persist its normal outcome",
  );

  await releaseGate(secondGate);
  await waitPidsGone(secondGate);
  assert.equal(runnerCount(firstBranch, "START"), 1);
  assert.equal(runnerCount(firstBranch, "MERGED"), 1);
  assert.equal(runnerCount(secondBranch, "START"), 1, "the second same-target merge is not held behind a host resource lock");
  assert.equal(runnerCount(secondBranch, "MERGED"), 0, "Git's real content conflict must not be reported as a successful merge");
  assert.equal(await mainCommitCount(), firstCount + 2, "failed conflicting merge must not add a target commit");
  assert.ok(existsSync(secondCwd), "the actual conflict must preserve the source worktree for repair");
  const repairMessageId = `thread-merge-repair:${secondMergeId}`;
  const repairTask = await waitValue("one durable repair task for the actual Git conflict", () => taskRow(repairMessageId));
  await waitTaskCompleted(repairMessageId, "merge-conflict repair handoff");
  const mergeOutcome = hostMerge(secondMergeId);
  assert.ok(
    mergeOutcome === null || mergeOutcome.outcome_kind === "execution-failed",
    "the host merge outcome is either still recorded as failed or already settled after durable repair handoff",
  );
  const conflictFile = await readFile(join(mainWorktree, "concurrent-merge.txt"), "utf8");
  assert.match(conflictFile, /^<<<<<<< /mu, "real Git must leave a content-conflict marker before cleanup");
  assert.ok(
    !sentText(secondTarget, mergeBefore).some((text) => text.includes("子区合并成功")),
    "the failed merge must never emit a success receipt",
  );
  await run(["git", "-C", mainWorktree, "merge", "--abort"]);
  const finalHead = await mainHead();
  assert.notEqual(finalHead, firstHead, "the first successful merge remains committed after aborting the conflict");
  assert.equal(await mainCommitCount(), firstCount + 2);
  console.log(`CASE same-target-concurrent-git-conflict ${JSON.stringify({
    overlappingRunners: {
      first: { runnerPid: firstGate.runnerPid, pgid: firstGate.pgid, resourceCwds: JSON.parse(firstLease.resource_cwds!) },
      second: { runnerPid: secondGate.runnerPid, pgid: secondGate.pgid, resourceCwds: JSON.parse(secondLease.resource_cwds!) },
    },
    ackCounts: {
      firstMerge: ackCount(current.connection, firstMergeId),
      secondMerge: ackCount(current.connection, secondMergeId),
      status: ackCount(current.connection, statusId),
      query: ackCount(current.connection, queryId),
    },
    statusReply,
    queryReply,
    outcome: {
      gitConflict: true,
      durableRepairTask: taskRow(repairMessageId)?.state ?? repairTask.state,
      mergeLease: mergeOutcome?.outcome_kind ?? "settled",
      sourceWorktreePreserved: existsSync(secondCwd),
    },
    git: {
      headBefore: firstHead,
      headAfterConflictAbort: finalHead,
      commitsBefore: firstCount,
      commitsAfter: await mainCommitCount(),
      firstMergeEffects: runnerCount(firstBranch, "MERGED"),
      conflictingMergeEffects: runnerCount(secondBranch, "MERGED"),
      conflictMarkerObserved: true,
    },
  })}`);
}

async function cancelSpawnedMergeFlow(
  current: CliRun,
  firstTarget: ChatTarget,
  siblingTarget: ChatTarget,
  queryTarget: ChatTarget,
  paths: Map<string, string>,
): Promise<void> {
  const firstBranch = "ingress-cancel-spawned";
  const siblingBranch = "ingress-cancel-waiter";
  const firstCwd = paths.get(firstBranch)!;
  const siblingCwd = paths.get(siblingBranch)!;
  const firstHead = await mainHead();
  const firstCount = await mainCommitCount();
  const mergeBefore = fixture!.sentMessages.length;
  const firstMergeId = send(current.connection, firstTarget, "/thread merge", "merge-cancel-spawned");
  await waitAck(current.connection, firstMergeId, "spawned merge before cancellation");
  const firstGate = await waitRunnerGate(firstBranch);
  const firstLease = await waitValue("canceled merge process ledger", () => {
    const row = hostMerge(firstMergeId);
    return row?.phase === "running" && row.pid && row.pgid ? row : undefined;
  });
  assert.equal(firstLease.pid, firstLease.pgid, "canceled merge ledger must own its process group");
  assert.equal(firstGate.pgid, firstLease.pgid, "canceled hook must share the merge process group");
  assert.equal(firstGate.runnerPid, firstLease.pid, "canceled merge ledger must own the wt-compatible runner PID");

  const siblingMergeId = send(current.connection, siblingTarget, "/thread merge", "merge-sibling-during-cancel");
  await waitAck(current.connection, siblingMergeId, "same-target sibling merge while first is held");
  const siblingGate = await waitRunnerGate(siblingBranch);
  const siblingLease = await waitValue("sibling merge process ledger", () => {
    const row = hostMerge(siblingMergeId);
    return row?.phase === "running" && row.pid && row.pgid ? row : undefined;
  });
  assert.equal(siblingLease.pid, siblingGate.runnerPid);
  assert.equal(siblingLease.pgid, siblingGate.pgid);
  assert.notEqual(siblingLease.pgid, firstLease.pgid, "sibling merge must own an independent process group");
  assert.deepEqual(new Set(JSON.parse(firstLease.resource_cwds!) as string[]), new Set([firstCwd, mainWorktree]));
  assert.deepEqual(new Set(JSON.parse(siblingLease.resource_cwds!) as string[]), new Set([siblingCwd, mainWorktree]));
  assert.equal(Number(ingressJob(firstTarget, firstMergeId)?.ack_ready), 1);
  assert.equal(Number(ingressJob(siblingTarget, siblingMergeId)?.ack_ready), 1);
  assert.ok(isAlive(firstGate.sleeperPid), "first merge hook remains held after sibling runner starts");
  assert.ok(isAlive(siblingGate.sleeperPid), "both same-target merge process groups are simultaneously active");
  assert.equal(await mainHead(), firstHead, "neither held merge may mutate Git before cancellation or gate release");

  const statusBefore = fixture!.sentMessages.length;
  const statusId = send(current.connection, firstTarget, "/status", "same-chat-status-during-cancel-hook");
  await waitAck(current.connection, statusId, "same-chat status during cancellable held hook");
  const statusReply = await waitReply(firstTarget, statusBefore, "same-chat status during cancellable held hook", (text) => text.length > 0);
  assert.ok(isAlive(firstGate.sleeperPid), "same-chat status completes while the canceled merge hook remains held");
  assert.ok(isAlive(siblingGate.sleeperPid), "status leaves the sibling merge running");

  const queryBefore = fixture!.sentMessages.length;
  const queryId = send(current.connection, queryTarget, "INGRESS_QUERY:cancel-hook-independent", "independent-query-during-concurrent-cancel-hooks");
  await waitAck(current.connection, queryId, "independent query during concurrent merge hooks");
  const queryReply = await waitReply(queryTarget, queryBefore, "independent query reply during concurrent merge hooks", (text) => text.includes("INGRESS_REPLY:cancel-hook-independent"));
  assert.ok(isAlive(firstGate.sleeperPid), "unrelated query completes before the first merge is canceled");
  assert.ok(isAlive(siblingGate.sleeperPid), "unrelated query leaves the sibling process group active");
  assert.equal(await mainHead(), firstHead, "held hooks have not produced a target-side Git effect");

  const cancelBefore = fixture!.sentMessages.length;
  const cancelId = send(current.connection, firstTarget, "/cancel", "cancel-spawned-merge");
  await waitAck(current.connection, cancelId, "/cancel during spawned merge hook");
  const cancelReply = await waitReply(
    firstTarget,
    cancelBefore,
    "/cancel receipt for spawned merge",
    (text) => text.includes("取消") || text.includes("无法确认"),
  );
  await waitPidsGone(firstGate);
  assert.deepEqual(await processGroupMembers(firstGate.pgid), [], "cancel must terminate every process in its owned merge group");
  assert.ok(isAlive(siblingGate.sleeperPid), "cancel must not terminate another admitted merge's process group");

  const unknownLease = await waitValue("canceled spawned merge becomes unknown", () => {
    const row = hostMerge(firstMergeId);
    return row?.phase === "unknown" ? row : undefined;
  });
  assert.equal(unknownLease.source_id, firstMergeId, "unknown cancellation must retain the original merge source identity");
  assert.ok(unknownLease.resource_cwds, "unknown cancellation must retain the source/target process provenance");
  assert.deepEqual(
    new Set(JSON.parse(unknownLease.resource_cwds) as string[]),
    new Set([firstCwd, mainWorktree]),
    "unknown cancellation preserves the original source and target paths as outcome metadata",
  );
  assert.equal(await mainHead(), firstHead, "canceling the held hook must not change the target branch");
  assert.equal(await mainCommitCount(), firstCount, "canceling the first operation must not produce a target commit");
  assert.equal(runnerCount(firstBranch, "START"), 1, "the canceled merge must not be retried");
  assert.equal(runnerCount(firstBranch, "MERGED"), 0, "the canceled merge must not report a Git effect");
  assert.equal(siblingLease.phase, "running", "the sibling operation remains independently dispatched");
  assert.equal(siblingLease.source_id, siblingMergeId);
  assert.ok(existsSync(firstCwd), "the unknown canceled merge must preserve its original source worktree");
  assert.ok(existsSync(siblingCwd), "the independently running sibling source worktree remains present");

  const siblingStatusBefore = fixture!.sentMessages.length;
  const siblingStatusId = send(current.connection, siblingTarget, "/status", "status-while-unknown-sibling-runs");
  await waitAck(current.connection, siblingStatusId, "status while canceled sibling outcome is unknown");
  const siblingStatusReply = await waitReply(
    siblingTarget,
    siblingStatusBefore,
    "status remains responsive while the other merge outcome is unknown",
    (text) => text.length > 0,
  );
  assert.ok(isAlive(siblingGate.sleeperPid), "control query must not interrupt the independently running merge");

  await releaseGate(siblingGate);
  await waitPidsGone(siblingGate);
  await waitReply(siblingTarget, mergeBefore, "sibling merge completion receipt", (text) => text.includes("子区合并成功"));
  const siblingOutcome = hostMerge(siblingMergeId);
  assert.ok(
    siblingOutcome === null || siblingOutcome.outcome_kind === "succeeded",
    "a separate admitted merge can settle successfully while the canceled sibling remains unknown",
  );
  assert.equal(runnerCount(siblingBranch, "START"), 1);
  assert.equal(runnerCount(siblingBranch, "MERGED"), 1);
  assert.equal(await mainCommitCount(), firstCount + 2, "only the surviving sibling merge adds target commits");
  assert.notEqual(await mainHead(), firstHead);
  const unresolvedAfterSibling = hostMerge(firstMergeId);
  assert.equal(unresolvedAfterSibling?.phase, "unknown", "the sibling success must not rewrite the canceled merge outcome");
  console.log(`CASE cancel-spawned-merge-with-concurrent-sibling ${JSON.stringify({
    heldGroups: {
      canceled: { runnerPid: firstGate.runnerPid, hookPid: firstGate.hookPid, descendantPid: firstGate.sleeperPid, pgid: firstGate.pgid },
      sibling: { runnerPid: siblingGate.runnerPid, hookPid: siblingGate.hookPid, descendantPid: siblingGate.sleeperPid, pgid: siblingGate.pgid },
    },
    unknownLease: { phase: unresolvedAfterSibling?.phase, sourceId: unresolvedAfterSibling?.source_id, resources: JSON.parse(unresolvedAfterSibling!.resource_cwds!) },
    ackCounts: {
      merge: ackCount(current.connection, firstMergeId),
      sibling: ackCount(current.connection, siblingMergeId),
      status: ackCount(current.connection, statusId),
      query: ackCount(current.connection, queryId),
      cancel: ackCount(current.connection, cancelId),
      siblingStatus: ackCount(current.connection, siblingStatusId),
    },
    statusReply,
    queryReply,
    cancelReply,
    siblingStatusReply,
    canceledProcessGroupGone: true,
    siblingSurvivedCancellation: true,
    git: {
      headBefore: firstHead,
      headAfter: await mainHead(),
      commitsBefore: firstCount,
      commitsAfter: await mainCommitCount(),
      canceledMergeEffects: runnerCount(firstBranch, "MERGED"),
      siblingMergeEffects: runnerCount(siblingBranch, "MERGED"),
    },
  })}`);
}


async function waitingMergeRecoveryFlow(
  mode: "graceful" | "sigkill",
  target: ChatTarget,
  orderedChat: ChatTarget,
  sourceCwd: string,
): Promise<void> {
  assert.deepEqual(target, orderedChat, "recovery scenario queues merge behind prior work in the exact same chat");
  const current = active!;
  const modelGateLabel = mode === "graceful" ? "graceful-predecessor" : "sigkill-predecessor";
  const branch = mode === "graceful" ? "ingress-graceful-wait" : "ingress-sigkill-wait";
  holdModel(modelGateLabel);
  const taskId = send(current.connection, orderedChat, `INGRESS_HOLD:${modelGateLabel}`, `${mode}-same-chat-predecessor`);
  await waitAck(current.connection, taskId, `${mode} held same-chat predecessor task`);
  await waitModel(modelGateLabel);
  await waitTaskStarted(taskId, `${mode} same-chat predecessor`);
  assert.equal(binding(orderedChat)?.cwd, await realpath(sourceCwd), `${mode} predecessor must use the exact thread binding`);

  const headBefore = await mainHead();
  const commitsBefore = await mainCommitCount();
  const mergeId = send(current.connection, target, "/thread merge", `${mode}-same-chat-waiting-merge`);
  await waitAck(current.connection, mergeId, `${mode} same-chat queued merge`);
  const waiting = await assertWaitingIngress(target, mergeId, `${mode} same-chat merge`);
  assert.equal(hostMergeCount(mergeId), 0, `${mode} merge must not install its host process before its chat/session turn`);
  assert.equal(runnerCount(branch, "START"), 0, `${mode} merge must respect exact-chat ordering, not resource locking`);
  assert.equal(taskRow(taskId)?.state, "started", `${mode} predecessor remains active when the merge is durably queued`);

  const statusBefore = fixture!.sentMessages.length;
  const statusId = send(current.connection, target, "/status", `${mode}-status-beside-queued-merge`);
  await waitAck(current.connection, statusId, `${mode} status beside queued merge`);
  const statusReply = await waitReply(target, statusBefore, `${mode} status beside queued merge`, (text) => text.length > 0);
  assert.equal(runnerCount(branch, "START"), 0, `${mode} control handling must not bypass the queued merge's session order`);
  assert.equal(await mainHead(), headBefore, `${mode} queued merge cannot mutate the target before its ordered turn`);

  const stopCode = await stopActive(mode === "graceful" ? "SIGTERM" : "SIGKILL");
  if (mode === "sigkill") assert.notEqual(stopCode, 0, "SIGKILL must not look like graceful service completion");
  releaseModel(modelGateLabel);
  const restarted = await launch();
  await waitTaskCompleted(taskId, `${mode} recovered predecessor`);
  const gate = await waitRunnerGate(branch);
  await releaseGate(gate);
  await waitPidsGone(gate);
  await waitReply(target, 0, `${mode} recovered merge receipt`, (text) => text.includes("子区合并成功"));
  assert.equal(runnerCount(branch, "START"), 1, `${mode} recovered queued merge must start exactly once`);
  assert.equal(runnerCount(branch, "MERGED"), 1, `${mode} recovery must create one Git effect only`);
  assert.equal(await mainCommitCount(), commitsBefore + 2, `${mode} recovery must publish one source commit and one merge commit`);
  assert.notEqual(await mainHead(), headBefore, `${mode} recovery must complete the ordered merge`);
  assert.equal(ackCount(current.connection, mergeId), 1, `${mode} merge ACK must have been emitted exactly once before shutdown`);
  assert.equal(taskRow(taskId)?.state, "completed", `${mode} accepted predecessor resumes before the queued merge`);
  const completionLease = hostMerge(mergeId);
  assert.ok(completionLease === null || completionLease.outcome_kind === "succeeded", `${mode} merge must persist success before settling its lease`);
  console.log(`CASE ${mode}-same-chat-merge-ingress-recovery ${JSON.stringify({
    waitingIngress: { state: waiting.state, lane: waiting.lane, ackReady: waiting.ack_ready, checkpoint: Boolean(waiting.operation_checkpoint_json), hostMergeLedgerBeforeRestart: 0 },
    predecessor: { messageId: taskId, stateAfterRestart: taskRow(taskId)?.state, modelCalls: modelCallCount(modelGateLabel) },
    statusReply,
    shutdown: mode === "graceful" ? "SIGTERM" : "SIGKILL",
    stopExitCode: stopCode,
    recoveredRunnerPid: gate.runnerPid,
    ackCount: ackCount(current.connection, mergeId),
    git: { headBefore, headAfter: await mainHead(), commitsBefore, commitsAfter: await mainCommitCount(), runnerStarts: runnerCount(branch, "START"), mergeEffects: runnerCount(branch, "MERGED") },
    restartedPid: restarted.child.pid,
    sourceCwd,
  })}`);
}

async function mediaSpoolRecoveryFlow(mediaTarget: ChatTarget, queryTarget: ChatTarget): Promise<void> {
  const current = active!;
  const label = "media-spool";
  holdModel(label);
  const messageId = nextMessageId();
  const beforeMedia = fixture!.sentMessages.length;
  fixture!.sendInbound(current.connection, {
    fromUid: fixture!.ownerUid,
    channelId: mediaTarget.channelId,
    channelType: mediaTarget.channelType,
    messageId,
    text: `INGRESS_MEDIA:${label}`,
    mention: { uids: [fixture!.botUid] },
    payload: {
      type: 8,
      url: `http://127.0.0.1:${localServer!.port}/media/spool-proof`,
      name: "ingress-spool-proof.bin",
      size: 38,
      caption: `INGRESS_MEDIA:${label}`,
    },
  });
  deliveries.push({ connection: current.connection, messageId, label: "gated-media" });
  await waitUntil("attachment request reached the local HTTP gate", () => mediaRequests >= 1);
  assert.equal(ackCount(current.connection, messageId), 0, "media URL still held by local HTTP gate must not be ACKed");
  const spoolDirectory = join(stateDir, "attachments", `5-${safeSegment(mediaTarget.channelId)}`, messageId);
  assert.deepEqual(await filesUnder(spoolDirectory), [], "unfetched media must not be persisted before the HTTP gate opens");

  const queryBefore = fixture!.sentMessages.length;
  const queryId = send(current.connection, queryTarget, "INGRESS_QUERY:media-download-independent", "query-during-media-download");
  await waitAck(current.connection, queryId, "independent query while media HTTP is held");
  const queryReply = await waitReply(queryTarget, queryBefore, "independent query reply while media is held", (text) => text.includes("INGRESS_REPLY:media-download-independent"));
  assert.equal(ackCount(current.connection, messageId), 0, "unrelated completed work must not make held media ACK early");
  assert.equal(mediaRequests, 1, "one message must issue only one local attachment download");

  mediaGate.resolve();
  await waitAck(current.connection, messageId, "media after local spool persistence");
  const spoolFile = await waitValueAsync<string>("persisted media spool file", async () => {
    const files = await filesUnder(spoolDirectory);
    return files.length === 1 ? files[0]! : undefined;
  });
  const spoolBytes = await readFile(spoolFile);
  assert.equal(spoolBytes.toString("utf8"), "INGRESS_MEDIA_SPOOL_BYTES:000000000000", "ACKed attachment must already be completely written");
  const persisted = await assertWaitingOrHandedOffMedia(mediaTarget, messageId);
  await waitModel(label);
  await waitTaskStarted(messageId, "media spool task before crash");

  const stopCode = await stopActive("SIGKILL");
  assert.notEqual(stopCode, 0, "media recovery scenario must kill the original CLI process");
  releaseModel(label);
  mediaGate = deferred();
  const restarted = await launch();
  await waitTaskCompleted(messageId, "ACKed media recovery");
  await waitReply(mediaTarget, beforeMedia, "recovered media task reply", (text) => text.includes("INGRESS_REPLY:media-spool"));
  assert.equal(mediaRequests, 1, "recovery after ACK must use the saved local spool and never fetch the original URL again");
  assert.equal(modelError, undefined);
  const after = ingressJob(mediaTarget, messageId);
  assert.equal(Number(after?.ack_ready), 1, "media ingress ACK readiness must remain durable after recovery");
  console.log(`CASE media-ack-and-spool-recovery ${JSON.stringify({
    messageId,
    downloadRequests: mediaRequests,
    ackBeforeHttpRelease: 0,
    ackAfterDiskWrite: ackCount(current.connection, messageId),
    spoolFile,
    spoolBytes: spoolBytes.byteLength,
    ingressAfterDispatch: { state: after?.state, ackReady: after?.ack_ready, consumerKind: after?.consumer_kind },
    initialDurableRecord: { state: persisted.state, ackReady: persisted.ack_ready, preparedText: Boolean(persisted.prepared_text), spoolPaths: persisted.spool_paths_json },
    independentReply: queryReply,
    recoveredTask: taskRow(messageId)?.state,
    restartedPid: restarted.child.pid,
  })}`);
}

async function assertWaitingOrHandedOffMedia(target: ChatTarget, messageId: string): Promise<IngressJobRow> {
  const row = await waitValue("media durable admission record", () => {
    const current = ingressJob(target, messageId);
    return current && Number(current.ack_ready) === 1 ? current : undefined;
  });
  if (row.spool_paths_json !== null) {
    const paths = JSON.parse(row.spool_paths_json) as unknown;
    assert.ok(Array.isArray(paths), "persisted spool_paths_json must be a JSON array");
    for (const path of paths) assert.ok(typeof path === "string" && existsSync(path), "every persisted spool path must exist before ACK");
  }
  return row;
}

async function unknownMergeOutcomeFlow(holder: ChatTarget, candidate: ChatTarget, holderCwd: string): Promise<void> {
  await stopActive("SIGTERM");
  await createUnknownLease(holder, holderCwd);
  const current = await launch();
  const headBefore = await mainHead();
  const commitsBefore = await mainCommitCount();
  const effectsBefore = runnerEvents().filter((event) => event.kind === "MERGED").length;
  assert.equal(runnerCount("ingress-unknown-holder", "START"), 0, "startup must not replay the injected unknown merge");

  const mergeBefore = fixture!.sentMessages.length;
  const mergeId = send(current.connection, candidate, "/thread merge", "merge-beside-unknown-prior-outcome");
  await waitAck(current.connection, mergeId, "merge beside unknown prior outcome");
  const gate = await waitRunnerGate("ingress-unknown-candidate");
  const candidateLease = await waitValue("candidate merge process despite unknown sibling outcome", () => {
    const row = hostMerge(mergeId);
    return row?.phase === "running" && row.pid && row.pgid ? row : undefined;
  });
  assert.equal(candidateLease.pid, gate.runnerPid);
  assert.equal(candidateLease.pgid, gate.pgid);
  assert.equal(runnerCount("ingress-unknown-candidate", "START"), 1, "unknown merge outcome is not a workspace resource lock");
  assert.equal(runnerCount("ingress-unknown-holder", "START"), 0, "unknown original merge is never automatically replayed");
  assert.equal(await mainHead(), headBefore, "the admitted candidate remains before Git mutation while its gate is held");
  assert.equal(runnerEvents().filter((event) => event.kind === "MERGED").length, effectsBefore);
  const unknownRow = dbQuery<HostMergeRow>(
    "SELECT id, source_id, phase, pid, pgid, outcome_kind, resource_cwds FROM host_merge_leases WHERE id = ?",
    "ingress-benchmark-injected-unknown",
  );
  assert.equal(unknownRow?.phase, "unknown", "dispatching a new merge must not rewrite the unresolved old outcome");

  const statusBefore = fixture!.sentMessages.length;
  const statusId = send(current.connection, candidate, "/status", "status-during-unknown-merge-sibling");
  await waitAck(current.connection, statusId, "status beside running merge and unknown outcome");
  const statusReply = await waitReply(candidate, statusBefore, "status beside running merge and unknown outcome", (text) => text.length > 0);
  assert.ok(isAlive(gate.sleeperPid), "same-chat control remains responsive while the candidate merge is held");

  await releaseGate(gate);
  await waitPidsGone(gate);
  await waitReply(candidate, mergeBefore, "candidate merge receipt beside unknown outcome", (text) => text.includes("子区合并成功"));
  const candidateOutcome = hostMerge(mergeId);
  assert.ok(
    candidateOutcome === null || candidateOutcome.outcome_kind === "succeeded",
    "the independent candidate's actual outcome is persisted normally",
  );
  assert.equal(runnerCount("ingress-unknown-candidate", "START"), 1);
  assert.equal(runnerCount("ingress-unknown-candidate", "MERGED"), 1);
  assert.equal(runnerCount("ingress-unknown-holder", "START"), 0, "the unresolved original operation remains unreplayed");
  const unresolvedAfterCandidate = dbQuery<HostMergeRow>(
    "SELECT id, source_id, phase, pid, pgid, outcome_kind, resource_cwds FROM host_merge_leases WHERE id = ?",
    "ingress-benchmark-injected-unknown",
  );
  assert.equal(unresolvedAfterCandidate?.phase, "unknown", "a separate successful merge must not settle the old unknown outcome");
  assert.notEqual(await mainHead(), headBefore);
  assert.equal(await mainCommitCount(), commitsBefore + 2, "the independent candidate adds one source and one merge commit");
  assert.equal(
    runnerEvents().filter((event) => event.kind === "MERGED").length,
    effectsBefore + 1,
  );
  console.log(`CASE merge-runs-beside-unknown-outcome ${JSON.stringify({
    unresolved: { phase: unresolvedAfterCandidate?.phase, sourceId: unresolvedAfterCandidate?.source_id, resources: JSON.parse(unresolvedAfterCandidate!.resource_cwds!) },
    candidate: { messageId: mergeId, phase: candidateLease.phase, outcome: candidateOutcome?.outcome_kind, runnerPid: gate.runnerPid, pgid: gate.pgid },
    ackCounts: { merge: ackCount(current.connection, mergeId), status: ackCount(current.connection, statusId) },
    statusReply,
    unknownOriginalStarts: runnerCount("ingress-unknown-holder", "START"),
    candidateGitEffects: runnerCount("ingress-unknown-candidate", "MERGED"),
    process: current.child.pid,
  })}`);
  await stopActive("SIGTERM");
}

async function unknownNewRecoveryFlow(target: ChatTarget, cwd: string): Promise<void> {
  const initial = await launch();
  await bindSession(initial.connection, target, cwd, "unknown-new-recovery-initial");
  const originalBinding = binding(target)!;
  const queuedMarker = "unknown-new-queued";
  const originalNewText = `/new ${cwd}`;

  assert.ok(!existsSync(`${ompStartGate}.armed`), "controlled OMP gate must be unarmed before the recovery case");
  await writeFile(`${ompStartGate}.armed`, "hold\n");
  const oldNewId = send(initial.connection, target, originalNewText, "unknown-new-original");
  await waitAck(initial.connection, oldNewId, "unknown /new before SIGKILL");
  await waitUntil("old /new candidate reached the controlled OMP startup gate", () =>
    existsSync(`${ompStartGate}.started`) && existsSync(`${ompStartGate}.pid`));
  const oldWorkerPid = Number(readFileSync(`${ompStartGate}.pid`, "utf8").trim());
  assert.ok(Number.isSafeInteger(oldWorkerPid) && oldWorkerPid > 0 && isAlive(oldWorkerPid),
    "the interrupted /new must own a live candidate worker process");
  const oldWorkerLedger = await waitValue("exact old /new candidate worker ledger", () =>
    dbQuery<{ id: string; session_id: string; state: string }>(
      "SELECT id, session_id, state FROM worker_ledger WHERE pid = ?",
      oldWorkerPid,
    ));
  assert.equal(oldWorkerLedger.state, "running", "the interrupted candidate must be a managed live worker");
  assert.notEqual(oldWorkerLedger.session_id, originalBinding.session_id,
    "the interrupted /new candidate must already own a distinct session identity");
  const oldNewJob = await waitValue("old /new dispatch intent", () => {
    const row = ingressJob(target, oldNewId);
    return row?.state === "dispatch-intent" ? row : undefined;
  });
  assert.equal(Number(oldNewJob.ack_ready), 1, "the dispatched /new must already be durably ACK-ready");

  const queuedId = send(initial.connection, target, `INGRESS_HOLD:${queuedMarker}`, "message-behind-unknown-new");
  await waitAck(initial.connection, queuedId, "message behind the held /new");
  assert.equal(taskRow(queuedId), null, "the message behind the unresolved /new must not be handed to a task");
  assert.equal(modelCallCount(queuedMarker), 0, "the old message must not reach the local model before restart");

  const firstStop = await stopActive("SIGKILL");
  assert.notEqual(firstStop, 0, "SIGKILL must not be reported as a graceful CLI exit");
  await waitUntil("old /new candidate worker exited under the parent-death watchdog", () => !isAlive(oldWorkerPid), 15_000);
  const restarted = await launch();
  const unknownJob = await waitValue("old /new remains unknown after restart", () => {
    const row = ingressJob(target, oldNewId);
    return row?.state === "unknown" ? row : undefined;
  });
  assert.equal(Number(unknownJob.ack_ready), 1);
  assert.deepEqual(binding(target), originalBinding, "recovery must not publish the interrupted candidate as the binding");
  assert.equal(taskRow(queuedId), null, "startup must not replay the old message as a task");
  assert.equal(modelCallCount(queuedMarker), 0, "startup must not send the old message to the local model");

  replay(restarted.connection, target, originalNewText, oldNewId);
  await waitReplayAck(restarted.connection, oldNewId, "duplicate unknown /new");
  replay(restarted.connection, target, `INGRESS_HOLD:${queuedMarker}`, queuedId);
  await waitReplayAck(restarted.connection, queuedId, "duplicate blocked message");
  await Bun.sleep(3_000);
  assert.equal(ingressJob(target, oldNewId)?.state, "unknown", "redelivery must not make the old /new executable again");
  assert.deepEqual(binding(target), originalBinding, "redelivery must not change the old binding");
  assert.equal(taskRow(queuedId), null, "redelivery must not create a task for the old message");
  assert.equal(modelCallCount(queuedMarker), 0, "redelivery must not send the old message to the model");

  const recoveryStart = fixture!.sentMessages.length;
  const recoveryId = send(restarted.connection, target, `/new ${cwd}`, "unknown-new-explicit-recovery");
  await waitAck(restarted.connection, recoveryId, "new /new recovery command");
  const recoveryReceipt = await waitReply(target, recoveryStart, "successful explicit /new recovery", (text) =>
    text.startsWith("已创建会话 "));
  const recoveredBinding = await waitValue("new binding published by explicit /new recovery", () => {
    const row = binding(target);
    return row && row.session_id !== originalBinding.session_id && row.session_file ? row : undefined;
  });
  assert.notEqual(recoveredBinding.session_id, oldWorkerLedger.session_id,
    "explicit recovery must not reuse the unknown candidate's session identity");
  assert.equal(recoveredBinding.cwd, await realpath(cwd));
  const recoveredSessionFile = recoveredBinding.session_file;
  assert.ok(recoveredSessionFile, "recovery binding must include its session file");
  assert.notEqual(recoveredSessionFile, originalBinding.session_file,
    "explicit recovery must create a session distinct from the pre-interruption session");
  assert.ok(await Bun.file(recoveredSessionFile).exists(), "recovery binding must reference a real session file");
  assert.ok(recoveryReceipt.includes(recoveredBinding.session_id), "the success receipt must identify the committed new session");

  let queuedNotice = sentText(target).find((text) => text.includes(queuedId));
  if (!queuedNotice) {
    queuedNotice = await waitReply(target, 0, "not-executed notice for the old queued message", (text) => text.includes(queuedId));
  }
  assert.match(queuedNotice, /未执行|不会自动执行|尚未执行/u,
    "the notice must clearly say that the old queued message was not executed");
  assert.match(queuedNotice, /重新发送|忽略/u, "the notice must leave resending or ignoring to the user");
  const queuedNoticeCount = () => sentText(target).filter((text) => text.includes(queuedId)).length;
  assert.equal(queuedNoticeCount(), 1, "one old message must produce exactly one disposition notice");
  assert.equal(taskRow(queuedId), null, "the disposition notice must not be backed by an auto-executed task");
  assert.equal(modelCallCount(queuedMarker), 0, "the old message must never reach the local model");
  assert.equal(ingressJob(target, oldNewId)?.state, "unknown", "successful recovery must preserve the old /new outcome as unknown");

  const newMarker = "unknown-new-recovered-message";
  const newMessageId = send(restarted.connection, target, `INGRESS_HOLD:${newMarker}`, "message-after-new-recovery");
  await waitAck(restarted.connection, newMessageId, "message after new /new recovery");
  await waitTaskCompleted(newMessageId, "message after recovered /new");
  const newMessageReply = await waitReply(target, recoveryStart, "new-session task reply", (text) =>
    text.includes(`INGRESS_REPLY:${newMarker}`));
  const completedTask = taskRow(newMessageId);
  assert.equal(completedTask?.state, "completed");
  assert.equal(completedTask?.session_id, recoveredBinding.session_id,
    "the post-recovery message must execute in the newly committed session");
  assert.equal(modelCallCount(newMarker), 1, "the post-recovery message must reach the finite local model exactly once");
  assert.ok(modelRequests.find((request) => request.label === newMarker)?.body.includes(newMarker),
    "the finite local model request must contain the new message");
  assert.ok((await readFile(recoveredSessionFile, "utf8")).includes(newMarker),
    "the new message must be persisted in the recovered session history");
  assert.equal(modelCallCount(queuedMarker), 0, "the old queued message must remain unexecuted after recovery");
  assert.equal(taskRow(queuedId), null);

  const secondStop = await stopActive("SIGKILL");
  assert.notEqual(secondStop, 0);
  const afterSecondRestart = await launch();
  assert.deepEqual(binding(target), recoveredBinding, "a second restart must preserve the recovered binding");
  for (const [messageId, text, label] of [
    [oldNewId, originalNewText, "unknown /new after second restart"],
    [queuedId, `INGRESS_HOLD:${queuedMarker}`, "old queued message after second restart"],
    [newMessageId, `INGRESS_HOLD:${newMarker}`, "completed new message after second restart"],
  ] as const) {
    replay(afterSecondRestart.connection, target, text, messageId);
    await waitReplayAck(afterSecondRestart.connection, messageId, label);
  }
  await Bun.sleep(3_000);
  assert.deepEqual(binding(target), recoveredBinding, "duplicate delivery after restart must not change the recovered binding");
  assert.equal(ingressJob(target, oldNewId)?.state, "unknown");
  assert.equal(taskRow(queuedId), null, "old queued input must remain unexecuted after repeated delivery and restart");
  assert.equal(taskRow(newMessageId)?.state, "completed");
  assert.equal(taskRow(newMessageId)?.session_id, recoveredBinding.session_id);
  assert.equal(modelCallCount(queuedMarker), 0);
  assert.equal(modelCallCount(newMarker), 1, "repeated delivery and restart must not execute the completed message twice");
  assert.equal(queuedNoticeCount(), 1, "repeated delivery and restart must not duplicate the disposition notice");
  console.log(`CASE unknown-new-explicit-recovery ${JSON.stringify({
    oldNewMessageId: oldNewId,
    queuedMessageId: queuedId,
    oldWorkerPid,
    oldCandidateSessionId: oldWorkerLedger.session_id,
    workerLedgerAfterRecovery: dbQuery<{ state: string }>("SELECT state FROM worker_ledger WHERE pid = ?", oldWorkerPid)?.state,
    oldWorkerExited: !isAlive(oldWorkerPid),
    oldNewState: ingressJob(target, oldNewId)?.state,
    explicitRecoveryMessageId: recoveryId,
    originalSessionId: originalBinding.session_id,
    recoveredSessionId: recoveredBinding.session_id,
    recoveredTask: { messageId: newMessageId, state: taskRow(newMessageId)?.state, sessionId: taskRow(newMessageId)?.session_id },
    oldQueuedTask: taskRow(queuedId),
    localModelCalls: { oldQueued: modelCallCount(queuedMarker), recoveredMessage: modelCallCount(newMarker) },
    noticesForOldMessage: queuedNoticeCount(),
    ackCounts: {
      oldNewAfterRestart: ackCount(restarted.connection, oldNewId),
      queuedAfterRestart: ackCount(restarted.connection, queuedId),
      oldNewAfterSecondRestart: ackCount(afterSecondRestart.connection, oldNewId),
      queuedAfterSecondRestart: ackCount(afterSecondRestart.connection, queuedId),
      completedAfterSecondRestart: ackCount(afterSecondRestart.connection, newMessageId),
    },
    recoveryReceipt,
    newMessageReply,
    cliRestarts: 2,
  })}`);
  await stopActive("SIGTERM");
}

async function allProcessRows(): Promise<ProcessRow[]> {
  const output = await run(["/bin/ps", "-axo", "pid=,ppid=,pgid=,command="]);
  return output.split("\n").flatMap((line) => {
    const fields = line.trim().split(/\s+/u);
    if (fields.length < 4) return [];
    const pid = Number(fields[0]);
    const ppid = Number(fields[1]);
    const pgid = Number(fields[2]);
    if (![pid, ppid, pgid].every(Number.isSafeInteger)) return [];
    return [{ pid, ppid, pgid, command: fields.slice(3).join(" ") }];
  });
}

async function cleanup(): Promise<void> {
  await writeFile(`${ompStartGate}.abort`, "stop\n");
  for (const gate of modelGates.values()) gate.resolve();
  mediaGate.resolve();
  if (active?.child.exitCode === null) active.child.kill("SIGTERM");
  for (const child of children) {
    if (child.exitCode !== null) continue;
    child.kill("SIGTERM");
    try {
      await childExit(child, 5_000);
    } catch {
      child.kill("SIGKILL");
      await child.exited;
    }
  }
  active = undefined;

  // Stop only process groups created by this fixture's wt-compatible runner. Their PIDs are written
  // beneath its unique temporary directory and the CLI ledger proves they are our own children.
  for (const event of runnerEvents().filter((row) => row.kind === "START")) {
    const gatePrefix = join(gateDir, `${event.branch}.${event.attempt}`);
    const sleeperPath = `${gatePrefix}.sleep.pid`;
    if (!existsSync(sleeperPath)) continue;
    const sleeper = Number((await readFile(sleeperPath, "utf8")).trim());
    if (Number.isSafeInteger(sleeper) && sleeper > 0 && isAlive(sleeper)) process.kill(sleeper, "SIGTERM");
  }
  if (database) {
    database.close();
    database = undefined;
  }
  await fixture?.close();
  await localServer?.stop(true);

  const leftovers = (await allProcessRows()).filter((row) => row.pid !== process.pid && row.command.includes(root));
  for (const row of leftovers) {
    try { process.kill(row.pid, "SIGKILL"); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
    }
  }
  const logs = (await Promise.all(outputPromises)).flat().join("\n");
  if (!succeeded) console.error(logs.replaceAll(fixture?.botToken ?? "", "[fixture-token]"));
  const credentialLeaked = logs.includes(fixture?.botToken ?? "never-a-token");
  await rm(root, { recursive: true, force: true });
  if (succeeded) assert.deepEqual(leftovers, [], `temporary CLI/RPC/Git processes leaked: ${JSON.stringify(leftovers)}`);
  assert.ok(!credentialLeaked, "CLI output must not leak its fixture credential");
}

try {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  localServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === "/media/spool-proof") {
        mediaRequests += 1;
        // The test polls this counter with a deadline before releasing the explicit HTTP gate.
        if (mediaRequests > 1) return new Response("expired after first durable fetch", { status: 410 });
        await mediaGate.promise;
        return new Response("INGRESS_MEDIA_SPOOL_BYTES:000000000000", {
          headers: { "content-type": "application/octet-stream" },
        });
      }
      if (url.pathname === "/v1/models" && request.method === "GET") {
        return Response.json({ object: "list", data: [{ id: "probe", object: "model" }] });
      }
      if (request.method !== "POST") return new Response("not found", { status: 404 });
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        modelError = `non-JSON request ${request.method} ${url.pathname}`;
        return new Response("bad local model request", { status: 400 });
      }
      const serialized = JSON.stringify(body.messages ?? body);
      const label = modelLabel(serialized);
      const call = modelCallCount(label) + 1;
      modelRequests.push({ label, path: url.pathname, body: serialized, call });
      const gate = modelGates.get(label);
      if (gate) await gate.promise;
      return localModelResponse(label, call);
    },
  });
  await installLocalFilesAndEnvironment();
  await setupAndRun();

  for (const delivery of deliveries) {
    assert.equal(ackCount(delivery.connection, delivery.messageId), 1, `${delivery.label} message ${delivery.messageId} must have one exact ACK`);
  }
  assert.equal(modelError, undefined, "all OMP model requests must stay on the finite local fixture");
  assert.ok(fixture.registerRequests.length > 0, "real CLI must register only against the local encrypted Octo fixture");
  assert.ok(fixture.connections.every((connection) => Boolean(connection.aesKey && connection.aesIv)), "every CLI WebSocket must use fixture encryption");
  assert.ok(modelRequests.length > 0, "ordinary input must call the finite local model");
  assert.ok(modelRequests.every((request) => request.path.startsWith("/v1/")), "model requests must stay on the local API fixture");
  console.log(`RUNNER wt-compatible-script (not installed Worktrunk) ${JSON.stringify({ ompCommand, cliRuns: children.length, localModelRequests: modelRequests.length, encryptedConnections: fixture.connections.length })}`);
  console.log(`METRIC exact_recvacks=${deliveries.length}`);
  console.log(`METRIC worktree_merge_effects=${runnerEvents().filter((event) => event.kind === "MERGED").length}`);
  console.log(`METRIC media_download_requests=${mediaRequests}`);
  console.log(`EVIDENCE ${JSON.stringify({
    deliveries: deliveries.map(({ messageId, label, connection }) => ({ messageId, label, ackCount: ackCount(connection, messageId), connection: fixture!.connections.indexOf(connection) + 1 })),
    worktreeEvents: runnerEvents(),
    modelCalls: modelRequests.map(({ label, path, call }) => ({ label, path, call })),
    mediaRequests,
  })}`);
  succeeded = true;
} finally {
  await cleanup();
}
