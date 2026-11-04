import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  type FixtureConnection,
  type FixtureSentMessage,
} from "../fixtures/octo-protocol-server.ts";

// Opt-in end-to-end acceptance: the installed OMP binary, an isolated real session, finite
// loopback model/remote-compaction endpoints, and the encrypted local Octo protocol fixture.
// The manual command is sent through the bound channel; this never registers the resident bot or
// reaches a real provider/session.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-compact-notices-")));
const sessionRoot = join(root, "sessions");
const stateRoot = join(root, "state");
const wirePath = join(root, "omp-wire.jsonl");
const configPath = join(root, "config.json");
const nativeConfigPath = join(root, "native.yml");
const manualEndHoldPath = join(root, "hold-manual-end");
const manualEndReleasePath = join(root, "release-manual-end");
const automaticEndHoldPath = join(root, "hold-automatic-end");
const automaticEndReleasePath = join(root, "release-automatic-end");

type Phase = "manual" | "automatic";
type Gate = { promise: Promise<void>; resolve(): void };
type ProviderRequest = { atMs: number; url: string; body: Record<string, unknown> };
type HeldRemoteRequest = ProviderRequest & { phase: Phase; gate: Gate };
type WireRecord = {
  sequence: number;
  atMs: number;
  direction: "host" | "omp" | "forwarded";
  heldForHost?: boolean;
  type?: string;
  id?: string;
  message?: string;
  messageLength?: number;
  success?: boolean;
  agentInvoked?: boolean;
  mode?: string;
  reason?: string;
  action?: string;
  hasResult?: boolean;
  resultSummary?: string;
  aborted?: boolean;
  skipped?: boolean;
  willRetry?: boolean;
  errorMessage?: string;
};
type SessionEntry = { type?: string; summary?: string; method?: string };
type BoundSession = { session_id: string; cwd: string; session_file: string | null };
type TaskRow = { state: string; error_text: string | null };

function deferred(): Gate {
  const pending = Promise.withResolvers<void>();
  let settled = false;
  return {
    promise: pending.promise,
    resolve() {
      if (settled) return;
      settled = true;
      pending.resolve();
    },
  };
}
function writeNativeConfig(remoteEndpoint: string, thresholdPercent: number): string {
  return [
    "autoRetry:",
    "  enabled: false",
    "compaction:",
    "  enabled: true",
    "  methodOrder:",
    "    - remote",
    `  thresholdPercent: ${thresholdPercent}`,
    "  keepRecentTokens: 1",
    "  asyncEnabled: false",
    "  idleEnabled: false",
    "  midTurnEnabled: false",
    `  remoteEndpoint: ${JSON.stringify(remoteEndpoint)}`,
    "",
  ].join("\n");
}
function isStartNotice(text: string): boolean {
  return /(?:压缩|compaction|compact).*(?:已开始|starting|started)|(?:已开始|starting|started).*(?:压缩|compaction|compact)/i.test(text);
}
function isSuccessNotice(text: string): boolean {
  return /(?:compact|compaction|压缩).{0,40}(?:complete|completed|success|已完成|完成|成功|committed)|(?:complete|completed|success).{0,40}(?:compact|compaction)/i.test(text);
}

let channelId: string | undefined;
let fixture: Awaited<ReturnType<typeof createOctoProtocolFixture>> | undefined;
let modelServer: ReturnType<typeof Bun.serve> | undefined;
let db: Database | undefined;
let connection: FixtureConnection | undefined;
let nextRemotePhase: Phase | undefined;
let modelError: string | undefined;
let messageSequence = 50_000;
let automaticFollowUpBudget = 1;
let succeeded = false;
const children: Bun.Subprocess[] = [];
const childOutput: Promise<string>[] = [];
const generationSteps: string[] = [];
const providerRequests: ProviderRequest[] = [];
const remoteRequests: HeldRemoteRequest[] = [];
const remoteGates: Gate[] = [];

function recordModelError(message: string): Response {
  modelError ??= message;
  return new Response(message, { status: 400 });
}
function isCompactionSummaryRequest(body: Record<string, unknown>): boolean {
  if (!Array.isArray(body.messages)) return false;
  return body.messages.some(message => {
    if (!message || typeof message !== "object" || Array.isArray(message)) return false;
    const candidate = message as { role?: unknown; content?: unknown };
    return candidate.role === "system"
      && typeof candidate.content === "string"
      && candidate.content.includes("Summarize user–AI coding-assistant conversations");
  });
}
function isCompactionShortSummaryRequest(body: Record<string, unknown>): boolean {
  if (!Array.isArray(body.messages)) return false;
  return body.messages.some(message => {
    if (!message || typeof message !== "object" || Array.isArray(message)) return false;
    const candidate = message as { role?: unknown; content?: unknown };
    return candidate.role === "user"
      && typeof candidate.content === "string"
      && candidate.content.includes("Summarize conversation changes as a pull request description.");
  });
}
function activeCompactionPhase(): Phase | undefined {
  return nextRemotePhase ?? remoteRequests.at(-1)?.phase;
}
function remoteChatCompletion(text: string, body: Record<string, unknown>, index: number): Response {
  const promptTokens = Math.max(1, Math.ceil(JSON.stringify(body.messages ?? []).length / 4));
  return Response.json({
    id: `compact-notice-remote-${index}`,
    object: "chat.completion",
    created: 0,
    model: "probe",
    choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: "stop" }],
    usage: { prompt_tokens: promptTokens, completion_tokens: 12, total_tokens: promptTokens + 12 },
  });
}
function replyToGeneration(text: string, body: Record<string, unknown>, index: number): Response {
  const id = `compact-notice-${index}`;
  const promptTokens = Math.max(1, Math.ceil(JSON.stringify(body.messages ?? []).length / 4));
  const chunks = [
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: { role: "assistant", content: text }, finish_reason: null }],
    },
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: promptTokens, completion_tokens: 12, total_tokens: promptTokens + 12 },
    },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}
function messageText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}
function sentTexts(): string[] {
  return fixture!.sentMessages.map(messageText);
}
function findNotice(start: number, predicate: (text: string) => boolean): number {
  return noticeIndexes(start, predicate)[0] ?? -1;
}
function noticeIndexes(start: number, predicate: (text: string) => boolean): number[] {
  return fixture!.sentMessages.flatMap((message, index) =>
    index >= start && predicate(messageText(message)) ? [index] : [],
  );
}
async function send(text: string): Promise<string> {
  const activeFixture = fixture;
  if (!activeFixture) throw new Error("Local Octo fixture is not connected");
  await wait("Local Octo fixture connection did not complete CONNACK", () => {
    const candidate = activeFixture.connections.at(-1);
    if (
      candidate
      && !candidate.closed
      && candidate.socket.readyState === 1
      && candidate.connect
      && candidate.ready
      && candidate.aesKey
      && candidate.aesIv
    ) {
      connection = candidate;
      return true;
    }
    return false;
  }, 15_000);
  const activeConnection = connection;
  assert.ok(activeConnection, "Local Octo fixture did not retain its CONNACK-ready connection");
  const id = String(++messageSequence);
  activeFixture.sendInbound(activeConnection, {
    fromUid: activeFixture.ownerUid,
    channelId: activeFixture.ownerUid,
    channelType: 1,
    text,
    messageId: id,
  });
  return id;
}
function binding(): BoundSession | null {
  if (!db || !fixture || !channelId) return null;
  return db.query(
    "SELECT session_id, cwd, session_file FROM chats WHERE bot_uid = ? AND channel_type = 1 AND channel_id = ?",
  ).get(fixture.botUid, channelId) as BoundSession | null;
}
function task(messageId: string): TaskRow | null {
  if (!db) return null;
  return db.query("SELECT state, error_text FROM tasks WHERE message_id = ?").get(messageId) as TaskRow | null;
}
function activeWorkerCount(sessionId: string): number {
  return (db!.query(
    "SELECT COUNT(*) AS count FROM worker_ledger WHERE session_id = ? AND state IN ('running', 'stopping')",
  ).get(sessionId) as { count: number }).count;
}
async function sessionEntries(sessionFile: string): Promise<SessionEntry[]> {
  return (await readFile(sessionFile, "utf8"))
    .split(/\r?\n/)
    .filter(Boolean)
    .map(line => JSON.parse(line) as SessionEntry);
}
async function compactionEntries(sessionFile: string): Promise<SessionEntry[]> {
  return (await sessionEntries(sessionFile)).filter(entry => entry.type === "compaction");
}
async function nativeWire(): Promise<WireRecord[]> {
  const contents = await readFile(wirePath, "utf8");
  const lastCompleteLine = contents.lastIndexOf("\n");
  if (lastCompleteLine < 0) return [];
  return contents.slice(0, lastCompleteLine).split("\n").filter(Boolean).map(line => JSON.parse(line) as WireRecord);
}
async function wait(description: string, predicate: () => boolean | Promise<boolean>, timeoutMs = 45_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    assert.equal(modelError, undefined, `Finite loopback model failed: ${modelError}`);
    if (await predicate()) return;
    const tick = Promise.withResolvers<void>();
    setTimeout(tick.resolve, 5);
    await tick.promise;
  }
  throw new Error(description);
}
async function waitForTask(messageId: string, expectedText: string): Promise<void> {
  await wait(`Channel task ${messageId} did not complete with ${expectedText}`, () => {
    const row = task(messageId);
    if (row && ["failed", "cancelled", "interrupted"].includes(row.state)) {
      throw new Error(`Channel task ${messageId} ended ${row.state}: ${row.error_text ?? "no detail"}`);
    }
    return row?.state === "completed" && sentTexts().some(text => text.includes(expectedText));
  });
}
async function waitForWorkerCleanup(): Promise<void> {
  await wait("A settled task retained its real OMP worker", () => {
    const count = (db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number }).count;
    return count === 0;
  });
}
async function waitForExit(child: Bun.Subprocess, timeoutMs = 15_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = Promise.withResolvers<never>();
  try {
    timer = setTimeout(() => timeout.reject(new Error("Compact notice acceptance CLI did not exit")), timeoutMs);
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}
async function launch(configPath: string, root: string, binPath: string): Promise<Bun.Subprocess> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: root,
      PATH: `${binPath}:/usr/bin:/bin`,
      PI_CODING_AGENT_DIR: join(root, "agent"),
      PI_NO_TITLE: "1",
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  childOutput.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture!.waitForConnection(children.length, 15_000);
  await wait("Isolated channel CLI did not complete the local Octo CONNACK handshake", () =>
    !!connection
      && !connection.closed
      && connection.socket.readyState === 1
      && !!connection.connect
      && connection.ready
      && !!connection.aesKey
      && !!connection.aesIv,
    15_000,
  );
  return child;
}
async function readBoundSessionFile(): Promise<{ binding: BoundSession; entries: SessionEntry[]; compactions: SessionEntry[] }> {
  const current = binding();
  assert.ok(current, "Channel did not persist a chat binding");
  assert.equal(current.cwd, join(root, "cwd"), "The bound OMP session escaped its temporary workspace");
  assert.ok(current.session_file, "The real channel binding has no persisted OMP session file");
  assert.ok(resolve(current.session_file).startsWith(`${resolve(sessionRoot)}/`), "The bound session file escaped the isolated session root");
  const entries = await sessionEntries(current.session_file);
  return { binding: current, entries, compactions: entries.filter(entry => entry.type === "compaction") };
}
function findPromptRequest(frames: WireRecord[], text: string): WireRecord | undefined {
  return frames.find(frame => frame.direction === "host" && frame.type === "prompt" && frame.message === text);
}
function findPromptAck(frames: WireRecord[], request: WireRecord | undefined): WireRecord | undefined {
  // OMP reuses request ids after worker restarts; match only the response following this request.
  if (!request?.id) return undefined;
  const requestIndex = frames.indexOf(request);
  if (requestIndex < 0) return undefined;
  return frames.slice(requestIndex + 1).find(
    frame => frame.direction === "omp" && frame.type === "response" && frame.id === request.id,
  );
}
function compactionEvent(frames: WireRecord[], type: string): WireRecord | undefined {
  return frames.find(frame => frame.direction === "omp" && frame.type === type);
}
function forwardedCompactionEvent(
  frames: WireRecord[],
  type: string,
  expectedSummary: string,
): WireRecord | undefined {
  return frames.find(frame =>
    frame.direction === "forwarded"
    && frame.type === type
    && frame.hasResult === true
    && frame.aborted === false
    && frame.skipped !== true
    && frame.willRetry !== true
    && frame.errorMessage === undefined
    && frame.resultSummary?.includes(expectedSummary),
  );
}
function committedCompactionEnd(
  frames: WireRecord[],
  type: string,
  expectedSummary?: string,
): WireRecord | undefined {
  return frames.find(frame =>
    frame.direction === "omp"
    && frame.type === type
    && frame.hasResult === true
    && frame.aborted === false
    && frame.skipped !== true
    && frame.willRetry !== true
    && frame.errorMessage === undefined
    && (expectedSummary === undefined || frame.resultSummary?.includes(expectedSummary)),
  );
}
function latestAutomaticPassStart(frames: WireRecord[]): WireRecord | undefined {
  const lastEnd = frames
    .filter(frame => frame.direction === "omp" && frame.type === "auto_compaction_end")
    .at(-1);
  return frames
    .filter(frame =>
      frame.direction === "omp"
      && frame.type === "auto_compaction_start"
      && (!lastEnd || frame.sequence > lastEnd.sequence),
    )
    .at(-1);
}
function assertNativeResult(event: WireRecord | undefined, type: string, expectedSummary: string): asserts event is WireRecord {
  assert.ok(event, `Installed OMP did not emit ${type}`);
  assert.equal(event.hasResult, true, `${type} lacked the native result that alone proves compaction committed`);
  assert.equal(event.aborted, false, `${type} reported an aborted compaction`);
  assert.notEqual(event.skipped, true, `${type} reported a skipped compaction`);
  assert.equal(event.errorMessage, undefined, `${type} reported an error`);
  assert.notEqual(event.willRetry, true, `${type} reported a pending retry`);
  assert.ok(event.resultSummary?.includes(expectedSummary), `${type} omitted the finite remote summary result`);
}

try {
  for (const directory of ["bin", "agent", "cwd"]) await mkdir(join(root, directory));
  await writeFile(wirePath, "");
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }

  modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        return recordModelError("Loopback model received a non-JSON request");
      }
      if (new URL(request.url).hostname !== "127.0.0.1") return recordModelError("A provider request escaped the loopback model fixture");
      providerRequests.push({ atMs: Date.now(), url: request.url, body });
      // The short PR blurb is auxiliary; answer it without consuming the primary result gate.
      if (body.stream === false && isCompactionShortSummaryRequest(body)) {
        const phase = activeCompactionPhase();
        if (!phase) return recordModelError("OMP issued an unarmed compaction short-summary request");
        return remoteChatCompletion(`FIXTURE_${phase.toUpperCase()}_COMPACTION_SHORT_SUMMARY`, body, providerRequests.length);
      }
      // Hold the primary native remote result behind its gate; this endpoint uses OpenAI's chat-completions response shape.
      if (body.stream === false) {
        const phase = nextRemotePhase;
        if (!phase) return recordModelError("OMP requested a second primary remote compaction");
        nextRemotePhase = undefined;
        const gate = deferred();
        remoteGates.push(gate);
        remoteRequests.push({ atMs: Date.now(), url: request.url, body, phase, gate });
        await gate.promise;
        return remoteChatCompletion(`FIXTURE_${phase.toUpperCase()}_REMOTE_COMPACTION_RESULT`, body, providerRequests.length);
      }
      // A split-turn compaction can also request a local summary in parallel. Reply
      // without consuming the channel-task script or changing the remote gate.
      if (isCompactionSummaryRequest(body)) {
        const phase = activeCompactionPhase();
        if (!phase) return recordModelError("OMP issued an unarmed finite compaction summary request");
        return replyToGeneration(`FIXTURE_${phase.toUpperCase()}_COMPACTION_AUX_SUMMARY`, body, providerRequests.length);
      }
      let step = generationSteps.shift();
      if (!step && automaticFollowUpBudget > 0 && JSON.stringify(body.messages ?? []).includes("AUTOMATIC_COMPACTION_TRIGGER")) {
        automaticFollowUpBudget -= 1;
        step = "AUTOMATIC_COMPACTION_FOLLOW_UP_REPLY";
      }
      if (!step) return recordModelError("Finite local model had no scripted completion for OMP");
      return replyToGeneration(step, body, providerRequests.length);
    },
  });
  assert.ok(modelServer.port, "Finite loopback model did not bind a TCP port");
  const remoteEndpoint = `http://127.0.0.1:${modelServer.port}/v1/chat/completions`;
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({
    providers: {
      "local-compact-notices": {
        baseUrl: `http://127.0.0.1:${modelServer.port}/v1`,
        apiKey: "local-compact-notices-only",
        api: "openai-completions",
        models: [{
          id: "probe",
          name: "Compact Notices Acceptance Probe",
          reasoning: false,
          input: ["text"],
          contextWindow: 32_768,
          maxTokens: 2_048,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        }],
      },
    },
  }));
  await writeFile(nativeConfigPath, writeNativeConfig(remoteEndpoint, 95));
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must bind to loopback");
  channelId = fixture.ownerUid;
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [],
    stateDir: stateRoot,
    sessionsDir: sessionRoot,
    ompCommand: join(root, "real-omp.ts"),
    ompExtraArgs: [
      "--model", "local-compact-notices/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", nativeConfigPath,
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, `#!${process.execPath}
import { appendFileSync, existsSync, writeFileSync } from 'node:fs';
const child = Bun.spawn([${JSON.stringify(realOmp)}, ...process.argv.slice(2)], {
  stdin: 'pipe', stdout: 'pipe', stderr: 'inherit', env: process.env,
});
const wirePath = ${JSON.stringify(wirePath)};
const gates = {
  manual: { hold: ${JSON.stringify(manualEndHoldPath)}, release: ${JSON.stringify(manualEndReleasePath)} },
  automatic: { hold: ${JSON.stringify(automaticEndHoldPath)}, release: ${JSON.stringify(automaticEndReleasePath)} },
};
let heldPhase;
let shutdownRequested = false;
for (const signal of ['SIGTERM', 'SIGINT', 'SIGHUP']) {
  process.on(signal, () => {
    shutdownRequested = true;
    if (heldPhase) writeFileSync(gates[heldPhase].release, 'shutdown');
    child.kill(signal);
  });
}
let sequence = 0;
function record(direction, line) {
  const frame = JSON.parse(line);
  const data = frame.data && typeof frame.data === 'object' && !Array.isArray(frame.data) ? frame.data : undefined;
  const result = frame.result && typeof frame.result === 'object' && !Array.isArray(frame.result) ? frame.result : undefined;
  const message = typeof frame.message === 'string' ? frame.message : undefined;
  const endPhase = frame.type === 'manual_compaction_end'
    ? 'manual'
    : frame.type === 'auto_compaction_end'
      ? 'automatic'
      : undefined;
  const holdsNativeResult = direction === 'omp'
    && endPhase !== undefined
    && !!result
    && frame.aborted === false
    && frame.skipped !== true
    && frame.willRetry !== true
    && typeof frame.errorMessage !== 'string'
    && existsSync(gates[endPhase].hold)
    && !existsSync(gates[endPhase].release);
  const heldForHost = holdsNativeResult;
  const row = {
    sequence: ++sequence,
    atMs: Date.now(),
    direction,
    heldForHost: heldForHost ? true : undefined,
    type: typeof frame.type === 'string' ? frame.type : undefined,
    id: typeof frame.id === 'string' ? frame.id : undefined,
    message: message === undefined ? undefined : message.slice(0, 160),
    messageLength: message?.length,
    success: typeof frame.success === 'boolean' ? frame.success : undefined,
    agentInvoked: typeof data?.agentInvoked === 'boolean' ? data.agentInvoked : undefined,
    mode: typeof frame.mode === 'string' ? frame.mode : undefined,
    reason: typeof frame.reason === 'string' ? frame.reason : undefined,
    action: typeof frame.action === 'string' ? frame.action : undefined,
    hasResult: frame.result === undefined ? undefined : !!result,
    resultSummary: typeof result?.summary === 'string' ? result.summary.slice(0, 160) : undefined,
    aborted: typeof frame.aborted === 'boolean' ? frame.aborted : undefined,
    skipped: typeof frame.skipped === 'boolean' ? frame.skipped : undefined,
    errorMessage: typeof frame.errorMessage === 'string' ? frame.errorMessage.slice(0, 160) : undefined,
    willRetry: typeof frame.willRetry === 'boolean' ? frame.willRetry : undefined,
  };
  appendFileSync(wirePath, JSON.stringify(row) + '\\n');
  return frame;
}
async function maybeHoldEnd(frame) {
  const phase = frame.type === 'manual_compaction_end' ? 'manual' : frame.type === 'auto_compaction_end' ? 'automatic' : undefined;
  const result = frame.result && typeof frame.result === 'object' && !Array.isArray(frame.result) ? frame.result : undefined;
  if (
    !phase
    || !result
    || frame.aborted !== false
    || frame.skipped === true
    || frame.willRetry === true
    || typeof frame.errorMessage === 'string'
    || !existsSync(gates[phase].hold)
  ) return;
  if (shutdownRequested) writeFileSync(gates[phase].release, 'shutdown');
  try {
    while (!existsSync(gates[phase].release)) await Bun.sleep(10);
  } finally {
    heldPhase = undefined;
  }
}
async function relayInput() {
  let pending = '';
  const decoder = new TextDecoder();
  for await (const bytes of Bun.stdin.stream()) {
    pending += decoder.decode(bytes, { stream: true });
    for (;;) {
      const end = pending.indexOf('\\n');
      if (end < 0) break;
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (!line.trim()) continue;
      record('host', line);
      child.stdin.write(line + '\\n');
    }
  }
  pending += decoder.decode();
  if (pending.trim()) {
    record('host', pending);
    child.stdin.write(pending + '\\n');
  }
  child.stdin.end();
}
async function relayOutput() {
  let pending = '';
  const decoder = new TextDecoder();
  for await (const bytes of child.stdout) {
    pending += decoder.decode(bytes, { stream: true });
    for (;;) {
      const end = pending.indexOf('\\n');
      if (end < 0) break;
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (!line.trim()) continue;
      const frame = record('omp', line);
      await maybeHoldEnd(frame);
      const isCompactionEnd = frame.type === 'manual_compaction_end' || frame.type === 'auto_compaction_end';
      if (!isCompactionEnd) {
        process.stdout.write(line + '\\n');
        continue;
      }
      const flushed = Promise.withResolvers();
      process.stdout.write(line + '\\n', flushed.resolve);
      await flushed.promise;
      appendFileSync(wirePath, JSON.stringify({
        sequence: ++sequence,
        atMs: Date.now(),
        direction: 'forwarded',
        type: frame.type,
        id: typeof frame.id === 'string' ? frame.id : undefined,
        action: typeof frame.action === 'string' ? frame.action : undefined,
        hasResult: frame.result !== undefined && !!frame.result && typeof frame.result === 'object',
        resultSummary: typeof frame.result?.summary === 'string' ? frame.result.summary.slice(0, 160) : undefined,
        aborted: typeof frame.aborted === 'boolean' ? frame.aborted : undefined,
        skipped: typeof frame.skipped === 'boolean' ? frame.skipped : undefined,
        errorMessage: typeof frame.errorMessage === 'string' ? frame.errorMessage.slice(0, 160) : undefined,
        willRetry: typeof frame.willRetry === 'boolean' ? frame.willRetry : undefined,
      }) + '\\n');
    }
  }
  pending += decoder.decode();
  if (pending.trim()) throw new Error('Installed OMP ended with an unterminated RPC frame');
}
await Promise.all([relayInput(), relayOutput()]);
process.exit(await child.exited);
`, { mode: 0o755 });

  const binPath = join(root, "bin");
  const child = await launch(configPath, root, binPath);
  db = new Database(join(stateRoot, "channel.sqlite"), { readonly: true });

  const newStart = fixture.sentMessages.length;
  const newMessage = await send(`/new ${join(root, "cwd")}`);
  await wait("Real channel /new did not bind the isolated OMP session", () =>
    binding()?.cwd === join(root, "cwd")
      && fixture!.sentMessages.slice(newStart).some(message => messageText(message).includes(`cwd=${join(root, "cwd")}`)),
  );
  assert.equal(task(newMessage), null, "/new unexpectedly entered the normal task path");

  generationSteps.push(
    "MANUAL_COMPACTION_SEED_REPLY_1",
    "MANUAL_COMPACTION_SEED_REPLY_2",
    "MANUAL_COMPACTION_SEED_REPLY_3",
  );
  for (let turn = 1; turn <= 3; turn += 1) {
    const seedMessage = await send(`SEED_REAL_OMP_SESSION_FOR_REMOTE_COMPACTION_TURN_${turn}`);
    await waitForTask(seedMessage, `MANUAL_COMPACTION_SEED_REPLY_${turn}`);
    await waitForWorkerCleanup();
  }
  const seedSession = await readBoundSessionFile();
  assert.equal(seedSession.compactions.length, 0, "Seed turns unexpectedly committed a compaction");
  const sessionFile = seedSession.binding.session_file!;

  // Manual /compact remote: hold the actual remote compactor response. The wrapper records the
  // native end/result and gates its delivery to the host, letting this bench distinguish a
  // committed native result from a speculative/precomputed summary.
  const manualNoticeStart = fixture.sentMessages.length;
  // Scope RPC matching to this phase because OMP ids and wrapper sequences restart per worker.
  await rm(manualEndReleasePath, { force: true });
  await rm(manualEndHoldPath, { force: true });
  await writeFile(manualEndHoldPath, "hold");
  const manualWireStart = (await nativeWire()).length;
  nextRemotePhase = "manual";
  await send("/compact remote");
  await wait(
    "Manual /compact remote did not acknowledge, announce its native start, and reach the held loopback compactor before the RPC ACK deadline",
    async () => {
      const frames = (await nativeWire()).slice(manualWireStart);
      const request = findPromptRequest(frames, "/compact remote");
      const ack = findPromptAck(frames, request);
      const start = compactionEvent(frames, "manual_compaction_start");
      return !!request
        && !!ack
        && ack.success === true
        && ack.agentInvoked === false
        && start?.mode === "remote"
        && remoteRequests.some(remote => remote.phase === "manual")
        && findNotice(manualNoticeStart, isStartNotice) >= 0;
    },
    22_000,
  );
  const manualFramesAtHold = (await nativeWire()).slice(manualWireStart);
  const manualPromptFrame = findPromptRequest(manualFramesAtHold, "/compact remote")!;
  const manualAck = findPromptAck(manualFramesAtHold, manualPromptFrame)!;
  const manualStartEvent = compactionEvent(manualFramesAtHold, "manual_compaction_start")!;
  const manualRemote = remoteRequests.find(remote => remote.phase === "manual")!;
  const manualStartNoticeIndex = findNotice(manualNoticeStart, isStartNotice);
  assert.equal(manualStartEvent.mode, "remote", "Native OMP did not honor the explicit /compact remote mode");
  assert.equal(manualAck.agentInvoked, false, "The native /compact command was not acknowledged as local-only");
  assert.equal(compactionEvent(manualFramesAtHold, "manual_compaction_end"), undefined,
    "Manual compaction emitted an end event while its remote summary was still blocked");
  assert.equal(findNotice(manualNoticeStart, isSuccessNotice), -1,
    "Channel announced successful manual compaction before the remote result committed");
  assert.equal((await compactionEntries(sessionFile)).length, 0,
    "A speculative summary was mistaken for a committed manual compaction entry");
  assert.ok(activeWorkerCount(seedSession.binding.session_id) > 0,
    "Local /compact acknowledgement closed the channel's real OMP worker before native compaction finished");
  const manualStartText = messageText(fixture.sentMessages[manualStartNoticeIndex]!);
  console.log("CASE manual-remote-pending " + JSON.stringify({
    command: manualPromptFrame.message,
    promptSequence: manualPromptFrame.sequence,
    ackSequence: manualAck.sequence,
    ackAtMs: manualAck.atMs,
    startSequence: manualStartEvent.sequence,
    startAtMs: manualStartEvent.atMs,
    ackBeforeStart: manualAck.sequence < manualStartEvent.sequence,
    remoteRequestAtMs: manualRemote.atMs,
    startNotice: manualStartText.slice(0, 180),
    workerStillOwned: true,
    completionNoticeBeforeRemoteResult: false,
    committedEntryBeforeRemoteResult: false,
  }));

  manualRemote.gate.resolve();
  await wait("Native OMP did not expose the committed manual compaction result", async () =>
    !!committedCompactionEnd(
      (await nativeWire()).slice(manualWireStart),
      "manual_compaction_end",
      "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT",
    ),
  );
  const manualEndFrames = (await nativeWire()).slice(manualWireStart);
  const manualEndHeld = committedCompactionEnd(
    manualEndFrames,
    "manual_compaction_end",
    "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT",
  )!;
  assertNativeResult(manualEndHeld, "manual_compaction_end", "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT");
  assert.equal(manualEndHeld.heldForHost, true, "Wrapper did not hold the native end frame for host delivery");
  assert.equal(forwardedCompactionEvent(
    manualEndFrames,
    "manual_compaction_end",
    "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT",
  ), undefined, "Wrapper forwarded the manual end frame before the explicit release");
  assert.ok(manualEndHeld.sequence > manualStartEvent.sequence, "Native manual end preceded its start event");
  assert.ok(manualEndHeld.sequence > manualAck.sequence, "Native manual success preceded the local prompt ACK");
  const afterManualCommit = await compactionEntries(sessionFile);
  assert.equal(afterManualCommit.length, 1, "Native manual compaction did not persist exactly one compaction entry");
  assert.equal(afterManualCommit[0]?.method, "remote", "Persisted manual entry was not produced by remote compaction");
  assert.ok(afterManualCommit[0]?.summary?.includes("FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT"),
    "Persisted manual session entry omitted the actual remote summary");
  assert.equal(findNotice(manualNoticeStart, isSuccessNotice), -1,
    "Manual success notice escaped before the native end/result frame reached the host");
  await writeFile(manualEndReleasePath, "release");
  await wait("Wrapper did not forward the native manual end frame after release", async () =>
    !!forwardedCompactionEvent(
      (await nativeWire()).slice(manualWireStart),
      "manual_compaction_end",
      "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT",
    ),
  );
  const manualEndForwarded = forwardedCompactionEvent(
    (await nativeWire()).slice(manualWireStart),
    "manual_compaction_end",
    "FIXTURE_MANUAL_REMOTE_COMPACTION_RESULT",
  )!;
  assert.ok(manualEndForwarded.atMs >= manualEndHeld.atMs, "Manual end frame forwarding preceded its native event");
  await wait("Channel did not announce the committed manual remote compaction", () => findNotice(manualNoticeStart, isSuccessNotice) >= 0, 15_000);
  await waitForWorkerCleanup();
  await rm(manualEndHoldPath, { force: true });
  await rm(manualEndReleasePath, { force: true });
  const manualCompletionIndex = findNotice(manualNoticeStart, isSuccessNotice);
  assert.ok(manualCompletionIndex >= 0);
  console.log("CASE manual-remote-committed " + JSON.stringify({
    endSequence: manualEndHeld.sequence,
    nativeEndAtMs: manualEndHeld.atMs,
    forwardedAtMs: manualEndForwarded.atMs,
    endHasNativeResult: manualEndHeld.hasResult,
    resultSummary: manualEndHeld.resultSummary,
    persistedCompactionEntries: afterManualCommit.length,
    persistedMethod: afterManualCommit[0]?.method,
    completionNotice: messageText(fixture.sentMessages[manualCompletionIndex]!).slice(0, 180),
  }));

  // Lower the threshold enough to trigger on real prompt pressure while keeping fixed system and
  // tool context below the recovery band, so any pre-prompt skip cannot be confused with success.
  await writeFile(nativeConfigPath, writeNativeConfig(remoteEndpoint, 50));
  generationSteps.push("AUTOMATIC_COMPACTION_TRIGGER_REPLY");
  const autoNoticeStart = fixture.sentMessages.length;
  await rm(automaticEndReleasePath, { force: true });
  await rm(automaticEndHoldPath, { force: true });
  await writeFile(automaticEndHoldPath, "hold");
  const autoWireStart = (await nativeWire()).length;
  nextRemotePhase = "automatic";
  const largePrompt = `AUTOMATIC_COMPACTION_TRIGGER ${"bounded fixture context. ".repeat(2_400)}`;
  const autoMessage = await send(largePrompt);
  await wait(
    "Automatic compaction did not start from the real bound session and reach the held loopback compactor",
    async () => {
      const frames = (await nativeWire()).slice(autoWireStart);
      const start = latestAutomaticPassStart(frames);
      const starts = frames.filter(frame =>
        frame.direction === "omp" && frame.type === "auto_compaction_start",
      );
      return !!start
        && start.action === "remote"
        && remoteRequests.some(remote => remote.phase === "automatic")
        && noticeIndexes(autoNoticeStart, isStartNotice).length >= starts.length;
    },
    30_000,
  );
  const autoFramesAtHold = (await nativeWire()).slice(autoWireStart);
  const autoStartEvent = latestAutomaticPassStart(autoFramesAtHold)!;
  const automaticRemote = remoteRequests.find(remote => remote.phase === "automatic")!;
  const skippedAutoEnds = autoFramesAtHold.filter(frame =>
    frame.direction === "omp"
    && frame.type === "auto_compaction_end"
    && frame.skipped === true,
  );
  assert.equal(autoStartEvent.action, "remote", "Automatic compaction did not select the configured real remote method");
  assert.equal(autoStartEvent.reason, "threshold", "Automatic compaction was not caused by context pressure");
  assert.ok(skippedAutoEnds.every(end => end.sequence < autoStartEvent.sequence),
    "Automatic pass start was not distinct from its preceding skipped attempt");
  assert.equal(committedCompactionEnd(autoFramesAtHold, "auto_compaction_end"), undefined,
    "Automatic compaction committed before its remote summary was released");
  assert.equal(findNotice(autoNoticeStart, isSuccessNotice), -1,
    "Channel announced automatic compaction success before the remote result committed");
  assert.equal((await compactionEntries(sessionFile)).length, 1,
    "An uncommitted automatic summary changed the persisted session history");
  assert.ok(activeWorkerCount(seedSession.binding.session_id) > 0,
    "Automatic compaction lost the channel's OMP worker while its real summary was pending");
  const autoStartNoticeIndex = noticeIndexes(autoNoticeStart, isStartNotice).at(-1)!;
  console.log("CASE automatic-remote-pending " + JSON.stringify({
    triggerMessageId: autoMessage,
    startSequence: autoStartEvent.sequence,
    startAtMs: autoStartEvent.atMs,
    reason: autoStartEvent.reason,
    action: autoStartEvent.action,
    skippedAutomaticAttempts: skippedAutoEnds.length,
    remoteRequestAtMs: automaticRemote.atMs,
    startNotice: messageText(fixture.sentMessages[autoStartNoticeIndex]!).slice(0, 180),
    speculativeCompactionDisabled: true,
    completionNoticeBeforeNativeResult: false,
  }));

  automaticRemote.gate.resolve();
  await wait("Native OMP did not expose the committed automatic compaction result", async () =>
    !!committedCompactionEnd(
      (await nativeWire()).slice(autoWireStart),
      "auto_compaction_end",
      "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT",
    ),
  );
  const autoEndFrames = (await nativeWire()).slice(autoWireStart);
  const autoEndHeld = committedCompactionEnd(
    autoEndFrames,
    "auto_compaction_end",
    "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT",
  )!;
  assertNativeResult(autoEndHeld, "auto_compaction_end", "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT");
  assert.equal(autoEndHeld.heldForHost, true, "Wrapper did not hold the native automatic end frame");
  assert.equal(forwardedCompactionEvent(
    autoEndFrames,
    "auto_compaction_end",
    "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT",
  ), undefined, "Wrapper forwarded the automatic end frame before the explicit release");
  assert.ok(autoEndHeld.sequence > autoStartEvent.sequence, "Native automatic end preceded its start event");
  assert.equal(autoEndHeld.action, "remote", "Automatic end event did not preserve the committed remote action");
  const afterAutoCommit = await compactionEntries(sessionFile);
  assert.equal(afterAutoCommit.length, 2, "Native automatic compaction did not persist its own compaction entry");
  assert.equal(afterAutoCommit[1]?.method, "remote", "Persisted automatic entry was not produced by the remote method");
  assert.ok(afterAutoCommit[1]?.summary?.includes("FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT"),
    "Persisted automatic entry omitted the actual remote summary");
  assert.equal(findNotice(autoNoticeStart, isSuccessNotice), -1,
    "Automatic success notice escaped before its native end/result frame reached the host");
  await writeFile(automaticEndReleasePath, "release");
  await wait("Wrapper did not forward the native automatic end frame after release", async () =>
    !!forwardedCompactionEvent(
      (await nativeWire()).slice(autoWireStart),
      "auto_compaction_end",
      "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT",
    ),
  );
  const autoEndForwarded = forwardedCompactionEvent(
    (await nativeWire()).slice(autoWireStart),
    "auto_compaction_end",
    "FIXTURE_AUTOMATIC_REMOTE_COMPACTION_RESULT",
  )!;
  assert.ok(autoEndForwarded.atMs >= autoEndHeld.atMs, "Automatic end frame forwarding preceded its native event");
  await wait("Channel did not announce committed automatic remote compaction", () => findNotice(autoNoticeStart, isSuccessNotice) >= 0, 15_000);
  const autoCompletionIndex = findNotice(autoNoticeStart, isSuccessNotice);
  assert.ok(autoCompletionIndex >= 0);
  await waitForTask(autoMessage, "AUTOMATIC_COMPACTION_TRIGGER_REPLY");
  await waitForWorkerCleanup();
  await rm(automaticEndHoldPath, { force: true });
  await rm(automaticEndReleasePath, { force: true });
  console.log("CASE automatic-remote-committed " + JSON.stringify({
    endSequence: autoEndHeld.sequence,
    nativeEndAtMs: autoEndHeld.atMs,
    forwardedAtMs: autoEndForwarded.atMs,
    endHasNativeResult: autoEndHeld.hasResult,
    resultSummary: autoEndHeld.resultSummary,
    persistedCompactionEntries: afterAutoCommit.length,
    persistedMethod: afterAutoCommit[1]?.method,
    completionNotice: messageText(fixture.sentMessages[autoCompletionIndex]!).slice(0, 180),
  }));

  assert.equal(modelError, undefined);
  assert.equal(generationSteps.length, 0, "The finite model left an unconsumed generation response");
  assert.equal(nextRemotePhase, undefined, "The finite model did not receive its armed remote compaction request");
  assert.equal(remoteGates.length, 2, "The acceptance did not run exactly two remote compactions");
  assert.ok(providerRequests.length <= 10, "The isolated model exceeded its finite generation/compaction request budget");
  assert.ok(providerRequests.every(request => new URL(request.url).hostname === "127.0.0.1"),
    "At least one request escaped the loopback provider fixture");
  assert.equal(child.exitCode, null, "Channel CLI exited before controlled shutdown");
  child.kill("SIGTERM");
  assert.equal(await waitForExit(child), 0, "Channel CLI did not stop cleanly on SIGTERM");
  const logs = (await Promise.all(childOutput)).join("\n");
  assert.ok(!logs.includes(fixture.botToken), "The channel CLI leaked the local fixture bot token");
  succeeded = true;
  console.log(JSON.stringify({
    realOmp,
    isolatedProfile: true,
    boundChat: channelId,
    loopbackProviderRequests: providerRequests.length,
    manualRemoteCompaction: true,
    automaticRemoteCompaction: true,
    nativeResultsObservedBeforeSuccessNotices: true,
    persistedCompactionEntries: 2,
    encryptedLocalOctoFixture: true,
    residentServiceTouched: false,
  }));
} catch (error) {
  const redact = (value: string): string => fixture ? value.replaceAll(fixture.botToken, "[fixture-token]") : value;
  const frames = await nativeWire().catch(() => [] as WireRecord[]);
  const tasks = db?.query("SELECT message_id, state, error_text FROM tasks ORDER BY rowid").all() ?? [];
  console.error(JSON.stringify({
    root,
    failure: redact(error instanceof Error ? error.stack ?? error.message : String(error)),
    modelError,
    providerRequests: providerRequests.map((request, index) => ({
      index,
      stream: request.body.stream,
      messageCount: Array.isArray(request.body.messages) ? request.body.messages.length : null,
      requestAtMs: request.atMs,
    })),
    remoteCompactions: remoteRequests.map(request => ({
      phase: request.phase,
      requestAtMs: request.atMs,
      summaryHeld: true,
      model: request.body.model,
    })),
    wireTimeline: frames.filter(frame =>
      frame.type === "prompt" || frame.type === "response" || frame.type?.includes("compaction"),
    ),
    sentMessages: fixture?.sentMessages.slice(-18).map(message => redact(messageText(message).slice(0, 400))) ?? [],
    tasks: tasks.map(value => {
      const row = value as { message_id: string; state: string; error_text: string | null };
      return { ...row, error_text: row.error_text === null ? null : redact(row.error_text) };
    }),
    binding: binding(),
  }));
  process.exitCode = 1;
  throw error;
} finally {
  for (const gate of remoteGates) gate.resolve();
  for (const child of children) {
    if (child.exitCode !== null) continue;
    child.kill("SIGTERM");
    try {
      await waitForExit(child, 10_000);
    } catch {
      child.kill("SIGKILL");
      await child.exited;
    }
  }
  await Promise.allSettled(children.map(child => child.exited));
  db?.close();
  await fixture?.close();
  modelServer?.stop(true);
  if (succeeded) await rm(root, { recursive: true, force: true });
  else console.error(`Isolated compaction acceptance workspace retained for inspection: ${root}`);
}
