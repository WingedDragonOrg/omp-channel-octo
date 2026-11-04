import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type FixtureGroupContext,
  type FixtureSentMessage,
} from "../fixtures/octo-protocol-server.ts";
import { AGENT_GUIDANCE_PREFIX, MEMBER_CONTEXT_PREFIX, formatAgentGuidance } from "../../src/session/agent-context.ts";

// Opt-in behavior acceptance: the installed OMP executable, a finite loopback model, and the
// encrypted local Octo fixture. The CLI runs in an isolated HOME; this never contacts a resident
// service, a bot, or a real model provider.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-context-acceptance-")));
const groupId = "context-stability-parent-identity-7f3c9d";
const targetThreadId = `${groupId}____thread-0`;
const sourceTarget: Target = { channelType: 2, channelId: groupId };
const sourceContext: FixtureGroupContext = {
  content: "### Memory Start ###\nSOURCE_MEMORY_V1\n### Memory End ###\nSOURCE_GROUP_DESCRIPTION_V1",
  version: 1,
  updatedAt: "2026-11-04T00:00:00.000Z",
  updatedBy: "fixture-owner",
};
const targetContext: FixtureGroupContext = {
  content: "### Memory Start ###\nFORK_THREAD_MEMORY_FRESH\n### Memory End ###\nFORK_THREAD_DESCRIPTION_FRESH",
  version: 1,
  updatedAt: "2026-11-04T00:01:00.000Z",
  updatedBy: "fixture-owner",
};
const fixtureGroupMembersPath = `/v1/bot/groups/${groupId}/members`;
const fixtureThreadMembersPath = `/v1/bot/groups/${groupId}/threads/thread-0/members`;
const managementBodies: Record<string, unknown> = {
  [`GET ${fixtureGroupMembersPath}`]: [member("member-alpha-uid", "Alpha"), member("member-beta-uid", "Beta")],
  [`GET ${fixtureThreadMembersPath}`]: [member("thread-member-uid", "Thread Member")],
};
const fixture = await createOctoProtocolFixture({
  cardProfileBody: { enabled: false },
  botEventsStatus: 404,
  groupContexts: { [groupId]: sourceContext, [targetThreadId]: targetContext },
  managementBodies,
});
assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must bind to loopback");

interface Target {
  channelType: 2 | 5;
  channelId: string;
}
interface ModelRequest {
  readonly url: string;
  readonly body: Record<string, unknown>;
}
interface Gate {
  readonly promise: Promise<void>;
  resolve(): void;
}
type Step = {
  readonly match?: (body: Record<string, unknown>) => boolean;
  readonly text?: string;
  readonly tool?: { name: string; args: unknown | ((body: Record<string, unknown>) => unknown); id: string };
  readonly gate?: Gate;
};
type ContextBinding = { session_id: string; cwd: string; session_file: string | null };
type TaskRow = { state: string; error_text: string | null };
type InboundRow = { kind: string; command_state: string };
type PreservedContextSnapshot = {
  content: string;
  present: boolean;
  version: number;
  updatedAt: string;
  updatedBy: string;
};
type PreservedCompactionContext = {
  schemaVersion: number;
  requestId: string;
  botUid: string;
  target: Target;
  sessionId: string;
  turnId: string;
  attemptId: string;
  snapshot: PreservedContextSnapshot;
};
type OmpSessionEntry = {
  type?: string;
  preserveData?: { ompChannelOctoContext?: PreservedCompactionContext } | null;
};

const children: Bun.Subprocess[] = [];
const outputs: Promise<string>[] = [];
const gates: Gate[] = [];
const requests: ModelRequest[] = [];
const steps: Step[] = [];
const wirePath = join(root, "omp-wire.jsonl");
const configPath = join(root, "config.json");
const binPath = join(root, "bin");
const agentPath = join(root, "agent");
const cwdPath = join(root, "cwd");
let modelError: string | undefined;
let compactionReplyBudget = 0;
let modelServer: ReturnType<typeof Bun.serve> | undefined;
let connection: FixtureConnection | undefined;
let child: Bun.Subprocess | undefined;
let db: Database | undefined;
let messageSequence = 30_000;
let succeeded = false;

function member(uid: string, name: string): Record<string, unknown> {
  return { uid, name, role: 0, robot: 0, created_at: "2026-11-04T00:00:00.000Z" };
}
function gate(): Gate {
  const deferred = Promise.withResolvers<void>();
  let released = false;
  const result: Gate = {
    promise: deferred.promise,
    resolve() {
      if (released) return;
      released = true;
      deferred.resolve();
    },
  };
  gates.push(result);
  return result;
}
function messageText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}
function messageContains(target: Target, fragment: string, start = 0): boolean {
  return fixture.sentMessages.slice(start).some(message =>
    message.body.channel_id === target.channelId && messageText(message).includes(fragment),
  );
}
function send(target: Target, text: string): string {
  if (!connection) throw new Error("Octo fixture is not connected");
  const messageId = String(++messageSequence);
  const mention = {
    entities: [{ uid: fixture.botUid, offset: 0, length: `@${fixture.botName}`.length }],
  };
  fixture.sendInbound(connection, {
    fromUid: fixture.ownerUid,
    channelId: target.channelId,
    channelType: target.channelType,
    text: `@${fixture.botName} ${text}`,
    mention,
    messageId,
  });
  return messageId;
}
function binding(target: Target): ContextBinding | null {
  return db!.query(
    "SELECT session_id, cwd, session_file FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(fixture.botUid, target.channelType, target.channelId) as ContextBinding | null;
}
function task(messageId: string): TaskRow | null {
  return db!.query("SELECT state, error_text FROM tasks WHERE message_id = ?").get(messageId) as TaskRow | null;
}
function inbound(messageId: string): InboundRow | null {
  return db!.query("SELECT kind, command_state FROM inbound WHERE message_id = ?").get(messageId) as InboundRow | null;
}
function memberReadCount(target: Target = sourceTarget): number {
  const path = target.channelType === 5
    ? `/v1/bot/groups/${groupId}/threads/${target.channelId.split("____")[1]}/members`
    : fixtureGroupMembersPath;
  return fixture.managementRequests.filter(request => request.method === "GET" && request.path === path).length;
}
function contextReadCount(target: Target = sourceTarget): number {
  return fixture.groupContextRequests.filter(request => request.target === target.channelId).length;
}
function requestMessages(body: Record<string, unknown>): unknown[] {
  return Array.isArray(body.messages) ? body.messages : [];
}
function textBlocks(value: unknown): string[] {
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap(textBlocks);
  if (value === null || typeof value !== "object") return [];
  const record = value as Record<string, unknown>;
  if (typeof record.text === "string") return [record.text];
  if (record.content !== undefined) return textBlocks(record.content);
  return [];
}
function providerMessageText(value: unknown): string {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return textBlocks(value).join("\n");
  return textBlocks((value as Record<string, unknown>).content).join("\n");
}
function requestMessageText(body: Record<string, unknown>): string {
  return requestMessages(body).map(providerMessageText).join("\n");
}
function requestSystemMessages(body: Record<string, unknown>): string[] {
  const system = body.system === undefined ? [] : textBlocks(body.system);
  if (!Array.isArray(body.messages)) return system;
  for (const value of body.messages) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
    const message = value as Record<string, unknown>;
    if (message.role === "system") system.push(...textBlocks(message.content));
  }
  return system;
}
function requestSystemText(body: Record<string, unknown>): string {
  return requestSystemMessages(body).join("\n");
}
function requestPromptText(body: Record<string, unknown>): string {
  const developer = body.developer === undefined ? "" : textBlocks(body.developer).join("\n");
  return `${requestSystemText(body)}\n${developer}\n${requestMessageText(body)}`;
}
function requestContains(body: Record<string, unknown>, value: string): boolean {
  return requestMessageText(body).includes(value);
}
function findTool(body: Record<string, unknown>, name: string): Record<string, unknown> | undefined {
  if (!Array.isArray(body.tools)) return undefined;
  for (const value of body.tools) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
    const entry = value as Record<string, unknown>;
    if (entry.name === name) return entry;
    if (entry.function && typeof entry.function === "object" && !Array.isArray(entry.function)) {
      const fn = entry.function as Record<string, unknown>;
      if (fn.name === name) return { ...entry, ...fn };
    }
  }
  return undefined;
}
function toolNames(body: Record<string, unknown>): string[] {
  if (!Array.isArray(body.tools)) return [];
  return body.tools.flatMap(value => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
    const entry = value as Record<string, unknown>;
    if (typeof entry.name === "string") return [entry.name];
    if (entry.function && typeof entry.function === "object" && !Array.isArray(entry.function)) {
      const name = (entry.function as Record<string, unknown>).name;
      if (typeof name === "string") return [name];
    }
    return [];
  });
}
function historyHasTool(body: Record<string, unknown>, name: string): boolean {
  for (const value of requestMessages(body)) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
    const message = value as Record<string, unknown>;
    if (message.role === "tool" && message.name === name) return true;
    if (Array.isArray(message.tool_calls)) {
      for (const callValue of message.tool_calls) {
        if (callValue === null || typeof callValue !== "object" || Array.isArray(callValue)) continue;
        const call = callValue as Record<string, unknown>;
        const fn = call.function;
        if (fn && typeof fn === "object" && !Array.isArray(fn) && (fn as Record<string, unknown>).name === name) return true;
      }
    }
    if (Array.isArray(message.content)) {
      for (const blockValue of message.content) {
        if (blockValue && typeof blockValue === "object" && !Array.isArray(blockValue)) {
          const block = blockValue as Record<string, unknown>;
          if (block.type === "toolCall" && block.name === name) return true;
        }
      }
    }
  }
  return false;
}
function memberMessageIndices(body: Record<string, unknown>): number[] {
  return requestMessages(body).flatMap((message, index) =>
    providerMessageText(message).includes(MEMBER_CONTEXT_PREFIX) ? [index] : [],
  );
}
function memberSnapshotText(body: Record<string, unknown>, snapshotIndex: number): string {
  const index = memberMessageIndices(body)[snapshotIndex];
  if (index === undefined) throw new Error(`missing persisted member snapshot ${snapshotIndex}`);
  return providerMessageText(requestMessages(body)[index]);
}
function matchingProviderMessages(body: Record<string, unknown>, marker: string): Array<{ index: number; text: string }> {
  return requestMessages(body).flatMap((value, index) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
    const message = value as { role?: unknown };
    const text = providerMessageText(value);
    return text.includes(marker) && message.role !== "tool" ? [{ index, text }] : [];
  });
}
function onePersistentContextMessage(body: Record<string, unknown>, marker: string): { index: number; text: string } {
  const matches = matchingProviderMessages(body, marker);
  assert.equal(matches.length, 1, `Expected exactly one persistent context message containing ${marker}`);
  const match = matches[0]!;
  return { index: match.index, text: match.text };
}
type CapturedSystem = {
  readonly messages: string[];
  readonly guide: string;
  readonly guideIndex: number;
};
function captureSystem(body: Record<string, unknown>, target: Target): CapturedSystem {
  const messages = requestSystemMessages(body);
  const systemText = messages.join("\n");
  const expectedGuide = formatAgentGuidance(fixture.botUid, target, false);
  assert.ok(systemText.includes(expectedGuide), `Provider request omitted the fixed guide for unready Octo CLI identity ${target.channelId}`);
  assert.equal(systemText.split(AGENT_GUIDANCE_PREFIX).length - 1, 1, "Fixed guide was duplicated in the current OMP system");
  const guideStart = systemText.indexOf(AGENT_GUIDANCE_PREFIX);
  const guideIndex = messages.findIndex(message => message.includes(AGENT_GUIDANCE_PREFIX));
  assert.ok(guideIndex >= 0, "Fixed guide is not in an actual system message");
  return {
    messages,
    guide: systemText.slice(guideStart, guideStart + expectedGuide.length),
    guideIndex,
  };
}
function systemHash(system: CapturedSystem): string {
  return createHash("sha256").update(JSON.stringify(system.messages)).digest("hex");
}
function assertMessagePrefix(previous: Record<string, unknown>, next: Record<string, unknown>): number {
  const prefix = requestMessages(previous);
  const actual = requestMessages(next);
  assert.ok(actual.length >= prefix.length, "Tool continuation discarded prior provider messages");
  assert.deepEqual(actual.slice(0, prefix.length), prefix, "Tool continuation moved or rewrote existing provider messages");
  return prefix.length;
}
function stepsFor(...next: Step[]): void {
  steps.splice(0, steps.length, ...next);
}
function textStep(text: string, match?: Step["match"], hold?: Gate): Step {
  return { text, ...(match === undefined ? {} : { match }), ...(hold === undefined ? {} : { gate: hold }) };
}
function toolStep(name: string, args: Step["tool"] extends infer T ? T extends { args: infer A } ? A : never : never, id: string, match?: Step["match"]): Step {
  return { tool: { name, args, id }, ...(match === undefined ? {} : { match }) };
}
function textReply(step: Step, index: number): Response {
  const id = `context-${index}`;
  const toolCall = step.tool === undefined ? undefined : {
    role: "assistant",
    tool_calls: [{
      index: 0,
      id: step.tool.id,
      type: "function",
      function: {
        name: step.tool.name,
        arguments: JSON.stringify(typeof step.tool.args === "function" ? step.tool.args(requests.at(-1)!.body) : step.tool.args),
      },
    }],
  };
  const delta = toolCall ?? { role: "assistant", content: step.text ?? "" };
  const chunks = [
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: toolCall ? "tool_calls" : "stop" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}
function recordModelError(message: string): Response {
  modelError ??= message;
  return new Response(message, { status: 400 });
}
async function wait(description: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined, `Finite loopback model failed: ${modelError}`);
    return predicate() ? true : undefined;
  }, timeoutMs, description);
}
async function exited(process: Bun.Subprocess, timeoutMs = 15_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = Promise.withResolvers<never>();
  try {
    timer = setTimeout(() => timeout.reject(new Error("Context acceptance CLI did not exit before its deadline")), timeoutMs);
    return await Promise.race([process.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}
async function waitTask(messageId: string, reply: string): Promise<void> {
  await wait(`Task ${messageId} did not complete with ${reply}`, () => {
    const row = task(messageId);
    if (row && ["failed", "cancelled", "interrupted"].includes(row.state)) {
      throw new Error(`Task ${messageId} ended ${row.state}: ${row.error_text ?? "no detail"}`);
    }
    return row?.state === "completed" && fixture.sentMessages.some(message => messageText(message).includes(reply));
  });
}
async function waitForWorkerCleanup(): Promise<void> {
  await wait("A settled task retained a live OMP worker", () => {
    const row = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return row.count === 0;
  });
}
async function launch(): Promise<Bun.Subprocess> {
  const launched = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: root,
      PATH: `${binPath}:/usr/bin:/bin`,
      PI_CODING_AGENT_DIR: agentPath,
      PI_NO_TITLE: "1",
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(launched);
  outputs.push(Promise.all([new Response(launched.stdout).text(), new Response(launched.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(children.length, 15_000);
  await waitForFixture(() => connection?.ready ? true : undefined, 15_000, "Channel CLI did not authenticate to the local Octo fixture");
  child = launched;
  return launched;
}
function taskToolArgs(body: Record<string, unknown>): Record<string, unknown> {
  const tool = findTool(body, "task");
  assert.ok(tool, "Parent provider request did not advertise the real OMP task tool");
  const parameters = tool.parameters;
  assert.ok(parameters && typeof parameters === "object" && !Array.isArray(parameters), "OMP task tool omitted its live input schema");
  const properties = (parameters as Record<string, unknown>).properties;
  assert.ok(properties && typeof properties === "object" && !Array.isArray(properties), "OMP task schema omitted properties");
  const record = properties as Record<string, unknown>;
  const agentItem: Record<string, unknown> = {
    task: "TASK_CHILD_UNIQUE: return the requested short result.",
    solutionSpace: "one result: the requested short answer is given",
  };
  if (record.agent !== undefined) agentItem.agent = "task";
  if (record.tasks !== undefined) {
    return {
      context: "A standalone task with no caller-specific Octo context.",
      tasks: [agentItem],
    };
  }
  return agentItem;
}
function sourceMarkerPrompt(body: Record<string, unknown>, marker: string): boolean {
  return requestContains(body, marker);
}
async function threadList(): Promise<Array<{ short_id: string; channel_id: string; name: string }>> {
  const response = await fetch(`${fixture.apiUrl}/v1/bot/groups/${encodeURIComponent(groupId)}/threads`, {
    headers: { authorization: `Bearer ${fixture.botToken}` },
  });
  assert.equal(response.status, 200, "Could not inspect the local fixture's created thread");
  const body: unknown = await response.json();
  assert.ok(Array.isArray(body), "Local fixture thread list was not an array");
  return body as Array<{ short_id: string; channel_id: string; name: string }>;
}

try {
  for (const directory of ["bin", "agent", "cwd"]) await mkdir(join(root, directory));
  await writeFile(wirePath, "");
  await writeFile(join(cwdPath, "context-tool-fixture.txt"), "POST_COMPACT_TOOL_OUTPUT_V1\n");
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(binPath, command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }

  const initialProvider = {
    baseUrl: "",
    apiKey: "local-context-fixture-only",
    api: "openai-completions",
    models: [{ id: "probe", name: "Context Acceptance Probe", reasoning: false, input: ["text"], contextWindow: 32_768, maxTokens: 2_048, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  };
  modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        return recordModelError("Loopback model received a non-JSON provider request");
      }
      if (requests.length >= 64) return recordModelError("Finite context model exceeded its 64-request budget");
      const url = new URL(request.url);
      if (url.hostname !== "127.0.0.1") return recordModelError(`Provider request escaped loopback: ${url.hostname}`);
      requests.push({ url: request.url, body });
      const index = steps.findIndex(step => step.match?.(body) ?? true);
      let step = index < 0 ? undefined : steps.splice(index, 1)[0];
      if (!step && compactionReplyBudget > 0) {
        compactionReplyBudget -= 1;
        step = { text: "COMPACTED_CONTEXT_SUMMARY" };
      }
      if (!step) return recordModelError(`Finite provider script had no reply for request ${requests.length}`);
      if (step.tool && !toolNames(body).includes(step.tool.name)) {
        return recordModelError(`Provider request did not advertise the expected ${step.tool.name} tool`);
      }
      if (step.gate) await step.gate.promise;
      return textReply(step, requests.length);
    },
  });
  assert.ok(modelServer.port, "Loopback model did not bind a TCP port");
  initialProvider.baseUrl = `http://127.0.0.1:${modelServer.port}/v1`;
  await writeFile(join(agentPath, "models.yml"), JSON.stringify({ providers: { "local-context": initialProvider } }));
  const compactExtensionPath = join(root, "context-acceptance-compact.ts");
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\nasync:\n  enabled: false\ncompaction:\n  enabled: true\n  keepRecentTokens: 1\n");
  await writeFile(compactExtensionPath, `export default function (api) {
  api.registerCommand("context-acceptance-compact", {
    description: "Run a native compaction in the isolated context acceptance profile.",
    handler: async (_args, ctx) => {
      await ctx.compact();
    },
  });
}
`);
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [groupId],
    stateDir: join(root, "state"),
    sessionsDir: join(root, "sessions"),
    ompCommand: join(root, "real-omp.ts"),
    ompExtraArgs: [
      "--extension", compactExtensionPath,
      "--model", "local-context/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
      "--config", join(root, "native.yml"),
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });
  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, `#!${process.execPath}
import { appendFileSync } from 'node:fs';
const child = Bun.spawn([${JSON.stringify(realOmp)}, ...process.argv.slice(2)], { stdin: 'pipe', stdout: 'inherit', stderr: 'inherit', env: process.env });
let pending = '';
const decoder = new TextDecoder();
for await (const bytes of Bun.stdin.stream()) {
  pending += decoder.decode(bytes, { stream: true });
  let end;
  while ((end = pending.indexOf('\\n')) >= 0) {
    const line = pending.slice(0, end);
    pending = pending.slice(end + 1);
    if (!line.trim()) continue;
    const frame = JSON.parse(line);
    appendFileSync(${JSON.stringify(wirePath)}, JSON.stringify({ type: frame.type, message: frame.message, op: frame.op }) + '\\n');
    child.stdin.write(line + '\\n');
  }
}
pending += decoder.decode();
if (pending.trim()) {
  const frame = JSON.parse(pending);
  appendFileSync(${JSON.stringify(wirePath)}, JSON.stringify({ type: frame.type, message: frame.message, op: frame.op }) + '\\n');
  child.stdin.write(pending + '\\n');
}
child.stdin.end();
process.exit(await child.exited);
`, { mode: 0o755 });

  child = await launch();
  db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
  const startSessionAt = fixture.sentMessages.length;
  const createSessionMessage = send(sourceTarget, `/new ${cwdPath}`);
  await wait("Group /new did not establish its real source session", () =>
    binding(sourceTarget)?.cwd === cwdPath
      && messageContains(sourceTarget, `cwd=${cwdPath}`, startSessionAt),
  );
  assert.equal(task(createSessionMessage), null, "/new unexpectedly created an agent task");

  // A native goal is accepted and resumed without the host `prompt` RPC. Its first model
  // request must still receive and commit the fresh target explanation.
  const nativeGoalRequestAt = requests.length;
  const nativeGoalWireAt = (await readFile(wirePath, "utf8")).split("\n").filter(Boolean).length;
  stepsFor(
    toolStep("goal", { op: "get" }, "context-native-goal-get"),
    textStep("NATIVE_GOAL_FIRST_STEP"),
    toolStep("goal", { op: "complete" }, "context-native-goal-complete"),
    textStep("NATIVE_GOAL_ACCEPTED"),
  );
  const nativeGoalMessage = send(sourceTarget, "/goal set --budget 2000 CONTEXT_NATIVE_GOAL_FIRST_CONTEXT");
  await waitTask(nativeGoalMessage, "NATIVE_GOAL_ACCEPTED");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Native goal did not consume its finite model script");
  assert.ok(contextReadCount(sourceTarget) > 0, "Native goal skipped the first fresh description read");
  const nativeGoalRequest = requests[nativeGoalRequestAt]!.body;
  const nativeGoalContinuation = requests[nativeGoalRequestAt + 1]!.body;
  const nativeGoalSystem = captureSystem(nativeGoalRequest, sourceTarget);
  assert.deepEqual(captureSystem(nativeGoalContinuation, sourceTarget), nativeGoalSystem, "Native goal tool continuation changed the full captured system");
  const nativeGoalPrefixCount = assertMessagePrefix(nativeGoalRequest, nativeGoalContinuation);
  assert.ok(requestMessageText(nativeGoalRequest).includes("SOURCE_GROUP_DESCRIPTION_V1"), "Native goal omitted its first shared explanation");
  assert.ok(requestMessageText(nativeGoalRequest).includes("SOURCE_MEMORY_V1"), "Native goal omitted its first Memory section");
  const goalDescriptionMessageIndex = requestMessages(nativeGoalRequest)
    .findIndex(message => providerMessageText(message).includes("SOURCE_GROUP_DESCRIPTION_V1"));
  assert.ok(goalDescriptionMessageIndex >= 0, "Native goal explanation was not in persistent provider messages");
  const nativeGoalSession = binding(sourceTarget);
  assert.ok(nativeGoalSession, "Native goal did not leave a bound chat session");
  const goalContextReceipt = db!.query(
    "SELECT version, phase FROM session_context_ledger WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND session_id = ?",
  ).get(fixture.botUid, sourceTarget.channelType, sourceTarget.channelId, nativeGoalSession.session_id) as { version: number; phase: string } | null;
  assert.equal(goalContextReceipt?.version, sourceContext.version, "Native goal committed the wrong explanation version");
  assert.equal(goalContextReceipt?.phase, "committed", "Native goal explanation bypassed the context receipt");
  const goalWire = (await readFile(wirePath, "utf8")).split("\n").filter(Boolean)
    .map(line => JSON.parse(line) as { type?: string; op?: string })
    .slice(nativeGoalWireAt);
  assert.ok(goalWire.some(frame => frame.type === "goal" && frame.op === "create"), "Native goal did not use OMP's goal RPC");
  assert.ok(!goalWire.some(frame => frame.type === "prompt"), "Native goal unexpectedly used a host prompt RPC");
  console.log("CASE native-goal-first-context " + JSON.stringify({
    fixedGuide: true,
    descriptionPersisted: true,
    contextReceipt: "committed",
    hostPromptRpc: false,
    systemSha256: systemHash(nativeGoalSystem),
    systemMessageCount: nativeGoalSystem.messages.length,
    guideSystemIndex: nativeGoalSystem.guideIndex,
    existingMessagePrefixCount: nativeGoalPrefixCount,
  }));

  // The first ordinary turn after native goal verifies that the goal's description stayed in the
  // same history position rather than being dropped or transiently re-injected.
  const firstGoalDescriptionPosition = goalDescriptionMessageIndex;
  const firstGoalSystem = nativeGoalSystem;

  // The first model request demonstrates actual system-prefix delivery and first-session context.
  stepsFor(
    toolStep("octo_management", { action: "description-read" }, "initial-description-read", body => sourceMarkerPrompt(body, "INITIAL_GROUP_USER") && !historyHasTool(body, "octo_management")),
    textStep("INITIAL_GROUP_TURN_DONE", body => sourceMarkerPrompt(body, "INITIAL_GROUP_USER") && historyHasTool(body, "octo_management")),
  );
  const firstRequestAt = requests.length;
  const firstTask = send(sourceTarget, "INITIAL_GROUP_USER");
  await waitTask(firstTask, "INITIAL_GROUP_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Initial group model script did not consume its finite steps");
  const initialRequest = requests[firstRequestAt]!.body;
  const initialContinuation = requests[firstRequestAt + 1]!.body;
  const sourceSystem = captureSystem(initialRequest, sourceTarget);
  assert.deepEqual(captureSystem(initialContinuation, sourceTarget), sourceSystem, "Tool continuation changed the full captured system");
  assert.equal(sourceSystem.guide, firstGoalSystem.guide, "A normal worker changed the fixed guide after native goal acceptance");
  const initialDescriptionPosition = requestMessages(initialRequest)
    .findIndex(message => providerMessageText(message).includes("SOURCE_GROUP_DESCRIPTION_V1"));
  assert.equal(initialDescriptionPosition, firstGoalDescriptionPosition, "First explanation moved after native goal acceptance");
  const initialPrefixCount = assertMessagePrefix(initialRequest, initialContinuation);
  assert.ok(requestMessageText(initialRequest).includes("SOURCE_GROUP_DESCRIPTION_V1"), "First session omitted its shared description snapshot");
  assert.ok(requestMessageText(initialRequest).includes("SOURCE_MEMORY_V1"), "First session omitted its Memory snapshot");
  const initialMemberIndices = memberMessageIndices(initialRequest);
  assert.equal(initialMemberIndices.length, 1, "First user input did not persist exactly one member snapshot");
  const initialMemberText = memberSnapshotText(initialRequest, 0);
  assert.ok(initialMemberText.indexOf("member-alpha-uid") < initialMemberText.indexOf("member-beta-uid"), "Initial member order did not match the actual API response");
  const firstContextReadCount = contextReadCount(sourceTarget);
  console.log("CASE fixed-guide-tool-continuation " + JSON.stringify({
    guidePrefix: AGENT_GUIDANCE_PREFIX,
    sameSystem: true,
    providerRequests: 2,
    systemSha256: systemHash(sourceSystem),
    systemMessageCount: sourceSystem.messages.length,
    guideSystemIndex: sourceSystem.guideIndex,
    existingMessagePrefixCount: initialPrefixCount,
  }));

  // Reordering the API roster is not a semantic change; the stored message stays at its original
  // position. A changed roster appends a new snapshot instead of rewriting the first one.
  sourceContext.content = "### Memory Start ###\nSOURCE_MEMORY_V2_PENDING_COMPACT\n### Memory End ###\nSOURCE_GROUP_DESCRIPTION_V2_PENDING_COMPACT";
  sourceContext.version = 2;
  managementBodies[`GET ${fixtureGroupMembersPath}`] = [member("member-beta-uid", "Beta"), member("member-alpha-uid", "Alpha")];
  stepsFor(textStep("REORDERED_ROSTER_TURN_DONE", body => sourceMarkerPrompt(body, "REORDERED_ROSTER_USER")));
  const reorderRequestAt = requests.length;
  const reorderTask = send(sourceTarget, "REORDERED_ROSTER_USER");
  await waitTask(reorderTask, "REORDERED_ROSTER_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0);
  const reorderedRequest = requests[reorderRequestAt]!.body;
  assert.deepEqual(captureSystem(reorderedRequest, sourceTarget), sourceSystem);
  assert.equal(contextReadCount(sourceTarget), firstContextReadCount, "An ordinary turn refreshed the changed description");
  assert.ok(!requestMessageText(reorderedRequest).includes("SOURCE_GROUP_DESCRIPTION_V2_PENDING_COMPACT"), "An ordinary turn exposed a changed description before a fresh session or compaction");
  assert.deepEqual(memberMessageIndices(reorderedRequest), initialMemberIndices, "API reorder moved or reinserted the member snapshot");
  assert.equal(memberSnapshotText(reorderedRequest, 0), initialMemberText, "API reorder rewrote the historical member message");
  const reorderPrefixCount = assertMessagePrefix(initialContinuation, reorderedRequest);
  console.log("CASE member-reorder-stability " + JSON.stringify({
    systemSha256: systemHash(sourceSystem),
    systemMessageCount: sourceSystem.messages.length,
    guideSystemIndex: sourceSystem.guideIndex,
    existingMessagePrefixCount: reorderPrefixCount,
    memberSnapshotCount: 1,
  }));

  managementBodies[`GET ${fixtureGroupMembersPath}`] = [member("member-beta-uid", "Beta"), member("member-gamma-uid", "Gamma")];
  stepsFor(textStep("CHANGED_ROSTER_TURN_DONE", body => sourceMarkerPrompt(body, "CHANGED_ROSTER_USER")));
  const changedRequestAt = requests.length;
  const changedTask = send(sourceTarget, "CHANGED_ROSTER_USER");
  await waitTask(changedTask, "CHANGED_ROSTER_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0);
  const changedRequest = requests[changedRequestAt]!.body;
  assert.deepEqual(captureSystem(changedRequest, sourceTarget), sourceSystem);
  assert.deepEqual(memberMessageIndices(changedRequest).length, 2, "Semantic member change did not append exactly one persistent snapshot");
  assert.equal(memberSnapshotText(changedRequest, 0), initialMemberText, "Member change moved or replaced historical membership");
  assert.ok(memberSnapshotText(changedRequest, 1).includes("member-gamma-uid"), "Changed roster was not added to the current user history");
  assert.ok(memberMessageIndices(changedRequest)[1]! > memberMessageIndices(changedRequest)[0]!, "Changed roster was not appended after the original snapshot");
  assert.equal(contextReadCount(sourceTarget), firstContextReadCount, "An ordinary roster change refreshed the description");

  // Steering changes only become visible when OMP consumes the queued input. The member API is
  // changed while the first provider request is held, then the next real request must contain both.
  const steeringGate = gate();
  const steeringStart = requests.length;
  const steeringReceiptStart = fixture.sentMessages.length;
  stepsFor(
    textStep("STEERING_PRIMARY_TURN_DONE", body => sourceMarkerPrompt(body, "STEERING_PRIMARY_USER"), steeringGate),
    textStep("STEERING_CONSUMED_TURN_DONE", body => sourceMarkerPrompt(body, "STEER_RECEIPT_V1")),
  );
  const steeringTask = send(sourceTarget, "STEERING_PRIMARY_USER");
  await wait("Primary steering turn never reached the real model", () => requests.some(request => sourceMarkerPrompt(request.body, "STEERING_PRIMARY_USER")));
  const steeringPrimaryRequest = requests.find(request => sourceMarkerPrompt(request.body, "STEERING_PRIMARY_USER"));
  assert.ok(steeringPrimaryRequest, "OMP never sent the primary steering request to the model");
  assert.deepEqual(captureSystem(steeringPrimaryRequest.body, sourceTarget), sourceSystem);
  assert.equal(memberMessageIndices(steeringPrimaryRequest.body).length, 2, "Primary request unexpectedly saw the not-yet-consumed steering snapshot");
  assert.ok(!requestMessageText(steeringPrimaryRequest.body).includes("member-steering-uid"), "Steering roster changed before its input was consumed");
  managementBodies[`GET ${fixtureGroupMembersPath}`] = [member("member-steering-uid", "Steering Member")];
  const memberReadsBeforeSteering = memberReadCount(sourceTarget);
  const steeringMessage = send(sourceTarget, "STEER_RECEIPT_V1");
  await wait("Accepted steering did not refresh the target member roster", () => memberReadCount(sourceTarget) > memberReadsBeforeSteering);
  await wait("Steering was not acknowledged while the primary provider request remained held", () => {
    assert.equal(task(steeringMessage), null, "Steering fell back to a separate task before the primary request was released");
    const accepted = inbound(steeringMessage);
    return accepted?.kind === "command" && accepted.command_state === "completed";
  });
  steeringGate.resolve();
  await waitTask(steeringTask, "STEERING_CONSUMED_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(task(steeringMessage), null, "Steering was dispatched as a separate task");
  assert.equal(steps.length, 0, "Steering model script did not consume its finite steps");
  const steeringRequest = requests.find(request => sourceMarkerPrompt(request.body, "STEER_RECEIPT_V1"));
  assert.ok(steeringRequest, "OMP never sent accepted steering to the model");
  assert.deepEqual(captureSystem(steeringRequest.body, sourceTarget), sourceSystem);
  assert.ok(requestMessageText(steeringRequest.body).includes("member-steering-uid"), "Consumed steering omitted its fresh member snapshot");
  assert.equal(memberMessageIndices(steeringRequest.body).length, 3, "Steering did not append one persistent roster after the prior snapshots");
  assert.equal(contextReadCount(sourceTarget), firstContextReadCount, "Steering refreshed an unchanged shared description");
  const steeringReceipt = fixture.sentMessages.slice(steeringReceiptStart)
    .map(messageText)
    .find(text => text.includes("已并入当前回合"));
  assert.ok(steeringReceipt, "Consumed steering did not produce its user-visible receipt");
  assert.ok(steeringReceipt.includes("STEER_RECEIPT_V1"), "Steering receipt did not preserve the original user text");
  assert.ok(!steeringReceipt.includes("member-steering-uid"), "Steering receipt exposed the internal appended member snapshot");
  console.log("CASE member-persistence-steering " + JSON.stringify({
    reorderedSnapshotCount: 1,
    changedSnapshotCount: 2,
    steeringSnapshotCount: 3,
    providerRequests: requests.length - steeringStart,
    originalReceipt: true,
  }));

  // Crash a real in-flight turn and let the service restart the exact OMP session. The retry must
  // be native, keep the CLI-appended guide, and not fetch or append a second member snapshot.
  const recoveryGate = gate();
  const recoveryRequestAt = requests.length;
  stepsFor(textStep("RECOVERY_ORIGINAL_REPLY_MUST_NOT_FINISH", body => sourceMarkerPrompt(body, "RECOVERY_PARENT_UNIQUE"), recoveryGate));
  const recoveryTask = send(sourceTarget, "RECOVERY_PARENT_UNIQUE");
  await wait("Recovery checkpoint did not reach the loopback model", () => requests.length > recoveryRequestAt && task(recoveryTask)?.state === "started");
  const recoveryStartBinding = binding(sourceTarget)!;
  assert.ok(recoveryStartBinding.session_file, "Recovery turn had no persisted native OMP session");
  const recoveryMemberReads = memberReadCount(sourceTarget);
  const recoverySnapshotCount = memberMessageIndices(requests[recoveryRequestAt]!.body).length;
  const recoverySystem = captureSystem(requests[recoveryRequestAt]!.body, sourceTarget);
  child!.kill("SIGKILL");
  await exited(child!);
  recoveryGate.resolve();
  stepsFor(textStep("RECOVERY_NATIVE_RETRY_DONE", body => sourceMarkerPrompt(body, "RECOVERY_PARENT_UNIQUE")));
  child = await launch();
  await waitTask(recoveryTask, "RECOVERY_NATIVE_RETRY_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Restart recovery did not consume its finite model reply");
  const retryRequest = requests.slice(recoveryRequestAt + 1).find(request => sourceMarkerPrompt(request.body, "RECOVERY_PARENT_UNIQUE"));
  assert.ok(retryRequest, "Restart did not create a provider request for native recovery");
  assert.deepEqual(captureSystem(retryRequest.body, sourceTarget), recoverySystem, "Restarted native retry lost or changed the full system");
  const recoveryPrefixCount = assertMessagePrefix(requests[recoveryRequestAt]!.body, retryRequest.body);
  assert.equal(memberMessageIndices(retryRequest.body).length, recoverySnapshotCount, "Native retry replayed or moved the persisted member input");
  assert.equal(memberReadCount(sourceTarget), recoveryMemberReads, "Native retry fetched a new member roster");
  assert.equal(binding(sourceTarget)?.session_id, recoveryStartBinding.session_id, "Recovery replaced the bound OMP session");
  const nativeWire = (await readFile(wirePath, "utf8")).split("\n").filter(Boolean).map(line => JSON.parse(line) as { type?: string; message?: string });
  assert.ok(nativeWire.some(frame => frame.type === "prompt" && frame.message === "/retry"), "Service recovery did not dispatch OMP's native /retry");
  console.log("CASE native-retry-recovery " + JSON.stringify({
    sameSession: true,
    fixedGuide: true,
    memberFetchesDuringRetry: 0,
    nativeRetry: true,
    systemSha256: systemHash(recoverySystem),
    systemMessageCount: recoverySystem.messages.length,
    guideSystemIndex: recoverySystem.guideIndex,
    existingMessagePrefixCount: recoveryPrefixCount,
  }));

  // Exercise OMP's public native compaction API through an isolated extension command. The command
  // is registered only in this acceptance profile; it is not a production slash-command feature.
  sourceContext.content = "### Memory Start ###\nSOURCE_MEMORY_V3_COMPACT_FRESH\n### Memory End ###\nSOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH";
  sourceContext.version = 3;
  const compactedSnapshot = { ...sourceContext };
  const compactReadBaseline = contextReadCount(sourceTarget);
  const compactRequestAt = requests.length;
  compactionReplyBudget = 3;
  const compactMessage = send(sourceTarget, "/context-acceptance-compact");
  await wait("Native ctx.compact() did not emit a fresh summary request", () =>
    requests.length > compactRequestAt
      && contextReadCount(sourceTarget) > compactReadBaseline
      && inbound(compactMessage)?.kind === "task"
      && task(compactMessage)?.state === "completed",
  );
  compactionReplyBudget = 0;
  const compactProviderRequests = requests.slice(compactRequestAt);
  assert.equal(compactProviderRequests.length, 3, "Installed OMP compaction did not consume its conversation, split-turn, and short-summary requests");
  const compactRequestEvidence = compactProviderRequests.map((request, index) => {
    const messageText = requestMessageText(request.body);
    return {
      requestNumber: compactRequestAt + index + 1,
      roles: requestMessages(request.body).map(value => {
        if (value === null || typeof value !== "object" || Array.isArray(value)) return "<invalid>";
        const role = (value as Record<string, unknown>).role;
        return typeof role === "string" ? role : "<missing>";
      }),
      messageSha256: createHash("sha256").update(JSON.stringify(requestMessages(request.body))).digest("hex"),
      messagePrefix: messageText.slice(0, 800),
      includesFullV3Snapshot: messageText.includes(compactedSnapshot.content),
      systemSha256: createHash("sha256").update(JSON.stringify(requestSystemMessages(request.body))).digest("hex"),
    };
  });
  console.log("CASE compact-provider-request-flow " + JSON.stringify(compactRequestEvidence));
  const compactWire = (await readFile(wirePath, "utf8")).split("\n").filter(Boolean)
    .map(line => JSON.parse(line) as { type?: string; message?: string });
  assert.ok(compactWire.some(frame => frame.type === "prompt" && frame.message === "/context-acceptance-compact"),
    "Acceptance did not dispatch the isolated OMP native compaction command");
  const compactReadCount = contextReadCount(sourceTarget);
  const compactSummaryRequest = requests[compactRequestAt]!.body;
  const compactSummarySystem = requestSystemMessages(compactSummaryRequest);
  assert.ok(
    requestMessageText(compactSummaryRequest).includes(compactedSnapshot.content),
    "Actual compaction summary request omitted the complete fresh Memory and group description snapshot",
  );
  assert.ok(
    !requestSystemText(compactSummaryRequest).includes("SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH"),
    "Compaction context was injected into the dynamic provider system",
  );
  const compactBinding = binding(sourceTarget)!;
  assert.ok(compactBinding.session_file, "Compaction did not persist its native OMP session file");
  assert.equal(compactBinding.session_id, recoveryStartBinding.session_id, "Native compact replaced the current source session");
  const sessionEntries = (await readFile(compactBinding.session_file, "utf8"))
    .split(/\r?\n/)
    .filter(line => line.length > 0)
    .map(line => JSON.parse(line) as OmpSessionEntry);
  const compactionEntries = sessionEntries.filter(entry => entry.type === "compaction");
  assert.ok(compactionEntries.length >= 1, "Native OMP session file has no committed compaction entry");
  const compactionContext = compactionEntries.at(-1)!.preserveData?.ompChannelOctoContext;
  assert.ok(compactionContext, "Committed compaction entry omitted ompChannelOctoContext preserveData");
  assert.equal(compactionContext.schemaVersion, 1);
  assert.equal(compactionContext.botUid, fixture.botUid);
  assert.deepEqual(compactionContext.target, sourceTarget);
  assert.equal(compactionContext.sessionId, compactBinding.session_id);
  for (const [field, value] of Object.entries({
    requestId: compactionContext.requestId,
    turnId: compactionContext.turnId,
    attemptId: compactionContext.attemptId,
  })) {
    assert.ok(value.length > 0, `Compaction preserveData omitted ${field}`);
  }
  assert.deepEqual(compactionContext.snapshot, {
    content: compactedSnapshot.content,
    present: true,
    version: compactedSnapshot.version,
    updatedAt: compactedSnapshot.updatedAt,
    updatedBy: compactedSnapshot.updatedBy,
  }, "Compaction preserveData did not retain the complete fresh shared-context snapshot");
  console.log("CASE compact-summary-preserve-data " + JSON.stringify({
    summaryRequestSystemSha256: createHash("sha256").update(JSON.stringify(compactSummarySystem)).digest("hex"),
    summarySystemMessageCount: compactSummarySystem.length,
    compactionEntryCount: compactionEntries.length,
    preservedSnapshotVersion: compactionContext.snapshot.version,
    fullFreshSnapshotPreserved: true,
  }));
  sourceContext.content = "### Memory Start ###\nLIVE_REREAD_MEMORY_V4\n### Memory End ###\nLIVE_REREAD_DESCRIPTION_V4";
  sourceContext.version = 4;
  stepsFor(
    toolStep("octo_management", { action: "description-read" }, "post-compact-description-read", body => sourceMarkerPrompt(body, "DESCRIPTION_REREAD_AFTER_COMPACT") && !historyHasTool(body, "octo_management")),
    textStep("DESCRIPTION_REREAD_DONE", body => sourceMarkerPrompt(body, "DESCRIPTION_REREAD_AFTER_COMPACT") && historyHasTool(body, "octo_management")),
  );
  const rereadRequestAt = requests.length;
  const rereadTask = send(sourceTarget, "DESCRIPTION_REREAD_AFTER_COMPACT");
  await waitTask(rereadTask, "DESCRIPTION_REREAD_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Description reread did not consume its finite model steps");
  const rereadRequest = requests[rereadRequestAt]!.body;
  const rereadContinuation = requests[rereadRequestAt + 1]!.body;
  const rereadSystem = captureSystem(rereadRequest, sourceTarget);
  assert.deepEqual(captureSystem(rereadContinuation, sourceTarget), rereadSystem);
  assert.equal(rereadSystem.guide, sourceSystem.guide);
  const rereadPrefixCount = assertMessagePrefix(rereadRequest, rereadContinuation);
  assert.ok(
    requestMessageText(rereadRequest).includes(compactedSnapshot.content),
    "First post-compact provider request omitted the full snapshot before description-read could run",
  );
  assert.ok(!requestSystemText(rereadRequest).includes("SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH"), "Fresh context was placed in the dynamic provider system");
  const persistedFreshContextMessage = onePersistentContextMessage(rereadRequest, "SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH");
  assert.deepEqual(
    onePersistentContextMessage(rereadContinuation, "SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH"),
    persistedFreshContextMessage,
    "Tool continuation moved or rewrote the persisted fresh context message",
  );
  const liveDescriptionResult = requestMessages(rereadContinuation).find(value =>
    value !== null && typeof value === "object" && !Array.isArray(value)
    && (value as Record<string, unknown>).role === "tool"
    && (value as Record<string, unknown>).tool_call_id === "post-compact-description-read",
  ) as { content: string } | undefined;
  assert.ok(liveDescriptionResult, "Provider continuation omitted the live description-read result");
  assert.deepEqual(JSON.parse(liveDescriptionResult.content), {
    present: true,
    content: sourceContext.content,
    version: 4,
    updatedAt: sourceContext.updatedAt,
    updatedBy: sourceContext.updatedBy,
  }, "description-read omitted the externally updated live V4 snapshot");
  assert.ok(!requestMessageText(rereadRequest).includes("LIVE_REREAD_DESCRIPTION_V4"), "Ordinary post-compact input reloaded the remote V4 description before the tool ran");
  assert.ok(contextReadCount(sourceTarget) > compactReadCount, "description-read did not fetch the current shared description again");
  console.log("CASE compact-fresh-reread " + JSON.stringify({
    compactFreshRead: true,
    subsequentDescriptionRead: true,
    systemSha256: systemHash(rereadSystem),
    systemMessageCount: rereadSystem.messages.length,
    guideSystemIndex: rereadSystem.guideIndex,
    existingMessagePrefixCount: rereadPrefixCount,
    persistedContextMessageIndex: persistedFreshContextMessage.index,
    persistedContextUtf8Bytes: Buffer.byteLength(persistedFreshContextMessage.text),
  }));
  const ordinaryContextReadBaseline = contextReadCount(sourceTarget);
  const ordinaryToolRequestAt = requests.length;
  stepsFor(
    toolStep("read", { path: join(cwdPath, "context-tool-fixture.txt") }, "post-compact-stability-read",
      body => sourceMarkerPrompt(body, "POST_COMPACT_STABILITY_USER") && !historyHasTool(body, "read")),
    textStep("POST_COMPACT_STABILITY_DONE",
      body => sourceMarkerPrompt(body, "POST_COMPACT_STABILITY_USER") && historyHasTool(body, "read")),
  );
  const ordinaryToolTask = send(sourceTarget, "POST_COMPACT_STABILITY_USER");
  await waitTask(ordinaryToolTask, "POST_COMPACT_STABILITY_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Post-compact ordinary tool turn did not consume its finite model steps");
  const ordinaryToolRequest = requests[ordinaryToolRequestAt]!.body;
  const ordinaryToolContinuation = requests[ordinaryToolRequestAt + 1]!.body;
  assert.deepEqual(captureSystem(ordinaryToolRequest, sourceTarget), rereadSystem);
  assert.deepEqual(captureSystem(ordinaryToolContinuation, sourceTarget), rereadSystem);
  assertMessagePrefix(rereadContinuation, ordinaryToolRequest);
  const ordinaryToolPrefixCount = assertMessagePrefix(ordinaryToolRequest, ordinaryToolContinuation);
  assert.deepEqual(onePersistentContextMessage(ordinaryToolRequest, "SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH"), persistedFreshContextMessage);
  assert.deepEqual(onePersistentContextMessage(ordinaryToolContinuation, "SOURCE_GROUP_DESCRIPTION_V3_COMPACT_FRESH"), persistedFreshContextMessage);
  assert.ok(requestMessageText(ordinaryToolContinuation).includes("POST_COMPACT_TOOL_OUTPUT_V1"), "Actual read tool output was not present in its provider continuation");
  assert.equal(contextReadCount(sourceTarget), ordinaryContextReadBaseline, "An ordinary post-compact turn refreshed the shared description");
  console.log("CASE post-compact-ordinary-tool-continuation " + JSON.stringify({
    systemSha256: systemHash(rereadSystem),
    systemMessageCount: rereadSystem.messages.length,
    guideSystemIndex: rereadSystem.guideIndex,
    existingMessagePrefixCount: ordinaryToolPrefixCount,
    persistedContextMessageIndex: persistedFreshContextMessage.index,
    persistedContextUtf8Bytes: Buffer.byteLength(persistedFreshContextMessage.text),
    ordinaryTurnRefreshed: false,
    actualToolOutputCaptured: true,
  }));

  // /new is a fresh source session. Its first provider request gets only the current description and
  // membership. Later, a native fork is allowed to retain source business history, but its current
  // fixed guide, context, and member snapshot must belong to the new thread target.
  sourceContext.content = "### Memory Start ###\nNEW_SESSION_MEMORY_V4\n### Memory End ###\nNEW_SESSION_GROUP_DESCRIPTION_V4";
  sourceContext.version = 5;
  managementBodies[`GET ${fixtureGroupMembersPath}`] = [member("new-session-member-uid", "New Session Member")];
  const oldBinding = binding(sourceTarget)!;
  const newSessionMessage = send(sourceTarget, `/new ${cwdPath}`);
  await wait("Group /new did not create a distinct native session", () => {
    const current = binding(sourceTarget);
    return current !== null && current.session_id !== oldBinding.session_id && current.cwd === cwdPath
      && messageContains(sourceTarget, `cwd=${cwdPath}`);
  });
  assert.equal(task(newSessionMessage), null, "/new dispatched an agent task");
  stepsFor(textStep("NEW_SESSION_FIRST_TURN_DONE", body => sourceMarkerPrompt(body, "NEW_SESSION_FIRST_USER")));
  const newSessionRequestAt = requests.length;
  const newSessionTask = send(sourceTarget, "NEW_SESSION_FIRST_USER");
  await waitTask(newSessionTask, "NEW_SESSION_FIRST_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0);
  const newSessionRequest = requests[newSessionRequestAt]!.body;
  const newSessionSystem = captureSystem(newSessionRequest, sourceTarget);
  assert.ok(requestMessageText(newSessionRequest).includes("NEW_SESSION_GROUP_DESCRIPTION_V4"), "Fresh session did not read its current shared description");
  assert.ok(requestMessageText(newSessionRequest).includes("NEW_SESSION_MEMORY_V4"), "Fresh session did not read its current Memory section");
  assert.ok(requestMessageText(newSessionRequest).includes("new-session-member-uid"), "Fresh session omitted its current member snapshot");
  assert.ok(!requestMessageText(newSessionRequest).includes("member-alpha-uid"), "Fresh /new session inherited the prior session's member message");

  // Prove task and eval child sessions with actual provider requests. The positive control is the
  // parent's unique guide/description/member input; each child request must omit that implicit data.
  const parentTaskMarker = "TASK_PARENT_UNIQUE_WITH_SOURCE_CONTEXT";
  const taskChildMarker = "TASK_CHILD_UNIQUE";
  stepsFor(
    toolStep("task", (body: Record<string, unknown>) => taskToolArgs(body), "context-task-child", body => sourceMarkerPrompt(body, parentTaskMarker) && !historyHasTool(body, "task")),
    textStep("TASK_CHILD_FINISHED", body => requestContains(body, taskChildMarker) && !sourceMarkerPrompt(body, parentTaskMarker)),
    textStep("TASK_PARENT_FINISHED", body => sourceMarkerPrompt(body, parentTaskMarker) && historyHasTool(body, "task")),
  );
  const parentTask = send(sourceTarget, parentTaskMarker);
  await waitTask(parentTask, "TASK_PARENT_FINISHED");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Actual task child did not finish its finite model script");
  const taskChildRequest = requests.find(request => requestPromptText(request.body).includes(taskChildMarker) && !requestPromptText(request.body).includes(parentTaskMarker));
  assert.ok(taskChildRequest, "No actual OMP task child provider request was captured");
  const taskChildPrompt = requestPromptText(taskChildRequest.body);
  assert.ok(!taskChildPrompt.includes(AGENT_GUIDANCE_PREFIX), "Task child automatically inherited the parent's appended fixed guide");
  assert.ok(!taskChildPrompt.includes(fixture.botUid), "Task child automatically inherited the parent's bot identity");
  assert.ok(!taskChildPrompt.includes(groupId), "Task child automatically inherited the parent's unique group identity");
  assert.ok(!taskChildPrompt.includes("NEW_SESSION_GROUP_DESCRIPTION_V4"), "Task child automatically inherited the parent's shared description");
  assert.ok(!taskChildPrompt.includes("NEW_SESSION_MEMORY_V4"), "Task child automatically inherited the parent's Memory section");
  assert.ok(!taskChildPrompt.includes("new-session-member-uid"), "Task child automatically inherited the parent's member snapshot");

  const parentEvalMarker = "EVAL_PARENT_UNIQUE_WITH_SOURCE_CONTEXT";
  const evalChildMarker = "EVAL_CHILD_UNIQUE";
  const evalCode = `const h = await agent("${evalChildMarker}: return one short result.", { agent: "task", label: "context-eval-child" }); await wait([h]); console.log("EVAL_CHILD_FINISHED");`;
  stepsFor(
    toolStep("eval", { language: "js", code: evalCode }, "context-eval-agent", body => sourceMarkerPrompt(body, parentEvalMarker) && !historyHasTool(body, "eval")),
    toolStep("yield", { data: { result: "EVAL_CHILD_FINISHED" } }, "context-eval-yield", body => requestContains(body, evalChildMarker) && !sourceMarkerPrompt(body, parentEvalMarker)),
    textStep("EVAL_PARENT_FINISHED", body => sourceMarkerPrompt(body, parentEvalMarker) && historyHasTool(body, "eval")),
  );
  const parentEval = send(sourceTarget, parentEvalMarker);
  await waitTask(parentEval, "EVAL_PARENT_FINISHED");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Actual eval child did not finish its finite model script");
  const evalChildRequest = requests.find(request => requestPromptText(request.body).includes(evalChildMarker) && !requestPromptText(request.body).includes(parentEvalMarker));
  assert.ok(evalChildRequest, "No actual OMP eval-agent provider request was captured");
  const evalChildPrompt = requestPromptText(evalChildRequest.body);
  assert.ok(!evalChildPrompt.includes(AGENT_GUIDANCE_PREFIX), "Eval child automatically inherited the parent's appended fixed guide");
  assert.ok(!evalChildPrompt.includes(fixture.botUid), "Eval child automatically inherited the parent's bot identity");
  assert.ok(!evalChildPrompt.includes(groupId), "Eval child automatically inherited the parent's unique group identity");
  assert.ok(!evalChildPrompt.includes("NEW_SESSION_GROUP_DESCRIPTION_V4"), "Eval child automatically inherited the parent's shared description");
  assert.ok(!evalChildPrompt.includes("NEW_SESSION_MEMORY_V4"), "Eval child automatically inherited the parent's Memory section");
  assert.ok(!evalChildPrompt.includes("new-session-member-uid"), "Eval child automatically inherited the parent's member snapshot");
  console.log("CASE task-eval-isolation {" + JSON.stringify({ taskChildRequestCaptured: true, evalChildRequestCaptured: true, parentContextInherited: false }) + "}");

  const sourceBeforeFork = binding(sourceTarget)!;
  assert.ok(sourceBeforeFork.session_file, "Fork source had no persisted OMP session");
  stepsFor(
    toolStep("fork", { name: "context-stability-fork" }, "context-fork", body => sourceMarkerPrompt(body, "FORK_SOURCE_USER") && !historyHasTool(body, "fork")),
    textStep("FORK_SOURCE_TURN_DONE", body => sourceMarkerPrompt(body, "FORK_SOURCE_USER") && historyHasTool(body, "fork")),
  );
  const forkTask = send(sourceTarget, "FORK_SOURCE_USER");
  await waitTask(forkTask, "FORK_SOURCE_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0, "Native fork setup did not consume its finite model steps");
  const forkThread = (await threadList()).find(thread => thread.name === "context-stability-fork");
  assert.ok(forkThread, "OMP fork did not create its local Octo target thread");
  assert.equal(forkThread.short_id, "thread-0", "Fixture thread sequence changed; update the explicit target fixture");
  const forkTarget: Target = { channelType: 5, channelId: forkThread.channel_id };
  const forkBinding = binding(forkTarget);
  assert.ok(forkBinding?.session_file, "Fork target did not receive its separate native session");
  assert.notEqual(forkBinding.session_id, sourceBeforeFork.session_id, "Fork target reused the source session id");
  assert.deepEqual(binding(sourceTarget), sourceBeforeFork, "Creating a fork changed the source chat binding");
  const targetDescriptionWrite = await fetch(`${fixture.apiUrl}/v1/bot/groups/${encodeURIComponent(groupId)}/threads/${encodeURIComponent(forkThread.short_id)}/md`, {
    method: "PUT",
    headers: { authorization: `Bearer ${fixture.botToken}`, "content-type": "application/json" },
    body: JSON.stringify({ content: targetContext.content }),
  });
  assert.equal(targetDescriptionWrite.status, 200, "Could not write the disposable fork target's own fresh description");

  stepsFor(textStep("FORK_TARGET_TURN_DONE", body => sourceMarkerPrompt(body, "FORK_TARGET_CURRENT_USER")));
  const forkTargetRequestAt = requests.length;
  const forkTargetTask = send(forkTarget, "FORK_TARGET_CURRENT_USER");
  await waitTask(forkTargetTask, "FORK_TARGET_TURN_DONE");
  await waitForWorkerCleanup();
  assert.equal(steps.length, 0);
  const forkTargetRequest = requests[forkTargetRequestAt]!.body;
  const forkTargetSystem = captureSystem(forkTargetRequest, forkTarget);
  assert.notEqual(forkTargetSystem.guide, newSessionSystem.guide, "Fork target reused the source chat's current fixed guide");
  const forkTargetMessages = requestMessageText(forkTargetRequest);
  assert.ok(forkTargetMessages.includes("FORK_SOURCE_USER"), "Fork target lost the source's explicit business prompt history");
  assert.ok(!forkTargetMessages.includes("NEW_SESSION_GROUP_DESCRIPTION_V4"), "Fork target inherited the source-managed description");
  assert.ok(!forkTargetMessages.includes("NEW_SESSION_MEMORY_V4"), "Fork target inherited the source-managed Memory section");
  assert.ok(!forkTargetMessages.includes("new-session-member-uid"), "Fork target inherited the source-managed member snapshot");
  assert.ok(forkTargetMessages.includes("FORK_THREAD_DESCRIPTION_FRESH"), "Fork target did not read its own fresh thread description");
  assert.ok(forkTargetMessages.includes("FORK_THREAD_MEMORY_FRESH"), "Fork target did not read its own fresh Memory section");
  assert.ok(forkTargetMessages.includes("thread-member-uid"), "Fork target did not add its own thread member snapshot");
  assert.equal(memberMessageIndices(forkTargetRequest).length, 1, "Fork target retained a source-managed member snapshot");
  assert.ok(memberSnapshotText(forkTargetRequest, 0).includes("thread-member-uid"), "Fork target member message did not belong to its own thread");
  assert.equal(memberReadCount(forkTarget), 1, "Fork target did not fetch members from the precise thread endpoint");
  assert.ok(contextReadCount(forkTarget) > 0, "Fork target did not fetch the precise thread description");
  console.log("CASE new-session-fork-target " + JSON.stringify({
    sourceSystemSha256: systemHash(newSessionSystem),
    forkTargetSystemSha256: systemHash(forkTargetSystem),
    forkTargetSystemMessageCount: forkTargetSystem.messages.length,
    forkTargetGuideSystemIndex: forkTargetSystem.guideIndex,
    sourceHistoryRetained: true,
    targetContextFresh: true,
  }));

  assert.equal(modelError, undefined);
  assert.equal(requests.length <= 64, true, "Finite context model exceeded its request budget");
  assert.equal(steps.length, 0, "Acceptance left unconsumed model responses");
  assert.ok(requests.length > 0);
  assert.ok(requests.every(request => new URL(request.url).hostname === "127.0.0.1"), "A model request left the loopback fixture");
  assert.equal(child!.exitCode, null, "Channel CLI exited before controlled shutdown");
  await waitForWorkerCleanup();
  child!.kill("SIGTERM");
  assert.equal(await exited(child!), 0, "Channel CLI did not stop cleanly on SIGTERM");
  const logs = (await Promise.all(outputs)).join("\n");
  assert.ok(!logs.includes(fixture.botToken), "Channel CLI leaked the local fixture bot token");
  succeeded = true;
  console.log(JSON.stringify({ realOmp, loopbackProviderRequests: requests.length, encryptedOctoFixture: true, residentServiceTouched: false }));
} catch (error) {
  const redact = (value: string): string => value.replaceAll(fixture.botToken, "[fixture-token]");
  await writeFile(join(root, "provider-requests.json"), redact(JSON.stringify(requests, null, 2)), { mode: 0o600 });
  const taskRows = db?.query("SELECT message_id, state, error_text FROM tasks ORDER BY rowid").all() ?? [];
  const safeTasks = taskRows.map(value => {
    const row = value as { message_id: string; state: string; error_text: string | null };
    return { ...row, error_text: row.error_text === null ? null : redact(row.error_text) };
  });
  let contextLedger: unknown = null;
  try {
    contextLedger = db?.query(
      "SELECT bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version FROM session_context_ledger ORDER BY rowid",
    ).all() ?? null;
  } catch {
    contextLedger = null;
  }
  const failure = error instanceof Error ? error.stack ?? error.message : String(error);
  console.error(JSON.stringify({
    root,
    failure: redact(failure),
    modelError: modelError === undefined ? null : redact(modelError),
    requests: requests.map((request, index) => ({
      index,
      url: redact(request.url),
      system: redact(requestSystemText(request.body).slice(0, 400)),
      messages: redact(requestMessageText(request.body).slice(0, 1_000)),
    })),
    sentMessages: fixture.sentMessages.slice(-16).map(message => ({
      channelId: message.body.channel_id,
      text: redact(messageText(message).slice(0, 300)),
    })),
    tasks: safeTasks,
    contextLedger,
    bindings: db?.query("SELECT channel_type, channel_id, session_id, cwd, session_file FROM chats ORDER BY rowid").all(),
  }));
  process.exitCode = 1;
} finally {
  for (const held of gates) held.resolve();
  for (const process of children) {
    if (process.exitCode !== null) continue;
    process.kill("SIGTERM");
    try {
      await exited(process, 10_000);
    } catch {
      process.kill("SIGKILL");
      await process.exited;
    }
  }
  await Promise.allSettled(children.map(process => process.exited));
  db?.close();
  await fixture.close();
  modelServer?.stop(true);
  if (!succeeded) {
    const logs = (await Promise.allSettled(outputs)).flatMap(result => result.status === "fulfilled" ? [result.value] : []).join("\n");
    console.error(logs.replaceAll(fixture.botToken, "[fixture-token]").slice(-12_000));
  }
  if (succeeded) await rm(root, { recursive: true, force: true });
  else console.error(`Acceptance workspace retained for inspection: ${root}`);
}
