import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { OmpWorker, WorkerOptions } from "../../src/contracts.ts";
import { createWorkerFactory } from "../../src/omp/index.ts";
import { contextSidecarPath, writePreparedReceipt } from "../../src/session/context-sidecar.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  type CoreHarness,
  type SentCard,
} from "../fixtures/core-fakes.ts";

type ProviderRequest = { body: Record<string, unknown> };
type AuditRow = {
  event: "startup" | "set-ask-dialog" | "ui-request" | "ui-response" | "tool-execution" | "message-end" | "agent-start" | "rpc-response";
  mode?: string;
  enabled?: boolean;
  method?: string;
  selectionMarker?: string;
  checkedIndices?: number[];
  markableCount?: number;
  postMarkableActionCount?: number;
  postMarkableActionLabel?: string;
  optionsCount?: number;
  doneIndex?: number;
  otherIndex?: number;
  secret?: boolean;
  cancelled?: boolean;
  hasValue?: boolean;
  answersCount?: number;
  hasTarget?: boolean;
  order?: number;
  processId?: number;
  direction?: string;
  frameType?: string;
  eventType?: string;
  eventPath?: string;
  nested?: boolean;
  toolName?: string | null;
  toolNamePath?: string | null;
  hasToolCallId?: boolean;
  toolCallIdPaths?: string[];
  messagePath?: string;
  hasMessageId?: boolean;
  hasAskToolCall?: boolean;
  askToolCalls?: Array<{ path: string; hasIdField: boolean; idIsString: boolean }>;
  hasId?: boolean;
  command?: string;
  success?: boolean;
  agentInvoked?: boolean;
  status?: string;
  sessionSettled?: boolean;
};
type TaskRow = {
  id: string;
  state: string;
  prompt_accepted: number;
  session_id: string | null;
  error_text: string | null;
};
type QuestionRow = {
  id: string;
  state: string;
  answer: string | null;
  card_message_id: string | null;
};
type BindingRow = {
  session_id: string;
  session_file: string;
  cwd: string;
};
type CardAction = {
  actionId: string;
  inputs: Record<string, string>;
};
type ModelScript =
  | { marker: string; html: string; resultMarker: string; expected: readonly string[] }
  | { marker: string; html: string; cancel: true };

const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-native-ask-")));
const target = { channelType: 1 as const, channelId: "dm-native-ask" };
const profileName = "native-ask-acceptance";
const agentDir = join(root, ".omp", "profiles", profileName, "agent");
const binDir = join(root, "bin");
const cwd = join(root, "cwd");
const auditPath = join(root, "rpc-audit.jsonl");
const wrapperPath = join(root, "real-omp.ts");
const modelRequests: ProviderRequest[] = [];
const scriptResults = new Set<string>();
const emittedAskToolCalls = new Set<string>();
const cardActionTrace: Array<{ eventId: number; messageId: string; actionId: string }> = [];
const scripts: ModelScript[] = [
  {
    marker: "NATIVE_ASK_FULL_FORM_CASE_84",
    html: `<form>
      <p>Complete the isolated native ask acceptance form.</p>
      <fieldset><legend>Single choice</legend>
        <label><input type="radio" name="single" value="single-42"> single-42</label>
        <label><input type="radio" name="single" value="single-84"> single-84</label>
      </fieldset>
      <fieldset><legend>Selected features</legend>
        <label><input type="checkbox" name="features" value="feature:lint"> feature:lint</label>
        <label><input type="checkbox" name="features" value="feature/test"> feature/test</label>
      </fieldset>
      <label>Short text <input type="text" name="short" placeholder="Short acceptance text" required></label>
      <label>Editor text <textarea name="note" required>EDITOR_PREFILL_LOCAL</textarea></label>
    </form>`,
    resultMarker: "NATIVE_ASK_FULL_FORM_RESULT",
    expected: ["single-84", "feature:lint", "feature/test", "SHORT_TEXT_LOCAL_84", "EDITOR_TEXT_LOCAL_84"],
  },
  {
    marker: "NATIVE_ASK_CANCEL_CASE_39",
    html: `<form><label>Cancellation editor <textarea name="draft" required>UNANSWERED_DRAFT_LOCAL</textarea></label></form>`,
    cancel: true,
  },
  {
    marker: "NATIVE_ASK_RESTART_CASE_73",
    html: `<form>
      <label>Recovered worker route <select name="route" required>
        <option value="route-17">route-17</option>
        <option value="route-73">route-73</option>
      </select></label>
      <label>Recovered editor <textarea name="note" required>RESTART_EDITOR_PREFILL_LOCAL</textarea></label>
    </form>`,
    resultMarker: "NATIVE_ASK_RESTART_RESULT",
    expected: ["route-73", "EDITOR_TEXT_LOCAL_73"],
  },
];
const bootstrapReplies = [
  "RATCHET_ACCEPTANCE_SEED_1",
  "RATCHET_ACCEPTANCE_SEED_2",
  "RATCHET_ACCEPTANCE_SEED_3",
  "RATCHET_ACCEPTANCE_SEED_4",
];
let modelError: string | undefined;
let modelServer: ReturnType<typeof Bun.serve> | undefined;
let harness: CoreHarness | undefined;
let transport: FakeTransport | undefined;
let db: Database | undefined;
let eventId = 200;
let diagnosticMessageId: string | undefined;
const originalEnvironment = new Map<string, string | undefined>();
const isolatedEnvironment = [
  "HOME",
  "PATH",
  "TMPDIR",
  "PI_CONFIG_DIR",
  "OMP_CONFIG_DIR",
  "PI_CODING_AGENT_DIR",
  "OMP_CODING_AGENT_DIR",
  "PI_PROFILE",
  "OMP_PROFILE",
  "XDG_DATA_HOME",
  "PI_NO_TITLE",
  "PI_NO_PTY",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "ALL_PROXY",
  "http_proxy",
  "https_proxy",
  "all_proxy",
  "NO_PROXY",
  "no_proxy",
  "LANG",
  "TZ",
];
for (const key of isolatedEnvironment) originalEnvironment.set(key, process.env[key]);

function requestMessages(body: Record<string, unknown>): unknown[] {
  return Array.isArray(body.messages) ? body.messages : [];
}

function serializedMessages(body: Record<string, unknown>): string {
  return JSON.stringify(requestMessages(body));
}
function latestUserMessage(body: Record<string, unknown>): string {
  const messages = requestMessages(body);
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message !== null && typeof message === "object" && !Array.isArray(message)
      && "role" in message && message.role === "user") return JSON.stringify(message);
  }
  return "";
}

function askToolResultSincePrompt(body: Record<string, unknown>, marker: string): boolean {
  const messages = requestMessages(body);
  let promptIndex = -1;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message !== null && typeof message === "object" && !Array.isArray(message)
      && "role" in message && message.role === "user" && JSON.stringify(message).includes(marker)) {
      promptIndex = index;
      break;
    }
  }
  if (promptIndex < 0) return false;
  const askCallIds = new Set<string>();
  for (const value of messages.slice(promptIndex + 1)) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
    if ("role" in value && value.role === "assistant" && "tool_calls" in value && Array.isArray(value.tool_calls)) {
      for (const call of value.tool_calls) {
        if (call === null || typeof call !== "object" || Array.isArray(call) || !("id" in call)
          || typeof call.id !== "string" || !("function" in call)) continue;
        const fn = call.function;
        if (fn !== null && typeof fn === "object" && !Array.isArray(fn) && "name" in fn && fn.name === "ask") {
          askCallIds.add(call.id);
        }
      }
    }
    if ("role" in value && value.role === "tool" && "tool_call_id" in value
      && typeof value.tool_call_id === "string" && askCallIds.has(value.tool_call_id)) return true;
  }
  return false;
}

function hasTool(body: Record<string, unknown>, name: string): boolean {
  if (!Array.isArray(body.tools)) return false;
  return body.tools.some(value => {
    if (value === null || typeof value !== "object" || Array.isArray(value) || !("function" in value)) return false;
    const fn = value.function;
    return fn !== null && typeof fn === "object" && !Array.isArray(fn)
      && "name" in fn && fn.name === name;
  });
}

function askToolCallWasDispatched(body: Record<string, unknown>, marker: string): boolean {
  const messages = requestMessages(body);
  let promptIndex = -1;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message !== null && typeof message === "object" && !Array.isArray(message)
      && "role" in message && message.role === "user" && JSON.stringify(message).includes(marker)) {
      promptIndex = index;
      break;
    }
  }
  if (promptIndex < 0) return false;
  return messages.slice(promptIndex + 1).some(value => {
    if (value === null || typeof value !== "object" || Array.isArray(value)
      || !("role" in value) || value.role !== "assistant" || !("tool_calls" in value) || !Array.isArray(value.tool_calls)) return false;
    return value.tool_calls.some(call => {
      if (call === null || typeof call !== "object" || Array.isArray(call) || !("function" in call)) return false;
      const fn = call.function;
      return fn !== null && typeof fn === "object" && !Array.isArray(fn)
        && "name" in fn && fn.name === "ask";
    });
  });
}

function textReply(text: string, index: number): Response {
  const id = `native-ask-${index}`;
  const chunks = [
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: { role: "assistant", content: text }, finish_reason: null }] },
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function askReply(html: string, index: number, marker: string): Response {
  const id = `native-ask-${index}`;
  const chunks = [
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{
        index: 0,
        delta: {
          role: "assistant",
          tool_calls: [{
            index: 0,
            id: `native-ask-call-${index}`,
            type: "function",
            function: { name: "ask", arguments: JSON.stringify({ html }) },
          }],
        },
        finish_reason: null,
      }],
    },
    {
      id,
      object: "chat.completion.chunk",
      created: 0,
      model: "probe",
      choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  ];
  const response = new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
  emittedAskToolCalls.add(marker);
  return response;
}

function recordModelFailure(reason: string): Response {
  modelError ??= reason;
  return new Response(reason, { status: 400 });
}

function consumeModelScript(body: Record<string, unknown>, index: number): Response {
  const messages = serializedMessages(body);
  const latestUser = latestUserMessage(body);
  const script = scripts.find(candidate => latestUser.includes(candidate.marker));
  if (script) {
    if (askToolResultSincePrompt(body, script.marker)) {
      if ("cancel" in script) return recordModelFailure("unexpected-result-after-cancel-case");
      if (!script.expected.every(value => messages.includes(value))) return recordModelFailure("native-ask-answer-did-not-reach-model");
      scriptResults.add(script.marker);
      return textReply(script.resultMarker, index);
    }
    if ("cancel" in script && askToolCallWasDispatched(body, script.marker)) {
      return recordModelFailure("cancel-case-was-replayed-without-an-answer");
    }
    if (!hasTool(body, "ask")) return recordModelFailure("native-ask-tool-not-advertised");
    return askReply(script.html, index, script.marker);
  }
  const bootstrap = bootstrapReplies.shift();
  if (bootstrap) return textReply(bootstrap, index);
  return recordModelFailure("finite-native-ask-script-exhausted");
}

async function readAudit(): Promise<AuditRow[]> {
  try {
    return (await readFile(auditPath, "utf8"))
      .split("\n")
      .filter(Boolean)
      .map(line => JSON.parse(line) as AuditRow);
  } catch {
    return [];
  }
}
async function waitForAudit<T>(
  description: string,
  select: (rows: AuditRow[]) => T | undefined,
  timeoutMs = 45_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    assert.equal(modelError, undefined, "Finite loopback model rejected an unexpected request");
    const selected = select(await readAudit());
    if (selected !== undefined) return selected;
    await Bun.sleep(20);
  }
  throw new Error(description);
}

async function wait(description: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    assert.equal(modelError, undefined, "Finite loopback model rejected an unexpected request");
    if (predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error(description);
}

function task(messageId: string): TaskRow | null {
  return db!.query("SELECT id, state, prompt_accepted, session_id, error_text FROM tasks WHERE message_id = ?")
    .get(messageId) as TaskRow | null;
}

function questionsForTask(taskId: string): QuestionRow[] {
  return db!.query("SELECT id, state, answer, card_message_id FROM questions WHERE task_id = ? ORDER BY rowid")
    .all(taskId) as QuestionRow[];
}

function binding(): BindingRow | null {
  return db!.query(
    "SELECT session_id, session_file, cwd FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(harness!.bot.uid, target.channelType, target.channelId) as BindingRow | null;
}

async function waitForWorkerCleanup(): Promise<void> {
  await wait("A settled native-ask task retained an OMP worker", () => {
    const row = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return row.count === 0;
  });
}

function cardActionIds(value: unknown, ids: string[] = []): string[] {
  if (Array.isArray(value)) {
    for (const item of value) cardActionIds(item, ids);
  } else if (value !== null && typeof value === "object") {
    if ("id" in value && typeof value.id === "string" && value.id.startsWith("ask_")) ids.push(value.id);
    for (const child of Object.values(value)) cardActionIds(child, ids);
  }
  return ids;
}

function cardInputIds(
  value: unknown,
  ids: { choices: string[]; text: string[] } = { choices: [], text: [] },
): { choices: string[]; text: string[] } {
  if (Array.isArray(value)) {
    for (const item of value) cardInputIds(item, ids);
  } else if (value !== null && typeof value === "object") {
    if ("type" in value && value.type === "Input.ChoiceSet" && "id" in value && typeof value.id === "string") {
      ids.choices.push(value.id);
    }
    if ("type" in value && value.type === "Input.Text" && "id" in value && typeof value.id === "string") {
      ids.text.push(value.id);
    }
    for (const child of Object.values(value)) cardInputIds(child, ids);
  }
  return ids;
}

async function answerCard(card: SentCard, action: CardAction): Promise<void> {
  const currentTransport = transport!;
  const id = ++eventId;
  currentTransport.pushBotEvent(cardActionEvent(id, {
    ...target,
    messageId: card.messageId,
    actionId: action.actionId,
    operatorUid: "owner-1",
    inputs: action.inputs,
  }));
  cardActionTrace.push({ eventId: id, messageId: card.messageId, actionId: action.actionId });
  await wait(`Question-card action ${action.actionId} was not acknowledged`, () => currentTransport.ackedEvents.includes(id), 10_000);
}

async function waitForCard(index: number, description: string): Promise<SentCard> {
  await wait(description, () => transport!.sentCards.length > index);
  const card = transport!.sentCards[index]!;
  assert.equal(card.target.channelType, target.channelType);
  assert.equal(card.target.channelId, target.channelId);
  assert.equal(
    transport!.sentCards.filter(sent => sent.messageId === card.messageId).length,
    1,
    `The fixture reused card message ID ${card.messageId}`,
  );
  return card;
}

async function answerWithOption(card: SentCard, optionIndex: number): Promise<void> {
  const actionIds = cardActionIds(card.frame.card);
  const optionAction = actionIds.find(id => new RegExp(`^ask_opt_${optionIndex}(?:#\\d+)?$`, "u").test(id));
  if (optionAction) {
    await answerCard(card, { actionId: optionAction, inputs: {} });
    return;
  }

  const ids = cardInputIds(card.frame.card);
  assert.ok(ids.choices.length > 0, "Question station card exposed neither the requested option action nor a choice input");
  const submitAction = actionIds.find(id => id === "ask_submit" || id.startsWith("ask_submit#"));
  assert.ok(submitAction, "Choice-input question card omitted its submit action");
  await answerCard(card, {
    actionId: submitAction,
    inputs: Object.fromEntries(ids.choices.map(id => [id, String(optionIndex)])),
  });
}

async function answerWithText(card: SentCard, value: string): Promise<void> {
  const ids = cardInputIds(card.frame.card);
  assert.ok(ids.text.length > 0, "Text/editor question card omitted its Input.Text field");
  const actionId = cardActionIds(card.frame.card).find(id => id === "ask_submit" || id.startsWith("ask_submit#"));
  assert.ok(actionId, "Text/editor question card omitted its submit action");
  const inputs = Object.fromEntries(ids.text.map(id => [id, value]));
  await answerCard(card, { actionId, inputs });
}

async function answerInitialQuestion(card: SentCard): Promise<void> {
  const ids = cardInputIds(card.frame.card);
  const actionIds = cardActionIds(card.frame.card);
  const submit = actionIds.find(id => id === "ask_submit" || id.startsWith("ask_submit#"));
  if (submit) {
    const inputs: Record<string, string> = {};
    for (const id of ids.choices) inputs[id] = "1";
    if (ids.choices.length === 0) for (const id of ids.text) inputs[id] = "approve-local-ratchet";
    await answerCard(card, { actionId: submit, inputs });
    return;
  }
  const option = actionIds.find(id => /^ask_opt_\d+(?:#\d+)?$/u.test(id));
  assert.ok(option, "Initial ratchet question card had no answer action");
  await answerCard(card, { actionId: option, inputs: {} });
}

async function waitForInitialRatchet(messageId: string, firstCardIndex: number): Promise<void> {
  const handledCards = new Set<string>();
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    assert.equal(modelError, undefined, "Finite loopback model rejected an unexpected request");
    const row = task(messageId);
    if (row && ["failed", "cancelled", "interrupted"].includes(row.state)) {
      throw new Error("The isolated /ratchet seed task did not complete");
    }
    if (row?.state === "completed") return;
    const status = await harness!.service.status(target);
    if (status.pendingQuestionIds.length > 0) {
      const nextCard = transport!.sentCards
        .slice(firstCardIndex)
        .find(candidate => !handledCards.has(candidate.messageId));
      if (nextCard) {
        handledCards.add(nextCard.messageId);
        await answerInitialQuestion(nextCard);
        continue;
      }
    }
    await Bun.sleep(20);
  }
  throw new Error("The isolated /ratchet seed task did not settle");
}

async function waitForTask(messageId: string, result: string): Promise<void> {
  await wait(`Task ${messageId} did not finish through the expected local-model path`, () => {
    const row = task(messageId);
    if (row && ["failed", "cancelled", "interrupted"].includes(row.state)) {
      throw new Error(`An isolated native-ask task ended ${row.state}`);
    }
    return row?.state === "completed" && transport!.sent.some(message =>
      message.target.channelId === target.channelId && message.text.includes(result),
    );
  });
}

async function createHarness(): Promise<void> {
  transport = transport ?? new FakeTransport();
  transport.cardSupported = true;
  const workerFactory = createWorkerFactory({
    ompCommand: wrapperPath,
    extraArgs: [
      "--profile", profileName,
      "--model", "local-native-ask/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp",
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
  });
  class RpcWorkers extends FakeWorkerFactory {
    override async create(options: WorkerOptions): Promise<OmpWorker> {
      const worker = await workerFactory.create(options);
      const prompt = worker.prompt.bind(worker);
      worker.prompt = async (text, onEvent, context) => {
        const injection = context?.contextInjection;
        if (injection && injection.injectionDecision !== "unchanged" && options.contextScope) {
          const session = await worker.refreshState();
          await writePreparedReceipt(contextSidecarPath(options.contextScope.sidecarRoot, session.sessionId), {
            botUid: options.contextScope.botUid,
            target: options.contextScope.target,
            sessionId: session.sessionId,
            version: injection.snapshot.version,
            attemptId: injection.attemptId,
          });
        }
        return prompt(text, onEvent, context);
      };
      return worker;
    }
  }
  harness = await createCoreHarness({
    root,
    transport,
    workers: new RpcWorkers(),
    ask: { timeoutMs: 90_000 },
    groupHistory: { enabled: false, maxMessages: 50 },
  });
  db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
}

const fullForm: Extract<ModelScript, { expected: readonly string[] }> = scripts[0] as Extract<ModelScript, { expected: readonly string[] }>;
const cancelForm: Extract<ModelScript, { cancel: true }> = scripts[1] as Extract<ModelScript, { cancel: true }>;
const restartForm: Extract<ModelScript, { expected: readonly string[] }> = scripts[2] as Extract<ModelScript, { expected: readonly string[] }>;

try {
  for (const key of isolatedEnvironment) delete process.env[key];
  process.env.HOME = root;
  process.env.PATH = `${binDir}:/usr/bin:/bin`;
  process.env.TMPDIR = root;
  process.env.PI_NO_TITLE = "1";
  process.env.PI_NO_PTY = "1";
  process.env.NO_PROXY = "127.0.0.1,localhost";
  process.env.no_proxy = "127.0.0.1,localhost";
  process.env.LANG = "C";
  process.env.TZ = "UTC";

  await mkdir(agentDir, { recursive: true });
  await mkdir(binDir, { recursive: true });
  await mkdir(cwd, { recursive: true });
  await writeFile(auditPath, "", { mode: 0o600 });
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(binDir, command), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  }
  await writeFile(join(agentDir, "models.yml"), JSON.stringify({ providers: {
    "local-native-ask": {
      baseUrl: "",
      apiKey: "native-ask-local-placeholder",
      api: "openai-completions",
      models: [{ id: "probe", name: "Native Ask Acceptance Probe", reasoning: false, input: ["text"], contextWindow: 32_768, maxTokens: 2_048, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    },
  } }));
  await writeFile(join(agentDir, "config.yml"), "autoRetry:\n  enabled: false\nask:\n  enabled: true\n  timeout: 0\n");

  modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      if (request.method !== "POST") return new Response("unsupported model-fixture method", { status: 405 });
      if (modelRequests.length >= 32) return recordModelFailure("finite-native-ask-request-budget-exhausted");
      const url = new URL(request.url);
      if (url.hostname !== "127.0.0.1") return recordModelFailure("provider-request-left-loopback");
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        return recordModelFailure("loopback-model-request-was-not-json");
      }
      modelRequests.push({ body });
      return consumeModelScript(body, modelRequests.length);
    },
  });
  assert.ok(modelServer.port, "Local finite model did not bind");
  const profileProviderPath = join(agentDir, "models.yml");
  const providerConfig = JSON.parse(await readFile(profileProviderPath, "utf8")) as { providers: Record<string, { baseUrl: string }> };
  providerConfig.providers["local-native-ask"]!.baseUrl = `http://127.0.0.1:${modelServer.port}/v1`;
  await writeFile(profileProviderPath, JSON.stringify(providerConfig));

  const wrapperProgram = `#!${process.execPath}
import { appendFileSync } from "node:fs";
const auditPath = ${JSON.stringify(auditPath)};
const processId = process.pid;
const args = process.argv.slice(2);
const modeAt = args.indexOf("--mode");
appendFileSync(auditPath, JSON.stringify({ event: "startup", processId, mode: modeAt >= 0 ? args[modeAt + 1] : null }) + "\\n");
const child = Bun.spawn([${JSON.stringify(realOmp)}, ...args], { stdin: "pipe", stdout: "pipe", stderr: "pipe", env: process.env });
let wireOrder = 0;
function toolExecutionEvents(frame) {
  const events = [];
  function collectFields(node, path, eventPath, toolNames, toolCallIdPaths) {
    if (node === null || typeof node !== "object") return;
    if (Array.isArray(node)) {
      node.forEach((value, index) => collectFields(value, path + "[" + index + "]", eventPath, toolNames, toolCallIdPaths));
      return;
    }
    for (const [key, value] of Object.entries(node)) {
      const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
      if (["arguments", "params", "input", "body", "content", "result"].includes(normalized)) continue;
      const fieldPath = path + "." + key;
      const parent = path.slice(path.lastIndexOf(".") + 1).replace(/[^a-z0-9]/gi, "").toLowerCase();
      if ((normalized === "toolname" || (normalized === "name" && (path === eventPath || parent === "tool" || parent === "toolcall")))
        && typeof value === "string") {
        toolNames.push({ value, path: fieldPath, normalized });
      }
      if (normalized === "toolcallid" || normalized === "callid") toolCallIdPaths.push(fieldPath);
      collectFields(value, fieldPath, eventPath, toolNames, toolCallIdPaths);
    }
  }
  function visit(node, path) {
    if (node === null || typeof node !== "object") return;
    if (Array.isArray(node)) {
      node.forEach((value, index) => visit(value, path + "[" + index + "]"));
      return;
    }
    if (node.type === "tool_execution_start" || node.type === "tool_execution_end") {
      const toolNames = [];
      const toolCallIdPaths = [];
      collectFields(node, path, path, toolNames, toolCallIdPaths);
      const toolName = toolNames.find(field => field.normalized === "toolname") ?? toolNames[0];
      events.push({
        eventType: node.type,
        eventPath: path,
        nested: path !== "$",
        toolName: toolName?.value ?? null,
        toolNamePath: toolName?.path ?? null,
        hasToolCallId: toolCallIdPaths.length > 0,
        toolCallIdPaths,
      });
    }
    for (const [key, value] of Object.entries(node)) {
      const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
      if (["arguments", "params", "input", "body", "content", "result"].includes(normalized)) continue;
      visit(value, path + "." + key);
    }
  }
  visit(frame, "$");
  return events;
}
function assistantMessageEndRows(frame) {
  const rows = [];
  function visit(node, path) {
    if (node === null || typeof node !== "object") return;
    if (Array.isArray(node)) {
      node.forEach((value, index) => visit(value, path + "[" + index + "]"));
      return;
    }
    if (node.role === "assistant" && Array.isArray(node.content)) {
      const askToolCalls = [];
      node.content.forEach((block, index) => {
        if (block === null || typeof block !== "object" || Array.isArray(block)) return;
        const blockType = typeof block.type === "string"
          ? block.type.replace(/[^a-z0-9]/gi, "").toLowerCase()
          : "";
        if (blockType === "toolcall" && block.name === "ask") {
          askToolCalls.push({
            path: path + ".content[" + index + "]",
            hasIdField: Object.prototype.hasOwnProperty.call(block, "id"),
            idIsString: typeof block.id === "string",
          });
        }
      });
      rows.push({
        messagePath: path,
        hasMessageId: typeof node.id === "string",
        hasAskToolCall: askToolCalls.length > 0,
        askToolCalls,
      });
    }
    for (const [key, value] of Object.entries(node)) {
      const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
      if (["arguments", "params", "input", "body", "content", "result"].includes(normalized)) continue;
      visit(value, path + "." + key);
    }
  }
  visit(frame, "$");
  return rows;
}
function audit(direction, frame) {
  const order = ++wireOrder;
  if (direction === "out" && frame.type === "extension_ui_request") {
    const options = Array.isArray(frame.options) ? frame.options.filter(value => typeof value === "string") : [];
    const markableCount = Number.isSafeInteger(frame.markableCount) && frame.markableCount >= 0 ? frame.markableCount : null;
    const postMarkableActions = markableCount === null ? [] : options.slice(markableCount);
    const postMarkableActionCount = postMarkableActions.length;
    const doneIndex = markableCount !== null && postMarkableActionCount === 1 ? markableCount + 1 : 0;
    const postMarkableActionLabel = postMarkableActions[0]?.slice(0, 200);
    const otherIndex = options.findIndex(value => value === "Other (type your own)") + 1;
    const row = {
      event: "ui-request", processId, order, method: typeof frame.method === "string" ? frame.method : "unknown",
      ...(typeof frame.selectionMarker === "string" ? { selectionMarker: frame.selectionMarker } : {}),
      ...(Array.isArray(frame.checkedIndices) ? { checkedIndices: frame.checkedIndices.filter(Number.isSafeInteger) } : {}),
      ...(markableCount !== null ? { markableCount, postMarkableActionCount } : {}),
      ...(Array.isArray(frame.options) ? { optionsCount: options.length } : {}),
      ...(doneIndex > 0 ? { doneIndex } : {}),
      ...(postMarkableActionLabel !== undefined ? { postMarkableActionLabel } : {}),
      ...(otherIndex > 0 ? { otherIndex } : {}),
      ...(frame.secret === true ? { secret: true } : {}),
      ...(typeof frame.targetId === "string" ? { hasTarget: true } : {}),
    };
    appendFileSync(auditPath, JSON.stringify(row) + "\\n");
  } else if (direction === "out" && frame.type === "message_end") {
    for (const message of assistantMessageEndRows(frame)) {
      appendFileSync(auditPath, JSON.stringify({
        event: "message-end", direction, processId, order, frameType: frame.type, ...message,
      }) + "\\n");
    }
  } else if (direction === "out" && frame.type === "agent_start") {
    appendFileSync(auditPath, JSON.stringify({ event: "agent-start", direction, processId, order, frameType: frame.type }) + "\\n");
  } else if (direction === "out" && (frame.type === "response" || frame.type === "prompt_result")) {
    appendFileSync(auditPath, JSON.stringify({
      event: "rpc-response", direction, processId, order, frameType: frame.type,
      hasId: typeof frame.id === "string",
      ...(typeof frame.command === "string" ? { command: frame.command } : {}),
      ...(typeof frame.success === "boolean" ? { success: frame.success } : {}),
      ...(typeof frame.agentInvoked === "boolean" ? { agentInvoked: frame.agentInvoked } : {}),
      ...(typeof frame.status === "string" ? { status: frame.status } : {}),
      ...(typeof frame.sessionSettled === "boolean" ? { sessionSettled: frame.sessionSettled } : {}),
    }) + "\\n");
  } else if (direction === "out") {
    for (const toolEvent of toolExecutionEvents(frame)) {
      appendFileSync(auditPath, JSON.stringify({
        event: "tool-execution", direction, processId, order,
        frameType: typeof frame.type === "string" ? frame.type : "unknown",
        ...toolEvent,
      }) + "\\n");
    }
  } else if (direction === "in" && frame.type === "set_ask_dialog") {
    appendFileSync(auditPath, JSON.stringify({ event: "set-ask-dialog", processId, order, enabled: frame.enabled === true }) + "\\n");
  } else if (direction === "in" && frame.type === "extension_ui_response") {
    appendFileSync(auditPath, JSON.stringify({
      event: "ui-response", processId, order, cancelled: frame.cancelled === true,
      hasValue: typeof frame.value === "string", answersCount: Array.isArray(frame.answers) ? frame.answers.length : 0,
    }) + "\\n");
  }
}
const inputPump = (async () => {
  let pending = "";
  const decoder = new TextDecoder();
  for await (const bytes of Bun.stdin.stream()) {
    pending += decoder.decode(bytes, { stream: true });
    let end;
    while ((end = pending.indexOf("\\n")) >= 0) {
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (!line.trim()) continue;
      try { audit("in", JSON.parse(line)); } catch {}
      child.stdin.write(line + "\\n");
    }
  }
  pending += decoder.decode();
  if (pending.trim()) {
    try { audit("in", JSON.parse(pending)); } catch {}
    child.stdin.write(pending + "\\n");
  }
  child.stdin.end();
})();
const outputPump = (async () => {
  let pending = "";
  const decoder = new TextDecoder();
  for await (const bytes of child.stdout) {
    pending += decoder.decode(bytes, { stream: true });
    let end;
    while ((end = pending.indexOf("\\n")) >= 0) {
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (line.trim()) {
        try { audit("out", JSON.parse(line)); } catch {}
        process.stdout.write(line + "\\n");
      } else process.stdout.write("\\n");
    }
  }
  pending += decoder.decode();
  if (pending.length > 0) {
    try { audit("out", JSON.parse(pending)); } catch {}
    process.stdout.write(pending);
  }
})();
const stderrPump = (async () => {
  for await (const _ of child.stderr) {}
})();
await Promise.all([inputPump, outputPump, stderrPump]);
process.exit(await child.exited);
`;
  await writeFile(wrapperPath, wrapperProgram, { mode: 0o700 });

  await createHarness();
  const firstTransport = transport!;
  const initialSessionStart = firstTransport.sent.length;
  await harness!.service.submitInbound(inbound({ ...target, messageId: "native-ask-new-session", text: `/new ${cwd}` }));
  await wait("An isolated initial session was not established", () => binding()?.cwd === cwd);
  assert.ok(firstTransport.sent.slice(initialSessionStart).some(message => message.text.includes("已创建会话")), "The real session setup did not complete");
  const originalBinding = binding();
  assert.ok(originalBinding, "The benchmark did not persist its initial session binding");

  const seedCardStart = firstTransport.sentCards.length;
  const ratchetMessageId = "native-ask-ratchet-seed";
  await harness!.service.submitInbound(inbound({ ...target, messageId: ratchetMessageId, text: "/ratchet NATIVE_ASK_ACCEPTANCE_SEED" }));
  await waitForInitialRatchet(ratchetMessageId, seedCardStart);
  const acceptedRatchet = task(ratchetMessageId);
  assert.equal(acceptedRatchet?.prompt_accepted, 1, "The seed /ratchet prompt was not accepted by OMP");
  assert.equal(acceptedRatchet?.state, "completed", "The isolated /ratchet seed did not settle before follow-up work");
  assert.equal(acceptedRatchet?.session_id, originalBinding.session_id, "The accepted /ratchet changed session identity");
  assert.equal((await harness!.service.status(target)).pendingQuestionIds.length, 0, "The seed /ratchet left a question pending");
  await waitForWorkerCleanup();

  const ratchetWire = await readAudit();
  assert.ok(ratchetWire.some(row => row.event === "startup" && row.mode === "rpc"), "Ordinary /new did not use rpc mode");
  assert.ok(ratchetWire.some(row => row.event === "startup" && row.mode === "rpc-ui"), "Accepted /ratchet did not use rpc-ui mode");
  assert.ok(ratchetWire.some(row => row.event === "set-ask-dialog" && row.enabled), "Ratchet worker did not enable native ask dialogs");

  const fullFormCardStart = firstTransport.sentCards.length;
  const initialUiRequestCount = (await readAudit()).filter(row => row.event === "ui-request").length;
  await harness!.service.submitInbound(inbound({ ...target, messageId: "native-ask-full-form", text: fullForm.marker }));
  await wait("The real OMP worker did not invoke its native ask tool", () => emittedAskToolCalls.has(fullForm.marker));
  const singleCard = await waitForCard(fullFormCardStart, "Native single-select did not reach the CoreService question station");
  await answerWithOption(singleCard, 2);
  const checkboxFirstCard = await waitForCard(fullFormCardStart + 1, "Native checkbox selection did not reopen after its single-choice answer");
  const checkboxFirstFrame = await waitForAudit("OMP did not emit the initial checkbox select request", rows => {
    const selects = rows.filter(row => row.event === "ui-request").slice(initialUiRequestCount).filter(row => row.method === "select");
    return selects[1];
  });
  assert.deepEqual(checkboxFirstFrame.checkedIndices ?? [], [], "The first checkbox select did not start with an empty selection");
  await answerWithOption(checkboxFirstCard, 1);
  const checkboxSecondCard = await waitForCard(fullFormCardStart + 2, "OMP checkbox selection did not reopen after the first option");
  await waitForAudit("OMP did not report the first checked checkbox index", rows => {
    const selects = rows.filter(row => row.event === "ui-request").slice(initialUiRequestCount).filter(row => row.method === "select");
    const row = selects[2];
    return row && JSON.stringify(row.checkedIndices ?? []) === "[0]" ? row : undefined;
  });
  const checkboxThirdCard = await (async () => {
    await answerWithOption(checkboxSecondCard, 2);
    return waitForCard(fullFormCardStart + 3, "OMP checkbox selection did not reopen after the second option");
  })();
  const checkboxFrames = (await readAudit()).filter(row => row.event === "ui-request")
    .slice(initialUiRequestCount).filter(row => row.method === "select").slice(1);
  assert.deepEqual(checkboxFrames.slice(0, 3).map(row => row.checkedIndices ?? []), [[], [0], [0, 1]], "OMP did not preserve each incremental checkbox selection");
  const doneFrame = checkboxFrames[2];
  assert.ok(
    doneFrame?.markableCount !== undefined
      && doneFrame.postMarkableActionCount === 1
      && doneFrame.doneIndex === doneFrame.markableCount + 1,
    `The final checkbox select omitted its unique action after markableCount: ${JSON.stringify({
      markableCount: doneFrame?.markableCount,
      postMarkableActionCount: doneFrame?.postMarkableActionCount,
      postMarkableActionLabel: doneFrame?.postMarkableActionLabel,
    })}`,
  );
  await answerWithOption(checkboxThirdCard, doneFrame!.doneIndex!);

  const shortTextCard = await waitForCard(fullFormCardStart + 4, "Native empty text input did not reach the question station");
  await waitForAudit("OMP did not request the HTML text input", rows =>
    rows.find(row => row.event === "ui-request" && row.method === "input"),
  );
  await answerWithText(shortTextCard, "SHORT_TEXT_LOCAL_84");
  const editorCard = await waitForCard(fullFormCardStart + 5, "Native textarea editor did not reach the question station");
  const afterFormUi = await readAudit();
  const fullFormUi = afterFormUi.filter(row => row.event === "ui-request").slice(initialUiRequestCount);
  assert.ok(fullFormUi.some(row => row.method === "select"), "The native HTML radio field did not use select");
  assert.ok(fullFormUi.some(row => row.method === "input"), "The native HTML text field did not use input");
  assert.ok(fullFormUi.some(row => row.method === "editor"), "The native HTML textarea did not use editor");
  await answerWithText(editorCard, "EDITOR_TEXT_LOCAL_84");
  await waitForTask("native-ask-full-form", fullForm.resultMarker);
  assert.ok(scriptResults.has(fullForm.marker), "The full HTML form answers did not reach the model request");
  assert.equal(task("native-ask-full-form")?.session_id, originalBinding.session_id, "The full form task escaped its bound OMP session");
  await waitForWorkerCleanup();

  const cancelCardStart = firstTransport.sentCards.length;
  const cancelRequestStart = modelRequests.length;
  await harness!.service.submitInbound(inbound({ ...target, messageId: "native-ask-cancel", text: cancelForm.marker }));
  await wait("The cancellation case did not dispatch its native ask tool", () => emittedAskToolCalls.has(cancelForm.marker));
  const editorCancelCard = await waitForCard(cancelCardStart, "The cancel case editor did not reach the question station");
  assert.ok(cardInputIds(editorCancelCard.frame.card).text.length > 0, "The cancel case did not leave a text editor pending");
  const cancelInboundAt = firstTransport.sent.length;
  await harness!.service.submitInbound(inbound({ ...target, messageId: "native-ask-cancel-command", text: "/cancel" }));
  await wait("The native ask task did not become cancelled", () => task("native-ask-cancel")?.state === "cancelled");
  assert.equal((await harness!.service.status(target)).pendingQuestionIds.length, 0, "Cancelling the dialog left a pending question");
  await waitForWorkerCleanup();
  assert.equal(modelRequests.length, cancelRequestStart + 1, "The cancelled dialog unexpectedly returned an answer to the model");
  const cancelWire = await readAudit();
  assert.ok(cancelWire.some(row => row.event === "ui-request" && row.method === "cancel" && row.hasTarget), "OMP did not send its native dialog-cancel request");
  assert.ok(cancelWire.some(row => row.event === "ui-response" && row.cancelled), "The worker did not answer a cancelled dialog as cancelled");
  assert.ok(firstTransport.sent.slice(cancelInboundAt).some(message => /取消|cancel/i.test(message.text)), "The channel did not report the explicit task cancellation");

  const bindingBeforeRestart = binding();
  assert.deepEqual(bindingBeforeRestart, originalBinding, "Completed native ask work changed the persisted session binding");
  db!.close();
  db = undefined;
  await closeCoreHarness(harness!, { remove: false });
  harness = undefined;
  await createHarness();
  assert.strictEqual(transport, firstTransport, "The fake Octo server was replaced instead of surviving the service restart");
  assert.deepEqual(binding(), bindingBeforeRestart, "Controlled service restart changed the bound session identity");

  const restartTransport = transport!;
  const restartCardStart = restartTransport.sentCards.length;
  const restartRequestStart = modelRequests.length;
  const restartMessageId = "native-ask-after-restart";
  diagnosticMessageId = restartMessageId;
  await harness!.service.submitInbound(inbound({ ...target, messageId: restartMessageId, text: restartForm.marker }));
  await wait("The post-restart native ask did not reach the isolated model", () => emittedAskToolCalls.has(restartForm.marker));
  const routeCard = await waitForCard(restartCardStart, "The post-restart OMP select did not reach the CoreService question station");
  const uiRequestCountBeforeRouteAnswer = (await readAudit()).filter(row => row.event === "ui-request").length;
  await answerWithOption(routeCard, 2);
  await wait("The acknowledged route action did not answer the pending route question", () => {
    const currentTask = task(restartMessageId);
    return currentTask !== null && questionsForTask(currentTask.id)[0]?.state === "answered";
  }, 10_000);
  await waitForAudit("OMP did not request the native editor after the route answer", rows =>
    rows.filter(row => row.event === "ui-request").slice(uiRequestCountBeforeRouteAnswer)
      .find(row => row.method === "editor"),
    10_000,
  );
  const pendingEditorCard = await waitForCard(restartCardStart + 1, "The native editor did not become pending after the route answer");
  const interruptedTask = task(restartMessageId);
  assert.ok(interruptedTask, "The in-progress native ask task was not persisted");
  assert.equal(interruptedTask.prompt_accepted, 1, "The in-progress native ask prompt was not accepted");
  assert.equal(interruptedTask.session_id, originalBinding.session_id, "The in-progress native ask escaped its original session");
  const questionsBeforeRestart = questionsForTask(interruptedTask.id);
  assert.deepEqual(questionsBeforeRestart.map(({ state }) => state), ["answered", "pending"]);
  const pendingQuestion = questionsBeforeRestart[1]!;
  assert.equal(pendingQuestion.card_message_id, pendingEditorCard.messageId, "The pending editor was not attached to its original card");
  assert.deepEqual((await harness!.service.status(target)).pendingQuestionIds, [pendingQuestion.id]);
  const cardCountBeforeRecovery = restartTransport.sentCards.length;
  const uiRequestCountBeforeRestart = (await readAudit()).filter(row => row.event === "ui-request").length;

  db!.close();
  db = undefined;
  await closeCoreHarness(harness!, { remove: false });
  harness = undefined;
  await createHarness();
  assert.strictEqual(transport, restartTransport, "The fake Octo server was replaced during recovery");
  assert.deepEqual(binding(), bindingBeforeRestart, "Recovery changed the persisted session binding");
  const recoveredTransport = transport!;
  await waitForAudit("OMP did not restore the pending native editor request", rows =>
    rows.filter(row => row.event === "ui-request").slice(uiRequestCountBeforeRestart)
      .find(row => row.method === "editor"),
  );
  const recoveredTask = task(restartMessageId);
  assert.equal(recoveredTask?.id, interruptedTask.id, "Recovery did not resume the exact task");
  assert.equal(recoveredTask?.session_id, interruptedTask.session_id, "Recovery changed the task's OMP session");
  const questionsAfterRecovery = questionsForTask(interruptedTask.id);
  assert.deepEqual(questionsAfterRecovery, questionsBeforeRestart, "Recovery duplicated or replaced the pending question");
  assert.deepEqual((await harness!.service.status(target)).pendingQuestionIds, [pendingQuestion.id]);
  assert.equal(recoveredTransport.sentCards.length, cardCountBeforeRecovery, "Recovery sent a duplicate native ask card");

  await answerWithText(pendingEditorCard, "EDITOR_TEXT_LOCAL_73");
  await waitForTask(restartMessageId, restartForm.resultMarker);
  assert.ok(scriptResults.has(restartForm.marker), "Recovered editor answer and prior route choice did not reach the local model");
  assert.deepEqual(questionsForTask(interruptedTask.id).map(({ id, state }) => ({ id, state })), [
    { id: questionsBeforeRestart[0]!.id, state: "answered" },
    { id: pendingQuestion.id, state: "answered" },
  ], "Recovery did not finish the original question rows");
  await wait("Recovery did not edit the original question card", () =>
    recoveredTransport.cardEdits.some(({ messageId }) => messageId === pendingEditorCard.messageId),
  );
  assert.equal(recoveredTransport.sentCards.length, cardCountBeforeRecovery, "Recovery created another card instead of editing the original");
  assert.ok(modelRequests.length > restartRequestStart, "The recovered task never reached the finite model");
  await waitForWorkerCleanup();
  const finalAudit = await readAudit();
  const eventTrace = finalAudit.filter(row =>
    row.event === "tool-execution" || row.event === "ui-request" || row.event === "ui-response"
    || row.event === "message-end" || row.event === "agent-start" || row.event === "rpc-response",
  ).slice(-100);
  assert.ok(finalAudit.filter(row => row.event === "startup" && row.mode === "rpc-ui").length >= 3, "Subsequent and post-restart ordinary tasks did not restore rpc-ui mode");
  assert.ok(!finalAudit.some(row => row.event === "ui-request" && row.secret), "A secret input dialog was forwarded to the question station");
  console.log("CASE native-html-ask " + JSON.stringify({
    realOmp: true,
    fakeTransport: true,
    fullHtmlAsk: true,
    checkboxRoundTrips: checkboxFrames.slice(0, 3).map(row => row.checkedIndices ?? []),
    inputAndEditor: true,
    cancellation: true,
    controlledRestartSessionIdentity: true,
    midAskRecovery: true,
    localProviderRequests: modelRequests.length,
    eventTrace,
  }));
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  const auditRows = await readAudit();
  const lastStartup = [...auditRows].reverse().find(row => row.event === "startup" && row.processId !== undefined);
  const lastWorkerTrace = auditRows.filter(row =>
    row.processId === lastStartup?.processId
    && (row.event === "startup" || row.event === "tool-execution" || row.event === "ui-request" || row.event === "ui-response"
      || row.event === "message-end" || row.event === "agent-start" || row.event === "rpc-response"),
  ).slice(-40);
  const taskRow = db && diagnosticMessageId ? task(diagnosticMessageId) : null;
  const diagnostic = {
    taskMessageId: diagnosticMessageId ?? null,
    task: taskRow ? {
      state: taskRow.state,
      promptAccepted: taskRow.prompt_accepted,
      error: taskRow.error_text?.slice(0, 300) ?? null,
    } : null,
    questions: db && taskRow
      ? questionsForTask(taskRow.id).map(({ id, state, card_message_id }) => ({ id, state, cardMessageId: card_message_id }))
      : [],
    cards: transport ? {
      sentCount: transport.sentCards.length,
      uniqueMessageIdCount: new Set(transport.sentCards.map(card => card.messageId)).size,
      recentMessageIds: transport.sentCards.slice(-6).map(card => card.messageId),
      editCount: transport.cardEdits.length,
      recentEditedMessageIds: transport.cardEdits.slice(-4).map(edit => edit.messageId),
    } : null,
    cardActions: cardActionTrace.slice(-10),
    ackedBotEventIds: transport?.ackedEvents.slice(-10) ?? [],
    lastWorkerProcessId: lastStartup?.processId ?? null,
    lastWorkerTrace,
  };
  console.error(`Native ask acceptance failed: ${message.slice(0, 300)}; diagnostic=${JSON.stringify(diagnostic).slice(-6000)}`);
  process.exitCode = 1;
} finally {
  try {
    db?.close();
    if (harness) await closeCoreHarness(harness, { remove: false });
  } finally {
    try {
      modelServer?.stop(true);
    } finally {
      for (const [key, value] of originalEnvironment) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await rm(root, { recursive: true, force: true });
    }
  }
}
