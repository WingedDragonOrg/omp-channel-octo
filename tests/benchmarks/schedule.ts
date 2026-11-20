import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Opt-in acceptance: real installed OMP, two actual next-minute cron runs, and only loopback fixtures.
// The channel is a foreground `run` child using fixture credentials; no resident or production Bot is started.
const omp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(omp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-schedule-benchmark-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
fixture.userNames.set(fixture.ownerUid, "Fixture Owner");
const children: Bun.Subprocess[] = [];
const outputs: Promise<string>[] = [];
const auditPath = join(root, "octo-cli-audit.jsonl");
const configPath = join(root, "config.json");
const stateDir = join(root, "state");
const cwd = join(root, "cwd");
const agentDir = join(root, "agent");
const target = { channelType: 1 as const, channelId: fixture.ownerUid };
const sourceMessageBase = BigInt(Date.now()) * 1_000n + BigInt(Math.floor(Math.random() * 1000));
const scriptSourceMessageId = String(sourceMessageBase);
const stopSourceMessageId = String(sourceMessageBase + 1n);
const agentSourceMessageId = String(sourceMessageBase + 2n);
const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const scriptCreateMarker = `CREATE_SCRIPT_SCHEDULE_${suffix}`;
const scriptStopMarker = `STOP_SCRIPT_SCHEDULE_${suffix}`;
const agentCreateMarker = `CREATE_AGENT_SCHEDULE_${suffix}`;
const agentPromptMarker = `AGENT_SCHEDULE_ACCEPTANCE_${suffix}`;
const scriptName = `script-${suffix}`;
const agentName = `agent-${suffix}`;
const scriptArtifactPath = join(cwd, "script-output.txt");
const agentArtifactPath = join(cwd, "agent-output.txt");
const scriptArtifactContent = `SCRIPT_ARTIFACT_${suffix}`;
const agentArtifactContent = `AGENT_ARTIFACT_${suffix}`;
const scriptDelivery = `SCRIPT_DELIVERY_${suffix}`;
const agentDelivery = `AGENT_DELIVERY_${suffix}`;
const scriptClientMsgNo = `schedule-script-${suffix}`;
const agentClientMsgNo = `schedule-agent-${suffix}`;
const scriptCallId = `call_schedule_script_${suffix}`;
const scriptListCallId = `call_schedule_script_list_${suffix}`;
const agentListCallId = `call_schedule_agent_list_${suffix}`;
const agentCallId = `call_schedule_agent_${suffix}`;
const stopScriptCallId = `call_schedule_stop_${suffix}`;
const scriptManifestArtifactCallId = "script-manifest:0";
const agentArtifactCallId = `call_agent_artifact_${suffix}`;
const scriptArtifact = {
  name: "script-output.txt",
  type: "text/plain",
  path: scriptArtifactPath,
  target,
};
const agentArtifact = {
  name: "agent-output.txt",
  type: "text/plain",
  path: agentArtifactPath,
  target,
};
const scriptMessage = {
  channel_id: target.channelId,
  channel_type: target.channelType,
  payload: { type: 1, content: scriptDelivery },
  client_msg_no: scriptClientMsgNo,
};
const agentMessage = {
  channel_id: target.channelId,
  channel_type: target.channelType,
  payload: { type: 1, content: agentDelivery },
  client_msg_no: agentClientMsgNo,
};
const scriptBody = JSON.stringify(scriptMessage);
const agentBody = JSON.stringify(agentMessage);
const scriptText = `set -eu
test "$OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_TYPE" = ${shellQuote(String(target.channelType))}
test "$OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_ID" = ${shellQuote(target.channelId)}
printf '%s\\n' ${shellQuote(scriptArtifactContent)} > ${shellQuote(scriptArtifactPath)}
printf '%s' ${shellQuote(JSON.stringify({ artifacts: [scriptArtifact] }))} > "$OMP_CHANNEL_SCHEDULE_ARTIFACTS_FILE"
octo-cli api POST /v1/bot/sendMessage --data ${shellQuote(scriptBody)}
printf '%s\\n' SCRIPT_SCHEDULE_FINISHED
`;
const createScriptArgs = {
  action: "create",
  name: scriptName,
  cron: "* * * * *",
  timezone: "UTC",
  cwd,
  script: scriptText,
  target,
  timeoutSeconds: 90,
};
const createAgentArgs = {
  action: "create",
  name: agentName,
  cron: "* * * * *",
  timezone: "UTC",
  cwd,
  prompt: `${agentPromptMarker}\nWrite ${agentArtifactContent} to ${agentArtifactPath}, send ${agentDelivery} to ${JSON.stringify(target)} with the local octo-cli command, then register the artifact with schedule action=artifact. Do not claim the send succeeded unless the command succeeds.`,
  target,
  timeoutSeconds: 90,
};

type ModelStep =
  | { readonly name: string; readonly callId: string; readonly args: Record<string, unknown> }
  | { readonly text: string };
type PreviousCall = { name: string; args: Record<string, unknown> };
type ScheduleRow = {
  id: string;
  bot_uid: string;
  name: string;
  cron: string;
  timezone: string;
  cwd: string;
  spec_json: string;
  source_channel_type: number;
  source_channel_id: string;
  source_message_id: string;
  source_requester_uid: string | null;
  source_kind: string;
  source_task_id: string;
  tool_call_id: string;
  target_channel_type: number;
  target_channel_id: string;
  timeout_ms: number;
  created_at: number;
  status: string;
};
type RunRow = {
  id: string;
  schedule_id: string;
  bot_uid: string;
  scheduled_at: number;
  target_channel_type: number;
  target_channel_id: string;
  created_at: number;
  started_at: number | null;
  ended_at: number | null;
  status: string;
  stage: string;
  prompt_accepted: number;
  process_exited: number;
  session_id: string | null;
  session_file: string | null;
  result_text: string | null;
  error_text: string | null;
  stdout: string | null;
  stderr: string | null;
  artifacts_json: string;
};

let db: Database | undefined;
let model: Bun.Server<undefined> | undefined;
let modelError: string | undefined;
let connection: FixtureConnection;
let scriptScheduleId: string | undefined;
const modelRequests: Record<string, unknown>[] = [];

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function previousCalls(messages: unknown): PreviousCall[] {
  if (!Array.isArray(messages)) return [];
  const calls: PreviousCall[] = [];
  for (const message of messages) {
    const record = object(message);
    if (!Array.isArray(record?.["tool_calls"])) continue;
    for (const rawCall of record["tool_calls"]) {
      const call = object(rawCall);
      const fn = object(call?.["function"]);
      if (typeof fn?.["name"] !== "string") continue;
      const args = typeof fn["arguments"] === "string" ? JSON.parse(fn["arguments"]) as unknown : fn["arguments"];
      calls.push({ name: fn["name"], args: object(args) ?? {} });
    }
  }
  return calls;
}
function toolResult(messages: unknown, toolCallId: string): Record<string, unknown> {
  assert.ok(Array.isArray(messages), "Model conversation did not contain messages");
  let content: unknown;
  let found = false;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = object(messages[index]);
    if (message?.["role"] === "tool" && message["tool_call_id"] === toolCallId) {
      content = message["content"];
      found = true;
      break;
    }
  }
  assert.ok(found, `Model conversation omitted tool result ${toolCallId}`);
  assert.equal(typeof content, "string", `Tool result ${toolCallId} was not text`);
  const text = content as string;
  if (text.startsWith("schedule:")) {
    throw new Error(`Schedule tool ${toolCallId} returned error: ${text}`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`Tool result ${toolCallId} was not JSON: ${text}`);
  }
  const result = object(parsed);
  assert.ok(result, `Tool result ${toolCallId} was not a JSON object: ${text}`);
  return result;
}

function assertCreatedToolResult(messages: unknown, toolCallId: string, name: string): string {
  const result = toolResult(messages, toolCallId);
  assert.equal(result["kind"], "created");
  const schedule = object(result["schedule"]);
  assert.ok(schedule, "Create result did not contain its schedule record");
  assert.equal(schedule["name"], name);
  assert.equal(schedule["cron"], "* * * * *");
  assert.equal(schedule["timezone"], "UTC");
  assert.deepEqual(schedule["target"], target);
  assert.equal(typeof schedule["id"], "string");
  return schedule["id"] as string;
}

function assertListToolResult(
  messages: unknown,
  toolCallId: string,
  expected: readonly { id: string; name: string; status: string }[],
): void {
  const result = toolResult(messages, toolCallId);
  const scheduleValues = result["schedules"];
  assert.ok(Array.isArray(scheduleValues), "schedule list result did not contain schedules");
  const schedules: unknown[] = scheduleValues;
  assert.equal(schedules.length, expected.length, "schedule list returned an unexpected number of plans");
  for (const plan of expected) {
    const listed: Record<string, unknown> | undefined = schedules.map(object).find(
      (record: Record<string, unknown> | undefined) => record?.["id"] === plan.id,
    );
    assert.ok(listed, `Schedule list omitted ${plan.name}`);
    assert.equal(listed["name"], plan.name);
    assert.equal(listed["status"], plan.status);
  }
}

const createdScheduleIds = new Map<string, string>();

function recordCreatedSchedule(name: string, id: string): void {
  assert.equal(createdScheduleIds.has(name), false, `Schedule ${name} was created more than once`);
  createdScheduleIds.set(name, id);
}

function createdScheduleId(name: string): string {
  const id = createdScheduleIds.get(name);
  assert.ok(id, `Schedule ${name} creation result was not observed`);
  return id;
}


function toolNames(body: Record<string, unknown>): Set<string> {
  const names = new Set<string>();
  if (!Array.isArray(body["tools"])) return names;
  for (const rawTool of body["tools"]) {
    const fn = object(object(rawTool)?.["function"]);
    if (typeof fn?.["name"] === "string") names.add(fn["name"]);
  }
  return names;
}

function modelReply(step: ModelStep, index: number): Response {
  const delta = "text" in step
    ? { role: "assistant", content: step.text }
    : {
        role: "assistant",
        tool_calls: [{
          index: 0,
          id: step.callId,
          type: "function",
          function: { name: step.name, arguments: JSON.stringify(step.args) },
        }],
      };
  const chunks = [
    { id: `chatcmpl-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    { id: `chatcmpl-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "text" in step ? "stop" : "tool_calls" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}

function send(text: string, messageId: string): void {
  fixture.sendInbound(connection, {
    fromUid: fixture.ownerUid,
    channelId: `${fixture.ownerUid}@${fixture.botUid}`,
    channelType: 1,
    text,
    messageId,
  });
}

function task(messageId: string): { id: string; state: string } | null {
  return db!.query("SELECT id, state FROM tasks WHERE message_id = ?").get(messageId) as { id: string; state: string } | null;
}

function schedule(name: string): ScheduleRow | null {
  return db!.query("SELECT * FROM schedules WHERE name = ?").get(name) as ScheduleRow | null;
}

function runs(scheduleId: string): RunRow[] {
  return db!.query("SELECT * FROM schedule_runs WHERE schedule_id = ? ORDER BY scheduled_at, id").all(scheduleId) as RunRow[];
}

function successfulRun(scheduleId: string): RunRow | undefined {
  return runs(scheduleId).find(run => run.status === "success" && run.stage === "settled");
}

function assertScheduleOrigin(
  record: ScheduleRow,
  sourceMessageId: string,
  sourceTaskId: string,
  toolCallId: string,
): void {
  assert.equal(record.bot_uid, fixture.botUid);
  assert.equal(record.cron, "* * * * *");
  assert.equal(record.timezone, "UTC");
  assert.equal(record.cwd, cwd);
  assert.equal(record.source_channel_type, 1);
  assert.equal(record.source_channel_id, fixture.ownerUid);
  assert.equal(record.source_message_id, sourceMessageId);
  assert.equal(record.source_requester_uid, fixture.ownerUid);
  assert.equal(record.source_kind, "direct");
  assert.equal(record.source_task_id, sourceTaskId);
  assert.equal(record.tool_call_id, toolCallId);
  assert.equal(record.target_channel_type, target.channelType);
  assert.equal(record.target_channel_id, target.channelId);
  assert.equal(record.status, "active");
}

function assertSuccessfulMinuteRun(record: ScheduleRow, run: RunRow): void {
  assert.equal(run.schedule_id, record.id);
  assert.equal(run.bot_uid, fixture.botUid);
  assert.equal(run.status, "success");
  assert.equal(run.stage, "settled");
  assert.equal(run.target_channel_type, target.channelType);
  assert.equal(run.target_channel_id, target.channelId);
  assert.ok(run.started_at !== null && run.ended_at !== null && run.ended_at >= run.started_at);
  assert.ok(run.started_at !== null && run.started_at >= run.scheduled_at);
  assert.ok(run.scheduled_at > record.created_at, "Schedule fired before its next full minute");
  assert.equal(run.scheduled_at % 60_000, 0, "Occurrence was not a real UTC minute boundary");
  assert.equal(runs(record.id).filter(other => other.scheduled_at === run.scheduled_at).length, 1, "Occurrence was persisted more than once");
}

async function wait(description: string, predicate: () => boolean, timeoutMs = 180_000): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined, modelError);
    return predicate() ? true : undefined;
  }, timeoutMs, description);
}

async function exit(child: Bun.Subprocess): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("Schedule CLI did not exit")), 15_000);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

async function launch(): Promise<Bun.Subprocess> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: root,
      PATH: `${join(root, "bin")}:/usr/bin:/bin`,
      PI_CODING_AGENT_DIR: agentDir,
      PI_NO_TITLE: "1",
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(children.length, 15_000);
  await wait("Schedule CLI failed to authenticate", () => connection.ready, 15_000);
  return child;
}

function dbSummary(): Record<string, unknown> {
  if (!db) return {};
  return {
    schedules: db.query("SELECT id, name, source_channel_type, source_channel_id, source_message_id, source_requester_uid, source_kind, source_task_id, tool_call_id, target_channel_type, target_channel_id, status FROM schedules ORDER BY created_at").all(),
    runs: db.query("SELECT id, schedule_id, scheduled_at, target_channel_type, target_channel_id, started_at, ended_at, status, stage, prompt_accepted, process_exited, session_id, session_file, result_text, error_text, stdout, stderr, artifacts_json FROM schedule_runs ORDER BY scheduled_at").all(),
  };
}

try {
  assert.ok(fixture.apiUrl.startsWith("http://127.0.0.1:"), "Octo fixture must stay on loopback");
  await mkdir(join(root, "bin"));
  await mkdir(agentDir);
  await mkdir(cwd);
  await writeFile(join(root, "bin", "super"), "#!/bin/sh\nexit 127\n", { mode: 0o755 });

  // The fake CLI performs the documented bot API request against the Octo fixture only.
  // Its private token is received over auth-login stdin and is never written to the audit log.
  const fakeCli = `#!${process.execPath}
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
const args = process.argv.slice(2);
const configDir = process.env.OCTO_CONFIG_DIR;
const botId = process.env.OCTO_BOT_ID;
if (args[0] === "auth" && args[1] === "login") {
  // The credential provisioner passes the bot identity via --bot-id, not OCTO_BOT_ID, for login.
  let token = "";
  for await (const bytes of Bun.stdin.stream()) token += new TextDecoder().decode(bytes);
  const loginBotId = args[5];
  if (!configDir
    || args.length !== 7
    || args[2] !== "--profile"
    || args[3] !== "oco"
    || args[4] !== "--bot-id"
    || !loginBotId
    || args[6] !== "--with-token"
    || token.length === 0) {
    console.error("invalid fixture auth login argv or token stdin");
    process.exit(2);
  }
  writeFileSync(join(configDir, "fixture-token"), token, { mode: 0o600 });
  console.log(JSON.stringify({ ok: true, data: { robot_id: loginBotId } }));
}
else {
  if (args[0] !== "api" || args[1] !== "POST" || args[2] !== "/v1/bot/sendMessage") {
    console.error("unsupported fixture octo-cli command");
    process.exit(2);
  }
const dataIndex = args.indexOf("--data");
const apiBaseUrl = process.env.OCTO_API_BASE_URL;
if (!configDir || !botId || !apiBaseUrl || dataIndex < 0 || !args[dataIndex + 1]) process.exit(2);
const token = readFileSync(join(configDir, "fixture-token"), "utf8");
const body = JSON.parse(args[dataIndex + 1]);
const response = await fetch(new URL(args[2], apiBaseUrl), {
  method: "POST",
  headers: { authorization: \`Bearer \${token}\`, "content-type": "application/json" },
  body: JSON.stringify(body),
});
const responseText = await response.text();
appendFileSync(${JSON.stringify(auditPath)}, JSON.stringify({
  botId,
  target: { channelType: body.channel_type, channelId: body.channel_id },
  payload: body.payload,
  clientMsgNo: body.client_msg_no,
  status: response.status,
}) + "\\n");
if (!response.ok) {
  console.error(responseText);
  process.exit(1);
}
console.log(responseText);
}
`;
  await writeFile(join(root, "bin", "octo-cli"), fakeCli, { mode: 0o755 });

  const ompWrapper = join(root, "real-omp.ts");
  await writeFile(ompWrapper, `#!${process.execPath}
const child = Bun.spawn([${JSON.stringify(omp)}, ...process.argv.slice(2)], {
  stdin: "pipe", stdout: "inherit", stderr: "inherit", env: process.env,
});
let buffer = "";
const decoder = new TextDecoder();
for await (const bytes of Bun.stdin.stream()) {
  buffer += decoder.decode(bytes, { stream: true });
  let newline;
  while ((newline = buffer.indexOf("\\n")) >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    child.stdin.write(line + "\\n");
  }
}
child.stdin.end();
process.exit(await child.exited);
`, { mode: 0o755 });

  const provider = {
    apiKey: "local-test-placeholder",
    api: "openai-completions",
    models: [{ id: "probe", name: "Probe", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  };
  const localModel = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      try {
        const body = await request.json() as Record<string, unknown>;
        modelRequests.push(body);
        const messages = Array.isArray(body["messages"]) ? body["messages"] : [];
        let latestUserIndex = -1;
        for (let index = 0; index < messages.length; index += 1) {
          if (object(messages[index])?.["role"] === "user") latestUserIndex = index;
        }
        const turnMessages = messages.slice(latestUserIndex < 0 ? 0 : latestUserIndex);
        const userText = JSON.stringify(turnMessages[0] ?? {});
        const calls = previousCalls(turnMessages);
        const names = toolNames(body);
        let step: ModelStep;
        if (userText.includes(scriptCreateMarker)) {
          assert.ok(names.has("schedule"), "Source task has no schedule host tool");
          const creates = calls.filter(call => call.name === "schedule" && call.args["action"] === "create").length;
          const lists = calls.filter(call => call.name === "schedule" && call.args["action"] === "list").length;
          if (creates === 0 && lists === 0) step = { name: "schedule", callId: scriptCallId, args: createScriptArgs };
          else if (creates === 1 && lists === 0) {
            const id = assertCreatedToolResult(turnMessages, scriptCallId, scriptName);
            recordCreatedSchedule(scriptName, id);
            step = { name: "schedule", callId: scriptListCallId, args: { action: "list" } };
          } else if (creates === 1 && lists === 1) {
            assertListToolResult(turnMessages, scriptListCallId, [{
              id: createdScheduleId(scriptName),
              name: scriptName,
              status: "active",
            }]);
            step = { text: `SCRIPT_SCHEDULE_CREATED_${suffix}` };
          } else throw new Error(`Unexpected script schedule tool-call counts: create=${creates}, list=${lists}`);
        } else if (userText.includes(scriptStopMarker)) {
          assert.ok(names.has("schedule"), "Stop task has no schedule host tool");
          const stops = calls.filter(call => call.name === "schedule" && call.args["action"] === "stop").length;
          if (stops === 0) {
            assert.ok(scriptScheduleId, "Script schedule id was not available for stop");
            step = {
              name: "schedule",
              callId: stopScriptCallId,
              args: { action: "stop", schedule_id: scriptScheduleId },
            };
          } else if (stops === 1) step = { text: `SCRIPT_SCHEDULE_STOPPED_${suffix}` };
          else throw new Error(`Unexpected script schedule stop tool-call count: ${stops}`);
        } else if (userText.includes(agentCreateMarker)) {
          assert.ok(names.has("schedule"), "Agent-plan source task has no schedule host tool");
          const creates = calls.filter(call => call.name === "schedule" && call.args["action"] === "create").length;
          const lists = calls.filter(call => call.name === "schedule" && call.args["action"] === "list").length;
          if (creates === 0 && lists === 0) step = { name: "schedule", callId: agentCallId, args: createAgentArgs };
          else if (creates === 1 && lists === 0) {
            const id = assertCreatedToolResult(turnMessages, agentCallId, agentName);
            recordCreatedSchedule(agentName, id);
            step = { name: "schedule", callId: agentListCallId, args: { action: "list" } };
          } else if (creates === 1 && lists === 1) {
            assert.ok(scriptScheduleId, "Script schedule id was not available for list verification");
            assertListToolResult(turnMessages, agentListCallId, [
              { id: scriptScheduleId, name: scriptName, status: "stopped" },
              { id: createdScheduleId(agentName), name: agentName, status: "active" },
            ]);
            step = { text: `AGENT_SCHEDULE_CREATED_${suffix}` };
          } else throw new Error(`Unexpected agent schedule tool-call counts: create=${creates}, list=${lists}`);
        } else if (userText.includes(agentPromptMarker)) {
          assert.ok(names.has("schedule"), "Scheduled agent has no schedule artifact tool");
          assert.ok(names.has("bash"), "Scheduled agent has no real OMP bash builtin");
          const bashCalls = calls.filter(call => call.name === "bash");
          const artifactCalls = calls.filter(call => call.name === "schedule" && call.args["action"] === "artifact");
          if (bashCalls.length === 0 && artifactCalls.length === 0) {
            const command = `set -eu\nprintf '%s\\n' ${shellQuote(agentArtifactContent)} > ${shellQuote(agentArtifactPath)}\nocto-cli api POST /v1/bot/sendMessage --data ${shellQuote(agentBody)}\nprintf '%s\\n' AGENT_SCHEDULE_SEND_FINISHED\n`;
            step = { name: "bash", callId: `call_agent_bash_${suffix}`, args: { command } };
          } else if (bashCalls.length === 1 && artifactCalls.length === 0) {
            step = {
              name: "schedule",
              callId: agentArtifactCallId,
              args: { action: "artifact", ...agentArtifact },
            };
          } else if (bashCalls.length === 1 && artifactCalls.length === 1) {
            step = { text: `AGENT_SCHEDULE_FINISHED_${suffix}` };
          } else {
            throw new Error(`Unexpected scheduled-agent tool-call history: bash=${bashCalls.length}, artifact=${artifactCalls.length}`);
          }
        } else {
          throw new Error("Local provider received an unrelated or unrecognized prompt");
        }
        return modelReply(step, modelRequests.length);
      } catch (error) {
        modelError = error instanceof Error ? error.message : String(error);
        return new Response(modelError, { status: 400 });
      }
    },
  });
  model = localModel;
  const modelProviders = {
    "local-schedule": { ...provider, baseUrl: `http://127.0.0.1:${localModel.port}/v1` },
  };
  await writeFile(join(agentDir, "models.yml"), JSON.stringify({ providers: modelProviders }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [],
    stateDir,
    sessionsDir: join(root, "sessions"),
    ompCommand: ompWrapper,
    ompExtraArgs: ["--model", "local-schedule/probe", "--tools", "bash", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--config", join(root, "native.yml")],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  const child = await launch();
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  send(`Create the script plan with ${scriptCreateMarker}; run it every minute in UTC and target this owner DM.`, scriptSourceMessageId);
  await wait("Script-plan source task did not finish", () => task(scriptSourceMessageId)?.state === "completed");
  const scriptSourceTask = task(scriptSourceMessageId);
  assert.ok(scriptSourceTask, "Script-plan source task disappeared");
  const scriptSchedule = schedule(scriptName);
  assert.ok(scriptSchedule, "Real schedule tool did not persist the script plan");
  assert.equal(scriptSchedule.id, createdScheduleId(scriptName), "Create tool result did not identify its persisted plan");
  assertScheduleOrigin(scriptSchedule, scriptSourceMessageId, scriptSourceTask.id, scriptCallId);
  assert.deepEqual(JSON.parse(scriptSchedule.spec_json), { kind: "script", runner: { kind: "shell", script: scriptText } });
  assert.equal(schedule(agentName), null, "Agent plan existed before its sequential creation");

  await wait("Script cron did not complete a real minute occurrence", () => successfulRun(scriptSchedule.id) !== undefined);
  const scriptRun = successfulRun(scriptSchedule.id)!;
  assertSuccessfulMinuteRun(scriptSchedule, scriptRun);
  assert.equal(scriptRun.process_exited, 1);
  assert.ok(scriptRun.stdout?.includes("SCRIPT_SCHEDULE_FINISHED"), "Script run did not retain its stdout result");
  assert.equal(await readFile(scriptArtifactPath, "utf8"), `${scriptArtifactContent}\n`);
  const savedScriptArtifacts = JSON.parse(scriptRun.artifacts_json) as unknown[];
  assert.deepEqual(savedScriptArtifacts, [{ ...scriptArtifact, toolCallId: scriptManifestArtifactCallId }]);

  scriptScheduleId = scriptSchedule.id;
  send(`Stop the script schedule ${scriptSchedule.id} ${scriptStopMarker}.`, stopSourceMessageId);
  await wait("Script schedule stop task did not finish", () => task(stopSourceMessageId)?.state === "completed");
  assert.ok(task(stopSourceMessageId), "Script schedule stop task disappeared");
  await wait("Script plan did not persist its stopped state", () => schedule(scriptName)?.status === "stopped");
  assert.equal(successfulRun(scriptSchedule.id)?.id, scriptRun.id, "Stopping the plan discarded its completed run");

  send(`Create the agent plan with ${agentCreateMarker}; run it every minute in UTC and target this owner DM.`, agentSourceMessageId);
  await wait("Agent-plan source task did not finish", () => task(agentSourceMessageId)?.state === "completed");
  const agentSourceTask = task(agentSourceMessageId);
  assert.ok(agentSourceTask, "Agent-plan source task disappeared");
  const agentSchedule = schedule(agentName);
  assert.ok(agentSchedule, "Real schedule tool did not persist the agent plan");
  assert.equal(agentSchedule.id, createdScheduleId(agentName), "Create tool result did not identify its persisted plan");
  assertScheduleOrigin(agentSchedule, agentSourceMessageId, agentSourceTask.id, agentCallId);
  assert.deepEqual(JSON.parse(agentSchedule.spec_json), { kind: "agent", prompt: createAgentArgs.prompt });
  assert.equal(schedule(scriptName)?.status, "stopped", "Agent plan was created before the script plan stopped");

  await wait("Agent cron did not complete a real minute occurrence", () => successfulRun(agentSchedule.id) !== undefined);
  const agentRun = successfulRun(agentSchedule.id)!;
  assertSuccessfulMinuteRun(agentSchedule, agentRun);
  assert.notEqual(agentRun.scheduled_at, scriptRun.scheduled_at, "Sequential plan types did not trigger in different actual minutes");
  assert.equal(agentRun.prompt_accepted, 1);
  assert.ok(agentRun.session_id, "Agent run did not persist its independent OMP session id");
  assert.ok(agentRun.session_file, "Agent run did not persist its independent OMP session file");
  assert.ok(agentRun.session_file.startsWith(`${join(root, "sessions")}${sep}`), "Agent session escaped the temporary session root");
  assert.ok(existsSync(agentRun.session_file), "Agent session file was not produced");
  assert.ok(agentRun.result_text?.includes(`AGENT_SCHEDULE_FINISHED_${suffix}`), "Agent run result was not retained");
  assert.equal(await readFile(agentArtifactPath, "utf8"), `${agentArtifactContent}\n`);
  const savedAgentArtifacts = JSON.parse(agentRun.artifacts_json) as unknown[];
  assert.deepEqual(savedAgentArtifacts, [{ ...agentArtifact, toolCallId: agentArtifactCallId }]);
  const sourceBinding = db.query("SELECT session_id, session_file FROM chats WHERE channel_type = 1 LIMIT 1").get() as { session_id: string | null; session_file: string | null } | null;
  assert.ok(sourceBinding?.session_id && sourceBinding.session_file, "Direct source chat did not retain its own session");
  assert.notEqual(agentRun.session_id, sourceBinding.session_id, "Scheduled agent reused the source chat session");
  assert.notEqual(agentRun.session_file, sourceBinding.session_file, "Scheduled agent reused the source chat session file");

  const auditRows = (await readFile(auditPath, "utf8")).trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>);
  const scriptCliSends = auditRows.filter(row => row["clientMsgNo"] === scriptClientMsgNo);
  const agentCliSends = auditRows.filter(row => row["clientMsgNo"] === agentClientMsgNo);
  const cliSends = [...scriptCliSends, ...agentCliSends];
  assert.ok(scriptCliSends.length > 0, "Script execution did not send through the local octo-cli fixture");
  assert.ok(agentCliSends.length > 0, "Agent execution did not send through the local octo-cli fixture");
  for (const row of cliSends) {
    assert.equal(row["botId"], fixture.botUid);
    assert.equal(row["status"], 200);
    assert.deepEqual(row["target"], target);
  }
  const fixtureSends = fixture.sentMessages.filter(message => {
    const body = message.body;
    return body["client_msg_no"] === scriptClientMsgNo || body["client_msg_no"] === agentClientMsgNo;
  });
  assert.ok(fixtureSends.length >= 2, "Fake octo-cli did not reach the local Octo fixture for both plans");
  assert.ok(fixtureSends.every(message => message.authorization === `Bearer ${fixture.botToken}`));
  assert.deepEqual(fixtureSends.map(message => message.body["channel_id"]).sort(), Array.from({ length: fixtureSends.length }, () => target.channelId));
  assert.deepEqual(fixtureSends.map(message => message.body["channel_type"]).sort(), Array.from({ length: fixtureSends.length }, () => target.channelType));
  const deliveredContent = fixtureSends.map(message => object(message.body["payload"])?.["content"]);
  assert.ok(deliveredContent.includes(scriptDelivery), "Script delivery did not reach its registered target");
  assert.ok(deliveredContent.includes(agentDelivery), "Agent delivery did not reach its registered target");
  assert.ok(modelRequests.length >= 11, "Finite localhost model did not execute create/list, stop, and both scheduled runs");
  assert.equal(modelError, undefined);

  child.kill("SIGTERM");
  assert.equal(await exit(child), 0);
  db!.close();
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  assert.equal(successfulRun(scriptSchedule.id)?.id, scriptRun.id, "Script run record was not durable after service shutdown");
  assert.equal(successfulRun(agentSchedule.id)?.id, agentRun.id, "Agent run record was not durable after service shutdown");
  assert.equal(schedule(scriptName)?.source_task_id, scriptSourceTask.id, "Script source record was not durable after service shutdown");
  assert.equal(schedule(agentName)?.source_task_id, agentSourceTask.id, "Agent source record was not durable after service shutdown");
  console.log(JSON.stringify({
    realOmp: omp,
    localModelRequests: modelRequests.length,
    realMinuteCronOccurrences: [scriptRun.scheduled_at, agentRun.scheduled_at],
    scheduleIds: [scriptSchedule.id, agentSchedule.id],
    runIds: [scriptRun.id, agentRun.id],
    sourceTaskIds: [scriptSourceTask.id, agentSourceTask.id],
    sourceMessageIds: [scriptSourceMessageId, agentSourceMessageId],
    sourceToolCallIds: [scriptCallId, agentCallId],
    sourceRequesterUid: fixture.ownerUid,
    target,
    persistedArtifacts: [savedScriptArtifacts, savedAgentArtifacts],
    localCliDeliveries: cliSends.length,
  }));
} catch (error) {
  console.error(JSON.stringify({
    modelRequests: modelRequests.length,
    modelError,
    sentMessages: fixture.sentMessages.slice(-8).map(message => ({ body: message.body })),
    database: dbSummary(),
  }));
  for (const child of children) if (child.exitCode === null) child.kill("SIGTERM");
  await Promise.allSettled(children.map(child => exit(child)));
  const logs = (await Promise.all(outputs)).join("\n").replaceAll(fixture.botToken, "[fixture-token]");
  console.error(logs.slice(-12_000));
  throw error;
} finally {
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await Promise.allSettled(children.map(child => child.exited));
  db?.close();
  await fixture.close();
  model?.stop(true);
  await rm(root, { recursive: true, force: true });
}
