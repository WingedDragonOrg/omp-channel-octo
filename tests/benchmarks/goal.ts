import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Opt-in real-OMP acceptance: only loopback Octo/model servers and temporary profiles.
// A protocol-only worker cannot pass: the model must receive the native goal tool,
// two native runs must finish, and pause/restart must preserve the journaled goal.
const omp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(omp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-native-goal-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const children: Bun.Subprocess[] = [];
const outputs: Promise<string>[] = [];
const requests: Record<string, unknown>[] = [];
const wirePath = join(root, "wire.jsonl");
const checkout = resolve(import.meta.dir, "../..");
type Step = { op: "get" | "complete" } | { text: string; hold?: Promise<void> };
let steps: Step[] = [];
let modelError: string | undefined;
let connection: FixtureConnection;
let db: Database | undefined;
let sequence = 1000;

function modelReply(step: Step, index: number): Response {
  const delta = "op" in step
    ? { role: "assistant", tool_calls: [{ index: 0, id: `goal-call-${index}`, type: "function", function: { name: "goal", arguments: JSON.stringify({ op: step.op }) } }] }
    : { role: "assistant", content: step.text };
  const chunks = [
    { id: `chatcmpl-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    { id: `chatcmpl-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "op" in step ? "tool_calls" : "stop" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } });
}
const model = Bun.serve({
  hostname: "127.0.0.1", port: 0,
  async fetch(request) {
    const body = await request.json() as Record<string, unknown>;
    requests.push(body);
    const step = steps.shift();
    if (!step) {
      modelError = "Native OMP exceeded the finite model script";
      return new Response(modelError, { status: 400 });
    }
    const tools = body.tools as { function: { name: string } }[] | undefined;
    if (!tools?.some(tool => tool.function.name === "goal")) {
      modelError = "Request has no native goal tool";
      return new Response(modelError, { status: 400 });
    }
    if ("text" in step && step.hold) await step.hold;
    return modelReply(step, requests.length);
  },
});

function sent(): string[] {
  return fixture.sentMessages.map(message => (message.body.payload as { content?: string } | undefined)?.content ?? "");
}
function send(text: string): string {
  const id = String(++sequence);
  fixture.sendInbound(connection, { fromUid: fixture.ownerUid, channelId: `${fixture.ownerUid}@${fixture.botUid}`, channelType: 1, text, messageId: id });
  return id;
}
function taskState(id: string): string | undefined {
  return (db!.query("SELECT state FROM tasks WHERE message_id = ?").get(id) as { state: string } | null)?.state;
}
async function wait(description: string, predicate: () => boolean): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined);
    return predicate() ? true : undefined;
  }, 45_000, description);
}
async function waitForWorkerCleanup(): Promise<void> {
  await wait("A settled task retained its native worker", () => {
    const row = db!.query("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get() as { count: number };
    return row.count === 0;
  });
}
async function exit(child: Bun.Subprocess): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("Goal CLI did not exit")), 15_000);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally { clearTimeout(timer); }
}
async function launch(): Promise<Bun.Subprocess> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", join(root, "config.json")], {
    cwd: root, env: { HOME: root, TMPDIR: root, PATH: `${join(root, "bin")}:/usr/bin:/bin`, PI_CODING_AGENT_DIR: join(root, "agent"), PI_NO_TITLE: "1", LANG: "C", TZ: "UTC" },
    stdin: "ignore", stdout: "pipe", stderr: "pipe",
  });
  children.push(child);
  outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(children.length, 15_000);
  await wait("CLI failed to authenticate", () => connection.ready);
  return child;
}
async function nativeWire(): Promise<{ type: string; op?: string; message?: string }[]> {
  return (await readFile(wirePath, "utf8")).trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
}
function completeScript(prefix: string): Step[] {
  return [{ op: "get" }, { text: `${prefix}_FIRST` }, { op: "complete" }, { text: `${prefix}_DONE` }];
}
async function showIncludes(fragment: RegExp): Promise<void> {
  const start = fixture.sentMessages.length;
  send("/goal show");
  await wait(`Goal show did not report ${fragment}`, () => sent().slice(start).some(text => fragment.test(text)));
}

try {
  await mkdir(join(root, "bin"));
  await mkdir(join(root, "agent"));
  await mkdir(join(root, "cwd"));
  await writeFile(wirePath, "");
  for (const command of ["octo-cli", "super"]) await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  // Transparent audit shim; the actual installed OMP handles every protocol frame.
  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, `#!${process.execPath}\nimport {appendFileSync} from 'node:fs';\nconst child=Bun.spawn([${JSON.stringify(omp)},...process.argv.slice(2)],{stdin:'pipe',stdout:'inherit',stderr:'inherit',env:process.env});\nlet buffer=''; const decoder=new TextDecoder();\nfor await (const bytes of Bun.stdin.stream()){buffer+=decoder.decode(bytes,{stream:true});let n;while((n=buffer.indexOf('\\n'))>=0){const line=buffer.slice(0,n);buffer=buffer.slice(n+1);if(line.trim()){const f=JSON.parse(line);appendFileSync(${JSON.stringify(wirePath)},JSON.stringify({type:f.type,op:f.op,message:f.message})+'\\n');}child.stdin.write(line+'\\n');}}\nchild.stdin.end();process.exit(await child.exited);\n`, { mode: 0o755 });
  const initialProvider = { baseUrl: `http://127.0.0.1:${model.port}/v1`, apiKey: "local-test-placeholder", api: "openai-completions", models: [{ id: "probe", name: "Probe", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] };
  const modelProviders: Record<string, typeof initialProvider> = {
    "local-goal": initialProvider,
    "local-next": { ...initialProvider, models: [{ ...initialProvider.models[0]!, id: "replacement" }] },
  };
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: modelProviders }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
  await writeFile(join(root, "config.json"), JSON.stringify({
    apiUrl: fixture.apiUrl, botToken: fixture.botToken, ownerId: fixture.ownerUid, allowedGroupIds: [], stateDir: join(root, "state"), sessionsDir: join(root, "sessions"),
    ompCommand: wrapper, ompExtraArgs: ["--model", "local-goal/probe", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools", "--config", join(root, "native.yml")],
    startupTimeoutMs: 30_000, cancelTimeoutMs: 3_000, groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  let child = await launch();
  db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
  const newAt = fixture.sentMessages.length;
  send(`/new ${join(root, "cwd")}`);
  await wait("Fresh native session was not bound", () => sent().slice(newAt).some(text => text.includes("已创建会话")));

  steps = completeScript("COMPLETE");
  const completeTask = send("/goal set --budget 2000 finite-native-completion");
  await wait("Goal did not complete across both native turns", () => taskState(completeTask) === "completed" && sent().some(text => text.includes("COMPLETE_DONE")));
  assert.ok(sent().some(text => text.includes("COMPLETE_FIRST")));
  assert.equal(requests.length, 4);
  await waitForWorkerCleanup();
  await showIncludes(/(?:没有|无).*目标/);

  const hold = Promise.withResolvers<void>();
  const beforePause = requests.length;
  steps = [{ op: "get" }, { text: "PAUSE_CURRENT_FINISHED", hold: hold.promise }];
  const pausedTask = send("/goal set --budget 1000 finite-native-paused");
  await wait("Goal never reached its in-flight turn", () => requests.length === beforePause + 2);
  await showIncludes(/finite-native-paused/);
  await showIncludes(/tokenBudget=1000/);
  const pauseAt = fixture.sentMessages.length;
  send("/goal pause");
  await wait("Pause control queued behind goal", () => sent().slice(pauseAt).some(text => /暂停|paused/.test(text)));
  hold.resolve();
  await wait("Paused goal did not release its task", () => taskState(pausedTask) === "completed" || taskState(pausedTask) === "interrupted");
  assert.equal(requests.length, beforePause + 2);
  await waitForWorkerCleanup();
  await showIncludes(/status=paused/);
  const switchAt = fixture.sentMessages.length;
  send("/switch local-next/replacement");
  await wait("Chat model selection did not resolve", () => sent().slice(switchAt).some(text => text.includes("local-next/replacement")));
  delete modelProviders["local-goal"];
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: modelProviders }));

  steps = completeScript("RESUME");
  const resumeTask = send("/goal resume");
  await wait("Paused native goal did not resume and complete", () => taskState(resumeTask) === "completed" && sent().some(text => text.includes("RESUME_DONE")));
  assert.equal(requests.length, beforePause + 6);
  assert.equal(requests[beforePause + 2]?.model, "replacement", "Goal ignored the chat model selection");

  for (const mode of ["graceful", "crash"] as const) {
    const checkpoint = Promise.withResolvers<void>();
    steps = [{ op: "get" }, { text: `${mode}_INTERRUPTED`, hold: checkpoint.promise }];
    const requestBase = requests.length;
    const nativeCreateBase = (await nativeWire()).filter(row => row.type === "goal" && row.op === "create").length;
    const id = send(`/goal set --budget 2000 native-${mode}-recovery`);
    await wait("Goal checkpoint was not reached", () => requests.length === requestBase + 2 && taskState(id) === "started");
    const binding = db!.query("SELECT session_id, session_file FROM chats WHERE channel_type = 1").get();
    child.kill(mode === "crash" ? "SIGKILL" : "SIGTERM");
    const code = await exit(child);
    if (mode === "graceful") assert.equal(code, 0);
    checkpoint.resolve();
    steps = [{ op: "complete" }, { text: `${mode}_RECOVERED_DONE` }];
    child = await launch();
    await wait("Accepted native goal failed to recover", () => taskState(id) === "completed" && sent().some(text => text.includes(`${mode}_RECOVERED_DONE`)));
    assert.deepEqual(db!.query("SELECT session_id, session_file FROM chats WHERE channel_type = 1").get(), binding);
    const wire = await nativeWire();
    assert.equal(wire.filter(row => row.type === "goal" && row.op === "create").length, nativeCreateBase + 1, "Restart created the goal twice");
    assert.ok(wire.some(row => row.type === "prompt" && row.message === "/retry"), "Recovery never consulted native retry");
  }

  const beforeBudget = requests.length;
  steps = [{ op: "get" }, { text: "BUDGET_LIMIT_REACHED" }];
  const budgetTask = send("/goal set --budget 1 finite-native-budget-limit");
  await wait("Native token budget did not settle its run", () => taskState(budgetTask) === "completed");
  await waitForWorkerCleanup();
  await showIncludes(/status=budget-limited/);
  assert.equal(requests.length, beforeBudget + 2);
  const budgetResumeAt = fixture.sentMessages.length;
  send("/goal resume");
  await wait("Budget-limited goal was not refused resume", () => sent().slice(budgetResumeAt).some(text => /预算|budget-limited/.test(text)));
  assert.equal(requests.length, beforeBudget + 2, "Resume bypassed native token budget");
  const budgetDropAt = fixture.sentMessages.length;
  send("/goal drop");
  await wait("Budget-limited goal was not dropped", () => sent().slice(budgetDropAt).some(text => /已结算|dropped|(?:没有|无).*目标/.test(text)));

  steps = [{ op: "get" }, { text: "DROP_PREPARED" }, { text: "DROP_WAITING" }];
  const dropTask = send("/goal set finite-native-drop");
  await wait("No-progress goal failed to settle", () => taskState(dropTask) === "completed");
  await waitForWorkerCleanup();
  const dropAt = fixture.sentMessages.length;
  send("/goal drop");
  await wait("Native goal was not dropped", () => sent().slice(dropAt).some(text => /已结算|dropped|(?:没有|无).*目标/.test(text)));
  await showIncludes(/(?:没有|无).*目标/);
  const beforeDisabled = requests.length;
  await writeFile(join(root, "native.yml"), "goal:\n  enabled: false\nautoRetry:\n  enabled: false\n");
  const disabledAt = fixture.sentMessages.length;
  const disabledTask = send("/goal set disabled-native-goal");
  await wait("Disabled native goal was not refused", () => taskState(disabledTask) === "failed" && sent().slice(disabledAt).some(text => /disabled|未启用|已禁用/.test(text)));
  assert.equal(requests.length, beforeDisabled, "Disabled goal called the model");
  child.kill("SIGTERM");
  assert.equal(await exit(child), 0);
  assert.equal(steps.length, 0);
  assert.equal(modelError, undefined);
  console.log(JSON.stringify({ realOmp: omp, finiteCompletionTurns: 2, pauseAndResume: true, drop: true, gracefulGoalRecovery: true, crashGoalRecovery: true, disabledGoalRefused: true, modelRequests: requests.length, duplicateCreates: 0 }));
} catch (error) {
  // Output is isolated fixture data; redact its token even on acceptance failures.
  console.error(JSON.stringify({ modelRequests: requests.length, pendingSteps: steps.length, messages: sent().slice(-12), tasks: db?.query("SELECT message_id, state, error_text FROM tasks ORDER BY rowid").all() }));
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
  model.stop(true);
  await rm(root, { recursive: true, force: true });
}
