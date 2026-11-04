import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Opt-in acceptance: installed OMP, finite loopback model, encrypted local Octo, isolated profile.
const omp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(omp, "Install OMP or set OMP_REAL_COMMAND");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-native-btw-")));
const checkout = resolve(import.meta.dir, "../..");
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const wirePath = join(root, "wire.jsonl");
const requests: Record<string, unknown>[] = [];
type Step = { text: string; hold?: Promise<void> };
let steps: Step[] = [];
let modelError: string | undefined;
let child: Bun.Subprocess | undefined;
let logs: Promise<string> | undefined;
let connection: FixtureConnection;
let db: Database | undefined;
let sequence = 2000;
const gates: { promise: Promise<void>; resolve(): void }[] = [];
function gate() {
  const result = Promise.withResolvers<void>();
  gates.push(result);
  return result;
}
const model = Bun.serve({
  hostname: "127.0.0.1", port: 0,
  async fetch(request) {
    const body = await request.json() as Record<string, unknown>;
    requests.push(body);
    const step = steps.shift();
    if (!step) {
      modelError = "OMP exceeded the finite BTW model script";
      return new Response(modelError, { status: 400 });
    }
    if (step.hold) await step.hold;
    const index = requests.length;
    const chunks = [
      { id: `btw-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: { role: "assistant", content: step.text }, finish_reason: null }] },
      { id: `btw-${index}`, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } },
    ];
    return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } });
  },
});
function sent(): string[] {
  return fixture.sentMessages.map(message => {
    const payload = message.body.payload;
    return payload && typeof payload === "object" && "content" in payload && typeof payload.content === "string" ? payload.content : "";
  });
}
function send(text: string, id = String(++sequence)): string {
  fixture.sendInbound(connection, { fromUid: fixture.ownerUid, channelId: `${fixture.ownerUid}@${fixture.botUid}`, channelType: 1, text, messageId: id });
  return id;
}
function state(id: string): string | undefined {
  return db!.query<{ state: string }, [string]>("SELECT state FROM tasks WHERE message_id = ?").get(id)?.state;
}
function binding(): { session_id: string; session_file: string } {
  return db!.query<{ session_id: string; session_file: string }, [string]>("SELECT session_id, session_file FROM chats WHERE channel_type = 1 AND channel_id = ?").get(fixture.ownerUid)!;
}
async function wait(description: string, predicate: () => boolean): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined);
    return predicate() ? true : undefined;
  }, 45_000, description);
}
async function clean(): Promise<void> {
  await wait("BTW retained a managed worker", () => db!.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM worker_ledger WHERE state != 'stopped'").get()!.n === 0);
}
async function exit(process: Bun.Subprocess): Promise<number> {
  const deadline = Promise.withResolvers<never>();
  const timer = setTimeout(() => deadline.reject(new Error("BTW CLI did not exit")), 15_000);
  try { return await Promise.race([process.exited, deadline.promise]); }
  finally { clearTimeout(timer); }
}
async function wire(): Promise<{ type: string; question?: string; message?: string }[]> {
  return (await readFile(wirePath, "utf8")).trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
}
try {
  for (const directory of ["bin", "agent", "cwd"]) await mkdir(join(root, directory));
  await writeFile(wirePath, "");
  for (const command of ["octo-cli", "super"]) await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, `#!${process.execPath}\nimport {appendFileSync} from 'node:fs';\nconst child=Bun.spawn([${JSON.stringify(omp)},...process.argv.slice(2)],{stdin:'pipe',stdout:'inherit',stderr:'inherit',env:process.env});\nlet buffer='';const decoder=new TextDecoder();\nfor await(const bytes of Bun.stdin.stream()){buffer+=decoder.decode(bytes,{stream:true});let n;while((n=buffer.indexOf('\\n'))>=0){const line=buffer.slice(0,n);buffer=buffer.slice(n+1);if(line.trim()){const f=JSON.parse(line);appendFileSync(${JSON.stringify(wirePath)},JSON.stringify({type:f.type,question:f.question,message:f.message})+'\\n');}child.stdin.write(line+'\\n');}}\nchild.stdin.end();process.exit(await child.exited);\n`, { mode: 0o755 });
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: { "local-btw": { baseUrl: `http://127.0.0.1:${model.port}/v1`, apiKey: "local-fixture-only", api: "openai-completions", models: [{ id: "probe", name: "Probe", reasoning: false, input: ["text"], contextWindow: 32768, maxTokens: 8192, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");
  await writeFile(join(root, "config.json"), JSON.stringify({ apiUrl: fixture.apiUrl, botToken: fixture.botToken, ownerId: fixture.ownerUid, allowedGroupIds: [], stateDir: join(root, "state"), sessionsDir: join(root, "sessions"), ompCommand: wrapper, ompExtraArgs: ["--model", "local-btw/probe", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools", "--config", join(root, "native.yml")], startupTimeoutMs: 30000, cancelTimeoutMs: 3000, groupHistory: { enabled: false, maxMessages: 50 } }), { mode: 0o600 });
  const launched = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", join(root, "config.json")], { cwd: root, env: { HOME: root, TMPDIR: root, PATH: `${join(root, "bin")}:/usr/bin:/bin`, PI_CODING_AGENT_DIR: join(root, "agent"), PI_NO_TITLE: "1", LANG: "C", TZ: "UTC" }, stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  child = launched;
  logs = Promise.all([new Response(launched.stdout).text(), new Response(launched.stderr).text()]).then(parts => parts.join("\n"));
  connection = await fixture.waitForConnection(1, 15000);
  await wait("CLI authentication", () => connection.ready);
  db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
  send(`/new ${join(root, "cwd")}`);
  await wait("Session creation", () => sent().some(text => text.includes("已创建会话")));
  steps = [{ text: "MAIN_CONTEXT_SENTINEL" }];
  const seed = send("remember MAIN_CONTEXT_KEY");
  await wait("Seed context", () => state(seed) === "completed");
  await clean();
  const original = binding();

  const main = gate();
  steps = [{ text: "MAIN_HELD_FINISHED", hold: main.promise }, { text: "SIDE_LIVE_ANSWER" }];
  const held = send("main held task");
  await wait("Main held model request", () => requests.length === 2);
  const live = send("/btw SIDE_LIVE_QUESTION");
  await wait("BTW answered before main", () => sent().some(text => text.includes("SIDE_LIVE_ANSWER")));
  assert.equal(state(held), "started");
  assert.equal(state(live), undefined, "BTW became a normal task");
  assert.ok(JSON.stringify(requests[2]).includes("MAIN_CONTEXT_SENTINEL"), "BTW lost the original session context");
  main.resolve();
  await wait("Main continues after BTW", () => state(held) === "completed");
  await clean();

  const earlyMain = gate();
  const lateSide = gate();
  steps = [{ text: "MAIN_EARLY_FINISHED", hold: earlyMain.promise }, { text: "SIDE_LATE_ANSWER", hold: lateSide.promise }];
  const early = send("main ends before side");
  await wait("Second main held", () => requests.length === 4);
  send("/btw SIDE_LATE_QUESTION");
  await wait("Second BTW started", () => requests.length === 5);
  earlyMain.resolve();
  await wait("Main terminal before BTW", () => state(early) === "completed");
  assert.equal(sent().some(text => text.includes("SIDE_LATE_ANSWER")), false);
  assert.ok(db.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM worker_ledger WHERE state = 'running'").get()!.n > 0);
  lateSide.resolve();
  await wait("BTW survives main completion", () => sent().some(text => text.includes("SIDE_LATE_ANSWER")));
  await clean();

  const longAnswer = "SIDE_IDLE_BEGIN\n" + Array.from({ length: 500 }, (_, index) => `第${index}行完整回答🌱。\n`).join("") + "SIDE_IDLE_END";
  steps = [{ text: longAnswer }];
  const before = fixture.sentMessages.length;
  const idle = send("/btw SIDE_IDLE_QUESTION");
  await wait("Idle BTW complete", () => sent().slice(before).join("").includes("SIDE_IDLE_END"));
  await clean();
  assert.ok(sent().slice(before).join("").includes(longAnswer), "BTW answer lost bytes across outbox segments");
  assert.deepEqual(binding(), original, "Idle BTW changed the precise bound session");
  assert.equal(state(idle), undefined);
  send("/btw SIDE_IDLE_QUESTION", idle);
  send("/status");
  await wait("Duplicate inbound ack", () => connection.recvAcks.filter(ack => ack.messageId === idle).length >= 2);

  steps = [{ text: "MAIN_AFTER_BTW" }];
  const after = send("main after side answers");
  await wait("Main after BTW", () => state(after) === "completed");
  await clean();
  assert.equal(requests.length, 7);
  for (const marker of ["SIDE_LIVE_ANSWER", "SIDE_LATE_ANSWER", "SIDE_IDLE_BEGIN"]) {
    assert.equal(JSON.stringify(requests[6]).includes(marker), false, "BTW polluted the next main model context");
    assert.equal((await readFile(original.session_file, "utf8")).includes(marker), false, "BTW answer entered main session history");
  }
  const taskResults = db.query("SELECT result_text FROM tasks").all();
  assert.equal(JSON.stringify(taskResults).includes("SIDE_"), false, "BTW polluted task result");
  const frames = await wire();
  assert.deepEqual(frames.filter(frame => frame.type === "btw").map(frame => frame.question), ["SIDE_LIVE_QUESTION", "SIDE_LATE_QUESTION", "SIDE_IDLE_QUESTION"]);
  assert.equal(frames.some(frame => frame.type === "steer" || frame.type === "prompt" && frame.message?.startsWith("/btw")), false);
  assert.equal(steps.length, 0);
  const shutdownSide = gate();
  steps = [{ text: "SIDE_SHUTDOWN_MUST_NOT_DELIVER", hold: shutdownSide.promise }];
  send("/btw SIDE_SHUTDOWN_QUESTION");
  await wait("Shutdown BTW started", () => requests.length === 8);
  child.kill("SIGTERM");
  assert.equal(await exit(child), 0);
  await clean();
  assert.equal(sent().some(text => text.includes("SIDE_SHUTDOWN_MUST_NOT_DELIVER")), false);
  assert.equal(steps.length, 0);
  assert.ok((await wire()).some(frame => frame.type === "btw_cancel"), "Shutdown did not cancel native BTW");
  console.log(JSON.stringify({ realOmp: omp, liveBtwBeforeMain: true, mainFirstBtwSurvives: true, idleExactSession: true, authoritativeAnswerBytes: Buffer.byteLength(longAnswer), sideAnswersInMainHistory: 0, duplicateBtwRequests: 0, shutdownBtwCancelled: true, modelRequests: requests.length, managedWorkersRemaining: 0 }));
} catch (error) {
  console.error(JSON.stringify({ modelRequests: requests.length, pendingSteps: steps.length, messages: sent().slice(-8), tasks: db?.query("SELECT message_id,state,error_text FROM tasks").all() }));
  for (const item of gates) item.resolve();
  if (child && child.exitCode === null) child.kill("SIGTERM");
  if (child) await exit(child).catch(() => {});
  if (logs && child?.exitCode !== null) console.error((await logs).replaceAll(fixture.botToken, "[fixture-token]").slice(-12000));
  throw error;
} finally {
  for (const item of gates) item.resolve();
  if (child && child.exitCode === null) child.kill("SIGKILL");
  if (child) await child.exited;
  db?.close();
  await fixture.close();
  model.stop(true);
  await rm(root, { recursive: true, force: true });
}
