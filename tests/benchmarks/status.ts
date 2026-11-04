import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Opt-in local-only acceptance for /status: actual installed OMP, local model server, isolated Octo fixture.
const omp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(omp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-native-status-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const checkout = resolve(import.meta.dir, "../..");
const children: Bun.Subprocess[] = [];
const outputs: Promise<string>[] = [];
const modelRequests: Record<string, unknown>[] = [];
let modelError: string | undefined;
let modelHold = Promise.withResolvers<void>();
let modelEntered = Promise.withResolvers<void>();
let connection: FixtureConnection;
let db: Database | undefined;
let sequence = 1000;
let modelServer: ReturnType<typeof Bun.serve> | undefined;

function sent(): string[] {
  return fixture.sentMessages.map(message => (message.body.payload as { content?: string } | undefined)?.content ?? "");
}

function send(text: string): string {
  const id = String(++sequence);
  fixture.sendInbound(connection, {
    fromUid: fixture.ownerUid,
    channelId: `${fixture.ownerUid}@${fixture.botUid}`,
    channelType: 1,
    text,
    messageId: id,
  });
  return id;
}

function taskState(id: string): string | undefined {
  return (db!.query("SELECT state FROM tasks WHERE message_id = ?").get(id) as { state: string } | null)?.state;
}

function workerCounts(): { total: number; active: number } {
  const row = db!.query(`SELECT COUNT(*) AS total, SUM(CASE WHEN state != 'stopped' THEN 1 ELSE 0 END) AS active
    FROM worker_ledger`).get() as { total: number; active: number | null };
  return { total: row.total, active: row.active ?? 0 };
}

interface NativeContextSample {
  kind: "get_state_sample";
  success: boolean;
  sessionId: string | null;
  sessionFile: string | null;
  contextUsage: { tokens: number | null; contextWindow: number | null; percent: number | null };
  model: { provider: string | null; id: string | null; contextWindow: number | null };
  thinkingLevel: string | null;
}

async function readNativeContextSamples(path: string): Promise<NativeContextSample[]> {
  return (await readFile(path, "utf8"))
    .split("\n")
    .filter(Boolean)
    .map(line => JSON.parse(line) as NativeContextSample)
    .filter(sample => sample.kind === "get_state_sample");
}

async function wait(description: string, predicate: () => boolean): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined);
    return predicate() ? true : undefined;
  }, 45_000, description);
}

async function exit(child: Bun.Subprocess): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("Status CLI did not exit")), 15_000);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

async function launch(): Promise<Bun.Subprocess> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", join(root, "config.json")], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: root,
      PATH: `${join(root, "bin")}:/usr/bin:/bin`,
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
  outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(children.length, 15_000);
  await wait("CLI failed to authenticate", () => connection.ready);
  return child;
}

const model = {
  baseUrl: "",
  apiKey: "local-only-placeholder",
  api: "openai-completions",
  models: [{
    id: "probe",
    name: "Status Probe",
    reasoning: false,
    input: ["text"],
    contextWindow: 32768,
    maxTokens: 1024,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  }],
};

try {
  modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        modelError = "local model received a non-JSON request";
        return new Response(modelError, { status: 400 });
      }
      modelRequests.push(body);
      if (modelRequests.length > 1) {
        modelError = "the one-turn local model script was exhausted";
        return new Response(modelError, { status: 400 });
      }
      modelEntered.resolve();
      await modelHold.promise;
      const id = "chatcmpl-status-smoke";
      const chunks = [
        { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: { role: "assistant", content: "STATUS_SMOKE_DONE" }, finish_reason: null }] },
        { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
        { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [], usage: { prompt_tokens: 2_729, completion_tokens: 4, total_tokens: 2_733 } },
      ];
      return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
        headers: { "content-type": "text/event-stream" },
      });
    },
  });
  model.baseUrl = `http://127.0.0.1:${modelServer.port}/v1`;
  modelHold = Promise.withResolvers<void>();
  modelEntered = Promise.withResolvers<void>();

  await mkdir(join(root, "bin"));
  await mkdir(join(root, "agent"));
  await mkdir(join(root, "cwd"));
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  const wirePath = join(root, "omp-wire.jsonl");
  await writeFile(wirePath, "");
  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, String.raw`#!${process.execPath}
import {appendFileSync} from "node:fs";
const child=Bun.spawn([${JSON.stringify(omp)},...process.argv.slice(2)],{stdin:"pipe",stdout:"pipe",stderr:"inherit",env:process.env});
const wirePath=${JSON.stringify(wirePath)};
const newline=String.fromCharCode(10);
const waiting=new Set();
const input=(async()=>{
  let buffer="";
  const decoder=new TextDecoder();
  for await(const bytes of Bun.stdin.stream()){
    buffer+=decoder.decode(bytes,{stream:true});
    let n;
    while((n=buffer.indexOf(newline))>=0){
      const line=buffer.slice(0,n);buffer=buffer.slice(n+1);
      if(!line.trim())continue;
      const frame=JSON.parse(line);
      if(frame.type==="get_state"&&typeof frame.id==="string")waiting.add(frame.id);
      child.stdin.write(line+newline);
    }
  }
})();
const output=(async()=>{
  let buffer="";
  const decoder=new TextDecoder();
  for await(const bytes of child.stdout){
    process.stdout.write(bytes);
    buffer+=decoder.decode(bytes,{stream:true});
    let n;
    while((n=buffer.indexOf(newline))>=0){
      const line=buffer.slice(0,n);buffer=buffer.slice(n+1);
      if(!line.trim())continue;
      let frame;
      try{frame=JSON.parse(line);}catch{continue;}
      if(frame.type!=="response"||frame.command!=="get_state"||typeof frame.id!=="string"||!waiting.delete(frame.id))continue;
      const data=frame.data&&typeof frame.data==="object"?frame.data:{};
      const usage=data.contextUsage&&typeof data.contextUsage==="object"?data.contextUsage:{};
      const model=data.model&&typeof data.model==="object"?data.model:{};
      appendFileSync(wirePath,JSON.stringify({
        kind:"get_state_sample",
        success:frame.success,
        sessionId:typeof data.sessionId==="string"?data.sessionId:null,
        sessionFile:typeof data.sessionFile==="string"?data.sessionFile:null,
        contextUsage:{tokens:usage.tokens??null,contextWindow:usage.contextWindow??null,percent:usage.percent??null},
        model:{provider:model.provider??null,id:model.id??null,contextWindow:model.contextWindow??null},
        thinkingLevel:data.thinkingLevel??null
      })+newline);
    }
  }
})();
await input;
child.stdin.end();
await output;
process.exit(await child.exited);
`, { mode: 0o755 });
  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: { "local-status": model } }));
  const nativeConfig = join(root, "native.yml");
  await writeFile(nativeConfig, "autoRetry:\n  enabled: false\n");
  await writeFile(join(root, "config.json"), JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [],
    stateDir: join(root, "state"),
    sessionsDir: join(root, "sessions"),
    ompCommand: wrapper,
    ompExtraArgs: ["--model", "local-status/probe", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools", "--config", nativeConfig],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  const child = await launch();
  db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
  const newAt = fixture.sentMessages.length;
  send(`/new ${join(root, "cwd")}`);
  await wait("Fresh native session was not bound", () => sent().slice(newAt).some(text => text.includes("已创建会话")));
  const channelId = fixture.ownerUid;
  const taskId = send("held task for native status context");
  await wait("Native OMP never reached the held local model request", () => modelRequests.length === 1 && taskState(taskId) === "started");
  await modelEntered.promise;

  const bindingBefore = db.query("SELECT session_id, session_file, cwd FROM chats WHERE bot_uid = ? AND channel_type = 1 AND channel_id = ?").get(fixture.botUid, channelId) as { session_id: string; session_file: string; cwd: string } | null;
  assert.ok(bindingBefore?.session_id, "The in-flight task has no persisted session identity");
  const countsBeforeStatus = workerCounts();
  const requestCountBeforeStatus = modelRequests.length;
  const activeStatusStart = fixture.sentMessages.length;
  send("/status");
  await wait("Active /status did not report context", () => sent().slice(activeStatusStart).some(text => text.includes("context=")));
  const activeStatusWire = JSON.stringify(fixture.sentMessages.slice(activeStatusStart));
  assert.ok(activeStatusWire.includes(bindingBefore.session_id), "Active status omitted the exact session ID");
  assert.match(activeStatusWire, /[0-9][0-9,.]*k?\s*\/\s*[0-9][0-9,.]*k?/iu, "Active status omitted the current tokens/window ratio");
  assert.equal(modelRequests.length, requestCountBeforeStatus, "/status caused an extra local-model request");
  assert.deepEqual(workerCounts(), countsBeforeStatus, "/status created or stopped an OMP worker during the held turn");
  assert.deepEqual(
    db.query("SELECT session_id, session_file, cwd FROM chats WHERE bot_uid = ? AND channel_type = 1 AND channel_id = ?").get(fixture.botUid, channelId),
    bindingBefore,
    "/status changed the bound session identity",
  );

  modelHold.resolve();
  await wait("The held task did not continue and finish after /status", () => taskState(taskId) === "completed" && sent().some(text => text.includes("STATUS_SMOKE_DONE")));
  await wait("Native worker did not stop after the completed task", () => workerCounts().active === 0);
  const snapshot = db.query(`SELECT session_id, session_file, cwd, used_tokens, context_window, percent, model
    FROM session_context_snapshots WHERE bot_uid = ? AND channel_type = 1 AND channel_id = ?`).get(fixture.botUid, channelId) as {
      session_id: string; session_file: string; cwd: string; used_tokens: number | null; context_window: number | null; percent: number | null; model: string | null;
    } | null;
  const nativeContextSamples = await readNativeContextSamples(wirePath);
  const lastNativeSample = nativeContextSamples.at(-1);
  assert.ok(snapshot, `No live/final context sample was persisted; native get_state samples=${JSON.stringify(nativeContextSamples)}`);
  assert.ok(lastNativeSample, `OMP emitted no matching get_state response; native get_state samples=${JSON.stringify(nativeContextSamples)}`);
  assert.equal(snapshot.session_id, bindingBefore.session_id);
  assert.equal(snapshot.session_file, bindingBefore.session_file);
  assert.equal(snapshot.session_id, lastNativeSample.sessionId);
  assert.equal(snapshot.used_tokens, lastNativeSample.contextUsage.tokens);
  assert.equal(snapshot.context_window, lastNativeSample.contextUsage.contextWindow ?? lastNativeSample.model.contextWindow);
  assert.equal(snapshot.percent, lastNativeSample.contextUsage.percent);
  assert.ok(
    snapshot.used_tokens !== null && snapshot.used_tokens > 0,
    `Native OMP returned no usable current context tokens; snapshot=${JSON.stringify(snapshot)}; native get_state samples=${JSON.stringify(nativeContextSamples)}`,
  );

  const countsBeforeIdleStatus = workerCounts();
  const idleRequestCount = modelRequests.length;
  const idleStatusStart = fixture.sentMessages.length;
  send("/status");
  await wait("Idle /status did not reuse the persisted context snapshot", () => sent().slice(idleStatusStart).some(text => text.includes("context=")));
  const idleStatusWire = JSON.stringify(fixture.sentMessages.slice(idleStatusStart));
  assert.ok(idleStatusWire.includes(snapshot.session_id), "Idle status omitted the persisted session identity");
  assert.ok(idleStatusWire.includes("contextSource=persisted"), "Idle status did not identify its persisted context snapshot");
  assert.equal(modelRequests.length, idleRequestCount, "Idle /status called the local model");
  assert.deepEqual(workerCounts(), countsBeforeIdleStatus, "Idle /status created an OMP worker");

  child.kill("SIGTERM");
  assert.equal(await exit(child), 0);
  assert.equal(modelRequests.length, 1, "The one-turn local model script received an unexpected request");
  console.log(JSON.stringify({ realOmp: omp, heldTurnContinued: true, activeStatusReadOnly: true, idleSnapshotReadable: true, sessionId: snapshot.session_id, modelRequests: modelRequests.length }));
} catch (error) {
  console.error(JSON.stringify({ error: error instanceof Error ? error.message : String(error), modelRequests: modelRequests.length, modelError, messages: sent().slice(-12) }));
  for (const child of children) if (child.exitCode === null) child.kill("SIGTERM");
  await Promise.allSettled(children.map(child => child.exited));
  const logs = (await Promise.all(outputs)).join("\n").replaceAll(fixture.botToken, "[fixture-token]");
  console.error(logs.slice(-12_000));
  throw error;
} finally {
  modelHold.resolve();
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await Promise.allSettled(children.map(child => child.exited));
  db?.close();
  await fixture.close();
  modelServer?.stop(true);
  await rm(root, { recursive: true, force: true });
}
