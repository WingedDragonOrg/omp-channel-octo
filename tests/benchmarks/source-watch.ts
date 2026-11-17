import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelStore } from "../../src/store/index.ts";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Opt-in end-to-end source-watch acceptance: real OCO CLI, installed OMP RPC child,
// encrypted loopback Octo, a local GitLab API/Decisions fixture, and disposable SQLite.
// It never contacts a real provider, bot, resident service, or model endpoint.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-source-watch-acceptance-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const children: Bun.Subprocess[] = [];
const outputs: Promise<string[]>[] = [];
const binPath = join(root, "bin");
const agentPath = join(root, "agent");
const configPath = join(root, "config.json");
const stateDir = join(root, "state");
const sessionDir = join(root, "sessions");
const decisionKey = "local-source-decisions-key";
const sourceKey = "local-source-token";
const publishedNotes = new Map<number, Record<string, unknown>>();
const wakeRequests: string[] = [];
let provider: Bun.Server<undefined> | undefined;
let modelServer: Bun.Server<undefined> | undefined;
let store: ChannelStore | undefined;
let db: Database | undefined;
let createToolIssued = false;
let stopToolIssued = false;
let modelError: string | undefined;
let successful = false;

interface TaskRow { id: string; state: string; error_text: string | null }
interface WatchRow { id: string; state: string }
interface SourceEventRow { id: string; body: Record<string, unknown>; wakeStatus: string; wakeTaskId: string | null }

function task(messageId: string): TaskRow | null {
  return db!.query<TaskRow, [string]>("SELECT id, state, error_text FROM tasks WHERE message_id = ?").get(messageId) ?? null;
}
function watchRow(): WatchRow | null {
  return db!.query<WatchRow, [string]>("SELECT id, state FROM source_watches WHERE bot_uid = ?").get(fixture.botUid) ?? null;
}
function wakeTasks(): Array<{ id: string; message_id: string; state: string }> {
  return db!.query<{ id: string; message_id: string; state: string }, []>(
    "SELECT id, message_id, state FROM tasks WHERE message_id LIKE 'watch:event:%'",
  ).all();
}
function sourceEvents(watchId: string): SourceEventRow[] {
  return store!.listSourceWatchEvents(watchId, Number.POSITIVE_INFINITY).map(event => ({
    id: event.id,
    body: event.body,
    wakeStatus: event.wakeStatus,
    wakeTaskId: event.wakeTaskId,
  }));
}
async function wait(description: string, predicate: () => boolean, timeoutMs = 90_000): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined, `Local model fixture failed: ${modelError}`);
    return predicate() ? true : undefined;
  }, timeoutMs, description);
}
async function exit(child: Bun.Subprocess): Promise<number> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("source-watch OCO CLI exit deadline exceeded")), 15_000);
    });
    return await Promise.race([child.exited, deadline]);
  } finally {
    clearTimeout(timer);
  }
}
function send(connection: FixtureConnection, text: string, messageId: string): void {
  fixture.sendInbound(connection, {
    fromUid: fixture.ownerUid,
    channelId: `${fixture.ownerUid}@${fixture.botUid}`,
    channelType: 1,
    text,
    messageId,
  });
}
function responseText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.map(block => {
    if (block && typeof block === "object" && "text" in block && typeof block.text === "string") return block.text;
    return "";
  }).join("\n");
}
function lastUserText(body: Record<string, unknown>): string {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const user = messages.filter(value => value && typeof value === "object" &&
    !Array.isArray(value) && (value as Record<string, unknown>).role === "user").at(-1);
  return user && typeof user === "object" && !Array.isArray(user)
    ? responseText((user as Record<string, unknown>).content)
    : "";
}
function hasWatchTool(body: Record<string, unknown>): boolean {
  return Array.isArray(body.tools) && body.tools.some(value => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const fn = (value as Record<string, unknown>).function;
    return !!fn && typeof fn === "object" && !Array.isArray(fn) && (fn as Record<string, unknown>).name === "watch";
  });
}
function completion(reply: { text?: string; tool?: { id: string; args: Record<string, unknown> } }): Response {
  const id = `source-watch-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const delta = reply.tool
    ? { tool_calls: [{ index: 0, id: reply.tool.id, type: "function", function: { name: "watch", arguments: JSON.stringify(reply.tool.args) } }] }
    : { content: reply.text ?? "SOURCE_WAKE_ACK" };
  const chunks = [
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta, finish_reason: null }] },
    { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: reply.tool ? "tool_calls" : "stop" }], usage: { prompt_tokens: 20, completion_tokens: 8, total_tokens: 28 } },
  ];
  return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
    headers: { "content-type": "text/event-stream" },
  });
}
function note(index: number): Record<string, unknown> {
  const timestamp = new Date(Date.now() + index).toISOString();
  return {
    id: 700 + index,
    body: `fixture note ${index}`,
    created_at: timestamp,
    updated_at: timestamp,
    author: { username: "fixture-owner" },
  };
}
function publishNote(index: number): void {
  publishedNotes.set(index, note(index));
}
async function launch(): Promise<{ child: Bun.Subprocess; connection: FixtureConnection }> {
  const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      PATH: `${binPath}:/usr/bin:/bin`,
      TMPDIR: root,
      PI_CODING_AGENT_DIR: agentPath,
      PI_NO_TITLE: "1",
      NODE_EXTRA_CA_CERTS: join(root, "certificate.pem"),
      OMP_CHANNEL_OCTO_DECISION_KEY: decisionKey,
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]));
  const connection = await fixture.waitForConnection(children.length, 20_000);
  await waitForFixture(() => connection.ready ? true : undefined, 15_000, "source-watch OCO CLI did not authenticate to local Octo");
  return { child, connection };
}

try {
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must use loopback");
  await mkdir(binPath);
  await mkdir(agentPath);
  await mkdir(sessionDir);
  await writeFile(join(root, "tls.conf"), "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=v3\n[dn]\nCN=localhost\n[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:TRUE\nkeyUsage=keyCertSign,digitalSignature,keyEncipherment\n");
  const certificate = Bun.spawn([
    "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-x509", "-days", "1",
    "-keyout", join(root, "key.pem"), "-out", join(root, "certificate.pem"), "-config", join(root, "tls.conf"),
  ], { stdout: "ignore", stderr: "pipe" });
  const certificateError = await new Response(certificate.stderr).text();
  assert.equal(await certificate.exited, 0, certificateError);
  await writeFile(join(binPath, "glab"), "#!/bin/sh\nprintf '%s\\n' 'local-source-token'\n", { mode: 0o755 });
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\nasync:\n  enabled: false\n");

  provider = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: { key: Bun.file(join(root, "key.pem")), cert: Bun.file(join(root, "certificate.pem")) },
    async fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === "/v1/decisions") {
        assert.equal(request.method, "POST");
        assert.equal(request.headers.get("authorization"), `Bearer ${decisionKey}`);
        const body = await request.json() as Record<string, unknown>;
        assert.equal(typeof body.input, "string");
        const input = JSON.parse(body.input as string) as Record<string, unknown>;
        assert.ok(input.evidence && typeof input.evidence === "object");
        const evidence = input.evidence as Record<string, unknown>;
        assert.equal(typeof evidence.stdout, "string");
        assert.ok((evidence.stdout as string).includes("fixture note"));
        return Response.json({ answers: [{ name: "watch_condition", type: "choice", choice: "satisfied", probabilities: [{ value: "satisfied", probability: 0.99 }] }] });
      }
      assert.equal(request.method, "GET");
      assert.equal(request.headers.get("private-token"), sourceKey);
      if (url.pathname.includes("/merge_requests/7/notes")) {
        return Response.json([...publishedNotes.values()]);
      }
      if (url.pathname.endsWith("/merge_requests/7")) {
        return Response.json({
          state: "opened",
          title: "Local source-watch fixture MR",
          diff_refs: { head_sha: "fixture-head" },
          merged_at: null,
        });
      }
      return new Response("unexpected local GitLab fixture path", { status: 404 });
    },
  });
  assert.ok(provider.port, "local GitLab/Decisions fixture did not bind");

  let modelRequestCount = 0;
  modelServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      try {
        const url = new URL(request.url);
        assert.equal(url.hostname, "127.0.0.1", "model request escaped loopback");
        assert.equal(url.pathname, "/v1/chat/completions");
        const body = await request.json() as Record<string, unknown>;
        modelRequestCount += 1;
        if (modelRequestCount > 40) throw new Error("local model fixture request budget exceeded");
        const userText = lastUserText(body);
        if (userText.includes("SOURCE_WATCH_CREATE_REQUEST") && !createToolIssued) {
          assert.ok(hasWatchTool(body), "OMP did not advertise the source watch host tool");
          createToolIssued = true;
          return completion({ tool: {
            id: "source-watch-create",
            args: {
              action: "create",
              resource: `gitlab://localhost:${provider!.port}/local/project/-/merge_requests/7`,
              eventTypes: ["note"],
              consumeCondition: "Accept each note containing the deterministic local fixture event.",
              decisionConsent: true,
              taskScope: "Report each accepted local fixture note without modifying files.",
              timeout: 600,
              sourceBudget: 20,
              decisionBudget: 20,
              wakeBudget: 5,
            },
          } });
        }
        if (userText.includes("SOURCE_WATCH_STOP_REQUEST") && !stopToolIssued) {
          assert.ok(hasWatchTool(body), "OMP did not advertise the source watch host tool for stop");
          const current = watchRow();
          assert.ok(current, "source subscription disappeared before stop");
          stopToolIssued = true;
          return completion({ tool: { id: "source-watch-stop", args: { action: "stop", id: current.id } } });
        }
        if (!userText.includes("SOURCE_WATCH_CREATE_REQUEST") && !userText.includes("SOURCE_WATCH_STOP_REQUEST")) {
          wakeRequests.push(userText);
        }
        return completion({ text: userText.includes("SOURCE_WATCH_STOP_REQUEST") ? "SOURCE_WATCH_STOPPED" : "SOURCE_WATCH_CREATED_OR_WAKE_ACK" });
      } catch (error) {
        modelError = error instanceof Error ? error.message : String(error);
        return new Response(modelError, { status: 400 });
      }
    },
  });
  assert.ok(modelServer.port, "local model fixture did not bind");
  await writeFile(join(agentPath, "models.yml"), JSON.stringify({ providers: {
    "local-source": {
      baseUrl: `http://127.0.0.1:${modelServer.port}/v1`,
      apiKey: "local-source-model-key",
      api: "openai-completions",
      models: [{ id: "probe", name: "Source Watch Local Fixture", reasoning: false, input: ["text"], contextWindow: 32_768, maxTokens: 2_048, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    },
  } }));
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [],
    stateDir,
    sessionsDir: sessionDir,
    ompCommand: realOmp,
    ompExtraArgs: ["--model", "local-source/probe", "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--config", join(root, "native.yml")],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
    decisions: { enabled: true, protocol: "openai-decisions", baseUrl: `https://127.0.0.1:${provider.port}`, model: "local-fixture", apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY" },
  }), { mode: 0o600 });

  const first = await launch();
  store = await ChannelStore.open(join(stateDir, "channel.sqlite"));
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  const createMessage = "SOURCE_WATCH_CREATE_REQUEST";
  send(first.connection, createMessage, "900001");
  await wait("source watch creation task failed", () => task("900001")?.state === "completed");
  await wait("source baseline did not initialize", () => watchRow()?.state === "watching");
  const watch = watchRow();
  assert.ok(watch, "source watch was not persisted");
  assert.equal(store.listSourceWatches(fixture.botUid).length, 1);

  publishNote(1);
  const firstEvent = await waitForFixture(() => {
    const event = sourceEvents(watch.id).find(candidate => candidate.body.text === "fixture note 1");
    return event ? event : undefined;
  }, 90_000, "first local source event was not observed");
  const firstMessageId = `watch:event:${watch.id}:${firstEvent.id}`;
  await wait("first source event did not complete exactly one wake", () =>
    task(firstMessageId)?.state === "completed" && wakeTasks().filter(row => row.message_id === firstMessageId).length === 1,
  );
  assert.equal(sourceEvents(watch.id).filter(event => event.body.text === "fixture note 1").length, 1);
  console.log("CASE local GitLab note -> Decisions -> real OMP RPC wake: passed");

  publishNote(2);
  const crash = process.env.SOURCE_WATCH_CRASH === "1";
  first.child.kill(crash ? "SIGKILL" : "SIGTERM");
  const firstExit = await exit(first.child);
  if (crash) assert.notEqual(firstExit, 0, "first OCO CLI did not crash");
  else assert.equal(firstExit, 0, "first OCO CLI did not shut down cleanly");
  const second = await launch();
  await wait("recovered source watch disappeared", () => watchRow()?.state === "watching");
  const secondEvent = await waitForFixture(() => {
    const event = sourceEvents(watch.id).find(candidate => candidate.body.text === "fixture note 2");
    return event ? event : undefined;
  }, 90_000, "recovered watch did not observe the second local event");
  const secondMessageId = `watch:event:${watch.id}:${secondEvent.id}`;
  await wait("second source event did not complete exactly one wake", () =>
    task(secondMessageId)?.state === "completed" && wakeTasks().filter(row => row.message_id === secondMessageId).length === 1,
  );
  assert.equal(sourceEvents(watch.id).filter(event => event.body.text === "fixture note 1").length, 1, "restart re-observed the accepted first note");
  assert.equal(wakeTasks().length, 2, "source watch accepted duplicate or unexpected wake tasks");
  assert.deepEqual(new Set(wakeTasks().map(row => row.message_id)), new Set([firstMessageId, secondMessageId]));
  assert.ok(wakeRequests.some(text => text.includes("fixture note 1")), "first source wake never reached the local OMP model");
  assert.ok(wakeRequests.some(text => text.includes("fixture note 2")), "second source wake never reached the local OMP model");
  console.log("CASE SQLite restart resumes cursor and dispatches a distinct second event exactly once: passed");

  send(second.connection, "SOURCE_WATCH_STOP_REQUEST", "900002");
  await wait("source watch stop task failed", () => task("900002")?.state === "completed");
  await wait("source watch stop did not persist", () => watchRow()?.state === "stopped");
  publishNote(3);
  await new Promise(resolve => setTimeout(resolve, 1_000));
  assert.equal(sourceEvents(watch.id).some(event => event.body.text === "fixture note 3"), false, "stopped subscription accepted another note");
  assert.equal(wakeTasks().length, 2, "stopped subscription accepted a new wake");
  second.child.kill("SIGTERM");
  assert.equal(await exit(second.child), 0, "recovered OCO CLI did not shut down cleanly");

  const outputText = (await Promise.all(outputs)).flat().join("\n");
  assert.equal(outputText.includes(sourceKey), false, "source token reached CLI output");
  assert.equal(outputText.includes(decisionKey), false, "Decisions key reached CLI output");
  assert.equal(outputText.includes(fixture.botToken), false, "Octo token reached CLI output");
  successful = true;
  console.log(JSON.stringify({ result: "passed", sourceEvents: 2, uniqueWakeTasks: wakeTasks().length, recoveredAfterRestart: true, stoppedBeforeThirdEvent: true, realOmpCommand: realOmp }));
} catch (error) {
  console.error(`Source-watch benchmark diagnostic directory: ${root}`);
  throw error;
} finally {
  for (const child of children) {
    if (child.exitCode === null) {
      child.kill("SIGTERM");
      try { await exit(child); }
      catch { child.kill("SIGKILL"); await child.exited; }
    }
  }
  store?.close();
  db?.close();
  provider?.stop(true);
  modelServer?.stop(true);
  await fixture.close();
  if (successful) await rm(root, { recursive: true, force: true });
}
