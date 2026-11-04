import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "../fixtures/octo-protocol-server.ts";

// Opt-in acceptance: installed OMP, a finite loopback model, encrypted local Octo, and a real temporary Git repository.
// The deterministic model response proves request/prompt/delivery wiring only; it does not prove review quality.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
const realOmp = resolve(requestedOmp);
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-review-benchmark-")));
const repo = join(root, "project");
const stateDir = join(root, "state");
const auditPath = join(root, "omp-audit.jsonl");
const cliOutputs: Promise<string>[] = [];
const modelRequests: { kind: "main-seed" | "review" | "main-after"; text: string }[] = [];
const expectedRequests = [
  { kind: "main-seed", marker: "MAIN_SEED_USER_SENTINEL", response: "MAIN_SEED_RESPONSE_SENTINEL" },
  { kind: "review", marker: "REVIEW_FOCUS_SENTINEL", response: "REVIEW_FIXTURE_REPORT_BEGIN\nREVIEW_DELIVERY_SENTINEL: deterministic fixture output\nREVIEW_FIXTURE_REPORT_END" },
  { kind: "main-after", marker: "MAIN_AFTER_REVIEW_SENTINEL", response: "MAIN_AFTER_REVIEW_RESPONSE_SENTINEL" },
] as const;
const mainGate = Promise.withResolvers<void>();
const reviewGate = Promise.withResolvers<void>();
let modelError: string | undefined;
let fixture: OctoProtocolFixture | undefined;
let connection: FixtureConnection | undefined;
let db: Database | undefined;
let child: Bun.Subprocess | undefined;
let stopModel: (() => void) | undefined;
let messageSequence = 20_000;
let succeeded = false;

const model = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(request) {
    if (request.method !== "POST") return new Response("not found", { status: 404 });
    let body: Record<string, unknown>;
    try { body = await request.json() as Record<string, unknown>; }
    catch {
      modelError ??= "The local model received a non-JSON request";
      return new Response(modelError, { status: 400 });
    }
    const text = JSON.stringify(body.messages ?? body);
    const expected = expectedRequests[modelRequests.length];
    if (!expected) {
      modelError ??= "OMP exceeded the finite review benchmark model script";
      return new Response(modelError, { status: 400 });
    }
    if (!text.includes(expected.marker)) {
      modelError ??= `Expected ${expected.kind} model request marker ${expected.marker}`;
      return new Response(modelError, { status: 400 });
    }
    modelRequests.push({ kind: expected.kind, text });
    if (expected.kind === "main-seed") await mainGate.promise;
    if (expected.kind === "review") await reviewGate.promise;
    const id = `review-fixture-${modelRequests.length}`;
    const chunks = [
      { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: { role: "assistant", content: expected.response }, finish_reason: null }] },
      { id, object: "chat.completion.chunk", created: 0, model: "probe", choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 8, total_tokens: 18 } },
    ];
    return new Response(chunks.map(chunk => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n", {
      headers: { "content-type": "text/event-stream" },
    });
  },
});
stopModel = () => model.stop(true);
const modelPort = model.port;
assert.ok(modelPort, "The loopback model did not expose a port");

function sentText(message: FixtureSentMessage): string {
  const payload = message.body.payload;
  return payload && typeof payload === "object" && "content" in payload && typeof payload.content === "string"
    ? payload.content
    : "";
}


function send(text: string): string {
  const messageId = String(++messageSequence);
  fixture!.sendInbound(connection!, {
    fromUid: fixture!.ownerUid,
    channelId: `${fixture!.ownerUid}@${fixture!.botUid}`,
    channelType: 1,
    text,
    messageId,
  });
  return messageId;
}

interface Binding {
  session_id: string;
  cwd: string;
  session_file: string | null;
}

function binding(): Binding | null {
  return db!.query<Binding, [string, number, string]>(
    "SELECT session_id, cwd, session_file FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
  ).get(fixture!.botUid, 1, fixture!.ownerUid) ?? null;
}

function taskState(messageId: string): string | undefined {
  return db!.query<{ state: string }, [string]>("SELECT state FROM tasks WHERE message_id = ?").get(messageId)?.state;
}

async function wait(description: string, predicate: () => boolean): Promise<void> {
  await waitForFixture(() => {
    assert.equal(modelError, undefined);
    return predicate() ? true : undefined;
  }, 45_000, description);
}

async function clean(): Promise<void> {
  await wait("Review benchmark retained a managed worker", () =>
    db!.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM worker_ledger WHERE state != 'stopped'").get()!.count === 0,
  );
}

async function exit(process: Bun.Subprocess): Promise<number> {
  const deadline = Promise.withResolvers<never>();
  const timer = setTimeout(() => deadline.reject(new Error("Review benchmark CLI did not exit")), 15_000);
  try { return await Promise.race([process.exited, deadline.promise]); }
  finally { clearTimeout(timer); }
}

async function run(argv: string[], cwd?: string): Promise<string> {
  const process = Bun.spawn(argv, {
    ...(cwd === undefined ? {} : { cwd }),
    env: { ...globalThis.process.env, HOME: root, GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0" },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  assert.equal(code, 0, `${argv.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function git(args: string[]): Promise<string> {
  return run([gitPath, "-C", repo, ...args]);
}

async function audit(): Promise<AuditRow[]> {
  const content = await readFile(auditPath, "utf8");
  return content.split("\n").filter(Boolean).map(line => JSON.parse(line) as AuditRow);
}

interface AuditRow {
  kind: "spawn" | "prompt" | "exit";
  pid: number;
  args?: string[];
  message?: string;
  exitCode?: number;
}
const discoveredGitPath = Bun.which("git");
if (!discoveredGitPath) throw new Error("A local Git executable is required for the review benchmark");
const gitPath = discoveredGitPath;

try {
  fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  assert.equal(new URL(fixture.apiUrl).hostname, "127.0.0.1", "Octo fixture must bind loopback only");

  for (const directory of ["bin", "agent", "tmp"]) await mkdir(join(root, directory), { recursive: true });
  await mkdir(join(repo, "src"), { recursive: true });
  await writeFile(auditPath, "");
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  await writeFile(join(root, "bin", "git"), `#!/bin/sh\nexec ${JSON.stringify(gitPath)} "$@"\n`, { mode: 0o755 });

  await run([gitPath, "init", "-q", "-b", "main", repo]);
  await writeFile(join(repo, "src", "tax.ts"),
    "export function totalWithTax(subtotal: number, taxRate: number): number {\n  return subtotal * (1 + taxRate);\n}\n");
  await writeFile(join(repo, "src", "unstaged.ts"), 'export const unstaged = "baseline-unstaged";\n');
  await git(["add", "src/tax.ts", "src/unstaged.ts"]);
  await run([gitPath, "-C", repo, "-c", "user.name=Review Fixture", "-c", "user.email=review-fixture@example.invalid", "commit", "-q", "-m", "review benchmark baseline"]);
  const head = await git(["rev-parse", "HEAD"]);

  await writeFile(join(repo, "src", "tax.ts"),
    "export function totalWithTax(subtotal: number, taxRate: number): number {\n  // REVIEW_STAGED_CHANGE_SENTINEL: this intentionally drops the principal.\n  return subtotal * taxRate;\n}\n");
  await git(["add", "src/tax.ts"]);
  await writeFile(join(repo, "src", "unstaged.ts"), 'export const unstaged = "REVIEW_UNSTAGED_CHANGE_SENTINEL";\n');
  await writeFile(join(repo, "src", "tax.spec.ts"),
    'import assert from "node:assert/strict";\n'
    + 'import { totalWithTax } from "./tax.ts";\n'
    + 'assert.equal(totalWithTax(100, 0.1), 110, "REVIEW_UNTRACKED_CHANGE_SENTINEL: principal plus tax");\n');
  const initialStatus = await git(["status", "--porcelain=v1"]);
  assert.ok(initialStatus.includes("src/tax.ts"), "The fixture did not create a staged change");
  assert.ok(initialStatus.includes("src/unstaged.ts"), "The fixture did not create an unstaged change");
  assert.ok(initialStatus.includes("src/tax.spec.ts"), "The fixture did not create an untracked change");
  const canonicalRepo = await realpath(repo);

  await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: {
    "local-review": {
      baseUrl: `http://127.0.0.1:${modelPort}/v1`,
      apiKey: "local-fixture-only",
      api: "openai-completions",
      models: [{
        id: "probe",
        name: "Local Review Acceptance",
        reasoning: false,
        input: ["text"],
        contextWindow: 32768,
        maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }],
    },
  } }));
  await writeFile(join(root, "native.yml"), "autoRetry:\n  enabled: false\n");

  const wirePath = auditPath;
  const wrapper = join(root, "real-omp.ts");
  await writeFile(wrapper, `#!${process.execPath}
import { appendFileSync } from "node:fs";
const auditPath = ${JSON.stringify(wirePath)};
const command = ${JSON.stringify(realOmp)};
const args = process.argv.slice(2);
const child = Bun.spawn([command, ...args], { stdin: "pipe", stdout: "inherit", stderr: "inherit", env: process.env });
function record(row) { appendFileSync(auditPath, JSON.stringify(row) + "\\n"); }
record({ kind: "spawn", pid: child.pid, args });
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(signal, () => { try { child.kill(signal); } catch {} });
let pending = "";
const decoder = new TextDecoder();
for await (const bytes of Bun.stdin.stream()) {
  pending += decoder.decode(bytes, { stream: true });
  let newline;
  while ((newline = pending.indexOf("\\n")) >= 0) {
    const line = pending.slice(0, newline);
    pending = pending.slice(newline + 1);
    if (!line.trim()) continue;
    try {
      const frame = JSON.parse(line);
      if (frame.type === "prompt" && typeof frame.message === "string") record({ kind: "prompt", pid: child.pid, message: frame.message });
    } catch {}
    try { child.stdin.write(line + "\\n"); } catch { break; }
  }
}
pending += decoder.decode();
if (pending.trim()) {
  try {
    const frame = JSON.parse(pending);
    if (frame.type === "prompt" && typeof frame.message === "string") record({ kind: "prompt", pid: child.pid, message: frame.message });
  } catch {}
  try { child.stdin.write(pending); } catch {}
}
child.stdin.end();
const code = await child.exited;
record({ kind: "exit", pid: child.pid, exitCode: code ?? 1 });
process.exitCode = code ?? 1;
`, { mode: 0o755 });

  const configPath = join(root, "config.json");
  await writeFile(configPath, JSON.stringify({
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    ownerId: fixture.ownerUid,
    allowedGroupIds: [],
    stateDir,
    sessionsDir: join(root, "sessions"),
    ompCommand: wrapper,
    ompExtraArgs: [
      "--model", "local-review/probe",
      "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools",
      "--config", join(root, "native.yml"),
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
    groupHistory: { enabled: false, maxMessages: 50 },
  }), { mode: 0o600 });

  const launched = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", configPath], {
    cwd: root,
    env: {
      HOME: root,
      TMPDIR: join(root, "tmp"),
      PATH: `${join(root, "bin")}:${dirname(gitPath)}:/usr/bin:/bin`,
      PI_CODING_AGENT_DIR: join(root, "agent"),
      PI_NO_TITLE: "1",
      LANG: "C",
      TZ: "UTC",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  child = launched;
  cliOutputs.push(Promise.all([new Response(launched.stdout).text(), new Response(launched.stderr).text()]).then(parts => parts.join("\n")));
  connection = await fixture.waitForConnection(1, 15_000);
  await wait("Channel CLI failed to authenticate to local Octo", () => connection!.ready);
  db = new Database(join(stateDir, "channel.sqlite"), { readonly: true });

  send(`/new ${canonicalRepo}`);
  await wait("The temporary Git repository was not bound", () => binding()?.cwd === canonicalRepo);
  const reviewStatusBefore = await git(["status", "--porcelain=v1"]);
  const deliveredFrom = fixture.sentMessages.length;
  const focus = "REVIEW_FOCUS_SENTINEL: inspect the staged, unstaged, and untracked changes in this repository";
  const mainSeedId = send("MAIN_SEED_USER_SENTINEL: preserve this only in the main session");
  await wait("The main task did not enter the held model request", () =>
    modelRequests.some(request => request.kind === "main-seed")
      && taskState(mainSeedId) === "started"
      && typeof binding()?.session_file === "string",
  );
  const originalBinding = binding();
  assert.ok(originalBinding?.session_file, "The main task did not establish a persistent OMP session");
  const originalSessionFile = await realpath(originalBinding.session_file);
  assert.equal(modelRequests.length, 1, "The main task made an unexpected number of model requests before review");
  const seedPrompt = (await audit()).find(row => row.kind === "prompt" && row.message?.includes("MAIN_SEED_USER_SENTINEL"));
  assert.ok(seedPrompt, "The main OMP worker did not receive the seed task prompt");

  send(`/review ${focus}`);
  await wait("The independent review model request was not issued", () => modelRequests.some(request => request.kind === "review"));
  const reviewRequest = modelRequests.find(request => request.kind === "review")!;
  for (const evidence of [
    canonicalRepo,
    head,
    focus,
    "src/tax.ts",
    "totalWithTax(subtotal: number, taxRate: number)",
    "return subtotal * (1 + taxRate);",
    "return subtotal * taxRate;",
    "REVIEW_STAGED_CHANGE_SENTINEL",
    "src/unstaged.ts", "REVIEW_UNSTAGED_CHANGE_SENTINEL",
    "src/tax.spec.ts", "REVIEW_UNTRACKED_CHANGE_SENTINEL",
    "assert.equal(totalWithTax(100, 0.1), 110",
  ]) {
    assert.ok(reviewRequest.text.includes(evidence), `Review model request omitted required change evidence: ${evidence}`);
  }
  assert.equal(reviewRequest.text.includes("MAIN_SEED_USER_SENTINEL"), false,
    "Review inherited unrelated main-chat history");
  assert.equal(modelRequests.length, 2, "The review did not use exactly one independent finite-model request");
  assert.equal(binding()?.session_id, originalBinding.session_id, "Review changed the main session ID");
  assert.equal(binding()?.session_file, originalBinding.session_file, "Review changed the main session file");
  assert.equal(await git(["status", "--porcelain=v1"]), reviewStatusBefore, "Review modified the temporary repository");

  reviewGate.resolve();
  await wait("The review result was not delivered to the owner", () =>
    fixture!.sentMessages.slice(deliveredFrom).map(sentText).join("\n").includes("REVIEW_DELIVERY_SENTINEL"),
  );
  assert.equal(taskState(mainSeedId), "started", "The review completed only after the main task was released");
  assert.equal(binding()?.session_id, originalBinding.session_id, "Review changed the main session ID");
  assert.equal(binding()?.session_file, originalBinding.session_file, "Review changed the main session file");

  const statusStart = fixture.sentMessages.length;
  send("/status");
  await wait("Status did not respond while the main task remained held after review", () => {
    const statusText = fixture!.sentMessages.slice(statusStart).map(sentText).join("\n");
    return statusText.includes(`session=${originalBinding.session_id}`) && statusText.includes("state=running");
  });
  assert.equal(taskState(mainSeedId), "started", "The held main task was cancelled or completed by /status");

  const promptRows = (await audit()).filter(row => row.kind === "prompt");
  const reviewPrompt = promptRows.find(row => row.message?.includes("REVIEW_FOCUS_SENTINEL"));
  assert.ok(reviewPrompt, "The review OMP worker did not receive the assembled review body");
  assert.notEqual(reviewPrompt.pid, seedPrompt.pid, "Review reused the main OMP worker process");
  const reviewSpawn = (await audit()).find(row => row.kind === "spawn" && row.pid === reviewPrompt.pid);
  assert.ok(reviewSpawn, "Could not associate the review prompt with its OMP process");
  assert.equal(reviewSpawn.args?.includes(originalSessionFile), false, "Review resumed the bound main session file");

  mainGate.resolve();
  await wait("The held main task did not complete after release", () => taskState(mainSeedId) === "completed");
  await clean();
  assert.equal(binding()?.session_id, originalBinding.session_id, "The main task changed the bound session ID");
  assert.equal(binding()?.session_file, originalBinding.session_file, "The main task changed the bound session file");
  const mainSessionAfterReview = await readFile(originalSessionFile, "utf8");
  assert.ok(mainSessionAfterReview.includes("MAIN_SEED_USER_SENTINEL"), "The main session did not retain its task");
  assert.equal(mainSessionAfterReview.includes("REVIEW_FOCUS_SENTINEL"), false, "The review focus entered the main session history");
  assert.equal(mainSessionAfterReview.includes("REVIEW_DELIVERY_SENTINEL"), false, "The review result entered the main session history");

  const mainAfterId = send("MAIN_AFTER_REVIEW_SENTINEL: continue the original main conversation");
  await wait("The main task did not continue after review", () => taskState(mainAfterId) === "completed");
  await clean();
  const afterRequest = modelRequests.find(request => request.kind === "main-after");
  assert.ok(afterRequest, "The post-review main task did not make its own model request");
  assert.ok(afterRequest.text.includes("MAIN_SEED_USER_SENTINEL"), "The original main session context was not retained");
  assert.equal(afterRequest.text.includes("REVIEW_FOCUS_SENTINEL"), false, "The review focus leaked into the main model session");
  assert.equal(afterRequest.text.includes("REVIEW_DELIVERY_SENTINEL"), false, "The review result leaked into the main model session");
  assert.equal(binding()?.session_id, originalBinding.session_id, "The post-review task changed the main session ID");
  assert.equal(binding()?.session_file, originalBinding.session_file, "The post-review task changed the main session file");
  assert.ok(fixture.sentMessages.slice(deliveredFrom).map(sentText).join("\n").includes("REVIEW_DELIVERY_SENTINEL"),
    "The review output was not delivered through Octo");
  assert.equal(await git(["status", "--porcelain=v1"]), reviewStatusBefore, "The review workload modified the temporary repository");
  assert.equal(modelError, undefined);
  assert.deepEqual(modelRequests.map(request => request.kind), ["main-seed", "review", "main-after"]);
  assert.equal(child.exitCode, null, "Channel CLI exited before graceful shutdown");
  child.kill("SIGTERM");
  assert.equal(await exit(child), 0, "Channel CLI did not stop cleanly on SIGTERM");
  succeeded = true;
  console.log(JSON.stringify({
    reviewIndependentOmpProcess: true,
    reviewIndependentFromMainSession: true,
    reviewBodyEvidence: ["repo", "HEAD", "tax-total defect", "staged", "unstaged", "untracked", "focus"],
    reviewResultDeliveredByOcto: true,
    statusResponsiveWhileMainHeldAfterReview: true,
    mainSessionPreserved: true,
    deterministicFixtureProvesReviewQuality: false,
    modelRequests: modelRequests.length,
  }));
} catch (error) {
  mainGate.resolve();
  reviewGate.resolve();
  console.error(JSON.stringify({
    modelRequests: modelRequests.map(request => request.kind),
    modelError: modelError ?? null,
    sentMessages: fixture?.sentMessages.slice(-10).map(message => ({ channelId: message.body.channel_id, text: sentText(message) })),
    tasks: db?.query("SELECT message_id, state, error_text FROM tasks ORDER BY rowid").all(),
  }));
  if (child && child.exitCode === null) child.kill("SIGTERM");
  if (child) await exit(child).catch(() => {});
  throw error;
} finally {
  mainGate.resolve();
  reviewGate.resolve();
  if (child && child.exitCode === null) child.kill("SIGKILL");
  if (child) await child.exited;
  db?.close();
  if (fixture) await fixture.close();
  stopModel?.();
  const outputs = await Promise.allSettled(cliOutputs);
  if (!succeeded) {
    const logs = outputs.flatMap(result => result.status === "fulfilled" ? [result.value] : []).join("\n");
    console.error(logs.replaceAll(fixture?.botToken ?? "\u0000", "[fixture-token]").slice(-12_000));
  }
  await rm(root, { recursive: true, force: true });
}
