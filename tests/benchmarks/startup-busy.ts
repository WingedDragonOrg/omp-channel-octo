import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { OmpWorker, WorkerProcess } from "../../src/contracts.ts";
import { createWorkerFactory } from "../../src/omp/index.ts";

// Offline real-RPC acceptance: every OMP process uses a temporary profile/session and a finite
// localhost model. The startup extension starts a session_start next-turn run while a user prompt
// is submitted to that exact resumed OMP session. The gated SSE proves acceptance and streamed text
// are observable before the user turn completes. No channel bot or external provider is involved.
// Run `bun tests/benchmarks/startup-busy.ts` (or set OMP_REAL_COMMAND to the OMP executable).
// Before the fix, expect a nonzero exit with a same-session rejection while startup remains gated.
// After the fix, expect exit 0 and JSON reporting acceptance, streamed output, exactly-once completion,
// parallelism, and cancellation.
const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
// Process identity checks use ps timestamps; re-enter with UTC set before Bun initializes libc.
if (process.env.TZ !== "UTC") {
  const child = Bun.spawn([process.execPath, import.meta.path], {
    env: { ...process.env, TZ: "UTC", LANG: "C" },
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
  });
  process.exit(await child.exited);
}
const omp = resolve(requestedOmp);
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-startup-busy-")));
const startupText = "STARTUP_AUTONOMOUS_A";
const userTaskA = "STARTUP_BUSY_USER_TASK_A";
const userTaskB = "STARTUP_BUSY_USER_TASK_B";
const cancelTask = "STARTUP_BUSY_CANCEL_TASK";
const requests: Array<{ kind: string; text: string }> = [];
const processes = new Map<number, WorkerProcess>();
const workers: OmpWorker[] = [];
let modelError: string | undefined;
let startupRequestActive = false;
let userARequestActive = false;
let stopModelServer: (() => void) | undefined;
let succeeded = false;


interface Gate {
  promise: Promise<void>;
  readonly released: boolean;
  release(): void;
}
function gate(): Gate {
  const deferred = Promise.withResolvers<void>();
  let released = false;
  return {
    promise: deferred.promise,
    get released() { return released; },
    release() {
      if (released) return;
      released = true;
      deferred.resolve();
    },
  };
}
const startupGate = gate();
const userAResponseGate = gate();
const cancelGate = gate();


function textOf(value: unknown): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(textOf).join("\n");
  if (!value || typeof value !== "object") return "";
  const record = value as Record<string, unknown>;
  if (typeof record.text === "string") return record.text;
  return textOf(record.content);
}
function requestText(body: Record<string, unknown>): string {
  return Array.isArray(body.messages) ? body.messages.map(textOf).join("\n") : "";
}
function chunk(text: string, index: number, finishReason: "stop" | null, usage = false): Record<string, unknown> {
  return {
    id: `startup-busy-${index}`,
    object: "chat.completion.chunk",
    created: 0,
    model: "probe",
    choices: [{ index: 0, delta: text ? { content: text } : {}, finish_reason: finishReason }],
    ...(usage ? { usage: { prompt_tokens: 8, completion_tokens: 4, total_tokens: 12 } } : {}),
  };
}
function sse(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
}
function response(text: string, index: number): Response {
  return new Response(
    sse(chunk(text, index, null)) + sse(chunk("", index, "stop", true)) + "data: [DONE]\n\n",
    { headers: { "content-type": "text/event-stream" } },
  );
}
function gatedUserResponse(text: string, index: number): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      controller.enqueue(encoder.encode(sse(chunk("USER_TASK_A_PROGRESS ", index, null))));
      try {
        await userAResponseGate.promise;
        controller.enqueue(encoder.encode(sse(chunk(text, index, null))));
        controller.enqueue(encoder.encode(sse(chunk("", index, "stop", true)) + "data: [DONE]\n\n"));
        controller.close();
      } finally {
        userARequestActive = false;
      }
    },
  });
  return new Response(stream, { headers: { "content-type": "text/event-stream" } });
}
async function waitFor(label: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (modelError !== undefined) throw new Error(`Finite localhost model failed: ${modelError}`);
    if (predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`${label} timed out after ${timeoutMs} ms`);
}
async function bounded<T>(promise: Promise<T>, label: string, timeoutMs = 45_000): Promise<T> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
  try {
    return await Promise.race([promise, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}
function beginPrompt(worker: OmpWorker, text: string): {
  promise: Promise<void>;
  finalText: string[];
  observedText: Array<{ text: string; final: boolean }>;
  readonly promptAccepted: boolean;
  readonly settled: boolean;
  readonly rejected: boolean;
  readonly error: unknown;
} {
  const finalText: string[] = [];
  const observedText: Array<{ text: string; final: boolean }> = [];
  let promptAccepted = false;
  let settled = false;
  let rejected = false;
  let error: unknown;
  const observed = worker.prompt(text, event => {
    if (event.type === "prompt_accepted") promptAccepted = true;
    if (event.type === "text") {
      observedText.push({ text: event.text, final: event.final });
      if (event.final) finalText.push(event.text);
    }
  }).then(
    () => { settled = true; },
    reason => {
      settled = true;
      rejected = true;
      error = reason;
      throw reason;
    },
  );
  const promise = bounded(observed, `OMP prompt ${text}`);
  // Keep a rejection observed while the other session is deliberately held at its model gate.
  void promise.catch(() => {});
  return {
    promise,
    finalText,
    observedText,
    get promptAccepted() { return promptAccepted; },
    get settled() { return settled; },
    get rejected() { return rejected; },
    get error() { return error; },
  };
}


const previousBunEnvironment = new Map<string, string | undefined>();
const previousProcessEnvironment = new Map<string, string | undefined>();
function setEnvironment(key: string, value: string | undefined): void {
  if (!previousBunEnvironment.has(key)) previousBunEnvironment.set(key, Bun.env[key]);
  if (!previousProcessEnvironment.has(key)) previousProcessEnvironment.set(key, process.env[key]);
  if (value === undefined) {
    delete Bun.env[key];
    delete process.env[key];
  } else {
    Bun.env[key] = value;
    process.env[key] = value;
  }
}
function restoreEnvironment(): void {
  for (const [key, value] of previousBunEnvironment) {
    if (value === undefined) delete Bun.env[key];
    else Bun.env[key] = value;
  }
  for (const [key, value] of previousProcessEnvironment) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
}
function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

try {
  const model = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      let body: Record<string, unknown>;
      try {
        body = await request.json() as Record<string, unknown>;
      } catch {
        modelError = `non-JSON request to ${new URL(request.url).pathname}`;
        return new Response("invalid local model request", { status: 400 });
      }
      const text = requestText(body);
      const kind = text.includes(cancelTask) ? "cancel"
        : text.includes(userTaskA) ? "user-a"
          : text.includes(userTaskB) ? "user-b"
            : text.includes(startupText) ? "startup-a"
              : undefined;
      if (!kind) {
        modelError = `request did not contain a finite scenario marker at ${new URL(request.url).pathname}`;
        return new Response(modelError, { status: 400 });
      }
      requests.push({ kind, text });
      if (kind === "startup-a" && !startupGate.released) {
        startupRequestActive = true;
        try {
          await startupGate.promise;
        } finally {
          startupRequestActive = false;
        }
      }
      if (kind === "cancel" && !cancelGate.released) await cancelGate.promise;
      const answer = kind === "startup-a" ? "AUTONOMOUS_STARTUP_A_COMPLETE"
        : kind === "user-a" ? "USER_TASK_A_COMPLETE"
          : kind === "user-b" ? "USER_TASK_B_COMPLETE"
            : "CANCEL_TASK_SHOULD_NOT_COMPLETE";
      if (kind === "user-a") {
        userARequestActive = true;
        return gatedUserResponse(answer, requests.length);
      }
      return response(answer, requests.length);
    },
  });
  stopModelServer = () => model.stop(true);

  const bin = join(root, "bin");
  const agentA = join(root, "profile-startup-a");
  const agentB = join(root, "profile-parallel-b");
  const cwdA = join(root, "cwd-a");
  const cwdB = join(root, "cwd-b");
  const sessionDirA = join(root, "sessions-a");
  const sessionDirB = join(root, "sessions-b");
  const localModelProvider = {
    baseUrl: `http://127.0.0.1:${model.port}/v1`,
    apiKey: "local-only-placeholder",
    api: "openai-completions",
    models: [{
      id: "probe",
      name: "Startup Busy Probe",
      reasoning: false,
      input: ["text"],
      contextWindow: 32768,
      maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }],
  };
  await mkdir(bin, { recursive: true });
  await mkdir(join(agentA, "extensions"), { recursive: true });
  await mkdir(agentB, { recursive: true });
  for (const path of [cwdA, cwdB, sessionDirA, sessionDirB]) await mkdir(path, { recursive: true });
  for (const command of ["octo-cli", "super"]) {
    await writeFile(join(bin, command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
  }
  await writeFile(join(agentA, "models.yml"), JSON.stringify({ providers: { "local-startup-busy": localModelProvider } }));
  await writeFile(join(agentB, "models.yml"), JSON.stringify({ providers: { "local-startup-busy": localModelProvider } }));
  await writeFile(join(agentA, "extensions", "startup-busy.ts"), `
export default function (pi) {
  pi.on("session_start", () => {
    pi.sendMessage(
      { customType: "startup-busy-acceptance", content: "${startupText}", display: false, details: {} },
      { deliverAs: "nextTurn", triggerTurn: true },
    );
  });
}
`);
  const nativeConfig = join(root, "native.yml");
  await writeFile(nativeConfig, "autoRetry:\n  enabled: false\n");

  // Isolate all profile discovery and credentials before any worker is spawned.
  for (const key of new Set([...Object.keys(Bun.env), ...Object.keys(process.env)])) setEnvironment(key, undefined);
  for (const [key, value] of Object.entries({
    HOME: root,
    TMPDIR: root,
    PATH: `${bin}:/usr/bin:/bin:/usr/sbin:/sbin`,
    LANG: "C",
    TZ: "UTC",
    PI_NO_TITLE: "1",
    XDG_CONFIG_HOME: join(root, "xdg-config"),
    XDG_DATA_HOME: join(root, "xdg-data"),
    XDG_CACHE_HOME: join(root, "xdg-cache"),
  })) setEnvironment(key, value);
  const factory = createWorkerFactory({
    ompCommand: omp,
    extraArgs: [
      "--model", "local-startup-busy/probe", "--no-skills", "--no-rules", "--no-context-files",
      "--no-lsp", "--no-tools", "--config", nativeConfig,
    ],
    startupTimeoutMs: 30_000,
    cancelTimeoutMs: 3_000,
  });
  async function createWorker(
    profile: string,
    cwd: string,
    sessionDir: string,
    sessionFile?: string,
  ): Promise<OmpWorker> {
    setEnvironment("PI_CODING_AGENT_DIR", profile);
    const worker = await factory.create({
      cwd,
      sessionDir,
      ...(sessionFile === undefined ? {} : { sessionFile }),
      onSpawn(processInfo) { processes.set(processInfo.pid, processInfo); },
    });
    workers.push(worker);
    return worker;
  }

  // Seed the real session without the extension, then resume that exact file under profile A.
  // This prevents a later native new_session from escaping the same-session startup race.
  const seed = await bounded(createWorker(agentB, cwdA, sessionDirA), "startup session seed creation");
  const seededSessionFile = seed.sessionFile;
  const seededSessionId = seed.sessionId;
  assert.ok(seededSessionFile.startsWith(`${root}/`), "seed session must be stored under the temporary root");
  await bounded(seed.stop(), "startup session seed stop");
  const workerA = await bounded(
    createWorker(agentA, cwdA, sessionDirA, seededSessionFile),
    "startup session resume",
  );
  assert.equal(workerA.sessionFile, seededSessionFile, "startup worker did not resume the seeded OMP session");
  assert.equal(workerA.sessionId, seededSessionId, "startup worker changed the resumed OMP session ID");
  assert.ok(await Bun.file(workerA.sessionFile).exists(), "real OMP did not resume the isolated startup session");
  await waitFor("session_start extension did not start its autonomous model turn", () =>
    startupRequestActive && requests.some(request => request.kind === "startup-a"),
  );

  const outputA = beginPrompt(workerA, userTaskA);
  assert.equal(startupGate.released, false, "autonomous model gate was released before the user task API call");
  assert.equal(startupRequestActive, true, "user task API call did not start while the autonomous turn was active");

  // Profile B has the same local model but no startup extension. Its independent session must
  // finish a real user turn while A's startup model request remains deliberately in flight.
  const workerB = await bounded(createWorker(agentB, cwdB, sessionDirB), "parallel session worker creation");
  assert.notEqual(workerA.sessionId, workerB.sessionId, "independent workers reused one OMP session ID");
  assert.notEqual(workerA.sessionFile, workerB.sessionFile, "independent workers reused one OMP session file");
  assert.ok(workerB.sessionFile.startsWith(`${root}/`), "parallel session must be stored under the temporary root");
  assert.ok(await Bun.file(workerB.sessionFile).exists(), "real OMP did not create the parallel isolated session");
  const outputB = beginPrompt(workerB, userTaskB);
  await bounded(outputB.promise, "parallel user task completion");
  assert.ok(outputB.finalText.some(text => text.includes("USER_TASK_B_COMPLETE")), "parallel session returned the wrong final result");
  assert.equal(startupGate.released, false, "the startup turn ended before the other session completed");
  assert.equal(startupRequestActive, true, "the startup model request was not still active during parallel completion");
  // Allow native RPC errors or the extension's preflight queue to settle while the
  // same-session startup model request is still held. A busy rejection here is the regression.
  await Bun.sleep(100);
  if (outputA.rejected) {
    throw new Error(`real OMP rejected a same-session user task before startup released: ${String(outputA.error)}`, {
      cause: outputA.error,
    });
  }
  assert.equal(outputA.settled, false, "same-session user task did not remain queued while startup was active");

  startupGate.release();
  try {
    await waitFor("user task did not stream text while its model response remained gated", () =>
      userARequestActive && outputA.observedText.some(event => !event.final),
    );
    assert.equal(outputA.promptAccepted, true, "worker did not emit prompt_accepted while the user model response was gated");
    assert.ok(outputA.observedText.some(event => !event.final), "worker emitted no non-final text before the user turn ended");
    assert.equal(outputA.settled, false, "user prompt settled before its gated model response was released");
    userAResponseGate.release();
    await bounded(outputA.promise, "startup-conflicted user task completion");
  } catch (error) {
    throw new Error(`real OMP user task failed after the startup turn: ${String(error)}`, { cause: error });
  }
  assert.ok(outputA.finalText.some(text => text.includes("USER_TASK_A_COMPLETE")), "user task did not complete correctly after the startup turn");
  assert.equal(requests.filter(request => request.kind === "user-a").length, 1, "the user input reached the model more than once");
  assert.equal(requests.filter(request => request.kind === "startup-a").length, 1, "session_start triggered more than one autonomous turn");

  const cancelledOutput = beginPrompt(workerB, cancelTask);
  await waitFor("cancellable user task did not reach the held localhost model request", () =>
    requests.some(request => request.kind === "cancel") && !cancelGate.released,
  );
  await bounded(workerB.abortTurn(), "OMP abort acknowledgement");
  let cancellationOutcome: "resolved" | "aborted";
  try {
    await bounded(cancelledOutput.promise, "cancelled prompt settlement");
    cancellationOutcome = "resolved";
  } catch (error) {
    if (!(error instanceof Error) || error.message !== "OMP prompt was aborted") throw error;
    cancellationOutcome = "aborted";
  }
  assert.equal(
    cancelledOutput.rejected,
    cancellationOutcome === "aborted",
    "cancelled prompt settled inconsistently with its outcome",
  );
  assert.ok(
    cancelledOutput.observedText.every(event => event.text === ""),
    `cancelled prompt emitted text after abort: ${JSON.stringify(cancelledOutput.observedText)}`,
  );
  assert.ok(
    cancelledOutput.finalText.every(text => text === ""),
    `cancelled prompt produced a final answer: ${JSON.stringify(cancelledOutput.finalText)}`,
  );
  assert.equal(requests.filter(request => request.kind === "cancel").length, 1, "cancelled user input reached the model more than once");

  succeeded = true;
  console.log(JSON.stringify({
    realOmp: omp,
    autonomousSessionStartTurn: true,
    startupAndUserTaskSameSession: true,
    userPromptAcceptedBeforeModelTurnCompleted: true,
    userTaskStreamedTextBeforeModelTurnCompleted: true,
    userTaskCompletedExactlyOnce: true,
    cancellation: true,
    cancellationOutcome,
    modelRequests: requests.length,
  }));
} catch (error) {
  console.error(JSON.stringify({
    modelError,
    modelRequests: requests.map(request => request.kind),
  }));
  console.error(error);
  throw error;
} finally {
  startupGate.release();
  userAResponseGate.release();
  let remainingWorkers: number[] = [];
  try {
    for (const worker of [...workers].reverse()) {
      try {
        await bounded(worker.stop(), "OMP worker cleanup", 10_000);
      } catch {
        // Kill only process groups that this benchmark recorded from its own worker spawn callbacks.
      }
    }
    for (const processInfo of processes.values()) {
      if (!isAlive(processInfo.pid)) continue;
      try {
        process.kill(-processInfo.pgid, "SIGKILL");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
    }
    const deadline = performance.now() + 5_000;
    while (performance.now() < deadline && [...processes.keys()].some(isAlive)) await Bun.sleep(10);
    remainingWorkers = [...processes.keys()].filter(isAlive);
  } finally {
    cancelGate.release();
    stopModelServer?.();
    try {
      restoreEnvironment();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
  if (succeeded) assert.deepEqual(remainingWorkers, [], "benchmark left a real OMP RPC child alive");
}
