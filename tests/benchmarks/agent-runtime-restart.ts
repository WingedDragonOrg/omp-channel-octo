import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, realpath, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type FixtureSentMessage,
} from "../fixtures/octo-protocol-server.ts";

type Deferred<T> = { readonly promise: Promise<T>; readonly settled: boolean; resolve(value: T): void };
type Gate = { readonly released: boolean; readonly promise: Promise<void>; release(): void };
type TextStep = {
  kind: "text";
  label: string;
  segments: readonly string[];
  gates: ReadonlyMap<number, Gate>;
  consumed: Deferred<void>[];
  finished: Deferred<void>;
};
type ToolStep = {
  kind: "tool";
  label: string;
  name: string;
  callId: string;
  args: Record<string, unknown>;
  gate: Gate;
  consumed: Deferred<void>;
  finished: Deferred<void>;
};
type ModelStep = TextStep | ToolStep;
type ModelRequest = { body: Record<string, unknown>; step: ModelStep };
type RunningChannel = { child: Bun.Subprocess; connection: FixtureConnection };
type TaskRow = {
  id: string;
  state: string;
  result_text: string | null;
  error_text: string | null;
  session_id: string;
  prompt_accepted: number;
};
type BindingRow = { session_id: string; session_file: string | null };
type WorkerRow = {
  id: string;
  pid: number;
  identity: string;
  state: string;
};
type SpawnAudit = { pid: number; mode: string; sessionDir: string };
type ActiveRun = { task: TaskRow; binding: BindingRow; worker: WorkerRow };

function deferred<T>(): Deferred<T> {
  const result = Promise.withResolvers<T>();
  let settled = false;
  return {
    promise: result.promise,
    get settled() { return settled; },
    resolve(value: T) {
      if (settled) return;
      settled = true;
      result.resolve(value);
    },
  };
}

function gate(): Gate {
  const waiting = deferred<void>();
  let released = false;
  return {
    promise: waiting.promise,
    get released() { return released; },
    release() {
      if (released) return;
      released = true;
      waiting.resolve();
    },
  };
}

function textStep(
  label: string,
  segments: readonly string[],
  gates: ReadonlyMap<number, Gate> = new Map(),
): TextStep {
  assert.ok(segments.length > 0, "A finite model text step needs at least one output segment");
  return { kind: "text", label, segments, gates, consumed: segments.map(() => deferred<void>()), finished: deferred<void>() };
}

function toolStep(
  label: string,
  name: string,
  callId: string,
  args: Record<string, unknown>,
  responseGate: Gate,
): ToolStep {
  return { kind: "tool", label, name, callId, args, gate: responseGate, consumed: deferred<void>(), finished: deferred<void>() };
}

function openAiChunk(
  id: string,
  delta: Record<string, unknown>,
  finishReason: "stop" | "tool_calls" | null,
  usage = false,
): Record<string, unknown> {
  return {
    id,
    object: "chat.completion.chunk",
    created: 0,
    model: "probe",
    choices: [{ index: 0, delta, finish_reason: finishReason }],
    ...(usage ? { usage: { prompt_tokens: 16, completion_tokens: 8, total_tokens: 24 } } : {}),
  };
}


function modelToolNames(body: Record<string, unknown>): string[] {
  if (!Array.isArray(body.tools)) return [];
  return body.tools.flatMap((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return [];
    const tool = value as Record<string, unknown>;
    if (typeof tool.name === "string") return [tool.name];
    if (!tool.function || typeof tool.function !== "object" || Array.isArray(tool.function)) return [];
    const name = (tool.function as Record<string, unknown>).name;
    return typeof name === "string" ? [name] : [];
  });
}

function messageText(value: unknown): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(messageText).join("\n");
  if (!value || typeof value !== "object") return "";
  const record = value as Record<string, unknown>;
  if (typeof record.text === "string") return record.text;
  return messageText(record.content);
}

function messageList(body: Record<string, unknown>): Record<string, unknown>[] {
  return Array.isArray(body.messages)
    ? body.messages.filter((value): value is Record<string, unknown> =>
      value !== null && typeof value === "object" && !Array.isArray(value))
    : [];
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
    throw error;
  }
}

async function exited(child: Bun.Subprocess, timeoutMs = 15_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = Promise.withResolvers<never>();
  try {
    timer = setTimeout(() => timeout.reject(new Error(`CLI pid ${child.pid} did not exit within ${timeoutMs} ms`)), timeoutMs);
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

function auditRows(path: string): SpawnAudit[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split("\n").filter(Boolean).map((line) => {
    const [kind, rawPid, mode, sessionDir, extra] = line.split("\t");
    assert.equal(kind, "spawn", "Malformed isolated OMP spawn audit");
    assert.equal(extra, undefined, "Unexpected field in isolated OMP spawn audit");
    const pid = Number(rawPid);
    assert.ok(Number.isSafeInteger(pid) && pid > 1, "Invalid OMP PID in isolated spawn audit");
    assert.ok(mode && sessionDir, "Incomplete isolated OMP spawn audit");
    return { pid, mode, sessionDir };
  });
}

function fixtureText(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}

/**
 * End-to-end hot-restart acceptance: installed OMP, real channel CLI, and only loopback fixtures.
 * The model gates expose the channel-offline boundary; process/session assertions ensure the test
 * cannot pass by cold-restarting OMP or replaying the task prompt.
 *
 * Run with `bun tests/benchmarks/agent-runtime-restart.ts`; set OMP_REAL_COMMAND to select an
 * explicit installed OMP. This is opt-in and never contacts the resident bot or a model provider.
 */
export async function runAgentRuntimeRestartBenchmark(): Promise<void> {
  const requestedOmp = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
  assert.ok(requestedOmp, "Install OMP or set OMP_REAL_COMMAND to its executable");
  const omp = await realpath(resolve(requestedOmp));
  const checkout = resolve(import.meta.dir, "../..");
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-agent-runtime-restart-")));
  const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  const children: Bun.Subprocess[] = [];
  const childLogs: Promise<string>[] = [];
  const heldGates = new Set<Gate>();
  const modelRequests: ModelRequest[] = [];
  let modelSteps: ModelStep[] = [];
  let modelError: string | undefined;
  let modelServer: ReturnType<typeof Bun.serve> | undefined;
  let db: Database | undefined;
  let succeeded = false;
  const auditPath = join(root, "omp-spawn-audit.tsv");
  const channelId = `${fixture.ownerUid}@${fixture.botUid}`;

  function setSteps(...steps: ModelStep[]): void {
    assert.equal(modelSteps.length, 0, "The previous finite model script was not fully consumed");
    modelSteps = steps;
    for (const step of steps) {
      if (step.kind === "text") {
        for (const held of step.gates.values()) heldGates.add(held);
      } else {
        heldGates.add(step.gate);
      }
    }
  }

  function requestsFor(marker: string): ModelRequest[] {
    return modelRequests.filter(({ body }) => JSON.stringify(body.messages ?? []).includes(marker));
  }

  async function wait(label: string, predicate: () => boolean, timeoutMs = 45_000): Promise<void> {
    await waitForFixture(() => {
      if (modelError !== undefined) throw new Error(`Finite localhost model failed: ${modelError}`);
      return predicate() ? true : undefined;
    }, timeoutMs, label);
  }

  function task(messageId: string): TaskRow | null {
    return db!.query(
      "SELECT id, state, result_text, error_text, session_id, prompt_accepted FROM tasks WHERE bot_uid = ? AND message_id = ?",
    ).get(fixture.botUid, messageId) as TaskRow | null;
  }

  function binding(): BindingRow | null {
    return db!.query(
      "SELECT session_id, session_file FROM chats WHERE bot_uid = ? AND channel_type = 1 AND channel_id = ?",
    ).get(fixture.botUid, fixture.ownerUid) as BindingRow | null;
  }

  function workerForTask(taskId: string): WorkerRow | null {
    return db!.query(
      "SELECT id, pid, identity, state FROM worker_ledger WHERE bot_uid = ? AND id = ?",
    ).get(fixture.botUid, taskId) as WorkerRow | null;
  }

  function sessionFileBelongsToSpawn(bindingRow: BindingRow, spawn: SpawnAudit): boolean {
    if (!bindingRow.session_id || !bindingRow.session_file) return false;
    const fileFromSpawnDir = relative(resolve(spawn.sessionDir), resolve(bindingRow.session_file));
    return fileFromSpawnDir !== ""
      && fileFromSpawnDir !== ".."
      && !fileFromSpawnDir.startsWith(`..${sep}`);
  }

  async function activeRun(messageId: string): Promise<ActiveRun> {
    let result: ActiveRun | undefined;
    await wait(`Task ${messageId} did not acquire one live OMP worker`, () => {
      const currentTask = task(messageId);
      if (!currentTask || currentTask.state !== "started" || currentTask.prompt_accepted !== 1) return false;
      const currentBinding = binding();
      if (!currentBinding?.session_file) return false;
      const worker = workerForTask(currentTask.id);
      if (!worker || worker.state !== "running") return false;
      const spawn = auditRows(auditPath).find((row) => row.pid === worker.pid);
      if (!spawn || !sessionFileBelongsToSpawn(currentBinding, spawn)) return false;
      result = { task: currentTask, binding: currentBinding, worker };
      return true;
    });
    assert.ok(result);
    assert.ok(alive(result.worker.pid), `OMP worker ${result.worker.pid} is not alive`);
    assert.equal(result.worker.id, result.task.id, "Worker ledger is not keyed by its task identity");
    const spawn = auditRows(auditPath).find((row) => row.pid === result!.worker.pid);
    assert.ok(spawn, "Worker ledger PID is not the real OMP process from the exec-preserving audit shim");
    assert.equal(spawn.mode, "rpc", "The accepted task did not use the real OMP RPC process");
    assert.ok(spawn.sessionDir.startsWith(join(root, "sessions")), "OMP session escaped the isolated sessions directory");
    assert.ok(sessionFileBelongsToSpawn(result.binding, spawn), "The chat's committed native session file is outside this OMP process's session directory");
    return result;
  }

  async function assertSameActiveRun(messageId: string, expected: ActiveRun): Promise<ActiveRun> {
    let current: ActiveRun | undefined;
    await wait(`Channel reconnect did not reattach task ${messageId} to its original OMP worker`, () => {
      const currentTask = task(messageId);
      const currentBinding = binding();
      if (!currentTask || currentTask.state !== "started" || currentTask.id !== expected.task.id) return false;
      if (
        !currentBinding
        || currentBinding.session_id !== expected.binding.session_id
        || currentBinding.session_file !== expected.binding.session_file
      ) return false;
      const worker = workerForTask(expected.task.id);
      if (!worker || worker.state !== "running" || worker.pid !== expected.worker.pid) return false;
      const spawn = auditRows(auditPath).find((row) => row.pid === worker.pid);
      if (!spawn || !sessionFileBelongsToSpawn(currentBinding, spawn)) return false;
      current = { task: currentTask, binding: currentBinding, worker };
      return true;
    });
    assert.ok(current);
    assert.equal(current.worker.id, expected.worker.id, "Reconnect replaced the worker/run ledger identity");
    assert.equal(current.worker.identity, expected.worker.identity, "Reconnect changed the OMP process identity");
    assert.ok(alive(expected.worker.pid), "The original OMP PID exited during channel restart");
    return current;
  }


  function send(connection: FixtureConnection, body: string, messageId: string): void {
    fixture.sendInbound(connection, {
      fromUid: fixture.ownerUid,
      channelId,
      channelType: 1,
      text: body,
      messageId,
    });
  }

  async function launch(): Promise<RunningChannel> {
    const child = Bun.spawn([process.execPath, join(checkout, "src/cli.ts"), "run", "--config", join(root, "config.json")], {
      cwd: root,
      env: {
        HOME: root,
        TMPDIR: root,
        PATH: `${join(root, "bin")}:${dirname(process.execPath)}:/usr/bin:/bin:/usr/sbin:/sbin`,
        LANG: "C",
        TZ: "UTC",
        PI_CODING_AGENT_DIR: join(root, "agent"),
        PI_NO_TITLE: "1",
        XDG_CONFIG_HOME: join(root, "xdg-config"),
        XDG_DATA_HOME: join(root, "xdg-data"),
        XDG_CACHE_HOME: join(root, "xdg-cache"),
      },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    children.push(child);
    childLogs.push(Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]).then((parts) => parts.join("\n")));
    const connection = await fixture.waitForConnection(children.length, 15_000);
    await wait(`Channel CLI pid ${child.pid} did not authenticate`, () => connection.ready && child.exitCode === null, 15_000);
    return { child, connection };
  }

  async function stop(channel: RunningChannel, signal: "SIGTERM" | "SIGKILL"): Promise<void> {
    if (channel.child.exitCode !== null) return;
    channel.child.kill(signal);
    const code = await exited(channel.child);
    if (signal === "SIGTERM") assert.equal(code, 0, "Graceful channel shutdown did not exit cleanly");
  }

  async function waitForStepConsumed(step: TextStep | ToolStep, index = 0): Promise<void> {
    const consumed = step.kind === "text" ? step.consumed[index] : step.consumed;
    assert.ok(consumed, `Missing consumed marker for model step ${step.label}`);
    await wait(`OMP did not consume the scripted response chunk for ${step.label}`, () => consumed.settled);
  }
  async function waitForModelStepFinished(step: ModelStep): Promise<void> {
    await wait(`Local model response was not fully consumed for ${step.label}`, () => step.finished.settled);
  }

  function assertPromptNotReplayed(marker: string, expectedRequests: number): void {
    const matching = requestsFor(marker);
    assert.equal(matching.length, expectedRequests, `${marker}: unexpected local-model request count`);
    for (const request of matching) {
      const userMessages = messageList(request.body).filter((message) =>
        message.role === "user" && messageText(message.content).includes(marker));
      assert.equal(userMessages.length, 1, `${marker}: original task prompt was duplicated in one OMP session`);
    }
  }

  function fixtureOutputText(): string {
    const created = fixture.sentMessages.map(fixtureText);
    const edited = fixture.messageEdits.flatMap(({ body }) => {
      const encoded = body.content_edit;
      if (typeof encoded !== "string") return [];
      try {
        // Octo type-1 edits carry JSON { type: 1, content: string } (see octo-transport.test.ts).
        const contentEdit = JSON.parse(encoded) as Record<string, unknown>;
        return contentEdit.type === 1 && typeof contentEdit.content === "string"
          ? [contentEdit.content]
          : [];
      } catch {
        return [];
      }
    });
    return [...created, ...edited].join("\n");
  }

  async function waitForCompletion(messageId: string, resultMarker: string): Promise<TaskRow> {
    await wait(`Task ${messageId} did not complete with ${resultMarker}`, () => {
      const current = task(messageId);
      return current?.state === "completed"
        && current.result_text?.includes(resultMarker) === true
        && fixtureOutputText().includes(resultMarker);
    });
    return task(messageId)!;
  }


  type RuntimeIdentity = { path: string; botUid: string; runtimeId: string; pid: number; processIdentity: string };

  async function runtimeManifest(): Promise<RuntimeIdentity | undefined> {
    const runtimeDir = join(root, "state", "agent-runtime");
    let entries;
    try {
      entries = await readdir(runtimeDir, { withFileTypes: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
    const paths = entries.filter((entry) => entry.isDirectory())
      .map((entry) => join(runtimeDir, entry.name, "runtime.json"))
      .filter(existsSync);
    assert.ok(paths.length <= 1, "The isolated state directory contains multiple agent runtimes");
    if (paths.length === 0) return undefined;
    const path = paths[0]!;
    const value = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
    assert.equal(value.botUid, fixture.botUid, "Runtime manifest belongs to a different fixture Bot");
    assert.ok(typeof value.runtimeId === "string" && value.runtimeId.length > 0);
    assert.ok(typeof value.pid === "number" && Number.isSafeInteger(value.pid) && value.pid > 1);
    assert.ok(typeof value.processIdentity === "string" && value.processIdentity.length > 0);
    return {
      path,
      botUid: value.botUid as string,
      runtimeId: value.runtimeId as string,
      pid: value.pid as number,
      processIdentity: value.processIdentity as string,
    };
  }

  let runtimeIdentity: RuntimeIdentity | undefined;

  async function assertRuntimeSurvived(): Promise<void> {
    assert.ok(runtimeIdentity, "The first real task did not start an isolated agent runtime");
    const current = await runtimeManifest();
    assert.ok(current, "Channel restart removed the isolated runtime manifest");
    assert.equal(current.runtimeId, runtimeIdentity.runtimeId, "Channel restart replaced the agent runtime");
    assert.equal(current.pid, runtimeIdentity.pid, "Channel restart spawned another runtime process");
    assert.equal(await processIdentity(runtimeIdentity.pid), runtimeIdentity.processIdentity, "The original runtime process exited during channel restart");
    assert.equal(current.path, runtimeIdentity.path, "Channel restart changed the isolated runtime manifest path");
  }

  async function processIdentity(pid: number): Promise<string | undefined> {
    const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "lstart="], {
      stdout: "pipe", stderr: "ignore", env: { ...process.env, TZ: "UTC" },
    });
    const [output, code] = await Promise.all([new Response(ps.stdout).text(), ps.exited]);
    const started = output.trim();
    return code === 0 && started.length > 0 ? `pid=${pid};start=${started}` : undefined;
  }

  async function cleanupRuntime(): Promise<void> {
    const manifest = await runtimeManifest();
    if (!manifest) return;
    if (await processIdentity(manifest.pid) !== manifest.processIdentity) return;
    const ps = Bun.spawn(["/bin/ps", "-p", String(manifest.pid), "-o", "command="], { stdout: "pipe", stderr: "ignore" });
    const command = await new Response(ps.stdout).text();
    await ps.exited;
    assert.ok(command.includes(join(root, "state", "agent-runtime")), "Refusing to signal a PID without this fixture's runtime command path");
    process.kill(manifest.pid, "SIGTERM");
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline && await processIdentity(manifest.pid) === manifest.processIdentity) {
      await Bun.sleep(20);
    }
    if (await processIdentity(manifest.pid) === manifest.processIdentity) {
      const currentCommand = Bun.spawn(["/bin/ps", "-p", String(manifest.pid), "-o", "command="], { stdout: "pipe", stderr: "ignore" });
      const current = await new Response(currentCommand.stdout).text();
      await currentCommand.exited;
      if (current.includes(join(root, "state", "agent-runtime"))) process.kill(manifest.pid, "SIGKILL");
      const killDeadline = Date.now() + 3_000;
      while (Date.now() < killDeadline && await processIdentity(manifest.pid) === manifest.processIdentity) {
        await Bun.sleep(20);
      }
      assert.notEqual(await processIdentity(manifest.pid), manifest.processIdentity, "Isolated runtime remained alive after emergency cleanup");
    }
  }

  async function cleanupFixtureProcesses(): Promise<void> {
    // OMP children are ours only when their command line still names this benchmark's isolated
    // session directory. This is emergency cleanup, not a liveness assertion.
    for (const { pid } of auditRows(auditPath)) {
      if (!alive(pid)) continue;
      const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "command="], { stdout: "pipe", stderr: "ignore" });
      const command = await new Response(ps.stdout).text();
      await ps.exited;
      if (!command.includes(join(root, "sessions"))) continue;
      try { process.kill(pid, "SIGTERM"); } catch { /* It may exit after ps. */ }
      const deadline = Date.now() + 3_000;
      while (alive(pid) && Date.now() < deadline) await Bun.sleep(20);
      if (alive(pid)) {
        try { process.kill(pid, "SIGKILL"); } catch { /* The process may have exited. */ }
      }
    }
  }

  try {
    await mkdir(join(root, "bin"), { recursive: true });
    await mkdir(join(root, "agent"), { recursive: true });
    await mkdir(join(root, "cwd"), { recursive: true });
    await writeFile(auditPath, "");
    for (const command of ["octo-cli", "super"]) {
      await writeFile(join(root, "bin", command), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
    }
    modelServer = Bun.serve({
      hostname: "127.0.0.1",
      // The real OMP client is held at test checkpoints; Bun's 10s default resets those SSE streams.
      idleTimeout: 255,
      port: 0,
      async fetch(request) {
        const url = new URL(request.url);
        if (request.method !== "POST" || url.pathname !== "/v1/chat/completions") {
          modelError = `Unexpected local model request ${request.method} ${url.pathname}`;
          return new Response(modelError, { status: 404 });
        }
        let body: Record<string, unknown>;
        try {
          body = await request.json() as Record<string, unknown>;
        } catch {
          modelError = "OMP sent invalid JSON to the isolated localhost model";
          return new Response(modelError, { status: 400 });
        }
        const step = modelSteps.shift();
        if (!step) {
          modelError = `Finite model script has no reply for request ${modelRequests.length + 1}`;
          return new Response(modelError, { status: 400 });
        }
        if (step.kind === "tool" && !modelToolNames(body).includes(step.name)) {
          modelError = `OMP request omitted expected host tool ${step.name} for ${step.label}`;
          return new Response(modelError, { status: 400 });
        }
        modelRequests.push({ body, step });
        const gates = step.kind === "text" ? [...step.gates.values()] : [step.gate];
        request.signal.addEventListener("abort", () => gates.forEach((held) => held.release()), { once: true });

        const id = `agent-runtime-${modelRequests.length}`;
        const chunks = step.kind === "text"
          ? [
            ...step.segments.map((content, index) => openAiChunk(
              id,
              { ...(index === 0 ? { role: "assistant" } : {}), content },
              null,
            )),
            openAiChunk(id, {}, "stop", true),
          ]
          : [
            openAiChunk(id, {
              role: "assistant",
              tool_calls: [{
                index: 0,
                id: step.callId,
                type: "function",
                function: { name: step.name, arguments: JSON.stringify(step.args) },
              }],
            }, null),
            openAiChunk(id, {}, "tool_calls", true),
          ];
        let chunkIndex = 0;
        const waitedGates = new Set<number>();
        const encoder = new TextEncoder();
        const stream = new ReadableStream<Uint8Array>({
          async pull(controller) {
            if (step.kind === "text" && chunkIndex > 0 && chunkIndex <= step.segments.length) {
              step.consumed[chunkIndex - 1]?.resolve();
            } else if (step.kind === "tool" && chunkIndex === chunks.length) {
              step.consumed.resolve();
            }
            const held = step.kind === "text" ? step.gates.get(chunkIndex) : chunkIndex === 0 ? step.gate : undefined;
            if (held && !waitedGates.has(chunkIndex)) {
              waitedGates.add(chunkIndex);
              await held.promise;
            }
            if (chunkIndex < chunks.length) {
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunks[chunkIndex++])}\n\n`));
              return;
            }
            controller.enqueue(encoder.encode("data: [DONE]\n\n"));
            controller.close();
            step.finished.resolve();
          },
          cancel() {
            gates.forEach((held) => held.release());
          },
        }, { highWaterMark: 0 });
        return new Response(stream, {
          headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
        });
      },
    });
    assert.ok(modelServer.port, "Local model fixture failed to bind");

    const ompWrapper = join(root, "omp-exec-wrapper.sh");
    await writeFile(ompWrapper, `#!/bin/sh
mode=
session_dir=
expect_mode=0
expect_session_dir=0
for arg do
  if [ "$expect_mode" -eq 1 ]; then mode=$arg; expect_mode=0; fi
  if [ "$expect_session_dir" -eq 1 ]; then session_dir=$arg; expect_session_dir=0; fi
  case "$arg" in
    --mode) expect_mode=1 ;;
    --session-dir) expect_session_dir=1 ;;
  esac
done
printf 'spawn\\t%s\\t%s\\t%s\\n' "$$" "$mode" "$session_dir" >> ${shellQuote(auditPath)}
exec ${shellQuote(omp)} "$@"
`, { mode: 0o755 });

    const provider = {
      baseUrl: `http://127.0.0.1:${modelServer.port}/v1`,
      apiKey: "local-only-placeholder",
      api: "openai-completions",
      models: [{
        id: "probe",
        name: "Agent Runtime Restart Probe",
        reasoning: false,
        input: ["text"],
        contextWindow: 32768,
        maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }],
    };
    await writeFile(join(root, "agent", "models.yml"), JSON.stringify({ providers: { "local-agent-runtime": provider } }));
    const nativeConfigPath = join(root, "native.yml");
    await writeFile(nativeConfigPath, "autoRetry:\n  enabled: false\n");
    await writeFile(join(root, "config.json"), JSON.stringify({
      apiUrl: fixture.apiUrl,
      botToken: fixture.botToken,
      ownerId: fixture.ownerUid,
      allowedGroupIds: [],
      stateDir: join(root, "state"),
      sessionsDir: join(root, "sessions"),
      ompCommand: ompWrapper,
      ompExtraArgs: [
        "--model", "local-agent-runtime/probe",
        "--no-skills", "--no-rules", "--no-context-files", "--no-lsp", "--no-tools",
        "--config", nativeConfigPath,
      ],
      startupTimeoutMs: 30_000,
      cancelTimeoutMs: 3_000,
      groupHistory: { enabled: false, maxMessages: 50 },
    }), { mode: 0o600 });

    let channel = await launch();
    db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });

    // A real response chunk is consumed by OMP before SIGKILL. The second chunk is consumed while
    // no channel exists; a second graceful disconnect and reconnect must still retain the same PID.
    const outputGate1 = gate();
    const outputGate2 = gate();
    const outputMarker = "AGENT_RUNTIME_HOT_RESTART_OUTPUT_TASK";
    const offlineText = "AGENT_RUNTIME_OUTPUT_CONSUMED_WHILE_CHANNEL_OFFLINE";
    const outputResult = "AGENT_RUNTIME_OUTPUT_FINAL_RESULT";
    const outputStep = textStep("output across repeated reconnect", [
      "AGENT_RUNTIME_OUTPUT_BEFORE_RESTART",
      offlineText,
      outputResult,
    ], new Map([[1, outputGate1], [2, outputGate2]]));
    setSteps(outputStep);
    const outputMessageId = "810001";
    send(channel.connection, outputMarker, outputMessageId);
    await wait(`Local OMP never received ${outputMarker}`, () => requestsFor(outputMarker).length === 1);
    const outputRun = await activeRun(outputMessageId);
    runtimeIdentity = await runtimeManifest();
    assert.ok(runtimeIdentity, "The channel CLI did not launch an isolated agent runtime");
    assert.equal(await processIdentity(runtimeIdentity.pid), runtimeIdentity.processIdentity, "Runtime manifest PID identity did not match the operating system");
    assert.ok(alive(runtimeIdentity.pid));
    assert.notEqual(runtimeIdentity.pid, outputRun.worker.pid, "OMP must run under the independent runtime, not as the runtime process");
    await waitForStepConsumed(outputStep, 0);
    const outputSpawnCount = auditRows(auditPath).length;
    await stop(channel, "SIGKILL");
    assert.ok(alive(outputRun.worker.pid), "SIGKILL of the channel also killed its OMP child");
    assert.equal(task(outputMessageId)?.state, "started", "Channel crash settled the live task before reattachment");
    await assertRuntimeSurvived();
    outputGate1.release();
    await waitForStepConsumed(outputStep, 1);
    assert.ok(alive(outputRun.worker.pid), "OMP exited before the offline output could be reattached");
    channel = await launch();
    await assertSameActiveRun(outputMessageId, outputRun);
    await assertRuntimeSurvived();
    await stop(channel, "SIGTERM");
    assert.ok(alive(outputRun.worker.pid), "Graceful channel shutdown killed the detached OMP process");
    channel = await launch();
    await assertSameActiveRun(outputMessageId, outputRun);
    await assertRuntimeSurvived();
    assert.equal(auditRows(auditPath).length, outputSpawnCount, "Reconnect spawned a second OMP process for the active session");
    outputGate2.release();
    const completedOutput = await waitForCompletion(outputMessageId, outputResult);
    assert.ok(completedOutput.result_text?.includes(offlineText), "The output consumed while the channel was offline was lost");
    assertPromptNotReplayed(outputMarker, 1);
    assert.equal(task(outputMessageId)?.id, outputRun.task.id, "Reconnect replaced the active task identity");
    assert.deepEqual(binding(), outputRun.binding, "Reconnect changed the bound OMP session");
    console.log(`CASE output-and-repeated-reconnect ${JSON.stringify({
      taskId: outputRun.task.id,
      sessionId: outputRun.task.session_id,
      ompPid: outputRun.worker.pid,
      channelRestarts: 2,
      offlineOutput: true,
      completed: true,
      promptRequests: requestsFor(outputMarker).length,
    })}`);
    // The full final model response is consumed while the channel process is absent. Reattach must
    // recover that terminal output without sending the original prompt to OMP again.
    const offlineCompletionGate = gate();
    const offlineCompletionMarker = "AGENT_RUNTIME_TASK_COMPLETED_WHILE_CHANNEL_OFFLINE";
    const offlineCompletionStep = textStep("completion while channel is offline", [
      "AGENT_RUNTIME_OFFLINE_COMPLETION_CHECKPOINT",
      offlineCompletionMarker,
    ], new Map([[1, offlineCompletionGate]]));
    setSteps(offlineCompletionStep);
    const offlineCompletionMessageId = "810007";
    const offlineCompletionPrompt = "AGENT_RUNTIME_OFFLINE_COMPLETION_TASK";
    send(channel.connection, offlineCompletionPrompt, offlineCompletionMessageId);
    await wait(`Local OMP never received ${offlineCompletionPrompt}`, () =>
      requestsFor(offlineCompletionPrompt).length === 1);
    const offlineCompletionRun = await activeRun(offlineCompletionMessageId);
    const offlineCompletionSpawnCount = auditRows(auditPath).length;
    await waitForStepConsumed(offlineCompletionStep, 0);
    await stop(channel, "SIGKILL");
    assert.ok(alive(offlineCompletionRun.worker.pid), "Channel crash killed OMP before the offline completion");
    await assertRuntimeSurvived();
    offlineCompletionGate.release();
    await waitForStepConsumed(offlineCompletionStep, 1);
    await waitForModelStepFinished(offlineCompletionStep);
    assert.ok(!fixtureOutputText().includes(offlineCompletionMarker), "The offline result was delivered before a channel reconnected");
    channel = await launch();
    await assertRuntimeSurvived();
    const offlineCompleted = await waitForCompletion(offlineCompletionMessageId, offlineCompletionMarker);
    assert.equal(offlineCompleted.id, offlineCompletionRun.task.id, "Reconnect replaced the completed task identity");
    assert.deepEqual(binding(), offlineCompletionRun.binding, "Offline completion changed the bound OMP session");
    assertPromptNotReplayed(offlineCompletionPrompt, 1);
    assert.equal(auditRows(auditPath).length, offlineCompletionSpawnCount, "Reattach replayed completed work in a replacement OMP process");
    console.log(`CASE complete-while-channel-offline ${JSON.stringify({
      taskId: offlineCompletionRun.task.id,
      sessionId: offlineCompletionRun.task.session_id,
      ompPid: offlineCompletionRun.worker.pid,
      completedResponseConsumedWhileOffline: true,
      resultDeliveredAfterReconnect: true,
      promptRequests: requestsFor(offlineCompletionPrompt).length,
    })}`);


    // Native channel_control.status is a read-only host tool. Its model response is held until the
    // old channel has exited, forcing the runtime to retain and deliver that exact call after attach.
    const hostGate = gate();
    const hostMarker = "AGENT_RUNTIME_HOST_TOOL_TASK";
    const hostCallId = "agent-runtime-host-status-call";
    const hostStep = toolStep("offline host tool", "channel_control", hostCallId, { action: "status" }, hostGate);
    const hostResultMarker = "AGENT_RUNTIME_HOST_TOOL_RESULT_CONSUMED";
    setSteps(hostStep, textStep("host tool result", [hostResultMarker]));
    const hostMessageId = "810002";
    send(channel.connection, hostMarker, hostMessageId);
    await wait(`Local OMP never requested host tool for ${hostMarker}`, () => requestsFor(hostMarker).length === 1);
    const hostRun = await activeRun(hostMessageId);
    const hostSpawnCount = auditRows(auditPath).length;
    await stop(channel, "SIGKILL");
    assert.ok(alive(hostRun.worker.pid), "Channel crash killed OMP before its host call was released");
    hostGate.release();
    await waitForStepConsumed(hostStep);
    await waitForModelStepFinished(hostStep);
    channel = await launch();
    await assertSameActiveRun(hostMessageId, hostRun);
    await assertRuntimeSurvived();
    const hostCompleted = await waitForCompletion(hostMessageId, hostResultMarker);
    assertPromptNotReplayed(hostMarker, 2);
    assert.equal(auditRows(auditPath).length, hostSpawnCount, "Recovering the host tool spawned another OMP process");
    const hostFollowup = requestsFor(hostMarker).at(-1)!;
    const hostToolResults = messageList(hostFollowup.body).filter((message) =>
      message.role === "tool" && message.tool_call_id === hostCallId);
    assert.equal(hostToolResults.length, 1, "The original channel_control call did not return to its original OMP toolCallId");
    assert.ok(messageText(hostToolResults[0]!.content).includes("pid"), "The host tool result was empty or not delivered to OMP");
    assert.ok(hostCompleted.result_text?.includes(hostResultMarker));
    console.log(`CASE host-tool-after-reconnect ${JSON.stringify({
      taskId: hostRun.task.id,
      sessionId: hostRun.task.session_id,
      ompPid: hostRun.worker.pid,
      toolCallId: hostCallId,
      completed: true,
    })}`);

    // ask is an independent host-tool path. Reconnect twice while the original question is pending,
    // then answer the same durable question and verify the result returns under the native call ID.
    const askGate = gate();
    const askMarker = "AGENT_RUNTIME_ASK_TASK";
    const askCallId = "agent-runtime-ask-call";
    const askStep = toolStep("offline ask", "ask", askCallId, {
      questions: [{ id: "runtime-restart-question", question: "请回答本地运行时重连验收问题。" }],
    }, askGate);
    const askResultMarker = "AGENT_RUNTIME_ASK_ANSWER_CONSUMED";
    setSteps(askStep, textStep("ask answer result", [askResultMarker]));
    const askMessageId = "810003";
    send(channel.connection, askMarker, askMessageId);
    await wait(`Local OMP never requested ask for ${askMarker}`, () => requestsFor(askMarker).length === 1);
    const askRun = await activeRun(askMessageId);
    const askSpawnCount = auditRows(auditPath).length;
    await stop(channel, "SIGTERM");
    assert.ok(alive(askRun.worker.pid), "Graceful channel shutdown killed OMP before the ask call was released");
    askGate.release();
    await waitForStepConsumed(askStep);
    await waitForModelStepFinished(askStep);
    channel = await launch();
    await assertSameActiveRun(askMessageId, askRun);
    await assertRuntimeSurvived();

    type QuestionRow = { id: string; state: string; answer: string | null };
    const questions = (): QuestionRow[] => db!.query(
      "SELECT id, state, answer FROM questions WHERE task_id = ? ORDER BY created_at, rowid",
    ).all(askRun.task.id) as QuestionRow[];
    await wait("The reconnected ask tool did not create its pending question", () =>
      questions().length === 1 && questions()[0]?.state === "pending");
    const originalQuestionId = questions()[0]!.id;
    await stop(channel, "SIGKILL");
    assert.ok(alive(askRun.worker.pid), "Repeated channel reconnect killed the OMP ask waiter");
    channel = await launch();
    await assertSameActiveRun(askMessageId, askRun);
    await assertRuntimeSurvived();
    assert.deepEqual(questions().map(({ id, state }) => ({ id, state })), [
      { id: originalQuestionId, state: "pending" },
    ], "Repeated reconnect replaced or duplicated the waiting ask question");
    const answer = "AGENT_RUNTIME_ASK_LOCAL_ANSWER";
    send(channel.connection, answer, "810004");
    await wait("The original pending ask question was not answered", () =>
      questions().length === 1 && questions()[0]?.id === originalQuestionId
        && questions()[0]?.state === "answered" && questions()[0]?.answer === answer);
    const askCompleted = await waitForCompletion(askMessageId, askResultMarker);
    assertPromptNotReplayed(askMarker, 2);
    assert.equal(auditRows(auditPath).length, askSpawnCount, "Restoring the ask waiter spawned another OMP process");
    const askFollowup = requestsFor(askMarker).at(-1)!;
    const askToolResults = messageList(askFollowup.body).filter((message) =>
      message.role === "tool" && message.tool_call_id === askCallId);
    assert.equal(askToolResults.length, 1, "The ask result did not return to the original OMP toolCallId");
    assert.ok(messageText(askToolResults[0]!.content).includes(answer), "The original ask answer was not delivered back to OMP");
    assert.ok(askCompleted.result_text?.includes(askResultMarker));
    console.log(`CASE ask-after-repeated-reconnect ${JSON.stringify({
      taskId: askRun.task.id,
      sessionId: askRun.task.session_id,
      ompPid: askRun.worker.pid,
      questionId: originalQuestionId,
      toolCallId: askCallId,
      reconnectsWhileWaiting: 2,
      completed: true,
    })}`);

    // Cancellation after reattachment must reach the original OMP process, not a replayed prompt or
    // a replacement worker. The model's final segment remains gated until cancellation is durable.
    const cancelGate = gate();
    const cancelMarker = "AGENT_RUNTIME_CANCEL_TASK";
    const cancelResultMarker = "AGENT_RUNTIME_MUST_NOT_COMPLETE_AFTER_CANCEL";
    const cancelStep = textStep("cancel after reconnect", ["AGENT_RUNTIME_CANCEL_CHECKPOINT", cancelResultMarker], new Map([[1, cancelGate]]));
    setSteps(cancelStep);
    const cancelMessageId = "810005";
    send(channel.connection, cancelMarker, cancelMessageId);
    await wait(`Local OMP never received ${cancelMarker}`, () => requestsFor(cancelMarker).length === 1);
    const cancelRun = await activeRun(cancelMessageId);
    await waitForStepConsumed(cancelStep);
    const cancelSpawnCount = auditRows(auditPath).length;
    await stop(channel, "SIGKILL");
    assert.ok(alive(cancelRun.worker.pid), "Channel crash killed the OMP worker before cancellation could be reattached");
    channel = await launch();
    await assertSameActiveRun(cancelMessageId, cancelRun);
    await assertRuntimeSurvived();
    send(channel.connection, "/cancel", "810006");
    await wait("Cancellation did not settle the original task", () => task(cancelMessageId)?.state === "cancelled");
    cancelGate.release();
    await wait("The original OMP process did not exit after cancellation", () => !alive(cancelRun.worker.pid));
    assertPromptNotReplayed(cancelMarker, 1);
    assert.equal(auditRows(auditPath).length, cancelSpawnCount, "Cancellation spawned a replacement OMP process");
    assert.ok(!fixtureOutputText().includes(cancelResultMarker), "Cancelled work delivered a later model result");
    console.log(`CASE cancel-original-worker ${JSON.stringify({
      taskId: cancelRun.task.id,
      sessionId: cancelRun.task.session_id,
      ompPid: cancelRun.worker.pid,
      state: task(cancelMessageId)?.state,
      completed: false,
    })}`);

    assert.equal(modelError, undefined);
    assert.equal(modelSteps.length, 0, "The local model script had unused steps");
    succeeded = true;
    console.log(`METRIC same_pid_hot_restarts=7`);
    console.log(`METRIC completed_while_channel_offline=1`);
    console.log(`METRIC preserved_offline_output=1`);
    console.log(`METRIC recovered_host_tools=1`);
    console.log(`METRIC recovered_asks=1`);
    console.log(`METRIC cancelled_original_workers=1`);
  } finally {
    for (const held of heldGates) held.release();
    for (const child of children) {
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        try {
          await exited(child, 5_000);
        } catch {
          child.kill("SIGKILL");
          await child.exited;
        }
      }
    }
    let cleanupFailure: unknown;
    try {
      await cleanupRuntime();
    } catch (error) {
      cleanupFailure = error;
    }
    try {
      await cleanupFixtureProcesses();
    } catch (error) {
      cleanupFailure ??= error;
    }
    db?.close();
    await fixture.close();
    modelServer?.stop(true);
    const logs = (await Promise.all(childLogs)).join("\n");
    assert.ok(!logs.includes(fixture.botToken), "Channel CLI leaked its isolated fixture credential");
    if (!succeeded && logs.length > 0) console.error(logs.replaceAll(fixture.botToken, "[fixture-token]").slice(-12_000));
    await rm(root, { recursive: true, force: true });
    if (cleanupFailure !== undefined) throw cleanupFailure;
  }
}

if (import.meta.main) await runAgentRuntimeRestartBenchmark();
