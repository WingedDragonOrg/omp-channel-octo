import { createHash } from "node:crypto";
import { createServer, type Socket } from "node:net";
import { mkdtemp, open, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { describe, expect, test } from "bun:test";
import {
  AgentRuntimeClient,
  connectAgentRuntime,
  ensureAgentRuntime,
  type AgentRuntimeHandle,
} from "../src/runtime/agent-runtime.ts";
import { queryProcess } from "../src/omp/process.ts";
import { AGENT_RUNTIME_PROTOCOL_VERSION, type AgentRuntimeWorkerOptions } from "../src/runtime/agent-runtime-protocol.ts";

const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
const botUid = "agent-runtime-test-bot";
const fixtureAcceptedResultHash = createHash("sha256").update("fixture accepted result").digest("hex");

type LogEntry = Record<string, unknown>;
type RuntimeHarness = {
  readonly tempDir: string;
  readonly stateDir: string;
  readonly logPath: string;
  readonly handle: AgentRuntimeHandle;
  readonly client: AgentRuntimeClient;
  readonly reconnect: () => Promise<AgentRuntimeClient>;
  readonly workerOptions: (sessionFile?: string) => AgentRuntimeWorkerOptions;
};

async function readLog(path: string): Promise<LogEntry[]> {
  try {
    return (await readFile(path, "utf8"))
      .split("\n")
      .filter(line => line.length > 0)
      .map(line => JSON.parse(line) as LogEntry);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

// These polls observe real daemon/OMP process state and a child-written fixture log;
// dispatch completion itself is awaited through the runtime's watchRun event stream.
async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
    throw error;
  }
}

async function manifestPath(stateDir: string): Promise<string> {
  const runtimeRoot = join(stateDir, "agent-runtime");
  const [runtimeDir] = (await readdir(runtimeRoot, { withFileTypes: true })).filter(entry => entry.isDirectory());
  if (!runtimeDir) throw new Error("Agent runtime manifest directory was not created");
  return join(runtimeRoot, runtimeDir.name, "runtime.json");
}

async function withRuntime<T>(
  scenario:
    | "normal"
    | "crash"
    | "host-tool"
    | "host-cancel"
    | "host-tool-retry-both"
    | "host-tool-retry-first-only"
    | "host-tool-retry-second-only"
    | "host-tool-retry-reverse-order"
    | "host-tool-retry-unmatched",
  run: (harness: RuntimeHarness) => Promise<T>,
  launch: { readonly ompCommand?: string; readonly ompExtraArgs?: readonly string[] } = {},
): Promise<T> {
  const tempDir = await realpath(await mkdtemp(join(tmpdir(), "agent-runtime-test-")));
  const stateDir = join(tempDir, "state");
  const logPath = join(tempDir, "omp-fixture.jsonl");
  const started = await ensureAgentRuntime({
    stateDir,
    botUid,
    ompCommand: launch.ompCommand ?? process.execPath,
    ompExtraArgs: launch.ompExtraArgs ?? [
      fixturePath,
      "--mode",
      "interactive",
      "--approval-mode",
      "always-ask",
      "--fixture-scenario",
      scenario,
      "--fixture-log",
      logPath,
    ],
    startupTimeoutMs: 5_000,
    cancelTimeoutMs: 500,
  });
  const clients = new Set<AgentRuntimeClient>([started.client]);
  const harness: RuntimeHarness = {
    tempDir,
    stateDir,
    logPath,
    handle: started.handle,
    client: started.client,
    reconnect: async () => {
      const connected = await connectAgentRuntime({ stateDir, botUid });
      clients.add(connected.client);
      return connected.client;
    },
    workerOptions: (sessionFile?: string) => ({
      cwd: tempDir,
      sessionDir: tempDir,
      ...(sessionFile === undefined ? {} : { sessionFile }),
    }),
  };

  try {
    return await run(harness);
  } finally {
    await Promise.all([...clients].map(client => client.disconnect().catch(() => {})));
    await started.handle.shutdown().catch(() => {});
    await waitUntil(() => !processIsAlive(started.handle.pid), 15_000);
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function assertHostToolRetryResultHashes(
  scenario:
    | "host-tool-retry-both"
    | "host-tool-retry-first-only"
    | "host-tool-retry-second-only"
    | "host-tool-retry-reverse-order"
    | "host-tool-retry-unmatched",
  expectedResults: readonly string[],
  sentResults: readonly [string, string] = ["original result", "retry result"],
): Promise<void> {
  await withRuntime(scenario, async harness => {
    const firstStarted = Promise.withResolvers<void>();
    const secondStarted = Promise.withResolvers<void>();
    const firstResult = Promise.withResolvers<string>();
    const secondResult = Promise.withResolvers<string>();
    const accepted: Array<{ toolCallId: string; resultHash: string }> = [];
    let executions = 0;
    const tool = {
      name: "fixture_echo",
      description: "Echo fixture input",
      parameters: { type: "object" },
      execute: async (args: Record<string, unknown>) => {
        executions += 1;
        if (args.attempt === 1) {
          firstStarted.resolve();
          return firstResult.promise;
        }
        if (args.attempt === 2) {
          secondStarted.resolve();
          return secondResult.promise;
        }
        throw new Error(`unexpected retry attempt: ${String(args.attempt)}`);
      },
      onResultAccepted: (toolCallId: string, resultHash: string) => {
        accepted.push({ toolCallId, resultHash });
      },
    };
    const worker = await harness.client.createWorker({
      taskId: `task-${scenario}`,
      runId: `run-${scenario}`,
      workerOptions: harness.workerOptions(),
    }, [tool]);
    const dispatchId = `dispatch-${scenario}`;
    await worker.dispatch({
      dispatchId,
      kind: "prompt",
      text: "retry the same host tool call",
      context: { tools: [tool] },
    });
    const watching = worker.watchRun(dispatchId, 0, () => {});

    try {
      await Promise.all([firstStarted.promise, secondStarted.promise]);
      firstResult.resolve(sentResults[0]);
      await waitUntil(async () => (await readLog(harness.logPath)).some(
        entry => entry.kind === "host_tool_retry_result_received" && entry.id === "fixture-host-retry-1",
      ));
      secondResult.resolve(sentResults[1]);

      expect((await watching).state).toBe("completed");
      if (expectedResults.length === 0) {
        await harness.client.listWorkers();
      } else {
        await waitUntil(() => accepted.length === expectedResults.length);
      }
      expect(executions).toBe(2);
      expect(accepted.map(result => result.toolCallId)).toEqual(
        expectedResults.map(() => "fixture-tool-call-retry"),
      );
      expect(accepted.map(result => result.resultHash)).toEqual(
        expectedResults.map(result => createHash("sha256").update(result).digest("hex")),
      );
      expect((await readLog(harness.logPath))
        .filter(entry => entry.kind === "host_tool_retry_result_received")
        .map(entry => entry.id))
        .toEqual(["fixture-host-retry-1", "fixture-host-retry-2"]);
    } finally {
      firstResult.resolve(sentResults[0]);
      secondResult.resolve(sentResults[1]);
      await watching.catch(() => {});
    }
  });
}

type FakeRuntimeControls = {
  readonly manifestPath: string;
  connectReplacement(): Promise<AgentRuntimeClient>;
  addWorker(taskId: string, runId: string): void;
  failAttach(taskId: string, runId: string, error?: string): void;
  attachCount(taskId: string, runId: string): number;
  acceptedAckCount(): number;
  dropNextAcceptedAck(): void;
  acceptedEventCount(): number;
  rejectedConnectionCount(): number;
  queueAcceptedResult(taskId: string, runId: string, dispatchId: string, toolName: string, toolCallId: string): void;
  holdWatchRunResponse(hold: boolean): void;
  rejectConnections(reject: boolean): void;
  dropCurrent(): void;
  completeDispatchAndDropCurrent(): void;
  emitTerminalOnAttach(): void;
};

async function withFakeRuntime(
  run: (
    client: AgentRuntimeClient,
    requestCounts: { dispatch: number; runState: number; createWorker: number; attachWorker: number; watchRun: number },
    controls: FakeRuntimeControls,
  ) => Promise<void>,
): Promise<void> {
  const tempDir = await mkdtemp(join(tmpdir(), "agent-runtime-client-test-"));
  const socketPath = join(tempDir, "runtime.sock");
  const manifestPath = join(tempDir, "runtime.json");
  const runtimeId = "isolated-fake-runtime";
  const secret = "isolated-fake-secret";
  const processSnapshot = await queryProcess(process.pid);
  if (!processSnapshot) throw new Error("Could not verify test process identity");
  await writeFile(manifestPath, JSON.stringify({
    protocolVersion: AGENT_RUNTIME_PROTOCOL_VERSION,
    botUid,
    runtimeId,
    pid: process.pid,
    processIdentity: processSnapshot.identity,
    socketPath,
    secret,
    startedAt: Date.now(),
  }));

  const workers = new Map<string, Record<string, unknown>>();
  const requestCounts = { dispatch: 0, runState: 0, createWorker: 0, attachWorker: 0, watchRun: 0 };
  const attachCounts = new Map<string, number>();
  const failedAttachments = new Map<string, string>();
  let pendingAcceptedEvent: Record<string, unknown> | undefined;
  let acceptedAckCount = 0;
  let acceptedEventCount = 0;
  let dropNextAcceptedAck = false;
  let dispatchState: Record<string, unknown> | undefined;
  let dispatchEvents: Record<string, unknown>[] = [];
  let epoch = 0;
  let currentSocket: Socket | undefined;
  const clients = new Set<AgentRuntimeClient>();
  let rejectNewConnections = false;
  let rejectedConnectionCount = 0;
  let holdWatchRunResponses = false;
  let emitTerminalBeforeWatchRestore = false;
  const workerKey = (taskId: string, runId: string): string => `${taskId}\0${runId}`;
  const controls: FakeRuntimeControls = {
    manifestPath,
    async connectReplacement() {
      const replacement = await AgentRuntimeClient.connect({
        socketPath,
        secret,
        botUid,
        runtimeId,
        pid: process.pid,
        processIdentity: processSnapshot.identity,
        manifestPath,
        timeoutMs: 1_000,
      });
      clients.add(replacement);
      return replacement;
    },
    addWorker(taskId, runId) {
      workers.set(workerKey(taskId, runId), makeWorker(taskId, runId));
    },
    failAttach(taskId, runId, error = "Agent runtime worker identity is unverified") {
      failedAttachments.set(workerKey(taskId, runId), error);
    },
    attachCount(taskId, runId) {
      return attachCounts.get(workerKey(taskId, runId)) ?? 0;
    },
    acceptedAckCount() {
      return acceptedAckCount;
    },
    dropNextAcceptedAck() {
      dropNextAcceptedAck = true;
    },
    acceptedEventCount() {
      return acceptedEventCount;
    },
    rejectedConnectionCount() {
      return rejectedConnectionCount;
    },
    queueAcceptedResult(taskId, runId, dispatchId, toolName, toolCallId) {
      pendingAcceptedEvent = {
        event: "host-tool-result-accepted",
        taskId,
        runId,
        dispatchId,
        value: {
          ackId: `accepted-${taskId}-${toolCallId}`,
          toolName,
          toolCallId,
          resultHash: fixtureAcceptedResultHash,
        },
      };
    },
    holdWatchRunResponse(hold) {
      holdWatchRunResponses = hold;
    },
    rejectConnections(reject) {
      rejectNewConnections = reject;
    },
    dropCurrent() {
      currentSocket?.destroy();
    },
    completeDispatchAndDropCurrent() {
      if (!dispatchState) throw new Error("No fake dispatch is active");
      dispatchState.state = "completed";
      dispatchEvents.push({ type: "goal_accepted" });
      dispatchState.nextSeq = dispatchEvents.length + 1;
      currentSocket?.destroy();
    },
    emitTerminalOnAttach() {
      emitTerminalBeforeWatchRestore = true;
    },
  };
  const makeWorker = (taskId: string, runId: string): Record<string, unknown> => ({
    botUid,
    taskId,
    runId,
    runtimeId,
    sessionId: `session-${runId}`,
    sessionFile: join(tempDir, `${runId}.jsonl`),
    process: null,
    stage: "omp-ready",
    activeDispatchId: null,
    dispatches: [],
  });
  workers.set(workerKey("task-fake-runtime", "run-fake-runtime"), makeWorker("task-fake-runtime", "run-fake-runtime"));
  const server = createServer(socket => {
    socket.setEncoding("utf8");
    socket.on("error", () => {});
    let buffer = "";
    let socketEpoch = 0;
    let welcomed = false;
    const respond = (id: string, value: unknown): void => {
      if (socket !== currentSocket || socket.destroyed) return;
      socket.write(`${JSON.stringify({ kind: "response", id, ok: true, value })}\n`);
    };
    const fail = (id: string, error: string): void => {
      if (socket !== currentSocket || socket.destroyed) return;
      socket.write(`${JSON.stringify({ kind: "response", id, ok: false, error })}\n`);
    };
    const sendEvent = (event: Record<string, unknown>): void => {
      if (socket !== currentSocket || socket.destroyed) return;
      socket.write(`${JSON.stringify({ kind: "event", epoch: socketEpoch, ...event })}\n`);
    };
    socket.on("close", () => {
      if (currentSocket === socket) currentSocket = undefined;
    });
    socket.on("data", chunk => {
      buffer += String(chunk);
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) break;
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (!line) continue;
        const frame = JSON.parse(line) as Record<string, unknown>;
        if (!welcomed) {
          if (frame.kind !== "hello" || frame.version !== AGENT_RUNTIME_PROTOCOL_VERSION || frame.botUid !== botUid || frame.secret !== secret) {
            socket.destroy();
            return;
          }
          if (rejectNewConnections) {
            rejectedConnectionCount += 1;
            socket.destroy();
            return;
          }
          socketEpoch = ++epoch;
          welcomed = true;
          const previous = currentSocket;
          currentSocket = socket;
          previous?.destroy();
          socket.write(`${JSON.stringify({
            kind: "welcome",
            version: AGENT_RUNTIME_PROTOCOL_VERSION,
            runtimeId,
            pid: process.pid,
            epoch: socketEpoch,
            workers: [...workers.values()],
          })}\n`);
          continue;
        }
        if (frame.kind !== "request" || frame.epoch !== socketEpoch || socket !== currentSocket) continue;
        const id = String(frame.id ?? "");
        const args = (frame.args ?? {}) as Record<string, unknown>;
        const taskId = String(args.taskId ?? "");
        const runId = String(args.runId ?? "");
        if (frame.operation === "attach-worker") {
          const key = workerKey(taskId, runId);
          attachCounts.set(key, (attachCounts.get(key) ?? 0) + 1);
          if (emitTerminalBeforeWatchRestore && socketEpoch > 1 && dispatchState?.state === "completed") {
            sendEvent({
              event: "run-terminal",
              taskId,
              runId,
              dispatchId: dispatchState.dispatchId,
              value: dispatchState,
            });
          }
          const failure = failedAttachments.get(key);
          const snapshot = workers.get(key);
          if (failure) fail(id, failure);
          else if (snapshot) {
            if (pendingAcceptedEvent && pendingAcceptedEvent.taskId === taskId && pendingAcceptedEvent.runId === runId) {
              acceptedEventCount += 1;
              sendEvent(pendingAcceptedEvent);
              acceptedEventCount += 1;
              sendEvent(pendingAcceptedEvent);
            }
            respond(id, snapshot);
          } else fail(id, "Agent runtime worker identity does not match");
        } else if (frame.operation === "dispatch") {
          requestCounts.dispatch += 1;
          const dispatch = args.dispatch as Record<string, unknown>;
          const dispatchId = String(dispatch.dispatchId);
          dispatchEvents = [{ type: "agent_start" }, { type: "message_start" }];
          dispatchState = { dispatchId, kind: dispatch.kind, state: "active", nextSeq: dispatchEvents.length + 1 };
          const worker = workers.get(workerKey(taskId, runId));
          if (worker) {
            worker.stage = "active";
            worker.activeDispatchId = dispatchId;
          }
          if (requestCounts.dispatch === 1) {
            socket.destroy();
            return;
          }
          respond(id, dispatchState);
        } else if (frame.operation === "watch-run") {
          requestCounts.watchRun += 1;
          if (holdWatchRunResponses) continue;
          const dispatchId = String(args.dispatchId ?? "");
          const fromSeq = Number(args.fromSeq ?? 0);
          if (!dispatchState || dispatchState.dispatchId !== dispatchId) {
            fail(id, "Agent runtime dispatch does not exist");
            continue;
          }
          for (let index = fromSeq; index < dispatchEvents.length; index += 1) {
            sendEvent({
              event: "run-event",
              taskId,
              runId,
              dispatchId,
              seq: index + 1,
              value: dispatchEvents[index]!,
            });
          }
          if (dispatchState.state === "completed") {
            sendEvent({ event: "run-terminal", taskId, runId, dispatchId, value: dispatchState });
          }
          respond(id, dispatchState);
        } else if (frame.operation === "run-state") {
          requestCounts.runState += 1;
          if (dispatchState && dispatchState.dispatchId === args.dispatchId) respond(id, dispatchState);
          else fail(id, "Agent runtime dispatch does not exist");
        } else if (frame.operation === "create-worker") {
          requestCounts.createWorker += 1;
          const input = args as { taskId: string; runId: string };
          const snapshot = makeWorker(input.taskId, input.runId);
          workers.set(workerKey(input.taskId, input.runId), snapshot);
          if (requestCounts.createWorker === 1) {
            socket.destroy();
            return;
          }
          respond(id, snapshot);
        } else if (frame.operation === "host-tool-result-accepted-ack") {
          if (dropNextAcceptedAck) {
            dropNextAcceptedAck = false;
            socket.destroy();
            return;
          }
          if (
            pendingAcceptedEvent
            && (pendingAcceptedEvent.value as Record<string, unknown>).ackId === args.ackId
            && (pendingAcceptedEvent.value as Record<string, unknown>).resultHash === args.resultHash
          ) {
            pendingAcceptedEvent = undefined;
            acceptedAckCount += 1;
          }
          respond(id, { accepted: true });
        } else if (frame.operation === "list-workers") {
          respond(id, [...workers.values()]);
        } else {
          fail(id, `Unsupported fixture operation: ${String(frame.operation)}`);
        }
      }
    });
  });
  const listening = Promise.withResolvers<void>();
  server.once("error", listening.reject);
  server.listen(socketPath, () => listening.resolve());
  await listening.promise;
  server.off("error", listening.reject);

  let client: AgentRuntimeClient | undefined;
  try {
    client = await AgentRuntimeClient.connect({
      socketPath,
      secret,
      botUid,
      runtimeId,
      pid: process.pid,
      processIdentity: processSnapshot.identity,
      manifestPath,
      timeoutMs: 1_000,
    });
    clients.add(client);
    await run(client, requestCounts, controls);
  } finally {
    await Promise.all([...clients].map(connected => connected.disconnect().catch(() => {})));
    const closed = Promise.withResolvers<void>();
    server.close(() => closed.resolve());
    await closed.promise;
    await rm(tempDir, { recursive: true, force: true });
  }
}


describe("agent runtime channel recovery", () => {
  test("fails closed when the persisted runtime process identity is unknown", async () => {
    await withRuntime("normal", async harness => {
      const path = await manifestPath(harness.stateDir);
      const original = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
      expect(typeof original.processIdentity).toBe("string");
      await writeFile(path, JSON.stringify({
        ...original,
        processIdentity: `${String(original.processIdentity)}-unknown`,
      }));

      try {
        await expect(connectAgentRuntime({ stateDir: harness.stateDir, botUid })).rejects.toThrow(
          "Agent runtime process identity is unknown",
        );
      } finally {
        await writeFile(path, JSON.stringify(original));
      }
    });
  });

  test("retains a bounded startup diagnostic when OMP exits before ready", async () => {
    await withRuntime("normal", async harness => {
      await expect(harness.client.createWorker({
        taskId: "task-startup-failure",
        runId: "run-startup-failure",
        workerOptions: harness.workerOptions(),
      })).rejects.toThrow("OMP output ended before ready");

      const runtimeManifestPath = await manifestPath(harness.stateDir);
      const ledger = JSON.parse(await readFile(join(dirname(runtimeManifestPath), "workers.json"), "utf8")) as Array<Record<string, unknown>>;
      expect(ledger).toHaveLength(1);
      const failure = ledger[0]?.startupFailure as Record<string, unknown> | undefined;
      expect(failure?.code).toBe("exited");
      expect(failure?.diagnostic).toBe("退出码 23");
      expect(String(failure?.diagnostic).length).toBeLessThanOrEqual(256);
      expect(ledger[0]?.sessionId).toBe("");
      expect(ledger[0]?.stage).toBe("exited-confirmed");
    }, {
      ompCommand: "/bin/sh",
      ompExtraArgs: ["-c", "printf 'fixture OMP startup failure' >&2; sleep 1; exit 23"],
    });
  });

  test("rejects a second worker that tries to own the same OMP session", async () => {
    await withRuntime("normal", async harness => {
      const sessionFile = join(harness.tempDir, "shared-session.jsonl");
      const first = await harness.client.createWorker({
        taskId: "task-one",
        runId: "run-one",
        workerOptions: harness.workerOptions(sessionFile),
      });

      await expect(harness.client.createWorker({
        taskId: "task-two",
        runId: "run-two",
        workerOptions: harness.workerOptions(sessionFile),
      })).rejects.toThrow("该 OMP session 已由另一个 runtime run 占用");
      expect(await harness.client.listWorkers()).toHaveLength(1);
      expect(first.sessionFile).toBe(sessionFile);
    });
  });

  test("records an unexpected OMP child exit as a failed terminal dispatch", async () => {
    await withRuntime("crash", async harness => {
      const worker = await harness.client.createWorker({
        taskId: "task-crash",
        runId: "run-crash",
        workerOptions: harness.workerOptions(),
      });
      const dispatchId = "crash-prompt";
      await worker.dispatch({ dispatchId, kind: "prompt", text: "exit unexpectedly" });

      const terminal = await worker.watchRun(dispatchId, 0, () => {});
      expect(terminal.state).toBe("failed");
      expect(terminal.error).toBeTruthy();
      await expect(harness.client.attachWorker({
        taskId: "task-crash",
        runId: "run-crash",
        sessionId: worker.sessionId,
      })).rejects.toThrow(/exited|identity|unverified/i);
    });
  });

  test("reattaching and repeating a dispatch ID does not send its prompt twice", async () => {
    await withRuntime("normal", async harness => {
      const taskId = "task-repeat-attach";
      const runId = "run-repeat-attach";
      const dispatchId = "task-repeat-attach";
      const text = "send this prompt once";
      const worker = await harness.client.createWorker({
        taskId,
        runId,
        workerOptions: harness.workerOptions(),
      });
      const dispatch = { dispatchId, kind: "prompt" as const, text };
      await worker.dispatch(dispatch);
      await waitUntil(async () => (await readLog(harness.logPath)).some(entry => entry.kind === "prompt"));

      await harness.client.disconnect();
      const reconnected = await harness.reconnect();
      const attached = await reconnected.attachWorker({ taskId, runId, sessionId: worker.sessionId });
      await attached.dispatch(dispatch);
      const terminal = await attached.watchRun(dispatchId, 0, () => {});

      expect(terminal.state).toBe("completed");
      const prompts = (await readLog(harness.logPath)).filter(entry => entry.kind === "prompt");
      expect(prompts).toHaveLength(1);
      expect(prompts[0]?.message).toBe(text);
    });
  });
  test("reconnects the same client, resumes from consumed events, and does not rerun a pending host tool", async () => {
    await withRuntime("host-tool", async harness => {
      const toolStarted = Promise.withResolvers<void>();
      const toolGate = Promise.withResolvers<void>();
      let executions = 0;
      const acceptedResult = Promise.withResolvers<{ toolCallId: string; resultHash: string }>();
      const tool = {
        name: "fixture_echo",
        description: "Echo fixture input",
        parameters: { type: "object" },
        execute: async () => {
          executions += 1;
          toolStarted.resolve();
          await toolGate.promise;
          return "completed once";
        },
        onResultAccepted: (toolCallId: string, resultHash: string) => acceptedResult.resolve({ toolCallId, resultHash }),
      };
      const worker = await harness.client.createWorker({
        taskId: "task-runtime-reconnect",
        runId: "run-runtime-reconnect",
        workerOptions: harness.workerOptions(),
      }, [tool]);
      const dispatchId = "dispatch-runtime-reconnect";
      await worker.dispatch({
        dispatchId,
        kind: "prompt",
        text: "call the fixture tool once",
        context: { tools: [tool] },
      });
      const consumedSeqs: number[] = [];
      const watching = worker.watchRun(dispatchId, 0, (_event, seq) => consumedSeqs.push(seq));

      try {
        await toolStarted.promise;
        await waitUntil(() => consumedSeqs.length > 0);
        const replacement = await harness.reconnect();
        await replacement.disconnect();

        const workers = await harness.client.listWorkers();
        expect(workers.some(snapshot => snapshot.taskId === "task-runtime-reconnect" && snapshot.runId === "run-runtime-reconnect")).toBe(true);
        expect(executions).toBe(1);

        toolGate.resolve();
        expect((await watching).state).toBe("completed");
        const accepted = await acceptedResult.promise;
        expect(accepted.toolCallId).toBeTruthy();
        expect(accepted.resultHash).toBe(createHash("sha256").update("completed once").digest("hex"));
        expect(executions).toBe(1);
        expect(consumedSeqs.length).toBeGreaterThan(0);
        expect(new Set(consumedSeqs).size).toBe(consumedSeqs.length);
        expect(consumedSeqs).toEqual([...consumedSeqs].sort((left, right) => left - right));
      } finally {
        toolGate.resolve();
        await watching.catch(() => {});
      }
    });
  });

  test("matches accepted retry results to their result payloads", async () => {
    await assertHostToolRetryResultHashes("host-tool-retry-both", ["original result", "retry result"]);
  });
  test("retains the first accepted retry result when a later result is unaccepted", async () => {
    await assertHostToolRetryResultHashes("host-tool-retry-first-only", ["original result"]);
  });
  test("acknowledges only the second retry result when its payload is accepted", async () => {
    await assertHostToolRetryResultHashes("host-tool-retry-second-only", ["retry result"]);
  });

  test("accepts one of duplicate identical results for the same native call", async () => {
    await assertHostToolRetryResultHashes(
      "host-tool-retry-second-only",
      ["same result"],
      ["same result", "same result"],
    );
  });
  test("matches accepted retry results in OMP's result order rather than call order", async () => {
    await assertHostToolRetryResultHashes("host-tool-retry-reverse-order", ["retry result", "original result"]);
  });

  test("fails closed when a tool-end payload matches no sent host result", async () => {
    await assertHostToolRetryResultHashes("host-tool-retry-unmatched", []);
  });

  test("replays a cancelled host tool to the same client after reconnect", async () => {
    await withRuntime("host-cancel", async harness => {
      const toolStarted = Promise.withResolvers<void>();
      const observedAbort = Promise.withResolvers<void>();
      const releaseTool = Promise.withResolvers<void>();
      let aborted = false;
      let executions = 0;
      const acceptedResult = Promise.withResolvers<{ toolCallId: string; resultHash: string }>();
      const tool = {
        name: "fixture_echo",
        description: "Echo fixture input",
        parameters: { type: "object" },
        execute: async (_args: Record<string, unknown>, signal: AbortSignal) => {
          executions += 1;
          signal.addEventListener("abort", () => {
            aborted = true;
            observedAbort.resolve();
          }, { once: true });
          toolStarted.resolve();
          const replacement = await harness.reconnect();
          await replacement.disconnect();
          await Promise.race([observedAbort.promise, releaseTool.promise]);
          return "cancelled";
        },
        onResultAccepted: (toolCallId: string, resultHash: string) => acceptedResult.resolve({ toolCallId, resultHash }),
      };
      const worker = await harness.client.createWorker({
        taskId: "task-host-cancel-reconnect",
        runId: "run-host-cancel-reconnect",
        workerOptions: harness.workerOptions(),
      }, [tool]);
      const dispatchId = "dispatch-host-cancel-reconnect";
      await worker.dispatch({
        dispatchId,
        kind: "prompt",
        text: "cancel the host tool after channel loss",
        context: { tools: [tool] },
      });
      const watching = worker.watchRun(dispatchId, 0, () => {});
      try {
        await toolStarted.promise;
        await waitUntil(() => aborted, 5_000);
        expect(executions).toBe(1);
        expect((await watching).state).toBe("completed");
        const accepted = await acceptedResult.promise;
        expect(accepted.toolCallId).toBeTruthy();
        expect(accepted.resultHash).toMatch(/^[0-9a-f]{64}$/u);
        expect(accepted.resultHash).not.toBe(createHash("sha256").update("cancelled").digest("hex"));
      } finally {
        releaseTool.resolve();
        await watching.catch(() => {});
      }
    });
  });
  test("explicit disconnect prevents the original client from reconnecting", async () => {
    await withRuntime("normal", async harness => {
      const replacement = await harness.reconnect();
      await harness.client.disconnect();
      await replacement.disconnect();

      await expect(harness.client.listWorkers()).rejects.toThrow(/disconnected/i);
      const current = await harness.reconnect();
      expect(await current.listWorkers()).toHaveLength(0);
      await expect(harness.client.listWorkers()).rejects.toThrow(/disconnected/i);
    });
  });

  test("fails closed when the runtime daemon exits", async () => {
    await withRuntime("normal", async harness => {
      await harness.handle.shutdown();
      await waitUntil(() => !processIsAlive(harness.handle.pid), 15_000);
      await expect(harness.client.listWorkers()).rejects.toThrow(/identity|unknown|disconnected/i);
    });
  });

  test("refuses automatic reconnect when the runtime process identity changes", async () => {
    await withRuntime("normal", async harness => {
      const path = await manifestPath(harness.stateDir);
      const original = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
      const replacement = await harness.reconnect();

      try {
        await writeFile(path, JSON.stringify({
          ...original,
          processIdentity: `${String(original.processIdentity)}-changed`,
        }));
        await replacement.disconnect();
        await expect(harness.client.listWorkers()).rejects.toThrow(/identity|unknown/i);
      } finally {
        await writeFile(path, JSON.stringify(original));
      }
    });
  });
  test("disconnect during identity verification cannot take over a replacement connection", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      const path = controls.manifestPath;
      const originalManifest = await readFile(path, "utf8");
      await rm(path);
      const fifo = Bun.spawn(["mkfifo", path], { stdout: "ignore", stderr: "ignore" });
      expect(await fifo.exited).toBe(0);
      controls.dropCurrent();

      const writer = await open(path, "w");
      let replacement: AgentRuntimeClient | undefined;
      try {
        await writer.writeFile(originalManifest);
        await rm(path);
        await writeFile(path, originalManifest);
        replacement = await controls.connectReplacement();
        await client.disconnect();
        await writer.close();
        await Bun.sleep(100);
        expect(await replacement.listWorkers()).toHaveLength(1);
      } finally {
        await writer.close().catch(() => {});
        await replacement?.disconnect();
      }
    });
  });

  test("rejects a pending watch on explicit disconnect without leaving its terminal promise unhandled", async () => {
    await withFakeRuntime(async (client, requestCounts, controls) => {
      const worker = await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      await worker.dispatch({ dispatchId: "watch-disconnect", kind: "prompt", text: "watch" });
      controls.holdWatchRunResponse(true);
      const watching = worker.watchRun("watch-disconnect", 0, () => {});
      await waitUntil(() => requestCounts.watchRun === 1);

      await client.disconnect();
      await expect(watching).rejects.toThrow(/disconnected/i);
    });
  });

  test("rejects a pending watch when runtime identity changes without an unhandled terminal promise", async () => {
    await withFakeRuntime(async (client, requestCounts, controls) => {
      const worker = await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      await worker.dispatch({ dispatchId: "watch-identity-change", kind: "prompt", text: "watch" });
      controls.holdWatchRunResponse(true);
      const watching = worker.watchRun("watch-identity-change", 0, () => {});
      await waitUntil(() => requestCounts.watchRun === 1);

      const original = JSON.parse(await readFile(controls.manifestPath, "utf8")) as Record<string, unknown>;
      await writeFile(controls.manifestPath, JSON.stringify({
        ...original,
        processIdentity: `${String(original.processIdentity)}-changed`,
      }));
      controls.dropCurrent();

      await expect(watching).rejects.toThrow(/identity|unknown/i);
    });
  });

  test("buffers an early terminal event until replayed watch events are consumed", async () => {
    await withFakeRuntime(async (client, requestCounts, controls) => {
      const worker = await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      await worker.dispatch({ dispatchId: "terminal-reconnect-race", kind: "prompt", text: "watch" });
      const seqs: number[] = [];
      const watching = worker.watchRun("terminal-reconnect-race", 0, (_event, seq) => seqs.push(seq));
      try {
        await waitUntil(() => requestCounts.watchRun === 1 && seqs.length === 2);
        controls.emitTerminalOnAttach();
        controls.completeDispatchAndDropCurrent();
        const terminal = await watching;
        expect(terminal.state).toBe("completed");
        expect(seqs).toEqual([1, 2, 3]);
        expect(requestCounts.watchRun).toBe(2);
      } finally {
        await client.disconnect();
        await watching.catch(() => {});
      }
    });
  });

  test("drops a failed worker attachment without terminating unrelated registrations", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      controls.addWorker("task-restore-good", "run-restore-good");
      controls.addWorker("task-restore-bad", "run-restore-bad");
      await client.attachWorker({
        taskId: "task-restore-good",
        runId: "run-restore-good",
        sessionId: "session-run-restore-good",
      });
      await client.attachWorker({
        taskId: "task-restore-bad",
        runId: "run-restore-bad",
        sessionId: "session-run-restore-bad",
      });
      controls.failAttach("task-restore-bad", "run-restore-bad");

      controls.dropCurrent();
      expect(await client.listWorkers()).toHaveLength(3);
      expect(controls.attachCount("task-restore-good", "run-restore-good")).toBe(2);
      expect(controls.attachCount("task-restore-bad", "run-restore-bad")).toBe(2);

      controls.dropCurrent();
      expect(await client.listWorkers()).toHaveLength(3);
      expect(controls.attachCount("task-restore-good", "run-restore-good")).toBe(3);
      expect(controls.attachCount("task-restore-bad", "run-restore-bad")).toBe(2);
    });
  });

  test("detaches locally without waiting forever for a reconnect", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      controls.rejectConnections(true);
      controls.dropCurrent();
      await waitUntil(() => controls.rejectedConnectionCount() > 0);
      let detached = false;
      const detaching = client.detachWorker("task-fake-runtime", "run-fake-runtime").then(() => { detached = true; });
      await Promise.resolve();
      expect(detached).toBe(true);
      await detaching;
    });
  });
  test("does not claim a worker stopped while the runtime is unavailable", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      const worker = await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      controls.rejectConnections(true);
      controls.dropCurrent();
      await waitUntil(() => controls.rejectedConnectionCount() > 0);

      expect(await worker.stop()).toBe(false);
      await client.disconnect();
    });
  });


  test("replays accepted host-tool results once after reconnect", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      let acceptedCount = 0;
      let acceptedHash = "";
      await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      }, [{
        name: "fixture_echo",
        description: "Echo fixture input",
        parameters: { type: "object" },
        execute: async () => "unused",
        onResultAccepted: (_toolCallId: string, resultHash: string) => {
          acceptedCount += 1;
          acceptedHash = resultHash;
          controls.dropNextAcceptedAck();
        },
      }]);
      controls.queueAcceptedResult(
        "task-fake-runtime",
        "run-fake-runtime",
        "dispatch-accepted-result",
        "fixture_echo",
        "tool-call-accepted",
      );

      controls.dropCurrent();
      await client.listWorkers();
      await waitUntil(() => controls.acceptedAckCount() === 1);
      expect(acceptedCount).toBe(1);
      expect(acceptedHash).toBe(fixtureAcceptedResultHash);
      expect(controls.acceptedEventCount()).toBe(4);
      expect(controls.acceptedAckCount()).toBe(1);
    });
  });

  test("acknowledges registered accepted results without an optional callback", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      }, [{
        name: "fixture_echo",
        description: "Echo fixture input",
        parameters: { type: "object" },
        execute: async () => "unused",
      }]);
      controls.queueAcceptedResult(
        "task-fake-runtime",
        "run-fake-runtime",
        "dispatch-no-acceptance-callback",
        "fixture_echo",
        "tool-call-no-acceptance-callback",
      );

      controls.dropCurrent();
      await client.listWorkers();
      await waitUntil(() => controls.acceptedAckCount() === 1);
      expect(controls.acceptedEventCount()).toBe(2);
      expect(controls.acceptedAckCount()).toBe(1);
    });
  });

  test("does not acknowledge accepted results for unregistered tools", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      controls.queueAcceptedResult(
        "task-fake-runtime",
        "run-fake-runtime",
        "dispatch-unregistered-tool",
        "not_registered",
        "tool-call-unregistered",
      );

      controls.dropCurrent();
      await client.listWorkers();
      await waitUntil(() => controls.acceptedEventCount() === 2);
      expect(controls.acceptedAckCount()).toBe(0);
    });
  });

  test("replays an accepted result after its callback throws without acknowledging early", async () => {
    await withFakeRuntime(async (client, _requestCounts, controls) => {
      let acceptedCalls = 0;
      await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      }, [{
        name: "fixture_echo",
        description: "Echo fixture input",
        parameters: { type: "object" },
        execute: async () => "unused",
        onResultAccepted: () => {
          acceptedCalls += 1;
          if (acceptedCalls === 1) throw new Error("fixture callback failure");
        },
      }]);
      controls.queueAcceptedResult(
        "task-fake-runtime",
        "run-fake-runtime",
        "dispatch-callback-retry",
        "fixture_echo",
        "tool-call-callback-retry",
      );

      controls.dropCurrent();
      await client.listWorkers();
      await waitUntil(() => controls.acceptedAckCount() === 1);
      expect(acceptedCalls).toBe(2);
      expect(controls.acceptedAckCount()).toBe(1);
    });
  });


  test("confirms a lost dispatch by stable ID and does not retry an unknown create outcome", async () => {
    await withFakeRuntime(async (client, requestCounts) => {
      const worker = await client.attachWorker({
        taskId: "task-fake-runtime",
        runId: "run-fake-runtime",
        sessionId: "session-run-fake-runtime",
      });
      const dispatch = await worker.dispatch({
        dispatchId: "stable-dispatch-after-disconnect",
        kind: "prompt",
        text: "dispatch exactly once",
      });

      expect(dispatch.state).toBe("active");
      expect(requestCounts.dispatch).toBe(1);
      expect(requestCounts.runState).toBe(1);

      await expect(client.createWorker({
        taskId: "task-create-unknown",
        runId: "run-create-unknown",
        workerOptions: { cwd: "/tmp", sessionDir: "/tmp" },
      })).rejects.toThrow(/creation outcome is unknown; refusing to retry/i);

      const workers = await client.listWorkers();
      expect(workers).toHaveLength(2);
      expect(requestCounts.createWorker).toBe(1);
    });
  });
});
