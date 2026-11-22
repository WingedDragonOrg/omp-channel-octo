import { mkdtemp, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, test } from "bun:test";
import {
  AgentRuntimeClient,
  connectAgentRuntime,
  ensureAgentRuntime,
  type AgentRuntimeHandle,
} from "../src/runtime/agent-runtime.ts";
import type { AgentRuntimeWorkerOptions } from "../src/runtime/agent-runtime-protocol.ts";

const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
const botUid = "agent-runtime-test-bot";

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
  scenario: "normal" | "crash",
  run: (harness: RuntimeHarness) => Promise<T>,
): Promise<T> {
  const tempDir = await realpath(await mkdtemp(join(tmpdir(), "agent-runtime-test-")));
  const stateDir = join(tempDir, "state");
  const logPath = join(tempDir, "omp-fixture.jsonl");
  const started = await ensureAgentRuntime({
    stateDir,
    botUid,
    ompCommand: process.execPath,
    ompExtraArgs: [
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
});
