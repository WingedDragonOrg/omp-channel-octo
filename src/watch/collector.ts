import type { WorkerProcess } from "../contracts.ts";
import { workerEnv } from "../omp/profile.ts";
import { runCommand } from "../workspace/worktrunk.ts";
import type { WatchCollection, WatchSpec } from "./types.ts";

const WATCH_COMMAND_DEADLINE_MS = 30_000;
const WATCH_STDOUT_MAX_BYTES = 16 * 1024;
const WATCH_STDERR_MAX_BYTES = 8 * 1024;

/** Run the registered argv in its fixed cwd under a verified process-group watchdog. */
export async function collectWatch(
  spec: WatchSpec,
  signal: AbortSignal,
  onSpawn: (identity: WorkerProcess) => void,
  onExit: () => void,
  cliEnv?: Record<string, string>,
): Promise<WatchCollection> {
  type Lifecycle = "none" | "not-spawned" | "confirmed" | "unknown";
  const state: { lifecycle: Lifecycle } = { lifecycle: "none" };
  const getLifecycle = (): Lifecycle => state.lifecycle;
  let timedOut = false;
  let outputExceeded = false;
  let processIdentityPersisted = false;
  let spawnedProcess: WorkerProcess | undefined;
  let spawnFailure: { error: unknown } | undefined;
  const overflowController = new AbortController();
  const deadlineController = new AbortController();
  const deadlineTimer = setTimeout(() => {
    timedOut = true;
    deadlineController.abort(new DOMException("Watch command deadline exceeded", "TimeoutError"));
  }, WATCH_COMMAND_DEADLINE_MS);
  const commandSignal = AbortSignal.any([signal, deadlineController.signal, overflowController.signal]);
  const unknownExit = (): WatchCollection => ({
    kind: "unknown",
    code: "Watch process-group exit could not be confirmed",
    ...(spawnedProcess === undefined ? {} : { process: spawnedProcess }),
  });

  try {
    const result = await runCommand(spec.argv, {
      cwd: spec.cwd,
      timeoutMs: WATCH_COMMAND_DEADLINE_MS,
      signal: commandSignal,
      env: workerEnv(undefined, cliEnv),
      parentDeathWatchdog: true,
      operation: "Watch check",
      maxOutputBytes: { stdout: WATCH_STDOUT_MAX_BYTES, stderr: WATCH_STDERR_MAX_BYTES },
      onOutputExceeded: () => {
        if (outputExceeded) return;
        outputExceeded = true;
        overflowController.abort(new DOMException("Watch command output exceeded its byte limit", "QuotaExceededError"));
      },
      lifecycle: {
        beforeSpawn: () => undefined,
        onSpawn: (identity) => {
          try {
            onSpawn(identity);
          } catch (error) {
            spawnFailure = { error };
            throw error;
          }
          processIdentityPersisted = true;
          spawnedProcess = identity;
        },
        onConfirmedExit: () => {
          state.lifecycle = "confirmed";
          onExit();
        },
        onNotSpawned: () => {
          state.lifecycle = "not-spawned";
          if (processIdentityPersisted) {
            state.lifecycle = "confirmed";
            onExit();
          }
        },
        onUnknown: () => {
          state.lifecycle = "unknown";
        },
      },
    });

    if (getLifecycle() === "unknown") return unknownExit();
    if (outputExceeded || result?.outputExceeded) return { kind: "failure", code: "Watch command output exceeded its byte limit" };
    if (timedOut || result?.timedOut) {
      return { kind: "failure", code: `Watch command timed out after ${WATCH_COMMAND_DEADLINE_MS}ms` };
    }
    if (signal.aborted || result?.aborted) return { kind: "failure", code: "Watch command was cancelled" };
    if (result === null) return { kind: "failure", code: "Watch command could not be started" };
    if (result.exitCode !== 0) return { kind: "failure", code: `Watch command exited with status ${result.exitCode}` };
    if (getLifecycle() !== "confirmed") return unknownExit();
    return { kind: "success", stdout: result.stdout };
  } catch {
    if (spawnFailure !== undefined) throw spawnFailure.error;
    if (getLifecycle() === "unknown") return unknownExit();
    if (outputExceeded) return { kind: "failure", code: "Watch command output exceeded its byte limit" };
    if (timedOut) return { kind: "failure", code: `Watch command timed out after ${WATCH_COMMAND_DEADLINE_MS}ms` };
    if (signal.aborted) return { kind: "failure", code: "Watch command was cancelled" };
    return { kind: "failure", code: getLifecycle() === "not-spawned" ? "Watch command could not be started" : "Watch command failed" };
  } finally {
    clearTimeout(deadlineTimer);
  }
}
