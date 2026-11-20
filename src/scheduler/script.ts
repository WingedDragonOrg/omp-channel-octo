import { workerEnv } from "../omp/profile.ts";
import { runCommand } from "../workspace/worktrunk.ts";
import type { ScheduleScriptRunner } from "./types.ts";

const STDOUT_LIMIT_BYTES = 16 * 1024;
const STDERR_LIMIT_BYTES = 8 * 1024;

export const runScheduleScript: ScheduleScriptRunner = async (request) => {
  if (request.signal.aborted) return { kind: "not-started", reason: "cancelled" };
  if (request.schedule.spec.kind !== "script") {
    return { kind: "not-started", reason: "spawn-failed" };
  }
  const runner = request.schedule.spec.runner;
  const argv = runner.kind === "shell"
    ? ["/bin/bash", "-c", runner.script]
    : [runner.command, ...runner.args];
  const env = workerEnv(undefined, request.cliEnv);
  env.OMP_CHANNEL_SCHEDULE_ARTIFACTS_FILE = request.manifestPath;
  env.OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_TYPE = String(request.schedule.target.channelType);
  env.OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_ID = request.schedule.target.channelId;
  const overflowController = new AbortController();
  const commandSignal = AbortSignal.any([request.signal, overflowController.signal]);
  let spawned = false;
  let exited = false;
  let unknown = false;
  let unknownReason: "process-identity" | "process-exit" = "process-exit";
  let outputExceeded = false;
  try {
    const result = await runCommand(argv, {
      cwd: request.schedule.cwd,
      timeoutMs: request.schedule.timeoutMs,
      signal: commandSignal,
      env,
      parentDeathWatchdog: true,
      operation: "Scheduled script",
      maxOutputBytes: { stdout: STDOUT_LIMIT_BYTES, stderr: STDERR_LIMIT_BYTES },
      onOutputExceeded: () => {
        outputExceeded = true;
        overflowController.abort(new DOMException("Scheduled script output exceeded its byte limit", "QuotaExceededError"));
      },
      lifecycle: {
        beforeSpawn: () => request.beforeSpawn?.(),
        onSpawn: process => {
          spawned = true;
          try {
            request.onSpawn(process);
          } catch {
            unknown = true;
            unknownReason = "process-identity";
            throw new Error("schedule process identity was not persisted");
          }
        },
        onConfirmedExit: () => {
          exited = true;
          try {
            request.onExit();
          } catch {
            unknown = true;
          }
        },
        onNotSpawned: () => {
          if (spawned) {
            exited = true;
            try {
              request.onExit();
            } catch {
              unknown = true;
            }
          }
        },
        onUnknown: () => {
          unknown = true;
        },
      },
    });
    if (unknown || (spawned && !exited)) {
      return {
        kind: "unknown",
        reason: unknownReason,
        stdout: result?.stdout ?? "",
        stderr: result?.stderr ?? "",
      };
    }
    if (result === null || !result.executionStarted) {
      return spawned
        ? { kind: "unknown", reason: "process-exit", stdout: result?.stdout ?? "", stderr: result?.stderr ?? "" }
        : { kind: "not-started", reason: request.signal.aborted ? "cancelled" : "spawn-failed" };
    }
    if (outputExceeded || result.outputExceeded) {
      return { kind: "output-exceeded", processExited: exited, stdout: result.stdout, stderr: result.stderr };
    }
    if (result.timedOut) {
      return { kind: "timed-out", processExited: exited, stdout: result.stdout, stderr: result.stderr };
    }
    if (result.aborted || request.signal.aborted) {
      return { kind: "cancelled", processExited: exited, stdout: result.stdout, stderr: result.stderr };
    }
    if (!exited) {
      return { kind: "unknown", reason: "process-exit", stdout: result.stdout, stderr: result.stderr };
    }
    return { kind: "completed", exitCode: result.exitCode, stdout: result.stdout, stderr: result.stderr };
  } catch {
    if (unknown || spawned) {
      return { kind: "unknown", reason: unknownReason, stdout: "", stderr: "" };
    }
    return { kind: "not-started", reason: request.signal.aborted ? "cancelled" : "spawn-failed" };
  }
};
