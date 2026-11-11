import { open, readFile, unlink } from "node:fs/promises";
import { spawn, type ChildProcess } from "node:child_process";
import { launcherPaths, writeJsonAtomically, type LauncherPaths } from "./status.ts";
import { processStartIdentity } from "./identity.ts";

interface LockRecord {
  controllerPid: number;
  controllerStartedAt: string;
  monitorPid?: number;
  monitorStartedAt?: string;
}

const lockfPath = "/usr/bin/lockf";
const lockReadyLine = "LOCK_HELD";
const lockAcquireTimeoutMs = 10_000;

export class AlreadyRunningError extends Error {
  constructor(message = "OctoBuddy Enhancer is already running") {
    super(message);
    this.name = "AlreadyRunningError";
  }
}

async function isSameLiveProcess(pid: number | undefined, startedAt: string | undefined): Promise<boolean> {
  if (pid === undefined || startedAt === undefined) return false;
  try {
    return await processStartIdentity(pid) === startedAt;
  } catch (error) {
    if (error instanceof Error && error.message === `process ${pid} is not running`) return false;
    throw error;
  }
}

function isLockRecord(value: unknown): value is LockRecord {
  if (!value || typeof value !== "object" || !("controllerPid" in value) || !("controllerStartedAt" in value)) {
    return false;
  }
  if (typeof value.controllerPid !== "number" || !Number.isSafeInteger(value.controllerPid)
    || typeof value.controllerStartedAt !== "string") {
    return false;
  }
  if (("monitorPid" in value && value.monitorPid !== undefined
      && (typeof value.monitorPid !== "number" || !Number.isSafeInteger(value.monitorPid)))
    || ("monitorStartedAt" in value && value.monitorStartedAt !== undefined && typeof value.monitorStartedAt !== "string")) {
    return false;
  }
  return true;
}

async function readLock(path: string): Promise<LockRecord | null | undefined> {
  let contents: string;
  try {
    contents = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }

  try {
    const value: unknown = JSON.parse(contents);
    return isLockRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

function waitForChildExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (hasExited(child)) return Promise.resolve(true);
  return new Promise<boolean>((resolve) => {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const finish = (exited: boolean) => {
      if (timeout) clearTimeout(timeout);
      child.removeListener("exit", onExit);
      resolve(exited);
    };
    const onExit = () => finish(true);
    timeout = setTimeout(() => finish(false), timeoutMs);
    child.once("exit", onExit);
  });
}

async function releaseGuard(child: ChildProcess): Promise<void> {
  if (hasExited(child)) return;
  child.stdin?.end();
  if (await waitForChildExit(child, 2_000)) return;
  child.kill("SIGTERM");
  if (await waitForChildExit(child, 1_000)) return;
  child.kill("SIGKILL");
  await waitForChildExit(child, 1_000);
}

function waitForGuardReady(child: ChildProcess, guardPath: string): Promise<void> {
  const output = child.stdout;
  const diagnostics = child.stderr;
  if (!output || !diagnostics || !child.stdin) {
    return Promise.reject(new Error("could not establish launcher lock process pipes"));
  }

  return new Promise<void>((resolve, reject) => {
    let buffer = "";
    let errorText = "";
    let settled = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      output.removeListener("data", onData);
      diagnostics.removeListener("data", onDiagnostics);
      child.removeListener("error", onError);
      child.removeListener("exit", onExit);
      if (error) reject(error);
      else resolve();
    };
    const onData = (chunk: Buffer | string) => {
      buffer += chunk.toString();
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      const line = buffer.slice(0, newline).trim();
      finish(line === lockReadyLine
        ? undefined
        : new Error(`launcher lock helper returned unexpected readiness output: ${line}`));
    };
    const onDiagnostics = (chunk: Buffer | string) => {
      if (errorText.length < 4_096) errorText += chunk.toString();
    };
    const onError = (error: Error) => {
      finish(new Error("could not start launcher lock helper", { cause: error }));
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      if (code === 75) {
        finish(new AlreadyRunningError());
        return;
      }
      const details = errorText.trim();
      finish(new Error(`launcher lock helper exited before readiness (code=${code}, signal=${signal})${details ? `: ${details}` : ""}`));
    };

    output.on("data", onData);
    diagnostics.on("data", onDiagnostics);
    child.once("error", onError);
    child.once("exit", onExit);
    timeout = setTimeout(() => {
      child.stdin?.end();
      child.kill("SIGTERM");
      finish(new Error(`timed out acquiring launcher lock ${guardPath}`));
    }, lockAcquireTimeoutMs);
  });
}

async function acquireGuard(path: string): Promise<ChildProcess> {
  const file = await open(path, "a", 0o600);
  await file.close();

  const child = spawn(lockfPath, [
    "-k", "-s", "-t", "0", path,
    "/bin/sh", "-c", `printf '${lockReadyLine}\\n'; exec /bin/cat`,
  ], { stdio: ["pipe", "pipe", "pipe"] });
  try {
    await waitForGuardReady(child, path);
    return child;
  } catch (error) {
    await releaseGuard(child);
    throw error;
  }
}

export interface LauncherLock {
  paths: LauncherPaths;
  setMonitorIdentity(pid: number, startedAt: string): Promise<void>;
  release(): Promise<void>;
}

export async function acquireLauncherLock(stateDir: string): Promise<LauncherLock> {
  const paths = launcherPaths(stateDir);
  const guard = await acquireGuard(paths.lockGuardFile);
  let released = false;

  try {
    const controllerStartedAt = await processStartIdentity(process.pid);
    const record: LockRecord = { controllerPid: process.pid, controllerStartedAt };
    const existing = await readLock(paths.lockFile);
    if (existing === null) {
      throw new AlreadyRunningError("Launcher state is being initialized; retry after it settles");
    }
    if (existing) {
      if (await isSameLiveProcess(existing.controllerPid, existing.controllerStartedAt)
        || await isSameLiveProcess(existing.monitorPid, existing.monitorStartedAt)) {
        throw new AlreadyRunningError();
      }
      await unlink(paths.lockFile);
    }

    const file = await open(paths.lockFile, "wx", 0o600);
    try {
      await file.writeFile(`${JSON.stringify(record)}\n`, "utf8");
    } catch (error) {
      await unlink(paths.lockFile).catch(() => {});
      throw error;
    } finally {
      await file.close();
    }

    return {
      paths,
      async setMonitorIdentity(pid, startedAt) {
        if (released) throw new Error("launcher lock has already been released");
        record.monitorPid = pid;
        record.monitorStartedAt = startedAt;
        await writeJsonAtomically(paths.lockFile, record);
      },
      async release() {
        if (released) return;
        released = true;
        try {
          const current = await readLock(paths.lockFile);
          const monitorAlive = await isSameLiveProcess(record.monitorPid, record.monitorStartedAt);
          if (!monitorAlive
            && current?.controllerPid === record.controllerPid
            && current.controllerStartedAt === record.controllerStartedAt) {
            await unlink(paths.lockFile).catch(() => {});
          }
        } finally {
          await releaseGuard(guard);
        }
      },
    };
  } catch (error) {
    await releaseGuard(guard);
    throw error;
  }
}
