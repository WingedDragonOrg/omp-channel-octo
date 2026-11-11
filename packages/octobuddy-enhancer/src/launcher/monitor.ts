import { access, readFile, unlink } from "node:fs/promises";
import { launcherPaths, writeLauncherStatus } from "./status.ts";
import { processStartIdentity } from "./identity.ts";
import { showLauncherNotification } from "./notifications.ts";

interface ProcessIdentity {
  pid: number;
  startedAt: string;
}

async function readIdentity(path: string): Promise<ProcessIdentity | undefined> {
  let content: string;
  try {
    content = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  const value: unknown = JSON.parse(content);
  if (!value || typeof value !== "object") throw new Error(`invalid process identity file: ${path}`);
  const identity = value as Partial<ProcessIdentity>;
  if (!Number.isInteger(identity.pid) || typeof identity.startedAt !== "string") {
    throw new Error(`invalid process identity file: ${path}`);
  }
  return identity as ProcessIdentity;
}

async function readController(path: string): Promise<ProcessIdentity> {
  let content: string;
  try {
    content = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") throw new Error("launcher lock disappeared while monitoring");
    throw error;
  }
  const value: unknown = JSON.parse(content);
  if (!value || typeof value !== "object") throw new Error("launcher lock is invalid");
  const record = value as { controllerPid?: unknown; controllerStartedAt?: unknown };
  if (!Number.isInteger(record.controllerPid) || typeof record.controllerStartedAt !== "string") {
    throw new Error("launcher lock has no controller process identity");
  }
  return { pid: record.controllerPid as number, startedAt: record.controllerStartedAt };
}

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

async function processIsAlive(identity: ProcessIdentity): Promise<boolean> {
  try {
    return await processStartIdentity(identity.pid) === identity.startedAt;
  } catch (error) {
    if (error instanceof Error && error.message === `process ${identity.pid} is not running`) return false;
    throw error;
  }
}

function announceReady(): void {
  process.stdout.write(`${JSON.stringify({ type: "ready" })}\n`);
}

async function reportMonitorError(stateDir: string, message: string): Promise<void> {
  await writeLauncherStatus(stateDir, { state: "error", message, updatedAt: new Date().toISOString() });
}

export async function runMonitor(stateDir: string): Promise<void> {
  const paths = launcherPaths(stateDir);
  let stopping = false;
  const stop = () => { stopping = true; };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);

  try {
    await processStartIdentity(process.pid);
    announceReady();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await reportMonitorError(stateDir, `Process monitor could not initialize: ${message}`).catch(() => {});
    throw error;
  }

  try {
    while (!stopping) {
      if (await exists(paths.monitorStopFile)) {
        await unlink(paths.monitorStopFile).catch(() => {});
        return;
      }
      const controller = await readController(paths.lockFile);
      const controllerAlive = await processIsAlive(controller);
      const target = await readIdentity(paths.targetFile);
      if (!target) {
        if (!controllerAlive) {
          const message = "Launcher exited before registering OctoBuddy; an app started during shutdown may still have CDP enabled. Check its state and close OctoBuddy if needed.";
          await reportMonitorError(stateDir, message);
          showLauncherNotification(message);
          process.exitCode = 1;
          return;
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 750));
        continue;
      }
      if (!(await processIsAlive(target))) {
        await writeLauncherStatus(stateDir, { state: "stopped", message: "OctoBuddy exited", updatedAt: new Date().toISOString() });
        return;
      }
      const host = await readIdentity(paths.hostFile);
      if (!host && !controllerAlive) {
        const message = "Launcher exited before starting the enhancer host; OctoBuddy remains running with CDP enabled. Close OctoBuddy if the session is no longer needed.";
        await writeLauncherStatus(stateDir, {
          state: "error",
          message,
          pid: target.pid,
          updatedAt: new Date().toISOString(),
        });
        showLauncherNotification(message);
        process.exitCode = 1;
        return;
      }
      if (host && !(await processIsAlive(host))) {
        const message = "OctoBuddy Enhancer host exited while OctoBuddy is still running. CDP remains enabled; close OctoBuddy if the session is no longer needed.";
        await writeLauncherStatus(stateDir, {
          state: "error",
          message,
          pid: target.pid,
          updatedAt: new Date().toISOString(),
        });
        showLauncherNotification(message);
        process.exitCode = 1;
        return;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 750));
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const target = await readIdentity(paths.targetFile).catch(() => undefined);
    const detail = `Process monitor failed: ${message}${target ? "; OctoBuddy may still have CDP enabled—close it if the session is no longer needed." : ""}`;
    await reportMonitorError(stateDir, detail).catch(() => {});
    showLauncherNotification(detail);
    process.exitCode = 1;
  } finally {
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
  }
}

function parseArgs(args: string[]): string {
  if (args.length !== 2 || args[0] !== "--state-dir" || !args[1]) {
    throw new Error("usage: monitor.ts --state-dir <directory>");
  }
  return args[1];
}

if (import.meta.main) {
  try {
    const stateDir = parseArgs(process.argv.slice(2));
    await runMonitor(stateDir);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stdout.write(`${JSON.stringify({ type: "error", message })}\n`);
    process.exitCode = 1;
  }
}
