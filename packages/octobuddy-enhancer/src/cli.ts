import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { readFile, unlink, writeFile } from "node:fs/promises";
import type { ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { assertOwnedCdp, processStartIdentity } from "./launcher/identity.ts";
import { startOctoBuddy } from "./launcher/app.ts";
import { requestCdpConsent } from "./launcher/consent.ts";
import { acquireLauncherLock, type LauncherLock } from "./launcher/lock.ts";
import { findRunningOctoBuddy, resolveOctoBuddyApp } from "./launcher/paths.ts";
import { showLauncherNotification } from "./launcher/notifications.ts";
import { startManagedService, waitForProcessExit, type ManagedService, type ServiceExit } from "./launcher/service.ts";
import {
  ensureStateDirectory,
  launcherPaths,
  writeJsonAtomically,
  writeLauncherStatus,
  type LauncherStatus,
} from "./launcher/status.ts";

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const monitorScript = join(packageDir, "src", "launcher", "monitor.ts");
const hostScript = join(packageDir, "src", "host", "cli.ts");
const defaultStateDir = join(homedir(), "Library", "Application Support", "OctoBuddy Enhancer");

interface CliOptions {
  appPath?: string;
  stateDir: string;
  dock: boolean;
}

interface ProcessIdentity {
  pid: number;
  startedAt: string;
}

interface TargetIdentity extends ProcessIdentity {
  port: number;
}

type LifecycleExit =
  | { service: "target"; exit: ServiceExit }
  | { service: "host"; exit: ServiceExit }
  | { service: "monitor"; exit: ServiceExit };

function parseArgs(args: string[]): CliOptions {
  let appPath: string | undefined;
  let stateDir: string | undefined;
  let dock = false;
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]!;
    if (argument === "--dock") {
      if (dock) throw new Error("duplicate --dock option");
      dock = true;
      continue;
    }
    if (argument === "--app-path" || argument === "--state-dir") {
      const value = args[index + 1];
      if (!value || value.startsWith("--")) throw new Error(`${argument} requires a path`);
      index += 1;
      if (argument === "--app-path") {
        if (appPath) throw new Error("duplicate --app-path option");
        appPath = value;
      } else {
        if (stateDir) throw new Error("duplicate --state-dir option");
        stateDir = value;
      }
      continue;
    }
    throw new Error(`unknown option: ${argument}`);
  }
  return { ...(appPath === undefined ? {} : { appPath }), stateDir: resolve(stateDir ?? defaultStateDir), dock };
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function processIsAlive(identity: ProcessIdentity): Promise<boolean> {
  try {
    return await processStartIdentity(identity.pid) === identity.startedAt;
  } catch (error) {
    if (error instanceof Error && error.message === `process ${identity.pid} is not running`) return false;
    throw error;
  }
}

async function readStatus(path: string): Promise<LauncherStatus | undefined> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as LauncherStatus;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

async function waitForLifecycle(
  target: ChildProcess,
  host: ChildProcess,
  monitor: ChildProcess,
): Promise<LifecycleExit> {
  return await Promise.race([
    waitForProcessExit(target).then((exit) => ({ service: "target" as const, exit })),
    waitForProcessExit(host).then((exit) => ({ service: "host" as const, exit })),
    waitForProcessExit(monitor).then((exit) => ({ service: "monitor" as const, exit })),
  ]);
}

async function requestMonitorStop(monitor: ChildProcess, stateDir: string): Promise<boolean> {
  if (monitor.exitCode !== null || monitor.signalCode !== null) return true;
  const stopFile = launcherPaths(stateDir).monitorStopFile;
  try {
    await writeFile(stopFile, "stop\n", { encoding: "utf8", mode: 0o600, flag: "wx" });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") return false;
  }
  const stopped = await Promise.race([
    waitForProcessExit(monitor).then(() => true),
    new Promise<boolean>((resolveDelay) => setTimeout(() => resolveDelay(false), 2_500)),
  ]);
  return stopped || monitor.exitCode !== null || monitor.signalCode !== null;
}

async function clearPreviousRunState(paths: ReturnType<typeof launcherPaths>): Promise<void> {
  for (const path of [paths.targetFile, paths.hostFile, paths.monitorStopFile]) {
    try {
      await unlink(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
}

function notifyDock(options: CliOptions, message: string): void {
  if (options.dock) showLauncherNotification(message);
}

async function runWithLock(options: CliOptions, lock: LauncherLock): Promise<number> {
  const paths = lock.paths;
  let monitor: ManagedService | undefined;
  let targetPid: number | undefined;
  let targetIdentity: TargetIdentity | undefined;
  let host: ManagedService | undefined;
  let exitCode = 1;
  let finalError: string | undefined;

  try {
    const app = await resolveOctoBuddyApp(options.appPath);
    const existingPid = await findRunningOctoBuddy(app.executablePath);
    if (existingPid !== undefined) {
      throw new Error(`OctoBuddy is already running (PID ${existingPid}); close it before launching the enhancer`);
    }

    if (!(await requestCdpConsent(options.dock, options.stateDir))) {
      await writeLauncherStatus(options.stateDir, {
        state: "stopped",
        message: "CDP consent was not granted; OctoBuddy was not launched",
        updatedAt: new Date().toISOString(),
      });
      process.stdout.write("CDP consent not granted; OctoBuddy was not launched.\n");
      return 0;
    }

    await clearPreviousRunState(paths);
    await writeLauncherStatus(options.stateDir, {
      state: "starting",
      message: "Starting independent process monitor",
      updatedAt: new Date().toISOString(),
    });

    monitor = startManagedService(monitorScript, ["--state-dir", options.stateDir], "Process monitor", {
      detached: true,
      timeoutMs: 10_000,
    });
    const monitorStartedAt = await monitor.startedAt;
    await lock.setMonitorIdentity(monitor.child.pid!, monitorStartedAt);
    const monitorReadiness = await monitor.ready;
    if (!monitorReadiness.ok) throw monitorReadiness.error;
    monitor.child.unref();
    monitor.child.stdout?.destroy();

    const started = startOctoBuddy(app);
    const targetProcess = started.child;
    targetPid = started.pid;
    const portOutcomePromise = started.port.then(
      (port) => ({ ok: true as const, port }),
      (error: unknown) => ({ ok: false as const, error: error instanceof Error ? error : new Error(String(error)) }),
    );
    const startedAt = await processStartIdentity(started.pid);
    targetIdentity = { pid: started.pid, startedAt, port: 0 };
    await writeJsonAtomically(paths.targetFile, { pid: started.pid, startedAt });
    const portOutcome = await portOutcomePromise;
    if (!portOutcome.ok) throw portOutcome.error;
    targetIdentity.port = portOutcome.port;
    await writeJsonAtomically(paths.targetFile, targetIdentity);

    await assertOwnedCdp(targetIdentity);

    host = startManagedService(hostScript, [
      "--app-path", app.bundlePath,
      "--port", String(targetIdentity.port),
      "--pid", String(targetIdentity.pid),
      "--started-at", targetIdentity.startedAt,
    ], "OctoBuddy Enhancer host", { detached: false, timeoutMs: 30_000 });
    const hostStartedAt = await host.startedAt;
    await writeJsonAtomically(paths.hostFile, { pid: host.child.pid, startedAt: hostStartedAt });
    const hostReadiness = await host.ready;
    if (!hostReadiness.ok) throw hostReadiness.error;

    await writeLauncherStatus(options.stateDir, {
      state: "ready",
      message: "OctoBuddy and enhancer host are running",
      pid: targetIdentity.pid,
      port: targetIdentity.port,
      updatedAt: new Date().toISOString(),
    });

    const lifecycle = await waitForLifecycle(targetProcess, host.child, monitor.child);
    if (lifecycle.service === "target") {
      await writeLauncherStatus(options.stateDir, {
        state: "stopped",
        message: "OctoBuddy exited",
        updatedAt: new Date().toISOString(),
      });
      exitCode = 0;
    } else if (lifecycle.service === "host") {
      if (await processIsAlive(targetIdentity)) {
        finalError = `OctoBuddy Enhancer host exited (code=${lifecycle.exit.code}, signal=${lifecycle.exit.signal}); OctoBuddy remains running with CDP enabled. Close OctoBuddy if the session is no longer needed.`;
        await writeLauncherStatus(options.stateDir, {
          state: "error",
          message: finalError,
          pid: targetIdentity.pid,
          port: targetIdentity.port,
          updatedAt: new Date().toISOString(),
        });
        notifyDock(options, finalError);
        process.stderr.write(`${finalError}\n`);
      } else {
        await writeLauncherStatus(options.stateDir, {
          state: "stopped",
          message: "OctoBuddy exited",
          updatedAt: new Date().toISOString(),
        });
        exitCode = 0;
      }
    } else {
      const currentStatus = await readStatus(paths.statusFile);
      if (currentStatus?.state === "error") {
        finalError = currentStatus.message;
        process.stderr.write(`${finalError}\n`);
      } else if (await processIsAlive(targetIdentity)) {
        finalError = `Process monitor exited unexpectedly (code=${lifecycle.exit.code}, signal=${lifecycle.exit.signal}); OctoBuddy remains running with CDP enabled. Close OctoBuddy if the session is no longer needed.`;
        await writeLauncherStatus(options.stateDir, {
          state: "error",
          message: finalError,
          pid: targetIdentity.pid,
          port: targetIdentity.port,
          updatedAt: new Date().toISOString(),
        });
        notifyDock(options, finalError);
        process.stderr.write(`${finalError}\n`);
      } else {
        await writeLauncherStatus(options.stateDir, {
          state: "stopped",
          message: "OctoBuddy exited",
          updatedAt: new Date().toISOString(),
        });
        exitCode = 0;
      }
    }
    return exitCode;
  } catch (error) {
    const detail = errorText(error);
    finalError = targetPid !== undefined
      ? `${detail}; OctoBuddy PID ${targetPid} may still have CDP enabled. Close it if the session is no longer needed.`
      : detail;
    await writeLauncherStatus(options.stateDir, {
      state: "error",
      message: finalError,
      ...(targetIdentity ? { pid: targetIdentity.pid, ...(targetIdentity.port ? { port: targetIdentity.port } : {}) } : targetPid !== undefined ? { pid: targetPid } : {}),
      updatedAt: new Date().toISOString(),
    }).catch(() => {});
    notifyDock(options, finalError);
    process.stderr.write(`${finalError}\n`);
    return 1;
  } finally {
    if (monitor) await requestMonitorStop(monitor.child, options.stateDir);
    await lock.release();
  }
}

async function main(args: string[]): Promise<number> {
  const options = parseArgs(args);
  await ensureStateDirectory(options.stateDir);
  const lock = await acquireLauncherLock(options.stateDir);
  return await runWithLock(options, lock);
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${errorText(error)}\n`);
    process.exitCode = 1;
  }
}
