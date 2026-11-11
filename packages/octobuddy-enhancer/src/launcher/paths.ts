import { execFile } from "node:child_process";
import { access, realpath, stat } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { basename, join, resolve } from "node:path";
import { promisify } from "node:util";

import { processStartIdentity } from "./identity.ts";
const execFileAsync = promisify(execFile);
const systemPath = "/usr/bin:/bin:/usr/sbin:/sbin";
const commandEnvironment = { ...process.env, PATH: systemPath, LC_ALL: "C", TZ: "UTC" };

export interface OctoBuddyApp {
  bundlePath: string;
  executablePath: string;
}

async function isExecutable(path: string): Promise<boolean> {
  try {
    await stat(path);
    await access(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

export async function resolveOctoBuddyApp(explicitPath?: string): Promise<OctoBuddyApp> {
  const candidates = explicitPath
    ? [resolve(explicitPath)]
    : ["/Applications/OctoBuddy.app", join(homedir(), "Applications", "OctoBuddy.app")];
  for (const bundlePath of candidates) {
    const executablePath = join(bundlePath, "Contents", "MacOS", "OctoBuddy");
    if (await isExecutable(executablePath)) {
      return { bundlePath, executablePath: await realpath(executablePath) };
    }
  }
  throw new Error(explicitPath
    ? `OctoBuddy app bundle is not executable: ${explicitPath}`
    : "OctoBuddy.app was not found in /Applications or ~/Applications");
}

async function startIdentityIfRunning(pid: number): Promise<string | undefined> {
  try {
    return await processStartIdentity(pid);
  } catch (error) {
    if (error instanceof Error && error.message === `process ${pid} is not running`) return undefined;
    throw new Error(`cannot verify process identity for PID ${pid}`, { cause: error });
  }
}

async function executablePathsFor(pid: number): Promise<string[]> {
  const { stdout } = await execFileAsync(
    "/usr/sbin/lsof",
    ["-a", "-p", String(pid), "-d", "txt", "-Fn"],
    {
      env: commandEnvironment,
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 5_000,
    },
  );
  return stdout
    .split("\n")
    .filter((line) => line.startsWith("n"))
    .map((line) => line.slice(1).replace(/\r$/, ""))
    .filter(Boolean);
}

export async function findRunningOctoBuddy(expectedExecutablePath: string): Promise<number | undefined> {
  let targetExecutablePath: string;
  try {
    targetExecutablePath = await realpath(expectedExecutablePath);
  } catch (error) {
    throw new Error("cannot resolve OctoBuddy executable identity", { cause: error });
  }

  let stdout: string;
  try {
    ({ stdout } = await execFileAsync("/bin/ps", ["-axo", "pid=,comm="], {
      env: commandEnvironment,
      encoding: "utf8",
      maxBuffer: 2 * 1024 * 1024,
      timeout: 5_000,
    }));
  } catch (error) {
    throw new Error("cannot determine whether OctoBuddy is already running", { cause: error });
  }

  for (const line of stdout.split("\n")) {
    const match = line.trim().match(/^(\d+)\s+(.+)$/);
    if (!match) continue;
    const pid = Number(match[1]);
    // The command name only narrows candidates; the executable path below confirms identity.
    if (pid === process.pid || basename(match[2]!.trim()) !== "OctoBuddy") continue;

    const startedAt = await startIdentityIfRunning(pid);
    if (startedAt === undefined) continue;

    let executablePaths: string[];
    try {
      executablePaths = await executablePathsFor(pid);
    } catch (error) {
      const confirmedStart = await startIdentityIfRunning(pid);
      if (confirmedStart === undefined) continue;
      if (confirmedStart !== startedAt) {
        throw new Error(`process ${pid} changed during executable identity verification`, { cause: error });
      }
      throw new Error(`cannot verify executable identity for process ${pid}`, { cause: error });
    }

    const confirmedStart = await startIdentityIfRunning(pid);
    if (confirmedStart === undefined) continue;
    if (confirmedStart !== startedAt) {
      throw new Error(`process ${pid} changed during executable identity verification`);
    }
    if (executablePaths.length === 0) {
      throw new Error(`cannot verify executable identity for process ${pid}`);
    }
    if (executablePaths.includes(targetExecutablePath)) return pid;
    if (executablePaths.some((path) => path.endsWith(" (deleted)")
      && path.slice(0, -" (deleted)".length) === targetExecutablePath)) {
      throw new Error(`cannot verify whether process ${pid} uses the current OctoBuddy executable`);
    }
  }
  return undefined;
}
