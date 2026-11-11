import { chmod, lstat, mkdir, rename, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";

export type LauncherStatus =
  | { state: "starting"; message: string; updatedAt: string }
  | { state: "ready"; message: string; pid: number; port: number; updatedAt: string }
  | { state: "error"; message: string; pid?: number; port?: number; updatedAt: string }
  | { state: "stopped"; message: string; updatedAt: string };

export interface LauncherPaths {
  stateDir: string;
  statusFile: string;
  lockFile: string;
  lockGuardFile: string;
  consentFile: string;
  targetFile: string;
  hostFile: string;
  monitorStopFile: string;
}

export function launcherPaths(stateDir: string): LauncherPaths {
  return {
    stateDir,
    statusFile: join(stateDir, "status.json"),
    lockFile: join(stateDir, "launcher.lock"),
    lockGuardFile: join(stateDir, "launcher.guard"),
    consentFile: join(stateDir, "cdp-consent.json"),
    targetFile: join(stateDir, "target.json"),
    hostFile: join(stateDir, "host.json"),
    monitorStopFile: join(stateDir, "monitor-stop"),
  };
}

export async function ensureStateDirectory(stateDir: string): Promise<void> {
  let existed = true;
  try {
    await lstat(stateDir);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    existed = false;
  }
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const directory = await lstat(stateDir);
  if (!directory.isDirectory() || directory.isSymbolicLink()) {
    throw new Error(`launcher state path is not a real directory: ${stateDir}`);
  }
  if (process.getuid && directory.uid !== process.getuid()) {
    throw new Error(`launcher state directory is not owned by the current user: ${stateDir}`);
  }
  if (existed && (directory.mode & 0o077) !== 0) {
    throw new Error(`launcher state directory must not be accessible by other users: ${stateDir}`);
  }
  if (!existed) await chmod(stateDir, 0o700);
}

export async function writeJsonAtomically(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(value)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
  await rename(temporaryPath, path);
}

export async function writeLauncherStatus(stateDir: string, status: LauncherStatus): Promise<void> {
  const paths = launcherPaths(stateDir);
  await writeJsonAtomically(paths.statusFile, status);
}

