import { spawn } from "node:child_process";
import { access, realpath, stat } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { delimiter, join, isAbsolute } from "node:path";
import { isRecord } from "./type-guards.ts";

export interface OpenZedRequest {
  path: string;
  line?: number;
}

export interface ZedBackendOptions {
  zedExecutable?: string;
}

function parseRequest(value: unknown): OpenZedRequest {
  if (!isRecord(value)) throw new Error("invalid Zed request");
  if (typeof value.path !== "string" || (!isAbsolute(value.path) && !/^zed:\/\/[^/\s]+/i.test(value.path)) || value.path.includes("\0")) {
    throw new Error("Zed requires an absolute local file path or Zed URL");
  }
  const line = value.line;
  if (line !== undefined && (typeof line !== "number" || !Number.isSafeInteger(line) || line < 1)) {
    throw new Error("Zed line number must be a positive integer");
  }
  return { path: value.path, ...(typeof line === "number" ? { line } : {}) };
}

async function executableFile(path: string): Promise<string | undefined> {
  if (!isAbsolute(path)) return undefined;
  try {
    const resolved = await realpath(path);
    const info = await stat(resolved);
    if (!info.isFile()) return undefined;
    await access(resolved, constants.X_OK);
    return resolved;
  } catch {
    return undefined;
  }
}

async function resolveZedExecutable(options: ZedBackendOptions): Promise<string> {
  if (options.zedExecutable !== undefined) {
    const injected = await executableFile(options.zedExecutable);
    if (!injected) throw new Error(`configured Zed executable is unavailable: ${options.zedExecutable}`);
    return injected;
  }

  const candidates = [
    "/Applications/Zed.app/Contents/MacOS/cli",
    join(homedir(), "Applications", "Zed.app", "Contents", "MacOS", "cli"),
    ...(process.env.PATH ?? "").split(delimiter)
      .filter((directory) => directory.length > 0 && isAbsolute(directory))
      .map((directory) => join(directory, "zed")),
  ];
  for (const candidate of [...new Set(candidates)]) {
    const executable = await executableFile(candidate);
    if (executable) return executable;
  }
  throw new Error("Zed is unavailable; install Zed or make its executable available on PATH");
}

async function readableFilePath(path: string): Promise<string> {
  let resolved: string;
  try { resolved = await realpath(path); }
  catch { throw new Error(`local file is unavailable: ${path}`); }
  try {
    const info = await stat(resolved);
    if (!info.isFile()) throw new Error(`not a regular file: ${path}`);
    await access(resolved, constants.R_OK);
  } catch (error) {
    if (error instanceof Error && error.message === `not a regular file: ${path}`) throw error;
    throw new Error(`local file is unavailable: ${path}`);
  }
  return resolved;
}

export async function openInZed(
  value: unknown,
  options: ZedBackendOptions,
  stillAuthorized: () => boolean,
): Promise<void> {
  const request = parseRequest(value);
  const isZedUrl = /^zed:\/\//i.test(request.path);
  const [path, executable] = await Promise.all([
    isZedUrl ? request.path : readableFilePath(request.path),
    resolveZedExecutable(options),
  ]);
  if (!stillAuthorized()) throw new Error("plugin was disabled before Zed could be opened");

  const argument = isZedUrl ? path : `${path}:${request.line ?? 1}:1`;
  const child = spawn(executable, [argument], { detached: true, shell: false, stdio: "ignore" });
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  let spawned = false;
  child.once("spawn", () => {
    spawned = true;
    child.unref();
    resolve();
  });
  child.on("error", (error) => {
    if (spawned) process.stderr.write(`[octobuddy-enhancer] Zed process error: ${error.message}\n`);
    else reject(new Error(`could not start Zed: ${error.message}`, { cause: error }));
  });
  await promise;
}
