import { execFile, spawn } from "node:child_process";
import { access, realpath, stat } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { delimiter, join, isAbsolute } from "node:path";
import { promisify } from "node:util";
import { withoutStatusEnvironment } from "../launcher/status-environment.ts";
import { isRecord } from "./type-guards.ts";

export interface OpenZedRequest {
  path: string;
  line?: number;
}

export interface ZedBackendOptions {
  zedExecutable?: string;
  cwd?: string;
  workspace?: boolean;
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
    ...(options.cwd === undefined && !options.workspace ? [] : [
      "/Applications/Zed Preview.app/Contents/MacOS/cli",
      join(homedir(), "Applications", "Zed Preview.app", "Contents", "MacOS", "cli"),
    ]),
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
  stillAuthorized: () => boolean | Promise<boolean>,
): Promise<void> {
  const request = parseRequest(value);
  const isZedUrl = /^zed:\/\//i.test(request.path);
  const [path, executable] = await Promise.all([
    isZedUrl ? request.path : readableFilePath(request.path),
    resolveZedExecutable(options),
  ]);
  let args = [isZedUrl ? path : `${path}:${request.line ?? 1}:1`];
  if (options.workspace) {
    if (path.startsWith("zed://file/")) {
      const directory = await realpath(decodeURIComponent(new URL(path).pathname));
      if (!(await stat(directory)).isDirectory()) throw new Error("chat workspace is not a directory");
      args = [directory];
    } else if (path.startsWith("zed://ssh/")) {
      const remote = new URL(`ssh://${path.slice("zed://ssh/".length)}`);
      const directory = decodeURIComponent(remote.pathname);
      const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
      const host = `${remote.username ? `${decodeURIComponent(remote.username)}@` : ""}${remote.hostname}`;
      const { stdout } = await promisify(execFile)("ssh", [
        "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
        ...(remote.port ? ["-p", remote.port] : []), "--", host,
        `directory=$(realpath -- ${quote(directory)}) && test -d "$directory" && printf '%s' "$directory"`,
      ], { timeout: 15_000, env: withoutStatusEnvironment(process.env) });
      const authority = `${remote.username ? `${remote.username}@` : ""}${remote.host}`;
      args = [`ssh://${authority}${stdout.split("/").map(encodeURIComponent).join("/")}`];
    } else throw new Error("invalid Zed workspace URL");
  }
  if (options.cwd !== undefined) {
    const remote = new URL(`ssh://${path.slice("zed://ssh/".length)}`);
    const match = /:(\d+)(?::(\d+))?$/.exec(remote.pathname);
    const file = decodeURIComponent(match ? remote.pathname.slice(0, -match[0].length) : remote.pathname);
    const fragment = /^#L([1-9]\d*)$/.exec(remote.hash);
    const line = fragment ? `:${fragment[1]}` : match?.[0] ?? "";
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
    const command = `cwd=$(realpath -- ${quote(options.cwd)}) && file=$(realpath -- ${quote(file)}) && printf '%s\\0%s\\0' "$cwd" "$file"`;
    const host = `${remote.username ? `${decodeURIComponent(remote.username)}@` : ""}${remote.hostname}`;
    const sshArgs = ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
      ...(remote.port ? ["-p", remote.port] : []), "--", host, command];
    const { stdout } = await promisify(execFile)("ssh", sshArgs, {
      timeout: 15_000,
      env: withoutStatusEnvironment(process.env),
    }).catch(() => { throw new Error("Could not resolve the remote workspace or file over SSH"); });
    const [cwd, target] = stdout.split("\0");
    if (!cwd || !target || !target.startsWith(cwd === "/" ? "/" : `${cwd}/`)) {
      throw new Error("Zed file is outside the chat workspace");
    }
    const authority = `${remote.username ? `${remote.username}@` : ""}${remote.host}`;
    const encoded = (value: string) => value.split("/").map(encodeURIComponent).join("/");
    args = [`ssh://${authority}${encoded(cwd)}`, `ssh://${authority}${encoded(target)}${line}`];
  }
  if (!await stillAuthorized()) throw new Error("Zed request is no longer current before Zed could be opened");
  const child = spawn(executable, args, { detached: true, shell: false, stdio: "ignore", env: withoutStatusEnvironment(process.env) });
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  let spawned = false;
  child.once("spawn", () => {
    spawned = true;
    child.unref();
    if (options.cwd === undefined && !options.workspace) resolve();
  });
  if (options.cwd !== undefined || options.workspace) {
    child.once("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error("Zed could not open the workspace"));
    });
  }
  child.on("error", (error) => {
    if (spawned && options.cwd === undefined && !options.workspace) process.stderr.write(`[octobuddy-enhancer] Zed process error: ${error.message}\n`);
    else reject(new Error(`could not start Zed: ${error.message}`, { cause: error }));
  });
  await promise;
}
