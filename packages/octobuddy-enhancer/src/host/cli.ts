import { access, lstat, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { assertOwnedCdp, assertProcessExecutable } from "../launcher/identity.ts";

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

interface HostArguments {
  port: number;
  pid: number;
  startedAt: string;
  appPath: string;
}

function parseArguments(args: string[]): HostArguments {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    if (name !== "--port" && name !== "--pid" && name !== "--started-at" && name !== "--app-path") {
      throw new Error(`unknown host argument: ${name ?? ""}`);
    }
    const value = args[index + 1];
    if (!value || value.startsWith("--")) throw new Error(`missing value for ${name}`);
    if (values.has(name)) throw new Error(`duplicate host argument: ${name}`);
    values.set(name, value);
    index += 1;
  }

  const port = Number(values.get("--port"));
  const pid = Number(values.get("--pid"));
  const startedAt = values.get("--started-at");
  const appPath = values.get("--app-path");
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) throw new Error("invalid CDP port");
  if (!Number.isSafeInteger(pid) || pid < 1) throw new Error("invalid OctoBuddy process ID");
  if (!startedAt || startedAt.trim() !== startedAt) throw new Error("invalid OctoBuddy process start identity");
  if (!appPath || !isAbsolute(appPath)) throw new Error("invalid absolute OctoBuddy app bundle path");
  return { port, pid, startedAt, appPath: resolve(appPath) };
}

async function pluginDirectories(): Promise<string[]> {
  const directories = [join(packageDirectory, "plugins")];
  const userDirectory = join(homedir(), ".config", "octobuddy-enhancer", "plugins");
  try {
    const root = await lstat(userDirectory);
    if (!root.isDirectory() || root.isSymbolicLink() || root.uid !== process.getuid?.() || (root.mode & 0o077) !== 0) {
      throw new Error(`user plugin directory must be owned by the current user and private: ${userDirectory}`);
    }
    const resolved = await realpath(userDirectory);
    if (resolved !== userDirectory) throw new Error(`user plugin directory is not canonical: ${userDirectory}`);
    await access(userDirectory, constants.R_OK | constants.X_OK);
    directories.push(userDirectory);
  } catch (error) {
    if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") return directories;
    throw error;
  }
  return directories;
}

function errorMessage(error: unknown): string {
  if (error instanceof AggregateError) {
    const details = [...error.errors].map(errorMessage);
    return details.length > 0 ? `${error.message}: ${details.join("; ")}` : error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(error !== null && typeof error === "object" && "code" in error && error.code === "ESRCH");
  }
}

async function run(args: string[]): Promise<number> {
  let session: { stop(): Promise<void> } | undefined;
  let stopping = false;
  let failed = false;
  let failure: unknown;
  let exitCode = 0;
  const shutdown = () => { stopping = true; };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  try {
    const parsed = parseArguments(args);
    const identity = { ...parsed, appPath: await realpath(parsed.appPath) };
    await assertProcessExecutable(identity.pid, identity.startedAt, join(identity.appPath, "Contents", "MacOS", "OctoBuddy"));
    await assertOwnedCdp(identity);
    // Load the CDP runtime only after the process, executable, and listener are verified.
    const { startHost } = await import("./index.ts");
    const hostSession = await startHost({ ...identity, pluginDirectories: await pluginDirectories() });
    session = hostSession;
    if (!stopping) {
      process.stdout.write(`${JSON.stringify({ type: "ready" })}\n`);
      while (!stopping) {
        const { promise, resolve: resume } = Promise.withResolvers<void>();
        setTimeout(resume, 1_000);
        await promise;
        if (stopping) break;
        try {
          await assertOwnedCdp(identity);
        } catch (error) {
          if (stopping || !processIsAlive(identity.pid)) break;
          throw error;
        }
      }
    }
  } catch (error) {
    failed = true;
    failure = error;
  } finally {
    if (session) {
      try {
        await session.stop();
      } catch (error) {
        failure = failed ? new AggregateError([failure, error], "host failed and shutdown was incomplete") : error;
        failed = true;
      }
    }
    process.off("SIGINT", shutdown);
    process.off("SIGTERM", shutdown);
  }

  if (failed) {
    const message = errorMessage(failure);
    process.stdout.write(`${JSON.stringify({ type: "error", message })}\n`);
    process.stderr.write(`${message}\n`);
    exitCode = 1;
  }
  return exitCode;
}

if (import.meta.main) process.exitCode = await run(process.argv.slice(2));
