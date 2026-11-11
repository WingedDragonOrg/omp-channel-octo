import { realpath } from "node:fs/promises";
import { execFile } from "node:child_process";

const systemPath = "/usr/bin:/bin:/usr/sbin:/sbin";
const commandEnvironment = { ...process.env, PATH: systemPath, LC_ALL: "C", TZ: "UTC" };

function execute(file: string, args: string[]): Promise<string> {
  const { promise, resolve, reject } = Promise.withResolvers<string>();
  execFile(file, args, {
    encoding: "utf8",
    env: commandEnvironment,
    maxBuffer: 1024 * 1024,
    timeout: 5_000,
  }, (error, stdout) => {
    // macOS ps reports a missing PID as status 1 with no output.
    const missingProcess = file === "/bin/ps"
      && args[0] === "-p"
      && error?.code === 1
      && stdout.trim() === "";
    if (error && !missingProcess) reject(error);
    else resolve(stdout);
  });
  return promise;
}

export async function processStartIdentity(pid: number): Promise<string> {
  if (!Number.isSafeInteger(pid) || pid < 1) throw new Error(`invalid process ID: ${pid}`);
  const startedAt = (await execute("/bin/ps", ["-p", String(pid), "-o", "lstart="])).trim();
  if (!startedAt) throw new Error(`process ${pid} is not running`);
  return startedAt;
}

export interface OwnedCdpIdentity {
  pid: number;
  startedAt: string;
  port: number;
}

export async function assertOwnedCdp({ pid, startedAt, port }: OwnedCdpIdentity): Promise<void> {
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`invalid CDP port: ${port}`);
  }
  if (!startedAt || startedAt.trim() !== startedAt) throw new Error("invalid process start identity");

  const actualStart = await processStartIdentity(pid);
  if (actualStart !== startedAt) {
    throw new Error(`process ${pid} start identity changed`);
  }

  let listenerOutput: string;
  try {
    listenerOutput = await execute("/usr/sbin/lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"]);
  } catch (error) {
    throw new Error(`cannot verify listener on CDP port ${port}`, { cause: error });
  }
  const listeners = [...new Set(listenerOutput.split(/\s+/).filter(Boolean).map(Number))];
  if (listeners.length !== 1 || listeners[0] !== pid) {
    throw new Error(`CDP port ${port} is not exclusively owned by process ${pid}`);
  }
  const confirmedStart = await processStartIdentity(pid);
  if (confirmedStart !== startedAt) throw new Error(`process ${pid} start identity changed during CDP verification`);
}
async function readExecutablePaths(pid: number): Promise<string[]> {
  const output = await execute("/usr/sbin/lsof", ["-a", "-p", String(pid), "-d", "txt", "-Fn"]);
  return output
    .split("\n")
    .filter((line) => line.startsWith("n"))
    .map((line) => line.slice(1).replace(/\r$/, ""))
    .filter(Boolean);
}

export async function assertProcessExecutable(pid: number, startedAt: string, executablePath: string): Promise<void> {
  if (!Number.isSafeInteger(pid) || pid < 1) throw new Error(`invalid process ID: ${pid}`);
  if (!startedAt || startedAt.trim() !== startedAt) throw new Error("invalid process start identity");

  let expectedExecutable: string;
  try {
    expectedExecutable = await realpath(executablePath);
  } catch (error) {
    throw new Error("cannot resolve expected executable identity", { cause: error });
  }

  const actualStart = await processStartIdentity(pid);
  if (actualStart !== startedAt) throw new Error(`process ${pid} start identity changed`);

  let executablePaths: string[];
  try {
    executablePaths = await readExecutablePaths(pid);
  } catch (error) {
    const confirmedStart = await processStartIdentity(pid);
    if (confirmedStart !== startedAt) {
      throw new Error(`process ${pid} changed during executable identity verification`, { cause: error });
    }
    throw new Error(`cannot verify executable identity for process ${pid}`, { cause: error });
  }

  const confirmedStart = await processStartIdentity(pid);
  if (confirmedStart !== startedAt) throw new Error(`process ${pid} start identity changed during executable verification`);
  if (executablePaths.length === 0) throw new Error(`cannot verify executable identity for process ${pid}`);
  if (executablePaths.includes(expectedExecutable)) return;
  if (executablePaths.some((path) => path.endsWith(" (deleted)")
    && path.slice(0, -" (deleted)".length) === expectedExecutable)) {
    throw new Error(`cannot verify whether process ${pid} uses the current executable`);
  }
  throw new Error(`process ${pid} does not use expected executable ${expectedExecutable}`);
}
