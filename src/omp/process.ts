/**
 * 进程与看门狗:OMP 子进程的身份探测(/bin/ps 快照)、进程组信号、有界等待与强制回收,
 * 以及父进程死亡时终止子进程组的看门狗脚本。这里不解析协议,也不组装 worker。
 */

import { workerEnv } from "./profile.ts";

export type RpcChild = {
	readonly pid: number;
	readonly stdin: {
		write(data: string | Uint8Array): unknown;
		end?: () => unknown;
	};
	readonly stdout: ReadableStream<Uint8Array>;
	readonly stderr: ReadableStream<Uint8Array>;
	readonly exited: Promise<number>;
	kill(signal?: NodeJS.Signals): void;
};

export type ProcessSnapshot = {
	identity: string;
	pgid: number;
};

export const PROCESS_POLL_INTERVAL_MS = 20;
export const SAFE_MIN_PID = 2;
/**
 * The watchdog is deliberately a separate process group.  If the channel
 * process is killed, the watchdog remains alive long enough to terminate the
 * RPC worker's group and any tool descendants.  It also reaps descendants
 * after an unexpected worker-parent exit.
 */
const WATCHDOG_SCRIPT = String.raw`
const watchdogArgs = process.argv.slice(-4);
const parentPid = Number(watchdogArgs[0]);
const workerPid = Number(watchdogArgs[1]);
const workerPgid = Number(watchdogArgs[2]);
const parentIdentity = watchdogArgs[3] ?? "";
const decoder = new TextDecoder();
let terminating = false;
let killDeadline = 0;

function processAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid < 2) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error && typeof error === "object" && error.code === "EPERM";
  }
}

function processStart(pid) {
  try {
    const result = Bun.spawnSync(["/bin/ps", "-p", String(pid), "-o", "lstart="]);
    if (result.exitCode !== 0) return "";
    return decoder.decode(result.stdout).trim();
  } catch {
    return "";
  }
}

function parentAlive() {
  if (!processAlive(parentPid)) return false;
  if (!parentIdentity) return true;
  const current = processStart(parentPid);
  return current.length > 0 && ("pid=" + parentPid + ";start=" + current) === parentIdentity;
}

function groupAlive() {
  if (!Number.isSafeInteger(workerPgid) || workerPgid < 2) return false;
  try {
    process.kill(-workerPgid, 0);
    return true;
  } catch (error) {
    return error && typeof error === "object" && error.code === "EPERM";
  }
}

function signalGroup(signal) {
  if (!groupAlive()) return false;
  try {
    process.kill(-workerPgid, signal);
    return true;
  } catch {
    return false;
  }
}

function beginTermination() {
  if (!terminating) {
    terminating = true;
    killDeadline = Date.now() + 10000;
    signalGroup("SIGTERM");
  }
  if (!groupAlive()) {
    process.exit(0);
    return;
  }
  if (Date.now() >= killDeadline) signalGroup("SIGKILL");
}

process.stdout.write("__omp_watchdog_ready__\n");
setInterval(() => {
  if (terminating) {
    beginTermination();
    return;
  }
  if (!parentAlive() || !processAlive(workerPid)) beginTermination();
}, 100);
`;

export function isSafeGroupId(pgid: number): boolean {
	return Number.isSafeInteger(pgid) && pgid >= SAFE_MIN_PID && pgid !== process.pid;
}

export async function queryProcess(pid: number): Promise<ProcessSnapshot | undefined> {
	if (!Number.isSafeInteger(pid) || pid < SAFE_MIN_PID) return undefined;
	if (process.platform === "win32") return undefined;
	let child: RpcChild;
	try {
		child = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "lstart=", "-o", "pgid="], {
			stdin: "ignore",
			stdout: "pipe",
			stderr: "ignore",
		}) as unknown as RpcChild;
	} catch {
		return undefined;
	}
	const [output, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited.catch(() => -1)]);
	if (exitCode !== 0) return undefined;
	const match = output.trim().match(/^(.*?)\s+(\d+)$/s);
	if (!match) return undefined;
	const startTime = match[1];
	const pgidText = match[2];
	if (startTime === undefined || pgidText === undefined) return undefined;
	const pgid = Number(pgidText);
	if (!Number.isSafeInteger(pgid) || pgid < SAFE_MIN_PID) return undefined;
	return { identity: `pid=${pid};start=${startTime.trim()}`, pgid };
}

export function processAlive(pid: number): boolean {
	if (!Number.isSafeInteger(pid) || pid < SAFE_MIN_PID) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if (error && typeof error === "object" && "code" in error && error.code === "EPERM") return true;
		return false;
	}
}

export function groupAlive(pgid: number): boolean {
	if (!isSafeGroupId(pgid)) return false;
	try {
		process.kill(-pgid, 0);
		return true;
	} catch (error) {
		return typeof error === "object" && error !== null && "code" in error && error.code === "EPERM";
	}
}

export function signalGroup(pgid: number, signal: NodeJS.Signals): boolean {
	if (process.platform === "win32" || !isSafeGroupId(pgid)) return false;
	try {
		process.kill(-pgid, signal);
		return true;
	} catch {
		return false;
	}
}

export async function waitForExit(child: RpcChild, timeoutMs: number): Promise<boolean> {
	let timer: NodeJS.Timeout;
	const timeout = new Promise<boolean>(resolve => {
		timer = setTimeout(() => resolve(false), Math.max(0, timeoutMs));
	});
	const exited = child.exited.then(
		() => true,
		() => true,
	);
	const result = await Promise.race([exited, timeout]);
	clearTimeout(timer!);
	return result;
}

export async function waitForGroupGone(pgid: number, timeoutMs: number): Promise<boolean> {
	if (!isSafeGroupId(pgid)) return false;
	const deadline = Date.now() + Math.max(0, timeoutMs);
	while (groupAlive(pgid) && Date.now() < deadline) await Bun.sleep(PROCESS_POLL_INTERVAL_MS);
	return !groupAlive(pgid);
}

export async function terminateProcessGroup(pgid: number, cancelTimeoutMs: number): Promise<boolean> {
	if (process.platform === "win32" || !isSafeGroupId(pgid)) return false;
	signalGroup(pgid, "SIGTERM");
	if (await waitForGroupGone(pgid, cancelTimeoutMs)) return true;
	signalGroup(pgid, "SIGKILL");
	return waitForGroupGone(pgid, Math.min(1_000, cancelTimeoutMs));
}

export async function terminateOwnedProcess(child: RpcChild, pgid: number, cancelTimeoutMs: number): Promise<boolean> {
	const safeGroup = process.platform !== "win32" && isSafeGroupId(pgid);
	if (safeGroup) signalGroup(pgid, "SIGTERM");
	else {
		try {
			child.kill("SIGTERM");
		} catch {
			// The child may have exited between the identity check and this call.
		}
	}
	await waitForExit(child, cancelTimeoutMs);
	let groupStopped = safeGroup ? await waitForGroupGone(pgid, cancelTimeoutMs) : process.platform === "win32" && !processAlive(child.pid);
	if (!groupStopped) {
		if (safeGroup) signalGroup(pgid, "SIGKILL");
		try {
			child.kill("SIGKILL");
		} catch {
			// Already exited is acceptable; group verification below is decisive.
		}
		await waitForExit(child, Math.min(1_000, cancelTimeoutMs));
		groupStopped = safeGroup
			? await waitForGroupGone(pgid, Math.min(1_000, cancelTimeoutMs))
			: process.platform === "win32" && !processAlive(child.pid);
	}
	return groupStopped;
}

export async function createWatchdog(childPid: number, workerPgid: number, parentIdentity: string): Promise<RpcChild | undefined> {
	if (process.platform === "win32") return undefined;
	let watchdog: RpcChild;
	try {
		watchdog = Bun.spawn(
			[
				process.execPath,
				"-e",
				WATCHDOG_SCRIPT,
				String(process.pid),
				String(childPid),
				String(workerPgid),
				parentIdentity,
			],
			{
				stdin: "ignore",
				env: workerEnv(),
				stdout: "pipe",
				stderr: "ignore",
				detached: true,
			},
		) as unknown as RpcChild;
	} catch {
		return undefined;
	}
	const reader = watchdog.stdout.getReader();
	const decoder = new TextDecoder();
	const ready = (async () => {
		let output = "";
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) return false;
				output += decoder.decode(result.value, { stream: true });
				if (output.includes("__omp_watchdog_ready__")) return true;
			}
		} catch {
			return false;
		}
	})();
	let timer: NodeJS.Timeout;
	const timeout = new Promise<boolean>(resolve => {
		timer = setTimeout(() => resolve(false), 1_000);
	});
	const isReady = await Promise.race([ready, timeout]);
	clearTimeout(timer!);
	if (!isReady) {
		try {
			watchdog.kill("SIGKILL");
		} catch {
			// The watchdog may have exited after a syntax or startup failure.
		}
		return undefined;
	}
	await reader.cancel().catch(() => {});
	return watchdog;
}
export function stopWatchdog(watchdog: RpcChild | undefined): void {
	if (!watchdog) return;
	try {
		watchdog.kill("SIGTERM");
	} catch {
		// The watchdog normally exits itself when the worker group is gone.
	}
}
