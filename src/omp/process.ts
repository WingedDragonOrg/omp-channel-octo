/**
 * 进程与看门狗:OMP 子进程的身份探测(/bin/ps 快照)、进程组信号、有界等待与强制回收,
 * 以及父进程死亡时终止子进程组的看门狗脚本。这里不解析协议,也不组装 worker。
 */

import { workerEnv } from "./profile.ts";
import { failureText } from "../errors.ts";

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

/** Line the watchdog writes once it is running; the parent waits for it after spawn. */
export const WATCHDOG_READY_MARKER = "__omp_watchdog_ready__";
/**
 * How long the watchdog may take to write its ready marker. The factory passes the worker
 * startup budget through, so a loaded host that slows a cold Bun start gets the same allowance
 * for the watchdog as for the OMP child; the wait is still bounded and a real timeout fails.
 */
export const DEFAULT_WATCHDOG_READY_TIMEOUT_MS = 30_000;
/** Head of stderr kept for a failed watchdog start; the drain keeps reading so the pipe never backpressures. */
const WATCHDOG_STDERR_CAPTURE_CHARS = 1_024;
/** Rolling stdout window; the marker is short, so a doubled window cannot cut a split marker. */
const READY_TAIL_CHARS = WATCHDOG_READY_MARKER.length * 2;
/** Bounded wait for a killed watchdog to actually exit, so a failure leaves no child behind. */
const WATCHDOG_SHUTDOWN_GRACE_MS = 1_000;
/** After the watchdog's stdout closes, its exit status almost always settles immediately. */
const WATCHDOG_EXIT_SETTLE_MS = 500;

export type WatchdogFailureReason = "unsupported-platform" | "spawn-failed" | "exited" | "read-failed" | "timeout";

export type WatchdogStartResult =
	| { readonly ok: true; readonly watchdog: RpcChild }
	| { readonly ok: false; readonly reason: WatchdogFailureReason; readonly detail: string };

function spawnWatchdogProcess(childPid: number, workerPgid: number, parentIdentity: string): RpcChild {
	return Bun.spawn(
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
			stderr: "pipe",
			detached: true,
		},
	) as unknown as RpcChild;
}

/**
 * Drains a child stream to EOF keeping at most `maxChars` head characters. The stream is read
 * to completion regardless, so a chatty failing child can never wedge on a full pipe.
 */
async function drainBounded(stream: ReadableStream<Uint8Array>, maxChars: number): Promise<string> {
	const reader = stream.getReader();
	const decoder = new TextDecoder();
	let text = "";
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			if (value && text.length < maxChars) {
				text = (text + decoder.decode(value, { stream: true })).slice(0, maxChars);
			}
		}
	} catch {
		// Diagnostic capture is best effort; whatever was collected before the error is used.
	}
	return text;
}

export async function createWatchdog(
	childPid: number,
	workerPgid: number,
	parentIdentity: string,
	readyTimeoutMs: number = DEFAULT_WATCHDOG_READY_TIMEOUT_MS,
	spawnWatchdog: () => RpcChild = () => spawnWatchdogProcess(childPid, workerPgid, parentIdentity),
): Promise<WatchdogStartResult> {
	if (process.platform === "win32") {
		return { ok: false, reason: "unsupported-platform", detail: "parent-death watchdog is not available on this platform" };
	}
	let watchdog: RpcChild;
	try {
		watchdog = spawnWatchdog();
	} catch (error) {
		return { ok: false, reason: "spawn-failed", detail: `spawn failed: ${failureText(error, "unknown spawn error")}` };
	}
	const stderrDrain = drainBounded(watchdog.stderr, WATCHDOG_STDERR_CAPTURE_CHARS);
	const fail = async (reason: WatchdogFailureReason, detail: string): Promise<WatchdogStartResult> => {
		try {
			watchdog.kill("SIGKILL");
		} catch {
			// The watchdog may have exited between the observation and this cleanup.
		}
		await waitForExit(watchdog, WATCHDOG_SHUTDOWN_GRACE_MS);
		const stderr = failureText(await Promise.race([stderrDrain, Bun.sleep(150).then(() => "")]), "");
		return { ok: false, reason, detail: stderr.length > 0 ? `${detail}; stderr: ${stderr}` : detail };
	};
	const stdoutReader = watchdog.stdout.getReader();
	const decoder = new TextDecoder();
	let tail = "";
	const readyLoop = (async (): Promise<"ready" | "ended" | "read-failed"> => {
		try {
			for (;;) {
				const { done, value } = await stdoutReader.read();
				if (done) return "ended";
				if (value && value.byteLength > 0) {
					tail = (tail + decoder.decode(value, { stream: true })).slice(-READY_TAIL_CHARS);
					if (tail.includes(WATCHDOG_READY_MARKER)) return "ready";
				}
			}
		} catch {
			return "read-failed";
		}
	})();
	const readyObservation = readyLoop.then(kind => ({ kind }));
	const exitObservation = watchdog.exited.then(
		code => ({ kind: "exited" as const, code }),
		() => ({ kind: "exited" as const, code: -1 }),
	);
	let timer: NodeJS.Timeout | undefined;
	const timeoutObservation = new Promise<{ kind: "timeout" }>(resolve => {
		timer = setTimeout(() => resolve({ kind: "timeout" }), Math.max(1, readyTimeoutMs));
	});
	const observation = await Promise.race([readyObservation, exitObservation, timeoutObservation]);
	clearTimeout(timer);
	switch (observation.kind) {
		case "ready":
			await stdoutReader.cancel().catch(() => {});
			return { ok: true, watchdog };
		case "exited":
			return fail("exited", `exited with code ${observation.code} before ready`);
		case "ended": {
			if (await waitForExit(watchdog, WATCHDOG_EXIT_SETTLE_MS)) {
				const code = await watchdog.exited.catch(() => -1);
				return fail("exited", `stdout closed before ready (exit code ${code})`);
			}
			return fail("exited", "stdout closed before ready");
		}
		case "read-failed":
			return fail("read-failed", "stdout read failed");
		case "timeout":
			return fail("timeout", `no ready marker within ${Math.max(1, readyTimeoutMs)}ms`);
	}
}
export function stopWatchdog(watchdog: RpcChild | undefined): void {
	if (!watchdog) return;
	try {
		watchdog.kill("SIGTERM");
	} catch {
		// The watchdog normally exits itself when the worker group is gone.
	}
}
