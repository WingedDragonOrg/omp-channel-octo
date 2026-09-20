import { describe, expect, test } from "bun:test";
import {
	DEFAULT_WATCHDOG_READY_TIMEOUT_MS,
	WATCHDOG_READY_MARKER,
	createWatchdog,
	stopWatchdog,
	waitForExit,
	type RpcChild,
} from "../src/omp/process.ts";

/** A real Bun child running an arbitrary script, spawned exactly like the real watchdog child. */
function spawnScript(script: string): RpcChild {
	return Bun.spawn([process.execPath, "-e", script], {
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
		detached: true,
	}) as unknown as RpcChild;
}

function processIsAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
		if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
		throw error;
	}
}

async function waitForProcessExit(pid: number, timeoutMs = 3_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (!processIsAlive(pid)) return;
		await Bun.sleep(10);
	}
	throw new Error(`process ${pid} did not exit within ${timeoutMs}ms`);
}

const NEVER_READY_SCRIPT = "setInterval(() => {}, 1_000);";
// Real clock by design: the production failure was a fixed one-second wall-clock window racing a
// real subprocess startup, so fake timers cannot reproduce it. The child delay is controlled by
// the script, not by machine load, which keeps the reproduction deterministic.
const DELAYED_READY_SCRIPT = `setTimeout(() => { process.stdout.write(${JSON.stringify(`${WATCHDOG_READY_MARKER}\n`)}); setInterval(() => {}, 1_000); }, 1_200);`;

describe("createWatchdog", () => {
	test("starts the real watchdog script within the default budget and stops it cleanly", async () => {
		const start = await createWatchdog(process.pid, 1_073_741_823, "");
		expect(start.ok).toBe(true);
		if (!start.ok) return;
		const pid = start.watchdog.pid;
		expect(pid).toBeGreaterThan(0);
		stopWatchdog(start.watchdog);
		expect(await waitForExit(start.watchdog, 2_000)).toBe(true);
		expect(processIsAlive(pid)).toBe(false);
	}, 10_000);

	test("a ready marker slower than one second but inside the budget still starts", async () => {
		const started = Date.now();
		const start = await createWatchdog(0, 0, "", DEFAULT_WATCHDOG_READY_TIMEOUT_MS, () => spawnScript(DELAYED_READY_SCRIPT));
		const elapsed = Date.now() - started;
		expect(start.ok).toBe(true);
		if (!start.ok) return;
		expect(elapsed).toBeGreaterThanOrEqual(1_000);
		stopWatchdog(start.watchdog);
		expect(await waitForExit(start.watchdog, 2_000)).toBe(true);
	}, 10_000);

	test("a watchdog silent past the ready budget fails bounded with reason timeout and is reaped", async () => {
		// Real clock by design: the bounded-duration claim is about a real timer against a real child.
		let child: RpcChild | undefined;
		const started = Date.now();
		const start = await createWatchdog(0, 0, "", 400, () => (child = spawnScript(NEVER_READY_SCRIPT)));
		const elapsed = Date.now() - started;
		expect(start.ok).toBe(false);
		if (start.ok) return;
		expect(start.reason).toBe("timeout");
		expect(start.detail).toContain("no ready marker within 400ms");
		expect(elapsed).toBeLessThan(4_000);
		expect(child).toBeDefined();
		await waitForProcessExit(child!.pid);
		expect(processIsAlive(child!.pid)).toBe(false);
	}, 10_000);

	test("a watchdog that exits early reports reason exited with its code and a bounded stderr excerpt", async () => {
		const child = () => spawnScript('console.error("fatal watchdog boot failure"); process.exit(3);');
		const start = await createWatchdog(0, 0, "", DEFAULT_WATCHDOG_READY_TIMEOUT_MS, child);
		expect(start.ok).toBe(false);
		if (start.ok) return;
		expect(start.reason).toBe("exited");
		expect(start.detail).toContain("code 3");
		expect(start.detail).toContain("fatal watchdog boot failure");
	}, 10_000);

	test("a watchdog that closes stdout silently reports reason exited with code 0", async () => {
		const start = await createWatchdog(0, 0, "", DEFAULT_WATCHDOG_READY_TIMEOUT_MS, () => spawnScript("process.exit(0);"));
		expect(start.ok).toBe(false);
		if (start.ok) return;
		expect(start.reason).toBe("exited");
		expect(start.detail).toContain("code 0");
	}, 10_000);

	test("a stdout read failure reports reason read-failed and still kills the child", async () => {
		let killed = false;
		const fake: RpcChild = {
			pid: 0,
			stdin: { write: () => 0 },
			stdout: new ReadableStream<Uint8Array>({
				start(controller) {
					controller.error(new Error("pipe exploded"));
				},
			}),
			stderr: new ReadableStream<Uint8Array>({
				start(controller) {
					controller.close();
				},
			}),
			exited: Bun.sleep(50).then(() => 0),
			kill: () => {
				killed = true;
			},
		};
		const started = Date.now();
		const start = await createWatchdog(0, 0, "", 5_000, () => fake);
		expect(start.ok).toBe(false);
		if (start.ok) return;
		expect(start.reason).toBe("read-failed");
		expect(start.detail).toContain("stdout read failed");
		expect(killed).toBe(true);
		expect(Date.now() - started).toBeLessThan(2_000);
	});

	test("a spawn exception reports reason spawn-failed with the bounded error text", async () => {
		const start = await createWatchdog(0, 0, "", 5_000, () => {
			throw new Error("ENOENT: cannot run the bun binary");
		});
		expect(start.ok).toBe(false);
		if (start.ok) return;
		expect(start.reason).toBe("spawn-failed");
		expect(start.detail).toContain("spawn failed");
		expect(start.detail).toContain("ENOENT");
	});
});
