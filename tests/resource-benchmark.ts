import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { heapStats } from "bun:jsc";
import { createWorkerFactory } from "../src/omp/index.ts";
import type { OmpWorker } from "../src/contracts.ts";

// Fixed offline workload. Real RPC workers, watchdogs and descendants; no model or Bot.
// Heap retention is measured after full GC, not peak allocation. RSS is diagnostic:
// allocator/JIT page retention is not by itself evidence of a memory leak.
const WARMUP_CYCLES = 4;
const MEASURED_CYCLES = 24;
const fixture = join(import.meta.dir, "fixtures/omp-rpc-worker.ts");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-resources-")));
type ProcessRow = { pid: number; ppid: number; identity: string };
let residualProcesses = 0;
let completedPrompts = 0;
let cancelledPrompts = 0;
let crashedPrompts = 0;

async function processTable(): Promise<ProcessRow[]> {
	const child = Bun.spawn(["/bin/ps", "-axo", "pid=,ppid=,lstart="], {
		stdin: "ignore", stdout: "pipe", stderr: "pipe",
	});
	const [text, stderr, code] = await Promise.all([
		new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
	]);
	assert.equal(code, 0, `process snapshot failed: ${stderr}`);
	return text.trim().split("\n").map(line => {
		const match = /^\s*(\d+)\s+(\d+)\s+(.+)$/.exec(line);
		assert(match, "invalid process snapshot");
		return { pid: Number(match[1]), ppid: Number(match[2]), identity: match[3]! };
	}).filter(row => row.pid !== child.pid);
}

async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
	let timer: NodeJS.Timeout | undefined;
	try {
		return await Promise.race([promise, new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(`${label} timed out`)), 10_000);
		})]);
	} finally {
		clearTimeout(timer);
	}
}

async function lifecycle(scenario: "normal" | "cancel" | "crash"): Promise<void> {
	const marker = join(root, "descendant.alive");
	const owned = new Map<number, string>();
	let worker: OmpWorker | undefined;
	const rememberChildren = async () => {
		const rows = await processTable();
		const parents = new Set([process.pid]);
		for (let changed = true; changed;) {
			changed = false;
			for (const row of rows) {
				if (parents.has(row.ppid) && !parents.has(row.pid)) {
					parents.add(row.pid);
					owned.set(row.pid, row.identity);
					changed = true;
				}
			}
		}
	};
	try {
		const factory = createWorkerFactory({
			ompCommand: process.execPath,
			extraArgs: [fixture, "--fixture-scenario", scenario, "--fixture-marker", marker],
			startupTimeoutMs: 3_000,
			cancelTimeoutMs: 800,
		});
		worker = await factory.create({ cwd: root, sessionDir: root, onSpawn: () => {} });
		await rememberChildren(); // Includes the separate watchdog process group.
		let finalText = "";
		const prompt = worker.prompt("fixed resource lifecycle workload", event => {
			if (event.type === "text") finalText = event.text;
		}).then(() => true, () => false);
		if (scenario === "normal") {
			assert.equal(await bounded(prompt, "normal prompt"), true);
			assert.equal(finalText, "fixture completed");
			completedPrompts++;
		} else {
			const readyDeadline = performance.now() + 3_000;
			while (!existsSync(marker) && performance.now() < readyDeadline) await Bun.sleep(5);
			assert(existsSync(marker), "descendant readiness timed out");
			const pid = Number((await readFile(marker, "utf8")).trim());
			const descendant = (await processTable()).find(row => row.pid === pid);
			assert(descendant, "descendant must be alive before teardown");
			owned.set(pid, descendant.identity);
			if (scenario === "cancel") {
				assert.equal(await bounded(worker.stop(), "cancel stop"), true);
				assert.equal(await bounded(prompt, "cancelled prompt settlement"), false);
				cancelledPrompts++;
			} else {
				assert.equal(await bounded(prompt, "crashed prompt"), false);
				crashedPrompts++;
			}
		}
		assert.equal(await bounded(worker.stop(), "worker stop"), true);
	} finally {
		if (worker) await bounded(worker.stop(), "cleanup stop");
		await rememberChildren();
		const deadline = performance.now() + 1_500;
		let alive: ProcessRow[];
		do {
			alive = (await processTable()).filter(row => owned.get(row.pid) === row.identity);
			if (alive.length === 0) break;
			await Bun.sleep(20);
		} while (performance.now() < deadline);
		residualProcesses += alive.length;
		// Only identities observed in this benchmark's own process tree may be killed.
		for (const row of alive) {
			try { process.kill(row.pid, "SIGKILL"); } catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
			}
		}
		assert.equal(alive.length, 0, "worker/watchdog/descendant leaked after teardown");
		assert.equal(existsSync(marker), false, "descendant did not finish cleanup");
	}
}

async function cycle(): Promise<void> {
	for (const scenario of ["normal", "cancel", "crash"] as const) await lifecycle(scenario);
}

async function sample() {
	await Bun.sleep(25);
	Bun.gc(true);
	await Bun.sleep(25);
	Bun.gc(true);
	return { heap: heapStats().heapSize, rss: process.memoryUsage.rss() };
}

try {
	for (let i = 0; i < WARMUP_CYCLES; i++) await cycle();
	const baseline = await sample();
	let peakRss = baseline.rss;
	let last = baseline;
	for (let i = 0; i < MEASURED_CYCLES; i++) {
		await cycle();
		last = await sample();
		peakRss = Math.max(peakRss, last.rss);
	}
	assert.equal(completedPrompts, WARMUP_CYCLES + MEASURED_CYCLES);
	assert.equal(cancelledPrompts, completedPrompts);
	assert.equal(crashedPrompts, completedPrompts);
	console.log(`METRIC post_gc_heap_bytes=${last.heap}`);
	console.log(`METRIC retained_heap_growth_bytes=${Math.max(0, last.heap - baseline.heap)}`);
	console.log(`METRIC heap_delta_bytes=${last.heap - baseline.heap}`);
	console.log(`METRIC rss_growth_bytes=${last.rss - baseline.rss}`);
	console.log(`METRIC post_gc_peak_rss_bytes=${peakRss}`);
	console.log(`METRIC measured_worker_lifecycles=${MEASURED_CYCLES * 3}`);
} finally {
	console.log(`METRIC residual_processes=${residualProcesses}`);
	await rm(root, { recursive: true, force: true });
}
