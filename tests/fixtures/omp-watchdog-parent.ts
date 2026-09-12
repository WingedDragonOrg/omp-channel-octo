import { existsSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { createWorkerFactory } from "../../src/omp/index.ts";
import type { WorkerProcess } from "../../src/contracts.ts";

const argv = process.argv.slice(2);

function flag(name: string): string | undefined {
	for (let index = argv.length - 1; index >= 0; index--) {
		const value = argv[index]!;
		if (value === name) return argv[index + 1];
		if (value.startsWith(`${name}=`)) return value.slice(name.length + 1);
	}
	return undefined;
}

const tempDirArg = flag("--temp-dir");
const markerArg = flag("--marker");
const readyArg = flag("--ready");
const processInfoArg = flag("--process-info");
if (!tempDirArg || !markerArg || !readyArg || !processInfoArg) throw new Error("watchdog parent fixture paths are required");

const tempDir = resolve(tempDirArg);
const markerPath = resolve(markerArg);
const readyPath = resolve(readyArg);
const processInfoPath = resolve(processInfoArg);
const workerFixture = resolve(import.meta.dir, "omp-rpc-worker.ts");
const logPath = join(tempDir, "worker.log");

let spawned: WorkerProcess | undefined;
const factory = createWorkerFactory({
	ompCommand: process.execPath,
	extraArgs: [
		workerFixture,
		"--fixture-scenario",
		"cancel",
		"--fixture-log",
		logPath,
		"--fixture-marker",
		markerPath,
	],
	startupTimeoutMs: 2_000,
	cancelTimeoutMs: 800,
});
const worker = await factory.create({
	cwd: tempDir,
	sessionDir: tempDir,
	onSpawn: processInfo => {
		spawned = processInfo;
		writeFileSync(processInfoPath, JSON.stringify(processInfo), "utf8");
	},
});

void worker.prompt("keep the worker alive", () => {}).catch(() => {});
const deadline = Date.now() + 2_000;
while (!existsSync(markerPath) && Date.now() < deadline) await Bun.sleep(5);
if (!spawned || !existsSync(markerPath)) {
	await worker.stop();
	throw new Error("watchdog parent fixture worker did not become ready");
}
writeFileSync(readyPath, `${process.pid}\n`, "utf8");

// Keep this parent alive until the test kills it abruptly.  The worker's
// independent watchdog must then reap the worker group without host cleanup.
setInterval(() => {}, 1_000);
await new Promise<never>(() => {});
