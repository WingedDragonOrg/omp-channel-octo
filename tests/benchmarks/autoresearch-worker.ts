/**
 * /autoresearch worker-side capability benchmark.
 *
 * Runs the 5 fixed worker cases against the real production worker (createWorkerFactory,
 * real child processes on stdin/stdout RPC) driven by tests/fixtures/autoresearch-rpc-
 * worker.ts — a deterministic offline stand-in for OMP's autoresearch extension.
 *
 * Cases 1–3 measure whether the extension's notify frames become visible WorkerEvents
 * (a capability gap of the current adapter is a valid measurement, not an error);
 * cases 4–5 measure the goal turn: the streamed final body, the willContinue
 * continuation, and the four experiment tools' execution start/end frames.
 *
 * Only AssertionError becomes a failed check; setup, protocol, timeout and cleanup
 * failures are infrastructure errors and throw.
 */

import assert from "node:assert/strict";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { OmpWorker, WorkerEvent, WorkerProcess } from "../../src/contracts.ts";
import type { ExecutionEvent } from "../../src/cards/execution-tracker.ts";
import { createWorkerFactory } from "../../src/omp/index.ts";
import { check, type CheckResult } from "./autoresearch-types.ts";

const fixturePath = resolve(import.meta.dir, "../fixtures/autoresearch-rpc-worker.ts");

/** Bounded guards only: they trip on real hangs, never on a capability gap. */
const STARTUP_TIMEOUT_MS = 15_000;
const PROMPT_TIMEOUT_MS = 10_000;
const EXIT_TIMEOUT_MS = 5_000;
const CANCEL_TIMEOUT_MS = 1_000;

/** Literal upstream texts the fixture emits; expectations are never derived from output. */
const NOTIFY_ENABLED = "Autoresearch enabled—describe what to optimize in your next message.";
const NOTIFY_DISABLED = "Autoresearch mode disabled";
const NOTIFY_DIRTY =
	"Worktree is dirty (src/index.ts). Commit or stash these changes before starting autoresearch — a fresh autoresearch/* branch needs a clean baseline.";
const GOAL_PROMPT = "/autoresearch tune autoresearch.sh until the offline score stops improving";
const GOAL_BODY = "autoresearch loop done: ran 3 experiments and kept the best offline score";
const RESUME_BODY = "autoresearch resume: no pending run, holding current baseline";
const EXPERIMENT_TOOL_NAMES = ["init_experiment", "run_experiment", "log_experiment", "update_notes"] as const;

/**
 * Visibility of an event means a human-readable body on the event itself: the `text`
 * or `message` field (a future dedicated notice event would carry one of them).
 * Structured summaries and re-stringified frames deliberately do not count.
 */
function visibleText(event: WorkerEvent): string | undefined {
	const record = event as unknown as Record<string, unknown>;
	const text = record.text;
	if (typeof text === "string") return text;
	const message = record.message;
	if (typeof message === "string") return message;
	return undefined;
}

function processIsAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ESRCH") return false;
		if (code === "EPERM") return true;
		throw error;
	}
}

async function waitForProcessExit(pid: number): Promise<void> {
	const deadline = Date.now() + EXIT_TIMEOUT_MS;
	while (Date.now() < deadline) {
		if (!processIsAlive(pid)) return;
		await Bun.sleep(10);
	}
	throw new Error(`worker process ${pid} did not exit within ${EXIT_TIMEOUT_MS}ms`);
}

/** A prompt whose settle is bounded; a trip means the worker hung, so it throws. */
async function promptBounded(worker: OmpWorker, text: string, events: WorkerEvent[]): Promise<void> {
	let timer: NodeJS.Timeout | number | undefined;
	const watchdog = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new Error(`prompt "${text}" did not settle within ${PROMPT_TIMEOUT_MS}ms`)), PROMPT_TIMEOUT_MS);
	});
	try {
		await Promise.race([worker.prompt(text, event => events.push(event)), watchdog]);
	} finally {
		clearTimeout(timer);
	}
}

/** Runs `run` against a real worker in a temp cwd/sessionDir; stop and rm failures throw. */
async function withWorker<T>(worktreeDirty: boolean, run: (worker: OmpWorker) => Promise<T>): Promise<T> {
	const tempDir = await realpath(await mkdtemp(join(tmpdir(), "autoresearch-worker-")));
	const extraArgs = [
		fixturePath,
		"--cwd",
		join(tempDir, "wrong-cwd"),
		"--session-dir",
		join(tempDir, "wrong-session-dir"),
		...(worktreeDirty ? ["--fixture-worktree-dirty"] : []),
	];
	const factory = createWorkerFactory({
		ompCommand: process.execPath,
		extraArgs,
		startupTimeoutMs: STARTUP_TIMEOUT_MS,
		cancelTimeoutMs: CANCEL_TIMEOUT_MS,
	});
	let spawned: WorkerProcess | undefined;
	let worker: OmpWorker | undefined;
	try {
		worker = await factory.create({
			cwd: tempDir,
			sessionDir: tempDir,
			onSpawn: processInfo => {
				spawned = processInfo;
			},
		});
		return await run(worker);
	} finally {
		if (worker) {
			const stopped = await worker.stop();
			if (!stopped) throw new Error("autoresearch fixture worker did not stop cleanly");
			if (spawned) await waitForProcessExit(spawned.pid);
		}
		await rm(tempDir, { recursive: true, force: true });
	}
}

function assertNotifyVisible(events: readonly WorkerEvent[], expected: string): void {
	const visible = events
		.map(event => visibleText(event))
		.filter((text): text is string => text !== undefined);
	assert.ok(
		visible.some(text => text.includes(expected)),
		`notify text not visible in worker events; visible texts: ${JSON.stringify(visible)}`,
	);
}

function toolEvents<T extends "tool_start" | "tool_end">(
	events: readonly WorkerEvent[],
	type: T,
): Extract<ExecutionEvent, { type: T }>[] {
	return events.filter((event): event is Extract<ExecutionEvent, { type: T }> => event.type === type);
}

export async function measureWorker(): Promise<CheckResult[]> {
	const cleanEvents: { enable: WorkerEvent[]; off: WorkerEvent[]; goal: WorkerEvent[] } = { enable: [], off: [], goal: [] };
	await withWorker(false, async worker => {
		await promptBounded(worker, "/autoresearch", cleanEvents.enable);
		await promptBounded(worker, "/autoresearch off", cleanEvents.off);
		await promptBounded(worker, GOAL_PROMPT, cleanEvents.goal);
	});
	const dirtyEvents: WorkerEvent[] = [];
	await withWorker(true, async worker => {
		await promptBounded(worker, "/autoresearch", dirtyEvents);
	});

	const results: CheckResult[] = [];
	results.push(
		await check("worker/notify-enable-visible", () => assertNotifyVisible(cleanEvents.enable, NOTIFY_ENABLED)),
	);
	results.push(
		await check("worker/notify-off-visible", () => assertNotifyVisible(cleanEvents.off, NOTIFY_DISABLED)),
	);
	results.push(
		await check("worker/notify-branch-error-visible", () => assertNotifyVisible(dirtyEvents, NOTIFY_DIRTY)),
	);
	results.push(
		await check("worker/goal-continuation-visible", () => {
			const finalTexts = cleanEvents.goal
				.filter((event): event is Extract<WorkerEvent, { type: "text" }> => event.type === "text" && event.final)
				.map(event => event.text);
			assert.deepEqual(
				finalTexts,
				[GOAL_BODY, RESUME_BODY],
				"goal turn final body and the body after agent_end willContinue:true must stay visible",
			);
		}),
	);
	results.push(
		await check("worker/experiment-tools-visible", () => {
			const starts = toolEvents(cleanEvents.goal, "tool_start");
			const ends = toolEvents(cleanEvents.goal, "tool_end");
			for (const toolName of EXPERIMENT_TOOL_NAMES) {
				const start = starts.find(event => event.toolName === toolName);
				assert.ok(
					start,
					`tool_start for ${toolName} not observable; tool starts: ${JSON.stringify(starts.map(event => event.toolName))}`,
				);
				const end = ends.find(event => event.toolCallId === start.toolCallId);
				assert.ok(end, `tool_end for ${toolName} (${start.toolCallId}) not observable`);
				assert.equal(end.status, "succeeded", `tool_end for ${toolName} must succeed`);
				assert.ok(
					cleanEvents.goal.indexOf(start) < cleanEvents.goal.indexOf(end),
					`tool_start for ${toolName} must precede its tool_end`,
				);
			}
			const lastToolEnd = Math.max(...ends.map(event => cleanEvents.goal.indexOf(event)));
			const finalBodyIndex = cleanEvents.goal.findIndex(
				event => event.type === "text" && event.final && event.text === RESUME_BODY,
			);
			assert.ok(finalBodyIndex > lastToolEnd, "the run's final assistant body must complete after the experiment tools");
		}),
	);
	return results;
}
