import { mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, test } from "bun:test";
import { createWorkerFactory } from "../src/omp/index.ts";
import { TaskStartupError } from "../src/contracts.ts";
import type { HostTool, OmpWorker, PromptContext, WorkerEvent, WorkerOptions, WorkerProcess } from "../src/contracts.ts";
import type { RpcChild } from "../src/omp/process.ts";

const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
const watchdogParentFixturePath = resolve(import.meta.dir, "fixtures/omp-watchdog-parent.ts");

type LogEntry = Record<string, unknown>;

type Harness = {
	tempDir: string;
	logPath: string;
	markerPath: string;
	/** Environment the fixture child actually received; written only with `envReport`. */
	envPath: string;
	worker: OmpWorker;
	spawned: WorkerProcess | undefined;
	cleanup: () => Promise<void>;
};

const harnesses = new Set<Harness>();

afterEach(async () => {
	for (const harness of [...harnesses]) {
		await harness.cleanup();
		harnesses.delete(harness);
	}
});

async function readLog(path: string): Promise<LogEntry[]> {
	try {
		const contents = await readFile(path, "utf8");
		return contents
			.split("\n")
			.filter(line => line.length > 0)
			.map(line => JSON.parse(line) as LogEntry);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw error;
	}
}

async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 1500): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await Bun.sleep(10);
	}
	throw new Error(`condition was not met within ${timeoutMs}ms`);
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

async function waitForProcessExit(pid: number, timeoutMs = 1500): Promise<void> {
	await waitUntil(() => !processIsAlive(pid), timeoutMs);
}

async function withHarness<T>(
	scenario: string,
	run: (harness: Harness) => Promise<T>,
	options: {
		descendantDelayMs?: number;
		sessionFile?: string;
		envReport?: boolean;
		cliEnv?: () => Record<string, string> | undefined;
		model?: string;
	} = {},
): Promise<T> {
	const tempDir = await realpath(await mkdtemp(join(tmpdir(), "omp-rpc-worker-")));
	const logPath = join(tempDir, "fixture.log");
	const markerPath = join(tempDir, "descendant.alive");
	const envPath = join(tempDir, "child-env.json");
	const conflictingCwd = join(tempDir, "wrong-cwd");
	const conflictingSessionDir = join(tempDir, "wrong-session-dir");
	const extraArgs = [
		fixturePath,
		"--mode",
		"interactive",
		"--approval-mode",
		"always-ask",
		"--cwd",
		conflictingCwd,
		"--session-dir",
		conflictingSessionDir,
		"--fixture-scenario",
		scenario,
		"--fixture-log",
		logPath,
		"--fixture-marker",
		markerPath,
		"--fixture-descendant-delay",
		String(options.descendantDelayMs ?? 0),
		...(options.envReport === true ? ["--env-out", envPath] : []),
	];
	const requestedSessionFile =
		options.sessionFile && !options.sessionFile.startsWith("/") ? join(tempDir, options.sessionFile) : options.sessionFile;
	const factory = createWorkerFactory({
		ompCommand: process.execPath,
		extraArgs,
		startupTimeoutMs: 2_000,
		cancelTimeoutMs: 800,
		...(options.cliEnv === undefined ? {} : { cliEnv: options.cliEnv }),
	});
	let spawned: WorkerProcess | undefined;
	const workerOptions: WorkerOptions = {
		cwd: tempDir,
		sessionDir: tempDir,
		...(requestedSessionFile === undefined ? {} : { sessionFile: requestedSessionFile }),
		...(options.model === undefined ? {} : { model: options.model }),
		onSpawn: processInfo => {
			spawned = processInfo;
		},
	};
	const worker = await factory.create(workerOptions);
	const harness: Harness = {
		tempDir,
		logPath,
		markerPath,
		envPath,
		worker,
		get spawned() {
			return spawned;
		},
		cleanup: async () => {
			try {
				await worker.stop();
			} catch {
				// Individual tests assert the stop result they care about.  Cleanup
				// must still remove the fixture after a crash-path assertion.
			}
			if (spawned) {
				await waitUntil(() => !processIsAlive(spawned!.pid), 1_500).catch(() => {});
			}
			try {
				await rm(tempDir, { recursive: true, force: true });
			} catch {
				// Best effort only; process assertions happen before cleanup.
			}
		},
	};
	harnesses.add(harness);
	try {
		return await run(harness);
	} finally {
		await harness.cleanup();
		harnesses.delete(harness);
	}
}

function commandTypes(entries: LogEntry[]): string[] {
	return entries
		.filter(entry => entry.kind === "command")
		.map(entry => {
			const command = entry.command;
			return command && typeof command === "object" && "type" in command && typeof command.type === "string"
				? command.type
				: "<invalid>";
		});
}

function startupEntry(entries: LogEntry[]): LogEntry {
	const startup = entries.find(entry => entry.kind === "startup");
	if (!startup) throw new Error("fixture startup record was not written");
	return startup;
}

function argumentValue(argv: unknown, name: string): string | undefined {
	if (!Array.isArray(argv)) return undefined;
	for (let index = argv.length - 1; index >= 0; index--) {
		const value = argv[index];
		if (value === name && typeof argv[index + 1] === "string") return argv[index + 1] as string;
		if (typeof value === "string" && value.startsWith(`${name}=`)) return value.slice(name.length + 1);
	}
	return undefined;
}

function resultTextFromCommand(command: unknown): string {
	if (!command || typeof command !== "object" || Array.isArray(command)) return String(command ?? "");
	const result = (command as LogEntry).result;
	if (!result || typeof result !== "object" || Array.isArray(result)) return String(result ?? "");
	const content = (result as LogEntry).content;
	if (!Array.isArray(content)) return String(result);
	const text = content.find(
		value => value && typeof value === "object" && !Array.isArray(value) && (value as LogEntry).type === "text",
	);
	return text && typeof text === "object" && !Array.isArray(text) && typeof (text as LogEntry).text === "string"
		? (text as LogEntry).text as string
		: String(result);
}

function hasLoneSurrogate(text: string): boolean {
	for (let index = 0; index < text.length; index++) {
		const code = text.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = text.charCodeAt(index + 1);
			if (!(next >= 0xdc00 && next <= 0xdfff)) return true;
			index++;
		} else if (code >= 0xdc00 && code <= 0xdfff) return true;
	}
	return false;
}

/**
 * Read-only views of the worker's `research_update` ExecutionEvent branch. The production
 * types are still being built (RED phase), so the tests pin the observable wire shape with
 * local views instead of importing a module that does not exist yet; the tag comparison
 * widens through a plain string constant, the boundary cast is named and local.
 */
type ResearchRunView = {
	runNumber: number | null;
	segment: number;
	metric: number;
	status: "keep" | "discard" | "crash" | "checks_failed";
	description: string;
	flagged: boolean;
};

type ResearchSnapshotView = {
	sessionId: number | null;
	name: string;
	goal: string;
	segment: number;
	metricName: string;
	metricUnit: string;
	direction: "lower" | "higher";
	baseline: number | null;
	best: number | null;
	completedRuns: number;
	maxExperiments: number | null;
	phase: "ready" | "running" | "awaiting_log" | "limit_reached";
	activeRun: number | null;
	latestMetric: number | null;
	recent: readonly ResearchRunView[];
	totalRuns: number;
};

type ResearchEventView = { type: string; atMs: number; research: ResearchSnapshotView; report?: string };

/**
 * RED-phase widening: the production WorkerEvent union does not name the research branch
 * yet, so the tag is a plain string and the comparison never fabricates a shape.
 */
const RESEARCH_UPDATE_TAG: string = "research_update";

function researchUpdates(events: readonly WorkerEvent[]): ResearchEventView[] {
	const updates: ResearchEventView[] = [];
	for (const event of events) {
		if (event.type !== RESEARCH_UPDATE_TAG) continue;
		// Same object, literal widening only; the test bodies assert every field of this
		// locally pinned wire view against hand-derived expectations.
		const viewed = event as unknown as ResearchEventView;
		updates.push(viewed);
	}
	return updates;
}

/**
 * The effective phase transitions, collapsing consecutive repeats: how often an identical
 * phase is re-notified is an implementation detail; the order of the real transitions is not.
 */
function researchPhases(events: readonly ResearchEventView[]): ResearchSnapshotView["phase"][] {
	const phases: ResearchSnapshotView["phase"][] = [];
	for (const event of events) {
		if (phases[phases.length - 1] !== event.research.phase) phases.push(event.research.phase);
	}
	return phases;
}

/** Rows of the lower-is-better loop exactly as they must surface in `recent`. */
const legacyProbeView: ResearchRunView = {
	runNumber: null,
	segment: 1,
	metric: 1,
	status: "discard",
	description: "migrated regression probe",
	flagged: false,
};
const run8View: ResearchRunView = { runNumber: 8, segment: 1, metric: 10, status: "keep", description: "fresh baseline", flagged: false };
const run9View: ResearchRunView = { runNumber: 9, segment: 1, metric: 8, status: "keep", description: "cache-friendly layout", flagged: false };
const run10View: ResearchRunView = {
	runNumber: 10,
	segment: 1,
	metric: 0,
	status: "keep",
	description: "suspicious perfect score",
	flagged: true,
};

const run2BrokenView: ResearchRunView = { runNumber: 2, segment: 0, metric: 6, status: "keep", description: "first honest keep", flagged: false };

const run1HigherView: ResearchRunView = { runNumber: 1, segment: 0, metric: 0, status: "keep", description: "zero baseline", flagged: false };
const run2HigherView: ResearchRunView = { runNumber: 2, segment: 0, metric: 15, status: "discard", description: "discarded spike", flagged: false };
const run3HigherView: ResearchRunView = { runNumber: 3, segment: 0, metric: 7, status: "keep", description: "widened batch", flagged: false };

describe("OMP RPC worker", () => {
	test("starts a real child with enforced RPC/yolo/cwd flags and reads state after ready", async () => {
		await withHarness("normal", async harness => {
			expect(harness.spawned?.pid).toBeGreaterThan(0);
			expect(harness.spawned?.pgid).toBeGreaterThan(0);
			expect(harness.spawned?.cwd).toBe(harness.tempDir);
			expect(harness.spawned?.identity).toBeTruthy();
			expect(harness.worker.sessionId).toBe("fixture-session-normal");
			expect(harness.worker.sessionFile).toBe(join(harness.tempDir, "fixture-normal.jsonl"));

			await waitUntil(async () => (await readLog(harness.logPath)).some(entry => entry.kind === "command"));
			const entries = await readLog(harness.logPath);
			const startup = startupEntry(entries);
			expect(startup.cwd).toBe(harness.tempDir);
			expect(startup.cwdArg).toBe(harness.tempDir);
			expect(argumentValue(startup.argv, "--mode")).toBe("rpc");
			expect(argumentValue(startup.argv, "--approval-mode")).toBe("yolo");
			expect(argumentValue(startup.argv, "--cwd")).toBe(harness.tempDir);
			expect(argumentValue(startup.argv, "--session-dir")).toBe(harness.tempDir);
		});
	});
	test("carries the CLI credential names into the child and strips every other OCTO_ variable", async () => {
		const hostToken = Bun.env["OCTO_BOT_TOKEN"];
		Bun.env["OCTO_BOT_TOKEN"] = "host-secret-token";
		try {
			await withHarness(
				"normal",
				async harness => {
					const childEnv = JSON.parse(await readFile(harness.envPath, "utf8")) as Record<string, string>;
					expect(childEnv["OCTO_CONFIG_DIR"]).toBe("/tmp/oco-cred/bot-1");
					expect(childEnv["OCTO_API_BASE_URL"]).toBe("https://im.example.test");
					expect(childEnv["OCTO_BOT_ID"]).toBe("bot-1");
					// Both the host's token and anything a caller smuggles past the API stay out.
					expect(childEnv["OCTO_BOT_TOKEN"]).toBeUndefined();
					for (const name of Object.keys(childEnv)) {
						if (!name.startsWith("OCTO_")) continue;
						expect(["OCTO_CONFIG_DIR", "OCTO_API_BASE_URL", "OCTO_BOT_ID"]).toContain(name);
					}
				},
				{
					envReport: true,
					cliEnv: () => ({
						OCTO_CONFIG_DIR: "/tmp/oco-cred/bot-1",
						OCTO_API_BASE_URL: "https://im.example.test",
						OCTO_BOT_ID: "bot-1",
						OCTO_BOT_TOKEN: "smuggled-token",
					}),
				},
			);
		} finally {
			if (hostToken === undefined) delete Bun.env["OCTO_BOT_TOKEN"];
			else Bun.env["OCTO_BOT_TOKEN"] = hostToken;
		}
	});

	test("materializes a valid fresh session before returning the worker", async () => {
		await withHarness("normal", async harness => {
			expect(existsSync(harness.worker.sessionFile)).toBe(true);
			const firstLine = (await readFile(harness.worker.sessionFile, "utf8")).trim().split("\n")[0];
			if (firstLine === undefined) throw new Error("fixture session file is empty");
			const header = JSON.parse(firstLine) as LogEntry;
			expect(header).toMatchObject({
				type: "session",
				version: 3,
				id: "fixture-session-normal",
				cwd: harness.tempDir,
			});
		});
	});

	test("passes an explicit session file for recovery instead of selecting a recent session", async () => {
		await withHarness("normal", async harness => {
			expect(harness.worker.sessionFile).toBe(join(harness.tempDir, "resume.jsonl"));
			expect(existsSync(harness.worker.sessionFile)).toBe(true);
			const entries = await readLog(harness.logPath);
			const startup = startupEntry(entries);
			expect(argumentValue(startup.argv, "--resume")).toBe(join(harness.tempDir, "resume.jsonl"));
			expect(commandTypes(entries)).not.toContain("new_session");
		}, { sessionFile: "resume.jsonl" });
	});

	test("starts with the chat's model and reads the model catalog for /switch", async () => {
		await withHarness(
			"normal",
			async harness => {
				const catalog = await harness.worker.models();
				expect(catalog.current).toBe("deepminer/tencent/glm-5.3");
				expect(catalog.level).toBe("xhigh");
				// Malformed entries are skipped; an empty name is not reported as one.
				expect(catalog.available).toEqual([
					{ provider: "fixture", id: "default", name: "Fixture Default" },
					{ provider: "fixture", id: "fast" },
					{ provider: "fixture", id: "nested/model", name: "Nested" },
					{ provider: "other", id: "fast", name: "Other Fast" },
				]);
				const entries = await readLog(harness.logPath);
				expect(argumentValue(startupEntry(entries).argv, "--model")).toBe("deepminer/tencent/glm-5.3");
				expect(commandTypes(entries)).toContain("get_available_models");
			},
			{ model: "deepminer/tencent/glm-5.3" },
		);
	});

	test("keeps --model together with --resume so a resumed turn keeps the chat's model", async () => {
		await withHarness(
			"normal",
			async harness => {
				const catalog = await harness.worker.models();
				expect(catalog.current).toBe("fixture/fast");
				const startup = startupEntry(await readLog(harness.logPath));
				expect(argumentValue(startup.argv, "--resume")).toBe(join(harness.tempDir, "resumed.jsonl"));
				expect(argumentValue(startup.argv, "--model")).toBe("fixture/fast");
			},
			{ sessionFile: "resumed.jsonl", model: "fixture/fast" },
		);
	});

	test("reports an empty catalog when the deployment offers no models", async () => {
		await withHarness("no-models", async harness => {
			const catalog = await harness.worker.models();
			expect(catalog.current).toBe("fixture/default");
			expect(catalog.available).toEqual([]);
		});
	});

	test("switchModel hands the selector to OMP's own /switch and reports the resolved model", async () => {
		await withHarness("normal", async harness => {
			// A model selector keeps the session's level when it names none.
			await expect(harness.worker.switchModel("nested/model")).resolves.toEqual({
				ok: true,
				model: "fixture/nested/model",
				level: "xhigh",
			});
			// A role alias resolves through OMP's own role mapping, level included.
			await expect(harness.worker.switchModel("@fast")).resolves.toEqual({
				ok: true,
				model: "other/fast",
				level: "max",
			});
			const catalog = await harness.worker.models();
			expect(catalog.current).toBe("other/fast");
			expect(catalog.level).toBe("max");
			// A selector OMP cannot resolve leaves the session as it was.
			await expect(harness.worker.switchModel("nope")).resolves.toEqual({ ok: false });
			expect((await harness.worker.models()).current).toBe("other/fast");
			const prompts = (await readLog(harness.logPath)).filter(entry => entry.kind === "prompt");
			expect(prompts.map(entry => entry.message)).toEqual(["/switch nested/model", "/switch @fast", "/switch nope"]);
		});
	});

	test("switchModel works while a turn is streaming and leaves that turn running", async () => {
		await withHarness("steer", async harness => {
			const events: WorkerEvent[] = [];
			const prompt = harness.worker.prompt("start work", event => events.push(event));
			await waitUntil(() => events.some(event => event.type === "text" && event.text === "fixture first turn"));

			await expect(harness.worker.switchModel("@fast")).resolves.toEqual({
				ok: true,
				model: "other/fast",
				level: "max",
			});
			expect((await harness.worker.models()).current).toBe("other/fast");

			// The run itself is untouched: it still takes the steering frame and ends normally.
			await harness.worker.steer("also check the logs");
			await prompt;
			const finals = events
				.filter((event): event is Extract<WorkerEvent, { type: "text" }> => event.type === "text" && event.final)
				.map(event => event.text);
			expect(finals).toEqual(["fixture first turn", "fixture steered turn: also check the logs"]);
		});
	});

	test("steer injects a message into the streaming turn until that run ends", async () => {
		await withHarness("steer", async harness => {
			const events: WorkerEvent[] = [];
			const prompt = harness.worker.prompt("start work", event => events.push(event));
			await waitUntil(() => events.some(event => event.type === "text" && event.text === "fixture first turn"));

			await harness.worker.steer("also check the logs");

			await prompt;
			const finals = events
				.filter((event): event is Extract<WorkerEvent, { type: "text" }> => event.type === "text" && event.final)
				.map(event => event.text);
			expect(finals).toEqual(["fixture first turn", "fixture steered turn: also check the logs"]);
			const consumed = events.filter(
				(event): event is Extract<WorkerEvent, { type: "steer_consumed" }> => event.type === "steer_consumed",
			);
			expect(consumed).toHaveLength(1);
			expect(consumed[0]?.text).toBe("also check the logs");
			const steered = (await readLog(harness.logPath)).filter(entry => entry.kind === "steer");
			expect(steered).toHaveLength(1);
			expect(steered[0]?.message).toBe("also check the logs");
		});
	});

	test("steer fails without an accepted streaming turn instead of parking the message", async () => {
		await withHarness("normal", async harness => {
			await expect(harness.worker.steer("too early")).rejects.toThrow();

			await harness.worker.prompt("run", () => {});
			await expect(harness.worker.steer("too late")).rejects.toThrow();

			const entries = await readLog(harness.logPath);
			expect(entries.some(entry => entry.kind === "steer")).toBe(false);
		});
	});

	test("abortTurn aborts the streaming turn while the worker keeps running", async () => {
		await withHarness("interrupt", async harness => {
			const events: WorkerEvent[] = [];
			const prompt = harness.worker.prompt("long work", event => events.push(event));
			await waitUntil(() => events.some(event => event.type === "text"));

			await harness.worker.abortTurn();

			await prompt;
			expect(commandTypes(await readLog(harness.logPath))).toContain("abort");
			expect(harness.spawned).toBeDefined();
			expect(processIsAlive(harness.spawned!.pid)).toBe(true);
		});
	});


	test("does not treat the prompt acknowledgement as completion", async () => {
		await withHarness("normal", async harness => {
			const events: WorkerEvent[] = [];
			let settled = false;
			const prompt = harness.worker.prompt("run", event => events.push(event)).then(
				() => {
					settled = true;
				},
				(error: unknown) => {
					settled = true;
					throw error;
				},
			);
			await waitUntil(async () =>
				(await readLog(harness.logPath)).some(entry => {
					if (entry.kind !== "out" || !entry.frame || typeof entry.frame !== "object") return false;
					const frame = entry.frame as Record<string, unknown>;
					return frame.type === "response" && frame.command === "prompt" && frame.success === true;
				}),
			);
			// The fixture records the immediate acknowledgement before scheduling
			// its terminal events; no wall-clock sleep is needed to expose the
			// distinction between acceptance and completion.
			expect(settled).toBe(false);
			await prompt;
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents).toHaveLength(2);
			expect(textEvents[0]).toMatchObject({ type: "text", text: "fixture completed", final: false });
			expect(textEvents[1]).toMatchObject({ type: "text", text: "fixture completed", final: true });
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
			expect(textEvents[0]?.messageId).toBe(textEvents[1]?.messageId);
		});
	});

	test("completes a local-only prompt from the ack's agentInvoked false with one local command event", async () => {
		await withHarness("local", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("/help", event => events.push(event));
			// The ack settles the run; the fixture's same-id prompt_result replay 1ms later is a
			// completion replay on a settled run and must not open a second local command.
			expect(events.filter(event => event.type === "local_command")).toEqual([{ type: "local_command" }]);
			expect(events).toHaveLength(1);
		});
	});

	test("surfaces /autoresearch notify frames as levelled worker notice events with preserved ids", async () => {
		await withHarness("autoresearch", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("/autoresearch", event => events.push(event));
			const enabledText = "Autoresearch enabled—describe what to optimize in your next message.";
			expect(events.filter(event => event.type === "notice")).toEqual([
				{ type: "notice", messageId: "fixture-autoresearch-notify-enabled", text: enabledText, level: "info" },
				{
					type: "notice",
					messageId: "fixture-autoresearch-notify-warn-1",
					text: "Autoresearch worktree is dirty — commit or stash these changes first.",
					level: "warning",
				},
				{
					type: "notice",
					messageId: "fixture-autoresearch-notify-error-1",
					text: "Autoresearch run failed — offline score regressed, baseline kept.",
					level: "error",
				},
				{ type: "notice", messageId: "fixture-autoresearch-notify-plain", text: "Autoresearch heartbeat is still running.", level: "info" },
				{
					type: "notice",
					messageId: "fixture-autoresearch-notify-odd-level",
					text: "Autoresearch level tokens are advisory only.",
					level: "info",
				},
				{
					type: "notice",
					messageId: "fixture-autoresearch-notify-padded",
					text: "Autoresearch pads this notice with whitespace.",
					level: "info",
				},
				{
					type: "notice",
					messageId: "fixture-autoresearch-notify-multiline",
					text: "Autoresearch step one done.\nStep two pending.\tQueued at 09:00.",
					level: "info",
				},
				{
					type: "notice",
					messageId: "n".repeat(200),
					text: "the 200-unit id boundary stays usable",
					level: "info",
				},
				// The HUD re-broadcast surfaces again with the same id; the service stream
				// dedup, not the worker, collapses the duplicate downstream.
				{ type: "notice", messageId: "fixture-autoresearch-notify-enabled", text: enabledText, level: "info" },
			]);
			// A local-only command produces notices only: no assistant text joins the stream.
			expect(events.filter(event => event.type === "text")).toEqual([]);
			// The id-carrying prompt_result is the second real local completion path; it counts
			// the run once exactly.
			expect(events.filter(event => event.type === "local_command")).toEqual([{ type: "local_command" }]);
			expect(events).toHaveLength(10);
		});
	});

	test("ignores startup and post-settlement notifications and keeps them out of the next prompt", async () => {
		await withHarness("autoresearch", async harness => {
			const firstEvents: WorkerEvent[] = [];
			await harness.worker.prompt("/autoresearch", event => firstEvents.push(event));
			// Wait until the fixture's post-settlement re-broadcast has been offered to the host.
			await waitUntil(async () =>
				(await readLog(harness.logPath)).some(entry => entry.kind === "turn_complete" && entry.notifyTurn === true),
			);
			const firstNotices = firstEvents.filter(event => event.type === "notice");
			expect(firstNotices.length).toBeGreaterThan(0);
			expect(firstNotices.some(notice => notice.messageId === "fixture-autoresearch-notify-startup-1")).toBe(false);
			expect(firstNotices.some(notice => notice.messageId === "fixture-autoresearch-notify-late-1")).toBe(false);
			expect(firstEvents.filter(event => event.type === "local_command")).toHaveLength(1);

			const secondEvents: WorkerEvent[] = [];
			await harness.worker.prompt("run normally", event => secondEvents.push(event));
			expect(secondEvents.filter(event => event.type === "notice")).toEqual([]);
			// A real generation turn is not a local command and must not inherit one.
			expect(secondEvents.filter(event => event.type === "local_command")).toEqual([]);
			expect(secondEvents.filter(event => event.type === "text").map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "fixture completed", final: false },
				{ text: "fixture completed", final: true },
			]);
			// Re-broadcasts after settlement never reopen the settled run either.
			expect(firstEvents.filter(event => event.type === "notice")).toEqual(firstNotices);
		});
	});

	test("bounds an oversized unicode notification without split surrogates or control characters", async () => {
		await withHarness("autoresearch", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("/autoresearch huge", event => events.push(event));
			const notices = events.filter(event => event.type === "notice");
			expect(notices).toHaveLength(1);
			const notice = notices[0]!;
			expect(notice.messageId).toBe("fixture-autoresearch-notify-huge-1");
			expect(notice.level).toBe("warning");
			// The bounded contract: at most 4,000 UTF-16 code units of sanitized text.
			expect(notice.text.length).toBeLessThanOrEqual(4_000);
			expect(hasLoneSurrogate(notice.text)).toBe(false);
			// C0 controls other than \n \r \t, and DEL, are stripped before delivery.
			expect(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(notice.text)).toBe(false);
			expect(notice.text.startsWith("alphabravocharliedelta")).toBe(true);
			expect(notice.text.includes("字")).toBe(true);
			// The only other event on this local-only run is its local completion.
			expect(events.filter(event => event.type === "local_command")).toEqual([{ type: "local_command" }]);
			expect(events).toHaveLength(2);
		});
	});

	test("a stale prompt_result replay cannot terminate the next goal prompt", async () => {
		await withHarness("autoresearch", async harness => {
			const quietEvents: WorkerEvent[] = [];
			await harness.worker.prompt("/autoresearch quiet", event => quietEvents.push(event));

			const goalEvents: WorkerEvent[] = [];
			await harness.worker.prompt("run goal", event => goalEvents.push(event));
			// The finished local run's prompt_result replayed mid-goal is stale: it must not
			// settle the new prompt, whose own agent turn still completes it.
			expect(goalEvents.filter(event => event.type === "text").map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "fixture completed", final: false },
				{ text: "fixture completed", final: true },
			]);
			// A generation turn carries no notification and no local completion of its own.
			expect(goalEvents.filter(event => event.type === "notice")).toEqual([]);
			expect(goalEvents.filter(event => event.type === "local_command")).toEqual([]);
		});
	});

	test("accepts a legacy prompt_result without a request id as the local completion", async () => {
		await withHarness("autoresearch", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("/autoresearch legacy", event => events.push(event));
			expect(events.filter(event => event.type === "local_command")).toEqual([{ type: "local_command" }]);
			expect(events).toHaveLength(1);
		});
	});

	test("derives the authoritative research snapshot from native init/run/log details across continued turns", async () => {
		await withHarness("research", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run research", event => events.push(event));
			const research = researchUpdates(events);

			// Effective phase transitions only: how often a phase is re-notified is an
			// implementation detail; the order of the real transitions is the contract.
			expect(researchPhases(research)).toEqual([
				"ready",
				"running",
				"awaiting_log",
				"ready",
				"running",
				"awaiting_log",
				"ready",
				"running",
				"awaiting_log",
				"ready",
			]);

			// Init boundary: another segment's keep 5 must not leak into baseline/best/recent,
			// and the legacy unnumbered discard stays a valid (null run number) row.
			expect(research[0]?.research).toMatchObject({
				sessionId: 42,
				name: "offline-score",
				goal: "tune the offline score until it stops improving",
				segment: 1,
				metricName: "score",
				metricUnit: "ms",
				direction: "lower",
				maxExperiments: 8,
				baseline: null,
				best: null,
				completedRuns: 1,
				totalRuns: 2,
				phase: "ready",
				activeRun: null,
				latestMetric: null,
				recent: [legacyProbeView],
			});

			// Pending runs never append history: awaiting run 9 the counts are still 2 of 3.
			const pendingRun9 = research.find(event => event.research.phase === "awaiting_log" && event.research.activeRun === 9);
			expect(pendingRun9?.research).toMatchObject({
				latestMetric: 8,
				baseline: 10,
				best: 10,
				completedRuns: 2,
				totalRuns: 3,
				recent: [legacyProbeView, run8View],
			});

			// Final authoritative state: baseline stays the first unflagged keep (10) even
			// though every log served state.bestMetric 10 — the upstream baseline — while the
			// real best moved to 8; the flagged keep 0 counts but changes neither; the
			// unnumbered discard keeps its null id.
			expect(research.at(-1)?.research).toEqual({
				sessionId: 42,
				name: "offline-score",
				goal: "tune the offline score until it stops improving",
				segment: 1,
				metricName: "score",
				metricUnit: "ms",
				direction: "lower",
				baseline: 10,
				best: 8,
				completedRuns: 4,
				maxExperiments: 8,
				phase: "ready",
				activeRun: null,
				latestMetric: 0,
				recent: [legacyProbeView, run8View, run9View, run10View],
				totalRuns: 5,
			});

			// Duplicate and ghost replays after the last log must never roll the state back.
			const finalIndex = research.findIndex(event => event.research.completedRuns === 4);
			expect(finalIndex).toBeGreaterThan(0);
			for (const event of research.slice(finalIndex)) {
				expect(event.research.phase).toBe("ready");
				expect(event.research).toMatchObject({ baseline: 10, best: 8, completedRuns: 4, totalRuns: 5, activeRun: null });
			}

			// Only init/log updates carry the sanitized full-history report; pending never does.
			for (const event of research) {
				if (event.research.phase === "running" || event.research.phase === "awaiting_log") {
					expect(event.report).toBeUndefined();
				} else {
					expect(typeof event.report).toBe("string");
				}
			}
			expect(Number.isFinite(research.at(-1)?.atMs)).toBe(true);

			// No leak vector in the native payloads (ASI blobs, raw harness output, the
			// harness command line, notes, host paths) may reach an event or the report.
			const serialized = JSON.stringify(research);
			for (const marker of [
				"SECRET_ASI",
				"SECRET_RAW_OUTPUT",
				"SECRET_HARNESS_COMMAND",
				"SECRET_NOTES",
				"SECRET_HOST_PATH",
				"SECRET_MODIFIED_PATH",
			]) {
				expect(serialized).not.toContain(marker);
			}

			// The report is the sanitized full record: every recorded run across all
			// segments (the /autoresearch-history payload), their safe fields, nothing else.
			const finalReport = research.at(-1)?.report ?? "";
			for (const description of [
				"fresh baseline",
				"cache-friendly layout",
				"suspicious perfect score",
				"migrated regression probe",
				"pre-migration segment champion",
			]) {
				expect(finalReport).toContain(description);
			}
			expect(finalReport).not.toContain("SECRET");
			expect(Buffer.byteLength(finalReport, "utf8")).toBeLessThanOrEqual(1024 * 1024);

			// The research cache is per-prompt: a follow-up task never inherits this stream.
			const followUps: WorkerEvent[] = [];
			await harness.worker.prompt("run normally", event => followUps.push(event));
			expect(researchUpdates(followUps)).toEqual([]);
		});
	});

	test("never fabricates research events from missing, malformed, errored or stale tool results", async () => {
		await withHarness("research-malformed", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run research", event => events.push(event));
			const research = researchUpdates(events);

			// The errored run end reverts the phase instead of leaving the loop running
			// forever; the run started before the log completes reports running with the
			// pre-log history; every other junk shape is invisible to the stream.
			expect(researchPhases(research)).toEqual(["ready", "running", "ready", "running", "awaiting_log", "running", "ready"]);

			// The matched isError end reverts to ready with the numbers it had before the
			// crash: no history, no pending run, and the exploded run's details are not
			// fabricated into a metric, a report, or a run id.
			const reverted = research.find((event, index) => index > 0 && event.research.phase === "ready");
			expect(reverted?.research).toMatchObject({
				sessionId: 7,
				segment: 0,
				phase: "ready",
				activeRun: null,
				latestMetric: null,
				baseline: null,
				best: null,
				completedRuns: 0,
				totalRuns: 0,
				recent: [],
			});
			expect(reverted?.report).toBeUndefined();

			// The honest run is the only pending run the stream ever sees: the args-only
			// keep (metric 88), the shapeless state, the isError'd log payload, the poisoned
			// enum/Infinity rows (one bad row rejects the whole state), the bash state
			// dump, the string/null/Infinity metrics — none opened an awaiting_log.
			const pending = research.find(event => event.research.phase === "awaiting_log");
			expect(pending?.research).toMatchObject({
				activeRun: 2,
				latestMetric: 6,
				baseline: null,
				best: null,
				completedRuns: 0,
				totalRuns: 0,
				recent: [],
			});

			// The run started before the log completes and reports running against the
			// pre-log history; its late completion must never reopen awaiting_log on top
			// of the landed log nor lend the stream its run number or metric.
			const lateRunning = research.filter(event => event.research.phase === "running").at(-1);
			expect(lateRunning?.research).toMatchObject({ activeRun: null, latestMetric: 6, completedRuns: 0, totalRuns: 0, recent: [] });

			expect(research.at(-1)?.research).toEqual({
				sessionId: 7,
				name: "shiny-latency",
				goal: "drive the checkout latency down",
				segment: 0,
				metricName: "latency",
				metricUnit: "ms",
				direction: "lower",
				baseline: 6,
				best: 6,
				completedRuns: 1,
				maxExperiments: null,
				phase: "ready",
				activeRun: null,
				latestMetric: 6,
				recent: [run2BrokenView],
				totalRuns: 1,
			});

			// Reports ride only on authoritative updates: the init and the final log.
			expect(typeof research[0]?.report).toBe("string");
			expect(typeof research.at(-1)?.report).toBe("string");
			for (const event of research) {
				if (event === reverted || event.research.phase !== "ready") expect(event.report).toBeUndefined();
			}

			// Nothing in the gauntlet may leak: not the forged description, not the notes,
			// not the raw output or harness command smuggled inside the errored payloads.
			const serialized = JSON.stringify(research);
			for (const marker of [
				"SECRET_FORGED_DESCRIPTION",
				"SECRET_TOOL_NAME",
				"SECRET_NOTES",
				"SECRET_ASI",
				"SECRET_RAW_OUTPUT",
				"SECRET_HOST_PATH",
				"SECRET_MODIFIED_PATH",
				"SECRET_HARNESS_COMMAND",
			]) {
				expect(serialized).not.toContain(marker);
			}
		});
	});

	test("keeps a zero baseline, tracks best upward for higher-is-better, and reports limit_reached at the cap", async () => {
		await withHarness("research-higher", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run research", event => events.push(event));
			const research = researchUpdates(events);

			expect(researchPhases(research)).toEqual([
				"ready",
				"running",
				"awaiting_log",
				"ready",
				"running",
				"awaiting_log",
				"ready",
				"running",
				"awaiting_log",
				"limit_reached",
			]);

			// The model never stated a goal: upstream serves goal:null, which normalizes to an
			// empty goal instead of rejecting the whole state.
			expect(research[0]?.research).toMatchObject({
				sessionId: 9,
				name: "throughput-drive",
				goal: "",
				segment: 0,
				metricName: "throughput",
				metricUnit: "rps",
				direction: "higher",
				maxExperiments: 3,
				baseline: null,
				best: null,
				completedRuns: 0,
				totalRuns: 0,
				phase: "ready",
				activeRun: null,
				latestMetric: null,
				recent: [],
			});

			// A zero pending metric is a real measurement, never "missing".
			const firstPending = research.find(event => event.research.phase === "awaiting_log");
			expect(firstPending?.research).toMatchObject({ activeRun: 1, completedRuns: 0, totalRuns: 0, recent: [] });
			expect(firstPending?.research.latestMetric).toBe(0);

			// The zero-metric first keep is both baseline and best — zero, not null.
			const firstLog = research.find(event => event.research.completedRuns === 1);
			expect(firstLog?.research.baseline).toBe(0);
			expect(firstLog?.research.best).toBe(0);
			expect(firstLog?.research).toMatchObject({ completedRuns: 1, totalRuns: 1, latestMetric: 0, phase: "ready", recent: [run1HigherView] });

			// The larger discard counts toward progress, never toward the best.
			const discardLog = research.find(event => event.research.completedRuns === 2);
			expect(discardLog?.research).toMatchObject({
				baseline: 0,
				best: 0,
				totalRuns: 2,
				latestMetric: 15,
				recent: [run1HigherView, run2HigherView],
			});

			// The third completed run hits max_iterations: limit_reached, zero baseline kept,
			// the keep 7 (not the discard 15) is the best.
			expect(research.at(-1)?.research).toEqual({
				sessionId: 9,
				name: "throughput-drive",
				goal: "",
				segment: 0,
				metricName: "throughput",
				metricUnit: "rps",
				direction: "higher",
				baseline: 0,
				best: 7,
				completedRuns: 3,
				maxExperiments: 3,
				phase: "limit_reached",
				activeRun: null,
				latestMetric: 7,
				recent: [run1HigherView, run2HigherView, run3HigherView],
				totalRuns: 3,
			});

			for (const event of research) {
				if (event.research.phase === "running" || event.research.phase === "awaiting_log") {
					expect(event.report).toBeUndefined();
				} else {
					expect(typeof event.report).toBe("string");
				}
			}
		});
	});

	test("rejects a delayed asynchronous prompt error after the initial ack", async () => {
		await withHarness("error", async harness => {
			await expect(harness.worker.prompt("run", () => {})).rejects.toThrow("fixture delayed prompt failure");
		});
	});
	test("rejects a prompt failure sharing the acknowledgement stdout chunk", async () => {
		await withHarness("same-chunk-error", async harness => {
			await expect(harness.worker.prompt("same chunk", () => {})).rejects.toThrow("fixture same-chunk prompt failure");
		});
	});

	test("emits cumulative text snapshots and one authoritative final while filtering non-assistant events", async () => {
		await withHarness("message-end", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("message boundaries", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.map(event => ({ type: event.type, text: event.text, final: event.final }))).toEqual([
				{ type: "text", text: "prefix `in", final: false },
				{ type: "text", text: "prefix `inline\nco", final: false },
				{ type: "text", text: "prefix `inline\ncode` suffix\n\n", final: false },
				{ type: "text", text: "prefix `inline\ncode` suffix\n\nlast", final: false },
				{ type: "text", text: "prefix `inline\ncode` suffix\n\nlast (authoritative)", final: true },
			]);
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
			expect(textEvents.every(event => event.messageId === textEvents[0]?.messageId)).toBe(true);
			expect(textEvents.some(event => event.text.includes("secret user message"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("SECRET_THINKING"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("SECRET_TOOL_RESULT"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("turn_start"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("turn_end"))).toBe(false);
			// A user message the host did not steer into the queue is not a steering receipt.
			expect(events.some(event => event.type === "steer_consumed")).toBe(false);
		});
	});
	test("emits an assistant message_end body even when no text delta arrived", async () => {
		await withHarness("message-end-no-delta", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("message without deltas", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents).toHaveLength(1);
			expect(textEvents[0]).toMatchObject({ type: "text", text: "message without deltas", final: true });
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
		});
	});
	test("emits an empty final text event so an assistant message can close", async () => {
		await withHarness("message-end-empty", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("empty assistant message", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents).toHaveLength(1);
			expect(textEvents[0]).toMatchObject({ type: "text", text: "", final: true });
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
		});
	});

	test("waits for a terminal agent end across a continuation round", async () => {
		await withHarness("continuation", async harness => {
			const events: WorkerEvent[] = [];
			let settled = false;
			const prompt = harness.worker.prompt("continue this turn", event => events.push(event)).then(() => {
				settled = true;
			});
			await waitUntil(async () => (await readLog(harness.logPath)).some(entry => entry.kind === "continue_pending"));
			expect(settled).toBe(false);
			expect(events).toEqual([]);
			await prompt;
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "continued body", final: false },
				{ text: "continued body", final: true },
			]);
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
			expect(textEvents[0]?.messageId).toBe(textEvents[1]?.messageId);
		});
	});
	test("emits each assistant message body without artificial separators", async () => {
		await withHarness("multi-message", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("multiple assistant messages", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "first body", final: false },
				{ text: "first body", final: true },
				{ text: "second body", final: false },
				{ text: "second body", final: true },
			]);
			expect(textEvents[0]?.messageId).toBe(textEvents[1]?.messageId);
			expect(textEvents[2]?.messageId).toBe(textEvents[3]?.messageId);
			expect(textEvents[0]?.messageId).not.toBe(textEvents[2]?.messageId);
			expect(events.every(event => event.type === "text")).toBe(true);
		});
	});

	test("does not suppress identical text from distinct assistant messages", async () => {
		await withHarness("same-message-text", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("same assistant messages", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "same body", final: false },
				{ text: "same body", final: true },
				{ text: "same body", final: false },
				{ text: "same body", final: true },
			]);
			expect(textEvents[0]?.messageId).toBe(textEvents[1]?.messageId);
			expect(textEvents[2]?.messageId).toBe(textEvents[3]?.messageId);
			expect(textEvents[0]?.messageId).not.toBe(textEvents[2]?.messageId);
			expect(events.every(event => event.type === "text")).toBe(true);
		});
	});

	test("allocates a stable unique message ID when assistant messages have no upstream ID", async () => {
		await withHarness("generated-message-id", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("generate message IDs", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.map(event => ({ text: event.text, final: event.final }))).toEqual([
				{ text: "generated first", final: false },
				{ text: "generated first", final: true },
				{ text: "generated second", final: false },
				{ text: "generated second", final: true },
			]);
			const firstMessageId = textEvents[0]?.messageId;
			const secondMessageId = textEvents[2]?.messageId;
			expect(firstMessageId).toEqual(expect.any(String));
			expect(secondMessageId).toEqual(expect.any(String));
			expect(firstMessageId).toBe(textEvents[1]?.messageId);
			expect(secondMessageId).toBe(textEvents[3]?.messageId);
			expect(firstMessageId).not.toBe(secondMessageId);
		});
	});

	test("keeps a streamed prefix on the same message across late and repeated starts", async () => {
		await withHarness("late-message-start", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("message identity", event => events.push(event));
			const text = events.filter(event => event.type === "text");
			expect(text.map(event => event.text)).toEqual(["已", "已发送", "已发送", "下一条", "下一条"]);
			expect(new Set(text.slice(0, 3).map(event => event.messageId)).size).toBe(1);
			expect(text[3]?.messageId).not.toBe(text[0]?.messageId);
		});
	});

	test("maps real turn and tool lifecycle events without leaking tool payloads", async () => {
		const startedAt = Date.now();
		await withHarness("tool-progress", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("show tool progress", event => events.push(event));
			const executionEvents = events.filter(event => event.type !== "text");
			expect(executionEvents.map(event => event.type)).toEqual(["turn_start", "tool_start", "tool_end", "turn_end"]);
			expect(executionEvents[0]).toMatchObject({ type: "turn_start", turnId: 0, atMs: 2_000 });
			expect(executionEvents[1]).toMatchObject({
				type: "tool_start",
				toolCallId: "fixture-progress-call-1",
				toolName: "fixture_echo",
			});
			expect(executionEvents[2]).toMatchObject({
				type: "tool_end",
				toolCallId: "fixture-progress-call-1",
				status: "succeeded",
			});
			expect(executionEvents[3]).toMatchObject({ type: "turn_end", turnId: 0 });
			for (const event of executionEvents.slice(1)) {
				if ("atMs" in event) {
					expect(event.atMs).toBeGreaterThanOrEqual(startedAt - 1_000);
					expect(event.atMs).toBeLessThanOrEqual(Date.now() + 1_000);
				}
			}
			const serializedExecution = JSON.stringify(executionEvents);
			expect(serializedExecution).not.toContain("SECRET_TOOL_ARGUMENTS");
			expect(serializedExecution).not.toContain("SECRET_TOOL_RESULT");
			expect(serializedExecution).not.toContain("SECRET_THINKING");
			expect(executionEvents[1]).not.toHaveProperty("args");
			expect(executionEvents[2]).not.toHaveProperty("result");

			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents.some(event => event.text.includes("SECRET_TOOL_ARGUMENTS"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("SECRET_TOOL_RESULT"))).toBe(false);
			expect(textEvents.some(event => event.text.includes("SECRET_THINKING"))).toBe(false);
		});
	});

	test("subscribes to subagent frames and maps their lifecycle without leaking child payloads", async () => {
		await withHarness("subagent-events", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run a subagent", event => events.push(event));
			const log = await readLog(harness.logPath);
			const commands = log
				.filter(entry => entry.kind === "command")
				.map(entry => entry.command as Record<string, unknown> | undefined)
				.filter((command): command is Record<string, unknown> => command !== undefined);
			// The registry defaults to `off`, so the worker has to ask for the frames it renders.
			const subscription = commands.filter(command => command.type === "set_subagent_subscription");
			expect(subscription).toHaveLength(1);
			expect(subscription[0]?.level).toBe("progress");

			type SubagentReport = Record<string, unknown> & { readonly subagentId: string };
			const subagentEvents: SubagentReport[] = [];
			for (const event of events) {
				if (event.type === "subagent_start") {
					subagentEvents.push({
						type: event.type,
						subagentId: event.subagentId,
						agent: event.agent,
						state: event.state,
						summary: event.summary,
					});
					continue;
				}
				if (event.type === "subagent_update") {
					subagentEvents.push({
						type: event.type,
						subagentId: event.subagentId,
						state: event.state,
						summary: event.summary,
						currentTool: event.currentTool,
						toolCount: event.toolCount,
					});
					continue;
				}
				if (event.type === "subagent_end") {
					subagentEvents.push({
						type: event.type,
						subagentId: event.subagentId,
						state: event.state,
						summary: event.summary,
					});
				}
			}
			expect(subagentEvents).toEqual([
				{ type: "subagent_start", subagentId: "CountSrc", agent: "scout", state: "running", summary: "统计源码文件" },
				{
					type: "subagent_update",
					subagentId: "CountSrc",
					state: "running",
					summary: "扫描 src",
					currentTool: "glob",
					toolCount: 2,
				},
				{ type: "subagent_end", subagentId: "CountSrc", state: "completed", summary: "统计源码文件" },
			]);
			for (const event of subagentEvents) {
				expect(typeof event.subagentId).toBe("string");
			}
			// The child's own prompt and tool arguments stay in its transcript.
			const serialized = JSON.stringify(subagentEvents);
			expect(serialized).not.toContain("SECRET_CHILD_ASSIGNMENT");
			expect(serialized).not.toContain("SECRET_CHILD_ARGS");
		});
	});

	test("keeps the worker usable when the deployment offers no subagent subscription", async () => {
		await withHarness("no-subagent-subscription", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run a subagent", event => events.push(event));
			expect(events.some(event => event.type === "subagent_start" || event.type === "subagent_end")).toBe(false);
			// The parent's own tool lifecycle still arrives, so only the subagent band is missing.
			expect(events.some(event => event.type === "tool_start" && event.toolName === "task")).toBe(true);
			expect(events.some(event => event.type === "text" && event.text.includes("fixture subagent turn"))).toBe(true);
		});
	});

	test("publishes only a safe skill name as a read tool summary", async () => {
		await withHarness("tool-summary", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("show a skill file", event => events.push(event));
			const toolStart = events.find(event => event.type === "tool_start");
			expect(toolStart).toMatchObject({
				type: "tool_start",
				toolCallId: "fixture-summary-read-1",
				toolName: "read",
				summary: "spec-design",
			});
			expect(toolStart).not.toHaveProperty("args");
			const toolEnd = events.find(event => event.type === "tool_end");
			expect(toolEnd).toMatchObject({
				type: "tool_end",
				toolCallId: "fixture-summary-read-1",
				status: "succeeded",
				summary: "spec-design",
			});
			expect(toolEnd).not.toHaveProperty("result");
			const serialized = JSON.stringify(events);
			expect(serialized).not.toContain("Authorization");
			expect(serialized).not.toContain("SECRET_AUTHORIZATION");
			expect(serialized).not.toContain("SECRET_SUMMARY_SECRET");
			expect(serialized).not.toContain("SECRET_SUMMARY_RESULT");
		});
	});
	test("publishes semantic summaries for special tools without leaking payloads", async () => {
		await withHarness("hub-summary", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("show special tool summaries", event => events.push(event));
			const toolEvents = events.filter(event => event.type === "tool_start" || event.type === "tool_end");
			const summaries = toolEvents.map(event => {
				if (event.type === "tool_start") {
					return { type: event.type, toolName: event.toolName, summary: event.summary };
				}
				return { type: event.type, summary: event.summary };
			});
			expect(summaries).toEqual([
				{ type: "tool_start", toolName: "hub", summary: "IRC → agent-a · 等待回复" },
				{ type: "tool_end", summary: "IRC → agent-a · 等待回复" },
				{ type: "tool_start", toolName: "hub", summary: "IRC ← agent-b" },
				{ type: "tool_end", summary: "IRC ← agent-b" },
				{ type: "tool_start", toolName: "hub", summary: "IRC 收件箱 · 预览" },
				{ type: "tool_end", summary: "IRC 收件箱 · 预览" },
				{ type: "tool_start", toolName: "hub", summary: "IRC 协作代理" },
				{ type: "tool_end", summary: "IRC 协作代理" },
				{ type: "tool_start", toolName: "task", summary: "子代理 · scout" },
				{ type: "tool_end", summary: "子代理 · scout" },
				{ type: "tool_start", toolName: "todo", summary: "任务清单 · 完成" },
				{ type: "tool_end", summary: "任务清单 · 完成" },
				{ type: "tool_start", toolName: "ask", summary: "等待用户回答 · 1 个问题" },
				{ type: "tool_end", summary: "等待用户回答 · 1 个问题" },
			]);
			for (const event of toolEvents) {
				expect(event).not.toHaveProperty("args");
				expect(event).not.toHaveProperty("result");
			}
			const serialized = JSON.stringify(events);
			expect(serialized).not.toContain("SECRET_IRC_MESSAGE");
			expect(serialized).not.toContain("SECRET_IRC_RESULT");
			expect(serialized).not.toContain("SECRET_TASK_PROMPT");
			expect(serialized).not.toContain("SECRET_TASK_RESULT");
			expect(serialized).not.toContain("SECRET_TODO_TASK");
			expect(serialized).not.toContain("SECRET_TODO_RESULT");
			expect(serialized).not.toContain("SECRET_QUESTION");
			expect(serialized).not.toContain("SECRET_ASK_RESULT");
		});
	});

	test("publishes a bounded command summary for command tools without leaking payloads", async () => {
		await withHarness("command-summary", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("run two commands", event => events.push(event));
			const toolEvents = events.filter(event => event.type === "tool_start" || event.type === "tool_end");
			expect(toolEvents.map(event => {
				if (event.type === "tool_start") {
					return { type: event.type, toolName: event.toolName, summary: event.summary };
				}
				return { type: event.type, summary: event.summary };
			})).toEqual([
				{
					type: "tool_start",
					toolName: "bash",
					summary: "set -a && . ./.envrc && set +a && bun run src/cli.ts serve --config…",
				},
				{ type: "tool_end", summary: "set -a && . ./.envrc && set +a && bun run src/cli.ts serve --config…" },
				{
					type: "tool_start",
					toolName: "bash",
					summary: 'grep -rn "tool_execution_start" tests/ | head -20',
				},
				{ type: "tool_end", summary: 'grep -rn "tool_execution_start" tests/ | head -20' },
			]);
			for (const event of toolEvents) {
				expect(event).not.toHaveProperty("args");
				expect(event).not.toHaveProperty("result");
				if (event.summary === undefined) throw new Error("command tool summary was not published");
				expect(event.summary).not.toContain("\n");
			}
			const serialized = JSON.stringify(events);
			expect(serialized).not.toContain("SECRET_COMMAND_CWD");
			expect(serialized).not.toContain("SECRET_COMMAND_ENV");
			expect(serialized).not.toContain("SECRET_COMMAND_RESULT");
		});
	});

	test("maps only successful todo tool details to a typed update", async () => {
		await withHarness("todo-progress", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("show todo progress", event => events.push(event));
			const todoEvents = events.filter(event => event.type === "todo_update");
			const turnStart = events.find(event => event.type === "turn_start");
			const turnEnd = events.find(event => event.type === "turn_end");
			expect(turnStart).toMatchObject({ type: "turn_start", atMs: 3_000 });
			expect(turnEnd).toMatchObject({ type: "turn_end", atMs: expect.any(Number) });
			if (turnStart?.type === "turn_start" && turnEnd?.type === "turn_end") {
				expect(turnStart.turnId).toEqual(expect.anything());
				expect(turnEnd.turnId).toBe(turnStart.turnId);
			}
			expect(todoEvents).toHaveLength(1);
			expect(todoEvents[0]).toEqual({
				type: "todo_update",
				atMs: expect.any(Number),
				op: "init",
				phases: [
					{
						name: "Planning",
						tasks: [
							{ content: "draft plan", status: "completed" },
							{ content: "ship plan", status: "cancelled" },
							{ content: "wait for review", status: "blocked", blocker: "waiting on review" },
						],
					},
				],
			});
			const toolEnd = events.find(event => event.type === "tool_end");
			expect(toolEnd).toMatchObject({
				type: "tool_end",
				toolCallId: "fixture-todo-call-1",
				status: "succeeded",
			});
			const serialized = JSON.stringify(events);
			expect(serialized).not.toContain("SECRET_TODO_ARGUMENTS");
			expect(serialized).not.toContain("SECRET_TODO_TEXT");
			expect(serialized).not.toContain("result");
			expect(serialized).not.toContain("thinking");
			expect(events.some(event => event.type === "run_end")).toBe(false);
		});
	});

	test("does not publish todo phases from a failed todo tool result", async () => {
		await withHarness("todo-failed", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("failed todo progress", event => events.push(event));
			expect(events.filter(event => event.type === "todo_update")).toEqual([]);
			const toolEnd = events.find(event => event.type === "tool_end");
			expect(toolEnd).toMatchObject({
				type: "tool_end",
				toolCallId: "fixture-todo-call-1",
				status: "failed",
				errorCategory: "unknown",
			});
			const serialized = JSON.stringify(events);
			expect(serialized).not.toContain("SECRET_TODO_ARGUMENTS");
			expect(serialized).not.toContain("SECRET_TODO_TEXT");
		});
	});

	test("rejects a terminal agent turn with model stopReason error", async () => {
		await withHarness("model-error", async harness => {
			await expect(harness.worker.prompt("model failure", () => {})).rejects.toThrow(/model.*error|model failure/i);
		});
	});

	test("reassembles a lossless v2 frame larger than the v1 physical limit", async () => {
		await withHarness("v2", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("large output", event => events.push(event));
			const textEvents = events.filter(event => event.type === "text");
			expect(textEvents).toHaveLength(2);
			expect(textEvents[0]?.text.startsWith("fixture-v2-")).toBe(true);
			expect(textEvents[0]?.text.length).toBeGreaterThan(1_000_000);
			expect(textEvents[0]?.final).toBe(false);
			expect(textEvents[1]?.final).toBe(true);
			expect(textEvents[1]?.text).toBe(textEvents[0]?.text);
			expect(textEvents[0]?.messageId).toEqual(expect.any(String));
			expect(textEvents[0]?.messageId).toBe(textEvents[1]?.messageId);
		});
	});

	test("registers host tools as essential before prompting and returns textual results", async () => {
		await withHarness("host-tool", async harness => {
			let receivedArgs: Record<string, unknown> | undefined;
			const tool: HostTool = {
				name: "fixture_echo",
				description: "Echo a fixture value",
				parameters: { type: "object", properties: { message: { type: "string" } } },
				execute: async args => {
					receivedArgs = args;
					return `host result: ${String(args.message)}`;
				},
			};
			const context: PromptContext = { tools: [tool] };
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("use host tool", event => events.push(event), context);
			expect(events.some(event => event.type === "text" && event.text.includes("host result:"))).toBe(true);
			const entries = await readLog(harness.logPath);
			const setToolsEntry = entries.find(entry => entry.kind === "command" && (entry.command as LogEntry)?.type === "set_host_tools");
			expect(setToolsEntry).toBeDefined();
			const setTools = setToolsEntry!.command as LogEntry;
			expect(Array.isArray(setTools.tools)).toBe(true);
			expect((setTools.tools as LogEntry[])[0]?.loadMode).toBe("essential");
			expect(commandTypes(entries).indexOf("set_host_tools")).toBeLessThan(commandTypes(entries).indexOf("prompt"));
			const hostResult = entries.find(
				entry => entry.kind === "command" && (entry.command as LogEntry)?.type === "host_tool_result",
			);
			expect(hostResult).toBeDefined();
			expect((hostResult!.command as LogEntry).isError).toBe(false);
			expect(resultTextFromCommand(hostResult!.command)).toContain("host result: hello from fixture");
		});
	});

	test("fails an unknown host tool through an error result instead of hanging", async () => {
		await withHarness("host-unknown", async harness => {
			const prompt = harness.worker.prompt(
				"use missing host tool",
				() => {},
				{ tools: [] },
			);
			await prompt;
			const entries = await readLog(harness.logPath);
			const hostResult = entries.find(
				entry => entry.kind === "command" && (entry.command as LogEntry)?.type === "host_tool_result",
			);
			expect(hostResult).toBeDefined();
			expect((hostResult!.command as LogEntry).isError).toBe(true);
			expect(resultTextFromCommand(hostResult!.command)).toMatch(/unknown|missing|not found/i);
		});
	});

	test("aborts an in-flight host tool when the RPC side cancels it", async () => {
		await withHarness("host-cancel", async harness => {
			let aborted = false;
			const tool: HostTool = {
				name: "fixture_echo",
				description: "Wait until cancelled",
				parameters: { type: "object" },
				execute: async (_args, signal) =>
					await new Promise<string>((_resolve, reject) => {
						if (signal.aborted) {
							aborted = true;
							reject(new Error("host tool cancelled"));
							return;
						}
						signal.addEventListener(
							"abort",
							() => {
								aborted = true;
								reject(new Error("host tool cancelled"));
							},
							{ once: true },
						);
					}),
			};
			const promptResult = harness.worker.prompt("cancel host tool", () => {}, { tools: [tool] }).catch(() => undefined);
			await waitUntil(async () =>
				(await readLog(harness.logPath)).some(entry => {
					if (entry.kind !== "out" || !entry.frame || typeof entry.frame !== "object") return false;
					return (entry.frame as LogEntry).type === "host_tool_cancel";
				}),
			);
			await promptResult;
			expect(aborted).toBe(true);
			const entries = await readLog(harness.logPath);
			const hostResult = entries.find(
				entry => entry.kind === "command" && (entry.command as LogEntry)?.type === "host_tool_result",
			);
			expect(hostResult).toBeDefined();
			expect((hostResult!.command as LogEntry).isError).toBe(true);
		});
	});

	test("aborts a host tool signal during worker stop and does not leave the prompt pending", async () => {
		await withHarness("host-stop", async harness => {
			let aborted = false;
			const tool: HostTool = {
				name: "fixture_echo",
				description: "Wait for worker stop",
				parameters: { type: "object" },
				execute: async (_args, signal) =>
					await new Promise<string>((_resolve, reject) => {
						signal.addEventListener(
							"abort",
							() => {
								aborted = true;
								reject(new Error("worker stopped"));
							},
							{ once: true },
						);
					}),
			};
			const prompt = harness.worker.prompt("stop host tool", () => {}, { tools: [tool] }).catch(() => undefined);
			await waitUntil(async () =>
				(await readLog(harness.logPath)).some(entry => {
					if (entry.kind !== "out" || !entry.frame || typeof entry.frame !== "object") return false;
					return (entry.frame as LogEntry).type === "host_tool_call";
				}),
			);
			const stopped = await harness.worker.stop();
			await prompt;
			expect(stopped).toBe(true);
			expect(aborted).toBe(true);
		});
	});

	test("fails closed when a prompt asks for unsupported extension UI", async () => {
		await withHarness("ui", async harness => {
			const result = await Promise.race([
				harness.worker.prompt("interactive operation", () => {}).then(
					() => ({ kind: "resolved" as const }),
					error => ({ kind: "rejected" as const, error }),
				),
				Bun.sleep(500).then(() => ({ kind: "timeout" as const })),
			]);
			expect(result.kind).toBe("rejected");
			if (result.kind === "rejected") expect(result.error).toBeInstanceOf(Error);
		});
	});

	test("stops the whole process group after abort instead of releasing on parent exit", async () => {
		await withHarness("cancel", async harness => {
			const prompt = harness.worker.prompt("long running operation", () => {}).catch(() => undefined);
			await waitUntil(() => existsSync(harness.markerPath));
			const descendantPid = Number(readFileSync(harness.markerPath, "utf8").trim());
			expect(descendantPid).toBeGreaterThan(0);
			expect(processIsAlive(descendantPid)).toBe(true);

			const stopped = await harness.worker.stop();
			expect(stopped).toBe(true);
			await prompt;
			await waitForProcessExit(descendantPid);
			expect(processIsAlive(descendantPid)).toBe(false);
		});
	}, 5_000);

	test("reaps managed descendants when the RPC parent crashes", async () => {
		await withHarness("crash", async harness => {
			const promptResult = harness.worker.prompt("crash now", () => {}).then(
				() => ({ ok: true as const }),
				error => ({ ok: false as const, error }),
			);
			await waitUntil(() => existsSync(harness.markerPath));
			const descendantPid = Number(readFileSync(harness.markerPath, "utf8").trim());
			const result = await promptResult;
			expect(result.ok).toBe(false);
			await waitForProcessExit(descendantPid);
			expect(processIsAlive(descendantPid)).toBe(false);
		});
	}, 5_000);
	test("watchdog reaps the worker group after the actual channel parent dies", async () => {
		const tempDir = await realpath(await mkdtemp(join(tmpdir(), "omp-watchdog-parent-")));
		const markerPath = join(tempDir, "descendant.alive");
		const readyPath = join(tempDir, "parent.ready");
		const processInfoPath = join(tempDir, "worker-process.json");
		const parent = Bun.spawn(
			[
				process.execPath,
				watchdogParentFixturePath,
				"--temp-dir",
				tempDir,
				"--marker",
				markerPath,
				"--ready",
				readyPath,
				"--process-info",
				processInfoPath,
			],
			{ stdin: "ignore", stdout: "ignore", stderr: "ignore" },
		);
		try {
			await waitUntil(() => existsSync(readyPath), 5_000);
			const workerProcess = JSON.parse(readFileSync(processInfoPath, "utf8")) as WorkerProcess;
			const descendantPid = Number(readFileSync(markerPath, "utf8").trim());
			expect(workerProcess.pid).toBeGreaterThan(0);
			expect(workerProcess.pgid).toBeGreaterThan(0);
			expect(descendantPid).toBeGreaterThan(0);
			expect(processIsAlive(workerProcess.pid)).toBe(true);
			expect(processIsAlive(descendantPid)).toBe(true);

			parent.kill("SIGKILL");
			await parent.exited;
			await waitForProcessExit(workerProcess.pid, 5_000);
			await waitForProcessExit(descendantPid, 5_000);
			expect(processIsAlive(workerProcess.pid)).toBe(false);
			expect(processIsAlive(descendantPid)).toBe(false);
			// A failed watchdog argument mapping must not signal this host test
			// process or release an unverified/unknown PID.
			expect(processIsAlive(process.pid)).toBe(true);
		} finally {
			try {
				parent.kill("SIGKILL");
			} catch {
				// The parent may already have been reaped after the assertion.
			}
			await parent.exited.catch(() => {});
			await rm(tempDir, { recursive: true, force: true });
		}
	}, 10_000);

	test("a watchdog that never becomes ready stops the OMP child and fails with bounded detail", async () => {
		// Real clock by design: the worker factory must stop a real OMP child it just spawned once
		// the real watchdog handshake budget expires; fake timers cannot drive a real child.
		const tempDir = await realpath(await mkdtemp(join(tmpdir(), "omp-watchdog-fail-")));
		const logPath = join(tempDir, "fixture.log");
		const pidPath = join(tempDir, "child.pid");
		try {
			let watchdogPid = 0;
			const factory = createWorkerFactory({
				ompCommand: process.execPath,
				extraArgs: [fixturePath, "--fixture-scenario", "normal", "--fixture-log", logPath, "--fixture-pid-out", pidPath],
				startupTimeoutMs: 400,
				cancelTimeoutMs: 500,
				spawnWatchdog: () => {
					const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1_000);"], {
						stdin: "ignore",
						stdout: "pipe",
						stderr: "pipe",
						detached: true,
					}) as unknown as RpcChild;
					watchdogPid = child.pid;
					return child;
				},
			});
			let failure: unknown;
			try {
				await factory.create({ cwd: tempDir, sessionDir: tempDir, onSpawn: () => {} });
			} catch (error) {
				failure = error;
			}
			expect(failure).toBeInstanceOf(TaskStartupError);
			const startup = failure as TaskStartupError;
			expect(startup.code).toBe("watchdog-unavailable");
			expect(startup.message).toContain("Could not start the OMP parent-death watchdog");
			expect(startup.message).toContain("no ready marker within 400ms");
			// The just-spawned OMP child and the failed watchdog are both reaped.
			await waitUntil(() => existsSync(pidPath), 5_000);
			const workerPid = Number(readFileSync(pidPath, "utf8").trim());
			expect(workerPid).toBeGreaterThan(0);
			await waitForProcessExit(workerPid, 5_000);
			expect(processIsAlive(workerPid)).toBe(false);
			expect(watchdogPid).toBeGreaterThan(0);
			await waitForProcessExit(watchdogPid, 3_000);
			expect(processIsAlive(watchdogPid)).toBe(false);
			const entries = await readLog(logPath);
			expect(entries.some(entry => entry.kind === "prompt")).toBe(false);
		} finally {
			await rm(tempDir, { recursive: true, force: true });
		}
	}, 15_000);

	test("kills a child when durable onSpawn persistence fails and never prompts it", async () => {
		const tempDir = await mkdtemp(join(tmpdir(), "omp-rpc-spawn-failure-"));
		const logPath = join(tempDir, "fixture.log");
		let spawned: WorkerProcess | undefined;
		try {
			const factory = createWorkerFactory({
				ompCommand: process.execPath,
				extraArgs: [fixturePath, "--fixture-scenario", "normal", "--fixture-log", logPath],
				startupTimeoutMs: 2_000,
				cancelTimeoutMs: 500,
			});
			await expect(
				factory.create({
					cwd: tempDir,
					sessionDir: tempDir,
					onSpawn: processInfo => {
						spawned = processInfo;
						throw new Error("fixture persistence failed");
					},
				}),
			).rejects.toThrow("fixture persistence failed");
			expect(spawned?.pid).toBeGreaterThan(0);
			await waitForProcessExit(spawned!.pid);
			const entries = await readLog(logPath);
			expect(entries.some(entry => entry.kind === "prompt")).toBe(false);
		} finally {
			await rm(tempDir, { recursive: true, force: true });
		}
	});

	test("refuses recovery with an unverified identity and leaves the PID untouched", async () => {
		const tempDir = await mkdtemp(join(tmpdir(), "omp-rpc-recover-"));
		try {
			const factory = createWorkerFactory({ ompCommand: process.execPath });
			const recovered = await factory.recover({
				pid: process.pid,
				pgid: process.pid,
				identity: "not-the-current-process-start-identity",
				cwd: tempDir,
			});
			expect(recovered).toBe(false);
			expect(processIsAlive(process.pid)).toBe(true);
		} finally {
			await rm(tempDir, { recursive: true, force: true });
		}
	});

	test("accepts recovery only for an actively owned process identity", async () => {
		await withHarness("normal", async harness => {
			expect(harness.spawned).toBeDefined();
			const factory = createWorkerFactory({ ompCommand: process.execPath });
			await expect(factory.recover(harness.spawned!)).resolves.toBe(true);
			await waitForProcessExit(harness.spawned!.pid);
			expect(processIsAlive(harness.spawned!.pid)).toBe(false);
		});
	});
});
