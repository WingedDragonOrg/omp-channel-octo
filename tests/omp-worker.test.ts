import { mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, test } from "bun:test";
import { createWorkerFactory } from "../src/omp/index.ts";
import type { HostTool, OmpWorker, PromptContext, WorkerEvent, WorkerOptions, WorkerProcess } from "../src/contracts.ts";

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

	test("completes a local-only prompt from agentInvoked false without agent events", async () => {
		await withHarness("local", async harness => {
			const events: WorkerEvent[] = [];
			await harness.worker.prompt("/help", event => events.push(event));
			expect(events).toEqual([]);
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
