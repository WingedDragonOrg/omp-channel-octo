import { appendFileSync, existsSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

/**
 * Deterministic RPC-mode child used by omp-worker tests.
 *
 * This deliberately does not call a model.  Its only purpose is to exercise
 * the host-side process, framing, and prompt lifecycle contracts through a
 * real stdin/stdout child process.
 */

type Frame = Record<string, unknown>;

type Scenario =
	| "normal"
	| "local"
	| "retry"
	| "error"
	| "same-chunk-error"
	| "message-end"
	| "message-end-no-delta"
	| "message-end-empty"
	| "generated-message-id"
	| "late-message-start"
	| "multi-message"
	| "same-message-text"
	| "tool-progress"
	| "tool-summary"
	| "todo-progress"
	| "hub-summary"
	| "command-summary"
	| "todo-failed"
	| "continuation"
	| "model-error"
	| "v2"
	| "ui"
	| "cancel"
	| "steer"
	| "interrupt"
	| "crash"
	| "host-tool"
	| "host-unknown"
	| "host-cancel"
	| "host-stop"
	| "subagent-events"
	| "no-subagent-subscription"
	| "autoresearch"
	| "research"
	| "research-malformed"
	| "research-higher"
	| "no-models";

const argv = process.argv.slice(2);

function flag(name: string): string | undefined {
	for (let index = argv.length - 1; index >= 0; index--) {
		const value = argv[index]!;
		if (value === name) return argv[index + 1];
		if (value.startsWith(`${name}=`)) return value.slice(name.length + 1);
	}
	return undefined;
}

function numberFlag(name: string, fallback: number): number {
	const value = Number(flag(name));
	return Number.isFinite(value) && value >= 0 ? value : fallback;
}

function appendLog(value: Frame): void {
	const path = flag("--fixture-log");
	if (!path) return;
	try {
		appendFileSync(path, `${JSON.stringify(value)}\n`, "utf8");
	} catch {
		// A fixture log is diagnostic only.  Tests must exercise RPC behaviour,
		// not turn a diagnostic write failure into an unrelated child failure.
	}
}

/**
 * Optional pid report (`--fixture-pid-out <path>`): written before any RPC handshake so tests that
 * stop the child during startup can still find its pid and assert it was reaped.
 */
{
	const path = flag("--fixture-pid-out");
	if (path) {
		try {
			writeFileSync(path, `${process.pid}\n`, "utf8");
		} catch {
			// Diagnostic only.
		}
	}
}

/**
 * Optional environment report (`--env-out <path>`): lets tests assert exactly which
 * environment the host hands to OMP children, including the names it must strip.
 */
function writeEnvReport(): void {
	const path = flag("--env-out");
	if (!path) return;
	try {
		writeFileSync(path, `${JSON.stringify(process.env)}\n`, "utf8");
	} catch {
		// Same rule as the fixture log: a diagnostic write must not fail the child.
	}
}
writeEnvReport();

function writeFrame(frame: Frame): void {
	process.stdout.write(`${JSON.stringify(frame)}\n`);
}
function writeFrames(frames: readonly Frame[]): void {
	process.stdout.write(`${frames.map(frame => JSON.stringify(frame)).join("\n")}\n`);
}

function emitFrames(frames: readonly Frame[]): void {
	for (const frame of frames) appendLog({ kind: "out", frame });
	writeFrames(frames);
}

function writeChunkedFrame(frame: Frame): void {
	const json = JSON.stringify(frame);
	const bytes = Buffer.from(json, "utf8");
	const payloadBytes = 256 * 1024;
	const count = Math.ceil(bytes.byteLength / payloadBytes);
	if (count < 2) throw new Error("fixture v2 frame must have at least two chunks");
	const chunkId = `fixture-chunk-${Date.now()}-${Math.random().toString(16).slice(2)}`;
	for (let index = 0; index < count; index++) {
		const payload = bytes.subarray(index * payloadBytes, (index + 1) * payloadBytes);
		writeFrame({
			type: "rpc_chunk",
			chunkId,
			index,
			count,
			byteLength: bytes.byteLength,
			data: payload.toString("base64"),
		});
	}
}

function emit(frame: Frame, chunk = false): void {
	appendLog({ kind: "out", frame });
	if (chunk) writeChunkedFrame(frame);
	else writeFrame(frame);
}

function response(id: string | undefined, command: string, data?: unknown): void {
	const frame: Frame = { type: "response", id, command, success: true };
	if (data !== undefined) frame.data = data;
	emit(frame);
}

function failure(id: string | undefined, command: string, error: string, code?: string): void {
	const frame: Frame = { type: "response", id, command, success: false, error };
	if (code) frame.code = code;
	emit(frame);
}

async function runDescendant(marker: string, delayMs: number): Promise<void> {
	writeFileSync(marker, `${process.pid}\n`, "utf8");
	appendLog({ kind: "descendant_started", pid: process.pid, marker });
	let stopping = false;
	const stop = () => {
		if (stopping) return;
		stopping = true;
		setTimeout(() => {
			try {
				rmSync(marker, { force: true });
			} finally {
				process.exit(0);
			}
		}, delayMs);
	};
	process.on("SIGTERM", stop);
	process.on("SIGINT", stop);
	process.on("SIGHUP", stop);
	await new Promise<never>(() => {
		// Keep the descendant alive until the worker's process-group stop path
		// signals it.  The marker is removed only as the child exits.
	});
}

if (argv[0] === "--fixture-descendant") {
	const marker = argv[1];
	if (!marker) throw new Error("fixture descendant marker is required");
	await runDescendant(resolve(marker), Number(argv[2] ?? 0));
}

const scenario = (flag("--fixture-scenario") ?? "normal") as Scenario;
const cwdArg = flag("--cwd") ? resolve(flag("--cwd")!) : process.cwd();
const sessionDir = resolve(flag("--session-dir") ?? dirname(flag("--fixture-log") ?? cwdArg));
const resumeArg = flag("--resume") ?? flag("--session");
const sessionFile = resumeArg && (resumeArg.includes("/") || resumeArg.endsWith(".jsonl"))
	? resolve(resumeArg)
	: join(sessionDir, `fixture-${scenario}.jsonl`);
const sessionId = `fixture-session-${scenario}`;
/**
 * `--model` mirrors the real CLI: the first slash separates provider from model id, an optional
 * `:level` suffix sets the thinking level, and a worker started with it reports both.
 */
const THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;
/** The level a session runs at when `--model` names none; OMP always reports a level. */
const FIXTURE_DEFAULT_LEVEL = "xhigh";
const modelFlag = flag("--model");
const modelSelection = ((): { provider: string; id: string; level?: string } | undefined => {
	if (!modelFlag) return undefined;
	const slash = modelFlag.indexOf("/");
	if (slash <= 0 || slash === modelFlag.length - 1) return undefined;
	const tail = modelFlag.slice(slash + 1);
	const level = THINKING_LEVELS.find(candidate => tail.endsWith(`:${candidate}`));
	return level === undefined
		? { provider: modelFlag.slice(0, slash), id: tail }
		: { provider: modelFlag.slice(0, slash), id: tail.slice(0, -level.length - 1), level };
})();
let currentModel =
	modelSelection === undefined ? { provider: "fixture", id: "default" } : { provider: modelSelection.provider, id: modelSelection.id };
let currentLevel: string | null = modelSelection?.level ?? FIXTURE_DEFAULT_LEVEL;
/** Deliberately mixed shapes: a named model, an empty name, a nested id, and one junk entry. */
const availableModels: Array<{ provider: string; id?: string; name?: string }> = [
	{ provider: "fixture", id: "default", name: "Fixture Default" },
	{ provider: "fixture", id: "fast", name: "" },
	{ provider: "fixture", id: "nested/model", name: "Nested" },
	{ provider: "other", id: "fast", name: "Other Fast" },
	{ provider: "broken" },
];
/** Models a selector can resolve to; the junk entry above stays list-only, like an unparsable row. */
const resolvableModels = availableModels.filter(
	(model): model is { provider: string; id: string; name?: string } => typeof model.id === "string",
);
/** Role aliases OMP would expand from `settings.modelRoles`; `@fast` carries a level, `@default` none. */
const fixtureRoles: Record<string, { provider: string; id: string; level?: string }> = {
	fast: { provider: "other", id: "fast", level: "max" },
	default: { provider: "fixture", id: "default" },
};

/**
 * Mirror of OMP's `/switch` resolution over the fixture catalog: `@role` first, then an exact
 * `provider/id`, then a unique model id, then a unique substring; a trailing `:level` sets the
 * thinking level. An unresolvable selector stays unresolved — OMP answers `Unknown model`.
 */
function resolveFixtureSelector(selector: string): { provider: string; id: string; level?: string } | undefined {
	const trimmed = selector.trim();
	if (trimmed.startsWith("@")) {
		const role = fixtureRoles[trimmed.slice(1).toLowerCase()];
		return role === undefined ? undefined : { ...role };
	}
	const level = THINKING_LEVELS.find(candidate => trimmed.endsWith(`:${candidate}`));
	const body = level === undefined ? trimmed : trimmed.slice(0, -level.length - 1);
	if (body.length === 0) return undefined;
	const withLevel = (model: { provider: string; id: string }) =>
		level === undefined ? { provider: model.provider, id: model.id } : { provider: model.provider, id: model.id, level };
	const matches = [
		resolvableModels.filter(model => `${model.provider}/${model.id}` === body),
		resolvableModels.filter(model => model.id === body),
		resolvableModels.filter(model => model.id.includes(body) || `${model.provider}/${model.id}`.includes(body)),
	].find(candidates => candidates.length > 0);
	if (matches === undefined) return undefined;
	return matches.length === 1 ? withLevel(matches[0]!) : undefined;
}
const marker = flag("--fixture-marker");
const descendantDelay = numberFlag("--fixture-descendant-delay", 0);
let negotiated = false;
let stdinClosed = false;
let promptTimer: NodeJS.Timeout | number | undefined;
let descendantPid: number | undefined;
type HostToolRegistration = {
	name: string;
	parameters?: unknown;
};
const hostTools = new Map<string, HostToolRegistration>();
let pendingHostToolRequestId: string | undefined;
let streaming = false;
/**
 * Id of the last completed local-only run, remembered so a later prompt can replay its
 * `prompt_result` as a stale frame; consumed (and cleared) by the next "run goal" prompt.
 */
let staleReplayId: string | undefined;
let retryAvailable = true;
/** Subagent subscription level the fixture last accepted; lifecycle frames follow it. */
let subagentSubscription = "off";
function persistSessionHeader(): void {
	writeFileSync(
		sessionFile,
		`${JSON.stringify({
			type: "session",
			version: 3,
			id: sessionId,
			timestamp: new Date().toISOString(),
			cwd: process.cwd(),
		})}\n`,
		"utf8",
	);
}

// A supplied --resume path represents an already-selected session.  The real
// OMP process materializes a missing explicit path during startup as well.
if (resumeArg) persistSessionHeader();


appendLog({
	kind: "startup",
	argv,
	cwd: process.cwd(),
	cwdArg,
	scenario,
	sessionDir,
	sessionFile,
});

emit({
	type: "ready",
	protocolVersion: 1,
	supportedProtocolVersions: [1, 2],
	maxFrameBytes: 1024 * 1024,
	maxReassembledFrameBytes: 64 * 1024 * 1024,
});

if (scenario === "autoresearch") {
	// An extension re-attaching during startup can broadcast before any prompt exists;
	// with no active run, these frames must never surface as worker events.
	emit({
		type: "extension_ui_request",
		id: "fixture-autoresearch-notify-startup-1",
		method: "notify",
		message: "Autoresearch re-attached during startup.",
		notifyType: "info",
	});
}

async function spawnDescendantIfRequested(): Promise<void> {
	if (!marker || descendantPid !== undefined) return;
	const resolvedMarker = resolve(marker);
	const child = Bun.spawn(
		[process.execPath, import.meta.filename, "--fixture-descendant", resolvedMarker, String(descendantDelay)],
		{ stdin: "ignore", stdout: "ignore", stderr: "ignore" },
	);
	descendantPid = child.pid;
	appendLog({ kind: "descendant_spawned", pid: child.pid, marker: resolvedMarker });
	// The crash test waits on this durable marker, so parent exit cannot race
	// the child before it has advertised that it is alive.
	const deadline = Date.now() + 1_000;
	while (!existsSync(resolvedMarker) && Date.now() < deadline) await Bun.sleep(5);
	if (!existsSync(resolvedMarker)) throw new Error("fixture descendant did not become ready");
	appendLog({ kind: "descendant_ready", pid: child.pid, marker: resolvedMarker });
}

function emitAgentTurn(id: string | undefined): void {
	emit({ type: "agent_start" });
	emitAssistantMessage("fixture completed", 0);
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id });
}
async function emitMessageEndScenario(id: string | undefined): Promise<void> {
	const messageId = "fixture-message-end-1";
	const fullText = "prefix `inline\ncode` suffix\n\nlast (authoritative)";
	const fragments = ["prefix `in", "line\nco", "de` suffix\n\n", "last"];
	let streamedText = "";

	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 1_000 });
	emit({ type: "message_start", message: { id: messageId, role: "assistant", content: [] } });
	for (const fragment of fragments) {
		streamedText += fragment;
		const partial = { id: messageId, role: "assistant", content: [{ type: "text", text: streamedText }] };
		emit({
			type: "message_update",
			assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: fragment, partial },
			message: partial,
		});
		await Bun.sleep(5);
	}
	const partial = {
		id: messageId,
		role: "assistant",
		content: [
			{ type: "text", text: fullText },
			{ type: "thinking", thinking: "SECRET_THINKING" },
		],
	};
	emit({
		type: "message_update",
		assistantMessageEvent: { type: "thinking_delta", contentIndex: 1, delta: "SECRET_THINKING", partial },
		message: partial,
	});
	const assistant = { id: messageId, role: "assistant", content: [{ type: "text", text: fullText }] };
	emit({ type: "message_end", message: assistant });
	emit({ type: "message_end", message: assistant });
	emit({ type: "message_start", message: { id: "fixture-user-1", role: "user", content: [{ type: "text", text: "secret user message" }] } });
	emit({
		type: "message_end",
		message: { id: "fixture-user-1", role: "user", content: [{ type: "text", text: "secret user message" }] },
	});
	emit({
		type: "message_start",
		message: {
			id: "fixture-tool-result-1",
			role: "toolResult",
			toolCallId: "fixture-tool-call-1",
			toolName: "fixture_echo",
			content: [{ type: "text", text: "SECRET_TOOL_RESULT" }],
			isError: false,
		},
	});
	emit({
		type: "message_end",
		message: {
			id: "fixture-tool-result-1",
			role: "toolResult",
			toolCallId: "fixture-tool-call-1",
			toolName: "fixture_echo",
			content: [{ type: "text", text: "SECRET_TOOL_RESULT" }],
			isError: false,
		},
	});
	emit({ type: "turn_end", turnIndex: 0, message: assistant, toolResults: [] });
	emit({ type: "agent_end", messages: [assistant], willContinue: false });
	appendLog({ kind: "turn_complete", id, messageEnd: true });
}
function emitNoDeltaMessageEndScenario(id: string | undefined): void {
	const message = {
		id: "fixture-message-no-delta-1",
		role: "assistant",
		content: [{ type: "text", text: "message without deltas" }],
	};
	emit({ type: "agent_start" });
	emit({ type: "message_start", message: { ...message, content: [] } });
	emit({ type: "message_end", message });
	emit({ type: "message_end", message });
	emit({ type: "agent_end", messages: [message], willContinue: false });
	appendLog({ kind: "turn_complete", id, noDelta: true });
}
function emitEmptyMessageEndScenario(id: string | undefined): void {
	const message = {
		id: "fixture-message-empty-1",
		role: "assistant",
		content: [],
	};
	emit({ type: "agent_start" });
	emit({ type: "message_start", message });
	emit({ type: "message_end", message });
	emit({ type: "message_end", message });
	emit({ type: "agent_end", messages: [message], willContinue: false });
	appendLog({ kind: "turn_complete", id, emptyEnd: true });
}
function emitToolProgressScenario(id: string | undefined): void {
	const assistant = { id: "fixture-progress-assistant-1", role: "assistant", content: [] };
	const toolResult = {
		role: "toolResult",
		toolCallId: "fixture-progress-call-1",
		toolName: "fixture_echo",
		content: [{ type: "text", text: "SECRET_TOOL_RESULT" }],
		isError: false,
	};
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 2_000 });
	emit({ type: "message_start", message: assistant });
	emit({
		type: "message_update",
		assistantMessageEvent: {
			type: "thinking_delta",
			contentIndex: 0,
			delta: "SECRET_THINKING",
			partial: { ...assistant, content: [{ type: "thinking", thinking: "SECRET_THINKING" }] },
		},
		message: { ...assistant, content: [{ type: "thinking", thinking: "SECRET_THINKING" }] },
	});
	emit({
		type: "tool_execution_start",
		toolCallId: "fixture-progress-call-1",
		toolName: "fixture_echo",
		args: { secret: "SECRET_TOOL_ARGUMENTS" },
	});
	emit({
		type: "tool_execution_end",
		toolCallId: "fixture-progress-call-1",
		toolName: "fixture_echo",
		result: toolResult,
		isError: false,
	});
	emit({ type: "message_end", message: assistant });
	emit({ type: "turn_end", turnIndex: 0, message: assistant, toolResults: [toolResult] });
	emit({ type: "agent_end", messages: [assistant], willContinue: false });
	appendLog({ kind: "turn_complete", id, toolProgress: true });
}

function emitToolSummaryScenario(id: string | undefined): void {
	const toolResult = {
		content: [{ type: "text", text: "SECRET_SUMMARY_RESULT" }],
		details: {},
	};
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 4_000 });
	emit({
		type: "tool_execution_start",
		toolCallId: "fixture-summary-read-1",
		toolName: "read",
		args: {
			path: "skill://spec-design",
			Authorization: "Bearer SECRET_AUTHORIZATION",
			secret: "SECRET_SUMMARY_SECRET",
		},
	});
	emit({
		type: "tool_execution_end",
		toolCallId: "fixture-summary-read-1",
		toolName: "read",
		result: toolResult,
		isError: false,
	});
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, toolSummary: true });
}
function emitSpecialToolSummaryScenario(id: string | undefined): void {
	const calls = [
		{
			toolCallId: "fixture-hub-send-1",
			toolName: "hub",
			args: { op: "send", to: "agent-a", await: true, message: "SECRET_IRC_MESSAGE" },
			result: {
				content: [{ type: "text", text: "SECRET_IRC_RESULT" }],
				details: { op: "send", to: "agent-a", receipts: [] },
			},
		},
		{
			toolCallId: "fixture-hub-wait-1",
			toolName: "hub",
			args: { op: "wait", from: "agent-b", timeoutMs: 1_000 },
			result: { content: [{ type: "text", text: "safe wait result" }] },
		},
		{
			toolCallId: "fixture-hub-inbox-1",
			toolName: "hub",
			args: { op: "inbox", peek: true },
			result: { content: [{ type: "text", text: "safe inbox result" }] },
		},
		{
			toolCallId: "fixture-hub-list-1",
			toolName: "hub",
			args: { op: "list" },
			result: { content: [{ type: "text", text: "safe list result" }] },
		},
		{
			toolCallId: "fixture-task-1",
			toolName: "task",
			args: { agent: "scout", task: "SECRET_TASK_PROMPT" },
			result: { content: [{ type: "text", text: "SECRET_TASK_RESULT" }] },
		},
		{
			toolCallId: "fixture-todo-1",
			toolName: "todo",
			args: { op: "done", task: "SECRET_TODO_TASK" },
			result: {
				content: [{ type: "text", text: "SECRET_TODO_RESULT" }],
				details: {
					op: "done",
					phases: [{ name: "Plan", tasks: [{ content: "safe task", status: "completed" }] }],
				},
			},
		},
		{
			toolCallId: "fixture-ask-1",
			toolName: "ask",
			args: { questions: [{ question: "SECRET_QUESTION" }] },
			result: { content: [{ type: "text", text: "SECRET_ASK_RESULT" }] },
		},
	] as const;
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 5_000 });
	for (const call of calls) {
		emit({ type: "tool_execution_start", ...call });
		emit({
			type: "tool_execution_end",
			toolCallId: call.toolCallId,
			toolName: call.toolName,
			result: call.result,
			isError: false,
		});
	}
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, specialToolSummary: true });
}

function emitCommandSummaryScenario(id: string | undefined): void {
	const calls = [
		{
			toolCallId: "fixture-command-serve-1",
			toolName: "bash",
			args: {
				command: "set -a && . ./.envrc && set +a &&\nbun run src/cli.ts serve --config \"$HOME/Library/Application Support/omp-channel-octo/config.json\"",
				cwd: "SECRET_COMMAND_CWD",
				env: { OCTO_BOT_TOKEN: "SECRET_COMMAND_ENV" },
			},
		},
		{
			toolCallId: "fixture-command-grep-2",
			toolName: "bash",
			args: { command: 'grep -rn "tool_execution_start" tests/ | head -20' },
		},
	] as const;
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 6_000 });
	for (const call of calls) {
		emit({ type: "tool_execution_start", ...call });
		emit({
			type: "tool_execution_end",
			toolCallId: call.toolCallId,
			toolName: call.toolName,
			result: { content: [{ type: "text", text: "SECRET_COMMAND_RESULT" }], details: {} },
			isError: false,
		});
	}
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, commandSummary: true });
}

function emitTodoScenario(id: string | undefined, failed = false): void {
	const phases = [
		{
			name: "Planning",
			tasks: [
				{ content: "draft plan", status: "completed" },
				{ content: "ship plan", status: "cancelled" },
				{ content: "wait for review", status: "blocked", blocker: "waiting on review" },
			],
		},
	];
	const toolResult = {
		content: [{ type: "text", text: "SECRET_TODO_TEXT" }],
		details: { op: "init", phases, storage: "session" },
	};
	const assistant = { id: "fixture-todo-assistant-1", role: "assistant", content: [] };
	emit({ type: "agent_start" });
	emit({ type: "turn_start", timestamp: 3_000 });
	emit({ type: "message_start", message: assistant });
	emit({ type: "message_end", message: assistant });
	emit({
		type: "tool_execution_start",
		toolCallId: "fixture-todo-call-1",
		toolName: "todo",
		args: { op: "init", items: [{ content: "SECRET_TODO_ARGUMENTS", status: "pending" }] },
	});
	emit({
		type: "tool_execution_end",
		toolCallId: "fixture-todo-call-1",
		toolName: "todo",
		result: toolResult,
		isError: failed,
	});
	emit({ type: "turn_end", message: assistant, toolResults: [toolResult] });
	emit({ type: "agent_end", messages: [assistant], willContinue: false });
	appendLog({ kind: "turn_complete", id, todoProgress: true, failed });
}

async function emitContinuationScenario(id: string | undefined): Promise<void> {
	emit({ type: "agent_start" });
	emit({ type: "agent_end", messages: [], willContinue: true });
	appendLog({ kind: "continue_pending", id });
	await Bun.sleep(30);
	const message = { id: "fixture-continuation-message-1", role: "assistant", content: [{ type: "text", text: "continued body" }] };
	emit({ type: "message_start", message: { ...message, content: [] } });
	emit({
		type: "message_update",
		assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "continued body", partial: message },
		message,
	});
	emit({ type: "message_end", message });
	emit({ type: "agent_end", messages: [message], willContinue: false });
	appendLog({ kind: "turn_complete", id, continuation: true });
}
function emitAssistantMessage(text: string, index: number, includeId = true): void {
	const message = includeId
		? { id: `fixture-message-${index}`, role: "assistant", content: [{ type: "text", text }] }
		: { role: "assistant", content: [{ type: "text", text }] };
	emit({
		type: "message_start",
		message: { ...message, content: [] },
	});
	emit({
		type: "message_update",
		assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text, partial: message },
		message,
	});
	emit({
		type: "message_end",
		message,
	});
}

function emitGeneratedMessageIdScenario(id: string | undefined): void {
	emit({ type: "agent_start" });
	emitAssistantMessage("generated first", 1, false);
	emitAssistantMessage("generated second", 2, false);
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, generatedMessageIds: true });
}

function emitMultiMessageScenario(id: string | undefined, sameText: boolean): void {
	emit({ type: "agent_start" });
	emitAssistantMessage(sameText ? "same body" : "first body", 1);
	emitAssistantMessage(sameText ? "same body" : "second body", 2);
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, multiMessage: true, sameText });
}


function emitModelErrorScenario(id: string | undefined): void {
	emit({ type: "agent_start" });
	const message = { role: "assistant", stopReason: "error", errorMessage: "fixture model failure" };
	emit({ type: "message_end", message });
	emit({ type: "agent_end", messages: [message], willContinue: false });
	appendLog({ kind: "turn_error", id, stopReason: "error" });
}


function emitV2AgentTurn(id: string | undefined): void {
	emit({ type: "agent_start" });
	// RpcFrameDecoder intentionally rejects tiny v2 chunks.  Keep this logical
	// event over 1 MiB so the test proves real lossless reassembly.
	const text = `fixture-v2-${"x".repeat(1_100_000)}`;
	const message = { id: "fixture-v2-message-1", role: "assistant", content: [{ type: "text", text }] };
	emit({
		type: "message_start",
		message: { ...message, content: [] },
	});
	emit(
		{
			type: "message_update",
			assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text, partial: message },
			message,
		},
		true,
	);
	emit({ type: "message_end", message }, true);
	emit({ type: "agent_end", messages: [message], willContinue: false });
	appendLog({ kind: "turn_complete", id, v2: true, textLength: text.length });
}

function emitHostToolCall(): void {
	pendingHostToolRequestId = "fixture-host-1";
	const toolName =
		flag("--host-tool-name") ??
		(scenario === "host-unknown" ? "missing_fixture_tool" : "fixture_echo");
	const customArguments = flag("--host-tool-args");
	let toolArguments: Frame = { message: "hello from fixture" };
	if (customArguments !== undefined) {
		const parsed: unknown = JSON.parse(customArguments);
		if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
			throw new Error("--host-tool-args must be a JSON object");
		}
		toolArguments = parsed as Frame;
	}
	emit({ type: "agent_start" });
	emit({
		type: "host_tool_call",
		id: pendingHostToolRequestId,
		toolCallId: "fixture-tool-call-1",
		toolName,
		arguments: toolArguments,
	});
	if (scenario === "host-cancel") {
		setTimeout(() => {
			if (!pendingHostToolRequestId) return;
			emit({
				type: "host_tool_cancel",
				id: "fixture-host-cancel-1",
				targetId: pendingHostToolRequestId,
			});
		}, 20);
	}
}

function resultText(result: unknown): string {
	if (!result || typeof result !== "object" || Array.isArray(result)) return String(result ?? "");
	const content = (result as Frame).content;
	if (!Array.isArray(content)) return String(result);
	const text = content.find(
		value => value && typeof value === "object" && !Array.isArray(value) && (value as Frame).type === "text",
	);
	return text && typeof text === "object" && !Array.isArray(text) && typeof (text as Frame).text === "string"
		? ((text as Frame).text as string)
		: String(result);
}

function completeHostToolTurn(command: Frame): void {
	const isError = command.isError === true;
	const result = resultText(command.result);
	const text = isError ? `fixture tool failed: ${result}` : `fixture tool returned: ${result}`;
	const message = { id: "fixture-host-assistant-1", role: "assistant", content: [{ type: "text", text }] };
	pendingHostToolRequestId = undefined;
	emit({ type: "message_start", message: { ...message, content: [] } });
	emit({
		type: "message_update",
		assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text, partial: message },
		message,
	});
	emit({ type: "message_end", message });
	emit({ type: "agent_end", messages: [message], willContinue: false });
}

/**
 * Mirrors the upstream autoresearch extension's `ctx.ui.notify` calls: `extension_ui_request`
 * frames with method `notify` carrying `message` and `notifyType` (same shape as the frozen
 * capability fixture in tests/fixtures/autoresearch-rpc-worker.ts, which cannot grow new
 * rows). The batch deliberately mixes every level, an unlabeled and an unknown level so they
 * default, a whitespace-padded and a control-laden multiline message so sanitizing is visible,
 * and malformed ids/messages plus passive widgets that must all be ignored.
 */
function autoresearchNotifyFrames(): Frame[] {
	const enabled = "Autoresearch enabled—describe what to optimize in your next message.";
	return [
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-enabled", method: "notify", message: enabled, notifyType: "info" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-warn-1", method: "notify", message: "Autoresearch worktree is dirty — commit or stash these changes first.", notifyType: "warning" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-error-1", method: "notify", message: "Autoresearch run failed — offline score regressed, baseline kept.", notifyType: "error" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-plain", method: "notify", message: "Autoresearch heartbeat is still running." },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-odd-level", method: "notify", message: "Autoresearch level tokens are advisory only.", notifyType: "shouting" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-padded", method: "notify", message: "  Autoresearch pads this notice with whitespace.  ", notifyType: "info" },
		{
			type: "extension_ui_request",
			id: "fixture-autoresearch-notify-multiline",
			method: "notify",
			message: "Autoresearch step one done.\nStep two pending.\tQueued at 09:00. \u001B",
			notifyType: "info",
		},
		// A 201-unit id exceeds the bounded id contract and is dropped; its 200-unit
		// neighbour stays valid.
		{ type: "extension_ui_request", id: "n".repeat(201), method: "notify", message: "overlong ids are dropped", notifyType: "info" },
		{ type: "extension_ui_request", id: "n".repeat(200), method: "notify", message: "the 200-unit id boundary stays usable", notifyType: "info" },
		{ type: "extension_ui_request", id: "", method: "notify", message: "an empty id is dropped", notifyType: "info" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-\u0007-bell", method: "notify", message: "an id with control characters is dropped", notifyType: "info" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-missing-message", method: "notify", notifyType: "info" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-numeric-message", method: "notify", message: 42, notifyType: "info" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-blank-message", method: "notify", message: " \n\t ", notifyType: "info" },
		// Upstream re-broadcasts the same notification id when its HUD re-attaches; the
		// worker forwards both occurrences and the service stream dedups by message id.
		{ type: "extension_ui_request", id: "fixture-autoresearch-notify-enabled", method: "notify", message: enabled, notifyType: "info" },
		// Widgets and status lines share the delivery channel but stay passive.
		{ type: "extension_ui_request", id: "fixture-autoresearch-widget-1", method: "setWidget", widget: "autoresearch", content: "autoresearch running" },
		{ type: "extension_ui_request", id: "fixture-autoresearch-status-1", method: "setStatus", text: "autoresearch running" },
	];
}

function emitAutoresearchNotifyTurn(id: string | undefined): void {
	writeFrames(autoresearchNotifyFrames());
	emit({ type: "prompt_result", id, agentInvoked: false });
	// A re-broadcast after settlement must not reopen the settled run or bleed into the
	// next prompt; the log marker lets tests wait until it has been offered.
	setTimeout(() => {
		writeFrames([
			{
				type: "extension_ui_request",
				id: "fixture-autoresearch-notify-late-1",
				method: "notify",
				message: "Autoresearch re-broadcast after settlement.",
				notifyType: "info",
			},
		]);
		appendLog({ kind: "turn_complete", id, notifyTurn: true });
	}, 15);
}

/**
 * Oversized control-laden Unicode notification. The bounded contract is 4,000 UTF-16 code
 * units of sanitized text, so the surrogate pair straddling the naive cut position is the
 * tripwire for implementations that slice code units blindly.
 */
function emitAutoresearchHugeNotifyTurn(id: string | undefined): void {
	// NUL/ESC/DEL must be stripped, leaving 22 visible units: "alphabravocharliedelta".
	const head = "alpha\u0000bravo\u001Bcharlie\u007Fdelta";
	// After sanitizing, the pair occupies code units 3,998–3,999: a naive `slice(0, 3999)`
	// would end on a lone high surrogate.
	const filler = "字".repeat(3_998 - 22);
	const message = ` \t${head}${filler}😀${"z".repeat(600)}`;
	writeFrames([
		{
			type: "extension_ui_request",
			id: "fixture-autoresearch-notify-huge-1",
			method: "notify",
			message,
			notifyType: "warning",
		},
	]);
	emit({ type: "prompt_result", id, agentInvoked: false });
	appendLog({ kind: "turn_complete", id, notifyTurn: true, hugeLength: message.length });
}

/**
 * Native autoresearch tool choreography as OMP's RPC bridge reports it. The result `details`
 * mirror the installed upstream tools (@oh-my-pi/pi-coding-agent/src/autoresearch/tools/*.ts,
 * typed in @oh-my-pi/pi-tui/src/tools/autoresearch.ts). Two upstream truths are mirrored
 * exactly: `state.bestMetric` is the segment BASELINE (the first unflagged keep), never the
 * best kept metric, and a freshly logged `details.experiment` is always `flagged: false` even
 * when the same call's `flag_runs` flags that very run — only the state rows carry the flag.
 * Old records may carry `runNumber: null` (upstream's findBaselineRunNumber exists for these
 * legacy unnumbered rows). Every payload is laced with ASI blobs, raw harness output, shell
 * commands, free-form notes and host paths so tests can prove none of it reaches an event.
 */
type ResearchProfile = {
	name: string;
	/** Upstream ExperimentState.goal is nullable (init_experiment's goal is optional). */
	goal: string | null;
	metricName: string;
	metricUnit: string;
	direction: "lower" | "higher";
	segment: number;
	sessionId: number;
	maxExperiments: number | null;
	branch: string;
};
const RESEARCH_LOWER: ResearchProfile = {
	name: "offline-score",
	goal: "tune the offline score until it stops improving",
	metricName: "score",
	metricUnit: "ms",
	direction: "lower",
	segment: 1,
	sessionId: 42,
	maxExperiments: 8,
	branch: "autoresearch/offline-score",
};
const RESEARCH_BROKEN: ResearchProfile = {
	name: "shiny-latency",
	goal: "drive the checkout latency down",
	metricName: "latency",
	metricUnit: "ms",
	direction: "lower",
	segment: 0,
	sessionId: 7,
	maxExperiments: null,
	branch: "autoresearch/shiny-latency",
};
const RESEARCH_HIGHER: ResearchProfile = {
	name: "throughput-drive",
	// The model never stated a goal: upstream serves null (createExperimentState's default),
	// which the stream must normalize to an empty goal instead of rejecting the state.
	goal: null,
	metricName: "throughput",
	metricUnit: "rps",
	direction: "higher",
	segment: 0,
	sessionId: 9,
	maxExperiments: 3,
	branch: "autoresearch/throughput-drive",
};

function researchToolStart(toolCallId: string, toolName: string, args: Frame): Frame {
	return { type: "tool_execution_start", toolCallId, toolName, args };
}
function researchToolEnd(toolCallId: string, toolName: string, result: Frame, isError = false): Frame {
	return { type: "tool_execution_end", toolCallId, toolName, result, isError };
}
function researchToolResult(details: Frame | undefined, text: string): Frame {
	const result: Frame = { content: [{ type: "text", text }] };
	if (details !== undefined) result.details = details;
	return result;
}
/** One ExperimentResult row in the upstream state; every field the schema documents is present. */
function researchRunRow(profile: ResearchProfile, over: Frame): Frame {
	return {
		runNumber: null,
		commit: "c0ffee123456",
		metric: 0,
		metrics: { [profile.metricName]: 0 },
		status: "keep",
		description: "",
		timestamp: 1_758_000_000_000,
		segment: profile.segment,
		confidence: null,
		asi: { token: "SECRET_ASI" },
		modifiedPaths: ["/Users/SECRET_HOST_PATH/worktree/src/SECRET_MODIFIED_PATH.ts"],
		scopeDeviations: [],
		justification: null,
		flagged: false,
		flaggedReason: null,
		...over,
	};
}
/**
 * Upstream ExperimentState. `bestMetric` deliberately carries the BASELINE, matching
 * buildExperimentState (findBaselineMetric), so a consumer that trusts it as the best is
 * caught by the fixture's trips.
 */
function researchState(profile: ResearchProfile, results: readonly Frame[], baselineMetric: number | null): Frame {
	return {
		results,
		bestMetric: baselineMetric,
		bestDirection: profile.direction,
		metricName: profile.metricName,
		metricUnit: profile.metricUnit,
		secondaryMetrics: [],
		name: profile.name,
		goal: profile.goal,
		currentSegment: profile.segment,
		maxExperiments: profile.maxExperiments,
		confidence: null,
		scopePaths: ["src/knob.ts"],
		offLimits: [],
		constraints: [],
		notes: "SECRET_NOTES free-form operator notes",
		branch: profile.branch,
		baselineCommit: "ba5e11necommitsha",
		sessionId: profile.sessionId,
	};
}
function researchInitDetails(
	profile: ResearchProfile,
	results: readonly Frame[],
	baselineMetric: number | null,
	createdSession: boolean,
): Frame {
	return {
		state: researchState(profile, results, baselineMetric),
		createdSession,
		bumpedSegment: !createdSession,
		abandonedRuns: 0,
		harnessCommitted: true,
		baselineCommit: "ba5e11necommitsha",
	};
}
/** Upstream RunDetails: pending measurements plus every leak vector the tool reports. */
function researchRunDetails(profile: ResearchProfile, runNumber: number, parsedPrimary: number): Frame {
	return {
		runNumber,
		runDirectory: `/Users/SECRET_HOST_PATH/.omp/autoresearch/run-${runNumber}`,
		benchmarkLogPath: `/Users/SECRET_HOST_PATH/.omp/autoresearch/run-${runNumber}/benchmark.log`,
		command: "SECRET_HARNESS_COMMAND -- bash autoresearch.sh",
		exitCode: 0,
		durationSeconds: 2.5,
		passed: true,
		crashed: false,
		timedOut: false,
		tailOutput: `${profile.metricName}: ${parsedPrimary}\nSECRET_RAW_OUTPUT from the benchmark harness`,
		parsedMetrics: { [profile.metricName]: parsedPrimary },
		parsedPrimary,
		parsedAsi: { token: "SECRET_ASI" },
		metricName: profile.metricName,
		metricUnit: profile.metricUnit,
		preRunDirtyPaths: ["/Users/SECRET_HOST_PATH/dirty.ts"],
		abandonedPriorRun: null,
		fullOutputPath: `/Users/SECRET_HOST_PATH/.omp/autoresearch/run-${runNumber}/full.log`,
	};
}
function researchLogDetails(
	profile: ResearchProfile,
	experiment: Frame,
	results: readonly Frame[],
	baselineMetric: number | null,
	flaggedRuns: readonly Frame[] = [],
): Frame {
	return {
		experiment,
		state: researchState(profile, results, baselineMetric),
		wallClockSeconds: 2.5,
		scopeDeviations: [],
		justification: null,
		flaggedRuns,
	};
}
/** One physically written frame: JSON.stringify cannot produce 1e400, the raw line can. */
function emitRawFrameLine(json: string): void {
	appendLog({ kind: "out-raw", json });
	process.stdout.write(`${json}\n`);
}

/**
 * A bumped session's full loop across two continued turns of one prompt. The init history
 * plants two trips: another segment's unnumbered keep (metric 5 would win a segment leak),
 * and a current-segment legacy discard with `runNumber: null` (valid but unnumbered — dropping
 * it shrinks the counts, fabricating an id breaks the row equality). Logs 8/9/10 then carry
 * `state.bestMetric: 10` — the upstream baseline — while the real best moves to 8, and the
 * final keep (metric 0) is flagged by its own log call, so only the state row knows it.
 * Duplicate and ghost replays of finished tool frames close the turn: a re-notified identical
 * state is tolerated, but none of them may roll the authoritative snapshot or phase back.
 */
async function emitResearchLoopScenario(id: string | undefined): Promise<void> {
	const profile = RESEARCH_LOWER;
	const seg0Champion = researchRunRow(profile, {
		runNumber: null,
		segment: 0,
		metric: 5,
		description: "pre-migration segment champion",
		timestamp: 1_757_000_000_000,
	});
	const legacyProbe = researchRunRow(profile, {
		runNumber: null,
		metric: 1,
		status: "discard",
		description: "migrated regression probe",
		timestamp: 1_757_000_100_000,
	});
	const run8 = researchRunRow(profile, { runNumber: 8, metric: 10, status: "keep", description: "fresh baseline" });
	const run9 = researchRunRow(profile, { runNumber: 9, metric: 8, status: "keep", description: "cache-friendly layout" });
	const run10Logged = researchRunRow(profile, { runNumber: 10, metric: 0, status: "keep", description: "suspicious perfect score" });
	const run10Row = researchRunRow(profile, {
		runNumber: 10,
		metric: 0,
		status: "keep",
		description: "suspicious perfect score",
		flagged: true,
		flaggedReason: "suspect scale wiring",
	});
	const history1 = [seg0Champion, legacyProbe];
	const history2 = [seg0Champion, legacyProbe, run8];
	const history3 = [seg0Champion, legacyProbe, run8, run9];
	const history4 = [seg0Champion, legacyProbe, run8, run9, run10Row];
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 1_000 });
	emit(
		researchToolStart("research-init-1", "init_experiment", {
			name: profile.name,
			goal: profile.goal,
			primary_metric: profile.metricName,
			metric_unit: profile.metricUnit,
			direction: profile.direction,
			new_segment: true,
			max_iterations: profile.maxExperiments,
		}),
	);
	emit(researchToolEnd("research-init-1", "init_experiment", researchToolResult(researchInitDetails(profile, history1, null, false), `Bumped segment to 1 for session #42: ${profile.name}`)));
	emit(researchToolStart("research-run-8", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-8", "run_experiment", researchToolResult(researchRunDetails(profile, 8, 10), "Run #8 passed")));
	emit(researchToolStart("research-log-8", "log_experiment", { metric: 10, status: "keep", description: "fresh baseline" }));
	emit(researchToolEnd("research-log-8", "log_experiment", researchToolResult(researchLogDetails(profile, run8, history2, 10), "Logged run #8")));
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: true });
	appendLog({ kind: "research_turn", id, turn: 0 });
	await Bun.sleep(10);
	emit({ type: "turn_start", turnIndex: 1, timestamp: 2_000 });
	emit(researchToolStart("research-run-9", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-9", "run_experiment", researchToolResult(researchRunDetails(profile, 9, 8), "Run #9 passed")));
	emit(researchToolStart("research-log-9", "log_experiment", { metric: 8, status: "keep", description: "cache-friendly layout" }));
	emit(researchToolEnd("research-log-9", "log_experiment", researchToolResult(researchLogDetails(profile, run9, history3, 10), "Logged run #9")));
	emit(researchToolStart("research-run-10", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-10", "run_experiment", researchToolResult(researchRunDetails(profile, 10, 0), "Run #10 passed")));
	emit(
		researchToolStart("research-log-10", "log_experiment", {
			metric: 0,
			status: "keep",
			description: "suspicious perfect score",
			flag_runs: [{ run_id: 10, reason: "suspect scale wiring" }],
		}),
	);
	emit(
		researchToolEnd(
			"research-log-10",
			"log_experiment",
			researchToolResult(
				researchLogDetails(profile, run10Logged, history4, 10, [{ runId: 10, reason: "suspect scale wiring" }]),
				"Logged run #10",
			),
		),
	);
	// The final tool frames replay: a duplicate of the finished log, a duplicate of the
	// finished run, and a ghost end no start ever named. None may reopen awaiting_log,
	// re-append history, or emit anything at all.
	emit(
		researchToolEnd(
			"research-log-10",
			"log_experiment",
			researchToolResult(
				researchLogDetails(profile, run10Logged, history4, 10, [{ runId: 10, reason: "suspect scale wiring" }]),
				"Logged run #10",
			),
		),
	);
	emit(researchToolEnd("research-run-10", "run_experiment", researchToolResult(researchRunDetails(profile, 10, 0), "Run #10 passed")));
	emit(researchToolEnd("research-run-ghost", "run_experiment", researchToolResult(researchRunDetails(profile, 99, -50), "Run #99 passed")));
	emit({ type: "turn_end", turnIndex: 1 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, research: true });
}

/**
 * Corruption gauntlet: one honest init/run/log surrounded by result shapes that must never
 * become research events — the args-only upstream error ("no pending run"), a state whose
 * results is not an array, an isError result with a fully valid-looking payload, states with
 * one poisoned row (a "constructor" status or an Infinity metric — one invalid row rejects
 * the whole authoritative state), a non-native tool carrying state-shaped details,
 * update_notes traffic, stale ends whose ids no start ever named (string run number,
 * JSON-absent metric, raw 1e400 Infinity), and a run whose start predates the landed log
 * (its late completion is stale and must not reopen awaiting_log). The single exception:
 * a STARTED run whose end errored reverts the phase back to ready — the only update a
 * crashed benchmark may produce, without its numbers.
 */
function emitResearchMalformedScenario(id: string | undefined): void {
	const profile = RESEARCH_BROKEN;
	const run2 = researchRunRow(profile, { runNumber: 2, metric: 6, status: "keep", description: "first honest keep" });
	const forged = researchRunRow(profile, { runNumber: 41, metric: 88, status: "keep", description: "SECRET_FORGED_DESCRIPTION" });
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 1_000 });
	emit(
		researchToolStart("research-init", "init_experiment", {
			name: profile.name,
			goal: profile.goal,
			primary_metric: profile.metricName,
			metric_unit: profile.metricUnit,
		}),
	);
	emit(researchToolEnd("research-init", "init_experiment", researchToolResult(researchInitDetails(profile, [], null, true), `Started session #7: ${profile.name}`)));
	// Args-only temptation: upstream's real error text arrives with no details at all, so
	// any run history built from the args below is a fabrication.
	emit(
		researchToolStart("research-log-args", "log_experiment", { metric: 88, status: "keep", description: "forged from args" }),
	);
	emit(
		researchToolEnd(
			"research-log-args",
			"log_experiment",
			researchToolResult(undefined, "Error: no pending run available. Run run_experiment first."),
		),
	);
	// Malformed state: results is not an array.
	emit(
		researchToolEnd(
			"research-log-shapeless",
			"log_experiment",
			researchToolResult(
				{ experiment: forged, state: { ...researchState(profile, [], 88), results: "not-an-array" }, flaggedRuns: [] },
				"forged shape",
			),
		),
	);
	// isError with a fully valid-looking payload: an errored tool is never authoritative.
	emit(
		researchToolEnd("research-log-errored", "log_experiment", researchToolResult(researchLogDetails(profile, forged, [forged], 88), "tool exploded"), true),
	);
	// A poisoned enum row: "constructor" rides the prototype chain into a naive membership
	// lookup — the row is invalid, and one invalid row rejects the whole authoritative state.
	emit(researchToolStart("research-log-poisoned-status", "log_experiment", { metric: 3, status: "keep", description: "poisoned enum" }));
	emit(
		researchToolEnd(
			"research-log-poisoned-status",
			"log_experiment",
			researchToolResult(
				researchLogDetails(
					profile,
					researchRunRow(profile, { runNumber: 44, metric: 3, status: "keep", description: "SECRET_FORGED_DESCRIPTION" }),
					[
						researchRunRow(profile, { runNumber: 44, metric: 3, status: "keep", description: "SECRET_FORGED_DESCRIPTION" }),
						researchRunRow(profile, { runNumber: 45, metric: 2, status: "constructor", description: "poisoned enum row" }),
					],
					3,
				),
				"poisoned enum",
			),
		),
	);
	// A poisoned metric row: 1e400 parses to Infinity — again one invalid row rejects the
	// whole state rather than silently shrinking the counts.
	emit(researchToolStart("research-log-poisoned-metric", "log_experiment", { metric: 4, status: "keep", description: "poisoned metric" }));
	// The Infinity literal cannot survive JSON.stringify, so the marker is patched into the
	// serialized frame as raw text (same trick as the 1e400 run end above).
	emitRawFrameLine(
		JSON.stringify(
			researchToolEnd(
				"research-log-poisoned-metric",
				"log_experiment",
				researchToolResult(
					researchLogDetails(
						profile,
						researchRunRow(profile, { runNumber: 46, metric: 4, status: "keep", description: "SECRET_FORGED_DESCRIPTION" }),
						[
							researchRunRow(profile, { runNumber: 46, metric: 4, status: "keep", description: "SECRET_FORGED_DESCRIPTION" }),
							{ ...researchRunRow(profile, { runNumber: 47, status: "keep", description: "infinity metric row" }), metric: "__INF__" },
						],
						4,
					),
					"poisoned metric",
				),
			),
		).replace('"__INF__"', "1e400"),
	);
	// A non-native tool carrying state-shaped details is not part of the research loop.
	emit(researchToolStart("research-bash", "bash", { command: "cat experiment-state.json" }));
	emit(
		researchToolEnd(
			"research-bash",
			"bash",
			researchToolResult({ state: researchState(profile, [forged], 88) }, "SECRET_TOOL_NAME state dump"),
		),
	);
	emit(researchToolEnd("research-notes", "update_notes", researchToolResult({ notes: "SECRET_NOTES smuggled through update_notes" }, "notes updated")));
	// A real run whose end errored: the phase reverts to ready, but its valid-looking details
	// must not open awaiting_log, grow history, or become the latest metric.
	emit(researchToolStart("research-run-broken", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-broken", "run_experiment", researchToolResult(researchRunDetails(profile, 1, 3), "run blew up"), true));
	// Stale ends no start ever named: string run number, JSON-absent metric, and Infinity.
	emit(
		researchToolEnd(
			"research-run-string-id",
			"run_experiment",
			researchToolResult({ runNumber: "2", parsedPrimary: 2, metricName: profile.metricName, metricUnit: profile.metricUnit }, "stale"),
		),
	);
	emit(
		researchToolEnd(
			"research-run-null-metric",
			"run_experiment",
			researchToolResult({ runNumber: 5, parsedPrimary: null, metricName: profile.metricName, metricUnit: profile.metricUnit }, "stale"),
		),
	);
	emitRawFrameLine(
		`{"type":"tool_execution_end","toolCallId":"research-run-infinity","toolName":"run_experiment","result":{"content":[{"type":"text","text":"overflow"}],"details":{"runNumber":6,"parsedPrimary":1e400,"metricName":"latency","metricUnit":"ms"}},"isError":false}`,
	);
	// The loop's one honest run and its log.
	emit(researchToolStart("research-run-2", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-2", "run_experiment", researchToolResult(researchRunDetails(profile, 2, 6), "Run #2 passed")));
	emit(researchToolStart("research-run-late", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolStart("research-log-2", "log_experiment", { metric: 6, status: "keep", description: "first honest keep" }));
	emit(researchToolEnd("research-log-2", "log_experiment", researchToolResult(researchLogDetails(profile, run2, [run2], 6), "Logged run #2")));
	// The run whose start predates the landed log: its late end must never reopen
	// awaiting_log on top of the newer authoritative state, and must not lend its numbers.
	emit(researchToolEnd("research-run-late", "run_experiment", researchToolResult(researchRunDetails(profile, 9, 2), "Run #9 passed")));
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, researchMalformed: true });
}

/**
 * Higher-is-better with a zero baseline: the first keep (metric 0) is both baseline and best,
 * a larger discard (15) never counts, a keep (7) takes the best, and the third completed run
 * reaches max_iterations, so the last update reports limit_reached. Upstream serves
 * `state.bestMetric: 0` throughout — the baseline, truthy-falsy trap included.
 */
function emitResearchHigherScenario(id: string | undefined): void {
	const profile = RESEARCH_HIGHER;
	const run1 = researchRunRow(profile, { runNumber: 1, metric: 0, status: "keep", description: "zero baseline" });
	const run2 = researchRunRow(profile, { runNumber: 2, metric: 15, status: "discard", description: "discarded spike" });
	const run3 = researchRunRow(profile, { runNumber: 3, metric: 7, status: "keep", description: "widened batch" });
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 1_000 });
	emit(
		researchToolStart("research-init", "init_experiment", {
			// The model never passed a goal: the key is absent, and upstream serves goal:null.
			name: profile.name,
			primary_metric: profile.metricName,
			metric_unit: profile.metricUnit,
			direction: profile.direction,
			max_iterations: profile.maxExperiments,
		}),
	);
	emit(researchToolEnd("research-init", "init_experiment", researchToolResult(researchInitDetails(profile, [], null, true), `Started session #9: ${profile.name}`)));
	emit(researchToolStart("research-run-1", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-1", "run_experiment", researchToolResult(researchRunDetails(profile, 1, 0), "Run #1 passed")));
	emit(researchToolStart("research-log-1", "log_experiment", { metric: 0, status: "keep", description: "zero baseline" }));
	emit(researchToolEnd("research-log-1", "log_experiment", researchToolResult(researchLogDetails(profile, run1, [run1], 0), "Logged run #1")));
	emit(researchToolStart("research-run-2", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-2", "run_experiment", researchToolResult(researchRunDetails(profile, 2, 15), "Run #2 passed")));
	emit(researchToolStart("research-log-2", "log_experiment", { metric: 15, status: "discard", description: "discarded spike" }));
	emit(researchToolEnd("research-log-2", "log_experiment", researchToolResult(researchLogDetails(profile, run2, [run1, run2], 0), "Logged run #2")));
	emit(researchToolStart("research-run-3", "run_experiment", { command: "bash autoresearch.sh" }));
	emit(researchToolEnd("research-run-3", "run_experiment", researchToolResult(researchRunDetails(profile, 3, 7), "Run #3 passed")));
	emit(researchToolStart("research-log-3", "log_experiment", { metric: 7, status: "keep", description: "widened batch" }));
	emit(researchToolEnd("research-log-3", "log_experiment", researchToolResult(researchLogDetails(profile, run3, [run1, run2, run3], 0), "Logged run #3")));
	emit({ type: "turn_end", turnIndex: 0 });
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, researchHigher: true });
}

async function handlePrompt(id: string | undefined, message: string): Promise<void> {
	appendLog({ kind: "prompt", id, message });
	if (message.startsWith("/switch")) {
		// OMP dispatches `/switch` as a builtin: no agent turn, a `command_output` line, and on
		// success the `config_update` frame that announces the resolved model and level. The
		// dispatch is local, so it is served even while another prompt is streaming.
		const selector = message.slice("/switch".length).trim();
		const resolved = resolveFixtureSelector(selector);
		if (resolved === undefined) {
			emit({ type: "command_output", text: `Unknown model: ${selector}` });
			response(id, "prompt", { agentInvoked: false });
			return;
		}
		currentModel = { provider: resolved.provider, id: resolved.id };
		if (resolved.level !== undefined) currentLevel = resolved.level;
		emit({ type: "command_output", text: `Session-only model: ${currentModel.provider}/${currentModel.id}.` });
		emit({ type: "config_update", model: { ...currentModel }, thinkingLevel: currentLevel });
		response(id, "prompt", { agentInvoked: false });
		return;
	}
	if (scenario === "retry" && message === "/retry") {
		emit({ type: "command_output", text: retryAvailable ? "Retrying the last failed turn." : "Nothing to retry." });
		response(id, "prompt", { agentInvoked: retryAvailable });
		if (retryAvailable) {
			retryAvailable = false;
			promptTimer = setTimeout(() => emitAgentTurn(id), 60);
		}
		return;
	}
	if (scenario === "local") {
		response(id, "prompt", { agentInvoked: false });
		setTimeout(() => emit({ type: "prompt_result", id, agentInvoked: false }), 1);
		return;
	}

	if (scenario === "autoresearch") {
		// The extension's local-only commands: the ack goes out first, its notify frames
		// stream afterwards, and `prompt_result agentInvoked:false` settles the run.
		if (message === "/autoresearch") {
			response(id, "prompt");
			setTimeout(() => emitAutoresearchNotifyTurn(id), 15);
			return;
		}
		if (message === "/autoresearch huge") {
			response(id, "prompt");
			setTimeout(() => emitAutoresearchHugeNotifyTurn(id), 15);
			return;
		}
		if (message === "/autoresearch legacy") {
			// Legacy OMP emits the local-only settle frame without a request id; it must still
			// count as the run's local completion.
			response(id, "prompt");
			setTimeout(() => {
				emit({ type: "prompt_result", agentInvoked: false });
				appendLog({ kind: "turn_complete", id, legacySettle: true });
			}, 15);
			return;
		}
		if (message === "/autoresearch quiet") {
			// A local-only completion without any notify traffic: its prompt_result carries the
			// request id, which the next goal replays as the stale late frame.
			staleReplayId = id;
			response(id, "prompt");
			setTimeout(() => {
				emit({ type: "prompt_result", id, agentInvoked: false });
				appendLog({ kind: "turn_complete", id, quietSettle: true });
			}, 15);
			return;
		}
		if (message === "run goal") {
			const replayId = staleReplayId;
			staleReplayId = undefined;
			response(id, "prompt", { agentInvoked: true });
			streaming = true;
			if (replayId !== undefined) {
				// The finished local run's `prompt_result` arrives late, mid-goal, together
				// with junk-id variants; none names this prompt, so none may settle it.
				setTimeout(() =>
					emitFrames([
						{ type: "prompt_result", id: replayId, agentInvoked: false },
						{ type: "prompt_result", id: 42, agentInvoked: false },
						{ type: "prompt_result", id: null, agentInvoked: false },
					]),
				10);
			}
			promptTimer = setTimeout(() => emitAgentTurn(id), 60);
			return;
		}
		// Anything else takes the stock agent-turn path below; the isolation test uses it
		// to prove earlier notifications never bleed into a later prompt.
	}

	if (scenario === "research" || scenario === "research-malformed" || scenario === "research-higher") {
		if (message === "run research") {
			response(id, "prompt", { agentInvoked: true });
			streaming = true;
			const emitScenario =
				scenario === "research"
					? emitResearchLoopScenario
					: scenario === "research-malformed"
						? async (promptId: string | undefined) => emitResearchMalformedScenario(promptId)
						: async (promptId: string | undefined) => emitResearchHigherScenario(promptId);
			promptTimer = setTimeout(() => void emitScenario(id), 35);
			return;
		}
		// Any other message takes the stock agent-turn path: the per-prompt isolation test
		// proves the follow-up prompt never inherits the research stream.
	}

	if (scenario === "same-chunk-error") {
		emitFrames([
			{ type: "response", id, command: "prompt", success: true, data: { agentInvoked: true } },
			{
				type: "response",
				id,
				command: "prompt",
				success: false,
				error: "fixture same-chunk prompt failure",
				code: "fixture_same_chunk_error",
			},
		]);
		return;
	}
	response(id, "prompt", { agentInvoked: true });
	streaming = true;
	if (scenario === "late-message-start") {
		const message = { role: "assistant", content: [] };
		emit({ type: "agent_start" });
		emit({ type: "message_update", message, assistantMessageEvent: { type: "text_delta", delta: "已" } });
		emit({ type: "message_start", message });
		emit({ type: "message_update", message, assistantMessageEvent: { type: "text_delta", delta: "发送" } });
		emit({ type: "message_start", message });
		emit({ type: "message_end", message: { ...message, content: [{ type: "text", text: "已发送" }] } });
		emitAssistantMessage("下一条", 2, false);
		emit({ type: "agent_end", messages: [], willContinue: false });
		return;
	}
	if (scenario === "continuation") {
		setTimeout(() => void emitContinuationScenario(id), 35);
		return;
	}
	if (scenario === "message-end-no-delta") {
		setTimeout(() => emitNoDeltaMessageEndScenario(id), 35);
		return;
	}
	if (scenario === "message-end-empty") {
		setTimeout(() => emitEmptyMessageEndScenario(id), 35);
		return;
	}
	if (scenario === "todo-progress") {
		setTimeout(() => emitTodoScenario(id), 35);
		return;
	}
	if (scenario === "todo-failed") {
		setTimeout(() => emitTodoScenario(id, true), 35);
		return;
	}
	if (scenario === "generated-message-id") {
		setTimeout(() => emitGeneratedMessageIdScenario(id), 35);
		return;
	}
	if (scenario === "tool-progress") {
		setTimeout(() => emitToolProgressScenario(id), 35);
		return;
	}
	if (scenario === "tool-summary") {
		setTimeout(() => emitToolSummaryScenario(id), 35);
		return;
	}
	if (scenario === "hub-summary") {
		setTimeout(() => emitSpecialToolSummaryScenario(id), 35);
		return;
	}
	if (scenario === "command-summary") {
		setTimeout(() => emitCommandSummaryScenario(id), 35);
		return;
	}
	if (scenario === "message-end") {
		setTimeout(() => emitMessageEndScenario(id), 35);
		return;
	}
	if (scenario === "multi-message" || scenario === "same-message-text") {
		setTimeout(() => emitMultiMessageScenario(id, scenario === "same-message-text"), 35);
		return;
	}
	if (scenario === "model-error") {
		setTimeout(() => emitModelErrorScenario(id), 35);
		return;
	}
	if (scenario === "host-tool" || scenario === "host-unknown" || scenario === "host-cancel" || scenario === "host-stop") {
		emitHostToolCall();
		return;
	}
	if (scenario === "error") {
		promptTimer = setTimeout(() => {
			failure(id, "prompt", "fixture delayed prompt failure", "fixture_delayed_error");
		}, 40);
		return;
	}
	if (scenario === "ui") {
		setTimeout(() => {
			emit({
				type: "extension_ui_request",
				id: "fixture-ui-1",
				method: "confirm",
				title: "Fixture confirmation",
				message: "No UI host is configured",
			});
		}, 15);
		return;
	}
	if (scenario === "steer") {
		// The run stays open until a steering frame arrives; only that frame ends it.
		emit({ type: "agent_start" });
		emitAssistantMessage("fixture first turn", 1, true);
		promptTimer = setTimeout(() => {
			if (!streaming) return;
			failure(id, "prompt", "fixture steer scenario never received a steering frame", "fixture_steer_timeout");
		}, numberFlag("--fixture-steer-timeout", 5_000));
		return;
	}
	if (scenario === "interrupt") {
		// A partial turn with no end frame: the host must abort it and keep the worker alive.
		setTimeout(() => {
			emit({ type: "agent_start" });
			emitAssistantMessage("fixture partial turn", 1, true);
		}, 10);
		return;
	}
	if (scenario === "cancel" || scenario === "crash") await spawnDescendantIfRequested();
	if (scenario === "crash") {
		setTimeout(() => process.exit(71), 35);
		return;
	}
	if (scenario === "subagent-events" || scenario === "no-subagent-subscription") {
		await emitSubagentScenario(id);
		return;
	}
	if (scenario === "v2") {
		setTimeout(() => {
			if (!negotiated) {
				failure(id, "prompt", "fixture v2 prompt arrived before negotiation", "fixture_protocol");
				return;
			}
			emitV2AgentTurn(id);
		}, 35);
		return;
	}
	promptTimer = setTimeout(() => emitAgentTurn(id), scenario === "normal" ? 90 : 60);
}

async function emitSubagentScenario(id: string | undefined): Promise<void> {
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 1_000 });
	// The parent spawns one child through `task`; the child's own lifetime is only observable
	// through the subagent channels, never through that tool call's name or result.
	emit({
		type: "tool_execution_start",
		toolCallId: "fixture-task-call",
		toolName: "task",
		args: { tasks: [{ agent: "scout", task: "SECRET_CHILD_ASSIGNMENT" }] },
	});
	if (subagentSubscription !== "progress" && subagentSubscription !== "events") {
		emitAssistantMessage("fixture subagent turn", 1, true);
		emit({ type: "agent_end", messages: [], willContinue: false });
		appendLog({ kind: "turn_complete", id, subagentFrames: 0 });
		return;
	}
	emit({
		type: "subagent_lifecycle",
		payload: {
			id: "CountSrc",
			agent: "scout",
			agentSource: "bundled",
			status: "started",
			description: "统计源码文件",
			index: 0,
			parentToolCallId: "fixture-task-call",
			detached: true,
		},
	});
	emit({
		type: "subagent_progress",
		payload: {
			index: 0,
			agent: "scout",
			agentSource: "bundled",
			task: "SECRET_CHILD_ASSIGNMENT",
			parentToolCallId: "fixture-task-call",
			detached: true,
			progress: {
				id: "CountSrc",
				index: 0,
				agent: "scout",
				agentSource: "bundled",
				status: "running",
				task: "SECRET_CHILD_ASSIGNMENT",
				lastIntent: "扫描 src",
				currentTool: "glob",
				toolCount: 2,
				recentTools: [{ tool: "glob", args: "SECRET_CHILD_ARGS", endMs: 1_500 }],
			},
		},
	});
	await Bun.sleep(5);
	emit({
		type: "subagent_lifecycle",
		payload: {
			id: "CountSrc",
			agent: "scout",
			agentSource: "bundled",
			status: "completed",
			description: "统计源码文件",
			index: 0,
			parentToolCallId: "fixture-task-call",
			detached: true,
		},
	});
	emit({
		type: "tool_execution_end",
		toolCallId: "fixture-task-call",
		toolName: "task",
		result: { content: [{ type: "text", text: "Spawned 1 background agent" }] },
	});
	emitAssistantMessage("fixture subagent turn", 1, true);
	emit({ type: "agent_end", messages: [], willContinue: false });
	appendLog({ kind: "turn_complete", id, subagentFrames: 2 });
}

function handleCommand(command: Frame): void {
	appendLog({ kind: "command", command });
	const id = typeof command.id === "string" ? command.id : undefined;
	const type = typeof command.type === "string" ? command.type : "";
	if (type === "negotiate_protocol") {
		if (command.protocolVersion !== 2) {
			failure(id, type, "fixture only supports protocol version 2 negotiation", "fixture_protocol");
			return;
		}
		negotiated = true;
		response(id, type, { protocolVersion: 2 });
		return;
	}
	if (type === "new_session") {
		persistSessionHeader();
		appendLog({ kind: "session_materialized", sessionFile, sessionId });
		response(id, type, { cancelled: false });
		return;
	}
	if (type === "get_state") {
		response(id, type, {
			model: { ...currentModel, name: currentModel.id },
			thinkingLevel: currentLevel ?? "off",
			isStreaming: false,
			isCompacting: false,
			steeringMode: "one-at-a-time",
			followUpMode: "one-at-a-time",
			interruptMode: "immediate",
			sessionFile,
			sessionId,
			autoCompactionEnabled: true,
			fastModeEnabled: false,
			fastModeActive: false,
			tokensPerSecond: null,
			messageCount: 0,
			queuedMessageCount: 0,
			todoPhases: [],
		});
		return;
	}
	if (type === "get_available_models") {
		if (scenario === "no-models") {
			response(id, type, { models: [] });
			return;
		}
		response(id, type, { models: availableModels.map(model => ({ ...model })) });
		return;
	}
	if (type === "set_subagent_subscription") {
		if (scenario === "no-subagent-subscription") {
			failure(id, type, "fixture rejects subagent subscriptions", "fixture_subagent_unsupported");
			return;
		}
		subagentSubscription = typeof command.level === "string" ? command.level : "off";
		response(id, type, { level: subagentSubscription });
		return;
	}
	if (type === "set_host_tools") {
		hostTools.clear();
		const tools = Array.isArray(command.tools) ? command.tools : [];
		for (const value of tools) {
			if (!value || typeof value !== "object" || Array.isArray(value)) continue;
			const tool = value as Frame;
			if (typeof tool.name !== "string") continue;
			hostTools.set(tool.name, { name: tool.name, parameters: tool.parameters });
		}
		response(id, type, { toolNames: [...hostTools.keys()] });
		return;
	}
	if (type === "host_tool_result") {
		if (!pendingHostToolRequestId || id !== pendingHostToolRequestId) {
			appendLog({ kind: "unexpected_host_tool_result", id });
			return;
		}
		completeHostToolTurn(command);
		return;
	}
	if (type === "prompt") {
		void handlePrompt(id, typeof command.message === "string" ? command.message : "");
		return;
	}
	if (type === "steer") {
		appendLog({ kind: "steer", id, message: command.message });
		if (!streaming) {
			failure(id, type, "fixture is not streaming a turn", "fixture_not_streaming");
			return;
		}
		response(id, type);
		clearTimeout(promptTimer);
		promptTimer = undefined;
		const steered = typeof command.message === "string" ? command.message : "";
		// Mirrors OMP: the queue drains at the next injection boundary, the message enters the
		// transcript, and only then does the model turn run.
		const steerMessage = {
			role: "user",
			content: [{ type: "text", text: steered }],
			steering: true,
			attribution: "user",
			timestamp: Date.now(),
		};
		emit({ type: "turn_start", turnIndex: 1, timestamp: Date.now() });
		emit({ type: "message_start", message: steerMessage });
		emit({ type: "message_end", message: steerMessage });
		emitAssistantMessage(`fixture steered turn: ${steered}`, 2, true);
		emit({ type: "agent_end", messages: [], willContinue: false });
		streaming = false;
		return;
	}
	if (type === "abort") {
		if (promptTimer) clearTimeout(promptTimer);
		promptTimer = undefined;
		response(id, type);
		if (scenario === "interrupt" && streaming) {
			streaming = false;
			emit({ type: "agent_end", messages: [], willContinue: false });
		}
		if (scenario === "cancel") {
			// Exit the RPC parent immediately.  The descendant intentionally
			// remains alive, so the host must stop the whole owned process group
			// instead of treating parent exit as successful termination.
			setTimeout(() => process.exit(0), 5);
		}
		return;
	}
	if (type === "abort_and_prompt") {
		if (promptTimer) clearTimeout(promptTimer);
		response(id, type);
		return;
	}
	if (type === "get_last_assistant_text") {
		response(id, type, { text: "fixture completed" });
		return;
	}
	failure(id, type || "unknown", `fixture does not implement command ${type || "<missing>"}`, "fixture_unknown_command");
}

let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string | Buffer) => {
	input += String(chunk);
	for (;;) {
		const newline = input.indexOf("\n");
		if (newline < 0) break;
		const line = input.slice(0, newline);
		input = input.slice(newline + 1);
		if (!line.trim()) continue;
		try {
			const value: unknown = JSON.parse(line);
			if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("command must be an object");
			handleCommand(value as Frame);
		} catch (error) {
			failure(undefined, "parse", error instanceof Error ? error.message : String(error), "fixture_parse");
		}
	}
});
process.stdin.on("end", () => {
	if (stdinClosed) return;
	stdinClosed = true;
	appendLog({ kind: "stdin_closed", descendantPid });
	clearTimeout(promptTimer);
	// Do not attempt to clean up the descendant here: a real worker must own
	// and terminate descendants via its process-group stop path.
	setTimeout(() => process.exit(0), 5);
});
process.stdin.resume();


