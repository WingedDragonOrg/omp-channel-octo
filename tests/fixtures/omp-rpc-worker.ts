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
	| "no-subagent-subscription";

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

async function handlePrompt(id: string | undefined, message: string): Promise<void> {
	appendLog({ kind: "prompt", id, message });
	if (scenario === "local") {
		response(id, "prompt", { agentInvoked: false });
		setTimeout(() => emit({ type: "prompt_result", id, agentInvoked: false }), 1);
		return;
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
			model: undefined,
			thinkingLevel: "off",
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


