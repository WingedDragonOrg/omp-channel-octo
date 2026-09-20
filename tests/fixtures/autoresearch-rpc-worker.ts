/**
 * Deterministic RPC-mode child for the /autoresearch capability benchmark
 * (tests/benchmarks/autoresearch-worker.ts).
 *
 * It mirrors the upstream OMP autoresearch extension (pi-coding-agent
 * src/autoresearch/index.ts + autoresearch/git.ts) at the RPC boundary the production
 * worker speaks:
 *
 * - `/autoresearch` (enable) and `/autoresearch off` reproduce the extension's
 *   `ctx.ui.notify(...)` as `extension_ui_request` frames with method `notify`
 *   (upstream rpc-types.ts: `message` + `notifyType` info/warning/error).
 * - Started with `--fixture-worktree-dirty`, enabling reproduces the upstream
 *   dirty-worktree branch-creation refusal as an error notify; the fixture itself
 *   never runs git.
 * - `/autoresearch <goal>` reproduces `api.sendUserMessage(goal)`: an agent turn with
 *   the four experiment tools (init_experiment, run_experiment, log_experiment,
 *   update_notes) as real `tool_execution_start`/`tool_execution_end` frames, an
 *   `agent_end` with `willContinue: true`, then the continuation body and a terminal
 *   `agent_end`.
 *
 * Everything is fixed and offline: no model, no network, no git, no clock (the session
 * id and every timestamp are constants).
 */

import { writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

type Frame = Record<string, unknown>;

const argv = process.argv.slice(2);

/** The factory appends its enforced flags after extraArgs, so the last occurrence wins. */
function flag(name: string): string | undefined {
	for (let index = argv.length - 1; index >= 0; index--) {
		const value = argv[index]!;
		if (value === name) return argv[index + 1];
		if (value.startsWith(`${name}=`)) return value.slice(name.length + 1);
	}
	return undefined;
}
const SESSION_ID = "fixture-session-autoresearch";
const SESSION_TIMESTAMP = "2026-09-20T00:00:00.000Z";
/** Gap between the prompt acknowledgement and streamed frames, as in omp-rpc-worker.ts. */
const STREAM_DELAY_MS = 15;

/** Literal upstream notify texts (src/autoresearch/index.ts, src/autoresearch/git.ts). */
const NOTIFY_ENABLED = "Autoresearch enabled—describe what to optimize in your next message.";
const NOTIFY_DISABLED = "Autoresearch mode disabled";
const NOTIFY_DIRTY =
	"Worktree is dirty (src/index.ts). Commit or stash these changes before starting autoresearch — a fresh autoresearch/* branch needs a clean baseline.";

/** Deterministic assistant bodies: the goal turn and its willContinue continuation. */
const GOAL_BODY = "autoresearch loop done: ran 3 experiments and kept the best offline score";
const GOAL_BODY_CHUNKS = ["autoresearch loop done: ran 3 experiments ", "and kept the best offline score"] as const;
const RESUME_BODY = "autoresearch resume: no pending run, holding current baseline";

/** Tool args follow the real upstream schemas (src/autoresearch/tools/*.ts). */
const EXPERIMENT_TOOLS = [
	{
		toolCallId: "fixture-autoresearch-init-1",
		toolName: "init_experiment",
		args: {
			name: "offline-autoresearch",
			goal: "tune autoresearch.sh until the offline score stops improving",
			primary_metric: "checks_passed",
			direction: "higher",
			metric_unit: "checks",
		},
		resultText: "init ok",
	},
	{
		toolCallId: "fixture-autoresearch-run-1",
		toolName: "run_experiment",
		args: { timeout_seconds: 600 },
		resultText: "run ok",
	},
	{
		toolCallId: "fixture-autoresearch-log-1",
		toolName: "log_experiment",
		args: { status: "keep", description: "offline loop keeps the baseline", metric: 5 },
		resultText: "log ok",
	},
	{
		toolCallId: "fixture-autoresearch-notes-1",
		toolName: "update_notes",
		args: { body: "## Hypotheses\n- offline loop converged" },
		resultText: "notes ok",
	},
] as const;

/** The only goal prompt this fixture runs; anything else is rejected, never guessed as a goal. */
const GOAL_COMMAND = "/autoresearch tune autoresearch.sh until the offline score stops improving";

const sessionDir = resolve(flag("--session-dir") ?? process.cwd());
const sessionFile = join(sessionDir, "fixture-autoresearch.jsonl");
const worktreeDirty = argv.includes("--fixture-worktree-dirty");
/** The extension's per-session runtime toggle; starts disabled on a fresh session. */
let autoresearchMode = false;

function writeFrames(frames: readonly Frame[]): void {
	process.stdout.write(`${frames.map(frame => JSON.stringify(frame)).join("\n")}\n`);
}
function emit(frame: Frame): void {
	writeFrames([frame]);
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

function persistSessionHeader(): void {
	writeFileSync(
		sessionFile,
		`${JSON.stringify({
			type: "session",
			version: 3,
			id: SESSION_ID,
			timestamp: SESSION_TIMESTAMP,
			cwd: process.cwd(),
		})}\n`,
		"utf8",
	);
}

function notifyFrame(suffix: string, message: string, notifyType: "info" | "warning" | "error"): Frame {
	return {
		type: "extension_ui_request",
		id: `fixture-autoresearch-notify-${suffix}`,
		method: "notify",
		message,
		notifyType,
	};
}

/**
 * Local-only outcome, mirroring upstream rpc-mode: the response acknowledges the prompt,
 * the extension's frames stream afterwards, and `prompt_result agentInvoked:false`
 * settles the run (no agent was invoked).
 */
function respondLocalOnly(id: string | undefined, frames: readonly Frame[]): void {
	response(id, "prompt");
	setTimeout(() => {
		writeFrames(frames);
		emit({ type: "prompt_result", id, agentInvoked: false });
	}, STREAM_DELAY_MS);
}

function emitAssistantText(messageId: string, text: string, chunks: readonly string[]): void {
	const message = { id: messageId, role: "assistant", content: [{ type: "text", text }] };
	emit({ type: "message_start", message: { ...message, content: [] } });
	let streamed = "";
	for (const chunk of chunks) {
		streamed += chunk;
		const partial = { id: messageId, role: "assistant", content: [{ type: "text", text: streamed }] };
		emit({
			type: "message_update",
			assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: chunk, partial },
			message: partial,
		});
	}
	emit({ type: "message_end", message });
}

/**
 * The goal path: `api.sendUserMessage(goal)` starts an agent turn. The assistant streams
 * its body, the four experiment tools run as real execution frames, and the extension's
 * `agent_end` handler schedules the auto-resume continuation — so the first `agent_end`
 * carries `willContinue: true` — after which the continuation body streams and the run
 * settles on the terminal `agent_end`.
 */
function emitGoalRun(): void {
	const goalMessageId = "fixture-autoresearch-goal-1";
	const goalMessage = { id: goalMessageId, role: "assistant", content: [{ type: "text", text: GOAL_BODY }] };
	emit({ type: "agent_start" });
	emit({ type: "turn_start", turnIndex: 0, timestamp: 4_000 });
	emitAssistantText(goalMessageId, GOAL_BODY, GOAL_BODY_CHUNKS);
	for (const tool of EXPERIMENT_TOOLS) {
		emit({ type: "tool_execution_start", toolCallId: tool.toolCallId, toolName: tool.toolName, args: tool.args });
		emit({
			type: "tool_execution_end",
			toolCallId: tool.toolCallId,
			toolName: tool.toolName,
			result: { content: [{ type: "text", text: tool.resultText }] },
			isError: false,
		});
	}
	emit({ type: "turn_end", turnIndex: 0, message: goalMessage, toolResults: [] });
	emit({ type: "agent_end", messages: [goalMessage], willContinue: true });
	// The continuation must arrive in its own batch, one stream delay later: a same-batch
	// delivery could still be consumed by a host that (wrongly) settled the run at the
	// willContinue boundary, masking the early-settle defect this benchmark measures.
	const resumeMessageId = "fixture-autoresearch-resume-1";
	setTimeout(() => {
		emitAssistantText(resumeMessageId, RESUME_BODY, [RESUME_BODY]);
		const resumeMessage = { id: resumeMessageId, role: "assistant", content: [{ type: "text", text: RESUME_BODY }] };
		emit({ type: "agent_end", messages: [goalMessage, resumeMessage], willContinue: false });
	}, STREAM_DELAY_MS);
}

function handlePrompt(id: string | undefined, message: string): void {
	if (message === "/autoresearch") {
		if (autoresearchMode) {
			// Upstream toggles the mode off when the bare command arrives while enabled.
			autoresearchMode = false;
			respondLocalOnly(id, [notifyFrame("disabled-toggle", NOTIFY_DISABLED, "info")]);
			return;
		}
		if (worktreeDirty) {
			respondLocalOnly(id, [notifyFrame("dirty", NOTIFY_DIRTY, "error")]);
			return;
		}
		autoresearchMode = true;
		respondLocalOnly(id, [notifyFrame("enabled", NOTIFY_ENABLED, "info")]);
		return;
	}
	if (message === "/autoresearch off") {
		autoresearchMode = false;
		respondLocalOnly(id, [notifyFrame("disabled", NOTIFY_DISABLED, "info")]);
		return;
	}
	if (message === GOAL_COMMAND) {
		if (worktreeDirty) {
			respondLocalOnly(id, [notifyFrame("dirty", NOTIFY_DIRTY, "error")]);
			return;
		}
		autoresearchMode = true;
		response(id, "prompt");
		setTimeout(() => emitGoalRun(), STREAM_DELAY_MS);
		return;
	}
	failure(id, "prompt", `autoresearch fixture only implements /autoresearch, /autoresearch off and the fixed goal prompt, got: ${message}`, "fixture_prompt");
}

function handleCommand(command: Frame): void {
	const id = typeof command.id === "string" ? command.id : undefined;
	const type = typeof command.type === "string" ? command.type : "";
	if (type === "negotiate_protocol") {
		if (command.protocolVersion !== 2) {
			failure(id, type, "fixture only supports protocol version 2 negotiation", "fixture_protocol");
			return;
		}
		response(id, type, { protocolVersion: 2 });
		return;
	}
	if (type === "new_session") {
		persistSessionHeader();
		response(id, type, { cancelled: false });
		return;
	}
	if (type === "get_state") {
		response(id, type, {
			model: { provider: "fixture", id: "autoresearch", name: "autoresearch" },
			thinkingLevel: "off",
			isStreaming: false,
			isCompacting: false,
			steeringMode: "one-at-a-time",
			followUpMode: "one-at-a-time",
			interruptMode: "immediate",
			sessionFile,
			sessionId: SESSION_ID,
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
		response(id, type, { models: [{ provider: "fixture", id: "autoresearch", name: "autoresearch" }] });
		return;
	}
	if (type === "set_subagent_subscription") {
		const level = typeof command.level === "string" ? command.level : "off";
		response(id, type, { level });
		return;
	}
	if (type === "set_host_tools") {
		const tools = Array.isArray(command.tools) ? command.tools : [];
		const toolNames = tools
			.filter(value => value && typeof value === "object" && !Array.isArray(value))
			.map(value => (value as Frame).name)
			.filter((name): name is string => typeof name === "string");
		response(id, type, { toolNames });
		return;
	}
	if (type === "prompt") {
		handlePrompt(id, typeof command.message === "string" ? command.message : "");
		return;
	}
	if (type === "abort" || type === "abort_and_prompt") {
		response(id, type);
		return;
	}
	if (type === "extension_ui_response") {
		// Passive UI frames need no answer; production never sends these for them.
		return;
	}
	failure(id, type || "unknown", `fixture does not implement command ${type || "<missing>"}`, "fixture_unknown_command");
}

persistSessionHeader();

emit({
	type: "ready",
	protocolVersion: 1,
	supportedProtocolVersions: [1, 2],
	maxFrameBytes: 1024 * 1024,
	maxReassembledFrameBytes: 64 * 1024 * 1024,
});

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
	setTimeout(() => process.exit(0), 5);
});
process.stdin.resume();
