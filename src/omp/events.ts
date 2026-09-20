/**
 * 事件折叠:把 OMP 的 RPC 帧翻译成 `WorkerEvent` 与 `ExecutionEvent`,维护一次 prompt
 * 运行里的 assistant 消息身份、工具摘要与 todo 明细。这里不写协议,也不做卡片布局。
 */

import type { WorkerEvent, WorkerNoticeEvent } from "../contracts.ts";
import type { ExecutionEvent, ExecutionTodoPhase, TodoOperation, TodoStatus } from "../cards/execution-tracker.ts";
import type { ResearchTracker } from "../research/index.ts";
import { isRecord, type RpcFrame } from "./rpc.ts";

export type PendingResponse = {
	resolve: (frame: RpcFrame) => void;
	reject: (error: Error) => void;
	timer: NodeJS.Timeout;
};

export type HostCall = {
	requestId: string;
	controller: AbortController;
};

export type PromptRun = {
	requestId: string;
	onEvent: (event: WorkerEvent) => void;
	resolve: () => void;
	reject: (error: Error) => void;
	completion: Promise<void>;
	settled: boolean;
	assistantMessageEnded: boolean;
	assistantMessageId: string | undefined;
	assistantMessageKey: string | undefined;
	assistantMessageText: string;
	assistantMessageCounter: number;
	completedAssistantMessageIds: Set<string>;
	completedAssistantMessageKeys: Set<string>;
	toolSummaries: Map<string, string>;
	nextTurnId: number;
	activeTurnId: string | number | undefined;
	/** The prompt-scoped research stream: per-prompt cache, never shared across runs. */
	research: ResearchTracker;
};

export function hostResult(text: string): RpcFrame {
	return { content: [{ type: "text", text }] };
}
export function modelErrorFromFrame(frame: RpcFrame): Error | undefined {
	const candidates: unknown[] = [];
	if (frame.type === "message_end") candidates.push(frame.message);
	if (frame.type === "agent_end" && Array.isArray(frame.messages)) candidates.push(...frame.messages);
	for (const candidate of candidates) {
		if (!isRecord(candidate)) continue;
		const stopReason = candidate.stopReason ?? candidate.stop_reason;
		if (stopReason !== "error") continue;
		const detail =
			typeof candidate.errorMessage === "string"
				? candidate.errorMessage
				: typeof candidate.error === "string"
					? candidate.error
					: "";
		return new Error(detail ? `OMP model reported an error: ${detail}` : "OMP model reported an error");
	}
	return undefined;
}

export const TODO_STATUSES: ReadonlySet<TodoStatus> = new Set([
	"pending",
	"in_progress",
	"completed",
	"abandoned",
	"blocked",
	"cancelled",
]);
export const TODO_OPERATIONS: ReadonlySet<TodoOperation> = new Set([
	"init",
	"start",
	"done",
	"rm",
	"drop",
	"block",
	"unblock",
	"append",
	"view",
]);

export function asTodoStatus(value: unknown): TodoStatus | undefined {
	return typeof value === "string" && TODO_STATUSES.has(value as TodoStatus) ? (value as TodoStatus) : undefined;
}

export function asTodoOperation(value: unknown): TodoOperation | undefined {
	return typeof value === "string" && TODO_OPERATIONS.has(value as TodoOperation) ? (value as TodoOperation) : undefined;
}

function asAssistantMessageKey(message: RpcFrame): string | undefined {
	const id = message.id;
	return typeof id === "string" && id.length > 0 ? id : undefined;
}

/** Text blocks of one message frame; image and other blocks are skipped. */
function messageText(message: RpcFrame): string {
	if (!Array.isArray(message.content)) return "";
	return message.content
		.filter(item => isRecord(item) && item.type === "text" && typeof item.text === "string")
		.map(item => (item as RpcFrame).text as string)
		.join("");
}

/**
 * A steering frame is a user-attributed message marked `steering: true`. The IRC bridge steers
 * its own user-role frames with `attribution: "agent"`, so those stay unreported.
 */
function isUserSteerMessage(value: unknown): value is RpcFrame {
	return isRecord(value) && value.role === "user" && value.steering === true && value.attribution !== "agent";
}

function emitExecutionEvent(active: PromptRun, event: ExecutionEvent): void {
	active.onEvent(event);
}

export function allocateAssistantMessage(active: PromptRun, key: string | undefined): string {
	const messageId = `assistant-${++active.assistantMessageCounter}`;
	active.assistantMessageId = messageId;
	active.assistantMessageKey = key;
	active.assistantMessageText = "";
	active.assistantMessageEnded = false;
	return messageId;
}

function ensureAssistantMessage(active: PromptRun, message: RpcFrame | undefined): string | undefined {
	const key = message ? asAssistantMessageKey(message) : undefined;
	if (!active.assistantMessageId || active.assistantMessageEnded || (key && active.assistantMessageKey && key !== active.assistantMessageKey)) {
		if (key && active.completedAssistantMessageKeys.has(key)) return undefined;
		return allocateAssistantMessage(active, key);
	}
	if (key && !active.assistantMessageKey) active.assistantMessageKey = key;
	return active.assistantMessageId;
}

function turnIdValue(value: unknown): string | number | undefined {
	if (typeof value === "string" && value.length > 0) return value;
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function eventTime(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : Date.now();
}

function safeToolName(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const name = value.trim();
	return /^[A-Za-z0-9_.:-]{1,64}$/u.test(name) ? name : undefined;
}

function safeToolCallId(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const id = value.trim();
	return id.length > 0 ? id : undefined;
}

export const PATH_SUMMARY_TOOLS: ReadonlySet<string> = new Set(["read", "glob", "grep", "write", "edit"]);
export const SAFE_SUMMARY_COMPONENT = /^[A-Za-z0-9._-]{1,64}$/u;
export const TODO_OPERATION_LABELS: Record<string, string> = {
	init: "初始化",
	start: "开始",
	done: "完成",
	rm: "清除",
	drop: "放弃",
	block: "阻塞",
	unblock: "解除阻塞",
	append: "追加",
	view: "查看",
};

export function safeSummaryComponent(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const component = value.trim();
	return SAFE_SUMMARY_COMPONENT.test(component) ? component : undefined;
}

export function safeHubSummary(args: unknown): string {
	if (!isRecord(args)) return "Hub";
	const op = typeof args.op === "string" ? args.op.trim().toLowerCase() : "";
	const to = safeSummaryComponent(args.to);
	const from = safeSummaryComponent(args.from);
	const name = safeSummaryComponent(args.name);
	switch (op) {
		case "send": {
			if (name && !to) return `进程输入 · ${name}`;
			const parts = [`IRC → ${to ?? "agent"}`];
			if (args.await === true) parts.push("等待回复");
			if (to === "all") parts.push("广播");
			return parts.join(" · ");
		}
		case "wait": {
			if (name && !from) return `等待进程 · ${name}`;
			if (from) return `IRC ← ${from}`;
			const ids = Array.isArray(args.ids)
				? args.ids.filter(id => safeSummaryComponent(id) !== undefined).length
				: 0;
			return ids > 0 ? `等待 ${ids} 个后台任务` : "等待消息或任务";
		}
		case "inbox":
			return args.peek === true ? "IRC 收件箱 · 预览" : "IRC 收件箱";
		case "list":
			return "IRC 协作代理";
		case "jobs":
			return "后台任务";
		case "cancel": {
			const ids = Array.isArray(args.ids)
				? args.ids.filter(id => safeSummaryComponent(id) !== undefined).length
				: 0;
			return ids > 0 ? `取消 ${ids} 个后台任务` : "取消后台任务";
		}
		case "start":
		case "ps":
		case "logs":
		case "stop":
		case "restart":
		case "describe":
			return name ? `进程 · ${op} · ${name}` : `进程 · ${op}`;
		default:
			return "Hub";
	}
}

export function safeTaskSummary(args: unknown): string {
	if (!isRecord(args)) return "子代理 · 默认";
	const entries = Array.isArray(args.tasks) ? args.tasks : undefined;
	const count = Math.max(1, entries?.length ?? 1);
	let agent = safeSummaryComponent(args.agent);
	if (!agent && entries?.[0] && isRecord(entries[0])) agent = safeSummaryComponent(entries[0].agent);
	const label = agent ?? "默认";
	return count === 1 ? `子代理 · ${label}` : `子代理 · ${label} ×${count}`;
}

export function safeTodoSummary(args: unknown): string {
	const op = isRecord(args) && typeof args.op === "string" ? args.op.trim().toLowerCase() : "";
	return `任务清单 · ${TODO_OPERATION_LABELS[op] ?? "更新"}`;
}

export function safeAskSummary(args: unknown): string {
	const count = isRecord(args) && Array.isArray(args.questions)
		? Math.min(args.questions.length, 99)
		: 0;
	return count > 0 ? `等待用户回答 · ${count} 个问题` : "等待用户回答";
}

// One bounded line: the progress row prints the command itself, so the summary has to stay short
// enough to read at a glance on a phone-width card.  Credential-shaped text is redacted by the
// card layer, which sanitizes every summary before it reaches a client.
export const COMMAND_SUMMARY_CHARS = 72;

export function safeCommandSummary(args: unknown): string | undefined {
	if (!isRecord(args) || typeof args.command !== "string") return undefined;
	const command = args.command.replace(/\s+/gu, " ").trim();
	if (command.length === 0) return undefined;
	if (command.length <= COMMAND_SUMMARY_CHARS) return command;
	const clipped = command.slice(0, COMMAND_SUMMARY_CHARS - 1);
	const boundary = clipped.lastIndexOf(" ");
	return `${boundary > 0 ? clipped.slice(0, boundary) : clipped}…`;
}

export function safeSpecialToolSummary(toolName: string, args: unknown): string | undefined {
	switch (toolName) {
		case "bash":
			return safeCommandSummary(args);
		case "hub":
			return safeHubSummary(args);
		case "task":
			return safeTaskSummary(args);
		case "todo":
			return safeTodoSummary(args);
		case "ask":
			return safeAskSummary(args);
		case "checkpoint":
			return "建立上下文检查点";
		case "rewind":
			return "恢复上下文检查点";
		case "context_notes":
			return "读取上下文说明";
		case "new_context":
			return "切换上下文";
		case "security_scan":
			return "执行安全扫描";
		case "web_search":
			return "搜索网页";
		case "lsp":
			return "代码智能服务";
		case "github":
			return "GitHub 操作";
		case "memory_edit":
		case "retain":
		case "recall":
		case "reflect":
		case "learn":
		case "manage_skill":
			return "管理上下文资料";
		default:
			return undefined;
	}
}

function safeToolSummary(toolName: string, args: unknown): string | undefined {
	const normalized = toolName.toLowerCase();
	const special = safeSpecialToolSummary(normalized, args);
	if (special !== undefined) return special;
	if (!PATH_SUMMARY_TOOLS.has(normalized) || !isRecord(args) || typeof args.path !== "string") return undefined;
	let path = args.path.trim();
	if (path.startsWith("skill://")) path = path.slice("skill://".length);
	const queryStart = path.search(/[?#]/u);
	if (queryStart >= 0) path = path.slice(0, queryStart);
	path = path.replaceAll("\\", "/");
	const slash = path.lastIndexOf("/");
	let component = path.slice(slash + 1);
	const selector = component.indexOf(":");
	if (selector >= 0) component = component.slice(0, selector);
	component = component.trim();
	return SAFE_SUMMARY_COMPONENT.test(component) ? component : undefined;
}

function todoDetailsFromResult(result: unknown): { phases: ExecutionTodoPhase[]; op: TodoOperation | undefined } | undefined {
	if (!isRecord(result) || !isRecord(result.details) || !Array.isArray(result.details.phases)) return undefined;
	const phases: ExecutionTodoPhase[] = [];
	for (const rawPhase of result.details.phases) {
		if (!isRecord(rawPhase) || typeof rawPhase.name !== "string" || !Array.isArray(rawPhase.tasks)) return undefined;
		const tasks: ExecutionTodoPhase["tasks"][number][] = [];
		for (const rawTask of rawPhase.tasks) {
			if (!isRecord(rawTask) || typeof rawTask.content !== "string") return undefined;
			const status = asTodoStatus(rawTask.status);
			if (!status) return undefined;
			if (typeof rawTask.blocker === "string") {
				tasks.push({ content: rawTask.content, status, blocker: rawTask.blocker });
			} else {
				tasks.push({ content: rawTask.content, status });
			}
		}
		phases.push({ name: rawPhase.name, tasks });
	}
	return { phases, op: asTodoOperation(result.details.op) };
}


/**
 * Subagent lifecycle as OMP reports it, instead of as a tool name implies. The frame's payload is
 * the same `SubagentLifecyclePayload` the interactive HUD consumes; `aborted` is the child's own
 * cancellation and normalizes to the run-level `cancelled` state.
 */
export type SubagentLifecycleEvent = Extract<ExecutionEvent, { type: "subagent_start" | "subagent_end" }>;
export type SubagentProgressEvent = Extract<ExecutionEvent, { type: "subagent_update" }>;

function subagentLifecycleEvent(payload: unknown, atMs: number): SubagentLifecycleEvent | undefined {
	if (!isRecord(payload) || typeof payload.id !== "string" || payload.id.length === 0) return undefined;
	const status = payload.status;
	if (status !== "started" && status !== "completed" && status !== "failed" && status !== "aborted") return undefined;
	const agent = typeof payload.agent === "string" ? payload.agent : "";
	const description = typeof payload.description === "string" ? payload.description : undefined;
	if (status === "started") {
		return {
			type: "subagent_start",
			subagentId: payload.id,
			agent,
			state: "running",
			atMs,
			...(description === undefined ? {} : { summary: description }),
		};
	}
	return {
		type: "subagent_end",
		subagentId: payload.id,
		state: status === "aborted" ? "cancelled" : status,
		atMs,
		...(description === undefined ? {} : { summary: description }),
	};
}

/**
 * One aggregated progress frame: the child's `status`, the intent it is working on, the tool it is
 * currently running, and how many tools it has used. The prompt text (`task`) and the child's
 * recent tool arguments are dropped — the card shows a bounded state line, and raw arguments
 * belong to the child's own transcript.
 */
function subagentProgressEvent(payload: unknown, atMs: number): SubagentProgressEvent | undefined {
	if (!isRecord(payload) || !isRecord(payload.progress)) return undefined;
	const progress = payload.progress;
	if (typeof progress.id !== "string" || progress.id.length === 0) return undefined;
	const status = progress.status;
	if (status !== "pending" && status !== "running") return undefined;
	const intent = typeof progress.lastIntent === "string" ? progress.lastIntent : undefined;
	const currentTool = typeof progress.currentTool === "string" && progress.currentTool.length > 0
		? progress.currentTool
		: undefined;
	const toolCount = typeof progress.toolCount === "number" && Number.isSafeInteger(progress.toolCount) && progress.toolCount >= 0
		? progress.toolCount
		: undefined;
	return {
		type: "subagent_update",
		subagentId: progress.id,
		state: status,
		atMs,
		...(intent === undefined ? {} : { summary: intent }),
		...(currentTool === undefined ? {} : { currentTool }),
		...(toolCount === undefined ? {} : { toolCount }),
	};
}

/**
 * Upstream `extension_ui_request` notifications (method `notify`, e.g. the /autoresearch
 * extension's updates) translated to the bounded WorkerNoticeEvent contract. The upstream
 * frame id is untrusted input: nonempty, control-free and at most NOTICE_ID_CHARS UTF-16
 * code units — anything wider is dropped, never truncated, because an id is a dedup key
 * downstream, and mutating it would merge distinct notifications.
 */
export const NOTICE_ID_CHARS = 200;
/**
 * Bounded message budget: at most 4,000 UTF-16 code units after sanitizing, ellipsis
 * included. The service outbox chunks this further for delivery; the worker bound is what
 * keeps one hostile notification from growing a run's event log without limit.
 */
export const NOTICE_TEXT_CHARS = 4_000;

/** Controls a notification id must never carry: C0 plus DEL. */
const NOTICE_FORBIDDEN_ID_CHARS = /[\x00-\x1F\x7F]/u;

/** Controls stripped from the message body: C0 other than \n \r \t, plus DEL. */
const NOTICE_STRIPPED_TEXT_CHARS = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/gu;

/** Sanitized-body bound with a one-unit ellipsis, cut back to a surrogate-pair boundary. */
function truncateNoticeText(text: string): string {
	if (text.length <= NOTICE_TEXT_CHARS) return text;
	const end = NOTICE_TEXT_CHARS - 1;
	const last = text.charCodeAt(end - 1);
	const boundary = last >= 0xd800 && last <= 0xdbff ? end - 1 : end;
	return `${text.slice(0, boundary)}…`;
}

/**
 * One notify frame as a WorkerNoticeEvent, or undefined when the id is invalid or the
 * message is missing, non-string or blank. An unknown or absent `notifyType` is `info`.
 */
export function noticeEventFromFrame(frame: RpcFrame): WorkerNoticeEvent | undefined {
	if (typeof frame.id !== "string" || frame.id.length === 0 || frame.id.length > NOTICE_ID_CHARS) return undefined;
	if (NOTICE_FORBIDDEN_ID_CHARS.test(frame.id)) return undefined;
	if (typeof frame.message !== "string") return undefined;
	const text = frame.message.replace(NOTICE_STRIPPED_TEXT_CHARS, "").trim();
	if (text.length === 0) return undefined;
	const level = frame.notifyType === "warning" || frame.notifyType === "error" ? frame.notifyType : "info";
	return { type: "notice", messageId: frame.id, text: truncateNoticeText(text), level };
}

export function describeUiFailure(method: string): Error {
	return new Error(`OMP requested unsupported interactive UI (${method}); worker stopped safely`);
}

/**
 * Fold one RPC frame into the run's `WorkerEvent`/`ExecutionEvent` stream: assistant message
 * identity and text-delta/final dedup, turn numbering, the subagent subscription gate, and the
 * tool/todo summaries. `subagentFrames` is the worker's subscription state, passed on every
 * call rather than cached here. A failing consumer callback is deliberately not caught: the
 * error propagates so the caller rejects that one prompt instead of turning the worker fatal.
 */
export function emitWorkerEvent(active: PromptRun, frame: RpcFrame, subagentFrames: boolean): void {
	if (frame.type === "message_start") {
		const message = frame.message;
		if (isUserSteerMessage(message)) {
			active.onEvent({ type: "steer_consumed", text: messageText(message), atMs: eventTime(frame.timestamp) });
			return;
		}
		if (isRecord(message) && message.role === "assistant") {
			const key = asAssistantMessageKey(message);
			if (!(key && active.completedAssistantMessageKeys.has(key))) ensureAssistantMessage(active, message);
		}
		return;
	}
	if (frame.type === "message_update") {
		const message = frame.message;
		if (!isRecord(message) || message.role !== "assistant") return;
		const assistantMessageEvent = frame.assistantMessageEvent;
		if (
			!isRecord(assistantMessageEvent) ||
			assistantMessageEvent.type !== "text_delta" ||
			typeof assistantMessageEvent.delta !== "string"
		) {
			return;
		}
		const messageId = ensureAssistantMessage(active, message);
		if (!messageId) return;
		active.assistantMessageText += assistantMessageEvent.delta;
		active.onEvent({
			type: "text",
			messageId,
			text: active.assistantMessageText,
			final: false,
		});
		return;
	}
	if (frame.type === "message_end") {
		const message = frame.message;
		if (!isRecord(message) || message.role !== "assistant") return;
		const key = asAssistantMessageKey(message);
		if (key && active.completedAssistantMessageKeys.has(key)) return;
		if (active.assistantMessageEnded && (!key || active.assistantMessageKey === key)) return;
		const messageId = ensureAssistantMessage(active, message);
		if (!messageId || active.assistantMessageEnded || active.completedAssistantMessageIds.has(messageId)) return;
		const text = messageText(message);
		active.assistantMessageText = text;
		active.assistantMessageEnded = true;
		active.completedAssistantMessageIds.add(messageId);
		if (key) active.completedAssistantMessageKeys.add(key);
		active.onEvent({ type: "text", messageId, text, final: true });
	}
	if (frame.type === "turn_start") {
		const upstreamTurnId = turnIdValue(frame.turnIndex);
		const turnId = upstreamTurnId ?? active.nextTurnId++;
		if (typeof turnId === "number") active.nextTurnId = Math.max(active.nextTurnId, Math.floor(turnId) + 1);
		active.activeTurnId = turnId;
		emitExecutionEvent(active, { type: "turn_start", turnId, atMs: eventTime(frame.timestamp) });
		return;
	}
	if (frame.type === "turn_end") {
		const turnId = turnIdValue(frame.turnIndex) ?? active.activeTurnId;
		const atMs = eventTime(frame.timestamp);
		if (turnId === undefined) emitExecutionEvent(active, { type: "turn_end", atMs });
		else emitExecutionEvent(active, { type: "turn_end", turnId, atMs });
		active.activeTurnId = undefined;
		return;
	}
	if (frame.type === "subagent_lifecycle") {
		if (!subagentFrames) return;
		const event = subagentLifecycleEvent(frame.payload, eventTime(frame.timestamp));
		if (event) emitExecutionEvent(active, event);
		return;
	}
	if (frame.type === "subagent_progress") {
		if (!subagentFrames) return;
		const event = subagentProgressEvent(frame.payload, eventTime(frame.timestamp));
		if (event) emitExecutionEvent(active, event);
		return;
	}
	if (frame.type === "tool_execution_start") {
		const toolCallId = safeToolCallId(frame.toolCallId);
		const toolName = safeToolName(frame.toolName);
		if (!toolCallId || !toolName) return;
		const summary = safeToolSummary(toolName, frame.args);
		if (summary) active.toolSummaries.set(toolCallId, summary);
		if (summary === undefined) {
			emitExecutionEvent(active, { type: "tool_start", toolCallId, toolName, atMs: Date.now() });
		} else {
			emitExecutionEvent(active, { type: "tool_start", toolCallId, toolName, atMs: Date.now(), summary });
		}
		const researchStart = active.research.startTool(toolName, toolCallId);
		if (researchStart) emitExecutionEvent(active, researchStart);
		return;
	}
	if (frame.type === "tool_execution_end") {
		const toolCallId = safeToolCallId(frame.toolCallId);
		const toolName = safeToolName(frame.toolName);
		if (!toolCallId || !toolName) return;
		const summary = active.toolSummaries.get(toolCallId);
		active.toolSummaries.delete(toolCallId);
		const failed = frame.isError === true;
		const atMs = Date.now();
		if (failed) {
			if (summary === undefined) {
				emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "failed", errorCategory: "unknown" });
			} else {
				emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "failed", errorCategory: "unknown", summary });
			}
		} else if (summary === undefined) {
			emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "succeeded" });
		} else {
			emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "succeeded", summary });
		}
		if (!failed && toolName === "todo") {
			const details = todoDetailsFromResult(frame.result);
			if (details) {
				const todoEvent =
					details.op === undefined
						? { type: "todo_update" as const, atMs, phases: details.phases }
						: { type: "todo_update" as const, atMs, op: details.op, phases: details.phases };
				emitExecutionEvent(active, todoEvent);
			}
		}
		const researchEnd = active.research.endTool(toolName, toolCallId, frame.result, failed);
		if (researchEnd) emitExecutionEvent(active, researchEnd);
	}
}

