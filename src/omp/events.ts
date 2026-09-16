/**
 * 事件折叠:把 OMP 的 RPC 帧翻译成 `WorkerEvent` 与 `ExecutionEvent`,维护一次 prompt
 * 运行里的 assistant 消息身份、工具摘要与 todo 明细。这里不写协议,也不做卡片布局。
 */

import type { WorkerEvent } from "../contracts.ts";
import type { ExecutionEvent, ExecutionTodoPhase, TodoOperation, TodoStatus } from "../execution-tracker.ts";
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

export function asAssistantMessageKey(message: RpcFrame): string | undefined {
	const id = message.id;
	return typeof id === "string" && id.length > 0 ? id : undefined;
}

/** Text blocks of one message frame; image and other blocks are skipped. */
export function messageText(message: RpcFrame): string {
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
export function isUserSteerMessage(value: unknown): value is RpcFrame {
	return isRecord(value) && value.role === "user" && value.steering === true && value.attribution !== "agent";
}

export function emitExecutionEvent(active: PromptRun, event: ExecutionEvent): void {
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

export function ensureAssistantMessage(active: PromptRun, message: RpcFrame | undefined): string | undefined {
	const key = message ? asAssistantMessageKey(message) : undefined;
	if (!active.assistantMessageId || active.assistantMessageEnded || (key && active.assistantMessageKey && key !== active.assistantMessageKey)) {
		if (key && active.completedAssistantMessageKeys.has(key)) return undefined;
		return allocateAssistantMessage(active, key);
	}
	if (key && !active.assistantMessageKey) active.assistantMessageKey = key;
	return active.assistantMessageId;
}

export function turnIdValue(value: unknown): string | number | undefined {
	if (typeof value === "string" && value.length > 0) return value;
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function eventTime(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : Date.now();
}

export function safeToolName(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const name = value.trim();
	return /^[A-Za-z0-9_.:-]{1,64}$/u.test(name) ? name : undefined;
}

export function safeToolCallId(value: unknown): string | undefined {
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

export function safeToolSummary(toolName: string, args: unknown): string | undefined {
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

export function todoDetailsFromResult(result: unknown): { phases: ExecutionTodoPhase[]; op: TodoOperation | undefined } | undefined {
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


export function describeUiFailure(method: string): Error {
	return new Error(`OMP requested unsupported interactive UI (${method}); worker stopped safely`);
}

