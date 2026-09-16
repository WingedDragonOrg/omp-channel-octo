import { existsSync, realpathSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import type {
	HostTool,
	OmpWorker,
	PromptContext,
	WorkerEvent,
	WorkerFactory,
	WorkerOptions,
	WorkerProcess,
} from "../contracts.ts";
import { loadValidatedSessionState, type ProjectSessionState } from "../project-session.ts";
import {
	RpcCommand,
	RpcFrame,
	RpcFrameDecoder,
	asError,
	errorFromResponse,
	isRecord,
} from "./rpc.ts";
import {
	RpcChild,
	createWatchdog,
	groupAlive,
	isSafeGroupId,
	queryProcess,
	signalGroup,
	stopWatchdog,
	terminateOwnedProcess,
	terminateProcessGroup,
} from "./process.ts";
import {
	resolveOmpSessionRootForCwd,
	stripEnforcedArgs,
	workerEnv,
} from "./profile.ts";
import {
	HostCall,
	PendingResponse,
	PromptRun,
	asAssistantMessageKey,
	describeUiFailure,
	emitExecutionEvent,
	ensureAssistantMessage,
	eventTime,
	hostResult,
	isUserSteerMessage,
	messageText,
	modelErrorFromFrame,
	safeToolCallId,
	safeToolName,
	safeToolSummary,
	subagentLifecycleEvent,
	subagentProgressEvent,
	todoDetailsFromResult,
	turnIdValue,
} from "./events.ts";

const FIXED_CONTEXT_EXTENSION_PATH = resolve(import.meta.dir, "context-extension.ts");

type WorkerFactoryOptions = {
	ompCommand: string;
	extraArgs?: string[];
	startupTimeoutMs?: number;
	cancelTimeoutMs?: number;
	/**
	 * Extra environment for OMP children, read at spawn time so a credential prepared after
	 * startup reaches later workers. Only `OCTO_CLI_ENV_NAMES` pass through; see `workerEnv`.
	 */
	cliEnv?: () => Record<string, string> | undefined;
};

const DEFAULT_STARTUP_TIMEOUT_MS = 10_000;
const DEFAULT_CANCEL_TIMEOUT_MS = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const PASSIVE_UI_METHODS = new Set(["notify", "setStatus", "setWidget", "setTitle", "set_editor_text", "open_url"]);
class RpcWorker implements OmpWorker {
	#child: RpcChild;
	#watchdog: RpcChild | undefined;
	#pgid: number;
	#startupTimeoutMs: number;
	#cancelTimeoutMs: number;
	#newSessionOnStart: boolean;
	#decoder = new RpcFrameDecoder();
	#protocolV2 = false;
	#ready = false;
	#readyV2Supported = false;
	/** True once OMP confirmed it will stream `subagent_lifecycle`/`subagent_progress` frames. */
	#subagentFrames = false;
	#closed = false;
	#stopping = false;
	#stopPromise: Promise<boolean> | undefined;
	#requestCounter = 0;
	#pending = new Map<string, PendingResponse>();
	#activePrompt: PromptRun | undefined;
	/** True from prompt acknowledgement until the terminal `agent_end` settles the run. */
	#streaming = false;
	#activeTools = new Map<string, HostTool>();
	#hostCalls = new Map<string, HostCall>();
	#sessionFile = "";
	#sessionId = "";
	#readyPromise: Promise<void>;
	#readyResolve: () => void;
	#readyReject: (error: Error) => void;

	constructor(
		child: RpcChild,
		watchdog: RpcChild | undefined,
		pgid: number,
		startupTimeoutMs: number,
		cancelTimeoutMs: number,
		newSessionOnStart: boolean,
	) {
		this.#child = child;
		this.#watchdog = watchdog;
		this.#pgid = pgid;
		this.#startupTimeoutMs = startupTimeoutMs;
		this.#cancelTimeoutMs = cancelTimeoutMs;
		this.#newSessionOnStart = newSessionOnStart;
		const ready = Promise.withResolvers<void>();
		this.#readyPromise = ready.promise;
		this.#readyResolve = () => ready.resolve();
		this.#readyReject = error => ready.reject(error);
		void this.#readStdout();
		void this.#readStderr();
		void this.#child.exited.then(
			code => this.#onChildExit(code),
			error => this.#onFatal(asError(error, "OMP worker exited")),
		);
	}

	get sessionFile(): string {
		return this.#sessionFile;
	}

	get sessionId(): string {
		return this.#sessionId;
	}

	async initialize(): Promise<void> {
		await this.#waitWithTimeout(this.#readyPromise, this.#startupTimeoutMs, "OMP worker did not become ready");
		if (this.#readyV2Supported) {
			const negotiated = await this.#request({ type: "negotiate_protocol", protocolVersion: 2 }, this.#startupTimeoutMs);
			this.#assertSuccess(negotiated, "negotiate_protocol");
			const data = negotiated.data;
			if (!isRecord(data) || data.protocolVersion !== 2) throw new Error("OMP RPC protocol negotiation returned an invalid version");
			this.#protocolV2 = true;
		}
		await this.#subscribeToSubagents();
		if (this.#newSessionOnStart) {
			const newSession = await this.#request({ type: "new_session" }, this.#startupTimeoutMs);
			this.#assertSuccess(newSession, "new_session");
			const data = newSession.data;
			if (!isRecord(data) || data.cancelled !== false) throw new Error("OMP RPC new_session was cancelled");
		}
		await this.refreshState();
	}

	/**
	 * Ask OMP to stream `subagent_lifecycle`/`subagent_progress` frames — the registry defaults to
	 * `off`, so without this command a spawned child is invisible to the channel. The aggregated
	 * `progress` level is enough for the card and a fraction of the volume of raw child events;
	 * an OMP that does not offer the command keeps the worker usable, its subagent section just
	 * stays empty.
	 */
	async #subscribeToSubagents(): Promise<void> {
		try {
			const response = await this.#request(
				{ type: "set_subagent_subscription", level: "progress" },
				this.#startupTimeoutMs,
			);
			const data = isRecord(response.data) ? response.data : undefined;
			if (response.success !== true || data?.level !== "progress") {
				console.warn("[omp] subagent 事件订阅未生效，进度卡不再显示子代理区块");
				return;
			}
			this.#subagentFrames = true;
		} catch {
			console.warn("[omp] 无法订阅 subagent 事件，进度卡不再显示子代理区块");
		}
	}

	async refreshState(): Promise<ProjectSessionState> {
		const stateResponse = await this.#request({ type: "get_state" }, this.#startupTimeoutMs);
		this.#assertSuccess(stateResponse, "get_state");
		const state = stateResponse.data;
		if (
			!isRecord(state) ||
			typeof state.sessionFile !== "string" ||
			state.sessionFile.length === 0 ||
			typeof state.sessionId !== "string" ||
			state.sessionId.length === 0
		)
			throw new Error("OMP RPC get_state did not return a session file and session ID");
		const loaded = await loadValidatedSessionState(state.sessionFile, state.sessionId);
		this.#sessionFile = loaded.state.sessionFile;
		this.#sessionId = loaded.state.sessionId;
		return loaded.state;
	}

	async prompt(text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (this.#activePrompt) throw new Error("OMP worker already has a prompt in progress");
		if (typeof text !== "string") throw new Error("OMP prompt text must be a string");
		const completion = Promise.withResolvers<void>();
		const run: PromptRun = {
			requestId: "",
			onEvent,
			resolve: () => completion.resolve(),
			reject: error => completion.reject(error),
			completion: completion.promise,
			settled: false,
			assistantMessageEnded: false,
			assistantMessageId: undefined,
			assistantMessageKey: undefined,
			assistantMessageText: "",
			assistantMessageCounter: 0,
			completedAssistantMessageIds: new Set(),
			completedAssistantMessageKeys: new Set(),
			toolSummaries: new Map(),
			nextTurnId: 0,
			activeTurnId: undefined,
		};
		this.#activePrompt = run;
		try {
			await this.#installHostTools(context?.tools ?? []);
			const response = await this.#request(
				{ type: "prompt", message: text },
				DEFAULT_REQUEST_TIMEOUT_MS,
				requestId => {
					run.requestId = requestId;
				},
			);
			this.#assertSuccess(response, "prompt");
			if (response.id !== run.requestId) throw new Error("OMP prompt acknowledgement did not contain the expected request ID");
			this.#streaming = true;
			const data = response.data;
			if (isRecord(data) && data.agentInvoked === false) this.#settlePrompt(run);
			await run.completion;
		} catch (error) {
			this.#rejectPrompt(run, asError(error, "OMP prompt failed"));
			throw error;
		} finally {
			this.#abortHostCalls(new Error("prompt finished"));
			await this.#clearHostTools();
			if (this.#activePrompt === run) this.#activePrompt = undefined;
			this.#streaming = false;
		}
	}

	/**
	 * Steering is only meaningful while a turn streams: once the terminal `agent_end` arrived OMP
	 * would park the message in a queue no later turn drains, so the host must fall back to a task.
	 */
	async steer(text: string): Promise<void> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (!this.#activePrompt || !this.#streaming) {
			throw new Error("OMP worker is not streaming a turn");
		}
		if (typeof text !== "string" || text.trim().length === 0) {
			throw new Error("OMP steering text must be a non-empty string");
		}
		const response = await this.#request({ type: "steer", message: text }, DEFAULT_REQUEST_TIMEOUT_MS);
		this.#assertSuccess(response, "steer");
	}

	async abortTurn(): Promise<void> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const response = await this.#request({ type: "abort" }, Math.min(1_000, this.#cancelTimeoutMs));
		this.#assertSuccess(response, "abort");
	}

	async stop(): Promise<boolean> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#stopPromise = this.#stopInternal();
		return this.#stopPromise;
	}


	async #stopInternal(): Promise<boolean> {
		this.#stopping = true;
		this.#abortHostCalls(new Error("worker stopped"));
		const active = this.#activePrompt;
		if (active) this.#rejectPrompt(active, new Error("OMP prompt cancelled"));
		if (!this.#closed) {
			try {
				const abort = await this.#request({ type: "abort" }, Math.min(1_000, this.#cancelTimeoutMs));
				this.#assertSuccess(abort, "abort");
			} catch {
				// Group termination below is authoritative if the control request
				// cannot be acknowledged.
			}
		}
		this.#closed = true;
		this.#rejectPending(new Error("OMP worker stopped"));
		const stopped = await terminateOwnedProcess(this.#child, this.#pgid, this.#cancelTimeoutMs);
		if (stopped) stopWatchdog(this.#watchdog);
		return stopped;
	}

	async #installHostTools(tools: readonly HostTool[]): Promise<void> {
		const definitions: RpcFrame[] = [];
		const nextTools = new Map<string, HostTool>();
		for (const tool of tools) {
			if (!tool || typeof tool.name !== "string" || tool.name.trim().length === 0) throw new Error("Host tool name is required");
			if (nextTools.has(tool.name)) throw new Error(`Duplicate host tool: ${tool.name}`);
			if (typeof tool.description !== "string" || !tool.parameters || typeof tool.parameters !== "object")
				throw new Error(`Invalid host tool definition: ${tool.name}`);
			nextTools.set(tool.name, tool);
			definitions.push({ name: tool.name, description: tool.description, parameters: tool.parameters, loadMode: "essential" });
		}
		const response = await this.#request({ type: "set_host_tools", tools: definitions }, DEFAULT_REQUEST_TIMEOUT_MS);
		this.#assertSuccess(response, "set_host_tools");
		this.#activeTools.clear();
		for (const [name, tool] of nextTools) this.#activeTools.set(name, tool);
	}

	async #clearHostTools(): Promise<void> {
		this.#abortHostCalls(new Error("host tools cleared"));
		this.#activeTools.clear();
		if (this.#closed) return;
		try {
			const response = await this.#request({ type: "set_host_tools", tools: [] }, DEFAULT_REQUEST_TIMEOUT_MS);
			this.#assertSuccess(response, "set_host_tools");
		} catch {
			// The worker may have exited between turn completion and cleanup.
		}
	}

	#assertSuccess(frame: RpcFrame, command: string): void {
		if (frame.success !== true) throw errorFromResponse(frame, command);
	}

	async #request(command: RpcCommand, timeoutMs: number, onId?: (id: string) => void): Promise<RpcFrame> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const id = `omp-${++this.#requestCounter}`;
		onId?.(id);
		const frame = { ...command, id };
		return new Promise<RpcFrame>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(new Error(`Timed out waiting for OMP RPC ${command.type}`));
			}, timeoutMs);
			this.#pending.set(id, { resolve, reject, timer });
			try {
				this.#writeFrame(frame);
			} catch (error) {
				clearTimeout(timer);
				this.#pending.delete(id);
				reject(asError(error, `Could not send OMP RPC ${command.type}`));
			}
		});
	}

	#writeFrame(frame: RpcFrame): void {
		if (this.#closed) throw new Error("OMP worker is not running");
		this.#child.stdin.write(`${JSON.stringify(frame)}\n`);
	}

	async #waitWithTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
		let timer: NodeJS.Timeout;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(message)), timeoutMs);
		});
		try {
			return await Promise.race([promise, timeout]);
		} finally {
			clearTimeout(timer!);
		}
	}

	async #readStdout(): Promise<void> {
		const reader = this.#child.stdout.getReader();
		const decoder = new TextDecoder("utf-8", { fatal: true });
		let buffered = "";
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) break;
				buffered += decoder.decode(result.value, { stream: true });
				for (;;) {
					const newline = buffered.indexOf("\n");
					if (newline < 0) break;
					const line = buffered.slice(0, newline);
					buffered = buffered.slice(newline + 1);
					if (!line.trim()) continue;
					const value: unknown = JSON.parse(line);
					if (!isRecord(value)) throw new Error("OMP RPC frame must be an object");
					this.#handlePhysicalFrame(value);
				}
			}
			buffered += decoder.decode();
			if (buffered.trim().length > 0) throw new Error("OMP RPC output ended with an unterminated frame");
			if (!this.#ready) this.#readyReject(new Error("OMP output ended before ready"));
			else if (!this.#stopping) this.#onFatal(new Error("OMP output ended unexpectedly"));
		} catch (error) {
			if (!this.#ready) this.#readyReject(asError(error, "OMP RPC output could not be decoded"));
			else if (!this.#stopping) this.#onFatal(asError(error, "OMP RPC output could not be decoded"));
		}
	}

	async #readStderr(): Promise<void> {
		const reader = this.#child.stderr.getReader();
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) break;
			}
		} catch {
			// Stderr is deliberately discarded to keep credentials out of errors
			// and logs while still draining the pipe to avoid child deadlocks.
		}
	}

	#handlePhysicalFrame(value: RpcFrame): void {
		if (!this.#ready && value.type === "ready") {
			this.#ready = true;
			this.#readyV2Supported =
				Array.isArray(value.supportedProtocolVersions) && value.supportedProtocolVersions.some(version => version === 2);
			this.#readyResolve();
			return;
		}
		let frame: object | undefined;
		try {
			frame = this.#decoder.push(value);
		} catch (error) {
			this.#onFatal(asError(error, "OMP RPC frame reassembly failed"));
			return;
		}
		if (frame && isRecord(frame)) this.#handleFrame(frame);
	}

	#handleFrame(frame: RpcFrame): void {
		if (frame.type === "response") {
			this.#handleResponse(frame);
			return;
		}
		if (frame.type === "extension_ui_request") {
			this.#handleUiRequest(frame);
			return;
		}
		if (frame.type === "host_tool_call") {
			this.#handleHostToolCall(frame);
			return;
		}
		if (frame.type === "host_tool_cancel") {
			this.#handleHostToolCancel(frame);
			return;
		}
		const active = this.#activePrompt;
		if (!active) return;
		const modelError = modelErrorFromFrame(frame);
		if (modelError) {
			this.#rejectPrompt(active, modelError);
			return;
		}
		if (frame.type === "agent_end") {
			if (frame.willContinue !== true) this.#settlePrompt(active);
			return;
		}
		if (frame.type === "prompt_result" && frame.agentInvoked === false) {
			this.#settlePrompt(active);
			return;
		}
		this.#emitWorkerEvent(active, frame);
	}

	#handleResponse(frame: RpcFrame): void {
		const id = typeof frame.id === "string" ? frame.id : undefined;
		if (id) {
			const pending = this.#pending.get(id);
			if (pending) {
				this.#pending.delete(id);
				clearTimeout(pending.timer);
				pending.resolve(frame);
				return;
			}
			const active = this.#activePrompt;
			if (active && active.requestId === id && frame.success === false) {
				this.#rejectPrompt(active, errorFromResponse(frame, "prompt"));
				return;
			}
		}
	}

	#handleUiRequest(frame: RpcFrame): void {
		const method = typeof frame.method === "string" ? frame.method : "unknown";
		if (PASSIVE_UI_METHODS.has(method)) return;
		try {
			if (typeof frame.id === "string" && !this.#closed) this.#writeFrame({ type: "extension_ui_response", id: frame.id, cancelled: true });
		} catch {
			// The fatal path below still rejects the prompt and stops the group.
		}
		const active = this.#activePrompt;
		if (active) this.#rejectPrompt(active, describeUiFailure(method));
		void this.stop();
	}

	#handleHostToolCall(frame: RpcFrame): void {
		const requestId = typeof frame.id === "string" ? frame.id : "";
		const toolName = typeof frame.toolName === "string" ? frame.toolName : "";
		if (!requestId) return;
		const tool = this.#activeTools.get(toolName);
		if (!tool) {
			this.#sendHostToolResult(requestId, true, `Unknown host tool: ${toolName || "<missing>"}`);
			return;
		}
		const controller = new AbortController();
		const call: HostCall = { requestId, controller };
		this.#hostCalls.set(requestId, call);
		const args = isRecord(frame.arguments) ? frame.arguments : {};
		void Promise.resolve()
			.then(() => tool.execute(args, controller.signal))
			.then(
				text => this.#sendHostToolResult(requestId, false, typeof text === "string" ? text : String(text)),
				error => this.#sendHostToolResult(requestId, true, asError(error, "Host tool failed").message),
			)
			.finally(() => {
				if (this.#hostCalls.get(requestId) === call) this.#hostCalls.delete(requestId);
			});
	}

	#handleHostToolCancel(frame: RpcFrame): void {
		const targetId = typeof frame.targetId === "string" ? frame.targetId : "";
		if (!targetId) return;
		const call = this.#hostCalls.get(targetId);
		if (call) call.controller.abort(new Error("OMP cancelled host tool"));
	}

	#sendHostToolResult(requestId: string, isError: boolean, text: string): void {
		if (this.#closed) return;
		try {
			this.#writeFrame({ type: "host_tool_result", id: requestId, isError, result: hostResult(text) });
		} catch {
			// Process termination will reject the active prompt.
		}
	}

	#emitWorkerEvent(active: PromptRun, frame: RpcFrame): void {
		try {
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
				if (!this.#subagentFrames) return;
				const event = subagentLifecycleEvent(frame.payload, eventTime(frame.timestamp));
				if (event) emitExecutionEvent(active, event);
				return;
			}
			if (frame.type === "subagent_progress") {
				if (!this.#subagentFrames) return;
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
			}
		} catch (error) {
			this.#rejectPrompt(active, asError(error, "OMP event callback failed"));
		}
	}
	#settlePrompt(run: PromptRun): void {
		if (run.settled) return;
		run.settled = true;
		this.#streaming = false;
		run.resolve();
	}

	#rejectPrompt(run: PromptRun, error: Error): void {
		if (run.settled) return;
		run.settled = true;
		this.#streaming = false;
		run.reject(error);
	}

	#abortHostCalls(reason: Error): void {
		for (const call of this.#hostCalls.values()) call.controller.abort(reason);
		this.#hostCalls.clear();
	}

	#rejectPending(error: Error): void {
		for (const pending of this.#pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.#pending.clear();
	}

	#onChildExit(code: number): void {
		if (this.#stopping || this.#closed) return;
		const error = new Error(`OMP worker exited with code ${code}`);
		this.#onFatal(error);
	}

	#onFatal(error: Error): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#readyReject(error);
		this.#rejectPending(error);
		this.#abortHostCalls(error);
		if (this.#activePrompt) this.#rejectPrompt(this.#activePrompt, error);
		void terminateOwnedProcess(this.#child, this.#pgid, this.#cancelTimeoutMs).then(stopped => {
			if (stopped) stopWatchdog(this.#watchdog);
		});
	}
}

class OmpWorkerFactory implements WorkerFactory {
	#options: WorkerFactoryOptions;
	#startupTimeoutMs: number;
	#cancelTimeoutMs: number;

	constructor(options: WorkerFactoryOptions) {
		if (!options || typeof options.ompCommand !== "string" || options.ompCommand.length === 0)
			throw new Error("ompCommand is required");
		this.#options = { ...options, extraArgs: [...(options.extraArgs ?? [])] };
		this.#startupTimeoutMs = options.startupTimeoutMs && options.startupTimeoutMs > 0 ? options.startupTimeoutMs : DEFAULT_STARTUP_TIMEOUT_MS;
		this.#cancelTimeoutMs = options.cancelTimeoutMs && options.cancelTimeoutMs > 0 ? options.cancelTimeoutMs : DEFAULT_CANCEL_TIMEOUT_MS;
	}
	async sessionRootsFor(cwd: string): Promise<readonly string[]> {
		if (!isAbsolute(cwd)) return [];
		return [resolveOmpSessionRootForCwd(resolve(cwd), this.#options.extraArgs ?? [])];
	}

	async create(options: WorkerOptions): Promise<OmpWorker> {

		if (!options || !isAbsolute(options.cwd) || !isAbsolute(options.sessionDir))
			throw new Error("OMP worker cwd and sessionDir must be absolute");
		if (!existsSync(options.cwd)) throw new Error(`OMP worker cwd does not exist: ${options.cwd}`);
		if (!existsSync(options.sessionDir)) throw new Error(`OMP worker sessionDir does not exist: ${options.sessionDir}`);
		const cwd = realpathSync(options.cwd);
		const sessionDir = realpathSync(options.sessionDir);
		if (!existsSync(sessionDir)) throw new Error(`OMP worker sessionDir does not exist: ${sessionDir}`);
		const command = [
			this.#options.ompCommand,
			...stripEnforcedArgs(this.#options.extraArgs ?? []),
			"--extension",
			FIXED_CONTEXT_EXTENSION_PATH,
			"--mode",
			"rpc",
			"--approval-mode",
			"yolo",
			"--cwd",
			cwd,
			"--session-dir",
			sessionDir,
		];
		if (options.sessionFile) command.push("--resume", resolve(options.sessionFile));
		let child: RpcChild;
		try {
			child = Bun.spawn(command, {
				cwd,
				stdin: "pipe",
				stdout: "pipe",
				env: workerEnv(options.contextScope, this.#options.cliEnv?.()),
				stderr: "pipe",
				detached: process.platform !== "win32",
			}) as unknown as RpcChild;
		} catch (error) {
			throw asError(error, "Could not spawn OMP RPC worker");
		}
		const parentSnapshot = await queryProcess(process.pid);
		const snapshot = await queryProcess(child.pid);
		if (!snapshot) {
			signalGroup(child.pid, "SIGKILL");
			try {
				child.kill("SIGKILL");
			} catch {
				// Spawn failure cleanup is best effort; no worker is returned.
			}
			throw new Error("Could not verify the OMP worker process identity");
		}
		const watchdog = await createWatchdog(child.pid, snapshot.pgid, parentSnapshot?.identity ?? "");
		if (process.platform !== "win32" && !watchdog) {
			await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			throw new Error("Could not start the OMP parent-death watchdog");
		}
		const processInfo: WorkerProcess = { pid: child.pid, pgid: snapshot.pgid, identity: snapshot.identity, cwd };
		try {
			options.onSpawn(processInfo);
		} catch (error) {
			const stopped = await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			if (stopped) stopWatchdog(watchdog);
			throw error;
		}
		const worker = new RpcWorker(child, watchdog, snapshot.pgid, this.#startupTimeoutMs, this.#cancelTimeoutMs, !options.sessionFile);
		try {
			await worker.initialize();
			return worker;
		} catch (error) {
			await worker.stop();
			throw asError(error, "OMP RPC worker failed during startup");
		}
	}

	async recover(processInfo: WorkerProcess): Promise<boolean> {
		if (!processInfo || !isSafeGroupId(processInfo.pgid) || !isAbsolute(processInfo.cwd) || !processInfo.identity) return false;
		if (!existsSync(processInfo.cwd)) return false;
		const snapshot = await queryProcess(processInfo.pid);
		if (snapshot) {
			if (snapshot.pgid !== processInfo.pgid || snapshot.identity !== processInfo.identity) return false;
			if (!groupAlive(processInfo.pgid)) return true;
			return terminateProcessGroup(processInfo.pgid, this.#cancelTimeoutMs);
		}
		// A missing worker PID is recoverable only when the whole persisted group
		// is definitely gone.  A live or permission-denied group is ambiguous.
		return !groupAlive(processInfo.pgid);
	}
}

export function createWorkerFactory(options: WorkerFactoryOptions): WorkerFactory {
	return new OmpWorkerFactory(options);
}
