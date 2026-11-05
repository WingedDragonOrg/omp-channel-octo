import { existsSync, realpathSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import type {
	CommitWorker,
	CommitWorkerOptions,
	HostTool,
	OmpWorker,
	PromptContext,
	WorkerEvent,
	WorkerFactory,
	WorkerGoal,
	WorkerGoalErrorCode,
	WorkerGoalResult,
	WorkerGoalRun,
	WorkerGoalStatus,
	WorkerContextUsage,
	WorkerModelCatalog,
	WorkerModelInfo,
	WorkerModelSwitch,
	WorkerOptions,
	WorkerProcess,
} from "../contracts.ts";
import {
	RESUME_CONTINUATION_PROMPT,
	TaskStartupError,
	WorkerGoalError,
	WorkerSteerRejectedError,
	taskStartupError,
} from "../contracts.ts";
import { loadValidatedSessionState, type ProjectSessionState } from "../session/project-session.ts";
import { formatAgentGuidance } from "../session/agent-context.ts";
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
import { ResearchTracker } from "../research/index.ts";
import {
	HostCall,
	PendingResponse,
	PromptRun,
	emitWorkerEvent,
	describeUiFailure,
	hostResult,
	modelErrorFromFrame,
	noticeEventFromFrame,
} from "./events.ts";
import { createNativeCommitWorker } from "./commit.ts";

const FIXED_CONTEXT_EXTENSION_PATH = resolve(import.meta.dir, "context-extension.ts");
const FIXED_GOAL_RPC_CONFIG_PATH = resolve(import.meta.dir, "goal-rpc.yml");

const GOAL_ERROR_MESSAGES: Record<WorkerGoalErrorCode, string> = {
	unsupported: "当前 OMP 不支持原生目标 RPC",
	disabled: "OMP 原生目标功能未启用",
	"already-exists": "当前会话已有原生目标，请先删除",
	"already-active": "当前原生目标正在执行，拒绝重复恢复",
	"identity-mismatch": "原生目标 ID 与已接受目标不一致，已停止恢复",
	"invalid-request": "原生目标请求无效",
	"invalid-response": "OMP 原生目标 RPC 返回了无效结果",
	"continuation-disabled": "当前 worker 未启用原生目标自动续跑",
	"native-error": "OMP 原生目标操作失败",
};

function workerGoalError(code: WorkerGoalErrorCode, message?: string): WorkerGoalError {
	return new WorkerGoalError(code, message ?? GOAL_ERROR_MESSAGES[code]);
}

function asGoalRpcError(error: unknown): WorkerGoalError {
	if (error instanceof WorkerGoalError) return error;
	const message = asError(error, "native goal RPC failed").message.toLowerCase();
	if (/unknown (?:rpc )?command|unsupported.*goal|goal.*unsupported/u.test(message)) {
		return workerGoalError("unsupported");
	}
	if (/disabled|not enabled|goal\.enabled/u.test(message)) return workerGoalError("disabled");
	if (/already.*goal|goal.*already|active or paused/u.test(message)) {
		return workerGoalError("already-exists");
	}
	return workerGoalError("native-error");
}

const WORKER_GOAL_STATUSES: Record<WorkerGoalStatus, true> = {
	active: true,
	paused: true,
	"budget-limited": true,
	complete: true,
	dropped: true,
};

function parseWorkerGoal(value: unknown, source: string): WorkerGoal | null {
	if (value === null) return null;
	if (
		!isRecord(value) ||
		typeof value.id !== "string" ||
		value.id.length === 0 ||
		typeof value.objective !== "string" ||
		typeof value.status !== "string" ||
		WORKER_GOAL_STATUSES[value.status as WorkerGoalStatus] !== true ||
		typeof value.tokensUsed !== "number" ||
		!Number.isSafeInteger(value.tokensUsed) ||
		value.tokensUsed < 0 ||
		typeof value.timeUsedSeconds !== "number" ||
		!Number.isFinite(value.timeUsedSeconds) ||
		value.timeUsedSeconds < 0 ||
		typeof value.createdAt !== "number" ||
		!Number.isFinite(value.createdAt) ||
		typeof value.updatedAt !== "number" ||
		!Number.isFinite(value.updatedAt) ||
		(value.tokenBudget !== undefined &&
			(typeof value.tokenBudget !== "number" || !Number.isSafeInteger(value.tokenBudget) || value.tokenBudget <= 0))
	) {
		throw workerGoalError("invalid-response", `${source} returned an invalid native goal`);
	}
	const goal: WorkerGoal = {
		id: value.id,
		objective: value.objective,
		status: value.status as WorkerGoalStatus,
		tokensUsed: value.tokensUsed,
		timeUsedSeconds: value.timeUsedSeconds,
	};
	if (typeof value.tokenBudget === "number") goal.tokenBudget = value.tokenBudget;
	return goal;
}

function parseWorkerGoalResult(value: unknown, source: string): WorkerGoalResult {
	if (!isRecord(value) || !("goal" in value) || !("state" in value)) {
		throw workerGoalError("invalid-response", `${source} returned an invalid native goal response`);
	}
	const goal = parseWorkerGoal(value.goal, source);
	if (value.state === null) {
		if (goal !== null) throw workerGoalError("invalid-response", `${source} returned a goal without native goal state`);
		return { goal: null, state: null };
	}
	const state = value.state;
	if (
		!isRecord(state) ||
		typeof state.enabled !== "boolean" ||
		(state.mode !== "active" && state.mode !== "exiting") ||
		!("goal" in state)
	) {
		throw workerGoalError("invalid-response", `${source} returned an invalid native goal state`);
	}
	const stateGoal = parseWorkerGoal(state.goal, source);
	if (goal === null || stateGoal === null || goal.id !== stateGoal.id) {
		throw workerGoalError("invalid-response", `${source} returned inconsistent native goal state`);
	}
	return {
		goal,
		state: { enabled: state.enabled, mode: state.mode, goal: stateGoal },
	};
}

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
	/**
	 * Test seam: replace the watchdog child spawn. Production never passes this; a task must only
	 * ever run behind the real spawn discipline (same executable, same detached group).
	 */
	spawnWatchdog?: () => RpcChild;
};

/**
 * How long a freshly spawned OMP child may take to become ready. A cold start on a loaded machine
 * (memory pressure, a concurrent Bun process spawned by another LaunchAgent) can take tens of
 * seconds while the child makes progress; a smaller budget kills it and fails the task even though
 * nothing about the workspace, session or lock is wrong.
 */
const DEFAULT_STARTUP_TIMEOUT_MS = 30_000;
const DEFAULT_CANCEL_TIMEOUT_MS = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const CONTEXT_USAGE_TIMEOUT_MS = 2_000;
const PASSIVE_UI_METHODS: Record<string, true> = {
	setStatus: true,
	setWidget: true,
	setTitle: true,
	set_editor_text: true,
	open_url: true,
};

/** The thinking level a `get_state` or `config_update` frame reports, or null when it carries none. */
function modelLevel(frame: unknown): string | null {
	if (!isRecord(frame)) return null;
	const level = frame.thinkingLevel;
	return typeof level === "string" && level.length > 0 ? level : null;
}

function startupAbortError(signal: AbortSignal): Error {
	return signal.reason instanceof Error ? signal.reason : new Error("OMP worker startup aborted");
}

type ActiveBtw = {
	question: string;
	requestId: string | undefined;
	recordId: string | undefined;
	responseSettled: boolean;
	responseSucceeded: boolean;
	responseError: Error | undefined;
	terminal: boolean;
	terminalAnswer: string | undefined;
	terminalError: Error | undefined;
	cancelRequestSent: boolean;
	cancelResponseSettled: boolean;
	cancelError: Error | undefined;
	abortError: Error | undefined;
	settled: boolean;
	resolve: (answer: string) => void;
	reject: (error: Error) => void;
	abortListener: (() => void) | undefined;
};

class RpcWorker implements OmpWorker {
	#child: RpcChild;
	#watchdog: RpcChild | undefined;
	#pgid: number;
	#startupTimeoutMs: number;
	#cancelTimeoutMs: number;
	#newSessionOnStart: boolean;
	#goalContinuation: boolean;
	/** Invalidates goal starts that have not crossed their structured RPC dispatch boundary. */
	#goalControlGeneration = 0;
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
	#commandOutputCounter = 0;
	/**
	 * True while the internal `/retry` dispatch of `resume` runs: its native command-output
	 * receipt is control traffic and never becomes a worker notice.
	 */
	#suppressCommandOutput = false;
	#pending = new Map<string, PendingResponse>();
	#activePrompt: PromptRun | undefined;
	#activeBtw: ActiveBtw | undefined;
	/** True after prompt acceptance or during an agent turn; false between turns and at settlement. */
	#streaming = false;
	#activeTools = new Map<string, HostTool>();
	#hostCalls = new Map<string, HostCall>();
	#pendingSteerReceipts: Array<{ text: string }> = [];
	#sessionFile = "";
	#sessionId = "";
	/**
	 * The session model OMP last announced through `config_update` — the frame its `/switch`
	 * builtin emits after a successful switch, carrying the resolved model and thinking level.
	 */
	#modelUpdate: { model: string; level: string | null; seq: number } | undefined;
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
		goalContinuation: boolean,
	) {
		this.#child = child;
		this.#watchdog = watchdog;
		this.#pgid = pgid;
		this.#startupTimeoutMs = startupTimeoutMs;
		this.#cancelTimeoutMs = cancelTimeoutMs;
		this.#newSessionOnStart = newSessionOnStart;
		this.#goalContinuation = goalContinuation;
		const ready = Promise.withResolvers<void>();
		this.#readyPromise = ready.promise;
		this.#readyResolve = ready.resolve;
		this.#readyReject = ready.reject;
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

	async initialize(signal?: AbortSignal): Promise<ProjectSessionState> {
		if (!signal) return this.#initializeCore();
		if (signal.aborted) {
			this.#readyPromise.catch(() => {});
			await this.stop();
			throw startupAbortError(signal);
		}

		const aborted = Promise.withResolvers<never>();
		const onAbort = (): void => {
			aborted.reject(startupAbortError(signal));
			void this.stop().catch(() => false);
		};
		signal.addEventListener("abort", onAbort, { once: true });
		const initialization = this.#initializeCore();
		try {
			const state = await Promise.race([initialization, aborted.promise]);
			if (signal.aborted) throw startupAbortError(signal);
			return state;
		} catch (error) {
			if (signal.aborted) {
				const stopped = await this.stop();
				if (stopped) await initialization.catch(() => {});
				throw startupAbortError(signal);
			}
			throw error;
		} finally {
			signal.removeEventListener("abort", onAbort);
		}
	}

	async #initializeCore(): Promise<ProjectSessionState> {
		try {
			await this.#waitWithTimeout(this.#readyPromise, this.#startupTimeoutMs, "OMP worker did not become ready");
		} catch (error) {
			// The ready promise may already carry a typed reason (the child exited, or its output
			// ended); only a plain timeout is the "never became ready" case.
			throw taskStartupError("not-ready", error);
		}
		try {
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
		} catch (error) {
			throw taskStartupError("handshake-failed", error);
		}
		try {
			return await this.refreshState();
		} catch (error) {
			throw taskStartupError("session-invalid", error);
		}
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
	/**
	 * Return OMP's current in-memory context estimate for this worker. This is one read-only
	 * `get_state` request; it never starts or steers an agent turn.
	 */
	async contextUsage(signal?: AbortSignal): Promise<WorkerContextUsage> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const stateResponse = await this.#request({ type: "get_state" }, CONTEXT_USAGE_TIMEOUT_MS, undefined, signal);
		this.#assertSuccess(stateResponse, "get_state");
		const state = stateResponse.data;
		if (
			!isRecord(state) ||
			typeof state.sessionId !== "string" ||
			state.sessionId.length === 0 ||
			typeof state.sessionFile !== "string" ||
			state.sessionFile.length === 0
		) {
			throw new Error("OMP RPC get_state did not return a session file and session ID");
		}
		const rawUsage = isRecord(state.contextUsage) ? state.contextUsage : undefined;
		const tokens = typeof rawUsage?.tokens === "number" && Number.isFinite(rawUsage.tokens) && rawUsage.tokens >= 0
			? rawUsage.tokens
			: null;
		const modelState = isRecord(state.model) ? state.model : undefined;
		const contextWindow =
			typeof rawUsage?.contextWindow === "number" && Number.isFinite(rawUsage.contextWindow) && rawUsage.contextWindow > 0
				? rawUsage.contextWindow
				: typeof modelState?.contextWindow === "number" && Number.isFinite(modelState.contextWindow) && modelState.contextWindow > 0
					? modelState.contextWindow
					: null;
		const percent =
			typeof rawUsage?.percent === "number" && Number.isFinite(rawUsage.percent) && rawUsage.percent >= 0
				? rawUsage.percent
				: null;
		const level = modelLevel(state);
		const model =
			modelState &&
			typeof modelState.provider === "string" &&
			modelState.provider.length > 0 &&
			typeof modelState.id === "string" &&
			modelState.id.length > 0
				? `${modelState.provider}/${modelState.id}${level === null ? "" : `:${level}`}`
				: null;
		return {
			sessionId: state.sessionId,
			sessionFile: state.sessionFile,
			tokens,
			contextWindow,
			percent,
			model,
		};
	}


	/**
	 * The live session's model plus everything OMP currently offers. No prompt is involved, so a
	 * freshly started worker answers with the model a fresh session would use — the default the
	 * `/switch` command reports for a chat that never set one.
	 */
	async models(): Promise<WorkerModelCatalog> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const stateResponse = await this.#request({ type: "get_state" }, this.#startupTimeoutMs);
		this.#assertSuccess(stateResponse, "get_state");
		const state = stateResponse.data;
		const model = isRecord(state) && isRecord(state.model) ? state.model : undefined;
		const current =
			model && typeof model.provider === "string" && typeof model.id === "string" && model.provider.length > 0 && model.id.length > 0
				? `${model.provider}/${model.id}`
				: null;
		const level = modelLevel(state);
		const listResponse = await this.#request({ type: "get_available_models" }, DEFAULT_REQUEST_TIMEOUT_MS);
		this.#assertSuccess(listResponse, "get_available_models");
		const data = listResponse.data;
		const raw = isRecord(data) && Array.isArray(data.models) ? data.models : [];
		const available: WorkerModelInfo[] = [];
		for (const entry of raw) {
			if (!isRecord(entry)) continue;
			const provider = entry.provider;
			const id = entry.id;
			if (typeof provider !== "string" || provider.length === 0 || typeof id !== "string" || id.length === 0) continue;
			available.push({
				provider,
				id,
				...(typeof entry.name === "string" && entry.name.length > 0 ? { name: entry.name } : {}),
			});
		}
		return { current, level, available };
	}

	/**
	 * Hand a selector to OMP's own `/switch` builtin and report what it resolved to. The dispatch
	 * is a prompt frame that OMP consumes locally (`agentInvoked: false`) — it never reaches the
	 * model and is accepted while a turn is streaming, so a running session switches in place.
	 * Success is the `config_update` frame OMP emits after switching; without one the selector was
	 * rejected and the session is untouched.
	 */
	async switchModel(selector: string): Promise<WorkerModelSwitch> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const before = this.#modelUpdate?.seq ?? 0;
		const response = await this.#request({ type: "prompt", message: `/switch ${selector}` }, DEFAULT_REQUEST_TIMEOUT_MS);
		this.#assertSuccess(response, "prompt");
		const update = this.#modelUpdate;
		if (!update || update.seq === before) return { ok: false };
		return { ok: true, model: update.model, level: update.level };
	}

	/**
	 * Fold a `config_update` frame: OMP announces the session model here after `/switch` succeeds.
	 * Frames without a usable provider/id are ignored, so a malformed update cannot be mistaken
	 * for a switch.
	 */
	#handleConfigUpdate(frame: RpcFrame): void {
		const model = isRecord(frame.model) ? frame.model : undefined;
		const provider = typeof model?.provider === "string" ? model.provider : undefined;
		const id = typeof model?.id === "string" ? model.id : undefined;
		if (provider === undefined || id === undefined || provider.length === 0 || id.length === 0) return;
		this.#modelUpdate = { model: `${provider}/${id}`, level: modelLevel(frame), seq: (this.#modelUpdate?.seq ?? 0) + 1 };
	}

	async prompt(text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
		await this.#dispatchPrompt(text, onEvent, context, false);
	}

	btw(question: string, signal?: AbortSignal): Promise<string> {
		if (this.#closed || this.#stopping) return Promise.reject(new Error("OMP worker is not running"));
		if (this.#activeBtw) return Promise.reject(new Error("OMP worker already has a BTW question in progress"));
		if (signal?.aborted) {
			return Promise.reject(signal.reason instanceof Error ? signal.reason : new Error("OMP BTW was aborted"));
		}

		const completion = Promise.withResolvers<string>();
		const run: ActiveBtw = {
			question,
			requestId: undefined,
			recordId: undefined,
			responseSettled: false,
			responseSucceeded: false,
			responseError: undefined,
			terminal: false,
			terminalAnswer: undefined,
			terminalError: undefined,
			cancelRequestSent: false,
			cancelResponseSettled: false,
			cancelError: undefined,
			abortError: undefined,
			settled: false,
			resolve: completion.resolve,
			reject: completion.reject,
			abortListener: undefined,
		};
		this.#activeBtw = run;
		if (signal) {
			run.abortListener = () =>
				this.#abortBtw(
					run,
					signal.reason instanceof Error ? signal.reason : new Error("OMP BTW was aborted"),
				);
			signal.addEventListener("abort", run.abortListener, { once: true });
			if (signal.aborted) run.abortListener();
		}

		void this.#request(
			{ type: "btw", question },
			DEFAULT_REQUEST_TIMEOUT_MS,
			requestId => {
				run.requestId = requestId;
			},
		)
			.then(
				response => {
					if (!run.responseSettled) this.#handleBtwStartResponse(run, response);
				},
				error => this.#handleBtwStartFailure(run, asError(error, "OMP BTW request failed")),
			)
			.catch(error => this.#failBtw(run, asError(error, "OMP BTW request failed")));

		return completion.promise.finally(() => {
			if (signal && run.abortListener) signal.removeEventListener("abort", run.abortListener);
		});
	}

	/**
	 * Resume the bound session's interrupted turn after a worker restart by dispatching OMP's own
	 * `/retry`, which continues from the recorded transcript instead of replaying the original
	 * request. The dispatch's native command-output receipt and a local-only completion are
	 * control traffic, never assistant output. When OMP answers `/retry` locally — there is no
	 * resumable failed turn — one same-session continuation instruction continues the recorded
	 * work. A real failure rejects and nothing is sent after it.
	 */
	async resume(onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
		let localOnly = false;
		await this.#dispatchPrompt(
			"/retry",
			event => {
				// The retry dispatch's local completion belongs to the internal control flow: it
				// must neither mark the resumed task local-only nor surface as an assistant event.
				if (event.type === "local_command") {
					localOnly = true;
					return;
				}
				onEvent(event);
			},
			context,
			true,
		);
		if (!localOnly) return;
		await this.#dispatchPrompt(RESUME_CONTINUATION_PROMPT, onEvent, context, false);
	}


	async goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> {
		if (op !== "get") this.#goalControlGeneration++;
		if (this.#closed) throw new Error("OMP worker is not running");
		try {
			const response = await this.#request({ type: "goal", op }, DEFAULT_REQUEST_TIMEOUT_MS);
			this.#assertSuccess(response, `goal ${op}`);
			return parseWorkerGoalResult(response.data, `OMP goal ${op}`);
		} catch (error) {
			throw asGoalRpcError(error);
		}
	}

	async runGoal(
		request: WorkerGoalRun,
		onEvent: (event: WorkerEvent) => void,
		context?: PromptContext,
	): Promise<WorkerGoalResult> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (!this.#goalContinuation) throw workerGoalError("continuation-disabled");
		if (this.#activePrompt) throw new Error("OMP worker already has a run in progress");
		if (!request || typeof request !== "object") throw workerGoalError("invalid-request");
		const controlGeneration = this.#goalControlGeneration;

		if (request.op === "create") {
			if (typeof request.objective !== "string" || request.objective.trim().length === 0) {
				throw workerGoalError("invalid-request");
			}
			if (
				request.tokenBudget !== undefined &&
				(!Number.isSafeInteger(request.tokenBudget) || request.tokenBudget <= 0)
			) {
				throw workerGoalError("invalid-request");
			}
			const current = await this.goal("get");
			if (controlGeneration !== this.#goalControlGeneration) return this.#goalAfterControl();
			if (current.goal) throw workerGoalError("already-exists");
			return this.#startNativeGoal("create", onEvent, context, request, undefined, controlGeneration);
		}

		if (request.op === "resume") {
			const current = await this.goal("get");
			if (controlGeneration !== this.#goalControlGeneration) return this.#goalAfterControl();
			if (
				!current.goal ||
				current.goal.status === "complete" ||
				current.goal.status === "dropped" ||
				current.goal.status === "budget-limited"
			) {
				return current;
			}
			if (current.goal.status !== "paused") throw workerGoalError("already-active");
			return this.#startNativeGoal("resume", onEvent, context, undefined, current.goal.id, controlGeneration);
		}

		if (request.op === "recover") return this.#recoverGoal(request.goalId, onEvent, context, controlGeneration);
		throw workerGoalError("invalid-request");
	}

	async #goalAfterControl(expectedGoalId?: string): Promise<WorkerGoalResult> {
		while (true) {
			const generation = this.#goalControlGeneration;
			const current = await this.goal("get");
			if (expectedGoalId !== undefined && current.goal && current.goal.id !== expectedGoalId) {
				throw workerGoalError("identity-mismatch");
			}
			if (generation === this.#goalControlGeneration) return current;
		}
	}

	async #recoverGoal(
		goalId: string,
		onEvent: (event: WorkerEvent) => void,
		context: PromptContext | undefined,
		controlGeneration: number,
	): Promise<WorkerGoalResult> {
		if (typeof goalId !== "string" || goalId.length === 0) throw workerGoalError("invalid-request");
		let current = await this.goal("get");
		if (current.goal && current.goal.id !== goalId) throw workerGoalError("identity-mismatch");
		if (controlGeneration !== this.#goalControlGeneration) return this.#goalAfterControl(goalId);
		if (
			!current.goal ||
			current.goal.status === "complete" ||
			current.goal.status === "dropped" ||
			current.goal.status === "budget-limited"
		) {
			return current;
		}

		// Retry only the failed native turn. Unlike ordinary resume, an empty retry never sends a
		// generic continuation prompt; the structured goal resume below is the only continuation.
		await this.#dispatchPrompt(
			"/retry",
			event => {
				if (event.type === "prompt_accepted" || event.type === "local_command") return;
				onEvent(event);
			},
			context,
			true,
		);

		current = await this.goal("get");
		if (current.goal && current.goal.id !== goalId) throw workerGoalError("identity-mismatch");
		if (controlGeneration !== this.#goalControlGeneration) return this.#goalAfterControl(goalId);
		if (
			!current.goal ||
			current.goal.status === "complete" ||
			current.goal.status === "dropped" ||
			current.goal.status === "budget-limited"
		) {
			return current;
		}
		return this.#startNativeGoal("resume", onEvent, context, undefined, goalId, controlGeneration);
	}


	async #startNativeGoal(
		op: "create" | "resume",
		onEvent: (event: WorkerEvent) => void,
		context: PromptContext | undefined,
		createRequest: Extract<WorkerGoalRun, { op: "create" }> | undefined,
		expectedGoalId: string | undefined,
		controlGeneration: number,
	): Promise<WorkerGoalResult> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (this.#activePrompt) throw new Error("OMP worker already has a run in progress");
		const run = this.#createPromptRun(onEvent, true);
		this.#activePrompt = run;
		this.#suppressCommandOutput = false;
		let acceptedGoalId: string | undefined;
		try {
			await this.#installHostTools(context?.tools ?? []);
			if (controlGeneration !== this.#goalControlGeneration) {
				this.#settlePrompt(run);
				return await this.#goalAfterControl(expectedGoalId);
			}
			const command: RpcCommand =
				op === "create"
					? {
							type: "goal",
							op,
							objective: createRequest!.objective,
							...(createRequest!.tokenBudget === undefined ? {} : { token_budget: createRequest!.tokenBudget }),
						}
					: { type: "goal", op };
			run.goalDispatchIssued = true;
			const response = await this.#request(command, DEFAULT_REQUEST_TIMEOUT_MS, requestId => {
				run.requestId = requestId;
			});
			try {
				this.#assertSuccess(response, `goal ${op}`);
			} catch (error) {
				throw asGoalRpcError(error);
			}
			run.goalRpcSucceeded = true;
			const result = parseWorkerGoalResult(response.data, `OMP goal ${op}`);
			run.goalResponseReceived = true;
			if (op === "create" && !result.goal) {
				throw workerGoalError("invalid-response", "OMP goal create succeeded without creating a native goal");
			}
			if (!result.goal) {
				this.#settlePrompt(run);
				return result;
			}
			if (expectedGoalId !== undefined && result.goal.id !== expectedGoalId) {
				throw workerGoalError("identity-mismatch");
			}
			acceptedGoalId = result.goal.id;
			try {
				onEvent({ type: "goal_accepted", goalId: acceptedGoalId });
			} catch (error) {
				throw asError(error, "OMP goal acceptance callback failed");
			}
			if (run.settled) await run.completion;
			if (!run.settled && (run.sessionSettledSeen || !run.agentStarted) && (await this.#sessionIsSettled())) {
				this.#settlePrompt(run);
			}
			await run.completion;

			let final = await this.goal("get");
			if (final.goal && final.goal.id !== acceptedGoalId) throw workerGoalError("identity-mismatch");
			if (final.goal?.status === "active") {
				const paused = await this.goal("pause");
				if (paused.goal && paused.goal.id !== acceptedGoalId) throw workerGoalError("identity-mismatch");
				final = await this.goal("get");
				if (final.goal && final.goal.id !== acceptedGoalId) throw workerGoalError("identity-mismatch");
				if (final.goal?.status === "active") {
					throw workerGoalError("native-error", "OMP 未完成目标仍处于 active，native pause 未生效");
				}
			}
			return final;
		} catch (error) {
			const failure = asError(error, `OMP goal ${op} failed`);
			this.#rejectPrompt(run, failure);
			const goalToPause = acceptedGoalId ?? (op === "resume" ? expectedGoalId : run.goalUpdatedId);
			if (goalToPause !== undefined) {
				await this.#pauseCurrentGoal(goalToPause);
			} else if (op === "create" && run.goalRpcSucceeded) {
				await this.#pauseCurrentGoal();
			}
			throw failure;
		} finally {
			this.#abortHostCalls(new Error("goal run finished"));
			await this.#clearHostTools();
			if (this.#activePrompt === run) this.#activePrompt = undefined;
			this.#streaming = false;
			this.#suppressCommandOutput = false;
		}
	}

	async #sessionIsSettled(): Promise<boolean> {
		const response = await this.#request({ type: "get_state" }, this.#startupTimeoutMs);
		this.#assertSuccess(response, "get_state");
		if (!isRecord(response.data) || typeof response.data.isSettled !== "boolean") {
			throw workerGoalError("invalid-response", "OMP RPC get_state did not report session settlement");
		}
		return response.data.isSettled;
	}
	async #confirmGoalSettlement(run: PromptRun): Promise<void> {
		try {
			if (await this.#sessionIsSettled() && this.#activePrompt === run && !run.settled) {
				this.#settlePrompt(run);
			}
		} catch (error) {
			if (this.#activePrompt === run && !run.settled) {
				this.#rejectPrompt(run, asError(error, "OMP goal session settlement could not be confirmed"));
			}
		}
	}

	async #pauseCurrentGoal(expectedGoalId?: string): Promise<void> {
		try {
			const current = await this.goal("get");
			if (
				current.goal?.status === "active" &&
				(expectedGoalId === undefined || current.goal.id === expectedGoalId)
			) {
				await this.goal("pause");
			}
		} catch {
			// The caller already has the operation failure; this best-effort pause only prevents an
			// unaccepted or failed run from silently scheduling more native work.
		}
	}

	#createPromptRun(
		onEvent: (event: WorkerEvent) => void,
		goalRun: boolean,
		manualCompact = false,
	): PromptRun {
		const completion = Promise.withResolvers<void>();
		void completion.promise.catch(() => {});
		return {
			requestId: "",
			onEvent,
			resolve: () => completion.resolve(),
			reject: error => completion.reject(error),
			completion: completion.promise,
			settled: false,
			goalRun,
			waitForSessionSettled: goalRun,
			goalDispatchIssued: false,
			goalRpcSucceeded: false,
			goalResponseReceived: false,
			sessionSettledSeen: false,
			goalUpdatedId: undefined,
			agentStarted: false,
			agentInvoked: false,
			accepted: false,
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
			research: new ResearchTracker(),
			manualCompact,
			manualCompactStarted: false,
			manualCompactEnded: false,
			manualCompactCommandOutputSeen: false,
			manualCompactAbortAcknowledged: false,
			localCompletionRequested: false,
		};
	}
	async #dispatchPrompt(
		text: string,
		onEvent: (event: WorkerEvent) => void,
		context: PromptContext | undefined,
		internalCommandOutput: boolean,
	): Promise<void> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (this.#activePrompt) throw new Error("OMP worker already has a prompt in progress");
		if (typeof text !== "string") throw new Error("OMP prompt text must be a string");
		const firstLineEnd = text.search(/[\r\n]/u);
		const promptFirstLine = (firstLineEnd < 0 ? text : text.slice(0, firstLineEnd)).trimStart();
		const run = this.#createPromptRun(
			onEvent,
			false,
			/^\/compact(?:[\s:]|$)/iu.test(promptFirstLine),
		);
		this.#activePrompt = run;
		this.#suppressCommandOutput = internalCommandOutput;
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
			// The acknowledgement's agent-turn verdict was recorded in line with its frame
			// (`#notePromptAck`); only an explicit local-only completion settles here.
			if (isRecord(response.data) && response.data.agentInvoked === false) this.#completeLocalCommand(run);
			await run.completion;
		} catch (error) {
			this.#rejectPrompt(run, asError(error, "OMP prompt failed"));
			throw error;
		} finally {
			this.#abortHostCalls(new Error("prompt finished"));
			await this.#clearHostTools();
			if (this.#activePrompt === run) this.#activePrompt = undefined;
			this.#streaming = false;
			this.#suppressCommandOutput = false;
		}
	}

	/**
	 * Steering is only meaningful while a turn streams. A modern terminal `agent_end` can leave the
	 * session unsettled for background or goal work; keep that run open but close the turn gate.
	 */
	async steer(text: string): Promise<void> {
		if (this.#closed) throw new WorkerSteerRejectedError("OMP worker is not running");
		if (!this.#activePrompt || !this.#streaming) {
			throw new WorkerSteerRejectedError("OMP worker is not streaming a turn");
		}
		if (typeof text !== "string" || text.trim().length === 0) {
			throw new WorkerSteerRejectedError("OMP steering text must be a non-empty string");
		}
		const receipt = { text };
		this.#pendingSteerReceipts.push(receipt);
		try {
			const response = await this.#request({ type: "steer", message: text }, DEFAULT_REQUEST_TIMEOUT_MS);
			if (response.success === false) {
				throw new WorkerSteerRejectedError("OMP explicitly rejected steering");
			}
			this.#assertSuccess(response, "steer");
		} catch (error) {
			const index = this.#pendingSteerReceipts.indexOf(receipt);
			if (index >= 0) this.#pendingSteerReceipts.splice(index, 1);
			throw error;
		}
	}

	async abortTurn(): Promise<void> {
		this.#goalControlGeneration++;
		if (this.#closed) throw new Error("OMP worker is not running");
		const response = await this.#request({ type: "abort" }, Math.min(1_000, this.#cancelTimeoutMs));
		this.#assertSuccess(response, "abort");
		const active = this.#activePrompt;
		if (
			active?.manualCompact
			&& !active.manualCompactStarted
			&& !active.manualCompactEnded
			&& !active.settled
		) {
			active.manualCompactAbortAcknowledged = true;
			if (active.localCompletionRequested) this.#completeLocalCommand(active);
		}
	}

	async stop(): Promise<boolean> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#goalControlGeneration++;
		this.#stopPromise = this.#stopInternal();
		return this.#stopPromise;
	}


	async #stopInternal(): Promise<boolean> {
		this.#stopping = true;
		if (this.#activeBtw) this.#failBtw(this.#activeBtw, new Error("OMP worker stopped"));
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

	async #request(
		command: RpcCommand,
		timeoutMs: number,
		onId?: (id: string) => void,
		signal?: AbortSignal,
	): Promise<RpcFrame> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (signal?.aborted) {
			throw signal.reason instanceof Error ? signal.reason : new Error(`OMP RPC ${command.type} aborted`);
		}
		const id = `omp-${++this.#requestCounter}`;
		onId?.(id);
		const frame = { ...command, id };
		return new Promise<RpcFrame>((resolve, reject) => {
			let onAbort: (() => void) | undefined;
			const cleanup = (): void => {
				if (onAbort) signal?.removeEventListener("abort", onAbort);
			};
			const settleResolve = (value: RpcFrame): void => {
				cleanup();
				resolve(value);
			};
			const settleReject = (error: Error): void => {
				cleanup();
				reject(error);
			};
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				settleReject(new Error(`Timed out waiting for OMP RPC ${command.type}`));
			}, timeoutMs);
			this.#pending.set(id, { resolve: settleResolve, reject: settleReject, timer });
			if (signal) {
				onAbort = () => {
					if (!this.#pending.delete(id)) return;
					clearTimeout(timer);
					settleReject(signal.reason instanceof Error ? signal.reason : new Error(`OMP RPC ${command.type} aborted`));
				};
				signal.addEventListener("abort", onAbort, { once: true });
				if (signal.aborted) {
					onAbort();
					return;
				}
			}
			try {
				this.#writeFrame(frame);
			} catch (error) {
				clearTimeout(timer);
				this.#pending.delete(id);
				settleReject(asError(error, `Could not send OMP RPC ${command.type}`));
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
			if (buffered.trim().length > 0) throw new TaskStartupError("exited", "OMP RPC output ended with an unterminated frame");
			if (!this.#ready) this.#readyReject(new TaskStartupError("exited", "OMP output ended before ready"));
			else if (!this.#stopping) this.#onFatal(new Error("OMP output ended unexpectedly"));
		} catch (error) {
			if (!this.#ready) this.#readyReject(taskStartupError("handshake-failed", error));
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
		if (frame.type === "btw_record") {
			this.#handleBtwRecord(frame);
			return;
		}
		if (frame.type === "btw_delta") return;
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
		if (frame.type === "config_update") {
			this.#handleConfigUpdate(frame);
			return;
		}
		if (frame.type === "command_output") {
			const active = this.#activePrompt;
			if (active?.manualCompact) {
				// Native compact output can include provider-controlled error details.
				// It has no request id, so suppress raw output for every compact builtin.
				if (
					!active.settled
					&& !active.manualCompactCommandOutputSeen
					&& !active.manualCompactStarted
					&& !active.manualCompactEnded
					&& typeof frame.text === "string"
					&& (
						(frame.text.startsWith("/compact ") && frame.text.includes(" does not take focus instructions"))
						|| frame.text.startsWith("Compaction failed:")
						|| frame.text.startsWith("Compaction complete.")
					)
				) {
					active.manualCompactCommandOutputSeen = true;
					if (active.localCompletionRequested) this.#completeLocalCommand(active);
				}
				return;
			}
			// Native builtins output before acknowledging their prompt. Frames have no request
			// id, so only collect during this prompt's dispatch, not a mid-turn /switch probe.
			// `resume`'s internal `/retry` dispatch suppresses the receipt: it is control
			// traffic, and the chat must not receive a restart disruption notice.
			if (active && this.#pending.has(active.requestId) && !this.#suppressCommandOutput) {
				this.#handleNotice({
					id: `command:${active.requestId}:${++this.#commandOutputCounter}`,
					message: frame.text,
				});
			}
			return;
		}
		const active = this.#activePrompt;
		if (frame.type === "goal_updated") {
			if (active && !active.settled) this.#handleGoalUpdated(active, frame);
			return;
		}
		if (frame.type === "manual_compaction_start" || frame.type === "manual_compaction_end") {
			if (!active || active.settled || !active.manualCompact) return;
			if (typeof frame.sessionId === "string" && frame.sessionId !== this.#sessionId) return;
			if (frame.type === "manual_compaction_start") {
				if (active.manualCompactStarted) return;
				active.manualCompactStarted = true;
				active.manualCompactAbortAcknowledged = false;
			} else if (active.manualCompactStarted && !active.manualCompactEnded) {
				active.manualCompactEnded = true;
				if (active.localCompletionRequested) this.#completeLocalCommand(active);
			}
			return;
		}
		if (frame.type === "auto_compaction_start" || frame.type === "auto_compaction_end") return;
		if (!active || active.settled) return;
		if (frame.type === "session_settled") {
			if (!active.waitForSessionSettled) return;
			if (!active.goalRun) {
				this.#settlePrompt(active);
				return;
			}
			if (!active.goalDispatchIssued) return;
			if (!active.goalResponseReceived) {
				active.sessionSettledSeen = true;
				return;
			}
			void this.#confirmGoalSettlement(active);
			return;
		}
		const modelError = modelErrorFromFrame(frame);
		if (modelError) {
			this.#rejectPrompt(active, modelError);
			return;
		}
		if (frame.type === "agent_end") {
			// A native compact command completes at its matching lifecycle end, not
			// through an unrelated trailing agent_end frame.
			if (active.manualCompact && !active.agentInvoked) return;
			if (typeof frame.isTerminal === "boolean" || typeof frame.yielded === "boolean") {
				active.waitForSessionSettled = true;
			}
			this.#streaming = false;
			if (!active.goalRun && !active.waitForSessionSettled && frame.willContinue !== true) {
				this.#settlePrompt(active);
			}
			return;
		}
		if (frame.type === "prompt_result") {
			if (active.goalRun) return;
			if (frame.id !== undefined && frame.id !== active.requestId) return;
			if (frame.agentInvoked === true) {
				if (frame.id !== active.requestId) return;
				active.waitForSessionSettled = true;
				if (frame.sessionSettled === true) this.#settlePrompt(active);
				return;
			}
			if (frame.agentInvoked === false) {
				// A run OMP acknowledged as an agent turn ignores stale local completion frames.
				if (!active.agentInvoked) this.#completeLocalCommand(active);
			}
			return;
		}
		if (frame.type === "agent_start") {
			active.agentStarted = true;
			this.#streaming = true;
			if (!this.#acceptPrompt(active)) return;
		}
		this.#emitWorkerEvent(active, frame);
	}

	#handleBtwStartResponse(run: ActiveBtw, frame: RpcFrame): void {
		if (this.#activeBtw !== run || run.responseSettled) return;
		run.responseSettled = true;
		if (frame.command !== "btw") {
			run.responseError = new Error("OMP BTW response acknowledged a different command");
			run.terminal = true;
			run.terminalError = run.responseError;
			this.#maybeSettleBtw(run);
			this.#releaseBtw(run);
			return;
		}
		if (frame.success !== true) {
			run.responseError = errorFromResponse(frame, "btw");
			run.terminal = true;
			run.terminalError = run.responseError;
			this.#maybeSettleBtw(run);
			this.#releaseBtw(run);
			return;
		}
		const data = frame.data;
		const record = isRecord(data) && isRecord(data.record) ? data.record : undefined;
		if (!record || typeof record.id !== "string" || record.id.length === 0) {
			run.responseError = new Error("OMP BTW response did not contain a record ID");
			run.terminal = true;
			run.terminalError = run.responseError;
			this.#maybeSettleBtw(run);
			this.#releaseBtw(run);
			return;
		}
		if (run.recordId !== undefined && run.recordId !== record.id) {
			run.responseError = new Error("OMP BTW record ID changed during startup");
			run.terminal = true;
			run.terminalError = run.responseError;
			this.#maybeSettleBtw(run);
			this.#releaseBtw(run);
			return;
		}
		run.recordId = record.id;
		run.responseSucceeded = true;
		if (run.abortError) this.#sendBtwCancel(run);
		this.#maybeSettleBtw(run);
		this.#releaseBtw(run);
	}

	#handleBtwStartFailure(run: ActiveBtw, error: Error): void {
		if (this.#activeBtw !== run || run.responseSettled) return;
		run.responseSettled = true;
		run.responseError = error;
		run.terminal = true;
		run.terminalError = error;
		this.#maybeSettleBtw(run);
		this.#releaseBtw(run);
	}

	#handleBtwRecord(frame: RpcFrame): void {
		const run = this.#activeBtw;
		if (!run || !isRecord(frame.record)) return;
		const record = frame.record;
		if (typeof record.id !== "string" || record.id.length === 0) return;
		if (
			run.recordId === undefined &&
			record.status === "running" &&
			record.question === run.question
		) {
			run.recordId = record.id;
			if (run.abortError) this.#sendBtwCancel(run);
		}
		if (record.id !== run.recordId || run.terminal || record.status === "running") return;
		run.terminal = true;
		if (record.status === "complete") {
			if (typeof record.answer === "string") run.terminalAnswer = record.answer;
			else run.terminalError = new Error("OMP BTW completion did not contain an answer");
		} else if (
			record.status === "cancelled" ||
			record.status === "error" ||
			record.status === "interrupted"
		) {
			run.terminalError =
				typeof record.error === "string" && record.error.length > 0
					? new Error(record.error)
					: new Error(`OMP BTW ended with status ${record.status}`);
		} else {
			run.terminalError = new Error("OMP BTW returned an invalid terminal record");
		}
		this.#maybeSettleBtw(run);
		this.#releaseBtw(run);
	}

	#abortBtw(run: ActiveBtw, error: Error): void {
		if (this.#activeBtw !== run || run.terminal || run.abortError) return;
		run.abortError = error;
		if (run.recordId) this.#sendBtwCancel(run);
	}

	#sendBtwCancel(run: ActiveBtw): void {
		if (
			this.#activeBtw !== run ||
			run.cancelRequestSent ||
			!run.recordId ||
			run.terminal ||
			this.#closed ||
			this.#stopping
		) {
			return;
		}
		run.cancelRequestSent = true;
		void this.#request(
			{ type: "btw_cancel", recordId: run.recordId },
			Math.min(1_000, this.#cancelTimeoutMs),
		)
			.then(
				response => {
					if (this.#activeBtw !== run) return;
					run.cancelResponseSettled = true;
					if (response.success !== true) {
						run.cancelError = errorFromResponse(response, "btw_cancel");
					} else if (!isRecord(response.data) || typeof response.data.cancelled !== "boolean") {
						run.cancelError = new Error("OMP BTW cancellation returned an invalid response");
					}
					this.#maybeSettleBtw(run);
					this.#releaseBtw(run);
				},
				error => {
					if (this.#activeBtw !== run) return;
					run.cancelResponseSettled = true;
					run.cancelError = asError(error, "OMP BTW cancellation failed");
					if (!run.settled) {
						run.settled = true;
						run.reject(run.cancelError);
					}
					this.#releaseBtw(run);
				},
			)
			.catch(error => this.#failBtw(run, asError(error, "OMP BTW cancellation failed")));
	}

	#maybeSettleBtw(run: ActiveBtw): void {
		if (
			this.#activeBtw !== run ||
			run.settled ||
			!run.responseSettled ||
			!run.terminal ||
			(run.cancelRequestSent && !run.cancelResponseSettled)
		) {
			return;
		}
		const error =
			run.cancelError ??
			(!run.responseSucceeded ? run.responseError : undefined) ??
			run.abortError ??
			run.terminalError;
		if (error) {
			run.settled = true;
			run.reject(error);
			return;
		}
		if (typeof run.terminalAnswer !== "string") {
			run.settled = true;
			run.reject(new Error("OMP BTW completed without an answer"));
			return;
		}
		run.settled = true;
		run.resolve(run.terminalAnswer);
	}

	#releaseBtw(run: ActiveBtw): void {
		if (
			this.#activeBtw === run &&
			run.responseSettled &&
			run.terminal &&
			(!run.cancelRequestSent || run.cancelResponseSettled)
		) {
			this.#activeBtw = undefined;
		}
	}

	#failBtw(run: ActiveBtw, error: Error): void {
		if (this.#activeBtw !== run) return;
		if (!run.settled) {
			run.settled = true;
			run.reject(error);
		}
		this.#activeBtw = undefined;
	}

	#handleGoalUpdated(active: PromptRun, frame: RpcFrame): void {
		try {
			if (!("goal" in frame)) throw workerGoalError("invalid-response", "OMP goal_updated omitted its goal field");
			const goal = parseWorkerGoal(frame.goal, "OMP goal_updated");
			active.goalUpdatedId = goal?.id;
			active.onEvent({ type: "goal_updated", goal });
		} catch (error) {
			this.#rejectPrompt(active, asError(error, "OMP goal_updated event was invalid"));
		}
	}

	#handleResponse(frame: RpcFrame): void {
		const id = typeof frame.id === "string" ? frame.id : undefined;
		const activeBtw = this.#activeBtw;
		if (id && activeBtw && activeBtw.requestId === id) this.#handleBtwStartResponse(activeBtw, frame);
		if (id) {
			const pending = this.#pending.get(id);
			if (pending) {
				this.#pending.delete(id);
				clearTimeout(pending.timer);
				// The acknowledgement's verdict is recorded while its frame is on the wire: the
				// frames that follow it in the same stdout read must already see it.
				if (frame.success === true) this.#notePromptAck(frame, id);
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

	/**
	 * The acknowledgement's verdict, recorded in line with the frame instead of in the awaiting
	 * dispatch: the frames that follow it in the same stdout read must already see whether this
	 * run is an agent turn, so a replayed local completion can never settle a regenerating turn.
	 * Only an explicit `agentInvoked: true` is recorded here; an explicit local-only completion is
	 * settled by the dispatch that owns the request, and an acknowledgement without data leaves
	 * the run waiting for its real completion.
	 */
	#notePromptAck(frame: RpcFrame, id: string): void {
		const active = this.#activePrompt;
		if (!active || active.settled || active.requestId !== id) return;
		const data = frame.data;
		if (isRecord(data) && data.agentInvoked === true) active.agentInvoked = true;
	}

	#handleUiRequest(frame: RpcFrame): void {
		const method = typeof frame.method === "string" ? frame.method : "unknown";
		if (method === "notify") {
			this.#handleNotice(frame);
			return;
		}
		if (PASSIVE_UI_METHODS[method] === true) return;
		try {
			if (typeof frame.id === "string" && !this.#closed) this.#writeFrame({ type: "extension_ui_response", id: frame.id, cancelled: true });
		} catch {
			// The fatal path below still rejects the prompt and stops the group.
		}
		const active = this.#activePrompt;
		if (active) this.#rejectPrompt(active, describeUiFailure(method));
		void this.stop();
	}

	/**
	 * Upstream `notify` requests surface as notice events on the run they arrived in. Only an
	 * active, unsettled prompt receives them: startup and post-settlement broadcasts are
	 * ignored, while the HUD's duplicate re-broadcasts ride through for the service stream to
	 * dedup by message id. A callback failure rejects the prompt exactly as in
	 * #emitWorkerEvent — the error must never escape into the stdout handler.
	 */
	#handleNotice(frame: RpcFrame): void {
		const active = this.#activePrompt;
		if (!active || active.settled) return;
		try {
			const event = noticeEventFromFrame(frame);
			if (event) active.onEvent(event);
		} catch (error) {
			this.#rejectPrompt(active, asError(error, "OMP event callback failed"));
		}
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
		// OMP's own call id, independent of the RPC request id and reused by a retry that
		// re-executes the call; a frame without one leaves it undefined.
		const toolCallId = typeof frame.toolCallId === "string" && frame.toolCallId.length > 0 ? frame.toolCallId : undefined;
		void Promise.resolve()
			.then(() => tool.execute(args, controller.signal, toolCallId))
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
			if (frame.type === "tool_execution_end") {
				const toolCallId = typeof frame.toolCallId === "string" ? frame.toolCallId : "";
				const toolName = typeof frame.toolName === "string" ? frame.toolName : "";
				if (toolCallId.length > 0 && toolName.length > 0) {
					this.#activeTools.get(toolName)?.onResultAccepted?.(toolCallId);
				}
			}
			const message = frame.message;
			let projectedFrame = frame;
			if (
				frame.type === "message_start"
				&& isRecord(message)
				&& message.role === "user"
				&& message.steering === true
				&& message.attribution !== "agent"
			) {
				const receipt = this.#pendingSteerReceipts.shift();
				if (receipt) projectedFrame = { ...frame, message: { ...message, originalText: receipt.text } };
			}
			emitWorkerEvent(active, projectedFrame, this.#subagentFrames);
		} catch (error) {
			this.#rejectPrompt(active, asError(error, "OMP event callback failed"));
		}
	}

	/**
	 * The run's live turn boundary: OMP started generating (`agent_start`), so this prompt is a
	 * real turn rather than the early acknowledgement of a local-only command. Reported once,
	 * before any output of the turn, so the service can record the acceptance durably; a failing
	 * callback rejects the run exactly like a failing output callback and reports false, stopping
	 * further frames of the settled run.
	 */
	#acceptPrompt(run: PromptRun): boolean {
		if (run.accepted) return true;
		run.accepted = true;
		try {
			run.onEvent({ type: "prompt_accepted" });
			return true;
		} catch (error) {
			this.#rejectPrompt(run, asError(error, "OMP event callback failed"));
			return false;
		}
	}
	#settlePrompt(run: PromptRun): void {
		if (run.settled) return;
		run.settled = true;
		this.#streaming = false;
		run.resolve();
	}

	/**
	 * A run that finished without generation (agentInvoked:false) announces its local
	 * completion once, immediately before settling; a replayed completion on a settled run
	 * emits nothing. The callback failure path matches #emitWorkerEvent: reject the prompt
	 * instead of letting the error escape into the stdout handler.
	 */
	#completeLocalCommand(run: PromptRun): void {
		if (run.settled) return;
		if (run.manualCompact) {
			run.localCompletionRequested = true;
			// The native command returns locally while compaction continues in the background.
			this.#streaming = false;
			if (
				!run.manualCompactEnded
				&& !run.manualCompactCommandOutputSeen
				&& !run.manualCompactAbortAcknowledged
			) {
				return;
			}
		}
		try {
			run.onEvent({ type: "local_command" });
		} catch (error) {
			this.#rejectPrompt(run, asError(error, "OMP event callback failed"));
			return;
		}
		this.#settlePrompt(run);
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
		this.#onFatal(new TaskStartupError("exited", `OMP worker exited with code ${code}`));
	}

	#onFatal(error: Error): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#readyReject(error);
		this.#rejectPending(error);
		this.#abortHostCalls(error);
		if (this.#activePrompt) this.#rejectPrompt(this.#activePrompt, error);
		if (this.#activeBtw) this.#failBtw(this.#activeBtw, error);
		void this.stop();
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
		if (options.signal?.aborted) throw startupAbortError(options.signal);
		if (options.sessionFile !== undefined && options.forkFrom !== undefined) {
			throw new TaskStartupError("session-invalid", "OMP worker cannot resume and fork in the same startup");
		}
		if (options.forkFrom !== undefined && (!isAbsolute(options.forkFrom) || options.forkFrom.length === 0)) {
			throw new TaskStartupError("session-invalid", "OMP fork source session file must be absolute");
		}
		if (!existsSync(options.cwd)) throw new TaskStartupError("cwd-missing", `OMP worker cwd does not exist: ${options.cwd}`);
		if (!existsSync(options.sessionDir)) throw new TaskStartupError("session-dir-missing", `OMP worker sessionDir does not exist: ${options.sessionDir}`);
		const cwd = realpathSync(options.cwd);
		const sessionDir = realpathSync(options.sessionDir);
		if (!existsSync(sessionDir)) throw new TaskStartupError("session-dir-missing", `OMP worker sessionDir does not exist: ${sessionDir}`);

		let forkSourceState: ProjectSessionState | undefined;
		if (options.forkFrom !== undefined) {
			try {
				forkSourceState = (await loadValidatedSessionState(options.forkFrom)).state;
				if (forkSourceState.cwd !== cwd) throw new Error("OMP fork source cwd does not match the requested worker cwd");
			} catch (error) {
				throw taskStartupError("session-invalid", error);
			}
		}
		if (options.signal?.aborted) throw startupAbortError(options.signal);

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
		if (options.contextScope) {
			command.push(
				"--append-system-prompt",
				formatAgentGuidance(
					options.contextScope.botUid,
					options.contextScope.target,
					options.contextScope.cliReady,
				),
			);
		}
		if (options.model) command.push("--model", options.model);
		if (options.sessionFile) command.push("--resume", resolve(options.sessionFile));
		else if (forkSourceState) command.push("--fork", forkSourceState.sessionFile);
		if (options.goalContinuation === true) command.push("--config", FIXED_GOAL_RPC_CONFIG_PATH);
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
			throw taskStartupError("spawn-failed", asError(error, "Could not spawn OMP RPC worker"));
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
			throw new TaskStartupError("identity-unverified", "Could not verify the OMP worker process identity");
		}
		const watchdogStart = await createWatchdog(
			child.pid,
			snapshot.pgid,
			parentSnapshot?.identity ?? "",
			this.#startupTimeoutMs,
			this.#options.spawnWatchdog,
		);
		if (process.platform !== "win32" && !watchdogStart.ok) {
			await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			throw new TaskStartupError(
				"watchdog-unavailable",
				watchdogStart.detail.length > 0
					? `Could not start the OMP parent-death watchdog: ${watchdogStart.detail}`
					: "Could not start the OMP parent-death watchdog",
			);
		}
		const watchdog = watchdogStart.ok ? watchdogStart.watchdog : undefined;
		const processInfo: WorkerProcess = { pid: child.pid, pgid: snapshot.pgid, identity: snapshot.identity, cwd };
		try {
			options.onSpawn(processInfo);
		} catch (error) {
			const stopped = await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			if (stopped) stopWatchdog(watchdog);
			throw error;
		}
		const worker = new RpcWorker(
			child,
			watchdog,
			snapshot.pgid,
			this.#startupTimeoutMs,
			this.#cancelTimeoutMs,
			!options.sessionFile && forkSourceState === undefined,
			options.goalContinuation === true,
		);
		try {
			const state = await worker.initialize(options.signal);
			if (options.signal?.aborted) throw startupAbortError(options.signal);
			if (forkSourceState) {
				if (state.sessionId === forkSourceState.sessionId || state.sessionFile === forkSourceState.sessionFile) {
					throw new TaskStartupError("session-invalid", "OMP native fork did not create a distinct session");
				}
				if (state.cwd !== cwd || state.cwd !== forkSourceState.cwd) {
					throw new TaskStartupError("session-invalid", "OMP native fork session cwd does not match the source session");
				}
			}
			return worker;
		} catch (error) {
			await worker.stop();
			if (options.signal?.aborted) throw startupAbortError(options.signal);
			throw taskStartupError("handshake-failed", asError(error, "OMP RPC worker failed during startup"));
		}
	}

	async createCommit(options: CommitWorkerOptions): Promise<CommitWorker> {
		const cliEnv = this.#options.cliEnv?.();
		return createNativeCommitWorker({
			ompCommand: this.#options.ompCommand,
			extraArgs: this.#options.extraArgs ?? [],
			startupTimeoutMs: this.#startupTimeoutMs,
			cancelTimeoutMs: this.#cancelTimeoutMs,
			...(cliEnv === undefined ? {} : { cliEnv }),
			...(this.#options.spawnWatchdog === undefined ? {} : { spawnWatchdog: this.#options.spawnWatchdog }),
			commit: options,
		});
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
