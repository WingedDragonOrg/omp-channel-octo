import {
	AGENT_GUIDANCE_PREFIX,
	MEMBER_CONTEXT_PREFIX,
	formatAgentGuidance,
	formatMemberContext,
} from "../session/agent-context.ts";
import { isAbsolute } from "node:path";
import {
	contextSidecarPath,
	formatSharedContext,
	preparedReceiptFor,
	readBoundSession,
	readContextSidecar,
	readAgentContextOverlay,
	writePreparedReceipt,
	type ContextSidecar,
	type AgentContextOverlay,
} from "../session/context-sidecar.ts";

export { contextSidecarFileName } from "../session/context-sidecar.ts";

/**
 * The extension is loaded inside OMP, not inside the channel host.  Keep the
 * API surface structural so this module does not load the OMP runtime or its
 * SDK from the host process.
 */
type ExtensionEvent = Record<string, unknown>;
type ExtensionContext = {
	sessionManager?: {
		getSessionId?: () => unknown;
	};
	abort?: () => unknown;
};
type ExtensionHandler = (event: ExtensionEvent, context: ExtensionContext) => unknown | Promise<unknown>;
type ExtensionApi = {
	on(event: "before_agent_start" | "context" | "session.compacting", handler: ExtensionHandler): void;
};

type ExtensionScope = {
	root: string;
	botUid: string;
	target: {
		channelType: 1 | 2 | 5;
		channelId: string;
	};
};

type TurnSidecar = {
	scope: ExtensionScope;
	sessionId: string;
	sidecar: ContextSidecar;
	agentContextOverlay?: AgentContextOverlay;
};

type ContextObservation = {
	turnId: string | undefined;
	attemptId: string;
	userMessageCount: number;
	acceptedPendingCount: number;
};

const ROOT_ENV = "OMP_CHANNEL_OCTO_CONTEXT_ROOT";
const BOT_ENV = "OMP_CHANNEL_OCTO_CONTEXT_BOT_UID";
const CHANNEL_TYPE_ENV = "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE";
const CHANNEL_ID_ENV = "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID";


function nonEmptyString(value: unknown, field: string): string {
	if (typeof value !== "string" || value.length === 0) throw new Error(`${field} is missing`);
	return value;
}

function sameTarget(
	left: { channelType: number; channelId: string },
	right: { channelType: number; channelId: string },
): boolean {
	return left.channelType === right.channelType && left.channelId === right.channelId;
}

function readScope(): ExtensionScope | undefined {
	const root = process.env[ROOT_ENV];
	const botUid = process.env[BOT_ENV];
	const channelType = process.env[CHANNEL_TYPE_ENV];
	const channelId = process.env[CHANNEL_ID_ENV];
	const supplied = [root, botUid, channelType, channelId].some(value => value !== undefined);
	if (!supplied) return undefined;
	if (
		typeof root !== "string" || root.length === 0
		|| typeof botUid !== "string" || botUid.length === 0
		|| typeof channelType !== "string" || channelType.length === 0
		|| typeof channelId !== "string" || channelId.length === 0
	) {
		throw new Error("context extension scope is incomplete");
	}
	if (!isAbsolute(root)) throw new Error("context extension root must be absolute");
	const parsedChannelType = Number(channelType);
	if (parsedChannelType !== 1 && parsedChannelType !== 2 && parsedChannelType !== 5) throw new Error("context extension target type is invalid");
	return {
		root,
		botUid,
		target: { channelType: parsedChannelType, channelId },
	};
}

function sessionIdFromContext(context: ExtensionContext): string {
	const sessionId = context.sessionManager?.getSessionId?.();
	return nonEmptyString(sessionId, "OMP session ID");
}

function abortAndThrow(context: ExtensionContext, reason: string): never {
	try {
		context.abort?.();
	} catch {
		// The lifecycle runner may throw while aborting; the fail-closed error is
		// still returned below and, importantly, no context body is returned.
	}
	throw new Error(`Octo context extension rejected the turn: ${reason}`);
}

/**
 * Extension handlers are registered once per worker process, so every session
 * OMP opens in it reaches them, subagent and eval sessions included.  The host
 * publishes the session it drives for this scope before every prompt and keeps
 * it as the scope's current session; a published session that is not this one
 * runs untouched.  A scope that never published a pointer keeps the original
 * strict behavior, so a turn the host prepared is still validated.
 */
async function boundSessionFor(scope: ExtensionScope, context: ExtensionContext): Promise<string | undefined> {
	const bound = await readBoundSession(scope.root);
	if (bound === undefined) return undefined;
	if (bound.botUid !== scope.botUid || !sameTarget(bound.target, scope.target)) {
		return abortAndThrow(context, "bound session scope mismatch");
	}
	return bound.sessionId;
}

function sidecarForScope(
	scope: ExtensionScope,
	sessionId: string,
	context: ExtensionContext,
): Promise<ContextSidecar> {
	const path = contextSidecarPath(scope.root, sessionId);
	return readContextSidecar(path).catch(error =>
		abortAndThrow(context, error instanceof Error ? error.message : "sidecar could not be read"));
}

function validateSidecar(
	sidecar: ContextSidecar,
	scope: ExtensionScope,
	sessionId: string,
	context: ExtensionContext,
): ContextSidecar {
	if (sidecar.botUid !== scope.botUid) return abortAndThrow(context, "bot identity mismatch");
	if (sidecar.sessionId !== sessionId) return abortAndThrow(context, "session identity mismatch");
	if (!sameTarget(sidecar.target, scope.target)) return abortAndThrow(context, "target identity mismatch");
	if (
		sidecar.marker.botUid !== scope.botUid
		|| sidecar.marker.sessionId !== sessionId
		|| !sameTarget(sidecar.marker.target, scope.target)
		|| sidecar.marker.version !== sidecar.snapshot.version
		|| sidecar.marker.attemptId !== sidecar.attemptId
	) {
		return abortAndThrow(context, "sidecar marker mismatch");
	}
	if (sidecar.injectionDecision === "unchanged") {
		if (sidecar.marker.phase !== "committed") return abortAndThrow(context, "unchanged sidecar is not committed");
	} else if (sidecar.marker.phase !== "pending") {
		return abortAndThrow(context, "injection sidecar is not pending");
	}
	if (sidecar.prepared !== undefined && !samePreparedIdentity(sidecar)) {
		return abortAndThrow(context, "prepared receipt mismatch");
	}
	return sidecar;
}

function samePreparedIdentity(sidecar: ContextSidecar): boolean {
	const prepared = sidecar.prepared;
	return prepared !== undefined
		&& prepared.botUid === sidecar.botUid
		&& prepared.sessionId === sidecar.sessionId
		&& prepared.version === sidecar.snapshot.version
		&& prepared.attemptId === sidecar.attemptId
		&& sameTarget(prepared.target, sidecar.target);
}

function eventSessionMatches(event: ExtensionEvent, sessionId: string): void {
	if (event.sessionId !== undefined && event.sessionId !== sessionId) throw new Error("event session identity mismatch");
}

function systemPromptFromEvent(event: ExtensionEvent, context: ExtensionContext): string[] {
	if (!Array.isArray(event.systemPrompt) || !event.systemPrompt.every(item => typeof item === "string")) {
		return abortAndThrow(context, "system prompt is invalid");
	}
	return event.systemPrompt as string[];
}

function formatCompactionContext(sidecar: ContextSidecar): string {
	const lines = [
		sidecar.target.channelType === 1
			? "压缩恢复边界（当前私聊 turn 身份）："
			: "压缩恢复边界（来自当前 turn 的 immutable 群/线程快照）：",
		`bot=${sidecar.botUid}`,
		`target=${sidecar.target.channelType}:${sidecar.target.channelId}`,
		`session=${sidecar.sessionId}`,
	];
	if (sidecar.target.channelType !== 1) {
		lines.push(
			`version=${sidecar.snapshot.version}`,
			formatSharedContext(sidecar.snapshot, "unchanged"),
		);
	}
	return lines.join("\n");
}

async function turnSidecar(event: ExtensionEvent, context: ExtensionContext): Promise<TurnSidecar | undefined> {
	let scope: ExtensionScope | undefined;
	try {
		scope = readScope();
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "scope is invalid");
	}
	if (!scope) return undefined;

	let sessionId: string;
	try {
		sessionId = sessionIdFromContext(context);
		eventSessionMatches(event, sessionId);
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "session identity is invalid");
	}
	const bound = await boundSessionFor(scope, context);
	if (bound !== undefined && bound !== sessionId) return undefined;
	const sidecar = validateSidecar(await sidecarForScope(scope, sessionId, context), scope, sessionId, context);
	const agentContextOverlay = await readAgentContextOverlay(
		contextSidecarPath(scope.root, sessionId),
		sidecar,
	).catch(error => abortAndThrow(
		context,
		error instanceof Error ? error.message : "agent context overlay could not be read",
	));
	return {
		scope,
		sessionId,
		sidecar,
		...(agentContextOverlay === undefined ? {} : { agentContextOverlay }),
	};
}

async function prepareSharedContext(scope: ExtensionScope, sidecar: ContextSidecar, context: ExtensionContext): Promise<string> {
	// The receipt is the extension's acknowledgement that it validated and
	// rendered this exact sidecar.  It is published before returning any body.
	const path = contextSidecarPath(scope.root, sidecar.sessionId);
	try {
		if (!sidecar.prepared) await writePreparedReceipt(path, preparedReceiptFor(sidecar));
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "prepared receipt could not be written");
	}
	return formatSharedContext(sidecar.snapshot, sidecar.injectionDecision, sidecar.previousVersion);
}

function isManagedAgentContextMessage(value: unknown): boolean {
	if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
	const message = value as Record<string, unknown>;
	if (message.role !== "developer" || typeof message.content !== "string") return false;
	return message.content.startsWith(MEMBER_CONTEXT_PREFIX)
		|| message.content.startsWith(AGENT_GUIDANCE_PREFIX);
}

function userMessageText(value: unknown): string | undefined {
	if (typeof value === "string") return value;
	if (Array.isArray(value)) {
		let text = "";
		for (const part of value) {
			if (
				part !== null && typeof part === "object" && !Array.isArray(part)
				&& "text" in part && typeof part.text === "string"
			) {
				text += part.text;
			}
		}
		return text;
	}
	if (
		value !== null && typeof value === "object" && !Array.isArray(value)
		&& "text" in value && typeof value.text === "string"
	) {
		return value.text;
	}
	return undefined;
}

function userMessageTexts(messages: readonly unknown[]): Array<string | undefined> {
	const texts: Array<string | undefined> = [];
	for (const value of messages) {
		if (value === null || typeof value !== "object" || Array.isArray(value)) continue;
		const message = value as Record<string, unknown>;
		if (message.role === "user") texts.push(userMessageText(message.content));
	}
	return texts;
}

function noteAgentStart(
	observations: Map<string, ContextObservation>,
	turn: TurnSidecar,
	prompt: unknown,
): void {
	const previous = observations.get(turn.sessionId);
	const sameTurn = previous !== undefined
		&& previous.turnId === turn.sidecar.turnId
		&& previous.attemptId === turn.sidecar.attemptId;
	let acceptedPendingCount = sameTurn ? previous.acceptedPendingCount : 0;
	const nextPending = turn.agentContextOverlay?.pending[acceptedPendingCount];
	if (sameTurn && typeof prompt === "string" && nextPending?.text === prompt) {
		acceptedPendingCount += 1;
	}
	observations.set(turn.sessionId, {
		turnId: turn.sidecar.turnId,
		attemptId: turn.sidecar.attemptId,
		userMessageCount: (previous?.userMessageCount ?? 0) + 1,
		acceptedPendingCount,
	});
}

function agentContextForModelRequest(
	observations: Map<string, ContextObservation>,
	turn: TurnSidecar,
	messages: readonly unknown[],
): AgentContextOverlay["agentContext"] | ContextSidecar["agentContext"] {
	const currentUserMessages = userMessageTexts(messages);
	const overlay = turn.agentContextOverlay;
	const previous = observations.get(turn.sessionId);
	const sameTurn = previous !== undefined
		&& previous.turnId === turn.sidecar.turnId
		&& previous.attemptId === turn.sidecar.attemptId;
	let acceptedPendingCount = sameTurn
		? Math.min(previous.acceptedPendingCount, overlay?.pending.length ?? 0)
		: 0;
	if (sameTurn && overlay) {
		for (const text of currentUserMessages.slice(previous.userMessageCount)) {
			const pending = overlay.pending[acceptedPendingCount];
			if (!pending || text !== pending.text) break;
			acceptedPendingCount += 1;
		}
	}
	observations.set(turn.sessionId, {
		turnId: turn.sidecar.turnId,
		attemptId: turn.sidecar.attemptId,
		userMessageCount: currentUserMessages.length,
		acceptedPendingCount,
	});
	return overlay?.pending[acceptedPendingCount - 1]?.agentContext
		?? overlay?.agentContext
		?? turn.sidecar.agentContext;
}

async function beforeAgentStart(
	event: ExtensionEvent,
	context: ExtensionContext,
	observeAgentStart: (turn: TurnSidecar, prompt: unknown) => void,
): Promise<{ systemPrompt: string[] } | undefined> {
	const turn = await turnSidecar(event, context);
	if (!turn) return undefined;
	observeAgentStart(turn, event.prompt);
	const originalSystemPrompt = systemPromptFromEvent(event, context)
		.filter(line => !line.startsWith(AGENT_GUIDANCE_PREFIX));
	const agentContext = turn.agentContextOverlay?.agentContext ?? turn.sidecar.agentContext;
	const systemPrompt = agentContext === undefined || agentContext.guideInDeveloper
		? originalSystemPrompt
		: [
				...originalSystemPrompt,
				formatAgentGuidance(
					turn.sidecar.botUid,
					turn.sidecar.target,
					agentContext.cliReady,
				),
			];
	if (turn.sidecar.injectionDecision === "unchanged") return { systemPrompt };
	const formatted = await prepareSharedContext(turn.scope, turn.sidecar, context);
	return { systemPrompt: [...systemPrompt, formatted] };
}

async function sessionCompacting(event: ExtensionEvent, context: ExtensionContext): Promise<{
	context: string[];
	preserveData: Record<string, unknown>;
} | undefined> {
	let scope: ExtensionScope | undefined;
	try {
		scope = readScope();
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "scope is invalid");
	}
	if (!scope) return undefined;

	let sessionId: string;
	try {
		sessionId = sessionIdFromContext(context);
		eventSessionMatches(event, sessionId);
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "session identity is invalid");
	}
	const bound = await boundSessionFor(scope, context);
	if (bound !== undefined && bound !== sessionId) return undefined;
	const sidecar = validateSidecar(await sidecarForScope(scope, sessionId, context), scope, sessionId, context);
	return {
		context: [formatCompactionContext(sidecar)],
		preserveData: {
			botUid: sidecar.botUid,
			target: { ...sidecar.target },
			sessionId: sidecar.sessionId,
			version: sidecar.snapshot.version,
			attemptId: sidecar.attemptId,
		},
	};
}

export default function contextExtension(api: ExtensionApi): void {
	let continuationAttempt: string | undefined;
	const contextObservations = new Map<string, ContextObservation>();
	api.on("before_agent_start", async (event, context) => {
		const result = await beforeAgentStart(
			event,
			context,
			(turn, prompt) => noteAgentStart(contextObservations, turn, prompt),
		);
		if (result) continuationAttempt = undefined;
		return result;
	});
	// Native retry continues the agent without before_agent_start. `context` runs before
	// each model call, and its developer message is transient rather than a new user turn.
	api.on("context", async (event, context) => {
		const turn = await turnSidecar(event, context);
		if (!turn) return undefined;
		const key = `${turn.sessionId}:${turn.sidecar.attemptId}`;
		const injectShared = turn.sidecar.injectionDecision !== "unchanged"
			&& (!turn.sidecar.prepared || continuationAttempt === key);
		const baseAgentContext = turn.agentContextOverlay?.agentContext ?? turn.sidecar.agentContext;
		if (!Array.isArray(event.messages)) {
			if (baseAgentContext === undefined && !injectShared) return undefined;
			return abortAndThrow(context, "context messages are invalid");
		}
		const messages = event.messages.filter(message => !isManagedAgentContextMessage(message));
		const agentContext = agentContextForModelRequest(contextObservations, turn, messages);
		const nextMessages = [...messages];
		const timestamp = Date.now();
		if (agentContext?.guideInDeveloper) {
			nextMessages.push({
				role: "developer",
				content: formatAgentGuidance(turn.sidecar.botUid, turn.sidecar.target, agentContext.cliReady),
				timestamp,
			});
		}
		if (agentContext?.memberSnapshot) {
			nextMessages.push({
				role: "developer",
				content: formatMemberContext(turn.sidecar.target, agentContext.memberSnapshot),
				timestamp,
			});
		}
		if (injectShared) {
			const content = await prepareSharedContext(turn.scope, turn.sidecar, context);
			continuationAttempt = key;
			nextMessages.push({ role: "developer", content, timestamp });
		}
		if (nextMessages.length === event.messages.length
			&& messages.length === event.messages.length
			&& agentContext?.guideInDeveloper !== true
			&& agentContext?.memberSnapshot === undefined
			&& !injectShared) {
			return undefined;
		}
		return { messages: nextMessages };
	});
	api.on("session.compacting", sessionCompacting);
}
