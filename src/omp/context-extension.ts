import { randomUUID } from "node:crypto";
import { readFile, rm } from "node:fs/promises";
import { isAbsolute } from "node:path";
import {
	MEMBER_CONTEXT_PREFIX,
	MAX_INLINE_MEMBERS,
	formatMemberContext,
	type AgentMemberSnapshot,
} from "../session/agent-context.ts";
import type { GroupContext } from "../contracts.ts";
import {
	contextSidecarPath,
	compactedContextFromUnknown,
	formatSharedContext,
	preparedReceiptFor,
	readBoundSession,
	readContextSidecar,
	readAgentContextOverlay,
	writePreparedReceipt,
	writeCompactedContextReceipt,
	writePendingCompaction,
	commitPendingCompaction,
	FRESH_CONTEXT_READ_SCHEMA_VERSION,
	freshContextReadPaths,
	isContextSidecarObject,
	normalizeGroupContext,
	writeFreshContextReadFile,
	type ContextSidecar,
	type FreshContextReadIdentity,
	type FreshContextReadPaths,
	type CompactedContextReceipt,
	type AgentContextOverlay,
	type PendingCompactionContext,
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
		getBranch?: () => unknown;
	};
	abort?: () => unknown;
};
type ExtensionHandler = (event: ExtensionEvent, context: ExtensionContext) => unknown | Promise<unknown>;
type ExtensionApi = {
	on(
		event: "input" | "before_agent_start" | "context" | "session_before_compact" | "session.compacting",
		handler: ExtensionHandler,
	): void;
	sendMessage?(
		message: {
			customType: string;
			content: string;
			display: false;
			details: Record<string, unknown>;
		},
		options?: { triggerTurn?: boolean; deliverAs?: "steer" },
	): void;
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

type InputCursor = {
	turnId: string;
	processedIds: Set<string>;
};

type FreshCompactionSnapshot = {
	turnId: string;
	requestId: string;
	snapshot: GroupContext;
};

const ROOT_ENV = "OMP_CHANNEL_OCTO_CONTEXT_ROOT";
const BOT_ENV = "OMP_CHANNEL_OCTO_CONTEXT_BOT_UID";
const CHANNEL_TYPE_ENV = "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE";
const CHANNEL_ID_ENV = "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID";


function nonEmptyString(value: unknown, field: string): string {
	if (typeof value !== "string" || value.length === 0) throw new Error(`${field} is missing`);
	return value;
}
const FRESH_CONTEXT_WAIT_TIMEOUT_MS = 7_000;
const FRESH_CONTEXT_POLL_MS = 20;

function isMissingFile(error: unknown): boolean {
	return isContextSidecarObject(error) && error.code === "ENOENT";
}

function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
	if (signal.aborted) return Promise.resolve();
	const deferred = Promise.withResolvers<void>();
	const finish = (): void => {
		clearTimeout(timer);
		signal.removeEventListener("abort", finish);
		deferred.resolve();
	};
	const timer = setTimeout(finish, milliseconds);
	signal.addEventListener("abort", finish, { once: true });
	if (signal.aborted) finish();
	return deferred.promise;
}

function eventSignal(event: ExtensionEvent): AbortSignal {
	const value = event.signal;
	if (
		value === null || typeof value !== "object"
		|| !("aborted" in value) || typeof value.aborted !== "boolean"
		|| !("addEventListener" in value) || typeof value.addEventListener !== "function"
		|| !("removeEventListener" in value) || typeof value.removeEventListener !== "function"
	) {
		throw new Error("compaction abort signal is unavailable");
	}
	return value as AbortSignal;
}

async function waitForFreshResponse(path: string, signal: AbortSignal): Promise<unknown> {
	while (!signal.aborted) {
		try {
			return JSON.parse(await readFile(path, "utf8")) as unknown;
		} catch (error) {
			if (!isMissingFile(error)) throw error;
		}
		await pause(FRESH_CONTEXT_POLL_MS, signal);
	}
	throw signal.reason ?? new Error("fresh compaction read was cancelled");
}

function freshGroupContext(value: unknown): GroupContext {
	if (
		!isContextSidecarObject(value)
		|| typeof value.content !== "string"
		|| !Number.isSafeInteger(value.version)
		|| (value.version as number) < 0
	) {
		throw new Error("fresh group context response is invalid");
	}
	return normalizeGroupContext({
		content: value.content,
		version: value.version as number,
		updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : null,
		updatedBy: typeof value.updatedBy === "string" ? value.updatedBy : "",
	});
}

function freshSnapshotFromResponse(
	value: unknown,
	identity: FreshContextReadIdentity,
): GroupContext {
	if (
		!isContextSidecarObject(value)
		|| value.schemaVersion !== identity.schemaVersion
		|| value.requestId !== identity.requestId
		|| value.botUid !== identity.botUid
		|| value.sessionId !== identity.sessionId
		|| value.turnId !== identity.turnId
		|| !isContextSidecarObject(value.target)
		|| value.target.channelType !== identity.target.channelType
		|| value.target.channelId !== identity.target.channelId
	) {
		throw new Error("fresh group context response identity mismatch");
	}
	if (typeof value.error === "string") throw new Error(value.error);
	return freshGroupContext(value.snapshot);
}

function sameTarget(
	left: { channelType: number; channelId: string },
	right: { channelType: number; channelId: string },
): boolean {
	return left.channelType === right.channelType && left.channelId === right.channelId;
}

function managedContextIdentity(scope: ExtensionScope, sessionId: string): Record<string, unknown> {
	return {
		ompChannelOctoContext: {
			schemaVersion: 1,
			botUid: scope.botUid,
			target: { ...scope.target },
			sessionId,
		},
	};
}

function managedContextMatches(
	value: unknown,
	scope: ExtensionScope,
	sessionId: string,
): boolean | undefined {
	if (!isContextSidecarObject(value) || !isContextSidecarObject(value.ompChannelOctoContext)) {
		return undefined;
	}
	const identity = value.ompChannelOctoContext;
	if (
		identity.schemaVersion !== 1
		|| typeof identity.botUid !== "string"
		|| typeof identity.sessionId !== "string"
		|| !isContextSidecarObject(identity.target)
		|| (identity.target.channelType !== 1
			&& identity.target.channelType !== 2
			&& identity.target.channelType !== 5)
		|| typeof identity.target.channelId !== "string"
	) {
		return undefined;
	}
	return identity.botUid === scope.botUid
		&& identity.sessionId === sessionId
		&& identity.target.channelType === scope.target.channelType
		&& identity.target.channelId === scope.target.channelId;
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


function formatCompactionContext(sidecar: ContextSidecar, snapshot: GroupContext): string {
	const lines = [
		sidecar.target.channelType === 1
			? "压缩恢复边界（当前私聊 turn 身份）："
			: "压缩恢复边界（来自本次 host fresh read 的群/线程快照）：",
		`bot=${sidecar.botUid}`,
		`target=${sidecar.target.channelType}:${sidecar.target.channelId}`,
		`session=${sidecar.sessionId}`,
	];
	if (sidecar.target.channelType !== 1) {
		lines.push(
			`version=${snapshot.version}`,
			formatSharedContext(snapshot, "unchanged"),
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

function canonicalMemberSnapshot(value: AgentMemberSnapshot): AgentMemberSnapshot {
	if (value.members === undefined) {
		return {
			count: value.count,
			...(value.queryAction === undefined ? {} : { queryAction: value.queryAction }),
			...(value.rosterFingerprint === undefined ? {} : { rosterFingerprint: value.rosterFingerprint }),
		};
	}
	return {
		count: value.count,
		members: [...value.members]
			.map(({ uid, name }) => ({ uid, name }))
			.sort((left, right) => left.uid < right.uid ? -1 : left.uid > right.uid ? 1 : left.name < right.name ? -1 : left.name > right.name ? 1 : 0),
	};
}

function memberSnapshotFromText(text: string, target: ExtensionScope["target"]): AgentMemberSnapshot | undefined {
	const marker = text.lastIndexOf(MEMBER_CONTEXT_PREFIX);
	if (marker < 0) return undefined;
	try {
		const payload = JSON.parse(text.slice(marker + MEMBER_CONTEXT_PREFIX.length)) as unknown;
		if (
			!isContextSidecarObject(payload)
			|| !isContextSidecarObject(payload.target)
			|| payload.target.channelType !== target.channelType
			|| payload.target.channelId !== target.channelId
			|| !Number.isSafeInteger(payload.count)
			|| (payload.count as number) < 0
		) {
			return undefined;
		}
		if ((payload.count as number) <= MAX_INLINE_MEMBERS) {
			if (!Array.isArray(payload.members) || payload.members.length !== payload.count) return undefined;
			const members = payload.members.flatMap(item => {
				if (
					!isContextSidecarObject(item)
					|| typeof item.uid !== "string"
					|| typeof item.name !== "string"
				) {
					return [];
				}
				return [{ uid: item.uid, name: item.name }];
			});
			if (members.length !== payload.count) return undefined;
			return canonicalMemberSnapshot({ count: payload.count as number, members });
		}
		if (
			payload.members !== undefined
			|| (payload.queryAction !== "octo_management.group-members"
				&& payload.queryAction !== "octo_management.thread-members")
			|| (payload.rosterFingerprint !== undefined
				&& (typeof payload.rosterFingerprint !== "string" || payload.rosterFingerprint.length === 0))
		) {
			return undefined;
		}
		return {
			count: payload.count as number,
			queryAction: payload.queryAction,
			...(payload.rosterFingerprint === undefined
				? {}
				: { rosterFingerprint: payload.rosterFingerprint }),
		};
	} catch {
		return undefined;
	}
}

function stripForeignMemberContext(
	text: string,
	target: ExtensionScope["target"],
): string | undefined {
	const marker = text.lastIndexOf(MEMBER_CONTEXT_PREFIX);
	if (marker < 0) return undefined;
	try {
		const payload = JSON.parse(text.slice(marker + MEMBER_CONTEXT_PREFIX.length)) as unknown;
		if (!isContextSidecarObject(payload) || !isContextSidecarObject(payload.target)) return undefined;
		const foreignTarget = payload.target;
		if (
			(foreignTarget.channelType !== 1
				&& foreignTarget.channelType !== 2
				&& foreignTarget.channelType !== 5)
			|| typeof foreignTarget.channelId !== "string"
			|| sameTarget(foreignTarget as ExtensionScope["target"], target)
			|| memberSnapshotFromText(text, foreignTarget as ExtensionScope["target"]) === undefined
		) {
			return undefined;
		}
		const separatorStart = marker >= 2 && text.slice(marker - 2, marker) === "\n\n"
			? marker - 2
			: marker;
		return text.slice(0, separatorStart);
	} catch {
		return undefined;
	}
}

function projectManagedContext(
	messages: readonly unknown[],
	scope: ExtensionScope,
	sessionId: string,
): { messages: unknown[]; changed: boolean } {
	let changed = false;
	const projected: unknown[] = [];
	for (const message of messages) {
		if (!isContextSidecarObject(message)) {
			projected.push(message);
			continue;
		}
		if (
			message.role === "custom"
			&& message.customType === "omp-channel-octo-context"
			&& managedContextMatches(message.details, scope, sessionId) === false
		) {
			changed = true;
			continue;
		}
		if (message.role === "user") {
			if (typeof message.content === "string") {
				const content = stripForeignMemberContext(message.content, scope.target);
				if (content !== undefined) {
					projected.push({ ...message, content });
					changed = true;
					continue;
				}
			} else if (Array.isArray(message.content)) {
				let contentChanged = false;
				const content = message.content.flatMap(part => {
					if (!isContextSidecarObject(part) || typeof part.text !== "string") return [part];
					const text = stripForeignMemberContext(part.text, scope.target);
					if (text === undefined) return [part];
					contentChanged = true;
					return text.length === 0 ? [] : [{ ...part, text }];
				});
				if (contentChanged) {
					projected.push({ ...message, content });
					changed = true;
					continue;
				}
			}
		}
		projected.push(message);
	}
	return { messages: projected, changed };
}

function equalMemberSnapshots(
	left: AgentMemberSnapshot | undefined,
	right: AgentMemberSnapshot,
): boolean {
	if (left === undefined) return false;
	return JSON.stringify(canonicalMemberSnapshot(left)) === JSON.stringify(canonicalMemberSnapshot(right));
}

function latestHistoryMemberSnapshot(
	context: ExtensionContext,
	target: ExtensionScope["target"],
): AgentMemberSnapshot | undefined {
	const manager = context.sessionManager;
	const getBranch = manager?.getBranch;
	if (typeof getBranch !== "function") throw new Error("OMP session history is unavailable");
	const entries = getBranch.call(manager);
	if (!Array.isArray(entries)) throw new Error("OMP session history is invalid");
	let latest: AgentMemberSnapshot | undefined;
	for (const entry of entries) {
		if (!isContextSidecarObject(entry) || entry.type !== "message" || !isContextSidecarObject(entry.message)) continue;
		if (entry.message.role !== "user") continue;
		const text = userMessageText(entry.message.content);
		if (text === undefined) continue;
		latest = memberSnapshotFromText(text, target) ?? latest;
	}
	return latest;
}

function sessionBranch(context: ExtensionContext): unknown[] {
	const manager = context.sessionManager;
	const getBranch = manager?.getBranch;
	if (typeof getBranch !== "function") throw new Error("OMP session history is unavailable");
	const entries = getBranch.call(manager);
	if (!Array.isArray(entries)) throw new Error("OMP session history is invalid");
	return entries;
}

function latestCompactionContext(
	context: ExtensionContext,
	scope: ExtensionScope,
	sessionId: string,
): CompactedContextReceipt | undefined {
	const entries = sessionBranch(context);
	for (let index = entries.length - 1; index >= 0; index -= 1) {
		const entry = entries[index];
		if (!isContextSidecarObject(entry) || entry.type !== "compaction") continue;
		if (!isContextSidecarObject(entry.preserveData)) return undefined;
		const candidate = entry.preserveData.ompChannelOctoContext;
		if (candidate === undefined) return undefined;
		const receipt = compactedContextFromUnknown(candidate);
		if (
			receipt.botUid !== scope.botUid
			|| receipt.sessionId !== sessionId
			|| receipt.target.channelType !== scope.target.channelType
			|| receipt.target.channelId !== scope.target.channelId
		) {
			throw new Error("committed compaction context scope mismatch");
		}
		return receipt;
	}
	return undefined;
}

function committedPendingCompaction(
	context: ExtensionContext,
	pending: PendingCompactionContext | undefined,
): PendingCompactionContext | undefined {
	if (!pending) return undefined;
	const branch = sessionBranch(context);
	const baseIndex = pending.baseLeafId === null
		? -1
		: branch.findIndex(entry => isContextSidecarObject(entry) && entry.id === pending.baseLeafId);
	if (pending.baseLeafId !== null && baseIndex < 0) return undefined;
	const committed = branch.slice(baseIndex + 1).some(entry =>
		isContextSidecarObject(entry) && entry.type === "compaction");
	return committed ? pending : undefined;
}

function hasPersistentSharedContext(
	context: ExtensionContext,
	content: string,
	scope: ExtensionScope,
	sessionId: string,
): boolean {
	return sessionBranch(context).some(entry => {
		if (!isContextSidecarObject(entry)) return false;
		if (entry.type === "custom_message") {
			return entry.customType === "omp-channel-octo-context"
				&& userMessageText(entry.content) === content
				&& managedContextMatches(entry.details, scope, sessionId) !== false;
		}
		return entry.type === "message"
			&& isContextSidecarObject(entry.message)
			&& entry.message.role === "custom"
			&& entry.message.customType === "omp-channel-octo-context"
			&& userMessageText(entry.message.content) === content
			&& managedContextMatches(entry.message.details, scope, sessionId) !== false;
	});
}

function sameGroupContext(left: GroupContext, right: GroupContext): boolean {
	return JSON.stringify(normalizeGroupContext(left)) === JSON.stringify(normalizeGroupContext(right));
}

function branchLeafId(context: ExtensionContext): string | null {
	const branch = sessionBranch(context);
	const leaf = branch.at(-1);
	return isContextSidecarObject(leaf) && typeof leaf.id === "string" && leaf.id.length > 0
		? leaf.id
		: null;
}

async function persistPendingCompaction(
	turn: TurnSidecar,
	context: ExtensionContext,
	requestId: string,
	snapshot: GroupContext,
): Promise<void> {
	await writePendingCompaction(
		contextSidecarPath(turn.scope.root, turn.sessionId),
		{
			schemaVersion: FRESH_CONTEXT_READ_SCHEMA_VERSION,
			requestId,
			botUid: turn.scope.botUid,
			target: { ...turn.scope.target },
			sessionId: turn.sessionId,
			turnId: nonEmptyString(turn.sidecar.turnId, "context turn ID"),
			attemptId: turn.sidecar.attemptId,
			snapshot,
			baseLeafId: branchLeafId(context),
		},
	);
}

async function transformUserInput(
	event: ExtensionEvent,
	context: ExtensionContext,
	cursors: Map<string, InputCursor>,
): Promise<{ text: string } | undefined> {
	if (event.source !== "rpc") return undefined;
	if (typeof event.text !== "string") return abortAndThrow(context, "input text is invalid");
	if (event.text.startsWith("/")) return undefined;
	const turn = await turnSidecar(event, context);
	if (!turn || turn.scope.target.channelType === 1) return undefined;

	const baseContext = turn.agentContextOverlay?.agentContext ?? turn.sidecar.agentContext;
	const pending = turn.agentContextOverlay?.pending ?? [];
	const turnId = nonEmptyString(turn.sidecar.turnId, "context turn ID");
	const previous = cursors.get(turn.sessionId);
	const processedIds = previous?.turnId === turnId
		? new Set(previous.processedIds)
		: new Set<string>();
	const pendingIndex = pending.findIndex(item =>
		!processedIds.has(item.messageId) && item.text === event.text);
	const staged = pendingIndex < 0 ? undefined : pending[pendingIndex];
	const priorPending = pending.filter(item => processedIds.has(item.messageId));
	if (staged) processedIds.add(staged.messageId);
	cursors.set(turn.sessionId, { turnId, processedIds });

	const currentContext = staged?.agentContext ?? baseContext;
	if (!currentContext?.memberSnapshot) return undefined;
	const currentSnapshot = canonicalMemberSnapshot(currentContext.memberSnapshot);
	let previousSnapshot: AgentMemberSnapshot | undefined;
	try {
		previousSnapshot = latestHistoryMemberSnapshot(context, turn.scope.target);
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "session history could not be read");
	}
	for (const item of priorPending) {
		if (item.agentContext.memberSnapshot) {
			previousSnapshot = canonicalMemberSnapshot(item.agentContext.memberSnapshot);
		}
	}
	if (equalMemberSnapshots(previousSnapshot, currentSnapshot)) return undefined;
	const content = formatMemberContext(turn.scope.target, currentSnapshot);
	return { text: event.text.length === 0 ? content : `${event.text}\n\n${content}` };
}

async function beforeAgentStart(
	event: ExtensionEvent,
	context: ExtensionContext,
): Promise<{
	message: {
		customType: string;
		content: string;
		display: false;
		details: Record<string, unknown>;
	};
} | undefined> {
	const turn = await turnSidecar(event, context);
	if (!turn) return undefined;
	const path = contextSidecarPath(turn.scope.root, turn.sessionId);
	const compacted = latestCompactionContext(context, turn.scope, turn.sessionId);
	if (compacted) {
		await writeCompactedContextReceipt(path, compacted);
		if (
			compacted.snapshot.version > turn.sidecar.snapshot.version
			|| (
				compacted.snapshot.version === turn.sidecar.snapshot.version
				&& compacted.snapshot.present === turn.sidecar.snapshot.present
				&& compacted.snapshot.content === turn.sidecar.snapshot.content
			)
		) {
			if (
				turn.sidecar.injectionDecision !== "unchanged"
				&& sameGroupContext(compacted.snapshot, turn.sidecar.snapshot)
			) {
				await writePreparedReceipt(path, preparedReceiptFor(turn.sidecar));
			}
			return undefined;
		}
		if (compacted.snapshot.version === turn.sidecar.snapshot.version) {
			throw new Error("同版本群说明与已提交压缩快照内容不一致");
		}
	}
	if (turn.sidecar.injectionDecision === "unchanged") return undefined;
	const content = formatSharedContext(
		turn.sidecar.snapshot,
		turn.sidecar.injectionDecision,
		turn.sidecar.previousVersion,
	);
	if (hasPersistentSharedContext(context, content, turn.scope, turn.sessionId)) {
		await writePreparedReceipt(path, preparedReceiptFor(turn.sidecar));
		return undefined;
	}
	const rendered = await prepareSharedContext(turn.scope, turn.sidecar, context);
	return {
		message: {
			customType: "omp-channel-octo-context",
			content: rendered,
			display: false,
			details: managedContextIdentity(turn.scope, turn.sessionId),
		},
	};
}

async function sessionBeforeCompact(
	event: ExtensionEvent,
	context: ExtensionContext,
	freshSnapshots: Map<string, FreshCompactionSnapshot>,
): Promise<{ cancel: true } | undefined> {
	let paths: FreshContextReadPaths | undefined;
	let identity: FreshContextReadIdentity | undefined;
	try {
		const turn = await turnSidecar(event, context);
		if (!turn) return undefined;
		const turnId = nonEmptyString(turn.sidecar.turnId, "context turn ID");
		freshSnapshots.delete(turn.sessionId);
		const requestId = randomUUID();
		identity = {
			schemaVersion: FRESH_CONTEXT_READ_SCHEMA_VERSION,
			requestId,
			botUid: turn.scope.botUid,
			target: { ...turn.scope.target },
			sessionId: turn.sessionId,
			turnId,
		};
		paths = freshContextReadPaths(turn.scope.root, turnId, requestId);
		if (turn.scope.target.channelType === 1) {
			freshSnapshots.set(turn.sessionId, {
				turnId,
				requestId,
				snapshot: turn.sidecar.snapshot,
			});
			await persistPendingCompaction(turn, context, requestId, turn.sidecar.snapshot);
			return undefined;
		}

		const signal = eventSignal(event);
		await writeFreshContextReadFile(paths.request, identity);
		const waitSignal = AbortSignal.any([signal, AbortSignal.timeout(FRESH_CONTEXT_WAIT_TIMEOUT_MS)]);
		const response = await waitForFreshResponse(paths.response, waitSignal);
		const snapshot = freshSnapshotFromResponse(response, identity);
		if (signal.aborted) throw signal.reason ?? new Error("fresh compaction read was cancelled");
		freshSnapshots.set(turn.sessionId, { turnId, requestId, snapshot });
		await persistPendingCompaction(turn, context, requestId, snapshot);
		return undefined;
	} catch {
		if (paths && identity) {
			await writeFreshContextReadFile(paths.cancel, identity).catch(() => {});
		}
		console.error("[context] fresh compaction context unavailable; compaction will be cancelled");
		return { cancel: true };
	} finally {
		if (paths) {
			await rm(paths.request, { force: true }).catch(() => {});
			await rm(paths.response, { force: true }).catch(() => {});
		}
	}
}

async function sessionCompacting(
	event: ExtensionEvent,
	context: ExtensionContext,
	freshSnapshots: Map<string, FreshCompactionSnapshot>,
): Promise<{
	context: string[];
	preserveData: Record<string, unknown>;
} | undefined> {
	const turn = await turnSidecar(event, context);
	if (!turn) return undefined;
	const fresh = freshSnapshots.get(turn.sessionId);
	if (!fresh || fresh.turnId !== turn.sidecar.turnId) {
		return abortAndThrow(context, "fresh compaction snapshot is unavailable");
	}
	freshSnapshots.delete(turn.sessionId);
	return {
		context: [formatCompactionContext(turn.sidecar, fresh.snapshot)],
		preserveData: {
			ompChannelOctoContext: {
				schemaVersion: FRESH_CONTEXT_READ_SCHEMA_VERSION,
				requestId: fresh.requestId,
				botUid: turn.sidecar.botUid,
				target: { ...turn.sidecar.target },
				sessionId: turn.sidecar.sessionId,
				turnId: fresh.turnId,
				attemptId: turn.sidecar.attemptId,
				snapshot: fresh.snapshot,
			} satisfies CompactedContextReceipt,
		},
	};
}


export default function contextExtension(api: ExtensionApi): void {
	const freshSnapshots = new Map<string, FreshCompactionSnapshot>();
	const inputCursors = new Map<string, InputCursor>();
	const persistentContextRequests = new Set<string>();
	api.on("before_agent_start", beforeAgentStart);
	api.on("input", (event, context) => transformUserInput(event, context, inputCursors));
	api.on("context", async (event, context) => {
		const turn = await turnSidecar(event, context);
		if (!turn) return undefined;
		if (!Array.isArray(event.messages)) return abortAndThrow(context, "context messages are invalid");
		const projection = projectManagedContext(event.messages as unknown[], turn.scope, turn.sessionId);
		const messages = projection.messages;
		const path = contextSidecarPath(turn.scope.root, turn.sessionId);
		let compacted = latestCompactionContext(context, turn.scope, turn.sessionId);
		if (!compacted) {
			const pending = committedPendingCompaction(context, turn.sidecar.pendingCompaction);
			if (pending) {
				await commitPendingCompaction(path, pending);
				compacted = pending;
			}
		}
		if (compacted) {
			await writeCompactedContextReceipt(path, compacted);
			const sameVersion = compacted.snapshot.version === turn.sidecar.snapshot.version;
			const sameSnapshot = sameGroupContext(compacted.snapshot, turn.sidecar.snapshot);
			if (sameVersion && !sameSnapshot) {
				return abortAndThrow(context, "同版本群说明与已提交压缩快照内容不一致");
			}
			if (compacted.snapshot.version >= turn.sidecar.snapshot.version) {
				if (turn.sidecar.injectionDecision !== "unchanged" && sameSnapshot) {
					await writePreparedReceipt(path, preparedReceiptFor(turn.sidecar));
				}
				if (turn.scope.target.channelType !== 1) {
					let summaryIndex = -1;
					for (let index = 0; index < messages.length; index += 1) {
						const message = messages[index];
						if (isContextSidecarObject(message) && message.role === "compactionSummary") {
							summaryIndex = index;
						}
					}
					if (summaryIndex >= 0) {
						const content = formatSharedContext(compacted.snapshot, "unchanged");
						const alreadyAfterSummary = messages.slice(summaryIndex + 1).some(message =>
							isContextSidecarObject(message)
							&& message.role === "custom"
							&& message.customType === "omp-channel-octo-context"
							&& userMessageText(message.content) === content,
						);
						if (!alreadyAfterSummary) {
							const contextMessage = {
								role: "custom",
								customType: "omp-channel-octo-context",
								content,
								display: false,
								details: managedContextIdentity(turn.scope, turn.sessionId),
							};
							return {
								messages: [
									...messages.slice(0, summaryIndex + 1),
									contextMessage,
									...messages.slice(summaryIndex + 1),
								],
							};
						}
					}
				}
				return projection.changed ? { messages } : undefined;
			}
		}
		if (turn.sidecar.injectionDecision !== "unchanged" && turn.scope.target.channelType !== 1) {
			const content = formatSharedContext(
				turn.sidecar.snapshot,
				turn.sidecar.injectionDecision,
				turn.sidecar.previousVersion,
			);
			const persistent = hasPersistentSharedContext(context, content, turn.scope, turn.sessionId);
			const inRequest = messages.some(message =>
				isContextSidecarObject(message)
				&& message.role === "custom"
				&& message.customType === "omp-channel-octo-context"
				&& userMessageText(message.content) === content
				&& managedContextMatches(message.details, turn.scope, turn.sessionId) !== false,
			);
			const customMessage = {
				role: "custom",
				customType: "omp-channel-octo-context",
				content,
				display: false,
				details: managedContextIdentity(turn.scope, turn.sessionId),
			};
			const persistenceKey = `${turn.sessionId}\u0000${turn.sidecar.attemptId}`;
			const persistOnce = (): void => {
				if (persistent || turn.sidecar.prepared || persistentContextRequests.has(persistenceKey)) return;
				if (!api.sendMessage) return abortAndThrow(context, "persistent context message API is unavailable");
				persistentContextRequests.add(persistenceKey);
				try {
					api.sendMessage(
						{
							customType: customMessage.customType,
							content,
							display: false,
							details: customMessage.details,
						},
						{ triggerTurn: false, deliverAs: "steer" },
					);
				} catch (error) {
					persistentContextRequests.delete(persistenceKey);
					throw error;
				}
			};
			if (persistent || inRequest) {
				persistOnce();
				await writePreparedReceipt(path, preparedReceiptFor(turn.sidecar));
				return projection.changed ? { messages } : undefined;
			}
			persistOnce();
			await writePreparedReceipt(path, preparedReceiptFor(turn.sidecar));
			let insertIndex = 0;
			for (let index = 0; index < messages.length; index += 1) {
				const message = messages[index];
				if (isContextSidecarObject(message) && message.role === "user") insertIndex = index + 1;
			}
			return {
				messages: [
					...messages.slice(0, insertIndex),
					customMessage,
					...messages.slice(insertIndex),
				],
			};
		}
		return projection.changed ? { messages } : undefined;
	});
	api.on("session_before_compact", (event, context) =>
		sessionBeforeCompact(event, context, freshSnapshots));
	api.on("session.compacting", (event, context) =>
		sessionCompacting(event, context, freshSnapshots));
}
