import { isAbsolute } from "node:path";
import {
	contextSidecarPath,
	formatSharedContext,
	preparedReceiptFor,
	readContextSidecar,
	writePreparedReceipt,
	type ContextSidecar,
} from "../context-sidecar.ts";

export { contextSidecarFileName } from "../context-sidecar.ts";

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
	on(event: "before_agent_start" | "session.compacting", handler: ExtensionHandler): void;
};

type ExtensionScope = {
	root: string;
	botUid: string;
	target: {
		channelType: 2 | 5;
		channelId: string;
	};
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
	if (parsedChannelType !== 2 && parsedChannelType !== 5) throw new Error("context extension target type is invalid");
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
	const snapshot = formatSharedContext(sidecar.snapshot, "unchanged");
	return [
		"压缩恢复边界（来自当前 turn 的 immutable 群/线程快照）：",
		`bot=${sidecar.botUid}`,
		`target=${sidecar.target.channelType}:${sidecar.target.channelId}`,
		`session=${sidecar.sessionId}`,
		`version=${sidecar.snapshot.version}`,
		snapshot,
	].join("\n");
}

async function beforeAgentStart(event: ExtensionEvent, context: ExtensionContext): Promise<{ systemPrompt: string[] } | undefined> {
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
	const sidecar = validateSidecar(await sidecarForScope(scope, sessionId, context), scope, sessionId, context);
	const originalSystemPrompt = systemPromptFromEvent(event, context);
	if (sidecar.injectionDecision === "unchanged") return { systemPrompt: [...originalSystemPrompt] };

	// The receipt is the extension's acknowledgement that it validated and
	// rendered this exact sidecar.  It is published before returning any body.
	const path = contextSidecarPath(scope.root, sessionId);
	try {
		const receipt = preparedReceiptFor(sidecar);
		await writePreparedReceipt(path, receipt);
	} catch (error) {
		return abortAndThrow(context, error instanceof Error ? error.message : "prepared receipt could not be written");
	}
	const formatted = formatSharedContext(sidecar.snapshot, sidecar.injectionDecision, sidecar.previousVersion);
	return { systemPrompt: [...originalSystemPrompt, formatted] };
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
	api.on("before_agent_start", beforeAgentStart);
	api.on("session.compacting", sessionCompacting);
}
