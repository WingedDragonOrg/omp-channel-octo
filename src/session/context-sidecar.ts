import { randomUUID, createHash } from "node:crypto";
import { chmod, mkdir, open, readFile, rename, rm, unlink, type FileHandle } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import type { ChatTarget, ContextInjection, GroupContext } from "../contracts.ts";
import { agentContextFromUnknown, type AgentContext } from "./agent-context.ts";

export const CONTEXT_SIDECAR_SCHEMA_VERSION = 1 as const;

export type InjectionDecision = ContextInjection["injectionDecision"];

export interface ContextSidecarMarker {
  phase: "pending" | "committed";
  botUid: string;
  target: ChatTarget;
  sessionId: string;
  version: number;
  attemptId: string;
}

export interface ContextPreparedReceipt {
  botUid: string;
  target: ChatTarget;
  sessionId: string;
  version: number;
  attemptId: string;
  preparedAt?: string;
}

export interface ContextSidecar {
  schemaVersion: typeof CONTEXT_SIDECAR_SCHEMA_VERSION;
  botUid: string;
  target: ChatTarget;
  sessionId: string;
  turnId?: string;
  snapshot: GroupContext;
  injectionDecision: InjectionDecision;
  attemptId: string;
  marker: ContextSidecarMarker;
  previousVersion?: number;
  prepared?: ContextPreparedReceipt;
  agentContext?: AgentContext;
  compactedContext?: CompactedContextReceipt;
  pendingCompaction?: PendingCompactionContext;
}

export interface StagedAgentContext {
  messageId: string;
  text: string;
  agentContext: AgentContext;
}

export interface AgentContextOverlay {
  botUid: string;
  target: ChatTarget;
  sessionId: string;
  turnId: string;
  attemptId: string;
  agentContext: AgentContext;
  pending: StagedAgentContext[];
}

const SIDECAR_FILE_PREFIX = "context-";
const SIDECAR_FILE_SUFFIX = ".json";
const BOUND_SESSION_FILE = "bound-session.json";
const AGENT_CONTEXT_OVERLAY_SCHEMA_VERSION = 1 as const;
const AGENT_CONTEXT_OVERLAY_SUFFIX = ".agent-context.json";

/**
 * The session the host is currently driving inside this scope.  One worker
 * process serves one chat binding, but OMP opens further sessions in it for
 * subagents and eval children; only this session is a channel turn.
 */
export interface BoundContextSession {
  botUid: string;
  target: ChatTarget;
  sessionId: string;
}
const FRESH_CONTEXT_READ_UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
export const FRESH_CONTEXT_READ_SCHEMA_VERSION = 1 as const;

export interface FreshContextReadIdentity {
  schemaVersion: typeof FRESH_CONTEXT_READ_SCHEMA_VERSION;
  requestId: string;
  botUid: string;
  target: ChatTarget;
  sessionId: string;
  turnId: string;
}
export interface CompactedContextReceipt extends FreshContextReadIdentity {
  attemptId: string;
  snapshot: GroupContext;
}
export interface PendingCompactionContext extends CompactedContextReceipt {
  baseLeafId: string | null;
}

export type FreshContextReadResponse = FreshContextReadIdentity & (
  | { snapshot: GroupContext; error?: never }
  | { error: string; snapshot?: never }
);

export interface FreshContextReadPaths {
  directory: string;
  request: string;
  response: string;
  cancel: string;
}

export function freshContextReadPaths(
  sidecarRoot: string,
  turnId: string,
  requestId: string,
): FreshContextReadPaths {
  if (!FRESH_CONTEXT_READ_UUID_PATTERN.test(turnId) || !FRESH_CONTEXT_READ_UUID_PATTERN.test(requestId)) {
    throw new Error("fresh context read identity is invalid");
  }
  const directory = join(sidecarRoot, "fresh-context", turnId);
  const prefix = join(directory, requestId);
  return {
    directory,
    request: `${prefix}.request.json`,
    response: `${prefix}.response.json`,
    cancel: `${prefix}.cancel`,
  };
}

export async function writeFreshContextReadFile(path: string, value: unknown): Promise<void> {
  await atomicWriteJson(path, value);
}

export function isContextSidecarObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requiredNonEmptyString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`context sidecar ${field} is invalid`);
  }
  return value;
}

function safeVersion(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`context sidecar ${field} is invalid`);
  }
  return value as number;
}

function targetFromUnknown(value: unknown, field: string): ChatTarget {
  if (!isContextSidecarObject(value)
    || (value.channelType !== 1 && value.channelType !== 2 && value.channelType !== 5)
    || typeof value.channelId !== "string"
    || value.channelId.length === 0) {
    throw new Error(`context sidecar ${field} is invalid`);
  }
  return { channelType: value.channelType, channelId: value.channelId };
}

function snapshotFromUnknown(value: unknown): GroupContext {
  if (!isContextSidecarObject(value)
    || typeof value.content !== "string"
    || typeof value.present !== "boolean") {
    throw new Error("context sidecar snapshot is invalid");
  }
  const version = safeVersion(value.version, "snapshot version");
  const updatedAt = value.updatedAt === undefined || value.updatedAt === null
    ? null
    : typeof value.updatedAt === "string"
      ? value.updatedAt
      : null;
  const updatedBy = value.updatedBy === undefined
    ? ""
    : typeof value.updatedBy === "string"
      ? value.updatedBy
      : "";
  return {
    present: value.present,
    content: value.content,
    version,
    updatedAt,
    updatedBy,
  };
}

function markerFromUnknown(value: unknown): ContextSidecarMarker {
  if (!isContextSidecarObject(value)
    || (value.phase !== "pending" && value.phase !== "committed")) {
    throw new Error("context sidecar marker is invalid");
  }
  return {
    phase: value.phase,
    botUid: requiredNonEmptyString(value.botUid, "marker botUid"),
    target: targetFromUnknown(value.target, "marker target"),
    sessionId: requiredNonEmptyString(value.sessionId, "marker sessionId"),
    version: safeVersion(value.version, "marker version"),
    attemptId: requiredNonEmptyString(value.attemptId, "marker attemptId"),
  };
}

function preparedFromUnknown(value: unknown): ContextPreparedReceipt {
  if (!isContextSidecarObject(value)) throw new Error("context sidecar prepared receipt is invalid");
  const preparedAt = value.preparedAt === undefined
    ? undefined
    : typeof value.preparedAt === "string"
      ? value.preparedAt
      : undefined;
  return {
    botUid: requiredNonEmptyString(value.botUid, "prepared botUid"),
    target: targetFromUnknown(value.target, "prepared target"),
    sessionId: requiredNonEmptyString(value.sessionId, "prepared sessionId"),
    version: safeVersion(value.version, "prepared version"),
    attemptId: requiredNonEmptyString(value.attemptId, "prepared attemptId"),
    ...(preparedAt === undefined ? {} : { preparedAt }),
  };
}
export function compactedContextFromUnknown(value: unknown): CompactedContextReceipt {
  if (!isContextSidecarObject(value) || value.schemaVersion !== FRESH_CONTEXT_READ_SCHEMA_VERSION) {
    throw new Error("context sidecar compacted snapshot is invalid");
  }
  const requestId = requiredNonEmptyString(value.requestId, "compaction requestId");
  const turnId = requiredNonEmptyString(value.turnId, "compaction turnId");
  if (!FRESH_CONTEXT_READ_UUID_PATTERN.test(requestId) || !FRESH_CONTEXT_READ_UUID_PATTERN.test(turnId)) {
    throw new Error("context sidecar compacted snapshot identity is invalid");
  }
  return {
    schemaVersion: FRESH_CONTEXT_READ_SCHEMA_VERSION,
    requestId,
    botUid: requiredNonEmptyString(value.botUid, "compaction botUid"),
    target: targetFromUnknown(value.target, "compaction target"),
    sessionId: requiredNonEmptyString(value.sessionId, "compaction sessionId"),
    turnId,
    attemptId: requiredNonEmptyString(value.attemptId, "compaction attemptId"),
    snapshot: snapshotFromUnknown(value.snapshot),
  };
}

function pendingCompactionFromUnknown(value: unknown): PendingCompactionContext {
  const compacted = compactedContextFromUnknown(value);
  if (!isContextSidecarObject(value)
    || (value.baseLeafId !== null && (typeof value.baseLeafId !== "string" || value.baseLeafId.length === 0))) {
    throw new Error("context sidecar pending compaction identity is invalid");
  }
  return { ...compacted, baseLeafId: value.baseLeafId as string | null };
}


function assertSidecarIdentity(sidecar: ContextSidecar): void {
  if (sidecar.schemaVersion !== CONTEXT_SIDECAR_SCHEMA_VERSION) {
    throw new Error("context sidecar schema version is unsupported");
  }
  requiredNonEmptyString(sidecar.botUid, "botUid");
  requiredNonEmptyString(sidecar.sessionId, "sessionId");
  requiredNonEmptyString(sidecar.attemptId, "attemptId");
  if (sidecar.turnId !== undefined) requiredNonEmptyString(sidecar.turnId, "turnId");
  const target = targetFromUnknown(sidecar.target, "target");
  const snapshot = snapshotFromUnknown(sidecar.snapshot);
  if (sidecar.injectionDecision !== "initial"
    && sidecar.injectionDecision !== "version-change"
    && sidecar.injectionDecision !== "unchanged") {
    throw new Error("context sidecar injection decision is invalid");
  }
  const marker = markerFromUnknown(sidecar.marker);
  if (marker.botUid !== sidecar.botUid
    || marker.sessionId !== sidecar.sessionId
    || marker.version !== snapshot.version
    || marker.attemptId !== sidecar.attemptId
    || marker.target.channelType !== target.channelType
    || marker.target.channelId !== target.channelId) {
    throw new Error("context sidecar marker does not match its snapshot identity");
  }
  if (sidecar.previousVersion !== undefined) safeVersion(sidecar.previousVersion, "previous version");
  if (sidecar.prepared !== undefined) {
    const prepared = preparedFromUnknown(sidecar.prepared);
    if (!preparedReceiptMatches(sidecar, prepared)) {
      throw new Error("context sidecar prepared receipt does not match its identity");
    }
  }
  if (sidecar.compactedContext !== undefined) {
    const compacted = compactedContextFromUnknown(sidecar.compactedContext);
    if (
      compacted.botUid !== sidecar.botUid
      || compacted.sessionId !== sidecar.sessionId
      || compacted.target.channelType !== target.channelType
      || compacted.target.channelId !== target.channelId
    ) {
      throw new Error("context sidecar compacted snapshot scope mismatch");
    }
  }
  if (sidecar.pendingCompaction !== undefined) {
    const pending = pendingCompactionFromUnknown(sidecar.pendingCompaction);
    if (
      pending.botUid !== sidecar.botUid
      || pending.sessionId !== sidecar.sessionId
      || pending.target.channelType !== target.channelType
      || pending.target.channelId !== target.channelId
    ) {
      throw new Error("context sidecar pending compaction scope mismatch");
    }
  }
}

function sidecarForValidation(value: unknown): ContextSidecar {
  if (!isContextSidecarObject(value)) throw new Error("context sidecar is not an object");
  const snapshot = snapshotFromUnknown(value.snapshot);
  const target = targetFromUnknown(value.target, "target");
  const marker = markerFromUnknown(value.marker);
  const prepared = value.prepared === undefined ? undefined : preparedFromUnknown(value.prepared);
  const agentContext = value.agentContext === undefined
    ? undefined
    : agentContextFromUnknown(value.agentContext);
  const compactedContext = value.compactedContext === undefined
    ? undefined
    : compactedContextFromUnknown(value.compactedContext);
  const pendingCompaction = value.pendingCompaction === undefined
    ? undefined
    : pendingCompactionFromUnknown(value.pendingCompaction);
  const sidecar: ContextSidecar = {
    schemaVersion: value.schemaVersion as typeof CONTEXT_SIDECAR_SCHEMA_VERSION,
    botUid: requiredNonEmptyString(value.botUid, "botUid"),
    target,
    sessionId: requiredNonEmptyString(value.sessionId, "sessionId"),
    ...(value.turnId === undefined ? {} : { turnId: requiredNonEmptyString(value.turnId, "turnId") }),
    snapshot,
    injectionDecision: value.injectionDecision as InjectionDecision,
    attemptId: requiredNonEmptyString(value.attemptId, "attemptId"),
    marker,
    ...(value.previousVersion === undefined ? {} : { previousVersion: safeVersion(value.previousVersion, "previous version") }),
    ...(prepared === undefined ? {} : { prepared }),
    ...(agentContext === undefined ? {} : { agentContext }),
    ...(compactedContext === undefined ? {} : { compactedContext }),
    ...(pendingCompaction === undefined ? {} : { pendingCompaction }),
  };
  assertSidecarIdentity(sidecar);
  return sidecar;
}

export function emptyGroupContext(version = 0): GroupContext {
  if (!Number.isSafeInteger(version) || version < 0) throw new Error("group context version is invalid");
  return { present: false, content: "", version, updatedAt: null, updatedBy: "" };
}

/** Normalize an optional transport value without allowing null to enter group task state. */
export type GroupContextInput = Omit<GroupContext, "present"> & Partial<Pick<GroupContext, "present">>;
export function normalizeGroupContext(value: GroupContextInput | null | undefined): GroupContext {
  if (!value) return emptyGroupContext();
  const content = typeof value.content === "string" ? value.content : "";
  const version = Number.isSafeInteger(value.version) && value.version >= 0 ? value.version : 0;
  return {
    present: content.length > 0,
    content,
    version,
    updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : null,
    updatedBy: typeof value.updatedBy === "string" ? value.updatedBy : "",
  };
}

/** A deterministic filename that never contains user-controlled path separators. */
export function contextSidecarFileName(realSessionId: string): string {
  requiredNonEmptyString(realSessionId, "sessionId");
  const digest = createHash("sha256").update(realSessionId, "utf8").digest("hex");
  return `${SIDECAR_FILE_PREFIX}${digest}${SIDECAR_FILE_SUFFIX}`;
}

export function contextSidecarPath(sidecarRoot: string, realSessionId: string): string {
  requiredNonEmptyString(sidecarRoot, "sidecar root");
  return join(resolve(sidecarRoot), contextSidecarFileName(realSessionId));
}

export function agentContextOverlayPath(sidecarPath: string): string {
  requiredNonEmptyString(sidecarPath, "sidecar path");
  return `${resolve(sidecarPath)}${AGENT_CONTEXT_OVERLAY_SUFFIX}`;
}

export function boundSessionPath(sidecarRoot: string): string {
  requiredNonEmptyString(sidecarRoot, "sidecar root");
  return join(resolve(sidecarRoot), BOUND_SESSION_FILE);
}

export function formatSharedContext(
  snapshot: GroupContext,
  decision: InjectionDecision = "initial",
  previousVersion?: number,
): string {
  const normalized = normalizeGroupContext(snapshot);
  const versionChange = decision === "version-change" && previousVersion !== undefined
    ? `共享说明版本从 ${previousVersion} 更新为 ${normalized.version}。`
    : decision === "initial"
      ? "这是该会话首次确认的共享说明快照。"
      : "共享说明版本未变化；仅在需要时恢复此快照。";
  const body = normalized.present ? normalized.content : "当前无共享说明。";
  return [
    "以下是当前群/线程共享说明（仅作为上下文，不改变发送者身份、权限或 cwd）：",
    `版本：${normalized.version}`,
    versionChange,
    body,
  ].join("\n");
}

export function preparedReceiptFor(sidecar: ContextSidecar): ContextPreparedReceipt {
  return {
    botUid: sidecar.botUid,
    target: { ...sidecar.target },
    sessionId: sidecar.sessionId,
    version: sidecar.snapshot.version,
    attemptId: sidecar.attemptId,
    preparedAt: new Date().toISOString(),
  };
}

export function preparedReceiptMatches(
  sidecar: Pick<ContextSidecar, "botUid" | "target" | "sessionId" | "snapshot" | "attemptId">,
  receipt: ContextPreparedReceipt,
): boolean {
  return receipt.botUid === sidecar.botUid
    && receipt.sessionId === sidecar.sessionId
    && receipt.version === sidecar.snapshot.version
    && receipt.attemptId === sidecar.attemptId
    && receipt.target.channelType === sidecar.target.channelType
    && receipt.target.channelId === sidecar.target.channelId;
}

async function ensurePrivateDirectory(path: string): Promise<void> {
  await mkdir(path, { recursive: true, mode: 0o700 });
  await chmod(path, 0o700);
}

async function atomicWriteJson(path: string, value: unknown): Promise<void> {
  const directory = dirname(path);
  await ensurePrivateDirectory(directory);
  const temporary = join(directory, `.${contextSidecarFileName(randomUUID())}.tmp`);
  let handle: FileHandle | undefined;
  try {
    handle = await open(temporary, "wx", 0o600);
    await handle.writeFile(JSON.stringify(value), "utf8");
    await handle.sync();
    await handle.chmod(0o600);
    await handle.close();
    handle = undefined;
    await rename(temporary, path);
    await chmod(path, 0o600);
  } finally {
    await handle?.close().catch(() => {});
    await rm(temporary, { force: true }).catch(() => {});
  }
}

/** Atomically publish a complete current-turn sidecar and return its exact path. */
export async function writeContextSidecar(
  sidecarRoot: string,
  sidecar: ContextSidecar,
): Promise<string> {
  assertSidecarIdentity(sidecar);
  const root = resolve(sidecarRoot);
  await ensurePrivateDirectory(root);
  const path = contextSidecarPath(root, sidecar.sessionId);
  await atomicWriteJson(path, sidecar);
  await removeAgentContextOverlay(path).catch(() => {});
  return path;
}

export async function writeContextSidecarFile(path: string, sidecar: ContextSidecar): Promise<void> {
  assertSidecarIdentity(sidecar);
  await atomicWriteJson(resolve(path), sidecar);
}

/**
 * Publish the session this scope's next turn belongs to before prompting, so
 * the extension can tell a channel turn apart from a sub-session OMP opened in
 * the same process.
 */
export async function writeBoundSession(
  sidecarRoot: string,
  bound: BoundContextSession,
): Promise<string> {
  const path = boundSessionPath(sidecarRoot);
  await atomicWriteJson(path, {
    botUid: requiredNonEmptyString(bound.botUid, "bound botUid"),
    target: targetFromUnknown(bound.target, "bound target"),
    sessionId: requiredNonEmptyString(bound.sessionId, "bound sessionId"),
  });
  return path;
}

/** The published bound session, or `undefined` when this scope has none. */
export async function readBoundSession(sidecarRoot: string): Promise<BoundContextSession | undefined> {
  let value: unknown;
  try {
    value = JSON.parse(await readFile(boundSessionPath(sidecarRoot), "utf8")) as unknown;
  } catch {
    return undefined;
  }
  if (!isContextSidecarObject(value)) return undefined;
  try {
    return {
      botUid: requiredNonEmptyString(value.botUid, "bound botUid"),
      target: targetFromUnknown(value.target, "bound target"),
      sessionId: requiredNonEmptyString(value.sessionId, "bound sessionId"),
    };
  } catch {
    return undefined;
  }
}

export async function readContextSidecar(path: string): Promise<ContextSidecar> {
  let value: unknown;
  try {
    value = JSON.parse(await readFile(path, "utf8")) as unknown;
  } catch (error) {
    throw new Error(`context sidecar could not be read: ${error instanceof Error ? error.message : "invalid JSON"}`);
  }
  return sidecarForValidation(value);
}

export async function readAgentContextOverlay(
  path: string,
  sidecar: ContextSidecar,
): Promise<AgentContextOverlay | undefined> {
  let value: unknown;
  try {
    value = JSON.parse(await readFile(agentContextOverlayPath(path), "utf8")) as unknown;
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return undefined;
    throw new Error(`context sidecar agent context could not be read: ${error instanceof Error ? error.message : "invalid JSON"}`);
  }
  if (!isContextSidecarObject(value) || value.schemaVersion !== AGENT_CONTEXT_OVERLAY_SCHEMA_VERSION) {
    throw new Error("context sidecar agent context is invalid");
  }
  const botUid = requiredNonEmptyString(value.botUid, "agent context botUid");
  const target = targetFromUnknown(value.target, "agent context target");
  const sessionId = requiredNonEmptyString(value.sessionId, "agent context sessionId");
  const turnId = requiredNonEmptyString(value.turnId, "agent context turnId");
  const attemptId = requiredNonEmptyString(value.attemptId, "agent context attemptId");
  if (
    botUid !== sidecar.botUid
    || sessionId !== sidecar.sessionId
    || target.channelType !== sidecar.target.channelType
    || target.channelId !== sidecar.target.channelId
  ) {
    throw new Error("context sidecar agent context identity does not match");
  }
  if (sidecar.turnId === undefined || turnId !== sidecar.turnId || attemptId !== sidecar.attemptId) {
    return undefined;
  }
  if (!Array.isArray(value.pending)) throw new Error("context sidecar staged agent context is invalid");
  const pending = value.pending.map((entry): StagedAgentContext => {
    if (!isContextSidecarObject(entry)) throw new Error("context sidecar staged agent context is invalid");
    return {
      messageId: requiredNonEmptyString(entry.messageId, "staged messageId"),
      text: requiredNonEmptyString(entry.text, "staged text"),
      agentContext: agentContextFromUnknown(entry.agentContext),
    };
  });
  return {
    botUid,
    target,
    sessionId,
    turnId,
    attemptId,
    agentContext: agentContextFromUnknown(value.agentContext),
    pending,
  };
}

export async function writeAgentContextOverlay(
  path: string,
  sidecar: ContextSidecar,
  agentContext: AgentContext,
  pending: readonly StagedAgentContext[],
): Promise<void> {
  assertSidecarIdentity(sidecar);
  if (sidecar.turnId === undefined) throw new Error("context sidecar turn ID is missing");
  const normalizedPending = pending.map((entry): StagedAgentContext => {
    if (typeof entry.messageId !== "string" || entry.messageId.length === 0 || typeof entry.text !== "string") {
      throw new Error("context sidecar staged agent context is invalid");
    }
    return {
      messageId: entry.messageId,
      text: entry.text,
      agentContext: agentContextFromUnknown(entry.agentContext),
    };
  });
  await atomicWriteJson(agentContextOverlayPath(path), {
    schemaVersion: AGENT_CONTEXT_OVERLAY_SCHEMA_VERSION,
    botUid: sidecar.botUid,
    target: { ...sidecar.target },
    sessionId: sidecar.sessionId,
    turnId: sidecar.turnId,
    attemptId: sidecar.attemptId,
    agentContext: agentContextFromUnknown(agentContext),
    pending: normalizedPending,
  });
}

export async function removeAgentContextOverlay(path: string): Promise<void> {
  await removeFileIfPresent(agentContextOverlayPath(path));
}

/** Add only the extension's matching receipt; snapshot and marker remain unchanged. */
export async function writePreparedReceipt(
  path: string,
  receipt?: ContextPreparedReceipt,
): Promise<ContextPreparedReceipt> {
  const current = await readContextSidecar(path);
  const next = receipt ?? preparedReceiptFor(current);
  if (!preparedReceiptMatches(current, next)) {
    throw new Error("context sidecar prepared receipt does not match its identity");
  }
  if (current.prepared !== undefined) {
    if (!preparedReceiptMatches(current, current.prepared) || current.prepared.attemptId !== next.attemptId) {
      throw new Error("context sidecar already has a different prepared receipt");
    }
    return current.prepared;
  }
  const updated: ContextSidecar = { ...current, prepared: next };
  await writeContextSidecarFile(path, updated);
  return next;
}
export async function writeCompactedContextReceipt(
  path: string,
  value: unknown,
): Promise<void> {
  const current = await readContextSidecar(path);
  const compactedContext = compactedContextFromUnknown(value);
  if (
    compactedContext.botUid !== current.botUid
    || compactedContext.sessionId !== current.sessionId
    || compactedContext.target.channelType !== current.target.channelType
    || compactedContext.target.channelId !== current.target.channelId
  ) {
    throw new Error("compaction receipt does not match the context sidecar scope");
  }
  const { pendingCompaction, ...rest } = current;
  await writeContextSidecarFile(
    path,
    pendingCompaction?.requestId === compactedContext.requestId
      ? { ...rest, compactedContext }
      : { ...current, compactedContext },
  );
}

export async function writePendingCompaction(
  path: string,
  value: PendingCompactionContext,
): Promise<void> {
  const current = await readContextSidecar(path);
  const pendingCompaction = pendingCompactionFromUnknown(value);
  if (
    pendingCompaction.botUid !== current.botUid
    || pendingCompaction.sessionId !== current.sessionId
    || pendingCompaction.target.channelType !== current.target.channelType
    || pendingCompaction.target.channelId !== current.target.channelId
  ) {
    throw new Error("pending compaction does not match the context sidecar scope");
  }
  await writeContextSidecarFile(path, { ...current, pendingCompaction });
}

export async function commitPendingCompaction(
  path: string,
  value: PendingCompactionContext,
): Promise<void> {
  const current = await readContextSidecar(path);
  const pending = current.pendingCompaction;
  if (
    !pending
    || pending.requestId !== value.requestId
    || pending.turnId !== value.turnId
    || pending.baseLeafId !== value.baseLeafId
  ) {
    throw new Error("pending compaction changed before commit");
  }
  const { pendingCompaction: _pendingCompaction, ...rest } = current;
  await writeContextSidecarFile(path, { ...rest, compactedContext: value });
}


async function removeFileIfPresent(path: string): Promise<void> {
  try {
    await unlink(path);
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) throw error;
  }
}
export async function removeContextSidecar(path: string): Promise<void> {
  const results = await Promise.allSettled([
    removeFileIfPresent(path),
    removeAgentContextOverlay(path),
  ]);
  const failure = results.find((result) => result.status === "rejected");
  if (failure?.status === "rejected") throw failure.reason;
}

export const clearContextSidecar = removeContextSidecar;
export const readSidecar = readContextSidecar;
export const writeSidecar = writeContextSidecar;
export const writePreparedContextReceipt = writePreparedReceipt;
