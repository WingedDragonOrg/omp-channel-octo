import { relative, isAbsolute, sep } from "node:path";
import { Database } from "bun:sqlite";
import type { ChatTarget, ThreadListenLevel, ThreadSummary } from "../contracts.ts";
import type {
  BindingRecord,
  ThreadWorkspaceCleanupInput,
  ThreadWorkspaceCleanupPhase,
  ThreadWorkspaceCleanupRecord,
  ThreadWorkspaceIntentInput,
  ThreadWorkspaceIntentRecord,
  ThreadWorkspaceIntentStatus,
  ThreadWorkspaceOperationInput,
  ThreadWorkspaceOperationRecord,
  ThreadWorkspaceOperationStatus,
  ThreadWorkspaceProjectRecord,
  ThreadWorkspaceRecord,
} from "./store-records.ts";
import { getBinding, ensureBindingIfMatches } from "./store-chats.ts";
import { getThreadListen, recordCreatedThreadListen } from "./store-thread-listen.ts";
import {
  allocateScheduleOrder,
  asNullableString,
  asNumber,
  asString,
  query,
  run,
  transaction,
} from "./store-schema.ts";
import { threadParentGroupId, threadShortId } from "../threads/thread-target.ts";

export function saveThreadWorkspace(db: Database, record: ThreadWorkspaceRecord): void {
  saveThreadWorkspaces(db, [record]);
}

export function saveThreadWorkspaces(db: Database, records: readonly ThreadWorkspaceRecord[]): void {
  if (records.length === 0) return;
  transaction(db, () => {
    const updatedAt = Date.now();
    for (const record of records) {
      run(
        db,
        `INSERT INTO thread_workspaces(
           bot_uid, channel_type, channel_id, layout, status, entry_path, workspace_root, projects, legacy_worktree_name, error, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(bot_uid, channel_id) DO UPDATE SET
           channel_type = excluded.channel_type,
           layout = excluded.layout,
           status = excluded.status,
           entry_path = excluded.entry_path,
           workspace_root = excluded.workspace_root,
           projects = excluded.projects,
           legacy_worktree_name = excluded.legacy_worktree_name,
           error = excluded.error,
           updated_at = excluded.updated_at`,
        [
          record.botUid,
          record.channelType,
          record.channelId,
          record.layout,
          record.status,
          record.entryPath,
          record.workspaceRoot ?? null,
          JSON.stringify(record.projects),
          record.legacyWorktreeName ?? null,
          record.error,
          updatedAt,
        ],
      );
    }
  });
}

export function getThreadWorkspace(
  db: Database,
  botUid: string,
  target: ChatTarget,
): ThreadWorkspaceRecord | null {
  const row = query(
    db,
    `SELECT bot_uid, channel_type, channel_id, layout, status, entry_path, workspace_root, projects, legacy_worktree_name, error
     FROM thread_workspaces WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?`,
    [botUid, target.channelType, target.channelId],
  )[0];
  return row ? threadWorkspaceFromRow(row) : null;
}

export function listThreadWorkspaces(db: Database, botUid: string): ThreadWorkspaceRecord[] {
  return query(
    db,
    `SELECT bot_uid, channel_type, channel_id, layout, status, entry_path, workspace_root, projects, legacy_worktree_name, error
     FROM thread_workspaces WHERE bot_uid = ? ORDER BY channel_id ASC`,
    [botUid],
  ).map(threadWorkspaceFromRow);
}

export function saveThreadWorkspaceIntent(db: Database, record: ThreadWorkspaceIntentInput): boolean {
  const kind = record.kind ?? "workspace";
  const creatorChannelType = record.creatorChannelType ?? null;
  const creatorChannelId = record.creatorChannelId ?? null;
  validateIntentCreator(record.groupId, kind, creatorChannelType, creatorChannelId);
  return transaction(db, () => {
    const existing = query(
      db,
      `SELECT bot_uid, group_id, intent_kind, creator_channel_type, creator_channel_id
       FROM thread_workspace_intents WHERE id = ?`,
      [record.id],
    )[0];
    if (existing) {
      if (asString(existing.bot_uid) !== record.botUid || asString(existing.group_id) !== record.groupId) {
        throw new Error("thread workspace intent id belongs to a different owner");
      }
      if (
        asString(existing.intent_kind) !== kind
        || nullableChannelType(existing.creator_channel_type) !== creatorChannelType
        || asNullableString(existing.creator_channel_id) !== creatorChannelId
      ) {
        throw new Error("thread workspace intent id belongs to a different creation source");
      }
      return false;
    }
    const now = Date.now();
    run(
      db,
      `INSERT INTO thread_workspace_intents(
         id, bot_uid, group_id, intent_kind, creator_channel_type, creator_channel_id,
         status, channel_ids, error, created_at, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.id,
        record.botUid,
        record.groupId,
        kind,
        creatorChannelType,
        creatorChannelId,
        record.status,
        JSON.stringify(record.channelIds),
        record.error ?? null,
        now,
        now,
      ],
    );
    return true;
  });
}

export function addThreadWorkspaceIntentChannel(db: Database, botUid: string, id: string, channelId: string): void {
  transaction(db, () => {
    const row = query(
      db,
      `SELECT status, channel_ids FROM thread_workspace_intents
       WHERE bot_uid = ? AND id = ?`,
      [botUid, id],
    )[0];
    if (!row) throw new Error("thread workspace intent does not exist");
    const status = threadWorkspaceIntentStatus(row.status);
    const channelIds = threadWorkspaceIntentChannels(row.channel_ids);
    if (channelIds.includes(channelId)) return;
    if (status !== "preparing") throw new Error("thread workspace intent is no longer preparing");
    channelIds.push(channelId);
    run(
      db,
      `UPDATE thread_workspace_intents SET channel_ids = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND status = 'preparing'`,
      [JSON.stringify(channelIds), Date.now(), botUid, id],
    );
  });
}

export function completeThreadCreatorIntent(
  db: Database,
  botUid: string,
  id: string,
  thread: ThreadSummary,
  listen: ThreadListenLevel,
): void {
  transaction(db, () => {
    const row = query(
      db,
      `SELECT id, bot_uid, group_id, intent_kind, creator_channel_type, creator_channel_id,
              status, channel_ids, error
       FROM thread_workspace_intents WHERE bot_uid = ? AND id = ?`,
      [botUid, id],
    )[0];
    if (!row) throw new Error("thread creator intent does not exist");
    const intent = threadWorkspaceIntentFromRow(row);
    if (intent.kind !== "thread-create" || intent.creatorChannelType === null || intent.creatorChannelId === null) {
      throw new Error("thread creator intent has no authoritative source");
    }
    if (
      thread.groupId !== intent.groupId
      || threadParentGroupId(thread.channelId) !== intent.groupId
      || threadShortId(thread.channelId) === null
    ) {
      throw new Error("created thread does not belong to its persisted source group");
    }
    if (intent.channelIds.length > 1 || (intent.channelIds.length === 1 && intent.channelIds[0] !== thread.channelId)) {
      throw new Error("thread creator intent is already associated with a different thread");
    }
    if (intent.status !== "preparing" && intent.status !== "complete") {
      throw new Error("thread creator intent is no longer recoverable");
    }
    recordCreatedThreadListen(db, {
      botUid,
      channelId: thread.channelId,
      creatorChannelType: intent.creatorChannelType,
      creatorChannelId: intent.creatorChannelId,
      threadName: thread.name,
      listen,
    });
    if (intent.status === "complete") return;
    run(
      db,
      `UPDATE thread_workspace_intents SET status = 'complete', channel_ids = ?, error = NULL, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND status = 'preparing'`,
      [JSON.stringify([thread.channelId]), Date.now(), botUid, id],
    );
  });
}

export function completeThreadWorkspaceIntent(db: Database, botUid: string, id: string): void {
  updateThreadWorkspaceIntent(db, botUid, id, "complete", null);
}

export function failThreadWorkspaceIntent(db: Database, botUid: string, id: string, error: string | null): void {
  updateThreadWorkspaceIntent(db, botUid, id, "failed", error);
}

export function getThreadWorkspaceIntent(
  db: Database,
  botUid: string,
  id: string,
): ThreadWorkspaceIntentRecord | null {
  const row = query(
    db,
    `SELECT id, bot_uid, group_id, intent_kind, creator_channel_type, creator_channel_id,
            status, channel_ids, error
     FROM thread_workspace_intents WHERE bot_uid = ? AND id = ?`,
    [botUid, id],
  )[0];
  return row ? threadWorkspaceIntentFromRow(row) : null;
}

export function listThreadWorkspaceIntents(
  db: Database,
  botUid: string,
  groupId: string,
): ThreadWorkspaceIntentRecord[] {
  return query(
    db,
    `SELECT id, bot_uid, group_id, intent_kind, creator_channel_type, creator_channel_id,
            status, channel_ids, error
     FROM thread_workspace_intents
     WHERE bot_uid = ? AND group_id = ?
     ORDER BY created_at ASC, id ASC`,
    [botUid, groupId],
  ).map(threadWorkspaceIntentFromRow);
}

export function isThreadWorkspaceIntentBlocked(db: Database, botUid: string, target: ChatTarget): boolean {
  if (target.channelType !== 5) return false;
  if (isThreadWorkspaceOperationBlocked(db, botUid, target) || isThreadWorkspaceCleanupBlocked(db, botUid, target)) {
    return true;
  }
  const fork = query(
    db,
    "SELECT id, phase FROM thread_forks WHERE bot_uid = ? AND thread_channel_id = ?",
    [botUid, target.channelId],
  )[0];
  if (fork) {
    if (fork.phase !== "ready") return true;
    const intent = getThreadWorkspaceIntent(db, botUid, asString(fork.id));
    if (intent?.status !== "complete") return true;
  }
  const separator = target.channelId.indexOf("____");
  if (separator <= 0) return false;
  const groupId = target.channelId.slice(0, separator);
  const intents = listThreadWorkspaceIntents(db, botUid, groupId)
    .map((intent) => finalizeWorkspaceIntentFromRecords(db, intent));
  if (intents.some(
    (intent) => intent.kind === "thread-create"
      && intent.status === "preparing"
      && intent.channelIds.includes(target.channelId),
  )) return true;
  if (intents.length === 0) return false;
  // Workspace rows govern their own readiness; without a row, an established binding lets an
  // explicit `/new` or `/move` recover when the ownership write for that thread failed.
  if (getThreadWorkspace(db, botUid, target) !== null) return false;
  const hasBinding = query(
    db,
    `SELECT 1 FROM chats WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? LIMIT 1`,
    [botUid, target.channelId],
  ).length > 0;
  if (hasBinding) return false;
  return intents.some((intent) =>
    intent.status !== "complete"
    && (intent.channelIds.includes(target.channelId) || intent.status === "preparing"),
  );
}

/**
 * If the process died after persisting every terminal workspace row but before finalizing its
 * intent, the rows are sufficient evidence to finish that commit. Missing or preparing rows keep
 * the intent open: a remote resource can still exist without a confirmed local ownership record.
 */
function finalizeWorkspaceIntentFromRecords(
  db: Database,
  intent: ThreadWorkspaceIntentRecord,
): ThreadWorkspaceIntentRecord {
  if (intent.kind !== "workspace" || intent.status !== "preparing" || intent.channelIds.length === 0) return intent;
  // Fork completion publishes its own ready record and intent in one transaction. Workspace rows
  // are not fork-owned evidence, and a read-side recovery must not finalize a fork intent early.
  if (query(db, "SELECT 1 FROM thread_forks WHERE bot_uid = ? AND id = ?", [intent.botUid, intent.id]).length > 0) {
    return intent;
  }
  const workspaces = intent.channelIds.map((channelId) =>
    getThreadWorkspace(db, intent.botUid, { channelType: 5, channelId }),
  );
  if (workspaces.some((workspace) =>
    workspace === null
    || workspace.status === "preparing"
    || (workspace.status === "ready" && workspace.projects.some((project) => !project.created))
  )) return intent;
  const failed = workspaces.find((workspace) => workspace?.status === "failed");
  const status = failed ? "failed" : "complete";
  if (status === "complete" && !threadWorkspaceIntentCreatorRecorded(db, intent)) return intent;
  const error = failed?.error ?? intent.error ?? null;
  updateThreadWorkspaceIntent(db, intent.botUid, intent.id, status, error);
  return { ...intent, status, error };
}
function threadWorkspaceIntentCreatorRecorded(
  db: Database,
  intent: ThreadWorkspaceIntentRecord,
): boolean {
  if (intent.creatorChannelType === null && intent.creatorChannelId === null) return true;
  if (intent.creatorChannelType === null || intent.creatorChannelId === null) return false;
  return intent.channelIds.every((channelId) => {
    const policy = getThreadListen(db, intent.botUid, { channelType: 5, channelId });
    return policy?.creatorChannelType === intent.creatorChannelType
      && policy.creatorChannelId === intent.creatorChannelId;
  });
}

function updateThreadWorkspaceIntent(
  db: Database,
  botUid: string,
  id: string,
  status: "complete" | "failed",
  error: string | null,
): void {
  transaction(db, () => updateThreadWorkspaceIntentInTransaction(db, botUid, id, status, error));
}

export function completeThreadWorkspaceIntentInTransaction(db: Database, botUid: string, id: string): void {
  updateThreadWorkspaceIntentInTransaction(db, botUid, id, "complete", null);
}

function updateThreadWorkspaceIntentInTransaction(
  db: Database,
  botUid: string,
  id: string,
  status: "complete" | "failed",
  error: string | null,
): void {
  const row = query(
    db,
    `SELECT status, group_id, channel_ids, intent_kind, creator_channel_type, creator_channel_id
     FROM thread_workspace_intents WHERE bot_uid = ? AND id = ?`,
    [botUid, id],
  )[0];
  if (!row) throw new Error("thread workspace intent does not exist");
  const current = threadWorkspaceIntentStatus(row.status);
  const intentKind = threadWorkspaceIntentKind(row.intent_kind);
  const channelIds = threadWorkspaceIntentChannels(row.channel_ids);
  if (intentKind === "thread-create" && (status !== "failed" || channelIds.length !== 0)) {
    throw new Error("thread creator intents can only complete with their authoritative listen record");
  }
  if (current === status) return;
  if (current !== "preparing") throw new Error("thread workspace intent is already finalized");
  const creatorChannelType = nullableChannelType(row.creator_channel_type);
  const creatorChannelId = asNullableString(row.creator_channel_id);
  if (
    status === "complete"
    && creatorChannelType !== null
    && (creatorChannelId === null || channelIds.some((channelId) => {
      const policy = getThreadListen(db, botUid, { channelType: 5, channelId });
      return policy?.creatorChannelType !== creatorChannelType || policy.creatorChannelId !== creatorChannelId;
    }))
  ) {
    throw new Error("thread workspace intent cannot complete before its creator relation is confirmed");
  }
  const workspaces = channelIds.map((channelId) =>
    getThreadWorkspace(db, botUid, { channelType: 5, channelId }),
  );
  const hasForkRecord = query(
    db,
    "SELECT 1 FROM thread_forks WHERE id = ? AND bot_uid = ?",
    [id, botUid],
  ).length > 0;
  const readyFork = status === "complete"
    && readyForkIntentEvidence(db, botUid, id, asString(row.group_id), channelIds);
  if (status === "complete" && hasForkRecord && !readyFork) {
    throw new Error("thread workspace intent cannot complete before its fork is ready");
  }
  if (
    (status === "complete" && !readyFork && (
      channelIds.length === 0
      || workspaces.some((workspace) =>
        workspace?.status !== "ready" || workspace.projects.some((project) => !project.created)
      )
    ))
    || (status === "failed" && workspaces.some(
      (workspace) => workspace === null || workspace.status === "preparing",
    ))
  ) {
    throw new Error("thread workspace intent cannot finalize before every mapped workspace is terminal");
  }
  run(
    db,
    `UPDATE thread_workspace_intents SET status = ?, error = ?, updated_at = ?
     WHERE bot_uid = ? AND id = ? AND status = 'preparing'`,
    [status, error, Date.now(), botUid, id],
  );
}

function readyForkIntentEvidence(
  db: Database,
  botUid: string,
  id: string,
  groupId: string,
  channelIds: readonly string[],
): boolean {
  if (channelIds.length !== 1) return false;
  const fork = query(
    db,
    `SELECT phase, group_id, thread_channel_id, session_id, session_file, cwd
     FROM thread_forks WHERE id = ? AND bot_uid = ?`,
    [id, botUid],
  )[0];
  return Boolean(
    fork
    && fork.phase === "ready"
    && asString(fork.group_id) === groupId
    && asString(fork.thread_channel_id) === channelIds[0]
    && asString(fork.session_id).length > 0
    && asString(fork.session_file).length > 0
    && asString(fork.cwd).length > 0
  );
}

function threadWorkspaceIntentFromRow(row: Record<string, unknown>): ThreadWorkspaceIntentRecord {
  const kind = threadWorkspaceIntentKind(row.intent_kind);
  const creatorChannelType = nullableChannelType(row.creator_channel_type);
  const creatorChannelId = asNullableString(row.creator_channel_id);
  const groupId = asString(row.group_id);
  validateIntentCreator(groupId, kind, creatorChannelType, creatorChannelId);
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    groupId,
    kind,
    creatorChannelType,
    creatorChannelId,
    status: threadWorkspaceIntentStatus(row.status),
    channelIds: threadWorkspaceIntentChannels(row.channel_ids),
    error: asNullableString(row.error),
  };
}
function threadWorkspaceIntentKind(value: unknown): "workspace" | "thread-create" {
  if (value !== "workspace" && value !== "thread-create") {
    throw new Error("invalid thread workspace intent kind in store");
  }
  return value;
}

function nullableChannelType(value: unknown): 2 | 5 | null {
  if (value === null || value === undefined) return null;
  if (value === 2 || value === 5) return value;
  throw new Error("invalid thread creator channel type in store");
}

function validateIntentCreator(
  groupId: string,
  kind: "workspace" | "thread-create",
  creatorChannelType: 2 | 5 | null,
  creatorChannelId: string | null,
): void {
  if (groupId.length === 0) throw new Error("thread workspace intent requires a parent group");
  if (creatorChannelId === "") throw new Error("thread workspace intent creator identity cannot be empty");
  if ((creatorChannelType === null) !== (creatorChannelId === null)) {
    throw new Error("thread workspace intent creator identity must be complete");
  }
  if (kind === "thread-create" && creatorChannelType === null) {
    throw new Error("thread creator intent requires its source chat identity");
  }
  if (creatorChannelType === null || creatorChannelId === null) return;
  if (
    creatorChannelType === 2
      ? creatorChannelId !== groupId
      : threadParentGroupId(creatorChannelId) !== groupId || threadShortId(creatorChannelId) === null
  ) {
    throw new Error("thread workspace intent creator is outside its parent group");
  }
}

function threadWorkspaceIntentStatus(value: unknown): ThreadWorkspaceIntentStatus {
  if (value !== "preparing" && value !== "failed" && value !== "complete") {
    throw new Error("invalid thread workspace intent status in store");
  }
  return value;
}

function threadWorkspaceIntentChannels(value: unknown): string[] {
  const channelIds = JSON.parse(asString(value)) as unknown;
  if (!Array.isArray(channelIds) || !channelIds.every((channelId) => typeof channelId === "string")) {
    throw new Error("invalid thread workspace intent channel ids in store");
  }
  return channelIds;
}

export function workspaceDirectories(db: Database, cwd: string): string[] {
  const directories = new Set([cwd]);
  for (const record of listThreadWorkspaceRecords(db)) {
    if (!matchesOwnedResource(cwd, record)) continue;

    const entry = record.entryPath;
    const workspaceRoot = record.workspaceRoot ?? null;
    const projects = record.projects.flatMap((project) => project.path ? [project.path] : []);
    if (entry !== null) directories.add(entry);
    if (workspaceRoot !== null) directories.add(workspaceRoot);
    for (const projectPath of projects) directories.add(projectPath);
  }

  return [...directories];
}

/** True when cwd is related to an owned task root, entry, or project path. */
export function isWorkspaceDirectoryOwned(db: Database, cwd: string): boolean {
  return listThreadWorkspaceRecords(db).some((record) => matchesOwnedResource(cwd, record));
}

function listThreadWorkspaceRecords(db: Database): ThreadWorkspaceRecord[] {
  return query(
    db,
    `SELECT bot_uid, channel_type, channel_id, layout, status, entry_path, workspace_root, projects, legacy_worktree_name, error
     FROM thread_workspaces`,
  ).map(threadWorkspaceFromRow);
}

function matchesOwnedResource(cwd: string, record: ThreadWorkspaceRecord): boolean {
  const entry = record.entryPath;
  const workspaceRoot = record.workspaceRoot ?? null;
  const projects = record.projects.flatMap((project) => project.path ? [project.path] : []);
  return (workspaceRoot !== null && pathsRelated(cwd, workspaceRoot))
    || (entry !== null && pathsRelated(cwd, entry))
    || projects.some((projectPath) => pathsRelated(cwd, projectPath));
}

function threadWorkspaceFromRow(row: Record<string, unknown>): ThreadWorkspaceRecord {
  const layout = row.layout;
  const status = row.status;
  if (layout !== "single" && layout !== "shared" && layout !== "separate") {
    throw new Error("invalid thread workspace layout in store");
  }
  if (status !== "preparing" && status !== "ready" && status !== "failed") {
    throw new Error("invalid thread workspace status in store");
  }
  const projects = JSON.parse(asString(row.projects)) as unknown;
  if (!Array.isArray(projects) || !projects.every(isWorkspaceProject)) {
    throw new Error("invalid thread workspace projects in store");
  }
  const legacyWorktreeName = asNullableString(row.legacy_worktree_name);
  return {
    botUid: asString(row.bot_uid),
    channelType: 5,
    channelId: asString(row.channel_id),
    layout,
    status,
    entryPath: asNullableString(row.entry_path),
    workspaceRoot: asNullableString(row.workspace_root),
    projects,
    ...(legacyWorktreeName === null ? {} : { legacyWorktreeName }),
    error: asNullableString(row.error),
  };
}

function isWorkspaceProject(value: unknown): value is ThreadWorkspaceProjectRecord {
  if (value === null || typeof value !== "object") return false;
  const project = value as Record<string, unknown>;
  return (
    typeof project.name === "string"
    && typeof project.repo === "string"
    && (project.base === undefined || typeof project.base === "string")
    && (project.path === null || typeof project.path === "string")
    && (project.branch === null || typeof project.branch === "string")
    && typeof project.created === "boolean"
    && (project.accessPath === undefined || typeof project.accessPath === "string")
    && (project.accessTarget === undefined || typeof project.accessTarget === "string")
  );
}

function pathsRelated(first: string, second: string): boolean {
  return pathContains(first, second) || pathContains(second, first);
}

function pathContains(parent: string, child: string): boolean {
  const relativePath = relative(parent, child);
  return (
    relativePath === ""
    || (!isAbsolute(relativePath) && relativePath !== ".." && !relativePath.startsWith(`..${sep}`))
  );
}
const THREAD_WORKSPACE_OPERATION_STATUSES = new Set<ThreadWorkspaceOperationStatus>([
  "queued",
  "running",
  "complete",
  "failed",
  "cancelled",
  "unknown",
]);
const THREAD_WORKSPACE_OPERATION_PHASES = new Set([
  "accepted",
  "entry-create-intent",
  "entry-creating",
  "entry-root-created",
  "entry-created",
  "alias-creating",
  "alias-created",
  "project-not-started",
  "project-creating",
  "project-process-exited",
  "project-created",
  "move-dispatch-intent",
  "move-confirmed",
  "committed",
]);

function activeThreadRemovalExists(db: Database, botUid: string, channelId: string): boolean {
  return query(
    db,
    `SELECT 1 FROM host_merge_leases
     WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND operation_kind = 'thread-remove'
       AND (phase IN ('starting', 'running', 'unknown') OR removal_outcome_json IS NULL)
     LIMIT 1`,
    [botUid, channelId],
  ).length > 0;
}

function activeThreadWorkspaceOperationExists(db: Database, botUid: string, channelId: string): boolean {
  return query(
    db,
    `SELECT 1 FROM thread_workspace_operations
     WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ?
       AND status IN ('queued', 'running', 'unknown')
     LIMIT 1`,
    [botUid, channelId],
  ).length > 0;
}

function threadWorkspaceCleanupExists(db: Database, botUid: string, channelId: string): boolean {
  return query(
    db,
    "SELECT 1 FROM thread_workspace_cleanups WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? LIMIT 1",
    [botUid, channelId],
  ).length > 0;
}

function activeWorkspaceRootReserved(
  db: Database,
  botUid: string,
  channelId: string,
  workspaceRoot: string | null | undefined,
): boolean {
  if (workspaceRoot == null) return false;
  if (query(
    db,
    `SELECT 1 FROM thread_workspaces
     WHERE bot_uid = ? AND channel_id <> ? AND (workspace_root = ? OR entry_path = ?)
     LIMIT 1`,
    [botUid, channelId, workspaceRoot, workspaceRoot],
  ).length > 0) return true;
  return query(
    db,
    `SELECT candidate_workspace_json FROM thread_workspace_operations
     WHERE bot_uid = ? AND channel_id <> ? AND status IN ('queued', 'running', 'unknown')`,
    [botUid, channelId],
  ).some(row => {
    const candidate = JSON.parse(asString(row.candidate_workspace_json)) as Record<string, unknown>;
    return candidate.workspaceRoot === workspaceRoot || candidate.entryPath === workspaceRoot;
  });
}

export function reserveThreadWorkspaceCleanup(
  db: Database,
  input: ThreadWorkspaceCleanupInput,
): ThreadWorkspaceCleanupRecord {
  const { id, botUid, target } = input;
  if (
    target.channelType !== 5
    || id.length === 0
    || botUid.length === 0
    || target.channelId.length === 0
    || threadParentGroupId(target.channelId) === null
    || threadShortId(target.channelId) === null
  ) throw new Error("invalid thread workspace cleanup identity");

  return transaction(db, () => {
    const byId = query(
      db,
      "SELECT * FROM thread_workspace_cleanups WHERE id = ?",
      [id],
    )[0];
    if (byId !== undefined) {
      const existing = threadWorkspaceCleanupFromRow(byId);
      if (
        existing.botUid !== botUid
        || existing.channelType !== target.channelType
        || existing.channelId !== target.channelId
      ) throw new Error("thread workspace cleanup id was replayed for a different target");
      return existing;
    }
    const activeCleanup = query(
      db,
      `SELECT 1 FROM thread_workspace_cleanups
       WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ?
         AND phase IN ('reserved', 'cleaning', 'unknown')
       LIMIT 1`,
      [botUid, target.channelId],
    )[0];
    if (activeCleanup) throw new Error("子区工作区清理结果尚未确认");
    if (activeThreadWorkspaceOperationExists(db, botUid, target.channelId)) {
      throw new Error("子区工作区追加尚未结算，拒绝删除");
    }
    if (activeThreadRemovalExists(db, botUid, target.channelId)) {
      throw new Error("子区工作区正在清理，拒绝删除");
    }

    const ownership = getThreadWorkspace(db, botUid, target);
    const binding = getBinding(db, botUid, target);
    const now = Date.now();
    run(
      db,
      `INSERT INTO thread_workspace_cleanups(
         id, bot_uid, channel_type, channel_id, phase, ownership_json, binding_json, created_at, updated_at
       ) VALUES(?, ?, 5, ?, 'reserved', ?, ?, ?, ?)`,
      [
        id,
        botUid,
        target.channelId,
        ownership === null ? null : JSON.stringify(ownership),
        binding === null ? null : JSON.stringify(binding),
        now,
        now,
      ],
    );
    const reserved = getThreadWorkspaceCleanup(db, botUid, id);
    if (reserved === null) throw new Error("thread workspace cleanup reservation disappeared");
    return reserved;
  });
}

export function getThreadWorkspaceCleanup(
  db: Database,
  botUid: string,
  id: string,
): ThreadWorkspaceCleanupRecord | null {
  const row = query(
    db,
    "SELECT * FROM thread_workspace_cleanups WHERE bot_uid = ? AND id = ?",
    [botUid, id],
  )[0];
  return row === undefined ? null : threadWorkspaceCleanupFromRow(row);
}

export function listThreadWorkspaceCleanups(
  db: Database,
  botUid: string,
): ThreadWorkspaceCleanupRecord[] {
  return query(
    db,
    "SELECT * FROM thread_workspace_cleanups WHERE bot_uid = ? ORDER BY created_at, id",
    [botUid],
  ).map(threadWorkspaceCleanupFromRow);
}

/** The remote delete is confirmed; mark owner state before any local resource cleanup begins. */
export function markThreadWorkspaceCleanupDeleted(
  db: Database,
  botUid: string,
  id: string,
): ThreadWorkspaceCleanupRecord {
  return transaction(db, () => {
    const record = getThreadWorkspaceCleanup(db, botUid, id);
    if (record === null) throw new Error("thread workspace cleanup reservation does not exist");
    if (record.phase === "cleaning" || record.phase === "settled") return record;
    if (record.phase !== "reserved") throw new Error("thread workspace cleanup outcome is unknown");

    const target = { channelType: 5 as const, channelId: record.channelId };
    const ownership = getThreadWorkspace(db, botUid, target);
    const binding = getBinding(db, botUid, target);
    if (
      JSON.stringify(ownership) !== JSON.stringify(record.ownership)
      || JSON.stringify(binding) !== JSON.stringify(record.binding)
    ) {
      const now = Date.now();
      run(
        db,
        `UPDATE thread_workspace_cleanups SET phase = 'unknown', updated_at = ?
         WHERE bot_uid = ? AND id = ? AND phase = 'reserved'`,
        [now, botUid, id],
      );
      return { ...record, phase: "unknown", updatedAt: now };
    }

    const nextOwnership = ownership === null ? null : {
      ...ownership,
      status: "failed" as const,
      error: "线程已删除；本地工作区清理尚未完成",
    };
    if (nextOwnership !== null) saveThreadWorkspace(db, nextOwnership);
    const now = Date.now();
    run(
      db,
      `UPDATE thread_workspace_cleanups SET phase = 'cleaning', ownership_json = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND phase = 'reserved'`,
      [
        nextOwnership === null ? null : JSON.stringify(nextOwnership),
        now,
        botUid,
        id,
      ],
    );
    return { ...record, phase: "cleaning", ownership: nextOwnership, updatedAt: now };
  });
}

export function markThreadWorkspaceCleanupUnknown(db: Database, botUid: string, id: string): void {
  transaction(db, () => {
    const record = getThreadWorkspaceCleanup(db, botUid, id);
    if (record === null) return;
    if (record.phase === "settled" || record.phase === "unknown") return;
    const ownership = getThreadWorkspace(db, botUid, {
      channelType: 5,
      channelId: record.channelId,
    });
    const now = Date.now();
    run(
      db,
      `UPDATE thread_workspace_cleanups SET phase = 'unknown', ownership_json = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND phase IN ('reserved', 'cleaning')`,
      [ownership === null ? null : JSON.stringify(ownership), now, botUid, id],
    );
  });
}

/** Retain a tombstone after known cleanup, preventing a deleted thread from accepting new additions. */
export function finishThreadWorkspaceCleanup(db: Database, botUid: string, id: string): void {
  transaction(db, () => {
    const record = getThreadWorkspaceCleanup(db, botUid, id);
    if (record === null) throw new Error("thread workspace cleanup reservation does not exist");
    if (record.phase === "settled") return;
    if (record.phase !== "cleaning") throw new Error("thread workspace cleanup is not known to be settleable");
    const ownership = getThreadWorkspace(db, botUid, {
      channelType: 5,
      channelId: record.channelId,
    });
    const result = run(
      db,
      `UPDATE thread_workspace_cleanups SET phase = 'settled', ownership_json = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND phase = 'cleaning'`,
      [ownership === null ? null : JSON.stringify(ownership), Date.now(), botUid, id],
    );
    if (result.changes !== 1) throw new Error("thread workspace cleanup changed before settlement");
  });
}

/** Only a known permanent remote rejection (or a request not sent) may release a reservation. */
export function releaseThreadWorkspaceCleanup(db: Database, botUid: string, id: string): void {
  const result = run(
    db,
    "DELETE FROM thread_workspace_cleanups WHERE bot_uid = ? AND id = ? AND phase = 'reserved'",
    [botUid, id],
  );
  if (
    result.changes !== 1
    && query(db, "SELECT 1 FROM thread_workspace_cleanups WHERE bot_uid = ? AND id = ?", [botUid, id]).length > 0
  ) throw new Error("thread workspace cleanup cannot be released after deletion may have occurred");
}

/** Pending work may have crossed the remote-delete or local-cleanup boundary before a crash. */
export function recoverThreadWorkspaceCleanups(
  db: Database,
  botUid: string,
): ThreadWorkspaceCleanupRecord[] {
  return transaction(db, () => {
    const pending = query(
      db,
      `SELECT id, channel_id FROM thread_workspace_cleanups
       WHERE bot_uid = ? AND phase IN ('reserved', 'cleaning')`,
      [botUid],
    );
    for (const row of pending) {
      const channelId = asString(row.channel_id);
      const ownership = getThreadWorkspace(db, botUid, { channelType: 5, channelId });
      run(
        db,
        `UPDATE thread_workspace_cleanups SET phase = 'unknown', ownership_json = ?, updated_at = ?
         WHERE bot_uid = ? AND id = ? AND phase IN ('reserved', 'cleaning')`,
        [ownership === null ? null : JSON.stringify(ownership), Date.now(), botUid, asString(row.id)],
      );
    }
    return query(
      db,
      "SELECT * FROM thread_workspace_cleanups WHERE bot_uid = ? AND phase = 'unknown' ORDER BY created_at, id",
      [botUid],
    ).map(threadWorkspaceCleanupFromRow);
  });
}

export function isThreadWorkspaceCleanupBlocked(db: Database, botUid: string, target: ChatTarget): boolean {
  return target.channelType === 5 && threadWorkspaceCleanupExists(db, botUid, target.channelId);
}

export function acceptThreadWorkspaceOperation(
  db: Database,
  input: ThreadWorkspaceOperationInput,
): { operation: ThreadWorkspaceOperationRecord; created: boolean } {
  if (
    input.channelType !== 5
    || input.id.length === 0
    || input.botUid.length === 0
    || input.channelId.length === 0
    || input.groupId.length === 0
    || input.toolCallId.length === 0
    || input.taskId.length === 0
    || input.worktreeNameSeed.length === 0
    || input.expectedWorkspace.channelId !== input.channelId
    || input.expectedWorkspace.botUid !== input.botUid
    || input.expectedBinding.channelType !== 5
    || input.expectedBinding.channelId !== input.channelId
    || input.expectedBinding.botUid !== input.botUid
  ) {
    throw new Error("invalid thread workspace operation identity");
  }
  return transaction(db, () => {
    const existingRow = query(
      db,
      `SELECT * FROM thread_workspace_operations
       WHERE bot_uid = ? AND channel_id = ? AND tool_call_id = ?`,
      [input.botUid, input.channelId, input.toolCallId],
    )[0];
    if (existingRow !== undefined) {
      const existing = threadWorkspaceOperationFromRow(existingRow);
      if (
        existing.id !== input.id
        || existing.taskId !== input.taskId
        || existing.worktreeNameSeed !== input.worktreeNameSeed
        || JSON.stringify(existing.projects) !== JSON.stringify(input.projects)
      ) {
        throw new Error("thread workspace toolCallId was replayed with different arguments");
      }
      return { operation: existing, created: false };
    }
    if (threadWorkspaceCleanupExists(db, input.botUid, input.channelId)) {
      throw new Error("子区工作区已删除或正在清理，拒绝追加项目");
    }
    if (activeThreadRemovalExists(db, input.botUid, input.channelId)) {
      throw new Error("子区工作区正在清理，拒绝追加项目");
    }
    if (activeThreadWorkspaceOperationExists(db, input.botUid, input.channelId)) {
      throw new Error("子区工作区已有未结算追加操作");
    }
    if (activeWorkspaceRootReserved(db, input.botUid, input.channelId, input.candidateWorkspace.workspaceRoot)) {
      throw new Error("shared workspace root is already reserved");
    }

    const now = Date.now();
    const operation: ThreadWorkspaceOperationRecord = {
      ...input,
      scheduleOrder: allocateScheduleOrder(db),
      phase: "accepted",
      status: "queued",
      cancelRequested: false,
      workerLedgerId: null,
      processIdentity: null,
      error: null,
      version: 1,
      createdAt: now,
      updatedAt: now,
    };
    run(
      db,
      `INSERT INTO thread_workspace_operations(
         id, bot_uid, channel_type, channel_id, group_id, tool_call_id, task_id, schedule_order,
         worktree_name_seed, projects_json, expected_workspace_json, expected_binding_json,
         candidate_workspace_json, candidate_binding_json, phase, status, worker_ledger_id,
         process_identity_json, error, version, created_at, updated_at
       ) VALUES(?, ?, 5, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 1, ?, ?)`,
      [
        operation.id,
        operation.botUid,
        operation.channelId,
        operation.groupId,
        operation.toolCallId,
        operation.taskId,
        operation.scheduleOrder,
        operation.worktreeNameSeed,
        JSON.stringify(operation.projects),
        JSON.stringify(operation.expectedWorkspace),
        JSON.stringify(operation.expectedBinding),
        JSON.stringify(operation.candidateWorkspace),
        JSON.stringify(operation.candidateBinding),
        operation.phase,
        operation.status,
        now,
        now,
      ],
    );
    return { operation, created: true };
  });
}

export function getThreadWorkspaceOperation(
  db: Database,
  botUid: string,
  id: string,
): ThreadWorkspaceOperationRecord | null {
  const row = query(
    db,
    "SELECT * FROM thread_workspace_operations WHERE bot_uid = ? AND id = ?",
    [botUid, id],
  )[0];
  return row === undefined ? null : threadWorkspaceOperationFromRow(row);
}
export function requestThreadWorkspaceOperationCancellation(
  db: Database,
  botUid: string,
  id: string,
): boolean {
  const result = run(
    db,
    `UPDATE thread_workspace_operations SET cancel_requested = 1, updated_at = ?
     WHERE bot_uid = ? AND id = ? AND status IN ('queued', 'running')`,
    [Date.now(), botUid, id],
  );
  return result.changes === 1;
}
export function getLatestThreadWorkspaceOperation(
  db: Database,
  botUid: string,
  target: ChatTarget,
): ThreadWorkspaceOperationRecord | null {
  if (target.channelType !== 5) return null;
  const row = query(
    db,
    `SELECT * FROM thread_workspace_operations
     WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ?
     ORDER BY schedule_order DESC, created_at DESC, id DESC LIMIT 1`,
    [botUid, target.channelId],
  )[0];
  return row === undefined ? null : threadWorkspaceOperationFromRow(row);
}

export function listThreadWorkspaceOperations(
  db: Database,
  botUid: string,
): ThreadWorkspaceOperationRecord[] {
  return query(
    db,
    `SELECT * FROM thread_workspace_operations
     WHERE bot_uid = ? ORDER BY schedule_order ASC, created_at ASC, id ASC`,
    [botUid],
  ).map(threadWorkspaceOperationFromRow);
}

export function updateThreadWorkspaceOperation(
  db: Database,
  operation: ThreadWorkspaceOperationRecord,
): ThreadWorkspaceOperationRecord | null {
  const next: ThreadWorkspaceOperationRecord = {
    ...operation,
    version: operation.version + 1,
    updatedAt: Date.now(),
  };
  const result = run(
    db,
    `UPDATE thread_workspace_operations SET
       candidate_workspace_json = ?, candidate_binding_json = ?, phase = ?, status = ?,
       worker_ledger_id = ?, process_identity_json = ?, error = ?, version = ?, updated_at = ?
     WHERE bot_uid = ? AND id = ? AND version = ?`,
    [
      JSON.stringify(next.candidateWorkspace),
      JSON.stringify(next.candidateBinding),
      next.phase,
      next.status,
      next.workerLedgerId,
      next.processIdentity === null ? null : JSON.stringify(next.processIdentity),
      next.error,
      next.version,
      next.updatedAt,
      next.botUid,
      next.id,
      operation.version,
    ],
  );
  return result.changes === 1 ? next : null;
}

/**
 * Publishes actual ownership and the matching binding in one CAS transaction. A failed operation
 * may publish its confirmed partial resources without changing the binding; success requires both
 * original snapshots to still match.
 */
export function finishThreadWorkspaceOperation(
  db: Database,
  operation: ThreadWorkspaceOperationRecord,
  status: "complete" | "failed" | "cancelled",
  publishCandidate: boolean,
  publishBinding: boolean,
): ThreadWorkspaceOperationRecord | null {
  return transaction(db, () => {
    const current = getThreadWorkspaceOperation(db, operation.botUid, operation.id);
    if (
      current === null
      || current.version !== operation.version
      || current.status !== "running"
    ) return null;

    if (publishCandidate) {
      const currentWorkspace = getThreadWorkspace(
        db,
        operation.botUid,
        { channelType: 5, channelId: operation.channelId },
      );
      if (JSON.stringify(currentWorkspace) !== JSON.stringify(operation.expectedWorkspace)) return null;
      if (publishBinding) {
        const currentBinding = getBinding(db, operation.botUid, {
          channelType: 5,
          channelId: operation.channelId,
        });
        if (!sameOperationBinding(currentBinding, operation.expectedBinding)) return null;
      }
      saveThreadWorkspace(db, operation.candidateWorkspace);
      if (
        publishBinding
        && !ensureBindingIfMatches(
          db,
          operation.candidateBinding,
          operation.expectedBinding,
          { clearSessionInitialization: true },
        )
      ) {
        throw new Error("thread workspace binding changed during atomic publication");
      }
    }

    const next: ThreadWorkspaceOperationRecord = {
      ...operation,
      status,
      ...(status === "complete" ? { phase: "committed" as const } : {}),
      version: operation.version + 1,
      updatedAt: Date.now(),
    };
    const result = run(
      db,
      `UPDATE thread_workspace_operations SET
         candidate_workspace_json = ?, candidate_binding_json = ?, phase = ?, status = ?,
         worker_ledger_id = ?, process_identity_json = ?, error = ?, version = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND version = ? AND status = 'running'`,
      [
        JSON.stringify(next.candidateWorkspace),
        JSON.stringify(next.candidateBinding),
        next.phase,
        next.status,
        next.workerLedgerId,
        next.processIdentity === null ? null : JSON.stringify(next.processIdentity),
        next.error,
        next.version,
        next.updatedAt,
        next.botUid,
        next.id,
        operation.version,
      ],
    );
    return result.changes === 1 ? next : null;
  });
}

export function isThreadWorkspaceOperationBlocked(
  db: Database,
  botUid: string,
  target: ChatTarget,
): boolean {
  if (target.channelType !== 5) return false;
  // The session scheduler lets prior work drain and holds later work behind queued operations.
  return query(
    db,
    `SELECT 1 FROM thread_workspace_operations
     WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ?
       AND status IN ('running', 'unknown')
     LIMIT 1`,
    [botUid, target.channelId],
  ).length > 0;
}

function sameOperationBinding(left: BindingRecord | null, right: BindingRecord): boolean {
  return left !== null
    && left.botUid === right.botUid
    && left.channelType === right.channelType
    && left.channelId === right.channelId
    && left.sessionId === right.sessionId
    && left.cwd === right.cwd
    && left.sessionFile === right.sessionFile
    && (left.cwdPinned === true) === (right.cwdPinned === true)
    && (left.sessionSwitchedAt ?? null) === (right.sessionSwitchedAt ?? null)
    && JSON.stringify(left.sessionInitialization ?? null) === JSON.stringify(right.sessionInitialization ?? null);
}

function threadWorkspaceOperationFromRow(row: Record<string, unknown>): ThreadWorkspaceOperationRecord {
  const status = row.status;
  const phase = row.phase;
  if (
    typeof status !== "string"
    || !THREAD_WORKSPACE_OPERATION_STATUSES.has(status as ThreadWorkspaceOperationStatus)
    || typeof phase !== "string"
    || !THREAD_WORKSPACE_OPERATION_PHASES.has(phase)
  ) throw new Error("invalid thread workspace operation status in store");

  const projects = JSON.parse(asString(row.projects_json)) as unknown;
  if (!Array.isArray(projects) || !projects.every((project) =>
    project !== null
    && typeof project === "object"
    && !Array.isArray(project)
    && typeof project.name === "string"
    && typeof project.repo === "string"
    && (project.base === undefined || typeof project.base === "string")
  )) throw new Error("invalid thread workspace operation projects in store");

  const expectedWorkspace = JSON.parse(asString(row.expected_workspace_json)) as unknown;
  const expectedBinding = JSON.parse(asString(row.expected_binding_json)) as unknown;
  const candidateWorkspace = JSON.parse(asString(row.candidate_workspace_json)) as unknown;
  const candidateBinding = JSON.parse(asString(row.candidate_binding_json)) as unknown;
  if (
    expectedWorkspace === null || typeof expectedWorkspace !== "object"
    || expectedBinding === null || typeof expectedBinding !== "object"
    || candidateWorkspace === null || typeof candidateWorkspace !== "object"
    || candidateBinding === null || typeof candidateBinding !== "object"
  ) throw new Error("invalid thread workspace operation snapshot in store");
  const processJson = asNullableString(row.process_identity_json);
  const process = processJson === null ? null : JSON.parse(processJson) as unknown;
  if (
    process !== null
    && (
      process === null
      || typeof process !== "object"
      || !Number.isSafeInteger((process as Record<string, unknown>).pid)
      || !Number.isSafeInteger((process as Record<string, unknown>).pgid)
      || typeof (process as Record<string, unknown>).identity !== "string"
      || typeof (process as Record<string, unknown>).cwd !== "string"
    )
  ) throw new Error("invalid thread workspace operation process identity in store");

  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: 5,
    channelId: asString(row.channel_id),
    groupId: asString(row.group_id),
    toolCallId: asString(row.tool_call_id),
    taskId: asString(row.task_id),
    scheduleOrder: asNumber(row.schedule_order),
    worktreeNameSeed: asString(row.worktree_name_seed),
    projects: projects as Array<{ name: string; repo: string; base?: string }>,
    expectedWorkspace: expectedWorkspace as ThreadWorkspaceRecord,
    expectedBinding: expectedBinding as BindingRecord,
    candidateWorkspace: candidateWorkspace as ThreadWorkspaceRecord,
    candidateBinding: candidateBinding as BindingRecord,
    phase: phase as ThreadWorkspaceOperationRecord["phase"],
    status: status as ThreadWorkspaceOperationStatus,
    cancelRequested: asNumber(row.cancel_requested) === 1,
    workerLedgerId: asNullableString(row.worker_ledger_id),
    processIdentity: process as ThreadWorkspaceOperationRecord["processIdentity"],
    error: asNullableString(row.error),
    version: asNumber(row.version),
    createdAt: asNumber(row.created_at),
    updatedAt: asNumber(row.updated_at),
  };
}

function threadWorkspaceCleanupFromRow(row: Record<string, unknown>): ThreadWorkspaceCleanupRecord {
  const phase = row.phase;
  if (
    phase !== "reserved"
    && phase !== "cleaning"
    && phase !== "unknown"
    && phase !== "settled"
  ) throw new Error("invalid thread workspace cleanup phase in store");

  const botUid = asString(row.bot_uid);
  const channelId = asString(row.channel_id);
  const ownershipJson = asNullableString(row.ownership_json);
  const bindingJson = asNullableString(row.binding_json);
  const ownership = ownershipJson === null ? null : JSON.parse(ownershipJson) as unknown;
  const binding = bindingJson === null ? null : JSON.parse(bindingJson) as unknown;
  if (
    ownership !== null
    && (
      ownership === null
      || typeof ownership !== "object"
      || Array.isArray(ownership)
      || (ownership as Record<string, unknown>).botUid !== botUid
      || (ownership as Record<string, unknown>).channelType !== 5
      || (ownership as Record<string, unknown>).channelId !== channelId
      || !Array.isArray((ownership as Record<string, unknown>).projects)
    )
  ) throw new Error("invalid thread workspace cleanup ownership snapshot in store");
  if (
    binding !== null
    && (
      binding === null
      || typeof binding !== "object"
      || Array.isArray(binding)
      || (binding as Record<string, unknown>).botUid !== botUid
      || (binding as Record<string, unknown>).channelType !== 5
      || (binding as Record<string, unknown>).channelId !== channelId
    )
  ) throw new Error("invalid thread workspace cleanup binding snapshot in store");

  return {
    id: asString(row.id),
    botUid,
    channelType: 5,
    channelId,
    phase: phase as ThreadWorkspaceCleanupPhase,
    ownership: ownership as ThreadWorkspaceRecord | null,
    binding: binding as BindingRecord | null,
    createdAt: asNumber(row.created_at),
    updatedAt: asNumber(row.updated_at),
  };
}
