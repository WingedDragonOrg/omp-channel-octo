import { relative, isAbsolute, sep } from "node:path";
import { Database } from "bun:sqlite";
import type { ChatTarget, ThreadListenLevel, ThreadSummary } from "../contracts.ts";
import type {
  ThreadWorkspaceIntentInput,
  ThreadWorkspaceIntentRecord,
  ThreadWorkspaceIntentStatus,
  ThreadWorkspaceProjectRecord,
  ThreadWorkspaceRecord,
} from "./store-records.ts";
import { getThreadListen, recordCreatedThreadListen } from "./store-thread-listen.ts";
import {
  asNullableString,
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
